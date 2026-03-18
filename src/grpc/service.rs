use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::gossip::GossipService;
use crate::replication::coordinator::{self, ReplicationCoordinator};
use crate::storage::engine::StorageEngine;
use crate::storage::memtable::VersionedValue;

use super::proto;
use super::proto::kv_service_server::KvService;
use super::proto::internal_service_server::InternalService;

// ─── Client-facing service ───

pub struct KvServiceImpl {
    coordinator: Arc<ReplicationCoordinator>,
    engine: Arc<StorageEngine>,
}

impl KvServiceImpl {
    pub fn new(coordinator: Arc<ReplicationCoordinator>, engine: Arc<StorageEngine>) -> Self {
        Self {
            coordinator,
            engine,
        }
    }
}

#[tonic::async_trait]
impl KvService for KvServiceImpl {
    async fn get(
        &self,
        request: Request<proto::GetRequest>,
    ) -> Result<Response<proto::GetResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .coordinator
            .get(&req.key, req.read_quorum)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        if !result.is_success() {
            return Err(Status::unavailable(format!(
                "Read quorum not met: got {}/{} successes",
                result.successes, result.required
            )));
        }

        match result.value {
            Some(val) => Ok(Response::new(proto::GetResponse {
                found: true,
                value: val.value,
                vector_clock: Some(coordinator::vc_to_proto(&val.vector_clock)),
            })),
            None => Ok(Response::new(proto::GetResponse {
                found: false,
                value: Vec::new(),
                vector_clock: None,
            })),
        }
    }

    async fn put(
        &self,
        request: Request<proto::PutRequest>,
    ) -> Result<Response<proto::PutResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .coordinator
            .put(req.key, req.value, req.ttl_ms, req.write_quorum)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        if !result.is_success() {
            return Err(Status::unavailable(format!(
                "Write quorum not met: got {}/{} successes",
                result.successes, result.required
            )));
        }

        Ok(Response::new(proto::PutResponse {
            success: true,
            vector_clock: result.value.map(|vc| coordinator::vc_to_proto(&vc)),
        }))
    }

    async fn delete(
        &self,
        request: Request<proto::DeleteRequest>,
    ) -> Result<Response<proto::DeleteResponse>, Status> {
        let req = request.into_inner();
        let result = self
            .coordinator
            .delete(&req.key, req.write_quorum)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::DeleteResponse {
            success: result.is_success(),
        }))
    }

    async fn scan(
        &self,
        request: Request<proto::ScanRequest>,
    ) -> Result<Response<proto::ScanResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };
        let results = self.engine.scan(&req.start_key, &req.end_key, limit);

        let pairs = results
            .into_iter()
            .map(|(k, v)| proto::KeyValue {
                key: k,
                value: v.value,
                vector_clock: Some(coordinator::vc_to_proto(&v.vector_clock)),
            })
            .collect();

        Ok(Response::new(proto::ScanResponse { pairs }))
    }
}

// ─── Internal inter-node service ───

pub struct InternalServiceImpl {
    engine: Arc<StorageEngine>,
    gossip: Arc<GossipService>,
}

impl InternalServiceImpl {
    pub fn new(engine: Arc<StorageEngine>, gossip: Arc<GossipService>) -> Self {
        Self { engine, gossip }
    }
}

#[tonic::async_trait]
impl InternalService for InternalServiceImpl {
    async fn replicate(
        &self,
        request: Request<proto::ReplicateRequest>,
    ) -> Result<Response<proto::ReplicateResponse>, Status> {
        let req = request.into_inner();
        let versioned = req
            .value
            .map(coordinator::value_from_proto)
            .ok_or_else(|| Status::invalid_argument("missing value"))?;

        if req.is_delete {
            self.engine
                .delete(&req.key)
                .map_err(|e| Status::internal(e.to_string()))?;
        } else {
            self.engine
                .put(req.key, versioned)
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        Ok(Response::new(proto::ReplicateResponse { success: true }))
    }

    async fn get_internal(
        &self,
        request: Request<proto::GetInternalRequest>,
    ) -> Result<Response<proto::GetInternalResponse>, Status> {
        let req = request.into_inner();
        match self.engine.get_raw(&req.key) {
            Some(val) => Ok(Response::new(proto::GetInternalResponse {
                found: true,
                value: Some(coordinator::value_to_proto(&val)),
            })),
            None => Ok(Response::new(proto::GetInternalResponse {
                found: false,
                value: None,
            })),
        }
    }

    async fn delete_internal(
        &self,
        request: Request<proto::DeleteInternalRequest>,
    ) -> Result<Response<proto::DeleteInternalResponse>, Status> {
        let req = request.into_inner();
        self.engine
            .delete(&req.key)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::DeleteInternalResponse {
            success: true,
        }))
    }

    async fn gossip(
        &self,
        request: Request<proto::GossipMessage>,
    ) -> Result<Response<proto::GossipAck>, Status> {
        let msg = request.into_inner();
        let response_nodes = self.gossip.handle_gossip(&msg);
        Ok(Response::new(proto::GossipAck {
            nodes: response_nodes,
        }))
    }

    async fn hinted_handoff(
        &self,
        request: Request<proto::HandoffRequest>,
    ) -> Result<Response<proto::HandoffResponse>, Status> {
        let req = request.into_inner();
        let mut accepted = 0u32;

        for hint in req.hints {
            if let Some(value) = hint.value {
                let versioned = coordinator::value_from_proto(value);
                if hint.is_delete {
                    if self.engine.delete(&hint.key).is_ok() {
                        accepted += 1;
                    }
                } else if self.engine.put(hint.key, versioned).is_ok() {
                    accepted += 1;
                }
            }
        }

        Ok(Response::new(proto::HandoffResponse { accepted }))
    }

    async fn transfer_keys(
        &self,
        request: Request<proto::TransferRequest>,
    ) -> Result<Response<proto::TransferResponse>, Status> {
        let req = request.into_inner();
        let mut accepted = 0u32;

        for kv in req.pairs {
            let vc = kv
                .vector_clock
                .map(coordinator::vc_from_proto)
                .unwrap_or_default();
            let versioned = VersionedValue {
                value: kv.value,
                vector_clock: vc,
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                ttl_ms: 0,
                deleted: false,
            };
            if self.engine.put(kv.key, versioned).is_ok() {
                accepted += 1;
            }
        }

        Ok(Response::new(proto::TransferResponse { accepted }))
    }
}
