use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rkv::storage::memtable::{MemTable, VersionedValue};

fn bench_memtable_put(c: &mut Criterion) {
    let mt = MemTable::new(256 * 1024 * 1024);

    c.bench_function("memtable_put", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key-{}", i);
            let val = VersionedValue::new(vec![0u8; 256], "node1", 0);
            mt.put(black_box(key), black_box(val));
            i += 1;
        });
    });
}

fn bench_memtable_get(c: &mut Criterion) {
    let mt = MemTable::new(256 * 1024 * 1024);

    // Pre-populate
    for i in 0..100_000 {
        let key = format!("key-{}", i);
        let val = VersionedValue::new(vec![0u8; 256], "node1", 0);
        mt.put(key, val);
    }

    c.bench_function("memtable_get", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key-{}", i % 100_000);
            black_box(mt.get(&key));
            i += 1;
        });
    });
}

fn bench_memtable_scan(c: &mut Criterion) {
    let mt = MemTable::new(256 * 1024 * 1024);

    for i in 0..100_000 {
        let key = format!("key-{:08}", i);
        let val = VersionedValue::new(vec![0u8; 64], "node1", 0);
        mt.put(key, val);
    }

    c.bench_function("memtable_scan_100", |b| {
        b.iter(|| {
            black_box(mt.scan("key-00010000", "key-00090000", 100));
        });
    });
}

criterion_group!(benches, bench_memtable_put, bench_memtable_get, bench_memtable_scan);
criterion_main!(benches);
