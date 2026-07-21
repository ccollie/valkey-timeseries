use criterion::{Criterion, criterion_group, criterion_main};

fn allocations_smoke(c: &mut Criterion) {
    c.bench_function("allocations/smoke", |b| b.iter(|| 4usize + 4));
}

criterion_group!(benches, allocations_smoke);
criterion_main!(benches);
