use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use generator::build_test_data;

fn bench_generator_data(c: &mut Criterion) {
    let mut group = c.benchmark_group("generator_data");
    group
        .significance_level(0.1)
        .sample_size(10)
        .measurement_time(Duration::from_secs(120)    )
        ;
    group.bench_with_input(
        BenchmarkId::new("generator_data", "1_000_000_000"),
        &1_000_000_000,
        |b, num_rows_to_create| {
            b.iter(|| {
                build_test_data(black_box(*num_rows_to_create)).unwrap();
            });
        },
    );

    group.finish();
}

criterion_group!(benches, bench_generator_data);
criterion_main!(benches);
