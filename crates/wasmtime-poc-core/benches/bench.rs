use std::time::{Duration, Instant};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;
use wasmtime_poc_core::{AppConfig, UninitializedAppContext};

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();

    println!("{:?}", std::env::current_dir());

    c.bench_function("app", |b| {
        b.to_async(&runtime).iter_custom(move |iters| async move {
            let app_config = AppConfig::from_app_config_file("../../bench-config.toml").unwrap();
            let mut total_duration = Duration::from_secs(0);

            for _ in 0..iters {
                let unitialized_app_context = UninitializedAppContext::new(&app_config).unwrap();
                let initialized_app_context =
                    unitialized_app_context.async_initialize_modules().unwrap();

                let start = Instant::now();
                black_box(initialized_app_context.start().await.unwrap());
                total_duration += start.elapsed();
            }

            total_duration
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
