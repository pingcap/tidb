// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Executable translations of every benchmark in
//! `pkg/lightning/membuf/buffer_test.go`.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use tidb_util::membuf::{
    with_block_size, with_pool_memory_limiter, Buffer, Bytes, Limiter, Pool, SliceLocation,
};

const DATA_NUM: usize = 100 * 1024 * 1024;
const SORT_DATA_NUM: usize = 1024 * 1024;

fn finish(name: &str, started: Instant) {
    println!("{name}: {:?}", started.elapsed());
}

fn default_buffer() -> (Arc<Pool>, Buffer) {
    let pool = Arc::new(Pool::new([]));
    let buffer = pool.new_buffer([]);
    (pool, buffer)
}

fn benchmark_store_slice() {
    let mut data: Vec<Option<Bytes>> = vec![None; DATA_NUM];
    let started = Instant::now();
    let (pool, mut buffer) = default_buffer();
    for item in &mut data {
        *item = buffer.alloc_bytes(10);
    }
    black_box(&data);
    data.fill(None);
    buffer.destroy();
    pool.destroy();
    finish("BenchmarkStoreSlice", started);
}

fn benchmark_store_location() {
    let mut data = vec![SliceLocation::default(); DATA_NUM];
    let started = Instant::now();
    let (pool, mut buffer) = default_buffer();
    for item in &mut data {
        let (_, location) = buffer.alloc_bytes_with_slice_location(10);
        *item = location;
    }
    black_box(&data);
    buffer.destroy();
    pool.destroy();
    finish("BenchmarkStoreLocation", started);
}

fn fill_slice(random: &mut standard_fastrand::Rng, bytes: &Bytes) {
    random.fill(&mut bytes.as_mut_slice());
}

fn benchmark_sort_slice(name: &str, collect_runtime_garbage: bool) {
    let mut data = Vec::with_capacity(SORT_DATA_NUM);
    let started = Instant::now();
    let (pool, mut buffer) = default_buffer();
    let mut random = standard_fastrand::Rng::with_seed(6716);
    for _ in 0..SORT_DATA_NUM {
        let bytes = buffer.alloc_bytes(10).unwrap();
        fill_slice(&mut random, &bytes);
        data.push(bytes);
    }
    if collect_runtime_garbage {
        // Rust has no tracing runtime garbage collector.
        black_box(());
    }
    data.sort_unstable_by(|left, right| left.as_slice().cmp(&right.as_slice()));
    black_box(&data);
    data.clear();
    buffer.destroy();
    pool.destroy();
    finish(name, started);
}

fn benchmark_sort_location(name: &str, collect_runtime_garbage: bool) {
    let mut data = Vec::with_capacity(SORT_DATA_NUM);
    let started = Instant::now();
    let (pool, mut buffer) = default_buffer();
    let mut random = standard_fastrand::Rng::with_seed(6716);
    for _ in 0..SORT_DATA_NUM {
        let (bytes, location) = buffer.alloc_bytes_with_slice_location(10);
        fill_slice(&mut random, &bytes.unwrap());
        data.push(location);
    }
    if collect_runtime_garbage {
        black_box(());
    }
    data.sort_unstable_by(|left, right| {
        buffer
            .get_slice(left)
            .as_slice()
            .cmp(&buffer.get_slice(right).as_slice())
    });
    black_box(&data);
    buffer.destroy();
    pool.destroy();
    finish(name, started);
}

fn benchmark_sort_location_escape(escape: bool) {
    enum Duplicate {
        Escaped(Box<SliceLocation>),
        Value(SliceLocation),
    }

    let mut data = Vec::with_capacity(SORT_DATA_NUM);
    let started = Instant::now();
    let (pool, mut buffer) = default_buffer();
    let mut random = standard_fastrand::Rng::with_seed(6716);
    for _ in 0..SORT_DATA_NUM {
        let (bytes, location) = buffer.alloc_bytes_with_slice_location(10);
        fill_slice(&mut random, &bytes.unwrap());
        data.push(location);
    }
    let mut duplicate = None;
    data.sort_unstable_by(|left, right| {
        let ordering = buffer
            .get_slice(left)
            .as_slice()
            .cmp(&buffer.get_slice(right).as_slice());
        if ordering.is_eq() && duplicate.is_none() {
            duplicate = Some(if escape {
                Duplicate::Escaped(Box::new(*left))
            } else {
                Duplicate::Value(*left)
            });
        }
        ordering
    });
    match duplicate {
        Some(Duplicate::Escaped(location)) => {
            black_box(location.length);
        }
        Some(Duplicate::Value(location)) => {
            black_box(location.length);
        }
        None => {
            black_box(0_i32);
        }
    }
    buffer.destroy();
    pool.destroy();
    finish(
        if escape {
            "BenchmarkSortLocationWithEscape"
        } else {
            "BenchmarkSortLocationWithoutEscape"
        },
        started,
    );
}

fn benchmark_concurrent_acquire() {
    let started = Instant::now();
    let limiter = Arc::new(Limiter::new(512 * 1024 * 1024));
    let pool = Arc::new(Pool::new([
        with_pool_memory_limiter(Some(limiter)),
        with_block_size(4 * 1024),
    ]));
    let handles = (0..1000)
        .map(|_| {
            let pool = Arc::clone(&pool);
            std::thread::spawn(move || {
                let mut buffer = pool.new_buffer([]);
                for _ in 0..1000 {
                    let _ = buffer.alloc_bytes(100);
                }
                buffer.destroy();
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }
    pool.destroy();
    finish("BenchmarkConcurrentAcquire", started);
}

fn main() {
    benchmark_store_slice();
    benchmark_store_location();
    benchmark_sort_slice("BenchmarkSortSlice", false);
    benchmark_sort_location("BenchmarkSortLocation", false);
    benchmark_sort_slice("BenchmarkSortSliceWithGC", true);
    benchmark_sort_location("BenchmarkSortLocationWithGC", true);
    benchmark_sort_location_escape(true);
    benchmark_sort_location_escape(false);
    benchmark_concurrent_acquire();
}
