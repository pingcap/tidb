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

//! The five original root benchmarks. Store cases retain parallel calls;
//! batch timing includes native worker creation and is not a Go speedup claim.
extern crate test;
use super::*;
use test::{black_box, Bencher};
#[bench]
fn benchmark_sketch_increment(b: &mut Bencher) {
    let mut sketch = sketch::Sketch::new(16);
    b.bytes = 1;
    b.iter(|| sketch.increment(black_box(1)));
}
#[bench]
fn benchmark_sketch_estimate(b: &mut Bencher) {
    let mut sketch = sketch::Sketch::new(16);
    sketch.increment(1);
    b.bytes = 1;
    b.iter(|| black_box(sketch.estimate(black_box(1))));
}
fn value() -> Item<i64> {
    Item {
        key: 1,
        conflict: 0,
        value: Some(1),
        cost: 0,
        expiration: None,
    }
}
fn parallel(b: &mut Bencher, operation: impl Fn(&Store<i64>) + Sync) {
    let store = Store::new(Duration::from_secs(5));
    store.set(value());
    b.bytes = 4096;
    b.iter(|| {
        thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..1024 {
                        operation(&store);
                    }
                });
            }
        })
    });
}
#[bench]
fn benchmark_store_get(b: &mut Bencher) {
    parallel(b, |s| {
        black_box(s.get(1, 0));
    });
}
#[bench]
fn benchmark_store_set(b: &mut Bencher) {
    parallel(b, |s| s.set(value()));
}
#[bench]
fn benchmark_store_update(b: &mut Bencher) {
    parallel(b, |s| {
        black_box(s.update(&value()));
    });
}
