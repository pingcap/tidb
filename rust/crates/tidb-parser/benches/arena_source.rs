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

//! Stable workloads corresponding to Go's three arena benchmarks.

use std::hint::black_box;

use tidb_parser::arena::{alloc, Arena, Slab};

#[derive(Default)]
struct Node {
    value: i64,
}

#[test]
fn benchmark_arena_alloc() {
    let mut arena = Arena::new();
    for index in 0..10_000 {
        if index % 1_000 == 0 {
            arena.reset();
        }
        let mut node = alloc::<Node>(&arena);
        node.value = index;
        black_box(node);
    }
}

#[test]
fn benchmark_heap_alloc() {
    for index in 0..10_000 {
        black_box(Box::new(Node { value: index }));
    }
}

#[test]
fn benchmark_slab_alloc() {
    let mut slab = Slab::<Node>::default();
    for index in 0..10_000 {
        if index % 1_000 == 0 {
            slab.reset();
        }
        let node = slab.alloc();
        node.borrow_mut().value = index;
        black_box(node);
    }
}
