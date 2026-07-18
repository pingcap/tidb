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

//! Source-backed tests for sharded hash-join entry chains.

use std::sync::Arc;
use std::thread;

use tidb_exec::concurrent_entry_map::{ConcurrentEntryMap, RowPointer, SHARD_COUNT};

#[test]
fn concurrent_map_preserves_source_insert_and_lookup_contract() {
    // Source: pkg/executor/join/concurrent_map.go:20-79 and
    // pkg/executor/join/concurrent_map_test.go:27-67 (TestConcurrentMap).
    let map = Arc::new(ConcurrentEntryMap::new());
    const ITERATIONS: u32 = 1_000;
    const MOD: u64 = 111;

    let first = Arc::clone(&map);
    let left = thread::spawn(move || {
        for index in 0..ITERATIONS / 2 {
            first.insert(
                u64::from(index) % MOD,
                RowPointer {
                    chunk_index: index,
                    row_index: index,
                },
            );
        }
    });
    let second = Arc::clone(&map);
    let right = thread::spawn(move || {
        for index in ITERATIONS / 2..ITERATIONS {
            second.insert(
                u64::from(index) % MOD,
                RowPointer {
                    chunk_index: index,
                    row_index: index,
                },
            );
        }
    });
    left.join().expect("first insert worker");
    right.join().expect("second insert worker");

    for index in 0..ITERATIONS {
        let (head, found) = map.get(u64::from(index) % MOD);
        assert!(found);
        assert!(head.expect("key has a chain").iter().any(|pointer| pointer
            == RowPointer {
                chunk_index: index,
                row_index: index,
            }));
    }
    assert_eq!(map.get(MOD), (None, false));
    assert_eq!(map.get(MOD + 1), (None, false));
    assert_eq!(map.len(), ITERATIONS as usize);
    assert_eq!(SHARD_COUNT, 320);
}

#[test]
fn concurrent_map_memory_accounting_is_deterministic() {
    // Source: pkg/executor/join/concurrent_map_test.go:70-102
    // (TestConcurrentMapMemoryUsage). Exact Go MemAwareMap bytes are ABI
    // dependent, so this leaf verifies the portable inserted-entry accounting.
    let map = ConcurrentEntryMap::new();
    const ITERATIONS: u32 = 10_240;
    for index in 0..ITERATIONS {
        assert_eq!(
            map.insert(
                u64::from(index),
                RowPointer {
                    chunk_index: index,
                    row_index: index,
                }
            ),
            std::mem::size_of::<RowPointer>()
        );
    }
    assert_eq!(map.len(), ITERATIONS as usize);
    assert_eq!(
        map.estimated_memory_bytes(),
        ITERATIONS as usize * std::mem::size_of::<RowPointer>()
    );
    assert!(!map.is_empty());
    assert_eq!(map.snapshot().len(), ITERATIONS as usize);
}
