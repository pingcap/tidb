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

//! Go `pkg/meta.meta_test.go` middle-slice benchmark bodies ("pkg/meta.part2")
//! that no other Rust test in this crate pins. The surrounding `func Test*`
//! items of this slice (TestSnapshot, TestElement, TestDBKey, TestTableKey,
//! TestAutoTableIDKey, TestAutoRandomTableIDKey, TestIterDatabases,
//! TestSequenceKey, TestCreateMySQLDatabase, TestIsTableInfoMustLoad,
//! TestIsTableInfoMustLoadSubStringsOrder, TestTableNameExtract,
//! TestNameExtractFromJob, TestPlacementPolicy, TestMaskingPolicy,
//! TestResourceGroup and the TestMeta body) are already ported; see
//! `rust/testport/receipts/b039.md` for that mapping table.
//!
//! The two functions below translate Go's `BenchmarkGenGlobalIDs` /
//! `BenchmarkGenGlobalIDOneByOne` loop assertions into correctness tests:
//! batched allocation hands out `count` contiguous ids ending at the new
//! global counter value, and one-by-one allocation is strictly contiguous,
//! both starting from a fresh metadata root where the first id is 1. Go runs
//! them against `mockstore.EmbedUnistore` inside a rolled-back transaction;
//! here [`tidb_meta::transaction::MemoryTransaction`] is the same in-memory
//! boundary used by every other semantic test in this crate, and the loop
//! count is fixed instead of driven by the benchmark timer.

use tidb_meta::transaction::{MemoryTransaction, Mutator};

/// Go `BenchmarkGenGlobalIDs` (`pkg/meta/meta_test.go:599`): after `b.N`
/// iterations of `m.GenGlobalIDs(10)` on a fresh mutator (transaction never
/// committed), the last returned batch has length 10 and `ids[9] == b.N*10`.
#[test]
fn bench_gen_global_ids_batches_are_contiguous_and_end_at_the_counter() {
    const ITERATIONS: i64 = 100;
    let meta = Mutator::new(MemoryTransaction::at_start_ts(42));
    let mut ids = Vec::new();
    for _ in 0..ITERATIONS {
        ids = meta.gen_global_ids(10).unwrap();
    }
    assert_eq!(ids.len(), 10);
    assert_eq!(ids[9], ITERATIONS * 10);
}

/// Go `BenchmarkGenGlobalIDOneByOne` (`pkg/meta/meta_test.go:625`): after
/// `b.N` iterations of ten `m.GenGlobalID()` calls each on a fresh mutator
/// (transaction never committed), the final id equals `b.N*10`.
#[test]
fn bench_gen_global_id_one_by_one_is_strictly_contiguous() {
    const ITERATIONS: i64 = 100;
    let meta = Mutator::new(MemoryTransaction::at_start_ts(42));
    let mut id = 0;
    for _ in 0..ITERATIONS {
        for _ in 0..10 {
            id = meta.gen_global_id().unwrap();
        }
    }
    assert_eq!(id, ITERATIONS * 10);
}
