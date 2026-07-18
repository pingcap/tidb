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

//! Source-backed tests for transaction-history summary metadata.

use tidb_exec::txn_summary::TransactionSummaryCache;

#[test]
fn transaction_summary_cache_preserves_fnv_and_lru_contract() {
    // Source: pkg/session/txninfo/summary.go:15-106 and
    // pkg/infoschema/test/clustertablestest/tables_test.go:1424-1452
    // (TestTiDBTrxSummary's distinct digest sequence and summary lookup).
    let mut cache = TransactionSummaryCache::new(2);
    cache.on_transaction_end(["begin", "update", "commit"]);
    let first = cache.summaries();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].sql_digests(), ["begin", "update", "commit"]);

    cache.on_transaction_end(["select 1"]);
    assert_eq!(cache.summaries().len(), 2);
    let first_digest = cache.summaries()[1].digest();

    // A repeated sequence moves to the front without adding a duplicate.
    cache.on_transaction_end(["begin", "update", "commit"]);
    let summaries = cache.summaries();
    assert_eq!(summaries.len(), 2);
    assert_eq!(summaries[0].digest(), first_digest);
    assert_eq!(summaries[1].sql_digests(), ["select 1"]);

    // A third distinct sequence evicts the least-recently-used entry.
    cache.on_transaction_end(["select 2"]);
    let summaries = cache.summaries();
    assert_eq!(summaries.len(), 2);
    assert_eq!(summaries[0].sql_digests(), ["select 2"]);
    assert_eq!(summaries[1].sql_digests(), ["begin", "update", "commit"]);
}

#[test]
fn transaction_summary_cache_resize_and_zero_capacity_match_source() {
    // Source: pkg/session/txninfo/summary.go:58-70,99-106.
    let mut cache = TransactionSummaryCache::new(2);
    cache.on_transaction_end(["a"]);
    cache.on_transaction_end(["b"]);
    cache.resize(1);
    assert_eq!(cache.summaries().len(), 1);
    assert_eq!(cache.summaries()[0].sql_digests(), ["b"]);

    cache.resize(0);
    cache.on_transaction_end(["c"]);
    assert!(cache.summaries().is_empty());
}
