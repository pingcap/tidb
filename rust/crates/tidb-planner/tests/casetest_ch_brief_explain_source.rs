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

//! Documentary gap ports for `pkg/planner/core/casetest/ch`
//! (`pkg/planner.part3` items 155–157 on `origin/master`).
//!
//! Both tests plan TPC-C-shaped queries over `tpcc` tables whose schemas
//! live in the main_test.go helpers (`createCustomer`/`createItem`/...
//! ch/main_test.go:48-141), mark every table with an available TiFlash
//! replica (`testkit.SetTiFlashReplica`), load histogram jsons per table,
//! zero both broadcast-join thresholds, then compare whole
//! `explain format='brief'` outputs from the `ch_suite` book. The Rust
//! workspace has no executor/domain/TiFlash-replica meta injection, so
//! these stay documented gaps. The bootstrap (`main_test.go:32 TestMain`)
//! also zeroes async-commit SafeWindow/AllowedClockDrift and enables the
//! stats-cache memory quota; skipped-reason in the receipt.

/// GO PORT of `pkg/planner/core/casetest/ch/ch_test.go:25 TestQ2`.
///
/// Re-derived contract: item/nation/region/stock/supplier created with
/// TiFlash replicas; their per-table stats loaded from `tpcc.*.json`; with
/// `tidb_broadcast_join_threshold_size/count = 0`, every `ch_suite` input
/// must keep its exact recorded `explain format='brief'` output.
#[test]
#[ignore = "go-parity-gap: needs domain/table-meta TiFlash replica injection, json stats loading and full SQL planning -- none of the ch suite's execution surface exists in tidb-planner"]
fn ch_q2_brief_explain_golden_with_loaded_stats() {
    // Restore: recreate the tpcc subset, SetTiFlashReplica equivalents,
    // load stats, then diff explain format='brief' rows per input.
}

/// GO PORT of `pkg/planner/core/casetest/ch/ch_test.go:64 TestQ5`.
///
/// Same contract as TestQ2 but over customer/orders/order_line too (all
/// seven tpcc tables plus their jsons); the extra joins push broadcast
/// thresholds to 0 so every join becomes shuffle/repartitioned, which is
/// exactly what the brief goldens capture.
#[test]
#[ignore = "go-parity-gap: same missing surface as ch_q2_brief_explain_golden_with_loaded_stats"]
fn ch_q5_brief_explain_golden_with_loaded_stats() {}
