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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

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
