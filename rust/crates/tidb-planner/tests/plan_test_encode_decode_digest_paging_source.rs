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

//! `pkg/planner.part14` DOCUMENTED GAP ports from
//! `pkg/planner/core/plan_test.go` (encode/digest/paging/import items;
//! the BuildFinalModeAggregation and clone items live in sibling files):
//!
//! Every Go test here retrieves the LIVE plan from
//! `tk.Session().ShowProcess()` after executing a statement, then inspects
//! encodings, digests or explain trees. This crate has no session, no
//! executor, and no `core.EncodePlan`/`FlattenPhysicalPlan` walker over
//! physical plans (the low-level codec leaves DO exist in
//! `tidb_util::plancodec`), so each item is an honest `#[ignore]` gap port.
//! Benchmarks keep Go's Benchmark name shape so the batch gate filter
//! `not test(/bench/)` skips them exactly like `go test` skips Benchmarks.

/// GO PARITY GAP port of `pkg/planner/core/plan_test.go:46
/// TestEncodeDecodePlan`.
///
/// go-parity-gap: needs `core.EncodePlan`/`core.FlattenPhysicalPlan`/
/// `core.EncodeFlatPlan` over live session plans plus
/// `plancodec.DecodePlan`. Go pins that the encoded tree decodes with
/// `time`/`loops` runtime fields for select/insert/update/delete/CTE
/// statements, is EMPTY for prepared-pointer plans, and keeps
/// statement-specific nodes (`PartitionUnion`, `Shuffle`,
/// `ShuffleReceiver`, CTE `1->Column#3` projection renaming) in BOTH the
/// tree and flat encodings.
#[test]
#[ignore = "go-parity-gap: EncodePlan/FlattenPhysicalPlan walkers over live session plans unported (low-level plancodec leaves exist in tidb-util)"]
fn encode_decode_plan_roundtrips_runtime_stats_and_special_nodes() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_test.go:150
/// TestNormalizedDigest`.
///
/// go-parity-gap: needs session plan building over the eight fixture tables
/// plus `core.NormalizedPhysicalPlan`. Go pins a 40+ row golden table of
/// (query, normalized plan string, plan digest, normalized digest) where
/// constant literals, db names and partition names normalize away, and
/// normalization is KEYED (e.g. same digest for `a=1` and `a=2`, different
/// across join orders and index choices).
#[test]
#[ignore = "go-parity-gap: NormalizedPhysicalPlan over session-optimized plans unported; golden table preserved in the Go source"]
fn normalized_digest_pins_normalized_plan_and_digest_pairs() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_test.go:368
/// TestExplainFormatHintRecoverableForTiFlashReplica`.
///
/// go-parity-gap: needs a virtual TiFlash replica over a session table. Go
/// pins the round trip: `explain` shows `mpp[tiflash]`, `explain
/// format='hint'` prints `read_from_storage(@`sel_1` tiflash[`test`.`t`])`,
/// and re-explaining WITH that hint recovers the `mpp[tiflash]` plan.
#[test]
#[ignore = "go-parity-gap: needs TiFlash replica session explain and the hint-format printer"]
fn explain_format_hint_recoverable_for_tiflash_replica() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_test.go:394
/// BenchmarkDecodePlan`.
///
/// go-parity-gap: benchmark over `plancodec.DecodePlan` of a 50k-union
/// session plan's encoding.
#[test]
#[ignore = "go-parity-gap: benchmark over DecodePlan of a session-built 50k-union plan encoding"]
fn benchmark_decode_plan() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_test.go:427
/// BenchmarkEncodePlan`.
///
/// go-parity-gap: benchmark over `core.EncodePlan` of a six-way
/// 8192-partition static-pruned join session plan.
#[test]
#[ignore = "go-parity-gap: benchmark over EncodePlan of a session-built partitioned join plan"]
fn benchmark_encode_plan() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_test.go:450
/// BenchmarkEncodeFlatPlan`.
///
/// go-parity-gap: benchmark over `FlattenPhysicalPlan` + `EncodeFlatPlan`
/// of the same six-way partitioned join plan.
#[test]
#[ignore = "go-parity-gap: benchmark over FlattenPhysicalPlan/EncodeFlatPlan of a session-built plan"]
fn benchmark_encode_flat_plan() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_test.go:474 TestCopPaging`.
///
/// go-parity-gap: needs the cop-paging cost decision inside task
/// conversion over an analyzed 1024-row table. Go pins, via `explain
/// format='plan_tree'`, that `limit 960` under an ordered index scan with
/// residual conditions goes PAGING (cop `Selection` above the
/// `IndexRangeScan keep order:true`), while `limit 961` — the threshold
/// being cop-row cap 1024 vs rows — does not, and the shape is stable
/// across 10 repetitions per case.
#[test]
#[ignore = "go-parity-gap: needs cop paging decision + plan_tree explain over an analyzed session table"]
fn cop_paging_limit_thresholds_stable_across_repetitions() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_test.go:753
/// TestImportIntoBuildPlan`.
///
/// go-parity-gap: needs the IMPORT INTO plan builder over session tables
/// and the tidb_snapshot write gate. Go pins
/// `ErrWrongValueCountOnRow` for `IMPORT INTO t1 FROM select a from t2`
/// (column count mismatch, also with an explicit column list), the
/// "can not execute write statement when 'tidb_snapshot' is set" error,
/// and `ErrTableNotExists` when the target table does not exist.
#[test]
#[ignore = "go-parity-gap: IMPORT INTO plan building + tidb_snapshot gate over session tables unported"]
fn import_into_build_plan_rejects_mismatched_and_snapshot_writes() {}
