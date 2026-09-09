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

//! Documentary gap ports for `pkg/planner/core/casetest/enforcempp`
//! (`pkg/planner.part3` items 174–180 on `origin/master`).
//!
//! All seven tests compare whole EXPLAIN outputs (and session warning
//! lists) from the `enforce_mpp_suite` book on tables whose meta gets a
//! hacked available TiFlash replica (`testkit.SetTiFlashReplica` or direct
//! `tbl.Meta().TiFlashReplica` assignment). Plan-row node ids are
//! normalized by stripping `_[0-9]+` suffixes into `_N`
//! (enforce_mpp_test.go:33-46). The Rust crate has no TiFlash replica meta,
//! no MPP-task costing and no warning sink, so every port is a documented
//! gap. The package's enforce_mpp TestMain is outside this part's item
//! range; nothing else is skipped silently.

/// GO PORT of `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:69
/// TestEnforceMPP`.
///
/// Re-derived contract: general cases over t(a,b,idx(a)), s(unique a + ii
/// composite) and t3 partitioned `by list columns (sala)`; with
/// tidb_enable_chunk_rpc=on and tidb_allow_tiflash_cop=ON plus TiFlash
/// replicas on all three tables, each suite input must reproduce its plan
/// rows — retried up to 5s via require.Eventuallyf because replica-marking
/// is asynchronous — and warnings filtered of skyline-pruning noise
/// (`"remain after pruning paths for"`) must equal the book.
#[test]
#[ignore = "go-parity-gap: needs live MPP costing with TiFlash replica meta injection; no MPP task generation exists in tidb-planner"]
fn enforce_mpp_general_cases_node_id_normalized() {}

/// GO PORT of `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:135
/// TestEnforceMPPWarning1`.
///
/// General warning cases planned against
/// t(a int, b int as (a+1), c enum('xx','yy'), d bit(1)): generated-column
/// expression plus enum/bit types cannot always push to TiFlash and the
/// recorded warnings must match exactly alongside plan rows.
#[test]
#[ignore = "go-parity-gap: warning emission paths (RaiseWarningWhenMPPEnforced) are unported, as recorded in src/enforce.rs module header refusals"]
fn enforce_mpp_warning_generated_column_enum_bit() {}

/// GO PORT of `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:188
/// TestEnforceMPPWarning2`.
///
/// Hash-partitioned table `t (a int, b char(20))` with an available replica:
/// pins which hash-partitioned reads survive MPP enforcement and which
/// warnings they raise.
#[test]
#[ignore = "go-parity-gap: same missing MPP enforcement/warning surface as enforce_mpp_warning_generated_column_enum_bit"]
fn enforce_mpp_warning_hash_partitioned_char() {}

/// GO PORT of `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:233
/// TestEnforceMPPWarning3`.
///
/// New-collation matrix: inputs include literal directives
/// `cmd: enable-new-collation` / `cmd: disable-new-collation` that flip
/// `collate.SetNewCollationEnabledForTest`, so the same char(20) table must
/// produce collation-dependent plans AND warnings per enabled state.
#[test]
#[ignore = "go-parity-gap: collation-dependent MPP enforcement decisions and the test-only collation toggle are unported"]
fn enforce_mpp_warning_new_collation_toggle() {}

/// GO PORT of `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:287
/// TestEnforceMPPWarning4`.
///
/// Join-focused warnings under `tidb_hash_join_version=optimized` over two
/// primary-key tables with replicas: which join shapes keep MPP alive vs.
/// degrade with matching warnings.
#[test]
#[ignore = "go-parity-gap: optimized-hash-join variance lives in executor/plan costing this workspace never ported"]
fn enforce_mpp_warning_optimized_hash_join() {}

/// GO PORT of `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:330
/// TestMPP2PhaseAggPushDown`.
///
/// Two-phase aggregate pushdown goldens over c(c_id bigint),
/// o(o_id,c_id not null) plus t filled with five identical (1,1) rows, all
/// carrying TiFlash replicas; suite rows pin when the partial+final agg
/// pair appears in the MPP plan.
#[test]
#[ignore = "go-parity-gap: two-phase MPP aggregate rule operates only inside the unported optimizer/implementation set"]
fn mpp_two_phase_agg_push_down_golden() {}

/// GO PORT of `pkg/planner/core/casetest/enforcempp/enforce_mpp_test.go:380
/// TestMPPSkewedGroupDistinctRewrite`.
///
/// Skewed group-distinct aggregate rewrite with
/// tidb_allow_tiflash_cop=ON over t(a,b not null,c,d date,e varchar): the
/// rewrite of distinct aggregates under group-skew must reproduce both the
/// transformed plan rows and their warnings from the book.
#[test]
#[ignore = "go-parity-gap: skewed distinct-agg rewrite rules are part of the unported MPP optimization pass"]
fn mpp_skewed_group_distinct_rewrite_golden() {}
