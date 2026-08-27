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

//! Documentary gap ports for
//! `pkg/planner/core/casetest/tiflash_predicate_push_down_test.go`
//! (`pkg/planner.part9` items 516-517 on `origin/master`).
//!
//! Both Go tests execute their input statements, pull the plan out of
//! ShowProcess(), assert `NormalizePlan` == `FlattenPhysicalPlan` +
//! `NormalizeFlatPlan` digests, then decode/compare normalized-plan rows from
//! the plan_normalized book. Everything downstream of `core.NormalizePlan` is
//! unported here.

/// GO PORT of
/// `pkg/planner/core/casetest/tiflash_predicate_push_down_test.go:33
/// TestTiFlashLateMaterialization`.
///
/// t1(a,b,c,time) indexed on (a,b,c,t), doubled 13 times and fully analyzed
/// plus `flush stats_delta *.*` (:42), allow_tiflash_cop ON (:44); TiFlash
/// replica injected (:47), late-materialization ON (:49); each plan
/// normalized twice via NormalizePlan vs FlattenPhysicalPlan+NormalizeFlatPlan
/// requiring identical strings AND digests (:66-71), then golden rows are the
/// DECODED normalized plan (`plancodec.DecodeNormalizedPlan`, :74-78).
#[test]
#[ignore = "go-parity-gap: plancodec normalization/decode pipeline and TiFlash replica metadata unported"]
fn tiflash_late_materialization_normalized_plan_golden() {}

/// GO PORT of
/// `pkg/planner/core/casetest/tiflash_predicate_push_down_test.go:86
/// TestInvertedIndex`.
///
/// Runs under a mocked dual-TiFlash cluster (`mockstore.WithMockTiFlash(2)`)
/// with `infosync.NewMockTiFlash()` and DDL failpoint MockCheckColumnarIndexProcess
/// returning 1 (:90-99); t1 gains two columnar inverted indexes
/// (`columnar index idx_a (a) using inverted ...`) :102-104, is doubled,
/// replicated and analyzed; the same normalize-twice contract as above gates
/// each plan_normalized entry while isolation engines stay tiflash
/// (:107-129).
#[test]
#[ignore = "go-parity-gap: columnar/inverted index DDL + mock TiFlash cluster + normalized-plan decoding unported"]
fn inverted_index_columnar_scan_normalized_plan_golden() {}
