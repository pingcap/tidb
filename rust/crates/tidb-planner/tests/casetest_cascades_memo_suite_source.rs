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

//! Documentary gap ports for `pkg/planner/core/casetest/cascades`
//! (`pkg/planner.part3` items 131–134 on `origin/master`).
//!
//! These tests parse SQL, run the Go preprocess/build/logical-optimize
//! pipeline over a live session, feed the optimized plan into
//! `memo.NewMemo(...).Init(lp)`, and compare per-group rendered state
//! (group stringer + first group-expression + logical property stats/
//! schema/FD strings) against the `cascades_suite`/`cascades_template`
//! books. The Rust workspace ports memo scaffolding types (`memo_group_id`,
//! `group_expr`, `explore_mark`) but has no end-to-end SQL->logical-plan
//! builder, no group stat derivation, and no testkit session, so every
//! port below is a documented gap, not an approximation. The bootstrap
//! (`main_test.go:29 TestMain`) only loads the two suites; recorded as
//! skipped-reason in the receipt.

/// GO PORT of `pkg/planner/core/casetest/cascades/memo_test.go:37
/// TestCascadesTemplate`.
///
/// Re-derived contract: inside `RunTestUnderCascades`, basic data flow is
/// checked first (`t(a int primary key, b int)` with rows (1..4), then
/// `select a from t` returns them in order), then every input of the
/// `cascades_template` book must produce byte-stable
/// `explain format='plan_tree'` output regardless of planner mode — the
/// book records per-caller goldens because plans may differ between
/// cascades and classic modes.
#[test]
#[ignore = "go-parity-gap: RunTestUnderCascades dual-mode execution needs the live session/executor stack; explain format='plan_tree' rendering over real plans is unported"]
fn cascades_template_plan_tree_golden_cases() {
    // Restore: create+insert+select against the mock store, then re-run each
    // book input through explain format='plan_tree' comparing row goldens.
}

/// GO PORT of `pkg/planner/core/casetest/cascades/memo_test.go:65
/// TestDeriveStats`.
///
/// Re-derived contract: for `cascades_suite` inputs over analyzed
/// t1/t2 (key(a,b)), after build + logical optimize +
/// FlagCollectPredicateColumnsPoint + `ExtractFD`, `Memo.Init` derives
/// stats, at which point each memo group's rendered line —
/// `g.String()`, first `ge.String()`, and
/// `logic prop:{stats:{count X, ColNDVs Y, GroupNDVs Z}, schema:{...},
/// fd:{...}}` with explicit `nil` spellings when missing — must equal the
/// golden. Also resets `PlanColumnID` per case and pins
/// `StmtCtx.OperatorNum` in the record.
#[test]
#[ignore = "go-parity-gap: needs Preprocess/Build/LogicalOptimizeTest pipeline, Memo.Init stat derivation (up-down group-NDV propagation) and FD extraction -- all unported; Rust memo leaf keeps only ids/marks"]
fn derive_stats_memo_group_states_golden() {
    // Restore: rebuild the pipeline, call the equivalent of ForEachGroup and
    // diff every rendered group line against cascades_suite.json.
}

/// GO PORT of `pkg/planner/core/casetest/cascades/memo_test.go:160
/// TestGroupNDVCols`.
///
/// Same rendering contract as TestDeriveStats but on unanalyzed-shape
/// t1/t2 fixtures (4 vs 9 distinct-grouping rows): it specifically pins the
/// GroupNDV column sets after up-down propagation, plus enabling
/// tidb_enable_chunk_rpc only to keep explain stats stable. The comment
/// before the loop fixes the invariant: once derive finished inside
/// `Init`, upper operators no longer pass group cols down during memo
/// bottom-up building.
#[test]
#[ignore = "go-parity-gap: same missing pipeline as derive_stats_memo_group_states_golden (no plan builder, no stat propagation, no live session)"]
fn group_ndv_cols_memo_group_states_golden() {
    // Restore: identical to the DeriveStats restore path but for the
    // group-NDV book entries.
}
