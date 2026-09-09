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

//! Functional kernel port of `TestOptimizerCostFactorHints`
//! (`pkg/planner/core/casetest/hint/hint_test.go:360`, `pkg/planner.part4`
//! item 207 on `origin/master`).
//!
//! # What Go's test pins and what transfers here
//!
//! The Go test builds t(a int primary key, b int key(b)), inserts five rows
//! (1..5)³-style triples and `analyze`s it (:367-369). Each of five scenarios
//! then picks a plan shape whose dominant operator O ∈ {TableFullScan,
//! TableReader, TableRangeScan, IndexScan, IndexReader}, pre-raises IRRELEVANT
//! factors to 100 on the session (both compared statements see them), runs
//! `explain format=verbose` baseline versus a statement wrapped in
//! `/*+ SET_VAR(tidb_opt_O_cost_factor=2) */`, and requires baseline < hinted
//! (:370-441). The behavior pinned is therefore: each named session variable
//! enters the final plan cost exactly at its own operator, multiplicatively,
//! so raising only that variable from its default strictly raises the total.
//!
//! The Rust owner of that placement is [`tidb_planner::plan_cost_ver2`]:
//! `table_scan_cost` selects `factors.table_row_id_scan` /
//! `table_range_scan` / `table_full_scan` off the same isChildOfINL +
//! HasFullRangeScan switch as `getPlanCostVer24PhysicalTableScan`
//! (pkg/planner/core/plan_cost_ver2.go:212-221), `index_scan_cost`
//! multiplies `IndexScanCostFactor` (:144-145), and `reader_cost` divides by
//! concurrency before multiplying the reader factor (:307-308 index reader,
//! :346-347 table reader). Defaults are all 1.0 both sides
//! (pkg/sessionctx/vardef/tidb_vars.go:1523-1530 vs
//! `CostFactorVars::default()`).
//!
//! Each scenario below mirrors the SAME plan shape through those transcreated
//! primitives with the SAME two variable states (session isolation + SET_VAR)
//! and asserts the same inequality. Row counts/sizes are Go-shaped models of
//! the analyzed fixture (five rows; three int columns ⇒ row size 24 via
//! getAvgRowSize's per-type widths), which cancels in every comparison.
//!
//! NOT transferred — recorded as the trailing ignored port: the SET_VAR hint
//! evaluation itself, explain-verbose rendering, and choosing the shape in a
//! live optimizer loop.

use tidb_planner::cost_usage::CostVer2;
use tidb_planner::plan_cost_ver2::{
    index_scan_cost, reader_cost, table_scan_cost, CostFactorVars, TableScanInput,
    TableScanPenaltyInput, Ver2Factors,
};

/// Rows of the analyzed fixture table (`insert into t values` ×5,
/// hint_test.go:368-369).
const T_ROWS: f64 = 5.0;

/// Model of `getAvgRowSize` for three analyzed int columns (a, b, c): three
/// 8-byte columns. Equal on both sides of every comparison.
const SELECT_ALL_ROW_SIZE: f64 = 24.0;

/// Single projected int column `b` over the covering key.
const B_COLUMN_ROW_SIZE: f64 = 8.0;

/// Cardinality of `a > 3` / `b > 3` over {1..5}: values 4 and 5 qualify.
const RANGE_ROWS: f64 = 2.0;

/// Go reads `DistSQLScanConcurrency()` inside both reader costings; the
/// default 15 (`CostSessionOpts::default().distsql_scan_concurrency`) applies
/// here and cancels between the compared statements.
const DISTSQL_SCAN_CONCURRENCY: f64 = 15.0;

/// Penalty inputs of an ANALYZEd table with zero pending modifications: no
/// full-range-scan penalty rows (`getTableScanPenalty` returns 0, which keeps
/// the full-scan tree to its single scan term).
fn analyzed_stable_table_penalty() -> TableScanPenaltyInput {
    TableScanPenaltyInput {
        analyze_row_count: 5,
        ..Default::default()
    }
}

/// `PhysicalTableReader(TableFullScan)` ver2 total:
/// `div(scan + net, concurrency) * table_reader`, scan carrying
/// `* table_full_scan` (plan_cost_ver2.go:213-221 full branch, :346-347
/// reader factor).
fn full_scan_under_table_reader(factors: &CostFactorVars) -> CostVer2 {
    let factors_v2 = Ver2Factors::default();
    let scan = table_scan_cost(
        None,
        TableScanInput {
            rows: T_ROWS,
            row_size: SELECT_ALL_ROW_SIZE,
            is_child_of_inl: None,
            has_full_range_scan: true,
            penalty: analyzed_stable_table_penalty(),
        },
        &factors_v2.tikv_scan,
        factors,
    );
    reader_cost(
        None,
        T_ROWS,
        SELECT_ALL_ROW_SIZE,
        &factors_v2.tidb_to_kv_net,
        DISTSQL_SCAN_CONCURRENCY,
        &scan,
        factors.table_reader,
    )
}

/// `PhysicalTableReader(TableRangeScan on the clustered PK)` ver2 total; the
/// non-full-range branch selects `table_range_scan` (:217-219).
fn range_scan_under_table_reader(factors: &CostFactorVars) -> CostVer2 {
    let factors_v2 = Ver2Factors::default();
    let scan = table_scan_cost(
        None,
        TableScanInput {
            rows: RANGE_ROWS,
            row_size: SELECT_ALL_ROW_SIZE,
            is_child_of_inl: None,
            has_full_range_scan: false,
            penalty: analyzed_stable_table_penalty(),
        },
        &factors_v2.tikv_scan,
        factors,
    );
    reader_cost(
        None,
        RANGE_ROWS,
        SELECT_ALL_ROW_SIZE,
        &factors_v2.tidb_to_kv_net,
        DISTSQL_SCAN_CONCURRENCY,
        &scan,
        factors.table_reader,
    )
}

/// `PhysicalIndexReader(IndexRangeScan(key(b)))` ver2 total: the covering
/// index scan multiplied by `IndexScanCostFactor` (:144-145), wrapped by the
/// index-reader net/div term with `IndexReaderCostFactor` (:300-308). The
/// index-ID jitter Go adds (`idx.ID % 100 / 1e6`) is carried as `Some(1)`
/// identically on both sides so it cannot flip any comparison.
fn covering_index_range_scan_under_index_reader(factors: &CostFactorVars) -> CostVer2 {
    let factors_v2 = Ver2Factors::default();
    let scan = index_scan_cost(
        None,
        RANGE_ROWS,
        B_COLUMN_ROW_SIZE,
        &factors_v2.tikv_scan,
        factors.index_scan,
        Some(1),
    );
    reader_cost(
        None,
        RANGE_ROWS,
        B_COLUMN_ROW_SIZE,
        &factors_v2.tidb_to_kv_net,
        DISTSQL_SCAN_CONCURRENCY,
        &scan,
        factors.index_reader,
    )
}

/// Scenario pair state: session variables shared by both statements plus the
/// one variable the SET_VAR hint overrides on the second statement.
struct ScenarioPair {
    session: CostFactorVars,
}

impl ScenarioPair {
    fn with_session(mutate: impl FnOnce(&mut CostFactorVars)) -> Self {
        let mut session = CostFactorVars::default();
        mutate(&mut session);
        Self { session }
    }

    fn baseline(&self) -> CostFactorVars {
        self.session
    }

    fn set_var(&self, mutate: impl FnOnce(&mut CostFactorVars)) -> CostFactorVars {
        let mut hinted = self.session;
        mutate(&mut hinted);
        hinted
    }
}

fn assert_baseline_cheaper(shape: impl Fn(&CostFactorVars) -> CostVer2, baseline: &CostFactorVars, hinted: &CostFactorVars) {
    let base_total = shape(baseline);
    let hint_total = shape(hinted);
    assert!(
        base_total.value() < hint_total.value(),
        "baseline {:e} must stay cheaper than hinted {:e}",
        base_total.value(),
        hint_total.value()
    );
}

/// GO PORT scenario 1, hint_test.go:370-382 "Test tableFullScan cost factor
/// increase via hint": session raises `tidb_opt_index_scan_cost_factor=100`
/// (:372, irrelevant-operator isolation seen by BOTH statements), hint sets
/// `tidb_opt_table_full_scan_cost_factor=2` (:376); require.Less(:380).
#[test]
fn set_var_raising_table_full_scan_factor_raises_full_scan_reader_total() {
    let pair = ScenarioPair::with_session(|vars| vars.index_scan = 100.0);
    assert_baseline_cheaper(
        full_scan_under_table_reader,
        &pair.baseline(),
        &pair.set_var(|vars| vars.table_full_scan = 2.0),
    );
}

/// GO PORT scenario 2, hint_test.go:384-396 "Test tableReader cost factor
/// increase via hint": same isolation (:386), hint sets
/// `tidb_opt_table_reader_cost_factor=2` (:390); require.Less(:394).
#[test]
fn set_var_raising_table_reader_factor_raises_full_scan_reader_total() {
    let pair = ScenarioPair::with_session(|vars| vars.index_scan = 100.0);
    assert_baseline_cheaper(
        full_scan_under_table_reader,
        &pair.baseline(),
        &pair.set_var(|vars| vars.table_reader = 2.0),
    );
}

/// GO PORT scenario 3, hint_test.go:398-409 "Test tableRangeScan cost factor
/// increase": session raises index-scan AND table-full-scan factors to 100
/// (:400-401), hint sets `tidb_opt_table_range_scan_cost_factor=2` (:405);
/// require.Less(:409).
#[test]
fn set_var_raising_table_range_scan_factor_raises_pk_range_reader_total() {
    let pair = ScenarioPair::with_session(|vars| {
        vars.index_scan = 100.0;
        vars.table_full_scan = 100.0;
    });
    assert_baseline_cheaper(
        range_scan_under_table_reader,
        &pair.baseline(),
        &pair.set_var(|vars| vars.table_range_scan = 2.0),
    );
}

/// GO PORT scenario 4, hint_test.go:414-426 "Test IndexScan cost factor
/// increase": session raises table-full-scan to 100 (:416), hint sets
/// `tidb_opt_index_scan_cost_factor=2` (:420); require.Less(:424).
#[test]
fn set_var_raising_index_scan_factor_raises_covering_index_reader_total() {
    let pair = ScenarioPair::with_session(|vars| vars.table_full_scan = 100.0);
    assert_baseline_cheaper(
        covering_index_range_scan_under_index_reader,
        &pair.baseline(),
        &pair.set_var(|vars| vars.index_scan = 2.0),
    );
}

/// GO PORT scenario 5, hint_test.go:428-440 "Test IndexReader cost factor
/// increase": session raises table-full-scan to 100 (:430), hint sets
/// `tidb_opt_index_reader_cost_factor=2` (:434); require.Less(:438).
#[test]
fn set_var_raising_index_reader_factor_raises_covering_index_reader_total() {
    let pair = ScenarioPair::with_session(|vars| vars.table_full_scan = 100.0);
    assert_baseline_cheaper(
        covering_index_range_scan_under_index_reader,
        &pair.baseline(),
        &pair.set_var(|vars| vars.index_reader = 2.0),
    );
}

/// GO PORT remainder of `hint_test.go:360 TestOptimizerCostFactorHints` that
/// cannot transfer: the `/*+ SET_VAR(...) */` hint must be parsed out of the
/// statement text, applied ONLY to that statement's session snapshot inside
/// live planning, and observed through `explain format=verbose`'s printed
/// total-cost column over the actual chosen physical tree; the fixture also
/// runs DML + analyze through a store.
#[test]
#[ignore = "go-parity-gap: no SET_VAR hint-to-variable pipeline, verbose-explain renderer or live plan selection exists; the per-factor placement itself is pinned by the five running scenario ports above"]
fn optimizer_cost_factor_hints_setvar_end_to_end() {}
