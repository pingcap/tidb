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

//! `pkg/executor/explain_test.go` on the Rust side: the one plain-EXPLAIN
//! format contract this tier can run, plus the EXPLAIN ANALYZE runtime-column
//! tests that remain gaps because `crate::explain` renders `N/A` execution
//! info (the port's documented placeholder for counters it never collects).

use tidb_ast::{QueryStmt, Stmt};
use tidb_parser;

use crate::StmtContext;
use crate::ddl::run_create_table_on;
use crate::driver::Catalog;
use crate::explain::{ExplainFormat, explain_select_stmt};

/// Go `pkg/executor/explain_test.go:552::TestExplainFormatPlanTree` (first half): plain
/// `EXPLAIN FORMAT = 'plan_tree'` reports exactly Go's four columns
/// (`pkg/planner/core/common_plans.go:712`:
/// `{"id", "task", "access object", "operator info"}`).
#[test]
fn explain_plan_tree_reports_four_columns() {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE t (a INT, b INT, INDEX idx(a))", &mut catalog).unwrap();

    // Go parses `explain format='plan_tree' select * from t where a = 5`;
    // the format lands in ExplainFormat::parse the same way Go's
    // preprocessor normalizes it.
    let format = ExplainFormat::parse("plan_tree").unwrap();
    let stmt = tidb_parser::parse("select * from t where a = 5").unwrap();
    let Stmt::Query(query) = stmt else {
        panic!("expected a query statement");
    };
    let QueryStmt::Select(select) = &*query else {
        panic!("expected a select statement");
    };

    let (columns, rows) =
        explain_select_stmt(select, &catalog, "test", &StmtContext::for_query(), format).unwrap();

    assert_eq!(
        columns
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        ["id", "task", "access object", "operator info"],
    );
    assert!(!rows.is_empty(), "plan_tree must still report the plan");
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.len(), 4, "row {index} should have 4 columns");
    }
}

/// Go `pkg/executor/explain_test.go:559-563` (second half): `EXPLAIN ANALYZE
/// FORMAT = 'plan_tree'` must fail with an error mentioning `plan_tree`
/// (`pkg/planner/core/common_plans.go` default arm: "explain format
/// 'plan_tree' with analyze is not supported now").
// go-parity-gap: this tier's `explain_analyze_select_stmt` renders the
// analyze columns for every parsed format instead of rejecting
// plan_tree-with-analyze; the refusal Go's switch arm produces is unported.
#[test]
#[ignore]
fn explain_analyze_plan_tree_is_rejected_with_format_error() {}

/// Go `pkg/executor/explain_test.go:34::TestExplainAnalyzeMemory`.
// go-parity-gap: pins that memory columns are `N/A` exactly for operators
// without memory accounting in `explain analyze` output; this tier renders
// constant N/A execution/memory/disk columns (`crate::explain`) and has no
// per-operator memory column policy to contradict.
#[test]
#[ignore]
fn explain_analyze_memory_columns_follow_operator_class() {}

/// Go `pkg/executor/explain_test.go:86::TestMemoryAndDiskUsageAfterClose`.
// go-parity-gap: asserts session `StmtCtx.MemTracker` is fully released and
// its max consumed after a query closes (`pkg/executor/explain_test.go:86`);
// this tier's StatementMemory is per-statement with no post-close session
// tracker readout to assert.
#[test]
#[ignore]
fn memory_and_disk_usage_return_to_baseline_after_close() {}

/// Go `pkg/executor/explain_test.go:123::TestExplainAnalyzeExecutionInfo`.
// go-parity-gap: pins the non-zero "time:..., loops:..., rows:..." execution
// info column across operator classes; this tier collects no runtime timing.
#[test]
#[ignore]
fn explain_analyze_execution_info_is_present_for_every_operator() {}

/// Go `pkg/executor/explain_test.go:205::TestCheckActRowsWithUnistore`.
// go-parity-gap: pins exact actRows per plan node against real unistore reads
// (plus `explain analyze format='ru'`), requiring the storage-backed
// act-rows metering this tier replaces with trace-metered counts for the
// whole tree only in `explain_analyze_select_stmt`'s actRows column; the
// per-SQL expectations need the store-level row accounting Go records.
#[test]
#[ignore]
fn check_act_rows_with_unistore_matches_recorded_counts() {}

/// Go `pkg/executor/explain_test.go:284::TestExplainAnalyzeCTEMemoryAndDiskInfo`.
// go-parity-gap: CTE executor memory/disk columns in `explain analyze` under
// a 10240-byte query quota; this tier renders N/A disk/memory and has no CTE
// spill counters.
#[test]
#[ignore]
fn explain_analyze_cte_memory_and_disk_info_switches_with_quota() {}

/// Go `pkg/executor/explain_test.go:306::TestIssue35296AndIssue43024`.
// go-parity-gap: IndexMerge `explain analyze` per-child execution-info
// columns (`^time:0s` refutation across the five merge children); IndexMerge
// execution info rendering is unported.
#[test]
#[ignore]
fn issue35296_index_merge_children_report_execution_time() {}

/// Go `pkg/executor/explain_test.go:323::TestIssue35911`.
// go-parity-gap: pins IndexLookUp build-side vs probe-side duration
// ordering and the `table_task: [{... concurrency: N}]` stats rendering for
// parallel apply; no runtime durations or apply concurrency stats exist in
// this tier.
#[test]
#[ignore]
fn issue35911_apply_stats_report_recorded_concurrency() {}

/// Go `pkg/executor/explain_test.go:364::TestTotalTimeCases`.
// go-parity-gap: pins which plan lines print wall `time:` versus
// `total_time:` for a correlated scalar subquery, with and without parallel
// apply; requires the runtime timing columns this tier renders as N/A.
#[test]
#[ignore]
fn total_time_cases_distinguish_walltime_and_total_time_lines() {}

/// Go `pkg/executor/explain_test.go:417::TestExplainJSON`.
// go-parity-gap: `EXPLAIN FORMAT = 'tidb_json'` tree-of-operators JSON
// encoding cross-checked row-for-row against `format = 'row'`
// (`pkg/planner/core` `ExplainInfoForEncode`); the tidb_json format is
// unported (`ExplainFormat::parse` accepts row/brief/plan_tree only).
#[test]
#[ignore]
fn explain_json_matches_row_format_tree() {}

/// Go `pkg/executor/explain_test.go:499::TestExplainFormatInCtx`.
// go-parity-gap: pins `StmtCtx.InExplainStmt`/`ExplainFormat` per format and
// the plan-cache interaction (`@@last_plan_from_cache` for
// `format='plan_cache'`); this tier's StmtContext carries neither the
// explain-format snapshot nor a plan cache.
#[test]
#[ignore]
fn explain_format_in_ctx_records_format_and_cache_state() {}

/// Go `pkg/executor/explain_test.go:538::TestExplainImportFromSelect`.
// go-parity-gap: `EXPLAIN IMPORT INTO ... FROM SELECT ...` plan shape
// (ImportInto -> TableReader -> TableFullScan); this tier's explain surface
// has no IMPORT INTO statement.
#[test]
#[ignore]
fn explain_import_from_select_reports_import_into_tree() {}
