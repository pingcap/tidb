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

use crate::ddl::run_create_table_on;
use crate::driver::Catalog;
use crate::explain::{explain_select_stmt, ExplainFormat};
use crate::StmtContext;

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
