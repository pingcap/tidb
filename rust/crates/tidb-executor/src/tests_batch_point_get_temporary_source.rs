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

//! Port of the observable half of Go
//! `pkg/executor/batch_point_get_test.go::TestPointGetForTemporaryTable`
//! (`pkg/executor/batch_point_get_test.go:204`): point and batch point gets
//! over a GLOBAL temporary table read the session's own rows and nothing
//! else.
//!
//! Go pins the "never visits the shared store" half with the
//! `unistore/rpcServerBusy` failpoint (any store RPC would fail); that
//! mechanism does not exist here -- this tier's storage seam is in-process --
//! and the same property is structural:
//! `pkg/executor/batch_point_get_test.go` needs a failpoint because Go's read
//! path COULD silently fall through to the shared store, while this tier's
//! global temporary rows live in the table's own session overlay
//! (`kv_table.rs` `swap_storage`, citing Go's
//! `temptable.TemporaryTableSnapshotInterceptor`). The row-level assertions
//! below are the same contract on both sides. The pessimistic-lock and
//! cached-snapshot halves of that Go file are recorded as `#[ignore]` gap
//! tests in the sibling `tests_batch_point_get_locking_gaps` module.

use tidb_ast::Stmt;
use tidb_datatype::Datum;

use crate::{run_create_table_on, run_insert_on, run_select_on, Catalog, StmtContext};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn text_rows(catalog: &Catalog, sql: &str) -> Vec<Vec<String>> {
    run_select_on(sql, catalog, &ctx())
        .expect("select succeeds")
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|datum| match &datum {
                    Datum::Int(value) => value.to_string(),
                    Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
                    Datum::Null => "NULL".to_owned(),
                    other => panic!("unexpected datum {other:?}"),
                })
                .collect()
        })
        .collect()
}

fn global_temp_catalog() -> Catalog {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create global temporary table t1 (id int primary key, val int) on commit delete rows",
        &mut catalog,
    )
    .expect("global temporary table creates");
    run_insert_on("insert into t1 values (1, 1)", &mut catalog, &ctx())
        .expect("the session's own row is written");
    catalog
}

/// Go `pkg/executor/batch_point_get_test.go:204::TestPointGetForTemporaryTable`,
/// batch point get half: `select * from t1 where id in (1, 2, 3)` answers
/// only the session's row `(1, 1)` -- the missing handles `(2, 3)` produce no
/// output row (Go `batch_point_get.go:446`, the `IsValueEmpty` -> no-row
/// rule), and `where id in (2, 3)` answers nothing at all.
#[test]
fn batch_point_get_over_a_global_temporary_table_reads_only_session_rows() {
    let catalog = global_temp_catalog();
    assert_eq!(
        text_rows(&catalog, "select * from t1 where id in (1, 2, 3)"),
        vec![vec!["1".to_owned(), "1".to_owned()]],
    );
    assert_eq!(text_rows(&catalog, "select * from t1 where id in (2, 3)"), Vec::<Vec<String>>::new());
}

/// Go `pkg/executor/batch_point_get_test.go:224-226::TestPointGetForTemporaryTable`,
/// plan half: `explain format = 'brief' select * from t1 where id in (1, 2, 3)`
/// plans `Batch_Point_Get` over the temporary table (`handle:[1 2 3]`).
#[test]
fn explain_of_a_global_temporary_table_batch_get_names_batch_point_get() {
    let catalog = global_temp_catalog();
    let ctx = ctx();
    let sql = "select * from t1 where id in (1, 2, 3)";
    let Stmt::Query(query) = tidb_parser::parse(sql).expect("parses") else {
        panic!("a query");
    };
    let tidb_ast::QueryStmt::Select(select) = &*query else {
        panic!("a SELECT");
    };
    let (_, rows) = crate::explain::explain_select_stmt(
        select.as_ref(),
        &catalog,
        crate::DEFAULT_DATABASE,
        &ctx,
        crate::explain::ExplainFormat::Brief,
    )
    .expect("the batch point get over a temporary table explains");
    let operators = rows
        .iter()
        .filter_map(|row| match &row[0] {
            Datum::Bytes(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
            Datum::String(text) => Some(String::from_utf8_lossy(text.bytes()).into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        operators.iter().any(|name| name.contains("Batch_Point_Get")),
        "expected a Batch_Point_Get operator, got {operators:?}"
    );
}

/// Go `pkg/executor/batch_point_get_test.go:204::TestPointGetForTemporaryTable`,
/// point get half: the single-row point get sees the same session rows.
#[test]
fn point_get_over_a_global_temporary_table_reads_only_session_rows() {
    let catalog = global_temp_catalog();
    assert_eq!(
        text_rows(&catalog, "select * from t1 where id = 1"),
        vec![vec!["1".to_owned(), "1".to_owned()]],
    );
    assert_eq!(text_rows(&catalog, "select * from t1 where id = 2"), Vec::<Vec<String>>::new());
}
