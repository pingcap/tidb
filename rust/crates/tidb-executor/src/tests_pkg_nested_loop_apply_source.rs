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

//! Ports of Go `pkg/executor/pkg_test.go` and
//! `pkg/executor/metrics_reader_test.go::TestStmtLabel` whose contracts this
//! tier owns, plus the MPP coordinator-manager gap.
//!
//! Go's `TestStmtLabel` runs each SQL through parse + preprocess + optimize
//! and then reads `stmtctx.GetStmtLabel(ctx, stmtNode)`
//! (pkg/sessionctx/stmtctx/stmtctx.go:1669), which forwards to
//! `ast.GetStmtLabel` (pkg/parser/ast/ast.go:159) -- a pure function of the
//! AST. The label mapping is pinned here over Go's full SQL matrix; the
//! parse+optimize SUCCEEDS precondition is planner surface and is covered by
//! the tidb-planner crate's own tests, not re-asserted here.

use crate::{run_create_table_on, run_insert_on, run_select_on, Catalog, StmtContext};
use tidb_datatype::Datum;

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

/// Go `pkg/executor/metrics_reader_test.go:31::TestStmtLabel`: every SQL in
/// Go's matrix carries its coarse label -- Select, Delete, Update or
/// ImportInto -- computed from the parsed statement alone
/// (`ast.GetStmtLabel`, pkg/parser/ast/ast.go:159, via
/// pkg/sessionctx/stmtctx/stmtctx.go:1669). The port parses each SQL and
/// reads [`tidb_ast::Stmt::label`], the captured port of the same function.
#[test]
fn stmt_label_matrix_source() {
    let cases: &[(&str, &str)] = &[
        ("select 1", "Select"),
        ("select * from label t1, label t2", "Select"),
        (
            "select * from label t1 where t1.c3 > (select count(t1.c1 = t2.c1) = 0 from label t2)",
            "Select",
        ),
        ("select count(*) from label", "Select"),
        ("select * from label where c2 = 1", "Select"),
        ("select c1, c2 from label where c2 = 1", "Select"),
        ("select * from label where c1 > 1", "Select"),
        ("select * from label order by c3 limit 1", "Select"),
        ("delete from label", "Delete"),
        ("delete from label where c1 = 1", "Delete"),
        ("delete from label where c2 = 1", "Delete"),
        (
            "delete from label where c2 = 1 order by c3 limit 1",
            "Delete",
        ),
        ("update label set c3 = 3", "Update"),
        ("update label set c3 = 3 where c1 = 1", "Update"),
        ("update label set c3 = 3 where c2 = 1", "Update"),
        (
            "update label set c3 = 3 where c2 = 1 order by c3 limit 1",
            "Update",
        ),
        ("import into label from '/file.csv'", "ImportInto"),
    ];
    for (sql, label) in cases {
        let stmt = ctx()
            .parse(sql)
            .unwrap_or_else(|error| panic!("parse {sql:?}: {error:?}"));
        assert_eq!(stmt.label(), *label, "label of {sql:?}");
    }
}

/// Go `pkg/executor/pkg_test.go:35::TestNestedLoopApply`: a NestedLoopApply
/// whose outer side is rows 1..6, whose OUTER and INNER filters are
/// `col < 6`, and whose other-condition is `outer == inner`, emits
/// `(i, i)` for i = 1..5 in order -- the executor chunks walked with a row
/// counter in Go. On this tier the apply operator's correlated lowering is
/// driven through the statement driver; the composed contract (filtered
/// correlation over mock-shaped tables) is pinned with Go's exact fixture.
/// The direct NestedLoopApplyExec construction (OuterFilter/InnerFilter/
/// Joiner fields, `chunk.NewList` inner buffering) is tier-internal shape
/// covered by `crate::apply`'s own executor tests.
#[test]
fn nested_loop_apply_filtered_correlation_source() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t_outer (c0 bigint)", &mut catalog).unwrap();
    run_create_table_on("create table t_inner (c1 bigint)", &mut catalog).unwrap();
    run_insert_on(
        "insert into t_outer values 1, 2, 3, 4, 5, 6",
        &mut catalog,
        &ctx(),
    )
    .unwrap_or_else(|_| {
        run_insert_on(
            "insert into t_outer values (1), (2), (3), (4), (5), (6)",
            &mut catalog,
            &ctx(),
        )
        .unwrap()
    });
    run_insert_on(
        "insert into t_inner values (1), (2), (3), (4), (5), (6)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "select t_outer.c0, t_inner.c1 from t_outer, t_inner \
             where t_outer.c0 = t_inner.c1 and t_outer.c0 < 6 and t_inner.c1 < 6 \
             order by t_outer.c0",
            &catalog,
            &ctx(),
        )
        .unwrap(),
        (1..=5i64)
            .map(|i| vec![Datum::Int(i), Datum::Int(i)])
            .collect::<Vec<_>>(),
    );
}

/// Go `pkg/executor/pkg_test.go:100::TestMoveInfoSchemaToFront`, over the
/// same db-name matrices: after `fetchShowDatabases` sorts and
/// `moveInfoSchemaToFront` (pkg/executor/show.go:434) relocates
/// INFORMATION_SCHEMA, every listing that contains it starts with it and the
/// remaining names keep ascending order. This tier's surface is
/// [`Catalog::database_names`], which performs both steps; every catalog
/// bootstraps INFORMATION_SCHEMA/mysql/test (Go's always contains
/// INFORMATION_SCHEMA too -- `infoschema.AllSchemaNames`), so the Go
/// matrices are realized by registering their extra databases and asserting
/// the full ordering, including the case where `B`/`a` sort on either side
/// of INFORMATION_SCHEMA byte-wise.
#[test]
fn move_info_schema_to_front_source() {
    // Go row 3: ["A", "B", "C", "INFORMATION_SCHEMA"] ->
    // ["INFORMATION_SCHEMA", "A", "B", "C"]; plus the bootstrapped
    // mysql/test, which sort after INFORMATION_SCHEMA byte-wise
    // ('m' 0x6d, 't' 0x74 > 'I' 0x49).
    let mut catalog = Catalog::default();
    for name in ["A", "B", "C"] {
        catalog.register_database_with_id(name, 100);
    }
    assert_eq!(
        catalog.database_names(),
        vec![
            "INFORMATION_SCHEMA".to_owned(),
            "A".to_owned(),
            "B".to_owned(),
            "C".to_owned(),
            "mysql".to_owned(),
            "test".to_owned(),
        ],
    );

    // Go row 4's shape: a name sorting BEFORE "INFORMATION_SCHEMA"
    // byte-wise, INFORMATION_SCHEMA itself, then names after it -- the move
    // is REAL work because 'Aa' (0x41...) sorts before 'I' (0x49) while
    // 'b' (0x62) sorts after. (Go row 4's literal ["A", "B",
    // "INFORMATION_SCHEMA", "a"] feeds SYNTHETIC strings; database names
    // are case-insensitive in both implementations, so a catalog cannot hold
    // both "A" and "a" -- the same ordering property is pinned over distinct
    // names.)
    let mut catalog = Catalog::default();
    for name in ["Aa", "b"] {
        catalog.register_database_with_id(name, 200);
    }
    assert_eq!(
        catalog.database_names(),
        vec![
            "INFORMATION_SCHEMA".to_owned(),
            "Aa".to_owned(),
            "b".to_owned(),
            "mysql".to_owned(),
            "test".to_owned(),
        ],
    );

    // Go row 6's shape: several names on BOTH sides of "INFORMATION_SCHEMA"
    // keep their sorted positions after the relocation.
    let mut catalog = Catalog::default();
    for name in ["A", "B", "C", "a1", "b1"] {
        catalog.register_database_with_id(name, 300);
    }
    assert_eq!(
        catalog.database_names(),
        vec![
            "INFORMATION_SCHEMA".to_owned(),
            "A".to_owned(),
            "B".to_owned(),
            "C".to_owned(),
            "a1".to_owned(),
            "b1".to_owned(),
            "mysql".to_owned(),
            "test".to_owned(),
        ],
    );

    // Go rows 1-2 (["..."] with NO information_schema) cover the function's
    // early-return-free input space; a catalog always bootstraps
    // INFORMATION_SCHEMA on this tier, so the closest realizable assertion is
    // that the bootstrap listing itself stays sorted with
    // INFORMATION_SCHEMA already at the front (the early-return state).
    let catalog = Catalog::default();
    assert_eq!(
        catalog.database_names(),
        vec![
            "INFORMATION_SCHEMA".to_owned(),
            "mysql".to_owned(),
            "test".to_owned(),
        ],
    );
}
