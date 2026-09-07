//! The auto-increment -> LAST_INSERT_ID() statement flow: an ALLOCATED id is
//! published (`e.lastInsertID`, first-allocated wins) and becomes what
//! `LAST_INSERT_ID()` reads after the statement boundary; an EXPLICIT value
//! only feeds the OK packet's fallback (`StmtCtx.InsertID`) and never moves
//! the function (insert_common.go:1011, :1482).

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, Catalog, CreateTableSettings,
    StmtContext,
};

/// Go `ResetContextOfStmt`: what the preceding statement published becomes
/// `PrevLastInsertID`, which `LAST_INSERT_ID()` reads (builtin_info.go:487).
fn boundary(insert_ctx: &StmtContext, prev: u64) -> StmtContext {
    StmtContext::for_query()
        .with_previous_statement(insert_ctx.published_last_insert_id().unwrap_or(prev), -1)
}

fn last_id(catalog: &Catalog, q: &StmtContext) -> u64 {
    match run_select_on("select last_insert_id()", catalog, q).unwrap()[0][0] {
        tidb_datatype::Datum::UInt(id) => id,
        ref other => panic!("expected a UInt id, got {other:?}"),
    }
}

#[test]
fn allocated_ids_flow_to_last_insert_id() {
    let mut catalog = Catalog::default();
    ddl::run_create_table_in(
        "create table t (a int auto_increment primary key, b varchar(3))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();

    // INSERT (b) VALUES ('x'): id 1 is allocated and published.
    let stmt = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into t (b) values ('x')", &mut catalog, &stmt).unwrap();
    let mut prev = stmt.published_last_insert_id().expect("allocated id published");
    assert_eq!(prev, 1);

    // INSERT (a, b) VALUES (5, 'y'): the EXPLICIT value must NOT move the
    // function -- the next read still answers the previous allocation.
    let stmt = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into t (a, b) values (5, 'y')", &mut catalog, &stmt).unwrap();
    let q = boundary(&stmt, prev);
    prev = q.published_last_insert_id().unwrap_or(prev);
    assert_eq!(last_id(&catalog, &q), 1, "an explicit id never moves LAST_INSERT_ID()");

    // INSERT (b) VALUES ('z'): the NEXT allocation (6) is published.
    let stmt = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into t (b) values ('z')", &mut catalog, &stmt).unwrap();
    let q = boundary(&stmt, prev);
    assert_eq!(last_id(&catalog, &q), 6);
}
