//! CHECK constraints in the write path: `tidb_enable_check_constraint` gates
//! ATTACHMENT at CREATE time (OFF drops the constraint with one warning,
//! create_table.go:1470); once attached, a violating INSERT fails with Go's
//! `ErrCheckConstraintViolated` (3819, "Check constraint '<name>' is
//! violated.") and conforming rows pass.

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, Catalog, CreateTableSettings,
    StmtContext,
};

fn setup(checks_on: bool) -> Catalog {
    let mut catalog = Catalog::default();
    let settings = CreateTableSettings {
        enable_check_constraint: checks_on,
        ..Default::default()
    };
    ddl::run_create_table_in(
        "create table t (a int check (a > 0))",
        &mut catalog,
        "test",
        settings,
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

#[test]
fn attached_check_rejects_violating_inserts_with_3819() {
    let mut catalog = setup(true);
    let ctx = StmtContext::for_dml(false, true, false);
    let error = run_insert_on("insert into t values (-5)", &mut catalog, &ctx)
        .expect_err("the violating row must fail");
    assert_eq!(
        error.to_string(),
        "Check constraint 't_chk_1' is violated.",
        "Go: ErrCheckConstraintViolated (3819)"
    );
    // A conforming row still passes.
    run_insert_on("insert into t values (3)", &mut catalog, &ctx).expect("TRUE passes CHECK");
}

#[test]
fn checks_off_drops_the_constraint_at_create() {
    let mut catalog = setup(false);
    let ctx = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into t values (-5)", &mut catalog, &ctx)
        .expect("with the variable OFF the constraint never attached");
}
