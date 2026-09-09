//! Statement atomicity for UPDATE: Go runs the write phase in one
//! transaction, so a CHECK violation on a LATER row rolls back the EARLIER
//! rows this statement already rewrote. The staged rewrites carry each
//! row's pre-image, which replays in reverse on failure.

use tidb_executor::{
    ddl, run_create_table_on, run_insert_on, run_select_on, run_update_on, Catalog,
    CreateTableSettings, StmtContext,
};

#[test]
fn check_failure_on_a_later_row_rolls_back_earlier_updates() {
    let mut catalog = Catalog::default();
    let settings = CreateTableSettings {
        enable_check_constraint: true,
        ..Default::default()
    };
    ddl::run_create_table_in(
        "create table t (a int primary key, b int check (b > 0))",
        &mut catalog,
        "test",
        settings,
        &StmtContext::for_query(),
    )
    .unwrap();
    let strict = StmtContext::for_dml(false, true, false);
    run_insert_on("insert into t values (1, 1), (2, 2)", &mut catalog, &strict).unwrap();

    // The first assignment succeeds, the second violates the CHECK.
    let error = run_update_on(
        "update t set b = case a when 1 then 100 when 2 then -200 end",
        &mut catalog,
        &strict,
    )
    .expect_err("the violating row must fail the statement");
    assert_eq!(error.to_string(), "Check constraint 't_chk_1' is violated.");

    // Rolled back: NEITHER row changed.
    let rows = run_select_on("select a, b from t", &catalog, &strict).unwrap();
    assert_eq!(format!("{:?}", rows), "[[Int(1), Int(1)], [Int(2), Int(2)]]");
}
