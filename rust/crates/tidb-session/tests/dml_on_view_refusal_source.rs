//! DML through a view is refused with Go's exact texts: INSERT/REPLACE
//! ("insert into view %s is not supported now", planbuilder.go:4123),
//! UPDATE (ErrNonUpdatableTable 1288, errname.go:296), DELETE ("delete view
//! %s is not supported now", logical_plan_builder.go:6626).

use tidb_session::Session;

fn error(session: &mut Session, sql: &str) -> String {
    session.run(sql).expect_err(sql).to_string()
}

#[test]
fn insert_update_delete_on_view_refuse() {
    let mut session = Session::new();
    session.run("create table t (a int primary key)").unwrap();
    session.run("create view v as select a from t").unwrap();

    assert_eq!(
        error(&mut session, "insert into v (a) values (1)"),
        "insert into view v is not supported now"
    );
    assert_eq!(
        error(&mut session, "update v set a = 5"),
        "The target table v of the UPDATE is not updatable"
    );
    assert_eq!(
        error(&mut session, "delete from v"),
        "delete view v is not supported now"
    );
}
