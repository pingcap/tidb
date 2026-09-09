//! Explicit writes to a generated column are refused with Go's
//! ErrBadGeneratedColumn text — both INSERT and UPDATE.

use tidb_session::Session;

fn error(session: &mut Session, sql: &str) -> String {
    session.run(sql).expect_err(sql).to_string()
}

#[test]
fn generated_column_writes_refused() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b int as (a * 2) stored)")
        .unwrap();

    let want = "The value specified for generated column 'b' in table 't' is not allowed.";
    assert_eq!(error(&mut session, "insert into t (a, b) values (3, 9)"), want);
    assert_eq!(error(&mut session, "update t set b = 10 where a = 3"), want);
}
