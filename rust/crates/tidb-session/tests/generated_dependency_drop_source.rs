//! DROP COLUMN referenced by a generated column: refused with Go's
//! "Column 'a' has a generated column dependency."

use tidb_session::Session;

fn error(session: &mut Session, sql: &str) -> String {
    session.run(sql).expect_err(sql).to_string()
}

#[test]
fn generated_base_column_cannot_be_dropped() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b int as (a + 1) stored)")
        .unwrap();

    assert_eq!(
        error(&mut session, "alter table t drop column a"),
        "Column 'a' has a generated column dependency."
    );
}
