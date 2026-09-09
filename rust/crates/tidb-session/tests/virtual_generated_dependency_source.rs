//! DROP COLUMN referenced by a VIRTUAL generated column: refused with Go's
//! exact ErrDependentByGeneratedColumn text — the guard covers both stored
//! and virtual variants.

use tidb_session::Session;

fn error(session: &mut Session, sql: &str) -> String {
    session.run(sql).expect_err(sql).to_string()
}

#[test]
fn virtual_generated_base_column_cannot_be_dropped() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b int as (a * 2) virtual)")
        .unwrap();

    assert_eq!(
        error(&mut session, "alter table t drop column a"),
        "Column 'a' has a generated column dependency."
    );
}
