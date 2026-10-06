//! `SET autocommit = 0`: an INSERT opens a transaction that stays open
//! until an explicit COMMIT — the row is visible to the same session
//! immediately, persists across COMMIT, and a later ROLLBACK reverts the
//! next write.

use tidb_session::Session;

use crate::support::integer_rows_with_sql as rows;

#[test]
fn autocommit_off_requires_explicit_commit() {
    let mut session = Session::new();
    session.run("create table u (a int primary key)").unwrap();
    session.run("set autocommit = 0").unwrap();

    session.run("insert into u values (1)").unwrap();
    assert_eq!(rows(&mut session, "select a from u"), "1", "visible in its own txn");
    session.run("commit").unwrap();
    assert_eq!(rows(&mut session, "select a from u"), "1", "persists after COMMIT");

    session.run("insert into u values (2)").unwrap();
    session.run("rollback").unwrap();
    assert_eq!(rows(&mut session, "select a from u"), "1", "the uncommitted insert reverts");
}
