//! Commit persistence: both `START TRANSACTION ... COMMIT` and
//! `BEGIN ... COMMIT` keep the transaction's writes — the row survives the
//! transaction boundary.

use tidb_session::Session;

use crate::support::integer_rows_with_sql as rows;

#[test]
fn committed_writes_persist() {
    let mut session = Session::new();
    session.run("create table u (a int primary key)").unwrap();

    session.run("start transaction").unwrap();
    session.run("insert into u values (1)").unwrap();
    session.run("commit").unwrap();
    assert_eq!(rows(&mut session, "select a from u"), "1");

    session.run("begin").unwrap();
    session.run("insert into u values (2)").unwrap();
    session.run("commit").unwrap();
    assert_eq!(rows(&mut session, "select a from u order by a"), "1;2");
}
