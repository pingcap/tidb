//! Multi-table RENAME: `rename table a to b, c to d` moves both tables in
//! one atomic statement — the old names vanish, the new names carry the
//! rows.

use tidb_session::Session;

use crate::support::tagged_integer_rows as rows;

use crate::support::execute as try_sql;

fn setup(session: &mut Session) {
    session.run("create table a (v int)").unwrap();
    session.run("create table c (v int)").unwrap();
    session.run("insert into a values (1)").unwrap();
    session.run("insert into c values (2)").unwrap();
}

#[test]
fn multi_rename_moves_both_tables() {
    let mut session = Session::new();
    setup(&mut session);

    assert_eq!(try_sql(&mut session, "rename table a to b, c to d"), "affected 0");

    assert_eq!(rows(&mut session, "select v from b"), "i:1");
    assert_eq!(rows(&mut session, "select v from d"), "i:2");

    // The old names are gone.
    let error = try_sql(&mut session, "select * from a limit 1");
    assert!(error.contains("doesn't exist"), "{error}");
}
