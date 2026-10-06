//! `ALTER TABLE ... RENAME INDEX uq TO uq_renamed`: SHOW INDEX reports the
//! new name (Key_name `uq_renamed`, Non_unique 0) and uniqueness survives —
//! the dup insert refuses against the renamed key.

use tidb_session::Session;

use crate::support::byte_rows_joined as rows;

fn error(session: &mut Session, sql: &str) -> String {
    session.run(sql).expect_err(sql).to_string()
}

#[test]
fn renamed_index_keeps_uniqueness() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, k int, unique index uq (k))")
        .unwrap();

    session
        .run("alter table t rename index uq to uq_renamed")
        .unwrap();

    let index_rows = rows(&mut session, "show index from t");
    let renamed = index_rows
        .split(';')
        .find(|row| row.contains("|uq_renamed|"))
        .expect("renamed index listed");
    assert!(renamed.contains("|Int(0)|"), "Non_unique 0: {renamed}");

    session.run("insert into t values (1, 7)").unwrap();
    assert_eq!(
        error(&mut session, "insert into t values (2, 7)"),
        "Duplicate entry '7' for key 't.uq_renamed'"
    );
}
