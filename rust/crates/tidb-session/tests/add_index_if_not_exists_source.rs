//! `ALTER TABLE ... ADD INDEX IF NOT EXISTS`: the first run creates the
//! index and the second run is a no-op (Go suppresses the duplicate with a
//! Note, matching `IF NOT EXISTS` semantics).

use tidb_session::Session;

use crate::support::byte_rows as strings;

#[test]
fn add_index_if_not_exists_is_idempotent() {
    let mut session = Session::new();
    session.run("create table t (a int primary key, b int)").unwrap();

    session
        .run("alter table t add index if not exists kb (b)")
        .unwrap();
    session
        .run("alter table t add index if not exists kb (b)")
        .unwrap();

    // Exactly one `kb` entry exists on the table.
    let shown = strings(&mut session, "show index from t");
    assert_eq!(
        shown.iter().filter(|row| row.contains("kb")).count(),
        1,
        "{shown:?}"
    );
}
