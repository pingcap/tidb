//! Invisible indexes: `CREATE INDEX idx (k) invisible` reports Visible=NO in
//! SHOW INDEX, and `ALTER TABLE ... ALTER INDEX idx visible` flips it to YES
//! (Go `AlterIndexVisibility`).

use tidb_session::Session;

use crate::support::byte_rows_joined as rows;

fn visibility(session: &mut Session) -> String {
    let index_rows = rows(session, "show index from t");
    let row = index_rows
        .split(';')
        .find(|row| row.contains("|idx|"))
        .expect("idx listed");
    row.split('|').nth(13).expect("visible field").to_owned()
}

#[test]
fn index_visibility_flips() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, k int, index idx (k) invisible)")
        .unwrap();

    assert_eq!(visibility(&mut session), "NO");

    session
        .run("alter table t alter index idx visible")
        .unwrap();
    assert_eq!(visibility(&mut session), "YES");

    // And back: a plain index goes invisible.
    session
        .run("alter table t alter index idx invisible")
        .unwrap();
    assert_eq!(visibility(&mut session), "NO");

    // Column-level INVISIBLE is NOT in the oracle grammar — refused.
    let error = session
        .run("alter table t add column ghost int invisible")
        .expect_err("no column-level invisible")
        .to_string();
    assert!(
        error.contains("check the manual that corresponds"),
        "{error}"
    );
}
