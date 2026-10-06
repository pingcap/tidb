//! SEQUENCE lifecycle: `CREATE SEQUENCE ... START 10 INCREMENT 5` hands out
//! 10, 15, 20 through `NEXTVAL(seq)`, and DROP SEQUENCE removes it.

use tidb_session::Session;

use crate::support::signed_unsigned_rows_with_sql as rows;

#[test]
fn sequence_allocates_by_increment_and_drops() {
    let mut session = Session::new();
    session
        .run("create sequence seq start 10 increment 5")
        .unwrap();

    assert_eq!(rows(&mut session, "select nextval(seq)"), "10");
    assert_eq!(rows(&mut session, "select nextval(seq)"), "15");
    assert_eq!(rows(&mut session, "select nextval(seq)"), "20");

    session.run("drop sequence seq").unwrap();
    let error = session
        .run("select nextval(seq)")
        .expect_err("the sequence is gone");
    assert!(error.to_string().contains("seq"), "{error}");
}
