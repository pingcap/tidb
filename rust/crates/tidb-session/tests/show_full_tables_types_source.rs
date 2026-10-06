//! SHOW FULL TABLES types each object per Go's `getTableType`
//! (`executor/show.go:519-528`): a view is VIEW, a sequence is SEQUENCE,
//! a table is BASE TABLE — all three in one listing.

use tidb_session::Session;

use crate::support::byte_rows as strings;

#[test]
fn full_tables_types_all_three_kinds() {
    let mut session = Session::new();
    session.run("create table t (a int)").unwrap();
    session.run("create sequence s").unwrap();
    session.run("create view v as select a from t").unwrap();

    assert_eq!(
        strings(&mut session, "show full tables"),
        vec![
            "s|SEQUENCE".to_owned(),
            "t|BASE TABLE".to_owned(),
            "v|VIEW".to_owned()
        ]
    );
}
