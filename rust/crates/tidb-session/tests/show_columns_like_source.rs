//! `SHOW COLUMNS FROM t LIKE 'pattern'`: the LIKE filter restricts the
//! listed columns — both matching columns answer, the non-matching one is
//! omitted.

use tidb_session::Session;

use crate::support::byte_rows as strings;

#[test]
fn like_filter_restricts_show_columns() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, name varchar(8), name2 varchar(8))")
        .unwrap();

    let filtered = strings(&mut session, "show columns from t like 'name%'");
    assert_eq!(filtered.len(), 2, "only the two name columns match");
    assert!(filtered[0].starts_with("name|"), "{filtered:?}");
    assert!(filtered[1].starts_with("name2|"), "{filtered:?}");

    // Unfiltered: all three columns.
    assert_eq!(strings(&mut session, "show columns from t").len(), 3);
}
