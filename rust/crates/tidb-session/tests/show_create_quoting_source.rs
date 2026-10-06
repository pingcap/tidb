//! SHOW CREATE TABLE identifier quoting: mixed-case and reserved-word
//! column names round-trip backquoted (`Key`, `select`), matching Go's
//! `constructResultOfShowCreateTable` quoting rule.

use tidb_session::Session;

use crate::support::byte_lines as strings;

#[test]
fn reserved_and_mixed_case_names_stay_backquoted() {
    let mut session = Session::new();
    session
        .run("create table t (`Key` int primary key, `select` int)")
        .unwrap();

    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("`Key`"), "{shown}");
    assert!(shown.contains("`select`"), "{shown}");
}
