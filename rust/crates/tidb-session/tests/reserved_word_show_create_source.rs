//! SHOW CREATE backtick-quotes reserved-word column names (`order`,
//! `select`) — the DDL output remains re-parseable.

use tidb_session::Session;

use crate::support::byte_lines as strings;

#[test]
fn reserved_word_columns_backtick_quoted() {
    let mut session = Session::new();
    session.run("create table t (`order` int, `select` int)").unwrap();

    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("`order` int DEFAULT NULL"), "{shown}");
    assert!(shown.contains("`select` int DEFAULT NULL"), "{shown}");

    // The output is re-parseable: CREATE TABLE LIKE on the same name.
    session.run("create table t2 like t").unwrap();
}
