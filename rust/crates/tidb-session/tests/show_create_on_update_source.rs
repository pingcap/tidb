//! SHOW CREATE for an `ON UPDATE CURRENT_TIMESTAMP` column: the clause
//! round-trips in the DDL output.

use tidb_session::Session;

use crate::support::byte_lines as strings;

#[test]
fn on_update_clause_round_trips() {
    let mut session = Session::new();
    session
        .run(
            "create table t (id int primary key, updated timestamp \
             default current_timestamp on update current_timestamp)",
        )
        .unwrap();

    let shown = strings(&mut session, "show create table t");
    assert!(
        shown.contains("ON UPDATE CURRENT_TIMESTAMP"),
        "{shown}"
    );
}
