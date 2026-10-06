//! SHOW CREATE TABLE carries CHECK definitions with the expression in Go's
//! restore form: `CONSTRAINT `pos_b` CHECK ((`a` + `b` > 0))` — the CHECK
//! parens plus the comparison, whose `+` operand needs no extra parens.

use tidb_session::Session;

use crate::support::byte_lines as strings;

fn setup(session: &mut Session) {
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run("create table t (a int, b int, constraint pos_b check (a + b > 0))")
        .unwrap();
}

#[test]
fn show_create_carries_the_check_definition() {
    let mut session = Session::new();
    setup(&mut session);

    let shown = strings(&mut session, "show create table t");
    assert!(
        shown.contains("CONSTRAINT `pos_b` CHECK ((`a` + `b` > 0))"),
        "{shown}"
    );
}
