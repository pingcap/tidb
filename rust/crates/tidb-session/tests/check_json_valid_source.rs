//! CHECK (json_valid(j)) over a JSON column: the valid document inserts
//! (CHECK passes), and an invalid document refuses at the JSON CAST
//! (3140 "Invalid JSON text") before the CHECK can even see it.

use tidb_session::Session;

use crate::support::tagged_nullable_integer_rows as rows;

use crate::support::execute as try_sql;

fn setup(session: &mut Session) {
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run(
            "create table t (id int primary key, j json, check (json_valid(j)))",
        )
        .unwrap();
}

#[test]
fn json_valid_check_and_cast_order() {
    let mut session = Session::new();
    setup(&mut session);

    // A valid document lands and satisfies the CHECK.
    assert_eq!(try_sql(&mut session, "insert into t values (1, '{\"a\": 1}')"), "affected 1");

    // An invalid document refuses at the JSON CAST (3140) — the CHECK never
    // sees it because the value never becomes a JSON at all.
    let error = try_sql(&mut session, "insert into t values (2, 'nope')");
    assert!(error.contains("Invalid JSON text"), "{error}");

    // Only the valid row landed.
    assert_eq!(rows(&mut session, "select id from t"), "i:1");
}
