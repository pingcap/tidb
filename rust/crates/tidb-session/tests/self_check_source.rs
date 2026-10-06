//! A self-referencing CHECK (`check (a < a * 0)` — always false for any
//! non-NULL): only SQL NULL inserts, since CHECK treats UNKNOWN as pass.

use tidb_session::Session;

use crate::support::tagged_nullable_integer_rows as rows;

use crate::support::execute as try_sql;

fn setup(session: &mut Session) {
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run("create table t (a int, check (a < a * 0))")
        .unwrap();
}

#[test]
fn self_referencing_check_blocks_every_value() {
    let mut session = Session::new();
    setup(&mut session);

    // NULL passes (CHECK UNKNOWN = pass).
    assert_eq!(try_sql(&mut session, "insert into t values (null)"), "affected 1");

    // Every value >= 0 violates `a < a * 0` (i.e. `a < 0`); -1 would pass.
    for value in ["0", "1", "5"] {
        let error = try_sql(&mut session, &format!("insert into t values ({value})"));
        assert!(error.contains("is violated"), "{value}: {error}");
    }

    // Only the NULL row landed.
    assert_eq!(rows(&mut session, "select a from t"), "Null");
}
