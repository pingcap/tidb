//! User variables in UPDATE SET: `SET @x = 77` then `UPDATE ... SET b =
//! @x` binds the session value at execution — the assigned value applies
//! to the matched row, and an unset variable propagates NULL.

use tidb_session::Session;

use crate::support::nullable_integer_rows as rows;

#[test]
fn uservar_assigns_in_update_set() {
    let mut session = Session::new();
    session.run("create table t (a int primary key, b int)").unwrap();
    session.run("insert into t values (1, 10), (2, 20)").unwrap();

    session.run("set @x = 77").unwrap();
    let changed = match session.run("update t set b = @x where a = 1").unwrap() {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(changed, 1);
    assert_eq!(rows(&mut session, "select a, b from t order by a"), "1|77;2|20");

    // An unset variable is NULL: the column takes NULL (b is nullable).
    let changed = match session.run("update t set b = @missing where a = 1").unwrap() {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(changed, 1);
    assert_eq!(rows(&mut session, "select a, b from t order by a"), "1|NULL;2|20");
}
