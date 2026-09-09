//! User variables in UPDATE SET: `SET @x = 77` then `UPDATE ... SET b =
//! @x` binds the session value at execution — the assigned value applies
//! to the matched row, and an unset variable propagates NULL.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::Null => "NULL".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows, got {other:?}"),
    }
}

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
