//! Two TiDB behaviors that differ from MySQL: a duplicated SET column
//! (`set b = 5, b = 6`) applies LAST-WIN (no 1110 refusal), and a
//! single-table `DELETE ... ORDER BY a` is accepted WITHOUT a LIMIT
//! (MySQL requires the LIMIT there; TiDB does not).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
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

fn setup(session: &mut Session) {
    session.run("create table t (a int primary key, b int)").unwrap();
    session.run("insert into t values (1, 1)").unwrap();
}

#[test]
fn duplicate_set_last_win_and_order_by_without_limit() {
    // Duplicate SET: last wins, no 1110 (TiDB accepts).
    let mut session = Session::new();
    setup(&mut session);
    let changed = match session.run("update t set b = 5, b = 6 where a = 1").unwrap() {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(changed, 1);
    assert_eq!(rows(&mut session, "select b from t"), "6");

    // DELETE with ORDER BY and no LIMIT: TiDB accepts (MySQL would refuse).
    let mut session = Session::new();
    setup(&mut session);
    let removed = match session.run("delete from t order by a").unwrap() {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(removed, 1);
    assert_eq!(rows(&mut session, "select a from t"), "");
}
