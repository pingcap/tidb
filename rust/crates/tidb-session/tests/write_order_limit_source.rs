//! ORDER BY + LIMIT on writes: DELETE removes the LAST rows first under
//! `order by a desc limit 2` (leaving a=1), and UPDATE doubles only the two
//! highest `a` rows under the same clause.

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
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

fn seed(session: &mut Session) {
    session.run("create table t (a int primary key, b int)").unwrap();
    session.run("insert into t values (1, 10), (2, 20), (3, 30)").unwrap();
}

#[test]
fn delete_removes_the_ordered_tail() {
    let mut session = Session::new();
    seed(&mut session);
    let removed = match session
        .run("delete from t order by a desc limit 2")
        .unwrap()
    {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(removed, 2);
    assert_eq!(rows(&mut session, "select a from t order by a"), "1");
}

#[test]
fn update_applies_to_the_ordered_head() {
    let mut session = Session::new();
    seed(&mut session);
    let changed = match session
        .run("update t set b = b * 10 order by a desc limit 2")
        .unwrap()
    {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(changed, 2);
    assert_eq!(
        rows(&mut session, "select a, b from t order by a"),
        "1|10;2|200;3|300",
        "rows 3 and 2 doubled; row 1 untouched"
    );
}
