//! CASE in an UPDATE: each row's own value drives the branch — a row
//! assigned to itself keeps its value (unchanged), others take the new
//! value, all in one statement.

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
    session.run("insert into t values (1, 1), (2, 2), (3, 3)").unwrap();
}

#[test]
fn case_driven_update_sets_each_row() {
    let mut session = Session::new();
    setup(&mut session);

    session
        .run("update t set b = case a when 1 then 100 when 2 then b when 3 then 300 end")
        .unwrap();

    // Row 2's branch returns its own value: unchanged. Rows 1 and 3 update.
    assert_eq!(rows(&mut session, "select a, b from t order by a"), "1|100;2|2;3|300");
}
