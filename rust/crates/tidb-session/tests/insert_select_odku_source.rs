//! `INSERT ... SELECT ... ON DUPLICATE KEY UPDATE` where the UPDATE branch
//! references SOURCE-table columns (`t.v = src.v`): Go resolves an ODKU
//! assignment column against the target tables first and falls back to the
//! source's output row — so `src.v` reads the row the insert would have
//! written, exactly like `VALUES(src.v)`.

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

#[test]
fn source_columns_drive_the_update_branch() {
    let mut session = Session::new();
    session.run("create table src (a int primary key, v int)").unwrap();
    session.run("insert into src values (1, 100), (2, 200)").unwrap();
    session.run("create table t (a int primary key, v int)").unwrap();
    session.run("insert into t values (1, 0), (3, 0)").unwrap();

    let affected = match session
        .run("insert into t (a, v) select a, v from src on duplicate key update t.v = src.v")
        .unwrap()
    {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(affected, 3, "one insert plus two per updated row");

    assert_eq!(
        rows(&mut session, "select a, v from t order by a"),
        "1|100;2|200;3|0",
        "the dup takes src.v, the new row inserts, row 3 is untouched"
    );
}
