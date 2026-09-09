//! Mixed target/source references in one ODKU assignment (`t.v = t.v +
//! src.d`): the qualified target ref reads the STORED row (7), the source
//! ref reads the candidate (40), and the sum is stored — 47.

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
fn target_and_source_refs_compose() {
    let mut session = Session::new();
    session.run("create table src (a int primary key, d int)").unwrap();
    session.run("insert into src values (1, 40)").unwrap();
    session.run("create table t (a int primary key, v int)").unwrap();
    session.run("insert into t values (1, 7)").unwrap();

    let affected = match session
        .run("insert into t (a, v) select a, d from src on duplicate key update t.v = t.v + src.d")
        .unwrap()
    {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(affected, 2, "one dup-key update");

    assert_eq!(rows(&mut session, "select a, v from t"), "1|47");
}
