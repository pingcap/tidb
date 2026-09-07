//! `CREATE INDEX IF NOT EXISTS`: the first run creates the index and the
//! second run is a no-op.

use tidb_session::Session;

fn strings(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(bytes) => {
                            String::from_utf8_lossy(bytes).into_owned()
                        }
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn create_index_if_not_exists_is_idempotent() {
    let mut session = Session::new();
    session.run("create table t (a int primary key, b int)").unwrap();

    session.run("create index if not exists kb on t (b)").unwrap();
    session.run("create index if not exists kb on t (b)").unwrap();

    let shown = strings(&mut session, "show index from t");
    assert_eq!(
        shown.iter().filter(|row| row.contains("kb")).count(),
        1,
        "{shown:?}"
    );
}
