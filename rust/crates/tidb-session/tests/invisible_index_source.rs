//! Invisible indexes: `CREATE INDEX idx (k) invisible` reports Visible=NO in
//! SHOW INDEX, and `ALTER TABLE ... ALTER INDEX idx visible` flips it to YES
//! (Go `AlterIndexVisibility`).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
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
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn visibility(session: &mut Session) -> String {
    let index_rows = rows(session, "show index from t");
    let row = index_rows
        .split(';')
        .find(|row| row.contains("|idx|"))
        .expect("idx listed");
    row.split('|').nth(13).expect("visible field").to_owned()
}

#[test]
fn index_visibility_flips() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, k int, index idx (k) invisible)")
        .unwrap();

    assert_eq!(visibility(&mut session), "NO");

    session
        .run("alter table t alter index idx visible")
        .unwrap();
    assert_eq!(visibility(&mut session), "YES");
}
