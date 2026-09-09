//! `ALTER TABLE ... MODIFY COLUMN v int DEFAULT 9` replaces the column's
//! default: pre-existing rows keep their values, new omissions take the new
//! default, and SHOW CREATE prints the new one (Go `ModifyColumn` ->
//! `SetDefaultValue`).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| format!("{d:?}"))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn strings(session: &mut Session, sql: &str) -> String {
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
                    .join("\n")
            })
            .collect::<Vec<_>>()
            .join("\n"),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn modify_replaces_column_default() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, v int default 1)")
        .unwrap();

    session.run("insert into t (id) values (1)").unwrap();
    session.run("alter table t modify column v int default 9").unwrap();
    session.run("insert into t (id) values (2)").unwrap();

    // Row 1 keeps its stored 1; row 2 takes the NEW default 9.
    assert_eq!(
        rows(&mut session, "select id, v from t order by id"),
        "Int(1)|Int(1);Int(2)|Int(9)"
    );
    assert!(strings(&mut session, "show create table t").contains("DEFAULT '9'"));
}
