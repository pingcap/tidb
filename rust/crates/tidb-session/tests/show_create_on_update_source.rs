//! SHOW CREATE for an `ON UPDATE CURRENT_TIMESTAMP` column: the clause
//! round-trips in the DDL output.

use tidb_session::Session;

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
fn on_update_clause_round_trips() {
    let mut session = Session::new();
    session
        .run(
            "create table t (id int primary key, updated timestamp \
             default current_timestamp on update current_timestamp)",
        )
        .unwrap();

    let shown = strings(&mut session, "show create table t");
    assert!(
        shown.contains("ON UPDATE CURRENT_TIMESTAMP"),
        "{shown}"
    );
}
