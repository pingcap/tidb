//! CREATE VIEW with an explicit column list: the exposed columns carry the
//! aliases (`key_a`, `label`) and SHOW CREATE VIEW round-trips the full
//! definition including the column list.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::String(s) => {
                            format!("'{}'", String::from_utf8_lossy(&s.bytes()))
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
                        tidb_datatype::Datum::String(s) => {
                            format!("'{}'", String::from_utf8_lossy(&s.bytes()))
                        }
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn aliased_view_columns_and_show_create_view() {
    let mut session = Session::new();
    session
        .run("create table base_t (a int primary key, b varchar(4))")
        .unwrap();
    session.run("insert into base_t values (1, 'x')").unwrap();

    session
        .run("create view v (key_a, label) as select a, b from base_t")
        .unwrap();

    // The aliases expose the base columns.
    assert_eq!(rows(&mut session, "select key_a, label from v"), "1|'x'");

    // SHOW CREATE VIEW round-trips the column list.
    let shown = strings(&mut session, "show create view v");
    assert!(
        shown.iter().any(|row| row.contains("VIEW `v` (`key_a`, `label`)")),
        "{shown:?}"
    );
}
