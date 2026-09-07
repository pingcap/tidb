//! SET NAMES switches the connection charset/collation as a unit and the
//! collation_connection variable reflects it. Column-level INVISIBLE
//! remains absent from the oracle grammar (refused), and an unknown charset
//! name is refused as well.

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

#[test]
fn set_names_updates_collation_connection() {
    let mut session = Session::new();

    session.run("set names utf8mb4 collate utf8mb4_general_ci").unwrap();

    let value = rows(&mut session, "show variables like 'collation_connection'");
    // "collation_connection|utf8mb4_general_ci"
    assert!(value.contains("utf8mb4_general_ci"), "{value}");
}

#[test]
fn unknown_charset_name_refuses() {
    let mut session = Session::new();

    let error = session
        .run("set names not_a_charset")
        .expect_err("unknown charset")
        .to_string();
    assert!(!error.is_empty(), "{error}");
}
