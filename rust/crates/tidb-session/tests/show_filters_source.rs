//! SHOW statement filters: `LIKE 'pattern'` matches the name column,
//! `WHERE database = '...'` filters by value, and `SHOW COLLATION WHERE
//! charset = ...` restricts to the charset's collations.

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
                        tidb_datatype::Datum::String(s) => {
                            String::from_utf8_lossy(&s.bytes()).into_owned()
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

fn setup(session: &mut Session) {
    session.run("create database zzz_alpha").unwrap();
    session.run("create database zzz_beta").unwrap();
    session.run("create database other_db").unwrap();
}

#[test]
fn show_filters() {
    let mut session = Session::new();
    setup(&mut session);

    // LIKE matches the name column.
    let matched = strings(&mut session, "show databases like 'zzz%'");
    assert_eq!(matched, vec!["zzz_alpha", "zzz_beta"]);

    // WHERE filters by value.
    let filtered = strings(&mut session, "show databases where database = 'zzz_alpha'");
    assert_eq!(filtered, vec!["zzz_alpha"]);

    // SHOW COLLATION WHERE restricts to the charset's collations.
    let collations = strings(&mut session, "show collation where charset = 'utf8mb4'");
    assert!(!collations.is_empty());
    assert!(collations.iter().all(|row| row.contains("utf8mb4")), "{collations:?}");
}
