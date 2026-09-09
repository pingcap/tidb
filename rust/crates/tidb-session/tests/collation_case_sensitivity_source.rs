//! Collation case-sensitivity: TiDB's default utf8mb4_bin makes '=' and
//! LIKE case-SENSITIVE ('a' != 'A'), an explicit `_general_ci` collation
//! makes them equal, and a _bin column keeps LIKE strict.

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
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn default_bin_is_strict_and_ci_collation_relaxes() {
    let mut session = Session::new();

    // Default utf8mb4_bin: case-sensitive equality.
    assert_eq!(rows(&mut session, "select 'a' = 'A'"), "0");

    // An explicit CI collation treats them as equal.
    assert_eq!(
        rows(
            &mut session,
            "select 'a' collate utf8mb4_general_ci = 'A' collate utf8mb4_general_ci"
        ),
        "1"
    );

    // A _bin column keeps LIKE and '=' strict.
    session
        .run("create table t (s varchar(8)) charset utf8mb4 collate utf8mb4_bin")
        .unwrap();
    session.run("insert into t values ('Apple'), ('apple')").unwrap();
    assert_eq!(rows(&mut session, "select s from t where s like 'apple%'"), "'apple'");
    assert_eq!(rows(&mut session, "select s from t where s = 'apple'"), "'apple'");
}
