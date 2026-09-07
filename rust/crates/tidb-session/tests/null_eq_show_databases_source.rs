//! Null-safe equality and SHOW DATABASES: `<=>` matches NULL to NULL where
//! `=` yields UNKNOWN, `<=> NULL` answers directly, and SHOW DATABASES
//! lists the created schema beside the system ones.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::Bytes(b) => {
                            String::from_utf8_lossy(b).into_owned().to_uppercase()
                        }
                        tidb_datatype::Datum::String(s) => {
                            String::from_utf8_lossy(&s.bytes()).into_owned().to_uppercase()
                        }
                        tidb_datatype::Datum::Null => "NULL".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect(),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

fn one(session: &mut Session, sql: &str) -> String {
    rows(session, sql).join(";")
}

#[test]
fn null_safe_equality_and_schema_list() {
    let mut session = Session::new();
    session.run("create table t (a int, b int)").unwrap();
    session
        .run("insert into t values (1, 1), (2, NULL), (NULL, NULL)")
        .unwrap();

    // Regular `=`: only the concrete match survives.
    assert_eq!(one(&mut session, "select a, b from t where a = b"), "1|1");

    // `<=>`: NULL matches NULL.
    assert_eq!(one(&mut session, "select a, b from t where a <=> b"), "1|1;NULL|NULL");
    assert_eq!(one(&mut session, "select a <=> NULL from t where a is null"), "1");

    // SHOW DATABASES lists created schemas beside the system ones.
    session.run("create database zzz_probe").unwrap();
    // Each row of `show databases` is ONE cell; rows() already uppercased it.
    let mut dbs = rows(&mut session, "show databases");
    dbs.sort();
    assert!(dbs.contains(&"ZZZ_PROBE".to_owned()), "{dbs:?}");
    assert!(dbs.iter().any(|db| db == "MYSQL" || db == "INFORMATION_SCHEMA"), "{dbs:?}");
}
