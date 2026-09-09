//! The SHOW family: SHOW TABLES lists table names, SHOW COLUMNS reports the
//! column/type/nullability/key shapes, and SHOW CREATE TABLE prints TiDB's
//! canonical DDL (backquoted columns, PRIMARY KEY with the clustered-index
//! comment, ENGINE/CHARSET/COLLATE tail).

use tidb_session::Session;

fn strings(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
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
                        tidb_datatype::Datum::Null => "NULL".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn show_tables_columns_and_create() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b varchar(4))")
        .unwrap();

    assert_eq!(strings(&mut session, "show tables"), vec![vec!["t"]]);

    let columns = strings(&mut session, "show columns from t");
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0][0], "a");
    assert_eq!(columns[0][1], "int");
    assert_eq!(columns[0][2], "NO");
    assert_eq!(columns[0][3], "PRI");
    assert_eq!(columns[1][0], "b");
    assert_eq!(columns[1][1], "varchar(4)");
    assert_eq!(columns[1][2], "YES");

    let create = strings(&mut session, "show create table t");
    assert_eq!(create[0][0], "t");
    assert!(create[0][1].contains("CREATE TABLE `t`"), "{}", create[0][1]);
    assert!(create[0][1].contains("`a` int NOT NULL"), "{}", create[0][1]);
    assert!(
        create[0][1].contains("PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */"),
        "{}",
        create[0][1]
    );
    assert!(
        create[0][1].contains("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"),
        "{}",
        create[0][1]
    );
}
