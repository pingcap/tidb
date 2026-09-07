//! SHOW CHARSET/COLLATION include the utf8mb4 entries, SELECT DISTINCT
//! deduplicates, and constant SELECTs work without a FROM clause.

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
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        other => format!("{other:?}"),
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn show_variants_distinct_and_constants() {
    let mut session = Session::new();

    let charset = strings(&mut session, "show charset");
    assert!(charset.iter().any(|row| row.iter().any(|c| c == "utf8mb4")));

    let collation = strings(&mut session, "show collation");
    assert!(collation.iter().any(|row| row.iter().any(|c| c == "utf8mb4_bin")));

    session.run("create table t (a int, b int)").unwrap();
    session.run("insert into t values (1, 1), (1, 2), (2, 3)").unwrap();
    assert_eq!(
        strings(&mut session, "select distinct a from t order by a"),
        vec![vec!["1"], vec!["2"]]
    );

    assert_eq!(strings(&mut session, "select 1 + 1, 'x'"), vec![vec!["2", "x"]]);
}
