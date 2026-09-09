//! COUNT's NULL semantics: COUNT(*) counts every row, COUNT(col) skips
//! NULLs, COUNT(DISTINCT col) dedupes after the skip, and SUM over an
//! all-NULL input is NULL (not 0).

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Null => "Null".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", e.to_string()),
    }
}

fn setup(session: &mut Session) {
    session.run("create table t (a int, b int)").unwrap();
    session.run("insert into t values (1, 10), (2, null), (3, 30), (null, 40)").unwrap();
}

#[test]
fn count_variants_and_null_sum() {
    let mut session = Session::new();
    setup(&mut session);

    // 4 rows; 3 non-NULL a's and b's; DISTINCT b = {10, 30, 40}.
    assert_eq!(
        try_sql(&mut session, "select count(*), count(a), count(b), count(distinct b) from t"),
        "i:4|i:3|i:3|i:3"
    );

    // SUM of an all-NULL selection is NULL, not 0.
    assert_eq!(try_sql(&mut session, "select sum(b) from t where a = 2"), "Null");
}
