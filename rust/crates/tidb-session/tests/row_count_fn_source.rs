//! ROW_COUNT() reports the previous statement's affected-row count: 2 after
//! a two-row insert, 1 after a single-row update.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Affected(n)) => format!("affected {n}"),
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
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

#[test]
fn row_count_tracks_the_previous_statement() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, v int)")
        .unwrap();

    assert_eq!(
        try_sql(&mut session, "insert into t values (1, 10), (2, 20)"),
        "affected 2"
    );
    assert_eq!(try_sql(&mut session, "select row_count()"), "i:2");

    assert_eq!(
        try_sql(&mut session, "update t set v = 99 where id = 1"),
        "affected 1"
    );
    assert_eq!(try_sql(&mut session, "select row_count()"), "i:1");
}
