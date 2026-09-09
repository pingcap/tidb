//! only_full_group_by (the default sql_mode) refuses a SELECT list column
//! that is neither grouped nor aggregated — Go's ErrFieldInGroupingNotInGroupBy
//! text — while the grouped/aggregate form answers.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Decimal(value) => format!("d:{}", value.to_string()),
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
    session.run("create table t (a int, v int)").unwrap();
    session.run("insert into t values (1, 10), (1, 20)").unwrap();
}

#[test]
fn non_grouped_column_refused() {
    let mut session = Session::new();
    setup(&mut session);

    let error = try_sql(&mut session, "select a, v from t group by a");
    assert!(
        error.contains("not in GROUP BY clause"),
        "{error}"
    );

    // The proper grouped form answers.
    assert_eq!(
        try_sql(&mut session, "select a, sum(v) from t group by a"),
        "i:1|d:30"
    );
}
