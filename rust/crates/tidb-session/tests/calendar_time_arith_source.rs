//! Calendar accessors and time arithmetic: WEEKOFYEAR/YEARWEEK (Jan 4
//! 2024 belongs to week 53 of 2023 under YEARWEEK), ODBC DAYOFWEEK
//! (Sunday = 1), DAYOFYEAR, and ADDTIME/SUBTIME crossing midnight.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::String(v) => {
                            format!("s:{}", String::from_utf8_lossy(v.bytes()))
                        }
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
        Err(e) => format!("ERR {}", &e.to_string()[..60.min(e.to_string().len())]),
    }
}

#[test]
fn week_and_time_arithmetic() {
    let mut session = Session::new();

    // YEARWEEK counts the year belonging to the WEEK, not the date.
    assert_eq!(
        try_sql(&mut session, "select weekofyear('2024-01-04'), yearweek('2024-01-04')"),
        "i:1|i:202353"
    );

    // ODBC numbering: 2024-02-15 is a Thursday (5), day 46 of the year.
    assert_eq!(
        try_sql(&mut session, "select dayofweek('2024-02-15'), dayofyear('2024-02-15')"),
        "i:5|i:46"
    );

    // Both directions cross midnight.
    assert_eq!(
        try_sql(&mut session, "select addtime('2024-01-01 23:00:00', '02:00:00')"),
        "s:2024-01-02 01:00:00"
    );
    assert_eq!(
        try_sql(&mut session, "select subtime('2024-01-01 01:00:00', '02:00:00')"),
        "s:2023-12-31 23:00:00"
    );
}
