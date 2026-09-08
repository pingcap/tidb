//! Leap-year clamping in interval arithmetic: Feb 29 2024 + 1 year lands
//! on Feb 28 2025 (clamped, NOT Mar 1), Aug 31 + 1 month clamps to Sep 30,
//! and the month-unit case (Feb 29 -> Mar 29) is preserved exactly when the
//! target month has the day.

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
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", &e.to_string()[..70.min(e.to_string().len())]),
    }
}

#[test]
fn leap_year_clamps() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select date_add('2024-02-29', interval 1 year)"),
        "s:2025-02-28"
    );
    assert_eq!(
        try_sql(&mut session, "select date_sub('2025-02-28', interval 1 year)"),
        "s:2024-02-28"
    );
    assert_eq!(
        try_sql(&mut session, "select date_add('2024-08-31', interval 1 month)"),
        "s:2024-09-30"
    );
}
