//! DATE_FORMAT's week-family specifiers for 2024-01-04 (a Thursday in the
//! first partial week): %U week 00 (Sunday start, pre-first-Sunday days),
//! %u week 01 (Monday start), %V/%v the corresponding roman-style and
//! ISO-ish variants, %X/%x the week-owned years (2023/2024), plus %d %e %a
//! for the day fields.

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
        Err(e) => format!("ERR {}", &e.to_string()[..70.min(e.to_string().len())]),
    }
}

#[test]
fn week_mode_specifiers() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select date_format('2024-01-04', '%U %u %V %v')"),
        "s:00 01 53 01"
    );

    // %X and %x disagree across the year boundary — the whole point.
    assert_eq!(
        try_sql(&mut session, "select date_format('2024-01-04', '%X %x')"),
        "s:2023 2024"
    );

    assert_eq!(
        try_sql(&mut session, "select date_format('2024-02-15', '%d %e %a')"),
        "s:15 15 Thu"
    );
}
