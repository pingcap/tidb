//! EXTRACT with compound units packs the fields into one integer:
//! DAY_HOUR '03:04' -> 1503, YEAR_MONTH -> 202402, MINUTE_SECOND
//! '04:05' -> 405, and SECOND_MICROSECOND '05.12' -> 5120000.

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
        Err(e) => format!("ERR {}", &e.to_string()[..70.min(e.to_string().len())]),
    }
}

#[test]
fn compound_units_pack_positionally() {
    let mut session = Session::new();

    // DAY(15) and HOUR(3) pack as DAY*100 + HOUR.
    assert_eq!(
        try_sql(&mut session, "select extract(day_hour from '2024-02-15 03:04:05')"),
        "i:1503"
    );
    // YEAR(2024) and MONTH(2) pack as YEAR*100 + MONTH.
    assert_eq!(
        try_sql(&mut session, "select extract(year_month from '2024-02-15 03:04:05')"),
        "i:202402"
    );
    // MINUTE(4), SECOND(5) pack as MINUTE*100 + SECOND.
    assert_eq!(
        try_sql(&mut session, "select extract(minute_second from '2024-02-15 03:04:05')"),
        "i:405"
    );
    // SECOND_MICROSECOND scales: 5s * 1e6 + 120000us.
    assert_eq!(
        try_sql(&mut session, "select extract(second_microsecond from '2024-02-15 03:04:05.12')"),
        "i:5120000"
    );
}
