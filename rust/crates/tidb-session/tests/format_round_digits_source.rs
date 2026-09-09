//! FORMAT's thousands separators with rounding at the digit count, and
//! ROUND with negative digits: TiDB's FLOAT path rounds half away (2.5 ->
//! 3) while the exact-DECIMAL path rounds half-even (1250, -2 -> 1200) —
//! both oracle conventions, pinned side by side.

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
                        tidb_datatype::Datum::Decimal(value) => format!("d:{}", value.to_string()),
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
fn thousands_separators_and_negative_digits() {
    let mut session = Session::new();

    assert_eq!(try_sql(&mut session, "select format(1234567.891, 2)"), "s:1,234,567.89");
    // Zero digits: rounded, no decimal point.
    assert_eq!(try_sql(&mut session, "select format(1234567.891, 0)"), "s:1,234,568");

    // Negative digits zero out integer positions.
    assert_eq!(try_sql(&mut session, "select round(1234, -2)"), "i:1200");
    assert_eq!(try_sql(&mut session, "select round(1250, -2)"), "i:1200");

    // The float path rounds half away from zero.
    assert_eq!(try_sql(&mut session, "select round(2.5), round(-2.5)"), "d:3|d:-3");
}
