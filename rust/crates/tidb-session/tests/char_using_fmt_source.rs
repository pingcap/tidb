//! CHAR(N, ...) composes each integer's minimal big-endian bytes and then
//! DECODES the byte string in the USING charset (builtin_string.go:2507-
//! 2530: `convertToBytes` + `OpDecode`) — so char(22823 using utf8mb4) is
//! the two ASCII bytes 0x59 0x27 ("Y'"), NOT the Unicode codepoint 大.
//! DATE_FORMAT's %b/%c short month and day-without-zero render too.

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
fn char_composition_and_short_formats() {
    let mut session = Session::new();

    // Two ints: 'H', 'I'.
    assert_eq!(try_sql(&mut session, "select char(72, 73)"), "s:HI");

    // 0x5927 is emitted as the two bytes 0x59 0x27, decoded as ASCII-valid.
    assert_eq!(try_sql(&mut session, "select char(22823 using utf8mb4)"), "s:Y'");

    // Short month + day without a leading zero.
    assert_eq!(try_sql(&mut session, "select date_format('2024-02-15', '%b %c')"), "s:Feb 2");

    // 12-hour clock with the AM/PM marker.
    assert_eq!(
        try_sql(&mut session, "select date_format('2024-02-15 13:45:00', '%r')"),
        "s:01:45:00 PM"
    );
}
