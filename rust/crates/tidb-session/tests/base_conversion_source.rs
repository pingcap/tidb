//! Base-conversion functions: CONV translates between bases in either
//! direction (string or number input), BIN/OCT are base-2/base-8
//! projections, and TO_BASE64/FROM_BASE64 round-trip the RFC 4648 standard
//! alphabet ('abc' -> 'YWJj').

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
fn base_conversions_round_trip() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select conv('ff', 16, 2), conv(255, 10, 16)"),
        "s:11111111|s:FF"
    );
    assert_eq!(try_sql(&mut session, "select bin(10)"), "s:1010");
    assert_eq!(try_sql(&mut session, "select oct(8)"), "s:10");

    // RFC 4648: base64("abc") = "YWJj"; the round trip restores the input.
    assert_eq!(try_sql(&mut session, "select to_base64('abc')"), "s:YWJj");
    assert_eq!(
        try_sql(&mut session, "select from_base64(to_base64('abc'))"),
        "s:abc"
    );
}
