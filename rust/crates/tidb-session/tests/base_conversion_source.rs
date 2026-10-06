//! Base-conversion functions: CONV translates between bases in either
//! direction (string or number input), BIN/OCT are base-2/base-8
//! projections, and TO_BASE64/FROM_BASE64 round-trip the RFC 4648 standard
//! alphabet ('abc' -> 'YWJj').

use tidb_session::Session;

use crate::support::try_tagged_rows_60 as try_sql;

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
