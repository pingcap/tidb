//! Fractional durations: TIME_FORMAT renders the %f microsecond field,
//! SEC_TO_TIME preserves a half-second as fsp 1, and EXTRACT(MICROSECOND)
//! reads the six-digit tail.

use tidb_session::Session;

use crate::support::try_tagged_rows_70 as try_sql;

#[test]
fn microsecond_fields() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select time_format('10:20:30.123456', '%H %i %s %f')"),
        "s:10 20 30 123456"
    );

    // SEC_TO_TIME keeps the fractional part (fsp 1 = one decimal digit).
    let kept = try_sql(&mut session, "select sec_to_time(3661.5)");
    assert!(kept.contains("fsp: 1"), "{kept}");

    assert_eq!(
        try_sql(&mut session, "select extract(microsecond from '10:20:30.123456')"),
        "i:123456"
    );
}
