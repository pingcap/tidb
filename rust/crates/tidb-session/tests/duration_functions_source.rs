//! Duration-aware time functions: HOUR reads durations beyond 24 hours
//! ('25:00:00' -> 25), TIME_FORMAT renders the full duration width,
//! TIME() extracts the time part of a datetime, and MICROSECOND reads
//! the fractional tail.

use tidb_session::Session;

use crate::support::try_tagged_rows_70 as try_sql;

#[test]
fn duration_width_and_parts() {
    let mut session = Session::new();

    // Durations exceed the 24-hour wall clock.
    assert_eq!(try_sql(&mut session, "select hour('25:00:00')"), "i:25");
    assert_eq!(
        try_sql(&mut session, "select time_format('25:30:00', '%H %i')"),
        "s:25 30"
    );

    // TIME() of a datetime extracts the clock part.
    assert_eq!(
        try_sql(&mut session, "select time_to_sec(time('2024-01-01 10:20:30'))"),
        "i:37230"
    );

    assert_eq!(
        try_sql(&mut session, "select microsecond('10:20:30.123456')"),
        "i:123456"
    );
}
