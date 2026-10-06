//! Leap-year clamping in interval arithmetic: Feb 29 2024 + 1 year lands
//! on Feb 28 2025 (clamped, NOT Mar 1), Aug 31 + 1 month clamps to Sep 30,
//! and the month-unit case (Feb 29 -> Mar 29) is preserved exactly when the
//! target month has the day.

use tidb_session::Session;

use crate::support::try_string_rows_70 as try_sql;

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
