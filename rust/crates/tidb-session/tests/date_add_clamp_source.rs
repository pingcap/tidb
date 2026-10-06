//! DATE_ADD/DATE_SUB clamp month arithmetic to the month's last day across
//! leap years (Jan 31 + 1 month -> Feb 29 in 2024); negative intervals
//! subtract; LAST_DAY/MONTHNAME/QUARTER answer per the calendar.

use tidb_session::Session;

use crate::support::tagged_rows_with_sql as rows;

#[test]
fn month_arithmetic_clamps_to_last_day() {
    let mut session = Session::new();

    // Both directions clamp Jan 31 / Mar 31 onto Feb 29 (leap year).
    assert_eq!(
        rows(&mut session, "select date_add('2024-01-31', interval 1 month)"),
        "s:2024-02-29"
    );
    assert_eq!(
        rows(&mut session, "select date_sub('2024-03-31', interval 1 month)"),
        "s:2024-02-29"
    );

    // A negative day interval subtracts.
    assert_eq!(
        rows(&mut session, "select date_add('2024-01-10', interval -5 day)"),
        "s:2024-01-05"
    );

    // Calendar accessors.
    assert_eq!(
        rows(&mut session, "select last_day('2024-02-15')").contains("2024 2 29"),
        true
    );
    assert_eq!(rows(&mut session, "select monthname('2024-02-15')"), "s:February");
    assert_eq!(rows(&mut session, "select year('2024-02-15'), quarter('2024-02-15')"), "i:2024|i:1");
}
