//! DATE_FORMAT's week-family specifiers for 2024-01-04 (a Thursday in the
//! first partial week): %U week 00 (Sunday start, pre-first-Sunday days),
//! %u week 01 (Monday start), %V/%v the corresponding roman-style and
//! ISO-ish variants, %X/%x the week-owned years (2023/2024), plus %d %e %a
//! for the day fields.

use tidb_session::Session;

use crate::support::try_tagged_rows_70 as try_sql;

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
