//! BIT_COUNT's population count (64 for the full 64-bit pattern of -1),
//! DATE_FORMAT's NULL propagation, and IFNULL's string promotion making
//! `ifnull(1, 'x') = '1'` compare as strings.

use tidb_session::Session;

use crate::support::try_tagged_integer_rows_60 as try_sql;

#[test]
fn popcount_null_and_promotion() {
    let mut session = Session::new();

    // 7 -> 3 bits; 0 -> 0; -1 -> all 64 bits of the two's complement.
    assert_eq!(
        try_sql(&mut session, "select bit_count(7), bit_count(0), bit_count(-1)"),
        "i:3|i:0|i:64"
    );

    // NULL date -> NULL string.
    assert_eq!(try_sql(&mut session, "select date_format(null, '%Y')"), "Null");

    // IFNULL promotes to the string type, so the comparison is textual.
    assert_eq!(try_sql(&mut session, "select ifnull(1, 'x') = '1'"), "i:1");
}
