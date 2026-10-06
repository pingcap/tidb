//! The INSERT(str, pos, len, new) splice function: pos is 1-based, pos 0
//! or beyond the length returns the input unchanged, a negative len splices
//! to the end, and NULL input propagates.

use tidb_session::Session;

use crate::support::try_tagged_string_rows_60 as try_sql;

#[test]
fn splice_position_and_length_rules() {
    let mut session = Session::new();

    // Replace 3 chars starting at position 2.
    assert_eq!(try_sql(&mut session, "select insert('abcdef', 2, 3, 'XY')"), "s:aXYef");
    // Position 0: out of range, input unchanged.
    assert_eq!(try_sql(&mut session, "select insert('abcdef', 0, 3, 'XY')"), "s:abcdef");
    // Position beyond the end: unchanged.
    assert_eq!(try_sql(&mut session, "select insert('abcdef', 10, 3, 'XY')"), "s:abcdef");
    // Negative length: splice to the end.
    assert_eq!(try_sql(&mut session, "select insert('abcdef', 2, -1, 'XY')"), "s:aXY");
    // NULL propagates.
    assert_eq!(try_sql(&mut session, "select insert(null, 2, 3, 'X')"), "Null");
}
