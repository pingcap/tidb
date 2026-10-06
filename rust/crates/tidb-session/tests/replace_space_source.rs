//! REPLACE substitutes every occurrence (none -> unchanged; an empty
//! search string changes nothing) and SPACE clamps negative counts to the
//! empty string; NULL input propagates.

use tidb_session::Session;

use crate::support::try_tagged_string_rows_60 as try_sql;

#[test]
fn replace_all_and_space_clamp() {
    let mut session = Session::new();

    // Every occurrence expands.
    assert_eq!(try_sql(&mut session, "select replace('aaa', 'a', 'bb')"), "s:bbbbbb");
    // No occurrence: unchanged.
    assert_eq!(try_sql(&mut session, "select replace('abc', 'z', 'y')"), "s:abc");
    // Empty search string changes nothing.
    assert_eq!(try_sql(&mut session, "select replace('abc', '', '-')"), "s:abc");

    // SPACE: 3 spaces, then empty for 0 and -1.
    assert_eq!(try_sql(&mut session, "select space(3), space(0), space(-1)"), "s:   |s:|s:");

    // NULL propagates.
    assert_eq!(try_sql(&mut session, "select replace(null, 'a', 'b')"), "Null");
}
