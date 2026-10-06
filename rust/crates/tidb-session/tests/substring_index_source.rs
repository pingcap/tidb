//! SUBSTRING_INDEX count semantics: positive counts take everything before
//! the Nth delimiter from the left, negative counts from the right, zero
//! yields the empty string, and a missing delimiter returns the whole
//! input.

use tidb_session::Session;

use crate::support::tagged_string_rows_with_sql as rows;

#[test]
fn count_sign_and_missing_delimiter() {
    let mut session = Session::new();

    assert_eq!(
        rows(&mut session, "select substring_index('a.b.c', '.', 2)"),
        "s:a.b"
    );
    assert_eq!(
        rows(&mut session, "select substring_index('a.b.c', '.', -1)"),
        "s:c"
    );
    assert_eq!(
        rows(&mut session, "select substring_index('a.b.c', '.', -2)"),
        "s:b.c"
    );
    assert_eq!(rows(&mut session, "select substring_index('a.b.c', '.', 0)"), "s:");
    assert_eq!(rows(&mut session, "select substring_index('abc', '.', 2)"), "s:abc");
}
