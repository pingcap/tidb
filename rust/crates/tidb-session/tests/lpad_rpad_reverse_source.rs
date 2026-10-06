//! LPAD/RPAD cycle the pad string, truncate the input when the target is
//! shorter, and treat length 0 as empty; REVERSE reverses by rune; REPEAT
//! with a negative count is empty.

use tidb_session::Session;

use crate::support::tagged_string_rows_with_sql as rows;

#[test]
fn pad_reverse_repeat_edges() {
    let mut session = Session::new();

    assert_eq!(rows(&mut session, "select lpad('ab', 4, 'xy')"), "s:xyab");
    assert_eq!(rows(&mut session, "select lpad('abcdef', 3, 'x')"), "s:abc");
    assert_eq!(rows(&mut session, "select lpad('ab', 0, 'x')"), "s:");
    assert_eq!(rows(&mut session, "select rpad('ab', 4, 'xy')"), "s:abxy");

    // Multibyte-safe reversal.
    assert_eq!(rows(&mut session, "select reverse('héllo')"), "s:olléh");

    assert_eq!(
        rows(&mut session, "select repeat('a', -1), repeat('ab', 3)"),
        "s:|s:ababab"
    );
}
