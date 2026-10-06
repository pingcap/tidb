//! QUOTE renders a re-parseable SQL literal (doubling-quote style with
//! backslash escapes) and ORD/ASCII answer character codes — ORD computes
//! the multibyte first-character arithmetic (ORD('中') = 228*65536 +
//! 184*256 + 173 = 14989485) while ASCII('') is 0.

use tidb_session::Session;

use crate::support::try_tagged_rows_60 as try_sql;

#[test]
fn quoting_and_character_codes() {
    let mut session = Session::new();

    // QUOTE doubles the quote so the output re-parses to the input.
    assert_eq!(try_sql(&mut session, "select quote('a''b')"), "s:'a\\'b'");

    assert_eq!(try_sql(&mut session, "select ord('A'), ascii('A'), ascii('')"), "i:65|i:65|i:0");

    // ORD on a multibyte character composes its leading bytes.
    assert_eq!(try_sql(&mut session, "select ord('中')"), "i:14989485");
}
