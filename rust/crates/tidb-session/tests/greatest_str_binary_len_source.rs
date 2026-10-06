//! String GREATEST picks the lexicographic maximum, and BINARY strings
//! lose character semantics: length and char_length both count bytes
//! (binary('中a') -> 4/4, not 4/2).

use tidb_session::Session;

use crate::support::try_tagged_rows_70 as try_sql;

#[test]
fn string_extremum_and_binary_lengths() {
    let mut session = Session::new();

    // Lexicographic maximum ('b' > 'ab' > 'aa').
    assert_eq!(
        try_sql(&mut session, "select greatest('b', 'aa', 'ab')"),
        "s:b"
    );

    // BINARY strips the charset: every byte is a character.
    assert_eq!(
        try_sql(&mut session, "select length(binary('中a')), char_length(binary('中a'))"),
        "i:4|i:4"
    );
}
