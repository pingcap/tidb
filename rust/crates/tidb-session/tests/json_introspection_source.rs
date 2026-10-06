//! JSON introspection: JSON_KEYS lists an object's keys as an array,
//! JSON_LENGTH counts members of objects and arrays, and JSON_UNQUOTE
//! unwraps a quoted string INCLUDING escape resolution (\t -> a literal
//! tab) — the semantics differ from a plain string strip.

use tidb_session::Session;

use crate::support::try_tagged_rows_60 as try_sql;

#[test]
fn json_introspection_and_unquote() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select json_keys('{\"a\": 1, \"b\": 2}')"),
        "s:[\"a\", \"b\"]"
    );
    assert_eq!(
        try_sql(&mut session, "select json_length('{\"a\": 1, \"b\": 2}')"),
        "i:2"
    );
    assert_eq!(try_sql(&mut session, "select json_length('[1, 2, 3]')"), "i:3");

    // Plain unwrap.
    assert_eq!(try_sql(&mut session, "select json_unquote('\"hi\"')"), "s:hi");

    // Escape resolution through the extracted value: a literal TAB.
    assert_eq!(
        try_sql(&mut session, "select json_unquote(json_extract('{\"a\": \"x\\\\ty\"}', '$.a'))"),
        "s:x\ty"
    );
}
