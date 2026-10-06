//! JSON_CONTAINS_PATH's one/all modes and JSON_QUOTE's escape rendering
//! (a quote inside the value becomes `\"` inside the returned JSON string).

use tidb_session::Session;

use crate::support::try_tagged_rows_70 as try_sql;

#[test]
fn path_presence_and_quoting() {
    let mut session = Session::new();

    // 'one': at least one of the paths exists.
    assert_eq!(
        try_sql(
            &mut session,
            "select json_contains_path('{\"a\": 1, \"b\": 2}', 'one', '$.a')"
        ),
        "i:1"
    );

    // 'all': every named path must exist; none do -> 0.
    assert_eq!(
        try_sql(
            &mut session,
            "select json_contains_path('{\"a\": 1}', 'all', '$.x', '$.y')"
        ),
        "i:0"
    );

    // QUOTE: the inner quote is backslash-escaped in the JSON string.
    assert_eq!(
        try_sql(&mut session, "select json_quote('a\"b')"),
        "s:\"a\\\"b\""
    );
}
