//! JSON_EXTRACT with multiple paths: all matches collect into an array,
//! a missing path is omitted from that array, every path missing yields
//! NULL, and array-index paths (`$[1]`) pick by position.

use tidb_session::Session;

use crate::support::try_tagged_rows_70 as try_sql;

#[test]
fn multi_path_extraction() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select json_extract('{\"a\": 1, \"b\": 2}', '$.a', '$.b')"),
        "s:[1, 2]"
    );
    assert_eq!(
        try_sql(&mut session, "select json_extract('{\"a\": 1}', '$.a', '$.zz')"),
        "s:[1]"
    );
    assert_eq!(
        try_sql(&mut session, "select json_extract('{\"a\": 1}', '$.x', '$.y')"),
        "Null"
    );
    assert_eq!(
        try_sql(&mut session, "select json_extract('[10, 20, 30]', '$[1]')"),
        "s:20"
    );
}
