//! JSON_MERGE_PRESERVE: arrays concatenate, objects deep-merge, and a
//! scalar merges into an array by wrapping — the PRESERVE counterpart of
//! PATCH's null-deletion.

use tidb_session::Session;

use crate::support::try_string_rows_70 as try_sql;

#[test]
fn preserve_concatenates_and_deep_merges() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select json_merge_preserve('[1]', '[2, 3]')"),
        "s:[1, 2, 3]"
    );
    assert_eq!(
        try_sql(
            &mut session,
            "select json_merge_preserve('{\"a\": {\"x\": 1}}', '{\"a\": {\"y\": 2}}')"
        ),
        "s:{\"a\": {\"x\": 1, \"y\": 2}}"
    );
    // A scalar merges into an array by wrapping.
    assert_eq!(
        try_sql(&mut session, "select json_merge_preserve('[1]', '2')"),
        "s:[1, 2]"
    );
}
