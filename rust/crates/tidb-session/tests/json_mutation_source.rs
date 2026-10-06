//! The JSON mutation family: JSON_SET adds-or-replaces, JSON_REPLACE only
//! replaces existing paths, JSON_INSERT only adds new ones, JSON_REMOVE
//! deletes, JSON_MERGE_PATCH implements RFC 7396 (a null member deletes the
//! target's member), and JSON_CONTAINS answers membership.

use tidb_session::Session;

use crate::support::try_tagged_rows_60 as try_sql;

#[test]
fn mutation_family() {
    let mut session = Session::new();

    // SET: replace OR add.
    assert_eq!(
        try_sql(&mut session, "select json_set('{\"a\": 1}', '$.b', 2)"),
        "s:{\"a\": 1, \"b\": 2}"
    );
    // REPLACE: existing paths only.
    assert_eq!(
        try_sql(&mut session, "select json_replace('{\"a\": 1}', '$.a', 9)"),
        "s:{\"a\": 9}"
    );
    // INSERT: new paths only (existing `a` untouched).
    assert_eq!(
        try_sql(&mut session, "select json_insert('{\"a\": 1}', '$.b', 2)"),
        "s:{\"a\": 1, \"b\": 2}"
    );
    // REMOVE.
    assert_eq!(
        try_sql(&mut session, "select json_remove('{\"a\": 1, \"b\": 2}', '$.b')"),
        "s:{\"a\": 1}"
    );
    // RFC 7396: a null in the patch DELETES the target member.
    assert_eq!(
        try_sql(
            &mut session,
            "select json_merge_patch('{\"a\": 1, \"b\": 2}', '{\"b\": null, \"c\": 3}')"
        ),
        "s:{\"a\": 1, \"c\": 3}"
    );
    // Membership.
    assert_eq!(try_sql(&mut session, "select json_contains('[1, 2, 3]', '2')"), "i:1");
}
