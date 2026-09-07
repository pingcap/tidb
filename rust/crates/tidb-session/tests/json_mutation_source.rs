//! The JSON mutation family: JSON_SET adds-or-replaces, JSON_REPLACE only
//! replaces existing paths, JSON_INSERT only adds new ones, JSON_REMOVE
//! deletes, JSON_MERGE_PATCH implements RFC 7396 (a null member deletes the
//! target's member), and JSON_CONTAINS answers membership.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::String(v) => {
                            format!("s:{}", String::from_utf8_lossy(v.bytes()))
                        }
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Null => "Null".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", &e.to_string()[..60.min(e.to_string().len())]),
    }
}

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
