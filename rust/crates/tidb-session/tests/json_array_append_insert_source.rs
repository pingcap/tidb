//! JSON_ARRAY_APPEND and JSON_ARRAY_INSERT: appending at a document path,
//! at a nested object path, and inserting at a positional index. The
//! `$[*]` wildcard is refused (Go: path expressions may not contain the *
//! token in this position).

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
                        tidb_datatype::Datum::Null => "Null".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", e.to_string()),
    }
}

#[test]
fn append_insert_and_wildcard_refusal() {
    let mut session = Session::new();

    // Plain array append.
    assert_eq!(
        try_sql(&mut session, "select json_array_append('[1, 2]', '$', 3)"),
        "s:[1, 2, 3]"
    );
    // Nested object array append.
    assert_eq!(
        try_sql(&mut session, "select json_array_append('{\"a\": [1]}', '$.a', 2)"),
        "s:{\"a\": [1, 2]}"
    );
    // Wildcard path refused in this position.
    let error = try_sql(
        &mut session,
        "select json_array_append('{\"a\": [1], \"b\": [2]}', '$[*]', 9)",
    );
    assert!(
        error.contains("path expressions may not contain the *"),
        "{error}"
    );
    // Positional insert at index 1.
    assert_eq!(
        try_sql(&mut session, "select json_array_insert('[1, 3]', '$[1]', 2)"),
        "s:[1, 2, 3]"
    );
}
