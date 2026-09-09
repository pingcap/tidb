//! JSON_CONTAINS_PATH's one/all modes and JSON_QUOTE's escape rendering
//! (a quote inside the value becomes `\"` inside the returned JSON string).

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
        Err(e) => format!("ERR {}", &e.to_string()[..70.min(e.to_string().len())]),
    }
}

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
