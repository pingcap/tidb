//! JSON_SEARCH: 'one' returns the first matching path, 'all' returns every
//! matching path as an array — with %-wildcards supported.

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
        Err(e) => format!("ERR {}", &e.to_string()[..70.min(e.to_string().len())]),
    }
}

#[test]
fn one_and_all_modes() {
    let mut session = Session::new();

    // 'one': the first path whose value matches the pattern.
    assert_eq!(
        try_sql(
            &mut session,
            "select json_search('{\"a\": \"xyz\", \"b\": \"abc\"}', 'one', '%y%')"
        ),
        "s:\"$.a\""
    );

    // 'all': every matching path.
    assert_eq!(
        try_sql(
            &mut session,
            "select json_search('{\"a\": \"xy\", \"b\": \"xy\"}', 'all', 'xy')"
        ),
        "s:[\"$.a\", \"$.b\"]"
    );

    // No match: NULL.
    assert_eq!(
        try_sql(
            &mut session,
            "select json_search('{\"a\": \"xyz\"}', 'one', 'missing')"
        ),
        "Null"
    );
}
