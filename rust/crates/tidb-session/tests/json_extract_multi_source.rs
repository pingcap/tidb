//! JSON_EXTRACT with multiple paths: all matches collect into an array,
//! a missing path is omitted from that array, every path missing yields
//! NULL, and array-index paths (`$[1]`) pick by position.

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
