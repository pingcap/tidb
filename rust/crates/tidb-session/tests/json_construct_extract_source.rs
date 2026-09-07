//! The JSON constructor/extraction family: JSON_OBJECT and JSON_ARRAY
//! render canonical spacing, JSON_TYPE reports ARRAY, nested-path
//! JSON_EXTRACT descends `$.a.b`, and JSON_VALID distinguishes documents.

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
fn json_construct_extract_validate() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select json_object('a', 1, 'b', 'x')"),
        "s:{\"a\": 1, \"b\": \"x\"}"
    );
    assert_eq!(
        try_sql(&mut session, "select json_array(1, 'x', true)"),
        "s:[1, \"x\", true]"
    );
    assert_eq!(
        try_sql(&mut session, "select json_type(json_array(1))"),
        "s:ARRAY"
    );
    assert_eq!(
        try_sql(&mut session, "select json_extract('{\"a\": {\"b\": 7}}', '$.a.b')"),
        "s:7"
    );
    assert_eq!(
        try_sql(&mut session, "select json_valid('{\"a\":1}'), json_valid('nope')"),
        "i:1|i:0"
    );
}
