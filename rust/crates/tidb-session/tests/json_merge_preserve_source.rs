//! JSON_MERGE_PRESERVE: arrays concatenate, objects deep-merge, and a
//! scalar merges into an array by wrapping — the PRESERVE counterpart of
//! PATCH's null-deletion.

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
