//! JSON_STORAGE_FREE always answers 0 for parsed documents (TiDB's binary
//! form reserves no free space) and JSON_STORAGE_SIZE answers the binary
//! payload length plus its one-byte root type code — both from
//! `builtin_ext/json2.rs`, mirroring `builtinJSONStorage*Sig`.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
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
fn storage_semantics() {
    let mut session = Session::new();

    // Parsed documents reserve no free space.
    assert_eq!(
        try_sql(&mut session, "select json_storage_free('{\"a\": 1}')"),
        "i:0"
    );

    // The size is the binary payload length plus the type byte.
    let size = try_sql(&mut session, "select json_storage_size('{\"a\": 1}')");
    let value: i64 = size.trim_start_matches("i:").parse().expect("int size");
    assert!(value > 1 && value < 64, "plausible binary size: {size}");

    // SQL NULL propagates.
    assert_eq!(try_sql(&mut session, "select json_storage_size(null)"), "Null");
}
