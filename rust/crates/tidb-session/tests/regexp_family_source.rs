//! The REGEXP family: RLIKE/REGEXP matching, REGEXP_REPLACE with a global
//! character-class substitution, REGEXP_SUBSTR extracting the first match,
//! REGEXP_INSTR's 1-based position, and the case-insensitive REGEXP_LIKE.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
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
        Err(e) => format!("ERR {}", &e.to_string()[..60.min(e.to_string().len())]),
    }
}

#[test]
fn regexp_operators_and_functions() {
    let mut session = Session::new();

    assert_eq!(
        try_sql(&mut session, "select 'hello' regexp '^h.*o$', 'hello' rlike 'xyz'"),
        "i:1|i:0"
    );
    // Every digit is replaced.
    assert_eq!(
        try_sql(&mut session, "select regexp_replace('a1b2', '[0-9]', '#')"),
        "s:a#b#"
    );
    assert_eq!(
        try_sql(&mut session, "select regexp_substr('abc123def', '[0-9]+')"),
        "s:123"
    );
    // The first match starts at character 4 (1-based).
    assert_eq!(
        try_sql(&mut session, "select regexp_instr('abc123', '[0-9]+')"),
        "i:4"
    );
    assert_eq!(
        try_sql(&mut session, "select regexp_like('Hello', '^[hH]')"),
        "i:1"
    );
}
