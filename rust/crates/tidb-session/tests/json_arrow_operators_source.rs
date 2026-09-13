//! The `->` and `->>` JSON operators: `->` returns the quoted JSON value
//! (`"str"` with the quotes) while `->>` returns the unquoted text
//! (`str`), and both filter in WHERE.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(b) => {
                            format!("b:{}", String::from_utf8_lossy(b))
                        }
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
        other => panic!("expected rows, got {other:?}"),
    }
}

fn setup(session: &mut Session) {
    session.run("create table t (id int primary key, j json)").unwrap();
    session
        .run("insert into t values (1, '{\"k\": \"str\", \"n\": 42}')")
        .unwrap();
}

#[test]
fn arrow_operators_quote_semantics() {
    let mut session = Session::new();
    setup(&mut session);

    // ->> strips the JSON quotes.
    assert_eq!(rows(&mut session, "select j->>'$.k' from t"), "s:str");
    // -> keeps the JSON quoting.
    assert_eq!(rows(&mut session, "select j->'$.k' from t"), "s:\"str\"");

    // Filtering through ->> on the numeric member (rendered textually).
    assert_eq!(
        rows(&mut session, "select id from t where j->>'$.n' = '42'"),
        "i:1"
    );
}
