//! A generated column extracting from a JSON document: k = the unquoted
//! `$.k` member of j, computed on write and usable in a WHERE filter.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
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
        other => panic!("expected rows, got {other:?}"),
    }
}

fn setup(session: &mut Session) {
    session
        .run(
            "create table t (id int primary key, j json, \
             k varchar(8) as (json_unquote(j->'$.k')) stored)",
        )
        .unwrap();
    session
        .run("insert into t (id, j) values (1, '{\"k\": \"hello\"}')")
        .unwrap();
}

#[test]
fn generated_column_over_json_extraction() {
    let mut session = Session::new();
    setup(&mut session);

    // The extracted value lands in the stored column.
    assert_eq!(rows(&mut session, "select k from t"), "s:hello");

    // The generated column filters like a real column.
    assert_eq!(rows(&mut session, "select id from t where k = 'hello'"), "i:1");
    assert_eq!(rows(&mut session, "select id from t where k = 'bye'"), "");
}
