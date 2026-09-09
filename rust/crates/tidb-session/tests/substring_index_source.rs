//! SUBSTRING_INDEX count semantics: positive counts take everything before
//! the Nth delimiter from the left, negative counts from the right, zero
//! yields the empty string, and a missing delimiter returns the whole
//! input.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
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
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn count_sign_and_missing_delimiter() {
    let mut session = Session::new();

    assert_eq!(
        rows(&mut session, "select substring_index('a.b.c', '.', 2)"),
        "s:a.b"
    );
    assert_eq!(
        rows(&mut session, "select substring_index('a.b.c', '.', -1)"),
        "s:c"
    );
    assert_eq!(
        rows(&mut session, "select substring_index('a.b.c', '.', -2)"),
        "s:b.c"
    );
    assert_eq!(rows(&mut session, "select substring_index('a.b.c', '.', 0)"), "s:");
    assert_eq!(rows(&mut session, "select substring_index('abc', '.', 2)"), "s:abc");
}
