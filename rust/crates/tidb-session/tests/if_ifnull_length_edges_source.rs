//! IF() treats NULL as falsy; IFNULL is the 2-arg fallback; LEFT/RIGHT
//! return the empty string for zero or negative lengths and the whole
//! string when the length exceeds the input.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::String(value) => {
                            format!("s:{}", String::from_utf8_lossy(value.bytes()))
                        }
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
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
fn control_and_length_edges() {
    let mut session = Session::new();

    assert_eq!(
        rows(&mut session, "select if(1, 'a', 'b'), if(0, 'a', 'b'), if(null, 'a', 'b')"),
        "s:a|s:b|s:b"
    );
    assert_eq!(
        rows(&mut session, "select ifnull(null, 7), ifnull(3, 7)"),
        "i:7|i:3"
    );
    assert_eq!(
        rows(&mut session, "select left('hello', -2), right('hello', -2)"),
        "s:|s:"
    );
    assert_eq!(rows(&mut session, "select left('hello', 0)"), "s:");
    assert_eq!(
        rows(&mut session, "select left('hi', 10), right('hi', 10)"),
        "s:hi|s:hi"
    );
}
