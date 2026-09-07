//! JSON arrow operators on a JSON column — `j -> '$.a'` extracts the JSON
//! value, `j ->> '$.b'` extracts UNQUOTED — and the bitwise expression
//! family (&, |, ^, <<, >>, ~ over unsigned BIGINT: ~5 = 2^64-6).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::UInt(i) => format!("{i}"),
                        tidb_datatype::Datum::String(s) => {
                            format!("'{}'", String::from_utf8_lossy(&s.bytes()))
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

fn seed(session: &mut Session) {
    session.run("create table t (id int primary key, j json)").unwrap();
    session
        .run(r#"insert into t values (1, '{"a": 5, "b": "hi"}')"#)
        .unwrap();
}

#[test]
fn json_arrows_and_bitwise_family() {
    let mut session = Session::new();
    seed(&mut session);

    // `->` extracts the JSON value at the path.
    assert_eq!(rows(&mut session, "select j -> '$.a' from t"), "'5'");
    // `->>` extracts UNQUOTED.
    assert_eq!(rows(&mut session, "select j ->> '$.b' from t"), "'hi'");

    // The bitwise family over unsigned BIGINT.
    assert_eq!(
        rows(&mut session, "select 5 & 3, 5 | 3, 5 ^ 3, 5 << 1, 10 >> 1, ~5"),
        "1|7|6|10|5|18446744073709551610"
    );
}
