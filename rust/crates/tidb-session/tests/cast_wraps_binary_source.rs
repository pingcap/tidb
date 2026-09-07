//! CAST wraps across the signed boundary both ways (-1 AS UNSIGNED,
//! MaxUint64 AS SIGNED), DECIMAL(n,1) rounds to the declared shape, and the
//! BINARY operator forces a byte-exact comparison (which utf8mb4_bin
//! already is).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::UInt(v) => format!("u:{v}"),
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::String(value) => {
                            format!("s:{}", String::from_utf8_lossy(value.bytes()))
                        }
                        tidb_datatype::Datum::Decimal(value) => format!("d:{}", value.to_string()),
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
fn cast_boundaries_and_binary_operator() {
    let mut session = Session::new();

    assert_eq!(
        rows(&mut session, "select cast(-1 as unsigned), cast(18446744073709551615 as signed)"),
        "u:18446744073709551615|i:-1"
    );
    assert_eq!(rows(&mut session, "select cast(123 as char)"), "s:123");
    assert_eq!(rows(&mut session, "select cast('1.999' as decimal(3,1))"), "d:2.0");
    assert_eq!(
        rows(&mut session, "select 'abc' = binary 'ABC', 'abc' = 'ABC'"),
        "i:0|i:0"
    );
}
