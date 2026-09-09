//! CEIL rounds toward +infinity (-1.2 -> -1), FLOOR toward -infinity
//! (-1.8 -> -2), and TRUNCATE cuts digits without rounding — including
//! negative decimal places, which zero out integer positions
//! (truncate(123, -2) = 100).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
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
fn rounding_direction_edges() {
    let mut session = Session::new();

    assert_eq!(
        rows(&mut session, "select ceil(1.2), ceil(-1.2), ceil(2)"),
        "i:2|i:-1|i:2"
    );
    assert_eq!(rows(&mut session, "select floor(1.8), floor(-1.8)"), "i:1|i:-2");

    // truncate(1.999, 1) = 1.9, truncate(-1.999, 1) = -1.9, both DECIMAL;
    // negative decimal places zero out integer positions.
    assert_eq!(
        rows(&mut session, "select truncate(1.999, 1), truncate(-1.999, 1), truncate(123, -2)"),
        "d:1.9|d:-1.9|i:100"
    );
}
