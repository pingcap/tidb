//! The bitwise aggregate family over [5, 3, 7]: BIT_AND = 1, BIT_OR = 7,
//! BIT_XOR = 1 (5^3 = 6, 6^7 = 1). Empty input yields the identity values
//! (all-ones AND, 0 for OR/XOR) with no NULL row.

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
fn bit_aggregates_combine_all_rows() {
    let mut session = Session::new();
    session.run("create table t (v int)").unwrap();
    session.run("insert into t values (5), (3), (7)").unwrap();

    assert_eq!(
        rows(&mut session, "select bit_and(v), bit_or(v), bit_xor(v) from t"),
        "u:1|u:7|u:1"
    );
}

#[test]
fn bit_aggregates_empty_input() {
    let mut session = Session::new();
    session.run("create table t (v int)").unwrap();

    // MySQL identities: AND starts all-ones, OR/XOR start at zero.
    assert_eq!(
        rows(&mut session, "select bit_and(v), bit_or(v), bit_xor(v) from t"),
        "u:18446744073709551615|u:0|u:0"
    );
}
