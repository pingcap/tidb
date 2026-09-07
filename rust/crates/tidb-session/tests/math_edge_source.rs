//! Math edge semantics: SQRT of a negative is NULL, POW of a negative base
//! with a fractional exponent raises the out-of-range error, and SIGN/ABS
//! follow the usual sign rules.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Real(f) => format!("f:{f}"),
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
        Err(e) => format!("ERR {}", e.to_string()),
    }
}

#[test]
fn sqrt_pow_sign_edges() {
    let mut session = Session::new();

    assert_eq!(try_sql(&mut session, "select sqrt(9), sqrt(2), sqrt(-1)").contains("Null"), true);
    assert_eq!(
        try_sql(&mut session, "select pow(2, 10), pow(2, -2)").contains("1024"),
        true
    );

    // Negative base with fractional exponent: out of range, not NaN.
    let error = try_sql(&mut session, "select pow(-2, 0.5)");
    assert!(error.contains("out of range"), "{error}");

    assert_eq!(
        try_sql(&mut session, "select sign(-3), sign(0), sign(2.5), abs(-4)"),
        "i:-1|i:0|i:1|i:4"
    );
}
