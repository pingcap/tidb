//! The IS TRUE / IS FALSE / IS UNKNOWN (UNKNOWN alias of NULL) family:
//! NULL never satisfies IS TRUE or IS FALSE but satisfies IS UNKNOWN, and
//! each has a negated IS NOT form.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| format!("{d:?}"))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn truth_value_predicates() {
    let mut session = Session::new();

    assert_eq!(
        rows(&mut session, "select 1 is true, 0 is true, null is true"),
        "Int(1)|Int(0)|Int(0)"
    );
    assert_eq!(
        rows(&mut session, "select 1 is false, 0 is false, null is false"),
        "Int(0)|Int(1)|Int(0)"
    );
    assert_eq!(
        rows(&mut session, "select 1 is unknown, null is unknown, null is not unknown"),
        "Int(0)|Int(1)|Int(0)"
    );
    assert_eq!(
        rows(&mut session, "select null is not true, 0 is not true"),
        "Int(1)|Int(1)"
    );
}
