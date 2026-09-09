//! GREATEST/LEAST propagate NULL (any NULL argument -> NULL), and RAND with
//! an explicit seed is deterministic within the session and always in
//! [0, 1).

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
fn null_propagation_and_seeded_rand() {
    let mut session = Session::new();

    assert_eq!(
        rows(&mut session, "select greatest(1, null, 3), least(1, null, 3)"),
        "Null|Null"
    );
    assert_eq!(
        rows(&mut session, "select greatest(1, 5), least(2, 8)"),
        "Int(5)|Int(2)"
    );

    // Seeded RAND is reproducible and in range.
    let first = rows(&mut session, "select rand(42)");
    let second = rows(&mut session, "select rand(42)");
    assert_eq!(first, second, "same seed must reproduce");
    assert!(first.starts_with("Real(0."), "{first}");
    assert_eq!(
        rows(&mut session, "select rand(7) >= 0 and rand(7) < 1"),
        "Int(1)"
    );
}
