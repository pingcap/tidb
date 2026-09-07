//! COALESCE returns its first non-NULL argument (all-NULL -> NULL) and
//! NULLIF(a, b) yields NULL when equal, a otherwise; the two compose
//! (`coalesce(nullif(null, 1), 5)` = 5).

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
fn null_handling_functions() {
    let mut session = Session::new();

    assert_eq!(
        rows(&mut session, "select coalesce(null, null, 3), coalesce(1, 2)"),
        "Int(3)|Int(1)"
    );
    assert_eq!(rows(&mut session, "select coalesce(null, null)"), "Null");
    assert_eq!(rows(&mut session, "select nullif(1, 1), nullif(1, 2)"), "Null|Int(1)");
    assert_eq!(rows(&mut session, "select coalesce(nullif(null, 1), 5)"), "Int(5)");
}
