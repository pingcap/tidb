//! The clock functions as shape contracts (no wall-clock values asserted):
//! CURRENT_DATE is a DATE that equals DATE(CURRENT_DATE()), CURTIME()
//! renders at least `HH:MM:SS`, and NOW() renders a full 19-character
//! datetime.

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
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn clock_shape_contracts() {
    let mut session = Session::new();

    // CURRENT_DATE carries kind Date (a zeroed time component), and equals
    // its own DATE() projection.
    let curdate = rows(&mut session, "select current_date");
    assert!(curdate.contains("kind: Date"), "{curdate}");
    assert_eq!(
        rows(&mut session, "select current_date = date(current_date())"),
        "Int(1)"
    );

    // CURTIME is at least HH:MM:SS.
    assert_eq!(
        rows(&mut session, "select char_length(curtime()) >= 8"),
        "Int(1)"
    );

    // NOW() is a full datetime.
    assert_eq!(rows(&mut session, "select char_length(now()) = 19"), "Int(1)");
}
