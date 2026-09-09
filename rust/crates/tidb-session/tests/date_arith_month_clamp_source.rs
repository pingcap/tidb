//! DATE arithmetic month-end clamping: adding/subtracting months clamps to
//! the last valid day — Jan 31 + 1 month = Feb 29 (leap 2024), Feb 29
//! + 1 year = Feb 28 (2025), Mar 31 - 1 month = Feb 29.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
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

#[test]
fn month_arithmetic_clamps_to_valid_days() {
    let mut session = Session::new();

    // Jan 31 + 1 month lands on the leap-year Feb 29.
    assert_eq!(
        rows(&mut session, "select date_add('2024-01-31', interval 1 month)"),
        "'2024-02-29'"
    );

    // Feb 29 + 1 year clamps to Feb 28 in non-leap 2025.
    assert_eq!(
        rows(&mut session, "select date_add('2024-02-29', interval 1 year)"),
        "'2025-02-28'"
    );

    // Mar 31 - 1 month clamps to Feb 29.
    assert_eq!(
        rows(&mut session, "select date_sub('2024-03-31', interval 1 month)"),
        "'2024-02-29'"
    );
}
