//! DATE_FORMAT renders the %-specifiers (%Y %m %d %H %i %W %M %e) and the
//! date-arithmetic family answers: DATEDIFF spans 364 days across 2023 and
//! TIMESTAMPDIFF(HOUR, ...) counts 27.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::String(v) => {
                            format!("s:{}", String::from_utf8_lossy(v.bytes()))
                        }
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
fn format_and_diff() {
    let mut session = Session::new();

    assert_eq!(
        rows(
            &mut session,
            "select date_format('2023-07-14 09:30:00', '%Y/%m/%d %H:%i')"
        ),
        "s:2023/07/14 09:30"
    );
    // 2023-07-14 was a Friday.
    assert_eq!(
        rows(&mut session, "select date_format('2023-07-14', '%W %M %e')"),
        "s:Friday July 14"
    );
    assert_eq!(
        rows(&mut session, "select datediff('2023-12-31', '2023-01-01')"),
        "i:364"
    );
    assert_eq!(
        rows(
            &mut session,
            "select timestampdiff(hour, '2023-01-01 00:00:00', '2023-01-02 03:00:00')"
        ),
        "i:27"
    );
}
