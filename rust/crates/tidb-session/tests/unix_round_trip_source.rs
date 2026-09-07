//! `unix_timestamp(from_unixtime(n))` round-trips to n in ANY session zone,
//! EXTRACT pulls calendar fields, and ADDDATE/SUBDATE are the dated
//! aliases of DATE_ADD/DATE_SUB.

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
fn zone_invariant_round_trip() {
    let mut session = Session::new();

    // The round trip is exact no matter the session zone.
    assert_eq!(
        rows(&mut session, "select unix_timestamp(from_unixtime(86400))"),
        "i:86400"
    );

    assert_eq!(
        rows(&mut session, "select extract(year from '2024-03-15'), extract(month from '2024-03-15')"),
        "i:2024|i:3"
    );

    // ADDDATE/SUBDATE: the DATE_ADD/DATE_SUB aliases.
    assert_eq!(
        rows(&mut session, "select adddate('2024-01-31', interval 1 day), subdate('2024-02-01', interval 1 day)"),
        "s:2024-02-01|s:2024-01-31"
    );
}
