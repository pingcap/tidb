//! Pagination and aggregate ordering: the MySQL two-arg `LIMIT off, cnt`
//! and `LIMIT cnt OFFSET off` forms skip+truncate identically, and ORDER BY
//! sorts by a folded aggregate with a group tiebreaker.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
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

fn seed(session: &mut Session) {
    session.run("create table t (g int, v int)").unwrap();
    session
        .run("insert into t values (1, 1), (1, 2), (2, 3), (3, 4), (3, 5), (3, 6)")
        .unwrap();
}

#[test]
fn offset_forms_and_aggregate_ordering() {
    let mut session = Session::new();
    seed(&mut session);

    // `LIMIT 2, 3` skips two rows and returns the next three.
    assert_eq!(
        rows(&mut session, "select g, v from t order by v limit 2, 3"),
        "2|3;3|4;3|5"
    );

    // `LIMIT 3 OFFSET 1` is the same skip semantics, other spelling.
    assert_eq!(
        rows(&mut session, "select g, v from t order by v limit 3 offset 1"),
        "1|2;2|3;3|4"
    );

    // ORDER BY a folded aggregate with an explicit group tiebreaker. SUM
    // folds to a DECIMAL, so assert on the debug form's digit bytes:
    // group 3 holds "15" (bytes 49,53) and the tie groups hold "3" (49).
    let ordered = rows(&mut session, "select g, sum(v) from t group by g order by sum(v) desc, g");
    let groups: Vec<&str> = ordered.split(';').collect();
    assert_eq!(groups.len(), 3);
    assert!(groups[0].starts_with("3|") && groups[0].contains("49, 53"), "{ordered}");
    assert!(groups[1].starts_with("1|") && groups[1].contains("51"), "{ordered}");
    assert!(groups[2].starts_with("2|") && groups[2].contains("51"), "{ordered}");
}
