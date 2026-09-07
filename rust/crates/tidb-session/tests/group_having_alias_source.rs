//! GROUP BY compositions: ORDER BY + LIMIT applies after grouping (top-N
//! groups by count), HAVING accepts the SELECT alias (`cnt`) as MySQL's
//! extension, and HAVING with a bare aggregate filters on the folded value.

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
fn limit_after_grouping_and_having_forms() {
    let mut session = Session::new();
    seed(&mut session);

    // The limit truncates AFTER grouping and ordering.
    assert_eq!(
        rows(
            &mut session,
            "select g, count(*) as cnt from t group by g order by cnt desc, g limit 2"
        ),
        "3|3;1|2"
    );

    // HAVING through the alias (MySQL extension).
    assert_eq!(
        rows(&mut session, "select g, count(*) as cnt from t group by g having cnt > 1 order by g"),
        "1|2;3|3"
    );

    // HAVING with a bare aggregate.
    assert_eq!(
        rows(&mut session, "select g from t group by g having sum(v) > 5 order by g"),
        "3"
    );
}
