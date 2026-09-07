//! Derived tables over aggregation: an outer query filters the grouped
//! subquery (`c > 1`) and joins the derived result back to the base table.

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
fn derived_aggregation_filters_and_joins() {
    let mut session = Session::new();
    seed(&mut session);

    // Outer filter over the grouped subquery.
    assert_eq!(
        rows(
            &mut session,
            "select g, c from (select g, count(*) as c from t group by g) d where c > 1 order by g"
        ),
        "1|2;3|3"
    );

    // The derived result joins back to the base table (c = 1 matches g = 2).
    assert_eq!(
        rows(
            &mut session,
            "select d.g, d.c from (select g, count(*) as c from t group by g) d join t on t.g = d.g where d.c = 1 order by d.g"
        ),
        "2|1"
    );
}
