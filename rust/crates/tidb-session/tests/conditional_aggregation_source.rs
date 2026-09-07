//! The conditional-aggregation idiom: `sum(case when v > 0 then v else 0
//! end)` pivots signs per group (a NULL v falls to ELSE 0), and
//! `count(case when v > 5 then 1 end)` counts only satisfied branches
//! because COUNT skips NULLs.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Decimal(value) => format!("d:{}", value.to_string()),
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
fn case_inside_aggregates() {
    let mut session = Session::new();
    session
        .run("create table t (g int, v int)")
        .unwrap();
    session
        .run("insert into t values (1, 10), (1, -5), (2, 7), (2, null)")
        .unwrap();

    // g=1: pos 10, neg -5. g=2: pos 7, neg 0 (the NULL row's ELSE 0).
    assert_eq!(
        rows(
            &mut session,
            "select g, sum(case when v > 0 then v else 0 end) pos, \
             sum(case when v < 0 then v else 0 end) neg from t group by g order by g"
        ),
        "i:1|d:10|d:-5;i:2|d:7|d:0"
    );

    // v > 5 holds for rows 10 and 7; the NULL-else row is not counted.
    assert_eq!(
        rows(&mut session, "select count(case when v > 5 then 1 end) from t"),
        "i:2"
    );
}
