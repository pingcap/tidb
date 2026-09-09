//! Common table expressions: a single-reference non-recursive CTE filters
//! its source, and a RECURSIVE CTE iterates to its bound (`WITH RECURSIVE
//! seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 5)`).

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

#[test]
fn non_recursive_cte_filters_its_source() {
    let mut session = Session::new();
    session.run("create table t (a int, b int)").unwrap();
    session.run("insert into t values (1, 10), (2, 20), (3, 30)").unwrap();
    assert_eq!(
        rows(
            &mut session,
            "with big as (select a, b from t where b > 10) select a from big order by a"
        ),
        "2;3"
    );
}

#[test]
fn recursive_cte_iterates_to_its_bound() {
    let mut session = Session::new();
    assert_eq!(
        rows(
            &mut session,
            "with recursive seq (n) as (select 1 union all select n + 1 from seq where n < 5) \
             select n from seq order by n"
        ),
        "1;2;3;4;5"
    );
}
