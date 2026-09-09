//! Set operations compose as derived tables and honor per-branch ORDER BY
//! + LIMIT: a UNION ALL under a FROM clause flows through the outer ORDER
//! BY, and parenthesized branches each keep their own ordered limit.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", &e.to_string()[..60.min(e.to_string().len())]),
    }
}

#[test]
fn union_as_derived_and_branch_limits() {
    let mut session = Session::new();
    session.run("create table t (a int)").unwrap();
    session.run("insert into t values (1), (2), (3)").unwrap();

    // The derived table sees all six rows; the outer ORDER BY sorts them.
    assert_eq!(
        rows(
            &mut session,
            "select a from (select a from t union all select a + 10 from t) u order by a"
        ),
        "i:1;i:2;i:3;i:11;i:12;i:13"
    );

    // Each parenthesized branch keeps its own ORDER BY + LIMIT.
    assert_eq!(
        rows(
            &mut session,
            "(select a from t order by a limit 1) union all (select a from t order by a desc limit 1)"
        ),
        "i:1;i:3"
    );
}
