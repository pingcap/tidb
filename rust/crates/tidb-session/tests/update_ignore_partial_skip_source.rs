//! `UPDATE IGNORE` with a CHECK constraint applies only the conforming
//! rows: the violating row is downgraded to a warning and SKIPPED in place,
//! while the other rows update normally (Go's per-row skip semantics).

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
fn ignore_applies_conforming_rows_and_skips_violating_ones() {
    let mut session = Session::new();
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run("create table t (a int primary key, b int check (b > 0))")
        .unwrap();
    session.run("insert into t values (1, 1), (2, 2), (3, 3)").unwrap();

    // Only row 2's new value violates; rows 1 and 3 apply.
    let affected = match session
        .run("update ignore t set b = case a when 1 then 11 when 2 then -2 when 3 then 33 end")
        .unwrap()
    {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(affected, 2, "rows 1 and 3 changed; row 2 skipped");

    assert_eq!(
        rows(&mut session, "select a, b from t order by a"),
        "1|11;2|2;3|33",
        "the violating row keeps its original value"
    );
}
