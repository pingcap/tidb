//! UPDATE IGNORE with a generated-CHECK violation applies per row: the
//! violating row (a=-10 -> b=-20) is skipped unchanged, the valid row
//! (a=4 -> b=8) updates, and the statement reports 1 affected row.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Null => "Null".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Affected(n)) => format!("affected {n}"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", e.to_string()),
    }
}

fn setup(session: &mut Session) {
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session
        .run(
            "create table t (id int primary key, a int, \
             b int as (a * 2) stored, check (b >= 0))",
        )
        .unwrap();
    session.run("insert into t (id, a) values (1, 5), (2, 3)").unwrap();
}

#[test]
fn mixed_outcome_applies_only_valid_rows() {
    let mut session = Session::new();
    setup(&mut session);

    // id=1: a -> -10 makes b = -20, violating the CHECK (skipped under
    // IGNORE). id=2: a -> 4 makes b = 8 (applied).
    assert_eq!(
        try_sql(
            &mut session,
            "update ignore t set a = case when id = 1 then a * -2 else a + 1 end \
             where id in (1, 2)"
        ),
        "affected 1"
    );

    // The violating row keeps its original values; the valid row updated.
    assert_eq!(
        rows(&mut session, "select id, a, b from t order by id"),
        "i:1|i:5|i:10;i:2|i:4|i:8"
    );
}
