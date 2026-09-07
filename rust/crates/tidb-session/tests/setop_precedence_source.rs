//! Set-operation precedence: INTERSECT binds tighter than UNION. The query
//! `{v=1} UNION {v in (2,3)} INTERSECT {v=2}` therefore evaluates as
//! `{1} UNION ({2,3} INTERSECT {2})` = {1, 2} — not the left-to-right {2}.

use tidb_session::Session;

fn sorted_rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => {
            let mut values: Vec<String> = rows
                .into_iter()
                .map(|row| {
                    row.iter()
                        .map(|d| match d {
                            tidb_datatype::Datum::Int(v) => format!("{v}"),
                            other => format!("{other:?}"),
                        })
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect();
            values.sort();
            values.join(";")
        }
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", e.to_string()),
    }
}

fn setup(session: &mut Session) {
    session.run("create table t (v int)").unwrap();
    session.run("insert into t values (1), (2), (3)").unwrap();
}

#[test]
fn intersect_binds_tighter_than_union() {
    let mut session = Session::new();
    setup(&mut session);

    assert_eq!(
        sorted_rows(
            &mut session,
            "select 1 from t where v = 1 union \
             select v from t where v in (2, 3) intersect select v from t where v = 2"
        ),
        "1;2"
    );
}
