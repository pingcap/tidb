//! LAST_INSERT_ID after a multi-row insert reports the FIRST generated id
//! of the batch (MySQL's batch convention), not the last.

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Affected(n)) => format!("affected {n}"),
        Ok(tidb_session::StmtResult::Rows(rows)) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::UInt(v) => format!("u:{v}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", e.to_string()),
    }
}

fn setup(session: &mut Session) {
    session
        .run("create table t (id int unsigned auto_increment primary key, v int)")
        .unwrap();
    session.run("insert into t (v) values (1)").unwrap();
}

#[test]
fn multirow_insert_reports_first_generated_id() {
    let mut session = Session::new();
    setup(&mut session);

    // The single insert stamps 1.
    assert_eq!(try_sql(&mut session, "select last_insert_id()"), "u:1");

    // The three-row insert generates 2, 3, 4; LAST_INSERT_ID stays at 2 —
    // the FIRST id of the batch.
    assert_eq!(try_sql(&mut session, "insert into t (v) values (2), (3), (4)"), "affected 3");
    assert_eq!(try_sql(&mut session, "select last_insert_id()"), "u:2");
    assert_eq!(
        try_sql(&mut session, "select id from t order by id"),
        "u:1;u:2;u:3;u:4"
    );
}
