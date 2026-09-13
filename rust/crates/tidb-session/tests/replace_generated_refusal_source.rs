//! REPLACE with an explicit generated-column target refuses with the same
//! ErrBadGeneratedColumn text as INSERT/UPDATE — the write guard covers
//! all three statements.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
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
        .run("create table t (id int primary key, a int, b int as (a * 2) stored)")
        .unwrap();
    session.run("replace into t (id, a) values (1, 5)").unwrap();
}

#[test]
fn replace_refuses_generated_column_target() {
    let mut session = Session::new();
    setup(&mut session);

    let error = try_sql(&mut session, "replace into t (id, a, b) values (2, 3, 6)");
    assert!(
        error.contains("The value specified for generated column 'b' in table 't' is not allowed"),
        "{error}"
    );

    // The refused REPLACE stored nothing.
    assert_eq!(rows(&mut session, "select id, a, b from t order by id"), "i:1|i:5|i:10");
}
