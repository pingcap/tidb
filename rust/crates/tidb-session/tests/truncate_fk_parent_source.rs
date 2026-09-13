//! TRUNCATE on an FK-referenced parent refuses with MySQL's 1701 text and
//! both tables stay untouched; truncating the referencing child is fine.

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
    session.run("create table p (id int primary key)").unwrap();
    session
        .run("create table c (id int primary key, pid int, foreign key (pid) references p(id))")
        .unwrap();
    session.run("insert into p values (1), (2)").unwrap();
    session.run("insert into c values (1, 1)").unwrap();
}

#[test]
fn truncate_fk_parent_refuses() {
    let mut session = Session::new();
    setup(&mut session);

    let error = try_sql(&mut session, "truncate table p");
    assert!(
        error.contains("referenced in a foreign key constraint"),
        "{error}"
    );

    // Both tables untouched by the failed truncate.
    assert_eq!(rows(&mut session, "select count(*) from p"), "i:2");
    assert_eq!(rows(&mut session, "select count(*) from c"), "i:1");

    // Truncating the child is allowed (DDL reports affected 0).
    assert_eq!(try_sql(&mut session, "truncate table c"), "affected 0");
    assert_eq!(rows(&mut session, "select count(*) from c"), "i:0");
}
