//! ADD FOREIGN KEY validates EXISTING rows: if any row violates, the ALTER
//! refuses with the child-row FK text. After the offending row is removed
//! the same ALTER succeeds and new violations refuse again.

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
    session.run("create table c (id int primary key, pid int)").unwrap();
    session.run("insert into p values (1), (2)").unwrap();
    session.run("insert into c values (1, 1), (2, 99)").unwrap();
}

#[test]
fn add_fk_validates_existing_rows() {
    let mut session = Session::new();
    setup(&mut session);

    // The violating existing row (pid=99) refuses the ALTER...
    let error = try_sql(&mut session, "alter table c add foreign key (pid) references p(id)");
    assert!(error.contains("a foreign key constraint fails"), "{error}");
    // ...and the data is untouched.
    assert_eq!(rows(&mut session, "select id, pid from c order by id"), "i:1|i:1;i:2|i:99");

    // Remove the offender; the ALTER now succeeds.
    session.run("delete from c where id = 2").unwrap();
    assert_eq!(
        try_sql(&mut session, "alter table c add foreign key (pid) references p(id)"),
        "affected 0"
    );

    // And the constraint is live again.
    let error = try_sql(&mut session, "insert into c values (3, 99)");
    assert!(error.contains("a foreign key constraint fails"), "{error}");
}
