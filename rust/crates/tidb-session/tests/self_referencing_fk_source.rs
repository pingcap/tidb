//! A self-referencing foreign key: the root row takes a NULL manager,
//! child rows reference an existing id, a bad manager refuses with the
//! child-row FK text, and deleting the still-referenced root refuses with
//! the parent-row text.

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
        .run(
            "create table emp (id int primary key, mgr int, \
             foreign key (mgr) references emp(id))",
        )
        .unwrap();
    session.run("insert into emp values (1, null)").unwrap();
    session.run("insert into emp values (2, 1)").unwrap();
}

#[test]
fn self_referencing_fk_enforced() {
    let mut session = Session::new();
    setup(&mut session);

    // A bad manager refuses on insert.
    let error = try_sql(&mut session, "insert into emp values (3, 99)");
    assert!(error.contains("a foreign key constraint fails"), "{error}");

    // Deleting the referenced root refuses too.
    let error = try_sql(&mut session, "delete from emp where id = 1");
    assert!(error.contains("Cannot delete or update a parent row"), "{error}");

    // Both rows survive.
    assert_eq!(rows(&mut session, "select id, mgr from emp order by id"), "i:1|Null;i:2|i:1");
}
