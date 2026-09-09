//! A two-column foreign key: the child row must match BOTH columns as a
//! pair — a half-matching pair (fa=1 exists, fb=2 pairs with a=2 only)
//! refuses with the child-row FK text.

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
        .run("create table p (a int, b int, primary key (a, b))")
        .unwrap();
    session
        .run(
            "create table c (id int primary key, fa int, fb int, \
             foreign key (fa, fb) references p(a, b))",
        )
        .unwrap();
    session.run("insert into p values (1, 1), (2, 2)").unwrap();
    session.run("insert into c values (1, 1, 1)").unwrap();
}

#[test]
fn composite_fk_checks_both_columns() {
    let mut session = Session::new();
    setup(&mut session);

    // (fa=1, fb=2) is not a parent pair even though each half exists.
    let error = try_sql(&mut session, "insert into c values (2, 1, 2)");
    assert!(error.contains("a foreign key constraint fails"), "{error}");

    // The matching pair lands.
    assert_eq!(try_sql(&mut session, "insert into c values (3, 2, 2)"), "affected 1");
    assert_eq!(rows(&mut session, "select * from c order by id"), "i:1|i:1|i:1;i:3|i:2|i:2");
}
