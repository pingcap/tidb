//! INSERT IGNORE downgrades foreign-key violations to a skipped row: the
//! plain insert refuses with 1452, the ignored one affects 0 rows and
//! stores nothing, and a valid ignored insert still lands.

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

#[test]
fn ignore_skips_fk_violating_rows() {
    let mut session = Session::new();
    session.run("create table p (id int primary key)").unwrap();
    session
        .run("create table c (id int primary key, pid int, foreign key (pid) references p(id))")
        .unwrap();
    session.run("insert into p values (1)").unwrap();

    // Plain: refused with the FK text.
    let error = try_sql(&mut session, "insert into c values (1, 99)");
    assert!(error.contains("foreign key constraint fails"), "{error}");

    // Ignored: the violating row is skipped, not stored.
    assert_eq!(try_sql(&mut session, "insert ignore into c values (1, 99)"), "affected 0");

    // A valid ignored row still lands.
    assert_eq!(try_sql(&mut session, "insert ignore into c values (1, 1)"), "affected 1");
    assert_eq!(rows(&mut session, "select id, pid from c"), "i:1|i:1");
}
