//! UPDATE IGNORE also downgrades foreign-key violations: the plain UPDATE
//! to a missing parent refuses with the 1452 text and the row is untouched,
//! the ignored violating UPDATE affects 0 rows, and the ignored valid
//! UPDATE lands.

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
fn update_ignore_skips_fk_violations() {
    let mut session = Session::new();
    session.run("create table p (id int primary key)").unwrap();
    session
        .run("create table c (id int primary key, pid int, foreign key (pid) references p(id))")
        .unwrap();
    session.run("insert into p values (1), (2)").unwrap();
    session.run("insert into c values (1, 1)").unwrap();

    // Plain: refused with the FK text, row untouched.
    let error = try_sql(&mut session, "update c set pid = 99 where id = 1");
    assert!(error.contains("foreign key constraint fails"), "{error}");
    assert_eq!(rows(&mut session, "select id, pid from c"), "i:1|i:1");

    // Ignored: the violating update is skipped.
    assert_eq!(
        try_sql(&mut session, "update ignore c set pid = 99 where id = 1"),
        "affected 0"
    );
    assert_eq!(rows(&mut session, "select id, pid from c"), "i:1|i:1");

    // Ignored + valid: lands.
    assert_eq!(
        try_sql(&mut session, "update ignore c set pid = 2 where id = 1"),
        "affected 1"
    );
    assert_eq!(rows(&mut session, "select id, pid from c"), "i:1|i:2");
}
