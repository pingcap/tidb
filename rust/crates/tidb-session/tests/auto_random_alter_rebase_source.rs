//! Auto-increment statement flows: `ALTER TABLE ... AUTO_INCREMENT = 100`
//! rebases the allocator (the next insert lands at 100), an AUTO_RANDOM
//! column allocates implicitly, and an EXPLICIT insert into an AUTO_RANDOM
//! column is refused with TiDB's message naming the session variable.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn alter_rebases_the_allocator() {
    let mut session = Session::new();
    session
        .run("create table t (id int auto_increment primary key, v int)")
        .unwrap();
    session.run("insert into t (v) values (1)").unwrap();
    session.run("alter table t auto_increment = 100").unwrap();
    session.run("insert into t (v) values (2)").unwrap();
    assert_eq!(rows(&mut session, "select id from t order by id"), "1;100");
}

#[test]
fn auto_random_implicit_allocates_explicit_is_refused() {
    let mut session = Session::new();
    session
        .run("create table r (id bigint auto_random primary key, v int)")
        .unwrap();
    session.run("insert into r (v) values (1)").unwrap();
    assert_eq!(rows(&mut session, "select count(*) from r"), "1");

    let error = session
        .run("insert into r (id, v) values (5, 2)")
        .expect_err("explicit insertion on an auto_random column is disabled");
    assert!(
        error
            .to_string()
            .contains("Explicit insertion on auto_random column is disabled"),
        "{error}"
    );
    assert!(
        error.to_string().contains("allow_auto_random_explicit_insert"),
        "{error}"
    );
}
