//! Standalone index statements: `CREATE INDEX`, `CREATE UNIQUE INDEX`, and
//! `DROP INDEX ... ON ...` all work, and a unique index created this way
//! still enforces uniqueness on later writes.

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
        other => panic!("expected rows, got {other:?}"),
    }
}

fn seed(session: &mut Session) {
    session.run("create table t (a int primary key, b int)").unwrap();
    session.run("insert into t values (1, 10), (2, 20)").unwrap();
}

#[test]
fn standalone_index_statements_enforce_uniqueness() {
    let mut session = Session::new();
    seed(&mut session);

    // Standalone CREATE INDEX.
    session.run("create index kb on t (b)").unwrap();
    // CREATE UNIQUE INDEX enforces uniqueness on later writes.
    session.run("create unique index uqb on t (b)").unwrap();
    let error = session
        .run("insert into t values (3, 10)")
        .expect_err("b=10 is a duplicate under uqb");
    assert!(error.to_string().contains("Duplicate entry"), "{error}");

    // DROP INDEX ON removes the index statement's own name.
    session.run("drop index kb on t").unwrap();
    assert_eq!(rows(&mut session, "select count(*) from t"), "2");
}
