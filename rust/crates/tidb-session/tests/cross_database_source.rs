//! Cross-schema statements: creating a second schema, schema-qualified DDL
//! and DML, `USE` switching, qualified reads from both schemas, and a join
//! across schemas.

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
fn cross_schema_reads_writes_and_join() {
    let mut session = Session::new();
    session.run("create table t (a int primary key)").unwrap();
    session.run("insert into t values (1)").unwrap();

    session.run("create database other").unwrap();
    session.run("create table other.u (a int primary key)").unwrap();
    session.run("insert into other.u values (99)").unwrap();

    // USE switches the current schema.
    session.run("use other").unwrap();
    assert_eq!(rows(&mut session, "select a from u"), "99");
    // ...but schema-qualified reads still reach the first schema.
    assert_eq!(rows(&mut session, "select a from test.t"), "1");

    // A join across schemas pairs the matching rows.
    session.run("insert into other.u values (1)").unwrap();
    assert_eq!(
        rows(&mut session, "select t.a, u.a from test.t join other.u on t.a = u.a"),
        "1|1"
    );
}
