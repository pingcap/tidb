//! A DEFAULT clause on a generated column is refused with Go's coded
//! ErrWrongUsage 1221 — `Incorrect usage of DEFAULT and generated
//! column` — NOT the generic 1064 syntax error (verified against the Go
//! parser: `[ddl:1221]Incorrect usage of DEFAULT and generated column`).

use tidb_session::Session;

fn error(session: &mut Session, sql: &str) -> String {
    session.run(sql).expect_err(sql).to_string()
}

#[test]
fn default_on_generated_is_coded_1221() {
    let mut session = Session::new();

    for sql in [
        "create table t (a int, b int as (a * 2) default 5)",
        "create table t (a int, b int as (a * 2) virtual default 5)",
        "create table t (a int, b int as (a * 2) stored default 5)",
    ] {
        let error = error(&mut session, sql);
        assert!(
            error.contains("Incorrect usage of DEFAULT and generated column"),
            "{sql}: {error}"
        );
        assert!(
            !error.contains("You have an error in your SQL syntax"),
            "{sql}: {error}"
        );
    }
}
