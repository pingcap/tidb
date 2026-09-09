//! ODKU assigning to a generated column is refused with the same
//! ErrBadGeneratedColumn text as INSERT/UPDATE (`generated_write_refusal_
//! source.rs`) — the write guard covers the ON DUPLICATE KEY UPDATE path.

use tidb_session::Session;

fn error(session: &mut Session, sql: &str) -> String {
    session.run(sql).expect_err(sql).to_string()
}

#[test]
fn odku_cannot_assign_generated_column() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, k int unique, b int as (id * 2) virtual)")
        .unwrap();
    session.run("insert into t (id, k) values (1, 100)").unwrap();

    assert_eq!(
        error(
            &mut session,
            "insert into t (id, k) values (1, 100) on duplicate key update b = 99"
        ),
        "The value specified for generated column 'b' in table 't' is not allowed."
    );
}
