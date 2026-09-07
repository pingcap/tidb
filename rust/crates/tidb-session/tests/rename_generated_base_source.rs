//! RENAME COLUMN of a generated column's base is refused with Go's
//! `ErrDependentByGeneratedColumn` text (`pkg/ddl/executor.go:3509` ->
//! `checkModifyColumnWithGeneratedColumnsConstraint`,
//! `pkg/ddl/modify_column.go:1415`) — TiDB does not rewrite the expression.

use tidb_session::Session;

fn error(session: &mut Session, sql: &str) -> String {
    session.run(sql).expect_err(sql).to_string()
}

#[test]
fn generated_base_column_cannot_be_renamed() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, a int, b int as (a * 2) virtual)")
        .unwrap();

    assert_eq!(
        error(&mut session, "alter table t rename column a to src"),
        "Column 'a' has a generated column dependency."
    );
}
