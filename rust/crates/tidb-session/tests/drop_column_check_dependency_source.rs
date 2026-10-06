//! DROP COLUMN versus dependent CHECKs: a CHECK whose SOLE dependency is
//! the dropped column is removed along with it (Go's lazy invalidation),
//! while a CHECK spanning OTHER columns too refuses the drop with Go's
//! 3959 "uses column ..., hence column cannot be dropped or renamed".

use tidb_session::Session;

use crate::support::byte_lines as strings;

fn setup(session: &mut Session, sql: &str) {
    session
        .run("set global tidb_enable_check_constraint = on")
        .unwrap();
    session.run(sql).unwrap();
}

#[test]
fn sole_dependency_drop_removes_the_constraint() {
    let mut session = Session::new();
    setup(
        &mut session,
        "create table t (a int primary key, b int, constraint pos_b check (b > 0))",
    );

    // The CHECK's only dependency is `b`: Go drops it with the column
    // (lazy invalidation).
    session.run("alter table t drop column b").unwrap();
    let shown = strings(&mut session, "show create table t");
    assert!(!shown.contains("pos_b"), "the constraint is gone: {shown}");
}

#[test]
fn multi_column_check_blocks_the_drop() {
    let mut session = Session::new();
    setup(
        &mut session,
        "create table t (a int primary key, b int, constraint pair check (a + b > 0))",
    );

    let error = session
        .run("alter table t drop column b")
        .expect_err("the multi-column CHECK blocks the drop");
    assert!(
        error
            .to_string()
            .contains("Check constraint 'pair' uses column 'b', hence column cannot be dropped or renamed."),
        "{error}"
    );
}
