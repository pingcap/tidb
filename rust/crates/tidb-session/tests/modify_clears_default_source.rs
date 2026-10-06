//! MODIFY COLUMN without a DEFAULT clears the previous default (Go
//! `ModifyColumn` overwrites the column options): SHOW CREATE drops the
//! clause and an omitted value now fails with 1364.

use tidb_session::Session;

use crate::support::byte_lines as strings;

#[test]
fn modify_without_default_clears_it() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, v int not null default 5)")
        .unwrap();
    session.run("alter table t modify column v int not null").unwrap();

    // The DEFAULT clause is gone from the column's DDL line.
    let shown = strings(&mut session, "show create table t");
    let v_line = shown.lines().find(|line| line.contains("`v`")).unwrap();
    assert!(!v_line.contains("DEFAULT"), "{shown}");

    // Omitting the column now refuses with Go's 1364 text.
    let error = session
        .run("insert into t (id) values (1)")
        .expect_err("no default")
        .to_string();
    assert_eq!(error, "Field 'v' doesn't have a default value");
}
