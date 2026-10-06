//! `ALTER TABLE ... ADD COLUMN ... FIRST` positions the new column at the
//! head of the row layout — `select *` returns it before every other
//! column.

use tidb_session::Session;

use crate::support::quoted_string_integer_rows as rows;

#[test]
fn first_positions_the_new_column() {
    let mut session = Session::new();
    session.run("create table t (a int primary key, b int)").unwrap();
    session.run("insert into t values (1, 10)").unwrap();

    session
        .run("alter table t add column z varchar(3) not null default 'zz' first")
        .unwrap();
    assert_eq!(rows(&mut session, "select * from t limit 1"), "'zz'|1|10");
}
