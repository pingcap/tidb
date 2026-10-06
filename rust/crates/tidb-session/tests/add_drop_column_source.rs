//! `ALTER TABLE` column lifecycle: ADD COLUMN backfills existing rows with
//! the column default, `AFTER` positions the new column, and DROP COLUMN
//! removes it from every read.

use tidb_session::Session;

use crate::support::quoted_string_integer_rows_with_sql as rows;

#[test]
fn column_add_backfill_position_and_drop() {
    let mut session = Session::new();
    session.run("create table t (a int primary key)").unwrap();
    session.run("insert into t values (1), (2)").unwrap();

    // ADD with a default backfills the existing rows.
    session.run("alter table t add column b int default 7").unwrap();
    assert_eq!(rows(&mut session, "select a, b from t order by a"), "1|7;2|7");

    // AFTER positions the column in the row layout.
    session
        .run("alter table t add column c varchar(3) not null default 'zz' after a")
        .unwrap();
    assert_eq!(rows(&mut session, "select * from t order by a limit 1"), "1|'zz'|7");

    // DROP COLUMN removes it.
    session.run("alter table t drop column b").unwrap();
    assert_eq!(rows(&mut session, "select * from t order by a"), "1|'zz';2|'zz'");
}
