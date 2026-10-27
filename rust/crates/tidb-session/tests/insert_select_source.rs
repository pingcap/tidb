//! `INSERT ... SELECT`: the source query runs over its own tables and the
//! projected values (computed expressions, WHERE filtering) land in the
//! target, with Go's affected-rows accounting.

use crate::support::testkit::TestKit;

#[test]
fn insert_select_with_computed_projection() {
    let mut tk = TestKit::new();
    tk.must_exec("create table src (a int primary key, b varchar(8))");
    tk.must_exec("create table dst (a int primary key, b varchar(8))");
    tk.must_exec("insert into src values (1, 'one'), (2, 'two'), (3, 'three')");

    tk.must_exec("insert into dst select a + 10, concat(b, '!') from src where a <= 2");
    // The WHERE filters row 3 out.
    assert_eq!(tk.affected_rows(), 2);

    tk.must_query("select a from dst order by a").check(["11", "12"]);
    // 'one!' stored.
    tk.must_query("select b from dst order by a limit 1").check(["one!"]);
}
