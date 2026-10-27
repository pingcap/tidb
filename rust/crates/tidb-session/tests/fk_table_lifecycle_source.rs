//! FK-guarded table lifecycle: with `foreign_key_checks` on, TRUNCATE of a
//! referenced parent fails with Go's ErrTruncateIllegalForeignKey text and
//! DROP with ErrForeignKeyCannotDropParent's, both naming the child (Go
//! `TestTruncateOrDropTableWithForeignKeyReferred`,
//! `pkg/ddl/tests/fk/foreign_key_test.go:801`).

use crate::support::testkit::TestKit;

fn setup() -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec("set @@foreign_key_checks=1");
    tk.must_exec("create table p (a int primary key)");
    tk.must_exec("create table c (x int primary key, y int, foreign key (y) references p(a))");
    tk.must_exec("insert into p values (1)");
    tk.must_exec("insert into c values (10, 1)");
    tk
}

#[test]
fn truncate_of_a_referenced_parent_is_rejected() {
    let mut tk = setup();
    let error = tk.must_get_err_code("truncate table p", 1701);
    assert_eq!(
        error.message,
        "Cannot truncate a table referenced in a foreign key constraint (`test`.`c` CONSTRAINT `fk_1`)"
    );
}

#[test]
fn drop_of_a_referenced_parent_is_rejected() {
    let mut tk = setup();
    let error = tk.must_get_err_code("drop table p", 3730);
    assert_eq!(
        error.message,
        "Cannot drop table 'p' referenced by a foreign key constraint 'fk_1' on table 'c'."
    );
}
