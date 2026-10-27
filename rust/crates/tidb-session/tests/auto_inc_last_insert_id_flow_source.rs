//! The auto-increment -> LAST_INSERT_ID() statement flow: an ALLOCATED id is
//! published (`e.lastInsertID`, first-allocated wins) and becomes what
//! `LAST_INSERT_ID()` reads after the statement boundary; an EXPLICIT value
//! only feeds the OK packet's fallback (`StmtCtx.InsertID`) and never moves
//! the function (insert_common.go:1011, :1482).

use crate::support::testkit::TestKit;

#[test]
fn allocated_ids_flow_to_last_insert_id() {
    let mut tk = TestKit::new();
    tk.must_exec("create table t (a int auto_increment primary key, b varchar(3))");

    // INSERT (b) VALUES ('x'): id 1 is allocated and published.
    tk.must_exec("insert into t (b) values ('x')");
    tk.must_query("select last_insert_id()").check(["1"]);

    // INSERT (a, b) VALUES (5, 'y'): the EXPLICIT value must NOT move the
    // function -- the next read still answers the previous allocation.
    tk.must_exec("insert into t (a, b) values (5, 'y')");
    tk.must_query("select last_insert_id()").check(["1"]);

    // INSERT (b) VALUES ('z'): the NEXT allocation (6) is published.
    tk.must_exec("insert into t (b) values ('z')");
    tk.must_query("select last_insert_id()").check(["6"]);
}
