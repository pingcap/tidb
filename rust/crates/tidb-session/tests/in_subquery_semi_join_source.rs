//! IN and NOT IN with a subquery operand work (unlike the recorded
//! uncorrelated-scalar-subquery family): `ref in (select ref from v)` keeps
//! the matching rows, `not in` keeps the rest.

use tidb_session::Session;

use crate::support::debug_rows_with_sql as rows;

#[test]
fn in_and_not_in_subqueries() {
    let mut session = Session::new();
    session
        .run("create table u (id int primary key, ref int)")
        .unwrap();
    session.run("create table v (ref int)").unwrap();
    session.run("insert into u values (1, 10), (2, 20), (3, 30)").unwrap();
    session.run("insert into v values (10), (30)").unwrap();

    assert_eq!(
        rows(&mut session, "select id from u where ref in (select ref from v) order by id"),
        "Int(1);Int(3)"
    );
    assert_eq!(
        rows(&mut session, "select id from u where ref not in (select ref from v) order by id"),
        "Int(2)"
    );
}
