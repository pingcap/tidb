//! Three-part names (`db.table.column`) resolve in the SELECT list, in
//! WHERE predicates, and as schema-qualified INSERT targets.

use tidb_session::Session;

use crate::support::integer_rows as rows;

#[test]
fn three_part_names_resolve_everywhere() {
    let mut session = Session::new();
    session.run("create database pdb").unwrap();
    session.run("create table pdb.t (a int primary key)").unwrap();
    session.run("insert into pdb.t values (1), (2)").unwrap();

    assert_eq!(rows(&mut session, "select pdb.t.a from pdb.t order by a"), "1;2");
    assert_eq!(rows(&mut session, "select pdb.t.a from pdb.t where pdb.t.a > 1"), "2");

    session.run("insert into pdb.t values (9)").unwrap();
    assert_eq!(rows(&mut session, "select count(*) from pdb.t"), "3");
}
