//! DROP INDEX needed by a foreign key: Go refuses with 1553 "Cannot drop
//! index 'ik': needed in a foreign key constraint" — the FK's parent-side
//! index lookup depends on it.

use tidb_session::Session;

fn setup(session: &mut Session) {
    session.run("create table p (a int primary key)").unwrap();
    session.run("create table c (x int primary key, pa int)").unwrap();
    session.run("alter table c add index ik (pa)").unwrap();
    session
        .run("alter table c add foreign key (pa) references p(a)")
        .unwrap();
}

#[test]
fn fk_needed_index_cannot_be_dropped() {
    let mut session = Session::new();
    setup(&mut session);

    let error = session
        .run("alter table c drop index ik")
        .expect_err("the FK needs this index");
    assert!(
        error
            .to_string()
            .contains("Cannot drop index 'ik': needed in a foreign key constraint"),
        "{error}"
    );
}
