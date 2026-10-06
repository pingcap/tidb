//! ALTER TABLE ... DROP FOREIGN KEY removes only the constraint: the same
//! insert that refused before now lands — the row violating the removed
//! constraint is stored.

use tidb_session::Session;

use crate::support::tagged_integer_rows as rows;

use crate::support::execute as try_sql;

fn setup(session: &mut Session) {
    session.run("create table p (id int primary key)").unwrap();
    session
        .run(
            "create table c (id int primary key, pid int, \
             constraint fk_c foreign key (pid) references p(id))",
        )
        .unwrap();
    session.run("insert into p values (1)").unwrap();
}

#[test]
fn drop_foreign_key_re_enables_violations() {
    let mut session = Session::new();
    setup(&mut session);

    // Before the drop: the violating insert refuses.
    let error = try_sql(&mut session, "insert into c values (1, 99)");
    assert!(error.contains("a foreign key constraint fails"), "{error}");

    session.run("alter table c drop foreign key fk_c").unwrap();

    // After the drop: the same insert lands.
    assert_eq!(try_sql(&mut session, "insert into c values (1, 99)"), "affected 1");
    assert_eq!(rows(&mut session, "select id, pid from c"), "i:1|i:99");
}
