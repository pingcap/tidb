//! FK ON UPDATE CASCADE: renaming the parent's key (1 -> 10) propagates the
//! new value into the referencing children.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn setup(session: &mut Session) {
    session.run("create table p (id int primary key)").unwrap();
    session
        .run(
            "create table c (id int primary key, pid int, \
             foreign key (pid) references p(id) on update cascade)",
        )
        .unwrap();
    session.run("insert into p values (1), (2)").unwrap();
    session.run("insert into c values (1, 1), (2, 2)").unwrap();
}

#[test]
fn parent_key_update_cascades() {
    let mut session = Session::new();
    setup(&mut session);

    session.run("update p set id = 10 where id = 1").unwrap();

    // Child 1 follows the parent's new id; child 2 is untouched.
    assert_eq!(rows(&mut session, "select id, pid from c order by id"), "i:1|i:10;i:2|i:2");
    assert_eq!(rows(&mut session, "select id from p order by id"), "i:2;i:10");
}
