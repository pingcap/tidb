//! FK ON DELETE CASCADE: deleting the parent deletes exactly the children
//! that referenced it.

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
             foreign key (pid) references p(id) on delete cascade)",
        )
        .unwrap();
    session.run("insert into p values (1), (2)").unwrap();
    session.run("insert into c values (1, 1), (2, 1), (3, 2)").unwrap();
}

#[test]
fn parent_delete_cascades_to_children() {
    let mut session = Session::new();
    setup(&mut session);

    session.run("delete from p where id = 1").unwrap();

    // Children 1 and 2 cascade away; child 3 (parent 2) survives.
    assert_eq!(rows(&mut session, "select id, pid from c order by id"), "i:3|i:2");
}
