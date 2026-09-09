//! FK ON DELETE SET NULL: deleting the parent NULLs every child row that
//! referenced it and leaves the others alone.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Null => "Null".to_owned(),
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
             foreign key (pid) references p(id) on delete set null)",
        )
        .unwrap();
    session.run("insert into p values (1), (2)").unwrap();
    session.run("insert into c values (1, 1), (2, 1), (3, 2)").unwrap();
}

#[test]
fn parent_delete_nulls_referencing_children() {
    let mut session = Session::new();
    setup(&mut session);

    session.run("delete from p where id = 1").unwrap();

    // Children 1 and 2 referenced parent 1 -> NULL; child 3 keeps 2.
    assert_eq!(rows(&mut session, "select id, pid from c order by id"), "i:1|Null;i:2|Null;i:3|i:2");
}
