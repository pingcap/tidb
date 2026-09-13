//! FK ON UPDATE SET NULL: renaming the parent's key (1 -> 10) NULLs the
//! referencing child's FK column instead of cascading or refusing.

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
             foreign key (pid) references p(id) on update set null)",
        )
        .unwrap();
    session.run("insert into p values (1)").unwrap();
    session.run("insert into c values (1, 1)").unwrap();
}

#[test]
fn parent_key_update_nulls_children() {
    let mut session = Session::new();
    setup(&mut session);

    session.run("update p set id = 10 where id = 1").unwrap();

    // The child's pid NULLs; the parent carries the new key.
    assert_eq!(rows(&mut session, "select id, pid from c order by id"), "i:1|Null");
    assert_eq!(rows(&mut session, "select id from p"), "i:10");
}
