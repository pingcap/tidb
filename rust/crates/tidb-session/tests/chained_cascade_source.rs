//! A chained two-level FK cascade: deleting the grandparent (a=1) cascades
//! through the child (b=1) to the grandchild (c=1) — all three tables end
//! empty.

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
    session.run("create table a (id int primary key)").unwrap();
    session
        .run(
            "create table b (id int primary key, aid int, \
             foreign key (aid) references a(id) on delete cascade)",
        )
        .unwrap();
    session
        .run(
            "create table c (id int primary key, bid int, \
             foreign key (bid) references b(id) on delete cascade)",
        )
        .unwrap();
    session.run("insert into a values (1)").unwrap();
    session.run("insert into b values (1, 1)").unwrap();
    session.run("insert into c values (1, 1)").unwrap();
}

#[test]
fn cascade_chains_through_two_levels() {
    let mut session = Session::new();
    setup(&mut session);

    session.run("delete from a where id = 1").unwrap();

    assert_eq!(rows(&mut session, "select count(*) from a"), "i:0");
    assert_eq!(rows(&mut session, "select count(*) from b"), "i:0");
    assert_eq!(rows(&mut session, "select count(*) from c"), "i:0");
}
