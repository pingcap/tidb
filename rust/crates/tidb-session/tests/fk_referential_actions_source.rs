//! FK referential actions: `ON DELETE CASCADE` removes the dependent rows
//! when their parent goes, and `ON DELETE SET NULL` nulls the referencing
//! column instead — only rows referencing the DELETED parent are touched.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::Null => "NULL".to_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn on_delete_cascade_removes_dependents() {
    let mut session = Session::new();
    session.run("create table p (a int primary key)").unwrap();
    session
        .run("create table c (x int primary key, pa int, foreign key (pa) references p(a) on delete cascade)")
        .unwrap();
    session.run("insert into p values (1), (2)").unwrap();
    session.run("insert into c values (10, 1), (11, 1), (12, 2)").unwrap();

    session.run("delete from p where a = 1").unwrap();
    assert_eq!(
        rows(&mut session, "select x, pa from c order by x"),
        "12|2",
        "only the dependents of the deleted parent cascade away"
    );
}

#[test]
fn on_delete_set_null_nulls_the_reference() {
    let mut session = Session::new();
    session.run("create table p (a int primary key)").unwrap();
    session
        .run("create table c (x int primary key, pa int, foreign key (pa) references p(a) on delete set null)")
        .unwrap();
    session.run("insert into p values (1), (2)").unwrap();
    session.run("insert into c values (10, 1), (12, 2)").unwrap();

    session.run("delete from p where a = 1").unwrap();
    assert_eq!(
        rows(&mut session, "select x, pa from c order by x"),
        "10|NULL;12|2",
        "the referencing column is NULLed; the other child is untouched"
    );
}
