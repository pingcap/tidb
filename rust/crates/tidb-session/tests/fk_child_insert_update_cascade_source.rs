//! FK child-row guards and ON UPDATE actions: inserting a child row whose
//! parent is missing fails with Go's `ErrNoReferencedRow2` (1452, carrying
//! the constraint detail), and `ON UPDATE CASCADE` re-points dependents when
//! the parent key changes.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
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
fn orphan_insert_and_update_cascade() {
    let mut session = Session::new();
    session.run("create table p (a int primary key)").unwrap();
    session
        .run("create table c (x int primary key, pa int, foreign key (pa) references p(a))")
        .unwrap();
    session.run("insert into p values (1)").unwrap();

    // A child row referencing a missing parent is refused (1452).
    let error = session
        .run("insert into c values (10, 99)")
        .expect_err("an orphan child row must be refused");
    let rendered = error.to_string();
    assert!(
        rendered.contains("Cannot add or update a child row: a foreign key constraint fails"),
        "{rendered}"
    );
    assert!(rendered.contains("`test`.`c`") && rendered.contains("`fk_1`"), "{rendered}");

    // ON UPDATE CASCADE re-points dependents when the parent key moves.
    session
        .run("create table c2 (x int primary key, pa int, foreign key (pa) references p(a) on update cascade)")
        .unwrap();
    session.run("insert into c2 values (10, 1)").unwrap();
    session.run("update p set a = 7 where a = 1").unwrap();
    assert_eq!(rows(&mut session, "select x, pa from c2 order by x"), "10|7");
}
