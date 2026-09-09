//! `INSERT ... SELECT` over a UNION source (dedup across the union arms)
//! and a `FOR UPDATE` suffix, which parses and reads normally in the
//! single-session harness.

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

fn seed(session: &mut Session) {
    session.run("create table s1 (a int)").unwrap();
    session.run("insert into s1 values (1), (2)").unwrap();
    session.run("create table s2 (a int)").unwrap();
    session.run("insert into s2 values (3)").unwrap();
    session.run("create table dst (a int primary key)").unwrap();
}

#[test]
fn union_source_insert_and_for_update_read() {
    let mut session = Session::new();
    seed(&mut session);

    // The UNION dedups the overlapping arm values (2 vs 3 don't overlap, so
    // 3 distinct rows land).
    let inserted = match session
        .run("insert into dst select a from s1 union select a from s2")
        .unwrap()
    {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(inserted, 3);
    assert_eq!(rows(&mut session, "select a from dst order by a"), "1;2;3");

    // FOR UPDATE parses and reads normally.
    assert_eq!(rows(&mut session, "select a from dst where a = 1 for update"), "1");
}
