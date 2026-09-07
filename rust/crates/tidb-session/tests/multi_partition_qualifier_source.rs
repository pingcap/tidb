//! A `PARTITION (p0, p1)` qualifier unions the named partitions' rows and
//! excludes the rest — id % 4 puts 1→p1, 2→p2, 3→p3, 4→p0, so the pair
//! (p0, p1) answers exactly ids 1 and 4.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| format!("{d:?}"))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn qualifier_unions_named_partitions() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key) partition by hash(id) partitions 4")
        .unwrap();
    session.run("insert into t values (1), (2), (3), (4)").unwrap();

    assert_eq!(
        rows(&mut session, "select id from t partition (p0, p1) order by id"),
        "Int(1);Int(4)"
    );
    assert_eq!(rows(&mut session, "select id from t partition (p2)"), "Int(2)");
}
