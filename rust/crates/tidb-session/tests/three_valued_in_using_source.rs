//! Three-valued logic in IN/NOT IN and USING joins: `NOT IN` over a list
//! containing NULL answers NOTHING (every comparison is UNKNOWN), `IN`
//! keeps only the real match, and `JOIN ... USING (id)` merges the join
//! column and pairs the sides.

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
fn null_in_list_and_using_join() {
    let mut session = Session::new();
    session.run("create table t (a int)").unwrap();
    session.run("insert into t values (1), (2), (3)").unwrap();
    session.run("create table s (k int)").unwrap();
    session.run("insert into s values (2), (NULL)").unwrap();

    // NULL poisons NOT IN: nothing can be proven "not in".
    assert_eq!(rows(&mut session, "select a from t where a not in (select k from s)"), "");
    // IN keeps the concrete match.
    assert_eq!(rows(&mut session, "select a from t where a in (select k from s)"), "2");

    // USING merges the join column.
    session.run("create table l (id int, v int)").unwrap();
    session.run("create table r (id int, w int)").unwrap();
    session.run("insert into l values (1, 10), (2, 20)").unwrap();
    session.run("insert into r values (2, 99), (3, 30)").unwrap();
    assert_eq!(
        rows(&mut session, "select id, v, w from l join r using (id) order by id"),
        "2|20|99"
    );
}
