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
                        tidb_datatype::Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
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
fn not_in_null_semantics_by_case() {
    let mut failures = Vec::new();
    for (name, right, predicate, expected) in [
        ("empty", "", "a not in (select k from s)", "1;2;3;4"),
        ("non_null", "(2)", "a not in (select k from s)", "1;3"),
        ("rhs_null", "(2),(NULL)", "a not in (select k from s)", ""),
        ("exists", "(2),(NULL)", "not exists (select 1 from s where k=a)", "1;3;4"),
    ] {
        let mut session = Session::new();
        session.run("create table t (id int, a int)").unwrap();
        session.run("insert into t values (1,1),(2,2),(3,3),(4,NULL)").unwrap();
        session.run("create table s (k int)").unwrap();
        if !right.is_empty() {
            session.run(&format!("insert into s values {right}")).unwrap();
        }
        let sql = format!("select id from t where {predicate} order by id");
        let plan = rows(&mut session, &format!("explain {sql}"));
        let actual = rows(&mut session, &sql);
        if actual != expected {
            failures.push(format!("{name}: expected {expected:?}, got {actual:?}; plan: {plan}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
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
