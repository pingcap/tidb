//! Mixed set-operation chains and output naming: `a UNION b EXCEPT c` is
//! left-associative ((a∪b)−c, deduped), and the result columns take the
//! FIRST arm's names.

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
    session.run("create table a (v int)").unwrap();
    session.run("insert into a values (1), (2), (3)").unwrap();
    session.run("create table b (v int)").unwrap();
    session.run("insert into b values (2), (3), (4)").unwrap();
    session.run("create table c (v int)").unwrap();
    session.run("insert into c values (3), (5)").unwrap();
}

#[test]
fn left_assoc_chain_and_first_arm_names() {
    let mut session = Session::new();
    seed(&mut session);

    // (a UNION b) = {1,2,3,4}; EXCEPT c {3,5} = {1,2,4}.
    assert_eq!(
        rows(
            &mut session,
            "select v from a union select v from b except select v from c order by v"
        ),
        "1;2;4"
    );

    // The output column keeps the FIRST arm's alias.
    match session
        .run_with_columns("select v as first_name from a union select v from b")
        .unwrap()
    {
        tidb_session::StmtOutput::Rows { columns, rows } => {
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].0, "first_name");
            assert_eq!(rows.len(), 4, "a union b dedups the shared values");
        }
        other => panic!("expected rows, got {other:?}"),
    }
}
