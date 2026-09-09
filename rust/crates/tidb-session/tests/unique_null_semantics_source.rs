//! Unique indexes treat NULLs as distinct: two NULL `b` values coexist,
//! while a second concrete duplicate of `b = 1` fails with Go's
//! "Duplicate entry '1' for key 't.b'".

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
fn multiple_nulls_pass_a_real_duplicate_fails() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b int unique)")
        .unwrap();
    session.run("insert into t values (1, NULL), (2, NULL)").unwrap();
    assert_eq!(
        rows(&mut session, "select a, b from t order by a"),
        "1|NULL;2|NULL",
        "NULLs are distinct for unique enforcement"
    );

    session.run("insert into t values (3, 1)").unwrap();
    let error = session
        .run("insert into t values (4, 1)")
        .expect_err("the second b=1 is a real duplicate");
    assert!(
        error.to_string().contains("Duplicate entry '1' for key 't.b'"),
        "{error}"
    );
    assert_eq!(
        rows(&mut session, "select a, b from t order by a"),
        "1|NULL;2|NULL;3|1"
    );
}
