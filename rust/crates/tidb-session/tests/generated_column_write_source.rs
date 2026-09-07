//! Stored generated columns in writes: assigning one directly fails with
//! Go's `ErrBadGeneratedColumn` (3105, "The value specified for generated
//! column ... is not allowed."), and an UPDATE that moves the base column
//! regenerates the stored value.

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
fn generated_columns_reject_writes_and_regenerate_on_update() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, b int as (a * 2) stored, v int)")
        .unwrap();

    // Assigning the generated column directly is refused (3105).
    let error = session
        .run("insert into t (a, b, v) values (1, 99, 5)")
        .expect_err("a generated column cannot be assigned");
    assert!(
        error
            .to_string()
            .contains("The value specified for generated column 'b' in table 't' is not allowed."),
        "{error}"
    );

    // The normal path: a plain insert materializes `b`, and moving `a`
    // regenerates it.
    session.run("insert into t (a, v) values (3, 8)").unwrap();
    assert_eq!(rows(&mut session, "select a, b from t"), "3|6");
    session.run("update t set a = 10 where a = 3").unwrap();
    assert_eq!(
        rows(&mut session, "select a, b, v from t order by a"),
        "10|20|8",
        "the stored value follows its base column"
    );
}
