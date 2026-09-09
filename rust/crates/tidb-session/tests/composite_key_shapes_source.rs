//! Composite key write shapes: a duplicate on a two-column PRIMARY key
//! reports the dash-separated entry ("Duplicate entry '1-x' for key
//! 't.PRIMARY'"), and a REPLACE with no conflicting row counts 1 (plain
//! insert accounting).

use tidb_session::Session;

#[test]
fn composite_pk_and_conflict_free_replace() {
    let mut session = Session::new();
    session
        .run("create table t (a int, b varchar(3), c int, primary key (a, b))")
        .unwrap();
    session.run("insert into t values (1, 'x', 10)").unwrap();

    // The composite duplicate names both key values.
    let error = session
        .run("insert into t values (1, 'x', 99)")
        .expect_err("the composite duplicate must fail");
    assert!(
        error.to_string().contains("Duplicate entry '1-x' for key 't.PRIMARY'"),
        "{error}"
    );

    // A REPLACE with no conflict is just an insert: 1 affected row.
    let affected = match session.run("replace into t values (2, 'y', 20)").unwrap() {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(affected, 1);
}
