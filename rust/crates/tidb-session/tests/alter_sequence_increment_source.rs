//! `ALTER SEQUENCE s INCREMENT BY 10` applies the new increment to
//! subsequent NEXTVAL calls: consecutive draws differ by exactly the new
//! step. (The absolute first value after the alter depends on TiDB's
//! cache-block alignment, so the pin asserts the step, not the level.)

use tidb_session::Session;

fn nextval(session: &mut Session) -> i64 {
    match session.run("select nextval(s)").unwrap() {
        tidb_session::StmtResult::Rows(rows) => match rows[0][0] {
            tidb_datatype::Datum::Int(v) => v,
            ref other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

#[test]
fn alter_sequence_changes_the_step() {
    let mut session = Session::new();
    session.run("create sequence s increment 1 start with 10").unwrap();

    let first = nextval(&mut session);
    session.run("alter sequence s increment 10").unwrap();
    let a = nextval(&mut session);
    let b = nextval(&mut session);

    // The old increment produced consecutive integers (10); the new one
    // produces values spaced by 10.
    assert_eq!(first, 10);
    assert_eq!(b - a, 10);
}
