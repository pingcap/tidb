//! ODKU affected-row accounting follows MySQL's delete+insert convention:
//! an ODKU that CHANGES the duplicate reports 2, one that assigns identical
//! values reports 0.

use tidb_session::Session;

use crate::support::execute as try_sql;

#[test]
fn changed_counts_two_noop_counts_zero() {
    let mut session = Session::new();
    session.run("create table t (id int primary key, v int)").unwrap();
    session.run("insert into t values (1, 10)").unwrap();

    // Changing the duplicate: 1 (the notional insert) + 1 (the update).
    assert_eq!(
        try_sql(
            &mut session,
            "insert into t values (1, 10) on duplicate key update v = 20"
        ),
        "affected 2"
    );

    // Assigning identical values: nothing changed, 0.
    assert_eq!(
        try_sql(
            &mut session,
            "insert into t values (1, 20) on duplicate key update v = 20"
        ),
        "affected 0"
    );
}
