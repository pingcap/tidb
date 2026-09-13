//! An explicitly assigned value wins over ON UPDATE CURRENT_TIMESTAMP: the
//! column keeps the fixed value. A LATER update that omits the column
//! fires ON UPDATE and stamps the current time (asserted by the value
//! changing, not by any wall-clock reading).

use tidb_session::Session;

fn updated(session: &mut Session) -> String {
    match session.run("select updated from t").unwrap() {
        tidb_session::StmtResult::Rows(rows) => format!("{:?}", &rows[0][0]),
        other => panic!("{other:?}"),
    }
}

fn setup(session: &mut Session) {
    session
        .run(
            "create table t (id int primary key, updated timestamp \
             default current_timestamp on update current_timestamp)",
        )
        .unwrap();
    session.run("insert into t (id) values (1)").unwrap();
}

#[test]
fn explicit_assignment_wins_then_on_update_fires() {
    let mut session = Session::new();
    setup(&mut session);

    // Explicit assignment: ON UPDATE does not override it.
    session
        .run("update t set updated = '2000-01-01 00:00:00'")
        .unwrap();
    assert!(updated(&mut session).contains("2000 1 1"), "{}", updated(&mut session));

    // An update that OMITS the column fires ON UPDATE: the stamp changes.
    session.run("update t set id = id + 1").unwrap();
    let stamped = updated(&mut session);
    assert!(stamped.contains("kind: Timestamp"), "{stamped}");
    assert!(!stamped.contains("2000 1 1"), "{stamped}");
}
