//! SHOW CREATE prints the MySQL-compatible `AUTO_INCREMENT=<n>` table
//! option from Go's `NextGlobalAutoID` (`executor/show.go:1376-1387`): the
//! first id beyond every reserved range, so one insert at the production
//! step (30000) prints 30001 and a fresh table prints nothing. Every number
//! below is Go's own output.

use tidb_session::Session;

use crate::support::byte_lines as strings;

#[test]
fn show_create_prints_auto_increment_option() {
    let mut session = Session::new();
    session
        .run("create table t (id int auto_increment primary key, v int)")
        .unwrap();

    // Fresh table: the allocator's next value is 1, so nothing prints.
    assert!(
        !strings(&mut session, "show create table t").contains("AUTO_INCREMENT="),
        "fresh table must not print AUTO_INCREMENT"
    );

    session.run("insert into t (v) values (1), (2)").unwrap();
    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("AUTO_INCREMENT=30001"), "{shown}");

    // The option rides outside the version gate (plain MySQL-compatible text).
    assert!(!shown.contains("/*T![auto_inc"), "{shown}");
}

#[test]
fn alter_auto_increment_rebases_and_shows() {
    let mut session = Session::new();
    session
        .run("create table t (id int auto_increment primary key, v int)")
        .unwrap();
    session.run("insert into t (v) values (1)").unwrap();
    // Below `NextGlobalAutoID` (30001) the request is raised to it.
    session.run("alter table t auto_increment = 100").unwrap();
    assert_eq!(
        session
            .warnings()
            .iter()
            .map(|warning| (warning.code, warning.message.clone()))
            .collect::<Vec<_>>(),
        [(
            1105,
            "Can't reset AUTO_INCREMENT to 100 without FORCE option, using 30001 instead".to_owned()
        )]
    );
    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("AUTO_INCREMENT=30001"), "{shown}");

    // The dropped allocator starts a fresh reservation at the new base.
    session.run("insert into t (v) values (2)").unwrap();
    let shown = strings(&mut session, "show create table t");
    assert!(shown.contains("AUTO_INCREMENT=60001"), "{shown}");
    assert_eq!(
        session.run("select id from t order by id").unwrap(),
        tidb_session::StmtResult::Rows(vec![
            vec![tidb_datatype::Datum::Int(1)],
            vec![tidb_datatype::Datum::Int(30001)],
        ])
    );

    // On a table that has drawn nothing the counter may move to 100.
    session
        .run("create table u (id int auto_increment primary key, v int)")
        .unwrap();
    session.run("alter table u auto_increment = 100").unwrap();
    assert!(session.warnings().is_empty());
    session.run("insert into u (v) values (1)").unwrap();
    assert_eq!(
        session.run("select id from u").unwrap(),
        tidb_session::StmtResult::Rows(vec![vec![tidb_datatype::Datum::Int(100)]])
    );
    let shown = strings(&mut session, "show create table u");
    assert!(shown.contains("AUTO_INCREMENT=30100"), "{shown}");
}
