//! `checkTTLInfoValid` (`pkg/ddl/ttl.go`): the TTL config's column must
//! exist (1054 "Unknown column 'x' in 'TTL config'"), must be a time type
//! (8148), and cannot be dropped while the config stands (8149).

use tidb_session::Session;

fn error(session: &mut Session, sql: &str) -> String {
    session.run(sql).expect_err(sql).to_string()
}

#[test]
fn ttl_column_must_be_an_existing_time_column() {
    let mut session = Session::new();

    // (a) TTL on an INT column: 8148.
    assert_eq!(
        error(
            &mut session,
            "create table t (id int primary key) ttl = id + interval 3 month"
        ),
        "Field 'id' is of a not supported type for TTL config, expect DATETIME, DATE or TIMESTAMP"
    );

    // (b) TTL on a missing column: 1054 with the TTL clause name.
    assert_eq!(
        error(
            &mut session,
            "create table t2 (id int primary key) ttl = gone + interval 3 month"
        ),
        "Unknown column 'gone' in 'TTL config'"
    );

    // (c) A DATETIME TTL column is accepted.
    session
        .run(
            "create table t3 (id int primary key, created datetime) \
             ttl = created + interval 3 month",
        )
        .unwrap();
}

#[test]
fn ttl_column_cannot_be_dropped() {
    let mut session = Session::new();
    session
        .run(
            "create table t (id int primary key, created datetime) \
             ttl = created + interval 3 month",
        )
        .unwrap();

    assert_eq!(
        error(&mut session, "alter table t drop column created"),
        "Cannot drop column 'created': needed in TTL config"
    );

    // Dropping a column the config does NOT name still works.
    session
        .run(
            "create table t4 (id int primary key, created datetime, note varchar(8)) \
             ttl = created + interval 3 month",
        )
        .unwrap();
    session.run("alter table t4 drop column note").unwrap();
}
