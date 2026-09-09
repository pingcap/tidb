//! Go `checkTTLInfoValid` (`pkg/ddl/ttl.go:104-107`): a table another
//! table's foreign key refers to cannot take a TTL config — 8152 "Set TTL
//! for a table referenced by foreign key is not allowed" — on both CREATE
//! and ALTER.

use tidb_session::Session;

fn error(session: &mut Session, sql: &str) -> String {
    session.run(sql).expect_err(sql).to_string()
}

#[test]
fn ttl_on_fk_referred_table_refused() {
    let mut session = Session::new();
    session
        .run("create table p (id int primary key, created datetime)")
        .unwrap();
    session
        .run(
            "create table c (id int primary key, pid int, \
             foreign key (pid) references p(id))",
        )
        .unwrap();

    // ALTER adding the config to the referred parent: refused.
    assert_eq!(
        error(&mut session, "alter table p ttl = created + interval 3 month"),
        "Set TTL for a table referenced by foreign key is not allowed"
    );

    // Enable-only and interval-only forms are NOT the referred check — Go
    // only runs it with a full definition; here the parent has no config,
    // so they refuse with 8150 instead.
    assert_eq!(
        error(&mut session, "alter table p ttl_enable = 'ON'"),
        "Cannot set TTL_ENABLE on a table without TTL config"
    );

    // The child table (referring, not referred) takes a config fine.
    session
        .run(
            "create table c2 (id int primary key, created datetime) \
             ttl = created + interval 3 month",
        )
        .unwrap();
}
