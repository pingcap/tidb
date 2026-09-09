//! `checkPrimaryKeyForTTLTable` (`pkg/ddl/ttl.go:155-168`): a TTL table
//! whose CLUSTERED primary key contains a FLOAT or DOUBLE column is refused
//! with 8153; an integer or non-clustered PK is fine.

use tidb_session::Session;

fn error(session: &mut Session, sql: &str) -> String {
    session.run(sql).expect_err(sql).to_string()
}

#[test]
fn float_clustered_pk_refused_for_ttl() {
    let mut session = Session::new();

    // FLOAT is a clustered handle by default: refused.
    assert_eq!(
        error(
            &mut session,
            "create table t (id float primary key, created datetime) \
             ttl = created + interval 3 month"
        ),
        "Unsupported clustered primary key type FLOAT/DOUBLE for TTL"
    );

    // DOUBLE likewise.
    assert_eq!(
        error(
            &mut session,
            "create table t2 (id double primary key, created datetime) \
             ttl = created + interval 3 month"
        ),
        "Unsupported clustered primary key type FLOAT/DOUBLE for TTL"
    );

    // An integer clustered handle is fine.
    session
        .run(
            "create table t3 (id int primary key, created datetime) \
             ttl = created + interval 3 month",
        )
        .unwrap();

    // FLOAT without a TTL config is fine.
    session
        .run("create table t4 (id float primary key, created datetime)")
        .unwrap();
}
