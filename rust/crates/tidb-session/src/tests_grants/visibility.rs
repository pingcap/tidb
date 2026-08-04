//! What a connected account may SEE: `SHOW DATABASES`, `SHOW TABLES`, `USE`,
//! `SHOW TABLE STATUS`, and the `information_schema` retrievers.
//!
//! Two DIFFERENT Go rules meet here and every test below pins which one
//! applies where:
//!
//! * `DBIsVisible` (`privileges/cache.go` around line 1693) decides
//!   `SHOW DATABASES`, `USE`, and the pre-lookup 1044 gate on `SHOW TABLES` /
//!   `SHOW TABLE STATUS`. It accepts a global privilege in `globalDBVisible`,
//!   `information_schema` unconditionally, ANY nonzero `mysql.db` row, ANY
//!   `mysql.tables_priv` row in the schema, or ANY `mysql.columns_priv` row.
//! * `RequestVerification(db, table, "", AllPrivMask)` decides which TABLES
//!   are listed and which `information_schema` rows exist. It is strictly
//!   narrower: no column-scope grant satisfies it, and no table-scope grant
//!   satisfies the schema-scope form `information_schema.SCHEMATA` asks.
//!
//! Every expectation was measured against a real TiDB session in this
//! checkout, driven through `session.Session.Auth` as the named account.

use crate::tests_support::*;
use crate::*;

/// Three schemas, tables in two of them, and an unrestricted session to
/// grant through.
fn world() -> (privilege::PrivilegeRegistry, Session) {
    let privs = privilege::PrivilegeRegistry::default();
    let mut boot = bootstrap_session(&privs);
    for sql in [
        "CREATE DATABASE d1",
        "CREATE DATABASE d2",
        "CREATE TABLE d1.t1 (a INT, b INT)",
        "CREATE TABLE d1.t2 (a INT)",
        "CREATE TABLE d2.t1 (a INT)",
    ] {
        boot.run(sql).unwrap();
    }
    (privs, boot)
}

fn names(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(sql))
        .into_iter()
        .map(|row| row[0].clone())
        .collect()
}

fn denial(session: &mut Session, sql: &str) -> (u16, String) {
    match session.run(sql) {
        Err(error) => {
            let rendered = error.to_mysql_error();
            (rendered.code, rendered.message)
        }
        Ok(output) => panic!("{sql} must be denied, got {output:?}"),
    }
}

/// Go `ErrDBaccessDenied` (1044).
fn db_denied(user: &str, database: &str) -> (u16, String) {
    (
        1044,
        format!("Access denied for user '{user}'@'%' to database '{database}'"),
    )
}

/// A single TABLE grant makes its schema visible and lists exactly that one
/// table -- and nothing else leaks.
///
/// Measured in Go with `GRANT SELECT ON d1.t1 TO 'u1'@'%'`:
/// `SHOW DATABASES` = `INFORMATION_SCHEMA, d1`; `SHOW TABLES IN d1` = `t1`;
/// `SHOW TABLES IN d2` = 1044.
#[test]
fn a_single_table_grant_shows_one_schema_and_one_table() {
    let (privs, mut boot) = world();
    boot.run("CREATE USER 'u1'@'%'").unwrap();
    boot.run("GRANT SELECT ON d1.t1 TO 'u1'@'%'").unwrap();
    let mut u1 = session_as(&privs, boot.shared_catalog(), "u1", "%");

    assert_eq!(
        names(&mut u1, "SHOW DATABASES"),
        vec!["INFORMATION_SCHEMA".to_owned(), "d1".to_owned()],
    );
    assert_eq!(names(&mut u1, "SHOW TABLES IN d1"), vec!["t1".to_owned()]);
    assert_eq!(denial(&mut u1, "SHOW TABLES IN d2"), db_denied("u1", "d2"));

    // The unrestricted bootstrap session still sees everything, which is
    // Go's `checker == nil` arm and is what keeps every other suite here
    // working.
    assert!(names(&mut boot, "SHOW DATABASES").contains(&"d2".to_owned()));
    assert_eq!(names(&mut boot, "SHOW TABLES IN d1").len(), 2);
}

/// A schema the account cannot see reports 1044 whether or not it exists,
/// because Go asks `DBIsVisible` BEFORE `SchemaExists`.
///
/// Measured in Go: `SHOW TABLES IN nosuchdb` as `u1` is
/// `[executor:1044]Access denied ... to database 'nosuchdb'`, not `ErrBadDB`.
#[test]
fn an_invisible_schema_reports_1044_even_when_it_does_not_exist() {
    let (privs, mut boot) = world();
    boot.run("CREATE USER 'u1'@'%'").unwrap();
    boot.run("GRANT SELECT ON d1.t1 TO 'u1'@'%'").unwrap();
    let mut u1 = session_as(&privs, boot.shared_catalog(), "u1", "%");

    assert_eq!(
        denial(&mut u1, "SHOW TABLES IN nosuchdb"),
        db_denied("u1", "nosuchdb"),
    );
    assert_eq!(denial(&mut u1, "USE d2"), db_denied("u1", "d2"));
    assert_eq!(
        denial(&mut u1, "SHOW TABLE STATUS FROM d2"),
        db_denied("u1", "d2"),
    );
    // The schema the grant reaches opens all three.
    assert!(u1.run("USE d1").is_ok());
    assert!(u1.run("SHOW TABLE STATUS FROM d1").is_ok());
}

/// `information_schema` is visible to everyone and needs no grant at all --
/// Go's `DBIsVisible` returns early for it, and
/// `UserPrivileges.RequestVerification` admits every table in it.
///
/// Measured in Go: an account with a single unrelated grant lists all 88
/// `information_schema` tables.
#[test]
fn information_schema_is_visible_without_any_grant() {
    let (privs, mut boot) = world();
    boot.run("CREATE USER 'nobody'@'%'").unwrap();
    let mut nobody = session_as(&privs, boot.shared_catalog(), "nobody", "%");

    assert_eq!(
        names(&mut nobody, "SHOW DATABASES"),
        vec!["INFORMATION_SCHEMA".to_owned()],
    );
    assert!(nobody.run("USE information_schema").is_ok());
    assert!(!names(&mut nobody, "SHOW TABLES IN information_schema").is_empty());
    assert_eq!(
        names(
            &mut nobody,
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA"
        ),
        vec!["INFORMATION_SCHEMA".to_owned()],
    );
    // An account with no evidence anywhere sees no user table at all.
    assert_eq!(
        row_text(nobody.run(
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA IN ('d1', 'd2')",
        ))
        .len(),
        0,
    );
}

/// A GLOBAL privilege OUTSIDE `globalDBVisible` shows nothing: `PROCESS` on
/// `*.*` is not schema visibility.
///
/// Measured in Go with `GRANT USAGE ON *.* TO 'u3'@'%'` (which stores no
/// bit): `SHOW DATABASES` = `INFORMATION_SCHEMA` and `SHOW TABLES IN d1` is
/// 1044. `PROCESS` behaves the same way because it is absent from
/// `globalDBVisible`.
#[test]
fn a_server_admin_privilege_is_not_schema_visibility() {
    let (privs, mut boot) = world();
    boot.run("CREATE USER 'u3'@'%'").unwrap();
    boot.run("GRANT PROCESS ON *.* TO 'u3'@'%'").unwrap();
    let mut u3 = session_as(&privs, boot.shared_catalog(), "u3", "%");

    assert_eq!(
        names(&mut u3, "SHOW DATABASES"),
        vec!["INFORMATION_SCHEMA".to_owned()],
    );
    assert_eq!(denial(&mut u3, "SHOW TABLES IN d1"), db_denied("u3", "d1"));

    // A privilege that IS in `globalDBVisible` opens every schema at once.
    boot.run("GRANT SELECT ON *.* TO 'u3'@'%'").unwrap();
    assert_eq!(
        names(&mut u3, "SHOW DATABASES"),
        vec![
            "INFORMATION_SCHEMA".to_owned(),
            "d1".to_owned(),
            "d2".to_owned(),
            "mysql".to_owned(),
            "test".to_owned(),
        ],
    );
}

/// The measured divergence between the two rules: a COLUMN grant makes the
/// schema visible and lists NO table.
///
/// Measured in Go with `GRANT SELECT(a) ON d1.t2 TO 'u2'@'%'`:
/// `SHOW DATABASES` = `INFORMATION_SCHEMA, d1` (the `mysql.columns_priv` arm
/// of `DBIsVisible`) while `SHOW TABLES IN d1` is EMPTY -- `fetchShowTables`
/// passes an empty column to `RequestVerification`, which no column-scope
/// row can satisfy. Go carries a standing TODO about this; the TODO is Go's
/// behavior and therefore this tier's.
#[test]
fn a_column_grant_shows_the_schema_but_lists_no_table() {
    let (privs, mut boot) = world();
    boot.run("CREATE USER 'u2'@'%'").unwrap();
    boot.run("GRANT SELECT(a) ON d1.t2 TO 'u2'@'%'").unwrap();
    let mut u2 = session_as(&privs, boot.shared_catalog(), "u2", "%");

    assert_eq!(
        names(&mut u2, "SHOW DATABASES"),
        vec!["INFORMATION_SCHEMA".to_owned(), "d1".to_owned()],
    );
    assert!(names(&mut u2, "SHOW TABLES IN d1").is_empty());
    assert!(u2.run("USE d1").is_ok());
}

/// `information_schema.SCHEMATA` is NOT `SHOW DATABASES`: it asks the
/// schema-scope "any privilege" question, which a table-scope grant does not
/// answer.
///
/// Measured in Go: `u1` (holding only `SELECT ON d1.t1`) lists `d1` in
/// `SHOW DATABASES` and only `INFORMATION_SCHEMA` in
/// `information_schema.SCHEMATA`. A DB-scope grant closes the gap.
#[test]
fn schemata_asks_a_narrower_question_than_show_databases() {
    let (privs, mut boot) = world();
    boot.run("CREATE USER 'u1'@'%'").unwrap();
    boot.run("GRANT SELECT ON d1.t1 TO 'u1'@'%'").unwrap();
    let mut u1 = session_as(&privs, boot.shared_catalog(), "u1", "%");

    assert!(names(&mut u1, "SHOW DATABASES").contains(&"d1".to_owned()));
    assert_eq!(
        names(
            &mut u1,
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA"
        ),
        vec!["INFORMATION_SCHEMA".to_owned()],
    );
    // The table itself IS listed by `information_schema.TABLES`, which asks
    // the table-scope form.
    assert_eq!(
        names(
            &mut u1,
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA IN ('d1', 'd2')",
        ),
        vec!["t1".to_owned()],
    );

    boot.run("GRANT SELECT ON d1.* TO 'u1'@'%'").unwrap();
    assert_eq!(
        names(
            &mut u1,
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA"
        ),
        vec!["INFORMATION_SCHEMA".to_owned(), "d1".to_owned()],
    );
}

/// Every `information_schema` retriever that walks tables filters, not just
/// `TABLES` -- `COLUMNS`, `STATISTICS`, `KEY_COLUMN_USAGE`,
/// `TABLE_CONSTRAINTS` and `VIEWS` all pass through the same gate, so a
/// table's column names and index shape cannot be read around the check.
#[test]
fn every_table_walking_retriever_filters() {
    let (privs, mut boot) = world();
    boot.run("CREATE TABLE d2.secret (hidden INT PRIMARY KEY)")
        .unwrap();
    boot.run("CREATE USER 'u1'@'%'").unwrap();
    boot.run("GRANT SELECT ON d1.t1 TO 'u1'@'%'").unwrap();
    let mut u1 = session_as(&privs, boot.shared_catalog(), "u1", "%");

    for query in [
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'd2'",
        "SELECT TABLE_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = 'd2'",
        "SELECT TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = 'd2'",
        "SELECT TABLE_NAME FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = 'd2'",
    ] {
        assert!(
            names(&mut u1, query).is_empty(),
            "{query} leaked a d2 object",
        );
    }
    // The one table the grant reaches does report its columns, which is the
    // positive control that the filter is not simply refusing everything.
    assert_eq!(
        names(
            &mut u1,
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'd1'",
        ),
        vec!["a".to_owned(), "b".to_owned()],
    );
}

/// `information_schema.COLUMNS` uses `mysql.AllColumnPrivs`, not
/// `AllPrivMask`: a grant OUTSIDE those four privileges lists the table in
/// `TABLES` and none of its columns in `COLUMNS`.
#[test]
fn columns_uses_the_narrower_column_privilege_mask() {
    let (privs, mut boot) = world();
    boot.run("CREATE USER 'u4'@'%'").unwrap();
    boot.run("GRANT DROP ON d1.t1 TO 'u4'@'%'").unwrap();
    let mut u4 = session_as(&privs, boot.shared_catalog(), "u4", "%");

    assert_eq!(
        names(
            &mut u4,
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA IN ('d1', 'd2')",
        ),
        vec!["t1".to_owned()],
    );
    assert!(names(
        &mut u4,
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'd1'",
    )
    .is_empty());
}

/// A role's grants reach every one of these decisions, because Go asks
/// `DBIsVisible`/`RequestVerification` once per effective role.
#[test]
fn a_role_carries_visibility() {
    let (privs, mut boot) = world();
    boot.run("CREATE USER 'u5'@'%'").unwrap();
    boot.run("CREATE ROLE 'reader'@'%'").unwrap();
    boot.run("GRANT SELECT ON d1.t1 TO 'reader'@'%'").unwrap();
    boot.run("GRANT 'reader'@'%' TO 'u5'@'%'").unwrap();
    let mut u5 = session_as(&privs, boot.shared_catalog(), "u5", "%");

    // Not activated yet: the role's grant confers nothing.
    assert_eq!(
        names(&mut u5, "SHOW DATABASES"),
        vec!["INFORMATION_SCHEMA".to_owned()],
    );
    u5.run("SET ROLE 'reader'@'%'").unwrap();
    assert_eq!(
        names(&mut u5, "SHOW DATABASES"),
        vec!["INFORMATION_SCHEMA".to_owned(), "d1".to_owned()],
    );
    assert_eq!(names(&mut u5, "SHOW TABLES IN d1"), vec!["t1".to_owned()]);
}

/// `CREATE TEMPORARY TABLES` is the ONE privilege masked out of the
/// `SHOW TABLES` filter: it makes the schema visible and lists nothing.
///
/// Measured in Go with `GRANT CREATE TEMPORARY TABLES ON d1.* TO 'u6'@'%'`:
/// `USE d1` succeeds, `SHOW DATABASES` = `INFORMATION_SCHEMA, d1`, and
/// `SHOW TABLES` is EMPTY -- `fetchShowTables` filters with
/// `AllPrivMask &^ CreateTMPTablePriv`. `information_schema.TABLES`, which
/// uses the UNMASKED `AllPrivMask`, does list both tables, which is the
/// other half of the same measurement.
#[test]
fn create_temporary_tables_shows_the_schema_and_no_table() {
    let (privs, mut boot) = world();
    boot.run("CREATE USER 'u6'@'%'").unwrap();
    boot.run("GRANT CREATE TEMPORARY TABLES ON d1.* TO 'u6'@'%'")
        .unwrap();
    let mut u6 = session_as(&privs, boot.shared_catalog(), "u6", "%");

    assert_eq!(
        names(&mut u6, "SHOW DATABASES"),
        vec!["INFORMATION_SCHEMA".to_owned(), "d1".to_owned()],
    );
    assert!(u6.run("USE d1").is_ok());
    assert!(
        names(&mut u6, "SHOW TABLES IN d1").is_empty(),
        "CREATE TEMPORARY TABLES must be masked out of the SHOW TABLES filter",
    );
    assert_eq!(
        names(
            &mut u6,
            "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA IN ('d1', 'd2')",
        ),
        vec!["t1".to_owned(), "t2".to_owned()],
    );
}
