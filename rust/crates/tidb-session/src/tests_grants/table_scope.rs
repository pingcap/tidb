//! Go's `visitInfo` on ordinary reads and writes: `SELECT`, `INSERT`,
//! `REPLACE`, `INSERT ... ON DUPLICATE KEY UPDATE`, `UPDATE`, `DELETE`, and
//! the table DDL kinds.
//!
//! Go collects one entry per table while planning
//! (`logical_plan_builder.go`'s `buildDataSource` around line 4972,
//! `planbuilder.go`'s `buildInsert` around line 4176,
//! `buildNewAssignments` around line 6490, `buildDelete` around line 6640)
//! and checks them in `optimizer.go`'s `CheckPrivilege`. Every test here
//! pins both the denial and its POSITIVE control -- the same statement run
//! once the grant exists.

use crate::tests_support::*;
use crate::*;

/// One table `t` in schema `test`, an unprivileged `bob`, and an
/// unrestricted bootstrap session to grant through.
///
/// `bob` gets exactly one grant before he connects: `CREATE TEMPORARY
/// TABLES ON test.*`. That is not scaffolding, it is what Go requires --
/// `USE test` is `ErrDBaccessDenied` (1044) for an account with no evidence
/// at all on the schema, so an account that cannot see `test` could never
/// reach the 1142 denials these tests are about. The privilege is chosen
/// because it is the one privilege that makes a schema visible
/// (`DBIsVisible` accepts ANY nonzero `mysql.db` row) while being demanded by
/// no statement here AND being the single bit `SHOW TABLES` masks OUT, so it
/// leaks no table name either. All three facts measured against Go:
/// with only this grant, `USE test` succeeds, `SELECT * FROM t` is still
/// `1142 SELECT command denied`, and `SHOW TABLES` is empty.
fn scoped() -> (privilege::PrivilegeRegistry, Session, Session) {
    let privs = privilege::PrivilegeRegistry::default();
    let mut boot = bootstrap_session(&privs);
    boot.run("CREATE USER 'bob'@'%'").unwrap();
    boot.run("GRANT CREATE TEMPORARY TABLES ON test.* TO 'bob'@'%'")
        .unwrap();
    boot.run("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
        .unwrap();
    boot.run("CREATE TABLE u (a INT PRIMARY KEY, b INT)")
        .unwrap();
    boot.run("INSERT INTO t VALUES (1, 10)").unwrap();
    let mut bob = session_as(&privs, boot.shared_catalog(), "bob", "%");
    bob.run("USE test").unwrap();
    (privs, boot, bob)
}

fn denied(session: &mut Session, sql: &str) -> (u16, String) {
    match session.run(sql) {
        Err(error) => {
            let rendered = error.to_mysql_error();
            (rendered.code, rendered.message)
        }
        Ok(output) => panic!("{sql} must be denied, got {output:?}"),
    }
}

/// Go `ErrTableaccessDenied` (1142): `"%s command denied to user
/// '%s'@'%s' for table '%s'"`, with the table name lowercased
/// (`tableInfo.Name.L`).
fn table_denied(command: &str, table: &str) -> (u16, String) {
    (
        1142,
        format!("{command} command denied to user 'bob'@'%' for table '{table}'"),
    )
}

/// A `SELECT` demands `SELECT` on every table it reads, and the grant that
/// opens it may sit at any scope.
#[test]
fn select_demands_select_on_every_table_it_reads() {
    let (_, mut boot, mut bob) = scoped();

    assert_eq!(
        denied(&mut bob, "SELECT * FROM t"),
        table_denied("SELECT", "t")
    );
    // A join names both tables, and the FIRST missing one is reported.
    assert_eq!(
        denied(&mut bob, "SELECT * FROM t JOIN u ON t.a = u.a"),
        table_denied("SELECT", "t")
    );
    // A subquery is a data source like any other.
    assert_eq!(
        denied(&mut bob, "SELECT (SELECT COUNT(*) FROM t)"),
        table_denied("SELECT", "t")
    );

    // A statement that reads NO table needs nothing, which is why
    // `SELECT 1` works on a fresh connection.
    assert!(bob.run("SELECT 1").is_ok());

    // POSITIVE CONTROL, one scope at a time: a table-scope grant opens
    // exactly its own table.
    boot.run("GRANT SELECT ON test.t TO 'bob'@'%'").unwrap();
    assert!(bob.run("SELECT * FROM t").is_ok());
    assert_eq!(
        denied(&mut bob, "SELECT * FROM t JOIN u ON t.a = u.a"),
        table_denied("SELECT", "u")
    );
    boot.run("GRANT SELECT ON test.* TO 'bob'@'%'").unwrap();
    assert!(bob.run("SELECT * FROM t JOIN u ON t.a = u.a").is_ok());
}

/// A CTE is referenced through the table grammar but is not a table, so it
/// demands nothing -- the one shape a naive `TableRef` sweep would refuse
/// that Go allows.
#[test]
fn a_cte_reference_is_not_a_table() {
    let (_, mut boot, mut bob) = scoped();
    boot.run("GRANT SELECT ON test.t TO 'bob'@'%'").unwrap();
    assert!(bob
        .run("WITH c AS (SELECT a FROM t) SELECT * FROM c")
        .is_ok());
}

/// `information_schema` is answered by Go's fixed rules before any grant is
/// read (`privileges.go` around line 201), so every session may read it.
#[test]
fn information_schema_needs_no_grant() {
    let (_, _boot, mut bob) = scoped();
    assert!(bob
        .run("SELECT TABLE_NAME FROM information_schema.TABLES")
        .is_ok());
}

/// `buildInsert` (`planbuilder.go` around line 4176): `INSERT` on the
/// target, and additionally `DELETE` for `REPLACE` or `UPDATE` for
/// `ON DUPLICATE KEY UPDATE`.
#[test]
fn insert_replace_and_on_duplicate_demand_their_own_privileges() {
    let (_, mut boot, mut bob) = scoped();

    assert_eq!(
        denied(&mut bob, "INSERT INTO t VALUES (2, 20)"),
        table_denied("INSERT", "t")
    );
    boot.run("GRANT INSERT ON test.t TO 'bob'@'%'").unwrap();
    assert!(bob.run("INSERT INTO t VALUES (2, 20)").is_ok());

    // REPLACE can remove a row, so it needs DELETE too.
    assert_eq!(
        denied(&mut bob, "REPLACE INTO t VALUES (2, 21)"),
        table_denied("DELETE", "t")
    );
    // ON DUPLICATE KEY UPDATE can rewrite one, so it needs UPDATE.
    assert_eq!(
        denied(
            &mut bob,
            "INSERT INTO t VALUES (2, 22) ON DUPLICATE KEY UPDATE b = 22"
        ),
        table_denied("UPDATE", "t")
    );

    // POSITIVE CONTROL for both.
    boot.run("GRANT DELETE, UPDATE ON test.t TO 'bob'@'%'")
        .unwrap();
    assert!(bob.run("REPLACE INTO t VALUES (2, 21)").is_ok());
    assert!(bob
        .run("INSERT INTO t VALUES (2, 22) ON DUPLICATE KEY UPDATE b = 22")
        .is_ok());
}

/// `INSERT ... SELECT` reads its source, so the source needs `SELECT` even
/// when the target's `INSERT` is already held.
#[test]
fn insert_select_demands_select_on_the_source() {
    let (_, mut boot, mut bob) = scoped();
    boot.run("GRANT INSERT ON test.u TO 'bob'@'%'").unwrap();
    assert_eq!(
        denied(&mut bob, "INSERT INTO u SELECT * FROM t"),
        table_denied("SELECT", "t")
    );
    boot.run("GRANT SELECT ON test.t TO 'bob'@'%'").unwrap();
    assert!(bob.run("INSERT INTO u SELECT * FROM t").is_ok());
}

/// An `UPDATE` reads before it writes, so Go demands `SELECT` first (1142)
/// and then `UPDATE` on the assignment's table -- the latter with no
/// statement-specific error, which is why it reports 8121.
#[test]
fn update_demands_select_then_update() {
    let (_, mut boot, mut bob) = scoped();

    assert_eq!(
        denied(&mut bob, "UPDATE t SET b = 11 WHERE a = 1"),
        table_denied("SELECT", "t")
    );
    boot.run("GRANT SELECT ON test.t TO 'bob'@'%'").unwrap();
    assert_eq!(
        denied(&mut bob, "UPDATE t SET b = 11 WHERE a = 1"),
        (8121, "privilege check for 'Update' fail".to_owned())
    );
    boot.run("GRANT UPDATE ON test.t TO 'bob'@'%'").unwrap();
    assert!(bob.run("UPDATE t SET b = 11 WHERE a = 1").is_ok());
}

/// `buildDelete` (`logical_plan_builder.go` around line 6640): `SELECT` on
/// every source, then `DELETE` on the target, this one WITH the 1142.
#[test]
fn delete_demands_select_then_delete() {
    let (_, mut boot, mut bob) = scoped();

    assert_eq!(
        denied(&mut bob, "DELETE FROM t WHERE a = 1"),
        table_denied("SELECT", "t")
    );
    boot.run("GRANT SELECT ON test.t TO 'bob'@'%'").unwrap();
    assert_eq!(
        denied(&mut bob, "DELETE FROM t WHERE a = 1"),
        table_denied("DELETE", "t")
    );
    boot.run("GRANT DELETE ON test.t TO 'bob'@'%'").unwrap();
    assert!(bob.run("DELETE FROM t WHERE a = 1").is_ok());
}

/// `planbuilder.go`'s DDL arm: `CREATE`/`DROP`/`ALTER` on the table, and
/// `INDEX` for either index statement.
#[test]
fn table_ddl_demands_its_own_privilege() {
    let (_, mut boot, mut bob) = scoped();

    assert_eq!(
        denied(&mut bob, "CREATE TABLE made (a INT)"),
        table_denied("CREATE", "made")
    );
    assert_eq!(denied(&mut bob, "DROP TABLE t"), table_denied("DROP", "t"));
    assert_eq!(
        denied(&mut bob, "ALTER TABLE t ADD COLUMN c INT"),
        table_denied("ALTER", "t")
    );
    assert_eq!(
        denied(&mut bob, "CREATE INDEX idx ON t (b)"),
        table_denied("INDEX", "t")
    );

    // POSITIVE CONTROL: each grant opens exactly its own statement.
    boot.run("GRANT CREATE, ALTER, INDEX, DROP ON test.* TO 'bob'@'%'")
        .unwrap();
    assert!(bob.run("CREATE TABLE made (a INT)").is_ok());
    assert!(bob.run("ALTER TABLE t ADD COLUMN c INT").is_ok());
    assert!(bob.run("CREATE INDEX idx ON t (b)").is_ok());
    assert!(bob.run("DROP TABLE made").is_ok());
}

/// `CREATE/DROP DATABASE` carry database-scoped visitInfo and therefore use
/// Go's 1044 denial, before the early schema arm can mutate the catalog.
#[test]
fn database_ddl_demands_create_and_drop_before_catalog_mutation() {
    let (_, mut boot, mut bob) = scoped();

    assert_eq!(
        denied(&mut bob, "CREATE DATABASE blocked"),
        (
            1044,
            "Access denied for user 'bob'@'%' to database 'blocked'".to_owned(),
        )
    );
    assert_eq!(
        denied(&mut bob, "DROP DATABASE test"),
        (
            1044,
            "Access denied for user 'bob'@'%' to database 'test'".to_owned(),
        )
    );

    boot.run("GRANT CREATE, DROP ON *.* TO 'bob'@'%'").unwrap();
    assert!(bob.run("CREATE DATABASE allowed").is_ok());
    assert!(bob.run("DROP DATABASE allowed").is_ok());
}

/// A session with no authenticated identity -- the in-process driver, and
/// the server's own bootstrap -- stays unrestricted, which is the rule
/// every other check here follows and what keeps the embedded driver
/// working.
#[test]
fn a_session_with_no_identity_is_unrestricted() {
    let privs = privilege::PrivilegeRegistry::default();
    let mut boot = bootstrap_session(&privs);
    boot.run("CREATE TABLE t (a INT)").unwrap();
    boot.run("INSERT INTO t VALUES (1)").unwrap();
    assert!(boot.run("SELECT * FROM t").is_ok());
    assert!(boot.run("UPDATE t SET a = 2").is_ok());
    assert!(boot.run("DELETE FROM t").is_ok());
    assert!(boot.run("DROP TABLE t").is_ok());
}
