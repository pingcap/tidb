//! Static privileges: `GRANT`/`REVOKE`/`SHOW GRANTS` at global, database and
//! table scope, `GRANT OPTION`, and the refusals -- Go `mysql.user`,
//! `mysql.db` and `mysql.tables_priv`.

use crate::tests_support::*;
use crate::*;

/// The `information_schema` PRIVILEGES family: `SCHEMA_PRIVILEGES`,
/// `TABLE_PRIVILEGES`, `COLUMN_PRIVILEGES`.
///
/// The surprising part, and the reason this test exists: these three are
/// DECLARED in Go's `pkg/infoschema/tables.go` but have NO retriever in
/// `pkg/executor`, so real TiDB serves the header and NEVER a row --
/// even when grants exist. CAPTURED from `testkit.CreateMockStore` after
/// `GRANT SELECT, INSERT ON db1.* TO 'u1'@'%'`,
/// `GRANT ALL PRIVILEGES ON db1.* TO 'u2'@'localhost'`,
/// `GRANT SELECT ON db1.t1 TO 'u1'@'%' WITH GRANT OPTION` and
/// `GRANT UPDATE, DELETE ON db1.t1 TO 'u2'@'localhost'`: every
/// `SELECT *` came back empty and `SELECT COUNT(*)` came back `0`.
///
/// So filling these in from the privilege registry -- which HAS all the
/// grant data -- would be a DIVERGENCE from Go, not a completion. The
/// emptiness is the transcreated behavior.
#[test]
fn infoschema_privileges_tables_are_header_only() {
    let mut session = Session::new();
    session.attach_privileges(privilege::PrivilegeRegistry::default());
    session.run("CREATE DATABASE db1").unwrap();
    session.run("CREATE TABLE db1.t1 (a INT)").unwrap();
    session.run("CREATE USER 'u1'@'%'").unwrap();
    session.run("CREATE USER 'u2'@'localhost'").unwrap();
    session
        .run("GRANT SELECT, INSERT ON db1.* TO 'u1'@'%'")
        .unwrap();
    session
        .run("GRANT ALL PRIVILEGES ON db1.* TO 'u2'@'localhost'")
        .unwrap();
    // Table scope too, so the emptiness is not just a DB-scope artifact.
    // (Go's capture also used `WITH GRANT OPTION` here; this tier does
    // not model that yet, and it makes no difference to the result --
    // the table is empty either way.)
    session.run("GRANT SELECT ON db1.t1 TO 'u1'@'%'").unwrap();

    let query = |session: &mut Session, sql: &str| match session.run_with_columns(sql).unwrap() {
        StmtOutput::Rows { columns, rows } => (
            columns
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            rows,
        ),
        other => panic!("expected rows, got {other:?}"),
    };

    let (names, rows) = query(
        &mut session,
        "SELECT * FROM information_schema.schema_privileges",
    );
    assert_eq!(
        names,
        [
            "GRANTEE",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "PRIVILEGE_TYPE",
            "IS_GRANTABLE",
        ]
    );
    assert!(rows.is_empty(), "grants must NOT surface here");

    let (names, rows) = query(
        &mut session,
        "SELECT * FROM information_schema.table_privileges",
    );
    assert_eq!(
        names,
        [
            "GRANTEE",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "PRIVILEGE_TYPE",
            "IS_GRANTABLE",
        ]
    );
    assert!(rows.is_empty(), "grants must NOT surface here");

    let (names, rows) = query(
        &mut session,
        "SELECT * FROM information_schema.column_privileges",
    );
    assert_eq!(
        names,
        [
            "GRANTEE",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "PRIVILEGE_TYPE",
            "IS_GRANTABLE",
        ]
    );
    assert!(rows.is_empty(), "grants must NOT surface here");

    // Go returns `0`, not an error, for the aggregate over the empty
    // body -- so the tables are real relations, not stubs that fail.
    for table in ["schema_privileges", "table_privileges", "column_privileges"] {
        let (_, rows) = query(
            &mut session,
            &format!("SELECT COUNT(*) FROM information_schema.{table}"),
        );
        assert_eq!(rows, vec![vec![Datum::Int(0)]], "COUNT(*) over {table}");
    }

    // A WHERE filter over the empty body also runs the ordinary plan
    // path rather than erroring on an unknown table.
    let (_, rows) = query(
        &mut session,
        "SELECT grantee FROM information_schema.schema_privileges WHERE table_schema = 'db1'",
    );
    assert!(rows.is_empty());
}

/// CAPTURED end to end (`pkg/executor/grant.go`, `revoke.go`,
/// `simple.go`, `show.go`): `CREATE USER` -> fresh `SHOW GRANTS` reports
/// `USAGE` -> `GRANT` in scrambled order prints in Go's fixed
/// `mysql.AllGlobalPrivs` order -> `REVOKE` removes exactly the one
/// privilege -> `DROP USER` then a missing-user error, matching the Go
/// source's `ErrCannotUser`/1396 wording exactly (`user@host`, unquoted).
#[test]
fn grant_revoke_and_show_grants_round_trip() {
    let mut session = session_with_privileges();

    session.run("CREATE USER 'u1'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [["GRANT USAGE ON *.* TO 'u1'@'%'"]]
    );

    session
        .run("GRANT SELECT, PROCESS, INSERT, SUPER, UPDATE ON *.* TO 'u1'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [["GRANT SELECT,INSERT,UPDATE,PROCESS,SUPER ON *.* TO 'u1'@'%'"]]
    );

    session.run("REVOKE SUPER ON *.* FROM 'u1'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [["GRANT SELECT,INSERT,UPDATE,PROCESS ON *.* TO 'u1'@'%'"]]
    );

    session.run("DROP USER 'u1'@'%'").unwrap();
    match session.run("DROP USER 'nosuchuser'@'%'") {
        Err(DriverError::DropUserMissing { accounts }) => {
            assert_eq!(accounts, "nosuchuser@%");
        }
        other => panic!("expected DropUserMissing, got {other:?}"),
    }
}

/// CAPTURED: `SHOW GRANTS` with no `FOR` reports the current session's
/// own account, and a fresh cluster's bootstrap `root`@`%` carries
/// `ALL PRIVILEGES ... WITH GRANT OPTION`.
#[test]
fn show_grants_for_current_user_reports_root_bootstrap() {
    let mut session = session_with_privileges();
    session.set_user("root@%".to_owned(), "root@127.0.0.1".to_owned());
    assert_eq!(
        row_text(session.run("SHOW GRANTS")),
        [["GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION"]]
    );
}

/// CAPTURED: re-creating an existing account is `ErrCannotUser`/1396,
/// quoted `'user'@'host'` (unlike `DROP USER`'s unquoted form).
#[test]
fn create_user_rejects_a_duplicate_account() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'dup1'@'%'").unwrap();
    match session.run("CREATE USER 'dup1'@'%'") {
        Err(DriverError::CreateUserAlreadyExists { user, host }) => {
            assert_eq!(user, "dup1");
            assert_eq!(host, "%");
        }
        other => panic!("expected CreateUserAlreadyExists, got {other:?}"),
    }
}

/// CAPTURED: `GRANT ... TO` an account that was never created is
/// `ErrCantCreateUserWithGrant`/1410 -- TiDB's default sql_mode refuses
/// to implicitly create the target.
#[test]
fn grant_to_an_unknown_user_is_refused() {
    let mut session = session_with_privileges();
    assert!(matches!(
        session.run("GRANT SELECT ON *.* TO 'nouser'@'%'"),
        Err(DriverError::GrantToUnknownUser)
    ));
}

/// CAPTURED: an unrecognized privilege name parses (through
/// `tidb-parser`'s dynamic-privilege grammar branch) but is refused at
/// execution with `ErrDynamicPrivilegeNotRegistered`/3929, naming the
/// privilege.
#[test]
fn granting_an_unregistered_privilege_name_is_refused() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'dup1'@'%'").unwrap();
    match session.run("GRANT FOOBAR ON *.* TO 'dup1'@'%'") {
        Err(DriverError::DynamicPrivilegeNotRegistered(name)) => assert_eq!(name, "FOOBAR"),
        other => panic!("expected DynamicPrivilegeNotRegistered, got {other:?}"),
    }
}

/// CAPTURED: `REVOKE ... FROM` an account that does not exist is Go's
/// plain `errors.Errorf("Unknown user: %s", user)`.
#[test]
fn revoke_from_an_unknown_user_is_refused() {
    let mut session = session_with_privileges();
    match session.run("REVOKE SELECT ON *.* FROM 'nouser'@'%'") {
        Err(DriverError::RevokeUnknownUser { user, host }) => {
            assert_eq!(user, "nouser");
            assert_eq!(host, "%");
        }
        other => panic!("expected RevokeUnknownUser, got {other:?}"),
    }
}

/// `ALL PRIVILEGES` grants every modeled global privilege, which folds
/// `SHOW GRANTS` to the `ALL PRIVILEGES` literal (Go `userPrivToString`).
#[test]
fn grant_all_privileges_collapses_show_grants() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'dup1'@'%'").unwrap();
    session
        .run("GRANT ALL PRIVILEGES ON *.* TO 'dup1'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'dup1'@'%'")),
        [["GRANT ALL PRIVILEGES ON *.* TO 'dup1'@'%'"]]
    );
}

/// CAPTURED (`pkg/executor/zz_dump_authlc_test.go`): `WITH GRANT OPTION`
/// at all three scopes, its ` WITH GRANT OPTION` suffix printing at the
/// END of each affected `SHOW GRANTS` line (never inside the privilege
/// list), and `REVOKE GRANT OPTION ON <level>` clearing exactly that one
/// scope's bit and nothing else.
#[test]
fn grant_option_is_a_per_scope_bit_printed_as_a_line_suffix() {
    let mut session = session_with_privileges();
    session.run("CREATE TABLE test.t (a int)").unwrap();
    session.run("CREATE USER 'bob'@'%'").unwrap();

    session
        .run("GRANT SELECT ON *.* TO 'bob'@'%' WITH GRANT OPTION")
        .unwrap();
    session
        .run("GRANT SELECT ON test.* TO 'bob'@'%' WITH GRANT OPTION")
        .unwrap();
    session
        .run("GRANT SELECT ON test.t TO 'bob'@'%' WITH GRANT OPTION")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'bob'@'%'")),
        [
            ["GRANT SELECT ON *.* TO 'bob'@'%' WITH GRANT OPTION"],
            ["GRANT SELECT ON `test`.* TO 'bob'@'%' WITH GRANT OPTION"],
            ["GRANT SELECT ON `test`.`t` TO 'bob'@'%' WITH GRANT OPTION"],
        ]
    );

    // Each REVOKE clears one scope, innermost first, leaving the others
    // untouched -- the captured Go sequence exactly.
    session
        .run("REVOKE GRANT OPTION ON test.t FROM 'bob'@'%'")
        .unwrap();
    session
        .run("REVOKE GRANT OPTION ON test.* FROM 'bob'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'bob'@'%'")),
        [
            ["GRANT SELECT ON *.* TO 'bob'@'%' WITH GRANT OPTION"],
            ["GRANT SELECT ON `test`.* TO 'bob'@'%'"],
            ["GRANT SELECT ON `test`.`t` TO 'bob'@'%'"],
        ]
    );
    session
        .run("REVOKE GRANT OPTION ON *.* FROM 'bob'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'bob'@'%'")),
        [
            ["GRANT SELECT ON *.* TO 'bob'@'%'"],
            ["GRANT SELECT ON `test`.* TO 'bob'@'%'"],
            ["GRANT SELECT ON `test`.`t` TO 'bob'@'%'"],
        ]
    );
}

/// CAPTURED: `GRANT ALL` does NOT confer `GRANT OPTION` (the `ALL
/// PRIVILEGES` literal still prints with no suffix), and naming
/// `GRANT OPTION` as an ordinary privilege confers exactly that bit --
/// which is why `mysql.GrantPriv` must live outside every `ALL_*` list.
#[test]
fn grant_all_withholds_grant_option_but_the_named_privilege_confers_it() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'occupied'@'%'").unwrap();
    session.run("GRANT ALL ON *.* TO 'occupied'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'occupied'@'%'")),
        [["GRANT ALL PRIVILEGES ON *.* TO 'occupied'@'%'"]]
    );
    session
        .run("GRANT GRANT OPTION ON *.* TO 'occupied'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'occupied'@'%'")),
        [["GRANT ALL PRIVILEGES ON *.* TO 'occupied'@'%' WITH GRANT OPTION"]]
    );
}

/// CAPTURED end to end (`pkg/executor/grant.go`/`revoke.go`,
/// `pkg/privilege/privileges/cache.go`'s `showGrants`): DB-scope
/// `GRANT`/`REVOKE`/`SHOW GRANTS`, including the `ALL PRIVILEGES`
/// literal and the lexical (not insertion, not plain-name) sort order
/// across multiple databases.
#[test]
fn db_scope_grant_revoke_and_show_grants_round_trip() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'u1'@'%'").unwrap();
    session.run("CREATE DATABASE db1").unwrap();
    session.run("CREATE DATABASE aaadb").unwrap();

    session.run("GRANT SELECT ON db1.* TO 'u1'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [
            vec!["GRANT USAGE ON *.* TO 'u1'@'%'".to_owned()],
            vec!["GRANT SELECT ON `db1`.* TO 'u1'@'%'".to_owned()],
        ]
    );

    // A second DB, granted later, still sorts before `db1` (captured:
    // Go sorts DB-scope lines lexically by their formatted text).
    session.run("GRANT SELECT ON aaadb.* TO 'u1'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [
            vec!["GRANT USAGE ON *.* TO 'u1'@'%'".to_owned()],
            vec!["GRANT SELECT ON `aaadb`.* TO 'u1'@'%'".to_owned()],
            vec!["GRANT SELECT ON `db1`.* TO 'u1'@'%'".to_owned()],
        ]
    );

    // Once `db1`'s line becomes `GRANT ALL PRIVILEGES ...`, it sorts
    // *before* `aaadb`'s `GRANT SELECT ...` line: the sort key is the
    // whole formatted string, which starts with the privilege text, not
    // the database name ('A' < 'S').
    session.run("GRANT ALL ON db1.* TO 'u1'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'"))[1],
        vec!["GRANT ALL PRIVILEGES ON `db1`.* TO 'u1'@'%'".to_owned()]
    );

    session.run("REVOKE ALL ON db1.* FROM 'u1'@'%'").unwrap();
    session.run("REVOKE SELECT ON db1.* FROM 'u1'@'%'").unwrap();
    // A row stripped of every privilege prints NO line at all -- Go emits
    // a DB-scope line only when the privilege list is non-empty, or (the
    // USAGE special case) when `GRANT OPTION` is the only bit left.
    // CAPTURED: after `REVOKE SELECT ON cg.*`, `SHOW GRANTS` reports the
    // global `USAGE` line alone.
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [
            vec!["GRANT USAGE ON *.* TO 'u1'@'%'".to_owned()],
            vec!["GRANT SELECT ON `aaadb`.* TO 'u1'@'%'".to_owned()],
        ]
    );
}

/// CAPTURED: `GRANT PROCESS ON db.*` (a global-only privilege) is Go's
/// `ErrWrongUsage`/1221, "Incorrect usage of DB GRANT and GLOBAL
/// PRIVILEGES".
#[test]
fn db_scope_grant_rejects_global_only_privilege() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'u1'@'%'").unwrap();
    session.run("CREATE DATABASE db1").unwrap();
    assert!(matches!(
        session.run("GRANT PROCESS ON db1.* TO 'u1'@'%'"),
        Err(DriverError::DbGrantGlobalOnlyPriv)
    ));
}

/// CAPTURED: `REVOKE ... ON db.*` for an account with no `mysql.DB` row
/// for that database at all is Go's plain "There is no such grant
/// defined for user '%s' on host '%s' on database %s".
#[test]
fn db_scope_revoke_without_any_grant_row_is_refused() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'u1'@'%'").unwrap();
    session.run("CREATE DATABASE emptydb").unwrap();
    match session.run("REVOKE SELECT ON emptydb.* FROM 'u1'@'%'") {
        Err(DriverError::RevokeNoDbGrant {
            user,
            host,
            database,
        }) => {
            assert_eq!(user, "u1");
            assert_eq!(host, "%");
            assert_eq!(database, "emptydb");
        }
        other => panic!("expected RevokeNoDbGrant, got {other:?}"),
    }
}

/// CAPTURED end to end: TABLE-scope `GRANT`/`REVOKE`/`SHOW GRANTS`,
/// including the `ALL PRIVILEGES` literal, backtick-quoted
/// `` `db`.`table` `` (both segments escaped, same as Go's
/// `stringutil.Escape`), and the invalid-scope-privilege / missing-table
/// error split (Go checks privilege validity before table existence).
#[test]
fn table_scope_grant_revoke_and_show_grants_round_trip() {
    let mut session = session_with_privileges();
    session.run("CREATE USER 'u1'@'%'").unwrap();
    session.run("CREATE DATABASE db1").unwrap();
    session.run("CREATE TABLE db1.t1 (a INT)").unwrap();

    session
        .run("GRANT SELECT, INSERT ON db1.t1 TO 'u1'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [
            vec!["GRANT USAGE ON *.* TO 'u1'@'%'".to_owned()],
            vec!["GRANT SELECT,INSERT ON `db1`.`t1` TO 'u1'@'%'".to_owned()],
        ]
    );

    session.run("GRANT ALL ON db1.t1 TO 'u1'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'"))[1],
        vec!["GRANT ALL PRIVILEGES ON `db1`.`t1` TO 'u1'@'%'".to_owned()]
    );

    // Same zero-row rule as DB scope: the `mysql.Tables_priv` row survives
    // the REVOKE, but with no privilege left it prints nothing (CAPTURED).
    session.run("REVOKE ALL ON db1.t1 FROM 'u1'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u1'@'%'")),
        [vec!["GRANT USAGE ON *.* TO 'u1'@'%'".to_owned()]]
    );

    // Invalid-scope privilege: refused before the table-existence
    // check runs (captured `ErrIllegalGrantForTable`/1144).
    assert!(matches!(
        session.run("GRANT PROCESS ON db1.t1 TO 'u1'@'%'"),
        Err(DriverError::IllegalGrantForTable)
    ));

    // A valid privilege on a table that does not exist: refused with
    // `ErrTableNotExists`/1146 (captured), unless `CREATE` is among the
    // granted privileges (Go's issue #28533/#29268 exception).
    assert!(matches!(
        session.run("GRANT SELECT ON db1.nosuchtable TO 'u1'@'%'"),
        Err(DriverError::Schema(SchemaErrorKind::UnknownTable(ref name)))
            if name == "db1.nosuchtable"
    ));
    session
        .run("GRANT CREATE ON db1.nosuchtable TO 'u1'@'%'")
        .unwrap();

    // REVOKE for an account with no `mysql.Tables_priv` row at all.
    session.run("CREATE TABLE db1.t2 (a INT)").unwrap();
    match session.run("REVOKE SELECT ON db1.t2 FROM 'u1'@'%'") {
        Err(DriverError::RevokeNoTableGrant {
            user,
            host,
            database,
            table,
        }) => {
            assert_eq!(user, "u1");
            assert_eq!(host, "%");
            assert_eq!(database, "db1");
            assert_eq!(table, "t2");
        }
        other => panic!("expected RevokeNoTableGrant, got {other:?}"),
    }
}
