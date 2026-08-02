#![cfg(test)]

//! `SET GLOBAL` / `SELECT @@global.x` / `SHOW GLOBAL VARIABLES`: the
//! GLOBAL-scope sysvar tier over [`vars::GlobalSysvars`]. See that module's
//! doc comment for the inheritance rule these tests capture: a session's
//! copy is made from the shared table once, at connect.

use crate::tests_support::*;
use crate::*;

/// Two sessions sharing one [`vars::GlobalSysvars`] table, standing in for
/// two connections through the same [`crate::PipelineSessionFactory`
/// (`tidb-server`)]. Root is bootstrapped with every privilege on both, so a
/// `SET GLOBAL` is not itself blocked by the privilege gate under test
/// elsewhere in this file.
fn two_sessions_sharing_globals() -> (Session, Session, vars::GlobalSysvars) {
    let globals = vars::GlobalSysvars::new();
    let registry = privilege::PrivilegeRegistry::default();
    let catalog: SharedCatalog = std::sync::Arc::new(std::sync::Mutex::new(Catalog::default()));

    let mut first = Session::with_catalog(catalog.clone());
    first.set_user("root@%".to_owned(), "root@%".to_owned());
    first.attach_privileges(registry.clone());
    first.attach_globals(globals.clone());

    let mut second = Session::with_catalog(catalog);
    second.set_user("root@%".to_owned(), "root@%".to_owned());
    second.attach_privileges(registry);
    second.attach_globals(globals.clone());

    (first, second, globals)
}

/// The MySQL inheritance rule, captured end to end through `SET`/`SELECT`
/// rather than the unit-level `vars` module: `SET GLOBAL` on one session is
/// visible to a peer's `@@global.x` immediately, but the peer's own plain
/// `@@x` (its session copy, made at connect) does not move -- and a THIRD
/// session opened after the `SET GLOBAL` inherits the new value into ITS
/// session copy. (`autocommit` is `TypeBool`, so the reads report Go's
/// integer domain, `1`/`0`, while the stored form stays `ON`/`OFF`.)
#[test]
fn set_global_is_visible_to_a_peer_only_through_the_global_form() {
    let (mut first, mut second, globals) = two_sessions_sharing_globals();

    assert_eq!(
        second.run("SELECT @@autocommit").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    first.run("SET GLOBAL autocommit = OFF").unwrap();

    // The peer's own session copy is untouched...
    assert_eq!(
        second.run("SELECT @@autocommit").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );
    // ...but the peer's @@global read sees it immediately.
    assert_eq!(
        second.run("SELECT @@global.autocommit").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(0)]])
    );

    // A brand new session opened AFTER the SET GLOBAL inherits it as its own
    // session default -- the same snapshot-at-connect step
    // `PipelineSessionFactory::open_session` performs via `attach_globals`.
    let mut fresh = Session::new();
    fresh.attach_globals(globals);
    assert_eq!(
        fresh.run("SELECT @@autocommit").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(0)]])
    );
}

/// `SHOW GLOBAL VARIABLES` reads the shared table live; `SHOW SESSION
/// VARIABLES` (and the unqualified default) reads the session's own copy --
/// so the two diverge after a session-only `SET` exactly as they do after a
/// `SET GLOBAL`.
#[test]
fn show_global_and_session_variables_diverge() {
    let mut session = Session::new();
    session.attach_privileges(privilege::PrivilegeRegistry::default());
    session.attach_globals(vars::GlobalSysvars::new());

    session.run("SET autocommit = OFF").unwrap();
    assert_eq!(
        row_text(session.run("SHOW VARIABLES LIKE 'autocommit'")),
        [["autocommit", "OFF"]]
    );
    // The session-only SET never touched the shared table.
    assert_eq!(
        row_text(session.run("SHOW GLOBAL VARIABLES LIKE 'autocommit'")),
        [["autocommit", "ON"]]
    );

    session.run("SET GLOBAL autocommit = OFF").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GLOBAL VARIABLES LIKE 'autocommit'")),
        [["autocommit", "OFF"]]
    );
}

/// Go's `ErrLocalVariable` (1228): `SET GLOBAL` on a SESSION-only variable.
#[test]
fn set_global_on_a_session_only_variable_is_rejected() {
    let mut session = session_with_privileges();
    session.attach_globals(vars::GlobalSysvars::new());
    let error = session.run("SET GLOBAL debug_sync = 'x'").unwrap_err();
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1228, "{mysql:?}");
}

/// Go's `ErrGlobalVariable` (1229): `SET SESSION` (the unqualified form,
/// here) on a GLOBAL-only variable.
#[test]
fn set_session_on_a_global_only_variable_is_rejected() {
    let mut session = session_with_privileges();
    session.attach_globals(vars::GlobalSysvars::new());
    let error = session
        .run("SET default_password_lifetime = 5")
        .unwrap_err();
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1229, "{mysql:?}");
}

/// Go's `ErrIncorrectGlobalLocalVar` (1238), read side: `SELECT
/// @@global.x` on a SESSION-only variable has no GLOBAL copy to read.
#[test]
fn reading_at_global_scope_on_a_session_only_variable_is_rejected() {
    let mut session = session_with_privileges();
    session.attach_globals(vars::GlobalSysvars::new());
    let error = session.run("SELECT @@global.debug_sync").unwrap_err();
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1238, "{mysql:?}");
}

/// Go's `ErrSpecificAccessDenied` (1227): `SET GLOBAL` needs SUPER or the
/// dynamic `SYSTEM_VARIABLES_ADMIN` privilege. A freshly created account
/// with neither is refused; granting `SYSTEM_VARIABLES_ADMIN` admits it.
#[test]
fn set_global_requires_super_or_system_variables_admin() {
    let registry = privilege::PrivilegeRegistry::default();
    let catalog: SharedCatalog = std::sync::Arc::new(std::sync::Mutex::new(Catalog::default()));

    let mut root = session_as(&registry, catalog.clone(), "root", "%");
    root.run("CREATE USER 'plain'@'%'").unwrap();

    let mut plain = session_as(&registry, catalog, "plain", "%");
    plain.attach_globals(vars::GlobalSysvars::new());
    let error = plain.run("SET GLOBAL autocommit = OFF").unwrap_err();
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1227, "{mysql:?}");

    root.run("GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'plain'@'%'")
        .unwrap();
    // Re-open the session so the newly granted dynamic privilege is what
    // this connection's identity resolves to (matches how every other
    // privilege check in this tier is exercised after a GRANT).
    let mut plain = session_as(&registry, root.shared_catalog(), "plain", "%");
    plain.attach_globals(vars::GlobalSysvars::new());
    plain.run("SET GLOBAL autocommit = OFF").unwrap();
}

/// `tidb_enable_table_partition` and `tidb_enable_list_partition` name a
/// feature that is now ALWAYS ON, and their `Validation` closures say so in
/// two different ways: the first rewrites any assignment to `ON` and warns
/// when someone tried to turn it off, the second refuses. Captured through
/// `gorun`, for both scopes:
///
/// ```text
/// set tidb_enable_table_partition=off;        show warnings;
///   Warning|1105|tidb_enable_table_partition is always turned on. ...
/// show variables like 'tidb_enable_table_partition';        -> ON
/// set global tidb_enable_table_partition=off;
/// show global variables like 'tidb_enable_table_partition'; -> ON
/// set tidb_enable_list_partition=on;          show warnings;
///   Warning|1681|tidb_enable_list_partition is deprecated and will be removed in a future release.
/// set tidb_enable_list_partition=off;
///   Error 1105 (HY000): tidb_enable_list_partition is now always on, and cannot be turned off
/// show variables like 'tidb_enable_list_partition';         -> ON
/// ```
#[test]
fn the_partition_switches_are_always_on() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();

    session
        .run("SET tidb_enable_table_partition = off")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![vec![
            "Warning".to_owned(),
            "1105".to_owned(),
            "tidb_enable_table_partition is always turned on. This variable has been deprecated \
             and will be removed in the future releases"
                .to_owned(),
        ]]
    );
    assert_eq!(
        row_text(session.run("SHOW VARIABLES LIKE 'tidb_enable_table_partition'")),
        vec![vec![
            "tidb_enable_table_partition".to_owned(),
            "ON".to_owned()
        ]]
    );
    session
        .run("SET GLOBAL tidb_enable_table_partition = off")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GLOBAL VARIABLES LIKE 'tidb_enable_table_partition'")),
        vec![vec![
            "tidb_enable_table_partition".to_owned(),
            "ON".to_owned()
        ]]
    );

    session.run("SET tidb_enable_list_partition = on").unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![vec![
            "Warning".to_owned(),
            "1681".to_owned(),
            "tidb_enable_list_partition is deprecated and will be removed in a future release."
                .to_owned(),
        ]]
    );
    let refused = session
        .run("SET tidb_enable_list_partition = off")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(refused.code, 1105, "{refused:?}");
    assert_eq!(
        refused.message,
        "tidb_enable_list_partition is now always on, and cannot be turned off"
    );
    assert_eq!(
        row_text(session.run("SHOW VARIABLES LIKE 'tidb_enable_list_partition'")),
        vec![vec![
            "tidb_enable_list_partition".to_owned(),
            "ON".to_owned()
        ]]
    );
}

/// `tidb_session_alias` is cut to 64 RUNES and then stripped of trailing
/// spaces, because it labels log lines as an identifier. Captured through
/// `gorun`: `set @@tidb_session_alias='abc  '` reads back as `abc`.
#[test]
fn a_session_alias_is_cut_to_64_runes_and_trimmed() {
    let mut session = Session::new();

    let long = "0123456789".repeat(7);
    session
        .run(&format!("SET @@tidb_session_alias = '{long}'"))
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT @@tidb_session_alias")),
        vec![vec![long[..64].to_owned()]]
    );

    // Runes, not bytes: 65 three-byte characters lose exactly the last one.
    let chinese = "中文测试1中文测试2中文测试3中文测试4中文测试5中文测试6中文测试7中文测试8中文测试9中文测试0中文测试a中文测试b中文测试c";
    session
        .run(&format!("SET @@tidb_session_alias = '{chinese}'"))
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT @@tidb_session_alias")),
        vec![vec![chinese.chars().take(64).collect::<String>()]]
    );

    session.run("SET @@tidb_session_alias = 'abc  '").unwrap();
    assert_eq!(
        row_text(session.run("SELECT @@tidb_session_alias")),
        vec![vec!["abc".to_owned()]]
    );

    // The 64-rune cut lands inside a run of spaces, and the identifier trim
    // then removes all of them.
    session
        .run("SET @@tidb_session_alias = 'abc                                                                    1'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT @@tidb_session_alias")),
        vec![vec!["abc".to_owned()]]
    );
}

// ---------------------------------------------------------------------------
// The INSTANCE tier (Go `vardef.ScopeInstance`, 28 variables).
// ---------------------------------------------------------------------------

/// Go `validateScope` (`pkg/sessionctx/variable/variable.go:265`) admits
/// `SET GLOBAL` when `sv.HasGlobalScope() || sv.HasInstanceScope()`, and the
/// value must be READABLE afterwards -- a set that stores where no reader
/// looks is a silent no-op, which is worse than the refusal it replaces.
#[test]
fn set_global_on_an_instance_variable_succeeds_and_reads_back() {
    let (mut first, mut second, _globals) = two_sessions_sharing_globals();
    first.run("SET GLOBAL tidb_general_log = 1").unwrap();
    // Both spellings of the read, on the setting session and on a peer: the
    // instance tier is per NODE, so there is no session copy to lag behind.
    assert_eq!(
        scalar_text(&mut first, "SELECT @@tidb_general_log"),
        Some("1".to_owned())
    );
    assert_eq!(
        scalar_text(&mut first, "SELECT @@global.tidb_general_log"),
        Some("1".to_owned())
    );
    assert_eq!(
        scalar_text(&mut second, "SELECT @@tidb_general_log"),
        Some("1".to_owned())
    );
}

/// `SELECT @@global.max_connections` -- some drivers ask at connect. Go's
/// read path does not run `validateScope`, so an instance-scoped variable
/// answers it.
#[test]
fn reading_at_global_scope_on_an_instance_variable_answers() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.max_connections"),
        Some("0".to_owned())
    );
    session.run("SET GLOBAL max_connections = 512").unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.max_connections"),
        Some("512".to_owned())
    );
}

/// Go `pkg/executor/set.go:152`: an unqualified `SET` on an instance-scoped
/// variable is REWRITTEN to an instance set and warned about with
/// `ErrInstanceScope` (8142), because `DefEnableLegacyInstanceScope = true`.
/// The value must land in the instance tier, not in a session copy nothing
/// reads.
#[test]
fn an_unqualified_set_on_an_instance_variable_warns_8142_and_lands_in_the_tier() {
    let (mut session, mut peer, _globals) = two_sessions_sharing_globals();
    session.run("SET tidb_general_log = 1").unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        [[
            "Warning",
            "8142",
            "modifying tidb_general_log will require SET GLOBAL in a future version of TiDB"
        ]]
    );
    // Node-wide, so the peer sees it: this is what distinguishes the instance
    // tier from a session write.
    assert_eq!(
        scalar_text(&mut peer, "SELECT @@tidb_general_log"),
        Some("1".to_owned())
    );
}

/// The warning reaches the OK packet's count as well as `SHOW WARNINGS` --
/// the two channels a driver can learn from.
#[test]
fn the_instance_scope_warning_is_counted_on_the_wire() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    session.run("SET tidb_general_log = 1").unwrap();
    assert_eq!(session.wire_warning_count(), 1);
}

/// With the legacy rewrite turned OFF, Go's `validateScope` is reached and a
/// SESSION write to an instance-scoped variable is `errGlobalVariable`
/// (1229).
#[test]
fn without_the_legacy_rewrite_a_session_set_on_an_instance_variable_is_1229() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    session
        .run("SET tidb_enable_legacy_instance_scope = OFF")
        .unwrap();
    let error = session.run("SET tidb_general_log = 1").unwrap_err();
    assert_eq!(error.to_mysql_error().code, 1229);
}

/// The guard relaxation must not widen to variables that are genuinely
/// SESSION-only: `SET GLOBAL` on one is still `ErrLocalVariable` (1228).
/// This is the mutation probe for the `has_global_scope() ||
/// has_instance_scope()` condition -- widening it to `true` breaks here.
#[test]
fn a_session_only_variable_still_refuses_set_global() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    let error = session.run("SET GLOBAL debug_sync = 'x'").unwrap_err();
    assert_eq!(error.to_mysql_error().code, 1228);
}

/// An instance-scoped value is NOT cluster state: it must stay out of the
/// map that feeds `mysql.GLOBAL_VARIABLES` persistence and the connect-time
/// session seed. Go writes it to a `vardef` atomic, never a row.
#[test]
fn an_instance_value_is_not_offered_as_cluster_state() {
    let (mut session, _peer, globals) = two_sessions_sharing_globals();
    session.run("SET GLOBAL tidb_general_log = 1").unwrap();
    session.run("SET GLOBAL autocommit = OFF").unwrap();
    let overrides = globals.overrides();
    assert!(!overrides.contains_key("tidb_general_log"), "{overrides:?}");
    assert_eq!(overrides.get("autocommit").map(String::as_str), Some("OFF"));
}

// ---------------------------------------------------------------------------
// Two of the "accepted, stored, never read" variables get their Go contract.
// ---------------------------------------------------------------------------

/// Go `checkIsolationLevel` (`varsutil.go:116`): `SERIALIZABLE` is refused
/// with 8048, on both spellings of the variable.
#[test]
fn an_unsupported_isolation_level_is_refused_8048() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    for sql in [
        "SET SESSION transaction_isolation = 'SERIALIZABLE'",
        "SET SESSION tx_isolation = 'SERIALIZABLE'",
        "SET SESSION transaction_isolation = 'READ-UNCOMMITTED'",
        "SET GLOBAL transaction_isolation = 'SERIALIZABLE'",
    ] {
        let error = session.run(sql).unwrap_err();
        assert_eq!(error.to_mysql_error().code, 8048, "{sql}");
    }
    // Refused means NOT stored: the session keeps its old level rather than
    // reporting a level it is not running at.
    assert_eq!(
        scalar_text(&mut session, "SELECT @@transaction_isolation"),
        Some("REPEATABLE-READ".to_owned())
    );
}

/// The skip switch downgrades the same error to a warning, and the level is
/// then stored and read back -- through the alias too.
#[test]
fn skipping_the_isolation_check_warns_and_stores() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    session
        .run("SET tidb_skip_isolation_level_check = 1")
        .unwrap();
    session
        .run("SET SESSION transaction_isolation = 'SERIALIZABLE'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        [[
            "Warning",
            "8048",
            "The isolation level 'SERIALIZABLE' is not supported. Set \
             tidb_skip_isolation_level_check=1 to skip this error"
        ]]
    );
    assert_eq!(
        scalar_text(&mut session, "SELECT @@transaction_isolation"),
        Some("SERIALIZABLE".to_owned())
    );
    assert_eq!(
        scalar_text(&mut session, "SELECT @@tx_isolation"),
        Some("SERIALIZABLE".to_owned())
    );
}

/// The two ACCEPTED levels are untouched by the new check -- the mutation
/// probe for widening the refusal beyond Go's two names.
#[test]
fn an_accepted_isolation_level_still_stores_and_reads_back() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    session
        .run("SET SESSION transaction_isolation = 'READ-COMMITTED'")
        .unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@transaction_isolation"),
        Some("READ-COMMITTED".to_owned())
    );
    assert!(row_text(session.run("SHOW WARNINGS")).is_empty());
}

/// Go's `max_allowed_packet` `Validation`: a SESSION write is `ErrReadOnly`
/// (1621) even though the variable has session scope for READING.
#[test]
fn setting_max_allowed_packet_at_session_scope_is_refused_1621() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    let error = session
        .run("SET SESSION max_allowed_packet = 1048576")
        .unwrap_err();
    assert_eq!(error.to_mysql_error().code, 1621);
    // The read side is unaffected.
    assert_eq!(
        scalar_text(&mut session, "SELECT @@max_allowed_packet"),
        Some("67108864".to_owned())
    );
}

/// The accepted GLOBAL value is rounded DOWN to a multiple of 1024, with
/// `ErrTruncatedWrongValue` (1292) naming the value as TYPED.
#[test]
fn a_global_max_allowed_packet_is_rounded_down_to_1024() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    session.run("SET GLOBAL max_allowed_packet = 1025").unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        [[
            "Warning",
            "1292",
            "Truncated incorrect max_allowed_packet value: '1025'"
        ]]
    );
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.max_allowed_packet"),
        Some("1024".to_owned())
    );
    // An exact multiple is stored untouched and says nothing.
    session.run("SET GLOBAL max_allowed_packet = 2048").unwrap();
    assert!(row_text(session.run("SHOW WARNINGS")).is_empty());
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.max_allowed_packet"),
        Some("2048".to_owned())
    );
}

/// A system variable's name is case-insensitive on every surface, and the
/// name the user WROTE is echoed back in exactly one of them.
///
/// This is #157's pin, and #157 turned out to be MIS-PREMISED: the claim was
/// that a bare `SET` delivers the name un-lowercased. Every surface reachable
/// from a session was measured against real TiDB with `gorun` and a
/// throwaway `Fields()` probe, and all of them already agree:
///
/// * the stored value -- `SET AUTOCOMMIT=0` is read back by `@@autocommit`;
/// * the registry LOOKUP -- Go lowercases inside `variable.GetSysVar`
///   (`variable.go:519`) and at `executor/set.go:91`, and every write path in
///   [`crate::vars`] lowercases its key before it becomes a map key;
/// * every message that INTERPOLATES the name, which Go renders lowercased:
///   1193 unknown, 1238 read-only, 1231 bad value, 1621 session-read-only,
///   and the 8142 legacy-instance-scope warning;
/// * the COLUMN HEADER, which is the one surface that echoes the written
///   case rather than the canonical one -- `SELECT @@Max_Allowed_Packet` is
///   headed `@@Max_Allowed_Packet` by TiDB, not `@@max_allowed_packet`, and
///   the qualifier keeps its case too.
///
/// Captured (`gorun`, and column names from a throwaway probe over
/// `ResultSet.Fields()`):
///
/// ```text
/// SET AUTOCOMMIT=0                  ; select @@autocommit  -> RS:0
/// set @@SQL_MODE='ANSI_QUOTES'      ; select @@sql_mode    -> RS:ANSI_QUOTES
/// SELECT @@Max_Allowed_Packet                              -> RS:67108864
/// set NoSuchVar=1     -> ERR, Error|1193|Unknown system variable 'nosuchvar'
/// set @@Version=1     -> ERR, Error|1238|Variable 'version' is a read only variable
/// set @@SESSION.MAX_CONNECTIONS=10
///   -> Warning|8142|modifying max_connections will require SET GLOBAL in a future version of TiDB
///
/// COLS: @@Max_Allowed_Packet
/// COLS: @@max_allowed_packet
/// COLS: @@SESSION.Sql_Mode
/// COLS: @@GLOBAL.Max_Connections
/// COLS: @@AutoCommit | @@sql_MODE
/// ```
///
/// MEASURED NEGATIVE found while probing, a DIFFERENT divergence and not
/// #157: `set @@Max_Allowed_Packet=100` is `Error|1621|...` in both, but Go
/// ALSO leaves `Warning|1292|Truncated incorrect max_allowed_packet value:
/// '100'` because it validates before it checks the session-read-only guard.
/// This tier checks the guard first and raises no 1292. Not fixed here.
#[test]
fn a_sysvar_name_is_case_insensitive_but_the_column_header_keeps_its_case() {
    let mut session = Session::new();

    // The stored value survives a case change in either direction.
    session.run("SET AUTOCOMMIT=0").unwrap();
    assert_eq!(
        row_text(session.run("SELECT @@autocommit")),
        [["0".to_owned()]]
    );
    session.run("set @@SQL_MODE='ANSI_QUOTES'").unwrap();
    assert_eq!(
        row_text(session.run("select @@sql_mode")),
        [["ANSI_QUOTES".to_owned()]]
    );
    assert_eq!(
        row_text(session.run("SELECT @@Max_Allowed_Packet")),
        [["67108864".to_owned()]]
    );

    // Every message that names the variable names it LOWERCASED, whatever
    // case the statement wrote.
    for (sql, message) in [
        ("set NoSuchVar=1", "Unknown system variable 'nosuchvar'"),
        ("select @@NoSuchVar", "Unknown system variable 'nosuchvar'"),
        (
            "set @@Version=1",
            "Variable 'version' is a read only variable",
        ),
        (
            "set @@SQL_MODE='NO_SUCH_MODE'",
            "Variable 'sql_mode' can't be set to the value of 'NO_SUCH_MODE'",
        ),
    ] {
        assert_eq!(
            session.run(sql).unwrap_err().to_mysql_error().message,
            message,
            "{sql}"
        );
    }
    // ... including a WARNING that names it.
    session.run("set @@SESSION.MAX_CONNECTIONS=10").unwrap();
    assert_eq!(
        session
            .warnings()
            .iter()
            .map(|w| (w.code, w.message.clone()))
            .collect::<Vec<_>>(),
        vec![(
            8142,
            "modifying max_connections will require SET GLOBAL in a future version of TiDB"
                .to_owned()
        )]
    );

    // The COLUMN HEADER is the exception: it echoes what was written, scope
    // qualifier included.
    for (sql, header) in [
        ("SELECT @@Max_Allowed_Packet", "@@Max_Allowed_Packet"),
        ("SELECT @@max_allowed_packet", "@@max_allowed_packet"),
        ("SELECT @@SESSION.Sql_Mode", "@@SESSION.Sql_Mode"),
        (
            "SELECT @@GLOBAL.Max_Connections",
            "@@GLOBAL.Max_Connections",
        ),
    ] {
        let StmtOutput::Rows { columns, .. } = session.run_with_columns(sql).unwrap() else {
            panic!("{sql} is a row set");
        };
        assert_eq!(
            columns.iter().map(|c| c.0.as_str()).collect::<Vec<_>>(),
            [header],
            "{sql}"
        );
    }
}

/// #181: Go's overflow message ends `in '<expr>'`, naming the expression, and
/// this tier stops at the class. The seam is REPORTED here rather than
/// half-built, because two separate pieces of plumbing are missing and an
/// approximation would diverge on both.
///
/// # Capture (throwaway probe printing `err.Error()`; `gorun` prints bare ERR)
///
/// Schema `t(a bigint, b bigint)`, row `(9223372036854775807, 2)`:
///
/// ```text
/// select 9223372036854775807 + 1
///   [types:1690]BIGINT value is out of range in '(9223372036854775807 + 1)'
/// select 9223372036854775807 * 2
///   [types:1690]BIGINT value is out of range in '(9223372036854775807 * 2)'
/// select -9223372036854775808 - 1
///   [types:1690]BIGINT value is out of range in '(-9223372036854775808 - 1)'
/// select a + b from t
///   [types:1690]BIGINT value is out of range in '(test.t.a + test.t.b)'
/// select a + 1 from t
///   [types:1690]BIGINT value is out of range in '(test.t.a + 1)'
/// select a+b as x from t          -- the ALIAS is not in the text
///   [types:1690]BIGINT value is out of range in '(test.t.a + test.t.b)'
/// select abs(-9223372036854775808)
///   [types:1690]BIGINT value is out of range in 'abs(-9223372036854775808)'
/// select 1e308 + 1e308
///   [types:1690]DOUBLE value is out of range in '(1e+308 + 1e+308)'
/// ```
///
/// # Where the text comes from in Go, and why it cannot be restored here
///
/// It is built AT THE SIGNATURE, not from the statement: `builtin_arithmetic.go`
/// writes `fmt.Sprintf("(%s + %s)", s.args[0].StringWithCtx(...),
/// s.args[1].StringWithCtx(...))` -- each ARGUMENT's own `Expression.String()`,
/// after resolution. Two consequences make a hand-written approximation wrong:
///
/// * a column renders FULLY QUALIFIED, `test.t.a`, which is resolution output.
///   This tier's rewritten `tidb_expr::Expr::Column` holds the path AS
///   WRITTEN (`["a"]`); the resolved `db.table.column` lives in the
///   executor's `FromScope`, and the `Columns` trait the evaluator holds
///   exposes no way to ask for it. So the qualifier is not merely unformatted
///   here -- it is not present at the raising frame at all.
/// * a literal renders as Go's own formatting of the VALUE, not the source
///   text: `1e308` in the statement comes back as `1e+308`. An AST restore of
///   the source diverges even for a constant-only expression, which is the
///   case that otherwise looks trivially portable.
///
/// #74's generated-column `expr_text` is NOT reusable: it is restored with
/// `WITHOUT_SCHEMA_NAME | WITHOUT_TABLE_NAME | NAME_BACK_QUOTES`, so it
/// produces `` (`a` + 1) `` -- it strips exactly the qualifiers this message
/// requires and back-quotes the names it leaves bare.
///
/// # The exact insertion point, when the plumbing exists
///
/// `tidb_expr::eval_in`'s `Expr::Binary(op, l, r)` arm
/// (`crates/tidb-expr/src/lib.rs`, the
/// `eval_binary_with_div_precision(*op, ...)` call): that frame holds the
/// operator and BOTH argument expressions, which is Go's `s.args`. What it
/// still needs is a way to render one argument the way Go's
/// `Expression.String()` does -- which means the resolver must record the
/// qualified name on the rewritten `Column` node, or `Columns` must be able
/// to answer it for a path.
#[test]
fn an_overflow_names_its_class_but_not_yet_its_expression() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
    session
        .run("INSERT INTO t VALUES (9223372036854775807, 2)")
        .unwrap();

    for sql in [
        "SELECT 9223372036854775807 + 1",
        "SELECT 9223372036854775807 * 2",
        "SELECT a + b FROM t",
        "SELECT a + 1 FROM t",
    ] {
        let error = session.run(sql).unwrap_err().to_mysql_error();
        assert_eq!(error.code, 1690, "{sql}");
        assert_eq!(&error.state, b"22003", "{sql}");
        // DIVERGENCE (#181): Go appends ` in '<expr>'` here.
        assert_eq!(error.message, "BIGINT value is out of range", "{sql}");
    }

    let error = session
        .run("SELECT 1e308 + 1e308")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1690);
    // DIVERGENCE (#181): Go says `DOUBLE value is out of range in
    // '(1e+308 + 1e+308)'` -- note `1e+308`, the VALUE's formatting, not the
    // `1e308` the statement wrote.
    assert_eq!(error.message, "DOUBLE value is out of range");
}
