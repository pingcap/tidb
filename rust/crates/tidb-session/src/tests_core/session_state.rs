//! Session state a statement reads or changes: system variables, `sql_mode`,
//! the noop-function gate, `sql_select_limit`, the `SET_VAR` hint, and
//! prepared-statement parameters -- Go `pkg/sessionctx/variable`.

use tidb_planner::stmt_ctx::PlannerStmtCtx as _;
use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use crate::tests_support::*;
use crate::*;

#[test]
fn resource_group_is_session_scoped_and_statement_hint_overrides_once() {
    let mut session = Session::new();
    assert_eq!(session.current_resource_group(), "default");
    // Go `executeSetResourceGroupName`: an unknown group is 8249.
    assert_eq!(
        session
            .run("SET RESOURCE GROUP analytics")
            .unwrap_err()
            .to_mysql_error()
            .message,
        "Unknown resource group 'analytics'"
    );
    for name in ["analytics", "burst"] {
        session
            .run(&format!("CREATE RESOURCE GROUP {name} RU_PER_SEC = 1000"))
            .unwrap();
    }

    // Go `SimpleExec.executeSetResourceGroupName`.
    session.run("SET RESOURCE GROUP analytics").unwrap();
    assert_eq!(session.current_resource_group(), "analytics");

    let hinted = session
        .parse_statement("SELECT /*+ RESOURCE_GROUP(Burst) */ 1")
        .unwrap();
    assert_eq!(session.statement_resource_group(&hinted), "Burst");
    assert_eq!(session.current_resource_group(), "analytics");
    session.activate_statement_resource_group(&hinted);
    assert_eq!(session.active_resource_group(), "burst");
    assert_eq!(
        session.statement_context(false).resource_group_name(),
        "burst"
    );

    // Go `ExecuteStmt`: a hinted group that does not exist falls back to
    // the session's group.
    let unknown = session
        .parse_statement("SELECT /*+ RESOURCE_GROUP(nosuch) */ 1")
        .unwrap();
    session.activate_statement_resource_group(&unknown);
    assert_eq!(session.active_resource_group(), "analytics");

    let ordinary = session.parse_statement("SELECT 1").unwrap();
    assert_eq!(session.statement_resource_group(&ordinary), "analytics");
    session.activate_statement_resource_group(&ordinary);
    assert_eq!(session.active_resource_group(), "analytics");
    assert_eq!(
        session.statement_context(false).resource_group_name(),
        "analytics"
    );
}

/// Pinned Go `pkg/planner/optimize.go`: RESOURCE_GROUP is an optimizer
/// postcondition, not an unconditional parser-side override.  Disabled
/// resource control leaves the session group in force and emits 8250.
#[test]
fn resource_group_hint_is_ignored_when_resource_control_is_disabled() {
    let mut session = Session::new();
    session
        .run("SET GLOBAL tidb_enable_resource_control = OFF")
        .unwrap();

    session
        .run("SELECT /*+ RESOURCE_GROUP(analytics) */ 1")
        .unwrap();

    assert_eq!(session.active_resource_group(), "default");
    assert_eq!(
        warnings_of(&session),
        vec![(
            8250,
            "Resource control feature is disabled. Run `SET GLOBAL tidb_enable_resource_control='on'` to enable the feature".to_owned(),
        )]
    );
}

/// Pinned Go `pkg/planner/optimize.go`: strict resource control accepts
/// SUPER (through the dynamic-privilege fallback), RESOURCE_GROUP_ADMIN, or
/// RESOURCE_GROUP_USER.  A connected account with none keeps its session
/// group and receives the ordinary 1227 warning.
#[test]
fn strict_resource_group_hint_requires_resource_group_authority() {
    let privileges = tidb_privilege::PrivilegeRegistry::default();
    let mut session = authenticated_session(&privileges, "ordinary", "%");

    session
        .run("SELECT /*+ RESOURCE_GROUP(analytics) */ 1")
        .unwrap();

    assert_eq!(session.active_resource_group(), "default");
    assert_eq!(
        warnings_of(&session),
        vec![(
            1227,
            "Access denied; you need (at least one of) the SUPER or RESOURCE_GROUP_ADMIN or RESOURCE_GROUP_USER privilege(s) for this operation".to_owned(),
        )]
    );
}

/// Pinned Go `pkg/session/test/session_test.go:TestStmtHints`: statement-level
/// fields come from the one `hint.ParseStmtHints` result retained in StmtCtx.
#[test]
fn statement_hints_use_the_canonical_parse_result() {
    let mut session = Session::new();

    session.run("SELECT /*+ MEMORY_QUOTA(1 MB) */ 1").unwrap();
    assert!(session.stmt_hints.has_mem_quota_hint);
    assert_eq!(session.stmt_hints.mem_quota_query, 1024 * 1024);
    assert_eq!(
        session.statement_context(false).statement_memory().quota(),
        1024 * 1024,
    );

    session.run("SELECT /*+ NO_INDEX_MERGE() */ 1").unwrap();
    assert!(session.stmt_hints.no_index_merge_hint);
    assert!(!session.statement_context(false).index_merge());

    session.run("SELECT /*+ STRAIGHT_JOIN() */ 1").unwrap();
    assert!(session.stmt_hints.straight_join_order);

    session.run("SELECT /*+ USE_TOJA(false) */ 1").unwrap();
    assert!(session.stmt_hints.has_allow_in_subq_to_join_and_agg_hint);
    assert!(!session.stmt_hints.allow_in_subq_to_join_and_agg);
    assert!(!session
        .statement_context(false)
        .allow_in_subq_to_join_and_agg());

    session.run("SELECT /*+ USE_CASCADES(true) */ 1").unwrap();
    assert!(session.stmt_hints.has_enable_cascades_planner_hint);
    assert!(session.stmt_hints.enable_cascades_planner);

    session
        .run("SELECT /*+ READ_CONSISTENT_REPLICA() */ 1")
        .unwrap();
    assert!(session.stmt_hints.has_replica_read_hint);
    assert_eq!(session.stmt_hints.replica_read, 1);
    assert_eq!(
        session.statement_context(false).replica_read(),
        tidb_txnkv::ReplicaReadType::Follower,
    );

    // Statement-local fields are not part of the generation-cached session
    // variable snapshot. An unhinted successor immediately sees the
    // persistent session values again.
    session.run("SELECT 1").unwrap();
    let ctx = session.statement_context(false);
    assert!(ctx.index_merge());
    assert!(ctx.allow_in_subq_to_join_and_agg());
    assert_eq!(ctx.replica_read(), tidb_txnkv::ReplicaReadType::Leader,);
    assert_eq!(
        ctx.statement_memory().quota(),
        session
            .vars
            .get_system("tidb_mem_quota_query")
            .unwrap()
            .parse::<i64>()
            .unwrap(),
    );
}

#[test]
fn replica_read_sysvar_reaches_statement_snapshot() {
    let mut session = Session::new();
    if tidb_config_kerneltype::is_next_gen() {
        assert!(session.run("SET tidb_replica_read = 'follower'").is_err());
        return;
    }

    session.run("SET tidb_replica_read = 'follower'").unwrap();
    assert_eq!(
        session.statement_context(false).replica_read(),
        tidb_txnkv::ReplicaReadType::Follower,
    );
    session
        .run("SET tidb_replica_read = 'leader-and-follower'")
        .unwrap();
    assert_eq!(
        session.statement_context(false).replica_read(),
        tidb_txnkv::ReplicaReadType::Mixed,
    );
}

/// Pinned Go `pkg/session/test/variable/variable_test.go::TestIsolationRead`:
/// the session variable is the source of the planner's per-statement engine
/// set, not a catalog-side preference.
#[test]
fn isolation_read_engines_reach_the_statement_context() {
    let mut session = Session::new();
    assert_eq!(
        session.statement_context(false).isolation_read_engines(),
        "tikv,tiflash,tidb"
    );

    session
        .run("SET @@session.tidb_isolation_read_engines = 'tiflash'")
        .unwrap();
    assert_eq!(
        session.statement_context(false).isolation_read_engines(),
        "tiflash"
    );
}

/// SET and the variable reads a connecting client performs.
#[test]
fn session_variables() {
    let mut session = Session::new();

    // A stock client's opening statements.
    assert_eq!(session.apply_set("SET NAMES utf8mb4").unwrap(), Some(()));
    assert_eq!(
        session.vars().get_system("character_set_client").unwrap(),
        "utf8mb4"
    );
    assert_eq!(session.apply_set("SET autocommit = 0").unwrap(), Some(()));
    // Go's checkBoolSystemVar canonicalizes 0/1 to OFF/ON.
    assert_eq!(session.vars().get_system("autocommit").unwrap(), "OFF");

    // Reading variables back through a query. The STORED form is `OFF`, but
    // Go's `GetNativeValType` gives a `TypeBool` variable the integer domain,
    // so the query reports `0` (confirmed against Go: `SELECT @@autocommit`
    // is `1` on a fresh session, never `ON`).
    assert_eq!(
        scalar_text(&mut session, "SELECT @@autocommit"),
        Some("0".to_owned())
    );
    let comment = scalar_text(&mut session, "SELECT @@version_comment").unwrap();
    assert!(
        comment.starts_with("TiDB Server (Apache License 2.0)"),
        "{comment}"
    );

    // DEFAULT restores the registry default.
    session.apply_set("SET autocommit = DEFAULT").unwrap();
    assert_eq!(session.vars().get_system("autocommit").unwrap(), "ON");

    // An unknown system variable is Go's 1193, on read and on write.
    crate::tests_support::assert_err(
        session.apply_set("SET nonexistent_variable = 1"),
        1193,
        "Unknown system variable 'nonexistent_variable'",
    );
    crate::tests_support::assert_err(
        session.run("SELECT @@nonexistent_variable"),
        1193,
        "Unknown system variable 'nonexistent_variable'",
    );
    // A read-only variable cannot be set.
    crate::tests_support::assert_err(
        session.apply_set("SET version = '1'"),
        1238,
        "Variable 'version' is a read only variable",
    );

    // User variables: unset reads as NULL, never an error.
    assert_eq!(
        session.run("SELECT @nope").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Null]])
    );
    session.apply_set("SET @x = 41 + 1").unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @x"),
        Some("42".to_owned())
    );

    // A non-SET statement is not claimed by the hook.
    assert_eq!(session.apply_set("SELECT 1").unwrap(), None);
}

/// Hash-join versions are source string variables with a closed value domain:
/// casing is accepted and retained, while every other spelling is refused by
/// the variable-specific validation closure.
#[test]
fn hash_join_versions_accept_only_legacy_or_optimized() {
    let mut session = Session::new();

    for (name, default) in [
        ("tidb_hash_join_version", "optimized"),
        ("tiflash_hash_join_version", "legacy"),
    ] {
        assert_eq!(
            scalar_text(&mut session, &format!("SELECT @@{name}")),
            Some(default.to_owned())
        );

        for value in ["Legacy", "OptimiZed"] {
            session
                .apply_set(&format!("SET {name} = '{value}'"))
                .unwrap();
            assert_eq!(
                scalar_text(&mut session, &format!("SELECT @@{name}")),
                Some(value.to_owned())
            );
            if name == "tidb_hash_join_version" {
                for is_dml in [false, true] {
                    assert_eq!(
                        session
                            .statement_context(is_dml)
                            .optimizer_cost_env()
                            .session
                            .use_hash_join_v2,
                        value.eq_ignore_ascii_case("optimized")
                    );
                }
            }
        }

        for value in ["invalid", "v2", "optimized "] {
            let error = session
                .apply_set(&format!("SET {name} = '{value}'"))
                .unwrap_err()
                .to_mysql_error();
            assert_eq!(error.code, 1105);
            assert_eq!(error.state, *b"HY000");
            assert_eq!(
                error.message,
                format!("incorrect value: `{value}`. {name} options: legacy, optimized")
            );
            assert_eq!(
                scalar_text(&mut session, &format!("SELECT @@{name}")),
                Some("OptimiZed".to_owned()),
                "a refused SET must leave the previous value intact"
            );
        }
    }

    // Go outer-table build and probe-filter cases must use the same session
    // selection boundary for both query and INSERT SELECT contexts.
    session
        .run("create table join_version_a (k int, v int)")
        .unwrap();
    session
        .run("create table join_version_b (k int, v int)")
        .unwrap();
    session
        .run("insert into join_version_a values (null,0),(1,1),(1,2),(2,3)")
        .unwrap();
    session
        .run("insert into join_version_b values (1,8),(1,9),(2,10)")
        .unwrap();
    for version in ["legacy", "optimized"] {
        session
            .apply_set(&format!("set tidb_hash_join_version='{version}'"))
            .unwrap();
        for vectorized in [false, true] {
            session
                .apply_set(&format!(
                    "set tidb_enable_vectorized_expression={}",
                    u8::from(vectorized)
                ))
                .unwrap();
            for is_dml in [false, true] {
                assert_eq!(
                    session
                        .statement_context(is_dml)
                        .enable_vectorized_expression(),
                    vectorized
                );
            }
            for build in ["a", "b"] {
                let sql = format!("select /*+ HASH_JOIN_BUILD({build}) */ a.k,b.v from join_version_a a left join join_version_b b on a.k=b.k and a.v>1 order by a.v,b.v");
                assert_eq!(
                    row_text(session.run(&sql)),
                    vec![
                        vec!["NULL", "NULL"],
                        vec!["1", "NULL"],
                        vec!["1", "8"],
                        vec!["1", "9"],
                        vec!["2", "10"],
                    ],
                    "version={version}, vectorized={vectorized}, build={build}"
                );
            }
        }
    }
}

/// Pinned `sysvar.go`'s `TiDBAnalyzeVersion` entry: the compatibility
/// variable still has the integer range `[1, 2]`, but its validation closure
/// refuses v1 because the planner always uses Analyze v2.
#[test]
fn analyze_version_one_is_no_longer_supported() {
    let mut session = Session::new();

    let error = session
        .apply_set("SET tidb_analyze_version = 1")
        .expect_err("Analyze v1 must be refused")
        .to_mysql_error();
    assert_eq!(error.code, 1105);
    assert_eq!(error.state, *b"HY000");
    assert_eq!(
        error.message,
        "tidb_analyze_version=1 is no longer supported, please set tidb_analyze_version to 2"
    );
    assert_eq!(
        scalar_text(&mut session, "SELECT @@tidb_analyze_version"),
        Some("2".to_owned()),
        "a refused SET leaves the v2 default intact"
    );
}

/// `sql_mode` is normalized at SET time, so every reader afterwards sees the
/// expanded, canonical set rather than the shorthand the user typed.
///
/// Captured from TiDB (`SET sql_mode='TRADITIONAL'; SELECT @@sql_mode`): the
/// combination expands to its member modes AND keeps its own name at the end.
/// Without this normalization `SET sql_mode='TRADITIONAL'` left the literal
/// string `TRADITIONAL` stored, and every reader looking for
/// `STRICT_TRANS_TABLES` silently found a NON-strict session -- an over-long
/// INSERT then truncated and stored a wrong value instead of failing.
#[test]
fn sql_mode_is_normalized_when_it_is_set() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (c VARCHAR(3))").unwrap();

    session.apply_set("SET sql_mode = 'TRADITIONAL'").unwrap();
    assert_eq!(
        session.run("SELECT @@sql_mode").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string(
            "STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,\
             ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,TRADITIONAL"
        )]])
    );
    // Captured: `[types:1406]Data too long for column 'c' at row 1`, and the
    // row is NOT stored.
    assert!(session.run("INSERT INTO t VALUES ('abcdef')").is_err());
    assert_eq!(
        session.run("SELECT c FROM t").unwrap(),
        StmtResult::Rows(vec![])
    );

    // Captured: ANSI expands to its five member modes plus its own name, which
    // is how ONLY_FULL_GROUP_BY comes along with it.
    session.apply_set("SET sql_mode = 'ansi'").unwrap();
    assert_eq!(
        session.run("SELECT @@sql_mode").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string(
            "REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"
        )]])
    );

    // A non-combination mode is only uppercased, and duplicates collapse.
    session
        .apply_set("SET sql_mode = 'ansi_quotes,ANSI_QUOTES'")
        .unwrap();
    assert_eq!(
        session.run("SELECT @@sql_mode").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("ANSI_QUOTES")]])
    );

    // Captured: an unknown mode and a numeric bitmask are both 1231, and the
    // message names the offending TOKEN rather than the whole assignment.
    for (value, token) in [
        ("NO_SUCH_MODE", "NO_SUCH_MODE"),
        ("2097152", "2097152"),
        ("ANSI_QUOTES,bogus", "BOGUS"),
        // Captured: only TRAILING spaces are trimmed, so a leading one makes
        // the whole token invalid.
        (" STRICT_TRANS_TABLES", " STRICT_TRANS_TABLES"),
    ] {
        // A `Validation` closure's own `errors.Errorf`: unclassed, 1105.
        let error = session
            .apply_set(&format!("SET sql_mode = '{value}'"))
            .expect_err("the value must be rejected")
            .to_mysql_error();
        assert_eq!(error.identity, None, "for {value}");
        assert_eq!(
            error.message,
            format!(
                "ERROR 1231 (42000): Variable 'sql_mode' can't be set to the value of '{token}'"
            ),
            "for {value}"
        );
    }
    // A rejected SET left the previous value in place.
    assert_eq!(
        session.run("SELECT @@sql_mode").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("ANSI_QUOTES")]])
    );
}

/// The clauses TiDB parses but only implements as no-ops, checked
/// against captured TiDB output with `tidb_enable_noop_functions` at its
/// `OFF` default.
///
/// The real locking executor is still outside this tier, but the
/// `tidb_enable_shared_lock_promotion` admission rewrite is covered by the
/// focused regression below.
#[test]
fn noop_function_gate() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1, 10), (2, 20)")
        .unwrap();

    // Captured: FOR UPDATE runs and returns the rows.
    assert_eq!(
        session
            .run("SELECT b FROM t WHERE a = 1 FOR UPDATE")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(10)]])
    );
    // Its waiting options only shape a lock this tier does not take.
    session.run("SELECT b FROM t FOR UPDATE NOWAIT").unwrap();
    session.run("SELECT b FROM t FOR UPDATE OF t").unwrap();

    // Captured: the shared lock and SQL_CALC_FOUND_ROWS are 1235.
    for sql in [
        "SELECT b FROM t FOR SHARE",
        "SELECT b FROM t LOCK IN SHARE MODE",
        "SELECT SQL_CALC_FOUND_ROWS b FROM t LIMIT 1",
        "SELECT b FROM t GROUP BY b DESC",
    ] {
        assert!(
            session.run(sql).is_err_and(
                |error| error.equal(&tidb_error::catalog::expression::ERR_FUNCTIONS_NOOP_IMPL)
            ),
            "expected a noop-function error from {sql}"
        );
    }
    // An explicit ASC is written too, so it is gated the same way.
    crate::tests_support::assert_err(session.run("SELECT b FROM t GROUP BY b ASC"), 1235, "function GROUP BY expr ASC|DESC has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions");
    // A GROUP BY with no direction is not.
    session.run("SELECT b FROM t GROUP BY b").unwrap();

    // The gate reaches a subquery, a derived table and a set operation.
    crate::tests_support::assert_err(session.run("SELECT b FROM t WHERE a IN (SELECT a FROM t LOCK IN SHARE MODE)"), 1235, "function LOCK IN SHARE MODE has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions");
    crate::tests_support::assert_err(session.run("SELECT x.b FROM (SELECT b FROM t LOCK IN SHARE MODE) x"), 1235, "function LOCK IN SHARE MODE has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions");
    crate::tests_support::assert_err(session.run("SELECT b FROM t UNION SELECT a FROM t LOCK IN SHARE MODE"), 1235, "function LOCK IN SHARE MODE has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions");

    // ON: the clause is accepted and does nothing, with no warning.
    session
        .apply_set("SET tidb_enable_noop_functions = 'ON'")
        .unwrap();
    session.run("SELECT b FROM t LOCK IN SHARE MODE").unwrap();
    assert!(session.warnings().is_empty());

    // WARN: accepted, with the same message as a warning.
    session
        .apply_set("SET tidb_enable_noop_functions = 'WARN'")
        .unwrap();
    session.run("SELECT b FROM t LOCK IN SHARE MODE").unwrap();
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code(), 1235);
    assert!(session.warnings()[0].message().contains("LOCK IN SHARE MODE"));
    // The warnings belong to the last statement only.
    session.run("SELECT b FROM t").unwrap();
    assert!(session.warnings().is_empty());
}

/// Go `preprocess.checkSelectNoopFuncs`: enabling shared-lock promotion turns
/// `FOR SHARE` into the real `FOR UPDATE` path before the no-op gate, so the
/// clause is accepted even while `tidb_enable_noop_functions` remains OFF.
#[test]
fn shared_lock_promotion_bypasses_noop_share_gate() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT)")
        .unwrap();
    session.run("INSERT INTO t VALUES (1, 10)").unwrap();

    crate::tests_support::assert_err(session.run("SELECT b FROM t WHERE a = 1 FOR SHARE"), 1235, "function LOCK IN SHARE MODE has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions");
    session
        .run("SET tidb_enable_shared_lock_promotion = ON")
        .unwrap();
    assert!(session.vars().shared_lock_promotion_enabled());
    session
        .run("SELECT b FROM t WHERE a = 1 FOR SHARE")
        .unwrap();
    assert!(session.warnings().is_empty());

    session
        .run("SET tidb_enable_shared_lock_promotion = OFF")
        .unwrap();
    assert!(!session.vars().shared_lock_promotion_enabled());
    crate::tests_support::assert_err(session.run("SELECT b FROM t WHERE a = 1 LOCK IN SHARE MODE"), 1235, "function LOCK IN SHARE MODE has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions");
}

/// Go `varsutil.go:checkReadOnly`: turning one of `noop.go`'s read-only
/// variables ON needs `tidb_enable_noop_functions`, because the server does
/// not actually stop writes.
///
/// Captured from TiDB (`testkit`, `pkg/executor`) at the `OFF` default:
///
/// ```text
/// set transaction_read_only = 1  ERR errno=1235 "[variable:1235]function READ ONLY has only
///                                noop implementation in tidb now, use
///                                tidb_enable_noop_functions to enable these functions"
/// set tx_read_only = 1           ERR errno=1235 (same message)
/// set tidb_enable_noop_functions = warn   OK
/// set tx_read_only = 1                    OK
/// select @@tx_read_only, @@transaction_read_only   [[1 1]]
/// ```
#[test]
fn read_only_noop_variables_need_the_noop_gate() {
    let mut session = Session::new();
    /// The one row a `SELECT @@...` returns, as text.
    fn read(session: &mut Session, sql: &str) -> Vec<String> {
        match session.run(sql).unwrap() {
            StmtResult::Rows(rows) => rows[0].iter().map(|d| format!("{d:?}")).collect(),
            other => panic!("expected rows from {sql}, got {other:?}"),
        }
    }

    // The default is OFF for both spellings, and neither can be turned ON.
    assert_eq!(
        read(
            &mut session,
            "SELECT @@tx_read_only, @@transaction_read_only"
        ),
        ["Int(0)", "Int(0)"]
    );
    for sql in ["SET tx_read_only = 1", "SET transaction_read_only = 1"] {
        let error = session.run(sql).expect_err(sql).to_mysql_error();
        assert_eq!(
            (error.code, error.message.as_str()),
            (
                1235,
                "function READ ONLY has only noop implementation in tidb now, use \
                 tidb_enable_noop_functions to enable these functions"
            ),
            "{sql}"
        );
    }
    // The refusal leaves the value alone -- including the alias's copy.
    assert_eq!(
        read(
            &mut session,
            "SELECT @@tx_read_only, @@transaction_read_only"
        ),
        ["Int(0)", "Int(0)"]
    );

    // Turning one OFF is never gated.
    session.run("SET tx_read_only = 0").unwrap();

    // WARN accepts the value and reports the same message as a warning.
    session
        .apply_set("SET tidb_enable_noop_functions = 'WARN'")
        .unwrap();
    session.run("SET tx_read_only = 1").unwrap();
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code(), 1235);
    assert!(session.warnings()[0].message().contains("READ ONLY"));
    assert_eq!(
        read(
            &mut session,
            "SELECT @@tx_read_only, @@transaction_read_only"
        ),
        ["Int(1)", "Int(1)"]
    );

    // ON accepts it silently, and `SET TRANSACTION READ ONLY` -- the same
    // assignment under another spelling -- goes through the same gate.
    session
        .apply_set("SET tidb_enable_noop_functions = 'ON'")
        .unwrap();
    session.run("SET TRANSACTION READ WRITE").unwrap();
    assert_eq!(read(&mut session, "SELECT @@tx_read_only"), ["Int(0)"]);
    session.run("SET TRANSACTION READ ONLY").unwrap();
    assert!(session.warnings().is_empty());
    assert_eq!(read(&mut session, "SELECT @@tx_read_only"), ["Int(1)"]);

    // Back at OFF, `SET TRANSACTION READ ONLY` is refused too: it is one and
    // the same `tx_read_only = 1`.
    session.run("SET TRANSACTION READ WRITE").unwrap();
    session
        .apply_set("SET tidb_enable_noop_functions = 'OFF'")
        .unwrap();
    crate::tests_support::assert_err(session.run("SET TRANSACTION READ ONLY"), 1235, "function READ ONLY has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions");
}

/// Go's `tidb_enable_noop_functions` Validation rejects disabling the SESSION
/// gate while a same-scope no-op read-only variable is still ON.
#[test]
fn noop_gate_cannot_be_disabled_while_session_read_only_is_on() {
    let mut session = Session::new();
    session
        .apply_set("SET tidb_enable_noop_functions = 'ON'")
        .unwrap();
    session.run("SET tx_read_only = ON").unwrap();

    let error = session
        .run("SET tidb_enable_noop_functions = OFF")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1235);
    assert_eq!(
        error.message,
        "tidb_enable_noop_functions = OFF is not supported when tx_read_only = ON"
    );
    assert_eq!(
        session.run("SELECT @@tidb_enable_noop_functions").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("ON")]])
    );

    session.run("SET tx_read_only = OFF").unwrap();
    session
        .apply_set("SET tidb_enable_noop_functions = 'OFF'")
        .unwrap();
}

/// Go `preprocess.go:TryAddExtraLimit`: `sql_select_limit` caps a top-level
/// SELECT or set operation that writes no LIMIT of its own, and nothing else.
///
/// Captured from TiDB (`testkit`, `pkg/executor`) with three rows 1,2,3 in
/// `sslt` and `set @@sql_select_limit = 2`:
///
/// ```text
/// select a from sslt order by a                    [[1] [2]]
/// select a from sslt order by a limit 3            [[1] [2] [3]]
/// select a from sslt order by a limit 1            [[1]]
/// select a from sslt order by a limit 1, 5         [[2] [3]]
/// select 1 union all select 2 union all select 3   two rows
/// select (select count(*) from sslt)               [[3]]
/// select a from (select a from sslt order by a) d order by a   [[1] [2]]
/// select count(*) from sslt                        [[3]]
/// select * from sslt where a in (select a from sslt)           [[1] [2]]
/// set @@sql_select_limit = 0   -> select a from sslt order by a  []
/// set @@sql_select_limit = default -> [[1] [2] [3]]
/// ```
#[test]
fn sql_select_limit_caps_a_statement_that_wrote_no_limit() {
    let mut session = Session::new();
    session.run("CREATE TABLE sslt (a INT)").unwrap();
    session
        .run("INSERT INTO sslt VALUES (1), (2), (3)")
        .unwrap();
    // The default is MaxUint64, which caps nothing.
    assert_eq!(
        row_text(session.run("SELECT a FROM sslt ORDER BY a")),
        [["1"], ["2"], ["3"]]
    );

    session.apply_set("SET @@sql_select_limit = 2").unwrap();
    assert_eq!(
        row_text(session.run("SELECT a FROM sslt ORDER BY a")),
        [["1"], ["2"]]
    );
    // A statement that writes its OWN limit is left alone, even one asking
    // for more rows than the cap.
    assert_eq!(
        row_text(session.run("SELECT a FROM sslt ORDER BY a LIMIT 3")),
        [["1"], ["2"], ["3"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM sslt ORDER BY a LIMIT 1")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM sslt ORDER BY a LIMIT 1, 5")),
        [["2"], ["3"]]
    );
    // A set operation is capped at the statement level.
    assert_eq!(
        row_text(session.run("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")).len(),
        2
    );

    // Only the TOP-LEVEL statement is capped: a scalar subquery, a derived
    // table and an IN subquery all still see every row.
    assert_eq!(
        row_text(session.run("SELECT (SELECT COUNT(*) FROM sslt)")),
        [["3"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM (SELECT a FROM sslt ORDER BY a) d ORDER BY a")),
        [["1"], ["2"]]
    );
    assert_eq!(row_text(session.run("SELECT COUNT(*) FROM sslt")), [["3"]]);
    assert_eq!(
        row_text(session.run("SELECT a FROM sslt WHERE a IN (SELECT a FROM sslt) ORDER BY a")),
        [["1"], ["2"]]
    );

    // Zero is a cap like any other, and DEFAULT lifts it.
    session.apply_set("SET @@sql_select_limit = 0").unwrap();
    assert!(row_text(session.run("SELECT a FROM sslt ORDER BY a")).is_empty());
    session
        .apply_set("SET @@sql_select_limit = DEFAULT")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT a FROM sslt ORDER BY a")),
        [["1"], ["2"], ["3"]]
    );
}

/// Go `hint.go`'s `set_var` arm and `optimize.go`'s application of
/// `StmtHints.SetVars`: a `SET_VAR` hint overlays a session variable for ONE
/// statement, first occurrence wins, and the overlay is put back whether the
/// statement succeeded or failed.
///
/// Captured from TiDB (`testkit`, `pkg/executor`):
///
/// ```text
/// select @@sql_safe_updates                                        [[0]]
/// select /*+ SET_VAR(sql_safe_updates=1) */ @@sql_safe_updates      [[1]]
/// select @@sql_safe_updates                                        [[0]]
/// select /*+ SET_VAR(sql_safe_updates=1) SET_VAR(sql_safe_updates=0) */
///        @@sql_safe_updates                                        [[1]]
/// set sql_safe_updates=1
/// select /*+ SET_VAR(sql_safe_updates=0) */ @@sql_safe_updates      [[0]]
/// select @@sql_safe_updates                                        [[1]]
/// select /*+ SET_VAR(sql_safe_updates=0) */ no_such_column
///     ERR errno=1054 "[planner:1054]Unknown column 'no_such_column' in
///                     'field list'"
/// select @@sql_safe_updates                                        [[1]]
/// select /*+ SET_VAR(no_such_variable=1) */ @@sql_safe_updates      [[1]]
/// select /*+ SET_VAR(sql_safe_updates=99) */ @@sql_safe_updates     [[1]]
/// set sql_safe_updates=0
/// select /*+ SET_VAR(max_execution_time=123) */ @@max_execution_time [[123]]
/// select @@max_execution_time                                      [[0]]
/// select /*+ SET_VAR(sql_select_limit=1) */ 1 union all select 2    [[1]]
/// ```
#[test]
fn set_var_hint_overlays_one_statement() {
    let mut session = Session::new();
    assert_eq!(row_text(session.run("SELECT @@sql_safe_updates")), [["0"]]);
    assert_eq!(
        row_text(session.run("SELECT /*+ SET_VAR(sql_safe_updates=1) */ @@sql_safe_updates")),
        [["1"]]
    );
    // The next statement restores the preceding overlay.
    assert_eq!(row_text(session.run("SELECT @@sql_safe_updates")), [["0"]]);

    // Two hints for one name: the FIRST wins.
    assert_eq!(
        row_text(session.run(
            "SELECT /*+ SET_VAR(sql_safe_updates=1) SET_VAR(sql_safe_updates=0) */ \
             @@sql_safe_updates"
        )),
        [["1"]]
    );

    // The overlay is a write, not a floor: it can also turn a value OFF.
    session.run("SET sql_safe_updates = 1").unwrap();
    assert_eq!(
        row_text(session.run("SELECT /*+ SET_VAR(sql_safe_updates=0) */ @@sql_safe_updates")),
        [["0"]]
    );
    assert_eq!(row_text(session.run("SELECT @@sql_safe_updates")), [["1"]]);

    // A statement that FAILS restores it too. Go reports 1054 "Unknown column
    // 'no_such_column' in 'field list'" here; this tier still answers a
    // FROM-less unresolved column with 1105, a SEPARATE gap that predates this
    // overlay -- what is asserted is the failure and the restore.
    assert!(session
        .run("SELECT /*+ SET_VAR(sql_safe_updates=0) */ no_such_column")
        .is_err());
    assert_eq!(row_text(session.run("SELECT @@sql_safe_updates")), [["1"]]);

    // An unknown name and a value the registry rejects are both ignored
    // rather than failing the statement.
    assert_eq!(
        row_text(session.run("SELECT /*+ SET_VAR(no_such_variable=1) */ @@sql_safe_updates")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT /*+ SET_VAR(sql_safe_updates=99) */ @@sql_safe_updates")),
        [["1"]]
    );
    assert_eq!(row_text(session.run("SELECT @@sql_safe_updates")), [["1"]]);

    // Any hint-writable variable, not just the boolean: a numeric one
    // restores to its default rather than to the default's text.
    session.apply_set("SET sql_safe_updates = 0").unwrap();
    assert_eq!(
        row_text(session.run("SELECT /*+ SET_VAR(max_execution_time=123) */ @@max_execution_time")),
        [["123"]]
    );
    assert_eq!(
        row_text(session.run("SELECT @@max_execution_time")),
        [["0"]]
    );

    // A set operation's hints are its FIRST term's, and they reach the
    // statement-level `sql_select_limit` cap.
    assert_eq!(
        row_text(session.run("SELECT /*+ SET_VAR(sql_select_limit=1) */ 1 UNION ALL SELECT 2")),
        [["1"]]
    );
}

/// Prepared-statement parameters: the marker count a PREPARE reports and
/// the values an EXECUTE binds.
///
/// This is the session half of the binary protocol -- what a JDBC or Go
/// driver client needs to run anything at all. The wire half wires
/// `COM_STMT_PREPARE`/`EXECUTE` to it.
#[test]
fn prepared_statement_parameters() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT)")
        .unwrap();

    // The marker count is what PREPARE reports.
    assert_eq!(
        session
            .parameter_count("SELECT a FROM t WHERE a = ?")
            .unwrap(),
        1
    );
    assert_eq!(
        session
            .parameter_count("INSERT INTO t (a,b,c) VALUES (?,?,?)")
            .unwrap(),
        3
    );
    assert_eq!(session.parameter_count("SELECT 1").unwrap(), 0);
    assert_eq!(
        session
            .parameter_count("SELECT a FROM t WHERE b LIKE ? AND c BETWEEN ? AND ?")
            .unwrap(),
        3
    );

    // An INSERT binds its values positionally.
    session
        .run_with_params(
            "INSERT INTO t (a,b,c) VALUES (?,?,?)",
            &[Datum::Int(1), Datum::Bytes(b"one".to_vec()), Datum::Int(10)],
        )
        .unwrap();
    session
        .run_with_params(
            "INSERT INTO t (a,b,c) VALUES (?,?,?)",
            &[Datum::Int(2), Datum::Bytes(b"two".to_vec()), Datum::Int(20)],
        )
        .unwrap();

    // A SELECT binds in WHERE, and the markers keep their source order.
    let output = session
        .run_with_params("SELECT b FROM t WHERE a = ?", &[Datum::Int(2)])
        .unwrap();
    match output {
        StmtOutput::Rows { rows, .. } => {
            assert_eq!(datum_text(&rows[0][0]).unwrap(), "two");
        }
        other => panic!("expected rows, got {other:?}"),
    }
    let output = session
        .run_with_params(
            "SELECT a FROM t WHERE c BETWEEN ? AND ? ORDER BY a",
            &[Datum::Int(5), Datum::Int(15)],
        )
        .unwrap();
    match output {
        StmtOutput::Rows { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("expected rows, got {other:?}"),
    }

    // A value that is not UTF-8 does NOT fit a utf8mb4 column: captured, TiDB
    // answers 1366 "Incorrect string value '\xFF' for column 'b'" rather than
    // storing mangled bytes.
    crate::tests_support::assert_err(
        session.run_with_params(
            "INSERT INTO t (a,b,c) VALUES (?,?,?)",
            &[
                Datum::Int(3),
                Datum::Bytes(vec![0xff, 0xfe, b'z']),
                Datum::Int(30),
            ],
        ),
        1366,
        "Incorrect string value '\\xFF' for column 'b'",
    );

    // The same bytes DO survive the round trip through a binary column, which
    // has no character set to validate them against (captured).
    session
        .run("CREATE TABLE tv (a BIGINT PRIMARY KEY, b VARBINARY(20))")
        .unwrap();
    session
        .run_with_params(
            "INSERT INTO tv (a,b) VALUES (?,?)",
            &[Datum::Int(3), Datum::Bytes(vec![0xff, 0xfe, b'z'])],
        )
        .unwrap();
    match session
        .run_with_params("SELECT b FROM tv WHERE a = ?", &[Datum::Int(3)])
        .unwrap()
    {
        StmtOutput::Rows { rows, .. } => {
            let stored = match &rows[0][0] {
                Datum::Bytes(bytes) => bytes.clone(),
                Datum::String(text) => text.bytes().to_vec(),
                other => panic!("expected a string datum, got {other:?}"),
            };
            assert_eq!(stored, vec![0xff, 0xfe, b'z']);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // A NULL parameter binds as NULL, not as the text "NULL".
    session
        .run_with_params(
            "INSERT INTO t (a,b,c) VALUES (?,?,?)",
            &[Datum::Int(4), Datum::Null, Datum::Int(40)],
        )
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE b IS NULL")),
        [["4"]]
    );

    // Too few or too many values is Go's `plannererrors.ErrWrongParamCount`
    // (8112, `[planner:8112]Wrong parameter count`), captured from TiDB in
    // both directions: `EXECUTE p0 USING @a` for a marker-free statement and
    // `EXECUTE stmt` for one that carries a marker report the same code. The
    // check is `planCachePreprocess`'s step 1, which the binary protocol and
    // the SQL-level `EXECUTE` both reach.
    match session.run_with_params("SELECT a FROM t WHERE a = ?", &[]) {
        Ok(_) => panic!("an unbound marker should fail"),
        Err(error) => assert_eq!(error.to_mysql_error().code, 8112),
    }
    match session.run_with_params(
        "SELECT a FROM t WHERE a = ?",
        &[Datum::Int(1), Datum::Int(2)],
    ) {
        Ok(_) => panic!("an extra value should fail"),
        Err(error) => assert_eq!(error.to_mysql_error().code, 8112),
    }
}

/// Go's one warning sink stops appending at `math.MaxUint16`
/// (`StaticWarnHandler.appendWarningWithLevel`), because the count it
/// publishes is a `uint16`. The session buffer is that sink here, so the limit
/// belongs on its one door rather than at each of the seven callers.
#[test]
fn the_session_warning_buffer_stops_at_the_source_retention_limit() {
    let mut session = Session::new();
    for index in 0..u16::MAX as usize + 16 {
        session.append_warning(WarningLevel::Warning, 1292, format!("value {index}"));
    }
    assert_eq!(session.warnings().len(), u16::MAX as usize);
    // The FIRST entries survive: Go appends until the limit and then drops.
    assert_eq!(session.warnings()[0].message(), "value 0");
    assert_eq!(
        session.warnings()[u16::MAX as usize - 1].message(),
        "value 65534"
    );
}

/// A warning TiKV reported for a coprocessor request reaches BOTH channels a
/// client can learn about it from: the buffer `SHOW WARNINGS` reads and the
/// count the OK/EOF packet carries.
///
/// The two are proven independent here (`wire_warning_count` returns 0 while
/// `InShowWarning` is set, so it is not the buffer's length), and a fix
/// validated through only one of them is exactly how eleven of these stayed
/// invisible: `response_channel` appended them correctly into a collector
/// every production site built FRESH and then dropped.
///
/// DEFERRED LIVE CHECK: the audit's named case is `SELECT ROUND(s) FROM t`
/// with `s = '12abc'`, where TiDB reports the truncated value plus a 1292
/// warning. Producing the warning needs a real region acting on
/// `DAGRequest.flags = 482`; what is pinned here is that a warning which
/// arrives through the statement's coprocessor sink is reported.
#[test]
fn a_coprocessor_warning_reaches_both_the_show_warnings_buffer_and_the_wire_count() {
    let mut session = Session::new();
    let ctx = tidb_sessionctx::StmtContext::for_query();
    // What `tidb_distsql`'s `response_channel` does with
    // `SelectResponse.warnings`; the sink is the statement's own.
    ctx.cop_warning_sink()
        .append_tikv_warning(1292, "Truncated incorrect DOUBLE value: '12abc'");

    session.drain_eval_warnings(&ctx);

    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code(), 1292);
    assert_eq!(
        session.warnings()[0].message(),
        "Truncated incorrect DOUBLE value: '12abc'"
    );
    assert_eq!(
        session.wire_warning_count(),
        1,
        "the OK packet's count is a separate channel from the buffer"
    );
}

/// Whether a statement inherits the previous statement's warning buffer is a
/// decision Go makes on the PARSED node: `ResetContextOfStmt` switches on
/// `*ast.ShowStmt` and copies the outgoing context's entries forward only for
/// `ShowWarnings`, `ShowErrors`, and `ShowSessionStates`
/// (`pkg/executor/select.go`).
///
/// `WARNINGS` and `ERRORS` are UNRESERVED keywords, so `warnings` is a legal
/// table name. Deciding on the raw SQL text instead of the node therefore let
/// `SHOW CREATE TABLE warnings` inherit the buffer, and the `SHOW WARNINGS`
/// after it reported a statement two back.
#[test]
fn only_the_parsed_reporting_nodes_inherit_the_warning_buffer() {
    let mut session = Session::new();
    session.run("CREATE TABLE warnings (a INT)").unwrap();
    session
        .apply_set("SET tidb_enable_noop_functions = 'WARN'")
        .unwrap();

    let warn_once = |session: &mut Session| {
        session
            .run("SELECT a FROM warnings LOCK IN SHARE MODE")
            .unwrap();
        assert_eq!(session.warnings().len(), 1);
    };
    let reported = |session: &mut Session, sql: &str| match session.run_with_columns(sql).unwrap() {
        StmtOutput::Rows { rows, .. } => rows.len(),
        other => panic!("expected rows, got {other:?}"),
    };

    // The reporting nodes DO inherit it, and keep inheriting it: Go's
    // `SetWarnings` installs the entries into the fresh context, so the next
    // `SHOW WARNINGS` copies them forward again.
    warn_once(&mut session);
    assert_eq!(reported(&mut session, "SHOW WARNINGS"), 1);
    assert_eq!(reported(&mut session, "SHOW WARNINGS"), 1);

    // A SHOW that merely NAMES a table called `warnings` or `errors` does not.
    warn_once(&mut session);
    session.run("SHOW CREATE TABLE warnings").unwrap();
    assert_eq!(reported(&mut session, "SHOW WARNINGS"), 0);

    warn_once(&mut session);
    session.run("SHOW COLUMNS FROM warnings").unwrap();
    assert_eq!(reported(&mut session, "SHOW WARNINGS"), 0);

    session.run("CREATE TABLE errors (a INT)").unwrap();
    warn_once(&mut session);
    session.run("SHOW CREATE TABLE errors").unwrap();
    assert_eq!(reported(&mut session, "SHOW WARNINGS"), 0);

    // A statement that fails to parse clears the buffer, because a failed
    // parse never reaches the copy that would carry it forward. What is left
    // is the failure's OWN entry, which `run_with_columns` files as an
    // `Error` row -- not the preceding statement's warning.
    warn_once(&mut session);
    assert!(session.run("SELCT 1").is_err());
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].warning_level(), WarningLevel::Error);
    assert_ne!(session.warnings()[0].code(), 1235);
}

#[test]
fn session_migration_exports_and_restores_variables_and_prepared_statements() {
    let mut source = Session::new();
    source.run("CREATE DATABASE migration").unwrap();
    source.run("USE migration").unwrap();
    source.run("SET @answer = 41").unwrap();
    source.run("SET sql_mode = 'ANSI_QUOTES'").unwrap();
    source.run("PREPARE saved FROM 'SELECT ? + 1'").unwrap();
    let StmtOutput::Rows { rows, .. } = source.run_with_columns("SHOW SESSION_STATES").unwrap()
    else {
        panic!("migration must return state and token columns")
    };
    assert_eq!(rows[0].len(), 2);
    let state = crate::datum_text(&rows[0][0]).unwrap();
    let mut target = Session::with_catalog(source.catalog.clone());
    target
        .run(&format!(
            "SET SESSION_STATES '{}'",
            state.replace('\\', "\\\\").replace('\'', "''")
        ))
        .unwrap();
    assert_eq!(target.current_database(), "migration");
    assert_eq!(
        target.vars().get_system("sql_mode").unwrap(),
        source.vars().get_system("sql_mode").unwrap()
    );
    let StmtOutput::Rows { rows, .. } = target
        .run_with_columns("EXECUTE saved USING @answer")
        .unwrap()
    else {
        panic!("rows expected")
    };
    assert_eq!(rows[0][0], Datum::Int(42));
}

/// Go `EncodeSessionStates` / `DecodeSessionStates` carry
/// `SessionVars.HypoIndexes` and `HypoTiFlashReplicas`: a migrated session
/// keeps the hypothetical indexes it created.
#[test]
fn session_migration_carries_hypothetical_indexes() {
    let mut source = Session::new();
    source.run("CREATE DATABASE hypo_migration").unwrap();
    source.run("USE hypo_migration").unwrap();
    source.run("CREATE TABLE t (a INT, b INT, c INT)").unwrap();
    source.run("CREATE INDEX hypo_bc TYPE HYPO ON t (b, c)").unwrap();
    let StmtOutput::Rows { rows, .. } = source.run_with_columns("SHOW SESSION_STATES").unwrap()
    else {
        panic!("migration must return state and token columns")
    };
    let state = crate::datum_text(&rows[0][0]).unwrap();
    assert!(state.contains("hypo-indexes"), "{state}");
    let mut target = Session::with_catalog(source.catalog.clone());
    target
        .run(&format!(
            "SET SESSION_STATES '{}'",
            state.replace('\\', "\\\\").replace('\'', "''")
        ))
        .unwrap();
    let StmtOutput::Rows { rows, .. } =
        target.run_with_columns("SHOW CREATE TABLE t").unwrap()
    else {
        panic!("rows expected")
    };
    let create = crate::datum_text(&rows[0][1]).unwrap();
    assert!(
        create.contains("KEY `hypo_bc` (`b`,`c`) /* HYPO INDEX */"),
        "{create}"
    );
}

#[test]
fn session_migration_refuses_nonportable_session_resources() {
    for (setup, reason) in [
        ("BEGIN", "session has an active transaction"),
        (
            "CREATE TEMPORARY TABLE test.local_state (a INT)",
            "session has local temporary tables",
        ),
        (
            "SELECT GET_LOCK('migration_lock', 0)",
            "session has advisory locks",
        ),
    ] {
        let mut session = Session::new();
        session.run("CREATE DATABASE IF NOT EXISTS test").unwrap();
        session.run(setup).unwrap();
        let error = session.run("SHOW SESSION_STATES").unwrap_err();
        assert!(error.to_string().contains(reason), "{error}");
    }
}

/// Go `TestShowStateFail`'s "hold table locks" / "unlock the tables" cases:
/// a session holding `LOCK TABLES` cannot migrate, one that released them can.
#[test]
fn session_migration_refuses_held_table_locks() {
    use tidb_config::config_tree::config::{get_global_config, store_global_config, update_global};
    let previous = get_global_config();
    update_global(|config| config.enable_table_lock = true);
    let mut session = Session::new();
    session.run("CREATE DATABASE IF NOT EXISTS test").unwrap();
    session.run("CREATE TABLE test.t1 (id INT)").unwrap();
    session.run("LOCK TABLES test.t1 WRITE").unwrap();
    let error = session.run("SHOW SESSION_STATES").unwrap_err();
    session.run("UNLOCK TABLES").unwrap();
    let after_unlock = session.run("SHOW SESSION_STATES");
    store_global_config((*previous).clone());
    assert!(error.to_string().contains("session has locked tables"), "{error}");
    after_unlock.unwrap();
}

#[test]
fn session_migration_restores_dependency_order_rng_warnings_and_bindings() {
    let mut source = Session::new();
    source.run("USE test").unwrap();
    source
        .run("CREATE TABLE migration_binding (a INT PRIMARY KEY)")
        .unwrap();
    source.run("CREATE SESSION BINDING FOR SELECT * FROM migration_binding WHERE a = 1 USING SELECT /*+ USE_INDEX(migration_binding, PRIMARY) */ * FROM migration_binding WHERE a = 1").unwrap();
    source
        .run("SET rand_seed1 = 123, rand_seed2 = 456")
        .unwrap();
    source.run("SELECT RAND()").unwrap();
    source.append_warning(WarningLevel::Warning, 1292, "retained warning".into());
    let mut state = serde_json::to_value(source.encode_session_states().unwrap()).unwrap();
    state["sys-vars"]["tidb_allow_mpp"] = serde_json::json!("ON");
    state["sys-vars"]["tidb_enforce_mpp"] = serde_json::json!("ON");
    state["sys-vars"]["future_removed_variable"] = serde_json::json!("1");
    let mut target = Session::with_catalog(source.catalog.clone());
    target.run("SET tidb_allow_mpp=OFF").unwrap();
    target.decode_session_states(&state.to_string()).unwrap();
    assert_eq!(target.vars().get_system("tidb_enforce_mpp").unwrap(), "ON");
    assert_eq!(source.rand.get_seed1(), target.rand.get_seed1());
    assert_eq!(source.rand.get_seed2(), target.rand.get_seed2());
    assert_eq!(source.warnings, target.warnings);
    assert_eq!(
        source.run("SHOW SESSION BINDINGS").unwrap(),
        target.run("SHOW SESSION BINDINGS").unwrap()
    );
    assert_eq!(
        source.run("SELECT RAND()").unwrap(),
        target.run("SELECT RAND()").unwrap()
    );
}

/// Go `executeSetSessionStates` decodes what Go's `SHOW SESSION_STATES`
/// encoded (captured with gorun after `set @a = 1; set @b = 'x'; prepare s
/// from 'select ?'; select 1/0`, its system variables trimmed): user
/// variables with their types, the database, the counters and the warning.
#[test]
fn session_states_restore_what_go_encoded() {
    let go_states = r#"{"affected-rows": -1, "current-db": "test", "found-rows": 1, "prepared-stmt-id": 1, "rs-group": "default", "status": 2, "sys-vars": {"div_precision_increment": "7"}, "user-var-types": {"a": {"Array": false, "Charset": "binary", "Collate": "binary", "Decimal": 0, "Elems": null, "ElemsIsBinaryLit": null, "Flag": 129, "Flen": 1, "Tp": 8}, "b": {"Array": false, "Charset": "utf8mb4", "Collate": "utf8mb4_bin", "Decimal": -1, "Elems": null, "ElemsIsBinaryLit": null, "Flag": 1, "Flen": 1, "Tp": 253}}, "user-var-values": {"a": {"i": 1, "k": 1, "time": 0}, "b": {"b": "eA==", "collation": "utf8mb4_bin", "k": 5, "time": 0}}, "warnings": [{"err": {"class": 6, "code": 1365, "message": "Division by 0", "rfccode": "expression:1365"}, "level": "Warning"}]}"#;
    let mut target = Session::new();
    target.decode_session_states(go_states).unwrap();
    // The warning keeps Go's error: it encodes back as Go wrote it.
    let encoded = serde_json::to_value(target.encode_session_states().unwrap()).unwrap();
    assert_eq!(
        encoded["warnings"],
        serde_json::json!([{
            "err": {"class": 6, "code": 1365, "message": "Division by 0", "rfccode": "expression:1365"},
            "level": "Warning"
        }])
    );
    assert_eq!(
        target.warnings[0].parts(),
        (WarningLevel::Warning, 1365, "Division by 0".to_owned())
    );
    assert_eq!(target.current_database(), "test");
    assert_eq!(target.vars.prev_row_count, -1);
    assert_eq!(target.vars.last_found_rows, 1);
    assert_eq!(target.vars().get_system("div_precision_increment").unwrap(), "7");
    assert_eq!(
        crate::tests_support::row_text(target.run("SELECT @a, @b")),
        vec![vec!["1".to_owned(), "x".to_owned()]]
    );
}

/// Go's `SessionVars.LastQueryInfo` starts as the zero `QueryInfo`: the
/// first read in a session sees an empty scope (gorun).
#[test]
fn last_query_info_starts_as_gos_zero_value() {
    let mut session = Session::new();
    assert_eq!(
        crate::tests_support::row_text(session.run("SELECT @@tidb_last_query_info")),
        vec![vec![
            r#"{"txn_scope":"","start_ts":0,"for_update_ts":0,"ru_consumption":0}"#.to_owned()
        ]]
    );
}

#[test]
fn session_migration_preserves_prepare_database_and_cleans_up_after_restore_error() {
    let mut source = Session::new();
    source.run("CREATE DATABASE migration_prepare").unwrap();
    source.run("USE migration_prepare").unwrap();
    source.run("CREATE TABLE t(a INT)").unwrap();
    source.run("INSERT INTO t VALUES (7)").unwrap();
    source.run("PREPARE saved FROM 'SELECT a FROM t'").unwrap();
    source.run("USE test").unwrap();
    let state = serde_json::to_value(source.encode_session_states().unwrap()).unwrap();
    let mut target = Session::with_catalog(source.catalog.clone());
    target.decode_session_states(&state.to_string()).unwrap();
    assert_eq!(target.current_database(), "test");
    assert_eq!(
        source.run("EXECUTE saved").unwrap(),
        target.run("EXECUTE saved").unwrap()
    );
    let before_id = target.vars.prepared_statement_id;
    assert!(target.decode_session_states(r#"{"prepared-stmts":{"91":{"name":"bad","text":"SELECT missing_column","db":"migration_prepare"}}}"#).is_err());
    assert_eq!(target.current_database(), "test");
    assert_eq!(target.vars.prepared_statement_id, before_id);
}

#[test]
fn session_migration_retains_affected_rows_through_statement_completion() {
    let mut session = Session::new();
    session.run(r#"SET SESSION_STATES '{"affected-rows":37,"last-insert-id":18446744073709551615,"found-rows":12}'"#).unwrap();
    assert_eq!(
        session
            .run("SELECT ROW_COUNT(), LAST_INSERT_ID(), FOUND_ROWS()")
            .unwrap(),
        StmtResult::Rows(vec![vec![
            Datum::Int(37),
            Datum::UInt(u64::MAX),
            Datum::UInt(12)
        ]])
    );
}

#[test]
fn session_migration_restored_timestamp_uses_shared_historical_read_owner() {
    let mut source = Session::new();
    // Go tests seed the GC safe point a historical read validates against
    // (`safePointSQL`); a store with no GC worker has none.
    source
        .run("INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20060102-15:04:05.000 -0700', '')")
        .unwrap();
    source.run("USE test").unwrap();
    source.run("CREATE TABLE migrated_snapshot(a INT)").unwrap();
    source
        .run("INSERT INTO migrated_snapshot VALUES (7)")
        .unwrap();
    source.run("UPDATE migrated_snapshot SET a=8").unwrap();
    // Go validates the snapshot against the seeded safe point (oracle:
    // 8055 for a timestamp older than it).
    // Go prints the safe point with `time.Local` (`TSConvert2Time(..).String()`),
    // so the text names this machine's zone; the instant is what is fixed.
    let error = source.run("SET tidb_snapshot=100").unwrap_err().to_mysql_error();
    assert_eq!(error.code, 8055);
    let printed = error
        .message
        .strip_prefix("snapshot is older than GC safe point ")
        .unwrap_or_else(|| panic!("{}", error.message));
    let (stamp, _zone_name) = printed.rsplit_once(' ').expect("Go's time.String layout");
    let instant = chrono::DateTime::parse_from_str(stamp, "%Y-%m-%d %H:%M:%S %z")
        .unwrap_or_else(|parse| panic!("{printed}: {parse}"));
    assert_eq!(
        instant,
        chrono::DateTime::parse_from_rfc3339("2006-01-02T15:04:05-07:00").unwrap()
    );
}

#[test]
fn session_migration_rejects_malformed_state_before_restoring_handlers() {
    let mut session = Session::new();
    for invalid in [
        serde_json::json!(-1),
        serde_json::json!(4294967296u64),
        serde_json::json!("1"),
    ] {
        let state = serde_json::json!({"prepared-stmt-id": invalid, "prepared-stmts":{"5":{"name":"must_not_exist","text":"SELECT 1"}}});
        assert!(session.decode_session_states(&state.to_string()).is_err());
        assert!(session.prepared_statements.is_empty());
    }
}

#[test]
fn session_migration_batch_grant_accounts_print_raw_in_single_quotes() {
    // Go `showGrants` prints the account as `'%s'@'%s'` whatever the SQL
    // mode; only schema and table names follow it.
    let mut session = Session::new();
    session.attach_privileges(tidb_privilege::PrivilegeRegistry::default());
    session.run("CREATE USER 'quote`\"user'@'%'").unwrap();
    for sql_mode in ["''", "'ANSI_QUOTES'"] {
        session.run(&format!("SET sql_mode={sql_mode}")).unwrap();
        assert_eq!(
            session.run("SHOW GRANTS FOR 'quote`\"user'@'%'").unwrap(),
            StmtResult::Rows(vec![vec![Datum::new_string(
                "GRANT USAGE ON *.* TO 'quote`\"user'@'%'"
            )]])
        );
    }
}

#[test]
fn session_migration_batch_grant_executors_have_no_result_set() {
    let session = Session::new();
    for sql in [
        "GRANT SELECT ON *.* TO 'u'@'%'",
        "REVOKE SELECT ON *.* FROM 'u'@'%'",
        "GRANT r TO 'u'@'%'",
        "REVOKE r FROM 'u'@'%'",
        "GRANT PROXY ON 'u'@'%' TO 'v'@'%'",
    ] {
        assert_eq!(
            session.statement_kind_parsed(&session.parse(sql).unwrap()),
            StmtKind::Write,
            "{sql}"
        );
    }
}

/// Go's `StmtCtx` keeps a warning's error, so `SHOW SESSION_STATES` encodes
/// its class and RFC code (`SQLWarn.MarshalJSON`): the note `CREATE DATABASE
/// IF NOT EXISTS` files is `infoschema.ErrDatabaseExists` (gorun).
#[test]
fn session_states_carry_a_warnings_go_error() {
    let mut session = Session::new();
    session.run("create database if not exists test").unwrap();
    let states = serde_json::to_value(session.encode_session_states().unwrap()).unwrap();
    assert_eq!(
        states["warnings"],
        serde_json::json!([{
            "err": {
                "class": 14,
                "code": 1007,
                "message": "Can't create database 'test'; database exists",
                "rfccode": "schema:1007"
            },
            "level": "Note"
        }])
    );
}

/// Go `handleDivisionByZeroError` appends `expression.ErrDivisionByZero`
/// itself, so `select 1/0`'s warning encodes Go's class and RFC code
/// (captured with gorun).
#[test]
fn a_division_by_zero_warning_keeps_its_go_error() {
    let mut session = Session::new();
    session.run("select 1/0").unwrap();
    let states = serde_json::to_value(session.encode_session_states().unwrap()).unwrap();
    assert_eq!(
        states["warnings"],
        serde_json::json!([{
            "err": {"class": 6, "code": 1365, "message": "Division by 0", "rfccode": "expression:1365"},
            "level": "Warning"
        }])
    );
}

/// A pushed-down filter's warning comes back from TiKV as a code and a
/// message, which Go's `selectResult` appends as
/// `dbterror.ClassTiKV.Synthesize(code, msg)` (captured with gorun).
#[test]
fn a_coprocessor_warning_is_a_tikv_class_error_like_go() {
    let mut session = Session::new();
    session.run("create table tw (a varchar(10))").unwrap();
    session.run("insert into tw values ('x')").unwrap();
    session.run("select * from tw where a = 1").unwrap();
    let states = serde_json::to_value(session.encode_session_states().unwrap()).unwrap();
    assert_eq!(
        states["warnings"],
        serde_json::json!([{
            "err": {
                "class": 24,
                "code": 1292,
                "message": "Truncated incorrect DOUBLE value: 'x'",
                "rfccode": "tikv:1292"
            },
            "level": "Warning"
        }])
    );
}

/// The same code is two Go errors: a cast's truncation is
/// `types.ErrTruncatedWrongVal` and `WEIGHT_STRING`'s is the expression
/// package's `errTruncatedWrongValue`, which `SHOW SESSION_STATES` tells
/// apart (captured with gorun).
#[test]
fn a_truncation_warning_keeps_the_class_of_its_go_error() {
    for (sql, class, message, rfc_code) in [
        ("select cast('1x' as signed)", 20, "Truncated incorrect INTEGER value: '1x'", "types:1292"),
        (
            "select weight_string('abc' as binary(1))",
            6,
            "Truncated incorrect BINARY(1) value: 'abc'",
            "expression:1292",
        ),
    ] {
        let mut session = Session::new();
        session.run(sql).unwrap();
        let states = serde_json::to_value(session.encode_session_states().unwrap()).unwrap();
        assert_eq!(
            states["warnings"],
            serde_json::json!([{
                "err": {"class": class, "code": 1292, "message": message, "rfccode": rfc_code},
                "level": "Warning"
            }]),
            "{sql}"
        );
    }
}

/// Go `fetchShowSessionStates`: with a signing certificate configured, the
/// second column is a token for the logged-in user, which validates for that
/// user only; a session with no user gets none.
#[test]
fn show_session_states_signs_a_token_for_the_logged_in_user_like_go() {
    let dir = std::env::temp_dir().join(format!("session-token-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (cert_path, key_path) = (dir.join("cert.pem"), dir.join("key.pem"));
    let key = openssl::pkey::PKey::from_rsa(openssl::rsa::Rsa::generate(2048).unwrap()).unwrap();
    let mut builder = openssl::x509::X509Builder::new().unwrap();
    builder.set_pubkey(&key).unwrap();
    builder
        .set_not_before(&openssl::asn1::Asn1Time::days_from_now(0).unwrap())
        .unwrap();
    builder
        .set_not_after(&openssl::asn1::Asn1Time::days_from_now(90).unwrap())
        .unwrap();
    builder.sign(&key, openssl::hash::MessageDigest::sha256()).unwrap();
    std::fs::write(&cert_path, builder.build().to_pem().unwrap()).unwrap();
    std::fs::write(&key_path, key.private_key_to_pem_pkcs8().unwrap()).unwrap();
    tidb_sessionstates::set_cert_path(cert_path.to_str().unwrap());
    tidb_sessionstates::set_key_path(key_path.to_str().unwrap());
    tidb_sessionstates::reload_signing_cert();

    let privileges = tidb_privilege::PrivilegeRegistry::default();
    let mut session = authenticated_session(&privileges, "migrated", "%");
    let StmtOutput::Rows { rows, .. } = session.run_with_columns("SHOW SESSION_STATES").unwrap()
    else {
        panic!("SHOW SESSION_STATES returns rows")
    };
    let token = crate::datum_text(&rows[0][1]).expect("a token for the logged-in user");
    tidb_sessionstates::validate_session_token(token.as_bytes(), "migrated").unwrap();
    let error =
        tidb_sessionstates::validate_session_token(token.as_bytes(), "another_user").unwrap_err();
    assert!(error.contains("username does not match"), "{error}");

    let mut anonymous = Session::new();
    let StmtOutput::Rows { rows, .. } = anonymous.run_with_columns("SHOW SESSION_STATES").unwrap()
    else {
        panic!("SHOW SESSION_STATES returns rows")
    };
    assert_eq!(rows[0][1], Datum::Null);
    std::fs::remove_dir_all(&dir).unwrap();
}
