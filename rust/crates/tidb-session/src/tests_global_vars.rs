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
    first.attach_globals(globals.clone()).unwrap();

    let mut second = Session::with_catalog(catalog);
    second.set_user("root@%".to_owned(), "root@%".to_owned());
    second.attach_privileges(registry);
    second.attach_globals(globals.clone()).unwrap();

    (first, second, globals)
}

#[test]
fn ttl_job_enable_global_hook_updates_the_process_switch() {
    struct RestoreTtlJobEnable(bool);
    impl Drop for RestoreTtlJobEnable {
        fn drop(&mut self) {
            tidb_vardef::ENABLE_TTL_JOB.store(self.0, std::sync::atomic::Ordering::SeqCst);
        }
    }

    let _restore =
        RestoreTtlJobEnable(tidb_vardef::ENABLE_TTL_JOB.load(std::sync::atomic::Ordering::SeqCst));
    let globals = vars::GlobalSysvars::new();

    globals
        .set(
            tidb_vardef::tidb_vars::TIDB_TTL_JOB_ENABLE,
            "OFF".to_owned(),
        )
        .unwrap();
    assert!(!tidb_vardef::ENABLE_TTL_JOB.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(
        globals
            .get(tidb_vardef::tidb_vars::TIDB_TTL_JOB_ENABLE)
            .unwrap(),
        "OFF"
    );

    globals
        .set(tidb_vardef::tidb_vars::TIDB_TTL_JOB_ENABLE, "ON".to_owned())
        .unwrap();
    assert!(tidb_vardef::ENABLE_TTL_JOB.load(std::sync::atomic::Ordering::SeqCst));

    globals
        .reset(tidb_vardef::tidb_vars::TIDB_TTL_JOB_ENABLE)
        .unwrap();
    assert!(tidb_vardef::ENABLE_TTL_JOB.load(std::sync::atomic::Ordering::SeqCst));
}

// Transcreated from pinned Go `pkg/util/workloadrepo.TestSettingSQLVariables`.
#[test]
fn test_setting_sql_variables() {
    let (mut session, _, _) = two_sessions_sharing_globals();
    let worker = tidb_workloadrepo::Worker::new(None, None, None, "worker");
    session.set_workload_repository(std::sync::Arc::clone(&worker));

    for (statement, expected) in [
        (
            "SET GLOBAL tidb_workload_repository_active_sampling_interval = -1",
            "0",
        ),
        (
            "SET GLOBAL tidb_workload_repository_snapshot_interval = 899",
            "900",
        ),
        (
            "SET GLOBAL tidb_workload_repository_retention_days = -1",
            "0",
        ),
        (
            "SET GLOBAL tidb_workload_repository_active_sampling_interval = 601",
            "600",
        ),
        (
            "SET GLOBAL tidb_workload_repository_snapshot_interval = 7201",
            "7200",
        ),
        (
            "SET GLOBAL tidb_workload_repository_retention_days = 366",
            "365",
        ),
    ] {
        session.run(statement).unwrap();
        let name = statement
            .split_ascii_whitespace()
            .nth(2)
            .expect("SET GLOBAL variable name");
        assert_eq!(
            scalar_text(&mut session, &format!("SELECT @@global.{name}")),
            Some(expected.to_owned())
        );
    }

    for name in [
        "tidb_workload_repository_active_sampling_interval",
        "tidb_workload_repository_snapshot_interval",
        "tidb_workload_repository_retention_days",
    ] {
        assert!(session
            .run(&format!("SET GLOBAL {name} = 'invalid'"))
            .is_err());
    }

    session
        .run("SET GLOBAL tidb_workload_repository_dest = 'table'")
        .unwrap();
    assert!(worker.enabled());
    session
        .run("SET GLOBAL tidb_workload_repository_dest = ''")
        .unwrap();
    assert!(!worker.enabled());
    assert!(session
        .run("SET GLOBAL tidb_workload_repository_dest = 'invalid'")
        .is_err());
}

/// Transcreated from Go `TestRemovedOpt` and the executor's removed-variable
/// compatibility path: SET accepts removed names as parse-but-ignore shims,
/// while a SELECT read identifies the option and its replacement guidance.
#[test]
fn removed_system_variables_ignore_set_and_explain_reads() {
    assert!(sysvar::is_removed_sys_var("tidb_enable_alter_placement"));
    assert!(sysvar::is_removed_sys_var("TIDB_ENABLE_ALTER_PLACEMENT"));
    assert!(!sysvar::is_removed_sys_var(
        tidb_vardef::tidb_vars::TIDB_ENABLE1_PC
    ));

    let mut session = Session::new();
    session
        .run("SET tidb_enable_alter_placement = ON")
        .expect("removed SET is parse-but-ignore");
    session
        .run("SET GLOBAL TIDB_ENABLE_ALTER_PLACEMENT = OFF")
        .expect("removed SET GLOBAL is parse-but-ignore before privilege checks");

    let error = session
        .run("SELECT @@TIDB_ENABLE_ALTER_PLACEMENT")
        .expect_err("removed reads must not return a dummy value")
        .to_mysql_error();
    assert_eq!(error.code, 8136);
    assert_eq!(
        error.message,
        "option 'tidb_enable_alter_placement' is no longer supported. Reason: alter placement is now always enabled"
    );
}

/// Transcreated from Go `TestSetTIDBDistributeReorg`: the global distribution
/// switch accepts both boolean values through the shared global accessor.
#[test]
fn distribute_reorg_global_switch_round_trips() {
    let (mut session, _, _) = two_sessions_sharing_globals();
    session
        .run("SET GLOBAL tidb_enable_dist_task = OFF")
        .unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.tidb_enable_dist_task"),
        Some("0".to_owned())
    );
    session
        .run("SET GLOBAL tidb_enable_dist_task = ON")
        .unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.tidb_enable_dist_task"),
        Some("1".to_owned())
    );
}

/// Transcreated from Go `TestIndexMergeSwitcher`, `TestSetTIDBFastDDL`,
/// `TestSetTIDBDiskQuota`, `TestSetAggPushDownGlobally`, and
/// `TestSetDeriveTopNGlobally`: these GLOBAL registry entries expose their
/// Go defaults and retain the validated value in the shared accessor after a
/// write. Bool reads use the native `1`/`0` domain, while the byte quota stays
/// an unsigned decimal string.
#[test]
fn optimizer_and_ddl_global_switches_round_trip_like_go() {
    let (mut session, _, _) = two_sessions_sharing_globals();

    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.tidb_enable_index_merge"),
        Some("1".to_owned())
    );
    session
        .run("SET GLOBAL tidb_enable_index_merge = OFF")
        .unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.tidb_enable_index_merge"),
        Some("0".to_owned())
    );

    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.tidb_ddl_enable_fast_reorg"),
        Some("1".to_owned())
    );
    session
        .run("SET GLOBAL tidb_ddl_enable_fast_reorg = OFF")
        .unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.tidb_ddl_enable_fast_reorg"),
        Some("0".to_owned())
    );

    let gb = 1024_i64 * 1024 * 1024;
    let pb = gb * 1024 * 1024;
    let quota = |session: &mut Session| scalar_text(session, "SELECT @@global.tidb_ddl_disk_quota");
    assert_eq!(quota(&mut session), Some((100 * gb).to_string()));
    session
        .run(&format!("SET GLOBAL tidb_ddl_disk_quota = {}", 50 * gb))
        .unwrap();
    assert_eq!(quota(&mut session), Some((100 * gb).to_string()));
    session
        .run(&format!("SET GLOBAL tidb_ddl_disk_quota = {}", 200 * gb))
        .unwrap();
    assert_eq!(quota(&mut session), Some((200 * gb).to_string()));
    session
        .run(&format!("SET GLOBAL tidb_ddl_disk_quota = {}", 2 * pb))
        .unwrap();
    assert_eq!(quota(&mut session), Some(pb.to_string()));

    for (name, default) in [
        ("tidb_opt_agg_push_down", "0"),
        ("tidb_opt_derive_topn", "0"),
    ] {
        assert_eq!(
            scalar_text(&mut session, &format!("SELECT @@global.{name}")),
            Some(default.to_owned())
        );
        session.run(&format!("SET GLOBAL {name} = ON")).unwrap();
        assert_eq!(
            scalar_text(&mut session, &format!("SELECT @@global.{name}")),
            Some("1".to_owned())
        );
    }
}

/// Go's `tidb_opt_partial_ordered_index_for_topn` Validation accepts only
/// DISABLE/COST (case-insensitively), stores the uppercase mode, and refuses
/// enum ordinals or unknown text with ErrWrongValueForVar (1231).
#[test]
fn partial_ordered_index_for_topn_validation_matches_go() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();

    session
        .run("SET SESSION tidb_opt_partial_ordered_index_for_topn = 'cost'")
        .unwrap();
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@session.tidb_opt_partial_ordered_index_for_topn"
        ),
        Some("COST".to_owned())
    );

    let error = session
        .run("SET SESSION tidb_opt_partial_ordered_index_for_topn = 0")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1231);
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@session.tidb_opt_partial_ordered_index_for_topn"
        ),
        Some("COST".to_owned())
    );

    session
        .run("SET GLOBAL tidb_opt_partial_ordered_index_for_topn = 'disable'")
        .unwrap();
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_opt_partial_ordered_index_for_topn"
        ),
        Some("DISABLE".to_owned())
    );
}

/// Go's retired partition-statistics concurrency variable accepts assignments
/// for compatibility, warns for non-1 values, and always reads back `1` from
/// both SESSION and GLOBAL getters.
#[test]
fn merge_partition_stats_concurrency_is_fixed_at_one_like_go() {
    let (mut session, _peer, globals) = two_sessions_sharing_globals();

    session
        .run("SET SESSION tidb_merge_partition_stats_concurrency = 4")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        [[
            "Warning",
            "1287",
            "tidb_merge_partition_stats_concurrency is deprecated: the merge no longer runs concurrently, so this setting has no effect. Kept for backward compatibility."
        ]]
    );
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@session.tidb_merge_partition_stats_concurrency"
        ),
        Some("1".to_owned())
    );

    session
        .run("SET GLOBAL tidb_merge_partition_stats_concurrency = 8")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        [[
            "Warning",
            "1287",
            "tidb_merge_partition_stats_concurrency is deprecated: the merge no longer runs concurrently, so this setting has no effect. Kept for backward compatibility."
        ]]
    );
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_merge_partition_stats_concurrency"
        ),
        Some("1".to_owned())
    );

    // Startup/upgrade images may contain an old persisted value; Go's fixed
    // GetGlobal hook still masks it on read.
    globals.set_startup(
        "tidb_merge_partition_stats_concurrency",
        "99".to_owned(),
    );
    assert_eq!(
        globals.get("tidb_merge_partition_stats_concurrency").unwrap(),
        "1"
    );
}

/// Transcreated from Go `TestTiDBServerMemoryLimitSessMinSize` and
/// `TestTiDBServerMemoryLimitGCTrigger`: GLOBAL writes store the canonical
/// byte/fraction representation that subsequent `@@global` reads expose.
/// The process-wide memory tuner atomics are intentionally outside this SQL
/// registry's ownership and are covered as a receipt boundary.
#[test]
fn memory_limit_global_values_are_canonicalized_like_go() {
    let (mut session, _, _) = two_sessions_sharing_globals();

    let old_server_limit =
        tidb_util::memory::SERVER_MEMORY_LIMIT.load(std::sync::atomic::Ordering::SeqCst);
    session
        .run("SET GLOBAL tidb_server_memory_limit = '100MB'")
        .unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.tidb_server_memory_limit"),
        Some("512MB".to_owned())
    );
    session
        .run("SET GLOBAL tidb_server_memory_limit = '0'")
        .unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.tidb_server_memory_limit"),
        Some("0".to_owned())
    );
    session
        .run("SET GLOBAL tidb_server_memory_limit = '18446744073709551615'")
        .unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.tidb_server_memory_limit"),
        Some("18446744073709551615".to_owned())
    );
    for (input, expected) in [
        ("1234", "512MB"),
        ("1234567890123", "1234567890123"),
        ("10KB", "512MB"),
        ("12345678KB", "12345678KB"),
        ("10MB", "512MB"),
        ("700MB", "700MB"),
        ("20GB", "20GB"),
        ("2TB", "2TB"),
    ] {
        session
            .run(&format!("SET GLOBAL tidb_server_memory_limit = '{input}'"))
            .unwrap();
        assert_eq!(
            scalar_text(&mut session, "SELECT @@global.tidb_server_memory_limit"),
            Some(expected.to_owned()),
            "{input}"
        );
    }
    tidb_util::memory::SERVER_MEMORY_LIMIT
        .store(old_server_limit, std::sync::atomic::Ordering::SeqCst);

    session
        .run("SET GLOBAL tidb_server_memory_limit_sess_min_size = '123MB'")
        .unwrap();
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_server_memory_limit_sess_min_size"
        ),
        Some("128974848".to_owned())
    );
    session
        .run("SET GLOBAL tidb_server_memory_limit_sess_min_size = '100'")
        .unwrap();
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_server_memory_limit_sess_min_size"
        ),
        Some("128".to_owned())
    );

    session
        .run("SET GLOBAL tidb_server_memory_limit_gc_trigger = '90%'")
        .unwrap();
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_server_memory_limit_gc_trigger"
        ),
        Some("0.9".to_owned())
    );
    let error = session
        .run("SET GLOBAL tidb_server_memory_limit_gc_trigger = '100%'")
        .expect_err("Go rejects the percent parser's 100% boundary");
    assert_eq!(error.to_mysql_error().code, 1231);
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_server_memory_limit_gc_trigger"
        ),
        Some("0.9".to_owned())
    );
}

/// Transcreated from Go `TestDefaultPartitionPruneMode` and
/// `TestTiDBIgnoreInlistPlanDigest`: the registry defaults are visible through
/// the same session/global read paths used by the Go mock accessor.
#[test]
fn remaining_optimizer_defaults_match_go() {
    let (mut session, _, _) = two_sessions_sharing_globals();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@tidb_partition_prune_mode"),
        Some("dynamic".to_owned())
    );
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_ignore_inlist_plan_digest"
        ),
        Some("1".to_owned())
    );
    session
        .run("SET GLOBAL tidb_ignore_inlist_plan_digest = ON")
        .unwrap();
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_ignore_inlist_plan_digest"
        ),
        Some("1".to_owned())
    );
}

/// Transcreated from Go `TestTiDBTraceEventSysVar`: a valid JSON GLOBAL
/// assignment starts the process recorder with the requested categories and
/// sampling trigger, while an empty assignment closes it.
#[test]
fn trace_event_global_sysvar_controls_the_flight_recorder() {
    if let Some(recorder) = tidb_util::traceevent::get_flight_recorder() {
        recorder.close();
    }
    let (mut session, _, _) = two_sessions_sharing_globals();
    session
        .run(
            r#"SET GLOBAL tidb_trace_event = '{"enabled_categories":["*"],"dump_trigger":{"type":"sampling","sampling":1}}'"#,
        )
        .unwrap();
    let recorder = tidb_util::traceevent::get_flight_recorder().expect("recorder started");
    assert_eq!(
        recorder.config,
        tidb_util::traceevent::FlightRecorderConfig {
            enabled_categories: vec!["*".to_owned()],
            dump_trigger: tidb_util::traceevent::DumpTriggerConfig {
                kind: "sampling".to_owned(),
                sampling: 1,
                ..Default::default()
            },
        }
    );
    session.run("SET GLOBAL tidb_trace_event = ''").unwrap();
    assert!(tidb_util::traceevent::get_flight_recorder().is_none());
}

/// Transcreated from the real sysvar portion of Go `TestMockAPI`: the
/// default-authentication-plugin enum rejects unknown names and accepts a
/// supported plugin through the GLOBAL setter.
#[test]
fn default_authentication_plugin_global_validation_matches_go() {
    let (mut session, _, _) = two_sessions_sharing_globals();
    let error = session
        .run("SET GLOBAL default_authentication_plugin = 'invalidvalue'")
        .expect_err("unknown authentication plugins must be rejected");
    assert_eq!(error.to_mysql_error().code, 1231);
    session
        .run("SET GLOBAL default_authentication_plugin = 'mysql_native_password'")
        .unwrap();
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.default_authentication_plugin"
        ),
        Some("mysql_native_password".to_owned())
    );
}

#[test]
fn statement_context_reads_global_sysvars_through_the_live_accessor() {
    let globals = vars::GlobalSysvars::new();
    let mut session = Session::new();
    session.attach_globals(globals.clone()).unwrap();
    let context = session.statement_context(false);
    let read = || {
        tidb_executor::Columns::sysvar(
            &context,
            Some(tidb_ast::SysVarScope::Global),
            "validate_password.enable",
        )
    };

    assert_eq!(read(), Some(Datum::Bytes(b"OFF".to_vec())));
    globals
        .set("validate_password.enable", "ON".to_owned())
        .unwrap();
    assert_eq!(read(), Some(Datum::Bytes(b"ON".to_vec())));
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
    fresh.attach_globals(globals).unwrap();
    assert_eq!(
        fresh.run("SELECT @@autocommit").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(0)]])
    );
}

/// Go `TestTiDBEnableSharedLockUpgradeGate`: the new transaction switch is a
/// normal GLOBAL|SESSION boolean, defaults OFF, and keeps the session/global
/// copies independent after either tier is changed. Classic builds reject
/// enabling the switch; NextGen builds accept it, matching Go's kernel gate.
#[test]
fn shared_lock_upgrade_variable_has_go_scope_and_default() {
    let (mut first, mut second, globals) = two_sessions_sharing_globals();

    assert!(!first.vars().shared_lock_upgrade_enabled());
    assert!(!second.vars().shared_lock_upgrade_enabled());

    assert_eq!(
        first
            .run("SELECT @@tidb_enable_shared_lock_upgrade")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(0)]])
    );
    if tidb_config::kerneltype::is_next_gen() {
        first
            .run("SET tidb_enable_shared_lock_upgrade = ON")
            .unwrap();
        assert_eq!(
            first
                .run("SELECT @@tidb_enable_shared_lock_upgrade")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
        assert!(first.vars().shared_lock_upgrade_enabled());
        assert!(!second.vars().shared_lock_upgrade_enabled());
        assert_eq!(
            second
                .run("SELECT @@tidb_enable_shared_lock_upgrade")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(0)]])
        );
        assert!(!second.vars().shared_lock_upgrade_enabled());

        first
            .run("SET GLOBAL tidb_enable_shared_lock_upgrade = ON")
            .unwrap();
        assert_eq!(
            second
                .run("SELECT @@global.tidb_enable_shared_lock_upgrade")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
        // A connected session keeps its own copy until reconnect, matching
        // Go's NewSessionVars inheritance rule.
        assert_eq!(
            second
                .run("SELECT @@tidb_enable_shared_lock_upgrade")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(0)]])
        );

        let mut fresh = Session::new();
        fresh.attach_globals(globals).unwrap();
        assert!(fresh.vars().shared_lock_upgrade_enabled());
        assert_eq!(
            fresh
                .run("SELECT @@tidb_enable_shared_lock_upgrade")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(1)]])
        );
    } else {
        for value in ["ON", "1"] {
            let error = first
                .run(&format!("SET tidb_enable_shared_lock_upgrade = {value}"))
                .unwrap_err();
            assert_eq!(error.to_mysql_error().code, 1231);
            assert!(!first.vars().shared_lock_upgrade_enabled());
            assert_eq!(
                first
                    .run("SELECT @@tidb_enable_shared_lock_upgrade")
                    .unwrap(),
                StmtResult::Rows(vec![vec![Datum::Int(0)]])
            );
        }
        let error = first
            .run("SET GLOBAL tidb_enable_shared_lock_upgrade = ON")
            .unwrap_err();
        assert_eq!(error.to_mysql_error().code, 1231);
        assert_eq!(
            second
                .run("SELECT @@global.tidb_enable_shared_lock_upgrade")
                .unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(0)]])
        );
    }
}

/// Go's transport-sensitive validator runs only on SQL `SET GLOBAL`: a
/// plaintext session cannot enable the process gate and lock itself out, but
/// a TLS session can. Bool validation still precedes that transport check.
#[test]
fn require_secure_transport_can_only_be_enabled_by_a_secure_session() {
    let (mut plaintext, mut secure, globals) = two_sessions_sharing_globals();

    let invalid = plaintext
        .run("SET GLOBAL require_secure_transport = 2")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(invalid.code, 1231, "Bool validation wins: {invalid:?}");

    let refused = plaintext
        .run("SET GLOBAL require_secure_transport = ON")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(refused.code, 1105, "{refused:?}");
    assert_eq!(refused.state, *b"HY000");
    assert_eq!(
        refused.message,
        "require_secure_transport can only be set to ON if the connection issuing the change is secure"
    );
    assert_eq!(
        globals.get("require_secure_transport").as_deref(),
        Ok("OFF"),
        "a refused assignment cannot publish partial state"
    );

    secure.set_secure_transport(true);
    secure
        .run("SET GLOBAL require_secure_transport = TRUE")
        .unwrap();
    assert_eq!(globals.get("require_secure_transport").as_deref(), Ok("ON"));
}

/// `SHOW GLOBAL VARIABLES` reads the shared table live; `SHOW SESSION
/// VARIABLES` (and the unqualified default) reads the session's own copy --
/// so the two diverge after a session-only `SET` exactly as they do after a
/// `SET GLOBAL`.
#[test]
fn show_global_and_session_variables_diverge() {
    let mut session = Session::new();
    session.attach_privileges(privilege::PrivilegeRegistry::default());
    session.attach_globals(vars::GlobalSysvars::new()).unwrap();

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
    session.attach_globals(vars::GlobalSysvars::new()).unwrap();
    let error = session.run("SET GLOBAL debug_sync = 'x'").unwrap_err();
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1228, "{mysql:?}");
}

/// Go's `ErrGlobalVariable` (1229): `SET SESSION` (the unqualified form,
/// here) on a GLOBAL-only variable.
#[test]
fn set_session_on_a_global_only_variable_is_rejected() {
    let mut session = session_with_privileges();
    session.attach_globals(vars::GlobalSysvars::new()).unwrap();
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
    session.attach_globals(vars::GlobalSysvars::new()).unwrap();
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
    plain.attach_globals(vars::GlobalSysvars::new()).unwrap();
    let error = plain.run("SET GLOBAL autocommit = OFF").unwrap_err();
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1227, "{mysql:?}");

    root.run("GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'plain'@'%'")
        .unwrap();
    // Re-open the session so the newly granted dynamic privilege is what
    // this connection's identity resolves to (matches how every other
    // privilege check in this tier is exercised after a GRANT).
    let mut plain = session_as(&registry, root.shared_catalog(), "plain", "%");
    plain.attach_globals(vars::GlobalSysvars::new()).unwrap();
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

/// Pinned Go's validation closure warns on every assignment to the async
/// global-statistics merge switch, for both session and global scope.
#[test]
fn async_global_stats_switch_warns_on_every_assignment() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    let expected = vec![vec![
        "Warning".to_owned(),
        "1105".to_owned(),
        "The 'tidb_enable_async_merge_global_stats' variable will always be enabled in a future \
         release; changing it is discouraged."
            .to_owned(),
    ]];

    session
        .run("SET SESSION tidb_enable_async_merge_global_stats = OFF")
        .unwrap();
    assert_eq!(row_text(session.run("SHOW WARNINGS")), expected);

    session
        .run("SET GLOBAL tidb_enable_async_merge_global_stats = ON")
        .unwrap();
    assert_eq!(row_text(session.run("SHOW WARNINGS")), expected);

    session
        .run("SET SESSION tidb_enable_async_merge_global_stats = DEFAULT")
        .unwrap();
    assert_eq!(row_text(session.run("SHOW WARNINGS")), expected);

    session
        .run("SET GLOBAL tidb_enable_async_merge_global_stats = DEFAULT")
        .unwrap();
    assert_eq!(row_text(session.run("SHOW WARNINGS")), expected);
}

/// Go's statement-summary GLOBAL setters update the process-wide summary map
/// immediately; storing the SQL value alone is not sufficient for readers and
/// collectors that use the map directly.
#[test]
fn stmt_summary_global_hooks_update_runtime_map() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    let map = &tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP;
    let old_enabled = tidb_stmtsummary::v2::stmtsummary::enabled();
    let old_internal = tidb_stmtsummary::v2::stmtsummary::enabled_internal();
    let old_refresh = map.refresh_interval();
    let old_history = map.history_size();
    let old_max_stmt_count = map.max_stmt_count();
    let old_max_sql_length = map.max_sql_length();
    let old_group_by_user = map.group_by_user();

    session
        .run("SET GLOBAL tidb_enable_stmt_summary = OFF")
        .unwrap();
    session
        .run("SET GLOBAL tidb_stmt_summary_internal_query = ON")
        .unwrap();
    session
        .run("SET GLOBAL tidb_stmt_summary_refresh_interval = 77")
        .unwrap();
    session
        .run("SET GLOBAL tidb_stmt_summary_history_size = 9")
        .unwrap();
    session
        .run("SET GLOBAL tidb_stmt_summary_max_stmt_count = 41")
        .unwrap();
    session
        .run("SET GLOBAL tidb_stmt_summary_max_sql_length = 1234")
        .unwrap();
    session
        .run("SET GLOBAL tidb_stmt_summary_group_by_user = ON")
        .unwrap();

    assert!(!tidb_stmtsummary::v2::stmtsummary::enabled());
    assert!(tidb_stmtsummary::v2::stmtsummary::enabled_internal());
    assert_eq!(map.refresh_interval(), 77);
    assert_eq!(map.history_size(), 9);
    assert_eq!(map.max_stmt_count(), 41);
    assert_eq!(map.max_sql_length(), 1234);
    assert!(map.group_by_user());

    tidb_stmtsummary::v2::stmtsummary::set_enabled(old_enabled);
    tidb_stmtsummary::v2::stmtsummary::set_enable_internal_query(old_internal);
    tidb_stmtsummary::v2::stmtsummary::set_refresh_interval(old_refresh);
    tidb_stmtsummary::v2::stmtsummary::set_history_size(old_history as i32);
    tidb_stmtsummary::v2::stmtsummary::set_max_stmt_count(old_max_stmt_count as i64);
    tidb_stmtsummary::v2::stmtsummary::set_max_sql_length(old_max_sql_length as i32);
    tidb_stmtsummary::v2::stmtsummary::set_group_by_user(old_group_by_user);
}

/// Go `TestTiDBOptTxnAutoRetry`: OFF is a deprecated compatibility spelling
/// that warns and remains ON for both SESSION and GLOBAL assignments.
#[test]
fn disable_txn_auto_retry_off_warns_and_stays_on() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    let expected = vec![vec![
        "Warning".to_owned(),
        "1287".to_owned(),
        "'OFF' is deprecated and will be removed in a future release. Please use ON instead"
            .to_owned(),
    ]];

    session
        .run("SET SESSION tidb_disable_txn_auto_retry = OFF")
        .unwrap();
    assert_eq!(row_text(session.run("SHOW WARNINGS")), expected);
    assert_eq!(
        row_text(session.run("SHOW VARIABLES LIKE 'tidb_disable_txn_auto_retry'")),
        vec![vec![
            "tidb_disable_txn_auto_retry".to_owned(),
            "ON".to_owned()
        ]]
    );
    session
        .run("SET GLOBAL tidb_disable_txn_auto_retry = OFF")
        .unwrap();
    assert_eq!(row_text(session.run("SHOW WARNINGS")), expected);
    assert_eq!(
        row_text(session.run("SHOW GLOBAL VARIABLES LIKE 'tidb_disable_txn_auto_retry'")),
        vec![vec![
            "tidb_disable_txn_auto_retry".to_owned(),
            "ON".to_owned()
        ]]
    );
}

/// Go `TestDeprecation`: executor-concurrency compatibility variables accept
/// their value but append the replacement warning with MySQL code 1287.
#[test]
fn deprecated_index_lookup_concurrency_warns_like_go() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    session
        .run("SET SESSION tidb_index_lookup_concurrency = 123")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![vec![
            "Warning".to_owned(),
            "1287".to_owned(),
            "'tidb_index_lookup_concurrency' is deprecated and will be removed in a future release. Please use tidb_executor_concurrency instead".to_owned(),
        ]]
    );
}

/// Go `TestTiDBLowResTSOUpdateInterval`: GLOBAL integer bounds clamp to the
/// declared range and report the original value with warning 1292.
#[test]
fn low_resolution_tso_update_interval_clamps_and_warns() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();

    session
        .run("SET GLOBAL tidb_low_resolution_tso_update_interval = 0")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![vec![
            "Warning".to_owned(),
            "1292".to_owned(),
            "Truncated incorrect tidb_low_resolution_tso_update_interval value: '0'".to_owned()
        ]]
    );
    assert_eq!(
        row_text(
            session.run("SHOW GLOBAL VARIABLES LIKE 'tidb_low_resolution_tso_update_interval'")
        ),
        vec![vec![
            "tidb_low_resolution_tso_update_interval".to_owned(),
            "10".to_owned()
        ]]
    );

    session
        .run("SET GLOBAL tidb_low_resolution_tso_update_interval = 100000")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![vec![
            "Warning".to_owned(),
            "1292".to_owned(),
            "Truncated incorrect tidb_low_resolution_tso_update_interval value: '100000'"
                .to_owned()
        ]]
    );
    assert_eq!(
        row_text(
            session.run("SHOW GLOBAL VARIABLES LIKE 'tidb_low_resolution_tso_update_interval'")
        ),
        vec![vec![
            "tidb_low_resolution_tso_update_interval".to_owned(),
            "60000".to_owned()
        ]]
    );

    session
        .run("SET GLOBAL tidb_low_resolution_tso_update_interval = 1000")
        .unwrap();
    assert!(row_text(session.run("SHOW WARNINGS")).is_empty());
}

/// Go `TestTiDBSchemaCacheSize`: byte-size GLOBAL values preserve their
/// origin spelling while publishing the parsed byte count used by the cache.
#[test]
fn schema_cache_size_global_hook_publishes_bytes() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();

    session
        .run("SET GLOBAL tidb_schema_cache_size = '10KB'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GLOBAL VARIABLES LIKE 'tidb_schema_cache_size'")),
        vec![vec!["tidb_schema_cache_size".to_owned(), "64MB".to_owned()]]
    );
    assert_eq!(
        tidb_vardef::SCHEMA_CACHE_SIZE.load(std::sync::atomic::Ordering::SeqCst),
        64 << 20
    );

    session
        .run("SET GLOBAL tidb_schema_cache_size = '700MB'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GLOBAL VARIABLES LIKE 'tidb_schema_cache_size'")),
        vec![vec![
            "tidb_schema_cache_size".to_owned(),
            "700MB".to_owned()
        ]]
    );
    assert_eq!(
        tidb_vardef::SCHEMA_CACHE_SIZE.load(std::sync::atomic::Ordering::SeqCst),
        700 << 20
    );

    session
        .run("SET GLOBAL tidb_schema_cache_size = DEFAULT")
        .unwrap();
    assert_eq!(
        tidb_vardef::SCHEMA_CACHE_SIZE.load(std::sync::atomic::Ordering::SeqCst),
        tidb_vardef::defaults::DEF_TIDB_SCHEMA_CACHE_SIZE as u64
    );
}

/// Go `TestTiDBCircuitBreakerPDMetadataErrorRateThresholdRatio`: GLOBAL
/// writes clamp the ratio to [0, 1], report warning 1292 for out-of-range
/// inputs, and publish the validated float to the process-wide circuit
/// breaker state consumed by PD metadata requests.
#[test]
fn circuit_breaker_pd_metadata_ratio_global_hook_publishes_float() {
    struct RestoreRatio(f64);
    impl Drop for RestoreRatio {
        fn drop(&mut self) {
            tidb_vardef::set_circuit_breaker_pd_metadata_error_rate_threshold_ratio(self.0);
        }
    }

    let _restore =
        RestoreRatio(tidb_vardef::circuit_breaker_pd_metadata_error_rate_threshold_ratio());
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();

    session
        .run("SET GLOBAL tidb_cb_pd_metadata_error_rate_threshold_ratio = -1")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![vec![
            "Warning".to_owned(),
            "1292".to_owned(),
            "Truncated incorrect tidb_cb_pd_metadata_error_rate_threshold_ratio value: '-1'"
                .to_owned()
        ]]
    );
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_cb_pd_metadata_error_rate_threshold_ratio"
        ),
        Some("0".to_owned())
    );
    assert_eq!(
        tidb_vardef::circuit_breaker_pd_metadata_error_rate_threshold_ratio(),
        0.0
    );

    session
        .run("SET GLOBAL tidb_cb_pd_metadata_error_rate_threshold_ratio = 1.1")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![vec![
            "Warning".to_owned(),
            "1292".to_owned(),
            "Truncated incorrect tidb_cb_pd_metadata_error_rate_threshold_ratio value: '1.1'"
                .to_owned()
        ]]
    );
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_cb_pd_metadata_error_rate_threshold_ratio"
        ),
        Some("1".to_owned())
    );
    assert_eq!(
        tidb_vardef::circuit_breaker_pd_metadata_error_rate_threshold_ratio(),
        1.0
    );

    session
        .run("SET GLOBAL tidb_cb_pd_metadata_error_rate_threshold_ratio = 0.9")
        .unwrap();
    assert!(row_text(session.run("SHOW WARNINGS")).is_empty());
    assert_eq!(
        tidb_vardef::circuit_breaker_pd_metadata_error_rate_threshold_ratio(),
        0.9
    );
}

/// Go `TestEnableWindowFunction`: the session bool is initialized from the
/// default and updated by the SetSession hook for ON/0/1 spellings while the
/// normalized SQL value remains available to SHOW/@@ reads.
#[test]
fn enable_window_function_session_hook_updates_typed_state() {
    let mut session = Session::new();
    assert!(session.vars().window_function_enabled());

    session.run("SET tidb_enable_window_function = ON").unwrap();
    assert!(session.vars().window_function_enabled());

    session.run("SET tidb_enable_window_function = 0").unwrap();
    assert!(!session.vars().window_function_enabled());
    assert_eq!(
        scalar_text(&mut session, "SELECT @@tidb_enable_window_function"),
        Some("0".to_owned())
    );

    session.run("SET tidb_enable_window_function = 1").unwrap();
    assert!(session.vars().window_function_enabled());
}

/// Go `TestTiDBAutoAnalyzeConcurrencyValidation`: concurrency writes require
/// both process-wide auto-analyze switches, then publish the validated value
/// to the scheduler-facing atomic when the prerequisites are enabled.
#[test]
fn auto_analyze_concurrency_requires_enabled_scheduler() {
    struct RestoreAutoAnalyze {
        run: bool,
        priority_queue: bool,
        concurrency: i64,
    }
    impl Drop for RestoreAutoAnalyze {
        fn drop(&mut self) {
            tidb_vardef::RUN_AUTO_ANALYZE.store(self.run, std::sync::atomic::Ordering::SeqCst);
            tidb_vardef::ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE
                .store(self.priority_queue, std::sync::atomic::Ordering::SeqCst);
            tidb_vardef::AUTO_ANALYZE_CONCURRENCY
                .store(self.concurrency, std::sync::atomic::Ordering::SeqCst);
        }
    }

    let _restore = RestoreAutoAnalyze {
        run: tidb_vardef::RUN_AUTO_ANALYZE.load(std::sync::atomic::Ordering::SeqCst),
        priority_queue: tidb_vardef::ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE
            .load(std::sync::atomic::Ordering::SeqCst),
        concurrency: tidb_vardef::AUTO_ANALYZE_CONCURRENCY
            .load(std::sync::atomic::Ordering::SeqCst),
    };
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();

    session
        .run("SET GLOBAL tidb_enable_auto_analyze = OFF")
        .unwrap();
    assert!(!tidb_vardef::RUN_AUTO_ANALYZE.load(std::sync::atomic::Ordering::SeqCst));
    let error = session
        .run("SET GLOBAL tidb_auto_analyze_concurrency = 10")
        .expect_err("disabled auto-analyze must reject concurrency changes");
    assert!(error.to_mysql_error().message.contains(
        "requires both tidb_enable_auto_analyze and tidb_enable_auto_analyze_priority_queue"
    ));

    session
        .run("SET GLOBAL tidb_enable_auto_analyze = ON")
        .unwrap();
    tidb_vardef::RUN_AUTO_ANALYZE.store(true, std::sync::atomic::Ordering::SeqCst);
    tidb_vardef::ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let error = sysvar::get_sys_var(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_CONCURRENCY)
        .expect("auto-analyze concurrency is registered")
        .validate_in_scope("10", sysvar::SCOPE_GLOBAL)
        .expect_err("disabled priority queue must reject concurrency changes");
    assert!(
        matches!(error, sysvar::ValidationError::Refused(message) if message.contains("tidb_enable_auto_analyze_priority_queue=false"))
    );

    tidb_vardef::ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE
        .store(true, std::sync::atomic::Ordering::SeqCst);
    session
        .run("SET GLOBAL tidb_auto_analyze_concurrency = 10")
        .unwrap();
    assert_eq!(
        tidb_vardef::AUTO_ANALYZE_CONCURRENCY.load(std::sync::atomic::Ordering::SeqCst),
        10
    );
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_auto_analyze_concurrency"
        ),
        Some("10".to_owned())
    );
}

/// Go `TestTiDBEnableResourceControl` and
/// `TestTiDBResourceControlStrictMode`: the GLOBAL hooks publish the
/// process-wide switches consumed by resource-group hint admission, while
/// SQL reads retain the normalized ON/OFF values.
#[test]
fn resource_control_global_hooks_publish_process_switches() {
    struct RestoreResourceControl {
        enabled: bool,
        strict: bool,
    }
    impl Drop for RestoreResourceControl {
        fn drop(&mut self) {
            tidb_vardef::ENABLE_RESOURCE_CONTROL
                .store(self.enabled, std::sync::atomic::Ordering::SeqCst);
            tidb_vardef::ENABLE_RESOURCE_CONTROL_STRICT_MODE
                .store(self.strict, std::sync::atomic::Ordering::SeqCst);
        }
    }

    let _restore = RestoreResourceControl {
        enabled: tidb_vardef::ENABLE_RESOURCE_CONTROL.load(std::sync::atomic::Ordering::SeqCst),
        strict: tidb_vardef::ENABLE_RESOURCE_CONTROL_STRICT_MODE
            .load(std::sync::atomic::Ordering::SeqCst),
    };
    tidb_vardef::ENABLE_RESOURCE_CONTROL.store(false, std::sync::atomic::Ordering::SeqCst);
    tidb_vardef::ENABLE_RESOURCE_CONTROL_STRICT_MODE
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    assert!(session.vars().resource_control_enabled());
    assert!(!tidb_vardef::ENABLE_RESOURCE_CONTROL.load(std::sync::atomic::Ordering::SeqCst));
    assert!(session.vars().resource_control_strict_mode());

    session
        .run("SET GLOBAL tidb_enable_resource_control = ON")
        .unwrap();
    assert!(session.vars().resource_control_enabled());
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.tidb_enable_resource_control"),
        Some("1".to_owned())
    );

    session
        .run("SET GLOBAL tidb_resource_control_strict_mode = OFF")
        .unwrap();
    assert!(!session.vars().resource_control_strict_mode());
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_resource_control_strict_mode"
        ),
        Some("0".to_owned())
    );

    session
        .run("SET GLOBAL tidb_enable_resource_control = OFF")
        .unwrap();
    assert!(!session.vars().resource_control_enabled());
}

/// Go `TestTiDBAutoAnalyzeRatio`: values greater than one remain valid, while
/// tiny positive ratios are refused at 0.00001 and leave the prior GLOBAL
/// value unchanged.
#[test]
fn auto_analyze_ratio_validation_matches_go() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();

    session
        .run("SET GLOBAL tidb_auto_analyze_ratio = 1.1")
        .unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.tidb_auto_analyze_ratio"),
        Some("1.1".to_owned())
    );

    let error = session
        .run("SET GLOBAL tidb_auto_analyze_ratio = 0")
        .expect_err("zero ratio must be refused");
    assert_eq!(error.to_mysql_error().code, 1105);
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.tidb_auto_analyze_ratio"),
        Some("1.1".to_owned())
    );

    let error = session
        .run("SET GLOBAL tidb_auto_analyze_ratio = 0.0000000001")
        .expect_err("tiny ratio must be refused");
    assert_eq!(error.to_mysql_error().code, 1105);
    session
        .run("SET GLOBAL tidb_auto_analyze_ratio = 0.00001")
        .unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.tidb_auto_analyze_ratio"),
        Some("0.00001".to_owned())
    );
}

/// Go `TestTiDBAnalyzeStoreBatchSize`: the SetSession hook stores the
/// normalized unsigned value, including the zero disable sentinel and the
/// configured upper bound, and fresh sessions inherit GLOBAL state.
#[test]
fn analyze_store_batch_size_uses_go_typed_session_hook() {
    let (mut session, _peer, globals) = two_sessions_sharing_globals();

    assert_eq!(
        session.vars().analyze_store_batch_size(),
        tidb_vardef::defaults::DEF_TIDB_ANALYZE_STORE_BATCH_SIZE
    );
    session
        .run("SET tidb_analyze_store_batch_size = 0")
        .unwrap();
    assert_eq!(session.vars().analyze_store_batch_size(), 0);

    session
        .run("SET tidb_analyze_store_batch_size = 9")
        .unwrap();
    assert_eq!(session.vars().analyze_store_batch_size(), 8);

    session
        .run("SET GLOBAL tidb_analyze_store_batch_size = 6")
        .unwrap();
    let mut fresh = vars::SessionVars::new();
    fresh.seed_from_globals(globals).unwrap();
    assert_eq!(fresh.analyze_store_batch_size(), 6);
}

/// Go `TestTiDBOptSelectivityFactor`: the typed optimizer factor follows
/// SESSION writes, statement snapshots, and GLOBAL inheritance, while the
/// generic float validator clamps values above one to the source maximum.
#[test]
fn opt_selectivity_factor_uses_go_typed_session_hook() {
    let (mut session, _peer, globals) = two_sessions_sharing_globals();

    assert_eq!(
        session.vars().selectivity_factor(),
        tidb_vardef::defaults::DEF_OPT_SELECTIVITY_FACTOR
    );
    session
        .run("SET tidb_opt_selectivity_factor = 0.7")
        .unwrap();
    assert_eq!(session.vars().selectivity_factor(), 0.7);
    assert_eq!(session.ddl_statement_context().selectivity_factor(), 0.7);

    session
        .run("SET GLOBAL tidb_opt_selectivity_factor = 1.1")
        .unwrap();
    assert_eq!(globals.get("tidb_opt_selectivity_factor").unwrap(), "1");
    let mut fresh = vars::SessionVars::new();
    fresh.seed_from_globals(globals).unwrap();
    assert_eq!(fresh.selectivity_factor(), 1.0);
}

/// Go `TestTiDBAnalyzeDefaultBucketAndTopNOptions`: GLOBAL writes publish the
/// validated unsigned values and clamp out-of-range input to each configured
/// boundary instead of refusing the assignment.
#[test]
fn analyze_default_bucket_and_topn_global_hooks_match_go() {
    struct Restore {
        buckets: u64,
        top_n: u64,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS
                .store(self.buckets, std::sync::atomic::Ordering::SeqCst);
            tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N
                .store(self.top_n, std::sync::atomic::Ordering::SeqCst);
        }
    }

    let _restore = Restore {
        buckets: tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.load(std::sync::atomic::Ordering::SeqCst),
        top_n: tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.load(std::sync::atomic::Ordering::SeqCst),
    };
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();

    session
        .run("SET GLOBAL tidb_analyze_default_num_buckets = 100")
        .unwrap();
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_analyze_default_num_buckets"
        ),
        Some("100".to_owned())
    );
    assert_eq!(
        tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.load(std::sync::atomic::Ordering::SeqCst),
        100
    );
    session
        .run("SET GLOBAL tidb_analyze_default_num_buckets = 0")
        .unwrap();
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_analyze_default_num_buckets"
        ),
        Some("1".to_owned())
    );
    assert_eq!(
        tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    session
        .run("SET GLOBAL tidb_analyze_default_num_buckets = 100001")
        .unwrap();
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_analyze_default_num_buckets"
        ),
        Some("100000".to_owned())
    );
    assert_eq!(
        tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.load(std::sync::atomic::Ordering::SeqCst),
        100_000
    );

    session
        .run("SET GLOBAL tidb_analyze_default_num_topn = 50")
        .unwrap();
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_analyze_default_num_topn"
        ),
        Some("50".to_owned())
    );
    assert_eq!(
        tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.load(std::sync::atomic::Ordering::SeqCst),
        50
    );
    session
        .run("SET GLOBAL tidb_analyze_default_num_topn = 0")
        .unwrap();
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_analyze_default_num_topn"
        ),
        Some("0".to_owned())
    );
    session
        .run("SET GLOBAL tidb_analyze_default_num_topn = 100001")
        .unwrap();
    assert_eq!(
        scalar_text(
            &mut session,
            "SELECT @@global.tidb_analyze_default_num_topn"
        ),
        Some("100000".to_owned())
    );
    assert_eq!(
        tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.load(std::sync::atomic::Ordering::SeqCst),
        100_000
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

/// `tidb_service_scope` is a node-wide identifier. Go validates the original
/// spelling through `pkg/util/naming.Check`, stores its ASCII-lowercase form,
/// and leaves the previous value untouched when validation fails.
#[test]
fn service_scope_uses_the_shared_naming_contract() {
    let (mut first, mut second, _globals) = two_sessions_sharing_globals();

    first
        .run("SET GLOBAL tidb_service_scope = 'Scope_1-A'")
        .unwrap();
    assert_eq!(
        scalar_text(&mut first, "SELECT @@tidb_service_scope"),
        Some("scope_1-a".to_owned())
    );
    assert_eq!(
        scalar_text(&mut second, "SELECT @@global.tidb_service_scope"),
        Some("scope_1-a".to_owned())
    );

    let invalid = "bad scope";
    let error = first
        .run(&format!("SET GLOBAL tidb_service_scope = '{invalid}'"))
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1105, "{error:?}");
    assert_eq!(
        error.message,
        "the value 'bad scope' is invalid. It must be 64 characters or fewer and consist only of letters (a-z, A-Z), numbers (0-9), hyphens (-), and underscores (_)"
    );
    assert_eq!(
        scalar_text(&mut first, "SELECT @@tidb_service_scope"),
        Some("scope_1-a".to_owned()),
        "a rejected assignment must not mutate the node-wide value"
    );

    let too_long = "a".repeat(65);
    let error = first
        .run(&format!("SET GLOBAL tidb_service_scope = '{too_long}'"))
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1105, "{error:?}");
    assert!(error.message.contains("64 characters or fewer"));
}

/// `pkg/util/tikvutil.CommitterConcurrency` is the process authority behind
/// the GLOBAL variable. The stored row and the runtime client configuration
/// must therefore move together.
#[test]
fn committer_concurrency_updates_the_process_authority() {
    struct Restore(i32);

    impl Drop for Restore {
        fn drop(&mut self) {
            tidb_tikvutil::COMMITTER_CONCURRENCY.store(self.0, std::sync::atomic::Ordering::SeqCst);
        }
    }

    let _restore =
        Restore(tidb_tikvutil::COMMITTER_CONCURRENCY.load(std::sync::atomic::Ordering::SeqCst));
    let (mut first, mut second, globals) = two_sessions_sharing_globals();

    globals.load_from_cluster([(
        tidb_vardef::tidb_vars::TIDB_COMMITTER_CONCURRENCY.to_owned(),
        "256".to_owned(),
    )]);
    assert_eq!(
        tidb_tikvutil::COMMITTER_CONCURRENCY.load(std::sync::atomic::Ordering::SeqCst),
        256,
        "loading the live cluster table must initialize the process authority"
    );

    first
        .run("SET GLOBAL tidb_committer_concurrency = 1024")
        .unwrap();
    assert_eq!(
        scalar_text(&mut second, "SELECT @@global.tidb_committer_concurrency"),
        Some("1024".to_owned())
    );
    assert_eq!(
        tidb_tikvutil::COMMITTER_CONCURRENCY.load(std::sync::atomic::Ordering::SeqCst),
        1024,
        "the live atomic must follow the accepted GLOBAL assignment"
    );

    let scratch = vars::GlobalSysvars::from_cluster_rows([(
        tidb_vardef::tidb_vars::TIDB_COMMITTER_CONCURRENCY.to_owned(),
        "2048".to_owned(),
    )]);
    assert_eq!(
        tidb_tikvutil::COMMITTER_CONCURRENCY.load(std::sync::atomic::Ordering::SeqCst),
        1024,
        "loading transaction scratch state must not publish it"
    );
    scratch
        .set(
            tidb_vardef::tidb_vars::TIDB_COMMITTER_CONCURRENCY,
            "4096".to_owned(),
        )
        .unwrap();
    assert_eq!(
        tidb_tikvutil::COMMITTER_CONCURRENCY.load(std::sync::atomic::Ordering::SeqCst),
        1024,
        "validated but uncommitted scratch state must remain private"
    );

    globals.replace_from(&scratch);
    assert_eq!(
        tidb_tikvutil::COMMITTER_CONCURRENCY.load(std::sync::atomic::Ordering::SeqCst),
        4096,
        "committed cluster state must publish atomically with the live table"
    );
    globals
        .reset(tidb_vardef::tidb_vars::TIDB_COMMITTER_CONCURRENCY)
        .unwrap();
    assert_eq!(
        tidb_tikvutil::COMMITTER_CONCURRENCY.load(std::sync::atomic::Ordering::SeqCst),
        i32::try_from(tidb_vardef::defaults::DEF_TIDB_COMMITTER_CONCURRENCY)
            .expect("committer concurrency default fits i32")
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

/// Go `TestTxnIsolation`: a GLOBAL skip switch does not mutate the current
/// session, but a connection seeded after that GLOBAL write inherits it. The
/// inherited session accepts the unsupported level with the same 8048 warning
/// that Go's `checkIsolationLevel` appends after the relaxed validation path.
#[test]
fn global_isolation_skip_waits_for_the_next_session() {
    let (mut current, _peer, globals) = two_sessions_sharing_globals();

    let error = current
        .run("SET SESSION transaction_isolation = 'on'")
        .unwrap_err();
    assert_eq!(error.to_mysql_error().code, 1231);

    current
        .run("SET GLOBAL tidb_skip_isolation_level_check = ON")
        .unwrap();
    // The GLOBAL write is cluster state only; the writer's session copy stays
    // OFF until a new session explicitly changes it.
    let error = current
        .run("SET SESSION transaction_isolation = 'SERIALIZABLE'")
        .unwrap_err();
    assert_eq!(error.to_mysql_error().code, 8048);

    let mut inherited = Session::new();
    inherited.attach_globals(globals).unwrap();
    inherited
        .run("SET SESSION transaction_isolation = 'SERIALIZABLE'")
        .unwrap();
    assert_eq!(
        row_text(inherited.run("SHOW WARNINGS")),
        [[
            "Warning",
            "8048",
            "The isolation level 'SERIALIZABLE' is not supported. Set \
             tidb_skip_isolation_level_check=1 to skip this error"
        ]]
    );
    assert_eq!(
        scalar_text(&mut inherited, "SELECT @@transaction_isolation"),
        Some("SERIALIZABLE".to_owned())
    );
}

/// Go `TestReadOnlyNoop`: GLOBAL writes consult the GLOBAL copy of
/// `tidb_enable_noop_functions`, and all five `noop.go` variables refuse ON
/// with 1235 until that gate is enabled. A refused write leaves the global
/// value OFF; once enabled, each variable stores ON and can be reset.
#[test]
fn global_read_only_noop_variables_need_the_global_gate() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();

    for (name, clause) in [
        ("tx_read_only", "READ ONLY"),
        ("transaction_read_only", "READ ONLY"),
        ("offline_mode", "OFFLINE MODE"),
        ("super_read_only", "READ ONLY"),
        ("read_only", "READ ONLY"),
    ] {
        let error = session
            .run(&format!("SET GLOBAL {name} = ON"))
            .unwrap_err()
            .to_mysql_error();
        assert_eq!(error.code, 1235, "{name}");
        assert!(error.message.contains(clause), "{error:?}");
        assert_eq!(
            scalar_text(&mut session, &format!("SELECT @@global.{name}")),
            Some("0".to_owned()),
            "a refused global write must keep {name}=OFF"
        );

        session
            .run("SET GLOBAL tidb_enable_noop_functions = ON")
            .unwrap();
        session.run(&format!("SET GLOBAL {name} = ON")).unwrap();
        assert_eq!(
            scalar_text(&mut session, &format!("SELECT @@global.{name}")),
            Some("1".to_owned()),
            "the global gate must allow {name}=ON"
        );
        session.run(&format!("SET GLOBAL {name} = OFF")).unwrap();
        session
            .run("SET GLOBAL tidb_enable_noop_functions = OFF")
            .unwrap();
    }
}

/// Go's `tidb_enable_noop_functions` Validation rejects disabling the GLOBAL
/// gate while a same-scope no-op read-only variable is still ON.
#[test]
fn global_noop_gate_cannot_be_disabled_while_read_only_is_on() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    session
        .run("SET GLOBAL tidb_enable_noop_functions = ON")
        .unwrap();
    session.run("SET GLOBAL tx_read_only = ON").unwrap();

    let error = session
        .run("SET GLOBAL tidb_enable_noop_functions = OFF")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1235);
    assert_eq!(
        error.message,
        "tidb_enable_noop_functions = OFF is not supported when tx_read_only = ON"
    );
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.tidb_enable_noop_functions"),
        Some("ON".to_owned())
    );

    session.run("SET GLOBAL tx_read_only = OFF").unwrap();
    session
        .run("SET GLOBAL tidb_enable_noop_functions = OFF")
        .unwrap();
}

/// Go `SysVar.SkipInit` includes `IsNoop` variables: a fresh session keeps the
/// compatibility default even when the shared GLOBAL no-op row is ON.
#[test]
fn noop_globals_are_not_copied_into_new_sessions() {
    let (mut session, _peer, globals) = two_sessions_sharing_globals();
    session
        .run("SET GLOBAL tidb_enable_noop_functions = ON")
        .unwrap();
    session.run("SET GLOBAL tx_read_only = ON").unwrap();
    let mut fresh = SessionVars::new();
    fresh.seed_from_globals(globals).unwrap();
    assert_eq!(fresh.system_value("tx_read_only").unwrap(), "OFF");
    assert_eq!(
        fresh.system_value("tidb_enable_noop_functions").unwrap(),
        "ON"
    );
}

/// Go `TestSecureAuth`: the global compatibility switch cannot be disabled;
/// the rejected OFF write leaves the default ON intact, while ON remains a
/// valid global assignment.
#[test]
fn secure_auth_global_write_rejects_off() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();

    let error = session
        .run("SET GLOBAL secure_auth = OFF")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1231);
    assert_eq!(
        error.message,
        "Variable 'secure_auth' can't be set to the value of 'OFF'"
    );
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.secure_auth"),
        Some("1".to_owned())
    );

    session.run("SET GLOBAL secure_auth = ON").unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@global.secure_auth"),
        Some("1".to_owned())
    );
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

/// The read-only refusal is the LAST of `SysVar.Validate`'s three steps
/// (`validateScope` -> `ValidateFromType` -> `Validation`), so a value the
/// type check clamps reports BOTH the 1292 warning and the 1621 error, and a
/// value the type check REJECTS never reaches the refusal at all.
///
/// Captured from TiDB:
///
/// ```text
/// set @@Max_Allowed_Packet=100;
///   ERROR 1621 SESSION variable 'max_allowed_packet' is read-only. ...
///   Warning 1292 Truncated incorrect max_allowed_packet value: '100'
/// set @@max_allowed_packet=1000000000000;   -- same pair, MaxValue side
/// set @@max_allowed_packet='abc';
///   ERROR 1232 Incorrect argument type to variable 'max_allowed_packet'
/// ```
///
/// The `Warning`-level rows are read rather than the whole `SHOW WARNINGS`
/// output: this tier ALSO files the statement's own error as an `Error` row,
/// which real TiDB does not (measured -- after the same failed `SET`, TiDB's
/// `SHOW WARNINGS` returns the 1292 row alone). That gap is older and wider
/// than this seam, so it is named here rather than pinned into this case.
#[test]
fn a_refused_session_max_allowed_packet_still_reports_the_truncation() {
    let (mut session, _peer, _globals) = two_sessions_sharing_globals();
    let truncations = |session: &mut Session| -> Vec<Vec<String>> {
        row_text(session.run("SHOW WARNINGS"))
            .into_iter()
            .filter(|row| row.first().is_some_and(|level| level == "Warning"))
            .collect()
    };

    // Below MinValue (1024): clamped, warned, and then refused.
    let error = session.run("SET @@Max_Allowed_Packet = 100").unwrap_err();
    assert_eq!(error.to_mysql_error().code, 1621);
    assert_eq!(
        truncations(&mut session),
        [[
            "Warning",
            "1292",
            "Truncated incorrect max_allowed_packet value: '100'"
        ]]
    );

    // Above MaxValue: the same pair from the other side of the range.
    let error = session
        .run("SET @@max_allowed_packet = 1000000000000")
        .unwrap_err();
    assert_eq!(error.to_mysql_error().code, 1621);
    assert_eq!(
        truncations(&mut session),
        [[
            "Warning",
            "1292",
            "Truncated incorrect max_allowed_packet value: '1000000000000'"
        ]]
    );

    // A value the TYPE rejects stops at `ValidateFromType`: 1232, not 1621.
    // This is the control -- a fix that ran the refusal first, or that
    // swallowed the type error to reach the refusal, fails exactly here.
    let error = session.run("SET @@max_allowed_packet = 'abc'").unwrap_err();
    assert_eq!(error.to_mysql_error().code, 1232);

    // And an IN-RANGE value is still the bare 1621, with nothing to warn
    // about: the truncation must not be manufactured by the refusal path.
    let error = session.run("SET @@max_allowed_packet = 2048").unwrap_err();
    assert_eq!(error.to_mysql_error().code, 1621);
    assert!(truncations(&mut session).is_empty());
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
fn an_overflow_names_its_class_and_folded_constants_name_their_expression() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
    session
        .run("INSERT INTO t VALUES (9223372036854775807, 2)")
        .unwrap();

    for (sql, folded_expression) in [
        (
            "SELECT 9223372036854775807 + 1",
            "(9223372036854775807 + 1)",
        ),
        (
            "SELECT 9223372036854775807 * 2",
            "(9223372036854775807 * 2)",
        ),
    ] {
        let error = session.run(sql).unwrap_err().to_mysql_error();
        assert_eq!(error.code, 1690, "{sql}");
        assert_eq!(&error.state, b"22003", "{sql}");
        // Folded constants carry their expression exactly as Go does
        // (captured live from Go nightly on the `+` case).
        assert_eq!(
            error.message,
            format!("BIGINT value is out of range in '{folded_expression}'"),
            "{sql}"
        );
    }

    // Go renders the qualified expression for runtime column arithmetic
    // (`ErrOverflow.GenWithStackByArgs("BIGINT", "(a + b)")`), which the
    // runtime now mirrors.
    for (sql, expression) in [
        ("SELECT a + b FROM t", "(test.t.a + test.t.b)"),
        ("SELECT a + 1 FROM t", "(test.t.a + 1)"),
    ] {
        let error = session.run(sql).unwrap_err().to_mysql_error();
        assert_eq!(error.code, 1690, "{sql}");
        assert_eq!(&error.state, b"22003", "{sql}");
        assert_eq!(
            error.message,
            format!("BIGINT value is out of range in '{expression}'"),
            "{sql}"
        );
    }

    let error = session
        .run("SELECT 1e308 + 1e308")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1690);
    // Go spells the VALUE `1e+308` (not the statement's `1e308`) inside the
    // qualified expression, and the runtime mirrors that spelling.
    assert_eq!(
        error.message,
        "DOUBLE value is out of range in '(1e+308 + 1e+308)'"
    );
}

/// Go `EvalContext.GetMaxAllowedPacket`, which every result-sizing string
/// builtin reads (`builtinSpaceSig.maxAllowedPacket` and friends): the
/// SESSION copy of `max_allowed_packet`, not the live global.
///
/// The statement context never carried it, so every builtin sized its result
/// against the `Columns` trait default (`DefMaxAllowedPacket`, 64 MiB) no
/// matter what the server was configured with.
///
/// Captured from TiDB, in this order and in ONE session:
///
/// ```text
/// select space(2000) is null;              -> 0
/// set global max_allowed_packet = 1024;
/// select @@max_allowed_packet;             -> 67108864
/// select space(2000) is null;              -> 0
/// select length(repeat('ab', 2000));       -> 4000
/// ```
///
/// -- a `SET GLOBAL` does NOT reach the session that issued it, which is why
/// the session copy rather than the global table is the right read.
#[test]
fn a_result_sizing_builtin_reads_the_sessions_max_allowed_packet() {
    let mut session = Session::new();
    assert_eq!(
        scalar_text(&mut session, "SELECT space(2000) IS NULL").as_deref(),
        Some("0")
    );
    session.run("SET GLOBAL max_allowed_packet = 1024").unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@max_allowed_packet").as_deref(),
        Some("67108864")
    );
    assert_eq!(
        scalar_text(&mut session, "SELECT space(2000) IS NULL").as_deref(),
        Some("0"),
        "the SET GLOBAL must not reach the session that issued it"
    );
    assert_eq!(
        scalar_text(&mut session, "SELECT length(repeat('ab', 2000))").as_deref(),
        Some("4000")
    );
}

/// Go `SetExecutor.getVarValue`: `SET x = DEFAULT` resolves to
/// `GlobalSystemVariableInitialValue(sysVar.Name, sysVar.Value)` -- the
/// registry value with the FOR-NEW-INSTALLS-ONLY overrides applied -- and
/// writes that string, rather than clearing the override and answering the
/// raw registry value.
///
/// Captured against a v8.5.6 `tiup playground` (one PD, one TiKV, one Go
/// tidb-server), over the MySQL protocol:
///
/// ```text
/// mysql> SET tidb_row_format_version = DEFAULT; SELECT @@tidb_row_format_version;
/// 2
/// ```
///
/// while the sysvar registry (`sysvar/catalog/ddl_schema.rs`, and Go's own
/// `SysVar` struct) carries `1`. Three of the four overridden variables are
/// spelled out because they are the ones a fresh cluster actually runs with:
/// row format v2, `FAST` assertions, and fair locking on.
#[test]
fn set_default_resolves_the_new_install_initial_value() {
    let mut session = Session::new();

    session.run("SET tidb_row_format_version = 1").unwrap();
    session
        .run("SET tidb_row_format_version = DEFAULT")
        .unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@tidb_row_format_version").as_deref(),
        Some("2"),
        "the registry value is 1, but no install runs with it"
    );

    session.run("SET tidb_txn_assertion_level = OFF").unwrap();
    session
        .run("SET tidb_txn_assertion_level = DEFAULT")
        .unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@tidb_txn_assertion_level").as_deref(),
        Some("FAST")
    );

    session
        .run("SET tidb_pessimistic_txn_fair_locking = OFF")
        .unwrap();
    session
        .run("SET tidb_pessimistic_txn_fair_locking = DEFAULT")
        .unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@tidb_pessimistic_txn_fair_locking").as_deref(),
        Some("1")
    );

    // A variable with no override still answers its plain registry default,
    // which is the `return varVal` arm of the same Go function.
    session.run("SET autocommit = OFF").unwrap();
    session.run("SET autocommit = DEFAULT").unwrap();
    assert_eq!(
        scalar_text(&mut session, "SELECT @@autocommit").as_deref(),
        Some("1")
    );
}

/// Go's `validate_password.*` Validation closures (`sysvar.go:717-790`) keep
/// the five settings coupled: raising a count raises the sibling `length` to
/// `number + special + 2 * mixed_case`, and setting `length` below that
/// minimum adjusts it up instead of storing the too-small value.
#[test]
fn validate_password_count_sets_couple_the_length_sibling_like_go() {
    let globals = vars::GlobalSysvars::new();

    // Stock counts are number 1 / special 1 / mixed 1 and length 8: raising
    // mixed_case to 5 moves length to 1 + 1 + 2*5 = 12.
    globals
        .set("validate_password.mixed_case_count", "5".to_owned())
        .unwrap();
    assert_eq!(globals.get("validate_password.length").unwrap(), "12");

    // With mixed 5 the required minimum stays 12, so a too-small length is
    // adjusted up rather than stored.
    globals
        .set("validate_password.length", "2".to_owned())
        .unwrap();
    assert_eq!(globals.get("validate_password.length").unwrap(), "12");

    // A length above the requirement passes through untouched.
    globals
        .set("validate_password.length", "20".to_owned())
        .unwrap();
    assert_eq!(globals.get("validate_password.length").unwrap(), "20");

    // Dropping the number count to 0 lowers the required floor to
    // 0 + 1 + 2*5 = 11, which the current length 20 already exceeds, so the
    // length is untouched; the next length set enforces the new floor.
    globals
        .set("validate_password.number_count", "0".to_owned())
        .unwrap();
    assert_eq!(globals.get("validate_password.length").unwrap(), "20");
    globals
        .set("validate_password.length", "11".to_owned())
        .unwrap();
    assert_eq!(globals.get("validate_password.length").unwrap(), "11");
    globals
        .set("validate_password.length", "5".to_owned())
        .unwrap();
    assert_eq!(globals.get("validate_password.length").unwrap(), "11");
}

/// Go's `tidb_super_read_only` Validation (`sysvar.go:999`): turning the
/// flag OFF through a user SET is refused while `tidb_restricted_read_only`
/// is ON.
#[test]
fn super_read_only_cannot_be_turned_off_under_restricted_read_only() {
    let globals = vars::GlobalSysvars::new();
    globals
        .set("tidb_restricted_read_only", "ON".to_owned())
        .unwrap();
    assert_eq!(
        globals.get("tidb_super_read_only").unwrap(),
        "ON",
        "restricted read-only must promote super read-only"
    );

    let refused = globals.set("tidb_super_read_only", "OFF".to_owned());
    assert!(refused.is_err(), "the OFF set must be refused");

    // With the sibling off, the OFF set goes through.
    globals
        .set("tidb_restricted_read_only", "OFF".to_owned())
        .unwrap();
    globals
        .set("tidb_super_read_only", "OFF".to_owned())
        .unwrap();
    assert_eq!(globals.get("tidb_super_read_only").unwrap(), "OFF");
}
