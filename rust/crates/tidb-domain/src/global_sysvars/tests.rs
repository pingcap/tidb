// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `SessionVars` over the node's GLOBAL accessor, and the accessor's own
//! publication rules.

use super::*;
use tidb_variable::SessionVars;

/// A session as the node's factory opens one: its GLOBAL accessor installed
/// (Go `createSession`), its own copy not yet seeded.
fn session_vars() -> SessionVars {
    let mut vars = SessionVars::new();
    vars.swap_globals(Some(Arc::new(GlobalSysvars::new())));
    vars
}

#[cfg(test)]
mod session_vars {
    /// Serializes the tests that flip process-wide switches (Go `vardef`
    /// globals) within this test binary.
    fn process_switch_tests() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    use super::*;

    struct RestoreAnalyzeDefaults {
        buckets: u64,
        top_n: u64,
        stats_cache_mem_quota: i64,
    }

    impl Drop for RestoreAnalyzeDefaults {
        fn drop(&mut self) {
            tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS
                .store(self.buckets, std::sync::atomic::Ordering::SeqCst);
            tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N
                .store(self.top_n, std::sync::atomic::Ordering::SeqCst);
            tidb_vardef::STATS_CACHE_MEM_QUOTA.store(
                self.stats_cache_mem_quota,
                std::sync::atomic::Ordering::SeqCst,
            );
        }
    }

    #[test]
    fn tiflash_set_session_hooks_update_typed_state() {
        let mut vars = session_vars();
        assert_eq!(
            vars.ti_flash_max_bytes_before_ext_join(),
            tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_JOIN
        );
        assert_eq!(
            vars.ti_flash_mem_quota_query_per_node(),
            tidb_vardef::defaults::DEF_TIFLASH_MEM_QUOTA_QUERY_PER_NODE
        );
        assert_eq!(
            vars.ti_flash_query_spill_ratio(),
            tidb_vardef::defaults::DEF_TIFLASH_QUERY_SPILL_RATIO
        );

        let join_snapshot = vars
            .snapshot_system(tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN);
        vars.set_system(
            tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN,
            "10000".to_owned(),
        )
        .unwrap();
        vars.set_system(
            tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY,
            "20000".to_owned(),
        )
        .unwrap();
        vars.set_system(
            tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT,
            "30000".to_owned(),
        )
        .unwrap();
        vars.set_system(
            tidb_vardef::tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE,
            "40000".to_owned(),
        )
        .unwrap();
        vars.set_system(
            tidb_vardef::tidb_vars::TIFLASH_QUERY_SPILL_RATIO,
            "0.75".to_owned(),
        )
        .unwrap();

        assert_eq!(vars.ti_flash_max_bytes_before_ext_join(), 10_000);
        assert_eq!(vars.ti_flash_max_bytes_before_ext_agg(), 20_000);
        assert_eq!(vars.ti_flash_max_bytes_before_ext_sort(), 30_000);
        assert_eq!(vars.ti_flash_mem_quota_query_per_node(), 40_000);
        assert_eq!(vars.ti_flash_query_spill_ratio(), 0.75);

        vars.restore_system(join_snapshot);
        assert_eq!(
            vars.ti_flash_max_bytes_before_ext_join(),
            tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_JOIN
        );
    }

    #[test]
    fn tiflash_global_values_seed_typed_state_for_new_sessions() {
        let globals = GlobalSysvars::new();
        globals
            .set(
                tidb_vardef::tidb_vars::TIFLASH_QUERY_SPILL_RATIO,
                "0.85".to_owned(),
            )
            .unwrap();
        globals
            .set(
                tidb_vardef::tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE,
                "10000".to_owned(),
            )
            .unwrap();

        let mut vars = session_vars();
        vars.seed_from_globals(Arc::new(globals)).unwrap();
        assert_eq!(vars.ti_flash_query_spill_ratio(), 0.85);
        assert_eq!(vars.ti_flash_mem_quota_query_per_node(), 10_000);
        assert_eq!(
            vars.get_system(tidb_vardef::tidb_vars::TIFLASH_QUERY_SPILL_RATIO)
                .unwrap(),
            "0.85"
        );
    }

    #[test]
    fn pessimistic_fair_locking_uses_go_typed_session_hook() {
        let mut vars = session_vars();
        assert!(!vars.pessimistic_transaction_fair_locking_enabled());

        vars.set_system(
            tidb_vardef::tidb_vars::TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING,
            "OFF".to_owned(),
        )
        .unwrap();
        assert!(!vars.pessimistic_transaction_fair_locking_enabled());

        if tidb_config_kerneltype::is_next_gen() {
            let error = vars
                .set_system(
                    tidb_vardef::tidb_vars::TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING,
                    "ON".to_owned(),
                )
                .expect_err("nextgen rejects fair-locking ON");
            assert_eq!(error.code, 1235);
            assert!(!vars.pessimistic_transaction_fair_locking_enabled());
        } else {
            vars.set_system(
                tidb_vardef::tidb_vars::TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING,
                "ON".to_owned(),
            )
            .unwrap();
            assert!(vars.pessimistic_transaction_fair_locking_enabled());
        }
    }

    #[test]
    fn dml_type_and_replica_read_use_go_typed_hooks() {
        let mut vars = session_vars();
        assert!(!vars.bulk_dml_enabled());
        assert_eq!(vars.replica_read(), tidb_txnkv::ReplicaReadType::Leader);

        vars.set_system(tidb_vardef::tidb_vars::TIDB_DML_TYPE, "standard".to_owned())
            .unwrap();
        assert!(!vars.bulk_dml_enabled());

        if tidb_config_kerneltype::is_next_gen() {
            let error = vars
                .set_system(tidb_vardef::tidb_vars::TIDB_DML_TYPE, "bulk".to_owned())
                .expect_err("nextgen rejects bulk DML");
            assert_eq!(error.code, 1235);
            assert!(!vars.bulk_dml_enabled());
        } else {
            vars.set_system(tidb_vardef::tidb_vars::TIDB_DML_TYPE, "bulk".to_owned())
                .unwrap();
            assert!(vars.bulk_dml_enabled());
        }

        let modes = [
            ("follower", tidb_txnkv::ReplicaReadType::Follower),
            ("leader-and-follower", tidb_txnkv::ReplicaReadType::Mixed),
            ("closest-replicas", tidb_txnkv::ReplicaReadType::Closest),
            (
                "closest-adaptive",
                tidb_txnkv::ReplicaReadType::ClosestAdaptive,
            ),
            ("learner", tidb_txnkv::ReplicaReadType::Learner),
            (
                "prefer-leader",
                tidb_txnkv::ReplicaReadType::PreferLeader,
            ),
        ];
        for (name, expected) in modes {
            if tidb_config_kerneltype::is_next_gen() {
                let error = vars
                    .set_system(tidb_vardef::tidb_vars::TIDB_REPLICA_READ, name.to_owned())
                    .expect_err("nextgen rejects non-leader replica reads");
                assert_eq!(error.code, 1235);
                assert_eq!(vars.replica_read(), tidb_txnkv::ReplicaReadType::Leader);
            } else {
                vars.set_system(tidb_vardef::tidb_vars::TIDB_REPLICA_READ, name.to_owned())
                    .unwrap();
                assert_eq!(vars.replica_read(), expected);
            }
        }
    }

    #[test]
    fn global_analyze_defaults_update_the_vardef_backing_values() {
        // `ANALYZE_DEFAULT_NUM_*` are process-wide too; the session-tier test
        // that asserts on them runs apart from this one.
        let _serialised = process_switch_tests();
        let _restore = RestoreAnalyzeDefaults {
            buckets: tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS
                .load(std::sync::atomic::Ordering::SeqCst),
            top_n: tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.load(std::sync::atomic::Ordering::SeqCst),
            stats_cache_mem_quota: tidb_vardef::STATS_CACHE_MEM_QUOTA
                .load(std::sync::atomic::Ordering::SeqCst),
        };
        let globals = GlobalSysvars::new();

        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_ANALYZE_DEFAULT_NUM_BUCKETS,
                "4".to_owned(),
            )
            .unwrap();
        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_ANALYZE_DEFAULT_NUM_TOP_N,
                "5".to_owned(),
            )
            .unwrap();
        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_STATS_CACHE_MEM_QUOTA,
                "6".to_owned(),
            )
            .unwrap();
        assert_eq!(
            tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.load(std::sync::atomic::Ordering::SeqCst),
            4
        );
        assert_eq!(
            tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.load(std::sync::atomic::Ordering::SeqCst),
            5
        );
        assert_eq!(
            tidb_vardef::STATS_CACHE_MEM_QUOTA.load(std::sync::atomic::Ordering::SeqCst),
            6
        );

        globals
            .reset(tidb_vardef::tidb_vars::TIDB_ANALYZE_DEFAULT_NUM_BUCKETS)
            .unwrap();
        globals
            .reset(tidb_vardef::tidb_vars::TIDB_ANALYZE_DEFAULT_NUM_TOP_N)
            .unwrap();
        globals
            .reset(tidb_vardef::tidb_vars::TIDB_STATS_CACHE_MEM_QUOTA)
            .unwrap();
        assert_eq!(
            tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.load(std::sync::atomic::Ordering::SeqCst),
            tidb_vardef::defaults::DEF_TIDB_ANALYZE_DEFAULT_NUM_BUCKETS as u64
        );
        assert_eq!(
            tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.load(std::sync::atomic::Ordering::SeqCst),
            tidb_vardef::defaults::DEF_TIDB_ANALYZE_DEFAULT_NUM_TOP_N as u64
        );
        assert_eq!(
            tidb_vardef::STATS_CACHE_MEM_QUOTA.load(std::sync::atomic::Ordering::SeqCst),
            tidb_vardef::defaults::DEF_TIDB_STATS_CACHE_MEM_QUOTA
        );
    }

    #[test]
    fn empty_statement_restore_preserves_the_session_generation() {
        let mut vars = session_vars();
        let generation = vars.generation();

        vars.restore_system(Vec::new());

        assert_eq!(vars.generation(), generation);
    }

    /// Go `TestSessionStatesSystemVar`: only explicitly cached session values
    /// are serialized; timestamp's default and the insert-ID hooks are not.
    #[test]
    fn session_states_system_var_matches_go() {
        let mut vars = session_vars();

        vars.set_system("autocommit", "1".to_owned()).unwrap();
        assert_eq!(
            vars.get_session_states_system_var("autocommit").unwrap(),
            ("ON".to_owned(), true)
        );

        assert_eq!(
            vars.get_session_states_system_var("timestamp").unwrap(),
            (String::new(), false)
        );
        assert_eq!(
            vars.get_session_states_system_var("max_allowed_packet")
                .unwrap(),
            ("67108864".to_owned(), true)
        );
        vars.set_system("timestamp", "1.25".to_owned()).unwrap();
        assert_eq!(
            vars.get_session_states_system_var("timestamp").unwrap(),
            ("1.25".to_owned(), true)
        );
        vars.set_system("timestamp", "0".to_owned()).unwrap();
        assert_eq!(
            vars.get_session_states_system_var("timestamp").unwrap(),
            (String::new(), false)
        );

        vars.set_system("max_allowed_packet", "1024".to_owned())
            .unwrap();
        assert_eq!(
            vars.get_session_states_system_var("max_allowed_packet")
                .unwrap(),
            ("1024".to_owned(), true)
        );

        assert_eq!(
            vars.get_session_states_system_var("last_insert_id")
                .unwrap(),
            (String::new(), false)
        );
        assert!(vars
            .get_session_states_system_var("not_a_sysvar")
            .is_err_and(
                |error| error.equal(&tidb_error::catalog::variable::ERR_UNKNOWN_SYSTEM_VAR)
            ));
    }

    #[test]
    fn session_sql_mode_uses_go_typed_state() {
        let mut vars = session_vars();
        assert!(vars.sql_mode().has_strict_mode());
        assert!(vars.sql_mode().has_only_full_group_by());

        let restore = vars.snapshot_system("sql_mode");
        vars.set_system("sql_mode", "strict_trans_tabLES  ".to_owned())
            .unwrap();
        assert_eq!(vars.get_system("sql_mode").unwrap(), "STRICT_TRANS_TABLES");
        assert!(vars.sql_mode().has_strict_mode());
        let error = vars
            .set_system("sql_mode", "strict_trans_tabLES,nonsense_option".to_owned())
            .unwrap_err();
        // go's sql_mode Validation wraps the catalogued 1231 rendering in an
        // uncatalogued error, so the SET surfaces 1105 with the client text.
        assert_eq!(
            error.identity, None,
            "expected the uncatalogued validation refusal"
        );
        assert_eq!(
            error.message,
            "ERROR 1231 (42000): Variable 'sql_mode' can't be set to the value of 'NONSENSE_OPTION'"
        );
        assert!(vars.sql_mode().has_strict_mode());

        vars.set_system("sql_mode", "ANSI".to_owned()).unwrap();
        assert!(vars.sql_mode().has_ansi_quotes_mode());
        assert!(vars.sql_mode().has_pipes_as_concat_mode());
        assert!(!vars.sql_mode().has_strict_mode());
        vars.restore_system(restore);
        assert!(vars.sql_mode().has_strict_mode());
        assert!(!vars.sql_mode().has_ansi_quotes_mode());

        let globals = GlobalSysvars::new();
        globals
            .set("sql_mode", "NO_UNSIGNED_SUBTRACTION".to_owned())
            .unwrap();
        let mut inherited = session_vars();
        inherited.seed_from_globals(Arc::new(globals)).unwrap();
        assert!(inherited.sql_mode().has_no_unsigned_subtraction_mode());
        assert!(!inherited.sql_mode().has_strict_mode());
    }

    /// Go `TestTiDBMaxKeysRead` + `TestGetMaxKeysRead`: validation clips a
    /// negative value to zero, the session hook stores the positive value in
    /// typed state, and the accessor returns that state only for SELECTs.
    #[test]
    fn max_keys_read_uses_go_select_gate_and_typed_state() {
        let definition = get_sys_var("tidb_max_keys_read").unwrap();
        assert_eq!(definition.validate("-1").unwrap().value, "0");
        assert_eq!(definition.validate("0").unwrap().value, "0");
        assert_eq!(definition.validate("1000").unwrap().value, "1000");

        let mut vars = session_vars();
        assert_eq!(vars.max_keys_read(false), 0);
        assert_eq!(vars.max_keys_read(true), 0);

        vars.set_system("tidb_max_keys_read", "500".to_owned())
            .unwrap();
        assert_eq!(vars.max_keys_read(false), 0);
        assert_eq!(vars.max_keys_read(true), 500);

        let restore = vars.snapshot_system("tidb_max_keys_read");
        vars.set_system("tidb_max_keys_read", "100".to_owned())
            .unwrap();
        vars.restore_system(restore);
        assert_eq!(vars.max_keys_read(true), 500);

        let globals = GlobalSysvars::new();
        globals.set("tidb_max_keys_read", "100".to_owned()).unwrap();
        let mut inherited = session_vars();
        inherited.seed_from_globals(Arc::new(globals)).unwrap();
        assert_eq!(inherited.max_keys_read(true), 100);
        assert_eq!(inherited.max_keys_read(false), 0);
    }

    /// Go `TestMaxExecutionTime`: the unsigned value clamps a negative input,
    /// the session hook publishes the millisecond deadline into typed state,
    /// and statement-scoped restore/global inheritance keep that state in
    /// sync with the authoritative variable image.
    #[test]
    fn max_execution_time_uses_go_typed_state() {
        let definition = get_sys_var("max_execution_time").unwrap();
        assert_eq!(definition.validate("-10").unwrap().value, "0");
        assert_eq!(definition.validate("99999").unwrap().value, "99999");

        let mut vars = session_vars();
        assert_eq!(vars.max_execution_time(), 0);
        vars.set_system("max_execution_time", "99999".to_owned())
            .unwrap();
        assert_eq!(vars.max_execution_time(), 99999);

        let restore = vars.snapshot_system("max_execution_time");
        vars.set_system("max_execution_time", "100".to_owned())
            .unwrap();
        assert_eq!(vars.max_execution_time(), 100);
        vars.restore_system(restore);
        assert_eq!(vars.max_execution_time(), 99999);

        let globals = GlobalSysvars::new();
        globals.set("max_execution_time", "250".to_owned()).unwrap();
        let mut inherited = session_vars();
        inherited.seed_from_globals(Arc::new(globals)).unwrap();
        assert_eq!(inherited.max_execution_time(), 250);
    }

    /// Go `TestTiDBMultiStatementMode`: enum spellings normalize to the
    /// SessionVars integer mode, statement overlays can restore it, and a
    /// newly connected session inherits the GLOBAL value.
    #[test]
    fn multi_statement_mode_uses_go_typed_state() {
        let definition = get_sys_var("tidb_multi_statement_mode").unwrap();
        assert_eq!(definition.validate("on").unwrap().value, "ON");
        assert_eq!(definition.validate("0").unwrap().value, "OFF");
        assert_eq!(definition.validate("Warn").unwrap().value, "WARN");

        let mut vars = session_vars();
        assert_eq!(vars.multi_statement_mode(), 0);
        vars.set_system("tidb_multi_statement_mode", "ON".to_owned())
            .unwrap();
        assert_eq!(vars.multi_statement_mode(), 1);

        let restore = vars.snapshot_system("tidb_multi_statement_mode");
        vars.set_system("tidb_multi_statement_mode", "WARN".to_owned())
            .unwrap();
        assert_eq!(vars.multi_statement_mode(), 2);
        vars.restore_system(restore);
        assert_eq!(vars.multi_statement_mode(), 1);

        let globals = GlobalSysvars::new();
        globals
            .set("tidb_multi_statement_mode", "WARN".to_owned())
            .unwrap();
        let mut inherited = session_vars();
        inherited.seed_from_globals(Arc::new(globals)).unwrap();
        assert_eq!(inherited.multi_statement_mode(), 2);
    }

    /// Go `TestSQLSelectLimit`: the unsigned limit clips negatives to zero,
    /// stores the normalized value in `SessionVars.SelectLimit`, and restores
    /// the unlimited MaxUint64 default through the ordinary session image.
    #[test]
    fn sql_select_limit_uses_go_typed_state() {
        let definition = get_sys_var("sql_select_limit").unwrap();
        assert_eq!(definition.validate("-10").unwrap().value, "0");
        assert_eq!(definition.validate("9999").unwrap().value, "9999");

        let mut vars = session_vars();
        assert_eq!(vars.select_limit(), u64::MAX);
        vars.set_system("sql_select_limit", "9999".to_owned())
            .unwrap();
        assert_eq!(vars.select_limit(), 9999);

        let restore = vars.snapshot_system("sql_select_limit");
        vars.set_system("sql_select_limit", "0".to_owned()).unwrap();
        assert_eq!(vars.select_limit(), 0);
        vars.restore_system(restore);
        assert_eq!(vars.select_limit(), 9999);

        let globals = GlobalSysvars::new();
        globals.set("sql_select_limit", "2".to_owned()).unwrap();
        let mut inherited = session_vars();
        inherited.seed_from_globals(Arc::new(globals)).unwrap();
        assert_eq!(inherited.select_limit(), 2);
    }

    /// Go `TestTimeZone`: validated names and fixed offsets are resolved by
    /// the session hook, retained for statement contexts, and restored or
    /// inherited with the session image.
    #[test]
    fn time_zone_uses_go_typed_state() {
        let definition = get_sys_var("time_zone").unwrap();
        for value in ["America/Edmonton", "+10:00", "UTC", "+00:00"] {
            assert_eq!(definition.validate(value).unwrap().value, value);
        }

        let mut vars = session_vars();
        vars.set_system("time_zone", "+10:00".to_owned()).unwrap();
        assert_eq!(
            vars.session_time_zone(),
            tidb_datatype::SessionTimeZone::Fixed {
                name: "+10:00".to_owned(),
                offset_secs: 10 * 60 * 60,
            }
        );

        let restore = vars.snapshot_system("time_zone");
        vars.set_system("time_zone", "UTC".to_owned()).unwrap();
        assert_eq!(
            vars.session_time_zone(),
            tidb_datatype::SessionTimeZone::Named(chrono_tz::Tz::UTC)
        );
        vars.restore_system(restore);
        assert_eq!(
            vars.session_time_zone(),
            tidb_datatype::SessionTimeZone::Fixed {
                name: "+10:00".to_owned(),
                offset_secs: 10 * 60 * 60,
            }
        );

        let globals = GlobalSysvars::new();
        globals.set("time_zone", "UTC".to_owned()).unwrap();
        let mut inherited = session_vars();
        inherited.seed_from_globals(Arc::new(globals)).unwrap();
        assert_eq!(
            inherited.session_time_zone(),
            tidb_datatype::SessionTimeZone::Named(chrono_tz::Tz::UTC)
        );
    }

    /// Transcreated from Go `TestSetJobScheduleWindow`: a short TTL schedule
    /// time is interpreted in the issuing session's location, while an
    /// already-expanded value keeps its explicit numeric offset.
    #[test]
    fn ttl_schedule_window_global_write_uses_session_time_zone() {
        let mut vars = session_vars();
        vars.set_system("time_zone", "UTC".to_owned()).unwrap();
        vars.set_global(
            "tidb_ttl_job_schedule_window_start_time",
            "16:11".to_owned(),
        )
        .unwrap();
        assert_eq!(
            vars.get_global("tidb_ttl_job_schedule_window_start_time")
                .unwrap(),
            "16:11 +0000"
        );

        vars.set_system("time_zone", "Asia/Shanghai".to_owned())
            .unwrap();
        vars.set_global(
            "tidb_ttl_job_schedule_window_start_time",
            "16:11".to_owned(),
        )
        .unwrap();
        assert_eq!(
            vars.get_global("tidb_ttl_job_schedule_window_start_time")
                .unwrap(),
            "16:11 +0800"
        );
        vars.set_global(
            "tidb_ttl_job_schedule_window_start_time",
            "16:11 +0000".to_owned(),
        )
        .unwrap();
        assert_eq!(
            vars.get_global("tidb_ttl_job_schedule_window_start_time")
                .unwrap(),
            "16:11 +0000"
        );
    }

    /// All Go `TypeTime` globals, not only TTL's schedule-window pair, use the
    /// issuing session's location before their GLOBAL hook stores the value.
    #[test]
    fn generic_time_and_duration_globals_use_go_normalization() {
        let mut vars = session_vars();
        vars.set_system("time_zone", "Asia/Shanghai".to_owned())
            .unwrap();
        vars.set_global("tidb_auto_analyze_start_time", "3:00".to_owned())
            .unwrap();
        assert_eq!(
            vars.get_global("tidb_auto_analyze_start_time").unwrap(),
            "03:00 +0800"
        );
        let error = vars
            .set_global("tidb_auto_analyze_start_time", "25:00".to_owned())
            .unwrap_err();
        // Go returns `time.Parse`'s range error verbatim.
        assert_eq!(
            error,
            tidb_error::MysqlError::unknown(
                r#"parsing time "25:00": hour out of range"#.to_owned()
            )
        );

        let truncated = vars
            .set_global("tidb_gc_run_interval", "1ms".to_owned())
            .unwrap();
        assert!(truncated);
        assert_eq!(vars.get_global("tidb_gc_run_interval").unwrap(), "10m0s");
    }

    #[test]
    fn shared_lock_promotion_switch_uses_go_typed_state() {
        let mut vars = session_vars();
        assert!(!vars.shared_lock_promotion_enabled());
        let restore =
            vars.snapshot_system(tidb_vardef::tidb_vars::TIDB_ENABLE_SHARED_LOCK_PROMOTION);
        vars.set_system(
            tidb_vardef::tidb_vars::TIDB_ENABLE_SHARED_LOCK_PROMOTION,
            "ON".to_owned(),
        )
        .unwrap();
        assert!(vars.shared_lock_promotion_enabled());
        vars.restore_system(restore);
        assert!(!vars.shared_lock_promotion_enabled());

        let globals = GlobalSysvars::new();
        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_ENABLE_SHARED_LOCK_PROMOTION,
                "ON".to_owned(),
            )
            .unwrap();
        vars.seed_from_globals(Arc::new(globals)).unwrap();
        assert!(vars.shared_lock_promotion_enabled());
    }

    #[test]
    fn defaults_come_from_the_registry() {
        let vars = session_vars();
        assert_eq!(vars.get_system("autocommit").unwrap(), "ON");
        assert_eq!(vars.get_system("character_set_client").unwrap(), "utf8mb4");
        assert_eq!(
            vars.get_system("transaction_isolation").unwrap(),
            "REPEATABLE-READ"
        );
        assert_eq!(vars.get_system("max_allowed_packet").unwrap(), "67108864");
        // The lookup is case-insensitive, as Go's is after lowercasing.
        assert_eq!(vars.get_system("AUTOCOMMIT").unwrap(), "ON");
    }

    #[test]
    fn an_unknown_system_variable_is_rejected_both_ways() {
        let mut vars = session_vars();
        assert_eq!(
            vars.get_system("nope"),
            Err(tidb_error::MysqlError::from_terror(
                &tidb_error::catalog::variable::ERR_UNKNOWN_SYSTEM_VAR
                    .generate_with_stack_by_args(&[("nope".to_owned()).into()])
            ))
        );
        assert_eq!(
            vars.set_system("nope", "1".to_owned()),
            Err(tidb_error::MysqlError::from_terror(
                &tidb_error::catalog::variable::ERR_UNKNOWN_SYSTEM_VAR
                    .generate_with_stack_by_args(&[("nope".to_owned()).into()])
            ))
        );
    }

    #[test]
    fn a_loaded_row_overrides_and_an_absent_one_still_falls_back_to_default() {
        // Loading or replacing a whole GLOBAL table republishes the process
        // switches; the tests that assert on them run apart from this one.
        let _serialised = process_switch_tests();
        let globals = GlobalSysvars::new();
        globals.load_from_cluster([("AUTOCOMMIT".to_owned(), "OFF".to_owned())]);
        // The seeded row overrides, case-insensitively, exactly like `set`.
        assert_eq!(globals.get("autocommit").unwrap(), "OFF");
        // A variable the fixture never mentioned still falls back to the
        // registry default -- `load_from_cluster` only overwrites names it is
        // given.
        assert_eq!(globals.get("max_allowed_packet").unwrap(), "67108864");
    }

    #[test]
    fn a_loaded_row_naming_an_unknown_variable_is_skipped_not_refused() {
        // Loading or replacing a whole GLOBAL table republishes the process
        // switches; the tests that assert on them run apart from this one.
        let _serialised = process_switch_tests();
        let globals = GlobalSysvars::new();
        // Bypasses `set`'s validation on purpose (a stored row already passed
        // it once), but an unrecognized name from an older/newer cluster must
        // still not panic or poison every other loaded row.
        globals.load_from_cluster([
            ("not_a_real_variable".to_owned(), "x".to_owned()),
            ("autocommit".to_owned(), "OFF".to_owned()),
        ]);
        assert_eq!(
            globals.get("not_a_real_variable"),
            Err(tidb_error::MysqlError::from_terror(
                &tidb_error::catalog::variable::ERR_UNKNOWN_SYSTEM_VAR
                    .generate_with_stack_by_args(&[("not_a_real_variable".to_owned()).into()])
            ))
        );
        assert_eq!(globals.get("autocommit").unwrap(), "OFF");
    }

    #[test]
    fn a_read_only_variable_cannot_be_set() {
        let mut vars = session_vars();
        assert!(vars.get_system("version").unwrap().starts_with("8.0.11-"));
        assert_eq!(
            vars.set_system("version", "9".to_owned()),
            Err(tidb_error::MysqlError::from_terror(
                &tidb_error::catalog::variable::ERR_INCORRECT_SCOPE.generate_with_stack_by_args(&[
                    ("version".to_owned()).into(),
                    "read only".into()
                ])
            ))
        );
    }

    #[test]
    fn set_overrides_the_default_and_set_names_moves_three_variables() {
        let mut vars = session_vars();
        vars.set_system("autocommit", "OFF".to_owned()).unwrap();
        assert_eq!(vars.get_system("autocommit").unwrap(), "OFF");

        vars.set_charset("latin1", Some("latin1_bin"), true)
            .unwrap();
        for name in [
            "character_set_client",
            "character_set_connection",
            "character_set_results",
        ] {
            assert_eq!(vars.get_system(name).unwrap(), "latin1", "{name}");
        }
        assert_eq!(
            vars.get_system("collation_connection").unwrap(),
            "latin1_bin"
        );
        // SET NAMES leaves the server-side character set alone.
        assert_eq!(vars.get_system("character_set_server").unwrap(), "utf8mb4");
    }

    /// Go's charset/collation `SetSession` hooks are reciprocal: changing a
    /// charset picks its default collation, while changing a collation picks
    /// its owning charset. The client/results pair keeps its independent
    /// behavior, including the empty results escape hatch.
    #[test]
    fn charset_and_collation_hooks_match_go() {
        let mut vars = session_vars();

        vars.set_system("character_set_connection", "latin1".to_owned())
            .unwrap();
        assert_eq!(
            vars.get_system("character_set_connection").unwrap(),
            "latin1"
        );
        assert_eq!(
            vars.get_system("collation_connection").unwrap(),
            "latin1_bin"
        );

        vars.set_system("collation_connection", "UTF8MB4_GENERAL_CI".to_owned())
            .unwrap();
        assert_eq!(
            vars.get_system("collation_connection").unwrap(),
            "utf8mb4_general_ci"
        );
        assert_eq!(
            vars.get_system("character_set_connection").unwrap(),
            "utf8mb4"
        );

        vars.set_system("character_set_database", "utf8mb3".to_owned())
            .unwrap();
        assert_eq!(vars.get_system("character_set_database").unwrap(), "utf8");
        assert_eq!(vars.get_system("collation_database").unwrap(), "utf8_bin");

        vars.set_system("collation_database", "latin1_bin".to_owned())
            .unwrap();
        assert_eq!(vars.get_system("character_set_database").unwrap(), "latin1");

        vars.set_system("character_set_server", "ascii".to_owned())
            .unwrap();
        assert_eq!(vars.get_system("collation_server").unwrap(), "ascii_bin");
        vars.set_system("collation_server", "utf8mb4_bin".to_owned())
            .unwrap();
        assert_eq!(vars.get_system("character_set_server").unwrap(), "utf8mb4");

        vars.set_system("character_set_results", String::new())
            .unwrap();
        assert_eq!(vars.get_system("character_set_results").unwrap(), "");
        assert!(vars
            .set_system("character_set_connection", "not-a-charset".to_owned())
            .is_err_and(|error| error.identity.is_none()
                && error.message == "Unknown charset not-a-charset"));
        assert!(vars
            .set_system("collation_connection", "not-a-collation".to_owned())
            .is_err_and(|error| error.code == 1273));
    }

    /// Go's `tidb_enforce_mpp` Validation only permits ON after the
    /// session's `tidb_allow_mpp` gate is enabled.
    #[test]
    fn enforce_mpp_requires_allow_mpp_like_go() {
        let mut vars = session_vars();
        vars.set_system("tidb_allow_mpp", "OFF".to_owned()).unwrap();
        assert_eq!(vars.get_system("tidb_enforce_mpp").unwrap(), "OFF");
        assert_eq!(
            vars.set_system("tidb_enforce_mpp", "ON".to_owned()),
            Err(tidb_error::MysqlError::from_terror(
                &tidb_error::catalog::variable::ERR_WRONG_VALUE_FOR_VAR
                    .generate_with_stack_by_args(&[
                        ("tidb_enforce_mpp".to_owned()).into(),
                        ("1' but tidb_allow_mpp is 0, please activate tidb_allow_mpp at first."
                            .to_owned())
                        .into()
                    ])
            ))
        );

        vars.set_system("tidb_allow_mpp", "ON".to_owned()).unwrap();
        vars.set_system("tidb_enforce_mpp", "1".to_owned()).unwrap();
        assert_eq!(vars.get_system("tidb_enforce_mpp").unwrap(), "ON");
    }

    #[test]
    fn set_global_writes_the_shared_table_not_this_sessions_own_copy() {
        let mut vars = session_vars();
        // `autocommit` (scope 3: both SESSION and GLOBAL) is the case Go
        // documents: `SET GLOBAL` never moves the setting session's own
        // `@@autocommit`.
        vars.set_global("autocommit", "OFF".to_owned()).unwrap();
        assert_eq!(vars.get_system("autocommit").unwrap(), "ON");
        assert_eq!(vars.get_global("autocommit").unwrap(), "OFF");
    }

    #[test]
    fn set_global_redact_log_updates_the_process_redaction_authority() {
        // Loading or replacing a whole GLOBAL table republishes the process
        // switches; the tests that assert on them run apart from this one.
        let _serialised = process_switch_tests();
        let globals = GlobalSysvars::new();
        let mut vars = SessionVars::new();
        vars.swap_globals(Some(Arc::new(globals.clone())));
        vars.set_global("tidb_redact_log", "MARKER".to_owned())
            .unwrap();
        assert!(tidb_redact::need_redact());
        assert_eq!(tidb_redact::value("secret"), "?");

        globals.reset("tidb_redact_log").unwrap();
        assert!(!tidb_redact::need_redact());

        let scratch =
            GlobalSysvars::from_cluster_rows([("tidb_redact_log".to_owned(), "MARKER".to_owned())]);
        assert!(!tidb_redact::need_redact());
        globals.replace_from(&scratch);
        assert!(tidb_redact::need_redact());

        vars.set_global("tidb_redact_log", "OFF".to_owned())
            .unwrap();
        assert!(!tidb_redact::need_redact());

        globals
            .load_from_cluster([("tidb_redact_log".to_owned(), "ON".to_owned())]);
        assert!(tidb_redact::need_redact());
        globals.reset("tidb_redact_log").unwrap();
    }

    /// Go `TestValidateInternalSessionVariable`: explicit session writes hide
    /// internal variables, while a GLOBAL-only variable reports its scope.
    #[test]
    fn internal_session_variable_rejects_explicit_set_like_go() {
        let mut vars = session_vars();
        assert!(vars
            .set_system("tidb_redact_log", "ON".to_owned())
            .is_err_and(|error| error
                .equal(&tidb_error::catalog::variable::ERR_UNKNOWN_SYSTEM_VAR)
                && error.message == "Unknown system variable 'tidb_redact_log'"));
        let instance_error = vars
            .set_system("tidb_instance_plan_cache_max_size", "1".to_owned())
            .expect_err("instance plan cache max size is GLOBAL-only");
        assert!(instance_error.equal(&tidb_error::catalog::variable::ERR_GLOBAL_VARIABLE));
        assert_eq!(
            instance_error.message,
            "Variable 'tidb_instance_plan_cache_max_size' is a GLOBAL variable and should be set with SET GLOBAL"
        );
    }

    /// Go `TestInstanceScope`: instance-scoped variables have no SESSION
    /// hooks, accept typed values through the INSTANCE route, and expose the
    /// node-local value to subsequent reads without touching SESSION state.
    #[test]
    fn instance_scope_routes_and_metadata_match_go() {
        for definition in tidb_variable::sysvar::SYS_VARS {
            if definition.has_instance_scope() {
                assert!(
                    !definition.has_session_scope(),
                    "instance variable {} must not expose SESSION hooks",
                    definition.name
                );
            }
        }

        let mut vars = session_vars();
        assert_eq!(vars.get_system("tidb_general_log").unwrap(), "OFF");
        vars.set_instance("tidb_general_log", "oN".to_owned())
            .unwrap();
        assert_eq!(vars.get_system("tidb_general_log").unwrap(), "ON");
        assert!(vars
            .set_system("tidb_general_log", "OFF".to_owned())
            .is_err_and(
                |error| error.equal(&tidb_error::catalog::variable::ERR_GLOBAL_VARIABLE)
                    && error.message.starts_with("Variable 'tidb_general_log' ")
            ));
    }

    #[test]
    fn global_memory_arbitration_settings_update_the_running_process_authority() {
        struct NoopRecorder;

        impl tidb_memory::RecordMemState for NoopRecorder {
            fn load(&self) -> Result<Option<tidb_memory::RuntimeMemStateV1>, String> {
                Ok(None)
            }

            fn store(&self, _: &tidb_memory::RuntimeMemStateV1) -> Result<(), String> {
                Ok(())
            }
        }

        let arbitrator =
            tidb_memory::MemArbitrator::new(1 << 30, 4, 4, 64 << 10, Box::new(NoopRecorder));
        let _registration = tidb_memory::install_process_arbitrator(&arbitrator);
        let globals = GlobalSysvars::new();
        // Every cache rebuild runs each GLOBAL variable's SetGlobal hook, as
        // Go's `rebuildSysVarCache` does, so the server limit the arbitrator
        // was built with is stored as the global value first; its registry
        // value (80% of the host memory) would otherwise be applied.
        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_SERVER_MEMORY_LIMIT,
                "1GiB".to_owned(),
            )
            .unwrap();
        assert_eq!(arbitrator.limit_u64(), 1 << 30);

        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_MODE,
                "priority".to_owned(),
            )
            .unwrap();
        assert_eq!(
            arbitrator.work_mode(),
            tidb_memory::ArbitratorWorkMode::Priority
        );

        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_SOFT_LIMIT,
                "0.5".to_owned(),
            )
            .unwrap();
        assert_eq!(arbitrator.soft_limit(), 512 << 20);

        globals
            .set_instance(
                tidb_vardef::tidb_vars::TIDB_SERVER_MEMORY_LIMIT,
                "2GiB".to_owned(),
            )
            .unwrap();
        assert_eq!(arbitrator.limit_u64(), 2 << 30);

        globals
            .reset(tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_MODE)
            .unwrap();
        assert_eq!(
            arbitrator.work_mode(),
            tidb_memory::ArbitratorWorkMode::Disable
        );
    }

    #[test]
    fn set_global_on_a_session_only_variable_is_1228() {
        let mut vars = session_vars();
        // `error_count` is SESSION-only (scope 2): Go's `ErrLocalVariable`.
        assert_eq!(
            vars.set_global("debug_sync", "x".to_owned()),
            Err(tidb_error::MysqlError::from_terror(
                &tidb_error::catalog::variable::ERR_LOCAL_VARIABLE
                    .generate_with_stack_by_args(&[("debug_sync".to_owned()).into()])
            ))
        );
    }

    #[test]
    fn replacing_a_cluster_global_image_preserves_instance_only_values() {
        // Loading or replacing a whole GLOBAL table republishes the process
        // switches; the tests that assert on them run apart from this one.
        let _serialised = process_switch_tests();
        let globals = GlobalSysvars::new();
        globals
            .set_instance("tidb_general_log", "ON".to_owned())
            .expect("the instance-only variable sets");
        globals
            .set("autocommit", "OFF".to_owned())
            .expect("the old global override sets");

        let fresh = GlobalSysvars::from_cluster_rows([(
            "require_secure_transport".to_owned(),
            "ON".to_owned(),
        )]);
        globals.replace_from(&fresh);

        assert_eq!(globals.get("tidb_general_log").as_deref(), Ok("ON"));
        assert_eq!(globals.get("require_secure_transport").as_deref(), Ok("ON"));
        assert_eq!(globals.get("autocommit").as_deref(), Ok("ON"));
    }

    #[test]
    fn set_session_on_a_global_only_variable_is_1229() {
        let mut vars = session_vars();
        // `default_password_lifetime` is GLOBAL-only (scope 1): Go's
        // `ErrGlobalVariable`.
        assert_eq!(
            vars.set_system("default_password_lifetime", "5".to_owned()),
            Err(tidb_error::MysqlError::from_terror(
                &tidb_error::catalog::variable::ERR_GLOBAL_VARIABLE.generate_with_stack_by_args(&[
                    ("default_password_lifetime".to_owned()).into()
                ])
            ))
        );
    }

    #[test]
    fn reading_global_on_a_session_only_variable_is_1238() {
        let vars = session_vars();
        assert_eq!(
            vars.get_global("debug_sync"),
            Err(tidb_error::MysqlError::from_terror(
                &tidb_error::catalog::variable::ERR_INCORRECT_SCOPE.generate_with_stack_by_args(&[
                    ("debug_sync".to_owned()).into(),
                    "SESSION".into()
                ])
            ))
        );
    }

    #[test]
    fn a_new_session_inherits_the_global_but_a_live_session_does_not() {
        let globals = GlobalSysvars::new();

        // A session already open BEFORE the GLOBAL changes keeps its own
        // value -- MySQL's rule that the session copy is made once, at
        // connect.
        let mut live = session_vars();
        live.seed_from_globals(Arc::new(globals.clone())).unwrap();
        globals.set("autocommit", "OFF".to_owned()).unwrap();
        assert_eq!(live.get_system("autocommit").unwrap(), "ON");

        // `SET GLOBAL` again, then a brand NEW session opens: it inherits
        // the value that was live at ITS connect time.
        globals.set("autocommit", "OFF".to_owned()).unwrap();
        let mut fresh = session_vars();
        fresh.seed_from_globals(Arc::new(globals)).unwrap();
        assert_eq!(fresh.get_system("autocommit").unwrap(), "OFF");

        // Neither session's inherited copy is the live table: further
        // session-only `SET` traffic on one does not leak to the other.
        live.set_system("autocommit", "OFF".to_owned()).unwrap();
        fresh.set_system("autocommit", "ON".to_owned()).unwrap();
        assert_eq!(live.get_system("autocommit").unwrap(), "OFF");
        assert_eq!(fresh.get_system("autocommit").unwrap(), "ON");
    }
}

mod pd_region_batch_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Debug, Default)]
    struct Policy(AtomicBool);
    impl PdRegionPolicy for Policy {
        fn set_follower_handle(&self, value: bool) {
            self.0.store(value, Ordering::SeqCst);
        }
    }

    /// Records the concurrency Go's `opt.TSOClientRPCConcurrency` would carry.
    #[derive(Debug, Default)]
    struct ConcurrencyPolicy(std::sync::atomic::AtomicIsize);
    impl PdRegionPolicy for ConcurrencyPolicy {
        fn set_follower_handle(&self, _value: bool) {}
        fn set_tso_rpc_concurrency(&self, concurrency: isize) {
            self.0.store(concurrency, Ordering::SeqCst);
        }
    }

    /// Go maps the `tidb_tso_client_rpc_mode` enum onto a concurrency and
    /// sends THAT to the PD client (`pkg/domain/domain_sysvars.go:92-106`):
    /// `DEFAULT` 1, `PARALLEL` 2, `PARALLEL-FAST` 4. The variable's text
    /// never reaches the client, so a node that stored the string without
    /// mapping it left the option at its compiled default forever -- the
    /// shape this pins against.
    #[test]
    fn tso_client_rpc_mode_publishes_gos_mapped_concurrency() {
        let globals = GlobalSysvars::default();
        let policy = Arc::new(ConcurrencyPolicy::default());
        let name = tidb_vardef::tidb_vars::TIDB_TSO_CLIENT_RPC_MODE;
        globals.set_pd_region_policy(policy.clone());
        assert_eq!(
            policy.0.load(Ordering::SeqCst),
            1,
            "installation publishes Go's DEFAULT concurrency"
        );
        globals.set(name, "PARALLEL".to_owned()).unwrap();
        assert_eq!(policy.0.load(Ordering::SeqCst), 2);
        globals.set(name, "PARALLEL-FAST".to_owned()).unwrap();
        assert_eq!(policy.0.load(Ordering::SeqCst), 4);
        globals.set(name, "DEFAULT".to_owned()).unwrap();
        assert_eq!(policy.0.load(Ordering::SeqCst), 1);
        // Go's enum validator refuses anything else before the mapping runs,
        // so a rejected write leaves the published concurrency untouched.
        globals.set(name, "PARALLEL-SLOW".to_owned()).unwrap_err();
        assert_eq!(policy.0.load(Ordering::SeqCst), 1);
        // A missing cluster row restores Go's own default, as the sibling
        // PD options do.
        globals.set(name, "PARALLEL".to_owned()).unwrap();
        globals.reset(name).unwrap();
        assert_eq!(policy.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn pd_region_batch_concurrent_publication_keeps_the_latest_value() {
        use std::sync::{mpsc, Mutex};
        use std::time::Duration;
        #[derive(Debug)]
        struct BlockingPolicy {
            value: AtomicBool,
            block: AtomicBool,
            entered: mpsc::Sender<()>,
            release: Mutex<mpsc::Receiver<()>>,
        }
        impl PdRegionPolicy for BlockingPolicy {
            fn set_follower_handle(&self, value: bool) {
                if value && self.block.swap(false, Ordering::SeqCst) {
                    self.entered.send(()).unwrap();
                    self.release.lock().unwrap().recv().unwrap();
                }
                self.value.store(value, Ordering::SeqCst);
            }
        }
        let name = tidb_vardef::tidb_vars::PD_ENABLE_FOLLOWER_HANDLE_REGION;
        let globals = GlobalSysvars::default();
        globals.set(name, "OFF".to_owned()).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let policy = Arc::new(BlockingPolicy {
            value: AtomicBool::new(false),
            block: AtomicBool::new(true),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        globals.set_pd_region_policy(policy.clone());
        let first_globals = globals.clone();
        let first = std::thread::spawn(move || first_globals.set(name, "ON".to_owned()).unwrap());
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let second_globals = globals.clone();
        let (done_tx, done_rx) = mpsc::channel();
        let second = std::thread::spawn(move || {
            second_globals.set(name, "OFF".to_owned()).unwrap();
            done_tx.send(()).unwrap();
        });
        // A correct publication lock keeps the second callback behind the
        // first. The old unlocked callback lets OFF complete before stale ON.
        let _ = done_rx.recv_timeout(Duration::from_millis(100));
        release_tx.send(()).unwrap();
        first.join().unwrap();
        second.join().unwrap();
        assert_eq!(globals.get(name).unwrap(), "OFF");
        assert!(!policy.value.load(Ordering::SeqCst));
    }

    #[test]
    fn pd_region_batch_setting_default_commit_reload_and_reset() {
        let globals = GlobalSysvars::default();
        let policy = Arc::new(Policy::default());
        let name = tidb_vardef::tidb_vars::PD_ENABLE_FOLLOWER_HANDLE_REGION;
        globals.set_pd_region_policy(policy.clone());
        assert!(policy.0.load(Ordering::SeqCst));
        globals.set(name, "OFF".to_owned()).unwrap();
        assert!(!policy.0.load(Ordering::SeqCst));
        let scratch = GlobalSysvars::from_cluster_rows([(name.to_owned(), "OFF".to_owned())]);
        scratch.set_pd_region_policy(policy.clone());
        scratch.set(name, "ON".to_owned()).unwrap();
        assert!(
            !policy.0.load(Ordering::SeqCst),
            "uncommitted validation must not publish"
        );
        globals.replace_from(&scratch);
        assert!(policy.0.load(Ordering::SeqCst));
        globals.replace_from(&GlobalSysvars::from_cluster_rows([]));
        assert!(
            policy.0.load(Ordering::SeqCst),
            "missing row restores Go default"
        );
        globals.set(name, "OFF".to_owned()).unwrap();
        globals.reset(name).unwrap();
        assert!(policy.0.load(Ordering::SeqCst));
        assert!(globals.set(name, "invalid".to_owned()).is_err());
        assert!(policy.0.load(Ordering::SeqCst));
        // Go `variable.SetSysVar` replaces only the registry value: a stored
        // row still wins, and the replaced value is what a missing row
        // serves.
        globals.set(name, "ON".to_owned()).unwrap();
        globals.set_startup(name, "OFF".to_owned());
        assert!(policy.0.load(Ordering::SeqCst));
        globals.reset(name).unwrap();
        assert!(!policy.0.load(Ordering::SeqCst));
    }

    #[test]
    fn pd_region_batch_changed_name_publication_preserves_other_settings() {
        let globals = GlobalSysvars::default();
        let policy = Arc::new(Policy(AtomicBool::new(true)));
        let name = tidb_vardef::tidb_vars::PD_ENABLE_FOLLOWER_HANDLE_REGION;
        globals.set_pd_region_policy(policy.clone());
        let scratch = GlobalSysvars::from_cluster_rows([(name.to_owned(), "OFF".to_owned())]);
        globals.publish_global_changes_from(&scratch, &[name.to_owned()]);
        assert!(!policy.0.load(Ordering::SeqCst));
        globals.set("max_connections", "333".to_owned()).unwrap();
        assert!(!policy.0.load(Ordering::SeqCst));
        assert_eq!(globals.get("max_connections").unwrap(), "333");
    }
}
