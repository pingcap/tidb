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

//! Go `sysvar.go`'s `SetGlobal` / `GetGlobal` closures over `vardef`'s
//! process state: `SetGlobal` stores the variable's process-wide value (on
//! an explicit `SET GLOBAL` and on every sysvar cache rebuild) and
//! `GetGlobal` reads it back.

use std::sync::atomic::Ordering::SeqCst;

use crate::error::VarError;
use crate::vars::SessionVars;

/// Go `TiDBOptOn`.
fn on(value: &str) -> bool {
    tidb_vardef::option_values::tidb_opt_on(value)
}

/// Go `BoolToOnOff`.
fn on_off(value: bool) -> String {
    if value { "ON" } else { "OFF" }.to_owned()
}

macro_rules! set_global {
    ($($name:ident => |$value:ident| $body:expr;)*) => {
        $(
            pub(super) fn $name(_vars: &mut SessionVars, $value: &str) -> Result<(), VarError> {
                $body;
                Ok(())
            }
        )*
    };
}

macro_rules! get_global {
    ($($name:ident => $body:expr;)*) => {
        $(
            pub(super) fn $name() -> String {
                $body
            }
        )*
    };
}

set_global! {
    set_tidb_ttl_job_enable => |s| tidb_vardef::ENABLE_TTL_JOB.store(on(s), SeqCst);
    // Go: `strconv.ParseInt(s, 10, 64)` then `Store`; the registry has
    // validated the value, so the parse cannot fail.
    set_tidb_ttl_scan_batch_size => |s| if let Ok(v) = s.parse::<i64>() {
        tidb_vardef::TTL_SCAN_BATCH_SIZE.store(v, SeqCst);
    };
    set_tidb_ttl_delete_batch_size => |s| if let Ok(v) = s.parse::<i64>() {
        tidb_vardef::TTL_DELETE_BATCH_SIZE.store(v, SeqCst);
    };
    set_tidb_ttl_delete_rate_limit => |s| if let Ok(v) = s.parse::<i64>() {
        tidb_vardef::TTL_DELETE_RATE_LIMIT.store(v, SeqCst);
    };
    set_tidb_ttl_scan_worker_count => |s| if let Ok(v) = s.parse::<i64>() {
        tidb_vardef::TTL_SCAN_WORKER_COUNT.store(v as i32, SeqCst);
    };
    set_tidb_ttl_delete_worker_count => |s| if let Ok(v) = s.parse::<i64>() {
        tidb_vardef::TTL_DELETE_WORKER_COUNT.store(v as i32, SeqCst);
    };
    set_tidb_ttl_running_tasks => |s| if let Ok(v) = s.parse::<i64>() {
        tidb_vardef::TTL_RUNNING_TASKS.store(v as i32, SeqCst);
    };
    set_tidb_enable_foreign_key => |s| tidb_vardef::ENABLE_FOREIGN_KEY.store(on(s), SeqCst);
    set_tidb_enable_row_level_checksum => |s| tidb_vardef::ENABLE_ROW_LEVEL_CHECKSUM.store(on(s), SeqCst);
    set_tidb_enable_check_constraint => |s| tidb_vardef::ENABLE_CHECK_CONSTRAINT.store(on(s), SeqCst);
    set_tidb_ddl_enable_fast_reorg => |s| tidb_vardef::ENABLE_FAST_REORG.store(on(s), SeqCst);
    set_tidb_enable_auto_analyze => |s| tidb_vardef::RUN_AUTO_ANALYZE.store(on(s), SeqCst);
    set_tidb_enable_instance_plan_cache => |s| tidb_vardef::ENABLE_INSTANCE_PLAN_CACHE.store(on(s), SeqCst);
    set_tidb_resource_control_strict_mode => |s| {
        tidb_vardef::ENABLE_RESOURCE_CONTROL_STRICT_MODE.store(on(s), SeqCst)
    };
    set_tidb_enable_fast_create_table => |s| tidb_vardef::ENABLE_FAST_CREATE_TABLE.store(on(s), SeqCst);
    set_tidb_accelerate_user_creation_update => |s| {
        tidb_vardef::ACCELERATE_USER_CREATION_UPDATE.store(on(s), SeqCst)
    };
    set_max_user_connections => |s| tidb_vardef::MAX_USER_CONNECTIONS_VALUE.store(
        crate::varsutil::tidb_opt_int64(s, tidb_vardef::defaults::DEF_MAX_USER_CONNECTIONS) as u32,
        SeqCst,
    );
    set_performance_schema_session_connect_attrs_size => |s| tidb_vardef::CONNECT_ATTRS_SIZE.store(
        crate::varsutil::tidb_opt_int64(s, tidb_vardef::defaults::DEF_CONNECT_ATTRS_SIZE),
        SeqCst,
    );
    // Go: `vardef.DDLDiskQuota.Store(TidbOptUint64(val, DefTiDBDDLDiskQuota))`.
    set_tidb_ddl_disk_quota => |s| tidb_vardef::DDL_DISK_QUOTA.store(
        s.parse::<u64>().unwrap_or(tidb_vardef::defaults::DEF_TIDB_DDL_DISK_QUOTA as u64),
        SeqCst,
    );
    set_tidb_auto_analyze_concurrency => |s| tidb_vardef::AUTO_ANALYZE_CONCURRENCY.store(
        s.parse::<i64>().unwrap_or(tidb_vardef::defaults::DEF_TIDB_AUTO_ANALYZE_CONCURRENCY),
        SeqCst,
    );
    set_tidb_max_auto_analyze_time => |s| if let Ok(v) = s.parse::<i64>() {
        tidb_vardef::MAX_AUTO_ANALYZE_TIME.store(v, SeqCst);
    };
    set_tidb_stats_cache_mem_quota => |s| if let Ok(v) = s.parse::<i64>() {
        tidb_vardef::STATS_CACHE_MEM_QUOTA.store(v, SeqCst);
    };
    set_tidb_memory_usage_alarm_keep_record_num => |s| if let Ok(v) = s.parse::<i64>() {
        tidb_vardef::MEMORY_USAGE_ALARM_KEEP_RECORD_NUM.store(v, SeqCst);
    };
    // Go: `vardef.TxnEntrySizeLimit.Store(TidbOptUint64(val, Def))`.
    set_tidb_txn_entry_size_limit => |s| tidb_vardef::TXN_ENTRY_SIZE_LIMIT.store(
        crate::varsutil::tidb_opt_uint64(s, tidb_vardef::defaults::DEF_TIDB_TXN_ENTRY_SIZE_LIMIT as u64),
        SeqCst,
    );
    set_tidb_table_cache_lease => |s| if let Ok(v) = s.parse::<i64>() {
        tidb_vardef::TABLE_CACHE_LEASE.store(v, SeqCst);
    };
    // Go: `SetMaxDeltaSchemaCount(TidbOptInt64(val, Def))`.
    set_tidb_max_delta_schema_count => |s| tidb_vardef::set_max_delta_schema_count(
        crate::varsutil::tidb_opt_int64(s, tidb_vardef::defaults::DEF_TIDB_MAX_DELTA_SCHEMA_COUNT),
    );
    // Go `tidbOptPositiveInt32(val, Def...)` for the three DDL knobs.
    set_tidb_ddl_flashback_concurrency => |s| tidb_vardef::set_ddl_flashback_concurrency(
        s.parse::<i32>().ok().filter(|cnt| *cnt > 0)
            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_DDL_FLASHBACK_CONCURRENCY as i32),
    );
    set_tidb_ddl_reorg_worker_count => |s| tidb_vardef::set_ddl_reorg_worker_counter(
        s.parse::<i32>().ok().filter(|cnt| *cnt > 0)
            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_DDL_REORG_WORKER_COUNT as i32),
    );
    set_tidb_ddl_reorg_batch_size => |s| tidb_vardef::set_ddl_reorg_batch_size(
        s.parse::<i32>().ok().filter(|cnt| *cnt > 0)
            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_DDL_REORG_BATCH_SIZE as i32),
    );
    set_tidb_ddl_error_count_limit => |s| tidb_vardef::set_ddl_error_count_limit(
        s.parse::<i64>().unwrap_or(tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT),
    );
}

/// Go `tidb_instance_plan_cache_max_size`'s `SetGlobal`: a byte size of at
/// least `MinTiDBInstancePlanCacheMemSize` (100 MiB).
pub(super) fn set_tidb_instance_plan_cache_max_mem_size(
    _vars: &mut SessionVars,
    s: &str,
) -> Result<(), VarError> {
    let Some((v, _)) = crate::varsutil::parse_byte_size(s) else {
        return Err(tidb_terror::MysqlError::unknown(format!(
            "invalid tidb_instance_plan_cache_max_mem_size value {s}"
        )));
    };
    if v < 100 * 1024 * 1024 {
        return Err(tidb_terror::MysqlError::unknown(
            "tidb_instance_plan_cache_max_mem_size should be at least 100MiB".to_owned(),
        ));
    }
    tidb_vardef::INSTANCE_PLAN_CACHE_MAX_MEM_SIZE.store(v.min(i64::MAX as u64) as i64, SeqCst);
    Ok(())
}

get_global! {
    get_tidb_ttl_job_enable => on_off(tidb_vardef::ENABLE_TTL_JOB.load(SeqCst));
    get_tidb_ttl_scan_batch_size => tidb_vardef::TTL_SCAN_BATCH_SIZE.load(SeqCst).to_string();
    get_tidb_ttl_delete_batch_size => tidb_vardef::TTL_DELETE_BATCH_SIZE.load(SeqCst).to_string();
    get_tidb_ttl_delete_rate_limit => tidb_vardef::TTL_DELETE_RATE_LIMIT.load(SeqCst).to_string();
    get_tidb_ttl_scan_worker_count => tidb_vardef::TTL_SCAN_WORKER_COUNT.load(SeqCst).to_string();
    get_tidb_ttl_delete_worker_count => tidb_vardef::TTL_DELETE_WORKER_COUNT.load(SeqCst).to_string();
    get_tidb_ttl_running_tasks => tidb_vardef::TTL_RUNNING_TASKS.load(SeqCst).to_string();
    get_tidb_enable_foreign_key => on_off(tidb_vardef::ENABLE_FOREIGN_KEY.load(SeqCst));
    get_tidb_enable_row_level_checksum => on_off(tidb_vardef::ENABLE_ROW_LEVEL_CHECKSUM.load(SeqCst));
    get_tidb_enable_check_constraint => on_off(tidb_vardef::ENABLE_CHECK_CONSTRAINT.load(SeqCst));
    get_tidb_ddl_enable_fast_reorg => on_off(tidb_vardef::ENABLE_FAST_REORG.load(SeqCst));
    get_tidb_enable_auto_analyze => on_off(tidb_vardef::RUN_AUTO_ANALYZE.load(SeqCst));
    get_tidb_enable_instance_plan_cache => on_off(tidb_vardef::ENABLE_INSTANCE_PLAN_CACHE.load(SeqCst));
    get_tidb_resource_control_strict_mode => on_off(tidb_vardef::ENABLE_RESOURCE_CONTROL_STRICT_MODE.load(SeqCst));
    get_tidb_enable_fast_create_table => on_off(tidb_vardef::ENABLE_FAST_CREATE_TABLE.load(SeqCst));
    get_max_user_connections => tidb_vardef::MAX_USER_CONNECTIONS_VALUE.load(SeqCst).to_string();
    get_performance_schema_session_connect_attrs_size => tidb_vardef::CONNECT_ATTRS_SIZE.load(SeqCst).to_string();
    get_tidb_ddl_disk_quota => tidb_vardef::DDL_DISK_QUOTA.load(SeqCst).to_string();
    get_tidb_auto_analyze_concurrency => tidb_vardef::AUTO_ANALYZE_CONCURRENCY.load(SeqCst).to_string();
    get_tidb_max_auto_analyze_time => tidb_vardef::MAX_AUTO_ANALYZE_TIME.load(SeqCst).to_string();
    get_tidb_stats_cache_mem_quota => tidb_vardef::STATS_CACHE_MEM_QUOTA.load(SeqCst).to_string();
    get_tidb_memory_usage_alarm_keep_record_num => tidb_vardef::MEMORY_USAGE_ALARM_KEEP_RECORD_NUM.load(SeqCst).to_string();
    get_tidb_instance_plan_cache_max_mem_size => tidb_vardef::INSTANCE_PLAN_CACHE_MAX_MEM_SIZE.load(SeqCst).to_string();
}

/// Go `tidbOptFloat64`.
fn opt_float64(value: &str, default: f64) -> f64 {
    crate::varsutil::tidb_opt_float64(value, default)
}

set_global! {
    set_tidb_mem_oom_action => |s| tidb_vardef::set_oom_action(s);
    set_tidb_memory_usage_alarm_ratio => |s| tidb_vardef::set_memory_usage_alarm_ratio(
        opt_float64(s, tidb_vardef::defaults::DEF_MEMORY_USAGE_ALARM_RATIO),
    );
    // Go: `atomic.StoreInt32(&vardef.ForcePriority, int32(mysql.Str2Priority(val)))`.
    set_tidb_force_priority => |s| tidb_vardef::FORCE_PRIORITY
        .store(tidb_mysql::priority_from_str(s).as_enum(), SeqCst);
    // Go: `tikvstore.TxnCommitBatchSize.Store(uint64(TidbOptInt64(val, Def)))`.
    set_tidb_txn_commit_batch_size => |s| tikv_client::kv::TXN_COMMIT_BATCH_SIZE.store(
        crate::varsutil::tidb_opt_int64(s, tikv_client::kv::DEF_TXN_COMMIT_BATCH_SIZE as i64) as u64,
        SeqCst,
    );
    // Go: `tikvstore.StoreLimit.Store(TidbOptInt64(val, DefTiDBStoreLimit))`.
    set_tidb_store_limit => |s| tikv_client::kv::STORE_LIMIT.store(
        crate::varsutil::tidb_opt_int64(s, tidb_vardef::defaults::DEF_TIDB_STORE_LIMIT),
        SeqCst,
    );
    // Go: starter mode keeps the internal TLS transport gate disabled.
    set_require_secure_transport => |s| tidb_tls::REQUIRE_SECURE_TRANSPORT
        .store(!tidb_config_deploymode::is_starter() && on(s), SeqCst);
    set_tidb_check_mb4_value_in_utf8 => |s| tidb_config::config_tree::config::update_global(|config| {
        config.instance.check_mb4_value_in_utf8 =
            tidb_config::config_tree::marshal::AtomicBool::new(on(s));
    });
    // Go: `vardef.ServiceScope.Store(strings.ToLower(s))` and the config's
    // `Instance.TiDBServiceScope`.
    set_tidb_service_scope => |s| {
        let new_value = s.to_lowercase();
        tidb_vardef::set_service_scope(new_value.clone());
        if tidb_config::config_tree::config::get_global_config().instance.tidb_service_scope != new_value {
            tidb_config::config_tree::config::update_global(|config| {
                config.instance.tidb_service_scope = new_value;
            });
        }
    };
    // Go: only a change reaches the resource-control switch.
    set_tidb_enable_resource_control => |s| {
        let enable = on(s);
        if tidb_vardef::ENABLE_RESOURCE_CONTROL.swap(enable, SeqCst) != enable {
            crate::vars::set_global_resource_control(enable);
            tidb_logutil::bg_logger().info(
                "set resource control",
                &[tidb_log::Field::new("enable", tidb_log::Value::Bool(enable))],
            );
        }
    };
}

/// Go `tidb_ttl_job_schedule_window_start_time`'s `SetGlobal`:
/// `time.ParseInLocation(FullDayTimeFormat, s, time.UTC)`.
pub(super) fn set_tidb_ttl_job_schedule_window_start_time(
    _vars: &mut SessionVars,
    s: &str,
) -> Result<(), VarError> {
    let time = tidb_vardef::DayTime::parse(s).map_err(tidb_terror::MysqlError::unknown)?;
    *tidb_vardef::TTL_JOB_SCHEDULE_WINDOW_START_TIME
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = time;
    Ok(())
}

/// Go `tidb_ttl_job_schedule_window_end_time`'s `SetGlobal`.
pub(super) fn set_tidb_ttl_job_schedule_window_end_time(
    _vars: &mut SessionVars,
    s: &str,
) -> Result<(), VarError> {
    let time = tidb_vardef::DayTime::parse(s).map_err(tidb_terror::MysqlError::unknown)?;
    *tidb_vardef::TTL_JOB_SCHEDULE_WINDOW_END_TIME
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = time;
    Ok(())
}

/// Go `tidb_instance_plan_cache_reserved_percentage`'s `SetGlobal`.
pub(super) fn set_tidb_instance_plan_cache_reserved_percentage(
    _vars: &mut SessionVars,
    s: &str,
) -> Result<(), VarError> {
    let v = opt_float64(
        s,
        tidb_vardef::defaults::DEF_TIDB_INSTANCE_PLAN_CACHE_RESERVED_PERCENTAGE,
    );
    if !(0.0..=1.0).contains(&v) {
        return Err(tidb_terror::MysqlError::unknown(format!(
            "invalid tidb_instance_plan_cache_reserved_percentage value {s}"
        )));
    }
    tidb_vardef::INSTANCE_PLAN_CACHE_RESERVED_PERCENTAGE.store(v.to_bits(), SeqCst);
    Ok(())
}

/// Go `tidb_cb_pd_metadata_error_rate_threshold_ratio`'s `SetGlobal`.
pub(super) fn set_tidb_circuit_breaker_pd_metadata_error_rate_threshold_ratio(
    _vars: &mut SessionVars,
    s: &str,
) -> Result<(), VarError> {
    let v = opt_float64(
        s,
        tidb_vardef::defaults::DEF_TIDB_CIRCUIT_BREAKER_PD_META_ERROR_RATE_RATIO,
    );
    if !(0.0..=1.0).contains(&v) {
        return Err(tidb_terror::MysqlError::unknown(format!(
            "invalid tidb_cb_pd_metadata_error_rate_threshold_ratio value {s}"
        )));
    }
    tidb_vardef::set_circuit_breaker_pd_metadata_error_rate_threshold_ratio(v);
    Ok(())
}

get_global! {
    get_tidb_mem_oom_action => tidb_vardef::oom_action().to_owned();
    get_tidb_txn_commit_batch_size => tikv_client::kv::TXN_COMMIT_BATCH_SIZE.load(SeqCst).to_string();
    get_tidb_store_limit => tikv_client::kv::STORE_LIMIT.load(SeqCst).to_string();
    // Go: starter mode intentionally exposes require_secure_transport as ON.
    get_require_secure_transport => if tidb_config_deploymode::is_starter() {
        "ON".to_owned()
    } else {
        on_off(tidb_tls::REQUIRE_SECURE_TRANSPORT.load(SeqCst))
    };
    get_tidb_check_mb4_value_in_utf8 => on_off(
        tidb_config::config_tree::config::get_global_config().instance.check_mb4_value_in_utf8.load(),
    );
    get_tidb_service_scope => tidb_vardef::service_scope();
    get_tidb_ttl_job_schedule_window_start_time => tidb_vardef::TTL_JOB_SCHEDULE_WINDOW_START_TIME
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .format();
    get_tidb_ttl_job_schedule_window_end_time => tidb_vardef::TTL_JOB_SCHEDULE_WINDOW_END_TIME
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .format();
    // Go `strconv.FormatFloat(v, 'f', -1, 64)`.
    get_tidb_instance_plan_cache_reserved_percentage => f64::from_bits(
        tidb_vardef::INSTANCE_PLAN_CACHE_RESERVED_PERCENTAGE.load(SeqCst),
    )
    .to_string();
    get_tidb_circuit_breaker_pd_metadata_error_rate_threshold_ratio =>
        tidb_vardef::circuit_breaker_pd_metadata_error_rate_threshold_ratio().to_string();
}

/// Go `fmt.Sprintf("%g", v)` for a ratio: the shortest representation,
/// in exponent form (`1e-05`) below 1e-4.
fn go_g(v: f64) -> String {
    if v != 0.0 && v.abs() < 1e-4 {
        let formatted = format!("{v:e}");
        let (mantissa, exponent) = formatted.split_once('e').expect("exponent form");
        let (sign, digits) = match exponent.strip_prefix('-') {
            Some(digits) => ('-', digits),
            None => ('+', exponent),
        };
        return format!("{mantissa}e{sign}{digits:0>2}");
    }
    v.to_string()
}

/// Go `mysql.Priority2Str`.
fn priority_to_str(priority: i32) -> String {
    match priority {
        1 => "LOW_PRIORITY",
        2 => "HIGH_PRIORITY",
        3 => "DELAYED",
        0 => "NO_PRIORITY",
        _ => "",
    }
    .to_owned()
}

get_global! {
    get_tidb_enable_resource_control => on_off(tidb_vardef::ENABLE_RESOURCE_CONTROL.load(SeqCst));
    get_tidb_memory_usage_alarm_ratio => go_g(tidb_vardef::memory_usage_alarm_ratio());
    get_tidb_force_priority => priority_to_str(tidb_vardef::FORCE_PRIORITY.load(SeqCst));
}

set_global! {
    // Go `TiDBAnalyzeDefaultNumBuckets` / `TopN`: `strconv.ParseUint` then
    // `Store`; the registry's unsigned type has validated the value.
    set_tidb_analyze_default_num_buckets => |s| if let Ok(v) = s.parse::<u64>() {
        tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.store(v, SeqCst);
    };
    set_tidb_analyze_default_num_top_n => |s| if let Ok(v) = s.parse::<u64>() {
        tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.store(v, SeqCst);
    };
    set_tidb_enable_auto_analyze_priority_queue => |s| {
        tidb_vardef::ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE.store(on(s), SeqCst);
    };
    // Go: `oracles.EnableTSValidation.Store(TiDBOptOn(val))`.
    set_tidb_enable_ts_validation => |s| {
        tikv_client::oracle::oracles::ENABLE_TS_VALIDATION.store(on(s), SeqCst);
    };
    // Go: `tikvutil.CommitterConcurrency.Store(int32(TidbOptInt64(val, def)))`.
    set_tidb_committer_concurrency => |s| tidb_tikvutil::COMMITTER_CONCURRENCY.store(
        crate::varsutil::tidb_opt_int64(s, tidb_vardef::defaults::DEF_TIDB_COMMITTER_CONCURRENCY) as i32,
        SeqCst,
    );
}

get_global! {
    get_tidb_analyze_default_num_buckets => tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.load(SeqCst).to_string();
    get_tidb_analyze_default_num_top_n => tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.load(SeqCst).to_string();
    get_tidb_enable_auto_analyze_priority_queue => on_off(tidb_vardef::ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE.load(SeqCst));
    get_tidb_committer_concurrency => tidb_tikvutil::COMMITTER_CONCURRENCY.load(SeqCst).to_string();
    get_tidb_enable_metadata_lock => on_off(tidb_vardef::is_mdl_enabled(tidb_config_kerneltype::is_next_gen()));
    get_tidb_schema_cache_size => tidb_vardef::SCHEMA_CACHE_SIZE_ORIGIN_TEXT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
}

/// Go's `SetGlobal` for the PD client options:
/// `(*SetPDClientDynamicOption.Load())(name, val)`, the domain's
/// `setPDClientDynamicOption`.
macro_rules! pd_client_dynamic_option {
    ($($fn_name:ident => $var:path;)*) => {
        $(
            pub(super) fn $fn_name(_vars: &mut SessionVars, value: &str) -> Result<(), VarError> {
                crate::domain_slots::pd_client_dynamic_option($var, value)
            }
        )*
    };
}

pd_client_dynamic_option! {
    set_tidb_tso_client_batch_max_wait_time => tidb_vardef::tidb_vars::TIDB_TSO_CLIENT_BATCH_MAX_WAIT_TIME;
    set_tidb_enable_tso_follower_proxy => tidb_vardef::tidb_vars::TIDB_ENABLE_TSO_FOLLOWER_PROXY;
    set_pd_enable_follower_handle_region => tidb_vardef::tidb_vars::PD_ENABLE_FOLLOWER_HANDLE_REGION;
    set_tidb_enable_batch_query_region => tidb_vardef::tidb_vars::TIDB_ENABLE_BATCH_QUERY_REGION;
    set_tidb_tso_client_rpc_mode => tidb_vardef::tidb_vars::TIDB_TSO_CLIENT_RPC_MODE;
}

/// Go `TiDBLowResolutionTSOUpdateInterval`'s `SetGlobal`: store the interval,
/// then `SetLowResolutionTSOUpdateInterval(interval)` when the domain set it.
pub(super) fn set_tidb_low_resolution_tso_update_interval(
    _vars: &mut SessionVars,
    value: &str,
) -> Result<(), VarError> {
    tidb_vardef::LOW_RESOLUTION_TSO_UPDATE_INTERVAL.store(
        crate::varsutil::tidb_opt_int64(value, tidb_vardef::defaults::DEF_TIDB_LOW_RESOLUTION_TSO_UPDATE_INTERVAL) as u32,
        SeqCst,
    );
    crate::domain_slots::low_resolution_tso_update_interval(std::time::Duration::from_millis(u64::from(
        tidb_vardef::LOW_RESOLUTION_TSO_UPDATE_INTERVAL.load(SeqCst),
    )))
}

get_global! {
    // Go `strconv.FormatFloat(vardef.MaxTSOBatchWaitInterval.Load(), 'f', -1, 64)`.
    get_tidb_tso_client_batch_max_wait_time =>
        f64::from_bits(tidb_vardef::MAX_TSO_BATCH_WAIT_INTERVAL.load(SeqCst)).to_string();
    get_tidb_enable_tso_follower_proxy => on_off(tidb_vardef::ENABLE_TSO_FOLLOWER_PROXY.load(SeqCst));
    get_pd_enable_follower_handle_region => on_off(tidb_vardef::ENABLE_PD_FOLLOWER_HANDLE_REGION.load(SeqCst));
    get_tidb_enable_batch_query_region => on_off(tidb_vardef::ENABLE_BATCH_QUERY_REGION.load(SeqCst));
}

set_global! {
    // Go: `stmtsummaryv2.SetEnabled(TiDBOptOn(val))` and its siblings; the
    // registry has validated each value, so the parse cannot fail.
    set_tidb_enable_stmt_summary => |s| tidb_stmtsummary::v2::stmtsummary::set_enabled(on(s));
    set_tidb_stmt_summary_internal_query => |s| tidb_stmtsummary::v2::stmtsummary::set_enable_internal_query(on(s));
    set_tidb_stmt_summary_refresh_interval => |s| if let Ok(v) = s.parse::<i64>() {
        tidb_stmtsummary::v2::stmtsummary::set_refresh_interval(v);
    };
    set_tidb_stmt_summary_history_size => |s| if let Ok(v) = s.parse::<i32>() {
        tidb_stmtsummary::v2::stmtsummary::set_history_size(v);
    };
    set_tidb_stmt_summary_max_stmt_count => |s| if let Ok(v) = s.parse::<i64>() {
        tidb_stmtsummary::v2::stmtsummary::set_max_stmt_count(v);
    };
    set_tidb_stmt_summary_max_sql_length => |s| if let Ok(v) = s.parse::<i32>() {
        tidb_stmtsummary::v2::stmtsummary::set_max_sql_length(v);
    };
    set_tidb_stmt_summary_persist_evicted => |s| tidb_stmtsummary::v2::stmtsummary::set_persist_evicted(on(s));
    set_tidb_stmt_summary_group_by_user => |s| tidb_stmtsummary::v2::stmtsummary::set_group_by_user(on(s));
}

/// Go's memory-arbitration `SetGlobal` (`tidb_server_memory_limit`, its
/// session minimum, the arbitrator's mode and soft limit): the process-wide
/// limit and the global arbitrator take the value.
macro_rules! process_memory_setting {
    ($($fn_name:ident => $var:path;)*) => {
        $(
            pub(super) fn $fn_name(_vars: &mut SessionVars, value: &str) -> Result<(), VarError> {
                tidb_memory::apply_process_memory_setting($var, value).map_err(VarError::unknown)
            }
        )*
    };
}

process_memory_setting! {
    set_tidb_server_memory_limit => tidb_vardef::tidb_vars::TIDB_SERVER_MEMORY_LIMIT;
    set_tidb_server_memory_limit_sess_min_size => tidb_vardef::tidb_vars::TIDB_SERVER_MEMORY_LIMIT_SESS_MIN_SIZE;
    set_tidb_mem_arbitrator_mode => tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_MODE;
    set_tidb_mem_arbitrator_soft_limit => tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_SOFT_LIMIT;
}

get_global! {
    // Go: `vardef.CloudStorageURI.Load()`, redacted (`ast.RedactURL`).
    get_tidb_cloud_storage_uri => crate::cloud_storage::get_cloud_storage_uri();
}

/// Go `tidbOptPositiveInt32`.
fn opt_positive_int32(value: &str, default: i32) -> i32 {
    value.parse::<i32>().ok().filter(|v| *v > 0).unwrap_or(default)
}

set_global! {
    set_tidb_enable_tiflash_pipeline_model => |s| tidb_vardef::TI_FLASH_ENABLE_PIPELINE_MODE.store(on(s), SeqCst);
    set_tidb_general_log => |s| tidb_vardef::PROCESS_GENERAL_LOG.store(on(s), SeqCst);
    set_tidb_rc_read_check_ts => |s| tidb_vardef::ENABLE_RC_READ_CHECK_TS.store(on(s), SeqCst);
    set_tidb_persist_analyze_options => |s| tidb_vardef::PERSIST_ANALYZE_OPTIONS.store(on(s), SeqCst);
    set_tidb_stats_load_pseudo_timeout => |s| tidb_vardef::STATS_LOAD_PSEUDO_TIMEOUT.store(on(s), SeqCst);
    set_tidb_enable_batch_dml => |s| tidb_vardef::ENABLE_BATCH_DML.store(on(s), SeqCst);
    set_tidb_enable_noop_variables => |s| tidb_vardef::ENABLE_NOOP_VARIABLES.store(on(s), SeqCst);
    set_tidb_enable_tmp_storage_on_oom => |s| tidb_vardef::ENABLE_TMP_STORAGE_ON_OOM.store(on(s), SeqCst);
    set_tidb_enable_binding_usage => |s| tidb_vardef::ENABLE_BINDING_USAGE.store(on(s), SeqCst);
    set_tidb_mlog_log_slow_purge => |s| tidb_vardef::M_LOG_LOG_SLOW_PURGE.store(on(s), SeqCst);
    set_tidb_ignore_inlist_plan_digest => |s| tidb_vardef::IGNORE_INLIST_PLAN_DIGEST.store(on(s), SeqCst);
    set_tidb_enable_connection_event_log => |s| tidb_vardef::ENABLE_CONNECTION_EVENT_LOG.store(on(s), SeqCst);
    set_tidb_enable_dist_task => |s| tidb_vardef::ENABLE_DIST_TASK.store(on(s), SeqCst);
    set_validate_password_length => |s| tidb_vardef::PASSWORD_VALIDATION_LENGTH.store(crate::varsutil::tidb_opt_int64(s, 8 as i64) as i32, SeqCst);
    set_validate_password_mixed_case_count => |s| tidb_vardef::PASSWORD_VALIDATION_MIXED_CASE_COUNT.store(crate::varsutil::tidb_opt_int64(s, 1 as i64) as i32, SeqCst);
    set_validate_password_number_count => |s| tidb_vardef::PASSWORD_VALIDTAION_NUMBER_COUNT.store(crate::varsutil::tidb_opt_int64(s, 1 as i64) as i32, SeqCst);
    set_validate_password_special_char_count => |s| tidb_vardef::PASSWORD_VALIDATION_SPECIAL_CHAR_COUNT.store(crate::varsutil::tidb_opt_int64(s, 1 as i64) as i32, SeqCst);
    set_password_history => |s| tidb_vardef::PASSWORD_HISTORY.store(crate::varsutil::tidb_opt_int64(s, tidb_vardef::defaults::DEF_PASSWORD_REUSE_HISTORY as i64), SeqCst);
    set_password_reuse_interval => |s| tidb_vardef::PASSWORD_REUSE_INTERVAL.store(crate::varsutil::tidb_opt_int64(s, tidb_vardef::defaults::DEF_PASSWORD_REUSE_TIME as i64), SeqCst);
    set_tidb_mem_quota_binding_cache => |s| tidb_vardef::MEM_QUOTA_BINDING_CACHE.store(crate::varsutil::tidb_opt_int64(s, tidb_vardef::defaults::DEF_TIDB_MEM_QUOTA_BINDING_CACHE as i64), SeqCst);
    set_tidb_stats_load_sync_wait => |s| tidb_vardef::STATS_LOAD_SYNC_WAIT.store(crate::varsutil::tidb_opt_int64(s, tidb_vardef::defaults::DEF_TIDB_STATS_LOAD_SYNC_WAIT as i64), SeqCst);
    set_tidb_query_log_max_len => |s| tidb_vardef::QUERY_LOG_MAX_LEN.store(crate::varsutil::tidb_opt_int64(s, tidb_vardef::defaults::DEF_TIDB_QUERY_LOG_MAX_LEN as i64) as i32, SeqCst);
    set_tidb_paging_size_bytes => |s| if let Ok(v) = s.parse::<i64>() {
        tidb_vardef::PAGING_SIZE_BYTES.store(v, SeqCst);
    };
    set_tidb_expensive_query_time_threshold => |s| tidb_vardef::EXPENSIVE_QUERY_TIME_THRESHOLD.store(opt_positive_int32(s, tidb_vardef::defaults::DEF_TIDB_EXPENSIVE_QUERY_TIME_THRESHOLD as i32) as u64, SeqCst);
    set_tidb_expensive_txn_time_threshold => |s| tidb_vardef::EXPENSIVE_TXN_TIME_THRESHOLD.store(opt_positive_int32(s, tidb_vardef::defaults::DEF_TIDB_EXPENSIVE_TXN_TIME_THRESHOLD as i32) as u64, SeqCst);
    set_tidb_pprof_sql_cpu => |s| tidb_vardef::ENABLE_P_PROF_SQLCPU.store(opt_positive_int32(s, 0) > 0, SeqCst);
}

get_global! {
    get_tidb_enable_tiflash_pipeline_model => on_off(tidb_vardef::TI_FLASH_ENABLE_PIPELINE_MODE.load(SeqCst));
    get_tidb_general_log => on_off(tidb_vardef::PROCESS_GENERAL_LOG.load(SeqCst));
    get_tidb_rc_read_check_ts => on_off(tidb_vardef::ENABLE_RC_READ_CHECK_TS.load(SeqCst));
    get_tidb_persist_analyze_options => on_off(tidb_vardef::PERSIST_ANALYZE_OPTIONS.load(SeqCst));
    get_tidb_stats_load_pseudo_timeout => on_off(tidb_vardef::STATS_LOAD_PSEUDO_TIMEOUT.load(SeqCst));
    get_tidb_enable_batch_dml => on_off(tidb_vardef::ENABLE_BATCH_DML.load(SeqCst));
    get_tidb_enable_noop_variables => on_off(tidb_vardef::ENABLE_NOOP_VARIABLES.load(SeqCst));
    get_tidb_enable_tmp_storage_on_oom => on_off(tidb_vardef::ENABLE_TMP_STORAGE_ON_OOM.load(SeqCst));
    get_tidb_enable_binding_usage => on_off(tidb_vardef::ENABLE_BINDING_USAGE.load(SeqCst));
    get_tidb_mlog_log_slow_purge => on_off(tidb_vardef::M_LOG_LOG_SLOW_PURGE.load(SeqCst));
    get_tidb_ignore_inlist_plan_digest => on_off(tidb_vardef::IGNORE_INLIST_PLAN_DIGEST.load(SeqCst));
    get_tidb_enable_connection_event_log => on_off(tidb_vardef::ENABLE_CONNECTION_EVENT_LOG.load(SeqCst));
    get_tidb_enable_dist_task => on_off(tidb_vardef::ENABLE_DIST_TASK.load(SeqCst));
    get_validate_password_length => tidb_vardef::PASSWORD_VALIDATION_LENGTH.load(SeqCst).to_string();
    get_validate_password_mixed_case_count => tidb_vardef::PASSWORD_VALIDATION_MIXED_CASE_COUNT.load(SeqCst).to_string();
    get_validate_password_number_count => tidb_vardef::PASSWORD_VALIDTAION_NUMBER_COUNT.load(SeqCst).to_string();
    get_validate_password_special_char_count => tidb_vardef::PASSWORD_VALIDATION_SPECIAL_CHAR_COUNT.load(SeqCst).to_string();
    get_password_history => tidb_vardef::PASSWORD_HISTORY.load(SeqCst).to_string();
    get_password_reuse_interval => tidb_vardef::PASSWORD_REUSE_INTERVAL.load(SeqCst).to_string();
    get_tidb_mem_quota_binding_cache => tidb_vardef::MEM_QUOTA_BINDING_CACHE.load(SeqCst).to_string();
    get_tidb_stats_load_sync_wait => tidb_vardef::STATS_LOAD_SYNC_WAIT.load(SeqCst).to_string();
    get_tidb_query_log_max_len => tidb_vardef::QUERY_LOG_MAX_LEN.load(SeqCst).to_string();
    get_tidb_paging_size_bytes => tidb_vardef::PAGING_SIZE_BYTES.load(SeqCst).to_string();
    get_tidb_expensive_query_time_threshold => tidb_vardef::EXPENSIVE_QUERY_TIME_THRESHOLD.load(SeqCst).to_string();
    get_tidb_expensive_txn_time_threshold => tidb_vardef::EXPENSIVE_TXN_TIME_THRESHOLD.load(SeqCst).to_string();
    get_tidb_pprof_sql_cpu => if tidb_vardef::ENABLE_P_PROF_SQLCPU.load(SeqCst) { "1" } else { "0" }.to_owned();
}

set_global! {
    // Go: `strconv.ParseFloat`, stored when it parses.
    set_tidb_prepared_plan_cache_memory_guard_ratio => |s| if let Ok(v) = s.parse::<f64>() {
        tidb_vardef::PREPARED_PLAN_CACHE_MEMORY_GUARD_RATIO.store(v.to_bits(), SeqCst);
    };
    set_tidb_analyze_column_options => |s| {
        *tidb_vardef::ANALYZE_COLUMN_OPTIONS.write().unwrap_or_else(std::sync::PoisonError::into_inner) =
            s.to_ascii_uppercase();
    };
    set_tidb_advancer_check_point_lag_limit => |s| if let Ok(d) = tidb_config_configtypes::parse_duration(s) {
        tidb_vardef::ADVANCER_CHECK_POINT_LAG_LIMIT.store(d, SeqCst);
    };
    // Go also resets the session's `TxnScope` to global on ON -> OFF; this
    // port has no local transaction scope.
    set_tidb_enable_local_txn => |s| tidb_vardef::ENABLE_LOCAL_TXN.store(on(s), SeqCst);
}

get_global! {
    get_tidb_prepared_plan_cache_memory_guard_ratio => tidb_goruntime::go_strconv::format_float(
        f64::from_bits(tidb_vardef::PREPARED_PLAN_CACHE_MEMORY_GUARD_RATIO.load(SeqCst)),
        b'f',
        -1,
        64,
    );
    get_tidb_analyze_column_options => {
        let value = tidb_vardef::ANALYZE_COLUMN_OPTIONS.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        if value.is_empty() { tidb_vardef::defaults::DEF_TIDB_ANALYZE_COLUMN_OPTIONS.to_owned() } else { value.clone() }
    };
    get_tidb_advancer_check_point_lag_limit => tidb_config_configtypes::format_go_duration(
        tidb_vardef::ADVANCER_CHECK_POINT_LAG_LIMIT.load(SeqCst),
    );
    get_tidb_enable_local_txn => on_off(tidb_vardef::ENABLE_LOCAL_TXN.load(SeqCst));
    // Go: `BoolToOnOff(true)` / `vardef.On` / `"1"`.
    get_tidb_enable_column_tracking => on_off(true);
    get_tidb_opt_index_join_build_v2 => on_off(true);
    get_tidb_merge_partition_stats_concurrency => "1".to_owned();
    get_disconnect_on_expired_password => on_off(!tidb_vardef::IS_SAND_BOX_MODE_ENABLED.load(SeqCst));
}

// Go's `GetGlobal` closures over the instance configuration
// (`config.GetGlobalConfig().Instance`).
get_global! {
    get_max_connections => tidb_config::config_tree::config::get_global_config().instance.max_connections.to_string();
    get_tidb_enable_ddl => on_off(tidb_config::config_tree::config::get_global_config().instance.tidb_enable_ddl.load());
    get_tidb_enable_stats_owner => on_off(tidb_config::config_tree::config::get_global_config().instance.tidb_enable_stats_owner.load());
    get_plugin_audit_log_buffer_size => tidb_config::config_tree::config::get_global_config().instance.plugin_audit_log_buffer_size.to_string();
    get_plugin_audit_log_flush_interval => tidb_config::config_tree::config::get_global_config().instance.plugin_audit_log_flush_interval.to_string();
    get_tidb_config => tidb_config::config_tree::config::get_global_config().get_json_config().unwrap_or_default();
    get_tidb_slow_log_threshold => tidb_config::config_tree::config::get_global_config().instance.slow_threshold.to_string();
    get_tidb_enable_slow_log => on_off(tidb_config::config_tree::config::get_global_config().instance.enable_slow_log.load());
    get_tidb_record_plan_in_slow_log => on_off(tidb_config::config_tree::config::get_global_config().instance.record_plan_in_slow_log != 0);
    get_tidb_enable_collect_execution_info => on_off(tidb_config::config_tree::config::get_global_config().instance.enable_collect_execution_info.load());
    get_plugin_dir => tidb_config::config_tree::config::get_global_config().instance.plugin_dir.clone();
    get_plugin_load => tidb_config::config_tree::config::get_global_config().instance.plugin_load.clone();
    get_tidb_stmt_summary_enable_persistent => on_off(tidb_config::config_tree::config::get_global_config().instance.stmt_summary_enable_persistent);
    get_tidb_stmt_summary_filename => tidb_config::config_tree::config::get_global_config().instance.stmt_summary_filename.clone();
    get_tidb_stmt_summary_file_max_days => tidb_config::config_tree::config::get_global_config().instance.stmt_summary_file_max_days.to_string();
    get_tidb_stmt_summary_file_max_size => tidb_config::config_tree::config::get_global_config().instance.stmt_summary_file_max_size.to_string();
    get_tidb_stmt_summary_file_max_backups => tidb_config::config_tree::config::get_global_config().instance.stmt_summary_file_max_backups.to_string();
    get_tidb_log_file_max_days => tidb_config::config_tree::config::get_global_config().log.file.max_days.to_string();
}

set_global! {
    set_ddl_slow_threshold => |s| tidb_vardef::DDL_SLOW_OPR_THRESHOLD.store(opt_positive_int32(s, 300) as u32, SeqCst);
}

get_global! {
    get_ddl_slow_threshold => tidb_vardef::DDL_SLOW_OPR_THRESHOLD.load(SeqCst).to_string();
}

// Go's `SetGlobal` closures over the instance configuration.
set_global! {
    set_tidb_slow_log_threshold => |s| {
        let value = crate::varsutil::tidb_opt_int64(s, 300) as u64;
        tidb_config::config_tree::config::update_global(|c| c.instance.slow_threshold = value);
    };
    set_tidb_record_plan_in_slow_log => |s| {
        let value = crate::varsutil::tidb_opt_int64(s, 1) as u32;
        tidb_config::config_tree::config::update_global(|c| c.instance.record_plan_in_slow_log = value);
    };
    set_tidb_enable_slow_log => |s| {
        let value = on(s);
        tidb_config::config_tree::config::update_global(|c| c.instance.enable_slow_log.0 = value);
    };
    // Go stores a copy of the configuration when the value changes.
    set_tidb_enable_collect_execution_info => |s| {
        let new_value = on(s);
        if tidb_config::config_tree::config::get_global_config().instance.enable_collect_execution_info.load()
            != new_value
        {
            tidb_config::config_tree::config::update_global(|c| {
                c.instance.enable_collect_execution_info.0 = new_value;
            });
        }
    };
    set_max_connections => |s| {
        let value = crate::varsutil::tidb_opt_int64(s, 0) as u32;
        tidb_config::config_tree::config::update_global(|c| c.instance.max_connections = value);
    };
    // Go also switches the DDL / statistics owner (`switchDDL`,
    // `switchStats`), which the node does where it reads the switch.
    set_tidb_enable_ddl => |s| {
        let value = on(s);
        tidb_config::config_tree::config::update_global(|c| c.instance.tidb_enable_ddl.0 = value);
    };
    set_tidb_enable_stats_owner => |s| {
        let value = on(s);
        tidb_config::config_tree::config::update_global(|c| c.instance.tidb_enable_stats_owner.0 = value);
    };
    // Go also replaces the logger with the new file retention.
    set_tidb_log_file_max_days => |s| if let Ok(max_age) = s.parse::<i32>() {
        tidb_config::config_tree::config::update_global(|c| c.log.file.max_days = max_age as _);
    };
}
