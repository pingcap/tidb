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
