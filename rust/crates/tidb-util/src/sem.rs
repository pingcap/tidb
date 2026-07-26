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

//! `pkg/util/sem`: Security Enhanced Mode (SEM) visibility policy.
//!
//! SEM hides a fixed set of schemas, tables, status/system variables, and
//! restricts certain dynamic privileges. Those sets are SEM's own policy
//! data; Go references `vardef`/`metadef`/`mysql` constants for readability,
//! and this port inlines the identical string values (verified against
//! `pkg/sessionctx/vardef` and `pkg/parser/mysql`).
//!
//! PACKAGE IN PROGRESS: Go's `Enable`/`Disable` additionally write the
//! `tidb_enable_enhanced_security` and `hostname` system variables through
//! `variable.SetSysVar` (the process-wide sysvar registry) and log a notice.
//! That registry is the mutable global singleton the rewrite deliberately
//! replaces with explicit wiring, so here `enable`/`disable` flip only the
//! SEM mode flag; the sysvar-registry sync and log belong to session-layer
//! integration. Every predicate (the package's entire tested surface) is
//! ported with its upstream tests.

use std::sync::atomic::{AtomicBool, Ordering};

// SEM's restricted system-table names (Go's package-local constants).
const EXPR_PUSHDOWN_BLACKLIST: &str = "expr_pushdown_blacklist";
const GC_DELETE_RANGE: &str = "gc_delete_range";
const GC_DELETE_RANGE_DONE: &str = "gc_delete_range_done";
const OPT_RULE_BLACKLIST: &str = "opt_rule_blacklist";
const TIDB: &str = "tidb";
const GLOBAL_VARIABLES: &str = "global_variables";
const CLUSTER_CONFIG: &str = "cluster_config";
const CLUSTER_HARDWARE: &str = "cluster_hardware";
const CLUSTER_LOAD: &str = "cluster_load";
const CLUSTER_LOG: &str = "cluster_log";
const CLUSTER_SYSTEM_INFO: &str = "cluster_systeminfo";
const INSPECTION_RESULT: &str = "inspection_result";
const INSPECTION_RULES: &str = "inspection_rules";
const INSPECTION_SUMMARY: &str = "inspection_summary";
const METRICS_SUMMARY: &str = "metrics_summary";
const METRICS_SUMMARY_BY_LABEL: &str = "metrics_summary_by_label";
const METRICS_TABLES: &str = "metrics_tables";
const TIDB_HOT_REGIONS: &str = "tidb_hot_regions";
const PD_PROFILE_ALLOCS: &str = "pd_profile_allocs";
const PD_PROFILE_BLOCK: &str = "pd_profile_block";
const PD_PROFILE_CPU: &str = "pd_profile_cpu";
const PD_PROFILE_GOROUTINES: &str = "pd_profile_goroutines";
const PD_PROFILE_MEMORY: &str = "pd_profile_memory";
const PD_PROFILE_MUTEX: &str = "pd_profile_mutex";
const TIDB_PROFILE_ALLOCS: &str = "tidb_profile_allocs";
const TIDB_PROFILE_BLOCK: &str = "tidb_profile_block";
const TIDB_PROFILE_CPU: &str = "tidb_profile_cpu";
const TIDB_PROFILE_GOROUTINES: &str = "tidb_profile_goroutines";
const TIDB_PROFILE_MEMORY: &str = "tidb_profile_memory";
const TIDB_PROFILE_MUTEX: &str = "tidb_profile_mutex";
const TIKV_PROFILE_CPU: &str = "tikv_profile_cpu";
const TIDB_GC_LEADER_DESC: &str = "tidb_gc_leader_desc";
const RESTRICTED_PRIV: &str = "RESTRICTED_";
// A sysvar installed by a plugin.
const TIDB_AUDIT_REDACT_LOG: &str = "tidb_audit_redact_log";

// Database names (mysql.SystemDB and metadef's CIStr `.L` lower forms).
const SYSTEM_DB: &str = "mysql";
const INFORMATION_SCHEMA_L: &str = "information_schema";
const PERFORMANCE_SCHEMA_L: &str = "performance_schema";
const METRIC_SCHEMA_L: &str = "metrics_schema";

/// The system variables SEM hides (Go's `vardef.*` values, inlined).
const INVISIBLE_SYS_VARS: &[&str] = &[
    "ddl_slow_threshold",                  // TiDBDDLSlowOprThreshold
    "tidb_check_mb4_value_in_utf8",        // TiDBCheckMb4ValueInUTF8
    "tidb_config",                         // TiDBConfig
    "tidb_enable_slow_log",                // TiDBEnableSlowLog
    "tidb_enable_telemetry",               // TiDBEnableTelemetry
    "tidb_expensive_query_time_threshold", // TiDBExpensiveQueryTimeThreshold
    "tidb_force_priority",                 // TiDBForcePriority
    "tidb_general_log",                    // TiDBGeneralLog
    "tidb_metric_query_range_duration",    // TiDBMetricSchemaRangeDuration
    "tidb_metric_query_step",              // TiDBMetricSchemaStep
    "tidb_opt_write_row_id",               // TiDBOptWriteRowID
    "tidb_pprof_sql_cpu",                  // TiDBPProfSQLCPU
    "tidb_record_plan_in_slow_log",        // TiDBRecordPlanInSlowLog
    "tidb_row_format_version",             // TiDBRowFormatVersion
    "tidb_slow_query_file",                // TiDBSlowQueryFile
    "tidb_slow_log_threshold",             // TiDBSlowLogThreshold
    "tidb_enable_collect_execution_info",  // TiDBEnableCollectExecutionInfo
    "tidb_memory_usage_alarm_ratio",       // TiDBMemoryUsageAlarmRatio
    "tidb_redact_log",                     // TiDBRedactLog
    "tidb_restricted_read_only",           // TiDBRestrictedReadOnly
    "tidb_top_sql_max_time_series_count",  // TiDBTopSQLMaxTimeSeriesCount
    "tidb_top_sql_max_meta_count",         // TiDBTopSQLMaxMetaCount
    "tidb_service_scope",                  // TiDBServiceScope
    "tidb_cloud_storage_uri",              // TiDBCloudStorageURI
    "tidb_stmt_summary_max_stmt_count",    // TiDBStmtSummaryMaxStmtCount
    "tidb_server_memory_limit",            // TiDBServerMemoryLimit
    "tidb_server_memory_limit_gc_trigger", // TiDBServerMemoryLimitGCTrigger
    "tidb_instance_plan_cache_max_size",   // TiDBInstancePlanCacheMaxMemSize
    "tidb_stats_cache_mem_quota",          // TiDBStatsCacheMemQuota
    "tidb_mem_quota_binding_cache",        // TiDBMemQuotaBindingCache
    "tidb_schema_cache_size",              // TiDBSchemaCacheSize
    TIDB_AUDIT_REDACT_LOG,
];

/// The process-wide SEM flag (Go's `semEnabled int32`).
static SEM_ENABLED: AtomicBool = AtomicBool::new(false);

/// Go `Enable`: turns SEM on.
///
/// Only the mode flag is set here; see the module-level note on the deferred
/// sysvar-registry sync and log notice.
pub fn enable() {
    SEM_ENABLED.store(true, Ordering::SeqCst);
}

/// Go `Disable`: turns SEM off (see the note on `enable`).
pub fn disable() {
    SEM_ENABLED.store(false, Ordering::SeqCst);
}

/// Go `IsEnabled`: whether SEM is currently on.
#[must_use]
pub fn is_enabled() -> bool {
    SEM_ENABLED.load(Ordering::SeqCst)
}

/// Go `IsInvisibleSchema`: whether `db_name` is hidden under SEM.
#[must_use]
pub fn is_invisible_schema(db_name: &str) -> bool {
    db_name.eq_ignore_ascii_case(METRIC_SCHEMA_L)
}

/// Go `IsInvisibleTable`: whether the lower-cased schema/table is hidden.
#[must_use]
pub fn is_invisible_table(db_lower_name: &str, tbl_lower_name: &str) -> bool {
    match db_lower_name {
        SYSTEM_DB => matches!(
            tbl_lower_name,
            EXPR_PUSHDOWN_BLACKLIST
                | GC_DELETE_RANGE
                | GC_DELETE_RANGE_DONE
                | OPT_RULE_BLACKLIST
                | TIDB
                | GLOBAL_VARIABLES
        ),
        INFORMATION_SCHEMA_L => matches!(
            tbl_lower_name,
            CLUSTER_CONFIG
                | CLUSTER_HARDWARE
                | CLUSTER_LOAD
                | CLUSTER_LOG
                | CLUSTER_SYSTEM_INFO
                | INSPECTION_RESULT
                | INSPECTION_RULES
                | INSPECTION_SUMMARY
                | METRICS_SUMMARY
                | METRICS_SUMMARY_BY_LABEL
                | METRICS_TABLES
                | TIDB_HOT_REGIONS
        ),
        PERFORMANCE_SCHEMA_L => matches!(
            tbl_lower_name,
            PD_PROFILE_ALLOCS
                | PD_PROFILE_BLOCK
                | PD_PROFILE_CPU
                | PD_PROFILE_GOROUTINES
                | PD_PROFILE_MEMORY
                | PD_PROFILE_MUTEX
                | TIDB_PROFILE_ALLOCS
                | TIDB_PROFILE_BLOCK
                | TIDB_PROFILE_CPU
                | TIDB_PROFILE_GOROUTINES
                | TIDB_PROFILE_MEMORY
                | TIDB_PROFILE_MUTEX
                | TIKV_PROFILE_CPU
        ),
        METRIC_SCHEMA_L => true,
        _ => false,
    }
}

/// Go `IsInvisibleStatusVar`: whether the status variable is hidden.
#[must_use]
pub fn is_invisible_status_var(var_name: &str) -> bool {
    var_name == TIDB_GC_LEADER_DESC
}

/// Go `IsInvisibleSysVar`: whether the (lower-cased) system variable is
/// hidden under SEM.
#[must_use]
pub fn is_invisible_sys_var(var_name_in_lower: &str) -> bool {
    INVISIBLE_SYS_VARS.contains(&var_name_in_lower)
}

/// Go `IsRestrictedPrivilege`: whether a dynamic privilege must not be
/// satisfied by `SUPER` (i.e. it is a `RESTRICTED_*` privilege).
#[must_use]
pub fn is_restricted_privilege(priv_name_in_upper: &str) -> bool {
    debug_assert!(
        priv_name_in_upper == priv_name_in_upper.to_uppercase(),
        "privilege name must be uppercase"
    );
    // Go requires len >= 12 (a bare "RESTRICTED_" of length 11 is not one).
    priv_name_in_upper.len() >= 12 && priv_name_in_upper.starts_with(RESTRICTED_PRIV)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go TestInvisibleSchema.
    #[test]
    fn invisible_schema() {
        assert!(is_invisible_schema(METRIC_SCHEMA_L));
        assert!(is_invisible_schema("METRICS_ScHEma"));
        assert!(!is_invisible_schema("mysql"));
        assert!(!is_invisible_schema(INFORMATION_SCHEMA_L));
        assert!(!is_invisible_schema("Bogusname"));
    }

    // Go TestIsInvisibleTable.
    #[test]
    fn invisible_table() {
        let mysql_tbls = [
            EXPR_PUSHDOWN_BLACKLIST,
            GC_DELETE_RANGE,
            GC_DELETE_RANGE_DONE,
            OPT_RULE_BLACKLIST,
            TIDB,
            GLOBAL_VARIABLES,
        ];
        let info_schema_tbls = [
            CLUSTER_CONFIG,
            CLUSTER_HARDWARE,
            CLUSTER_LOAD,
            CLUSTER_LOG,
            CLUSTER_SYSTEM_INFO,
            INSPECTION_RESULT,
            INSPECTION_RULES,
            INSPECTION_SUMMARY,
            METRICS_SUMMARY,
            METRICS_SUMMARY_BY_LABEL,
            METRICS_TABLES,
            TIDB_HOT_REGIONS,
        ];
        let perf_schema_tbls = [
            PD_PROFILE_ALLOCS,
            PD_PROFILE_BLOCK,
            PD_PROFILE_CPU,
            PD_PROFILE_GOROUTINES,
            PD_PROFILE_MEMORY,
            PD_PROFILE_MUTEX,
            TIDB_PROFILE_ALLOCS,
            TIDB_PROFILE_BLOCK,
            TIDB_PROFILE_CPU,
            TIDB_PROFILE_GOROUTINES,
            TIDB_PROFILE_MEMORY,
            TIDB_PROFILE_MUTEX,
            TIKV_PROFILE_CPU,
        ];

        for tbl in mysql_tbls {
            assert!(is_invisible_table(SYSTEM_DB, tbl), "mysql.{tbl}");
        }
        for tbl in info_schema_tbls {
            assert!(is_invisible_table(INFORMATION_SCHEMA_L, tbl), "is.{tbl}");
        }
        for tbl in perf_schema_tbls {
            assert!(is_invisible_table(PERFORMANCE_SCHEMA_L, tbl), "ps.{tbl}");
        }
        assert!(is_invisible_table(METRIC_SCHEMA_L, "acdc"));
        assert!(is_invisible_table(METRIC_SCHEMA_L, "fdsgfd"));
        assert!(!is_invisible_table("test", "t1"));
    }

    // Go TestIsRestrictedPrivilege.
    #[test]
    fn restricted_privilege() {
        assert!(is_restricted_privilege("RESTRICTED_TABLES_ADMIN"));
        assert!(is_restricted_privilege("RESTRICTED_STATUS_VARIABLES_ADMIN"));
        assert!(!is_restricted_privilege("CONNECTION_ADMIN"));
        assert!(!is_restricted_privilege("BACKUP_ADMIN"));
        assert!(!is_restricted_privilege("AA"));
    }

    // Go TestIsInvisibleStatusVar.
    #[test]
    fn invisible_status_var() {
        assert!(is_invisible_status_var(TIDB_GC_LEADER_DESC));
        assert!(!is_invisible_status_var("server_id"));
        assert!(!is_invisible_status_var("ddl_schema_version"));
        assert!(!is_invisible_status_var("Ssl_version"));
    }

    // Go TestIsInvisibleSysVar.
    #[test]
    fn invisible_sys_var() {
        // Visible (not in the set).
        assert!(!is_invisible_sys_var("hostname"));
        assert!(!is_invisible_sys_var("tidb_enable_enhanced_security"));
        assert!(!is_invisible_sys_var("tidb_allow_remove_auto_inc"));

        // Invisible.
        for name in INVISIBLE_SYS_VARS {
            assert!(is_invisible_sys_var(name), "{name}");
        }
    }

    // The mode flag round-trips (Go's IsEnabled after Enable/Disable). Only
    // this test touches the global flag; it restores it afterward.
    #[test]
    fn enabled_flag() {
        let prev = is_enabled();
        enable();
        assert!(is_enabled());
        disable();
        assert!(!is_enabled());
        SEM_ENABLED.store(prev, Ordering::SeqCst);
    }
}
