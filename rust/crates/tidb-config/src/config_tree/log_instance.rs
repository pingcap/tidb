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

//! The `Log` and `Instance` sub-sections of Go `pkg/config/config.go`'s
//! `Config`, with their `defaultConf` values.

use serde::{Deserialize, Serialize};

/// File log config (Go `logutil.FileLogConfig`, wrapping `pingcap/log`'s
/// `FileLogConfig`). Defined here rather than reused from `tidb-log` to
/// keep `tidb-config` a leaf crate (`tidb-log` depends on `tidb-config`
/// for its duration formatter). Same shape as `tidb_log::FileLogConfig`.
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FileLogConfig {
    /// Log filename; empty disables file log.
    #[serde(rename = "filename")]
    pub filename: String,
    /// Max size for a single file, in MB.
    #[serde(rename = "max-size")]
    pub max_size: i64,
    /// Max log keep days; default never deletes.
    #[serde(rename = "max-days")]
    pub max_days: i64,
    /// Maximum number of old log files to retain.
    #[serde(rename = "max-backups")]
    pub max_backups: i64,
    /// Compression for rotated files: `gzip` or empty (disabled).
    #[serde(rename = "compression")]
    pub compression: String,
    /// Whether to use a buffered logger.
    #[serde(rename = "is-buffered")]
    pub is_buffered: bool,
    /// Buffer size when buffered.
    #[serde(rename = "buffer-size")]
    pub buffer_size: i64,
    /// Buffer flush interval (nanoseconds, Go `time.Duration`).
    #[serde(rename = "buffer-flush-interval")]
    pub buffer_flush_interval: i64,
}

use super::marshal::{AtomicBool, NullableBool, NB_UNSET};

// logutil defaults (Go `pkg/util/logutil`), inlined to avoid a heavy dep.
const DEFAULT_LOG_MAX_SIZE: i64 = 300;
const DEFAULT_SLOW_THRESHOLD: u64 = 300;
const DEFAULT_QUERY_LOG_MAX_LEN: u64 = 4096;
const DEFAULT_RECORD_PLAN_IN_SLOW_LOG: u32 = 1;
const DEFAULT_TIDB_ENABLE_SLOW_LOG: bool = true;

/// Log section of the config (Go `Log`).
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Log {
    /// Log level.
    #[serde(rename = "level")]
    pub level: String,
    /// Log format (json or text).
    #[serde(rename = "format")]
    pub format: String,
    /// Deprecated: use `enable-timestamp`.
    #[serde(rename = "disable-timestamp")]
    pub disable_timestamp: NullableBool,
    /// Enable automatic timestamps in log output.
    #[serde(rename = "enable-timestamp")]
    pub enable_timestamp: NullableBool,
    /// Deprecated: use `enable-error-stack`.
    #[serde(rename = "disable-error-stack")]
    pub disable_error_stack: NullableBool,
    /// Enable full-stack error annotation.
    #[serde(rename = "enable-error-stack")]
    pub enable_error_stack: NullableBool,
    /// File log config.
    #[serde(rename = "file")]
    pub file: FileLogConfig,
    /// Slow-query log filename.
    #[serde(rename = "slow-query-file")]
    pub slow_query_file: String,
    /// Deprecated expensive-query threshold.
    #[serde(rename = "expensive-threshold")]
    pub expensive_threshold: u32,
    /// General log filename.
    #[serde(rename = "general-log-file")]
    pub general_log_file: String,
    /// Deprecated query-log max length.
    #[serde(rename = "query-log-max-len")]
    pub query_log_max_len: u64,
    /// Deprecated enable-slow-log (moved to instance).
    #[serde(rename = "enable-slow-log")]
    pub enable_slow_log: AtomicBool,
    /// Deprecated slow-threshold.
    #[serde(rename = "slow-threshold")]
    pub slow_threshold: u64,
    /// Deprecated record-plan-in-slow-log.
    #[serde(rename = "record-plan-in-slow-log")]
    pub record_plan_in_slow_log: u32,
    /// Panic if a log write hangs this many seconds.
    #[serde(rename = "timeout")]
    pub timeout: i64,
}

impl Default for Log {
    // From Go `defaultConf.Log`.
    fn default() -> Self {
        Log {
            level: "info".into(),
            format: "text".into(),
            disable_timestamp: NB_UNSET,
            enable_timestamp: NB_UNSET,
            disable_error_stack: NB_UNSET,
            enable_error_stack: NB_UNSET,
            file: FileLogConfig {
                max_size: DEFAULT_LOG_MAX_SIZE,
                ..Default::default()
            },
            slow_query_file: "tidb-slow.log".into(),
            expensive_threshold: 10000,
            general_log_file: String::new(),
            query_log_max_len: DEFAULT_QUERY_LOG_MAX_LEN,
            enable_slow_log: AtomicBool::new(DEFAULT_TIDB_ENABLE_SLOW_LOG),
            slow_threshold: DEFAULT_SLOW_THRESHOLD,
            record_plan_in_slow_log: DEFAULT_RECORD_PLAN_IN_SLOW_LOG,
            timeout: 0,
        }
    }
}

impl Log {
    /// Go `getDisableTimestamp`.
    pub fn get_disable_timestamp(&self) -> bool {
        if self.enable_timestamp == NB_UNSET && self.disable_timestamp == NB_UNSET {
            return false;
        }
        if self.enable_timestamp == NB_UNSET {
            return self.disable_timestamp.to_bool();
        }
        !self.enable_timestamp.to_bool()
    }

    /// Go `getDisableErrorStack`.
    pub fn get_disable_error_stack(&self) -> bool {
        if self.enable_error_stack == NB_UNSET && self.disable_error_stack == NB_UNSET {
            return true;
        }
        if self.enable_error_stack == NB_UNSET {
            return self.disable_error_stack.to_bool();
        }
        !self.enable_error_stack.to_bool()
    }
}

/// Instance section of the config (Go `Instance`).
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Instance {
    /// Log every query in info level.
    #[serde(rename = "tidb_general_log")]
    pub tidb_general_log: bool,
    /// Add SQL label to pprof result.
    #[serde(rename = "tidb_pprof_sql_cpu")]
    pub enable_pprof_sql_cpu: bool,
    /// DDL slow-operation threshold.
    #[serde(rename = "ddl_slow_threshold")]
    pub ddl_slow_opr_threshold: u32,
    /// Expensive-query time threshold.
    #[serde(rename = "tidb_expensive_query_time_threshold")]
    pub expensive_query_time_threshold: u64,
    /// Expensive-transaction time threshold.
    #[serde(rename = "tidb_expensive_txn_time_threshold")]
    pub expensive_txn_time_threshold: u64,
    /// Enable stmtsummary file persistence.
    #[serde(rename = "tidb_stmt_summary_enable_persistent")]
    pub stmt_summary_enable_persistent: bool,
    /// Stmtsummary persistence filename.
    #[serde(rename = "tidb_stmt_summary_filename")]
    pub stmt_summary_filename: String,
    /// Stmtsummary file retention days.
    #[serde(rename = "tidb_stmt_summary_file_max_days")]
    pub stmt_summary_file_max_days: i64,
    /// Stmtsummary file max size (MB).
    #[serde(rename = "tidb_stmt_summary_file_max_size")]
    pub stmt_summary_file_max_size: i64,
    /// Stmtsummary file max backups.
    #[serde(rename = "tidb_stmt_summary_file_max_backups")]
    pub stmt_summary_file_max_backups: i64,
    /// Max stmts kept in memory.
    #[serde(rename = "tidb_stmt_summary_max_stmt_count")]
    pub stmt_summary_max_stmt_count: u64,
    /// Server memory limit.
    #[serde(rename = "tidb_server_memory_limit")]
    pub server_memory_limit: String,
    /// Global mem-arbitrator work mode.
    #[serde(rename = "tidb_mem_arbitrator_mode")]
    pub mem_arbitrator_mode: String,
    /// Global mem-arbitrator soft limit.
    #[serde(rename = "tidb_mem_arbitrator_soft_limit")]
    pub mem_arbitrator_soft_limit: String,
    /// GC trigger percentage of the server memory limit.
    #[serde(rename = "tidb_server_memory_limit_gc_trigger")]
    pub server_memory_limit_gc_trigger: String,
    /// Max memory of the instance plan cache.
    #[serde(rename = "tidb_instance_plan_cache_max_size")]
    pub instance_plan_cache_max_mem_size: String,
    /// Stats-cache mem quota.
    #[serde(rename = "tidb_stats_cache_mem_quota")]
    pub stats_cache_mem_quota: u64,
    /// Bind-cache mem quota.
    #[serde(rename = "tidb_mem_quota_binding_cache")]
    pub mem_quota_binding_cache: u64,
    /// Infoschema V2 cache size.
    #[serde(rename = "tidb_schema_cache_size")]
    pub schema_cache_size: String,
    /// Enable slow log (instance takes precedence).
    #[serde(rename = "tidb_enable_slow_log")]
    pub enable_slow_log: AtomicBool,
    /// Slow-log threshold.
    #[serde(rename = "tidb_slow_log_threshold")]
    pub slow_threshold: u64,
    /// Record plan in slow log.
    #[serde(rename = "tidb_record_plan_in_slow_log")]
    pub record_plan_in_slow_log: u32,
    /// Check mb4 value in utf8.
    #[serde(rename = "tidb_check_mb4_value_in_utf8")]
    pub check_mb4_value_in_utf8: AtomicBool,
    /// Force priority.
    #[serde(rename = "tidb_force_priority")]
    pub force_priority: String,
    /// Memory-usage alarm ratio.
    #[serde(rename = "tidb_memory_usage_alarm_ratio")]
    pub memory_usage_alarm_ratio: f64,
    /// Enable collect execution info.
    #[serde(rename = "tidb_enable_collect_execution_info")]
    pub enable_collect_execution_info: AtomicBool,
    /// Plugin directory.
    #[serde(rename = "plugin_dir")]
    pub plugin_dir: String,
    /// Plugins to load.
    #[serde(rename = "plugin_load")]
    pub plugin_load: String,
    /// Plugin audit-log buffer size (bytes; 0 disables buffering).
    #[serde(rename = "plugin_audit_log_buffer_size")]
    pub plugin_audit_log_buffer_size: i64,
    /// Plugin audit-log flush interval (seconds).
    #[serde(rename = "plugin_audit_log_flush_interval")]
    pub plugin_audit_log_flush_interval: i64,
    /// Max simultaneous client connections.
    #[serde(rename = "max_connections")]
    pub max_connections: u32,
    /// Enable DDL.
    #[serde(rename = "tidb_enable_ddl")]
    pub tidb_enable_ddl: AtomicBool,
    /// Enable stats owner.
    #[serde(rename = "tidb_enable_stats_owner")]
    pub tidb_enable_stats_owner: AtomicBool,
    /// RC read check TS.
    #[serde(rename = "tidb_rc_read_check_ts")]
    pub tidb_rc_read_check_ts: bool,
    /// Role for the distributed task framework.
    #[serde(rename = "tidb_service_scope")]
    pub tidb_service_scope: String,
}

impl Default for Instance {
    // From Go `defaultConf.Instance`.
    fn default() -> Self {
        Instance {
            tidb_general_log: false,
            enable_pprof_sql_cpu: false,
            ddl_slow_opr_threshold: 300,        // DefDDLSlowOprThreshold
            expensive_query_time_threshold: 60, // DefExpensiveQueryTimeThreshold
            expensive_txn_time_threshold: 600,  // DefExpensiveTxnTimeThreshold
            stmt_summary_enable_persistent: false,
            stmt_summary_filename: "tidb-statements.log".into(),
            stmt_summary_file_max_days: 3,
            stmt_summary_file_max_size: 64,
            stmt_summary_file_max_backups: 0,
            stmt_summary_max_stmt_count: 0,
            server_memory_limit: String::new(),
            mem_arbitrator_mode: String::new(),
            mem_arbitrator_soft_limit: String::new(),
            server_memory_limit_gc_trigger: String::new(),
            instance_plan_cache_max_mem_size: String::new(),
            stats_cache_mem_quota: 0,
            mem_quota_binding_cache: 0,
            schema_cache_size: String::new(),
            enable_slow_log: AtomicBool::new(DEFAULT_TIDB_ENABLE_SLOW_LOG),
            slow_threshold: DEFAULT_SLOW_THRESHOLD,
            record_plan_in_slow_log: DEFAULT_RECORD_PLAN_IN_SLOW_LOG,
            check_mb4_value_in_utf8: AtomicBool::new(true),
            force_priority: "NO_PRIORITY".into(),
            memory_usage_alarm_ratio: 0.8, // DefMemoryUsageAlarmRatio
            enable_collect_execution_info: AtomicBool::new(true),
            plugin_dir: "/data/deploy/plugin".into(),
            plugin_load: String::new(),
            plugin_audit_log_buffer_size: 0,
            plugin_audit_log_flush_interval: 30,
            max_connections: 0,
            tidb_enable_ddl: AtomicBool::new(true),
            tidb_enable_stats_owner: AtomicBool::new(true),
            tidb_rc_read_check_ts: false,
            tidb_service_scope: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_defaults() {
        let l = Log::default();
        assert_eq!(l.level, "info");
        assert_eq!(l.format, "text");
        assert_eq!(l.file.max_size, 300);
        assert_eq!(l.slow_query_file, "tidb-slow.log");
        assert_eq!(l.expensive_threshold, 10000);
        assert!(l.enable_slow_log.load());
        // Both stack options unset -> disable-error-stack true.
        assert!(l.get_disable_error_stack());
        // Both timestamp options unset -> disable-timestamp false.
        assert!(!l.get_disable_timestamp());
    }

    #[test]
    fn instance_defaults() {
        let i = Instance::default();
        assert_eq!(i.ddl_slow_opr_threshold, 300);
        assert_eq!(i.stmt_summary_filename, "tidb-statements.log");
        assert_eq!(i.stmt_summary_file_max_size, 64);
        assert!(i.check_mb4_value_in_utf8.load());
        assert_eq!(i.plugin_dir, "/data/deploy/plugin");
        assert_eq!(i.plugin_audit_log_flush_interval, 30);
        assert!(i.tidb_enable_ddl.load());
        assert_eq!(i.max_connections, 0);
        assert_eq!(i.memory_usage_alarm_ratio, 0.8);
    }

    // The nullableBool interplay from TestNullableBoolUnmarshal's Log part.
    #[test]
    fn log_nullable_bool_toml() {
        let l: Log = toml::from_str("enable-error-stack = true").unwrap();
        assert_eq!(l.enable_error_stack, super::super::marshal::NB_TRUE);
        let l: Log = toml::from_str(r#"enable-error-stack = """#).unwrap();
        assert_eq!(l.enable_error_stack, NB_UNSET);
        assert!(toml::from_str::<Log>("enable-error-stack = 1").is_err());
    }
}
