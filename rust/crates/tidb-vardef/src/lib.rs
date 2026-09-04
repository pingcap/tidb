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

//! Constants and mode enums from `pkg/sessionctx/vardef/tidb_vars.go`.
//!
//! [`tidb_vars`] holds the system-variable **name** constants -- the string
//! identifiers used to reference session/global system variables throughout
//! parse -> plan -> execute. [`defaults`] holds the `Def*` **default-value**
//! constants for those variables. [`modes`] holds the small
//! `ClusteredIndexDefMode` / `ExchangeCompressionMode` enums and their helpers.
//! [`is_mdl_enabled`] and [`set_enable_mdl`] retain the source's exceptional
//! runtime MDL switch: NextGen always reports enabled even if the mutable
//! classic-kernel backing value is false.
//!
//! SCOPE (documented, not yet the whole `vardef` package): the name constants
//! (521), the `Def*` defaults (395), and the mode enums are ported; constants
//! are script-extracted and byte-verified against the Go source. `ScopeFlag`
//! and the sysvar `TypeFlag` already live in `tidb-exec`
//! (`sysvar_scope`/`sysvar_type`). Still DEFERRED from the full package: the
//! remainder of the mutable `var (...)` block of runtime-tunable global sysvar
//! backing stores, apart from the two ANALYZE defaults and plan-replayer
//! retention setting above (many need
//! config/system-memory-derived initializers,
//! `rate.Limiter`, or typed pointers, and are runtime state better wired when
//! the session layer consumes them, not on the simple-query path),
//! `sysvar.go`'s `SysVar` struct together with the `GetSysVar`/`SetSysVar`
//! global registry (the singleton the rewrite deliberately replaces with
//! explicit wiring), and `runtime.go`.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicU8, Ordering};

static ENABLE_MDL: AtomicBool = AtomicBool::new(false);

/// Go `vardef.AnalyzeDefaultNumBuckets`.
pub static ANALYZE_DEFAULT_NUM_BUCKETS: AtomicU64 =
    AtomicU64::new(defaults::DEF_TIDB_ANALYZE_DEFAULT_NUM_BUCKETS as u64);

/// Go `vardef.AnalyzeDefaultNumTopN`.
pub static ANALYZE_DEFAULT_NUM_TOP_N: AtomicU64 =
    AtomicU64::new(defaults::DEF_TIDB_ANALYZE_DEFAULT_NUM_TOP_N as u64);

/// Go `vardef.StatsCacheMemQuota`.
pub static STATS_CACHE_MEM_QUOTA: AtomicI64 =
    AtomicI64::new(defaults::DEF_TIDB_STATS_CACHE_MEM_QUOTA);

/// Go `vardef.MemoryUsageAlarmRatio`, stored as the IEEE-754 bit pattern.
pub static MEMORY_USAGE_ALARM_RATIO: AtomicU64 =
    AtomicU64::new(defaults::DEF_MEMORY_USAGE_ALARM_RATIO.to_bits());

/// Go `vardef.MemoryUsageAlarmKeepRecordNum`.
pub static MEMORY_USAGE_ALARM_KEEP_RECORD_NUM: AtomicI64 =
    AtomicI64::new(defaults::DEF_MEMORY_USAGE_ALARM_KEEP_RECORD_NUM);

/// Go `vardef`'s process-wide plan-replayer file retention duration.
///
/// The Go API uses `time.Duration`, whose underlying representation is a
/// signed number of nanoseconds. Keeping the same representation here lets
/// callers preserve the source setter/getter contract without silently
/// clamping or converting values at this package boundary.
pub static PLAN_REPLAYER_FILE_RETENTION_TIME: AtomicI64 =
    AtomicI64::new(defaults::DEF_TIDB_PLAN_REPLAYER_FILE_RETENTION_TIME);

/// Go `vardef.EnableTTLJob`, the process-wide switch used by the TTL worker.
pub static ENABLE_TTL_JOB: AtomicBool = AtomicBool::new(defaults::DEF_TIDB_TTL_JOB_ENABLE);

/// Go `vardef.SchemaCacheSize`, the process-wide byte count used by the
/// infoschema cache after a GLOBAL/INSTANCE update.
pub static SCHEMA_CACHE_SIZE: AtomicU64 =
    AtomicU64::new(defaults::DEF_TIDB_SCHEMA_CACHE_SIZE as u64);

/// Go `vardef.CircuitBreakerPDMetadataErrorRateThresholdRatio`, stored as
/// the IEEE-754 bit pattern for lock-free process-wide reads by the PD
/// circuit breaker.
pub static CIRCUIT_BREAKER_PD_METADATA_ERROR_RATE_THRESHOLD_RATIO: AtomicU64 =
    AtomicU64::new(defaults::DEF_TIDB_CIRCUIT_BREAKER_PD_META_ERROR_RATE_RATIO.to_bits());

/// Go `vardef.RunAutoAnalyze`, the process-wide auto-analyze enable switch.
pub static RUN_AUTO_ANALYZE: AtomicBool =
    AtomicBool::new(defaults::DEF_TIDB_ENABLE_AUTO_ANALYZE);

/// Go `vardef.EnableAutoAnalyzePriorityQueue`, the process-wide scheduler
/// mode switch consulted by auto-analyze concurrency validation.
pub static ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE: AtomicBool =
    AtomicBool::new(defaults::DEF_TIDB_ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE);

/// Go `vardef.AutoAnalyzeConcurrency`, the process-wide concurrency value.
pub static AUTO_ANALYZE_CONCURRENCY: AtomicI64 =
    AtomicI64::new(defaults::DEF_TIDB_AUTO_ANALYZE_CONCURRENCY);

/// Go `vardef.EnableResourceControl`. The classic server initializes this
/// process switch only when its global-variable cache is rebuilt, so the
/// zero-value is intentionally disabled even though the SQL default is ON.
pub static ENABLE_RESOURCE_CONTROL: AtomicBool = AtomicBool::new(false);

/// Go `vardef.EnableResourceControlStrictMode`.
pub static ENABLE_RESOURCE_CONTROL_STRICT_MODE: AtomicBool =
    AtomicBool::new(defaults::DEF_TIDB_RESOURCE_CONTROL_STRICT_MODE);

const OOM_ACTION_CANCEL: u8 = 0;
const OOM_ACTION_LOG: u8 = 1;

/// Go `vardef.OOMAction`'s typed process-wide value.
static OOM_ACTION: AtomicU8 = AtomicU8::new(OOM_ACTION_CANCEL);

/// Loads Go `vardef.MemoryUsageAlarmRatio`.
#[must_use]
pub fn memory_usage_alarm_ratio() -> f64 {
    f64::from_bits(MEMORY_USAGE_ALARM_RATIO.load(Ordering::SeqCst))
}

/// Stores Go `vardef.MemoryUsageAlarmRatio`.
pub fn set_memory_usage_alarm_ratio(value: f64) {
    MEMORY_USAGE_ALARM_RATIO.store(value.to_bits(), Ordering::SeqCst);
}

/// Loads Go's PD metadata circuit-breaker error-rate threshold ratio.
#[must_use]
pub fn circuit_breaker_pd_metadata_error_rate_threshold_ratio() -> f64 {
    f64::from_bits(
        CIRCUIT_BREAKER_PD_METADATA_ERROR_RATE_THRESHOLD_RATIO.load(Ordering::SeqCst),
    )
}

/// Stores Go's PD metadata circuit-breaker error-rate threshold ratio after
/// sysvar validation.
pub fn set_circuit_breaker_pd_metadata_error_rate_threshold_ratio(value: f64) {
    CIRCUIT_BREAKER_PD_METADATA_ERROR_RATE_THRESHOLD_RATIO.store(value.to_bits(), Ordering::SeqCst);
}

/// Loads Go `vardef.GetPlanReplayerFileRetentionTime` as nanoseconds.
#[must_use]
pub fn plan_replayer_file_retention_time() -> i64 {
    PLAN_REPLAYER_FILE_RETENTION_TIME.load(Ordering::SeqCst)
}

/// Stores Go `vardef.SetPlanReplayerFileRetentionTime` as nanoseconds.
pub fn set_plan_replayer_file_retention_time(nanoseconds: i64) {
    PLAN_REPLAYER_FILE_RETENTION_TIME.store(nanoseconds, Ordering::SeqCst);
}

/// Loads Go `vardef.OOMAction`.
#[must_use]
pub fn oom_action() -> &'static str {
    if OOM_ACTION.load(Ordering::SeqCst) == OOM_ACTION_LOG {
        tidb_vars::OOM_ACTION_LOG
    } else {
        tidb_vars::OOM_ACTION_CANCEL
    }
}

/// Stores Go `vardef.OOMAction` after sysvar validation.
pub fn set_oom_action(value: &str) {
    OOM_ACTION.store(
        if value.eq_ignore_ascii_case(tidb_vars::OOM_ACTION_LOG) {
            OOM_ACTION_LOG
        } else {
            OOM_ACTION_CANCEL
        },
        Ordering::SeqCst,
    );
}

/// Go `IsMDLEnabled` with the process-global kernel selection made explicit.
///
/// NextGen cannot disable metadata locking; classic mode reads the mutable
/// value changed by [`set_enable_mdl`].
#[must_use]
pub fn is_mdl_enabled(next_gen: bool) -> bool {
    next_gen || ENABLE_MDL.load(Ordering::SeqCst)
}

/// Go `SetEnableMDL`: changes the classic-kernel MDL backing value.
pub fn set_enable_mdl(enabled: bool) {
    ENABLE_MDL.store(enabled, Ordering::SeqCst);
}

/// Go `IsReadOnlyVarInNextGen`: checks whether a system variable name is
/// read-only in the next-generation kernel.
#[must_use]
pub fn is_read_only_var_in_next_gen(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        tidb_vars::TIDB_ENABLE_MDL
            | tidb_vars::TIDB_MAX_DIST_TASK_NODES
            | tidb_vars::TIDB_DDL_REORG_MAX_WRITE_SPEED
            | tidb_vars::TIDB_DDL_DISK_QUOTA
            | tidb_vars::TIDB_ENABLE_DIST_TASK
            | tidb_vars::TIDB_DDL_ENABLE_FAST_REORG
    )
}

pub mod bounds;
pub mod defaults;
/// One function from `sessionctx/variable/sysvar.go` rather than from
/// `vardef`: `GlobalSystemVariableInitialValue`, which decides the value a
/// `Def*` constant above actually takes on a real install. It lives here
/// because both tiers that need it -- the bootstrap writer above this crate
/// and `SET <var> = DEFAULT` in `tidb-session` -- can only share it from a
/// leaf, and because it is pure policy over those same constants.
pub mod global_sysvar_initial;
pub mod modes;
#[cfg(test)]
mod tests_sysvar_port;
#[cfg(test)]
mod tests_vardef_port;
#[cfg(test)]
mod tests_variable_p2_port;
pub mod tidb_vars;
