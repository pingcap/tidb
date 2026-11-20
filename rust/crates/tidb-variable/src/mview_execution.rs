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

//! Go master `variable.MViewExecutionSessionVars` (`variable.go`): the
//! session settings a materialized view's maintenance runs under.

use tidb_vardef::{defaults, tidb_vars};

use crate::error::VarError;
use crate::SessionVars;

/// Go master `MViewExecutionSessionVars`.
#[derive(Clone, Debug, PartialEq)]
pub struct MViewExecutionSessionVars {
    pub maintain_mem_quota: i64,
    pub isolation_read_engines: String,
    pub tiflash_max_threads: i64,
    pub tiflash_max_bytes_before_ext_join: i64,
    pub tiflash_max_bytes_before_ext_agg: i64,
    pub tiflash_max_bytes_before_ext_sort: i64,
    pub tiflash_mem_quota_query_per_node: i64,
    pub tiflash_query_spill_ratio: f64,
    pub fine_grained_stream_count: i64,
    pub fine_grained_batch_size: u64,
    pub import_threads: i64,
    pub import_disk_quota: String,
    pub track_aggregate_memory_usage: bool,
    pub enable_parallel_hashagg_spill: bool,
}

impl Default for MViewExecutionSessionVars {
    fn default() -> Self {
        Self {
            maintain_mem_quota: 0,
            isolation_read_engines: String::new(),
            tiflash_max_threads: 0,
            tiflash_max_bytes_before_ext_join: 0,
            tiflash_max_bytes_before_ext_agg: 0,
            tiflash_max_bytes_before_ext_sort: 0,
            tiflash_mem_quota_query_per_node: 0,
            tiflash_query_spill_ratio: 0.0,
            fine_grained_stream_count: 0,
            fine_grained_batch_size: 0,
            import_threads: 0,
            import_disk_quota: String::new(),
            track_aggregate_memory_usage: false,
            enable_parallel_hashagg_spill: false,
        }
    }
}

impl MViewExecutionSessionVars {
    /// Go master `CaptureMViewExecutionSessionVars`: the session's values,
    /// read from its system variables (`get` answers a variable's session
    /// value), each as Go's `SetSession` keeps it.
    #[must_use]
    pub fn capture(get: impl Fn(&str) -> Option<String>) -> Self {
        let int = |name: &str, default: i64| {
            get(name).map_or(default, |value| crate::varsutil::tidb_opt_int64(&value, default))
        };
        let on = |name: &str, default: bool| get(name).map_or(default, |value| tidb_vardef::option_values::tidb_opt_on(&value));
        Self {
            maintain_mem_quota: int(tidb_vars::TIDB_MVIEW_MAINTAIN_MEM_QUOTA, defaults::DEF_TIDB_MVIEW_MAINTAIN_MEM_QUOTA),
            isolation_read_engines: get(tidb_vars::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES).unwrap_or_default(),
            tiflash_max_threads: int(tidb_vars::TIDB_MAX_TIFLASH_THREADS, defaults::DEF_TIFLASH_MAX_THREADS),
            tiflash_max_bytes_before_ext_join: int(
                tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN,
                defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_JOIN,
            ),
            tiflash_max_bytes_before_ext_agg: int(
                tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY,
                defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_JOIN,
            ),
            tiflash_max_bytes_before_ext_sort: int(
                tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT,
                defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_JOIN,
            ),
            tiflash_mem_quota_query_per_node: int(
                tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE,
                defaults::DEF_TIFLASH_MEM_QUOTA_QUERY_PER_NODE,
            ),
            tiflash_query_spill_ratio: get(tidb_vars::TIFLASH_QUERY_SPILL_RATIO)
                .and_then(|value| value.parse().ok())
                .unwrap_or(defaults::DEF_TIFLASH_QUERY_SPILL_RATIO),
            fine_grained_stream_count: int(
                tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT,
                defaults::DEF_TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT,
            ),
            fine_grained_batch_size: u64::try_from(int(
                tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE,
                defaults::DEF_TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE,
            ))
            .unwrap_or_default(),
            import_threads: int(tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS, defaults::DEF_TIDB_MVIEW_MAINTAIN_IMPORT_THREADS),
            import_disk_quota: get(tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA)
                .unwrap_or_else(|| defaults::DEF_TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA.to_owned()),
            track_aggregate_memory_usage: on(
                tidb_vars::TIDB_TRACK_AGGREGATE_MEMORY_USAGE,
                defaults::DEF_TIDB_TRACK_AGGREGATE_MEMORY_USAGE,
            ),
            enable_parallel_hashagg_spill: on(
                tidb_vars::TIDB_ENABLE_PARALLEL_HASHAGG_SPILL,
                defaults::DEF_TIDB_ENABLE_PARALLEL_HASHAGG_SPILL,
            ),
        }
    }

    /// Go master `CaptureAppliedMViewExecutionSessionVars`: the values a
    /// session currently runs under, the maintenance memory quota and read
    /// engines being its own `tidb_mem_quota_query` and
    /// `tidb_isolation_read_engines`.
    #[must_use]
    pub fn capture_applied(get: impl Fn(&str) -> Option<String>) -> Self {
        Self::capture(|name| match name {
            tidb_vars::TIDB_MVIEW_MAINTAIN_MEM_QUOTA => get(tidb_vars::TIDB_MEM_QUOTA_QUERY),
            tidb_vars::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES => get(tidb_vars::TIDB_ISOLATION_READ_ENGINES),
            other => get(other),
        })
    }

    /// Go master `buildMViewExecutionSessionVarAssignments`: each variable
    /// these settings set, its value, and the message a failure to set it is
    /// annotated with -- the maintenance memory quota and read engines under
    /// the names the caller applies them as.
    #[must_use]
    pub fn assignments(
        &self,
        maintain_mem_quota_var_name: &'static str,
        maintain_isolation_read_engines_var_name: &'static str,
    ) -> Vec<MViewExecutionSessionVarAssignment> {
        let on_off = |value: bool| if value { "ON" } else { "OFF" }.to_owned();
        let assignment = |name: &'static str, value: String, failure_message: &'static str| MViewExecutionSessionVarAssignment {
            name,
            value,
            failure_message,
        };
        vec![
            assignment(maintain_mem_quota_var_name, self.maintain_mem_quota.to_string(), "mv execution: failed to apply maintain mem quota"),
            assignment(
                maintain_isolation_read_engines_var_name,
                self.isolation_read_engines.clone(),
                "mv execution: failed to apply tidb_isolation_read_engines",
            ),
            assignment(
                tidb_vars::TIDB_MAX_TIFLASH_THREADS,
                self.tiflash_max_threads.to_string(),
                "mv execution: failed to apply tidb_max_tiflash_threads",
            ),
            assignment(
                tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN,
                self.tiflash_max_bytes_before_ext_join.to_string(),
                "mv execution: failed to apply tidb_max_bytes_before_tiflash_external_join",
            ),
            assignment(
                tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY,
                self.tiflash_max_bytes_before_ext_agg.to_string(),
                "mv execution: failed to apply tidb_max_bytes_before_tiflash_external_group_by",
            ),
            assignment(
                tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT,
                self.tiflash_max_bytes_before_ext_sort.to_string(),
                "mv execution: failed to apply tidb_max_bytes_before_tiflash_external_sort",
            ),
            assignment(
                tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE,
                self.tiflash_mem_quota_query_per_node.to_string(),
                "mv execution: failed to apply tiflash_mem_quota_query_per_node",
            ),
            assignment(
                tidb_vars::TIFLASH_QUERY_SPILL_RATIO,
                tidb_goruntime::go_strconv::format_float(self.tiflash_query_spill_ratio, b'f', -1, 64),
                "mv execution: failed to apply tiflash_query_spill_ratio",
            ),
            assignment(
                tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT,
                self.fine_grained_stream_count.to_string(),
                "mv execution: failed to apply tiflash_fine_grained_shuffle_stream_count",
            ),
            assignment(
                tidb_vars::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE,
                self.fine_grained_batch_size.to_string(),
                "mv execution: failed to apply tiflash_fine_grained_shuffle_batch_size",
            ),
            assignment(
                tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS,
                self.import_threads.to_string(),
                "mv execution: failed to apply tidb_mview_maintain_import_threads",
            ),
            assignment(
                tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA,
                self.import_disk_quota.clone(),
                "mv execution: failed to apply tidb_mview_maintain_import_disk_quota",
            ),
            assignment(
                tidb_vars::TIDB_TRACK_AGGREGATE_MEMORY_USAGE,
                on_off(self.track_aggregate_memory_usage),
                "mv execution: failed to apply tidb_track_aggregate_memory_usage",
            ),
            assignment(
                tidb_vars::TIDB_ENABLE_PARALLEL_HASHAGG_SPILL,
                on_off(self.enable_parallel_hashagg_spill),
                "mv execution: failed to apply tidb_enable_parallel_hashagg_spill",
            ),
        ]
    }

    /// Go master `AddMViewExecutionSessionVarsToJob`'s entries, in Go's order:
    /// the job's `SessionVars` that carry these settings.
    #[must_use]
    pub fn job_system_vars(&self) -> Vec<(&'static str, String)> {
        self.assignments(tidb_vars::TIDB_MVIEW_MAINTAIN_MEM_QUOTA, tidb_vars::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES)
            .into_iter()
            .map(|assignment| (assignment.name, assignment.value))
            .collect()
    }
}

/// Go master `mViewExecutionSessionVarAssignment`.
#[derive(Clone, Debug, PartialEq)]
pub struct MViewExecutionSessionVarAssignment {
    pub name: &'static str,
    pub value: String,
    pub failure_message: &'static str,
}

/// Go master `CaptureMViewExecutionSessionVars`.
#[must_use]
pub fn capture_m_view_execution_session_vars(vars: &SessionVars) -> MViewExecutionSessionVars {
    MViewExecutionSessionVars::capture(|name| vars.get_system(name).ok())
}

/// Go master `CaptureAppliedMViewExecutionSessionVars`.
#[must_use]
pub fn capture_applied_m_view_execution_session_vars(vars: &SessionVars) -> MViewExecutionSessionVars {
    MViewExecutionSessionVars::capture_applied(|name| vars.get_system(name).ok())
}

/// Go master `MViewExecutionSessionVarsApplyConfig`.
#[derive(Clone, Copy, Default)]
pub struct MViewExecutionSessionVarsApplyConfig {
    pub maintain_mem_quota_var_name: &'static str,
    pub maintain_isolation_read_engines_var_name: &'static str,
    pub capture_applied_vars: Option<fn(&SessionVars) -> MViewExecutionSessionVars>,
    pub best_effort: bool,
    pub inject_apply_error: Option<fn(&str) -> Option<VarError>>,
    pub on_apply_error: Option<fn(&str, &str, &VarError)>,
    pub on_restore_error: Option<fn(&str, &str, &str, &VarError)>,
}

/// The restore closure Go master `ApplyMViewExecutionSessionVarsWithConfig`
/// answers.
#[must_use = "the session keeps the applied settings until it is restored"]
pub struct MViewExecutionSessionVarsRestore {
    origin: MViewExecutionSessionVars,
    applied: MViewExecutionSessionVars,
    maintain_mem_quota_var_name: &'static str,
    maintain_isolation_read_engines_var_name: &'static str,
    on_restore_error: Option<fn(&str, &str, &str, &VarError)>,
}

impl MViewExecutionSessionVarsRestore {
    /// Runs the restore closure.
    pub fn restore(self, vars: &mut SessionVars) {
        restore_m_view_execution_session_vars(
            vars,
            &self.origin,
            &self.applied,
            self.maintain_mem_quota_var_name,
            self.maintain_isolation_read_engines_var_name,
            self.on_restore_error,
        );
    }
}

/// Go master `ApplyMViewExecutionSessionVarsWithConfig`: `target` applied
/// onto the session, answered with the restore closure. In best-effort mode
/// a variable that fails to apply keeps the session's value.
///
/// # Errors
///
/// A variable failed to apply (not best effort); the session is restored.
pub fn apply_m_view_execution_session_vars_with_config(
    vars: &mut SessionVars,
    target: &MViewExecutionSessionVars,
    cfg: MViewExecutionSessionVarsApplyConfig,
) -> Result<MViewExecutionSessionVarsRestore, VarError> {
    let capture_applied = cfg.capture_applied_vars.unwrap_or(capture_m_view_execution_session_vars);
    let maintain_mem_quota_var_name = if cfg.maintain_mem_quota_var_name.is_empty() {
        tidb_vars::TIDB_MVIEW_MAINTAIN_MEM_QUOTA
    } else {
        cfg.maintain_mem_quota_var_name
    };
    let maintain_isolation_read_engines_var_name = if cfg.maintain_isolation_read_engines_var_name.is_empty() {
        tidb_vars::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES
    } else {
        cfg.maintain_isolation_read_engines_var_name
    };

    // "Always call SetSystemVar to synchronize the session-variable cache
    // used by EncodeStates, even when the fields already match."
    let origin = capture_applied(vars);
    for assignment in target.assignments(maintain_mem_quota_var_name, maintain_isolation_read_engines_var_name) {
        let mut error = cfg.inject_apply_error.and_then(|inject| inject(assignment.name));
        if error.is_none() {
            error = vars.set_system_var(assignment.name, assignment.value.clone()).err();
        }
        let Some(error) = error else { continue };
        if !cfg.best_effort {
            let current = capture_applied(vars);
            restore_m_view_execution_session_vars(
                vars,
                &origin,
                &current,
                maintain_mem_quota_var_name,
                maintain_isolation_read_engines_var_name,
                cfg.on_restore_error,
            );
            return Err(error.annotate(assignment.failure_message));
        }
        if let Some(on_apply_error) = cfg.on_apply_error {
            on_apply_error(assignment.name, &assignment.value, &error);
        }
    }

    let applied = capture_applied(vars);
    Ok(MViewExecutionSessionVarsRestore {
        origin,
        applied,
        maintain_mem_quota_var_name,
        maintain_isolation_read_engines_var_name,
        on_restore_error: cfg.on_restore_error,
    })
}

/// Go master `restoreMViewExecutionSessionVars`.
fn restore_m_view_execution_session_vars(
    vars: &mut SessionVars,
    origin: &MViewExecutionSessionVars,
    current: &MViewExecutionSessionVars,
    maintain_mem_quota_var_name: &'static str,
    maintain_isolation_read_engines_var_name: &'static str,
    on_restore_error: Option<fn(&str, &str, &str, &VarError)>,
) {
    let origin_assignments = origin.assignments(maintain_mem_quota_var_name, maintain_isolation_read_engines_var_name);
    let current_assignments = current.assignments(maintain_mem_quota_var_name, maintain_isolation_read_engines_var_name);
    for (assignment, current) in origin_assignments.iter().zip(&current_assignments) {
        if let Err(error) = vars.set_system_var(assignment.name, assignment.value.clone()) {
            if let Some(on_restore_error) = on_restore_error {
                on_restore_error(assignment.name, &assignment.value, &current.value, &error);
            }
        }
    }
}
