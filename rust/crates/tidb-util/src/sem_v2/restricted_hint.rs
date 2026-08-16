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

//! Go `restricted_hint.go`: the optimizer hints SEM strips from statements.

use super::{load_global_sem, SemImpl};

/// Go `hintGuardVars`: an optimizer hint that overrides a system variable, and
/// that variable. Such a hint is restricted only while its variable is hidden
/// or read-only under SEM, so a hint that mirrors a still-tunable variable
/// stays available. Other restricted hints are stripped unconditionally.
///
/// The variable names are Go's `vardef.TiDBMemQuotaQuery`,
/// `vardef.TiDBReplicaRead`, and `vardef.MaxExecutionTime`, inlined.
pub const HINT_GUARD_VARS: &[(&str, &str)] = &[
    ("memory_quota", "tidb_mem_quota_query"),
    ("read_consistent_replica", "tidb_replica_read"),
    ("max_execution_time", "max_execution_time"),
];

fn hint_guard_var(hint_name_lower: &str) -> Option<&'static str> {
    HINT_GUARD_VARS
        .iter()
        .find(|(hint, _)| *hint == hint_name_lower)
        .map(|(_, var)| *var)
}

/// Go `IsRestrictedHint`: an error when the optimizer hint is restricted by the
/// SEM configuration. A restricted hint is stripped from the statement with a
/// warning rather than rejected outright. `hint_name_lower` is the lower-case
/// hint name.
///
/// # Errors
///
/// Returns Go's user-facing message when the hint is restricted.
pub fn is_restricted_hint(hint_name_lower: &str) -> Result<(), String> {
    match load_global_sem() {
        None => Ok(()),
        Some(sem) => is_restricted_hint_impl(&sem, hint_name_lower),
    }
}

/// Go `semImpl.isRestrictedHint`.
pub(super) fn is_restricted_hint_impl(sem: &SemImpl, hint_name_lower: &str) -> Result<(), String> {
    if !sem.restricted_hints.contains(hint_name_lower) {
        return Ok(());
    }
    // A variable-overriding hint is only restricted while its variable is, so a
    // hint mirroring a still-tunable variable stays available.
    if let Some(var) = hint_guard_var(hint_name_lower) {
        if !sem.is_invisible_sys_var(var) && !sem.is_read_only_variable(var) {
            return Ok(());
        }
    }
    Err(format!(
        "the {}() optimizer hint is restricted under the current security policy and is ignored",
        hint_name_lower.to_uppercase()
    ))
}
