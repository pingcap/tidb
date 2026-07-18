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

//! Removed-system-variable compatibility policy from TiDB's session layer.
//!
//! TiDB keeps removed variables addressable so clients can receive a specific
//! unsupported-variable error instead of a fabricated value. This leaf owns
//! only the exact name/reason registry and lookup semantics. Error-code
//! construction, parser normalization, SET/SELECT dispatch, and the live
//! system-variable registry remain outside this policy owner.

/// A removed system variable and the source migration reason shown to users.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemovedSystemVariable {
    /// The exact source lookup spelling.
    pub name: &'static str,
    /// The source replacement/removal reason.
    pub reason: &'static str,
}

/// The complete source `removedSysVars` registry.
pub const REMOVED_SYSTEM_VARIABLES: &[RemovedSystemVariable] = &[
    RemovedSystemVariable {
        name: "tidb_enable_alter_placement",
        reason: "alter placement is now always enabled",
    },
    RemovedSystemVariable {
        name: "tidb_enable_global_temporary_table",
        reason: "temporary table support is now always enabled",
    },
    RemovedSystemVariable {
        name: "tidb_slow_log_masking",
        reason: "use tidb_redact_log instead",
    },
    RemovedSystemVariable {
        name: "placement_checks",
        reason: "placement_checks is removed and use tidb_placement_mode instead",
    },
    RemovedSystemVariable {
        name: "tidb_mem_quota_hashjoin",
        reason: "use tidb_mem_quota_query instead",
    },
    RemovedSystemVariable {
        name: "tidb_mem_quota_mergejoin",
        reason: "use tidb_mem_quota_query instead",
    },
    RemovedSystemVariable {
        name: "tidb_mem_quota_sort",
        reason: "use tidb_mem_quota_query instead",
    },
    RemovedSystemVariable {
        name: "tidb_mem_quota_topn",
        reason: "use tidb_mem_quota_query instead",
    },
    RemovedSystemVariable {
        name: "tidb_mem_quota_indexlookupreader",
        reason: "use tidb_mem_quota_query instead",
    },
    RemovedSystemVariable {
        name: "tidb_mem_quota_indexlookupjoin",
        reason: "use tidb_mem_quota_query instead",
    },
    RemovedSystemVariable {
        name: "tidb_enable_streaming",
        reason: "streaming is no longer supported",
    },
    RemovedSystemVariable {
        name: "tidb_opt_broadcast_join",
        reason: "tidb_opt_broadcast_join is removed and use tidb_allow_mpp instead",
    },
    RemovedSystemVariable {
        name: "tidb_enable_change_multi_schema",
        reason: "alter multiple schema objects in a table is now always enabled",
    },
];

/// Returns the source removal reason for an exact variable name.
#[must_use]
pub fn removal_reason(name: &str) -> Option<&'static str> {
    REMOVED_SYSTEM_VARIABLES
        .iter()
        .find(|variable| variable.name == name)
        .map(|variable| variable.reason)
}

/// Returns whether the exact variable name is removed.
#[must_use]
pub fn is_removed(name: &str) -> bool {
    removal_reason(name).is_some()
}
