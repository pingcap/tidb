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

//! Native implementation of the next-generation read-only-variable predicate
//! from Go `pkg/sessionctx/vardef/runtime.go`.

/// Variable names treated as read-only by the next-generation kernel.
pub const NEXTGEN_READ_ONLY_VARIABLES: &[&str] = &[
    "tidb_enable_metadata_lock",
    "tidb_max_dist_task_nodes",
    "tidb_ddl_reorg_max_write_speed",
    "tidb_ddl_disk_quota",
    "tidb_enable_dist_task",
    "tidb_ddl_enable_fast_reorg",
];

/// Returns whether `name` is read-only in the next-generation kernel.
#[must_use]
pub fn is_read_only_var_in_nextgen(name: &str) -> bool {
    let name = name.to_lowercase();
    NEXTGEN_READ_ONLY_VARIABLES.contains(&name.as_str())
}
