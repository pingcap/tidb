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

//! The flat, name-ordered system-variable registry, merged from the
//! per-subject catalog slices.
//!
//! Go keeps all 948 entries in one `sysVars` map in
//! `pkg/sessionctx/variable/sysvar.go`. Here they are grouped by SUBJECT --
//! what each variable controls -- so a reader looking for, say, the optimizer
//! switches opens `optimizer.rs` rather than an alphabetical shard.
//!
//! Lookup binary-searches the assembled slice, so the assembly must be
//! name-ordered. Each subject file keeps its own entries sorted and this
//! module MERGES them at const-eval time: sortedness is a property of the
//! construction, not a convention anyone has to remember. The registry's own
//! `the_registry_is_complete_and_sorted` test proves it.

use super::SysVarDef;

mod concurrency;
mod connections;
mod ddl_schema;
mod distsql_storage;
mod gc;
mod innodb;
mod logging;
mod memory_limits;
mod mysql_compat_inert;
mod observability;
mod optimizer;
mod replication;
mod security;
mod server_identity;
mod sql_behavior;
mod statistics;
mod transactions;
mod ttl;
mod types_and_expressions;

/// The subject slices, each independently name-ordered.
const SLICES: &[&[SysVarDef]] = &[
    &concurrency::ENTRIES,
    &connections::ENTRIES,
    &ddl_schema::ENTRIES,
    &distsql_storage::ENTRIES,
    &gc::ENTRIES,
    &innodb::ENTRIES,
    &logging::ENTRIES,
    &memory_limits::ENTRIES,
    &mysql_compat_inert::ENTRIES,
    &observability::ENTRIES,
    &optimizer::ENTRIES,
    &replication::ENTRIES,
    &security::ENTRIES,
    &server_identity::ENTRIES,
    &sql_behavior::ENTRIES,
    &statistics::ENTRIES,
    &transactions::ENTRIES,
    &ttl::ENTRIES,
    &types_and_expressions::ENTRIES,
];

/// Total entry count across every subject slice.
const TOTAL: usize = concurrency::ENTRIES.len()
    + connections::ENTRIES.len()
    + ddl_schema::ENTRIES.len()
    + distsql_storage::ENTRIES.len()
    + gc::ENTRIES.len()
    + innodb::ENTRIES.len()
    + logging::ENTRIES.len()
    + memory_limits::ENTRIES.len()
    + mysql_compat_inert::ENTRIES.len()
    + observability::ENTRIES.len()
    + optimizer::ENTRIES.len()
    + replication::ENTRIES.len()
    + security::ENTRIES.len()
    + server_identity::ENTRIES.len()
    + sql_behavior::ENTRIES.len()
    + statistics::ENTRIES.len()
    + transactions::ENTRIES.len()
    + ttl::ENTRIES.len()
    + types_and_expressions::ENTRIES.len();

/// `true` when `a` sorts before `b`, comparing as bytes -- `str::cmp` is not
/// available in a `const fn`.
const fn name_lt(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut i = 0;
    while i < a.len() && i < b.len() {
        if a[i] != b[i] {
            return a[i] < b[i];
        }
        i += 1;
    }
    a.len() < b.len()
}

/// Merges the sorted subject slices into one sorted array.
///
/// A k-way merge rather than a sort: every input is already ordered, so this
/// is O(entries x subjects) byte comparisons at compile time and cannot
/// produce an out-of-order result from in-order inputs.
const fn merged() -> [SysVarDef; TOTAL] {
    let mut out = [SysVarDef::PLACEHOLDER; TOTAL];
    // How far each subject slice has been consumed.
    let mut heads = [0usize; SLICES.len()];
    let mut written = 0;
    while written < TOTAL {
        let mut pick = usize::MAX;
        let mut group = 0;
        while group < SLICES.len() {
            if heads[group] < SLICES[group].len()
                && (pick == usize::MAX
                    || name_lt(
                        SLICES[group][heads[group]].name,
                        SLICES[pick][heads[pick]].name,
                    ))
            {
                pick = group;
            }
            group += 1;
        }
        out[written] = SLICES[pick][heads[pick]];
        heads[pick] += 1;
        written += 1;
    }
    out
}

/// Every system variable this build knows, name-ordered for binary search.
pub static SYS_VARS: &[SysVarDef] = {
    static MERGED: [SysVarDef; TOTAL] = merged();
    &MERGED
};
