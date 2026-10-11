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
//! Go keeps its base entries plus dynamically registered variables in one `sysVars` map in
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
mod recent;
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
    &recent::ENTRIES,
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
    + recent::ENTRIES.len()
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

#[cfg(test)]
mod registry_tests {
    use super::super::VarType;
    use super::*;

    /// The assembled registry stays name-ordered so binary search remains
    /// valid, and every sysvar Go's registry carries that this port carries
    /// too resolves here — including later additions for columnar storage
    /// and the query cop store limit.
    #[test]
    fn registry_is_name_ordered_and_carries_the_late_additions() {
        let mut sorted_names: Vec<&str> = SYS_VARS.iter().map(|v| v.name).collect();
        sorted_names.sort_unstable();
        let names: Vec<&str> = SYS_VARS.iter().map(|v| v.name).collect();
        assert_eq!(names, sorted_names, "registry must stay name-ordered");

        let columnar = SYS_VARS
            .iter()
            .find(|v| v.name == "tidb_columnar_storage_enabled");
        let columnar = columnar.expect("tidb_columnar_storage_enabled in registry");
        assert_eq!(columnar.scope, 1_u8);
        assert_eq!(columnar.value, "ON");
        assert!(matches!(columnar.var_type, VarType::Bool));

        let cop = SYS_VARS
            .iter()
            .find(|v| v.name == "tidb_query_cop_store_limit");
        let cop = cop.expect("tidb_query_cop_store_limit in registry");
        assert_eq!(cop.scope, 3_u8);
        assert_eq!(cop.value, "15");
        assert!(matches!(cop.var_type, VarType::Unsigned));
        assert_eq!(cop.min_value, 0_i64);
        assert_eq!(cop.max_value, 256_u64);
    }

    /// The registry is this branch's Go registry, read from its source:
    /// every `Name:` of `defaultSysVars` and `noopSysVars`
    /// (`pkg/sessionctx/variable/sysvar.go`, `noop.go`), every
    /// `newExecConcurrencySysVar`, and the workload repository's
    /// `RegisterSysVar`s (`pkg/util/workloadrepo/worker.go`, which the server
    /// imports), with the `vardef` constants resolved. A variable Go-master
    /// adds but this branch does not register answers 1193 in Go, so serving
    /// it here is a divergence.
    #[test]
    fn registry_is_this_branchs_go_registry() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let read = |path: &str| {
            std::fs::read_to_string(root.join(path))
                .unwrap_or_else(|error| panic!("read {path}: {error}"))
        };
        // `Ident = "text"` and `Ident string = "text"` constant lines.
        let constants = |source: &str| {
            let mut constants = std::collections::HashMap::new();
            for line in source.lines() {
                let Some((left, right)) = line.split_once('=') else {
                    continue;
                };
                let right = right.trim();
                let Some(text) = right
                    .strip_prefix('"')
                    .and_then(|rest| rest.split('"').next())
                else {
                    continue;
                };
                let mut words = left.split_whitespace();
                let Some(ident) = words.next() else {
                    continue;
                };
                if words.next().is_some_and(|word| word != "string") || words.next().is_some() {
                    continue;
                }
                if ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                    constants.insert(ident.to_owned(), text.to_owned());
                }
            }
            constants
        };
        let vardef_dir = root.join("pkg/sessionctx/vardef");
        let mut vardef = std::collections::HashMap::new();
        for entry in std::fs::read_dir(&vardef_dir).expect("read pkg/sessionctx/vardef") {
            let path = entry.expect("vardef entry").path();
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            if name.ends_with(".go") && !name.ends_with("_test.go") {
                vardef.extend(constants(
                    &std::fs::read_to_string(&path).expect("read vardef"),
                ));
            }
        }
        // The token after `marker`: `vardef.X`, a quoted literal, or (for the
        // workload repository) a package-local constant.
        let names_after =
            |source: &str, marker: &str, local: &std::collections::HashMap<String, String>| {
                let mut names = Vec::new();
                for (index, _) in source.match_indices(marker) {
                    // A word boundary: `GlobalConfigName:` is not `Name:`.
                    if source[..index]
                        .chars()
                        .next_back()
                        .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
                    {
                        continue;
                    }
                    let rest = source[index + marker.len()..].trim_start();
                    let token: String = rest
                        .chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '"'))
                        .collect();
                    let name = if let Some(constant) = token.strip_prefix("vardef.") {
                        vardef.get(constant).cloned()
                    } else if let Some(literal) = token.strip_prefix('"') {
                        literal.strip_suffix('"').map(str::to_owned)
                    } else {
                        local.get(&token).cloned()
                    };
                    names.extend(name);
                }
                names
            };
        let none = std::collections::HashMap::new();
        let mut go = std::collections::BTreeSet::new();
        for path in [
            "pkg/sessionctx/variable/sysvar.go",
            "pkg/sessionctx/variable/noop.go",
        ] {
            let source = read(path);
            go.extend(names_after(&source, "Name:", &none));
            go.extend(names_after(&source, "newExecConcurrencySysVar(", &none));
        }
        // Only the `Name:` of each `RegisterSysVar(&variable.SysVar{...})`:
        // the file declares other structs with a `Name` field too.
        let workloadrepo = read("pkg/util/workloadrepo/worker.go");
        let local = constants(&workloadrepo);
        for (index, _) in workloadrepo.match_indices("RegisterSysVar(") {
            let call = &workloadrepo[index..];
            let call = &call[..call.find("Name:").expect("a registered sysvar has a Name") + 5];
            let tail = &workloadrepo[index + call.len() - 5..];
            go.extend(names_after(
                &tail[..tail.find('\n').unwrap_or(tail.len())],
                "Name:",
                &local,
            ));
        }

        let rust: std::collections::BTreeSet<String> =
            SYS_VARS.iter().map(|def| def.name.to_owned()).collect();
        let rust_only: Vec<_> = rust.difference(&go).collect();
        let go_only: Vec<_> = go.difference(&rust).collect();
        assert!(
            rust_only.is_empty() && go_only.is_empty(),
            "registry differs from this branch's Go: rust-only {rust_only:?}, go-only {go_only:?}"
        );
    }
}
