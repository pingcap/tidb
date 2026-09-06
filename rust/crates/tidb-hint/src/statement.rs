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

use std::collections::HashMap;
use std::sync::RwLock;

use tidb_ast::{CiString, Hint, HintKind, IndexType};
use tidb_datatype::UNSPECIFIED_LENGTH;
use tidb_model::{GoShared, GoSharedPointerSlice, IndexColumn, IndexInfo, SchemaState};

/// One warning returned by Go `ParseStmtHints`/`ParsePlanHints`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HintWarning {
    /// MySQL warning code. Ordinary source `errors.NewNoStackError` warnings
    /// use 1105; conflicting hints use 3126.
    pub code: u16,
    /// Warning message.
    pub message: String,
}

impl HintWarning {
    fn ordinary(message: impl Into<String>) -> Self {
        Self {
            code: 1105,
            message: message.into(),
        }
    }

    pub(crate) fn conflicting(hint: impl Into<String>) -> Self {
        Self {
            code: 3126,
            message: format!("Hint {} is ignored as conflicting/duplicated.", hint.into()),
        }
    }

    pub(crate) fn optimizer(message: impl Into<String>) -> Self {
        Self {
            code: 1815,
            message: message.into(),
        }
    }
}

/// Go `RestrictedHintChecker`.
pub type RestrictedHintChecker = fn(&str) -> Option<HintWarning>;

static RESTRICTED_HINT_CHECKER: RwLock<Option<RestrictedHintChecker>> = RwLock::new(None);

/// Go `RegisterRestrictedHintChecker`.
pub fn register_restricted_hint_checker(checker: RestrictedHintChecker) {
    *RESTRICTED_HINT_CHECKER
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(checker);
}

/// Go `setVarHintChecker` callback.
pub trait SetVarHintChecker {
    /// Returns whether the variable may be used and an optional warning.
    fn check(&mut self, variable_name: &str, hint_name: &str) -> (bool, Option<HintWarning>);
}

impl<F> SetVarHintChecker for F
where
    F: FnMut(&str, &str) -> (bool, Option<HintWarning>),
{
    fn check(&mut self, variable_name: &str, hint_name: &str) -> (bool, Option<HintWarning>) {
        self(variable_name, hint_name)
    }
}

/// Go `hypoIndexChecker` callback.
pub trait HypoIndexChecker {
    /// Returns a column offset or the source warning detail.
    fn column_offset(&mut self, database: &str, table: &str, column: &str) -> Result<i64, String>;
}

impl<F> HypoIndexChecker for F
where
    F: FnMut(&str, &str, &str) -> Result<i64, String>,
{
    fn column_offset(&mut self, database: &str, table: &str, column: &str) -> Result<i64, String> {
        self(database, table, column)
    }
}

/// Go `StmtHints`.
#[derive(Debug, Default)]
pub struct StmtHints {
    /// Whether the original statement contained any hints before filtering.
    pub query_has_hints: bool,
    /// Statement memory quota in bytes.
    pub mem_quota_query: i64,
    /// Maximum execution time in milliseconds.
    pub max_execution_time: u64,
    /// Statement replica-read policy byte.
    pub replica_read: u8,
    /// Whether IN subqueries may be rewritten to join and aggregation.
    pub allow_in_subq_to_join_and_agg: bool,
    /// Whether index merge is disabled for the statement.
    pub no_index_merge_hint: bool,
    /// Whether written join order is forced.
    pub straight_join_order: bool,
    /// Whether the cascades planner is enabled for the statement.
    pub enable_cascades_planner: bool,
    /// One-based physical-plan ordinal, or -1 when disabled.
    pub force_nth_plan: i64,
    /// Statement resource-group name.
    pub resource_group: String,
    /// Whether both plan caches must be bypassed.
    pub ignore_plan_cache: bool,
    /// Whether explicit-hint plan-cache strategy is requested.
    pub use_plan_cache: bool,
    /// Whether slow-log writing is explicitly requested.
    pub write_slow_log: bool,
    /// Whether USE_TOJA was present.
    pub has_allow_in_subq_to_join_and_agg_hint: bool,
    /// Whether a valid MEMORY_QUOTA was present.
    pub has_mem_quota_hint: bool,
    /// Whether READ_CONSISTENT_REPLICA was present.
    pub has_replica_read_hint: bool,
    /// Whether MAX_EXECUTION_TIME was present.
    pub has_max_execution_time: bool,
    /// Whether USE_CASCADES was present.
    pub has_enable_cascades_planner_hint: bool,
    /// Whether RESOURCE_GROUP was present.
    pub has_resource_group: bool,
    /// First valid SET_VAR value for each variable name.
    pub set_vars: HashMap<String, String>,
    /// Hypothetical indexes keyed by database, table, and index name.
    pub hinted_hypo_indexes: HashMap<String, HashMap<String, HashMap<String, IndexInfo>>>,
    /// Hints remaining after restricted-hint filtering.
    pub original_table_hints: Vec<Hint>,
}

// Go's Clone deliberately does not copy HintedHypoIndexes.
impl Clone for StmtHints {
    fn clone(&self) -> Self {
        Self {
            query_has_hints: self.query_has_hints,
            mem_quota_query: self.mem_quota_query,
            max_execution_time: self.max_execution_time,
            replica_read: self.replica_read,
            allow_in_subq_to_join_and_agg: self.allow_in_subq_to_join_and_agg,
            no_index_merge_hint: self.no_index_merge_hint,
            straight_join_order: self.straight_join_order,
            enable_cascades_planner: self.enable_cascades_planner,
            force_nth_plan: self.force_nth_plan,
            resource_group: self.resource_group.clone(),
            ignore_plan_cache: self.ignore_plan_cache,
            use_plan_cache: self.use_plan_cache,
            write_slow_log: self.write_slow_log,
            has_allow_in_subq_to_join_and_agg_hint: self.has_allow_in_subq_to_join_and_agg_hint,
            has_mem_quota_hint: self.has_mem_quota_hint,
            has_replica_read_hint: self.has_replica_read_hint,
            has_max_execution_time: self.has_max_execution_time,
            has_enable_cascades_planner_hint: self.has_enable_cascades_planner_hint,
            has_resource_group: self.has_resource_group,
            set_vars: self.set_vars.clone(),
            hinted_hypo_indexes: HashMap::new(),
            original_table_hints: self.original_table_hints.clone(),
        }
    }
}

impl StmtHints {
    /// Go `StmtHints.TaskMapNeedBackUp`.
    #[must_use]
    pub fn task_map_need_backup(&self) -> bool {
        self.force_nth_plan != -1
    }

    fn add_hypo_index(&mut self, database: String, table: String, index: String, info: IndexInfo) {
        self.hinted_hypo_indexes
            .entry(database)
            .or_default()
            .entry(table)
            .or_default()
            .insert(index, info);
    }
}

pub(crate) fn should_warn_restricted(hint: &Hint) -> bool {
    matches!(
        hint.name.to_ascii_lowercase().as_str(),
        "memory_quota"
            | "resource_group"
            | "use_toja"
            | "use_cascades"
            | "no_index_merge"
            | "read_consistent_replica"
            | "max_execution_time"
            | "nth_plan"
            | "hypo_index"
            | "set_var"
            | "ignore_plan_cache"
            | "use_plan_cache"
            | "write_slow_log"
    )
}

pub(crate) fn filter_restricted(
    hints: &[Hint],
    should_warn: impl Fn(&Hint) -> bool,
) -> (Vec<Hint>, Vec<HintWarning>) {
    let checker = *RESTRICTED_HINT_CHECKER
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(checker) = checker else {
        return (hints.to_vec(), Vec::new());
    };
    let mut filtered = Vec::with_capacity(hints.len());
    let mut warnings = Vec::new();
    for hint in hints {
        if let Some(warning) = checker(&hint.name.to_ascii_lowercase()) {
            if should_warn(hint) {
                warnings.push(warning);
            }
        } else {
            filtered.push(hint.clone());
        }
    }
    (filtered, warnings)
}

/// Go `ParseStmtHints`.
pub fn parse_stmt_hints<S, H>(
    hints: &[Hint],
    set_var_checker: &mut S,
    hypo_index_checker: &mut H,
    current_database: &str,
    replica_read_follower: u8,
) -> (StmtHints, Vec<usize>, Vec<HintWarning>)
where
    S: SetVarHintChecker,
    H: HypoIndexChecker,
{
    let query_has_hints = !hints.is_empty();
    let (hints, mut warnings) = filter_restricted(hints, should_warn_restricted);
    let mut result = StmtHints {
        query_has_hints,
        ..StmtHints::default()
    };
    if hints.is_empty() {
        return (result, Vec::new(), warnings);
    }

    let mut offsets = HashMap::<String, usize>::new();
    let mut counts = HashMap::<String, usize>::new();
    let mut nth_plan: Option<i64> = None;
    let mut set_var_offsets = Vec::new();

    for (offset, hint) in hints.iter().enumerate() {
        let name = hint.name.to_ascii_lowercase();
        match name.as_str() {
            "memory_quota"
            | "resource_group"
            | "use_toja"
            | "use_cascades"
            | "no_index_merge"
            | "read_consistent_replica"
            | "max_execution_time"
            | "straight_join" => {
                offsets.insert(name.clone(), offset);
                *counts.entry(name).or_default() += 1;
            }
            "nth_plan" => {
                *counts.entry(name).or_default() += 1;
                if let HintKind::Number { value, .. } = hint.kind {
                    nth_plan = Some(value);
                }
            }
            "hypo_index" => {
                let HintKind::Tables { tables, .. } = &hint.kind else {
                    continue;
                };
                if tables.len() < 3 {
                    warnings.push(HintWarning::ordinary(
                        "Invalid HYPO_INDEX hint, valid usage: HYPO_INDEX(tableName, indexName, cols...)",
                    ));
                    continue;
                }
                // Go keys on `DBName.L`, the lowercased db name.
                let database = tables[0]
                    .db_name
                    .clone()
                    .unwrap_or_else(|| current_database.to_owned())
                    .to_ascii_lowercase();
                let table = tables[0].name.clone();
                let index = tables[1].name.clone();
                let mut columns = Vec::with_capacity(tables.len() - 2);
                let mut invalid = false;
                for column in &tables[2..] {
                    match hypo_index_checker.column_offset(&database, &table, &column.name) {
                        Ok(column_offset) => columns.push(Some(GoShared::new(IndexColumn {
                            name: CiString::new(&column.name),
                            offset: column_offset,
                            length: i64::from(UNSPECIFIED_LENGTH),
                            ..IndexColumn::default()
                        }))),
                        Err(error) => {
                            warnings.push(HintWarning::ordinary(format!(
                                "invalid HYPO_INDEX hint: {error}"
                            )));
                            invalid = true;
                            break;
                        }
                    }
                }
                if !invalid {
                    result.add_hypo_index(
                        database,
                        table.to_ascii_lowercase(),
                        index.to_ascii_lowercase(),
                        IndexInfo {
                            name: CiString::new(&index),
                            columns: GoSharedPointerSlice::from_handles(columns),
                            state: SchemaState::PUBLIC,
                            tp: IndexType::HYPO,
                            ..IndexInfo::default()
                        },
                    );
                }
            }
            "set_var" => {
                let HintKind::SetVar { var_name, value } = &hint.kind else {
                    continue;
                };
                let (allowed, warning) = set_var_checker.check(var_name, &hint.name);
                warnings.extend(warning);
                if !allowed {
                    continue;
                }
                if result.set_vars.contains_key(var_name) {
                    warnings.push(HintWarning::conflicting(format!(
                        "{}({var_name}={value})",
                        hint.name
                    )));
                    continue;
                }
                result.set_vars.insert(var_name.clone(), value.clone());
                set_var_offsets.push(offset);
            }
            "ignore_plan_cache" => result.ignore_plan_cache = true,
            "use_plan_cache" => result.use_plan_cache = true,
            "write_slow_log" => result.write_slow_log = true,
            _ => {}
        }
    }
    result.original_table_hints = hints.clone();

    let count = |name: &str| counts.get(name).copied().unwrap_or_default();
    if let Some(offset) = offsets.get("memory_quota").copied() {
        let HintKind::MemoryQuota { bytes, .. } = hints[offset].kind else {
            unreachable!("MEMORY_QUOTA parser shape")
        };
        if count("memory_quota") > 1 {
            warnings.push(HintWarning::ordinary(format!(
                "MEMORY_QUOTA() is defined more than once, only the last definition takes effect: MEMORY_QUOTA({bytes})"
            )));
        }
        if bytes < 0 {
            offsets.remove("memory_quota");
            warnings.push(HintWarning::ordinary(
                "The use of MEMORY_QUOTA hint is invalid, valid usage: MEMORY_QUOTA(10 MB) or MEMORY_QUOTA(10 GB)",
            ));
        } else {
            result.has_mem_quota_hint = true;
            result.mem_quota_query = bytes;
            if bytes == 0 {
                warnings.push(HintWarning::ordinary(
                    "Setting the MEMORY_QUOTA to 0 means no memory limit",
                ));
            }
        }
    }
    apply_last_bool(
        &hints,
        &offsets,
        &counts,
        "use_toja",
        "USE_TOJA",
        &mut result.has_allow_in_subq_to_join_and_agg_hint,
        &mut result.allow_in_subq_to_join_and_agg,
        &mut warnings,
    );
    apply_last_bool(
        &hints,
        &offsets,
        &counts,
        "use_cascades",
        "USE_CASCADES",
        &mut result.has_enable_cascades_planner_hint,
        &mut result.enable_cascades_planner,
        &mut warnings,
    );
    for (name, display) in [
        ("no_index_merge", "NO_INDEX_MERGE"),
        ("straight_join", "STRAIGHT_JOIN"),
        ("read_consistent_replica", "READ_CONSISTENT_REPLICA"),
    ] {
        if count(name) > 1 {
            warnings.push(HintWarning::ordinary(format!(
                "{display}() is defined more than once, only the last definition takes effect"
            )));
        }
    }
    result.no_index_merge_hint = count("no_index_merge") != 0;
    result.straight_join_order = count("straight_join") != 0;
    if count("read_consistent_replica") != 0 {
        result.has_replica_read_hint = true;
        result.replica_read = replica_read_follower;
    }
    if let Some(offset) = offsets.get("max_execution_time").copied() {
        let HintKind::Number { value, .. } = hints[offset].kind else {
            unreachable!("MAX_EXECUTION_TIME parser shape")
        };
        if count("max_execution_time") > 1 {
            warnings.push(HintWarning::ordinary(format!(
                "MAX_EXECUTION_TIME() is defined more than once, only the last definition takes effect: MAX_EXECUTION_TIME({value})"
            )));
        }
        result.has_max_execution_time = true;
        result.max_execution_time = value as u64;
    }
    if let Some(offset) = offsets.get("resource_group").copied() {
        let HintKind::Name { name, .. } = &hints[offset].kind else {
            unreachable!("RESOURCE_GROUP parser shape")
        };
        if count("resource_group") > 1 {
            warnings.push(HintWarning::ordinary(format!(
                "RESOURCE_GROUP() is defined more than once, only the last definition takes effect: RESOURCE_GROUP({name})"
            )));
        }
        result.has_resource_group = true;
        result.resource_group.clone_from(name);
    }
    if let Some(value) = nth_plan {
        if count("nth_plan") > 1 {
            warnings.push(HintWarning::ordinary(format!(
                "NTH_PLAN() is defined more than once, only the last definition takes effect: NTH_PLAN({value})"
            )));
        }
        // Go assigns the hintdata first and clamps to -1 afterwards
        // (hint.go:521-525), so an out-of-range value never stays visible.
        result.force_nth_plan = value;
        if value < 1 {
            result.force_nth_plan = -1;
            warnings.push(HintWarning::ordinary(
                "the hintdata for NTH_PLAN() is too small, hint ignored",
            ));
        }
    } else {
        result.force_nth_plan = -1;
    }

    let mut effective_offsets = offsets.into_values().collect::<Vec<_>>();
    effective_offsets.extend(set_var_offsets);
    effective_offsets.sort_unstable();
    (result, effective_offsets, warnings)
}

#[allow(clippy::too_many_arguments)]
fn apply_last_bool(
    hints: &[Hint],
    offsets: &HashMap<String, usize>,
    counts: &HashMap<String, usize>,
    name: &str,
    display: &str,
    present: &mut bool,
    target: &mut bool,
    warnings: &mut Vec<HintWarning>,
) {
    let Some(offset) = offsets.get(name).copied() else {
        return;
    };
    let HintKind::Bool { value, .. } = hints[offset].kind else {
        unreachable!("boolean hint parser shape")
    };
    if counts.get(name).copied().unwrap_or_default() > 1 {
        warnings.push(HintWarning::ordinary(format!(
            "{display}() is defined more than once, only the last definition takes effect: {display}({value})"
        )));
    }
    *present = true;
    *target = value;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::statement::parse_stmt_hints;
    use tidb_ast::{Hint, HintKind};

    fn hint_table(name: &str, db: Option<&str>) -> tidb_ast::HintTable {
        tidb_ast::HintTable {
            name: name.to_owned(),
            db_name: db.map(str::to_owned),
            qb_name: None,
            partitions: Vec::new(),
        }
    }

    fn number_hint(name: &str, value: i64) -> Hint {
        Hint {
            name: name.to_owned(),
            kind: HintKind::Number {
                qb_name: None,
                value,
            },
        }
    }

    /// Go hint.go:521-525 assigns the hintdata first and clamps out-of-range
    /// values to -1, so `NTH_PLAN(0)` never leaves an "enabled" zero behind.
    #[test]
    fn nth_plan_zero_clamps_to_disabled() {
        fn accept_set_var(_: &str, _: &str) -> (bool, Option<HintWarning>) {
            (true, None)
        }
        fn reject_column(_: &str, _: &str, _: &str) -> Result<i64, String> {
            Err("no table".to_owned())
        }
        let hints = vec![number_hint("NTH_PLAN", 0)];
        let (stmt_hints, _, _) =
            parse_stmt_hints(&hints, &mut accept_set_var, &mut reject_column, "test", 0);
        assert_eq!(-1, stmt_hints.force_nth_plan);
        assert!(!stmt_hints.task_map_need_backup());
    }

    /// Go keys HYPO_INDEX on `DBName.L` (hint.go:364): the checker input and
    /// the added-hypo db name are lowercased even when the SQL wrote them
    /// with uppercase.
    #[test]
    fn hypo_index_database_key_is_lowercased() {
        fn accept_set_var(_: &str, _: &str) -> (bool, Option<HintWarning>) {
            (true, None)
        }
        let hints = vec![Hint {
            name: "HYPO_INDEX".to_owned(),
            kind: HintKind::Tables {
                qb_name: None,
                tables: vec![
                    hint_table("t1", Some("DB1")),
                    hint_table("idx", Some("DB1")),
                    hint_table("c1", Some("DB1")),
                ],
            },
        }];
        let mut seen = Vec::new();
        let mut checker = |database: &str, table: &str, column: &str| {
            seen.push((database.to_owned(), table.to_owned(), column.to_owned()));
            if database == "DB1" {
                return Err("database must already be lowercased".to_owned());
            }
            Ok(0)
        };
        let (stmt_hints, _, warnings) =
            parse_stmt_hints(&hints, &mut accept_set_var, &mut checker, "test", 0);
        assert!(
            seen.iter().all(|(database, _, _)| database == "db1"),
            "the checker must see the lowercased db: {seen:?}"
        );
        assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
        assert_eq!(1, stmt_hints.hinted_hypo_indexes.len());
    }
}
