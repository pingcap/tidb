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

//! SEED of Go `pkg/util/hint`, covering `hint.go`'s statement-level half:
//! `StmtHints`, `ParseStmtHints` with every per-hint arm and warning, the
//! restricted-hint filter, and the hypo-index construction.
//!
//! Hints arrive as [`StmtHintInput`], a uniform view of Go's
//! `ast.TableOptimizerHint` (one struct with `HintName`/`HintData`/`Tables`;
//! the Rust hint AST is an enum, so the caller builds the view). Go's
//! `RegisterRestrictedHintChecker` installs a package-global hook from the
//! SEM layer; the checker is a parameter here, `None` reproducing the
//! unregistered state. The table-level `PlanHints` family stays open, as
//! does `hint_processor.go`'s parser plumbing.

use std::collections::BTreeMap;

use tidb_ast::CiString;
use tidb_model::index::{IndexColumn, IndexInfo};
use tidb_model::schema_state::SchemaState;

/// Go `HintMemoryQuota` and friends — the statement-hint names this parser
/// dispatches on, lowercase as `HintName.L` is.
pub const HINT_MEMORY_QUOTA: &str = "memory_quota";
/// Go `HintUseToja`.
pub const HINT_USE_TOJA: &str = "use_toja";
/// Go `HintNoIndexMerge`.
pub const HINT_NO_INDEX_MERGE: &str = "no_index_merge";
/// Go `HintMaxExecutionTime`.
pub const HINT_MAX_EXECUTION_TIME: &str = "max_execution_time";
/// Go `HintStraightJoin`.
pub const HINT_STRAIGHT_JOIN: &str = "straight_join";
/// Go `HintIgnorePlanCache`.
pub const HINT_IGNORE_PLAN_CACHE: &str = "ignore_plan_cache";
/// Go `HintUsePlanCache`.
pub const HINT_USE_PLAN_CACHE: &str = "use_plan_cache";
/// Go `HintWriteSlowLog`.
pub const HINT_WRITE_SLOW_LOG: &str = "write_slow_log";

/// The typed payload Go stores in `TableOptimizerHint.HintData`.
#[derive(Clone, Debug, PartialEq)]
pub enum StmtHintData {
    /// No payload.
    None,
    /// `int64` payloads (`MEMORY_QUOTA`, `NTH_PLAN`).
    Int(i64),
    /// `uint64` payloads (`MAX_EXECUTION_TIME`).
    Uint(u64),
    /// `bool` payloads (`USE_TOJA`, `USE_CASCADES`).
    Bool(bool),
    /// `string` payloads (`RESOURCE_GROUP`).
    Str(String),
    /// `ast.HintSetVar`.
    SetVar {
        /// The variable name as written.
        var_name: String,
        /// The value text as written.
        value: String,
    },
}

/// One table argument, narrowed to what the statement hints read.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StmtHintTable {
    /// `DBName.L`; empty when unqualified.
    pub db_name: String,
    /// The table (or, positionally in `HYPO_INDEX`, index/column) name.
    pub table_name: CiString,
}

/// A uniform view of Go `ast.TableOptimizerHint` for statement-hint parsing.
#[derive(Clone, Debug, PartialEq)]
pub struct StmtHintInput {
    /// `HintName.L`.
    pub name: String,
    /// `HintName.String()`, used verbatim in warnings.
    pub display_name: String,
    /// `Tables`.
    pub tables: Vec<StmtHintTable>,
    /// `HintData`.
    pub data: StmtHintData,
}

/// Go `StmtHints`: the statement-level hint state the session consumes.
#[derive(Clone, Debug, Default)]
pub struct StmtHints {
    /// True iff there were hints in the statement.
    pub query_has_hints: bool,
    /// Go `MemQuotaQuery`.
    pub mem_quota_query: i64,
    /// Go `MaxExecutionTime`.
    pub max_execution_time: u64,
    /// Go `ReplicaRead`.
    pub replica_read: u8,
    /// Go `AllowInSubqToJoinAndAgg`.
    pub allow_in_subq_to_join_and_agg: bool,
    /// Go `NoIndexMergeHint`.
    pub no_index_merge_hint: bool,
    /// Go `StraightJoinOrder`.
    pub straight_join_order: bool,
    /// Go `EnableCascadesPlanner`.
    pub enable_cascades_planner: bool,
    /// Go `ForceNthPlan`; -1 disables.
    pub force_nth_plan: i64,
    /// Go `ResourceGroup`.
    pub resource_group: String,
    /// Go `IgnorePlanCache`.
    pub ignore_plan_cache: bool,
    /// Go `UsePlanCache`.
    pub use_plan_cache: bool,
    /// Go `WriteSlowLog`.
    pub write_slow_log: bool,
    /// Go `HasAllowInSubqToJoinAndAggHint`.
    pub has_allow_in_subq_to_join_and_agg_hint: bool,
    /// Go `HasMemQuotaHint`.
    pub has_mem_quota_hint: bool,
    /// Go `HasReplicaReadHint`.
    pub has_replica_read_hint: bool,
    /// Go `HasMaxExecutionTime`.
    pub has_max_execution_time: bool,
    /// Go `HasEnableCascadesPlannerHint`.
    pub has_enable_cascades_planner_hint: bool,
    /// Go `HasResourceGroup`.
    pub has_resource_group: bool,
    /// Go `SetVars`.
    pub set_vars: BTreeMap<String, String>,
    /// Go `HintedHypoIndexes`: db -> table -> index -> info.
    pub hinted_hypo_indexes: BTreeMap<String, BTreeMap<String, BTreeMap<String, IndexInfo>>>,
    /// Go `OriginalTableHints`, after restricted-hint filtering.
    pub original_table_hints: Vec<StmtHintInput>,
}

impl StmtHints {
    /// Go `TaskMapNeedBackUp`.
    #[must_use]
    pub fn task_map_need_back_up(&self) -> bool {
        self.force_nth_plan != -1
    }

    /// Go `Clone`: field-by-field, deliberately NOT carrying
    /// `HintedHypoIndexes` — the source's clone omits it too.
    #[must_use]
    pub fn clone_hints(&self) -> Self {
        Self {
            hinted_hypo_indexes: BTreeMap::new(),
            ..self.clone()
        }
    }

    /// Go's private `addHypoIndex`.
    fn add_hypo_index(&mut self, db: &str, tbl: &str, idx: &str, idx_info: IndexInfo) {
        self.hinted_hypo_indexes
            .entry(db.to_owned())
            .or_default()
            .entry(tbl.to_owned())
            .or_default()
            .insert(idx.to_owned(), idx_info);
    }
}

/// What `ParseStmtHints` answers: the hints, the offsets of the recognized
/// statement hints in the input (sorted), and the warnings, in order.
#[derive(Clone, Debug, Default)]
pub struct ParsedStmtHints {
    /// The statement hints.
    pub hints: StmtHints,
    /// Go `offs`.
    pub offsets: Vec<usize>,
    /// Go `warns`, each rendered as its Go message.
    pub warnings: Vec<String>,
}

/// Go `RestrictedHintChecker`: a non-`None` message strips the hint.
pub type RestrictedHintChecker<'a> = &'a dyn Fn(&str) -> Option<String>;

/// The `SET_VAR` permission checker: whether the variable may be set, plus
/// an optional warning either way (Go's `setVarHintChecker`).
pub type SetVarHintChecker<'a> = &'a dyn Fn(&str, &str) -> (bool, Option<String>);

/// Go's private `shouldWarnRestrictedHintInParseStmtHints`.
pub(crate) fn should_warn_restricted(name: &str) -> bool {
    matches!(
        name,
        HINT_MEMORY_QUOTA
            | "resource_group"
            | HINT_USE_TOJA
            | "use_cascades"
            | HINT_NO_INDEX_MERGE
            | "read_consistent_replica"
            | HINT_MAX_EXECUTION_TIME
            | "nth_plan"
            | "hypo_index"
            | "set_var"
            | HINT_IGNORE_PLAN_CACHE
            | HINT_USE_PLAN_CACHE
            | HINT_WRITE_SLOW_LOG
    )
}

/// Go `ParseStmtHints`.
///
/// `set_var_hint_checker` answers whether a variable may be set through
/// `SET_VAR`, with an optional warning either way; `hypo_index_checker`
/// resolves a column to its offset or fails the hint;
/// `replica_read_follower` is the byte `READ_CONSISTENT_REPLICA` stores
/// (Go passes `kv.ReplicaReadFollower` in to avoid an import cycle).
#[expect(clippy::too_many_lines, reason = "one Go function, kept whole")]
pub fn parse_stmt_hints(
    hints: &[StmtHintInput],
    set_var_hint_checker: SetVarHintChecker<'_>,
    hypo_index_checker: &dyn Fn(&str, &CiString, &CiString) -> Result<i64, String>,
    restricted_hint_checker: Option<RestrictedHintChecker<'_>>,
    current_db: &str,
    replica_read_follower: u8,
) -> ParsedStmtHints {
    let mut parsed = ParsedStmtHints {
        hints: StmtHints {
            query_has_hints: !hints.is_empty(),
            ..StmtHints::default()
        },
        ..ParsedStmtHints::default()
    };

    // Go `filterRestrictedHints`: an unregistered checker filters nothing.
    let mut filtered: Vec<&StmtHintInput> = Vec::with_capacity(hints.len());
    if let Some(checker) = restricted_hint_checker {
        for hint in hints {
            if let Some(warning) = checker(&hint.name) {
                if should_warn_restricted(&hint.name) {
                    parsed.warnings.push(warning);
                }
                continue;
            }
            filtered.push(hint);
        }
    } else {
        filtered.extend(hints.iter());
    }

    if filtered.is_empty() {
        // Go returns here for an empty incoming list AND for a fully
        // filtered one — `hints` is reassigned by the filter — leaving
        // ForceNthPlan at its zero value in both shapes.
        return parsed;
    }

    let mut hint_offs: BTreeMap<&str, usize> = BTreeMap::new();
    let mut force_nth_plan: Option<&StmtHintInput> = None;
    let mut memory_quota_cnt = 0;
    let mut use_toja_cnt = 0;
    let mut use_cascades_cnt = 0;
    let mut no_index_merge_cnt = 0;
    let mut read_replica_cnt = 0;
    let mut max_execution_time_cnt = 0;
    let mut force_nth_plan_cnt = 0;
    let mut straight_join_cnt = 0;
    let mut resource_group_cnt = 0;
    let mut set_vars: BTreeMap<String, String> = BTreeMap::new();
    let mut set_vars_offs: Vec<usize> = Vec::new();

    for (i, hint) in filtered.iter().enumerate() {
        match hint.name.as_str() {
            HINT_MEMORY_QUOTA => {
                hint_offs.insert(HINT_MEMORY_QUOTA, i);
                memory_quota_cnt += 1;
            }
            "resource_group" => {
                hint_offs.insert("resource_group", i);
                resource_group_cnt += 1;
            }
            HINT_USE_TOJA => {
                hint_offs.insert(HINT_USE_TOJA, i);
                use_toja_cnt += 1;
            }
            "use_cascades" => {
                hint_offs.insert("use_cascades", i);
                use_cascades_cnt += 1;
            }
            HINT_NO_INDEX_MERGE => {
                hint_offs.insert(HINT_NO_INDEX_MERGE, i);
                no_index_merge_cnt += 1;
            }
            "read_consistent_replica" => {
                hint_offs.insert("read_consistent_replica", i);
                read_replica_cnt += 1;
            }
            HINT_MAX_EXECUTION_TIME => {
                hint_offs.insert(HINT_MAX_EXECUTION_TIME, i);
                max_execution_time_cnt += 1;
            }
            "nth_plan" => {
                force_nth_plan_cnt += 1;
                force_nth_plan = Some(hint);
            }
            HINT_STRAIGHT_JOIN => {
                hint_offs.insert(HINT_STRAIGHT_JOIN, i);
                straight_join_cnt += 1;
            }
            "hypo_index" => {
                if hint.tables.len() < 3 {
                    parsed.warnings.push(
                        "Invalid HYPO_INDEX hint, valid usage: HYPO_INDEX(tableName, indexName, cols...)"
                            .to_owned(),
                    );
                    continue;
                }
                let db = if hint.tables[0].db_name.is_empty() {
                    current_db.to_owned()
                } else {
                    hint.tables[0].db_name.clone()
                };
                let tbl = &hint.tables[0].table_name;
                let idx = &hint.tables[1].table_name;
                let mut cols = Vec::new();
                let mut invalid = false;
                for table in &hint.tables[2..] {
                    match hypo_index_checker(&db, tbl, &table.table_name) {
                        Err(error) => {
                            invalid = true;
                            parsed
                                .warnings
                                .push(format!("invalid HYPO_INDEX hint: {error}"));
                            break;
                        }
                        Ok(offset) => cols.push(IndexColumn {
                            name: table.table_name.clone(),
                            offset,
                            length: tidb_datatype::UNSPECIFIED_LENGTH,
                            ..IndexColumn::default()
                        }),
                    }
                }
                if invalid {
                    continue;
                }
                let idx_info = IndexInfo {
                    name: idx.clone(),
                    columns: cols.into(),
                    state: SchemaState::PUBLIC,
                    tp: tidb_ast::IndexType::HYPO,
                    ..IndexInfo::default()
                };
                parsed
                    .hints
                    .add_hypo_index(&db, tbl.lowercase(), idx.lowercase(), idx_info);
            }
            "set_var" => {
                let StmtHintData::SetVar { var_name, value } = &hint.data else {
                    continue;
                };
                let (ok, warning) = set_var_hint_checker(var_name, &hint.display_name);
                if let Some(warning) = warning {
                    parsed.warnings.push(warning);
                }
                if !ok {
                    continue;
                }
                if set_vars.contains_key(var_name) {
                    // Go `ErrWarnConflictingHint` (3126).
                    parsed.warnings.push(format!(
                        "Hint {}({var_name}={value}) is ignored as conflicting/duplicated.",
                        hint.display_name
                    ));
                    continue;
                }
                set_vars.insert(var_name.clone(), value.clone());
                set_vars_offs.push(i);
            }
            HINT_IGNORE_PLAN_CACHE => parsed.hints.ignore_plan_cache = true,
            HINT_USE_PLAN_CACHE => parsed.hints.use_plan_cache = true,
            HINT_WRITE_SLOW_LOG => parsed.hints.write_slow_log = true,
            _ => {}
        }
    }
    parsed.hints.original_table_hints = filtered.iter().map(|hint| (*hint).clone()).collect();
    parsed.hints.set_vars = set_vars;

    // MEMORY_QUOTA.
    if memory_quota_cnt != 0 {
        let hint = filtered[hint_offs[HINT_MEMORY_QUOTA]];
        let quota = match hint.data {
            StmtHintData::Int(quota) => quota,
            _ => 0,
        };
        if memory_quota_cnt > 1 {
            parsed.warnings.push(format!(
                "MEMORY_QUOTA() is defined more than once, only the last definition takes effect: MEMORY_QUOTA({quota})"
            ));
        }
        if quota < 0 {
            hint_offs.remove(HINT_MEMORY_QUOTA);
            parsed.warnings.push(
                "The use of MEMORY_QUOTA hint is invalid, valid usage: MEMORY_QUOTA(10 MB) or MEMORY_QUOTA(10 GB)"
                    .to_owned(),
            );
        } else {
            parsed.hints.has_mem_quota_hint = true;
            parsed.hints.mem_quota_query = quota;
            if quota == 0 {
                parsed
                    .warnings
                    .push("Setting the MEMORY_QUOTA to 0 means no memory limit".to_owned());
            }
        }
    }
    // USE_TOJA.
    if use_toja_cnt != 0 {
        let hint = filtered[hint_offs[HINT_USE_TOJA]];
        let value = matches!(hint.data, StmtHintData::Bool(true));
        if use_toja_cnt > 1 {
            parsed.warnings.push(format!(
                "USE_TOJA() is defined more than once, only the last definition takes effect: USE_TOJA({value})"
            ));
        }
        parsed.hints.has_allow_in_subq_to_join_and_agg_hint = true;
        parsed.hints.allow_in_subq_to_join_and_agg = value;
    }
    // USE_CASCADES.
    if use_cascades_cnt != 0 {
        let hint = filtered[hint_offs["use_cascades"]];
        let value = matches!(hint.data, StmtHintData::Bool(true));
        if use_cascades_cnt > 1 {
            parsed.warnings.push(format!(
                "USE_CASCADES() is defined more than once, only the last definition takes effect: USE_CASCADES({value})"
            ));
        }
        parsed.hints.has_enable_cascades_planner_hint = true;
        parsed.hints.enable_cascades_planner = value;
    }
    // NO_INDEX_MERGE.
    if no_index_merge_cnt != 0 {
        if no_index_merge_cnt > 1 {
            parsed.warnings.push(
                "NO_INDEX_MERGE() is defined more than once, only the last definition takes effect"
                    .to_owned(),
            );
        }
        parsed.hints.no_index_merge_hint = true;
    }
    // STRAIGHT_JOIN.
    if straight_join_cnt != 0 {
        if straight_join_cnt > 1 {
            parsed.warnings.push(
                "STRAIGHT_JOIN() is defined more than once, only the last definition takes effect"
                    .to_owned(),
            );
        }
        parsed.hints.straight_join_order = true;
    }
    // READ_CONSISTENT_REPLICA.
    if read_replica_cnt != 0 {
        if read_replica_cnt > 1 {
            parsed.warnings.push(
                "READ_CONSISTENT_REPLICA() is defined more than once, only the last definition takes effect"
                    .to_owned(),
            );
        }
        parsed.hints.has_replica_read_hint = true;
        parsed.hints.replica_read = replica_read_follower;
    }
    // MAX_EXECUTION_TIME.
    if max_execution_time_cnt != 0 {
        let hint = filtered[hint_offs[HINT_MAX_EXECUTION_TIME]];
        let value = match hint.data {
            StmtHintData::Uint(value) => value,
            _ => 0,
        };
        if max_execution_time_cnt > 1 {
            parsed.warnings.push(format!(
                "MAX_EXECUTION_TIME() is defined more than once, only the last definition takes effect: MAX_EXECUTION_TIME({value})"
            ));
        }
        parsed.hints.has_max_execution_time = true;
        parsed.hints.max_execution_time = value;
    }
    // RESOURCE_GROUP.
    if resource_group_cnt != 0 {
        let hint = filtered[hint_offs["resource_group"]];
        let value = match &hint.data {
            StmtHintData::Str(value) => value.clone(),
            _ => String::new(),
        };
        if resource_group_cnt > 1 {
            parsed.warnings.push(format!(
                "RESOURCE_GROUP() is defined more than once, only the last definition takes effect: RESOURCE_GROUP({value})"
            ));
        }
        parsed.hints.has_resource_group = true;
        parsed.hints.resource_group = value;
    }
    // NTH_PLAN.
    if force_nth_plan_cnt != 0 {
        let value = match force_nth_plan.map(|hint| &hint.data) {
            Some(StmtHintData::Int(value)) => *value,
            _ => 0,
        };
        if force_nth_plan_cnt > 1 {
            parsed.warnings.push(format!(
                "NTH_PLAN() is defined more than once, only the last definition takes effect: NTH_PLAN({value})"
            ));
        }
        parsed.hints.force_nth_plan = value;
        if parsed.hints.force_nth_plan < 1 {
            parsed.hints.force_nth_plan = -1;
            parsed
                .warnings
                .push("the hintdata for NTH_PLAN() is too small, hint ignored".to_owned());
        }
    } else {
        parsed.hints.force_nth_plan = -1;
    }

    parsed.offsets = hint_offs.into_values().collect();
    parsed.offsets.extend(set_vars_offs);
    parsed.offsets.sort_unstable();
    parsed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accept_all(_: &str, _: &str) -> (bool, Option<String>) {
        (true, None)
    }

    fn no_hypo(_: &str, _: &CiString, _: &CiString) -> Result<i64, String> {
        Err("unexpected".to_owned())
    }

    fn hint(name: &str, data: StmtHintData) -> StmtHintInput {
        StmtHintInput {
            name: name.to_owned(),
            display_name: name.to_uppercase(),
            tables: Vec::new(),
            data,
        }
    }

    fn parse(hints: &[StmtHintInput]) -> ParsedStmtHints {
        parse_stmt_hints(hints, &accept_all, &no_hypo, None, "test", 2)
    }

    // No hints at all: the flag is down and nth-plan is untouched by the
    // tail (Go returns early with the zero value).
    #[test]
    fn empty_input_returns_early() {
        let parsed = parse(&[]);
        assert!(!parsed.hints.query_has_hints);
        assert!(parsed.warnings.is_empty());
    }

    // Each single-shot hint sets its field and flag.
    #[test]
    fn single_hints_set_their_fields() {
        let parsed = parse(&[
            hint(HINT_MEMORY_QUOTA, StmtHintData::Int(1 << 30)),
            hint(HINT_USE_TOJA, StmtHintData::Bool(true)),
            hint(HINT_NO_INDEX_MERGE, StmtHintData::None),
            hint(HINT_MAX_EXECUTION_TIME, StmtHintData::Uint(500)),
            hint("resource_group", StmtHintData::Str("rg1".to_owned())),
            hint("read_consistent_replica", StmtHintData::None),
            hint(HINT_STRAIGHT_JOIN, StmtHintData::None),
            hint(HINT_IGNORE_PLAN_CACHE, StmtHintData::None),
            hint(HINT_WRITE_SLOW_LOG, StmtHintData::None),
        ]);
        let hints = &parsed.hints;
        assert!(hints.query_has_hints);
        assert!(hints.has_mem_quota_hint);
        assert_eq!(hints.mem_quota_query, 1 << 30);
        assert!(hints.has_allow_in_subq_to_join_and_agg_hint);
        assert!(hints.allow_in_subq_to_join_and_agg);
        assert!(hints.no_index_merge_hint);
        assert!(hints.has_max_execution_time);
        assert_eq!(hints.max_execution_time, 500);
        assert!(hints.has_resource_group);
        assert_eq!(hints.resource_group, "rg1");
        assert!(hints.has_replica_read_hint);
        assert_eq!(hints.replica_read, 2);
        assert!(hints.straight_join_order);
        assert!(hints.ignore_plan_cache);
        assert!(hints.write_slow_log);
        assert_eq!(hints.force_nth_plan, -1);
        assert!(!hints.task_map_need_back_up());
        assert!(parsed.warnings.is_empty());
        // Every recognized statement hint contributes its offset, sorted.
        assert_eq!(parsed.offsets, vec![0, 1, 2, 3, 4, 5, 6]);
    }

    // Go's duplicate warnings, byte for byte, with last-definition wins.
    #[test]
    fn duplicates_warn_and_the_last_definition_wins() {
        let parsed = parse(&[
            hint(HINT_MEMORY_QUOTA, StmtHintData::Int(100)),
            hint(HINT_MEMORY_QUOTA, StmtHintData::Int(200)),
            hint(HINT_MAX_EXECUTION_TIME, StmtHintData::Uint(1)),
            hint(HINT_MAX_EXECUTION_TIME, StmtHintData::Uint(2)),
        ]);
        assert_eq!(parsed.hints.mem_quota_query, 200);
        assert_eq!(parsed.hints.max_execution_time, 2);
        assert!(parsed.warnings.contains(&
            "MEMORY_QUOTA() is defined more than once, only the last definition takes effect: MEMORY_QUOTA(200)".to_owned()));
        assert!(parsed.warnings.contains(&
            "MAX_EXECUTION_TIME() is defined more than once, only the last definition takes effect: MAX_EXECUTION_TIME(2)".to_owned()));
    }

    // MEMORY_QUOTA's three arms: negative invalidates, zero warns, positive
    // takes.
    #[test]
    fn memory_quota_validates_its_range() {
        let negative = parse(&[hint(HINT_MEMORY_QUOTA, StmtHintData::Int(-1))]);
        assert!(!negative.hints.has_mem_quota_hint);
        assert!(negative.warnings.contains(&
            "The use of MEMORY_QUOTA hint is invalid, valid usage: MEMORY_QUOTA(10 MB) or MEMORY_QUOTA(10 GB)".to_owned()));
        // The invalidated hint's offset is withdrawn.
        assert!(negative.offsets.is_empty());

        let zero = parse(&[hint(HINT_MEMORY_QUOTA, StmtHintData::Int(0))]);
        assert!(zero.hints.has_mem_quota_hint);
        assert_eq!(zero.hints.mem_quota_query, 0);
        assert!(zero
            .warnings
            .contains(&"Setting the MEMORY_QUOTA to 0 means no memory limit".to_owned()));
    }

    // NTH_PLAN: valid values arm the task-map backup; too-small values
    // disarm with a warning.
    #[test]
    fn nth_plan_clamps_small_values() {
        let valid = parse(&[hint("nth_plan", StmtHintData::Int(3))]);
        assert_eq!(valid.hints.force_nth_plan, 3);
        assert!(valid.hints.task_map_need_back_up());

        let small = parse(&[hint("nth_plan", StmtHintData::Int(0))]);
        assert_eq!(small.hints.force_nth_plan, -1);
        assert!(small
            .warnings
            .contains(&"the hintdata for NTH_PLAN() is too small, hint ignored".to_owned()));
    }

    // SET_VAR: the checker gates, the first setting wins, and the conflict
    // warning is ErrWarnConflictingHint's message.
    #[test]
    fn set_var_hints_check_and_deduplicate() {
        let set_var = |name: &str, value: &str| StmtHintInput {
            name: "set_var".to_owned(),
            display_name: "SET_VAR".to_owned(),
            tables: Vec::new(),
            data: StmtHintData::SetVar {
                var_name: name.to_owned(),
                value: value.to_owned(),
            },
        };
        let parsed = parse(&[
            set_var("max_execution_time", "100"),
            set_var("max_execution_time", "200"),
        ]);
        assert_eq!(
            parsed.hints.set_vars.get("max_execution_time"),
            Some(&"100".to_owned())
        );
        assert_eq!(
            parsed.warnings,
            vec![
                "Hint SET_VAR(max_execution_time=200) is ignored as conflicting/duplicated."
                    .to_owned()
            ]
        );

        // A refusing checker drops the hint, keeping its warning.
        let refuse = |_: &str, _: &str| (false, Some("not allowed".to_owned()));
        let refused = parse_stmt_hints(&[set_var("x", "1")], &refuse, &no_hypo, None, "test", 2);
        assert!(refused.hints.set_vars.is_empty());
        assert_eq!(refused.warnings, vec!["not allowed".to_owned()]);
    }

    // HYPO_INDEX builds a public hypothetical index through the checker.
    #[test]
    fn hypo_index_builds_through_the_checker() {
        let table = |name: &str| StmtHintTable {
            db_name: String::new(),
            table_name: CiString::new(name),
        };
        let input = StmtHintInput {
            name: "hypo_index".to_owned(),
            display_name: "HYPO_INDEX".to_owned(),
            tables: vec![table("t1"), table("idx_a"), table("a"), table("b")],
            data: StmtHintData::None,
        };
        let checker = |db: &str, tbl: &CiString, col: &CiString| {
            assert_eq!(db, "test");
            assert_eq!(tbl.lowercase(), "t1");
            match col.lowercase() {
                "a" => Ok(0),
                "b" => Ok(1),
                _ => Err(format!("unknown column {}", col.lowercase())),
            }
        };
        let parsed = parse_stmt_hints(
            std::slice::from_ref(&input),
            &accept_all,
            &checker,
            None,
            "test",
            2,
        );
        let index = &parsed.hints.hinted_hypo_indexes["test"]["t1"]["idx_a"];
        assert_eq!(index.columns.len(), 2);
        assert_eq!(index.columns.get(1).unwrap().read().offset, 1);
        assert_eq!(index.state, SchemaState::PUBLIC);
        // Clone drops the hypo indexes, as Go's Clone does.
        assert!(parsed.hints.clone_hints().hinted_hypo_indexes.is_empty());

        // A failing column fails the whole hint with the wrapped warning.
        let failing = |_: &str, _: &CiString, _: &CiString| Err("no such column".to_owned());
        let failed = parse_stmt_hints(&[input], &accept_all, &failing, None, "test", 2);
        assert!(failed.hints.hinted_hypo_indexes.is_empty());
        assert_eq!(
            failed.warnings,
            vec!["invalid HYPO_INDEX hint: no such column".to_owned()]
        );

        // Too few arguments is its own warning.
        let short = StmtHintInput {
            name: "hypo_index".to_owned(),
            display_name: "HYPO_INDEX".to_owned(),
            tables: vec![table("t1")],
            data: StmtHintData::None,
        };
        let parsed = parse_stmt_hints(&[short], &accept_all, &no_hypo, None, "test", 2);
        assert_eq!(
            parsed.warnings,
            vec![
                "Invalid HYPO_INDEX hint, valid usage: HYPO_INDEX(tableName, indexName, cols...)"
                    .to_owned()
            ]
        );
    }

    // The restricted-hint checker strips hints, warning only for the names
    // this parser owns the warning for.
    #[test]
    fn restricted_hints_are_stripped() {
        let checker = |name: &str| {
            if name == HINT_MEMORY_QUOTA || name == HINT_STRAIGHT_JOIN {
                Some(format!("hint {name} is restricted"))
            } else {
                None
            }
        };
        let parsed = parse_stmt_hints(
            &[
                hint(HINT_MEMORY_QUOTA, StmtHintData::Int(100)),
                // STRAIGHT_JOIN is filtered but warned elsewhere.
                hint(HINT_STRAIGHT_JOIN, StmtHintData::None),
                hint(HINT_NO_INDEX_MERGE, StmtHintData::None),
            ],
            &accept_all,
            &no_hypo,
            Some(&checker),
            "test",
            2,
        );
        assert!(!parsed.hints.has_mem_quota_hint);
        assert!(!parsed.hints.straight_join_order);
        assert!(parsed.hints.no_index_merge_hint);
        assert_eq!(
            parsed.warnings,
            vec!["hint memory_quota is restricted".to_owned()]
        );
        assert_eq!(parsed.hints.original_table_hints.len(), 1);

        // A fully filtered list returns early with ForceNthPlan at Go's
        // zero value, exactly like an empty input.
        let all = |_: &str| Some("restricted".to_owned());
        let stripped = parse_stmt_hints(
            &[hint(HINT_MEMORY_QUOTA, StmtHintData::Int(1))],
            &accept_all,
            &no_hypo,
            Some(&all),
            "test",
            2,
        );
        assert!(stripped.hints.query_has_hints);
        assert_eq!(stripped.hints.force_nth_plan, 0);
        assert!(stripped.hints.original_table_hints.is_empty());
    }
}
