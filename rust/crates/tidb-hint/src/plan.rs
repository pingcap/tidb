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

use tidb_ast::{Hint, HintKind, LeadingElement};

use crate::query_block::hint_tables;
use crate::statement::{filter_restricted, should_warn_restricted};
use crate::{HintWarning, QBHintHandler};

/// Go `HintFlagSemiJoinRewrite`.
pub const HINT_FLAG_SEMI_JOIN_REWRITE: u64 = 1;
/// Go `HintFlagNoDecorrelate`.
pub const HINT_FLAG_NO_DECORRELATE: u64 = 1 << 1;

/// Go `PreferHashAgg`.
pub const PREFER_HASH_AGG: u32 = 1 << 25;
/// Go `PreferStreamAgg`.
pub const PREFER_STREAM_AGG: u32 = 1 << 26;
/// Go `PreferMPP1PhaseAgg`.
pub const PREFER_MPP_1_PHASE_AGG: u32 = 1 << 27;
/// Go `PreferMPP2PhaseAgg`.
pub const PREFER_MPP_2_PHASE_AGG: u32 = 1 << 28;

/// Go `HintedTable`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HintedTable {
    /// Database name, defaulted during parsing.
    pub database_name: String,
    /// Table name or alias.
    pub table_name: String,
    /// Named partitions.
    pub partitions: Vec<String>,
    /// Query-block offset.
    pub select_offset: i32,
    /// Whether a consumer applied the hint.
    pub matched: bool,
}

impl HintedTable {
    /// Go `HintedTable.Match`.
    pub fn matches(&self, other: &Self) -> bool {
        self.select_offset == other.select_offset
            && self.table_name.eq_ignore_ascii_case(&other.table_name)
            && (self
                .database_name
                .eq_ignore_ascii_case(&other.database_name)
                || self.database_name == "*"
                || other.database_name == "*")
    }
}

/// Go `ast.IndexHintType` values used by `HintedIndex`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HintedIndexKind {
    /// USE_INDEX.
    Use,
    /// IGNORE_INDEX.
    Ignore,
    /// FORCE_INDEX.
    Force,
    /// ORDER_INDEX.
    Order,
    /// NO_ORDER_INDEX.
    NoOrder,
}

/// Go `HintedIndex`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HintedIndex {
    /// Database name.
    pub database_name: String,
    /// Table name.
    pub table_name: String,
    /// Named partitions.
    pub partitions: Vec<String>,
    /// Index hint type.
    pub kind: HintedIndexKind,
    /// Index names.
    pub index_names: Vec<String>,
    /// Whether index lookup should be pushed down.
    pub push_down_lookup: bool,
    /// Whether a consumer applied the hint.
    pub matched: bool,
}

impl HintedIndex {
    /// Go `HintedIndex.Match`.
    pub fn matches(&self, database_name: &str, table_name: &str) -> bool {
        self.table_name.eq_ignore_ascii_case(table_name)
            && (self.database_name.eq_ignore_ascii_case(database_name) || self.database_name == "*")
    }

    /// Go `HintedIndex.ShouldPushDownIndexLookUp`.
    pub fn should_push_down_index_lookup(&self) -> bool {
        self.kind == HintedIndexKind::Use && self.push_down_lookup
    }

    /// Go `HintedIndex.HintTypeString`.
    pub fn hint_type_string(&self) -> &'static str {
        match self.kind {
            HintedIndexKind::Use if self.push_down_lookup => "index_lookup_pushdown",
            HintedIndexKind::Use => "use_index",
            HintedIndexKind::Ignore => "ignore_index",
            HintedIndexKind::Force => "force_index",
            HintedIndexKind::Order | HintedIndexKind::NoOrder => "",
        }
    }

    /// Go `HintedIndex.IndexString`.
    pub fn index_string(&self) -> String {
        let mut value = format!("{}.{}", self.database_name, self.table_name);
        if !self.index_names.is_empty() {
            value.push_str(", ");
            value.push_str(&self.index_names.join(", "));
        }
        value
    }
}

/// Go `IndexJoinHints`.
#[derive(Clone, Debug, Default)]
pub struct IndexJoinHints {
    /// INL_JOIN tables.
    pub inlj_tables: Vec<HintedTable>,
    /// INL_HASH_JOIN tables.
    pub inlhj_tables: Vec<HintedTable>,
    /// INL_MERGE_JOIN tables.
    pub inlmj_tables: Vec<HintedTable>,
}

/// Go `PlanHints`.
#[derive(Clone, Debug, Default)]
pub struct PlanHints {
    /// Positive index-join hints.
    pub index_join: IndexJoinHints,
    /// Negative index-join hints.
    pub no_index_join: IndexJoinHints,
    /// HASH_JOIN tables.
    pub hash_join: Vec<HintedTable>,
    /// NO_HASH_JOIN tables.
    pub no_hash_join: Vec<HintedTable>,
    /// MERGE_JOIN tables.
    pub sort_merge_join: Vec<HintedTable>,
    /// NO_MERGE_JOIN tables.
    pub no_merge_join: Vec<HintedTable>,
    /// BROADCAST_JOIN tables.
    pub broadcast_join: Vec<HintedTable>,
    /// SHUFFLE_JOIN tables.
    pub shuffle_join: Vec<HintedTable>,
    /// Ordinary index hints.
    pub index_hint_list: Vec<HintedIndex>,
    /// USE_INDEX_MERGE hints.
    pub index_merge_hint_list: Vec<HintedIndex>,
    /// TiFlash storage hints.
    pub tiflash_tables: Vec<HintedTable>,
    /// TiKV storage hints.
    pub tikv_tables: Vec<HintedTable>,
    /// Flattened LEADING order.
    pub leading_join_order: Vec<HintedTable>,
    /// Recursive LEADING tree.
    pub leading_list: Option<Vec<LeadingElement>>,
    /// HASH_JOIN_BUILD tables.
    pub hash_join_build: Vec<HintedTable>,
    /// HASH_JOIN_PROBE tables.
    pub hash_join_probe: Vec<HintedTable>,
    /// NO_INDEX_LOOKUP_PUSHDOWN tables.
    pub no_index_lookup_pushdown: Vec<HintedTable>,
    /// Aggregation preference bits.
    pub prefer_agg_type: u32,
    /// Whether aggregation pushdown is preferred.
    pub prefer_agg_to_cop: bool,
    /// Whether LIMIT pushdown is preferred.
    pub prefer_limit_to_cop: bool,
    /// Whether CTE merge is enabled.
    pub cte_merge: bool,
    /// TIME_RANGE bounds.
    pub time_range: Option<(String, String)>,
    /// Whether straight join order is requested by a hint.
    pub straight_join_order: bool,
}

impl PlanHints {
    /// Go `MatchTableName`; either join side matching is sufficient.
    pub fn match_table_names(
        candidates: &[Option<&HintedTable>],
        hinted: &mut [HintedTable],
    ) -> bool {
        let mut matched = false;
        for candidate in candidates.iter().copied().flatten() {
            if let Some(entry) = hinted.iter_mut().find(|entry| entry.matches(candidate)) {
                entry.matched = true;
                matched = true;
            }
        }
        matched
    }

    /// Go `IfPreferMergeJoin`.
    pub fn prefer_merge_join(&mut self, tables: &[Option<&HintedTable>]) -> bool {
        Self::match_table_names(tables, &mut self.sort_merge_join)
    }

    /// Go `IfPreferBroadcastJoin`.
    pub fn prefer_broadcast_join(&mut self, tables: &[Option<&HintedTable>]) -> bool {
        Self::match_table_names(tables, &mut self.broadcast_join)
    }

    /// Go `IfPreferShuffleJoin`.
    pub fn prefer_shuffle_join(&mut self, tables: &[Option<&HintedTable>]) -> bool {
        Self::match_table_names(tables, &mut self.shuffle_join)
    }

    /// Go `IfPreferHashJoin`.
    pub fn prefer_hash_join(&mut self, tables: &[Option<&HintedTable>]) -> bool {
        Self::match_table_names(tables, &mut self.hash_join)
    }

    /// Go `IfPreferNoHashJoin`.
    pub fn prefer_no_hash_join(&mut self, tables: &[Option<&HintedTable>]) -> bool {
        Self::match_table_names(tables, &mut self.no_hash_join)
    }

    /// Go `IfPreferNoMergeJoin`.
    pub fn prefer_no_merge_join(&mut self, tables: &[Option<&HintedTable>]) -> bool {
        Self::match_table_names(tables, &mut self.no_merge_join)
    }

    /// Go `IfPreferHJBuild`.
    pub fn prefer_hash_join_build(&mut self, tables: &[Option<&HintedTable>]) -> bool {
        Self::match_table_names(tables, &mut self.hash_join_build)
    }

    /// Go `IfPreferHJProbe`.
    pub fn prefer_hash_join_probe(&mut self, tables: &[Option<&HintedTable>]) -> bool {
        Self::match_table_names(tables, &mut self.hash_join_probe)
    }

    /// Go `IfPreferINLJ`.
    pub fn prefer_index_join(&mut self, tables: &[Option<&HintedTable>]) -> bool {
        Self::match_table_names(tables, &mut self.index_join.inlj_tables)
    }

    /// Go `IfPreferINLHJ`.
    pub fn prefer_index_hash_join(&mut self, tables: &[Option<&HintedTable>]) -> bool {
        Self::match_table_names(tables, &mut self.index_join.inlhj_tables)
    }

    /// Go `IfPreferINLMJ`.
    pub fn prefer_index_merge_join(&mut self, tables: &[Option<&HintedTable>]) -> bool {
        Self::match_table_names(tables, &mut self.index_join.inlmj_tables)
    }

    /// Go `IfPreferNoIndexJoin`.
    pub fn prefer_no_index_join(&mut self, tables: &[Option<&HintedTable>]) -> bool {
        Self::match_table_names(tables, &mut self.no_index_join.inlj_tables)
    }

    /// Go `IfPreferNoIndexHashJoin`.
    pub fn prefer_no_index_hash_join(&mut self, tables: &[Option<&HintedTable>]) -> bool {
        Self::match_table_names(tables, &mut self.no_index_join.inlhj_tables)
    }

    /// Go `IfPreferNoIndexMergeJoin`.
    pub fn prefer_no_index_merge_join(&mut self, tables: &[Option<&HintedTable>]) -> bool {
        Self::match_table_names(tables, &mut self.no_index_join.inlmj_tables)
    }

    /// Go `IfPreferTiFlash`, including its returned value-copy behavior.
    pub fn prefer_tiflash(&mut self, table: Option<&HintedTable>) -> Option<HintedTable> {
        match_storage_table(table, &mut self.tiflash_tables)
    }

    /// Go `IfPreferTiKV`, including its returned value-copy behavior.
    pub fn prefer_tikv(&mut self, table: Option<&HintedTable>) -> Option<HintedTable> {
        match_storage_table(table, &mut self.tikv_tables)
    }
}

fn match_storage_table(
    table: Option<&HintedTable>,
    hints: &mut [HintedTable],
) -> Option<HintedTable> {
    let table = table?;
    for hint in hints {
        if hint.matches(table) {
            let returned_copy = hint.clone();
            hint.matched = true;
            return Some(returned_copy);
        }
    }
    None
}

/// Go `ParsePlanHints`.
#[allow(clippy::too_many_arguments)]
pub fn parse_plan_hints(
    hints: &[Hint],
    current_level: i32,
    current_database: &str,
    query_blocks: &QBHintHandler,
    straight_join_order: bool,
    handling_in_subquery: bool,
    handling_exists_subquery: bool,
    not_handling_subquery: bool,
) -> (PlanHints, u64, Vec<HintWarning>) {
    let (hints, mut warnings) = filter_restricted(hints, |hint| !should_warn_restricted(hint));
    let mut plan = PlanHints::default();
    let mut subquery_flags = 0;
    let mut leading_count = 0;

    for hint in &hints {
        let name = hint.name.to_ascii_lowercase();
        let tables = hint_tables(hint);
        if requires_table_names(&name) && tables.is_empty() {
            warnings.push(HintWarning::optimizer(format!(
                "Hint {} is inapplicable. Please specify the table names in the arguments.",
                hint.restore()
            )));
            continue;
        }
        match name.as_str() {
            "tidb_smj" | "merge_join" => plan.sort_merge_join.extend(table_infos(
                current_database,
                &name,
                &tables,
                query_blocks,
                current_level,
                &mut warnings,
            )),
            "tidb_bcj" | "broadcast_join" => plan.broadcast_join.extend(table_infos(
                current_database,
                &name,
                &tables,
                query_blocks,
                current_level,
                &mut warnings,
            )),
            "shuffle_join" => plan.shuffle_join.extend(table_infos(
                current_database,
                &name,
                &tables,
                query_blocks,
                current_level,
                &mut warnings,
            )),
            "tidb_inlj" | "inl_join" => plan.index_join.inlj_tables.extend(table_infos(
                current_database,
                &name,
                &tables,
                query_blocks,
                current_level,
                &mut warnings,
            )),
            "inl_hash_join" => plan.index_join.inlhj_tables.extend(table_infos(
                current_database,
                &name,
                &tables,
                query_blocks,
                current_level,
                &mut warnings,
            )),
            "inl_merge_join" => warnings.push(HintWarning::optimizer(
                "The INDEX MERGE JOIN hint is deprecated for usage, try other hints.",
            )),
            "tidb_hj" | "hash_join" => plan.hash_join.extend(table_infos(
                current_database,
                &name,
                &tables,
                query_blocks,
                current_level,
                &mut warnings,
            )),
            "no_hash_join" => plan.no_hash_join.extend(table_infos(
                current_database,
                &name,
                &tables,
                query_blocks,
                current_level,
                &mut warnings,
            )),
            "no_merge_join" => plan.no_merge_join.extend(table_infos(
                current_database,
                &name,
                &tables,
                query_blocks,
                current_level,
                &mut warnings,
            )),
            "no_index_join" | "no_index_hash_join" | "no_index_merge_join" => {
                let converted = table_infos(
                    current_database,
                    &name,
                    &tables,
                    query_blocks,
                    current_level,
                    &mut warnings,
                );
                match name.as_str() {
                    "no_index_join" => plan.no_index_join.inlj_tables.extend(converted),
                    "no_index_hash_join" => plan.no_index_join.inlhj_tables.extend(converted),
                    _ => plan.no_index_join.inlmj_tables.extend(converted),
                }
            }
            "mpp_1phase_agg" => plan.prefer_agg_type |= PREFER_MPP_1_PHASE_AGG,
            "mpp_2phase_agg" => plan.prefer_agg_type |= PREFER_MPP_2_PHASE_AGG,
            "hash_join_build" | "hash_join_probe" => {
                let converted = table_infos(
                    current_database,
                    &name,
                    &tables,
                    query_blocks,
                    current_level,
                    &mut warnings,
                );
                if name == "hash_join_build" {
                    plan.hash_join_build.extend(converted);
                } else {
                    plan.hash_join_probe.extend(converted);
                }
            }
            "hash_agg" => plan.prefer_agg_type |= PREFER_HASH_AGG,
            "stream_agg" => plan.prefer_agg_type |= PREFER_STREAM_AGG,
            "agg_to_cop" => plan.prefer_agg_to_cop = true,
            "no_index_lookup_pushdown" => {
                let HintKind::Index { table, indexes, .. } = &hint.kind else {
                    continue;
                };
                if !indexes.is_empty() {
                    warnings.push(HintWarning::optimizer(
                        "hint NO_INDEX_LOOKUP_PUSH_DOWN is inapplicable, only table name without indexes is supported",
                    ));
                    continue;
                }
                plan.no_index_lookup_pushdown.push(HintedTable {
                    database_name: table
                        .db_name
                        .clone()
                        .unwrap_or_else(|| current_database.to_owned()),
                    table_name: table.name.clone(),
                    ..HintedTable::default()
                });
            }
            "use_index"
            | "ignore_index"
            | "force_index"
            | "order_index"
            | "no_order_index"
            | "index_lookup_pushdown"
            | "use_index_merge" => {
                let HintKind::Index { table, indexes, .. } = &hint.kind else {
                    continue;
                };
                if name == "index_lookup_pushdown" && indexes.is_empty() {
                    warnings.push(HintWarning::optimizer(
                        "hint INDEX_LOOKUP_PUSH_DOWN is inapplicable, the index names should be specified",
                    ));
                    continue;
                }
                let entry = HintedIndex {
                    database_name: table
                        .db_name
                        .clone()
                        .unwrap_or_else(|| current_database.to_owned()),
                    table_name: table.name.clone(),
                    partitions: table.partitions.clone(),
                    kind: match name.as_str() {
                        "ignore_index" => HintedIndexKind::Ignore,
                        "force_index" => HintedIndexKind::Force,
                        "order_index" => HintedIndexKind::Order,
                        "no_order_index" => HintedIndexKind::NoOrder,
                        _ => HintedIndexKind::Use,
                    },
                    index_names: indexes.clone(),
                    push_down_lookup: name == "index_lookup_pushdown",
                    matched: false,
                };
                if name == "use_index_merge" {
                    plan.index_merge_hint_list.push(entry);
                } else {
                    plan.index_hint_list.push(entry);
                }
            }
            "read_from_storage" => {
                let HintKind::ReadFromStorage { groups, .. } = &hint.kind else {
                    continue;
                };
                for (store, group) in groups {
                    let refs = group.iter().collect::<Vec<_>>();
                    let converted = table_infos(
                        current_database,
                        &name,
                        &refs,
                        query_blocks,
                        current_level,
                        &mut warnings,
                    );
                    if store.eq_ignore_ascii_case("tiflash") {
                        plan.tiflash_tables.extend(converted);
                    } else if store.eq_ignore_ascii_case("tikv") {
                        plan.tikv_tables.extend(converted);
                    }
                }
            }
            "time_range" => {
                if let HintKind::TimeRange { from, to } = &hint.kind {
                    plan.time_range = Some((from.clone(), to.clone()));
                }
            }
            "limit_to_cop" => plan.prefer_limit_to_cop = true,
            "merge" => {
                if !tables.is_empty() {
                    warnings.push(HintWarning::optimizer(
                        "The MERGE hint is not used correctly, maybe it inputs a table name.",
                    ));
                } else {
                    plan.cte_merge = true;
                }
            }
            "leading" => {
                if leading_count == 0 {
                    plan.leading_join_order.extend(table_infos(
                        current_database,
                        &name,
                        &tables,
                        query_blocks,
                        current_level,
                        &mut warnings,
                    ));
                    if let HintKind::Leading { elements, .. } = &hint.kind {
                        plan.leading_list = Some(elements.clone());
                    }
                }
                leading_count += 1;
            }
            "semi_join_rewrite" => {
                if handling_exists_subquery || handling_in_subquery {
                    subquery_flags |= HINT_FLAG_SEMI_JOIN_REWRITE;
                } else {
                    warnings.push(HintWarning::optimizer(
                        "The SEMI_JOIN_REWRITE hint is not used correctly, maybe it's not in a subquery or the subquery is not IN/EXISTS clause.",
                    ));
                }
            }
            "no_decorrelate" => {
                if not_handling_subquery {
                    warnings.push(HintWarning::optimizer(
                        "NO_DECORRELATE() is inapplicable because it's not in an IN subquery, an EXISTS subquery, an ANY/ALL/SOME subquery or a scalar subquery.",
                    ));
                } else {
                    subquery_flags |= HINT_FLAG_NO_DECORRELATE;
                }
            }
            "straight_join" => plan.straight_join_order = true,
            _ => {}
        }
    }
    if leading_count > 1 || (leading_count > 0 && straight_join_order) {
        plan.leading_join_order.clear();
        if leading_count > 1 {
            warnings.push(HintWarning::optimizer(
                "We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid",
            ));
        } else {
            warnings.push(HintWarning::optimizer(
                "We can only use the straight_join hint, when we use the leading hint and straight_join hint at the same time, all leading hints will be invalid",
            ));
        }
    }
    (plan, subquery_flags, warnings)
}

fn requires_table_names(name: &str) -> bool {
    matches!(
        name,
        "tidb_smj"
            | "merge_join"
            | "tidb_inlj"
            | "inl_join"
            | "inl_hash_join"
            | "inl_merge_join"
            | "no_hash_join"
            | "no_merge_join"
            | "tidb_hj"
            | "hash_join"
            | "use_index"
            | "ignore_index"
            | "force_index"
            | "order_index"
            | "no_order_index"
            | "index_lookup_pushdown"
            | "use_index_merge"
            | "leading"
    )
}

fn table_infos(
    current_database: &str,
    hint_name: &str,
    tables: &[&tidb_ast::HintTable],
    query_blocks: &QBHintHandler,
    current_offset: i32,
    warnings: &mut Vec<HintWarning>,
) -> Vec<HintedTable> {
    let result = tables
        .iter()
        .map(|table| HintedTable {
            database_name: table
                .db_name
                .clone()
                .unwrap_or_else(|| current_database.to_owned()),
            table_name: table.name.clone(),
            partitions: table.partitions.clone(),
            select_offset: query_blocks.hint_offset(table.qb_name.as_deref(), current_offset),
            matched: false,
        })
        .collect::<Vec<_>>();
    if join_hint_disallows_partitions(hint_name)
        && result.iter().any(|table| !table.partitions.is_empty())
    {
        warnings.push(HintWarning::optimizer(format!(
            "Optimizer Hint {} is inapplicable on specified partitions",
            restore_join_hint(hint_name, &result)
        )));
        Vec::new()
    } else {
        result
    }
}

fn join_hint_disallows_partitions(name: &str) -> bool {
    matches!(
        name,
        "tidb_smj"
            | "merge_join"
            | "tidb_inlj"
            | "inl_join"
            | "inl_hash_join"
            | "inl_merge_join"
            | "tidb_hj"
            | "hash_join"
            | "leading"
    )
}

/// Go `Restore2JoinHint`.
pub fn restore_join_hint(hint_type: &str, tables: &[HintedTable]) -> String {
    if tables.is_empty() {
        return hint_type.to_ascii_uppercase();
    }
    format!(
        "/*+ {}({}) */",
        hint_type.to_ascii_uppercase(),
        tables
            .iter()
            .map(|table| {
                let mut value = table.table_name.to_ascii_lowercase();
                if !table.partitions.is_empty() {
                    value.push_str(" PARTITION(");
                    value.push_str(&table.partitions.join(", ").to_ascii_lowercase());
                    value.push(')');
                }
                value
            })
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// Go `Restore2IndexHint`.
pub fn restore_index_hint(hint_type: &str, hint: &HintedIndex) -> String {
    let mut value = format!(
        "/*+ {}({}",
        hint_type.to_ascii_uppercase(),
        hint.table_name.to_ascii_lowercase()
    );
    if !hint.partitions.is_empty() {
        value.push_str(" PARTITION(");
        value.push_str(&hint.partitions.join(", ").to_ascii_lowercase());
        value.push(')');
    }
    for (offset, index) in hint.index_names.iter().enumerate() {
        if offset > 0 {
            value.push(',');
        }
        value.push(' ');
        value.push_str(&index.to_ascii_lowercase());
    }
    value.push_str(") */");
    value
}

/// Go `Restore2StorageHint`.
pub fn restore_storage_hint(tiflash: &[HintedTable], tikv: &[HintedTable]) -> String {
    let restore_tables = |tables: &[HintedTable]| {
        tables
            .iter()
            .map(restore_table_argument)
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut value = "/*+ READ_FROM_STORAGE(".to_owned();
    if !tiflash.is_empty() {
        value.push_str("tiflash[");
        value.push_str(&restore_tables(tiflash));
        value.push(']');
        if !tikv.is_empty() {
            value.push_str(", ");
        }
    }
    if !tikv.is_empty() {
        value.push_str("tikv[");
        value.push_str(&restore_tables(tikv));
        value.push(']');
    }
    value.push_str(") */");
    value
}

fn restore_table_argument(table: &HintedTable) -> String {
    let mut value = table.table_name.to_ascii_lowercase();
    if !table.partitions.is_empty() {
        value.push_str(" PARTITION(");
        value.push_str(&table.partitions.join(", ").to_ascii_lowercase());
        value.push(')');
    }
    value
}

/// Go `ExtractUnmatchedTables`.
pub fn extract_unmatched_tables(tables: &[HintedTable]) -> Vec<String> {
    unmatched_table_names(tables)
}

/// Go `RemoveDuplicatedHints`, preserving the first structurally equal hint.
pub fn remove_duplicated_hints(hints: &[Hint]) -> Vec<Hint> {
    let mut seen = std::collections::HashSet::with_capacity(hints.len());
    let mut result = Vec::with_capacity(hints.len());
    for hint in hints {
        if seen.insert(crate::restore_table_optimizer_hint(hint)) {
            result.push(hint.clone());
        }
    }
    result
}

/// Go `CollectUnmatchedHintWarnings`.
pub fn collect_unmatched_hint_warnings(plan: &PlanHints) -> Vec<String> {
    let mut warnings = Vec::new();
    collect_unmatched_indexes(&mut warnings, &plan.index_hint_list, false);
    collect_unmatched_indexes(&mut warnings, &plan.index_merge_hint_list, true);
    for (kind, alias, tables) in [
        ("inl_join", "tidb_inlj", &plan.index_join.inlj_tables),
        ("inl_hash_join", "", &plan.index_join.inlhj_tables),
        ("inl_merge_join", "", &plan.index_join.inlmj_tables),
        ("merge_join", "tidb_smj", &plan.sort_merge_join),
        ("broadcast_join", "tidb_bcj", &plan.broadcast_join),
        ("shuffle_join", "shuffle_join", &plan.shuffle_join),
        ("hash_join", "tidb_hj", &plan.hash_join),
        ("hash_join_build", "", &plan.hash_join_build),
        ("hash_join_probe", "", &plan.hash_join_probe),
        ("leading", "", &plan.leading_join_order),
    ] {
        collect_unmatched_join(&mut warnings, kind, alias, tables);
    }
    let mut names = unmatched_table_names(&plan.tiflash_tables);
    names.extend(unmatched_table_names(&plan.tikv_tables));
    if !names.is_empty() {
        warnings.push(format!(
            "There are no matching table names for ({}) in optimizer hint {}. Maybe you can use the table alias name",
            names.join(", "),
            restore_storage_hint(&plan.tiflash_tables, &plan.tikv_tables)
        ));
    }
    warnings
}

fn collect_unmatched_indexes(warnings: &mut Vec<String>, hints: &[HintedIndex], index_merge: bool) {
    for hint in hints.iter().filter(|hint| !hint.matched) {
        let kind = if index_merge {
            "use_index_merge"
        } else {
            hint.hint_type_string()
        };
        warnings.push(format!(
            "{kind}({}) is inapplicable, check whether the table({}.{}) exists",
            hint.index_string(),
            hint.database_name,
            hint.table_name
        ));
    }
}

fn collect_unmatched_join(
    warnings: &mut Vec<String>,
    kind: &str,
    alias: &str,
    tables: &[HintedTable],
) {
    let names = unmatched_table_names(tables);
    if names.is_empty() {
        return;
    }
    let alias = if alias.is_empty() {
        String::new()
    } else {
        format!(" or {}", restore_join_hint(alias, tables))
    };
    warnings.push(format!(
        "There are no matching table names for ({}) in optimizer hint {}{alias}. Maybe you can use the table alias name",
        names.join(", "),
        restore_join_hint(kind, tables)
    ));
}

fn unmatched_table_names(tables: &[HintedTable]) -> Vec<String> {
    tables
        .iter()
        .filter(|table| !table.matched)
        .map(|table| table.table_name.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[deny(unused_must_use)]
    fn go_plan_api_returns_may_be_ignored_like_go() {
        let table = HintedTable::default();
        table.matches(&table);
        let index = HintedIndex {
            database_name: "db".to_owned(),
            table_name: "tbl".to_owned(),
            partitions: Vec::new(),
            kind: HintedIndexKind::Use,
            index_names: vec!["idx".to_owned()],
            push_down_lookup: true,
            matched: false,
        };
        index.matches("db", "tbl");
        index.should_push_down_index_lookup();
        index.hint_type_string();
        index.index_string();
        restore_join_hint("inl_join", &[]);
        restore_index_hint("use_index", &index);
        restore_storage_hint(&[], &[]);
        extract_unmatched_tables(&[table]);
        remove_duplicated_hints(&[]);
        collect_unmatched_hint_warnings(&PlanHints::default());
    }
}
