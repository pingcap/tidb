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

//! SEED of Go `pkg/util/hint`, covering `hint.go`'s table-level match
//! machinery: [`HintedTable`]/[`HintedIndex`] with their match rules,
//! [`PlanHints`] with the whole `IfPrefer*` family, and the hint-type/index
//! rendering the planner's warnings use.
//!
//! `ParsePlanHints` — the walk that builds a [`PlanHints`] from the hint
//! list — stays open with `hint_processor.go`; this module owns what the
//! planner consults once the struct exists. Go's `IfPrefer*` methods mutate
//! `Matched` on the hint entries through shared slices; the same writes
//! happen here through `&mut self`, which is the ownership-honest spelling
//! of the same contract.

use tidb_ast::CiString;
use tidb_ast::{IndexHint, IndexHintKind};

/// Go `HintUseIndex`.
pub const HINT_USE_INDEX: &str = "use_index";
/// Go `HintIgnoreIndex`.
pub const HINT_IGNORE_INDEX: &str = "ignore_index";
/// Go `HintForceIndex`.
pub const HINT_FORCE_INDEX: &str = "force_index";
/// Go `HintIndexLookUpPushDown`.
pub const HINT_INDEX_LOOKUP_PUSHDOWN: &str = "index_lookup_pushdown";

/// Go `HintedTable`: which table a hint should take effect on.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HintedTable {
    /// The database name.
    pub db_name: CiString,
    /// The table name.
    pub tbl_name: CiString,
    /// Partition information.
    pub partitions: Vec<CiString>,
    /// The select block offset of this hint.
    pub select_offset: i64,
    /// Whether this hint was applied successfully.
    pub matched: bool,
}

impl HintedTable {
    /// Go `HintedTable.Match`: same block offset, same table, and the
    /// database matches — with `*` on either side standing for any database
    /// (cross-db bindings, e.g. `*.t`).
    #[must_use]
    pub fn matches(&self, other: &Self) -> bool {
        self.select_offset == other.select_offset
            && self.tbl_name.lowercase() == other.tbl_name.lowercase()
            && (self.db_name.lowercase() == other.db_name.lowercase()
                || self.db_name.lowercase() == "*"
                || other.db_name.lowercase() == "*")
    }
}

/// Go `HintedIndex`: which index a hint should take effect on.
#[derive(Clone, Debug, PartialEq)]
pub struct HintedIndex {
    /// The database name.
    pub db_name: CiString,
    /// The table name.
    pub tbl_name: CiString,
    /// Partition information.
    pub partitions: Vec<CiString>,
    /// The original parser index-hint structure.
    pub index_hint: IndexHint,
    /// Whether to push down the index lookup.
    pub push_down_look_up: bool,
    /// Whether this hint was applied to a data source; an unmatched hint
    /// warns after building the statement.
    pub matched: bool,
}

impl HintedIndex {
    /// Go `HintedIndex.Match`: table equal, database equal or `*`
    /// (universal bindings).
    #[must_use]
    pub fn matches(&self, db_name: &CiString, tbl_name: &CiString) -> bool {
        self.tbl_name.lowercase() == tbl_name.lowercase()
            && (self.db_name.lowercase() == db_name.lowercase() || self.db_name.lowercase() == "*")
    }

    /// Go `ShouldPushDownIndexLookUp`.
    #[must_use]
    pub fn should_push_down_index_look_up(&self) -> bool {
        self.index_hint.kind == IndexHintKind::Use && self.push_down_look_up
    }

    /// Go `HintTypeString`.
    #[must_use]
    pub fn hint_type_string(&self) -> &'static str {
        match self.index_hint.kind {
            IndexHintKind::Use => {
                if self.push_down_look_up {
                    HINT_INDEX_LOOKUP_PUSHDOWN
                } else {
                    HINT_USE_INDEX
                }
            }
            IndexHintKind::Ignore => HINT_IGNORE_INDEX,
            IndexHintKind::Force => HINT_FORCE_INDEX,
        }
    }

    /// Go `IndexString`: `DBName.tableName[, indexNames]`, names printed in
    /// their original case and index names lowercased, as the source's
    /// `%s` of `CIStr` and `.L` reads produce.
    #[must_use]
    pub fn index_string(&self) -> String {
        let index_list: Vec<String> = self
            .index_hint
            .indexes
            .iter()
            .map(|name| name.to_lowercase())
            .collect();
        let index_list_string = if index_list.is_empty() {
            String::new()
        } else {
            format!(", {}", index_list.join(", "))
        };
        format!(
            "{}.{}{}",
            self.db_name.original(),
            self.tbl_name.original(),
            index_list_string
        )
    }
}

/// Go `IndexJoinHints`: the three index-join variants' table lists.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct IndexJoinHints {
    /// `inl_join` tables.
    pub inlj_tables: Vec<HintedTable>,
    /// `inl_hash_join` tables.
    pub inlhj_tables: Vec<HintedTable>,
    /// `inl_merge_join` tables.
    pub inlmj_tables: Vec<HintedTable>,
}

/// Go `HintTimeRange` (`ast.HintTimeRange`): the `time_range` hint's window.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HintTimeRange {
    /// The `FROM` text as written.
    pub from: String,
    /// The `TO` text as written.
    pub to: String,
}

/// Go `PlanHints`: the optimizer plan-choice hints.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PlanHints {
    /// Go `IndexJoin`.
    pub index_join: IndexJoinHints,
    /// Go `NoIndexJoin`.
    pub no_index_join: IndexJoinHints,
    /// Go `HashJoin`.
    pub hash_join: Vec<HintedTable>,
    /// Go `NoHashJoin`.
    pub no_hash_join: Vec<HintedTable>,
    /// Go `SortMergeJoin`.
    pub sort_merge_join: Vec<HintedTable>,
    /// Go `NoMergeJoin`.
    pub no_merge_join: Vec<HintedTable>,
    /// Go `BroadcastJoin`.
    pub broadcast_join: Vec<HintedTable>,
    /// Go `ShuffleJoin`.
    pub shuffle_join: Vec<HintedTable>,
    /// Go `IndexHintList`.
    pub index_hint_list: Vec<HintedIndex>,
    /// Go `IndexMergeHintList`.
    pub index_merge_hint_list: Vec<HintedIndex>,
    /// Go `TiFlashTables`.
    pub tiflash_tables: Vec<HintedTable>,
    /// Go `TiKVTables`.
    pub tikv_tables: Vec<HintedTable>,
    /// Go `LeadingJoinOrder`.
    pub leading_join_order: Vec<HintedTable>,
    /// Go `HJBuild`.
    pub hj_build: Vec<HintedTable>,
    /// Go `HJProbe`.
    pub hj_probe: Vec<HintedTable>,
    /// Go `NoIndexLookUpPushDown`.
    pub no_index_lookup_pushdown: Vec<HintedTable>,
    /// Go `PreferAggType`, a bit set of the aggregation-hint flags.
    pub prefer_agg_type: u64,
    /// Go `PreferAggToCop`.
    pub prefer_agg_to_cop: bool,
    /// Go `PreferLimitToCop`.
    pub prefer_limit_to_cop: bool,
    /// Go `CTEMerge`.
    pub cte_merge: bool,
    /// Go `TimeRangeHint`.
    pub time_range_hint: HintTimeRange,
    /// Go `StraightJoinOrder`.
    pub straight_join_order: bool,
}

/// Go `MatchTableName` over one hint list: any queried table matching any
/// entry marks that entry matched and answers true. Each queried table stops
/// at its first matching entry.
fn match_table_name(tables: &[Option<&HintedTable>], hint_tables: &mut [HintedTable]) -> bool {
    let mut hint_matched = false;
    for table in tables {
        let Some(table) = table else { continue };
        for entry in hint_tables.iter_mut() {
            if entry.matches(table) {
                entry.matched = true;
                hint_matched = true;
                break;
            }
        }
    }
    hint_matched
}

/// Go's private `matchTiKVOrTiFlash`: the first matching entry is marked and
/// a copy of it answered.
fn match_engine_table(
    table_name: Option<&HintedTable>,
    hint_tables: &mut [HintedTable],
) -> Option<HintedTable> {
    let table_name = table_name?;
    for entry in hint_tables.iter_mut() {
        if entry.matches(table_name) {
            // Go copies the loop variable before marking the slice entry, so
            // the answered copy still reads unmatched.
            let copy = entry.clone();
            entry.matched = true;
            return Some(copy);
        }
    }
    None
}

macro_rules! if_prefer {
    ($(#[$doc:meta])* $method:ident, $($field:ident).+) => {
        $(#[$doc])*
        pub fn $method(&mut self, table_names: &[Option<&HintedTable>]) -> bool {
            match_table_name(table_names, &mut self.$($field).+)
        }
    };
}

impl PlanHints {
    if_prefer!(
        /// Go `IfPreferMergeJoin`.
        if_prefer_merge_join,
        sort_merge_join
    );
    if_prefer!(
        /// Go `IfPreferBroadcastJoin`.
        if_prefer_broadcast_join,
        broadcast_join
    );
    if_prefer!(
        /// Go `IfPreferShuffleJoin`.
        if_prefer_shuffle_join,
        shuffle_join
    );
    if_prefer!(
        /// Go `IfPreferHashJoin`.
        if_prefer_hash_join,
        hash_join
    );
    if_prefer!(
        /// Go `IfPreferNoHashJoin`.
        if_prefer_no_hash_join,
        no_hash_join
    );
    if_prefer!(
        /// Go `IfPreferNoMergeJoin`.
        if_prefer_no_merge_join,
        no_merge_join
    );
    if_prefer!(
        /// Go `IfPreferHJBuild`.
        if_prefer_hj_build,
        hj_build
    );
    if_prefer!(
        /// Go `IfPreferHJProbe`.
        if_prefer_hj_probe,
        hj_probe
    );
    if_prefer!(
        /// Go `IfPreferINLJ`.
        if_prefer_inlj,
        index_join.inlj_tables
    );
    if_prefer!(
        /// Go `IfPreferINLHJ`.
        if_prefer_inlhj,
        index_join.inlhj_tables
    );
    if_prefer!(
        /// Go `IfPreferINLMJ`.
        if_prefer_inlmj,
        index_join.inlmj_tables
    );
    if_prefer!(
        /// Go `IfPreferNoIndexJoin`.
        if_prefer_no_index_join,
        no_index_join.inlj_tables
    );
    if_prefer!(
        /// Go `IfPreferNoIndexHashJoin`.
        if_prefer_no_index_hash_join,
        no_index_join.inlhj_tables
    );
    if_prefer!(
        /// Go `IfPreferNoIndexMergeJoin`.
        if_prefer_no_index_merge_join,
        no_index_join.inlmj_tables
    );

    /// Go `IfPreferTiFlash`.
    pub fn if_prefer_tiflash(&mut self, table_name: Option<&HintedTable>) -> Option<HintedTable> {
        match_engine_table(table_name, &mut self.tiflash_tables)
    }

    /// Go `IfPreferTiKV`.
    pub fn if_prefer_tikv(&mut self, table_name: Option<&HintedTable>) -> Option<HintedTable> {
        match_engine_table(table_name, &mut self.tikv_tables)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::IndexHintScope;

    fn table(db: &str, tbl: &str, offset: i64) -> HintedTable {
        HintedTable {
            db_name: CiString::new(db),
            tbl_name: CiString::new(tbl),
            partitions: Vec::new(),
            select_offset: offset,
            matched: false,
        }
    }

    // Go `HintedTable.Match`: offset, table, and the `*` database wildcard
    // on either side.
    #[test]
    fn hinted_tables_match_with_wildcards() {
        let hint = table("test", "T1", 1);
        assert!(hint.matches(&table("TEST", "t1", 1)));
        assert!(!hint.matches(&table("test", "t1", 2)));
        assert!(!hint.matches(&table("test", "t2", 1)));
        assert!(!hint.matches(&table("other", "t1", 1)));
        assert!(table("*", "t1", 1).matches(&table("any", "t1", 1)));
        assert!(table("any", "t1", 1).matches(&table("*", "t1", 1)));
    }

    // Go `MatchTableName` marks the first matching entry per queried table
    // and reports whether anything matched.
    #[test]
    fn matching_marks_entries_and_reports() {
        let mut hints = PlanHints {
            sort_merge_join: vec![table("test", "t1", 1), table("test", "t2", 1)],
            ..PlanHints::default()
        };
        let query_table = table("test", "t2", 1);
        assert!(hints.if_prefer_merge_join(&[Some(&query_table), None]));
        assert!(!hints.sort_merge_join[0].matched);
        assert!(hints.sort_merge_join[1].matched);

        // A table from another block matches nothing and marks nothing.
        let other_block = table("test", "t1", 9);
        assert!(!hints.if_prefer_hash_join(&[Some(&other_block)]));
    }

    // Go's engine match answers a copy of the entry while marking the list.
    #[test]
    fn engine_preferences_answer_the_matched_entry() {
        let mut hints = PlanHints {
            tiflash_tables: vec![table("test", "t1", 1)],
            tikv_tables: vec![table("test", "t2", 1)],
            ..PlanHints::default()
        };
        let queried = table("test", "t1", 1);
        let matched = hints.if_prefer_tiflash(Some(&queried)).unwrap();
        assert_eq!(matched.tbl_name.lowercase(), "t1");
        assert!(hints.tiflash_tables[0].matched);
        // The copy reflects the pre-mark state, as Go's `&tbl` copy does.
        assert!(!matched.matched);

        assert!(hints.if_prefer_tikv(Some(&queried)).is_none());
        assert!(hints.if_prefer_tikv(None).is_none());
    }

    // Go `HintedIndex`: match rules, push-down predicate, and the rendered
    // hint-type/index strings.
    #[test]
    fn hinted_indexes_match_and_render() {
        let index = HintedIndex {
            db_name: CiString::new("Test"),
            tbl_name: CiString::new("T1"),
            partitions: Vec::new(),
            index_hint: IndexHint {
                kind: IndexHintKind::Use,
                scope: IndexHintScope::All,
                indexes: vec!["Idx_A".to_owned(), "idx_b".to_owned()],
            },
            push_down_look_up: false,
            matched: false,
        };
        assert!(index.matches(&CiString::new("TEST"), &CiString::new("t1")));
        assert!(!index.matches(&CiString::new("test"), &CiString::new("t2")));
        let universal = HintedIndex {
            db_name: CiString::new("*"),
            ..index.clone()
        };
        assert!(universal.matches(&CiString::new("anything"), &CiString::new("t1")));

        assert_eq!(index.hint_type_string(), HINT_USE_INDEX);
        assert!(!index.should_push_down_index_look_up());
        // Original case for names, lowercase for indexes.
        assert_eq!(index.index_string(), "Test.T1, idx_a, idx_b");

        let pushdown = HintedIndex {
            push_down_look_up: true,
            ..index.clone()
        };
        assert_eq!(pushdown.hint_type_string(), HINT_INDEX_LOOKUP_PUSHDOWN);
        assert!(pushdown.should_push_down_index_look_up());

        let ignore = HintedIndex {
            index_hint: IndexHint {
                kind: IndexHintKind::Ignore,
                scope: IndexHintScope::All,
                indexes: Vec::new(),
            },
            ..index.clone()
        };
        assert_eq!(ignore.hint_type_string(), HINT_IGNORE_INDEX);
        // No indexes: just DBName.tableName.
        assert_eq!(ignore.index_string(), "Test.T1");

        let force = HintedIndex {
            index_hint: IndexHint {
                kind: IndexHintKind::Force,
                scope: IndexHintScope::All,
                indexes: Vec::new(),
            },
            push_down_look_up: true,
            ..index
        };
        assert_eq!(force.hint_type_string(), HINT_FORCE_INDEX);
        // Push-down only applies to USE.
        assert!(!force.should_push_down_index_look_up());
    }
}
