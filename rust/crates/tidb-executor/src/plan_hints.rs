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

/// The index-hint kind Go stores as `ast.IndexHintType` — five values,
/// two more than the SQL-level `USE/IGNORE/FORCE` grammar carries, because
/// `ORDER_INDEX`/`NO_ORDER_INDEX` exist only as optimizer hints.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanIndexHintKind {
    /// Go `ast.HintUse`.
    Use,
    /// Go `ast.HintIgnore`.
    Ignore,
    /// Go `ast.HintForce`.
    Force,
    /// Go `ast.HintOrderIndex`.
    OrderIndex,
    /// Go `ast.HintNoOrderIndex`.
    NoOrderIndex,
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
    /// Go `IndexHint.IndexNames`.
    pub index_names: Vec<CiString>,
    /// Go `IndexHint.HintType`; the scope is always Go `HintForScan` here.
    pub kind: PlanIndexHintKind,
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
        self.kind == PlanIndexHintKind::Use && self.push_down_look_up
    }

    /// Go `HintTypeString`; the order-index kinds render empty, exactly as
    /// the source's uncovered switch arms do.
    #[must_use]
    pub fn hint_type_string(&self) -> &'static str {
        match self.kind {
            PlanIndexHintKind::Use => {
                if self.push_down_look_up {
                    HINT_INDEX_LOOKUP_PUSHDOWN
                } else {
                    HINT_USE_INDEX
                }
            }
            PlanIndexHintKind::Ignore => HINT_IGNORE_INDEX,
            PlanIndexHintKind::Force => HINT_FORCE_INDEX,
            PlanIndexHintKind::OrderIndex | PlanIndexHintKind::NoOrderIndex => "",
        }
    }

    /// Go `IndexString`: `DBName.tableName[, indexNames]`, names printed in
    /// their original case and index names lowercased, as the source's
    /// `%s` of `CIStr` and `.L` reads produce.
    #[must_use]
    pub fn index_string(&self) -> String {
        let index_list: Vec<&str> = self.index_names.iter().map(CiString::lowercase).collect();
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
    /// Go `LeadingList`: the recursive `LEADING` structure when one was
    /// written.
    pub leading_list: Option<Vec<tidb_ast::LeadingElement>>,
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
            index_names: vec![CiString::new("Idx_A"), CiString::new("idx_b")],
            kind: PlanIndexHintKind::Use,
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
            kind: PlanIndexHintKind::Ignore,
            index_names: Vec::new(),
            ..index.clone()
        };
        assert_eq!(ignore.hint_type_string(), HINT_IGNORE_INDEX);
        // No indexes: just DBName.tableName.
        assert_eq!(ignore.index_string(), "Test.T1");

        let force = HintedIndex {
            kind: PlanIndexHintKind::Force,
            push_down_look_up: true,
            ..index.clone()
        };
        assert_eq!(force.hint_type_string(), HINT_FORCE_INDEX);
        // Push-down only applies to USE.
        assert!(!force.should_push_down_index_look_up());

        // The order kinds render empty, as Go's uncovered arms do.
        let order = HintedIndex {
            kind: PlanIndexHintKind::OrderIndex,
            ..index
        };
        assert_eq!(order.hint_type_string(), "");
    }

    fn plan_hint(name: &str, tables: Vec<PlanHintTable>) -> PlanHintInput {
        PlanHintInput {
            name: name.to_owned(),
            tables,
            indexes: Vec::new(),
            data: PlanHintData::None,
            restored: format!("{}()", name.to_uppercase()),
        }
    }

    fn plan_table(name: &str) -> PlanHintTable {
        PlanHintTable {
            db_name: CiString::new(""),
            table_name: CiString::new(name),
            qb_name: String::new(),
            partitions: Vec::new(),
        }
    }

    fn parse(hints: &[PlanHintInput], straight: bool) -> (PlanHints, u64, Vec<String>) {
        let handler = crate::qb_hint::QbHintHandler::new();
        let mut warnings = Vec::new();
        let (p, flags) = parse_plan_hints(
            hints,
            1,
            "test",
            &handler,
            straight,
            false,
            false,
            true,
            None,
            &mut warnings,
        );
        (p, flags, warnings)
    }

    // Table-requiring hints without tables warn with the restored text.
    #[test]
    fn table_hints_without_tables_warn() {
        let (p, _, warnings) = parse(&[plan_hint("merge_join", vec![])], false);
        assert!(p.sort_merge_join.is_empty());
        assert_eq!(
            warnings,
            vec![
                "Hint MERGE_JOIN() is inapplicable. Please specify the table names in the arguments."
                    .to_owned()
            ]
        );
    }

    // Join tables resolve the default database and block offset; partitions
    // invalidate the join hints that cannot honor them.
    #[test]
    fn join_tables_resolve_and_partition_qualifiers_invalidate() {
        let (p, _, warnings) = parse(&[plan_hint("merge_join", vec![plan_table("t1")])], false);
        assert_eq!(p.sort_merge_join.len(), 1);
        assert_eq!(p.sort_merge_join[0].db_name.lowercase(), "test");
        assert_eq!(p.sort_merge_join[0].select_offset, 1);
        assert!(warnings.is_empty());

        let partitioned = PlanHintTable {
            partitions: vec![CiString::new("p0")],
            ..plan_table("t1")
        };
        let (p, _, warnings) = parse(&[plan_hint("hash_join", vec![partitioned])], false);
        assert!(p.hash_join.is_empty());
        assert_eq!(
            warnings,
            vec![
                "Optimizer Hint /*+ HASH_JOIN(t1 PARTITION(p0)) */ is inapplicable on specified partitions"
                    .to_owned()
            ]
        );
    }

    // The deprecated INL_MERGE_JOIN warns instead of collecting tables.
    #[test]
    fn inl_merge_join_is_deprecated() {
        let (p, _, warnings) = parse(
            &[plan_hint("inl_merge_join", vec![plan_table("t1")])],
            false,
        );
        assert!(p.index_join.inlmj_tables.is_empty());
        assert_eq!(
            warnings,
            vec!["The INDEX MERGE JOIN hint is deprecated for usage, try other hints.".to_owned()]
        );
    }

    // Index hints build HintedIndex entries, with push-down demanding names.
    #[test]
    fn index_hints_build_entries() {
        let mut with_index = plan_hint("use_index", vec![plan_table("t1")]);
        with_index.indexes = vec![CiString::new("idx_a")];
        let (p, _, warnings) = parse(&[with_index], false);
        assert_eq!(p.index_hint_list.len(), 1);
        assert_eq!(p.index_hint_list[0].kind, PlanIndexHintKind::Use);
        assert_eq!(p.index_hint_list[0].db_name.lowercase(), "test");
        assert!(warnings.is_empty());

        let bare_pushdown = plan_hint("index_lookup_pushdown", vec![plan_table("t1")]);
        let (p, _, warnings) = parse(&[bare_pushdown], false);
        assert!(p.index_hint_list.is_empty());
        assert_eq!(
            warnings,
            vec![
                "hint INDEX_LOOKUP_PUSH_DOWN is inapplicable, the index names should be specified"
                    .to_owned()
            ]
        );

        let mut order = plan_hint("order_index", vec![plan_table("t1")]);
        order.indexes = vec![CiString::new("idx_a")];
        let (p, _, _) = parse(&[order], false);
        assert_eq!(p.index_hint_list[0].kind, PlanIndexHintKind::OrderIndex);
    }

    // read_from_storage routes by engine label; aggregation hints set bits.
    #[test]
    fn storage_and_aggregation_hints() {
        let mut tiflash = plan_hint("read_from_storage", vec![plan_table("t1")]);
        tiflash.data = PlanHintData::Storage("tiflash".to_owned());
        let mut tikv = plan_hint("read_from_storage", vec![plan_table("t2")]);
        tikv.data = PlanHintData::Storage("tikv".to_owned());
        let (p, _, _) = parse(
            &[
                tiflash,
                tikv,
                plan_hint("hash_agg", vec![]),
                plan_hint("agg_to_cop", vec![]),
                plan_hint("limit_to_cop", vec![]),
            ],
            false,
        );
        assert_eq!(p.tiflash_tables.len(), 1);
        assert_eq!(p.tikv_tables.len(), 1);
        assert_eq!(p.prefer_agg_type & PREFER_HASH_AGG, PREFER_HASH_AGG);
        assert!(p.prefer_agg_to_cop);
        assert!(p.prefer_limit_to_cop);
    }

    // The subquery flags gate on the caller's context, with Go's messages.
    #[test]
    fn subquery_hints_gate_on_context() {
        let (_, flags, warnings) = parse(&[plan_hint("semi_join_rewrite", vec![])], false);
        assert_eq!(flags, 0);
        assert!(warnings[0].contains("SEMI_JOIN_REWRITE hint is not used correctly"));

        let handler = crate::qb_hint::QbHintHandler::new();
        let mut warnings = Vec::new();
        let (_, flags) = parse_plan_hints(
            &[
                plan_hint("semi_join_rewrite", vec![]),
                plan_hint("no_decorrelate", vec![]),
            ],
            1,
            "test",
            &handler,
            false,
            true,
            false,
            false,
            None,
            &mut warnings,
        );
        assert_eq!(
            flags,
            HINT_FLAG_SEMI_JOIN_REWRITE | HINT_FLAG_NO_DECORRELATE
        );
        assert!(warnings.is_empty());
    }

    // Multiple leading hints, or leading plus straight_join, invalidate all
    // leading hints with the source's messages.
    #[test]
    fn leading_hints_invalidate_on_conflict() {
        let leading = |t: &str| plan_hint("leading", vec![plan_table(t)]);
        let (p, _, warnings) = parse(&[leading("t1"), leading("t2")], false);
        assert!(p.leading_join_order.is_empty());
        assert!(warnings[0].contains("one leading hint at most"));

        let (p, _, warnings) = parse(&[leading("t1")], true);
        assert!(p.leading_join_order.is_empty());
        assert!(warnings[0].contains("only use the straight_join hint"));

        let (p, _, warnings) = parse(&[leading("t1")], false);
        assert_eq!(p.leading_join_order.len(), 1);
        assert!(warnings.is_empty());
    }
}

/// Go `HintFlagSemiJoinRewrite`.
pub const HINT_FLAG_SEMI_JOIN_REWRITE: u64 = 1 << 0;
/// Go `HintFlagNoDecorrelate`.
pub const HINT_FLAG_NO_DECORRELATE: u64 = 1 << 1;

/// One table argument of a plan hint (Go `ast.HintTable`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PlanHintTable {
    /// `DBName`; empty when unqualified.
    pub db_name: CiString,
    /// `TableName`.
    pub table_name: CiString,
    /// `QBName.L`; empty when absent.
    pub qb_name: String,
    /// `PartitionList`.
    pub partitions: Vec<CiString>,
}

/// The typed payload plan hints carry (Go `HintData`).
#[derive(Clone, Debug, Default, PartialEq)]
pub enum PlanHintData {
    /// No payload.
    #[default]
    None,
    /// `READ_FROM_STORAGE`'s engine label, lowercased.
    Storage(String),
    /// `TIME_RANGE`'s window.
    TimeRange(HintTimeRange),
    /// `LEADING`'s recursive list.
    Leading(Vec<tidb_ast::LeadingElement>),
}

/// A uniform view of Go `ast.TableOptimizerHint` for plan-hint parsing.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PlanHintInput {
    /// `HintName.L`.
    pub name: String,
    /// `Tables`.
    pub tables: Vec<PlanHintTable>,
    /// `Indexes`.
    pub indexes: Vec<CiString>,
    /// `HintData`.
    pub data: PlanHintData,
    /// The hint's restored text, used verbatim by the no-tables warning
    /// (Go calls `hint.Restore` there).
    pub restored: String,
}

/// Go's private `restore2TableHint`.
fn restore2_table_hint(hint_tables: &[HintedTable]) -> String {
    let mut buffer = String::new();
    for (i, table) in hint_tables.iter().enumerate() {
        buffer.push_str(table.tbl_name.lowercase());
        if !table.partitions.is_empty() {
            buffer.push_str(" PARTITION(");
            for (j, partition) in table.partitions.iter().enumerate() {
                if j > 0 {
                    buffer.push_str(", ");
                }
                buffer.push_str(partition.lowercase());
            }
            buffer.push(')');
        }
        if i < hint_tables.len() - 1 {
            buffer.push_str(", ");
        }
    }
    buffer
}

/// Go `Restore2JoinHint`.
#[must_use]
pub fn restore2_join_hint(hint_type: &str, hint_tables: &[HintedTable]) -> String {
    if hint_tables.is_empty() {
        return hint_type.to_uppercase();
    }
    format!(
        "/*+ {}({}) */",
        hint_type.to_uppercase(),
        restore2_table_hint(hint_tables)
    )
}

/// Go `Restore2IndexHint`.
#[must_use]
pub fn restore2_index_hint(hint_type: &str, hint_index: &HintedIndex) -> String {
    let table = HintedTable {
        db_name: hint_index.db_name.clone(),
        tbl_name: hint_index.tbl_name.clone(),
        partitions: hint_index.partitions.clone(),
        select_offset: 0,
        matched: false,
    };
    let mut buffer = format!(
        "/*+ {}({}",
        hint_type.to_uppercase(),
        restore2_table_hint(std::slice::from_ref(&table))
    );
    if !hint_index.index_names.is_empty() {
        buffer.push(' ');
        let names: Vec<&str> = hint_index
            .index_names
            .iter()
            .map(CiString::lowercase)
            .collect();
        buffer.push_str(&names.join(", "));
    }
    buffer.push_str(") */");
    buffer
}

/// Go's private `tableNames2HintTableInfo`: resolves each table's block
/// offset through the handler and rejects partition-qualified tables for the
/// join hints that cannot honor them.
fn table_names_to_hinted_tables(
    current_db: &str,
    hint_name: &str,
    hint_tables: &[PlanHintTable],
    handler: &crate::qb_hint::QbHintHandler,
    current_offset: i64,
    warnings: &mut Vec<String>,
) -> Vec<HintedTable> {
    if hint_tables.is_empty() {
        return Vec::new();
    }
    let mut infos = Vec::with_capacity(hint_tables.len());
    let mut is_inapplicable = false;
    for hint_table in hint_tables {
        let mut info = HintedTable {
            db_name: hint_table.db_name.clone(),
            tbl_name: hint_table.table_name.clone(),
            partitions: hint_table.partitions.clone(),
            select_offset: handler.hint_offset(&hint_table.qb_name, current_offset),
            matched: false,
        };
        if info.db_name.lowercase().is_empty() {
            info.db_name = CiString::new(current_db);
        }
        if matches!(
            hint_name,
            TIDB_MERGE_JOIN
                | HINT_SMJ
                | TIDB_INDEX_NESTED_LOOP_JOIN
                | HINT_INLJ
                | HINT_INLHJ
                | HINT_INLMJ
                | TIDB_HASH_JOIN
                | HINT_HJ
                | HINT_LEADING
        ) && !info.partitions.is_empty()
        {
            is_inapplicable = true;
        }
        infos.push(info);
    }
    if is_inapplicable {
        warnings.push(format!(
            "Optimizer Hint {} is inapplicable on specified partitions",
            restore2_join_hint(hint_name, &infos)
        ));
        return Vec::new();
    }
    infos
}

/// Go hint-name constants consumed by `ParsePlanHints`.
pub const TIDB_MERGE_JOIN: &str = "tidb_smj";
/// Go `HintSMJ`.
pub const HINT_SMJ: &str = "merge_join";
/// Go `TiDBBroadCastJoin`.
pub const TIDB_BROADCAST_JOIN: &str = "tidb_bcj";
/// Go `HintBCJ`.
pub const HINT_BCJ: &str = "broadcast_join";
/// Go `HintShuffleJoin`.
pub const HINT_SHUFFLE_JOIN: &str = "shuffle_join";
/// Go `TiDBIndexNestedLoopJoin`.
pub const TIDB_INDEX_NESTED_LOOP_JOIN: &str = "tidb_inlj";
/// Go `HintINLJ`.
pub const HINT_INLJ: &str = "inl_join";
/// Go `HintINLHJ`.
pub const HINT_INLHJ: &str = "inl_hash_join";
/// Go `HintINLMJ` (deprecated in usage).
pub const HINT_INLMJ: &str = "inl_merge_join";
/// Go `TiDBHashJoin`.
pub const TIDB_HASH_JOIN: &str = "tidb_hj";
/// Go `HintHJ`.
pub const HINT_HJ: &str = "hash_join";
/// Go `HintLeading`.
pub const HINT_LEADING: &str = "leading";

/// Go `ParsePlanHints`, over the uniform view. The restricted-hint filter
/// warns only for the names `ParseStmtHints` does NOT own, mirroring the
/// source's complemented predicate; warnings accumulate in order.
#[expect(clippy::too_many_lines, reason = "one Go function, kept whole")]
#[expect(clippy::too_many_arguments, reason = "the Go signature's arity")]
#[expect(clippy::fn_params_excessive_bools, reason = "the Go signature's flags")]
pub fn parse_plan_hints(
    hints: &[PlanHintInput],
    current_level: i64,
    current_db: &str,
    hint_processor: &crate::qb_hint::QbHintHandler,
    straight_join_order: bool,
    handling_in_subquery: bool,
    handling_exists_subquery: bool,
    not_handling_subquery: bool,
    restricted_hint_checker: Option<crate::stmt_hints::RestrictedHintChecker<'_>>,
    warnings: &mut Vec<String>,
) -> (PlanHints, u64) {
    let mut filtered: Vec<&PlanHintInput> = Vec::with_capacity(hints.len());
    if let Some(checker) = restricted_hint_checker {
        for hint in hints {
            if let Some(warning) = checker(&hint.name) {
                if !crate::stmt_hints::should_warn_restricted(&hint.name) {
                    warnings.push(warning);
                }
                continue;
            }
            filtered.push(hint);
        }
    } else {
        filtered.extend(hints.iter());
    }

    let mut p = PlanHints::default();
    let mut sub_query_hint_flags = 0_u64;
    let mut leading_hint_cnt = 0;
    let resolve = |tables: &[PlanHintTable], name: &str, warnings: &mut Vec<String>| {
        table_names_to_hinted_tables(
            current_db,
            name,
            tables,
            hint_processor,
            current_level,
            warnings,
        )
    };

    for hint in &filtered {
        // The hints that require table names warn and skip without any.
        if matches!(
            hint.name.as_str(),
            TIDB_MERGE_JOIN
                | HINT_SMJ
                | TIDB_INDEX_NESTED_LOOP_JOIN
                | HINT_INLJ
                | HINT_INLHJ
                | HINT_INLMJ
                | "no_hash_join"
                | "no_merge_join"
                | TIDB_HASH_JOIN
                | HINT_HJ
                | HINT_USE_INDEX
                | HINT_IGNORE_INDEX
                | HINT_FORCE_INDEX
                | "order_index"
                | "no_order_index"
                | HINT_INDEX_LOOKUP_PUSHDOWN
                | "use_index_merge"
                | HINT_LEADING
        ) && hint.tables.is_empty()
        {
            warnings.push(format!(
                "Hint {} is inapplicable. Please specify the table names in the arguments.",
                hint.restored
            ));
            continue;
        }

        match hint.name.as_str() {
            TIDB_MERGE_JOIN | HINT_SMJ => {
                p.sort_merge_join
                    .extend(resolve(&hint.tables, &hint.name, warnings))
            }
            TIDB_BROADCAST_JOIN | HINT_BCJ => {
                p.broadcast_join
                    .extend(resolve(&hint.tables, &hint.name, warnings))
            }
            HINT_SHUFFLE_JOIN => p
                .shuffle_join
                .extend(resolve(&hint.tables, &hint.name, warnings)),
            TIDB_INDEX_NESTED_LOOP_JOIN | HINT_INLJ => {
                p.index_join
                    .inlj_tables
                    .extend(resolve(&hint.tables, &hint.name, warnings))
            }
            HINT_INLHJ => {
                p.index_join
                    .inlhj_tables
                    .extend(resolve(&hint.tables, &hint.name, warnings))
            }
            HINT_INLMJ => {
                warnings.push(
                    "The INDEX MERGE JOIN hint is deprecated for usage, try other hints."
                        .to_owned(),
                );
            }
            TIDB_HASH_JOIN | HINT_HJ => {
                p.hash_join
                    .extend(resolve(&hint.tables, &hint.name, warnings))
            }
            "no_hash_join" => p
                .no_hash_join
                .extend(resolve(&hint.tables, &hint.name, warnings)),
            "no_merge_join" => p
                .no_merge_join
                .extend(resolve(&hint.tables, &hint.name, warnings)),
            "no_index_join" => {
                p.no_index_join
                    .inlj_tables
                    .extend(resolve(&hint.tables, &hint.name, warnings))
            }
            "no_index_hash_join" => {
                p.no_index_join
                    .inlhj_tables
                    .extend(resolve(&hint.tables, &hint.name, warnings))
            }
            "no_index_merge_join" => {
                p.no_index_join
                    .inlmj_tables
                    .extend(resolve(&hint.tables, &hint.name, warnings))
            }
            "mpp_1phase_agg" => p.prefer_agg_type |= PREFER_MPP_1PHASE_AGG,
            "mpp_2phase_agg" => p.prefer_agg_type |= PREFER_MPP_2PHASE_AGG,
            "hash_join_build" => p
                .hj_build
                .extend(resolve(&hint.tables, &hint.name, warnings)),
            "hash_join_probe" => p
                .hj_probe
                .extend(resolve(&hint.tables, &hint.name, warnings)),
            "hash_agg" => p.prefer_agg_type |= PREFER_HASH_AGG,
            "stream_agg" => p.prefer_agg_type |= PREFER_STREAM_AGG,
            "agg_to_cop" => p.prefer_agg_to_cop = true,
            "no_index_lookup_pushdown" => {
                if !hint.indexes.is_empty() {
                    warnings.push(
                        "hint NO_INDEX_LOOKUP_PUSH_DOWN is inapplicable, only table name without indexes is supported"
                            .to_owned(),
                    );
                    continue;
                }
                let mut db_name = hint.tables[0].db_name.clone();
                if db_name.lowercase().is_empty() {
                    db_name = CiString::new(current_db);
                }
                p.no_index_lookup_pushdown.push(HintedTable {
                    db_name,
                    tbl_name: hint.tables[0].table_name.clone(),
                    partitions: Vec::new(),
                    select_offset: 0,
                    matched: false,
                });
            }
            HINT_USE_INDEX
            | HINT_IGNORE_INDEX
            | HINT_FORCE_INDEX
            | "order_index"
            | "no_order_index"
            | HINT_INDEX_LOOKUP_PUSHDOWN => {
                let mut db_name = hint.tables[0].db_name.clone();
                if db_name.lowercase().is_empty() {
                    db_name = CiString::new(current_db);
                }
                let mut push_down_look_up = false;
                let kind = match hint.name.as_str() {
                    HINT_USE_INDEX => PlanIndexHintKind::Use,
                    HINT_IGNORE_INDEX => PlanIndexHintKind::Ignore,
                    HINT_FORCE_INDEX => PlanIndexHintKind::Force,
                    "order_index" => PlanIndexHintKind::OrderIndex,
                    "no_order_index" => PlanIndexHintKind::NoOrderIndex,
                    _ => {
                        // index_lookup_pushdown
                        if hint.indexes.is_empty() {
                            warnings.push(
                                "hint INDEX_LOOKUP_PUSH_DOWN is inapplicable, the index names should be specified"
                                    .to_owned(),
                            );
                            continue;
                        }
                        push_down_look_up = true;
                        PlanIndexHintKind::Use
                    }
                };
                p.index_hint_list.push(HintedIndex {
                    db_name,
                    tbl_name: hint.tables[0].table_name.clone(),
                    partitions: hint.tables[0].partitions.clone(),
                    index_names: hint.indexes.clone(),
                    kind,
                    push_down_look_up,
                    matched: false,
                });
            }
            "read_from_storage" => {
                if let PlanHintData::Storage(engine) = &hint.data {
                    if engine == "tiflash" {
                        p.tiflash_tables
                            .extend(resolve(&hint.tables, &hint.name, warnings));
                    } else if engine == "tikv" {
                        p.tikv_tables
                            .extend(resolve(&hint.tables, &hint.name, warnings));
                    }
                }
            }
            "use_index_merge" => {
                let mut db_name = hint.tables[0].db_name.clone();
                if db_name.lowercase().is_empty() {
                    db_name = CiString::new(current_db);
                }
                p.index_merge_hint_list.push(HintedIndex {
                    db_name,
                    tbl_name: hint.tables[0].table_name.clone(),
                    partitions: hint.tables[0].partitions.clone(),
                    index_names: hint.indexes.clone(),
                    kind: PlanIndexHintKind::Use,
                    push_down_look_up: false,
                    matched: false,
                });
            }
            "time_range" => {
                if let PlanHintData::TimeRange(range) = &hint.data {
                    p.time_range_hint = range.clone();
                }
            }
            "limit_to_cop" => p.prefer_limit_to_cop = true,
            "merge" => {
                if !hint.tables.is_empty() {
                    warnings.push(
                        "The MERGE hint is not used correctly, maybe it inputs a table name."
                            .to_owned(),
                    );
                    continue;
                }
                p.cte_merge = true;
            }
            HINT_LEADING => {
                if leading_hint_cnt == 0 {
                    p.leading_join_order
                        .extend(resolve(&hint.tables, &hint.name, warnings));
                    if let PlanHintData::Leading(list) = &hint.data {
                        p.leading_list = Some(list.clone());
                    }
                }
                leading_hint_cnt += 1;
            }
            "semi_join_rewrite" => {
                if !handling_exists_subquery && !handling_in_subquery {
                    warnings.push(
                        "The SEMI_JOIN_REWRITE hint is not used correctly, maybe it's not in a subquery or the subquery is not IN/EXISTS clause."
                            .to_owned(),
                    );
                    continue;
                }
                sub_query_hint_flags |= HINT_FLAG_SEMI_JOIN_REWRITE;
            }
            "no_decorrelate" => {
                if not_handling_subquery {
                    warnings.push(
                        "NO_DECORRELATE() is inapplicable because it's not in an IN subquery, an EXISTS subquery, an ANY/ALL/SOME subquery or a scalar subquery."
                            .to_owned(),
                    );
                    continue;
                }
                sub_query_hint_flags |= HINT_FLAG_NO_DECORRELATE;
            }
            "straight_join" => p.straight_join_order = true,
            // Hints not implemented are ignored.
            _ => {}
        }
    }

    if leading_hint_cnt > 1 || (leading_hint_cnt > 0 && straight_join_order) {
        p.leading_join_order.clear();
        if leading_hint_cnt > 1 {
            warnings.push(
                "We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid"
                    .to_owned(),
            );
        } else if straight_join_order {
            warnings.push(
                "We can only use the straight_join hint, when we use the leading hint and straight_join hint at the same time, all leading hints will be invalid"
                    .to_owned(),
            );
        }
    }
    (p, sub_query_hint_flags)
}

/// Go `PreferHashAgg` and friends — the aggregation-preference bits, at the
/// source's exact positions in its shared `iota` run.
pub const PREFER_HASH_AGG: u64 = 1 << 25;
/// Go `PreferStreamAgg`.
pub const PREFER_STREAM_AGG: u64 = 1 << 26;
/// Go `PreferMPP1PhaseAgg`.
pub const PREFER_MPP_1PHASE_AGG: u64 = 1 << 27;
/// Go `PreferMPP2PhaseAgg`.
pub const PREFER_MPP_2PHASE_AGG: u64 = 1 << 28;
