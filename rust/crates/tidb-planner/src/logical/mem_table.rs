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

//! Go `pkg/planner/core/operator/logicalop/logical_mem_table.go`:
//! `LogicalMemTable`, the memory / virtual table scan.
//!
//! SEED of `pkg/planner/core`. `LogicalMemTable` was a
//! [`crate::logical::TodoLogicalOp`] before this batch.
//!
//! The crate's `logical_mem_table` identity leaf is KEPT rather than merged:
//! `difftests/planner-tests/tests/logical_mem_table.rs` consumes its
//! `LogicalMemTableIdentity`/`MemTableColumnIdentity` from OUTSIDE this crate.
//!
//! Go's own framing of what makes this operator special: some memory tables
//! want to OWN their predicates. `SELECT * FROM cluster_log WHERE type='tikv'
//! AND address='192.16.5.32'` should send one log-search request to that TiKV
//! rather than fan out to the whole cluster and filter in TiDB.
//!
//! # Narrowings, by name
//!
//! * `Extractor base.MemTablePredicateExtractor`. The extractor interface, its
//!   per-table implementations in `pkg/planner/core/memtable_predicate_extractor.go`,
//!   and the `skipExtractor` failpoint are not transcreated, so
//!   `PredicatePushDown` cannot run `Extractor.Extract`; see
//!   [`LogicalMemTable::predicate_push_down`], which reports whether the
//!   extractor would have run instead of guessing what it would have kept.
//! * `base.MemTableRowLimitHintSetter` / `base.MemTableDescHintSetter` are the
//!   two OPTIONAL halves of that interface. `PushDownTopN`'s whole decision is
//!   which hint to set, so [`MemTableTopNHints`] carries the decision and the
//!   caller applies it to whatever extractor it holds.
//! * `TableInfo *model.TableInfo` / `Columns []*model.ColumnInfo`.
//!   `pkg/meta/model` is transcreated in `tidb-model`, which this crate does not
//!   depend on. The three facts these bodies read off it are kept directly: the
//!   table's `Name.O`, and each column's `Name.O` and `ID`.
//! * `DeriveStats` builds `statistics.PseudoTable(p.TableInfo, false, false)`
//!   and reads its `RealtimeCount`, plus a `HistColl` from
//!   `GenerateHistCollFromColumnInfo`. `statistics.PseudoTable` is not
//!   transcreated, so [`LogicalMemTable::derive_stats`] takes the realtime count
//!   from the caller; [`crate::stats_info::StatsInfo`] carries no `HistColl` or
//!   `StatsVersion`, so those two assignments have no counterpart.
//! * `QueryTimeRange util.QueryTimeRange` is the `time_range` hint's window; it
//!   is carried as its two bounds and no body here reads it.

use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use crate::logical::topn::LogicalTopN;
use crate::logical::{schema_producer, BaseLogicalPlan};
use crate::stats_info::StatsInfo;

/// Go `variable.SlowLogTimeStr`.
pub const SLOW_LOG_TIME_STR: &str = "Time";

/// Go `infoschema.TableSlowQuery`.
pub const TABLE_SLOW_QUERY: &str = "SLOW_QUERY";
/// Go `infoschema.ClusterTableSlowLog`.
pub const CLUSTER_TABLE_SLOW_LOG: &str = "CLUSTER_SLOW_QUERY";

/// The tables `LogicalMemTable.PruneColumns` (`logical_mem_table.go:80`) prunes,
/// by `infoschema` name.
///
/// Go spells this as a `switch` whose `default` returns UNPRUNED, so the list is
/// an allow-list rather than a deny-list: a memory table not named here keeps
/// its whole schema, because its reader is indexed by column position and would
/// mis-read a narrowed one.
pub const PRUNABLE_MEM_TABLES: &[&str] = &[
    // `infoschema.TableStatementsSummary`
    "STATEMENTS_SUMMARY",
    // `infoschema.TableStatementsSummaryHistory`
    "STATEMENTS_SUMMARY_HISTORY",
    // `infoschema.TableTiDBStatementsStats`
    "TIDB_STATEMENTS_STATS",
    // `infoschema.TableSlowQuery`
    TABLE_SLOW_QUERY,
    // `infoschema.ClusterTableStatementsSummary`
    "CLUSTER_STATEMENTS_SUMMARY",
    // `infoschema.ClusterTableStatementsSummaryHistory`
    "CLUSTER_STATEMENTS_SUMMARY_HISTORY",
    // `infoschema.ClusterTableTiDBStatementsStats`
    "CLUSTER_TIDB_STATEMENTS_STATS",
    // `infoschema.ClusterTableSlowLog`
    CLUSTER_TABLE_SLOW_LOG,
    // `infoschema.TableTiDBTrx`
    "TIDB_TRX",
    // `infoschema.ClusterTableTiDBTrx`
    "CLUSTER_TIDB_TRX",
    // `infoschema.TableDataLockWaits`
    "DATA_LOCK_WAITS",
    // `infoschema.TableDeadlocks`
    "DEADLOCKS",
    // `infoschema.ClusterTableDeadlocks`
    "CLUSTER_DEADLOCKS",
    // `infoschema.TableTables`
    "TABLES",
];

/// What the ported `LogicalMemTable` bodies read off a `*model.ColumnInfo`.
#[derive(Clone, Debug, Default)]
pub struct MemTableColumn {
    /// Go `ColumnInfo.ID`.
    pub id: i64,
    /// Go `ColumnInfo.Name.O`.
    pub name: String,
}

/// Go `util.QueryTimeRange`, the `time_range` hint's window.
#[derive(Clone, Debug, Default)]
pub struct QueryTimeRange {
    /// Go `QueryTimeRange.From`, as the caller's own timestamp encoding.
    pub from: String,
    /// Go `QueryTimeRange.To`.
    pub to: String,
}

/// The decision `LogicalMemTable.PushDownTopN` (`logical_mem_table.go:114`)
/// makes, separated from the extractor it would write it to; see this module's
/// header.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemTableTopNHints {
    /// Go `MemTableRowLimitHintSetter.SetRowLimitHint(end)`, when the extractor
    /// implements it.
    pub row_limit_hint: Option<u64>,
    /// Go `MemTableDescHintSetter.SetDesc(topN.ByItems[0].Desc)`, which only the
    /// slow-log-by-time case sets.
    pub desc: Option<bool>,
}

/// Go `logicalop.LogicalMemTable` (`logical_mem_table.go:42`).
#[derive(Clone, Debug, Default)]
pub struct LogicalMemTable {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `DBName`.
    pub db_name: String,
    /// Go `TableInfo.Name.O`.
    pub table_name: String,
    /// Go `TableInfo.Columns`: the TABLE's columns, which
    /// [`Self::is_slow_log_topn_by_time`] searches by name and id.
    pub table_columns: Vec<MemTableColumn>,
    /// Go `Columns`: the columns this scan produces, in schema order.
    pub columns: Vec<MemTableColumn>,
    /// Go `QueryTimeRange`.
    pub query_time_range: QueryTimeRange,
    /// Whether the operator carries a `base.MemTablePredicateExtractor`; see
    /// this module's header.
    pub has_extractor: bool,
}

impl LogicalMemTable {
    /// Go `plancodec.TypeMemTableScan`.
    pub const TYPE: &'static str = "MemTableScan";

    /// Go `LogicalMemTable.Init(ctx, offset)` (`logical_mem_table.go:58`).
    #[must_use]
    pub fn new(
        base: BaseLogicalPlan,
        db_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            base,
            db_name: db_name.into(),
            table_name: table_name.into(),
            ..Self::default()
        }
    }

    /// Go `LogicalMemTable.PredicatePushDown(predicates)`
    /// (`logical_mem_table.go:68`): the predicates are handed to the table's own
    /// EXTRACTOR, and whatever it does not claim comes back.
    ///
    /// BOUNDARY: `base.MemTablePredicateExtractor.Extract` is not transcreated,
    /// so this cannot run the extraction. It returns the predicates unchanged —
    /// which is Go's answer whenever there is no extractor, and the SAFE half
    /// otherwise, since an unclaimed predicate is still evaluated above — and
    /// reports whether an extractor would have run, so a caller cannot mistake
    /// the two cases.
    #[must_use]
    pub fn predicate_push_down(&self, predicates: Vec<Expression>) -> (Vec<Expression>, bool) {
        (predicates, self.has_extractor)
    }

    /// Go `LogicalMemTable.PruneColumns(parentUsedCols)`'s table test
    /// (`logical_mem_table.go:80`); see [`PRUNABLE_MEM_TABLES`].
    #[must_use]
    pub fn is_prunable(&self) -> bool {
        PRUNABLE_MEM_TABLES.contains(&self.table_name.as_str())
    }

    /// The rest of `LogicalMemTable.PruneColumns` (`logical_mem_table.go:98`):
    /// drop every unused column from the schema and from [`Self::columns`] in
    /// lockstep, walking backwards.
    ///
    /// Go's extra guard `p.Schema().Len() > 1` keeps the LAST column whatever
    /// happens: a mem-table reader with a zero-column schema has nothing to
    /// return a row shape from. Returns the removed positions, in descending
    /// order, so the driver can delete the matching output names.
    pub fn prune_columns(
        &mut self,
        schema: &mut Schema,
        parent_used_cols: &[Column],
    ) -> Vec<usize> {
        if !self.is_prunable() {
            return Vec::new();
        }
        let used = schema_producer::get_used_list(parent_used_cols, schema);
        let mut pruned = Vec::new();
        for i in (0..used.len()).rev() {
            if !used[i] && schema.len() > 1 {
                pruned.push(i);
                schema.columns.remove(i);
                if i < self.columns.len() {
                    self.columns.remove(i);
                }
            }
        }
        pruned
    }

    /// Go `LogicalMemTable.PushDownTopN(topN)` (`logical_mem_table.go:114`): the
    /// TopN is ALWAYS re-attached above this scan, and the only question is
    /// which hints the extractor gets on the way.
    ///
    /// * a partitioned TopN pushes nothing — the hint is a whole-result row
    ///   limit and a per-partition limit is not one;
    /// * a TopN with no `ByItems` is a plain LIMIT, so the row limit is a sound
    ///   hint on its own;
    /// * a slow-log TopN ordered by the `Time` column can push both the limit
    ///   and the DIRECTION, because the slow-log reader can walk its files from
    ///   either end;
    /// * anything else pushes nothing, because the reader cannot honour an order
    ///   it does not store.
    ///
    /// Go returns `topN.AttachChild(p)` in every case, which is why the caller
    /// keeps the TopN.
    #[must_use]
    pub fn push_down_topn(&self, topn: &LogicalTopN) -> MemTableTopNHints {
        let mut hints = MemTableTopNHints::default();
        if !topn.get_partition_by().is_empty() {
            return hints;
        }
        if topn.is_limit() {
            hints.row_limit_hint = Some(Self::push_down_row_limit(topn));
        } else if self.is_slow_log_topn_by_time(topn) {
            hints.desc = topn.by_items.first().map(|item| item.desc);
            hints.row_limit_hint = Some(Self::push_down_row_limit(topn));
        }
        hints
    }

    /// Go `pushDownRowLimit(topN, limitSetter)` (`logical_mem_table.go:145`):
    /// the hint is `Offset + Count`, SATURATED — Go detects the wrap explicitly
    /// (`if end < topN.Offset`) and asks for everything instead.
    #[must_use]
    pub const fn push_down_row_limit(topn: &LogicalTopN) -> u64 {
        topn.offset.saturating_add(topn.count)
    }

    /// Go `LogicalMemTable.isSlowLogTopNByTime(topN)`
    /// (`logical_mem_table.go:153`): exactly one `ByItem`, that item a bare
    /// COLUMN, this table one of the two slow-log tables, and the column the
    /// table's own `Time` column BY ID.
    ///
    /// The id test is what makes it safe: a column merely named `Time` in some
    /// other position would not be the file-ordering key the reader walks.
    #[must_use]
    pub fn is_slow_log_topn_by_time(&self, topn: &LogicalTopN) -> bool {
        let [item] = &topn.by_items[..] else {
            return false;
        };
        let Expression::Column(col) = &item.expr else {
            return false;
        };
        if self.table_name != TABLE_SLOW_QUERY && self.table_name != CLUSTER_TABLE_SLOW_LOG {
            return false;
        }
        self.table_columns
            .iter()
            .any(|column| column.name == SLOW_LOG_TIME_STR && column.id == col.id)
    }

    /// Go `LogicalMemTable.DeriveStats(_, selfSchema, _, reloads)`
    /// (`logical_mem_table.go:181`): the PSEUDO table's realtime count, with
    /// every schema column given that same NDV.
    ///
    /// See this module's header for `realtime_count`, and for the `HistColl`
    /// and `StatsVersion` assignments that have no counterpart here.
    pub fn derive_stats(
        &mut self,
        self_schema: &Schema,
        reloads: &[bool],
        realtime_count: f64,
    ) -> (StatsInfo, bool) {
        let reload = reloads.len() == 1 && reloads[0];
        if !reload {
            if let Some(existing) = self.base.base.stats_info() {
                return (existing.clone(), false);
            }
        }
        let stats = StatsInfo::new(
            realtime_count,
            self_schema
                .columns
                .iter()
                .map(|col| (col.unique_id, realtime_count)),
        );
        self.base.base.set_stats(Some(stats.clone()));
        (stats, true)
    }

    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`].
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            db_name: self.db_name.clone(),
            table_name: self.table_name.clone(),
            table_columns: self.table_columns.clone(),
            columns: self.columns.clone(),
            query_time_range: self.query_time_range.clone(),
            has_extractor: self.has_extractor,
        }
    }
}
