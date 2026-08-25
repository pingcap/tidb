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

//! `pkg/executor/show_stats.go`: the `SHOW STATS_*` statement family --
//! `STATS_META` (:36), `STATS_LOCKED` (:144), `STATS_HISTOGRAMS` (:204),
//! `STATS_BUCKETS` (:285), `STATS_TOPN` (:348), `STATS_HEALTHY` (:468),
//! `SHOW HISTOGRAMS_IN_FLIGHT` (:535), `SHOW ANALYZE STATUS` (:540) and
//! `SHOW COLUMN_STATS_USAGE` (:553).
//!
//! # What each of these statements actually is
//!
//! Every one of them is the same two-part shape: a TRAVERSAL of the schemas,
//! tables and partitions the session can see, and a per-target ROW SHAPE. The
//! traversal is nearly identical across the nine and is written out nine
//! times in Go; the row shape is where each statement differs and is what
//! users, tests and tooling actually depend on. This file ports both halves,
//! with the traversal written ONCE ([`PartitionTargets`]) because the nine
//! copies agree.
//!
//! They are also, deliberately, a debugging surface rather than an
//! optimiser input: nothing here feeds estimation. `SHOW STATS_BUCKETS`
//! renders exactly the histogram the estimator reads, which is why
//! [`buckets_to_rows`] must not round or reformat anything.
//!
//! # Reused rather than restated
//!
//! * [`tidb_stats::Table`] IS Go `statistics.Table`, including
//!   `Pseudo`, `IsAnalyzed`, `RealtimeCount`/`ModifyCount` and
//!   `GetStatsHealthy` -- the last as
//!   [`tidb_stats::Table::stats_healthy`], whose `(healthy, ok)` pair is
//!   Go's exactly, so [`healthy_row`] only has to honour the `ok` skip.
//! * [`tidb_stats::Histogram`] and [`tidb_stats::Bucket`] ARE Go
//!   `statistics.Histogram` / `Bucket`, so [`histogram_row`] and
//!   [`buckets_to_rows`] read the same fields Go's do.
//! * [`tidb_stats::TopNEntry`] IS Go `statistics.TopNMeta`.
//! * [`tidb_stats::memory_usage::ColumnMemUsage`] /
//!   [`tidb_stats::memory_usage::IndexMemUsage`] ARE Go's
//!   `statistics.CacheItemMemoryUsage` implementations, and their four
//!   accessors are the four memory columns of `SHOW STATS_HISTOGRAMS`.
//!
//! # Narrowings (each named)
//!
//! * The traversal's SOURCE -- `domain.GetDomain(e.Ctx()).InfoSchema()`,
//!   `AllSchemaNames`, `SchemaTableInfos`, `ListTablesWithSpecialAttribute`
//!   and the `StatsHandle` lookups `GetNonPseudoPhysicalTableStats` /
//!   `GetPhysicalTableStats` / `GetLockedTables` / `LoadColumnStatsUsage` --
//!   is a session-scoped domain handle with no local peer. This file takes
//!   already-resolved targets and already-resolved statistics.
//! * `statistics.ValueToString` needs the session's time zone AND
//!   `tablecodec` index-key decoding to render an index bound. It is taken as
//!   a caller-supplied closure ([`ValueRenderer`]) rather than reimplemented,
//!   so a caller wiring a real session gets Go's exact rendering and a test
//!   can be dependency-closed.
//! * `cardinality.AvgColSize` (:243) lives in the planner's cardinality
//!   package; [`histogram_row`] takes the computed value, which is what Go
//!   passes it.
//! * `StatsLoadedStatus.StatusToString` (:244) is a stats-cache load-state
//!   string; taken as a caller value for the same reason.
//! * `fetchShowHistogramsInFlight` (:535) is one row holding
//!   `statsStorage.CleanFakeItemsForShowHistInFlights(statsHandle)`, a
//!   cache-mutating counter read. Only its row shape is ported
//!   ([`histograms_in_flight_row`]); the counter itself is a handle call.
//! * `fetchShowAnalyzeStatus` (:540) delegates wholesale to
//!   `dataForAnalyzeStatusHelper`, which reads `mysql.analyze_jobs` through a
//!   restricted SQL executor. Not ported; it is a different file's function
//!   appended verbatim.
//! * The `SHOW STATS_META` extractor filters (:41-58) -- `Field()`,
//!   `FieldPatternLike()` and the `StatsMetaDBFilters`/`StatsMetaTableFilters`
//!   duck-typed pair -- are ported as [`MetaFilters`], but the LIKE pattern
//!   itself is `collate.WildcardPattern` and is supplied as a closure.
//!
//! # Sequential here, sequential there
//!
//! Unlike the reader and analyze operators, nothing in `show_stats.go` is
//! concurrent: every fetcher is a plain nested loop appending to
//! `e.result`. There is nothing to narrow.

use tidb_datatype::{core_time_from_datetime, Datum, Time, TimeType};
use tidb_stats::memory_usage::{ColumnMemUsage, IndexMemUsage};
use tidb_stats::{Histogram, Table, TopN};

/// Go `oracle.physicalShiftBits`: a TSO's low 18 bits are its logical
/// counter, so the physical half is milliseconds since the Unix epoch.
pub const PHYSICAL_SHIFT_BITS: u64 = 18;

/// Go `(*ShowExec).versionToTime` (:280): `oracle.GetTimeFromTS` then
/// `types.NewTime(types.FromGoTime(t), mysql.TypeDatetime, 0)`.
///
/// The `fsp` of 0 is Go's and is observable: a stats version carries
/// millisecond resolution, but the rendered `update_time` column TRUNCATES to
/// whole seconds. Two analyses within the same second therefore show the same
/// time, which is expected rather than a bug.
///
/// Go builds the `time.Time` in the process's LOCAL zone
/// (`oracle.GetTimeFromTS` calls `time.Unix`), not the session zone; the
/// caller supplies the zone here so that choice stays visible.
pub fn version_to_time<TZ: chrono::TimeZone>(version: u64, zone: &TZ) -> Option<Time> {
    let physical_millis = i64::try_from(version >> PHYSICAL_SHIFT_BITS).ok()?;
    let instant = chrono::DateTime::from_timestamp_millis(physical_millis)?;
    let core = core_time_from_datetime(instant.with_timezone(zone));
    Time::new(core, TimeType::DateTime, 0).ok()
}

/// The three partition spellings every fetcher in this file produces.
///
/// The empty string for a non-partitioned table and the literal `"global"`
/// for a partitioned table's aggregate row are Go's, and the distinction is
/// load-bearing: `""` means "this table has no partitions", while `"global"`
/// means "this row is the merged view across partitions". A partitioned table
/// under STATIC pruning produces neither -- it has no global statistics at
/// all, which is exactly why the `IsDynamicPartitionPruneEnabled()` branch
/// exists at :215, :299, :362 and :161.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionLabel {
    /// A table with no partitions: Go's `""`.
    None,
    /// A partitioned table's merged row: Go's `"global"`.
    Global,
    /// One partition, named by its definition: Go's `def.Name.O`.
    Definition(String),
}

impl PartitionLabel {
    /// The string that reaches the result column.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::None => "",
            Self::Global => "global",
            Self::Definition(name) => name,
        }
    }
}

/// One physical target a fetcher emits rows for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalTarget {
    /// Go `tbl.ID` for a whole table or `def.ID` for a partition. This is the
    /// key the statistics cache is looked up by, and it differs from the
    /// logical table ID for every partition.
    pub physical_id: i64,
    /// How this row names its partition column.
    pub label: PartitionLabel,
}

/// Whether the table has partitions and how the session prunes them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionTargets;

impl PartitionTargets {
    /// The traversal Go writes out at :212-231, :296-315, :360-379 and
    /// :161-178, which agree.
    ///
    /// * A non-partitioned table yields exactly one target, labelled `""`.
    /// * A partitioned table under DYNAMIC pruning yields the global row
    ///   FIRST and then one row per partition definition, in definition
    ///   order.
    /// * A partitioned table under STATIC pruning yields ONLY the partition
    ///   rows -- no global row, because static pruning never builds global
    ///   statistics, so a global row would be either absent or stale.
    ///
    /// Definition order is preserved rather than sorted: `SHOW STATS_*`
    /// output order follows `PartitionInfo.Definitions`, which is the order
    /// the partitions were declared in.
    #[must_use]
    pub fn for_table(
        table_id: i64,
        partition_definitions: &[(i64, String)],
        dynamic_pruning: bool,
    ) -> Vec<PhysicalTarget> {
        if partition_definitions.is_empty() {
            return vec![PhysicalTarget {
                physical_id: table_id,
                label: PartitionLabel::None,
            }];
        }
        let mut targets = Vec::with_capacity(partition_definitions.len() + 1);
        if dynamic_pruning {
            targets.push(PhysicalTarget {
                physical_id: table_id,
                label: PartitionLabel::Global,
            });
        }
        for (partition_id, name) in partition_definitions {
            targets.push(PhysicalTarget {
                physical_id: *partition_id,
                label: PartitionLabel::Definition(name.clone()),
            });
        }
        targets
    }
}

/// Go `fetchShowStatsMeta` :41-58: the `SHOW STATS_META` filters.
///
/// Go reads `Field()` and `FieldPatternLike()` off the extractor, then
/// duck-types the extractor for the DB/table filter pair. All three narrow the
/// SCHEMA loop except `table_names`, which narrows the table loop. An EMPTY
/// filter set is treated as absent (Go's `!raw.StatsMetaDBFilters().Empty()`
/// guard), which is not the same as a set that excludes everything.
#[derive(Debug, Clone, Default)]
pub struct MetaFilters {
    /// Go `Extractor.Field()`: an exact lower-cased schema name, or empty.
    pub schema_name: String,
    /// Go `StatsMetaDBFilters()`, already emptied to `None` when Go's
    /// `Empty()` guard would have left it nil.
    pub schema_names: Option<Vec<String>>,
    /// Go `StatsMetaTableFilters()`, same treatment.
    pub table_names: Option<Vec<String>>,
}

impl MetaFilters {
    /// Go :66-72. `pattern_matches` stands in for
    /// `collate.WildcardPattern.DoMatch`; `None` is Go's nil pattern, which
    /// skips the check entirely rather than failing it.
    #[must_use]
    pub fn accepts_schema(
        &self,
        lower_schema_name: &str,
        pattern_matches: Option<&dyn Fn(&str) -> bool>,
    ) -> bool {
        // Go's chain is if/else-if: an exact Field() SHORT-CIRCUITS the
        // pattern and the set filter, so the three are not conjunctive.
        if !self.schema_name.is_empty() {
            return lower_schema_name == self.schema_name;
        }
        if let Some(matches) = pattern_matches {
            return matches(lower_schema_name);
        }
        if let Some(names) = &self.schema_names {
            return names.iter().any(|name| name == lower_schema_name);
        }
        true
    }

    /// Go :76-78.
    #[must_use]
    pub fn accepts_table(&self, lower_table_name: &str) -> bool {
        self.table_names
            .as_ref()
            .is_none_or(|names| names.iter().any(|name| name == lower_table_name))
    }
}

/// Go `appendTableForStatsMeta` (:108): one `SHOW STATS_META` row.
///
/// `None` is Go's early return for a nil or PSEUDO statistics table: a pseudo
/// table is the placeholder the optimiser uses when nothing has been
/// collected, and showing its invented counts as if they were measured would
/// be a wrong answer rather than a missing one.
///
/// The `IsAnalyzed` branch changes ONLY the last column: an un-analyzed table
/// still reports its version, modify count and realtime count (all of which
/// the delta-tracking path maintains without any `ANALYZE`), but reports NULL
/// rather than a fabricated `last_analyze_time`.
#[must_use]
pub fn stats_meta_row<TZ: chrono::TimeZone>(
    db_name: &str,
    table_name: &str,
    partition: &PartitionLabel,
    stats: &Table,
    zone: &TZ,
) -> Option<Vec<Datum>> {
    if stats.hist_coll.pseudo {
        return None;
    }
    let last_analyze = if stats.is_analyzed() {
        version_to_time(stats.last_analyze_version, zone).map_or(Datum::Null, Datum::Time)
    } else {
        Datum::Null
    };
    Some(vec![
        Datum::new_string(db_name.as_bytes().to_vec()),
        Datum::new_string(table_name.as_bytes().to_vec()),
        Datum::new_string(partition.as_str().as_bytes().to_vec()),
        version_to_time(stats.version, zone).map_or(Datum::Null, Datum::Time),
        Datum::Int(stats.hist_coll.modify_count),
        Datum::Int(stats.hist_coll.realtime_count),
        last_analyze,
    ])
}

/// Go `appendTableForStatsLocked` (:135): one `SHOW STATS_LOCKED` row.
///
/// The fourth column is the literal `"locked"` for every row: the statement
/// lists only locked tables, so the column exists to make the output
/// self-describing rather than to carry a varying value.
///
/// Go's :189-197 sorts the table IDs before emitting, explicitly "to make the
/// output stable" -- the traversal built a map, so without the sort the row
/// order would be Go's randomised map order. A caller reproducing
/// `fetchShowStatsLocked` must sort by physical ID, not by name.
#[must_use]
pub fn stats_locked_row(db_name: &str, table_name: &str, partition: &PartitionLabel) -> Vec<Datum> {
    vec![
        Datum::new_string(db_name.as_bytes().to_vec()),
        Datum::new_string(table_name.as_bytes().to_vec()),
        Datum::new_string(partition.as_str().as_bytes().to_vec()),
        Datum::new_string(b"locked".to_vec()),
    ]
}

/// The four memory columns of `SHOW STATS_HISTOGRAMS`, Go
/// `statistics.CacheItemMemoryUsage` as :270-273 reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ItemMemoryUsage {
    /// Go `TotalMemoryUsage()`.
    pub total: i64,
    /// Go `HistMemUsage()`.
    pub histogram: i64,
    /// Go `TopnMemUsage()`.
    pub topn: i64,
    /// Go `CMSMemUsage()`.
    pub cms: i64,
}

impl From<ColumnMemUsage> for ItemMemoryUsage {
    fn from(usage: ColumnMemUsage) -> Self {
        Self {
            total: usage.total_memory_usage(),
            histogram: usage.hist_mem_usage(),
            topn: usage.topn_mem_usage(),
            cms: usage.cms_mem_usage(),
        }
    }
}

impl From<IndexMemUsage> for ItemMemoryUsage {
    fn from(usage: IndexMemUsage) -> Self {
        Self {
            total: usage.total_memory_usage(),
            histogram: usage.hist_mem_usage(),
            topn: usage.topn_mem_usage(),
            cms: usage.cms_mem_usage(),
        }
    }
}

/// Go `histogramToRow` (:258): one `SHOW STATS_HISTOGRAMS` row.
///
/// `avg_col_size` is Go's `cardinality.AvgColSize(col, RealtimeCount, false)`
/// for a column and a hard `0` for an index (:250) -- an index's entry size is
/// not a column size, so Go does not compute one rather than computing a
/// meaningless one. `correlation` is likewise column-only in meaning; the
/// column still exists on index rows and holds whatever the histogram carries.
// Go's own signature is this wide; narrowing it into a struct would hide
// which value feeds which result column.
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn histogram_row<TZ: chrono::TimeZone>(
    db_name: &str,
    table_name: &str,
    partition: &PartitionLabel,
    column_name: &str,
    is_index: bool,
    histogram: &Histogram,
    avg_col_size: f64,
    load_status: &str,
    memory: ItemMemoryUsage,
    zone: &TZ,
) -> Vec<Datum> {
    vec![
        Datum::new_string(db_name.as_bytes().to_vec()),
        Datum::new_string(table_name.as_bytes().to_vec()),
        Datum::new_string(partition.as_str().as_bytes().to_vec()),
        Datum::new_string(column_name.as_bytes().to_vec()),
        Datum::Int(i64::from(is_index)),
        version_to_time(histogram.last_update_version, zone).map_or(Datum::Null, Datum::Time),
        Datum::Int(histogram.ndv),
        Datum::Int(histogram.null_count),
        Datum::Real(avg_col_size),
        Datum::Real(histogram.correlation),
        Datum::new_string(load_status.as_bytes().to_vec()),
        Datum::Int(memory.total),
        Datum::Int(memory.histogram),
        Datum::Int(memory.topn),
        Datum::Int(memory.cms),
    ]
}

/// Renders one encoded statistics value, Go `statistics.ValueToString`.
///
/// `num_of_cols` is 0 for a column value and the index's column count for an
/// index value; `column_types` carries the index key parts' MySQL type bytes
/// so a composite index key can be split back into its parts. Both are Go's
/// parameters unchanged.
///
/// boundary: Go `pkg/statistics/histogram.go` `ValueToString`, which needs the
/// session's time zone and `tablecodec.DecodeValuesBytesToStrings`.
pub type ValueRenderer<'a, E> = &'a mut dyn FnMut(&Datum, usize, &[u8]) -> Result<String, E>;

/// Go `bucketsToRows` (:437): the `SHOW STATS_BUCKETS` rows for one histogram.
///
/// `num_of_cols > 0` is what makes a row an INDEX row (:439-442) -- the flag
/// is derived from the column count rather than passed, because an index bound
/// cannot be rendered without knowing how many key parts to split it into.
///
/// The emitted `bucket_id` is the bucket's INDEX, and `count` is the histogram's
/// CUMULATIVE count through that bucket rather than the bucket's own row count.
/// That is Go's field and users read it as such; converting it here would break
/// every existing query against the statement.
// Go's own signature is this wide; narrowing it into a struct would hide
// which value feeds which result column.
#[allow(clippy::too_many_arguments)]
pub fn buckets_to_rows<E>(
    db_name: &str,
    table_name: &str,
    partition: &PartitionLabel,
    column_name: &str,
    num_of_cols: usize,
    histogram: &Histogram,
    index_column_types: &[u8],
    render: ValueRenderer<'_, E>,
) -> Result<Vec<Vec<Datum>>, E> {
    let is_index = i64::from(num_of_cols > 0);
    let mut rows = Vec::with_capacity(histogram.buckets.len());
    for (position, bucket) in histogram.buckets.iter().enumerate() {
        let lower = render(&bucket.lower_bound, num_of_cols, index_column_types)?;
        let upper = render(&bucket.upper_bound, num_of_cols, index_column_types)?;
        rows.push(vec![
            Datum::new_string(db_name.as_bytes().to_vec()),
            Datum::new_string(table_name.as_bytes().to_vec()),
            Datum::new_string(partition.as_str().as_bytes().to_vec()),
            Datum::new_string(column_name.as_bytes().to_vec()),
            Datum::Int(is_index),
            Datum::Int(position as i64),
            Datum::Int(bucket.count),
            Datum::Int(bucket.repeat),
            Datum::new_string(lower.into_bytes()),
            Datum::new_string(upper.into_bytes()),
            Datum::Int(bucket.ndv),
        ]);
    }
    Ok(rows)
}

/// Go `topNToRows` (:411): the `SHOW STATS_TOPN` rows for one TopN.
///
/// A missing TopN produces NO rows rather than an empty-valued row (Go's
/// `if topN == nil { return nil }`), which is why the return is a possibly
/// empty vector and never a placeholder.
///
/// Note Go passes `numOfCols = 1` for a COLUMN here (:394) while
/// `bucketsToRows` passes 0 for the same column (:328). That is not an
/// inconsistency in the source: a TopN value is always a full encoded key,
/// so it always needs a one-part split, whereas a bucket bound for a column is
/// stored as a typed datum needing none.
// Go's own signature is this wide; narrowing it into a struct would hide
// which value feeds which result column.
#[allow(clippy::too_many_arguments)]
pub fn topn_to_rows<E>(
    db_name: &str,
    table_name: &str,
    partition: &PartitionLabel,
    column_name: &str,
    num_of_cols: usize,
    is_index: bool,
    topn: Option<&TopN>,
    column_types: &[u8],
    render: ValueRenderer<'_, E>,
) -> Result<Vec<Vec<Datum>>, E> {
    let Some(topn) = topn else {
        return Ok(Vec::new());
    };
    let mut rows = Vec::with_capacity(topn.entries().len());
    for entry in topn.entries() {
        // Go wraps the encoded bytes in a scratch Datum before rendering.
        let value = Datum::new_bytes(entry.encoded.clone());
        let rendered = render(&value, num_of_cols, column_types)?;
        rows.push(vec![
            Datum::new_string(db_name.as_bytes().to_vec()),
            Datum::new_string(table_name.as_bytes().to_vec()),
            Datum::new_string(partition.as_str().as_bytes().to_vec()),
            Datum::new_string(column_name.as_bytes().to_vec()),
            Datum::Int(i64::from(is_index)),
            Datum::new_string(rendered.into_bytes()),
            Datum::Int(entry.count as i64),
        ]);
    }
    Ok(rows)
}

/// Go `appendTableForStatsHealthy` (:522): one `SHOW STATS_HEALTHY` row.
///
/// `None` is Go's `if !ok { return }`. [`tidb_stats::Table::stats_healthy`]
/// returns `ok == false` for a PSEUDO table only; an analyzed-but-unmodified
/// table returns `(100, true)` and an un-analyzed non-pseudo table returns
/// `(0, true)`. So a missing row means "no statistics object at all", while a
/// zero means "statistics exist and are entirely stale" -- two different
/// states that must not be conflated.
#[must_use]
pub fn healthy_row(
    db_name: &str,
    table_name: &str,
    partition: &PartitionLabel,
    stats: &Table,
) -> Option<Vec<Datum>> {
    let (healthy, ok) = stats.stats_healthy();
    if !ok {
        return None;
    }
    Some(vec![
        Datum::new_string(db_name.as_bytes().to_vec()),
        Datum::new_string(table_name.as_bytes().to_vec()),
        Datum::new_string(partition.as_str().as_bytes().to_vec()),
        Datum::Int(healthy),
    ])
}

/// Go `fetchShowHistogramsInFlight` (:535): the single-column, single-row
/// result of `SHOW HISTOGRAMS_IN_FLIGHT`.
///
/// boundary: Go
/// `pkg/statistics/handle/storage.CleanFakeItemsForShowHistInFlights`
/// produces the count, and does so by SWEEPING the sync-load queue as a side
/// effect, so it is a handle call rather than a readable counter.
#[must_use]
pub fn histograms_in_flight_row(in_flight: i64) -> Vec<Datum> {
    vec![Datum::Int(in_flight)]
}

/// Go `fetchShowColumnStatsUsage` :571-587: one `SHOW COLUMN_STATS_USAGE` row.
///
/// Both timestamps are nullable and independently so: a column can have been
/// USED as a predicate without ever having been analyzed, and vice versa.
///
/// The physical ID a usage entry is keyed by is Go's `TableItemID{TableID:
/// tblID, ...}` where `tblID` is the PARTITION's ID when a definition is
/// given. Go's comment at :599 flags the surprise: predicate columns are
/// dumped under the table ID even in static pruning mode, so this fetcher is
/// called for the global row AND every partition row in BOTH pruning modes --
/// unlike every other fetcher in this file.
#[must_use]
pub fn column_stats_usage_row(
    db_name: &str,
    table_name: &str,
    partition: &PartitionLabel,
    column_name: &str,
    last_used_at: Option<Time>,
    last_analyzed_at: Option<Time>,
) -> Vec<Datum> {
    vec![
        Datum::new_string(db_name.as_bytes().to_vec()),
        Datum::new_string(table_name.as_bytes().to_vec()),
        Datum::new_string(partition.as_str().as_bytes().to_vec()),
        Datum::new_string(column_name.as_bytes().to_vec()),
        last_used_at.map_or(Datum::Null, Datum::Time),
        last_analyzed_at.map_or(Datum::Null, Datum::Time),
    ]
}

/// Go `fetchShowColumnStatsUsage` :575-580: the partition label that fetcher
/// uses, which is NOT [`PartitionTargets::for_table`]'s.
///
/// Go's `appendTableForColumnStatsUsage(dbName, tbl, global, def)` derives the
/// label from the pair: a definition names itself, otherwise `global` decides
/// between `"global"` and `""`. The caller passes `global = pi != nil`, so a
/// partitioned table's table-level row is labelled `"global"` in BOTH pruning
/// modes -- the one place in this file where a static-pruned partitioned table
/// still emits a global row.
#[must_use]
pub fn column_stats_usage_label(global: bool, definition_name: Option<&str>) -> PartitionLabel {
    match (definition_name, global) {
        (Some(name), _) => PartitionLabel::Definition(name.to_owned()),
        (None, true) => PartitionLabel::Global,
        (None, false) => PartitionLabel::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_stats::histogram::Bucket;
    use tidb_stats::table::HistColl;

    fn empty_stats_table() -> Table {
        Table {
            existence_map: None,
            hist_coll: HistColl::new(1, 0, 0, 0, 0),
            version: 0,
            last_analyze_version: 0,
            last_stats_hist_version: 0,
            table_info_update_ts: 0,
            is_pk_handle: false,
        }
    }

    fn label_names(targets: &[PhysicalTarget]) -> Vec<(i64, String)> {
        targets
            .iter()
            .map(|target| (target.physical_id, target.label.as_str().to_owned()))
            .collect()
    }

    // WRITTEN test: Go covers the traversal only through testkit `SHOW`
    // statements. The three shapes and their ORDER are what the row output
    // depends on.
    #[test]
    fn traversal_covers_global_only_under_dynamic_pruning() {
        assert_eq!(
            label_names(&PartitionTargets::for_table(7, &[], true)),
            vec![(7, String::new())]
        );
        let definitions = vec![(11, "p0".to_owned()), (12, "p1".to_owned())];
        assert_eq!(
            label_names(&PartitionTargets::for_table(7, &definitions, true)),
            vec![
                (7, "global".to_owned()),
                (11, "p0".to_owned()),
                (12, "p1".to_owned()),
            ]
        );
        // Static pruning has no global statistics, so no global row.
        assert_eq!(
            label_names(&PartitionTargets::for_table(7, &definitions, false)),
            vec![(11, "p0".to_owned()), (12, "p1".to_owned())]
        );
    }

    // WRITTEN test for :66-78: the filters are a short-circuiting chain, not
    // a conjunction.
    #[test]
    fn meta_filters_short_circuit_like_gos_else_if_chain() {
        let exact = MetaFilters {
            schema_name: "test".to_owned(),
            schema_names: Some(vec!["other".to_owned()]),
            ..MetaFilters::default()
        };
        // An exact Field() wins outright: the set filter never runs.
        assert!(exact.accepts_schema("test", None));
        assert!(!exact.accepts_schema("other", None));

        let set_only = MetaFilters {
            schema_names: Some(vec!["a".to_owned()]),
            ..MetaFilters::default()
        };
        assert!(set_only.accepts_schema("a", None));
        assert!(!set_only.accepts_schema("b", None));

        // A nil pattern skips the check; a present pattern replaces the set.
        let pattern_only = MetaFilters {
            schema_names: Some(vec!["a".to_owned()]),
            ..MetaFilters::default()
        };
        let matches = |name: &str| name.starts_with('z');
        assert!(pattern_only.accepts_schema("zed", Some(&matches)));
        assert!(!pattern_only.accepts_schema("a", Some(&matches)));

        assert!(MetaFilters::default().accepts_table("anything"));
        let table_filtered = MetaFilters {
            table_names: Some(vec!["t".to_owned()]),
            ..MetaFilters::default()
        };
        assert!(table_filtered.accepts_table("t"));
        assert!(!table_filtered.accepts_table("u"));
    }

    // WRITTEN test for :280: the TSO's physical half, truncated to seconds.
    #[test]
    fn version_to_time_reads_the_tso_physical_half() {
        // 1700000000123 ms since the epoch, shifted into a TSO.
        let version = 1_700_000_000_123u64 << PHYSICAL_SHIFT_BITS;
        let time = version_to_time(version, &chrono::Utc).expect("a valid instant");
        assert_eq!(time.kind(), TimeType::DateTime);
        // fsp 0: the 123 ms are not rendered.
        assert_eq!(time.to_string(), "2023-11-14 22:13:20");
        // A logical-only TSO has no physical part at all.
        assert!(version_to_time(0, &chrono::Utc).is_some());
    }

    // WRITTEN test for :108-133: the pseudo skip and the IsAnalyzed branch.
    #[test]
    fn stats_meta_row_nulls_the_analyze_time_when_never_analyzed() {
        let version = 1_700_000_000_000u64 << PHYSICAL_SHIFT_BITS;
        let mut stats = empty_stats_table();
        stats.version = version;
        stats.hist_coll.modify_count = 5;
        stats.hist_coll.realtime_count = 100;
        stats.last_analyze_version = 0;
        assert!(!stats.is_analyzed());

        let row = stats_meta_row("d", "t", &PartitionLabel::None, &stats, &chrono::Utc)
            .expect("a non-pseudo table produces a row");
        assert_eq!(row.len(), 7);
        assert_eq!(row[4], Datum::Int(5));
        assert_eq!(row[5], Datum::Int(100));
        // Never analyzed: NULL rather than the epoch.
        assert_eq!(row[6], Datum::Null);

        stats.last_analyze_version = version;
        assert!(stats.is_analyzed());
        let row = stats_meta_row("d", "t", &PartitionLabel::None, &stats, &chrono::Utc).unwrap();
        assert!(matches!(row[6], Datum::Time(_)));

        // A pseudo table produces no row at all.
        stats.hist_coll.pseudo = true;
        assert!(stats_meta_row("d", "t", &PartitionLabel::None, &stats, &chrono::Utc).is_none());
    }

    // WRITTEN test for :135-142.
    #[test]
    fn locked_row_always_says_locked() {
        let row = stats_locked_row("d", "t", &PartitionLabel::Definition("p0".to_owned()));
        assert_eq!(row.len(), 4);
        assert_eq!(row[2], Datum::new_string(b"p0".to_vec()));
        assert_eq!(row[3], Datum::new_string(b"locked".to_vec()));
    }

    // WRITTEN test for :258-278: the 15-column histogram row shape.
    #[test]
    fn histogram_row_has_fifteen_columns_in_gos_order() {
        let histogram = Histogram {
            id: 3,
            ndv: 42,
            null_count: 7,
            last_update_version: 1_700_000_000_000u64 << PHYSICAL_SHIFT_BITS,
            correlation: -0.5,
            ..Histogram::default()
        };
        let memory = ItemMemoryUsage {
            total: 100,
            histogram: 60,
            topn: 30,
            cms: 10,
        };
        let row = histogram_row(
            "d",
            "t",
            &PartitionLabel::Global,
            "idx",
            true,
            &histogram,
            0.0,
            "allLoaded",
            memory,
            &chrono::Utc,
        );
        assert_eq!(row.len(), 15);
        assert_eq!(row[4], Datum::Int(1));
        assert_eq!(row[6], Datum::Int(42));
        assert_eq!(row[7], Datum::Int(7));
        assert_eq!(row[8], Datum::Real(0.0));
        assert_eq!(row[9], Datum::Real(-0.5));
        assert_eq!(row[11], Datum::Int(100));
        assert_eq!(row[14], Datum::Int(10));
    }

    // WRITTEN test for :437-466: bucket rows, including the is_index flag
    // being derived from the column count.
    #[test]
    fn bucket_rows_derive_is_index_from_the_column_count() {
        let histogram = Histogram {
            buckets: vec![
                Bucket {
                    count: 10,
                    repeat: 2,
                    ndv: 5,
                    lower_bound: Datum::Int(1),
                    upper_bound: Datum::Int(9),
                },
                Bucket {
                    count: 25,
                    repeat: 3,
                    ndv: 6,
                    lower_bound: Datum::Int(10),
                    upper_bound: Datum::Int(19),
                },
            ],
            ..Histogram::default()
        };
        let mut render = |value: &Datum, _: usize, _: &[u8]| {
            Ok::<_, std::convert::Infallible>(format!("{value:?}"))
        };
        let rows = buckets_to_rows(
            "d",
            "t",
            &PartitionLabel::None,
            "c",
            0,
            &histogram,
            &[],
            &mut render,
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 11);
        // Column histogram: is_index 0.
        assert_eq!(rows[0][4], Datum::Int(0));
        assert_eq!(rows[0][5], Datum::Int(0));
        assert_eq!(rows[1][5], Datum::Int(1));
        // count is CUMULATIVE, straight from the bucket.
        assert_eq!(rows[1][6], Datum::Int(25));
        assert_eq!(rows[1][7], Datum::Int(3));
        assert_eq!(rows[1][10], Datum::Int(6));

        let rows = buckets_to_rows(
            "d",
            "t",
            &PartitionLabel::None,
            "i",
            2,
            &histogram,
            &[3, 3],
            &mut render,
        )
        .unwrap();
        assert_eq!(rows[0][4], Datum::Int(1));
    }

    // WRITTEN test for :411-435: a nil TopN produces no rows.
    #[test]
    fn topn_rows_are_absent_rather_than_empty_without_a_topn() {
        let mut render =
            |_: &Datum, _: usize, _: &[u8]| Ok::<_, std::convert::Infallible>("v".to_owned());
        let rows = topn_to_rows(
            "d",
            "t",
            &PartitionLabel::None,
            "c",
            1,
            false,
            None,
            &[3],
            &mut render,
        )
        .unwrap();
        assert!(rows.is_empty());

        let mut topn = TopN::new(2);
        topn.append(&[1], 9);
        topn.append(&[2], 4);
        let rows = topn_to_rows(
            "d",
            "t",
            &PartitionLabel::None,
            "c",
            1,
            false,
            Some(&topn),
            &[3],
            &mut render,
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].len(), 7);
        assert_eq!(rows[0][4], Datum::Int(0));
        assert_eq!(rows[0][6], Datum::Int(9));
        assert_eq!(rows[1][6], Datum::Int(4));
    }

    // WRITTEN test for :522-534: the pseudo skip versus a zero health.
    #[test]
    fn healthy_row_distinguishes_absent_from_zero() {
        let mut stats = empty_stats_table();
        stats.hist_coll.pseudo = true;
        assert!(healthy_row("d", "t", &PartitionLabel::None, &stats).is_none());

        // Non-pseudo and never analyzed: a real row holding 0.
        stats.hist_coll.pseudo = false;
        let row = healthy_row("d", "t", &PartitionLabel::None, &stats).unwrap();
        assert_eq!(row.len(), 4);
        assert_eq!(row[3], Datum::Int(0));
    }

    // WRITTEN test for :571-587 and :575-580: independently nullable
    // timestamps, and the label rule that differs from every other fetcher.
    #[test]
    fn column_stats_usage_nulls_each_timestamp_independently() {
        let row = column_stats_usage_row("d", "t", &PartitionLabel::None, "c", None, None);
        assert_eq!(row.len(), 6);
        assert_eq!(row[4], Datum::Null);
        assert_eq!(row[5], Datum::Null);

        // A partitioned table's table-level row is "global" regardless of the
        // pruning mode -- unlike PartitionTargets::for_table.
        assert_eq!(column_stats_usage_label(true, None), PartitionLabel::Global);
        assert_eq!(column_stats_usage_label(false, None), PartitionLabel::None);
        assert_eq!(
            column_stats_usage_label(true, Some("p0")),
            PartitionLabel::Definition("p0".to_owned())
        );
    }

    // WRITTEN test for :535-538.
    #[test]
    fn histograms_in_flight_is_one_column() {
        assert_eq!(histograms_in_flight_row(3), vec![Datum::Int(3)]);
    }
}
