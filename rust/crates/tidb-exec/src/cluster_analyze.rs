// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `ANALYZE TABLE`: reading a table's rows and turning them into the
//! statistics a planner estimates from.
//!
//! This is the WRITE half of [`crate::cluster_stats_load`]. That module reads
//! `mysql.stats_*` rows a Go `ANALYZE` wrote; this one produces the same
//! rows, in the same shapes, so that a Go TiDB reading them back cannot tell
//! which node wrote them. The value of the pair is the differential: `SHOW
//! STATS_BUCKETS` on a Go server renders what this builds, and its `EXPLAIN`
//! estimates from it.
//!
//! # Where the samples come from, and how that differs from Go
//!
//! Go pushes sampling into TiKV: the coprocessor runs `AnalyzeReq` against
//! each region, samples there, and TiDB merges the per-region collectors
//! (`analyze_col_sampling.go`). This node scans the table's record range
//! through the same snapshot every other read on it uses and samples
//! in-process. The *mechanism* differs; the *algorithm* does not --
//! [`tidb_stats::row_sample_collector`] is Go's collector, weight rule and
//! all -- and the sample it draws has the same distribution, because both
//! Bernoulli and reservoir selection are indifferent to where the rows were
//! split.
//!
//! Two consequences are worth stating rather than hiding. Every row crosses
//! the wire, so this costs a full table read where Go costs a sampled one;
//! and the transaction that reads the rows is the transaction that writes the
//! statistics, so the row count and the histograms are one consistent view
//! rather than two.
//!
//! # The one deliberate divergence, and why it is harmless
//!
//! Go's FM sketch hashes a value's *doubly* encoded form -- its column
//! encoding, wrapped again as a byte string by `codec.EncodeValue` -- because
//! the collector receives a coprocessor result set whose fields are already
//! bytes. This hashes the value's own `codec.EncodeValue` once. An FM
//! sketch's NDV depends only on the *set* of distinct hashes, so any
//! injective encoding gives the same estimator; only the sketch's
//! randomisation differs, which is already different between two Go runs.
//! What must match, and does, is which values are considered the same: a
//! new-collation string column is hashed by its collation key here exactly as
//! it is there, so `'a'` and `'A'` are one distinct value under
//! `utf8mb4_general_ci` on both.
//!
//! # What this refuses, and why refusing is the honest answer
//!
//! Anything whose sampled value this node cannot reproduce *exactly* is
//! refused by name rather than approximated, because a wrong histogram is
//! worse than no histogram: the planner trusts it. See [`AnalyzeError`].

use std::collections::BTreeMap;

use tidb_codec::{encode_key, encode_value};
use tidb_datatype::{Collation, Datum, EvalType, FieldTypeCode, UNSPECIFIED_LENGTH};
use tidb_model::table_info::TableInfo;
use tidb_model::SchemaState;
use tidb_stats::builder::{build_hist_and_topn, BuildOptions, SampleCollector, SampleItem};
use tidb_stats::row_sample_collector::{
    adjusted_sample_rate, RowSampleCollector, SamplePolicy, ScannedRow, SlotValue,
};
use tidb_stats::sample_bytes::MAX_SAMPLE_VALUE_LENGTH;

use crate::cluster_catalog::MetaSnapshot;
use crate::cluster_stats_load::{ClusterStatsItem, ClusterTableStats};
use crate::mysql_system_tables::{scan_system_table, SystemRow, SystemTableError, SystemTableView};

/// `statistics.Version2`, the only generation this node writes.
pub const STATS_VERSION_2: i64 = 2;

/// Why one `ANALYZE TABLE` could not run.
#[derive(Debug)]
pub enum AnalyzeError {
    /// The table's rows could not be read.
    Read(SystemTableError),
    /// A sampled value could not be encoded into the domain the builder
    /// compares in.
    Encode(String),
    /// A table shape whose statistics this node does not produce. The detail
    /// names it.
    Unsupported(String),
}

impl std::fmt::Display for AnalyzeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(error) => write!(formatter, "{error}"),
            Self::Encode(detail) => write!(formatter, "a sampled value did not encode: {detail}"),
            Self::Unsupported(detail) => formatter.write_str(detail),
        }
    }
}

impl std::error::Error for AnalyzeError {}

impl From<SystemTableError> for AnalyzeError {
    fn from(error: SystemTableError) -> Self {
        Self::Read(error)
    }
}

/// The knobs one `ANALYZE TABLE ... WITH ...` statement set.
///
/// Every field is already the *effective* value: the statement's, or the
/// session default when the statement named none. Which of the two it was
/// still matters -- Go switches its TopN and bucket-count heuristics off for
/// a value the user chose -- so the defaults travel alongside.
#[derive(Clone, Copy, Debug)]
pub struct AnalyzeOptions {
    /// `WITH n BUCKETS`.
    pub num_buckets: usize,
    /// `WITH m TOPN`.
    pub num_topn: usize,
    /// `WITH k SAMPLES`; `0` leaves the rate in charge.
    pub num_samples: usize,
    /// `WITH r SAMPLERATE`; `None` derives it from the table's row count.
    pub sample_rate: Option<f64>,
    /// `tidb_analyze_default_num_buckets`.
    pub default_num_buckets: usize,
    /// `tidb_analyze_default_num_topn`.
    pub default_num_topn: usize,
}

impl Default for AnalyzeOptions {
    fn default() -> Self {
        Self {
            num_buckets: tidb_stats::constants::DEFAULT_HISTOGRAM_BUCKETS,
            num_topn: tidb_stats::constants::DEFAULT_TOP_N_VALUE,
            num_samples: 0,
            sample_rate: None,
            default_num_buckets: tidb_stats::constants::DEFAULT_HISTOGRAM_BUCKETS,
            default_num_topn: tidb_stats::constants::DEFAULT_TOP_N_VALUE,
        }
    }
}

impl AnalyzeOptions {
    /// The subset the histogram builder reads.
    #[must_use]
    pub const fn build_options(&self) -> BuildOptions {
        BuildOptions {
            num_buckets: self.num_buckets,
            num_topn: self.num_topn,
            default_num_buckets: self.default_num_buckets,
            default_num_topn: self.default_num_topn,
        }
    }
}

/// One table an `ANALYZE TABLE` statement names, with the knobs it carries.
#[derive(Clone, Debug)]
pub struct AnalyzeStatement {
    /// The schema the table lives in, already resolved against the session's
    /// current database.
    pub schema: String,
    /// The table's name.
    pub table: String,
    /// The effective knobs.
    pub options: AnalyzeOptions,
}

/// Whether this statement is an `ANALYZE TABLE` this node runs, and against
/// which tables.
///
/// `Ok(None)` means the statement is not an `ANALYZE TABLE` at all and takes
/// its ordinary path; an error names the clause that stopped it, at parse
/// time, before any read. This mirrors [`crate::cluster_ddl::lower_ddl`]: a
/// refusal a caller can print is worth more than a partial analysis.
pub fn lower_analyze(
    statement: &tidb_ast::Stmt,
    default_schema: &str,
) -> Result<Option<Vec<AnalyzeStatement>>, AnalyzeError> {
    let tidb_ast::Stmt::Admin(admin) = statement else {
        return Ok(None);
    };
    let analyze = match admin.as_ref() {
        tidb_ast::AdminStmt::AnalyzeTable(analyze) => analyze.as_ref(),
        tidb_ast::AdminStmt::AnalyzeIncremental(_) => {
            return Err(AnalyzeError::Unsupported(
                "this node does not run ANALYZE INCREMENTAL TABLE: it extends the previous \
                 histogram from its last bound rather than rebuilding one"
                    .to_owned(),
            ))
        }
        _ => return Ok(None),
    };
    if !analyze.partitions.is_empty() {
        return Err(AnalyzeError::Unsupported(
            "this node does not analyze named partitions".to_owned(),
        ));
    }
    match &analyze.target {
        tidb_ast::AnalyzeTarget::Default | tidb_ast::AnalyzeTarget::AllColumns => {}
        tidb_ast::AnalyzeTarget::Index(_) => {
            return Err(AnalyzeError::Unsupported(
                "this node does not run ANALYZE TABLE ... INDEX: it rewrites a table's whole \
                 statistics, and storing only some of them would leave the rest describing an \
                 older row count"
                    .to_owned(),
            ))
        }
        tidb_ast::AnalyzeTarget::PredicateColumns | tidb_ast::AnalyzeTarget::Columns(_) => {
            return Err(AnalyzeError::Unsupported(
                "this node analyzes every column of the table; a column list would leave the \
                 unnamed columns' histograms stamped with a version their rows no longer match"
                    .to_owned(),
            ))
        }
        tidb_ast::AnalyzeTarget::Histogram { .. } => {
            return Err(AnalyzeError::Unsupported(
                "this node does not run UPDATE/DROP HISTOGRAM ON".to_owned(),
            ))
        }
    }

    let mut options = AnalyzeOptions::default();
    for option in &analyze.options {
        let number = |value: &str| -> Result<usize, AnalyzeError> {
            value
                .parse::<usize>()
                .map_err(|_| AnalyzeError::Unsupported(format!("`{value}` is not a whole number")))
        };
        match option.kind {
            tidb_ast::AnalyzeOptionKind::Buckets => options.num_buckets = number(&option.value)?,
            tidb_ast::AnalyzeOptionKind::TopN => options.num_topn = number(&option.value)?,
            tidb_ast::AnalyzeOptionKind::Samples => options.num_samples = number(&option.value)?,
            tidb_ast::AnalyzeOptionKind::SampleRate => {
                let rate = option.value.parse::<f64>().map_err(|_| {
                    AnalyzeError::Unsupported(format!("`{}` is not a rate", option.value))
                })?;
                if !(0.0..=1.0).contains(&rate) {
                    return Err(AnalyzeError::Unsupported(format!(
                        "SAMPLERATE must be in [0, 1], not `{}`",
                        option.value
                    )));
                }
                options.sample_rate = Some(rate);
            }
            // Analyze v2 stores no CMSketch at all, so accepting a size for
            // one would be accepting a knob with no effect.
            tidb_ast::AnalyzeOptionKind::CmSketchDepth
            | tidb_ast::AnalyzeOptionKind::CmSketchWidth => {
                return Err(AnalyzeError::Unsupported(
                    "CMSKETCH DEPTH/WIDTH have no effect on this node: analyze v2 stores no \
                     CMSketch"
                        .to_owned(),
                ))
            }
            tidb_ast::AnalyzeOptionKind::NdvRate => {
                return Err(AnalyzeError::Unsupported(
                    "NDVRATE is not a knob this node reads".to_owned(),
                ))
            }
        }
    }

    let mut tables = Vec::with_capacity(analyze.tables.len());
    for path in &analyze.tables {
        let (schema, table) = match path.as_slice() {
            [table] => (default_schema.to_owned(), table.clone()),
            [schema, table] => (schema.clone(), table.clone()),
            _ => {
                return Err(AnalyzeError::Unsupported(format!(
                    "`{}` does not name a table",
                    path.join(".")
                )))
            }
        };
        if schema.is_empty() {
            return Err(AnalyzeError::Unsupported("no database selected".to_owned()));
        }
        tables.push(AnalyzeStatement {
            schema,
            table,
            options,
        });
    }
    Ok(Some(tables))
}

/// What one `ANALYZE TABLE` produced, plus the receipt of how it got there.
#[derive(Clone, Debug)]
pub struct AnalyzeReport {
    /// The statistics, in exactly the shape
    /// [`crate::cluster_stats_load::ClusterStatsLoader`] reads back.
    pub stats: ClusterTableStats,
    /// Rows read.
    pub scanned_rows: i64,
    /// Rows the sampler kept.
    pub sampled_rows: i64,
    /// The Bernoulli rate in force, or `1.0` under `WITH n SAMPLES`.
    pub sample_rate: f64,
}

/// Runs one `ANALYZE TABLE` against one snapshot.
///
/// `realtime_count` is the table's `mysql.stats_meta.count` as this snapshot
/// sees it -- what Go's `getAdjustedSampleRate` calls `RealtimeCount` -- and
/// `None` means the table has no row there at all.
///
/// `version` is the TSO the statistics are stamped with, and must be the
/// `start_ts` of the transaction that will store them: that is what makes a
/// concurrent `ANALYZE` on a Go node either lose the write conflict or be
/// ordered after this one, rather than interleave with it.
pub fn analyze_table<S: MetaSnapshot>(
    snapshot: &mut S,
    table: &TableInfo,
    options: &AnalyzeOptions,
    realtime_count: Option<i64>,
    version: u64,
) -> Result<AnalyzeReport, AnalyzeError> {
    let plan = AnalyzePlan::of(table)?;
    let sample_rate = options
        .sample_rate
        .unwrap_or_else(|| adjusted_sample_rate(realtime_count, None));
    let policy = SamplePolicy::choose(options.num_samples, sample_rate).ok_or_else(|| {
        AnalyzeError::Unsupported(
            "ANALYZE TABLE needs a positive SAMPLES count or a positive SAMPLERATE".to_owned(),
        )
    })?;

    let view = SystemTableView::project(table.name.original(), table, &plan.column_names());
    let mut collector = RowSampleCollector::new(plan.slot_count(), policy);
    for (key, value) in scan_system_table(snapshot, &view)? {
        let stored = SystemRow::parse(&view, &key, &value)?;
        let mut columns = Vec::with_capacity(plan.columns.len());
        for column in &plan.columns {
            columns.push(stored.datum(&column.name)?.cloned().unwrap_or(Datum::Null));
        }
        let row = plan.row_of(&columns)?;
        let slots: Vec<SlotValue<'_>> = row
            .slots
            .iter()
            .map(|slot| SlotValue {
                encoded_value: &slot.encoded_value,
                size: slot.size,
                is_null: slot.is_null,
            })
            .collect();
        collector.collect(&ScannedRow {
            columns: &row.stored,
            slots: &slots,
        });
    }
    let (scanned_rows, slot_stats, sampled) = collector.into_parts();

    let mut stats = ClusterTableStats {
        table_id: table.id,
        version,
        // Go's `SaveAnalyzeResultToStorage` stores
        // `max(curModifyCnt - results.BaseModifyCnt, 0)`, which for a
        // non-incremental `ANALYZE` of the whole table is zero: every
        // modification the previous count described has now been measured.
        modify_count: 0,
        row_count: u64::try_from(scanned_rows).unwrap_or_default(),
        columns: Vec::new(),
        indexes: Vec::new(),
    };

    for (position, column) in plan.columns.iter().enumerate() {
        let slot = &slot_stats[position];
        let mut collected = SampleCollector {
            samples: Vec::new(),
            null_count: slot.null_count,
            // Go's per-column count is `Count - NullCount`: a histogram and a
            // TopN describe values, and NULLs travel beside them.
            count: scanned_rows - slot.null_count,
            ndv: slot.ndv,
            total_size: slot.total_size,
        };
        for row in &sampled {
            let value = &row.columns[position];
            if value.is_null() {
                continue;
            }
            // Go's length gate: a value this long is not one that occurs many
            // times, and storing it would put half a `LONGTEXT` in
            // `mysql.stats_buckets`.
            if value_length(value) > MAX_SAMPLE_VALUE_LENGTH {
                continue;
            }
            collected.samples.push(SampleItem {
                encoded: encode_key_of(value)?,
                value: value.clone(),
                ordinal: row.ordinal,
            });
        }
        let mut built_with = options.build_options();
        // A column a single-column unique index covers has no repeated value
        // to put in a TopN, so Go asks for none rather than storing a list of
        // ones.
        if plan.unique_covered[position] {
            built_with.num_topn = 0;
        }
        let built = build_hist_and_topn(column.id, &collected, built_with, true);
        stats.columns.push(ClusterStatsItem {
            id: column.id,
            is_index: false,
            stats_ver: STATS_VERSION_2,
            flag: 0,
            histogram: built.histogram,
            topn: built.topn,
            cms: None,
        });
    }

    for (position, index) in plan.indexes.iter().enumerate() {
        let slot = &slot_stats[plan.index_slot(position)];
        let mut collected = SampleCollector {
            samples: Vec::new(),
            null_count: slot.null_count,
            count: scanned_rows - slot.null_count,
            ndv: slot.ndv,
            total_size: slot.total_size,
        };
        for row in &sampled {
            let Some(encoded) = plan.index_sample(index, &row.columns)? else {
                continue;
            };
            collected.samples.push(SampleItem {
                value: Datum::Bytes(encoded.clone()),
                encoded,
                // Go leaves an index sample's ordinal at zero: correlation is
                // a column-only statistic, and the builder does not read the
                // ordinal when `isColumn` is false.
                ordinal: 0,
            });
        }
        let mut built_with = options.build_options();
        if index.single_column_unique {
            built_with.num_topn = 0;
        }
        let built = build_hist_and_topn(index.id, &collected, built_with, false);
        stats.indexes.push(ClusterStatsItem {
            id: index.id,
            is_index: true,
            stats_ver: STATS_VERSION_2,
            flag: 0,
            histogram: built.histogram,
            topn: built.topn,
            cms: None,
        });
    }

    Ok(AnalyzeReport {
        stats,
        scanned_rows,
        sampled_rows: sampled.len() as i64,
        sample_rate,
    })
}

/// One column this `ANALYZE` builds a histogram for.
struct AnalyzedColumn {
    id: i64,
    name: String,
    /// The collation whose sort key replaces the value, when the column has
    /// one. `None` covers every non-string column and `ENUM`/`SET`, whose
    /// order is their declaration order rather than any collation's.
    collation: Option<Collation>,
}

/// One index this `ANALYZE` builds a histogram for.
struct AnalyzedIndex {
    id: i64,
    /// Offsets into [`AnalyzePlan::columns`], in index-key order.
    column_positions: Vec<usize>,
    single_column_unique: bool,
}

/// Which columns and indexes one table's `ANALYZE` covers, decided once.
struct AnalyzePlan {
    columns: Vec<AnalyzedColumn>,
    indexes: Vec<AnalyzedIndex>,
    /// Column positions covered by a single-column unique index, which is
    /// what switches a column's TopN off.
    unique_covered: Vec<bool>,
}

/// One slot's contribution for one scanned row.
struct SlotContribution {
    encoded_value: Vec<u8>,
    size: i64,
    is_null: bool,
}

/// One scanned row, in the two forms the collector needs.
struct ScannedRowValues {
    /// The values as the histogram stores them: a string column's collation
    /// key, every other column's own value. This is what a bucket bound and a
    /// TopN entry are built from, and what the loader reads back.
    stored: Vec<Datum>,
    slots: Vec<SlotContribution>,
}

impl AnalyzePlan {
    fn of(table: &TableInfo) -> Result<Self, AnalyzeError> {
        if table.partition.is_some() {
            return Err(AnalyzeError::Unsupported(format!(
                "this node does not analyze the partitioned table `{}`: its statistics are one \
                 set per partition plus a merged global set, which is a separate write path",
                table.name.original()
            )));
        }
        let mut columns = Vec::new();
        let mut by_offset: BTreeMap<i32, usize> = BTreeMap::new();
        for column in table.cols() {
            if column.state != SchemaState::PUBLIC || column.hidden {
                continue;
            }
            if column.is_generated() {
                return Err(AnalyzeError::Unsupported(format!(
                    "this node does not analyze `{}`.`{}`: a generated column's value is an \
                     expression it does not evaluate over stored rows",
                    table.name.original(),
                    column.name.original()
                )));
            }
            let collation = if column.field_type.eval_type() == EvalType::String
                && column.field_type.code() != FieldTypeCode::Enum
                && column.field_type.code() != FieldTypeCode::Set
            {
                let collation = column.field_type.collation();
                if matches!(collation, Collation::Utf8Mb4ZhPinyinTiDbAsCs) {
                    return Err(AnalyzeError::Unsupported(format!(
                        "this node does not analyze `{}`.`{}`: it has no sort key for \
                         utf8mb4_zh_pinyin_tidb_as_cs",
                        table.name.original(),
                        column.name.original()
                    )));
                }
                Some(collation)
            } else {
                None
            };
            by_offset.insert(column.offset, columns.len());
            columns.push(AnalyzedColumn {
                id: column.id,
                name: column.name.lowercase().to_owned(),
                collation,
            });
        }
        if columns.is_empty() {
            return Err(AnalyzeError::Unsupported(format!(
                "`{}` has no analyzable column",
                table.name.original()
            )));
        }

        let mut indexes = Vec::new();
        let mut unique_covered = vec![false; columns.len()];
        for index in &table.indices {
            if index.state != SchemaState::PUBLIC {
                continue;
            }
            if index.mv_index
                || index.global
                || index.vector_info.is_some()
                || index.inverted_info.is_some()
                || index.full_text_info.is_some()
            {
                return Err(AnalyzeError::Unsupported(format!(
                    "this node does not analyze index `{}` on `{}`: its keys are not the plain \
                     column encoding a stored histogram bound is read back as",
                    index.name.original(),
                    table.name.original()
                )));
            }
            let mut column_positions = Vec::with_capacity(index.columns.len());
            for index_column in &index.columns {
                if i64::from(index_column.length) != UNSPECIFIED_LENGTH {
                    return Err(AnalyzeError::Unsupported(format!(
                        "this node does not analyze the prefix index `{}` on `{}`: its sampled \
                         value is each column value cut to the prefix, which it does not cut",
                        index.name.original(),
                        table.name.original()
                    )));
                }
                let position = by_offset
                    .get(&index_column.offset)
                    .copied()
                    .ok_or_else(|| {
                        AnalyzeError::Unsupported(format!(
                            "index `{}` covers a column of `{}` that is not analyzable",
                            index.name.original(),
                            table.name.original()
                        ))
                    })?;
                column_positions.push(position);
            }
            let single_column_unique = index.unique && column_positions.len() == 1;
            if single_column_unique {
                unique_covered[column_positions[0]] = true;
            }
            indexes.push(AnalyzedIndex {
                id: index.id,
                column_positions,
                single_column_unique,
            });
        }

        Ok(Self {
            columns,
            indexes,
            unique_covered,
        })
    }

    fn column_names(&self) -> Vec<&str> {
        self.columns
            .iter()
            .map(|column| column.name.as_str())
            .collect()
    }

    /// Go's collector counts one slot per column, then one per *multi*-column
    /// group; a single-column index's facts are its column's own and are
    /// copied rather than recounted (`row_sampler.go:215`).
    fn slot_count(&self) -> usize {
        self.columns.len() + self.multi_column_indexes().count()
    }

    fn multi_column_indexes(&self) -> impl Iterator<Item = &AnalyzedIndex> {
        self.indexes
            .iter()
            .filter(|index| index.column_positions.len() > 1)
    }

    fn index_slot(&self, index_position: usize) -> usize {
        let index = &self.indexes[index_position];
        if index.column_positions.len() == 1 {
            return index.column_positions[0];
        }
        self.columns.len()
            + self.indexes[..index_position]
                .iter()
                .filter(|earlier| earlier.column_positions.len() > 1)
                .count()
    }

    /// One row's stored values and its contribution to every slot.
    fn row_of(&self, columns: &[Datum]) -> Result<ScannedRowValues, AnalyzeError> {
        let mut stored = Vec::with_capacity(columns.len());
        let mut slots = Vec::with_capacity(self.slot_count());
        let mut sizes = Vec::with_capacity(columns.len());
        for (position, value) in columns.iter().enumerate() {
            // The size is deliberately the ORIGINAL value's, not the
            // collation key's: Go computes it before the substitution and
            // says so, because `tot_col_size` describes what the table
            // stores.
            let size = encode_value(std::slice::from_ref(value))
                .map_err(|error| AnalyzeError::Encode(error.to_string()))?
                .len() as i64
                - 1;
            let keyed = self.columns[position].stored_value(value);
            let encoded_value = encode_value(std::slice::from_ref(&keyed))
                .map_err(|error| AnalyzeError::Encode(error.to_string()))?;
            sizes.push(size);
            slots.push(SlotContribution {
                encoded_value,
                size,
                is_null: value.is_null(),
            });
            stored.push(keyed);
        }
        for index in self.multi_column_indexes() {
            let group: Vec<Datum> = index
                .column_positions
                .iter()
                .map(|position| stored[*position].clone())
                .collect();
            let encoded_value =
                encode_value(&group).map_err(|error| AnalyzeError::Encode(error.to_string()))?;
            let size = index
                .column_positions
                .iter()
                .filter(|position| !columns[**position].is_null())
                .map(|position| sizes[*position])
                .sum();
            slots.push(SlotContribution {
                encoded_value,
                size,
                // Go hashes a group's datums whatever they are and keeps no
                // null count for one, so a group slot is never NULL.
                is_null: false,
            });
        }
        Ok(ScannedRowValues { stored, slots })
    }

    /// One index sample: the index key the histogram is built over.
    ///
    /// `None` when Go would skip the row -- a single-column index whose
    /// column is NULL, or any member past the sample length gate.
    fn index_sample(
        &self,
        index: &AnalyzedIndex,
        stored: &[Datum],
    ) -> Result<Option<Vec<u8>>, AnalyzeError> {
        if index.column_positions.len() == 1 && stored[index.column_positions[0]].is_null() {
            return Ok(None);
        }
        let mut key = Vec::new();
        for position in &index.column_positions {
            let value = &stored[*position];
            if value_length(value) > MAX_SAMPLE_VALUE_LENGTH {
                return Ok(None);
            }
            key.extend_from_slice(&encode_key_of(value)?);
        }
        Ok(Some(key))
    }
}

impl AnalyzedColumn {
    /// The value the histogram stores and compares.
    ///
    /// Under a new collation only the collation key orders the column
    /// correctly, so it -- not the text -- is what Go samples, stores and
    /// compares. That is also why [`crate::cluster_stats_load`] reads a
    /// string bound back as raw bytes rather than at the column's declared
    /// type: the stored bound may not even be valid in the column's charset.
    fn stored_value(&self, value: &Datum) -> Datum {
        let Some(collation) = self.collation else {
            return value.clone();
        };
        match value {
            Datum::String(string) => Datum::Bytes(collation.key(string.bytes())),
            Datum::Bytes(bytes) => Datum::Bytes(collation.key(bytes)),
            other => other.clone(),
        }
    }
}

/// Go's `len(datum.GetBytes())` length gate, which only ever fires for the
/// byte-valued domains.
fn value_length(value: &Datum) -> usize {
    match value {
        Datum::Bytes(bytes) => bytes.len(),
        Datum::String(string) => string.bytes().len(),
        _ => 0,
    }
}

fn encode_key_of(value: &Datum) -> Result<Vec<u8>, AnalyzeError> {
    encode_key(std::slice::from_ref(value)).map_err(|error| AnalyzeError::Encode(error.to_string()))
}
