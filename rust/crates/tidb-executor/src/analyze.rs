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

//! What `ANALYZE TABLE` COMPUTES: which columns and indexes it covers, how a
//! scanned row contributes to the sample, and the histogram/TopN pair each
//! one ends up with.
//!
//! Everything here is independent of where the rows came from and of where
//! the result is stored. That separation is the point: this engine has two
//! tiers that both run `ANALYZE`, and a table analyzed by one must estimate
//! identically to the same table analyzed by the other.
//!
//! * The cluster tier (`tidb_exec::cluster_analyze`) scans a table's record
//!   range through a transaction's snapshot and writes `mysql.stats_*` rows a
//!   Go TiDB reads back.
//! * The in-process tier ([`kv::analyze_kv_table`]) scans a
//!   [`crate::kv_table::KvTable`]'s own rows and publishes the result straight
//!   into the catalog's statistics map, which is what `EXPLAIN` reads.
//!
//! Both drive the SAME [`AnalyzePlan`] and the SAME [`AnalyzeRun`], so
//! neither can drift into estimating differently from the other. The Go
//! source of truth is `pkg/executor/analyze_col_v2.go` (the sampling) and
//! `pkg/statistics/builder.go` (the histogram), reached through
//! [`tidb_stats::row_sample_collector`] and [`tidb_stats::builder`].
//!
//! # What a caller still owns
//!
//! The row source, and the storage. A caller opens the scan, hands each row's
//! analyzed-column values to [`AnalyzeRun::push`], and decides what to do
//! with the [`AnalyzedTable`] that [`AnalyzeRun::finish`] returns. Its own
//! read failures never enter [`AnalyzeError`], which is why this module needs
//! no error type from either tier.
//!
//! # What is refused, and why refusing is the honest answer
//!
//! A value this engine cannot reproduce exactly is refused by name rather
//! than approximated: a wrong histogram is worse than no histogram, because
//! the planner trusts it and a missing one falls back to the pseudo rates
//! that are visibly labelled `stats:pseudo`.

pub mod kv;
pub mod panic_recovery;

use tidb_codec::{encode_key, encode_value};
use tidb_datatype::{Collation, Datum, EvalType, FieldType, FieldTypeCode};
use tidb_stats::builder::{build_hist_and_topn, BuildOptions, SampleCollector, SampleItem};
use tidb_stats::cmsketch::TopN;
use tidb_stats::histogram::Histogram;
use tidb_stats::row_sample_collector::{
    adjusted_sample_rate, RowSampleCollector, SamplePolicy, ScannedRow, SlotValue,
};
use tidb_stats::sample_bytes::MAX_SAMPLE_VALUE_LENGTH;

pub use tidb_stats::row_sample_collector::{SampleMemoryExceeded, SampleMemoryQuota};

/// `statistics.Version2`, the only generation this engine writes.
pub const STATS_VERSION_2: i64 = 2;

/// The system variable that carries [`AnalyzeOptions::memory_quota`].
///
/// Named here so the caller that reads a session's variables does not have to
/// depend on the variable registry to spell it.
pub const MEM_QUOTA_ANALYZE_VARIABLE: &str = "tidb_mem_quota_analyze";

/// Why one `ANALYZE TABLE` could not be computed.
///
/// A caller's own read failure is NOT here -- it drives the scan and keeps
/// its own error type (see the module doc).
#[derive(Debug)]
pub enum AnalyzeError {
    /// A sampled value could not be encoded into the domain the builder
    /// compares in.
    Encode(String),
    /// A table or statement shape whose statistics this engine does not
    /// produce. The detail names it.
    Unsupported(String),
    /// The kept sample outgrew `tidb_mem_quota_analyze`.
    MemoryQuota(SampleMemoryExceeded),
}

impl std::fmt::Display for AnalyzeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encode(detail) => write!(formatter, "a sampled value did not encode: {detail}"),
            Self::Unsupported(detail) => formatter.write_str(detail),
            Self::MemoryQuota(exceeded) => write!(formatter, "{exceeded}"),
        }
    }
}

impl std::error::Error for AnalyzeError {}

/// The knobs one `ANALYZE TABLE ... WITH ...` statement set.
///
/// Every field is already the *effective* value: the statement's, or the
/// session default when the statement named none. Which of the two it was
/// still matters -- Go switches its TopN and bucket-count heuristics off for
/// a value the user chose -- so the defaults travel alongside.
#[derive(Clone, Copy, Debug)]
pub struct AnalyzeOptions {
    /// `WITH n BUCKETS`.
    pub num_buckets: isize,
    /// `WITH m TOPN`.
    pub num_topn: isize,
    /// `WITH k SAMPLES`; `0` leaves the rate in charge.
    pub num_samples: usize,
    /// `WITH r SAMPLERATE`; `None` derives it from the table's row count.
    pub sample_rate: Option<f64>,
    /// `tidb_analyze_default_num_buckets`.
    pub default_num_buckets: u64,
    /// `tidb_analyze_default_num_topn`.
    pub default_num_topn: u64,
    /// `tidb_mem_quota_analyze`: the bound on what the kept sample may
    /// occupy. Go's default is `-1`, no bound.
    pub memory_quota: SampleMemoryQuota,
}

impl Default for AnalyzeOptions {
    fn default() -> Self {
        Self {
            num_buckets: tidb_stats::constants::DEFAULT_HISTOGRAM_BUCKETS as isize,
            num_topn: tidb_stats::constants::DEFAULT_TOP_N_VALUE as isize,
            num_samples: 0,
            sample_rate: None,
            default_num_buckets: tidb_stats::constants::DEFAULT_HISTOGRAM_BUCKETS as u64,
            default_num_topn: tidb_stats::constants::DEFAULT_TOP_N_VALUE as u64,
            memory_quota: SampleMemoryQuota::unlimited(),
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

/// Whether this statement is an `ANALYZE TABLE` this engine runs, and against
/// which tables.
///
/// `Ok(None)` means the statement is not an `ANALYZE TABLE` at all and takes
/// its ordinary path; an error names the clause that stopped it, at parse
/// time, before any read. A refusal a caller can print is worth more than a
/// partial analysis.
///
/// Both tiers ask this same question, so a clause one of them refuses is
/// refused by the other too -- an `ANALYZE TABLE ... INDEX i` that the
/// cluster tier declines must not quietly succeed in-process with statistics
/// of a different shape.
pub fn lower_analyze(
    statement: &tidb_ast::Stmt,
    default_schema: &str,
) -> Result<Option<Vec<AnalyzeStatement>>, AnalyzeError> {
    let tidb_ast::Stmt::Admin(admin) = statement else {
        return Ok(None);
    };
    lower_analyze_admin(admin, default_schema)
}

/// [`lower_analyze`] for a caller that has already unwrapped the statement.
pub fn lower_analyze_admin(
    admin: &tidb_ast::AdminStmt,
    default_schema: &str,
) -> Result<Option<Vec<AnalyzeStatement>>, AnalyzeError> {
    let analyze = match admin {
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
        let number = |value: &str| -> Result<u64, AnalyzeError> {
            value
                .parse::<u64>()
                .map_err(|_| AnalyzeError::Unsupported(format!("`{value}` is not a whole number")))
        };
        match option.kind {
            tidb_ast::AnalyzeOptionKind::Buckets => {
                options.num_buckets = isize::try_from(number(&option.value)?).map_err(|_| {
                    AnalyzeError::Unsupported(format!(
                        "`{}` exceeds the native ANALYZE integer domain",
                        option.value
                    ))
                })?;
            }
            tidb_ast::AnalyzeOptionKind::TopN => {
                options.num_topn = isize::try_from(number(&option.value)?).map_err(|_| {
                    AnalyzeError::Unsupported(format!(
                        "`{}` exceeds the native ANALYZE integer domain",
                        option.value
                    ))
                })?;
            }
            tidb_ast::AnalyzeOptionKind::Samples => {
                options.num_samples = usize::try_from(number(&option.value)?).map_err(|_| {
                    AnalyzeError::Unsupported(format!(
                        "`{}` exceeds the native sample-size domain",
                        option.value
                    ))
                })?;
            }
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

/// One column this `ANALYZE` builds a histogram for.
#[derive(Clone, Debug)]
pub struct AnalyzedColumn {
    /// Go `ColumnInfo.ID`, the `hist_id` the built histogram is stored under.
    pub id: i64,
    /// The column's lowercase name, for a row source that addresses columns
    /// by name.
    pub name: String,
    /// What a row with no entry for this column at all reads as.
    ///
    /// Not NULL: a row written before an `ALTER TABLE ... ADD COLUMN` carries
    /// nothing for the added column, and TiDB substitutes the column's
    /// `OriginDefaultValue` on read -- Go encodes it into the analyze scan
    /// request itself (`pkg/executor/builder.go:3246`
    /// `tables.SetPBColumnsDefaultValue`). Reading those rows as NULL would
    /// give the column a `null_count` of every old row and no bucket covering
    /// the default, so `WHERE c = <default>` would estimate ~0 rows.
    pub absent_value: Datum,
    /// The collation whose sort key replaces the value, when the column has
    /// one. `None` covers every non-string column and `ENUM`/`SET`, whose
    /// order is their declaration order rather than any collation's.
    pub collation: Option<Collation>,
}

impl AnalyzedColumn {
    /// The collation a column of this type is sampled under, or the refusal
    /// its collation earns.
    ///
    /// One rule for both tiers: a string column is stored and compared by its
    /// collation KEY, and a collation with no key generator has no sampling
    /// this engine can reproduce.
    pub fn sampling_collation(
        field_type: &FieldType,
        qualified_name: &str,
    ) -> Result<Option<Collation>, AnalyzeError> {
        if field_type.eval_type() != EvalType::String
            || field_type.code() == FieldTypeCode::Enum
            || field_type.code() == FieldTypeCode::Set
        {
            return Ok(None);
        }
        let collation = field_type.collation();
        if matches!(collation, Collation::Utf8Mb4ZhPinyinTiDbAsCs) {
            return Err(AnalyzeError::Unsupported(format!(
                "this node does not analyze {qualified_name}: it has no sort key for \
                 utf8mb4_zh_pinyin_tidb_as_cs"
            )));
        }
        Ok(Some(collation))
    }

    /// The value the histogram stores and compares.
    ///
    /// Under a new collation only the collation key orders the column
    /// correctly, so it -- not the text -- is what Go samples, stores and
    /// compares. That is also why `tidb_exec::cluster_stats_load` reads a
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

/// One index this `ANALYZE` builds a histogram for.
#[derive(Clone, Debug)]
pub struct AnalyzedIndex {
    /// Go `IndexInfo.ID`, the `hist_id` the built histogram is stored under.
    pub id: i64,
    /// Offsets into [`AnalyzePlan::columns`], in index-key order.
    pub column_positions: Vec<usize>,
    /// Go `IndexInfo.Unique` on a one-column index: what switches the TopN
    /// off for the index AND for the column it covers, because a value that
    /// occurs at most once has no "top" to list.
    pub single_column_unique: bool,
}

/// Which columns and indexes one table's `ANALYZE` covers, decided once.
#[derive(Clone, Debug)]
pub struct AnalyzePlan {
    columns: Vec<AnalyzedColumn>,
    indexes: Vec<AnalyzedIndex>,
    /// Column positions covered by a single-column unique index, which is
    /// what switches a column's TopN off.
    unique_covered: Vec<bool>,
}

impl AnalyzePlan {
    /// The plan over an already-vetted column and index list.
    ///
    /// Each tier derives the two lists from its own schema representation;
    /// everything downstream of them -- which slots exist, which TopNs are
    /// suppressed, how a row contributes -- is decided here, once.
    pub fn new(
        columns: Vec<AnalyzedColumn>,
        indexes: Vec<AnalyzedIndex>,
        table_name: &str,
    ) -> Result<Self, AnalyzeError> {
        if columns.is_empty() {
            return Err(AnalyzeError::Unsupported(format!(
                "`{table_name}` has no analyzable column"
            )));
        }
        let mut unique_covered = vec![false; columns.len()];
        for index in &indexes {
            for position in &index.column_positions {
                if *position >= columns.len() {
                    return Err(AnalyzeError::Unsupported(format!(
                        "an index of `{table_name}` covers a column that is not analyzable"
                    )));
                }
            }
            if let (true, Some(first)) = (
                index.single_column_unique,
                index.column_positions.first().copied(),
            ) {
                unique_covered[first] = true;
            }
        }
        Ok(Self {
            columns,
            indexes,
            unique_covered,
        })
    }

    /// The analyzed columns, in the order [`AnalyzeRun::push`] expects a
    /// row's values in.
    #[must_use]
    pub fn columns(&self) -> &[AnalyzedColumn] {
        &self.columns
    }

    /// The analyzed indexes, in the order [`AnalyzedTable::indexes`] reports
    /// them.
    #[must_use]
    pub fn indexes(&self) -> &[AnalyzedIndex] {
        &self.indexes
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
    /// TopN entry are built from, and what a loader reads back.
    stored: Vec<Datum>,
    slots: Vec<SlotContribution>,
}

/// One built histogram, before either tier decides where to keep it.
#[derive(Clone, Debug)]
pub struct AnalyzedHistogram {
    /// The column id, or the index id: `mysql.stats_histograms.hist_id`.
    pub id: i64,
    /// `stats_ver`, always [`STATS_VERSION_2`] here.
    pub stats_ver: i64,
    /// The histogram, with cumulative bucket counts.
    pub histogram: Histogram,
    /// The TopN, when this histogram has one.
    pub topn: Option<TopN>,
}

/// Everything one `ANALYZE TABLE` computed about one table.
#[derive(Clone, Debug)]
pub struct AnalyzedTable {
    /// Rows read: Go's new `mysql.stats_meta.count`.
    pub scanned_rows: i64,
    /// Rows the sampler kept.
    pub sampled_rows: i64,
    /// The Bernoulli rate in force, or `1.0` under `WITH n SAMPLES`.
    pub sample_rate: f64,
    /// One per [`AnalyzePlan::columns`] entry, in the same order.
    pub columns: Vec<AnalyzedHistogram>,
    /// One per [`AnalyzePlan::indexes`] entry, in the same order.
    pub indexes: Vec<AnalyzedHistogram>,
}

/// One `ANALYZE TABLE` in progress: the caller pushes rows, then finishes.
///
/// Splitting the scan from the computation is what lets the two tiers share
/// it: one pushes rows decoded from a transaction's snapshot, the other rows
/// decoded from an in-process store, and neither of their read failures has
/// to be a variant of [`AnalyzeError`].
pub struct AnalyzeRun<'a> {
    plan: &'a AnalyzePlan,
    options: AnalyzeOptions,
    collector: RowSampleCollector,
    sample_rate: f64,
    /// Both native callers scan one record-key range in ascending KV-handle
    /// order. Carry that order through the sampler so its heap layout cannot
    /// replace Go's post-merge `Handle.Compare` order.
    next_handle_order: i64,
}

impl<'a> AnalyzeRun<'a> {
    /// Opens the run.
    ///
    /// `realtime_count` is the table's current `mysql.stats_meta.count` --
    /// what Go's `getAdjustedSampleRate` reads -- and `None` means nothing
    /// has ever counted this table, which is Go's read-all-of-it case.
    pub fn start(
        plan: &'a AnalyzePlan,
        options: &AnalyzeOptions,
        realtime_count: Option<i64>,
    ) -> Result<Self, AnalyzeError> {
        let sample_rate = options
            .sample_rate
            .unwrap_or_else(|| adjusted_sample_rate(realtime_count, None));
        let policy = SamplePolicy::choose(options.num_samples, sample_rate).ok_or_else(|| {
            AnalyzeError::Unsupported(
                "ANALYZE TABLE needs a positive SAMPLES count or a positive SAMPLERATE".to_owned(),
            )
        })?;
        Ok(Self {
            plan,
            options: *options,
            collector: RowSampleCollector::with_memory_quota(
                plan.slot_count(),
                policy,
                options.memory_quota,
            ),
            sample_rate,
            next_handle_order: 0,
        })
    }

    /// Feeds one scanned row, its values in [`AnalyzePlan::columns`] order.
    /// Rows must arrive in ascending record-key/handle order; both native
    /// production scanners satisfy that contract.
    pub fn push(&mut self, columns: &[Datum]) -> Result<(), AnalyzeError> {
        let mut row = self.plan.row_of(columns)?;
        // Go's sample row contains the handle columns and rebuilds a Handle
        // after collectors merge. This engine has one ordered record-range
        // scan rather than region collectors, so the scan position is an
        // exact monotone handle-order key even for an implicit `_tidb_rowid`.
        // It travels as an internal final Datum and is never a stats slot.
        row.stored.push(Datum::Int(self.next_handle_order));
        self.next_handle_order = self.next_handle_order.wrapping_add(1);
        let slots: Vec<SlotValue<'_>> = row
            .slots
            .iter()
            .map(|slot| SlotValue {
                encoded_value: &slot.encoded_value,
                size: slot.size,
                is_null: slot.is_null,
            })
            .collect();
        self.collector
            .collect(&ScannedRow {
                columns: &row.stored,
                slots: &slots,
            })
            .map_err(AnalyzeError::MemoryQuota)
    }

    /// Builds every histogram from the rows pushed so far.
    pub fn finish(self) -> Result<AnalyzedTable, AnalyzeError> {
        let plan = self.plan;
        let (scanned_rows, slot_stats, sampled) =
            self.collector.into_parts(|columns| match columns.last() {
                Some(Datum::Int(order)) => {
                    Ok::<_, AnalyzeError>(tidb_txnkv::IntHandle::new(*order).into())
                }
                _ => Err(AnalyzeError::Unsupported(
                    "an internal sampled row lost its handle-order key".to_owned(),
                )),
            })?;

        let mut columns = Vec::with_capacity(plan.columns.len());
        for (position, column) in plan.columns.iter().enumerate() {
            let slot = &slot_stats[position];
            let mut collected = SampleCollector {
                samples: Vec::new(),
                null_count: slot.null_count,
                // Go's per-column count is `Count - NullCount`: a histogram
                // and a TopN describe values, and NULLs travel beside them.
                count: scanned_rows - slot.null_count,
                ndv: slot.ndv,
                total_size: slot.total_size,
            };
            for row in &sampled {
                let value = &row.columns[position];
                if value.is_null() {
                    continue;
                }
                // Go's length gate: a value this long is not one that occurs
                // many times, and storing it would put half a `LONGTEXT` in
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
            let mut built_with = self.options.build_options();
            // A column a single-column unique index covers has no repeated
            // value to put in a TopN, so Go asks for none rather than storing
            // a list of ones.
            if plan.unique_covered[position] {
                built_with.num_topn = 0;
            }
            let built = build_hist_and_topn(column.id, &collected, built_with, true);
            columns.push(AnalyzedHistogram {
                id: column.id,
                stats_ver: STATS_VERSION_2,
                histogram: built.histogram,
                topn: built.topn,
            });
        }

        let mut indexes = Vec::with_capacity(plan.indexes.len());
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
                    // Go leaves an index sample's ordinal at zero: correlation
                    // is a column-only statistic, and the builder does not
                    // read the ordinal when `isColumn` is false.
                    ordinal: 0,
                });
            }
            let mut built_with = self.options.build_options();
            if index.single_column_unique {
                built_with.num_topn = 0;
            }
            let built = build_hist_and_topn(index.id, &collected, built_with, false);
            indexes.push(AnalyzedHistogram {
                id: index.id,
                stats_ver: STATS_VERSION_2,
                histogram: built.histogram,
                topn: built.topn,
            });
        }

        Ok(AnalyzedTable {
            scanned_rows,
            sampled_rows: sampled.len() as i64,
            sample_rate: self.sample_rate,
            columns,
            indexes,
        })
    }
}

/// Go's `len(datum.GetBytes())` length gate, which only ever fires for the
/// byte-valued domains.
pub(crate) fn value_length(value: &Datum) -> usize {
    match value {
        Datum::Bytes(bytes) => bytes.len(),
        Datum::String(string) => string.bytes().len(),
        _ => 0,
    }
}

fn encode_key_of(value: &Datum) -> Result<Vec<u8>, AnalyzeError> {
    encode_key(std::slice::from_ref(value)).map_err(|error| AnalyzeError::Encode(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_int_column_plan() -> AnalyzePlan {
        AnalyzePlan::new(
            vec![AnalyzedColumn {
                id: 1,
                name: "a".to_owned(),
                absent_value: Datum::Null,
                collation: None,
            }],
            Vec::new(),
            "t",
        )
        .unwrap()
    }

    #[test]
    fn stats_builder_options_preserve_the_signed_go_integer_domain() {
        let options = AnalyzeOptions {
            num_buckets: -1,
            num_topn: -2,
            default_num_buckets: u64::MAX,
            default_num_topn: u64::MAX - 1,
            ..AnalyzeOptions::default()
        };
        let built = options.build_options();
        assert_eq!(built.num_buckets, -1);
        assert_eq!(built.num_topn, -2);
        assert_eq!(built.default_num_buckets, u64::MAX);
        assert_eq!(built.default_num_topn, u64::MAX - 1);
    }

    #[test]
    fn reservoir_heap_order_does_not_replace_physical_handle_order() {
        let plan = one_int_column_plan();
        let options = AnalyzeOptions {
            num_buckets: 3,
            num_topn: 0,
            num_samples: 3,
            ..AnalyzeOptions::default()
        };
        let mut run = AnalyzeRun::start(&plan, &options, None).unwrap();
        // Both production row sources call `push` in record-key/handle order.
        // The value order is intentionally different, making correlation pin
        // the sample ordinals rather than merely the chosen row set.
        run.push(&[Datum::Int(3)]).unwrap();
        run.push(&[Datum::Int(1)]).unwrap();
        run.push(&[Datum::Int(2)]).unwrap();
        let analyzed = run.finish().unwrap();
        assert_eq!(analyzed.sampled_rows, 3);
        assert_eq!(analyzed.columns[0].histogram.correlation, -0.5);
    }
}
