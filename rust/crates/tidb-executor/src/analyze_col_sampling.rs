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

//! `pkg/executor/analyze_col_sampling.go`: the EXECUTOR-side driving logic of
//! the v2 (row-sampling) `ANALYZE` path -- `analyzeColumnsPushDown` (:53),
//! `buildSamplingStats` (:193), `subMergeWorker` (:596) and `subBuildWorker`
//! (:712).
//!
//! # What this file is, and what it deliberately is not
//!
//! The statistics THEMSELVES -- the reservoir/Bernoulli row sampler, the FM
//! sketch, the sample collector, and the histogram/TopN builder -- are already
//! transcreated in `tidb-stats` and are REUSED verbatim here rather than
//! restated:
//!
//! * [`tidb_stats::row_sample_collector::RowSampleCollector`] IS Go
//!   `statistics.RowSampleCollector` + `baseCollector`, including
//!   `MergeCollector`'s two policy-dependent behaviours, its `MemSize`
//!   arithmetic, and `DestroyAndPutToPool`.
//! * [`RowSampleCollector::into_parts`] IS Go :303-322 together: build every
//!   sampled row's handle via `HandleCols.BuildHandleByDatums`, sort the
//!   samples by `Handle.Compare`, and number them -- the step that makes the
//!   physical-order correlation computable after merging destroyed scan order.
//! * [`tidb_stats::builder::try_build_hist_and_topn`] IS Go
//!   `statistics.BuildHistAndTopN`.
//! * [`tidb_stats::sample_bytes::MAX_SAMPLE_VALUE_LENGTH`] IS Go
//!   `statistics.MaxSampleValueLength`.
//!
//! What is NOT in `tidb-stats`, and therefore what this file is, is the
//! executor's *driving* logic around them: which indexes get only a pushed-down
//! NDV, how a merged root collector is turned into one `SampleCollector` PER
//! column and PER index, the `WITH n TOPN` override for unique indexes, and how
//! the resulting histogram vector is split back into a column result and a
//! column-group result.
//!
//! # Sequential here, concurrent in Go
//!
//! Go runs three concurrent stages over channels:
//!
//! 1. `readDataAndSendTask` (:919) pulls raw `tipb.AnalyzeColumnsResp` bytes
//!    from the distsql result handler onto `mergeTaskCh`.
//! 2. `samplingStatsConcurrency` copies of `subMergeWorker` (:596) each keep a
//!    PRIVATE `RowSampleCollector`, merge whatever batches they win off the
//!    channel into it, and flush that one collector onto `mergeResultCh` when
//!    the channel closes. A single goroutine (:245) then merges each worker's
//!    flushed collector into the root collector.
//! 3. `samplingStatsConcurrency` copies of `subBuildWorker` (:712) take
//!    `samplingBuildTask`s off a channel and write `hists[slicePos]` /
//!    `topns[slicePos]`.
//!
//! Here all three are sequential: [`merge_sampled_collectors`] is a fold, and
//! [`build_sampling_stats`] is a loop over the same task list. What that costs
//! and what it does not:
//!
//! * **Not lost: the merge RESULT.** Stage 2's answer is deliberately
//!   order-independent. Counts, null counts, total sizes and FM sketches are
//!   commutative sums/unions. The reservoir is not commutative in WHICH rows
//!   survive -- but the survivors are chosen by each row's random WEIGHT, and
//!   Go's `MergeCollector` keeps the `max_sample_size` largest weights
//!   regardless of grouping, so the surviving SET is the same for any grouping
//!   of the same weighted rows. The two-level fan-in (batch -> worker
//!   collector -> root) therefore exists purely to parallelise, and folding it
//!   flat is the same answer. The one arithmetic that IS grouping-dependent is
//!   Go `MemSize`'s proportional rescale on reservoir merge (`tidb-stats`
//!   reproduces the formula exactly, including its integer division) -- so a
//!   different grouping yields a different `MemSize`, which feeds only the
//!   memory tracker and never a stored statistic.
//! * **Genuinely lost: the memory tracker and its kill path.** Go charges the
//!   in-flight response bytes, the merged collector delta, and the per-task
//!   sample-item buffers against `e.memTracker`, and a session over
//!   `tidb_mem_quota_query` is killed mid-analyze. No tracker is threaded
//!   here; a caller wanting the bound uses
//!   [`tidb_stats::row_sample_collector::SampleMemoryQuota`] on the collector
//!   itself, which bounds the KEPT ROWS but not the build-stage buffers.
//! * **Genuinely lost: partial failure ordering.** Go's `errgroup` cancels its
//!   siblings on the first error and joins the producer's error with the
//!   mergers'; `getAnalyzePanicErr`/`isAnalyzeWorkerPanic` (:265) count worker
//!   panics so a panicking worker does not hang the collector loop. A
//!   sequential fold returns the FIRST error and never needs either. Observably
//!   `ANALYZE` still fails with an error; WHICH error, when two stages fail at
//!   once, may differ.
//! * **Not lost: `subBuildWorker`'s output.** Each task writes a disjoint
//!   `slicePos`, reads the root collector immutably, and takes no other task's
//!   output as input. Running them in order writes the same vector.
//!
//! # Narrowings (each named, none invented)
//!
//! * `analyzeColumnsPushDown` (:53) itself is not ported as a whole: its body
//!   is a distsql request plus [`analyze_full_scan_range`] and
//!   [`split_sampling_results`], and the request half has no local peer.
//! * `handleNDVForSpecialIndexes` (:420), `subIndexWorkerForNDV` (:494) and
//!   `buildSubIndexJobForSpecialIndex` (:532) push a v1 index-NDV job down to
//!   TiKV for exactly the indexes [`special_index_offsets`] identifies. The
//!   SELECTION is ported; the pushdown is not. [`build_sampling_stats`] takes
//!   the answer as [`SamplingOptions::pushed_down_index_ndv`], which is what
//!   :335-341 splices into the root collector.
//! * `decodeSampleDataWithVirtualColumn` (:129) evaluates generated-column
//!   expressions over a chunk. Sample columns arrive here ALREADY decoded, so
//!   the caller owns that step; what this file keeps of it is the consequence
//!   at :755 -- a virtual generated column gets no histogram at all.
//! * `printAnalyzeMergeCollectorLog` (:172) is debug logging only.
//! * `drainPendingSamplingMergeTasks` (:706) exists to release tracker bytes
//!   for batches nobody will merge. With no channel and no tracker it has no
//!   counterpart.
//! * Go carries live `*statistics.FMSketch` objects into the `AnalyzeResult`;
//!   [`RowSampleCollector::into_parts`] collapses each to its NDV estimate.
//!   The stored histogram's `ndv` is identical either way; what a caller
//!   cannot do here is merge the sketches AGAIN afterwards (which global-stats
//!   merging does, through `tidb-stats`' own path).

use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode, UNSPECIFIED_LENGTH};
use tidb_model::{ColumnInfo, IndexInfo, SchemaState, TableInfo};
use tidb_stats::builder::{
    try_build_hist_and_topn, BuildOptions, HistogramAndTopN, SampleCollector, SampleItem,
};
use tidb_stats::row_sample_collector::{RowSampleCollector, SampledRow, SlotStats};
use tidb_stats::sample_bytes::MAX_SAMPLE_VALUE_LENGTH;
use tidb_stats::{Histogram, TopN};

use crate::index_prefix_cut::cut_datum_by_prefix_len;

/// What went wrong while driving the sampling build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SamplingError {
    /// `codec.EncodeKey` rejected a sample value. Go routes this through
    /// `errCtx.HandleError` (:830) so a truncation-class error can be demoted
    /// to a warning by the statement context; with no statement context here
    /// every codec error is fatal.
    Encode(String),
    /// `statistics.BuildHistAndTopN` failed for the named slot.
    Build {
        /// The `slicePos` of the failing task.
        slice_pos: usize,
        /// The underlying build error, rendered.
        message: String,
    },
}

impl std::fmt::Display for SamplingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encode(message) => write!(formatter, "encode sample value: {message}"),
            Self::Build { slice_pos, message } => {
                write!(formatter, "build stats for slot {slice_pos}: {message}")
            }
        }
    }
}

impl std::error::Error for SamplingError {}

/// The full-table range `analyzeColumnsPushDown` (:56-65) scans.
///
/// Go returns `[]*ranger.Range`; the DECISION is the whole of what :56-65
/// does, and the three outcomes are what this enum names. The ranges
/// themselves are built by `ranger.FullIntRange` / `ranger.FullNotNullRange`,
/// which are not part of this file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyzeScanRange {
    /// `ranger.FullIntRange(unsigned)`: the table has an integer handle.
    FullIntRange {
        /// Go `mysql.HasUnsignedFlag(hc.GetCol(0).RetType.GetFlag())`.
        unsigned: bool,
    },
    /// `ranger.FullNotNullRange()`: a clustered common handle.
    FullNotNullRange,
}

/// Which handle the analyzed table has, as :56-65 interrogates it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyzeHandleKind {
    /// `handleCols.IsInt()`.
    Int {
        /// The handle column's `UNSIGNED` flag.
        unsigned: bool,
    },
    /// A non-integer (clustered, possibly composite) handle.
    Common,
}

/// Go `analyzeColumnsPushDown` :56-65.
///
/// The `None` arm is Go's `else` branch: with NO handle columns at all the
/// scan still uses `FullIntRange(false)`, not the not-null range -- the table
/// is then read by its hidden `_tidb_rowid`, which is a signed integer handle.
#[must_use]
pub fn analyze_full_scan_range(handle: Option<AnalyzeHandleKind>) -> AnalyzeScanRange {
    match handle {
        Some(AnalyzeHandleKind::Int { unsigned }) => AnalyzeScanRange::FullIntRange { unsigned },
        Some(AnalyzeHandleKind::Common) => AnalyzeScanRange::FullNotNullRange,
        None => AnalyzeScanRange::FullIntRange { unsigned: false },
    }
}

/// Go `analyzeColumnsPushDown` :70-89: the offsets, into the analyze task's
/// index list, of the indexes whose statistics CANNOT come from the shared row
/// sample.
///
/// An index qualifies when any of its key parts is a virtual generated column
/// or a prefix (`col.Length != types.UnspecifiedLength`). Go's comment states
/// the reason and it is not a limitation of the sampler: the point of v2
/// `ANALYZE` is that every column and index sees THE SAME sampled rows, and a
/// virtual/prefix key part's value is not in the row as scanned. For these the
/// executor keeps only the NDV, computed by a separate pushed-down job, and
/// derives everything else from the samples in TiDB.
///
/// `col.Offset` indexes `cols_info`, which is the analyze task's column list
/// (not `TableInfo.Columns`); an offset outside it is skipped rather than
/// panicking, because a malformed task must not abort the whole `ANALYZE`.
#[must_use]
pub fn special_index_offsets(indexes: &[IndexInfo], cols_info: &[ColumnInfo]) -> Vec<usize> {
    let mut offsets = Vec::new();
    for (position, index) in indexes.iter().enumerate() {
        let is_special = index.columns.iter_deref().any(|key_part| {
            let key_part = key_part.read();
            let is_prefix = key_part.length != UNSPECIFIED_LENGTH;
            let is_virtual = usize::try_from(key_part.offset)
                .ok()
                .and_then(|offset| cols_info.get(offset))
                .is_some_and(ColumnInfo::is_virtual_generated);
            is_prefix || is_virtual
        });
        if is_special {
            offsets.push(position);
        }
    }
    offsets
}

/// Go `pkg/executor/analyze_col.go` `isSingleColNonPrefixUniqueIndex` (:78).
///
/// A public, single-column, unconditional, non-prefix UNIQUE (or PRIMARY)
/// index proves every non-NULL value of that column occurs exactly once, which
/// is why [`topn_count_for_task`] suppresses its TopN entirely.
#[must_use]
pub fn is_single_col_non_prefix_unique_index(index: &IndexInfo) -> bool {
    index.state == SchemaState::PUBLIC
        && (index.unique || index.primary)
        && index.columns.len() == 1
        && !index.has_prefix_index()
        && !index.has_condition()
}

/// Go `pkg/executor/analyze_col.go` `isColumnCoveredBySingleColUniqueIndex`
/// (:63).
#[must_use]
pub fn is_column_covered_by_single_col_unique_index(table: &TableInfo, col_offset: i64) -> bool {
    table.indices.iter_deref().any(|index| {
        let index = index.read();
        index.state == SchemaState::PUBLIC
            && is_single_col_non_prefix_unique_index(&index)
            && index
                .columns
                .get(0)
                .is_some_and(|key| key.read().offset == col_offset)
    })
}

/// Go `subBuildWorker` :868-880: `WITH n TOPN`, forced to zero when the values
/// are known to be distinct.
///
/// Go reads `e.opts[ast.AnalyzeOptNumTopN]` and then overrides it. The
/// override is not an optimisation: a TopN over a unique column would list
/// count-1 entries, which carries no information and costs a row of
/// `mysql.stats_top_n` per sampled value.
#[must_use]
pub fn topn_count_for_task(
    requested_topn: isize,
    task: &SamplingBuildTask,
    table_info: Option<&TableInfo>,
    cols_info: &[ColumnInfo],
    indexes: &[IndexInfo],
) -> isize {
    let col_len = cols_info.len();
    if task.is_column {
        // Go guards on `e.tableInfo != nil`: an analyze task built without a
        // TableInfo keeps the requested TopN.
        let covered =
            table_info
                .zip(cols_info.get(task.slice_pos))
                .is_some_and(|(table, column)| {
                    is_column_covered_by_single_col_unique_index(table, column.offset)
                });
        if covered {
            return 0;
        }
    } else if task
        .slice_pos
        .checked_sub(col_len)
        .and_then(|offset| indexes.get(offset))
        .is_some_and(is_single_col_non_prefix_unique_index)
    {
        return 0;
    }
    requested_topn
}

/// Go `samplingBuildTask` (:911).
///
/// `rootRowCollector` is not a field here: Go must hand each channel-borne
/// task its own reference to the shared collector, whereas the sequential loop
/// already holds it.
#[derive(Debug, Clone)]
pub struct SamplingBuildTask {
    /// The column ID or index ID the built histogram is stored under.
    pub id: i64,
    /// The type the bucket bounds are interpreted as. Go uses the column's
    /// own `FieldType` for a column and a bare `TypeBlob` for an index,
    /// because an index sample is already an encoded key.
    pub field_type: FieldType,
    /// Go `isColumn`.
    pub is_column: bool,
    /// The slot in the collector, and in the output vectors: `i` for
    /// `colsInfo[i]`, `len(colsInfo) + i` for `indexes[i]`.
    pub slice_pos: usize,
}

/// The merged, decoded, handle-sorted sample set `buildSamplingStats` works
/// from once :193-322 have run -- i.e. the output of
/// [`RowSampleCollector::into_parts`].
#[derive(Debug, Clone)]
pub struct SampledData {
    /// Go `rootRowCollector.Base().Count`: rows SCANNED, not rows kept.
    pub count: i64,
    /// Per-slot whole-scan facts: null count, total size, NDV.
    pub slots: Vec<SlotStats>,
    /// The kept rows, sorted by handle and numbered.
    pub rows: Vec<SampledRow>,
}

/// The pushed-down answer for one special index, Go
/// `analyzeIndexNDVTotalResult.results[idx.ID]` as :335-341 consumes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PushedDownIndexNdv {
    /// The index's offset in the analyze task's index list.
    pub index_offset: usize,
    /// Go `ret.Count`, written into `NullCount[colLen+offset]`. The name is
    /// Go's: the v1 index job reports the analyzed row count in `Count`, and
    /// the sampling path stores it in the null-count slot because for a
    /// pushed-down index that slot is what the builder subtracts.
    pub null_count: i64,
    /// Go `ret.Ars[0].Fms[0].NDV()`, replacing the sample-derived sketch.
    pub ndv: i64,
}

/// Everything [`build_sampling_stats`] needs beyond the samples themselves.
#[derive(Debug, Clone, Default)]
pub struct SamplingOptions {
    /// Go `e.opts[ast.AnalyzeOptNumBuckets]` / `[ast.AnalyzeOptNumTopN]`.
    pub build: BuildOptions,
    /// Go :330-341's `<-idxNDVPushDownCh`, already resolved.
    pub pushed_down_index_ndv: Vec<PushedDownIndexNdv>,
}

/// What `buildSamplingStats` (:193) returns, minus the live FM sketches.
#[derive(Debug, Clone)]
pub struct SamplingStats {
    /// Go's returned `count`, the scanned row count.
    pub count: i64,
    /// One entry per slot. `None` is Go's explicit `hists[slicePos] = nil`
    /// for a virtual generated column (:756).
    pub histograms: Vec<Option<Histogram>>,
    /// One entry per slot, aligned with `histograms`.
    pub topns: Vec<Option<TopN>>,
    /// Per-slot NDV, standing in for Go's `fmSketches`.
    pub ndvs: Vec<i64>,
}

/// Go `analyzeColumnsPushDown` :96-118: how many leading slots are COLUMN
/// results.
///
/// Go starts from `len(e.analyzePB.ColReq.ColumnsInfo)` and then drops one
/// more when the last column histogram belongs to `_tidb_rowid` (`ID == -1`).
/// The comment at :107 is the whole justification, and it is a load-bearing
/// ordering contract rather than a heuristic: `buildAnalyzeFullSamplingTask`
/// always appends `_tidb_rowid` LAST, and the result order matches `colsInfo`,
/// so if a `_tidb_rowid` histogram exists it is at the end. The `!= nil` guard
/// is Go's: a virtual column's slot is nil and must not be dereferenced.
#[must_use]
pub fn column_result_len(column_count: usize, histograms: &[Option<Histogram>]) -> usize {
    if column_count == 0 {
        return 0;
    }
    let is_row_id = histograms
        .get(column_count - 1)
        .and_then(Option::as_ref)
        .is_some_and(|histogram| histogram.id == -1);
    if is_row_id {
        column_count - 1
    } else {
        column_count
    }
}

/// The two `statistics.AnalyzeResult`s :96-126 splits one slot vector into.
#[derive(Debug, Clone)]
pub struct SplitSamplingResults {
    /// Go `colResult`: `hists[:cLen]`, `IsIndex` unset.
    pub column_slots: std::ops::Range<usize>,
    /// Go `colGroupResult`: `hists[cLen:]`, `IsIndex: 1`. Note Go slices the
    /// GROUP result at the UN-decremented `cLen`, so a discarded
    /// `_tidb_rowid` slot lands in NEITHER result.
    pub index_slots: std::ops::Range<usize>,
}

/// Go `analyzeColumnsPushDown` :96-126.
#[must_use]
pub fn split_sampling_results(
    column_count: usize,
    total_len: usize,
    histograms: &[Option<Histogram>],
) -> SplitSamplingResults {
    let column_len = column_result_len(column_count, histograms);
    SplitSamplingResults {
        column_slots: 0..column_len,
        index_slots: column_count..total_len,
    }
}

/// Go `buildSamplingStats` :245-283 plus `subMergeWorker`'s merge (:672-681),
/// folded flat.
///
/// See this module's header for why flattening the two-level fan-in preserves
/// the merge result exactly, and what it does change (`MemSize`). Go returns
/// the progress counts to `UpdateAnalyzeJobProgress` (:670) batch by batch;
/// the total is returned here so a caller that reports progress can, while a
/// caller that does not is not forced to thread a job handle through.
pub fn merge_sampled_collectors(
    root: &mut RowSampleCollector,
    sub_collectors: impl IntoIterator<Item = RowSampleCollector>,
) -> i64 {
    let mut merged_rows = 0i64;
    for sub_collector in sub_collectors {
        merged_rows = merged_rows.wrapping_add(sub_collector.count());
        // Go `MergeCollector` (:678) then `DestroyAndPutToPool` (:283): the
        // sub-collector is dead after the merge, so it moves in rather than
        // being freed separately -- the FM-sketch slice Go's `Destroy` clears
        // is dropped with it.
        root.merge(sub_collector);
    }
    merged_rows
}

/// Go `subBuildWorker` :755-800: one column's `statistics.SampleCollector`.
///
/// Three filters, all Go's and all observable:
///
/// * NULL values are SKIPPED but were already counted in `NullCount` during
///   the scan, which is why `Count` below is the scanned count MINUS the null
///   count rather than the number of samples.
/// * A value longer than `MaxSampleValueLength` is dropped, on the stated
///   reasoning that a value that big cannot repeat often enough to matter.
///   The row is still counted; only the sample is dropped, so the histogram
///   under-covers rather than mis-scales.
/// * A new-collation string is replaced by its COLLATION KEY, not merely
///   compared by it. Go's comment ties this to
///   `(*statistics.Column).GetColumnRowCount`: the stored bucket bounds must be
///   in the same order the estimator later compares in. ENUM and SET are
///   excluded because their datum payload is the numeric member, not text.
///
/// `ordinal` is the sample's position in the HANDLE-sorted vector (Go's `j`),
/// which is what makes the physical/logical correlation meaningful.
pub fn column_sample_collector(
    data: &SampledData,
    slice_pos: usize,
    field_type: &FieldType,
) -> Result<SampleCollector, SamplingError> {
    let collator = if field_type.eval_type() == EvalType::String
        && field_type.code() != FieldTypeCode::Enum
        && field_type.code() != FieldTypeCode::Set
    {
        Some(field_type.runtime_collator())
    } else {
        None
    };

    let mut samples = Vec::with_capacity(data.rows.len());
    for (position, row) in data.rows.iter().enumerate() {
        let Some(value) = row.columns.get(slice_pos) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        if value
            .as_raw_bytes()
            .is_some_and(|bytes| bytes.len() > MAX_SAMPLE_VALUE_LENGTH)
        {
            continue;
        }
        let mut value = value.clone();
        if let Some(collator) = collator {
            if let Some(bytes) = value.as_raw_bytes() {
                let key = collator.key(bytes);
                value.set_bytes(key);
            }
        }
        let encoded = tidb_codec::encode_key(std::slice::from_ref(&value))
            .map_err(|error| SamplingError::Encode(error.to_string()))?;
        samples.push(SampleItem {
            encoded,
            value,
            ordinal: position as isize,
        });
    }

    let slot = data.slots.get(slice_pos).cloned().unwrap_or_default();
    Ok(SampleCollector {
        samples,
        null_count: slot.null_count,
        count: data.count - slot.null_count,
        ndv: slot.ndv,
        total_size: slot.total_size,
    })
}

/// Go `subBuildWorker` :801-860: one index's `statistics.SampleCollector`.
///
/// The sample VALUE is the encoded index key built from this row's key-part
/// columns, so the histogram compares exactly what an index range scan seeks.
/// Go's three rules, all reproduced:
///
/// * A SINGLE-column index skips rows whose only key part is NULL (:806).
///   A composite index does NOT -- a partially-NULL composite entry still
///   exists in the index and must still be estimable.
/// * `continue indexSampleCollectLoop` (:812): if ANY key part exceeds
///   `MaxSampleValueLength` the WHOLE row is dropped, not just that part --
///   dropping one part would produce a key that indexes nothing.
/// * A prefix key part is CUT to its declared length before encoding
///   (:815-817), via the same [`cut_datum_by_prefix_len`] the range builder
///   uses, so sample keys and seek keys are cut identically.
pub fn index_sample_collector(
    data: &SampledData,
    slice_pos: usize,
    index: &IndexInfo,
    cols_info: &[ColumnInfo],
) -> Result<SampleCollector, SamplingError> {
    let single_column = index.columns.len() == 1;
    let mut samples = Vec::with_capacity(data.rows.len());
    'rows: for row in &data.rows {
        if single_column {
            let is_null = index
                .columns
                .get(0)
                .and_then(|key| usize::try_from(key.read().offset).ok())
                .and_then(|offset| row.columns.get(offset))
                .is_some_and(Datum::is_null);
            if is_null {
                continue;
            }
        }
        let mut key_parts = Vec::with_capacity(index.columns.len());
        for key in index.columns.iter_deref() {
            let key = key.read();
            let Some(value) = usize::try_from(key.offset)
                .ok()
                .and_then(|offset| row.columns.get(offset))
            else {
                continue 'rows;
            };
            if value
                .as_raw_bytes()
                .is_some_and(|bytes| bytes.len() > MAX_SAMPLE_VALUE_LENGTH)
            {
                continue 'rows;
            }
            let mut value = value.clone();
            if key.length != UNSPECIFIED_LENGTH {
                if let Some(column) = usize::try_from(key.offset)
                    .ok()
                    .and_then(|offset| cols_info.get(offset))
                {
                    cut_datum_by_prefix_len(&mut value, key.length, &column.field_type);
                }
            }
            key_parts.push(value);
        }
        // Go encodes key part by key part onto one buffer; one call over the
        // whole tuple produces the same bytes because `EncodeKey` is a plain
        // concatenation of per-datum encodings.
        let encoded = tidb_codec::encode_key(&key_parts)
            .map_err(|error| SamplingError::Encode(error.to_string()))?;
        samples.push(SampleItem {
            value: Datum::new_bytes(encoded.clone()),
            encoded,
            ordinal: 0,
        });
    }

    let slot = data.slots.get(slice_pos).cloned().unwrap_or_default();
    Ok(SampleCollector {
        samples,
        null_count: slot.null_count,
        count: data.count - slot.null_count,
        ndv: slot.ndv,
        total_size: slot.total_size,
    })
}

/// Go `buildSamplingStats` :324-400 with `subBuildWorker` (:712) inlined as a
/// sequential loop: build every column's and every index's histogram and TopN.
///
/// The task ORDER is Go's, and it matters for the output layout rather than
/// for any single result: columns first in `colsInfo` order at slots
/// `0..colLen`, then indexes at `colLen + i`. [`split_sampling_results`]
/// relies on exactly that.
///
/// `pushed_down_index_ndv` is spliced in BEFORE the index tasks run (:335-341),
/// because the special indexes' NDV and null count come from TiKV rather than
/// from the samples.
pub fn build_sampling_stats(
    data: &mut SampledData,
    cols_info: &[ColumnInfo],
    indexes: &[IndexInfo],
    table_info: Option<&TableInfo>,
    options: &SamplingOptions,
) -> Result<SamplingStats, SamplingError> {
    let col_len = cols_info.len();
    let total_len = col_len + indexes.len();
    let mut histograms: Vec<Option<Histogram>> = vec![None; total_len];
    let mut topns: Vec<Option<TopN>> = vec![None; total_len];

    // Go :335-341. Applied before the index tasks read the slots.
    for pushed_down in &options.pushed_down_index_ndv {
        let slot = col_len + pushed_down.index_offset;
        if let Some(entry) = data.slots.get_mut(slot) {
            entry.null_count = pushed_down.null_count;
            entry.ndv = pushed_down.ndv;
        }
    }

    let mut tasks = Vec::with_capacity(total_len);
    for (position, column) in cols_info.iter().enumerate() {
        tasks.push(SamplingBuildTask {
            id: column.id,
            field_type: column.field_type.clone(),
            is_column: true,
            slice_pos: position,
        });
    }
    for (position, index) in indexes.iter().enumerate() {
        tasks.push(SamplingBuildTask {
            id: index.id,
            // Go `types.NewFieldType(mysql.TypeBlob)`: an index sample is an
            // encoded key, so its bucket bounds are opaque bytes.
            field_type: FieldType::new(FieldTypeCode::Blob),
            is_column: false,
            slice_pos: col_len + position,
        });
    }

    for task in &tasks {
        // Go :756-758: a virtual generated column has no stored value, so it
        // gets no histogram and no TopN at all -- explicitly nil, not empty.
        if task.is_column
            && cols_info
                .get(task.slice_pos)
                .is_some_and(ColumnInfo::is_virtual_generated)
        {
            continue;
        }
        let collector = if task.is_column {
            column_sample_collector(data, task.slice_pos, &task.field_type)?
        } else {
            index_sample_collector(
                data,
                task.slice_pos,
                &indexes[task.slice_pos - col_len],
                cols_info,
            )?
        };
        let mut build = options.build;
        build.num_topn =
            topn_count_for_task(options.build.num_topn, task, table_info, cols_info, indexes);
        let HistogramAndTopN { histogram, topn } =
            try_build_hist_and_topn(task.id, &collector, build, task.is_column, |sample, _| {
                Ok::<_, std::convert::Infallible>(sample.encoded.clone())
            })
            .map_err(|error| SamplingError::Build {
                slice_pos: task.slice_pos,
                message: format!("{error:?}"),
            })?;
        histograms[task.slice_pos] = Some(histogram);
        topns[task.slice_pos] = topn;
    }

    Ok(SamplingStats {
        count: data.count,
        histograms,
        topns,
        ndvs: data.slots.iter().map(|slot| slot.ndv).collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::CiString;
    use tidb_model::IndexColumn;
    use tidb_txnkv::{Handle, IntHandle};

    fn column(id: i64, offset: i64, code: FieldTypeCode) -> ColumnInfo {
        ColumnInfo {
            id,
            offset,
            name: CiString::new("c"),
            field_type: FieldType::new(code),
            ..ColumnInfo::default()
        }
    }

    fn index(id: i64, offsets: &[(i64, i64)], unique: bool) -> IndexInfo {
        IndexInfo {
            id,
            name: CiString::new("i"),
            state: SchemaState::PUBLIC,
            unique,
            columns: offsets
                .iter()
                .map(|&(offset, length)| IndexColumn {
                    name: CiString::new("c"),
                    offset,
                    length,
                    ..IndexColumn::default()
                })
                .collect::<Vec<_>>()
                .into(),
            ..IndexInfo::default()
        }
    }

    fn sampled(rows: Vec<Vec<Datum>>, slots: Vec<SlotStats>, count: i64) -> SampledData {
        SampledData {
            count,
            slots,
            rows: rows
                .into_iter()
                .enumerate()
                .map(|(position, columns)| SampledRow {
                    columns,
                    handle: Handle::Int(IntHandle::new(position as i64)),
                    ordinal: position as isize,
                })
                .collect(),
        }
    }

    // WRITTEN test: Go's coverage of :56-65 runs through testkit `ANALYZE`.
    #[test]
    fn full_scan_range_follows_the_handle_kind() {
        assert_eq!(
            analyze_full_scan_range(Some(AnalyzeHandleKind::Int { unsigned: true })),
            AnalyzeScanRange::FullIntRange { unsigned: true }
        );
        assert_eq!(
            analyze_full_scan_range(Some(AnalyzeHandleKind::Common)),
            AnalyzeScanRange::FullNotNullRange
        );
        // No handle columns still means an integer scan, over _tidb_rowid.
        assert_eq!(
            analyze_full_scan_range(None),
            AnalyzeScanRange::FullIntRange { unsigned: false }
        );
    }

    // WRITTEN test for :70-89.
    #[test]
    fn special_indexes_are_the_virtual_and_prefix_ones() {
        let virtual_column = ColumnInfo {
            generated_expr_string: "a+1".into(),
            generated_stored: false,
            ..column(3, 2, FieldTypeCode::Varchar)
        };
        let cols = vec![
            column(1, 0, FieldTypeCode::Long),
            column(2, 1, FieldTypeCode::Varchar),
            virtual_column,
        ];
        let indexes = vec![
            index(1, &[(0, UNSPECIFIED_LENGTH)], false),
            index(2, &[(1, 4)], false),
            index(
                3,
                &[(0, UNSPECIFIED_LENGTH), (2, UNSPECIFIED_LENGTH)],
                false,
            ),
        ];
        assert_eq!(special_index_offsets(&indexes, &cols), vec![1, 2]);
    }

    // WRITTEN test for analyze_col.go :63/:78.
    #[test]
    fn unique_single_column_index_suppresses_topn() {
        let cols = vec![column(1, 0, FieldTypeCode::Long)];
        let unique = index(1, &[(0, UNSPECIFIED_LENGTH)], true);
        let prefix_unique = index(2, &[(0, 4)], true);
        assert!(is_single_col_non_prefix_unique_index(&unique));
        assert!(!is_single_col_non_prefix_unique_index(&prefix_unique));

        let table = TableInfo {
            indices: vec![unique.clone()].into(),
            ..TableInfo::default()
        };
        assert!(is_column_covered_by_single_col_unique_index(&table, 0));
        assert!(!is_column_covered_by_single_col_unique_index(&table, 1));

        let column_task = SamplingBuildTask {
            id: 1,
            field_type: FieldType::new(FieldTypeCode::Long),
            is_column: true,
            slice_pos: 0,
        };
        assert_eq!(
            topn_count_for_task(
                20,
                &column_task,
                Some(&table),
                &cols,
                std::slice::from_ref(&unique)
            ),
            0
        );
        // With no TableInfo the requested TopN survives, exactly as Go's
        // `e.tableInfo != nil` guard leaves it.
        assert_eq!(
            topn_count_for_task(20, &column_task, None, &cols, std::slice::from_ref(&unique)),
            20
        );
        let index_task = SamplingBuildTask {
            id: 1,
            field_type: FieldType::new(FieldTypeCode::Blob),
            is_column: false,
            slice_pos: 1,
        };
        assert_eq!(
            topn_count_for_task(20, &index_task, Some(&table), &cols, &[unique]),
            0
        );
        assert_eq!(
            topn_count_for_task(20, &index_task, Some(&table), &cols, &[prefix_unique]),
            20
        );
    }

    // WRITTEN test for :107-118: the _tidb_rowid discard rule.
    #[test]
    fn row_id_histogram_is_dropped_from_the_column_result() {
        let with_row_id = vec![
            Some(Histogram {
                id: 1,
                ..Histogram::default()
            }),
            Some(Histogram {
                id: -1,
                ..Histogram::default()
            }),
            Some(Histogram {
                id: 7,
                ..Histogram::default()
            }),
        ];
        let split = split_sampling_results(2, 3, &with_row_id);
        assert_eq!(split.column_slots, 0..1);
        // The dropped _tidb_rowid slot lands in neither result: the group
        // result still starts at the un-decremented column count.
        assert_eq!(split.index_slots, 2..3);

        let without_row_id = vec![
            Some(Histogram {
                id: 1,
                ..Histogram::default()
            }),
            Some(Histogram {
                id: 2,
                ..Histogram::default()
            }),
            Some(Histogram {
                id: 7,
                ..Histogram::default()
            }),
        ];
        let split = split_sampling_results(2, 3, &without_row_id);
        assert_eq!(split.column_slots, 0..2);
        assert_eq!(split.index_slots, 2..3);

        // A virtual column's nil slot must not be dereferenced.
        let virtual_last = vec![
            Some(Histogram {
                id: 1,
                ..Histogram::default()
            }),
            None,
        ];
        assert_eq!(column_result_len(2, &virtual_last), 2);
    }

    // WRITTEN test for :761-799: NULL, oversized and collation handling.
    #[test]
    fn column_collector_skips_nulls_and_oversized_values() {
        let rows = vec![
            vec![Datum::new_bytes(b"a".to_vec())],
            vec![Datum::Null],
            vec![Datum::new_bytes(vec![b'x'; MAX_SAMPLE_VALUE_LENGTH + 1])],
            vec![Datum::new_bytes(b"b".to_vec())],
        ];
        let data = sampled(
            rows,
            vec![SlotStats {
                null_count: 1,
                total_size: 12,
                ndv: 3,
            }],
            10,
        );
        let collector =
            column_sample_collector(&data, 0, &FieldType::new(FieldTypeCode::Blob)).unwrap();
        assert_eq!(collector.samples.len(), 2);
        // Ordinals are positions in the handle-sorted vector, so the skipped
        // rows leave gaps rather than renumbering.
        assert_eq!(collector.samples[0].ordinal, 0);
        assert_eq!(collector.samples[1].ordinal, 3);
        // Count is scanned-minus-null, NOT the sample count.
        assert_eq!(collector.count, 9);
        assert_eq!(collector.null_count, 1);
        assert_eq!(collector.ndv, 3);
        assert_eq!(collector.total_size, 12);
    }

    // WRITTEN test for :806-812: the single-column NULL skip and the
    // whole-row drop on an oversized key part.
    #[test]
    fn index_collector_drops_whole_row_on_one_oversized_part() {
        let cols = vec![
            column(1, 0, FieldTypeCode::Varchar),
            column(2, 1, FieldTypeCode::Varchar),
        ];
        let slots = vec![
            SlotStats::default(),
            SlotStats::default(),
            SlotStats::default(),
        ];

        let single = index(1, &[(0, UNSPECIFIED_LENGTH)], false);
        let data = sampled(
            vec![
                vec![
                    Datum::new_bytes(b"a".to_vec()),
                    Datum::new_bytes(b"z".to_vec()),
                ],
                vec![Datum::Null, Datum::new_bytes(b"z".to_vec())],
            ],
            slots.clone(),
            2,
        );
        let collector = index_sample_collector(&data, 2, &single, &cols).unwrap();
        assert_eq!(collector.samples.len(), 1);

        // A composite index keeps the partially-NULL row: it exists in the
        // index and must remain estimable.
        let composite = index(
            2,
            &[(0, UNSPECIFIED_LENGTH), (1, UNSPECIFIED_LENGTH)],
            false,
        );
        let collector = index_sample_collector(&data, 2, &composite, &cols).unwrap();
        assert_eq!(collector.samples.len(), 2);

        // One oversized key part drops the whole row, not just that part.
        let oversized = sampled(
            vec![
                vec![
                    Datum::new_bytes(b"a".to_vec()),
                    Datum::new_bytes(vec![b'x'; MAX_SAMPLE_VALUE_LENGTH + 1]),
                ],
                vec![
                    Datum::new_bytes(b"a".to_vec()),
                    Datum::new_bytes(b"z".to_vec()),
                ],
            ],
            slots,
            2,
        );
        let collector = index_sample_collector(&oversized, 2, &composite, &cols).unwrap();
        assert_eq!(collector.samples.len(), 1);
    }

    // WRITTEN test for :815-817: a prefix key part is cut before encoding, so
    // two rows differing only past the prefix produce ONE distinct sample key.
    #[test]
    fn prefix_key_parts_are_cut_before_encoding() {
        let cols = vec![column(1, 0, FieldTypeCode::VarString)];
        let prefix = index(1, &[(0, 2)], false);
        let data = sampled(
            vec![
                vec![Datum::new_bytes(b"abcdef".to_vec())],
                vec![Datum::new_bytes(b"abzzzz".to_vec())],
            ],
            vec![SlotStats::default(), SlotStats::default()],
            2,
        );
        let collector = index_sample_collector(&data, 1, &prefix, &cols).unwrap();
        assert_eq!(collector.samples.len(), 2);
        assert_eq!(collector.samples[0].encoded, collector.samples[1].encoded);
    }

    // WRITTEN test for :335-341 and :756: the pushed-down NDV splice and the
    // virtual column's absent histogram.
    #[test]
    fn build_lays_out_columns_then_indexes() {
        let virtual_column = ColumnInfo {
            generated_expr_string: "a+1".into(),
            ..column(2, 1, FieldTypeCode::Varchar)
        };
        let cols = vec![column(1, 0, FieldTypeCode::Long), virtual_column];
        let indexes = vec![index(10, &[(0, UNSPECIFIED_LENGTH)], false)];
        let mut data = sampled(
            vec![
                vec![Datum::Int(1), Datum::Null],
                vec![Datum::Int(2), Datum::Null],
            ],
            vec![
                SlotStats {
                    null_count: 0,
                    total_size: 2,
                    ndv: 2,
                },
                SlotStats::default(),
                SlotStats::default(),
            ],
            2,
        );
        let options = SamplingOptions {
            build: BuildOptions::default(),
            pushed_down_index_ndv: vec![PushedDownIndexNdv {
                index_offset: 0,
                null_count: 5,
                ndv: 42,
            }],
        };
        let stats = build_sampling_stats(&mut data, &cols, &indexes, None, &options).unwrap();
        assert_eq!(stats.count, 2);
        assert_eq!(stats.histograms.len(), 3);
        assert!(stats.histograms[0].is_some());
        // The virtual generated column gets no histogram at all.
        assert!(stats.histograms[1].is_none());
        assert!(stats.topns[1].is_none());
        assert!(stats.histograms[2].is_some());
        // The pushed-down NDV replaced the sample-derived one.
        assert_eq!(stats.ndvs[2], 42);
        assert_eq!(stats.histograms[2].as_ref().unwrap().null_count, 5);
    }
}
