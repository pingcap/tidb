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

//! SEED of Go `pkg/util/execdetails`, covering `runtime_stats.go` and
//! `util.go`: the `Tp*` runtime-stats kind constants, the [`RuntimeStats`]
//! interface, `basicCopRuntimeStats`/[`CopRuntimeStats`]/
//! [`StmtCopRuntimeStats`]/[`BasicRuntimeStats`]/[`RootRuntimeStats`]/
//! [`RuntimeStatsColl`]/
//! [`ConcurrencyInfo`]/[`RuntimeStatsWithConcurrencyInfo`]/
//! [`RuntimeStatsWithCommit`]/[`RuRuntimeStats`] with their byte-exact
//! `String()` renderings, `getPlanIDFromExecutionSummary`, and `util.go`'s
//! `canGetFloat64`/[`Int64`]/[`Duration`]/[`DurationWithAddr`]/
//! [`Percentile`]/[`format_duration`] (`FormatDuration`) surface.
//!
//! Narrowings (Go input → Rust snapshot shape):
//! - `*tipb.ExecutorExecutionSummary` → [`ExecutorExecutionSummarySnapshot`]:
//!   only the fields the ported code reads (`TimeProcessedNs`,
//!   `NumProducedRows`, `NumIterations`, `GetConcurrency()`,
//!   `GetExecutorId()`), with proto optionals collapsed to Go's
//!   zero-defaulted getter results. The `DetailInfo` oneof narrows to
//!   [`ExecutionSummaryDetailInfo`] (the `TiflashScanContext`/
//!   `ColumnarScanContext` variants the ported arms read), and
//!   `TiflashWaitSummary`/`TiflashNetworkSummary` ride along as the
//!   [`crate::tiflash_stats`] snapshot narrowings, `Option`-wrapped like
//!   Go's nil-checked getters.
//! - client-go `util.ScanDetail` → [`CopScanDetail`]: composition over the
//!   already-ported [`crate::exec_details::ScanDetail`] plus the
//!   `ProcessedKeysSize` field only this module reads. Its `String()` is
//!   recovered from the Go test literals (per-field zero suppression;
//!   `get_snapshot_time` and the IA fields are never rendered by the
//!   fixtures and stay unrendered), and its `Merge` is the additive merge
//!   the `TestCopRuntimeStats2` sums pin.
//! - client-go `util.TimeDetail` → [`CopTimeDetail`]: composition over
//!   [`crate::exec_details::TimeDetail`] plus `SuspendTime` and
//!   `KvReadWallTime`. `String()` is recovered from `TestCopRuntimeStats2`
//!   (`total_process_time`, `total_suspend_time`, `total_wait_time`,
//!   `total_kv_read_wall_time`, `tikv_wall_time`, zero-suppressed);
//!   `KvGrpcWaitTime`/`KvGrpcProcessTime` are not representable in the
//!   reused type and stay open. `Merge` is additive.
//! - client-go `*util.CommitDetails`/`*util.LockKeysDetails` reuse
//!   [`crate::exec_details::CommitDetails`]/[`crate::exec_details::LockKeysDetails`];
//!   their client-go `Merge`/`Clone` (source not on disk) are recovered as
//!   [`merge_commit_details`]/[`merge_lock_keys_details`]: additive at every
//!   test-pinned field, slowest-request slots by larger `ReqTotalTime`;
//!   unpinned arms (for example client-go's exact `RetryCount` handling)
//!   stay open under the additive reading.
//! - client-go `*util.RUDetails` → the reused
//!   [`crate::slow_log_format::RuDetailsSnapshot`]; `RUDetails.Merge` is
//!   recovered as the additive [`merge_ru_details`].
//! - `*execdetails.RUV2Metrics`/`RUV2Weights` (`ruv2_metrics.go`, outside
//!   this claim) reuse [`crate::slow_log_format::RuV2MetricsSnapshot`]/
//!   [`crate::slow_log_format::RuV2Weights`]; the `TotalRU`/
//!   `CalculateRUValues`/`Merge` slices [`RuRuntimeStats`] needs are seeded
//!   here as [`ruv2_total_ru`]/[`merge_ruv2_metrics_snapshot`] (the weighted
//!   sum is restated because `slow_log_format`'s copy is private), with the
//!   prometheus side effects dropped.
//! - `rmclient.RUVersion` (pd client, source not on disk) → plain [`i64`]
//!   ([`RU_VERSION_V1`] `= 1`, [`RU_VERSION_V2`] `= 2`), pinned by the Go
//!   doc comment on `RURuntimeStats` ("1 (v1) … 2 (v2) … 0 / unknown
//!   defaults to v1").
//! - `kv.StoreType` → [`StoreType`] with the `Name()` spellings from
//!   `pkg/kv/kv.go`.
//!
//! The TiFlash arms — `basicCopRuntimeStats.tiflashStats` with the TiFlash
//! sub-blocks of `basicCopRuntimeStats.String`/`Clone`/`Merge`/
//! `mergeExecSummary` and `CopRuntimeStats.String`'s
//! `printTiFlashSpecificInfo`, plus [`StmtCopRuntimeStats`] and
//! `RuntimeStatsColl.stmtCopStats`/`GetStmtCopRuntimeStats` — are wired
//! against the [`crate::tiflash_stats`] types. Their Go tests
//! (`TestCopRuntimeStatsForTiFlash`, `TestVectorSearchStats`,
//! `TestColumnarScanContextStats`) live in [`crate::tiflash_stats`]'s test
//! module and drive these production types.
//!
//! Boundaries:
//! - `util.go`'s `context.Context` plumbing has no Rust context here:
//!   `ContextWithInitializedExecDetails`,
//!   `ContextWithMissingExecDetailsInitialized`,
//!   `ContextWithInheritedRUV2Details`, `ContextWithRUV2Metrics`,
//!   `SyncRUV2MetricsFromContext`, and `GetExecDetailsFromContext` stay
//!   open. `LoadTiKVExecDetails` collapses into the already-ported
//!   post-load [`crate::slow_log_format::TikvExecDetailsSnapshot`]
//!   narrowing.
//! - `github.com/influxdata/tdigest` is replaced by a simplified merging
//!   centroid digest: exact for the degenerate distributions the tests pin
//!   (all-equal values past `MaxDetailsNumsForOneQuery`), approximate
//!   quantiles elsewhere.
//! - Go `sync.Mutex`/`sync/atomic` guards: `RuntimeStatsColl.mu` and
//!   `RuntimeStatsWithConcurrencyInfo`'s embedded mutex flatten to plain
//!   single-threaded fields; [`BasicRuntimeStats`] keeps real
//!   `std::sync::atomic` fields (SeqCst) because Go shares one instance
//!   across executors with the same id and the tests exercise that sharing
//!   (here through `Rc`).
//! - `strconv.FormatFloat(x, 'f', 2, 64)` renders through Rust `{:.2}`;
//!   they agree at every fixture point, exotic ties stay open. client-go
//!   `util.FormatBytes` (source not on disk) is restated from TiDB's own
//!   `memory.FormatBytes` and pinned at the `Bytes` and `98.2 KB` fixture
//!   points; client-go's duration rendering coincides with
//!   [`format_duration`] at every pinned point.

use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::time::Duration as StdDuration;

use crate::exec_details::{
    format_go_duration, format_seconds_3, CommitDetails, LockKeysDetails, ScanDetail, TimeDetail,
};
use crate::slow_log_format::{RuDetailsSnapshot, RuV2MetricsSnapshot, RuV2Weights};
use crate::tiflash_stats::{
    ColumnarScanContextSnapshot, TiFlashNetworkSummarySnapshot, TiFlashNetworkTrafficSummary,
    TiFlashScanContextSnapshot, TiFlashWaitSummarySnapshot, TiflashStats,
};

/// Go `TpBasicRuntimeStats`: the tp for `BasicRuntimeStats`.
pub const TP_BASIC_RUNTIME_STATS: i32 = 0;
/// Go `TpRuntimeStatsWithCommit`: the tp for `RuntimeStatsWithCommit`.
pub const TP_RUNTIME_STATS_WITH_COMMIT: i32 = 1;
/// Go `TpRuntimeStatsWithConcurrencyInfo`: the tp for
/// `RuntimeStatsWithConcurrencyInfo`.
pub const TP_RUNTIME_STATS_WITH_CONCURRENCY_INFO: i32 = 2;
/// Go `TpSnapshotRuntimeStats`: the tp for `SnapshotRuntimeStats`.
pub const TP_SNAPSHOT_RUNTIME_STATS: i32 = 3;
/// Go `TpHashJoinRuntimeStats`: the tp for `HashJoinRuntimeStats`.
pub const TP_HASH_JOIN_RUNTIME_STATS: i32 = 4;
/// Go `TpHashJoinRuntimeStatsV2`: the tp for `hashJoinRuntimeStatsV2`.
pub const TP_HASH_JOIN_RUNTIME_STATS_V2: i32 = 5;
/// Go `TpIndexLookUpJoinRuntimeStats`: the tp for
/// `IndexLookUpJoinRuntimeStats`.
pub const TP_INDEX_LOOK_UP_JOIN_RUNTIME_STATS: i32 = 6;
/// Go `TpRuntimeStatsWithSnapshot`: the tp for `RuntimeStatsWithSnapshot`.
pub const TP_RUNTIME_STATS_WITH_SNAPSHOT: i32 = 7;
/// Go `TpJoinRuntimeStats`: the tp for `JoinRuntimeStats`.
pub const TP_JOIN_RUNTIME_STATS: i32 = 8;
/// Go `TpSelectResultRuntimeStats`: the tp for `SelectResultRuntimeStats`.
pub const TP_SELECT_RESULT_RUNTIME_STATS: i32 = 9;
/// Go `TpInsertRuntimeStat`: the tp for `InsertRuntimeStat`.
pub const TP_INSERT_RUNTIME_STAT: i32 = 10;
/// Go `TpIndexLookUpRunTimeStats`: the tp for `IndexLookUpRunTimeStats`.
pub const TP_INDEX_LOOK_UP_RUN_TIME_STATS: i32 = 11;
/// Go `TpSlowQueryRuntimeStat`: the tp for `SlowQueryRuntimeStat`.
pub const TP_SLOW_QUERY_RUNTIME_STAT: i32 = 12;
/// Go `TpHashAggRuntimeStat`: the tp for `HashAggRuntimeStat`.
pub const TP_HASH_AGG_RUNTIME_STAT: i32 = 13;
/// Go `TpIndexMergeRunTimeStats`: the tp for `IndexMergeRunTimeStats`.
pub const TP_INDEX_MERGE_RUN_TIME_STATS: i32 = 14;
/// Go `TpBasicCopRunTimeStats`: the tp for `BasicCopRunTimeStats`.
pub const TP_BASIC_COP_RUN_TIME_STATS: i32 = 15;
/// Go `TpUpdateRuntimeStats`: the tp for `UpdateRuntimeStats`.
pub const TP_UPDATE_RUNTIME_STATS: i32 = 16;
/// Go `TpFKCheckRuntimeStats`: the tp for `FKCheckRuntimeStats`.
pub const TP_FK_CHECK_RUNTIME_STATS: i32 = 17;
/// Go `TpFKCascadeRuntimeStats`: the tp for `FKCascadeRuntimeStats`.
pub const TP_FK_CASCADE_RUNTIME_STATS: i32 = 18;
/// Go `TpRURuntimeStats`: the tp for `RURuntimeStats`.
pub const TP_RU_RUNTIME_STATS: i32 = 19;

/// Go `execdetails.MaxDetailsNumsForOneQuery` (`execdetails.go`): the max
/// number of details kept exactly before [`Percentile`] switches to the
/// digest.
pub const MAX_DETAILS_NUMS_FOR_ONE_QUERY: usize = 1000;

/// Go `rmclient.RUVersionV1` (pd client): RU accounting v1.
pub const RU_VERSION_V1: i64 = 1;
/// Go `rmclient.RUVersionV2` (pd client): RU accounting v2.
pub const RU_VERSION_V2: i64 = 2;

/// Go `kv.StoreType` (`pkg/kv/kv.go`): the type of storage engine, with
/// Go's zero value (`TiKV`) as the default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StoreType {
    /// Go `kv.TiKV`.
    #[default]
    TiKv,
    /// Go `kv.TiFlash`.
    TiFlash,
    /// Go `kv.TiDB`.
    TiDb,
    /// Go `kv.UnSpecified`.
    Unspecified,
}

impl StoreType {
    /// Go `StoreType.Name`: the name of the store type.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            StoreType::TiFlash => "tiflash",
            StoreType::TiDb => "tidb",
            StoreType::TiKv => "tikv",
            StoreType::Unspecified => "unspecified",
        }
    }
}

/// Go `util.go`'s `canGetFloat64`: a value that can be read as `float64`.
pub trait CanGetFloat64 {
    /// Go `GetFloat64`.
    fn get_float64(&self) -> f64;
}

/// Go `execdetails.Int64`: an `int64` wrapper implementing
/// [`CanGetFloat64`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Int64(pub i64);

impl CanGetFloat64 for Int64 {
    #[expect(clippy::cast_precision_loss, reason = "Go float64(int64) conversion")]
    fn get_float64(&self) -> f64 {
        self.0 as f64
    }
}

/// Go `execdetails.Duration`: a `time.Duration` (nanoseconds as `int64`)
/// wrapper implementing [`CanGetFloat64`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Duration(pub i64);

impl CanGetFloat64 for Duration {
    #[expect(
        clippy::cast_precision_loss,
        reason = "Go float64(time.Duration) conversion"
    )]
    fn get_float64(&self) -> f64 {
        self.0 as f64
    }
}

/// Go `execdetails.DurationWithAddr`: a duration paired with the store
/// address it was observed at.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DurationWithAddr {
    /// Go `DurationWithAddr.D` (nanoseconds as `int64`).
    pub d: i64,
    /// Go `DurationWithAddr.Addr`.
    pub addr: String,
}

impl CanGetFloat64 for DurationWithAddr {
    #[expect(
        clippy::cast_precision_loss,
        reason = "Go float64(time.Duration) conversion"
    )]
    fn get_float64(&self) -> f64 {
        self.d as f64
    }
}

/// One weighted centroid of the simplified digest.
#[derive(Clone, Copy, Debug)]
struct Centroid {
    mean: f64,
    weight: f64,
}

/// The simplified stand-in for `github.com/influxdata/tdigest`: a sorted
/// centroid list merging exactly-equal means, compressed pairwise past a
/// cap. Exact when all values coincide (what the tests pin); approximate
/// quantiles otherwise.
#[derive(Clone, Debug, Default)]
struct TDigest {
    centroids: Vec<Centroid>,
}

impl TDigest {
    const COMPRESS_LIMIT: usize = 4096;

    fn add(&mut self, mean: f64, weight: f64) {
        match self.centroids.binary_search_by(|c| c.mean.total_cmp(&mean)) {
            Ok(idx) => self.centroids[idx].weight += weight,
            Err(idx) => self.centroids.insert(idx, Centroid { mean, weight }),
        }
        if self.centroids.len() > Self::COMPRESS_LIMIT {
            self.compress();
        }
    }

    fn compress(&mut self) {
        let mut merged = Vec::with_capacity(self.centroids.len().div_ceil(2));
        for pair in self.centroids.chunks(2) {
            let weight: f64 = pair.iter().map(|c| c.weight).sum();
            let mean = pair.iter().map(|c| c.mean * c.weight).sum::<f64>() / weight;
            merged.push(Centroid { mean, weight });
        }
        self.centroids = merged;
    }

    fn add_centroid_list(&mut self, other: &TDigest) {
        for c in &other.centroids {
            self.add(c.mean, c.weight);
        }
    }

    /// Go `tdigest.Quantile`, approximated: the mean of the centroid the
    /// cumulative weight `q * total` falls into.
    fn quantile(&self, q: f64) -> f64 {
        let total: f64 = self.centroids.iter().map(|c| c.weight).sum();
        if total == 0.0 {
            return f64::NAN;
        }
        let target = q * total;
        let mut cumulative = 0.0;
        for c in &self.centroids {
            cumulative += c.weight;
            if cumulative >= target {
                return c.mean;
            }
        }
        self.centroids[self.centroids.len() - 1].mean
    }
}

/// Go `execdetails.Percentile`: percentile calculation over a series of
/// values, exact up to [`MAX_DETAILS_NUMS_FOR_ONE_QUERY`] values and
/// digest-backed beyond.
#[derive(Clone, Debug)]
pub struct Percentile<T> {
    values: Vec<T>,
    size: usize,
    is_sorted: bool,
    min_val: T,
    max_val: T,
    sum_val: f64,
    dt: Option<TDigest>,
}

impl<T: Default> Default for Percentile<T> {
    fn default() -> Self {
        Percentile {
            values: Vec::new(),
            size: 0,
            is_sorted: false,
            min_val: T::default(),
            max_val: T::default(),
            sum_val: 0.0,
            dt: None,
        }
    }
}

impl<T: CanGetFloat64 + Clone + Default> Percentile<T> {
    /// Go `Percentile.Add`: adds a value to calculate the percentile.
    pub fn add(&mut self, value: T) {
        self.is_sorted = false;
        self.sum_val += value.get_float64();
        self.size += 1;
        if self.dt.is_none() && self.values.is_empty() {
            self.min_val = value.clone();
            self.max_val = value.clone();
        } else {
            if value.get_float64() < self.min_val.get_float64() {
                self.min_val = value.clone();
            }
            if value.get_float64() > self.max_val.get_float64() {
                self.max_val = value.clone();
            }
        }
        match &mut self.dt {
            None => {
                self.values.push(value);
                if self.values.len() >= MAX_DETAILS_NUMS_FOR_ONE_QUERY {
                    let mut dt = TDigest::default();
                    for v in &self.values {
                        dt.add(v.get_float64(), 1.0);
                    }
                    self.values = Vec::new();
                    self.dt = Some(dt);
                }
            }
            Some(dt) => dt.add(value.get_float64(), 1.0),
        }
    }

    /// Go `Percentile.GetPercentile`: the percentile `f` of the values.
    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "Go int(float64(len(p.values)) * f) index arithmetic"
    )]
    pub fn get_percentile(&mut self, f: f64) -> f64 {
        match &self.dt {
            None => {
                if !self.is_sorted {
                    self.is_sorted = true;
                    self.values
                        .sort_by(|i, j| i.get_float64().total_cmp(&j.get_float64()));
                }
                self.values[(self.values.len() as f64 * f) as usize].get_float64()
            }
            Some(dt) => dt.quantile(f),
        }
    }

    /// Go `Percentile.GetMax`: the max value.
    #[must_use]
    pub fn get_max(&self) -> T {
        self.max_val.clone()
    }

    /// Go `Percentile.GetMin`: the min value.
    #[must_use]
    pub fn get_min(&self) -> T {
        self.min_val.clone()
    }

    /// Go `Percentile.MergePercentile`: merges two `Percentile`s. As in Go,
    /// the digest-to-digest branch leaves `minVal`/`maxVal` untouched.
    pub fn merge_percentile(&mut self, p2: &Percentile<T>) {
        self.is_sorted = false;
        let Some(other_dt) = &p2.dt else {
            for v in &p2.values {
                self.add(v.clone());
            }
            return;
        };
        self.sum_val += p2.sum_val;
        self.size += p2.size;
        if self.dt.is_none() {
            let mut dt = TDigest::default();
            for v in &self.values {
                dt.add(v.get_float64(), 1.0);
            }
            self.values = Vec::new();
            self.dt = Some(dt);
        }
        if let Some(dt) = &mut self.dt {
            dt.add_centroid_list(other_dt);
        }
    }

    /// Go `Percentile.Size`: the number of values.
    #[must_use]
    pub fn size(&self) -> usize {
        self.size
    }

    /// Go `Percentile.Sum`: the sum of the values.
    #[must_use]
    pub fn sum(&self) -> f64 {
        self.sum_val
    }
}

/// Go `execdetails.FormatDuration` (`util.go`): formats a duration for
/// explain output, pruning precision for human readability — under 1µs the
/// plain Go spelling; otherwise 2 decimals below 10 units and 1 decimal
/// from 10 units up.
#[must_use]
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "Go time.Duration(float64) and float64(time.Duration) conversions"
)]
pub fn format_duration(d: StdDuration) -> String {
    let ns = d.as_nanos() as i64;
    if ns <= 1_000 {
        return format_go_duration(d);
    }
    let unit = get_unit(ns);
    if unit == 1 {
        return format_go_duration(d);
    }
    let integer = (ns / unit) * unit;
    let mut decimal = (ns % unit) as f64 / unit as f64;
    if ns < 10 * unit {
        decimal = (decimal * 100.0).round() / 100.0;
    } else {
        decimal = (decimal * 10.0).round() / 10.0;
    }
    let pruned = integer + (decimal * unit as f64) as i64;
    format_go_duration(StdDuration::from_nanos(pruned as u64))
}

/// Go `getUnit` (`util.go`): the pruning unit for [`format_duration`], in
/// nanoseconds.
fn get_unit(ns: i64) -> i64 {
    if ns >= 1_000_000_000 {
        1_000_000_000
    } else if ns >= 1_000_000 {
        1_000_000
    } else if ns >= 1_000 {
        1_000
    } else {
        1
    }
}

/// client-go `util.FormatBytes`, restated from TiDB's own
/// `memory.FormatBytes` (`pkg/util/memory/tracker.go`) because the
/// client-go source is not on disk; pinned by the fixtures at the plain
/// `Bytes` and `98.2 KB` points.
#[expect(clippy::cast_precision_loss, reason = "Go float64(int64) conversion")]
fn format_bytes(num_bytes: i64) -> String {
    const KB: i64 = 1 << 10;
    const MB: i64 = 1 << 20;
    const GB: i64 = 1 << 30;
    if num_bytes <= KB {
        return format!("{num_bytes} Bytes");
    }
    let (unit, unit_str) = if num_bytes > GB {
        (GB, "GB")
    } else if num_bytes > MB {
        (MB, "MB")
    } else {
        (KB, "KB")
    };
    let v = num_bytes as f64 / unit as f64;
    let decimal = if num_bytes % unit == 0 {
        0
    } else if v < 10.0 {
        2
    } else {
        1
    };
    format!("{v:.decimal$} {unit_str}")
}

/// Go `tipb.ExecutorExecutionSummary.DetailInfo` oneof, narrowed to the
/// two variants the ported TiFlash arms read (Go's
/// `summary.GetTiflashScanContext()`/`summary.GetColumnarScanContext()`
/// yield nil unless `DetailInfo` holds the matching variant).
#[derive(Clone, Debug, PartialEq, Eq)]
#[expect(
    clippy::large_enum_variant,
    reason = "narrowing of the Go oneof; the wide scan snapshot variant is \
              intentional"
)]
pub enum ExecutionSummaryDetailInfo {
    /// Go `*tipb.ExecutorExecutionSummary_TiflashScanContext`.
    TiflashScanContext(TiFlashScanContextSnapshot),
    /// Go `*tipb.ExecutorExecutionSummary_ColumnarScanContext`.
    ColumnarScanContext(ColumnarScanContextSnapshot),
}

/// The fields this module reads off a `*tipb.ExecutorExecutionSummary`,
/// with proto optionals collapsed to Go's zero-defaulted getter results.
/// The TiFlash `DetailInfo`/wait/network payloads carry the
/// [`crate::tiflash_stats`] snapshot narrowings, `Option`-wrapped like
/// Go's nil-checked getters.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExecutorExecutionSummarySnapshot {
    /// Go `summary.TimeProcessedNs`.
    pub time_processed_ns: u64,
    /// Go `summary.NumProducedRows`.
    pub num_produced_rows: u64,
    /// Go `summary.NumIterations`.
    pub num_iterations: u64,
    /// Go `summary.GetConcurrency()` (0 when unset).
    pub concurrency: u64,
    /// Go `summary.GetExecutorId()` (`""` when unset).
    pub executor_id: String,
    /// Go `summary.DetailInfo` (`None` when unset).
    pub detail_info: Option<ExecutionSummaryDetailInfo>,
    /// Go `summary.GetTiflashWaitSummary()` (`None` when unset).
    pub tiflash_wait_summary: Option<TiFlashWaitSummarySnapshot>,
    /// Go `summary.GetTiflashNetworkSummary()` (`None` when unset).
    pub tiflash_network_summary: Option<TiFlashNetworkSummarySnapshot>,
}

/// client-go `util.ScanDetail` as `runtime_stats.go` reads it: the
/// already-ported [`ScanDetail`] value plus `ProcessedKeysSize`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CopScanDetail {
    /// The [`crate::exec_details::ScanDetail`] slice of the client-go
    /// struct.
    pub base: ScanDetail,
    /// Go `ScanDetail.ProcessedKeysSize` (`int64`).
    pub processed_keys_size: i64,
}

impl CopScanDetail {
    /// client-go `ScanDetail.Merge`: the additive merge pinned by the
    /// `TestCopRuntimeStats2` sums.
    pub fn merge(&mut self, other: &CopScanDetail) {
        self.base.processed_keys += other.base.processed_keys;
        self.processed_keys_size += other.processed_keys_size;
        self.base.total_keys += other.base.total_keys;
        self.base.get_snapshot_duration += other.base.get_snapshot_duration;
        self.base.rocksdb_delete_skipped_count += other.base.rocksdb_delete_skipped_count;
        self.base.rocksdb_key_skipped_count += other.base.rocksdb_key_skipped_count;
        self.base.rocksdb_block_cache_hit_count += other.base.rocksdb_block_cache_hit_count;
        self.base.rocksdb_block_read_count += other.base.rocksdb_block_read_count;
        self.base.rocksdb_block_read_byte += other.base.rocksdb_block_read_byte;
        self.base.rocksdb_block_read_duration += other.base.rocksdb_block_read_duration;
        self.base.ia_remote_read_segment_count += other.base.ia_remote_read_segment_count;
        self.base.ia_remote_read_segment_bytes += other.base.ia_remote_read_segment_bytes;
        self.base.ia_remote_read_segment_duration += other.base.ia_remote_read_segment_duration;
    }

    /// client-go `ScanDetail.String`, recovered from the Go test literals:
    /// each field zero-suppressed, `""` when nothing renders.
    #[must_use]
    #[expect(
        clippy::cast_possible_wrap,
        reason = "Go renders the uint64 count verbatim"
    )]
    pub fn string(&self) -> String {
        let mut block: Vec<String> = Vec::new();
        if self.base.rocksdb_block_cache_hit_count > 0 {
            block.push(format!(
                "cache_hit_count: {}",
                self.base.rocksdb_block_cache_hit_count
            ));
        }
        if self.base.rocksdb_block_read_count > 0 {
            block.push(format!(
                "read_count: {}",
                self.base.rocksdb_block_read_count
            ));
        }
        if self.base.rocksdb_block_read_byte > 0 {
            block.push(format!(
                "read_byte: {}",
                format_bytes(self.base.rocksdb_block_read_byte as i64)
            ));
        }
        if self.base.rocksdb_block_read_duration > StdDuration::ZERO {
            block.push(format!(
                "read_time: {}",
                format_duration(self.base.rocksdb_block_read_duration)
            ));
        }
        let mut rocksdb: Vec<String> = Vec::new();
        if self.base.rocksdb_delete_skipped_count > 0 {
            rocksdb.push(format!(
                "delete_skipped_count: {}",
                self.base.rocksdb_delete_skipped_count
            ));
        }
        if self.base.rocksdb_key_skipped_count > 0 {
            rocksdb.push(format!(
                "key_skipped_count: {}",
                self.base.rocksdb_key_skipped_count
            ));
        }
        if !block.is_empty() {
            rocksdb.push(format!("block: {{{}}}", block.join(", ")));
        }
        let mut parts: Vec<String> = Vec::new();
        if self.base.processed_keys > 0 {
            parts.push(format!("total_process_keys: {}", self.base.processed_keys));
        }
        if self.processed_keys_size > 0 {
            parts.push(format!(
                "total_process_keys_size: {}",
                self.processed_keys_size
            ));
        }
        if self.base.total_keys > 0 {
            parts.push(format!("total_keys: {}", self.base.total_keys));
        }
        if !rocksdb.is_empty() {
            parts.push(format!("rocksdb: {{{}}}", rocksdb.join(", ")));
        }
        if parts.is_empty() {
            return String::new();
        }
        format!("scan_detail: {{{}}}", parts.join(", "))
    }
}

/// client-go `util.TimeDetail` as `runtime_stats.go` reads it: the
/// already-ported [`TimeDetail`] value plus `SuspendTime` and
/// `KvReadWallTime`. The `KvGrpc*` fields are not representable in the
/// reused type and stay open.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CopTimeDetail {
    /// The [`crate::exec_details::TimeDetail`] slice of the client-go
    /// struct.
    pub base: TimeDetail,
    /// Go `TimeDetail.SuspendTime`.
    pub suspend_time: StdDuration,
    /// Go `TimeDetail.KvReadWallTime`.
    pub kv_read_wall_time: StdDuration,
}

impl CopTimeDetail {
    /// client-go `TimeDetail.Merge`: the additive merge pinned by the
    /// `TestCopRuntimeStats2` sums.
    pub fn merge(&mut self, other: &CopTimeDetail) {
        self.base.process_time += other.base.process_time;
        self.suspend_time += other.suspend_time;
        self.base.wait_time += other.base.wait_time;
        self.kv_read_wall_time += other.kv_read_wall_time;
        self.base.total_rpc_wall_time += other.base.total_rpc_wall_time;
    }

    /// client-go `TimeDetail.String`, recovered from the
    /// `TestCopRuntimeStats2` literal: each field zero-suppressed, `""`
    /// when nothing renders.
    #[must_use]
    pub fn string(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if self.base.process_time > StdDuration::ZERO {
            parts.push(format!(
                "total_process_time: {}",
                format_duration(self.base.process_time)
            ));
        }
        if self.suspend_time > StdDuration::ZERO {
            parts.push(format!(
                "total_suspend_time: {}",
                format_duration(self.suspend_time)
            ));
        }
        if self.base.wait_time > StdDuration::ZERO {
            parts.push(format!(
                "total_wait_time: {}",
                format_duration(self.base.wait_time)
            ));
        }
        if self.kv_read_wall_time > StdDuration::ZERO {
            parts.push(format!(
                "total_kv_read_wall_time: {}",
                format_duration(self.kv_read_wall_time)
            ));
        }
        if self.base.total_rpc_wall_time > StdDuration::ZERO {
            parts.push(format!(
                "tikv_wall_time: {}",
                format_duration(self.base.total_rpc_wall_time)
            ));
        }
        if parts.is_empty() {
            return String::new();
        }
        format!("time_detail: {{{}}}", parts.join(", "))
    }
}

/// Go `RuntimeStats`: the executor runtime information interface.
pub trait RuntimeStats: Any {
    /// Go `String`.
    fn string(&self) -> String;
    /// Go `Merge`.
    fn merge(&mut self, other: &dyn RuntimeStats);
    /// Go `Clone`.
    fn clone_box(&self) -> Box<dyn RuntimeStats>;
    /// Go `Tp`.
    fn tp(&self) -> i32;
    /// The Go type switch's `rs.(*ConcreteType)` seam.
    fn as_any(&self) -> &dyn Any;
}

/// Go `basicCopRuntimeStats` (package-private): the per-executor cop task
/// accumulator.
#[derive(Clone, Debug, Default)]
pub struct BasicCopRuntimeStats {
    /// Go `basicCopRuntimeStats.loop`.
    loops: i32,
    /// Go `basicCopRuntimeStats.rows`.
    rows: i64,
    /// Go `basicCopRuntimeStats.threads`.
    threads: i32,
    /// Go `basicCopRuntimeStats.procTimes`.
    proc_times: Percentile<Duration>,
    /// Go `basicCopRuntimeStats.tiflashStats` ("executor extra infos").
    tiflash_stats: Option<TiflashStats>,
}

impl BasicCopRuntimeStats {
    /// Go `basicCopRuntimeStats.mergeExecSummary`: merges an
    /// `ExecutorExecutionSummary` directly.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        reason = "Go int32/int64(uint64) conversions"
    )]
    fn merge_exec_summary(&mut self, summary: &ExecutorExecutionSummarySnapshot) {
        self.loops += summary.num_iterations as i32;
        self.rows += summary.num_produced_rows as i64;
        self.threads += summary.concurrency as i32;
        self.proc_times
            .add(Duration(summary.time_processed_ns as i64));
        match &summary.detail_info {
            Some(ExecutionSummaryDetailInfo::TiflashScanContext(tiflash_scan_context)) => {
                self.tiflash_stats
                    .get_or_insert_with(TiflashStats::default)
                    .scan_context
                    .merge_exec_summary(Some(tiflash_scan_context));
            }
            Some(ExecutionSummaryDetailInfo::ColumnarScanContext(columnar_scan_context)) => {
                self.tiflash_stats
                    .get_or_insert_with(TiflashStats::default)
                    .columnar_scan_context
                    .merge_exec_summary(Some(columnar_scan_context));
            }
            None => {}
        }
        if let Some(tiflash_wait_summary) = &summary.tiflash_wait_summary {
            self.tiflash_stats
                .get_or_insert_with(TiflashStats::default)
                .wait_summary
                .merge_exec_summary(Some(tiflash_wait_summary), summary.time_processed_ns);
        }
        if let Some(tiflash_network_summary) = &summary.tiflash_network_summary {
            self.tiflash_stats
                .get_or_insert_with(TiflashStats::default)
                .network_summary
                .merge_exec_summary(Some(tiflash_network_summary));
        }
    }
}

impl RuntimeStats for BasicCopRuntimeStats {
    /// Go `basicCopRuntimeStats.String`.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Go time.Duration(float64) conversion"
    )]
    fn string(&self) -> String {
        let mut buf = String::with_capacity(16);
        buf.push_str("time:");
        buf.push_str(&format_duration(StdDuration::from_nanos(
            (self.proc_times.sum_val as i64).max(0) as u64,
        )));
        buf.push_str(", loops:");
        buf.push_str(&self.loops.to_string());
        if let Some(tiflash_stats) = &self.tiflash_stats {
            buf.push_str(", threads:");
            buf.push_str(&self.threads.to_string());
            if !tiflash_stats.wait_summary.can_be_ignored() {
                buf.push_str(", ");
                buf.push_str(&tiflash_stats.wait_summary.string());
            }
            if !tiflash_stats.network_summary.empty() {
                buf.push_str(", ");
                buf.push_str(&tiflash_stats.network_summary.string());
            }
            buf.push_str(", ");
            buf.push_str(&tiflash_stats.scan_context.string());
        }
        buf
    }

    fn merge(&mut self, other: &dyn RuntimeStats) {
        let Some(tmp) = other.as_any().downcast_ref::<BasicCopRuntimeStats>() else {
            return;
        };
        self.loops += tmp.loops;
        self.rows += tmp.rows;
        self.threads += tmp.threads;
        if tmp.proc_times.size() > 0 {
            self.proc_times.merge_percentile(&tmp.proc_times);
        }
        if let Some(tmp_tiflash_stats) = &tmp.tiflash_stats {
            let tiflash_stats = self.tiflash_stats.get_or_insert_with(TiflashStats::default);
            tiflash_stats
                .scan_context
                .merge(&tmp_tiflash_stats.scan_context);
            tiflash_stats
                .columnar_scan_context
                .merge(&tmp_tiflash_stats.columnar_scan_context);
            tiflash_stats
                .wait_summary
                .merge(&tmp_tiflash_stats.wait_summary);
            tiflash_stats
                .network_summary
                .merge(&tmp_tiflash_stats.network_summary);
        }
    }

    fn clone_box(&self) -> Box<dyn RuntimeStats> {
        Box::new(BasicCopRuntimeStats {
            loops: self.loops,
            rows: self.rows,
            threads: self.threads,
            proc_times: self.proc_times.clone(),
            tiflash_stats: self.tiflash_stats.clone(),
        })
    }

    fn tp(&self) -> i32 {
        TP_BASIC_COP_RUN_TIME_STATS
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Go `StmtCopRuntimeStats`: stores the cop runtime stats of the total
/// statement.
#[derive(Clone, Copy, Debug, Default)]
pub struct StmtCopRuntimeStats {
    /// Go `StmtCopRuntimeStats.TiflashNetworkStats`: stats all mpp tasks'
    /// network traffic info, `None` if no any mpp tasks' network traffic.
    pub tiflash_network_stats: Option<TiFlashNetworkTrafficSummary>,
}

impl StmtCopRuntimeStats {
    /// Go `StmtCopRuntimeStats.mergeExecSummary`: merges an
    /// `ExecutorExecutionSummary` into stmt cop runtime stats directly.
    fn merge_exec_summary(&mut self, summary: &ExecutorExecutionSummarySnapshot) {
        if let Some(tiflash_network_summary) = &summary.tiflash_network_summary {
            self.tiflash_network_stats
                .get_or_insert_with(TiFlashNetworkTrafficSummary::default)
                .merge_exec_summary(Some(tiflash_network_summary));
        }
    }
}

/// Go `CopRuntimeStats`: collects cop tasks' execution info for one plan.
/// The fields are Go-package-private; `pub(crate)` is the Go
/// same-package-test seam for [`crate::tiflash_stats`]'s test module.
#[derive(Clone, Debug, Default)]
pub struct CopRuntimeStats {
    /// Go `CopRuntimeStats.stats`.
    pub(crate) stats: BasicCopRuntimeStats,
    /// Go `CopRuntimeStats.scanDetail`.
    pub(crate) scan_detail: CopScanDetail,
    /// Go `CopRuntimeStats.timeDetail`.
    pub(crate) time_detail: CopTimeDetail,
    /// Go `CopRuntimeStats.storeType`.
    store_type: StoreType,
}

impl CopRuntimeStats {
    /// Go `CopRuntimeStats.GetActRows`: total rows.
    #[must_use]
    pub fn get_act_rows(&self) -> i64 {
        self.stats.rows
    }

    /// Go `CopRuntimeStats.GetTasks`: total tasks.
    #[must_use]
    #[expect(clippy::cast_possible_truncation, reason = "Go int32(size) conversion")]
    pub fn get_tasks(&self) -> i32 {
        self.stats.proc_times.size as i32
    }

    /// Go `CopRuntimeStats.String`.
    #[must_use]
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "Go time.Duration(float64) and int64 division conversions"
    )]
    pub fn string(&self) -> String {
        let mut proc_times = self.stats.proc_times.clone();
        let total_tasks = proc_times.size();
        let is_tiflash_cop = self.store_type == StoreType::TiFlash;
        let mut buf = String::with_capacity(16);
        let print_tiflash_specific_info = |buf: &mut String| {
            if is_tiflash_cop {
                buf.push_str(", ");
                buf.push_str("threads:");
                buf.push_str(&self.stats.threads.to_string());
                buf.push('}');
                if let Some(tiflash_stats) = &self.stats.tiflash_stats {
                    if !tiflash_stats.wait_summary.can_be_ignored() {
                        buf.push_str(", ");
                        buf.push_str(&tiflash_stats.wait_summary.string());
                    }
                    if !tiflash_stats.network_summary.empty() {
                        buf.push_str(", ");
                        buf.push_str(&tiflash_stats.network_summary.string());
                    }
                    if !tiflash_stats.columnar_scan_context.empty() {
                        buf.push_str(", ");
                        buf.push_str(&tiflash_stats.columnar_scan_context.string());
                    } else if !tiflash_stats.scan_context.empty() {
                        buf.push_str(", ");
                        buf.push_str(&tiflash_stats.scan_context.string());
                    }
                }
            } else {
                buf.push('}');
            }
        };
        if total_tasks == 1 {
            buf.push_str(self.store_type.name());
            buf.push_str("_task:{time:");
            buf.push_str(&format_duration(StdDuration::from_nanos(
                (proc_times.get_percentile(0.0) as i64).max(0) as u64,
            )));
            buf.push_str(", loops:");
            buf.push_str(&self.stats.loops.to_string());
            print_tiflash_specific_info(&mut buf);
        } else if total_tasks > 0 {
            buf.push_str(self.store_type.name());
            buf.push_str("_task:{proc max:");
            buf.push_str(&format_duration(StdDuration::from_nanos(
                (proc_times.get_max().get_float64() as i64).max(0) as u64,
            )));
            buf.push_str(", min:");
            buf.push_str(&format_duration(StdDuration::from_nanos(
                (proc_times.get_min().get_float64() as i64).max(0) as u64,
            )));
            buf.push_str(", avg: ");
            buf.push_str(&format_duration(StdDuration::from_nanos(
                ((proc_times.sum() as i64) / (total_tasks as i64)).max(0) as u64,
            )));
            buf.push_str(", p80:");
            buf.push_str(&format_duration(StdDuration::from_nanos(
                (proc_times.get_percentile(0.8) as i64).max(0) as u64,
            )));
            buf.push_str(", p95:");
            buf.push_str(&format_duration(StdDuration::from_nanos(
                (proc_times.get_percentile(0.95) as i64).max(0) as u64,
            )));
            buf.push_str(", iters:");
            buf.push_str(&self.stats.loops.to_string());
            buf.push_str(", tasks:");
            buf.push_str(&total_tasks.to_string());
            print_tiflash_specific_info(&mut buf);
        }
        if !is_tiflash_cop {
            let detail = self.scan_detail.string();
            if !detail.is_empty() {
                buf.push_str(", ");
                buf.push_str(&detail);
            }
            if self.time_detail != CopTimeDetail::default() {
                let time_detail_str = self.time_detail.string();
                if !time_detail_str.is_empty() {
                    buf.push_str(", ");
                    buf.push_str(&time_detail_str);
                }
            }
        }
        buf
    }
}

/// Go `BasicRuntimeStats`: the basic runtime stats. All executors with the
/// same executor id share one instance (through [`Rc`] here), so the
/// counters stay real atomics as in Go (SeqCst).
#[derive(Debug, Default)]
pub struct BasicRuntimeStats {
    /// Go `BasicRuntimeStats.executorCount`.
    executor_count: AtomicI32,
    /// Go `BasicRuntimeStats.loop`: executor's `Next()` called times.
    loops: AtomicI32,
    /// Go `BasicRuntimeStats.consume`: total consume time (open + next +
    /// close), in nanoseconds.
    consume: AtomicI64,
    /// Go `BasicRuntimeStats.open`: executor open time, in nanoseconds.
    open: AtomicI64,
    /// Go `BasicRuntimeStats.close`: executor close time, in nanoseconds.
    close: AtomicI64,
    /// Go `BasicRuntimeStats.rows`: executor returned row count.
    rows: AtomicI64,
}

impl BasicRuntimeStats {
    /// Go `BasicRuntimeStats.GetActRows`: total rows.
    #[must_use]
    pub fn get_act_rows(&self) -> i64 {
        self.rows.load(Ordering::SeqCst)
    }

    /// Go `BasicRuntimeStats.Record`: records one executor round.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Go int64(time.Duration) conversion"
    )]
    pub fn record(&self, d: StdDuration, row_num: i64) {
        self.loops.fetch_add(1, Ordering::SeqCst);
        self.consume
            .fetch_add(d.as_nanos() as i64, Ordering::SeqCst);
        self.rows.fetch_add(row_num, Ordering::SeqCst);
    }

    /// Go `BasicRuntimeStats.RecordOpen`: records executor open time.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Go int64(time.Duration) conversion"
    )]
    pub fn record_open(&self, d: StdDuration) {
        self.consume
            .fetch_add(d.as_nanos() as i64, Ordering::SeqCst);
        self.open.fetch_add(d.as_nanos() as i64, Ordering::SeqCst);
    }

    /// Go `BasicRuntimeStats.RecordClose`: records executor close time.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Go int64(time.Duration) conversion"
    )]
    pub fn record_close(&self, d: StdDuration) {
        self.consume
            .fetch_add(d.as_nanos() as i64, Ordering::SeqCst);
        self.close.fetch_add(d.as_nanos() as i64, Ordering::SeqCst);
    }

    /// Go `BasicRuntimeStats.SetRowNum`: sets the row num.
    pub fn set_row_num(&self, row_num: i64) {
        self.rows.store(row_num, Ordering::SeqCst);
    }

    /// Go `BasicRuntimeStats.GetTime`: the total consume time in
    /// nanoseconds.
    #[must_use]
    pub fn get_time(&self) -> i64 {
        self.consume.load(Ordering::SeqCst)
    }
}

impl RuntimeStats for BasicRuntimeStats {
    /// Go `BasicRuntimeStats.String` (Go's nil-receiver `""` branch is the
    /// caller's `Option` here).
    #[expect(clippy::cast_sign_loss, reason = "Go time.Duration(int64) conversion")]
    fn string(&self) -> String {
        let mut s = String::new();
        let time_prefix = if self.executor_count.load(Ordering::SeqCst) > 1 {
            "total_"
        } else {
            ""
        };
        let total_time = self.consume.load(Ordering::SeqCst);
        let open_time = self.open.load(Ordering::SeqCst);
        let close_time = self.close.load(Ordering::SeqCst);
        s.push_str(time_prefix);
        s.push_str("time:");
        s.push_str(&format_duration(StdDuration::from_nanos(
            total_time.max(0) as u64
        )));
        s.push_str(", ");
        s.push_str(time_prefix);
        s.push_str("open:");
        s.push_str(&format_duration(StdDuration::from_nanos(
            open_time.max(0) as u64
        )));
        s.push_str(", ");
        s.push_str(time_prefix);
        s.push_str("close:");
        s.push_str(&format_duration(StdDuration::from_nanos(
            close_time.max(0) as u64
        )));
        s.push_str(", loops:");
        s.push_str(&self.loops.load(Ordering::SeqCst).to_string());
        s
    }

    fn merge(&mut self, other: &dyn RuntimeStats) {
        let Some(tmp) = other.as_any().downcast_ref::<BasicRuntimeStats>() else {
            return;
        };
        self.loops
            .fetch_add(tmp.loops.load(Ordering::SeqCst), Ordering::SeqCst);
        self.consume
            .fetch_add(tmp.consume.load(Ordering::SeqCst), Ordering::SeqCst);
        self.open
            .fetch_add(tmp.open.load(Ordering::SeqCst), Ordering::SeqCst);
        self.close
            .fetch_add(tmp.close.load(Ordering::SeqCst), Ordering::SeqCst);
        self.rows
            .fetch_add(tmp.rows.load(Ordering::SeqCst), Ordering::SeqCst);
    }

    /// Go panics here: all executors with the same executor id must share
    /// one `BasicRuntimeStats`.
    fn clone_box(&self) -> Box<dyn RuntimeStats> {
        panic!("BasicRuntimeStats should not implement Clone function");
    }

    fn tp(&self) -> i32 {
        TP_BASIC_RUNTIME_STATS
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Go `RootRuntimeStats`: combines the shared basic stats with the
/// registered per-kind group stats.
#[derive(Default)]
pub struct RootRuntimeStats {
    /// Go `RootRuntimeStats.basic` (shared across same-id executors).
    basic: Option<Rc<BasicRuntimeStats>>,
    /// Go `RootRuntimeStats.groupRss`.
    group_rss: Vec<Box<dyn RuntimeStats>>,
}

impl RootRuntimeStats {
    /// Go `NewRootRuntimeStats`.
    #[must_use]
    pub fn new() -> RootRuntimeStats {
        RootRuntimeStats::default()
    }

    /// Go `RootRuntimeStats.GetActRows`: total rows.
    #[must_use]
    pub fn get_act_rows(&self) -> i64 {
        match &self.basic {
            None => 0,
            Some(basic) => basic.rows.load(Ordering::SeqCst),
        }
    }

    /// Go `RootRuntimeStats.MergeStats`: the stats suitable for display.
    #[must_use]
    pub fn merge_stats(&self) -> (Option<&Rc<BasicRuntimeStats>>, &[Box<dyn RuntimeStats>]) {
        (self.basic.as_ref(), &self.group_rss)
    }

    /// Go `RootRuntimeStats.String`.
    #[must_use]
    pub fn string(&self) -> String {
        let (basic, groups) = self.merge_stats();
        let mut strs = Vec::with_capacity(groups.len() + 1);
        if let Some(basic) = basic {
            strs.push(basic.string());
        }
        for group in groups {
            let s = group.string();
            if !s.is_empty() {
                strs.push(s);
            }
        }
        strs.join(", ")
    }
}

/// Go `RuntimeStatsColl`: collects executors' execution info. The Go
/// `sync.Mutex` flattens away — this snapshot-side port is
/// single-threaded.
#[derive(Default)]
pub struct RuntimeStatsColl {
    /// Go `RuntimeStatsColl.rootStats`.
    root_stats: HashMap<i64, RootRuntimeStats>,
    /// Go `RuntimeStatsColl.copStats`.
    cop_stats: HashMap<i64, CopRuntimeStats>,
    /// Go `RuntimeStatsColl.stmtCopStats`.
    stmt_cop_stats: StmtCopRuntimeStats,
}

impl RuntimeStatsColl {
    /// Go `NewRuntimeStatsColl`: creates a new collector, clearing and
    /// reusing `reuse`'s maps when given one.
    #[must_use]
    pub fn new(reuse: Option<RuntimeStatsColl>) -> RuntimeStatsColl {
        match reuse {
            Some(mut reuse) => {
                reuse.root_stats.clear();
                reuse.cop_stats.clear();
                reuse
            }
            None => RuntimeStatsColl::default(),
        }
    }

    /// Go `RuntimeStatsColl.RegisterStats`: registers a group stat for an
    /// executor, merging into an existing group of the same tp.
    pub fn register_stats(&mut self, plan_id: i64, info: Box<dyn RuntimeStats>) {
        let stats = self.root_stats.entry(plan_id).or_default();
        let tp = info.tp();
        for rss in &mut stats.group_rss {
            if rss.tp() == tp {
                rss.merge(info.as_ref());
                return;
            }
        }
        stats.group_rss.push(info);
    }

    /// Go `RuntimeStatsColl.GetBasicRuntimeStats`: the shared basic stats
    /// for an executor. With `init_new_executor_stats` it creates missing
    /// state and counts the executor; otherwise missing state yields
    /// `None`.
    pub fn get_basic_runtime_stats(
        &mut self,
        plan_id: i64,
        init_new_executor_stats: bool,
    ) -> Option<Rc<BasicRuntimeStats>> {
        let stats = if init_new_executor_stats {
            self.root_stats.entry(plan_id).or_default()
        } else {
            self.root_stats.get_mut(&plan_id)?
        };
        if init_new_executor_stats {
            let basic = stats
                .basic
                .get_or_insert_with(|| Rc::new(BasicRuntimeStats::default()));
            basic.executor_count.fetch_add(1, Ordering::SeqCst);
        }
        stats.basic.clone()
    }

    /// Go `RuntimeStatsColl.GetStmtCopRuntimeStats`: gets execStat for a
    /// executor. Go returns the struct by value; its network pointer
    /// aliasing collapses to the `Copy` of the snapshot-side summary.
    #[must_use]
    pub fn get_stmt_cop_runtime_stats(&self) -> StmtCopRuntimeStats {
        self.stmt_cop_stats
    }

    /// Go `RuntimeStatsColl.GetRootStats`: the root stats for an executor,
    /// created when missing.
    pub fn get_root_stats(&mut self, plan_id: i64) -> &RootRuntimeStats {
        self.root_stats.entry(plan_id).or_default()
    }

    /// Go `RuntimeStatsColl.GetPlanActRows`: the actual rows of the plan.
    #[must_use]
    pub fn get_plan_act_rows(&self, plan_id: i64) -> i64 {
        match self.root_stats.get(&plan_id) {
            None => 0,
            Some(stats) => stats.get_act_rows(),
        }
    }

    /// Go `RuntimeStatsColl.GetCopStats`: the `CopRuntimeStats` for
    /// `plan_id`, `None` when absent. Go returns a live pointer; the
    /// mutable borrow is that seam.
    pub fn get_cop_stats(&mut self, plan_id: i64) -> Option<&mut CopRuntimeStats> {
        self.cop_stats.get_mut(&plan_id)
    }

    /// Go `RuntimeStatsColl.GetCopCountAndRows`: total cop-task count and
    /// rows.
    #[must_use]
    pub fn get_cop_count_and_rows(&self, plan_id: i64) -> (i32, i64) {
        match self.cop_stats.get(&plan_id) {
            None => (0, 0),
            Some(cop) => (cop.get_tasks(), cop.get_act_rows()),
        }
    }

    /// Go `RuntimeStatsColl.RecordCopStats`: records one cop task's
    /// execution detail, returning the (possibly executor-id-overridden)
    /// plan id.
    pub fn record_cop_stats(
        &mut self,
        mut plan_id: i64,
        store_type: StoreType,
        scan: Option<&CopScanDetail>,
        time: CopTimeDetail,
        summary: Option<&ExecutorExecutionSummarySnapshot>,
    ) -> i64 {
        if let Some(cop_stats) = self.cop_stats.get_mut(&plan_id) {
            if let Some(scan) = scan {
                cop_stats.scan_detail.merge(scan);
            }
            cop_stats.time_detail.merge(&time);
        } else {
            self.cop_stats.insert(
                plan_id,
                CopRuntimeStats {
                    scan_detail: scan.cloned().unwrap_or_default(),
                    time_detail: time,
                    store_type,
                    ..CopRuntimeStats::default()
                },
            );
        }
        if let Some(summary) = summary {
            // For a TiFlash cop response the summary carries an executor
            // id; a valid one overwrites the plan id.
            if let Some(id) = get_plan_id_from_execution_summary(summary) {
                if id != plan_id {
                    plan_id = id;
                    self.cop_stats
                        .entry(plan_id)
                        .or_insert_with(|| CopRuntimeStats {
                            store_type,
                            ..CopRuntimeStats::default()
                        });
                }
            }
            if let Some(cop_stats) = self.cop_stats.get_mut(&plan_id) {
                cop_stats.stats.merge_exec_summary(summary);
            }
            self.stmt_cop_stats.merge_exec_summary(summary);
        }
        plan_id
    }

    /// Go `RuntimeStatsColl.RecordOneCopTask`: records one cop task's
    /// execution summary, returning the (possibly executor-id-overridden)
    /// plan id.
    pub fn record_one_cop_task(
        &mut self,
        mut plan_id: i64,
        store_type: StoreType,
        summary: &ExecutorExecutionSummarySnapshot,
    ) -> i64 {
        if let Some(id) = get_plan_id_from_execution_summary(summary) {
            plan_id = id;
        }
        let cop_stats = self
            .cop_stats
            .entry(plan_id)
            .or_insert_with(|| CopRuntimeStats {
                store_type,
                ..CopRuntimeStats::default()
            });
        cop_stats.stats.merge_exec_summary(summary);
        self.stmt_cop_stats.merge_exec_summary(summary);
        plan_id
    }

    /// Go `RuntimeStatsColl.ExistsRootStats`.
    #[must_use]
    pub fn exists_root_stats(&self, plan_id: i64) -> bool {
        self.root_stats.contains_key(&plan_id)
    }

    /// Go `RuntimeStatsColl.ExistsCopStats`.
    #[must_use]
    pub fn exists_cop_stats(&self, plan_id: i64) -> bool {
        self.cop_stats.contains_key(&plan_id)
    }
}

/// Go `getPlanIDFromExecutionSummary`: parses the plan id off the summary's
/// executor id (the digits after the last `_`).
fn get_plan_id_from_execution_summary(summary: &ExecutorExecutionSummarySnapshot) -> Option<i64> {
    if summary.executor_id.is_empty() {
        return None;
    }
    summary
        .executor_id
        .split('_')
        .next_back()
        .and_then(|last| last.parse::<i64>().ok())
}

/// Go `ConcurrencyInfo`: the concurrency information of one executor
/// operator.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConcurrencyInfo {
    /// Go `ConcurrencyInfo.concurrencyName`.
    concurrency_name: String,
    /// Go `ConcurrencyInfo.concurrencyNum`.
    concurrency_num: i64,
}

impl ConcurrencyInfo {
    /// Go `NewConcurrencyInfo`.
    #[must_use]
    pub fn new(name: &str, num: i64) -> ConcurrencyInfo {
        ConcurrencyInfo {
            concurrency_name: name.to_owned(),
            concurrency_num: num,
        }
    }
}

/// Go `RuntimeStatsWithConcurrencyInfo`: concurrency info attached to the
/// runtime stats. Go's embedded `sync.Mutex` flattens away.
#[derive(Clone, Debug, Default)]
pub struct RuntimeStatsWithConcurrencyInfo {
    /// Go `RuntimeStatsWithConcurrencyInfo.concurrency`.
    concurrency: Vec<ConcurrencyInfo>,
}

impl RuntimeStatsWithConcurrencyInfo {
    /// Go `SetConcurrencyInfo`: replaces the concurrency information.
    /// `num <= 0` means the operator is not executed in parallel.
    pub fn set_concurrency_info(&mut self, infos: Vec<ConcurrencyInfo>) {
        self.concurrency = infos;
    }
}

impl RuntimeStats for RuntimeStatsWithConcurrencyInfo {
    fn string(&self) -> String {
        let mut buf = String::with_capacity(8);
        for (i, concurrency) in self.concurrency.iter().enumerate() {
            if i > 0 {
                buf.push_str(", ");
            }
            if concurrency.concurrency_num > 0 {
                buf.push_str(&concurrency.concurrency_name);
                buf.push(':');
                buf.push_str(&concurrency.concurrency_num.to_string());
            } else {
                buf.push_str(&concurrency.concurrency_name);
                buf.push_str(":OFF");
            }
        }
        buf
    }

    fn merge(&mut self, _other: &dyn RuntimeStats) {}

    fn clone_box(&self) -> Box<dyn RuntimeStats> {
        Box::new(self.clone())
    }

    fn tp(&self) -> i32 {
        TP_RUNTIME_STATS_WITH_CONCURRENCY_INFO
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// client-go `CommitDetails.Merge`, recovered as the additive merge (the
/// slowest-request slots keep the larger `ReqTotalTime`); client-go's
/// unpinned arms stay open under this reading.
pub fn merge_commit_details(dst: &mut CommitDetails, src: &CommitDetails) {
    dst.get_commit_ts_time += src.get_commit_ts_time;
    dst.get_latest_ts_time += src.get_latest_ts_time;
    dst.prewrite_time += src.prewrite_time;
    dst.wait_prewrite_binlog_time += src.wait_prewrite_binlog_time;
    dst.commit_time += src.commit_time;
    dst.local_latch_time += src.local_latch_time;
    dst.commit_backoff_time += src.commit_backoff_time;
    dst.prewrite_backoff_types
        .extend(src.prewrite_backoff_types.iter().cloned());
    dst.commit_backoff_types
        .extend(src.commit_backoff_types.iter().cloned());
    if dst.slowest_prewrite.req_total_time < src.slowest_prewrite.req_total_time {
        dst.slowest_prewrite = src.slowest_prewrite.clone();
    }
    if dst.commit_primary.req_total_time < src.commit_primary.req_total_time {
        dst.commit_primary = src.commit_primary.clone();
    }
    dst.resolve_lock.resolve_lock_time += src.resolve_lock.resolve_lock_time;
    dst.write_keys += src.write_keys;
    dst.write_size += src.write_size;
    dst.prewrite_region_num += src.prewrite_region_num;
    dst.txn_retry += src.txn_retry;
}

/// client-go `LockKeysDetails.Merge`, recovered as the additive merge (the
/// slowest-request slot keeps the larger total time); client-go's unpinned
/// arms — notably its exact `RetryCount` handling — stay open under this
/// reading.
pub fn merge_lock_keys_details(dst: &mut LockKeysDetails, src: &LockKeysDetails) {
    dst.total_time += src.total_time;
    dst.region_num += src.region_num;
    dst.lock_keys += src.lock_keys;
    dst.resolve_lock.resolve_lock_time += src.resolve_lock.resolve_lock_time;
    dst.backoff_time += src.backoff_time;
    dst.lock_rpc_time += src.lock_rpc_time;
    dst.lock_rpc_count += src.lock_rpc_count;
    dst.backoff_types.extend(src.backoff_types.iter().cloned());
    if dst.slowest_req_total_time < src.slowest_req_total_time {
        dst.slowest_req_total_time = src.slowest_req_total_time;
        dst.slowest_region = src.slowest_region;
        dst.slowest_store_addr = src.slowest_store_addr.clone();
        dst.slowest_exec_details = src.slowest_exec_details.clone();
    }
    dst.retry_count += src.retry_count;
}

/// Go `RuntimeStatsWithCommit`: the runtime stats with commit and lock-keys
/// detail.
#[derive(Clone, Debug, Default)]
pub struct RuntimeStatsWithCommit {
    /// Go `RuntimeStatsWithCommit.Commit`.
    pub commit: Option<CommitDetails>,
    /// Go `RuntimeStatsWithCommit.LockKeys`.
    pub lock_keys: Option<LockKeysDetails>,
    /// Go `RuntimeStatsWithCommit.SharedLockKeys`.
    pub shared_lock_keys: Option<LockKeysDetails>,
    /// Go `RuntimeStatsWithCommit.TxnCnt`.
    pub txn_cnt: i64,
}

impl RuntimeStatsWithCommit {
    /// Go `MergeCommitDetails`: merges the commit details.
    pub fn merge_commit_details(&mut self, detail: Option<&CommitDetails>) {
        let Some(detail) = detail else {
            return;
        };
        match &mut self.commit {
            None => {
                self.commit = Some(detail.clone());
                self.txn_cnt = 1;
            }
            Some(commit) => {
                merge_commit_details(commit, detail);
                self.txn_cnt += 1;
            }
        }
    }

    /// Go `formatBackoff`: dedupes, sorts, and renders backoff types as
    /// `[a b]`.
    fn format_backoff(buf: &mut String, backoff_types: &[String]) {
        if backoff_types.is_empty() {
            return;
        }
        let mut tp_array: Vec<&String> = Vec::new();
        for tp_str in backoff_types {
            if !tp_array.contains(&tp_str) {
                tp_array.push(tp_str);
            }
        }
        tp_array.sort();
        buf.push('[');
        for (i, tp) in tp_array.iter().enumerate() {
            if i > 0 {
                buf.push(' ');
            }
            buf.push_str(tp);
        }
        buf.push(']');
    }

    /// Go `formatLockKeysDetails`: one `lock_keys`/`shared_lock_keys`
    /// block.
    #[expect(clippy::cast_sign_loss, reason = "Go time.Duration(int64) conversions")]
    fn format_lock_keys_details(
        buf: &mut String,
        label: &str,
        lock_keys: Option<&LockKeysDetails>,
    ) {
        let Some(lock_keys) = lock_keys else {
            return;
        };
        if !buf.is_empty() {
            buf.push_str(", ");
        }
        buf.push_str(label);
        buf.push_str(": {");
        if lock_keys.total_time > StdDuration::ZERO {
            buf.push_str("time:");
            buf.push_str(&format_duration(lock_keys.total_time));
        }
        if lock_keys.region_num > 0 {
            buf.push_str(", region:");
            buf.push_str(&lock_keys.region_num.to_string());
        }
        if lock_keys.lock_keys > 0 {
            buf.push_str(", keys:");
            buf.push_str(&lock_keys.lock_keys.to_string());
        }
        if lock_keys.resolve_lock.resolve_lock_time > 0 {
            buf.push_str(", resolve_lock:");
            buf.push_str(&format_duration(StdDuration::from_nanos(
                lock_keys.resolve_lock.resolve_lock_time as u64,
            )));
        }
        if lock_keys.backoff_time > 0 {
            buf.push_str(", backoff: {time: ");
            buf.push_str(&format_duration(StdDuration::from_nanos(
                lock_keys.backoff_time as u64,
            )));
            if !lock_keys.backoff_types.is_empty() {
                buf.push_str(", type: ");
                Self::format_backoff(buf, &lock_keys.backoff_types);
            }
            buf.push('}');
        }
        if lock_keys.slowest_req_total_time > StdDuration::ZERO {
            buf.push_str(", slowest_rpc: {total: ");
            buf.push_str(&format_seconds_3(lock_keys.slowest_req_total_time));
            buf.push_str("s, region_id: ");
            buf.push_str(&lock_keys.slowest_region.to_string());
            buf.push_str(", store: ");
            buf.push_str(&lock_keys.slowest_store_addr);
            buf.push_str(", ");
            buf.push_str(&lock_keys.slowest_exec_details.to_string());
            buf.push('}');
        }
        if lock_keys.lock_rpc_time > 0 {
            buf.push_str(", lock_rpc:");
            buf.push_str(&format_go_duration(StdDuration::from_nanos(
                lock_keys.lock_rpc_time as u64,
            )));
        }
        if lock_keys.lock_rpc_count > 0 {
            buf.push_str(", rpc_count:");
            buf.push_str(&lock_keys.lock_rpc_count.to_string());
        }
        if lock_keys.retry_count > 0 {
            buf.push_str(", retry_count:");
            buf.push_str(&lock_keys.retry_count.to_string());
        }
        buf.push('}');
    }
}

impl RuntimeStats for RuntimeStatsWithCommit {
    #[expect(clippy::cast_sign_loss, reason = "Go time.Duration(int64) conversions")]
    fn string(&self) -> String {
        let mut buf = String::with_capacity(32);
        if let Some(commit) = &self.commit {
            buf.push_str("commit_txn: {");
            // Only print out when there are more than 1 transaction.
            if self.txn_cnt > 1 {
                buf.push_str("count: ");
                buf.push_str(&self.txn_cnt.to_string());
                buf.push_str(", ");
            }
            if commit.prewrite_time > StdDuration::ZERO {
                buf.push_str("prewrite:");
                buf.push_str(&format_duration(commit.prewrite_time));
            }
            if commit.wait_prewrite_binlog_time > StdDuration::ZERO {
                buf.push_str(", wait_prewrite_binlog:");
                buf.push_str(&format_duration(commit.wait_prewrite_binlog_time));
            }
            if commit.get_commit_ts_time > StdDuration::ZERO {
                buf.push_str(", get_commit_ts:");
                buf.push_str(&format_duration(commit.get_commit_ts_time));
            }
            if commit.commit_time > StdDuration::ZERO {
                buf.push_str(", commit:");
                buf.push_str(&format_duration(commit.commit_time));
            }
            // Go takes commit.Mu here; the guarded fields are flattened.
            let commit_backoff_time = commit.commit_backoff_time;
            if commit_backoff_time > 0 {
                buf.push_str(", backoff: {time: ");
                buf.push_str(&format_duration(StdDuration::from_nanos(
                    commit_backoff_time as u64,
                )));
                if !commit.prewrite_backoff_types.is_empty() {
                    buf.push_str(", prewrite type: ");
                    Self::format_backoff(&mut buf, &commit.prewrite_backoff_types);
                }
                if !commit.commit_backoff_types.is_empty() {
                    buf.push_str(", commit type: ");
                    Self::format_backoff(&mut buf, &commit.commit_backoff_types);
                }
                buf.push('}');
            }
            if commit.slowest_prewrite.req_total_time > StdDuration::ZERO {
                buf.push_str(", slowest_prewrite_rpc: {total: ");
                buf.push_str(&format_seconds_3(commit.slowest_prewrite.req_total_time));
                buf.push_str("s, region_id: ");
                buf.push_str(&commit.slowest_prewrite.region.to_string());
                buf.push_str(", store: ");
                buf.push_str(&commit.slowest_prewrite.store_addr);
                buf.push_str(", ");
                buf.push_str(&commit.slowest_prewrite.exec_details.to_string());
                buf.push('}');
            }
            if commit.commit_primary.req_total_time > StdDuration::ZERO {
                buf.push_str(", commit_primary_rpc: {total: ");
                buf.push_str(&format_seconds_3(commit.commit_primary.req_total_time));
                buf.push_str("s, region_id: ");
                buf.push_str(&commit.commit_primary.region.to_string());
                buf.push_str(", store: ");
                buf.push_str(&commit.commit_primary.store_addr);
                buf.push_str(", ");
                buf.push_str(&commit.commit_primary.exec_details.to_string());
                buf.push('}');
            }
            if commit.resolve_lock.resolve_lock_time > 0 {
                buf.push_str(", resolve_lock: ");
                buf.push_str(&format_duration(StdDuration::from_nanos(
                    commit.resolve_lock.resolve_lock_time as u64,
                )));
            }
            if commit.prewrite_region_num > 0 {
                buf.push_str(", region_num:");
                buf.push_str(&commit.prewrite_region_num.to_string());
            }
            if commit.write_keys > 0 {
                buf.push_str(", write_keys:");
                buf.push_str(&commit.write_keys.to_string());
            }
            if commit.write_size > 0 {
                buf.push_str(", write_byte:");
                buf.push_str(&commit.write_size.to_string());
            }
            if commit.txn_retry > 0 {
                buf.push_str(", txn_retry:");
                buf.push_str(&commit.txn_retry.to_string());
            }
            buf.push('}');
        }
        Self::format_lock_keys_details(&mut buf, "lock_keys", self.lock_keys.as_ref());
        Self::format_lock_keys_details(
            &mut buf,
            "shared_lock_keys",
            self.shared_lock_keys.as_ref(),
        );
        buf
    }

    fn merge(&mut self, other: &dyn RuntimeStats) {
        let Some(tmp) = other.as_any().downcast_ref::<RuntimeStatsWithCommit>() else {
            return;
        };
        self.txn_cnt += tmp.txn_cnt;
        if let Some(src) = &tmp.commit {
            merge_commit_details(self.commit.get_or_insert_with(CommitDetails::default), src);
        }
        if let Some(src) = &tmp.lock_keys {
            merge_lock_keys_details(
                self.lock_keys.get_or_insert_with(LockKeysDetails::default),
                src,
            );
        }
        if let Some(src) = &tmp.shared_lock_keys {
            merge_lock_keys_details(
                self.shared_lock_keys
                    .get_or_insert_with(LockKeysDetails::default),
                src,
            );
        }
    }

    fn clone_box(&self) -> Box<dyn RuntimeStats> {
        Box::new(self.clone())
    }

    fn tp(&self) -> i32 {
        TP_RUNTIME_STATS_WITH_COMMIT
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Go `RUV2Metrics.CalculateRUValues` (`ruv2_metrics.go`), seeded here for
/// [`RuRuntimeStats`]: the weighted TiDB RU, zero under bypass.
#[must_use]
#[expect(clippy::cast_precision_loss, reason = "Go float64(int64) conversions")]
pub fn ruv2_calculate_ru_values(metrics: &RuV2MetricsSnapshot, weights: &RuV2Weights) -> f64 {
    if metrics.bypass {
        return 0.0;
    }
    let sum = |map: &std::collections::BTreeMap<String, i64>| map.values().sum::<i64>();
    let tidb_ru_float = metrics.result_chunk_cells as f64 * weights.result_chunk_cells
        + sum(&metrics.executor_l1) as f64 * weights.executor_l1
        + sum(&metrics.executor_l2) as f64 * weights.executor_l2
        + sum(&metrics.executor_l3) as f64 * weights.executor_l3
        + metrics.executor_l5_insert_rows as f64 * weights.executor_l5_insert_rows
        + metrics.plan_cnt as f64 * weights.plan_cnt
        + metrics.plan_derive_stats_paths as f64 * weights.plan_derive_stats_paths
        + metrics.resource_manager_read_cnt as f64 * weights.resource_manager_read_cnt
        + metrics.resource_manager_write_cnt as f64 * weights.resource_manager_write_cnt
        + metrics.write_keys as f64 * weights.write_keys
        + metrics.session_parser_total as f64 * weights.session_parser_total
        + metrics.txn_cnt as f64 * weights.txn_cnt;
    tidb_ru_float * weights.ru_scale
}

/// Go `RUV2Metrics.TotalRU` (`ruv2_metrics.go`), seeded here for
/// [`RuRuntimeStats`]: TiDB + TiKV + TiFlash RU, with Go's nil-receiver
/// (TiKV + TiFlash only) and bypass (zero) branches.
#[must_use]
pub fn ruv2_total_ru(
    metrics: Option<&RuV2MetricsSnapshot>,
    weights: &RuV2Weights,
    tikv_ru: f64,
    tiflash_ru: f64,
) -> f64 {
    match metrics {
        None => tikv_ru + tiflash_ru,
        Some(metrics) if metrics.bypass => 0.0,
        Some(metrics) => ruv2_calculate_ru_values(metrics, weights) + tikv_ru + tiflash_ru,
    }
}

/// Go `RUV2Metrics.Merge` (`ruv2_metrics.go`) over the snapshot narrowing:
/// no-op when either side bypasses (or `src` is absent), additive counters
/// and label maps otherwise.
pub fn merge_ruv2_metrics_snapshot(
    dst: &mut RuV2MetricsSnapshot,
    src: Option<&RuV2MetricsSnapshot>,
) {
    let Some(src) = src else {
        return;
    };
    if dst.bypass || src.bypass {
        return;
    }
    let merge_map = |dst: &mut std::collections::BTreeMap<String, i64>,
                     src: &std::collections::BTreeMap<String, i64>| {
        for (label, value) in src {
            if *value != 0 {
                *dst.entry(label.clone()).or_insert(0) += value;
            }
        }
    };
    dst.result_chunk_cells += src.result_chunk_cells;
    merge_map(&mut dst.executor_l1, &src.executor_l1);
    merge_map(&mut dst.executor_l2, &src.executor_l2);
    merge_map(&mut dst.executor_l3, &src.executor_l3);
    dst.executor_l5_insert_rows += src.executor_l5_insert_rows;
    dst.plan_cnt += src.plan_cnt;
    dst.plan_derive_stats_paths += src.plan_derive_stats_paths;
    dst.session_parser_total += src.session_parser_total;
    dst.txn_cnt += src.txn_cnt;
    dst.resource_manager_read_cnt += src.resource_manager_read_cnt;
    dst.resource_manager_write_cnt += src.resource_manager_write_cnt;
    dst.write_keys += src.write_keys;
    dst.write_size += src.write_size;
    dst.tikv_kv_engine_cache_miss += src.tikv_kv_engine_cache_miss;
    dst.tikv_coprocessor_executor_iterations += src.tikv_coprocessor_executor_iterations;
    dst.tikv_coprocessor_response_bytes += src.tikv_coprocessor_response_bytes;
    dst.tikv_raftstore_store_write_trigger_wb += src.tikv_raftstore_store_write_trigger_wb;
    dst.tikv_storage_processed_keys_batch_get += src.tikv_storage_processed_keys_batch_get;
    dst.tikv_storage_processed_keys_get += src.tikv_storage_processed_keys_get;
    merge_map(
        &mut dst.tikv_coprocessor_executor_work_total,
        &src.tikv_coprocessor_executor_work_total,
    );
}

/// client-go `RUDetails.Merge`, recovered as the additive merge over the
/// snapshot narrowing.
pub fn merge_ru_details(dst: &mut RuDetailsSnapshot, src: &RuDetailsSnapshot) {
    dst.rru += src.rru;
    dst.wru += src.wru;
    dst.ru_wait_duration += src.ru_wait_duration;
    dst.tikv_ru_v2 += src.tikv_ru_v2;
    dst.tiflash_ru += src.tiflash_ru;
}

/// Go `RURuntimeStats`: RU details and statement-level RU v2 metrics for
/// EXPLAIN output. `ru_version` selects the accounting version — `1` (v1)
/// shows RRU + WRU, `2` (v2) shows the v2 total, `0`/unknown defaults to
/// v1.
#[derive(Clone, Debug, Default)]
pub struct RuRuntimeStats {
    /// Go's embedded `*util.RUDetails`, as the snapshot narrowing.
    pub ru_details: Option<RuDetailsSnapshot>,
    /// Go `RURuntimeStats.Metrics`.
    pub metrics: Option<RuV2MetricsSnapshot>,
    /// Go `RURuntimeStats.Weights`.
    pub weights: RuV2Weights,
    /// Go `RURuntimeStats.RUVersion` (`rmclient.RUVersion`, narrowed to a
    /// plain integer).
    pub ru_version: i64,
}

impl RuRuntimeStats {
    /// Go `RURuntimeStats.Clone`'s nil-receiver seam: cloning a nil
    /// receiver yields the zero value.
    #[must_use]
    pub fn clone_nullable(stats: Option<&RuRuntimeStats>) -> RuRuntimeStats {
        match stats {
            None => RuRuntimeStats::default(),
            Some(stats) => stats.clone(),
        }
    }
}

impl RuntimeStats for RuRuntimeStats {
    fn string(&self) -> String {
        if self.ru_version == RU_VERSION_V2 {
            let (tikv_ru, tiflash_ru) = match &self.ru_details {
                Some(details) => (details.tikv_ru_v2, details.tiflash_ru),
                None => (0.0, 0.0),
            };
            let total_ru = ruv2_total_ru(self.metrics.as_ref(), &self.weights, tikv_ru, tiflash_ru);
            if total_ru == 0.0 {
                return String::new();
            }
            return format!("RU:{total_ru:.2}");
        }
        // v1 or unknown.
        match &self.ru_details {
            Some(details) => format!("RU:{:.2}", details.rru + details.wru),
            None => String::new(),
        }
    }

    fn merge(&mut self, other: &dyn RuntimeStats) {
        let Some(tmp) = other.as_any().downcast_ref::<RuRuntimeStats>() else {
            return;
        };
        match (&mut self.ru_details, &tmp.ru_details) {
            (Some(dst), Some(src)) => merge_ru_details(dst, src),
            (dst @ None, Some(src)) => *dst = Some(*src),
            _ => {}
        }
        match &mut self.metrics {
            Some(metrics) => merge_ruv2_metrics_snapshot(metrics, tmp.metrics.as_ref()),
            None => self.metrics = tmp.metrics.clone(),
        }
        if self.weights == RuV2Weights::default() {
            self.weights = tmp.weights;
        }
        if self.ru_version == 0 {
            self.ru_version = tmp.ru_version;
        }
    }

    fn clone_box(&self) -> Box<dyn RuntimeStats> {
        Box::new(self.clone())
    }

    fn tp(&self) -> i32 {
        TP_RU_RUNTIME_STATS
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec_details::{ReqDetailInfo, ResolveLockDetail, TiKVExecDetails, WriteDetail};

    /// Go `mockExecutorExecutionSummary`.
    fn mock_executor_execution_summary(
        time_processed_ns: u64,
        num_produced_rows: u64,
        num_iterations: u64,
    ) -> ExecutorExecutionSummarySnapshot {
        ExecutorExecutionSummarySnapshot {
            time_processed_ns,
            num_produced_rows,
            num_iterations,
            ..ExecutorExecutionSummarySnapshot::default()
        }
    }

    /// Go `defaultRUV2WeightsForTest`, seeded with the values of
    /// `config.DefaultRUV2Config()` (`pkg/config/config.go`).
    fn default_ruv2_weights_for_test() -> RuV2Weights {
        RuV2Weights {
            ru_scale: 2.01,
            result_chunk_cells: 0.000_1,
            executor_l1: 0.000_132_78,
            executor_l2: 0.000_003_83,
            executor_l3: 0.001_417_39,
            executor_l5_insert_rows: 0.004_725_72,
            plan_cnt: 0.153_922_17,
            plan_derive_stats_paths: 0.249_681_82,
            resource_manager_read_cnt: 0.020_720_03,
            resource_manager_write_cnt: 0.071_797_79,
            write_keys: 0.330_760_861_554_226,
            session_parser_total: 0.192_304_99,
            txn_cnt: 0.030_137_09,
        }
    }

    /// Port of Go `TestCopRuntimeStats` (`execdetails_test.go`), expected
    /// strings byte for byte.
    #[test]
    fn cop_runtime_stats() {
        let mut stats = RuntimeStatsColl::new(None);
        let table_scan_id = 1;
        let agg_id = 2;
        let table_reader_id = 3;
        stats.record_one_cop_task(
            table_scan_id,
            StoreType::TiKv,
            &mock_executor_execution_summary(1, 1, 1),
        );
        stats.record_one_cop_task(
            table_scan_id,
            StoreType::TiKv,
            &mock_executor_execution_summary(2, 2, 2),
        );
        stats.record_one_cop_task(
            agg_id,
            StoreType::TiKv,
            &mock_executor_execution_summary(3, 3, 3),
        );
        stats.record_one_cop_task(
            agg_id,
            StoreType::TiKv,
            &mock_executor_execution_summary(4, 4, 4),
        );
        let scan_detail = CopScanDetail {
            base: ScanDetail {
                total_keys: 15,
                processed_keys: 10,
                rocksdb_delete_skipped_count: 5,
                rocksdb_key_skipped_count: 1,
                rocksdb_block_cache_hit_count: 10,
                rocksdb_block_read_count: 20,
                rocksdb_block_read_byte: 100,
                ..ScanDetail::default()
            },
            processed_keys_size: 10,
        };
        stats.record_cop_stats(
            table_scan_id,
            StoreType::TiKv,
            Some(&scan_detail),
            CopTimeDetail::default(),
            None,
        );
        assert!(stats.exists_cop_stats(table_scan_id));

        let cop = stats.get_cop_stats(table_scan_id).unwrap();
        let expected = "tikv_task:{proc max:2ns, min:1ns, avg: 1ns, p80:2ns, p95:2ns, iters:3, tasks:2}, \
            scan_detail: {total_process_keys: 10, total_process_keys_size: 10, total_keys: 15, rocksdb: {delete_skipped_count: 5, key_skipped_count: 1, block: {cache_hit_count: 10, read_count: 20, read_byte: 100 Bytes}}}";
        assert_eq!(expected, cop.string());

        assert_eq!("time:3ns, loops:3", cop.stats.string());
        assert_eq!(
            "tikv_task:{proc max:4ns, min:3ns, avg: 3ns, p80:4ns, p95:4ns, iters:7, tasks:2}",
            stats.get_cop_stats(agg_id).unwrap().string()
        );

        let root_stats = stats.get_root_stats(table_reader_id);
        let _ = root_stats;
        assert!(stats.exists_root_stats(table_reader_id));

        let cop = stats.get_cop_stats(table_scan_id).unwrap();
        cop.scan_detail.base.processed_keys = 0;
        cop.scan_detail.processed_keys_size = 0;
        cop.scan_detail.base.rocksdb_key_skipped_count = 0;
        cop.scan_detail.base.rocksdb_block_read_count = 0;
        // Print all fields even though the value of some fields is 0.
        let s = "tikv_task:{proc max:2ns, min:1ns, avg: 1ns, p80:2ns, p95:2ns, iters:3, tasks:2}, scan_detail: {total_keys: 15, rocksdb: {delete_skipped_count: 5, block: {cache_hit_count: 10, read_byte: 100 Bytes}}}";
        assert_eq!(s, cop.string());
        let zero_scan_detail = CopScanDetail::default();
        let zero_cop_stats = CopRuntimeStats::default();
        assert_eq!("", zero_scan_detail.string());
        assert_eq!("", CopTimeDetail::default().string());
        assert_eq!("", zero_cop_stats.string());
    }

    /// Port of Go `TestCopRuntimeStats2` (`execdetails_test.go`): the
    /// digest path past `MaxDetailsNumsForOneQuery`, expected string byte
    /// for byte.
    #[test]
    fn cop_runtime_stats2() {
        let mut stats = RuntimeStatsColl::new(None);
        let table_scan_id = 1;
        let scan_detail = CopScanDetail {
            base: ScanDetail {
                total_keys: 15,
                processed_keys: 10,
                rocksdb_delete_skipped_count: 5,
                rocksdb_key_skipped_count: 1,
                rocksdb_block_cache_hit_count: 10,
                rocksdb_block_read_count: 20,
                rocksdb_block_read_byte: 100,
                ..ScanDetail::default()
            },
            processed_keys_size: 10,
        };
        let time_detail = CopTimeDetail {
            base: TimeDetail {
                process_time: StdDuration::from_millis(10),
                wait_time: StdDuration::from_millis(30),
                total_rpc_wall_time: StdDuration::from_millis(50),
            },
            suspend_time: StdDuration::from_millis(20),
            kv_read_wall_time: StdDuration::from_millis(5),
        };
        stats.record_cop_stats(
            table_scan_id,
            StoreType::TiKv,
            Some(&scan_detail),
            CopTimeDetail::default(),
            None,
        );
        for _ in 0..1005 {
            stats.record_cop_stats(
                table_scan_id,
                StoreType::TiKv,
                Some(&scan_detail),
                time_detail.clone(),
                Some(&mock_executor_execution_summary(2, 2, 2)),
            );
        }

        let cop = stats.get_cop_stats(table_scan_id).unwrap();
        let expected = "tikv_task:{proc max:2ns, min:2ns, avg: 2ns, p80:2ns, p95:2ns, iters:2010, tasks:1005}, \
            scan_detail: {total_process_keys: 10060, total_process_keys_size: 10060, total_keys: 15090, \
            rocksdb: {delete_skipped_count: 5030, key_skipped_count: 1006, \
            block: {cache_hit_count: 10060, read_count: 20120, read_byte: 98.2 KB}}}, \
            time_detail: {total_process_time: 10.1s, total_suspend_time: 20.1s, total_wait_time: 30.2s, \
            total_kv_read_wall_time: 5.03s, tikv_wall_time: 50.3s}";
        assert_eq!(expected, cop.string());
        assert_eq!(expected, cop.string());
    }

    /// Port of Go `TestRuntimeStatsWithCommit` (`execdetails_test.go`).
    ///
    /// Divergence from the Go literal, in the commit fixture only: the
    /// reused [`crate::exec_details::TimeDetail`]/[`WriteDetail`] carry
    /// neither `KvGrpcWaitTime`/`KvGrpcProcessTime` nor the scheduler
    /// `latch_wait`/`pessimistic_lock_wait`/`throttle` durations, so those
    /// fixture fields are dropped and the expected literal reads
    /// `time_detail: {tikv_wall_time: 500ms}` (Go:
    /// `{tikv_grpc_process_time: 200ms, tikv_grpc_wait_time: 100ms,
    /// tikv_wall_time: 500ms}`) and `scheduler: {process: 104µs}` (Go:
    /// `{process: 104µs, latch_wait: 103µs, pessimistic_lock_wait: 106µs,
    /// throttle: 105µs}`). Everything else, including the whole lock-keys
    /// half, is byte for byte.
    #[test]
    fn runtime_stats_with_commit() {
        let commit_detail = CommitDetails {
            get_commit_ts_time: StdDuration::from_secs(1),
            prewrite_time: StdDuration::from_secs(1),
            commit_time: StdDuration::from_secs(1),
            commit_backoff_time: 1_000_000_000,
            prewrite_backoff_types: vec![
                "backoff1".to_owned(),
                "backoff2".to_owned(),
                "backoff1".to_owned(),
            ],
            commit_backoff_types: vec![],
            slowest_prewrite: ReqDetailInfo {
                req_total_time: StdDuration::from_secs(1),
                region: 1000,
                store_addr: "tikv-1:20160".to_owned(),
                exec_details: TiKVExecDetails {
                    time_detail: Some(TimeDetail {
                        total_rpc_wall_time: StdDuration::from_millis(500),
                        ..TimeDetail::default()
                    }),
                    scan_detail: Some(ScanDetail {
                        processed_keys: 10,
                        total_keys: 100,
                        rocksdb_delete_skipped_count: 1,
                        rocksdb_key_skipped_count: 1,
                        rocksdb_block_cache_hit_count: 1,
                        rocksdb_block_read_count: 1,
                        rocksdb_block_read_byte: 100,
                        rocksdb_block_read_duration: StdDuration::from_millis(20),
                        ..ScanDetail::default()
                    }),
                    write_detail: Some(WriteDetail {
                        store_batch_wait_duration: StdDuration::from_micros(10),
                        propose_send_wait_duration: StdDuration::from_micros(20),
                        persist_log_duration: StdDuration::from_micros(30),
                        raft_db_write_leader_wait_duration: StdDuration::from_micros(40),
                        raft_db_sync_log_duration: StdDuration::from_micros(45),
                        raft_db_write_memtable_duration: StdDuration::from_micros(50),
                        commit_log_duration: StdDuration::from_micros(60),
                        apply_batch_wait_duration: StdDuration::from_micros(70),
                        apply_log_duration: StdDuration::from_micros(80),
                        apply_mutex_lock_duration: StdDuration::from_micros(90),
                        apply_write_leader_wait_duration: StdDuration::from_micros(100),
                        apply_write_wal_duration: StdDuration::from_micros(101),
                        apply_write_memtable_duration: StdDuration::from_micros(102),
                        scheduler_process_duration: StdDuration::from_micros(104),
                    }),
                },
            },
            commit_primary: ReqDetailInfo::default(),
            write_keys: 3,
            write_size: 66,
            prewrite_region_num: 5,
            txn_retry: 2,
            resolve_lock: ResolveLockDetail {
                resolve_lock_time: 1_000_000_000,
            },
            ..CommitDetails::default()
        };
        let stats = RuntimeStatsWithCommit {
            commit: Some(commit_detail),
            ..RuntimeStatsWithCommit::default()
        };
        let expect = "commit_txn: {prewrite:1s, get_commit_ts:1s, commit:1s, backoff: {time: 1s, prewrite type: [backoff1 backoff2]}, \
            slowest_prewrite_rpc: {total: 1.000s, region_id: 1000, store: tikv-1:20160, \
            time_detail: {tikv_wall_time: 500ms}, \
            scan_detail: {total_process_keys: 10, total_keys: 100, rocksdb: {delete_skipped_count: 1, key_skipped_count: 1, \
            block: {cache_hit_count: 1, read_count: 1, read_byte: 100 Bytes, read_time: 20ms}}}, \
            write_detail: {store_batch_wait: 10µs, propose_send_wait: 20µs, persist_log: {total: 30µs, write_leader_wait: 40µs, \
            sync_log: 45µs, write_memtable: 50µs}, commit_log: 60µs, apply_batch_wait: 70µs, apply: {total:80µs, mutex_lock: 90µs, \
            write_leader_wait: 100µs, write_wal: 101µs, write_memtable: 102µs}, scheduler: {process: 104µs}}}, resolve_lock: 1s, region_num:5, write_keys:3\
            , write_byte:66, txn_retry:2}";
        assert_eq!(expect, stats.string());

        let lock_detail = LockKeysDetails {
            total_time: StdDuration::from_secs(1),
            region_num: 2,
            lock_keys: 10,
            backoff_time: 3_000_000_000,
            backoff_types: vec![
                "backoff4".to_owned(),
                "backoff5".to_owned(),
                "backoff5".to_owned(),
            ],
            slowest_req_total_time: StdDuration::from_secs(1),
            slowest_region: 1000,
            slowest_store_addr: "tikv-1:20160".to_owned(),
            slowest_exec_details: TiKVExecDetails {
                time_detail: Some(TimeDetail {
                    total_rpc_wall_time: StdDuration::from_millis(500),
                    ..TimeDetail::default()
                }),
                scan_detail: Some(ScanDetail {
                    processed_keys: 10,
                    total_keys: 100,
                    rocksdb_delete_skipped_count: 1,
                    rocksdb_key_skipped_count: 1,
                    rocksdb_block_cache_hit_count: 1,
                    rocksdb_block_read_count: 1,
                    rocksdb_block_read_byte: 100,
                    rocksdb_block_read_duration: StdDuration::from_millis(20),
                    ..ScanDetail::default()
                }),
                write_detail: Some(WriteDetail {
                    store_batch_wait_duration: StdDuration::from_micros(10),
                    propose_send_wait_duration: StdDuration::from_micros(20),
                    persist_log_duration: StdDuration::from_micros(30),
                    raft_db_write_leader_wait_duration: StdDuration::from_micros(40),
                    raft_db_sync_log_duration: StdDuration::from_micros(45),
                    raft_db_write_memtable_duration: StdDuration::from_micros(50),
                    commit_log_duration: StdDuration::from_micros(60),
                    apply_batch_wait_duration: StdDuration::from_micros(70),
                    apply_log_duration: StdDuration::from_micros(80),
                    apply_mutex_lock_duration: StdDuration::from_micros(90),
                    apply_write_leader_wait_duration: StdDuration::from_micros(100),
                    apply_write_wal_duration: StdDuration::from_micros(101),
                    apply_write_memtable_duration: StdDuration::from_micros(102),
                    scheduler_process_duration: StdDuration::ZERO,
                }),
            },
            lock_rpc_time: 5_000_000_000,
            lock_rpc_count: 50,
            retry_count: 2,
            resolve_lock: ResolveLockDetail {
                resolve_lock_time: 2_000_000_000,
            },
        };
        let mut stats = RuntimeStatsWithCommit {
            lock_keys: Some(lock_detail.clone()),
            ..RuntimeStatsWithCommit::default()
        };
        let expect = "lock_keys: {time:1s, region:2, keys:10, resolve_lock:2s, backoff: {time: 3s, type: [backoff4 backoff5]}, \
            slowest_rpc: {total: 1.000s, region_id: 1000, store: tikv-1:20160, time_detail: {tikv_wall_time: 500ms}, scan_detail: \
            {total_process_keys: 10, total_keys: 100, rocksdb: {delete_skipped_count: 1, key_skipped_count: 1, block: \
            {cache_hit_count: 1, read_count: 1, read_byte: 100 Bytes, read_time: 20ms}}}, write_detail: \
            {store_batch_wait: 10µs, propose_send_wait: 20µs, persist_log: {total: 30µs, write_leader_wait: 40µs, sync_log: 45µs, write_memtable: 50µs}, \
            commit_log: 60µs, apply_batch_wait: 70µs, apply: {total:80µs, mutex_lock: 90µs, write_leader_wait: 100µs, write_wal: 101µs, write_memtable: 102µs}, \
            scheduler: {process: 0s}}}, lock_rpc:5s, rpc_count:50, retry_count:2}";
        assert_eq!(expect, stats.string());

        stats.shared_lock_keys = Some(lock_detail);
        assert_eq!(format!("{expect}, shared_{expect}"), stats.string());

        // Test Clone with SharedLockKeys.
        let cloned = stats.clone_box();
        let cloned_stats = cloned
            .as_any()
            .downcast_ref::<RuntimeStatsWithCommit>()
            .unwrap();
        assert_eq!(stats.string(), cloned_stats.string());
        assert!(cloned_stats.shared_lock_keys.is_some());
        assert_eq!(
            stats.shared_lock_keys.as_ref().unwrap().lock_keys,
            cloned_stats.shared_lock_keys.as_ref().unwrap().lock_keys
        );

        // Test Merge with SharedLockKeys.
        let stats2 = RuntimeStatsWithCommit {
            shared_lock_keys: Some(LockKeysDetails {
                total_time: StdDuration::from_secs(1),
                region_num: 3,
                lock_keys: 5,
                ..LockKeysDetails::default()
            }),
            ..RuntimeStatsWithCommit::default()
        };
        stats.merge(&stats2);
        assert_eq!(5, stats.shared_lock_keys.as_ref().unwrap().region_num);
        assert_eq!(15, stats.shared_lock_keys.as_ref().unwrap().lock_keys);

        // Test Merge into empty SharedLockKeys.
        let mut stats3 = RuntimeStatsWithCommit::default();
        stats3.merge(&stats2);
        assert!(stats3.shared_lock_keys.is_some());
        assert_eq!(3, stats3.shared_lock_keys.as_ref().unwrap().region_num);
        assert_eq!(5, stats3.shared_lock_keys.as_ref().unwrap().lock_keys);
    }

    /// Port of Go `TestRootRuntimeStats` (`execdetails_test.go`), expected
    /// string byte for byte.
    #[test]
    fn root_runtime_stats() {
        let pid = 1;
        let mut stmt_stats = RuntimeStatsColl::new(None);
        let basic1 = stmt_stats.get_basic_runtime_stats(pid, true).unwrap();
        let basic2 = stmt_stats.get_basic_runtime_stats(pid, true).unwrap();
        basic1.record_open(StdDuration::from_millis(10));
        basic1.record(StdDuration::from_secs(1), 20);
        basic2.record(StdDuration::from_secs(2), 30);
        basic2.record_close(StdDuration::from_millis(100));
        let mut concurrency = RuntimeStatsWithConcurrencyInfo::default();
        concurrency.set_concurrency_info(vec![ConcurrencyInfo::new("worker", 15)]);
        let commit_detail = CommitDetails {
            get_commit_ts_time: StdDuration::from_secs(1),
            prewrite_time: StdDuration::from_secs(1),
            commit_time: StdDuration::from_secs(1),
            write_keys: 3,
            write_size: 66,
            prewrite_region_num: 5,
            txn_retry: 2,
            ..CommitDetails::default()
        };
        stmt_stats.register_stats(pid, Box::new(concurrency));
        stmt_stats.register_stats(
            pid,
            Box::new(RuntimeStatsWithCommit {
                commit: Some(commit_detail),
                ..RuntimeStatsWithCommit::default()
            }),
        );
        let stats = stmt_stats.get_root_stats(1);
        let expect = "total_time:3.11s, total_open:10ms, total_close:100ms, loops:2, worker:15, commit_txn: {prewrite:1s, get_commit_ts:1s, commit:1s, region_num:5, write_keys:3, write_byte:66, txn_retry:2}";
        assert_eq!(expect, stats.string());
    }

    /// Port of Go `TestFormatDurationForExplain` (`execdetails_test.go`):
    /// every case, with the Go `time.ParseDuration` inputs precomputed to
    /// nanoseconds.
    #[test]
    fn format_duration_for_explain() {
        let cases: &[(u64, &str)] = &[
            (0, "0s"),                           // 0s
            (1, "1ns"),                          // 1ns
            (9, "9ns"),                          // 9ns
            (10, "10ns"),                        // 10ns
            (999, "999ns"),                      // 999ns
            (1_000, "1µs"),                      // 1µs
            (1_123, "1.12µs"),                   // 1.123µs
            (1_023, "1.02µs"),                   // 1.023µs
            (1_003, "1µs"),                      // 1.003µs
            (10_456, "10.5µs"),                  // 10.456µs
            (10_956, "11µs"),                    // 10.956µs
            (999_056, "999.1µs"),                // 999.056µs
            (999_988, "1ms"),                    // 999.988µs
            (1_123_000, "1.12ms"),               // 1.123ms
            (1_023_000, "1.02ms"),               // 1.023ms
            (1_003_000, "1ms"),                  // 1.003ms
            (10_456_000, "10.5ms"),              // 10.456ms
            (10_956_000, "11ms"),                // 10.956ms
            (999_056_000, "999.1ms"),            // 999.056ms
            (999_988_000, "1s"),                 // 999.988ms
            (1_123_000_000, "1.12s"),            // 1.123s
            (1_023_000_000, "1.02s"),            // 1.023s
            (1_003_000_000, "1s"),               // 1.003s
            (10_456_000_000, "10.5s"),           // 10.456s
            (10_956_000_000, "11s"),             // 10.956s
            (999_056_000_000, "16m39.1s"),       // 16m39.056s
            (999_988_000_000, "16m40s"),         // 16m39.988s
            (87_399_388_662_000, "24h16m39.4s"), // 24h16m39.388662s
            (9_412_345, "9.41ms"),               // 9.412345ms
            (10_412_345, "10.4ms"),              // 10.412345ms
            (5_999_000_000, "6s"),               // 5.999s
            (100_450, "100.5µs"),                // 100.45µs
        ];
        for (ns, expected) in cases {
            assert_eq!(
                *expected,
                format_duration(StdDuration::from_nanos(*ns)),
                "input: {ns}ns"
            );
        }
    }

    /// Port of Go `TestRURuntimeStatsStringV1` (`execdetails_test.go`).
    #[test]
    fn ru_runtime_stats_string_v1() {
        let stats = RuRuntimeStats {
            ru_details: Some(RuDetailsSnapshot {
                rru: 10.5,
                wru: 20.3,
                ..RuDetailsSnapshot::default()
            }),
            metrics: Some(RuV2MetricsSnapshot::default()),
            weights: default_ruv2_weights_for_test(),
            ru_version: RU_VERSION_V1,
        };
        // v1: shows RRU + WRU.
        assert_eq!("RU:30.80", stats.string());
    }

    /// Port of Go `TestRURuntimeStatsStringV1NilDetails`.
    #[test]
    fn ru_runtime_stats_string_v1_nil_details() {
        let stats = RuRuntimeStats {
            metrics: Some(RuV2MetricsSnapshot::default()),
            weights: default_ruv2_weights_for_test(),
            ru_version: RU_VERSION_V1,
            ..RuRuntimeStats::default()
        };
        // v1 with nil RUDetails returns empty.
        assert_eq!("", stats.string());
    }

    /// Port of Go `TestRURuntimeStatsStringV2`. Go builds the details with
    /// `AddTiKVRUV2(200)` and `UpdateTiFlash(&Consumption{RRU: 100, WRU:
    /// 200})`; the snapshot narrowing carries the resulting accessor values
    /// (`TiKVRUV2() == 200`, `TiflashRU() == 300`).
    #[test]
    fn ru_runtime_stats_string_v2() {
        let stats = RuRuntimeStats {
            ru_details: Some(RuDetailsSnapshot {
                tikv_ru_v2: 200.0,
                tiflash_ru: 300.0,
                ..RuDetailsSnapshot::default()
            }),
            metrics: Some(RuV2MetricsSnapshot::default()),
            weights: default_ruv2_weights_for_test(),
            ru_version: RU_VERSION_V2,
        };
        // v2: shows total RU from v2 metrics (tikvRU + tiflashRU + tidbRU).
        assert_eq!("RU:500.00", stats.string());
    }

    /// Port of Go `TestRURuntimeStatsStringV2ZeroRU`.
    #[test]
    fn ru_runtime_stats_string_v2_zero_ru() {
        let stats = RuRuntimeStats {
            metrics: Some(RuV2MetricsSnapshot::default()),
            weights: default_ruv2_weights_for_test(),
            ru_version: RU_VERSION_V2,
            ..RuRuntimeStats::default()
        };
        // v2 with zero total RU returns empty.
        assert_eq!("", stats.string());
    }

    /// Port of Go `TestRURuntimeStatsStringDefaultVersion`.
    #[test]
    fn ru_runtime_stats_string_default_version() {
        // RUVersion=0 (zero value) should default to v1 for backward
        // compatibility.
        let stats = RuRuntimeStats {
            ru_details: Some(RuDetailsSnapshot {
                rru: 10.5,
                wru: 20.3,
                ..RuDetailsSnapshot::default()
            }),
            metrics: Some(RuV2MetricsSnapshot::default()),
            weights: default_ruv2_weights_for_test(),
            ..RuRuntimeStats::default()
        };
        // default (v1): shows RRU + WRU.
        assert_eq!("RU:30.80", stats.string());
    }

    /// Port of Go `TestRURuntimeStatsClonePreservesRUVersion`.
    #[test]
    fn ru_runtime_stats_clone_preserves_ru_version() {
        let stats = RuRuntimeStats {
            ru_details: Some(RuDetailsSnapshot {
                rru: 10.0,
                wru: 20.0,
                ..RuDetailsSnapshot::default()
            }),
            metrics: Some(RuV2MetricsSnapshot::default()),
            weights: default_ruv2_weights_for_test(),
            ru_version: RU_VERSION_V1,
        };
        let cloned = stats.clone_box();
        let cloned = cloned.as_any().downcast_ref::<RuRuntimeStats>().unwrap();
        assert_eq!(RU_VERSION_V1, cloned.ru_version);
        // Verify the clone produces the same output.
        assert_eq!(stats.string(), cloned.string());
    }

    /// Port of Go `TestRURuntimeStatsCloneNilPreservesZeroVersion`.
    #[test]
    fn ru_runtime_stats_clone_nil_preserves_zero_version() {
        let cloned = RuRuntimeStats::clone_nullable(None);
        assert_eq!(0, cloned.ru_version);
    }

    /// Port of Go `TestRURuntimeStatsMergeRUVersion`.
    #[test]
    fn ru_runtime_stats_merge_ru_version() {
        // Merge takes RUVersion from other when receiver has zero value.
        let mut dst = RuRuntimeStats {
            metrics: Some(RuV2MetricsSnapshot::default()),
            weights: default_ruv2_weights_for_test(),
            ..RuRuntimeStats::default()
        };
        let src = RuRuntimeStats {
            metrics: Some(RuV2MetricsSnapshot::default()),
            weights: default_ruv2_weights_for_test(),
            ru_version: RU_VERSION_V2,
            ..RuRuntimeStats::default()
        };
        dst.merge(&src);
        assert_eq!(RU_VERSION_V2, dst.ru_version);
    }

    /// Port of Go `TestRURuntimeStatsMergeKeepsExistingRUVersion`.
    #[test]
    fn ru_runtime_stats_merge_keeps_existing_ru_version() {
        // Merge does NOT override a non-zero RUVersion.
        let mut dst = RuRuntimeStats {
            metrics: Some(RuV2MetricsSnapshot::default()),
            weights: default_ruv2_weights_for_test(),
            ru_version: RU_VERSION_V1,
            ..RuRuntimeStats::default()
        };
        let src = RuRuntimeStats {
            metrics: Some(RuV2MetricsSnapshot::default()),
            weights: default_ruv2_weights_for_test(),
            ru_version: RU_VERSION_V2,
            ..RuRuntimeStats::default()
        };
        dst.merge(&src);
        assert_eq!(RU_VERSION_V1, dst.ru_version);
    }

    /// Port of Go `TestRURuntimeStatsStringIncludesTiFlashRU`. Go builds
    /// the details with `AddTiKVRUV2(200)` and
    /// `UpdateTiFlash(&Consumption{RRU: 100, WRU: 200})`; the snapshot
    /// narrowing carries the resulting accessor values.
    #[test]
    fn ru_runtime_stats_string_includes_tiflash_ru() {
        let stats = RuRuntimeStats {
            ru_details: Some(RuDetailsSnapshot {
                tikv_ru_v2: 200.0,
                tiflash_ru: 300.0,
                ..RuDetailsSnapshot::default()
            }),
            metrics: Some(RuV2MetricsSnapshot::default()),
            weights: default_ruv2_weights_for_test(),
            ru_version: RU_VERSION_V2,
        };
        assert_eq!("RU:500.00", stats.string());
    }
}
