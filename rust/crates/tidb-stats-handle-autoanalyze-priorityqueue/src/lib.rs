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

//! Go `pkg/statistics/handle/autoanalyze/priorityqueue`.

use chrono::{DateTime, FixedOffset, Timelike, Utc};
use serde::{ser::SerializeMap, Serialize, Serializer};
use serde_json::value::RawValue;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tidb_model::{SchemaState, TableInfo};
use tidb_stats::Table;

/// Go `EventNone`.
pub const EVENT_NONE: f64 = 0.0;
/// Go `EventNewIndex`.
pub const EVENT_NEW_INDEX: f64 = 2.0;
/// Go `NoRecord` represented as a duration sentinel.
pub const NO_RECORD: i64 = -1;
/// Go `defaultFailedAnalysisWaitTime`.
pub const DEFAULT_FAILED_ANALYSIS_WAIT_TIME: i64 = 30 * 60 * 1_000_000_000;
/// Go `justFailed`.
pub const JUST_FAILED: i64 = 0;

const CHANGE_RATIO_WEIGHT: f64 = 0.6;
const SIZE_WEIGHT: f64 = 0.1;
const ANALYSIS_INTERVAL_WEIGHT: f64 = 0.3;
const UNANALYZED_LAST_ANALYSIS_DURATION: i64 = 30 * 60 * 1_000_000_000;
const LAST_ANALYSIS_DURATION_REFRESH_INTERVAL: Duration = Duration::from_secs(10 * 60);
const DML_CHANGES_FETCH_INTERVAL: Duration = Duration::from_secs(2 * 60);
const MUST_RETRY_JOB_REQUEUE_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Go `Indicators`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Indicators {
    /// Modified rows divided by the last analyzed count.
    pub change_percentage: f64,
    /// Realtime rows multiplied by the number of columns.
    pub table_size: f64,
    /// Time since the last successful analysis.
    pub last_analysis_duration: i64,
}

/// Go `IndicatorsJSON`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IndicatorsJson {
    /// Percentage formatted with two fractional digits.
    pub change_percentage: String,
    /// Table size formatted with two fractional digits.
    pub table_size: String,
    /// Go-duration rendering.
    pub last_analysis_duration: String,
}

/// Go `AnalysisJobJSON`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AnalysisJobJson {
    /// Concrete analysis type.
    #[serde(rename = "type")]
    pub kind: String,
    /// Logical or physical table identity used by the queue.
    pub table_id: i64,
    /// Calculated priority.
    #[serde(serialize_with = "serialize_go_json_f64")]
    pub weight: f64,
    /// Dynamic partition targets.
    pub partition_ids: Vec<i64>,
    /// Ordinary/static newly added index targets.
    pub index_ids: Vec<i64>,
    /// Dynamic newly added index targets.
    #[serde(serialize_with = "serialize_partition_index_ids")]
    pub partition_index_ids: HashMap<i64, Vec<i64>>,
    /// Ranking inputs.
    pub indicators: IndicatorsJson,
    /// Whether index creation supplies the special-event weight.
    pub has_newly_added_index: bool,
}

fn serialize_partition_index_ids<S>(
    values: &HashMap<i64, Vec<i64>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut entries = values
        .iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let mut map = serializer.serialize_map(Some(entries.len()))?;
    for (key, value) in entries {
        map.serialize_entry(&key, value)?;
    }
    map.end()
}

fn serialize_go_json_f64<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if !value.is_finite() {
        return Err(serde::ser::Error::custom(format!(
            "json: unsupported value: {}",
            if value.is_nan() {
                "NaN"
            } else if value.is_sign_positive() {
                "+Inf"
            } else {
                "-Inf"
            }
        )));
    }
    let absolute = value.abs();
    let encoded = if absolute != 0.0 && !(1e-6..1e21).contains(&absolute) {
        let scientific = format!("{value:e}");
        let (mantissa, exponent) = scientific
            .split_once('e')
            .expect("Rust lower-exponential formatting always has an exponent");
        let exponent = exponent
            .parse::<i32>()
            .expect("Rust lower-exponential formatting has an integer exponent");
        format!("{mantissa}e{exponent:+}")
    } else {
        value.to_string()
    };
    RawValue::from_string(encoded)
        .map_err(serde::ser::Error::custom)?
        .serialize(serializer)
}

/// Rust ownership form of Go's three concrete `AnalysisJob` implementations.
#[derive(Clone, Debug)]
pub enum AnalysisJob {
    /// Go `NonPartitionedTableAnalysisJob`.
    NonPartitioned(NonPartitionedTableAnalysisJob),
    /// Go `StaticPartitionedTableAnalysisJob`.
    StaticPartitioned(StaticPartitionedTableAnalysisJob),
    /// Go `DynamicPartitionedTableAnalysisJob`.
    DynamicPartitioned(DynamicPartitionedTableAnalysisJob),
}

/// Go `NonPartitionedTableAnalysisJob`.
#[derive(Clone, Debug, Default)]
pub struct NonPartitionedTableAnalysisJob {
    /// Physical table ID.
    pub table_id: i64,
    /// Missing public indexes.
    pub index_ids: HashSet<i64>,
    /// Ranking inputs.
    pub indicators: Indicators,
    /// Requested statistics version.
    pub table_stats_version: i32,
    /// Whether a v1-to-v2 rewrite warning is required.
    pub need_version_rewrite_warning: bool,
    /// Priority score.
    pub weight: f64,
    /// Lazily resolved schema name.
    pub schema_name: String,
    /// Lazily resolved table name.
    pub table_name: String,
    /// Lazily resolved index names.
    pub index_names: Vec<String>,
}

/// Go `StaticPartitionedTableAnalysisJob`.
#[derive(Clone, Debug, Default)]
pub struct StaticPartitionedTableAnalysisJob {
    /// Logical table ID.
    pub global_table_id: i64,
    /// Physical partition ID, also the queue key.
    pub static_partition_id: i64,
    /// Missing public indexes.
    pub index_ids: HashSet<i64>,
    /// Ranking inputs.
    pub indicators: Indicators,
    /// Requested statistics version.
    pub table_stats_version: i32,
    /// Whether a v1-to-v2 rewrite warning is required.
    pub need_version_rewrite_warning: bool,
    /// Priority score.
    pub weight: f64,
    /// Lazily resolved schema name.
    pub schema_name: String,
    /// Lazily resolved logical-table name.
    pub global_table_name: String,
    /// Lazily resolved partition name.
    pub static_partition_name: String,
    /// Lazily resolved index names.
    pub index_names: Vec<String>,
}

/// Go `DynamicPartitionedTableAnalysisJob`.
#[derive(Clone, Debug, Default)]
pub struct DynamicPartitionedTableAnalysisJob {
    /// Logical table ID, also the queue key.
    pub global_table_id: i64,
    /// Partitions needing full analysis.
    pub partition_ids: HashSet<i64>,
    /// Missing index ID to physical partition IDs.
    pub partition_index_ids: HashMap<i64, Vec<i64>>,
    /// Ranking inputs.
    pub indicators: Indicators,
    /// Requested statistics version.
    pub table_stats_version: i32,
    /// Whether a v1-to-v2 rewrite warning is required.
    pub need_version_rewrite_warning: bool,
    /// Priority score.
    pub weight: f64,
    /// Lazily resolved schema name.
    pub schema_name: String,
    /// Lazily resolved logical-table name.
    pub global_table_name: String,
    /// Lazily resolved full-analysis partition names.
    pub partition_names: Vec<String>,
    /// Lazily resolved index name to partition names.
    pub partition_index_names: HashMap<String, Vec<String>>,
}

/// Current metadata lookup result consumed by Go `ValidateAndPrepare`.
#[derive(Clone, Debug)]
pub enum TableLookup {
    /// `InfoSchema.TableInfoByID` did not find the table.
    TableMissing,
    /// The table exists but `InfoSchema.SchemaByID` did not find its schema.
    SchemaMissing,
    /// Both current table and schema metadata exist.
    Found {
        /// Original-case schema name.
        schema_name: String,
        /// Current logical table metadata.
        table: Arc<TableInfo>,
    },
}

/// Rust ownership boundary for Go's session, statistics handle, and process
/// tracker arguments used by `ValidateAndPrepare` and `Analyze`.
pub trait AnalysisJobContext {
    /// Resolve the current logical table and schema.
    fn lookup_table(&self, table_id: i64) -> TableLookup;

    /// Execute Go `GetLastFailedAnalysisDuration`.
    fn last_failed_analysis_duration(
        &self,
        schema: &str,
        table: &str,
        partitions: &[String],
    ) -> Result<i64, String>;

    /// Execute Go `GetAverageAnalysisDuration`.
    fn average_analysis_duration(
        &self,
        schema: &str,
        table: &str,
        partitions: &[String],
    ) -> Result<i64, String>;

    /// Execute one generated statement through Go `exec.AutoAnalyze`'s
    /// ordinary restricted-session ANALYZE route.
    fn auto_analyze(
        &self,
        stats_version: i32,
        need_version_rewrite_warning: bool,
        sql: &str,
        arguments: &[String],
    ) -> bool;

    /// Live Go `vardef.AutoAnalyzePartitionBatchSize` read when a dynamic
    /// partition job begins execution.
    fn auto_analyze_partition_batch_size(&self) -> usize;
}

/// Go's statistics handle, session pool, and latest InfoSchema as one queue
/// ownership boundary.
pub trait PriorityQueueSource: AnalysisJobContext + Send + Sync {
    /// Go `StatsHandle.GetNextCheckVersionWithOffset`. Queue operations call
    /// this before scanning the cache so DML committed during a long scan is
    /// observed again rather than missed.
    fn next_check_version_with_offset(&self) -> u64;

    /// Build the current cache/InfoSchema image used by initialization,
    /// rebuild, DML refresh, retry, and DDL handling.
    fn queue_inventory(&self) -> Result<QueueInventory, String>;
}

/// Go `ValidateAndPrepare` result plus its failure-hook retry decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationResult {
    /// Whether the prepared job may execute.
    pub valid: bool,
    /// Go's exact skip reason, empty on success.
    pub reason: String,
    /// Argument passed to Go's failure hook.
    pub must_retry: bool,
}

impl ValidationResult {
    fn valid() -> Self {
        Self {
            valid: true,
            reason: String::new(),
            must_retry: false,
        }
    }

    fn invalid(reason: impl Into<String>, must_retry: bool) -> Self {
        Self {
            valid: false,
            reason: reason.into(),
            must_retry,
        }
    }
}

impl AnalysisJob {
    fn analyze_type(&self) -> &'static str {
        match self {
            Self::NonPartitioned(job) if job.index_ids.is_empty() => "analyzeTable",
            Self::NonPartitioned(_) => "analyzeIndex",
            Self::StaticPartitioned(job) if job.index_ids.is_empty() => "analyzeStaticPartition",
            Self::StaticPartitioned(_) => "analyzeStaticPartitionIndex",
            Self::DynamicPartitioned(job) if job.partition_index_ids.is_empty() => {
                "analyzeDynamicPartition"
            }
            Self::DynamicPartitioned(_) => "analyzeDynamicPartitionIndex",
        }
    }

    /// Go `GetTableID`.
    pub const fn table_id(&self) -> i64 {
        match self {
            Self::NonPartitioned(job) => job.table_id,
            Self::StaticPartitioned(job) => job.static_partition_id,
            Self::DynamicPartitioned(job) => job.global_table_id,
        }
    }

    /// Go `GetWeight`.
    pub const fn weight(&self) -> f64 {
        match self {
            Self::NonPartitioned(job) => job.weight,
            Self::StaticPartitioned(job) => job.weight,
            Self::DynamicPartitioned(job) => job.weight,
        }
    }

    /// Go `SetWeight`.
    pub fn set_weight(&mut self, weight: f64) {
        match self {
            Self::NonPartitioned(job) => job.weight = weight,
            Self::StaticPartitioned(job) => job.weight = weight,
            Self::DynamicPartitioned(job) => job.weight = weight,
        }
    }

    /// Go `GetIndicators`.
    pub const fn indicators(&self) -> Indicators {
        match self {
            Self::NonPartitioned(job) => job.indicators,
            Self::StaticPartitioned(job) => job.indicators,
            Self::DynamicPartitioned(job) => job.indicators,
        }
    }

    /// Go `SetIndicators`.
    pub fn set_indicators(&mut self, indicators: Indicators) {
        match self {
            Self::NonPartitioned(job) => job.indicators = indicators,
            Self::StaticPartitioned(job) => job.indicators = indicators,
            Self::DynamicPartitioned(job) => job.indicators = indicators,
        }
    }

    /// Go `HasNewlyAddedIndex`.
    pub fn has_newly_added_index(&self) -> bool {
        match self {
            Self::NonPartitioned(job) => !job.index_ids.is_empty(),
            Self::StaticPartitioned(job) => !job.index_ids.is_empty(),
            Self::DynamicPartitioned(job) => !job.partition_index_ids.is_empty(),
        }
    }

    /// Go `IsDynamicPartitionedTableAnalysisJob`.
    pub const fn is_dynamic_partitioned(&self) -> bool {
        matches!(self, Self::DynamicPartitioned(_))
    }

    /// Go `AsJSON`.
    pub fn as_json(&self) -> AnalysisJobJson {
        let indicators = self.indicators();
        let common =
            |kind: &str,
             table_id: i64,
             partition_ids: Vec<i64>,
             index_ids: Vec<i64>,
             partition_index_ids: HashMap<i64, Vec<i64>>| AnalysisJobJson {
                kind: kind.to_owned(),
                table_id,
                weight: self.weight(),
                partition_ids,
                index_ids,
                partition_index_ids,
                indicators: IndicatorsJson {
                    change_percentage: format!("{:.2}%", indicators.change_percentage * 100.0),
                    table_size: format!("{:.2}", indicators.table_size),
                    last_analysis_duration: format_go_duration(indicators.last_analysis_duration),
                },
                has_newly_added_index: self.has_newly_added_index(),
            };
        match self {
            Self::NonPartitioned(job) => common(
                self.analyze_type(),
                job.table_id,
                Vec::new(),
                job.index_ids.iter().copied().collect(),
                HashMap::new(),
            ),
            Self::StaticPartitioned(job) => common(
                self.analyze_type(),
                job.static_partition_id,
                Vec::new(),
                job.index_ids.iter().copied().collect(),
                HashMap::new(),
            ),
            Self::DynamicPartitioned(job) => common(
                self.analyze_type(),
                job.global_table_id,
                job.partition_ids.iter().copied().collect(),
                Vec::new(),
                job.partition_index_ids.clone(),
            ),
        }
    }

    /// Go `ValidateAndPrepare` for all three concrete job types.
    pub fn validate_and_prepare<C: AnalysisJobContext + ?Sized>(
        &mut self,
        context: &C,
    ) -> ValidationResult {
        match self {
            Self::NonPartitioned(job) => job.validate_and_prepare(context),
            Self::StaticPartitioned(job) => job.validate_and_prepare(context),
            Self::DynamicPartitioned(job) => job.validate_and_prepare(context),
        }
    }

    /// Go `Analyze` for all three concrete job types.
    pub fn analyze<C: AnalysisJobContext + ?Sized>(&self, context: &C) -> bool {
        match self {
            Self::NonPartitioned(job) => job.analyze(context),
            Self::StaticPartitioned(job) => job.analyze(context),
            Self::DynamicPartitioned(job) => job.analyze(context),
        }
    }
}

impl fmt::Display for AnalysisJob {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonPartitioned(job) => write!(
                formatter,
                "NonPartitionedTableAnalysisJob:\n\tAnalyzeType: {}\n\tIndexes: {}\n\tSchema: {}\n\tTable: {}\n\tTableID: {}\n\tTableStatsVer: {}\n\tChangePercentage: {:.6}\n\tTableSize: {:.2}\n\tLastAnalysisDuration: {}\n\tWeight: {:.6}\n",
                self.analyze_type(),
                job.index_names.join(", "),
                job.schema_name,
                job.table_name,
                job.table_id,
                job.table_stats_version,
                job.indicators.change_percentage,
                job.indicators.table_size,
                format_go_duration(job.indicators.last_analysis_duration),
                job.weight,
            ),
            Self::StaticPartitioned(job) => write!(
                formatter,
                "StaticPartitionedTableAnalysisJob:\n\tAnalyzeType: {}\n\tIndexes: {}\n\tSchema: {}\n\tGlobalTable: {}\n\tGlobalTableID: {}\n\tStaticPartition: {}\n\tStaticPartitionID: {}\n\tTableStatsVer: {}\n\tChangePercentage: {:.6}\n\tTableSize: {:.2}\n\tLastAnalysisDuration: {}\n\tWeight: {:.6}\n",
                self.analyze_type(),
                job.index_names.join(", "),
                job.schema_name,
                job.global_table_name,
                job.global_table_id,
                job.static_partition_name,
                job.static_partition_id,
                job.table_stats_version,
                job.indicators.change_percentage,
                job.indicators.table_size,
                format_go_duration(job.indicators.last_analysis_duration),
                job.weight,
            ),
            Self::DynamicPartitioned(job) => write!(
                formatter,
                "DynamicPartitionedTableAnalysisJob:\n\tAnalyzeType: {}\n\tPartitions: {}\n\tPartitionIndexes: {}\n\tSchema: {}\n\tGlobal Table: {}\n\tGlobal TableID: {}\n\tTableStatsVer: {}\n\tChangePercentage: {:.6}\n\tTableSize: {:.2}\n\tLastAnalysisDuration: {}\n\tWeight: {:.6}\n",
                self.analyze_type(),
                job.partition_names.join(", "),
                format_go_string_slice_map(&job.partition_index_names),
                job.schema_name,
                job.global_table_name,
                job.global_table_id,
                job.table_stats_version,
                job.indicators.change_percentage,
                job.indicators.table_size,
                format_go_duration(job.indicators.last_analysis_duration),
                job.weight,
            ),
        }
    }
}

fn format_go_string_slice_map(values: &HashMap<String, Vec<String>>) -> String {
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(right.0));
    format!(
        "map[{}]",
        entries
            .into_iter()
            .map(|(key, values)| format!("{key}:[{}]", values.join(" ")))
            .collect::<Vec<_>>()
            .join(" ")
    )
}

/// Go `PriorityCalculator`.
#[derive(Clone, Copy, Debug, Default)]
pub struct PriorityCalculator;

impl PriorityCalculator {
    /// Go `CalculateWeight`.
    pub fn calculate_weight(self, job: &AnalysisJob) -> f64 {
        let indicators = job.indicators();
        let change_ratio = 100.0 * indicators.change_percentage;
        CHANGE_RATIO_WEIGHT * (1.0 + change_ratio).log10()
            + SIZE_WEIGHT * (1.0 - (1.0 + indicators.table_size).log10())
            + ANALYSIS_INTERVAL_WEIGHT
                * (1.0 + ((indicators.last_analysis_duration as f64) / 1_000_000_000.0).sqrt())
                    .log10()
            + self.special_event(job)
    }

    /// Go `GetSpecialEvent`.
    pub fn special_event(self, job: &AnalysisJob) -> f64 {
        if job.has_newly_added_index() {
            EVENT_NEW_INDEX
        } else {
            EVENT_NONE
        }
    }
}

/// Go `pqHeapImpl`, a keyed max heap.
#[derive(Debug, Default)]
pub struct JobHeap {
    items: HashMap<i64, AnalysisJob>,
    queue: Vec<i64>,
    positions: HashMap<i64, usize>,
}

/// Errors exposed by Go's heap and queue initialization gates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueueError {
    /// Go `ErrHeapIsEmpty`.
    HeapIsEmpty,
    /// Go `errors.New("object not found")`.
    ObjectNotFound,
    /// Go `notInitializedErrMsg`.
    NotInitialized,
}

/// Errors returned by queue operations that read the owned production source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueueRuntimeError {
    /// Queue lifecycle/heap API error.
    Queue(QueueError),
    /// Session, cache, or InfoSchema snapshot failure.
    Source(String),
}

impl fmt::Display for QueueRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Queue(error) => error.fmt(formatter),
            Self::Source(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for QueueRuntimeError {}

impl From<QueueError> for QueueRuntimeError {
    fn from(error: QueueError) -> Self {
        Self::Queue(error)
    }
}

impl fmt::Display for QueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::HeapIsEmpty => "heap is empty",
            Self::ObjectNotFound => "object not found",
            Self::NotInitialized => "priority queue not initialized",
        })
    }
}

impl std::error::Error for QueueError {}

impl JobHeap {
    /// Go `newHeap`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `addOrUpdate`/`update`.
    pub fn add_or_update(&mut self, job: AnalysisJob) {
        let key = job.table_id();
        if let Some(position) = self.positions.get(&key).copied() {
            self.items.insert(key, job);
            self.fix(position);
        } else {
            self.items.insert(key, job);
            let position = self.queue.len();
            self.queue.push(key);
            self.positions.insert(key, position);
            self.sift_up(position);
        }
    }

    /// Go `delete`.
    pub fn delete(&mut self, table_id: i64) -> Result<AnalysisJob, QueueError> {
        let position = self
            .positions
            .get(&table_id)
            .copied()
            .ok_or(QueueError::ObjectNotFound)?;
        Ok(self.remove_at(position))
    }

    /// Go `peek`.
    pub fn peek(&self) -> Result<&AnalysisJob, QueueError> {
        self.queue
            .first()
            .and_then(|key| self.items.get(key))
            .ok_or(QueueError::HeapIsEmpty)
    }

    /// Go `pop`.
    pub fn pop(&mut self) -> Result<AnalysisJob, QueueError> {
        if self.queue.is_empty() {
            return Err(QueueError::HeapIsEmpty);
        }
        Ok(self.remove_at(0))
    }

    /// Go `getByKey`.
    #[must_use]
    pub fn get(&self, table_id: i64) -> Option<&AnalysisJob> {
        self.items.get(&table_id)
    }

    /// Go `list`.
    pub fn list(&self) -> impl Iterator<Item = &AnalysisJob> {
        self.items.values()
    }

    /// Go `ListKeys`.
    pub fn keys(&self) -> impl Iterator<Item = i64> + '_ {
        self.items.keys().copied()
    }

    /// Go `isEmpty`.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Go `len`.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    fn remove_at(&mut self, position: usize) -> AnalysisJob {
        let last = self.queue.len() - 1;
        self.swap(position, last);
        let key = self.queue.pop().expect("heap position exists");
        self.positions.remove(&key);
        let job = self.items.remove(&key).expect("heap item exists");
        if position < self.queue.len() {
            self.fix(position);
        }
        job
    }

    fn fix(&mut self, position: usize) {
        let after_up = self.sift_up(position);
        self.sift_down(after_up);
    }

    fn sift_up(&mut self, mut position: usize) -> usize {
        while position > 0 {
            let parent = (position - 1) / 2;
            if !self.greater(position, parent) {
                break;
            }
            self.swap(position, parent);
            position = parent;
        }
        position
    }

    fn sift_down(&mut self, mut position: usize) {
        loop {
            let left = position * 2 + 1;
            if left >= self.queue.len() {
                break;
            }
            let right = left + 1;
            let larger = if right < self.queue.len() && self.greater(right, left) {
                right
            } else {
                left
            };
            if !self.greater(larger, position) {
                break;
            }
            self.swap(position, larger);
            position = larger;
        }
    }

    fn greater(&self, left: usize, right: usize) -> bool {
        self.items[&self.queue[left]].weight() > self.items[&self.queue[right]].weight()
    }

    fn swap(&mut self, left: usize, right: usize) {
        self.queue.swap(left, right);
        self.positions.insert(self.queue[left], left);
        self.positions.insert(self.queue[right], right);
    }
}

/// Go `variable.PartitionPruneMode` as consumed by this package.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartitionPruneMode {
    /// One queue job per physical partition.
    Static,
    /// One queue job per logical partitioned table.
    Dynamic,
}

/// One logical table and its canonical statistics-cache entries.
#[derive(Clone, Debug)]
pub struct InventoryTable {
    /// Original-case schema name.
    pub schema_name: String,
    /// Current table metadata.
    pub table: Arc<TableInfo>,
    /// Logical/nonpartitioned statistics entry.
    pub global_stats: Option<Table>,
    /// Eligible physical partition entries.
    pub partition_stats: HashMap<PartitionIdAndName, Table>,
}

/// One complete cache/InfoSchema image consumed by Go `Initialize`.
#[derive(Clone, Debug)]
pub struct QueueInventory {
    /// Current tables, including ones that do not produce a job.
    pub tables: Vec<InventoryTable>,
    /// One lock-table query result for the whole scan.
    pub locked_table_ids: HashSet<i64>,
    /// Current prune mode.
    pub prune_mode: PartitionPruneMode,
    /// Current `tidb_auto_analyze_ratio`.
    pub auto_analyze_ratio: f64,
    /// Current session analyze version.
    pub requested_version: i32,
    /// TSO read from the restricted session.
    pub current_ts: u64,
    /// Go's live mutable `AutoAnalyzeMinCnt`.
    pub auto_analyze_min_count: i64,
}

/// Go DDL notifier events consumed by `HandleDDLEvent`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PriorityQueueDdlEvent {
    /// `ActionAddIndex`.
    AddIndex {
        /// Current logical table ID.
        table_id: i64,
        /// Whether DDL already analyzed the index.
        analyzed: bool,
    },
    /// `ActionTruncateTable`; all IDs belong to the dropped table image.
    TruncateTable {
        /// Logical and physical IDs from the dropped table image.
        dropped_ids: Vec<i64>,
    },
    /// `ActionDropTable`.
    DropTable {
        /// Logical and physical IDs from the dropped table image.
        dropped_ids: Vec<i64>,
    },
    /// `ActionTruncateTablePartition`.
    TruncatePartition {
        /// Current logical table ID.
        table_id: i64,
        /// Old physical partition IDs.
        dropped_partition_ids: Vec<i64>,
    },
    /// `ActionDropTablePartition`.
    DropPartition {
        /// Current logical table ID.
        table_id: i64,
        /// Removed physical partition IDs.
        dropped_partition_ids: Vec<i64>,
    },
    /// `ActionExchangeTablePartition`.
    ExchangePartition {
        /// Current partitioned-table ID.
        partitioned_table_id: i64,
        /// Physical ID of the exchanged-out partition.
        old_partition_id: i64,
        /// Old standalone-table ID.
        old_standalone_table_id: i64,
        /// Current standalone-table ID after exchange.
        new_standalone_table_id: Option<i64>,
    },
    /// `ActionReorganizePartition`.
    ReorganizePartition {
        /// Current logical table ID.
        table_id: i64,
        /// Replaced physical partition IDs.
        dropped_partition_ids: Vec<i64>,
    },
    /// `ActionAlterTablePartitioning`.
    AlterTablePartitioning {
        /// Former nonpartitioned physical ID.
        old_table_id: i64,
        /// Current logical partitioned-table ID.
        new_table_id: i64,
    },
    /// `ActionRemovePartitioning`.
    RemovePartitioning {
        /// Former logical partitioned-table ID.
        old_table_id: i64,
        /// Former physical partition IDs.
        dropped_partition_ids: Vec<i64>,
        /// Current nonpartitioned table ID.
        new_table_id: i64,
    },
    /// `ActionDropSchema`, flattened from Go `MiniDBInfo`.
    DropSchema {
        /// Every logical and physical table ID in the dropped schema.
        dropped_ids: Vec<i64>,
    },
    /// Every notifier action Go deliberately ignores.
    Other,
}

/// Go notifier's only externally returned queue-handler error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DdlHandleError {
    /// Go `notifier.ErrNotReadyRetryLater`.
    NotReadyRetryLater,
}

impl fmt::Display for DdlHandleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("not ready, retry later")
    }
}

impl std::error::Error for DdlHandleError {}

impl QueueInventory {
    fn stats_by_physical_id(&self, physical_id: i64) -> Option<&Table> {
        self.tables.iter().find_map(|entry| {
            entry
                .global_stats
                .as_ref()
                .filter(|stats| stats.hist_coll.physical_id == physical_id)
                .or_else(|| {
                    entry.partition_stats.iter().find_map(|(partition, stats)| {
                        (partition.id == physical_id).then_some(stats)
                    })
                })
        })
    }

    fn entry_by_physical_id(&self, physical_id: i64) -> Option<&InventoryTable> {
        self.tables.iter().find(|entry| {
            entry.table.id == physical_id
                || entry
                    .partition_stats
                    .keys()
                    .any(|partition| partition.id == physical_id)
        })
    }
}

#[derive(Debug, Default)]
struct QueueState {
    initialized: bool,
    heap: JobHeap,
    running_jobs: HashSet<i64>,
    must_retry_jobs: HashSet<i64>,
    last_dml_update_fetch_timestamp: u64,
}

/// Go `AnalysisPriorityQueue` synchronized state and APIs.
pub struct AnalysisPriorityQueue {
    source: Arc<dyn PriorityQueueSource>,
    state: Mutex<QueueState>,
    stopped: Condvar,
    worker: Mutex<Option<QueueWorker>>,
    calculator: PriorityCalculator,
}

/// One popped job carrying Go's registered success/failure hooks.
pub struct RunningAnalysisJob {
    job: AnalysisJob,
    queue: std::sync::Weak<AnalysisPriorityQueue>,
}

impl RunningAnalysisJob {
    /// Go `GetTableID`.
    pub fn table_id(&self) -> i64 {
        self.job.table_id()
    }

    /// Go `ValidateAndPrepare`, including its failure-hook side effect.
    pub fn validate_and_prepare(&mut self) -> ValidationResult {
        let Some(queue) = self.queue.upgrade() else {
            return ValidationResult::invalid("priority queue not initialized", false);
        };
        let result = self.job.validate_and_prepare(queue.source.as_ref());
        if !result.valid {
            queue.finish_failure(self.job.table_id(), result.must_retry);
        }
        result
    }

    /// Go `Analyze`, including the registered success/failure hook.
    pub fn analyze(self) -> bool {
        let Some(queue) = self.queue.upgrade() else {
            return false;
        };
        let success = self.job.analyze(queue.source.as_ref());
        if success {
            queue.finish_success(self.job.table_id());
        } else {
            queue.finish_failure(self.job.table_id(), true);
        }
        success
    }
}

struct QueueWorker {
    stop: Sender<()>,
    join: JoinHandle<()>,
}

impl AnalysisPriorityQueue {
    /// Go `NewAnalysisPriorityQueue` without starting `Initialize`.
    pub fn new(source: Arc<dyn PriorityQueueSource>) -> Arc<Self> {
        Arc::new(Self {
            source,
            state: Mutex::new(QueueState::default()),
            stopped: Condvar::new(),
            worker: Mutex::new(None),
            calculator: PriorityCalculator,
        })
    }

    /// Go `Initialize`'s cache/InfoSchema scan and atomic publication.
    pub fn initialize(self: &Arc<Self>) -> Result<(), QueueRuntimeError> {
        let mut state = self.lock_state();
        if state.initialized {
            return Ok(());
        }
        let next_check_version = self.source.next_check_version_with_offset();
        let inventory = self
            .source
            .queue_inventory()
            .map_err(QueueRuntimeError::Source)?;
        state.heap = JobHeap::new();
        state.running_jobs = HashSet::new();
        state.must_retry_jobs = HashSet::new();
        Self::build_jobs(&self.calculator, &mut state, &inventory);
        state.last_dml_update_fetch_timestamp = next_check_version;
        state.initialized = true;
        let (stop, receiver) = mpsc::channel();
        let queue = Arc::downgrade(self);
        let join = std::thread::spawn(move || {
            let Some(queue) = queue.upgrade() else {
                return;
            };
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                queue.run(receiver);
            }));
            queue.reset_sync_fields();
        });
        *self.lock_worker() = Some(QueueWorker { stop, join });
        Ok(())
    }

    /// Go `Rebuild`, replacing the heap while retaining lifecycle state.
    pub fn rebuild(&self) -> Result<(), QueueRuntimeError> {
        let mut state = self.lock_state();
        Self::require_initialized(&state)?;
        state.heap = JobHeap::new();
        let next_check_version = self.source.next_check_version_with_offset();
        let inventory = self
            .source
            .queue_inventory()
            .map_err(QueueRuntimeError::Source)?;
        Self::build_jobs(&self.calculator, &mut state, &inventory);
        state.last_dml_update_fetch_timestamp = next_check_version;
        Ok(())
    }

    /// Go `ProcessDMLChanges` over one current cache and InfoSchema image.
    pub fn process_dml_changes(&self) -> Result<(), QueueRuntimeError> {
        let mut state = self.lock_state();
        Self::require_initialized(&state)?;
        let next_check_version = self.source.next_check_version_with_offset();
        let inventory = self
            .source
            .queue_inventory()
            .map_err(QueueRuntimeError::Source)?;
        let last_version = state.last_dml_update_fetch_timestamp;
        let factory = Self::job_factory(&inventory);

        for entry in &inventory.tables {
            if tidb_metadef::is_mem_or_sys_db(&entry.schema_name.to_ascii_lowercase())
                || entry.table.is_view()
            {
                continue;
            }
            let partitioned = entry.table.partition.is_some();
            if let Some(stats) = entry
                .global_stats
                .as_ref()
                .filter(|stats| stats.version > last_version)
            {
                Self::process_changed_stats(
                    &self.calculator,
                    &mut state,
                    &inventory,
                    entry,
                    stats,
                    partitioned,
                    None,
                    &factory,
                );
            }
            for (partition, stats) in &entry.partition_stats {
                if stats.version <= last_version {
                    continue;
                }
                Self::process_changed_stats(
                    &self.calculator,
                    &mut state,
                    &inventory,
                    entry,
                    stats,
                    partitioned,
                    Some(partition),
                    &factory,
                );
            }
        }
        if next_check_version > last_version {
            state.last_dml_update_fetch_timestamp = next_check_version;
        }
        Ok(())
    }

    /// Go `RequeueMustRetryJobs` using the current metadata/statistics image.
    pub fn requeue_must_retry_jobs(&self) -> Result<(), QueueRuntimeError> {
        let mut state = self.lock_state();
        Self::require_initialized(&state)?;
        let inventory = self
            .source
            .queue_inventory()
            .map_err(QueueRuntimeError::Source)?;
        let table_ids = state.must_retry_jobs.drain().collect::<Vec<_>>();
        for table_id in table_ids {
            let Some(entry) = inventory.entry_by_physical_id(table_id) else {
                continue;
            };
            Self::recreate_table_jobs(&self.calculator, &mut state, &inventory, entry.table.id);
        }
        Ok(())
    }

    /// Go `RefreshLastAnalysisDuration` using the current TSO and stats cache.
    pub fn refresh_last_analysis_duration(&self) -> Result<(), QueueRuntimeError> {
        let mut state = self.lock_state();
        Self::require_initialized(&state)?;
        let inventory = self
            .source
            .queue_inventory()
            .map_err(QueueRuntimeError::Source)?;
        let factory = Self::job_factory(&inventory);
        let keys = state.heap.keys().collect::<Vec<_>>();
        for table_id in keys {
            let stats = inventory.stats_by_physical_id(table_id);
            let Some(stats) = stats else {
                let _ = state.heap.delete(table_id);
                continue;
            };
            let Some(mut job) = state.heap.get(table_id).cloned() else {
                continue;
            };
            let mut indicators = job.indicators();
            indicators.last_analysis_duration = factory.last_analysis_duration(stats);
            job.set_indicators(indicators);
            job.set_weight(self.calculator.calculate_weight(&job));
            state.heap.add_or_update(job);
        }
        Ok(())
    }

    /// Go `HandleDDLEvent`, including its initialization gate and best-effort
    /// mutation semantics.
    pub fn handle_ddl_event(
        &self,
        run_auto_analyze: bool,
        event: &PriorityQueueDdlEvent,
    ) -> Result<(), DdlHandleError> {
        let mut state = self.lock_state();
        if !state.initialized {
            return if run_auto_analyze {
                Err(DdlHandleError::NotReadyRetryLater)
            } else {
                Ok(())
            };
        }
        let inventory = match self.source.queue_inventory() {
            Ok(inventory) => inventory,
            Err(_) => return Ok(()),
        };
        let mut delete = |table_id| {
            if state.heap.get(table_id).is_some() {
                let _ = state.heap.delete(table_id);
            }
        };
        match event {
            PriorityQueueDdlEvent::AddIndex { table_id, analyzed } => {
                if !analyzed {
                    Self::recreate_table_jobs(&self.calculator, &mut state, &inventory, *table_id);
                }
            }
            PriorityQueueDdlEvent::TruncateTable { dropped_ids }
            | PriorityQueueDdlEvent::DropTable { dropped_ids }
            | PriorityQueueDdlEvent::DropSchema { dropped_ids } => {
                for table_id in dropped_ids {
                    delete(*table_id);
                }
            }
            PriorityQueueDdlEvent::TruncatePartition {
                table_id,
                dropped_partition_ids,
            }
            | PriorityQueueDdlEvent::DropPartition {
                table_id,
                dropped_partition_ids,
            }
            | PriorityQueueDdlEvent::ReorganizePartition {
                table_id,
                dropped_partition_ids,
            } => {
                for partition_id in dropped_partition_ids {
                    delete(*partition_id);
                }
                delete(*table_id);
                Self::recreate_table_jobs(&self.calculator, &mut state, &inventory, *table_id);
            }
            PriorityQueueDdlEvent::ExchangePartition {
                partitioned_table_id,
                old_partition_id,
                old_standalone_table_id,
                new_standalone_table_id,
            } => {
                delete(*old_partition_id);
                delete(*old_standalone_table_id);
                delete(*partitioned_table_id);
                Self::recreate_table_jobs(
                    &self.calculator,
                    &mut state,
                    &inventory,
                    *partitioned_table_id,
                );
                if let Some(table_id) = new_standalone_table_id {
                    Self::recreate_table_jobs(&self.calculator, &mut state, &inventory, *table_id);
                }
            }
            PriorityQueueDdlEvent::AlterTablePartitioning {
                old_table_id,
                new_table_id,
            } => {
                delete(*old_table_id);
                delete(*new_table_id);
                Self::recreate_table_jobs(&self.calculator, &mut state, &inventory, *new_table_id);
            }
            PriorityQueueDdlEvent::RemovePartitioning {
                old_table_id,
                dropped_partition_ids,
                new_table_id,
            } => {
                for partition_id in dropped_partition_ids {
                    delete(*partition_id);
                }
                delete(*old_table_id);
                Self::recreate_table_jobs(&self.calculator, &mut state, &inventory, *new_table_id);
            }
            PriorityQueueDdlEvent::Other => {}
        }
        Ok(())
    }

    /// Go `IsInitialized`.
    pub fn is_initialized(&self) -> bool {
        self.lock_state().initialized
    }

    /// Go `pushWithoutLock`, including running/must-retry suppression.
    pub fn push(&self, job: Option<AnalysisJob>) -> Result<(), QueueError> {
        let mut state = self.lock_state();
        Self::require_initialized(&state)?;
        Self::push_locked(&self.calculator, &mut state, job);
        Ok(())
    }

    /// Go `Pop`: removes the highest-priority job and marks its identity running.
    pub fn pop(self: &Arc<Self>) -> Result<RunningAnalysisJob, QueueError> {
        let mut state = self.lock_state();
        Self::require_initialized(&state)?;
        let job = state.heap.pop()?;
        state.running_jobs.insert(job.table_id());
        Ok(RunningAnalysisJob {
            job,
            queue: Arc::downgrade(self),
        })
    }

    fn finish_success(&self, table_id: i64) {
        let mut state = self.lock_state();
        if state.initialized {
            state.running_jobs.remove(&table_id);
        }
    }

    fn finish_failure(&self, table_id: i64, must_retry: bool) {
        let mut state = self.lock_state();
        if !state.initialized {
            return;
        }
        state.running_jobs.remove(&table_id);
        if must_retry {
            state.must_retry_jobs.insert(table_id);
        }
    }

    /// Go `getAndDeleteJob`; a missing identity is a successful no-op.
    pub fn delete(&self, table_id: i64) -> Result<(), QueueError> {
        let mut state = self.lock_state();
        Self::require_initialized(&state)?;
        if state.heap.get(table_id).is_some() {
            state.heap.delete(table_id)?;
        }
        Ok(())
    }

    /// Go `PeekForTest`.
    pub fn peek(&self) -> Result<AnalysisJob, QueueError> {
        let state = self.lock_state();
        Self::require_initialized(&state)?;
        state.heap.peek().cloned()
    }

    /// Go `IsEmptyForTest`.
    pub fn is_empty(&self) -> Result<bool, QueueError> {
        let state = self.lock_state();
        Self::require_initialized(&state)?;
        Ok(state.heap.is_empty())
    }

    /// Go `Len`.
    pub fn len(&self) -> Result<usize, QueueError> {
        let state = self.lock_state();
        Self::require_initialized(&state)?;
        Ok(state.heap.len())
    }

    /// Go `GetRunningJobs`; deliberately not initialization-gated.
    pub fn running_jobs(&self) -> HashSet<i64> {
        self.lock_state().running_jobs.clone()
    }

    /// Go `Snapshot`, sorting only current jobs by descending weight.
    pub fn snapshot(&self) -> Result<PriorityQueueSnapshot, QueueError> {
        let state = self.lock_state();
        Self::require_initialized(&state)?;
        let mut current_jobs = state
            .heap
            .list()
            .map(AnalysisJob::as_json)
            .collect::<Vec<_>>();
        current_jobs.sort_by(|left, right| {
            right
                .weight
                .partial_cmp(&left.weight)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(PriorityQueueSnapshot {
            current_jobs,
            must_retry_tables: state.must_retry_jobs.iter().copied().collect(),
        })
    }

    /// Go `Close`: cancel under the lock, then wait outside it.
    pub fn close(&self) {
        let worker = {
            let state = self.lock_state();
            if !state.initialized {
                return;
            }
            self.lock_worker().take()
        };
        if let Some(worker) = worker {
            let _ = worker.stop.send(());
            let _ = worker.join.join();
        } else {
            let mut state = self.lock_state();
            while state.initialized {
                state = self
                    .stopped
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
    }

    fn run(&self, receiver: Receiver<()>) {
        let started = Instant::now();
        let mut next_dml = started + DML_CHANGES_FETCH_INTERVAL;
        let mut next_duration = started + LAST_ANALYSIS_DURATION_REFRESH_INTERVAL;
        let mut next_retry = started + MUST_RETRY_JOB_REQUEUE_INTERVAL;
        loop {
            let next = next_dml.min(next_duration).min(next_retry);
            match receiver.recv_timeout(next.saturating_duration_since(Instant::now())) {
                Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
                Err(RecvTimeoutError::Timeout) => {}
            }
            let now = Instant::now();
            if now >= next_dml {
                let _ = self.process_dml_changes();
                while next_dml <= now {
                    next_dml += DML_CHANGES_FETCH_INTERVAL;
                }
            }
            if now >= next_duration {
                let _ = self.refresh_last_analysis_duration();
                while next_duration <= now {
                    next_duration += LAST_ANALYSIS_DURATION_REFRESH_INTERVAL;
                }
            }
            if now >= next_retry {
                let _ = self.requeue_must_retry_jobs();
                while next_retry <= now {
                    next_retry += MUST_RETRY_JOB_REQUEUE_INTERVAL;
                }
            }
        }
    }

    fn reset_sync_fields(&self) {
        *self.lock_state() = QueueState::default();
        self.stopped.notify_all();
    }

    fn push_locked(
        calculator: &PriorityCalculator,
        state: &mut QueueState,
        mut job: Option<AnalysisJob>,
    ) {
        let Some(mut job) = job.take() else {
            return;
        };
        let table_id = job.table_id();
        if state.must_retry_jobs.contains(&table_id) {
            return;
        }
        if state.running_jobs.contains(&table_id) {
            state.must_retry_jobs.insert(table_id);
            return;
        }
        let weight = calculator.calculate_weight(&job);
        job.set_weight(weight);
        state.heap.add_or_update(job);
    }

    fn build_jobs(
        calculator: &PriorityCalculator,
        state: &mut QueueState,
        inventory: &QueueInventory,
    ) {
        let factory = Self::job_factory(inventory);
        for entry in &inventory.tables {
            if tidb_metadef::is_mem_or_sys_db(&entry.schema_name.to_ascii_lowercase())
                || entry.table.is_view()
                || inventory.locked_table_ids.contains(&entry.table.id)
            {
                continue;
            }
            let job = match (entry.table.partition.is_some(), inventory.prune_mode) {
                (false, _) => entry
                    .global_stats
                    .as_ref()
                    .and_then(|stats| factory.create_non_partitioned(&entry.table, stats)),
                (true, PartitionPruneMode::Static) => {
                    for (partition, stats) in &entry.partition_stats {
                        if inventory.locked_table_ids.contains(&partition.id) {
                            continue;
                        }
                        let job =
                            factory.create_static_partition(&entry.table, partition.id, stats);
                        Self::push_locked(calculator, state, job);
                    }
                    None
                }
                (true, PartitionPruneMode::Dynamic) => {
                    let global = entry.global_stats.as_ref();
                    global.and_then(|global| {
                        let partitions = entry
                            .partition_stats
                            .iter()
                            .filter(|(partition, _)| {
                                !inventory.locked_table_ids.contains(&partition.id)
                            })
                            .map(|(partition, stats)| (partition.clone(), stats.clone()))
                            .collect();
                        factory.create_dynamic_partitioned(&entry.table, global, &partitions)
                    })
                }
            };
            Self::push_locked(calculator, state, job);
        }
    }

    fn process_changed_stats(
        calculator: &PriorityCalculator,
        state: &mut QueueState,
        inventory: &QueueInventory,
        entry: &InventoryTable,
        stats: &Table,
        partitioned: bool,
        partition: Option<&PartitionIdAndName>,
        factory: &AnalysisJobFactory,
    ) {
        if !stats.is_eligible_for_analysis(inventory.auto_analyze_min_count) {
            return;
        }
        let physical_id = stats.hist_coll.physical_id;
        let old = state.heap.get(physical_id).cloned();
        let job = if let Some(mut old) = old {
            if inventory.locked_table_ids.contains(&physical_id) {
                let _ = state.heap.delete(physical_id);
                return;
            }
            if matches!(old, AnalysisJob::DynamicPartitioned(_)) {
                Self::create_job_for_entry(inventory, entry, None, factory)
            } else {
                let mut indicators = old.indicators();
                indicators.change_percentage = factory.change_percentage(stats);
                indicators.table_size = factory.table_size(stats);
                old.set_indicators(indicators);
                Some(old)
            }
        } else if !partitioned {
            Self::create_job_for_entry(inventory, entry, None, factory)
        } else {
            match inventory.prune_mode {
                PartitionPruneMode::Static => partition.and_then(|partition| {
                    Self::create_job_for_entry(inventory, entry, Some(partition.id), factory)
                }),
                PartitionPruneMode::Dynamic => {
                    Self::create_job_for_entry(inventory, entry, None, factory)
                }
            }
        };
        Self::push_locked(calculator, state, job);
    }

    fn recreate_table_jobs(
        calculator: &PriorityCalculator,
        state: &mut QueueState,
        inventory: &QueueInventory,
        table_id: i64,
    ) {
        let Some(entry) = inventory.entry_by_physical_id(table_id) else {
            return;
        };
        let factory = Self::job_factory(inventory);
        if entry.table.partition.is_some() && inventory.prune_mode == PartitionPruneMode::Static {
            let Some(partition_info) = entry.table.get_partition_info() else {
                return;
            };
            for partition in partition_info.read().definitions.snapshot() {
                if !entry
                    .partition_stats
                    .keys()
                    .any(|candidate| candidate.id == partition.id)
                {
                    return;
                }
                let job =
                    Self::create_job_for_entry(inventory, entry, Some(partition.id), &factory);
                Self::push_locked(calculator, state, job);
            }
        } else {
            let job = Self::create_job_for_entry(inventory, entry, None, &factory);
            Self::push_locked(calculator, state, job);
        }
    }

    fn create_job_for_entry(
        inventory: &QueueInventory,
        entry: &InventoryTable,
        partition_id: Option<i64>,
        factory: &AnalysisJobFactory,
    ) -> Option<AnalysisJob> {
        if entry.table.partition.is_none() {
            if inventory.locked_table_ids.contains(&entry.table.id) {
                return None;
            }
            return entry
                .global_stats
                .as_ref()
                .and_then(|stats| factory.create_non_partitioned(&entry.table, stats));
        }
        match inventory.prune_mode {
            PartitionPruneMode::Static => {
                let partition_id = partition_id?;
                if inventory.locked_table_ids.contains(&partition_id) {
                    return None;
                }
                let stats = entry
                    .partition_stats
                    .iter()
                    .find_map(|(partition, stats)| {
                        (partition.id == partition_id).then_some(stats)
                    })?;
                factory.create_static_partition(&entry.table, partition_id, stats)
            }
            PartitionPruneMode::Dynamic => {
                if inventory.locked_table_ids.contains(&entry.table.id) {
                    return None;
                }
                let global = entry.global_stats.as_ref()?;
                let partitions = entry
                    .partition_stats
                    .iter()
                    .filter(|(partition, _)| !inventory.locked_table_ids.contains(&partition.id))
                    .map(|(partition, stats)| (partition.clone(), stats.clone()))
                    .collect();
                factory.create_dynamic_partitioned(&entry.table, global, &partitions)
            }
        }
    }

    fn job_factory(inventory: &QueueInventory) -> AnalysisJobFactory {
        AnalysisJobFactory::new(
            inventory.auto_analyze_ratio,
            inventory.current_ts,
            inventory.requested_version,
            inventory.auto_analyze_min_count,
        )
    }

    fn require_initialized(state: &QueueState) -> Result<(), QueueError> {
        if state.initialized {
            Ok(())
        } else {
            Err(QueueError::NotInitialized)
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, QueueState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_worker(&self) -> std::sync::MutexGuard<'_, Option<QueueWorker>> {
        self.worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Go `PriorityQueueSnapshot`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PriorityQueueSnapshot {
    /// Waiting jobs in descending priority order.
    pub current_jobs: Vec<AnalysisJobJson>,
    /// Identities deferred by a running job.
    pub must_retry_tables: Vec<i64>,
}

/// Go `AnalysisJobFactory` over the package's canonical metadata/statistics objects.
#[derive(Clone, Copy, Debug)]
pub struct AnalysisJobFactory {
    auto_analyze_ratio: f64,
    current_ts: u64,
    requested_version: i32,
    auto_analyze_min_count: i64,
}

impl AnalysisJobFactory {
    /// Go `NewAnalysisJobFactory`; the minimum count is explicit because the
    /// Rust statistics package does not expose Go's mutable test global.
    pub const fn new(
        auto_analyze_ratio: f64,
        current_ts: u64,
        requested_version: i32,
        auto_analyze_min_count: i64,
    ) -> Self {
        Self {
            auto_analyze_ratio,
            current_ts,
            requested_version,
            auto_analyze_min_count,
        }
    }

    /// Go `CreateNonPartitionedTableAnalysisJob`.
    #[must_use]
    pub fn create_non_partitioned(self, table: &TableInfo, stats: &Table) -> Option<AnalysisJob> {
        if !stats.is_eligible_for_analysis(self.auto_analyze_min_count) {
            return None;
        }
        let change_percentage = self.change_percentage(stats);
        let index_ids = indexes_needing_analyze(table, stats);
        if change_percentage == 0.0 && index_ids.is_empty() {
            return None;
        }
        Some(AnalysisJob::NonPartitioned(
            NonPartitionedTableAnalysisJob {
                table_id: table.id,
                index_ids,
                indicators: Indicators {
                    change_percentage,
                    table_size: table_size(stats),
                    last_analysis_duration: self.last_analysis_duration(stats),
                },
                table_stats_version: self.requested_version,
                need_version_rewrite_warning: !analyze_version_matches(
                    stats,
                    self.requested_version,
                ),
                ..NonPartitionedTableAnalysisJob::default()
            },
        ))
    }

    /// Go `CreateStaticPartitionAnalysisJob`.
    #[must_use]
    pub fn create_static_partition(
        self,
        table: &TableInfo,
        partition_id: i64,
        stats: &Table,
    ) -> Option<AnalysisJob> {
        let AnalysisJob::NonPartitioned(job) = self.create_non_partitioned(table, stats)? else {
            unreachable!()
        };
        Some(AnalysisJob::StaticPartitioned(
            StaticPartitionedTableAnalysisJob {
                global_table_id: table.id,
                static_partition_id: partition_id,
                index_ids: job.index_ids,
                indicators: job.indicators,
                table_stats_version: job.table_stats_version,
                need_version_rewrite_warning: job.need_version_rewrite_warning,
                ..StaticPartitionedTableAnalysisJob::default()
            },
        ))
    }

    /// Go `CreateDynamicPartitionedTableAnalysisJob`.
    #[must_use]
    pub fn create_dynamic_partitioned(
        self,
        table: &TableInfo,
        global_stats: &Table,
        partition_stats: &HashMap<PartitionIdAndName, Table>,
    ) -> Option<AnalysisJob> {
        if !global_stats.is_eligible_for_analysis(self.auto_analyze_min_count) {
            return None;
        }
        let eligible_partition_stats = partition_stats
            .iter()
            .filter(|(_, stats)| stats.is_eligible_for_analysis(self.auto_analyze_min_count))
            .map(|(partition, stats)| (partition.clone(), stats.clone()))
            .collect::<HashMap<_, _>>();
        let (indicators, partition_ids) =
            self.partition_indicators(global_stats, &eligible_partition_stats);
        let partition_index_ids =
            partition_indexes_needing_analyze(table, &eligible_partition_stats);
        if partition_ids.is_empty() && partition_index_ids.is_empty() {
            return None;
        }
        let versions_match = analyze_version_matches(global_stats, self.requested_version)
            && eligible_partition_stats
                .values()
                .all(|stats| analyze_version_matches(stats, self.requested_version));
        Some(AnalysisJob::DynamicPartitioned(
            DynamicPartitionedTableAnalysisJob {
                global_table_id: table.id,
                partition_ids,
                partition_index_ids,
                indicators,
                table_stats_version: self.requested_version,
                need_version_rewrite_warning: !versions_match,
                ..DynamicPartitionedTableAnalysisJob::default()
            },
        ))
    }

    /// Go `CalculateChangePercentage`.
    pub fn change_percentage(self, stats: &Table) -> f64 {
        if !stats.is_analyzed() {
            return 1.0;
        }
        if self.auto_analyze_ratio == 0.0 {
            return 0.0;
        }
        let analyzed = stats.hist_coll.analyze_row_count();
        let count = if analyzed > 0.0 {
            analyzed
        } else {
            stats.hist_coll.realtime_count as f64
        };
        let change = stats.hist_coll.modify_count as f64 / count;
        if change > self.auto_analyze_ratio {
            change
        } else {
            0.0
        }
    }

    /// Go `GetTableLastAnalyzeDuration`.
    pub fn last_analysis_duration(self, stats: &Table) -> i64 {
        if !stats.is_analyzed() {
            return UNANALYZED_LAST_ANALYSIS_DURATION;
        }
        let current_ms = self.current_ts >> 18;
        let analyzed_ms = stats.last_analyze_version >> 18;
        let milliseconds = i128::from(current_ms) - i128::from(analyzed_ms);
        i64::try_from(milliseconds.saturating_mul(1_000_000)).unwrap_or_else(|_| {
            if milliseconds.is_negative() {
                i64::MIN
            } else {
                i64::MAX
            }
        })
    }

    /// Go `CalculateTableSize`.
    pub fn table_size(self, stats: &Table) -> f64 {
        table_size(stats)
    }

    /// Go `CalculateIndicatorsForPartitions`.
    pub fn partition_indicators(
        self,
        global_stats: &Table,
        partition_stats: &HashMap<PartitionIdAndName, Table>,
    ) -> (Indicators, HashSet<i64>) {
        let columns = global_stats
            .existence_map
            .as_ref()
            .expect("eligible global statistics must have an existence map")
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .column_count();
        assert_ne!(columns, 0, "Column count should not be 0");

        let mut total_change = 0.0;
        let mut total_size = 0.0;
        let mut total_duration = 0_i64;
        let mut partition_ids = HashSet::with_capacity(partition_stats.len());
        for (partition, stats) in partition_stats {
            let change = self.change_percentage(stats);
            if change == 0.0 {
                continue;
            }
            total_change += change;
            total_size += stats.hist_coll.realtime_count as f64 * columns as f64;
            total_duration = total_duration.wrapping_add(self.last_analysis_duration(stats));
            partition_ids.insert(partition.id);
        }
        if partition_ids.is_empty() {
            return (Indicators::default(), partition_ids);
        }
        let count = partition_ids.len();
        (
            Indicators {
                change_percentage: total_change / count as f64,
                table_size: total_size / count as f64,
                last_analysis_duration: total_duration / count as i64,
            },
            partition_ids,
        )
    }
}

/// Go `PartitionIDAndName`.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PartitionIdAndName {
    /// Original-case partition name.
    pub name: String,
    /// Physical partition ID.
    pub id: i64,
}

/// Go `AutoAnalysisTimeWindow`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AutoAnalysisTimeWindow {
    start: Option<DateTime<FixedOffset>>,
    end: Option<DateTime<FixedOffset>>,
}

impl AutoAnalysisTimeWindow {
    /// Go `NewAutoAnalysisTimeWindow`.
    pub const fn new(start: DateTime<FixedOffset>, end: DateTime<FixedOffset>) -> Self {
        Self {
            start: Some(start),
            end: Some(end),
        }
    }

    /// Go `IsWithinTimeWindow`, comparing inclusive UTC hour/minute values.
    pub fn is_within_time_window(&self, current: DateTime<Utc>) -> bool {
        let (Some(start), Some(end)) = (self.start, self.end) else {
            return false;
        };
        let minute = |time: DateTime<Utc>| time.hour() * 60 + time.minute();
        let start = minute(start.with_timezone(&Utc));
        let end = minute(end.with_timezone(&Utc));
        let current = minute(current);
        if end >= start {
            current >= start && current <= end
        } else {
            current <= end || current >= start
        }
    }
}

impl PartitionIdAndName {
    /// Go `NewPartitionIDAndName`.
    pub fn new(name: impl Into<String>, id: i64) -> Self {
        Self {
            name: name.into(),
            id,
        }
    }
}

fn table_size(stats: &Table) -> f64 {
    let columns = stats
        .existence_map
        .as_ref()
        .expect("eligible statistics must have an existence map")
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .column_count();
    assert_ne!(columns, 0, "Column count should not be 0");
    stats.hist_coll.realtime_count as f64 * columns as f64
}

fn analyze_version_matches(stats: &Table, requested_version: i32) -> bool {
    tidb_stats::analyze_version_matches(
        Some(i64::from(stats.hist_coll.stats_version)),
        stats.hist_coll.pseudo,
        i64::from(requested_version),
    )
}

fn indexes_needing_analyze(table: &TableInfo, stats: &Table) -> HashSet<i64> {
    if !stats.is_analyzed() {
        return HashSet::new();
    }
    let Some(existence) = &stats.existence_map else {
        return HashSet::new();
    };
    let existence = existence
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    table
        .indices
        .iter_deref()
        .filter(|index| {
            let index = index.read();
            index.state == SchemaState::PUBLIC
                && !index.is_columnar_index()
                && stats.hist_coll.get_index(index.id).is_none()
                && !existence.has_analyzed(index.id, true)
        })
        .map(|index| index.read().id)
        .collect()
}

fn partition_indexes_needing_analyze(
    table: &TableInfo,
    partition_stats: &HashMap<PartitionIdAndName, Table>,
) -> HashMap<i64, Vec<i64>> {
    let mut result = HashMap::with_capacity(table.indices.len());
    for shared_index in table.indices.iter_deref() {
        let index = shared_index.read();
        if index.state != SchemaState::PUBLIC
            || index.is_columnar_index()
            || tidb_stats_handle_util::is_special_global_index(&index, table)
        {
            continue;
        }
        let mut partition_ids = Vec::with_capacity(partition_stats.len());
        for (partition, stats) in partition_stats {
            let analyzed = stats.existence_map.as_ref().is_some_and(|map| {
                map.read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .has_analyzed(index.id, true)
            });
            if stats.hist_coll.get_index(index.id).is_none() && !analyzed {
                partition_ids.push(partition.id);
            }
        }
        if !partition_ids.is_empty() {
            result.insert(index.id, partition_ids);
        }
    }
    result
}

/// Converts Go's successful-analysis AVG query result to `time.Duration`.
pub fn average_analysis_duration(seconds: Option<f64>) -> i64 {
    let Some(seconds) = seconds else {
        return NO_RECORD;
    };
    if seconds < 0.0 {
        return NO_RECORD;
    }
    (seconds as i64).wrapping_mul(1_000_000_000)
}

/// Converts Go's latest-failure TIMESTAMPDIFF result to `time.Duration`.
pub fn last_failed_analysis_duration(seconds: Option<i64>) -> i64 {
    match seconds {
        None => NO_RECORD,
        Some(0) => JUST_FAILED,
        Some(value) if value < 0 => DEFAULT_FAILED_ANALYSIS_WAIT_TIME,
        Some(value) => value.wrapping_mul(1_000_000_000),
    }
}

/// The decision half of Go `isValidToAnalyze` after its two SQL reads.
pub fn valid_to_analyze(last_failed: i64, average: i64) -> (bool, String) {
    if last_failed == JUST_FAILED {
        return (false, "last analysis just failed".to_owned());
    }
    if last_failed != NO_RECORD
        && average == NO_RECORD
        && last_failed < DEFAULT_FAILED_ANALYSIS_WAIT_TIME
    {
        return (
            false,
            format!(
                "last failed analysis duration is less than {}",
                format_go_duration(DEFAULT_FAILED_ANALYSIS_WAIT_TIME)
            ),
        );
    }
    if last_failed != NO_RECORD && last_failed < average.wrapping_mul(2) {
        return (
            false,
            "last failed analysis duration is less than 2 times the average analysis duration"
                .to_owned(),
        );
    }
    (true, String::new())
}

impl NonPartitionedTableAnalysisJob {
    fn validate_and_prepare<C: AnalysisJobContext + ?Sized>(
        &mut self,
        context: &C,
    ) -> ValidationResult {
        let (schema_name, table) = match context.lookup_table(self.table_id) {
            TableLookup::TableMissing => {
                return ValidationResult::invalid("table does not exist", false);
            }
            TableLookup::SchemaMissing => {
                return ValidationResult::invalid("schema does not exist", false);
            }
            TableLookup::Found { schema_name, table } => (schema_name, table),
        };
        self.schema_name = schema_name;
        self.table_name = table.name.original().to_owned();
        self.index_names = current_index_names(&table, &self.index_ids);
        validate_analysis_interval(context, &self.schema_name, &self.table_name, &[])
    }

    fn analyze<C: AnalysisJobContext + ?Sized>(&self, context: &C) -> bool {
        if self.index_ids.is_empty() {
            let (sql, arguments) = self.analyze_table_sql();
            return context.auto_analyze(
                self.table_stats_version,
                self.need_version_rewrite_warning,
                sql,
                &arguments,
            );
        }
        let Some(index) = self.index_names.first() else {
            return true;
        };
        let (sql, arguments) = self.analyze_index_sql(index);
        context.auto_analyze(
            self.table_stats_version,
            self.need_version_rewrite_warning,
            sql,
            &arguments,
        )
    }

    /// Go `GenSQLForAnalyzeTable`.
    pub fn analyze_table_sql(&self) -> (&'static str, Vec<String>) {
        (
            "analyze table %n.%n",
            vec![self.schema_name.clone(), self.table_name.clone()],
        )
    }

    /// Go `GenSQLForAnalyzeIndex`.
    pub fn analyze_index_sql(&self, index: &str) -> (&'static str, Vec<String>) {
        (
            "analyze table %n.%n index %n",
            vec![
                self.schema_name.clone(),
                self.table_name.clone(),
                index.to_owned(),
            ],
        )
    }
}

impl StaticPartitionedTableAnalysisJob {
    fn validate_and_prepare<C: AnalysisJobContext + ?Sized>(
        &mut self,
        context: &C,
    ) -> ValidationResult {
        let (schema_name, table) = match context.lookup_table(self.global_table_id) {
            TableLookup::TableMissing => {
                return ValidationResult::invalid("table does not exist", false);
            }
            TableLookup::SchemaMissing => {
                return ValidationResult::invalid("schema does not exist", false);
            }
            TableLookup::Found { schema_name, table } => (schema_name, table),
        };
        let Some(partition) = table.get_partition_info() else {
            return ValidationResult::invalid("table is not a partitioned table", false);
        };
        let partition_name = partition
            .read()
            .definitions
            .snapshot()
            .into_iter()
            .find_map(|partition| {
                (partition.id == self.static_partition_id)
                    .then(|| partition.name.original().to_owned())
            });
        let Some(partition_name) = partition_name else {
            return ValidationResult::invalid("partition does not exist", false);
        };
        self.schema_name = schema_name;
        self.global_table_name = table.name.original().to_owned();
        self.static_partition_name = partition_name;
        self.index_names = current_index_names(&table, &self.index_ids);
        validate_analysis_interval(
            context,
            &self.schema_name,
            &self.global_table_name,
            std::slice::from_ref(&self.static_partition_name),
        )
    }

    fn analyze<C: AnalysisJobContext + ?Sized>(&self, context: &C) -> bool {
        if self.index_ids.is_empty() {
            let (sql, arguments) = self.analyze_partition_sql();
            return context.auto_analyze(
                self.table_stats_version,
                self.need_version_rewrite_warning,
                sql,
                &arguments,
            );
        }
        let Some(index) = self.index_names.first() else {
            return true;
        };
        let (sql, arguments) = self.analyze_partition_index_sql(index);
        context.auto_analyze(
            self.table_stats_version,
            self.need_version_rewrite_warning,
            sql,
            &arguments,
        )
    }

    /// Go `GenSQLForAnalyzeStaticPartition`.
    pub fn analyze_partition_sql(&self) -> (&'static str, Vec<String>) {
        (
            "analyze table %n.%n partition %n",
            vec![
                self.schema_name.clone(),
                self.global_table_name.clone(),
                self.static_partition_name.clone(),
            ],
        )
    }

    /// Go `GenSQLForAnalyzeStaticPartitionIndex`.
    pub fn analyze_partition_index_sql(&self, index: &str) -> (&'static str, Vec<String>) {
        (
            "analyze table %n.%n partition %n index %n",
            vec![
                self.schema_name.clone(),
                self.global_table_name.clone(),
                self.static_partition_name.clone(),
                index.to_owned(),
            ],
        )
    }
}

impl DynamicPartitionedTableAnalysisJob {
    fn validate_and_prepare<C: AnalysisJobContext + ?Sized>(
        &mut self,
        context: &C,
    ) -> ValidationResult {
        let (schema_name, table) = match context.lookup_table(self.global_table_id) {
            TableLookup::TableMissing => {
                return ValidationResult::invalid("table does not exist", false);
            }
            TableLookup::SchemaMissing => {
                return ValidationResult::invalid("schema does not exist", false);
            }
            TableLookup::Found { schema_name, table } => (schema_name, table),
        };
        let Some(partition) = table.get_partition_info() else {
            return ValidationResult::invalid("table is not a partitioned table", false);
        };
        let definitions = partition.read().definitions.snapshot();
        let partition_names_by_id = definitions
            .iter()
            .map(|partition| (partition.id, partition.name.original().to_owned()))
            .collect::<HashMap<_, _>>();
        self.partition_names = definitions
            .iter()
            .filter(|partition| self.partition_ids.contains(&partition.id))
            .map(|partition| partition.name.original().to_owned())
            .collect();
        self.partition_index_names = HashMap::with_capacity(self.partition_index_ids.len());
        for shared_index in table.indices.iter_deref() {
            let index = shared_index.read();
            let Some(partition_ids) = self.partition_index_ids.get(&index.id) else {
                continue;
            };
            let names = partition_ids
                .iter()
                .filter_map(|partition_id| partition_names_by_id.get(partition_id).cloned())
                .collect::<Vec<_>>();
            if !names.is_empty() {
                self.partition_index_names
                    .insert(index.name.original().to_owned(), names);
            }
        }
        self.schema_name = schema_name;
        self.global_table_name = table.name.original().to_owned();
        if self.partition_names.is_empty() && self.partition_index_names.is_empty() {
            return ValidationResult::valid();
        }
        let mut all_partitions = self.partition_names.clone();
        all_partitions.extend(self.partition_index_names.values().flatten().cloned());
        validate_analysis_interval(
            context,
            &self.schema_name,
            &self.global_table_name,
            &all_partitions,
        )
    }

    fn analyze<C: AnalysisJobContext + ?Sized>(&self, context: &C) -> bool {
        let partition_batch_size = context.auto_analyze_partition_batch_size();
        assert_ne!(partition_batch_size, 0, "auto analyze partition batch size");
        if self.partition_index_ids.is_empty() {
            for partitions in self.partition_names.chunks(partition_batch_size) {
                let sql = partition_sql("analyze table %n.%n partition", "", partitions.len());
                let mut arguments = vec![self.schema_name.clone(), self.global_table_name.clone()];
                arguments.extend_from_slice(partitions);
                if !context.auto_analyze(
                    self.table_stats_version,
                    self.need_version_rewrite_warning,
                    &sql,
                    &arguments,
                ) {
                    return false;
                }
            }
            return true;
        }
        let Some((index, partitions)) = self.partition_index_names.iter().next() else {
            return false;
        };
        for partitions in partitions.chunks(partition_batch_size) {
            let sql = partition_sql(
                "analyze table %n.%n partition",
                " index %n",
                partitions.len(),
            );
            let mut arguments = vec![self.schema_name.clone(), self.global_table_name.clone()];
            arguments.extend_from_slice(partitions);
            arguments.push(index.clone());
            if !context.auto_analyze(
                self.table_stats_version,
                self.need_version_rewrite_warning,
                &sql,
                &arguments,
            ) {
                return false;
            }
        }
        true
    }
}

fn current_index_names(table: &TableInfo, index_ids: &HashSet<i64>) -> Vec<String> {
    table
        .indices
        .iter_deref()
        .filter_map(|index| {
            let index = index.read();
            index_ids
                .contains(&index.id)
                .then(|| index.name.original().to_owned())
        })
        .collect()
}

fn validate_analysis_interval<C: AnalysisJobContext + ?Sized>(
    context: &C,
    schema: &str,
    table: &str,
    partitions: &[String],
) -> ValidationResult {
    let last_failed = match context.last_failed_analysis_duration(schema, table, partitions) {
        Ok(duration) => duration,
        Err(error) => {
            return ValidationResult::invalid(
                format!("fail to get last failed analysis duration: {error}"),
                true,
            );
        }
    };
    let average = match context.average_analysis_duration(schema, table, partitions) {
        Ok(duration) => duration,
        Err(error) => {
            return ValidationResult::invalid(
                format!("fail to get average analysis duration: {error}"),
                true,
            );
        }
    };
    let (valid, reason) = valid_to_analyze(last_failed, average);
    if valid {
        ValidationResult::valid()
    } else {
        ValidationResult::invalid(reason, true)
    }
}

/// Go `getPartitionSQL`.
pub fn partition_sql(prefix: &str, suffix: &str, partition_count: usize) -> String {
    let mut sql = prefix.to_owned();
    for index in 0..partition_count {
        if index != 0 {
            sql.push(',');
        }
        sql.push_str(" %n");
    }
    sql.push_str(suffix);
    sql
}

fn format_go_duration(duration: i64) -> String {
    if duration == 0 {
        return "0s".to_owned();
    }
    let negative = duration.is_negative();
    let nanos = duration.unsigned_abs();
    if nanos < 1_000 {
        return format!("{}{nanos}ns", if negative { "-" } else { "" });
    }
    let decimal_unit = |unit: u64, suffix: &str| {
        let whole = nanos / unit;
        let remainder = nanos % unit;
        let sign = if negative { "-" } else { "" };
        if remainder == 0 {
            return format!("{sign}{whole}{suffix}");
        }
        let width = unit.ilog10() as usize;
        let mut fraction = format!("{remainder:0width$}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        format!("{sign}{whole}.{fraction}{suffix}")
    };
    if nanos < 1_000_000 {
        return decimal_unit(1_000, "µs");
    }
    if nanos < 1_000_000_000 {
        return decimal_unit(1_000_000, "ms");
    }
    let seconds = nanos / 1_000_000_000;
    let subsecond = nanos % 1_000_000_000;
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;
    let mut output = if negative {
        "-".to_owned()
    } else {
        String::new()
    };
    if hours > 0 {
        output.push_str(&format!("{hours}h"));
    }
    if minutes > 0 || hours > 0 {
        output.push_str(&format!("{minutes}m"));
    }
    if subsecond == 0 {
        output.push_str(&format!("{secs}s"));
    } else {
        let mut fraction = format!("{subsecond:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        output.push_str(&format!("{secs}.{fraction}s"));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;
    use std::sync::RwLock;
    use tidb_ast::CiString;
    use tidb_model::go_runtime::{GoShared, GoSharedPointerSlice, GoSharedSlice};
    use tidb_model::{IndexInfo, PartitionDefinition, PartitionInfo};
    use tidb_stats::{ColAndIdxExistenceMap, HistColl};

    fn job(table_id: i64, weight: f64) -> AnalysisJob {
        AnalysisJob::NonPartitioned(NonPartitionedTableAnalysisJob {
            table_id,
            weight,
            ..NonPartitionedTableAnalysisJob::default()
        })
    }

    fn table_info(id: i64, name: &str, partitions: &[(i64, &str)]) -> Arc<TableInfo> {
        let mut table = TableInfo {
            id,
            name: CiString::new(name),
            ..TableInfo::default()
        };
        if !partitions.is_empty() {
            table.partition = Some(GoShared::new(PartitionInfo {
                enable: true,
                definitions: GoSharedSlice::from_vec(
                    partitions
                        .iter()
                        .map(|(id, name)| PartitionDefinition {
                            id: *id,
                            name: CiString::new(*name),
                            ..PartitionDefinition::default()
                        })
                        .collect(),
                ),
                ..PartitionInfo::default()
            }));
        }
        Arc::new(table)
    }

    fn stats(physical_id: i64, version: u64, count: i64, modify: i64) -> Table {
        let mut existence = ColAndIdxExistenceMap::new(1, 0);
        existence.insert_column(1, true);
        Table {
            existence_map: Some(Arc::new(RwLock::new(existence))),
            hist_coll: HistColl::new(physical_id, count, modify, 0, 0),
            version,
            last_analyze_version: 1,
            last_stats_hist_version: 1,
            table_info_update_ts: 0,
            is_pk_handle: false,
        }
    }

    fn inventory(tables: Vec<InventoryTable>, prune_mode: PartitionPruneMode) -> QueueInventory {
        QueueInventory {
            tables,
            locked_table_ids: HashSet::new(),
            prune_mode,
            auto_analyze_ratio: 0.5,
            requested_version: 2,
            current_ts: 10 << 18,
            auto_analyze_min_count: 0,
        }
    }

    struct MockJobContext {
        inventory: Mutex<Option<QueueInventory>>,
        next_check_version: std::sync::atomic::AtomicU64,
        lookup: Mutex<Option<TableLookup>>,
        last_failed: Mutex<Result<i64, String>>,
        average: Mutex<Result<i64, String>>,
        calls: Mutex<Vec<(i32, bool, String, Vec<String>)>>,
        succeed: Mutex<bool>,
        source_calls: Mutex<Vec<&'static str>>,
    }

    impl Default for MockJobContext {
        fn default() -> Self {
            Self {
                inventory: Mutex::new(None),
                next_check_version: std::sync::atomic::AtomicU64::new(10),
                lookup: Mutex::new(None),
                last_failed: Mutex::new(Ok(NO_RECORD)),
                average: Mutex::new(Ok(NO_RECORD)),
                calls: Mutex::new(Vec::new()),
                succeed: Mutex::new(false),
                source_calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl PriorityQueueSource for MockJobContext {
        fn next_check_version_with_offset(&self) -> u64 {
            self.source_calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push("version");
            self.next_check_version
                .load(std::sync::atomic::Ordering::Relaxed)
        }

        fn queue_inventory(&self) -> Result<QueueInventory, String> {
            self.source_calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push("inventory");
            self.inventory
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .ok_or_else(|| "inventory unavailable".to_owned())
        }
    }

    impl AnalysisJobContext for MockJobContext {
        fn lookup_table(&self, _table_id: i64) -> TableLookup {
            self.lookup
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .unwrap_or(TableLookup::TableMissing)
        }

        fn last_failed_analysis_duration(
            &self,
            _schema: &str,
            _table: &str,
            _partitions: &[String],
        ) -> Result<i64, String> {
            self.last_failed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        fn average_analysis_duration(
            &self,
            _schema: &str,
            _table: &str,
            _partitions: &[String],
        ) -> Result<i64, String> {
            self.average
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        fn auto_analyze(
            &self,
            stats_version: i32,
            warning: bool,
            sql: &str,
            arguments: &[String],
        ) -> bool {
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push((stats_version, warning, sql.to_owned(), arguments.to_vec()));
            *self
                .succeed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }

        fn auto_analyze_partition_batch_size(&self) -> usize {
            2
        }
    }

    #[test]
    #[deny(unused_must_use)]
    fn go_priority_queue_returns_can_be_ignored() {
        let context = Arc::new(MockJobContext::default());
        let analysis_job = job(1, 1.0);
        analysis_job.table_id();
        analysis_job.weight();
        analysis_job.indicators();
        analysis_job.has_newly_added_index();
        analysis_job.is_dynamic_partitioned();
        analysis_job.as_json();
        analysis_job.analyze(context.as_ref());

        let calculator = PriorityCalculator;
        calculator.calculate_weight(&analysis_job);
        calculator.special_event(&analysis_job);

        JobHeap::new();
        let heap = JobHeap::new();
        heap.is_empty();
        heap.len();

        AnalysisPriorityQueue::new(context.clone());
        let queue = AnalysisPriorityQueue::new(context.clone());
        queue.is_initialized();
        queue.running_jobs();
        let running = RunningAnalysisJob {
            job: job(2, 1.0),
            queue: Arc::downgrade(&queue),
        };
        running.table_id();
        running.analyze();

        AnalysisJobFactory::new(0.5, 10 << 18, 2, 0);
        let factory = AnalysisJobFactory::new(0.5, 10 << 18, 2, 0);
        let table_stats = stats(3, 5, 100, 60);
        factory.change_percentage(&table_stats);
        factory.last_analysis_duration(&table_stats);
        factory.table_size(&table_stats);
        factory.partition_indicators(&table_stats, &HashMap::new());

        let start = DateTime::parse_from_rfc3339("1970-01-01T22:00:00+00:00").unwrap();
        let end = DateTime::parse_from_rfc3339("1970-01-01T06:00:00+00:00").unwrap();
        AutoAnalysisTimeWindow::new(start, end);
        let window = AutoAnalysisTimeWindow::new(start, end);
        window.is_within_time_window(Utc::now());
        PartitionIdAndName::new("p0", 3);

        average_analysis_duration(Some(1.0));
        last_failed_analysis_duration(Some(1));
        valid_to_analyze(NO_RECORD, NO_RECORD);

        let ordinary = NonPartitionedTableAnalysisJob::default();
        ordinary.analyze_table_sql();
        ordinary.analyze_index_sql("idx");
        let static_partition = StaticPartitionedTableAnalysisJob::default();
        static_partition.analyze_partition_sql();
        static_partition.analyze_partition_index_sql("idx");
        partition_sql("analyze table %n.%n partition", "", 1);
    }

    #[test]
    fn source_heap_add_update_delete_peek_and_pop() {
        let mut heap = JobHeap::new();
        for item in [job(1, 10.0), job(2, 1.0), job(3, 11.0), job(4, 30.0)] {
            heap.add_or_update(item);
        }
        heap.add_or_update(job(1, 13.0));
        assert_eq!(heap.peek().unwrap().table_id(), 4);
        assert_eq!(heap.pop().unwrap().table_id(), 4);
        assert_eq!(heap.pop().unwrap().table_id(), 1);
        heap.delete(3).unwrap();
        heap.add_or_update(job(1, 14.0));
        assert_eq!(heap.pop().unwrap().table_id(), 1);
        assert_eq!(heap.pop().unwrap().table_id(), 2);
        assert_eq!(heap.pop().unwrap_err(), QueueError::HeapIsEmpty);
        assert_eq!(heap.delete(99).unwrap_err(), QueueError::ObjectNotFound);
    }

    #[test]
    fn source_priority_calculator_orders_each_indicator() {
        let calculator = PriorityCalculator;
        let weight = |change, size, duration: i64| {
            let mut value = job(1, 0.0);
            value.set_indicators(Indicators {
                change_percentage: change,
                table_size: size,
                last_analysis_duration: duration,
            });
            calculator.calculate_weight(&value)
        };
        let hour = 3_600_000_000_000;
        assert!(weight(0.6, 1_000.0, hour) < weight(1.0, 1_000.0, hour));
        assert!(weight(0.6, 100_000.0, hour) < weight(0.6, 1_000.0, hour));
        assert!(weight(0.6, 1_000.0, hour) < weight(0.6, 1_000.0, 24 * hour));
    }

    /// Pinned `calculatoranalysis.TestPriorityCalculatorWithGeneratedData`:
    /// all 690 realistic size/change/elapsed-time combinations are stable
    /// sorted and byte-compared with the original package fixture.
    #[test]
    fn source_priority_calculator_matches_complete_golden_matrix() {
        const BASE_CHANGE_RATE: f64 = 0.001;
        const CHANGE_RATE_DECAY_LOG: f64 = 3.0;
        const SMALL_TABLE_THRESHOLD: i64 = 100_000;
        const MAX_CHANGE_PERCENTAGE: f64 = 3.0;

        let table_sizes = [
            1_000_i64,
            5_000,
            10_000,
            50_000,
            100_000,
            500_000,
            1_000_000,
            5_000_000,
            10_000_000,
            50_000_000,
            100_000_000,
        ];
        let analyze_times = [
            10_i64, 60, 300, 900, 1_800, 3_600, 7_200, 14_400, 28_800, 43_200, 86_400, 172_800,
            259_200,
        ];
        let mut rows = Vec::new();
        let mut id = 1_i64;
        for table_size in table_sizes {
            for elapsed_seconds in analyze_times {
                let change_rate = if table_size < SMALL_TABLE_THRESHOLD {
                    BASE_CHANGE_RATE
                } else {
                    BASE_CHANGE_RATE
                        * 0.5_f64.powf((table_size as f64).log10() / CHANGE_RATE_DECAY_LOG)
                };
                let max_change = ((table_size as f64) * change_rate * (elapsed_seconds as f64))
                    .min((table_size as f64) * MAX_CHANGE_PERCENTAGE)
                    as i64;
                for changes in [
                    max_change / 10,
                    max_change / 5,
                    max_change / 2,
                    max_change,
                    max_change * 2,
                    max_change * 3,
                ] {
                    if changes <= 0 || changes > table_size * 3 {
                        continue;
                    }
                    let mut analysis_job = job(id, 0.0);
                    analysis_job.set_indicators(Indicators {
                        change_percentage: (changes as f64) / (table_size as f64),
                        table_size: table_size as f64,
                        last_analysis_duration: elapsed_seconds * 1_000_000_000,
                    });
                    rows.push((
                        id,
                        PriorityCalculator.calculate_weight(&analysis_job),
                        table_size,
                        changes,
                        elapsed_seconds,
                    ));
                    id += 1;
                }
            }
        }
        rows.sort_by(|left, right| right.1.total_cmp(&left.1));

        let mut actual = String::from(
            "ID,CalculatedPriority,TableSize,Changes,TimeSinceLastAnalyze,ChangeRatio\n",
        );
        for (id, priority, table_size, changes, elapsed_seconds) in rows {
            writeln!(
                actual,
                "{id},{priority:.4},{table_size},{changes},{elapsed_seconds},{:.4}",
                (changes as f64) / (table_size as f64)
            )
            .expect("writing to a String cannot fail");
        }
        assert_eq!(
            actual,
            include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../../pkg/statistics/handle/autoanalyze/priorityqueue/",
                "calculatoranalysis/testdata/calculated_priorities.golden.csv"
            ))
        );
    }

    #[test]
    fn source_sql_and_json_shapes() {
        let ordinary = NonPartitionedTableAnalysisJob {
            table_id: 1,
            schema_name: "test".to_owned(),
            table_name: "t".to_owned(),
            indicators: Indicators {
                change_percentage: 0.5,
                table_size: 100.0,
                last_analysis_duration: 1_800_000_000_000,
            },
            ..NonPartitionedTableAnalysisJob::default()
        };
        assert_eq!(
            ordinary.analyze_table_sql(),
            (
                "analyze table %n.%n",
                vec!["test".to_owned(), "t".to_owned()]
            )
        );
        let json = AnalysisJob::NonPartitioned(ordinary).as_json();
        assert_eq!(json.kind, "analyzeTable");
        assert_eq!(json.indicators.change_percentage, "50.00%");
        assert_eq!(json.indicators.last_analysis_duration, "30m0s");
        let json_value = |weight| {
            let mut partition_index_ids = HashMap::new();
            partition_index_ids.insert(2, vec![22]);
            partition_index_ids.insert(10, vec![101]);
            AnalysisJobJson {
                kind: "analyzeTable".to_owned(),
                table_id: 1,
                weight,
                partition_ids: Vec::new(),
                index_ids: Vec::new(),
                partition_index_ids,
                indicators: IndicatorsJson {
                    change_percentage: "0.00%".to_owned(),
                    table_size: "0.00".to_owned(),
                    last_analysis_duration: "0s".to_owned(),
                },
                has_newly_added_index: true,
            }
        };
        for _ in 0..64 {
            let encoded = serde_json::to_string(&json_value(1.0)).unwrap();
            assert_eq!(
                encoded,
                "{\"type\":\"analyzeTable\",\"table_id\":1,\"weight\":1,\"partition_ids\":[],\"index_ids\":[],\"partition_index_ids\":{\"10\":[101],\"2\":[22]},\"indicators\":{\"change_percentage\":\"0.00%\",\"table_size\":\"0.00\",\"last_analysis_duration\":\"0s\"},\"has_newly_added_index\":true}",
                "Go encoding/json preserves declaration order, emits integral floats without `.0`, and sorts integer map keys by decimal text"
            );
        }
        let encoded_weight = |weight| {
            let encoded = serde_json::to_string(&json_value(weight)).unwrap();
            encoded
                .split_once("\"weight\":")
                .unwrap()
                .1
                .split_once(',')
                .unwrap()
                .0
                .to_owned()
        };
        for (weight, expected) in [
            (-0.0, "-0"),
            (0.0, "0"),
            (1e-6, "0.000001"),
            (1e-7, "1e-7"),
            (1e20, "100000000000000000000"),
            (1e21, "1e+21"),
            (f64::MAX, "1.7976931348623157e+308"),
        ] {
            assert_eq!(encoded_weight(weight), expected);
        }
        for weight in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(serde_json::to_string(&json_value(weight)).is_err());
        }
        assert_eq!(
            partition_sql("analyze table %n.%n partition", " index %n", 2),
            "analyze table %n.%n partition %n, %n index %n"
        );
    }

    #[test]
    fn source_interval_sentinels_and_failure_gate() {
        assert_eq!(average_analysis_duration(None), NO_RECORD);
        assert_eq!(average_analysis_duration(Some(-1.0)), NO_RECORD);
        assert_eq!(average_analysis_duration(Some(1.75)), 1_000_000_000);
        assert_eq!(last_failed_analysis_duration(None), NO_RECORD);
        assert_eq!(last_failed_analysis_duration(Some(0)), JUST_FAILED);
        assert_eq!(
            last_failed_analysis_duration(Some(-1)),
            DEFAULT_FAILED_ANALYSIS_WAIT_TIME
        );
        assert_eq!(
            valid_to_analyze(JUST_FAILED, NO_RECORD),
            (false, "last analysis just failed".to_owned())
        );
        assert_eq!(
            valid_to_analyze(60_000_000_000, NO_RECORD).1,
            "last failed analysis duration is less than 30m0s"
        );
        assert_eq!(
            valid_to_analyze(60_000_000_000, 40_000_000_000).1,
            "last failed analysis duration is less than 2 times the average analysis duration"
        );
        assert_eq!(
            valid_to_analyze(NO_RECORD, NO_RECORD),
            (true, String::new())
        );
    }

    #[test]
    fn source_queue_initialization_dml_retry_and_ddl_gate() {
        let table = table_info(1, "t", &[]);
        let mut image = inventory(
            vec![InventoryTable {
                schema_name: "test".to_owned(),
                table: Arc::clone(&table),
                global_stats: Some(stats(1, 5, 100, 60)),
                partition_stats: HashMap::new(),
            }],
            PartitionPruneMode::Dynamic,
        );
        let source = Arc::new(MockJobContext {
            inventory: Mutex::new(Some(image.clone())),
            next_check_version: std::sync::atomic::AtomicU64::new(10),
            succeed: Mutex::new(true),
            ..MockJobContext::default()
        });
        let queue = AnalysisPriorityQueue::new(source.clone());
        assert_eq!(queue.len().unwrap_err(), QueueError::NotInitialized);
        assert_eq!(
            queue
                .handle_ddl_event(true, &PriorityQueueDdlEvent::Other)
                .unwrap_err(),
            DdlHandleError::NotReadyRetryLater
        );
        assert!(queue
            .handle_ddl_event(false, &PriorityQueueDdlEvent::Other)
            .is_ok());

        queue.initialize().unwrap();
        assert_eq!(queue.len().unwrap(), 1);
        let running = queue.pop().unwrap();
        assert_eq!(running.table_id(), 1);
        assert_eq!(queue.running_jobs(), HashSet::from([1]));

        image.tables[0].global_stats = Some(stats(1, 11, 100, 70));
        source
            .next_check_version
            .store(12, std::sync::atomic::Ordering::Relaxed);
        *source
            .inventory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(image.clone());
        queue.process_dml_changes().unwrap();
        assert!(queue.is_empty().unwrap());
        assert_eq!(queue.snapshot().unwrap().must_retry_tables, vec![1]);

        assert!(running.analyze());
        queue.requeue_must_retry_jobs().unwrap();
        assert_eq!(queue.len().unwrap(), 1);
        queue
            .handle_ddl_event(
                true,
                &PriorityQueueDdlEvent::DropTable {
                    dropped_ids: vec![1],
                },
            )
            .unwrap();
        assert!(queue.is_empty().unwrap());
        queue.close();
        assert_eq!(queue.len().unwrap_err(), QueueError::NotInitialized);
    }

    #[test]
    fn source_queue_static_lock_and_system_view_filters() {
        let partitioned = table_info(10, "pt", &[(11, "p0"), (12, "p1")]);
        let mut view = (*table_info(20, "v", &[])).clone();
        view.view = Some(GoShared::new(Default::default()));
        let mut image = inventory(
            vec![
                InventoryTable {
                    schema_name: "test".to_owned(),
                    table: partitioned,
                    global_stats: Some(stats(10, 5, 200, 150)),
                    partition_stats: HashMap::from([
                        (PartitionIdAndName::new("p0", 11), stats(11, 5, 100, 60)),
                        (PartitionIdAndName::new("p1", 12), stats(12, 5, 100, 60)),
                    ]),
                },
                InventoryTable {
                    schema_name: "test".to_owned(),
                    table: Arc::new(view),
                    global_stats: Some(stats(20, 5, 100, 60)),
                    partition_stats: HashMap::new(),
                },
                InventoryTable {
                    schema_name: "mysql".to_owned(),
                    table: table_info(30, "sys", &[]),
                    global_stats: Some(stats(30, 5, 100, 60)),
                    partition_stats: HashMap::new(),
                },
            ],
            PartitionPruneMode::Static,
        );
        image.locked_table_ids.insert(10);
        let source = Arc::new(MockJobContext {
            inventory: Mutex::new(Some(image.clone())),
            ..MockJobContext::default()
        });
        let queue = AnalysisPriorityQueue::new(source.clone());
        queue.initialize().unwrap();
        assert!(queue.is_empty().unwrap());
        queue.close();

        image.locked_table_ids = HashSet::from([12]);
        *source
            .inventory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(image);
        queue.initialize().unwrap();
        assert_eq!(queue.len().unwrap(), 1);
        assert_eq!(queue.peek().unwrap().table_id(), 11);
    }

    #[test]
    fn source_validate_prepare_and_dynamic_batch_execution() {
        let mut table = (*table_info(10, "pt", &[(11, "p0"), (12, "p1"), (13, "p2")])).clone();
        table.indices = GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(IndexInfo {
            id: 7,
            name: CiString::new("idx"),
            state: SchemaState::PUBLIC,
            ..IndexInfo::default()
        }))]);
        let context = MockJobContext {
            lookup: Mutex::new(Some(TableLookup::Found {
                schema_name: "test".to_owned(),
                table: Arc::new(table),
            })),
            last_failed: Mutex::new(Ok(NO_RECORD)),
            average: Mutex::new(Ok(NO_RECORD)),
            succeed: Mutex::new(true),
            ..MockJobContext::default()
        };
        let mut job = AnalysisJob::DynamicPartitioned(DynamicPartitionedTableAnalysisJob {
            global_table_id: 10,
            partition_ids: HashSet::from([11, 12, 13]),
            table_stats_version: 2,
            need_version_rewrite_warning: true,
            ..DynamicPartitionedTableAnalysisJob::default()
        });
        assert_eq!(
            job.validate_and_prepare(&context),
            ValidationResult::valid()
        );
        assert!(job.analyze(&context));
        let calls = context
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].2, "analyze table %n.%n partition %n, %n");
        assert_eq!(calls[0].3, ["test", "pt", "p0", "p1"]);
        assert_eq!(calls[1].3, ["test", "pt", "p2"]);
    }

    #[test]
    fn source_auto_analysis_time_window_is_utc_minute_inclusive() {
        let start = DateTime::parse_from_rfc3339("1970-01-01T22:00:00+00:00").unwrap();
        let end = DateTime::parse_from_rfc3339("1970-01-01T06:00:00+00:00").unwrap();
        let window = AutoAnalysisTimeWindow::new(start, end);
        let at = |hour| {
            DateTime::parse_from_rfc3339(&format!("2026-01-01T{hour:02}:00:00+00:00"))
                .unwrap()
                .with_timezone(&Utc)
        };
        assert!(window.is_within_time_window(at(22)));
        assert!(window.is_within_time_window(at(6)));
        assert!(!window.is_within_time_window(at(12)));
        assert!(!AutoAnalysisTimeWindow::default().is_within_time_window(at(1)));
    }

    #[test]
    fn source_captures_dml_watermark_before_each_scan() {
        let source = Arc::new(MockJobContext {
            inventory: Mutex::new(Some(inventory(Vec::new(), PartitionPruneMode::Dynamic))),
            ..MockJobContext::default()
        });
        let queue = AnalysisPriorityQueue::new(source.clone());
        queue.initialize().unwrap();
        assert_eq!(
            *source
                .source_calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            ["version", "inventory"]
        );
        source
            .source_calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        queue.process_dml_changes().unwrap();
        assert_eq!(
            *source
                .source_calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            ["version", "inventory"]
        );
        queue.close();
    }

    #[test]
    fn source_static_retry_recreates_the_whole_table() {
        let table = table_info(10, "pt", &[(11, "p0"), (12, "p1")]);
        let image = inventory(
            vec![InventoryTable {
                schema_name: "test".to_owned(),
                table,
                global_stats: Some(stats(10, 5, 200, 150)),
                partition_stats: HashMap::from([
                    (PartitionIdAndName::new("p0", 11), stats(11, 5, 100, 60)),
                    (PartitionIdAndName::new("p1", 12), stats(12, 5, 100, 70)),
                ]),
            }],
            PartitionPruneMode::Static,
        );
        let source = Arc::new(MockJobContext {
            inventory: Mutex::new(Some(image)),
            ..MockJobContext::default()
        });
        let queue = AnalysisPriorityQueue::new(source.clone());
        queue.initialize().unwrap();
        let failed = queue.pop().unwrap();
        let completed = queue.pop().unwrap();
        assert!(!failed.analyze());
        *source
            .succeed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        assert!(completed.analyze());
        assert!(queue.is_empty().unwrap());
        queue.requeue_must_retry_jobs().unwrap();
        assert_eq!(queue.len().unwrap(), 2);
        assert_eq!(
            queue
                .snapshot()
                .unwrap()
                .current_jobs
                .iter()
                .map(|job| job.table_id)
                .collect::<HashSet<_>>(),
            HashSet::from([11, 12])
        );
        queue.close();
    }

    #[test]
    fn source_concurrent_close_waits_for_one_worker_reset() {
        let source = Arc::new(MockJobContext {
            inventory: Mutex::new(Some(inventory(Vec::new(), PartitionPruneMode::Dynamic))),
            ..MockJobContext::default()
        });
        let queue = AnalysisPriorityQueue::new(source);
        queue.initialize().unwrap();
        let threads = (0..8)
            .map(|_| {
                let queue = Arc::clone(&queue);
                std::thread::spawn(move || queue.close())
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(!queue.is_initialized());
        assert_eq!(queue.len().unwrap_err(), QueueError::NotInitialized);
    }

    #[test]
    fn source_rebuild_failure_keeps_go_empty_initialized_queue() {
        let source = Arc::new(MockJobContext {
            inventory: Mutex::new(Some(inventory(
                vec![InventoryTable {
                    schema_name: "test".to_owned(),
                    table: table_info(1, "t", &[]),
                    global_stats: Some(stats(1, 5, 100, 60)),
                    partition_stats: HashMap::new(),
                }],
                PartitionPruneMode::Dynamic,
            ))),
            ..MockJobContext::default()
        });
        let queue = AnalysisPriorityQueue::new(source.clone());
        queue.initialize().unwrap();
        assert_eq!(queue.len().unwrap(), 1);
        *source
            .inventory
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        assert_eq!(
            queue.rebuild().unwrap_err(),
            QueueRuntimeError::Source("inventory unavailable".to_owned())
        );
        assert!(queue.is_initialized());
        assert!(queue.is_empty().unwrap());
        queue.close();
    }

    #[test]
    fn source_ddl_partition_and_table_event_matrix() {
        let table = table_info(10, "pt", &[(11, "p0"), (12, "p1")]);
        let source = Arc::new(MockJobContext {
            inventory: Mutex::new(Some(inventory(
                vec![InventoryTable {
                    schema_name: "test".to_owned(),
                    table,
                    global_stats: Some(stats(10, 5, 200, 150)),
                    partition_stats: HashMap::from([
                        (PartitionIdAndName::new("p0", 11), stats(11, 5, 100, 60)),
                        (PartitionIdAndName::new("p1", 12), stats(12, 5, 100, 60)),
                    ]),
                }],
                PartitionPruneMode::Dynamic,
            ))),
            ..MockJobContext::default()
        });
        let queue = AnalysisPriorityQueue::new(source.clone());
        queue.initialize().unwrap();
        assert_eq!(queue.peek().unwrap().table_id(), 10);
        queue
            .handle_ddl_event(
                true,
                &PriorityQueueDdlEvent::DropPartition {
                    table_id: 10,
                    dropped_partition_ids: vec![11],
                },
            )
            .unwrap();
        assert_eq!(queue.len().unwrap(), 1);
        queue
            .handle_ddl_event(
                true,
                &PriorityQueueDdlEvent::TruncatePartition {
                    table_id: 10,
                    dropped_partition_ids: vec![12],
                },
            )
            .unwrap();
        assert_eq!(queue.len().unwrap(), 1);
        queue
            .handle_ddl_event(
                true,
                &PriorityQueueDdlEvent::DropSchema {
                    dropped_ids: vec![10, 11, 12],
                },
            )
            .unwrap();
        assert!(queue.is_empty().unwrap());

        queue.rebuild().unwrap();
        let mut running = queue.pop().unwrap();
        if let AnalysisJob::DynamicPartitioned(job) = &mut running.job {
            job.schema_name = "test".to_owned();
            job.global_table_name = "pt".to_owned();
            job.partition_names = vec!["p0".to_owned()];
        }
        queue
            .handle_ddl_event(
                true,
                &PriorityQueueDdlEvent::AddIndex {
                    table_id: 10,
                    analyzed: false,
                },
            )
            .unwrap();
        assert_eq!(queue.snapshot().unwrap().must_retry_tables, [10]);
        assert!(!running.analyze());
        queue.close();
    }

    #[test]
    fn source_factory_matches_ratio_index_version_and_partition_rules() {
        let mut table = (*table_info(1, "t", &[])).clone();
        table.indices = GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(IndexInfo {
            id: 7,
            name: CiString::new("idx"),
            state: SchemaState::PUBLIC,
            ..IndexInfo::default()
        }))]);
        let mut table_stats = stats(1, 5, 100, 40);
        table_stats.hist_coll.stats_version = 1;
        let disabled = AnalysisJobFactory::new(0.0, 10 << 18, 2, 0);
        let job = disabled
            .create_non_partitioned(&table, &table_stats)
            .unwrap();
        assert_eq!(job.indicators().change_percentage, 0.0);
        assert_eq!(job.as_json().index_ids, [7]);
        assert!(matches!(
            job,
            AnalysisJob::NonPartitioned(NonPartitionedTableAnalysisJob {
                need_version_rewrite_warning: true,
                table_stats_version: 2,
                ..
            })
        ));

        table.indices = GoSharedPointerSlice::default();
        assert!(disabled
            .create_non_partitioned(&table, &table_stats)
            .is_none());
        table_stats.last_analyze_version = 0;
        let unanalyzed = disabled
            .create_non_partitioned(&table, &table_stats)
            .unwrap();
        assert_eq!(unanalyzed.indicators().change_percentage, 1.0);
        assert_eq!(
            unanalyzed.indicators().last_analysis_duration,
            UNANALYZED_LAST_ANALYSIS_DURATION
        );

        let threshold = AnalysisJobFactory::new(0.4, 10 << 18, 2, 0);
        table_stats.last_analyze_version = 1;
        assert!(threshold
            .create_non_partitioned(&table, &table_stats)
            .is_none());
        table_stats.hist_coll.modify_count = 41;
        assert_eq!(
            threshold
                .create_non_partitioned(&table, &table_stats)
                .unwrap()
                .indicators()
                .change_percentage,
            0.41
        );
    }

    #[test]
    fn source_validation_reasons_and_retry_flags_match_go() {
        let context = MockJobContext::default();
        let mut ordinary = job(1, 0.0);
        assert_eq!(
            ordinary.validate_and_prepare(&context),
            ValidationResult::invalid("table does not exist", false)
        );
        *context
            .lookup
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(TableLookup::SchemaMissing);
        assert_eq!(
            ordinary.validate_and_prepare(&context),
            ValidationResult::invalid("schema does not exist", false)
        );

        let plain = table_info(1, "t", &[]);
        *context
            .lookup
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(TableLookup::Found {
            schema_name: "test".to_owned(),
            table: plain,
        });
        let mut static_job = AnalysisJob::StaticPartitioned(StaticPartitionedTableAnalysisJob {
            global_table_id: 1,
            static_partition_id: 2,
            ..StaticPartitionedTableAnalysisJob::default()
        });
        assert_eq!(
            static_job.validate_and_prepare(&context),
            ValidationResult::invalid("table is not a partitioned table", false)
        );

        let partitioned = table_info(1, "pt", &[(2, "p0")]);
        *context
            .lookup
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(TableLookup::Found {
            schema_name: "test".to_owned(),
            table: partitioned,
        });
        if let AnalysisJob::StaticPartitioned(job) = &mut static_job {
            job.static_partition_id = 3;
        }
        assert_eq!(
            static_job.validate_and_prepare(&context),
            ValidationResult::invalid("partition does not exist", false)
        );

        *context
            .last_failed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Err("read failed".to_owned());
        let mut ordinary = job(1, 0.0);
        assert_eq!(
            ordinary.validate_and_prepare(&context),
            ValidationResult::invalid(
                "fail to get last failed analysis duration: read failed",
                true,
            )
        );
    }

    #[test]
    fn source_analysis_stops_on_failure_and_uses_only_first_index() {
        let context = MockJobContext::default();
        let ordinary = AnalysisJob::NonPartitioned(NonPartitionedTableAnalysisJob {
            index_ids: HashSet::from([1, 2]),
            schema_name: "test".to_owned(),
            table_name: "t".to_owned(),
            index_names: vec!["i1".to_owned(), "i2".to_owned()],
            table_stats_version: 2,
            ..NonPartitionedTableAnalysisJob::default()
        });
        assert!(!ordinary.analyze(&context));
        let calls = context
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].3, ["test", "t", "i1"]);
        drop(calls);

        context
            .calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        let dynamic = AnalysisJob::DynamicPartitioned(DynamicPartitionedTableAnalysisJob {
            partition_ids: HashSet::from([1, 2, 3]),
            schema_name: "test".to_owned(),
            global_table_name: "pt".to_owned(),
            partition_names: vec!["p0".to_owned(), "p1".to_owned(), "p2".to_owned()],
            table_stats_version: 2,
            ..DynamicPartitionedTableAnalysisJob::default()
        });
        assert!(!dynamic.analyze(&context));
        assert_eq!(
            context
                .calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            1
        );
    }

    #[test]
    fn source_stringer_and_duration_units_match_go() {
        let value = AnalysisJob::NonPartitioned(NonPartitionedTableAnalysisJob {
            table_id: 1,
            index_ids: HashSet::from([7]),
            indicators: Indicators {
                change_percentage: 0.5,
                table_size: 12.0,
                last_analysis_duration: 1_500_000,
            },
            table_stats_version: 2,
            weight: 3.25,
            schema_name: "test".to_owned(),
            table_name: "t".to_owned(),
            index_names: vec!["idx".to_owned()],
            ..NonPartitionedTableAnalysisJob::default()
        });
        assert_eq!(format_go_duration(1_500), "1.5µs");
        assert_eq!(format_go_duration(1_500_000), "1.5ms");
        assert_eq!(format_go_duration(-1_500_000), "-1.5ms");
        assert_eq!(
            value.to_string(),
            "NonPartitionedTableAnalysisJob:\n\tAnalyzeType: analyzeIndex\n\tIndexes: idx\n\tSchema: test\n\tTable: t\n\tTableID: 1\n\tTableStatsVer: 2\n\tChangePercentage: 0.500000\n\tTableSize: 12.00\n\tLastAnalysisDuration: 1.5ms\n\tWeight: 3.250000\n"
        );
    }
}
