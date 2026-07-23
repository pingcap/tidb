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

//! Complete dependency-neutral data contracts from `pkg/kv/kv.go`.

use std::cmp::Ordering;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::{Key, KeyRange, ReplicaReadType, RequestSource, ResourceGroupTagBuilder};

/// Marker appended to unchanged index KVs.
pub const UNCOMMITTED_INDEX_KV_FLAG: u8 = b'1';
/// Default one-entry transaction size limit.
pub const DEFAULT_TXN_ENTRY_SIZE_LIMIT: u64 = 6 * 1024 * 1024;
/// Default total transaction size limit.
pub const DEFAULT_TXN_TOTAL_SIZE_LIMIT: u64 = 100 * 1024 * 1024;

static TXN_ENTRY_SIZE_LIMIT: AtomicU64 = AtomicU64::new(DEFAULT_TXN_ENTRY_SIZE_LIMIT);
static TXN_TOTAL_SIZE_LIMIT: AtomicU64 = AtomicU64::new(DEFAULT_TXN_TOTAL_SIZE_LIMIT);

/// Returns the process-wide one-entry transaction size limit.
#[must_use]
pub fn txn_entry_size_limit() -> u64 {
    TXN_ENTRY_SIZE_LIMIT.load(AtomicOrdering::Relaxed)
}

/// Updates the process-wide one-entry transaction size limit.
pub fn set_txn_entry_size_limit(limit: u64) {
    TXN_ENTRY_SIZE_LIMIT.store(limit, AtomicOrdering::Relaxed);
}

/// Returns the process-wide total transaction size limit.
#[must_use]
pub fn txn_total_size_limit() -> u64 {
    TXN_TOTAL_SIZE_LIMIT.load(AtomicOrdering::Relaxed)
}

/// Updates the process-wide total transaction size limit.
pub fn set_txn_total_size_limit(limit: u64) {
    TXN_TOTAL_SIZE_LIMIT.store(limit, AtomicOrdering::Relaxed);
}

/// Storage engine selected for a request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreType(u8);

#[allow(non_upper_case_globals)]
impl StoreType {
    /// TiKV.
    pub const TiKv: Self = Self(0);
    /// TiFlash.
    pub const TiFlash: Self = Self(1);
    /// TiDB memory data.
    pub const TiDb: Self = Self(2);
    /// Unknown engine.
    pub const Unspecified: Self = Self(255);

    /// Preserves every raw Go `uint8` value.
    #[must_use]
    pub const fn from_raw(value: u8) -> Self {
        Self(value)
    }

    /// Returns the source integer representation.
    #[must_use]
    pub const fn raw(self) -> u8 {
        self.0
    }
}

/// Request type values carried by `Request.Tp`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RequestType(i64);

#[allow(non_upper_case_globals)]
impl RequestType {
    /// No request type selected.
    pub const Unknown: Self = Self(0);
    /// Legacy select request.
    pub const Select: Self = Self(101);
    /// Legacy index request.
    pub const Index: Self = Self(102);
    /// DAG coprocessor request.
    pub const Dag: Self = Self(103);
    /// Analyze request.
    pub const Analyze: Self = Self(104);
    /// Checksum request.
    pub const Checksum: Self = Self(105);

    /// Preserves every raw Go `int64` value.
    #[must_use]
    pub const fn from_raw(value: i64) -> Self {
        Self(value)
    }

    /// Returns the source integer representation.
    #[must_use]
    pub const fn raw(self) -> i64 {
        self.0
    }
}

impl StoreType {
    /// Returns the Go source name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::TiKv => "tikv",
            Self::TiFlash => "tiflash",
            Self::TiDb => "tidb",
            _ => "unspecified",
        }
    }
}

/// Transaction isolation level.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IsolationLevel(isize);

#[allow(non_upper_case_globals)]
impl IsolationLevel {
    /// Snapshot isolation.
    pub const Snapshot: Self = Self(0);
    /// Read committed.
    pub const ReadCommitted: Self = Self(1);
    /// Read committed with timestamp checking.
    pub const RcCheckTs: Self = Self(2);

    /// Preserves every raw Go `int` value.
    #[must_use]
    pub const fn from_raw(value: isize) -> Self {
        Self(value)
    }

    /// Returns the source integer representation.
    #[must_use]
    pub const fn raw(self) -> isize {
        self.0
    }
}

/// Transaction/request priority.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Priority(isize);

#[allow(non_upper_case_globals)]
impl Priority {
    /// Normal priority.
    pub const Normal: Self = Self(0);
    /// Low priority.
    pub const Low: Self = Self(1);
    /// High priority.
    pub const High: Self = Self(2);

    /// Preserves every raw Go `int` value.
    #[must_use]
    pub const fn from_raw(value: isize) -> Self {
        Self(value)
    }

    /// Returns the source integer representation.
    #[must_use]
    pub const fn raw(self) -> isize {
        self.0
    }
}

/// Partition-aware request ranges from Go `kv.KeyRanges`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PartitionedKeyRanges {
    ranges: Vec<Vec<KeyRange>>,
    row_count_hints: Vec<Vec<usize>>,
    partitioned: bool,
}

impl PartitionedKeyRanges {
    /// Constructs partitioned ranges without hints.
    #[must_use]
    pub fn new_partitioned(ranges: Vec<Vec<KeyRange>>) -> Self {
        Self::new_partitioned_with_hints(ranges, Vec::new())
    }

    /// Constructs non-partitioned ranges without hints.
    #[must_use]
    pub fn new_non_partitioned(ranges: Vec<KeyRange>) -> Self {
        Self {
            ranges: vec![ranges],
            row_count_hints: Vec::new(),
            partitioned: false,
        }
    }

    /// Constructs partitioned ranges with aligned row-count hints.
    #[must_use]
    pub fn new_partitioned_with_hints(
        ranges: Vec<Vec<KeyRange>>,
        row_count_hints: Vec<Vec<usize>>,
    ) -> Self {
        Self {
            ranges,
            row_count_hints,
            partitioned: true,
        }
    }

    /// Constructs one non-partitioned range group with optional hints.
    #[must_use]
    pub fn new_non_partitioned_with_hints(ranges: Vec<KeyRange>, hints: Vec<usize>) -> Self {
        Self {
            ranges: vec![ranges],
            row_count_hints: vec![hints],
            partitioned: false,
        }
    }

    /// Returns all partition groups in source order.
    #[must_use]
    pub fn partitions(&self) -> &[Vec<KeyRange>] {
        &self.ranges
    }

    /// Returns row-count hints aligned with [`Self::partitions`].
    #[must_use]
    pub fn row_count_hints(&self) -> &[Vec<usize>] {
        &self.row_count_hints
    }

    /// Returns the first range group, or an empty slice.
    #[must_use]
    pub fn first_partition_ranges(&self) -> &[KeyRange] {
        self.ranges.first().map_or(&[], Vec::as_slice)
    }

    /// Marks a zero/one-group value non-partitioned.
    pub fn set_to_non_partitioned(&mut self) -> Result<(), &'static str> {
        if self.ranges.len() > 1 {
            return Err("you want to change the partitioned ranges to non-partitioned ranges");
        }
        self.partitioned = false;
        Ok(())
    }

    /// Returns whether the outer grouping represents partitions.
    #[must_use]
    pub const fn is_partitioned(&self) -> bool {
        self.partitioned
    }

    /// Returns whether this envelope is non-partitioned.
    #[must_use]
    pub const fn is_non_partitioned(&self) -> bool {
        !self.partitioned
    }

    /// Appends all ranges in partition order.
    pub fn append_to(&self, output: &mut Vec<KeyRange>) {
        output.extend(self.ranges.iter().flatten().cloned());
    }

    /// Sorts partitions by their first range, then ranges inside each partition.
    pub fn sort_by(&mut self, mut compare: impl FnMut(&KeyRange, &KeyRange) -> Ordering) {
        let outer_sorted = self.ranges.windows(2).all(|pair| {
            pair[0].is_empty()
                || pair[1].is_empty()
                || compare(&pair[0][0], &pair[1][0]) != Ordering::Greater
        });
        if !outer_sorted {
            self.ranges
                .sort_by(|left, right| match (left.first(), right.first()) {
                    (None, None) => Ordering::Equal,
                    (None, _) => Ordering::Less,
                    (_, None) => Ordering::Greater,
                    (Some(left), Some(right)) => compare(left, right),
                });
        }
        for ranges in &mut self.ranges {
            if !ranges
                .windows(2)
                .all(|pair| compare(&pair[0], &pair[1]) != Ordering::Greater)
            {
                ranges.sort_by(&mut compare);
            }
        }
    }

    /// Visits every partition and its aligned hints.
    pub fn try_for_each_partition<E>(
        &self,
        mut visit: impl FnMut(&[KeyRange], &[usize]) -> Result<(), E>,
    ) -> Result<(), E> {
        for (index, ranges) in self.ranges.iter().enumerate() {
            visit(
                ranges,
                self.row_count_hints.get(index).map_or(&[], Vec::as_slice),
            )?;
        }
        Ok(())
    }

    /// Visits every partition.
    pub fn for_each_partition(&self, mut visit: impl FnMut(&[KeyRange])) {
        for ranges in &self.ranges {
            visit(ranges);
        }
    }

    /// Returns the number of range groups.
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.ranges.len()
    }

    /// Returns whether partitions and their ranges are byte-order sorted.
    #[must_use]
    pub fn is_fully_sorted(&self) -> bool {
        let first_keys_sorted = self.ranges.windows(2).all(|pair| {
            pair[0].is_empty() || pair[1].is_empty() || pair[0][0].start_key <= pair[1][0].start_key
        });
        first_keys_sorted
            && self.ranges.iter().all(|ranges| {
                ranges
                    .windows(2)
                    .all(|pair| pair[0].start_key <= pair[1].start_key)
            })
    }

    /// Returns the number of individual ranges.
    #[must_use]
    pub fn total_range_count(&self) -> usize {
        self.ranges.iter().map(Vec::len).sum()
    }
}

/// Physical partition and its ranges for TiFlash scans.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PartitionIdAndRanges {
    /// Physical partition identifier.
    pub id: i64,
    /// Ordered ranges for the partition.
    pub key_ranges: Vec<KeyRange>,
}

/// Paging controls embedded in one KV request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Paging {
    /// Whether row-count paging is enabled.
    pub enabled: bool,
    /// Minimum rows per page.
    pub min_size: u64,
    /// Maximum rows per page.
    pub max_size: u64,
    /// Byte budget per page, zero when disabled.
    pub size_bytes: u64,
}

/// Store label required by a request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StoreLabel {
    /// Label key.
    pub key: String,
    /// Label value.
    pub value: String,
}

/// Complete dependency-neutral metadata carried by Go `kv.Request`.
#[derive(Clone, Default)]
pub struct Request {
    /// Request type.
    pub request_type: RequestType,
    /// Read timestamp.
    pub start_ts: u64,
    /// Serialized request body.
    pub data: Option<Vec<u8>>,
    /// Partition-aware key ranges.
    pub key_ranges: Option<PartitionedKeyRanges>,
    /// TiFlash partition-table ranges.
    pub partition_id_and_ranges: Vec<PartitionIdAndRanges>,
    /// Request concurrency.
    pub concurrency: isize,
    /// Shared in-flight coprocessor request limiter.
    pub coprocessor_rate_limit: Option<Arc<Semaphore>>,
    /// Isolation level.
    pub isolation_level: IsolationLevel,
    /// Priority.
    pub priority: Priority,
    /// Coprocessor memory tracker.
    pub memory_tracker: Option<Arc<AtomicI64>>,
    /// Preserve response ordering.
    pub keep_order: bool,
    /// Descending scan.
    pub desc: bool,
    /// Bypass storage block cache.
    pub not_fill_cache: bool,
    /// Replica-read policy.
    pub replica_read: ReplicaReadType,
    /// Target store engine.
    pub store_type: StoreType,
    /// Coprocessor cache eligibility.
    pub cacheable: bool,
    /// Schema version.
    pub schema_version: i64,
    /// Batch-coprocessor marker.
    pub batch_cop: bool,
    /// Statement task ID.
    pub task_id: u64,
    /// Target TiDB server ID.
    pub tidb_server_id: u64,
    /// Transaction scope.
    pub txn_scope: String,
    /// Read-replica scope.
    pub read_replica_scope: String,
    /// Staleness-read marker.
    pub is_staleness: bool,
    /// Replica-read request adjuster.
    pub closest_replica_read_adjuster: Option<Arc<dyn CoprocessorRequestAdjuster>>,
    /// Required store labels.
    pub match_store_labels: Vec<StoreLabel>,
    /// Resource-group tag builder.
    pub resource_group_tagger: Option<ResourceGroupTagBuilder>,
    /// Paging controls.
    pub paging: Paging,
    /// Request-origin metadata.
    pub request_source: RequestSource,
    /// Per-store coprocessor batch size.
    pub store_batch_size: isize,
    /// Resource-group name.
    pub resource_group_name: String,
    /// Scan/limit size.
    pub limit_size: u64,
    /// Store-busy threshold in Go `time.Duration` nanoseconds.
    pub store_busy_threshold_ns: i64,
    /// TiKV read timeout in milliseconds.
    pub tikv_client_read_timeout_ms: u64,
    /// Whole-query execution timeout in milliseconds.
    pub max_execution_time_ms: u64,
    /// Statement-wide maximum keys read.
    pub max_keys_read: u64,
    /// Shared statement-wide keys-read accumulator.
    pub max_keys_read_counter: Option<Arc<AtomicU64>>,
    /// Runaway-query policy.
    pub runaway_checker: Option<Arc<dyn RunawayChecker>>,
    /// Connection ID.
    pub connection_id: u64,
    /// Connection alias.
    pub connection_alias: String,
}

/// Adjusts a coprocessor request for the current store count.
pub trait CoprocessorRequestAdjuster: Send + Sync {
    /// Returns whether the request changed.
    fn adjust(&self, request: &mut Request, store_count: usize) -> bool;
}

impl<F> CoprocessorRequestAdjuster for F
where
    F: Fn(&mut Request, usize) -> bool + Send + Sync,
{
    fn adjust(&self, request: &mut Request, store_count: usize) -> bool {
        self(request, store_count)
    }
}

/// Runaway-query action carried by a storage request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RunawayAction {
    /// No action.
    #[default]
    None,
    /// Observe without enforcement.
    DryRun,
    /// Rate-limit the request.
    CoolDown,
    /// Kill the query.
    Kill,
    /// Move the query to another resource group.
    SwitchGroup,
}

/// Runaway-query checker carried into the coprocessor layer.
pub trait RunawayChecker: Send + Sync {
    /// Error type is erased only at this cross-package carrier boundary.
    fn before_executor(&self) -> Result<Option<String>, String>;
    /// Checks and optionally rewrites an encoded storage request.
    fn before_coprocessor_request(&self, request: &mut Vec<u8>) -> Result<(), String>;
    /// Applies post-request thresholds.
    fn check_thresholds(
        &self,
        read_units: Option<f64>,
        processed_keys: i64,
        storage_error: Option<&str>,
    ) -> Result<(), String>;
    /// Resets total processed keys.
    fn reset_total_processed_keys(&self);
    /// Returns the current action.
    fn action(&self) -> RunawayAction;
    /// Returns a kill reason when a watch rule requires termination.
    fn rule_kill_action(&self) -> Option<String>;
}

/// Returns every key in one stage that satisfies `predicate`.
pub fn find_keys_in_stage(
    entries: impl IntoIterator<Item = (Key, crate::KeyFlags, Vec<u8>)>,
    mut predicate: impl FnMut(&Key, crate::KeyFlags, &[u8]) -> bool,
) -> Vec<Key> {
    entries
        .into_iter()
        .filter_map(|(key, flags, value)| predicate(&key, flags, &value).then_some(key))
        .collect()
}

/// Default/global replica scope.
pub const GLOBAL_REPLICA_SCOPE: &str = "global";
