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

//! Source-shaped region splitting and child-task metadata before TiKV transport.
//!
//! `pkg/store/copr/coprocessor.go` constructs `copTask` values after region
//! lookup and turns batched children into `coprocessor.StoreBatchTask` in
//! `ToPBBatchTasks`. This module constructs those tasks from built request
//! metadata plus caller-supplied checked topology, and then projects child
//! tasks onto the existing protobuf shape. It keeps region epochs, peer
//! fields, ordered key ranges, task IDs, versioned point ranges, and bucket
//! versions exact while leaving cache lookup, retries, endpoint selection,
//! and RPC dispatch to a future TiKV owner.

use std::collections::BTreeMap;

use prost::Message;
use tidb_proto::{
    CoprocessorKeyRange, CoprocessorPeer, CoprocessorRegionEpoch, CoprocessorVersionedKeyRange,
    StoreBatchTask,
};

use crate::{KvRequestMetadata, ReadBytesEma, RequestKeyRange};
use tidb_txnkv::{Key, KeyRange, KeyRanges};

/// Region epoch metadata copied from `metapb.RegionEpoch`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RegionTaskEpoch {
    /// Conf-change version.
    pub conf_ver: u64,
    /// Split/merge version.
    pub version: u64,
}

/// Peer metadata copied from `metapb.Peer`.
///
/// `role` is intentionally an integer rather than a closed Rust enum. The Go
/// wire contract can acquire a new enum value, and preserving that value is
/// safer than silently mapping it to a made-up role.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RegionTaskPeer {
    /// Peer identifier.
    pub id: u64,
    /// TiKV store identifier.
    pub store_id: u64,
    /// `metapb.PeerRole` numeric value.
    pub role: i32,
    /// Whether this peer is a witness.
    pub is_witness: bool,
}

/// One checked region snapshot and its raw bucket split keys.
///
/// Region bounds belong to topology; request fragments belong to the built
/// [`RegionTaskEnvelope`]. Bucket keys may extend beyond the region and are
/// normalized to these bounds before task construction, matching the region
/// cache boundary used by Go.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegionTaskTopology {
    /// Region identifier.
    pub region_id: u64,
    /// Optional region epoch from the region cache.
    pub region_epoch: Option<RegionTaskEpoch>,
    /// Optional store-selected peer. Absence keeps store batching disabled for
    /// this region while ordinary region splitting remains available.
    pub peer: Option<RegionTaskPeer>,
    /// Inclusive region boundary. Empty means the beginning of keyspace.
    pub start_key: Vec<u8>,
    /// Exclusive region boundary. Empty means the end of keyspace.
    pub end_key: Vec<u8>,
    /// Raw bucket split keys. Keys outside the region are ignored; region
    /// start/end are inserted by normalization.
    pub bucket_keys: Vec<Vec<u8>>,
    /// Bucket metadata version copied to every task built for this region.
    pub buckets_version: u64,
    /// Whether the selected peer is the region leader and may participate in
    /// TiKV store batching. Ordinary task construction remains available when
    /// this is false.
    pub store_batch_eligible: bool,
}

impl Default for RegionTaskTopology {
    fn default() -> Self {
        Self {
            region_id: 0,
            region_epoch: None,
            peer: None,
            start_key: Vec::new(),
            end_key: Vec::new(),
            bucket_keys: Vec::new(),
            buckets_version: 0,
            store_batch_eligible: true,
        }
    }
}

/// A versioned point range for TiCI lookup.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VersionedRegionKeyRange {
    /// Point range carried by the task.
    pub range: RequestKeyRange,
    /// Read timestamp attached to the point lookup.
    pub read_ts: u64,
}

/// One `coprocessor.StoreBatchTask` wire envelope.
///
/// The envelope is immutable metadata from the caller's perspective: its
/// serializer does not split ranges, query a region cache, or attach a store
/// endpoint. `None` keeps absent proto message fields absent instead of
/// inventing zero-valued epoch or peer metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegionTaskEnvelope {
    /// Region identifier selected by the future region owner.
    pub region_id: u64,
    /// Optional region epoch from the region cache.
    pub region_epoch: Option<RegionTaskEpoch>,
    /// Optional target peer from the region cache.
    pub peer: Option<RegionTaskPeer>,
    /// Ordered key ranges belonging to this child task.
    pub ranges: Vec<RequestKeyRange>,
    /// Coprocessor task identifier used to match responses.
    pub task_id: u64,
    /// Optional TiCI versioned point ranges.
    pub versioned_ranges: Vec<VersionedRegionKeyRange>,
    /// Bucket metadata version used to validate this child task.
    pub buckets_version: u64,
    /// Whether row-count paging remains enabled for this task.
    pub paging: bool,
    /// Row-count page size selected while constructing this task.
    pub paging_size: u64,
    /// Estimated source rows whose ranges overlap this task, or `-1` when
    /// hints were unavailable or invalidated by range reordering.
    pub row_count_hint: i64,
    /// Response channel capacity selected by the source paging policy.
    pub response_channel_capacity: usize,
    /// Store-busy threshold retained by non-batched tasks.
    pub store_busy_threshold_ms: u64,
    /// TiKV client read timeout copied into the task.
    pub tikv_client_read_timeout_ms: u64,
    /// Child region tasks grouped behind this task by store batching.
    pub batch_task_list: Vec<RegionTaskEnvelope>,
    /// Whether this task's selected peer is eligible for TiKV store batching.
    pub store_batch_eligible: bool,
}

impl Default for RegionTaskEnvelope {
    fn default() -> Self {
        Self {
            region_id: 0,
            region_epoch: None,
            peer: None,
            ranges: Vec::new(),
            task_id: 0,
            versioned_ranges: Vec::new(),
            buckets_version: 0,
            paging: false,
            paging_size: 0,
            row_count_hint: -1,
            response_channel_capacity: 0,
            store_busy_threshold_ms: 0,
            tikv_client_read_timeout_ms: 0,
            batch_task_list: Vec::new(),
            store_batch_eligible: true,
        }
    }
}

impl RegionTaskEnvelope {
    /// Encodes the exact `StoreBatchTask` protobuf field numbers.
    #[must_use]
    pub fn encode_to_vec(&self) -> Vec<u8> {
        StoreBatchTask {
            region_id: self.region_id,
            region_epoch: self.region_epoch.map(|epoch| CoprocessorRegionEpoch {
                conf_ver: epoch.conf_ver,
                version: epoch.version,
            }),
            peer: self.peer.map(|peer| CoprocessorPeer {
                id: peer.id,
                store_id: peer.store_id,
                role: peer.role,
                is_witness: peer.is_witness,
            }),
            ranges: self
                .ranges
                .iter()
                .map(|range| CoprocessorKeyRange {
                    start: range.start_key.clone(),
                    end: range.end_key.clone(),
                })
                .collect(),
            task_id: self.task_id,
            versioned_ranges: self
                .versioned_ranges
                .iter()
                .map(|range| CoprocessorVersionedKeyRange {
                    range: Some(CoprocessorKeyRange {
                        start: range.range.start_key.clone(),
                        end: range.range.end_key.clone(),
                    }),
                    read_ts: range.read_ts,
                })
                .collect(),
            buckets_version: self.buckets_version,
        }
        .encode_to_vec()
    }

    /// Encodes the source `ToPBBatchTasks` child list in order.
    #[must_use]
    pub fn encode_batch_tasks(&self) -> Vec<Vec<u8>> {
        self.batch_task_list
            .iter()
            .map(Self::encode_to_vec)
            .collect()
    }

    /// Counts source-defined small tasks and calculates their extra worker
    /// concurrency with the exact sigma and per-core cap.
    #[must_use]
    pub fn small_task_concurrency(tasks: &[Self], num_cpus: usize) -> (usize, usize) {
        let count = tasks
            .iter()
            .filter(|task| {
                (task.row_count_hint > 0
                    && task.batch_task_list.is_empty()
                    && task.row_count_hint <= 32)
                    || (!task.batch_task_list.is_empty() && task.row_count_hint <= 64)
            })
            .count();
        if count == 0 {
            return (0, 0);
        }
        let count_as_float = count as f64;
        let concurrency =
            (count_as_float / (1.0 + 0.5 * (2.0 * count_as_float.ln()).sqrt())) as usize;
        (count, concurrency.min(20 * num_cpus.max(1)))
    }

    /// Returns the EMA prediction when row paging or request-level byte
    /// paging is active for this task.
    #[must_use]
    pub fn predicted_read_bytes(&self, request_paging_size_bytes: u64, ema: &ReadBytesEma) -> u64 {
        if self.paging || request_paging_size_bytes > 0 {
            ema.predict()
        } else {
            0
        }
    }
}

/// Builds source-shaped coprocessor tasks from immutable request metadata and
/// normalized region topology.
///
/// Each topology value represents one region with true region bounds and raw
/// bucket split keys. Returned envelopes contain only request fragments.
/// Region lookup, cache refresh, retry, store selection, and RPC remain owned
/// by the caller that supplies the topology.
pub(crate) fn build_region_tasks(
    metadata: &KvRequestMetadata,
    topology: &[RegionTaskTopology],
) -> Option<Vec<RegionTaskEnvelope>> {
    let key_ranges = metadata.key_ranges.as_ref()?;
    let mut original_ranges = Vec::new();
    let mut original_hints = Vec::new();
    let hints_shape_valid = key_ranges.row_count_hints.len() == key_ranges.partitions.len()
        && key_ranges
            .partitions
            .iter()
            .zip(&key_ranges.row_count_hints)
            .all(|(ranges, hints)| ranges.len() == hints.len());
    for (partition_index, partition) in key_ranges.partitions.iter().enumerate() {
        for (range_index, range) in partition.iter().enumerate() {
            original_ranges.push(to_txn_range(range));
            if hints_shape_valid {
                original_hints.push(key_ranges.row_count_hints[partition_index][range_index]);
            }
        }
    }

    let mut ranges = KeyRanges::new(original_ranges.clone());
    let reordered = ensure_monotonic_key_ranges(&mut ranges);
    let hints = (!reordered && hints_shape_valid).then_some(original_hints.as_slice());
    if ranges.is_empty() {
        return Some(Vec::new());
    }
    if !topology_is_valid(topology) {
        return None;
    }

    let sorted_ranges = ranges.to_ranges();
    let mut tasks = Vec::new();
    for region in topology {
        for bucket in normalized_bucket_ranges(region)? {
            let mut fragments = Vec::new();
            for range in &sorted_ranges {
                if let Some(fragment) = intersect_range(range, &bucket) {
                    fragments.push(fragment);
                }
            }
            if fragments.is_empty() {
                continue;
            }
            let request_ranges: Vec<_> = fragments.iter().map(to_request_range).collect();
            let mut paging = metadata.session.paging.enabled;
            let mut paging_size = if paging {
                metadata.session.paging.min_size
            } else {
                0
            };
            if paging && metadata.limit_size != 0 && metadata.limit_size < paging_size {
                paging = false;
                paging_size = 0;
            }
            let response_channel_capacity = if metadata.keep_order {
                if metadata.session.paging.enabled || metadata.session.paging.size_bytes > 0 {
                    18
                } else {
                    2
                }
            } else {
                0
            };
            tasks.push(RegionTaskEnvelope {
                region_id: region.region_id,
                region_epoch: region.region_epoch,
                peer: region.peer,
                ranges: request_ranges,
                task_id: 0,
                versioned_ranges: Vec::new(),
                buckets_version: region.buckets_version,
                paging,
                paging_size,
                row_count_hint: hints.map_or(-1, |hints| {
                    row_count_hint(&fragments, &original_ranges, hints)
                }),
                response_channel_capacity,
                store_busy_threshold_ms: metadata.session.store_busy_threshold_ms,
                tikv_client_read_timeout_ms: metadata.session.tikv_client_read_timeout_ms,
                batch_task_list: Vec::new(),
                store_batch_eligible: region.store_batch_eligible,
            });
        }
    }

    if !all_ranges_covered(&sorted_ranges, &tasks) {
        return None;
    }
    if metadata.session.store_batch_size > 0 && hints.is_some() {
        tasks = batch_tasks(tasks, metadata.session.store_batch_size);
    }
    // Go batches while visiting regions in ascending key order, then reverses
    // only the resulting parent task list for descending scans. Children stay
    // associated with the parent/store chosen during ascending construction.
    if metadata.desc {
        tasks.reverse();
    }
    Some(tasks)
}

fn ensure_monotonic_key_ranges(ranges: &mut KeyRanges) -> bool {
    let ordered = (0..ranges.len()).all(|index| {
        let range = ranges.ref_at(index);
        range.end_key.as_bytes().is_empty() || range.start_key <= range.end_key
    }) && (1..ranges.len()).all(|index| {
        let previous = ranges.ref_at(index - 1);
        let current = ranges.ref_at(index);
        !previous.end_key.as_bytes().is_empty() && previous.end_key <= current.start_key
    });
    if ordered {
        return false;
    }
    let mut sorted = ranges.to_ranges();
    sorted.sort_by(|left, right| {
        left.start_key
            .cmp(&right.start_key)
            .then_with(|| left.end_key.cmp(&right.end_key))
    });
    ranges.reset(sorted);
    true
}

fn topology_is_valid(topology: &[RegionTaskTopology]) -> bool {
    let mut previous_start: Option<&[u8]> = None;
    let mut previous_end: Option<&[u8]> = None;
    for region in topology {
        if !region.end_key.is_empty() && region.start_key >= region.end_key {
            return false;
        }
        if previous_start.is_some_and(|start| start >= region.start_key.as_slice()) {
            return false;
        }
        // Region snapshots may omit unrelated keyspace. Reject overlap, but do
        // not require global contiguity; requested-range coverage is checked
        // after intersections are built.
        if previous_end.is_some_and(|end| end.is_empty() || end > region.start_key.as_slice()) {
            return false;
        }
        previous_start = Some(&region.start_key);
        previous_end = Some(&region.end_key);
    }
    true
}

fn normalized_bucket_ranges(region: &RegionTaskTopology) -> Option<Vec<RequestKeyRange>> {
    let mut interior: Vec<Vec<u8>> = region
        .bucket_keys
        .iter()
        .filter_map(|key| {
            let after_start = key.as_slice() > region.start_key.as_slice();
            let before_end =
                region.end_key.is_empty() || key.as_slice() < region.end_key.as_slice();
            (after_start && before_end).then(|| key.clone())
        })
        .collect();
    interior.sort();
    interior.dedup();
    let mut keys = Vec::with_capacity(interior.len() + 2);
    keys.push(region.start_key.clone());
    keys.extend(interior);
    keys.push(region.end_key.clone());
    if keys.len() < 2 {
        return None;
    }
    Some(
        keys.windows(2)
            .map(|pair| RequestKeyRange {
                start_key: pair[0].clone(),
                end_key: pair[1].clone(),
            })
            .collect(),
    )
}

fn to_txn_range(range: &RequestKeyRange) -> KeyRange {
    KeyRange::new(
        Key::from_bytes(range.start_key.clone()),
        Key::from_bytes(range.end_key.clone()),
    )
}

fn to_request_range(range: &KeyRange) -> RequestKeyRange {
    RequestKeyRange {
        start_key: range.start_key.as_bytes().to_vec(),
        end_key: range.end_key.as_bytes().to_vec(),
    }
}

fn intersect_range(range: &KeyRange, bucket: &RequestKeyRange) -> Option<KeyRange> {
    let start = range.start_key.as_bytes();
    let end = range.end_key.as_bytes();
    if start == end {
        return contains_key(bucket, start).then(|| range.clone());
    }
    let intersection_start = if start < bucket.start_key.as_slice() {
        bucket.start_key.as_slice()
    } else {
        start
    };
    let intersection_end = min_end(end, &bucket.end_key);
    if !intersection_end.is_empty() && intersection_start >= intersection_end {
        return None;
    }
    Some(KeyRange::new(
        Key::from_bytes(intersection_start),
        Key::from_bytes(intersection_end),
    ))
}

fn min_end<'a>(left: &'a [u8], right: &'a [u8]) -> &'a [u8] {
    match (left.is_empty(), right.is_empty()) {
        (true, true) => left,
        (true, false) => right,
        (false, true) => left,
        (false, false) if left <= right => left,
        (false, false) => right,
    }
}

fn contains_key(range: &RequestKeyRange, key: &[u8]) -> bool {
    key >= range.start_key.as_slice()
        && (range.end_key.is_empty() || key < range.end_key.as_slice())
}

fn ranges_overlap(left: &KeyRange, right: &KeyRange) -> bool {
    (left.end_key.as_bytes().is_empty() || left.end_key > right.start_key)
        && (right.end_key.as_bytes().is_empty() || right.end_key > left.start_key)
}

fn row_count_hint(fragments: &[KeyRange], originals: &[KeyRange], hints: &[usize]) -> i64 {
    originals
        .iter()
        .zip(hints)
        .filter(|(original, _)| fragments.iter().any(|part| ranges_overlap(original, part)))
        .map(|(_, hint)| i64::try_from(*hint).unwrap_or(i64::MAX))
        .sum()
}

fn all_ranges_covered(ranges: &[KeyRange], tasks: &[RegionTaskEnvelope]) -> bool {
    ranges.iter().all(|range| {
        if range.start_key == range.end_key {
            return tasks.iter().flat_map(|task| &task.ranges).any(|part| {
                part.start_key == range.start_key.as_bytes()
                    && part.end_key == range.end_key.as_bytes()
            });
        }
        let mut cursor = range.start_key.as_bytes().to_vec();
        for part in tasks.iter().flat_map(|task| &task.ranges) {
            if part.end_key.as_slice() <= cursor.as_slice() && !part.end_key.is_empty() {
                continue;
            }
            if part.start_key != cursor {
                continue;
            }
            cursor.clone_from(&part.end_key);
            if cursor == range.end_key.as_bytes() {
                return true;
            }
            if cursor.is_empty() {
                return range.end_key.as_bytes().is_empty();
            }
        }
        false
    })
}

fn batch_tasks(tasks: Vec<RegionTaskEnvelope>, batch_size: u64) -> Vec<RegionTaskEnvelope> {
    let batch_size = usize::try_from(batch_size).unwrap_or(usize::MAX).max(1);
    let mut result = Vec::new();
    let mut store_parent = BTreeMap::<u64, usize>::new();
    for (index, mut task) in tasks.into_iter().enumerate() {
        task.task_id = u64::try_from(index + 1).unwrap_or(u64::MAX);
        let small =
            task.store_batch_eligible && task.row_count_hint > 0 && task.row_count_hint <= 32;
        if !small {
            result.push(task);
            continue;
        }
        let Some(store_id) = task.peer.map(|peer| peer.store_id).filter(|id| *id != 0) else {
            result.push(task);
            continue;
        };
        let Some(parent_index) = store_parent.get(&store_id).copied() else {
            store_parent.insert(store_id, result.len());
            result.push(task);
            continue;
        };
        if result[parent_index].batch_task_list.len() >= batch_size {
            store_parent.insert(store_id, result.len());
            result.push(task);
            continue;
        }
        task.store_busy_threshold_ms = 0;
        task.paging = false;
        task.paging_size = 0;
        let parent = &mut result[parent_index];
        parent.store_busy_threshold_ms = 0;
        parent.paging = false;
        parent.paging_size = 0;
        parent.row_count_hint = parent.row_count_hint.saturating_add(task.row_count_hint);
        parent.batch_task_list.push(task);
    }
    result
}
