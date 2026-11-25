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
//! and RPC dispatch to the separate TiKV transport owner.

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use prost::Message;
use tidb_proto::{
    CoprocessorKeyRange, CoprocessorPeer, CoprocessorRegionEpoch, CoprocessorVersionedKeyRange,
    StoreBatchTask,
};

use tidb_txnkv::KvRequestMetadata;
use crate::read_bytes_ema::ReadBytesEma;
use tidb_txnkv::Key;
use tidb_txnkv::KeyRange;
use tidb_txnkv::PartitionedKeyRanges;
use crate::key_ranges::KeyRanges;
use crate::range_diagnostics::{compare_range_order, range_issues};

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
    pub range: KeyRange,
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
    pub ranges: KeyRanges,
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
            ranges: KeyRanges::default(),
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
        self.to_proto().encode_to_vec()
    }

    pub(crate) fn to_proto(&self) -> StoreBatchTask {
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
            ranges: self.ranges.to_pb_ranges(),
            task_id: self.task_id,
            versioned_ranges: self
                .versioned_ranges
                .iter()
                .map(|range| CoprocessorVersionedKeyRange {
                    range: Some(CoprocessorKeyRange {
                        start: prost::bytes::Bytes::copy_from_slice(&range.range.start_key),
                        end: prost::bytes::Bytes::copy_from_slice(&range.range.end_key),
                    }),
                    read_ts: range.read_ts,
                })
                .collect(),
            buckets_version: self.buckets_version,
        }
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
        let count = tasks.iter().filter(|task| task.is_small()).count();
        (count, Self::small_concurrency(count, num_cpus))
    }

    pub(crate) fn is_small(&self) -> bool {
        (self.row_count_hint > 0 && self.batch_task_list.is_empty() && self.row_count_hint <= 32)
            || (!self.batch_task_list.is_empty() && self.row_count_hint <= 64)
    }

    pub(crate) fn small_concurrency(count: usize, num_cpus: usize) -> usize {
        if count == 0 {
            return 0;
        }
        let count_as_float = count as f64;
        let concurrency =
            (count_as_float / (1.0 + 0.5 * (2.0 * count_as_float.ln()).sqrt())) as usize;
        concurrency.min(20 * num_cpus.max(1))
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
pub fn build_region_tasks(
    metadata: &KvRequestMetadata,
    topology: &[RegionTaskTopology],
) -> Option<Vec<RegionTaskEnvelope>> {
    let key_ranges = metadata.key_ranges.as_ref()?;
    let partitions = key_ranges.partitions();
    let hint_groups = key_ranges.row_count_hints();
    let hints_shape_valid = hint_groups.len() == partitions.len()
        && partitions
            .iter()
            .zip(hint_groups)
            .all(|(ranges, hints)| ranges.len() == hints.len());

    if let [ranges] = partitions {
        let hints = hints_shape_valid.then(|| hint_groups[0].as_slice());
        return build_region_tasks_for_ranges(metadata, topology, ranges, hints, Some(key_ranges));
    }

    let ranges: Vec<_> = partitions.iter().flatten().collect();
    let hints: Option<Vec<_>> =
        hints_shape_valid.then(|| hint_groups.iter().flatten().copied().collect());
    build_region_tasks_for_ranges(metadata, topology, &ranges, hints.as_deref(), None)
}

fn build_region_tasks_for_ranges(
    metadata: &KvRequestMetadata,
    topology: &[RegionTaskTopology],
    ranges: &[impl Borrow<KeyRange>],
    hints: Option<&[usize]>,
    shared: Option<&Arc<PartitionedKeyRanges>>,
) -> Option<Vec<RegionTaskEnvelope>> {
    if ranges.is_empty() {
        return Some(Vec::new());
    }
    if !topology_is_valid(topology) {
        return None;
    }
    let original = ranges.iter().map(Borrow::borrow);
    if range_issues(original.clone()).is_empty() {
        return build_sorted_region_tasks(metadata, topology, ranges, hints, shared);
    }
    let mut reordered: Vec<_> = original.collect();
    reordered.sort_by(|left, right| compare_range_order(left, right));
    build_sorted_region_tasks(metadata, topology, &reordered, None, None)
}

fn build_sorted_region_tasks(
    metadata: &KvRequestMetadata,
    topology: &[RegionTaskTopology],
    ranges: &[impl Borrow<KeyRange>],
    hints: Option<&[usize]>,
    shared: Option<&Arc<PartitionedKeyRanges>>,
) -> Option<Vec<RegionTaskEnvelope>> {
    // Keep the original keys borrowed through both splitting and coverage checking.
    let mut tasks = Vec::new();
    // Both the request ranges and the region/bucket boundaries are monotonic.
    // Go's `SplitKeyRangesByLocations` walks them with one cursor; restarting
    // at the first request range for every bucket turns a large index-join
    // lookup into O(regions * buckets * ranges) work.
    let mut range_cursor = 0;
    let mut hint_cursor = 0;
    for region in topology {
        let bounds = [region.start_key.as_slice(), region.end_key.as_slice()];
        let bucket_keys;
        let boundaries = if region.bucket_keys.is_empty() {
            &bounds[..]
        } else {
            bucket_keys = normalized_bucket_keys(region);
            &bucket_keys[..]
        };
        for keys in boundaries.windows(2) {
            let bucket = keys[0]..keys[1];
            let fragments = intersect_sorted_ranges(ranges, &bucket, &mut range_cursor, shared);
            if fragments.is_empty() {
                continue;
            }
            let mut paging = metadata.paging.enabled;
            let mut paging_size = if paging { metadata.paging.min_size } else { 0 };
            if paging && metadata.limit_size != 0 && metadata.limit_size < paging_size {
                paging = false;
                paging_size = 0;
            }
            let response_channel_capacity = if metadata.keep_order {
                if metadata.paging.enabled || metadata.paging.size_bytes > 0 {
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
                ranges: fragments,
                task_id: 0,
                versioned_ranges: Vec::new(),
                buckets_version: region.buckets_version,
                paging,
                paging_size,
                row_count_hint: hints.map_or(-1, |hints| {
                    row_count_hint_for_bucket(&bucket, ranges, hints, &mut hint_cursor)
                }),
                response_channel_capacity,
                store_busy_threshold_ms: u64::try_from(metadata.store_busy_threshold_ns)
                    .unwrap_or(0)
                    / 1_000_000,
                tikv_client_read_timeout_ms: metadata.tikv_client_read_timeout_ms,
                batch_task_list: Vec::new(),
                store_batch_eligible: region.store_batch_eligible,
            });
        }
    }

    if !coverage_scan(ranges.iter().map(Borrow::borrow), &tasks).0 {
        return None;
    }
    if metadata.store_batch_size > 0 && (hints.is_some() || metadata.allow_batch_task_data_merge) {
        tasks = batch_tasks(
            tasks,
            u64::try_from(metadata.store_batch_size).unwrap_or(u64::MAX),
            metadata.allow_batch_task_data_merge,
        );
    }
    // Go batches while visiting regions in ascending key order, then reverses
    // only the resulting parent task list for descending scans. Children stay
    // associated with the parent/store chosen during ascending construction.
    if metadata.desc {
        tasks.reverse();
    }
    Some(tasks)
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

fn normalized_bucket_keys(region: &RegionTaskTopology) -> Vec<&[u8]> {
    let start = region.start_key.as_slice();
    let end = region.end_key.as_slice();
    let mut keys = Vec::with_capacity(region.bucket_keys.len() + 2);
    keys.push(start);
    keys.extend(
        region
            .bucket_keys
            .iter()
            .map(Vec::as_slice)
            .filter(|key| *key > start && (end.is_empty() || *key < end)),
    );
    keys[1..].sort_unstable();
    keys.dedup();
    keys.push(end);
    keys
}

fn intersect_range(range: &KeyRange, bucket: &Range<&[u8]>) -> Option<KeyRange> {
    let start = range.start_key.as_bytes();
    let end = range.end_key.as_bytes();
    if start == end {
        return (start >= bucket.start && (bucket.end.is_empty() || start < bucket.end))
            .then(|| range.clone());
    }
    let intersection_start = start.max(bucket.start);
    let intersection_end = min_end(end, bucket.end);
    if !intersection_end.is_empty() && intersection_start >= intersection_end {
        return None;
    }
    Some(KeyRange::new(
        Key::from_bytes(intersection_start),
        Key::from_bytes(intersection_end),
    ))
}

/// Intersects two already-sorted streams while retaining the request-range
/// order Go gives to one region task. A point range (`start == end`) is kept
/// only in the bucket that contains that point.
fn intersect_sorted_ranges(
    ranges: &[impl Borrow<KeyRange>],
    bucket: &Range<&[u8]>,
    cursor: &mut usize,
    shared: Option<&Arc<PartitionedKeyRanges>>,
) -> KeyRanges {
    while ranges
        .get(*cursor)
        .is_some_and(|range| ends_before_bucket(range.borrow(), bucket))
    {
        *cursor += 1;
    }
    let start = *cursor;
    if let Some(shared) = shared {
        return clip_shared_ranges(shared, start, bucket);
    }
    let mut fragments = Vec::new();
    for range in ranges[start..].iter().map(Borrow::borrow) {
        if starts_at_or_after_bucket_end(range, bucket) {
            break;
        }
        if let Some(fragment) = intersect_range(range, bucket) {
            fragments.push(fragment);
        }
    }
    fragments.into()
}

/// A validated input group intersects a bucket in one contiguous span.
/// Only the first and last ranges can require new boundaries.
fn clip_shared_ranges(
    shared: &Arc<PartitionedKeyRanges>,
    mut start: usize,
    bucket: &Range<&[u8]>,
) -> KeyRanges {
    let ranges = shared.first_partition_ranges();
    let mut end = if bucket.end.is_empty() {
        ranges.len()
    } else {
        start + ranges[start..].partition_point(|range| !starts_at_or_after_bucket_end(range, bucket))
    };
    if start == end {
        return KeyRanges::default();
    }
    let first = if ranges[start].start_key.as_slice() < bucket.start {
        let clipped = intersect_range(&ranges[start], bucket);
        start += 1;
        clipped
    } else {
        None
    };
    let last = if start < end && !bucket.end.is_empty() {
        let range = &ranges[end - 1];
        if range.end_key.is_empty() || range.end_key.as_slice() > bucket.end {
            end -= 1;
            intersect_range(range, bucket)
        } else {
            None
        }
    } else {
        None
    };
    KeyRanges::from_request(Arc::clone(shared), start..end, first, last)
}

fn ends_before_bucket(range: &KeyRange, bucket: &Range<&[u8]>) -> bool {
    let start = range.start_key.as_bytes();
    let end = range.end_key.as_bytes();
    if start == end {
        return start < bucket.start;
    }
    !end.is_empty() && end <= bucket.start
}

fn starts_at_or_after_bucket_end(range: &KeyRange, bucket: &Range<&[u8]>) -> bool {
    !bucket.end.is_empty() && range.start_key.as_bytes() >= bucket.end
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

fn row_count_hint_for_bucket(
    bucket: &Range<&[u8]>,
    originals: &[impl Borrow<KeyRange>],
    hints: &[usize],
    cursor: &mut usize,
) -> i64 {
    while originals
        .get(*cursor)
        .is_some_and(|range| ends_before_bucket(range.borrow(), bucket))
    {
        *cursor += 1;
    }
    let start = *cursor;
    originals[start..]
        .iter()
        .map(Borrow::borrow)
        .zip(&hints[start..])
        .take_while(|(original, _)| !starts_at_or_after_bucket_end(original, bucket))
        .map(|(_, hint)| i64::try_from(*hint).unwrap_or(i64::MAX))
        .sum()
}

fn coverage_scan<'a>(
    ranges: impl IntoIterator<Item = &'a KeyRange>,
    tasks: &[RegionTaskEnvelope],
) -> (bool, usize) {
    let mut fragments = tasks.iter().flat_map(|task| task.ranges.iter());
    let mut examined_fragments = 0;
    for range in ranges {
        let Some(mut fragment) = fragments.next() else {
            return (false, examined_fragments);
        };
        examined_fragments += 1;
        if std::ptr::eq(fragment, range) {
            continue;
        }
        if range.start_key == range.end_key {
            if fragment.start_key != range.start_key || fragment.end_key != range.end_key {
                return (false, examined_fragments);
            }
            continue;
        }

        let mut expected_start = range.start_key.as_bytes();
        loop {
            if fragment.start_key.as_slice() != expected_start {
                return (false, examined_fragments);
            }

            let fragment_end = fragment.end_key.as_slice();
            let range_end = range.end_key.as_bytes();
            if range_end.is_empty() {
                if fragment_end.is_empty() {
                    break;
                }
            } else {
                if fragment_end.is_empty() || fragment_end > range_end {
                    return (false, examined_fragments);
                }
                if fragment_end == range_end {
                    break;
                }
            }
            expected_start = fragment_end;
            let Some(next) = fragments.next() else {
                return (false, examined_fragments);
            };
            fragment = next;
            examined_fragments += 1;
        }
    }

    let covered = fragments.next().is_none();
    (covered, examined_fragments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_validation_is_linear_for_many_point_ranges() {
        let mut ranges = Vec::new();
        let mut fragments = Vec::new();
        for value in 0_u32..4_097 {
            let key = value.to_be_bytes().to_vec();
            let range = KeyRange {
                start_key: key.clone().into(),
                end_key: key.into(),
            };
            ranges.push(range.clone());
            fragments.push(range);
        }
        let tasks = [RegionTaskEnvelope {
            ranges: fragments.into(),
            ..RegionTaskEnvelope::default()
        }];

        let (covered, examined_fragments) = coverage_scan(&ranges, &tasks);
        assert!(covered);
        assert!(
            examined_fragments <= 2 * ranges.len(),
            "coverage validation examined {examined_fragments} fragments for {} ranges",
            ranges.len()
        );
    }

    #[test]
    fn bucket_tasks_share_the_unchanged_middle_range() {
        let range = |start: &[u8], end: &[u8]| KeyRange::new(start.into(), end.into());
        let mut metadata = KvRequestMetadata::default();
        let input = Arc::new(PartitionedKeyRanges::new_non_partitioned(vec![
            range(b"a", b"c"),
            range(b"d", b"f"),
            range(b"g", b"z"),
        ]));
        metadata.key_ranges = Some(input.clone());
        let topology = [RegionTaskTopology {
            bucket_keys: vec![b"b".to_vec(), b"m".to_vec()],
            ..Default::default()
        }];
        let tasks = build_region_tasks(&metadata, &topology).unwrap();
        assert_eq!(tasks.len(), 3);
        assert_eq!(tasks[1].ranges[0], range(b"b", b"c"));
        assert!(std::ptr::eq(&tasks[1].ranges[1], &input.first_partition_ranges()[1]));
        assert_eq!(tasks[1].ranges[2], range(b"g", b"m"));
    }

    /// Every region task carries the row-count hint of the ranges that fall
    /// inside it, and a request whose ranges are not monotonic keeps no hint
    /// at all (`-1`) because sorting them breaks the alignment Go relies on.
    #[test]
    fn region_tasks_carry_the_hints_of_the_ranges_they_cover() {
        let key = |value: u32| value.to_be_bytes().to_vec();
        let range = |start: u32, end: u32| tidb_txnkv::KeyRange {
            start_key: key(start).into(),
            end_key: key(end).into(),
        };
        let topology = |split: u32| {
            vec![
                RegionTaskTopology {
                    region_id: 1,
                    start_key: Vec::new(),
                    end_key: key(split),
                    ..RegionTaskTopology::default()
                },
                RegionTaskTopology {
                    region_id: 2,
                    start_key: key(split),
                    end_key: Vec::new(),
                    ..RegionTaskTopology::default()
                },
            ]
        };
        let metadata_for = |ranges: Vec<tidb_txnkv::KeyRange>, hints: Vec<usize>| {
            let mut request = tidb_txnkv::Request::default();
            request.key_ranges = Some(std::sync::Arc::new(
                tidb_txnkv::PartitionedKeyRanges::new_non_partitioned_with_hints(ranges, hints),
            ));
            KvRequestMetadata::from_request(request)
        };

        let sorted = metadata_for(
            vec![range(10, 11), range(20, 21), range(40, 41)],
            vec![3, 5, 7],
        );
        let tasks = build_region_tasks(&sorted, &topology(30)).expect("tasks");
        let hints: Vec<i64> = tasks.iter().map(|task| task.row_count_hint).collect();
        assert_eq!(
            hints,
            vec![8, 7],
            "the first region holds the 3 and 5 ranges"
        );

        let mut partitioned = sorted.clone();
        partitioned.key_ranges = Some(
            tidb_txnkv::PartitionedKeyRanges::new_partitioned_with_hints(
                vec![vec![range(10, 11)], vec![], vec![range(20, 21), range(40, 41)]],
                vec![vec![3], vec![], vec![5, 7]],
            )
            .into(),
        );
        assert_eq!(build_region_tasks(&partitioned, &topology(30)), Some(tasks));

        // The same ranges out of order: the sort that repairs them also
        // detaches the hints, so no task may claim one.
        let unsorted = metadata_for(
            vec![range(40, 41), range(10, 11), range(20, 21)],
            vec![7, 3, 5],
        );
        let tasks = build_region_tasks(&unsorted, &topology(30)).expect("tasks");
        assert!(
            tasks.iter().all(|task| task.row_count_hint == -1),
            "reordered ranges keep no hints"
        );
    }
}

fn batch_tasks(
    tasks: Vec<RegionTaskEnvelope>,
    batch_size: u64,
    allow_unhinted_merge: bool,
) -> Vec<RegionTaskEnvelope> {
    let batch_size = usize::try_from(batch_size).unwrap_or(usize::MAX).max(1);
    let mut result = Vec::new();
    let mut store_parent = BTreeMap::<u64, usize>::new();
    for (index, mut task) in tasks.into_iter().enumerate() {
        task.task_id = u64::try_from(index + 1).unwrap_or(u64::MAX);
        let small = task.store_batch_eligible
            && (allow_unhinted_merge || (task.row_count_hint > 0 && task.row_count_hint <= 32));
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
        parent.row_count_hint =
            if allow_unhinted_merge && (parent.row_count_hint <= 0 || task.row_count_hint <= 0) {
                -1
            } else {
                parent.row_count_hint.saturating_add(task.row_count_hint)
            };
        parent.batch_task_list.push(task);
    }
    result
}
