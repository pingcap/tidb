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

//! Direct pre-transport composition of checked coprocessor read tasks.
//!
//! Go constructs region tasks, prepares the cache predicate, restores a cache
//! hit, advances paging, and publishes the response in that order. This module
//! composes the same already-translated authorities without inventing a region
//! cache, retry loop, endpoint, or RPC transport.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use tidb_proto::CoprocessorResponse;

use super::{CopPagingError, CopPagingOutcome, CopPagingState, ReadEngineGeneration};
use crate::{
    region_task::build_region_tasks, CoprCache, CoprCacheError, CoprCacheLookup,
    CoprCacheRequestContext, CoprCacheResponseContext, CoprCacheResponseOutcome,
    CoprocessorRequestEnvelope, KvRequestMetadata, ReadBytesEma, RegionTaskEnvelope,
    RegionTaskTopology, RequestKeyRange, RequestType, ResponseChannelEvent, StoreType,
};

const MAX_RANGES_PER_TASK_BUILD: usize = 25_000;

/// One immutable request attempt prepared before a transport owner exists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedCopReadTask {
    attempt_id: u64,
    logical_task_id: u64,
    page_index: u32,
    task: RegionTaskEnvelope,
    request: CoprocessorRequestEnvelope,
    cache_key: Option<Vec<u8>>,
}

impl PreparedCopReadTask {
    /// Unique identifier used to match exactly one response.
    #[must_use]
    pub const fn attempt_id(&self) -> u64 {
        self.attempt_id
    }

    /// Stable task identifier shared by all paging attempts for one task.
    #[must_use]
    pub const fn logical_task_id(&self) -> u64 {
        self.logical_task_id
    }

    /// Iterator-wide paging-task index assigned in preparation order.
    ///
    /// Like Go's shared `atomic.AddUint32`, active paging attempts are
    /// one-based and wrap at `u32::MAX`; non-paging attempts retain zero.
    #[must_use]
    pub const fn page_index(&self) -> u32 {
        self.page_index
    }

    /// Checked region task used to build this attempt.
    #[must_use]
    pub const fn task(&self) -> &RegionTaskEnvelope {
        &self.task
    }

    /// Immutable task-local coprocessor request including cache predicate.
    #[must_use]
    pub const fn request(&self) -> &CoprocessorRequestEnvelope {
        &self.request
    }

    /// Exact cache key retained with the matching in-flight lookup.
    #[must_use]
    pub fn cache_key(&self) -> Option<&[u8]> {
        self.cache_key.as_deref()
    }
}

#[derive(Debug)]
struct InFlightCopReadTask {
    prepared: Arc<PreparedCopReadTask>,
    lookup: Option<CoprCacheLookup>,
    logical_task_index: usize,
}

struct LogicalCopReadTask {
    task: RegionTaskEnvelope,
    paging: CopPagingState,
}

/// Error fields carried by the same transport response as its protobuf body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CopReadResponseError {
    /// The response carries a region error requiring refresh and backoff.
    Region,
    /// The response carries a lock requiring resolution and backoff.
    Lock,
    /// The response carries TiKV's opaque other-error string.
    Other(String),
    /// The response belongs to the unsupported batch protocol.
    Batch,
}

/// One transport response envelope presented atomically to the coordinator.
///
/// The raw-response owner decodes protobuf region, lock, other, and batch
/// fields completely, then projects their precedence into this explicit enum
/// before presenting the envelope here. This preserves Go's single atomic
/// response-ordering boundary without giving the coordinator a transport.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CopReadTaskResponse {
    response: CoprocessorResponse,
    error: Option<CopReadResponseError>,
}

impl CopReadTaskResponse {
    /// Creates a successful response envelope.
    #[must_use]
    pub fn success(response: CoprocessorResponse) -> Self {
        Self {
            response,
            error: None,
        }
    }

    /// Creates a response carrying a region error.
    #[must_use]
    pub fn region_error(response: CoprocessorResponse) -> Self {
        Self {
            response,
            error: Some(CopReadResponseError::Region),
        }
    }

    /// Creates a response carrying a lock error.
    #[must_use]
    pub fn lock_error(response: CoprocessorResponse) -> Self {
        Self {
            response,
            error: Some(CopReadResponseError::Lock),
        }
    }

    /// Creates a response carrying TiKV's opaque other error.
    #[must_use]
    pub fn other_error(response: CoprocessorResponse, message: impl Into<String>) -> Self {
        Self {
            response,
            error: Some(CopReadResponseError::Other(message.into())),
        }
    }

    /// Creates a response from the unsupported batch protocol.
    #[must_use]
    pub fn batch(response: CoprocessorResponse) -> Self {
        Self {
            response,
            error: Some(CopReadResponseError::Batch),
        }
    }
}

impl From<CoprocessorResponse> for CopReadTaskResponse {
    fn from(response: CoprocessorResponse) -> Self {
        Self::success(response)
    }
}

/// Result of accepting one matched response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopReadAcceptedResponse {
    /// Stable logical task whose channel received the response.
    pub logical_task_id: u64,
    /// Cache lifecycle result when a cache owner was enabled.
    pub cache_outcome: Option<CoprCacheResponseOutcome>,
    /// Paging continuation after cache restoration and response publication.
    pub paging: CopPagingOutcome,
    /// Newly prepared continuation attempt, if ranges remain.
    pub next_attempt_id: Option<u64>,
}

/// Exact fail-closed boundaries of the pre-transport coordinator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CopReadTaskError {
    /// Only TiKV has the unary coprocessor/cache contract used here.
    UnsupportedStore,
    /// Only DAG requests are admitted by this bounded runtime.
    UnsupportedRequestType,
    /// Batch coprocessor requests require a separate transport protocol.
    BatchCoprocessor,
    /// Store batching requires shared/unordered response dispatch.
    StoreBatching,
    /// Unordered response publication is not owned by the task-local channel.
    UnorderedResponse,
    /// Partitioned range envelopes require a partition-aware coordinator.
    PartitionedRanges,
    /// Statement-wide max-keys accounting has no transport feedback owner yet.
    MaxKeysRead,
    /// No request ranges were supplied.
    MissingRanges,
    /// The source guard rejects more than 25,000 ranges per task build.
    TooManyRanges,
    /// Ranges are malformed, overlapping, or non-monotonic.
    InvalidRanges,
    /// Region or bucket topology is malformed, stale, outside, or incomplete.
    InvalidTopology,
    /// A response did not match a currently active attempt.
    UnmatchedResponse,
    /// A completed attempt received a second response.
    DuplicateResponse,
    /// Region errors require a future refresh and backoff owner.
    RegionError,
    /// Lock errors require a future lock resolver and backoff owner.
    LockError,
    /// Other TiKV errors require the future transport error pipeline.
    OtherError(String),
    /// Batch responses require shared/unordered dispatch and child matching.
    BatchResponse,
    /// TiKV observed bucket metadata newer than the prepared task.
    NewerBuckets {
        /// Bucket version carried by the prepared task.
        request_version: u64,
        /// Newer version returned by TiKV.
        latest_version: u64,
    },
    /// Cache restoration or admission rejected the response.
    Cache(CoprCacheError),
    /// Bounded response publication or paging advancement failed.
    Paging(CopPagingError),
}

impl CopReadTaskError {
    /// Stable category used by source-shaped fail-closed tests.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::UnsupportedStore => "unsupported_store",
            Self::UnsupportedRequestType => "unsupported_request_type",
            Self::BatchCoprocessor => "batch_coprocessor",
            Self::StoreBatching => "store_batching",
            Self::UnorderedResponse => "unordered_response",
            Self::PartitionedRanges => "partitioned_ranges",
            Self::MaxKeysRead => "max_keys_read",
            Self::MissingRanges => "missing_ranges",
            Self::TooManyRanges => "too_many_ranges",
            Self::InvalidRanges => "invalid_ranges",
            Self::InvalidTopology => "invalid_topology",
            Self::UnmatchedResponse => "unmatched_response",
            Self::DuplicateResponse => "duplicate_response",
            Self::RegionError => "region_error",
            Self::LockError => "lock_error",
            Self::OtherError(_) => "other_error",
            Self::BatchResponse => "batch_response",
            Self::NewerBuckets { .. } => "newer_buckets",
            Self::Cache(_) => "cache",
            Self::Paging(_) => "paging",
        }
    }
}

impl std::fmt::Display for CopReadTaskError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OtherError(message) => write!(formatter, "coprocessor other error: {message}"),
            Self::NewerBuckets {
                request_version,
                latest_version,
            } => write!(
                formatter,
                "bucket version {latest_version} is newer than prepared version {request_version}"
            ),
            Self::Cache(error) => error.fmt(formatter),
            Self::Paging(error) => error.fmt(formatter),
            _ => formatter.write_str(self.kind()),
        }
    }
}

impl std::error::Error for CopReadTaskError {}

impl From<CoprCacheError> for CopReadTaskError {
    fn from(error: CoprCacheError) -> Self {
        Self::Cache(error)
    }
}

impl From<CopPagingError> for CopReadTaskError {
    fn from(error: CopPagingError) -> Self {
        Self::Paging(error)
    }
}

/// Deterministic coordinator that stops immediately before RPC transport.
pub struct CopReadTaskRuntime {
    metadata: KvRequestMetadata,
    tasks: Vec<LogicalCopReadTask>,
    in_flight: BTreeMap<u64, InFlightCopReadTask>,
    prepared: Vec<Arc<PreparedCopReadTask>>,
    completed_attempts: BTreeSet<u64>,
    next_attempt_id: u64,
    next_paging_task_index: u32,
    cache: Option<CoprCache>,
    ema: Arc<ReadBytesEma>,
}

impl std::fmt::Debug for CopReadTaskRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CopReadTaskRuntime")
            .field("logical_tasks", &self.tasks.len())
            .field("in_flight", &self.in_flight.keys().collect::<Vec<_>>())
            .field("prepared_attempts", &self.prepared.len())
            .field("completed_attempts", &self.completed_attempts)
            .field("cache_enabled", &self.cache.is_some())
            .field("predicted_read_bytes", &self.ema.predict())
            .finish()
    }
}

impl CopReadTaskRuntime {
    pub(super) fn prepare(
        metadata: &KvRequestMetadata,
        topology: &[RegionTaskTopology],
        cache: Option<CoprCache>,
        generation: ReadEngineGeneration,
        seed_read_bytes: u64,
    ) -> Result<Self, CopReadTaskError> {
        validate_request(metadata)?;
        validate_topology(topology)?;
        let mut envelopes =
            build_region_tasks(metadata, topology).ok_or(CopReadTaskError::InvalidTopology)?;
        if envelopes
            .iter()
            .any(|task| !task.batch_task_list.is_empty())
        {
            return Err(CopReadTaskError::StoreBatching);
        }

        let ema = Arc::new(ReadBytesEma::new(seed_read_bytes));
        let tasks = envelopes
            .iter_mut()
            .enumerate()
            .map(|(index, task)| {
                task.task_id = u64::try_from(index + 1).unwrap_or(u64::MAX);
                LogicalCopReadTask {
                    paging: CopPagingState::new_with_shared_ema(
                        task,
                        metadata.desc,
                        metadata.session.paging.max_size,
                        generation,
                        ema.clone(),
                    ),
                    task: task.clone(),
                }
            })
            .collect();
        let mut runtime = Self {
            metadata: metadata.clone(),
            tasks,
            in_flight: BTreeMap::new(),
            prepared: Vec::new(),
            completed_attempts: BTreeSet::new(),
            next_attempt_id: 1,
            next_paging_task_index: 0,
            cache,
            ema,
        };
        for logical_task_index in 0..runtime.tasks.len() {
            let ranges = runtime.tasks[logical_task_index].task.ranges.clone();
            let paging_size = runtime.tasks[logical_task_index].task.paging_size;
            runtime.prepare_attempt(logical_task_index, ranges, paging_size)?;
        }
        Ok(runtime)
    }

    /// Returns all immutable attempts in source preparation order.
    pub fn prepared_attempts(&self) -> impl Iterator<Item = &PreparedCopReadTask> {
        self.prepared.iter().map(AsRef::as_ref)
    }

    /// Returns one immutable prepared attempt by its response-matching ID.
    #[must_use]
    pub fn prepared_attempt(&self, attempt_id: u64) -> Option<&PreparedCopReadTask> {
        self.prepared
            .iter()
            .find(|prepared| prepared.attempt_id == attempt_id)
            .map(AsRef::as_ref)
    }

    /// Returns the active attempt IDs in deterministic order.
    #[must_use]
    pub fn in_flight_attempt_ids(&self) -> Vec<u64> {
        self.in_flight.keys().copied().collect()
    }

    /// Returns the prediction shared by all logical tasks.
    #[must_use]
    pub fn predicted_read_bytes(&self) -> u64 {
        self.ema.predict()
    }

    /// Returns one task's view of the shared EMA prediction.
    #[must_use]
    pub fn task_predicted_read_bytes(&self, logical_task_id: u64) -> Option<u64> {
        self.tasks
            .iter()
            .find(|task| task.task.task_id == logical_task_id)
            .map(|task| task.paging.predicted_read_bytes())
    }

    /// Returns the bounded cache owner for exact-key inspection.
    #[must_use]
    pub const fn cache(&self) -> Option<&CoprCache> {
        self.cache.as_ref()
    }

    /// Returns the bounded cache owner after all in-flight attempts are done.
    pub fn into_cache(self) -> Result<Option<CoprCache>, CopReadTaskError> {
        if self.in_flight.is_empty() {
            Ok(self.cache)
        } else {
            Err(CopReadTaskError::UnmatchedResponse)
        }
    }

    /// Restores cache state, advances paging, and publishes one matched response.
    pub fn accept_response(
        &mut self,
        attempt_id: u64,
        response: impl Into<CopReadTaskResponse>,
        process_time_nanos: Option<i64>,
        now: Duration,
    ) -> Result<CopReadAcceptedResponse, CopReadTaskError> {
        let (prepared, lookup, logical_task_index) = {
            let in_flight = self.require_in_flight(attempt_id)?;
            (
                in_flight.prepared.clone(),
                in_flight.lookup.clone(),
                in_flight.logical_task_index,
            )
        };
        let response = response.into();
        if let Some(error) = response.error {
            return Err(match error {
                CopReadResponseError::Region => CopReadTaskError::RegionError,
                CopReadResponseError::Lock => CopReadTaskError::LockError,
                CopReadResponseError::Other(message) => CopReadTaskError::OtherError(message),
                CopReadResponseError::Batch => CopReadTaskError::BatchResponse,
            });
        }
        let mut response = response.response;
        if response.latest_buckets_version > prepared.task.buckets_version {
            return Err(CopReadTaskError::NewerBuckets {
                request_version: prepared.task.buckets_version,
                latest_version: response.latest_buckets_version,
            });
        }

        // Cache restoration/insertion is mutable. Prove that the task-local
        // response queue can accept this page before touching the cache, EMA,
        // ranges, in-flight ownership, or continuation state.
        self.tasks[logical_task_index]
            .paging
            .preflight_accept_response()?;

        // Go restores a cache hit before paging consumes response data/range.
        let cache_outcome = if let Some(cache) = self.cache.as_mut() {
            Some(cache.handle_response(
                &mut response,
                lookup.as_ref(),
                CoprCacheResponseContext {
                    start_ts: self.metadata.start_ts,
                    region_id: prepared.task.region_id,
                    process_time_nanos,
                    paging_task_index: prepared.page_index,
                    paging_enabled: prepared.task.paging
                        || self.metadata.session.paging.size_bytes > 0,
                },
            )?)
        } else if response.is_cache_hit {
            return Err(CopReadTaskError::Cache(CoprCacheError::IllegalCacheHit));
        } else {
            None
        };
        let paging = self.tasks[logical_task_index]
            .paging
            .accept_response(&response, now)?;

        self.in_flight.remove(&attempt_id);
        self.completed_attempts.insert(attempt_id);
        let next_attempt_id = if paging.remaining_ranges.is_empty() {
            None
        } else {
            Some(self.prepare_attempt(
                logical_task_index,
                paging.remaining_ranges.clone(),
                paging.next_paging_size,
            )?)
        };
        Ok(CopReadAcceptedResponse {
            logical_task_id: prepared.logical_task_id,
            cache_outcome,
            paging,
            next_attempt_id,
        })
    }

    /// Fails closed on a region error without consuming the matched attempt.
    pub fn accept_region_error(&self, attempt_id: u64) -> Result<(), CopReadTaskError> {
        self.require_in_flight(attempt_id)?;
        Err(CopReadTaskError::RegionError)
    }

    /// Fails closed on a lock error without inventing lock resolution.
    pub fn accept_lock_error(&self, attempt_id: u64) -> Result<(), CopReadTaskError> {
        self.require_in_flight(attempt_id)?;
        Err(CopReadTaskError::LockError)
    }

    /// Fails closed on an opaque TiKV error without interpreting its identity.
    pub fn accept_other_error(
        &self,
        attempt_id: u64,
        message: impl Into<String>,
    ) -> Result<(), CopReadTaskError> {
        self.require_in_flight(attempt_id)?;
        Err(CopReadTaskError::OtherError(message.into()))
    }

    /// Fails closed on batch response delivery.
    pub fn accept_batch_response(&self, attempt_id: u64) -> Result<(), CopReadTaskError> {
        self.require_in_flight(attempt_id)?;
        Err(CopReadTaskError::BatchResponse)
    }

    /// Drains the next bounded response event for one logical task.
    pub fn next_response(&mut self, logical_task_id: u64) -> Option<ResponseChannelEvent<Vec<u8>>> {
        self.tasks
            .iter_mut()
            .find(|task| task.task.task_id == logical_task_id)
            .and_then(|task| task.paging.next_response())
    }

    fn require_in_flight(&self, attempt_id: u64) -> Result<&InFlightCopReadTask, CopReadTaskError> {
        self.in_flight.get(&attempt_id).ok_or_else(|| {
            if self.completed_attempts.contains(&attempt_id) {
                CopReadTaskError::DuplicateResponse
            } else {
                CopReadTaskError::UnmatchedResponse
            }
        })
    }

    fn prepare_attempt(
        &mut self,
        logical_task_index: usize,
        ranges: Vec<RequestKeyRange>,
        paging_size: u64,
    ) -> Result<u64, CopReadTaskError> {
        let base_task = &self.tasks[logical_task_index].task;
        let mut task = base_task.clone();
        task.ranges.clone_from(&ranges);
        task.paging_size = paging_size;
        let page_index = allocate_paging_task_index(
            &mut self.next_paging_task_index,
            task.paging || self.metadata.session.paging.size_bytes > 0,
        );
        let mut request = CoprocessorRequestEnvelope::from_metadata(&self.metadata, ranges)
            .with_paging_size(paging_size);
        let lookup = self.cache.as_ref().and_then(|cache| {
            cache.prepare_request(
                &mut request,
                CoprCacheRequestContext {
                    is_unary_cop: true,
                    cacheable: self.metadata.cacheable,
                    region_id: task.region_id,
                    start_ts: self.metadata.start_ts,
                },
            )
        });
        let attempt_id = self.next_attempt_id;
        self.next_attempt_id = self.next_attempt_id.saturating_add(1);
        let prepared = Arc::new(PreparedCopReadTask {
            attempt_id,
            logical_task_id: task.task_id,
            page_index,
            task,
            request,
            cache_key: lookup.as_ref().map(|lookup| lookup.key().to_vec()),
        });
        self.prepared.push(prepared.clone());
        self.in_flight.insert(
            attempt_id,
            InFlightCopReadTask {
                prepared,
                lookup,
                logical_task_index,
            },
        );
        Ok(attempt_id)
    }
}

fn allocate_paging_task_index(counter: &mut u32, paging_active: bool) -> u32 {
    if !paging_active {
        return 0;
    }
    *counter = counter.wrapping_add(1);
    *counter
}

pub(super) fn validate_request(metadata: &KvRequestMetadata) -> Result<(), CopReadTaskError> {
    if metadata.store_type != StoreType::TiKv {
        return Err(CopReadTaskError::UnsupportedStore);
    }
    if metadata.request_type != RequestType::Dag {
        return Err(CopReadTaskError::UnsupportedRequestType);
    }
    if metadata.batch_cop {
        return Err(CopReadTaskError::BatchCoprocessor);
    }
    if metadata.session.store_batch_size > 0 {
        return Err(CopReadTaskError::StoreBatching);
    }
    if !metadata.keep_order {
        return Err(CopReadTaskError::UnorderedResponse);
    }
    if !metadata.partition_id_and_ranges.is_empty() {
        return Err(CopReadTaskError::PartitionedRanges);
    }
    if metadata.session.max_keys_read > 0 || metadata.session.max_keys_read_counter.is_some() {
        return Err(CopReadTaskError::MaxKeysRead);
    }
    let ranges = metadata
        .key_ranges
        .as_ref()
        .ok_or(CopReadTaskError::MissingRanges)?;
    if ranges.partitioned || ranges.partitions.len() != 1 {
        return Err(CopReadTaskError::PartitionedRanges);
    }
    let ranges = ranges
        .partitions
        .first()
        .ok_or(CopReadTaskError::MissingRanges)?;
    if ranges.is_empty() {
        return Err(CopReadTaskError::MissingRanges);
    }
    if ranges.len() > MAX_RANGES_PER_TASK_BUILD {
        return Err(CopReadTaskError::TooManyRanges);
    }
    if !ranges_are_strictly_monotonic(ranges) {
        return Err(CopReadTaskError::InvalidRanges);
    }
    Ok(())
}

fn ranges_are_strictly_monotonic(ranges: &[RequestKeyRange]) -> bool {
    ranges.iter().enumerate().all(|(index, range)| {
        if range.end_key.is_empty() {
            return index + 1 == ranges.len();
        }
        if range.start_key >= range.end_key {
            return false;
        }
        index == 0 || ranges[index - 1].end_key <= range.start_key
    })
}

fn validate_topology(topology: &[RegionTaskTopology]) -> Result<(), CopReadTaskError> {
    if topology.is_empty() {
        return Err(CopReadTaskError::InvalidTopology);
    }
    let mut region_ids = BTreeSet::new();
    let mut previous_start: Option<&[u8]> = None;
    let mut previous_end: Option<&[u8]> = None;
    for region in topology {
        if region.region_id == 0 || !region_ids.insert(region.region_id) {
            return Err(CopReadTaskError::InvalidTopology);
        }
        if !region.end_key.is_empty() && region.start_key >= region.end_key {
            return Err(CopReadTaskError::InvalidTopology);
        }
        if previous_start.is_some_and(|start| start >= region.start_key.as_slice())
            || previous_end.is_some_and(|end| end.is_empty() || end > region.start_key.as_slice())
        {
            return Err(CopReadTaskError::InvalidTopology);
        }
        if !bucket_keys_are_current(region) {
            return Err(CopReadTaskError::InvalidTopology);
        }
        previous_start = Some(&region.start_key);
        previous_end = Some(&region.end_key);
    }
    Ok(())
}

fn bucket_keys_are_current(region: &RegionTaskTopology) -> bool {
    if region.bucket_keys.is_empty() {
        return true;
    }
    if region.buckets_version == 0 {
        return false;
    }
    region.bucket_keys.iter().enumerate().all(|(index, key)| {
        key.as_slice() > region.start_key.as_slice()
            && (region.end_key.is_empty() || key.as_slice() < region.end_key.as_slice())
            && (index == 0 || region.bucket_keys[index - 1].as_slice() < key.as_slice())
    })
}

#[cfg(test)]
mod tests {
    use super::allocate_paging_task_index;

    #[test]
    fn paging_task_index_is_one_based_iterator_wide_and_wrapping() {
        let mut counter = 0;
        assert_eq!(allocate_paging_task_index(&mut counter, true), 1);
        assert_eq!(allocate_paging_task_index(&mut counter, true), 2);

        counter = u32::MAX;
        assert_eq!(allocate_paging_task_index(&mut counter, true), 0);

        let before = counter;
        assert_eq!(allocate_paging_task_index(&mut counter, false), 0);
        assert_eq!(counter, before);
    }
}
