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

//! Connected adaptive-paging continuation from the coprocessor worker.

mod cop_read_task_runtime;
mod direct_unary_query_transport;
mod tikv_rpc_contract;
mod transport_failure;

pub use cop_read_task_runtime::{
    CopReadAcceptedResponse, CopReadResponseError, CopReadTaskError, CopReadTaskReplacement,
    CopReadTaskResponse, CopReadTaskRuntime, FailedCopReadAttempt, PreparedCopReadTask,
};
pub use direct_unary_query_transport::{
    DirectUnaryClient, DirectUnaryClientError, DirectUnaryQueryResponse, DirectUnaryQueryTransport,
    DirectUnaryRequest, DirectUnaryResponse, DirectUnaryRuntimeConfig, DirectUnaryTransportError,
    RegionRetryCancelled, RegionRetryControl,
};
pub use tikv_rpc_contract::{
    build_tikv_unary_request, build_tikv_unary_request_for_dispatch, decode_tikv_unary_response,
    TikvUnaryRequest,
};
pub use transport_failure::{classify_transport_failure, TransportFailureAction};

use std::sync::Arc;
use std::time::Duration;

use tidb_proto::{CoprocessorKeyRange, CoprocessorResponse};
use tidb_txnkv::{Key, KeyRange, KeyRanges};

use crate::{
    grow_paging_size, CoprCache, KvRequestMetadata, ReadBytesEma, RegionTaskEnvelope,
    RegionTaskTopology, ResponseChannel, ResponseChannelError, ResponseChannelEvent,
};

/// Storage-engine generation controlling the billed MVCC-byte basis.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadEngineGeneration {
    /// Classic TiKV bills processed MVCC bytes.
    Classic,
    /// Next-generation TiKV bills the larger of total and processed bytes.
    NextGeneration,
}

/// Extracts the MVCC byte count observed by adaptive paging.
#[must_use]
pub fn paging_response_read_bytes(
    response: Option<&CoprocessorResponse>,
    generation: ReadEngineGeneration,
) -> u64 {
    let Some(scan) = response
        .and_then(|response| response.exec_details_v2.as_ref())
        .and_then(|details| details.scan_detail_v2.as_ref())
    else {
        return 0;
    };
    match generation {
        ReadEngineGeneration::Classic => scan.processed_versions_size,
        ReadEngineGeneration::NextGeneration => {
            scan.total_versions_size.max(scan.processed_versions_size)
        }
    }
}

/// Calculates ranges that must be retried after a partial paging response.
#[must_use]
pub fn calculate_paging_retry(
    ranges: &KeyRanges,
    split: Option<&CoprocessorKeyRange>,
    descending: bool,
) -> KeyRanges {
    let Some(split) = split else {
        return ranges.clone();
    };
    if descending {
        ranges.split(&Key::from_bytes(split.end.as_slice())).0
    } else {
        ranges.split(&Key::from_bytes(split.start.as_slice())).1
    }
}

/// Calculates unconsumed ranges after a successful paging response.
#[must_use]
pub fn calculate_paging_remain(
    ranges: &KeyRanges,
    split: Option<&CoprocessorKeyRange>,
    descending: bool,
) -> KeyRanges {
    let Some(split) = split else {
        return ranges.clone();
    };
    if descending {
        ranges.split(&Key::from_bytes(split.start.as_slice())).0
    } else {
        ranges.split(&Key::from_bytes(split.end.as_slice())).1
    }
}

/// Result of consuming one successful coprocessor page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopPagingOutcome {
    /// Ranges still requiring a request.
    pub remaining_ranges: Vec<crate::RequestKeyRange>,
    /// Page size for the next request, or zero when paging is complete.
    pub next_paging_size: u64,
    /// MVCC bytes contributed to the EMA by this response.
    pub observed_read_bytes: u64,
}

/// Failure to publish or advance one paging response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CopPagingError {
    /// The bounded response queue must be drained before another page arrives.
    Backpressure {
        /// Maximum number of queued response payloads.
        capacity: usize,
    },
    /// The owned response channel rejected a lifecycle transition.
    ResponseChannel(ResponseChannelError),
}

impl std::fmt::Display for CopPagingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backpressure { capacity } => {
                write!(
                    formatter,
                    "paging response queue capacity {capacity} is full"
                )
            }
            Self::ResponseChannel(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for CopPagingError {}

impl From<ResponseChannelError> for CopPagingError {
    fn from(error: ResponseChannelError) -> Self {
        Self::ResponseChannel(error)
    }
}

/// One task's adaptive paging and response-publication state.
pub struct CopPagingState {
    ranges: KeyRanges,
    descending: bool,
    paging_size: u64,
    complete: bool,
    max_paging_size: u64,
    generation: ReadEngineGeneration,
    ema: Arc<ReadBytesEma>,
    responses: ResponseChannel<Vec<u8>>,
    response_capacity: usize,
    queued_responses: usize,
}

impl CopPagingState {
    /// Creates continuation state from one built region task.
    #[must_use]
    pub fn new(
        task: &RegionTaskEnvelope,
        descending: bool,
        max_paging_size: u64,
        generation: ReadEngineGeneration,
        seed_read_bytes: u64,
    ) -> Self {
        Self::new_with_shared_ema(
            task,
            descending,
            max_paging_size,
            generation,
            Arc::new(ReadBytesEma::new(seed_read_bytes)),
        )
    }

    /// Constructs the dependency-closed pre-transport read-task runtime.
    ///
    /// The returned runtime owns immutable per-attempt request snapshots,
    /// cache lookup matching, task-local paging continuations, bounded ordered
    /// response publication, and one shared read-byte EMA. Region discovery,
    /// retries/backoff, endpoint selection, and RPC remain outside this API.
    pub fn prepare_read_tasks(
        metadata: &KvRequestMetadata,
        topology: &[RegionTaskTopology],
        cache: Option<CoprCache>,
        generation: ReadEngineGeneration,
        seed_read_bytes: u64,
    ) -> Result<CopReadTaskRuntime, CopReadTaskError> {
        CopReadTaskRuntime::prepare(metadata, topology, cache, generation, seed_read_bytes)
    }

    /// Validates the request-owned invariants before region discovery begins.
    ///
    /// The full preparation path repeats this check before constructing tasks;
    /// transports call this entry point first so an unsupported request cannot
    /// perform PD I/O or mutate a region cache merely to discover that it is
    /// unsendable.
    pub fn validate_read_request(metadata: &KvRequestMetadata) -> Result<(), CopReadTaskError> {
        cop_read_task_runtime::validate_request(metadata)
    }

    pub(super) fn new_with_shared_ema(
        task: &RegionTaskEnvelope,
        descending: bool,
        max_paging_size: u64,
        generation: ReadEngineGeneration,
        ema: Arc<ReadBytesEma>,
    ) -> Self {
        let ranges = task
            .ranges
            .iter()
            .map(|range| {
                KeyRange::new(
                    Key::from_bytes(range.start_key.as_slice()),
                    Key::from_bytes(range.end_key.as_slice()),
                )
            })
            .collect();
        Self {
            ranges: KeyRanges::new(ranges),
            descending,
            paging_size: task.paging_size,
            complete: false,
            max_paging_size,
            generation,
            ema,
            responses: ResponseChannel::new(),
            // A zero source capacity denotes the non-ordered shared response
            // path. This task-local seam still needs one rendezvous slot so a
            // synchronous producer can hand the response to its consumer.
            response_capacity: task.response_channel_capacity.max(1),
            queued_responses: 0,
        }
    }

    pub(super) fn replace_ranges_for_region_retry(
        &mut self,
        ranges: Vec<crate::RequestKeyRange>,
        paging_size: u64,
    ) {
        self.ranges = KeyRanges::new(
            ranges
                .into_iter()
                .map(|range| {
                    KeyRange::new(
                        Key::from_bytes(range.start_key),
                        Key::from_bytes(range.end_key),
                    )
                })
                .collect(),
        );
        self.paging_size = paging_size;
        self.complete = false;
    }

    pub(super) const fn generation(&self) -> ReadEngineGeneration {
        self.generation
    }

    /// Checks the only fallible producer condition before a caller mutates
    /// cache or other response state. The coordinator holds `&mut self`
    /// across this check and the subsequent append, so the capacity cannot
    /// change between the two operations.
    pub(super) fn preflight_accept_response(&self) -> Result<(), CopPagingError> {
        if !self.complete && self.queued_responses >= self.response_capacity {
            return Err(CopPagingError::Backpressure {
                capacity: self.response_capacity,
            });
        }
        Ok(())
    }

    /// Publishes response data and advances the successful-page continuation.
    pub fn accept_response(
        &mut self,
        response: &CoprocessorResponse,
        now: Duration,
    ) -> Result<CopPagingOutcome, CopPagingError> {
        if self.complete {
            return Ok(self.completed_outcome());
        }
        self.preflight_accept_response()?;
        self.responses.push_result(response.data.clone())?;
        self.queued_responses += 1;
        let Some(page_range) = response.range.as_ref() else {
            self.ranges.reset(Vec::new());
            self.paging_size = 0;
            self.complete = true;
            self.responses.finish()?;
            return Ok(self.completed_outcome());
        };
        let read_bytes = paging_response_read_bytes(Some(response), self.generation);
        if read_bytes > 0 {
            self.ema.observe(read_bytes, now);
        }
        self.ranges = calculate_paging_remain(&self.ranges, Some(page_range), self.descending);
        if self.ranges.is_empty() {
            self.paging_size = 0;
            self.complete = true;
            self.responses.finish()?;
        } else {
            self.paging_size = grow_paging_size(self.paging_size, self.max_paging_size);
        }
        Ok(CopPagingOutcome {
            remaining_ranges: self
                .ranges
                .to_ranges()
                .into_iter()
                .map(|range| crate::RequestKeyRange {
                    start_key: range.start_key.as_bytes().to_vec(),
                    end_key: range.end_key.as_bytes().to_vec(),
                })
                .collect(),
            next_paging_size: self.paging_size,
            observed_read_bytes: read_bytes,
        })
    }

    /// Builds a retry continuation for a region or lock error without growing
    /// the current page size or observing response bytes.
    #[must_use]
    pub fn retry_after_error(&self, split: Option<&CoprocessorKeyRange>) -> CopPagingOutcome {
        if self.complete {
            return self.completed_outcome();
        }
        let retry = calculate_paging_retry(&self.ranges, split, self.descending);
        CopPagingOutcome {
            remaining_ranges: retry
                .to_ranges()
                .into_iter()
                .map(|range| crate::RequestKeyRange {
                    start_key: range.start_key.as_bytes().to_vec(),
                    end_key: range.end_key.as_bytes().to_vec(),
                })
                .collect(),
            next_paging_size: self.paging_size,
            observed_read_bytes: 0,
        }
    }

    /// Drains the next accepted response or terminal lifecycle event.
    pub fn next_response(&mut self) -> Option<ResponseChannelEvent<Vec<u8>>> {
        let event = self.responses.next_event();
        if matches!(
            event.as_ref(),
            Some(ResponseChannelEvent::Result(_) | ResponseChannelEvent::ResultWithRuntime { .. })
        ) {
            self.queued_responses -= 1;
        }
        event
    }

    /// Returns the current EMA prediction.
    #[must_use]
    pub fn predicted_read_bytes(&self) -> u64 {
        self.ema.predict()
    }

    fn completed_outcome(&self) -> CopPagingOutcome {
        CopPagingOutcome {
            remaining_ranges: Vec::new(),
            next_paging_size: 0,
            observed_read_bytes: 0,
        }
    }
}

/// Semantic cache update extracted from one failed batched child task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchBucketVersionUpdate {
    /// Child region whose cached buckets are stale.
    pub region_id: u64,
    /// Bucket version used by the failed request.
    pub request_version: u64,
    /// Latest version returned by TiKV.
    pub latest_version: u64,
    /// Replacement bucket split keys returned by TiKV.
    pub keys: Vec<Vec<u8>>,
}

impl BatchBucketVersionUpdate {
    /// Selects the addressed child, never the batch parent, and builds its update.
    #[must_use]
    pub fn for_child(
        parent: &RegionTaskEnvelope,
        task_id: u64,
        latest_version: u64,
        keys: Vec<Vec<u8>>,
    ) -> Option<Self> {
        let child = parent
            .batch_task_list
            .iter()
            .find(|task| task.task_id == task_id)?;
        Some(Self {
            region_id: child.region_id,
            request_version: child.buckets_version,
            latest_version,
            keys,
        })
    }

    /// Applies the returned version and raw keys to the matching topology entry.
    pub fn apply(&self, topology: &mut [RegionTaskTopology]) -> bool {
        let Some(region) = topology
            .iter_mut()
            .find(|region| region.region_id == self.region_id)
        else {
            return false;
        };
        region.buckets_version = self.latest_version;
        region.bucket_keys.clone_from(&self.keys);
        true
    }
}
