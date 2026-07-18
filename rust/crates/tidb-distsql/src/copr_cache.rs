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

//! Coprocessor cache lifecycle translated from
//! `pkg/store/copr/coprocessor_cache.go` and its worker call sites.
//!
//! [`CoprCache`] is the single owner for key construction, admission, bounded
//! storage, request preparation, collision-safe lookup, and response handling.
//! Its deterministic eviction order is intentionally not presented as
//! Ristretto's sampled LFU admission or asynchronous write buffer.
//! The crate still has no TiKV RPC owner, so these request/response methods are
//! the callable worker boundary rather than a claim that transport invokes it.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::mem;

use tidb_proto::{CoprocessorKeyRange, CoprocessorResponse};

use crate::CoprocessorRequestEnvelope;

const MEBIBYTE: f64 = 1024.0 * 1024.0;
const MILLISECOND_NANOS: i64 = 1_000_000;

/// Exact source errors emitted while building or configuring the cache policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoprCacheError {
    /// `coprocessor.Request.Tp` does not fit the one-byte cache-key field.
    RequestTypeTooBig,
    /// The opaque request data length does not fit its four-byte prefix.
    DataTooBig,
    /// A range start key does not fit its two-byte prefix.
    StartKeyTooBig,
    /// A range end key does not fit its two-byte prefix.
    EndKeyTooBig,
    /// Configured capacity truncates to a non-positive byte count.
    CapacityMustBePositive,
    /// Configured maximum result size truncates to zero bytes.
    AdmissionMaxResultMustBePositive,
    /// TiKV reported a cache hit without a matching local cache value.
    IllegalCacheHit,
}

impl fmt::Display for CoprCacheError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RequestTypeTooBig => "Request Tp too big",
            Self::DataTooBig => "Cache data too big",
            Self::StartKeyTooBig => "Cache start key too big",
            Self::EndKeyTooBig => "Cache end key too big",
            Self::CapacityMustBePositive => "Capacity must be > 0 to enable the cache",
            Self::AdmissionMaxResultMustBePositive => {
                "AdmissionMaxResultMB must be > 0 to enable the cache"
            }
            Self::IllegalCacheHit => "Internal error: received illegal TiKV response",
        })
    }
}

impl std::error::Error for CoprCacheError {}

/// Builds the byte-exact cache key for one projected coprocessor request.
///
/// `start_ts` and every field other than type, data, ranges, and paging state
/// are deliberately excluded, matching the source. Row-count and byte-budget
/// paging share one trailing marker regardless of their exact sizes.
pub fn build_copr_cache_key(
    request: &CoprocessorRequestEnvelope,
) -> Result<Vec<u8>, CoprCacheError> {
    if request.tp > u8::MAX.into() {
        return Err(CoprCacheError::RequestTypeTooBig);
    }
    let data_len = u32::try_from(request.data.len()).map_err(|_| CoprCacheError::DataTooBig)?;

    let mut key = Vec::new();
    // Go only rejects values above MaxUint8. Its uint8 conversion therefore
    // deliberately preserves the low byte for a negative request type.
    key.push(request.tp as u8);
    key.extend_from_slice(&data_len.to_le_bytes());
    key.extend_from_slice(&request.data);

    for range in &request.ranges {
        let start_len =
            u16::try_from(range.start_key.len()).map_err(|_| CoprCacheError::StartKeyTooBig)?;
        let end_len =
            u16::try_from(range.end_key.len()).map_err(|_| CoprCacheError::EndKeyTooBig)?;
        key.extend_from_slice(&start_len.to_le_bytes());
        key.extend_from_slice(&range.start_key);
        key.extend_from_slice(&end_len.to_le_bytes());
        key.extend_from_slice(&range.end_key);
    }

    if request.paging_size > 0 || request.paging_size_bytes > 0 {
        key.push(1);
    }
    Ok(key)
}

/// Cache value metadata and payload whose cost is charged to the cache.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CoprCacheValue {
    /// Cache key retained to detect hash collisions.
    pub key: Vec<u8>,
    /// Encoded coprocessor response data.
    pub data: Vec<u8>,
    /// Transaction timestamp used to validate the result.
    pub timestamp: u64,
    /// Region identifier used to validate the result.
    pub region_id: u64,
    /// Region data version used to validate the result.
    pub region_data_version: u64,
    /// Paging response range start.
    pub page_start: Option<Vec<u8>>,
    /// Paging response range end.
    pub page_end: Option<Vec<u8>>,
}

#[allow(clippy::len_without_is_empty)]
impl CoprCacheValue {
    /// Returns the source `unsafe.Sizeof` base plus all owned byte lengths.
    #[must_use]
    pub fn len(&self) -> usize {
        mem::size_of::<Self>()
            + self.key.len()
            + self.data.len()
            + self.page_start.as_ref().map_or(0, Vec::len)
            + self.page_end.as_ref().map_or(0, Vec::len)
    }
}

impl fmt::Display for CoprCacheValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{{ Ts = {}, RegionID = {}, RegionDataVersion = {}, len(Data) = {} }}",
            self.timestamp,
            self.region_id,
            self.region_data_version,
            self.data.len()
        )
    }
}

/// Source-shaped subset of TiKV's coprocessor-cache configuration.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct CoprCacheConfig {
    /// Cache capacity in MiB. Zero disables the cache.
    pub capacity_mb: f64,
    /// Maximum admitted range count; zero means unlimited.
    pub admission_max_ranges: u64,
    /// Maximum admitted result size in MiB.
    pub admission_max_result_mb: f64,
    /// Minimum TiKV processing time in milliseconds.
    pub admission_min_process_ms: u64,
}

/// Deterministic request and response admission state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoprCacheAdmission {
    admission_max_ranges: i64,
    admission_max_size: i64,
    admission_min_process_time: i64,
}

/// Cache lookup retained across the TiKV request/response boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoprCacheLookup {
    key: Vec<u8>,
    value: Option<CoprCacheValue>,
}

impl CoprCacheLookup {
    /// Exact source-derived key used for a possible miss insertion.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Valid same-region, old-enough value sent as the match predicate.
    #[must_use]
    pub const fn value(&self) -> Option<&CoprCacheValue> {
        self.value.as_ref()
    }
}

/// Source inputs that decide whether and how a request uses the cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoprCacheRequestContext {
    /// Only unary coprocessor requests are cacheable in the source.
    pub is_unary_cop: bool,
    /// `kv.Request.Cacheable` from the planner/request builder.
    pub cacheable: bool,
    /// Region that will receive this task.
    pub region_id: u64,
    /// Transaction timestamp used to validate an existing entry.
    pub start_ts: u64,
}

/// Source inputs used after TiKV returns a coprocessor response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoprCacheResponseContext {
    /// Timestamp stored with an admitted miss.
    pub start_ts: u64,
    /// Region stored with an admitted miss.
    pub region_id: u64,
    /// TiKV process time, absent when execution details were not collected.
    pub process_time_nanos: Option<i64>,
    /// Adaptive paging task index used by response admission.
    pub paging_task_index: u32,
    /// Row-count or byte-budget paging is active for this request.
    pub paging_enabled: bool,
}

/// Observable result of applying the source cache response lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoprCacheResponseOutcome {
    /// TiKV accepted the match version and the cached response was restored.
    Hit,
    /// TiKV computed a response, but it did not qualify for insertion.
    Miss,
    /// TiKV computed a response and the bounded owner accepted it.
    Stored,
}

/// Bounded deterministic cache owner.
///
/// Exact byte keys are retained by the map and again inside each value, so a
/// hash collision cannot return another request's response. FIFO eviction is a
/// deterministic Rust backend, not a claim of Ristretto eviction equivalence.
#[derive(Debug)]
pub struct CoprCache {
    admission: CoprCacheAdmission,
    capacity_bytes: usize,
    cost_bytes: usize,
    values: HashMap<Vec<u8>, CoprCacheValue>,
    insertion_order: VecDeque<Vec<u8>>,
}

impl CoprCacheAdmission {
    /// Validates configuration and creates admission state.
    ///
    /// `Ok(None)` is the source's disabled-cache result for zero capacity.
    pub fn from_config(config: &CoprCacheConfig) -> Result<Option<Self>, CoprCacheError> {
        if config.capacity_mb == 0.0 {
            return Ok(None);
        }
        let capacity_in_bytes = (config.capacity_mb * MEBIBYTE) as i64;
        if capacity_in_bytes <= 0 {
            return Err(CoprCacheError::CapacityMustBePositive);
        }
        let maximum_entity_bytes = (config.admission_max_result_mb * MEBIBYTE) as i64;
        if maximum_entity_bytes == 0 {
            return Err(CoprCacheError::AdmissionMaxResultMustBePositive);
        }

        Ok(Some(Self {
            admission_max_ranges: config.admission_max_ranges as i64,
            admission_max_size: maximum_entity_bytes,
            admission_min_process_time: (config.admission_min_process_ms as i64)
                .wrapping_mul(MILLISECOND_NANOS),
        }))
    }

    /// Returns whether a request range count is admitted.
    #[must_use]
    pub fn check_request(&self, ranges: i64) -> bool {
        self.admission_max_ranges == 0 || ranges <= self.admission_max_ranges
    }

    /// Returns whether response size, process duration, and page index qualify.
    ///
    /// Durations use signed nanoseconds, the representation of Go
    /// `time.Duration`. Paging tasks divide the configured threshold by three
    /// with truncation toward zero.
    #[must_use]
    pub fn check_response(
        &self,
        data_size: i64,
        process_time_nanos: i64,
        paging_task_index: u32,
    ) -> bool {
        if data_size == 0 || data_size > self.admission_max_size {
            return false;
        }
        if paging_task_index > 50 {
            return false;
        }

        let minimum_process_time = if paging_task_index > 0 {
            self.admission_min_process_time / 3
        } else {
            self.admission_min_process_time
        };
        process_time_nanos >= minimum_process_time
    }
}

impl CoprCache {
    /// Builds the enabled cache, or returns `None` for the source's disabled
    /// zero-capacity surface.
    pub fn from_config(config: &CoprCacheConfig) -> Result<Option<Self>, CoprCacheError> {
        Self::from_optional_config(Some(config))
    }

    /// Builds from Go's nullable configuration pointer.
    ///
    /// An absent configuration and an explicit zero capacity are the same
    /// disabled state in `newCoprCache`.
    pub fn from_optional_config(
        config: Option<&CoprCacheConfig>,
    ) -> Result<Option<Self>, CoprCacheError> {
        let Some(config) = config else {
            return Ok(None);
        };
        let Some(admission) = CoprCacheAdmission::from_config(config)? else {
            return Ok(None);
        };
        let capacity_bytes = (config.capacity_mb * MEBIBYTE) as i64;
        Ok(Some(Self {
            admission,
            capacity_bytes: capacity_bytes as usize,
            cost_bytes: 0,
            values: HashMap::new(),
            insertion_order: VecDeque::new(),
        }))
    }

    /// Number of values currently retained by the deterministic backend.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether the cache currently holds no values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Applies the source request-admission policy through the cache owner.
    ///
    /// This forwarding method keeps the disabled [`Option<CoprCache>`] caller
    /// path capable of expressing Go's nil-receiver `false` result without
    /// exposing the policy fields themselves.
    #[must_use]
    pub fn check_request_admission(&self, ranges: i64) -> bool {
        self.admission.check_request(ranges)
    }

    /// Applies the source response-admission policy through the cache owner.
    #[must_use]
    pub fn check_response_admission(
        &self,
        data_size: i64,
        process_time_nanos: i64,
        paging_task_index: u32,
    ) -> bool {
        self.admission
            .check_response(data_size, process_time_nanos, paging_task_index)
    }

    /// Gets an exact-key value. `HashMap` equality plus the retained value key
    /// is the source's post-hash collision check at this boundary.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&CoprCacheValue> {
        self.values.get(key).filter(|value| value.key == key)
    }

    /// Inserts a value after forcing its retained collision-check key to equal
    /// the caller's key. Values larger than the configured capacity are denied.
    pub fn set(&mut self, key: Vec<u8>, mut value: CoprCacheValue) -> bool {
        value.key.clone_from(&key);
        let cost = value.len();
        if cost > self.capacity_bytes {
            return false;
        }

        if let Some(replaced) = self.values.remove(key.as_slice()) {
            self.cost_bytes -= replaced.len();
            self.insertion_order.retain(|candidate| candidate != &key);
        }
        while self.cost_bytes + cost > self.capacity_bytes {
            let Some(evicted_key) = self.insertion_order.pop_front() else {
                break;
            };
            if let Some(evicted) = self.values.remove(evicted_key.as_slice()) {
                self.cost_bytes -= evicted.len();
            }
        }
        self.cost_bytes += cost;
        self.insertion_order.push_back(key.clone());
        self.values.insert(key, value);
        true
    }

    /// Applies Go's `buildCacheKey` eligibility and match-version rules to one
    /// already projected task request.
    pub fn prepare_request(
        &self,
        request: &mut CoprocessorRequestEnvelope,
        context: CoprCacheRequestContext,
    ) -> Option<CoprCacheLookup> {
        request.is_cache_enabled = false;
        request.cache_if_match_version = 0;
        if !context.is_unary_cop
            || !context.cacheable
            || !self.check_request_admission(request.ranges.len() as i64)
        {
            return None;
        }

        // Go logs key-construction failure and continues the request without
        // cache fields; cache policy must never turn a valid SQL read into an
        // RPC error.
        let Ok(key) = build_copr_cache_key(request) else {
            return None;
        };
        let value = self
            .get(&key)
            .filter(|value| {
                value.region_id == context.region_id && value.timestamp <= context.start_ts
            })
            .cloned();
        request.is_cache_enabled = true;
        request.cache_if_match_version =
            value.as_ref().map_or(0, |value| value.region_data_version);
        Some(CoprCacheLookup { key, value })
    }

    /// Restores a hit or admits and stores a computed response exactly at the
    /// source worker's post-response cache boundary.
    pub fn handle_response(
        &mut self,
        response: &mut CoprocessorResponse,
        lookup: Option<&CoprCacheLookup>,
        context: CoprCacheResponseContext,
    ) -> Result<CoprCacheResponseOutcome, CoprCacheError> {
        if response.is_cache_hit {
            let value = lookup
                .and_then(CoprCacheLookup::value)
                .ok_or(CoprCacheError::IllegalCacheHit)?;
            response.data.clone_from(&value.data);
            if context.paging_enabled {
                response.range = if value.page_start.is_some() || value.page_end.is_some() {
                    Some(CoprocessorKeyRange {
                        start: value.page_start.clone().unwrap_or_default(),
                        end: value.page_end.clone().unwrap_or_default(),
                    })
                } else {
                    None
                };
            }
            return Ok(CoprCacheResponseOutcome::Hit);
        }

        let Some(lookup) = lookup else {
            return Ok(CoprCacheResponseOutcome::Miss);
        };
        if !response.can_be_cached || response.cache_last_version == 0 {
            return Ok(CoprCacheResponseOutcome::Miss);
        }
        let Some(process_time_nanos) = context.process_time_nanos else {
            return Ok(CoprCacheResponseOutcome::Miss);
        };
        if !self.check_response_admission(
            response.data.len() as i64,
            process_time_nanos,
            context.paging_task_index,
        ) {
            return Ok(CoprCacheResponseOutcome::Miss);
        }

        let value = CoprCacheValue {
            data: response.data.clone(),
            timestamp: context.start_ts,
            region_id: context.region_id,
            region_data_version: response.cache_last_version,
            page_start: response.range.as_ref().map(|range| range.start.clone()),
            page_end: response.range.as_ref().map(|range| range.end.clone()),
            ..CoprCacheValue::default()
        };
        if self.set(lookup.key.clone(), value) {
            Ok(CoprCacheResponseOutcome::Stored)
        } else {
            Ok(CoprCacheResponseOutcome::Miss)
        }
    }
}
