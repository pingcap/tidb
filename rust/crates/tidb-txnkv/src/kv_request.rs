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

//! Go `pkg/kv`'s built `kv.Request` as it crosses `kv.Client.Send`: the
//! request metadata DistSQL's builder produces, and the explicit transport
//! binding that retains request-local cancellation, statement attribution and
//! the send timestamp. Region routing and wire encoding belong to the
//! concrete coprocessor transport and request envelope.

use std::ops::{Deref, DerefMut};
use std::{sync::Arc, time::Instant};

use tidb_paging::{MIN_ALLOWED_MAX_PAGING_SIZE, MIN_PAGING_SIZE};

use crate::{CancelHandle, Paging, Request, RequestSource, StatementKvContext};
use tidb_tiflash::ReplicaRead;

/// DistSQL-only metadata attached to the canonical `pkg/kv.Request`.
///
/// All KV request fields live in [`Request`]. This wrapper carries
/// only TiFlash's client-send selection policy, which is not a field of Go
/// `kv.Request`.
#[derive(Clone)]
pub struct KvRequestMetadata {
    request: Request,
    /// TiFlash node-selection policy projected into client-send metadata.
    pub tiflash_replica_read: ReplicaRead,
}

impl Default for KvRequestMetadata {
    fn default() -> Self {
        let mut request = Request::default();
        // Go `DefTiDBEnablePaging` and `pkg/util/paging`'s bounds, the
        // defaults of DistSQL's `PagingConfig`.
        request.paging = Paging {
            enabled: true,
            min_size: MIN_PAGING_SIZE,
            max_size: MIN_ALLOWED_MAX_PAGING_SIZE,
            size_bytes: 0,
        };
        Self {
            request,
            tiflash_replica_read: ReplicaRead::default(),
        }
    }
}

impl KvRequestMetadata {
    /// Wraps one canonical KV request with default DistSQL-only metadata.
    #[must_use]
    pub fn from_request(request: Request) -> Self {
        Self {
            request,
            tiflash_replica_read: ReplicaRead::default(),
        }
    }

    /// Consumes the wrapper and returns the canonical KV request.
    #[must_use]
    pub fn into_request(self) -> Request {
        self.request
    }
}

impl Deref for KvRequestMetadata {
    type Target = Request;

    fn deref(&self) -> &Self::Target {
        &self.request
    }
}

impl DerefMut for KvRequestMetadata {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.request
    }
}

impl std::fmt::Debug for KvRequestMetadata {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KvRequestMetadata")
            .field("request_type", &self.request_type)
            .field("start_ts", &self.start_ts)
            .field(
                "range_count",
                &self
                    .key_ranges
                    .as_ref()
                    .map(|ranges| ranges.total_range_count()),
            )
            .field("store_type", &self.store_type)
            .field("connection_id", &self.connection_id)
            .finish_non_exhaustive()
    }
}

/// A proof that a caller owns a transport capable of taking this request.
///
/// This marker intentionally has no endpoint, region, protobuf, or RPC
/// fields.  Those belong to the TiKV client owner, not the DistSQL
/// request metadata leaf.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransportBinding;

impl TransportBinding {
    /// Creates an opaque binding marker for a transport owner.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

/// Errors produced while claiming or consuming a transport request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportRequestError {
    /// `bind` was called on a request that already has a transport owner.
    AlreadyBound,
    /// A send/consume operation was attempted before a transport owner was
    /// attached.
    Unbound,
    /// Supplied region/bucket topology is malformed or does not cover every
    /// request range.
    InvalidRegionTopology,
}

/// Immutable request metadata plus an explicit transport binding state.
///
/// `bind` returns a new snapshot rather than mutating the original request.
/// This mirrors the source ownership split: request construction is finished
/// before `kv.Client.Send` receives the request, and a client cannot
/// accidentally mutate the metadata while claiming transport ownership.
#[derive(Clone, Debug)]
pub struct TransportRequest {
    metadata: KvRequestMetadata,
    kv_exec_counter: tidb_topsql_stmtstats::KvExecCounterHandle,
    /// Go's statement ctx values (`util.RUDetailsCtxKey`,
    /// `util.ExecDetailsKey`) the copr worker's `bo.GetCtx()` carries to
    /// every send.
    kv_context: StatementKvContext,
    cop_lite_worker: Option<Arc<std::sync::atomic::AtomicBool>>,
    execution_cancellation: Arc<CancelHandle>,
    request_cancellation: Option<Arc<CancelHandle>>,
    bound_at: Option<Instant>,
    binding: Option<TransportBinding>,
}

impl TransportRequest {
    /// Creates an unbound transport request from built request metadata.
    #[must_use]
    pub fn new(metadata: KvRequestMetadata, execution_cancellation: Arc<CancelHandle>) -> Self {
        Self {
            metadata,
            kv_exec_counter: Default::default(),
            kv_context: StatementKvContext::default(),
            cop_lite_worker: None,
            execution_cancellation,
            request_cancellation: None,
            bound_at: None,
            binding: None,
        }
    }

    /// Shares Go's statement-scoped TryCopLiteWorker across this statement's
    /// readers. It is local execution ownership, never serialized to TiKV.
    #[must_use]
    pub fn with_cop_lite_worker(mut self, token: Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.cop_lite_worker = Some(token);
        self
    }

    /// Retains the statement interceptor through paging and worker copies.
    #[must_use]
    pub fn with_kv_exec_counter(
        mut self,
        counter: tidb_topsql_stmtstats::KvExecCounterHandle,
    ) -> Self {
        self.kv_exec_counter = counter;
        self
    }

    /// The statement interceptor every send of this request counts in.
    #[must_use]
    pub fn kv_exec_counter(&self) -> tidb_topsql_stmtstats::KvExecCounterHandle {
        self.kv_exec_counter.clone()
    }

    /// Carries the statement's ctx values -- its `RUDetails` and
    /// `ExecDetails` -- to every send of this request (Go `CopClient.Send`'s
    /// ctx, which `bo.GetCtx()` hands `SendReqCtx`).
    #[must_use]
    pub fn with_kv_context(mut self, kv_context: StatementKvContext) -> Self {
        self.kv_context = kv_context;
        self
    }

    /// The statement's ctx values every send of this request carries.
    #[must_use]
    pub fn kv_context(&self) -> StatementKvContext {
        self.kv_context.clone()
    }

    /// The statement's shared TryCopLiteWorker token, when it has one.
    #[must_use]
    pub fn cop_lite_worker(&self) -> Option<Arc<std::sync::atomic::AtomicBool>> {
        self.cop_lite_worker.clone()
    }

    /// Returns the mandatory outer execution cancellation owner.
    #[must_use]
    pub const fn execution_cancellation(&self) -> &Arc<CancelHandle> {
        &self.execution_cancellation
    }

    /// Returns the one request-local authority minted at the send binding.
    pub fn request_cancellation(&self) -> Result<&Arc<CancelHandle>, TransportRequestError> {
        self.request_cancellation
            .as_ref()
            .ok_or(TransportRequestError::Unbound)
    }

    /// Returns the instant at which this request acquired its transport owner.
    pub fn bound_at(&self) -> Result<Instant, TransportRequestError> {
        self.bound_at.ok_or(TransportRequestError::Unbound)
    }

    /// Returns the immutable request metadata snapshot.
    #[must_use]
    pub const fn metadata(&self) -> &KvRequestMetadata {
        &self.metadata
    }

    /// Builds the source resource-group tag from the first request key.
    ///
    /// This is a real pre-transport consumer of the canonical request
    /// envelope. It deliberately stops before `tikvrpc.Request`, which is
    /// encoded separately by the concrete transport.
    #[must_use]
    pub fn resource_group_tag(&self) -> Option<Vec<u8>> {
        let tagger = self.metadata.resource_group_tagger.as_ref()?;
        let first_key = self
            .metadata
            .key_ranges
            .as_ref()
            .and_then(|ranges| ranges.partitions().first())
            .and_then(|ranges| ranges.first())
            .map_or(&[][..], |range| range.start_key.as_slice());
        Some(tagger.encode_tag_with_key(first_key))
    }

    /// Returns whether a transport owner has been attached.
    #[must_use]
    pub const fn is_bound(&self) -> bool {
        self.binding.is_some()
    }

    /// Returns metadata only when a transport owner is attached.
    ///
    /// The error is deliberate: returning fake serialized bytes or silently
    /// treating an unbound request as sendable would bypass the query runtime
    /// transport boundary.
    pub fn metadata_for_send(&self) -> Result<&KvRequestMetadata, TransportRequestError> {
        if self.binding.is_some() {
            Ok(&self.metadata)
        } else {
            Err(TransportRequestError::Unbound)
        }
    }

    /// Attaches an opaque transport owner without mutating this snapshot.
    pub fn bind(&self, binding: TransportBinding) -> Result<Self, TransportRequestError> {
        if self.binding.is_some() {
            return Err(TransportRequestError::AlreadyBound);
        }
        Ok(Self {
            metadata: self.metadata.clone(),
            kv_exec_counter: self.kv_exec_counter.clone(),
            kv_context: self.kv_context.clone(),
            cop_lite_worker: self.cop_lite_worker.clone(),
            execution_cancellation: Arc::clone(&self.execution_cancellation),
            request_cancellation: Some(self.execution_cancellation.request_child()),
            bound_at: Some(Instant::now()),
            binding: Some(binding),
        })
    }

    /// Binds a transport and atomically applies the source request-source
    /// mutation required by `Analyze` before the send boundary observes it.
    ///
    /// The original built request stays reusable, just as `bind` preserves
    /// immutable request ownership. A second binding remains an explicit
    /// error instead of silently replacing a transport's request source.
    pub fn bind_with_request_source(
        &self,
        binding: TransportBinding,
        request_source: RequestSource,
    ) -> Result<Self, TransportRequestError> {
        if self.binding.is_some() {
            return Err(TransportRequestError::AlreadyBound);
        }
        let mut metadata = self.metadata.clone();
        metadata.request_source = request_source;
        Ok(Self {
            metadata,
            kv_exec_counter: self.kv_exec_counter.clone(),
            kv_context: self.kv_context.clone(),
            cop_lite_worker: self.cop_lite_worker.clone(),
            execution_cancellation: Arc::clone(&self.execution_cancellation),
            request_cancellation: Some(self.execution_cancellation.request_child()),
            bound_at: Some(Instant::now()),
            binding: Some(binding),
        })
    }
}
