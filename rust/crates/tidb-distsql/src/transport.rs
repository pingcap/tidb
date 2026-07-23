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

//! Explicit transport ownership at the `kv.Client.Send` boundary.
//!
//! Go's `distsql.RequestBuilder.Build` produces request metadata and the
//! caller later supplies that metadata to `kv.Client.Send`.  Keep those two
//! ownership steps separate while the Rust rewrite has no TiKV client,
//! protobuf transport, or region router.  [`TransportRequest`] is therefore
//! an immutable metadata snapshot with an explicit unbound state.  A future
//! transport owner can attach a [`TransportBinding`] without changing the
//! request fields or inventing an endpoint/RPC representation here.

use std::{sync::Arc, time::Instant};

use crate::{
    region_task::build_region_tasks, CancelHandle, CoprocessorRequestEnvelope, KvRequestMetadata,
    RegionTaskEnvelope, RegionTaskTopology, RequestKeyRange, RequestSource,
};

/// The state of a request at the transport boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportRequestState {
    /// No client/transport owner has been attached yet.
    Unbound,
    /// A future transport owner has claimed this immutable request snapshot.
    Bound,
}

/// A proof that a caller owns a transport capable of taking this request.
///
/// This marker intentionally has no endpoint, region, protobuf, or RPC
/// fields.  Those belong to the eventual TiKV client owner, not the DistSQL
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
/// before `kv.Client.Send` receives the request, and a future client cannot
/// accidentally mutate the metadata while claiming transport ownership.
#[derive(Clone, Debug)]
pub struct TransportRequest {
    metadata: KvRequestMetadata,
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
            execution_cancellation,
            request_cancellation: None,
            bound_at: None,
            binding: None,
        }
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
    /// envelope. It deliberately stops before `tikvrpc.Request`, which does
    /// not exist in the Rust dependency graph yet.
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

    /// Returns the current transport ownership state.
    #[must_use]
    pub const fn state(&self) -> TransportRequestState {
        if self.binding.is_some() {
            TransportRequestState::Bound
        } else {
            TransportRequestState::Unbound
        }
    }

    /// Returns whether a transport owner has been attached.
    #[must_use]
    pub const fn is_bound(&self) -> bool {
        self.binding.is_some()
    }

    /// Returns metadata only when a transport owner is attached.
    ///
    /// The error is deliberate: returning fake serialized bytes or silently
    /// treating an unbound request as sendable would hide the missing TiKV
    /// transport layer.
    pub fn metadata_for_send(&self) -> Result<&KvRequestMetadata, TransportRequestError> {
        if self.binding.is_some() {
            Ok(&self.metadata)
        } else {
            Err(TransportRequestError::Unbound)
        }
    }

    /// Serializes one task's coprocessor request only after transport ownership
    /// has been claimed.
    ///
    /// The bytes are still a protobuf envelope, not an RPC: Context encoding,
    /// region splitting, retries, and endpoint selection belong to the future
    /// transport owner represented by [`TransportBinding`].
    pub fn encode_coprocessor_request(
        &self,
        ranges: Vec<RequestKeyRange>,
    ) -> Result<Vec<u8>, TransportRequestError> {
        let metadata = self.metadata_for_send()?;
        Ok(CoprocessorRequestEnvelope::from_metadata(metadata, ranges).encode_to_vec())
    }

    /// Splits this immutable built request into source-shaped region tasks.
    ///
    /// The caller supplies a checked region snapshot. This method performs no
    /// cache lookup, endpoint selection, retry, or RPC, and it does not require
    /// a transport binding because task construction precedes client send.
    pub fn build_region_tasks(
        &self,
        topology: &[RegionTaskTopology],
    ) -> Result<Vec<RegionTaskEnvelope>, TransportRequestError> {
        build_region_tasks(&self.metadata, topology)
            .ok_or(TransportRequestError::InvalidRegionTopology)
    }

    /// Returns request ranges grouped by normalized region, honoring Go's
    /// `UnspecifiedLimit = -1` and zero-limit fast path.
    pub fn split_key_ranges_by_regions(
        &self,
        topology: &[RegionTaskTopology],
        limit: isize,
    ) -> Result<Vec<Vec<RequestKeyRange>>, TransportRequestError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut metadata = self.metadata.clone();
        metadata.store_batch_size = 0;
        let mut tasks = build_region_tasks(&metadata, topology)
            .ok_or(TransportRequestError::InvalidRegionTopology)?;
        if limit > 0 {
            tasks.truncate(usize::try_from(limit).expect("positive limit fits usize"));
        }
        Ok(tasks.into_iter().map(|task| task.ranges).collect())
    }

    /// Flattens [`Self::split_key_ranges_by_regions`] in region order.
    pub fn split_region_ranges(
        &self,
        topology: &[RegionTaskTopology],
        limit: isize,
    ) -> Result<Vec<RequestKeyRange>, TransportRequestError> {
        Ok(self
            .split_key_ranges_by_regions(topology, limit)?
            .into_iter()
            .flatten()
            .collect())
    }

    /// Encodes the coprocessor request for one already-built task after the
    /// transport owner has been attached.
    pub fn encode_region_task_request(
        &self,
        task: &RegionTaskEnvelope,
    ) -> Result<Vec<u8>, TransportRequestError> {
        self.encode_coprocessor_request(task.ranges.clone())
    }

    /// Attaches an opaque transport owner without mutating this snapshot.
    pub fn bind(&self, binding: TransportBinding) -> Result<Self, TransportRequestError> {
        if self.binding.is_some() {
            return Err(TransportRequestError::AlreadyBound);
        }
        Ok(Self {
            metadata: self.metadata.clone(),
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
            execution_cancellation: Arc::clone(&self.execution_cancellation),
            request_cancellation: Some(self.execution_cancellation.request_child()),
            bound_at: Some(Instant::now()),
            binding: Some(binding),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KvRequestBuilder, RequestType, GLOBAL_REPLICA_SCOPE};

    #[test]
    fn transport_request_starts_unbound_and_rejects_send() {
        let mut builder = KvRequestBuilder::new();
        builder.set_request_type(RequestType::Dag);
        let metadata = builder.build().expect("request metadata");
        assert_eq!(metadata.read_replica_scope, GLOBAL_REPLICA_SCOPE);

        let request = TransportRequest::new(metadata, Arc::new(CancelHandle::default()));
        assert_eq!(request.state(), TransportRequestState::Unbound);
        assert!(!request.is_bound());
        assert!(matches!(
            request.metadata_for_send(),
            Err(TransportRequestError::Unbound)
        ));
        assert_eq!(request.metadata().request_type, RequestType::Dag);
    }

    #[test]
    fn binding_returns_an_immutable_bound_snapshot() {
        let mut builder = KvRequestBuilder::new();
        builder.set_start_ts(42);
        let request = TransportRequest::new(
            builder.build().expect("request metadata"),
            Arc::new(CancelHandle::default()),
        );

        let bound = request
            .bind(TransportBinding::new())
            .expect("unbound request can be claimed");
        assert_eq!(request.state(), TransportRequestState::Unbound);
        assert_eq!(bound.state(), TransportRequestState::Bound);
        assert!(bound.is_bound());
        assert_eq!(bound.metadata().start_ts, 42);
        assert_eq!(
            bound
                .metadata_for_send()
                .expect("bound request is sendable")
                .start_ts,
            42
        );
    }

    #[test]
    fn repeated_binding_is_an_explicit_error() {
        let request = TransportRequest::new(
            KvRequestBuilder::new().build().expect("metadata"),
            Arc::new(CancelHandle::default()),
        );
        let bound = request
            .bind(TransportBinding::new())
            .expect("first binding");
        assert!(matches!(
            bound.bind(TransportBinding::new()),
            Err(TransportRequestError::AlreadyBound)
        ));
    }

    #[test]
    fn binding_marker_carries_no_transport_details() {
        assert_eq!(TransportBinding::new(), TransportBinding);
        assert_eq!(format!("{:?}", TransportBinding::new()), "TransportBinding");
    }
}
