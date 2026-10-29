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
//! The built request and its send binding are Go `pkg/kv`'s and live in
//! `tidb_txnkv`; splitting one into region tasks is the coprocessor client's
//! `buildCopTasks`, which stays with the region-task builder here.

use tidb_copr::region_task::build_region_tasks;
use tidb_copr::region_task::RegionTaskEnvelope;
use tidb_copr::region_task::RegionTaskTopology;
use crate::RequestKeyRange;
use tidb_txnkv::{TransportRequest, TransportRequestError};

/// Go `copr.buildCopTasks` / `SplitKeyRangesByLocations` over a built
/// request.
pub trait RegionTaskRequest {
    /// Splits this immutable built request into source-shaped region tasks.
    ///
    /// The caller supplies a checked region snapshot. This method performs no
    /// cache lookup, endpoint selection, retry, or RPC, and it does not require
    /// a transport binding because task construction precedes client send.
    fn build_region_tasks(
        &self,
        topology: &[RegionTaskTopology],
    ) -> Result<Vec<RegionTaskEnvelope>, TransportRequestError>;

    /// Returns request ranges grouped by normalized region, honoring Go's
    /// `UnspecifiedLimit = -1` and zero-limit fast path.
    fn split_key_ranges_by_regions(
        &self,
        topology: &[RegionTaskTopology],
        limit: isize,
    ) -> Result<Vec<Vec<RequestKeyRange>>, TransportRequestError>;

    /// Flattens [`Self::split_key_ranges_by_regions`] in region order.
    fn split_region_ranges(
        &self,
        topology: &[RegionTaskTopology],
        limit: isize,
    ) -> Result<Vec<RequestKeyRange>, TransportRequestError>;
}

impl RegionTaskRequest for TransportRequest {
    fn build_region_tasks(
        &self,
        topology: &[RegionTaskTopology],
    ) -> Result<Vec<RegionTaskEnvelope>, TransportRequestError> {
        build_region_tasks(self.metadata(), topology)
            .ok_or(TransportRequestError::InvalidRegionTopology)
    }

    fn split_key_ranges_by_regions(
        &self,
        topology: &[RegionTaskTopology],
        limit: isize,
    ) -> Result<Vec<Vec<RequestKeyRange>>, TransportRequestError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut metadata = self.metadata().clone();
        metadata.store_batch_size = 0;
        let mut tasks = build_region_tasks(&metadata, topology)
            .ok_or(TransportRequestError::InvalidRegionTopology)?;
        if limit > 0 {
            tasks.truncate(usize::try_from(limit).expect("positive limit fits usize"));
        }
        Ok(tasks.into_iter().map(|task| task.ranges).collect())
    }

    fn split_region_ranges(
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use tidb_txnkv::TransportBinding;
    use crate::{CancelHandle, KvRequestBuilder, RequestType, GLOBAL_REPLICA_SCOPE};

    #[test]
    fn transport_request_starts_unbound_and_rejects_send() {
        let mut builder = KvRequestBuilder::new();
        builder.set_request_type(RequestType::Dag);
        let metadata = builder.build().expect("request metadata");
        assert_eq!(metadata.read_replica_scope, GLOBAL_REPLICA_SCOPE);

        let request = TransportRequest::new(metadata, Arc::new(CancelHandle::default()));
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
}
