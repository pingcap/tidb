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

use tidb_proto::{KvrpcContext, KvrpcPeer, KvrpcRegionEpoch};

use super::{
    ReadPolicy, RegionLocation, RegionRouteError, RegionSendError, RegionVerId, ReplicaReadMode,
    ReplicaSelector,
};

/// Request state before client-go's final `AttachContext` boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingRegionRequest {
    /// Region identity captured when the DistSQL task was built.
    expected_region: RegionVerId,
    /// Requested replica semantics.
    read_policy: ReadPolicy,
    /// Caller context with replica/stale flags canonicalized from `read_policy`.
    context: KvrpcContext,
    attached: bool,
}

impl PendingRegionRequest {
    /// Creates an unattached request and canonicalizes policy-owned flags.
    #[must_use]
    pub fn new(
        expected_region: RegionVerId,
        read_policy: ReadPolicy,
        mut context: KvrpcContext,
    ) -> Self {
        context.replica_read = read_policy.mode != ReplicaReadMode::Leader;
        context.stale_read = read_policy.stale_read;
        Self {
            expected_region,
            read_policy,
            context,
            attached: false,
        }
    }

    /// Whether final request context has already been attached.
    #[must_use]
    pub const fn is_attached(&self) -> bool {
        self.attached
    }

    /// Immutable versioned region identity owned by this request.
    #[must_use]
    pub const fn expected_region(&self) -> RegionVerId {
        self.expected_region
    }

    /// Immutable replica policy canonicalized into the protobuf context.
    #[must_use]
    pub const fn read_policy(&self) -> ReadPolicy {
        self.read_policy
    }

    /// Immutable canonical context, including any final route attachment.
    #[must_use]
    pub const fn context(&self) -> &KvrpcContext {
        &self.context
    }
}

/// Single-attempt source-shaped region request sender.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SingleRegionRequestSender {
    cluster_id: u64,
}

impl SingleRegionRequestSender {
    /// Creates a sender for one PD cluster identity.
    #[must_use]
    pub const fn new(cluster_id: u64) -> Self {
        Self { cluster_id }
    }

    /// Selects, attaches, then immediately invokes one address-directed RPC.
    ///
    /// All validation happens before mutation. Once attached, this request can
    /// never be dispatched again through this owner.
    pub fn send<T, E>(
        &self,
        location: &RegionLocation,
        request: &mut PendingRegionRequest,
        rpc: impl FnOnce(&str, &KvrpcContext) -> Result<T, E>,
    ) -> Result<T, RegionSendError<E>> {
        if self.cluster_id == 0 {
            return Err(RegionRouteError::MissingClusterId.into());
        }
        if request.attached {
            return Err(RegionRouteError::ContextAlreadyAttached.into());
        }
        if request.expected_region != location.region {
            return Err(RegionRouteError::StaleRequestEpoch {
                expected: request.expected_region,
                actual: location.region,
            }
            .into());
        }
        let route = ReplicaSelector::select_leader(location, request.read_policy)
            .map_err(RegionSendError::Route)?;

        request.context.region_id = location.region.id;
        request.context.region_epoch = Some(KvrpcRegionEpoch {
            conf_ver: location.region.epoch.conf_ver,
            version: location.region.epoch.version,
        });
        request.context.peer = Some(KvrpcPeer {
            id: route.peer.id,
            store_id: route.peer.store_id,
            role: route.peer.role as i32,
            is_witness: route.peer.is_witness,
        });
        request.context.cluster_id = self.cluster_id;
        request.attached = true;

        rpc(&route.store.address, &request.context).map_err(RegionSendError::DirectUnary)
    }
}
