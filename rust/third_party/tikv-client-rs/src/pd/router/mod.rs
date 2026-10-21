// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! pd-client's `clients/router` package: the router client (`Cli`,
//! client.go) that batches `GetRegion` / `GetPrevRegion` / `GetRegionByID`
//! requests (request.go) into `QueryRegion` streams to the PD leader (or a
//! follower, or a router service node), over the `servicediscovery` facts it
//! consumes.

use async_trait::async_trait;
use tonic::transport::Channel;

use crate::proto::{metapb, pdpb};
use crate::Result;

mod client;
mod request;
mod stream;

pub(crate) use client::Cli;

/// Go `defaultMaxRouterRequestBatchSize`.
pub(crate) const DEFAULT_MAX_ROUTER_REQUEST_BATCH_SIZE: usize = 10_000;

/// Go `router.Region`: a region's meta and its peers.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Region {
    pub(crate) meta: metapb::Region,
    pub(crate) leader: Option<metapb::Peer>,
    pub(crate) down_peers: Vec<metapb::Peer>,
    pub(crate) pending_peers: Vec<metapb::Peer>,
    pub(crate) buckets: Option<metapb::Buckets>,
}

impl Region {
    /// Go `convertToRegionCopy` (the response is owned here, so the copy is
    /// the response's own fields): `None` for a response without a region.
    pub(crate) fn from_response(response: pdpb::RegionResponse) -> Option<Self> {
        let meta = response.region?;
        Some(Self {
            meta,
            leader: response.leader,
            down_peers: response
                .down_peers
                .into_iter()
                .filter_map(|stats| stats.peer)
                .collect(),
            pending_peers: response.pending_peers,
            buckets: response.buckets,
        })
    }
}

/// The `servicediscovery.ServiceDiscovery` methods the router client uses:
/// PD's (`svcDiscovery`) and the router service's (`msDiscovery`).
#[async_trait]
pub(crate) trait ServiceDiscovery: Send + Sync {
    /// Go `GetClusterID`.
    fn get_cluster_id(&self) -> u64;
    /// Go `GetServingURL`.
    fn get_serving_url(&self) -> String;
    /// Go `GetClientConns`: every connection the discovery holds, by URL.
    fn get_client_conns(&self) -> Vec<(String, Channel)>;
    /// Go `ScheduleCheckMemberChanged`.
    fn schedule_check_member_changed(&self);
    /// Go `CheckMemberChanged`.
    async fn check_member_changed(&self) -> Result<()>;
    /// Go `ExecAndAddLeaderSwitchedCallback`.
    fn exec_and_add_leader_switched_callback(
        &self,
        callback: crate::pd::service_discovery::LeaderSwitchedCallback,
    );
    /// Go `AddMembersChangedCallback`.
    fn add_members_changed_callback(
        &self,
        callback: crate::pd::service_discovery::MembersChangedCallback,
    );
}

#[cfg(test)]
mod tests;
