// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod batch;
mod client;
mod command;
mod endpoint;
mod errors;
#[cfg(any(test, feature = "mock"))]
pub mod mockserver;
mod priority_queue;
mod request;

use std::cmp::max;
use std::cmp::min;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use futures::prelude::*;
use futures::stream::BoxStream;

#[allow(unused_imports)]
pub use self::client::ClientEventListener;
pub use self::client::KvClient;
pub use self::client::KvConnect;
pub use self::client::TikvConnect;
#[allow(unused_imports)]
pub use self::command::{BatchCommandRequest, BatchCommandResponse, CommandType};
pub use self::endpoint::EndpointType;
pub use self::errors::HasKeyErrors;
pub use self::errors::HasRegionError;
pub use self::errors::HasRegionErrors;
#[allow(unused_imports)]
pub use self::errors::RegionErrorResponse;
pub(crate) use self::request::network_response_size;
pub use self::request::Request;
pub(crate) use self::request::{exec_details_v2, exec_details_v2_mut};
#[allow(unused_imports)]
pub use self::request::{
    get_default_request_origin, set_default_request_origin, BatchCoprocessorStreamRequest,
    BatchCoprocessorStreamResponse, CoprocessorStreamRequest, CoprocessorStreamResponse,
    MppStreamRequest, MppStreamResponse, ResourceGroupTagger, ResponseExt,
};
use crate::kv::AccessLocationType;
use crate::locate::StoreHealthStatus;
use crate::pd::PdClient;
use crate::proto::kvrpcpb;
use crate::proto::metapb;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::BoundRange;
use crate::Key;
use crate::Result;

#[derive(Clone)]
pub struct RegionStore {
    pub region_with_leader: RegionWithLeader,
    /// Logical peer carried in the request context. This is normally the
    /// cached leader, but a replica selector may choose a follower while
    /// retaining the region's cached leader separately.
    pub target_peer: Option<metapb::Peer>,
    pub client: Arc<dyn KvClient + Send + Sync>,
    /// Network address of the physical TiKV target, when the PD implementation has it.
    pub target: String,
    /// Store ID of the physical RPC destination. This differs from
    /// `target_peer.store_id` when a logical leader request is forwarded by a
    /// healthy proxy store.
    pub(crate) physical_store_id: Option<StoreId>,
    /// Endpoint classification of the physical destination. Source liveness
    /// checks deliberately apply only to ordinary TiKV stores.
    pub(crate) physical_endpoint_type: EndpointType,
    /// Logical target address to forward to when `target` is a proxy store.
    /// An empty value means this is a direct request.
    pub forwarded_host: String,
    /// Stale reads are logically replica reads but must retain TiKV's distinct
    /// `Context.stale_read=true, replica_read=false` wire representation.
    pub stale_read: bool,
    /// Source `Context.busy_threshold_ms` carried unchanged to TiKV.
    pub busy_threshold_ms: u32,
    /// Selector-local source all-busy fallback. `Dispatch::apply_store`
    /// persists it so later retries retain the reset threshold.
    pub(crate) busy_threshold_disabled: bool,
    /// Force a follower transport route to retain source leader-read context.
    /// This is used only by the busy-leader NotLeader-hint probe.
    pub(crate) force_leader_read: bool,
    /// The mixed selector exhausted every probe and restored its temporarily
    /// suspected cached leader. Applying this route must clear the
    /// request-local suspicion just as client-go mutates its replica object.
    pub(crate) restores_suspect_leader: bool,
    /// Source prefer-leader latency sampling targets the logical store even
    /// when an RPC is physically sent through a forwarding proxy.
    pub(crate) health_status: Option<Arc<StoreHealthStatus>>,
    pub(crate) record_client_side_slow_score: bool,
    /// Source resource-control replica count: voters and learners in the
    /// selected region, or one when PD omitted peer metadata.
    pub(crate) resource_control_replica_number: i64,
    /// Source resource-control traffic zone for selector-owned TiKV routes.
    pub(crate) resource_control_access_location: AccessLocationType,
    pub(crate) store_token_count: Arc<AtomicI64>,
}

impl RegionStore {
    pub fn new(
        region_with_leader: RegionWithLeader,
        client: Arc<dyn KvClient + Send + Sync>,
    ) -> Self {
        let resource_control_replica_number = source_replica_number(&region_with_leader);
        Self {
            target_peer: region_with_leader.leader.clone(),
            region_with_leader,
            client,
            target: String::new(),
            physical_store_id: None,
            physical_endpoint_type: EndpointType::TiKv,
            forwarded_host: String::new(),
            stale_read: false,
            busy_threshold_ms: 0,
            busy_threshold_disabled: false,
            force_leader_read: false,
            restores_suspect_leader: false,
            health_status: None,
            record_client_side_slow_score: false,
            resource_control_replica_number,
            resource_control_access_location: AccessLocationType::Unknown,
            store_token_count: Arc::new(AtomicI64::new(0)),
        }
    }

    pub fn with_target(mut self, target: impl Into<String>) -> Self {
        self.target = target.into();
        self
    }

    pub(crate) fn with_physical_store(
        mut self,
        store_id: StoreId,
        endpoint_type: EndpointType,
    ) -> Self {
        self.physical_store_id = Some(store_id);
        self.physical_endpoint_type = endpoint_type;
        self
    }

    /// Selects the logical peer whose identity must be placed in the TiKV
    /// request context. It does not change the physical RPC destination.
    pub fn with_target_peer(mut self, peer: metapb::Peer) -> Self {
        self.target_peer = Some(peer);
        self
    }

    /// Marks this route as a source-compatible forwarding request: transport
    /// goes to `target`, while TiKV forwards to this logical host.
    pub fn with_forwarded_host(mut self, host: impl Into<String>) -> Self {
        self.forwarded_host = host.into();
        self
    }

    pub fn with_stale_read(mut self, stale_read: bool) -> Self {
        self.stale_read = stale_read;
        self
    }

    pub(crate) fn with_busy_threshold(mut self, busy_threshold_ms: u32) -> Self {
        self.busy_threshold_ms = busy_threshold_ms;
        self
    }

    pub(crate) fn with_busy_threshold_disabled(mut self) -> Self {
        self.busy_threshold_disabled = true;
        self
    }

    pub(crate) fn with_force_leader_read(mut self) -> Self {
        self.force_leader_read = true;
        self
    }

    pub(crate) fn with_restored_suspect_leader(mut self) -> Self {
        self.restores_suspect_leader = true;
        self
    }

    pub(crate) fn with_health_status(mut self, health_status: Arc<StoreHealthStatus>) -> Self {
        self.health_status = Some(health_status);
        self
    }

    pub(crate) fn with_prefer_leader_slow_score(mut self, enabled: bool) -> Self {
        self.record_client_side_slow_score = enabled;
        self
    }

    pub(crate) fn with_resource_control_access_location(
        mut self,
        self_zone_label: &str,
        target_store: &metapb::Store,
    ) -> Self {
        self.resource_control_access_location =
            source_access_location(self_zone_label, target_store);
        self
    }

    pub(crate) fn with_store_token_count(mut self, store_token_count: Arc<AtomicI64>) -> Self {
        self.store_token_count = store_token_count;
        self
    }

    /// Returns the region metadata with the selected logical peer installed
    /// as its request-context leader. `Request::set_leader` historically uses
    /// this shape, so this preserves its API while permitting replica reads.
    pub fn request_region(&self) -> RegionWithLeader {
        let mut region = self.region_with_leader.clone();
        region.leader = self.target_peer.clone();
        region
    }

    /// Source `tikvrpc.Request.ReplicaRead` is set only when a non-leader
    /// peer was selected. Forwarding a leader through a proxy is still a
    /// leader read and therefore remains false.
    pub fn is_replica_read(&self) -> bool {
        if self.stale_read || self.force_leader_read {
            return false;
        }
        match (&self.region_with_leader.leader, &self.target_peer) {
            (Some(leader), Some(target)) => leader.id != target.id,
            _ => false,
        }
    }

    /// Source `replicaSelector.onSendSuccess` learns a replacement leader
    /// from a successful forced leader-read probe sent to a non-leader peer.
    /// Ordinary replica and stale reads must not promote their target.
    pub(crate) fn successful_forced_leader_peer(&self) -> Option<metapb::Peer> {
        let target = self.target_peer.as_ref()?;
        let leader = self.region_with_leader.leader.as_ref()?;
        (self.force_leader_read && target.id != leader.id).then(|| target.clone())
    }
}

pub(crate) struct StoreToken {
    count: Arc<AtomicI64>,
}

impl StoreToken {
    pub(crate) fn acquire(
        count: Arc<AtomicI64>,
        store_id: StoreId,
        store_addr: &str,
        limit: i64,
    ) -> Result<Self> {
        let current = count.load(Ordering::Relaxed);
        if current >= limit {
            crate::stats::increment_store_limit_error(store_addr, store_id);
            return Err(crate::Error::TokenLimit(crate::error::TokenLimitError {
                store_id,
            }));
        }
        count.fetch_add(1, Ordering::Relaxed);
        Ok(Self { count })
    }
}

impl Drop for StoreToken {
    fn drop(&mut self) {
        let current = self.count.load(Ordering::Relaxed);
        if current > 0 {
            self.count.fetch_sub(1, Ordering::Relaxed);
        } else {
            log::warn!("source store-token release observed a zero count");
        }
    }
}

fn source_replica_number(region: &RegionWithLeader) -> i64 {
    if region.region.peers.is_empty() {
        return 1;
    }
    region
        .region
        .peers
        .iter()
        .filter(|peer| {
            matches!(
                metapb::PeerRole::try_from(peer.role),
                Ok(metapb::PeerRole::Voter | metapb::PeerRole::Learner)
            )
        })
        .count() as i64
}

fn source_access_location(
    self_zone_label: &str,
    target_store: &metapb::Store,
) -> AccessLocationType {
    let target_zone_label = target_store
        .labels
        .iter()
        .find(|label| label.key == "zone")
        .map(|label| label.value.as_str())
        .unwrap_or_default();
    if self_zone_label.is_empty() || target_zone_label.is_empty() {
        AccessLocationType::Unknown
    } else if self_zone_label == target_zone_label {
        AccessLocationType::LocalZone
    } else {
        AccessLocationType::CrossZone
    }
}

#[derive(Clone)]
pub struct Store {
    pub client: Arc<dyn KvClient + Send + Sync>,
    /// Transport used by `StoreSafeTS`. TiFlash exposes that RPC on its peer
    /// service address, while ordinary all-store requests use `target`.
    pub safe_ts_client: Arc<dyn KvClient + Send + Sync>,
    /// Network address of the TiKV target, when the PD implementation has it.
    pub target: String,
    /// PD store identifier. Custom clients that construct a bare store retain zero.
    pub id: StoreId,
    /// Endpoint class derived from PD's engine labels.
    pub endpoint_type: EndpointType,
    /// PD labels retained for transaction-scope safe-TS aggregation.
    pub labels: Vec<metapb::StoreLabel>,
    /// TiFlash's peer service address. Ordinary TiKV stores leave this empty.
    pub peer_address: String,
}

impl Store {
    pub fn new(client: Arc<dyn KvClient + Send + Sync>) -> Self {
        Self {
            safe_ts_client: client.clone(),
            client,
            target: String::new(),
            id: 0,
            endpoint_type: EndpointType::TiKv,
            labels: Vec::new(),
            peer_address: String::new(),
        }
    }

    pub fn with_target(mut self, target: impl Into<String>) -> Self {
        self.target = target.into();
        self
    }

    /// Retains the PD metadata needed by root `tikv` store-wide operations.
    pub fn with_metadata(mut self, store: &metapb::Store) -> Self {
        self.id = store.id;
        self.endpoint_type = EndpointType::from_store(store);
        self.labels = store.labels.clone();
        self.peer_address = store.peer_address.clone();
        self
    }

    pub fn with_safe_ts_client(mut self, client: Arc<dyn KvClient + Send + Sync>) -> Self {
        self.safe_ts_client = client;
        self
    }

    /// Address used by TiKV's StoreSafeTS RPC.
    pub fn safe_ts_target(&self) -> &str {
        if self.endpoint_type == EndpointType::TiFlash && !self.peer_address.is_empty() {
            &self.peer_address
        } else {
            &self.target
        }
    }

    pub fn label_value(&self, key: &str) -> Option<&str> {
        self.labels
            .iter()
            .find(|label| label.key == key)
            .map(|label| label.value.as_str())
    }
}

/// Maps keys to a stream of stores. `key_data` must be sorted in increasing order
pub fn region_stream_for_keys<K, KOut, PdC>(
    key_data: impl Iterator<Item = K> + Send + Sync + 'static,
    pd_client: Arc<PdC>,
) -> BoxStream<'static, Result<(Vec<KOut>, RegionWithLeader)>>
where
    PdC: PdClient,
    K: AsRef<Key> + Into<KOut> + Send + Sync + 'static,
    KOut: Send + Sync + 'static,
{
    pd_client.clone().group_keys_by_region(key_data)
}

#[allow(clippy::type_complexity)]
pub fn region_stream_for_range<PdC: PdClient>(
    range: (Vec<u8>, Vec<u8>),
    pd_client: Arc<PdC>,
) -> BoxStream<'static, Result<((Vec<u8>, Vec<u8>), RegionWithLeader)>> {
    let bnd_range = if range.1.is_empty() {
        BoundRange::range_from(range.0.clone().into())
    } else {
        BoundRange::from(range.clone())
    };
    pd_client
        .regions_for_range(bnd_range)
        .map_ok(move |region| {
            let region_range = region.range();
            let result_range = range_intersection(
                region_range,
                (range.0.clone().into(), range.1.clone().into()),
            );
            ((result_range.0.into(), result_range.1.into()), region)
        })
        .boxed()
}

/// The range used for request should be the intersection of `region_range` and `range`.
fn range_intersection(region_range: (Key, Key), range: (Key, Key)) -> (Key, Key) {
    let (lower, upper) = region_range;
    let up = if upper.is_empty() {
        range.1
    } else if range.1.is_empty() {
        upper
    } else {
        min(upper, range.1)
    };
    (max(lower, range.0), up)
}

pub fn region_stream_for_ranges<PdC: PdClient>(
    ranges: Vec<kvrpcpb::KeyRange>,
    pd_client: Arc<PdC>,
) -> BoxStream<'static, Result<(Vec<kvrpcpb::KeyRange>, RegionWithLeader)>> {
    pd_client.clone().group_ranges_by_region(ranges)
}

#[cfg(test)]
mod tests {
    use std::any::Any;

    use async_trait::async_trait;

    use super::*;

    #[derive(Clone)]
    struct NoopClient;

    #[async_trait]
    impl KvClient for NoopClient {
        async fn dispatch(&self, _request: &dyn Request) -> Result<Box<dyn Any>> {
            Err(crate::Error::Unimplemented)
        }
    }

    #[test]
    fn source_region_store_keeps_logical_peer_separate_from_physical_proxy() {
        let leader = metapb::Peer {
            id: 1,
            store_id: 10,
            ..Default::default()
        };
        let follower = metapb::Peer {
            id: 2,
            store_id: 20,
            ..Default::default()
        };
        let region = RegionWithLeader::new(
            metapb::Region {
                id: 7,
                peers: vec![leader.clone(), follower.clone()],
                ..Default::default()
            },
            Some(leader.clone()),
        );

        let route = RegionStore::new(region, Arc::new(NoopClient))
            .with_target("proxy:20160")
            .with_target_peer(follower.clone())
            .with_forwarded_host("logical:20160");

        assert_eq!(route.region_with_leader.leader, Some(leader));
        assert_eq!(route.request_region().leader, Some(follower.clone()));
        assert!(route.is_replica_read());
        assert_eq!(route.target, "proxy:20160");
        assert_eq!(route.forwarded_host, "logical:20160");

        let stale_route = route.clone().with_stale_read(true);
        assert!(!stale_route.is_replica_read());
        assert!(stale_route.stale_read);

        let leader_probe_route = route.with_force_leader_read();
        assert!(!leader_probe_route.is_replica_read());
        assert_eq!(leader_probe_route.request_region().leader, Some(follower));
        assert_eq!(
            leader_probe_route.successful_forced_leader_peer(),
            leader_probe_route.target_peer.clone()
        );

        let threshold_route = stale_route.with_busy_threshold(123);
        assert_eq!(threshold_route.busy_threshold_ms, 123);
    }

    #[test]
    fn source_resource_control_route_counts_voters_and_learners_and_classifies_zone() {
        let region = RegionWithLeader::new(
            metapb::Region {
                peers: vec![
                    metapb::Peer {
                        role: metapb::PeerRole::Voter.into(),
                        ..Default::default()
                    },
                    metapb::Peer {
                        role: metapb::PeerRole::Learner.into(),
                        ..Default::default()
                    },
                    metapb::Peer {
                        role: metapb::PeerRole::IncomingVoter.into(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            None,
        );
        let store = metapb::Store {
            labels: vec![metapb::StoreLabel {
                key: "zone".to_owned(),
                value: "us-east-1a".to_owned(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let route = RegionStore::new(region, Arc::new(NoopClient))
            .with_resource_control_access_location("us-east-1a", &store);
        assert_eq!(route.resource_control_replica_number, 2);
        assert_eq!(
            route.resource_control_access_location,
            AccessLocationType::LocalZone
        );
        assert_eq!(
            route
                .clone()
                .with_resource_control_access_location("us-west-1a", &store)
                .resource_control_access_location,
            AccessLocationType::CrossZone
        );
        assert_eq!(
            route
                .with_resource_control_access_location("", &store)
                .resource_control_access_location,
            AccessLocationType::Unknown
        );
    }

    #[test]
    fn source_store_token_limit_rejects_and_releases() {
        let count = Arc::new(AtomicI64::new(0));
        let address = "store-42:20160";
        let metric_before = crate::stats::store_limit_error_count(address, 42);
        let token = StoreToken::acquire(count.clone(), 42, address, 1).unwrap();
        assert_eq!(count.load(Ordering::Relaxed), 1);
        assert!(matches!(
            StoreToken::acquire(count.clone(), 42, address, 1),
            Err(crate::Error::TokenLimit(error)) if error.store_id == 42
        ));
        assert_eq!(
            crate::stats::store_limit_error_count(address, 42),
            metric_before + 1
        );

        drop(token);
        assert_eq!(count.load(Ordering::Relaxed), 0);
    }
}
