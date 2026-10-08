// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Which PD member serves a call, and when a failure means moving to another
//! one.
//!
//! Go boundary: `pd/client/servicediscovery/service_discovery.go` — the leader is tried
//! first unless shared health selects configured forwarding. The retained retry
//! owner walks accepted membership on failures, and
//! a membership refresh is what makes a new leader visible. The shared native
//! channel map retains endpoint connections until close, as Go's discovery
//! owner does; metadata membership does not evict active TSO connections.
//!
//! Every function is bounded by the *current* member set. None of them
//! discovers an endpoint PD did not name.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tidb_proto::pdpb::{self, pd_client::PdClient as TonicPdClient};
use tokio::sync::watch;
use tonic::transport::Channel;

use crate::{
    secure_endpoint, ClusterSecurity, PdClientError, PdGcState, PdMemberSet, PdRegion, PdStore,
};

use super::requests::{
    batch_scan_regions, get_all_stores, get_gc_state, get_prev_region, get_region,
    get_region_by_id, get_store, scan_regions,
};
use super::topology::invalid_topology;
use super::{PdSharedState, RpcControl};

pub(super) fn get_region_with_failover(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    shutdown: &watch::Receiver<bool>,
    encoded_key: &[u8],
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    region_with_failover(
        runtime,
        clients,
        timeout,
        state,
        shutdown,
        true,
        |runtime, clients, endpoint, cluster_id, control| {
            get_region(
                runtime,
                clients,
                endpoint,
                control,
                cluster_id,
                encoded_key,
                need_buckets,
            )
        },
    )
}

pub(super) fn get_prev_region_with_failover(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    shutdown: &watch::Receiver<bool>,
    encoded_key: &[u8],
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    region_with_failover(
        runtime,
        clients,
        timeout,
        state,
        shutdown,
        true,
        |runtime, clients, endpoint, cluster_id, control| {
            get_prev_region(
                runtime,
                clients,
                endpoint,
                control,
                cluster_id,
                encoded_key,
                need_buckets,
            )
        },
    )
}

pub(super) fn get_region_by_id_with_failover(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    shutdown: &watch::Receiver<bool>,
    region_id: u64,
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    region_with_failover(
        runtime,
        clients,
        timeout,
        state,
        shutdown,
        true,
        |runtime, clients, endpoint, cluster_id, control| {
            get_region_by_id(
                runtime,
                clients,
                endpoint,
                control,
                cluster_id,
                region_id,
                need_buckets,
            )
        },
    )
}

pub(super) fn scan_regions_with_failover(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    shutdown: &watch::Receiver<bool>,
    request: &pdpb::ScanRegionsRequest,
) -> Result<Vec<PdRegion>, PdClientError> {
    region_with_failover(
        runtime,
        clients,
        timeout,
        state,
        shutdown,
        true,
        |runtime, clients, endpoint, cluster_id, control| {
            scan_regions(runtime, clients, endpoint, control, cluster_id, request)
        },
    )
}

pub(super) fn batch_scan_regions_with_failover(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    shutdown: &watch::Receiver<bool>,
    request: &pdpb::BatchScanRegionsRequest,
    allow_follower: bool,
) -> Result<Vec<PdRegion>, PdClientError> {
    region_with_failover(
        runtime,
        clients,
        timeout,
        state,
        shutdown,
        allow_follower,
        |runtime, clients, endpoint, cluster_id, control| {
            batch_scan_regions(runtime, clients, endpoint, control, cluster_id, request)
        },
    )
}

// Region metadata may be served locally by a follower only when both policy
// layers permit it. A follower error gets one leader attempt within the same
// deadline; projection errors and cancellation are not service retry signals.
fn region_with_failover<T, F>(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    shutdown: &watch::Receiver<bool>,
    allowed: bool,
    mut action: F,
) -> Result<T, PdClientError>
where
    F: FnMut(
        &tokio::runtime::Runtime,
        &mut PdChannelCache,
        &str,
        u64,
        RpcControl<'_>,
    ) -> Result<T, PdClientError>,
{
    if !clients.options.get_enable_follower_handle() && !clients.options.enable_forwarding {
        return foreground_with_failover(
            runtime,
            clients,
            timeout,
            state,
            shutdown,
            |runtime, clients, endpoint, cluster_id| {
                action(
                    runtime,
                    clients,
                    endpoint,
                    cluster_id,
                    RpcControl {
                        forwarding: None,
                        timeout,
                        shutdown,
                        follower: false,
                    },
                )
            },
        );
    }
    let snapshot = state.read().expect("PD state lock poisoned").clone();
    let target = clients.regions.select_with_forwarding(
        &snapshot.members.leader_url,
        &snapshot.members.member_urls,
        clients.options.get_enable_follower_handle(),
        allowed,
        clients.options.enable_forwarding,
    );
    let forwarding = tikv_client::pd_region_service::Forwarding(target.forwarded_host.clone());
    let deadline = std::time::Instant::now() + timeout;
    let result = action(
        runtime,
        clients,
        &target.endpoint,
        snapshot.members.cluster_id,
        RpcControl {
            forwarding: Some(&forwarding),
            timeout,
            shutdown,
            follower: target.follower,
        },
    );
    let retry = target.observe_error(
        matches!(
            &result,
            Err(PdClientError::Transport { .. } | PdClientError::Timeout { .. })
        ),
        match &result {
            Err(PdClientError::HeaderError { error_type, .. }) => Some(*error_type),
            _ => None,
        },
    );
    if retry && !*shutdown.borrow() {
        if let Some(remaining) = deadline
            .checked_duration_since(std::time::Instant::now())
            .filter(|duration| !duration.is_zero())
        {
            let current = state.read().expect("PD state lock poisoned").clone();
            let (endpoint, forwarding) = clients.regions.forwarding_target(
                &current.members.leader_url,
                clients.options.enable_forwarding,
            );
            return action(
                runtime,
                clients,
                &endpoint,
                current.members.cluster_id,
                RpcControl {
                    forwarding: Some(&forwarding),
                    timeout: remaining,
                    shutdown,
                    follower: false,
                },
            );
        }
    }
    result
}

pub(super) fn foreground_with_failover<T, F>(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    shutdown: &watch::Receiver<bool>,
    mut action: F,
) -> Result<T, PdClientError>
where
    F: FnMut(&tokio::runtime::Runtime, &mut PdChannelCache, &str, u64) -> Result<T, PdClientError>,
{
    let snapshot = state.read().expect("PD state lock poisoned").clone();
    if clients.options.enable_forwarding {
        // Go getClientAndContext selects one service for this RPC. A failed
        // proxy must not enter the legacy direct-member walk without metadata.
        return action(
            runtime,
            clients,
            &snapshot.members.leader_url,
            snapshot.members.cluster_id,
        );
    }
    let mut attempted = HashSet::new();
    attempted.insert(snapshot.active_endpoint.clone());
    match action(
        runtime,
        clients,
        &snapshot.active_endpoint,
        snapshot.members.cluster_id,
    ) {
        Ok(value) => Ok(value),
        Err(error) if is_unimplemented(&error) => Err(error),
        Err(error) if needs_failover_probe(&error) => {
            let direct_failure = is_direct_failure(&error);
            let mut last_error = error;
            if let Err(error @ PdClientError::ClusterMismatch { .. }) =
                refresh_membership(runtime, clients, timeout, state, shutdown)
            {
                return Err(error);
            }
            let current = state.read().expect("PD state lock poisoned").clone();
            if !direct_failure && snapshot.active_endpoint == current.members.leader_url {
                return Err(last_error);
            }
            for endpoint in endpoint_attempt_order(&current) {
                if !attempted.insert(endpoint.clone()) {
                    continue;
                }
                match action(runtime, clients, &endpoint, current.members.cluster_id) {
                    Ok(value) => {
                        set_active_endpoint(state, endpoint);
                        return Ok(value);
                    }
                    Err(error) if is_unimplemented(&error) => return Err(error),
                    Err(error)
                        if is_retryable_endpoint_error(
                            &error,
                            &endpoint,
                            &current.members.leader_url,
                        ) =>
                    {
                        last_error = error;
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(last_error)
        }
        Err(error) => Err(error),
    }
}

pub(super) fn foreground_leader_only<T, F>(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    state: &Arc<RwLock<PdSharedState>>,
    mut action: F,
) -> Result<T, PdClientError>
where
    F: FnMut(&tokio::runtime::Runtime, &mut PdChannelCache, &str, u64) -> Result<T, PdClientError>,
{
    let snapshot = state.read().expect("PD state lock poisoned").clone();
    action(
        runtime,
        clients,
        &snapshot.members.leader_url,
        snapshot.members.cluster_id,
    )
}

pub(super) fn get_gc_state_with_failover(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    shutdown: &watch::Receiver<bool>,
    keyspace_id: Option<u32>,
) -> Result<PdGcState, PdClientError> {
    foreground_with_failover(
        runtime,
        clients,
        timeout,
        state,
        shutdown,
        |runtime, clients, endpoint, cluster_id| {
            get_gc_state(
                runtime,
                clients,
                endpoint,
                timeout,
                shutdown,
                cluster_id,
                keyspace_id,
            )
        },
    )
}

/// Whether PD rejected the call because it does not implement the method.
///
/// This is the one PD failure a caller may answer by falling back to an older
/// mechanism rather than by retrying elsewhere.
#[must_use]
pub fn is_unimplemented(error: &PdClientError) -> bool {
    matches!(
        error,
        PdClientError::Transport { code, .. } if code == "Unimplemented"
    )
}

pub(super) fn get_store_with_failover(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    shutdown: &watch::Receiver<bool>,
    store_id: u64,
) -> Result<Option<PdStore>, PdClientError> {
    foreground_with_failover(
        runtime,
        clients,
        timeout,
        state,
        shutdown,
        |runtime, clients, endpoint, cluster_id| {
            get_store(
                runtime, clients, endpoint, timeout, shutdown, cluster_id, store_id,
            )
        },
    )
}

/// Go boundary: `client.go` -> `GetAllStores`, routed through the same
/// leader-first, member-set walk every other PD call uses.
pub(super) fn get_all_stores_with_failover(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    shutdown: &watch::Receiver<bool>,
) -> Result<Vec<tidb_proto::metapb::Store>, PdClientError> {
    foreground_with_failover(
        runtime,
        clients,
        timeout,
        state,
        shutdown,
        |runtime, clients, endpoint, cluster_id| {
            get_all_stores(runtime, clients, endpoint, timeout, shutdown, cluster_id)
        },
    )
}

pub(super) fn refresh_membership(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    shutdown: &watch::Receiver<bool>,
) -> Result<PdMemberSet, PdClientError> {
    runtime.block_on(refresh_membership_async(clients, timeout, state, shutdown))
}

pub(super) async fn refresh_membership_async(
    clients: &PdChannelCache,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    shutdown: &watch::Receiver<bool>,
) -> Result<PdMemberSet, PdClientError> {
    // Background and foreground observations publish in request order, while
    // timestamp discovery uses its own lock and continues independently.
    let _refresh = clients.membership_refresh.lock().await;
    let snapshot = state.read().expect("PD state lock poisoned").clone();
    let mut last_error = None;
    let mut cluster_mismatch = None;
    for endpoint in endpoint_attempt_order(&snapshot) {
        match super::requests::get_members_async(
            clients,
            &endpoint,
            timeout,
            shutdown,
            Some(snapshot.members.cluster_id),
        )
        .await
        {
            Ok(observation) => match observation.projected {
                Ok(members) => {
                    let mut current = state.write().expect("PD state lock poisoned");
                    current.active_endpoint = members.leader_url.clone();
                    current.members = members.clone();
                    clients
                        .regions
                        .update_members(&members.leader_url, &members.member_urls);
                    return Ok(members);
                }
                Err(error) => last_error = Some(error),
            },
            Err(error @ PdClientError::ClusterMismatch { .. }) => cluster_mismatch = Some(error),
            Err(error) => last_error = Some(error),
        }
    }
    Err(cluster_mismatch.or(last_error).unwrap_or_else(|| {
        invalid_topology(
            "missing_pd_member",
            "membership contains no usable endpoint",
        )
    }))
}

pub(super) fn endpoint_attempt_order(state: &PdSharedState) -> Vec<String> {
    let mut endpoints = Vec::with_capacity(state.members.member_urls.len() + 2);
    let mut seen = HashSet::new();
    for endpoint in std::iter::once(&state.active_endpoint)
        .chain(std::iter::once(&state.members.leader_url))
        .chain(state.members.member_urls.iter())
    {
        if seen.insert(endpoint.clone()) {
            endpoints.push(endpoint.clone());
        }
    }
    endpoints
}

pub(super) fn set_active_endpoint(state: &Arc<RwLock<PdSharedState>>, endpoint: String) {
    state
        .write()
        .expect("PD state lock poisoned")
        .active_endpoint = endpoint;
}

pub(super) fn is_direct_failure(error: &PdClientError) -> bool {
    match error {
        PdClientError::Timeout { .. } => true,
        PdClientError::Transport { code, .. } => {
            matches!(
                code.as_str(),
                "Unavailable" | "DeadlineExceeded" | "Cancelled"
            )
        }
        _ => false,
    }
}

pub(super) fn needs_failover_probe(error: &PdClientError) -> bool {
    is_direct_failure(error)
        || matches!(
            error,
            PdClientError::Transport { .. } | PdClientError::HeaderError { .. }
        )
}

pub(super) fn is_retryable_endpoint_error(
    error: &PdClientError,
    endpoint: &str,
    leader_endpoint: &str,
) -> bool {
    is_direct_failure(error)
        || (endpoint != leader_endpoint
            && matches!(
                error,
                PdClientError::Transport { .. } | PdClientError::HeaderError { .. }
            ))
}

/// The shared PD endpoint owner. Protobuf stubs are short-lived Channel
/// clones; discovery, membership and TSO retain the same connection map.
#[derive(Clone)]
pub(crate) struct PdChannelCache {
    pub(super) options: Arc<tikv_client::pd_options::Options>,
    pub(super) regions: Arc<tikv_client::pd_region_service::RegionService>,
    pub(super) channels: Arc<tikv_client::pd_service_discovery::ChannelCache>,
    security: Arc<ClusterSecurity>,
    membership_refresh: Arc<tokio::sync::Mutex<()>>,
    pub(super) tso_discovery: Arc<tokio::sync::Mutex<TsoDiscoveryState>>,
    pub(super) tso_forwarding: tikv_client::pd_service_discovery::TsoForwarding,
    pub(super) service_mode: tikv_client::pd_service_discovery::ServiceModeDiscovery,
    pub(super) member_wake: Arc<tokio::sync::Notify>,
    pub(super) tso_routes:
        tokio::sync::watch::Sender<Vec<tikv_client::pd_service_discovery::TsoRoute>>,
}

#[derive(Default)]
pub(super) struct TsoDiscoveryState {
    pub(super) discovery: tikv_client::pd_service_discovery::TsoDiscovery,
    pub(super) route: Option<(
        Vec<tikv_client::pd_service_discovery::TsoRoute>,
        std::time::Instant,
        String,
        bool,
        Vec<String>,
    )>,
}

impl PdChannelCache {
    pub(super) fn new(security: Arc<ClusterSecurity>) -> Self {
        let discovery = TsoDiscoveryState::default();
        let tso_forwarding = discovery.discovery.forwarding();
        let service_mode = discovery.discovery.service_mode();
        Self {
            channels: Arc::new(tikv_client::pd_service_discovery::ChannelCache::default()),
            options: Arc::new(tikv_client::pd_options::Options::new()),
            regions: Arc::default(),
            security,
            membership_refresh: Arc::default(),
            tso_discovery: Arc::new(tokio::sync::Mutex::new(discovery)),
            tso_forwarding,
            service_mode,
            member_wake: Arc::default(),
            tso_routes: tokio::sync::watch::channel(Vec::new()).0,
        }
    }

    /// Called while entered into the single process runtime, including by
    /// its discovery task. Lazy channel drivers must never belong to a
    /// temporary or separately stopped runtime.
    pub(super) fn channel(&self, endpoint: &str) -> Result<Channel, PdClientError> {
        self.channels
            .get_or_insert_with(endpoint, || {
                secure_endpoint(endpoint, &self.security)
                    .map(|endpoint| {
                        tikv_client::pd_service_discovery::lazy_channel(endpoint, &self.options)
                    })
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))
            })
            .map_err(|error| {
                if error.code() == tonic::Code::Cancelled {
                    PdClientError::Closed
                } else {
                    PdClientError::InvalidEndpoint {
                        endpoint: endpoint.to_owned(),
                        message: error.to_string(),
                    }
                }
            })
    }

    pub(super) fn tls_enabled(&self) -> bool {
        self.security.is_tls_enabled()
    }

    pub(super) fn close(&self) {
        self.channels.close();
    }
}

pub(super) fn tonic_client(
    runtime: &tokio::runtime::Runtime,
    clients: &PdChannelCache,
    endpoint: &str,
) -> Result<
    TonicPdClient<
        tonic::service::interceptor::InterceptedService<
            Channel,
            tikv_client::pd_region_service::Forwarding,
        >,
    >,
    PdClientError,
> {
    let _guard = runtime.enter();
    let (endpoint, forwarding) = clients
        .regions
        .forwarding_target(endpoint, clients.options.enable_forwarding);
    Ok(TonicPdClient::with_interceptor(
        clients.channel(&endpoint)?,
        forwarding,
    ))
}

pub(super) fn region_tonic_client(
    runtime: &tokio::runtime::Runtime,
    clients: &PdChannelCache,
    endpoint: &str,
    control: RpcControl<'_>,
) -> Result<
    TonicPdClient<
        tonic::service::interceptor::InterceptedService<
            Channel,
            tikv_client::pd_region_service::Forwarding,
        >,
    >,
    PdClientError,
> {
    let _guard = runtime.enter();
    Ok(TonicPdClient::with_interceptor(
        clients.channel(endpoint)?,
        control.forwarding.cloned().unwrap_or_default(),
    ))
}
