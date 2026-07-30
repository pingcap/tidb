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
//! Go boundary: `pd/client`'s `pd_service_discovery.go` — the leader is tried
//! first, a transport-level failure walks the current member set in order, and
//! a membership refresh is what makes a new leader visible. The channel cache
//! here is the equivalent of Go's per-member `grpc.ClientConn` map: connections
//! are kept per endpoint and dropped when the member leaves the set.
//!
//! Every function is bounded by the *current* member set. None of them
//! discovers an endpoint PD did not name.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tidb_proto::pdpb::{self, pd_client::PdClient as TonicPdClient};
use tokio::sync::watch;
use tonic::transport::Channel;

use crate::{
    secure_endpoint, ClusterSecurity, PdClientError, PdGcState, PdMemberSet, PdRegion, PdStore,
};

use super::requests::{
    batch_scan_regions, get_gc_state, get_members, get_prev_region, get_region, get_region_by_id,
    get_store, scan_regions,
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
    let snapshot = state.read().expect("PD state lock poisoned").clone();
    let mut attempted = HashSet::new();
    attempted.insert(snapshot.active_endpoint.clone());
    match get_region(
        runtime,
        clients,
        &snapshot.active_endpoint,
        RpcControl { timeout, shutdown },
        snapshot.members.cluster_id,
        encoded_key,
        need_buckets,
    ) {
        Ok(region) => Ok(region),
        Err(error) if needs_failover_probe(&error) => {
            let direct_failure = is_direct_failure(&error);
            let mut last_error = error;
            // A bad membership observation never erases the last accepted
            // snapshot; its remaining direct endpoints are still candidates.
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
                match get_region(
                    runtime,
                    clients,
                    &endpoint,
                    RpcControl { timeout, shutdown },
                    current.members.cluster_id,
                    encoded_key,
                    need_buckets,
                ) {
                    Ok(region) => {
                        set_active_endpoint(state, endpoint);
                        return Ok(region);
                    }
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

pub(super) fn get_prev_region_with_failover(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    shutdown: &watch::Receiver<bool>,
    encoded_key: &[u8],
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    foreground_with_failover(
        runtime,
        clients,
        timeout,
        state,
        shutdown,
        |runtime, clients, endpoint, cluster_id| {
            get_prev_region(
                runtime,
                clients,
                endpoint,
                RpcControl { timeout, shutdown },
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
    foreground_with_failover(
        runtime,
        clients,
        timeout,
        state,
        shutdown,
        |runtime, clients, endpoint, cluster_id| {
            get_region_by_id(
                runtime,
                clients,
                endpoint,
                RpcControl { timeout, shutdown },
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
    foreground_with_failover(
        runtime,
        clients,
        timeout,
        state,
        shutdown,
        |runtime, clients, endpoint, cluster_id| {
            scan_regions(
                runtime, clients, endpoint, timeout, shutdown, cluster_id, request,
            )
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
) -> Result<Vec<PdRegion>, PdClientError> {
    foreground_with_failover(
        runtime,
        clients,
        timeout,
        state,
        shutdown,
        |runtime, clients, endpoint, cluster_id| {
            batch_scan_regions(
                runtime, clients, endpoint, timeout, shutdown, cluster_id, request,
            )
        },
    )
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
    let mut attempted = HashSet::new();
    attempted.insert(snapshot.active_endpoint.clone());
    match action(
        runtime,
        clients,
        &snapshot.active_endpoint,
        snapshot.members.cluster_id,
    ) {
        Ok(value) => Ok(value),
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
    let snapshot = state.read().expect("PD state lock poisoned").clone();
    let mut attempted = HashSet::new();
    attempted.insert(snapshot.active_endpoint.clone());
    match get_gc_state(
        runtime,
        clients,
        &snapshot.active_endpoint,
        timeout,
        shutdown,
        snapshot.members.cluster_id,
        keyspace_id,
    ) {
        Ok(gc_state) => Ok(gc_state),
        // An `Unimplemented` PD is uniformly old, so probing its peers would
        // only repeat the same answer; the caller latches the fallback instead.
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
                match get_gc_state(
                    runtime,
                    clients,
                    &endpoint,
                    timeout,
                    shutdown,
                    current.members.cluster_id,
                    keyspace_id,
                ) {
                    Ok(gc_state) => {
                        set_active_endpoint(state, endpoint);
                        return Ok(gc_state);
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
    let snapshot = state.read().expect("PD state lock poisoned").clone();
    let mut attempted = HashSet::new();
    attempted.insert(snapshot.active_endpoint.clone());
    match get_store(
        runtime,
        clients,
        &snapshot.active_endpoint,
        timeout,
        shutdown,
        snapshot.members.cluster_id,
        store_id,
    ) {
        Ok(store) => Ok(store),
        Err(error) if needs_failover_probe(&error) => {
            let direct_failure = is_direct_failure(&error);
            let mut last_error = error;
            // A bad membership observation never erases the last accepted
            // snapshot; its remaining direct endpoints are still candidates.
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
                match get_store(
                    runtime,
                    clients,
                    &endpoint,
                    timeout,
                    shutdown,
                    current.members.cluster_id,
                    store_id,
                ) {
                    Ok(store) => {
                        set_active_endpoint(state, endpoint);
                        return Ok(store);
                    }
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

pub(super) fn refresh_membership(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    shutdown: &watch::Receiver<bool>,
) -> Result<PdMemberSet, PdClientError> {
    let snapshot = state.read().expect("PD state lock poisoned").clone();
    let mut last_error = None;
    let mut cluster_mismatch = None;
    for endpoint in endpoint_attempt_order(&snapshot) {
        match get_members(
            runtime,
            clients,
            &endpoint,
            timeout,
            shutdown,
            Some(snapshot.members.cluster_id),
        ) {
            Ok(observation) => match observation.projected {
                Ok(members) => {
                    retain_member_clients(clients, &members);
                    let mut current = state.write().expect("PD state lock poisoned");
                    current.active_endpoint = members.leader_url.clone();
                    current.members = members.clone();
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

/// The worker-owned PD channel cache, paired with the cluster TLS security so
/// every channel this client opens routes through the one shared
/// [`crate::secure_endpoint`] helper and none is left plaintext by omission.
pub(crate) struct PdChannelCache {
    clients: HashMap<String, TonicPdClient<Channel>>,
    security: Arc<ClusterSecurity>,
}

impl PdChannelCache {
    pub(super) fn new(security: Arc<ClusterSecurity>) -> Self {
        Self {
            clients: HashMap::new(),
            security,
        }
    }
}

impl std::ops::Deref for PdChannelCache {
    type Target = HashMap<String, TonicPdClient<Channel>>;
    fn deref(&self) -> &Self::Target {
        &self.clients
    }
}

impl std::ops::DerefMut for PdChannelCache {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.clients
    }
}

pub(super) fn tonic_client<'a>(
    runtime: &tokio::runtime::Runtime,
    clients: &'a mut PdChannelCache,
    endpoint: &str,
) -> Result<&'a mut TonicPdClient<Channel>, PdClientError> {
    let security = Arc::clone(&clients.security);
    match clients.clients.entry(endpoint.to_owned()) {
        std::collections::hash_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
        std::collections::hash_map::Entry::Vacant(entry) => {
            let parsed = secure_endpoint(endpoint, &security).map_err(|error| {
                PdClientError::InvalidEndpoint {
                    endpoint: endpoint.to_owned(),
                    message: error.to_string(),
                }
            })?;
            let channel = {
                let _guard = runtime.enter();
                parsed.connect_lazy()
            };
            Ok(entry.insert(TonicPdClient::new(channel)))
        }
    }
}

pub(super) fn retain_member_clients(clients: &mut PdChannelCache, members: &PdMemberSet) {
    clients.retain(|endpoint, _| members.member_urls.contains(endpoint));
}
