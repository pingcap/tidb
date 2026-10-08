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

//! The synchronous PD command loop and its process-owned asynchronous runtime.
//! Discovery and all channel drivers share that runtime and joined shutdown.
//!
//! Go boundary: `pd/client`'s `client.go` — the client goroutine that owns the
//! Tokio-equivalent runtime, bootstraps the member set before the first call,
//! and holds the retained TSO stream (`tso_client.go`) so a timestamp costs one
//! stream round trip rather than one connection.

use std::collections::VecDeque;
use std::sync::{mpsc, Arc, RwLock};
use std::time::{Duration, Instant};
use tikv_client::pd_service_discovery::TsoStreamSet;

use tidb_proto::pdpb;
use tokio::sync::watch;

use crate::tso::{
    batch_error, remaining as remaining_tso_time, request_batch, retry_delay, timeout_error,
    TimestampTracker, TsoBatch,
};

/// Upper bound on waiters merged into one PD Tso round trip.
///
/// Go dispatcher uses `defaultMaxTSOBatchSize * 2` for its controller.
const MAX_TSO_BATCH_SIZE: usize = 20000;

/// What one batched PD Tso round trip must satisfy: the earliest waiter's
/// deadline, and how many waiters share the reply.
#[derive(Clone, Copy)]
pub(super) struct TsoBatchSpec {
    pub(super) deadline: Instant,
    pub(super) count: u32,
}
use crate::{PdClientError, PdMemberSet, PdOperation};

use super::failover::{
    batch_scan_regions_with_failover, endpoint_attempt_order, foreground_leader_only,
    get_all_stores_with_failover, get_gc_state_with_failover, get_prev_region_with_failover,
    get_region_by_id_with_failover, get_region_with_failover, get_store_with_failover,
    refresh_membership, scan_regions_with_failover, PdChannelCache,
};
use super::requests::{get_members, get_prev_region, get_region, get_region_by_id, scan_regions};
use super::topology::invalid_topology;
use super::{wait_for_shutdown, PdSharedState, RpcControl, WorkerCommand};

// Go pkg/store/store.go retries opening a store 30 times with a 500 ms
// linearly increasing delay when PD reports NOT_BOOTSTRAPPED. PD client
// servicediscovery.initRetry also waits through ErrServerNotStarted, which
// PD encodes as UNKNOWN rather than NOT_BOOTSTRAPPED.
const BOOTSTRAP_MAX_ATTEMPTS: usize = 30;
const BOOTSTRAP_RETRY_INTERVAL: Duration = Duration::from_millis(500);

type TsoWaiter = (Instant, mpsc::Sender<Result<u64, PdClientError>>);

// Project the timestamp queue while keeping other commands in their original
// order. Collection, adaptive waiting and completion belong to native batch.
struct TsoRequests<'a> {
    first: Option<TsoWaiter>,
    commands: &'a mut tokio::sync::mpsc::UnboundedReceiver<WorkerCommand>,
    deferred: &'a mut VecDeque<WorkerCommand>,
}

impl tikv_client::pd_batch::RequestReceiver<TsoWaiter> for TsoRequests<'_> {
    async fn recv(&mut self) -> Option<TsoWaiter> {
        if let Some(first) = self.first.take() {
            return Some(first);
        }
        loop {
            match self.commands.recv().await? {
                WorkerCommand::GetTimestamp { deadline, reply } => return Some((deadline, reply)),
                close @ WorkerCommand::Close { .. } => {
                    self.deferred.push_back(close);
                    return None;
                }
                other => self.deferred.push_back(other),
            }
        }
    }

    fn try_recv(&mut self) -> Result<TsoWaiter, tokio::sync::mpsc::error::TryRecvError> {
        if let Some(first) = self.first.take() {
            return Ok(first);
        }
        loop {
            match self.commands.try_recv()? {
                WorkerCommand::GetTimestamp { deadline, reply } => return Ok((deadline, reply)),
                close @ WorkerCommand::Close { .. } => {
                    self.deferred.push_back(close);
                    return Err(tokio::sync::mpsc::error::TryRecvError::Disconnected);
                }
                other => self.deferred.push_back(other),
            }
        }
    }
}

pub(super) fn run_worker(
    runtime: tokio::runtime::Runtime,
    mut clients: PdChannelCache,
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<WorkerCommand>,
    timeout: Duration,
    state: Arc<RwLock<PdSharedState>>,
    shutdown: watch::Receiver<bool>,
) {
    {
        let current = state.read().expect("PD state lock poisoned");
        clients
            .regions
            .update_members(&current.members.leader_url, &current.members.member_urls);
    }
    let mut discovery_worker =
        DiscoveryWorker::start(&runtime, &clients, timeout, state.clone(), shutdown.clone());
    let mut tso_stream = TsoStreamSet::default();
    let last_timestamp = TimestampTracker::default();
    // Non-TSO commands displaced while draining the channel for TSO waiters.
    let mut deferred: VecDeque<WorkerCommand> = VecDeque::new();
    let mut collector = tikv_client::pd_batch::Controller::new(
        MAX_TSO_BATCH_SIZE,
        Some(Box::new(|_, (_, reply): TsoWaiter, _| {
            let _ = reply.send(Err(PdClientError::Closed));
        })),
        Some(
            tikv_client::pd_metrics::global_metrics()
                .tso_best_batch_size
                .clone(),
        ),
    );
    loop {
        let command = match deferred.pop_front() {
            Some(command) => command,
            None => {
                match runtime.block_on(async {
                    tokio::time::timeout(
                        tikv_client::pd_service_discovery::UPDATE_INTERVAL,
                        receiver.recv(),
                    )
                    .await
                }) {
                    Ok(Some(command)) => command,
                    Err(_) => {
                        if *shutdown.borrow() {
                            // The owner signals cancellation before enqueueing
                            // Close. Keep receiving until we acknowledge it.
                            continue;
                        }
                        if let Ok(discovery) = clients.tso_discovery.try_lock() {
                            if let Some((routes, _, _, _, _)) = &discovery.route {
                                tso_stream.retain_routes(routes);
                            }
                        }
                        continue;
                    }
                    Ok(None) => break,
                }
            }
        };
        if *shutdown.borrow() {
            match command {
                WorkerCommand::RefreshMembers { reply } => {
                    let _ = reply.send(Err(PdClientError::Closed));
                }
                WorkerCommand::GetRegion { reply, .. } => {
                    let _ = reply.send(Err(PdClientError::Closed));
                }
                WorkerCommand::GetPrevRegion { reply, .. } => {
                    let _ = reply.send(Err(PdClientError::Closed));
                }
                WorkerCommand::GetRegionById { reply, .. } => {
                    let _ = reply.send(Err(PdClientError::Closed));
                }
                WorkerCommand::ScanRegions { reply, .. } => {
                    let _ = reply.send(Err(PdClientError::Closed));
                }
                WorkerCommand::BatchScanRegions { reply, .. } => {
                    let _ = reply.send(Err(PdClientError::Closed));
                }
                WorkerCommand::GetStore { reply, .. } => {
                    let _ = reply.send(Err(PdClientError::Closed));
                }
                WorkerCommand::GetAllStores { reply } => {
                    let _ = reply.send(Err(PdClientError::Closed));
                }
                WorkerCommand::GetTimestamp { reply, .. } => {
                    let _ = reply.send(Err(PdClientError::Closed));
                }
                WorkerCommand::ExternalTimestamp { reply, .. } => {
                    let _ = reply.send(Err(PdClientError::Closed));
                }
                WorkerCommand::GetGcState { reply, .. } => {
                    let _ = reply.send(Err(PdClientError::Closed));
                }
                WorkerCommand::StoreGlobalConfig { reply, .. } => {
                    let _ = reply.send(Err(PdClientError::Closed));
                }
                WorkerCommand::Close { reply } => {
                    discovery_worker.close();
                    tso_stream.clear();
                    let _ = reply.send(());
                    break;
                }
            }
            continue;
        }
        match command {
            WorkerCommand::RefreshMembers { reply } => {
                let previous_leader = state
                    .read()
                    .expect("PD state lock poisoned")
                    .members
                    .leader_url
                    .clone();
                let result = refresh_membership(&runtime, &mut clients, timeout, &state, &shutdown);
                if result
                    .as_ref()
                    .is_ok_and(|members| members.leader_url != previous_leader)
                {
                    tso_stream.clear();
                }
                let _ = reply.send(result);
            }
            WorkerCommand::GetRegion {
                encoded_key,
                need_buckets,
                leader_only,
                reply,
            } => {
                let result = if leader_only {
                    foreground_leader_only(
                        &runtime,
                        &mut clients,
                        &state,
                        |runtime, clients, endpoint, cluster_id| {
                            get_region(
                                runtime,
                                clients,
                                endpoint,
                                RpcControl {
                                    forwarding: None,
                                    follower: false,
                                    timeout,
                                    shutdown: &shutdown,
                                },
                                cluster_id,
                                &encoded_key,
                                need_buckets,
                            )
                        },
                    )
                } else {
                    get_region_with_failover(
                        &runtime,
                        &mut clients,
                        timeout,
                        &state,
                        &shutdown,
                        &encoded_key,
                        need_buckets,
                    )
                };
                let _ = reply.send(result);
            }
            WorkerCommand::GetPrevRegion {
                encoded_key,
                need_buckets,
                leader_only,
                reply,
            } => {
                let result = if leader_only {
                    foreground_leader_only(
                        &runtime,
                        &mut clients,
                        &state,
                        |runtime, clients, endpoint, cluster_id| {
                            get_prev_region(
                                runtime,
                                clients,
                                endpoint,
                                RpcControl {
                                    forwarding: None,
                                    follower: false,
                                    timeout,
                                    shutdown: &shutdown,
                                },
                                cluster_id,
                                &encoded_key,
                                need_buckets,
                            )
                        },
                    )
                } else {
                    get_prev_region_with_failover(
                        &runtime,
                        &mut clients,
                        timeout,
                        &state,
                        &shutdown,
                        &encoded_key,
                        need_buckets,
                    )
                };
                let _ = reply.send(result);
            }
            WorkerCommand::GetRegionById {
                region_id,
                need_buckets,
                leader_only,
                reply,
            } => {
                let result = if leader_only {
                    foreground_leader_only(
                        &runtime,
                        &mut clients,
                        &state,
                        |runtime, clients, endpoint, cluster_id| {
                            get_region_by_id(
                                runtime,
                                clients,
                                endpoint,
                                RpcControl {
                                    forwarding: None,
                                    follower: false,
                                    timeout,
                                    shutdown: &shutdown,
                                },
                                cluster_id,
                                region_id,
                                need_buckets,
                            )
                        },
                    )
                } else {
                    get_region_by_id_with_failover(
                        &runtime,
                        &mut clients,
                        timeout,
                        &state,
                        &shutdown,
                        region_id,
                        need_buckets,
                    )
                };
                let _ = reply.send(result);
            }
            WorkerCommand::ScanRegions {
                request,
                leader_only,
                reply,
            } => {
                let result = if leader_only {
                    foreground_leader_only(
                        &runtime,
                        &mut clients,
                        &state,
                        |runtime, clients, endpoint, cluster_id| {
                            scan_regions(
                                runtime,
                                clients,
                                endpoint,
                                RpcControl {
                                    forwarding: None,
                                    timeout,
                                    shutdown: &shutdown,
                                    follower: false,
                                },
                                cluster_id,
                                &request,
                            )
                        },
                    )
                } else {
                    scan_regions_with_failover(
                        &runtime,
                        &mut clients,
                        timeout,
                        &state,
                        &shutdown,
                        &request,
                    )
                };
                let _ = reply.send(result);
            }
            WorkerCommand::BatchScanRegions {
                request,
                allow_follower,
                reply,
            } => {
                let result = batch_scan_regions_with_failover(
                    &runtime,
                    &mut clients,
                    timeout,
                    &state,
                    &shutdown,
                    &request,
                    allow_follower,
                );
                let _ = reply.send(result);
            }
            WorkerCommand::GetStore { store_id, reply } => {
                let result = get_store_with_failover(
                    &runtime,
                    &mut clients,
                    timeout,
                    &state,
                    &shutdown,
                    store_id,
                );
                let _ = reply.send(result);
            }
            WorkerCommand::GetAllStores { reply } => {
                let result = get_all_stores_with_failover(
                    &runtime,
                    &mut clients,
                    timeout,
                    &state,
                    &shutdown,
                );
                let _ = reply.send(result);
            }
            WorkerCommand::GetTimestamp { deadline, reply } => {
                let cancellation = tikv_client::async_util::Cancellation::default();
                let mut shutdown_wait = shutdown.clone();
                let mut requests = TsoRequests {
                    first: Some((deadline, reply)),
                    commands: &mut receiver,
                    deferred: &mut deferred,
                };
                let collected = runtime.block_on(async {
                    tokio::select! {
                        biased;
                        () = super::shutdown_requested(&mut shutdown_wait) => Err(tikv_client::Error::ContextCanceled),
                        result = collector.fetch_pending_requests(&cancellation, &mut requests, None,
                            clients.options.get_max_tso_batch_wait_interval()) => result,
                    }
                });
                if collected.is_err() {
                    continue;
                }
                collector.adjust_best_batch_size();
                let batch_deadline = collector
                    .get_collected_requests()
                    .iter()
                    .map(|(deadline, _)| *deadline)
                    .min()
                    .expect("collected first request");
                let count = collector.get_collected_request_count() as u32;
                let result = get_timestamps_with_retry(
                    &runtime,
                    &mut clients,
                    RpcControl {
                        forwarding: None,
                        follower: false,
                        timeout,
                        shutdown: &shutdown,
                    },
                    TsoBatchSpec {
                        deadline: batch_deadline,
                        count,
                    },
                    &state,
                    &mut tso_stream,
                    &last_timestamp,
                );
                collector.finish_collected_requests(
                    Some(&mut |index, (_, reply), _| {
                        let one = result.clone().and_then(|batch| {
                            batch.split(index as u32).compose().map_err(batch_error)
                        });
                        let _ = reply.send(one);
                    }),
                    None,
                );
            }
            WorkerCommand::ExternalTimestamp {
                deadline,
                value,
                reply,
            } => {
                let state = state.read().expect("PD state lock poisoned").clone();
                let remaining = deadline.saturating_duration_since(Instant::now());
                let result = if remaining.is_zero() {
                    Err(PdClientError::Timeout {
                        operation: PdOperation::ExternalTimestamp,
                        endpoint: state.members.leader_url.clone(),
                        timeout_ms: timeout.as_millis() as u64,
                    })
                } else {
                    super::requests::external_timestamp(
                        &runtime,
                        &mut clients,
                        &state.members.leader_url,
                        RpcControl {
                            forwarding: None,
                            follower: false,
                            timeout: remaining,
                            shutdown: &shutdown,
                        },
                        state.members.cluster_id,
                        value,
                    )
                };
                let _ = reply.send(result);
            }
            WorkerCommand::GetGcState { keyspace_id, reply } => {
                let result = get_gc_state_with_failover(
                    &runtime,
                    &mut clients,
                    timeout,
                    &state,
                    &shutdown,
                    keyspace_id,
                );
                let _ = reply.send(result);
            }
            WorkerCommand::StoreGlobalConfig {
                deadline,
                request,
                reply,
            } => {
                let endpoint = state
                    .read()
                    .expect("PD state lock poisoned")
                    .members
                    .leader_url
                    .clone();
                let remaining = deadline.saturating_duration_since(Instant::now());
                let result = if remaining.is_zero() {
                    Err(PdClientError::Timeout {
                        operation: PdOperation::StoreGlobalConfig,
                        endpoint: endpoint.clone(),
                        timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
                    })
                } else {
                    super::requests::store_global_config(
                        &runtime,
                        &mut clients,
                        &endpoint,
                        RpcControl {
                            forwarding: None,
                            follower: false,
                            timeout: remaining,
                            shutdown: &shutdown,
                        },
                        request,
                    )
                };
                let _ = reply.send(result);
            }
            WorkerCommand::Close { reply } => {
                discovery_worker.close();
                tso_stream.clear();
                clients.close();
                let _ = reply.send(());
                break;
            }
        }
    }
    discovery_worker.close();
    drop(tso_stream);
    clients.close();
}

pub(super) fn get_timestamps_with_retry(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    control: RpcControl<'_>,
    spec: TsoBatchSpec,
    state: &Arc<RwLock<PdSharedState>>,
    stream: &mut TsoStreamSet,
    last_timestamp: &TimestampTracker,
) -> Result<TsoBatch, PdClientError> {
    let TsoBatchSpec { deadline, count } = spec;
    // Go's dispatcher retries EVERY processRequest failure until the context
    // is done (dispatcher.go:356-398) -- there is no attempt cap; the batch
    // deadline is the natural bound.
    let mut attempt = 0usize;
    loop {
        let snapshot = state.read().expect("PD state lock poisoned").clone();
        let leader = snapshot.members.leader_url;
        let mut selected_endpoint = None;
        let result: Result<TsoBatch, PdClientError> = (|| {
            let routes = discover_tso(
                runtime,
                clients,
                &leader,
                &snapshot.members.member_urls,
                snapshot.members.cluster_id,
                deadline,
                control.shutdown,
                stream.is_empty(),
            )?;
            stream.retain_routes(&routes);
            let route = tikv_client::pd_service_discovery::pick_stream_route(&routes)
                .ok_or_else(|| PdClientError::Transport {
                    operation: PdOperation::Tso,
                    endpoint: leader.clone(),
                    code: "Unavailable".into(),
                    message: "no healthy timestamp service endpoint".into(),
                })?
                .clone();
            let endpoint = route.endpoint.clone();
            selected_endpoint = Some(endpoint.clone());
            let channel = {
                let _guard = runtime.enter();
                clients.channel(&endpoint)?
            };
            let before_request = last_timestamp.snapshot();
            let batch = request_batch(
                stream,
                runtime,
                channel,
                route,
                &clients.tso_forwarding,
                snapshot.members.cluster_id,
                deadline,
                control.shutdown,
                &clients.tso_routes,
                count,
            )?;
            last_timestamp
                .accept(batch, before_request)
                .map_err(batch_error)?;
            Ok(batch)
        })();

        match result {
            Ok(timestamp) => return Ok(timestamp),
            Err(error) => {
                // Validation failures also retire the selected stream.
                if let Some(endpoint) = selected_endpoint {
                    stream.remove(&endpoint);
                }
                // Go panics on a monotonicity violation
                // (dispatcher.go:522-536) -- it is terminal there and stays
                // terminal here (documented narrowing: error instead of
                // process death); retrying a regressed timestamp can never
                // fix it.
                if error.kind() == "tso_fallback" {
                    return Err(error);
                }
                // Go's dispatcher surfaces ctx.Err() when the deadline
                // expires (dispatcher.go:344-349): the caller sees the
                // deadline miss, not the underlying transport error.
                if Instant::now() >= deadline {
                    return Err(timeout_error(&leader.to_string(), Duration::ZERO));
                }
                attempt += 1;
            }
        }

        match refresh_membership_before_deadline(
            runtime,
            clients,
            control.timeout,
            deadline,
            state,
            control.shutdown,
        ) {
            Ok(_) => {}
            Err(error @ PdClientError::ClusterMismatch { .. }) => return Err(error),
            Err(error @ PdClientError::Timeout { .. }) => return Err(error),
            Err(_) => {}
        }

        let delay = retry_delay(attempt + 1);
        if !delay.is_zero() {
            let leader = state
                .read()
                .expect("PD state lock poisoned")
                .members
                .leader_url
                .clone();
            let remaining = remaining_tso_time(deadline, &leader)?;
            if wait_for_shutdown(runtime, control.shutdown, delay.min(remaining)) {
                return Err(PdClientError::Closed);
            }
        }
    }
}

pub(super) fn refresh_membership_before_deadline(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    timeout: Duration,
    deadline: Instant,
    state: &Arc<RwLock<PdSharedState>>,
    shutdown: &watch::Receiver<bool>,
) -> Result<PdMemberSet, PdClientError> {
    let snapshot = state.read().expect("PD state lock poisoned").clone();
    let mut last_error = None;
    let mut cluster_mismatch = None;
    for endpoint in endpoint_attempt_order(&snapshot) {
        let attempt_timeout = timeout.min(remaining_tso_time(deadline, &endpoint)?);
        match get_members(
            runtime,
            clients,
            &endpoint,
            attempt_timeout,
            shutdown,
            Some(snapshot.members.cluster_id),
        ) {
            Ok(observation) => match observation.projected {
                Ok(members) => {
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

pub(super) fn bootstrap_members(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    seeds: &[String],
    timeout: Duration,
    shutdown: &watch::Receiver<bool>,
) -> Result<PdMemberSet, PdClientError> {
    let mut cluster_id = None;
    for attempt in 1..=BOOTSTRAP_MAX_ATTEMPTS {
        let mut accepted = None;
        let mut last_error = None;
        for seed in seeds {
            match get_members(runtime, clients, seed, timeout, shutdown, cluster_id) {
                Ok(observation) => {
                    cluster_id = Some(observation.cluster_id);
                    match observation.projected {
                        Ok(members) => accepted = Some(members),
                        Err(error) => last_error = Some(error),
                    }
                }
                Err(error @ PdClientError::ClusterMismatch { .. }) => return Err(error),
                Err(error) => last_error = Some(error),
            }
        }
        if let Some(members) = accepted {
            return Ok(members);
        }
        let error = last_error
            .unwrap_or_else(|| invalid_topology("missing_pd_seed", "no PD seed was configured"));
        let retryable = matches!(
            error,
            PdClientError::HeaderError {
                operation: PdOperation::GetMembers,
                error_type,
                ref message,
            } if error_type == pdpb::ErrorType::NotBootstrapped as i32
                || (error_type == pdpb::ErrorType::Unknown as i32
                    && message.starts_with("[PD:server:ErrServerNotStarted]"))
        );
        if !retryable || attempt == BOOTSTRAP_MAX_ATTEMPTS {
            return Err(error);
        }
        if wait_for_shutdown(runtime, shutdown, BOOTSTRAP_RETRY_INTERVAL * attempt as u32) {
            return Err(PdClientError::Closed);
        }
    }
    unreachable!("bounded bootstrap loop always returns")
}

// Discovery uses the native owner; the synchronous adapter only supplies its
// TLS policy and cancellation/deadline budget. Metadata still targets PD.
fn discover_tso(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    leader: &str,
    members: &[String],
    cluster_id: u64,
    deadline: Instant,
    shutdown: &watch::Receiver<bool>,
    force: bool,
) -> Result<Vec<tikv_client::pd_service_discovery::TsoRoute>, PdClientError> {
    let timeout = remaining_tso_time(deadline, leader)?;
    let mut shutdown = shutdown.clone();
    runtime.block_on(async {
        tokio::select! {
            biased;
            () = super::shutdown_requested(&mut shutdown) => Err(PdClientError::Closed),
            result = tokio::time::timeout(timeout, refresh_tso(clients, leader, members, cluster_id, timeout, force)) => result.unwrap_or_else(|_| Err(timeout_error(leader, timeout))),
        }
    })
}

async fn refresh_tso(
    clients: &PdChannelCache,
    leader: &str,
    members: &[String],
    cluster_id: u64,
    timeout: Duration,
    force: bool,
) -> Result<Vec<tikv_client::pd_service_discovery::TsoRoute>, PdClientError> {
    // Only timestamp discovery serializes here. Metadata commands never wait
    // for the independent periodic probe or hold this guard.
    let mut shared = clients.tso_discovery.lock().await;
    let proxy = clients.options.get_enable_tso_follower_proxy();
    if !force {
        if let Some((route, checked, previous_leader, previous_proxy, previous_members)) =
            &shared.route
        {
            if previous_leader == leader
                && *previous_proxy == proxy
                && previous_members == members
                && checked.elapsed() < tikv_client::pd_service_discovery::UPDATE_INTERVAL
            {
                return Ok(route.clone());
            }
        }
    }
    let mut discovery = shared.discovery.clone();
    let (route, _) = discovery
        .discover(cluster_id, leader, clients.options.use_tso_server_proxy, timeout, &clients.channels, |endpoint| {
            let channel = clients.channel(&endpoint);
            async move { channel.map_err(|error| tonic::Status::unavailable(error.to_string())) }
        })
        .await
        .map_err(|error| PdClientError::Transport {
            operation: PdOperation::Tso,
            endpoint: leader.to_owned(),
            code: format!("{:?}", error.code()),
            message: error.message().to_owned(),
        })?;
    let routes = discovery
        .stream_routes(&route, members, proxy, clients.options.enable_forwarding, timeout, |endpoint| {
            let channel = clients.channel(&endpoint);
            async move { channel.map_err(|error| tonic::Status::unavailable(error.to_string())) }
        })
        .await
        .map_err(|error| PdClientError::Transport {
            operation: PdOperation::Tso,
            endpoint: leader.to_owned(),
            code: format!("{:?}", error.code()),
            message: error.message().to_owned(),
        })?
        .into_iter()
        .map(|(route, _)| route)
        .collect::<Vec<_>>();
    clients.tso_routes.send_if_modified(|current| {
        if *current == routes {
            return false;
        }
        *current = routes.clone();
        true
    });
    shared.discovery = discovery;
    shared.route = Some((
        routes.clone(),
        Instant::now(),
        leader.to_owned(),
        proxy,
        members.to_vec(),
    ));
    Ok(routes)
}

struct DiscoveryWorker {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    worker: Option<tokio::task::JoinHandle<()>>,
    runtime: tokio::runtime::Handle,
}

impl DiscoveryWorker {
    fn start(
        runtime: &tokio::runtime::Runtime,
        clients: &PdChannelCache,
        timeout: Duration,
        state: Arc<RwLock<PdSharedState>>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Self {
        let clients = clients.clone();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let worker = runtime.spawn(async move {
            let discover = async {
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(tikv_client::pd_service_discovery::UPDATE_INTERVAL) => {},
                        _ = async { clients.options.enable_tso_follower_proxy_ch.receiver.lock().await.recv().await } => {},
                    }
                    let snapshot = state.read().expect("PD state lock poisoned").clone();
                    let _ = refresh_tso(
                        &clients,
                        &snapshot.members.leader_url,
                        &snapshot.members.member_urls,
                        snapshot.members.cluster_id,
                        timeout,
                        true,
                    )
                    .await;
                }
            };
            let health = async {
                let period = tikv_client::pd_region_service::HEALTH_CHECK_INTERVAL;
                let mut ticks =
                    tokio::time::interval_at(tokio::time::Instant::now() + period, period);
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    ticks.tick().await;
                    clients
                        .regions
                        .check_health(timeout, |endpoint| {
                            let channel = clients
                                .channel(&endpoint)
                                .map_err(|error| tonic::Status::unavailable(error.to_string()));
                            async { channel }
                        })
                        .await;
                }
            };
            // Poll both maintenance owners independently. Dropping their joined
            // future cancels in-flight dialing/RPCs before this task completes.
            tokio::select! {
                biased;
                _ = &mut stopped => {},
                () = super::shutdown_requested(&mut shutdown) => {},
                _ = async { tokio::join!(discover, health); } => {},
            }
        });
        Self {
            stop: Some(stop),
            worker: Some(worker),
            runtime: runtime.handle().clone(),
        }
    }

    fn close(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take() {
            self.runtime
                .block_on(worker)
                .expect("PD discovery task panicked");
        }
    }
}

impl Drop for DiscoveryWorker {
    fn drop(&mut self) {
        self.close();
    }
}
