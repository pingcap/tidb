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

//! The one thread that owns PD's connections, and the loop that serves every
//! command a client handle sends it.
//!
//! Go boundary: `pd/client`'s `client.go` — the client goroutine that owns the
//! Tokio-equivalent runtime, bootstraps the member set before the first call,
//! and holds the retained TSO stream (`tso_client.go`) so a timestamp costs one
//! stream round trip rather than one connection.

use std::collections::VecDeque;
use std::sync::{mpsc, Arc, RwLock};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::tso::{
    is_retryable_tso_error, remaining as remaining_tso_time, retry_delay, RetainedTsoStream,
    TimestampParts, TsoBatch, MAX_TSO_RETRIES,
};

/// Upper bound on waiters merged into one PD Tso round trip.
///
/// Go boundary: `pd/client`'s `defaultMaxTSOBatchSize` in `tso_client.go`.
const MAX_TSO_BATCH_SIZE: usize = 10000;
use crate::{PdClientError, PdMemberSet};

use super::failover::{
    batch_scan_regions_with_failover, endpoint_attempt_order, foreground_leader_only,
    get_gc_state_with_failover, get_prev_region_with_failover, get_region_by_id_with_failover,
    get_region_with_failover, get_store_with_failover, refresh_membership, retain_member_clients,
    scan_regions_with_failover, tonic_client, PdChannelCache,
};
use super::requests::{get_members, get_prev_region, get_region, get_region_by_id, scan_regions};
use super::topology::invalid_topology;
use super::{wait_for_shutdown, PdSharedState, RpcControl, WorkerCommand};

pub(super) fn run_worker(
    runtime: tokio::runtime::Runtime,
    mut clients: PdChannelCache,
    receiver: mpsc::Receiver<WorkerCommand>,
    timeout: Duration,
    state: Arc<RwLock<PdSharedState>>,
    shutdown: watch::Receiver<bool>,
) {
    let mut tso_stream = None;
    let mut last_timestamp = None;
    // Non-TSO commands displaced while draining the channel for TSO waiters.
    let mut deferred: VecDeque<WorkerCommand> = VecDeque::new();
    loop {
        let command = match deferred.pop_front() {
            Some(command) => command,
            None => match receiver.recv() {
                Ok(command) => command,
                Err(_) => break,
            },
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
                WorkerCommand::GetTimestamp { reply, .. } => {
                    let _ = reply.send(Err(PdClientError::Closed));
                }
                WorkerCommand::GetGcState { reply, .. } => {
                    let _ = reply.send(Err(PdClientError::Closed));
                }
                WorkerCommand::Close { reply } => {
                    drop(tso_stream.take());
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
                    tso_stream = None;
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
                                runtime, clients, endpoint, timeout, &shutdown, cluster_id,
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
            WorkerCommand::BatchScanRegions { request, reply } => {
                let result = batch_scan_regions_with_failover(
                    &runtime,
                    &mut clients,
                    timeout,
                    &state,
                    &shutdown,
                    &request,
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
            WorkerCommand::GetTimestamp { deadline, reply } => {
                // Go boundary: `tso_dispatcher.go` -> `tsoBatchController`
                // collects every waiter already queued and serves them with a
                // single `count`-wide request.
                let mut waiters = vec![(deadline, reply)];
                let mut batch_deadline = deadline;
                while waiters.len() < MAX_TSO_BATCH_SIZE {
                    match receiver.try_recv() {
                        Ok(WorkerCommand::GetTimestamp { deadline, reply }) => {
                            batch_deadline = batch_deadline.min(deadline);
                            waiters.push((deadline, reply));
                        }
                        Ok(other) => {
                            deferred.push_back(other);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                let count = u32::try_from(waiters.len()).expect("TSO batch fits u32");
                let result = get_timestamps_with_retry(
                    &runtime,
                    &mut clients,
                    RpcControl {
                        timeout,
                        shutdown: &shutdown,
                    },
                    batch_deadline,
                    &state,
                    &mut tso_stream,
                    &mut last_timestamp,
                    count,
                );
                for (index, (_, reply)) in waiters.into_iter().enumerate() {
                    let index = u32::try_from(index).expect("TSO batch fits u32");
                    let one = result
                        .clone()
                        .and_then(|batch| batch.split(index).compose());
                    let _ = reply.send(one);
                }
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
            WorkerCommand::Close { reply } => {
                drop(tso_stream.take());
                let _ = reply.send(());
                break;
            }
        }
    }
}

pub(super) fn get_timestamps_with_retry(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    control: RpcControl<'_>,
    deadline: Instant,
    state: &Arc<RwLock<PdSharedState>>,
    stream: &mut Option<RetainedTsoStream>,
    last_timestamp: &mut Option<TimestampParts>,
    count: u32,
) -> Result<TsoBatch, PdClientError> {
    let mut last_error = None;
    for attempt in 0..MAX_TSO_RETRIES {
        let snapshot = state.read().expect("PD state lock poisoned").clone();
        let leader = snapshot.members.leader_url;
        if stream
            .as_ref()
            .is_some_and(|stream| stream.endpoint() != leader.as_str())
        {
            *stream = None;
        }

        let result = (|| {
            let batch = if stream.is_none() {
                let client = tonic_client(runtime, clients, &leader)?;
                let (opened, timestamp) = RetainedTsoStream::open_and_request(
                    runtime,
                    client,
                    &leader,
                    snapshot.members.cluster_id,
                    deadline,
                    control.shutdown,
                    count,
                )?;
                *stream = Some(opened);
                timestamp
            } else {
                stream
                    .as_mut()
                    .expect("TSO stream exists before retained request")
                    .request(
                        runtime,
                        snapshot.members.cluster_id,
                        deadline,
                        control.shutdown,
                        count,
                    )?
            };
            // The first timestamp of the batch must still advance past the
            // last one handed out; the rest advance by construction.
            batch.split(0).ensure_after(*last_timestamp)?;
            *last_timestamp = Some(batch.last());
            Ok(batch)
        })();

        match result {
            Ok(timestamp) => return Ok(timestamp),
            Err(error) => {
                *stream = None;
                if !is_retryable_tso_error(&error) || attempt + 1 == MAX_TSO_RETRIES {
                    return Err(error);
                }
                last_error = Some(error);
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

        let delay = retry_delay(attempt);
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
    Err(last_error.expect("bounded TSO retry loop records every failure"))
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

pub(super) fn bootstrap_members(
    runtime: &tokio::runtime::Runtime,
    clients: &mut PdChannelCache,
    seeds: &[String],
    timeout: Duration,
    shutdown: &watch::Receiver<bool>,
) -> Result<PdMemberSet, PdClientError> {
    let mut accepted = None;
    let mut cluster_id = None;
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
        Ok(members)
    } else {
        Err(last_error
            .unwrap_or_else(|| invalid_topology("missing_pd_seed", "no PD seed was configured")))
    }
}
