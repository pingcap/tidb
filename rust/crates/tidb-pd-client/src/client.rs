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

use std::collections::{HashMap, HashSet};
use std::sync::{mpsc, Arc, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use tidb_proto::metapb;
use tidb_proto::pdpb::{self, pd_client::PdClient as TonicPdClient};
use tonic::transport::{Channel, Endpoint};

use crate::{
    PdBucketStats, PdBuckets, PdClientError, PdKeyRange, PdMemberSet, PdNodeState, PdOperation,
    PdPeer, PdRegion, PdRegionEpoch, PdStore, PdStoreState,
};

/// Exact method paths generated from the checked source projection.
pub const GET_MEMBERS_PATH: &str = "/pdpb.PD/GetMembers";
/// Exact key lookup method path.
pub const GET_REGION_PATH: &str = "/pdpb.PD/GetRegion";
/// Exact region-by-ID method path.
pub const GET_REGION_BY_ID_PATH: &str = "/pdpb.PD/GetRegionByID";
/// Exact deprecated contiguous scan method path.
pub const SCAN_REGIONS_PATH: &str = "/pdpb.PD/ScanRegions";
/// Exact ordered batch scan method path.
pub const BATCH_SCAN_REGIONS_PATH: &str = "/pdpb.PD/BatchScanRegions";
/// Exact store lookup method path.
pub const GET_STORE_PATH: &str = "/pdpb.PD/GetStore";

enum WorkerCommand {
    RefreshMembers {
        reply: mpsc::Sender<Result<PdMemberSet, PdClientError>>,
    },
    GetRegion {
        encoded_key: Vec<u8>,
        need_buckets: bool,
        reply: mpsc::Sender<Result<PdRegion, PdClientError>>,
    },
    GetRegionById {
        region_id: u64,
        need_buckets: bool,
        reply: mpsc::Sender<Result<PdRegion, PdClientError>>,
    },
    ScanRegions {
        request: pdpb::ScanRegionsRequest,
        reply: mpsc::Sender<Result<Vec<PdRegion>, PdClientError>>,
    },
    BatchScanRegions {
        request: pdpb::BatchScanRegionsRequest,
        reply: mpsc::Sender<Result<Vec<PdRegion>, PdClientError>>,
    },
    GetStore {
        store_id: u64,
        reply: mpsc::Sender<Result<Option<PdStore>, PdClientError>>,
    },
    Close {
        reply: mpsc::Sender<()>,
    },
}

#[derive(Clone, Debug)]
struct PdSharedState {
    members: PdMemberSet,
    active_endpoint: String,
}

struct PdMemberObservation {
    cluster_id: u64,
    projected: Result<PdMemberSet, PdClientError>,
}

/// Synchronous foreground PD client backed by a dedicated Tokio worker.
pub struct PdClient {
    bootstrap_endpoint: String,
    timeout: Duration,
    cluster_id: u64,
    state: Arc<RwLock<PdSharedState>>,
    commands: Option<mpsc::Sender<WorkerCommand>>,
    worker: Option<JoinHandle<()>>,
}

impl PdClient {
    /// Connects to one plaintext seed and discovers its PD membership.
    pub fn connect(endpoint: impl Into<String>, timeout: Duration) -> Result<Self, PdClientError> {
        Self::connect_seeds([endpoint.into()], timeout)
    }

    /// Connects through one or more plaintext seeds in caller-provided order.
    pub fn connect_seeds<I, S>(seeds: I, timeout: Duration) -> Result<Self, PdClientError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let raw_seeds = seeds.into_iter().map(Into::into).collect::<Vec<String>>();
        let bootstrap_endpoint = raw_seeds
            .first()
            .cloned()
            .ok_or_else(|| invalid_topology("missing_pd_seed", "no PD seed was configured"))?;
        let seeds = normalize_endpoints(raw_seeds, false)?;
        let (commands, receiver) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let worker_seeds = seeds.clone();
        let worker = std::thread::spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = ready_tx.send(Err(PdClientError::Runtime(error.to_string())));
                    return;
                }
            };
            let mut clients = HashMap::new();
            let members = match bootstrap_members(&runtime, &mut clients, &worker_seeds, timeout) {
                Ok(members) => members,
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                    return;
                }
            };
            let state = Arc::new(RwLock::new(PdSharedState {
                active_endpoint: members.leader_url.clone(),
                members,
            }));
            if ready_tx.send(Ok(Arc::clone(&state))).is_err() {
                return;
            }
            retain_member_clients(
                &mut clients,
                &state.read().expect("PD state lock poisoned").members,
            );
            run_worker(runtime, clients, receiver, timeout, state);
        });

        match ready_rx.recv() {
            Ok(Ok(state)) => {
                let cluster_id = state
                    .read()
                    .expect("PD state lock poisoned")
                    .members
                    .cluster_id;
                Ok(Self {
                    bootstrap_endpoint,
                    timeout,
                    cluster_id,
                    state,
                    commands: Some(commands),
                    worker: Some(worker),
                })
            }
            Ok(Err(error)) => {
                let _ = worker.join();
                Err(error)
            }
            Err(error) => {
                let _ = worker.join();
                Err(PdClientError::Runtime(error.to_string()))
            }
        }
    }

    /// Returns the cluster identity obtained from GetMembers.
    #[must_use]
    pub const fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Returns the first configured seed for backward-compatible diagnostics.
    ///
    /// This value is not the routing authority. Use [`Self::member_set`] and
    /// [`Self::active_endpoint`] to inspect current discovery state.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.bootstrap_endpoint
    }

    /// Returns the latest validated membership snapshot.
    #[must_use]
    pub fn member_set(&self) -> PdMemberSet {
        self.state
            .read()
            .expect("PD state lock poisoned")
            .members
            .clone()
    }

    /// Returns the endpoint that most recently completed a foreground action.
    #[must_use]
    pub fn active_endpoint(&self) -> String {
        self.state
            .read()
            .expect("PD state lock poisoned")
            .active_endpoint
            .clone()
    }

    /// Returns the one-attempt deadline applied independently to each call.
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Refreshes membership through the first reachable known endpoint.
    pub fn refresh_members(&mut self) -> Result<PdMemberSet, PdClientError> {
        let Some(commands) = &self.commands else {
            return Err(PdClientError::Closed);
        };
        let (reply, response) = mpsc::channel();
        commands
            .send(WorkerCommand::RefreshMembers { reply })
            .map_err(|_| PdClientError::Closed)?;
        response.recv().unwrap_or(Err(PdClientError::Closed))
    }

    /// Loads the region containing one already encoded PD wire key.
    pub fn get_region(&mut self, encoded_key: &[u8]) -> Result<PdRegion, PdClientError> {
        self.get_region_with_buckets(encoded_key, true)
    }

    /// Loads a region while preserving the caller's exact bucket request flag.
    pub fn get_region_with_buckets(
        &mut self,
        encoded_key: &[u8],
        need_buckets: bool,
    ) -> Result<PdRegion, PdClientError> {
        let Some(commands) = &self.commands else {
            return Err(PdClientError::Closed);
        };
        let (reply, response) = mpsc::channel();
        commands
            .send(WorkerCommand::GetRegion {
                encoded_key: encoded_key.to_vec(),
                need_buckets,
                reply,
            })
            .map_err(|_| PdClientError::Closed)?;
        response.recv().unwrap_or(Err(PdClientError::Closed))
    }

    /// Loads one region identity with the exact bucket request flag.
    pub fn get_region_by_id(
        &mut self,
        region_id: u64,
        need_buckets: bool,
    ) -> Result<PdRegion, PdClientError> {
        let Some(commands) = &self.commands else {
            return Err(PdClientError::Closed);
        };
        let (reply, response) = mpsc::channel();
        commands
            .send(WorkerCommand::GetRegionById {
                region_id,
                need_buckets,
                reply,
            })
            .map_err(|_| PdClientError::Closed)?;
        response.recv().unwrap_or(Err(PdClientError::Closed))
    }

    /// Scans one contiguous encoded key interval through the pinned legacy RPC.
    pub fn scan_regions(
        &mut self,
        start_key: &[u8],
        end_key: &[u8],
        limit: i32,
    ) -> Result<Vec<PdRegion>, PdClientError> {
        let Some(commands) = &self.commands else {
            return Err(PdClientError::Closed);
        };
        let (reply, response) = mpsc::channel();
        commands
            .send(WorkerCommand::ScanRegions {
                request: pdpb::ScanRegionsRequest {
                    header: None,
                    start_key: start_key.to_vec(),
                    limit,
                    end_key: end_key.to_vec(),
                },
                reply,
            })
            .map_err(|_| PdClientError::Closed)?;
        response.recv().unwrap_or(Err(PdClientError::Closed))
    }

    /// Batch-scans ordered encoded ranges with exact source request options.
    pub fn batch_scan_regions(
        &mut self,
        ranges: &[PdKeyRange],
        limit: i32,
        need_buckets: bool,
        contain_all_key_range: bool,
    ) -> Result<Vec<PdRegion>, PdClientError> {
        let Some(commands) = &self.commands else {
            return Err(PdClientError::Closed);
        };
        let (reply, response) = mpsc::channel();
        commands
            .send(WorkerCommand::BatchScanRegions {
                request: pdpb::BatchScanRegionsRequest {
                    header: None,
                    need_buckets,
                    ranges: ranges
                        .iter()
                        .map(|range| pdpb::KeyRange {
                            start_key: range.start_key.clone(),
                            end_key: range.end_key.clone(),
                        })
                        .collect(),
                    limit,
                    contain_all_key_range,
                },
                reply,
            })
            .map_err(|_| PdClientError::Closed)?;
        response.recv().unwrap_or(Err(PdClientError::Closed))
    }

    /// Loads a store. None means PD marked it tombstone or removed.
    pub fn get_store(&mut self, store_id: u64) -> Result<Option<PdStore>, PdClientError> {
        if store_id == 0 {
            return Err(invalid_topology(
                "zero_store_id",
                "requested store ID is zero",
            ));
        }
        let Some(commands) = &self.commands else {
            return Err(PdClientError::Closed);
        };
        let (reply, response) = mpsc::channel();
        commands
            .send(WorkerCommand::GetStore { store_id, reply })
            .map_err(|_| PdClientError::Closed)?;
        response.recv().unwrap_or(Err(PdClientError::Closed))
    }

    fn shutdown(&mut self) {
        if let Some(commands) = self.commands.take() {
            let (reply, response) = mpsc::channel();
            if commands.send(WorkerCommand::Close { reply }).is_ok() {
                let _ = response.recv();
            }
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for PdClient {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run_worker(
    runtime: tokio::runtime::Runtime,
    mut clients: HashMap<String, TonicPdClient<Channel>>,
    receiver: mpsc::Receiver<WorkerCommand>,
    timeout: Duration,
    state: Arc<RwLock<PdSharedState>>,
) {
    while let Ok(command) = receiver.recv() {
        match command {
            WorkerCommand::RefreshMembers { reply } => {
                let result = refresh_membership(&runtime, &mut clients, timeout, &state);
                let _ = reply.send(result);
            }
            WorkerCommand::GetRegion {
                encoded_key,
                need_buckets,
                reply,
            } => {
                let result = get_region_with_failover(
                    &runtime,
                    &mut clients,
                    timeout,
                    &state,
                    &encoded_key,
                    need_buckets,
                );
                let _ = reply.send(result);
            }
            WorkerCommand::GetRegionById {
                region_id,
                need_buckets,
                reply,
            } => {
                let result = get_region_by_id_with_failover(
                    &runtime,
                    &mut clients,
                    timeout,
                    &state,
                    region_id,
                    need_buckets,
                );
                let _ = reply.send(result);
            }
            WorkerCommand::ScanRegions { request, reply } => {
                let result =
                    scan_regions_with_failover(&runtime, &mut clients, timeout, &state, &request);
                let _ = reply.send(result);
            }
            WorkerCommand::BatchScanRegions { request, reply } => {
                let result = batch_scan_regions_with_failover(
                    &runtime,
                    &mut clients,
                    timeout,
                    &state,
                    &request,
                );
                let _ = reply.send(result);
            }
            WorkerCommand::GetStore { store_id, reply } => {
                let result =
                    get_store_with_failover(&runtime, &mut clients, timeout, &state, store_id);
                let _ = reply.send(result);
            }
            WorkerCommand::Close { reply } => {
                let _ = reply.send(());
                break;
            }
        }
    }
}

fn bootstrap_members(
    runtime: &tokio::runtime::Runtime,
    clients: &mut HashMap<String, TonicPdClient<Channel>>,
    seeds: &[String],
    timeout: Duration,
) -> Result<PdMemberSet, PdClientError> {
    let mut accepted = None;
    let mut cluster_id = None;
    let mut last_error = None;
    for seed in seeds {
        match get_members(runtime, clients, seed, timeout, cluster_id) {
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

fn get_members(
    runtime: &tokio::runtime::Runtime,
    clients: &mut HashMap<String, TonicPdClient<Channel>>,
    endpoint: &str,
    timeout: Duration,
    expected_cluster_id: Option<u64>,
) -> Result<PdMemberObservation, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let response = runtime.block_on(async {
        tokio::time::timeout(
            timeout,
            client.get_members(pdpb::GetMembersRequest { header: None }),
        )
        .await
    });
    let response = map_rpc_result(response, PdOperation::GetMembers, endpoint, timeout)?;
    let response = response.into_inner();
    let header = response
        .header
        .as_ref()
        .ok_or(PdClientError::MissingHeader(PdOperation::GetMembers))?;
    reject_header_error(PdOperation::GetMembers, header)?;
    if header.cluster_id == 0 {
        return Err(PdClientError::ZeroClusterId);
    }
    if let Some(expected) = expected_cluster_id {
        if header.cluster_id != expected {
            return Err(PdClientError::ClusterMismatch {
                operation: PdOperation::GetMembers,
                expected,
                actual: header.cluster_id,
            });
        }
    }
    let cluster_id = header.cluster_id;
    Ok(PdMemberObservation {
        cluster_id,
        projected: project_member_set(response),
    })
}

fn get_region(
    runtime: &tokio::runtime::Runtime,
    clients: &mut HashMap<String, TonicPdClient<Channel>>,
    endpoint: &str,
    timeout: Duration,
    cluster_id: u64,
    encoded_key: &[u8],
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let response = runtime.block_on(async {
        tokio::time::timeout(
            timeout,
            client.get_region(pdpb::GetRegionRequest {
                header: Some(request_header(cluster_id)),
                region_key: encoded_key.to_vec(),
                need_buckets,
            }),
        )
        .await
    });
    let response =
        map_rpc_result(response, PdOperation::GetRegion, endpoint, timeout)?.into_inner();
    validate_response_header(PdOperation::GetRegion, response.header.as_ref(), cluster_id)?;
    project_region(response, need_buckets)
}

fn get_region_by_id(
    runtime: &tokio::runtime::Runtime,
    clients: &mut HashMap<String, TonicPdClient<Channel>>,
    endpoint: &str,
    timeout: Duration,
    cluster_id: u64,
    region_id: u64,
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let response = runtime.block_on(async {
        tokio::time::timeout(
            timeout,
            client.get_region_by_id(pdpb::GetRegionByIdRequest {
                header: Some(request_header(cluster_id)),
                region_id,
                need_buckets,
            }),
        )
        .await
    });
    let response =
        map_rpc_result(response, PdOperation::GetRegionById, endpoint, timeout)?.into_inner();
    validate_response_header(
        PdOperation::GetRegionById,
        response.header.as_ref(),
        cluster_id,
    )?;
    project_region(response, need_buckets)
}

fn scan_regions(
    runtime: &tokio::runtime::Runtime,
    clients: &mut HashMap<String, TonicPdClient<Channel>>,
    endpoint: &str,
    timeout: Duration,
    cluster_id: u64,
    request: &pdpb::ScanRegionsRequest,
) -> Result<Vec<PdRegion>, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let mut request = request.clone();
    request.header = Some(request_header(cluster_id));
    let response = runtime
        .block_on(async { tokio::time::timeout(timeout, client.scan_regions(request)).await });
    let response =
        map_rpc_result(response, PdOperation::ScanRegions, endpoint, timeout)?.into_inner();
    validate_response_header(
        PdOperation::ScanRegions,
        response.header.as_ref(),
        cluster_id,
    )?;
    project_scan_regions(response)
}

fn batch_scan_regions(
    runtime: &tokio::runtime::Runtime,
    clients: &mut HashMap<String, TonicPdClient<Channel>>,
    endpoint: &str,
    timeout: Duration,
    cluster_id: u64,
    request: &pdpb::BatchScanRegionsRequest,
) -> Result<Vec<PdRegion>, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let need_buckets = request.need_buckets;
    let mut request = request.clone();
    request.header = Some(request_header(cluster_id));
    let response = runtime.block_on(async {
        tokio::time::timeout(timeout, client.batch_scan_regions(request)).await
    });
    let response =
        map_rpc_result(response, PdOperation::BatchScanRegions, endpoint, timeout)?.into_inner();
    validate_response_header(
        PdOperation::BatchScanRegions,
        response.header.as_ref(),
        cluster_id,
    )?;
    response
        .regions
        .into_iter()
        .map(|region| project_extended_region(region, need_buckets))
        .collect()
}

fn get_store(
    runtime: &tokio::runtime::Runtime,
    clients: &mut HashMap<String, TonicPdClient<Channel>>,
    endpoint: &str,
    timeout: Duration,
    cluster_id: u64,
    store_id: u64,
) -> Result<Option<PdStore>, PdClientError> {
    let client = tonic_client(runtime, clients, endpoint)?;
    let response = runtime.block_on(async {
        tokio::time::timeout(
            timeout,
            client.get_store(pdpb::GetStoreRequest {
                header: Some(request_header(cluster_id)),
                store_id,
            }),
        )
        .await
    });
    let response = map_rpc_result(response, PdOperation::GetStore, endpoint, timeout)?.into_inner();
    if store_is_removed(response.header.as_ref(), cluster_id)? {
        return Ok(None);
    }
    project_store(response.store, store_id)
}

fn get_region_with_failover(
    runtime: &tokio::runtime::Runtime,
    clients: &mut HashMap<String, TonicPdClient<Channel>>,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
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
        timeout,
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
                refresh_membership(runtime, clients, timeout, state)
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
                    timeout,
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

fn get_region_by_id_with_failover(
    runtime: &tokio::runtime::Runtime,
    clients: &mut HashMap<String, TonicPdClient<Channel>>,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    region_id: u64,
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    foreground_with_failover(
        runtime,
        clients,
        timeout,
        state,
        |runtime, clients, endpoint, cluster_id| {
            get_region_by_id(
                runtime,
                clients,
                endpoint,
                timeout,
                cluster_id,
                region_id,
                need_buckets,
            )
        },
    )
}

fn scan_regions_with_failover(
    runtime: &tokio::runtime::Runtime,
    clients: &mut HashMap<String, TonicPdClient<Channel>>,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    request: &pdpb::ScanRegionsRequest,
) -> Result<Vec<PdRegion>, PdClientError> {
    foreground_with_failover(
        runtime,
        clients,
        timeout,
        state,
        |runtime, clients, endpoint, cluster_id| {
            scan_regions(runtime, clients, endpoint, timeout, cluster_id, request)
        },
    )
}

fn batch_scan_regions_with_failover(
    runtime: &tokio::runtime::Runtime,
    clients: &mut HashMap<String, TonicPdClient<Channel>>,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    request: &pdpb::BatchScanRegionsRequest,
) -> Result<Vec<PdRegion>, PdClientError> {
    foreground_with_failover(
        runtime,
        clients,
        timeout,
        state,
        |runtime, clients, endpoint, cluster_id| {
            batch_scan_regions(runtime, clients, endpoint, timeout, cluster_id, request)
        },
    )
}

fn foreground_with_failover<T, F>(
    runtime: &tokio::runtime::Runtime,
    clients: &mut HashMap<String, TonicPdClient<Channel>>,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
    mut action: F,
) -> Result<T, PdClientError>
where
    F: FnMut(
        &tokio::runtime::Runtime,
        &mut HashMap<String, TonicPdClient<Channel>>,
        &str,
        u64,
    ) -> Result<T, PdClientError>,
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
                refresh_membership(runtime, clients, timeout, state)
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

fn get_store_with_failover(
    runtime: &tokio::runtime::Runtime,
    clients: &mut HashMap<String, TonicPdClient<Channel>>,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
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
                refresh_membership(runtime, clients, timeout, state)
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

fn refresh_membership(
    runtime: &tokio::runtime::Runtime,
    clients: &mut HashMap<String, TonicPdClient<Channel>>,
    timeout: Duration,
    state: &Arc<RwLock<PdSharedState>>,
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

fn endpoint_attempt_order(state: &PdSharedState) -> Vec<String> {
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

fn set_active_endpoint(state: &Arc<RwLock<PdSharedState>>, endpoint: String) {
    state
        .write()
        .expect("PD state lock poisoned")
        .active_endpoint = endpoint;
}

fn is_direct_failure(error: &PdClientError) -> bool {
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

fn needs_failover_probe(error: &PdClientError) -> bool {
    is_direct_failure(error)
        || matches!(
            error,
            PdClientError::Transport { .. } | PdClientError::HeaderError { .. }
        )
}

fn is_retryable_endpoint_error(
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

fn tonic_client<'a>(
    runtime: &tokio::runtime::Runtime,
    clients: &'a mut HashMap<String, TonicPdClient<Channel>>,
    endpoint: &str,
) -> Result<&'a mut TonicPdClient<Channel>, PdClientError> {
    match clients.entry(endpoint.to_owned()) {
        std::collections::hash_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
        std::collections::hash_map::Entry::Vacant(entry) => {
            let parsed = Endpoint::from_shared(endpoint.to_owned()).map_err(|error| {
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

fn retain_member_clients(
    clients: &mut HashMap<String, TonicPdClient<Channel>>,
    members: &PdMemberSet,
) {
    clients.retain(|endpoint, _| members.member_urls.contains(endpoint));
}

fn project_member_set(response: pdpb::GetMembersResponse) -> Result<PdMemberSet, PdClientError> {
    let cluster_id = response
        .header
        .as_ref()
        .expect("GetMembers header validated before projection")
        .cluster_id;
    let leader = response
        .leader
        .ok_or_else(|| invalid_topology("missing_pd_leader", "GetMembers omitted the PD leader"))?;
    let leader_url = leader
        .client_urls
        .first()
        .ok_or_else(|| {
            invalid_topology(
                "missing_pd_leader_url",
                format!("PD leader {} has no client URL", leader.member_id),
            )
        })
        .and_then(|url| normalize_plaintext_endpoint(url))?;
    let member_urls = normalize_endpoints(
        response
            .members
            .into_iter()
            .flat_map(|member| member.client_urls),
        true,
    )?;
    if member_urls.is_empty() {
        return Err(invalid_topology(
            "missing_pd_member_url",
            "GetMembers returned no member client URL",
        ));
    }
    if !member_urls.contains(&leader_url) {
        return Err(invalid_topology(
            "leader_not_in_members",
            format!("PD leader URL {leader_url} is absent from member URLs"),
        ));
    }
    Ok(PdMemberSet {
        cluster_id,
        leader_url,
        member_urls,
    })
}

fn store_is_removed(
    header: Option<&pdpb::ResponseHeader>,
    cluster_id: u64,
) -> Result<bool, PdClientError> {
    let header = header.ok_or(PdClientError::MissingHeader(PdOperation::GetStore))?;
    if header.cluster_id != cluster_id {
        return Err(PdClientError::ClusterMismatch {
            operation: PdOperation::GetStore,
            expected: cluster_id,
            actual: header.cluster_id,
        });
    }
    if let Some(error) = &header.error {
        let store_not_found = error.r#type == pdpb::ErrorType::StoreTombstone as i32
            || (error.message.contains("invalid store ID") && error.message.contains("not found"));
        if store_not_found {
            return Ok(true);
        }
        reject_header_error(PdOperation::GetStore, header)?;
    }
    Ok(false)
}

fn map_rpc_result<T>(
    result: Result<Result<tonic::Response<T>, tonic::Status>, tokio::time::error::Elapsed>,
    operation: PdOperation,
    endpoint: &str,
    timeout: Duration,
) -> Result<tonic::Response<T>, PdClientError> {
    match result {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(status)) if status.code() == tonic::Code::DeadlineExceeded => {
            Err(timeout_error(operation, endpoint, timeout))
        }
        Ok(Err(status)) => Err(PdClientError::Transport {
            operation,
            endpoint: endpoint.to_owned(),
            code: format!("{:?}", status.code()),
            message: status.message().to_owned(),
        }),
        Err(_) => Err(timeout_error(operation, endpoint, timeout)),
    }
}

fn timeout_error(operation: PdOperation, endpoint: &str, timeout: Duration) -> PdClientError {
    PdClientError::Timeout {
        operation,
        endpoint: endpoint.to_owned(),
        timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
    }
}

fn request_header(cluster_id: u64) -> pdpb::RequestHeader {
    pdpb::RequestHeader {
        cluster_id,
        sender_id: 0,
        caller_id: String::new(),
        caller_component: "codec-pd-client".to_owned(),
    }
}

fn validate_response_header(
    operation: PdOperation,
    header: Option<&pdpb::ResponseHeader>,
    cluster_id: u64,
) -> Result<(), PdClientError> {
    let header = header.ok_or(PdClientError::MissingHeader(operation))?;
    reject_header_error(operation, header)?;
    if header.cluster_id != cluster_id {
        return Err(PdClientError::ClusterMismatch {
            operation,
            expected: cluster_id,
            actual: header.cluster_id,
        });
    }
    Ok(())
}

fn reject_header_error(
    operation: PdOperation,
    header: &pdpb::ResponseHeader,
) -> Result<(), PdClientError> {
    if let Some(error) = &header.error {
        return Err(PdClientError::HeaderError {
            operation,
            error_type: error.r#type,
            message: error.message.clone(),
        });
    }
    Ok(())
}

fn project_region(
    response: pdpb::GetRegionResponse,
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    let region = response
        .region
        .ok_or_else(|| invalid_topology("missing_region", "GetRegion omitted region"))?;
    project_region_parts(
        region,
        response.leader,
        response.down_peers,
        response.pending_peers,
        response.buckets,
        need_buckets,
    )
}

fn project_extended_region(
    region: pdpb::Region,
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    let metadata = region
        .region
        .ok_or_else(|| invalid_topology("missing_region", "scan result omitted region"))?;
    project_region_parts(
        metadata,
        region.leader,
        region.down_peers,
        region.pending_peers,
        region.buckets,
        need_buckets,
    )
}

fn project_scan_regions(
    response: pdpb::ScanRegionsResponse,
) -> Result<Vec<PdRegion>, PdClientError> {
    if !response.regions.is_empty() {
        return response
            .regions
            .into_iter()
            .map(|region| project_extended_region(region, false))
            .collect();
    }

    let leaders = response.leaders;
    response
        .region_metas
        .into_iter()
        .enumerate()
        .map(|(index, region)| {
            project_region_parts(
                region,
                leaders.get(index).cloned(),
                Vec::new(),
                Vec::new(),
                None,
                false,
            )
        })
        .collect()
}

fn project_region_parts(
    region: metapb::Region,
    leader: Option<metapb::Peer>,
    down_peer_stats: Vec<pdpb::PeerStats>,
    pending_peer_metadata: Vec<metapb::Peer>,
    buckets: Option<metapb::Buckets>,
    need_buckets: bool,
) -> Result<PdRegion, PdClientError> {
    if region.id == 0 {
        return Err(invalid_topology("zero_region_id", "region ID is zero"));
    }
    let epoch = region.region_epoch.ok_or_else(|| {
        invalid_topology(
            "missing_region_epoch",
            format!("region {} omitted epoch", region.id),
        )
    })?;
    if region.peers.is_empty() {
        return Err(invalid_topology(
            "missing_peers",
            format!("region {} has no peers", region.id),
        ));
    }
    let peers = region
        .peers
        .into_iter()
        .map(project_peer)
        .collect::<Result<Vec<_>, _>>()?;
    let mut identities = HashSet::with_capacity(peers.len());
    if peers.iter().any(|peer| !identities.insert(peer.id)) {
        return Err(invalid_topology(
            "duplicate_peer_id",
            format!("region {} repeats a peer ID", region.id),
        ));
    }
    let leader = match leader {
        None => None,
        Some(leader) if leader.id == 0 => None,
        Some(leader) => {
            let returned_leader = project_peer(leader)?;
            Some(
                peers
                    .iter()
                    .find(|peer| same_peer_identity(peer, &returned_leader))
                    .cloned()
                    .ok_or_else(|| {
                        invalid_topology(
                            "leader_not_in_peers",
                            format!(
                                "region {} leader {} is not a region peer",
                                region.id, returned_leader.id
                            ),
                        )
                    })?,
            )
        }
    };
    let down_peers = down_peer_stats
        .into_iter()
        .map(|stats| {
            let peer = stats.peer.ok_or_else(|| {
                invalid_topology("missing_down_peer", "down peer stats omitted peer")
            })?;
            let peer = project_peer(peer)?;
            if !peers
                .iter()
                .any(|candidate| same_peer_identity(candidate, &peer))
            {
                return Err(invalid_topology(
                    "down_peer_not_in_peers",
                    format!("down peer {} is not an exact region peer", peer.id),
                ));
            }
            Ok(peer)
        })
        .collect::<Result<Vec<_>, PdClientError>>()?;
    let pending_peers = pending_peer_metadata
        .into_iter()
        .map(project_peer)
        .collect::<Result<Vec<_>, PdClientError>>()?;
    // PD can return batch-wide bucket metadata for a request that did not ask
    // for it. Enforce the per-request contract at the shared projection point.
    let buckets = buckets.filter(|_| need_buckets).map(project_buckets);

    Ok(PdRegion {
        id: region.id,
        start_key: region.start_key,
        end_key: region.end_key,
        epoch: PdRegionEpoch {
            conf_ver: epoch.conf_ver,
            version: epoch.version,
        },
        peers,
        leader,
        down_peers,
        pending_peers,
        buckets,
    })
}

fn project_buckets(buckets: metapb::Buckets) -> PdBuckets {
    PdBuckets {
        region_id: buckets.region_id,
        version: buckets.version,
        keys: buckets.keys,
        stats: buckets.stats.map(|stats| PdBucketStats {
            read_bytes: stats.read_bytes,
            write_bytes: stats.write_bytes,
            read_qps: stats.read_qps,
            write_qps: stats.write_qps,
            read_keys: stats.read_keys,
            write_keys: stats.write_keys,
        }),
        period_in_ms: buckets.period_in_ms,
    }
}

fn project_peer(peer: metapb::Peer) -> Result<PdPeer, PdClientError> {
    if peer.id == 0 {
        return Err(invalid_topology("zero_peer_id", "peer ID is zero"));
    }
    if peer.store_id == 0 {
        return Err(invalid_topology(
            "zero_peer_store_id",
            format!("peer {} references store zero", peer.id),
        ));
    }
    Ok(PdPeer {
        id: peer.id,
        store_id: peer.store_id,
        role: peer.role,
        is_witness: peer.is_witness,
    })
}

const fn same_peer_identity(left: &PdPeer, right: &PdPeer) -> bool {
    left.id == right.id && left.store_id == right.store_id
}

fn project_store(
    store: Option<metapb::Store>,
    requested_id: u64,
) -> Result<Option<PdStore>, PdClientError> {
    let store = store.ok_or_else(|| {
        invalid_topology(
            "missing_store",
            format!("GetStore({requested_id}) omitted store"),
        )
    })?;
    if store.id == 0 {
        return Err(invalid_topology("zero_store_id", "store ID is zero"));
    }
    if store.id != requested_id {
        return Err(invalid_topology(
            "store_id_mismatch",
            format!("requested store {requested_id}, received {}", store.id),
        ));
    }
    let state = match metapb::StoreState::try_from(store.state) {
        Ok(metapb::StoreState::Up) => PdStoreState::Up,
        Ok(metapb::StoreState::Offline) => PdStoreState::Offline,
        Ok(metapb::StoreState::Tombstone) => return Ok(None),
        Err(_) => {
            return Err(invalid_topology(
                "invalid_store_state",
                format!("store {} has state discriminant {}", store.id, store.state),
            ))
        }
    };
    let node_state = match metapb::NodeState::try_from(store.node_state) {
        Ok(metapb::NodeState::Preparing) => PdNodeState::Preparing,
        Ok(metapb::NodeState::Serving) => PdNodeState::Serving,
        Ok(metapb::NodeState::Removing) => PdNodeState::Removing,
        Ok(metapb::NodeState::Removed) => return Ok(None),
        Err(_) => {
            return Err(invalid_topology(
                "invalid_node_state",
                format!(
                    "store {} has node-state discriminant {}",
                    store.id, store.node_state
                ),
            ))
        }
    };
    if store.address.is_empty() {
        return Err(invalid_topology(
            "empty_store_address",
            format!("store {} has an empty client address", store.id),
        ));
    }
    let address_uri = normalize_plaintext_endpoint(&store.address).map_err(|error| {
        invalid_topology(
            "invalid_store_address",
            format!("store {}: {error}", store.id),
        )
    })?;
    Endpoint::from_shared(address_uri).map_err(|error| {
        invalid_topology(
            "invalid_store_address",
            format!("store {}: {error}", store.id),
        )
    })?;
    Ok(Some(PdStore {
        id: store.id,
        address: store.address,
        state,
        node_state,
        labels: store
            .labels
            .into_iter()
            .map(|label| (label.key, label.value))
            .collect(),
    }))
}

fn normalize_plaintext_endpoint(endpoint: &str) -> Result<String, PdClientError> {
    if endpoint.is_empty() {
        return Err(PdClientError::InvalidEndpoint {
            endpoint: endpoint.to_owned(),
            message: "endpoint is empty".to_owned(),
        });
    }
    if endpoint.starts_with("https://") {
        return Err(PdClientError::InvalidEndpoint {
            endpoint: endpoint.to_owned(),
            message: "TLS endpoints are outside this bounded client".to_owned(),
        });
    }
    if endpoint.contains("://") && !endpoint.starts_with("http://") {
        return Err(PdClientError::InvalidEndpoint {
            endpoint: endpoint.to_owned(),
            message: "only plaintext http endpoints are supported".to_owned(),
        });
    }
    let normalized = if endpoint.starts_with("http://") {
        endpoint.to_owned()
    } else {
        format!("http://{endpoint}")
    };
    Endpoint::from_shared(normalized.clone()).map_err(|error| PdClientError::InvalidEndpoint {
        endpoint: endpoint.to_owned(),
        message: error.to_string(),
    })?;
    Ok(normalized)
}

fn normalize_endpoints<I, S>(endpoints: I, sort: bool) -> Result<Vec<String>, PdClientError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut normalized = Vec::new();
    let mut seen = HashSet::new();
    for endpoint in endpoints {
        let endpoint = normalize_plaintext_endpoint(endpoint.as_ref())?;
        if seen.insert(endpoint.clone()) {
            normalized.push(endpoint);
        }
    }
    if sort {
        normalized.sort();
    }
    Ok(normalized)
}

fn invalid_topology(kind: &'static str, message: impl Into<String>) -> PdClientError {
    PdClientError::InvalidTopology {
        kind,
        message: message.into(),
    }
}
