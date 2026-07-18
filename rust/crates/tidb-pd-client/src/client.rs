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

use std::collections::HashSet;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use tidb_proto::metapb;
use tidb_proto::pdpb::{self, pd_client::PdClient as TonicPdClient};
use tonic::transport::{Channel, Endpoint};

use crate::{
    PdClientError, PdNodeState, PdOperation, PdPeer, PdPeerRole, PdRegion, PdRegionEpoch, PdStore,
    PdStoreState,
};

/// Exact method paths generated from the checked source projection.
pub const GET_MEMBERS_PATH: &str = "/pdpb.PD/GetMembers";
/// Exact key lookup method path.
pub const GET_REGION_PATH: &str = "/pdpb.PD/GetRegion";
/// Exact store lookup method path.
pub const GET_STORE_PATH: &str = "/pdpb.PD/GetStore";

enum WorkerCommand {
    GetRegion {
        encoded_key: Vec<u8>,
        reply: mpsc::Sender<Result<PdRegion, PdClientError>>,
    },
    GetStore {
        store_id: u64,
        reply: mpsc::Sender<Result<Option<PdStore>, PdClientError>>,
    },
    Close {
        reply: mpsc::Sender<()>,
    },
}

/// Synchronous one-endpoint PD client backed by a dedicated Tokio worker.
pub struct PdClient {
    endpoint: String,
    timeout: Duration,
    cluster_id: u64,
    commands: Option<mpsc::Sender<WorkerCommand>>,
    worker: Option<JoinHandle<()>>,
}

impl PdClient {
    /// Connects to one plaintext endpoint and bootstraps a nonzero cluster ID.
    pub fn connect(endpoint: impl Into<String>, timeout: Duration) -> Result<Self, PdClientError> {
        let endpoint = endpoint.into();
        let uri = normalize_plaintext_endpoint(&endpoint)?;
        let parsed =
            Endpoint::from_shared(uri).map_err(|error| PdClientError::InvalidEndpoint {
                endpoint: endpoint.clone(),
                message: error.to_string(),
            })?;
        let (commands, receiver) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let worker_endpoint = endpoint.clone();
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
            let channel = {
                let _guard = runtime.enter();
                parsed.connect_lazy()
            };
            let mut client = TonicPdClient::new(channel);
            let cluster_id = match get_members(&runtime, &mut client, &worker_endpoint, timeout) {
                Ok(cluster_id) => cluster_id,
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                    return;
                }
            };
            if ready_tx.send(Ok(cluster_id)).is_err() {
                return;
            }
            run_worker(
                runtime,
                client,
                receiver,
                worker_endpoint,
                timeout,
                cluster_id,
            );
        });

        match ready_rx.recv() {
            Ok(Ok(cluster_id)) => Ok(Self {
                endpoint,
                timeout,
                cluster_id,
                commands: Some(commands),
                worker: Some(worker),
            }),
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

    /// Returns the sole configured endpoint.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Returns the one-attempt deadline applied independently to each call.
    #[must_use]
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Loads the region containing one already encoded PD wire key.
    pub fn get_region(&mut self, encoded_key: &[u8]) -> Result<PdRegion, PdClientError> {
        let Some(commands) = &self.commands else {
            return Err(PdClientError::Closed);
        };
        let (reply, response) = mpsc::channel();
        commands
            .send(WorkerCommand::GetRegion {
                encoded_key: encoded_key.to_vec(),
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
    mut client: TonicPdClient<Channel>,
    receiver: mpsc::Receiver<WorkerCommand>,
    endpoint: String,
    timeout: Duration,
    cluster_id: u64,
) {
    while let Ok(command) = receiver.recv() {
        match command {
            WorkerCommand::GetRegion { encoded_key, reply } => {
                let result = get_region(
                    &runtime,
                    &mut client,
                    &endpoint,
                    timeout,
                    cluster_id,
                    &encoded_key,
                );
                let _ = reply.send(result);
            }
            WorkerCommand::GetStore { store_id, reply } => {
                let result = get_store(
                    &runtime,
                    &mut client,
                    &endpoint,
                    timeout,
                    cluster_id,
                    store_id,
                );
                let _ = reply.send(result);
            }
            WorkerCommand::Close { reply } => {
                let _ = reply.send(());
                break;
            }
        }
    }
}

fn get_members(
    runtime: &tokio::runtime::Runtime,
    client: &mut TonicPdClient<Channel>,
    endpoint: &str,
    timeout: Duration,
) -> Result<u64, PdClientError> {
    let response = runtime.block_on(async {
        tokio::time::timeout(
            timeout,
            client.get_members(pdpb::GetMembersRequest { header: None }),
        )
        .await
    });
    let response = map_rpc_result(response, PdOperation::GetMembers, endpoint, timeout)?;
    let header = response
        .into_inner()
        .header
        .ok_or(PdClientError::MissingHeader(PdOperation::GetMembers))?;
    reject_header_error(PdOperation::GetMembers, &header)?;
    if header.cluster_id == 0 {
        return Err(PdClientError::ZeroClusterId);
    }
    Ok(header.cluster_id)
}

fn get_region(
    runtime: &tokio::runtime::Runtime,
    client: &mut TonicPdClient<Channel>,
    endpoint: &str,
    timeout: Duration,
    cluster_id: u64,
    encoded_key: &[u8],
) -> Result<PdRegion, PdClientError> {
    let response = runtime.block_on(async {
        tokio::time::timeout(
            timeout,
            client.get_region(pdpb::GetRegionRequest {
                header: Some(request_header(cluster_id)),
                region_key: encoded_key.to_vec(),
                need_buckets: true,
            }),
        )
        .await
    });
    let response =
        map_rpc_result(response, PdOperation::GetRegion, endpoint, timeout)?.into_inner();
    validate_response_header(PdOperation::GetRegion, response.header.as_ref(), cluster_id)?;
    project_region(response)
}

fn get_store(
    runtime: &tokio::runtime::Runtime,
    client: &mut TonicPdClient<Channel>,
    endpoint: &str,
    timeout: Duration,
    cluster_id: u64,
    store_id: u64,
) -> Result<Option<PdStore>, PdClientError> {
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
    validate_response_header(PdOperation::GetStore, response.header.as_ref(), cluster_id)?;
    project_store(response.store, store_id)
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

fn project_region(response: pdpb::GetRegionResponse) -> Result<PdRegion, PdClientError> {
    let region = response
        .region
        .ok_or_else(|| invalid_topology("missing_region", "GetRegion omitted region"))?;
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
    let returned_leader = project_peer(response.leader.ok_or_else(|| {
        invalid_topology(
            "missing_leader",
            format!("region {} omitted leader", region.id),
        )
    })?)?;
    let leader = peers
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
        })?;
    let down_peer_ids = response
        .down_peers
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
            Ok(peer.id)
        })
        .collect::<Result<Vec<_>, PdClientError>>()?;

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
        down_peer_ids,
    })
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
    let role = match metapb::PeerRole::try_from(peer.role) {
        Ok(metapb::PeerRole::Voter) => PdPeerRole::Voter,
        Ok(metapb::PeerRole::Learner) => PdPeerRole::Learner,
        Ok(metapb::PeerRole::IncomingVoter) => PdPeerRole::IncomingVoter,
        Ok(metapb::PeerRole::DemotingVoter) => PdPeerRole::DemotingVoter,
        Err(_) => {
            return Err(invalid_topology(
                "invalid_peer_role",
                format!("peer {} has role discriminant {}", peer.id, peer.role),
            ))
        }
    };
    Ok(PdPeer {
        id: peer.id,
        store_id: peer.store_id,
        role,
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
    Ok(if endpoint.starts_with("http://") {
        endpoint.to_owned()
    } else {
        format!("http://{endpoint}")
    })
}

fn invalid_topology(kind: &'static str, message: impl Into<String>) -> PdClientError {
    PdClientError::InvalidTopology {
        kind,
        message: message.into(),
    }
}
