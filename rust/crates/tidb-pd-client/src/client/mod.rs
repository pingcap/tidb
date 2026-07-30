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

//! The PD client handle and the single worker thread every call goes through.
//!
//! Go boundary: the `pd/client` module's `client.go` — one client object,
//! cloneable request handles, a background goroutine owning the connections,
//! and an explicit `Close` that drains it. The phases that client.go keeps in
//! its own files keep their own module here:
//!
//! | module | subject | Go boundary |
//! | --- | --- | --- |
//! | [`worker`] | the thread that serializes every PD call and the TSO stream it retains | `client.go` goroutine loop, `tso_client.go` |
//! | [`requests`] | one PD gRPC method each, and the header contract they share | `client.go` request wrappers, `gc_client.go` |
//! | [`failover`] | which member serves a call, and when to move to another | `pd_service_discovery.go` |
//! | [`topology`] | projecting PD's answer into regions, stores, and members | `metapb`/`pdpb` projection at the call sites |

mod failover;
mod requests;
mod topology;
mod worker;

use std::future::Future;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tidb_proto::pdpb;
use tokio::sync::watch;

use crate::{
    ClusterSecurity, PdClientError, PdClientShutdownError, PdGcState, PdKeyRange, PdMemberSet,
    PdOperation, PdRegion, PdStore,
};

use failover::retain_member_clients;
use failover::PdChannelCache;
use topology::invalid_topology;

pub(crate) use topology::normalize_endpoints;
use worker::{bootstrap_members, run_worker};

pub use failover::is_unimplemented;

/// Exact method paths generated from the checked source projection.
pub const GET_MEMBERS_PATH: &str = "/pdpb.PD/GetMembers";
/// Exact key lookup method path.
pub const GET_REGION_PATH: &str = "/pdpb.PD/GetRegion";
/// Exact previous-region lookup method path.
pub const GET_PREV_REGION_PATH: &str = "/pdpb.PD/GetPrevRegion";
/// Exact region-by-ID method path.
pub const GET_REGION_BY_ID_PATH: &str = "/pdpb.PD/GetRegionByID";
/// Exact deprecated contiguous scan method path.
pub const SCAN_REGIONS_PATH: &str = "/pdpb.PD/ScanRegions";
/// Exact ordered batch scan method path.
pub const BATCH_SCAN_REGIONS_PATH: &str = "/pdpb.PD/BatchScanRegions";
/// Exact store lookup method path.
pub const GET_STORE_PATH: &str = "/pdpb.PD/GetStore";
/// Exact legacy PD timestamp-oracle stream method path.
pub const TSO_PATH: &str = "/pdpb.PD/Tso";
/// Exact GC-state lookup method path.
pub const GET_GC_STATE_PATH: &str = "/pdpb.PD/GetGCState";

enum WorkerCommand {
    RefreshMembers {
        reply: mpsc::Sender<Result<PdMemberSet, PdClientError>>,
    },
    GetRegion {
        encoded_key: Vec<u8>,
        need_buckets: bool,
        leader_only: bool,
        reply: mpsc::Sender<Result<PdRegion, PdClientError>>,
    },
    GetPrevRegion {
        encoded_key: Vec<u8>,
        need_buckets: bool,
        leader_only: bool,
        reply: mpsc::Sender<Result<PdRegion, PdClientError>>,
    },
    GetRegionById {
        region_id: u64,
        need_buckets: bool,
        leader_only: bool,
        reply: mpsc::Sender<Result<PdRegion, PdClientError>>,
    },
    ScanRegions {
        request: pdpb::ScanRegionsRequest,
        leader_only: bool,
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
    GetTimestamp {
        deadline: Instant,
        reply: mpsc::Sender<Result<u64, PdClientError>>,
    },
    GetGcState {
        keyspace_id: Option<u32>,
        reply: mpsc::Sender<Result<PdGcState, PdClientError>>,
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

#[derive(Clone, Copy)]
struct RpcControl<'a> {
    timeout: Duration,
    shutdown: &'a watch::Receiver<bool>,
}

struct PdClientShared {
    bootstrap_endpoint: String,
    timeout: Duration,
    cluster_id: u64,
    state: Arc<RwLock<PdSharedState>>,
    commands: mpsc::Sender<WorkerCommand>,
    shutdown: watch::Sender<bool>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

/// Cloneable synchronous foreground PD client backed by one shared Tokio worker.
pub struct PdClient {
    shared: Arc<PdClientShared>,
    owns_worker: bool,
}

impl Clone for PdClient {
    /// Creates a request-only handle without shutdown or join authority.
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            owns_worker: false,
        }
    }
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
        Self::connect_seeds_with_security(seeds, timeout, Arc::new(ClusterSecurity::plaintext()))
    }

    /// Connects through one or more seeds, securing every PD channel with the
    /// given cluster TLS material. Plaintext security keeps the backward-compatible
    /// `http://` behavior of [`Self::connect_seeds`].
    pub fn connect_seeds_with_security<I, S>(
        seeds: I,
        timeout: Duration,
        security: Arc<ClusterSecurity>,
    ) -> Result<Self, PdClientError>
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
        let (shutdown, shutdown_rx) = watch::channel(false);
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
            let mut clients = PdChannelCache::new(security);
            let members = match bootstrap_members(
                &runtime,
                &mut clients,
                &worker_seeds,
                timeout,
                &shutdown_rx,
            ) {
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
            run_worker(runtime, clients, receiver, timeout, state, shutdown_rx);
        });

        match ready_rx.recv() {
            Ok(Ok(state)) => {
                let cluster_id = state
                    .read()
                    .expect("PD state lock poisoned")
                    .members
                    .cluster_id;
                Ok(Self {
                    shared: Arc::new(PdClientShared {
                        bootstrap_endpoint,
                        timeout,
                        cluster_id,
                        state,
                        commands,
                        shutdown,
                        worker: Mutex::new(Some(worker)),
                    }),
                    owns_worker: true,
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
    pub fn cluster_id(&self) -> u64 {
        self.shared.cluster_id
    }

    /// Returns the first configured seed for backward-compatible diagnostics.
    ///
    /// This value is not the routing authority. Use [`Self::member_set`] and
    /// [`Self::active_endpoint`] to inspect current discovery state.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.shared.bootstrap_endpoint
    }

    /// Returns the latest validated membership snapshot.
    #[must_use]
    pub fn member_set(&self) -> PdMemberSet {
        self.shared
            .state
            .read()
            .expect("PD state lock poisoned")
            .members
            .clone()
    }

    /// Returns the endpoint that most recently completed a foreground action.
    #[must_use]
    pub fn active_endpoint(&self) -> String {
        self.shared
            .state
            .read()
            .expect("PD state lock poisoned")
            .active_endpoint
            .clone()
    }

    /// Returns the configured PD deadline.
    ///
    /// Unary control-plane operations apply it per attempt. TSO applies it to
    /// the complete bounded allocation, including membership refresh/retry.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.shared.timeout
    }

    /// Whether this value retains the unique PD worker lifecycle authority.
    #[must_use]
    pub const fn is_worker_owner(&self) -> bool {
        self.owns_worker
    }

    /// Consumes the unique owner, cancels foreground work, and joins the worker.
    ///
    /// Every request handle must be drained first. Dropping the owner invokes
    /// the same idempotent machinery only as a best-effort containment path;
    /// production lifecycle success must come from this explicit result.
    pub fn shutdown(mut self) -> Result<(), PdClientShutdownError> {
        self.shutdown_inner(true)
    }

    /// Refreshes membership through the first reachable known endpoint.
    pub fn refresh_members(&self) -> Result<PdMemberSet, PdClientError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(WorkerCommand::RefreshMembers { reply })
            .map_err(|_| PdClientError::Closed)?;
        response.recv().unwrap_or(Err(PdClientError::Closed))
    }

    /// Returns one current, strictly monotonic TiKV snapshot timestamp.
    ///
    /// The configured PD timeout is the total bound across stream creation,
    /// request/response I/O, membership refresh, and every retry.
    pub fn get_timestamp(&self) -> Result<u64, PdClientError> {
        let timeout = self.shared.timeout;
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            invalid_topology(
                "invalid_tso_deadline",
                format!("PD Tso timeout {timeout:?} exceeds Instant range"),
            )
        })?;
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(WorkerCommand::GetTimestamp { deadline, reply })
            .map_err(|_| PdClientError::Closed)?;
        match response.recv_timeout(timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(PdClientError::Closed),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(PdClientError::Timeout {
                operation: PdOperation::Tso,
                endpoint: self.member_set().leader_url,
                timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
            }),
        }
    }

    /// Loads the region containing one already encoded PD wire key.
    pub fn get_region(&self, encoded_key: &[u8]) -> Result<PdRegion, PdClientError> {
        self.get_region_with_buckets(encoded_key, true)
    }

    /// Loads a region while preserving the caller's exact bucket request flag.
    pub fn get_region_with_buckets(
        &self,
        encoded_key: &[u8],
        need_buckets: bool,
    ) -> Result<PdRegion, PdClientError> {
        self.get_region_routed(encoded_key, need_buckets, false)
    }

    /// Loads a region through either the active endpoint or the discovered PD
    /// leader. `leader_only` is per-attempt and never changes global routing.
    pub fn get_region_routed(
        &self,
        encoded_key: &[u8],
        need_buckets: bool,
        leader_only: bool,
    ) -> Result<PdRegion, PdClientError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(WorkerCommand::GetRegion {
                encoded_key: encoded_key.to_vec(),
                need_buckets,
                leader_only,
                reply,
            })
            .map_err(|_| PdClientError::Closed)?;
        response.recv().unwrap_or(Err(PdClientError::Closed))
    }

    /// Loads the region immediately before one already encoded PD wire key.
    pub fn get_prev_region(&self, encoded_key: &[u8]) -> Result<PdRegion, PdClientError> {
        self.get_prev_region_with_buckets(encoded_key, true)
    }

    /// Loads the previous region with the caller's exact bucket request flag.
    pub fn get_prev_region_with_buckets(
        &self,
        encoded_key: &[u8],
        need_buckets: bool,
    ) -> Result<PdRegion, PdClientError> {
        self.get_prev_region_routed(encoded_key, need_buckets, false)
    }

    /// Loads the previous region through the active endpoint or only the PD leader.
    pub fn get_prev_region_routed(
        &self,
        encoded_key: &[u8],
        need_buckets: bool,
        leader_only: bool,
    ) -> Result<PdRegion, PdClientError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(WorkerCommand::GetPrevRegion {
                encoded_key: encoded_key.to_vec(),
                need_buckets,
                leader_only,
                reply,
            })
            .map_err(|_| PdClientError::Closed)?;
        response.recv().unwrap_or(Err(PdClientError::Closed))
    }

    /// Loads one region identity with the exact bucket request flag.
    pub fn get_region_by_id(
        &self,
        region_id: u64,
        need_buckets: bool,
    ) -> Result<PdRegion, PdClientError> {
        self.get_region_by_id_routed(region_id, need_buckets, false)
    }

    /// Loads one region identity through the active endpoint or PD leader.
    pub fn get_region_by_id_routed(
        &self,
        region_id: u64,
        need_buckets: bool,
        leader_only: bool,
    ) -> Result<PdRegion, PdClientError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(WorkerCommand::GetRegionById {
                region_id,
                need_buckets,
                leader_only,
                reply,
            })
            .map_err(|_| PdClientError::Closed)?;
        response.recv().unwrap_or(Err(PdClientError::Closed))
    }

    /// Scans one contiguous encoded key interval through the pinned legacy RPC.
    pub fn scan_regions(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: i32,
    ) -> Result<Vec<PdRegion>, PdClientError> {
        self.scan_regions_routed(start_key, end_key, limit, false)
    }

    /// Scans one interval through the active endpoint or only the PD leader.
    pub fn scan_regions_routed(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: i32,
        leader_only: bool,
    ) -> Result<Vec<PdRegion>, PdClientError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(WorkerCommand::ScanRegions {
                request: pdpb::ScanRegionsRequest {
                    header: None,
                    start_key: start_key.to_vec(),
                    limit,
                    end_key: end_key.to_vec(),
                },
                leader_only,
                reply,
            })
            .map_err(|_| PdClientError::Closed)?;
        response.recv().unwrap_or(Err(PdClientError::Closed))
    }

    /// Batch-scans ordered encoded ranges with exact source request options.
    pub fn batch_scan_regions(
        &self,
        ranges: &[PdKeyRange],
        limit: i32,
        need_buckets: bool,
        contain_all_key_range: bool,
    ) -> Result<Vec<PdRegion>, PdClientError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
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
    pub fn get_store(&self, store_id: u64) -> Result<Option<PdStore>, PdClientError> {
        if store_id == 0 {
            return Err(invalid_topology(
                "zero_store_id",
                "requested store ID is zero",
            ));
        }
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(WorkerCommand::GetStore { store_id, reply })
            .map_err(|_| PdClientError::Closed)?;
        response.recv().unwrap_or(Err(PdClientError::Closed))
    }

    /// Loads PD's current GC state for one keyspace scope.
    ///
    /// `keyspace_id` is `None` for the null keyspace, which is the scope every
    /// non-keyspace deployment reads under. A PD older than the GC-state API
    /// answers `Unimplemented`; callers that must keep working against such a
    /// cluster fall back to the deprecated etcd txn-safe-point key rather than
    /// treating the failure as fatal.
    pub fn get_gc_state(&self, keyspace_id: Option<u32>) -> Result<PdGcState, PdClientError> {
        let (reply, response) = mpsc::channel();
        self.shared
            .commands
            .send(WorkerCommand::GetGcState { keyspace_id, reply })
            .map_err(|_| PdClientError::Closed)?;
        response.recv().unwrap_or(Err(PdClientError::Closed))
    }

    fn shutdown_inner(&mut self, require_unique: bool) -> Result<(), PdClientShutdownError> {
        if !self.owns_worker {
            return Err(PdClientShutdownError::NotOwner);
        }
        if require_unique {
            let owners = Arc::strong_count(&self.shared);
            if owners != 1 {
                return Err(PdClientShutdownError::SharedOwners { owners });
            }
        }

        let (worker, worker_state_poisoned) = match self.shared.worker.lock() {
            Ok(mut worker) => (worker.take(), false),
            Err(poisoned) => (poisoned.into_inner().take(), true),
        };
        let Some(worker) = worker else {
            return Ok(());
        };

        let mut failures = Vec::new();
        if worker_state_poisoned {
            failures.push(PdClientShutdownError::WorkerStatePoisoned);
        }

        let _ = self.shared.shutdown.send(true);
        let (reply, response) = mpsc::channel();
        if self
            .shared
            .commands
            .send(WorkerCommand::Close { reply })
            .is_err()
        {
            failures.push(PdClientShutdownError::CommandSend);
        } else if response.recv().is_err() {
            failures.push(PdClientShutdownError::MissingAcknowledgement);
        }
        if worker.join().is_err() {
            failures.push(PdClientShutdownError::WorkerPanicked);
        }
        PdClientShutdownError::from_failures(failures)
    }
}

impl Drop for PdClient {
    fn drop(&mut self) {
        if self.owns_worker {
            let _ = self.shutdown_inner(false);
        }
    }
}

enum RpcCompletion<T> {
    Completed(Result<tonic::Response<T>, tonic::Status>),
    Timeout,
    Shutdown,
}

fn block_on_rpc<F, T>(
    runtime: &tokio::runtime::Runtime,
    timeout: Duration,
    shutdown: &watch::Receiver<bool>,
    future: F,
) -> RpcCompletion<T>
where
    F: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
{
    if *shutdown.borrow() {
        return RpcCompletion::Shutdown;
    }
    let mut cancellation = shutdown.clone();
    runtime.block_on(async move {
        tokio::select! {
            biased;
            () = shutdown_requested(&mut cancellation) => RpcCompletion::Shutdown,
            result = tokio::time::timeout(timeout, future) => match result {
                Ok(result) => RpcCompletion::Completed(result),
                Err(_) => RpcCompletion::Timeout,
            },
        }
    })
}

fn wait_for_shutdown(
    runtime: &tokio::runtime::Runtime,
    shutdown: &watch::Receiver<bool>,
    duration: Duration,
) -> bool {
    if *shutdown.borrow() {
        return true;
    }
    let mut cancellation = shutdown.clone();
    runtime.block_on(async move {
        tokio::select! {
            biased;
            () = shutdown_requested(&mut cancellation) => true,
            () = tokio::time::sleep(duration) => false,
        }
    })
}

async fn shutdown_requested(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.changed().await;
}

#[cfg(test)]
mod worker_lifecycle_tests {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::mpsc;

    use super::{
        watch, Arc, Duration, JoinHandle, Mutex, PdClient, PdClientError, PdClientShared,
        PdClientShutdownError, PdMemberSet, PdSharedState, RwLock, WorkerCommand,
    };

    fn test_client(
        worker: impl FnOnce(mpsc::Receiver<WorkerCommand>, watch::Receiver<bool>) + Send + 'static,
    ) -> PdClient {
        let members = PdMemberSet {
            cluster_id: 42,
            leader_url: "http://127.0.0.1:2379".to_owned(),
            member_urls: vec!["http://127.0.0.1:2379".to_owned()],
        };
        let (commands, receiver) = mpsc::channel();
        let (shutdown, shutdown_rx) = watch::channel(false);
        let worker: JoinHandle<()> = std::thread::spawn(move || worker(receiver, shutdown_rx));
        PdClient {
            shared: Arc::new(PdClientShared {
                bootstrap_endpoint: "http://127.0.0.1:2379".to_owned(),
                timeout: Duration::from_secs(30),
                cluster_id: 42,
                state: Arc::new(RwLock::new(PdSharedState {
                    active_endpoint: members.leader_url.clone(),
                    members,
                })),
                commands,
                shutdown,
                worker: Mutex::new(Some(worker)),
            }),
            owns_worker: true,
        }
    }

    fn acknowledge_close(
        receiver: mpsc::Receiver<WorkerCommand>,
        _shutdown: watch::Receiver<bool>,
    ) {
        let WorkerCommand::Close { reply } = receiver.recv().expect("close command") else {
            panic!("fixture accepts only Close")
        };
        reply.send(()).expect("shutdown receiver");
    }

    #[test]
    fn clones_are_request_handles_and_explicit_shutdown_requires_drain() {
        let owner = test_client(acknowledge_close);
        assert!(owner.is_worker_owner());

        let non_owner = owner.clone();
        assert!(!non_owner.is_worker_owner());
        assert_eq!(non_owner.shutdown(), Err(PdClientShutdownError::NotOwner));

        let retained = owner.clone();
        assert_eq!(
            owner.shutdown(),
            Err(PdClientShutdownError::SharedOwners { owners: 2 })
        );
        assert_eq!(retained.refresh_members(), Err(PdClientError::Closed));
    }

    #[test]
    fn shutdown_reports_worker_panic_after_acknowledgement() {
        let owner = test_client(|receiver, _shutdown| {
            let WorkerCommand::Close { reply } = receiver.recv().expect("close command") else {
                panic!("fixture accepts only Close")
            };
            reply.send(()).expect("shutdown receiver");
            panic!("deterministic worker panic");
        });

        assert_eq!(owner.shutdown(), Err(PdClientShutdownError::WorkerPanicked));
    }

    #[test]
    fn shutdown_reports_missing_acknowledgement() {
        let owner = test_client(|receiver, _shutdown| {
            let WorkerCommand::Close { reply } = receiver.recv().expect("close command") else {
                panic!("fixture accepts only Close")
            };
            drop(reply);
        });

        assert_eq!(
            owner.shutdown(),
            Err(PdClientShutdownError::MissingAcknowledgement)
        );
    }

    #[test]
    fn shutdown_reports_closed_command_channel() {
        let (closed, observed) = mpsc::channel();
        let owner = test_client(move |receiver, _shutdown| {
            drop(receiver);
            closed.send(()).expect("test receiver");
        });
        observed.recv().expect("worker closed command channel");

        assert_eq!(owner.shutdown(), Err(PdClientShutdownError::CommandSend));
    }

    #[test]
    fn shutdown_recovers_poison_and_is_idempotent() {
        let mut owner = test_client(acknowledge_close);
        let shared = Arc::clone(&owner.shared);
        let poison = catch_unwind(AssertUnwindSafe(move || {
            let _worker = shared.worker.lock().expect("initial worker state");
            panic!("deterministic lifecycle-state poison");
        }));
        assert!(poison.is_err());

        assert_eq!(
            owner.shutdown_inner(true),
            Err(PdClientShutdownError::WorkerStatePoisoned)
        );
        assert_eq!(owner.shutdown_inner(true), Ok(()));
    }

    #[test]
    fn owner_drop_cancels_in_flight_request_before_join() {
        let (started, observed) = mpsc::channel();
        let owner = test_client(move |receiver, shutdown| {
            let WorkerCommand::RefreshMembers { reply } =
                receiver.recv().expect("foreground command")
            else {
                panic!("expected RefreshMembers")
            };
            started.send(()).expect("test receiver");
            while !*shutdown.borrow() {
                std::thread::sleep(Duration::from_millis(1));
            }
            reply
                .send(Err(PdClientError::Closed))
                .expect("foreground receiver");

            let WorkerCommand::Close { reply } = receiver.recv().expect("close command") else {
                panic!("expected Close")
            };
            reply.send(()).expect("shutdown receiver");
        });
        let request = owner.clone();
        let foreground = std::thread::spawn(move || request.refresh_members());
        observed.recv().expect("worker started foreground request");

        assert_eq!(
            owner.shutdown(),
            Err(PdClientShutdownError::SharedOwners { owners: 2 })
        );
        assert_eq!(
            foreground.join().expect("foreground thread"),
            Err(PdClientError::Closed)
        );
    }
}
