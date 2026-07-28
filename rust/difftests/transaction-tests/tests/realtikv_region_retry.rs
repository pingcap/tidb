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

#![allow(missing_docs)]

use std::cell::RefCell;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};

use tidb_distsql::{
    DirectUnaryClient, DirectUnaryClientError, DirectUnaryQueryTransport, DirectUnaryRequest,
    DirectUnaryResponse, DirectUnaryRuntimeConfig, InjectedQueryRuntime, KvRequestMetadata,
    QueryResultContext, RequestKeyRange, RequestKeyRanges, RequestType, SelectInput, StoreType,
    TransportRequest, WarningCollector,
};
use tidb_txnkv::region::{
    RegionCache, RegionLoadError, RegionLoader, RegionLocation, RegionMetadata,
    RegionRecoveryLoader, StoreLiveness,
};
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::PdRegionLoader;

const PHASE_TIMEOUT: Duration = Duration::from_secs(120);

struct RecordingClient {
    inner: TonicCoprocessorClient,
    addresses: Rc<RefCell<Vec<String>>>,
}

struct SharedPrimedLoader {
    shared: Rc<RefCell<PdRegionLoader>>,
    primed: Rc<RefCell<Option<RegionLocation>>>,
}

impl RegionLoader for SharedPrimedLoader {
    fn cluster_id(&self) -> u64 {
        self.shared.borrow().cluster_id()
    }

    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        if let Some(location) = self.primed.borrow_mut().take() {
            return Ok(location);
        }
        self.shared.borrow_mut().load_region(key)
    }
}

impl RegionRecoveryLoader for SharedPrimedLoader {
    fn hydrate_region(
        &mut self,
        metadata: &RegionMetadata,
        leader_store_id: u64,
    ) -> Result<RegionLocation, RegionLoadError> {
        self.shared
            .borrow_mut()
            .hydrate_region(metadata, leader_store_id)
    }
}

impl DirectUnaryClient for RecordingClient {
    fn send_request(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.addresses.borrow_mut().push(address.to_owned());
        self.inner.send_request(address, request, timeout)
    }

    fn send_request_with_context(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.addresses.borrow_mut().push(address.to_owned());
        self.inner.send_request_with_context(address, request, call)
    }

    fn close_address(&mut self, address: &str) -> Result<(), DirectUnaryClientError> {
        self.inner.close_address(address)
    }

    fn close_address_version(
        &mut self,
        address: &str,
        version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        self.inner.close_address_version(address, version)
    }

    fn liveness(
        &self,
        address: &str,
        timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        self.inner.liveness(address, timeout)
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        self.inner.close()
    }
}

impl tidb_distsql::LockRecoveryClient for RecordingClient {
    fn check_txn_status_for_lock(
        &mut self,
        address: &str,
        request: &tidb_proto::KvrpcCheckTxnStatusRequest,
        context: &tidb_proto::KvrpcContext,
        call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        self.inner.check_txn_status(address, request, context, call)
    }


    fn pessimistic_rollback_for_lock(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcPessimisticRollbackRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcPessimisticRollbackResponse, DirectUnaryClientError> {
        panic!("this realtikv test does not clean pessimistic locks")
    }
    fn resolve_lock_for_read(
        &mut self,
        address: &str,
        request: &tidb_proto::KvrpcResolveLockRequest,
        context: &tidb_proto::KvrpcContext,
        call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcResolveLockResponse, DirectUnaryClientError> {
        self.inner.resolve_lock(address, request, context, call)
    }
}

#[test]
#[ignore = "requires the cleanup-safe Campaign 11 three-PD/three-TiKV runner"]
fn same_process_survives_pd_removal_and_region_leader_transfer() {
    let pd_seed = std::env::var("REGION_RETRY_PD_SEED")
        .expect("REGION_RETRY_PD_SEED must be supplied by run-realtikv-region-retry.sh");
    let phase_dir = PathBuf::from(
        std::env::var("REGION_RETRY_PHASE_DIR")
            .expect("REGION_RETRY_PHASE_DIR must be supplied by run-realtikv-region-retry.sh"),
    );
    assert!(phase_dir.is_dir(), "phase directory must already exist");

    let shared_loader = Rc::new(RefCell::new(
        PdRegionLoader::connect(pd_seed, Duration::from_secs(5))
            .expect("bootstrap live PD region loader from the sole seed"),
    ));
    let primed = Rc::new(RefCell::new(None));
    let cache = RegionCache::new(SharedPrimedLoader {
        shared: Rc::clone(&shared_loader),
        primed: Rc::clone(&primed),
    });
    let members = shared_loader.borrow().member_set();
    assert_eq!(members.member_urls.len(), 3, "runner must expose three PDs");
    let mut member_phase = format!(
        "cluster_id={}\nleader_url={}\n",
        members.cluster_id, members.leader_url
    );
    for url in &members.member_urls {
        member_phase.push_str(&format!("member_url={url}\n"));
    }
    write_phase(&phase_dir, "members-ready", &member_phase);
    wait_for_phase(&phase_dir, "pd-removed");

    // This call must outlive loss of the bootstrap/leader endpoint using only
    // the member URLs learned above. No region, store, or TiKV address enters
    // the process through the environment or phase files.
    let discovered = shared_loader
        .borrow_mut()
        .load_region(&[])
        .expect("discover region through a surviving PD member");
    let active_pd = shared_loader.borrow().active_endpoint();
    assert_ne!(
        active_pd, members.leader_url,
        "removed PD must not remain active"
    );
    let old_leader_peer = discovered
        .peers
        .iter()
        .find(|peer| Some(peer.id) == discovered.leader_peer_id)
        .expect("cached region must have its discovered leader");
    let old_leader_store = discovered
        .stores
        .iter()
        .find(|store| store.id == old_leader_peer.store_id)
        .expect("cached leader must have its Rust-discovered store route");
    let mut route_phase = format!(
        "active_pd={active_pd}\nregion_id={}\nold_leader_store_id={}\nold_leader_address={}\n",
        discovered.region.id, old_leader_peer.store_id, old_leader_store.address
    );
    for peer in &discovered.peers {
        route_phase.push_str(&format!("peer_store_id={}\n", peer.store_id));
    }
    for store in &discovered.stores {
        route_phase.push_str(&format!("store_address={}\n", store.address));
        route_phase.push_str(&format!("store_route={}\t{}\n", store.id, store.address));
    }
    *primed.borrow_mut() = Some(discovered);
    write_phase(&phase_dir, "route-ready", &route_phase);
    wait_for_phase(&phase_dir, "region-moved");

    let addresses = Rc::new(RefCell::new(Vec::new()));
    let client = RecordingClient {
        inner: TonicCoprocessorClient::new().expect("construct live unary client"),
        addresses: Rc::clone(&addresses),
    };
    let transport = DirectUnaryQueryTransport::new_injected(
        client,
        cache,
        DirectUnaryRuntimeConfig {
            default_timeout: Duration::from_secs(5),
            ..DirectUnaryRuntimeConfig::default()
        },
        tidb_distsql::FixedTimestampSource::new(1 << 18),
    )
    .expect("construct response over the pre-existing primed cache");
    let mut runtime = InjectedQueryRuntime::new(transport);
    let metadata = KvRequestMetadata::from_request(tidb_txnkv::Request {
        request_type: RequestType::Dag,
        data: Some(Vec::new()),
        key_ranges: Some(RequestKeyRanges::new_non_partitioned(vec![
            RequestKeyRange {
                start_key: Vec::new().into(),
                end_key: Vec::new().into(),
            },
        ])),
        keep_order: true,
        store_type: StoreType::TiKv,
        start_ts: 1,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        ..tidb_txnkv::Request::default()
    });
    let mut result = runtime
        .select_with_runtime_stats(
            &TransportRequest::new(
                metadata,
                std::sync::Arc::new(tidb_distsql::CancelHandle::default()),
            ),
            SelectInput::default(),
            QueryResultContext::new(Vec::new(), WarningCollector::new()),
            Vec::new(),
            0,
            false,
        )
        .expect("bind query by consuming the primed old route after transfer");
    let error = result
        .next_raw()
        .expect_err("TiKV must reject the deliberately empty DAG after recovery");
    let message = error.to_string();
    assert!(
        message.contains("coprocessor other error"),
        "expected structured application response after retry, got: {message}"
    );
    assert!(
        !message.contains("unary client failed")
            && !message.contains("connection")
            && !message.contains("region loader"),
        "movement proof must cross both surviving control and data planes: {message}"
    );

    let addresses = addresses.borrow();
    assert!(
        addresses.len() >= 2,
        "leader movement must require a resend"
    );
    assert_ne!(
        addresses.first(),
        addresses.last(),
        "retry must select the transferred leader address"
    );
    let mut completed = format!("active_pd={active_pd}\n");
    for address in addresses.iter() {
        completed.push_str(&format!("tikv_address={address}\n"));
    }
    write_phase(&phase_dir, "completed", &completed);
}

fn write_phase(directory: &Path, name: &str, body: &str) {
    let temporary = directory.join(format!(".{name}.tmp"));
    fs::write(&temporary, body).expect("write temporary phase file");
    fs::rename(temporary, directory.join(name)).expect("publish phase file atomically");
}

fn wait_for_phase(directory: &Path, name: &str) {
    let path = directory.join(name);
    let deadline = Instant::now() + PHASE_TIMEOUT;
    while !path.is_file() {
        assert!(Instant::now() < deadline, "timed out waiting for {name}");
        thread::sleep(Duration::from_millis(100));
    }
}
