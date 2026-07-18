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
use std::collections::VecDeque;
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
const TABLE_START: &[u8] = b"t\x80\0\0\0\0\0\0*_r";
const SPLIT_KEY: &[u8] = b"t\x80\0\0\0\0\0\0*_r\x80\0\0\0\0\0\0\0";
const TABLE_END: &[u8] = b"t\x80\0\0\0\0\0\0+";
const TABLE_SCAN_DAG: &[u8] = &[0x12, 0x04, 0x12, 0x02, 0x08, 0x2a];

#[derive(Default)]
struct TransportTrace {
    addresses: Vec<String>,
    failures: Vec<(String, u64)>,
    liveness: Vec<(String, StoreLiveness)>,
}

struct RecordingClient {
    inner: TonicCoprocessorClient,
    trace: Rc<RefCell<TransportTrace>>,
}

struct SharedPrimedLoader {
    shared: Rc<RefCell<PdRegionLoader>>,
    primed: Rc<RefCell<VecDeque<RegionLocation>>>,
}

impl RegionLoader for SharedPrimedLoader {
    fn cluster_id(&self) -> u64 {
        self.shared.borrow().cluster_id()
    }

    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        let primed_index = self.primed.borrow().iter().position(|location| {
            location.start_key.as_slice() <= key
                && (location.end_key.is_empty() || key < location.end_key.as_slice())
        });
        if let Some(index) = primed_index {
            return Ok(self
                .primed
                .borrow_mut()
                .remove(index)
                .expect("primed index was observed under the same single-threaded owner"));
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
        self.trace.borrow_mut().addresses.push(address.to_owned());
        let result = self.inner.send_request(address, request, timeout);
        if let Err(error) = &result {
            if let Some(connection) = error.connection() {
                self.trace
                    .borrow_mut()
                    .failures
                    .push((connection.address().to_owned(), connection.version()));
            }
        }
        result
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
        let liveness = self.inner.liveness(address, timeout)?;
        self.trace
            .borrow_mut()
            .liveness
            .push((address.to_owned(), liveness));
        Ok(liveness)
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        self.inner.close()
    }
}

#[test]
#[ignore = "requires the cleanup-safe Campaign 12 three-PD/three-TiKV runner"]
fn one_lazy_response_recovers_after_its_cached_tikv_leader_stops() {
    let pd_seed = std::env::var("C12_PD_SEED")
        .expect("C12_PD_SEED must be supplied by run-campaign12-realtikv.sh");
    let phase_dir = PathBuf::from(
        std::env::var("C12_PHASE_DIR")
            .expect("C12_PHASE_DIR must be supplied by run-campaign12-realtikv.sh"),
    );
    assert!(phase_dir.is_dir(), "phase directory must already exist");

    let shared_loader = Rc::new(RefCell::new(
        PdRegionLoader::connect(pd_seed, Duration::from_secs(5))
            .expect("bootstrap live PD region loader"),
    ));
    let split_source = shared_loader
        .borrow_mut()
        .load_region(TABLE_START)
        .expect("discover the region which the runner will split");
    let mut split_source_phase = format!("region_id={}\n", split_source.region.id);
    for peer in &split_source.peers {
        split_source_phase.push_str(&format!("peer_store_id={}\n", peer.store_id));
    }
    write_phase(&phase_dir, "split-source", &split_source_phase);
    wait_for_phase(&phase_dir, "split-complete");

    let left_after_split = shared_loader
        .borrow_mut()
        .load_region(TABLE_START)
        .expect("discover left split region");
    let right_after_split = shared_loader
        .borrow_mut()
        .load_region(SPLIT_KEY)
        .expect("discover right split region");
    assert_ne!(
        left_after_split.region, right_after_split.region,
        "runner split must produce two exact cached regions"
    );
    let mut split_regions_phase = String::new();
    for location in [&left_after_split, &right_after_split] {
        split_regions_phase.push_str(&format!("region_id={}\n", location.region.id));
        for peer in &location.peers {
            split_regions_phase.push_str(&format!(
                "region_peer={}\t{}\n",
                location.region.id, peer.store_id
            ));
        }
    }
    write_phase(&phase_dir, "split-regions", &split_regions_phase);
    wait_for_phase(&phase_dir, "leaders-aligned");

    let left = shared_loader
        .borrow_mut()
        .load_region(TABLE_START)
        .expect("reload aligned left region");
    let right = shared_loader
        .borrow_mut()
        .load_region(SPLIT_KEY)
        .expect("reload aligned right region");
    assert_ne!(left.region, right.region);
    let left_leader = left
        .peers
        .iter()
        .find(|peer| Some(peer.id) == left.leader_peer_id)
        .expect("left region must have an aligned leader");
    let right_leader = right
        .peers
        .iter()
        .find(|peer| Some(peer.id) == right.leader_peer_id)
        .expect("right region must have an aligned leader");
    assert_eq!(
        left_leader.store_id, right_leader.store_id,
        "both bound tasks must initially reference one canonical store"
    );
    let leader_store = left
        .stores
        .iter()
        .find(|store| store.id == left_leader.store_id)
        .expect("aligned leader must have a Rust-discovered address");
    assert!(
        left.peers
            .iter()
            .any(|peer| peer.store_id != left_leader.store_id)
            && right
                .peers
                .iter()
                .any(|peer| peer.store_id != right_leader.store_id),
        "live proof requires an alternate voter"
    );
    let old_address = leader_store.address.clone();
    let primed = Rc::new(RefCell::new(VecDeque::from([left.clone(), right.clone()])));
    let cache = RegionCache::new(SharedPrimedLoader {
        shared: Rc::clone(&shared_loader),
        primed,
    });
    let trace = Rc::new(RefCell::new(TransportTrace::default()));
    let client = RecordingClient {
        inner: TonicCoprocessorClient::new().expect("construct live unary client"),
        trace: Rc::clone(&trace),
    };
    let transport = DirectUnaryQueryTransport::new(
        client,
        cache,
        DirectUnaryRuntimeConfig {
            default_timeout: Duration::from_secs(5),
            ..DirectUnaryRuntimeConfig::default()
        },
    )
    .expect("construct direct unary transport");
    let mut runtime = InjectedQueryRuntime::new(transport);
    let request = TransportRequest::new(KvRequestMetadata {
        request_type: RequestType::Dag,
        data: Some(TABLE_SCAN_DAG.to_vec()),
        key_ranges: Some(RequestKeyRanges::new_non_partitioned(vec![
            RequestKeyRange {
                start_key: TABLE_START.to_vec(),
                end_key: TABLE_END.to_vec(),
            },
        ])),
        keep_order: true,
        store_type: StoreType::TiKv,
        start_ts: 1,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        ..KvRequestMetadata::default()
    });
    let mut result = runtime
        .select_with_runtime_stats(
            &request,
            SelectInput::default(),
            QueryResultContext::new(Vec::new(), WarningCollector::new()),
            Vec::new(),
            0,
            false,
        )
        .expect("bind one lazy response while the cached leader is alive");
    assert!(
        trace.borrow().addresses.is_empty(),
        "binding must not dispatch before the runner stops the cached leader"
    );

    let mut ready = format!(
        "old_leader_store_id={}\nold_leader_address={}\n",
        left_leader.store_id, old_address
    );
    for location in [&left, &right] {
        ready.push_str(&format!("region_id={}\n", location.region.id));
        for store in &location.stores {
            ready.push_str(&format!("store_address={}\n", store.address));
        }
    }
    write_phase(&phase_dir, "route-ready", &ready);
    wait_for_phase(&phase_dir, "leader-stopped");

    let mut structured_results = 0;
    while result
        .next_raw()
        .expect("same response must recover and complete both region tasks")
        .is_some()
    {
        structured_results += 1;
    }
    assert_eq!(structured_results, 2, "one result per split region");

    let trace = trace.borrow();
    let failure = trace
        .failures
        .iter()
        .find(|(address, _)| address == &old_address)
        .expect("stopped leader must retain its exact failed generation");
    let survivors: Vec<_> = trace
        .addresses
        .iter()
        .filter(|address| *address != &old_address)
        .collect();
    assert!(
        survivors.len() >= 2,
        "both already-bound tasks must dispatch through surviving stores"
    );
    assert_eq!(
        trace
            .addresses
            .iter()
            .filter(|address| *address == &old_address)
            .count(),
        1,
        "a stale store generation must never receive a future dispatch"
    );
    let recovered_liveness = trace
        .liveness
        .iter()
        .find(|(address, _)| address == &old_address)
        .map(|(_, liveness)| *liveness)
        .expect("ordinary connection failure must run foreground liveness");
    assert_ne!(recovered_liveness, StoreLiveness::Reachable);
    write_phase(
        &phase_dir,
        "completed",
        &format!(
            "failed_address={}\nfailed_generation={}\nsurvivor_address={}\nsurvivor_dispatches={}\nstale_future_dispatches=0\nrecovered_store_liveness={:?}\nstructured_results={}\n",
            failure.0,
            failure.1,
            survivors[0],
            survivors.len(),
            recovered_liveness,
            structured_results
        ),
    );
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
