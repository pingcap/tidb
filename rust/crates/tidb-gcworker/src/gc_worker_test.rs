// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/store/gcworker/gc_worker_test.go`. As Go's `createGCWorkerSuite`
//! does, the worker runs on the embedded store (`tidb-unistore`, three
//! stores) through client-rust: its PD answers the GC-state calls, and a
//! hijacking client over the store's own (Go's `mockGCWorkerClient`)
//! records `UnsafeDestroyRange`, `DeleteRange` and `ScanLock`. `mysql.tidb`
//! and the delete-range tables live in a fake session; the store's oracle
//! is client-go's `MockOracle`, which tests move forward.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tidb_ddl::ddl_delete_range::DelRangeTask;
use tidb_unistore::cluster::{bootstrap_with_multi_stores, Cluster};
use tidb_unistore::mock::StorePdClient;
use tidb_unistore::mock_pd::{AdvanceTxnSafePointResult as PdAdvanceTxnSafePointResult, GcState};
use tikv_client::oracle::oracles::MockOracle;
use tikv_client::proto::keyspacepb::{keyspace_meta, KeyspaceMeta};
use tikv_client::proto::{kvrpcpb, metapb};
use tikv_client::tikv::{KvStore, MockSafePointKv};

use crate::tikv_store::TikvStoreClient;
use tikv_client::transaction::gc::{
    new_gc_resolve_lock_max_backoff, resolve_locks_for_range, BaseRegionLockResolver, KeyLocation,
    RegionLockResolver,
};

use crate::gc_worker::*;
use crate::seams::*;
use tikv_client::pd_keyspace::{
    KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE, KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE_KEYSPACE_LEVEL,
    KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE_UNIFIED,
};

fn keyspace_id(meta: &KeyspaceMeta) -> u32 {
    match meta.keyspace {
        Some(keyspace_meta::Keyspace::Id(id)) => id,
        _ => 0,
    }
}

type DestroyHandler = Box<dyn Fn(&str, &[u8], &[u8]) -> GcResult<Option<String>> + Send + Sync>;
/// One sent `UnsafeDestroyRange`: (address, start, end).
type DestroyRequest = (String, Vec<u8>, Vec<u8>);
/// One scan-lock request: (start, end, txn safe point).
type ScanLockRequest = (Vec<u8>, Vec<u8>, u64);
/// The failing (start key, store address) of `test_delete_ranges_failure`.
type FailingRequest = Arc<Mutex<Option<(Vec<u8>, String)>>>;

/// Go `mockGCWorkerClient`: the store's own TiKV client, with handlers that
/// record `UnsafeDestroyRange` (and may answer it) and `DeleteRange`; the
/// `ScanLock` requests it records come from the RPC client's send hook.
struct FakeKv {
    inner: Arc<TikvStoreClient<StorePdClient>>,
    destroy_handler: Mutex<Option<DestroyHandler>>,
    destroy_requests: Mutex<Vec<DestroyRequest>>,
    delete_range_requests: Mutex<Vec<(Vec<u8>, Vec<u8>)>>,
    /// One `(start, end, txn safe point)` per scan-lock request, as Go's
    /// `scanLockRequestHandler` collects them.
    scan_lock_requests: Arc<Mutex<Vec<ScanLockRequest>>>,
}

impl GcKvClient for FakeKv {
    fn unsafe_destroy_range(
        &self,
        address: &str,
        start: &[u8],
        end: &[u8],
        timeout: Duration,
    ) -> GcResult<Option<String>> {
        self.destroy_requests.lock().unwrap().push((
            address.to_owned(),
            start.to_vec(),
            end.to_vec(),
        ));
        match &*self.destroy_handler.lock().unwrap() {
            Some(handler) => handler(address, start, end),
            None => self
                .inner
                .unsafe_destroy_range(address, start, end, timeout),
        }
    }

    fn delete_range_task(&self, start: &[u8], end: &[u8], concurrency: usize) -> GcResult<()> {
        self.delete_range_requests
            .lock()
            .unwrap()
            .push((start.to_vec(), end.to_vec()));
        self.inner.delete_range_task(start, end, concurrency)
    }

    fn resolve_locks_runner(
        &self,
        name: &str,
        concurrency: usize,
        max_version: u64,
        scan_limit: u32,
    ) -> Box<dyn ResolveLocksRunner + '_> {
        self.inner
            .resolve_locks_runner(name, concurrency, max_version, scan_limit)
    }

    fn gc_region(
        &self,
        region: &tikv_client::RegionVerId,
        safe_point: u64,
    ) -> GcResult<Option<tikv_client::proto::errorpb::Error>> {
        self.inner.gc_region(region, safe_point)
    }
}

/// `mysql.tidb`, `mysql.gc_delete_range` and `mysql.gc_delete_range_done`.
#[derive(Default)]
struct SysState {
    tidb: BTreeMap<String, (String, String)>,
    /// Rows in insertion order, as an un-clustered table scans them.
    delete_ranges: Vec<(DelRangeTask, u64)>,
    done_ranges: Vec<(DelRangeTask, u64)>,
    raft_kv2: bool,
    /// Go's `mockHistoryJobForGC` / `mockHistoryJob` failpoints.
    history_job: Option<fn(i64) -> tidb_model::Job>,
}

#[derive(Default)]
struct FakeSessions {
    state: Mutex<SysState>,
}

struct FakeSession<'a> {
    sessions: &'a FakeSessions,
}

impl GcSession for FakeSession<'_> {
    fn begin(&mut self) -> GcResult<()> {
        Ok(())
    }

    fn commit(&mut self) -> GcResult<()> {
        Ok(())
    }

    fn rollback(&mut self) {}

    fn load_value(&mut self, key: &str) -> GcResult<Option<String>> {
        Ok(self
            .sessions
            .state
            .lock()
            .unwrap()
            .tidb
            .get(key)
            .map(|(value, _)| value.clone()))
    }

    fn save_value(&mut self, key: &str, value: &str, comment: &str) -> GcResult<()> {
        self.sessions
            .state
            .lock()
            .unwrap()
            .tidb
            .insert(key.to_owned(), (value.to_owned(), comment.to_owned()));
        Ok(())
    }

    fn load_delete_ranges(&mut self, done: bool, safe_point: u64) -> GcResult<Vec<DelRangeTask>> {
        let state = self.sessions.state.lock().unwrap();
        let rows = if done {
            &state.done_ranges
        } else {
            &state.delete_ranges
        };
        Ok(rows
            .iter()
            .filter(|(_, ts)| *ts < safe_point)
            .map(|(range, _)| range.clone())
            .collect())
    }

    fn complete_delete_range(&mut self, range: &DelRangeTask, record_done: bool) -> GcResult<()> {
        let mut state = self.sessions.state.lock().unwrap();
        let position = state
            .delete_ranges
            .iter()
            .position(|(row, _)| row.job_id == range.job_id && row.element_id == range.element_id);
        if let Some(position) = position {
            let row = state.delete_ranges.remove(position);
            if record_done {
                state.done_ranges.push(row);
            }
        }
        Ok(())
    }

    fn delete_done_record(&mut self, range: &DelRangeTask) -> GcResult<()> {
        self.sessions
            .state
            .lock()
            .unwrap()
            .done_ranges
            .retain(|(row, _)| !(row.job_id == range.job_id && row.element_id == range.element_id));
        Ok(())
    }

    fn is_raft_kv2(&mut self) -> GcResult<bool> {
        Ok(self.sessions.state.lock().unwrap().raft_kv2)
    }

    fn history_job(&mut self, job_id: i64) -> GcResult<Option<tidb_model::Job>> {
        Ok(self
            .sessions
            .state
            .lock()
            .unwrap()
            .history_job
            .map(|make| make(job_id)))
    }
}

impl GcSessionFactory for FakeSessions {
    fn create_session(&self) -> Box<dyn GcSession + '_> {
        Box::new(FakeSession { sessions: self })
    }
}

/// `infosync`'s rule services, recording the calls.
#[derive(Default)]
struct FakeRules {
    deleted_tiflash_rules: Mutex<Vec<Vec<i64>>>,
    put_bundles: Mutex<Vec<Vec<String>>>,
    label_rules: Mutex<HashMap<String, tidb_ddl_label::Rule>>,
    deleted_label_rules: Mutex<Vec<Vec<String>>>,
}

impl GcRuleClient for FakeRules {
    fn delete_tiflash_placement_rules(&self, physical_table_ids: &[i64]) -> GcResult<()> {
        self.deleted_tiflash_rules
            .lock()
            .unwrap()
            .push(physical_table_ids.to_vec());
        Ok(())
    }

    fn put_rule_bundles(&self, bundles: Vec<tidb_placement::Bundle>) -> GcResult<()> {
        self.put_bundles
            .lock()
            .unwrap()
            .push(bundles.into_iter().map(|bundle| bundle.id).collect());
        Ok(())
    }

    fn get_label_rules(
        &self,
        rule_ids: &[String],
    ) -> GcResult<HashMap<String, tidb_ddl_label::Rule>> {
        let rules = self.label_rules.lock().unwrap();
        Ok(rule_ids
            .iter()
            .filter_map(|id| rules.get(id).map(|rule| (id.clone(), rule.clone())))
            .collect())
    }

    fn delete_label_rules(&self, rule_ids: Vec<String>) -> GcResult<()> {
        self.deleted_label_rules.lock().unwrap().push(rule_ids);
        Ok(())
    }
}

/// Go `gcProbe`: a key with two versions, the first collectable.
struct GcProbe {
    key: Vec<u8>,
    v1_ts: u64,
    v2_ts: u64,
}

/// Go `mockGCWorkerSuite`.
struct Suite {
    /// Go `s.oracle`, installed as the store's oracle.
    oracle: MockOracle,
    /// The worker's store, PD and TiKV halves over the embedded store.
    tikv: Arc<TikvStoreClient<StorePdClient>>,
    /// Go `s.pdClient`'s embedded-store half: unistore's PD client.
    pd: Arc<tidb_unistore::pd::PdClient>,
    cluster: Arc<Cluster>,
    /// Go `s.initRegion.regionID`.
    init_region_id: u64,
    kv: Arc<FakeKv>,
    sessions: Arc<FakeSessions>,
    rules: Arc<FakeRules>,
    worker: GcWorker,
}

/// Go `createGCWorkerSuite`: three stores, the worker created and ticked
/// once (Go's `Start` ticks immediately, then the suite closes it).
fn create_gc_worker_suite() -> Suite {
    create_gc_worker_suite_with(None, Vec::new())
}

fn create_gc_worker_suite_with(
    current_keyspace: Option<KeyspaceMeta>,
    keyspaces: Vec<KeyspaceMeta>,
) -> Suite {
    create_gc_worker_suite_full(current_keyspace, keyspaces, None)
}

fn create_gc_worker_suite_full(
    current_keyspace: Option<KeyspaceMeta>,
    keyspaces: Vec<KeyspaceMeta>,
    gcv2: Option<Arc<dyn GcV2Manager>>,
) -> Suite {
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
    );
    let (rpc, pd, cluster) = tidb_unistore::mock::new(keyspaces).unwrap();
    let (_, _, init_region_id, _) = bootstrap_with_multi_stores(&cluster, 3);
    let scan_lock_requests: Arc<Mutex<Vec<ScanLockRequest>>> = Arc::default();
    let recorded = Arc::clone(&scan_lock_requests);
    rpc.set_send_hook(Some(Arc::new(move |request| {
        if let Some(scan_lock) = request.as_any().downcast_ref::<kvrpcpb::ScanLockRequest>() {
            // Resolving locks scans at txn safe point - 1.
            recorded.lock().unwrap().push((
                scan_lock.start_key.clone(),
                scan_lock.end_key.clone(),
                scan_lock.max_version + 1,
            ));
        }
    })));
    // Go `NewTestKeyspaceTiKVStore` with a current keyspace, else
    // `NewTestTiKVStore`.
    let mut config = tikv_client::Config::default();
    config.keyspace = current_keyspace.as_ref().map(|meta| meta.name.clone());
    let store_pd = runtime
        .block_on(tidb_unistore::mock::store_pd_client(
            &rpc,
            Arc::clone(&pd),
            config.clone(),
        ))
        .unwrap();
    let kv_store = runtime
        .block_on(KvStore::new_with_pd_client(
            Arc::new(store_pd),
            Arc::new(MockSafePointKv::new()),
            config,
        ))
        .unwrap();
    // Go: `store.GetOracle().Close(); store.SetOracle(s.oracle)`.
    let oracle = MockOracle::new();
    kv_store.set_oracle(Arc::new(oracle.clone()));
    let tikv = Arc::new(TikvStoreClient::new(
        runtime,
        kv_store,
        current_keyspace.clone(),
    ));
    let kv = Arc::new(FakeKv {
        inner: Arc::clone(&tikv),
        destroy_handler: Mutex::new(None),
        destroy_requests: Mutex::new(Vec::new()),
        delete_range_requests: Mutex::new(Vec::new()),
        scan_lock_requests,
    });
    let sessions = Arc::new(FakeSessions::default());
    let rules = Arc::new(FakeRules::default());
    let mut worker = GcWorker::new(GcWorkerDeps {
        store: tikv.clone(),
        pd: tikv.clone(),
        kv: kv.clone(),
        sessions: sessions.clone(),
        rules: rules.clone(),
        gcv2,
    })
    .unwrap();
    worker.tick();
    Suite {
        oracle,
        tikv,
        pd,
        cluster,
        init_region_id,
        kv,
        sessions,
        rules,
        worker,
    }
}

impl Suite {
    fn job(&self) -> &GcJobContext {
        &self.worker.job
    }

    /// The embedded store's PD.
    fn mock_pd(&self) -> Arc<tidb_unistore::mock_pd::MockPd> {
        Arc::clone(self.pd.mock_pd())
    }

    /// Go `s.pdClient.GetGCStatesClient(keyspace).GetGCState`.
    fn gc_state(&self) -> GcState {
        let keyspace = self
            .tikv
            .keyspace_meta()
            .as_ref()
            .map_or(NULL_KEYSPACE_ID, keyspace_id);
        self.mock_pd()
            .gc_states()
            .gc_states_client(keyspace)
            .get_gc_state(false)
    }

    /// Go `s.pdClient.GetGCInternalController(keyspace).AdvanceTxnSafePoint`.
    fn advance_txn_safe_point(&self, target: u64) -> PdAdvanceTxnSafePointResult {
        let keyspace = self
            .tikv
            .keyspace_meta()
            .as_ref()
            .map_or(NULL_KEYSPACE_ID, keyspace_id);
        self.mock_pd()
            .gc_states()
            .gc_internal_controller(keyspace)
            .advance_txn_safe_point(target)
            .unwrap()
    }

    /// Go `s.oracle.AddOffset`.
    fn add_offset(&self, offset: Duration) {
        self.oracle
            .add_offset_milliseconds(i64::try_from(offset.as_millis()).unwrap());
    }

    /// The worker's collaborators, for the package-level job APIs.
    fn deps(&self) -> GcWorkerDeps {
        GcWorkerDeps {
            store: self.tikv.clone(),
            pd: self.tikv.clone(),
            kv: self.kv.clone(),
            sessions: self.sessions.clone(),
            rules: self.rules.clone(),
            gcv2: None,
        }
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.tikv.runtime().block_on(future)
    }

    /// Go `mustPut`.
    fn must_put(&self, key: &[u8], value: &[u8]) {
        self.block_on(async {
            let mut txn = self.tikv.store().begin_optimistic().await.unwrap();
            txn.put(key.to_vec(), value.to_vec()).await.unwrap();
            txn.commit().await.unwrap();
        });
    }

    fn snapshot_get(&self, key: &[u8], ts: u64) -> tikv_client::Result<Option<Vec<u8>>> {
        self.block_on(async {
            let mut snapshot = self.tikv.store().snapshot(
                <tikv_client::Timestamp as tikv_client::TimestampExt>::from_version(ts),
                tikv_client::transaction::TransactionOptions::new_optimistic(),
            );
            snapshot.get(key.to_vec()).await
        })
    }

    /// Go `mustGet`.
    fn must_get(&self, key: &[u8], ts: u64) -> Vec<u8> {
        self.snapshot_get(key, ts).unwrap().unwrap_or_default()
    }

    /// Go `mustGetNone`: unistore's GC rides compaction, so a version still
    /// there is no failure; only an error other than not-found is.
    fn must_get_none(&self, key: &[u8], ts: u64) {
        self.snapshot_get(key, ts).unwrap();
    }

    /// Go `createGCProbe`: `key` at "v1" then "v2", a timestamp after each.
    fn create_gc_probe(&self, key: &[u8]) -> GcProbe {
        self.must_put(key, b"v1");
        let v1_ts = self.must_alloc_ts();
        self.must_put(key, b"v2");
        let v2_ts = self.must_alloc_ts();
        let probe = GcProbe {
            key: key.to_vec(),
            v1_ts,
            v2_ts,
        };
        self.check_not_collected(&probe);
        probe
    }

    /// Go `checkCollected`.
    fn check_collected(&self, probe: &GcProbe) {
        self.must_get_none(&probe.key, probe.v1_ts);
        assert_eq!(b"v2".to_vec(), self.must_get(&probe.key, probe.v2_ts));
    }

    /// Go `checkNotCollected`.
    fn check_not_collected(&self, probe: &GcProbe) {
        assert_eq!(b"v1".to_vec(), self.must_get(&probe.key, probe.v1_ts));
        assert_eq!(b"v2".to_vec(), self.must_get(&probe.key, probe.v2_ts));
    }

    /// A pessimistic transaction holding `key` (Go: `Begin`, `Set`,
    /// `LockKeys` with `ForUpdateTS` = start ts), at `start_ts` or a fresh
    /// timestamp.
    fn pending_pessimistic_txn(
        &self,
        start_ts: Option<u64>,
        key: &[u8],
        value: &[u8],
    ) -> tikv_client::transaction::Transaction<StorePdClient> {
        self.block_on(async {
            let mut options = tikv_client::transaction::TransactionOptions::new_pessimistic();
            if let Some(start_ts) = start_ts {
                options = options.start_timestamp(
                    <tikv_client::Timestamp as tikv_client::TimestampExt>::from_version(start_ts),
                );
            }
            let mut txn = self.tikv.store().begin_with_options(options).await.unwrap();
            txn.put(key.to_vec(), value.to_vec()).await.unwrap();
            txn.lock_keys(vec![key.to_vec()]).await.unwrap();
            txn
        })
    }

    /// Go `leaderTick`, then waiting for the job it starts.
    fn leader_tick_and_wait(&mut self) {
        self.worker.leader_tick().unwrap();
        self.worker
            .done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("receive from s.gcWorker.done")
            .unwrap();
        self.worker.gc_is_running = false;
    }

    /// Go `loadTxnSafePoint`.
    fn load_txn_safe_point(&self) -> u64 {
        self.gc_state().txn_safe_point
    }

    /// Go `splitAtKeys`: the cluster split at the raw keys (the store's PD
    /// keeps no region cache to invalidate).
    fn split_at_keys(&self, keys: &[Vec<u8>]) {
        let mut keys = keys.to_vec();
        keys.sort();
        self.cluster.split_arbitrary(&keys);
    }

    /// Go sets its package variable `txnSafePointSyncWaitTime`.
    fn set_sync_wait_time(&self, wait: Duration) {
        set_txn_safe_point_sync_wait_time(wait);
    }

    /// Go `mustAllocTs`.
    fn must_alloc_ts(&self) -> u64 {
        self.tikv.current_version().unwrap()
    }

    /// Go `INSERT INTO mysql.gc_delete_range VALUES (...)`.
    fn insert_delete_ranges(&self, rows: &[(i64, i64, &str, &str, u64)]) {
        let mut state = self.sessions.state.lock().unwrap();
        for &(job_id, element_id, start_key, end_key, ts) in rows {
            state.delete_ranges.push((
                DelRangeTask {
                    job_id,
                    element_id,
                    start_key: start_key.as_bytes().to_vec(),
                    end_key: end_key.as_bytes().to_vec(),
                },
                ts,
            ));
        }
    }

    fn load_delete_ranges(&self, done: bool, safe_point: u64) -> Vec<DelRangeTask> {
        self.sessions
            .create_session()
            .load_delete_ranges(done, safe_point)
            .unwrap()
    }

    /// Go `checkDestroyRangeReq`: every range sent to every store, sorted by
    /// start key then address.
    fn check_destroy_range_req(&self, expected_ranges: &[DelRangeTask], stores: &[metapb::Store]) {
        let mut sent = std::mem::take(&mut *self.kv.destroy_requests.lock().unwrap());
        sent.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
        let mut sorted_ranges = expected_ranges.to_vec();
        sorted_ranges.sort_by(|left, right| left.start_key.cmp(&right.start_key));
        assert_eq!(sent.len(), sorted_ranges.len() * stores.len());
        for (range_index, range) in sorted_ranges.iter().enumerate() {
            for (store_index, store) in stores.iter().enumerate() {
                let request = &sent[range_index * stores.len() + store_index];
                assert_eq!(store.address, request.0);
                assert_eq!(range.start_key, request.1);
                assert_eq!(range.end_key, request.2);
            }
        }
    }

    /// Go `checkDestroyRangeReqV2`.
    fn check_destroy_range_req_v2(&self, expected_ranges: &[DelRangeTask]) {
        let mut sent = std::mem::take(&mut *self.kv.delete_range_requests.lock().unwrap());
        sent.sort();
        let mut sorted_ranges = expected_ranges.to_vec();
        sorted_ranges.sort_by(|left, right| left.start_key.cmp(&right.start_key));
        assert_eq!(sent.len(), sorted_ranges.len());
        for (request, range) in sent.iter().zip(&sorted_ranges) {
            assert_eq!(range.start_key, request.0);
            assert_eq!(range.end_key, request.1);
        }
    }
}

fn time_equal(
    left: chrono::DateTime<chrono::FixedOffset>,
    right: chrono::DateTime<chrono::FixedOffset>,
    epsilon: Duration,
) {
    let difference = (left - right).num_milliseconds().unsigned_abs();
    assert!(
        difference < epsilon.as_millis() as u64,
        "{left} and {right} differ by {difference}ms"
    );
}

const MINUTE: Duration = Duration::from_secs(60);
const MINUTE_NANOS: i64 = 60 * 1_000_000_000;

/// The job Go's `mockHistoryJobForGC` and `mockHistoryJob` failpoints build:
/// a finished DROP TABLE of table 1 whose old partition is 1 and whose label
/// rule is `schema/d1/t1`, round-tripped through its encoding.
fn mock_drop_table_job(job_id: i64, table_id: i64, rule_id: &str) -> tidb_model::Job {
    let mut job = tidb_model::Job::default();
    job.version = tidb_model::job_enums::get_job_ver_in_use();
    job.id = job_id;
    job.type_ = tidb_model::ActionType::ACTION_DROP_TABLE;
    job.table_id = table_id;
    job.fill_finished_args(Some(tidb_model::GoShared::new(tidb_model::DropTableArgs {
        old_partition_ids: vec![table_id],
        old_rule_ids: if rule_id.is_empty() {
            Vec::new()
        } else {
            vec![rule_id.to_owned()]
        },
        ..Default::default()
    })));
    let bytes = job.encode(true).unwrap();
    let mut decoded = tidb_model::Job::default();
    decoded.decode(&bytes).unwrap();
    decoded
}

fn mock_history_job_for_delete_ranges(job_id: i64) -> tidb_model::Job {
    mock_drop_table_job(job_id, 1, "schema/d1/t1")
}

#[test]
fn test_get_oracle_time() {
    let s = create_gc_worker_suite();
    let t1 = s.job().get_oracle_time().unwrap();
    time_equal(
        chrono::Local::now().fixed_offset(),
        t1,
        Duration::from_millis(10),
    );

    s.add_offset(Duration::from_secs(10));
    let t2 = s.job().get_oracle_time().unwrap();
    time_equal(
        t2,
        t1 + chrono::Duration::seconds(10),
        Duration::from_millis(10),
    );
}

#[test]
fn test_prepare_gc() {
    let s = create_gc_worker_suite();
    let job = s.job();

    let now = job.get_oracle_time().unwrap();
    let last_run_before = job.load_time(GC_LAST_RUN_TIME_KEY).unwrap().unwrap();
    let safe_point_before = job.load_time(GC_SAFE_POINT_KEY).unwrap().unwrap();
    time_equal(
        safe_point_before + chrono::Duration::nanoseconds(GC_DEFAULT_LIFE_TIME),
        now,
        Duration::from_secs(2),
    );

    let (ok, _) = job.prepare().unwrap();
    assert!(!ok);
    let last_run = job.load_time(GC_LAST_RUN_TIME_KEY).unwrap().unwrap();
    let safe_point = job.load_time(GC_SAFE_POINT_KEY).unwrap().unwrap();
    assert_eq!(last_run_before, last_run);
    assert_eq!(safe_point_before, safe_point);
    time_equal(
        safe_point + chrono::Duration::nanoseconds(GC_DEFAULT_LIFE_TIME),
        last_run,
        Duration::from_secs(2),
    );

    // Change the GC run interval.
    job.save_duration(GC_RUN_INTERVAL_KEY, 5 * MINUTE_NANOS)
        .unwrap();
    s.add_offset(4 * MINUTE);
    assert!(!job.prepare().unwrap().0);
    s.add_offset(2 * MINUTE);
    assert!(job.prepare().unwrap().0);

    // Change the GC life time.
    job.save_duration(GC_LIFE_TIME_KEY, 30 * MINUTE_NANOS)
        .unwrap();
    s.add_offset(5 * MINUTE);
    assert!(!job.prepare().unwrap().0);
    s.add_offset(40 * MINUTE);
    assert!(job.prepare().unwrap().0);
    let last_run = job.load_time(GC_LAST_RUN_TIME_KEY).unwrap().unwrap();
    let safe_point = job.load_time(GC_SAFE_POINT_KEY).unwrap().unwrap();
    time_equal(
        safe_point + chrono::Duration::minutes(30),
        last_run,
        Duration::from_secs(2),
    );

    // Change the GC concurrency.
    let (concurrency, result) = job.load_gc_concurrency_with_default();
    result.unwrap();
    assert_eq!(GC_DEFAULT_CONCURRENCY, concurrency);

    job.save_value_to_sys_table(GC_CONCURRENCY_KEY, &GC_MIN_CONCURRENCY.to_string())
        .unwrap();
    let (concurrency, result) = job.load_gc_concurrency_with_default();
    result.unwrap();
    assert_eq!(GC_MIN_CONCURRENCY, concurrency);

    job.save_value_to_sys_table(GC_CONCURRENCY_KEY, "-1")
        .unwrap();
    let (concurrency, result) = job.load_gc_concurrency_with_default();
    result.unwrap();
    assert_eq!(GC_MIN_CONCURRENCY, concurrency);

    job.save_value_to_sys_table(GC_CONCURRENCY_KEY, "1000000")
        .unwrap();
    let (concurrency, result) = job.load_gc_concurrency_with_default();
    result.unwrap();
    assert_eq!(GC_MAX_CONCURRENCY, concurrency);

    // Change the GC enable status.
    s.add_offset(40 * MINUTE);
    job.save_value_to_sys_table(GC_ENABLE_KEY, BOOLEAN_FALSE)
        .unwrap();
    assert!(!job.prepare().unwrap().0);
    job.save_value_to_sys_table(GC_ENABLE_KEY, BOOLEAN_TRUE)
        .unwrap();
    assert!(job.prepare().unwrap().0);

    // A GC life time below the minimum.
    s.add_offset(40 * MINUTE);
    job.save_duration(GC_LIFE_TIME_KEY, MINUTE_NANOS).unwrap();
    assert!(job.prepare().unwrap().0);
    let life_time = job.load_duration(GC_LIFE_TIME_KEY).unwrap().unwrap();
    assert_eq!(GC_MIN_LIFE_TIME, life_time);

    s.add_offset(40 * MINUTE);
    job.save_duration(GC_LIFE_TIME_KEY, 30 * MINUTE_NANOS)
        .unwrap();
    assert!(job.prepare().unwrap().0);
    let life_time = job.load_duration(GC_LIFE_TIME_KEY).unwrap().unwrap();
    assert_eq!(30 * MINUTE_NANOS, life_time);

    // Change the auto concurrency.
    job.save_value_to_sys_table(GC_AUTO_CONCURRENCY_KEY, BOOLEAN_FALSE)
        .unwrap();
    assert!(!job.check_use_auto_concurrency().unwrap());
    job.save_value_to_sys_table(GC_AUTO_CONCURRENCY_KEY, BOOLEAN_TRUE)
        .unwrap();
    assert!(job.check_use_auto_concurrency().unwrap());

    // GC is skipped when the safe point does not move: a GC barrier blocks it.
    let last_txn_safe_point = s.gc_state().txn_safe_point;
    assert_ne!(0, last_txn_safe_point);
    s.mock_pd()
        .gc_states()
        .gc_states_client(NULL_KEYSPACE_ID)
        .set_gc_barrier("a", last_txn_safe_point, Duration::MAX)
        .unwrap();
    s.add_offset(40 * MINUTE);
    let (ok, safe_point) = job.prepare().unwrap();
    assert!(!ok);
    assert_eq!(0, safe_point);
}

#[test]
fn test_status_vars() {
    let s = create_gc_worker_suite();
    // The safe point and the last run time exist after the first tick.
    let safe_point = s
        .job()
        .load_value_from_sys_table(GC_SAFE_POINT_KEY)
        .unwrap();
    let last_run_time = s
        .job()
        .load_value_from_sys_table(GC_LAST_RUN_TIME_KEY)
        .unwrap();

    let status_vars = s.worker.stats();
    assert_eq!(Some(&safe_point), status_vars.get(TIDB_GC_SAFE_POINT));
    assert_eq!(Some(&last_run_time), status_vars.get(TIDB_GC_LAST_RUN_TIME));
}

#[test]
fn test_get_gc_concurrency() {
    let s = create_gc_worker_suite();
    let job = s.job();

    // A concurrency that differs from the number of stores.
    let concurrency_config = 25;
    assert_ne!(s.cluster.get_all_stores().len(), concurrency_config);
    job.save_value_to_sys_table(GC_CONCURRENCY_KEY, &concurrency_config.to_string())
        .unwrap();

    job.save_value_to_sys_table(GC_AUTO_CONCURRENCY_KEY, BOOLEAN_FALSE)
        .unwrap();
    assert_eq!(concurrency_config, job.get_gc_concurrency().unwrap().v);

    job.save_value_to_sys_table(GC_AUTO_CONCURRENCY_KEY, BOOLEAN_TRUE)
        .unwrap();
    assert_eq!(
        s.cluster.get_all_stores().len(),
        job.get_gc_concurrency().unwrap().v
    );
}

#[test]
fn test_check_gc_mode() {
    let s = create_gc_worker_suite();
    let job = s.job();

    assert!(job.check_use_distributed_gc());
    // The row now holds the default.
    assert_eq!(
        GC_MODE_DISTRIBUTED,
        job.load_value_from_sys_table(GC_MODE_KEY).unwrap()
    );

    // Central mode is deprecated since v5.0.
    job.save_value_to_sys_table(GC_MODE_KEY, GC_MODE_CENTRAL)
        .unwrap();
    assert!(job.check_use_distributed_gc());

    job.save_value_to_sys_table(GC_MODE_KEY, GC_MODE_DISTRIBUTED)
        .unwrap();
    assert!(job.check_use_distributed_gc());

    job.save_value_to_sys_table(GC_MODE_KEY, "invalid_mode")
        .unwrap();
    assert!(job.check_use_distributed_gc());
}

#[test]
fn test_needs_gc_operation_for_store() {
    let new_store = |state: metapb::StoreState, has_engine_label: bool, engine_label: &str| {
        let mut store = metapb::Store {
            state: state as i32,
            ..metapb::Store::default()
        };
        if has_engine_label {
            store.labels = vec![metapb::StoreLabel {
                key: tidb_placement::ENGINE_LABEL_KEY.to_owned(),
                value: engine_label.to_owned(),
            }];
        }
        store
    };

    // TiKV needs the store-level GC operations.
    for state in [
        metapb::StoreState::Up,
        metapb::StoreState::Offline,
        metapb::StoreState::Tombstone,
    ] {
        let need_gc = state != metapb::StoreState::Tombstone;
        assert_eq!(
            need_gc,
            needs_gc_operation_for_store(&new_store(state, false, "")).unwrap()
        );
        assert_eq!(
            need_gc,
            needs_gc_operation_for_store(&new_store(state, true, "")).unwrap()
        );
        assert_eq!(
            need_gc,
            needs_gc_operation_for_store(&new_store(
                state,
                true,
                tidb_placement::ENGINE_LABEL_TIKV
            ))
            .unwrap()
        );
        // TiFlash does not.
        assert!(!needs_gc_operation_for_store(&new_store(
            state,
            true,
            tidb_placement::ENGINE_LABEL_TIFLASH
        ))
        .unwrap());
    }
    // An unknown store type is an error.
    assert!(
        needs_gc_operation_for_store(&new_store(metapb::StoreState::Up, true, "invalid")).is_err()
    );
}

const FAIL_RPC_ERR: usize = 0;
const FAIL_NIL_RESP: usize = 1;
const FAIL_ERR_RESP: usize = 2;

#[test]
fn test_delete_ranges_failure() {
    for fail_type in [FAIL_RPC_ERR, FAIL_NIL_RESP, FAIL_ERR_RESP] {
        let s = create_gc_worker_suite();
        s.sessions.state.lock().unwrap().history_job = Some(mock_history_job_for_delete_ranges);

        // Some delete range tasks.
        s.insert_delete_ranges(&[
            (1, 2, "1", "2", 10),
            (3, 4, "3", "4", 10),
            (5, 6, "5", "6", 10),
        ]);
        let task = |job_id, element_id, start: &str, end: &str| DelRangeTask {
            job_id,
            element_id,
            start_key: start.as_bytes().to_vec(),
            end_key: end.as_bytes().to_vec(),
        };
        let mut ranges = vec![
            task(1, 2, "1", "2"),
            task(3, 4, "3", "4"),
            task(5, 6, "5", "6"),
        ];
        assert_eq!(ranges, s.load_delete_ranges(false, 20));

        let mut stores = s.job().get_stores_for_gc().unwrap();
        assert_eq!(3, stores.len());
        stores.sort_by(|left, right| left.address.cmp(&right.address));

        // The request for one key on one store fails.
        let fail: FailingRequest = Arc::new(Mutex::new(None));
        let handler_fail = Arc::clone(&fail);
        *s.kv.destroy_handler.lock().unwrap() = Some(Box::new(move |address, start, _| {
            let fail = handler_fail.lock().unwrap();
            if fail
                .as_ref()
                .is_some_and(|(key, store)| key.as_slice() == start && store == address)
            {
                return match fail_type {
                    FAIL_RPC_ERR => Err(GcError::new("error")),
                    FAIL_NIL_RESP => Ok(None),
                    FAIL_ERR_RESP => Ok(Some("error".to_owned())),
                    _ => unreachable!(),
                };
            }
            Ok(Some(String::new()))
        }));

        let concurrency = GcConcurrency {
            v: 1,
            is_auto: false,
        };
        let check = |redo: bool, ranges: &[DelRangeTask]| {
            let delete = |safe_point| {
                if redo {
                    s.job().redo_delete_ranges(safe_point, concurrency)
                } else {
                    s.job().delete_ranges(safe_point, concurrency)
                }
            };
            // The first request fails.
            *fail.lock().unwrap() = Some((ranges[0].start_key.clone(), stores[0].address.clone()));
            delete(20).unwrap();
            s.check_destroy_range_req(ranges, &stores);

            // The first task stays, since it did not succeed.
            assert_eq!(ranges[..1].to_vec(), s.load_delete_ranges(redo, 20));

            *fail.lock().unwrap() = None;
            // Delete the remaining range again.
            delete(20).unwrap();
            s.check_destroy_range_req(&ranges[..1], &stores);
            assert!(s.load_delete_ranges(redo, 20).is_empty());
        };

        check(false, &ranges);
        // The first range was the last one deleted successfully.
        ranges.rotate_left(1);
        // Go's redo reads `safePoint - 24h` as a TSO; the done rows' ts of 10
        // sits below it only through the unsigned wrap Go also performs.
        check(true, &ranges);
    }
}

#[test]
fn test_concurrent_delete_ranges() {
    // The parallelization of deleteRanges works.
    let s = create_gc_worker_suite();
    s.sessions.state.lock().unwrap().history_job = Some(mock_history_job_for_delete_ranges);
    s.insert_delete_ranges(&[
        (1, 2, "31", "32", 10),
        (3, 4, "33", "34", 10),
        (5, 6, "35", "36", 15),
        (7, 8, "37", "38", 15),
        (9, 10, "39", "40", 15),
    ]);
    let ranges = s.load_delete_ranges(false, 20);
    assert_eq!(5, ranges.len());

    let mut stores = s.job().get_stores_for_gc().unwrap();
    assert_eq!(3, stores.len());
    stores.sort_by(|left, right| left.address.cmp(&right.address));

    s.job()
        .delete_ranges(
            20,
            GcConcurrency {
                v: 3,
                is_auto: false,
            },
        )
        .unwrap();
    s.check_destroy_range_req(&ranges, &stores);
    assert!(s.load_delete_ranges(false, 20).is_empty());
}

#[test]
fn test_unsafe_destroy_range_for_raftkv2() {
    let s = create_gc_worker_suite();
    {
        let mut state = s.sessions.state.lock().unwrap();
        state.raft_kv2 = true;
        state.history_job = Some(mock_history_job_for_delete_ranges);
    }
    s.insert_delete_ranges(&[
        (1, 2, "1", "2", 5),
        (3, 4, "3", "4", 10),
        (5, 6, "5", "6", 15),
        (7, 8, "7", "8", 15),
    ]);
    let ranges = s.load_delete_ranges(false, 20);
    assert_eq!(4, ranges.len());

    let concurrency = GcConcurrency {
        v: 1,
        is_auto: false,
    };
    s.job().delete_ranges(8, concurrency).unwrap();
    s.check_destroy_range_req_v2(&ranges[..1]);
    assert_eq!(ranges[1..].to_vec(), s.load_delete_ranges(false, 20));

    s.job().delete_ranges(20, concurrency).unwrap();
    s.check_destroy_range_req_v2(&ranges[1..]);

    // raft-kv2 records no done range.
    assert!(s.load_delete_ranges(true, 20).is_empty());
}

#[test]
fn test_leader_tick() {
    let mut s = create_gc_worker_suite();
    s.set_sync_wait_time(Duration::ZERO);

    let very_long = Duration::from_nanos((GC_DEFAULT_LIFE_TIME * 10) as u64);
    // Avoid failing at the interval check; `lastFinish` is checked by OS time.
    s.worker.last_finish = Instant::now() - very_long;
    // Central mode is configured, and distributed mode runs anyway.
    s.job()
        .save_value_to_sys_table(GC_MODE_KEY, GC_MODE_CENTRAL)
        .unwrap();
    let p = s.create_gc_probe(b"k1");
    s.add_offset(Duration::from_nanos((GC_DEFAULT_LIFE_TIME * 2) as u64));
    let reset_last_run = |s: &Suite| {
        let long_ago =
            time_from_ts(s.must_alloc_ts()) - chrono::Duration::from_std(very_long).unwrap();
        s.job().save_time(GC_LAST_RUN_TIME_KEY, long_ago).unwrap();
    };
    let gc_safe_point = |s: &Suite| s.gc_state().gc_safe_point;

    // Skipped while GC is running.
    let before = gc_safe_point(&s);
    s.worker.gc_is_running = true;
    s.worker.leader_tick().unwrap();
    assert!(s.worker.done_rx.try_recv().is_err());
    s.check_not_collected(&p);
    s.worker.gc_is_running = false;
    reset_last_run(&s);

    // Skipped when prepare fails (disabled GC makes it answer false).
    s.job()
        .save_value_to_sys_table(GC_ENABLE_KEY, BOOLEAN_FALSE)
        .unwrap();
    s.worker.leader_tick().unwrap();
    assert!(!s.worker.gc_is_running);
    s.check_not_collected(&p);
    s.job()
        .save_value_to_sys_table(GC_ENABLE_KEY, BOOLEAN_TRUE)
        .unwrap();
    reset_last_run(&s);

    // Skipped when gcWaitTime has not passed.
    s.worker.last_finish = Instant::now();
    s.worker.leader_tick().unwrap();
    assert!(!s.worker.gc_is_running);
    s.check_not_collected(&p);
    s.worker.last_finish = Instant::now() - very_long;
    reset_last_run(&s);
    // The skipped tick's prepare advanced PD's txn safe point; move the
    // oracle so the next target is strictly higher.
    s.add_offset(Duration::from_secs(1));
    assert_eq!(before, gc_safe_point(&s));

    // GC runs once every check passes.
    s.worker.leader_tick().unwrap();
    assert!(s.worker.gc_is_running);
    s.worker
        .done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the GC job finishes")
        .unwrap();
    s.worker.gc_is_running = false;
    s.check_collected(&p);
    let first = gc_safe_point(&s);
    assert!(first > before);

    // Again, so the hand-off between threads is right.
    reset_last_run(&s);
    s.worker.last_finish = Instant::now() - very_long;
    let p = s.create_gc_probe(b"k1");
    s.add_offset(Duration::from_nanos((GC_DEFAULT_LIFE_TIME * 2) as u64));
    s.worker.leader_tick().unwrap();
    s.worker
        .done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the GC job finishes")
        .unwrap();
    s.worker.gc_is_running = false;
    s.check_collected(&p);
    assert!(gc_safe_point(&s) > first);

    // No other signal.
    assert!(s
        .worker
        .done_rx
        .recv_timeout(Duration::from_secs(1))
        .is_err());
}

/// Go `TestGCWithPendingTxn`. Go's `ServerMinStartTSPath` put into the
/// safe-point KV is skipped: the worker no longer reads it (Go's own TODO says
/// the mock PD ignores it).
#[test]
fn test_gc_with_pending_txn() {
    if tidb_config_kerneltype::is_next_gen() {
        return;
    }
    let mut s = create_gc_worker_suite();
    s.set_sync_wait_time(Duration::ZERO);
    s.job()
        .save_value_to_sys_table(GC_ENABLE_KEY, BOOLEAN_FALSE)
        .unwrap();

    // Lock the key.
    let mut txn = s.pending_pessimistic_txn(None, b"tk1", b"v1");

    // Prepare to run gc with txn's startTS as the safepoint ts.
    let very_long = chrono::Duration::nanoseconds(GC_DEFAULT_LIFE_TIME * 100);
    s.job()
        .save_time(
            GC_LAST_RUN_TIME_KEY,
            time_from_ts(s.must_alloc_ts()) - very_long,
        )
        .unwrap();
    s.worker.last_finish =
        Instant::now() - Duration::from_nanos((GC_DEFAULT_LIFE_TIME * 10) as u64);
    s.add_offset(10 * MINUTE);
    s.job()
        .save_value_to_sys_table(GC_ENABLE_KEY, BOOLEAN_TRUE)
        .unwrap();
    // Go's SQL writes since the begin take milliseconds, which puts the
    // job's millisecond-truncated safe point (oracle now - life time) past
    // the txn's start ts. The in-memory session takes none; wait one.
    std::thread::sleep(Duration::from_millis(2));

    // Trigger the tick let the gc job start.
    s.leader_tick_and_wait();

    // Go's TODO: the mock PD keeps no `SafePointKV` data, so the commit fails.
    assert!(s.block_on(txn.commit()).is_err());
}

/// Go `TestGCWithPendingTxn2`: two pessimistic locks newer than the job's
/// safe point survive it, and both transactions commit.
#[test]
fn test_gc_with_pending_txn2() {
    let mut s = create_gc_worker_suite();
    s.set_sync_wait_time(Duration::ZERO);
    s.job()
        .save_value_to_sys_table(GC_ENABLE_KEY, BOOLEAN_FALSE)
        .unwrap();
    let now = s.must_alloc_ts();

    let very_long = chrono::Duration::nanoseconds(GC_DEFAULT_LIFE_TIME * 100);
    s.job()
        .save_time(
            GC_LAST_RUN_TIME_KEY,
            time_from_ts(s.must_alloc_ts()) - very_long,
        )
        .unwrap();
    s.worker.last_finish =
        Instant::now() - Duration::from_nanos((GC_DEFAULT_LIFE_TIME * 10) as u64);
    s.job()
        .save_value_to_sys_table(GC_ENABLE_KEY, BOOLEAN_TRUE)
        .unwrap();

    // Lock key1 at now, key2 ten seconds later.
    let mut txn = s.pending_pessimistic_txn(Some(now), b"tk1", b"v1");
    // Go `oracle.ComposeTS(ExtractPhysical(now)+10000, ExtractLogical(now))`.
    let start_ts = (((now >> 18) + 10_000) << 18) | (now & ((1 << 18) - 1));
    let mut txn2 = s.pending_pessimistic_txn(Some(start_ts), b"tk2", b"v2");

    // Trigger the tick let the gc job start.
    s.add_offset(5 * MINUTE);
    s.leader_tick_and_wait();

    s.block_on(txn.commit()).unwrap();
    s.block_on(txn2.commit()).unwrap();
}

/// Go `TestSkipGCAndOnlyResolveLock`: with a life time pushing the new safe
/// point below the saved one, the tick skips GC, the lock is not resolved,
/// and the last run time stays.
#[test]
fn test_skip_gc_and_only_resolve_lock() {
    let mut s = create_gc_worker_suite();
    s.set_sync_wait_time(Duration::ZERO);
    let job = s.job();
    job.save_value_to_sys_table(GC_ENABLE_KEY, BOOLEAN_FALSE)
        .unwrap();
    let now = s.must_alloc_ts();

    assert_eq!(now, job.set_gc_worker_service_safe_point(now).unwrap());
    let very_long = chrono::Duration::nanoseconds(GC_DEFAULT_LIFE_TIME * 100);
    let last_run_time = time_from_ts(s.must_alloc_ts()) - very_long;
    let new_gc_life_time: i64 = 24 * 60 * 60 * 1_000_000_000;
    job.save_time(GC_LAST_RUN_TIME_KEY, last_run_time).unwrap();
    job.save_time(
        GC_SAFE_POINT_KEY,
        time_from_ts(s.must_alloc_ts()) - chrono::Duration::minutes(10),
    )
    .unwrap();
    job.save_duration(GC_LIFE_TIME_KEY, new_gc_life_time)
        .unwrap();
    s.worker.last_finish =
        Instant::now() - Duration::from_nanos((GC_DEFAULT_LIFE_TIME * 10) as u64);
    s.job()
        .save_value_to_sys_table(GC_ENABLE_KEY, BOOLEAN_TRUE)
        .unwrap();

    // Lock key1.
    let mut txn = s.pending_pessimistic_txn(Some(now), b"tk1", b"v1");

    // Trigger the tick let the gc job start.
    s.add_offset(5 * MINUTE);
    s.worker.leader_tick().unwrap();

    // The lock has not been resolved.
    s.block_on(txn.commit()).unwrap();

    // GC was skipped.
    let last = s.job().load_time(GC_LAST_RUN_TIME_KEY).unwrap().unwrap();
    assert_eq!(last.timestamp(), last_run_time.timestamp());
}

type ScanLocksFn = Arc<
    dyn Fn(&[kvrpcpb::LockInfo], &[u8]) -> (Vec<kvrpcpb::LockInfo>, Option<KeyLocation>)
        + Send
        + Sync,
>;
type BatchResolveLocksFn = Arc<
    dyn Fn(&[kvrpcpb::LockInfo], Option<KeyLocation>) -> tikv_client::Result<Option<KeyLocation>>
        + Send
        + Sync,
>;

/// Go `mockGCWorkerLockResolver`: the store's resolver, with the scanned
/// locks and their location replaced by `scan_locks` and the resolution by
/// `batch_resolve_locks` when set.
struct MockGcWorkerLockResolver<PdC: tikv_client::PdClient> {
    inner: BaseRegionLockResolver<PdC>,
    scan_locks: Arc<Mutex<Option<ScanLocksFn>>>,
    batch_resolve_locks: Mutex<Option<BatchResolveLocksFn>>,
}

impl<PdC: tikv_client::PdClient> MockGcWorkerLockResolver<PdC> {
    fn new(inner: BaseRegionLockResolver<PdC>) -> Self {
        Self {
            inner,
            scan_locks: Arc::default(),
            batch_resolve_locks: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl<PdC: tikv_client::PdClient> RegionLockResolver for MockGcWorkerLockResolver<PdC> {
    fn identifier(&self) -> String {
        "gc worker test".to_owned()
    }

    async fn resolve_locks_in_one_region(
        &self,
        backoff: &mut tikv_client::Backoff,
        locks: Vec<kvrpcpb::LockInfo>,
        location: Option<KeyLocation>,
    ) -> tikv_client::Result<Option<KeyLocation>> {
        let batch = self.batch_resolve_locks.lock().unwrap().clone();
        match batch {
            Some(batch) => batch(&locks, location),
            None => {
                self.inner
                    .resolve_locks_in_one_region(backoff, locks, location)
                    .await
            }
        }
    }

    async fn scan_locks_in_one_region(
        &self,
        backoff: &mut tikv_client::Backoff,
        key: &[u8],
        end_key: &[u8],
        max_version: u64,
        scan_limit: u32,
    ) -> tikv_client::Result<(Vec<kvrpcpb::LockInfo>, Option<KeyLocation>)> {
        let (mut locks, mut location) = self
            .inner
            .scan_locks_in_one_region(backoff, key, end_key, max_version, scan_limit)
            .await?;
        let scan = self.scan_locks.lock().unwrap().clone();
        if let Some(scan) = scan {
            let (mock_locks, mock_location) = scan(&locks, key);
            // Append the mock's locks; use its location.
            locks.extend(mock_locks);
            location = mock_location;
        }
        Ok((locks, location))
    }
}

/// Go `tikv.NewRegionVerID(id, confVer, ver)` as a location with no bounds.
fn location_of(id: u64, conf_ver: u64, ver: u64) -> KeyLocation {
    KeyLocation {
        region: tikv_client::RegionVerId { id, conf_ver, ver },
        start_key: Vec::new(),
        end_key: Vec::new(),
    }
}

fn lock(key: &[u8], txn_id: u64, ttl: u64) -> kvrpcpb::LockInfo {
    kvrpcpb::LockInfo {
        key: key.to_vec(),
        lock_version: txn_id,
        lock_ttl: ttl,
        ..kvrpcpb::LockInfo::default()
    }
}

/// Go `TestResolveLockRangeInfine`: a resolution error stops the range.
/// Go also enables client-go's `invalidCacheAndRetry` failpoint; the store's
/// PD keeps no region cache to invalidate.
#[test]
fn test_resolve_lock_range_infine() {
    let s = create_gc_worker_suite();
    let resolver = MockGcWorkerLockResolver::new(BaseRegionLockResolver::new(
        "test",
        (**s.tikv.store()).clone(),
    ));
    *resolver.scan_locks.lock().unwrap() =
        Some(Arc::new(|_, _| (Vec::new(), Some(location_of(0, 0, 0)))));
    // A mock error, to test the backoff.
    *resolver.batch_resolve_locks.lock().unwrap() = Some(Arc::new(|_, _| {
        Err(tikv_client::Error::StringError("mock error".to_owned()))
    }));
    let (_, result) = s.block_on(resolve_locks_for_range(
        tikv_client::async_util::Cancellation::default(),
        &resolver,
        1,
        vec![0],
        vec![1],
        &tikv_client::Backoff::no_backoff,
        10,
    ));
    assert!(result.is_err());
}

/// Go `TestResolveLockRangeMeetRegionCacheMiss`: a resolution that answers
/// no location makes the range scan the region again.
#[test]
fn test_resolve_lock_range_meet_region_cache_miss() {
    let s = create_gc_worker_suite();
    let safepoint_ts: u64 = 434_245_550_444_904_450;
    let all_locks = vec![
        // TxnID < safepointTS
        lock(&[1], 434_245_550_444_904_449, 5),
        // safepointTS < TxnID < lowResolveTS, TxnID + TTL < lowResolveTS
        lock(&[2], 434_245_550_445_166_592, 10),
        // safepointTS < TxnID < lowResolveTS, TxnID + TTL > lowResolveTS
        lock(&[3], 434_245_550_445_166_593, 20),
        // TxnID > lowResolveTS
        lock(&[4], 434_245_550_449_099_752, 20),
    ];
    let scan_cnt = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let resolve_cnt = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let resolver = MockGcWorkerLockResolver::new(BaseRegionLockResolver::new(
        "test",
        (**s.tikv.store()).clone(),
    ));
    let region_id = s.init_region_id;
    let scans = Arc::clone(&scan_cnt);
    *resolver.scan_locks.lock().unwrap() = Some(Arc::new(move |_, _| {
        scans.fetch_add(1, Ordering::AcqRel);
        (all_locks.clone(), Some(location_of(region_id, 0, 0)))
    }));
    let resolves = Arc::clone(&resolve_cnt);
    *resolver.batch_resolve_locks.lock().unwrap() = Some(Arc::new(move |_, location| {
        if resolves.fetch_add(1, Ordering::AcqRel) == 0 {
            // The region cache misses: no location, so the region is scanned
            // again (Go invalidates the cached region first).
            return Ok(None);
        }
        Ok(location)
    }));
    let (_, result) = s.block_on(resolve_locks_for_range(
        tikv_client::async_util::Cancellation::default(),
        &resolver,
        safepoint_ts,
        vec![0],
        vec![10],
        &tikv_client::Backoff::no_backoff,
        10,
    ));
    result.unwrap();
    assert_eq!(2, resolve_cnt.load(Ordering::Acquire));
    assert_eq!(2, scan_cnt.load(Ordering::Acquire));
}

/// Go `TestResolveLockRangeMeetRegionEnlargeCausedByRegionMerge`, on
/// client-go's mock cluster (Go's `MockTiKV`): the region merges while its
/// locks resolve, and the rescan of the merged region resolves all four.
#[test]
fn test_resolve_lock_range_meet_region_enlarge_caused_by_region_merge() {
    if tidb_config_kerneltype::is_next_gen() {
        return;
    }
    use tikv_client::mock::mocktikv::{
        bootstrap_with_multi_stores as mock_bootstrap_with_multi_stores, new_tikv_and_pd_client,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let (_rpc, cluster, pd) = new_tikv_and_pd_client("", None).unwrap();
    let (_, _, init_region_id, _) = mock_bootstrap_with_multi_stores(&cluster, 3);
    let store = runtime
        .block_on(tikv_client::transaction::Client::new_with_pd_client(
            Arc::new(pd),
            tikv_client::Config::default(),
        ))
        .unwrap();

    // key range: ['' - 'm' - 'z']
    let region2 = cluster.alloc_id();
    let new_peers = cluster.alloc_ids(3);
    cluster.split(init_region_id, region2, b"m", &new_peers, new_peers[0]);

    let resolver = Arc::new(MockGcWorkerLockResolver::new(BaseRegionLockResolver::new(
        "test", store,
    )));
    let region_location = |region: &tikv_client::proto::metapb::Region| {
        let epoch = region.region_epoch.clone().unwrap_or_default();
        location_of(region.id, epoch.conf_ver, epoch.version)
    };
    let region_by_key = {
        let cluster = cluster.clone();
        move |key: &[u8]| {
            let mut encoded = Vec::new();
            tidb_codec::encode_bytes(&mut encoded, key);
            cluster.region_by_key(&encoded).map(|lookup| lookup.0)
        }
    };
    *resolver.scan_locks.lock().unwrap() = Some(Arc::new(move |_, key| {
        let Some(region) = region_by_key(key) else {
            return (Vec::new(), None);
        };
        // First scans: two locks in each region.
        if region.id == init_region_id {
            return (
                vec![lock(b"a", 0, 0), lock(b"b", 0, 0)],
                Some(region_location(&region)),
            );
        }
        if region.id == region2 {
            return (
                vec![lock(b"o", 0, 0), lock(b"p", 0, 0)],
                Some(region_location(&region)),
            );
        }
        (Vec::new(), None)
    }));
    let first_access = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let resolved_lock: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
    let scan_locks = Arc::clone(&resolver.scan_locks);
    let resolved = Arc::clone(&resolved_lock);
    *resolver.batch_resolve_locks.lock().unwrap() = Some(Arc::new(move |locks, location| {
        let location = location.expect("a location");
        if location.region.id == init_region_id && first_access.swap(false, Ordering::AcqRel) {
            // Merge region2 into region1 and answer EpochNotMatch; the mock
            // PD keeps no region cache to update.
            cluster.merge(init_region_id, region2);
            let merged = cluster.region_by_id(init_region_id).unwrap().0;
            let merged_location = region_location(&merged);
            // Region1 now holds all four locks.
            *scan_locks.lock().unwrap() = Some(Arc::new(move |_, key| {
                if key.is_empty() {
                    let locks = vec![
                        lock(b"a", 0, 0),
                        lock(b"b", 0, 0),
                        lock(b"o", 0, 0),
                        lock(b"p", 0, 0),
                    ];
                    if let Some(i) = locks.iter().position(|lock| key <= lock.key.as_slice()) {
                        return (locks[i..].to_vec(), Some(merged_location.clone()));
                    }
                }
                (Vec::new(), None)
            }));
            return Ok(None);
        }
        resolved
            .lock()
            .unwrap()
            .extend(locks.iter().map(|lock| lock.key.clone()));
        Ok(Some(location))
    }));
    let (_, result) = runtime.block_on(resolve_locks_for_range(
        tikv_client::async_util::Cancellation::default(),
        resolver.as_ref(),
        1,
        Vec::new(),
        b"z".to_vec(),
        &new_gc_resolve_lock_max_backoff,
        10,
    ));
    result.unwrap();
    assert_eq!(
        *resolved_lock.lock().unwrap(),
        vec![b"a".to_vec(), b"b".to_vec(), b"o".to_vec(), b"p".to_vec()]
    );
}

/// Go `TestResolveLocksNearTxnSafePoint`: of three pessimistic locks at
/// the txn safe point - 1, at it and + 1, GC resolves only the first. Of three
/// competing lockers one proceeds at once and two wait in the store's lock
/// waiter until the holders commit, then answer a write conflict.
#[test]
fn test_resolve_locks_near_txn_safe_point() {
    let s = create_gc_worker_suite();
    let current_ts = s.must_alloc_ts();
    let txn_safe_point = go_time_to_ts(time_from_ts(current_ts) - chrono::Duration::minutes(5));

    let mut txns = Vec::new();
    for (i, start_ts) in [txn_safe_point - 1, txn_safe_point, txn_safe_point + 1]
        .into_iter()
        .enumerate()
    {
        txns.push(s.block_on(async {
            let mut txn = s
                .tikv
                .store()
                .begin_with_options(
                    tikv_client::transaction::TransactionOptions::new_pessimistic()
                        .start_timestamp(
                            <tikv_client::Timestamp as tikv_client::TimestampExt>::from_version(
                                start_ts,
                            ),
                        ),
                )
                .await
                .unwrap();
            txn.lock_keys(vec![format!("k{}", i + 1).into_bytes()])
                .await
                .unwrap();
            txn
        }));
    }

    s.job().resolve_locks(txn_safe_point, 1).unwrap();

    // Prevent amending lock behavior by making new write to the keys.
    let (results_tx, results_rx) = std::sync::mpsc::channel();
    for i in 0..3 {
        let store = (**s.tikv.store()).clone();
        let results_tx = results_tx.clone();
        s.tikv.runtime().spawn(async move {
            let mut txn = store.begin_pessimistic().await.unwrap();
            let result = txn
                .lock_keys(vec![format!("k{}", i + 1).into_bytes()])
                .await;
            results_tx.send((result, txn)).unwrap();
        });
    }

    // Only the first transaction in `txns` was rolled back by GC, so one of
    // the competing lockers proceeds while the other two wait.
    let (first, mut first_txn) = results_rx
        .recv_timeout(Duration::from_millis(200))
        .expect("no transaction is resolved, which is not expected");
    first.unwrap();
    assert!(
        results_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "more than one transaction is resolved, which is not expected"
    );

    let mut txns = txns.into_iter();
    assert!(s.block_on(txns.next().unwrap().commit()).is_err());
    s.block_on(txns.next().unwrap().commit()).unwrap();
    s.block_on(txns.next().unwrap().commit()).unwrap();

    let mut others = vec![];
    for _ in 0..2 {
        let (result, txn) = results_rx
            .recv_timeout(Duration::from_millis(200))
            .expect("not all transactions are finished");
        let error = result.expect_err("a write conflict");
        assert!(
            format!("{error:?}").to_lowercase().contains("conflict"),
            "{error:?}"
        );
        others.push(txn);
    }

    // Clear unfinished transactions.
    s.block_on(first_txn.rollback()).unwrap();
    for mut txn in others {
        s.block_on(txn.rollback()).unwrap();
    }
}

#[test]
fn test_unified_gc_needs_to_wait() {
    if tidb_config_kerneltype::is_classic() {
        // The starter deploy mode exists only in the nextgen kernel.
        return;
    }
    let original = tidb_config_deploymode::get();
    let cases = [
        // (deploy mode, in test, finished first job, expected)
        (tidb_config_deploymode::Mode::Starter, true, false, true),
        (tidb_config_deploymode::Mode::Starter, false, false, false),
        (tidb_config_deploymode::Mode::Starter, false, true, true),
        (tidb_config_deploymode::Mode::Premium, false, false, true),
    ];
    for (mode, in_test, finished, expected) in cases {
        tidb_config_deploymode::set(mode).unwrap();
        let mut s = create_gc_worker_suite();
        s.worker.in_test = in_test;
        s.worker.last_finish = Instant::now();
        s.worker.has_finished_first_gc_job = finished;
        assert_eq!(expected, s.worker.needs_to_wait(), "{mode:?}");
    }
    tidb_config_deploymode::set(original).unwrap();
}

fn make_keyspace(id: u32, name: &str, keyspace_level_gc: bool) -> KeyspaceMeta {
    let management_type = if keyspace_level_gc {
        KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE_KEYSPACE_LEVEL
    } else {
        KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE_UNIFIED
    };
    KeyspaceMeta {
        keyspace: Some(keyspace_meta::Keyspace::Id(id)),
        name: name.to_owned(),
        config: HashMap::from([(
            KEYSPACE_CONFIG_GC_MANAGEMENT_TYPE.to_owned(),
            management_type.to_owned(),
        )]),
        ..KeyspaceMeta::default()
    }
}

struct StubGcV2Manager {
    role: &'static str,
    meta: Option<KeyspaceMeta>,
    fail_recycle: bool,
    register_count: Mutex<usize>,
    recycle: Mutex<Vec<u64>>,
}

impl GcV2Manager for StubGcV2Manager {
    fn role(&self) -> tidb_config::external_workload::ExternalWorkloadRole {
        tidb_config::external_workload::ExternalWorkloadRole(self.role.to_owned())
    }

    fn meta(&self) -> Option<KeyspaceMeta> {
        self.meta.clone()
    }

    fn register_gcv2(&self, _safe_point: u64, _life_time: Duration) -> GcResult<()> {
        *self.register_count.lock().unwrap() += 1;
        Ok(())
    }

    fn recycle_gcv2(&self, safe_point: u64) -> GcResult<()> {
        self.recycle.lock().unwrap().push(safe_point);
        if self.fail_recycle {
            return Err(GcError::new("mock recycle GCV2 failure"));
        }
        Ok(())
    }
}

#[test]
fn test_notify_gcv2_after_gc_for_dedicated_worker() {
    const SAFE_POINT: u64 = 123;
    for (name, keyspace_level, fail_recycle, want_recycle) in [
        ("keyspace level GC", true, false, 1),
        ("recycle failure is best effort", true, true, 1),
        ("unified GC", false, false, 0),
    ] {
        let manager = Arc::new(StubGcV2Manager {
            role: tidb_config::external_workload::ROLE_GCV2_WORKER,
            meta: Some(make_keyspace(1, "ks", keyspace_level)),
            fail_recycle,
            register_count: Mutex::new(0),
            recycle: Mutex::new(Vec::new()),
        });
        let s = create_gc_worker_suite_full(None, Vec::new(), Some(manager.clone()));
        s.job().notify_gcv2_after_gc(SAFE_POINT);
        assert_eq!(0, *manager.register_count.lock().unwrap(), "{name}");
        let recycle = manager.recycle.lock().unwrap();
        assert_eq!(want_recycle, recycle.len(), "{name}");
        if want_recycle > 0 {
            assert_eq!(SAFE_POINT, recycle[0], "{name}");
        }
    }
}

/// Go `makeKey`: `key` under keyspace `id`'s txn prefix.
fn make_key(id: u32, key: &str) -> Vec<u8> {
    let mut encoded = vec![b'x'];
    encoded.extend_from_slice(&id.to_be_bytes()[1..]);
    encoded.extend_from_slice(key.as_bytes());
    encoded
}

/// Go `collectAndMergeRanges`: the scan-lock ranges merged where they touch;
/// an empty key stands for the unbounded end.
fn collect_and_merge_ranges(kv: &FakeKv) -> Vec<(Vec<u8>, Vec<u8>, u64)> {
    let mut ranges = kv.scan_lock_requests.lock().unwrap().clone();
    ranges.sort_by(|left, right| left.0.cmp(&right.0));
    let mut merged: Vec<(Vec<u8>, Vec<u8>, u64)> = Vec::new();
    for range in ranges {
        match merged.last_mut() {
            Some(previous) if !previous.1.is_empty() && range.0 <= previous.1 => {
                if range.1.is_empty() || range.1 > previous.1 {
                    previous.1 = range.1;
                }
            }
            _ => merged.push(range),
        }
    }
    merged
}

fn scan_lock_count(kv: &FakeKv) -> usize {
    kv.scan_lock_requests.lock().unwrap().len()
}

fn req(start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>, u64) {
    (start.to_vec(), end.to_vec(), 100)
}

#[test]
fn test_resolve_locks_with_keyspaces_null_keyspace_only() {
    if tidb_config_kerneltype::is_next_gen() {
        return;
    }
    let s = create_gc_worker_suite_with(None, Vec::new());
    s.job().resolve_locks(100, 1).unwrap();
    // With no keyspace in the cluster at all, nothing is excluded.
    assert_eq!(vec![req(b"", b"")], collect_and_merge_ranges(&s.kv));
    assert_eq!(1, scan_lock_count(&s.kv));
}

#[test]
fn test_resolve_locks_with_keyspaces_null_keyspace_only_multi_region() {
    if tidb_config_kerneltype::is_next_gen() {
        return;
    }
    let s = create_gc_worker_suite_with(None, Vec::new());
    s.split_at_keys(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    s.job().resolve_locks(100, 1).unwrap();
    assert_eq!(vec![req(b"", b"")], collect_and_merge_ranges(&s.kv));
    assert_eq!(4, scan_lock_count(&s.kv));
}

#[test]
fn test_resolve_locks_with_keyspaces_null_keyspace_in_multi_keyspace_environment() {
    if tidb_config_kerneltype::is_next_gen() {
        return;
    }
    let s = create_gc_worker_suite_with(
        None,
        vec![make_keyspace(1, "ks1", true), make_keyspace(3, "ks3", true)],
    );
    let mut splits = Vec::new();
    for id in 1..=4 {
        splits.push(make_key(id, ""));
        splits.push(make_key(id, "a"));
    }
    splits.extend([b"t1".to_vec(), b"t2".to_vec(), b"m".to_vec()]);
    s.split_at_keys(&splits);
    s.job().resolve_locks(100, 2).unwrap();
    // The ranges outside every keyspace.
    assert_eq!(
        vec![req(b"", b"r"), req(b"s", b"x"), req(b"y", b"")],
        collect_and_merge_ranges(&s.kv)
    );
    assert_eq!(6, scan_lock_count(&s.kv));
}

#[test]
fn test_resolve_locks_with_keyspaces_non_null_keyspace_in_multi_keyspace_environment() {
    if !tidb_config_kerneltype::is_next_gen() {
        return;
    }
    let system = make_keyspace(MAX_KEYSPACE_ID_FOR_TEST - 1, "SYSTEM", true);
    let s = create_gc_worker_suite_with(
        Some(system.clone()),
        vec![
            make_keyspace(1, "ks1", true),
            make_keyspace(3, "ks3", true),
            system,
        ],
    );
    s.split_at_keys(&[b"a".to_vec()]);
    s.job().resolve_locks(100, 2).unwrap();
    // The keyspace resolves its own unbounded range; the codec adds the prefix.
    assert_eq!(vec![req(b"", b"")], collect_and_merge_ranges(&s.kv));
    assert_eq!(2, scan_lock_count(&s.kv));
}

const MAX_KEYSPACE_ID_FOR_TEST: u32 = 0x00ff_ffff;

#[test]
fn test_resolve_locks_with_keyspaces_unified_gc_in_mixed_usage() {
    if tidb_config_kerneltype::is_next_gen() {
        return;
    }
    let s = create_gc_worker_suite_with(
        None,
        vec![
            make_keyspace(0, "DEFAULT", false),
            make_keyspace(1, "ks1", true),
            make_keyspace(2, "ks2", false),
            make_keyspace(3, "ks3", true),
            make_keyspace(5, "ks5", false),
            make_keyspace(8, "ks8", true),
        ],
    );
    let mut splits = vec![b"t1".to_vec(), b"t2".to_vec(), b"m".to_vec()];
    for id in 0..8 {
        splits.push(make_key(id, ""));
        splits.push(make_key(id, "a"));
    }
    s.split_at_keys(&splits);
    s.job().resolve_locks(100, 2).unwrap();
    assert_eq!(
        vec![
            req(b"", b"r"),
            req(b"s", b"x"),
            req(b"x\x00\x00\x00", b"x\x00\x00\x01"),
            req(b"x\x00\x00\x02", b"x\x00\x00\x03"),
            req(b"x\x00\x00\x05", b"x\x00\x00\x06"),
            req(b"y", b""),
        ],
        collect_and_merge_ranges(&s.kv)
    );
    assert_eq!(12, scan_lock_count(&s.kv));
}

#[test]
fn test_resolve_locks_with_keyspaces_unified_gc_with_max_keyspace_id() {
    if tidb_config_kerneltype::is_next_gen() {
        return;
    }
    let s = create_gc_worker_suite_with(
        None,
        vec![make_keyspace(MAX_KEYSPACE_ID_FOR_TEST, "max", false)],
    );
    s.split_at_keys(&[
        b"t1".to_vec(),
        b"t2".to_vec(),
        b"m".to_vec(),
        make_key(MAX_KEYSPACE_ID_FOR_TEST, ""),
        make_key(MAX_KEYSPACE_ID_FOR_TEST, "a"),
        b"y".to_vec(),
        b"y\x00\x00\x00".to_vec(),
    ]);
    s.job().resolve_locks(100, 2).unwrap();
    assert_eq!(
        vec![req(b"", b"r"), req(b"s", b"x"), req(b"x\xff\xff\xff", b"")],
        collect_and_merge_ranges(&s.kv)
    );
    // The range ["y", "y\x00\x00\x00") is handled twice, as in Go.
    assert_eq!(10, scan_lock_count(&s.kv));
}

fn test_unified_gc_in_multi_batches_of_keyspaces(
    start_id: u32,
    count: u32,
    step: u32,
    load_batch_size: u32,
    expected_batch_count: usize,
) {
    if tidb_config_kerneltype::is_next_gen() {
        return;
    }
    let mut keyspaces = Vec::new();
    let mut splits = vec![b"t1".to_vec(), b"t2".to_vec(), b"m".to_vec()];
    let mut expected = vec![req(b"", b"r"), req(b"s", b"x")];
    for i in 0..count {
        let id = start_id + i * step;
        keyspaces.push(make_keyspace(id, &format!("ks{id}"), false));
        splits.push(make_key(id, ""));
        splits.push(make_key(id, "a"));
        let start = make_key(id, "");
        let end = tikv_client::kv::prefix_next_key(&start);
        let last = expected.last_mut().unwrap();
        if start == last.1 {
            last.1 = end;
        } else {
            expected.push((start, end, 100));
        }
    }
    let last = expected.last_mut().unwrap();
    if last.1.as_slice() >= b"y".as_slice() {
        last.1 = Vec::new();
    } else {
        expected.push(req(b"y", b""));
    }

    let s = create_gc_worker_suite_with(None, keyspaces);
    s.job()
        .load_keyspaces_batch_size
        .store(load_batch_size, Ordering::Release);
    s.split_at_keys(&splits);
    s.job().resolve_locks(100, 10).unwrap();
    assert_eq!(expected, collect_and_merge_ranges(&s.kv));
    assert_eq!(count as usize * 2 + 6, scan_lock_count(&s.kv));
    assert_eq!(
        expected_batch_count,
        s.job().load_keyspaces_batch_count.load(Ordering::Acquire)
    );
}

#[test]
fn test_resolve_locks_with_keyspaces_unified_gc_in_multi_batches_of_keyspaces_8() {
    // Batches: [1,2,3], [4,5,6], [7,8], [].
    test_unified_gc_in_multi_batches_of_keyspaces(1, 8, 1, 3, 4);
}

#[test]
fn test_resolve_locks_with_keyspaces_unified_gc_in_multi_batches_of_keyspaces_last8_step2() {
    // Batches: [max-14, -12, -10], [-8, -6, -4], [-2, max].
    test_unified_gc_in_multi_batches_of_keyspaces(MAX_KEYSPACE_ID_FOR_TEST - 14, 8, 2, 3, 3);
}

#[test]
fn test_resolve_locks_with_keyspaces_unified_gc_in_multi_batches_of_keyspaces_multiple_of_batch_size(
) {
    // Batches: [1,2,3], [4,5,6], [].
    test_unified_gc_in_multi_batches_of_keyspaces(1, 6, 1, 3, 3);
}

#[test]
fn test_resolve_locks_with_keyspaces_unified_gc_in_multi_batches_of_keyspaces_multiple_of_batch_size_to_end(
) {
    // Batches: [max-5, -4, -3], [-2, -1, max].
    test_unified_gc_in_multi_batches_of_keyspaces(MAX_KEYSPACE_ID_FOR_TEST - 5, 6, 1, 3, 2);
}

#[test]
fn test_run_gc_job() {
    let s = create_gc_worker_suite();
    s.set_sync_wait_time(Duration::ZERO);
    let job = s.job();
    let concurrency = GcConcurrency {
        v: 1,
        is_auto: false,
    };

    assert!(job.check_use_distributed_gc());
    let safe_point = s.must_alloc_ts();
    // runGCJob does not advance the txn safe point itself.
    let new = s.advance_txn_safe_point(safe_point).new_txn_safe_point;
    assert_eq!(safe_point, new);
    job.run_gc_job(safe_point, concurrency).unwrap();
    assert_eq!(safe_point, s.gc_state().gc_safe_point);

    job.save_time(GC_SAFE_POINT_KEY, time_from_ts(safe_point))
        .unwrap();
    assert_eq!(
        time_from_ts(safe_point),
        job.load_time(GC_SAFE_POINT_KEY).unwrap().unwrap()
    );
    assert_eq!(safe_point, s.gc_state().txn_safe_point);

    // A regressing safe point fails.
    assert!(job.run_gc_job(safe_point - 1, concurrency).is_err());

    // Central mode falls back to distributed mode.
    job.save_value_to_sys_table(GC_MODE_KEY, GC_MODE_CENTRAL)
        .unwrap();
    assert!(job.check_use_distributed_gc());

    let p = s.create_gc_probe(b"k1");
    let safe_point = s.must_alloc_ts();
    let new = s.advance_txn_safe_point(safe_point).new_txn_safe_point;
    assert_eq!(safe_point, new);
    job.run_gc_job(safe_point, concurrency).unwrap();
    s.check_collected(&p);
    assert_eq!(safe_point, s.load_txn_safe_point());
}

/// Go `TestDoGCForOneRegion`, its first arm: `CmdGC` to the region with a
/// safe point past the probe. The remaining arms inject send failures
/// through client-go's `tikvStoreSendReqResult` failpoint, which the client
/// has no counterpart for.
#[test]
fn test_do_gc_for_one_region() {
    let s = create_gc_worker_suite();
    let region = s
        .tikv
        .runtime()
        .block_on(tikv_client::PdClient::region_for_key(
            s.tikv.store().pd_client().as_ref(),
            &tikv_client::Key::from(Vec::new()),
        ))
        .unwrap()
        .ver_id();

    let p = s.create_gc_probe(b"k1");
    let region_error = s
        .job()
        .do_gc_for_region(s.must_alloc_ts(), &region)
        .unwrap();
    assert_eq!(None, region_error);
    s.check_collected(&p);

    // Go `failpoint.Enable("tikvclient/tikvStoreSendReqResult", ...)`.
    let scenario = fail::FailScenario::setup();
    fail::cfg("tikvStoreSendReqResult", "return(timeout)").unwrap();
    assert!(s
        .job()
        .do_gc_for_region(s.must_alloc_ts(), &region)
        .is_err());
    fail::remove("tikvStoreSendReqResult");

    fail::cfg("tikvStoreSendReqResult", "return(GCNotLeader)").unwrap();
    let region_error = s
        .job()
        .do_gc_for_region(s.must_alloc_ts(), &region)
        .unwrap();
    assert!(region_error.unwrap().not_leader.is_some());
    fail::remove("tikvStoreSendReqResult");

    fail::cfg("tikvStoreSendReqResult", "return(GCServerIsBusy)").unwrap();
    let region_error = s
        .job()
        .do_gc_for_region(s.must_alloc_ts(), &region)
        .unwrap();
    assert!(region_error.unwrap().server_is_busy.is_some());
    fail::remove("tikvStoreSendReqResult");
    scenario.teardown();
}

/// Go `TestRunGCJobAPI`: the store has no locks, so the store's own lock
/// resolver stands in for Go's lock-free mock.
#[test]
fn test_run_gc_job_api() {
    if tidb_config_kerneltype::is_next_gen() {
        return;
    }
    let s = create_gc_worker_suite();
    let p = s.create_gc_probe(b"k1");
    let safe_point = s.must_alloc_ts();
    let deps = s.deps();
    s.set_sync_wait_time(Duration::ZERO);
    run_gc_job(deps, safe_point, "mock", 1).unwrap();
    s.check_collected(&p);
    assert_eq!(safe_point, s.load_txn_safe_point());
}

/// Go `TestRunDistGCJobAPI`.
#[test]
fn test_run_dist_gc_job_api() {
    if tidb_config_kerneltype::is_next_gen() {
        return;
    }
    let s = create_gc_worker_suite();
    let safe_point = s.must_alloc_ts();
    let deps = s.deps();
    s.set_sync_wait_time(Duration::ZERO);
    run_distributed_gc_job(deps, safe_point, "mock", 1).unwrap();
    assert_eq!(safe_point, s.gc_state().gc_safe_point);
    assert_eq!(safe_point, s.load_txn_safe_point());
}

#[test]
fn test_set_service_safe_point() {
    let s = create_gc_worker_suite();
    let job = s.job();
    let must_set_tidb = |safe_point: u64, expected_min: u64| {
        assert_eq!(
            expected_min,
            job.set_gc_worker_service_safe_point(safe_point).unwrap()
        );
    };
    let min_service_safe_point = || s.tikv.update_service_gc_safe_point("test", 0, 0).unwrap();
    let must_update = |service: &str, safe_point: u64, expected_min: u64| {
        assert_eq!(
            expected_min,
            s.tikv
                .update_service_gc_safe_point(service, i64::MAX, safe_point)
                .unwrap()
        );
    };
    let must_remove = |service: &str, safe_point: u64, expected_min: u64| {
        assert_eq!(
            expected_min,
            s.tikv
                .update_service_gc_safe_point(service, 0, safe_point)
                .unwrap()
        );
    };

    // Safe point calculations are based on time rather than ts value.
    let mut safe_point = s.must_alloc_ts();
    must_set_tidb(safe_point, safe_point);
    assert_eq!(safe_point, min_service_safe_point());

    // Advance the service safe point.
    safe_point += 100;
    must_set_tidb(safe_point, safe_point);
    assert_eq!(safe_point, min_service_safe_point());

    // A greater safe point of another service does not matter.
    safe_point += 100;
    must_update("svc1", safe_point + 10, safe_point - 100);
    must_set_tidb(safe_point, safe_point);
    assert_eq!(safe_point, min_service_safe_point());

    // A smaller safe point of another service holds GC back.
    safe_point += 100;
    must_update("svc1", safe_point - 10, safe_point - 100);
    must_set_tidb(safe_point, safe_point - 10);
    assert_eq!(safe_point - 10, min_service_safe_point());

    // Removing the minimum: the compatible wrapper around GC barriers keeps
    // "gc_worker" blocked at `safePoint - 10`.
    must_remove("svc1", safe_point - 10, safe_point - 10);
    assert_eq!(safe_point - 10, min_service_safe_point());
    must_set_tidb(safe_point, safe_point);

    // Many safe points.
    safe_point += 100;
    for i in 0..10u64 {
        must_update(&format!("svc{i}"), safe_point + i * 10, safe_point - 100);
    }
    must_set_tidb(safe_point + 50, safe_point);
}

#[test]
fn test_start_with_run_gc_job_failures() {
    let s = create_gc_worker_suite();
    let done = s.worker.done_tx.clone();
    let handle = s.worker.start();
    // The worker survives failing jobs.
    for _ in 0..3 {
        done.send(Err(GcError::new("mock error"))).unwrap();
    }
    std::thread::sleep(Duration::from_millis(100));
    handle.close();
    drop(handle);
}

#[test]
fn test_gc_placement_rules() {
    let s = create_gc_worker_suite();
    s.sessions.state.lock().unwrap().history_job =
        Some(|job_id| mock_drop_table_job(job_id, 10, ""));

    let cache = Mutex::new(std::collections::HashSet::new());
    let range = DelRangeTask {
        job_id: 1,
        element_id: 10,
        start_key: Vec::new(),
        end_key: Vec::new(),
    };
    let mut session = s.sessions.create_session();
    do_gc_placement_rules(session.as_mut(), s.rules.as_ref(), 1, &range, &cache).unwrap();
    assert!(cache.lock().unwrap().contains(&10));
    // The table's rules go: one empty bundle per physical id. Go's mock job
    // names table 10 as both the table and its old partition.
    assert_eq!(
        vec![vec!["TiDB_DDL_10".to_owned(), "TiDB_DDL_10".to_owned()]],
        *s.rules.put_bundles.lock().unwrap()
    );

    // The same table id again is skipped.
    do_gc_placement_rules(session.as_mut(), s.rules.as_ref(), 1, &range, &cache).unwrap();
    assert!(cache.lock().unwrap().contains(&10));
    assert_eq!(1, s.rules.put_bundles.lock().unwrap().len());
}

#[test]
fn test_gc_label_rules() {
    let s = create_gc_worker_suite();
    s.sessions.state.lock().unwrap().history_job = Some(mock_history_job_for_delete_ranges);
    let range = DelRangeTask {
        job_id: 1,
        element_id: 1,
        start_key: Vec::new(),
        end_key: Vec::new(),
    };
    s.job().do_gc_label_rules(&range).unwrap();
}

#[test]
fn test_calc_delete_range_concurrency() {
    let cases = [
        ("Auto: Low concurrency, few ranges", 16, true, 50_000, 1),
        (
            "Auto: High concurrency, many ranges",
            400,
            true,
            1_000_000,
            10,
        ),
        ("Auto: High concurrency, few ranges", 400, true, 50_000, 1),
        ("Auto: Low concurrency, many ranges", 16, true, 1_000_000, 4),
        ("Non-auto: Low concurrency", 16, false, 1_000_000, 4),
        ("Non-auto: High concurrency", 400, false, 50_000, 100),
        ("Edge case: Zero concurrency", 0, true, 100_000, 1),
        ("Edge case: Zero ranges", 100, true, 0, 1),
        ("Large range number", 400, true, 10_000_000, 100),
        ("Exact RequestsPerThread", 400, true, 200_000, 2),
    ];
    for (name, v, is_auto, range_num, expected) in cases {
        let result = calc_delete_range_concurrency(GcConcurrency { v, is_auto }, range_num);
        assert_eq!(expected, result, "{name}");
        assert!(result >= 1, "{name}");
    }
}

#[test]
fn leader_desc_renders_the_start_time_as_go_time_string() {
    use chrono::TimeZone;
    let time = chrono::Local
        .with_ymd_and_hms(2026, 10, 19, 12, 12, 25)
        .unwrap()
        + chrono::Duration::microseconds(258_428);
    let rendered = go_time_string(time);
    // The fraction loses its trailing zeros; the offset and a zone name follow.
    assert!(
        rendered.starts_with("2026-10-19 12:12:25.258428 "),
        "{rendered}"
    );
    assert_eq!(4, rendered.split(' ').count(), "{rendered}");
    let whole = chrono::Local
        .with_ymd_and_hms(2026, 10, 19, 12, 12, 25)
        .unwrap();
    assert!(
        go_time_string(whole).starts_with("2026-10-19 12:12:25 "),
        "a whole second prints no fraction"
    );
}
