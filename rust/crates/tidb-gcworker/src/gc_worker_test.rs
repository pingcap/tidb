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

//! Go `pkg/store/gcworker/gc_worker_test.go`, the tests that need the
//! worker's collaborators only: `mysql.tidb` and the delete-range tables in a
//! fake session, PD's GC state in the client's mock PD (client-go
//! `mocktikv`), and a recording TiKV client standing in for Go's
//! `mockGCWorkerClient` and `mockGCWorkerLockResolver`. The tests that read
//! MVCC data back through a store (`TestDoGCForOneRegion`,
//! `TestResolveLockRange*`, `TestResolveLocksNearTxnSafePoint`,
//! `TestRunGCJobAPI`, `TestRunDistGCJobAPI`, `TestGCWithPendingTxn*`,
//! `TestSkipGCAndOnlyResolveLock`, and the probe checks of `TestLeaderTick`
//! and `TestRunGCJob`) run against the server's store adapters.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tikv_client::mock::mocktikv::{bootstrap_with_multi_stores, Cluster, MockEngine, MockPdClient};
use tikv_client::proto::keyspacepb::{keyspace_meta, KeyspaceMeta};
use tikv_client::proto::metapb;

use crate::gc_worker::*;
use crate::seams::*;

/// Go `oracles.MockOracle`: wall-clock time plus an offset tests add to.
struct FakeStore {
    offset_ms: AtomicI64,
    logical: AtomicU64,
    keyspace: Option<KeyspaceMeta>,
}

impl FakeStore {
    fn add_offset(&self, offset: Duration) {
        self.offset_ms
            .fetch_add(offset.as_millis() as i64, Ordering::AcqRel);
    }
}

impl GcStore for FakeStore {
    fn current_version(&self) -> GcResult<u64> {
        let physical =
            chrono::Utc::now().timestamp_millis() + self.offset_ms.load(Ordering::Acquire);
        let logical = self.logical.fetch_add(1, Ordering::AcqRel) % (1 << 18);
        Ok(((physical as u64) << 18) + logical)
    }

    fn keyspace_meta(&self) -> Option<KeyspaceMeta> {
        self.keyspace.clone()
    }
}

/// PD's GC state, as client-go's mock PD keeps it.
struct FakePd {
    cluster: Cluster,
    pd: MockPdClient,
    keyspaces: Vec<KeyspaceMeta>,
}

impl GcPdClient for FakePd {
    fn get_all_stores(&self) -> GcResult<Vec<metapb::Store>> {
        Ok(self.cluster.all_stores())
    }

    fn update_gc_safe_point(&self, safe_point: u64) -> GcResult<u64> {
        Ok(self.pd.update_gc_safe_point(safe_point))
    }

    fn update_service_gc_safe_point(
        &self,
        service_id: &str,
        ttl: i64,
        safe_point: u64,
    ) -> GcResult<u64> {
        // Go unistore's `gcStatesManagerSimulator.UpdateServiceGCSafePoint`,
        // PD's compatibility wrapper: "gc_worker" advances the txn safe
        // point, other services set or delete a GC barrier, and the answer
        // is the txn safe point.
        if service_id == "gc_worker" {
            if ttl != i64::MAX {
                return Err(GcError::new(
                    "ttl of gc_worker's service safe point must be math.MaxInt64",
                ));
            }
            return Ok(self.advance_txn_safe_point(safe_point)?.new_txn_safe_point);
        }
        if ttl > 0 {
            let behind = safe_point < self.pd.gc_state().transaction_safe_point;
            if let Err(error) = self.pd.set_gc_barrier(service_id, safe_point) {
                if behind {
                    return Err(GcError::new(error.to_string()));
                }
            }
        } else {
            self.pd.delete_gc_barrier(service_id);
        }
        Ok(self.pd.gc_state().transaction_safe_point)
    }

    fn advance_txn_safe_point(&self, target: u64) -> GcResult<AdvanceTxnSafePointResult> {
        match self.pd.advance_transaction_safe_point(target) {
            Ok((old, new, blocker)) => Ok(AdvanceTxnSafePointResult {
                old_txn_safe_point: old,
                target,
                new_txn_safe_point: new,
                blocker_description: blocker,
            }),
            // PD reports a decreasing target under this error code.
            Err(error) => Err(GcError::new(format!(
                "[PD:gc:ErrDecreasingTxnSafePoint]{error}"
            ))),
        }
    }

    fn advance_gc_safe_point(&self, target: u64) -> GcResult<AdvanceGcSafePointResult> {
        self.pd
            .advance_gc_safe_point(target)
            .map(|(old, new)| AdvanceGcSafePointResult {
                old_gc_safe_point: old,
                target,
                new_gc_safe_point: new,
            })
            .map_err(|error| GcError::new(error.to_string()))
    }

    fn get_all_keyspaces(&self, start_id: u32, limit: u32) -> GcResult<Vec<KeyspaceMeta>> {
        let mut keyspaces: Vec<_> = self
            .keyspaces
            .iter()
            .filter(|meta| keyspace_id(meta) >= start_id)
            .cloned()
            .collect();
        keyspaces.sort_by_key(keyspace_id);
        keyspaces.truncate(limit as usize);
        Ok(keyspaces)
    }
}

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

/// Go `mockGCWorkerClient` and the lock resolver: records what is sent.
#[derive(Default)]
struct FakeKv {
    destroy_handler: Mutex<Option<DestroyHandler>>,
    destroy_requests: Mutex<Vec<DestroyRequest>>,
    delete_range_requests: Mutex<Vec<(Vec<u8>, Vec<u8>)>>,
    /// Region boundaries, as Go's `splitAtKeys` leaves them.
    split_keys: Mutex<Vec<Vec<u8>>>,
    /// One `(start, end, txn safe point)` per scan-lock request, as Go's
    /// `scanLockRequestHandler` collects them.
    scan_lock_requests: Mutex<Vec<ScanLockRequest>>,
}

struct RecordingRunner<'a> {
    kv: &'a FakeKv,
    max_version: u64,
    completed: std::sync::atomic::AtomicUsize,
}

impl ResolveLocksRunner for RecordingRunner<'_> {
    fn run_on_range(&self, start: &[u8], end: &[u8]) -> GcResult<()> {
        let splits = self.kv.split_keys.lock().unwrap().clone();
        let mut piece_start = start.to_vec();
        for split in splits
            .iter()
            .filter(|split| split.as_slice() > start && (end.is_empty() || split.as_slice() < end))
        {
            self.kv.scan_lock_requests.lock().unwrap().push((
                piece_start.clone(),
                split.clone(),
                self.max_version + 1,
            ));
            self.completed.fetch_add(1, Ordering::AcqRel);
            piece_start = split.clone();
        }
        self.kv.scan_lock_requests.lock().unwrap().push((
            piece_start,
            end.to_vec(),
            self.max_version + 1,
        ));
        self.completed.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    fn completed_regions(&self) -> usize {
        self.completed.load(Ordering::Acquire)
    }
}

impl GcKvClient for FakeKv {
    fn unsafe_destroy_range(
        &self,
        address: &str,
        start: &[u8],
        end: &[u8],
        _timeout: Duration,
    ) -> GcResult<Option<String>> {
        self.destroy_requests.lock().unwrap().push((
            address.to_owned(),
            start.to_vec(),
            end.to_vec(),
        ));
        match &*self.destroy_handler.lock().unwrap() {
            Some(handler) => handler(address, start, end),
            None => Ok(Some(String::new())),
        }
    }

    fn delete_range_task(&self, start: &[u8], end: &[u8], _concurrency: usize) -> GcResult<()> {
        self.delete_range_requests
            .lock()
            .unwrap()
            .push((start.to_vec(), end.to_vec()));
        Ok(())
    }

    fn resolve_locks_runner(
        &self,
        _name: &str,
        _concurrency: usize,
        max_version: u64,
        _scan_limit: u32,
    ) -> Box<dyn ResolveLocksRunner + '_> {
        Box::new(RecordingRunner {
            kv: self,
            max_version,
            completed: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn gc_region(&self, _region_key: &[u8], _safe_point: u64) -> GcResult<Option<String>> {
        Ok(None)
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
    label_rules: Mutex<HashMap<String, tidb_executor::ddl_label::Rule>>,
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
    ) -> GcResult<HashMap<String, tidb_executor::ddl_label::Rule>> {
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

/// Go `mockGCWorkerSuite`.
struct Suite {
    store: Arc<FakeStore>,
    pd: Arc<FakePd>,
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
    let cluster = Cluster::new(MockEngine::new());
    bootstrap_with_multi_stores(&cluster, 3);
    let store = Arc::new(FakeStore {
        offset_ms: AtomicI64::new(0),
        logical: AtomicU64::new(0),
        keyspace: current_keyspace,
    });
    let pd = Arc::new(FakePd {
        pd: MockPdClient::new(cluster.clone()),
        cluster,
        keyspaces,
    });
    let kv = Arc::new(FakeKv::default());
    let sessions = Arc::new(FakeSessions::default());
    let rules = Arc::new(FakeRules::default());
    let mut worker = GcWorker::new(GcWorkerDeps {
        store: store.clone(),
        pd: pd.clone(),
        kv: kv.clone(),
        sessions: sessions.clone(),
        rules: rules.clone(),
        gcv2: None,
    })
    .unwrap();
    worker.tick();
    Suite {
        store,
        pd,
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

    fn job_mut(&mut self) -> &mut GcJobContext {
        Arc::get_mut(&mut self.worker.job).expect("no GC job is running")
    }

    /// Go `mustAllocTs`.
    fn must_alloc_ts(&self) -> u64 {
        self.store.current_version().unwrap()
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

    s.store.add_offset(Duration::from_secs(10));
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
    s.store.add_offset(4 * MINUTE);
    assert!(!job.prepare().unwrap().0);
    s.store.add_offset(2 * MINUTE);
    assert!(job.prepare().unwrap().0);

    // Change the GC life time.
    job.save_duration(GC_LIFE_TIME_KEY, 30 * MINUTE_NANOS)
        .unwrap();
    s.store.add_offset(5 * MINUTE);
    assert!(!job.prepare().unwrap().0);
    s.store.add_offset(40 * MINUTE);
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
    s.store.add_offset(40 * MINUTE);
    job.save_value_to_sys_table(GC_ENABLE_KEY, BOOLEAN_FALSE)
        .unwrap();
    assert!(!job.prepare().unwrap().0);
    job.save_value_to_sys_table(GC_ENABLE_KEY, BOOLEAN_TRUE)
        .unwrap();
    assert!(job.prepare().unwrap().0);

    // A GC life time below the minimum.
    s.store.add_offset(40 * MINUTE);
    job.save_duration(GC_LIFE_TIME_KEY, MINUTE_NANOS).unwrap();
    assert!(job.prepare().unwrap().0);
    let life_time = job.load_duration(GC_LIFE_TIME_KEY).unwrap().unwrap();
    assert_eq!(GC_MIN_LIFE_TIME, life_time);

    s.store.add_offset(40 * MINUTE);
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
    let last_txn_safe_point = s.pd.pd.gc_state().transaction_safe_point;
    assert_ne!(0, last_txn_safe_point);
    s.pd.pd.set_gc_barrier("a", last_txn_safe_point).unwrap();
    s.store.add_offset(40 * MINUTE);
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
    assert_ne!(s.pd.cluster.all_stores().len(), concurrency_config);
    job.save_value_to_sys_table(GC_CONCURRENCY_KEY, &concurrency_config.to_string())
        .unwrap();

    job.save_value_to_sys_table(GC_AUTO_CONCURRENCY_KEY, BOOLEAN_FALSE)
        .unwrap();
    assert_eq!(concurrency_config, job.get_gc_concurrency().unwrap().v);

    job.save_value_to_sys_table(GC_AUTO_CONCURRENCY_KEY, BOOLEAN_TRUE)
        .unwrap();
    assert_eq!(
        s.pd.cluster.all_stores().len(),
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
    s.job_mut().txn_safe_point_sync_wait_time = Duration::ZERO;

    let very_long = Duration::from_nanos((GC_DEFAULT_LIFE_TIME * 10) as u64);
    // Avoid failing at the interval check; `lastFinish` is checked by OS time.
    s.worker.last_finish = Instant::now() - very_long;
    // Central mode is configured, and distributed mode runs anyway.
    s.job()
        .save_value_to_sys_table(GC_MODE_KEY, GC_MODE_CENTRAL)
        .unwrap();
    s.store
        .add_offset(Duration::from_nanos((GC_DEFAULT_LIFE_TIME * 2) as u64));
    let reset_last_run = |s: &Suite| {
        let long_ago =
            time_from_ts(s.must_alloc_ts()) - chrono::Duration::from_std(very_long).unwrap();
        s.job().save_time(GC_LAST_RUN_TIME_KEY, long_ago).unwrap();
    };
    let gc_safe_point = |s: &Suite| s.pd.pd.gc_state().gc_safe_point;

    // Skipped while GC is running.
    let before = gc_safe_point(&s);
    s.worker.gc_is_running = true;
    s.worker.leader_tick().unwrap();
    assert!(s.worker.done_rx.try_recv().is_err());
    s.worker.gc_is_running = false;
    reset_last_run(&s);

    // Skipped when prepare fails (disabled GC makes it answer false).
    s.job()
        .save_value_to_sys_table(GC_ENABLE_KEY, BOOLEAN_FALSE)
        .unwrap();
    s.worker.leader_tick().unwrap();
    assert!(!s.worker.gc_is_running);
    s.job()
        .save_value_to_sys_table(GC_ENABLE_KEY, BOOLEAN_TRUE)
        .unwrap();
    reset_last_run(&s);

    // Skipped when gcWaitTime has not passed.
    s.worker.last_finish = Instant::now();
    s.worker.leader_tick().unwrap();
    assert!(!s.worker.gc_is_running);
    s.worker.last_finish = Instant::now() - very_long;
    reset_last_run(&s);
    // The skipped tick's prepare advanced PD's txn safe point; move the
    // oracle so the next target is strictly higher.
    s.store.add_offset(Duration::from_secs(1));
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
    let first = gc_safe_point(&s);
    assert!(first > before);

    // Again, so the hand-off between threads is right.
    reset_last_run(&s);
    s.worker.last_finish = Instant::now() - very_long;
    s.store
        .add_offset(Duration::from_nanos((GC_DEFAULT_LIFE_TIME * 2) as u64));
    s.worker.leader_tick().unwrap();
    s.worker
        .done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the GC job finishes")
        .unwrap();
    s.worker.gc_is_running = false;
    assert!(gc_safe_point(&s) > first);

    // No other signal.
    assert!(s
        .worker
        .done_rx
        .recv_timeout(Duration::from_secs(1))
        .is_err());
}

#[test]
fn test_unified_gc_needs_to_wait() {
    if tidb_config::kerneltype::is_classic() {
        // The starter deploy mode exists only in the nextgen kernel.
        return;
    }
    let original = tidb_config::deploymode::get();
    let cases = [
        // (deploy mode, in test, finished first job, expected)
        (tidb_config::deploymode::Mode::Starter, true, false, true),
        (tidb_config::deploymode::Mode::Starter, false, false, false),
        (tidb_config::deploymode::Mode::Starter, false, true, true),
        (tidb_config::deploymode::Mode::Premium, false, false, true),
    ];
    for (mode, in_test, finished, expected) in cases {
        tidb_config::deploymode::set(mode).unwrap();
        let mut s = create_gc_worker_suite();
        s.worker.in_test = in_test;
        s.worker.last_finish = Instant::now();
        s.worker.has_finished_first_gc_job = finished;
        assert_eq!(expected, s.worker.needs_to_wait(), "{mode:?}");
    }
    tidb_config::deploymode::set(original).unwrap();
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
        let mut s = create_gc_worker_suite();
        let manager = Arc::new(StubGcV2Manager {
            role: tidb_config::external_workload::ROLE_GCV2_WORKER,
            meta: Some(make_keyspace(1, "ks", keyspace_level)),
            fail_recycle,
            register_count: Mutex::new(0),
            recycle: Mutex::new(Vec::new()),
        });
        s.job_mut().deps.gcv2 = Some(manager.clone());
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

fn split_at_keys(kv: &FakeKv, keys: &[Vec<u8>]) {
    let mut splits = kv.split_keys.lock().unwrap();
    splits.extend(keys.iter().cloned());
    splits.sort();
    splits.dedup();
}

fn req(start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>, u64) {
    (start.to_vec(), end.to_vec(), 100)
}

#[test]
fn test_resolve_locks_with_keyspaces_null_keyspace_only() {
    if tidb_config::kerneltype::is_next_gen() {
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
    if tidb_config::kerneltype::is_next_gen() {
        return;
    }
    let s = create_gc_worker_suite_with(None, Vec::new());
    split_at_keys(&s.kv, &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    s.job().resolve_locks(100, 1).unwrap();
    assert_eq!(vec![req(b"", b"")], collect_and_merge_ranges(&s.kv));
    assert_eq!(4, scan_lock_count(&s.kv));
}

#[test]
fn test_resolve_locks_with_keyspaces_null_keyspace_in_multi_keyspace_environment() {
    if tidb_config::kerneltype::is_next_gen() {
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
    split_at_keys(&s.kv, &splits);
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
    if !tidb_config::kerneltype::is_next_gen() {
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
    split_at_keys(&s.kv, &[b"a".to_vec()]);
    s.job().resolve_locks(100, 2).unwrap();
    // The keyspace resolves its own unbounded range; the codec adds the prefix.
    assert_eq!(vec![req(b"", b"")], collect_and_merge_ranges(&s.kv));
    assert_eq!(2, scan_lock_count(&s.kv));
}

const MAX_KEYSPACE_ID_FOR_TEST: u32 = 0x00ff_ffff;

#[test]
fn test_resolve_locks_with_keyspaces_unified_gc_in_mixed_usage() {
    if tidb_config::kerneltype::is_next_gen() {
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
    split_at_keys(&s.kv, &splits);
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
    if tidb_config::kerneltype::is_next_gen() {
        return;
    }
    let s = create_gc_worker_suite_with(
        None,
        vec![make_keyspace(MAX_KEYSPACE_ID_FOR_TEST, "max", false)],
    );
    split_at_keys(
        &s.kv,
        &[
            b"t1".to_vec(),
            b"t2".to_vec(),
            b"m".to_vec(),
            make_key(MAX_KEYSPACE_ID_FOR_TEST, ""),
            make_key(MAX_KEYSPACE_ID_FOR_TEST, "a"),
            b"y".to_vec(),
            b"y\x00\x00\x00".to_vec(),
        ],
    );
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
    if tidb_config::kerneltype::is_next_gen() {
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

    let mut s = create_gc_worker_suite_with(None, keyspaces);
    s.job_mut().load_keyspaces_batch_size = load_batch_size;
    split_at_keys(&s.kv, &splits);
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
    let mut s = create_gc_worker_suite();
    s.job_mut().txn_safe_point_sync_wait_time = Duration::ZERO;
    let job = s.job();
    let concurrency = GcConcurrency {
        v: 1,
        is_auto: false,
    };

    assert!(job.check_use_distributed_gc());
    let safe_point = s.must_alloc_ts();
    // runGCJob does not advance the txn safe point itself.
    let (_, new, _) = s.pd.pd.advance_transaction_safe_point(safe_point).unwrap();
    assert_eq!(safe_point, new);
    job.run_gc_job(safe_point, concurrency).unwrap();
    assert_eq!(safe_point, s.pd.pd.gc_state().gc_safe_point);

    job.save_time(GC_SAFE_POINT_KEY, time_from_ts(safe_point))
        .unwrap();
    assert_eq!(
        time_from_ts(safe_point),
        job.load_time(GC_SAFE_POINT_KEY).unwrap().unwrap()
    );
    assert_eq!(safe_point, s.pd.pd.gc_state().transaction_safe_point);

    // A regressing safe point fails.
    assert!(job.run_gc_job(safe_point - 1, concurrency).is_err());

    // Central mode falls back to distributed mode.
    job.save_value_to_sys_table(GC_MODE_KEY, GC_MODE_CENTRAL)
        .unwrap();
    assert!(job.check_use_distributed_gc());

    let safe_point = s.must_alloc_ts();
    let (_, new, _) = s.pd.pd.advance_transaction_safe_point(safe_point).unwrap();
    assert_eq!(safe_point, new);
    job.run_gc_job(safe_point, concurrency).unwrap();
    assert_eq!(safe_point, s.pd.pd.gc_state().transaction_safe_point);
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
    let min_service_safe_point = || s.pd.update_service_gc_safe_point("test", 0, 0).unwrap();
    let must_update = |service: &str, safe_point: u64, expected_min: u64| {
        assert_eq!(
            expected_min,
            s.pd.update_service_gc_safe_point(service, i64::MAX, safe_point)
                .unwrap()
        );
    };
    let must_remove = |service: &str, safe_point: u64, expected_min: u64| {
        assert_eq!(
            expected_min,
            s.pd.update_service_gc_safe_point(service, 0, safe_point)
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
