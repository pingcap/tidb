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


//! Go `streamhelper/advancer_test.go`, `subscription_test.go`,
//! `regioniter_test.go` and `basic_lib_for_test.go` over the fake cluster.

mod fakecluster;

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fakecluster::{Cluster, StoreHook};
use tidb_gocontext::Context;
use tidb_proto::backup::{storage_backend, Local, StorageBackend, StreamBackupTaskInfo};

use super::advancer::{CheckpointAdvancer, ResolveLockPlan};
use super::advancer_cliext::{EventType, StreamMeta, TaskEvent};
use super::advancer_env::{LockResolverEnv, LogBackupClient, LogBackupFlushIntervalGetter, LogBackupService};
use super::client::PauseTaskOption;
use super::collector::ClusterCollector;
use super::config::{CommandConfig, Config};
use super::flush_subscriber::FlushSubscriber;
use super::models::Range;
use super::regioniter::{iterate_region, RegionWithLeader, Store, TiKVClusterMeta};
use crate::errors::{self, Result};
use crate::spans::sorted::ValuedFull;
use crate::spans::value_sorted::{sorted, ValueSortedFull};

const SPLITS: &[&str] = &["01", "02", "022", "023", "033", "04", "043"];

/// Go `testEnv`.
struct TestEnv {
    cluster: Arc<Cluster>,
    checkpoint: Mutex<u64>,
    pd_disconnected: AtomicBool,
    ranges: Vec<Range>,
    task_ch: Mutex<Option<crossbeam_channel::Sender<TaskEvent>>>,
    task: Mutex<TaskEvent>,
}

fn whole_task(ranges: &[Range], start_ts: u64) -> TaskEvent {
    TaskEvent {
        type_: EventType::Add,
        name: "whole".into(),
        info: Some(StreamBackupTaskInfo { name: "whole".into(), start_ts, ..Default::default() }),
        ranges: if ranges.is_empty() { vec![Range::default()] } else { ranges.to_vec() },
        err: None,
    }
}

impl TestEnv {
    /// Go `newTestEnv`.
    fn new(cluster: &Arc<Cluster>) -> Arc<Self> {
        Self::with_ranges(cluster, Vec::new(), 0)
    }

    fn with_ranges(cluster: &Arc<Cluster>, ranges: Vec<Range>, start_ts: u64) -> Arc<Self> {
        let task = whole_task(&ranges, start_ts);
        Arc::new(Self {
            cluster: Arc::clone(cluster),
            checkpoint: Mutex::new(0),
            pd_disconnected: AtomicBool::new(false),
            ranges,
            task_ch: Mutex::new(None),
            task: Mutex::new(task),
        })
    }

    fn get_checkpoint(&self) -> u64 {
        *self.checkpoint.lock().unwrap()
    }

    fn send(&self, e: TaskEvent) {
        let ch = self.task_ch.lock().unwrap().clone().expect("the task listener has begun");
        ch.send(e).unwrap();
    }

    fn unregister_task(&self) {
        self.send(TaskEvent { type_: EventType::Del, name: "whole".into(), info: None, ranges: Vec::new(), err: None });
    }

    fn put_task(&self) {
        self.send(whole_task(&self.ranges, 0));
    }

    fn resume_task(&self) {
        self.send(TaskEvent { type_: EventType::Resume, name: "whole".into(), info: None, ranges: Vec::new(), err: None });
    }

    fn pause(&self) {
        self.send(TaskEvent { type_: EventType::Pause, name: "whole".into(), info: None, ranges: Vec::new(), err: None });
    }
}

impl StreamMeta for TestEnv {
    fn begin(&self, _ctx: &Context, ch: crossbeam_channel::Sender<TaskEvent>) -> Result<()> {
        ch.send(self.task.lock().unwrap().clone()).unwrap();
        *self.task_ch.lock().unwrap() = Some(ch);
        Ok(())
    }

    fn upload_v3_global_checkpoint_for_task(&self, _ctx: &Context, _task_name: &str, checkpoint: u64) -> Result<()> {
        let mut cp = self.checkpoint.lock().unwrap();
        if checkpoint < *cp {
            return Err(errors::errorf("checkpoint rolling back"));
        }
        *cp = checkpoint;
        Ok(())
    }

    fn get_global_checkpoint_for_task(&self, _ctx: &Context, _task_name: &str) -> Result<u64> {
        if self.pd_disconnected.swap(false, Ordering::SeqCst) {
            return Err(errors::grpc(tonic::Status::unavailable("pd disconnected")));
        }
        Ok(self.get_checkpoint())
    }

    fn clear_v3_global_checkpoint_for_task(&self, _ctx: &Context, _task_name: &str) -> Result<()> {
        *self.checkpoint.lock().unwrap() = 0;
        Ok(())
    }

    fn pause_task(&self, _ctx: &Context, task_name: &str, _opts: Vec<PauseTaskOption>) -> Result<()> {
        self.send(TaskEvent { type_: EventType::Pause, name: task_name.into(), info: None, ranges: Vec::new(), err: None });
        Ok(())
    }
}

impl TiKVClusterMeta for TestEnv {
    fn region_scan(&self, ctx: &Context, key: &[u8], end_key: &[u8], limit: usize) -> Result<Vec<RegionWithLeader>> {
        self.cluster.region_scan(ctx, key, end_key, limit)
    }
    fn stores(&self, ctx: &Context) -> Result<Vec<Store>> {
        self.cluster.stores(ctx)
    }
    fn block_gc_until(&self, ctx: &Context, at: u64) -> Result<u64> {
        self.cluster.block_gc_until(ctx, at)
    }
    fn unblock_gc(&self, ctx: &Context) -> Result<()> {
        self.cluster.unblock_gc(ctx)
    }
    fn fetch_current_ts(&self, ctx: &Context) -> Result<u64> {
        self.cluster.fetch_current_ts(ctx)
    }
}

impl LogBackupService for TestEnv {
    fn get_log_backup_client(&self, ctx: &Context, store_id: u64) -> Result<Arc<dyn LogBackupClient>> {
        self.cluster.get_log_backup_client(ctx, store_id)
    }
    fn clear_cache(&self, ctx: &Context, store_id: u64) -> Result<()> {
        self.cluster.clear_cache(ctx, store_id)
    }
}

impl LockResolverEnv for TestEnv {
    fn resolve_locks_on_range(&self, _ctx: &Context, _start_key: &[u8], _end_key: &[u8], _plan: ResolveLockPlan) -> Result<()> {
        Ok(())
    }
}

impl LogBackupFlushIntervalGetter for TestEnv {
    fn get_log_backup_flush_interval(&self, _ctx: &Context) -> Result<Duration> {
        Ok(CommandConfig::default().get_resolve_lock_interval())
    }
}

/// Go `NewCheckpointAdvancer`: the TiDB config reads a process-wide lag
/// limit the lag tests change, so the tests use the command config.
fn new_advancer(env: &Arc<TestEnv>) -> Arc<CheckpointAdvancer> {
    CheckpointAdvancer::new_command(Arc::clone(env) as Arc<dyn super::Env>)
}

/// Go `UpdateCheckPointLagLimit` on a command-config advancer.
fn update_check_point_lag_limit(adv: &CheckpointAdvancer, limit: Duration) {
    adv.update_config(Arc::new(CommandConfig { check_point_lag_limit: limit, ..CommandConfig::default() }));
}

/// Go `require.Eventually`.
fn eventually(timeout: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("condition not met within {timeout:?}");
}

/// Go `shouldFinishInTime`.
fn should_finish_in_time<T: Send + 'static>(d: Duration, name: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = crossbeam_channel::bounded(1);
    std::thread::spawn(move || {
        let _ = tx.send(f());
    });
    rx.recv_timeout(d).unwrap_or_else(|_| panic!("{name} should finish in {d:?}, but not"))
}

fn grpc_err(code: tonic::Code, msg: &str) -> errors::Error {
    errors::grpc(tonic::Status::new(code, msg))
}

/// Go `oneStoreFailure`.
fn one_store_failure() -> StoreHook {
    let victim = Mutex::new(0u64);
    Arc::new(move |u| {
        let mut v = victim.lock().unwrap();
        if *v == 0 {
            *v = u;
        }
        if *v == u {
            return Err(grpc_err(
                tonic::Code::NotFound,
                "The place once lit by the warm lamplight has been swallowed up by the debris now.",
            ));
        }
        Ok(())
    })
}

/// Go `TestBasic`.
#[test]
fn test_basic() {
    let c = Cluster::new_basic(4, false);
    c.split_and_scatter(SPLITS);
    let ctx = Context::background();
    let min_checkpoint = c.advance_checkpoints();
    let env = TestEnv::new(&c);
    let adv = new_advancer(&env);
    let coll = ClusterCollector::new(&ctx, Arc::clone(&env) as Arc<dyn LogBackupService>);
    adv.get_checkpoint_in_range(&ctx, &[], &[], &coll).unwrap();
    let r = coll.finish(&ctx).unwrap();
    assert!(r.failure_sub_ranges.is_empty());
    assert_eq!(r.checkpoint, min_checkpoint);
}

/// Go `TestTick`.
#[test]
fn test_tick() {
    let c = Cluster::new_basic(4, false);
    c.split_and_scatter(SPLITS);
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let env = TestEnv::new(&c);
    let adv = new_advancer(&env);
    adv.start_task_listener(&ctx);
    adv.on_tick(&ctx).unwrap();
    for _ in 0..5 {
        let cp = c.advance_checkpoints();
        adv.on_tick(&ctx).unwrap();
        assert_eq!(env.get_checkpoint(), cp);
    }
    cancel.cancel();
}

/// Go `countingStorage` and `writeRecordStorage`: counts closes and
/// writes; `fail_writes` is Go's `writeFailStorage`.
struct RecordStorage {
    inner: Arc<dyn tidb_objstore::storeapi::Storage>,
    close_count: Arc<AtomicI32>,
    write_count: Arc<AtomicI32>,
    on_write: Option<Box<dyn Fn() + Send + Sync>>,
    fail_writes: bool,
}

impl tidb_objstore::storeapi::Storage for RecordStorage {
    fn write_file(&self, ctx: &Context, name: &str, data: &[u8]) -> tidb_objstore::error::Result<()> {
        if let Some(on_write) = &self.on_write {
            on_write();
        }
        if self.fail_writes {
            return Err(tidb_objstore::Error::other("injected external storage error"));
        }
        self.write_count.fetch_add(1, Ordering::SeqCst);
        self.inner.write_file(ctx, name, data)
    }
    fn read_file(&self, ctx: &Context, name: &str) -> tidb_objstore::error::Result<Vec<u8>> {
        self.inner.read_file(ctx, name)
    }
    fn file_exists(&self, ctx: &Context, name: &str) -> tidb_objstore::error::Result<bool> {
        self.inner.file_exists(ctx, name)
    }
    fn delete_file(&self, ctx: &Context, name: &str) -> tidb_objstore::error::Result<()> {
        self.inner.delete_file(ctx, name)
    }
    fn open(
        &self,
        ctx: &Context,
        path: &str,
        option: Option<&tidb_objstore::storeapi::ReaderOption>,
    ) -> tidb_objstore::error::Result<Box<dyn tidb_objstore::objectio::Reader>> {
        self.inner.open(ctx, path, option)
    }
    fn delete_files(&self, ctx: &Context, names: &[String]) -> tidb_objstore::error::Result<()> {
        self.inner.delete_files(ctx, names)
    }
    fn walk_dir(
        &self,
        ctx: &Context,
        opt: Option<&tidb_objstore::storeapi::WalkOption>,
        f: &mut dyn FnMut(&str, i64) -> tidb_objstore::error::Result<()>,
    ) -> tidb_objstore::error::Result<()> {
        self.inner.walk_dir(ctx, opt, f)
    }
    fn uri(&self) -> String {
        self.inner.uri()
    }
    fn create(
        &self,
        ctx: &Context,
        path: &str,
        option: Option<&tidb_objstore::storeapi::WriterOption>,
    ) -> tidb_objstore::error::Result<Box<dyn tidb_objstore::objectio::Writer>> {
        self.inner.create(ctx, path, option)
    }
    fn rename(&self, ctx: &Context, old_file_name: &str, new_file_name: &str) -> tidb_objstore::error::Result<()> {
        self.inner.rename(ctx, old_file_name, new_file_name)
    }
    fn presign_file(&self, ctx: &Context, file_name: &str, expire: Duration) -> tidb_objstore::error::Result<String> {
        self.inner.presign_file(ctx, file_name, expire)
    }
    fn close(&self) {
        self.close_count.fetch_add(1, Ordering::SeqCst);
        self.inner.close();
    }
}

struct StorageCounters {
    create: Arc<AtomicI32>,
    close: Arc<AtomicI32>,
    write: Arc<AtomicI32>,
}

fn install_record_storage(adv: &CheckpointAdvancer, fail_writes: bool) -> StorageCounters {
    let counters = StorageCounters { create: Arc::default(), close: Arc::default(), write: Arc::default() };
    let (create, close, write) = (Arc::clone(&counters.create), Arc::clone(&counters.close), Arc::clone(&counters.write));
    adv.set_create_global_checkpoint_storage(Arc::new(move |ctx, backend| {
        create.fetch_add(1, Ordering::SeqCst);
        let inner = tidb_objstore::create(ctx, backend, false).map_err(|e| errors::errorf(e.to_string()))?;
        Ok(Arc::new(RecordStorage {
            inner,
            close_count: Arc::clone(&close),
            write_count: Arc::clone(&write),
            on_write: None,
            fail_writes,
        }) as Arc<dyn tidb_objstore::storeapi::Storage>)
    }));
    counters
}

fn local_backend(dir: &std::path::Path) -> StorageBackend {
    StorageBackend {
        backend: Some(storage_backend::Backend::Local(Local { path: dir.to_str().unwrap().to_owned() })),
    }
}

/// Go `TestTickWritesGlobalCheckpointToStorage`.
#[test]
fn test_tick_writes_global_checkpoint_to_storage() {
    let c = Cluster::new_basic(4, false);
    c.split_and_scatter(SPLITS);
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let dir = tempfile::tempdir().unwrap();
    let env = TestEnv::new(&c);
    env.task.lock().unwrap().info.as_mut().unwrap().storage = Some(local_backend(dir.path()));
    let adv = new_advancer(&env);
    let counters = install_record_storage(&adv, false);
    adv.start_task_listener(&ctx);
    adv.on_tick(&ctx).unwrap();
    assert_eq!(counters.create.load(Ordering::SeqCst), 0);
    assert_eq!(counters.write.load(Ordering::SeqCst), 0);
    c.advance_checkpoints();
    adv.on_tick(&ctx).unwrap();
    assert_eq!(counters.create.load(Ordering::SeqCst), 1);
    let checkpoint = c.advance_checkpoints();
    adv.on_tick(&ctx).unwrap();
    assert_eq!(counters.create.load(Ordering::SeqCst), 1);
    let writes_after_checkpoint = counters.write.load(Ordering::SeqCst);
    adv.on_tick(&ctx).unwrap();
    assert_eq!(counters.write.load(Ordering::SeqCst), writes_after_checkpoint);
    assert_eq!(counters.create.load(Ordering::SeqCst), 1);
    let data = std::fs::read(dir.path().join("v1").join("global_checkpoint").join("central_global.ts")).unwrap();
    assert_eq!(data.len(), 8);
    assert_eq!(u64::from_le_bytes(data.try_into().unwrap()), checkpoint);
    adv.on_stop();
    assert_eq!(counters.close.load(Ordering::SeqCst), 1);
    cancel.cancel();
}

/// Go `TestTickIgnoresGlobalCheckpointStorageFailure`: a failed write
/// leaves the tick, the etcd checkpoint and the GC safe point alone; the
/// safe point is moved before the write.
#[test]
fn test_tick_ignores_global_checkpoint_storage_failure() {
    let c = Cluster::new_basic(4, false);
    c.split_and_scatter(SPLITS);
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let dir = tempfile::tempdir().unwrap();
    let env = TestEnv::new(&c);
    env.task.lock().unwrap().info.as_mut().unwrap().storage = Some(local_backend(dir.path()));
    let adv = new_advancer(&env);
    let gc_blocked_before_write = Arc::new(AtomicBool::new(false));
    let (flag, cluster) = (Arc::clone(&gc_blocked_before_write), Arc::clone(&c));
    adv.set_create_global_checkpoint_storage(Arc::new(move |ctx, backend| {
        let inner = tidb_objstore::create(ctx, backend, false).map_err(|e| errors::errorf(e.to_string()))?;
        let (flag, cluster) = (Arc::clone(&flag), Arc::clone(&cluster));
        Ok(Arc::new(RecordStorage {
            inner,
            close_count: Arc::default(),
            write_count: Arc::default(),
            on_write: Some(Box::new(move || flag.store(cluster.lock().service_gc_safe_point_set, Ordering::SeqCst))),
            fail_writes: true,
        }) as Arc<dyn tidb_objstore::storeapi::Storage>)
    }));
    adv.start_task_listener(&ctx);
    let checkpoint = c.advance_checkpoints();
    adv.on_tick(&ctx).unwrap();
    assert_eq!(env.get_checkpoint(), checkpoint);
    assert!(c.lock().service_gc_safe_point_set);
    assert_eq!(c.lock().service_gc_safe_point, checkpoint - 1);
    assert!(gc_blocked_before_write.load(Ordering::SeqCst));
    cancel.cancel();
}

/// Go `TestWithFailure`: a region that didn't flush holds the checkpoint.
#[test]
fn test_with_failure() {
    let c = Cluster::new_basic(4, true);
    c.split_and_scatter(SPLITS);
    c.flush_all();
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let env = TestEnv::new(&c);
    let adv = new_advancer(&env);
    adv.start_task_listener(&ctx);
    adv.on_tick(&ctx).unwrap();
    let cp = c.advance_checkpoints();
    c.flush_store(c.store_ids()[0]);
    adv.on_tick(&ctx).unwrap();
    assert!(env.get_checkpoint() < cp, "{} {cp}", env.get_checkpoint());
    c.flush_all();
    adv.on_tick(&ctx).unwrap();
    assert_eq!(env.get_checkpoint(), cp);
    cancel.cancel();
}

/// Go `TestCollectorFailure`: a store that always fails ends the
/// collection with an error instead of hanging it.
#[test]
fn test_collector_failure() {
    let c = Cluster::new_basic(4, true);
    c.set_on_get_client(Some(Arc::new(|_| {
        Err(grpc_err(tonic::Code::DataLoss, "Exiled requests from the client, please slow down and listen a story"))
    })));
    let keys: Vec<String> = (0..10000).map(|i| format!("{i:04}")).collect();
    c.split_and_scatter(&keys.iter().map(String::as_str).collect::<Vec<_>>());
    let env = TestEnv::new(&c);
    let adv = new_advancer(&env);
    should_finish_in_time(Duration::from_secs(30), "scan with always fail", move || {
        let ctx = Context::background();
        let coll = ClusterCollector::new(&ctx, Arc::clone(&env) as Arc<dyn LogBackupService>);
        // The sending may or may not fail here: batching is asynchronous.
        let _ = adv.get_checkpoint_in_range(&ctx, &[], &[], &coll);
        // ...but this must fail, not get stuck.
        assert!(coll.finish(&ctx).is_err());
    });
}

/// Go `TestOneStoreFailure`.
#[test]
fn test_one_store_failure() {
    let c = Cluster::new_basic(4, true);
    let ctx = Context::background();
    let keys: Vec<String> = (0..1000).map(|i| format!("{i:04}")).collect();
    c.split_and_scatter(&keys.iter().map(String::as_str).collect::<Vec<_>>());
    c.flush_all();
    let env = TestEnv::new(&c);
    let adv = new_advancer(&env);
    adv.start_task_listener(&ctx);
    adv.on_tick(&ctx).unwrap();
    c.set_on_get_client(Some(one_store_failure()));
    for _ in 0..100 {
        c.advance_checkpoints();
        c.flush_all();
        let err = adv.on_tick(&ctx).unwrap_err();
        assert!(err.to_string().contains("the warm lamplight"), "{err}");
    }
    c.set_on_get_client(None);
    let cp = c.advance_checkpoints();
    c.flush_all();
    adv.on_tick(&ctx).unwrap();
    assert_eq!(env.get_checkpoint(), cp);
}

/// Go `TestGCServiceSafePoint`.
#[test]
fn test_gc_service_safe_point() {
    let c = Cluster::new_basic(4, true);
    let ctx = Context::background();
    c.split_and_scatter(SPLITS);
    let env = TestEnv::new(&c);
    let adv = new_advancer(&env);
    adv.start_task_listener(&ctx);
    let cp = c.advance_checkpoints();
    c.flush_all();
    adv.on_tick(&ctx).unwrap();
    assert_eq!(c.lock().service_gc_safe_point, cp - 1);
    assert!(c.lock().service_gc_safe_point_set);
    env.unregister_task();
    eventually(Duration::from_secs(3), || c.lock().service_gc_safe_point_deleted);
}

fn kr(s: &str, e: &str) -> Range {
    Range { start_key: s.as_bytes().to_vec(), end_key: e.as_bytes().to_vec() }
}

/// Go `TestTaskRanges`.
#[test]
fn test_task_ranges() {
    let c = Cluster::new_basic(4, true);
    let ctx = Context::background();
    c.split_and_scatter(&["0001", "0002", "0012", "0034", "0048"]);
    c.advance_checkpoints();
    c.flush_all_except(&["0000", "0049"]);
    let env = TestEnv::with_ranges(&c, vec![kr("0002", "0048")], 0);
    let adv = new_advancer(&env);
    adv.start_task_listener(&ctx);
    let a = Arc::clone(&adv);
    should_finish_in_time(Duration::from_secs(10), "first advancing", move || a.on_tick(&Context::background()).unwrap());
    assert!(env.get_checkpoint() > 0);
}

/// Go `TestTaskRangesWithSplit`.
#[test]
fn test_task_ranges_with_split() {
    let c = Cluster::new_basic(4, true);
    let ctx = Context::background();
    c.split_and_scatter(&["0012", "0034", "0048"]);
    c.advance_checkpoints();
    c.flush_all_except(&["0049"]);
    let env = TestEnv::with_ranges(&c, vec![kr("0002", "0048")], 0);
    let adv = new_advancer(&env);
    adv.start_task_listener(&ctx);
    let a = Arc::clone(&adv);
    should_finish_in_time(Duration::from_secs(10), "first advancing", move || a.on_tick(&Context::background()).unwrap());
    let fst_checkpoint = env.get_checkpoint();
    assert!(fst_checkpoint > 0);
    c.split_and_scatter(&["0002"]);
    c.advance_checkpoints();
    c.flush_all_except(&["0000", "0049"]);
    let a = Arc::clone(&adv);
    should_finish_in_time(Duration::from_secs(10), "second advancing", move || a.on_tick(&Context::background()).unwrap());
    assert!(env.get_checkpoint() > fst_checkpoint);
}

/// Go `TestClearCache`: a failed checkpoint request drops the store's
/// cached connection.
#[test]
fn test_clear_cache() {
    let c = Cluster::new_basic(4, true);
    let ctx = Context::background();
    c.split_and_scatter(&["0012", "0034", "0048"]);
    let cleared_cache = Arc::new(Mutex::new(std::collections::HashSet::new()));
    let cc = Arc::clone(&cleared_cache);
    c.set_on_clear_cache(Some(Arc::new(move |u| {
        cc.lock().unwrap().insert(u);
        Ok(())
    })));
    let failed_store_id = Arc::new(Mutex::new(0u64));
    let has_failed = Arc::new(AtomicBool::new(false));
    for sid in c.store_ids() {
        let (failed, has_failed) = (Arc::clone(&failed_store_id), Arc::clone(&has_failed));
        c.lock().store_map.get_mut(&sid).unwrap().on_get_region_checkpoint = Some(Arc::new(move |_| {
            // One failed store is enough.
            if has_failed.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                *failed.lock().unwrap() = sid;
                return Err(errors::errorf("failed to get checkpoint"));
            }
            Ok(())
        }));
    }
    let env = TestEnv::new(&c);
    let adv = new_advancer(&env);
    adv.start_task_listener(&ctx);
    let a = Arc::clone(&adv);
    let res = should_finish_in_time(Duration::from_secs(1), "ticking", move || a.on_tick(&Context::background()));
    assert!(res.is_err());
    let failed = *failed_store_id.lock().unwrap();
    assert!(failed > 0, "failed to mark the cluster");
    assert!(cleared_cache.lock().unwrap().contains(&failed));
}

/// Go `TestBlocked`: a blocked checkpoint RPC ends the tick at the tick
/// timeout.
#[test]
fn test_blocked() {
    let c = Cluster::new_basic(4, true);
    let ctx = Context::background();
    c.split_and_scatter(&["0012", "0034", "0048"]);
    let (block_tx, block_rx) = crossbeam_channel::bounded::<()>(0);
    let (first_tx, first_rx) = crossbeam_channel::bounded::<()>(1);
    for sid in c.store_ids() {
        let (block_rx, first_tx) = (block_rx.clone(), first_tx.clone());
        c.lock().store_map.get_mut(&sid).unwrap().on_get_region_checkpoint = Some(Arc::new(move |_| {
            let _ = first_tx.try_send(());
            // Blocks, as a TiKV that is down or too busy.
            let _ = block_rx.recv();
            Ok(())
        }));
    }
    let env = TestEnv::new(&c);
    let adv = new_advancer(&env);
    adv.start_task_listener(&ctx);
    // Enough headroom that the blocked RPC is seen before the timeout.
    adv.update_config(Arc::new(CommandConfig { tick_duration: Duration::from_millis(100), ..CommandConfig::default() }));
    let (err_tx, err_rx) = crossbeam_channel::bounded(1);
    let a = Arc::clone(&adv);
    std::thread::spawn(move || err_tx.send(a.on_tick(&Context::background())).unwrap());
    first_rx.recv_timeout(Duration::from_secs(5)).expect("wait until blocked request observed");
    let err = err_rx.recv_timeout(Duration::from_secs(5)).expect("ticking").unwrap_err();
    assert!(err.to_string().contains("deadline exceeded"), "{err}");
    drop(block_tx);
}

/// Go `installSubscribeSupport`.
fn install_subscribe_support(c: &Cluster) {
    for id in c.store_ids() {
        c.set_support_flush_sub(id, true);
    }
}

/// Go `installSubscribeSupportForRandomN`.
fn install_subscribe_support_for_n(c: &Cluster, n: usize) {
    for id in c.store_ids().into_iter().take(n) {
        c.set_support_flush_sub(id, true);
    }
}

/// Go `TestOwnerDropped`: `OnStop` waits for an in-flight subscribe tick,
/// and the advancer polls once the subscriptions are gone.
#[test]
fn test_owner_dropped() {
    let ctx = Context::background();
    let c = Cluster::new_basic(4, false);
    c.split_and_scatter(SPLITS);
    install_subscribe_support(&c);
    let env = TestEnv::new(&c);
    let adv = new_advancer(&env);
    super::daemon::Interface::on_start(&adv, &ctx);
    adv.spawn_subscription_handler(&ctx);
    adv.on_tick(&ctx).unwrap();
    assert!(adv.has_subscriptions());
    let (enter_tx, enter_rx) = crossbeam_channel::bounded::<()>(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded::<()>(0);
    let fp = "github.com/pingcap/tidb/br/pkg/streamhelper/get_subscriber";
    let guard = tidb_failpoint::enable_call_for_test(fp, move |_| {
        let _ = enter_tx.try_send(());
        let _ = release_rx.recv();
    });
    let (tick_tx, tick_rx) = crossbeam_channel::bounded(1);
    let a = Arc::clone(&adv);
    std::thread::spawn(move || tick_tx.send(a.on_tick(&Context::background())).unwrap());
    enter_rx.recv_timeout(Duration::from_secs(1)).expect("in-flight OnTick enters subscribeTick");
    let (stop_tx, stop_rx) = crossbeam_channel::bounded::<()>(1);
    let a = Arc::clone(&adv);
    std::thread::spawn(move || {
        a.on_stop();
        stop_tx.send(()).unwrap();
    });
    std::thread::sleep(Duration::from_millis(50));
    assert!(stop_rx.try_recv().is_err(), "OnStop returned before subscribeTick was released");
    drop(release_tx);
    drop(guard);
    tick_rx.recv_timeout(Duration::from_secs(1)).expect("in-flight OnTick finishes").unwrap();
    stop_rx.recv_timeout(Duration::from_secs(1)).expect("OnStop finishes");
    assert!(!adv.has_subscriptions());
    let cp = c.advance_checkpoints();
    adv.on_tick(&ctx).unwrap();
    // The advancer polls the checkpoint itself.
    adv.with_checkpoints(|vsf| assert_eq!(vsf.unwrap().min_value(), cp));
}

/// Go `TestRemoveTaskAndFlush` (#50839): flush events of a removed task
/// don't panic the handler.
#[test]
fn test_remove_task_and_flush() {
    let ctx = Context::background();
    let c = Cluster::new_basic(4, true);
    install_subscribe_support(&c);
    let env = TestEnv::new(&c);
    let adv = new_advancer(&env);
    adv.start_task_listener(&ctx);
    adv.spawn_subscription_handler(&ctx);
    adv.on_tick(&ctx).unwrap();
    let fp = "github.com/pingcap/tidb/br/pkg/streamhelper/subscription-handler-loop";
    tidb_failpoint::enable(fp, "pause").unwrap();
    c.flush_all();
    env.unregister_task();
    eventually(Duration::from_secs(10), || !adv.has_task());
    tidb_failpoint::disable(fp);
    eventually(Duration::from_secs(10), || !adv.has_subscriptions());
}

fn minutes(n: u64) -> Duration {
    Duration::from_secs(60 * n)
}

/// Go `oracle.GoTimeToTS(oracle.GetTimeFromTS(0).Add(d))`.
fn ts_at(d: Duration) -> u64 {
    tikv_client::oracle::go_time_to_ts(tikv_client::oracle::get_time_from_ts(0) + d)
}

/// Go `testEnv.advanceCheckpointBy`.
fn env_advance_checkpoint_by(env: &TestEnv, d: Duration) {
    let mut cp = env.checkpoint.lock().unwrap();
    *cp = tikv_client::oracle::go_time_to_ts(tikv_client::oracle::get_time_from_ts(*cp) + d);
}

/// Go `TestEnableCheckPointLimit`.
#[test]
fn test_enable_check_point_limit() {
    let c = Cluster::new_basic(4, false);
    c.split_and_scatter(SPLITS);
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let env = TestEnv::with_ranges(&c, Vec::new(), ts_at(minutes(1)));
    let adv = new_advancer(&env);
    update_check_point_lag_limit(&adv, minutes(1));
    c.advance_cluster_time_by(minutes(1), false);
    c.advance_checkpoint_by(minutes(1));
    adv.start_task_listener(&ctx);
    for _ in 0..5 {
        c.advance_cluster_time_by(Duration::from_secs(30), false);
        c.advance_checkpoint_by(Duration::from_secs(20));
        adv.on_tick(&ctx).unwrap();
    }
    cancel.cancel();
}

fn assert_lagged(res: Result<()>) {
    let err = res.unwrap_err();
    assert!(err.to_string().contains("lagged too large"), "{err}");
}

/// Go `TestCheckPointLagged`.
#[test]
fn test_check_point_lagged() {
    let c = Cluster::new_basic(4, false);
    c.split_and_scatter(SPLITS);
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let env = TestEnv::with_ranges(&c, Vec::new(), ts_at(minutes(1)));
    let adv = new_advancer(&env);
    update_check_point_lag_limit(&adv, minutes(1));
    adv.start_task_listener(&ctx);
    c.advance_cluster_time_by(minutes(2), false);
    // The checkpoint isn't lagged while the global ts isn't advanced.
    c.advance_checkpoint_by(minutes(2));
    adv.on_tick(&ctx).unwrap();
    c.advance_cluster_time_by(minutes(3), false);
    assert_lagged(adv.on_tick(&ctx));
    // Once paused, ticks are skipped.
    eventually(Duration::from_secs(5), || adv.on_tick(&ctx).is_ok());
    cancel.cancel();
}

/// Go `TestCheckPointResume`: a resumed task runs normally.
#[test]
fn test_check_point_resume() {
    let c = Cluster::new_basic(4, false);
    c.split_and_scatter(SPLITS);
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let env = TestEnv::new(&c);
    let adv = new_advancer(&env);
    update_check_point_lag_limit(&adv, minutes(1));
    adv.start_task_listener(&ctx);
    c.advance_cluster_time_by(minutes(1), false);
    c.advance_checkpoint_by(minutes(1));
    adv.on_tick(&ctx).unwrap();
    c.advance_cluster_time_by(minutes(2), false);
    assert_lagged(adv.on_tick(&ctx));
    eventually(Duration::from_secs(5), || adv.on_tick(&ctx).is_ok());
    // The checkpoint is fixed and the task resumed.
    c.advance_checkpoint_by(minutes(1));
    env.resume_task();
    eventually(Duration::from_secs(5), || adv.on_tick(&ctx).is_ok());
    // In time the checkpoint exceeds the limit again.
    c.advance_cluster_time_by(minutes(2), false);
    eventually(Duration::from_secs(5), || adv.on_tick(&ctx).is_err_and(|e| e.to_string().contains("lagged too large")));
    cancel.cancel();
}

fn lagged_err(res: &Result<()>) -> bool {
    res.as_ref().is_err_and(|e| e.to_string().contains("check point lagged too large"))
}

/// Go `TestUnregisterAfterPause`: a re-added task of a paused one is
/// checked again.
#[test]
fn test_unregister_after_pause() {
    let c = Cluster::new_basic(4, false);
    c.split_and_scatter(SPLITS);
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let env = TestEnv::new(&c);
    let adv = new_advancer(&env);
    update_check_point_lag_limit(&adv, minutes(1));
    adv.start_task_listener(&ctx);
    eventually(Duration::from_secs(5), || adv.has_task());
    // A lagged checkpoint pauses the task, even at the start ts.
    c.advance_cluster_time_by(minutes(1), false);
    adv.on_tick(&ctx).unwrap();
    env.pause();
    c.advance_cluster_time_by(minutes(1), false);
    eventually(Duration::from_secs(5), || adv.on_tick(&ctx).is_ok());
    env.unregister_task();
    eventually(Duration::from_secs(5), || !adv.has_task());
    env.put_task();
    eventually(Duration::from_secs(5), || adv.has_task());
    eventually(Duration::from_secs(5), || lagged_err(&adv.on_tick(&ctx)));
    env.unregister_task();
    eventually(Duration::from_secs(5), || !adv.has_task());
    c.advance_cluster_time_by(minutes(1), true);
    adv.on_tick(&ctx).unwrap();
    env.pause();
    c.advance_cluster_time_by(minutes(1), false);
    eventually(Duration::from_secs(5), || adv.on_tick(&ctx).is_ok());
    env.unregister_task();
    eventually(Duration::from_secs(5), || !adv.has_task());
    env.put_task();
    eventually(Duration::from_secs(5), || adv.has_task());
    eventually(Duration::from_secs(5), || lagged_err(&adv.on_tick(&ctx)));
    cancel.cancel();
}

/// Go `TestAddTaskWithLongRunTask0`: a start ts that isn't lagged runs,
/// even with the cluster and PD lagged.
#[test]
fn test_add_task_with_long_run_task0() {
    let c = Cluster::new_basic(4, false);
    c.split_and_scatter(SPLITS);
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let env = TestEnv::with_ranges(&c, Vec::new(), ts_at(minutes(2)));
    let adv = new_advancer(&env);
    update_check_point_lag_limit(&adv, minutes(1));
    c.advance_cluster_time_by(minutes(3), false);
    c.advance_checkpoint_by(minutes(1));
    env_advance_checkpoint_by(&env, minutes(1));
    env.pd_disconnected.store(true, Ordering::SeqCst);
    adv.start_task_listener(&ctx);
    adv.on_tick(&ctx).unwrap();
    adv.on_tick(&ctx).unwrap();
    cancel.cancel();
}

/// Go `TestAddTaskWithLongRunTask1`: a lagged start ts runs once the
/// cluster has advanced.
#[test]
fn test_add_task_with_long_run_task1() {
    let c = Cluster::new_basic(4, false);
    c.split_and_scatter(SPLITS);
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let env = TestEnv::with_ranges(&c, Vec::new(), ts_at(minutes(1)));
    let adv = new_advancer(&env);
    update_check_point_lag_limit(&adv, minutes(1));
    c.advance_cluster_time_by(minutes(3), false);
    c.advance_checkpoint_by(minutes(2));
    env_advance_checkpoint_by(&env, minutes(1));
    adv.start_task_listener(&ctx);
    adv.on_tick(&ctx).unwrap();
    adv.on_tick(&ctx).unwrap();
    cancel.cancel();
}

/// Go `TestAddTaskWithLongRunTask2`: a lagged start ts runs once PD holds
/// an advanced checkpoint; a transient PD error doesn't stop it.
#[test]
fn test_add_task_with_long_run_task2() {
    let c = Cluster::new_basic(4, false);
    c.split_and_scatter(SPLITS);
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let env = TestEnv::with_ranges(&c, Vec::new(), ts_at(minutes(1)));
    let adv = new_advancer(&env);
    update_check_point_lag_limit(&adv, minutes(1));
    adv.start_task_listener(&ctx);
    c.advance_cluster_time_by(minutes(3), false);
    c.advance_checkpoint_by(minutes(1));
    env_advance_checkpoint_by(&env, minutes(2));
    env.pd_disconnected.store(true, Ordering::SeqCst);
    // The global ts is at 2 minutes, the local checkpoint at 1: the upload
    // would roll it back.
    assert!(adv.on_tick(&ctx).is_err(), "checkpoint rollback");
    // Ticks are normal once the local checkpoint passes the global ts.
    c.advance_checkpoint_by(minutes(12));
    adv.on_tick(&ctx).unwrap();
    cancel.cancel();
}

/// Go `TestAddTaskWithLongRunTask3`: lagged start ts, PD and cluster
/// checkpoints pause the task.
#[test]
fn test_add_task_with_long_run_task3() {
    let c = Cluster::new_basic(4, false);
    c.split_and_scatter(SPLITS);
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let env = TestEnv::with_ranges(&c, Vec::new(), ts_at(minutes(1)));
    let adv = new_advancer(&env);
    update_check_point_lag_limit(&adv, minutes(1));
    // A start ts equal to the checkpoint doesn't pause the task.
    adv.start_task_listener(&ctx);
    c.advance_cluster_time_by(minutes(2), false);
    c.advance_checkpoint_by(minutes(1));
    env_advance_checkpoint_by(&env, minutes(1));
    adv.on_tick(&ctx).unwrap();
    c.advance_cluster_time_by(minutes(2), false);
    c.advance_checkpoint_by(minutes(1));
    env_advance_checkpoint_by(&env, minutes(1));
    assert_lagged(adv.on_tick(&ctx));
    // No error once paused.
    eventually(Duration::from_secs(5), || adv.on_tick(&ctx).is_ok());
    cancel.cancel();
}

/// Go `TestOwnerChangeCheckPointLagged`: a new owner picks the lag check up.
#[test]
fn test_owner_change_check_point_lagged() {
    use super::daemon::Interface as _;
    let c = Cluster::new_basic(4, false);
    c.split_and_scatter(SPLITS);
    let ctx = Context::background();
    let env = TestEnv::with_ranges(&c, Vec::new(), ts_at(minutes(1)));
    let adv = new_advancer(&env);
    update_check_point_lag_limit(&adv, minutes(1));
    let (ctx1, cancel1) = Context::with_cancel(&Context::background());
    adv.on_start(&ctx1);
    adv.on_become_owner(&ctx1);
    adv.on_tick(&ctx1).unwrap();
    // Another advancer that never advanced the checkpoint.
    let adv2 = new_advancer(&env);
    update_check_point_lag_limit(&adv2, minutes(1));
    let (ctx2, cancel2) = Context::with_cancel(&Context::background());
    for _ in 0..5 {
        c.advance_cluster_time_by(minutes(2), false);
        c.advance_checkpoint_by(minutes(2));
        adv.on_tick(&ctx1).unwrap();
    }
    c.advance_cluster_time_by(minutes(2), false);
    assert_lagged(adv.on_tick(&ctx1));
    // Resume so the next tick is normal.
    c.advance_checkpoint_by(minutes(2));
    env.resume_task();
    // advancer1 stops; advancer2 takes over.
    cancel1.cancel();
    adv2.on_start(&ctx2);
    adv2.on_become_owner(&ctx2);
    adv2.on_tick(&ctx2).unwrap();
    for _ in 0..10 {
        c.advance_cluster_time_by(minutes(2), false);
        c.advance_checkpoint_by(minutes(2));
        adv2.on_tick(&ctx2).unwrap();
    }
    c.advance_cluster_time_by(minutes(2), false);
    assert_lagged(adv2.on_tick(&ctx2));
    // advancer2 stops; advancer1 comes back.
    c.advance_checkpoint_by(minutes(2));
    env.resume_task();
    cancel2.cancel();
    adv.on_become_owner(&ctx);
    adv.on_tick(&ctx).unwrap();
}

/// Go `TestGCCheckpoint`: a GC safe point above the checkpoint fails the
/// tick once the task resumes.
#[test]
fn test_gc_checkpoint() {
    let c = Cluster::new_basic(4, false);
    c.split_and_scatter(SPLITS);
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let env = TestEnv::with_ranges(&c, Vec::new(), ts_at(Duration::ZERO));
    let adv = new_advancer(&env);
    adv.start_task_listener(&ctx);
    eventually(Duration::from_secs(5), || adv.has_task());
    c.advance_cluster_time_by(minutes(1), false);
    c.advance_checkpoint_by(minutes(1));
    env.pause();
    c.lock().service_gc_safe_point = ts_at(minutes(2));
    eventually(Duration::from_secs(5), || adv.on_tick(&ctx).is_ok());
    env.resume_task();
    eventually(Duration::from_secs(5), || {
        adv.on_tick(&ctx).is_err_and(|e| e.to_string().contains("greater than the target"))
    });
    cancel.cancel();
}

fn new_subscriber(c: &Arc<Cluster>) -> FlushSubscriber {
    FlushSubscriber::new(
        Arc::new(Arc::clone(c)) as Arc<dyn LogBackupService>,
        Arc::new(Arc::clone(c)) as Arc<dyn TiKVClusterMeta>,
        Vec::new(),
    )
}

/// Go `collectCheckpointSpans`: the events merged until the minimum
/// reaches `checkpoint`.
fn collect_checkpoint_spans(sub: &FlushSubscriber, checkpoint: u64) -> ValueSortedFull {
    let mut observed = sorted(ValuedFull::new_full_with(&crate::spans::full(), 1));
    let events = sub.events();
    eventually(Duration::from_secs(3), || {
        while let Ok(event) = events.try_recv() {
            observed.merge(event);
        }
        observed.min_value() >= checkpoint
    });
    observed
}

/// Go `TestSubBasic`.
#[test]
fn test_sub_basic() {
    let ctx = Context::background();
    let c = Cluster::new_basic(4, true);
    c.split_and_scatter(&["0001", "0002", "0003", "0008", "0009"]);
    install_subscribe_support(&c);
    let mut sub = new_subscriber(&c);
    sub.update_store_topology(&ctx).unwrap();
    let mut cp = 0;
    for _ in 0..10 {
        cp = c.advance_checkpoints();
        c.flush_all();
    }
    sub.handle_errors();
    sub.pending_errors().unwrap();
    let s = collect_checkpoint_spans(&sub, cp);
    sub.drop_subscriber();
    assert!(s.min_value() >= cp, "s.MinValue() = {}, cp = {cp}", s.min_value());
}

/// Go `TestNormalError`: a failed connect is retried.
#[test]
fn test_normal_error() {
    let ctx = Context::background();
    let c = Cluster::new_basic(4, true);
    c.split_and_scatter(&["0001", "0002", "0003", "0008", "0009"]);
    install_subscribe_support(&c);
    let mut sub = new_subscriber(&c);
    c.set_on_get_client(Some(one_store_failure()));
    sub.update_store_topology(&ctx).unwrap();
    c.set_on_get_client(None);
    assert!(sub.pending_errors().is_err());
    sub.handle_errors();
    sub.pending_errors().unwrap();
    let mut cp = 0;
    for _ in 0..10 {
        cp = c.advance_checkpoints();
        c.flush_all();
    }
    let s = collect_checkpoint_spans(&sub, cp);
    sub.drop_subscriber();
    assert_eq!(cp, s.min_value());
}

/// Go `TestHasFailureStores`: an unsupported store's error isn't retried
/// until the store restarts.
#[test]
fn test_has_failure_stores() {
    let ctx = Context::background();
    let c = Cluster::new_basic(4, true);
    c.split_and_scatter(&["0001", "0002", "0003", "0008", "0009"]);
    install_subscribe_support_for_n(&c, 3);
    let mut sub = new_subscriber(&c);
    sub.update_store_topology(&ctx).unwrap();
    sub.handle_errors();
    assert!(sub.pending_errors().is_err());
    install_subscribe_support(&c);
    sub.update_store_topology(&ctx).unwrap();
    sub.handle_errors();
    sub.pending_errors().unwrap();
}

/// Go `TestStoreOffline`.
#[test]
fn test_store_offline() {
    let ctx = Context::background();
    let c = Cluster::new_basic(4, true);
    c.split_and_scatter(&["0001", "0002", "0003", "0008", "0009"]);
    install_subscribe_support(&c);
    c.set_on_get_client(Some(Arc::new(|_| {
        Err(grpc_err(tonic::Code::DataLoss, "upon an eclipsed night, some of data (not all data) have fled from the dataset"))
    })));
    let mut sub = new_subscriber(&c);
    sub.update_store_topology(&ctx).unwrap();
    assert!(sub.pending_errors().is_err());
    c.set_on_get_client(None);
    sub.handle_errors();
    sub.pending_errors().unwrap();
}

/// Go `TestStoreRemoved`.
#[test]
fn test_store_removed() {
    let ctx = Context::background();
    let c = Cluster::new_basic(4, true);
    c.split_and_scatter(&["0001", "0002", "0003", "0008", "0009", "0010", "0100", "0956", "1000"]);
    install_subscribe_support(&c);
    let mut sub = new_subscriber(&c);
    sub.update_store_topology(&ctx).unwrap();
    let mut cp = 0;
    for _ in 0..10 {
        cp = c.advance_checkpoints();
        c.flush_all();
    }
    sub.handle_errors();
    sub.pending_errors().unwrap();
    c.remove_store(c.store_ids()[0]);
    sub.update_store_topology(&ctx).unwrap();
    for _ in 0..10 {
        cp = c.advance_checkpoints();
        c.flush_all();
    }
    sub.handle_errors();
    sub.pending_errors().unwrap();
    let s = collect_checkpoint_spans(&sub, cp);
    sub.drop_subscriber();
    assert!(s.min_value() >= cp, "s.MinValue() = {}, cp = {cp}", s.min_value());
}

/// Go `constantRegions`.
struct ConstantRegions(Vec<RegionWithLeader>);

impl TiKVClusterMeta for ConstantRegions {
    fn region_scan(&self, _ctx: &Context, key: &[u8], end_key: &[u8], limit: usize) -> Result<Vec<RegionWithLeader>> {
        let want = Range { start_key: key.to_vec(), end_key: end_key.to_vec() };
        let mut result = Vec::new();
        for region in &self.0 {
            let rng = Range { start_key: region.region.start_key.clone(), end_key: region.region.end_key.clone() };
            if crate::spans::overlaps(&want, &rng) && result.len() < limit {
                result.push(region.clone());
            } else if region.region.start_key.as_slice() > key {
                break;
            }
        }
        Ok(result)
    }
    fn stores(&self, _ctx: &Context) -> Result<Vec<Store>> {
        Err(grpc_err(tonic::Code::Unimplemented, "Unsupported operation"))
    }
    fn block_gc_until(&self, _ctx: &Context, _at: u64) -> Result<u64> {
        Err(grpc_err(tonic::Code::Unimplemented, "Unsupported operation"))
    }
    fn unblock_gc(&self, _ctx: &Context) -> Result<()> {
        Err(grpc_err(tonic::Code::Unimplemented, "Unsupported operation"))
    }
    fn fetch_current_ts(&self, _ctx: &Context) -> Result<u64> {
        Ok(tikv_client::oracle::go_time_to_ts(std::time::SystemTime::now()))
    }
}

/// Go `makeSubrangeRegions`.
fn make_subrange_regions(keys: &[String]) -> Vec<RegionWithLeader> {
    keys.windows(2)
        .enumerate()
        .map(|(i, w)| RegionWithLeader {
            region: tidb_proto::metapb::Region {
                id: i as u64 + 1,
                start_key: w[0].as_bytes().to_vec(),
                end_key: w[1].as_bytes().to_vec(),
                ..Default::default()
            },
            leader: Default::default(),
        })
        .collect()
}

/// Go `manyRegions`.
fn many_regions(from: usize, to: usize) -> Vec<String> {
    (from..to).map(|i| format!("{i:06}")).collect()
}

/// Go `TestRegionIterator`.
#[test]
fn test_region_iterator() {
    let s = |v: &[&str]| v.iter().map(|x| (*x).to_owned()).collect::<Vec<_>>();
    let with = |mut a: Vec<String>, initial: bool, last: bool| {
        if initial {
            a.insert(0, String::new());
        }
        if last {
            a.push(String::new());
        }
        a
    };
    let cases: Vec<(Vec<String>, &str, &str, Vec<String>)> = vec![
        (s(&["0001", "0003", "0008", "0078"]), "0077", "0079", s(&["0008", "0078", ""])),
        (s(&["0001", "0005", "0008", "0097"]), "0000", "0008", s(&["", "0001", "0005", "0008"])),
        (many_regions(0, 10000), "000001", "005000", many_regions(1, 5001)),
        (many_regions(0, 10000), "000100", "", with(many_regions(100, 10000), false, true)),
        (many_regions(0, 10000), "", "003000", with(many_regions(0, 3001), true, false)),
    ];
    let ctx = Context::background();
    for (i, (boundary, start, end, required)) in cases.into_iter().enumerate() {
        let regions = ConstantRegions(make_subrange_regions(&with(boundary, true, true)));
        let required = make_subrange_regions(&required);
        let mut collected = Vec::new();
        let mut iter = iterate_region(&regions, start.as_bytes(), end.as_bytes());
        while !iter.done() {
            collected.extend(iter.next(&ctx).unwrap());
        }
        let ranges = |v: &[RegionWithLeader]| {
            v.iter().map(|r| (r.region.start_key.clone(), r.region.end_key.clone())).collect::<Vec<_>>()
        };
        assert_eq!(ranges(&required), ranges(&collected), "case#{i}");
    }
}

/// Go `TestGetLogBackupFlushIntervalFromTiKVConfig`.
#[test]
fn test_get_log_backup_flush_interval_from_tikv_config() {
    use super::advancer_env::get_log_backup_flush_interval_from_tikv_config;
    let ctx = Context::background();
    let fetch = |configs: Vec<&'static str>| {
        move |_: &Context, collect: &mut dyn FnMut(Vec<u8>) -> Result<()>| -> Result<()> {
            for c in &configs {
                collect(c.as_bytes().to_vec())?;
            }
            Ok(())
        }
    };
    let got = get_log_backup_flush_interval_from_tikv_config(
        &ctx,
        &fetch(vec![r#"{"log-backup":{"max-flush-interval":"20s"}}"#, r#"{"log-backup":{"max-flush-interval":"30s"}}"#]),
    )
    .unwrap();
    assert_eq!(got, Duration::from_secs(30));
    let err = get_log_backup_flush_interval_from_tikv_config(&ctx, &fetch(vec![r#"{"log-backup":{"enable":true}}"#])).unwrap_err();
    assert!(err.to_string().contains("log-backup.max-flush-interval is not found"), "{err}");
    let err = get_log_backup_flush_interval_from_tikv_config(&ctx, &fetch(vec![])).unwrap_err();
    assert!(err.to_string().contains("no TiKV config found"), "{err}");
}
