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

//! Go `pkg/ddl/schemaver/syncer.go`: [`EtcdSyncer`], the etcd-backed syncer
//! used wherever TiKV is the store -- which for a Rust node attached to a
//! shared cluster means ALWAYS.
//!
//! # The two report paths, and why both matter
//!
//! A Go owner's `WaitVersionSynced` branches on `variable.EnableMDL`:
//!
//! * MDL ON: each follower PUTs its loaded version to
//!   `/tidb/ddl/all_schema_by_job_versions/<jobID>/<ddlID>`; the owner
//!   mirrors that prefix through [`EtcdSyncer::sync_job_schema_ver_loop`] and
//!   waits until every LIVE server id has reported at least `latest_ver`.
//! * MDL OFF: each follower PUTs to its own
//!   `/tidb/ddl/all_schema_versions/<ddlID>` under a session LEASE, so a dead
//!   node stops blocking; the owner polls the whole prefix instead.
//!
//! Both paths are ported; [`crate::Syncer::update_self_version`] picks one at
//! call time exactly as upstream does.
//!
//! # The etcd seam
//!
//! [`EtcdOps`](tidb_domain::serverinfo_syncer::EtcdOps) already covers
//! lease/put/get_prefix/delete primitives;
//! [`EtcdWatchOps`] adds only what this package needs beyond it: prefix reads
//! WITH the header revision, the two compare-based writes Init and
//! `PutKVToEtcdMono` need, and prefix watches as an event stream. The
//! production adapter is `tidb_pd_client::etcd::{EtcdClient, EtcdWatcher}`
//! (PD embeds etcd and serves the full v3 surface on its client port, see
//! `crates/tidb-exec/src/catalog_watch.rs`'s module doc); tests drive a
//! recording fake, the same tier convention `serverinfo_syncer.rs` uses.
//!
//! Go's `concurrency.Session` (a granted lease PLUS an automatic keepalive
//! loop whose death closes a `Done()` channel) is reproduced by
//! [`Session`]: same grant, same keepalive thread, same close-on-death.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_domain::serverinfo::ServerInfo;
use tidb_domain::serverinfo_syncer::{join_host_port, EtcdOps};

use crate::{
    mdl_enabled, AllServerInfo, Context, DoneCh, GlobalVerRx, SharedRecv, SyncSummary, Syncer,
    WatchEvent, CHECK_VERS_INTERVAL, DDL_ALL_SCHEMA_VERSIONS, DDL_ALL_SCHEMA_VERSIONS_BY_JOB,
    DDL_GLOBAL_SCHEMA_VERSION, DDL_PROMPT, INITIAL_VERSION, KEY_OP_DEFAULT_RETRY_CNT,
    KEY_OP_DEFAULT_TIMEOUT, KEY_OP_RETRY_INTERVAL, NEW_SESSION_DEFAULT_RETRY_CNT,
    NEW_SESSION_RETRY_INTERVAL, NEW_SESSION_RETRY_UNLIMITED, PUT_KEY_RETRY_UNLIMITED,
    SESSION_TTL_SECONDS,
};

/// One prefix read's answer: the entries plus the header revision.
pub type PrefixRead = (Vec<(String, Vec<u8>)>, i64);

/// The extra etcd surface this package needs beyond
/// [`EtcdOps`](tidb_domain::serverinfo_syncer::EtcdOps).
pub trait EtcdWatchOps: EtcdOps {
    /// `KV.Get` with `WithPrefix()`, answering the entries plus the store
    /// revision the read saw (`resp.Header.Revision`).
    ///
    /// # Errors
    /// Transport or store failure.
    fn get_prefix_with_rev(&self, prefix: &str) -> Result<PrefixRead, String>;

    /// One key's value and its mod revision (`0` when absent), for the CAS
    /// precondition below.
    ///
    /// # Errors
    /// Transport or store failure.
    fn get_with_mod_revision(&self, key: &str) -> Result<(Option<Vec<u8>>, i64), String>;

    /// `Txn(If(ModRevision(key) == expected).Then(Put(key, value)))`,
    /// answering whether the compare held. `false` means SOMEONE ELSE wrote
    /// between the read above and now -- retry, do not treat as transport
    /// failure.
    ///
    /// # Errors
    /// Transport or store failure.
    fn compare_and_swap(
        &self,
        key: &str,
        expected_mod_revision: i64,
        value: &[u8],
    ) -> Result<bool, String>;

    /// `Txn(If(CreateRevision(key) == 0).Then(Put(key, value)))`, answering
    /// whether THIS call created the key.
    ///
    /// # Errors
    /// Transport or store failure.
    fn put_if_not_exists(&self, key: &str, value: &[u8]) -> Result<bool, String>;

    /// `Watch(key, WithRev(start_revision))`, optionally with `WithPrefix()`:
    /// events from `start_revision` on, streamed until the stream ends or
    /// [`WatchStream::stop_watching`] fires. A reconnected implementation is
    /// expected to resume from where it left off, like Go's watch channel.
    ///
    /// `require_leader` mirrors Go's `clientv3.WithRequireLeader`: when set,
    /// the stream must error out promptly if the connected etcd loses its
    /// leader, so the caller rebuilds the watch instead of idling on a
    /// follower (Go sets it only for the job-version mirror watch,
    /// syncer.go:519).
    ///
    /// # Errors
    /// Failure establishing the watch.
    fn watch(
        &self,
        key: &str,
        start_revision: i64,
        with_prefix: bool,
        require_leader: bool,
    ) -> Result<WatchStream, String>;
}

/// One running prefix watch: an event queue plus the switch that ends it.
#[derive(Debug)]
pub struct WatchStream {
    /// Events in store order; an `Err` is Go's `wresp.Err()` -- the watch
    /// itself failed (compaction, transport) and the caller must restart.
    pub events: SharedRecv<Result<WatchEvent, String>>,
    /// Set this to ask the implementation to stop promptly.
    pub stop: Arc<AtomicBool>,
}

impl WatchStream {
    /// Ends the watch. Idempotent.
    pub fn stop_watching(&self) {
        self.stop.store(true, Ordering::Release);
    }
}

/// Go `util.PutKVToEtcd`: PUT with retries, optionally under a lease.
/// `retry_cnt == PUT_KEY_RETRY_UNLIMITED` retries until success or context
/// end, exactly like upstream's `math.MaxInt64`.
pub(crate) fn put_kv_to_etcd(
    ctx: &Context,
    etcd: &dyn EtcdOps,
    retry_cnt: u32,
    key: &str,
    value: &str,
    lease: Option<i64>,
) -> Result<(), String> {
    let mut last = String::new();
    for _ in 0..retry_cnt {
        if let Err(error) = ctx.err() {
            return Err(error.to_string());
        }
        let outcome = match lease {
            Some(lease) => etcd.put_with_lease(key, value.as_bytes(), lease),
            None => etcd.put(key, value.as_bytes()),
        };
        match outcome {
            Ok(()) => return Ok(()),
            Err(error) => {
                log_warn("etcd-cli put kv failed", &[("key", key)]);
                last = error;
                std::thread::sleep(KEY_OP_RETRY_INTERVAL);
            }
        }
    }
    Err(last)
}

/// Go `util.PutKVToEtcdMono`: PUT monotonously, via read-modify-CAS on the
/// mod revision, so concurrent writers never regress the value; conflicts
/// are retried up to `retry_cnt`.
pub(crate) fn put_kv_to_etcd_mono(
    ctx: &Context,
    etcd: &dyn EtcdWatchOps,
    retry_cnt: u32,
    key: &str,
    value: &str,
) -> Result<(), String> {
    let mut last = String::new();
    for _ in 0..retry_cnt {
        if let Err(error) = ctx.err() {
            return Err(error.to_string());
        }
        let prev_revision = match etcd.get_with_mod_revision(key) {
            Ok((_, revision)) => revision,
            Err(error) => {
                log_warn("etcd-cli put kv failed", &[("key", key)]);
                last = error;
                std::thread::sleep(KEY_OP_RETRY_INTERVAL);
                continue;
            }
        };
        let swapped = match etcd.compare_and_swap(key, prev_revision, value.as_bytes()) {
            Ok(swapped) => swapped,
            Err(error) => {
                log_warn("etcd-cli put kv failed", &[("key", key)]);
                last = error;
                std::thread::sleep(KEY_OP_RETRY_INTERVAL);
                continue;
            }
        };
        if swapped {
            return Ok(());
        }
        last = "performing compare-and-swap during PutKVToEtcd failed".to_owned();
        log_warn("etcd-cli put kv failed", &[("key", key)]);
        std::thread::sleep(KEY_OP_RETRY_INTERVAL);
    }
    Err(last)
}

/// Go `util.DeleteKeyFromEtcd`: DELETE with retries.
fn delete_key_from_etcd(etcd: &dyn EtcdOps, retry_cnt: u32, key: &str) -> Result<(), String> {
    let mut last = String::new();
    for _ in 0..retry_cnt {
        match etcd.delete(key) {
            Ok(()) => return Ok(()),
            Err(error) => {
                log_warn("etcd-cli delete key failed", &[("key", key)]);
                last = error;
            }
        }
    }
    Err(last)
}

fn log_warn(event: &str, fields: &[(&str, &str)]) {
    let pairs = fields
        .iter()
        .map(|(k, v)| format!("{k:?}:{v:?}"))
        .collect::<Vec<_>>()
        .join(",");
    let suffix = if pairs.is_empty() {
        String::new()
    } else {
        format!(",{pairs}")
    };
    eprintln!("{{\"level\":\"warn\",\"event\":\"{event}\"{suffix}}}");
}

fn log_info(event: &str, message: &str) {
    eprintln!("{{\"level\":\"info\",\"event\":\"{event}\",\"message\":{message:?}}}");
}

/// Go `nodeVersions`: one DDL job's per-node reported versions, plus the
/// once-only predicate that fires the moment every node has reported enough.
pub(crate) struct NodeVersions {
    inner: Mutex<NodeVersionsInner>,
}

struct NodeVersionsInner {
    node_versions: HashMap<String, i64>,
    /// Go `onceMatchFn`: installed by `match_or_set`, cleared by the first
    /// `add` that satisfies it.
    once_match_fn: Option<MatchFn>,
}

/// Go's `func(map[string]int64) bool` match predicate.
pub(crate) type MatchFn = Box<dyn FnMut(&HashMap<String, i64>) -> bool + Send>;

impl NodeVersions {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(NodeVersionsInner {
                node_versions: HashMap::new(),
                once_match_fn: None,
            }),
        }
    }

    /// Go `add`.
    pub(crate) fn add(&self, node_id: &str, version: i64) {
        let mut inner = self.lock();
        inner.node_versions.insert(node_id.to_owned(), version);
        // Calling the predicate under the lock mirrors upstream; the
        // predicate only touches its own captures.
        if let Some(mut function) = inner.once_match_fn.take() {
            if !function(&inner.node_versions) {
                // Not satisfied yet: reinstall for the next `add`.
                inner.once_match_fn = Some(function);
            }
        }
    }

    /// Go `del`. The predicate is NOT re-run: only `add` can newly satisfy
    /// it, for the same reason the Go comment gives.
    pub(crate) fn del(&self, node_id: &str) {
        let mut inner = self.lock();
        inner.node_versions.remove(node_id);
    }

    /// Go `len`.
    pub(crate) fn len(&self) -> usize {
        self.lock().node_versions.len()
    }

    /// Go `matchOrSet`: run `function` now; if it does not hold yet, install
    /// it to be retried by future `add`s. Callers must have cleared any
    /// previous predicate first, as upstream requires.
    pub(crate) fn match_or_set(&self, mut function: MatchFn) -> bool {
        let mut inner = self.lock();
        if function(&inner.node_versions) {
            return true;
        }
        inner.once_match_fn = Some(function);
        false
    }

    /// Go `clearData`: keep the map's allocation, drop its contents.
    pub(crate) fn clear_data(&self) {
        let mut inner = self.lock();
        let capacity = inner.node_versions.len();
        inner.node_versions = HashMap::with_capacity(capacity);
    }

    /// Go `clearMatchFn`.
    pub(crate) fn clear_match_fn(&self) {
        self.lock().once_match_fn = None;
    }

    /// Go `emptyAndNotUsed`.
    pub(crate) fn empty_and_not_used(&self) -> bool {
        let inner = self.lock();
        inner.node_versions.is_empty() && inner.once_match_fn.is_none()
    }

    /// Go `getMatchFn`, reduced to what tests assert.
    #[cfg(test)]
    pub(crate) fn has_match_fn(&self) -> bool {
        self.lock().once_match_fn.is_some()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, NodeVersionsInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Go `concurrency.Session` as used by `tidbutil.NewSession`: a granted
/// lease PLUS a keepalive thread; losing the lease closes [`Session::done`],
/// which is exactly the event Go selects on.
pub(crate) struct Session {
    lease: i64,
    done: DoneCh,
}

impl Session {
    /// Go `tidbutil.NewSession`: grant a lease of `SESSION_TTL_SECONDS`,
    /// start keeping it alive under `ctx`. Losing the lease or ending the
    /// context closes [`Self::done`] exactly like
    /// `concurrency.Session.Done()`.
    ///
    /// clientv3's `KeepAlive` retries transport failures internally and only
    /// closes `Done` once the lease is truly gone, so one failed RPC here
    /// must not kill the session: failures accumulate, and `done` closes only
    /// after a full lease TTL elapsed without a single successful round (the
    /// lease has then certainly expired server-side).
    pub(crate) fn new(
        etcd: Arc<dyn EtcdWatchOps>,
        ctx: &Context,
        _log_prefix: &str,
    ) -> Result<Self, String> {
        let lease = etcd.lease_grant(SESSION_TTL_SECONDS)?;
        let (sender, receiver) = mpsc::channel::<()>();
        let keep_alive = Duration::from_secs(SESSION_TTL_SECONDS as u64 / 3);
        const STEP: Duration = Duration::from_millis(20);
        let session_ctx = ctx.clone();
        std::thread::Builder::new()
            .name("ddl-syncer-keepalive".to_owned())
            .spawn(move || {
                // Owned so its drop IS the close of `done`.
                let _keep_sender = sender;
                let mut slept = Duration::ZERO;
                let mut failing_for = Duration::ZERO;
                loop {
                    if session_ctx.err().is_err() {
                        return;
                    }
                    if slept >= keep_alive {
                        slept = Duration::ZERO;
                        if etcd.lease_keep_alive_once(lease).is_ok() {
                            failing_for = Duration::ZERO;
                        } else {
                            failing_for += keep_alive;
                            if failing_for >= Duration::from_secs(SESSION_TTL_SECONDS as u64) {
                                // No keepalive succeeded for a whole lease
                                // TTL: the lease has expired server-side.
                                // Dropping the sender closes every receiver
                                // -- exactly what Go's session.Done() does.
                                return;
                            }
                        }
                    }
                    std::thread::sleep(STEP);
                    slept += STEP;
                }
            })
            .map_err(|error| format!("failed to spawn ddl syncer keepalive: {error}"))?;
        Ok(Self {
            lease,
            done: SharedRecv::new(receiver),
        })
    }

    pub(crate) fn lease(&self) -> i64 {
        self.lease
    }

    pub(crate) fn done(&self) -> DoneCh {
        self.done.clone()
    }
}

/// Go `util.Watcher` reduced to this package's single use: one watch on the
/// global schema version key, re-established by `rewatch`. The stream object
/// itself parks in a thread whose only job is to keep it alive until the
/// next `rewatch` ends it.
struct GlobalVerWatcher {
    etcd: Arc<dyn EtcdWatchOps>,
    current: Mutex<Option<ActiveGlobalWatch>>,
}

struct ActiveGlobalWatch {
    receiver: GlobalVerRx,
    stop: Arc<AtomicBool>,
}

impl GlobalVerWatcher {
    fn new(etcd: Arc<dyn EtcdWatchOps>) -> Self {
        Self {
            etcd,
            current: Mutex::new(None),
        }
    }

    /// Go `Watcher.Watch`: start feeding the global-version channel.
    fn watch(&self, ctx: &Context) {
        self.rewatch(ctx);
    }

    /// Go `Watcher.Rewatch`: end the old watch, start a fresh one. A new
    /// channel replaces the old, exactly as upstream swaps `watchCh`.
    fn rewatch(&self, ctx: &Context) {
        if let Some(previous) = self
            .current
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            previous.stop.store(true, Ordering::Release);
        }
        match self.etcd.watch(DDL_GLOBAL_SCHEMA_VERSION, 0, false, false) {
            Ok(stream) => {
                // A forwarding thread turns the raw stream into Go's watch
                // channel: plain events for the consumer, ended on error.
                let (sender, receiver) = mpsc::channel::<WatchEvent>();
                *self
                    .current
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(ActiveGlobalWatch {
                    receiver: SharedRecv::new(receiver),
                    stop: Arc::clone(&stream.stop),
                });
                let watch_ctx = ctx.clone();
                std::thread::Builder::new()
                    .name("ddl-global-ver-watch".to_owned())
                    .spawn(move || {
                        loop {
                            match stream.events.recv_timeout(Duration::from_millis(50)) {
                                crate::Recv::Item(Ok(event)) => {
                                    if sender.send(event).is_err() {
                                        return;
                                    }
                                }
                                // A failed watch ends this channel, exactly
                                // like Go's closed watchCh; the consumer's
                                // rewatch starts a new one.
                                crate::Recv::Item(Err(_)) | crate::Recv::Closed => return,
                                crate::Recv::Timeout => {
                                    if stream.stop.load(Ordering::Acquire)
                                        || watch_ctx.err().is_err()
                                    {
                                        stream.stop_watching();
                                        return;
                                    }
                                }
                            }
                        }
                    })
                    .ok();
            }
            Err(error) => {
                log_warn("watch global schema version failed", &[("error", &error)]);
            }
        }
    }

    /// Go `Watcher.WatchChan`.
    fn watch_chan(&self) -> GlobalVerRx {
        let current = self
            .current
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &*current {
            Some(active) => SharedRecv::clone(&active.receiver),
            None => never_channel(),
        }
    }
}

fn never_channel<T>() -> SharedRecv<T> {
    let (sender, receiver) = mpsc::channel::<T>();
    std::mem::forget(sender);
    SharedRecv::new(receiver)
}

/// Go `etcdSyncer`, ported whole.
pub struct EtcdSyncer {
    etcd: Arc<dyn EtcdWatchOps>,
    self_schema_ver_path: String,
    ddl_id: String,
    session: Mutex<Option<Arc<Session>>>,
    global_ver_watcher: GlobalVerWatcher,
    all_server_info: Mutex<Option<AllServerInfo>>,
    job_node_versions: Mutex<HashMap<i64, Arc<NodeVersions>>>,
    job_node_ver_prefix: String,
}

/// Go `NewEtcdSyncer`. The server-info syncer is supplied later through
/// [`Syncer::set_server_info_syncer`], exactly like Go.
pub fn new_etcd_syncer(etcd: Arc<dyn EtcdWatchOps>, id: &str) -> EtcdSyncer {
    EtcdSyncer {
        global_ver_watcher: GlobalVerWatcher::new(Arc::clone(&etcd)),
        etcd,
        self_schema_ver_path: format!("{DDL_ALL_SCHEMA_VERSIONS}/{id}"),
        ddl_id: id.to_owned(),
        session: Mutex::new(None),
        all_server_info: Mutex::new(None),
        job_node_versions: Mutex::new(HashMap::new()),
        job_node_ver_prefix: format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/"),
    }
}

impl EtcdSyncer {
    fn load_session(&self) -> Option<Arc<Session>> {
        self.session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn store_session(&self, session: Session) -> Arc<Session> {
        let shared = Arc::new(session);
        *self
            .session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(&shared));
        shared
    }

    /// Go `newSession`, named for its log prefix shape.
    fn new_session(&self, ctx: &Context, retry_count: u64) -> Result<Arc<Session>, String> {
        let log_prefix = format!("[{DDL_PROMPT}] {}", self.self_schema_ver_path);
        let mut last_error = String::new();
        for failed_count in 0..retry_count {
            if let Err(error) = ctx.err() {
                return Err(error.to_string());
            }
            match Session::new(Arc::clone(&self.etcd), ctx, &log_prefix) {
                Ok(session) => return Ok(self.store_session(session)),
                Err(error) => {
                    last_error = error;
                    if failed_count % 15 == 0 {
                        log_warn(
                            "failed to establish new session to etcd",
                            &[("ownerInfo", &log_prefix), ("error", &last_error)],
                        );
                    }
                    std::thread::sleep(NEW_SESSION_RETRY_INTERVAL);
                }
            }
        }
        Err(last_error)
    }

    /// Go `jobSchemaVerMatchOrSet`: get-or-create the job's entry, then run
    /// or install the match predicate -- both under the job-map lock, as
    /// upstream does.
    fn job_schema_ver_match_or_set(&self, job_id: i64, match_fn: MatchFn) -> Arc<NodeVersions> {
        let mut jobs = self
            .job_node_versions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let item = jobs
            .entry(job_id)
            .or_insert_with(|| Arc::new(NodeVersions::new()))
            .clone();
        if !item.match_or_set(match_fn) {
            // Predicate installed; `add` will retry it.
        }
        drop(jobs);
        item
    }
}

impl Syncer for EtcdSyncer {
    /// Go `Init`.
    fn init(&self, ctx: &Context) -> Result<(), String> {
        self.etcd
            .put_if_not_exists(DDL_GLOBAL_SCHEMA_VERSION, INITIAL_VERSION.as_bytes())?;
        let session = self.new_session(ctx, NEW_SESSION_DEFAULT_RETRY_CNT)?;
        self.global_ver_watcher.watch(ctx);
        put_kv_to_etcd(
            ctx,
            self.etcd.as_ref(),
            KEY_OP_DEFAULT_RETRY_CNT,
            &self.self_schema_ver_path,
            INITIAL_VERSION,
            Some(session.lease()),
        )
    }

    /// Go `Done`.
    fn done(&self) -> DoneCh {
        match self.load_session() {
            Some(session) => session.done(),
            None => never_channel(),
        }
    }

    /// Go `Restart`.
    fn restart(&self, ctx: &Context) -> Result<(), String> {
        let session = self.new_session(ctx, NEW_SESSION_RETRY_UNLIMITED)?;
        let child_ctx = Context::with_timeout(ctx, KEY_OP_DEFAULT_TIMEOUT);
        put_kv_to_etcd(
            &child_ctx,
            self.etcd.as_ref(),
            PUT_KEY_RETRY_UNLIMITED,
            &self.self_schema_ver_path,
            INITIAL_VERSION,
            Some(session.lease()),
        )
    }

    /// Go `GlobalVersionCh`.
    fn global_version_ch(&self) -> GlobalVerRx {
        self.global_ver_watcher.watch_chan()
    }

    /// Go `WatchGlobalSchemaVer`.
    fn watch_global_schema_ver(&self, ctx: &Context) {
        self.global_ver_watcher.rewatch(ctx);
    }

    /// Go `UpdateSelfVersion`: MDL ON reports under the job prefix with a
    /// monotonic CAS write; MDL OFF overwrites the leased self path.
    fn update_self_version(&self, ctx: &Context, job_id: i64, version: i64) -> Result<(), String> {
        let version_text = version.to_string();
        if mdl_enabled() {
            if job_id == 0 {
                return Ok(());
            }
            let path = format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/{job_id}/{}", self.ddl_id);
            put_kv_to_etcd_mono(
                ctx,
                self.etcd.as_ref(),
                KEY_OP_DEFAULT_RETRY_CNT,
                &path,
                &version_text,
            )
        } else {
            let lease = self.load_session().map(|session| session.lease());
            put_kv_to_etcd(
                ctx,
                self.etcd.as_ref(),
                PUT_KEY_RETRY_UNLIMITED,
                &self.self_schema_ver_path,
                &version_text,
                lease,
            )
        }
    }

    /// Go `OwnerUpdateGlobalVersion`.
    fn owner_update_global_version(&self, ctx: &Context, version: i64) -> Result<(), String> {
        put_kv_to_etcd(
            ctx,
            self.etcd.as_ref(),
            PUT_KEY_RETRY_UNLIMITED,
            DDL_GLOBAL_SCHEMA_VERSION,
            &version.to_string(),
            None,
        )
    }

    /// Go `WaitVersionSynced`. See the module doc for the two paths.
    fn wait_version_synced(
        &self,
        ctx: &Context,
        job_id: i64,
        latest_ver: i64,
        check_assumed_server: bool,
    ) -> Result<SyncSummary, String> {
        if !mdl_enabled() {
            std::thread::sleep(crate::check_vers_first_wait_time());
        }
        let mut not_match_ver_cnt: usize = 0;
        let interval_cnt =
            usize::try_from(Duration::from_secs(1).as_millis() / CHECK_VERS_INTERVAL.as_millis())
                .unwrap_or(50);

        // MDL OFF: keys already checked this wait are cached here.
        let mut updated_map: HashMap<String, ()> = HashMap::new();

        loop {
            if let Err(error) = ctx.err() {
                return Err(error.to_string());
            }

            if mdl_enabled() {
                // Rebuild the live-server set every round, keeping the NEWEST
                // entry per instance (ip:port): a node that restarted has a
                // new id and the stale one must not be waited on.
                let server_infos = self.get_servers_for_is_sync(check_assumed_server)?;
                let (updated_map_mdl, sync_summary) = calculate_updated_map(&server_infos);

                let (notify_tx, notify_rx) = mpsc::channel::<()>();
                let unmatched: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
                let expected = updated_map_mdl.clone();
                let unmatched_for_fn = Arc::clone(&unmatched);
                let match_fn: MatchFn = Box::new(move |node_versions| {
                    if node_versions.is_empty() {
                        return false;
                    }
                    for (tidb_id, info) in &expected {
                        match node_versions.get(tidb_id) {
                            Some(node_ver) if *node_ver >= latest_ver => {}
                            _ => {
                                *unmatched_for_fn
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                                    Some(info.clone());
                                return false;
                            }
                        }
                    }
                    // Go closes notifyCh here; one queued item is the same
                    // signal on an unbounded channel.
                    let _ = notify_tx.send(());
                    true
                });
                let item = self.job_schema_ver_match_or_set(job_id, match_fn);
                let notify_rx = SharedRecv::new(notify_rx);
                let deadline = std::time::Instant::now() + Duration::from_secs(1);
                loop {
                    if let Err(error) = ctx.err() {
                        item.clear_match_fn();
                        return Err(error.to_string());
                    }
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    if remaining.is_zero() {
                        item.clear_match_fn();
                        let info = unmatched
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .clone();
                        match info {
                            Some(info) => {
                                log_info("syncer check all versions, someone is not synced", &info)
                            }
                            None => {
                                log_info("syncer check all versions, all nodes are not synced", "")
                            }
                        }
                        break;
                    }
                    match notify_rx.recv_timeout(remaining.min(Duration::from_millis(2))) {
                        crate::Recv::Item(()) => return Ok(sync_summary),
                        crate::Recv::Closed | crate::Recv::Timeout => {}
                    }
                }
            } else {
                // Get all the schema versions from etcd.
                let entries = match self.etcd.get_prefix(DDL_ALL_SCHEMA_VERSIONS) {
                    Ok(entries) => entries,
                    Err(_error) => {
                        log_info("syncer check all versions failed, continue checking.", "");
                        continue;
                    }
                };
                let mut succ = true;
                for (key, value) in &entries {
                    let value_text = String::from_utf8_lossy(value).to_string();
                    if updated_map.contains_key(key) {
                        continue;
                    }
                    succ = is_updated_latest_version(
                        key,
                        &value_text,
                        latest_ver,
                        not_match_ver_cnt,
                        interval_cnt,
                    );
                    if !succ {
                        break;
                    }
                    updated_map.insert(key.clone(), ());
                }
                if succ {
                    return Ok(SyncSummary {
                        server_count: updated_map.len(),
                        assumed_server_count: 0,
                    });
                }
                let _ = ctx.sleep(CHECK_VERS_INTERVAL);
                not_match_ver_cnt += 1;
            }
        }
    }

    /// Go `SyncJobSchemaVerLoop`.
    fn sync_job_schema_ver_loop(&self, ctx: &Context) {
        loop {
            self.sync_job_schema_ver(ctx);
            log_info("schema version sync loop interrupted, retrying...", "");
            if ctx.sleep(Duration::from_secs(1)).is_err() {
                return;
            }
        }
    }

    fn set_server_info_syncer(&self, all_server_info: AllServerInfo) {
        *self
            .all_server_info
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(all_server_info);
    }

    /// Go `Close`.
    fn close(&self) {
        if let Err(error) = delete_key_from_etcd(
            self.etcd.as_ref(),
            KEY_OP_DEFAULT_RETRY_CNT,
            &self.self_schema_ver_path,
        ) {
            log_warn("remove self version path failed", &[("error", &error)]);
        }
    }
}

impl EtcdSyncer {
    fn get_servers_for_is_sync(
        &self,
        check_assumed_server: bool,
    ) -> Result<Vec<ServerInfo>, String> {
        let read = self
            .all_server_info
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .expect("server info syncer is required by WaitVersionSynced");
        let mut servers = read()?;
        if tidb_config::kerneltype::is_next_gen() && !check_assumed_server {
            servers.retain(|server| !server.static_info.is_assumed());
        }
        Ok(servers)
    }

    /// Go `syncJobSchemaVer`: one full mirror pass plus an event tail.
    fn sync_job_schema_ver(&self, ctx: &Context) {
        let (entries, revision) = match self.etcd.get_prefix_with_rev(&self.job_node_ver_prefix) {
            Ok(result) => result,
            Err(_error) => {
                log_info("get all job versions failed", "");
                return;
            }
        };
        {
            let mut jobs = self
                .job_node_versions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            jobs.retain(|_, item| {
                // Missed DELETE events leave entries empty and unused; drop them.
                item.clear_data();
                !item.empty_and_not_used()
            });
        }
        for (key, value) in &entries {
            self.handle_job_schema_ver_kv(key, value, false);
        }

        let stream = match self
            .etcd
            .watch(&self.job_node_ver_prefix, revision + 1, true, true)
        {
            Ok(stream) => stream,
            Err(_error) => {
                log_warn("watch job version failed", &[("error", "start failed")]);
                return;
            }
        };
        loop {
            if ctx.err().is_err() {
                stream.stop_watching();
                return;
            }
            match stream.events.recv_timeout(Duration::from_millis(50)) {
                crate::Recv::Item(Ok(event)) => {
                    self.handle_job_schema_ver_kv(&event.key, &event.value, event.deleted);
                }
                crate::Recv::Item(Err(error)) => {
                    log_warn("watch job version failed", &[("error", &error)]);
                    stream.stop_watching();
                    return;
                }
                crate::Recv::Closed => {
                    stream.stop_watching();
                    return;
                }
                crate::Recv::Timeout => {
                    if stream.stop.load(Ordering::Acquire) {
                        return;
                    }
                }
            }
        }
    }

    /// Go `handleJobSchemaVerKV`.
    fn handle_job_schema_ver_kv(&self, key: &str, value: &[u8], deleted: bool) {
        let event_type = if deleted { "DELETE" } else { "PUT" };
        let Some((job_id, tidb_id, schema_ver)) =
            decode_job_version_event(key, value, deleted, &self.job_node_ver_prefix)
        else {
            eprintln!(
                "{{\"level\":\"error\",\"event\":\"invalid job version kv\",\"key\":{key:?},\"type\":{event_type:?}}}"
            );
            return;
        };
        let mut jobs = self
            .job_node_versions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !deleted {
            let item = jobs
                .entry(job_id)
                .or_insert_with(|| Arc::new(NodeVersions::new()));
            let item = Arc::clone(item);
            drop(jobs);
            item.add(&tidb_id, schema_ver);
        } else if let Some(item) = jobs.get(&job_id).cloned() {
            item.del(&tidb_id);
            if item.len() == 0 {
                jobs.remove(&job_id);
            }
        }
    }
}

/// Go `decodeJobVersionEvent`: `<prefix><jobID>/<tidbID>` with a decimal
/// version on PUT; DELETE events carry no value.
fn decode_job_version_event(
    key: &str,
    value: &[u8],
    deleted: bool,
    prefix: &str,
) -> Option<(i64, String, i64)> {
    let left = key.strip_prefix(prefix).unwrap_or(key);
    let parts: Vec<&str> = left.split('/').collect();
    if parts.len() != 2 {
        return None;
    }
    let job_id = parts[0].parse::<i64>().ok()?;
    let mut schema_ver = 0_i64;
    if !deleted {
        schema_ver = std::str::from_utf8(value).ok()?.parse::<i64>().ok()?;
    }
    Some((job_id, parts[1].to_owned(), schema_ver))
}

/// Go `isUpdatedLatestVersion`.
#[allow(clippy::too_many_arguments)]
fn is_updated_latest_version(
    key: &str,
    val: &str,
    latest_ver: i64,
    not_match_ver_cnt: usize,
    interval_cnt: usize,
) -> bool {
    let ver = match val.parse::<i64>() {
        Ok(ver) => ver,
        Err(_error) => {
            eprintln!(
                "{{\"level\":\"info\",\"event\":\"syncer check all versions, convert value to int failed, continue checking.\",\"ddl\":{key:?},\"value\":{val:?}}}"
            );
            return false;
        }
    };
    if ver < latest_ver {
        if not_match_ver_cnt.is_multiple_of(interval_cnt) {
            eprintln!(
                "{{\"level\":\"info\",\"event\":\"syncer check all versions, someone is not synced, continue checking\",\"ddl\":{key:?},\"currentVer\":{ver},\"latestVer\":{latest_ver}}}"
            );
        }
        return false;
    }
    true
}

/// Go `calculateUpdatedMap`.
fn calculate_updated_map(server_infos: &[ServerInfo]) -> (HashMap<String, String>, SyncSummary) {
    let mut updated_map = HashMap::new();
    let mut instance_to_server: HashMap<String, (String, i64, bool)> = HashMap::new();
    let mut assumed_server_count = 0usize;

    for info in server_infos {
        let instance = join_host_port(&info.static_info.ip, info.static_info.port);
        let is_assumed = info.static_info.is_assumed();
        match instance_to_server.get(&instance).cloned() {
            Some((existing_id, existing_start, existing_assumed)) => {
                if info.static_info.start_timestamp > existing_start {
                    updated_map.remove(&existing_id);
                    if existing_assumed {
                        assumed_server_count -= 1;
                    }
                    updated_map.insert(info.static_info.id.clone(), get_server_info_for_log(info));
                    instance_to_server.insert(
                        instance,
                        (
                            info.static_info.id.clone(),
                            info.static_info.start_timestamp,
                            is_assumed,
                        ),
                    );
                    if is_assumed {
                        assumed_server_count += 1;
                    }
                }
            }
            None => {
                updated_map.insert(info.static_info.id.clone(), get_server_info_for_log(info));
                instance_to_server.insert(
                    instance,
                    (
                        info.static_info.id.clone(),
                        info.static_info.start_timestamp,
                        is_assumed,
                    ),
                );
                if is_assumed {
                    assumed_server_count += 1;
                }
            }
        }
    }

    (
        updated_map,
        SyncSummary {
            server_count: instance_to_server.len(),
            assumed_server_count,
        },
    )
}

/// Go `getSvrInfoForLog`.
fn get_server_info_for_log(info: &ServerInfo) -> String {
    if info.static_info.is_assumed() {
        format!(
            "instance ip {}, port {}, id {}, origin keyspace {}",
            info.static_info.ip,
            info.static_info.port,
            info.static_info.id,
            info.static_info.keyspace
        )
    } else {
        format!(
            "instance ip {}, port {}, id {}",
            info.static_info.ip, info.static_info.port, info.static_info.id
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering as AtomicOrdering};
    use std::sync::mpsc;
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    use tidb_domain::serverinfo::{DynamicInfo, ServerInfo, StaticInfo};

    use super::*;
    use crate::{
        globals_test_lock, set_check_vers_first_wait_time, Context, Syncer,
        DDL_ALL_SCHEMA_VERSIONS_BY_JOB, DDL_GLOBAL_SCHEMA_VERSION,
    };

    /// One stored key: value, mod revision, lease.
    #[derive(Clone, Debug)]
    struct FakeEntry {
        value: Vec<u8>,
        mod_rev: i64,
        #[allow(dead_code)]
        lease: Option<i64>,
    }

    #[derive(Default)]
    struct FakeState {
        kv: Mutex<BTreeMap<String, FakeEntry>>,
        /// Revision-ordered MVCC events retained for watch replay.
        events: Mutex<Vec<(i64, WatchEvent)>>,
        cond: Condvar,
        revision: AtomicI64,
        leases: AtomicI64,
        keepalive_failures: AtomicBool,
        /// Simulates network latency on single-key reads so concurrent
        /// read-modify-write windows genuinely overlap, like they do against
        /// real etcd.
        read_delay_micros: AtomicU64,
        /// Go `mockCompaction`: fail this many newly-created watch streams.
        watch_failures: AtomicU64,
    }

    /// A single-node in-process etcd stand-in with working watches and CAS.
    #[derive(Clone, Default)]
    struct FakeEtcd {
        state: Arc<FakeState>,
        server_infos: Arc<Mutex<Vec<ServerInfo>>>,
    }

    impl FakeEtcd {
        fn put_raw(&self, key: &str, value: &str) {
            let mut kv = self.state.kv.lock().unwrap_or_else(|e| e.into_inner());
            let rev = self.state.revision.fetch_add(1, AtomicOrdering::SeqCst) + 1;
            kv.insert(
                key.to_owned(),
                FakeEntry {
                    value: value.as_bytes().to_vec(),
                    mod_rev: rev,
                    lease: None,
                },
            );
            self.state
                .events
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((
                    rev,
                    WatchEvent {
                        key: key.to_owned(),
                        value: value.as_bytes().to_vec(),
                        deleted: false,
                    },
                ));
            self.state.cond.notify_all();
        }

        fn delete_raw(&self, key: &str) {
            let mut kv = self.state.kv.lock().unwrap_or_else(|e| e.into_inner());
            let existed = kv.remove(key).is_some();
            drop(kv);
            if existed {
                let rev = self.state.revision.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                self.state
                    .events
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push((
                        rev,
                        WatchEvent {
                            key: key.to_owned(),
                            value: Vec::new(),
                            deleted: true,
                        },
                    ));
                self.state.cond.notify_all();
            }
        }

        fn get_value(&self, key: &str) -> Option<Vec<u8>> {
            let kv = self.state.kv.lock().unwrap_or_else(|e| e.into_inner());
            kv.get(key).map(|entry| entry.value.clone())
        }

        fn set_server_infos(&self, infos: &[(&str, &str, usize, i64)]) {
            let mut all = self.server_infos.lock().unwrap_or_else(|e| e.into_inner());
            *all = infos
                .iter()
                .map(|(id, ip, port, start)| ServerInfo {
                    static_info: StaticInfo {
                        id: (*id).to_owned(),
                        ip: (*ip).to_owned(),
                        port: *port,
                        start_timestamp: *start,
                        ..StaticInfo::default()
                    },
                    dynamic_info: DynamicInfo::default(),
                })
                .collect();
        }

        fn all_server_info_fn(etcd: &FakeEtcd) -> crate::etcd_syncer::AllServerInfo {
            let state = Arc::clone(&etcd.server_infos);
            Arc::new(move || Ok(state.lock().unwrap_or_else(|e| e.into_inner()).clone()))
        }
    }

    impl tidb_domain::serverinfo_syncer::EtcdOps for FakeEtcd {
        fn lease_grant(&self, _ttl_seconds: i64) -> Result<i64, String> {
            Ok(self.state.leases.fetch_add(1, AtomicOrdering::SeqCst) + 1000)
        }
        fn lease_keep_alive_once(&self, _lease: i64) -> Result<(), String> {
            if self.state.keepalive_failures.load(AtomicOrdering::Acquire) {
                Err("lease expired".to_owned())
            } else {
                Ok(())
            }
        }
        fn lease_revoke(&self, _lease: i64) -> Result<(), String> {
            Ok(())
        }
        fn put_with_lease(&self, key: &str, value: &[u8], lease: i64) -> Result<(), String> {
            let mut kv = self.state.kv.lock().unwrap_or_else(|e| e.into_inner());
            let rev = self.state.revision.fetch_add(1, AtomicOrdering::SeqCst) + 1;
            kv.insert(
                key.to_owned(),
                FakeEntry {
                    value: value.to_vec(),
                    mod_rev: rev,
                    lease: Some(lease),
                },
            );
            self.state
                .events
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((
                    rev,
                    WatchEvent {
                        key: key.to_owned(),
                        value: value.to_vec(),
                        deleted: false,
                    },
                ));
            self.state.cond.notify_all();
            Ok(())
        }
        fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
            Ok(self.get_prefix_with_rev(prefix)?.0)
        }
        fn delete(&self, key: &str) -> Result<(), String> {
            self.delete_raw(key);
            Ok(())
        }
        fn put(&self, key: &str, value: &[u8]) -> Result<(), String> {
            let text = String::from_utf8(value.to_vec()).map_err(|e| e.to_string())?;
            self.put_raw(key, &text);
            Ok(())
        }
        fn delete_prefix(&self, prefix: &str) -> Result<(), String> {
            let mut kv = self.state.kv.lock().unwrap_or_else(|e| e.into_inner());
            let doomed: Vec<String> = kv
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect();
            if !doomed.is_empty() {
                let rev = self.state.revision.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                for key in &doomed {
                    kv.remove(key);
                }
                self.state
                    .events
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .extend(doomed.into_iter().map(|key| {
                        (
                            rev,
                            WatchEvent {
                                key,
                                value: Vec::new(),
                                deleted: true,
                            },
                        )
                    }));
                self.state.cond.notify_all();
            }
            Ok(())
        }
    }

    impl EtcdWatchOps for FakeEtcd {
        fn get_prefix_with_rev(
            &self,
            prefix: &str,
        ) -> Result<(Vec<(String, Vec<u8>)>, i64), String> {
            let kv = self.state.kv.lock().unwrap_or_else(|e| e.into_inner());
            let entries = kv
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .map(|(key, entry)| (key.clone(), entry.value.clone()))
                .collect();
            Ok((entries, self.state.revision.load(AtomicOrdering::SeqCst)))
        }
        fn get_with_mod_revision(&self, key: &str) -> Result<(Option<Vec<u8>>, i64), String> {
            let delay = self.state.read_delay_micros.load(AtomicOrdering::Acquire);
            if delay > 0 {
                std::thread::sleep(Duration::from_micros(delay));
            }
            let kv = self.state.kv.lock().unwrap_or_else(|e| e.into_inner());
            Ok(match kv.get(key) {
                Some(entry) => (Some(entry.value.clone()), entry.mod_rev),
                None => (None, 0),
            })
        }
        fn compare_and_swap(
            &self,
            key: &str,
            expected_mod_revision: i64,
            value: &[u8],
        ) -> Result<bool, String> {
            let mut kv = self.state.kv.lock().unwrap_or_else(|e| e.into_inner());
            let current = kv.get(key).map(|entry| entry.mod_rev).unwrap_or(0);
            if current != expected_mod_revision {
                return Ok(false);
            }
            let rev = self.state.revision.fetch_add(1, AtomicOrdering::SeqCst) + 1;
            let previous_lease = kv.get(key).and_then(|entry| entry.lease);
            kv.insert(
                key.to_owned(),
                FakeEntry {
                    value: value.to_vec(),
                    mod_rev: rev,
                    lease: previous_lease,
                },
            );
            self.state
                .events
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((
                    rev,
                    WatchEvent {
                        key: key.to_owned(),
                        value: value.to_vec(),
                        deleted: false,
                    },
                ));
            self.state.cond.notify_all();
            Ok(true)
        }
        fn put_if_not_exists(&self, key: &str, value: &[u8]) -> Result<bool, String> {
            let mut kv = self.state.kv.lock().unwrap_or_else(|e| e.into_inner());
            if kv.contains_key(key) {
                return Ok(false);
            }
            let rev = self.state.revision.fetch_add(1, AtomicOrdering::SeqCst) + 1;
            kv.insert(
                key.to_owned(),
                FakeEntry {
                    value: value.to_vec(),
                    mod_rev: rev,
                    lease: None,
                },
            );
            self.state
                .events
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((
                    rev,
                    WatchEvent {
                        key: key.to_owned(),
                        value: value.to_vec(),
                        deleted: false,
                    },
                ));
            self.state.cond.notify_all();
            Ok(true)
        }
        fn watch(
            &self,
            key: &str,
            start_revision: i64,
            with_prefix: bool,
            _require_leader: bool,
        ) -> Result<WatchStream, String> {
            let (sender, receiver) = mpsc::channel::<Result<WatchEvent, String>>();
            let stop = Arc::new(AtomicBool::new(false));
            let state = Arc::clone(&self.state);
            let key = key.to_owned();
            let thread_stop = Arc::clone(&stop);
            // An etcd watch without `WithRev` starts after the revision
            // current when the watch is created; an explicit revision is
            // inclusive.
            let start_revision = if start_revision == 0 {
                state.revision.load(AtomicOrdering::SeqCst) + 1
            } else {
                start_revision
            };
            std::thread::Builder::new()
                .name("fake-etcd-watch".to_owned())
                .spawn(move || {
                    let mut next_revision = start_revision;
                    loop {
                        if state
                            .watch_failures
                            .try_update(
                                AtomicOrdering::AcqRel,
                                AtomicOrdering::Acquire,
                                |remaining| remaining.checked_sub(1),
                            )
                            .is_ok()
                        {
                            let _ =
                                sender.send(Err("required revision has been compacted".to_owned()));
                            return;
                        }
                        let events: Vec<(i64, WatchEvent)> = state
                            .events
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .iter()
                            .filter(|(revision, event)| {
                                *revision >= next_revision
                                    && if with_prefix {
                                        event.key.starts_with(&key)
                                    } else {
                                        event.key == key
                                    }
                            })
                            .cloned()
                            .collect();
                        for (revision, event) in events {
                            if thread_stop.load(AtomicOrdering::Acquire) {
                                return;
                            }
                            if sender.send(Ok(event)).is_err() {
                                return;
                            }
                            next_revision = next_revision.max(revision + 1);
                        }
                        if thread_stop.load(AtomicOrdering::Acquire) {
                            return;
                        }
                        let kv = state.kv.lock().unwrap_or_else(|e| e.into_inner());
                        let _unused_guard = state.cond.wait_timeout(kv, Duration::from_millis(2));
                    }
                })
                .map_err(|error| error.to_string())?;
            Ok(WatchStream {
                events: SharedRecv::new(receiver),
                stop,
            })
        }
    }

    fn new_syncer(etcd: &FakeEtcd) -> EtcdSyncer {
        let syncer = new_etcd_syncer(Arc::new(etcd.clone()), "1111");
        syncer.set_server_info_syncer(FakeEtcd::all_server_info_fn(etcd));
        syncer
    }

    /// clientv3 retries keepalive transport failures instead of killing the
    /// session, so one failed round must not close `done`.
    #[test]
    fn session_survives_transient_keepalive_failures() {
        let etcd = FakeEtcd::default();
        let syncer = new_syncer(&etcd);
        let ctx = Context::background();
        syncer.init(&ctx).expect("init must grant a lease");

        etcd.state
            .keepalive_failures
            .store(true, AtomicOrdering::Release);
        let done = syncer.done();
        // A whole lease TTL of continuous failure is needed before `done`
        // may close; well inside that window it must still be open.
        assert!(
            matches!(
                done.recv_timeout(Duration::from_millis(300)),
                crate::Recv::Timeout
            ),
            "a transient keepalive failure must not close the session"
        );

        // Context end still tears the session down promptly.
        let etcd2 = FakeEtcd::default();
        let syncer2 = new_syncer(&etcd2);
        let parent = Context::background();
        let ctx2 = Context::with_cancel(&parent);
        syncer2.init(&ctx2).expect("init must grant a lease");
        let done2 = syncer2.done();
        ctx2.cancel();
        assert!(
            matches!(
                done2.recv_timeout(Duration::from_secs(5)),
                crate::Recv::Closed
            ),
            "context end must close the session"
        );
    }

    fn server_info(id: &str, ip: &str, start: i64, keyspace: &str, assumed: &str) -> ServerInfo {
        ServerInfo {
            static_info: StaticInfo {
                id: id.to_owned(),
                ip: ip.to_owned(),
                start_timestamp: start,
                keyspace: keyspace.to_owned(),
                assumed_keyspace: assumed.to_owned(),
                ..StaticInfo::default()
            },
            dynamic_info: DynamicInfo::default(),
        }
    }

    // ---- Go TestNodeVersions ----

    #[test]
    fn test_node_versions() {
        let nv = NodeVersions::new();
        assert!(nv.empty_and_not_used());
        nv.add("a", 10);
        nv.add("b", 20);
        assert!(!nv.empty_and_not_used());
        assert_eq!(2, nv.len());
        struct Watermark(i64);
        let water_mark = Arc::new(Mutex::new(Watermark(10)));
        let make_fn = |water_mark: Arc<Mutex<Watermark>>| {
            Box::new(move |node_versions: &HashMap<String, i64>| {
                let mark = water_mark.lock().unwrap().0;
                node_versions.values().all(|v| *v >= mark)
            }) as MatchFn
        };
        assert!(
            nv.match_or_set(make_fn(Arc::clone(&water_mark))),
            "matched now"
        );
        assert!(!nv.has_match_fn()); // matched immediately
        *water_mark.lock().unwrap() = Watermark(20);
        assert!(!nv.match_or_set(make_fn(Arc::clone(&water_mark))));
        assert!(nv.has_match_fn());
        // matched and cleared
        nv.add("a", 20);
        assert!(!nv.has_match_fn());
        nv.del("a");
        assert_eq!(1, nv.len());
        nv.del("b");
        assert!(nv.empty_and_not_used());
        assert!(!nv.match_or_set(Box::new(|_| false)));
        assert!(!nv.empty_and_not_used());
    }

    // ---- Go TestDecodeJobVersionEvent ----

    #[test]
    fn test_decode_job_version_event() {
        let prefix = format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/");
        assert!(decode_job_version_event(&format!("{prefix}1"), b"", false, &prefix).is_none());
        assert!(decode_job_version_event(&format!("{prefix}a/aa"), b"", false, &prefix).is_none());
        assert!(
            decode_job_version_event(&format!("{prefix}1/aa"), b"aa", false, &prefix).is_none()
        );
        let (job_id, tidb_id, schema_ver) =
            decode_job_version_event(&format!("{prefix}1/aa"), b"123", false, &prefix).unwrap();
        assert_eq!((job_id, tidb_id.as_str(), schema_ver), (1, "aa", 123));
        // value is not used on delete
        let (job_id, tidb_id, schema_ver) =
            decode_job_version_event(&format!("{prefix}1/aa"), b"aaaa", true, &prefix).unwrap();
        assert_eq!((job_id, tidb_id.as_str(), schema_ver), (1, "aa", 0));
    }

    // ---- Go TestSyncJobSchemaVerLoop ----

    #[test]
    fn test_sync_job_schema_ver_loop() {
        let etcd = FakeEtcd::default();
        let ctx = Context::background();
        etcd.put_raw(&format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/1/aa"), "123");
        let syncer = Arc::new(new_syncer(&etcd));
        let loop_ctx = Context::with_cancel(&ctx);
        let loop_ctx_for_thread = loop_ctx.clone();
        let loop_handle = {
            let syncer = Arc::clone(&syncer);
            std::thread::Builder::new()
                .spawn(move || syncer.sync_job_schema_ver_loop(&loop_ctx_for_thread))
                .unwrap()
        };

        // job 1 is matched. The mirror pass may or may not have run before
        // the predicate installs; both orders converge on a notification.
        let (notify_tx, notify_rx) = mpsc::channel::<()>();
        {
            let notify_tx = notify_tx.clone();
            let item = syncer.job_schema_ver_match_or_set(
                1,
                Box::new(move |m| {
                    if m.values().all(|v| *v >= 123) {
                        let _ = notify_tx.send(());
                        true
                    } else {
                        false
                    }
                }),
            );
            assert_eq!((), notify_rx.recv_timeout(Duration::from_secs(5)).unwrap());
            assert!(!item.has_match_fn());
        }
        etcd.delete_raw(&format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/1/aa"));

        // job 2 requires aa and bb
        {
            let (notify_tx, notify_rx) = mpsc::channel::<()>();
            let tx2 = notify_tx.clone();
            let item = syncer.job_schema_ver_match_or_set(
                2,
                Box::new(move |m| {
                    for node_id in ["aa", "bb"] {
                        match m.get(node_id) {
                            Some(v) if *v >= 123 => {}
                            _ => return false,
                        }
                    }
                    let _ = tx2.send(());
                    true
                }),
            );
            assert!(item.has_match_fn());
            assert!(matches!(
                notify_rx.recv_timeout(Duration::from_millis(200)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ));
            etcd.put_raw(&format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/2/aa"), "123");
            etcd.put_raw(&format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/2/bb"), "124");
            assert_eq!((), notify_rx.recv_timeout(Duration::from_secs(5)).unwrap());
            assert!(!item.has_match_fn());
            // Job 1's emptied entry must be gone; the mirror loop needs a
            // moment to observe the delete.
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                let count = syncer.job_node_versions.lock().unwrap().len();
                if count == 1 {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "job 1's emptied entry must be gone"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            etcd.delete_raw(&format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/2/aa"));
            etcd.delete_raw(&format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/2/bb"));
        }

        // job 3 is matched after the watch restarts from Go's injected
        // compaction error.
        {
            etcd.state.watch_failures.store(1, AtomicOrdering::Release);
            let (notify_tx, notify_rx) = mpsc::channel::<()>();
            let item = syncer.job_schema_ver_match_or_set(
                3,
                Box::new(move |versions| {
                    if versions.get("aa").is_some_and(|version| *version >= 123) {
                        let _ = notify_tx.send(());
                        true
                    } else {
                        false
                    }
                }),
            );
            assert!(item.has_match_fn());
            etcd.put_raw(&format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/3/aa"), "123");
            notify_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("job 3 must match after compaction restart");
            assert!(!item.has_match_fn());
            etcd.delete_raw(&format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/3/aa"));
        }

        // job 4 is matched using WaitVersionSynced.
        {
            let guard = globals_test_lock();
            tidb_vardef::set_enable_mdl(true);
            etcd.set_server_infos(&[("aa", "test", 4000, 1)]);
            etcd.put_raw(&format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/4/aa"), "333");
            assert_eq!(
                syncer.wait_version_synced(&ctx, 4, 333, false).unwrap(),
                SyncSummary {
                    server_count: 1,
                    assumed_server_count: 0,
                }
            );
            etcd.delete_raw(&format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/4/aa"));
            tidb_vardef::set_enable_mdl(false);
            drop(guard);
        }

        loop_ctx.cancel();
        loop_handle.join().unwrap();
    }

    // ---- Go TestCalculateUpdatedMap ----

    #[test]
    fn test_calculate_updated_map() {
        let (updated, summary) = calculate_updated_map(&[
            server_info("a", "a", 0, "", ""),
            server_info("b", "b", 0, "", ""),
            server_info("c", "c", 0, "", ""),
        ]);
        assert_eq!(updated.len(), 3);
        assert_eq!(
            summary,
            SyncSummary {
                server_count: 3,
                assumed_server_count: 0,
            }
        );

        let (updated, summary) = calculate_updated_map(&[
            server_info("a", "a", 0, "", ""),
            server_info("b", "b", 0, "", ""),
            server_info("c", "c", 0, "", "a"),
        ]);
        assert_eq!(updated.len(), 3);
        assert_eq!(
            summary,
            SyncSummary {
                server_count: 3,
                assumed_server_count: 1,
            }
        );

        let (updated, summary) = calculate_updated_map(&[
            server_info("a", "a", 100, "", ""),
            server_info("b", "a", 200, "", ""),
            server_info("c", "a", 300, "", "a"),
        ]);
        assert_eq!(updated.len(), 1);
        assert_eq!(
            summary,
            SyncSummary {
                server_count: 1,
                assumed_server_count: 1,
            }
        );

        let (updated, summary) = calculate_updated_map(&[
            server_info("a", "a", 100, "", ""),
            server_info("b", "a", 200, "", "a"),
            server_info("c", "a", 300, "", ""),
        ]);
        assert_eq!(updated.len(), 1);
        assert_eq!(
            summary,
            SyncSummary {
                server_count: 1,
                assumed_server_count: 0,
            }
        );
    }

    // ---- Go TestGetServersForISSync ----

    #[test]
    fn test_get_servers_for_is_sync() {
        let etcd = FakeEtcd::default();
        *etcd.server_infos.lock().unwrap() = vec![
            server_info("s1", "", 0, "ks1", ""),
            server_info("s2", "", 0, "ks1", ""),
            server_info("s3", "", 0, "ks1", "SYSTEM"),
        ];
        let syncer = new_syncer(&etcd);

        let servers = syncer.get_servers_for_is_sync(false).unwrap();
        if tidb_config::kerneltype::is_classic() {
            assert_eq!(servers.len(), 3);
        } else {
            assert_eq!(servers.len(), 2);
            assert!(servers
                .iter()
                .all(|server| !server.static_info.is_assumed()));
        }

        assert_eq!(syncer.get_servers_for_is_sync(true).unwrap().len(), 3);
    }

    // ---- Go TestSyncerSimple (MDL OFF) ----

    #[test]
    fn test_syncer_simple() {
        if tidb_config::kerneltype::is_next_gen() {
            // Go skips this MDL-off test because MDL is always enabled in
            // next-generation TiDB.
            return;
        }
        let guard = globals_test_lock();
        tidb_vardef::set_enable_mdl(false);
        let origin = crate::check_vers_first_wait_time();
        set_check_vers_first_wait_time(Duration::ZERO);

        let etcd = FakeEtcd::default();
        let ctx = Context::background();
        let one = new_etcd_syncer(Arc::new(etcd.clone()), "1");
        one.set_server_info_syncer(FakeEtcd::all_server_info_fn(&etcd));
        let two = new_etcd_syncer(Arc::new(etcd.clone()), "2");
        two.set_server_info_syncer(FakeEtcd::all_server_info_fn(&etcd));
        one.init(&ctx).unwrap();
        two.init(&ctx).unwrap();

        for id in ["1", "2"] {
            let key = format!("{}/{}", crate::DDL_ALL_SCHEMA_VERSIONS, id);
            assert_eq!(
                Some(crate::INITIAL_VERSION.as_bytes().to_vec()),
                etcd.get_value(&key),
                "self path seeded"
            );
            // The session's lease must be attached to the seeded key.
            let (_, rev) = etcd.get_with_mod_revision(&key).unwrap();
            assert!(rev > 0);
        }

        // for watchCh: OwnerUpdateGlobalVersion must reach GlobalVersionCh.
        let watch_rx = one.global_version_ch();
        let (tx, rx) = mpsc::channel::<WatchEvent>();
        std::thread::spawn(move || {
            if let Some(event) = watch_rx_next(&watch_rx, Duration::from_secs(3)) {
                let _ = tx.send(event);
            }
        });
        let current_ver: i64 = 123;
        one.owner_update_global_version(&ctx, current_ver).unwrap();
        let event = rx
            .recv_timeout(Duration::from_secs(3))
            .expect("get update version failed");
        assert_eq!(DDL_GLOBAL_SCHEMA_VERSION, event.key);
        assert_eq!(current_ver.to_string().as_bytes(), event.value.as_slice());

        // for CheckAllVersions: nothing has reported 123 yet.
        let child = Context::with_timeout(&ctx, Duration::from_millis(200));
        assert!(is_deadline_error(one.wait_version_synced(
            &child,
            0,
            current_ver,
            false,
        )));

        // for UpdateSelfVersion (non-MDL: leased writes to the self path).
        one.update_self_version(&ctx, 0, current_ver).unwrap();
        two.update_self_version(&ctx, 0, current_ver).unwrap();

        // A spent context fails the write immediately.
        let tiny = Context::with_timeout(&ctx, Duration::ZERO);
        assert!(two.update_self_version(&tiny, 0, current_ver).is_err());

        // for CheckAllVersions after both reported.
        assert_eq!(
            one.wait_version_synced(&ctx, 0, current_ver - 1, false)
                .unwrap(),
            SyncSummary {
                server_count: 2,
                assumed_server_count: 0,
            }
        );
        assert_eq!(
            one.wait_version_synced(&ctx, 0, current_ver, false)
                .unwrap(),
            SyncSummary {
                server_count: 2,
                assumed_server_count: 0,
            }
        );

        let tiny = Context::with_timeout(&ctx, Duration::ZERO);
        assert!(is_deadline_error(one.wait_version_synced(
            &tiny,
            0,
            current_ver,
            false,
        )));

        // for Close
        let key = format!("{}/1", crate::DDL_ALL_SCHEMA_VERSIONS);
        assert_eq!(
            Some(current_ver.to_string().as_bytes().to_vec()),
            etcd.get_value(&key)
        );
        one.close();
        assert_eq!(None, etcd.get_value(&key));

        two.close();
        set_check_vers_first_wait_time(origin);
        tidb_vardef::set_enable_mdl(false);
        drop(guard);
    }

    fn watch_rx_next(rx: &GlobalVerRx, wait: Duration) -> Option<WatchEvent> {
        let deadline = std::time::Instant::now() + wait;
        while std::time::Instant::now() < deadline {
            match rx.recv_timeout(Duration::from_millis(20)) {
                crate::Recv::Item(item) => return Some(item),
                crate::Recv::Closed => return None,
                crate::Recv::Timeout => {}
            }
        }
        None
    }

    fn is_deadline_error(result: Result<SyncSummary, String>) -> bool {
        matches!(result, Err(message) if message.contains("deadline exceeded"))
    }

    /// Go `TestPutKVToEtcdMono`'s contract: monotonic under contention.
    #[test]
    fn test_put_kv_to_etcd_mono() {
        let etcd = FakeEtcd::default();
        let ctx = Context::background();

        put_kv_to_etcd_mono(&ctx, &etcd, 3, "testKey", "1").unwrap();
        put_kv_to_etcd_mono(&ctx, &etcd, 3, "testKey", "2").unwrap();
        put_kv_to_etcd_mono(&ctx, &etcd, 3, "testKey", "3").unwrap();

        // Concurrent mono puts SHOULD conflict and fail somewhere. A little
        // read latency widens the read-modify-write window, which is how the
        // real cluster behaves.
        etcd.state
            .read_delay_micros
            .store(800, AtomicOrdering::Release);
        let mut handles = Vec::new();
        for _ in 0..30 {
            let etcd = etcd.clone();
            let ctx = ctx.clone();
            handles.push(std::thread::spawn(move || {
                put_kv_to_etcd_mono(&ctx, &etcd, 1, "testKey", "5")
            }));
        }
        let results: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert!(
            results.iter().any(Result::is_err),
            "PutKVToEtcdMono should be conflicted and get errors"
        );

        // Concurrent plain puts all succeed.
        let mut handles = Vec::new();
        for _ in 0..30 {
            let etcd = etcd.clone();
            let ctx = ctx.clone();
            handles.push(std::thread::spawn(move || {
                put_kv_to_etcd(&ctx, &etcd, 1, "testKey", "5", None)
            }));
        }
        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        etcd.state
            .read_delay_micros
            .store(0, AtomicOrdering::Release);
        put_kv_to_etcd_mono(&ctx, &etcd, 3, "testKey", "1").unwrap();
        assert_eq!(Some(b"1".to_vec()), etcd.get_value("testKey"));
    }

    #[test]
    #[deny(unused_must_use)]
    fn return_values_may_be_ignored_like_go() {
        // Go permits callers to discard ordinary constructor/getter results;
        // these Rust-shaped wrappers must not impose a Rust-only diagnostic.
        Context::background();
        let parent = Context::background();
        Context::with_cancel(&parent);
        Context::with_timeout(&parent, Duration::from_secs(1));
        crate::check_vers_first_wait_time();
        crate::mem_syncer::MemSyncer::new();
        new_etcd_syncer(Arc::new(FakeEtcd::default()), "ignored");
    }
}
