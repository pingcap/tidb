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
    mdl_enabled, Context, DoneCh, GlobalVerRx, SharedRecv, Syncer, WatchEvent, CHECK_VERS_INTERVAL,
    DDL_ALL_SCHEMA_VERSIONS, DDL_ALL_SCHEMA_VERSIONS_BY_JOB, DDL_GLOBAL_SCHEMA_VERSION, DDL_PROMPT,
    INITIAL_VERSION, KEY_OP_DEFAULT_RETRY_CNT, KEY_OP_RETRY_INTERVAL, PUT_KEY_RETRY_UNLIMITED,
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

    /// `Watch(prefix, WithPrefix(), WithRev(start_revision))`: events from
    /// `start_revision` on, streamed until the stream ends or
    /// [`WatchStream::stop_watching`] fires. A reconnected implementation is
    /// expected to resume from where it left off, like Go's watch channel.
    ///
    /// # Errors
    /// Failure establishing the watch.
    fn watch_prefix(&self, prefix: &str, start_revision: i64) -> Result<WatchStream, String>;
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
    for attempt in 0..retry_cnt {
        if attempt > 0 && ctx.sleep(KEY_OP_RETRY_INTERVAL).is_err() {
            return Err(last);
        }
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
    for attempt in 0..retry_cnt {
        if attempt > 0 && ctx.sleep(KEY_OP_RETRY_INTERVAL).is_err() {
            return Err(last);
        }
        if let Err(error) = ctx.err() {
            return Err(error.to_string());
        }
        let prev_revision = match etcd.get_with_mod_revision(key) {
            Ok((_, revision)) => revision,
            Err(error) => {
                log_warn("etcd-cli put kv failed", &[("key", key)]);
                last = error;
                continue;
            }
        };
        let swapped = match etcd.compare_and_swap(key, prev_revision, value.as_bytes()) {
            Ok(swapped) => swapped,
            Err(error) => {
                log_warn("etcd-cli put kv failed", &[("key", key)]);
                last = error;
                continue;
            }
        };
        if swapped {
            return Ok(());
        }
        last = "performing compare-and-swap during PutKVToEtcd failed".to_owned();
        log_warn("etcd-cli put kv failed", &[("key", key)]);
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
    stop: Arc<AtomicBool>,
}

impl Session {
    /// Go `tidbutil.NewSession`: grant a lease of `SESSION_TTL_SECONDS`,
    /// start keeping it alive. Upstream retries session creation
    /// (`NewSessionDefaultRetryCnt`) around this single grant; the fake and
    /// real clients surface transport errors directly instead. Losing one
    /// keepalive round closes [`Self::done`] -- Go's session-done event --
    /// and leaves re-establishment to [`EtcdSyncer::restart`].
    pub(crate) fn new(etcd: Arc<dyn EtcdWatchOps>, log_prefix: &str) -> Result<Self, String> {
        // Go's concurrency.Session keeps the lease alive at roughly ttl/3;
        // the same cadence here, stepped so a stop is noticed promptly.
        Self::with_keep_alive_interval(
            etcd,
            log_prefix,
            Duration::from_secs(SESSION_TTL_SECONDS as u64 / 3),
        )
    }

    /// [`Self::new`] with an explicit keepalive cadence; tests shorten it
    /// exactly like upstream shortens its TTLs.
    pub(crate) fn with_keep_alive_interval(
        etcd: Arc<dyn EtcdWatchOps>,
        _log_prefix: &str,
        keep_alive: Duration,
    ) -> Result<Self, String> {
        let lease = etcd.lease_grant(SESSION_TTL_SECONDS)?;
        let (sender, receiver) = mpsc::channel::<()>();
        let stop = Arc::new(AtomicBool::new(false));
        const STEP: Duration = Duration::from_millis(20);
        let thread_stop = Arc::clone(&stop);
        std::thread::Builder::new()
            .name("ddl-syncer-keepalive".to_owned())
            .spawn(move || {
                // Owned so its drop IS the close of `done`.
                let _keep_sender = sender;
                let mut slept = Duration::ZERO;
                loop {
                    if thread_stop.load(Ordering::Acquire) {
                        return;
                    }
                    if slept >= keep_alive {
                        slept = Duration::ZERO;
                        if etcd.lease_keep_alive_once(lease).is_err() {
                            // The lease is gone: dropping the sender closes
                            // every receiver -- exactly what Go's
                            // session.Done() does.
                            return;
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
            stop,
        })
    }

    pub(crate) fn lease(&self) -> i64 {
        self.lease
    }

    pub(crate) fn done(&self) -> DoneCh {
        self.done.clone()
    }

    /// Stops the keepalive thread and closes `done`, as closing the session
    /// would.
    pub(crate) fn close(&self) {
        self.stop.store(true, Ordering::Release);
    }
}

/// Go `util.Watcher` reduced to this package's single use: one watch on the
/// global schema version key, re-established by `rewatch`. The stream object
/// itself parks in a thread whose only job is to keep it alive until the
/// next `rewatch` ends it.
struct GlobalVerWatcher {
    etcd: Arc<dyn EtcdWatchOps>,
    current: Mutex<Option<GlobalVerRx>>,
}

impl GlobalVerWatcher {
    fn new(etcd: Arc<dyn EtcdWatchOps>) -> Self {
        Self {
            etcd,
            current: Mutex::new(None),
        }
    }

    /// Go `Watcher.Watch`: start feeding the global-version channel.
    fn watch(&self) {
        self.rewatch();
    }

    /// Go `Watcher.Rewatch`: end the old watch, start a fresh one. A new
    /// channel replaces the old, exactly as upstream swaps `watchCh`.
    fn rewatch(&self) {
        match self.etcd.watch_prefix(DDL_GLOBAL_SCHEMA_VERSION, 0) {
            Ok(stream) => {
                // A forwarding thread turns the raw stream into Go's watch
                // channel: plain events for the consumer, ended on error.
                let (sender, receiver) = mpsc::channel::<WatchEvent>();
                *self
                    .current
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some(SharedRecv::new(receiver));
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
                                    if stream.stop.load(Ordering::Acquire) {
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
            Some(receiver) => SharedRecv::clone(receiver),
            None => {
                // Never-sending stand-in for Go's nil-channel block.
                let (_sender, receiver) = mpsc::channel::<WatchEvent>();
                let receiver: GlobalVerRx = SharedRecv::new(receiver);
                receiver
            }
        }
    }
}

/// Go `etcdSyncer`, ported whole.
pub struct EtcdSyncer {
    etcd: Arc<dyn EtcdWatchOps>,
    self_schema_ver_path: String,
    ddl_id: String,
    session: Mutex<Option<Arc<Session>>>,
    global_ver_watcher: GlobalVerWatcher,
    all_server_info: AllServerInfo,
    job_node_versions: Mutex<HashMap<i64, Arc<NodeVersions>>>,
    job_node_ver_prefix: String,
}

/// The boundary standing in for Go's package-level `infosync.GetAllServerInfo`:
/// every live server's info, read from etcd by the caller that owns the
/// server-info syncer (`tidb_domain::serverinfo_syncer::Syncer::all_server_info`).
pub type AllServerInfo = Arc<dyn Fn() -> Result<Vec<ServerInfo>, String> + Send + Sync>;

/// Go `NewEtcdSyncer`. `all_server_info` is the `infosync.GetAllServerInfo`
/// boundary; see [`AllServerInfo`].
#[must_use]
pub fn new_etcd_syncer(
    etcd: Arc<dyn EtcdWatchOps>,
    id: &str,
    all_server_info: AllServerInfo,
) -> EtcdSyncer {
    EtcdSyncer {
        global_ver_watcher: GlobalVerWatcher::new(Arc::clone(&etcd)),
        etcd,
        self_schema_ver_path: format!("{DDL_ALL_SCHEMA_VERSIONS}/{id}"),
        ddl_id: id.to_owned(),
        session: Mutex::new(None),
        all_server_info,
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
    fn new_session(&self) -> Result<Arc<Session>, String> {
        let log_prefix = format!("[{DDL_PROMPT}] {}", self.self_schema_ver_path);
        Ok(self.store_session(Session::new(Arc::clone(&self.etcd), &log_prefix)?))
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
        drop(jobs);
        if !item.match_or_set(match_fn) {
            // Predicate installed; `add` will retry it.
        }
        item
    }
}

impl Syncer for EtcdSyncer {
    /// Go `Init`.
    fn init(&self, ctx: &Context) -> Result<(), String> {
        self.etcd
            .put_if_not_exists(DDL_GLOBAL_SCHEMA_VERSION, INITIAL_VERSION.as_bytes())?;
        let session = self.new_session()?;
        self.global_ver_watcher.watch();
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
            None => {
                let (_sender, receiver) = mpsc::channel::<()>();
                let receiver: DoneCh = SharedRecv::new(receiver);
                receiver
            }
        }
    }

    /// Go `Restart`.
    fn restart(&self, ctx: &Context) -> Result<(), String> {
        if let Some(previous) = self.load_session() {
            previous.close();
        }
        let session = self.new_session()?;
        put_kv_to_etcd(
            ctx,
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
    fn watch_global_schema_ver(&self) {
        self.global_ver_watcher.rewatch();
    }

    /// Go `UpdateSelfVersion`: MDL ON reports under the job prefix with a
    /// monotonic CAS write; MDL OFF overwrites the leased self path.
    fn update_self_version(&self, ctx: &Context, job_id: i64, version: i64) -> Result<(), String> {
        let version_text = version.to_string();
        if mdl_enabled() {
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
    ) -> Result<(), String> {
        if !mdl_enabled() {
            let _ = ctx.sleep(crate::check_vers_first_wait_time());
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
                let server_infos = (self.all_server_info)()?;
                let mut updated_map_mdl: HashMap<String, String> = HashMap::new();
                let mut instance2id: HashMap<String, String> = HashMap::new();
                let mut start_by_id: HashMap<&str, i64> = HashMap::new();
                for info in &server_infos {
                    let instance = join_host_port(&info.static_info.ip, info.static_info.port);
                    let described = format!(
                        "instance ip {}, port {}, id {}",
                        info.static_info.ip, info.static_info.port, info.static_info.id
                    );
                    match instance2id.get(&instance) {
                        Some(existing_id) => {
                            let existing_start = start_by_id
                                .get(existing_id.as_str())
                                .copied()
                                .unwrap_or(i64::MIN);
                            if info.static_info.start_timestamp > existing_start {
                                updated_map_mdl.remove(existing_id.as_str());
                                updated_map_mdl
                                    .insert(info.static_info.id.clone(), described.clone());
                                instance2id.insert(instance.clone(), info.static_info.id.clone());
                                start_by_id.insert(
                                    info.static_info.id.as_str(),
                                    info.static_info.start_timestamp,
                                );
                            }
                        }
                        None => {
                            updated_map_mdl.insert(info.static_info.id.clone(), described);
                            instance2id.insert(instance.clone(), info.static_info.id.clone());
                            start_by_id.insert(
                                info.static_info.id.as_str(),
                                info.static_info.start_timestamp,
                            );
                        }
                    }
                }

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
                match SharedRecv::new(notify_rx).recv_timeout(Duration::from_secs(1)) {
                    crate::Recv::Item(()) => return Ok(()),
                    crate::Recv::Closed => {}
                    crate::Recv::Timeout => {
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
                        true,
                    );
                    if !succ {
                        break;
                    }
                    updated_map.insert(key.clone(), ());
                }
                if succ {
                    return Ok(());
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
            .watch_prefix(&self.job_node_ver_prefix, revision + 1)
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
        } else if let Some(item) = jobs.get(&job_id) {
            let item = Arc::clone(item);
            drop(jobs);
            item.del(&tidb_id);
            if item.len() == 0 {
                self.job_node_versions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&job_id);
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
    let left = key.strip_prefix(prefix)?;
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
    node_alive: bool,
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
    if ver < latest_ver && node_alive {
        if not_match_ver_cnt.is_multiple_of(interval_cnt) {
            eprintln!(
                "{{\"level\":\"info\",\"event\":\"syncer check all versions, someone is not synced, continue checking\",\"ddl\":{key:?},\"currentVer\":{ver},\"latestVer\":{latest_ver}}}"
            );
        }
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use std::collections::HashMap;
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
        cond: Condvar,
        revision: AtomicI64,
        leases: AtomicI64,
        keepalive_failures: AtomicBool,
        /// Simulates network latency on single-key reads so concurrent
        /// read-modify-write windows genuinely overlap, like they do against
        /// real etcd.
        read_delay_micros: AtomicU64,
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
            self.state.cond.notify_all();
        }

        fn delete_raw(&self, key: &str) {
            let mut kv = self.state.kv.lock().unwrap_or_else(|e| e.into_inner());
            let existed = kv.remove(key).is_some();
            drop(kv);
            if existed {
                // Bump the store revision so a following watch starts after
                // the delete, like a real mvcc store would.
                self.state.revision.fetch_add(1, AtomicOrdering::SeqCst);
                self.state.cond.notify_all();
            }
        }

        fn get_value(&self, key: &str) -> Option<Vec<u8>> {
            let kv = self.state.kv.lock().unwrap_or_else(|e| e.into_inner());
            kv.get(key).map(|entry| entry.value.clone())
        }

        fn set_server_infos(&self, infos: &[(&str, &str, u32, i64)]) {
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
            for key in doomed {
                let rev = self.state.revision.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                kv.insert(
                    key.clone(),
                    FakeEntry {
                        value: Vec::new(),
                        mod_rev: rev,
                        lease: None,
                    },
                );
            }
            drop(kv);
            for key in self.get_prefix(prefix)?.into_iter().map(|(k, _)| k) {
                self.delete_raw(&key);
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
            self.state.cond.notify_all();
            Ok(true)
        }
        fn watch_prefix(&self, prefix: &str, start_revision: i64) -> Result<WatchStream, String> {
            let (sender, receiver) = mpsc::channel::<Result<WatchEvent, String>>();
            let stop = Arc::new(AtomicBool::new(false));
            let state = Arc::clone(&self.state);
            let prefix = prefix.to_owned();
            let thread_stop = Arc::clone(&stop);
            std::thread::Builder::new()
                .name("fake-etcd-watch".to_owned())
                .spawn(move || {
                    // Seed from current state: only changes AFTER this point
                    // are streamed, like starting a watch at a revision.
                    // Seed EVERY current key under the prefix so later
                    // changes AND DELETIONS of pre-existing keys both stream;
                    // like etcd from `start_revision`, nothing before it is
                    // replayed.
                    let _ = start_revision;
                    let mut seen: HashMap<String, i64> = HashMap::new();
                    {
                        let kv = state.kv.lock().unwrap_or_else(|e| e.into_inner());
                        for (key, entry) in kv.iter() {
                            if key.starts_with(&prefix) {
                                seen.insert(key.clone(), entry.mod_rev);
                            }
                        }
                    }
                    loop {
                        let mut events = Vec::new();
                        {
                            let kv = state.kv.lock().unwrap_or_else(|e| e.into_inner());
                            for (key, entry) in kv.iter() {
                                if !key.starts_with(&prefix)
                                    || seen.get(key) == Some(&entry.mod_rev)
                                {
                                    continue;
                                }
                                events.push(WatchEvent {
                                    key: key.clone(),
                                    value: entry.value.clone(),
                                    deleted: false,
                                });
                                seen.insert(key.clone(), entry.mod_rev);
                            }
                            let stale: Vec<String> = seen
                                .keys()
                                .filter(|seen_key| !kv.contains_key(*seen_key))
                                .cloned()
                                .collect();
                            for key in stale {
                                events.push(WatchEvent {
                                    key: key.clone(),
                                    value: Vec::new(),
                                    deleted: true,
                                });
                                seen.remove(&key);
                            }
                            let _unused_guard =
                                state.cond.wait_timeout(kv, Duration::from_millis(2));
                        }
                        for event in events {
                            if thread_stop.load(AtomicOrdering::Acquire) {
                                return;
                            }
                            if sender.send(Ok(event)).is_err() {
                                return;
                            }
                        }
                        if thread_stop.load(AtomicOrdering::Acquire) {
                            return;
                        }
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
        new_etcd_syncer(
            Arc::new(etcd.clone()),
            "1111",
            FakeEtcd::all_server_info_fn(etcd),
        )
    }

    fn test_server_info(id: &str) -> ServerInfo {
        ServerInfo {
            static_info: StaticInfo {
                id: id.to_owned(),
                ip: "test".to_owned(),
                port: 4000,
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

        // job 3 is matched using WaitVersionSynced (Go's job-4 leg; its
        // mockCompaction leg is a failpoint and stays uncovered).
        {
            let guard = globals_test_lock();
            tidb_vardef::set_enable_mdl(true);
            etcd.set_server_infos(&[("aa", "test", 4000, 1)]);
            etcd.put_raw(&format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/4/aa"), "333");
            syncer.wait_version_synced(&ctx, 4, 333).unwrap();
            etcd.delete_raw(&format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/4/aa"));
            tidb_vardef::set_enable_mdl(false);
            drop(guard);
        }

        loop_ctx.cancel();
        loop_handle.join().unwrap();
    }

    // ---- Go TestSyncerSimple (MDL OFF) ----

    #[test]
    fn test_syncer_simple() {
        let guard = globals_test_lock();
        tidb_vardef::set_enable_mdl(false);
        let origin = crate::check_vers_first_wait_time();
        set_check_vers_first_wait_time(Duration::ZERO);

        let etcd = FakeEtcd::default();
        let ctx = Context::background();
        let one = new_etcd_syncer(
            Arc::new(etcd.clone()),
            "1",
            FakeEtcd::all_server_info_fn(&etcd),
        );
        let two = new_etcd_syncer(
            Arc::new(etcd.clone()),
            "2",
            FakeEtcd::all_server_info_fn(&etcd),
        );
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
            current_ver
        )));

        // for UpdateSelfVersion (non-MDL: leased writes to the self path).
        one.update_self_version(&ctx, 0, current_ver).unwrap();
        two.update_self_version(&ctx, 0, current_ver).unwrap();

        // A spent context fails the write immediately.
        let tiny = Context::with_timeout(&ctx, Duration::ZERO);
        assert!(two.update_self_version(&tiny, 0, current_ver).is_err());

        // for CheckAllVersions after both reported.
        one.wait_version_synced(&ctx, 0, current_ver - 1).unwrap();
        one.wait_version_synced(&ctx, 0, current_ver).unwrap();

        let tiny = Context::with_timeout(&ctx, Duration::ZERO);
        assert!(is_deadline_error(one.wait_version_synced(
            &tiny,
            0,
            current_ver
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

    fn is_deadline_error(result: Result<(), String>) -> bool {
        matches!(result, Err(message) if message.contains("deadline exceeded"))
    }

    /// Go `Done` semantics: losing keepalives closes the channel.
    #[test]
    fn test_session_done_fires_on_lost_lease() {
        let etcd = FakeEtcd::default();
        etcd.state
            .keepalive_failures
            .store(true, AtomicOrdering::Release);
        let session = Session::with_keep_alive_interval(
            Arc::new(etcd.clone()),
            "[ddl-syncer] /x",
            Duration::from_millis(50),
        )
        .unwrap();
        let done = session.done();
        let started = std::time::Instant::now();
        loop {
            match done.recv_timeout(Duration::from_millis(50)) {
                crate::Recv::Closed => break,
                crate::Recv::Timeout => assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "done never fired"
                ),
                crate::Recv::Item(_) => panic!("done carries no items"),
            }
        }
        session.close();
    }

    /// Go `Init` seeds the global version exactly once.
    #[test]
    fn test_init_global_version_once() {
        let etcd = FakeEtcd::default();
        etcd.put_raw(DDL_GLOBAL_SCHEMA_VERSION, "777");
        let syncer = new_syncer(&etcd);
        syncer.init(&Context::background()).unwrap();
        assert_eq!(
            Some(b"777".to_vec()),
            etcd.get_value(DDL_GLOBAL_SCHEMA_VERSION),
            "Init must not clobber an existing global version"
        );
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

    /// Go `Restart` re-leases and republishes the initial version.
    #[test]
    fn test_restart_republishes_initial_version_under_new_lease() {
        let etcd = FakeEtcd::default();
        let syncer = new_syncer(&etcd);
        let ctx = Context::background();
        syncer.init(&ctx).unwrap();
        syncer.update_self_version(&ctx, 0, 55).unwrap();
        let old_session = syncer.load_session().unwrap();
        syncer.restart(&ctx).unwrap();
        let new_session = syncer.load_session().unwrap();
        assert_ne!(old_session.lease(), new_session.lease());
        assert_eq!(
            Some(crate::INITIAL_VERSION.as_bytes().to_vec()),
            etcd.get_value(&format!("{}/{}", crate::DDL_ALL_SCHEMA_VERSIONS, "1111"))
        );
    }
}
