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

//! Go `pkg/ddl/schemaver`, ported whole: the syncer that carries schema
//! versions between one DDL owner and every follower through etcd, plus the
//! in-memory stand-in used where there is exactly one node.
//!
//! # Why this exists for a Rust node in a shared cluster
//!
//! A Go DDL owner publishes a new global version to
//! [`DDL_GLOBAL_SCHEMA_VERSION`] and then blocks in `WaitVersionSynced`
//! until EVERY node reports it has loaded that version -- under
//! `EnableMDL=true` at `/tidb/ddl/all_schema_by_job_versions/<jobID>/<ddlID>`,
//! otherwise at `/tidb/ddl/all_schema_versions/<id>`. A Rust tidb-server that
//! never wrote those keys would hang every cluster DDL forever. This package
//! owns the two syncer implementations and their exact contracts; process
//! bootstrap remains the consuming server package's responsibility.
//!
//! # Integration boundary
//!
//! Go constructs a syncer from DDL/domain bootstrap, outside this package.
//! Rust keeps the same boundary: [`etcd_syncer::EtcdWatchOps`] is the native
//! transport seam and [`AllServerInfo`] is the native server-info seam. The
//! server's catalog-reload/MDL loop is a separate Go package translation and
//! must bind those seams; this crate does not introduce a second bootstrap or
//! a cache-specific execution path.
//!
//! # Mapping
//!
//! | Go file | Rust module |
//! | --- | --- |
//! | `syncer.go` | [`etcd_syncer`] |
//! | `mem_syncer.go` | [`mem_syncer`] |
//!
//! The key constants below are Go `pkg/ddl/util/util.go`'s, spelled
//! identically on the wire. [`SESSION_TTL_SECONDS`] is Go `util.SessionTTL`.
//!
//! # What rides here and what does not
//!
//! * Go `metrics.DeploySyncerHistogram` /
//!   `UpdateSelfVersionHistogram` / `OwnerHandleSyncerHistogram`: this tier
//!   has no metrics registry yet, so the observations are dropped. Control
//!   flow never reads them.
//! * Go failpoints (`mockCompaction`, `ErrorMockSessionDone`,
//!   `PutKVToEtcdError`, `mockUpdateMDLToETCDError`,
//!   `mockOwnerCheckAllVersionSlow`): test hooks only; where a test needs the
//!   effect, an explicit `#[cfg(test)]` helper reproduces it.
//! * `pkg/ddl/util/util.go`'s PUT helpers are ported into
//!   [`etcd_syncer`] as free functions over the [`etcd_syncer::EtcdWatchOps`]
//!   seam, because they are the exact retry/CAS loops the syncer's contract
//!   rests on.

pub mod etcd_syncer;
pub mod mem_syncer;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_domain::serverinfo::ServerInfo;

/// Go `util.DDLAllSchemaVersions`.
pub const DDL_ALL_SCHEMA_VERSIONS: &str = "/tidb/ddl/all_schema_versions";
/// Go `util.DDLAllSchemaVersionsByJob`.
pub const DDL_ALL_SCHEMA_VERSIONS_BY_JOB: &str = "/tidb/ddl/all_schema_by_job_versions";
/// Go `util.DDLGlobalSchemaVersion`.
pub const DDL_GLOBAL_SCHEMA_VERSION: &str = "/tidb/ddl/global_schema_version";
/// Go `util.SessionTTL`, seconds.
pub const SESSION_TTL_SECONDS: i64 = 90;
/// Go `InitialVersion`: the initial schema version for every server,
/// exported by upstream for testing.
pub const INITIAL_VERSION: &str = "0";

/// Go `util.KeyOpDefaultRetryCnt`.
pub(crate) const KEY_OP_DEFAULT_RETRY_CNT: u32 = 3;
/// Go `putKeyRetryUnlimited` (`math.MaxInt64`).
pub(crate) const PUT_KEY_RETRY_UNLIMITED: u32 = u32::MAX;
/// Go `util.KeyOpRetryInterval`.
pub(crate) const KEY_OP_RETRY_INTERVAL: Duration = Duration::from_millis(30);
/// Go `etcd.KeyOpDefaultTimeout`.
pub(crate) const KEY_OP_DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);
/// Go `util.NewSessionDefaultRetryCnt`.
pub(crate) const NEW_SESSION_DEFAULT_RETRY_CNT: u64 = 3;
/// Go `util.NewSessionRetryUnlimited` (`math.MaxInt64`).
pub(crate) const NEW_SESSION_RETRY_UNLIMITED: u64 = i64::MAX as u64;
/// Go `newSessionRetryInterval`.
pub(crate) const NEW_SESSION_RETRY_INTERVAL: Duration = Duration::from_millis(200);
/// Go `checkVersInterval`.
pub(crate) const CHECK_VERS_INTERVAL: Duration = Duration::from_millis(20);
/// Go `ddlPrompt`.
pub(crate) const DDL_PROMPT: &str = "ddl-syncer";

/// Go `CheckVersFirstWaitTime`: exported BY UPSTREAM for testing, so it is a
/// settable static here rather than a `const`.
static CHECK_VERS_FIRST_WAIT_TIME_NS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(50_000_000);

/// Reads Go `CheckVersFirstWaitTime` without losing sub-millisecond values.
#[must_use]
pub fn check_vers_first_wait_time() -> Duration {
    let nanos = std::sync::atomic::AtomicU64::load(
        &CHECK_VERS_FIRST_WAIT_TIME_NS,
        std::sync::atomic::Ordering::Relaxed,
    );
    Duration::from_nanos(nanos)
}

/// Overwrites Go `CheckVersFirstWaitTime`.
pub fn set_check_vers_first_wait_time(wait: Duration) {
    std::sync::atomic::AtomicU64::store(
        &CHECK_VERS_FIRST_WAIT_TIME_NS,
        u64::try_from(wait.as_nanos()).unwrap_or(u64::MAX),
        std::sync::atomic::Ordering::Relaxed,
    );
}

/// Go `SyncSummary`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SyncSummary {
    /// Total live server instances checked.
    pub server_count: usize,
    /// Assumed-keyspace server instances checked.
    pub assumed_server_count: usize,
}

impl std::fmt::Display for SyncSummary {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.assumed_server_count > 0 {
            write!(
                formatter,
                "server count: {}, assumed server count: {}",
                self.server_count, self.assumed_server_count
            )
        } else {
            write!(formatter, "server count: {}", self.server_count)
        }
    }
}

/// Native boundary for Go `*serverinfo.Syncer.GetAllServerInfo`.
pub type AllServerInfo = Arc<dyn Fn() -> Result<Vec<ServerInfo>, String> + Send + Sync>;

/// Why a context stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CtxError {
    /// Go `context.Canceled`.
    Canceled,
    /// Go `context.DeadlineExceeded`.
    DeadlineExceeded,
}

impl std::fmt::Display for CtxError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Canceled => formatter.write_str("context canceled"),
            Self::DeadlineExceeded => formatter.write_str("context deadline exceeded"),
        }
    }
}

/// Go `context.Context`, reduced to what the syncers select on: a cancel
/// flag, an optional deadline, and a parent. Errors propagate up the chain,
/// exactly like cancellation does upstream.
#[derive(Clone, Debug, Default)]
pub struct Context {
    inner: Arc<CtxInner>,
}

#[derive(Debug, Default)]
struct CtxInner {
    canceled: std::sync::atomic::AtomicBool,
    deadline: Mutex<Option<std::time::Instant>>,
    parent: Option<Box<Context>>,
}

impl Context {
    /// Go `context.Background()`.
    #[must_use]
    pub fn background() -> Self {
        Self::default()
    }

    /// Go `context.WithCancel`.
    #[must_use]
    pub fn with_cancel(parent: &Context) -> Self {
        Self {
            inner: Arc::new(CtxInner {
                canceled: std::sync::atomic::AtomicBool::new(false),
                deadline: Mutex::new(None),
                parent: Some(Box::new(parent.clone())),
            }),
        }
    }

    /// Go `context.WithTimeout`.
    #[must_use]
    pub fn with_timeout(parent: &Context, timeout: Duration) -> Self {
        let child = Self::with_cancel(parent);
        *child
            .inner
            .deadline
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(std::time::Instant::now() + timeout);
        child
    }

    /// Cancels this context and no other.
    pub fn cancel(&self) {
        self.inner
            .canceled
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Go `ctx.Err()`: why the context is done, if it is.
    ///
    /// # Errors
    /// The reason the context stopped, matching Go's sentinel errors.
    pub fn err(&self) -> Result<(), CtxError> {
        if self
            .inner
            .canceled
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(CtxError::Canceled);
        }
        if let Some(deadline) = *self
            .inner
            .deadline
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            if std::time::Instant::now() >= deadline {
                return Err(CtxError::DeadlineExceeded);
            }
        }
        match &self.inner.parent {
            Some(parent) => parent.err(),
            None => Ok(()),
        }
    }

    /// Go `<-ctx.Done()` folded into a sleep: waits out `d`, but wakes early
    /// -- in small steps -- once the context is done, answering the reason.
    ///
    /// # Errors
    /// [`CtxError`] when the context stops before `d` elapses.
    pub fn sleep(&self, d: Duration) -> Result<(), CtxError> {
        const STEP: Duration = Duration::from_millis(2);
        let mut slept = Duration::ZERO;
        while slept < d {
            self.err()?;
            let step = STEP.min(d - slept);
            std::thread::sleep(step);
            slept += step;
        }
        self.err()
    }
}

/// One etcd key change, the projection of `mvccpb.Event` both watchers need.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WatchEvent {
    /// The changed key.
    pub key: String,
    /// The value written; empty for a delete.
    pub value: Vec<u8>,
    /// Whether the key was deleted.
    pub deleted: bool,
}

/// A receive end shared by any number of holders, like one Go channel passed
/// to several goroutines. Cloning shares the SAME queue.
#[derive(Debug)]
pub struct SharedRecv<T>(Arc<Mutex<std::sync::mpsc::Receiver<T>>>);

impl<T> Clone for SharedRecv<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

/// What a [`SharedRecv::recv_timeout`] answered.
#[derive(Debug)]
pub enum Recv<T> {
    /// One item.
    Item(T),
    /// Every sender is gone; Go's closed-channel zero read.
    Closed,
    /// Nothing arrived within the budget.
    Timeout,
}

impl<T> SharedRecv<T> {
    /// Wraps one native receiver as a cloneable Go-channel equivalent.
    pub fn new(receiver: std::sync::mpsc::Receiver<T>) -> Self {
        Self(Arc::new(Mutex::new(receiver)))
    }

    /// `recv_timeout`, with Go's closed-channel case surfaced as [`Recv::Closed`].
    pub fn recv_timeout(&self, wait: Duration) -> Recv<T> {
        let receiver = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match receiver.recv_timeout(wait) {
            Ok(item) => Recv::Item(item),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Recv::Timeout,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Recv::Closed,
        }
    }
}

/// Go `clientv3.WatchChan` for the global schema version.
pub type GlobalVerRx = SharedRecv<WatchEvent>;

/// Go `Done() <-chan struct{}`.
pub type DoneCh = SharedRecv<()>;

/// The shape both syncers implement, method for method Go `schemaver.Syncer`.
///
/// Methods take `&Context` where Go takes `context.Context`; blocking loops
/// (`wait_version_synced`, `sync_job_schema_ver_loop`) run on the CALLER's
/// thread and return when the context does.
pub trait Syncer: Send + Sync {
    /// Go `Init`: seed the global version key if absent, take a session,
    /// watch the global key, publish this node's initial version.
    ///
    /// # Errors
    /// Whatever etcd answers; startup has nothing to fall back to.
    fn init(&self, ctx: &Context) -> Result<(), String>;

    /// Go `UpdateSelfVersion`: report THIS node's loaded version for one DDL
    /// job (MDL path) or on the self path (non-MDL path).
    ///
    /// # Errors
    /// Etcd failures after the retries the implementation owes.
    fn update_self_version(&self, ctx: &Context, job_id: i64, version: i64) -> Result<(), String>;

    /// Go `OwnerUpdateGlobalVersion`: publish the new global version until
    /// it sticks or the context ends.
    ///
    /// # Errors
    /// Etcd failures after unlimited retries, or the context stopping first.
    fn owner_update_global_version(&self, ctx: &Context, version: i64) -> Result<(), String>;

    /// Go `GlobalVersionCh`: the channel global-version events arrive on.
    fn global_version_ch(&self) -> GlobalVerRx;

    /// Go `WatchGlobalSchemaVer`: (re)establish the watch feeding
    /// [`Syncer::global_version_ch`].
    fn watch_global_schema_ver(&self, ctx: &Context);

    /// Go `Done`: fires once when the session is lost and needs a restart.
    fn done(&self) -> DoneCh;

    /// Go `Restart`: rebuild the session and republish the initial version.
    ///
    /// # Errors
    /// Etcd failures while rebuilding.
    fn restart(&self, ctx: &Context) -> Result<(), String>;

    /// Go `WaitVersionSynced`: block until every server reports at least
    /// `latest_ver` for `job_id`, or the context ends.
    ///
    /// # Errors
    /// The context stopping, or an unrecoverable read failure.
    fn wait_version_synced(
        &self,
        ctx: &Context,
        job_id: i64,
        latest_ver: i64,
        check_assumed_server: bool,
    ) -> Result<SyncSummary, String>;

    /// Go `SyncJobSchemaVerLoop`: keep this node's per-job version entries
    /// mirrored from etcd so the OWNER can wait on them. Runs until `ctx`.
    fn sync_job_schema_ver_loop(&self, ctx: &Context);

    /// Go `SetServerInfoSyncer` at the native server-info read boundary.
    fn set_server_info_syncer(&self, all_server_info: AllServerInfo);

    /// Go `Close`: leave, removing this node's own version key.
    fn close(&self);
}

/// Go `vardef.IsMDLEnabled()`, including next-generation TiDB's always-on
/// MDL rule.
pub(crate) fn mdl_enabled() -> bool {
    tidb_vardef::is_mdl_enabled(tidb_config::kerneltype::is_next_gen())
}

#[cfg(test)]
/// Serializes tests that flip the process-global EnableMDL flag or
/// `CHECK_VERS_FIRST_WAIT_TIME_NS`, which Go tests also share per process.
#[doc(hidden)]
pub(crate) fn globals_test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
