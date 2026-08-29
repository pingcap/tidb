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

//! Go `pkg/owner`: etcd-backed owner election and its local-store stand-in.

mod mock;

pub use mock::{MockGlobalState, MockGlobalStateSelector, MockManager, MOCK_GLOBAL_STATE_ENTRY};

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tidb_pd_client::{EtcdClient, EtcdKeyValue, EtcdWatcher};

const KEY_OP_DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
const KEY_OP_RETRY_INTERVAL: Duration = Duration::from_millis(30);
const CAMPAIGN_POLL_INTERVAL: Duration = Duration::from_millis(20);
const NEW_SESSION_RETRY_COUNT: usize = 3;

/// Go `WaitTimeOnForceOwner`.
pub static WAIT_TIME_ON_FORCE_OWNER_MILLIS: AtomicI64 = AtomicI64::new(5_000);
/// Go `ManagerSessionTTL`.
pub static MANAGER_SESSION_TTL: AtomicI64 = AtomicI64::new(60);

/// Owner-key operation value.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OpType(u8);

impl OpType {
    /// Go `OpNone`.
    pub const NONE: Self = Self(0);
    /// Go `OpSyncUpgradingState`.
    pub const SYNC_UPGRADING_STATE: Self = Self(1);

    /// Constructs an operation from the byte stored in etcd.
    #[must_use]
    pub const fn from_byte(value: u8) -> Self {
        Self(value)
    }

    /// Returns the byte stored in etcd.
    #[must_use]
    pub const fn as_byte(self) -> u8 {
        self.0
    }

    /// Whether the upgrading state has been synchronized.
    #[must_use]
    pub const fn is_synced_upgrading_state(self) -> bool {
        self.0 == Self::SYNC_UPGRADING_STATE.0
    }
}

impl std::fmt::Display for OpType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(if self.is_synced_upgrading_state() {
            "sync upgrading state"
        } else {
            "none"
        })
    }
}

/// Cancellation and deadline state corresponding to Go `context.Context`.
#[derive(Clone, Debug, Default)]
pub struct Context {
    canceled: Arc<AtomicBool>,
    deadline: Option<Instant>,
}

impl Context {
    /// A context without cancellation or deadline.
    #[must_use]
    pub fn background() -> Self {
        Self::default()
    }

    /// A cancelable child context.
    #[must_use]
    pub fn cancelable() -> Self {
        Self::default()
    }

    /// A context with a deadline relative to now.
    #[must_use]
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            canceled: Arc::new(AtomicBool::new(false)),
            deadline: Some(Instant::now() + timeout),
        }
    }

    /// Cancels this context.
    pub fn cancel(&self) {
        self.canceled.store(true, Ordering::Release);
    }

    fn is_done(&self) -> bool {
        self.canceled.load(Ordering::Acquire)
            || self
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
    }
}

/// Owner lifecycle listener.
pub trait Listener: Send + Sync + 'static {
    /// Called after this manager becomes owner.
    fn on_become_owner(&self);
    /// Called after this manager retires.
    fn on_retire_owner(&self);
}

/// Broadcasts owner lifecycle events to multiple listeners.
pub struct ListenersWrapper {
    listeners: Vec<Arc<dyn Listener>>,
}

impl ListenersWrapper {
    /// Creates a listener broadcaster.
    #[must_use]
    pub fn new(listeners: Vec<Arc<dyn Listener>>) -> Self {
        Self { listeners }
    }
}

impl Listener for ListenersWrapper {
    fn on_become_owner(&self) {
        for listener in &self.listeners {
            listener.on_become_owner();
        }
    }

    fn on_retire_owner(&self) {
        for listener in &self.listeners {
            listener.on_retire_owner();
        }
    }
}

/// Storage operations required by Go's etcd concurrency election recipe.
pub trait OwnerStore: Send + Sync + 'static {
    /// Grants a lease.
    fn lease_grant(&self, ttl: i64) -> Result<i64, String>;
    /// Refreshes a lease once.
    fn lease_keep_alive_once(&self, lease: i64) -> Result<(), String>;
    /// Revokes a lease.
    fn lease_revoke(&self, lease: i64) -> Result<(), String>;
    /// Creates a leased key if absent.
    fn create_with_lease(&self, key: &[u8], value: &[u8], lease: i64) -> Result<bool, String>;
    /// Reads a creation-revision ordered prefix.
    fn get_prefix_metadata(&self, prefix: &[u8]) -> Result<Vec<EtcdKeyValue>, String>;
    /// Deletes one key.
    fn delete(&self, key: &[u8]) -> Result<(), String>;
    /// Compare-and-swaps a leased key.
    fn compare_and_put_with_lease(
        &self,
        key: &[u8],
        expected_mod_revision: i64,
        value: &[u8],
        lease: i64,
    ) -> Result<bool, String>;
    /// Atomically retires candidates and installs one campaign key.
    fn delete_keys_and_put_with_lease(
        &self,
        delete_keys: Vec<Vec<u8>>,
        key: &[u8],
        value: &[u8],
        lease: i64,
    ) -> Result<(), String>;
    /// Watches one key from an MVCC revision.
    fn watch(&self, key: &[u8], start_revision: i64) -> Result<Box<dyn OwnerWatch>, String>;
}

/// One active owner-key watch.
pub trait OwnerWatch: Send {
    /// Waits up to `timeout`; returns true when the key was deleted.
    fn wait_deleted(&mut self, timeout: Duration) -> Result<bool, String>;
}

struct EtcdOwnerWatch {
    events: std::sync::mpsc::Receiver<bool>,
    _watcher: EtcdWatcher,
}

impl OwnerWatch for EtcdOwnerWatch {
    fn wait_deleted(&mut self, timeout: Duration) -> Result<bool, String> {
        match self.events.recv_timeout(timeout) {
            Ok(deleted) => Ok(deleted),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Ok(false),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                Err("watcher is closed".to_owned())
            }
        }
    }
}

impl OwnerStore for EtcdClient {
    fn lease_grant(&self, ttl: i64) -> Result<i64, String> {
        EtcdClient::lease_grant(self, ttl)
            .map(|(lease, _)| lease)
            .map_err(|error| error.to_string())
    }

    fn lease_keep_alive_once(&self, lease: i64) -> Result<(), String> {
        EtcdClient::lease_keep_alive_once(self, lease)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn lease_revoke(&self, lease: i64) -> Result<(), String> {
        EtcdClient::lease_revoke(self, lease).map_err(|error| error.to_string())
    }

    fn create_with_lease(&self, key: &[u8], value: &[u8], lease: i64) -> Result<bool, String> {
        EtcdClient::create_with_lease(self, key, value, lease).map_err(|error| error.to_string())
    }

    fn get_prefix_metadata(&self, prefix: &[u8]) -> Result<Vec<EtcdKeyValue>, String> {
        EtcdClient::get_prefix_metadata(self, prefix).map_err(|error| error.to_string())
    }

    fn delete(&self, key: &[u8]) -> Result<(), String> {
        EtcdClient::delete(self, key).map_err(|error| error.to_string())
    }

    fn compare_and_put_with_lease(
        &self,
        key: &[u8],
        expected_mod_revision: i64,
        value: &[u8],
        lease: i64,
    ) -> Result<bool, String> {
        EtcdClient::compare_and_put_with_lease(self, key, expected_mod_revision, value, lease)
            .map_err(|error| error.to_string())
    }

    fn delete_keys_and_put_with_lease(
        &self,
        delete_keys: Vec<Vec<u8>>,
        key: &[u8],
        value: &[u8],
        lease: i64,
    ) -> Result<(), String> {
        EtcdClient::delete_keys_and_put_with_lease(self, delete_keys, key, value, lease)
            .map_err(|error| error.to_string())
    }

    fn watch(&self, key: &[u8], start_revision: i64) -> Result<Box<dyn OwnerWatch>, String> {
        let (sender, events) = std::sync::mpsc::channel();
        let watcher = self
            .watch_key(key.to_vec(), start_revision, move |event| {
                if event.deleted {
                    let _ = sender.send(true);
                }
            })
            .map_err(|error| error.to_string())?;
        Ok(Box::new(EtcdOwnerWatch {
            events,
            _watcher: watcher,
        }))
    }
}

/// Owner manager interface.
pub trait Manager: Send + Sync {
    /// Manager ID.
    fn id(&self) -> &str;
    /// Whether this manager is owner.
    fn is_owner(&self) -> bool;
    /// Retires this manager.
    fn retire_owner(&self);
    /// Reads the current owner ID.
    fn get_owner_id(&self, context: &Context) -> Result<String, String>;
    /// Updates the current owner's operation value.
    fn set_owner_op_value(&self, context: &Context, op: OpType) -> Result<(), String>;
    /// Starts the campaign loop. A one-element slice overrides the TTL.
    fn campaign_owner(&self, with_ttl: &[i64]) -> Result<(), String>;
    /// Cancels the campaign and closes its session.
    fn campaign_cancel(&self);
    /// Stops the campaign loop while retaining the session.
    fn break_campaign_loop(&self);
    /// Resigns and allows the campaign loop to elect again.
    fn resign_owner(&self, context: &Context) -> Result<(), String>;
    /// Closes the manager.
    fn close(&self);
    /// Sets the listener before campaigning.
    fn set_listener(&self, listener: Arc<dyn Listener>);
    /// Atomically retires older candidates and campaigns this manager.
    fn force_to_be_owner(&self, context: &Context) -> Result<(), String>;
}

/// The owner-checking subset used by DDL consumers.
pub trait DdlOwnerChecker: Send + Sync {
    /// Whether this instance is owner.
    fn is_owner(&self) -> bool;
}

impl<T: Manager + ?Sized> DdlOwnerChecker for T {
    fn is_owner(&self) -> bool {
        Manager::is_owner(self)
    }
}

struct Session {
    lease: i64,
    stop: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    keeper: Option<JoinHandle<()>>,
}

struct Campaign {
    stop: Arc<AtomicBool>,
    worker: JoinHandle<()>,
}

struct OwnerInner {
    id: String,
    key: String,
    context: Context,
    store: Arc<dyn OwnerStore>,
    owner: AtomicBool,
    closed: AtomicBool,
    session: Mutex<Option<Session>>,
    campaign: Mutex<Option<Campaign>>,
    listener: Mutex<Option<Arc<dyn Listener>>>,
}

/// Real etcd-backed owner manager.
#[derive(Clone)]
pub struct OwnerManager {
    inner: Arc<OwnerInner>,
}

impl OwnerManager {
    /// Creates an owner manager over the shared etcd authority.
    #[must_use]
    pub fn new(
        context: Context,
        store: Arc<dyn OwnerStore>,
        _prompt: impl Into<String>,
        id: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self {
            inner: Arc::new(OwnerInner {
                id: id.into(),
                key: key.into(),
                context,
                store,
                owner: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                session: Mutex::new(None),
                campaign: Mutex::new(None),
                listener: Mutex::new(None),
            }),
        }
    }

    fn refresh_session(&self, ttl: i64) -> Result<i64, String> {
        close_session(&self.inner);
        let mut last_error = String::new();
        for attempt in 0..NEW_SESSION_RETRY_COUNT {
            match self.inner.store.lease_grant(ttl) {
                Ok(lease) => {
                    let stop = Arc::new(AtomicBool::new(false));
                    let failed = Arc::new(AtomicBool::new(false));
                    let keeper_stop = Arc::clone(&stop);
                    let keeper_failed = Arc::clone(&failed);
                    let store = Arc::clone(&self.inner.store);
                    let cadence = Duration::from_secs((ttl.max(1) as u64).div_ceil(3));
                    let keeper = std::thread::Builder::new()
                        .name("owner-lease".to_owned())
                        .spawn(move || {
                            while !keeper_stop.load(Ordering::Acquire) {
                                if sleep_until_stopped(&keeper_stop, cadence) {
                                    return;
                                }
                                if store.lease_keep_alive_once(lease).is_err() {
                                    keeper_failed.store(true, Ordering::Release);
                                    return;
                                }
                            }
                        })
                        .map_err(|error| error.to_string())?;
                    let session = Session {
                        lease,
                        stop,
                        failed,
                        keeper: Some(keeper),
                    };
                    *lock(&self.inner.session) = Some(session);
                    return Ok(lease);
                }
                Err(error) => {
                    last_error = error;
                    if attempt + 1 < NEW_SESSION_RETRY_COUNT {
                        std::thread::sleep(KEY_OP_RETRY_INTERVAL);
                    }
                }
            }
        }
        Err(last_error)
    }

    fn current_lease(&self) -> Option<(i64, bool)> {
        lock(&self.inner.session)
            .as_ref()
            .map(|session| (session.lease, session.failed.load(Ordering::Acquire)))
    }

    fn become_owner(inner: &OwnerInner) {
        inner.owner.store(true, Ordering::Release);
        if let Some(listener) = lock(&inner.listener).as_ref() {
            listener.on_become_owner();
        }
    }

    fn retire(inner: &OwnerInner) {
        inner.owner.store(false, Ordering::Release);
        if let Some(listener) = lock(&inner.listener).as_ref() {
            listener.on_retire_owner();
        }
    }
}

impl Manager for OwnerManager {
    fn id(&self) -> &str {
        &self.inner.id
    }

    fn is_owner(&self) -> bool {
        self.inner.owner.load(Ordering::Acquire)
    }

    fn retire_owner(&self) {
        Self::retire(&self.inner);
    }

    fn get_owner_id(&self, context: &Context) -> Result<String, String> {
        let owner = get_owner_info(context, self.inner.store.as_ref(), &self.inner.key)?;
        String::from_utf8(split_owner_values(&owner.value).0.to_vec())
            .map_err(|error| error.to_string())
    }

    fn set_owner_op_value(&self, context: &Context, op: OpType) -> Result<(), String> {
        let owner = get_owner_info(context, self.inner.store.as_ref(), &self.inner.key)?;
        let (owner_id, current_op) = split_owner_values(&owner.value);
        if current_op == op {
            return Ok(());
        }
        if owner_id != self.inner.id.as_bytes() {
            return Err("ownerInfoNotMatch".to_owned());
        }
        let Some((lease, _)) = self.current_lease() else {
            return Err("owner session is not initialized".to_owned());
        };
        let value = join_owner_values(owner_id, op);
        if self.inner.store.compare_and_put_with_lease(
            &owner.key,
            owner.mod_revision,
            &value,
            lease,
        )? {
            Ok(())
        } else {
            Err("put owner key failed, cmp is false".to_owned())
        }
    }

    fn campaign_owner(&self, with_ttl: &[i64]) -> Result<(), String> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err("owner manager is closed".to_owned());
        }
        if lock(&self.inner.campaign).is_some() {
            return Ok(());
        }
        let ttl = if with_ttl.len() == 1 {
            with_ttl[0]
        } else {
            manager_session_ttl()
        };
        if self.current_lease().is_none() {
            self.refresh_session(ttl)?;
        }
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let manager = self.clone();
        let worker = std::thread::Builder::new()
            .name("owner-campaign".to_owned())
            .spawn(move || campaign_loop(&manager, ttl, &worker_stop))
            .map_err(|error| error.to_string())?;
        *lock(&self.inner.campaign) = Some(Campaign { stop, worker });
        Ok(())
    }

    fn campaign_cancel(&self) {
        self.break_campaign_loop();
        close_session(&self.inner);
    }

    fn break_campaign_loop(&self) {
        let campaign = lock(&self.inner.campaign).take();
        if let Some(campaign) = campaign {
            campaign.stop.store(true, Ordering::Release);
            let _ = campaign.worker.join();
        }
    }

    fn resign_owner(&self, context: &Context) -> Result<(), String> {
        if !Manager::is_owner(self) {
            return Err("This node is not a owner, can't be resigned".to_owned());
        }
        if context.is_done() {
            return Err("context canceled".to_owned());
        }
        let Some((lease, _)) = self.current_lease() else {
            return Err("owner session is not initialized".to_owned());
        };
        self.inner
            .store
            .delete(campaign_key(&self.inner.key, lease).as_bytes())
    }

    fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.campaign_cancel();
    }

    fn set_listener(&self, listener: Arc<dyn Listener>) {
        *lock(&self.inner.listener) = Some(listener);
    }

    fn force_to_be_owner(&self, _context: &Context) -> Result<(), String> {
        let lease = self.refresh_session(manager_session_ttl())?;
        let key = campaign_key(&self.inner.key, lease);
        for _ in 0..3 {
            if sleep_context(
                &self.inner.context,
                Duration::from_millis(
                    WAIT_TIME_ON_FORCE_OWNER_MILLIS.load(Ordering::Acquire) as u64
                ),
            )
            .is_err()
            {
                return Err("context canceled".to_owned());
            }
            let prefix = format!("{}/", self.inner.key);
            let candidates = self.inner.store.get_prefix_metadata(prefix.as_bytes())?;
            let delete_keys = candidates
                .into_iter()
                .filter_map(|candidate| (candidate.key != key.as_bytes()).then_some(candidate.key))
                .collect();
            if self
                .inner
                .store
                .delete_keys_and_put_with_lease(
                    delete_keys,
                    key.as_bytes(),
                    self.inner.id.as_bytes(),
                    lease,
                )
                .is_ok()
                && wait_until_first_with_timeout(
                    &self.inner.context,
                    self.inner.store.as_ref(),
                    &self.inner.key,
                    &key,
                    KEY_OP_DEFAULT_TIMEOUT,
                )
                .is_ok()
            {
                return Ok(());
            }
        }
        Ok(())
    }
}

impl Drop for OwnerManager {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            self.close();
        }
    }
}

fn campaign_loop(manager: &OwnerManager, ttl: i64, stop: &AtomicBool) {
    while !stop.load(Ordering::Acquire) && !manager.inner.context.is_done() {
        let lease = match manager.current_lease() {
            Some((lease, false)) => lease,
            _ => loop {
                match manager.refresh_session(ttl) {
                    Ok(lease) => break lease,
                    Err(_) if stop.load(Ordering::Acquire) || manager.inner.context.is_done() => {
                        return;
                    }
                    Err(_) => std::thread::sleep(KEY_OP_RETRY_INTERVAL),
                }
            },
        };
        let key = campaign_key(&manager.inner.key, lease);
        if manager
            .inner
            .store
            .create_with_lease(key.as_bytes(), manager.inner.id.as_bytes(), lease)
            .is_err()
        {
            std::thread::sleep(CAMPAIGN_POLL_INTERVAL);
            continue;
        }
        if wait_until_first_with_stop(
            &manager.inner.context,
            manager.inner.store.as_ref(),
            &manager.inner.key,
            &key,
            stop,
            None,
        )
        .is_err()
        {
            continue;
        }
        let owner = match get_owner_info(
            &manager.inner.context,
            manager.inner.store.as_ref(),
            &manager.inner.key,
        ) {
            Ok(owner) => owner,
            Err(_) => continue,
        };
        let (owner_id, _) = split_owner_values(&owner.value);
        if owner.key != key.as_bytes() || owner_id != manager.inner.id.as_bytes() {
            continue;
        }
        OwnerManager::become_owner(&manager.inner);
        let mut watcher = match manager
            .inner
            .store
            .watch(&owner.key, owner.mod_revision + 1)
        {
            Ok(watcher) => watcher,
            Err(_) => {
                OwnerManager::retire(&manager.inner);
                continue;
            }
        };
        while !stop.load(Ordering::Acquire) && !manager.inner.context.is_done() {
            let session_failed = manager.current_lease().is_none_or(|(_, failed)| failed);
            if session_failed || watcher.wait_deleted(CAMPAIGN_POLL_INTERVAL).unwrap_or(true) {
                break;
            }
        }
        OwnerManager::retire(&manager.inner);
    }
}

fn close_session(inner: &OwnerInner) {
    let session = lock(&inner.session).take();
    if let Some(mut session) = session {
        session.stop.store(true, Ordering::Release);
        let _ = inner.store.lease_revoke(session.lease);
        if let Some(keeper) = session.keeper.take() {
            let _ = keeper.join();
        }
    }
}

fn campaign_key(owner_path: &str, lease: i64) -> String {
    format!("{owner_path}/{lease:x}")
}

fn manager_session_ttl() -> i64 {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        if let Ok(value) = std::env::var("tidb_manager_ttl") {
            if let Ok(ttl) = value.parse::<i64>() {
                MANAGER_SESSION_TTL.store(ttl, Ordering::Release);
            }
        }
    });
    MANAGER_SESSION_TTL.load(Ordering::Acquire)
}

fn get_owner_info(
    context: &Context,
    store: &dyn OwnerStore,
    owner_path: &str,
) -> Result<EtcdKeyValue, String> {
    let mut last_error = String::new();
    for attempt in 0..3 {
        if context.is_done() {
            return Err("context canceled".to_owned());
        }
        match store.get_prefix_metadata(owner_path.as_bytes()) {
            Ok(entries) => {
                return entries
                    .into_iter()
                    .next()
                    .ok_or_else(|| "election: no leader".to_owned());
            }
            Err(error) => last_error = error,
        }
        if attempt < 2 {
            std::thread::sleep(KEY_OP_RETRY_INTERVAL);
        }
    }
    Err(last_error)
}

/// Reads the current owner operation value.
pub fn get_owner_op_value(
    context: &Context,
    store: Option<&dyn OwnerStore>,
    owner_path: &str,
) -> Result<OpType, String> {
    let Some(store) = store else {
        return Ok(mock::mock_owner_op_value());
    };
    let owner = get_owner_info(context, store, owner_path)?;
    Ok(split_owner_values(&owner.value).1)
}

/// Gets the current owner key and validates that its owner ID equals `id`.
pub fn get_owner_key_info(
    context: &Context,
    store: &dyn OwnerStore,
    owner_path: &str,
    id: &str,
) -> Result<(String, i64), String> {
    let owner = get_owner_info(context, store, owner_path)?;
    if split_owner_values(&owner.value).0 != id.as_bytes() {
        return Err("ownerInfoNotMatch".to_owned());
    }
    let key = String::from_utf8(owner.key).map_err(|error| error.to_string())?;
    Ok((key, owner.mod_revision))
}

/// Deletes the campaign key whose owner ID matches `id`.
pub fn delete_owner_key_by_id(
    context: &Context,
    store: &dyn OwnerStore,
    owner_path: &str,
    id: &str,
) {
    if context.is_done() {
        return;
    }
    let prefix = format!("{owner_path}/");
    let Ok(entries) = store.get_prefix_metadata(prefix.as_bytes()) else {
        return;
    };
    if let Some(entry) = entries
        .into_iter()
        .find(|entry| split_owner_values(&entry.value).0 == id.as_bytes())
    {
        let _ = store.delete(&entry.key);
    }
}

/// Watches one owner key from the supplied revision until it is deleted.
///
/// This is Go `WatchOwnerForTest`; the revision is retained in the API even
/// though the synchronous store seam observes the current MVCC state.
pub fn watch_owner_for_test(
    context: &Context,
    store: &dyn OwnerStore,
    key: &str,
    create_revision: i64,
) -> Result<(), String> {
    let mut watcher = store.watch(key.as_bytes(), create_revision + 1)?;
    loop {
        if context.is_done() {
            return Ok(());
        }
        if watcher.wait_deleted(CAMPAIGN_POLL_INTERVAL)? {
            return Ok(());
        }
    }
}

fn split_owner_values(value: &[u8]) -> (&[u8], OpType) {
    let mut parts = value.split(|byte| *byte == b'_');
    let owner_id = parts.next().unwrap_or_default();
    let Some(op_bytes) = parts.next() else {
        return (owner_id, OpType::NONE);
    };
    if parts.next().is_some() {
        return (owner_id, OpType::NONE);
    }
    (
        owner_id,
        OpType::from_byte(op_bytes.first().copied().unwrap_or(0)),
    )
}

fn join_owner_values(owner_id: &[u8], op: OpType) -> Vec<u8> {
    let mut value = Vec::with_capacity(owner_id.len() + 2);
    value.extend_from_slice(owner_id);
    value.push(b'_');
    value.push(op.as_byte());
    value
}

fn wait_until_first(
    context: &Context,
    store: &dyn OwnerStore,
    owner_path: &str,
    campaign_key: &str,
) -> Result<(), String> {
    wait_until_first_with_stop(
        context,
        store,
        owner_path,
        campaign_key,
        &AtomicBool::new(false),
        None,
    )
}

fn wait_until_first_with_timeout(
    context: &Context,
    store: &dyn OwnerStore,
    owner_path: &str,
    campaign_key: &str,
    timeout: Duration,
) -> Result<(), String> {
    wait_until_first_with_stop(
        context,
        store,
        owner_path,
        campaign_key,
        &AtomicBool::new(false),
        Some(Instant::now() + timeout),
    )
}

fn wait_until_first_with_stop(
    context: &Context,
    store: &dyn OwnerStore,
    owner_path: &str,
    campaign_key: &str,
    stop: &AtomicBool,
    deadline: Option<Instant>,
) -> Result<(), String> {
    loop {
        if context.is_done() || stop.load(Ordering::Acquire) {
            return Err("context canceled".to_owned());
        }
        let entries = store.get_prefix_metadata(owner_path.as_bytes())?;
        if entries
            .first()
            .is_some_and(|entry| entry.key == campaign_key.as_bytes())
        {
            return Ok(());
        }
        if !entries
            .iter()
            .any(|entry| entry.key == campaign_key.as_bytes())
        {
            return Err("campaign key disappeared".to_owned());
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err("context deadline exceeded".to_owned());
        }
        std::thread::sleep(CAMPAIGN_POLL_INTERVAL);
    }
}

fn sleep_context(context: &Context, duration: Duration) -> Result<(), ()> {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if context.is_done() {
            return Err(());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Acquires an etcd distributed lock and returns its release closure.
pub fn acquire_distributed_lock(
    context: &Context,
    store: Arc<dyn OwnerStore>,
    key: &str,
    ttl_seconds: i64,
) -> Result<Box<dyn FnOnce() + Send>, String> {
    let lease = store.lease_grant(ttl_seconds)?;
    let lock_key = campaign_key(key, lease);
    store.create_with_lease(lock_key.as_bytes(), b"", lease)?;
    if let Err(error) = wait_until_first(context, store.as_ref(), key, &lock_key) {
        let _ = store.lease_revoke(lease);
        return Err(error);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let keeper_stop = Arc::clone(&stop);
    let keeper_store = Arc::clone(&store);
    let cadence = Duration::from_secs((ttl_seconds.max(1) as u64).div_ceil(3));
    let keeper = std::thread::spawn(move || {
        while !keeper_stop.load(Ordering::Acquire) {
            if sleep_until_stopped(&keeper_stop, cadence)
                || keeper_store.lease_keep_alive_once(lease).is_err()
            {
                return;
            }
        }
    });
    Ok(Box::new(move || {
        stop.store(true, Ordering::Release);
        let _ = store.delete(lock_key.as_bytes());
        let _ = store.lease_revoke(lease);
        let _ = keeper.join();
    }))
}

fn sleep_until_stopped(stop: &AtomicBool, duration: Duration) -> bool {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if stop.load(Ordering::Acquire) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    stop.load(Ordering::Acquire)
}
