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

//! Failed-TiFlash probing and MPP server information from
//! `pkg/store/copr/mpp_probe.go`.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Go `DetectPeriod`.
pub const DETECT_PERIOD: Duration = Duration::from_secs(3);
/// Go `DetectTimeoutLimit`.
pub const DETECT_TIMEOUT_LIMIT: Duration = Duration::from_secs(2);
/// Go `MaxRecoveryTimeLimit`.
pub const MAX_RECOVERY_TIME_LIMIT: Duration = Duration::from_secs(15 * 60);
/// Go `MaxObsoletTimeLimit`.
pub const MAX_OBSOLETE_TIME_LIMIT: Duration = Duration::from_secs(60 * 60);
/// Go `mppServerInfoManagerCacheSize`.
pub const MPP_SERVER_INFO_CACHE_SIZE: usize = 1000;

/// Object-safe boundary for TiFlash `CmdMPPAlive`.
pub trait MppAliveClient: Send + Sync + 'static {
    /// Returns the response's `Available` flag or a transport error string.
    fn is_alive(&self, address: &str, timeout: Duration) -> Result<bool, String>;
}

#[derive(Debug)]
struct StoreTimes {
    recovery_time: Option<Instant>,
    last_lookup_time: Instant,
    last_detect_time: Option<Instant>,
}

struct MppStoreState {
    address: String,
    client: Arc<dyn MppAliveClient>,
    times: Mutex<StoreTimes>,
}

struct ProbeInner {
    stores: Mutex<HashMap<String, Arc<MppStoreState>>>,
    detect_period: Duration,
    detect_timeout: Duration,
    max_recovery: Duration,
    max_obsolete: Duration,
    stopped: AtomicBool,
    wake: Condvar,
    wake_lock: Mutex<()>,
    worker: Mutex<Option<JoinHandle<()>>>,
    probes: Mutex<ProbeTasks>,
    owners: AtomicUsize,
}

#[derive(Default)]
struct ProbeTasks {
    closed: bool,
    handles: Vec<JoinHandle<()>>,
}

/// Go `MPPFailedStoreProber`.
pub struct MppFailedStoreProber {
    inner: Arc<ProbeInner>,
}

impl Default for MppFailedStoreProber {
    fn default() -> Self {
        Self::new(
            DETECT_PERIOD,
            DETECT_TIMEOUT_LIMIT,
            MAX_RECOVERY_TIME_LIMIT,
            MAX_OBSOLETE_TIME_LIMIT,
        )
    }
}

impl MppFailedStoreProber {
    /// Builds a prober with explicit periods; production uses [`Default`].
    #[must_use]
    pub fn new(
        detect_period: Duration,
        detect_timeout: Duration,
        max_recovery: Duration,
        max_obsolete: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(ProbeInner {
                stores: Mutex::default(),
                detect_period,
                detect_timeout,
                max_recovery,
                max_obsolete,
                stopped: AtomicBool::new(true),
                wake: Condvar::new(),
                wake_lock: Mutex::new(()),
                worker: Mutex::new(None),
                probes: Mutex::default(),
                owners: AtomicUsize::new(1),
            }),
        }
    }

    /// Go `Add`: publishes or replaces a failed store.
    pub fn add(&self, address: impl Into<String>, client: Arc<dyn MppAliveClient>) {
        let address = address.into();
        let state = Arc::new(MppStoreState {
            address: address.clone(),
            client,
            times: Mutex::new(StoreTimes {
                recovery_time: None,
                last_lookup_time: Instant::now(),
                last_detect_time: None,
            }),
        });
        self.inner
            .stores
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(address, state);
    }

    /// Go `Delete`.
    pub fn delete(&self, address: &str) -> bool {
        self.inner
            .stores
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(address)
            .is_some()
    }

    /// Go `IsRecovery`. An address absent from the failed map is available.
    #[must_use]
    pub fn is_recovery(&self, address: &str, recovery_ttl: Duration) -> bool {
        let state = self
            .inner
            .stores
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(address)
            .cloned();
        let Some(state) = state else {
            return true;
        };
        let Ok(mut times) = state.times.try_lock() else {
            return false;
        };
        times.last_lookup_time = Instant::now();
        times
            .recovery_time
            .is_some_and(|recovered| recovered.elapsed() > recovery_ttl)
    }

    /// Starts one asynchronous detection for every currently failed store.
    /// Like Go's `scan`, the method does not wait for per-store probes.
    pub fn scan(&self) {
        scan(&self.inner);
    }

    /// Go `Run`: starts at most one background scanner.
    pub fn run(&self) {
        let mut worker = self
            .inner
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if worker.is_some() {
            return;
        }
        self.inner.stopped.store(false, SeqCst);
        self.inner
            .probes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = false;
        let inner = Arc::clone(&self.inner);
        *worker = Some(
            std::thread::Builder::new()
                .name("tidb-mpp-prober".to_owned())
                .spawn(move || {
                    while !inner.stopped.load(SeqCst) {
                        let guard = inner
                            .wake_lock
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        let _ = inner.wake.wait_timeout(guard, Duration::from_secs(1));
                        if !inner.stopped.load(SeqCst) {
                            scan(&inner);
                        }
                    }
                })
                .expect("spawn MPP failed-store prober"),
        );
    }

    /// Stops admission and joins the scanner and all outstanding store probes.
    pub fn stop(&self) {
        // Serialize Run/Stop through joining, so a replacement scanner cannot
        // race the old generation's probe drain.
        let mut worker = self
            .inner
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        {
            let _wake = self
                .inner
                .wake_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.inner.stopped.store(true, SeqCst);
            self.inner.wake.notify_all();
        }
        if let Some(worker) = worker.take() {
            let _ = worker.join();
        }
        let mut probes = self
            .inner
            .probes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        probes.closed = true;
        for probe in probes.handles.drain(..) {
            let _ = probe.join();
        }
        self.inner
            .stores
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    /// Number of failed-store entries, for diagnostics and tests.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .stores
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Whether no failed store is tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Clone for MppFailedStoreProber {
    fn clone(&self) -> Self {
        self.inner.owners.fetch_add(1, SeqCst);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for MppFailedStoreProber {
    fn drop(&mut self) {
        if self.inner.owners.fetch_sub(1, SeqCst) == 1 {
            self.stop();
        }
    }
}

fn scan(inner: &Arc<ProbeInner>) {
    let mut probes = inner
        .probes
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if probes.closed {
        return;
    }
    let mut active = Vec::new();
    for probe in probes.handles.drain(..) {
        if probe.is_finished() {
            let _ = probe.join();
        } else {
            active.push(probe);
        }
    }
    probes.handles = active;
    let stores = inner
        .stores
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .values()
        .cloned()
        .collect::<Vec<_>>();
    for state in stores {
        let inner = Arc::clone(inner);
        probes.handles.push(
            std::thread::Builder::new()
                .name("tidb-mpp-probe".into())
                .spawn(move || detect_one(&inner, &state))
                .expect("spawn MPP failed-store probe"),
        );
    }
}

fn detect_one(inner: &Arc<ProbeInner>, state: &Arc<MppStoreState>) {
    let Ok(mut times) = state.times.try_lock() else {
        return;
    };
    if times
        .last_detect_time
        .is_some_and(|last| last.elapsed() < inner.detect_period)
    {
        return;
    }
    let available = state
        .client
        .is_alive(&state.address, inner.detect_timeout)
        .unwrap_or(false);
    times.last_detect_time = Some(Instant::now());
    if available {
        times.recovery_time.get_or_insert_with(Instant::now);
    } else {
        times.recovery_time = None;
    }
    let remove = times
        .recovery_time
        .is_some_and(|time| time.elapsed() > inner.max_recovery)
        || (times.recovery_time.is_none() && times.last_lookup_time.elapsed() > inner.max_obsolete);
    drop(times);
    if remove {
        inner
            .stores
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&state.address);
    }
}

/// Go `MPPServerInfo`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MppServerInfo {
    /// TiFlash address.
    pub address: String,
    /// Logical CPU count reported by TiFlash.
    pub logical_cpu_count: u64,
    /// TiFlash start timestamp.
    pub start_timestamp: i64,
}

/// Go `MppServerInfoManager`, including its promoting 1000-entry LRU.
#[derive(Default)]
pub struct MppServerInfoManager {
    state: Mutex<(HashMap<String, MppServerInfo>, VecDeque<String>)>,
}

impl MppServerInfoManager {
    /// Adds or replaces one server and promotes it to most-recently used.
    pub fn add(&self, info: MppServerInfo) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.1.retain(|address| address != &info.address);
        state.1.push_back(info.address.clone());
        state.0.insert(info.address.clone(), info);
        while state.0.len() > MPP_SERVER_INFO_CACHE_SIZE {
            if let Some(address) = state.1.pop_front() {
                state.0.remove(&address);
            }
        }
    }

    /// Deletes one server.
    pub fn delete(&self, address: &str) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.0.remove(address);
        state.1.retain(|candidate| candidate != address);
    }

    /// Gets and promotes one server.
    #[must_use]
    pub fn get(&self, address: &str) -> Option<MppServerInfo> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let info = state.0.get(address)?.clone();
        state.1.retain(|candidate| candidate != address);
        state.1.push_back(address.to_owned());
        Some(info)
    }

    /// Number of cached server entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0
            .len()
    }

    /// Whether no server entry is cached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Process-global Go `GlobalMPPFailedStoreProber` equivalent.
#[must_use]
pub fn global_mpp_failed_store_prober() -> &'static MppFailedStoreProber {
    static PROBER: OnceLock<MppFailedStoreProber> = OnceLock::new();
    PROBER.get_or_init(MppFailedStoreProber::default)
}

/// Process-global Go `GlobalMPPServerInfoManager` equivalent.
#[must_use]
pub fn global_mpp_server_info_manager() -> &'static MppServerInfoManager {
    static MANAGER: OnceLock<MppServerInfoManager> = OnceLock::new();
    MANAGER.get_or_init(MppServerInfoManager::default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU8;

    struct MockClient(AtomicU8);

    impl MppAliveClient for MockClient {
        fn is_alive(&self, _: &str, _: Duration) -> Result<bool, String> {
            match self.0.load(SeqCst) {
                0 => Err("store error".to_owned()),
                1 => Ok(false),
                _ => Ok(true),
            }
        }
    }

    #[test]
    fn failed_store_requires_a_continuous_recovery_ttl() {
        let prober = MppFailedStoreProber::new(
            Duration::ZERO,
            Duration::from_secs(1),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );
        let client = Arc::new(MockClient(AtomicU8::new(0)));
        prober.add("store", client.clone());
        let state = prober.inner.stores.lock().unwrap()["store"].clone();
        detect_one(&prober.inner, &state);
        assert!(!prober.is_recovery("store", Duration::ZERO));
        client.0.store(2, SeqCst);
        let state = prober.inner.stores.lock().unwrap()["store"].clone();
        detect_one(&prober.inner, &state);
        assert!(prober.is_recovery("store", Duration::ZERO));
        assert!(!prober.is_recovery("store", Duration::from_secs(60)));
        client.0.store(1, SeqCst);
        let state = prober.inner.stores.lock().unwrap()["store"].clone();
        detect_one(&prober.inner, &state);
        assert!(!prober.is_recovery("store", Duration::ZERO));
        assert!(prober.is_recovery("missing", Duration::ZERO));
    }

    #[test]
    fn mpp_failure_batch_stop_joins_active_detection() {
        struct GatedClient {
            entered: std::sync::mpsc::Sender<()>,
            release: Mutex<std::sync::mpsc::Receiver<()>>,
        }
        impl MppAliveClient for GatedClient {
            fn is_alive(&self, _: &str, _: Duration) -> Result<bool, String> {
                self.entered.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
                Ok(true)
            }
        }
        let (entered_tx, entered) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let prober = MppFailedStoreProber::default();
        prober.add(
            "blocked",
            Arc::new(GatedClient {
                entered: entered_tx,
                release: Mutex::new(release_rx),
            }),
        );
        prober.scan();
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        let stopping = prober.clone();
        let (done_tx, done) = std::sync::mpsc::channel();
        let join = std::thread::spawn(move || {
            stopping.stop();
            done_tx.send(()).unwrap();
        });
        let returned_early = done.recv_timeout(Duration::from_millis(100)).is_ok();
        release.send(()).unwrap();
        join.join().unwrap();
        assert!(
            !returned_early,
            "Stop returned while a probe still held the transport"
        );
    }

    #[test]
    fn mpp_failure_batch_cooldown_starts_after_detection() {
        struct FinishedClient(Arc<Mutex<Option<Instant>>>);
        impl MppAliveClient for FinishedClient {
            fn is_alive(&self, _: &str, _: Duration) -> Result<bool, String> {
                *self.0.lock().unwrap() = Some(Instant::now());
                Ok(true)
            }
        }
        let finished = Arc::new(Mutex::new(None));
        let prober = MppFailedStoreProber::default();
        prober.add("slow", Arc::new(FinishedClient(finished.clone())));
        let state = prober.inner.stores.lock().unwrap()["slow"].clone();
        detect_one(&prober.inner, &state);
        assert!(
            state.times.lock().unwrap().last_detect_time.unwrap()
                >= finished.lock().unwrap().unwrap(),
            "cooldown must begin after the RPC finishes"
        );
    }

    #[test]
    fn mpp_failure_batch_last_owner_drop_joins_scanner() {
        let prober = MppFailedStoreProber::default();
        let observer = Arc::downgrade(&prober.inner);
        prober.run();
        drop(prober);
        assert!(
            observer.upgrade().is_none(),
            "scanner retained its own shutdown owner"
        );
    }

    #[test]
    fn server_info_manager_promotes_get_before_lru_eviction() {
        let manager = MppServerInfoManager::default();
        for index in 0..MPP_SERVER_INFO_CACHE_SIZE {
            manager.add(MppServerInfo {
                address: format!("store-{index}"),
                ..MppServerInfo::default()
            });
        }
        assert!(manager.get("store-0").is_some());
        manager.add(MppServerInfo {
            address: format!("store-{MPP_SERVER_INFO_CACHE_SIZE}"),
            ..MppServerInfo::default()
        });
        assert_eq!(manager.len(), MPP_SERVER_INFO_CACHE_SIZE);
        assert!(manager.get("store-0").is_some());
        assert!(manager.get("store-1").is_none());
    }

    #[test]
    fn background_prober_is_singleton_and_stoppable() {
        let prober = MppFailedStoreProber::default();
        prober.run();
        prober.run();
        prober.stop();
    }
}
