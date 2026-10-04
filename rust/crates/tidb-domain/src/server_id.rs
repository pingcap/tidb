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

//! Domain's leased numeric server identity and connection-admission lifetime.
//! Go `domain.go`: acquireServerID, proposeServerID, refreshServerIDTTL,
//! serverIDKeeper and releaseServerID. Connection IDs consume this live owner.

use crate::serverinfo_syncer::{EtcdOps, Syncer};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};
use tidb_util::globalconn::{MAX_SERVER_ID32, MAX_SERVER_ID64};

const SERVER_ID_PATH: &str = "/tidb/server_id";

/// Source timing policy; tests can shorten intervals without changing production.
#[derive(Clone, Copy)]
pub struct ServerIdIntervals {
    /// Lease TTL (Go: twelve hours).
    pub ttl: Duration,
    /// Refresh the leased claim (Go: five minutes).
    pub refresh: Duration,
    /// Retry after connectivity loss (Go: ten seconds).
    pub retry: Duration,
    /// Maximum interval without a successful refresh (Go: six hours).
    pub lost_timeout: Duration,
    /// Wait between candidate collisions (Go: 300 milliseconds).
    pub collision_retry: Duration,
}
impl Default for ServerIdIntervals {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(12 * 3600),
            refresh: Duration::from_secs(300),
            retry: Duration::from_secs(10),
            lost_timeout: Duration::from_secs(6 * 3600),
            collision_retry: Duration::from_millis(300),
        }
    }
}

type KillConnections = Arc<dyn Fn() + Send + Sync>;

/// One process identity shared by publication, connection allocation and admission.
pub struct ServerIdAuthority {
    id: AtomicU64,
    lost: AtomicBool,
    enable_32bits: bool,
    etcd: Option<Arc<dyn EtcdOps>>,
    kill_connections: Mutex<Option<KillConnections>>,
}
impl ServerIdAuthority {
    /// A nil etcd client is Go's standalone mode, with numeric identity one.
    #[must_use]
    pub fn new(etcd: Option<Arc<dyn EtcdOps>>, enable_32bits: bool) -> Arc<Self> {
        let clustered = etcd.is_some();
        Arc::new(Self {
            id: AtomicU64::new(if clustered { 0 } else { 1 }),
            lost: AtomicBool::new(clustered),
            enable_32bits,
            etcd,
            kill_connections: Mutex::new(None),
        })
    }
    /// Numeric ID read by both globalconn and server-info serialization.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.id.load(Ordering::Acquire)
    }
    /// Go IsLostConnectionToPD: new sockets must be closed before authentication.
    #[must_use]
    pub fn is_lost(&self) -> bool {
        self.lost.load(Ordering::Acquire)
    }
    /// Installs the server's actual socket/query retirement callback.
    pub fn set_connection_killer(&self, kill: Option<KillConnections>) {
        *self
            .kill_connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = kill;
    }
    fn mark_lost(&self) {
        if !self.lost.swap(true, Ordering::AcqRel) {
            let kill = self
                .kill_connections
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(kill) = kill {
                kill();
            }
        }
    }
}

struct LeaseState {
    authority: Arc<ServerIdAuthority>,
    lease: Option<i64>,
    last_success: Option<Instant>,
    policy: ServerIdIntervals,
}
impl LeaseState {
    fn acquire(&mut self, syncer: &Syncer, stop: &mpsc::Receiver<()>) -> Result<(), String> {
        if !matches!(stop.try_recv(), Err(mpsc::TryRecvError::Empty)) {
            return Err("server-ID keeper stopped".into());
        }
        self.authority.id.store(0, Ordering::Release);
        let etcd = self
            .authority
            .etcd
            .as_ref()
            .expect("cluster keeper has etcd");
        // Go's concurrency.Session independently signals expiration. During
        // recovery, recheck it before reusing a claim lease after an outage.
        if let Some(lease) = self.lease {
            if etcd.lease_keep_alive_ttl(lease)? <= 0 {
                self.lease = None;
            }
        }
        let lease = match self.lease {
            Some(lease) => lease,
            None => {
                let lease = etcd.lease_grant(self.policy.ttl.as_secs() as i64)?;
                self.lease = Some(lease);
                lease
            }
        };
        let mut conflicts = 0;
        loop {
            if !matches!(stop.try_recv(), Err(mpsc::TryRecvError::Empty)) {
                return Err("server-ID keeper stopped".into());
            }
            let id = self.propose(syncer, conflicts)?;
            if etcd.create_if_absent_with_lease(&format!("{SERVER_ID_PATH}/{id}"), b"0", lease)? {
                self.authority.id.store(id, Ordering::Release);
                return Ok(());
            }
            conflicts += 1;
            if !matches!(
                stop.recv_timeout(self.policy.collision_retry),
                Err(mpsc::RecvTimeoutError::Timeout)
            ) {
                return Err("server-ID keeper stopped".into());
            }
        }
    }
    fn propose(&self, syncer: &Syncer, conflicts: usize) -> Result<u64, String> {
        if !self.authority.enable_32bits {
            return random_id(1, MAX_SERVER_ID64);
        }
        if conflicts < 3 {
            let peers = syncer.all_server_info()?;
            if peers.len() as f32 <= 0.9 * MAX_SERVER_ID32 as f32 {
                let ids = peers
                    .values()
                    .map(|info| {
                        info.static_info
                            .server_id_getter
                            .as_ref()
                            .map_or(info.static_info.json_server_id, |get| get())
                    })
                    .collect::<std::collections::HashSet<_>>();
                for _ in 0..15 {
                    let id = random_id(1, MAX_SERVER_ID32)?;
                    if !ids.contains(&id) {
                        return Ok(id);
                    }
                }
            }
        }
        random_id(MAX_SERVER_ID32 + 1, MAX_SERVER_ID64)
    }
    fn refresh(&mut self) -> Result<(), String> {
        let etcd = self
            .authority
            .etcd
            .as_ref()
            .expect("cluster keeper has etcd");
        let lease = match self.lease {
            Some(lease) => lease,
            None => {
                let lease = etcd.lease_grant(self.policy.ttl.as_secs() as i64)?;
                self.lease = Some(lease);
                lease
            }
        };
        // Go concurrency.Session owns lease keepalive independently of the
        // claim refresh. Both operations run under this retained Rust worker.
        if etcd.lease_keep_alive_ttl(lease)? <= 0 {
            self.lease = None;
            self.authority.mark_lost();
            return Err("server-ID lease expired".into());
        }
        let key = format!("{SERVER_ID_PATH}/{}", self.authority.id());
        etcd.put_with_lease_retry(&key, b"0", lease, 3)
    }
    fn tick(&mut self, now: Instant, syncer: &Syncer, stop: &mpsc::Receiver<()>) {
        if self.authority.is_lost() {
            if self.acquire(syncer, stop).is_ok() {
                // Publish the new identity before reopening admission.
                if let Err(error) = syncer.store_server_info() {
                    tracing::warn!(%error, "StoreServerInfo after server-ID recovery");
                }
                self.last_success = Some(now);
                self.authority.lost.store(false, Ordering::Release);
            }
        } else {
            match self.refresh() {
                Ok(()) => self.last_success = Some(now),
                Err(error) => {
                    tracing::warn!(%error, "refreshServerIDTTL failed");
                    // Go initializes lastSucceedTimestamp to the zero time.
                    if !self.policy.lost_timeout.is_zero()
                        && self.last_success.is_none_or(|last| {
                            now.saturating_duration_since(last) > self.policy.lost_timeout
                        })
                    {
                        self.authority.mark_lost();
                    }
                }
            }
        }
    }
    fn release(&mut self) {
        self.authority.mark_lost();
        self.authority.id.store(0, Ordering::Release);
        if let Some(lease) = self.lease.take() {
            if let Some(etcd) = &self.authority.etcd {
                if let Err(error) = etcd.lease_revoke(lease) {
                    tracing::warn!(%error, "releaseServerID failed");
                }
            }
        }
    }
}
impl Drop for LeaseState {
    fn drop(&mut self) {
        self.release();
    }
}

fn random_id(min: u64, max: u64) -> Result<u64, String> {
    let range = max - min + 1;
    // Rejection sampling, preserving Go's uniform random candidate distribution.
    let bound = u64::MAX - u64::MAX % range;
    loop {
        let value = getrandom::u64().map_err(|error| error.to_string())?;
        if value < bound {
            return Ok(min + value % range);
        }
    }
}

/// Retained and joined Domain server-ID keeper. Drop precedes etcd retirement.
pub struct ServerIdKeeper {
    stop: Option<mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl ServerIdKeeper {
    /// Acquire once at startup, then own refresh/loss/recovery until joined close.
    pub fn start(
        authority: Arc<ServerIdAuthority>,
        syncer: Arc<Syncer>,
        policy: ServerIdIntervals,
    ) -> Result<Self, String> {
        if authority.etcd.is_none() {
            return Ok(Self {
                stop: None,
                thread: None,
            });
        }
        let (send, receive) = mpsc::channel();
        let mut state = LeaseState {
            authority,
            lease: None,
            last_success: None,
            policy,
        };
        match state.acquire(&syncer, &receive) {
            Ok(()) => {
                syncer.store_server_info()?;
                state.authority.lost.store(false, Ordering::Release);
            }
            Err(error) => tracing::warn!(%error, "acquire serverID failed; keeper will retry"),
        }
        let thread = std::thread::Builder::new()
            .name("server-id-keeper".into())
            .spawn(move || loop {
                let interval = if state.authority.is_lost() {
                    policy.retry
                } else {
                    policy.refresh
                };
                if !matches!(
                    receive.recv_timeout(interval),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ) {
                    break;
                }
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    state.tick(Instant::now(), &syncer, &receive)
                }))
                .is_err()
                {
                    state.authority.mark_lost();
                    tracing::error!(
                        "server-ID keeper recovered panic; admission closed until reacquisition"
                    );
                }
            })
            .map_err(|error| error.to_string())?;
        Ok(Self {
            stop: Some(send),
            thread: Some(thread),
        })
    }
}
impl Drop for ServerIdKeeper {
    fn drop(&mut self) {
        self.stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serverinfo::ServerInfo;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicI64, AtomicUsize};

    #[derive(Default)]
    struct Etcd {
        keys: Mutex<BTreeMap<String, (Vec<u8>, i64)>>,
        next: AtomicI64,
        fail: AtomicBool,
        expired: AtomicBool,
        conflicts: AtomicUsize,
    }
    impl EtcdOps for Etcd {
        fn lease_grant(&self, _: i64) -> Result<i64, String> {
            Ok(self.next.fetch_add(1, Ordering::SeqCst) + 1)
        }
        fn lease_keep_alive_once(&self, _: i64) -> Result<(), String> {
            if self.fail.load(Ordering::SeqCst) {
                Err("unavailable".into())
            } else {
                Ok(())
            }
        }
        fn lease_keep_alive_ttl(&self, lease: i64) -> Result<i64, String> {
            self.lease_keep_alive_once(lease)?;
            Ok(if self.expired.load(Ordering::SeqCst) {
                0
            } else {
                43200
            })
        }
        fn lease_revoke(&self, lease: i64) -> Result<(), String> {
            self.keys
                .lock()
                .unwrap()
                .retain(|_, (_, owner)| *owner != lease);
            Ok(())
        }
        fn put_with_lease(&self, key: &str, value: &[u8], lease: i64) -> Result<(), String> {
            self.keys
                .lock()
                .unwrap()
                .insert(key.into(), (value.to_vec(), lease));
            Ok(())
        }
        fn create_if_absent_with_lease(
            &self,
            key: &str,
            value: &[u8],
            lease: i64,
        ) -> Result<bool, String> {
            if self
                .conflicts
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Ok(false);
            }
            let mut keys = self.keys.lock().unwrap();
            if keys.contains_key(key) {
                return Ok(false);
            }
            keys.insert(key.into(), (value.to_vec(), lease));
            Ok(true)
        }
        fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
            Ok(self
                .keys
                .lock()
                .unwrap()
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .map(|(key, (value, _))| (key.clone(), value.clone()))
                .collect())
        }
        fn delete(&self, key: &str) -> Result<(), String> {
            self.keys.lock().unwrap().remove(key);
            Ok(())
        }
        fn put(&self, key: &str, value: &[u8]) -> Result<(), String> {
            self.put_with_lease(key, value, 0)
        }
        fn delete_prefix(&self, prefix: &str) -> Result<(), String> {
            self.keys
                .lock()
                .unwrap()
                .retain(|key, _| !key.starts_with(prefix));
            Ok(())
        }
    }
    fn fixture(etcd: &Arc<Etcd>, name: &str) -> (Arc<ServerIdAuthority>, Arc<Syncer>) {
        let owner = ServerIdAuthority::new(Some(etcd.clone()), true);
        let mut info = ServerInfo::default();
        info.static_info.id = name.into();
        let getter = owner.clone();
        info.static_info.server_id_getter = Some(Arc::new(move || getter.id()));
        let syncer = Arc::new(Syncer::new_with_status_endpoint_claim(
            info,
            Some(etcd.clone()),
            false,
        ));
        syncer.new_session_and_store_server_info().unwrap();
        (owner, syncer)
    }
    fn policy() -> ServerIdIntervals {
        ServerIdIntervals {
            collision_retry: Duration::ZERO,
            ..Default::default()
        }
    }

    #[test]
    fn cluster_lifecycle_batch_claim_collision_fallback_and_release() {
        let etcd = Arc::new(Etcd::default());
        let (first, first_info) = fixture(&etcd, "first");
        let first_keeper = ServerIdKeeper::start(first.clone(), first_info, policy()).unwrap();
        assert!((1..=MAX_SERVER_ID32).contains(&first.id()));
        etcd.conflicts.store(3, Ordering::SeqCst);
        let (second, second_info) = fixture(&etcd, "second");
        let second_keeper =
            ServerIdKeeper::start(second.clone(), second_info.clone(), policy()).unwrap();
        assert!(second.id() > MAX_SERVER_ID32 && second.id() <= MAX_SERVER_ID64);
        assert_ne!(first.id(), second.id());
        let published: serde_json::Value =
            serde_json::from_slice(&etcd.keys.lock().unwrap()[second_info.server_info_path()].0)
                .unwrap();
        assert_eq!(published["server_id"], second.id());
        let key = format!("{SERVER_ID_PATH}/{}", second.id());
        drop(second_keeper);
        assert!(second.is_lost());
        assert_eq!(second.id(), 0);
        assert!(!etcd.keys.lock().unwrap().contains_key(&key));
        assert!(!first.is_lost());
        drop(first_keeper);
    }

    #[test]
    fn cluster_lifecycle_batch_loss_kills_once_then_recovers_and_republishes() {
        let etcd = Arc::new(Etcd::default());
        let (owner, syncer) = fixture(&etcd, "recover");
        let kills = Arc::new(AtomicUsize::new(0));
        let observed = kills.clone();
        owner.set_connection_killer(Some(Arc::new(move || {
            observed.fetch_add(1, Ordering::SeqCst);
        })));
        let (_send, receive) = mpsc::channel();
        let mut state = LeaseState {
            authority: owner.clone(),
            lease: None,
            last_success: None,
            policy: policy(),
        };
        state.acquire(&syncer, &receive).unwrap();
        owner.lost.store(false, Ordering::Release);
        etcd.fail.store(true, Ordering::SeqCst);
        state.tick(Instant::now(), &syncer, &receive);
        assert!(owner.is_lost());
        assert_eq!(kills.load(Ordering::SeqCst), 1);
        owner.mark_lost();
        assert_eq!(kills.load(Ordering::SeqCst), 1);
        etcd.fail.store(false, Ordering::SeqCst);
        state.tick(Instant::now(), &syncer, &receive);
        assert!(!owner.is_lost());
        let published: serde_json::Value =
            serde_json::from_slice(&etcd.keys.lock().unwrap()[syncer.server_info_path()].0)
                .unwrap();
        assert_eq!(published["server_id"], owner.id());
        // Even shortly after a successful refresh, a confirmed expired lease
        // must close admission before a new owner can reuse its ID.
        etcd.expired.store(true, Ordering::SeqCst);
        state.tick(Instant::now(), &syncer, &receive);
        assert!(owner.is_lost());
        assert!(state.lease.is_none());
        assert_eq!(kills.load(Ordering::SeqCst), 2);
        etcd.expired.store(false, Ordering::SeqCst);
        state.tick(Instant::now(), &syncer, &receive);
        assert!(!owner.is_lost());
    }

    #[test]
    fn cluster_lifecycle_batch_recent_refresh_tolerates_transient_failure_and_stop_cancels_acquire()
    {
        let etcd = Arc::new(Etcd::default());
        let (owner, syncer) = fixture(&etcd, "transient");
        let (send, receive) = mpsc::channel();
        let now = Instant::now();
        let mut state = LeaseState {
            authority: owner.clone(),
            lease: None,
            last_success: Some(now),
            policy: policy(),
        };
        state.acquire(&syncer, &receive).unwrap();
        owner.lost.store(false, Ordering::Release);
        etcd.fail.store(true, Ordering::SeqCst);
        state.tick(now + Duration::from_secs(1), &syncer, &receive);
        assert!(!owner.is_lost());
        state.tick(
            now + policy().lost_timeout + Duration::from_secs(1),
            &syncer,
            &receive,
        );
        assert!(owner.is_lost());
        drop(send);
        assert!(state
            .acquire(&syncer, &receive)
            .unwrap_err()
            .contains("stopped"));
    }
}
