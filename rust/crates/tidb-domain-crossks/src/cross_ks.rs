// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, Instant};

use tidb_domain_sqlsvrapi::{KsRuntimeHandle, Runtime};
use tidb_gocontext::Context;

/// Go `crossKSRuntimeIdleTimeout`.
const CROSS_KS_RUNTIME_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// Go `crossKSRuntimeSweepInterval`.
const CROSS_KS_RUNTIME_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Go `SessionManager`'s runtime and close operations, implemented by the owner
/// of the target store, sessions, schema synchronization and server registration.
/// This trait is the Rust boundary for Go's concrete resource owner, not a
/// factory for reusing the current keyspace's sessions.
pub trait SessionManager<C: ?Sized>: Runtime<C> {
    /// Go `SessionManager.close`: closes sessions and workers before leases,
    /// etcd and the target store. The SYSTEM store is owned outside the runtime.
    fn close(&self);
}

/// Go `runtimeEntry`.
struct RuntimeEntry<C: ?Sized> {
    sess_mgr: Arc<dyn SessionManager<C>>,
    active_holders: HashSet<String>,
    last_release_at: Option<Instant>,
}

/// Go `Manager`.
pub struct Manager<C: ?Sized> {
    store_keyspace: String,
    runtimes: Mutex<HashMap<String, RuntimeEntry<C>>>,
}

impl<C: ?Sized + 'static> Manager<C> {
    /// Go `NewManager`: remembers the owning store's keyspace, not global config.
    pub fn new(store_keyspace: String) -> Self {
        Self {
            store_keyspace,
            runtimes: Mutex::new(HashMap::new()),
        }
    }

    /// Go `Manager.GetAllKeyspace`.
    pub fn get_all_keyspace(&self) -> Vec<String> {
        self.runtimes.lock().unwrap().keys().cloned().collect()
    }

    /// Go `Manager.get`.
    fn get(&self, ks: &str) -> Option<Arc<dyn SessionManager<C>>> {
        self.runtimes
            .lock()
            .unwrap()
            .get(ks)
            .map(|e| Arc::clone(&e.sess_mgr))
    }

    /// Go `Manager.validateTargetKS`.
    fn validate_target_ks(&self, ks: &str) -> Result<(), String> {
        if tidb_config_kerneltype::is_classic() || self.store_keyspace == ks {
            return Err(
                "cross keyspace is not available in classic kernel or current keyspace".into(),
            );
        }
        Ok(())
    }

    /// Go `Manager.getOrCreateEntryWithoutLock`. Only successful creation is cached.
    fn get_or_create_entry_without_lock<'a>(
        runtimes: &'a mut HashMap<String, RuntimeEntry<C>>,
        ks: &str,
        create_session_manager: impl FnOnce(&str) -> Result<Arc<dyn SessionManager<C>>, String>,
    ) -> Result<&'a mut RuntimeEntry<C>, String> {
        if !runtimes.contains_key(ks) {
            let sess_mgr = create_session_manager(ks)?;
            runtimes.insert(
                ks.to_owned(),
                RuntimeEntry {
                    sess_mgr,
                    active_holders: HashSet::new(),
                    last_release_at: None,
                },
            );
        }
        Ok(runtimes.get_mut(ks).unwrap())
    }

    /// Go `Manager.GetOrCreate`: raw callers do not start an idle deadline.
    pub fn get_or_create(
        &self,
        ks: &str,
        create_session_manager: impl FnOnce(&str) -> Result<Arc<dyn SessionManager<C>>, String>,
    ) -> Result<Arc<dyn SessionManager<C>>, String> {
        self.validate_target_ks(ks)?;
        if let Some(manager) = self.get(ks) {
            return Ok(manager);
        }
        let mut runtimes = self.runtimes.lock().unwrap();
        Ok(Arc::clone(
            &Self::get_or_create_entry_without_lock(&mut runtimes, ks, create_session_manager)?
                .sess_mgr,
        ))
    }

    /// Go `Manager.Acquire`: creates under the lock and rejects duplicate holders.
    pub fn acquire(
        self: &Arc<Self>,
        ks: &str,
        holder_id: &str,
        create_session_manager: impl FnOnce(&str) -> Result<Arc<dyn SessionManager<C>>, String>,
    ) -> Result<Arc<dyn KsRuntimeHandle<C>>, String> {
        if holder_id.is_empty() {
            return Err("cross keyspace runtime holderID must not be empty".into());
        }
        self.validate_target_ks(ks)?;
        let mut runtimes = self.runtimes.lock().unwrap();
        let entry =
            Self::get_or_create_entry_without_lock(&mut runtimes, ks, create_session_manager)?;
        if !entry.active_holders.insert(holder_id.to_owned()) {
            return Err(format!("cross keyspace runtime for keyspace {ks} is already acquired by holderID {holder_id}"));
        }
        Ok(Arc::new(RuntimeHandle {
            manager: Arc::clone(self),
            target_ks: ks.to_owned(),
            holder_id: holder_id.to_owned(),
            sess_mgr: Arc::clone(&entry.sess_mgr),
            release_once: Once::new(),
        }))
    }

    /// Go `Manager.release`.
    fn release(&self, target_ks: &str, holder_id: &str) {
        let mut runtimes = self.runtimes.lock().unwrap();
        let Some(entry) = runtimes.get_mut(target_ks) else {
            return;
        };
        entry.active_holders.remove(holder_id);
        if entry.active_holders.is_empty() {
            entry.last_release_at = Some(Instant::now());
        }
    }

    /// Go `Manager.RunSystemKSGCLoop`: its domain caller restricts it to SYSTEM.
    pub fn run_system_ks_gc_loop(&self, ctx: &Context) {
        let mut config = (
            CROSS_KS_RUNTIME_SWEEP_INTERVAL,
            CROSS_KS_RUNTIME_IDLE_TIMEOUT,
        );
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/domain/crossks/mockRuntimeGCLoopConfig",
            &mut config,
        );
        let ticker = crossbeam_channel::tick(config.0);
        loop {
            crossbeam_channel::select! {
                recv(ctx.done()) -> _ => return,
                recv(ticker) -> _ => self.sweep_idle_runtimes(config.1),
            }
        }
    }

    /// Go `Manager.sweepIdleRuntimes`: detach under lock, close outside it.
    fn sweep_idle_runtimes(&self, idle_timeout: Duration) {
        let now = Instant::now();
        let evicted = {
            let mut runtimes = self.runtimes.lock().unwrap();
            let keys: Vec<_> = runtimes
                .iter()
                .filter(|(_, entry)| {
                    entry.active_holders.is_empty()
                        && entry
                            .last_release_at
                            .is_some_and(|released| now.duration_since(released) >= idle_timeout)
                })
                .map(|(ks, _)| ks.clone())
                .collect();
            keys.into_iter()
                .map(|ks| runtimes.remove(&ks).unwrap())
                .collect::<Vec<_>>()
        };
        for entry in evicted {
            entry.sess_mgr.close();
        }
    }

    /// Go `Manager.Close`: closes all entries regardless of holders or idle time.
    pub fn close(&self) {
        let mut runtimes = self.runtimes.lock().unwrap();
        for entry in runtimes.values() {
            entry.sess_mgr.close();
        }
        runtimes.clear();
    }
}

/// Go `runtimeHandle`. Dropping a handle does not implicitly release it in Go.
struct RuntimeHandle<C: ?Sized> {
    manager: Arc<Manager<C>>,
    target_ks: String,
    holder_id: String,
    sess_mgr: Arc<dyn SessionManager<C>>,
    release_once: Once,
}

impl<C: ?Sized + 'static> Runtime<C> for RuntimeHandle<C> {
    /// Go `runtimeHandle.Store().GetKeyspace`.
    fn store_keyspace(&self) -> String {
        self.sess_mgr.store_keyspace()
    }
    /// Go `runtimeHandle.SysSessionPool`.
    fn sys_session_pool(&self) -> Arc<dyn tidb_syssession::Pool<C>> {
        self.sess_mgr.sys_session_pool()
    }
}

impl<C: ?Sized + 'static> KsRuntimeHandle<C> for RuntimeHandle<C> {
    /// Go `runtimeHandle.Release`.
    fn release(&self) {
        self.release_once
            .call_once(|| self.manager.release(&self.target_ks, &self.holder_id));
    }
    /// Go `runtimeHandle` as `sqlsvrapi.Runtime`.
    fn as_runtime(self: Arc<Self>) -> Arc<dyn Runtime<C>> {
        self
    }
}

#[cfg(test)]
#[path = "cross_ks_internal_test.rs"]
mod tests;
