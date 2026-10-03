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

//! Node-owned global bindings. Storage reload, owner GC and usage writes
//! share one live cache and independent internal sessions.

use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tidb_exec::catalog_watch::SharedCatalog as SharedClusterCatalog;
use tidb_exec::mysql_system_tables::{scan_system_table, SystemRow, SystemTableView};
use tidb_exec::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_executor::cluster_storage::MutationBuffer;
use tidb_session::binding_cache::SharedBindingCache;
use tidb_session::vars::GlobalSysvars;
use tidb_txnkv::transaction::{
    RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient, StoreWriteLoader,
};

/// Go pkg/bindinfo.Lease, independent of the schema lease.
const BINDING_LEASE: Duration = Duration::from_secs(3);
const COLUMNS: &[&str] = &[
    "original_sql",
    "bind_sql",
    "default_db",
    "status",
    "create_time",
    "update_time",
    "charset",
    "collation",
    "source",
    "sql_digest",
    "plan_digest",
];

/// Internal-session capability installed once the process factory is ready.
/// Each operation owns its transaction; no user transaction supplies storage.
pub trait BindingSessionPool: Send + Sync {
    /// Incremental SELECT with Go's clock-skew boundary, or initial full load.
    fn load(&self, boundary: Option<&str>) -> Result<Vec<Vec<tidb_datatype::Datum>>, String>;
    /// Go's serialized tombstone GC transaction.
    fn gc(&self) -> Result<(), String>;
    /// Persist usage in bounded transactions and acknowledge committed batches.
    fn write_usage(&self, bindings: &[Arc<tidb_session::binding::Binding>]) -> Result<(), String>;
}

/// The node's committed binding image and its storage refresh authority.
pub trait ClusterBindings: Send + Sync {
    /// Shared live planner cache.
    fn cache(&self) -> SharedBindingCache;
    /// Refresh from committed storage; failures retain the prior image.
    fn reload(&self) -> Result<(), String>;
    /// Whether a pending commit modifies a binding record.
    fn has_changes(&self, buffer: &MutationBuffer) -> bool;
    /// Connect the maintained internal-session owner after factory creation.
    fn attach_session_pool(&self, pool: Arc<dyn BindingSessionPool>);
    /// Run owner-gated tombstone collection.
    fn gc(&self) -> Result<(), String>;
    /// Run this node's usage flush when the live process switch allows it.
    fn write_usage(&self) -> Result<(), String>;
    /// Join shared cache and election lifetimes after the worker stops.
    fn close(&self);
}

struct StoredBindings<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> {
    opener: RealOptimisticTransactionOpener<C, L, P>,
    catalog: Arc<SharedClusterCatalog>,
    globals: GlobalSysvars,
    cache: SharedBindingCache,
    timeout: Duration,
    owner: Arc<dyn tidb_owner::Manager>,
    sessions: RwLock<Option<Arc<dyn BindingSessionPool>>>,
    // Serialize the entire read/publication, preventing an older reload from
    // overwriting the image reloaded after a successful local commit.
    refresh: Mutex<()>,
    record_range: RwLock<Option<(tidb_txnkv::Key, tidb_txnkv::Key)>>,
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> ClusterBindings
    for StoredBindings<C, L, P>
{
    fn cache(&self) -> SharedBindingCache {
        self.cache.clone()
    }

    fn reload(&self) -> Result<(), String> {
        let _refresh = self
            .refresh
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let catalog = self.catalog.load();
        let view = SystemTableView::locate(&catalog, "bind_info", COLUMNS)
            .map_err(|error| error.to_string())?;
        // Go readBindingsFromStorage runs SELECT through an internal session.
        // bind_info's create_time/update_time are TIMESTAMP(6), so decoding
        // needs that session's timezone rather than the no-TIMESTAMP shortcut.
        let timezone = crate::real_tikv_node::RealTiKvSessionTimeZone::parse(
            &self
                .globals
                .get("time_zone")
                .map_err(|error| format!("binding session time_zone: {error:?}"))?,
        )
        .map_err(|error| error.to_string())?
        .zone();
        let start = view.record_prefix(&[]).map_err(|error| error.to_string())?;
        let mut end = start.clone();
        // Record prefixes end in '_r'; its successor bounds precisely this table.
        *end.last_mut().expect("record prefix") += 1;
        let pool = self.sessions.read().unwrap().clone();
        let rows = if let Some(pool) = pool {
            pool.load(self.cache.load().update_time_boundary().as_deref())?
        } else {
            let mut transaction = self.opener.begin().map_err(|error| error.to_string())?;
            let rows = {
                let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
                scan_system_table(&mut snapshot, &view)
                    .map_err(|error| error.to_string())?
                    .into_iter()
                    .map(|(key, value)| {
                        let row =
                            SystemRow::parse_in_timezone(&view, &key, &value, Some(&timezone))
                                .map_err(|error| error.to_string())?;
                        COLUMNS
                            .iter()
                            .map(|name| {
                                row.datum(name)
                                    .map(|value| {
                                        value.cloned().unwrap_or(tidb_datatype::Datum::Null)
                                    })
                                    .map_err(|error| error.to_string())
                            })
                            .collect::<Result<Vec<_>, String>>()
                    })
                    .collect::<Result<Vec<_>, String>>()?
            };
            transaction
                .finish_without_writes()
                .map_err(|error| error.to_string())?;
            rows
        };
        let quota = self
            .globals
            .get("tidb_mem_quota_binding_cache")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| self.cache.load().mem_capacity());
        self.cache.load().update_storage_rows(rows, quota, false);
        *self
            .record_range
            .write()
            .unwrap_or_else(|error| error.into_inner()) = Some((start.into(), end.into()));
        Ok(())
    }

    fn attach_session_pool(&self, pool: Arc<dyn BindingSessionPool>) {
        *self.sessions.write().unwrap() = Some(pool);
    }
    fn gc(&self) -> Result<(), String> {
        if !self.owner.is_owner() {
            return Ok(());
        }
        let pool = self.sessions.read().unwrap().clone();
        if let Some(pool) = pool {
            pool.gc()?;
        }
        Ok(())
    }
    fn write_usage(&self) -> Result<(), String> {
        if !self
            .globals
            .get("tidb_enable_binding_usage")
            .ok()
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("ON"))
        {
            return Ok(());
        }
        let pool = self.sessions.read().unwrap().clone();
        if let Some(pool) = pool {
            pool.write_usage(&self.cache.load().get_all_bindings())?;
        }
        Ok(())
    }
    fn close(&self) {
        self.cache.load().close();
        self.owner.close();
    }

    fn has_changes(&self, buffer: &MutationBuffer) -> bool {
        let range = self
            .record_range
            .read()
            .unwrap_or_else(|error| error.into_inner());
        range
            .as_ref()
            .is_some_and(|(start, end)| buffer.has_keys_in_range(start, end))
    }
}

/// Stops and joins the binding worker before its transaction authority closes.
pub struct BindingReloader {
    stop: Option<mpsc::Sender<()>>,
    worker: Option<JoinHandle<()>>,
}

impl Drop for BindingReloader {
    fn drop(&mut self) {
        self.stop.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Load the initial image before accepting connections, then follow peer changes.
pub fn start_binding_cache<C, L, P>(
    opener: RealOptimisticTransactionOpener<C, L, P>,
    catalog: Arc<SharedClusterCatalog>,
    globals: GlobalSysvars,
    timeout: Duration,
    owner: Arc<dyn tidb_owner::Manager>,
) -> Result<(Arc<dyn ClusterBindings>, BindingReloader), String>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    let bindings: Arc<dyn ClusterBindings> = Arc::new(StoredBindings {
        opener,
        catalog,
        globals,
        timeout,
        owner: Arc::clone(&owner),
        sessions: RwLock::new(None),
        cache: SharedBindingCache::default(),
        refresh: Mutex::new(()),
        record_range: RwLock::new(None),
    });
    bindings.reload()?;
    owner.campaign_owner(&[]).map_err(|error| {
        bindings.close();
        error
    })?;
    let worker = BindingReloader::spawn(
        Arc::clone(&bindings),
        [BINDING_LEASE, BINDING_LEASE * 100, BINDING_LEASE * 100],
        || Duration::from_secs(rand::random_range(10_800..=21_600)),
    )?;
    Ok((bindings, worker))
}

impl BindingReloader {
    fn spawn(
        target: Arc<dyn ClusterBindings>,
        intervals: [Duration; 3],
        next_usage: impl Fn() -> Duration + Send + 'static,
    ) -> Result<Self, String> {
        let (stop, stopped) = mpsc::channel();
        let cleanup = Arc::clone(&target);
        let worker = std::thread::Builder::new()
            .name("tidb-binding-maintenance".to_owned())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut deadlines = intervals.map(|interval| Instant::now() + interval);
                    loop {
                        let delay = deadlines
                            .iter()
                            .min()
                            .unwrap()
                            .saturating_duration_since(Instant::now());
                        if !matches!(
                            stopped.recv_timeout(delay),
                            Err(mpsc::RecvTimeoutError::Timeout)
                        ) {
                            break;
                        }
                        for index in 0..3 {
                            let now = Instant::now();
                            if now < deadlines[index] {
                                continue;
                            }
                            let (label, result) = match index {
                                0 => ("reload", target.reload()),
                                1 => ("GC", target.gc()),
                                _ => ("usage", target.write_usage()),
                            };
                            if let Err(error) = result {
                                eprintln!("binding {label} failed: {error}");
                            }
                            let interval = if index == 2 {
                                next_usage()
                            } else {
                                intervals[index]
                            };
                            let now = Instant::now();
                            deadlines[index] = if index == 2 {
                                now + interval
                            } else {
                                let elapsed = now.saturating_duration_since(deadlines[index]);
                                now + interval
                                    - Duration::from_nanos(
                                        (elapsed.as_nanos() % interval.as_nanos()) as u64,
                                    )
                            };
                        }
                    }
                }));
                target.close();
                if result.is_err() {
                    eprintln!("binding maintenance worker panicked");
                }
            })
            .map_err(|error| {
                cleanup.close();
                error.to_string()
            })?;
        Ok(Self {
            stop: Some(stop),
            worker: Some(worker),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[test]
    fn binding_gc_follows_current_owner_and_stops_after_retirement() {
        use tidb_owner::Manager;
        struct Pool(AtomicUsize);
        impl BindingSessionPool for Pool {
            fn load(&self, _: Option<&str>) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
                unreachable!()
            }
            fn gc(&self) -> Result<(), String> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            fn write_usage(&self, _: &[Arc<tidb_session::binding::Binding>]) -> Result<(), String> {
                unreachable!()
            }
        }
        let (_authority, _pd, opener) = crate::unistore_node::in_process_write_stack().unwrap();
        let owner = Arc::new(tidb_owner::MockManager::new(
            tidb_owner::Context::background(),
            "binding-test".to_owned(),
            Some(&format!("binding-test-{}", opener.authority_id())),
            "/tidb/bindinfo/owner",
        ));
        let pool = Arc::new(Pool(AtomicUsize::new(0)));
        let bindings = StoredBindings {
            opener,
            catalog: Arc::new(SharedClusterCatalog::new(
                tidb_exec::cluster_catalog::ClusterCatalog {
                    schema_version: 0,
                    databases: Vec::new(),
                },
            )),
            globals: GlobalSysvars::default(),
            cache: SharedBindingCache::default(),
            timeout: Duration::from_secs(1),
            owner: owner.clone(),
            sessions: RwLock::new(Some(pool.clone())),
            refresh: Mutex::new(()),
            record_range: RwLock::new(None),
        };
        bindings.gc().unwrap();
        assert_eq!(pool.0.load(Ordering::SeqCst), 0);
        owner.campaign_owner(&[]).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !owner.is_owner() {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        bindings.gc().unwrap();
        assert_eq!(pool.0.load(Ordering::SeqCst), 1);
        owner.campaign_cancel();
        assert!(!owner.is_owner());
        bindings.gc().unwrap();
        assert_eq!(pool.0.load(Ordering::SeqCst), 1);
        bindings.close();
    }

    #[derive(Default)]
    struct Target {
        cache: SharedBindingCache,
        reload: AtomicUsize,
        gc: AtomicUsize,
        usage: AtomicUsize,
        closed: AtomicBool,
    }
    impl ClusterBindings for Target {
        fn cache(&self) -> SharedBindingCache {
            self.cache.clone()
        }
        fn reload(&self) -> Result<(), String> {
            self.reload.fetch_add(1, Ordering::SeqCst);
            Err("injected reload failure".to_owned())
        }
        fn gc(&self) -> Result<(), String> {
            self.gc.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn write_usage(&self) -> Result<(), String> {
            self.usage.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn has_changes(&self, _: &MutationBuffer) -> bool {
            false
        }
        fn attach_session_pool(&self, _: Arc<dyn BindingSessionPool>) {}
        fn close(&self) {
            self.closed.store(true, Ordering::SeqCst);
            self.cache.load().close();
        }
    }
    #[test]
    fn independent_maintenance_ticks_continue_after_errors_and_join_on_close() {
        let target = Arc::new(Target::default());
        let worker = BindingReloader::spawn(
            target.clone(),
            [
                Duration::from_millis(5),
                Duration::from_millis(10),
                Duration::from_millis(15),
            ],
            || Duration::from_millis(10),
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while target.usage.load(Ordering::SeqCst) < 2 {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        drop(worker);
        assert!(target.closed.load(Ordering::SeqCst));
        let counts = [
            target.reload.load(Ordering::SeqCst),
            target.gc.load(Ordering::SeqCst),
            target.usage.load(Ordering::SeqCst),
        ];
        assert!(counts.iter().all(|count| *count >= 2));
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(
            counts,
            [
                target.reload.load(Ordering::SeqCst),
                target.gc.load(Ordering::SeqCst),
                target.usage.load(Ordering::SeqCst)
            ]
        );
    }
}
