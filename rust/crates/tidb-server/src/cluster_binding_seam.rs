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

//! Node-owned global bindings. Storage reloads use their own transaction;
//! the planner pins an immutable image and never reads mysql.bind_info.

use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use tidb_exec::catalog_watch::SharedCatalog as SharedClusterCatalog;
use tidb_exec::mysql_system_tables::{scan_system_table, SystemRow, SystemTableView};
use tidb_exec::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_executor::cluster_storage::MutationBuffer;
use tidb_session::binding_cache::{BindingCache, SharedBindingCache};
use tidb_session::vars::GlobalSysvars;
use tidb_txnkv::transaction::{
    RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient, StoreWriteLoader,
    MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES,
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
];

/// The node's committed binding image and its storage refresh authority.
pub trait ClusterBindings: Send + Sync {
    /// Shared immutable planner image.
    fn cache(&self) -> SharedBindingCache;
    /// Refresh from committed storage; failures retain the prior image.
    fn reload(&self) -> Result<(), String>;
    /// Whether a pending commit modifies a binding record.
    fn has_changes(&self, buffer: &MutationBuffer) -> bool;
}

struct StoredBindings<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> {
    opener: RealOptimisticTransactionOpener<C, L, P>,
    catalog: Arc<SharedClusterCatalog>,
    globals: GlobalSysvars,
    cache: SharedBindingCache,
    timeout: Duration,
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
        let start = view.record_prefix(&[]).map_err(|error| error.to_string())?;
        let mut end = start.clone();
        // Record prefixes end in '_r'; its successor bounds precisely this table.
        *end.last_mut().expect("record prefix") += 1;
        let mut transaction = self
            .opener
            .begin(MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
            .map_err(|error| error.to_string())?;
        let rows = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
            scan_system_table(&mut snapshot, &view)
                .map_err(|error| error.to_string())?
                .into_iter()
                .map(|(key, value)| {
                    let row =
                        SystemRow::parse(&view, &key, &value).map_err(|error| error.to_string())?;
                    COLUMNS
                        .iter()
                        .map(|name| {
                            row.datum(name)
                                .map(|value| value.cloned().unwrap_or(tidb_datatype::Datum::Null))
                                .map_err(|error| error.to_string())
                        })
                        .collect::<Result<Vec<_>, String>>()
                })
                .collect::<Result<Vec<_>, String>>()?
        };
        transaction
            .finish_without_writes()
            .map_err(|error| error.to_string())?;
        let quota = self
            .globals
            .get("tidb_mem_quota_binding_cache")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| self.cache.load().mem_capacity());
        let cache = BindingCache::from_storage_rows(rows, quota);
        *self
            .record_range
            .write()
            .unwrap_or_else(|error| error.into_inner()) = Some((start.into(), end.into()));
        self.cache.publish(cache);
        Ok(())
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
        cache: SharedBindingCache::default(),
        refresh: Mutex::new(()),
        record_range: RwLock::new(None),
    });
    bindings.reload()?;
    let (stop, stopped) = mpsc::channel();
    let target = Arc::clone(&bindings);
    let worker = std::thread::Builder::new()
        .name("tidb-binding-reload".to_owned())
        .spawn(move || loop {
            match stopped.recv_timeout(BINDING_LEASE) {
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(error) = target.reload() {
                        eprintln!("binding cache reload failed: {error}");
                    }
                }
                _ => break,
            }
        })
        .map_err(|error| error.to_string())?;
    Ok((
        bindings,
        BindingReloader {
            stop: Some(stop),
            worker: Some(worker),
        },
    ))
}
