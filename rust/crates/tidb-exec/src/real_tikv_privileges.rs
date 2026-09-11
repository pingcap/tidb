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

//! The production `mysql.*` load: one real transaction, one PD timestamp, one
//! consistent view of the cluster's accounts.
//!
//! The catalog, the bootstrap flag, and every privilege row are read at the
//! same `start_ts`, so a node can never adopt a set of grants that never
//! coexisted — including the case that matters most: a `CREATE USER` a Go node
//! commits while this load is running is either wholly visible or wholly
//! absent.

use std::time::Duration;

use tidb_txnkv::transaction::RealOptimisticTransactionOpener;
use tidb_txnkv::transaction::{StorePdCapability, StoreWriteClient, StoreWriteLoader};

use crate::cluster_catalog::{load_cluster_catalog, ClusterCatalog, MetaSnapshot};
use crate::cluster_privilege_load::{
    load_cluster_privileges, read_bootstrap_state, read_system_tz, ClusterBootstrapState,
    ClusterPrivileges,
};
use crate::cluster_sysvar_load::load_cluster_sysvars;
use crate::mysql_system_tables::SystemTableError;
use crate::real_tikv_catalog::TransactionMetaSnapshot;

/// One snapshot's whole answer about who may connect to this cluster.
#[derive(Clone, Debug)]
pub struct ClusterAccounts {
    /// Whether Go's bootstrap already ran against this keyspace.
    pub bootstrap: ClusterBootstrapState,
    /// Every account and grant `mysql.*` held at that snapshot.
    pub privileges: ClusterPrivileges,
    /// Every `SET GLOBAL` override `mysql.global_variables` held at that same
    /// snapshot, as `(name, value)` pairs ready for
    /// `tidb_session::GlobalSysvars::load_from_cluster`.
    pub sysvars: Vec<(String, String)>,
}

/// The process-global values that must be installed from one startup
/// snapshot before the first session is admitted.
#[derive(Clone, Debug)]
pub struct ClusterStartupVariables {
    /// Go `mysql.tidb.system_tz`.
    pub system_tz: String,
    /// Persisted `SET GLOBAL` overrides.
    pub sysvars: Vec<(String, String)>,
}

/// Reads the bootstrap flag and every `mysql.*` account through one fresh
/// read-only transaction.
///
/// The transaction is finished without writes, so the load leaves no locks
/// behind and consumes exactly one PD timestamp — the same contract
/// [`crate::real_tikv_catalog::load_catalog_from_cluster`] keeps.
pub fn load_accounts_from_cluster<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
) -> Result<ClusterAccounts, SystemTableError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    let accounts = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot)?;
        let bootstrap = read_bootstrap_state(&mut snapshot, &catalog)?;
        // A keyspace no TiDB ever bootstrapped has no `mysql.user` to read,
        // and reading one anyway would report a missing table as if it were a
        // corrupt one. The empty account set is the honest answer, and its
        // caller already has `bootstrap` to tell the two apart.
        let (privileges, sysvars) = if matches!(bootstrap, ClusterBootstrapState::NotBootstrapped) {
            (ClusterPrivileges::default(), Vec::new())
        } else {
            (
                load_cluster_privileges(&mut snapshot, &catalog)?,
                load_cluster_sysvars(&mut snapshot, &catalog)?,
            )
        };
        ClusterAccounts {
            bootstrap,
            privileges,
            sysvars,
        }
    };
    transaction
        .finish_without_writes()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    Ok(accounts)
}

/// Reads the bootstrap marker and, only for a bootstrapped keyspace,
/// `mysql.global_variables` through one fresh read-only transaction.
///
/// Skip-grant-table disables TiDB's privilege-cache load, but Go still
/// rebuilds its global-sysvar cache synchronously before accepting sessions.
/// Keeping this separate from [`load_accounts_from_cluster`] lets recovery
/// mode preserve that ordering without consulting any account or grant row.
/// An unbootstrapped or partially bootstrapped keyspace returns no overrides.
pub fn load_sysvars_from_cluster<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
) -> Result<Vec<(String, String)>, SystemTableError> {
    load_sysvars_from_cluster_with_catalog_cache(opener, timeout, None)
}

/// The catalog a periodic reader keeps between passes, reused while the
/// cluster schema version it was loaded at is still current. Go reads
/// `mysql.global_variables` through the domain's cached infoschema, which
/// is only replaced when the schema version moves; loading the whole
/// catalog (every table's JSON) on every reload pass was this port's cost.
pub type CatalogCache = std::sync::Mutex<Option<std::sync::Arc<ClusterCatalog>>>;

/// [`load_sysvars_from_cluster`] with an optional caller-owned catalog cache.
pub fn load_sysvars_from_cluster_with_catalog_cache<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    catalog_cache: Option<&CatalogCache>,
) -> Result<Vec<(String, String)>, SystemTableError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    let sysvars = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        load_sysvars_from_snapshot_with_catalog_cache(&mut snapshot, catalog_cache)?
    };
    transaction
        .finish_without_writes()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    Ok(sysvars)
}

/// Reads the bootstrap-persisted system time zone and global variables from
/// one consistent read-only snapshot.
pub fn load_startup_variables_from_cluster<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
) -> Result<ClusterStartupVariables, SystemTableError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    let variables = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot)?;
        ClusterStartupVariables {
            system_tz: read_system_tz(&mut snapshot, &catalog)?,
            sysvars: load_cluster_sysvars(&mut snapshot, &catalog)?,
        }
    };
    transaction
        .finish_without_writes()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    Ok(variables)
}

#[cfg(test)]
fn load_sysvars_from_snapshot<S: MetaSnapshot>(
    snapshot: &mut S,
) -> Result<Vec<(String, String)>, SystemTableError> {
    load_sysvars_from_snapshot_with_catalog_cache(snapshot, None)
}

fn load_sysvars_from_snapshot_with_catalog_cache<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog_cache: Option<&CatalogCache>,
) -> Result<Vec<(String, String)>, SystemTableError> {
    let catalog = match catalog_cache {
        Some(cache) => {
            // Any DDL moves the schema version, so a catalog loaded at the
            // version this snapshot reads is this snapshot's catalog.
            let version = crate::cluster_catalog::read_schema_version(snapshot)?;
            let cached = lock_catalog_cache(cache)
                .as_ref()
                .filter(|catalog| version != 0 && catalog.schema_version == version)
                .cloned();
            match cached {
                Some(catalog) => catalog,
                None => {
                    let catalog = std::sync::Arc::new(load_cluster_catalog(snapshot)?);
                    *lock_catalog_cache(cache) = Some(std::sync::Arc::clone(&catalog));
                    catalog
                }
            }
        }
        None => std::sync::Arc::new(load_cluster_catalog(snapshot)?),
    };
    let bootstrap = read_bootstrap_state(snapshot, &catalog)?;
    if !bootstrap.already_bootstrapped() {
        return Ok(Vec::new());
    }
    load_cluster_sysvars(snapshot, &catalog)
}

fn lock_catalog_cache(
    cache: &CatalogCache,
) -> std::sync::MutexGuard<'_, Option<std::sync::Arc<ClusterCatalog>>> {
    cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod sysvar_only_tests {
    use crate::cluster_catalog::{ClusterCatalogError, MetaPairs};

    use super::*;

    /// An unbootstrapped keyspace has only absent metadata. Any table-record
    /// prefix would mean the sysvar-only startup path tried to consult a
    /// `mysql.*` row after already proving bootstrap never completed.
    struct UnbootstrappedSnapshot;

    impl MetaSnapshot for UnbootstrappedSnapshot {
        fn get(&mut self, _key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
            Ok(None)
        }

        fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
            assert_ne!(
                prefix.first(),
                Some(&b't'),
                "an unbootstrapped sysvar load must not scan a table record range"
            );
            Ok(Vec::new())
        }
    }

    #[test]
    fn an_unbootstrapped_keyspace_returns_no_sysvars_without_reading_table_rows() {
        let loaded = load_sysvars_from_snapshot(&mut UnbootstrappedSnapshot)
            .expect("an unbootstrapped keyspace is an empty sysvar source");
        assert!(loaded.is_empty());
    }
}
