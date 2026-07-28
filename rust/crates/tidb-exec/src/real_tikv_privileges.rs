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

use crate::cluster_catalog::load_cluster_catalog;
use crate::cluster_privilege_load::{
    load_cluster_privileges, read_bootstrap_state, ClusterBootstrapState, ClusterPrivileges,
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

/// Reads the bootstrap flag and every `mysql.*` account through one fresh
/// read-only transaction.
///
/// The transaction is finished without writes, so the load leaves no locks
/// behind and consumes exactly one PD timestamp — the same contract
/// [`crate::real_tikv_catalog::load_catalog_from_cluster`] keeps.
pub fn load_accounts_from_cluster(
    opener: &RealOptimisticTransactionOpener,
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
