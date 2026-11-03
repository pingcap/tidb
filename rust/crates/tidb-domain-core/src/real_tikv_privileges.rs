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

//! The `mysql.tidb` reads a node makes before its domain exists, each
//! through one fresh read-only transaction at one PD timestamp: whether Go's
//! bootstrap already ran, and the bootstrap-persisted system time zone (Go
//! `initGlobalVarFromSystemDB`). Every other `mysql.*` table is read by the
//! domain's own sessions once they exist.

use std::time::Duration;

use tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener;
use tidb_store_driver_txn::transaction::StoreClient;

use tidb_infoschema::cluster_catalog::load_info_schema;
use tidb_privilege::cluster_privilege_load::{read_bootstrap_state, read_system_tz, ClusterBootstrapState};
use tidb_syssession::mysql_system_tables::SystemTableError;
use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;

/// Reads `mysql.tidb`'s bootstrap flag through one fresh read-only
/// transaction.
///
/// The transaction is finished without writes, so the read leaves no locks
/// behind and consumes exactly one PD timestamp -- the same contract
/// [`tidb_infoschema_issyncer::real_tikv_catalog::load_catalog_from_cluster`] keeps.
pub fn read_bootstrap_state_from_cluster<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
) -> Result<ClusterBootstrapState, SystemTableError> {
    read_mysql_tidb(opener, timeout, |snapshot, catalog| read_bootstrap_state(snapshot, catalog))
}

/// Go `initGlobalVarFromSystemDB`'s `mysql.tidb.system_tz`.
pub fn load_system_tz_from_cluster<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
) -> Result<String, SystemTableError> {
    read_mysql_tidb(opener, timeout, |snapshot, catalog| read_system_tz(snapshot, catalog))
}

fn read_mysql_tidb<S: StoreClient, T>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
    read: impl FnOnce(
        &mut TransactionMetaSnapshot<'_>,
        &tidb_infoschema::SessionExtendedInfoSchema,
    ) -> Result<T, SystemTableError>,
) -> Result<T, SystemTableError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    let value = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_info_schema(&mut snapshot)?;
        read(&mut snapshot, &catalog)?
    };
    transaction
        .finish_without_writes()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    Ok(value)
}
