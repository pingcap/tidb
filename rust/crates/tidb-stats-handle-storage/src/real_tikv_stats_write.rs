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

//! The statistics writes the stats handle runs as their own pessimistic
//! transactions over real TiKV: Go `statsReadWriter.UpdateStatsMetaVersionForGC`,
//! `storage.UpdateStatsVersion` and `storage.ChangeGlobalStatsID`.

use std::time::Duration;

use tidb_store_driver_txn::transaction::{RealOptimisticTransactionOpener, StoreClient};

use tidb_infoschema::cluster_catalog::load_cluster_catalog;
use crate::cluster_stats_write::{plan_change_global_stats_id, plan_stats_meta_version_refresh, plan_update_stats_version};
use tidb_store_driver_txn::cluster_table_storage::{
    commit_pessimistic_statement, PessimisticStatementTransactionError,
};
use tidb_infoschema_issyncer::real_tikv_catalog::SnapshotMetaSnapshot;

/// Pinned Go `statsReadWriter.UpdateStatsMetaVersionForGC` over real TiKV.
///
/// The metadata refresh is the returned transaction result. Historical-meta
/// recording runs afterward in its own best-effort transaction and therefore
/// cannot turn a successful refresh into an error. `record_history` is Go's
/// `RecordHistoricalStatsMeta` through the stats handle, present when the
/// historical-stats/cache-initialization gate is open.
pub fn update_stats_meta_version_for_gc<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    physical_id: i64,
    timeout: Duration,
    record_history: Option<&dyn Fn(i64, u64, &str)>,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<u64, PessimisticStatementTransactionError> {
    let version = commit_pessimistic_statement(opener, timeout, |snapshot, version| {
        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
        let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
        let plan =
            plan_stats_meta_version_refresh(&mut snapshot, &catalog, physical_id, version, row_ids)
                .map_err(|error| error.to_string())?;
        Ok((version, plan.mutations))
    })?;
    if let Some(record_history) = record_history {
        record_history(physical_id, version, "schema change");
    }
    Ok(version)
}

/// Pinned Go `storage.UpdateStatsVersion` over real TiKV.
pub fn update_stats_version<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<u64, PessimisticStatementTransactionError> {
    commit_pessimistic_statement(opener, timeout, |snapshot, version| {
        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
        let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
        let plan = plan_update_stats_version(&mut snapshot, &catalog, version, row_ids)
            .map_err(|error| error.to_string())?;
        Ok((version, plan.mutations))
    })
}

/// Pinned Go `storage.ChangeGlobalStatsID` over real TiKV.
pub fn change_global_stats_id<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    from: i64,
    to: i64,
    timeout: Duration,
) -> Result<(), PessimisticStatementTransactionError> {
    commit_pessimistic_statement(opener, timeout, |snapshot, _version| {
        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
        let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
        let plan = plan_change_global_stats_id(&mut snapshot, &catalog, from, to)
            .map_err(|error| error.to_string())?;
        Ok(((), plan.mutations))
    })
}

