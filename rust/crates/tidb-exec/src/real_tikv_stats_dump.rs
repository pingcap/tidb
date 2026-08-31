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

//! Pinned Go statistics dump and BR persistence transaction boundaries.

use std::time::Duration;

use tidb_model::table_info::TableInfo;
use tidb_stats::JsonTable;
use tidb_txnkv::transaction::{
    RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient, StoreWriteLoader,
};

use crate::cluster_catalog::load_cluster_catalog;
use crate::cluster_stats_dump::{
    load_table_stats_payload, persist_stats_by_snapshot_impl, table_stats_to_json_from_loaded,
    PersistStatsBySnapshotError, StatsDumpError,
};
use crate::mysql_system_tables::SystemTableError;
use crate::real_tikv_catalog::TransactionMetaSnapshot;

/// Pinned Go `statsReadWriter.TableStatsToJSON` over real TiKV.
///
/// Go loads the histogram payload in one restricted session and refreshes
/// metadata/usage in another. A nonzero snapshot pins both reads to that
/// timestamp; zero lets each restricted transaction read its own current
/// timestamp.
pub fn table_stats_to_json_from_cluster<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    database_name: &str,
    table_info: &TableInfo,
    physical_id: i64,
    snapshot: u64,
) -> Result<Option<JsonTable>, StatsDumpError> {
    let mut payload_transaction = if snapshot == 0 {
        opener.begin_read_only()
    } else {
        opener.begin_read_only_at(snapshot)
    }
    .map_err(snapshot_error)?;
    let table = {
        let mut storage = TransactionMetaSnapshot::new(&mut payload_transaction, timeout);
        let catalog = load_cluster_catalog(&mut storage).map_err(snapshot_error)?;
        load_table_stats_payload(&mut storage, &catalog, table_info, physical_id)?
    };
    payload_transaction
        .finish_without_writes()
        .map_err(snapshot_error)?;
    let Some(table) = table else {
        return Ok(None);
    };

    let mut metadata_transaction = if snapshot == 0 {
        opener.begin_read_only()
    } else {
        opener.begin_read_only_at(snapshot)
    }
    .map_err(snapshot_error)?;
    let json = {
        let mut storage = TransactionMetaSnapshot::new(&mut metadata_transaction, timeout);
        let catalog = load_cluster_catalog(&mut storage).map_err(snapshot_error)?;
        table_stats_to_json_from_loaded(
            &mut storage,
            &catalog,
            database_name,
            table_info,
            physical_id,
            table,
        )?
    };
    metadata_transaction
        .finish_without_writes()
        .map_err(snapshot_error)?;
    Ok(json)
}

/// Pinned Go `statsReadWriter.PersistStatsBySnapshot` over real TiKV.
///
/// A nonpartitioned table invokes `persist` once even when no statistics are
/// present. A partitioned table skips absent partition/global statistics and
/// visits definitions in schema order followed by global statistics. The
/// first dump or callback error stops iteration.
pub fn persist_stats_by_snapshot<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
    E,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    database_name: &str,
    table_info: &TableInfo,
    snapshot: u64,
    persist: impl FnMut(Option<JsonTable>, i64) -> Result<(), E>,
) -> Result<(), PersistStatsBySnapshotError<E>> {
    let Some(partition) = table_info.get_partition_info() else {
        return persist_stats_by_snapshot_impl(
            std::slice::from_ref(&table_info.id),
            true,
            |physical_id| {
                table_stats_to_json_from_cluster(
                    opener,
                    timeout,
                    database_name,
                    table_info,
                    physical_id,
                    snapshot,
                )
            },
            persist,
        );
    };
    let mut physical_ids = partition
        .read()
        .definitions
        .snapshot()
        .into_iter()
        .map(|definition| definition.id)
        .collect::<Vec<_>>();
    physical_ids.push(table_info.id);
    persist_stats_by_snapshot_impl(
        &physical_ids,
        false,
        |physical_id| {
            table_stats_to_json_from_cluster(
                opener,
                timeout,
                database_name,
                table_info,
                physical_id,
                snapshot,
            )
        },
        persist,
    )
}

fn snapshot_error(error: impl ToString) -> StatsDumpError {
    StatsDumpError::Storage(SystemTableError::Snapshot(error.to_string()))
}
