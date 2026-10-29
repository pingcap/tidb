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

//! Go `history_stats.go`: the historical-stats planners and
//! `RecordHistoricalStatsMeta` as its own pessimistic transaction.


use tidb_datatype::{
    Datum, Time,
};
use tidb_stats::JsonTable;
use tidb_txnkv::transaction::BufferMutation;

use tidb_infoschema::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use tidb_syssession::system_row_write::defaults_row;
use tidb_syssession::stats_table_rows::*;
use std::sync::Arc;
use std::time::Duration;

use tidb_kv::cluster_storage::MutationBuffer;
use tidb_txnkv::transaction::{RealOptimisticTransactionOpener, StoreClient};

use tidb_infoschema::cluster_catalog::load_cluster_catalog;
use tidb_store_driver_txn::cluster_table_storage::{
    lock_pessimistic_statement, PessimisticStatementTransactionError,
    SessionTransaction,
};
use tidb_datatype::current_time::local_now_datetime6;
use tidb_infoschema_issyncer::real_tikv_catalog::SnapshotMetaSnapshot;
use tidb_stats_handle_storage::cluster_stats_write::*;

/// Plans pinned Go `RecordHistoricalStatsMeta`'s `SELECT ... FOR UPDATE`.
pub fn plan_historical_stats_meta_lock<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    version: u64,
) -> Result<((i64, i64), StatsWritePlan), StatsWriteError> {
    let meta = locate(catalog, "stats_meta")?;
    let meta_rows = StatsRows::open(snapshot, meta, &["table_id"], table_id)?;
    let identity = vec![format!("{:?}", Datum::Int(table_id))];
    let (key, values) = meta_rows.existing.get(&identity).ok_or_else(|| {
        StatsWriteError::HistoricalMeta("no historical meta stats can be recorded".to_owned())
    })?;
    let stored_version = unsigned_value(values.get(&column_id(meta, "version")?));
    if stored_version != Some(version) {
        return Err(StatsWriteError::HistoricalMeta(
            "no historical meta stats can be recorded".to_owned(),
        ));
    }
    let modify_count =
        signed_value(values.get(&column_id(meta, "modify_count")?)).ok_or_else(|| {
            StatsWriteError::HistoricalMeta("invalid stats_meta.modify_count".to_owned())
        })?;
    let count = signed_value(values.get(&column_id(meta, "count")?))
        .ok_or_else(|| StatsWriteError::HistoricalMeta("invalid stats_meta.count".to_owned()))?;
    let mut plan = StatsWritePlan::default();
    plan.mutations.push(
        BufferMutation::lock_only(key.clone())
            .map_err(|error| StatsWriteError::HistoricalMeta(error.to_string()))?,
    );
    Ok(((modify_count, count), plan))
}

/// Plans pinned Go `RecordHistoricalStatsMeta`'s `REPLACE` statement.
pub fn plan_historical_stats_meta_replace<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    modify_count: i64,
    count: i64,
    version: u64,
    source: &str,
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    let history = locate(catalog, "stats_meta_history")?;
    let mut rows = StatsRows::open(snapshot, history, &["table_id", "version"], table_id)?;
    let mut history_values = defaults_row(history, now)?;
    set(
        history,
        &mut history_values,
        "table_id",
        Datum::Int(table_id),
    );
    set(
        history,
        &mut history_values,
        "modify_count",
        Datum::Int(modify_count),
    );
    set(history, &mut history_values, "count", Datum::Int(count));
    set(
        history,
        &mut history_values,
        "version",
        Datum::UInt(version),
    );
    set(
        history,
        &mut history_values,
        "source",
        Datum::Bytes(source.as_bytes().to_vec()),
    );
    set(
        history,
        &mut history_values,
        "create_time",
        Datum::Time(now),
    );
    let mut plan = StatsWritePlan::default();
    rows.store_row(catalog, &history_values, &mut plan, row_ids)?;
    Ok(plan)
}

/// Pinned Go `history.RecordHistoricalStatsToStorage`'s preparation before its
/// per-block `INSERT` statements.
pub fn historical_stats_data_blocks(
    json: &JsonTable,
) -> Result<(u64, Vec<Vec<u8>>), StatsWriteError> {
    const MAX_COLUMN_SIZE: usize = 5 << 20;
    let version = match json.partitions.as_ref() {
        Some(partitions) if !partitions.is_empty() => partitions
            .values()
            .map(|partition| {
                partition
                    .as_ref()
                    .expect("nil partition in historical statistics JSON")
                    .version
            })
            .max()
            .unwrap_or(0),
        _ => json.version,
    };
    let blocks = tidb_stats_handle_storage::json::json_table_to_blocks(json, MAX_COLUMN_SIZE)
        .map_err(|error| StatsWriteError::HistoricalData(error.to_string()))?;
    Ok((version, blocks))
}

/// Plans one pinned Go `history.RecordHistoricalStatsToStorage` block
/// `INSERT ... ON DUPLICATE KEY UPDATE` statement.
pub fn plan_historical_stats_data_block<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    physical_id: i64,
    version: u64,
    sequence: usize,
    block: &[u8],
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    let history = locate(catalog, "stats_history")?;
    let mut rows = StatsRows::open(
        snapshot,
        history,
        &["table_id", "version", "seq_no"],
        physical_id,
    )?;
    let mut plan = StatsWritePlan::default();
    let mut values = defaults_row(history, now)?;
    set(history, &mut values, "table_id", Datum::Int(physical_id));
    set(
        history,
        &mut values,
        "stats_data",
        Datum::Bytes(block.to_vec()),
    );
    set(history, &mut values, "seq_no", Datum::Int(sequence as i64));
    set(history, &mut values, "version", Datum::UInt(version));
    set(history, &mut values, "create_time", Datum::Time(now));
    rows.store_row(catalog, &values, &mut plan, row_ids)?;
    Ok(plan)
}

/// Pinned Go `RecordHistoricalStatsMeta` as its own pessimistic transaction.
pub fn record_historical_stats_meta<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    table_id: i64,
    version: u64,
    source: &str,
    timeout: Duration,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<(), PessimisticStatementTransactionError> {
    let transaction = SessionTransaction::begin_pessimistic(
        Arc::new(opener.clone()),
        timeout,
        opener.commit_protocol(),
    )
    .map_err(|error| PessimisticStatementTransactionError::Build(error.to_string()))?;
    let staged = MutationBuffer::new();
    let (modify_count, count) =
        lock_pessimistic_statement(&transaction, &staged, |snapshot, _start_ts| {
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
            let (counts, plan) =
                plan_historical_stats_meta_lock(&mut snapshot, &catalog, table_id, version)
                    .map_err(|error| error.to_string())?;
            Ok((counts, plan.mutations))
        })?;
    lock_pessimistic_statement(&transaction, &staged, |snapshot, _start_ts| {
        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
        let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
        let plan = plan_historical_stats_meta_replace(
            &mut snapshot,
            &catalog,
            table_id,
            modify_count,
            count,
            version,
            source,
            local_now_datetime6(),
            row_ids,
        )
        .map_err(|error| error.to_string())?;
        Ok(((), plan.mutations))
    })?;
    transaction
        .commit(&staged)
        .map(|_| ())
        .map_err(PessimisticStatementTransactionError::Transaction)
}
