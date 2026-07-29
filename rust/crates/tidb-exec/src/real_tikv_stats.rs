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

//! The production statistics load: one real transaction, one PD timestamp, one
//! consistent view of a table's `mysql.stats_*` rows.
//!
//! `mysql.stats_meta`, `mysql.stats_histograms`, `mysql.stats_buckets`, and
//! `mysql.stats_top_n` are four separate tables that one `ANALYZE` writes in
//! one transaction. Reading them at four timestamps could therefore assemble a
//! histogram whose buckets came from one `ANALYZE` and whose TopN came from
//! the next — a combination that never existed and whose row counts would not
//! add up. Everything below is read at a single `start_ts`.

use std::collections::BTreeMap;
use std::time::Duration;

use tidb_datatype::FieldType;
use tidb_txnkv::transaction::RealOptimisticTransactionOpener;

use crate::cluster_catalog::load_cluster_catalog;
use crate::cluster_stats_load::{ClusterStatsLoader, ClusterTableStats};
use crate::mysql_system_tables::SystemTableError;
use crate::real_tikv_catalog::TransactionMetaSnapshot;
use crate::stats_watch::{StatsSnapshot, TableStatsState};

/// Reads one table's statistics through one fresh read-only transaction.
///
/// The catalog is loaded at that same timestamp, so the column types the
/// bounds are decoded at are the types the table had when the rows were read.
///
/// `Ok(None)` means the table has no `mysql.stats_meta` row: never analyzed.
pub fn load_table_stats_from_cluster(
    opener: &RealOptimisticTransactionOpener,
    timeout: Duration,
    schema: &str,
    table: &str,
) -> Result<Option<ClusterTableStats>, SystemTableError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    let loaded = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot)?;
        let info = catalog
            .databases
            .iter()
            .find(|database| database.info.name.lowercase() == schema.to_lowercase())
            .and_then(|database| {
                database
                    .tables
                    .iter()
                    .find(|stored| stored.name.lowercase() == table.to_lowercase())
            })
            .ok_or_else(|| SystemTableError::Missing {
                name: format!("{schema}.{table}"),
            })?;
        let column_types: BTreeMap<i64, FieldType> =
            crate::cluster_stats_load::column_types_of(info);
        let table_id = info.id;
        let loader = ClusterStatsLoader::locate(&catalog)?;
        loader.load_table(&mut snapshot, table_id, &column_types)
    };
    transaction
        .finish_without_writes()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    loaded
}

/// Reads every one of `tables`' statistics through one fresh read-only
/// transaction: one `start_ts` for every table's `mysql.stats_*` rows, not
/// just one table's, which is what makes the result a single consistent
/// [`StatsSnapshot`] a node can publish whole into
/// [`crate::stats_watch::SharedStats`].
///
/// `tables` pairs each physical table ID with the column-type map its bounds
/// decode against ([`crate::cluster_stats_load::column_types_of`]); a caller
/// with an already-loaded [`crate::cluster_catalog::ClusterCatalog`] builds
/// this from the `TableInfo`s it is booting the node over.
///
/// A table with no `mysql.stats_meta` row becomes
/// [`TableStatsState::Pseudo`] rather than being omitted: the node must know
/// it *asked* and the cluster said "never analyzed", which is a different
/// fact from never having asked at all (see the [`crate::stats_watch`]
/// module doc).
pub fn load_stats_snapshot_from_cluster(
    opener: &RealOptimisticTransactionOpener,
    timeout: Duration,
    tables: &[(i64, BTreeMap<i64, FieldType>)],
) -> Result<StatsSnapshot, SystemTableError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    let loaded = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot)?;
        let loader = ClusterStatsLoader::locate(&catalog)?;
        let mut result = StatsSnapshot::new();
        for (table_id, column_types) in tables {
            let state = match loader.load_table(&mut snapshot, *table_id, column_types)? {
                Some(stats) => TableStatsState::Loaded(std::sync::Arc::new(stats)),
                None => TableStatsState::Pseudo,
            };
            result.insert(*table_id, state);
        }
        Ok(result)
    };
    transaction
        .finish_without_writes()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    loaded
}
