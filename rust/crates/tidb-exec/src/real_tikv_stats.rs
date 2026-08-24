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
use tidb_txnkv::transaction::{StorePdCapability, StoreWriteClient, StoreWriteLoader};

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
pub fn load_table_stats_from_cluster<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
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
pub fn load_stats_snapshot_from_cluster<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    tables: &[(i64, BTreeMap<i64, FieldType>)],
) -> Result<StatsSnapshot, SystemTableError> {
    load_stats_snapshot_and_loader(opener, timeout, tables).map(|(snapshot, _)| snapshot)
}

/// [`Self::load_stats_snapshot_from_cluster`], also handing back the located
/// `mysql.stats_*` views so a caller can reuse them for the cheap per-tick
/// version probe without re-reading the catalog.
pub fn load_stats_snapshot_and_loader<
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    tables: &[(i64, BTreeMap<i64, FieldType>)],
) -> Result<(StatsSnapshot, ClusterStatsLoader), SystemTableError> {
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
        Ok((result, loader))
    };
    transaction
        .finish_without_writes()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    loaded
}

/// The per-tick version probe Go's `Handle.Update` runs before any reload
/// (`pkg/statistics/handle/update.go`): ONE scan of `mysql.stats_meta`, and
/// each tracked table's `version`, `None` for a table with no row (never
/// analyzed). Everything else -- histograms, buckets, top-n, the catalog
/// itself -- stays untouched unless some version moved.
pub fn load_stats_meta_versions<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    timeout: Duration,
    loader: &ClusterStatsLoader,
    table_ids: &[i64],
) -> Result<BTreeMap<i64, Option<u64>>, SystemTableError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    let versions = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let all = loader.load_all_meta(&mut snapshot)?;
        table_ids
            .iter()
            .map(|table_id| (*table_id, all.get(table_id).map(|(version, _, _)| *version)))
            .collect()
    };
    transaction
        .finish_without_writes()
        .map_err(|error| SystemTableError::Snapshot(error.to_string()))?;
    Ok(versions)
}

/// Whether the published snapshot already reflects exactly these versions:
/// same tracked set, and every table's state version (absent = pseudo/never
/// analyzed) equal to what `mysql.stats_meta` reports now. This is Go
/// `Handle.Update`'s publish gate (`pkg/statistics/handle/update.go`: a table
/// whose meta version equals its cached one is skipped), lifted to whole-
/// snapshot granularity so an unchanged cluster costs one scan and nothing
/// else.
pub fn stats_snapshot_unchanged_since(
    current: &StatsSnapshot,
    versions: &BTreeMap<i64, Option<u64>>,
    targets: &[(i64, BTreeMap<i64, FieldType>)],
) -> bool {
    if current.len() != targets.len() {
        return false;
    }
    for (table_id, _) in targets {
        let version = versions.get(table_id).copied().flatten();
        match current.get(table_id) {
            Some(state) if state.version() == version => {}
            _ => return false,
        }
    }
    true
}

#[cfg(test)]
mod unchanged_tests {
    use super::*;
    use crate::cluster_stats_load::ClusterTableStats;
    use crate::stats_watch::TableStatsState;

    fn loaded(table_id: i64, version: u64) -> TableStatsState {
        TableStatsState::Loaded(std::sync::Arc::new(ClusterTableStats {
            table_id,
            version,
            modify_count: 0,
            row_count: 0,
            columns: Vec::new(),
            indexes: Vec::new(),
            load_state: std::sync::Arc::default(),
        }))
    }

    fn targets(ids: &[i64]) -> Vec<(i64, BTreeMap<i64, FieldType>)> {
        ids.iter().map(|id| (*id, BTreeMap::new())).collect()
    }

    #[test]
    fn equal_versions_and_set_mean_unchanged() {
        let current = StatsSnapshot::from([(1, loaded(1, 5)), (2, TableStatsState::Pseudo)]);
        let mut versions = BTreeMap::new();
        versions.insert(1, Some(5));
        versions.insert(2, None);
        assert!(stats_snapshot_unchanged_since(
            &current,
            &versions,
            &targets(&[1, 2])
        ));
    }

    #[test]
    fn a_moved_or_new_or_dropped_table_counts_as_changed() {
        let moved = StatsSnapshot::from([(1, loaded(1, 5))]);
        let mut versions = BTreeMap::new();
        versions.insert(1, Some(9));
        assert!(!stats_snapshot_unchanged_since(
            &moved,
            &versions,
            &targets(&[1])
        ));
        // A target the published snapshot has never seen: a newly analyzed
        // table must fall through to the full load.
        let empty = StatsSnapshot::new();
        let mut fresh = BTreeMap::new();
        fresh.insert(5, Some(3));
        assert!(!stats_snapshot_unchanged_since(
            &empty,
            &fresh,
            &targets(&[5])
        ));
    }
}
