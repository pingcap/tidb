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

//! The wide-SQL session over cluster storage: the catalog comes from the
//! cluster loader, and every table it names reads and writes through a real
//! transaction instead of an in-process map.
//!
//! This is the join of the two halves the rewrite built separately. The
//! session driver (`tidb-session` over `tidb-executor`) owns SQL: parsing,
//! planning, expressions, joins, ordering, aggregation. The cluster loader and
//! the transaction coordinator own storage: schema at one snapshot, reads at
//! one `start_ts`, writes as one 2PC publication. Nothing in the driver knows
//! which side it is on -- it calls the row-level table API, which is why the
//! swap is one constructor argument.
//!
//! # What a table must be to run here
//!
//! Every loaded table is admitted whose *storage layout* this tier encodes and
//! decodes: a non-partitioned base table (not a view, not a sequence) whose
//! columns are public. A view, a sequence, a partitioned table, or a table
//! still mid-DDL is refused by name, and the refusal is kept and reported
//! rather than making the table silently disappear -- the same choice
//! [`configure_loaded_table`] makes for the bounded read path.
//!
//! [`configure_loaded_table`]: tidb_exec::cluster_catalog::configure_loaded_table

use std::sync::{Arc, Mutex};
use tidb_exec::cluster_catalog::ClusterCatalog;
use tidb_exec::cluster_stats_load::{ClusterStatsItem, ClusterTableStats};
use tidb_exec::stats_watch::{StatsSnapshot, TableStatsState};
use tidb_executor::access_cost::TableStatistics;
use tidb_executor::cluster_storage::ClusterTableStorage;
use tidb_executor::driver::Catalog;
use tidb_executor::kv_table::{KvColumn, KvIndex, KvTable};
use tidb_executor::storage::TableStorage;
use tidb_model::{SchemaState, TableInfo};
use tidb_planner::cardinality::row_count_estimator::{ColumnStats, IndexStats};
use tidb_session::{Session, SharedCatalog};

/// Go `mysql.PriKeyFlag`: what marks the column `PKIsHandle` points at.
const PRI_KEY_FLAG: u32 = 1 << 1;

/// Why one loaded table is not part of the session's catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkippedTable {
    /// Fully qualified stored name, `schema.table`.
    pub name: String,
    /// Exact, self-contained explanation.
    pub reason: String,
}

/// One session's catalog over cluster storage, plus every table left out.
#[derive(Debug)]
pub struct ClusterSessionCatalog {
    /// The driver's catalog: databases and tables backed by `storage`.
    pub catalog: Catalog,
    /// Tables the cluster has that this tier cannot serve.
    pub skipped: Vec<SkippedTable>,
}

/// Builds a driver catalog whose every table reads and writes through
/// `storage`.
///
/// `storage` is cloned per table, and cloning a [`ClusterTableStorage`] clones
/// *handles*: all of them share the statement's snapshot and the session's
/// staged writes.
#[must_use]
pub fn cluster_session_catalog(
    loaded: &ClusterCatalog,
    storage: &ClusterTableStorage,
    stats: &StatsSnapshot,
) -> ClusterSessionCatalog {
    let mut catalog = Catalog::default();
    let mut skipped = Vec::new();
    for database in &loaded.databases {
        let schema = database.info.name.original().to_owned();
        catalog.create_database(&schema);
        for table in &database.tables {
            match cluster_table(table, storage) {
                Ok(kv_table) => {
                    // A table the cluster reports as never analyzed
                    // (`TableStatsState::Pseudo`) or one this node has not
                    // loaded yet is left OUT of the map, which is exactly what
                    // makes the planner treat it as `statistics.PseudoTable`.
                    if let Some(loaded_stats) =
                        stats.get(&table.id).and_then(TableStatsState::loaded)
                    {
                        catalog.set_table_statistics(
                            table.id,
                            Arc::new(planner_statistics(loaded_stats, table)),
                        );
                    }
                    catalog.register_kv_in(&schema, table.name.original(), kv_table);
                }
                Err(reason) => skipped.push(SkippedTable {
                    name: format!("{schema}.{}", table.name.original()),
                    reason,
                }),
            }
        }
    }
    ClusterSessionCatalog { catalog, skipped }
}

/// Opens a session on a catalog loaded from the cluster.
///
/// The catalog is this session's own: a cluster-backed table is bound to one
/// statement's snapshot, so it cannot be shared with peers the way the
/// in-process catalog is.
#[must_use]
pub fn session_with_cluster_storage(
    loaded: &ClusterCatalog,
    storage: &ClusterTableStorage,
    stats: &StatsSnapshot,
) -> (Session, Vec<SkippedTable>) {
    let ClusterSessionCatalog { catalog, skipped } =
        cluster_session_catalog(loaded, storage, stats);
    let shared: SharedCatalog = Arc::new(Mutex::new(catalog));
    (Session::with_catalog(shared), skipped)
}

/// Translates one stored `TableInfo` into a table over cluster storage.
fn cluster_table(table: &TableInfo, storage: &ClusterTableStorage) -> Result<KvTable, String> {
    if table.is_view() {
        return Err("it is a view".to_owned());
    }
    if table.is_sequence() {
        return Err("it is a sequence".to_owned());
    }
    if table.partition.is_some() {
        return Err("it is partitioned".to_owned());
    }
    if table.state != SchemaState::PUBLIC {
        return Err(format!(
            "its schema state is {} rather than public",
            table.state.0
        ));
    }
    let columns: Vec<&tidb_model::column::ColumnInfo> = table.cols();
    if columns.is_empty() {
        return Err("it has no public columns".to_owned());
    }
    // The row layout is keyed by column id, so a column's *offset* is only a
    // position in the tuple the driver builds; both must come from the same
    // public-column list, which is why the offsets below are indexes into it.
    let kv_columns: Vec<KvColumn> = columns
        .iter()
        .map(|column| KvColumn {
            name: column.name.original().to_owned(),
            id: column.id,
            field_type: column.field_type.clone(),
            // Stored DEFAULTs are Go `any` values that need the source's
            // `GetColDefaultValue` conversion; until that path is shared, a
            // cluster table takes no default rather than a guessed one, and an
            // INSERT that omits such a column is refused instead of writing a
            // wrong value.
            default_value: None,
            origin_default: None,
        })
        .collect();
    let mut kv_table = KvTable::with_storage(table.id, kv_columns, storage.clone_box());
    kv_table.set_name(table.name.original());
    if table.pk_is_handle {
        let handle = columns
            .iter()
            .position(|column| column.field_type.flags() & PRI_KEY_FLAG != 0)
            .ok_or_else(|| {
                "it is marked PKIsHandle but has no public primary key column".to_owned()
            })?;
        kv_table.set_pk_handle_offset(handle);
    } else if table.is_common_handle {
        let handles = clustered_handle_offsets(table, &columns)?;
        kv_table.set_common_handle_offsets(handles);
    }
    for index in &table.indices {
        if index.state != SchemaState::PUBLIC {
            continue;
        }
        let mut offsets = Vec::with_capacity(index.columns.len());
        for column in &index.columns {
            let offset = columns
                .iter()
                .position(|public| public.name.lowercase() == column.name.lowercase())
                .ok_or_else(|| {
                    format!(
                        "its index {} covers non-public column {}",
                        index.name.original(),
                        column.name.original()
                    )
                })?;
            offsets.push(offset);
        }
        kv_table.add_index(KvIndex {
            id: index.id,
            name: index.name.original().to_owned(),
            unique: index.unique,
            column_offsets: offsets,
        });
    }
    Ok(kv_table)
}

/// Go `mysql.MaxUnsignedFlag` (`UnsignedFlag`), which the out-of-range
/// scaling in the estimator reads off a column.
const UNSIGNED_FLAG: u32 = 1 << 5;

/// Translates one table's loaded `mysql.stats_*` rows into the shape the
/// planner's estimator reads.
///
/// This is the ONE place the storage form and the estimation form meet: the
/// loader ([`tidb_exec::cluster_stats_load`]) owns how a histogram is stored,
/// [`tidb_planner::cardinality`] owns how it is read, and neither has to know
/// the other. A histogram whose `hist_id` names no current column or index --
/// a dropped one whose stats rows have not been GC'd -- is skipped, because
/// the estimator keys on the live schema.
fn planner_statistics(stats: &ClusterTableStats, table: &TableInfo) -> TableStatistics {
    let mut columns = std::collections::BTreeMap::new();
    for column in table.cols() {
        let Some(item) = stats.column(column.id).filter(stats_available) else {
            continue;
        };
        columns.insert(
            column.id,
            ColumnStats {
                histogram: item.histogram.clone(),
                topn: item.topn.clone(),
                cms: item.cms.clone(),
                stats_ver: item.stats_ver,
                unsigned: column.field_type.flags() & UNSIGNED_FLAG != 0,
            },
        );
    }
    let mut indexes = std::collections::BTreeMap::new();
    for index in &table.indices {
        let Some(item) = stats.index(index.id).filter(stats_available) else {
            continue;
        };
        indexes.insert(
            index.id,
            index_statistics(item, index.columns.len(), index.unique),
        );
    }
    TableStatistics {
        // Go `Table.IsInitialized()`: a table whose every histogram is
        // uninitialized is `HistColl.Pseudo`, even though the `stats_meta`
        // row that made this function run gives it a real row count.
        pseudo: columns.is_empty() && indexes.is_empty(),
        row_count: i64::try_from(stats.row_count).unwrap_or(i64::MAX),
        modify_count: stats.modify_count,
        columns,
        indexes,
    }
}

/// Go `Column.StatsAvailable()` / `IsColumnAnalyzedOrSynthesized`: whether
/// this histogram was actually collected.
///
/// A `stats_histograms` row can exist with `stats_ver = 0` -- an ADD COLUMN
/// synthesizes one from the default value -- so the version alone is not the
/// test; Go also accepts a non-zero NDV or null count, which is that
/// synthesized case.
fn stats_available(item: &&ClusterStatsItem) -> bool {
    item.stats_ver > 0 || item.histogram.ndv > 0 || item.histogram.null_count > 0
}

/// One index histogram, with the two schema facts the estimator needs that
/// the stored row does not carry.
fn index_statistics(item: &ClusterStatsItem, num_columns: usize, unique: bool) -> IndexStats {
    IndexStats {
        histogram: item.histogram.clone(),
        topn: item.topn.clone(),
        cms: item.cms.clone(),
        stats_ver: item.stats_ver,
        num_columns,
        unique,
    }
}

/// The public-column offsets of a clustered composite handle, in key order.
fn clustered_handle_offsets(
    table: &TableInfo,
    columns: &[&tidb_model::column::ColumnInfo],
) -> Result<Vec<usize>, String> {
    let primary = table
        .indices
        .iter()
        .find(|index| index.primary)
        .ok_or_else(|| "it is marked clustered but has no PRIMARY KEY index".to_owned())?;
    let mut offsets = Vec::with_capacity(primary.columns.len());
    for column in &primary.columns {
        let offset = columns
            .iter()
            .position(|public| public.name.lowercase() == column.name.lowercase())
            .ok_or_else(|| {
                format!(
                    "its clustered PRIMARY KEY covers non-public column {}",
                    column.name.original()
                )
            })?;
        offsets.push(offset);
    }
    Ok(offsets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_executor::cluster_storage::{ClusterSnapshot, MutationBuffer, SnapshotPairs};
    use tidb_executor::storage::StorageError;
    use tidb_model::column::ColumnInfo;
    use tidb_model::db::DBInfo;
    use tidb_session::StmtResult;
    use tidb_txnkv::Key;

    /// A snapshot with fixed contents: the cluster as this test's statements
    /// see it. Nothing a statement writes may appear in here, because a staged
    /// write is not a committed one.
    #[derive(Debug, Default)]
    struct FixedSnapshot {
        data: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    impl ClusterSnapshot for FixedSnapshot {
        fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.data.get(key.as_bytes()).cloned())
        }

        fn scan(&mut self, start: &Key, end: &Key) -> Result<SnapshotPairs, StorageError> {
            Ok(self
                .data
                .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
    }

    fn column(id: i64, offset: i32, name: &str, primary: bool) -> ColumnInfo {
        let mut field_type = FieldType::new(FieldTypeCode::LongLong);
        if primary {
            field_type.add_flags(PRI_KEY_FLAG);
        }
        let mut column = ColumnInfo::new(id, name, field_type);
        column.offset = offset;
        column
    }

    /// One database `app` holding `t(id BIGINT PRIMARY KEY, v BIGINT)`, plus a
    /// table still mid-DDL that the session must refuse by name.
    fn loaded_catalog() -> ClusterCatalog {
        let base = TableInfo {
            id: 101,
            name: CiString::new("t"),
            columns: vec![column(1, 0, "id", true), column(2, 1, "v", false)],
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let pending = TableInfo {
            id: 102,
            name: CiString::new("t_pending"),
            columns: vec![column(1, 0, "id", false)],
            state: SchemaState::NONE,
            ..TableInfo::default()
        };
        ClusterCatalog {
            schema_version: 7,
            databases: vec![tidb_exec::cluster_catalog::LoadedDatabase {
                info: DBInfo {
                    id: 5,
                    name: CiString::new("app"),
                    ..DBInfo::default()
                },
                tables: vec![base, pending],
            }],
        }
    }

    fn cluster_storage() -> (
        ClusterTableStorage,
        MutationBuffer,
        Arc<Mutex<FixedSnapshot>>,
    ) {
        let snapshot = Arc::new(Mutex::new(FixedSnapshot::default()));
        let buffer = MutationBuffer::new();
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&snapshot) as _;
        (
            ClusterTableStorage::new(buffer.clone(), handle),
            buffer,
            snapshot,
        )
    }

    #[test]
    fn wide_sql_runs_on_cluster_storage_without_committing() {
        let (storage, buffer, snapshot) = cluster_storage();
        let (mut session, skipped) =
            session_with_cluster_storage(&loaded_catalog(), &storage, &StatsSnapshot::new());
        // The unserved table is named in the refusal rather than silently
        // vanishing from the session's catalog.
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].name, "app.t_pending");
        assert_eq!(
            skipped[0].reason,
            "its schema state is 0 rather than public"
        );

        session.run("USE app").unwrap();
        assert!(matches!(
            session
                .run("INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
                .unwrap(),
            StmtResult::Affected(3)
        ));
        // The rows are staged, not committed: the cluster snapshot is
        // untouched and the session's buffer holds the bytes.
        assert!(snapshot.lock().unwrap().data.is_empty());
        assert_eq!(buffer.len(), 3);

        // The whole read path -- scan, WHERE, an expression over columns,
        // ORDER BY -- runs over the buffer-in-front-of-snapshot storage.
        let StmtResult::Rows(rows) = session
            .run("SELECT id, v + id * 2 FROM t WHERE v > 10 ORDER BY id DESC")
            .unwrap()
        else {
            panic!("expected rows");
        };
        let values: Vec<String> = rows
            .iter()
            .map(|row| format!("{:?}|{:?}", row[0], row[1]))
            .collect();
        assert_eq!(
            values,
            vec!["Int(3)|Int(36)".to_owned(), "Int(2)|Int(24)".to_owned()]
        );

        // A staged DELETE hides a staged row, and the cluster still sees none
        // of it.
        assert!(matches!(
            session.run("DELETE FROM t WHERE id = 2").unwrap(),
            StmtResult::Affected(1)
        ));
        let StmtResult::Rows(rows) = session.run("SELECT id FROM t ORDER BY id").unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows.len(), 2);
        assert!(snapshot.lock().unwrap().data.is_empty());
    }

    #[test]
    fn a_snapshot_row_is_visible_and_shadowed_by_a_staged_write() {
        let (storage, _, snapshot) = cluster_storage();
        let (mut session, _) =
            session_with_cluster_storage(&loaded_catalog(), &storage, &StatsSnapshot::new());
        session.run("USE app").unwrap();
        // Write one row through the driver, promote its staged bytes into the
        // snapshot, and start over: that is what a COMMIT leaves behind for the
        // next statement to read.
        session.run("INSERT INTO t (id, v) VALUES (9, 90)").unwrap();
        let staged = storage.buffer().staged();
        assert_eq!(staged.len(), 1);
        for (key, value) in staged {
            snapshot
                .lock()
                .unwrap()
                .data
                .insert(key.into_bytes(), value.expect("a row write, not a delete"));
        }
        storage.buffer().reset();

        let StmtResult::Rows(rows) = session.run("SELECT id, v FROM t").unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(format!("{rows:?}"), "[[Int(9), Int(90)]]");

        // The same row updated in this statement reads back the staged value,
        // while the committed bytes underneath stay as they were.
        session.run("UPDATE t SET v = 91 WHERE id = 9").unwrap();
        let StmtResult::Rows(rows) = session.run("SELECT v FROM t").unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(format!("{rows:?}"), "[[Int(91)]]");
        assert_eq!(snapshot.lock().unwrap().data.len(), 1);
    }
}
