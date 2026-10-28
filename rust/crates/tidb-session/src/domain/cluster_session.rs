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
//! Every loaded base table is admitted whose *storage layout* this tier
//! encodes and decodes: a non-partitioned table whose columns are public.
//! Views are metadata-only objects and are registered from their stored
//! `TableInfo.View` definition; a sequence, a partitioned table, or a table
//! still mid-DDL is refused by name, and the refusal is kept and reported
//! rather than making the table silently disappear -- the same choice
//! [`configure_loaded_table`] makes for the bounded read path.
//!
//! [`configure_loaded_table`]: tidb_cluster::cluster_catalog::configure_loaded_table

use std::sync::{Arc, Mutex};
use tidb_cluster::cluster_catalog::ClusterCatalog;
#[cfg(test)]
use tidb_exec::cluster_stats_load::ClusterStatsItem;
use tidb_exec::stats_watch::{StatsSnapshot, TableStatsState};
use tidb_executor::access_cost::TableStatistics;
use tidb_executor::cluster_storage::ClusterTableStorage;
use tidb_executor::driver::{Catalog, ViewDef};
use tidb_executor::kv_table::KvTable;
use tidb_executor::storage::TableStorage;
#[cfg(test)]
use tidb_model::GoShared;
use tidb_model::{SchemaState, TableInfo};
use crate::{Session, SharedCatalog};

/// Go `mysql.PriKeyFlag`: what marks the column `PKIsHandle` points at.
#[cfg(test)]
const PRI_KEY_FLAG: u32 = 1 << 1;

// Go `tables.TableFromMeta` and the auto-id seam moved beside the DDL
// planner; the server keeps reaching them by their old paths.
#[cfg(test)]
pub use tidb_cluster::table_from_meta::kv_index;
pub use tidb_cluster::table_from_meta::{cluster_table, AutoIdKind};
pub use tidb_cluster::table_from_meta::{AutoIdSource, LocalTableAutoIds, TableAutoIds};

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
    /// The domain statistics the sessions over this catalog plan from (Go's
    /// `GetStatsHandle()`), when the caller supplied a view.
    pub statistics: Option<Arc<tidb_executor::driver::StatisticsView>>,
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
    auto_ids: &dyn TableAutoIds,
) -> ClusterSessionCatalog {
    cluster_session_catalog_with_templates(
        loaded,
        storage,
        Some(snapshot_statistics_view(stats)),
        auto_ids,
        storage,
        None,
    )
}

/// A statistics view over one fixed snapshot, for a catalog built outside a
/// running node (boot, tests): the planner reads it exactly as it reads the
/// node's live statistics.
#[must_use]
pub fn snapshot_statistics_view(
    stats: &StatsSnapshot,
) -> Arc<tidb_executor::driver::StatisticsView> {
    Arc::new(tidb_executor::driver::StatisticsView::new(Arc::new(
        SnapshotStatisticsSource(stats.clone()),
    )))
}

struct SnapshotStatisticsSource(StatsSnapshot);

impl tidb_executor::driver::StatisticsSource for SnapshotStatisticsSource {
    fn table(&self, physical_id: i64) -> Option<Arc<tidb_stats::Table>> {
        self.0
            .get(&physical_id)
            .and_then(TableStatsState::loaded)
            .cloned()
    }
}

/// One schema version's worth of fully built [`KvTable`]s, reused by every
/// session opened against it.
///
/// Building a loaded table means materializing every column's owned name,
/// default and collation strings plus its index entries -- measured at ~30MB
/// retained across the hzbank schema's ~700 tables, PER CONNECTION, which is
/// what ran a busy node into its container memory ceiling. Go builds one
/// `table.Table` per `TableInfo` inside the shared `infoschema` and sessions
/// hold pointers; this cache is that single build. The cached tables are
/// never executed: a session CLONES its table (columns/indexes are already
/// `Arc`-shared) and swaps in its own storage seam through
/// [`KvTable::replace_storage`], so staging stays session-private exactly as
/// before.
///
/// The set belongs to ONE schema version; [`KvTableTemplates::reuse`] drops
/// it whenever the reloader publishes a different one.
#[derive(Default)]
pub struct KvTableTemplates {
    schema_version: i64,
    tables: std::collections::HashMap<(String, String), KvTable>,
}

impl KvTableTemplates {
    /// Drops the cache unless `loaded` is the schema version it was built
    /// from. Call once per catalog build, before any lookup.
    pub fn reuse(&mut self, loaded: &ClusterCatalog) {
        if self.schema_version != loaded.schema_version {
            self.tables.clear();
            self.schema_version = loaded.schema_version;
        }
    }

    fn get(&self, schema: &str, name: &str) -> Option<&KvTable> {
        self.tables.get(&(schema.to_owned(), name.to_owned()))
    }

    fn insert(&mut self, schema: &str, name: &str, table: KvTable) {
        self.tables
            .insert((schema.to_owned(), name.to_owned()), table);
    }
}

#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn cluster_session_catalog_with_templates(
    loaded: &ClusterCatalog,
    storage: &ClusterTableStorage,
    // Go's planner reads the domain's statistics handle
    // (`GetPhysicalTableStats`), never a copy in the schema: the catalog plans
    // through this view of it.
    statistics: Option<Arc<tidb_executor::driver::StatisticsView>>,
    auto_ids: &dyn TableAutoIds,
    // Neutral storage over which CACHED-MISS tables are built for the
    // template set; sessions rebind their own seam afterwards either way.
    template_storage: &ClusterTableStorage,
    mut kv_templates: Option<&mut KvTableTemplates>,
) -> ClusterSessionCatalog {
    let mut catalog = Catalog::new();
    let mut skipped = Vec::new();
    // Go `infoschema`'s resource groups come from meta, so a group another
    // TiDB created is one this node's `SET RESOURCE GROUP` finds.
    for group in &loaded.resource_groups {
        catalog.set_resource_group(group.clone());
    }
    // Go `infoschema`'s policy map, from meta: a table's, partition's or
    // schema's placement reference resolves against it.
    for policy in &loaded.policies {
        catalog.set_policy(policy.clone());
    }
    for database in &loaded.databases {
        let schema = database.info.name.original().to_owned();
        // Go `DBInfo.Charset`/`Collate`: what `SHOW CREATE DATABASE` and
        // `information_schema.schemata` report, and the default a table
        // created without its own charset inherits. An unrecognised stored
        // name falls back to the server default rather than refusing the
        // whole database.
        let charset = tidb_executor::TableCharset {
            charset: tidb_datatype::Charset::from_name(&database.info.charset)
                .unwrap_or(tidb_executor::TableCharset::default().charset),
            collation: tidb_datatype::Collation::from_name(&database.info.collate)
                .unwrap_or(tidb_executor::TableCharset::default().collation),
        };
        catalog.register_database_with_id_and_charset(&schema, database.info.id, charset);
        // Go `DBInfo.PlacementPolicyRef`: the schema default a table created
        // without its own takes, and what SHOW CREATE DATABASE prints.
        if let Some(reference) = &database.info.placement_policy_ref {
            catalog.set_database_placement_policy(&schema, Some(reference.read().clone()));
        }
        for table in &database.tables {
            if table.is_view() {
                let name = table.name.original();
                match cluster_view(table) {
                    Ok(view) => catalog
                        .register_view_in(&schema, name, view)
                        .expect("the schema was created just above this loop"),
                    Err(reason) => skipped.push(SkippedTable {
                        name: format!("{schema}.{name}"),
                        reason,
                    }),
                }
                continue;
            }
            if table.is_sequence() {
                // Go loads every PUBLIC sequence into the infoschema
                // (`infoschema.loadSequence`); SHOW TABLES lists it and
                // NEXTVAL/LASTVAL/SETVAL resolve against it. Skipping it here
                // is what made `SELECT nextval(...)` answer 1146 on this tier.
                match auto_ids.sequence_allocator_for(database.info.id, table) {
                    Some(sequence) => catalog
                        .register_sequence_in(&schema, table.name.original(), sequence)
                        .expect("the schema was created just above this loop"),
                    None => skipped.push(SkippedTable {
                        name: format!("{schema}.{}", table.name.original()),
                        reason: "it is a sequence and this tier has no sequence counter".to_owned(),
                    }),
                }
                continue;
            }
            let auto = AutoIdSource::In {
                db_id: database.info.id,
                ids: auto_ids,
            };
            // A cached template clone carries neutral storage and valid
            // process-wide allocator handles; only the store needs rebinding.
            let built = match kv_templates
                .as_ref()
                .and_then(|c| c.get(&schema, table.name.original()))
            {
                Some(template) => Ok(template.clone()),
                None => {
                    let fresh = cluster_table(table, template_storage, &auto);
                    if let (Ok(fresh), Some(cache)) = (&fresh, kv_templates.as_deref_mut()) {
                        cache.insert(&schema, table.name.original(), fresh.clone());
                    }
                    fresh
                }
            };
            match built {
                Ok(mut kv_table) => {
                    kv_table.replace_storage(storage.clone_box());
                    catalog
                        .register_kv_in(&schema, table.name.original(), kv_table)
                        .expect("the schema was created just above this loop");
                }
                Err(reason) => {
                    // A table a DDL job holds in an intermediate state (DROP's
                    // write-only / delete-only, CREATE's delete-only before
                    // its foreign keys go public) is not a port gap: Go loads
                    // it too and refuses every statement on it with
                    // `ErrTableNotExists` (`tryLockMDLAndUpdateSchemaIfNecessary`).
                    if table.state == tidb_model::SchemaState::PUBLIC {
                        report_unbuildable_table(&schema, table, &reason);
                    }
                    skipped.push(SkippedTable {
                        name: format!("{schema}.{}", table.name.original()),
                        reason,
                    });
                }
            }
        }
    }
    ClusterSessionCatalog { catalog, skipped, statistics }
}

/// A stored table this node cannot build is a gap in the port, not a state
/// Go has: Go loads every stored table, so here the table silently does not
/// exist for the session ("DDL succeeded but the table is invisible"). Each
/// table version is reported once per process, as a structured warning the
/// production replay gate fails on.
fn report_unbuildable_table(schema: &str, table: &TableInfo, reason: &str) {
    static REPORTED: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<(i64, u64)>>> =
        std::sync::LazyLock::new(Default::default);
    let first = REPORTED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert((table.id, table.update_ts));
    if first {
        // Not log content: Go builds every stored table, so it has no such
        // line. The corpus replay gate reads this JSON event from stderr.
        eprintln!(
            "{{\"level\":\"warning\",\"event\":\"table_skipped\",\"table\":{:?},\"reason\":{:?}}}",
            format!("{schema}.{}", table.name.original()),
            reason
        );
    }
}

/// Translates Go's persisted `TableInfo.View` into the driver's metadata-only
/// view entry. The body is parsed and planned when the view is read, as Go's
/// `BuildDataSourceFromView` does, so an invalidated definition remains in the
/// infoschema and reports a view error instead of disappearing at load time.
fn cluster_view(table: &TableInfo) -> Result<ViewDef, String> {
    if table.state != SchemaState::PUBLIC {
        return Err(format!(
            "its schema state is {} rather than public",
            table.state.0
        ));
    }
    let view = table
        .view
        .as_ref()
        .ok_or_else(|| "its view metadata is missing".to_owned())?
        .read();
    let columns = table
        .present_cols()
        .into_iter()
        .map(|column| {
            let column = column.read();
            (column.name.original().to_owned(), column.field_type.clone())
        })
        .collect();
    let (definer_user, definer_host) = view.definer.as_deref().map_or_else(
        || (String::new(), String::new()),
        |definer| (definer.username.clone(), definer.hostname.clone()),
    );
    Ok(ViewDef {
        name: table.name.original().to_owned(),
        columns,
        select_sql: view.select_stmt.clone(),
        definer_user,
        definer_host,
        character_set_client: table.charset.clone(),
        collation_connection: table.collate.clone(),
        algorithm: view.algorithm.sql().to_owned(),
        security: view.security.sql().to_owned(),
        check_option: view.check_option.sql().to_owned(),
        update_ts: table.update_ts,
        id: table.id,
    })
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
    auto_ids: &dyn TableAutoIds,
) -> (Session, Vec<SkippedTable>) {
    let ClusterSessionCatalog {
        catalog,
        skipped,
        statistics,
    } = cluster_session_catalog(loaded, storage, stats, auto_ids);
    let shared = SharedCatalog::new(catalog);
    let mut session = Session::over_catalog(shared);
    if let Some(view) = statistics {
        session.stats_handle().set_statistics_view(view);
    }
    (session, skipped)
}

/// Go `mysql.MaxUnsignedFlag` (`UnsignedFlag`), which the out-of-range
/// scaling in the estimator reads off a column.
const UNSIGNED_FLAG: u32 = 1 << 5;

/// Adapts Go `TableInfo` metadata to the common canonical-table planner view.
pub fn planner_statistics(stats: &tidb_stats::Table, table: &TableInfo) -> TableStatistics {
    let columns = table
        .cols()
        .iter_deref()
        .map(|column| {
            let column = column.read();
            (column.id, column.field_type.flags() & UNSIGNED_FLAG != 0)
        })
        .collect::<Vec<_>>();
    let indexes = table
        .indices
        .iter_deref()
        .map(|index| {
            let index = index.read();
            (index.id, index.columns.len(), index.unique)
        })
        .collect::<Vec<_>>();
    tidb_executor::load_stats::table_statistics_from_table_schema(stats, &columns, &indexes)
}

#[cfg(test)]
mod tests {
    use tidb_executor::KvTableDdl;
use super::*;

    /// The store half of a transaction for a session these tests drive over
    /// storage they own: the test holds the buffer, so committing or rolling
    /// it back is the test's business, not a store session's.
    struct TestOwnedStore;

    impl crate::TxnManagerOps for TestOwnedStore {
        fn commit_txn(&mut self, _session: &mut Session) -> Result<(), tidb_executor::DriverError> {
            Ok(())
        }

        fn rollback_txn(
            &mut self,
            _session: &mut Session,
        ) -> Result<(), tidb_executor::DriverError> {
            Ok(())
        }

        fn enter_new_txn_with_begin(
            &mut self,
            _session: &mut Session,
        ) -> Result<(), tidb_executor::DriverError> {
            Ok(())
        }

        fn savepoint_checkpoint(
            &mut self,
            _session: &mut Session,
        ) -> Result<
            Option<tidb_executor::cluster_storage::BufferCheckpoint>,
            tidb_executor::DriverError,
        > {
            Ok(None)
        }

        fn rollback_to_checkpoint(
            &mut self,
            _checkpoint: &tidb_executor::cluster_storage::BufferCheckpoint,
        ) {
        }
    }

    /// `BEGIN` on a session over test-owned storage.
    fn begin(session: &mut Session) {
        let stmt = tidb_parser::parse("BEGIN").unwrap();
        session
            .control_transaction_stmt(&stmt, &mut TestOwnedStore)
            .unwrap()
            .expect("a transaction statement");
    }
    use std::collections::BTreeMap;
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_exec::cluster_stats_load::ClusterTableStats;
    use tidb_executor::cluster_storage::{ClusterSnapshot, MutationBuffer, SnapshotPairs};
    use tidb_executor::storage::StorageError;
    use tidb_model::column::{ColumnDefaultValue, ColumnInfo};
    use tidb_model::db::DBInfo;
    use tidb_model::index::{IndexColumn, IndexInfo};
    use tidb_model::table::ViewInfo;
    use tidb_model::GoAny;
    use crate::StmtResult;
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

        fn scan(
            &mut self,
            start: &Key,
            end: &Key,
            limit: Option<usize>,
        ) -> Result<SnapshotPairs, StorageError> {
            Ok(self
                .data
                .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
                .take(limit.unwrap_or(usize::MAX))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
    }

    fn column(id: i64, offset: i64, name: &str, primary: bool) -> ColumnInfo {
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
            columns: vec![column(1, 0, "id", true), column(2, 1, "v", false)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let pending = TableInfo {
            id: 102,
            name: CiString::new("t_pending"),
            columns: vec![column(1, 0, "id", false)].into(),
            state: SchemaState::NONE,
            ..TableInfo::default()
        };
        ClusterCatalog {
            resource_groups: Vec::new(),
            policies: Vec::new(),
            schema_version: 7,
            databases: vec![tidb_cluster::cluster_catalog::LoadedDatabase {
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
    fn cluster_views_are_registered_from_go_table_info() {
        let base = TableInfo {
            id: 101,
            name: CiString::new("t"),
            columns: vec![column(1, 0, "id", true), column(2, 1, "v", false)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let view = TableInfo {
            id: 102,
            name: CiString::new("revenue0"),
            charset: "utf8mb4".to_owned(),
            collate: "utf8mb4_bin".to_owned(),
            columns: vec![
                column(1, 0, "supplier_no", false),
                column(2, 1, "total_revenue", false),
            ]
            .into(),
            state: SchemaState::PUBLIC,
            view: Some(GoShared::new(ViewInfo {
                select_stmt: "SELECT `app`.`t`.`id` AS `supplier_no`, \
                    SUM(`app`.`t`.`v`) AS `total_revenue` FROM `app`.`t` \
                    GROUP BY `app`.`t`.`id`"
                    .to_owned(),
                ..ViewInfo::default()
            })),
            ..TableInfo::default()
        };
        let loaded = ClusterCatalog {
            resource_groups: Vec::new(),
            policies: Vec::new(),
            schema_version: 7,
            databases: vec![tidb_cluster::cluster_catalog::LoadedDatabase {
                info: DBInfo {
                    id: 5,
                    name: CiString::new("app"),
                    ..DBInfo::default()
                },
                tables: vec![base, view],
            }],
        };
        let (storage, _, _) = cluster_storage();
        let (mut session, skipped) = session_with_cluster_storage(
            &loaded,
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        assert!(
            skipped.is_empty(),
            "cluster view must not be skipped: {skipped:?}"
        );
        session.run("USE app").unwrap();
        session
            .run(
                "EXPLAIN FORMAT='brief' SELECT supplier_no, total_revenue FROM revenue0 \
                 WHERE total_revenue = (SELECT MAX(total_revenue) FROM revenue0)",
            )
            .expect("the stored view body is planned through the cluster catalog");
    }

    #[test]
    fn loaded_column_ndv_reaches_grouped_cluster_plans() {
        struct FixedStorageStatistics {
            calls: Arc<std::sync::atomic::AtomicUsize>,
            column_length_requests: Arc<std::sync::Mutex<Vec<bool>>>,
            fail: Arc<std::sync::atomic::AtomicBool>,
            values: tidb_executor::table_size_stats::TableRowStatsSnapshot,
        }

        impl crate::TableStorageStatsProvider for FixedStorageStatistics {
            fn load_table_row_stats(
                &self,
                _resource_group: &str,
                need_column_lengths: bool,
                _physical_ids: &[i64],
            ) -> Result<tidb_executor::table_size_stats::TableRowStatsSnapshot, String>
            {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                self.column_length_requests
                    .lock()
                    .unwrap()
                    .push(need_column_lengths);
                if self.fail.load(std::sync::atomic::Ordering::Acquire) {
                    return Err("stats read failed".to_owned());
                }
                Ok(self.values.clone())
            }
        }

        let table = TableInfo {
            id: 130,
            name: CiString::new("order_line"),
            columns: vec![
                column(1, 0, "ol_o_id", true),
                column(2, 1, "ol_d_id", false),
                column(3, 2, "ol_amount", false),
            ]
            .into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let version = 42;
        let item = |id, ndv| {
            let mut item = ClusterStatsItem {
                id,
                is_index: false,
                stats_ver: 2,
                flag: 1,
                load_status: tidb_stats::StatsLoadedStatus::full_load(),
                histogram: Default::default(),
                topn: None,
                cms: None,
                fm_sketch: None,
            };
            item.histogram.id = id;
            item.histogram.ndv = ndv;
            item.histogram.last_update_version = version;
            item.histogram.append_bucket(
                tidb_datatype::Datum::Int(1),
                tidb_datatype::Datum::Int(3_000_065),
                3_000_065,
                1,
            );
            item
        };
        let loaded_stats = ClusterTableStats {
            table_id: table.id,
            version,
            snapshot: version,
            last_analyze_version: version,
            last_stats_hist_version: version,
            modify_count: 0,
            row_count: 3_000_065,
            columns: vec![item(1, 3_000_065), item(2, 10), item(3, 3_000_065)],
            indexes: Vec::new(),
        };
        let loaded_stats = loaded_stats.to_statistics_table(&table);
        let table_id = table.id;
        let translated = planner_statistics(loaded_stats.as_ref(), &table);
        assert!(!translated.pseudo);
        assert_eq!(
            translated.columns.keys().copied().collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let snapshot = StatsSnapshot::from([(table.id, TableStatsState::Loaded(loaded_stats))]);
        let (storage, _, _) = cluster_storage();
        let (mut session, skipped) = session_with_cluster_storage(
            &one_table_catalog(table),
            &storage,
            &snapshot,
            &LocalTableAutoIds::default(),
        );
        let storage_stats_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let column_length_requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let fail_storage_stats = Arc::new(std::sync::atomic::AtomicBool::new(false));
        session.set_table_storage_stats_provider(Arc::new(FixedStorageStatistics {
            calls: Arc::clone(&storage_stats_reads),
            column_length_requests: Arc::clone(&column_length_requests),
            fail: Arc::clone(&fail_storage_stats),
            // Three fixed-width BIGINT columns: 3,000,065 rows of 24 bytes
            // estimate a DATA_LENGTH of 72,001,560.
            values: tidb_executor::table_size_stats::TableRowStatsSnapshot {
                table_rows: [(table_id, 3_000_065)].into_iter().collect(),
                column_lengths: Default::default(),
            },
        }));
        assert!(skipped.is_empty(), "{skipped:?}");
        {
            let shared = session.shared_catalog();
            let catalog = shared.lock().unwrap();
            let statistics = catalog
                .table_statistics_in(&shared.stats_handle(), 130)
                .expect("loaded statistics survive session construction");
            assert!(!statistics.pseudo);
            assert_eq!(statistics.columns.get(&2).unwrap().histogram.ndv, 10);
        }
        session.run("USE app").unwrap();
        session
            .run(
                "SELECT TABLE_NAME FROM information_schema.tables \
                 WHERE TABLE_SCHEMA = 'app' AND TABLE_NAME = 'order_line'",
            )
            .unwrap();
        assert_eq!(
            storage_stats_reads.load(std::sync::atomic::Ordering::Acquire),
            0,
            "Go skips the two restricted reads when no size column is requested"
        );
        assert!(column_length_requests.lock().unwrap().is_empty());
        let StmtResult::Rows(rows) = session
            .run(
                "SELECT TABLE_ROWS, AVG_ROW_LENGTH, DATA_LENGTH, INDEX_LENGTH \
                 FROM information_schema.tables \
                 WHERE TABLE_SCHEMA = 'app' AND TABLE_NAME = 'order_line'",
            )
            .unwrap()
        else {
            panic!("expected information_schema rows");
        };
        assert_eq!(
            rows,
            vec![vec![
                Datum::Int(3_000_065),
                Datum::Int(24),
                Datum::Int(72_001_560),
                Datum::Int(0),
            ]]
        );
        assert_eq!(
            storage_stats_reads.load(std::sync::atomic::Ordering::Acquire),
            1
        );
        assert_eq!(*column_length_requests.lock().unwrap(), vec![true]);
        {
            let catalog = session.shared_catalog();
            let catalog = catalog.lock().unwrap();
            let tidb_executor::TableEntry::Kv(table) =
                catalog.table_in("app", "order_line").expect("table")
            else {
                panic!("order_line is a base table")
            };
            assert_eq!(
                table.storage_statistics(),
                (0, 0, 0, 0),
                "statement-local TableSizeStats must not mutate the shared catalog"
            );
        }
        let StmtResult::Rows(rows) = session
            .run(
                "WITH s AS (SELECT TABLE_ROWS FROM information_schema.tables \
                 WHERE TABLE_SCHEMA = 'app' AND TABLE_NAME = 'order_line') \
                 SELECT TABLE_ROWS FROM s",
            )
            .unwrap()
        else {
            panic!("CTE statistics query must return rows")
        };
        assert_eq!(rows, vec![vec![Datum::Int(3_000_065)]]);
        assert_eq!(
            storage_stats_reads.load(std::sync::atomic::Ordering::Acquire),
            2,
            "the resolved memory-table scan inside a CTE must refresh"
        );
        assert_eq!(
            *column_length_requests.lock().unwrap(),
            vec![true, false],
            "TABLE_ROWS alone reads stats_meta without scanning stats_histograms"
        );
        session
            .run(
                "SELECT TABLE_NAME FROM information_schema.partitions \
                 WHERE TABLE_SCHEMA = 'app' AND TABLE_NAME = 'order_line'",
            )
            .unwrap();
        assert_eq!(
            storage_stats_reads.load(std::sync::atomic::Ordering::Acquire),
            3,
            "Go's LogicalMemTable.PruneColumns does not list PARTITIONS, so \
             the scan keeps its size columns and updateStatsCacheIfNeed \
             (infoschema_reader.go:646-661) refreshes even for a \
             TABLE_NAME-only projection"
        );
        fail_storage_stats.store(true, std::sync::atomic::Ordering::Release);
        let StmtResult::Rows(rows) = session
            .run(
                "SELECT TABLE_ROWS FROM information_schema.tables \
                 WHERE TABLE_SCHEMA = 'app' AND TABLE_NAME = 'order_line'",
            )
            .unwrap()
        else {
            panic!("failed statistics read remains a successful information-schema query")
        };
        assert_eq!(
            rows,
            vec![vec![Datum::Int(0)]],
            "a seam provider that refuses its read is served with zero size \
             columns; Go's own provider path warns and serves its cache instead"
        );
        let StmtResult::Rows(rows) = session
            .run(
                "EXPLAIN FORMAT='brief' SELECT ol_d_id, SUM(ol_amount) \
                 FROM order_line WHERE ol_o_id BETWEEN 1 AND 1775 GROUP BY ol_d_id",
            )
            .unwrap()
        else {
            panic!("expected EXPLAIN rows");
        };
        let text = |row: usize, column: usize| match &rows[row][column] {
            tidb_datatype::Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            other => format!("{other:?}"),
        };
        let grouped = rows
            .iter()
            .enumerate()
            .find(|(row, _)| text(*row, 0).contains("HashAgg"))
            .map(|(row, _)| row)
            .expect("the GROUP BY has a physical aggregation");
        assert_eq!(text(grouped, 1), "1.00", "{rows:#?}");
    }

    #[test]
    fn wide_sql_runs_on_cluster_storage_without_committing() {
        let (storage, buffer, snapshot) = cluster_storage();
        let (mut session, skipped) = session_with_cluster_storage(
            &loaded_catalog(),
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        // The unserved table is named in the refusal rather than silently
        // vanishing from the session's catalog.
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].name, "app.t_pending");
        assert_eq!(
            skipped[0].reason,
            "its schema state is 0 rather than public"
        );

        session.run("USE app").unwrap();

        // The statements share one uncommitted buffer, which Go reads only

        // inside the transaction that wrote it (through a union scan).

        begin(&mut session);
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

    /// One database `app` holding `d(id BIGINT PRIMARY KEY, v BIGINT DEFAULT
    /// 7)`, as the loader receives it from the cluster: the DEFAULT lives on
    /// the stored `ColumnInfo`, not on anything this node builds by hand.
    fn default_catalog() -> ClusterCatalog {
        let mut v = column(2, 1, "v", false);
        v.default_value = ColumnDefaultValue::string_bytes(b"7".to_vec()).into();
        v.origin_default_value = ColumnDefaultValue::string_bytes(b"7".to_vec()).into();
        let table = TableInfo {
            id: 301,
            name: CiString::new("d"),
            columns: vec![column(1, 0, "id", true), v].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        ClusterCatalog {
            resource_groups: Vec::new(),
            policies: Vec::new(),
            schema_version: 7,
            databases: vec![tidb_cluster::cluster_catalog::LoadedDatabase {
                info: DBInfo {
                    id: 5,
                    name: CiString::new("app"),
                    ..DBInfo::default()
                },
                tables: vec![table],
            }],
        }
    }

    /// A column's DEFAULT is stored twice once a table is loaded: on the
    /// cluster's `ColumnInfo` (where `SHOW CREATE TABLE` and the system-row
    /// writer read it) and on the `KvColumn` the driver's INSERT path reads.
    /// The loader must carry it across, or an omitted column silently stores
    /// NULL where TiDB stores the default -- a wrong row, written without a
    /// word.
    #[test]
    fn a_cluster_columns_declared_default_reaches_the_row_an_insert_writes() {
        let (storage, _, _) = cluster_storage();
        let (mut session, skipped) = session_with_cluster_storage(
            &default_catalog(),
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        assert!(
            skipped.is_empty(),
            "a table with a literal DEFAULT is served"
        );
        session.run("USE app").unwrap();
        // The statements share one uncommitted buffer, which Go reads only
        // inside the transaction that wrote it (through a union scan).
        begin(&mut session);
        session.run("INSERT INTO d (id) VALUES (1)").unwrap();
        let StmtResult::Rows(rows) = session.run("SELECT v FROM d").unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows, vec![vec![tidb_datatype::Datum::Int(7)]]);
    }

    #[test]
    fn loaded_hidden_columns_preserve_native_layout_and_index_values() {
        let mut hidden = column(3, 1, "_V$_vi_0", false);
        hidden.hidden = true;
        hidden.generated_expr_string = "`v` + 1".to_owned();
        let mut generated = column(4, 3, "g", false);
        generated.generated_expr_string = "`id` + `v`".to_owned();
        let mut catalog = loaded_catalog();
        catalog.databases[0].tables.truncate(1);
        let info = &mut catalog.databases[0].tables[0];
        // A visible column may follow a hidden column in stored Go metadata.
        info.columns = vec![
            column(1, 0, "id", true),
            hidden,
            column(2, 2, "v", false),
            generated,
        ]
        .into();
        info.indices = vec![index(7, "vi", "_V$_vi_0", 1, -1)].into();
        let (storage, _, _) = cluster_storage();
        let table = cluster_table(info, &storage, &AutoIdSource::Unavailable).unwrap();
        assert_eq!(table.visible_column_count(), 3);
        assert_eq!(
            table
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.id))
                .collect::<Vec<_>>(),
            [("id", 1), ("v", 2), ("g", 4), ("_V$_vi_0", 3)],
        );
        assert_eq!(table.indexes()[0].column_offsets, [3]);
        let (mut session, skipped) = session_with_cluster_storage(
            &catalog,
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        assert!(skipped.is_empty(), "{skipped:?}");
        session.run("USE app").unwrap();
        // The statements share one uncommitted buffer, which Go reads only
        // inside the transaction that wrote it (through a union scan).
        begin(&mut session);
        session
            .run("INSERT INTO t(id,v) VALUES (1,10),(2,20)")
            .unwrap();
        let StmtResult::Rows(rows) = session.run("SELECT * FROM t ORDER BY id").unwrap() else {
            panic!("rows");
        };
        assert_eq!(
            rows,
            [
                vec![Datum::Int(1), Datum::Int(10), Datum::Int(11)],
                vec![Datum::Int(2), Datum::Int(20), Datum::Int(22)],
            ]
        );
        session.run("UPDATE t SET v=30 WHERE id=1").unwrap();
        let StmtResult::Rows(rows) = session
            .run("SELECT id,v,g FROM t FORCE INDEX(vi) ORDER BY id")
            .unwrap()
        else {
            panic!("rows");
        };
        assert_eq!(
            rows,
            [
                vec![Datum::Int(1), Datum::Int(30), Datum::Int(31)],
                vec![Datum::Int(2), Datum::Int(20), Datum::Int(22)],
            ]
        );
    }

    #[test]
    fn loaded_generated_columns_keep_virtual_and_stored_semantics() {
        let mut virtual_column = column(2, 1, "v", false);
        virtual_column.generated_expr_string = "`id` + 10".to_owned();
        let mut stored_column = column(3, 2, "s", false);
        stored_column.generated_expr_string = "`id` + 20".to_owned();
        stored_column.generated_stored = true;
        let mut catalog = loaded_catalog();
        catalog.databases[0].tables.truncate(1);
        catalog.databases[0].tables[0].columns =
            vec![column(1, 0, "id", true), virtual_column, stored_column].into();
        let (storage, _, snapshot) = cluster_storage();
        // A stored generated value is decoded, not recomputed by the reader.
        snapshot.lock().unwrap().data.insert(
            tidb_tablecodec::table_key::encode_row_key_with_handle(
                101,
                &tidb_tablecodec::table_key::RecordHandle::Int(9),
            ),
            tidb_tablecodec::encode_table_row(None, &[Datum::Int(999)], &[3], true, None).unwrap(),
        );
        let (mut session, skipped) = session_with_cluster_storage(
            &catalog,
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        assert!(skipped.is_empty());
        session.run("USE app").unwrap();
        // The statements share one uncommitted buffer, which Go reads only
        // inside the transaction that wrote it (through a union scan).
        begin(&mut session);
        session.run("INSERT INTO t(id) VALUES (1),(2),(3)").unwrap();
        let StmtResult::Rows(rows) = session.run("SELECT * FROM t ORDER BY id").unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(1), Datum::Int(11), Datum::Int(21)],
                vec![Datum::Int(2), Datum::Int(12), Datum::Int(22)],
                vec![Datum::Int(3), Datum::Int(13), Datum::Int(23)],
                vec![Datum::Int(9), Datum::Int(19), Datum::Int(999)],
            ]
        );
        session
            .run("UPDATE t SET id=id+10 WHERE id>=1 AND id<=3 AND v=12")
            .unwrap();
        session
            .run("DELETE FROM t WHERE id>=1 AND id<=12 AND s=23")
            .unwrap();
        let StmtResult::Rows(rows) = session.run("SELECT * FROM t ORDER BY id").unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int(1), Datum::Int(11), Datum::Int(21)],
                vec![Datum::Int(9), Datum::Int(19), Datum::Int(999)],
                vec![Datum::Int(12), Datum::Int(22), Datum::Int(32)],
            ]
        );
        let mut invalid = column(2, 1, "v", false);
        invalid.generated_expr_string = "`missing` + 10".to_owned();
        let mut invalid_table = catalog.databases[0].tables[0].clone();
        invalid_table.columns = vec![column(1, 0, "id", true), invalid].into();
        let Err(message) = cluster_table(&invalid_table, &storage, &AutoIdSource::Unavailable)
        else {
            panic!("an invalid generated expression must not become an ordinary column");
        };
        assert!(message.contains("generated column v"));
        assert!(message.contains("missing"));
    }

    #[test]
    fn a_cluster_table_restores_metadata_id_high_water_marks() {
        let table = TableInfo {
            id: 302,
            name: CiString::new("t"),
            columns: vec![column(1, 0, "id", true)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            max_column_id: 17,
            max_index_id: 23,
            ..TableInfo::default()
        };
        let (storage, _, _) = cluster_storage();
        let loaded = cluster_table(&table, &storage, &AutoIdSource::Unavailable).unwrap();
        assert_eq!(loaded.next_column_id(), 18);
        assert_eq!(loaded.next_index_id(), 24);
        // Go `BuildTableInfoWithLike` keeps the source's id high-water marks.
        let mut info = tidb_ddl_build::table_info_build::build_table_info_with_like(
            "app",
            "copy",
            &loaded.meta().read(),
            tidb_ast::CreateTableTemporary::None,
            false,
        )
        .unwrap();
        info.id = 303;
        let copy = cluster_table(&info, &storage, &AutoIdSource::Unavailable).unwrap();
        assert_eq!(copy.next_column_id(), 18);
        assert_eq!(copy.next_index_id(), 24);
    }

    #[test]
    fn a_cluster_table_restores_its_persisted_cache_status() {
        let table = TableInfo {
            id: 302,
            name: CiString::new("cached"),
            columns: vec![column(1, 0, "id", true)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            table_cache_status_type: tidb_model::TableCacheStatusType::ENABLE,
            ..TableInfo::default()
        };
        let (storage, _, _) = cluster_storage();
        let loaded = cluster_table(&table, &storage, &AutoIdSource::Unavailable)
            .expect("the cached table is otherwise ordinary");
        assert!(loaded.is_cached());
    }

    /// A lock another TiDB's lock job persisted reaches the loaded table,
    /// so this node's `CheckTableLock` sees it.
    #[test]
    fn a_cluster_table_restores_its_persisted_table_lock() {
        let table = TableInfo {
            id: 303,
            name: CiString::new("locked"),
            columns: vec![column(1, 0, "id", true)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            lock: Some(tidb_model::GoShared::new(tidb_model::TableLockInfo {
                tp: tidb_ast::TableLockType::WRITE,
                sessions: vec![tidb_model::SessionInfo {
                    server_id: "peer".to_owned(),
                    session_id: 7,
                }]
                .into(),
                state: tidb_model::TableLockState::PUBLIC,
                ts: 0,
            })),
            ..TableInfo::default()
        };
        let (storage, _, _) = cluster_storage();
        let loaded = cluster_table(&table, &storage, &AutoIdSource::Unavailable)
            .expect("the locked table is otherwise ordinary");
        assert!(loaded.is_locked());
        assert_eq!(
            loaded.lock().map(|lock| lock.tp),
            Some(tidb_ast::TableLockType::WRITE)
        );
    }

    #[test]
    fn a_cluster_table_retains_column_publication_state_for_point_reads() {
        let (storage, _, _) = cluster_storage();
        for state in [SchemaState::PUBLIC, SchemaState::WRITE_ONLY] {
            let mut value = column(2, 1, "v", false);
            value.state = state;
            let table = TableInfo {
                id: 302,
                name: CiString::new("t"),
                columns: vec![column(1, 0, "id", true), value].into(),
                pk_is_handle: true,
                state: SchemaState::PUBLIC,
                ..TableInfo::default()
            };
            let loaded = cluster_table(&table, &storage, &AutoIdSource::Unavailable).unwrap();
            assert_eq!(loaded.all_columns_public(), state == SchemaState::PUBLIC);
        }
    }

    /// Go `TableFromMeta` builds every generated column of
    /// `tblInfo.Columns`, public or not. A schema version that
    /// `ADD COLUMN ... AS (...)` or a generated column's `DROP COLUMN`
    /// publishes mid-job holds a non-public generated column while no public
    /// column is generated; any reader loading that version (the
    /// binding-maintenance worker reloads concurrently with DDL) must build
    /// it rather than panic.
    #[test]
    fn a_cluster_table_builds_a_nonpublic_generated_column() {
        let (storage, _, _) = cluster_storage();
        for state in [
            SchemaState::DELETE_ONLY,
            SchemaState::WRITE_ONLY,
            SchemaState::WRITE_REORGANIZATION,
            SchemaState::DELETE_REORGANIZATION,
        ] {
            let mut generated = column(2, 1, "b", false);
            generated.state = state;
            generated.generated_expr_string = "`id` + 1".to_owned();
            let table = TableInfo {
                id: 302,
                name: CiString::new("t"),
                columns: vec![column(1, 0, "id", true), generated].into(),
                pk_is_handle: true,
                state: SchemaState::PUBLIC,
                ..TableInfo::default()
            };
            let loaded = cluster_table(&table, &storage, &AutoIdSource::Unavailable).unwrap();
            let [nonpublic] = loaded.nonpublic_columns() else {
                panic!("the {state:?} column is carried as non-public");
            };
            assert!(nonpublic.column.generated.is_some());
        }
    }

    /// Go `AddRecord` truncates a prefix part of a clustered PRIMARY KEY
    /// (`TruncateIndexValues`) before it encodes the record key. The loader
    /// once carried only the key's offsets, so this node wrote `('abc', 1)`
    /// under the whole `'abc'` while every reader -- and Go -- keys it by
    /// `'ab'`, and an index lookup met "an unrequested handle".
    #[test]
    fn a_clustered_primary_key_keeps_its_prefix_lengths() {
        let mut primary = index(1, "PRIMARY", "a", 0, 2);
        primary.primary = true;
        primary.unique = true;
        primary.columns = vec![
            IndexColumn {
                name: CiString::new("a"),
                offset: 0,
                length: 2,
                ..IndexColumn::default()
            },
            IndexColumn {
                name: CiString::new("b"),
                offset: 1,
                length: -1,
                ..IndexColumn::default()
            },
        ]
        .into();
        let mut a = ColumnInfo::new(1, "a", FieldType::new(FieldTypeCode::Varchar));
        a.field_type.add_flags(PRI_KEY_FLAG);
        let table = TableInfo {
            id: 304,
            name: CiString::new("prefixed"),
            columns: vec![a, column(2, 1, "b", true)].into(),
            indices: vec![primary].into(),
            is_common_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let (storage, _, _) = cluster_storage();
        let loaded = cluster_table(&table, &storage, &AutoIdSource::Unavailable).unwrap();
        assert_eq!(loaded.common_handle_offsets(), [0, 1]);
        assert_eq!(loaded.common_handle_prefix_lengths(), [2, -1]);
    }

    /// Go's `getPossibleAccessPaths` attaches a clustered composite PRIMARY
    /// to the table path and skips it in the ordinary-index loop. The stored
    /// `IndexInfo` still has to reach the executor because common-handle range
    /// building reads it, but it must not become a second PRIMARY declaration
    /// or an `IndexFullScan` candidate after the cluster catalog is loaded.
    #[test]
    fn a_clustered_composite_primary_remains_the_table_path() {
        let mut primary = index(1, "PRIMARY", "a", 0, -1);
        primary.primary = true;
        primary.unique = true;
        primary.columns = vec![
            IndexColumn {
                name: CiString::new("a"),
                offset: 0,
                length: -1,
                ..IndexColumn::default()
            },
            IndexColumn {
                name: CiString::new("b"),
                offset: 1,
                length: -1,
                ..IndexColumn::default()
            },
        ]
        .into();
        let table = TableInfo {
            id: 303,
            name: CiString::new("clustered"),
            columns: vec![
                column(1, 0, "a", true),
                column(2, 1, "b", true),
                column(3, 2, "v", false),
            ]
            .into(),
            indices: vec![primary].into(),
            is_common_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let (storage, _, _) = cluster_storage();
        let (mut session, skipped) = session_with_cluster_storage(
            &one_table_catalog(table),
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        assert!(skipped.is_empty(), "{skipped:?}");
        session.run("USE app").unwrap();

        let StmtResult::Rows(rows) = session
            .run("EXPLAIN FORMAT='brief' SELECT SUM(v) FROM clustered")
            .unwrap()
        else {
            panic!("expected EXPLAIN rows");
        };
        let operators: Vec<_> = rows
            .iter()
            .filter_map(|row| match &row[0] {
                Datum::Bytes(value) => Some(String::from_utf8_lossy(value)),
                _ => None,
            })
            .collect();
        assert!(
            operators
                .iter()
                .any(|operator| operator.contains("TableFullScan")),
            "{operators:#?}"
        );
        assert!(
            operators
                .iter()
                .all(|operator| !operator.contains("IndexFullScan")),
            "{operators:#?}"
        );

        let StmtResult::Rows(rows) = session.run("SHOW CREATE TABLE clustered").unwrap() else {
            panic!("expected SHOW CREATE rows");
        };
        let Datum::String(create) = &rows[0][1] else {
            panic!("expected CREATE TABLE text");
        };
        let create = String::from_utf8_lossy(create.bytes());
        assert_eq!(create.matches("PRIMARY KEY").count(), 1, "{create}");
    }

    /// A default whose value is not a literal -- `CURRENT_TIMESTAMP`, whose
    /// instant belongs to each INSERT and not to the moment the catalog was
    /// loaded -- refuses the table by name. Carrying it as the frozen load
    /// instant, or dropping it, would both write rows TiDB does not.
    #[test]
    fn a_cluster_column_whose_default_is_not_a_literal_refuses_the_table() {
        let catalog = default_catalog();
        let column = catalog.databases[0].tables[0]
            .columns
            .get(1)
            .expect("the fixture has its value column");
        let mut column = column.write();
        column.default_value =
            ColumnDefaultValue::string_bytes(b"CURRENT_TIMESTAMP".to_vec()).into();
        column.origin_default_value = GoAny::nil();
        drop(column);
        let (storage, _, _) = cluster_storage();
        let (_, skipped) = session_with_cluster_storage(
            &catalog,
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].name, "app.d");
        assert!(
            skipped[0].reason.starts_with("its column v has a default"),
            "{}",
            skipped[0].reason
        );
    }

    /// One database `app` holding `ci(id BIGINT PRIMARY KEY, c VARCHAR(32)
    /// COLLATE utf8mb4_general_ci)`, built the way the cluster loader builds
    /// it: the column's `FieldType` is the one decoded from the stored
    /// descriptor, collation name and all.
    fn ci_catalog() -> ClusterCatalog {
        // Decoded from the stored descriptor rather than built by hand, so
        // this exercises the same `From<JsonFieldType>` the catalog loader
        // uses -- including its duty to fill BOTH the collation name and the
        // cached `Collation` enum.
        let text: FieldType = serde_json::from_str(
            r#"{"Tp":15,"Flag":0,"Flen":32,"Decimal":0,"Charset":"utf8mb4",
                "Collate":"utf8mb4_general_ci","Elems":null,
                "ElemsIsBinaryLit":null,"Array":false}"#,
        )
        .unwrap();
        assert_eq!(text.collation(), tidb_datatype::Collation::Utf8Mb4GeneralCi);
        let mut c = ColumnInfo::new(2, "c", text);
        c.offset = 1;
        let table = TableInfo {
            id: 201,
            name: CiString::new("ci"),
            columns: vec![column(1, 0, "id", true), c].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        ClusterCatalog {
            resource_groups: Vec::new(),
            policies: Vec::new(),
            schema_version: 7,
            databases: vec![tidb_cluster::cluster_catalog::LoadedDatabase {
                info: DBInfo {
                    id: 5,
                    name: CiString::new("app"),
                    ..DBInfo::default()
                },
                tables: vec![table],
            }],
        }
    }

    /// A `utf8mb4_general_ci` column loaded FROM THE CLUSTER keeps its
    /// collation all the way into execution: `cluster_table` copies the
    /// stored `FieldType`, the planner's `Column` carries it, and
    /// `collation_of_node` reads the collation name back off that type. So
    /// `ORDER BY`, `SELECT DISTINCT` and `GROUP BY` all run case-insensitively
    /// on the convergence node, where a byte-ordered comparator would give
    /// `B, C, a, b` and four groups.
    ///
    /// (This is the tier the bounded node's `configured_string_is_binary`
    /// gate exists to protect: THAT node's `compare_prepared_rows` hardcodes
    /// `utf8mb4_bin`, so it refuses such a column at load. The wide path
    /// needs no gate because it never reaches that comparator.)
    #[test]
    fn a_case_insensitive_cluster_column_orders_groups_and_dedups_by_its_collation() {
        let (storage, _, _) = cluster_storage();
        let (mut session, skipped) = session_with_cluster_storage(
            &ci_catalog(),
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        assert!(skipped.is_empty(), "a _ci table is served, not refused");
        session.run("USE app").unwrap();
        // The statements share one uncommitted buffer, which Go reads only
        // inside the transaction that wrote it (through a union scan).
        begin(&mut session);
        session
            .run("INSERT INTO ci (id, c) VALUES (1,'a'),(2,'B'),(3,'b'),(4,'C')")
            .unwrap();

        let text = |result: Result<StmtResult, _>| -> Vec<String> {
            let StmtResult::Rows(rows) = result.unwrap_or_else(|_| panic!("expected rows")) else {
                panic!("expected rows");
            };
            rows.iter()
                .map(|row| {
                    row.iter()
                        .map(|d| match d {
                            tidb_datatype::Datum::Bytes(b) => {
                                String::from_utf8_lossy(b).into_owned()
                            }
                            tidb_datatype::Datum::String(s) => {
                                String::from_utf8_lossy(s.bytes()).into_owned()
                            }
                            other => format!("{other:?}"),
                        })
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect()
        };

        // ORDER BY: `_ci` folds case, and equal keys keep insertion order.
        assert_eq!(
            text(session.run("SELECT c FROM ci ORDER BY c")),
            vec!["a", "B", "b", "C"]
        );
        // DISTINCT: 'b' folds into 'B', so three rows, not four.
        assert_eq!(
            text(session.run("SELECT DISTINCT c FROM ci ORDER BY c")),
            vec!["a", "B", "C"]
        );
        // GROUP BY: the same identity gives the folded pair one group of 2.
        assert_eq!(
            text(session.run("SELECT c, COUNT(*) FROM ci GROUP BY c ORDER BY c")),
            vec!["a|Int(1)", "B|Int(2)", "C|Int(1)"]
        );
        // WHERE string equality and a self-join key derive the same collation.
        assert_eq!(
            text(session.run("SELECT COUNT(*) FROM ci WHERE c = 'B'")),
            vec!["Int(2)"]
        );
        assert_eq!(
            text(session.run("SELECT COUNT(*) FROM ci a, ci b WHERE a.c = b.c")),
            vec!["Int(6)"]
        );
    }

    #[test]
    fn hash_join_preserves_the_planned_comparison_collation() {
        let (storage, _, _) = cluster_storage();
        let (mut session, _) = session_with_cluster_storage(
            &ci_catalog(),
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        session.run("USE app").unwrap();
        // The statements share one uncommitted buffer, which Go reads only
        // inside the transaction that wrote it (through a union scan).
        begin(&mut session);
        session
            .run("INSERT INTO ci (id,c) VALUES (1,'B'),(2,'b')")
            .unwrap();
        for (comparison, expected) in [
            ("a.c = b.c", 4),
            ("a.c COLLATE utf8mb4_bin = b.c COLLATE utf8mb4_bin", 2),
        ] {
            let StmtResult::Rows(rows) = session
                .run(&format!(
                    "SELECT COUNT(*) FROM ci a JOIN ci b ON {comparison}"
                ))
                .unwrap()
            else {
                panic!("expected join rows");
            };
            assert_eq!(
                rows,
                vec![vec![tidb_datatype::Datum::Int(expected)]],
                "{comparison}"
            );
        }
    }

    #[test]
    fn a_snapshot_row_is_visible_and_shadowed_by_a_staged_write() {
        let (storage, _, snapshot) = cluster_storage();
        let (mut session, _) = session_with_cluster_storage(
            &loaded_catalog(),
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        session.run("USE app").unwrap();
        // The statements share one uncommitted buffer, which Go reads only
        // inside the transaction that wrote it (through a union scan).
        begin(&mut session);
        // Write one row through the driver, promote its staged bytes into the
        // snapshot, and start over: that is what a COMMIT leaves behind for the
        // next statement to read.
        session.run("INSERT INTO t (id, v) VALUES (9, 90)").unwrap();
        let staged = storage.buffer().snapshot();
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

    /// An index on a column, as the cluster stores it. `prefix` is Go's
    /// `IndexColumn.Length`, which is `UnspecifiedLength` (-1) for a whole
    /// column and a byte count for a prefix index.
    fn index(id: i64, name: &str, column: &str, offset: i64, prefix: i64) -> IndexInfo {
        IndexInfo {
            id,
            name: CiString::new(name),
            columns: vec![IndexColumn {
                name: CiString::new(column),
                offset,
                length: prefix,
                ..IndexColumn::default()
            }]
            .into(),
            state: SchemaState::PUBLIC,
            ..IndexInfo::default()
        }
    }

    fn one_table_catalog(table: TableInfo) -> ClusterCatalog {
        ClusterCatalog {
            resource_groups: Vec::new(),
            policies: Vec::new(),
            schema_version: 7,
            databases: vec![tidb_cluster::cluster_catalog::LoadedDatabase {
                info: DBInfo {
                    id: 5,
                    name: CiString::new("app"),
                    ..DBInfo::default()
                },
                tables: vec![table],
            }],
        }
    }

    /// The table this loader used to refuse by name. It is served now: the
    /// counter it needs no longer starts at zero in this process, it comes
    /// from the node's registry over the cluster's own meta key.
    #[test]
    fn a_cluster_columns_auto_increment_is_served_with_a_counter_from_the_node() {
        let mut v = column(2, 1, "v", false);
        v.field_type
            .add_flags(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT | 1);
        let table = TableInfo {
            id: 401,
            name: CiString::new("ai"),
            columns: vec![column(1, 0, "id", true), v].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let (storage, _, _) = cluster_storage();
        let homes = LocalTableAutoIds::default();
        let built = cluster_session_catalog(
            &one_table_catalog(table.clone()),
            &storage,
            &StatsSnapshot::new(),
            &homes,
        );
        assert!(
            built.skipped.is_empty(),
            "the table is served now the counter has a home: {:?}",
            built.skipped
        );
        // The allocator handed to a rebuilt table is the SAME one, so the ids
        // it already reserved are not abandoned. This is the property that
        // makes a per-statement catalog rebuild invisible in the id sequence.
        let first = homes.allocator_for(1, &table);
        let second = homes.allocator_for(1, &table);
        assert!(first.same_allocator_as(&second));
    }

    #[test]
    fn auto_random_uses_a_distinct_stable_allocator() {
        let table = TableInfo {
            id: 402,
            name: CiString::new("ar"),
            columns: vec![column(1, 0, "id", true)].into(),
            pk_is_handle: true,
            auto_random_bits: 5,
            auto_random_range_bits: 64,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let homes = LocalTableAutoIds::default();
        let row = homes.allocator_for(1, &table);
        let random = homes.random_allocator_for(1, &table);
        let random_again = homes.random_allocator_for(1, &table);
        assert!(!row.same_allocator_as(&random));
        assert!(random.same_allocator_as(&random_again));
    }

    /// A table without a primary key uses the hidden `_tidb_rowid`. Separate
    /// SQL sessions on one node must draw those handles from one allocator,
    /// or concurrent inserts reuse the same record key and one row overwrites
    /// the other.
    #[test]
    fn non_clustered_tables_share_the_hidden_rowid_allocator() {
        let table = TableInfo {
            id: 403,
            name: CiString::new("no_pk"),
            columns: vec![column(1, 0, "v", false)].into(),
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let catalog = one_table_catalog(table);
        let (storage, buffer, _) = cluster_storage();
        let homes = LocalTableAutoIds::default();

        let (mut first, first_skipped) =
            session_with_cluster_storage(&catalog, &storage, &StatsSnapshot::new(), &homes);
        let (mut second, second_skipped) =
            session_with_cluster_storage(&catalog, &storage, &StatsSnapshot::new(), &homes);
        assert!(first_skipped.is_empty());
        assert!(second_skipped.is_empty());
        first.run("USE app").unwrap();
        second.run("USE app").unwrap();

        first.run("INSERT INTO no_pk (v) VALUES (10)").unwrap();
        second.run("INSERT INTO no_pk (v) VALUES (20)").unwrap();

        // Two record keys, one per session's row. (Neither session reads the
        // other's uncommitted row: a reader sees the snapshot, and only its
        // own transaction's buffer through a union scan.)
        assert_eq!(buffer.len(), 2, "peer sessions must not overwrite rowids");
    }

    /// The `CREATE INDEX`/`DROP INDEX` backfill walks the rows a table already
    /// has and never allocates, which is why it is given no counter -- and
    /// this is the proof rather than the assumption.
    ///
    /// The table is deliberately one WITH an `AUTO_INCREMENT` column, because
    /// that is the only case where a counter is installed at all. It is loaded
    /// twice from one storage, exactly as production does: once as a session's
    /// table, which inserts the rows, and once as
    /// [`AutoIdSource::Unavailable`], which is the shape
    /// `KvTableIndexBackfiller::stage` builds. Both index walks then succeed
    /// over a counter that answers every request with an error, so neither can
    /// have asked it for anything.
    ///
    /// The last assertion is what keeps the test from being vacuous: it shows
    /// the counter really does refuse, so the two passes above are evidence and
    /// not an allocator that quietly worked. Should `create_index` ever grow a
    /// path that allocates, this test fails loudly instead of that path reading
    /// a counter with no home.
    #[test]
    fn the_index_backfill_never_asks_the_counter_it_was_not_given() {
        let mut v = column(2, 1, "v", false);
        v.field_type
            .add_flags(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT | 1);
        let table = TableInfo {
            id: 402,
            name: CiString::new("ai"),
            columns: vec![column(1, 0, "id", true), v].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let (storage, buffer, _snapshot) = cluster_storage();

        // The rows the backfill will walk, inserted the way a session inserts
        // them -- through a real allocator, since an INSERT genuinely does
        // allocate.
        let (mut session, _) = session_with_cluster_storage(
            &one_table_catalog(table.clone()),
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        session.run("USE app").unwrap();
        session
            .run("INSERT INTO ai (id) VALUES (1), (2), (3)")
            .unwrap();
        assert_eq!(buffer.len(), 3, "three rows are staged for the walk");

        // Now the backfill's own view of the same table: no counter at all.
        let mut walked = cluster_table(&table, &storage, &AutoIdSource::Unavailable)
            .expect("the backfill builds this table");
        let index =
            kv_index(&index(1, "vi", "v", 1, -1), &walked.columns).expect("a full-value index");

        walked
            .create_index_with_context(index, &tidb_executor::StmtContext::default())
            .expect("a backfill that allocates nothing needs no allocator");
        assert!(
            walked
                .drop_index("vi")
                .expect("the removal walk also allocates nothing"),
            "the index it just created is the one it removes"
        );

        // Not vacuous: the counter these two walks held really does refuse.
        assert!(
            walked.rebase_auto_increment(9).is_err(),
            "a counter with no home must report that, not hand out an id"
        );
    }

    /// Prefix metadata must reach both key construction and access bounds.
    /// Equal prefixes still require the full-value residual predicate.
    #[test]
    fn a_loaded_prefix_index_preserves_full_value_filtering() {
        let (storage, _buffer, _snapshot) = cluster_storage();
        let mut catalog = ci_catalog();
        catalog.databases[0].tables[0].indices = vec![index(1, "idx", "c", 1, 4)].into();
        let (mut session, skipped) = session_with_cluster_storage(
            &catalog,
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        assert!(skipped.is_empty());
        session.run("USE app").unwrap();
        // The statements share one uncommitted buffer, which Go reads only
        // inside the transaction that wrote it (through a union scan).
        begin(&mut session);
        session
            .run("INSERT INTO ci(id,c) VALUES (1,'alphabet'),(2,'alphanumeric'),(3,'beta')")
            .unwrap();
        for (value, expected) in [("alphabet", 1), ("alphanumeric", 2), ("beta", 3)] {
            let StmtResult::Rows(rows) = session
                .run(&format!(
                    "SELECT id FROM ci FORCE INDEX(idx) WHERE c='{value}'"
                ))
                .unwrap()
            else {
                panic!("expected rows");
            };
            assert_eq!(rows, vec![vec![Datum::Int(expected)]]);
        }
    }

    /// Go `TableInfo.Charset`/`Collate` survive the load. They are the
    /// table default `SHOW CREATE TABLE` prints, what
    /// `information_schema.tables.table_collation` reports, and the value a
    /// column's own charset is compared against when the printer decides
    /// whether to emit an explicit CHARACTER SET clause. Dropping them made
    /// a table created as latin1 print `DEFAULT CHARSET=utf8mb4` while its
    /// columns correctly said latin1 -- a definition that does not
    /// round-trip through its own output.
    #[test]
    fn a_stored_table_charset_survives_the_load() {
        let table = TableInfo {
            id: 205,
            name: CiString::new("ch"),
            columns: vec![column(1, 0, "id", true)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            charset: "latin1".to_owned(),
            collate: "latin1_bin".to_owned(),
            ..TableInfo::default()
        };
        let (storage, _buffer, _snapshot) = cluster_storage();
        let (session, skipped) = session_with_cluster_storage(
            &one_table_catalog(table),
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        assert!(skipped.is_empty());
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().unwrap();
        let tidb_executor::driver::TableEntry::Kv(kv) = catalog
            .table_in("app", "ch")
            .expect("the table is in the catalog")
        else {
            panic!("a cluster table is a kv table");
        };
        assert_eq!(kv.charset().charset.name(), "latin1");
        assert_eq!(kv.charset().collation.name(), "latin1_bin");
    }

    /// Go `DBInfo.Charset`/`Collate` survive the load: they are what
    /// `SHOW CREATE DATABASE` and `information_schema.schemata` report, and
    /// the default a table created without its own charset inherits. This
    /// loader used to register every database with the server default, so
    /// `CREATE DATABASE ... CHARACTER SET latin1` and
    /// `ALTER DATABASE ... CHARACTER SET` both read back as utf8mb4.
    #[test]
    fn a_stored_database_charset_survives_the_load() {
        let table = TableInfo {
            id: 204,
            name: CiString::new("t"),
            columns: vec![column(1, 0, "id", true)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let mut catalog_source = one_table_catalog(table);
        catalog_source.databases[0].info.charset = "latin1".to_owned();
        catalog_source.databases[0].info.collate = "latin1_bin".to_owned();

        let (storage, _buffer, _snapshot) = cluster_storage();
        let (session, skipped) = session_with_cluster_storage(
            &catalog_source,
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        assert!(skipped.is_empty());
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().unwrap();
        let (name, charset) = catalog
            .database_definition("app")
            .expect("the database is in the catalog");
        assert_eq!(name, "app");
        assert_eq!(charset.charset.name(), "latin1");
        assert_eq!(charset.collation.name(), "latin1_bin");
    }

    /// Go `TableInfo.Comment` survives the load: `SHOW CREATE TABLE` prints
    /// it and `information_schema.tables.table_comment` reports it. This
    /// loader used to drop it, so a comment written by CREATE or by
    /// `ALTER TABLE ... COMMENT` was stored correctly and then invisible to
    /// every reader.
    #[test]
    fn a_stored_table_comment_survives_the_load() {
        let table = TableInfo {
            id: 203,
            name: CiString::new("cm"),
            columns: vec![column(1, 0, "id", true)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            comment: "the stored comment".to_owned(),
            ..TableInfo::default()
        };
        let (storage, _buffer, _snapshot) = cluster_storage();
        let (session, skipped) = session_with_cluster_storage(
            &one_table_catalog(table),
            &storage,
            &StatsSnapshot::new(),
            &LocalTableAutoIds::default(),
        );
        assert!(skipped.is_empty());
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().unwrap();
        let tidb_executor::driver::TableEntry::Kv(kv) = catalog
            .table_in("app", "cm")
            .expect("the table is in the catalog")
        else {
            panic!("a cluster table is a kv table");
        };
        assert_eq!(kv.comment(), "the stored comment");
    }

    /// Go never chooses an INVISIBLE index for an access path (captured:
    /// `EXPLAIN` shows a full table scan, and `USE INDEX(inv)` is
    /// `[planner:1176]Key 'inv' doesn't exist in table 'iv'`). The index is
    /// still loaded and still maintained by writes -- only the planner is
    /// blind to it -- and the full table scan answers the query exactly.
    ///
    /// Before this, `IndexInfo.Invisible` was dropped on the floor and the
    /// cost-based chooser preferred the invisible index's cheap point range.
    #[test]
    fn an_invisible_index_is_loaded_but_never_chosen() {
        let table = TableInfo {
            id: 202,
            name: CiString::new("iv"),
            columns: vec![column(1, 0, "id", true), column(2, 1, "a", false)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            indices: vec![index(1, "inv", "a", 1, -1)].into(),
            ..TableInfo::default()
        };
        table
            .indices
            .get(0)
            .expect("the fixture has its index")
            .write()
            .comment = "cluster comment".to_owned();
        let invisible = table.clone_like_go();
        invisible
            .indices
            .get(0)
            .expect("the fixture has its index")
            .write()
            .invisible = true;

        for (info, expect_plan_index) in [(table, true), (invisible, false)] {
            // Each pass gets its own storage: the two catalogs describe the
            // same table id, so a shared buffer would collide on the handle.
            let (storage, _buffer, _snapshot) = cluster_storage();
            let (mut session, skipped) = session_with_cluster_storage(
                &one_table_catalog(info),
                &storage,
                &StatsSnapshot::new(),
                &LocalTableAutoIds::default(),
            );
            assert!(skipped.is_empty());
            session.run("USE app").unwrap();
            // The statements share one uncommitted buffer, which Go reads only
            // inside the transaction that wrote it (through a union scan).
            begin(&mut session);
            session
                .run("INSERT INTO iv (id, a) VALUES (1, 10), (2, 20), (3, 30)")
                .unwrap();
            // The answer is the same either way: with the index hidden the
            // full table scan still returns exactly the matching row.
            let StmtResult::Rows(rows) = session.run("SELECT id FROM iv WHERE a = 20").unwrap()
            else {
                panic!("expected rows");
            };
            assert_eq!(format!("{rows:?}"), "[[Int(2)]]");

            // The index is loaded and maintained either way; only the
            // planner's view of it differs.
            let catalog = session.shared_catalog();
            let catalog = catalog.lock().unwrap();
            let tidb_executor::driver::TableEntry::Kv(kv) = catalog
                .table_in("app", "iv")
                .expect("the table is in the catalog")
            else {
                panic!("a cluster table is a kv table");
            };
            assert_eq!(kv.indexes().len(), 1);
            assert_eq!(kv.indexes()[0].comment, "cluster comment");
            assert_eq!(kv.indexes()[0].visible, expect_plan_index);
            assert_eq!(kv.plan_indexes().count(), usize::from(expect_plan_index));
        }
    }

    /// A HASH-partitioned table a Go cluster wrote LOADS, with the routing
    /// spec rebuilt from its stored metadata.
    ///
    /// The loader used to skip every partitioned table with "it is
    /// partitioned", so a Go cluster's partitioned tables were simply absent
    /// from this node's catalog. Go rebuilds the evaluable partition
    /// expression from `PartitionInfo.Expr` when it loads a table, and
    /// `ddl::partition_spec_from_metadata` is that rebuild here.
    #[test]
    fn a_hash_partitioned_table_loads_with_its_routing_rebuilt() {
        let mut partition = tidb_model::partition::PartitionInfo {
            partition_type: tidb_ast::PartitionType::HASH,
            expr: "`id`".to_owned(),
            num: 2,
            enable: true,
            ..tidb_model::partition::PartitionInfo::default()
        };
        partition.definitions = vec![
            partition_definition(401, "p0"),
            partition_definition(402, "p1"),
        ]
        .into();
        let table = TableInfo {
            id: 400,
            name: CiString::new("hashed"),
            columns: vec![column(1, 0, "id", true)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            partition: Some(GoShared::new(partition)),
            ..TableInfo::default()
        };
        let (storage, _, _) = cluster_storage();
        let loaded = cluster_table(&table, &storage, &AutoIdSource::Unavailable)
            .expect("a HASH-partitioned table loads");
        let spec = loaded
            .partition()
            .expect("the routing spec came back with the table");
        // The read path routes and prunes off these ids; before the rebuild
        // the table never reached the catalog at all.
        assert_eq!(
            spec.physical_ids(),
            vec![401, 402],
            "the read reaches both partitions' physical tables"
        );
        assert_eq!(spec.num(), 2);
    }

    /// Go `partitionedTable` during REORGANIZE PARTITION: before the change
    /// is public a row written to a current partition is written again into
    /// the adding partition the new scheme routes it to, and a delete
    /// removes both; once the change is public the mirror runs the other
    /// way, from the new partitions into the dropping ones.
    #[test]
    fn a_reorganizing_partition_mirrors_its_writes_like_go() {
        for (state, main, copy) in [
            (SchemaState::WRITE_REORGANIZATION, 402, 502),
            (SchemaState::DELETE_REORGANIZATION, 502, 402),
        ] {
            let mut partition = tidb_model::partition::PartitionInfo {
                partition_type: tidb_ast::PartitionType::HASH,
                expr: "`id`".to_owned(),
                num: 2,
                enable: true,
                ddl_action: tidb_model::ActionType::ACTION_REORGANIZE_PARTITION,
                ddl_state: state,
                ..tidb_model::partition::PartitionInfo::default()
            };
            let current = vec![
                partition_definition(401, "p0"),
                partition_definition(402, "p1"),
            ];
            let next = vec![
                partition_definition(501, "n0"),
                partition_definition(502, "n1"),
            ];
            // Go keeps both sets for the whole job (`DroppingDefinitions` are
            // the partitions being replaced) and swaps `Definitions` when the
            // change becomes public.
            partition.definitions = if state == SchemaState::WRITE_REORGANIZATION {
                current.clone().into()
            } else {
                next.clone().into()
            };
            partition.adding_definitions = next.into();
            partition.dropping_definitions = current.into();
            let table = TableInfo {
                id: 400,
                name: CiString::new("reorg"),
                columns: vec![column(1, 0, "id", true)].into(),
                pk_is_handle: true,
                state: SchemaState::PUBLIC,
                partition: Some(GoShared::new(partition)),
                ..TableInfo::default()
            };
            let (storage, _, _) = cluster_storage();
            let mut loaded = cluster_table(&table, &storage, &AutoIdSource::Unavailable)
                .expect("a reorganizing table loads");
            let handle = loaded
                .insert_row(&[Datum::Int(3)], &tidb_expr::NoColumns)
                .expect("the insert succeeds");
            let key = |id: i64, physical: i64| {
                tidb_codec::encode_row_key(
                    physical,
                    &tidb_txnkv::Handle::from(tidb_txnkv::IntHandle::new(id)).encoded(),
                )
            };
            assert!(
                loaded.stored_raw_value(&key(3, main)).is_some(),
                "{state:?} main"
            );
            assert!(
                loaded.stored_raw_value(&key(3, copy)).is_some(),
                "{state:?} copy"
            );
            // A clustered key change within the partition: the copy moves to
            // the new handle too (Go always removes and adds the copy).
            let ctx = tidb_executor::StmtContext::for_dml(false, false, false);
            loaded
                .update_row_with_context(&handle, &[Datum::Int(5)], &ctx)
                .expect("the update succeeds");
            assert!(
                loaded.stored_raw_value(&key(3, copy)).is_none(),
                "{state:?} old copy"
            );
            assert!(
                loaded.stored_raw_value(&key(5, copy)).is_some(),
                "{state:?} new copy"
            );
            let moved = tidb_executor::TableHandle::Int(5);
            loaded
                .delete_row_with_context(&moved, &ctx)
                .expect("the delete succeeds");
            assert!(
                loaded.stored_raw_value(&key(5, main)).is_none(),
                "{state:?} main"
            );
            assert!(
                loaded.stored_raw_value(&key(5, copy)).is_none(),
                "{state:?} copy"
            );
        }
    }

    /// A KEY-partitioned table loads too: Go's `generateKeyPartitionExpr`
    /// builds no expression at all, only the stored column list.
    #[test]
    fn a_key_partitioned_table_loads_from_its_stored_column_list() {
        let mut partition = tidb_model::partition::PartitionInfo {
            partition_type: tidb_ast::PartitionType::KEY,
            columns: vec![CiString::new("id")].into(),
            num: 2,
            enable: true,
            ..tidb_model::partition::PartitionInfo::default()
        };
        partition.definitions = vec![
            partition_definition(501, "p0"),
            partition_definition(502, "p1"),
        ]
        .into();
        let table = TableInfo {
            id: 500,
            name: CiString::new("keyed"),
            columns: vec![column(1, 0, "id", true)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            partition: Some(GoShared::new(partition)),
            ..TableInfo::default()
        };
        let (storage, _, _) = cluster_storage();
        let loaded = cluster_table(&table, &storage, &AutoIdSource::Unavailable)
            .expect("a KEY-partitioned table loads");
        let spec = loaded.partition().expect("the routing spec came back");
        assert_eq!(spec.physical_ids(), vec![501, 502]);
    }

    /// Partition metadata that is present but NOT enabled is not
    /// partitioning: Go's `GetPartitionInfo` returns nil for it and
    /// `TableFromMeta` builds an ordinary table.
    ///
    /// `ALTER TABLE ... REMOVE PARTITIONING` leaves the record behind with
    /// `Enable` false. Routing rows by a spec Go ignores would put every
    /// write in the wrong physical table.
    #[test]
    fn partition_metadata_that_is_not_enabled_loads_as_an_ordinary_table() {
        let mut partition = tidb_model::partition::PartitionInfo {
            partition_type: tidb_ast::PartitionType::HASH,
            expr: "`id`".to_owned(),
            num: 2,
            enable: false,
            ..tidb_model::partition::PartitionInfo::default()
        };
        partition.definitions = vec![
            partition_definition(601, "p0"),
            partition_definition(602, "p1"),
        ]
        .into();
        let table = TableInfo {
            id: 600,
            name: CiString::new("disabled"),
            columns: vec![column(1, 0, "id", true)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            partition: Some(GoShared::new(partition)),
            ..TableInfo::default()
        };
        let (storage, _, _) = cluster_storage();
        let loaded =
            cluster_table(&table, &storage, &AutoIdSource::Unavailable).expect("the table loads");
        assert!(
            loaded.partition().is_none(),
            "disabled partitioning must not route"
        );
    }

    fn partition_definition(id: i64, name: &str) -> tidb_model::partition::PartitionDefinition {
        let mut definition = tidb_model::partition::PartitionDefinition::default();
        definition.id = id;
        definition.name = CiString::new(name);
        definition
    }

    /// A RANGE-partitioned table loads with its stored bounds FOLDED: Go
    /// re-parses `PartitionDefinition.LessThan` on every load, so `10` and
    /// the literal `MAXVALUE` become the same upper bounds the original
    /// `CREATE TABLE` produced.
    #[test]
    fn a_range_partitioned_table_loads_with_its_stored_bounds_folded() {
        let mut partition = tidb_model::partition::PartitionInfo {
            partition_type: tidb_ast::PartitionType::RANGE,
            expr: "`id`".to_owned(),
            enable: true,
            ..tidb_model::partition::PartitionInfo::default()
        };
        partition.definitions = vec![
            bounded_definition(701, "p0", &["10"]),
            bounded_definition(702, "p1", &["MAXVALUE"]),
        ]
        .into();
        let table = TableInfo {
            id: 700,
            name: CiString::new("ranged"),
            columns: vec![column(1, 0, "id", true)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            partition: Some(GoShared::new(partition)),
            ..TableInfo::default()
        };
        let (storage, _, _) = cluster_storage();
        let loaded = cluster_table(&table, &storage, &AutoIdSource::Unavailable)
            .expect("a RANGE-partitioned table loads");
        let spec = loaded.partition().expect("the routing spec came back");
        assert_eq!(spec.physical_ids(), vec![701, 702]);
        let tidb_executor::partition_routing::PartitionKind::Range { less_than, .. } = &spec.kind
        else {
            panic!(
                "RANGE metadata must rebuild as a RANGE spec, got {:?}",
                spec.kind
            );
        };
        assert!(
            matches!(
                less_than.as_slice(),
                [
                    tidb_executor::partition_routing::RangeBound::Value(10),
                    tidb_executor::partition_routing::RangeBound::MaxValue
                ]
            ),
            "the stored text folded back to the written bounds, got {less_than:?}"
        );
    }

    /// A LIST COLUMNS table loads with its `VALUES IN` tuples folded through
    /// the named column's own type, and the stored `DEFAULT` marker
    /// recognised as Go's `buildListPartitionValueMap` recognises it.
    #[test]
    fn a_list_columns_partitioned_table_loads_with_its_default_partition() {
        let mut partition = tidb_model::partition::PartitionInfo {
            partition_type: tidb_ast::PartitionType::LIST,
            columns: vec![CiString::new("v")].into(),
            enable: true,
            ..tidb_model::partition::PartitionInfo::default()
        };
        partition.definitions = vec![
            listed_definition(801, "p0", &[&["1"], &["2"]]),
            listed_definition(802, "pd", &[&["DEFAULT"]]),
        ]
        .into();
        let table = TableInfo {
            id: 800,
            name: CiString::new("listed"),
            columns: vec![column(1, 0, "id", true), column(2, 1, "v", false)].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            partition: Some(GoShared::new(partition)),
            ..TableInfo::default()
        };
        let (storage, _, _) = cluster_storage();
        let loaded = cluster_table(&table, &storage, &AutoIdSource::Unavailable)
            .expect("a LIST COLUMNS table loads");
        let spec = loaded.partition().expect("the routing spec came back");
        assert_eq!(spec.physical_ids(), vec![801, 802]);
        let tidb_executor::partition_routing::PartitionKind::ListColumns {
            values,
            default_partition,
            ..
        } = &spec.kind
        else {
            panic!(
                "LIST COLUMNS metadata must rebuild as one, got {:?}",
                spec.kind
            );
        };
        assert_eq!(values.len(), 2, "both listed values kept their owner");
        assert_eq!(
            *default_partition,
            Some(1),
            "the stored DEFAULT marker names the catch-all partition"
        );
    }

    fn bounded_definition(
        id: i64,
        name: &str,
        less_than: &[&str],
    ) -> tidb_model::partition::PartitionDefinition {
        let mut definition = partition_definition(id, name);
        definition.less_than = less_than
            .iter()
            .map(|bound| (*bound).to_owned())
            .collect::<Vec<_>>()
            .into();
        definition
    }

    fn listed_definition(
        id: i64,
        name: &str,
        in_values: &[&[&str]],
    ) -> tidb_model::partition::PartitionDefinition {
        let mut definition = partition_definition(id, name);
        definition.in_values = in_values
            .iter()
            .map(|tuple| {
                tuple
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect::<Vec<_>>()
                    .into()
            })
            .collect::<Vec<_>>()
            .into();
        definition
    }
}
