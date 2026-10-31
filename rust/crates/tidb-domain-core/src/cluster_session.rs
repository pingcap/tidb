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
//! [`configure_loaded_table`]: tidb_infoschema::cluster_catalog::configure_loaded_table

use std::sync::{Arc, Mutex};
use tidb_infoschema::cluster_catalog::ClusterCatalog;
use tidb_stats_handle_syncload::stats_watch::{StatsSnapshot, TableStatsState};
use tidb_store_driver_txn::cluster_storage::ClusterTableStorage;
use tidb_infoschema::{Catalog, ViewDef};
use tidb_table::kv_table::KvTable;
use tidb_kv::storage::TableStorage;
#[cfg(test)]
use tidb_model::GoShared;
use tidb_model::{SchemaState, TableInfo};

/// Go `mysql.PriKeyFlag`: what marks the column `PKIsHandle` points at.
#[cfg(test)]
const PRI_KEY_FLAG: u32 = 1 << 1;

// Go `tables.TableFromMeta` and the auto-id seam moved beside the DDL
// planner; the server keeps reaching them by their old paths.
use tidb_table::table_from_meta::cluster_table;
use tidb_table::table_from_meta::{AutoIdSource, TableAutoIds};

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
    pub statistics: Option<Arc<tidb_stats_handle::StatisticsView>>,
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
) -> Arc<tidb_stats_handle::StatisticsView> {
    Arc::new(tidb_stats_handle::StatisticsView::new(Arc::new(
        SnapshotStatisticsSource(stats.clone()),
    )))
}

struct SnapshotStatisticsSource(StatsSnapshot);

impl tidb_stats_handle::StatisticsSource for SnapshotStatisticsSource {
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
    statistics: Option<Arc<tidb_stats_handle::StatisticsView>>,
    auto_ids: &dyn TableAutoIds,
    // Neutral storage over which CACHED-MISS tables are built for the
    // template set; sessions rebind their own seam afterwards either way.
    template_storage: &ClusterTableStorage,
    mut kv_templates: Option<&mut KvTableTemplates>,
) -> ClusterSessionCatalog {
    let mut catalog = tidb_infoschema_perfschema::catalog();
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
        let charset = tidb_table::kv_table::TableCharset {
            charset: tidb_charset::Charset::from_name(&database.info.charset)
                .unwrap_or(tidb_table::kv_table::TableCharset::default().charset),
            collation: tidb_charset::Collation::from_name(&database.info.collate)
                .unwrap_or(tidb_table::kv_table::TableCharset::default().collation),
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

#[cfg(test)]
mod tests {
    use tidb_executor::KvTableDdl;
use super::*;
use tidb_table::table_from_meta::kv_index;

    use std::collections::BTreeMap;
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_kv::cluster_storage::{ClusterSnapshot, SnapshotPairs};
    use tidb_store_driver_txn::cluster_storage::MutationBuffer;
    use tidb_kv::storage::StorageError;
    use tidb_model::column::ColumnInfo;
    use tidb_model::db::DBInfo;
    use tidb_model::index::{IndexColumn, IndexInfo};
    use tidb_table::table_from_meta::LocalTableAutoIds;
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

    /// An invalid generated expression refuses the table rather than
    /// loading the column as an ordinary one.
    #[test]
    fn an_invalid_generated_expression_refuses_the_table() {
        let mut invalid = column(2, 1, "v", false);
        invalid.generated_expr_string = "`missing` + 10".to_owned();
        let table = TableInfo {
            id: 101,
            name: CiString::new("t"),
            columns: vec![column(1, 0, "id", true), invalid].into(),
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let (storage, _, _) = cluster_storage();
        let Err(message) = cluster_table(&table, &storage, &AutoIdSource::Unavailable) else {
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
            databases: vec![tidb_infoschema::cluster_catalog::LoadedDatabase {
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

        // The rows the backfill will walk, staged with their values given,
        // so staging them asks no counter either.
        let mut writer = cluster_table(&table, &storage, &AutoIdSource::Unavailable)
            .expect("the writer builds this table");
        for id in 1..=3 {
            writer
                .insert_row(&[Datum::Int(id), Datum::Int(id)], &tidb_exprctx::NoColumns)
                .expect("the row is staged");
        }
        assert_eq!(buffer.len(), 3, "three rows are staged for the walk");

        // Now the backfill's own view of the same table: no counter at all.
        let mut walked = cluster_table(&table, &storage, &AutoIdSource::Unavailable)
            .expect("the backfill builds this table");
        let index =
            kv_index(&index(1, "vi", "v", 1, -1), &walked.columns()).expect("a full-value index");

        walked
            .create_index_with_context(index, &tidb_sessionctx::StmtContext::default())
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
                .insert_row(&[Datum::Int(3)], &tidb_exprctx::NoColumns)
                .expect("the insert succeeds");
            let key = |id: i64, physical: i64| {
                tidb_tablecodec::table_key::encode_row_key(
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
            let ctx = tidb_sessionctx::StmtContext::for_dml(false, false, false);
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
            let moved = tidb_table::kv_table::TableHandle::Int(5);
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
        let tidb_table::partition_routing::PartitionKind::Range { less_than, .. } = &spec.kind
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
                    tidb_table::partition_routing::RangeBound::Value(10),
                    tidb_table::partition_routing::RangeBound::MaxValue
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
        let tidb_table::partition_routing::PartitionKind::ListColumns {
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

/// Storage bound to no snapshot, used only to decide which tables a catalog
/// *could* be built over. Deciding that reads a `TableInfo`, never a row.
pub fn detached_storage() -> ClusterTableStorage {
    let slot: Arc<Mutex<dyn tidb_kv::cluster_storage::ClusterSnapshot>> =
        Arc::new(Mutex::new(tidb_store_driver_txn::cluster_storage::SwappableSnapshot::new()));
    ClusterTableStorage::new(tidb_store_driver_txn::cluster_storage::MutationBuffer::new(), slot)
}
