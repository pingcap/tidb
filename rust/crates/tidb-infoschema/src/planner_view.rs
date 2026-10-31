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

//! The planner's view of the schema image: Go's `PlanBuilder` reads the
//! infoschema through `table.Table` metadata only, never storage. This is
//! that projection of a [`Catalog`], and the privilege and hypothetical-index
//! state a statement's planning adds to it.

use crate::*;

use tidb_sessionctx::{SessionPrivilegeCheck, ViewDefinerPrivilegeCheck};
use tidb_variable::MdlRelatedTableRecorder;


/// An owned snapshot of the catalog fields Go's logical [`PlanBuilder`]
/// reads. The executor catalog owns storage handles and mutable row state;
/// the planner must see only immutable table/view metadata.
pub struct PlannerCatalog {
    current_database: String,
    view: Arc<PlannerSchemaView>,
    latest_index_schema: Option<Arc<crate::table_source::LatestIndexSchema>>,
    /// Go `SessionVars.HypoIndexes`, present only for an `EXPLAIN`.
    hypo_indexes: Option<Arc<tidb_variable::hypo_index::HypoIndexes>>,
    /// Go `SessionVars.HypoTiFlashReplicas`, present only for an `EXPLAIN`.
    hypo_tiflash_replicas: Option<Arc<tidb_variable::hypo_index::HypoTiFlashReplicas>>,
    /// The table ids the open transaction has staged writes under, which Go
    /// reads through `Context.HasDirtyContent` (`tableHasDirtyContent`).
    dirty_tables: Vec<i64>,
    /// Go's privilege manager as the planner sees it (`pm.RequestVerificationWithUser`).
    privilege_verifier: Option<DefinerVerifier>,
    /// The statement's metadata-lock map, which `buildDataSource` fills.
    mdl_related_table: Option<MdlRelatedTableRecorder>,
}

/// [`crate::table_source::PrivilegeVerifier`] over the
/// session's privilege closures.
pub struct DefinerVerifier {
    definer: ViewDefinerPrivilegeCheck,
    session: Option<SessionPrivilegeCheck>,
}

impl crate::table_source::PrivilegeVerifier for DefinerVerifier {
    fn verify_with_user(
        &self,
        user: &str,
        host: &str,
        database: &str,
        table: &str,
        column: &str,
        privilege: tidb_mysql::PrivilegeType,
    ) -> bool {
        (self.definer)(user, host, database, table, column, privilege)
    }

    fn request_verification(
        &self,
        database: &str,
        table: &str,
        column: &str,
        privilege: tidb_mysql::PrivilegeType,
    ) -> bool {
        self.session
            .as_ref()
            .is_none_or(|check| check(database, table, column, privilege))
    }
}

impl PlannerCatalog {
    /// The statement's transaction state: which tables it has already
    /// written, so the planner reads them through a `UnionScan`.
    ///
    /// Go's `HasDirtyContent` reads the transaction's membuffer, so only a
    /// statement inside an open transaction can see a table as dirty; an
    /// autocommit statement starts from a clean one.
    #[must_use]
    pub fn with_dirty_tables(mut self, dirty_tables: Vec<i64>) -> Self {
        self.dirty_tables = dirty_tables;
        self
    }

    /// Go asks `privilege.GetPrivilegeManager(b.ctx)` while it expands a
    /// `SQL SECURITY DEFINER` view; a session that enforces no privileges has
    /// no manager and verifies nothing.
    #[must_use]
    pub fn with_privilege_verifier(
        mut self,
        definer: Option<ViewDefinerPrivilegeCheck>,
        session: Option<SessionPrivilegeCheck>,
    ) -> Self {
        self.privilege_verifier = definer.map(|definer| DefinerVerifier { definer, session });
        self
    }

    /// The statement's `GetRelatedTableForMDL()`, which the planner fills as it
    /// resolves each stored table, a view's body included.
    #[must_use]
    pub fn with_related_tables_for_mdl(mut self, recorder: Option<MdlRelatedTableRecorder>) -> Self {
        self.mdl_related_table = recorder;
        self
    }

    /// Go reads `SessionVars.HypoIndexes` and `HypoTiFlashReplicas` only
    /// under `StmtCtx.InExplainStmt` (`getPossibleAccessPaths`,
    /// `UsedHypoTiFlashReplicas`), so only an EXPLAIN's source carries them.
    #[must_use]
    pub fn with_explain_hypo_state(
        mut self,
        hypo_indexes: Option<Arc<tidb_variable::hypo_index::HypoIndexes>>,
        hypo_tiflash_replicas: Option<Arc<tidb_variable::hypo_index::HypoTiFlashReplicas>>,
    ) -> Self {
        self.hypo_indexes = hypo_indexes;
        self.hypo_tiflash_replicas = hypo_tiflash_replicas;
        self
    }
}

/// Whether `database`.`name` is one of the virtual tables Go serves as a
/// memory table: every `information_schema` table and the predefined
/// `performance_schema` ones.
/// Go `infoschema.IsClusterTableByName` and `buildTableMeta`: a CLUSTER_*
/// table (and `tiflash_replica`, which reads from the DDL owner) is not a
/// `LogicalMemTable`; `buildDataSource` plans it as a table whose scan runs
/// on the TiDB store. Only `CLUSTER_SLOW_QUERY` has a primary key, and
/// because its `time` column is not an integer, Go makes it a COMMON handle.
fn mark_cluster_table(
    table: &mut crate::table_source::SourceTable,
    database: &str,
    name: &str,
) {
    if !database.go_lower_eq(crate::infoschema_meta::INFORMATION_SCHEMA)
        || !crate::infoschema_meta::is_cluster_table_name(name)
    {
        return;
    }
    table.is_memory_table = false;
    table.is_cluster_table = true;
    if !table.table_name.go_lower_eq("CLUSTER_SLOW_QUERY") {
        return;
    }
    let Some(offset) = table.columns.iter().position(|column| {
        column
            .ret_type
            .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)
    }) else {
        return;
    };
    table.columns[offset].is_primary_key = true;
    table.is_common_handle = true;
    table.common_handle_version = 1;
    table.handle_col_offsets = vec![offset];
    table.common_handle_col_offsets = vec![offset];
    table.common_handle_lens = vec![tidb_datatype::UNSPECIFIED_LENGTH];
    table.indexes = vec![tidb_model::GoShared::new(tidb_model::IndexInfo {
        name: tidb_ast::CiString::new("primary"),
        table: tidb_ast::CiString::new(table.table_name.clone()),
        columns: vec![tidb_model::IndexColumn {
            name: tidb_ast::CiString::new(table.columns[offset].name.clone()),
            offset: offset as i64,
            length: tidb_datatype::UNSPECIFIED_LENGTH,
            ..tidb_model::IndexColumn::default()
        }]
        .into(),
        state: tidb_model::SchemaState::PUBLIC,
        unique: true,
        primary: true,
        ..tidb_model::IndexInfo::default()
    })];
}

pub fn is_virtual_schema_table(database: &str, name: &str) -> bool {
    database.go_lower_eq(crate::infoschema_meta::INFORMATION_SCHEMA)
        || database.go_lower_eq(crate::metricsschema::METRICS_SCHEMA)
        || (database.go_lower_eq("performance_schema")
            && crate::virtual_tables::virtual_table(database, name).is_some())
}

/// The planner's view of one schema image: the position Go's immutable
/// `infoschema.InfoSchema` holds. Go builds it once per schema version and
/// every statement plans against the shared value; this tier builds it once
/// per catalog metadata version (the same epoch, see
/// [`Catalog::metadata_version`]) and hands every statement the same `Arc`,
/// rather than converting every column of every table per statement.
#[derive(Debug, Default)]
pub struct PlannerSchemaView {
    databases: std::collections::HashSet<String>,
    pub tables: HashMap<CatalogTableKey, crate::table_source::SourceTable>,
    views: HashMap<CatalogTableKey, crate::table_source::SourceView>,
    /// The sequences (Go `TableInfo.IsSequence()`), which a FROM reads as a
    /// one-row dual and so carry no planner table.
    sequences: HashMap<CatalogTableKey, String>,
    /// The same image with the session's local temporary tables detached
    /// (Go `temptable.DetachLocalTemporaryTableInfoSchema`), present only
    /// while one is attached.
    detached: Option<Arc<PlannerSchemaView>>,
}

impl crate::table_source::TableSource for PlannerCatalog {
    fn current_database(&self) -> &str {
        &self.current_database
    }

    fn table_has_dirty_content(&self, table_ids: &[i64]) -> bool {
        table_ids.iter().any(|id| self.dirty_tables.contains(id))
    }

    fn find_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Option<&crate::table_source::SourceTable> {
        self.view
            .tables
            .get(&CatalogTableKey::new(db_name, table_name))
    }

    fn database_exists(&self, db_name: &str) -> bool {
        self.view.databases.contains(&db_name.go_to_lower())
    }

    fn latest_index_schema(&self) -> Option<&crate::table_source::LatestIndexSchema> {
        self.latest_index_schema.as_deref()
    }

    fn hypo_indexes(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Vec<crate::table_source::SourceIndex> {
        // Go appends the session's `*model.IndexInfo` itself.
        let Some(hypo) = self.hypo_indexes.as_deref() else {
            return Vec::new();
        };
        if self.find_table(db_name, table_name).is_none() {
            return Vec::new();
        }
        hypo.table_indexes(db_name, table_name)
            .into_iter()
            .map(|index| tidb_model::GoShared::new(index.clone()))
            .collect()
    }

    fn uses_hypo_tiflash_replicas(&self, db_name: &str, table_name: &str) -> bool {
        self.hypo_tiflash_replicas
            .as_deref()
            .is_some_and(|replicas| replicas.contains(db_name, table_name))
    }

    fn find_table_ignoring_local_temporary(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Option<&crate::table_source::SourceTable> {
        self.view
            .detached
            .as_deref()
            .unwrap_or(&self.view)
            .tables
            .get(&CatalogTableKey::new(db_name, table_name))
    }

    fn find_view_ignoring_local_temporary(
        &self,
        db_name: &str,
        view_name: &str,
    ) -> Option<&crate::table_source::SourceView> {
        self.view
            .detached
            .as_deref()
            .unwrap_or(&self.view)
            .views
            .get(&CatalogTableKey::new(db_name, view_name))
    }

    fn find_view(
        &self,
        db_name: &str,
        view_name: &str,
    ) -> Option<&crate::table_source::SourceView> {
        self.view
            .views
            .get(&CatalogTableKey::new(db_name, view_name))
    }

    fn privilege_verifier(
        &self,
    ) -> Option<&dyn crate::table_source::PrivilegeVerifier> {
        self.privilege_verifier
            .as_ref()
            .map(|verifier| verifier as &dyn crate::table_source::PrivilegeVerifier)
    }

    fn record_related_table_for_mdl(&self, table_id: i64) {
        if let Some(record) = &self.mdl_related_table {
            record(table_id);
        }
    }

    fn find_sequence(&self, db_name: &str, table_name: &str) -> Option<&str> {
        self.view
            .sequences
            .get(&CatalogTableKey::new(db_name, table_name))
            .map(String::as_str)
    }
}

impl Catalog {
    /// Materializes the narrow `infoschema` view consumed by the ported Go
    /// logical planner. Storage handles, rows, statistics and allocators stay
    /// in this catalog; only immutable schema metadata crosses the seam.
    pub fn planner_catalog(
        &self,
        current_database: &str,
        latest_index_schema: Option<Arc<crate::table_source::LatestIndexSchema>>,
    ) -> PlannerCatalog {
        PlannerCatalog {
            current_database: current_database.to_owned(),
            view: self.planner_schema_view(),
            latest_index_schema,
            hypo_indexes: None,
            hypo_tiflash_replicas: None,
            dirty_tables: Vec::new(),
            privilege_verifier: None,
            mdl_related_table: None,
        }
    }

    /// The planner's view of this catalog's schema, built on first use and
    /// shared by every statement that plans against the same catalog image
    /// (Go's immutable `InfoSchema` per schema version). Any mutation drops
    /// it, so a statement never plans against a schema that has moved.
    pub fn planner_schema_view(&self) -> Arc<PlannerSchemaView> {
        Arc::clone(
            self.planner_view
                .get_or_init(|| Arc::new(self.build_planner_schema_view(true))),
        )
    }

    /// Builds the view, with the session's LOCAL temporary tables resolving
    /// first when `with_local_temporary` (Go's `SessionExtendedInfoSchema`),
    /// or without them (its `DetachLocalTemporaryTableInfoSchema`).
    fn build_planner_schema_view(&self, with_local_temporary: bool) -> PlannerSchemaView {
        use crate::table_source::{
            SourceColumn, SourceTable, SourceView,
        };

        let databases = self.schema.databases.keys().cloned().collect();
        let mut tables = HashMap::new();
        let mut views = HashMap::new();
        let mut sequences = HashMap::new();
        // A memory table's ID is Go's `TableInfo.ID` where the schema has one
        // (`information_schema`'s `tableIDMap`), and otherwise a negative id
        // fixed by its schema and name: two views of one catalog must agree
        // on which table an id names, so it cannot follow hash-map order.
        let mut memory_tables: Vec<(String, String)> = self
            .schema.databases
            .values()
            .flat_map(|database| {
                database.tables.iter().filter_map(move |(name, entry)| {
                    (matches!(**entry, TableEntry::Mem(_))
                        || is_virtual_schema_table(&database.name, name))
                    .then(|| (database.name.go_to_lower(), name.go_to_lower()))
                })
            })
            .collect();
        memory_tables.sort_unstable();
        let memory_table_id = |database: &str, name: &str| {
            if database.go_lower_eq(crate::infoschema_meta::INFORMATION_SCHEMA) {
                if let Some(id) = crate::infoschema_meta::memory_table_id(name) {
                    return id;
                }
            }
            let key = (database.go_to_lower(), name.go_to_lower());
            -1 - memory_tables
                .binary_search(&key)
                .map_or(0, |index| index as i64)
        };
        for (database_key, database) in self.schema.databases.iter() {
            let resolve_db_info = tidb_model::GoShared::new(database_info(database));
            for (entry_name, entry) in &database.tables {
                // The owning catalog already stores Go-folded keys. Preserve
                // those keys rather than folding original metadata again.
                let key = || CatalogTableKey {
                    database: database_key.clone(),
                    table: entry_name.clone(),
                };
                match &**entry {
                    // Go plans every `information_schema` and
                    // `performance_schema` table as a memory table
                    // (`buildDataSource`'s `LogicalMemTable`). The stored
                    // objects here carry the schema SHOW and DESCRIBE answer
                    // from; a plan reads one through a `MemTableScan` of the
                    // rows the session generates for the statement. Planned
                    // as stored tables, all the `information_schema` ones
                    // shared table ID 0, so a scan read whichever table that
                    // ID found.
                    TableEntry::Kv(table)
                        if is_virtual_schema_table(&database.name, entry_name) =>
                    {
                        let table_name = if database
                            .name
                            .go_lower_eq(crate::infoschema_meta::INFORMATION_SCHEMA)
                        {
                            crate::infoschema_meta::canonical_table_name(entry_name)
                                .map_or_else(|| entry_name.clone(), str::to_owned)
                        } else {
                            table.name().clone()
                        };
                        let table_id = if table.table_id() != 0 {
                            table.table_id()
                        } else {
                            memory_table_id(&database.name, entry_name)
                        };
                        let mut source_table = SourceTable {
                            is_memory_table: true,
                            table_id,
                            table_name,
                            db_name: database.name.clone(),
                            physical_table_id: table_id,
                            columns: table
                                .columns()
                                .iter()
                                .enumerate()
                                .map(|(offset, column)| SourceColumn {
                                    id: offset as i64 + 1,
                                    name: column.name.clone(),
                                    offset,
                                    ret_type: column.field_type.clone(),
                                    ..SourceColumn::default()
                                })
                                .collect(),
                            ..SourceTable::default()
                        };
                        mark_cluster_table(&mut source_table, &database.name, entry_name);
                        source_table.attach_resolve_metadata(resolve_db_info.clone(), None);
                        tables.insert(key(), source_table);
                    }
                    TableEntry::Kv(table) => {
                        tables.insert(
                            key(),
                            kv_source_table(table, &database.name, resolve_db_info.clone()),
                        );
                    }
                    TableEntry::Mem(table) => {
                        // Go's memory-table `TableInfo.Name.O` is the
                        // table's own spelling, which the scan's access
                        // object and output names print.
                        let table_name = if database
                            .name
                            .go_lower_eq(crate::infoschema_meta::INFORMATION_SCHEMA)
                        {
                            crate::infoschema_meta::canonical_table_name(entry_name)
                                .map_or_else(|| entry_name.clone(), str::to_owned)
                        } else {
                            entry_name.clone()
                        };
                        let table_id = memory_table_id(&database.name, entry_name);
                        let mut source_table = SourceTable {
                            is_memory_table: true,
                            table_id,
                            table_name,
                            db_name: database.name.clone(),
                            physical_table_id: table_id,
                            columns: table
                                .columns
                                .iter()
                                .enumerate()
                                .map(|(offset, (name, ret_type))| SourceColumn {
                                    id: offset as i64 + 1,
                                    name: name.clone(),
                                    offset,
                                    ret_type: ret_type.clone(),
                                    ..SourceColumn::default()
                                })
                                .collect(),
                            ..SourceTable::default()
                        };
                        mark_cluster_table(&mut source_table, &database.name, entry_name);
                        source_table.attach_resolve_metadata(resolve_db_info.clone(), None);
                        tables.insert(key(), source_table);
                    }
                    TableEntry::View(view) => {
                        views.insert(
                            key(),
                            SourceView {
                                db_name: database.name.clone(),
                                view_name: view.name.clone(),
                                select_sql: view.select_sql.clone(),
                                table_id: view.id,
                                columns: view
                                    .columns
                                    .iter()
                                    .enumerate()
                                    .map(|(offset, (name, ret_type))| SourceColumn {
                                        id: offset as i64 + 1,
                                        name: name.clone(),
                                        offset,
                                        ret_type: ret_type.clone(),
                                        ..SourceColumn::default()
                                    })
                                    .collect(),
                                definer_user: view.definer_user.clone(),
                                definer_host: view.definer_host.clone(),
                                security_definer: view.security.eq_ignore_ascii_case("DEFINER"),
                                ..SourceView::default()
                            },
                        );
                    }
                    TableEntry::Sequence(sequence) => {
                        sequences.insert(key(), sequence.name.clone());
                    }
                }
            }
        }
        let with_local_temporary = with_local_temporary && !self.local_temporary.is_empty();
        if with_local_temporary {
            for (key, entry) in &self.local_temporary {
                let TableEntry::Kv(table) = &**entry else {
                    continue;
                };
                // Go's `SessionTables` keeps each table's `DBInfo`, so a
                // table outlives its dropped schema under that name.
                let db_info = self.schema.databases.get(&key.database).map_or_else(
                    || tidb_model::DBInfo {
                        id: table.meta().read().db_id,
                        name: tidb_ast::CiString::new(key.database.clone()),
                        state: tidb_model::SchemaState::PUBLIC,
                        ..tidb_model::DBInfo::default()
                    },
                    |database| database_info(database),
                );
                let db_name = db_info.name.original().to_owned();
                // A view or sequence is a table of the same namespace.
                views.remove(key);
                sequences.remove(key);
                tables.insert(
                    key.clone(),
                    kv_source_table(table, &db_name, tidb_model::GoShared::new(db_info)),
                );
            }
        }
        // A view reads around the session's temporary tables (Go
        // `buildDataSource` under `buildingViewStack`).
        let detached =
            with_local_temporary.then(|| Arc::new(self.build_planner_schema_view(false)));
        PlannerSchemaView {
            databases,
            tables,
            views,
            sequences,
            detached,
        }
    }

    /// Snapshots the latest-domain metadata read by Go
    /// `planner/util/domainmisc.GetLatestIndexInfo`.
    #[must_use]
    pub fn latest_index_schema(&self) -> Arc<crate::table_source::LatestIndexSchema> {
        Arc::clone(self.schema.latest_index_schema.get_or_init(|| {
            let mut table_indexes = std::collections::BTreeMap::new();
            for database in self.schema.databases.values() {
                for entry in database.tables.values() {
                    let TableEntry::Kv(table) = &**entry else {
                        continue;
                    };
                    table_indexes.insert(
                        table.table_id(),
                        table.meta().read().indices.iter_deref().collect(),
                    );
                }
            }
            Arc::new(crate::table_source::LatestIndexSchema {
                schema_meta_version: self.schema.metadata_version,
                table_indexes,
            })
        }))
    }
}

/// The planner's `SourceTable` for a stored table of the schema `db_name`
/// (Go's `TableInfo` as `buildDataSource` reads it).
fn kv_source_table(
    table: &crate::KvTable,
    db_name: &str,
    resolve_db_info: tidb_model::GoShared<tidb_model::DBInfo>,
) -> crate::table_source::SourceTable {
    use crate::table_source::SourceColumn;

    // Go `buildDataSource` reads `TableInfo.Columns` and `.Indices` as
    // `TableFromMeta` loaded them, in their stored order.
    let meta = table.meta();
    let info = meta.read();
    let columns = info
        .cols()
        .iter_deref()
        .map(|column| {
            let column = column.read();
            SourceColumn {
                id: column.id,
                name: column.name.original().to_owned(),
                is_primary_key: column
                    .field_type
                    .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY),
                offset: column.offset as usize,
                ret_type: column.field_type.clone(),
                is_public: true,
                is_generated: column.is_generated(),
                is_hidden: column.hidden,
                is_virtual_generated: column.is_virtual_generated(),
                // Go `table.Column.GeneratedExpr`, parsed once at load.
                generated_expr: table
                    .columns()
                    .iter()
                    .find(|loaded| loaded.id == column.id)
                    .and_then(|loaded| loaded.generated.as_ref())
                    .map(|generated| generated.source.clone()),
            }
        })
        .collect::<Vec<_>>();
    let indexes = info.indices.iter_deref().collect::<Vec<_>>();
    // Go `buildDataSource`'s handle columns: the PKIsHandle column, else the
    // clustered PRIMARY KEY's columns (`tables.FindPrimaryIndex`).
    let primary = info
        .is_common_handle
        .then(|| indexes.iter().find(|index| index.read().primary))
        .flatten();
    let (common_handle_col_offsets, common_handle_lens): (Vec<usize>, Vec<i64>) = primary
        .map(|primary| {
            primary
                .read()
                .columns
                .iter_deref()
                .map(|column| {
                    let column = column.read();
                    (column.offset as usize, column.length)
                })
                .unzip()
        })
        .unwrap_or_default();
    let pk_handle_offset = info
        .pk_is_handle
        .then(|| info.get_pk_col_info())
        .flatten()
        .map(|column| column.read().offset as usize);
    let handle_col_offsets = pk_handle_offset
        .into_iter()
        .chain(common_handle_col_offsets.iter().copied())
        .collect();
    let mut source_table = crate::table_source::SourceTable {
        table_id: table.table_id(),
        table_name: table.name().clone(),
        db_name: db_name.to_owned(),
        physical_table_id: table.table_id(),
        is_partitioned: table.partition().is_some(),
        partition_expression_is_column: table.partition().is_some_and(
            |partition| {
                matches!(
                    partition.expr,
                    tidb_expr::expression::Expression::Column(_)
                )
            },
        ),
        partition_is_reorganizing: table.partition().is_some_and(|partition| {
            !partition.overlapping_dropping_partition_indices.is_empty()
        }),
        partition_definition_names: table
            .partition()
            .map(|partition| {
                partition
                    .definitions
                    .iter()
                    .map(|definition| definition.name.clone())
                    .collect()
            })
            .unwrap_or_default(),
        partition_definition_ids: table
            .partition()
            .map(|partition| {
                partition
                    .definitions
                    .iter()
                    .map(|definition| definition.id)
                    .collect()
            })
            .unwrap_or_default(),
        columns,
        indexes,
        pk_is_handle: info.pk_is_handle,
        is_common_handle: info.is_common_handle,
        common_handle_version: info.common_handle_version,
        is_temporary: table.is_temporary(),
        is_local_temporary: table.temp_table_type()
            == tidb_model::TempTableType::LOCAL,
        is_cached: table.cache_status()
            != tidb_model::TableCacheStatusType::DISABLE,
        has_affinity: table.has_affinity(),
        tiflash_replica: table.tiflash_replica(),
        handle_col_offsets,
        common_handle_col_offsets,
        common_handle_lens,
        ..crate::table_source::SourceTable::default()
    };
    drop(info);
    source_table.attach_resolve_metadata(resolve_db_info, Some(meta));
    source_table
}

/// The `DBInfo` the planner resolves a schema's names with.
fn database_info(database: &Database) -> tidb_model::DBInfo {
    tidb_model::DBInfo {
        id: database.id,
        name: tidb_ast::CiString::new(database.name.clone()),
        charset: database.charset.charset.name().to_owned(),
        collate: database.charset.collation.name().to_owned(),
        state: tidb_model::SchemaState::PUBLIC,
        ..tidb_model::DBInfo::default()
    }
}
