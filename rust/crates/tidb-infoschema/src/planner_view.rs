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
//! that projection of a [`SessionExtendedInfoSchema`], and the privilege and hypothetical-index
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
    /// Go `InfoSchema.SchemaMetaVersion()` of the statement's schema.
    schema_meta_version: u64,
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
    /// The statement's `StmtCtx.RelatedTableIDs`, which `buildDataSource`
    /// fills.
    related_table_ids: Option<MdlRelatedTableRecorder>,
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

    /// The statement's `StmtCtx.RelatedTableIDs`, which the planner fills
    /// with every object it resolves, a view's body included.
    #[must_use]
    pub fn with_related_table_ids(mut self, recorder: Option<MdlRelatedTableRecorder>) -> Self {
        self.related_table_ids = recorder;
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
        || !crate::infoschema_meta::is_cluster_table_by_name(name)
    {
        return;
    }
    table.is_memory_table = false;
    table.is_cluster_table = true;
    if !table.table_name().go_lower_eq("CLUSTER_SLOW_QUERY") {
        return;
    }
    let Some(offset) = table.columns.iter().position(|column| {
        column
            .read()
            .field_type
            .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)
    }) else {
        return;
    };
    table.set_is_common_handle(true);
    table.set_common_handle_version(1);
    let primary = vec![tidb_model::GoShared::new(tidb_model::IndexInfo {
        name: tidb_ast::CiString::new("primary"),
        table: tidb_ast::CiString::new(table.table_name()),
        columns: vec![tidb_model::IndexColumn {
            name: table.columns[offset].read().name.clone(),
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
    table.set_table_indices(primary);
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
/// [`SessionExtendedInfoSchema::metadata_version`]) and hands every statement the same `Arc`,
/// rather than converting every column of every table per statement.
///
/// Under InfoSchema V2 the image holds only the eagerly built objects (the
/// memory schemas and the session's local temporary tables); a stored table
/// resolves through the statement's [`LazyPlannerSchema`], which converts
/// only what the statement names, as Go's `infoschemaV2.TableByName` reads
/// through the bounded table cache rather than a whole-schema map.
#[derive(Debug)]
pub struct PlannerSchemaView {
    image: Arc<PlannerSchemaImage>,
    lazy: Option<LazyPlannerSchema>,
}

impl std::ops::Deref for PlannerSchemaView {
    type Target = PlannerSchemaImage;

    fn deref(&self) -> &PlannerSchemaImage {
        &self.image
    }
}

/// The eagerly built part of [`PlannerSchemaView`], shared by every
/// statement planning against the same catalog image.
#[derive(Debug, Default)]
pub struct PlannerSchemaImage {
    databases: std::collections::HashSet<String>,
    pub tables: HashMap<CatalogTableKey, crate::table_source::SourceTable>,
    views: HashMap<CatalogTableKey, crate::table_source::SourceView>,
    /// The sequences (Go `TableInfo.IsSequence()`), which a FROM reads as a
    /// one-row dual and so carry no planner table.
    sequences: HashMap<CatalogTableKey, (String, i64)>,
    /// The same image with the session's local temporary tables detached
    /// (Go `temptable.DetachLocalTemporaryTableInfoSchema`), present only
    /// while one is attached.
    detached: Option<Arc<PlannerSchemaImage>>,
}

#[derive(Debug)]
enum LazySource {
    Table(crate::table_source::SourceTable),
    View(crate::table_source::SourceView),
    /// The sequence's name and Go `TableInfo.ID`.
    Sequence(String, i64),
}
#[derive(Debug)]
struct LazyPlannerSchema {
    source: infoschema_v2::InfoSchemaV2,
    entries: infoschema_v2::BorrowCache<LazySource>,
}
impl LazyPlannerSchema {
    fn new(source: infoschema_v2::InfoSchemaV2) -> Self {
        Self { source, entries: Default::default() }
    }
    fn get(&self, key: &CatalogTableKey) -> Result<Option<&LazySource>, cluster_catalog::ClusterCatalogError> {
        self.source.keep_alive();
        if self.source.table_item_by_name(&key.database, &key.table).is_none() { return Ok(None); }
        if let Some((_, entry)) = self.entries.get(key) { return Ok(Some(entry)); }
        {
            let entry = self.source.table_by_name(&key.database, &key.table, None)?;
            // Go `tryLockMDLAndUpdateSchemaIfNecessary`: a loaded table in
            // an intermediate DDL state is not visible to SQL planning.
            if entry.info.read().state != tidb_model::SchemaState::PUBLIC { return Ok(None); }
            let db = self.source.schema_by_name(&key.database).expect("table identity owns its schema");
            let value = match entry.table.as_ref() {
                TableEntry::Kv(table) => LazySource::Table(kv_source_table(table, db.read().name.original(), db.clone())),
                TableEntry::View(view) => LazySource::View(crate::table_source::SourceView {
                    db_name: db.read().name.original().to_owned(), view_name: view.name.clone(), select_sql: view.select_sql.clone(),
                    table_id: view.id, columns: view.columns.iter().enumerate().map(|(offset, (name, ret_type))| memory_column(offset, name, ret_type)).collect(),
                    definer_user: view.definer_user.clone(), definer_host: view.definer_host.clone(), security_definer: view.security.eq_ignore_ascii_case("DEFINER"),
                    ..Default::default()
                }),
                TableEntry::Sequence(sequence) => LazySource::Sequence(sequence.name.clone(), sequence.id),
            };
            Ok(Some(self.entries.insert(key.clone(), value).1))
        }
    }
}
impl PlannerSchemaView {
    #[cfg(test)]
    pub(crate) fn shares_image_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.image, &other.image)
    }
    /// The image with the session's local temporary tables detached (Go
    /// `DetachLocalTemporaryTableInfoSchema`); the lazy stored tables are
    /// the same either way.
    fn detached(&self) -> &PlannerSchemaImage {
        self.image.detached.as_deref().unwrap_or(&self.image)
    }
    /// The stored object the lazy layer resolves, unless the image already
    /// holds an object under the name: a local temporary table hides a stored
    /// view or sequence of its name (Go's `SessionExtendedInfoSchema` answers
    /// `TableByName` from `LocalTemporaryTables` first).
    fn lazy_source(&self, image: &PlannerSchemaImage, key: &CatalogTableKey) -> Option<&LazySource> {
        if image.tables.contains_key(key) || image.views.contains_key(key) || image.sequences.contains_key(key) {
            return None;
        }
        self.lazy.as_ref()?.get(key).ok()?
    }
    fn ensure_loaded(&self, image: &PlannerSchemaImage, key: &CatalogTableKey) -> Result<(), tidb_terror::MysqlError> {
        if image.tables.contains_key(key) || image.views.contains_key(key) || image.sequences.contains_key(key) { return Ok(()); }
        if let Some(lazy) = &self.lazy { lazy.get(key).map_err(|e| e.to_mysql_error())?; }
        Ok(())
    }
    fn find_table<'a>(&'a self, image: &'a PlannerSchemaImage, key: &CatalogTableKey) -> Option<&'a crate::table_source::SourceTable> {
        image.tables.get(key).or_else(|| match self.lazy_source(image, key)? { LazySource::Table(table) => Some(table), _ => None })
    }
    fn find_view<'a>(&'a self, image: &'a PlannerSchemaImage, key: &CatalogTableKey) -> Option<&'a crate::table_source::SourceView> {
        image.views.get(key).or_else(|| match self.lazy_source(image, key)? { LazySource::View(view) => Some(view), _ => None })
    }
    fn find_sequence(&self, key: &CatalogTableKey) -> Option<(&str, i64)> {
        self.image.sequences.get(key).map(|(name, id)| (name.as_str(), *id)).or_else(|| match self.lazy_source(&self.image, key)? { LazySource::Sequence(name, id) => Some((name.as_str(), *id)), _ => None })
    }
}

impl crate::table_source::TableSource for PlannerCatalog {
    fn try_find_table(&self, db: &str, table: &str) -> Result<Option<&crate::table_source::SourceTable>, tidb_terror::MysqlError> {
        self.view.ensure_loaded(&self.view.image, &CatalogTableKey::new(db, table))?;
        Ok(self.find_table(db, table))
    }
    fn try_find_view(&self, db: &str, table: &str) -> Result<Option<&crate::table_source::SourceView>, tidb_terror::MysqlError> {
        self.view.ensure_loaded(&self.view.image, &CatalogTableKey::new(db, table))?;
        Ok(self.find_view(db, table))
    }
    fn try_find_table_ignoring_local_temporary(&self, db: &str, table: &str) -> Result<Option<&crate::table_source::SourceTable>, tidb_terror::MysqlError> {
        self.view.ensure_loaded(self.view.detached(), &CatalogTableKey::new(db, table))?;
        Ok(self.find_table_ignoring_local_temporary(db, table))
    }

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
            .find_table(&self.view.image, &CatalogTableKey::new(db_name, table_name))
    }

    fn database_exists(&self, db_name: &str) -> bool {
        self.view.image.databases.contains(&db_name.go_to_lower())
    }

    fn latest_index_schema(&self) -> Option<&crate::table_source::LatestIndexSchema> {
        self.latest_index_schema.as_deref()
    }

    fn schema_meta_version(&self) -> u64 {
        self.schema_meta_version
    }

    fn hypo_indexes(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Vec<tidb_model::GoShared<tidb_model::IndexInfo>> {
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
            .find_table(self.view.detached(), &CatalogTableKey::new(db_name, table_name))
    }

    fn try_find_view_ignoring_local_temporary(&self, db: &str, table: &str) -> Result<Option<&crate::table_source::SourceView>, tidb_terror::MysqlError> {
        let key = CatalogTableKey::new(db, table);
        self.view.ensure_loaded(self.view.detached(), &key)?;
        Ok(self.view.find_view(self.view.detached(), &key))
    }

    fn find_view_ignoring_local_temporary(
        &self,
        db_name: &str,
        view_name: &str,
    ) -> Option<&crate::table_source::SourceView> {
        self.view
            .find_view(self.view.detached(), &CatalogTableKey::new(db_name, view_name))
    }

    fn find_view(
        &self,
        db_name: &str,
        view_name: &str,
    ) -> Option<&crate::table_source::SourceView> {
        self.view
            .find_view(&self.view.image, &CatalogTableKey::new(db_name, view_name))
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

    fn record_related_table(&self, table_id: i64) {
        if let Some(record) = &self.related_table_ids {
            record(table_id);
        }
    }

    fn find_sequence(&self, db_name: &str, table_name: &str) -> Option<&str> {
        self.view
            .find_sequence(&CatalogTableKey::new(db_name, table_name))
            .map(|(name, _)| name)
    }

    fn sequence_id(&self, db_name: &str, table_name: &str) -> Option<i64> {
        self.view.find_sequence(&CatalogTableKey::new(db_name, table_name)).map(|(_, id)| id)
    }
}

impl SessionExtendedInfoSchema {
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
            schema_meta_version: self.metadata_version(),
            hypo_indexes: None,
            hypo_tiflash_replicas: None,
            dirty_tables: Vec::new(),
            privilege_verifier: None,
            mdl_related_table: None,
            related_table_ids: None,
        }
    }

    /// The planner's view of this catalog's schema, built on first use and
    /// shared by every statement that plans against the same catalog image
    /// (Go's immutable `InfoSchema` per schema version). Any mutation drops
    /// it, so a statement never plans against a schema that has moved.
    ///
    /// Under V2 the statement gets a fresh lazy layer over the shared image,
    /// so the stored tables it converts stay bounded by the V2 table cache.
    pub fn planner_schema_view(&self) -> Arc<PlannerSchemaView> {
        let shared = self.planner_view.get_or_init(|| {
            Arc::new(PlannerSchemaView {
                image: Arc::new(self.build_planner_schema_view(true)),
                lazy: None,
            })
        });
        match self.v2() {
            Some(source) => Arc::new(PlannerSchemaView {
                image: Arc::clone(&shared.image),
                lazy: Some(LazyPlannerSchema::new(source.clone())),
            }),
            None => Arc::clone(shared),
        }
    }

    /// Builds the view, with the session's LOCAL temporary tables resolving
    /// first when `with_local_temporary` (Go's `SessionExtendedInfoSchema`),
    /// or without them (its `DetachLocalTemporaryTableInfoSchema`).
    fn build_planner_schema_view(&self, with_local_temporary: bool) -> PlannerSchemaImage {
        use crate::table_source::{SourceTable, SourceView};

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
                database.tables.keys().filter_map(move |name| {
                    is_virtual_schema_table(&database.name(), name)
                        .then(|| (database.name().go_to_lower(), name.go_to_lower()))
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
            let resolve_db_info = database.info.clone();
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
                        if is_virtual_schema_table(&database.name(), entry_name) =>
                    {
                        let table_name = if database
                            .name()
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
                            memory_table_id(&database.name(), entry_name)
                        };
                        let mut source_table = SourceTable {
                            is_memory_table: true,
                            db_name: database.name(),
                            physical_table_id: table_id,
                            columns: self.memory_columns(table),
                            ..SourceTable::default()
                        };
                        // A memory table has no stored `TableInfo`; the one
                        // `attach_resolve_metadata` synthesizes carries these.
                        source_table.set_table_id(table_id);
                        source_table.set_table_name(table_name);
                        mark_cluster_table(&mut source_table, &database.name(), entry_name);
                        source_table.attach_resolve_metadata(resolve_db_info.clone(), None);
                        tables.insert(key(), source_table);
                    }
                    TableEntry::Kv(table) => {
                        tables.insert(
                            key(),
                            kv_source_table(table, &database.name(), resolve_db_info.clone()),
                        );
                    }
                    TableEntry::View(view) => {
                        views.insert(
                            key(),
                            SourceView {
                                db_name: database.name(),
                                view_name: view.name.clone(),
                                select_sql: view.select_sql.clone(),
                                table_id: view.id,
                                columns: view
                                    .columns
                                    .iter()
                                    .enumerate()
                                    .map(|(offset, (name, ret_type))| {
                                        memory_column(offset, name, ret_type)
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
                        sequences.insert(key(), (sequence.name.clone(), sequence.id));
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
        // `buildDataSource` under `buildingViewStack`); with none attached
        // the image is its own detached form (Go's
        // `DetachLocalTemporaryTableInfoSchema` returns it unchanged).
        let detached = (with_local_temporary && !self.local_temporary.is_empty())
            .then(|| Arc::new(self.build_planner_schema_view(false)));
        PlannerSchemaImage {
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
                lazy: self.v2().cloned(),
                codec: Arc::new(self.tidb_decode_key_snapshot()),
            })
        }))
    }
}

/// [`InfoSchema`]'s memo of the planner's memory-table columns: the stored
/// table's column list (held, so its address stays unique) and the columns
/// derived from it.
pub(crate) type MemoryColumns = HashMap<
    usize,
    (Arc<Vec<tidb_table::column::Column>>, Arc<Vec<crate::table_source::SourceColumn>>),
>;

impl SessionExtendedInfoSchema {
    /// A memory table's planner columns, derived once per stored column list.
    fn memory_columns(&self, table: &crate::TableCommon) -> Arc<Vec<crate::table_source::SourceColumn>> {
        let stored = table.shared_columns();
        let mut memo = self
            .schema
            .memory_columns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_, derived) = memo.entry(Arc::as_ptr(&stored) as usize).or_insert_with(|| {
            let derived = stored
                .iter()
                .enumerate()
                .map(|(offset, column)| {
                    memory_column(offset, column.read().name.original(), &column.read().field_type)
                })
                .collect();
            (Arc::clone(&stored), Arc::new(derived))
        });
        Arc::clone(derived)
    }
}

/// Go's memory-table `ColumnInfo` (`infoschema.buildColumnInfo`): a public
/// column at `offset` whose id is its position plus one (`c.ID = int64(i) + 1`
/// as `infoschema` installs the table).
fn memory_column(
    offset: usize,
    name: &str,
    field_type: &tidb_datatype::FieldType,
) -> crate::table_source::SourceColumn {
    crate::table_source::SourceColumn::new(tidb_model::GoShared::new(tidb_model::ColumnInfo {
        id: offset as i64 + 1,
        name: tidb_ast::CiString::new(name),
        offset: offset as i64,
        field_type: field_type.clone(),
        state: tidb_model::SchemaState::PUBLIC,
        ..tidb_model::ColumnInfo::default()
    }))
}

/// The planner's `SourceTable` for a stored table of the schema `db_name`
/// (Go's `TableInfo` as `buildDataSource` reads it).
fn kv_source_table(
    table: &crate::TableCommon,
    db_name: &str,
    resolve_db_info: tidb_model::GoShared<tidb_model::DBInfo>,
) -> crate::table_source::SourceTable {

    // Go `buildDataSource` reads `TableInfo.Columns` and `.Indices` as
    // `TableFromMeta` loaded them, in their stored order.
    let meta = table.meta();
    let info = meta.read();
    // Go `tbl.Cols()`: the table's own `table.Column`s.
    let columns = table.shared_columns();
    let mut source_table = crate::table_source::SourceTable {
        db_name: db_name.to_owned(),
        physical_table_id: table.table_id(),
        partition_expression_is_column: table.partition().is_some_and(
            |partition| {
                matches!(
                    partition.expr,
                    tidb_expr::expression::Expression::Column(_)
                )
            },
        ),
        columns,
        ..crate::table_source::SourceTable::default()
    };
    drop(info);
    source_table.attach_resolve_metadata(resolve_db_info, Some(meta));
    source_table
}

/// The `DBInfo` the planner resolves a schema's names with: the schema's own.
fn database_info(database: &Database) -> tidb_model::DBInfo {
    database.info.read().clone()
}
