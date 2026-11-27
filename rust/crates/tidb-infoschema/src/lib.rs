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

//! The catalog: the position Go's `infoschema` occupies, reduced to what this
//! tier resolves a name against -- databases, their tables, a table's backing
//! store, and views.
//!
//! Names are case-insensitive as in MySQL, and a bare table name resolves in
//! the session's current database ([`split_table_path`]) -- an empty one being
//! Go's `ErrNoDB`. [`TableResolver`] is the other half of the same lookup:
//! once a name has reached a table, it binds the column references in that
//! table's scope.

use tidb_table::sequence::SequenceDef;
use std::collections::HashMap;
use std::sync::Arc;
use tidb_datatype::FieldType;
use tidb_table::tables::TableCommon;
use tidb_driver_error::DriverError;
use tidb_table::tables::TableCharset;
use tidb_hack::GoCaseCompare;
use tidb_hack::GoToLower;

// Cache versions must distinguish independently rebuilt catalogs and schema
// mutations on divergent clones, not just count mutations in one instance.
// Clones/snapshots retain their version; DML does not allocate a new one.
fn next_metadata_version() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |version| {
        version.checked_add(1)
    })
    .expect("catalog metadata version exhausted")
}

/// The folded schema/table identity, corresponding to Go's two `CIStr.L`
/// fields. Construct once when resolving retained plan metadata; raw-name
/// catalog entrypoints normalize through the same constructor.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CatalogTableKey {
    database: String,
    table: String,
}

impl CatalogTableKey {
    pub fn new(database: &str, table: &str) -> Self {
        Self {
            database: database.go_to_lower(),
            table: table.go_to_lower(),
        }
    }
}

pub mod infoschema_errors;
pub mod masking_policy;
pub mod sieve;
pub mod cache;
pub mod infoschema_v2;
mod bundle_builder;
pub mod metric_table_def;
pub mod session_tables;
pub mod virtual_tables;
pub mod infoschema_meta;
pub mod metricsschema;
pub mod tidb_decode_key;
use tidb_decode_key::TidbDecodeKeySnapshot;
pub mod planner_view;
pub mod table_source;
pub mod sequence;
pub use sequence::SequenceSnapshot;
pub use planner_view::{PlannerCatalog, PlannerSchemaView};
pub mod statistics_access;
pub mod temporary;
pub mod cluster_catalog;

/// Go `dbutil.CheckTableModeIsNormal` (`pkg/util/dbutil/table.go:48`): only a
/// table in `TableModeNormal` can be accessed; `table_name` is reported as
/// the caller holds it.
///
/// # Errors
///
/// `ErrProtectedTableMode` for any other mode.
pub fn check_table_mode_is_normal(
    table_name: &str,
    mode: tidb_model::TableMode,
) -> Result<(), tidb_terror::TerrorError> {
    if mode == tidb_model::TableMode::NORMAL {
        return Ok(());
    }
    Err(infoschema_errors::ERR_PROTECTED_TABLE_MODE
        .fast_gen_by_args(&[table_name.to_owned().into(), mode.to_string().into()]))
}

fn table_flags(entry: &TableEntry) -> (bool, bool, bool) {
    let TableEntry::Kv(table) = entry else {
        return (false, false, false);
    };
    table_flags_from_meta(&table.read_meta())
}

fn table_flags_from_meta(meta: &tidb_model::TableInfo) -> (bool, bool, bool) {
    (
        meta.lock.is_some(),
        meta.mode != tidb_model::TableMode::NORMAL,
        !meta.foreign_keys.is_empty(),
    )
}

/// Splits a table reference into its schema and table names. A bare name
/// resolves in the default schema; `db.t` names its schema explicitly.
///
/// Splits a table path for executor-adjacent statement arms.
pub fn split_table_path_pub<'a>(
    path: &'a [String],
    current_db: &'a str,
) -> Result<(&'a str, &'a str), DriverError> {
    split_table_path(path, current_db)
}

/// `current_db` is the session's selected schema (Go `SessionVars.CurrentDB`);
/// an empty one is Go's `ErrNoDB`.
pub fn split_table_path<'a>(
    path: &'a [String],
    current_db: &'a str,
) -> Result<(&'a str, &'a str), DriverError> {
    match path {
        [name] => {
            if current_db.is_empty() {
                return Err(DriverError::from(
                    tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
                ));
            }
            Ok((current_db, name))
        }
        [database, name] => Ok((database, name)),
        _ => Err(DriverError::unsupported("empty table name")),
    }
}

/// Go's default schema: TiDB's bootstrap runs
/// `CREATE DATABASE IF NOT EXISTS test`, so a fresh server always has it and
/// a connection with no explicit database lands there.
pub const DEFAULT_DATABASE: &str = "test";

/// Go `metadef.SystemDB`: the schema TiDB's bootstrap creates its own tables
/// in. Spelled lower case, which is the name Go stores and reports.
pub const SYSTEM_DATABASE: &str = "mysql";

type CatalogMap<V> = indexmap::IndexMap<String, V, foldhash::quality::RandomState>;

/// One schema: Go `infoschema.schemaTables` -- the schema's `DBInfo`
/// pointer and its tables.
#[derive(Clone, Debug)]
struct Database {
    /// Go `schemaTables.dbInfo`.
    info: tidb_model::GoShared<tidb_model::db::DBInfo>,
    tables: CatalogMap<Arc<TableEntry>>,
}

/// Prior table objects and their source metadata for one schema reload.
pub struct BuiltTableTemplates<'a> {
    database_name: &'a str,
    database: &'a Database,
    sources: HashMap<usize, tidb_model::GoShared<tidb_model::TableInfo>>,
}

impl BuiltTableTemplates<'_> {
    fn entry_for(
        &self,
        table: &tidb_model::GoShared<tidb_model::TableInfo>,
    ) -> Option<(String, Arc<TableEntry>)> {
        if !self.sources.contains_key(&table.identity_address()) {
            return None;
        }
        let table = table.read();
        let (name, entry) = self.database.tables.get_key_value(table.name.lowercase())?;
        Some((name.clone(), Arc::clone(entry)))
    }
}

impl Database {
    fn new(info: tidb_model::db::DBInfo) -> Self {
        Self {
            info: tidb_model::GoShared::new(info),
            tables: CatalogMap::default(),
        }
    }

    /// Go `DBInfo.ID`.
    fn id(&self) -> i64 {
        self.info.read().id
    }

    /// Go `DBInfo.Name.O`.
    fn name(&self) -> String {
        self.info.read().name.original().to_owned()
    }

    /// Go `DBInfo.Charset` / `Collate`: the defaults a table created without
    /// its own inherits. A name this tier does not know falls back to the
    /// server default, as the loader always did.
    fn charset(&self) -> TableCharset {
        let info = self.info.read();
        TableCharset {
            charset: tidb_charset::Charset::from_name(&info.charset)
                .unwrap_or(TableCharset::default().charset),
            collation: tidb_charset::Collation::from_name(&info.collate)
                .unwrap_or(TableCharset::default().collation),
        }
    }

    /// Go `DBInfo.PlacementPolicyRef`.
    fn placement_policy(&self) -> Option<tidb_model::PolicyRefInfo> {
        self.info
            .read()
            .placement_policy_ref
            .as_ref()
            .map(|reference| reference.read().clone())
    }
}

/// A `DBInfo` for a schema registered by name: a memory schema or a
/// fixture's.
fn db_info(id: i64, name: &str, charset: TableCharset) -> tidb_model::db::DBInfo {
    tidb_model::db::DBInfo {
        id,
        name: tidb_ast::CiString::new(name),
        charset: charset.charset.name().to_owned(),
        collate: charset.collation.name().to_owned(),
        state: tidb_model::SchemaState::PUBLIC,
        ..tidb_model::db::DBInfo::default()
    }
}

/// The stored metadata for a table ID in a published v1 image.
#[derive(Debug)]
struct V1StoredTableIndex {
    schemas: Vec<tidb_model::GoShared<tidb_model::db::DBInfo>>,
    tables: HashMap<i64, (usize, tidb_model::GoShared<tidb_model::TableInfo>), foldhash::fast::RandomState>,
}

/// Positions of runtime tables in the catalog, indexed by table and partition ID.
#[derive(Debug)]
pub struct V1TableNameIndex {
    by_id: HashMap<i64, (usize, usize), foldhash::fast::RandomState>,
}

impl V1TableNameIndex {
    /// Whether a runtime table or partition owns this ID.
    pub fn contains_key(&self, id: &i64) -> bool {
        self.by_id.contains_key(id)
    }
}

/// Go `infoschema.InfoSchema`: one schema image -- databases, their tables,
/// views and sequences, placement policies and resource groups -- shared by
/// every session that reads it. Database and table names are
/// case-insensitive, as in MySQL.
#[derive(Clone, Debug)]
pub struct InfoSchema {
    /// Go infoschemaV2, absent for the V1 implementation.
    v2: Option<infoschema_v2::InfoSchemaV2>,
    schema_commit_ts: u64,
    empty_schema_versions: Vec<i64>,
    rule_bundle_map: HashMap<i64, tidb_placement::Bundle>,
    // Snapshot creation shares schema maps. Mutation detaches only the outer
    // name map and the database it touches, retaining all other table maps.
    databases: Arc<CatalogMap<Arc<Database>>>,
    /// V1's stored metadata includes tables and views without a runtime
    /// table entry. The builder indexes it when it publishes the image.
    v1_metadata_by_id: Option<Arc<V1StoredTableIndex>>,
    /// Go `infoschema`'s policy map, keyed by the FOLDED policy name.
    ///
    /// A placement policy is a schema object in its own right, not an
    /// attribute of a table: `DROP PLACEMENT POLICY` has to know whether any
    /// table or partition still references it, and `ALTER PLACEMENT POLICY`
    /// changes what every referencing object means at once. Both need the
    /// policy to live here rather than being copied into each user.
    policies: HashMap<String, tidb_model::PolicyInfo>,
    /// Go `infoschema`'s resource groups by lowercase name, `default`
    /// always among them.
    resource_groups: HashMap<String, tidb_model::ResourceGroupInfo>,
    /// Go infoSchema masking policies, shared by session views of this version.
    masking_policies: Arc<masking_policy::MaskingPolicyCache>,
    /// Go infoSchema.ts, the snapshot used to build this image.
    schema_ts: u64,
    /// Go Builder.enableV2: selects the owning masking-cache invalidation path.
    uses_infoschema_v2: bool,
    /// Sticky indication that a table with a foreign key has existed in
    /// this catalog. DML asks this on every write; keeping the bit avoids
    /// walking and inspecting every table in the common no-FK case. It is
    /// intentionally monotonic: stale `true` only makes the uncommon full
    /// referring-table scan run, while stale `false` would be incorrect.
    foreign_keys_present: bool,
    /// Sticky indication that some table of this catalog has carried a
    /// `LOCK TABLES` lock, which every statement's `CheckTableLock` asks;
    /// monotonic for the same reason as `foreign_keys_present`.
    table_locks_present: bool,
    /// Sticky indication that some table of this catalog has been in a
    /// mode other than `TableModeNormal`, which every statement's
    /// `CheckTableMode` asks; monotonic for the same reason as
    /// `foreign_keys_present`.
    table_modes_present: bool,
    /// The stored tables this tier could not build into this image (Go
    /// builds every stored table, so Go has no such list).
    skipped: Arc<[cluster_catalog::SkippedTable]>,
    /// Bumped by every mutation that actually CHANGED something, so a
    /// transaction can detect that the shared catalog moved under it (Go
    /// detects the same at commit through TiKV's optimistic conflict check on
    /// the written keys).
    ///
    /// "Actually changed" is the invariant, not a detail: every mutator used
    /// to bump on entry and then return `false` for the no-op cases, so a
    /// `CREATE DATABASE IF NOT EXISTS` over an existing schema, or any DDL
    /// that failed its precondition, aborted a concurrent transaction at
    /// commit (`tidb-session`'s `txn.rs` compares this against the version it
    /// started from). Go advances its schema version per COMPLETED DDL job.
    version: u64,
    /// Bumped ONLY by mutations that change schema METADATA a key decoder
    /// reads: databases registered or dropped, tables registered, renamed,
    /// dropped, or re-registered with new columns. Deliberately NOT bumped by
    /// [`Self::get_mut_in`] (every write path takes it, and Go's schema
    /// version -- what row-decode caches key on -- moves only on DDL), nor by
    /// staged-write marks, statistics refreshes, or id allocation. Bumped at
    /// each mutator's ENTRY, so a call that declines still invalidates: the
    /// cost of a spurious rebuild is one snapshot walk, never staleness.
    metadata_version: u64,
    /// Go `InfoSchema.SchemaMetaVersion`: the schema version the image was
    /// built at (0 for an image no loader built).
    schema_meta_version: i64,
    /// The immutable domain index view belongs to this metadata version.
    /// SessionExtendedInfoSchema clones share the view; DDL invalidates only the mutated owner.
    latest_index_schema: std::sync::OnceLock<Arc<crate::table_source::LatestIndexSchema>>,
    /// Memoized answer of walking EVERY database's table map for GLOBAL
    /// temporary tables, valid as of one `metadata_version`. Without the memo
    /// the walk runs twice per statement (`txn`'s overlay guard swaps global
    /// row storage in and out), which is pure overhead for the sessions that
    /// hold no temporary tables -- every OLTP workload.
    temporary_sweep: Option<(u64, Vec<(String, String)>)>,
    /// The planner's memory-table columns (Go `buildColumnInfo`'s
    /// `ColumnInfo`s), keyed by the stored table's shared column list, which
    /// each entry holds, so a key names one list for as long as it is here.
    /// Shared by every version of this image: a memory table never changes,
    /// and a planner image rebuilt per schema version re-derived all of them.
    memory_columns: Arc<std::sync::Mutex<crate::planner_view::MemoryColumns>>,
}

/// Go `infoschema.SessionExtendedInfoSchema`: the shared [`InfoSchema`] with
/// the session's LOCAL temporary tables attached for a statement, and the
/// lookup indexes built over both. Every statement plans and executes
/// against one of these.
#[derive(Clone, Debug)]
pub struct SessionExtendedInfoSchema {
    v2_pins: infoschema_v2::ReaderPins,
    v2_origin: Option<Arc<infoschema_v2::BuilderOrigin>>,
    v2_steps: Vec<(i64, Vec<tidb_model::GoShared<tidb_model::db::DBInfo>>)>,
    /// The shared schema image.
    /// Shared by every reader of this version (Go hands every reader the
    /// same `InfoSchema`); a mutation copies it first (`Arc::make_mut`), so
    /// a reader's clone is a pointer copy plus its own table pins.
    schema: Arc<InfoSchema>,
    /// The planner's view of this catalog image, shared by clones like the
    /// index view and dropped by every mutator that moves [`Self::version`]
    /// or the metadata epoch.
    planner_view: std::sync::OnceLock<Arc<PlannerSchemaView>>,
    /// Go InfoSchema's table-ID index. Positions resolve against this image's
    /// current entries without retaining a second copy of table storage.
    pub table_id_names: Arc<std::sync::OnceLock<V1TableNameIndex>>,
    /// Go `SessionExtendedInfoSchema.LocalTemporaryTables`: the LOCAL
    /// temporary tables of the session running a statement, attached for
    /// that statement. A name or an id resolves here before the shared
    /// schema (`SessionExtendedInfoSchema.TableByName` / `TableByID`),
    /// whether or not that schema still exists -- Go's `SessionTables` owns
    /// each table's `DBInfo` -- and no enumeration of the shared schema lists
    /// them.
    local_temporary: Vec<(CatalogTableKey, std::sync::Arc<TableEntry>)>,
    /// While a session's LOCAL temporary tables are attached: the metadata
    /// version of the image without them, and the version attaching gave.
    /// Go's `SessionExtendedInfoSchema` keeps the shared infoschema's
    /// `SchemaMetaVersion`, so a temporary table never moves a plan-cache key
    /// (see [`SessionExtendedInfoSchema::schema_version`]).
    overlay_versions: Option<(u64, u64)>,
    /// The `TableInfo` facts a statistics cache miss reads, by table /
    /// partition id; the statistics themselves live in the domain's
    /// `StatsHandle`, not in this image.
    statistics_schemas: Arc<std::sync::OnceLock<HashMap<i64, Arc<tidb_stats::PseudoStatsTable>>>>,
}

impl SessionExtendedInfoSchema {
    /// Go `Builder.InitWithDBInfos` for V2: retain lightweight identities,
    /// special attributes and DBInfo, and discard eager ordinary tables.
    pub fn init_with_db_infos_v2(
        &mut self,
        data: Arc<infoschema_v2::Data>,
        loader: Arc<dyn infoschema_v2::TableLoader>,
        capacity: u64,
    ) {
        let version = self.schema_meta_version();
        data.table_cache.set_capacity(capacity);
        let origin = self.v2_origin.take().filter(|origin| Arc::ptr_eq(&origin.source.data, &data));
        if let Some(origin) = &origin {
            let mut origin = (**origin).clone();
            for (version, image) in std::mem::take(&mut self.v2_steps) { origin.apply_image(&data, version, image); }
            origin.apply_image(&data, version, self.stored_schemas());
        } else { data.reset_before_full_load(version); self.v2_steps.clear(); }
        for database in self.stored_schemas() {
            let db = database.read();
            if origin.is_none() { data.add_db(version, db.clone()); }
            if origin.is_none() {
                if let Some(names) = &db.table_name2id {
                    for (name, id) in names.read().iter() {
                        data.add_table_item(infoschema_v2::TableItem { db_name: db.name.clone(), db_id: db.id,
                            table_name: tidb_ast::CiString::new(name), table_id: *id, schema_version: version });
                    }
                }
            }
            for table in db.deprecated_tables.iter_deref() {
                let ti = table.read();
                self.schema_mut().foreign_keys_present |= !ti.foreign_keys.is_empty();
                self.schema_mut().table_locks_present |= ti.lock.is_some();
                self.schema_mut().table_modes_present |= ti.mode != tidb_model::TableMode::NORMAL;
                if origin.is_none() { data.add_table_info(infoschema_v2::TableItem { db_name: db.name.clone(), db_id: db.id,
                    table_name: ti.name.clone(), table_id: ti.id, schema_version: version }, table.clone()); }
                let changed = origin.as_ref().and_then(|o| o.tables.get(&(db.id, ti.id)))
                    .is_none_or(|(name, old)| name != &db.name || !old.ptr_eq(&table));
                if changed {
                    if let Some(entry) = self.schema.databases.get(db.name.lowercase()).and_then(|db| db.tables.get(ti.name.lowercase())) {
                        let info = match entry.as_ref() { TableEntry::Kv(t) => t.meta(), _ => table.clone() };
                        let size = infoschema_v2::table_cache_entry_size(&info.read(), entry);
                        let table_version = data.table_item_version(ti.id, version).unwrap_or(version);
                        data.table_cache.set(infoschema_v2::TableCacheKey { table_id: ti.id, schema_version: table_version },
                            Arc::new(infoschema_v2::CachedTable { info, table: entry.clone(), size }), size);
                    }
                }
            }
        }
        for database in self.schema.databases.values().filter(|db| infoschema_v2::is_special_db(&db.name().go_to_lower())) {
            let mut tables = HashMap::new();
            for (name, entry) in &database.tables {
                if let TableEntry::Kv(table) = entry.as_ref() {
                    let info = table.meta().clone();
                    data.add_table_info(infoschema_v2::TableItem { db_name: database.info.read().name.clone(), db_id: database.id(), table_name: info.read().name.clone(), table_id: info.read().id, schema_version: version }, info.clone());
                    tables.insert(name.clone(), Arc::new(infoschema_v2::CachedTable { info, table: entry.clone(), size: 0 }));
                }
            }
            data.add_special_db(database.info.clone(), tables);
        }
        let v2 = infoschema_v2::InfoSchemaV2::new(data, loader, version, self.schema_ts());
        self.v2_pins = infoschema_v2::ReaderPins::new(&v2);
        for database in Arc::make_mut(&mut self.schema_mut().databases).values_mut() {
            if infoschema_v2::is_special_db(&database.name().go_to_lower()) { continue; }
            let database = Arc::make_mut(database);
            let mut db = database.info.read().clone();
            db.deprecated_tables = Default::default();
            database.info = tidb_model::GoShared::new(db);
            database.tables.clear();
        }
        self.schema_mut().v2 = Some(v2);
        self.update_info_schema_bundles_v2();
        self.planner_view.take();
        self.table_id_names = Default::default();
        self.schema_mut().v1_metadata_by_id = None;
        self.schema_mut().latest_index_schema.take();
    }

    /// Go `Builder`'s load timestamp and staged V2 diff state, copied into
    /// the table factory's newly built image without retaining old reader pins.
    pub fn inherit_loader_state(&mut self, loaded: &Self) {
        self.schema_mut().schema_ts = loaded.schema.schema_ts;
        self.schema_mut().schema_commit_ts = loaded.schema.schema_commit_ts;
        self.schema_mut().uses_infoschema_v2 = loaded.schema.uses_infoschema_v2;
        self.schema_mut().empty_schema_versions = loaded.schema.empty_schema_versions.clone();
        self.v2_origin = loaded.v2_origin.clone();
        self.v2_steps = loaded.v2_steps.clone();
        self.schema_mut().foreign_keys_present |= loaded.schema.foreign_keys_present;
        self.schema_mut().table_locks_present |= loaded.schema.table_locks_present;
        self.schema_mut().table_modes_present |= loaded.schema.table_modes_present;
    }

    #[cfg(test)]
    pub(crate) fn shares_schema_image_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.schema, &other.schema)
    }

    /// The image for writing: copied first while another reader shares it.
    pub(crate) fn schema_mut(&mut self) -> &mut InfoSchema {
        Arc::make_mut(&mut self.schema)
    }

    /// Go `IsV2` with the underlying implementation.
    pub fn v2(&self) -> Option<&infoschema_v2::InfoSchemaV2> { self.schema.v2.as_ref() }

    /// Go `InfoSchema.SchemaTableInfos`, including fallible lazy storage reads.
    pub fn schema_table_infos(&self, schema: &str) -> Result<Vec<tidb_model::GoShared<tidb_model::TableInfo>>, cluster_catalog::ClusterCatalogError> {
        if let Some(v2) = self.v2().filter(|_| !infoschema_v2::is_special_db(&schema.go_to_lower())) {
            return v2.schema_table_infos(schema);
        }
        Ok(self.schema_by_name(schema).map(|db| db.read().deprecated_tables.iter_deref().collect()).unwrap_or_default())
    }

    /// Go `SessionExtendedInfoSchema.TableExists`: a local temporary table,
    /// else the stored table by name -- under V2 from the name index alone,
    /// without loading the table (Go `infoschemaV2.TableExists`).
    pub fn table_exists(&self, schema: &str, table: &str) -> bool {
        if self.local_temporary_entry(&CatalogTableKey::new(schema, table)).is_some() {
            return true;
        }
        if let Some(v2) = self.v2().filter(|_| !infoschema_v2::is_special_db(&schema.go_to_lower())) {
            return v2.table_item_by_name(schema, table).is_some();
        }
        self.schema_by_name(schema).is_some_and(|db| {
            db.read().deprecated_tables.iter_deref().any(|candidate| candidate.read().name.original().go_lower_eq(table))
        })
    }

    /// Go `InfoSchema.SchemaSimpleTableInfos`: enumerate IDs and names without loads.
    pub fn schema_simple_table_infos(&self, schema: &str) -> Vec<infoschema_v2::TableItem> {
        if let Some(v2) = self.v2() { return v2.schema_simple_table_infos(schema); }
        let Some(db) = self.schema_by_name(schema) else { return Vec::new(); };
        let db = db.read();
        db.deprecated_tables.iter_deref().map(|table| {
            let table = table.read();
            infoschema_v2::TableItem { db_name: db.name.clone(), db_id: db.id, table_name: table.name.clone(), table_id: table.id, schema_version: self.schema_meta_version() }
        }).collect()
    }

    /// Go `InfoSchema.ListTablesWithSpecialAttribute`: resident metadata in V2.
    pub fn list_tables_with_special_attribute(&self, filter: impl Fn(&tidb_model::TableInfo) -> bool) -> Vec<(tidb_ast::CiString, Vec<tidb_model::GoShared<tidb_model::TableInfo>>)> {
        if let Some(v2) = self.v2() {
            return v2.list_tables_with_special_attribute(filter).into_iter().map(|(db, tables)| {
                let name = v2.schema_by_name(&db).map(|s| s.read().name.clone()).unwrap_or_else(|| tidb_ast::CiString::new(db));
                (name, tables)
            }).collect();
        }
        self.stored_schemas().into_iter().filter_map(|db| {
            let db = db.read();
            let tables: Vec<_> = db.deprecated_tables.iter_deref().filter(|t| filter(&t.read())).collect();
            (!tables.is_empty()).then(|| (db.name.clone(), tables))
        }).collect()
    }

    /// Go `InfoSchema.TableInfoByName`, preserving lazy storage failures.
    pub fn try_table_info_by_name(&self, schema: &str, table: &str) -> Result<Option<(tidb_model::GoShared<tidb_model::db::DBInfo>, tidb_model::GoShared<tidb_model::TableInfo>)>, cluster_catalog::ClusterCatalogError> {
        if let Some(v2) = self.v2().filter(|_| !infoschema_v2::is_special_db(&schema.go_to_lower())) { v2.keep_alive(); }
        let Some(db) = self.schema_by_name(schema) else { return Ok(None); };
        if let Some(v2) = self.v2().filter(|_| !infoschema_v2::is_special_db(&schema.go_to_lower())) {
            if v2.table_item_by_name(schema, table).is_none() { return Ok(None); }
            return v2.table_by_name(schema, table, None).map(|t| Some((db, t.info.clone())));
        }
        Ok(self.table_info_by_name(schema, table))
    }

    /// Go `Builder.InitWithOldInfoSchema` / `applyDropTableV2`: load only
    /// old table metadata addressed by the diff; dropping a schema lists it.
    pub fn metadata_copy_for_diff(&self, selection: Option<(&std::collections::HashSet<i64>, &std::collections::HashSet<i64>)>) -> Result<Self, cluster_catalog::ClusterCatalogError> {
        let mut databases = Vec::new();
        for database in self.stored_schemas() {
            if self.is_v2() {
                let mut db = database.read().clone();
                let tables = match selection {
                    None => self.schema_table_infos(db.name.original())?,
                    Some((_, dropped_schemas)) if dropped_schemas.contains(&db.id) => self.schema_table_infos(db.name.original())?,
                    Some((ids, _)) => {
                        let source = self.v2().unwrap();
                        let mut tables = Vec::new();
                        let mut loaded = std::collections::HashSet::new();
                        for &id in ids {
                            if let Some(item) = source.table_item_by_id(id).or_else(|| source.table_item_by_partition_id(id)).filter(|item| item.db_id == db.id) {
                                // A table and its partitions resolve to one
                                // table, loaded once: a duplicate survived the
                                // diff's drop of the table.
                                if !loaded.insert(item.table_id) { continue; }
                                // Go applies a diff over `TableByID(diff.OldTableID)`:
                                // by id, since a re-partitioned table's old and new
                                // ids share its name.
                                tables.push(source.table_for_item(&item, false)?.info.clone());
                            }
                        }
                        tables
                    }
                };
                if tables.is_empty() { databases.push(database.clone()); }
                else {
                    db.deprecated_tables = tidb_model::GoSharedPointerSlice::from_handles(tables.into_iter().map(Some).collect());
                    databases.push(tidb_model::GoShared::new(db));
                }
            } else { databases.push(database); }
        }
        let mut schema = Self::init_with_db_infos(databases, self.policies().cloned().collect(), self.resource_groups().cloned().collect(), self.schema_meta_version());
        schema.schema_mut().masking_policies = Arc::new(self.schema.masking_policies.clone_for_builder());
        schema.set_schema_ts(self.schema_ts());
        schema.set_schema_commit_ts(self.schema_commit_ts());
        schema.schema_mut().uses_infoschema_v2 = self.schema.uses_infoschema_v2;

        if let Some(source) = self.v2() {
            let mut origin = infoschema_v2::BuilderOrigin { source: source.clone(), databases: HashMap::new(), tables: HashMap::new() };
            for db in schema.stored_schemas() {
                let di = db.read();
                origin.databases.insert(di.id, db.clone());
                for table in di.deprecated_tables.iter_deref() { let id = table.read().id; origin.tables.insert((di.id, id), (di.name.clone(), table)); }
            }
            schema.v2_origin = Some(Arc::new(origin));
            schema.schema_mut().foreign_keys_present = self.schema.foreign_keys_present;
            schema.schema_mut().table_locks_present = self.schema.table_locks_present;
            schema.schema_mut().table_modes_present = self.schema.table_modes_present;
        }
        Ok(schema)
    }

    /// Go `Builder.ApplyDiff`: retain each successful diff's metadata so the
    /// shared btrees receive the table's change version, not the final batch version.
    pub fn record_v2_diff_image(&mut self, version: i64) {
        if self.v2_origin.is_some() { self.v2_steps.push((version, self.stored_schemas())); }
    }

    /// Go `SessionExtendedInfoSchema.TableByName`: fallible owned lookup.
    pub fn try_table_handle_by_key(&self, key: &CatalogTableKey) -> Result<Option<Arc<TableEntry>>, cluster_catalog::ClusterCatalogError> {
        if let Some(entry) = self.local_temporary_entry(key).or_else(|| self.schema.databases.get(&key.database).and_then(|db| db.tables.get(&key.table))) {
            return Ok(Some(entry.clone()));
        }
        match self.v2() {
            Some(v2) => self.v2_pins.get(v2, key).map(|t| t.map(|t| t.table.clone())),
            None => Ok(None),
        }
    }

    /// Go `InfoSchema.TableInfoByID` (storage failures return not found).
    pub fn table_info_by_id(&self, id: i64) -> Option<tidb_model::GoShared<tidb_model::TableInfo>> {
        if let Some(v2) = self.v2() { return v2.table_by_id(id, None).map(|t| t.info.clone()); }
        if let Some(metadata) = &self.schema.v1_metadata_by_id {
            return metadata.tables.get(&id).map(|(_, table)| table.clone());
        }
        self.stored_schemas().iter().flat_map(|db| db.read().deprecated_tables.iter_deref()).find(|t| t.read().id == id)
    }

    /// Go `InfoSchema.FindTableInfoByPartitionID`'s table metadata.
    pub fn physical_table_info_by_id(&self, id: i64) -> Option<tidb_model::GoShared<tidb_model::TableInfo>> {
        if let Some(v2) = self.v2() { return self.table_info_by_id(v2.table_id_by_partition_id(id).unwrap_or(id)); }
        self.stored_schemas().iter().flat_map(|db| db.read().deprecated_tables.iter_deref()).find(|t| {
            let t = t.read();
            t.id == id || t.get_partition_info().is_some_and(|pi| {
                pi.read().definitions.with_visible(|partitions| partitions.iter().any(|part| part.id == id))
            })
        })
    }

    /// Go `InfoSchema.TableItemByID`'s schema name, without table loading.
    pub fn schema_name_by_table_id(&self, id: i64) -> Option<String> {
        if let Some(v2) = self.v2() { return v2.table_item_by_id(id).map(|t| t.db_name.original().to_owned()); }
        if let Some(metadata) = &self.schema.v1_metadata_by_id {
            return metadata.tables.get(&id).map(|(schema, _)| {
                metadata.schemas[*schema].read().name.original().to_owned()
            });
        }
        self.stored_schemas().iter().find_map(|db| db.read().deprecated_tables.iter_deref().any(|t| t.read().id == id).then(|| db.read().name.original().to_owned()))
    }

    /// Go `TableByName`: preserve storage errors at fallible SQL entrypoints.
    pub fn try_get_in(&self, database: &str, name: &str) -> Result<Option<&TableEntry>, DriverError> {
        let key = CatalogTableKey::new(database, name);
        if let Some(entry) = self.local_temporary_entry(&key).or_else(|| self.schema.databases.get(&key.database).and_then(|db| db.tables.get(&key.table))) {
            return Ok(Some(entry.as_ref()));
        }
        match self.v2() {
            Some(v2) => self.v2_pins.get(v2, &key).map(|t| t.map(|t| t.table.as_ref())).map_err(|e| DriverError::from(e.to_mysql_error())),
            None => Ok(None),
        }
    }

    /// Go IsV2.
    pub fn is_v2(&self) -> bool { self.schema.v2.is_some() }
    /// Go schemaAndTimestamp.timestamp (schema-diff commit timestamp).
    pub fn schema_commit_ts(&self) -> u64 { self.schema.schema_commit_ts }
    /// Go `tryLoadSchemaDiffs`: versions whose schema diff is absent.
    pub fn empty_schema_versions(&self) -> &[i64] { &self.schema.empty_schema_versions }
    /// Go `InfoCache.InsertEmptySchemaVersion`, recorded for publication.
    pub fn record_empty_schema_version(&mut self, version: i64) { self.schema_mut().empty_schema_versions.push(version); }


    /// Go Loader.getTimestampForSchemaVersionWithNonEmptyDiff.
    pub fn set_schema_commit_ts(&mut self, ts: u64) { self.schema_mut().schema_commit_ts = ts; }

    /// Go `infoschema.NewBuilder(...).InitWithDBInfos` before any stored
    /// schema: only the memory schemas Go's `initVirtualTables` builds from
    /// its `drivers` -- `INFORMATION_SCHEMA`, `PERFORMANCE_SCHEMA` and
    /// `METRICS_SCHEMA` with their tables. Every stored schema (`test`,
    /// `mysql`, `sys`, the user's) and every policy and resource group comes
    /// from meta through [`Self::register_database_with_id_and_charset`],
    /// [`Self::set_policy`] and [`Self::set_resource_group`].
    #[must_use]
    pub fn new() -> Self {
        Self::virtual_schemas(true)
    }

    /// A schema image for tests that have no store behind them, Go
    /// `infoschema.MockInfoSchema`'s role: the memory schemas plus the
    /// stored schemas a bootstrapped TiDB holds -- `test`, `mysql` (as an
    /// object with no tables: this tier serves none of its rows, so naming
    /// one answers 1146 while `USE mysql` succeeds) and `sys` -- under
    /// fixed ids, and meta's implicit `default` resource group.
    #[must_use]
    pub fn mock() -> Self {
        let mut catalog = Self::new();
        for (id, name) in [(1, DEFAULT_DATABASE), (3, SYSTEM_DATABASE), (6, "sys")] {
            catalog.register_database_with_id_and_charset(name, id, TableCharset::default());
        }
        catalog.set_resource_group(tidb_model::ResourceGroupInfo::default_group());
        catalog
    }

    /// The memory schemas with none of their tables: the scratch Go's
    /// init-time builders (`infoschema.init`, `perfschema.Init`, the metric
    /// tables) fill once per process.
    pub fn memory_schema_scratch() -> Self {
        Self::virtual_schemas(false)
    }

    /// Go `RegisterVirtualTable` for a memory schema whose `tables` its
    /// init-time builder made: the schema joins every infoschema the DDL
    /// reads (`TableByName`), as Go's `initVirtualTables` adds it.
    pub fn register_virtual_schema(
        db_id: i64,
        name: &str,
        tables: &std::collections::HashMap<String, Arc<TableEntry>>,
    ) {
        let tables = tables
            .values()
            .filter_map(|entry| match &**entry {
                TableEntry::Kv(table) => Some(table.meta().read().clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        crate::virtual_tables::register_virtual_table(
            tidb_model::db::DBInfo {
                id: db_id,
                name: tidb_ast::CiString::new(name.to_owned()),
                charset: tidb_mysql::charset::DefaultCharset.to_owned(),
                collate: tidb_mysql::charset::DefaultCollationName.to_owned(),
                state: tidb_model::SchemaState::PUBLIC,
                deprecated_tables: tables.into(),
                ..tidb_model::db::DBInfo::default()
            },
        );
    }

    /// The tables of `database` (a lowercase key), by stored name.
    pub fn tables_of(
        &self,
        database: &str,
    ) -> impl Iterator<Item = (&String, &Arc<TableEntry>)> {
        let database = database.to_owned();
        self.schema.databases
            .get(&database)
            .into_iter()
            .flat_map(|database| database.tables.iter())
            .chain(self.v2().into_iter().flat_map(move |v2| {
                v2.schema_simple_table_infos(&database).into_iter().filter_map(move |item| {
                    let key = CatalogTableKey::new(item.db_name.lowercase(), item.table_name.lowercase());
                    if self.schema.databases.get(&key.database).is_some_and(|db| db.tables.contains_key(&key.table)) { return None; }
                    let (key, table) = self.v2_pins.get_entry(v2, &key, false).ok()??;
                    Some((&key.table, &table.table))
                })
            }))
    }

    fn virtual_schemas(with_memory_tables: bool) -> Self {
        // Go's `initVirtualTables` builds the memory schemas' tables into the
        // infoschema itself, so they are ordinary objects to every name
        // lookup; each schema's tables are built once per process, as Go's
        // init does. Go's metrics_schema carries 642 storage-backed metric
        // tables whose queries fail `query metric error: pd unavailable` (the
        // metric storage is absent) -- mirrored here as empty MemTables of the
        // same shape (oracle g-is).
        type Tables = HashMap<String, Arc<TableEntry>>;
        let tables = |build: fn() -> Tables| {
            if with_memory_tables {
                build()
            } else {
                Tables::new()
            }
        };
        let schemas = [
            // Go `autoid.InformationSchemaDBID`, `MetricSchemaDBID` and
            // `PerformanceSchemaDBID`: a memory schema's id has the system
            // flag, so it can never be a stored schema's id.
            (
                (1 << 62) | 1,
                "information_schema",
                "INFORMATION_SCHEMA",
                tables(crate::infoschema_meta::tables),
            ),
            (
                (1 << 62) | 20000,
                "metrics_schema",
                "METRICS_SCHEMA",
                tables(crate::metricsschema::tables),
            ),
            (
                (1 << 62) | 10000,
                "performance_schema",
                "PERFORMANCE_SCHEMA",
                tables(|| {
                    PERFORMANCE_SCHEMA_TABLES
                        .get()
                        .map(|tables| tables())
                        .unwrap_or_default()
                }),
            ),
        ];
        let mut databases = CatalogMap::default();
        for (id, key, display, tables) in schemas {
            // Go `initVirtualTables` adds the driver's `DBInfo`, whose
            // `Deprecated.Tables` lists the schema's tables (registered when
            // they were built above), so `TableInfoByName` finds them.
            let info = crate::virtual_tables::virtual_schema(key).unwrap_or_else(|| {
                tidb_model::GoShared::new(db_info(id, display, TableCharset::default()))
            });
            databases.insert(key.to_owned(), Arc::new(Database {
                info,
                tables: tables.into_iter().collect(),
            }));
        }
        let catalog = SessionExtendedInfoSchema {
            v2_pins: Default::default(),
            v2_origin: None,
            v2_steps: Vec::new(),
            schema: Arc::new(InfoSchema {
                v2: None,
                schema_commit_ts: 0,
                empty_schema_versions: Vec::new(),
                rule_bundle_map: HashMap::new(),
                databases: Arc::new(databases),
                v1_metadata_by_id: None,
                policies: HashMap::new(),
                resource_groups: HashMap::new(),
                masking_policies: Arc::default(),
                schema_ts: 0,
                uses_infoschema_v2: false,
                version: 0,
                metadata_version: next_metadata_version(),
                schema_meta_version: 0,
                latest_index_schema: std::sync::OnceLock::new(),
                temporary_sweep: None,
                memory_columns: Arc::default(),
                foreign_keys_present: false,
                table_locks_present: false,
                table_modes_present: false,
                skipped: Arc::from(Vec::new()),
            }),
            planner_view: std::sync::OnceLock::new(),
            table_id_names: Arc::default(),
            local_temporary: Vec::new(),
            overlay_versions: None,
            statistics_schemas: Arc::default(),
        };
        catalog
    }
}

/// A view: Go `model.TableInfo` whose `View` field is set. A view stores no
/// rows -- its `SELECT` is re-run whenever the name is read.
///
/// `columns` holds the names the view reports -- the explicit
/// `CREATE VIEW v (...)` list when one was written, the body's field names
/// otherwise -- paired with the types the body had at `CREATE VIEW`. The
/// names are fixed, but the types are not: Go re-plans the body for every
/// read and for every metadata answer, so `ALTER TABLE base MODIFY` shows
/// through immediately (captured: `DESCRIBE` and
/// `information_schema.columns` both report the new type). Use
/// [`view_column_list`] rather than these cached types wherever the answer is
/// user-visible.
///
/// NOT MODELLED (documented): enforcing `WITH CHECK OPTION` on write-through
/// -- the mode is recorded and reported, but this tier refuses writes through
/// a view outright, which is the only place the check would apply.
#[derive(Clone, Debug)]
pub struct ViewDef {
    /// The view name as written, for `SHOW CREATE VIEW`.
    pub name: String,
    /// The output columns, resolved when the view was created. The names are
    /// the explicit `CREATE VIEW v (...)` list when one was written.
    pub columns: Vec<(String, FieldType)>,
    /// The view's `SELECT`, canonicalized as Go stores it: every field
    /// explicitly aliased, every table reference schema-qualified.
    pub select_sql: String,
    /// The definer's user name (empty when the session has no user, which is
    /// this tier's only case).
    pub definer_user: String,
    /// The definer's host name.
    pub definer_host: String,
    /// The creator connection's client character set.
    pub character_set_client: String,
    /// The creator connection's collation.
    pub collation_connection: String,
    /// The `ALGORITHM` as written, defaulting to `UNDEFINED`.
    pub algorithm: String,
    /// The `SQL SECURITY` mode as written, defaulting to `DEFINER`.
    pub security: String,
    /// The `WITH CHECK OPTION` mode, `CASCADED` unless `LOCAL` was written.
    /// Go records it on every view, written or not, and reports it as
    /// `information_schema.views.CHECK_OPTION`; `SHOW CREATE VIEW` never
    /// prints it.
    pub check_option: String,
    /// Go `TableInfo.UpdateTS`, the TSO of the job that created it (0 when
    /// none was recorded).
    pub update_ts: u64,
    /// Go `TableInfo.ID` (0 when the tier allocated none).
    pub id: i64,
}

impl ViewDef {
    /// Go `TableFromMeta` for a view: built in any state (a DROP VIEW
    /// passes through WRITE_ONLY and DELETE_ONLY); only planning requires
    /// PUBLIC.
    ///
    /// # Errors
    /// The table has no view metadata.
    pub fn from_table_info(table: &tidb_model::TableInfo) -> Result<Self, String> {
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
        Ok(Self {
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
}

/// A catalog table's backing store.
#[derive(Clone, Debug)]
pub enum TableEntry {
    /// Rows stored as real TiKV-format bytes (see [`crate::kv_table`]).
    ///
    /// `Arc`-wrapped so a transient, read-only view of an already-private
    /// (already copy-on-write'd for this transaction, or never yet written
    /// to) table can be handed out for free: see
    /// [`SessionExtendedInfoSchema::physical_kv_table_by_id`]. A caller that mutates goes
    /// through [`std::sync::Arc::make_mut`] the same way [`Self::Kv`]'s own
    /// enclosing `Arc<TableEntry>` already does, so a table already
    /// EXCLUSIVELY owned by this transaction (the overwhelmingly common
    /// case -- nobody else's snapshot points at the same instance) mutates
    /// in place with no clone at all, and one still shared with another
    /// transaction's snapshot forks away first, exactly as before this
    /// change.
    Kv(std::sync::Arc<TableCommon>),
    /// A view: a stored `SELECT` rather than stored rows.
    View(ViewDef),
    /// A sequence: a counter rather than rows. It lives in the TABLE
    /// namespace because Go stores it as a `model.TableInfo` with `Sequence`
    /// set -- which is why `CREATE TABLE` over a sequence name collides, and
    /// why `SELECT * FROM <sequence>` and `DROP TABLE <sequence>` are
    /// errors rather than name-not-found.
    Sequence(SequenceDef),
}

impl TableEntry {
    /// The table's columns as `(name, type)` in schema order.
    ///
    /// HIDDEN columns are not here. This is the schema every user-facing
    /// enumeration is built from -- `SELECT *`, an `INSERT`'s arity, name
    /// resolution -- so the hidden column an expression index was rewritten
    /// into is excluded once, at the source. A position here is therefore NOT
    /// a stored-row offset when the table has a hidden column.
    pub fn columns(&self) -> impl Iterator<Item = (String, FieldType)> {
        let columns: Vec<_> = match self {
            TableEntry::Kv(kv) => kv
                .visible_columns()
                .into_iter()
                .map(|(_, column)| {
                    let column = column.read();
                    (column.name.original().to_owned(), column.field_type.clone())
                })
                .collect(),
            TableEntry::View(view) => view.columns.clone(),
            // Go gives a sequence a fixed one-column schema, but no statement
            // this tier accepts ever reads it: `SELECT * FROM <sequence>` is
            // refused, and `nextval` reaches the allocator directly.
            TableEntry::Sequence(_) => Vec::new(),
        };
        columns.into_iter()
    }

    /// An owned schema for consumers that retain or modify column types.
    pub fn column_list(&self) -> Vec<(String, FieldType)> {
        self.columns().collect()
    }

    /// The table's `(name, type)` pairs in schema order, for callers outside
    /// this crate that must resolve a written column name to its TYPE — the
    /// non-prepared plan cache's filter-column rule, which refuses a filter
    /// over a JSON, `ENUM`, `SET` or `BIT` column.
    #[must_use]
    pub fn column_types(&self) -> Vec<(String, FieldType)> {
        self.column_list()
    }

    /// The table's column names in schema order, for the callers outside
    /// this crate that resolve a written column name against the table
    /// (`GRANT SELECT (a) ON db.t`).
    #[must_use]
    pub fn column_names(&self) -> Vec<String> {
        self.columns().map(|(name, _)| name).collect()
    }

    /// Whether this entry is a view, which decides which of MySQL's two
    /// object kinds a statement is allowed to name.
    #[must_use]
    pub fn is_view(&self) -> bool {
        matches!(self, TableEntry::View(_))
    }

    /// Whether this entry is a sequence, which is the other non-row object
    /// kind a statement may name by mistake.
    #[must_use]
    pub fn is_sequence(&self) -> bool {
        matches!(self, TableEntry::Sequence(_))
    }
}

impl SessionExtendedInfoSchema {
    fn database_mut(&mut self, folded_name: &str) -> Option<&mut Database> {
        Arc::make_mut(&mut self.schema_mut().databases)
            .get_mut(folded_name)
            .map(Arc::make_mut)
    }

    /// Registers a TiKV-format-byte-backed `table` in the default database.
    ///
    /// # Panics
    /// As [`SessionExtendedInfoSchema::register`].
    pub fn register_kv(&mut self, name: &str, table: TableCommon) {
        self.bump_metadata_version();
        self.register_in(DEFAULT_DATABASE, name, TableEntry::Kv(Arc::new(table)))
            .expect("the default database exists in a freshly built catalog");
    }

    /// Registers `table` in `database`, or reports Go's 1049 when that schema
    /// does not exist.
    ///
    /// The signature is the whole point: a `TableEntry` can only be handed to
    /// a schema that was found, so "the table was accepted and dropped on the
    /// floor" is not a state a caller can reach. It used to return `()` and
    /// silently do nothing for an absent schema, which made
    /// `CREATE TABLE nosuchdb.t` answer success and create nothing.
    fn register_in(
        &mut self,
        database: &str,
        name: &str,
        mut table: TableEntry,
    ) -> Result<(), DriverError> {
        let (table_is_locked, table_mode_is_set, table_has_foreign_keys) = table_flags(&table);
        let schema = self.database_mut(&database.go_to_lower()).ok_or_else(|| {
            DriverError::from(
                crate::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                    .gen_with_stack_by_args(&[(database.to_owned()).into()]),
            )
        })?;
        // Go's infoschema key and `TableInfo.Name` are one identity. Keep the
        // same invariant here so every metadata consumer (including the
        // shared planner) observes the name under which the table was
        // registered. Hand-built tables intentionally start unnamed.
        if let TableEntry::Kv(table) = &mut table {
            Arc::make_mut(table).set_name(name);
        }
        schema
            .tables
            .insert(name.go_to_lower(), std::sync::Arc::new(table));
        self.schema_mut().foreign_keys_present |= table_has_foreign_keys;
        self.schema_mut().table_locks_present |= table_is_locked;
        self.schema_mut().table_modes_present |= table_mode_is_set;
        self.bump_version();
        Ok(())
    }

    /// Every database name, sorted, with `information_schema` first when it
    /// exists -- Go's `fetchShowDatabases` ordering.
    #[must_use]
    pub fn database_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .schema.databases
            .values()
            .map(|database| database.name())
            .collect();
        names.sort();
        if let Some(position) = names
            .iter()
            .position(|name| name.go_equal_fold("information_schema"))
        {
            let front = names.remove(position);
            names.insert(0, front);
        }
        names
    }

    /// Every table name in `database`, sorted as Go's `fetchShowTables` sorts
    /// them. `None` when the database does not exist.
    ///
    /// LOCAL temporary tables are not here: they are not in the infoschema
    /// an enumeration reads (`SchemaSimpleTableInfos`).
    ///
    /// The names are Go's `TableInfo.Name.O`, the spelling the table was
    /// created (or renamed) with, in byte order as Go's `fetchShowTables`
    /// sorts them; the catalog's keys are lower-cased and only for lookup.
    #[must_use]
    pub fn table_names(&self, database: &str) -> Option<Vec<String>> {
        if let Some(v2) = self.v2().filter(|_| !infoschema_v2::is_special_db(&database.go_to_lower())) {
            v2.schema_by_name(database)?;
            let mut names: Vec<_> = v2.schema_simple_table_infos(database).iter().map(|t| t.table_name.original().to_owned()).collect();
            names.sort();
            return Some(names);
        }
        let database = self.schema.databases.get(&database.go_to_lower())?;
        let mut names: Vec<String> = database
            .tables
            .values()
            .filter_map(|entry| match entry.as_ref() {
                TableEntry::Kv(table) => Some(table.name()),
                TableEntry::View(view) => Some(view.name.clone()),
                TableEntry::Sequence(sequence) => Some(sequence.name.clone()),
            })
            .collect();
        names.sort();
        Some(names)
    }

    /// Whether `database` exists (Go `is.SchemaExists`).
    #[must_use]
    pub fn schema_exists(&self, database: &str) -> bool {
        self.schema.databases.contains_key(&database.go_to_lower())
    }

    /// The effective defaults stored on a database.
    #[must_use]
    pub fn database_charset(&self, database: &str) -> Option<TableCharset> {
        self.schema.databases
            .get(&database.go_to_lower())
            .map(|database| database.charset())
    }

    /// Go `DBInfo.PlacementPolicyRef` of `database`.
    #[must_use]
    pub fn database_placement_policy(&self, database: &str) -> Option<tidb_model::PolicyRefInfo> {
        self.schema.databases
            .get(&database.go_to_lower())?
            .placement_policy()
    }

    /// Go `onModifySchemaDefaultPlacement`'s `DBInfo` edit (or the loader
    /// restoring a stored one); `false` for a database that does not exist.
    pub fn set_database_placement_policy(
        &mut self,
        database: &str,
        policy: Option<tidb_model::PolicyRefInfo>,
    ) -> bool {
        self.bump_metadata_version();
        let Some(schema) = self.database_mut(&database.go_to_lower()) else {
            return false;
        };
        // `DBInfo` is shared by every catalog image: a change is a new one.
        let mut info = schema.info.read().clone();
        info.placement_policy_ref = policy.map(tidb_model::GoShared::new);
        schema.info = tidb_model::GoShared::new(info);
        self.bump_version();
        true
    }

    /// The stored name and defaults needed by `SHOW CREATE DATABASE`.
    #[must_use]
    pub fn database_definition(&self, database: &str) -> Option<(String, TableCharset)> {
        self.schema.databases
            .get(&database.go_to_lower())
            .map(|database| (database.name(), database.charset()))
    }

    /// Go `infoschema.PolicyByName`: the policy a name refers to, folded.
    #[must_use]
    pub fn policy(&self, name: &str) -> Option<&tidb_model::PolicyInfo> {
        self.schema.policies
            .get(&tidb_ast::CiString::new(name).lowercase().to_owned())
    }

    /// Go `InfoSchema.SchemaMetaVersion`.
    #[must_use]
    pub fn schema_meta_version(&self) -> i64 {
        self.schema.schema_meta_version
    }

    /// Records the schema version the loader built this image at.
    pub fn set_schema_meta_version(&mut self, version: i64) {
        self.schema_mut().schema_meta_version = version;
    }

    /// Go `InfoSchema.AllSchemas` without the memory schemas (whose ids carry
    /// the system flag): every stored schema's `DBInfo`, in id order. Each
    /// holds all of its tables' `TableInfo`s, non-public ones included,
    /// as Go's loader keeps them.
    #[must_use]
    pub fn stored_schemas(&self) -> Vec<tidb_model::GoShared<tidb_model::db::DBInfo>> {
        let mut schemas: Vec<_> = self
            .schema
            .databases
            .values()
            .filter(|database| database.id() & (1 << 62) == 0)
            .map(|database| database.info.clone())
            .collect();
        schemas.sort_by_key(|info| info.read().id);
        schemas
    }

    /// Go `InfoSchema.SchemaByName`: the stored schema's `DBInfo`.
    #[must_use]
    pub fn schema_by_name(&self, schema: &str) -> Option<tidb_model::GoShared<tidb_model::db::DBInfo>> {
        self.schema
            .databases
            .get(&schema.go_to_lower())
            .map(|database| database.info.clone())
    }

    /// The `TableInfo` named `table` in `schema`, from the schema's `DBInfo`
    /// (so a table in an intermediate DDL state is found too, as Go's
    /// `TableInfoByName` finds it). Go's per-schema name map keeps the table
    /// inserted last under a name, so of two tables one lowercased name
    /// covers (a repair checks no name) the one listed last answers.
    #[must_use]
    pub fn table_info_by_name(
        &self,
        schema: &str,
        table: &str,
    ) -> Option<(
        tidb_model::GoShared<tidb_model::db::DBInfo>,
        tidb_model::GoShared<tidb_model::TableInfo>,
    )> {
        if let Some(v2) = self.v2().filter(|_| !infoschema_v2::is_special_db(&schema.go_to_lower())) { v2.keep_alive(); }
        let info = self.schema_by_name(schema)?;
        if let Some(v2) = self.v2().filter(|_| !infoschema_v2::is_special_db(&schema.go_to_lower())) {
            return v2.table_by_name(schema, table, None).ok().map(|table| (info, table.info.clone()));
        }
        let found = info
            .read()
            .deprecated_tables
            .iter_handles()
            .flatten()
            .filter(|candidate| candidate.read().name.original().go_lower_eq(table))
            .last();
        found.map(|table| (info, table))
    }

    /// Every policy, for `information_schema` and `SHOW` surfaces.
    pub fn policies(&self) -> impl Iterator<Item = &tidb_model::PolicyInfo> {
        self.schema.policies.values()
    }

    /// Go `infoschema.ResourceGroupByName`, folded.
    #[must_use]
    pub fn resource_group(&self, name: &str) -> Option<&tidb_model::ResourceGroupInfo> {
        self.schema.resource_groups.get(&name.go_to_lower())
    }

    /// Every resource group, for `information_schema.RESOURCE_GROUPS`.
    pub fn resource_groups(&self) -> impl Iterator<Item = &tidb_model::ResourceGroupInfo> {
        self.schema.resource_groups.values()
    }

    /// Go `infoSchemaMisc.setResourceGroup`: a group loaded from meta, as
    /// stored.
    pub fn set_resource_group(&mut self, group: tidb_model::ResourceGroupInfo) {
        self.bump_metadata_version();
        self.schema_mut().resource_groups
            .insert(group.name.lowercase().to_owned(), group);
        self.bump_version();
    }

    /// Go `infoSchemaMisc.setPolicy`: a policy loaded from meta, as stored
    /// (the references to it are by id).
    pub fn set_policy(&mut self, policy: tidb_model::PolicyInfo) {
        self.schema_mut().policies
            .insert(policy.name.lowercase().to_owned(), policy);
        if self.is_v2() { self.update_info_schema_bundles_v2(); }
    }

    /// Go `CheckPlacementPolicyNotInUseFromInfoSchema`: the first table or
    /// partition still pointing at `name`, if any.
    ///
    /// Go refuses `DROP PLACEMENT POLICY` while anything references it
    /// (8241), because dropping it would leave those objects naming a policy
    /// that no longer exists.
    #[must_use]
    pub fn check_placement_policy_not_in_use_from_info_schema(&self, name: &str) -> bool {
        let folded = tidb_ast::CiString::new(name).lowercase().to_owned();
        if let Some(v2) = self.v2() {
            if v2.list_tables_with_special_attribute(|t| {
                t.placement_policy_ref.as_ref().is_some_and(|p| p.read().name.lowercase() == folded)
                    || t.partition.as_ref().is_some_and(|p| p.read().definitions.with_visible(|defs| defs.iter().any(|d| d.placement_policy_ref.as_ref().is_some_and(|p| p.read().name.lowercase() == folded))))
            }).iter().any(|(_, tables)| !tables.is_empty()) { return true; }
        }
        self.schema.databases.values().any(|database| {
            // Go `checkPlacementPolicyNotInUseFromInfoSchema` checks each
            // schema's default before its tables.
            database
                .placement_policy()
                .is_some_and(|reference| reference.name.lowercase() == folded)
                || database.tables.values().any(|entry| {
                    let crate::TableEntry::Kv(table) = &**entry else {
                        return false;
                    };
                    if table
                        .placement_policy()
                        .is_some_and(|reference| reference.name.lowercase() == folded)
                    {
                        return true;
                    }
                    table.meta().read().get_partition_info().is_some_and(|pi| {
                        pi.read().definitions.with_visible(|definitions| {
                            definitions.iter().any(|definition| {
                                definition
                                    .placement_policy_ref
                                    .as_ref()
                                    .is_some_and(|reference| {
                                        reference.read().name.lowercase() == folded
                                    })
                            })
                        })
                    })
                })
        })
    }

    /// Go `infoschema.Builder.createSchemaTablesForDB`'s schema half: a
    /// database loaded from meta, under its stored id, charset and collation.
    /// The mock catalog already contains `test`, `mysql` and `sys`; loading
    /// one of those replaces its synthetic id.
    ///
    /// Go's `DBInfo.Charset`/`Collate` reach `SHOW CREATE DATABASE` and
    /// `information_schema.schemata`, and are the default a table created
    /// without its own charset inherits. A loader that dropped them made
    /// `ALTER DATABASE ... CHARACTER SET` apply to the stored catalog and
    /// then be invisible to every reader.
    pub fn register_database_with_id_and_charset(
        &mut self,
        database: &str,
        id: i64,
        charset: TableCharset,
    ) -> bool {
        self.bump_metadata_version();
        let key = database.go_to_lower();
        if let Some(existing) = self.database_mut(&key) {
            let changed = existing.id() != id
                || existing.name() != database
                || existing.charset() != charset;
            if changed {
                let mut info = existing.info.read().clone();
                info.id = id;
                info.name = tidb_ast::CiString::new(database);
                info.charset = charset.charset.name().to_owned();
                info.collate = charset.collation.name().to_owned();
                existing.info = tidb_model::GoShared::new(info);
            }
            self.schema_mut().version += u64::from(changed);
            return false;
        }
        Arc::make_mut(&mut self.schema_mut().databases)
            .insert(key, Arc::new(Database::new(db_info(id, database, charset))));
        self.bump_version();
        true
    }

    /// Go `Builder.copySchemaTables` for a schema the diffs did not touch:
    /// `previous`'s schema, its tables already built, when it holds this very
    /// `DBInfo` pointer (a reload copies a `DBInfo` only when it edits it).
    /// `false`, adopting nothing, otherwise.
    pub fn adopt_built_database(
        &mut self,
        previous: &SessionExtendedInfoSchema,
        info: &tidb_model::GoShared<tidb_model::db::DBInfo>,
    ) -> bool {
        let key = info.read().name.lowercase().to_owned();
        let Some(database) = previous
            .schema
            .databases
            .get(&key)
            .filter(|database| database.info.ptr_eq(info))
        else {
            return false;
        };
        self.bump_metadata_version();
        Arc::make_mut(&mut self.schema_mut().databases).insert(key, Arc::clone(database));
        // The flags are sticky: a stale `true` only costs a scan.
        self.schema_mut().foreign_keys_present |= previous.schema.foreign_keys_present;
        self.schema_mut().table_locks_present |= previous.schema.table_locks_present;
        self.schema_mut().table_modes_present |= previous.schema.table_modes_present;
        self.bump_version();
        true
    }

    /// Indexes the source metadata once for a database whose tables may be
    /// reused while applying a schema diff.
    pub fn built_table_templates(&self, database: &str) -> Option<BuiltTableTemplates<'_>> {
        let (database_name, built) = self.schema.databases.get_key_value(&database.go_to_lower())?;
        let sources = built
            .info
            .read()
            .deprecated_tables
            .iter_deref()
            .map(|table| (table.identity_address(), table))
            .collect();
        Some(BuiltTableTemplates {
            database_name,
            database: built,
            sources,
        })
    }

    /// Go `Builder.ApplyDiff` for a table no diff names: the table `previous`
    /// built from this very `TableInfo` pointer (one of its schema's
    /// `Deprecated.Tables`) joins this image unchanged. `false` when
    /// `previous` built nothing from it.
    pub fn adopt_built_table(
        &mut self,
        previous: &SessionExtendedInfoSchema,
        database: &str,
        table: &tidb_model::GoShared<tidb_model::TableInfo>,
    ) -> bool {
        let database_key = database.go_to_lower();
        let Some(built) = previous.schema.databases.get(&database_key) else {
            return false;
        };
        let built_from_it = built
            .info
            .read()
            .deprecated_tables
            .iter_deref()
            .any(|built_table| built_table.ptr_eq(table));
        if !built_from_it {
            return false;
        }
        let name = table.read().name.lowercase().to_owned();
        let Some(entry) = built.tables.get(&name).cloned() else {
            return false;
        };
        let (table_is_locked, table_mode_is_set, table_has_foreign_keys) = table_flags(&entry);
        self.bump_metadata_version();
        let Some(schema) = self.database_mut(&database_key) else {
            return false;
        };
        schema.tables.insert(name, entry);
        self.schema_mut().foreign_keys_present |= table_has_foreign_keys;
        self.schema_mut().table_locks_present |= table_is_locked;
        self.schema_mut().table_modes_present |= table_mode_is_set;
        self.bump_version();
        true
    }

    /// Loads the stored table handles so runtime entries can retain their
    /// corresponding Go metadata pointer.
    pub fn load_stored_schema_tables_from_handles(
        &mut self,
        info: tidb_model::GoShared<tidb_model::db::DBInfo>,
        templates: Option<&BuiltTableTemplates<'_>>,
        mut build: impl FnMut(&tidb_model::GoShared<tidb_model::TableInfo>) -> Option<TableEntry>,
    ) {
        let (database_key, stored) = {
            let info = info.read();
            (info.name.original().go_to_lower(), info.deprecated_tables.clone())
        };
        self.create_schema_tables_for_db(info);
        let mut database = self.database_mut(&database_key);
        if let Some(database) = &mut database {
            database.tables.reserve(stored.len());
        }
        let mut added = 0;
        let (mut locked, mut modes, mut foreign_keys) = (false, false, false);
        let templates = templates.filter(|templates| templates.database_name == database_key);
        for table in stored.iter_deref() {
            let mut stored_flags = None;
            let (name, entry) = if let Some(reused) = templates.and_then(|t| t.entry_for(&table)) {
                reused
            } else {
                let Some(mut entry) = build(&table) else {
                    continue;
                };
                let info = table.read();
                let name = info.name.original();
                let key = name.go_to_lower();
                if let TableEntry::Kv(ref mut runtime) = entry {
                    if !runtime.meta().ptr_eq(&table) {
                        Arc::make_mut(runtime).set_name(name);
                    } else {
                        if info.name.lowercase() != key {
                            let name = name.to_owned();
                            drop(info);
                            Arc::make_mut(runtime).set_name(&name);
                        } else {
                            stored_flags = Some(table_flags_from_meta(&info));
                        }
                    }
                }
                (key, Arc::new(entry))
            };
            let (table_locked, table_mode, table_foreign_keys) =
                stored_flags.unwrap_or_else(|| table_flags(&entry));
            locked |= table_locked;
            modes |= table_mode;
            foreign_keys |= table_foreign_keys;
            database
                .as_deref_mut()
                .expect("a stored schema's name resolves to its database")
                .tables
                .insert(name, entry);
            added += 1;
        }
        // This unpublished database is filled under one metadata version.
        // Preserve the mutation count and the sticky catalog-wide flags.
        let schema = self.schema_mut();
        schema.version += added;
        schema.table_locks_present |= locked;
        schema.table_modes_present |= modes;
        schema.foreign_keys_present |= foreign_keys;
    }

    /// Go `Builder.createSchemaTablesForDB`: the schema `info` names, holding
    /// that very `DBInfo` pointer. `false` when the schema already existed
    /// (its pointer is replaced).
    pub fn create_schema_tables_for_db(&mut self, info: tidb_model::GoShared<tidb_model::db::DBInfo>) -> bool {
        self.bump_metadata_version();
        let key = info.read().name.lowercase().to_owned();
        if let Some(existing) = self.database_mut(&key) {
            existing.info = info;
            self.bump_version();
            return false;
        }
        Arc::make_mut(&mut self.schema_mut().databases).insert(
            key,
            Arc::new(Database {
                info,
                tables: CatalogMap::default(),
            }),
        );
        self.bump_version();
        true
    }

    /// Go `Builder.InitWithDBInfos` without the `table.Table` objects: the
    /// memory schemas, then the stored schemas holding these very `DBInfo`
    /// pointers (each `Deprecated.Tables` holding its tables), the policies,
    /// the resource groups and the schema version the loader read. The
    /// domain's builder adds the tables (`cluster_session_catalog`).
    #[must_use]
    pub fn init_with_db_infos(
        databases: Vec<tidb_model::GoShared<tidb_model::db::DBInfo>>,
        policies: Vec<tidb_model::PolicyInfo>,
        resource_groups: Vec<tidb_model::ResourceGroupInfo>,
        schema_version: i64,
    ) -> Self {
        let mut schema = Self::new();
        schema.set_schema_meta_version(schema_version);
        for group in resource_groups {
            schema.set_resource_group(group);
        }
        for policy in policies {
            schema.set_policy(policy);
        }
        for database in databases {
            schema.create_schema_tables_for_db(database);
        }
        schema
    }

    /// Go `Builder.WithCrossKS(true).InitWithDBInfos`: omit virtual schemas.
    pub fn init_with_db_infos_for_cross_ks(
        databases: Vec<tidb_model::GoShared<tidb_model::db::DBInfo>>,
        policies: Vec<tidb_model::PolicyInfo>,
        resource_groups: Vec<tidb_model::ResourceGroupInfo>,
        schema_version: i64,
    ) -> Self {
        let mut schema = Self::init_with_db_infos(databases, policies, resource_groups, schema_version);
        Arc::make_mut(&mut schema.schema_mut().databases).retain(|name, _| !tidb_metadef::is_mem_db(name));
        schema
    }

    /// Go `Builder.InitWithOldInfoSchema` without the `table.Table` objects:
    /// a schema over this one's `DBInfo` pointers, policies, resource groups
    /// and version, which a reload applies diffs to before the domain's
    /// builder builds its tables.
    #[must_use]
    pub fn metadata_copy(&self) -> Self {
        let mut copied = Self::init_with_db_infos(
            self.stored_schemas(),
            self.policies().cloned().collect(),
            self.resource_groups().cloned().collect(),
            self.schema_meta_version(),
        );
        copied.schema_mut().schema_ts = self.schema.schema_ts;
        copied.schema_mut().uses_infoschema_v2 = self.schema.uses_infoschema_v2;
        copied.schema_mut().masking_policies = Arc::new(self.schema.masking_policies.clone_for_builder());
        copied.v2_origin = self.v2_origin.clone();
        copied.v2_steps = self.v2_steps.clone();
        copied.set_schema_commit_ts(self.schema_commit_ts());
        copied.schema_mut().empty_schema_versions = self.schema.empty_schema_versions.clone();
        copied.schema_mut().foreign_keys_present = self.schema.foreign_keys_present;
        copied.schema_mut().table_locks_present = self.schema.table_locks_present;
        copied.schema_mut().table_modes_present = self.schema.table_modes_present;
        copied
    }

    /// Go `Builder.enableV2`, used by the masking policy cache's builder path.
    pub fn uses_infoschema_v2(&self) -> bool {
        self.schema.uses_infoschema_v2
    }

    /// Go `NewBuilder(..., useV2)`.
    pub fn set_uses_infoschema_v2(&mut self, use_v2: bool) {
        self.schema_mut().uses_infoschema_v2 = use_v2;
    }

    /// Go `Builder.Build(schemaTS)`.
    /// Go `infoschemaV2.CloneAndUpdateTS` for a V2 image.
    pub fn set_schema_ts(&mut self, ts: u64) {
        self.schema_mut().schema_ts = ts;
        if let Some(v2) = &mut self.schema_mut().v2 {
            v2.ts = ts;
        }
        let mut cache = self.schema.masking_policies.clone_for_builder();
        cache.set_snapshot_ts(ts);
        self.schema_mut().masking_policies = Arc::new(cache);
    }

    /// Go `infoSchema.ts`.
    pub fn schema_ts(&self) -> u64 {
        self.schema.schema_ts
    }

    /// Go `Builder.factory` for lazy system-table reads on this image.
    pub fn set_masking_policy_factory(
        &mut self,
        factory: masking_policy::MaskingPolicySessionFactory,
    ) {
        self.schema_mut().masking_policies = Arc::new(
            self.schema
                .masking_policies
                .clone_for_builder()
                .with_factory_and_ts(Some(factory), self.schema.schema_ts),
        );
    }

    /// Go's table builder retains the schema's cache when building table objects.
    pub fn inherit_masking_policy_cache(&mut self, source: &Self) {
        self.schema_mut().masking_policies = Arc::clone(&source.schema.masking_policies);
        self.schema_mut().schema_ts = source.schema.schema_ts;
        self.schema_mut().uses_infoschema_v2 = source.schema.uses_infoschema_v2;
    }

    /// Go `infoSchema.resetMaskingPolicyCache` on a new schema image.
    pub fn reset_masking_policy_cache(&mut self) {
        let mut cache = self.schema.masking_policies.clone_for_builder();
        cache.reset_masking_policy_cache();
        self.schema_mut().masking_policies = Arc::new(cache);
    }

    /// Go `infoSchema.CloneMaskingPoliciesByTableColumn`.
    pub fn clone_masking_policies_by_table_column(
        &self,
    ) -> std::collections::BTreeMap<
        i64,
        std::collections::BTreeMap<i64, tidb_model::MaskingPolicyInfo>,
    > {
        self.schema
            .masking_policies
            .clone_masking_policies_by_table_column()
    }

    /// Go `infoSchema.MaskingPolicyByTableColumn`.
    pub fn masking_policy_by_table_column(
        &self,
        table_id: i64,
        column_id: i64,
    ) -> Option<tidb_model::MaskingPolicyInfo> {
        self.schema
            .masking_policies
            .masking_policy_by_table_column(table_id, column_id)
    }

    /// Go `infoSchema.MaskingPolicyByID`.
    pub fn masking_policy_by_id(&self, id: i64) -> Option<tidb_model::MaskingPolicyInfo> {
        self.schema.masking_policies.masking_policy_by_id(id)
    }

    /// Go `infoSchema.MaskingPolicyByName`.
    pub fn masking_policy_by_name(
        &self,
        name: &tidb_ast::CiString,
    ) -> Option<tidb_model::MaskingPolicyInfo> {
        self.schema.masking_policies.masking_policy_by_name(name)
    }

    /// Go `infoSchema.AllMaskingPolicies`.
    pub fn all_masking_policies(&self) -> Vec<tidb_model::MaskingPolicyInfo> {
        self.schema.masking_policies.all_masking_policies()
    }

    /// Go `InfoSchema.SchemaByID`: the schema with `id`, stored or memory.
    #[must_use]
    pub fn schema_by_id(&self, id: i64) -> Option<tidb_model::GoShared<tidb_model::db::DBInfo>> {
        self.schema
            .databases
            .values()
            .find(|database| database.id() == id)
            .map(|database| database.info.clone())
    }

    /// Go `applyDropSchema`'s `delete(b.infoSchema.schemaMap, di.Name.L)`:
    /// the stored schema with `id` leaves the image; its `DBInfo` is answered.
    pub fn drop_schema_by_id(&mut self, id: i64) -> Option<tidb_model::GoShared<tidb_model::db::DBInfo>> {
        let key = self
            .schema
            .databases
            .iter()
            .find(|(_, database)| database.id() == id)
            .map(|(key, _)| key.clone())?;
        self.bump_metadata_version();
        let dropped = Arc::make_mut(&mut self.schema_mut().databases).swap_remove(&key)?;
        self.bump_version();
        Some(dropped.info.clone())
    }

    /// Go `Builder.copySchemaTables` then an edit of the copy: the stored
    /// schema with `id` holds a copy of its `DBInfo` that `edit` changed,
    /// never a change through the pointer an older image shares. `false`
    /// when no schema has that id.
    pub fn edit_schema_by_id(&mut self, id: i64, edit: impl FnOnce(&mut tidb_model::db::DBInfo)) -> bool {
        let Some(info) = self
            .schema
            .databases
            .values()
            .find(|database| database.id() == id)
            .map(|database| database.info.clone())
        else {
            return false;
        };
        let mut copy = info.read().clone();
        edit(&mut copy);
        // The copy keeps its name (no DDL renames a schema), so it replaces
        // the original in its slot.
        self.create_schema_tables_for_db(tidb_model::GoShared::new(copy));
        true
    }

    /// Go `applyDropPolicy`'s `deletePolicy`: the policy with `id` leaves the
    /// map.
    pub fn delete_policy_by_id(&mut self, id: i64) {
        self.schema_mut().policies.retain(|_, policy| policy.id != id);
        if self.is_v2() { self.update_info_schema_bundles_v2(); }
    }

    /// Go `applyDropResourceGroup`'s `deleteResourceGroup`: the group with
    /// `id` leaves the map.
    pub fn delete_resource_group_by_id(&mut self, id: i64) {
        self.bump_metadata_version();
        self.schema_mut().resource_groups.retain(|_, group| group.id != id);
        self.bump_version();
    }

    /// The stored tables this tier could not build into this image.
    #[must_use]
    pub fn skipped_tables(&self) -> &[cluster_catalog::SkippedTable] {
        &self.schema.skipped
    }

    /// Records the stored tables the domain's builder could not build.
    pub fn set_skipped_tables(&mut self, skipped: Vec<cluster_catalog::SkippedTable>) {
        self.schema_mut().skipped = skipped.into();
    }

    /// Resolves a table in `database`.
    /// The CLUSTER table id behind a stored table's name, or `None` for a
    /// name that is no stored table here (a view, a CTE, a memory table, an
    /// unknown name). The id is the one TiKV keys carry and the one Go's
    /// `mysql.tidb_mdl_info.table_ids` names, which is what the metadata-lock
    /// gate matches on.
    #[must_use]
    pub fn stored_table_id(&self, database: &str, name: &str) -> Option<i64> {
        match self.get_in(database, name)? {
            TableEntry::Kv(table) => Some(table.table_id()),
            _ => None,
        }
    }

    /// Go `preprocessor.tableByName` then `tryLockMDLAndUpdateSchemaIfNecessary`
    /// for one name the statement writes: the id it locks, `Some(None)` for
    /// an object with no TiKV table id (a memory table, a sequence, a view
    /// with no stored id), and `None` when the name resolves to nothing. The
    /// tables a view reads are locked by the planner as it builds the view
    /// (`buildDataSource`).
    pub fn related_table_id_for_mdl(&self, database: &str, name: &str) -> Option<Option<i64>> {
        match self.get_in(database, name)? {
            TableEntry::Kv(table) => Some(Some(table.table_id())),
            TableEntry::View(view) => Some((view.id != 0).then_some(view.id)),
            TableEntry::Sequence(_) => Some(None),
        }
    }

    pub fn get_in(&self, database: &str, name: &str) -> Option<&TableEntry> {
        self.get_by_key(&CatalogTableKey::new(database, name))
    }

    #[inline]
    pub fn get_by_key(&self, key: &CatalogTableKey) -> Option<&TableEntry> {
        self.table_handle_ref(key).map(|entry| &**entry)
    }

    /// Retains immutable statement metadata without cloning the table or its store.
    pub fn table_handle_by_key(&self, key: &CatalogTableKey) -> Option<Arc<TableEntry>> {
        self.try_table_handle_by_key(key).ok().flatten()
    }

    /// Go `SessionExtendedInfoSchema.TableByName`: the session's LOCAL
    /// temporary table of that name, else the shared schema's entry.
    fn table_handle_ref(&self, key: &CatalogTableKey) -> Option<&Arc<TableEntry>> {
        self.local_temporary_entry(key)
            .or_else(|| self.schema_entry(key))
    }

    /// The shared schema's entry, without the session's temporary tables
    /// (Go `DetachLocalTemporaryTableInfoSchema`).
    fn schema_entry(&self, key: &CatalogTableKey) -> Option<&Arc<TableEntry>> {
        self.schema.databases.get(&key.database).and_then(|db| db.tables.get(&key.table))
            .or_else(|| self.v2().and_then(|v2| self.v2_pins.get(v2, key).ok().flatten()).map(|t| &t.table))
    }

    fn local_temporary_entry(&self, key: &CatalogTableKey) -> Option<&Arc<TableEntry>> {
        self.local_temporary
            .iter()
            .find(|(local, _)| local == key)
            .map(|(_, entry)| entry)
    }

    fn get(&self, name: &str) -> Option<&TableEntry> {
        self.get_in(DEFAULT_DATABASE, name)
    }

    /// Borrows current table metadata without copying names or resolving
    /// them through the catalog again. Callers impose ordering only on the
    /// entries they need; hash-map traversal itself is unordered.
    pub fn table_entries(&self) -> impl Iterator<Item = (String, &str, &TableEntry)> {
        self.schema.databases.values().flat_map(|database| {
            let schema = database.name();
            database
                .tables
                .iter()
                .map(move |(name, entry)| (schema.clone(), name.as_str(), entry.as_ref()))
        }).chain({
            let mut items = Vec::new();
            if let Some(v2) = self.v2() { v2.iterate_all_table_items(|item| { items.push(item); true }); }
            items.into_iter().filter_map(|item| {
                let key = CatalogTableKey::new(item.db_name.lowercase(), item.table_name.lowercase());
                if self.schema.databases.get(&key.database).is_some_and(|db| db.tables.contains_key(&key.table)) { return None; }
                let (key, table) = self.v2_pins.get_entry(self.v2()?, &key, true).ok()??;
                Some((key.database.clone(), key.table.as_str(), table.table.as_ref()))
            })
        })
    }

    /// Whether any stored table declares a foreign key.
    ///
    /// The DML executor checks parent-side referential actions for every
    /// mutation. Most OLTP catalogs have no foreign keys, so keep this as a
    /// sticky bit instead of inspecting every table on every write. Once a
    /// foreign key has existed, retaining `true` is safe: it may perform one
    /// extra table-path scan after the constraint is dropped, but never skips
    /// a required referential check.
    pub fn has_foreign_keys(&self) -> bool {
        self.schema.foreign_keys_present
    }

    /// Whether any table of this catalog may carry a `LOCK TABLES` lock.
    /// `false` lets Go's `CheckTableLock` answer without reading a table:
    /// with no lock anywhere and none held by the session, every arm of
    /// `lock.Checker` returns nil.
    #[must_use]
    pub fn has_table_locks(&self) -> bool {
        self.schema.table_locks_present
    }

    /// Whether any table of this catalog may be in a mode other than
    /// `TableModeNormal`. `false` lets Go's `CheckTableMode` answer without
    /// resolving a table name.
    #[must_use]
    pub fn has_table_modes(&self) -> bool {
        self.schema.table_modes_present
    }

    /// Go `core.CheckTableMode`'s loop over the statement's resolved table
    /// names (`resolve.Context.GetTableNames`): the first of `names`, as
    /// written and resolved against `current_db`, that is not in
    /// `TableModeNormal`, with its mode -- the arguments of the
    /// `ErrProtectedTableMode` that [`check_table_mode_is_normal`] raises.
    #[must_use]
    pub fn protected_table_mode(
        &self,
        names: &[Vec<String>],
        current_db: &str,
    ) -> Option<(String, tidb_model::TableMode)> {
        if !self.has_table_modes() {
            return None;
        }
        names.iter().find_map(|path| {
            let (database, name) = split_table_path(path, current_db).ok()?;
            let Some(TableEntry::Kv(table)) = self.table_in(database, name) else {
                return None;
            };
            let mode = table.meta().read().mode;
            (mode != tidb_model::TableMode::NORMAL).then(|| (name.to_owned(), mode))
        })
    }

    /// Go `DBInfo.ID` of `database`.
    #[must_use]
    pub fn database_id(&self, database: &str) -> Option<i64> {
        self.schema.databases
            .get(&database.go_to_lower())
            .map(|database| database.id())
    }

    /// The name of the database whose Go `DBInfo.ID` is `id`, as Go's
    /// `is.SchemaByID` finds it.
    #[must_use]
    pub fn database_name_by_id(&self, id: i64) -> Option<String> {
        self.schema.databases
            .values()
            .find(|schema| schema.id() == id)
            .map(|database| database.name())
    }

    /// The `(schema, table)` names of the stored table whose Go
    /// `TableInfo.ID` is `table_id`, as Go's `is.TableByID` finds it.
    #[must_use]
    pub fn table_names_by_id(&self, table_id: i64) -> Option<(String, String)> {
        // Memory tables are registered here under ID 0, which no Go table
        // has.
        if table_id == 0 {
            return None;
        }
        let (database, table) = self
            .table_by_id(table_id)
            .filter(|(_, table)| table.table_id() == table_id)?;
        let database = self
            .schema.databases
            .get(database)
            .map_or_else(|| database.to_owned(), |schema| schema.name());
        Some((database, table.name()))
    }

    /// Go `infoschema.FindTableByPartitionID`: the schema and table that own
    /// partition `partition_id`, or `None` when no table has a partition by
    /// that ID (a table's own ID is not a partition's).
    pub fn find_table_by_partition_id(&self, partition_id: i64) -> Option<(String, String)> {
        if let Some(v2) = self.v2() {
            let item = v2.table_item_by_partition_id(partition_id)?;
            let table = v2.table_by_id(item.table_id, None)?;
            return Some((item.db_name.original().to_owned(), table.info.read().name.original().to_owned()));
        }
        let (_, schema, entry) = self.v1_table_entry_by_id(partition_id)?;
        match entry {
            TableEntry::Kv(table)
                if table.meta().read().get_partition_info().is_some_and(|pi| {
                    pi.read()
                        .definitions
                        .with_visible(|definitions| {
                            definitions.iter().any(|definition| definition.id == partition_id)
                        })
                }) =>
            {
                Some((schema.name(), table.name()))
            }
            _ => None,
        }
    }

    /// Every table carrying a `TableInfo.Lock`, with its schema name -- Go
    /// `ListTablesWithSpecialAttribute(TableLockAttribute)`.
    pub fn locked_tables(&self) -> impl Iterator<Item = (String, &crate::TableCommon)> {
        let mut result = Vec::new();
        if self.schema.table_locks_present {
            if let Some(v2) = self.v2() {
                for (schema, tables) in v2.list_tables_with_special_attribute(|t| t.lock.is_some()) {
                    for table in tables {
                        let name = table.read().name.original().to_owned();
                        if let Some(TableEntry::Kv(table)) = self.table_in(&schema, &name) {
                            result.push((schema.clone(), table.as_ref()));
                        }
                    }
                }
            } else {
                result.extend(self.table_entries().filter_map(|(schema, _, entry)| match entry {
                    TableEntry::Kv(table) if table.lock().is_some() => Some((schema, &**table)),
                    _ => None,
                }));
            }
        }
        result.into_iter()
    }

    /// A mutable handle on a table of `database`, for the write paths.
    ///
    /// Taking it bumps [`SessionExtendedInfoSchema::version`], which is what a transaction's
    /// conflict check observes. The count is deliberately over-approximate:
    /// every write path goes through here, so a statement that ends up
    /// changing nothing still bumps it. That can refuse a commit Go would
    /// allow, never the reverse.
    pub fn get_mut_in(&mut self, database: &str, name: &str) -> Option<&mut TableEntry> {
        self.planner_view.take();
        self.get_rows_mut_in(database, name)
    }

    /// [`SessionExtendedInfoSchema::get_mut_in`] for the paths that write rows (DML, foreign-key
    /// actions, the stats-delta flush): it bumps [`SessionExtendedInfoSchema::version`] the
    /// same way but keeps the planner's schema view, which carries schema
    /// metadata only -- Go's `InfoSchema` does not move when rows do. A
    /// caller must not change the table's schema through this handle; a
    /// schema change takes [`SessionExtendedInfoSchema::table_mut_in`].
    pub fn get_rows_mut_in(
        &mut self,
        database: &str,
        name: &str,
    ) -> Option<&mut TableEntry> {
        self.schema_mut().version += 1;
        let key = CatalogTableKey::new(database, name);
        if self.is_v2() && self.local_temporary_entry(&key).is_none() && !self.schema.databases.get(&key.database).is_some_and(|db| db.tables.contains_key(&key.table)) {
            let table = self.try_table_handle_by_key(&key).ok().flatten()?;
            self.database_mut(&key.database)?.tables.insert(key.table.clone(), table);
        }
        let entry = match self.local_temporary.iter().position(|(local, _)| *local == key) {
            Some(position) => &mut self.local_temporary[position].1,
            None => self.database_mut(&key.database)?.tables.get_mut(&key.table)?,
        };
        // Go's write paths build a new `TableInfo` rather than editing the
        // shared one; `make_mut` is the same copy-on-write at the entry level.
        Some(Arc::make_mut(entry))
    }

    /// The catalog's mutation counter.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.schema.version
    }


    /// Process-local identity of this schema metadata image. Unlike a local
    /// mutation count, this cannot collide when a cluster catalog is rebuilt
    /// with the same number of tables. Clones retain it until metadata changes;
    /// DML leaves it unchanged, like Go's InfoSchema version cache boundary.
    /// This is an internal cache key, not the persisted cluster schema version.
    #[must_use]
    pub fn metadata_version(&self) -> u64 {
        self.schema.metadata_version
    }

    /// Go `InfoSchema.SchemaMetaVersion` as a plan-cache key and plan
    /// validity check read it: the metadata version of the shared image,
    /// which a session attaching its LOCAL temporary tables does not move
    /// (`SessionExtendedInfoSchema` keeps the inner version). A statement
    /// reading a temporary table is refused by the cache on its own.
    #[must_use]
    pub fn schema_version(&self) -> u64 {
        self.overlay_versions
            .map_or(self.schema.metadata_version, |(base, _)| base)
    }

    /// Advances the mutation counter and drops the derived views that
    /// belong to the image it counted.
    ///
    /// Every handle that can change a table takes this, `get_mut_in`
    /// included: a caller holding `&mut TableEntry` may add an index (Go's
    /// DDL owns that, but this tier hands the entry out), so the planner's
    /// view cannot outlive a version. `table_id_names` does not move here:
    /// it maps a table's id to its position in the maps, which only a
    /// schema-changing operation can move -- `bump_metadata_version` below.
    /// An ordinary DML
    /// write (every `get_mut_in` caller in production) never reassigns a
    /// table's id or its key, so it does not owe this cache a rebuild.
    fn bump_version(&mut self) {
        self.schema_mut().version += 1;
        self.planner_view.take();
    }

    fn bump_metadata_version(&mut self) {
        self.schema_mut().metadata_version = next_metadata_version();
        self.schema_mut().latest_index_schema.take();
        self.schema_mut().v1_metadata_by_id = None;
        self.planner_view.take();
        self.statistics_schemas = Arc::default();
        self.invalidate_table_id_names();
    }

    fn invalidate_table_id_names(&mut self) {
        if let Some(names) = Arc::get_mut(&mut self.table_id_names) {
            names.take();
        } else {
            self.table_id_names = Arc::default();
        }
    }

    /// Captures only the immutable schema metadata needed by `TIDB_DECODE_KEY`.
    #[must_use]
    pub fn tidb_decode_key_snapshot(&self) -> crate::TidbDecodeKeySnapshot {
        let mut snapshot = crate::TidbDecodeKeySnapshot::default();
        snapshot.set_lazy_reader(self.v2().cloned());
        for (database_name, database) in self.schema.databases.iter() {
            for (table_name, entry) in &database.tables {
                if let TableEntry::Kv(table) = &**entry {
                    snapshot.insert_table(table);
                }
                snapshot.insert_name(database_name, table_name, Arc::clone(entry));
            }
        }
        snapshot
    }

    /// A mutable table of `database`, for the schema-changing statements.
    pub fn table_mut_in(&mut self, database: &str, name: &str) -> Option<&mut TableEntry> {
        // Go advances InfoSchema.SchemaMetaVersion whenever a DDL job changes
        // table metadata. Keep that epoch separate from `version`, which also
        // moves for ordinary row writes and transaction conflict detection.
        // Every production caller of this accessor is a schema-changing DDL
        // path; DML reaches the deliberately narrower `get_mut_in` instead.
        self.bump_metadata_version();
        self.get_mut_in(database, name)
    }

    /// A table of `database`, for the metadata statements.
    #[must_use]
    pub fn table_in(&self, database: &str, name: &str) -> Option<&TableEntry> {
        self.get_in(database, name)
    }

    /// The base table selected by a retained physical access node.
    ///
    /// Go's executor builder resolves a `PhysicalTableScan.Table.ID` through
    /// the statement's InfoSchema. The cached Rust physical tree owns the
    /// same stable ID, so executor construction must not recover the table by
    /// walking SQL names or aliases again.
    pub fn kv_table_by_id(&self, table_id: i64) -> Option<&crate::TableCommon> {
        self.table_by_id(table_id)
            .map(|(_, table)| table)
            .filter(|table| table.table_id() == table_id)
            .map(|table| &**table)
    }

    /// Go `SessionExtendedInfoSchema.TableByID` (or `FindTableByPartitionID`
    /// for a partition's id): the session's LOCAL temporary table with that
    /// id, else the shared schema's table owning it, with its folded
    /// database.
    pub fn table_by_id(
        &self,
        physical_id: i64,
    ) -> Option<(&str, &Arc<crate::TableCommon>)> {
        let local = self.local_temporary.iter().find_map(|(key, entry)| match entry.as_ref() {
            TableEntry::Kv(table) if table.table_id() == physical_id => {
                Some((key.database.as_str(), table))
            }
            _ => None,
        });
        local.or_else(|| {
            if let Some(v2) = self.v2() {
                let Some(item) = v2.table_item_by_partition_id(physical_id).or_else(|| v2.table_item_by_id(physical_id)) else {
                    // Go `SessionExtendedInfoSchema.TableByID` reads `MdlTables`
                    // first: a table the transaction pinned that its snapshot
                    // lacks (an MLog created since it began) is only there.
                    return self.schema.databases.iter().find_map(|(database_name, database)| {
                        database.tables.values().find_map(|entry| match entry.as_ref() {
                            TableEntry::Kv(table) if table.table_id() == physical_id => Some((database_name.as_str(), table)),
                            _ => None,
                        })
                    });
                };
                let key = CatalogTableKey::new(item.db_name.lowercase(), item.table_name.lowercase());
                if let Some((database_name, database)) = self.schema.databases.get_key_value(&key.database) {
                    if let Some(entry) = database.tables.get(&key.table) {
                        if let TableEntry::Kv(table) = entry.as_ref() {
                            return Some((database_name.as_str(), table));
                        }
                    }
                }
                let (key, table) = self.v2_pins.get_entry(v2, &key, false).ok()??;
                return match table.table.as_ref() {
                    TableEntry::Kv(table) => Some((key.database.as_str(), table)), _ => None,
                };
            }
            let (database, _, entry) = self.v1_table_entry_by_id(physical_id)?;
            match entry {
                TableEntry::Kv(table) => Some((database, table)),
                _ => None,
            }
        })
    }

    fn v1_table_entry_by_id(&self, id: i64) -> Option<(&str, &Database, &TableEntry)> {
        let &(database, table) = self.table_id_names().by_id.get(&id)?;
        let (name, database) = self.schema.databases.get_index(database)?;
        let (_, entry) = database.tables.get_index(table)?;
        Some((name, database, entry))
    }

    /// Builds the v1 table-ID lookups before publishing the catalog, as Go's
    /// builder does. Identical stored schemas can share their lookups.
    pub fn prepare_v1_table_id_lookup(&mut self, previous: Option<&Self>) {
        debug_assert!(!self.is_v2());
        let previous = previous.filter(|previous| {
            previous.table_id_names.get().is_some() && self.has_same_tables(previous)
        });
        if let Some(previous) = previous {
            self.table_id_names = Arc::clone(&previous.table_id_names);
        } else {
            self.table_id_names();
        }
        let metadata = previous
            .and_then(|previous| previous.schema.v1_metadata_by_id.clone())
            .unwrap_or_else(|| Arc::new(self.build_v1_metadata_by_id()));
        self.schema_mut().v1_metadata_by_id = Some(metadata);
    }

    fn build_v1_metadata_by_id(&self) -> V1StoredTableIndex {
        let schemas = self.stored_schemas();
        let count = schemas.iter().map(|schema| schema.read().deprecated_tables.len()).sum();
        let mut tables = HashMap::with_capacity_and_hasher(count, foldhash::fast::RandomState::default());
        for (schema_index, schema) in schemas.iter().enumerate() {
            let result: std::ops::ControlFlow<()> = schema.read().deprecated_tables.visit_deref(|table, info| {
                tables.entry(info.id).or_insert_with(|| (schema_index, table.clone()));
                std::ops::ControlFlow::Continue(())
            });
            debug_assert!(result.is_continue());
        }
        V1StoredTableIndex { schemas, tables }
    }

    fn has_same_tables(&self, previous: &Self) -> bool {
        let databases = &self.schema.databases;
        let old_databases = &previous.schema.databases;
        databases.len() == old_databases.len()
            && databases.iter().zip(old_databases.iter()).all(
                |((name, database), (old_name, old))| {
                    name == old_name
                        && (Arc::ptr_eq(database, old)
                            // The builder recreates the virtual schema maps from
                            // the same process-wide table objects.
                            || (tidb_metadef::is_mem_db(name)
                                && database.tables.len() == old.tables.len()
                                && database.tables.iter().zip(old.tables.iter()).all(
                                    |((name, table), (old_name, entry))| {
                                        name == old_name && Arc::ptr_eq(table, entry)
                                    },
                                )))
                },
            )
    }

    fn table_id_names(&self) -> &V1TableNameIndex {
        self.table_id_names.get_or_init(|| {
            let table_count = self.schema.databases.values().map(|db| db.tables.len()).sum();
            let mut index = V1TableNameIndex {
                by_id: HashMap::with_capacity_and_hasher(
                    table_count,
                    foldhash::fast::RandomState::default(),
                ),
            };
            for (database_index, database) in self.schema.databases.values().enumerate() {
                for (table_index, entry) in database.tables.values().enumerate() {
                    let TableEntry::Kv(table) = entry.as_ref() else {
                        continue;
                    };
                    let position = (database_index, table_index);
                    index.by_id.insert(table.table_id(), position);
                    if let Some(pi) = table.read_meta().get_partition_info() {
                        pi.read().definitions.with_visible(|definitions| {
                            index.by_id.extend(definitions.iter().map(|part| (part.id, position)));
                        });
                    }
                }
            }
            index
        })
    }

    /// Resolves Go's physical table ID to a reader handle. A partition ID
    /// names its logical table with the read restricted to that one physical
    /// keyspace, matching executorBuilder's partition-table lookup.
    ///
    /// The common, unpartitioned case (`physical_id == table.table_id`,
    /// every table in most schemas) returns a cheap `Arc::clone` of the
    /// table already sitting in this transaction's own catalog view --
    /// no `TableCommon` clone at all, so no `staged_record_keys` deep copy
    /// either. Only the partitioned branch pays `Arc::make_mut`'s clone,
    /// since restricting reads to one partition needs an independent copy.
    pub fn physical_kv_table_by_id(&self, physical_id: i64) -> Option<Arc<crate::TableCommon>> {
        let (_, table) = self.table_by_id(physical_id)?;
        let mut physical = Arc::clone(table);
        if table.table_id() != physical_id {
            Arc::make_mut(&mut physical).restrict_read_to_partitions(&[physical_id]);
        }
        Some(physical)
    }

    /// Resolves a logical or partition table ID to its owning database name.
    pub fn physical_kv_table_database_by_id(&self, physical_id: i64) -> Option<String> {
        let (database, _) = self.table_by_id(physical_id)?;
        Some(self.schema.databases.get(database)?.name())
    }

    /// A table of the default database, for tests that inspect the entry.
    #[must_use]
    pub fn get_table_for_test(&self, name: &str) -> Option<&TableEntry> {
        self.get(name)
    }

    /// Whether `database` holds a table called `name`.
    #[must_use]
    pub fn contains_in(&self, database: &str, name: &str) -> bool {
        self.get_in(database, name).is_some()
    }

    /// Registers a TiKV-format-byte-backed table in `database`, or reports
    /// 1049 when that schema does not exist.
    /// go's stats meta bumps the table's row count on every write
    /// (`stats_meta.count`), which `I_S.TABLES`' TABLE_ROWS reads back.
    /// A DML that changed `delta` rows adjusts the stored estimate.
    pub fn adjust_table_row_count(&mut self, database: &str, name: &str, delta: i64) {
        let Some(database) = Arc::make_mut(&mut self.schema_mut().databases).get_mut(database) else {
            return;
        };
        let database = std::sync::Arc::make_mut(database);
        let Some(entry) = database.tables.get_mut(name) else {
            return;
        };
        let crate::TableEntry::Kv(table) = std::sync::Arc::make_mut(entry) else {
            return;
        };
        let table = std::sync::Arc::make_mut(table);
        let (rows, average, data, index) = table.storage_statistics();
        let updated = (i64::try_from(rows).unwrap_or(i64::MAX) + delta).max(0) as u64;
        table.set_storage_statistics((updated, average, data, index));
    }

    /// Go `SessionExtendedInfoSchema.UpdateTableInfo` (`MdlTables`): from now
    /// on this image -- a transaction's -- reads `entry`, the latest schema's
    /// table, for `database`.`name`. The image keeps the schema version the
    /// transaction pinned, as Go's overlay does; only the views derived from
    /// its tables are dropped.
    pub fn update_table_info(&mut self, database: &str, name: &str, entry: Arc<TableEntry>) -> bool {
        let key = CatalogTableKey::new(database, name);
        let Some(db) = self.database_mut(&key.database) else {
            return false;
        };
        db.tables.insert(key.table, entry);
        self.schema_mut().latest_index_schema.take();
        self.planner_view.take();
        self.statistics_schemas = Arc::default();
        self.invalidate_table_id_names();
        true
    }

    pub fn register_kv_in(
        &mut self,
        database: &str,
        name: &str,
        table: TableCommon,
    ) -> Result<(), DriverError> {
        self.bump_metadata_version();
        self.register_in(database, name, TableEntry::Kv(Arc::new(table)))
    }

    /// Registers an already-admitted view. The caller owns object-kind checks.
    /// Reports 1049 when the schema does not exist.
    pub fn register_view_in(
        &mut self,
        database: &str,
        name: &str,
        view: ViewDef,
    ) -> Result<(), DriverError> {
        self.bump_metadata_version();
        self.register_in(database, name, TableEntry::View(view))
    }

    /// Whether `name` in `database` is a view.
    #[must_use]
    pub fn is_view_in(&self, database: &str, name: &str) -> bool {
        self.get_in(database, name).is_some_and(TableEntry::is_view)
    }

    /// Whether `name` in `database` is a sequence.
    #[must_use]
    pub fn is_sequence_in(&self, database: &str, name: &str) -> bool {
        self.get_in(database, name)
            .is_some_and(TableEntry::is_sequence)
    }

    /// Registers a sequence in `database`, replacing whatever the name held.
    /// Callers own the name-collision check: Go answers 1050
    /// `Table 'db.name' already exists` for `CREATE SEQUENCE` over ANY
    /// existing name, table or sequence (captured).
    pub fn register_sequence_in(
        &mut self,
        database: &str,
        name: &str,
        sequence: SequenceDef,
    ) -> Result<(), DriverError> {
        self.bump_metadata_version();
        self.register_in(database, name, TableEntry::Sequence(sequence))
    }

    /// The sequence `name` in `database`, or `None` when the name is absent
    /// or holds something else.
    #[must_use]
    pub fn sequence_in(&self, database: &str, name: &str) -> Option<&SequenceDef> {
        match self.get_in(database, name) {
            Some(TableEntry::Sequence(sequence)) => Some(sequence),
            _ => None,
        }
    }

    /// Whether a table with `name` exists in the default database.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }
}


/// Go `perfschema`'s `init` registering its virtual tables
/// (`infoschema.RegisterVirtualTable`): the builder of the
/// `performance_schema` tables, which every catalog's memory schemas include
/// once it is set. tidb-infoschema-perfschema sets it (its `register`).
static PERFORMANCE_SCHEMA_TABLES: std::sync::OnceLock<
    fn() -> HashMap<String, Arc<TableEntry>>,
> = std::sync::OnceLock::new();

/// Sets the `performance_schema` table builder; the first call wins.
pub fn register_performance_schema(tables: fn() -> HashMap<String, Arc<TableEntry>>) {
    let _ = PERFORMANCE_SCHEMA_TABLES.set(tables);
}

// Go's test binary links `planner/core`, whose `init` sets `expression.BuildSimpleExpr`.
#[cfg(test)]
tidb_planner_init::link!();
