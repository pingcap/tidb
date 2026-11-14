// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/infoschema/infoschema_v2.go`. Versioned indexes retain identities;
//! SIEVE alone retains ordinary table objects. Every miss uses the reader TS.
use crate::{cluster_catalog::ClusterCatalogError, sieve::Sieve, TableEntry};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tidb_ast::CiString;
use tidb_hack::GoToLower;
use tidb_model::db::DBInfo;
use tidb_model::table::ReferredFKInfo;
use tidb_model::table_info::TableInfo;
use tidb_model::GoShared;

/// Go `tableItem`.
#[derive(Clone, Debug)]
pub struct TableItem {
    pub db_name: CiString,
    pub db_id: i64,
    pub table_name: CiString,
    pub table_id: i64,
    pub schema_version: i64,
}
/// Go `tableCacheKey`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct TableCacheKey {
    pub table_id: i64,
    pub schema_version: i64,
}

/// Go `table.Table` and `Meta()`, including views/sequences whose Rust entry
/// has a separate representation. Both are dropped by cache eviction.
#[derive(Clone, Debug)]
pub struct CachedTable {
    pub info: GoShared<TableInfo>,
    pub table: Arc<TableEntry>,
    pub size: u64,
}

/// Go's requirement.Store().GetSnapshot(ts), meta reader, and table factory.
/// Implementations must read at exactly `ts`, with Go's 3000ms meta timeout.
pub trait TableLoader: Send + Sync {
    /// Go `infoschemaV2.loadTableInfo`'s GetTable and tableFromMeta.
    fn load_table(
        &self,
        db_id: i64,
        table_id: i64,
        ts: u64,
    ) -> Result<CachedTable, ClusterCatalogError>;
    /// Go `infoschemaV2.SchemaTableInfos`'s ListTables (does not refill).
    fn list_tables(
        &self,
        db_id: i64,
        ts: u64,
    ) -> Result<Vec<GoShared<TableInfo>>, ClusterCatalogError>;
}

// Tombstones retain their identity payload, as Go tableItem does, so GC can
// remove the matching by-ID entry even after a drop or full-load reset.
#[derive(Clone, Debug)]
struct Versions<K, V>(BTreeMap<(K, i64), VersionRecord<V>>);
#[derive(Clone, Debug)]
struct VersionRecord<V> {
    value: Option<V>,
    tomb: bool,
}
impl<K: Ord + Clone, V: Clone> Default for Versions<K, V> {
    fn default() -> Self {
        Self(BTreeMap::new())
    }
}
impl<K: Ord + Clone, V: Clone> Versions<K, V> {
    fn set(&mut self, key: K, version: i64, value: Option<V>) {
        let tomb = value.is_none();
        self.0.insert((key, version), VersionRecord { value, tomb });
    }
    fn get(&self, key: &K, version: i64) -> Option<V> {
        let ((found, _), value) = self.0.range(..=(key.clone(), version)).next_back()?;
        (found == key && !value.tomb)
            .then(|| value.value.clone())
            .flatten()
    }
    fn visible(&self, version: i64) -> Vec<(K, V)> {
        let mut last = None;
        self.0
            .iter()
            .rev()
            .filter_map(|((key, ver), value)| {
                if *ver > version || last.as_ref() == Some(key) {
                    return None;
                }
                last = Some(key.clone());
                (!value.tomb)
                    .then(|| {
                        value
                            .value
                            .as_ref()
                            .map(|value| (key.clone(), value.clone()))
                    })
                    .flatten()
            })
            .collect()
    }
    /// Go reset*BeforeFullLoad: tomb every known identity at the new version.
    fn reset(&mut self, version: i64) {
        let keys: std::collections::BTreeSet<_> =
            self.0.keys().map(|(key, _)| key.clone()).collect();
        for key in keys {
            self.set(key, version, None);
        }
    }
    /// Go gcCollect*: retain the newest record strictly below the cutoff.
    fn old_keys(&self, cut: i64) -> Vec<(K, i64)> {
        let mut last = None;
        let mut result = Vec::new();
        for ((key, ver), _) in self.0.iter().rev() {
            if *ver < cut && last.as_ref().is_some_and(|(k, v)| k == key && *v < cut) {
                result.push((key.clone(), *ver));
                if result.len() == 1024 {
                    break;
                }
            }
            last = Some((key.clone(), *ver));
        }
        result
    }
}

impl<K: Ord + Clone> Versions<K, TableItem> {
    fn tomb(&mut self, key: K, version: i64) {
        let value = self
            .0
            .range(..=(key.clone(), version))
            .next_back()
            .filter(|((found, _), _)| *found == key)
            .and_then(|(_, old)| old.value.clone());
        self.0
            .insert((key, version), VersionRecord { value, tomb: true });
    }
    fn reset_items(&mut self, version: i64) {
        let keys: std::collections::BTreeSet<_> =
            self.0.keys().map(|(key, _)| key.clone()).collect();
        for key in keys {
            self.tomb(key, version);
        }
    }
}

#[derive(Debug, Default)]
struct Indexes {
    by_name: Versions<(String, String), TableItem>,
    by_id: Versions<i64, TableItem>,
    schema_map: Versions<String, GoShared<DBInfo>>,
    schema_id2name: Versions<i64, String>,
    pid2tid: Versions<i64, i64>,
    // Go infoSchema.globalTemporaryTableIDs, retained across V2 images.
    global_temporary_tables: Versions<i64, TableItem>,
    table_info_resident: Versions<(String, i64), GoShared<TableInfo>>,
    referred_foreign_keys: Versions<(String, String), Vec<ReferredFKInfo>>,
    specials: HashMap<String, (GoShared<DBInfo>, HashMap<String, Arc<CachedTable>>)>,
}

/// Go `Data`: shared by current and historical InfoSchemaV2 images.
#[derive(Debug)]
pub struct Data {
    indexes: RwLock<Indexes>,
    pub table_cache: Sieve<TableCacheKey, Arc<CachedTable>>,
    pub recent_min_ts: AtomicU64,
    // Go singleflight.Group: concurrent callers share a result, including
    // no-refill requests and errors. Finished calls are removed immediately.
    loads: Mutex<HashMap<(i64, i64, i64), Arc<LoadCall>>>,
}
#[derive(Debug, Default)]
struct LoadCall {
    result: Mutex<Option<Result<Arc<CachedTable>, ClusterCatalogError>>>,
    ready: std::sync::Condvar,
}
impl Default for Data {
    fn default() -> Self {
        Self::new()
    }
}
impl Data {
    /// Go `NewData` (initial capacity 1GiB; Builder sets the configured size).
    pub fn new() -> Self {
        let table_cache = Sieve::new(1024 * 1024 * 1024);
        table_cache.set_status_hook(Arc::new(crate::sieve::MetricsHook));
        Self {
            indexes: RwLock::new(Indexes::default()),
            table_cache,
            recent_min_ts: AtomicU64::new(0),
            loads: Mutex::new(HashMap::new()),
        }
    }
    pub(crate) fn table_item_version(&self, id: i64, version: i64) -> Option<i64> {
        self.indexes
            .read()
            .unwrap()
            .by_id
            .get(&id, version)
            .map(|item| item.schema_version)
    }
    /// Go `Data.CacheCapacity`.
    pub fn cache_capacity(&self) -> u64 {
        self.table_cache.capacity()
    }
    /// Go `Data.SetCacheCapacity`.
    pub fn set_cache_capacity(&self, capacity: u64) {
        self.table_cache.set_capacity_and_wait_evict(capacity);
    }
    /// Go `Data.addDB`: Deprecated.Tables must not retain evictable tables.
    pub fn add_db(&self, version: i64, mut db: DBInfo) {
        db.deprecated_tables = Default::default();
        let name = db.name.lowercase().to_owned();
        let mut idx = self.indexes.write().unwrap();
        idx.schema_id2name.set(db.id, version, Some(name.clone()));
        idx.schema_map.set(name, version, Some(GoShared::new(db)));
    }
    /// Go `Data.deleteDB`.
    pub fn delete_db(&self, db: &DBInfo, version: i64) {
        let mut idx = self.indexes.write().unwrap();
        idx.schema_map
            .set(db.name.lowercase().into(), version, None);
        idx.schema_id2name.set(db.id, version, None);
    }
    /// Go `Data.addSpecialDB`.
    pub fn add_special_db(&self, db: GoShared<DBInfo>, tables: HashMap<String, Arc<CachedTable>>) {
        let name = db.read().name.lowercase().to_owned();
        self.indexes
            .write()
            .unwrap()
            .specials
            .entry(name)
            .or_insert((db, tables));
    }
    /// Go `Data.addTableItem` for ordinary names during a V2 full load.
    pub fn add_table_item(&self, item: TableItem) {
        let mut idx = self.indexes.write().unwrap();
        idx.by_name.set(
            (
                item.db_name.lowercase().into(),
                item.table_name.lowercase().into(),
            ),
            item.schema_version,
            Some(item.clone()),
        );
        idx.by_id
            .set(item.table_id, item.schema_version, Some(item));
    }
    /// Go `Data.add`, including partition and resident special-attribute indexes.
    pub fn add(&self, item: TableItem, table: CachedTable) {
        self.add_table_info(item.clone(), table.info.clone());
        self.table_cache.set(
            TableCacheKey {
                table_id: item.table_id,
                schema_version: item.schema_version,
            },
            Arc::new(table.clone()),
            table.size,
        );
    }
    /// Go `Builder.InitWithDBInfos` V2: populate identity and resident indexes
    /// without retaining ordinary TableInfo or constructing table.Table.
    pub fn add_table_info(&self, item: TableItem, info: GoShared<TableInfo>) {
        let mut idx = self.indexes.write().unwrap();
        let ver = item.schema_version;
        idx.by_name.set(
            (
                item.db_name.lowercase().into(),
                item.table_name.lowercase().into(),
            ),
            ver,
            Some(item.clone()),
        );
        idx.by_id.set(item.table_id, ver, Some(item.clone()));
        let ti = info.read();
        if ti.temp_table_type == tidb_model::TempTableType::GLOBAL {
            idx.global_temporary_tables
                .set(item.table_id, ver, Some(item.clone()));
        }
        if let Some(pi) = ti.get_partition_info() {
            for id in pi.read().definitions.map_visible(|d| d.id) {
                idx.pid2tid.set(id, ver, Some(item.table_id));
            }
        }
        if has_special_attributes(&ti) {
            idx.table_info_resident.set(
                (item.db_name.lowercase().into(), item.table_id),
                ver,
                Some(info.clone()),
            );
        }
        drop(ti);
        drop(idx);
        self.add_referred_foreign_keys(&item.db_name, &info.read(), ver);
    }
    /// Go `Data.remove` and `Builder.applyDropTableV2` partition tombstones.
    pub fn remove(&self, item: &TableItem, version: i64) {
        let mut idx = self.indexes.write().unwrap();
        idx.by_name.tomb(
            (
                item.db_name.lowercase().into(),
                item.table_name.lowercase().into(),
            ),
            version,
        );
        idx.by_id.tomb(item.table_id, version);
        idx.global_temporary_tables
            .set(item.table_id, version, None);
        idx.table_info_resident.set(
            (item.db_name.lowercase().into(), item.table_id),
            version,
            None,
        );
        for (pid, tid) in idx.pid2tid.visible(version) {
            if tid == item.table_id {
                idx.pid2tid.set(pid, version, None);
            }
        }
        drop(idx);
        self.table_cache.remove(&TableCacheKey {
            table_id: item.table_id,
            schema_version: version,
        });
    }
    /// Go `Data.getTableReferredForeignKeys`.
    pub fn get_table_referred_foreign_keys(
        &self,
        schema: &str,
        table: &str,
        version: i64,
    ) -> Vec<ReferredFKInfo> {
        self.indexes
            .read()
            .unwrap()
            .referred_foreign_keys
            .get(&(schema.into(), table.into()), version)
            .unwrap_or_default()
    }
    /// Go `Data.addReferredForeignKeys`.
    pub fn add_referred_foreign_keys(&self, schema: &CiString, table: &TableInfo, version: i64) {
        let mut idx = self.indexes.write().unwrap();
        for fk in table.foreign_keys.iter_deref() {
            let fk = fk.read();
            if fk.version < tidb_model::table::FK_VERSION1 {
                continue;
            }
            let key = (
                fk.ref_schema.lowercase().to_owned(),
                fk.ref_table.lowercase().to_owned(),
            );
            let mut refs = idx
                .referred_foreign_keys
                .get(&key, version)
                .unwrap_or_default();
            if refs
                .iter()
                .any(|r| same_reference(r, schema, &table.name, &fk.name))
            {
                continue;
            }
            refs.push(ReferredFKInfo {
                child_schema: schema.clone(),
                child_table: table.name.clone(),
                child_fk_name: fk.name.clone(),
                cols: fk.ref_cols.map_visible(Clone::clone).into(),
            });
            refs.sort_by(|a, b| {
                (
                    a.child_schema.lowercase(),
                    a.child_table.lowercase(),
                    a.child_fk_name.lowercase(),
                )
                    .cmp(&(
                        b.child_schema.lowercase(),
                        b.child_table.lowercase(),
                        b.child_fk_name.lowercase(),
                    ))
            });
            idx.referred_foreign_keys.set(key, version, Some(refs));
        }
    }
    /// Go `Data.deleteReferredForeignKeys`.
    pub fn delete_referred_foreign_keys(&self, schema: &CiString, table: &TableInfo, version: i64) {
        let mut idx = self.indexes.write().unwrap();
        for fk in table.foreign_keys.iter_deref() {
            let fk = fk.read();
            if fk.version < tidb_model::table::FK_VERSION1 {
                continue;
            }
            let key = (
                fk.ref_schema.lowercase().to_owned(),
                fk.ref_table.lowercase().to_owned(),
            );
            let mut refs = idx
                .referred_foreign_keys
                .get(&key, version)
                .unwrap_or_default();
            let before = refs.len();
            refs.retain(|r| !same_reference(r, schema, &table.name, &fk.name));
            if before != refs.len() {
                idx.referred_foreign_keys
                    .set(key, version, (!refs.is_empty()).then_some(refs));
            }
        }
    }
    /// Go `Data.resetBeforeFullLoad`.
    pub fn reset_before_full_load(&self, version: i64) {
        let mut idx = self.indexes.write().unwrap();
        idx.by_name.reset_items(version);
        idx.by_id.reset_items(version);
        idx.schema_map.reset(version);
        idx.schema_id2name.reset(version);
        idx.global_temporary_tables.reset(version);
        idx.pid2tid.reset(version);
        idx.table_info_resident.reset(version);
        idx.referred_foreign_keys.reset(version);
    }
    /// Go `Data.gcOldFKVersion`.
    pub fn gc_old_fk_version(&self, version: i64) -> usize {
        let mut idx = self.indexes.write().unwrap();
        let keys = idx.referred_foreign_keys.old_keys(version);
        for key in &keys {
            idx.referred_foreign_keys.0.remove(key);
        }
        keys.len()
    }
    /// Go `Data.GCOldVersion`.
    pub fn gc_old_version(&self, version: i64) -> (usize, usize) {
        let mut idx = self.indexes.write().unwrap();
        if idx.by_name.0.is_empty() {
            return (0, 0);
        }
        let keys = idx.by_name.old_keys(version);
        for key in &keys {
            if let Some(VersionRecord {
                value: Some(item), ..
            }) = idx.by_name.0.remove(key)
            {
                idx.by_id.0.remove(&(item.table_id, key.1));
            }
        }
        // This index represents Go base infoSchema.globalTemporaryTableIDs.
        // Retain only history still reachable by a cached schema, just as the
        // Go base maps are released with their InfoSchema images.
        for key in idx.global_temporary_tables.old_keys(version) {
            idx.global_temporary_tables.0.remove(&key);
        }
        let total = idx.by_name.0.len();
        drop(idx);
        self.gc_old_fk_version(version);
        (keys.len(), total)
    }
}

fn same_reference(r: &ReferredFKInfo, schema: &CiString, table: &CiString, fk: &CiString) -> bool {
    r.child_schema.lowercase() == schema.lowercase()
        && r.child_table.lowercase() == table.lowercase()
        && r.child_fk_name.lowercase() == fk.lowercase()
}
/// Go `infoschemacontext.HasSpecialAttributes`.
pub fn has_special_attributes(t: &TableInfo) -> bool {
    t.ttl_info.is_some()
        || t.tiflash_replica.is_some()
        || t.placement_policy_ref.is_some()
        || t.get_partition_info().is_some()
        || t.lock.is_some()
        || t.affinity.is_some()
}
/// Go `IsSpecialDB`.
pub fn is_special_db(name: &str) -> bool {
    matches!(
        name,
        "information_schema" | "performance_schema" | "metrics_schema"
    )
}

/// Go `infoschemaV2`: one schema version and read timestamp over shared Data.
#[derive(Clone)]
pub struct InfoSchemaV2 {
    pub data: Arc<Data>,
    pub schema_meta_version: i64,
    pub ts: u64,
    pub loader: Arc<dyn TableLoader>,
}
impl std::fmt::Debug for InfoSchemaV2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfoSchemaV2")
            .field("version", &self.schema_meta_version)
            .field("ts", &self.ts)
            .finish_non_exhaustive()
    }
}
impl InfoSchemaV2 {
    /// Go `NewInfoSchemaV2`.
    pub fn new(data: Arc<Data>, loader: Arc<dyn TableLoader>, version: i64, ts: u64) -> Self {
        Self {
            data,
            loader,
            schema_meta_version: version,
            ts,
        }
    }
    /// Go `infoschemaV2.CloneAndUpdateTS`.
    pub fn clone_and_update_ts(&self, ts: u64) -> Self {
        Self { ts, ..self.clone() }
    }
    /// Go `infoschemaV2.keepAlive`.
    pub fn keep_alive(&self) {
        self.data.recent_min_ts.fetch_min(self.ts, Ordering::AcqRel);
    }
    /// Go `infoschemaV2.TableByName`'s resident identity lookup.
    pub fn table_item_by_name(&self, schema: &str, table: &str) -> Option<TableItem> {
        self.data.indexes.read().unwrap().by_name.get(
            &(schema.go_to_lower(), table.go_to_lower()),
            self.schema_meta_version,
        )
    }
    /// Go `infoschemaV2.TableItemByID`.
    pub fn table_item_by_id(&self, id: i64) -> Option<TableItem> {
        if id <= 0 {
            return None;
        }
        self.data
            .indexes
            .read()
            .unwrap()
            .by_id
            .get(&id, self.schema_meta_version)
    }
    /// Go `infoschemaV2.IterateAllTableItems`.
    pub fn iterate_all_table_items(&self, mut visit: impl FnMut(TableItem) -> bool) {
        let items = self
            .data
            .indexes
            .read()
            .unwrap()
            .by_name
            .visible(self.schema_meta_version);
        for (_, item) in items {
            if !visit(item) {
                break;
            }
        }
    }
    /// Go `infoschemaV2.TableByName`; None uses Go's default refill=true.
    pub fn table_by_name(
        &self,
        schema: &str,
        table: &str,
        refill: Option<bool>,
    ) -> Result<Arc<CachedTable>, ClusterCatalogError> {
        let key = (schema.go_to_lower(), table.go_to_lower());
        if is_special_db(&key.0) {
            return self
                .data
                .indexes
                .read()
                .unwrap()
                .specials
                .get(&key.0)
                .and_then(|(_, tables)| tables.get(&key.1))
                .cloned()
                .ok_or_else(|| not_exists(schema, table));
        }
        self.keep_alive();
        let item = self
            .data
            .indexes
            .read()
            .unwrap()
            .by_name
            .get(&key, self.schema_meta_version)
            .ok_or_else(|| not_exists(schema, table))?;
        self.table_for_item(&item, refill.unwrap_or(true))
    }
    /// Go `infoschemaV2.TableByID`; None uses Go's default refill=false.
    pub fn table_by_id(&self, id: i64, refill: Option<bool>) -> Option<Arc<CachedTable>> {
        if id <= 0 {
            return None;
        }
        self.keep_alive();
        let item = self.table_item_by_id(id)?;
        if is_special_db(item.db_name.lowercase()) {
            return self
                .data
                .indexes
                .read()
                .unwrap()
                .specials
                .get(item.db_name.lowercase())
                .and_then(|(_, tables)| tables.get(item.table_name.lowercase()))
                .cloned();
        }
        self.table_for_item(&item, refill.unwrap_or(false)).ok()
    }
    fn table_for_item(
        &self,
        item: &TableItem,
        refill: bool,
    ) -> Result<Arc<CachedTable>, ClusterCatalogError> {
        if is_special_db(item.db_name.lowercase()) {
            return self
                .data
                .indexes
                .read()
                .unwrap()
                .specials
                .get(item.db_name.lowercase())
                .and_then(|(_, tables)| tables.get(item.table_name.lowercase()))
                .cloned()
                .ok_or_else(|| not_exists(item.db_name.original(), item.table_name.original()));
        }
        let key = TableCacheKey {
            table_id: item.table_id,
            schema_version: item.schema_version,
        };
        if let Some(table) = self.data.table_cache.get(&key) {
            return Ok(table);
        }
        let table = self.load_table_info(item.table_id, item.db_id)?;
        if refill {
            self.data.table_cache.set(key, table.clone(), table.size);
        }
        Ok(table)
    }
    /// Go `infoschemaV2.loadTableInfo`, including singleflight and flashback retry.
    pub fn load_table_info(
        &self,
        table_id: i64,
        db_id: i64,
    ) -> Result<Arc<CachedTable>, ClusterCatalogError> {
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/infoschema/mockLoadTableInfoError")
            .is_some()
        {
            return Err(ClusterCatalogError::Snapshot(
                "mockLoadTableInfoError".into(),
            ));
        }
        let key = (db_id, table_id, self.schema_meta_version);
        let (call, owner) = {
            let mut loads = self.data.loads.lock().unwrap();
            if let Some(call) = loads.get(&key) {
                (call.clone(), false)
            } else {
                let call = Arc::new(LoadCall::default());
                loads.insert(key, call.clone());
                (call, true)
            }
        };
        if owner {
            let result = loop {
                match self.loader.load_table(db_id, table_id, self.ts) {
                    Err(e) if e.to_string().contains("in flashback progress") => {
                        std::thread::sleep(std::time::Duration::from_millis(200))
                    }
                    result => break result.map(Arc::new),
                }
            };
            *call.result.lock().unwrap() = Some(result.clone());
            self.data.loads.lock().unwrap().remove(&key);
            call.ready.notify_all();
            result
        } else {
            let mut result = call.result.lock().unwrap();
            while result.is_none() {
                result = call.ready.wait(result).unwrap();
            }
            result.clone().unwrap()
        }
    }
    /// Go `infoschemaV2.TableIsCached`.
    pub fn table_is_cached(&self, id: i64) -> bool {
        self.table_item_by_id(id).is_some_and(|item| {
            is_special_db(item.db_name.lowercase())
                || self
                    .data
                    .table_cache
                    .get(&TableCacheKey {
                        table_id: id,
                        schema_version: item.schema_version,
                    })
                    .is_some()
        })
    }
    /// Go `infoschemaV2.EvictTable`.
    pub fn evict_table(&self, schema: &str, table: &str) {
        if let Some(item) = self.data.indexes.read().unwrap().by_name.get(
            &(schema.go_to_lower(), table.go_to_lower()),
            self.schema_meta_version,
        ) {
            self.data.table_cache.remove(&TableCacheKey {
                table_id: item.table_id,
                schema_version: self.schema_meta_version,
            });
            self.data.table_cache.remove(&TableCacheKey {
                table_id: item.table_id,
                schema_version: item.schema_version,
            });
        }
    }
    /// Go `infoschemaV2.SchemaByName`.
    pub fn schema_by_name(&self, schema: &str) -> Option<GoShared<DBInfo>> {
        let idx = self.data.indexes.read().unwrap();
        let name = schema.go_to_lower();
        idx.specials
            .get(&name)
            .map(|(db, _)| db.clone())
            .or_else(|| idx.schema_map.get(&name, self.schema_meta_version))
    }
    /// Go `infoschemaV2.SchemaByID`.
    pub fn schema_by_id(&self, id: i64) -> Option<GoShared<DBInfo>> {
        let idx = self.data.indexes.read().unwrap();
        if let Some((db, _)) = idx.specials.values().find(|(db, _)| db.read().id == id) {
            return Some(db.clone());
        }
        let name = idx.schema_id2name.get(&id, self.schema_meta_version)?;
        idx.schema_map.get(&name, self.schema_meta_version)
    }
    /// Go `infoschemaV2.AllSchemas`.
    pub fn all_schemas(&self) -> Vec<GoShared<DBInfo>> {
        let idx = self.data.indexes.read().unwrap();
        idx.schema_map
            .visible(self.schema_meta_version)
            .into_iter()
            .map(|(_, db)| db)
            .chain(idx.specials.values().map(|(db, _)| db.clone()))
            .collect()
    }
    /// Go `infoschemaV2.SchemaTableInfos`.
    pub fn schema_table_infos(
        &self,
        schema: &str,
    ) -> Result<Vec<GoShared<TableInfo>>, ClusterCatalogError> {
        let name = schema.go_to_lower();
        if is_special_db(&name) {
            return Ok(self
                .data
                .indexes
                .read()
                .unwrap()
                .specials
                .get(&name)
                .map(|(_, tables)| tables.values().map(|t| t.info.clone()).collect())
                .unwrap_or_default());
        }
        self.keep_alive();
        let Some(db) = self.schema_by_name(schema) else {
            return Ok(Vec::new());
        };
        loop {
            match self.loader.list_tables(db.read().id, self.ts) {
                Err(e) if e.to_string().contains("in flashback progress") => {
                    std::thread::sleep(std::time::Duration::from_millis(200))
                }
                result => return result,
            }
        }
    }
    /// Go `infoschemaV2.SchemaSimpleTableInfos`: resident names, no table loads.
    pub fn schema_simple_table_infos(&self, schema: &str) -> Vec<TableItem> {
        let mut result = Vec::new();
        self.iterate_all_table_items(|item| {
            if item.db_name.lowercase() == schema.go_to_lower() {
                result.push(item);
            }
            true
        });
        result
    }
    /// Go `infoSchema.globalTemporaryTableIDs` at this reader's version.
    pub fn global_temporary_table_names(&self) -> Vec<(String, String)> {
        self.data
            .indexes
            .read()
            .unwrap()
            .global_temporary_tables
            .visible(self.schema_meta_version)
            .into_iter()
            .map(|(_, item)| {
                (
                    item.db_name.lowercase().to_owned(),
                    item.table_name.lowercase().to_owned(),
                )
            })
            .collect()
    }
    /// Go `infoschemaV2.TableIDByPartitionID`.
    pub fn table_id_by_partition_id(&self, id: i64) -> Option<i64> {
        self.data
            .indexes
            .read()
            .unwrap()
            .pid2tid
            .get(&id, self.schema_meta_version)
    }
    /// Go `infoschemaV2.TableItemByPartitionID`.
    pub fn table_item_by_partition_id(&self, id: i64) -> Option<TableItem> {
        self.table_item_by_id(self.table_id_by_partition_id(id)?)
    }
    /// Go `infoschemaV2.GetTableReferredForeignKeys`.
    pub fn get_table_referred_foreign_keys(
        &self,
        schema: &str,
        table: &str,
    ) -> Vec<ReferredFKInfo> {
        self.keep_alive();
        self.data
            .get_table_referred_foreign_keys(schema, table, self.schema_meta_version)
    }
    /// Go `infoschemaV2.ListTablesWithSpecialAttribute`.
    pub fn list_tables_with_special_attribute(
        &self,
        filter: impl Fn(&TableInfo) -> bool,
    ) -> Vec<(String, Vec<GoShared<TableInfo>>)> {
        let mut result: Vec<(String, Vec<GoShared<TableInfo>>)> = Vec::new();
        for ((db, _), table) in self
            .data
            .indexes
            .read()
            .unwrap()
            .table_info_resident
            .visible(self.schema_meta_version)
        {
            if !filter(&table.read()) {
                continue;
            }
            if result.last().is_none_or(|(name, _)| *name != db) {
                result.push((db, Vec::new()));
            }
            result.last_mut().unwrap().1.push(table);
        }
        result
    }
}
fn not_exists(schema: &str, table: &str) -> ClusterCatalogError {
    ClusterCatalogError::Sql(tidb_terror::MysqlError::from_terror(
        &crate::infoschema_errors::ERR_TABLE_NOT_EXISTS
            .fast_gen_by_args(&[schema.to_owned().into(), table.to_owned().into()]),
    ))
}

#[cfg(test)]
mod tests;

/// Borrowed Rust catalog consumers retain tables for the lifetime of their
/// reader, as Go callers retain table.Table pointers. Cloning a reader starts
/// with empty pins; the shared Data cache never owns these reader pins.
#[derive(Debug, Default)]
pub(crate) struct ReaderPins {
    entries: BorrowCache<Arc<CachedTable>>,
}
impl Clone for ReaderPins {
    fn clone(&self) -> Self {
        Self::default()
    }
}
impl ReaderPins {
    pub fn new(_: &InfoSchemaV2) -> Self {
        Self::default()
    }
    pub fn get_entry<'a>(
        &'a self,
        is: &InfoSchemaV2,
        key: &crate::CatalogTableKey,
        refill: bool,
    ) -> Result<Option<(&'a crate::CatalogTableKey, &'a Arc<CachedTable>)>, ClusterCatalogError>
    {
        is.keep_alive();
        let Some(item) = is.table_item_by_name(&key.database, &key.table) else {
            return Ok(None);
        };
        if let Some(entry) = self.entries.get(key) {
            return Ok(Some(entry));
        }
        let table = is.table_for_item(&item, refill)?;
        Ok(Some(self.entries.insert(key.clone(), table)))
    }
    pub fn get<'a>(
        &'a self,
        is: &InfoSchemaV2,
        key: &crate::CatalogTableKey,
    ) -> Result<Option<&'a Arc<CachedTable>>, ClusterCatalogError> {
        self.get_entry(is, key, true)
            .map(|entry| entry.map(|(_, table)| table))
    }
}

/// Stable, append-only storage for Rust borrowed catalog APIs. It owns only
/// objects accessed by this reader. OnceLock keeps published addresses stable
/// without unsafe lifetime extension; random hash order avoids sorted-name chains.
#[derive(Debug)]
pub(crate) struct BorrowCache<T> {
    root: std::sync::OnceLock<Box<BorrowNode<T>>>,
    hash: std::collections::hash_map::RandomState,
}
#[derive(Debug)]
struct BorrowNode<T> {
    hash: u64,
    key: crate::CatalogTableKey,
    value: T,
    left: std::sync::OnceLock<Box<BorrowNode<T>>>,
    right: std::sync::OnceLock<Box<BorrowNode<T>>>,
}
impl<T> Default for BorrowCache<T> {
    fn default() -> Self {
        Self {
            root: Default::default(),
            hash: Default::default(),
        }
    }
}
impl<T> BorrowCache<T> {
    fn hash(&self, key: &crate::CatalogTableKey) -> u64 {
        use std::hash::BuildHasher;
        self.hash.hash_one(key)
    }
    pub fn get(&self, key: &crate::CatalogTableKey) -> Option<(&crate::CatalogTableKey, &T)> {
        let hash = self.hash(key);
        let mut cell = &self.root;
        loop {
            let node = cell.get()?;
            match (hash, &key.database, &key.table).cmp(&(
                node.hash,
                &node.key.database,
                &node.key.table,
            )) {
                std::cmp::Ordering::Equal => return Some((&node.key, &node.value)),
                std::cmp::Ordering::Less => cell = &node.left,
                std::cmp::Ordering::Greater => cell = &node.right,
            }
        }
    }
    pub fn insert(&self, key: crate::CatalogTableKey, value: T) -> (&crate::CatalogTableKey, &T) {
        let hash = self.hash(&key);
        let mut value = Some(value);
        let mut cell = &self.root;
        loop {
            let node = cell.get_or_init(|| {
                Box::new(BorrowNode {
                    hash,
                    key: key.clone(),
                    value: value.take().unwrap(),
                    left: Default::default(),
                    right: Default::default(),
                })
            });
            match (hash, &key.database, &key.table).cmp(&(
                node.hash,
                &node.key.database,
                &node.key.table,
            )) {
                std::cmp::Ordering::Equal => return (&node.key, &node.value),
                std::cmp::Ordering::Less => cell = &node.left,
                std::cmp::Ordering::Greater => cell = &node.right,
            }
        }
    }
}

mod size;
/// Go `entry.Size` / `internal.Sizeof`: retained native metadata allocations.
pub fn table_cache_entry_size(info: &TableInfo, table: &TableEntry) -> u64 {
    size::table_size(info, table)
}

/// Go `Builder.InitWithOldInfoSchema`'s old metadata while applying diffs.
/// This transient owner is dropped before the new V2 image is published.
#[derive(Clone, Debug)]
pub(crate) struct BuilderOrigin {
    pub source: InfoSchemaV2,
    pub databases: HashMap<i64, GoShared<DBInfo>>,
    pub tables: HashMap<(i64, i64), (CiString, GoShared<TableInfo>)>,
}

impl BuilderOrigin {
    /// Go `Builder.ApplyDiff`'s V2 index mutations for one staged metadata image.
    pub fn apply_image(&mut self, data: &Data, version: i64, databases: Vec<GoShared<DBInfo>>) {
        let current: HashMap<_, _> = databases
            .into_iter()
            .map(|db| {
                let id = db.read().id;
                (id, db)
            })
            .collect();
        let mut tables = HashMap::new();
        for (&id, db) in &current {
            for table in db.read().deprecated_tables.iter_deref() {
                let table_id = table.read().id;
                tables.insert((id, table_id), (db.read().name.clone(), table));
            }
        }
        for (id, db) in &self.databases {
            if !current.contains_key(id) {
                data.delete_db(&db.read(), version);
            }
        }
        for (&(db_id, table_id), (name, old)) in &self.tables {
            if !tables
                .get(&(db_id, table_id))
                .is_some_and(|(new_name, new)| name == new_name && old.ptr_eq(new))
            {
                let info = old.read();
                data.remove(
                    &TableItem {
                        db_name: name.clone(),
                        db_id,
                        table_name: info.name.clone(),
                        table_id,
                        schema_version: version,
                    },
                    version,
                );
                data.delete_referred_foreign_keys(name, &info, version);
            }
        }
        for (&id, db) in &current {
            if !self.databases.get(&id).is_some_and(|old| old.ptr_eq(db)) {
                data.add_db(version, db.read().clone());
            }
        }
        for (&(db_id, table_id), (name, new)) in &tables {
            if !self
                .tables
                .get(&(db_id, table_id))
                .is_some_and(|(old_name, old)| name == old_name && old.ptr_eq(new))
            {
                let info = new.read();
                data.add_table_info(
                    TableItem {
                        db_name: name.clone(),
                        db_id,
                        table_name: info.name.clone(),
                        table_id,
                        schema_version: version,
                    },
                    new.clone(),
                );
            }
        }
        self.databases = current;
        self.tables = tables;
    }
}
