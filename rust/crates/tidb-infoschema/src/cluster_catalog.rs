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

//! Reads a real cluster's catalog out of TiKV's `m` meta namespace.
//!
//! Go source of truth: `pkg/meta/meta.go` `ListDatabases` / `ListTables` /
//! `GetMetasByDBID` and the load half of `pkg/infoschema`'s builder. The key
//! and value codecs stay in `tidb-meta`; this module owns only the traversal
//! (`DBs` hash -> one hash per database -> `Table:<id>` fields) and the
//! translation.
//!
//! Every read of one load comes from ONE snapshot at ONE timestamp, which is
//! what makes the result a schema *version* rather than a pile of independently
//! observed objects: a DDL committing halfway through the walk is either
//! entirely before or entirely after the snapshot.


use std::fmt;

use tidb_hack::GoToLower;
use tidb_meta::{key, value};
use tidb_model::db::DBInfo;
use tidb_model::table_info::{TableInfo, TABLE_INFO_VERSION2, TABLE_INFO_VERSION3};

/// Failure to read or interpret the stored catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterCatalogError {
    /// The snapshot read itself failed.
    Snapshot(String),
    /// The snapshot read failed with a storage error Go's `ToTiDBErr` gives a
    /// SQL identity (`ErrTxnAbortedByGC`, an exhausted backoff, ...). Go's
    /// meta reader returns it unwrapped, so it reaches the statement as is.
    Sql(tidb_error::mysql::MysqlError),
    /// A stored key or value did not decode.
    Decode(String),
}

impl fmt::Display for ClusterCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot(detail) => write!(formatter, "catalog snapshot read failed: {detail}"),
            Self::Sql(error) => formatter.write_str(&error.message),
            Self::Decode(detail) => write!(formatter, "catalog decode failed: {detail}"),
        }
    }
}

impl std::error::Error for ClusterCatalogError {}

/// Key/value pairs one prefix scan returned, in key order.
pub type MetaPairs = Vec<(Vec<u8>, Vec<u8>)>;

/// The one storage capability a catalog load needs: point reads and prefix
/// scans that all observe the same snapshot.
///
/// Abstracting it keeps the traversal testable against recorded bytes; the
/// production implementation is a live transaction's snapshot (see
/// `tidb_infoschema_issyncer::real_tikv_catalog`).
pub trait MetaSnapshot {
    /// Reads one meta key, `None` when the snapshot holds no value there.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError>;

    /// Reads every key/value pair under `prefix`, in key order.
    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError>;

    /// The node's auto-ID service client, for a job that reads or rebases a
    /// counter the service owns (`AUTO_ID_CACHE 1`). `None` for a snapshot
    /// that carries none.
    fn auto_id_client(&self) -> Option<&std::sync::Arc<dyn tidb_autoid::service_rpc::AutoIdServiceRpc>> {
        None
    }

    /// Reads every key/value pair in `[start, end)`, in key order.
    ///
    /// Recorded in-memory snapshots can implement only [`Self::scan_prefix`]:
    /// the default finds their shared prefix and trims the returned pairs.
    /// Live snapshots override this method so TiKV seeks directly to `start`.
    fn scan_range(&mut self, start: &[u8], end: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        let common_len = start
            .iter()
            .zip(end)
            .take_while(|(left, right)| left == right)
            .count();
        Ok(self
            .scan_prefix(&start[..common_len])?
            .into_iter()
            .filter(|(key, _)| key.as_slice() >= start && key.as_slice() < end)
            .collect())
    }
}

/// A [`MetaSnapshot`] that can also read one record range in bounded pages.
///
/// Go's `ANALYZE` streams each region's rows into its sampler as the
/// coprocessor returns them and never holds the whole table
/// (`AnalyzeColumnsExec`, `executor/analyze.go`). The paging seam is what lets
/// this engine do the same over a transaction snapshot: a caller walks a
/// range by asking for one page, consuming it, and asking again from the key
/// after the last pair it got, so memory stays bounded by the page size
/// rather than the table size.
pub trait PagedMetaSnapshot: MetaSnapshot {
    /// Reads up to `limit` key/value pairs in `[start, end)`, in key order.
    ///
    /// Fewer pairs than `limit` means the range is drained. An implementation
    /// MUST honour `limit`: returning more would make the caller read rows it
    /// asked not to be sent.
    fn scan_page(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<MetaPairs, ClusterCatalogError>;
}

/// A snapshot that preserves the serving-region boundary of a full range.
pub trait RegionPagedMetaSnapshot: PagedMetaSnapshot {
    /// Reads every pair in `[start, end)`, grouped like successful TiKV
    /// coprocessor responses.
    fn scan_regions(
        &mut self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<tidb_txnkv::transaction::SnapshotScanRegion>, ClusterCatalogError>;
}

/// The stored JSON of one `DBInfo`, as the catalog itself writes it.
///
/// Re-exported here so a reader outside this crate -- the status server's
/// `/schema` route -- serves the SAME bytes the catalog stores, rather than
/// a second rendering that could drift from it.
pub fn stored_db_info_json(info: &DBInfo) -> Result<Vec<u8>, String> {
    tidb_meta::value::serialize_db_info(info).map_err(|error| error.to_string())
}

/// The stored JSON of one `TableInfo`. See [`stored_db_info_json`].
pub fn stored_table_info_json(info: &TableInfo) -> Result<Vec<u8>, String> {
    tidb_meta::value::serialize_table_info(info).map_err(|error| error.to_string())
}

/// One database and the tables stored under it.
#[derive(Clone, Debug)]
pub struct LoadedDatabase {
    /// The stored `DBInfo`.
    pub info: DBInfo,
    /// The stored `TableInfo`s, in stored key order.
    pub tables: Vec<TableInfo>,
}

/// One whole catalog as of one snapshot.
#[derive(Clone, Debug)]
pub struct ClusterCatalog {
    /// Go `mSchemaVersionKey`. Absent means 0, matching Go's `GetInt64`.
    pub schema_version: i64,
    /// Every database, in stored key order.
    pub databases: Vec<LoadedDatabase>,
    /// Go `Mutator.ListResourceGroups`: the stored resource groups, with
    /// meta's default group when none is stored under that name.
    pub resource_groups: Vec<tidb_model::ResourceGroupInfo>,
    /// Go `Mutator.ListPolicies`: the stored placement policies, which a
    /// table, partition or database names by id.
    pub policies: Vec<tidb_model::PolicyInfo>,
}

impl ClusterCatalog {
    /// Go `InfoSchema.SchemaByName` with `SchemaTableInfos`: one database
    /// by case-insensitive name and its tables, a stored one, else one of
    /// the virtual schemas every infoschema serves (`initVirtualTables`).
    #[must_use]
    pub fn schema_by_name(&self, schema: &str) -> Option<(&DBInfo, &[TableInfo])> {
        let lower = schema.go_to_lower();
        self.databases
            .iter()
            .find(|database| database.info.name.lowercase() == lower)
            .map(|database| (&database.info, database.tables.as_slice()))
            .or_else(|| crate::virtual_tables::virtual_schema(schema))
    }

    /// Go `InfoSchema.TableByName`: a stored table, else one of the virtual
    /// schemas every infoschema serves (`initVirtualTables`).
    #[must_use]
    pub fn table_by_name(&self, schema: &str, table: &str) -> Option<(&DBInfo, &TableInfo)> {
        self.find_table(schema, table)
            .or_else(|| crate::virtual_tables::virtual_table(schema, table))
    }

    /// Finds one table by case-insensitive schema and table name.
    #[must_use]
    pub fn find_table(&self, schema: &str, table: &str) -> Option<(&DBInfo, &TableInfo)> {
        let schema = schema.go_to_lower();
        let table = table.go_to_lower();
        self.databases
            .iter()
            .filter(|database| database.info.name.lowercase() == schema)
            .find_map(|database| {
                database
                    .tables
                    .iter()
                    .find(|stored| stored.name.lowercase() == table)
                    .map(|stored| (&database.info, stored))
            })
    }
}

/// Reads the cluster schema version alone (Go `m.GetSchemaVersion`).
pub fn read_schema_version<S: MetaSnapshot>(snapshot: &mut S) -> Result<i64, ClusterCatalogError> {
    match snapshot.get(&key::schema_version_kv_key())? {
        Some(stored) => value::parse_int_value(&stored)
            .map_err(|error| ClusterCatalogError::Decode(format!("SchemaVersionKey: {error}"))),
        // Go's TxStructure.GetInt64 answers 0 for a missing key.
        None => Ok(0),
    }
}

/// Reads the whole catalog (every database and table) from one snapshot, at
/// its schema version.
///
/// Go `ListDatabases` then `ListTables` per database. Go filters a database's
/// hash by field prefix because the same hash also holds the per-table ID
/// allocators; so does this.
pub fn load_cluster_catalog<S: MetaSnapshot>(
    snapshot: &mut S,
) -> Result<ClusterCatalog, ClusterCatalogError> {
    let schema_version = read_schema_version(snapshot)?;

    let mut resource_groups = Vec::new();
    for (_, stored) in snapshot.scan_prefix(&key::resource_groups_kv_prefix())? {
        let encoded = value::detach_magic_byte(&stored)
            .map_err(|error| ClusterCatalogError::Decode(format!("ResourceGroupInfo: {error}")))?;
        resource_groups.push(
            serde_json::from_slice::<tidb_model::ResourceGroupInfo>(encoded).map_err(|error| {
                ClusterCatalogError::Decode(format!("ResourceGroupInfo: {error}"))
            })?,
        );
    }
    if !resource_groups
        .iter()
        .any(|group| group.name.lowercase() == tidb_model::DEFAULT_RESOURCE_GROUP_NAME)
    {
        resource_groups.push(tidb_model::ResourceGroupInfo::default_group());
    }

    let mut policies = Vec::new();
    for (_, stored) in snapshot.scan_prefix(&key::policies_kv_prefix())? {
        policies.push(
            value::parse_policy_info(&stored)
                .map_err(|error| ClusterCatalogError::Decode(format!("PolicyInfo: {error}")))?,
        );
    }

    let mut databases = Vec::new();
    for (raw_key, stored) in snapshot.scan_prefix(&key::databases_kv_prefix())? {
        let (_, field) = tidb_meta::structure::decode_hash_data_key(&raw_key)
            .map_err(|error| ClusterCatalogError::Decode(format!("DBs hash key: {error}")))?;
        if !key::has_prefix(key::DB_PREFIX, &field) {
            continue;
        }
        let info = value::parse_db_info(&stored)
            .map_err(|error| ClusterCatalogError::Decode(format!("DBInfo: {error}")))?;
        let tables = load_database_tables(snapshot, info.id)?;
        databases.push(LoadedDatabase { info, tables });
    }

    Ok(ClusterCatalog {
        resource_groups,
        policies,
        schema_version,
        databases,
    })
}

/// Every stored `TableInfo` under one database, in stored key order.
pub fn load_database_tables<S: MetaSnapshot>(
    snapshot: &mut S,
    db_id: i64,
) -> Result<Vec<TableInfo>, ClusterCatalogError> {
    let mut tables = Vec::new();
    for (raw_key, stored) in snapshot.scan_prefix(&key::database_metas_kv_prefix(db_id))? {
        let (_, field) = tidb_meta::structure::decode_hash_data_key(&raw_key)
            .map_err(|error| ClusterCatalogError::Decode(format!("DB hash key: {error}")))?;
        // The same hash also holds TID/IID/TARID/SID allocator fields.
        if !key::has_prefix(key::TABLE_PREFIX, &field) {
            continue;
        }
        let mut table = value::parse_table_info(&stored, db_id)
            .map_err(|error| ClusterCatalogError::Decode(format!("TableInfo: {error}")))?;
        normalize_full_loaded_table_info(&mut table);
        tables.push(table);
    }
    Ok(tables)
}

/// Go `ConvertCharsetCollateToLowerCaseIfNeed` (`builder.go:936-946`): a
/// `TableInfo` older than version 3 gets its own and every column's charset
/// and collation lower-cased.
fn lower_case_charsets(table: &mut TableInfo) {
    if table.version >= TABLE_INFO_VERSION3 {
        return;
    }
    table.charset = table.charset.to_lowercase();
    table.collate = table.collate.to_lowercase();
    for column in table.columns.iter_handles() {
        // Go dereferences each `*ColumnInfo`: a stored null is corrupt.
        let column = column.expect("nil column in stored TableInfo");
        let mut column = column.write();
        let charset = column.get_charset().to_lowercase();
        let collate = column.get_collate().to_lowercase();
        column.set_charset(charset);
        column.set_collate(collate);
    }
}

/// Go `ConvertOldVersionUTF8ToUTF8MB4IfNeed` (`builder.go:949-963`): a
/// pre-version-2 table's `utf8` -- and each pre-version-2 column's -- becomes
/// `utf8mb4`/`utf8mb4_bin` when `treat-old-version-utf8-as-utf8mb4` is on
/// (Go's default), read from the global config as Go reads
/// `config.GetGlobalConfig()`. The comparison is Go's, case-sensitive.
fn old_utf8_to_utf8mb4(table: &mut TableInfo) {
    if table.version >= TABLE_INFO_VERSION2
        || !tidb_config::config_tree::config::get_global_config().treat_old_version_utf8_as_utf8mb4
    {
        return;
    }
    if table.charset == "utf8" {
        table.charset = "utf8mb4".to_owned();
        table.collate = "utf8mb4_bin".to_owned();
    }
    for column in table.columns.iter_handles() {
        let column = column.expect("nil column in stored TableInfo");
        let mut column = column.write();
        // Go `ColumnInfoVersion2`.
        if column.version < 2 && column.get_charset() == "utf8" {
            column.set_charset("utf8mb4");
            column.set_collate("utf8mb4_bin");
        }
    }
}

/// What Go's full load does to every `TableInfo` it lists
/// (`fetchSchemasWithTables`, `loader.go:528-535`): the `utf8` conversion
/// first, then the lower-casing. The order is Go's and it is observable: a
/// pre-version-2 table stored as `UTF8` stays `utf8` here (the conversion's
/// case-sensitive comparison misses it), where the diff path below makes it
/// `utf8mb4`.
pub(crate) fn normalize_full_loaded_table_info(table: &mut TableInfo) {
    old_utf8_to_utf8mb4(table);
    lower_case_charsets(table);
}

/// What Go's `applyCreateTable` does to the `TableInfo` a diff reads
/// (`builder.go:868-869`): the lower-casing first, then the `utf8`
/// conversion.
pub fn normalize_diff_loaded_table_info(table: &mut TableInfo) {
    lower_case_charsets(table);
    old_utf8_to_utf8mb4(table);
}

