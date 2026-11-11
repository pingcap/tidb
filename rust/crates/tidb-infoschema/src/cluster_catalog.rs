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

use tidb_model::db::DBInfo;
use tidb_model::GoShared;
use tidb_model::table_info::{TableInfo, TABLE_INFO_VERSION2, TABLE_INFO_VERSION3};

/// Failure to read or interpret the stored catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterCatalogError {
    /// The snapshot read itself failed.
    Snapshot(String),
    /// The snapshot read failed with a storage error Go's `ToTiDBErr` gives a
    /// SQL identity (`ErrTxnAbortedByGC`, an exhausted backoff, ...). Go's
    /// meta reader returns it unwrapped, so it reaches the statement as is.
    Sql(tidb_terror::MysqlError),
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
    ) -> Result<Vec<tidb_txnkv::kv_storage::SnapshotScanRegion>, ClusterCatalogError>;
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

/// Go `meta.NewReader(snapshot)`'s transaction: the snapshot as the raw
/// transaction `meta.Mutator` decodes through, read-only. A snapshot read
/// that fails keeps its own error (`failed`), so a storage error with a SQL
/// identity (`ErrTxnAbortedByGC`, ...) reaches the caller unwrapped, as Go's
/// meta reader returns it.
pub struct MetaReadView<'a, S: ?Sized> {
    snapshot: &'a mut S,
    failed: Option<ClusterCatalogError>,
}

impl<S: MetaSnapshot + ?Sized> MetaReadView<'_, S> {
    fn read<R>(&mut self, result: Result<R, ClusterCatalogError>) -> tidb_meta::Result<R> {
        result.map_err(|error| {
            let message = error.to_string();
            self.failed.get_or_insert(error);
            tidb_meta::MetaError::Storage(message)
        })
    }
}

impl<S: MetaSnapshot + ?Sized> tidb_structure::transaction::RawTransaction for MetaReadView<'_, S> {
    type Error = tidb_meta::MetaError;

    fn get(&mut self, key: &[u8]) -> tidb_meta::Result<Option<Vec<u8>>> {
        let result = self.snapshot.get(key);
        self.read(result)
    }

    fn set(&mut self, _key: Vec<u8>, _value: Vec<u8>) -> tidb_meta::Result<()> {
        Err(tidb_meta::MetaError::Storage("a meta reader does not write".to_owned()))
    }

    fn delete(&mut self, _key: &[u8]) -> tidb_meta::Result<()> {
        Err(tidb_meta::MetaError::Storage("a meta reader does not write".to_owned()))
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> tidb_meta::Result<MetaPairs> {
        let result = self.snapshot.scan_prefix(prefix);
        self.read(result)
    }

    fn iterate_range(
        &mut self,
        start: &[u8],
        end: &[u8],
        visit: &mut tidb_structure::transaction::RawRangeVisitor<'_, tidb_meta::MetaError>,
    ) -> tidb_meta::Result<()> {
        let result = self.snapshot.scan_range(start, end);
        for (key, value) in self.read(result)? {
            visit(&key, &value)?;
        }
        Ok(())
    }
}

impl<S: MetaSnapshot + ?Sized> tidb_meta::transaction::MetaTransaction for MetaReadView<'_, S> {
    /// The load never reads `Mutator.StartTS`; a recorded snapshot has none.
    fn start_ts(&self) -> u64 {
        0
    }

    /// Go `NewReader` applies no transaction configuration.
    fn configure_meta_mutator(&mut self) {}
}

/// Go `meta.Reader` over a snapshot.
pub type MetaReader<'a, S> = tidb_meta::transaction::Mutator<MetaReadView<'a, S>>;

/// Go `meta.NewReader(snapshot)`: a mutator over the snapshot with no
/// transaction configuration (the view's is a no-op; the request-source tag
/// is the snapshot's own, set where it is opened).
fn meta_reader<S: MetaSnapshot + ?Sized>(snapshot: &mut S) -> MetaReader<'_, S> {
    tidb_meta::transaction::Mutator::new(MetaReadView {
        snapshot,
        failed: None,
    })
}

/// A meta reader's error: the snapshot's own when a read failed, else the
/// decode failure the reader reported.
fn reader_error<S: MetaSnapshot + ?Sized>(
    reader: &MetaReader<'_, S>,
    error: tidb_meta::MetaError,
) -> ClusterCatalogError {
    reader
        .inspect(|view| view.failed.clone())
        .ok()
        .flatten()
        .unwrap_or_else(|| ClusterCatalogError::Decode(error.to_string()))
}

/// Go `meta.NewReader(snapshot)` for one read: `read` runs against the
/// snapshot's meta reader, and a failed snapshot read answers its own error.
pub fn with_meta_reader<S: MetaSnapshot + ?Sized, R>(
    snapshot: &mut S,
    read: impl FnOnce(&MetaReader<'_, S>) -> tidb_meta::Result<R>,
) -> Result<R, ClusterCatalogError> {
    let reader = meta_reader(snapshot);
    read(&reader).map_err(|error| reader_error(&reader, error))
}

/// Reads the cluster schema version alone (Go `m.GetSchemaVersion`).
pub fn get_schema_version<S: MetaSnapshot + ?Sized>(snapshot: &mut S) -> Result<i64, ClusterCatalogError> {
    let reader = meta_reader(snapshot);
    reader.schema_version().map_err(|error| reader_error(&reader, error))
}

/// Go `Domain.loadInfoSchema`'s full load through `meta.NewReader`: the
/// schema version, `ListPolicies`, `ListResourceGroups` and every database
/// with its tables ([`fetch_all_schemas_with_tables`]), built into one schema
/// image (`InitWithDBInfos`) from one snapshot.
pub fn load_info_schema<S: MetaSnapshot + ?Sized>(
    snapshot: &mut S,
) -> Result<crate::SessionExtendedInfoSchema, ClusterCatalogError> {
    let reader = meta_reader(snapshot);
    let read = |error| reader_error(&reader, error);
    let schema_version = reader.schema_version().map_err(read)?;
    let policies = reader.policies().map_err(read)?;
    // Go `Mutator.ListResourceGroups` adds meta's default group when none is
    // stored under that name.
    let resource_groups = reader
        .resource_groups()
        .map_err(read)?
        .iter()
        .map(|group| (**group).clone())
        .collect();
    let databases = fetch_all_schemas_with_tables(&reader).map_err(read)?;
    Ok(crate::SessionExtendedInfoSchema::init_with_db_infos(
        databases,
        policies,
        resource_groups,
        schema_version,
    ))
}

/// Go `Loader.fetchAllSchemasWithTables`: `ListDatabases`, each database's
/// `Deprecated.Tables` its `ListTables`.
fn fetch_all_schemas_with_tables<S: MetaSnapshot + ?Sized>(
    reader: &MetaReader<'_, S>,
) -> tidb_meta::Result<Vec<GoShared<DBInfo>>> {
    let mut databases = Vec::new();
    for mut info in reader.databases()? {
        info.deprecated_tables = database_tables(reader, info.id)?.into();
        databases.push(GoShared::new(info));
    }
    Ok(databases)
}

fn database_tables<S: MetaSnapshot + ?Sized>(
    reader: &MetaReader<'_, S>,
    db_id: i64,
) -> tidb_meta::Result<Vec<TableInfo>> {
    let mut tables = reader.tables(db_id)?;
    for table in &mut tables {
        normalize_full_loaded_table_info(table);
    }
    Ok(tables)
}

/// Go `Mutator.ListTables`: every stored `TableInfo` under one database, in
/// stored key order, as a full load normalizes them.
pub fn list_tables<S: MetaSnapshot + ?Sized>(
    snapshot: &mut S,
    db_id: i64,
) -> Result<Vec<TableInfo>, ClusterCatalogError> {
    let reader = meta_reader(snapshot);
    database_tables(&reader, db_id).map_err(|error| reader_error(&reader, error))
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

/// Why one loaded table is not part of the built schema this tier serves.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkippedTable {
    /// Fully qualified stored name, `schema.table`.
    pub name: String,
    /// Exact, self-contained explanation.
    pub reason: String,
}
