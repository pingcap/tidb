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
//! translation into the node's [`ConfiguredTable`] surface.
//!
//! Every read of one load comes from ONE snapshot at ONE timestamp, which is
//! what makes the result a schema *version* rather than a pile of independently
//! observed objects: a DDL committing halfway through the walk is either
//! entirely before or entirely after the snapshot.

use std::fmt;

use tidb_datatype::{FieldTypeCode, FieldTypeFlags};
use tidb_meta::{key, value};
use tidb_model::column::ColumnInfo;
use tidb_model::db::DBInfo;
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;
use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};

/// Failure to read or interpret the stored catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterCatalogError {
    /// The snapshot read itself failed.
    Snapshot(String),
    /// A stored key or value did not decode.
    Decode(String),
}

impl fmt::Display for ClusterCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot(detail) => write!(formatter, "catalog snapshot read failed: {detail}"),
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
/// `crate::real_tikv_catalog`).
pub trait MetaSnapshot {
    /// Reads one meta key, `None` when the snapshot holds no value there.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError>;

    /// Reads every key/value pair under `prefix`, in key order.
    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError>;
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
}

impl ClusterCatalog {
    /// Finds one table by case-insensitive schema and table name.
    #[must_use]
    pub fn find_table(&self, schema: &str, table: &str) -> Option<(&DBInfo, &TableInfo)> {
        let schema = schema.to_lowercase();
        let table = table.to_lowercase();
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

/// Reads the whole catalog from one snapshot.
///
/// Go `ListDatabases` then `ListTables` per database. Go filters a database's
/// hash by field prefix because the same hash also holds the per-table ID
/// allocators; so does this.
pub fn load_cluster_catalog<S: MetaSnapshot>(
    snapshot: &mut S,
) -> Result<ClusterCatalog, ClusterCatalogError> {
    let schema_version = match snapshot.get(&key::schema_version_kv_key())? {
        Some(stored) => value::parse_int_value(&stored)
            .map_err(|error| ClusterCatalogError::Decode(format!("SchemaVersionKey: {error}")))?,
        // Go's TxStructure.GetInt64 answers 0 for a missing key.
        None => 0,
    };

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
        schema_version,
        databases,
    })
}

fn load_database_tables<S: MetaSnapshot>(
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
        tables.push(
            value::parse_table_info(&stored, db_id)
                .map_err(|error| ClusterCatalogError::Decode(format!("TableInfo: {error}")))?,
        );
    }
    Ok(tables)
}

/// Why one loaded table cannot be served by the bounded read path.
///
/// A refusal is deliberately kept and reported at query time instead of making
/// the table disappear: a table the cluster really has, that this node cannot
/// read yet, is a capability gap the operator must be able to see.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedTableRefusal {
    /// Fully qualified stored name, `schema.table`.
    pub name: String,
    /// Exact, self-contained explanation naming the offending column and type.
    pub reason: String,
}

impl fmt::Display for LoadedTableRefusal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "table {} is present in the cluster catalog but cannot be read by this node: {}",
            self.name, self.reason
        )
    }
}

/// Translates one loaded table into the node's read surface.
///
/// The admitted shape is exactly what the read path decodes today: a
/// non-partitioned base table with a signed `BIGINT` clustered handle and
/// columns of the widened scalar set (`BIGINT`, `BIGINT UNSIGNED`, `INT`,
/// `DOUBLE`, `CHAR`, `VARCHAR`, `DECIMAL`), each either `NOT NULL` or
/// nullable. A nullable column is read-only: the write path refuses the whole
/// table at plan time (`resolve_write_table`).
pub fn configure_loaded_table(
    schema: &str,
    table: &TableInfo,
) -> Result<ConfiguredTable, LoadedTableRefusal> {
    let name = format!("{}.{}", schema, table.name.original());
    let refuse = |reason: String| LoadedTableRefusal {
        name: name.clone(),
        reason,
    };

    if table.state != SchemaState::PUBLIC {
        return Err(refuse(format!(
            "its schema state is {} rather than public",
            table.state.0
        )));
    }
    if table.is_view() {
        return Err(refuse("it is a view".to_owned()));
    }
    if table.is_sequence() {
        return Err(refuse("it is a sequence".to_owned()));
    }
    if table.partition.is_some() {
        return Err(refuse("it is partitioned".to_owned()));
    }
    if table.is_common_handle {
        return Err(refuse(
            "its primary key is a clustered composite handle, not a signed BIGINT handle"
                .to_owned(),
        ));
    }
    if !table.pk_is_handle {
        return Err(refuse(
            "it has no signed BIGINT PRIMARY KEY serving as the row handle".to_owned(),
        ));
    }

    let mut columns = Vec::new();
    for column in table.cols() {
        columns.push(configure_loaded_column(column).map_err(&refuse)?);
    }
    if columns.is_empty() {
        return Err(refuse("it has no public columns".to_owned()));
    }
    Ok(ConfiguredTable::new(
        schema,
        table.name.original(),
        table.id,
        columns,
    ))
}

/// Validates a fractional-seconds precision loaded from persisted column
/// metadata, per Go `types.CheckFsp`'s `[MinFsp, MaxFsp] = [0, 6]` range.
/// By the time DDL persists a `DATETIME`/`TIMESTAMP`/`TIME` column, its
/// `decimal` is always this concrete value (never `UnspecifiedFsp`), since
/// `setCharsetCollationFlenDecimal` fills in `DefaultFsp` (`0`) at column
/// creation; an out-of-range value here would mean a corrupt catalog, so it
/// is refused rather than clamped.
fn configured_fsp(name: &str, sql_type: &str, decimal: i64) -> Result<u8, String> {
    u8::try_from(decimal)
        .ok()
        .filter(|&fsp| fsp <= 6)
        .ok_or_else(|| {
            format!("column `{name}` is {sql_type} with an unusable declared fsp {decimal}")
        })
}

/// Decides whether one stored string column is the `binary` variant, refusing
/// every charset/collation pairing the node's configured types cannot express.
///
/// `ConfiguredScalarType`'s string vocabulary is exactly two collations:
/// `utf8mb4_bin` and `binary`. Both the bounded node's `ORDER BY`
/// (`compare_prepared_rows`) and its `SELECT DISTINCT` grouping normalize
/// `Datum::Bytes` under a hardcoded `Collation::Utf8Mb4Bin`, because that is
/// the only string collation the configured surface can name. Admitting a
/// column stored at any other collation — `utf8mb4_general_ci`, say — would
/// therefore sort and group it by raw bytes, silently returning `'B' < 'a'`
/// where Go's collation-aware `SortExec` returns `'a' < 'B'`. Checking the
/// charset alone is not enough: `utf8mb4_general_ci` is a `utf8mb4` charset.
/// So the collation itself is the gate, and an unrepresentable one refuses the
/// whole table at load rather than answering it wrongly at query time.
fn configured_string_is_binary(
    name: &str,
    sql_type: &str,
    column: &ColumnInfo,
) -> Result<bool, String> {
    match (column.get_charset(), column.get_collate()) {
        ("utf8mb4", "utf8mb4_bin") => Ok(false),
        ("binary", "binary") => Ok(true),
        (charset, collate) => Err(format!(
            "column `{name}` is {sql_type} with charset `{charset}` collation `{collate}`, \
             and this node only reads `utf8mb4_bin` or `binary` strings"
        )),
    }
}

fn configure_loaded_column(column: &ColumnInfo) -> Result<ConfiguredColumn, String> {
    let name = column.name.original();
    let flags = column.get_flag();
    let unsigned = flags & FieldTypeFlags::UNSIGNED != 0;
    let handle = flags & FieldTypeFlags::PRI_KEY != 0;
    let nullable = flags & FieldTypeFlags::NOT_NULL == 0;
    if handle && nullable {
        // The clustered handle *is* the record key, so Go always stamps it
        // NOT NULL; a stored descriptor that says otherwise is malformed.
        return Err(format!(
            "column `{name}` is the row handle but is not NOT NULL"
        ));
    }
    if flags & FieldTypeFlags::GENERATED_COLUMN != 0 {
        return Err(format!("column `{name}` is a generated column"));
    }
    let code = column.get_type();
    if handle {
        if code != FieldTypeCode::LongLong || unsigned {
            return Err(format!(
                "column `{name}` is the row handle but has type {}, not signed BIGINT",
                describe_type(column)
            ));
        }
        return Ok(ConfiguredColumn::clustered_primary_key(name, column.id));
    }
    let column = match code {
        FieldTypeCode::LongLong if unsigned => Ok(
            ConfiguredColumn::stored_unsigned_bigint_not_null(name, column.id),
        ),
        FieldTypeCode::LongLong => Ok(ConfiguredColumn::stored_not_null(name, column.id)),
        FieldTypeCode::Long if !unsigned => {
            Ok(ConfiguredColumn::stored_int_not_null(name, column.id))
        }
        FieldTypeCode::Double if !unsigned => {
            Ok(ConfiguredColumn::stored_double_not_null(name, column.id))
        }
        FieldTypeCode::String if !unsigned => {
            let flen = column.get_flen();
            let max_length = u32::try_from(flen).map_err(|_| {
                format!("column `{name}` is CHAR with an unusable declared length {flen}")
            })?;
            if configured_string_is_binary(name, "CHAR", column)? {
                // `ConfiguredScalarType::Char` has no `binary` variant, so a
                // stored `BINARY(n)` (a `String` column at charset `binary`)
                // has no faithful configured type at all.
                return Err(format!(
                    "column `{name}` is BINARY, which this node cannot decode yet"
                ));
            }
            Ok(ConfiguredColumn::stored_char_not_null(
                name, column.id, max_length,
            ))
        }
        FieldTypeCode::Varchar | FieldTypeCode::VarString if !unsigned => {
            let flen = column.get_flen();
            let max_length = u32::try_from(flen).map_err(|_| {
                format!("column `{name}` is VARCHAR with an unusable declared length {flen}")
            })?;
            let binary = configured_string_is_binary(name, "VARCHAR", column)?;
            Ok(ConfiguredColumn::stored_varchar_not_null(
                name, column.id, max_length, binary,
            ))
        }
        FieldTypeCode::Date if !unsigned => {
            Ok(ConfiguredColumn::stored_date_not_null(name, column.id))
        }
        FieldTypeCode::Datetime if !unsigned => {
            let fsp = configured_fsp(name, "DATETIME", column.get_decimal())?;
            Ok(ConfiguredColumn::stored_datetime_not_null(
                name, column.id, fsp,
            ))
        }
        FieldTypeCode::Timestamp if !unsigned => {
            let fsp = configured_fsp(name, "TIMESTAMP", column.get_decimal())?;
            Ok(ConfiguredColumn::stored_timestamp_not_null(
                name, column.id, fsp,
            ))
        }
        FieldTypeCode::Duration if !unsigned => {
            let fsp = configured_fsp(name, "TIME", column.get_decimal())?;
            Ok(ConfiguredColumn::stored_duration_not_null(
                name, column.id, fsp,
            ))
        }
        FieldTypeCode::NewDecimal if !unsigned => {
            let flen = column.get_flen();
            let decimal = column.get_decimal();
            let precision = u32::try_from(flen).map_err(|_| {
                format!("column `{name}` is DECIMAL with an unusable declared precision {flen}")
            })?;
            let scale = u32::try_from(decimal).map_err(|_| {
                format!("column `{name}` is DECIMAL with an unusable declared scale {decimal}")
            })?;
            Ok(ConfiguredColumn::stored_decimal_not_null(
                name, column.id, precision, scale,
            ))
        }
        _ => Err(format!(
            "column `{name}` has type {}, which this node cannot decode yet",
            describe_type(column)
        )),
    }?;
    Ok(if nullable { column.nullable() } else { column })
}

/// Names a column's stored type the way an operator wrote it, so a refusal can
/// be acted on without reading TiDB internals.
fn describe_type(column: &ColumnInfo) -> String {
    let unsigned = if column.get_flag() & FieldTypeFlags::UNSIGNED != 0 {
        " UNSIGNED"
    } else {
        ""
    };
    let base = match column.get_type() {
        FieldTypeCode::Tiny => "TINYINT".to_owned(),
        FieldTypeCode::Short => "SMALLINT".to_owned(),
        FieldTypeCode::Int24 => "MEDIUMINT".to_owned(),
        FieldTypeCode::Long => "INT".to_owned(),
        FieldTypeCode::LongLong => "BIGINT".to_owned(),
        FieldTypeCode::Float => "FLOAT".to_owned(),
        FieldTypeCode::Double => "DOUBLE".to_owned(),
        FieldTypeCode::NewDecimal => "DECIMAL".to_owned(),
        FieldTypeCode::String => format!("CHAR({})", column.get_flen()),
        FieldTypeCode::Varchar | FieldTypeCode::VarString => {
            format!("VARCHAR({})", column.get_flen())
        }
        FieldTypeCode::Blob | FieldTypeCode::TinyBlob => "BLOB/TEXT".to_owned(),
        FieldTypeCode::MediumBlob | FieldTypeCode::LongBlob => "BLOB/TEXT".to_owned(),
        FieldTypeCode::Date | FieldTypeCode::NewDate => "DATE".to_owned(),
        FieldTypeCode::Datetime => "DATETIME".to_owned(),
        FieldTypeCode::Timestamp => "TIMESTAMP".to_owned(),
        FieldTypeCode::Duration => "TIME".to_owned(),
        FieldTypeCode::Year => "YEAR".to_owned(),
        FieldTypeCode::Bit => "BIT".to_owned(),
        FieldTypeCode::Json => "JSON".to_owned(),
        FieldTypeCode::Enum => "ENUM".to_owned(),
        FieldTypeCode::Set => "SET".to_owned(),
        other => format!("{other:?}"),
    };
    format!("{base}{unsigned}")
}

/// The exclusive end of a prefix scan: the prefix with its last byte below
/// `0xFF` incremented. An all-`0xFF` prefix has no finite end.
#[must_use]
pub fn prefix_scan_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != 0xFF {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}
