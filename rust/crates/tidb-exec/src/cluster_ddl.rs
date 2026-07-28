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

//! Writing the catalog: `CREATE`/`DROP` for databases and tables, planned as
//! one set of meta-key mutations over one snapshot.
//!
//! Go source of truth is the *final meta mutation* of a DDL job, not the job
//! queue around it:
//!
//! * `pkg/meta/meta.go` `GenGlobalIDs` — `Inc(NextGlobalID, n)` returns the new
//!   maximum, and the allocated IDs are `old+1 ..= new`. The key holds the max
//!   USED id, so a fresh allocation is always `read + n`.
//! * `pkg/ddl/create_table.go` `createTable` — the new `TableInfo` is stamped
//!   `StatePublic` with `UpdateTS = metaMut.StartTS`, then `CreateTableOrView`
//!   writes it as the `Table:<id>` field of the `DB:<dbID>` hash.
//! * `pkg/ddl/schema_version.go` `updateSchemaVersion` — `GenSchemaVersion`
//!   (`Inc(SchemaVersionKey, 1)`) then `SetSchemaDiff` writes `Diff:<version>`
//!   describing exactly what that version changed.
//!
//! Two deliberate differences from Go, both stated rather than hidden:
//!
//! * **Single owner.** There is no job queue and no owner election. Go moves a
//!   `DROP TABLE` through write-only and delete-only before it deletes the meta
//!   key, because other TiDB nodes may still be reading the table at an older
//!   schema version; this node performs the whole change in one version. That
//!   is only safe while this node is the only writer of the catalog, so a
//!   concurrent DDL must FAIL rather than interleave — see
//!   [`plan_ddl`] on the `SchemaVersionKey` write.
//! * **Bounded surface.** Only the column shapes this node can also serve are
//!   admitted, and every refusal happens in [`lower_ddl`], before a timestamp is
//!   spent or a single byte is written.

use std::fmt;

use tidb_ast::{CiString, ColumnTypeArg, IndexConstraintKind};
use tidb_ast::{
    ColumnDef, ColumnOption, CreateTableStmt, DdlStmt, DropTableStmt, IndexPart, InlineKeyKind,
    PrimaryKeyStorage, Stmt, TableConstraint,
};
use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_meta::{key, value};
use tidb_metadef::MAX_USER_GLOBAL_ID;
use tidb_model::action_type::ActionType;
use tidb_model::column::{ColumnInfo, CURR_LATEST_COLUMN_INFO_VERSION};
use tidb_model::db::DBInfo;
use tidb_model::schema_diff::SchemaDiff;
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::{TableInfo, TABLE_INFO_VERSION5};
use tidb_txnkv::transaction::{MutationSetError, OptimisticMutation};

use crate::cluster_catalog::{
    load_cluster_catalog, ClusterCatalog, ClusterCatalogError, MetaSnapshot,
};

/// The catalog charset every object this node creates carries.
///
/// Go derives these from the session's `character_set_server`/
/// `collation_server`; this node negotiates no such variables, so it writes the
/// TiDB defaults literally. A `SHOW CREATE TABLE` on the real Go server prints
/// exactly this pair for a table created with no explicit charset.
const CATALOG_CHARSET: &str = "utf8mb4";
/// The catalog collation paired with [`CATALOG_CHARSET`].
const CATALOG_COLLATION: &str = "utf8mb4_bin";
/// Go's `binary` charset/collation, used for every non-character column type.
const BINARY_CHARSET: &str = "binary";

/// Go `mysql.MaxIntWidth`: a `BIGINT`'s display width.
const BIGINT_DISPLAY_WIDTH: i64 = 20;
/// Go `mysql.MaxDoubleWidth`: a `DOUBLE`'s display width.
const DOUBLE_DISPLAY_WIDTH: i64 = 22;
/// Go `types.UnspecifiedLength`, which a `DOUBLE` carries as its decimal.
const UNSPECIFIED_LENGTH: i64 = -1;

/// One column shape this node both writes and serves.
///
/// The set is exactly [`crate::cluster_catalog::configure_loaded_table`]'s
/// admitted set, minus the read-only widenings that have no write path: a table
/// this node creates is one it could also load and read back.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmittedColumnType {
    /// `BIGINT` or `BIGINT UNSIGNED`.
    BigInt {
        /// Whether `UNSIGNED` was declared.
        unsigned: bool,
    },
    /// `DOUBLE`.
    Double,
    /// `CHAR(n)`, utf8mb4.
    Char {
        /// Declared character length.
        length: u32,
    },
    /// `VARCHAR(n)`, utf8mb4.
    Varchar {
        /// Declared character length.
        length: u32,
    },
    /// `DECIMAL(p, s)`.
    Decimal {
        /// Declared precision.
        precision: u32,
        /// Declared scale.
        scale: u32,
    },
}

impl AdmittedColumnType {
    /// The stored `FieldType`, byte-for-byte what the Go server writes for the
    /// same declaration (verified against a real cluster's stored `TableInfo`).
    fn field_type(self, not_null: bool, primary_key: bool) -> FieldType {
        let mut field_type = FieldType::new(FieldTypeCode::LongLong);
        match self {
            Self::BigInt { unsigned } => {
                field_type.set_code(FieldTypeCode::LongLong);
                field_type.set_flen(BIGINT_DISPLAY_WIDTH);
                field_type.set_decimal(0);
                field_type.set_charset_name(BINARY_CHARSET);
                field_type.set_collation_name(BINARY_CHARSET);
                if unsigned {
                    field_type.add_flags(FieldTypeFlags::UNSIGNED);
                }
            }
            Self::Double => {
                field_type.set_code(FieldTypeCode::Double);
                field_type.set_flen(DOUBLE_DISPLAY_WIDTH);
                field_type.set_decimal(UNSPECIFIED_LENGTH);
                field_type.set_charset_name(BINARY_CHARSET);
                field_type.set_collation_name(BINARY_CHARSET);
            }
            Self::Char { length } => {
                field_type.set_code(FieldTypeCode::String);
                field_type.set_flen(i64::from(length));
                field_type.set_decimal(0);
                field_type.set_charset_name(CATALOG_CHARSET);
                field_type.set_collation_name(CATALOG_COLLATION);
            }
            Self::Varchar { length } => {
                field_type.set_code(FieldTypeCode::Varchar);
                field_type.set_flen(i64::from(length));
                field_type.set_decimal(0);
                field_type.set_charset_name(CATALOG_CHARSET);
                field_type.set_collation_name(CATALOG_COLLATION);
            }
            Self::Decimal { precision, scale } => {
                field_type.set_code(FieldTypeCode::NewDecimal);
                field_type.set_flen(i64::from(precision));
                field_type.set_decimal(i64::from(scale));
                field_type.set_charset_name(BINARY_CHARSET);
                field_type.set_collation_name(BINARY_CHARSET);
            }
        }
        if not_null {
            field_type.add_flags(FieldTypeFlags::NOT_NULL);
        }
        if primary_key {
            field_type.add_flags(FieldTypeFlags::PRI_KEY);
        }
        // Go sets `NoDefaultValueFlag` on every column that declares neither a
        // DEFAULT nor AUTO_INCREMENT (`ddl.setNoDefaultValueFlag`); this node
        // admits no such option, so every column it writes carries it.
        field_type.add_flags(FieldTypeFlags::NO_DEFAULT_VALUE);
        field_type
    }
}

/// One admitted column of a `CREATE TABLE` this node will write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedColumn {
    /// The declared column name.
    pub name: String,
    /// The declared type.
    pub column_type: AdmittedColumnType,
    /// Whether this column is the table's clustered `BIGINT` handle.
    pub primary_key: bool,
}

/// One catalog change this node knows how to perform.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DdlStatement {
    /// `CREATE DATABASE [IF NOT EXISTS] name`.
    CreateDatabase {
        /// The database name as written.
        name: String,
        /// Whether an existing database is a no-op rather than an error.
        if_not_exists: bool,
    },
    /// `DROP DATABASE [IF EXISTS] name`.
    DropDatabase {
        /// The database name as written.
        name: String,
        /// Whether a missing database is a no-op rather than an error.
        if_exists: bool,
    },
    /// `CREATE TABLE [IF NOT EXISTS] [schema.]table (...)`.
    CreateTable {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// Whether an existing table is a no-op rather than an error.
        if_not_exists: bool,
        /// The admitted columns, in declaration order.
        columns: Vec<AdmittedColumn>,
    },
    /// `DROP TABLE [IF EXISTS] [schema.]table`.
    DropTable {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// Whether a missing table is a no-op rather than an error.
        if_exists: bool,
    },
}

/// Why a statement cannot be performed as written.
///
/// Every variant is produced by [`lower_ddl`], which runs before a PD
/// timestamp is spent and before any snapshot is read, so a refusal never
/// leaves a partial change behind.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdlAdmissionError {
    /// Exact, self-contained explanation naming the offending clause.
    pub reason: String,
}

impl DdlAdmissionError {
    fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl fmt::Display for DdlAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.reason)
    }
}

impl std::error::Error for DdlAdmissionError {}

/// Admits one parsed statement as a catalog change, or explains why not.
///
/// `None` means the statement is not a DDL this module owns at all, so the
/// caller runs it down its ordinary path. `Err` means it *is* one of the four
/// shapes but carries something this node refuses — that refusal is final and
/// happens before any mutation.
pub fn lower_ddl(
    statement: &Stmt,
    default_schema: &str,
) -> Result<Option<DdlStatement>, DdlAdmissionError> {
    let Stmt::Ddl(ddl) = statement else {
        return Ok(None);
    };
    match ddl.as_ref() {
        DdlStmt::CreateDatabase {
            if_not_exists,
            name,
            options,
        } => {
            if !options.is_empty() {
                return Err(DdlAdmissionError::new(
                    "CREATE DATABASE options are not supported by this node; \
                     it writes the server default utf8mb4 / utf8mb4_bin",
                ));
            }
            Ok(Some(DdlStatement::CreateDatabase {
                name: name.clone(),
                if_not_exists: *if_not_exists,
            }))
        }
        DdlStmt::DropDatabase { if_exists, name } => Ok(Some(DdlStatement::DropDatabase {
            name: name.clone(),
            if_exists: *if_exists,
        })),
        DdlStmt::CreateTable(create) => lower_create_table(create, default_schema).map(Some),
        DdlStmt::DropTable(drop) => lower_drop_table(drop, default_schema).map(Some),
        _ => Ok(None),
    }
}

/// Splits a written name path into `(schema, object)`, defaulting the schema.
fn split_name(
    path: &[String],
    default_schema: &str,
    what: &str,
) -> Result<(String, String), DdlAdmissionError> {
    match path {
        [object] => Ok((default_schema.to_owned(), object.clone())),
        [schema, object] => Ok((schema.clone(), object.clone())),
        _ => Err(DdlAdmissionError::new(format!(
            "{what} name `{}` is not a `[schema.]name` path",
            path.join(".")
        ))),
    }
}

fn lower_drop_table(
    drop: &DropTableStmt,
    default_schema: &str,
) -> Result<DdlStatement, DdlAdmissionError> {
    if drop.temporary != tidb_ast::DropTemporary::None {
        return Err(DdlAdmissionError::new(
            "DROP TEMPORARY TABLE is not supported: this node never creates temporary tables",
        ));
    }
    let [name] = drop.names.as_slice() else {
        return Err(DdlAdmissionError::new(
            "DROP TABLE names exactly one table on this node, so a failed drop \
             cannot leave the others half-applied",
        ));
    };
    let (schema, table) = split_name(name, default_schema, "table")?;
    Ok(DdlStatement::DropTable {
        schema,
        table,
        if_exists: drop.if_exists,
    })
}

fn lower_create_table(
    create: &CreateTableStmt,
    default_schema: &str,
) -> Result<DdlStatement, DdlAdmissionError> {
    let refuse = |what: &str| {
        Err(DdlAdmissionError::new(format!(
            "CREATE TABLE {what} is not supported by this node"
        )))
    };
    if create.temporary != tidb_ast::CreateTableTemporary::None {
        return refuse("TEMPORARY");
    }
    if create.like_table.is_some() {
        return refuse("... LIKE");
    }
    if create.ctas.is_some() {
        return refuse("... AS <query>");
    }
    if create.partitioning.is_some() {
        return refuse("PARTITION BY");
    }
    if !create.splits.is_empty() {
        return refuse("SPLIT REGION");
    }
    if !create.table_options.is_empty() {
        return Err(DdlAdmissionError::new(
            "CREATE TABLE options are not supported by this node; it writes the \
             server default ENGINE=InnoDB, utf8mb4 / utf8mb4_bin",
        ));
    }
    let (schema, table) = split_name(&create.name, default_schema, "table")?;

    // The primary key may be declared inline on its column or as a table
    // constraint; both resolve to the same single-column clustered handle.
    let mut constraint_primary_key: Option<String> = None;
    for constraint in &create.table_constraints {
        let TableConstraint::Index(index) = constraint else {
            return refuse("CHECK and FOREIGN KEY constraints");
        };
        if index.kind != IndexConstraintKind::PrimaryKey {
            return refuse("a secondary index");
        }
        if index.options.primary_key_storage == Some(PrimaryKeyStorage::NonClustered) {
            return refuse("PRIMARY KEY ... NONCLUSTERED");
        }
        let [IndexPart::Column {
            name,
            prefix_len: None,
            desc: false,
        }] = index.parts.as_slice()
        else {
            return refuse("a composite, prefixed, descending, or expression PRIMARY KEY");
        };
        if constraint_primary_key.replace(name.clone()).is_some() {
            return refuse("more than one PRIMARY KEY");
        }
    }

    let mut columns = Vec::with_capacity(create.columns.len());
    let mut inline_primary_key = None;
    for column in &create.columns {
        let (admitted, inline_pk) = lower_column(column)?;
        if inline_pk && inline_primary_key.replace(admitted.name.clone()).is_some() {
            return refuse("more than one PRIMARY KEY");
        }
        columns.push(admitted);
    }
    if columns.is_empty() {
        return Err(DdlAdmissionError::new("CREATE TABLE declares no columns"));
    }

    let primary_key = match (inline_primary_key, constraint_primary_key) {
        (Some(_), Some(_)) => return refuse("more than one PRIMARY KEY"),
        (Some(name), None) | (None, Some(name)) => name,
        (None, None) => {
            return Err(DdlAdmissionError::new(
                "CREATE TABLE requires a single-column clustered BIGINT PRIMARY KEY: \
                 this node stores every row under its primary key as the handle",
            ))
        }
    };
    let mut marked = false;
    for column in &mut columns {
        if column.name.to_lowercase() == primary_key.to_lowercase() {
            if column.column_type != (AdmittedColumnType::BigInt { unsigned: false }) {
                return Err(DdlAdmissionError::new(format!(
                    "PRIMARY KEY column `{}` must be a signed BIGINT to serve as the \
                     clustered row handle",
                    column.name
                )));
            }
            column.primary_key = true;
            marked = true;
        }
    }
    if !marked {
        return Err(DdlAdmissionError::new(format!(
            "PRIMARY KEY names column `{primary_key}`, which the table does not declare"
        )));
    }

    let mut seen = Vec::with_capacity(columns.len());
    for column in &columns {
        let lowered = column.name.to_lowercase();
        if seen.contains(&lowered) {
            return Err(DdlAdmissionError::new(format!(
                "CREATE TABLE declares column `{}` twice",
                column.name
            )));
        }
        seen.push(lowered);
    }

    Ok(DdlStatement::CreateTable {
        schema,
        table,
        if_not_exists: create.if_not_exists,
        columns,
    })
}

/// Admits one column definition, returning it and whether it declared the
/// primary key inline.
fn lower_column(column: &ColumnDef) -> Result<(AdmittedColumn, bool), DdlAdmissionError> {
    let name = &column.name;
    if !column.qualifier.is_empty() {
        return Err(DdlAdmissionError::new(format!(
            "column `{name}` carries a qualifier, which CREATE TABLE does not accept here"
        )));
    }
    let mut not_null = false;
    let mut inline_primary_key = false;
    for option in &column.options {
        match option {
            ColumnOption::NotNull => not_null = true,
            ColumnOption::InlineKey(inline) => match inline.kind {
                InlineKeyKind::Primary { storage } => {
                    if storage == Some(PrimaryKeyStorage::NonClustered) {
                        return Err(DdlAdmissionError::new(format!(
                            "column `{name}` declares a NONCLUSTERED PRIMARY KEY, which this \
                             node cannot store"
                        )));
                    }
                    if inline.global {
                        return Err(DdlAdmissionError::new(format!(
                            "column `{name}` declares a GLOBAL PRIMARY KEY, which this node \
                             cannot store"
                        )));
                    }
                    inline_primary_key = true;
                    // Go's `PRIMARY KEY` implies `NOT NULL`.
                    not_null = true;
                }
                InlineKeyKind::Unique => {
                    return Err(DdlAdmissionError::new(format!(
                        "column `{name}` declares UNIQUE, and this node maintains no unique index"
                    )))
                }
            },
            other => {
                return Err(DdlAdmissionError::new(format!(
                    "column `{name}` carries {}, which this node does not support",
                    describe_column_option(other)
                )))
            }
        }
    }
    if !not_null {
        return Err(DdlAdmissionError::new(format!(
            "column `{name}` must be declared NOT NULL: this node decodes only NOT NULL columns"
        )));
    }
    let column_type = lower_column_type(column)?;
    Ok((
        AdmittedColumn {
            name: name.clone(),
            column_type,
            primary_key: false,
        },
        inline_primary_key,
    ))
}

fn describe_column_option(option: &ColumnOption) -> &'static str {
    match option {
        ColumnOption::Null => "an explicit NULL",
        ColumnOption::AutoIncrement => "AUTO_INCREMENT",
        ColumnOption::Default(_) => "a DEFAULT",
        ColumnOption::Generated { .. } => "a generated expression",
        ColumnOption::OnUpdate(_) => "ON UPDATE",
        ColumnOption::Comment(_) => "a COMMENT",
        ColumnOption::Collate(_) => "an explicit COLLATE",
        ColumnOption::Check(_) => "a CHECK constraint",
        ColumnOption::Reference(_) => "a REFERENCES clause",
        ColumnOption::ColumnFormat(_) => "COLUMN_FORMAT",
        ColumnOption::Storage(_) => "STORAGE",
        ColumnOption::AutoRandom(_) => "AUTO_RANDOM",
        ColumnOption::SecondaryEngineAttribute(_) => "SECONDARY_ENGINE_ATTRIBUTE",
        ColumnOption::MariaDbRowStart => "ROW START",
        ColumnOption::MariaDbRowEnd => "ROW END",
        // Handled by the caller before it reaches this description.
        ColumnOption::NotNull | ColumnOption::InlineKey(_) => "a key or nullability option",
    }
}

/// One integer argument of a parenthesized type declaration.
fn type_argument(
    column_name: &str,
    type_name: &str,
    argument: &ColumnTypeArg,
) -> Result<u32, DdlAdmissionError> {
    let ColumnTypeArg::Text(text) = argument else {
        return Err(DdlAdmissionError::new(format!(
            "column `{column_name}` declares {type_name} with a non-numeric argument"
        )));
    };
    text.parse().map_err(|_| {
        DdlAdmissionError::new(format!(
            "column `{column_name}` declares {type_name}({text}), whose argument is not a \
             non-negative integer this node can store"
        ))
    })
}

fn lower_column_type(column: &ColumnDef) -> Result<AdmittedColumnType, DdlAdmissionError> {
    let name = &column.name;
    let declared = &column.ty;
    if declared.zerofill {
        return Err(DdlAdmissionError::new(format!(
            "column `{name}` declares ZEROFILL"
        )));
    }
    if declared.binary || declared.charset.is_some() {
        return Err(DdlAdmissionError::new(format!(
            "column `{name}` declares an explicit charset or BINARY modifier; this node \
             writes utf8mb4 / utf8mb4_bin for character columns"
        )));
    }
    let unsupported = || {
        DdlAdmissionError::new(format!(
            "column `{name}` has type {}, which this node cannot store; it accepts \
             BIGINT [UNSIGNED], DOUBLE, CHAR(n), VARCHAR(n), and DECIMAL(p,s)",
            declared.name
        ))
    };
    match (declared.name.as_str(), declared.args.as_slice()) {
        // A written display width (`BIGINT(20)`) is accepted and ignored, as
        // Go's own deprecation path does: the stored flen is always 20.
        ("BIGINT", [] | [_]) => Ok(AdmittedColumnType::BigInt {
            unsigned: declared.unsigned,
        }),
        ("DOUBLE", []) if !declared.unsigned => Ok(AdmittedColumnType::Double),
        ("CHAR", [length]) if !declared.unsigned => Ok(AdmittedColumnType::Char {
            length: type_argument(name, "CHAR", length)?,
        }),
        ("VARCHAR", [length]) if !declared.unsigned => Ok(AdmittedColumnType::Varchar {
            length: type_argument(name, "VARCHAR", length)?,
        }),
        ("DECIMAL", [precision, scale]) if !declared.unsigned => Ok(AdmittedColumnType::Decimal {
            precision: type_argument(name, "DECIMAL", precision)?,
            scale: type_argument(name, "DECIMAL", scale)?,
        }),
        _ => Err(unsupported()),
    }
}

/// Why a planned catalog change cannot be built from the observed snapshot.
#[derive(Clone, Debug)]
pub enum DdlPlanError {
    /// The catalog could not be read or decoded.
    Catalog(ClusterCatalogError),
    /// The named database is not in the catalog.
    UnknownDatabase(String),
    /// The named database is already in the catalog.
    DatabaseExists(String),
    /// The named table is not in the named database.
    UnknownTable {
        /// The database name as written.
        schema: String,
        /// The table name as written.
        table: String,
    },
    /// The named table is already in the named database.
    TableExists {
        /// The database name as written.
        schema: String,
        /// The table name as written.
        table: String,
    },
    /// Go `GenGlobalIDs`' own limit: the user ID space is exhausted.
    GlobalIdExhausted {
        /// The ID the allocation would have reached.
        wanted: i64,
    },
    /// A catalog object could not be encoded.
    Encode(String),
    /// The mutation set was rejected before it could be published.
    Mutations(MutationSetError),
}

impl fmt::Display for DdlPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => write!(formatter, "{error}"),
            Self::UnknownDatabase(name) => write!(formatter, "Unknown database '{name}'"),
            Self::DatabaseExists(name) => {
                write!(formatter, "Can't create database '{name}'; database exists")
            }
            Self::UnknownTable { schema, table } => {
                write!(formatter, "Unknown table '{schema}.{table}'")
            }
            Self::TableExists { schema, table } => {
                write!(formatter, "Table '{schema}.{table}' already exists")
            }
            Self::GlobalIdExhausted { wanted } => write!(
                formatter,
                "global id:{wanted} exceeds the limit:{MAX_USER_GLOBAL_ID}"
            ),
            Self::Encode(detail) => write!(formatter, "catalog encode failed: {detail}"),
            Self::Mutations(error) => write!(formatter, "catalog mutations: {error}"),
        }
    }
}

impl std::error::Error for DdlPlanError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Catalog(error) => Some(error),
            Self::Mutations(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ClusterCatalogError> for DdlPlanError {
    fn from(error: ClusterCatalogError) -> Self {
        Self::Catalog(error)
    }
}

impl From<MutationSetError> for DdlPlanError {
    fn from(error: MutationSetError) -> Self {
        Self::Mutations(error)
    }
}

/// What one planned catalog change will publish.
#[derive(Clone, Debug)]
pub enum DdlPlan {
    /// The change is already true (`IF [NOT] EXISTS` was satisfied), so nothing
    /// is written and no schema version is spent.
    AlreadySatisfied {
        /// Human-readable statement of what was already true.
        detail: String,
    },
    /// The mutations to publish in one transaction.
    Write(Box<DdlWrite>),
}

/// One catalog change's complete write set.
#[derive(Clone, Debug)]
pub struct DdlWrite {
    /// Every meta-key mutation, in a deterministic order.
    pub mutations: Vec<OptimisticMutation>,
    /// The schema version this change produces.
    pub schema_version: i64,
    /// The diff stored under `Diff:<schema_version>`.
    pub diff: SchemaDiff,
    /// The object the change created, if it created one.
    pub created_id: Option<i64>,
}

/// Plans one catalog change against one snapshot.
///
/// Everything is read at the one snapshot and every mutation is published in
/// the one transaction that owns it, so the change is atomic: the object, the
/// version bump, and the diff that makes the version readable all land or none
/// do.
///
/// **Concurrent DDL fails loudly.** `SchemaVersionKey` is always in the write
/// set, and it is written from a value this snapshot read. Under optimistic
/// 2PC, TiKV's Prewrite rejects a key whose latest commit is newer than the
/// transaction's `start_ts`, so any other DDL that committed in between — this
/// node's or a real TiDB's — turns this transaction into a definite
/// `WriteConflict` rather than an interleaved half-change. There is no owner
/// election here; that conflict IS the mutual exclusion.
pub fn plan_ddl<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    start_ts: u64,
) -> Result<DdlPlan, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    let schema_version = catalog.schema_version + 1;
    let mut writes = Vec::new();
    let mut created_id = None;
    let mut diff = SchemaDiff {
        version: schema_version,
        ..SchemaDiff::default()
    };

    match statement {
        DdlStatement::CreateDatabase {
            name,
            if_not_exists,
        } => {
            if let Some(existing) = find_database(&catalog, name) {
                if *if_not_exists {
                    return Ok(already(format!(
                        "database `{}` already exists",
                        existing.info.name.original()
                    )));
                }
                return Err(DdlPlanError::DatabaseExists(name.clone()));
            }
            let db_id = allocate(snapshot, &mut writes, 1)?[0];
            created_id = Some(db_id);
            let info = DBInfo {
                id: db_id,
                name: CiString::new(name.clone()),
                charset: CATALOG_CHARSET.to_owned(),
                collate: CATALOG_COLLATION.to_owned(),
                state: SchemaState::PUBLIC,
                ..DBInfo::default()
            };
            let encoded = value::serialize_db_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::database_kv_key(db_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_CREATE_SCHEMA;
            diff.schema_id = db_id;
        }
        DdlStatement::DropDatabase { name, if_exists } => {
            let Some(database) = find_database(&catalog, name) else {
                if *if_exists {
                    return Ok(already(format!("database `{name}` does not exist")));
                }
                return Err(DdlPlanError::UnknownDatabase(name.clone()));
            };
            let db_id = database.info.id;
            // Go `Mutator.DropDatabase` is `HClear(DB:<id>)` then
            // `HDel(DBs, DB:<id>)`: every field of the database's own hash —
            // its `Table:<id>` entries and its per-table ID allocators — goes
            // with it. Only fields this snapshot actually observed are deleted.
            for (raw_key, _) in snapshot.scan_prefix(&key::database_metas_kv_prefix(db_id))? {
                writes.push(OptimisticMutation::meta_delete(raw_key)?);
            }
            writes.push(OptimisticMutation::meta_delete(key::database_kv_key(
                db_id,
            ))?);
            diff.action_type = ActionType::ACTION_DROP_SCHEMA;
            diff.schema_id = db_id;
        }
        DdlStatement::CreateTable {
            schema,
            table,
            if_not_exists,
            columns,
        } => {
            let Some(database) = find_database(&catalog, schema) else {
                return Err(DdlPlanError::UnknownDatabase(schema.clone()));
            };
            if let Some(existing) = find_table(database, table) {
                if *if_not_exists {
                    return Ok(already(format!(
                        "table `{schema}`.`{}` already exists",
                        existing.name.original()
                    )));
                }
                return Err(DdlPlanError::TableExists {
                    schema: schema.clone(),
                    table: table.clone(),
                });
            }
            let db_id = database.info.id;
            let table_id = allocate(snapshot, &mut writes, 1)?[0];
            created_id = Some(table_id);
            let info = build_table_info(table_id, table, columns, start_ts);
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_CREATE_TABLE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::DropTable {
            schema,
            table,
            if_exists,
        } => {
            let Some(database) = find_database(&catalog, schema) else {
                if *if_exists {
                    return Ok(already(format!(
                        "table `{schema}`.`{table}` does not exist"
                    )));
                }
                return Err(DdlPlanError::UnknownDatabase(schema.clone()));
            };
            let Some(stored) = find_table(database, table) else {
                if *if_exists {
                    return Ok(already(format!(
                        "table `{schema}`.`{table}` does not exist"
                    )));
                }
                return Err(DdlPlanError::UnknownTable {
                    schema: schema.clone(),
                    table: table.clone(),
                });
            };
            let db_id = database.info.id;
            let table_id = stored.id;
            writes.push(OptimisticMutation::meta_delete(key::table_kv_key(
                db_id, table_id,
            ))?);
            // Go `GetAutoIDAccessors(dbID, tblID).Del()` removes the three
            // allocator fields with the table; each is deleted only if this
            // snapshot observed it, exactly as `HDel` does.
            for allocator in [
                key::auto_table_id_kv_key(db_id, table_id),
                key::auto_increment_id_kv_key(db_id, table_id),
                key::auto_random_table_id_kv_key(db_id, table_id),
            ] {
                if snapshot.get(&allocator)?.is_some() {
                    writes.push(OptimisticMutation::meta_delete(allocator)?);
                }
            }
            diff.action_type = ActionType::ACTION_DROP_TABLE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
    }

    // The version bump comes last so the write set always ends with the two
    // keys that make the change observable — and the version key is what a
    // concurrent DDL collides with.
    writes.push(OptimisticMutation::meta_put(
        key::schema_version_kv_key(),
        value::encode_int_value(schema_version),
    )?);
    let encoded_diff = value::serialize_schema_diff(&diff)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    writes.push(OptimisticMutation::meta_put(
        key::schema_diff_kv_key(schema_version),
        encoded_diff,
    )?);

    Ok(DdlPlan::Write(Box::new(DdlWrite {
        mutations: writes,
        schema_version,
        diff,
        created_id,
    })))
}

fn already(detail: String) -> DdlPlan {
    DdlPlan::AlreadySatisfied { detail }
}

/// Go `GenGlobalIDs(n)`: `Inc(NextGlobalID, n)` answers the new maximum, and
/// the allocated IDs are the `n` values ending there.
///
/// The key holds the max USED id, never a next-free one, so the increment IS
/// the allocation. The new maximum is written from the value this snapshot
/// read, which is what makes a competing allocation a write conflict rather
/// than a duplicate ID.
fn allocate<S: MetaSnapshot>(
    snapshot: &mut S,
    writes: &mut Vec<OptimisticMutation>,
    count: i64,
) -> Result<Vec<i64>, DdlPlanError> {
    let current = match snapshot.get(&key::next_global_id_kv_key())? {
        Some(stored) => value::parse_int_value(&stored)
            .map_err(|error| DdlPlanError::Encode(format!("NextGlobalID: {error}")))?,
        // Go's `Inc` treats a missing key as zero.
        None => 0,
    };
    let new_max = current
        .checked_add(count)
        .ok_or(DdlPlanError::GlobalIdExhausted { wanted: i64::MAX })?;
    if new_max > MAX_USER_GLOBAL_ID {
        return Err(DdlPlanError::GlobalIdExhausted { wanted: new_max });
    }
    writes.push(OptimisticMutation::meta_put(
        key::next_global_id_kv_key(),
        value::encode_int_value(new_max),
    )?);
    Ok(((current + 1)..=new_max).collect())
}

fn find_database<'catalog>(
    catalog: &'catalog ClusterCatalog,
    name: &str,
) -> Option<&'catalog crate::cluster_catalog::LoadedDatabase> {
    let name = name.to_lowercase();
    catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == name)
}

fn find_table<'database>(
    database: &'database crate::cluster_catalog::LoadedDatabase,
    name: &str,
) -> Option<&'database TableInfo> {
    let name = name.to_lowercase();
    database
        .tables
        .iter()
        .find(|table| table.name.lowercase() == name)
}

/// Builds the `TableInfo` a real Go TiDB would have written for the same
/// declaration.
///
/// A clustered signed-`BIGINT` primary key is stored as `pk_is_handle` with NO
/// `IndexInfo` of its own: the row's key IS the primary key, so Go's
/// `BuildTableInfo` records the fact in the flag and the column's `PriKeyFlag`
/// and creates no index. Column IDs start at 1 and `max_col_id` records the
/// highest one handed out, which is what a later `ADD COLUMN` continues from.
fn build_table_info(
    table_id: i64,
    name: &str,
    columns: &[AdmittedColumn],
    start_ts: u64,
) -> TableInfo {
    let stored_columns: Vec<ColumnInfo> = columns
        .iter()
        .enumerate()
        .map(|(offset, column)| {
            let column_id = i64::try_from(offset).expect("a column offset fits in i64") + 1;
            ColumnInfo {
                id: column_id,
                name: CiString::new(column.name.clone()),
                offset: i32::try_from(offset).expect("a column offset fits in i32"),
                origin_default_value: None,
                origin_default_value_bit: None,
                default_value: None,
                default_value_bit: None,
                default_is_expr: false,
                generated_expr_string: String::new(),
                generated_stored: false,
                dependences: std::collections::BTreeSet::new(),
                field_type: column.column_type.field_type(true, column.primary_key),
                changing_field_type: None,
                state: SchemaState::PUBLIC,
                comment: String::new(),
                hidden: false,
                change_state_info: None,
                version: CURR_LATEST_COLUMN_INFO_VERSION,
            }
        })
        .collect();
    let max_column_id = i64::try_from(stored_columns.len()).expect("a column count fits in i64");
    TableInfo {
        id: table_id,
        name: CiString::new(name.to_owned()),
        charset: CATALOG_CHARSET.to_owned(),
        collate: CATALOG_COLLATION.to_owned(),
        columns: stored_columns,
        state: SchemaState::PUBLIC,
        pk_is_handle: true,
        max_column_id,
        // Go `createTable` stamps the job transaction's own start timestamp.
        update_ts: start_ts,
        version: TABLE_INFO_VERSION5,
        ..TableInfo::default()
    }
}
