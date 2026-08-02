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

//! Writing the catalog: `CREATE`/`DROP` for databases, tables and indexes,
//! planned as one set of meta-key mutations over one snapshot.
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
//!
//!   `CREATE INDEX` widens that assumption, and this is the one place it is
//!   written down: Go's `delete only` -> `write only` -> `reorg` -> `public`
//!   ladder exists so a concurrent `INSERT` maintains the half-built index
//!   while the reorg scans. This node has no such states — the index and every
//!   entry the existing rows owe it become visible at ONE commit — so a row
//!   another writer commits between this transaction's `start_ts` and its
//!   commit is indexed by neither the scan nor the writer. The assumption is
//!   therefore no longer "no concurrent DDL" but "no concurrent WRITE to the
//!   table being indexed", and unlike the DDL half it is NOT enforced by a
//!   write conflict.
//! * **Bounded surface.** Only the column shapes this node can also serve are
//!   admitted, and every refusal happens in [`lower_ddl`], before a timestamp is
//!   spent or a single byte is written.

use std::fmt;

use tidb_ast::CiString;
use tidb_ast::{CreateIndexStmt, CreateTableStmt, DdlStmt, DropIndexStmt, DropTableStmt, Stmt};
use tidb_meta::{key, value};
use tidb_metadef::MAX_USER_GLOBAL_ID;
use tidb_model::action_type::ActionType;
use tidb_model::db::DBInfo;
use tidb_model::index::{IndexColumn, IndexInfo};
use tidb_model::schema_diff::SchemaDiff;
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;
use tidb_txnkv::transaction::{MutationSetError, OptimisticMutation};

use crate::cluster_catalog::{
    load_cluster_catalog, ClusterCatalog, ClusterCatalogError, MetaSnapshot,
};
use crate::table_info_build::{build_table_info, ClusteredIndexDefMode};

pub use crate::table_info_build::DdlAdmissionError;

/// The catalog charset every object this node creates carries.
///
/// Go derives these from the session's `character_set_server`/
/// `collation_server`; this node negotiates no such variables, so it writes the
/// TiDB defaults literally. A `SHOW CREATE TABLE` on the real Go server prints
/// exactly this pair for a table created with no explicit charset.
const CATALOG_CHARSET: &str = "utf8mb4";
/// The catalog collation paired with [`CATALOG_CHARSET`].
const CATALOG_COLLATION: &str = "utf8mb4_bin";

/// One catalog change this node knows how to perform.
///
/// `CreateTable` carries a whole `TableInfo`, which has no equality of its own
/// (a catalog object is compared by its serialized bytes, not structurally), so
/// this enum is `Debug`/`Clone` only.
#[derive(Clone, Debug)]
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
        /// The `TableInfo` this statement lowers to, complete except for the
        /// ID and timestamp the publishing transaction assigns.
        template: Box<TableInfo>,
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
    /// `CREATE [UNIQUE] INDEX name ON [schema.]table (columns)`.
    ///
    /// Unlike the other four, this one changes DATA as well as metadata: the
    /// rows the table already holds each need an index entry. Both halves are
    /// published in the one transaction — see [`DdlWrite::backfill`].
    CreateIndex {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// Whether an existing index of the same name is a no-op.
        if_not_exists: bool,
        /// The index to add, complete except for the ID and the column
        /// offsets the publishing transaction resolves against the stored
        /// table.
        index: Box<IndexInfo>,
    },
    /// `DROP INDEX name ON [schema.]table`.
    DropIndex {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The index name as written.
        index: String,
        /// Whether a missing index is a no-op rather than an error.
        if_exists: bool,
    },
}

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
        DdlStmt::CreateIndex(create) => lower_create_index(create, default_schema).map(Some),
        DdlStmt::DropIndex(drop) => lower_drop_index(drop, default_schema).map(Some),
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
    let (schema, table) = split_name(&create.name, default_schema, "table")?;
    // The server default `tidb_enable_clustered_index = ON`, which is what a
    // real TiDB builds a user table under. Bootstrap is the one caller that
    // uses a different mode, and it says so at its own call site.
    let template = build_table_info(
        create,
        CATALOG_CHARSET,
        CATALOG_COLLATION,
        ClusteredIndexDefMode::On,
    )?;
    Ok(DdlStatement::CreateTable {
        schema,
        table,
        if_not_exists: create.if_not_exists,
        template: Box::new(template),
    })
}

/// Admits a `CREATE INDEX`, refusing every shape whose entries this node would
/// not go on to maintain.
///
/// The gate is not a taste judgement: [`crate::cluster_catalog`]'s loader and
/// the session's table builder refuse a prefix index and a generated column
/// outright, so publishing one here would write a `TableInfo` this very node
/// then drops from its own catalog — the table would vanish from the
/// connection that just indexed it. Each refusal names which half cannot carry
/// the shape.
fn lower_create_index(
    create: &CreateIndexStmt,
    default_schema: &str,
) -> Result<DdlStatement, DdlAdmissionError> {
    let (schema, table) = split_name(&create.table, default_schema, "table")?;
    let unique = match create.kind {
        tidb_ast::IndexKind::Ordinary => false,
        tidb_ast::IndexKind::Unique => true,
        other => {
            return Err(DdlAdmissionError::unsupported(format!(
                "CREATE {} INDEX is not supported by this node",
                other.sql()
            )))
        }
    };
    if create.options.condition.is_some() {
        return Err(DdlAdmissionError::unsupported(
            "a partial index (CREATE INDEX ... WHERE) is not supported by this node: \
             nothing here evaluates the condition, so every row would be indexed under \
             a partial index's name",
        ));
    }
    if create.options.global {
        return Err(DdlAdmissionError::unsupported(
            "a GLOBAL index is not supported by this node, which does not serve \
             partitioned tables",
        ));
    }
    let mut columns = Vec::with_capacity(create.parts.len());
    for part in &create.parts {
        let tidb_ast::IndexPart::Column {
            name, prefix_len, ..
        } = part
        else {
            return Err(DdlAdmissionError::unsupported(
                "an expression index is not supported by this node: it is stored as a \
                 hidden GENERATED column, which this node's catalog loader refuses",
            ));
        };
        if prefix_len.is_some() {
            return Err(DdlAdmissionError::unsupported(
                "a prefix-length index is not supported by this node, which neither \
                 reads nor writes entries cut to a prefix",
            ));
        }
        columns.push(IndexColumn {
            name: CiString::new(name.clone()),
            // Resolved against the stored table when the change is planned.
            offset: 0,
            length: -1,
            ..IndexColumn::default()
        });
    }
    if columns.is_empty() {
        return Err(DdlAdmissionError::new("CREATE INDEX names no column"));
    }
    Ok(DdlStatement::CreateIndex {
        schema,
        table,
        if_not_exists: create.if_not_exists,
        index: Box::new(IndexInfo {
            // The publishing transaction allocates it from the table's own
            // space, which is `TableInfo.MaxIndexID` and not the global one.
            id: 0,
            name: CiString::new(create.name.clone()),
            columns,
            state: SchemaState::PUBLIC,
            comment: create.options.comment.clone().unwrap_or_default(),
            tp: create
                .options
                .index_type
                .unwrap_or(tidb_ast::IndexType::BTREE),
            unique,
            primary: false,
            invisible: create.options.visibility == Some(tidb_ast::IndexVisibility::Invisible),
            ..IndexInfo::default()
        }),
    })
}

fn lower_drop_index(
    drop: &DropIndexStmt,
    default_schema: &str,
) -> Result<DdlStatement, DdlAdmissionError> {
    let (schema, table) = split_name(&drop.table, default_schema, "table")?;
    if drop.is_hypo {
        return Err(DdlAdmissionError::unsupported(
            "DROP HYPO INDEX is not supported: this node creates no hypothetical indexes",
        ));
    }
    Ok(DdlStatement::DropIndex {
        schema,
        table,
        index: drop.name.clone(),
        if_exists: drop.if_exists,
    })
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
    /// The table already has an index of that name (Go 1061).
    DuplicateKeyName(String),
    /// The named index is not on the named table (Go 1091).
    UnknownIndex(String),
    /// The index names a column the table does not have (Go 1072).
    UnknownIndexColumn {
        /// The column name as written.
        column: String,
        /// The index name as written.
        index: String,
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
            Self::DuplicateKeyName(name) => write!(formatter, "Duplicate key name '{name}'"),
            Self::UnknownIndex(name) => {
                write!(formatter, "index {name} doesn't exist")
            }
            Self::UnknownIndexColumn { column, index } => write!(
                formatter,
                "Key column '{column}' doesn't exist in table (index {index})"
            ),
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
    /// The index entries the change also has to write or remove, if any.
    ///
    /// `CREATE INDEX` is the first change on this path whose correctness is not
    /// finished by the meta keys: an index whose existing rows were never
    /// scanned exists in the catalog and answers queries with the rows it
    /// happens to hold, which is a silent wrong answer rather than an error.
    /// The entries therefore ride the SAME transaction as the meta mutations
    /// (see [`crate::real_tikv_ddl::commit_cluster_ddl_with_backfill`]), so the
    /// index and its contents become visible at one commit timestamp and no
    /// reader can see one without the other.
    pub backfill: Option<IndexBackfill>,
}

/// The data half of an index change: which table's rows to walk, and what to
/// do with the entries.
///
/// The table is carried as it was BEFORE the change, because that is the shape
/// its stored rows have; the index is carried with its ID and offsets already
/// resolved, so the walker needs nothing but this.
#[derive(Clone, Debug)]
pub struct IndexBackfill {
    /// The table as the snapshot holds it, before this change.
    pub table: Box<TableInfo>,
    /// The index whose entries are to be written or removed.
    pub index: Box<IndexInfo>,
    /// Whether the entries are being written (`CREATE INDEX`) or removed
    /// (`DROP INDEX`).
    pub add: bool,
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
    let mut backfill = None;
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
            template,
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
            let mut info = TableInfo::clone(template);
            info.id = table_id;
            // Go `createTable` stamps the job transaction's own start timestamp.
            info.update_ts = start_ts;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            // Go `handleAutoIncID` seeds the allocator when the table option
            // asks for a first id above 1, and seeds it to `AutoIncID - 1`,
            // because the counter holds the id last handed out: "if the option
            // sets auto_increment to 10, the counter will be set to 9, so the
            // next allocated ID will be 10". At or below 1 it writes nothing,
            // and an absent key already reads as 0. Which key it is, is Go's
            // `SepAutoInc` choice, made in one place.
            if info.auto_inc_id > 1 {
                writes.push(OptimisticMutation::meta_put(
                    crate::cluster_auto_id::auto_id_key_for(db_id, &info),
                    value::encode_int_value(info.auto_inc_id - 1),
                )?);
            }
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
        DdlStatement::CreateIndex {
            schema,
            table,
            if_not_exists,
            index,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            if let Some(existing) = find_index(stored, index.name.original()) {
                if *if_not_exists {
                    return Ok(already(format!(
                        "index `{}` already exists on `{schema}`.`{table}`",
                        existing.name.original()
                    )));
                }
                return Err(DdlPlanError::DuplicateKeyName(
                    index.name.original().to_owned(),
                ));
            }
            let mut added = IndexInfo::clone(index);
            // Go's `IndexColumn.Offset` is a position in `TableInfo.Columns`,
            // and the loader reads it back that way, so it is resolved against
            // the stored table rather than trusted from the statement.
            for column in &mut added.columns {
                let Some(stored_column) = stored
                    .columns
                    .iter()
                    .find(|candidate| candidate.name.lowercase() == column.name.lowercase())
                else {
                    return Err(DdlPlanError::UnknownIndexColumn {
                        column: column.name.original().to_owned(),
                        index: index.name.original().to_owned(),
                    });
                };
                column.name = stored_column.name.clone();
                column.offset = stored_column.offset;
            }
            let mut info = TableInfo::clone(stored);
            info.max_index_id += 1;
            added.id = info.max_index_id;
            added.table = info.name.clone();
            info.indices.push(added.clone());
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            backfill = Some(IndexBackfill {
                table: Box::new(stored.clone()),
                index: Box::new(added),
                add: true,
            });
            diff.action_type = ActionType::ACTION_ADD_INDEX;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::DropIndex {
            schema,
            table,
            index,
            if_exists,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let Some(dropped) = find_index(stored, index).cloned() else {
                if *if_exists {
                    return Ok(already(format!(
                        "index `{index}` does not exist on `{schema}`.`{table}`"
                    )));
                }
                return Err(DdlPlanError::UnknownIndex(index.clone()));
            };
            let mut info = TableInfo::clone(stored);
            info.indices.retain(|candidate| candidate.id != dropped.id);
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            // Go moves a dropped index through `delete only` and hands its key
            // range to the delete-range GC worker; this node is the single
            // catalog writer and removes the entries in the same transaction,
            // for the same reason it drops a table in one version. Leaving them
            // behind would be worse than untidy: `TableInfo.MaxIndexID` never
            // goes down, but a later index on the same table would still walk
            // the same rows, and a stale entry under a REUSED id — which a
            // restored or rebuilt table can produce — reads as a row that is
            // not there.
            backfill = Some(IndexBackfill {
                table: Box::new(stored.clone()),
                index: Box::new(dropped),
                add: false,
            });
            diff.action_type = ActionType::ACTION_DROP_INDEX;
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
        backfill,
    })))
}

fn already(detail: String) -> DdlPlan {
    DdlPlan::AlreadySatisfied { detail }
}

/// Resolves `schema`.`table` to its database ID and stored `TableInfo`.
///
/// An index change has no `IF EXISTS` for the TABLE, only for the index, so a
/// missing database or table is always an error here.
fn locate_table<'catalog>(
    catalog: &'catalog ClusterCatalog,
    schema: &str,
    table: &str,
) -> Result<(i64, &'catalog TableInfo), DdlPlanError> {
    let Some(database) = find_database(catalog, schema) else {
        return Err(DdlPlanError::UnknownDatabase(schema.to_owned()));
    };
    let Some(stored) = find_table(database, table) else {
        return Err(DdlPlanError::UnknownTable {
            schema: schema.to_owned(),
            table: table.to_owned(),
        });
    };
    Ok((database.info.id, stored))
}

/// The table's index of that name, matched the way MySQL matches one:
/// case-insensitively.
fn find_index<'table>(table: &'table TableInfo, name: &str) -> Option<&'table IndexInfo> {
    table
        .indices
        .iter()
        .find(|index| index.name.original().eq_ignore_ascii_case(name))
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
