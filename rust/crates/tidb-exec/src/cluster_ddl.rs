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

use std::collections::BTreeMap;
use std::fmt;

use tidb_ast::CiString;
use tidb_ast::{
    AlterTableStmt, CreateIndexStmt, CreateTableStmt, DatabaseOption, DdlStmt, DropIndexStmt,
    DropTableStmt, IndexConstraintDefinition, IndexConstraintKind, RenameTableStmt, Stmt,
};
use tidb_datatype::new_collation_enabled;
use tidb_datatype::{FieldTypeCode, FieldTypeFlags};
use tidb_meta::{key, value};
use tidb_metadef::MAX_USER_GLOBAL_ID;
use tidb_model::action_type::ActionType;
use tidb_model::db::DBInfo;
use tidb_model::index::{IndexColumn, IndexInfo};
use tidb_model::schema_diff::{AffectedOption, SchemaDiff};
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;
use tidb_model::GoShared;
use tidb_txnkv::transaction::{MutationSetError, OptimisticMutation};

use crate::cluster_catalog::{
    load_cluster_catalog, ClusterCatalog, ClusterCatalogError, MetaSnapshot,
};
use crate::table_info_build::{
    build_table_info_with_context, default_ddl_statement_context, resolve_charset_collation,
    ClusteredIndexDefMode,
};

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

/// A validated `CREATE TABLE` recipe whose final metadata waits for `DBInfo`.
#[derive(Clone)]
pub struct CreateTableBuild {
    create: CreateTableStmt,
    context: tidb_executor::StmtContext,
    template: TableInfo,
}

impl std::fmt::Debug for CreateTableBuild {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CreateTableBuild")
            .field("create", &self.create)
            .field("template", &self.template)
            .finish_non_exhaustive()
    }
}

impl CreateTableBuild {
    fn new(
        create: &CreateTableStmt,
        context: &tidb_executor::StmtContext,
    ) -> Result<Self, DdlAdmissionError> {
        let template = build_table_info_with_context(
            create,
            CATALOG_CHARSET,
            CATALOG_COLLATION,
            ClusteredIndexDefMode::On,
            context,
        )?;
        Ok(Self {
            create: create.clone(),
            context: context.clone(),
            template,
        })
    }

    /// The validated server-default build used by admission tests and mocks.
    #[must_use]
    pub const fn template(&self) -> &TableInfo {
        &self.template
    }

    /// Builds the final metadata after the owning `DBInfo` has been loaded.
    pub fn for_database(
        &self,
        charset: &str,
        collate: &str,
    ) -> Result<TableInfo, DdlAdmissionError> {
        if charset.eq_ignore_ascii_case(CATALOG_CHARSET)
            && collate.eq_ignore_ascii_case(CATALOG_COLLATION)
        {
            return Ok(self.template.clone());
        }
        build_table_info_with_context(
            &self.create,
            charset,
            collate,
            ClusteredIndexDefMode::On,
            &self.context,
        )
    }
}

/// One column sub-action of a multi-action `ALTER TABLE`.
#[derive(Clone, Debug)]
pub enum AlterColumnAction {
    /// `ADD COLUMN`, appended at the end.
    Add {
        /// Whether an existing column of the same name is a no-op.
        if_not_exists: bool,
        /// The column as written.
        column: Box<tidb_ast::ColumnDef>,
        /// The context the plan-time build resolves defaults under.
        context: DdlStatementContext,
    },
    /// `DROP COLUMN`.
    Drop {
        /// Whether a missing column is a no-op.
        if_exists: bool,
        /// The column name as written.
        column: String,
    },
}

/// A `StmtContext` wrapper carrying the Debug the statement enum derives.
#[derive(Clone)]
pub struct DdlStatementContext(pub tidb_executor::StmtContext);

impl std::fmt::Debug for DdlStatementContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DdlStatementContext")
    }
}

/// One catalog change this node knows how to perform.
#[derive(Clone, Debug)]
pub enum DdlStatement {
    /// `CREATE DATABASE [IF NOT EXISTS] name`.
    CreateDatabase {
        /// The database name as written.
        name: String,
        /// Whether an existing database is a no-op rather than an error.
        if_not_exists: bool,
        /// The resolved charset persisted in `DBInfo`.
        charset: String,
        /// The resolved collation persisted in `DBInfo`.
        collate: String,
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
        /// The validated build recipe, finalized against the loaded database.
        build: Box<CreateTableBuild>,
    },
    /// `ALTER TABLE ... [FORCE] AUTO_RANDOM_BASE=n`.
    RebaseAutoRandom {
        /// Database containing the table.
        schema: String,
        /// Table whose TARID allocator is rebased.
        table: String,
        /// Requested next incremental ID, as Go's uint64-to-int64 pattern.
        next: i64,
        /// Whether a lower value replaces rather than preserves the counter.
        force: bool,
    },
    /// `ALTER TABLE ... AUTO_ID_CACHE=n`.
    ModifyAutoIdCache {
        /// Database containing the table.
        schema: String,
        /// Table whose cached allocator range is rebuilt.
        table: String,
        /// New fixed reservation size, or zero for TiDB's default.
        new_cache: i64,
    },
    /// The AUTO_RANDOM portion of `ALTER TABLE ... MODIFY COLUMN`.
    AlterAutoRandomBits {
        /// Database containing the table.
        schema: String,
        /// Table whose handle layout changes.
        table: String,
        /// Existing handle column named by MODIFY.
        column: String,
        /// New random-shard width.
        shard_bits: u64,
        /// New integer range width.
        range_bits: u64,
        /// Signedness written by the new column definition.
        unsigned: bool,
    },
    /// The single-action `ALTER TABLE ... ADD COLUMN` this node serves:
    /// one nullable, defaultless column appended at the end. Existing rows
    /// read the implicit NULL with no rewrite, which is MySQL's answer for
    /// the same shape; everything needing a row rewrite is refused by name
    /// at admission (`build_added_column`).
    AddColumn {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// Whether an existing column of the same name is a no-op.
        if_not_exists: bool,
        /// The column as written; built against the stored table's charset
        /// at plan time, exactly as Go's add-column job builds it.
        column: Box<tidb_ast::ColumnDef>,
        /// The statement context the plan-time build resolves defaults under.
        /// `StmtContext` carries no Debug; the build recipe pattern above
        /// (`CreateTableBuild`) hides it the same way.
        #[allow(missing_docs)]
        context: DdlStatementContext,
    },
    /// The single-action `ALTER TABLE ... DROP COLUMN` this node serves.
    ///
    /// Go `onDropColumn` + `isDroppableColumn`: the last column of the table,
    /// the integer-handle primary key, and any column covered by a primary,
    /// composite, or columnar index are refused with Go's exact messages;
    /// single-column secondary indexes on the column are dropped WITH it.
    /// The stored rows keep their old value bytes — the row decoder skips a
    /// column id the TableInfo no longer names, which is also how TiKV's
    /// data outlives Go's dropped column until rewrite.
    DropColumn {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// Whether a missing column is a no-op rather than an error.
        if_exists: bool,
        /// The column name as written.
        column: String,
    },
    /// A multi-action `ALTER TABLE` whose every action is a column
    /// add/drop this node owns. Go publishes ONE ActionMultiSchemaChange job
    /// whose sub-jobs commit atomically; here every sub-action folds over one
    /// evolving `TableInfo` inside the one catalog transaction, so a later
    /// action sees what the earlier one changed and the table lands whole or
    /// not at all.
    MultiSchemaChange {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The sub-actions in SQL order.
        actions: Vec<AlterColumnAction>,
    },
    /// `TRUNCATE TABLE [schema.]table`.
    ///
    /// Go's job keeps the schema and allocates a FRESH table id
    /// (`onTruncateTable`): the rows live under the old id's prefix, so the
    /// new id has none of them, and the old data is left for GC exactly as
    /// TiKV leaves it. The auto-id allocators are keyed by table id too, so
    /// the counters restart with the table.
    TruncateTable {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
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
    /// `RENAME TABLE [schema.]from TO [schema.]to`, including the single
    /// action form of `ALTER TABLE ... RENAME TO`.
    RenameTable {
        /// The schema which currently owns the table.
        from_schema: String,
        /// The current table name.
        from_table: String,
        /// The schema which will own the table.
        to_schema: String,
        /// The new table name.
        to_table: String,
    },
    /// `RENAME TABLE from1 TO to1, from2 TO to2, ...`.
    ///
    /// Go validates and publishes every pair in one catalog job. Keeping the
    /// pairs together is what prevents a later conflict from leaving earlier
    /// renames visible.
    RenameTables {
        /// Pairs in SQL order, including the transient namespace changes a
        /// later pair observes.
        pairs: Vec<RenameTablePair>,
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

/// One source/destination pair in [`DdlStatement::RenameTables`].
#[derive(Clone, Debug)]
pub struct RenameTablePair {
    /// The schema which currently owns the table.
    pub from_schema: String,
    /// The current table name.
    pub from_table: String,
    /// The schema which will own the table.
    pub to_schema: String,
    /// The new table name.
    pub to_table: String,
}

/// Admits one parsed statement as a catalog change, or explains why not.
///
/// `None` means the statement is not a DDL this module owns at all, so the
/// caller runs it down its ordinary path. `Err` means it *is* one of the
/// shapes but carries something this node refuses — that refusal is final and
/// happens before any mutation.
pub fn lower_ddl(
    statement: &Stmt,
    default_schema: &str,
) -> Result<Option<DdlStatement>, DdlAdmissionError> {
    let context = default_ddl_statement_context();
    lower_ddl_with_context(statement, default_schema, &context)
}

/// Go `executor.CreateSchema`: the last explicit charset/collation wins, and
/// either explicit half replaces the corresponding server default before the
/// pair is resolved and validated.
fn database_charset_collation(
    options: &[DatabaseOption],
) -> Result<(String, String), DdlAdmissionError> {
    let mut charset = None;
    let mut collate = None;
    for option in options {
        match option {
            DatabaseOption::CharacterSet(value) => charset = Some(value.as_str()),
            DatabaseOption::Collate(value) => collate = Some(value.as_str()),
            other => {
                return Err(DdlAdmissionError::new(format!(
                    "CREATE DATABASE option {other:?} is not supported by this node"
                )))
            }
        }
    }

    if charset.is_some() || collate.is_some() {
        resolve_charset_collation(charset, collate, CATALOG_CHARSET, CATALOG_COLLATION)
    } else {
        Ok((CATALOG_CHARSET.to_owned(), CATALOG_COLLATION.to_owned()))
    }
}

/// [`lower_ddl`] under the statement's actual SQL mode and time zone.
///
/// Live session paths must use this entrypoint so admission and metadata
/// persistence observe the same statement context that parsed the SQL.
pub fn lower_ddl_with_context(
    statement: &Stmt,
    default_schema: &str,
    context: &tidb_executor::StmtContext,
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
            let (charset, collate) = database_charset_collation(options)?;
            Ok(Some(DdlStatement::CreateDatabase {
                name: name.clone(),
                if_not_exists: *if_not_exists,
                charset,
                collate,
            }))
        }
        DdlStmt::DropDatabase { if_exists, name } => Ok(Some(DdlStatement::DropDatabase {
            name: name.clone(),
            if_exists: *if_exists,
        })),
        DdlStmt::CreateTable(create) => {
            lower_create_table(create, default_schema, context).map(Some)
        }
        DdlStmt::DropTable(drop) => lower_drop_table(drop, default_schema).map(Some),
        DdlStmt::CreateIndex(create) => lower_create_index(create, default_schema).map(Some),
        DdlStmt::DropIndex(drop) => lower_drop_index(drop, default_schema).map(Some),
        DdlStmt::AlterTable(alter) => lower_alter_table_catalog(alter, default_schema),
        DdlStmt::RenameTable(rename) => lower_rename_table_stmt(rename, default_schema),
        DdlStmt::TruncateTable(name) => {
            let (schema, table) = split_name(name, default_schema, "table")?;
            Ok(Some(DdlStatement::TruncateTable { schema, table }))
        }
        _ => Ok(None),
    }
}

/// Admits the single-action `ALTER TABLE` spelling of an index change.
///
/// Go lowers these actions to the same add/drop-index jobs as their standalone
/// statements.  Reusing the existing lowered statement keeps catalog changes
/// and backfill ownership in one place.  Multi-action ALTERs stay refused: the
/// catalog transaction has no representation for their atomic job bundle, so
/// accepting only its index action would silently half-apply the SQL.
fn lower_alter_table_catalog(
    alter: &AlterTableStmt,
    default_schema: &str,
) -> Result<Option<DdlStatement>, DdlAdmissionError> {
    let [action] = alter.actions.as_slice() else {
        // Go's one ActionMultiSchemaChange job: expressible here exactly when
        // every action is a column add/drop the catalog transaction owns.
        let mut actions = Vec::with_capacity(alter.actions.len());
        for action in &alter.actions {
            match action {
                tidb_ast::AlterTableAction::AddColumn {
                    if_not_exists,
                    column,
                    position,
                } if matches!(position, tidb_ast::ColumnPosition::Default) => {
                    actions.push(AlterColumnAction::Add {
                        if_not_exists: *if_not_exists,
                        column: Box::new(column.clone()),
                        context: DdlStatementContext(tidb_executor::StmtContext::for_query()),
                    });
                }
                tidb_ast::AlterTableAction::DropColumn { if_exists, name } => {
                    actions.push(AlterColumnAction::Drop {
                        if_exists: *if_exists,
                        column: name.clone(),
                    });
                }
                _ => {
                    actions.clear();
                    break;
                }
            }
        }
        if !actions.is_empty() {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            return Ok(Some(DdlStatement::MultiSchemaChange {
                schema,
                table,
                actions,
            }));
        }
        if alter.actions.iter().any(|action| {
            matches!(
                action,
                tidb_ast::AlterTableAction::AddIndexConstraint(_)
                    | tidb_ast::AlterTableAction::DropIndex { .. }
                    | tidb_ast::AlterTableAction::RenameTable { .. }
            )
        }) {
            return Err(DdlAdmissionError::unsupported(
                "ALTER TABLE index changes require exactly one action on this node; \
                 multiple actions need one atomic multi-schema DDL job",
            ));
        }
        return Ok(None);
    };

    match action {
        tidb_ast::AlterTableAction::AddIndexConstraint(index) => {
            lower_alter_add_index(alter, index, default_schema).map(Some)
        }
        tidb_ast::AlterTableAction::DropIndex { if_exists, name } => lower_drop_index(
            &DropIndexStmt {
                is_hypo: false,
                if_exists: *if_exists,
                name: name.clone(),
                table: alter.name.clone(),
                algorithm: None,
                lock: None,
            },
            default_schema,
        )
        .map(Some),
        tidb_ast::AlterTableAction::RenameTable { new_name } => {
            let (from_schema, from_table) =
                split_name(&alter.name, default_schema, "renamed table")?;
            let (to_schema, to_table) = split_name(new_name, default_schema, "new table name")?;
            if from_schema.eq_ignore_ascii_case(&to_schema)
                && from_table.eq_ignore_ascii_case(&to_table)
            {
                return Ok(None);
            }
            Ok(Some(DdlStatement::RenameTable {
                from_schema,
                from_table,
                to_schema,
                to_table,
            }))
        }
        tidb_ast::AlterTableAction::SetTableOptions { options } => {
            let [option] = options.as_slice() else {
                return Ok(None);
            };
            if let tidb_ast::TableOption::AutoIdCache(value) = option {
                let new_cache = value
                    .parse::<u64>()
                    .map_err(|_| DdlAdmissionError::new("AUTO_ID_CACHE needs an integer value"))?;
                if new_cache > i64::MAX as u64 {
                    return Err(DdlAdmissionError::new(
                        "table option auto_id_cache overflows int64",
                    ));
                }
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return Ok(Some(DdlStatement::ModifyAutoIdCache {
                    schema,
                    table,
                    new_cache: new_cache as i64,
                }));
            }
            let (value, force) = match option {
                tidb_ast::TableOption::AutoRandomBase(value) => (value, false),
                tidb_ast::TableOption::ForceAutoRandomBase(value) => (value, true),
                _ => return Ok(None),
            };
            let next = value
                .parse::<u64>()
                .map_err(|_| DdlAdmissionError::new("AUTO_RANDOM_BASE needs an integer value"))?
                as i64;
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::RebaseAutoRandom {
                schema,
                table,
                next,
                force,
            }))
        }
        tidb_ast::AlterTableAction::ModifyColumn {
            if_exists,
            column,
            position,
        } => {
            if *if_exists
                || !matches!(position, tidb_ast::ColumnPosition::Default)
                || !column.qualifier.is_empty()
                || column.ty.name != "BIGINT"
            {
                return Ok(None);
            }
            let Some(auto_random) = column.options.iter().find_map(|option| match option {
                tidb_ast::ColumnOption::AutoRandom(option) => Some(option),
                _ => None,
            }) else {
                return Ok(None);
            };
            if column.options.iter().any(|option| {
                !matches!(
                    option,
                    tidb_ast::ColumnOption::AutoRandom(_) | tidb_ast::ColumnOption::NotNull
                )
            }) {
                return Ok(None);
            }
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AlterAutoRandomBits {
                schema,
                table,
                column: column.name.clone(),
                shard_bits: auto_random.shard_bits.unwrap_or(5),
                range_bits: auto_random.range_bits.unwrap_or(64),
                unsigned: column.ty.unsigned,
            }))
        }
        tidb_ast::AlterTableAction::DropColumn { if_exists, name } => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::DropColumn {
                schema,
                table,
                if_exists: *if_exists,
                column: name.clone(),
            }))
        }
        tidb_ast::AlterTableAction::AddColumn {
            if_not_exists,
            column,
            position,
        } => {
            // Go's job appends at the end unless FIRST/AFTER reorders; the
            // reorder rewrites every row's offset map, so it is refused by
            // name rather than silently appended.
            if !matches!(position, tidb_ast::ColumnPosition::Default) {
                return Err(DdlAdmissionError::unsupported(
                    "ADD COLUMN FIRST/AFTER reorders stored offsets; this node appends only",
                ));
            }
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AddColumn {
                schema,
                table,
                if_not_exists: *if_not_exists,
                column: Box::new(column.clone()),
                context: DdlStatementContext(tidb_executor::StmtContext::for_query()),
            }))
        }
        _ => Ok(None),
    }
}

/// Lowers a top-level `RENAME TABLE` statement.
///
/// Go validates every pair before it publishes the one multi-table job. The
/// planner keeps that complete sequence and resolves its transient namespace
/// against one snapshot before it emits any mutation.
fn lower_rename_table_stmt(
    rename: &RenameTableStmt,
    default_schema: &str,
) -> Result<Option<DdlStatement>, DdlAdmissionError> {
    let pairs = rename
        .pairs
        .iter()
        .map(|(from, to)| lower_rename_table_pair(from, to, default_schema))
        .collect::<Result<Vec<_>, _>>()?;
    match pairs.as_slice() {
        [] => Err(DdlAdmissionError::new("RENAME TABLE names no table")),
        [pair] => Ok(Some(DdlStatement::RenameTable {
            from_schema: pair.from_schema.clone(),
            from_table: pair.from_table.clone(),
            to_schema: pair.to_schema.clone(),
            to_table: pair.to_table.clone(),
        })),
        _ => Ok(Some(DdlStatement::RenameTables { pairs })),
    }
}

fn lower_rename_table_pair(
    from: &[String],
    to: &[String],
    default_schema: &str,
) -> Result<RenameTablePair, DdlAdmissionError> {
    let (from_schema, from_table) = split_name(from, default_schema, "renamed table")?;
    let (to_schema, to_table) = split_name(to, default_schema, "new table name")?;
    Ok(RenameTablePair {
        from_schema,
        from_table,
        to_schema,
        to_table,
    })
}

fn lower_alter_add_index(
    alter: &AlterTableStmt,
    index: &IndexConstraintDefinition,
    default_schema: &str,
) -> Result<DdlStatement, DdlAdmissionError> {
    let kind = match index.kind {
        IndexConstraintKind::Key | IndexConstraintKind::Index => tidb_ast::IndexKind::Ordinary,
        IndexConstraintKind::Unique
        | IndexConstraintKind::UniqueKey
        | IndexConstraintKind::UniqueIndex => tidb_ast::IndexKind::Unique,
        IndexConstraintKind::PrimaryKey => {
            return Err(DdlAdmissionError::unsupported(
                "ALTER TABLE ADD PRIMARY KEY is not supported by this node",
            ))
        }
        IndexConstraintKind::Fulltext => {
            return Err(DdlAdmissionError::unsupported(
                "ALTER TABLE ADD FULLTEXT is not supported by this node",
            ))
        }
        IndexConstraintKind::Vector => {
            return Err(DdlAdmissionError::unsupported(
                "ALTER TABLE ADD VECTOR INDEX is not supported by this node",
            ))
        }
        IndexConstraintKind::Columnar => {
            return Err(DdlAdmissionError::unsupported(
                "ALTER TABLE ADD COLUMNAR INDEX is not supported by this node",
            ))
        }
    };
    let Some(name) = index.name.as_ref().filter(|name| !name.is_empty()) else {
        // Go resolves anonymous names only after it has loaded the current
        // table and can avoid an existing name. This catalog lowerer is
        // intentionally stateless, so admitting it here would create a
        // different collision contract from standalone CREATE INDEX.
        return Err(DdlAdmissionError::unsupported(
            "ALTER TABLE ADD INDEX needs an explicit index name on this node",
        ));
    };
    lower_create_index(
        &CreateIndexStmt {
            kind,
            if_not_exists: index.if_not_exists,
            name: name.clone(),
            table: alter.name.clone(),
            parts: index.parts.clone(),
            options: index.options.clone(),
            online: Default::default(),
        },
        default_schema,
    )
}

/// Splits a written name path into `(schema, object)`, defaulting the schema.

/// One sub-action's answer inside an evolving ALTER.
enum AlterColumnOutcome {
    Applied,
    /// The `IF [NOT] EXISTS` no-op, with the sentence `already` reports.
    AlreadySatisfied(String),
}

/// Go's add-column job body over an EVOLVING `TableInfo` — the same rules a
/// single-action ADD COLUMN commits, applied to whatever the previous
/// sub-action of a multi-schema change left behind.
fn apply_add_column(
    info: &mut TableInfo,
    schema: &str,
    table: &str,
    column: &tidb_ast::ColumnDef,
    if_not_exists: bool,
    context: &tidb_executor::StmtContext,
) -> Result<AlterColumnOutcome, DdlPlanError> {
    let wanted = column.name.to_lowercase();
    if info
        .columns
        .iter_deref()
        .any(|candidate| candidate.read().name.lowercase() == wanted)
    {
        if if_not_exists {
            return Ok(AlterColumnOutcome::AlreadySatisfied(format!(
                "column `{}` already exists on `{schema}`.`{table}`",
                column.name
            )));
        }
        return Err(DdlPlanError::DuplicateColumnName(column.name.clone()));
    }
    let mut added =
        crate::table_info_build::build_added_column(column, &info.charset, &info.collate, context)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    // Go `AllocateColumnID`: ids only ever grow, so a dropped column's id is
    // never reused.
    info.max_column_id += 1;
    added.id = info.max_column_id;
    added.offset = info.columns.len() as i64;
    added.state = tidb_model::SchemaState::PUBLIC;
    info.columns.push_handle_go(Some(GoShared::new(added)));
    Ok(AlterColumnOutcome::Applied)
}

/// Go `isDroppableColumn` + `onDropColumn` over the same evolving info.
fn apply_drop_column(
    info: &mut TableInfo,
    schema: &str,
    table: &str,
    column: &str,
    if_exists: bool,
) -> Result<AlterColumnOutcome, DdlPlanError> {
    let wanted = column.to_lowercase();
    let Some(dropped_offset) = info
        .columns
        .iter_deref()
        .position(|candidate| candidate.read().name.lowercase() == wanted)
    else {
        if if_exists {
            return Ok(AlterColumnOutcome::AlreadySatisfied(format!(
                "column `{column}` does not exist on `{schema}`.`{table}`"
            )));
        }
        // Go 1091 ErrCantDropFieldOrKey.
        return Err(DdlPlanError::Encode(format!(
            "Can't DROP '{column}'; check that column/key exists"
        )));
    };
    if info.columns.len() == 1 {
        // Go 1090 ErrCantRemoveAllFields.
        return Err(DdlPlanError::Encode(format!(
            "can't drop only column {column} in table {}",
            info.name.original()
        )));
    }
    let dropped = info
        .columns
        .iter_deref()
        .nth(dropped_offset)
        .expect("the position was just found")
        .read()
        .clone_like_go();
    if dropped.is_pk_handle_column(info) {
        // Go 8200 via checkModifyPKColumn's drop arm.
        return Err(DdlPlanError::Encode(
            "Unsupported drop integer primary key".to_owned(),
        ));
    }
    for index in info.indices.iter_deref() {
        let index = index.read();
        let covers = index
            .columns
            .iter_deref()
            .any(|col| col.read().name.lowercase() == wanted);
        if covers && (index.primary || index.columns.len() > 1) {
            // Go 8200 ErrCantDropColWithIndex, message verbatim.
            return Err(DdlPlanError::Encode(format!(
                "can't drop column {column} with composite index covered or \
                 Primary Key covered now"
            )));
        }
    }
    // Go `listIndicesWithColumn`: a single-column secondary index on the
    // dropped column goes with it.
    let surviving: Vec<_> = (0..info.indices.len())
        .filter_map(|position| {
            let handle = info.indices.get(position)?;
            let single_on_dropped = {
                let index = handle.read();
                index.columns.len() == 1
                    && index
                        .columns
                        .iter_deref()
                        .next()
                        .is_some_and(|col| col.read().name.lowercase() == wanted)
            };
            (!single_on_dropped).then_some(Some(handle))
        })
        .collect();
    info.indices = tidb_model::GoSharedPointerSlice::from_handles(surviving);
    info.columns.delete_go(dropped_offset, dropped_offset + 1);
    // Every later column shifts down one offset, and every index column
    // referring to one follows it.
    for column in info.columns.iter_deref() {
        let mut column = column.write();
        if column.offset > dropped.offset {
            column.offset -= 1;
        }
    }
    for index in info.indices.iter_deref() {
        let index = index.read();
        for col in index.columns.iter_deref() {
            let mut col = col.write();
            if col.offset > dropped.offset {
                col.offset -= 1;
            }
        }
    }
    Ok(AlterColumnOutcome::Applied)
}

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
    context: &tidb_executor::StmtContext,
) -> Result<DdlStatement, DdlAdmissionError> {
    let (schema, table) = split_name(&create.name, default_schema, "table")?;
    // The server default `tidb_enable_clustered_index = ON`, which is what a
    // real TiDB builds a user table under. Bootstrap is the one caller that
    // uses a different mode, and it says so at its own call site.
    let build = CreateTableBuild::new(create, context)?;
    Ok(DdlStatement::CreateTable {
        schema,
        table,
        if_not_exists: create.if_not_exists,
        build: Box::new(build),
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
            columns: columns.into(),
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
    /// TiDB `ErrInvalidAutoRandom` (8216).
    InvalidAutoRandom(String),
    /// TiDB `ErrAutoincReadFailed` (1467), used by FORCE base zero.
    AutoIdReadFailed,
    /// A source-defined DDL refusal reported as TiDB's generic 1105.
    Unsupported(String),
    /// The named table is already in the named database.
    TableExists {
        /// The database name as written.
        schema: String,
        /// The table name as written.
        table: String,
    },
    /// The table already has an index of that name (Go 1061).
    DuplicateKeyName(String),
    /// The table already has a column of that name (Go 1060).
    DuplicateColumnName(String),
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
            Self::InvalidAutoRandom(reason) => write!(formatter, "Invalid auto random: {reason}"),
            Self::AutoIdReadFailed => {
                formatter.write_str("Failed to read auto-increment value from storage engine")
            }
            Self::Unsupported(reason) => formatter.write_str(reason),
            Self::TableExists { schema, table } => {
                write!(formatter, "Table '{schema}.{table}' already exists")
            }
            Self::DuplicateKeyName(name) => write!(formatter, "Duplicate key name '{name}'"),
            Self::DuplicateColumnName(name) => {
                write!(formatter, "Duplicate column name '{name}'")
            }
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
    ///
    /// On `CREATE INDEX` this is the SAME Go pointer appended to the new
    /// `TableInfo.Indices`, just as Go's `checkAndBuildIndexInfo` returns the
    /// pointer it appended. Resolving the ID or columns through either owner
    /// must name one index, not two snapshots that merely started equal.
    pub index: GoShared<IndexInfo>,
    /// The persisted collation mode captured when this DDL was planned.
    ///
    /// Go stores this in `DDLReorgMeta.UseNewCollate`; the backfill must not
    /// re-read a process global after the metadata half has already chosen
    /// the key/value format.
    pub use_new_collation: bool,
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
    plan_ddl_with_collation(snapshot, statement, start_ts, new_collation_enabled())
}

/// [`plan_ddl`] with an already captured persisted collation mode.
///
/// This is the source-shaped equivalent of Go carrying
/// `DDLReorgMeta.UseNewCollate`: callers that already own the cluster setting
/// pass it once, and both halves of an index change keep that same value.
pub fn plan_ddl_with_collation<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    start_ts: u64,
    use_new_collation: bool,
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
            charset,
            collate,
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
                charset: charset.clone(),
                collate: collate.clone(),
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
            build,
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
            // Go's create-table path publishes the TableInfo the builder
            // produced; it does not call `TableInfo.Clone` on the way to
            // `Mutator.CreateTableOrView`. A plain struct/header copy is all
            // this immutable plan needs before stamping its two scalar
            // transaction-owned fields. `clone_like_go()` would allocate an
            // empty `Indices` slice even when the builder left it nil, turning
            // Go's persisted `"index_info":null` into `[]`.
            let mut info = build
                .for_database(&database.info.charset, &database.info.collate)
                .map_err(|error| DdlPlanError::Unsupported(error.to_string()))?;
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
            if info.auto_random_bits > 0 && info.auto_rand_id > 1 {
                writes.push(OptimisticMutation::meta_put(
                    crate::cluster_auto_id::auto_random_id_key_for(db_id, &info),
                    value::encode_int_value(info.auto_rand_id - 1),
                )?);
            }
            diff.action_type = ActionType::ACTION_CREATE_TABLE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::RebaseAutoRandom {
            schema,
            table,
            next,
            force,
        } => {
            let Some(database) = find_database(&catalog, schema) else {
                return Err(DdlPlanError::UnknownDatabase(schema.clone()));
            };
            let Some(stored) = find_table(database, table) else {
                return Err(DdlPlanError::UnknownTable {
                    schema: schema.clone(),
                    table: table.clone(),
                });
            };
            if stored.auto_random_bits == 0 {
                return Err(DdlPlanError::InvalidAutoRandom(
                    "alter auto_random_base of a non auto_random table".to_owned(),
                ));
            }
            let unsigned = stored.is_auto_random_bit_col_unsigned();
            let range_bits = if stored.auto_random_range_bits == 0 {
                64
            } else {
                stored.auto_random_range_bits
            };
            let incremental_bits = range_bits - stored.auto_random_bits - u64::from(!unsigned);
            let maximum = (1_u64 << incremental_bits) - 1;
            let requested = *next as u64;
            if *next < 0 || requested & maximum != requested {
                return Err(DdlPlanError::InvalidAutoRandom(format!(
                    "alter auto_random_base to {next} overflows the incremental bits, max allowed base is {maximum}"
                )));
            }
            if *force && requested == 0 {
                return Err(DdlPlanError::AutoIdReadFailed);
            }
            let db_id = database.info.id;
            let table_id = stored.id;
            let counter_key = key::auto_random_table_id_kv_key(db_id, table_id);
            let current = snapshot
                .get(&counter_key)?
                .map(|bytes| value::parse_int_value(&bytes))
                .transpose()
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?
                .unwrap_or(0)
                .saturating_add(1) as u64;
            let effective = if *force {
                requested
            } else {
                requested.max(current)
            };
            let mut info = stored.clone();
            info.auto_rand_id = effective as i64;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            writes.push(OptimisticMutation::meta_put(
                counter_key,
                value::encode_int_value(effective as i64 - 1),
            )?);
            diff.action_type = ActionType::ACTION_REBASE_AUTO_RANDOM_BASE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::ModifyAutoIdCache {
            schema,
            table,
            new_cache,
        } => {
            let Some(database) = find_database(&catalog, schema) else {
                return Err(DdlPlanError::UnknownDatabase(schema.clone()));
            };
            let Some(stored) = find_table(database, table) else {
                return Err(DdlPlanError::UnknownTable {
                    schema: schema.clone(),
                    table: table.clone(),
                });
            };
            if (*new_cache == 1) != (stored.auto_id_cache == 1) {
                return Err(DdlPlanError::Unsupported(
                    "Can't Alter AUTO_ID_CACHE between 1 and non-1, the underlying implementation is different"
                        .to_owned(),
                ));
            }
            let mut info = stored.clone_like_go();
            info.auto_id_cache = *new_cache;
            let db_id = database.info.id;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, stored.id),
                value::serialize_table_info(&info)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?);
            diff.action_type = ActionType::ACTION_MODIFY_TABLE_AUTO_IDCACHE;
            diff.schema_id = db_id;
            diff.table_id = stored.id;
        }
        DdlStatement::AlterAutoRandomBits {
            schema,
            table,
            column,
            shard_bits,
            range_bits,
            unsigned,
        } => {
            let Some(database) = find_database(&catalog, schema) else {
                return Err(DdlPlanError::UnknownDatabase(schema.clone()));
            };
            let Some(stored) = find_table(database, table) else {
                return Err(DdlPlanError::UnknownTable {
                    schema: schema.clone(),
                    table: table.clone(),
                });
            };
            let Some(target) = tidb_model::column::find_column_info(&stored.columns, column) else {
                return Err(DdlPlanError::InvalidAutoRandom(format!(
                    "unknown column `{column}`"
                )));
            };
            let target_read = target.read();
            let target_flags = target_read.get_flag();
            let target_type = target_read.get_type();
            let target_unsigned = target_read.field_type.is_unsigned();
            let target_name = target_read.name.lowercase().to_owned();
            drop(target_read);
            if *shard_bits == 0 {
                return Err(DdlPlanError::InvalidAutoRandom(
                    "the value of auto_random should be positive".to_owned(),
                ));
            }
            if *shard_bits > 15 {
                return Err(DdlPlanError::InvalidAutoRandom(format!(
                    "max allowed auto_random shard bits is 15, but got {shard_bits} on column `{column}`"
                )));
            }
            if !(32..=64).contains(range_bits) {
                return Err(DdlPlanError::InvalidAutoRandom(format!(
                    "auto_random range bits must be between 32 and 64, but got {range_bits}"
                )));
            }
            let target_is_clustered_key = if stored.pk_is_handle {
                target_flags & u64::from(FieldTypeFlags::PRI_KEY) != 0
            } else if stored.is_common_handle {
                stored.indices.iter_deref().any(|index| {
                    let index = index.read();
                    index.primary
                        && index
                            .columns
                            .iter_deref()
                            .any(|column| column.read().name.lowercase() == target_name.as_str())
                })
            } else {
                false
            };
            let previous_bits = if target_is_clustered_key {
                stored.auto_random_bits
            } else {
                0
            };
            let converting = previous_bits == 0;
            if converting {
                let auto_increment = target_flags & u64::from(FieldTypeFlags::AUTO_INCREMENT) != 0;
                let clustered_pk = stored.pk_is_handle && target_is_clustered_key;
                if !auto_increment || !clustered_pk {
                    return Err(DdlPlanError::InvalidAutoRandom(
                        "auto_random can only be converted from auto_increment clustered primary key"
                            .to_owned(),
                    ));
                }
            } else {
                if *shard_bits < previous_bits {
                    return Err(DdlPlanError::InvalidAutoRandom(
                        "decreasing auto_random shard bits is not supported".to_owned(),
                    ));
                }
            }
            if target_type != FieldTypeCode::LongLong || target_unsigned != *unsigned {
                return Err(DdlPlanError::InvalidAutoRandom(
                    "modifying the auto_random column type is not supported".to_owned(),
                ));
            }
            let previous_range = if stored.auto_random_range_bits == 0 {
                64
            } else {
                stored.auto_random_range_bits
            };
            if *range_bits != previous_range {
                return Err(DdlPlanError::InvalidAutoRandom(
                    "alter the range bits of auto_random column is not supported".to_owned(),
                ));
            }

            let db_id = database.info.id;
            let table_id = stored.id;
            let check_key = if converting {
                crate::cluster_auto_id::auto_id_key_for(db_id, stored)
            } else {
                key::auto_random_table_id_kv_key(db_id, table_id)
            };
            let previous_counter = snapshot
                .get(&check_key)?
                .map(|bytes| value::parse_int_value(&bytes))
                .transpose()
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?
                .unwrap_or(0) as u64;
            let checked_current = tidb_executor::kv_table::advance(previous_counter, 1, *unsigned);
            if checked_current == previous_counter {
                return Err(DdlPlanError::AutoIdReadFailed);
            }
            let incremental_bits = range_bits - shard_bits - u64::from(!unsigned);
            let used_bits = u64::from(u64::BITS - checked_current.leading_zeros());
            if used_bits > incremental_bits {
                let maximum = shard_bits.wrapping_sub(used_bits - incremental_bits);
                return Err(DdlPlanError::InvalidAutoRandom(format!(
                    "max allowed auto_random shard bits is {maximum}, but got {shard_bits} on column `{column}`"
                )));
            }

            let mut info = stored.clone_like_go();
            info.auto_random_bits = *shard_bits;
            if converting {
                let converted = tidb_model::column::find_column_info(&info.columns, column)
                    .expect("the cloned table retains the target column");
                converted
                    .write()
                    .del_flag(u64::from(FieldTypeFlags::AUTO_INCREMENT));
            }
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                value::serialize_table_info(&info)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?);
            let random_key = key::auto_random_table_id_kv_key(db_id, table_id);
            let current = if converting && stored.sep_auto_inc() {
                writes.push(OptimisticMutation::meta_put(
                    check_key,
                    value::encode_int_value(checked_current as i64),
                )?);
                snapshot
                    .get(&key::auto_table_id_kv_key(db_id, table_id))?
                    .map(|bytes| value::parse_int_value(&bytes))
                    .transpose()
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?
                    .unwrap_or(0) as u64
            } else {
                checked_current
            };
            let previous_random = snapshot
                .get(&random_key)?
                .map(|bytes| value::parse_int_value(&bytes))
                .transpose()
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?
                .unwrap_or(0) as u64;
            let rebased_random =
                if tidb_executor::kv_table::exceeds(current, previous_random, *unsigned) {
                    current
                } else {
                    previous_random
                };
            if !converting || rebased_random != previous_random {
                writes.push(OptimisticMutation::meta_put(
                    random_key,
                    value::encode_int_value(rebased_random as i64),
                )?);
            }
            if converting {
                let row_id_key = key::auto_table_id_kv_key(db_id, table_id);
                if snapshot.get(&row_id_key)?.is_some() {
                    writes.push(OptimisticMutation::meta_delete(row_id_key)?);
                }
            }
            diff.action_type = ActionType::ACTION_MODIFY_COLUMN;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::AddColumn {
            schema,
            table,
            if_not_exists,
            column,
            context,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let mut info = stored.clone_like_go();
            match apply_add_column(&mut info, schema, table, column, *if_not_exists, &context.0)? {
                AlterColumnOutcome::AlreadySatisfied(detail) => return Ok(already(detail)),
                AlterColumnOutcome::Applied => {}
            }
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_ADD_COLUMN;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::DropColumn {
            schema,
            table,
            if_exists,
            column,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let mut info = stored.clone_like_go();
            match apply_drop_column(&mut info, schema, table, column, *if_exists)? {
                AlterColumnOutcome::AlreadySatisfied(detail) => return Ok(already(detail)),
                AlterColumnOutcome::Applied => {}
            }
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_DROP_COLUMN;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::MultiSchemaChange {
            schema,
            table,
            actions,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let mut info = stored.clone_like_go();
            let mut applied = 0usize;
            let mut satisfied = Vec::new();
            for action in actions {
                let outcome = match action {
                    AlterColumnAction::Add {
                        if_not_exists,
                        column,
                        context,
                    } => apply_add_column(
                        &mut info,
                        schema,
                        table,
                        column,
                        *if_not_exists,
                        &context.0,
                    )?,
                    AlterColumnAction::Drop { if_exists, column } => {
                        apply_drop_column(&mut info, schema, table, column, *if_exists)?
                    }
                };
                match outcome {
                    AlterColumnOutcome::Applied => applied += 1,
                    AlterColumnOutcome::AlreadySatisfied(detail) => satisfied.push(detail),
                }
            }
            if applied == 0 {
                return Ok(already(satisfied.join("; ")));
            }
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_MULTI_SCHEMA_CHANGE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::TruncateTable { schema, table } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let old_table_id = stored.id;
            let [new_table_id] = allocate(snapshot, &mut writes, 1)?[..] else {
                return Err(DdlPlanError::Encode("truncate allocated no id".to_owned()));
            };
            let mut info = stored.clone_like_go();
            info.id = new_table_id;
            info.update_ts = start_ts;
            writes.push(OptimisticMutation::meta_delete(key::table_kv_key(
                db_id,
                old_table_id,
            ))?);
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, new_table_id),
                encoded,
            )?);
            // The allocators travel with the table id; deleting the old
            // ones is what restarts the counters, Go's `Del()` on truncate.
            for allocator in [
                key::auto_table_id_kv_key(db_id, old_table_id),
                key::auto_increment_id_kv_key(db_id, old_table_id),
                key::auto_random_table_id_kv_key(db_id, old_table_id),
            ] {
                if snapshot.get(&allocator)?.is_some() {
                    writes.push(OptimisticMutation::meta_delete(allocator)?);
                }
            }
            diff.action_type = ActionType::ACTION_TRUNCATE_TABLE;
            diff.schema_id = db_id;
            diff.table_id = new_table_id;
            diff.old_table_id = old_table_id;
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
        DdlStatement::RenameTable {
            from_schema,
            from_table,
            to_schema,
            to_table,
        } => {
            let pair = RenameTablePair {
                from_schema: from_schema.clone(),
                from_table: from_table.clone(),
                to_schema: to_schema.clone(),
                to_table: to_table.clone(),
            };
            plan_rename_tables(
                &catalog,
                std::slice::from_ref(&pair),
                start_ts,
                &mut writes,
                &mut diff,
            )?;
        }
        DdlStatement::RenameTables { pairs } => {
            plan_rename_tables(&catalog, pairs, start_ts, &mut writes, &mut diff)?;
        }
        DdlStatement::CreateIndex {
            schema,
            table,
            if_not_exists,
            index,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            if let Some(existing) = find_index(stored, index.name.original()) {
                let existing = existing.read();
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
            let mut added = index.clone_like_go();
            // Go's `IndexColumn.Offset` is a position in `TableInfo.Columns`,
            // and the loader reads it back that way, so it is resolved against
            // the stored table rather than trusted from the statement.
            for column in added.columns.iter_deref() {
                let mut column = column.write();
                let Some(stored_column) = stored
                    .columns
                    .iter_deref()
                    .find(|candidate| candidate.read().name.lowercase() == column.name.lowercase())
                else {
                    return Err(DdlPlanError::UnknownIndexColumn {
                        column: column.name.original().to_owned(),
                        index: index.name.original().to_owned(),
                    });
                };
                let stored_column = stored_column.read();
                column.name = stored_column.name.clone();
                column.offset = stored_column.offset;
            }
            let mut info = stored.clone_like_go();
            info.max_index_id += 1;
            added.id = info.max_index_id;
            added.table = info.name.clone();
            let added = GoShared::new(added);
            info.indices.push_handle_go(Some(added.clone()));
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            backfill = Some(IndexBackfill {
                table: Box::new(stored.clone_like_go()),
                index: added,
                use_new_collation,
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
            let Some(dropped) = find_index(stored, index) else {
                if *if_exists {
                    return Ok(already(format!(
                        "index `{index}` does not exist on `{schema}`.`{table}`"
                    )));
                }
                return Err(DdlPlanError::UnknownIndex(index.clone()));
            };
            let dropped = dropped.read().clone_like_go();
            let mut info = stored.clone_like_go();
            if let Some(offset) = info.indices.iter_handles().position(|candidate| {
                candidate
                    .as_ref()
                    .expect("nil *IndexInfo in TableInfo.Indices")
                    .read()
                    .id
                    == dropped.id
            }) {
                // Go `removeIndexInfo` stops at the first matching ID, then
                // `slices.Delete`s exactly that slot in the same backing.
                info.indices.delete_go(offset, offset + 1);
            }
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
                table: Box::new(stored.clone_like_go()),
                index: GoShared::new(dropped),
                use_new_collation,
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

#[derive(Clone)]
struct RenameState {
    original_schema_id: i64,
    current_schema_id: i64,
    table: TableInfo,
}

#[derive(Clone, Copy)]
struct RenamePairResult {
    table_id: i64,
    old_schema_id: i64,
    new_schema_id: i64,
}

/// Plans all of a top-level `RENAME TABLE` statement against one mutable
/// namespace snapshot, then emits only the final metadata location for each
/// table. This is the catalog transaction equivalent of Go's `ExtractTblInfos`
/// preflight followed by its one `ActionRenameTables` job.
fn plan_rename_tables(
    catalog: &ClusterCatalog,
    pairs: &[RenameTablePair],
    start_ts: u64,
    writes: &mut Vec<OptimisticMutation>,
    diff: &mut SchemaDiff,
) -> Result<(), DdlPlanError> {
    let database_ids = catalog
        .databases
        .iter()
        .map(|database| (database.info.name.lowercase().to_owned(), database.info.id))
        .collect::<BTreeMap<_, _>>();
    let mut namespace = BTreeMap::new();
    for database in &catalog.databases {
        for table in &database.tables {
            namespace.insert(
                table_name_key(database.info.name.lowercase(), table.name.lowercase()),
                RenameState {
                    original_schema_id: database.info.id,
                    current_schema_id: database.info.id,
                    table: table.clone_like_go(),
                },
            );
        }
    }

    let mut changed = BTreeMap::new();
    let mut results = Vec::with_capacity(pairs.len());
    for pair in pairs {
        let from_schema = pair.from_schema.to_lowercase();
        let to_schema = pair.to_schema.to_lowercase();
        if !database_ids.contains_key(&from_schema) {
            return Err(DdlPlanError::UnknownDatabase(pair.from_schema.clone()));
        }
        let from_key = table_name_key(&from_schema, &pair.from_table.to_lowercase());
        let Some(state) = namespace.get(&from_key) else {
            return Err(DdlPlanError::UnknownTable {
                schema: pair.from_schema.clone(),
                table: pair.from_table.clone(),
            });
        };
        let Some(&new_schema_id) = database_ids.get(&to_schema) else {
            return Err(DdlPlanError::UnknownDatabase(pair.to_schema.clone()));
        };
        let to_key = table_name_key(&to_schema, &pair.to_table.to_lowercase());
        if namespace.contains_key(&to_key) {
            return Err(DdlPlanError::TableExists {
                schema: pair.to_schema.clone(),
                table: pair.to_table.clone(),
            });
        }

        let mut state = state.clone();
        namespace.remove(&from_key);
        let old_schema_id = state.current_schema_id;
        if state.table.auto_id_schema_id == 0 && old_schema_id != new_schema_id {
            state.table.auto_id_schema_id = old_schema_id;
        } else if new_schema_id == state.table.auto_id_schema_id {
            state.table.auto_id_schema_id = 0;
        }
        state.current_schema_id = new_schema_id;
        state.table.name = CiString::new(pair.to_table.clone());
        state.table.update_ts = start_ts;
        let table_id = state.table.id;
        results.push(RenamePairResult {
            table_id,
            old_schema_id,
            new_schema_id,
        });
        changed.insert(table_id, state.clone());
        namespace.insert(to_key, state);
    }

    for state in changed.values() {
        let table_id = state.table.id;
        if state.original_schema_id != state.current_schema_id {
            writes.push(OptimisticMutation::meta_delete(key::table_kv_key(
                state.original_schema_id,
                table_id,
            ))?);
        }
        let encoded = value::serialize_table_info(&state.table)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
        writes.push(OptimisticMutation::meta_put(
            key::table_kv_key(state.current_schema_id, table_id),
            encoded,
        )?);
    }

    let first = results
        .first()
        .expect("a lowered RENAME TABLE has at least one pair");
    if results.len() == 1 {
        diff.action_type = ActionType::ACTION_RENAME_TABLE;
        diff.schema_id = first.new_schema_id;
        diff.table_id = first.table_id;
        diff.old_schema_id = first.old_schema_id;
        return Ok(());
    }

    // Go's RenameTables completion has already moved every table before it
    // writes the diff, so `OldSchemaIDForSchemaDiff` is the new schema for
    // each pair. The first pair lives in the diff header; the remaining pairs
    // are the affected options in source order.
    diff.action_type = ActionType::ACTION_RENAME_TABLES;
    diff.schema_id = first.new_schema_id;
    diff.table_id = first.table_id;
    diff.old_schema_id = first.new_schema_id;
    diff.affected_options = results[1..]
        .iter()
        .map(|result| AffectedOption {
            schema_id: result.new_schema_id,
            table_id: result.table_id,
            old_table_id: result.table_id,
            old_schema_id: result.new_schema_id,
        })
        .collect::<Vec<_>>()
        .into();
    Ok(())
}

fn table_name_key(schema: &str, table: &str) -> String {
    format!("{schema}\0{table}")
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
fn find_index(table: &TableInfo, name: &str) -> Option<GoShared<IndexInfo>> {
    table
        .indices
        .iter_deref()
        .find(|index| index.read().name.original().eq_ignore_ascii_case(name))
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
