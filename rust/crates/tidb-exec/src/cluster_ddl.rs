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
//! Go source of truth for every published schema is the DDL worker's meta
//! mutation:
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
//! CHECK actions follow Go's durable path: submission first inserts the full
//! job envelope in `mysql.tidb_ddl_job`, each owner transaction reloads that
//! envelope and publishes one state transition, and terminal handling moves
//! the job to both Go history stores. Other actions still use the ordinary
//! one-write planner below and must not borrow CHECK-specific recovery state.
//!
//! Only shapes this node can also serve are admitted, and every refusal
//! happens in [`lower_ddl`], before a timestamp is spent or a byte is written.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use tidb_ast::CiString;
use tidb_ast::{
    AlterTableStmt, CreateIndexStmt, CreateTableStmt, DatabaseOption, DdlStmt, DropIndexStmt,
    DropTableStmt, IndexConstraintDefinition, IndexConstraintKind, RenameTableStmt, Stmt,
};
use tidb_datatype::new_collation_enabled;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_ddl_notifier::SchemaChangeEvent;
use tidb_meta::{key, value};
use tidb_metadef::system_tables_def::NOTIFIER_TABLE_NAME;
use tidb_metadef::MAX_USER_GLOBAL_ID;
use tidb_model::action_type::ActionType;
use tidb_model::db::DBInfo;
use tidb_model::index::{IndexColumn, IndexInfo};
use tidb_model::partition::{PartitionDefinition, PartitionInfo};
use tidb_model::schema_diff::{AffectedOption, SchemaDiff};
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;
use tidb_model::{
    get_job_ver_in_use, AddCheckConstraintArgs, CheckConstraintArgs, GoField, GoShared,
    HistoryInfo, Job, JobArgsValue, JobState, TraceInfo,
};
use tidb_txnkv::transaction::{MutationSetError, OptimisticMutation};

use crate::cluster_catalog::{
    load_cluster_catalog, ClusterCatalog, ClusterCatalogError, MetaSnapshot,
};
use crate::ddl_job_submit::GlobalIdAllocator;
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

// Go's job scheduler owns one process-local atomic sequence allocator shared
// by all workers. It is intentionally not reconstructed from history after a
// restart.
static DDL_HISTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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
    /// `ADD COLUMN`, at its written position.
    Add {
        /// Whether an existing column of the same name is a no-op.
        if_not_exists: bool,
        /// The column as written.
        column: Box<tidb_ast::ColumnDef>,
        /// Go's `ast.ColumnPosition`; see [`DdlStatement::AddColumn`].
        position: tidb_ast::ColumnPosition,
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
    /// `ADD INDEX`/`ADD KEY`, resolved against the bundle's EVOLVED columns
    /// — an index on a column the same bundle adds is legal, exactly as
    /// Go's one multi-schema job makes it.
    AddIndex {
        /// Whether an existing index of the same name is a no-op.
        if_not_exists: bool,
        /// The index, complete except for id and column offsets.
        index: Box<IndexInfo>,
    },
    /// `DROP INDEX`/`DROP KEY`.
    DropIndex {
        /// Whether a missing index is a no-op.
        if_exists: bool,
        /// The index name as written.
        name: String,
    },
    /// A table-level CHECK split out of grouped `ADD COLUMN (...)` by Go's
    /// `resolveAlterTableAddColumns`.
    AddCheck {
        /// The parsed CHECK declaration.
        definition: Box<tidb_ast::CheckConstraintDefinition>,
        /// The statement context used to resolve and validate it.
        context: DdlStatementContext,
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
    /// The single-action `ALTER TABLE ... ADD COLUMN` this node serves,
    /// including Go's default/origin-default shapes and virtual generated
    /// columns. Stored generated ADD remains Go's own 3106 refusal because it
    /// would require a backfill.
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
        /// Go's `ast.ColumnPosition`: where the new column lands in the
        /// table's column ORDER. The stored rows are keyed by column id
        /// and never rewritten; what moves is the offset every reader and
        /// every index column addresses the column by.
        position: tidb_ast::ColumnPosition,
        /// The statement context the plan-time build resolves defaults under.
        /// `StmtContext` carries no Debug; the build recipe pattern above
        /// (`CreateTableBuild`) hides it the same way.
        #[allow(missing_docs)]
        context: DdlStatementContext,
    },
    /// `ALTER TABLE ... ADD [CONSTRAINT name] CHECK (...)`.
    AddCheckConstraint {
        /// Database containing the table.
        schema: String,
        /// Table receiving the constraint.
        table: String,
        /// The parsed CHECK declaration.
        definition: Box<tidb_ast::CheckConstraintDefinition>,
        /// The statement context used to resolve and type-check the expression.
        context: DdlStatementContext,
    },
    /// `ALTER TABLE ... DROP CONSTRAINT name` for a CHECK constraint.
    DropCheckConstraint {
        /// Database containing the table.
        schema: String,
        /// Table losing the constraint.
        table: String,
        /// Constraint name as written.
        name: String,
        /// Session context whose complete SQL mode Go persists in the job.
        context: DdlStatementContext,
    },
    /// `ALTER TABLE ... ALTER CONSTRAINT name [NOT] ENFORCED`.
    AlterCheckConstraint {
        /// Database containing the table.
        schema: String,
        /// Table owning the constraint.
        table: String,
        /// Constraint name as written.
        name: String,
        /// Desired enforcement state.
        enforced: bool,
        /// Evaluation context used when enabling the constraint.
        context: DdlStatementContext,
    },
    /// An ADD/ALTER CHECK discarded while
    /// `tidb_enable_check_constraint=OFF`.
    IgnoredCheckConstraint {
        /// Database containing the table, which Go still resolves.
        schema: String,
        /// Table named by the statement.
        table: String,
    },
    /// `ALTER TABLE ... ADD PARTITION`, validated and applied by the same
    /// partition implementation used by the ordinary local executor.
    AddPartitions {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// Canonical SQL retained so the shared partition DDL implementation
        /// parses and applies exactly the source action.
        sql: String,
    },
    /// `ALTER TABLE ... DROP PARTITION`, through the ordinary partition DDL
    /// implementation.
    DropPartitions {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// Canonical SQL for the shared partition DDL implementation.
        sql: String,
    },
    /// `ALTER TABLE ... TRUNCATE PARTITION`, through the ordinary partition
    /// DDL implementation with fresh cluster-global physical IDs.
    TruncatePartitions {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// Canonical SQL for the shared partition DDL implementation.
        sql: String,
    },
    /// `ALTER TABLE ... EXCHANGE PARTITION ... WITH TABLE ...`, Go
    /// `ActionExchangeTablePartition`.
    ExchangePartition {
        /// Database containing the partitioned table.
        schema: String,
        /// Partitioned table.
        table: String,
        /// Named partition to exchange.
        partition: String,
        /// Database containing the standalone table.
        standalone_schema: String,
        /// Standalone table whose records and physical ID are exchanged.
        standalone_table: String,
        /// Whether rows must be proven to belong to the named partition.
        with_validation: bool,
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
    /// The meta-only `ALTER TABLE ... MODIFY COLUMN` this node serves:
    /// Go `noReorgDataStrict`'s same-type widening, committed as a
    /// TableInfo rewrite with no row or index touched. Everything that
    /// would reorganize data — a type-family change, narrowing, a sign
    /// toggle, a decimal reshape, charset movement, nullability changes —
    /// is refused by name at plan time.
    ModifyColumn {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The column as re-declared.
        column: Box<tidb_ast::ColumnDef>,
        /// The statement context the plan-time build resolves under.
        /// Go's `ast.ColumnPosition`: a MODIFY/CHANGE may also MOVE the
        /// column, and Go locates the destination against the column's
        /// CURRENT offset (unlike ADD, which appends first).
        position: tidb_ast::ColumnPosition,
        context: DdlStatementContext,
        /// `CHANGE COLUMN`'s old name; `None` is a plain MODIFY. Go gives
        /// both spellings the one ActionModifyColumn job, the rename riding
        /// `renameColumnTo` over the column and every index column naming it.
        rename_from: Option<String>,
    },
    /// `ALTER TABLE ... RENAME COLUMN from TO to` — `renameColumnTo` alone,
    /// no type change, still Go's ActionModifyColumn.
    RenameColumn {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The existing column name.
        from: String,
        /// The replacement name.
        to: String,
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
    /// `CREATE [OR REPLACE] VIEW [schema.]name AS ...`, carrying the
    /// fully built `TableInfo` — columns resolved and view metadata
    /// captured at the ROUTE, against the resolving node's own catalog,
    /// which is Go's shape: `executeCreateView` preprocesses the body in
    /// the executor and hands DDL a finished `TableInfo`.
    CreateView {
        /// The owning schema.
        schema: String,
        /// The view name.
        name: String,
        /// Whether `OR REPLACE` was written.
        or_replace: bool,
        /// The finished metadata; `id`/`update_ts` are stamped at plan time.
        info: Box<TableInfo>,
    },
    /// `DROP VIEW [IF EXISTS] name [, ...]`.
    DropView {
        /// Each `(schema, name)` in written order.
        names: Vec<(String, String)>,
        /// Whether missing views demote to notes rather than the error.
        if_exists: bool,
    },
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
    /// `ALTER TABLE ... COMMENT = '...'`, Go's
    /// `ActionModifyTableComment`. Metadata only.
    ModifyTableComment {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The comment, already length-validated under the statement's
        /// SQL mode (Go `validateCommentLength`).
        comment: String,
    },
    /// `ALTER TABLE ... ALTER INDEX <i> VISIBLE|INVISIBLE`, Go's
    /// `ActionAlterIndexVisibility`. Metadata only: an invisible index is
    /// still maintained by writes, it is only hidden from the optimizer.
    AlterIndexVisibility {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The index name as written.
        index: String,
        /// The requested state.
        invisible: bool,
    },
    /// `ALTER TABLE ... [FORCE] AUTO_INCREMENT = n`, Go's
    /// `ActionRebaseAutoID` over `autoid.AutoIncrementType`.
    ///
    /// Two things move together: the table's recorded `AutoIncID` and the
    /// allocator counter behind it. Moving only the first would leave the
    /// next INSERT allocating from the old counter, which is the silent
    /// wrong answer this change exists to avoid.
    /// `ALTER TABLE ... DROP PRIMARY KEY`, Go's `CheckIsDropPrimaryKey`
    /// followed by the ordinary index drop.
    ///
    /// Kept distinct from `DROP INDEX` because the two answer differently
    /// for the same table: a clustered primary key cannot be dropped at all,
    /// since the rows are stored under it.
    DropPrimaryKey {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
    },
    /// `CREATE TABLE <new> LIKE <source>`, Go's `BuildTableInfoWithLike`.
    ///
    /// The source is resolved from the CATALOG rather than from the
    /// statement, so this cannot be lowered into a column list the way an
    /// ordinary CREATE TABLE is.
    CreateTableLike {
        /// The resolved database of the table being created.
        schema: String,
        /// The new table's name as written.
        table: String,
        /// The resolved database of the source table.
        source_schema: String,
        /// The source table's name as written.
        source_table: String,
        /// The target's requested temporary-table kind.
        temporary: tidb_ast::CreateTableTemporary,
        /// Whether the global temporary target discards rows at commit.
        on_commit_delete: bool,
        /// `IF NOT EXISTS`.
        if_not_exists: bool,
    },
    /// `ALTER TABLE ... CONVERT TO CHARACTER SET x [COLLATE y]` and
    /// `ALTER TABLE ... CHARACTER SET = x`, Go's
    /// `ActionModifyTableCharsetAndCollate`.
    ///
    /// The two forms differ in one flag: `CONVERT TO` also rewrites every
    /// column's own charset, while the bare option moves the table default
    /// alone. Neither rewrites stored bytes, which is why Go only permits
    /// the conversions whose encodings are compatible.
    ModifyTableCharsetAndCollate {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The target charset, empty to keep the table's own.
        charset: String,
        /// The target collation, empty to take the charset's default.
        collate: String,
        /// `CONVERT TO`: also rewrite each column's charset.
        overwrite_columns: bool,
    },
    /// `ALTER DATABASE <db> {CHARACTER SET | COLLATE} ...`, Go's
    /// `ActionModifySchemaCharsetAndCollate`. It changes the DEFAULT the
    /// database hands to tables created after it; existing tables keep the
    /// charset they were created with.
    ModifySchemaCharsetAndCollate {
        /// The database name as written.
        name: String,
        /// The resolved charset.
        charset: String,
        /// The resolved collation.
        collate: String,
    },
    /// `ALTER TABLE ... RENAME INDEX <from> TO <to>`, Go's
    /// `ActionRenameIndex`. Metadata only: the entries already written keep
    /// their key prefix, which is derived from the index ID rather than its
    /// name.
    RenameIndex {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The existing index name, as written.
        from: String,
        /// The replacement name, as written.
        to: String,
    },
    /// `ALTER TABLE ... ALTER [COLUMN] <c> SET DEFAULT <v>` and
    /// `... DROP DEFAULT`, Go's `ActionSetDefaultValue`. Metadata only: rows
    /// already written keep the values they were given, and only later
    /// omitted writes see the new default.
    SetColumnDefault {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The column name as written.
        column: String,
        /// The new default, or `None` for `DROP DEFAULT`.
        default_value: Option<Box<tidb_ast::Expr>>,
        /// The SQL mode and zone the statement was admitted under, which
        /// decide whether a doubtful spelling warns or refuses.
        context: DdlStatementContext,
    },
    /// `CREATE PLACEMENT POLICY`.
    CreatePlacementPolicy {
        /// The policy name as written.
        name: String,
        /// The folded settings clause.
        settings: tidb_model::PlacementSettings,
        /// Whether a duplicate name is demoted to a note.
        if_not_exists: bool,
        /// Whether an existing policy's settings are replaced.
        or_replace: bool,
    },
    /// `ALTER PLACEMENT POLICY`.
    AlterPlacementPolicy {
        /// The policy name as written.
        name: String,
        /// The settings that replace the stored ones.
        settings: tidb_model::PlacementSettings,
        /// Whether a missing policy is demoted to a note.
        if_exists: bool,
    },
    /// `DROP PLACEMENT POLICY`.
    DropPlacementPolicy {
        /// The policy name as written.
        name: String,
        /// Whether a missing policy is demoted to a note.
        if_exists: bool,
    },
    /// A table option Go's `ALTER TABLE` switch accepts and does nothing
    /// with: `ENGINE`, `ENGINE_ATTRIBUTE`, `STORAGE_CLASS`, `ROW_FORMAT`.
    /// Their cases in `executor.go` are empty, so the statement succeeds and
    /// spends no schema version. Refusing them instead would reject the
    /// `ENGINE=InnoDB` that every mysqldump emits.
    IgnoredTableOption {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The option name, for the reported detail.
        option: &'static str,
    },
    /// `ALTER TABLE ... ORDER BY <cols>`, Go's `OrderByColumns`: it verifies
    /// the table exists, warns when the table has a user-defined primary key
    /// column, and changes nothing.
    OrderByColumns {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
    },
    RebaseAutoIncrementId {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The first id the next INSERT should allocate.
        new_base: i64,
        /// `FORCE AUTO_INCREMENT`: set the counter exactly, even backwards.
        /// Without it Go only ever moves the counter forward.
        force: bool,
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
        DdlStmt::AlterDatabase { name, options } => {
            let mut charset = None;
            let mut collate = None;
            for option in options {
                match option {
                    DatabaseOption::CharacterSet(value) => charset = Some(value.as_str()),
                    DatabaseOption::Collate(value) => collate = Some(value.as_str()),
                    other => {
                        return Err(DdlAdmissionError::new(format!(
                            "ALTER DATABASE option {other:?} is not supported by this node"
                        )))
                    }
                }
            }
            // Go only publishes a job when a charset or collation was
            // written (`isAlterCharsetAndCollate`); an option list with
            // neither is not this change at all.
            if charset.is_none() && collate.is_none() {
                return Ok(None);
            }
            let (charset, collate) = crate::table_info_build::resolve_charset_collation(
                charset,
                collate,
                CATALOG_CHARSET,
                CATALOG_COLLATION,
            )?;
            Ok(Some(DdlStatement::ModifySchemaCharsetAndCollate {
                // Go permits the name to be omitted, meaning the session's
                // current database.
                name: name.clone().unwrap_or_else(|| default_schema.to_owned()),
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
        DdlStmt::DropView { if_exists, names } => {
            let mut resolved = Vec::with_capacity(names.len());
            for path in names {
                let (schema, name) = split_name(path, default_schema, "view")?;
                resolved.push((schema, name));
            }
            Ok(Some(DdlStatement::DropView {
                names: resolved,
                if_exists: *if_exists,
            }))
        }
        DdlStmt::CreateIndex(create) => lower_create_index(create, default_schema).map(Some),
        DdlStmt::DropIndex(drop) => lower_drop_index(drop, default_schema).map(Some),
        DdlStmt::CreatePlacementPolicy(create) => {
            // Go checks the pairing before building the settings, so a
            // statement wrong in both ways reports the contradiction
            // (`ddl/executor.go:6808`).
            if create.or_replace && create.if_not_exists {
                return Err(DdlAdmissionError::with_code(
                    1221,
                    "Incorrect usage of OR REPLACE and IF NOT EXISTS".to_owned(),
                ));
            }
            Ok(Some(DdlStatement::CreatePlacementPolicy {
                name: create.name.clone(),
                settings: placement_settings_from_options(&create.options)?,
                if_not_exists: create.if_not_exists,
                or_replace: create.or_replace,
            }))
        }
        DdlStmt::AlterPlacementPolicy(alter) => Ok(Some(DdlStatement::AlterPlacementPolicy {
            name: alter.name.clone(),
            settings: placement_settings_from_options(&alter.options)?,
            if_exists: alter.if_exists,
        })),
        DdlStmt::DropPlacementPolicy(drop) => Ok(Some(DdlStatement::DropPlacementPolicy {
            name: drop.name.clone(),
            if_exists: drop.if_exists,
        })),
        DdlStmt::AlterTable(alter) => lower_alter_table_catalog(alter, default_schema, context),
        DdlStmt::RenameTable(rename) => lower_rename_table_stmt(rename, default_schema),
        DdlStmt::TruncateTable(name) => {
            let (schema, table) = split_name(name, default_schema, "table")?;
            Ok(Some(DdlStatement::TruncateTable { schema, table }))
        }
        _ => Ok(None),
    }
}

/// Admits the `ALTER TABLE` spelling of an index change.
///
/// Go lowers these actions to add/drop-index jobs and folds multi-action ALTER
/// sub-jobs over one evolving table. Reusing the same lowered representation
/// keeps catalog changes and ordered backfills in one transaction.
/// Go `SetDirectPlacementOpt` (`ddl/placement_policy.go:530`): folds the
/// source-ordered options into one settings record, a later option of the
/// same kind overwriting an earlier one.
/// Go `updateExistPlacementPolicy`'s bundle rebuild
/// (`ddl/placement_policy.go:296-317`).
///
/// ONE bundle is built from the new settings and then cloned and re-pointed
/// per referencing object, which is what makes every object share the altered
/// policy's rules rather than a snapshot of them.
///
/// A TABLE's bundle covers its own id AND every partition id, because the
/// table's rules are what a partition naming no policy of its own falls under
/// -- that is the same reason Go does not copy an inherited policy down onto
/// a partition. A partition that DOES name the policy gets its own bundle at
/// the partition rule index, which overrides the table's for its range.
///
/// Go additionally resets the `global` and `meta` RANGE bundles when a range
/// names the policy. `ALTER RANGE` is not served here, so no range can name
/// one, and that arm has nothing to walk.
fn rebuilt_bundles_for_policy(
    catalog: &ClusterCatalog,
    policy: &CiString,
    settings: &tidb_model::PlacementSettings,
) -> Result<Vec<tidb_placement::Bundle>, DdlPlanError> {
    let bundle = tidb_placement::new_bundle_from_options(Some(settings)).map_err(|error| {
        DdlPlanError::Admission(DdlAdmissionError::new(format!(
            "building placement rules: {error}"
        )))
    })?;
    let names = |reference: &Option<tidb_model::GoShared<tidb_model::PolicyRefInfo>>| {
        reference
            .as_ref()
            .is_some_and(|reference| reference.read().name.lowercase() == policy.lowercase())
    };
    let mut bundles = Vec::new();
    for database in &catalog.databases {
        for table in &database.tables {
            let definitions = table
                .partition
                .as_ref()
                .map(|partition| partition.read().definitions.snapshot())
                .unwrap_or_default();
            if names(&table.placement_policy_ref) {
                let mut ids = vec![table.id];
                ids.extend(definitions.iter().map(|definition| definition.id));
                let mut copy = bundle.clone_bundle();
                copy.reset(tidb_placement::RULE_INDEX_TABLE, &ids);
                bundles.push(copy);
            }
            for definition in &definitions {
                if names(&definition.placement_policy_ref) {
                    let mut copy = bundle.clone_bundle();
                    copy.reset(tidb_placement::RULE_INDEX_PARTITION, &[definition.id]);
                    bundles.push(copy);
                }
            }
        }
    }
    Ok(bundles)
}

/// Go's `PolicyGetter` over the policies this statement already read.
///
/// Bundles resolve a reference BY ID, so the getter is keyed by id. The
/// policies are collected once from the same snapshot the statement plans
/// against, rather than re-read per lookup, so a bundle set cannot be built
/// half from one view of the catalog and half from another.
struct SnapshotPolicies {
    policies: Vec<tidb_model::PolicyInfo>,
}

impl tidb_placement::PolicyGetter for SnapshotPolicies {
    fn get_policy(
        &self,
        policy_id: i64,
    ) -> Result<tidb_model::PolicyInfo, tidb_placement::PlacementError> {
        // A reference is stamped from a policy that existed when the
        // statement resolved it, so a miss here means the catalog and the
        // reference disagree -- an internal inconsistency, not user error.
        self.policies
            .iter()
            .find(|policy| policy.id == policy_id)
            .cloned()
            .ok_or_else(|| {
                tidb_placement::PlacementError::wrap(
                    tidb_placement::PlacementErrorKind::InvalidBundleId,
                    format!("no placement policy with id {policy_id}"),
                )
            })
    }
}

/// Every stored policy, read from the statement's own snapshot.
fn load_policies<S: MetaSnapshot>(snapshot: &mut S) -> Result<SnapshotPolicies, DdlPlanError> {
    let mut policies = Vec::new();
    for (_, encoded) in snapshot.scan_prefix(&key::policies_kv_prefix())? {
        policies.push(
            value::parse_policy_info(&encoded)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        );
    }
    Ok(SnapshotPolicies { policies })
}

/// The stored policy a name refers to, folded as Go folds it.
///
/// Go reads policies out of the infoschema; this reads them from the same
/// snapshot the rest of the statement plans against, so a policy created by
/// a concurrent DDL is either visible to the whole statement or to none of
/// it.
fn find_policy<S: MetaSnapshot>(
    snapshot: &mut S,
    name: &str,
) -> Result<Option<tidb_model::PolicyInfo>, DdlPlanError> {
    let folded = CiString::new(name.to_owned());
    for (_, encoded) in snapshot.scan_prefix(&key::policies_kv_prefix())? {
        let policy = value::parse_policy_info(&encoded)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
        if policy.name.lowercase() == folded.lowercase() {
            return Ok(Some(policy));
        }
    }
    Ok(None)
}

/// Go `CheckPlacementPolicyNotInUseFromInfoSchema`: whether any table or
/// partition still names this policy.
fn policy_referenced(catalog: &ClusterCatalog, policy: &CiString) -> bool {
    catalog.databases.iter().any(|database| {
        database.tables.iter().any(|table| {
            if table
                .placement_policy_ref
                .as_ref()
                .is_some_and(|reference| reference.read().name.lowercase() == policy.lowercase())
            {
                return true;
            }
            table.partition.as_ref().is_some_and(|partition| {
                partition
                    .read()
                    .definitions
                    .snapshot()
                    .iter()
                    .any(|definition| {
                        definition
                            .placement_policy_ref
                            .as_ref()
                            .is_some_and(|reference| {
                                reference.read().name.lowercase() == policy.lowercase()
                            })
                    })
            })
        })
    })
}

fn placement_settings_from_options(
    options: &[tidb_ast::PlacementOption],
) -> Result<tidb_model::PlacementSettings, DdlAdmissionError> {
    let mut settings = tidb_model::PlacementSettings::default();
    for option in options {
        match option {
            tidb_ast::PlacementOption::PrimaryRegion(v) => settings.primary_region = v.clone(),
            tidb_ast::PlacementOption::Regions(v) => settings.regions = v.clone(),
            tidb_ast::PlacementOption::Followers(v) => settings.followers = *v,
            tidb_ast::PlacementOption::Voters(v) => settings.voters = *v,
            tidb_ast::PlacementOption::Learners(v) => settings.learners = *v,
            tidb_ast::PlacementOption::Schedule(v) => settings.schedule = v.clone(),
            tidb_ast::PlacementOption::Constraints(v) => settings.constraints = v.clone(),
            tidb_ast::PlacementOption::LeaderConstraints(v) => {
                settings.leader_constraints = v.clone();
            }
            tidb_ast::PlacementOption::FollowerConstraints(v) => {
                settings.follower_constraints = v.clone();
            }
            tidb_ast::PlacementOption::VoterConstraints(v) => {
                settings.voter_constraints = v.clone();
            }
            tidb_ast::PlacementOption::LearnerConstraints(v) => {
                settings.learner_constraints = v.clone();
            }
            tidb_ast::PlacementOption::SurvivalPreferences(v) => {
                settings.survival_preferences = v.clone();
            }
            // Go's `SetDirectPlacementOpt` has no arm for `PLACEMENT POLICY =
            // name`: that spelling is a table option or `ALTER RANGE`, not a
            // setting of a policy itself, and its `default` refuses it.
            tidb_ast::PlacementOption::Policy(_) => {
                return Err(DdlAdmissionError::unsupported(
                    "PLACEMENT POLICY = <name> is not a setting of a policy itself",
                ));
            }
        }
    }
    Ok(settings)
}

fn lower_alter_table_catalog(
    alter: &AlterTableStmt,
    default_schema: &str,
    context: &tidb_executor::StmtContext,
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
                } => {
                    actions.push(AlterColumnAction::Add {
                        if_not_exists: *if_not_exists,
                        column: Box::new(column.clone()),
                        position: position.clone(),
                        context: DdlStatementContext(context.clone()),
                    });
                }
                tidb_ast::AlterTableAction::ChangeColumn {
                    if_exists,
                    old_name,
                    column,
                    position,
                } => {
                    if *if_exists {
                        return Ok(None);
                    }
                    if column
                        .options
                        .iter()
                        .any(|option| !matches!(option, tidb_ast::ColumnOption::Null))
                    {
                        return Err(DdlAdmissionError::unsupported(
                            "CHANGE COLUMN with options changes more than the name and type; \
                     this node serves the option-free form only",
                        ));
                    }
                    let [old] = old_name.as_slice() else {
                        return Err(DdlAdmissionError::new(
                            "CHANGE COLUMN takes an unqualified source column name here",
                        ));
                    };
                    let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                    return Ok(Some(DdlStatement::ModifyColumn {
                        schema,
                        table,
                        column: Box::new(column.clone()),
                        position: position.clone(),
                        context: DdlStatementContext(context.clone()),
                        rename_from: Some(old.clone()),
                    }));
                }
                tidb_ast::AlterTableAction::RenameColumn(rename) => {
                    let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                    return Ok(Some(DdlStatement::RenameColumn {
                        schema,
                        table,
                        from: rename.from.clone(),
                        to: rename.to.clone(),
                    }));
                }
                tidb_ast::AlterTableAction::DropColumn { if_exists, name } => {
                    actions.push(AlterColumnAction::Drop {
                        if_exists: *if_exists,
                        column: name.clone(),
                    });
                }
                tidb_ast::AlterTableAction::AddIndexConstraint(index) => {
                    // The standalone lowering owns the validation; reuse it
                    // whole so the bundled and single spellings cannot drift.
                    match lower_alter_add_index(alter, index, default_schema)? {
                        DdlStatement::CreateIndex {
                            if_not_exists,
                            index,
                            ..
                        } => actions.push(AlterColumnAction::AddIndex {
                            if_not_exists,
                            index,
                        }),
                        other => {
                            unreachable!(
                                "lower_alter_add_index lowers to CreateIndex, got {other:?}"
                            )
                        }
                    }
                }
                tidb_ast::AlterTableAction::DropIndex { if_exists, name } => {
                    actions.push(AlterColumnAction::DropIndex {
                        if_exists: *if_exists,
                        name: name.clone(),
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
            if let tidb_ast::TableOption::CharacterSet(_) | tidb_ast::TableOption::Collate(_) =
                option
            {
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                let (charset, collate) = match option {
                    tidb_ast::TableOption::CharacterSet(value) => (value.clone(), String::new()),
                    tidb_ast::TableOption::Collate(value) => (String::new(), value.clone()),
                    _ => unreachable!("the arm matched one of the two"),
                };
                return Ok(Some(DdlStatement::ModifyTableCharsetAndCollate {
                    schema,
                    table,
                    charset,
                    collate,
                    // Without CONVERT TO, Go moves the table default only.
                    overwrite_columns: false,
                }));
            }
            let ignored = match option {
                tidb_ast::TableOption::Engine(_) => Some("ENGINE"),
                tidb_ast::TableOption::RowFormat(_) => Some("ROW_FORMAT"),
                tidb_ast::TableOption::StorageClass(_) => Some("STORAGE_CLASS"),
                _ => None,
            };
            if let Some(option) = ignored {
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return Ok(Some(DdlStatement::IgnoredTableOption {
                    schema,
                    table,
                    option,
                }));
            }
            if let tidb_ast::TableOption::AutoIncrement(value)
            | tidb_ast::TableOption::ForceAutoIncrement(value) = option
            {
                // Go parses the option into `opt.UintValue`, so the written
                // value is an unsigned literal that is then handed to
                // `RebaseAutoID` as an `int64`.
                let new_base = value
                    .parse::<u64>()
                    .map_err(|_| DdlAdmissionError::new("AUTO_INCREMENT needs an integer value"))?
                    as i64;
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return Ok(Some(DdlStatement::RebaseAutoIncrementId {
                    schema,
                    table,
                    new_base,
                    force: matches!(option, tidb_ast::TableOption::ForceAutoIncrement(_)),
                }));
            }
            if let tidb_ast::TableOption::Comment(comment) = option {
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                // Go `AlterTableComment` validates the length against the
                // statement's SQL mode BEFORE it publishes the job: strict
                // refuses, non-strict warns and truncates.
                let comment = tidb_executor::ddl::normalize_table_comment(
                    comment,
                    &table,
                    &tidb_executor::StmtContext::for_query().with_strict(true),
                )
                .map_err(|error| DdlAdmissionError::new(error.to_string()))?;
                return Ok(Some(DdlStatement::ModifyTableComment {
                    schema,
                    table,
                    comment,
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
            if *if_exists || !column.qualifier.is_empty() {
                return Ok(None);
            }
            let Some(auto_random) = column.options.iter().find_map(|option| match option {
                tidb_ast::ColumnOption::AutoRandom(option) => Some(option),
                _ => None,
            }) else {
                // No AUTO_RANDOM: the ordinary MODIFY. The meta-only subset
                // takes an option-free redeclaration (an explicit NULL is the
                // default nullability, so it carries no change).
                if column
                    .options
                    .iter()
                    .any(|option| !matches!(option, tidb_ast::ColumnOption::Null))
                {
                    return Err(DdlAdmissionError::unsupported(
                        "MODIFY COLUMN with options changes more than the type; \
                         this node serves the option-free widening only",
                    ));
                }
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return Ok(Some(DdlStatement::ModifyColumn {
                    schema,
                    table,
                    column: Box::new(column.clone()),
                    position: position.clone(),
                    context: DdlStatementContext(context.clone()),
                    rename_from: None,
                }));
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
        tidb_ast::AlterTableAction::ChangeColumn {
            if_exists,
            old_name,
            column,
            position,
        } => {
            if *if_exists {
                return Ok(None);
            }
            if column
                .options
                .iter()
                .any(|option| !matches!(option, tidb_ast::ColumnOption::Null))
            {
                return Err(DdlAdmissionError::unsupported(
                    "CHANGE COLUMN with options changes more than the name and type; \
                     this node serves the option-free form only",
                ));
            }
            let [old] = old_name.as_slice() else {
                return Err(DdlAdmissionError::new(
                    "CHANGE COLUMN takes an unqualified source column name here",
                ));
            };
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            return Ok(Some(DdlStatement::ModifyColumn {
                schema,
                table,
                column: Box::new(column.clone()),
                position: position.clone(),
                context: DdlStatementContext(context.clone()),
                rename_from: Some(old.clone()),
            }));
        }
        tidb_ast::AlterTableAction::RenameColumn(rename) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            return Ok(Some(DdlStatement::RenameColumn {
                schema,
                table,
                from: rename.from.clone(),
                to: rename.to.clone(),
            }));
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
        tidb_ast::AlterTableAction::ConvertCharacterSet { charset, collation } => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::ModifyTableCharsetAndCollate {
                schema,
                table,
                // Go's `CHARACTER SET DEFAULT` form leaves the charset to be
                // taken from the table itself at apply time.
                charset: charset.clone().unwrap_or_default(),
                collate: collation.clone().unwrap_or_default(),
                // Go `NeedToOverwriteColCharset`: true exactly for CONVERT TO.
                overwrite_columns: true,
            }))
        }
        tidb_ast::AlterTableAction::DropPrimaryKey(_) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::DropPrimaryKey { schema, table }))
        }
        tidb_ast::AlterTableAction::RenameIndex(action) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::RenameIndex {
                schema,
                table,
                from: action.from.clone(),
                to: action.to.clone(),
            }))
        }
        tidb_ast::AlterTableAction::AlterColumnDefault(action) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            let Some(column) = action.name.last() else {
                return Err(DdlAdmissionError::new("ALTER COLUMN needs a column name"));
            };
            Ok(Some(DdlStatement::SetColumnDefault {
                schema,
                table,
                column: column.clone(),
                default_value: action.default_value.clone().map(Box::new),
                context: DdlStatementContext(context.clone()),
            }))
        }
        tidb_ast::AlterTableAction::OrderByColumns { .. } => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::OrderByColumns { schema, table }))
        }
        tidb_ast::AlterTableAction::AlterIndexVisibility(action) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AlterIndexVisibility {
                schema,
                table,
                index: action.name.clone(),
                invisible: action.visibility == tidb_ast::IndexVisibility::Invisible,
            }))
        }
        tidb_ast::AlterTableAction::AddColumns {
            if_not_exists,
            columns,
            constraints,
        } => {
            // Go `resolveAlterTableAddColumns`: every column first, followed
            // by every table constraint, all inside one multi-schema job.
            let mut actions = Vec::with_capacity(columns.len() + constraints.len());
            for column in columns {
                actions.push(AlterColumnAction::Add {
                    if_not_exists: *if_not_exists,
                    column: Box::new(column.clone()),
                    position: tidb_ast::ColumnPosition::Default,
                    context: DdlStatementContext(context.clone()),
                });
            }
            for constraint in constraints {
                match constraint {
                    tidb_ast::TableConstraint::Check(definition) => {
                        if context.enable_check_constraint() {
                            actions.push(AlterColumnAction::AddCheck {
                                definition: Box::new(definition.clone()),
                                context: DdlStatementContext(context.clone()),
                            });
                        } else {
                            context
                                .append_warning_parts(1105, "tidb_enable_check_constraint is off");
                        }
                    }
                    tidb_ast::TableConstraint::Index(index) => {
                        match lower_alter_add_index(alter, index, default_schema)? {
                            DdlStatement::CreateIndex {
                                if_not_exists,
                                index,
                                ..
                            } => actions.push(AlterColumnAction::AddIndex {
                                if_not_exists,
                                index,
                            }),
                            other => unreachable!(
                                "lower_alter_add_index lowers to CreateIndex, got {other:?}"
                            ),
                        }
                    }
                    tidb_ast::TableConstraint::ForeignKey(_) => {
                        return Err(DdlAdmissionError::unsupported(
                            "grouped ADD COLUMN with a FOREIGN KEY is not supported by this node",
                        ));
                    }
                }
            }
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            if actions.is_empty() {
                return Ok(Some(DdlStatement::IgnoredCheckConstraint { schema, table }));
            }
            Ok(Some(DdlStatement::MultiSchemaChange {
                schema,
                table,
                actions,
            }))
        }
        tidb_ast::AlterTableAction::AddColumn {
            if_not_exists,
            column,
            position,
        } => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AddColumn {
                schema,
                table,
                if_not_exists: *if_not_exists,
                column: Box::new(column.clone()),
                position: position.clone(),
                context: DdlStatementContext(context.clone()),
            }))
        }
        tidb_ast::AlterTableAction::AddCheck(definition) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            if !context.enable_check_constraint() {
                context.append_warning_parts(1105, "tidb_enable_check_constraint is off");
                return Ok(Some(DdlStatement::IgnoredCheckConstraint { schema, table }));
            }
            Ok(Some(DdlStatement::AddCheckConstraint {
                schema,
                table,
                definition: Box::new(definition.clone()),
                context: DdlStatementContext(context.clone()),
            }))
        }
        tidb_ast::AlterTableAction::DropCheck(drop) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::DropCheckConstraint {
                schema,
                table,
                name: drop.name.clone(),
                context: DdlStatementContext(context.clone()),
            }))
        }
        tidb_ast::AlterTableAction::AlterCheck(action) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            if !context.enable_check_constraint() {
                context.append_warning_parts(1105, "tidb_enable_check_constraint is off");
                return Ok(Some(DdlStatement::IgnoredCheckConstraint { schema, table }));
            }
            Ok(Some(DdlStatement::AlterCheckConstraint {
                schema,
                table,
                name: action.name.clone(),
                enforced: action.enforced,
                context: DdlStatementContext(context.clone()),
            }))
        }
        tidb_ast::AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Add { .. }) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AddPartitions {
                schema,
                table,
                sql: Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::AlterTable(Box::new(
                    alter.clone(),
                ))))
                .restore(),
            }))
        }
        tidb_ast::AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Drop { .. }) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::DropPartitions {
                schema,
                table,
                sql: Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::AlterTable(Box::new(
                    alter.clone(),
                ))))
                .restore(),
            }))
        }
        tidb_ast::AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Truncate {
            ..
        }) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::TruncatePartitions {
                schema,
                table,
                sql: Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::AlterTable(Box::new(
                    alter.clone(),
                ))))
                .restore(),
            }))
        }
        tidb_ast::AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Exchange {
            partition,
            table: standalone,
            with_validation,
        }) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            let (standalone_schema, standalone_table) =
                split_name(standalone, default_schema, "exchange table")?;
            Ok(Some(DdlStatement::ExchangePartition {
                schema,
                table,
                partition: partition.clone(),
                standalone_schema,
                standalone_table,
                with_validation: *with_validation,
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
#[allow(clippy::too_many_arguments)]
fn apply_add_column(
    info: &mut TableInfo,
    schema: &str,
    table: &str,
    column: &tidb_ast::ColumnDef,
    position: &tidb_ast::ColumnPosition,
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
    // An admission refusal already carries Go's own error number (8200 for
    // `ErrUnsupportedDDLOperation`); wrapping it as an encode failure both
    // flattened that to the generic 1105 and prefixed the client's message
    // with "catalog encode failed", which names an internal step the
    // statement never reached.
    let appended = info.columns.len();
    let destination = locate_offset_to_move(appended, position, info)?;
    let generated_preceding = info
        .columns
        .iter_deref()
        .take(destination)
        .map(|column| column.read().clone_like_go())
        .collect::<Vec<_>>();
    let mut added = crate::table_info_build::build_added_column(
        column,
        &info.charset,
        &info.collate,
        context,
        Some(&generated_preceding),
    )
    .map_err(DdlPlanError::Admission)?;
    // Go `AllocateColumnID`: ids only ever grow, so a dropped column's id is
    // never reused.
    info.max_column_id += 1;
    added.id = info.max_column_id;
    added.offset = info.columns.len() as i64;
    added.state = tidb_model::SchemaState::PUBLIC;
    info.columns.push_handle_go(Some(GoShared::new(added)));
    // Go `onAddColumn`'s write-reorganization step: the column is APPENDED
    // first and only then moved to where `FIRST`/`AFTER` asked for, which
    // is why the destination is computed against the appended layout.
    move_column_info(info, appended, destination);
    Ok(AlterColumnOutcome::Applied)
}

/// Go `LocateOffsetToMove` (`ddl/column.go:516`): where a column at
/// `current_offset` must end up for this position clause.
///
/// `AFTER` names a column that must EXIST and be public; Go answers
/// `ErrColumnNotExists` otherwise. The `current_offset <= c.Offset` arm is
/// Go's: a column already left of its anchor lands ON the anchor's offset,
/// because removing it first shifts the anchor down by one.
fn locate_offset_to_move(
    current_offset: usize,
    position: &tidb_ast::ColumnPosition,
    info: &TableInfo,
) -> Result<usize, DdlPlanError> {
    match position {
        tidb_ast::ColumnPosition::Default => Ok(current_offset),
        tidb_ast::ColumnPosition::First => Ok(0),
        tidb_ast::ColumnPosition::After(name) => {
            let wanted = name.to_lowercase();
            let mut anchor = None;
            for column in info.columns.iter_deref() {
                let column = column.read();
                if column.name.lowercase() == wanted
                    && column.state == tidb_model::SchemaState::PUBLIC
                {
                    anchor = Some(column.offset);
                    break;
                }
            }
            let anchor = anchor.ok_or_else(|| {
                // Go `infoschema.ErrColumnNotExists` (1054).
                DdlPlanError::UnknownColumn {
                    column: name.clone(),
                    table: info.name.original().to_owned(),
                }
            })?;
            let anchor = usize::try_from(anchor).unwrap_or(0);
            if current_offset <= anchor {
                Ok(anchor)
            } else {
                Ok(anchor + 1)
            }
        }
    }
}

/// Go `TableInfo.MoveColumnInfo` (`meta/model/table.go:434`): moves one
/// column within the ordered list, renumbering every offset it passed and
/// re-pointing every INDEX column that addressed one of them.
///
/// The stored rows are untouched -- a row's values are keyed by column id,
/// not by position -- so this is purely a reordering of the descriptor
/// every reader resolves names through.
fn move_column_info(info: &mut TableInfo, from: usize, to: usize) {
    if from == to {
        return;
    }
    // Go builds `updatedOffsets` while shifting, then re-points the index
    // columns through it; the same map, built the same way.
    let mut updated: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
    let handles: Vec<_> = (0..info.columns.len())
        .map(|position| info.columns.get(position))
        .collect();
    let source = handles[from].clone();
    let mut reordered = handles;
    if from < to {
        for index in from..to {
            reordered[index] = reordered[index + 1].clone();
            updated.insert((index + 1) as i64, index as i64);
        }
    } else {
        for index in (to + 1..=from).rev() {
            reordered[index] = reordered[index - 1].clone();
            updated.insert((index - 1) as i64, index as i64);
        }
    }
    reordered[to] = source;
    for (position, handle) in reordered.iter().enumerate() {
        if let Some(handle) = handle {
            handle.write().offset = position as i64;
        }
    }
    info.columns = tidb_model::GoSharedPointerSlice::from_handles(reordered);
    updated.insert(from as i64, to as i64);
    for index in info.indices.iter_deref() {
        let index = index.read();
        for column in index.columns.iter_deref() {
            let mut column = column.write();
            if let Some(moved) = updated.get(&column.offset) {
                column.offset = *moved;
            }
        }
    }
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
    // Go `IsColumnDroppableWithCheckConstraint`: a CHECK that also names
    // another column cannot survive this DROP, so refuse it with 3959. A
    // single-column CHECK is allowed; `table.LoadCheckConstraint` removes
    // that now-invalid metadata lazily when the post-DDL table is loaded.
    if let Some(constraint) = info.constraints.iter_deref().find(|constraint| {
        let constraint = constraint.read();
        constraint.constraint_cols.len() > 1
            && tidb_executor::ddl::check_constraint::uses_column(&constraint, column)
    }) {
        let constraint = constraint.read();
        let error = tidb_executor::ddl::check_constraint::column_dependency_error(
            constraint.name.original(),
            column,
        );
        return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
            error.code,
            error.message,
        )));
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
    info.constraints = tidb_model::GoSharedPointerSlice::from_handles(
        info.constraints
            .iter_deref()
            .filter_map(|constraint| {
                let constraint = constraint.read();
                (!tidb_executor::ddl::check_constraint::uses_column(&constraint, column))
                    .then(|| Some(GoShared::new(constraint.clone())))
            })
            .collect(),
    );
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
        // Go `preprocess.go handleTableName`, the ONE pass every statement's
        // table names go through:
        //
        //     if tn.Schema.L == "" {
        //         currentDB := p.sctx.GetSessionVars().CurrentDB
        //         if currentDB == "" { p.err = ErrNoDB; return }
        //         tn.Schema = ast.NewCIStr(currentDB)
        //     }
        //
        // The refusal comes BEFORE the `inCreateOrDropTable` handling there,
        // so `CREATE` is not exempt. Filling an EMPTY schema in instead left
        // this tier looking a table up in the database called `""`, which
        // answered `1049 Unknown database ''` where TiDB answers 1046 -- and
        // on a node that defaulted its sessions into `test`, the same
        // statement had silently created the table in the wrong schema.
        [object] if default_schema.is_empty() => Err(DdlAdmissionError::with_code(
            tidb_error::mysql::errcode::ErrNoDB,
            "No database selected",
        )),
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
    // LOCAL temporary metadata belongs to the issuing session and must never
    // be persisted by a cluster DDL job. The server routes it through the
    // session executor before lowering; retain this guard for direct callers.
    if create.temporary == tidb_ast::CreateTableTemporary::Local {
        return Err(DdlAdmissionError::new(
            "LOCAL temporary-table DDL belongs to the session catalog",
        ));
    }
    let (schema, table) = split_name(&create.name, default_schema, "table")?;
    // Go `BuildTableInfoWithLike` copies a table that already exists, so the
    // statement carries no column list to build from and the source has to be
    // resolved against the catalog at apply time.
    if let Some(source) = &create.like_table {
        let (source_schema, source_table) = split_name(source, default_schema, "table")?;
        return Ok(DdlStatement::CreateTableLike {
            schema,
            table,
            source_schema,
            source_table,
            temporary: create.temporary,
            on_commit_delete: create.on_commit_delete,
            if_not_exists: create.if_not_exists,
        });
    }
    // The same gate [`lower_create_index`] applies, for the same reason and
    // with the same words: a prefix-length index in the INLINE list would be
    // published into a `TableInfo` that this node's own catalog loader then
    // refuses, so the CREATE would report success and the table would not
    // exist. `CREATE INDEX` and `ALTER TABLE ... ADD INDEX` have always
    // refused it; without this, only the inline spelling slipped through.
    for constraint in &create.table_constraints {
        let tidb_ast::TableConstraint::Index(index) = constraint else {
            continue;
        };
        for part in &index.parts {
            if matches!(
                part,
                tidb_ast::IndexPart::Column {
                    prefix_len: Some(_),
                    ..
                }
            ) {
                return Err(DdlAdmissionError::unsupported(
                    "a prefix-length index is not supported by this node, which neither \
                     reads nor writes entries cut to a prefix",
                ));
            }
        }
    }
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
/// The gate is not a taste judgement: [`crate::cluster_catalog`]'s bounded
/// loader still refuses a prefix index, so publishing one here would write a
/// `TableInfo` that path cannot serve. The refusal names the unsupported
/// storage shape.
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
            global: create.options.global,
            ..IndexInfo::default()
        }),
    })
}

/// Go `setGlobalIndexVersion`: chooses the persisted key format for a newly
/// created global index from the cluster capability and table/index shape.
pub(crate) fn set_global_index_version(table: &TableInfo, index: &mut IndexInfo) {
    index.global_index_version = tidb_model::index::GLOBAL_INDEX_VERSION_LEGACY;
    if !tidb_model::index::get_global_index_v1_supported()
        || !index.global
        || table.has_clustered_index()
    {
        return;
    }
    let needs_partition_in_key = !index.unique
        || index.columns.iter_deref().any(|part| {
            usize::try_from(part.read().offset)
                .ok()
                .and_then(|offset| table.cols().get(offset))
                .is_some_and(|column| {
                    !column
                        .read()
                        .field_type
                        .has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
                })
        });
    if needs_partition_in_key {
        index.global_index_version = tidb_model::index::GLOBAL_INDEX_VERSION_V1;
    }
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
    /// The statement named a table that is not in the catalog (Go
    /// `infoschema.ErrTableNotExists`, 1146).
    ///
    /// Distinct from [`Self::UnknownTable`], which is Go's `ErrBadTable`
    /// (1051): `DROP TABLE` answers that one, and Go's own
    /// `TestDropTableWithoutIfExists` pins the difference. Every other
    /// statement -- ALTER, CREATE INDEX, RENAME -- resolves its table
    /// through `getSchemaAndTableByIdent` and answers 1146.
    TableNotExists {
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
    /// A refusal raised by the shared admission code while the change was
    /// being planned, carrying the MySQL error number Go reports for it.
    ///
    /// `ALTER COLUMN ... SET DEFAULT` is the first change whose validation
    /// runs against the STORED column type and therefore cannot happen at
    /// lowering time; without this the exact refusals -- 1067 for a bad
    /// default, 1171 for a NULL default on a primary key -- would all
    /// flatten to the generic 1105.
    Admission(crate::table_info_build::DdlAdmissionError),
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
    /// The statement names a column the table does not have (Go 1054,
    /// `ErrBadField`). Go answers this for a MODIFY/CHANGE of a missing
    /// column and for a `FIRST`/`AFTER` anchor that is missing or not
    /// public; this port had folded them into the generic 1105.
    UnknownColumn {
        /// The column name as written.
        column: String,
        /// The table name as written.
        table: String,
    },
    /// The table has no such index (Go 1176, `ErrKeyNotExists`), which is
    /// the code Go's ALTER INDEX visibility path answers -- distinct from
    /// the 1091 a DROP INDEX answers.
    KeyNotExists {
        /// The index name as written.
        index: String,
        /// The table name as written.
        table: String,
    },
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
            Self::TableNotExists { schema, table } => {
                write!(formatter, "Table '{schema}.{table}' doesn't exist")
            }
            Self::InvalidAutoRandom(reason) => write!(formatter, "Invalid auto random: {reason}"),
            Self::AutoIdReadFailed => {
                formatter.write_str("Failed to read auto-increment value from storage engine")
            }
            Self::Unsupported(reason) => formatter.write_str(reason),
            Self::Admission(error) => formatter.write_str(&error.reason),
            Self::TableExists { schema, table } => {
                write!(formatter, "Table '{schema}.{table}' already exists")
            }
            Self::DuplicateKeyName(name) => write!(formatter, "Duplicate key name '{name}'"),
            Self::KeyNotExists { index, table } => {
                write!(formatter, "Key '{index}' doesn't exist in table '{table}'")
            }
            Self::UnknownColumn { column, table } => {
                write!(formatter, "Unknown column '{column}' in '{table}'")
            }
            Self::DuplicateColumnName(name) => {
                write!(formatter, "Duplicate column name '{name}'")
            }
            // Go `ErrCantDropFieldOrKey`, the message DROP INDEX and
            // DROP PRIMARY KEY both answer with.
            Self::UnknownIndex(name) => {
                write!(
                    formatter,
                    "Can't DROP '{name}'; check that column/key exists"
                )
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
        /// The warning the statement raises even though it changed nothing.
        /// Go's `OrderByColumns` is the case that has one.
        warning: Option<String>,
    },
    /// The mutations to publish in one transaction.
    Write(Box<DdlWrite>),
}

/// One worker transaction decoded from a persisted active DDL job.
#[derive(Clone, Debug)]
pub struct PersistedDdlJobStep {
    /// Catalog and active-job mutations committed atomically by the worker.
    pub write: DdlWrite,
    /// Whether this transaction removes the job from the active table.
    pub terminal: bool,
}

/// Plans pinned Go CHECK-job admission and queue insertion.
///
/// `None` means the statement is not one of the three CHECK job actions. The
/// returned write set deliberately contains no table, schema-version, schema
/// diff, notifier, or MDL mutation: Go's `GenGIDAndInsertJobsWithRetry`
/// commits before the DDL owner executes the first step.
pub fn prepare_check_constraint_job_submission<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<crate::ddl_job_submit::JobSpec>, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    let (job, args) = match statement {
        DdlStatement::AddCheckConstraint {
            schema,
            table,
            definition,
            context,
        } => {
            let (schema_id, stored) = locate_table(&catalog, schema, table)?;
            let mut candidate = stored.clone_like_go();
            let prior_len = candidate.constraints.len();
            crate::table_info_build::append_check_constraints(
                &mut candidate,
                &[tidb_executor::ddl::check_constraint::CheckConstraintInput {
                    definition: (**definition).clone(),
                    in_column: None,
                }],
                &context.0,
            )
            .map_err(DdlPlanError::Admission)?;
            let mut constraint = candidate
                .constraints
                .iter_deref()
                .nth(prior_len)
                .expect("one ADD CHECK input appends one constraint")
                .read()
                .clone();
            if catalog
                .databases
                .iter()
                .find(|database| database.info.id == schema_id)
                .is_some_and(|database| {
                    database.tables.iter().any(|candidate| {
                        candidate.constraints.iter_deref().any(|existing| {
                            existing.read().name.lowercase() == constraint.name.lowercase()
                        })
                    })
                })
            {
                return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                    tidb_error::tidb::errcode::ErrCheckConstraintDupName,
                    format!("Duplicate check constraint name '{}'.", constraint.name),
                )));
            }
            // Go's executor submits StateNone/ID zero. The first owner step
            // allocates the durable constraint ID.
            constraint.id = 0;
            constraint.state = SchemaState::NONE;
            let job = new_check_constraint_job(
                schema_id,
                schema,
                stored,
                ActionType::ACTION_ADD_CHECK_CONSTRAINT,
                &context.0,
            );
            let args = GoShared::new(AddCheckConstraintArgs {
                constraint: GoField::new(Some(GoShared::new(constraint))),
            });
            (job, JobArgsValue::AddCheckConstraint(Some(args)))
        }
        DdlStatement::DropCheckConstraint {
            schema,
            table,
            name,
            context,
        } => {
            let (schema_id, stored) = locate_table(&catalog, schema, table)?;
            let constraint = stored
                .constraints
                .iter_deref()
                .find(|constraint| constraint.read().name.lowercase() == name.to_lowercase())
                .ok_or_else(|| {
                    DdlPlanError::Admission(DdlAdmissionError::with_code(
                        tidb_error::tidb::errcode::ErrConstraintNotFound,
                        format!("Constraint '{name}' does not exist."),
                    ))
                })?;
            let job = new_check_constraint_job(
                schema_id,
                schema,
                stored,
                ActionType::ACTION_DROP_CHECK_CONSTRAINT,
                &context.0,
            );
            let args = GoShared::new(CheckConstraintArgs {
                constraint_name: GoField::new(constraint.read().name.clone()),
                enforced: GoField::new(false),
            });
            (job, JobArgsValue::CheckConstraint(Some(args)))
        }
        DdlStatement::AlterCheckConstraint {
            schema,
            table,
            name,
            enforced,
            context,
        } => {
            let (schema_id, stored) = locate_table(&catalog, schema, table)?;
            let constraint = stored
                .constraints
                .iter_deref()
                .find(|constraint| constraint.read().name.lowercase() == name.to_lowercase())
                .ok_or_else(|| {
                    DdlPlanError::Admission(DdlAdmissionError::with_code(
                        tidb_error::tidb::errcode::ErrConstraintNotFound,
                        format!("Constraint '{name}' does not exist."),
                    ))
                })?;
            let job = new_check_constraint_job(
                schema_id,
                schema,
                stored,
                ActionType::ACTION_ALTER_CHECK_CONSTRAINT,
                &context.0,
            );
            let args = GoShared::new(CheckConstraintArgs {
                constraint_name: GoField::new(constraint.read().name.clone()),
                enforced: GoField::new(*enforced),
            });
            (job, JobArgsValue::CheckConstraint(Some(args)))
        }
        _ => return Ok(None),
    };

    let mut specs = [crate::ddl_job_submit::JobSpec {
        job,
        args,
        id_allocated: true,
    }];
    crate::ddl_job_submit::prepare_submit_batch(
        snapshot, &catalog, &mut specs, start_ts, upgrading, min_job_id,
    )?;
    let [spec] = specs;
    Ok(Some(spec))
}

fn new_check_constraint_job(
    schema_id: i64,
    schema: &str,
    table: &TableInfo,
    action: ActionType,
    context: &tidb_executor::StmtContext,
) -> Job {
    let mut job = Job::default();
    job.version = get_job_ver_in_use();
    job.schema_id = schema_id;
    job.table_id = table.id;
    job.schema_name = schema.to_lowercase().into();
    job.table_name = table.name.lowercase().to_owned().into();
    job.type_ = action;
    job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
    job.query = context.ddl_query().into();
    job.sql_mode = context.ddl_sql_mode();
    job.cdc_write_source = context.ddl_cdc_write_source();
    if action == ActionType::ACTION_ADD_CHECK_CONSTRAINT {
        job.priority = context.ddl_reorg_priority();
    }
    job.trace_info = Some(GoShared::new(TraceInfo {
        session_alias: context.ddl_session_alias().into(),
        trace_id: context.ddl_trace_id().to_vec().into(),
        connection_id: context.ddl_connection_id(),
    }));
    job
}

/// Plans one CHECK worker step from `mysql.tidb_ddl_job` and current table
/// metadata, matching pinned Go `runOneJobStep` plus the three CHECK action
/// handlers.
///
/// The active row is the operation authority; no statement-local continuation
/// is accepted. A process may therefore disappear after any committed phase
/// and a later owner can call this function with the same job ID.
pub fn plan_persisted_check_constraint_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    ddl_job_id: i64,
    start_ts: u64,
) -> Result<PersistedDdlJobStep, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    let job_table = crate::ddl_job_table::DdlJobTable::locate(&catalog)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    let mut active = job_table
        .load(snapshot)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?
        .into_iter()
        .find(|active| active.job.id == ddl_job_id)
        .ok_or_else(|| DdlPlanError::Encode(format!("DDL job {ddl_job_id} does not exist")))?;

    if !matches!(
        active.job.type_,
        ActionType::ACTION_ADD_CHECK_CONSTRAINT
            | ActionType::ACTION_DROP_CHECK_CONSTRAINT
            | ActionType::ACTION_ALTER_CHECK_CONSTRAINT
    ) {
        return Err(DdlPlanError::Encode(format!(
            "DDL job {ddl_job_id} has unsupported action {}",
            active.job.type_
        )));
    }
    if active.job.real_start_ts == 0 {
        active.job.real_start_ts = start_ts;
    }
    if active.job.state != JobState::ROLLINGBACK {
        active.job.state = JobState::RUNNING;
    }

    let database = catalog
        .databases
        .iter()
        .find(|database| database.info.id == active.job.schema_id)
        .ok_or_else(|| DdlPlanError::UnknownDatabase(active.job.schema_name.to_string()))?;
    let stored = database
        .tables
        .iter()
        .find(|table| table.id == active.job.table_id)
        .ok_or_else(|| DdlPlanError::TableNotExists {
            schema: database.info.name.original().to_owned(),
            table: active.job.table_name.to_string(),
        })?;
    let mut info = stored.clone_like_go();
    let mut validation = None;
    let mut terminal = false;
    let mut schema_changed = true;
    let mut update_raw_args = true;

    match active.job.type_ {
        ActionType::ACTION_ADD_CHECK_CONSTRAINT => {
            let args = tidb_model::get_add_check_constraint_args(&mut active.job)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?
                .ok_or_else(|| DdlPlanError::Encode("ADD CHECK job has nil args".to_owned()))?;
            let constraint_handle = args.read().constraint.get().ok_or_else(|| {
                DdlPlanError::Encode("ADD CHECK job has nil constraint".to_owned())
            })?;
            let mut wanted = constraint_handle.read().name.lowercase().to_owned();
            let mut position = info
                .constraints
                .iter_deref()
                .position(|constraint| constraint.read().name.lowercase() == wanted);

            if active.job.state == JobState::ROLLINGBACK {
                if let Some(position) = position {
                    info.constraints = info
                        .constraints
                        .iter_deref()
                        .enumerate()
                        .filter_map(|(offset, constraint)| {
                            (offset != position).then(|| constraint.read().clone())
                        })
                        .collect::<Vec<_>>()
                        .into();
                    active.job.state = JobState::ROLLBACK_DONE;
                    terminal = true;
                } else {
                    active.job.state = JobState::CANCELLED;
                    terminal = true;
                    schema_changed = false;
                }
            } else {
                if position.is_none() {
                    let mut constraint = constraint_handle.read().clone();
                    info.max_constraint_id += 1;
                    constraint.id = info.max_constraint_id;
                    if constraint.name.original().is_empty() {
                        let names = info
                            .constraints
                            .iter_deref()
                            .map(|constraint| constraint.read().name.lowercase().to_owned())
                            .collect::<std::collections::HashSet<_>>();
                        let mut suffix = 1_i64;
                        loop {
                            let generated = format!("{}_chk_{suffix}", info.name.lowercase());
                            if !names.contains(&generated) {
                                constraint.name = CiString::new(generated);
                                break;
                            }
                            suffix += 1;
                        }
                    }
                    wanted = constraint.name.lowercase().to_owned();
                    if database.tables.iter().any(|table| {
                        table
                            .constraints
                            .iter_deref()
                            .any(|existing| existing.read().name.lowercase() == wanted)
                    }) {
                        return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                            tidb_error::tidb::errcode::ErrCheckConstraintDupName,
                            format!("Duplicate check constraint name '{}'.", constraint.name),
                        )));
                    }
                    for dependency in &constraint.constraint_cols {
                        if !info.columns.iter_deref().any(|column| {
                            let column = column.read();
                            column.state == SchemaState::PUBLIC
                                && column.name.lowercase() == dependency.lowercase()
                        }) {
                            return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                                tidb_error::tidb::errcode::ErrTableCheckConstraintReferUnknown,
                                format!(
                                    "Check constraint '{}' refers to non-existing column '{}'.",
                                    constraint.name, dependency
                                ),
                            )));
                        }
                    }
                    *constraint_handle.write() = constraint.clone();
                    info.constraints.push_go(constraint);
                    position = Some(info.constraints.len() - 1);
                }
                let position = position.expect("ADD created or found its constraint");
                let handle = info
                    .constraints
                    .get(position)
                    .expect("ADD constraint position exists");
                let mut constraint = handle.write();
                if !constraint.enforced {
                    constraint.state = SchemaState::PUBLIC;
                    terminal = true;
                } else {
                    match constraint.state {
                        SchemaState::NONE => {
                            constraint.state = SchemaState::WRITE_ONLY;
                            active.job.schema_state = SchemaState::WRITE_ONLY;
                        }
                        SchemaState::WRITE_ONLY => {
                            constraint.state = SchemaState::WRITE_REORGANIZATION;
                            active.job.schema_state = SchemaState::WRITE_REORGANIZATION;
                        }
                        SchemaState::WRITE_REORGANIZATION => {
                            constraint.state = SchemaState::PUBLIC;
                            let constraint_name = constraint.name.original().to_owned();
                            drop(constraint);
                            validation = Some(CheckConstraintValidation {
                                table: Box::new(info.clone_like_go()),
                                constraint_name,
                                context: DdlStatementContext(default_ddl_statement_context()),
                            });
                            terminal = true;
                        }
                        state => {
                            return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                                tidb_error::tidb::errcode::ErrInvalidDDLState,
                                format!("invalid CHECK constraint state {state:?}"),
                            )));
                        }
                    }
                }
            }
        }
        ActionType::ACTION_DROP_CHECK_CONSTRAINT => {
            let args = tidb_model::get_check_constraint_args(&mut active.job)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?
                .ok_or_else(|| DdlPlanError::Encode("DROP CHECK job has nil args".to_owned()))?;
            let wanted = args.read().constraint_name.get().lowercase().to_owned();
            let position = info
                .constraints
                .iter_deref()
                .position(|constraint| constraint.read().name.lowercase() == wanted)
                .ok_or_else(|| {
                    DdlPlanError::Admission(DdlAdmissionError::with_code(
                        tidb_error::tidb::errcode::ErrConstraintNotFound,
                        format!("Constraint '{wanted}' does not exist."),
                    ))
                })?;
            let state = info
                .constraints
                .get(position)
                .expect("DROP constraint position exists")
                .read()
                .state;
            if active.job.state == JobState::ROLLINGBACK && state == SchemaState::PUBLIC {
                active.job.state = JobState::CANCELLED;
                terminal = true;
                schema_changed = false;
            } else {
                active.job.state = JobState::RUNNING;
                match state {
                    SchemaState::PUBLIC => {
                        info.constraints
                            .get(position)
                            .expect("DROP constraint position exists")
                            .write()
                            .state = SchemaState::WRITE_ONLY;
                        active.job.schema_state = SchemaState::WRITE_ONLY;
                    }
                    SchemaState::WRITE_ONLY => {
                        info.constraints = info
                            .constraints
                            .iter_deref()
                            .enumerate()
                            .filter_map(|(offset, constraint)| {
                                (offset != position).then(|| constraint.read().clone())
                            })
                            .collect::<Vec<_>>()
                            .into();
                        terminal = true;
                    }
                    state => {
                        return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                            tidb_error::tidb::errcode::ErrInvalidDDLState,
                            format!("invalid CHECK constraint state {state:?}"),
                        )));
                    }
                }
            }
        }
        ActionType::ACTION_ALTER_CHECK_CONSTRAINT => {
            let args = tidb_model::get_check_constraint_args(&mut active.job)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?
                .ok_or_else(|| DdlPlanError::Encode("ALTER CHECK job has nil args".to_owned()))?;
            let args = args.read();
            let wanted = args.constraint_name.get().lowercase().to_owned();
            let enforced = args.enforced.get();
            let position = info
                .constraints
                .iter_deref()
                .position(|constraint| constraint.read().name.lowercase() == wanted)
                .ok_or_else(|| {
                    DdlPlanError::Admission(DdlAdmissionError::with_code(
                        tidb_error::tidb::errcode::ErrConstraintNotFound,
                        format!("Constraint '{wanted}' does not exist."),
                    ))
                })?;
            let handle = info
                .constraints
                .get(position)
                .expect("ALTER constraint position exists");
            let mut constraint = handle.write();
            if active.job.state == JobState::ROLLINGBACK {
                if constraint.state == SchemaState::PUBLIC {
                    active.job.state = JobState::CANCELLED;
                    terminal = true;
                    schema_changed = false;
                } else {
                    constraint.enforced = !enforced;
                    constraint.state = SchemaState::PUBLIC;
                    active.job.state = JobState::ROLLBACK_DONE;
                    terminal = true;
                }
            } else if constraint.state == SchemaState::PUBLIC && constraint.enforced == enforced {
                terminal = true;
                schema_changed = false;
            } else if !enforced {
                constraint.enforced = false;
                terminal = true;
            } else {
                match constraint.state {
                    SchemaState::PUBLIC => {
                        constraint.state = SchemaState::WRITE_REORGANIZATION;
                        constraint.enforced = true;
                        active.job.schema_state = SchemaState::WRITE_REORGANIZATION;
                    }
                    SchemaState::WRITE_REORGANIZATION => {
                        constraint.state = SchemaState::WRITE_ONLY;
                        active.job.schema_state = SchemaState::WRITE_ONLY;
                    }
                    SchemaState::WRITE_ONLY => {
                        constraint.state = SchemaState::PUBLIC;
                        let constraint_name = constraint.name.original().to_owned();
                        drop(constraint);
                        validation = Some(CheckConstraintValidation {
                            table: Box::new(info.clone_like_go()),
                            constraint_name,
                            context: DdlStatementContext(default_ddl_statement_context()),
                        });
                        terminal = true;
                    }
                    state => {
                        return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                            tidb_error::tidb::errcode::ErrInvalidDDLState,
                            format!("invalid CHECK constraint state {state:?}"),
                        )));
                    }
                }
            }
        }
        _ => unreachable!("the action was checked above"),
    }

    let schema_version = if schema_changed {
        catalog.schema_version + 1
    } else {
        0
    };
    let mut mutations = Vec::new();
    let diff = if schema_changed {
        info.update_ts = start_ts;
        mutations.push(OptimisticMutation::meta_put(
            key::table_kv_key(database.info.id, info.id),
            value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?);
        let diff = SchemaDiff {
            version: schema_version,
            action_type: active.job.type_,
            schema_id: database.info.id,
            table_id: info.id,
            ..SchemaDiff::default()
        };
        mutations.push(OptimisticMutation::meta_put(
            key::schema_version_kv_key(),
            value::encode_int_value(schema_version),
        )?);
        mutations.push(OptimisticMutation::meta_put(
            key::schema_diff_kv_key(schema_version),
            value::serialize_schema_diff(&diff)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?);
        active.job.last_schema_version = schema_version;
        diff
    } else {
        update_raw_args = false;
        SchemaDiff::default()
    };

    if terminal {
        if !matches!(
            active.job.state,
            JobState::ROLLBACK_DONE | JobState::CANCELLED
        ) {
            active.job.finish_table_job(
                JobState::DONE,
                if active.job.type_ == ActionType::ACTION_DROP_CHECK_CONSTRAINT {
                    SchemaState::NONE
                } else {
                    SchemaState::PUBLIC
                },
                schema_version,
                Some(GoShared::new(info.clone_like_go())),
            );
        }
        active
            .job
            .binlog_info
            .as_ref()
            .expect("CHECK jobs always carry BinlogInfo")
            .write()
            .finished_ts = start_ts;
        active.job.sequence_number = DDL_HISTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed) + 1;
        let encoded = active
            .job
            .encode(true)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
        if let Ok(history_table) = crate::ddl_history_table::DdlHistoryTable::locate(&catalog) {
            let _ =
                history_table.append_insert_ignore(snapshot, &active.job, &encoded, &mut mutations);
        }
        mutations.push(OptimisticMutation::meta_put(
            key::ddl_job_history_kv_key(active.job.id),
            encoded,
        )?);
        job_table
            .append_delete(&active, &mut mutations)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    } else {
        job_table
            .append_update(&mut active, update_raw_args, &mut mutations)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    }

    Ok(PersistedDdlJobStep {
        write: DdlWrite {
            ddl_job_id,
            mutations,
            schema_version,
            diff,
            created_id: None,
            backfill: Vec::new(),
            exchange_partition_validation: None,
            check_constraint_validation: validation,
            mdl_info_update: schema_changed
                .then(|| mdl_info_update(&catalog, info.id))
                .transpose()?,
            exchange_partition_label_swap: None,
            warning: None,
            placement_bundles: Vec::new(),
            placement_rollback_bundles: Vec::new(),
        },
        terminal,
    })
}

/// Persists Go's `Running -> Rollingback` transition after CHECK validation
/// returns 3819. No schema metadata changes in this transaction; the next
/// ordinary worker step reads this state and performs the action-specific
/// rollback.
pub fn plan_check_constraint_job_rollingback<S: MetaSnapshot>(
    snapshot: &mut S,
    ddl_job_id: i64,
    error_code: u16,
    error_message: &str,
) -> Result<Vec<OptimisticMutation>, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    let job_table = crate::ddl_job_table::DdlJobTable::locate(&catalog)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    let mut active = job_table
        .load(snapshot)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?
        .into_iter()
        .find(|active| active.job.id == ddl_job_id)
        .ok_or_else(|| DdlPlanError::Encode(format!("DDL job {ddl_job_id} does not exist")))?;
    active.job.state = JobState::ROLLINGBACK;
    active.job.error = Some(GoShared::new(tidb_error::terror::TerrorError::compatible(
        tidb_error::terror::TerrorCode::new(
            isize::try_from(error_code).expect("u16 error code fits isize"),
        ),
        error_message,
    )));
    active.job.error_count += 1;
    let mut mutations = Vec::new();
    job_table
        // This is a separate transaction after the validation step. The job
        // was freshly decoded from `job_meta`, so its private decoded-args
        // cache is intentionally empty; refreshing raw args here would erase
        // the durable action arguments. Go's `countForError` retains the raw
        // arguments when it persists this envelope-only state transition.
        .append_update(&mut active, false, &mut mutations)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    Ok(mutations)
}

/// One catalog change's complete write set.
#[derive(Clone, Debug)]
pub struct DdlWrite {
    /// Go `model.Job.ID`, allocated after IDs owned by the job.
    pub ddl_job_id: i64,
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
    pub backfill: Vec<IndexBackfill>,
    /// The row-routing proof owed by `EXCHANGE PARTITION ... WITH
    /// VALIDATION`, evaluated from the same snapshot before this write set is
    /// committed. `None` is Go's `WITHOUT VALIDATION` path.
    pub exchange_partition_validation: Option<ExchangePartitionValidation>,
    /// Existing-row validation owed before an enforced CHECK becomes public.
    pub check_constraint_validation: Option<CheckConstraintValidation>,
    /// The `mysql.tidb_mdl_info` row that must be replaced atomically with
    /// this schema phase before the owner waits for acknowledgements.
    pub mdl_info_update: Option<MdlInfoUpdate>,
    /// The PD region-label rule swap owed by `EXCHANGE PARTITION`.
    pub exchange_partition_label_swap: Option<ExchangePartitionLabelSwap>,
    /// The warning the change raises, if any.
    ///
    /// Go carries this as `job.Warning` (and, for the same adjustment made at
    /// admission time, `StmtCtx.AppendWarning`); the client reads it back with
    /// `SHOW WARNINGS`. A change that silently did something other than what
    /// was written would otherwise look like it did exactly what was written.
    pub warning: Option<String>,
    /// The placement rule bundles PD has to be told about before this change
    /// becomes visible.
    ///
    /// Go sends these inside the DDL job, BEFORE the schema version is
    /// published, and fails the job when PD refuses
    /// (`PutRuleBundlesWithDefaultRetry`). The transactional analogue is to
    /// deliver before the commit and abort the statement on failure: a
    /// catalog that claims placement PD never accepted is a table whose rows
    /// live somewhere other than where it says they do.
    pub placement_bundles: Vec<tidb_placement::Bundle>,
    /// The pre-change placement bundles for an exchange-partition attempt.
    ///
    /// Rust publishes the catalog change with an optimistic transaction, so
    /// it must undo the already-delivered PD change if that transaction loses
    /// its commit race. These bundles come from the same metadata snapshot as
    /// the forward bundles; deriving them here avoids a PD read that Go's
    /// exchange path never performs.
    pub placement_rollback_bundles: Vec<tidb_placement::Bundle>,
}

/// Go `checkExchangePartitionRecordValidation` expressed as a data obligation
/// on the final metadata write.
#[derive(Clone, Debug)]
pub struct ExchangePartitionValidation {
    /// Partitioned table before the physical-ID swap.
    pub partitioned: Box<TableInfo>,
    /// Standalone table whose existing rows must all route to `partition_id`.
    pub standalone: Box<TableInfo>,
    /// Physical ID of the named partition before the swap.
    pub partition_id: i64,
}

/// One Go `registerMDLInfo` replacement associated with a committed CHECK
/// schema phase.
#[derive(Clone, Debug)]
pub struct MdlInfoUpdate {
    /// Stored `mysql.tidb_mdl_info` table definition used by the ordinary
    /// clustered-row encoder.
    pub table: Box<TableInfo>,
    /// Tables touched by the job, stored as Go's comma-separated ID list.
    pub table_ids: Vec<i64>,
}

impl MdlInfoUpdate {
    fn row_values(
        &self,
        ddl_job_id: i64,
        schema_version: i64,
        owner_id: &str,
    ) -> Result<crate::system_row_write::RowValues, DdlPlanError> {
        let column_id = |name: &str| {
            self.table
                .cols()
                .iter_deref()
                .find(|column| column.read().name.lowercase() == name)
                .map(|column| column.read().id)
                .ok_or_else(|| {
                    DdlPlanError::Encode(format!("mysql.tidb_mdl_info has no column `{name}`"))
                })
        };
        let mut values = crate::system_row_write::RowValues::new();
        values.insert(column_id("job_id")?, Datum::Int(ddl_job_id));
        values.insert(column_id("version")?, Datum::Int(schema_version));
        values.insert(
            column_id("table_ids")?,
            Datum::Bytes(
                self.table_ids
                    .iter()
                    .map(i64::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
                    .into_bytes(),
            ),
        );
        values.insert(
            column_id("owner_id")?,
            Datum::Bytes(owner_id.as_bytes().to_vec()),
        );
        Ok(values)
    }

    /// Appends the clustered system-row mutations for this phase.
    ///
    /// Go deletes the row after every successful schema-sync wait, so each
    /// following phase inserts a fresh row rather than updating the preceding
    /// phase's value.
    pub fn append_mutations(
        &self,
        ddl_job_id: i64,
        schema_version: i64,
        owner_id: &str,
        mutations: &mut Vec<OptimisticMutation>,
    ) -> Result<(), DdlPlanError> {
        let values = self.row_values(ddl_job_id, schema_version, owner_id)?;
        mutations.extend(
            crate::system_row_write::replace_unindexed_clustered_row(&self.table, &values)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        );
        Ok(())
    }

    /// Appends Go `cleanMDLInfo`'s clustered-row deletion after a successful
    /// schema-sync wait.
    pub fn append_delete_mutations(
        &self,
        ddl_job_id: i64,
        schema_version: i64,
        owner_id: &str,
        mutations: &mut Vec<OptimisticMutation>,
    ) -> Result<(), DdlPlanError> {
        let values = self.row_values(ddl_job_id, schema_version, owner_id)?;
        mutations.extend(
            crate::system_row_write::delete_unindexed_clustered_row(&self.table, &values)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        );
        Ok(())
    }
}

/// The names and post-exchange physical IDs Go uses to swap the standalone
/// table and partition label rules in PD.
#[derive(Clone, Debug)]
pub struct ExchangePartitionLabelSwap {
    /// Partitioned-table database name.
    pub partitioned_schema: String,
    /// Partitioned-table name.
    pub partitioned_table: String,
    /// Exchanged partition name.
    pub partition: String,
    /// Standalone-table database name.
    pub standalone_schema: String,
    /// Standalone-table name.
    pub standalone_table: String,
    /// Physical ID now owned by the named partition.
    pub partition_id: i64,
    /// Physical ID now owned by the standalone table.
    pub standalone_id: i64,
}

impl ExchangePartitionLabelSwap {
    /// The two existing rule IDs Go fetches before constructing its patch.
    pub fn rule_ids(&self, codec: &dyn tidb_executor::ddl_label::LabelCodec) -> [String; 2] {
        [
            tidb_executor::ddl_label::new_rule_id(
                codec,
                &self.standalone_schema,
                &self.standalone_table,
                "",
            ),
            tidb_executor::ddl_label::new_rule_id(
                codec,
                &self.partitioned_schema,
                &self.partitioned_table,
                &self.partition,
            ),
        ]
    }

    /// Pinned Go `onExchangeTablePartition`'s four-way label-rule patch.
    pub fn patch(
        &self,
        codec: &dyn tidb_executor::ddl_label::LabelCodec,
        rules: &[tidb_executor::ddl_label::Rule],
    ) -> tidb_executor::ddl_label::LabelRulePatch {
        let [standalone_rule_id, partition_rule_id] = self.rule_ids(codec);
        let standalone_rule = rules.iter().find(|rule| rule.id == standalone_rule_id);
        let partition_rule = rules.iter().find(|rule| rule.id == partition_rule_id);
        let mut set_rules = Vec::with_capacity(2);
        let mut delete_rules = Vec::with_capacity(1);
        if let Some(rule) = standalone_rule {
            let mut rule = rule.clone_rule();
            rule.reset(
                codec,
                &self.partitioned_schema,
                &self.partitioned_table,
                &self.partition,
                &[self.partition_id],
            );
            set_rules.push(rule);
            if partition_rule.is_none() {
                delete_rules.push(standalone_rule_id);
            }
        }
        if let Some(rule) = partition_rule {
            let mut rule = rule.clone_rule();
            rule.reset(
                codec,
                &self.standalone_schema,
                &self.standalone_table,
                "",
                &[self.standalone_id],
            );
            set_rules.push(rule);
            if standalone_rule.is_none() {
                delete_rules.push(partition_rule_id);
            }
        }
        tidb_executor::ddl_label::new_rule_patch(set_rules, delete_rules)
    }
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

/// The candidate table shape whose enforced CHECK must hold for every
/// existing row before its metadata can be published.
#[derive(Clone, Debug)]
pub struct CheckConstraintValidation {
    /// Candidate metadata, including the newly enforced constraint.
    pub table: Box<TableInfo>,
    /// The constraint whose violation Go reports as 3819.
    pub constraint_name: String,
    /// Evaluation context captured from the DDL statement.
    pub context: DdlStatementContext,
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
/// Builds the `TableInfo` a `CREATE VIEW` publishes, from the definition the
/// resolving node settled ([`tidb_executor::resolve_view_definition`]).
///
/// Field mapping is Go's own: the creator connection's client charset and
/// collation land in `TableInfo.Charset`/`Collate` (which is where
/// `SHOW CREATE VIEW`'s two charset columns read them back —
/// `executor/show.go` appends `tb.Meta().Charset, tb.Meta().Collate`), the
/// resolved output columns land in `Columns`, and the view metadata in
/// `View`. `ViewInfo.Cols` stays nil: the resolved column names already
/// carry any explicit `CREATE VIEW v (...)` list.
#[must_use]
pub fn build_view_table_info(name: &str, view: &tidb_executor::ViewDef) -> TableInfo {
    use tidb_ast::{ViewAlgorithm, ViewCheckOption, ViewSecurity};
    use tidb_model::table::ViewInfo;
    use tidb_parser::auth::UserIdentity;
    let columns = view
        .columns
        .iter()
        .enumerate()
        .map(
            |(offset, (column_name, field_type))| tidb_model::ColumnInfo {
                id: i64::try_from(offset).expect("a column offset fits in i64") + 1,
                name: CiString::new(column_name.clone()),
                offset: i64::try_from(offset).expect("a column offset fits in i64"),
                field_type: field_type.clone(),
                state: SchemaState::PUBLIC,
                ..tidb_model::ColumnInfo::default()
            },
        )
        .collect::<Vec<_>>();
    // `ast/model.go`: UNDEFINED/MERGE/TEMPTABLE are 0/1/2, DEFINER/INVOKER
    // 0/1, LOCAL/CASCADED 0/1.
    let algorithm = ViewAlgorithm(match view.algorithm.as_str() {
        "MERGE" => 1,
        "TEMPTABLE" => 2,
        _ => 0,
    });
    let security = ViewSecurity(i64::from(view.security == "INVOKER"));
    let check_option = ViewCheckOption(i64::from(view.check_option != "LOCAL"));
    TableInfo {
        name: CiString::new(name.to_owned()),
        charset: view.character_set_client.clone(),
        collate: view.collation_connection.clone(),
        columns: columns.into(),
        state: SchemaState::PUBLIC,
        view: Some(GoShared::new(ViewInfo {
            algorithm,
            definer: Some(Box::new(UserIdentity {
                username: view.definer_user.clone(),
                hostname: view.definer_host.clone(),
                ..UserIdentity::default()
            })),
            security,
            select_stmt: view.select_sql.clone(),
            check_option,
            ..ViewInfo::default()
        })),
        ..TableInfo::default()
    }
}

pub fn plan_ddl<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    start_ts: u64,
) -> Result<DdlPlan, DdlPlanError> {
    plan_ddl_with_collation(snapshot, statement, start_ts, new_collation_enabled())
}

fn apply_partition_change(
    stored: &TableInfo,
    schema: &str,
    table: &str,
    sql: &str,
) -> Result<tidb_executor::partition_routing::PartitionSpec, DdlPlanError> {
    use tidb_executor::ddl::StoredPartitionDefinition;
    use tidb_executor::{Catalog, KvColumn, KvTable, TableEntry};

    let partition = stored.partition.as_ref().ok_or_else(|| {
        DdlPlanError::Admission(DdlAdmissionError::with_code(
            1505,
            "Partition management on a not partitioned table is not possible".to_owned(),
        ))
    })?;
    let partition = partition.read();
    let names = stored
        .columns
        .iter_deref()
        .map(|column| column.read().name.original().to_owned())
        .collect::<Vec<_>>();
    let types = stored
        .columns
        .iter_deref()
        .map(|column| column.read().field_type.clone())
        .collect::<Vec<_>>();
    let definitions = partition
        .definitions
        .snapshot()
        .into_iter()
        .map(|definition| StoredPartitionDefinition {
            id: definition.id,
            name: definition.name.original().to_owned(),
            comment: definition.comment.clone(),
            less_than: definition.less_than.snapshot(),
            in_values: definition
                .in_values
                .snapshot()
                .into_iter()
                .map(|tuple| tuple.snapshot())
                .collect(),
            placement_policy: definition
                .placement_policy_ref
                .as_ref()
                .map(|reference| reference.read().clone()),
        })
        .collect::<Vec<_>>();
    let columns = partition
        .columns
        .snapshot()
        .into_iter()
        .map(|column| column.original().to_owned())
        .collect::<Vec<_>>();
    let overlapping_dropping_partition_indices = (0..definitions.len())
        .map(|index| {
            usize::try_from(partition.get_overlapping_dropping_partition_idx(index as isize)).ok()
        })
        .collect::<Vec<_>>();
    let spec = tidb_executor::ddl::partition_spec_from_metadata(
        partition.partition_type,
        &partition.expr,
        &columns,
        partition.is_empty_columns,
        &definitions,
        &overlapping_dropping_partition_indices,
        &names,
        &types,
    )
    .map_err(|error| {
        let error = error.to_mysql_error();
        DdlPlanError::Admission(DdlAdmissionError::with_code(error.code, error.message))
    })?;

    let kv_columns = stored
        .columns
        .iter_deref()
        .map(|column| {
            let column = column.read();
            KvColumn {
                name: column.name.original().to_owned(),
                id: column.id,
                field_type: column.field_type.clone(),
                column_info_version: column.version,
                default_value: None,
                origin_default: None,
                comment: column.comment.clone(),
                generated: None,
            }
        })
        .collect();
    let mut kv_table = KvTable::new(stored.id, kv_columns);
    kv_table.name = table.to_owned();
    kv_table.set_tiflash_replica(
        stored
            .tiflash_replica
            .as_ref()
            .map(|replica| replica.read().clone()),
    );
    kv_table.set_partition(spec);
    let mut catalog = Catalog::default();
    catalog.create_database(schema);
    catalog
        .register_kv_in(schema, table, kv_table)
        .map_err(|error| {
            let error = error.to_mysql_error();
            DdlPlanError::Admission(DdlAdmissionError::with_code(error.code, error.message))
        })?;
    let context = tidb_executor::StmtContext::for_query();
    tidb_executor::ddl::run_alter_table_in(sql, &mut catalog, schema, &context).map_err(
        |error| {
            let error = error.to_mysql_error();
            DdlPlanError::Admission(DdlAdmissionError::with_code(error.code, error.message))
        },
    )?;
    let Some(TableEntry::Kv(table)) = catalog.table_in(schema, table) else {
        unreachable!("the temporary partition catalog retains its table")
    };
    Ok(table
        .partition()
        .expect("ALTER ADD/DROP PARTITION retains partitioning")
        .clone())
}

fn exchange_refusal(code: u16, message: impl Into<String>) -> DdlPlanError {
    DdlPlanError::Admission(DdlAdmissionError::with_code(code, message.into()))
}

/// Pinned Go `checkFieldTypeCompatible`.
fn exchange_field_type_compatible(left: &FieldType, right: &FieldType) -> bool {
    const COMPARED_FLAGS: u32 = FieldTypeFlags::UNSIGNED
        | FieldTypeFlags::AUTO_INCREMENT
        | FieldTypeFlags::NOT_NULL
        | FieldTypeFlags::ZEROFILL
        | FieldTypeFlags::BINARY
        | FieldTypeFlags::PRI_KEY;
    left.code() == right.code()
        && left.decimal() == right.decimal()
        && left.charset_name() == right.charset_name()
        && left.collation_name() == right.collation_name()
        && (left.flen() == right.flen() || left.storage_length() != tidb_datatype::VAR_STORAGE_LEN)
        && left.flags() & COMPARED_FLAGS == right.flags() & COMPARED_FLAGS
        && left.elems_snapshot() == right.elems_snapshot()
}

fn exchange_tiflash_compatible(left: &TableInfo, right: &TableInfo) -> bool {
    match (&left.tiflash_replica, &right.tiflash_replica) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            let left = left.read();
            let right = right.read();
            left.count == right.count
                && left.available == right.available
                && left.location_labels == right.location_labels
        }
        _ => false,
    }
}

/// Pinned Go `checkExchangePartition` plus `checkTableDefCompatible`.
fn check_exchange_tables(
    partitioned: &TableInfo,
    standalone: &TableInfo,
) -> Result<(), DdlPlanError> {
    if standalone.is_view() || standalone.is_sequence() {
        return Err(exchange_refusal(1177, "Can't open table"));
    }
    if partitioned.partition.is_none() {
        return Err(exchange_refusal(
            1505,
            "Partition management on a not partitioned table is not possible",
        ));
    }
    if standalone.partition.is_some() {
        return Err(exchange_refusal(
            1732,
            format!(
                "Table '{}' is partitioned. It cannot be used in EXCHANGE PARTITION",
                standalone.name
            ),
        ));
    }
    if standalone.affinity.is_some() || partitioned.affinity.is_some() {
        return Err(exchange_refusal(
            8200,
            "Unsupported DDL operation: EXCHANGE PARTITION of a table with AFFINITY option",
        ));
    }
    if !standalone.foreign_keys.is_empty() {
        return Err(exchange_refusal(
            1740,
            format!(
                "Table '{}' has foreign key constraint. It cannot be used in EXCHANGE PARTITION",
                standalone.name
            ),
        ));
    }
    if standalone.temp_table_type != tidb_model::TempTableType::NONE {
        return Err(exchange_refusal(
            1733,
            format!(
                "Table to exchange with partition is temporary: '{}'",
                standalone.name
            ),
        ));
    }
    let different_metadata = || exchange_refusal(1736, "Tables have different definitions");
    if partitioned.auto_random_bits != standalone.auto_random_bits
        || partitioned.auto_random_range_bits != standalone.auto_random_range_bits
        || partitioned.charset != standalone.charset
        || partitioned.collate != standalone.collate
        || partitioned.shard_row_id_bits != standalone.shard_row_id_bits
        || partitioned.max_shard_row_id_bits != standalone.max_shard_row_id_bits
        || partitioned.pk_is_handle != standalone.pk_is_handle
        || partitioned.is_common_handle != standalone.is_common_handle
        || !exchange_tiflash_compatible(partitioned, standalone)
        || partitioned.cols().len() != standalone.cols().len()
    {
        return Err(different_metadata());
    }
    for (source, target) in partitioned
        .cols()
        .iter_deref()
        .zip(standalone.cols().iter_deref())
    {
        let source = source.read();
        let target = target.read();
        if source.is_virtual_generated() != target.is_virtual_generated() {
            return Err(exchange_refusal(
                3106,
                "'Exchanging partitions for non-generated columns' is not supported for generated columns.",
            ));
        }
        if source.name.lowercase() != target.name.lowercase()
            || source.hidden != target.hidden
            || !exchange_field_type_compatible(&source.field_type, &target.field_type)
            || source.generated_expr_string != target.generated_expr_string
            || source.state != SchemaState::PUBLIC
            || target.state != SchemaState::PUBLIC
        {
            return Err(different_metadata());
        }
        if source.id != target.id {
            return Err(exchange_refusal(
                1731,
                format!(
                    "Non matching attribute 'column: {}' between partition and table",
                    source.name
                ),
            ));
        }
    }
    if partitioned.indices.len() != standalone.indices.len() {
        return Err(different_metadata());
    }
    for source in partitioned.indices.iter_deref() {
        let source = source.read();
        if source.global {
            return Err(exchange_refusal(
                1731,
                format!(
                    "Non matching attribute 'global index: {}' between partition and table",
                    source.name
                ),
            ));
        }
        let Some(target) = standalone
            .indices
            .iter_deref()
            .find(|candidate| candidate.read().name.lowercase() == source.name.lowercase())
        else {
            return Err(different_metadata());
        };
        let target = target.read();
        if source.tp != target.tp
            || source.unique != target.unique
            || source.primary != target.primary
            || source.columns.len() != target.columns.len()
        {
            return Err(different_metadata());
        }
        for (source_column, target_column) in
            source.columns.iter_deref().zip(target.columns.iter_deref())
        {
            let source_column = source_column.read();
            let target_column = target_column.read();
            if source_column.length != target_column.length
                || source_column.name.lowercase() != target_column.name.lowercase()
            {
                return Err(different_metadata());
            }
        }
        if source.id != target.id {
            return Err(exchange_refusal(
                1731,
                format!(
                    "Non matching attribute 'index: {}' between partition and table",
                    source.name
                ),
            ));
        }
    }
    Ok(())
}

fn read_auto_id<S: MetaSnapshot>(snapshot: &mut S, key: &[u8]) -> Result<i64, DdlPlanError> {
    match snapshot.get(key)? {
        Some(encoded) => value::parse_int_value(&encoded)
            .map_err(|error| DdlPlanError::Encode(error.to_string())),
        None => Ok(0),
    }
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
    // Filled by the arms that change what PD must know about an object's
    // placement; empty for every other statement, and an empty list is never
    // sent.
    let mut placement_bundles: Vec<tidb_placement::Bundle> = Vec::new();
    let mut placement_rollback_bundles: Vec<tidb_placement::Bundle> = Vec::new();
    let schema_version = catalog.schema_version + 1;
    let mut writes = Vec::new();
    let mut global_ids = GlobalIdAllocator::load(snapshot)?;
    let mut created_id = None;
    let mut backfill = Vec::new();
    let mut exchange_partition_validation = None;
    let mut check_constraint_validation = None;
    let mut exchange_partition_label_swap = None;
    let mut warning = None;
    let mut schema_change_events: Vec<(i64, SchemaChangeEvent)> = Vec::new();
    let mut diff = SchemaDiff {
        version: schema_version,
        ..SchemaDiff::default()
    };

    match statement {
        // A placement policy is a schema object of its own: Go keys it under
        // `POLICIES` by id (`Mutator.CreatePolicy`), draws the id from the
        // same global counter every schema object uses, and spends a schema
        // version like any other DDL.
        DdlStatement::CreatePlacementPolicy {
            name,
            settings,
            if_not_exists,
            or_replace,
        } => {
            let existing = find_policy(snapshot, name)?;
            match (&existing, *if_not_exists, *or_replace) {
                (Some(found), true, _) => {
                    return Ok(already(format!(
                        "placement policy `{}` already exists",
                        found.name.original()
                    )));
                }
                // Go's `OnExistReplace` keeps the policy OBJECT and swaps its
                // settings, so references by id stay pointed at it.
                (Some(found), _, true) => {
                    let updated = tidb_model::PolicyInfo {
                        placement_settings: Some(tidb_model::GoShared::new(settings.clone())),
                        id: found.id,
                        name: found.name.clone(),
                        state: tidb_model::SchemaState::PUBLIC,
                    };
                    let encoded = value::serialize_policy_info(&updated)
                        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
                    writes.push(OptimisticMutation::meta_put(
                        key::policy_kv_key(found.id),
                        encoded,
                    )?);
                    diff.action_type = ActionType::ACTION_ALTER_PLACEMENT_POLICY;
                    diff.schema_id = found.id;
                }
                (Some(_), _, _) => {
                    return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                        8238,
                        format!("Placement policy '{name}' already exists"),
                    )));
                }
                (None, _, _) => {
                    let policy_id = global_ids.allocate(1)?[0];
                    created_id = Some(policy_id);
                    let policy = tidb_model::PolicyInfo {
                        placement_settings: Some(tidb_model::GoShared::new(settings.clone())),
                        id: policy_id,
                        name: CiString::new(name.clone()),
                        state: tidb_model::SchemaState::PUBLIC,
                    };
                    let encoded = value::serialize_policy_info(&policy)
                        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
                    writes.push(OptimisticMutation::meta_put(
                        key::policy_kv_key(policy_id),
                        encoded,
                    )?);
                    diff.action_type = ActionType::ACTION_CREATE_PLACEMENT_POLICY;
                    diff.schema_id = policy_id;
                }
            }
        }
        DdlStatement::AlterPlacementPolicy {
            name,
            settings,
            if_exists,
        } => {
            let Some(found) = find_policy(snapshot, name)? else {
                if *if_exists {
                    return Ok(already(format!("placement policy `{name}` does not exist")));
                }
                return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                    8239,
                    format!("Unknown placement policy '{name}'"),
                )));
            };
            // The ID survives an ALTER: references are by id, and re-issuing
            // one would orphan every object naming this policy.
            let updated = tidb_model::PolicyInfo {
                placement_settings: Some(tidb_model::GoShared::new(settings.clone())),
                id: found.id,
                name: found.name.clone(),
                state: tidb_model::SchemaState::PUBLIC,
            };
            let encoded = value::serialize_policy_info(&updated)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::policy_kv_key(found.id),
                encoded,
            )?);
            // Go `updateExistPlacementPolicy` (`ddl/placement_policy.go:285`):
            // altering a policy changes what EVERY referencing object means,
            // so their bundles are rebuilt and resent, not just the policy
            // record. Leaving them alone would store new settings that PD
            // never hears about -- the catalog would describe placement the
            // cluster is not doing.
            placement_bundles = rebuilt_bundles_for_policy(&catalog, &found.name, settings)?;
            diff.action_type = ActionType::ACTION_ALTER_PLACEMENT_POLICY;
            diff.schema_id = found.id;
        }
        DdlStatement::DropPlacementPolicy { name, if_exists } => {
            let Some(found) = find_policy(snapshot, name)? else {
                if *if_exists {
                    return Ok(already(format!("placement policy `{name}` does not exist")));
                }
                return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                    8239,
                    format!("Unknown placement policy '{name}'"),
                )));
            };
            // Go `CheckPlacementPolicyNotInUseFromInfoSchema`: a policy still
            // named by a table or a partition cannot be dropped, or those
            // objects would point at nothing. `IF EXISTS` does not excuse it
            // -- the policy exists.
            if policy_referenced(&catalog, &found.name) {
                return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                    8241,
                    format!("Placement policy '{name}' is still in use"),
                )));
            }
            writes.push(OptimisticMutation::meta_delete(key::policy_kv_key(
                found.id,
            ))?);
            diff.action_type = ActionType::ACTION_DROP_PLACEMENT_POLICY;
            diff.schema_id = found.id;
        }
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
            let db_id = global_ids.allocate(1)?[0];
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
            if !tidb_metadef::is_mem_or_sys_db(&name.to_lowercase()) {
                let tables_per_event = if database.tables.len() > 100_000 {
                    500
                } else {
                    100
                };
                for (sub_job_id, tables) in database.tables.chunks(tables_per_event).enumerate() {
                    schema_change_events.push((
                        i64::try_from(sub_job_id).expect("drop-schema event count fits in i64"),
                        SchemaChangeEvent::drop_schema(&database.info, tables),
                    ));
                }
            }
        }
        DdlStatement::DropPrimaryKey { schema, table } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            // Go `CheckIsDropPrimaryKey`. A clustered primary key -- whether
            // it is the int handle (`PKIsHandle`) or a composite one
            // (`IsCommonHandle`) -- is what the rows are STORED under, so
            // dropping it would leave every row unaddressable.
            if stored.pk_is_handle || stored.is_common_handle {
                return Err(DdlPlanError::Admission(
                    crate::table_info_build::DdlAdmissionError::with_code(
                        8200,
                        "Unsupported drop primary key when the table is using clustered index"
                            .to_owned(),
                    ),
                ));
            }
            // Go's `PRIMARY` lookup is by the reserved name, and a general
            // index that merely happens to be called `primary` is not one
            // (the #14243 fix).
            let primary = stored.indices.iter_deref().find(|index| {
                let index = index.read();
                index.name.lowercase() == "primary" && index.primary
            });
            let Some(primary) = primary else {
                // Go `ErrCantDropFieldOrKey` (1091), the same code DROP INDEX
                // uses for a name that is not there.
                return Err(DdlPlanError::UnknownIndex("PRIMARY".to_owned()));
            };
            let dropped = primary.read().clone_like_go();
            let mut info = stored.clone_like_go();
            if let Some(offset) = info.indices.iter_handles().position(|candidate| {
                candidate
                    .as_ref()
                    .expect("nil *IndexInfo in TableInfo.Indices")
                    .read()
                    .id
                    == dropped.id
            }) {
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
            // The entries have to go with the definition, exactly as
            // `DROP INDEX` does: an index whose rows survive its `TableInfo`
            // is invisible garbage that a later index of the same id would
            // read as its own.
            backfill.push(IndexBackfill {
                table: Box::new(stored.clone_like_go()),
                index: GoShared::new(dropped),
                use_new_collation,
                add: false,
            });
            diff.action_type = ActionType::ACTION_DROP_PRIMARY_KEY;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::CreateTableLike {
            schema,
            table,
            source_schema,
            source_table,
            temporary,
            on_commit_delete,
            if_not_exists,
        } => {
            // Go's preprocessor resolves and validates the LIKE source
            // before the DDL executor checks the target database. A missing
            // source database is consequently reported as a missing table.
            let (_, source) = match locate_table(&catalog, source_schema, source_table) {
                Err(DdlPlanError::UnknownDatabase(_)) => {
                    return Err(DdlPlanError::TableNotExists {
                        schema: source_schema.clone(),
                        table: source_table.clone(),
                    });
                }
                result => result?,
            };
            if source.temp_table_type != tidb_model::TempTableType::NONE {
                return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                    8006,
                    "`create table like` is unsupported on temporary tables.",
                )));
            }
            if *temporary != tidb_ast::CreateTableTemporary::None {
                // Go `checkReferInfoForTemporaryTable`, in observable order.
                let temporary_option = |operation: &str| {
                    DdlPlanError::Admission(DdlAdmissionError::with_code(
                        8006,
                        format!("`{operation}` is unsupported on temporary tables."),
                    ))
                };
                if source.auto_random_bits != 0 {
                    return Err(temporary_option("auto_random"));
                }
                if source.pre_split_regions != 0 {
                    return Err(temporary_option("pre split regions"));
                }
                if source.partition.is_some() {
                    return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                        1562,
                        "Cannot create temporary table with partitions",
                    )));
                }
                if source.shard_row_id_bits != 0 {
                    return Err(temporary_option("shard_row_id_bits"));
                }
                if source.placement_policy_ref.is_some() {
                    return Err(temporary_option("placement"));
                }
            }
            let Some(database) = find_database(&catalog, schema) else {
                return Err(DdlPlanError::UnknownDatabase(schema.clone()));
            };
            // Go `BuildTableInfoWithLike` runs after the executor has found
            // the target database, but before target-name collision handling.
            if source.view.is_some() || source.sequence.is_some() {
                return Err(DdlPlanError::Unsupported(format!(
                    "'{source_schema}.{source_table}' is not BASE TABLE"
                )));
            }
            // Go reaches `setTemporaryType` only after the source checks.
            if *temporary == tidb_ast::CreateTableTemporary::Global && !*on_commit_delete {
                return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                    8200,
                    "TiDB doesn't support ON COMMIT PRESERVE ROWS for now",
                )));
            }
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
            let table_id = global_ids.allocate(1)?[0];
            created_id = Some(table_id);
            let mut info = source.clone_like_go();
            // Go keeps only the PUBLIC columns and indices: a column or index
            // still being added is not part of the definition being copied.
            info.columns = tidb_model::GoSharedPointerSlice::from_handles(
                info.columns
                    .iter_deref()
                    .filter(|column| column.read().state == tidb_model::SchemaState::PUBLIC)
                    .map(Some)
                    .collect(),
            );
            info.indices = tidb_model::GoSharedPointerSlice::from_handles(
                info.indices
                    .iter_deref()
                    .filter(|index| index.read().state == tidb_model::SchemaState::PUBLIC)
                    .map(Some)
                    .collect(),
            );
            info.name = CiString::new(table.clone());
            info.id = table_id;
            // Go's reset list. Each line matters on its own: an inherited
            // counter would make the copy's first row collide with the
            // source's handles, and an inherited foreign key would name a
            // constraint that already exists.
            info.auto_inc_id = 0;
            info.foreign_keys = tidb_model::GoSharedPointerSlice::from_handles(Vec::new());
            // Go `renameCheckConstraint` clears every copied name, points the
            // metadata at the target table, then assigns target-local names
            // from `<table>_chk_1` in declaration order. IDs and the allocator
            // high-water remain copied from the source.
            for (offset, constraint) in info.constraints.iter_deref().enumerate() {
                let mut constraint = constraint.write();
                constraint.name =
                    CiString::new(format!("{}_chk_{}", table.to_lowercase(), offset + 1));
                constraint.table = CiString::new(table.clone());
            }
            info.table_cache_status_type = tidb_model::TableCacheStatusType::DISABLE;
            match temporary {
                tidb_ast::CreateTableTemporary::None => {
                    info.temp_table_type = tidb_model::TempTableType::NONE;
                    if let Some(replica) = &info.tiflash_replica {
                        // Go copies the pointed-to replica struct before it
                        // clears availability; mutating the shared pointer
                        // would also mutate the source's catalog metadata.
                        let mut replica = replica.read().clone();
                        replica.available = false;
                        replica.available_partition_ids = Default::default();
                        info.tiflash_replica = Some(GoShared::new(replica));
                    }
                }
                tidb_ast::CreateTableTemporary::Global => {
                    info.temp_table_type = tidb_model::TempTableType::GLOBAL;
                    info.tiflash_replica = None;
                    info.ttl_info = None;
                    info.affinity = None;
                }
                tidb_ast::CreateTableTemporary::Local => {
                    unreachable!("LOCAL temporary CREATE LIKE is rejected during lowering")
                }
            }
            info.update_ts = start_ts;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_CREATE_TABLE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.to_lowercase()) {
                schema_change_events.push((-1, SchemaChangeEvent::create_table(info)));
            }
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
            // Go's create-table path publishes the TableInfo the builder
            // produced; it does not call `TableInfo.Clone` on the way to
            // `Mutator.CreateTableOrView`. A plain struct/header copy is all
            // this immutable plan needs before stamping its two scalar
            // transaction-owned fields. `clone_like_go()` would allocate an
            // empty `Indices` slice even when the builder left it nil, turning
            // Go's persisted `"index_info":null` into `[]`.
            let mut info = build
                .for_database(&database.info.charset, &database.info.collate)
                .map_err(DdlPlanError::Admission)?;
            // Go `assignIDsForTable` (`ddl/jobsubmit/submit.go`) draws
            // `1 + len(Definitions)` ids in ONE call: the table's own first,
            // then one physical table per partition in definition order.
            // Taking them together is what makes a partitioned table's
            // physical ids a contiguous ascending block after its own.
            let partition_count = info.partition.as_ref().map_or(0, |partition| {
                partition.read().definitions.with_visible(<[_]>::len)
            }) as i64;
            let ids = global_ids.allocate(1 + partition_count)?;
            let table_id = ids[0];
            created_id = Some(table_id);
            info.id = table_id;
            if let Some(partition) = &info.partition {
                let partition = partition.read();
                for (ordinal, id) in ids[1..].iter().enumerate() {
                    partition
                        .definitions
                        .update(ordinal, |definition| definition.id = *id);
                }
            }
            // Go `CreateTableWithInfo` resolves a table's
            // `PLACEMENT POLICY = name` against the policies in the
            // infoschema and refuses an unknown one with
            // `ErrPlacementPolicyNotExists` (8239). The reference records the
            // policy's ID as well as its name, because placement bundles
            // resolve by id -- a name-only reference would describe placement
            // that never reaches the scheduler.
            //
            // This runs HERE rather than in the lowering step because the
            // lookup needs the same snapshot the rest of the statement plans
            // against; `CreateTableBuild` keeps the original statement, so
            // the written name is still in reach.
            if let Some(policy_name) =
                build
                    .create
                    .table_options
                    .iter()
                    .find_map(|option| match option {
                        tidb_ast::TableOption::PlacementPolicy(name) => Some(name.clone()),
                        _ => None,
                    })
            {
                let Some(policy) = find_policy(snapshot, &policy_name)? else {
                    return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                        8239,
                        format!("Unknown placement policy '{policy_name}'"),
                    )));
                };
                info.placement_policy_ref =
                    Some(tidb_model::GoShared::new(tidb_model::PolicyRefInfo {
                        id: policy.id,
                        name: CiString::new(policy_name),
                    }));
            }
            // Go builds the table's bundles once the ids are assigned and
            // sends them before the schema version is published
            // (`ddl/create_table.go:143`). `NewFullTableBundles` covers the
            // table's own rules AND every partition that names a policy of
            // its own.
            if info.placement_policy_ref.is_some()
                || info.partition.as_ref().is_some_and(|partition| {
                    partition
                        .read()
                        .definitions
                        .snapshot()
                        .iter()
                        .any(|definition| definition.placement_policy_ref.is_some())
                })
            {
                let policies = load_policies(snapshot)?;
                placement_bundles = tidb_placement::new_full_table_bundles(&policies, &info)
                    .map_err(|error| {
                        DdlPlanError::Admission(DdlAdmissionError::new(format!(
                            "building placement rules: {error}"
                        )))
                    })?;
            }
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
            if !tidb_metadef::is_mem_or_sys_db(&schema.to_lowercase()) {
                schema_change_events.push((-1, SchemaChangeEvent::create_table(info)));
            }
        }
        DdlStatement::CreateView {
            schema,
            name,
            or_replace,
            info,
        } => {
            let Some(database) = find_database(&catalog, schema) else {
                return Err(DdlPlanError::UnknownDatabase(schema.clone()));
            };
            let existing = find_table(database, name).map(|table| table.id);
            if existing.is_some() && !or_replace {
                return Err(DdlPlanError::TableExists {
                    schema: schema.clone(),
                    table: name.clone(),
                });
            }
            let db_id = database.info.id;
            // Go `onCreateView` under OR REPLACE drops whatever object held
            // the name — table or view alike — and deletes its auto-id
            // accessors, then creates the view under a FRESH table id
            // (`DropTableOrView` + `createTableOrViewWithCheck`).
            if let Some(old_id) = existing {
                writes.push(OptimisticMutation::meta_delete(key::table_kv_key(
                    db_id, old_id,
                ))?);
            }
            let table_id = global_ids.allocate(1)?[0];
            created_id = Some(table_id);
            let mut info = (**info).clone();
            info.id = table_id;
            info.update_ts = start_ts;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_CREATE_VIEW;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::DropView { names, if_exists } => {
            // Go's executor files one `Note 1051` per missing name under
            // IF EXISTS and one ErrBadTable naming every missing view
            // without it; a name held by a base table is ErrWrongObject
            // immediately, even under IF EXISTS.
            let mut missing = Vec::new();
            let mut dropped_any = false;
            for (schema, name) in names {
                let Some(database) = find_database(&catalog, schema) else {
                    missing.push(format!("{schema}.{name}"));
                    continue;
                };
                let Some(table) = find_table(database, name) else {
                    missing.push(format!("{schema}.{name}"));
                    continue;
                };
                if table.view.is_none() {
                    return Err(DdlPlanError::Unsupported(format!(
                        "'{schema}.{name}' is a base table, not a VIEW (Go ErrWrongObject)"
                    )));
                }
                writes.push(OptimisticMutation::meta_delete(key::table_kv_key(
                    database.info.id,
                    table.id,
                ))?);
                diff.action_type = ActionType::ACTION_DROP_VIEW;
                diff.schema_id = database.info.id;
                diff.table_id = table.id;
                dropped_any = true;
            }
            if !missing.is_empty() && !if_exists {
                return Err(DdlPlanError::Unsupported(format!(
                    "Unknown table '{}'",
                    missing.join(",")
                )));
            }
            if !dropped_any {
                return Ok(already(format!(
                    "no named view exists: {}",
                    missing.join(",")
                )));
            }
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
                return Err(DdlPlanError::TableNotExists {
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
                return Err(DdlPlanError::TableNotExists {
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
                return Err(DdlPlanError::TableNotExists {
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
            if !tidb_metadef::is_mem_or_sys_db(&schema.to_lowercase()) {
                let modified = tidb_model::column::find_column_info(&info.columns, column)
                    .expect("the altered auto-random column is present")
                    .read()
                    .clone_like_go();
                schema_change_events.push((
                    -1,
                    SchemaChangeEvent::modify_columns(info, vec![modified], false),
                ));
            }
        }
        DdlStatement::AddPartitions { schema, table, sql }
        | DdlStatement::DropPartitions { schema, table, sql } => {
            let adding = matches!(statement, DdlStatement::AddPartitions { .. });
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let transformed = apply_partition_change(stored, schema, table, sql)?;
            let old_names = stored
                .partition
                .as_ref()
                .expect("the partition change validated partitioning")
                .read()
                .definitions
                .snapshot()
                .into_iter()
                .map(|definition| definition.name.lowercase().to_owned())
                .collect::<Vec<_>>();
            let new_names = transformed
                .definitions
                .iter()
                .map(|definition| definition.name.to_ascii_lowercase())
                .collect::<Vec<_>>();
            if old_names == new_names {
                return Ok(already(format!(
                    "partition change on `{schema}`.`{table}` is already satisfied"
                )));
            }
            let added_count = transformed
                .definitions
                .len()
                .saturating_sub(old_names.len());
            let mut allocated = if added_count == 0 {
                Vec::new().into_iter()
            } else {
                global_ids
                    .allocate(i64::try_from(added_count).expect("partition count fits in i64"))?
                    .into_iter()
            };
            let old_ids = stored
                .partition
                .as_ref()
                .expect("the partition change validated partitioning")
                .read()
                .definitions
                .snapshot()
                .into_iter()
                .map(|definition| (definition.name.lowercase().to_owned(), definition.id))
                .collect::<BTreeMap<_, _>>();
            let definitions = transformed
                .definitions
                .into_iter()
                .map(|definition| {
                    let id = old_ids
                        .get(&definition.name.to_ascii_lowercase())
                        .copied()
                        .unwrap_or_else(|| {
                            allocated
                                .next()
                                .expect("one global id was allocated for every added partition")
                        });
                    let mut converted = tidb_model::partition::PartitionDefinition {
                        id,
                        name: CiString::new(definition.name),
                        comment: definition.comment,
                        placement_policy_ref: definition.placement_policy.map(GoShared::new),
                        ..tidb_model::partition::PartitionDefinition::default()
                    };
                    converted.less_than = definition.less_than.into();
                    converted.in_values = definition
                        .in_values
                        .into_iter()
                        .map(Into::into)
                        .collect::<Vec<_>>()
                        .into();
                    converted
                })
                .collect::<Vec<_>>();
            let mut info = stored.clone_like_go();
            info.partition
                .as_ref()
                .expect("the partition change validated partitioning")
                .write()
                .definitions = definitions.into();
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = if adding {
                ActionType::ACTION_ADD_TABLE_PARTITION
            } else {
                ActionType::ACTION_DROP_TABLE_PARTITION
            };
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.to_lowercase()) {
                if adding {
                    let added = info
                        .partition
                        .as_ref()
                        .expect("the changed table remains partitioned")
                        .read()
                        .definitions
                        .snapshot()
                        .into_iter()
                        .filter(|definition| !old_ids.values().any(|id| *id == definition.id))
                        .collect();
                    schema_change_events.push((
                        -1,
                        SchemaChangeEvent::add_partitions(
                            info.clone_like_go(),
                            event_partition_info(added),
                        ),
                    ));
                } else {
                    let remaining = info
                        .partition
                        .as_ref()
                        .expect("the changed table remains partitioned")
                        .read()
                        .definitions
                        .snapshot()
                        .into_iter()
                        .map(|definition| definition.id)
                        .collect::<std::collections::HashSet<_>>();
                    let dropped = stored
                        .partition
                        .as_ref()
                        .expect("the old table is partitioned")
                        .read()
                        .definitions
                        .snapshot()
                        .into_iter()
                        .filter(|definition| !remaining.contains(&definition.id))
                        .collect();
                    schema_change_events.push((
                        -1,
                        SchemaChangeEvent::drop_partitions(
                            info.clone_like_go(),
                            event_partition_info(dropped),
                        ),
                    ));
                }
            }
        }
        DdlStatement::ExchangePartition {
            schema,
            table,
            partition,
            standalone_schema,
            standalone_table,
            with_validation,
        } => {
            let (partitioned_db_id, stored_partitioned) = locate_table(&catalog, schema, table)?;
            let (standalone_db_id, stored_standalone) =
                locate_table(&catalog, standalone_schema, standalone_table)?;
            check_exchange_tables(stored_partitioned, stored_standalone)?;
            if stored_partitioned.state != SchemaState::PUBLIC {
                return Err(exchange_refusal(
                    8200,
                    format!("Table '{}' is not in public state", stored_partitioned.name),
                ));
            }
            let original_definition = stored_partitioned
                .partition
                .as_ref()
                .expect("exchange compatibility requires partitioning")
                .read()
                .definitions
                .snapshot()
                .into_iter()
                .find(|definition| definition.name.original().eq_ignore_ascii_case(partition))
                .ok_or_else(|| {
                    exchange_refusal(
                        1735,
                        format!("Unknown partition '{partition}' in table '{table}'"),
                    )
                })?;

            // Pinned Go compares the standalone policy with the effective
            // partition policy (partition override, otherwise table policy)
            // after resolving both references through the same meta snapshot.
            let partition_policy = original_definition
                .placement_policy_ref
                .as_ref()
                .or(stored_partitioned.placement_policy_ref.as_ref());
            let standalone_policy = stored_standalone.placement_policy_ref.as_ref();
            let policies = load_policies(snapshot)?;
            let resolve_policy = |reference: &GoShared<tidb_model::PolicyRefInfo>| {
                let id = reference.read().id;
                policies.policies.iter().find(|policy| policy.id == id)
            };
            match (partition_policy, standalone_policy) {
                (None, None) => {}
                (Some(_), None) | (None, Some(_)) => {
                    return Err(exchange_refusal(1736, "Tables have different definitions"));
                }
                (Some(partition_policy), Some(standalone_policy)) => {
                    match (
                        resolve_policy(partition_policy),
                        resolve_policy(standalone_policy),
                    ) {
                        (None, None) => {}
                        (Some(left), Some(right))
                            if left.name.lowercase() == right.name.lowercase() => {}
                        _ => {
                            return Err(exchange_refusal(
                                1736,
                                "Tables have different definitions",
                            ));
                        }
                    }
                }
            }

            let original_partition_id = original_definition.id;
            let original_standalone_id = stored_standalone.id;

            // The forward exchange bundles below are Go's exact
            // `bundlesForExchangeTablePartition` result after the ID swap.
            // Preserve that same result for the pre-swap objects so an
            // optimistic commit failure can restore PD without introducing a
            // non-Go placement GET request.
            let original_table_bundle =
                tidb_placement::new_table_bundle(&policies, &stored_partitioned)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            let original_partition_bundle =
                tidb_placement::new_partition_bundle(&policies, &original_definition)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            let original_standalone_bundle =
                tidb_placement::new_table_bundle(&policies, &stored_standalone)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            placement_rollback_bundles.extend(original_table_bundle);
            placement_rollback_bundles.extend(original_partition_bundle.clone());
            placement_rollback_bundles.extend(original_standalone_bundle.clone());
            if original_partition_bundle.is_none() && original_standalone_bundle.is_some() {
                placement_rollback_bundles.push(tidb_placement::new_bundle(original_partition_id));
            }
            if original_partition_bundle.is_some() && original_standalone_bundle.is_none() {
                placement_rollback_bundles.push(tidb_placement::new_bundle(original_standalone_id));
            }
            if *with_validation {
                exchange_partition_validation = Some(ExchangePartitionValidation {
                    partitioned: Box::new(stored_partitioned.clone_like_go()),
                    standalone: Box::new(stored_standalone.clone_like_go()),
                    partition_id: original_partition_id,
                });
            }
            exchange_partition_label_swap = Some(ExchangePartitionLabelSwap {
                partitioned_schema: schema.to_lowercase(),
                partitioned_table: stored_partitioned.name.lowercase().to_owned(),
                partition: original_definition.name.lowercase().to_owned(),
                standalone_schema: standalone_schema.to_lowercase(),
                standalone_table: stored_standalone.name.lowercase().to_owned(),
                partition_id: original_standalone_id,
                standalone_id: original_partition_id,
            });

            let mut partitioned_info = stored_partitioned.clone_like_go();
            let mut definitions = partitioned_info
                .partition
                .as_ref()
                .expect("the exchanged table remains partitioned")
                .read()
                .definitions
                .snapshot();
            let exchanged_definition = {
                let definition = definitions
                    .iter_mut()
                    .find(|definition| definition.id == original_partition_id)
                    .expect("the original definition is still present");
                definition.id = original_standalone_id;
                definition.clone_like_go()
            };
            partitioned_info
                .partition
                .as_ref()
                .expect("the exchanged table remains partitioned")
                .write()
                .definitions = definitions.into();
            if let Some(replica) = &partitioned_info.tiflash_replica {
                for id in replica.write().available_partition_ids.iter_mut() {
                    if *id == original_partition_id {
                        *id = original_standalone_id;
                        break;
                    }
                }
            }
            partitioned_info.update_ts = start_ts;

            let mut standalone_info = stored_standalone.clone_like_go();
            standalone_info.id = original_partition_id;
            standalone_info.exchange_partition_info = None;
            standalone_info.update_ts = start_ts;

            writes.push(OptimisticMutation::meta_delete(key::table_kv_key(
                standalone_db_id,
                original_standalone_id,
            ))?);
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(partitioned_db_id, partitioned_info.id),
                value::serialize_table_info(&partitioned_info)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?);
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(standalone_db_id, standalone_info.id),
                value::serialize_table_info(&standalone_info)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?);

            for key_for in [
                key::auto_table_id_kv_key as fn(i64, i64) -> Vec<u8>,
                key::auto_increment_id_kv_key,
                key::auto_random_table_id_kv_key,
            ] {
                let partitioned_key = key_for(partitioned_db_id, stored_partitioned.id);
                let standalone_key = key_for(standalone_db_id, original_standalone_id);
                let maximum = std::cmp::max(
                    read_auto_id(snapshot, &partitioned_key)?,
                    read_auto_id(snapshot, &standalone_key)?,
                );
                writes.push(OptimisticMutation::meta_put(
                    partitioned_key,
                    value::encode_int_value(maximum),
                )?);
                writes.push(OptimisticMutation::meta_put(
                    key_for(standalone_db_id, original_partition_id),
                    value::encode_int_value(maximum),
                )?);
            }

            // Pinned `bundlesForExchangeTablePartition`: rebuild the table,
            // exchanged partition and standalone bundles under their new IDs;
            // explicitly clear the side that lost a policy.
            let table_bundle = tidb_placement::new_table_bundle(&policies, &partitioned_info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            let partition_bundle =
                tidb_placement::new_partition_bundle(&policies, &exchanged_definition)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            let standalone_bundle = tidb_placement::new_table_bundle(&policies, &standalone_info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            placement_bundles.extend(table_bundle);
            placement_bundles.extend(partition_bundle.clone());
            placement_bundles.extend(standalone_bundle.clone());
            if partition_bundle.is_none() && standalone_bundle.is_some() {
                placement_bundles.push(tidb_placement::new_bundle(exchanged_definition.id));
            }
            if partition_bundle.is_some() && standalone_bundle.is_none() {
                placement_bundles.push(tidb_placement::new_bundle(standalone_info.id));
            }

            diff.action_type = ActionType::ACTION_EXCHANGE_TABLE_PARTITION;
            diff.schema_id = standalone_db_id;
            diff.table_id = original_partition_id;
            diff.old_schema_id = standalone_db_id;
            diff.old_table_id = original_standalone_id;
            diff.affected_options = vec![AffectedOption {
                schema_id: partitioned_db_id,
                table_id: stored_partitioned.id,
                ..AffectedOption::default()
            }]
            .into();
            warning = Some(
                "after the exchange, please analyze related table of the exchange to update statistics"
                    .to_owned(),
            );
            if !tidb_metadef::is_mem_or_sys_db(&standalone_schema.to_lowercase()) {
                schema_change_events.push((
                    -1,
                    SchemaChangeEvent::exchange_partition(
                        partitioned_info,
                        event_partition_info(vec![original_definition]),
                        stored_standalone.clone_like_go(),
                    ),
                ));
            }
        }
        DdlStatement::TruncatePartitions { schema, table, sql } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let transformed = apply_partition_change(stored, schema, table, sql)?;
            let old_ids = stored
                .partition
                .as_ref()
                .expect("the partition change validated partitioning")
                .read()
                .definitions
                .snapshot()
                .into_iter()
                .map(|definition| (definition.name.lowercase().to_owned(), definition.id))
                .collect::<BTreeMap<_, _>>();
            let replaced = transformed
                .definitions
                .iter()
                .filter(|definition| {
                    old_ids
                        .get(&definition.name.to_ascii_lowercase())
                        .is_some_and(|old_id| *old_id != definition.id)
                })
                .count();
            if replaced == 0 {
                return Ok(already(format!(
                    "partition truncate on `{schema}`.`{table}` changes no partition"
                )));
            }
            let mut allocated = global_ids
                .allocate(i64::try_from(replaced).expect("partition count fits in i64"))?
                .into_iter();
            let definitions = transformed
                .definitions
                .into_iter()
                .map(|definition| {
                    let old_id = old_ids[&definition.name.to_ascii_lowercase()];
                    let id = if definition.id == old_id {
                        old_id
                    } else {
                        allocated
                            .next()
                            .expect("one global id was allocated for every truncated partition")
                    };
                    let mut converted = tidb_model::partition::PartitionDefinition {
                        id,
                        name: CiString::new(definition.name),
                        comment: definition.comment,
                        placement_policy_ref: definition.placement_policy.map(GoShared::new),
                        ..tidb_model::partition::PartitionDefinition::default()
                    };
                    converted.less_than = definition.less_than.into();
                    converted.in_values = definition
                        .in_values
                        .into_iter()
                        .map(Into::into)
                        .collect::<Vec<_>>()
                        .into();
                    converted
                })
                .collect::<Vec<_>>();
            debug_assert!(allocated.next().is_none());
            let mut info = stored.clone_like_go();
            info.partition
                .as_ref()
                .expect("the partition change validated partitioning")
                .write()
                .definitions = definitions.into();
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_TRUNCATE_TABLE_PARTITION;
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.to_lowercase()) {
                let new_by_name = info
                    .partition
                    .as_ref()
                    .expect("the changed table remains partitioned")
                    .read()
                    .definitions
                    .snapshot()
                    .into_iter()
                    .map(|definition| (definition.name.lowercase().to_owned(), definition))
                    .collect::<BTreeMap<_, _>>();
                let old_definitions = stored
                    .partition
                    .as_ref()
                    .expect("the old table is partitioned")
                    .read()
                    .definitions
                    .snapshot();
                let dropped = old_definitions
                    .iter()
                    .filter(|definition| {
                        new_by_name
                            .get(definition.name.lowercase())
                            .is_some_and(|new| new.id != definition.id)
                    })
                    .cloned()
                    .collect();
                let added = new_by_name
                    .values()
                    .filter(|definition| {
                        old_ids
                            .get(definition.name.lowercase())
                            .is_some_and(|old| *old != definition.id)
                    })
                    .cloned()
                    .collect();
                schema_change_events.push((
                    -1,
                    SchemaChangeEvent::truncate_partitions(
                        info.clone_like_go(),
                        event_partition_info(added),
                        event_partition_info(dropped),
                    ),
                ));
            }
        }
        DdlStatement::AddCheckConstraint { .. }
        | DdlStatement::DropCheckConstraint { .. }
        | DdlStatement::AlterCheckConstraint { .. } => {
            return Err(DdlPlanError::Encode(
                "CHECK constraint DDL must execute through mysql.tidb_ddl_job".to_owned(),
            ));
        }
        DdlStatement::IgnoredCheckConstraint { schema, table } => {
            locate_table(&catalog, schema, table)?;
            return Ok(already(format!(
                "CHECK constraint on `{schema}`.`{table}` is discarded while tidb_enable_check_constraint is off"
            )));
        }
        DdlStatement::AddColumn {
            schema,
            table,
            if_not_exists,
            column,
            position,
            context,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let mut info = stored.clone_like_go();
            match apply_add_column(
                &mut info,
                schema,
                table,
                column,
                position,
                *if_not_exists,
                &context.0,
            )? {
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
            if !tidb_metadef::is_mem_or_sys_db(&schema.to_lowercase()) {
                let added = info
                    .columns
                    .iter_deref()
                    .find(|candidate| {
                        candidate.read().name.lowercase() == column.name.to_lowercase()
                    })
                    .expect("the applied column is present")
                    .read()
                    .clone_like_go();
                schema_change_events.push((
                    -1,
                    SchemaChangeEvent::add_columns(info.clone_like_go(), vec![added]),
                ));
            }
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
        DdlStatement::RenameColumn {
            schema,
            table,
            from,
            to,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let wanted = from.to_lowercase();
            let Some(position) = stored
                .columns
                .iter_deref()
                .position(|candidate| candidate.read().name.lowercase() == wanted)
            else {
                return Err(DdlPlanError::UnknownColumn {
                    column: from.clone(),
                    table: table.clone(),
                });
            };
            // Go asks `IsColumnRenameableWithCheckConstraint` before the
            // same-name early return and before the duplicate-name check.
            if let Some(constraint) = stored.constraints.iter_deref().find(|constraint| {
                tidb_executor::ddl::check_constraint::uses_column(&constraint.read(), from)
            }) {
                let constraint = constraint.read();
                let error = tidb_executor::ddl::check_constraint::column_dependency_error(
                    constraint.name.original(),
                    from,
                );
                return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                    error.code,
                    error.message,
                )));
            }
            let new_name = to.to_lowercase();
            if new_name != wanted
                && stored
                    .columns
                    .iter_deref()
                    .any(|candidate| candidate.read().name.lowercase() == new_name)
            {
                return Err(DdlPlanError::DuplicateColumnName(to.clone()));
            }
            let mut info = stored.clone_like_go();
            {
                let handle = info
                    .columns
                    .get(position)
                    .expect("the position was just found");
                handle.write().name = CiString::new(to.clone());
            }
            for index in info.indices.iter_deref() {
                let index = index.read();
                for col in index.columns.iter_deref() {
                    let mut col = col.write();
                    if col.name.lowercase() == wanted {
                        col.name = CiString::new(to.clone());
                    }
                }
            }
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_MODIFY_COLUMN;
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.to_lowercase()) {
                let modified = info
                    .columns
                    .iter_deref()
                    .find(|candidate| candidate.read().name.lowercase() == to.to_lowercase())
                    .expect("the renamed column is present")
                    .read()
                    .clone_like_go();
                schema_change_events.push((
                    -1,
                    SchemaChangeEvent::modify_columns(info.clone_like_go(), vec![modified], false),
                ));
            }
        }
        DdlStatement::ModifyColumn {
            schema,
            table,
            column,
            position: requested_position,
            context,
            rename_from,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            // A CHANGE locates by the OLD name; a MODIFY by the (unchanged)
            // declared name.
            let wanted = rename_from
                .as_deref()
                .unwrap_or(column.name.as_str())
                .to_lowercase();
            let Some(position) = stored
                .columns
                .iter_deref()
                .position(|candidate| candidate.read().name.lowercase() == wanted)
            else {
                // Go 1054 ErrBadField through the modify path.
                return Err(DdlPlanError::UnknownColumn {
                    column: rename_from
                        .as_deref()
                        .unwrap_or(column.name.as_str())
                        .to_owned(),
                    table: table.clone(),
                });
            };
            let new_name = column.name.to_lowercase();
            if new_name != wanted
                && stored
                    .columns
                    .iter_deref()
                    .any(|candidate| candidate.read().name.lowercase() == new_name)
            {
                return Err(DdlPlanError::DuplicateColumnName(column.name.clone()));
            }
            let old = stored
                .columns
                .iter_deref()
                .nth(position)
                .expect("the position was just found")
                .read()
                .clone_like_go();
            let built = crate::table_info_build::build_added_column(
                column,
                &stored.charset,
                &stored.collate,
                &context.0,
                None,
            )
            .map_err(DdlPlanError::Admission)?;
            // Go `dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(reason)`
            // renders exactly "Unsupported modify column: <reason>", and the
            // reason strings below are Go's own.
            let refuse =
                |what: &str| DdlPlanError::Encode(format!("Unsupported modify column: {what}"));
            // Go `types.CheckModifyTypeCompatible`: the change is either
            // free (metadata only) or needs a data reorganization. This node
            // runs no reorganization, so a reorg answer is refused, carrying
            // Go's own reason.
            if let Some(reason) = modify_type_reorg_reason(&old.field_type, &built.field_type) {
                return Err(refuse(&reason));
            }
            let old_not_null = old.field_type.has_flag(FieldTypeFlags::NOT_NULL);
            let new_not_null = built.field_type.has_flag(FieldTypeFlags::NOT_NULL);
            if old_not_null != new_not_null {
                return Err(refuse(
                    "changing nullability needs a data reorganization this node does not run",
                ));
            }
            let mut info = stored.clone_like_go();
            {
                let handle = info
                    .columns
                    .get(position)
                    .expect("the position was just found");
                let mut stored_column = handle.write();
                // The identity, ordering, state, and defaults are the stored
                // column's own; only the declared type widens.
                let mut field_type = built.field_type.clone();
                field_type.set_flags(stored_column.field_type.flags());
                stored_column.field_type = field_type;
                if new_name != wanted {
                    // Go `renameColumnTo`: the column and every index column
                    // naming it take the new name together.
                    stored_column.name = CiString::new(column.name.clone());
                }
            }
            if new_name != wanted {
                for index in info.indices.iter_deref() {
                    let index = index.read();
                    for col in index.columns.iter_deref() {
                        let mut col = col.write();
                        if col.name.lowercase() == wanted {
                            col.name = CiString::new(column.name.clone());
                        }
                    }
                }
            }
            // Go `modify_column.go:700`: `MODIFY COLUMN b AFTER b` names
            // the column as its own anchor, which Go answers as
            // ErrColumnNotExists on THAT column rather than as a no-op.
            if let tidb_ast::ColumnPosition::After(anchor) = requested_position {
                if anchor.to_lowercase() == wanted {
                    return Err(DdlPlanError::UnknownColumn {
                        column: rename_from
                            .as_deref()
                            .unwrap_or(column.name.as_str())
                            .to_owned(),
                        table: table.clone(),
                    });
                }
            }
            // Unlike ADD COLUMN, the column is already AT its offset, so the
            // destination is located against that rather than against an
            // appended tail (Go `modify_column.go:704`).
            let destination = locate_offset_to_move(position, requested_position, &info)?;
            move_column_info(&mut info, position, destination);
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_MODIFY_COLUMN;
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.to_lowercase()) {
                let modified = info
                    .columns
                    .iter_deref()
                    .find(|candidate| candidate.read().name.lowercase() == new_name)
                    .expect("the modified column is present")
                    .read()
                    .clone_like_go();
                schema_change_events.push((
                    -1,
                    SchemaChangeEvent::modify_columns(info.clone_like_go(), vec![modified], false),
                ));
            }
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
            let mut enforced_check = None;
            // Go `mergeAddIndex` gathers multiple ADD INDEX sub-jobs into one
            // merged sub-job at the end, preserving every other sub-job's
            // relative order. Execute the equivalent order here so later
            // metadata, backfill snapshots, and notifier sequence IDs agree.
            let add_index_count = actions
                .iter()
                .filter(|action| matches!(action, AlterColumnAction::AddIndex { .. }))
                .count();
            let mut action_order: Vec<_> = (0..actions.len()).collect();
            if add_index_count > 1 {
                action_order.sort_by_key(|offset| {
                    usize::from(matches!(
                        actions[*offset],
                        AlterColumnAction::AddIndex { .. }
                    ))
                });
            }
            let mut merged_added_indexes = Vec::new();
            for (sequence, action_offset) in action_order.into_iter().enumerate() {
                let action = &actions[action_offset];
                let outcome = match action {
                    AlterColumnAction::Add {
                        if_not_exists,
                        column,
                        position,
                        context,
                    } => {
                        let outcome = apply_add_column(
                            &mut info,
                            schema,
                            table,
                            column,
                            position,
                            *if_not_exists,
                            &context.0,
                        )?;
                        if matches!(outcome, AlterColumnOutcome::Applied)
                            && !tidb_metadef::is_mem_or_sys_db(&schema.to_lowercase())
                        {
                            let added = tidb_model::column::find_column_info(
                                &info.columns,
                                column.name.as_str(),
                            )
                            .expect("the bundle-added column is present")
                            .read()
                            .clone_like_go();
                            schema_change_events.push((
                                sequence as i64,
                                SchemaChangeEvent::add_columns(info.clone_like_go(), vec![added]),
                            ));
                        }
                        outcome
                    }
                    AlterColumnAction::Drop { if_exists, column } => {
                        apply_drop_column(&mut info, schema, table, column, *if_exists)?
                    }
                    AlterColumnAction::AddIndex {
                        if_not_exists,
                        index,
                    } => {
                        if let Some(existing) = find_index(&info, index.name.original()) {
                            let existing = existing.read();
                            if *if_not_exists {
                                satisfied.push(format!(
                                    "index `{}` already exists on `{schema}`.`{table}`",
                                    existing.name.original()
                                ));
                                continue;
                            }
                            return Err(DdlPlanError::DuplicateKeyName(
                                index.name.original().to_owned(),
                            ));
                        }
                        let mut added = index.clone_like_go();
                        // Offsets resolve against the EVOLVED columns, so an
                        // index on a column this same bundle added is legal —
                        // Go's one multi-schema job.
                        for column in added.columns.iter_deref() {
                            let mut column = column.write();
                            let Some(stored_column) = info.columns.iter_deref().find(|candidate| {
                                candidate.read().name.lowercase() == column.name.lowercase()
                            }) else {
                                return Err(DdlPlanError::UnknownIndexColumn {
                                    column: column.name.original().to_owned(),
                                    index: index.name.original().to_owned(),
                                });
                            };
                            let stored_column = stored_column.read();
                            column.name = stored_column.name.clone();
                            column.offset = stored_column.offset;
                        }
                        // The backfill decodes EXISTING rows against the
                        // evolved columns (a bundle-added column reads its
                        // origin default) but must not walk the new index.
                        let backfill_table = Box::new(info.clone_like_go());
                        info.max_index_id += 1;
                        added.id = info.max_index_id;
                        added.table = info.name.clone();
                        let added = GoShared::new(added);
                        info.indices.push_handle_go(Some(added.clone()));
                        backfill.push(IndexBackfill {
                            table: backfill_table,
                            index: added.clone(),
                            use_new_collation,
                            add: true,
                        });
                        if add_index_count > 1 {
                            merged_added_indexes.push(added.read().clone_like_go());
                        } else if !tidb_metadef::is_mem_or_sys_db(&schema.to_lowercase()) {
                            schema_change_events.push((
                                sequence as i64,
                                SchemaChangeEvent::add_indexes(
                                    info.clone_like_go(),
                                    vec![added.read().clone_like_go()],
                                    false,
                                ),
                            ));
                        }
                        applied += 1;
                        continue;
                    }
                    AlterColumnAction::DropIndex { if_exists, name } => {
                        let Some(dropped) = find_index(&info, name) else {
                            if *if_exists {
                                satisfied.push(format!(
                                    "index `{name}` does not exist on `{schema}`.`{table}`"
                                ));
                                continue;
                            }
                            return Err(DdlPlanError::UnknownIndex(name.clone()));
                        };
                        let dropped = dropped.read().clone_like_go();
                        // The removal walk rebuilds the dropped index's entry
                        // keys, so its table must still CARRY the index —
                        // snapshot before the delete, evolved columns and all.
                        let backfill_table = Box::new(info.clone_like_go());
                        if let Some(offset) = info.indices.iter_handles().position(|candidate| {
                            candidate
                                .as_ref()
                                .expect("nil *IndexInfo in TableInfo.Indices")
                                .read()
                                .id
                                == dropped.id
                        }) {
                            info.indices.delete_go(offset, offset + 1);
                        }
                        backfill.push(IndexBackfill {
                            table: backfill_table,
                            index: GoShared::new(dropped),
                            use_new_collation,
                            add: false,
                        });
                        applied += 1;
                        continue;
                    }
                    AlterColumnAction::AddCheck {
                        definition,
                        context,
                    } => {
                        let prior_len = info.constraints.len();
                        crate::table_info_build::append_check_constraints(
                            &mut info,
                            &[tidb_executor::ddl::check_constraint::CheckConstraintInput {
                                definition: (**definition).clone(),
                                in_column: None,
                            }],
                            &context.0,
                        )
                        .map_err(DdlPlanError::Admission)?;
                        let added = info
                            .constraints
                            .iter_deref()
                            .nth(prior_len)
                            .expect("one grouped ADD CHECK appends one constraint")
                            .read()
                            .clone();
                        let duplicate_in_schema = catalog
                            .databases
                            .iter()
                            .find(|database| database.info.id == db_id)
                            .is_some_and(|database| {
                                database.tables.iter().any(|candidate| {
                                    candidate.constraints.iter_deref().any(|constraint| {
                                        constraint.read().name.lowercase() == added.name.lowercase()
                                    })
                                })
                            });
                        if duplicate_in_schema {
                            return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                                tidb_error::tidb::errcode::ErrCheckConstraintDupName,
                                format!("Duplicate check constraint name '{}'.", added.name),
                            )));
                        }
                        if added.enforced && enforced_check.is_none() {
                            enforced_check =
                                Some((added.name.original().to_owned(), context.clone()));
                        }
                        applied += 1;
                        continue;
                    }
                };
                match outcome {
                    AlterColumnOutcome::Applied => applied += 1,
                    AlterColumnOutcome::AlreadySatisfied(detail) => satisfied.push(detail),
                }
            }
            if !merged_added_indexes.is_empty()
                && !tidb_metadef::is_mem_or_sys_db(&schema.to_lowercase())
            {
                let sequence = actions.len() - add_index_count;
                schema_change_events.push((
                    sequence as i64,
                    SchemaChangeEvent::add_indexes(
                        info.clone_like_go(),
                        merged_added_indexes,
                        false,
                    ),
                ));
            }
            if applied == 0 {
                return Ok(already(satisfied.join("; ")));
            }
            if let Some((constraint_name, context)) = enforced_check {
                check_constraint_validation = Some(CheckConstraintValidation {
                    table: Box::new(info.clone_like_go()),
                    constraint_name,
                    context,
                });
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
            // Go `onTruncateTable` reassigns the PARTITION ids as well as the
            // table's (`ddl/table.go:510`), and its comment says why: "all the
            // old data is encoded with the old partition ID, it can not be
            // accessed anymore". A partitioned table's rows are keyed by the
            // PARTITION's physical id, not the table's -- so giving the table
            // a new id while the partitions kept theirs would leave every row
            // exactly where it was and still addressable. The truncate would
            // report success and empty nothing.
            //
            // The ids are drawn in ONE call, table first then one per
            // partition in definition order, which is the same contiguous
            // block shape `CREATE TABLE` uses.
            let partition_count = stored.partition.as_ref().map_or(0, |partition| {
                partition.read().definitions.with_visible(<[_]>::len)
            }) as i64;
            let ids = global_ids.allocate(1 + partition_count)?;
            let new_table_id = ids[0];
            let mut info = stored.clone_like_go();
            info.id = new_table_id;
            if let Some(partition) = &info.partition {
                let partition = partition.read();
                for (ordinal, id) in ids[1..].iter().enumerate() {
                    partition
                        .definitions
                        .update(ordinal, |definition| definition.id = *id);
                }
            }
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
            // Go `onTruncateTable` rebuilds the bundles once the new id is
            // stamped and sends them (`ddl/table.go:574`). Truncate gives the
            // table a NEW identity, and PD's rules are keyed by id -- so
            // without this the truncated table has no rules at all and its
            // data goes wherever the default says, silently losing the
            // placement the table still claims in its own catalog entry.
            if info.placement_policy_ref.is_some()
                || info.partition.as_ref().is_some_and(|partition| {
                    partition
                        .read()
                        .definitions
                        .snapshot()
                        .iter()
                        .any(|definition| definition.placement_policy_ref.is_some())
                })
            {
                let policies = load_policies(snapshot)?;
                placement_bundles = tidb_placement::new_full_table_bundles(&policies, &info)
                    .map_err(|error| {
                        DdlPlanError::Admission(DdlAdmissionError::new(format!(
                            "building placement rules: {error}"
                        )))
                    })?;
            }
            diff.action_type = ActionType::ACTION_TRUNCATE_TABLE;
            diff.schema_id = db_id;
            diff.table_id = new_table_id;
            diff.old_table_id = old_table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.to_lowercase()) {
                schema_change_events.push((
                    -1,
                    SchemaChangeEvent::truncate_table(info, stored.clone_like_go()),
                ));
            }
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
            if !tidb_metadef::is_mem_or_sys_db(&schema.to_lowercase()) {
                schema_change_events
                    .push((-1, SchemaChangeEvent::drop_table(stored.clone_like_go())));
            }
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
            set_global_index_version(&info, &mut added);
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
            backfill.push(IndexBackfill {
                table: Box::new(stored.clone_like_go()),
                index: added.clone(),
                use_new_collation,
                add: true,
            });
            diff.action_type = ActionType::ACTION_ADD_INDEX;
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.to_lowercase()) {
                schema_change_events.push((
                    -1,
                    SchemaChangeEvent::add_indexes(
                        info.clone_like_go(),
                        vec![added.read().clone_like_go()],
                        false,
                    ),
                ));
            }
        }
        DdlStatement::ModifyTableComment {
            schema,
            table,
            comment,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let mut info = stored.clone_like_go();
            // Go `onModifyTableComment` sets the field and publishes; it has
            // no early return for an unchanged comment, so neither has this.
            info.comment = comment.clone();
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_MODIFY_TABLE_COMMENT;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::AlterIndexVisibility {
            schema,
            table,
            index,
            invisible,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            // Go `validateAlterIndexVisibility`: the index must exist AND be
            // public, else `ErrKeyNotExists`.
            let found = find_index(stored, index)
                .filter(|candidate| candidate.read().state == tidb_model::SchemaState::PUBLIC);
            let Some(found) = found else {
                return Err(DdlPlanError::KeyNotExists {
                    index: index.clone(),
                    table: table.clone(),
                });
            };
            // Go's early return: a visibility that already matches finishes
            // the job without touching the table, so no schema version is
            // spent on a no-op.
            if found.read().invisible == *invisible {
                return Ok(already(format!(
                    "index `{index}` on `{schema}`.`{table}` is already {}",
                    if *invisible { "invisible" } else { "visible" }
                )));
            }
            let wanted = index.to_lowercase();
            let mut info = stored.clone_like_go();
            // Go `setIndexVisibility` walks EVERY index and sets each one
            // whose name matches, rather than stopping at the first.
            for candidate in info.indices.iter_deref() {
                let mut candidate = candidate.write();
                if candidate.name.lowercase() == wanted {
                    candidate.invisible = *invisible;
                }
            }
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_ALTER_INDEX_VISIBILITY;
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
            backfill.push(IndexBackfill {
                table: Box::new(stored.clone_like_go()),
                index: GoShared::new(dropped),
                use_new_collation,
                add: false,
            });
            diff.action_type = ActionType::ACTION_DROP_INDEX;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::ModifyTableCharsetAndCollate {
            schema,
            table,
            charset,
            collate,
            overwrite_columns,
        } => {
            // Go refuses the form that names neither before it resolves
            // anything.
            if charset.is_empty() && collate.is_empty() {
                return Err(DdlPlanError::Admission(
                    crate::table_info_build::DdlAdmissionError::with_code(
                        1115,
                        "Unknown character set: ''".to_owned(),
                    ),
                ));
            }
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let database = find_database(&catalog, schema)
                .expect("locate_table resolved the database")
                .info
                .clone();
            // Go: an omitted charset keeps the table's own, and an omitted
            // collation becomes that charset's default.
            let to_charset = if charset.is_empty() {
                stored.charset.clone()
            } else {
                charset.clone()
            };
            let (to_charset, to_collate) = crate::table_info_build::resolve_charset_collation(
                Some(to_charset.as_str()),
                if collate.is_empty() {
                    None
                } else {
                    Some(collate.as_str())
                },
                CATALOG_CHARSET,
                CATALOG_COLLATION,
            )
            .map_err(DdlPlanError::Admission)?;

            // Go `checkAlterTableCharset`'s early return: the table already
            // matches AND, when the columns are in scope, every non-binary
            // column matches too.
            if stored.charset == to_charset && stored.collate == to_collate {
                let columns_settled = !*overwrite_columns
                    || stored.columns.iter_deref().all(|column| {
                        let column = column.read();
                        let column_charset = column.field_type.charset_name();
                        column_charset == "binary"
                            || (column_charset == to_charset
                                && column.field_type.collation().name() == to_collate)
                    });
                if columns_settled {
                    return Ok(already(format!(
                        "`{schema}`.`{table}` is already {to_charset}/{to_collate}"
                    )));
                }
            }

            // Go resolves the table's ORIGINAL pair against its database
            // before comparing, so a table that never named one is judged by
            // what it actually inherited.
            let (from_charset, from_collate) = crate::table_info_build::resolve_charset_collation(
                if stored.charset.is_empty() {
                    Some(database.charset.as_str())
                } else {
                    Some(stored.charset.as_str())
                },
                if stored.collate.is_empty() {
                    Some(database.collate.as_str())
                } else {
                    Some(stored.collate.as_str())
                },
                CATALOG_CHARSET,
                CATALOG_COLLATION,
            )
            .map_err(DdlPlanError::Admission)?;
            check_modify_charset_and_collation(
                &to_charset,
                &to_collate,
                &from_charset,
                &from_collate,
                false,
            )?;

            let mut info = stored.clone_like_go();
            if *overwrite_columns {
                // Go checks every column BEFORE it rewrites any of them, so a
                // refusal leaves the table exactly as it was.
                for column in info.columns.iter_deref() {
                    let column = column.read();
                    let name = column.name.original().to_owned();
                    if column.field_type.code() == tidb_datatype::FieldTypeCode::Varchar {
                        check_varchar_field_length(column.field_type.flen(), &name, &to_charset)?;
                    }
                    let column_charset = column.field_type.charset_name().to_owned();
                    if column_charset == "binary" || column_charset.is_empty() {
                        continue;
                    }
                    check_modify_charset_and_collation(
                        &to_charset,
                        &to_collate,
                        &column_charset,
                        column.field_type.collation().name(),
                        is_column_with_index(&name, stored),
                    )?;
                }
            }
            info.charset = to_charset.clone();
            info.collate = to_collate.clone();
            if *overwrite_columns {
                let collation = tidb_datatype::Collation::from_name(&to_collate)
                    .unwrap_or(tidb_datatype::Collation::Binary);
                for column in info.columns.iter_deref() {
                    let mut column = column.write();
                    // Go `field_types.HasCharset`: a type that holds text
                    // takes the new pair, everything else is marked binary.
                    if column.field_type.has_charset() {
                        column.field_type.set_charset_name(to_charset.clone());
                        column.field_type.set_collation(collation);
                    } else {
                        column.field_type.set_charset_name("binary");
                        column
                            .field_type
                            .set_collation(tidb_datatype::Collation::Binary);
                    }
                }
            }
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::ModifySchemaCharsetAndCollate {
            name,
            charset,
            collate,
        } => {
            let Some(database) = find_database(&catalog, name) else {
                return Err(DdlPlanError::UnknownDatabase(name.clone()));
            };
            // Go's early return: the job finishes without touching the
            // database, so no schema version is spent on a no-op.
            if database.info.charset == *charset && database.info.collate == *collate {
                return Ok(already(format!(
                    "database `{name}` is already {charset}/{collate}"
                )));
            }
            let mut info = database.info.clone();
            info.charset = charset.clone();
            info.collate = collate.clone();
            let db_id = info.id;
            let encoded = value::serialize_db_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::database_kv_key(db_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE;
            diff.schema_id = db_id;
        }
        DdlStatement::RenameIndex {
            schema,
            table,
            from,
            to,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            // Go `ValidateRenameIndex`, in its order.
            if find_index(stored, from).is_none() {
                return Err(DdlPlanError::KeyNotExists {
                    index: from.clone(),
                    table: table.clone(),
                });
            }
            // Case-SENSITIVE equality is the no-op test: `inDex` -> `IndEX`
            // is a real rename that only changes the stored spelling.
            if from == to {
                return Ok(already(format!(
                    "index `{from}` on `{schema}`.`{table}` already has that name"
                )));
            }
            let from_lower = from.to_lowercase();
            let to_lower = to.to_lowercase();
            if from_lower != to_lower {
                if let Some(existing) = find_index(stored, to) {
                    return Err(DdlPlanError::DuplicateKeyName(
                        existing.read().name.original().to_owned(),
                    ));
                }
            }
            let mut info = stored.clone_like_go();
            // Go `renameIndexes` walks every index and renames each one whose
            // name matches, rather than stopping at the first.
            //
            // Its two other passes have nothing to rename here: temp indexes
            // exist only during a reorg this node does not run, and the
            // hidden columns of an expression index cannot occur because this
            // node refuses to create or load one.
            for candidate in info.indices.iter_deref() {
                let mut candidate = candidate.write();
                if candidate.name.lowercase() == from_lower {
                    candidate.name = CiString::new(to.clone());
                }
            }
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_RENAME_INDEX;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::SetColumnDefault {
            schema,
            table,
            column,
            default_value,
            context,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let mut info = stored.clone_like_go();
            // Go resolves the target column before it looks at the new
            // default at all, and a non-public one reads as absent.
            let wanted = column.to_lowercase();
            let Some(target) = info.columns.iter_deref().find(|candidate| {
                let candidate = candidate.read();
                candidate.name.lowercase() == wanted
                    && candidate.state == tidb_model::SchemaState::PUBLIC
            }) else {
                return Err(DdlPlanError::UnknownColumn {
                    column: column.clone(),
                    table: table.clone(),
                });
            };
            // Go `ErrInvalidAutoRandom`: the shard bits own the column's
            // values, so a default could never be written.
            if default_value.is_some()
                && info.auto_random_bits > 0
                && target.read().field_type.has_flag(FieldTypeFlags::PRI_KEY)
            {
                return Err(DdlPlanError::InvalidAutoRandom(
                    "auto_random is incompatible with default".to_owned(),
                ));
            }
            {
                let mut target = target.write();
                crate::table_info_build::set_column_default(
                    column,
                    &mut target,
                    default_value.as_deref(),
                    &context.0,
                )
                .map_err(DdlPlanError::Admission)?;
            }
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_SET_DEFAULT_VALUE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::IgnoredTableOption {
            schema,
            table,
            option,
        } => {
            // Go still resolves the table first, so a missing one is an
            // error rather than a silent success.
            locate_table(&catalog, schema, table)?;
            return Ok(already(format!(
                "table option {option} on `{schema}`.`{table}` is accepted and ignored"
            )));
        }
        DdlStatement::OrderByColumns { schema, table } => {
            let (_, stored) = locate_table(&catalog, schema, table)?;
            // Go `GetPkColInfo`: the first column carrying the primary-key
            // flag. Its presence is what makes the ORDER BY meaningless.
            let has_primary_key = stored.columns.iter_deref().any(|column| {
                column
                    .read()
                    .field_type
                    .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)
            });
            let warning = has_primary_key.then(|| {
                format!(
                    "ORDER BY ignored as there is a user-defined clustered index in the table '{table}'"
                )
            });
            return Ok(DdlPlan::AlreadySatisfied {
                detail: format!("ORDER BY on `{schema}`.`{table}` changes nothing"),
                warning,
            });
        }
        DdlStatement::RebaseAutoIncrementId {
            schema,
            table,
            new_base,
            force,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let counter_key = crate::cluster_auto_id::auto_id_key_for(db_id, stored);
            // Go `NextGlobalAutoID`: the stored counter holds the id LAST
            // handed out, so the next one to allocate is one past it. An
            // absent key reads as 0, matching Go's `GetInt64`.
            let stored_counter = match snapshot.get(&counter_key)? {
                Some(value) => value::parse_int_value(&value)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
                None => 0,
            };
            let next_global = (stored_counter as u64).wrapping_add(1);
            let mut new_base = *new_base;
            if !*force {
                // Go `adjustNewBaseToNextGlobalID` compares as UNSIGNED
                // whatever the column's signedness, and never lets the base
                // move backwards: another node's allocator may already have
                // handed out ids past the requested base.
                let adjusted = u64::max(new_base as u64, next_global) as i64;
                if adjusted != new_base {
                    warning = Some(format!(
                        "Can't reset AUTO_INCREMENT to {new_base} without FORCE option, \
                         using {adjusted} instead"
                    ));
                    new_base = adjusted;
                }
            }
            let mut info = stored.clone_like_go();
            info.auto_inc_id = new_base;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(OptimisticMutation::meta_put(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            // "The next value to allocate is `newBase`", so the counter --
            // which holds the last id handed out -- becomes one below it.
            // `Rebase` only ever grows, `ForceRebase` sets exactly; the
            // non-force base was already raised to at least the current
            // counter above, so both reduce to one write here.
            let new_end = new_base.wrapping_sub(1);
            if *force || (new_end as u64) > (stored_counter as u64) {
                writes.push(OptimisticMutation::meta_put(
                    counter_key,
                    value::encode_int_value(new_end),
                )?);
            }
            diff.action_type = ActionType::ACTION_REBASE_AUTO_ID;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
    }

    // Go `assignGIDsForJobs` consumes every schema-object and partition ID
    // first, then assigns `Job.ID` from the final ID in the one
    // `GenGlobalIDs` batch. The notifier uses that job ID as its ordered
    // primary-key prefix.
    let ddl_job_id = global_ids.allocate(1)?[0];
    let mdl_info_update = None;
    if let Some(global_id_mutation) = global_ids.mutation()? {
        writes.push(global_id_mutation);
    }
    append_schema_change_mutations(
        snapshot,
        &catalog,
        ddl_job_id,
        &schema_change_events,
        &mut writes,
    )?;

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
        ddl_job_id,
        mutations: writes,
        schema_version,
        diff,
        created_id,
        backfill,
        exchange_partition_validation,
        check_constraint_validation,
        mdl_info_update,
        exchange_partition_label_swap,
        warning,
        placement_bundles,
        placement_rollback_bundles,
    })))
}

/// Go `types.needReorgToChange` (`field_type.go:1535`): `None` when the new
/// type can replace the old as pure metadata, otherwise Go's own reason for
/// why the stored rows must be rewritten.
///
/// The integer arm is the load-bearing one: Go compares the types' DEFAULT
/// display widths, not the declared ones, so `INT` -> `BIGINT` is a widening
/// that costs nothing while `BIGINT` -> `INT` is a narrowing that does.
fn need_reorg_to_change(old: &FieldType, new: &FieldType) -> Option<String> {
    let (mut old_flen, mut new_flen) = (old.flen(), new.flen());
    if new.code().is_integer_type() && old.code().is_integer_type() {
        old_flen = i64::from(old.code().default_field_length_and_decimal().0);
        new_flen = i64::from(new.code().default_field_length_and_decimal().0);
    }
    if old.code().converts_between_char_and_varchar(new.code()) {
        return Some("conversion between char and varchar string".to_owned());
    }
    if new_flen > 0 && new_flen != old_flen {
        if new_flen < old_flen {
            return Some(format!("length {new_flen} is less than origin {old_flen}"));
        }
        // Go: a binary column pads with \x00, so any length change rewrites.
        let is_binary = |field: &FieldType| {
            field.code() == FieldTypeCode::String && field.collation_name() == "binary"
        };
        if is_binary(old) && is_binary(new) {
            return Some("can't change binary types of different length".to_owned());
        }
    }
    if new.decimal() > 0 && new.decimal() < old.decimal() {
        return Some(format!(
            "decimal {} is less than origin {}",
            new.decimal(),
            old.decimal()
        ));
    }
    if old.has_flag(FieldTypeFlags::UNSIGNED) != new.has_flag(FieldTypeFlags::UNSIGNED) {
        return Some("can't change unsigned integer to signed or vice versa".to_owned());
    }
    None
}

/// Go `types.CheckModifyTypeCompatible` (`field_type.go:1476`), answering
/// only what this node needs: `None` when the modification is metadata-only,
/// otherwise the reason it cannot be.
///
/// Go's `checkTypeChangeSupported` list — the conversions it refuses OUTRIGHT
/// rather than reorganizing — is folded into the same answer here, because
/// this node refuses both classes with the same message; the reason text
/// distinguishes them.
fn modify_type_reorg_reason(old: &FieldType, new: &FieldType) -> Option<String> {
    if old.code() == new.code() {
        return match old.code() {
            // Go compares the element lists; this node has no element list on
            // a stored column to compare, so any enum/set modify is refused.
            FieldTypeCode::Enum | FieldTypeCode::Set => {
                Some("changing enum or set elements".to_owned())
            }
            // Go: a decimal must match in flen, decimal AND sign exactly.
            FieldTypeCode::NewDecimal => {
                if new.flen() != old.flen()
                    || new.decimal() != old.decimal()
                    || old.has_flag(FieldTypeFlags::UNSIGNED)
                        != new.has_flag(FieldTypeFlags::UNSIGNED)
                {
                    Some(format!(
                        "decimal change from decimal({}, {}) to decimal({}, {})",
                        old.flen(),
                        old.decimal(),
                        new.flen(),
                        new.decimal()
                    ))
                } else {
                    need_reorg_to_change(old, new)
                }
            }
            _ => need_reorg_to_change(old, new),
        };
    }
    // Go's different-type arm: only string->string and integer->integer can
    // ever be free, and then only when nothing narrows.
    let string_to_string = old.code().is_string() && new.code().is_string();
    let integer_to_integer = old.code().is_integer_type() && new.code().is_integer_type();
    if string_to_string || integer_to_integer {
        return need_reorg_to_change(old, new);
    }
    Some(format!(
        "type {:?} not match origin {:?}",
        new.code(),
        old.code()
    ))
}

/// Go `collate.CompatibleCollate`: the pairs whose sort orders agree, so
/// rewriting index entries is unnecessary.
fn compatible_collate(one: &str, other: &str) -> bool {
    const GENERAL_CI: [&str; 2] = ["utf8mb4_general_ci", "utf8_general_ci"];
    const BIN: [&str; 3] = ["utf8mb4_bin", "utf8_bin", "latin1_bin"];
    const UNICODE_CI: [&str; 2] = ["utf8mb4_unicode_ci", "utf8_unicode_ci"];
    for family in [&GENERAL_CI[..], &BIN[..], &UNICODE_CI[..]] {
        if family.contains(&one) && family.contains(&other) {
            return true;
        }
    }
    one == other
}

/// Go `checkModifyCharsetAndCollation`.
///
/// TiDB never rewrites stored bytes for a charset change, so it permits only
/// the conversions whose encodings are a superset of the original: utf8 to
/// utf8mb4, latin1 to utf8mb4, and collation-only changes within
/// utf8/utf8mb4/latin1. Everything else is refused rather than silently
/// reinterpreting the rows.
fn check_modify_charset_and_collation(
    to_charset: &str,
    to_collate: &str,
    from_charset: &str,
    from_collate: &str,
    rewrites_collation_data: bool,
) -> Result<(), DdlPlanError> {
    let valid = tidb_datatype::get_collation_by_name(to_collate)
        .is_ok_and(|info| info.charset_name.eq_ignore_ascii_case(to_charset));
    if !valid {
        return Err(unsupported_charset_change(
            1115,
            format!("Unknown character set: '{to_charset}', collation: '{to_collate}'"),
        ));
    }
    if rewrites_collation_data
        && new_collation_enabled()
        && !compatible_collate(from_collate, to_collate)
    {
        return Err(unsupported_charset_change(
            8200,
            format!("Unsupported modifying collation of column '{from_collate}' from '{from_collate}' to '{to_collate}'"),
        ));
    }
    if matches!(
        (from_charset, to_charset),
        ("utf8", "utf8mb4") | ("utf8", "utf8") | ("utf8mb4", "utf8mb4") | ("latin1", "utf8mb4")
    ) {
        return Ok(());
    }
    if to_charset != from_charset {
        return Err(unsupported_charset_change(
            8200,
            format!("Unsupported modify charset from {from_charset} to {to_charset}"),
        ));
    }
    if to_collate != from_collate {
        return Err(unsupported_charset_change(
            8200,
            format!("Unsupported modify charset from {from_charset} to {to_charset}"),
        ));
    }
    Ok(())
}

fn unsupported_charset_change(code: u16, reason: String) -> DdlPlanError {
    DdlPlanError::Admission(crate::table_info_build::DdlAdmissionError::with_code(
        code, reason,
    ))
}

/// Go `types.IsVarcharTooBigFieldLength`: the declared length is in
/// CHARACTERS, so a wider charset lowers the ceiling.
fn check_varchar_field_length(flen: i64, name: &str, to_charset: &str) -> Result<(), DdlPlanError> {
    const MAX_FIELD_VARCHAR_LENGTH: i64 = 65535;
    let Ok(info) = tidb_datatype::get_charset_info(to_charset) else {
        return Ok(());
    };
    let max = MAX_FIELD_VARCHAR_LENGTH / info.maxlen as i64;
    if flen != tidb_datatype::UNSPECIFIED_LENGTH && flen > max {
        return Err(unsupported_charset_change(
            1074,
            format!(
                "Column length too big for column '{name}' (max = {max}); \
                 use BLOB or TEXT instead"
            ),
        ));
    }
    Ok(())
}

/// Go `isColumnWithIndex`: whether any index names this column, which is what
/// makes a collation change require rewriting the stored entries.
fn is_column_with_index(name: &str, table: &TableInfo) -> bool {
    let wanted = name.to_lowercase();
    table.indices.iter_deref().any(|index| {
        index
            .read()
            .columns
            .iter_deref()
            .any(|column| column.read().name.lowercase() == wanted)
    })
}

fn already(detail: String) -> DdlPlan {
    DdlPlan::AlreadySatisfied {
        detail,
        warning: None,
    }
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
            return Err(DdlPlanError::TableNotExists {
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
        return Err(DdlPlanError::TableNotExists {
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

fn append_schema_change_mutations<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    ddl_job_id: i64,
    events: &[(i64, SchemaChangeEvent)],
    writes: &mut Vec<OptimisticMutation>,
) -> Result<(), DdlPlanError> {
    if events.is_empty() {
        return Ok(());
    }
    let (_, table) = catalog
        .find_table("mysql", NOTIFIER_TABLE_NAME)
        .ok_or_else(|| {
            DdlPlanError::Encode(format!("mysql.{} does not exist", NOTIFIER_TABLE_NAME))
        })?;
    let column_id = |name: &str| {
        table
            .cols()
            .iter_deref()
            .find(|column| column.read().name.lowercase() == name)
            .map(|column| column.read().id)
            .ok_or_else(|| {
                DdlPlanError::Encode(format!(
                    "mysql.{} has no column `{name}`",
                    NOTIFIER_TABLE_NAME
                ))
            })
    };
    let ddl_job_id_column = column_id("ddl_job_id")?;
    let sub_job_id_column = column_id("sub_job_id")?;
    let schema_change_column = column_id("schema_change")?;
    let processed_by_column = column_id("processed_by_flag")?;
    let row_id_key = key::auto_table_id_kv_key(tidb_metadef::system::SYSTEM_DATABASE_ID, table.id);
    let mut row_id = snapshot
        .get(&row_id_key)?
        .map(|stored| value::parse_int_value(&stored))
        .transpose()
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?
        .unwrap_or(0);
    for (sub_job_id, event) in events {
        row_id = row_id
            .checked_add(1)
            .ok_or(DdlPlanError::GlobalIdExhausted { wanted: i64::MAX })?;
        let mut values = crate::system_row_write::RowValues::new();
        values.insert(ddl_job_id_column, Datum::Int(ddl_job_id));
        values.insert(sub_job_id_column, Datum::Int(*sub_job_id));
        values.insert(
            schema_change_column,
            Datum::Bytes(
                serde_json::to_vec(event)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            ),
        );
        values.insert(processed_by_column, Datum::UInt(0));
        writes.extend(
            crate::system_row_write::insert_row(table, row_id, &values)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        );
    }
    writes.push(OptimisticMutation::meta_put(
        row_id_key,
        value::encode_int_value(row_id),
    )?);
    Ok(())
}

fn mdl_info_update(catalog: &ClusterCatalog, table_id: i64) -> Result<MdlInfoUpdate, DdlPlanError> {
    let (_, table) = catalog
        .find_table("mysql", "tidb_mdl_info")
        .ok_or_else(|| DdlPlanError::Encode("mysql.tidb_mdl_info does not exist".to_owned()))?;
    Ok(MdlInfoUpdate {
        table: Box::new(table.clone_like_go()),
        table_ids: vec![table_id],
    })
}

fn event_partition_info(definitions: Vec<PartitionDefinition>) -> PartitionInfo {
    PartitionInfo {
        definitions: definitions.into(),
        ..PartitionInfo::default()
    }
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
