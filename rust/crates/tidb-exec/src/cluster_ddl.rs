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
use tidb_error::mysql::FormatArg;
use tidb_error::terror::TerrorError;
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
    GoSharedPointerSlice, GoSharedSlice, HistoryInfo, Job, JobArgsValue, JobState, TraceInfo,
};
use tidb_txnkv::transaction::{BufferMutation, MutationSetError};

use crate::cluster_catalog::{
    load_cluster_catalog, ClusterCatalog, ClusterCatalogError, MetaSnapshot,
};
use crate::ddl_job_submit::GlobalIdAllocator;
use crate::real_tikv_ddl::DdlWarningLevel;
use crate::table_info_build::{
    default_ddl_statement_context, resolve_charset_collation, ClusteredIndexDefMode,
    GENERIC_ERROR_CODE,
};
use tidb_hack::GoToLower;

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
    schema: String,
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
        schema: &str,
        context: &tidb_executor::StmtContext,
    ) -> Result<Self, DdlAdmissionError> {
        let template = crate::table_info_build::build_table_info_in_schema(
            create,
            schema,
            CATALOG_CHARSET,
            CATALOG_COLLATION,
            clustered_index_def_mode(context),
            context,
        )?;
        Ok(Self {
            create: create.clone(),
            schema: schema.to_owned(),
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
        crate::table_info_build::build_table_info_in_schema(
            &self.create,
            &self.schema,
            charset,
            collate,
            clustered_index_def_mode(&self.context),
            &self.context,
        )
    }
}

/// The session's `@@tidb_enable_clustered_index` in the builder's terms.
fn clustered_index_def_mode(context: &tidb_executor::StmtContext) -> ClusteredIndexDefMode {
    match context.clustered_index_def_mode() {
        tidb_vardef::modes::ClusteredIndexDefMode::ON => ClusteredIndexDefMode::On,
        tidb_vardef::modes::ClusteredIndexDefMode::OFF => ClusteredIndexDefMode::Off,
        _ => ClusteredIndexDefMode::IntOnly,
    }
}

/// One column sub-action of a multi-action `ALTER TABLE`.
#[derive(Clone, Debug)]
pub enum AlterColumnAction {    /// `ADD COLUMN`, at its written position.
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
    /// Metadata-only MODIFY/CHANGE, admitted by the standalone lowering.
    Modify {
        /// The replacement column declaration.
        column: Box<tidb_ast::ColumnDef>,
        /// Its requested position.
        position: tidb_ast::ColumnPosition,
        /// The context used to prepare defaults.
        context: DdlStatementContext,
        /// CHANGE locates the original column by this name.
        rename_from: Option<String>,
        /// Go `ModifyColumnArgs.ModifyColumnType` for this sub-job: `None`
        /// until the plan computes it; a failed VARCHAR -> CHAR precheck
        /// plans the statement again with `Reorg` for this sub-job only.
        modify_column_type: Option<tidb_executor::ddl::modify_column_type::ModifyColumnType>,
    },
    /// RENAME COLUMN shares the modify-column job owner.
    Rename {
        /// Original column name.
        from: String,
        /// Replacement name.
        to: String,
    },
    /// `DROP COLUMN`.
    Drop {
        /// Whether a missing column is a no-op.
        if_exists: bool,
        /// The column name as written.
        column: String,
        /// The session's `tidb_allow_remove_auto_inc`.
        allow_remove_auto_inc: bool,
    },
    /// `ADD INDEX`/`ADD KEY`, admitted against the original table metadata.
    AddIndex {
        /// Whether an existing index of the same name is a no-op.
        if_not_exists: bool,
        /// The index, complete except for id and column offsets; an empty
        /// name is anonymous (Go `GetName4AnonymousIndex` names it).
        index: Box<IndexInfo>,
        /// Per key part, the expression of an expression part, whose hidden
        /// column the plan builds (Go `buildHiddenColumnInfoWithCheck`).
        expressions: Vec<Option<tidb_ast::Expr>>,
        /// A partial index's `WHERE` (Go `IndexOption.Condition`).
        condition: Option<tidb_ast::Expr>,
        /// SQL mode and location captured for existing-row evaluation.
        context: DdlStatementContext,
    },
    /// `DROP INDEX`/`DROP KEY`.
    DropIndex {
        /// Whether a missing index is a no-op.
        if_exists: bool,
        /// The index name as written.
        name: String,
    },
    /// `ADD FOREIGN KEY` (Go `CreateForeignKey`).
    AddForeignKey {
        /// The constraint as written.
        definition: Box<tidb_ast::ForeignKeyConstraintDefinition>,
        /// The session's `foreign_key_checks` and the context the existing
        /// rows are checked under.
        context: DdlStatementContext,
    },
    /// `DROP FOREIGN KEY name` (Go `DropForeignKey`).
    DropForeignKey {
        /// The constraint name as written.
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
    /// `DROP PRIMARY KEY` (Go `ActionDropPrimaryKey`).
    DropPrimaryKey,
    /// `RENAME INDEX from TO to` (Go `ActionRenameIndex`).
    RenameIndex {
        /// The index's current name.
        from: String,
        /// The name it takes.
        to: String,
    },
    /// `ALTER INDEX name {VISIBLE | INVISIBLE}` (Go
    /// `ActionAlterIndexVisibility`).
    IndexVisibility {
        /// The index as written.
        name: String,
        /// Whether it becomes invisible.
        invisible: bool,
    },
    /// `ALTER COLUMN name {SET DEFAULT expr | DROP DEFAULT}` (Go
    /// `ActionSetDefaultValue`).
    SetDefault {
        /// The column as written.
        column: String,
        /// The new default; `None` drops it.
        default_value: Option<Box<tidb_ast::Expr>>,
        /// The context the default is evaluated under.
        context: DdlStatementContext,
    },
    /// `AUTO_INCREMENT [=] n` (Go `ActionRebaseAutoID`).
    RebaseAutoId {
        /// The requested next id.
        new_base: i64,
        /// `FORCE`: set the counter exactly, even backwards.
        force: bool,
    },
    /// `COMMENT [=] 'text'` (Go `ActionModifyTableComment`), already
    /// length-checked.
    TableComment {
        /// The comment the table stores.
        comment: String,
    },
    /// The table's `CHARACTER SET` / `COLLATE` pair, or `CONVERT TO` (Go
    /// `ActionModifyTableCharsetAndCollate`).
    TableCharset {
        /// The target charset, empty to keep the table's own.
        charset: String,
        /// The target collation, empty to take the charset's default.
        collate: String,
        /// `CONVERT TO`: also rewrite each column's charset.
        overwrite_columns: bool,
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

impl AlterColumnAction {
    fn statement_context(&self) -> Option<&tidb_executor::StmtContext> {
        match self {
            Self::Add { context, .. }
            | Self::Modify { context, .. }
            | Self::AddIndex { context, .. }
            | Self::AddForeignKey { context, .. }
            | Self::AddCheck { context, .. }
            | Self::SetDefault { context, .. } => Some(&context.0),
            _ => None,
        }
    }
}

impl DdlStatement {
    /// The issuing statement's context the plan-time build appends to.
    ///
    /// Go's executor raises plan-time warnings (an over-long comment in
    /// non-strict mode, a VARCHAR auto-converted to TEXT) directly on the
    /// session's statement context, so the caller drains this after the
    /// statement ran. Every context a multi-schema change carries is the
    /// same statement's.
    pub fn statement_context(&self) -> Option<&tidb_executor::StmtContext> {
        match self {
            Self::AddColumn { context, .. }
            | Self::AddCheckConstraint { context, .. }
            | Self::DropCheckConstraint { context, .. }
            | Self::AlterCheckConstraint { context, .. }
            | Self::ModifyColumn { context, .. }
            | Self::AddForeignKey { context, .. }
            | Self::CreateIndex { context, .. }
            | Self::SetColumnDefault { context, .. }
            | Self::ReorganizePartitions { context, .. } => Some(&context.0),
            Self::MultiSchemaChange { actions, .. } => actions
                .iter()
                .find_map(AlterColumnAction::statement_context),
            _ => None,
        }
    }
}

/// One catalog change this node knows how to perform.
#[derive(Clone, Debug)]
pub enum DdlStatement {    /// `CREATE DATABASE [IF NOT EXISTS] name`.
    CreateDatabase {
        /// The database name as written.
        name: String,
        /// Whether an existing database is a no-op rather than an error.
        if_not_exists: bool,
        /// The resolved charset persisted in `DBInfo`.
        charset: String,
        /// The resolved collation persisted in `DBInfo`.
        collate: String,
        /// The `PLACEMENT POLICY` written, resolved against the stored
        /// policies when planned (Go `handleDatabasePlacement`).
        placement_policy: Option<String>,
    },
    /// `ALTER DATABASE ... PLACEMENT POLICY`, Go's
    /// `ActionModifySchemaDefaultPlacement`.
    ModifySchemaDefaultPlacement {
        /// The database name.
        name: String,
        /// The policy written (`default` removes the placement).
        placement_policy: String,
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
    /// `ALTER TABLE ... SET TIFLASH REPLICA n [LOCATION LABELS '...']`, and
    /// `SET TIFLASH REPLICA 0` as the reset. Go
    /// `ast.AlterTableSetTiFlashReplica` -> the `ActionSetTiFlashReplica` job
    /// (pkg/ddl/executor.go:625), which only persists the replica metadata:
    /// PD placement rules and availability are the replica manager's business
    /// (pkg/domain/infosync/tiflash_manager.go).
    SetTiFlashReplica {
        /// Database containing the table.
        schema: String,
        /// Table whose replica metadata is set.
        table: String,
        /// Requested replica count; zero clears the replica entirely.
        count: u64,
        /// Placement labels, in written order.
        labels: Vec<String>,
        /// Go SetTiFlashReplicaArgs.ResetAvailable, used by restore repair.
        reset_available: bool,
    },
    /// Go `AlterTableSetRegionSplitPolicy` -> the
    /// `ActionAlterTableSetRegionSplitPolicy` job (`onAlterTableSetRegionSplitPolicy`):
    /// the normalized policy lands on the table or on the named index.
    AlterTableSetRegionSplitPolicy {
        /// Database containing the table.
        schema: String,
        /// Table the policy is set on.
        table: String,
        /// The keyspace the clause names.
        target: tidb_ast::SplitTarget,
        /// The clause's bounds and region count.
        option: tidb_ast::SplitOption,
    },
    /// Go `UpdateTableReplicaInfo` -> the `ActionUpdateTiFlashReplicaStatus`
    /// job: the replica manager's poller flips `Available` once TiFlash
    /// reports every region's learner peer in place. Resolved by PHYSICAL
    /// table id, never by SQL name (Go's API takes the id).
    UpdateTiFlashReplicaStatus {
        /// The table (logical) id whose replica availability changes.
        table_id: i64,
        /// Go `Available`.
        available: bool,
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
        /// The action as written, which a HASH/KEY table reorganizes
        /// instead (Go `hashPartitionManagement`).
        action: Box<tidb_ast::AlterPartitionAction>,
        /// The context a HASH/KEY reorganization runs under.
        context: DdlStatementContext,
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
        /// Go `vardef.EnableCheckConstraint` when the statement ran: whether
        /// the validation also applies each table's writable CHECK
        /// constraints to the other's rows.
        check_constraints: bool,
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
        /// The session's `tidb_allow_remove_auto_inc`.
        allow_remove_auto_inc: bool,
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
        /// Go's `ast.ColumnPosition`: a MODIFY/CHANGE may also MOVE the
        /// column, and Go locates the destination against the column's
        /// CURRENT offset (unlike ADD, which appends first).
        position: tidb_ast::ColumnPosition,
        /// The statement context the plan-time build resolves under.
        context: DdlStatementContext,
        /// `CHANGE COLUMN`'s old name; `None` is a plain MODIFY. Go gives
        /// both spellings the one ActionModifyColumn job, the rename riding
        /// `renameColumnTo` over the column and every index column naming it.
        rename_from: Option<String>,
        /// Go `ModifyColumnArgs.ModifyColumnType`: `None` until the plan
        /// computes it (`getModifyColumnType`); a failed VARCHAR -> CHAR
        /// precheck plans the statement again with `Reorg`.
        modify_column_type: Option<tidb_executor::ddl::modify_column_type::ModifyColumnType>,
    },
    /// `ALTER TABLE ... ADD FOREIGN KEY` (Go `CreateForeignKey`).
    AddForeignKey {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The constraint as written.
        definition: Box<tidb_ast::ForeignKeyConstraintDefinition>,
        /// The session's `foreign_key_checks` and the context the existing
        /// rows are checked under.
        context: DdlStatementContext,
    },
    /// `ALTER TABLE ... CACHE` / `NOCACHE` (Go `AlterTableCache` /
    /// `AlterTableNoCache`).
    AlterTableCache {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// CACHE, or NOCACHE.
        cache: bool,
    },
    /// `ALTER TABLE ... DROP FOREIGN KEY name` (Go `DropForeignKey`).
    DropForeignKey {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The constraint name as written.
        name: String,
    },
    /// Go `onCreateForeignKey` rolling back after `checkForeignKeyConstrain`
    /// refused the existing rows: `dropForeignKey` removes the key, and the
    /// rollback of every earlier sub-job of the same ALTER (the key's
    /// `createIndex` among them) what it added, but the ids those jobs
    /// allocated stay taken.
    AddForeignKeyRollback {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// `MaxColumnID` when the key's job ran.
        max_column_id: i64,
        /// `MaxIndexID` when the key's job ran.
        max_index_id: i64,
        /// `MaxForeignKeyID` after `allocateFKIndexID`.
        max_foreign_key_id: i64,
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
    /// A supported multi-action ALTER. Admission uses the original table;
    /// prepared sub-actions then fold over evolving metadata in one catalog
    /// transaction. Go's online multi-schema worker remains a separate gap.
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
    /// `DROP TABLE [IF EXISTS] [schema.]table`.
    DropTable {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// Whether a missing table is a no-op rather than an error.
        if_exists: bool,
    },
    /// `REORGANIZE PARTITION`, `PARTITION BY` on an existing table and
    /// `REMOVE PARTITIONING`: Go's `onReorganizePartition`, its rows moved to
    /// the new physical tables in the DDL transaction.
    ReorganizePartitions {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The partition action as written.
        action: Box<tidb_ast::AlterPartitionAction>,
        /// The context the new definitions are built and the rows moved
        /// under.
        context: DdlStatementContext,
    },
    /// `CREATE SEQUENCE [IF NOT EXISTS]`, Go's `ActionCreateSequence`: a
    /// `TableInfo` carrying `Sequence`, built before the job
    /// (`buildSequenceInfo`, `BuildTableInfo`).
    CreateSequence {
        /// The resolved database name.
        schema: String,
        /// The sequence name as written.
        name: String,
        /// Whether an existing name is a no-op rather than an error.
        if_not_exists: bool,
        /// The sequence's `TableInfo`, its id still unallocated.
        info: Box<TableInfo>,
    },
    /// `ALTER SEQUENCE [IF EXISTS]`, Go's `ActionAlterSequence`.
    AlterSequence {
        /// The resolved database name.
        schema: String,
        /// The sequence name as written.
        name: String,
        /// Whether a missing sequence is a note rather than an error.
        if_exists: bool,
        /// The options as written, applied over the stored ones.
        options: Vec<tidb_ast::SequenceOption>,
    },
    /// One `DROP SEQUENCE` target, Go's `ActionDropSequence`.
    DropSequence {
        /// The resolved database name.
        schema: String,
        /// The sequence name as written.
        name: String,
    },
    /// `DROP SEQUENCE [IF EXISTS] a, b`: ordered targets the DDL execution
    /// owner publishes one by one, as Go's `dropTableObject` does.
    DropSequences {
        /// Each `(schema, name)` in written order.
        names: Vec<(String, String)>,
        /// Whether missing sequences demote to notes rather than the error.
        if_exists: bool,
    },
    /// Ordered DROP targets. The DDL execution owner publishes each target
    /// independently and defers missing-name completion until the list ends.
    DropTables {
        /// Each `(schema, name)` in written order.
        names: Vec<(String, String)>,
        /// Whether missing tables demote to notes rather than the error.
        if_exists: bool,
    },
    /// go runs a no-op DDL job for the option-only ALTER forms
    /// (ALGORITHM / LOCK / WITH/WITHOUT VALIDATION); the optional warning is
    /// its `job.Warning` (8200, WITH VALIDATION).
    AcceptedNoOp {
        /// The table the statement names.
        schema: String,
        /// The table name.
        table: String,
        /// go `job.Warning`, when the option raised one.
        warning: Option<(u16, String)>,
    },
    /// `RENAME TABLE [schema.]from TO [schema.]to`, including the single
    /// action form of `ALTER TABLE ... RENAME TO`.
    RenameTable {
        /// ALTER permits an identity rename after resolving the source;
        /// RENAME TABLE reports the existing destination instead.
        is_alter: bool,
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
        /// table; an empty name is anonymous (Go `GetName4AnonymousIndex`
        /// names it against the stored table).
        index: Box<IndexInfo>,
        /// Per key part, the expression of an expression part, whose hidden
        /// column the plan builds (Go `buildHiddenColumnInfoWithCheck`).
        expressions: Vec<Option<tidb_ast::Expr>>,
        /// A partial index's `WHERE` (Go `IndexOption.Condition`).
        condition: Option<tidb_ast::Expr>,
        /// SQL mode and location captured for existing-row evaluation.
        context: DdlStatementContext,
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
    /// `ALTER TABLE ... ENGINE_ATTRIBUTE = '...'` or its `STORAGE_CLASS`
    /// sugar, Go's `ActionModifyEngineAttribute`. Metadata only.
    ModifyEngineAttribute {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The validated attribute JSON
        /// (`GetEngineAttributeFromStorageClassTableOptions`).
        engine_attribute: String,
    },
    /// `LOCK TABLES`, Go's `ActionLockTable` (`onLockTables`): the
    /// session's earlier locks released, then each named table locked.
    LockTables {
        /// Go `LockTablesArgs.LockTables`.
        lock_tables: Vec<tidb_model::TableLockTpInfo>,
        /// Go `LockTablesArgs.UnlockTables`: the session's current locks.
        unlock_tables: Vec<tidb_model::TableLockTpInfo>,
        /// Go `LockTablesArgs.SessionInfo`.
        session: tidb_model::SessionInfo,
    },
    /// `UNLOCK TABLES` and a closing session's release, Go's
    /// `ActionUnlockTable` (`onUnlockTables`).
    UnlockTables {
        /// Go `LockTablesArgs.UnlockTables`.
        unlock_tables: Vec<tidb_model::TableLockTpInfo>,
        /// Go `LockTablesArgs.SessionInfo`.
        session: tidb_model::SessionInfo,
        /// Go `LockTablesArgs.IsCleanup`: `ADMIN CLEANUP TABLE LOCK` drops
        /// every holder's lock.
        is_cleanup: bool,
    },
    /// `ALTER TABLE ... TTL = ... / TTL_ENABLE = ... / TTL_JOB_INTERVAL =
    /// ...` in one spec, Go's `AlterTableTTLInfoOrEnable` /
    /// `ActionAlterTTLInfo`.
    AlterTtl {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The `TTL=` definition, its enable flag and interval already folded
        /// in (`getTTLInfoInOptions`).
        ttl_info: Option<Box<tidb_model::TTLInfo>>,
        /// `TTL_ENABLE`, when written.
        enable: Option<bool>,
        /// `TTL_JOB_INTERVAL`, when written.
        job_interval: Option<String>,
    },
    /// `ALTER TABLE ... REMOVE TTL`, Go's `ActionAlterTTLRemove`.
    RemoveTtl {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
    },
    /// `ALTER TABLE ... PLACEMENT POLICY [=] name`, Go's
    /// `ActionAlterTablePlacement`.
    AlterTablePlacement {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The policy as written; `DEFAULT` clears the table's.
        policy: String,
    },
    /// `ALTER TABLE ... SHARD_ROW_ID_BITS = n`, Go's `ActionShardRowID`.
    /// Metadata only.
    ShardRowId {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The bit count, already clamped to `MaxShardRowIDBits`.
        bits: u64,
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
    /// with: `ENGINE`, `ROW_FORMAT`. Their cases in `executor.go` are empty,
    /// so the statement succeeds and spends no schema version.
    /// (`ENGINE_ATTRIBUTE` and `STORAGE_CLASS` also have empty cases, but Go
    /// publishes [`Self::ModifyEngineAttribute`] for them after the loop.) Refusing them instead would reject the
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
            // Resolved when the statement is planned.
            DatabaseOption::PlacementPolicy(_) => {}
            other => {
                return Err(DdlAdmissionError::with_code(
                    GENERIC_ERROR_CODE,
                    format!("CREATE DATABASE option {other:?} is not supported by this node"),
                ))
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
            let placement_policy = options.iter().rev().find_map(|option| match option {
                DatabaseOption::PlacementPolicy(policy) => Some(policy.clone()),
                _ => None,
            });
            Ok(Some(DdlStatement::CreateDatabase {
                name: name.clone(),
                if_not_exists: *if_not_exists,
                charset,
                collate,
                placement_policy,
            }))
        }
        DdlStmt::AlterDatabase { name, options } => {
            let mut charset = None;
            let mut collate = None;
            let mut placement_policy = None;
            for option in options {
                match option {
                    DatabaseOption::CharacterSet(value) => charset = Some(value.as_str()),
                    DatabaseOption::Collate(value) => collate = Some(value.as_str()),
                    DatabaseOption::PlacementPolicy(value) => placement_policy = Some(value),
                    other => {
                        return Err(DdlAdmissionError::with_code(
                            GENERIC_ERROR_CODE,
                            format!(
                                "ALTER DATABASE option {other:?} is not supported by this node"
                            ),
                        ))
                    }
                }
            }
            let name = name.clone().unwrap_or_else(|| default_schema.to_owned());
            if let Some(policy) = placement_policy {
                // Go submits the charset job and then the placement job; this
                // node publishes one change per statement.
                if charset.is_some() || collate.is_some() {
                    return Err(DdlAdmissionError::with_code(
                        GENERIC_ERROR_CODE,
                        "this node runs ALTER DATABASE charset and placement changes as \
                         separate statements"
                            .to_owned(),
                    ));
                }
                return Ok(Some(DdlStatement::ModifySchemaDefaultPlacement {
                    name,
                    placement_policy: policy.clone(),
                }));
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
                name,
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
        DdlStmt::CreateIndex(create) => {
            lower_create_index(create, default_schema, false, context).map(Some)
        }
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
        DdlStmt::CreateSequence(create) => {
            let (schema, name) = split_name(&create.name, default_schema, "sequence")?;
            let (info, comment) = tidb_executor::build_create_sequence(create, &schema, &name)
                .map_err(|error| {
                    let error = error.to_mysql_error();
                    DdlAdmissionError::with_code(error.code, error.message)
                })?;
            let info = build_sequence_table_info(&name, info, comment);
            Ok(Some(DdlStatement::CreateSequence {
                schema,
                name,
                if_not_exists: create.if_not_exists,
                info: Box::new(info),
            }))
        }
        DdlStmt::AlterSequence(alter) => {
            let (schema, name) = split_name(&alter.name, default_schema, "sequence")?;
            Ok(Some(DdlStatement::AlterSequence {
                schema,
                name,
                if_exists: alter.if_exists,
                options: alter.options.clone(),
            }))
        }
        DdlStmt::DropSequence(drop) => {
            let mut names = Vec::with_capacity(drop.names.len());
            for path in &drop.names {
                names.push(split_name(path, default_schema, "sequence")?);
            }
            Ok(Some(DdlStatement::DropSequences {
                names,
                if_exists: drop.if_exists,
            }))
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
        DdlPlanError::Admission(DdlAdmissionError::with_code(
            GENERIC_ERROR_CODE,
            format!("building placement rules: {error}"),
        ))
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
/// Go `checkAndNormalizePlacementPolicy` against the stored policies:
/// `default` removes the placement, any other name must be a policy.
fn check_and_normalize_placement_policy<S: MetaSnapshot>(
    snapshot: &mut S,
    name: &str,
) -> Result<Option<tidb_model::GoShared<tidb_model::PolicyRefInfo>>, DdlPlanError> {
    if name.eq_ignore_ascii_case("default") {
        return Ok(None);
    }
    let Some(policy) = find_policy(snapshot, name)? else {
        return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
            8239,
            format!("Unknown placement policy '{name}'"),
        )));
    };
    Ok(Some(tidb_model::GoShared::new(tidb_model::PolicyRefInfo {
        id: policy.id,
        name: CiString::new(name),
    })))
}

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
        // every action has an admitted job the catalog transaction owns.
        let mut actions = Vec::with_capacity(alter.actions.len());
        // `ALGORITHM=` records a request the executor would refuse; the 1846
        // warning is raised only once an operation is known to run (see the
        // arm below).
        let mut pending_algorithm: Option<(&'static str, &'static str)> = None;
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
                tidb_ast::AlterTableAction::ChangeColumn { .. }
                | tidb_ast::AlterTableAction::ModifyColumn { .. }
                | tidb_ast::AlterTableAction::RenameColumn(_) => {
                    let mut single = alter.clone();
                    single.actions = vec![action.clone()];
                    match lower_alter_table_catalog(&single, default_schema, context)? {
                        Some(DdlStatement::ModifyColumn {
                            column,
                            position,
                            context,
                            rename_from,
                            modify_column_type,
                            ..
                        }) => actions.push(AlterColumnAction::Modify {
                            column,
                            position,
                            context,
                            rename_from,
                            modify_column_type,
                        }),
                        Some(DdlStatement::RenameColumn { from, to, .. }) => {
                            actions.push(AlterColumnAction::Rename { from, to })
                        }
                        _ => return Ok(None),
                    }
                }
                // Go `fillMultiSchemaInfo`'s index, default and table-option
                // sub-jobs, each lowered exactly as its single spelling is.
                tidb_ast::AlterTableAction::DropPrimaryKey(_)
                | tidb_ast::AlterTableAction::RenameIndex(_)
                | tidb_ast::AlterTableAction::AlterIndexVisibility(_)
                | tidb_ast::AlterTableAction::AlterColumnDefault(_)
                | tidb_ast::AlterTableAction::ConvertCharacterSet { .. } => {
                    let mut single = alter.clone();
                    single.actions = vec![action.clone()];
                    match lower_alter_table_catalog(&single, default_schema, context)? {
                        Some(DdlStatement::DropPrimaryKey { .. }) => {
                            actions.push(AlterColumnAction::DropPrimaryKey)
                        }
                        Some(DdlStatement::RenameIndex { from, to, .. }) => {
                            actions.push(AlterColumnAction::RenameIndex { from, to })
                        }
                        Some(DdlStatement::AlterIndexVisibility {
                            index, invisible, ..
                        }) => actions.push(AlterColumnAction::IndexVisibility {
                            name: index,
                            invisible,
                        }),
                        Some(DdlStatement::SetColumnDefault {
                            column,
                            default_value,
                            context,
                            ..
                        }) => actions.push(AlterColumnAction::SetDefault {
                            column,
                            default_value,
                            context,
                        }),
                        Some(DdlStatement::ModifyTableCharsetAndCollate {
                            charset,
                            collate,
                            overwrite_columns,
                            ..
                        }) => actions.push(AlterColumnAction::TableCharset {
                            charset,
                            collate,
                            overwrite_columns,
                        }),
                        _ => return Ok(None),
                    }
                }
                // Go `AlterTable` walks one table-option spec's options in
                // order, one sub-job each, and handles the charset / collate
                // pair once (`handledCharsetOrCollate`) with the last of each
                // winning.
                // Go `fillMultiSchemaInfo` has no TTL sub-job.
                tidb_ast::AlterTableAction::SetTableOptions { options }
                    if options.iter().any(is_ttl_table_option) =>
                {
                    return Err(DdlAdmissionError::with_code(
                        tidb_error::tidb::errcode::ErrUnsupportedDDLOperation,
                        "Unsupported multi schema change for alter table ttl",
                    ));
                }
                tidb_ast::AlterTableAction::RemoveTtl(_) => {
                    return Err(DdlAdmissionError::with_code(
                        tidb_error::tidb::errcode::ErrUnsupportedDDLOperation,
                        "Unsupported multi schema change for alter table no_ttl",
                    ));
                }
                tidb_ast::AlterTableAction::SetTableOptions { options } => {
                    let mut charset_handled = false;
                    for option in options {
                        match option {
                            tidb_ast::TableOption::CharacterSet(_)
                            | tidb_ast::TableOption::Collate(_) => {
                                if charset_handled {
                                    continue;
                                }
                                charset_handled = true;
                                let mut charset = String::new();
                                let mut collate = String::new();
                                for option in options {
                                    match option {
                                        tidb_ast::TableOption::CharacterSet(value) => {
                                            charset = value.clone()
                                        }
                                        tidb_ast::TableOption::Collate(value) => {
                                            collate = value.clone()
                                        }
                                        _ => {}
                                    }
                                }
                                actions.push(AlterColumnAction::TableCharset {
                                    charset,
                                    collate,
                                    overwrite_columns: false,
                                });
                            }
                            tidb_ast::TableOption::AutoIncrement(_)
                            | tidb_ast::TableOption::ForceAutoIncrement(_)
                            | tidb_ast::TableOption::Comment(_) => {
                                let mut single = alter.clone();
                                single.actions =
                                    vec![tidb_ast::AlterTableAction::SetTableOptions {
                                        options: vec![option.clone()],
                                    }];
                                match lower_alter_table_catalog(&single, default_schema, context)? {
                                    Some(DdlStatement::RebaseAutoIncrementId {
                                        new_base,
                                        force,
                                        ..
                                    }) => actions
                                        .push(AlterColumnAction::RebaseAutoId { new_base, force }),
                                    Some(DdlStatement::ModifyTableComment { comment, .. }) => {
                                        actions.push(AlterColumnAction::TableComment { comment })
                                    }
                                    _ => return Ok(None),
                                }
                            }
                            _ => return Ok(None),
                        }
                    }
                }
                tidb_ast::AlterTableAction::DropColumn { if_exists, name } => {
                    actions.push(AlterColumnAction::Drop {
                        if_exists: *if_exists,
                        column: name.clone(),
                        allow_remove_auto_inc: context.allow_remove_auto_inc(),
                    });
                }
                tidb_ast::AlterTableAction::AddIndexConstraint(index) => {
                    // The standalone lowering owns the validation; reuse it
                    // whole so the bundled and single spellings cannot drift.
                    match lower_alter_add_index(alter, index, default_schema, context)? {
                        DdlStatement::CreateIndex {
                            if_not_exists,
                            index,
                            expressions,
                            condition,
                            context,
                            ..
                        } => actions.push(AlterColumnAction::AddIndex {
                            if_not_exists,
                            index,
                            expressions,
                            condition,
                            context,
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
                tidb_ast::AlterTableAction::AddForeignKey(definition) => {
                    actions.push(AlterColumnAction::AddForeignKey {
                        definition: Box::new(definition.clone()),
                        context: DdlStatementContext(context.clone()),
                    });
                }
                tidb_ast::AlterTableAction::DropForeignKey(drop) => {
                    actions.push(AlterColumnAction::DropForeignKey {
                        name: drop.name.clone(),
                    });
                }
                tidb_ast::AlterTableAction::Lock(_) => {
                    // go: `LOCK=NONE` is the supported lock for the column
                    // operations and selects nothing observable; the option
                    // is skipped without a warning (oracle: ADD COLUMN f INT,
                    // LOCK=NONE answers a plain (ok)).
                    continue;
                }
                tidb_ast::AlterTableAction::Algorithm(algorithm) => {
                    // go `getProperAlgorithm` + `ErrAlterOperationNotSupported`:
                    // the requested algorithm selects nothing here — the
                    // operation runs with its own default algorithm and the
                    // refusal lands as an ERROR-level 1846 warning (oracle:
                    // ALTER TABLE x1 ADD COLUMN d INT, ALGORITHM=INPLACE
                    // answers (ok) with `ALGORITHM=INPLACE is not supported.
                    // Reason: Cannot alter table by INPLACE. Try
                    // ALGORITHM=INSTANT.`). The column-operation descriptor's
                    // default is INSTANT, which is why INSTANT itself warns
                    // nothing.
                    //
                    // The warning is ALSO conditional on there being an
                    // operation to run: `ALTER TABLE t ALGORITHM=INPLACE,
                    // LOCK=NONE` on an unchanged table answers a plain (ok)
                    // with NO warning, because go never reaches
                    // `getProperAlgorithm` for a job with no actions. Record
                    // the request and decide after the loop, when `actions`
                    // is known.
                    pending_algorithm = match algorithm {
                        tidb_ast::AlterTableAlgorithm::Inplace => Some(("INPLACE", "INSTANT")),
                        tidb_ast::AlterTableAlgorithm::Copy => Some(("COPY", "INSTANT")),
                        _ => {
                            // DEFAULT and INSTANT are the operation's own
                            // default — no refusal.
                            continue;
                        }
                    };
                }
                _ => {
                    actions.clear();
                    break;
                }
            }
        }
        if !actions.is_empty() {
            if let Some((specify, def_algorithm)) = pending_algorithm {
                context.append_error_parts(
                    1846,
                    &format!(
                        "ALGORITHM={specify} is not supported. Reason: Cannot alter table by {specify}. Try ALGORITHM={def_algorithm}."
                    ),
                );
            }
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
        // go runs a no-op job when every specification is one of the
        // option-only forms (ALGORITHM / LOCK / WITH/WITHOUT VALIDATION);
        // WITH VALIDATION additionally raises job warning 8200.
        if alter.actions.iter().all(|action| {
            matches!(
                action,
                tidb_ast::AlterTableAction::Algorithm(_)
                    | tidb_ast::AlterTableAction::Lock(_)
                    | tidb_ast::AlterTableAction::WithValidation
                    | tidb_ast::AlterTableAction::WithoutValidation
            )
        }) {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            // go raises 8200 for BOTH validation forms, each naming its own
            // spelling (oracle g-alter: `ALTER TABLE ab3 WITHOUT VALIDATION`
            // answers `ALTER TABLE WITHOUT VALIDATION is currently
            // unsupported`, and the WITH form names WITH).
            let warning = alter.actions.iter().find_map(|action| match action {
                tidb_ast::AlterTableAction::WithValidation => Some((
                    8200_u16,
                    "ALTER TABLE WITH VALIDATION is currently unsupported".to_owned(),
                )),
                tidb_ast::AlterTableAction::WithoutValidation => Some((
                    8200_u16,
                    "ALTER TABLE WITHOUT VALIDATION is currently unsupported".to_owned(),
                )),
                _ => None,
            });
            return Ok(Some(DdlStatement::AcceptedNoOp {
                schema,
                table,
                warning,
            }));
        }
        return Ok(None);
    };

    match action {
        tidb_ast::AlterTableAction::AddIndexConstraint(index) => {
            lower_alter_add_index(alter, index, default_schema, context).map(Some)
        }
        tidb_ast::AlterTableAction::AddForeignKey(definition) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AddForeignKey {
                schema,
                table,
                definition: Box::new(definition.clone()),
                context: DdlStatementContext(context.clone()),
            }))
        }
        tidb_ast::AlterTableAction::Cache(mode) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AlterTableCache {
                schema,
                table,
                cache: *mode == tidb_ast::AlterTableCacheMode::Cache,
            }))
        }
        tidb_ast::AlterTableAction::DropForeignKey(drop) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::DropForeignKey {
                schema,
                table,
                name: drop.name.clone(),
            }))
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
            // Retain identity ALTER renames so planning resolves the source before
            // returning a no-op, as Go does.
            Ok(Some(DdlStatement::RenameTable {
                is_alter: true,
                from_schema,
                from_table,
                to_schema,
                to_table,
            }))
        }
        // Go `AlterTableSetRegionSplitPolicy`.
        tidb_ast::AlterTableAction::SplitRegion { target, option } => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AlterTableSetRegionSplitPolicy {
                schema,
                table,
                target: target.clone(),
                option: option.clone(),
            }))
        }
        tidb_ast::AlterTableAction::SetTiFlashReplica {
            hypo,
            count,
            labels,
        } => {
            if *hypo {
                // Go `setHypoTiFlashReplica` keeps the replica in
                // `SessionVars`; the session answers a lone `SET HYPO TIFLASH
                // REPLICA`, and a multi-action ALTER has no session arm yet.
                return Err(DdlAdmissionError::unsupported(
                    "SET HYPO TIFLASH REPLICA among other ALTER TABLE actions is not supported yet",
                ));
            }
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::SetTiFlashReplica {
                schema,
                table,
                count: *count,
                labels: labels.clone(),
                reset_available: false,
            }))
        }
        // Go `AlterTable` handles a spec's TTL, TTL_ENABLE and
        // TTL_JOB_INTERVAL once, as one `AlterTableTTLInfoOrEnable`.
        tidb_ast::AlterTableAction::SetTableOptions { options }
            if !options.is_empty() && options.iter().all(is_ttl_table_option) =>
        {
            let ttl_info = tidb_executor::ddl::ttl_info_from_options(options).map_err(|error| {
                let error = error.to_mysql_error();
                DdlAdmissionError::with_code(error.code, error.message)
            })?;
            let enable = options.iter().rev().find_map(|option| match option {
                tidb_ast::TableOption::TtlEnable(enabled) => Some(*enabled),
                _ => None,
            });
            let job_interval = options.iter().rev().find_map(|option| match option {
                tidb_ast::TableOption::TtlJobInterval(interval) => Some(interval.clone()),
                _ => None,
            });
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AlterTtl {
                schema,
                table,
                ttl_info: ttl_info.map(Box::new),
                enable,
                job_interval,
            }))
        }
        tidb_ast::AlterTableAction::RemoveTtl(_) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::RemoveTtl { schema, table }))
        }
        tidb_ast::AlterTableAction::SetTableOptions { options } => {
            let [option] = options.as_slice() else {
                return Ok(None);
            };
            if let tidb_ast::TableOption::AutoIdCache(value) = option {
                let new_cache = value.parse::<u64>().map_err(|_| {
                    DdlAdmissionError::with_code(
                        GENERIC_ERROR_CODE,
                        "AUTO_ID_CACHE needs an integer value",
                    )
                })?;
                if new_cache > i64::MAX as u64 {
                    return Err(DdlAdmissionError::with_code(
                        GENERIC_ERROR_CODE,
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
            if let tidb_ast::TableOption::EngineAttribute(_)
            | tidb_ast::TableOption::StorageClass(_) = option
            {
                let engine_attribute =
                    tidb_executor::ddl::storage_class::engine_attribute_from_table_options(options)
                        .map_err(|error| {
                            let error = error.to_mysql_error();
                            DdlAdmissionError::with_code(error.code, error.message)
                        })?
                        .expect("the one option is a storage-class option");
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return Ok(Some(DdlStatement::ModifyEngineAttribute {
                    schema,
                    table,
                    engine_attribute,
                }));
            }
            if let tidb_ast::TableOption::PlacementPolicy(policy) = option {
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return Ok(Some(DdlStatement::AlterTablePlacement {
                    schema,
                    table,
                    policy: policy.clone(),
                }));
            }
            let ignored = match option {
                tidb_ast::TableOption::Engine(_) => Some("ENGINE"),
                tidb_ast::TableOption::RowFormat(_) => Some("ROW_FORMAT"),
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
                let new_base = value.parse::<u64>().map_err(|_| {
                    DdlAdmissionError::with_code(
                        GENERIC_ERROR_CODE,
                        "AUTO_INCREMENT needs an integer value",
                    )
                })? as i64;
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
                let comment = tidb_executor::ddl::validate_comment_length(
                    comment,
                    &tidb_hack::go_to_lower(&table),
                    tidb_executor::ddl::CommentOwner::Table,
                    context,
                )
                .map_err(|error| {
                    let error = error.to_mysql_error();
                    DdlAdmissionError::with_code(error.code, error.message)
                })?;
                return Ok(Some(DdlStatement::ModifyTableComment {
                    schema,
                    table,
                    comment,
                }));
            }
            // Go `AlterTable` clamps the value to `MaxShardRowIDBits` before
            // `ShardRowID`.
            if let tidb_ast::TableOption::ShardRowIdBits(value) = option {
                let bits = value.parse::<u64>().map_err(|_| {
                    DdlAdmissionError::with_code(
                        GENERIC_ERROR_CODE,
                        "SHARD_ROW_ID_BITS needs an integer value",
                    )
                })?;
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return Ok(Some(DdlStatement::ShardRowId {
                    schema,
                    table,
                    bits: bits.min(tidb_vardef::tidb_vars::MAX_SHARD_ROW_ID_BITS),
                }));
            }
            let (value, force) = match option {
                tidb_ast::TableOption::AutoRandomBase(value) => (value, false),
                tidb_ast::TableOption::ForceAutoRandomBase(value) => (value, true),
                _ => return Ok(None),
            };
            let next = value.parse::<u64>().map_err(|_| {
                DdlAdmissionError::with_code(
                    GENERIC_ERROR_CODE,
                    "AUTO_RANDOM_BASE needs an integer value",
                )
            })? as i64;
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
                // No AUTO_RANDOM: the ordinary MODIFY. Its options are
                // admitted by the session (Go `GetModifiableColumnJob`) and
                // built by the plan.
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return Ok(Some(DdlStatement::ModifyColumn {
                    schema,
                    table,
                    column: Box::new(column.clone()),
                    position: position.clone(),
                    context: DdlStatementContext(context.clone()),
                    rename_from: None,
                    modify_column_type: None,
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
            let [old] = old_name.as_slice() else {
                return Err(DdlAdmissionError::with_code(
                    GENERIC_ERROR_CODE,
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
                modify_column_type: None,
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
                allow_remove_auto_inc: context.allow_remove_auto_inc(),
            }))
        }
        tidb_ast::AlterTableAction::ConvertCharacterSet { charset, collation } => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            if let Some(name) = charset {
                // go's CONVERT TO accepts any identifier at the grammar and
                // answers `ErrUnknownCharacterSet` (1115) when the name is
                // not a charset — a collation name, say.
                if tidb_datatype::get_charset_info(name).is_err() {
                    return Err(DdlAdmissionError::with_code(
                        1115,
                        format!("Unknown character set: '{name}'"),
                    ));
                }
            }
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
                return Err(DdlAdmissionError::with_code(
                    GENERIC_ERROR_CODE,
                    "ALTER COLUMN needs a column name",
                ));
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
                        match lower_alter_add_index(alter, index, default_schema, context)? {
                            DdlStatement::CreateIndex {
                                if_not_exists,
                                index,
                                expressions,
                                condition,
                                context,
                                ..
                            } => actions.push(AlterColumnAction::AddIndex {
                                if_not_exists,
                                index,
                                expressions,
                                condition,
                                context,
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
        tidb_ast::AlterTableAction::Partition(
            action @ tidb_ast::AlterPartitionAction::Add { .. },
        ) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AddPartitions {
                schema,
                table,
                sql: Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::AlterTable(Box::new(
                    alter.clone(),
                ))))
                .restore(),
                action: Box::new(action.clone()),
                context: DdlStatementContext(context.clone()),
            }))
        }
        // Go `AlterTablePartition` for INTERVAL partitioning: LAST PARTITION
        // LESS THAN is an ADD PARTITION job of the generated definitions ...
        tidb_ast::AlterTableAction::Partition(
            action @ tidb_ast::AlterPartitionAction::LastPartitionLessThan { .. },
        ) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AddPartitions {
                schema,
                table,
                sql: Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::AlterTable(Box::new(
                    alter.clone(),
                ))))
                .restore(),
                action: Box::new(action.clone()),
                context: DdlStatementContext(context.clone()),
            }))
        }
        // ... and FIRST PARTITION LESS THAN a DROP PARTITION job of the
        // definitions below the bound.
        tidb_ast::AlterTableAction::Partition(
            tidb_ast::AlterPartitionAction::Drop { .. }
            | tidb_ast::AlterPartitionAction::FirstPartitionLessThan { .. },
        ) => {
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
        tidb_ast::AlterTableAction::Partition(
            action @ (tidb_ast::AlterPartitionAction::Reorganize { .. }
            | tidb_ast::AlterPartitionAction::Repartition(_)
            | tidb_ast::AlterPartitionAction::RemovePartitioning
            | tidb_ast::AlterPartitionAction::Coalesce { .. }),
        ) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::ReorganizePartitions {
                schema,
                table,
                action: Box::new(action.clone()),
                context: DdlStatementContext(context.clone()),
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
                check_constraints: context.enable_check_constraint(),
            }))
        }
        tidb_ast::AlterTableAction::WithValidation => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AcceptedNoOp {
                schema,
                table,
                warning: Some((
                    8200,
                    "ALTER TABLE WITH VALIDATION is currently unsupported".to_owned(),
                )),
            }))
        }
        // go raises 8200 for the WITHOUT VALIDATION option-only form too,
        // naming its own spelling (oracle g-alter: `ALTER TABLE ab3 WITHOUT
        // VALIDATION` answers `ALTER TABLE WITHOUT VALIDATION is currently
        // unsupported`); ALGORITHM/LOCK stay silent.
        tidb_ast::AlterTableAction::WithoutValidation => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AcceptedNoOp {
                schema,
                table,
                warning: Some((
                    8200,
                    "ALTER TABLE WITHOUT VALIDATION is currently unsupported".to_owned(),
                )),
            }))
        }
        tidb_ast::AlterTableAction::Algorithm(_) | tidb_ast::AlterTableAction::Lock(_) => {
            // go runs these as no-op jobs: the option selects execution
            // strategy, and there is nothing to execute.
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AcceptedNoOp {
                schema,
                table,
                warning: None,
            }))
        }
        tidb_ast::AlterTableAction::SetStatsOptions(_) => Err(DdlAdmissionError::with_code(
            8200,
            "This type of ALTER TABLE is currently unsupported",
        )),
        tidb_ast::AlterTableAction::Force => Err(DdlAdmissionError::with_code(
            8200,
            "Unsupported Unsupported/unknown ALTER TABLE specification",
        )),
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
        [] => Err(DdlAdmissionError::with_code(
            GENERIC_ERROR_CODE,
            "RENAME TABLE names no table",
        )),
        [pair] => Ok(Some(DdlStatement::RenameTable {
            is_alter: false,
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
    context: &tidb_executor::StmtContext,
) -> Result<DdlStatement, DdlAdmissionError> {
    // go's `ADD PRIMARY KEY` is a unique index named PRIMARY whose columns
    // gain NOT NULL+PRI — the same shape a CREATE TABLE PK carries
    // (`ALTER TABLE pf ADD PRIMARY KEY(b(2))` → `PRIMARY KEY (b(2))
    // /*T![clustered_index] NONCLUSTERED */` with the columns stamped
    // NOT NULL), so it lowers through the same CreateIndex statement the
    // other index kinds use.
    let primary = index.kind == IndexConstraintKind::PrimaryKey;
    // Go `CreatePrimaryKey` refuses a clustered key before it looks at the
    // table.
    if primary
        && matches!(
            index.options.primary_key_storage,
            Some(tidb_ast::PrimaryKeyStorage::Clustered)
        )
    {
        return Err(DdlAdmissionError::with_code(
            tidb_error::tidb::errcode::ErrUnsupportedDDLOperation,
            "Adding clustered primary key is not supported. Please consider adding \
             NONCLUSTERED primary key instead",
        ));
    }
    let kind = match index.kind {
        IndexConstraintKind::Key | IndexConstraintKind::Index => tidb_ast::IndexKind::Ordinary,
        IndexConstraintKind::Unique
        | IndexConstraintKind::UniqueKey
        | IndexConstraintKind::UniqueIndex => tidb_ast::IndexKind::Unique,
        IndexConstraintKind::PrimaryKey => tidb_ast::IndexKind::Unique,
        IndexConstraintKind::Fulltext => {
            // go's message carries its own wording; `unsupported` would
            // prefix `Unsupported ` onto a sentence that already reads as a
            // refusal (oracle g-alter: `8200 FULLTEXT index is only
            // supported in starter deployment mode`).
            return Err(DdlAdmissionError::with_code(
                tidb_error::tidb::errcode::ErrUnsupportedDDLOperation,
                "FULLTEXT index is only supported in starter deployment mode",
            ));
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
    // go's `ADD PRIMARY KEY` has a FIXED reserved name (`PRIMARY`); any
    // other anonymous index stays unnamed here, and the plan names it with
    // `GetName4AnonymousIndex` against the loaded table, as Go's
    // `checkIndexNameAndColumns` does.
    let name = match index.name.as_ref().filter(|name| !name.is_empty()) {
        Some(name) => name.clone(),
        None if primary => "PRIMARY".to_owned(),
        None => String::new(),
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
        primary,
        context,
    )
}

/// Prepare one column job against its original table, shared by single and
/// bundled ALTER. The prepared column is never published during admission.
fn prepare_rename_column(
    stored: &TableInfo,
    table: &str,
    from: &str,
    to: &str,
) -> Result<Option<tidb_model::ColumnInfo>, DdlPlanError> {
    let wanted = from.go_to_lower();
    // Go `table.FindCol(tbl.VisibleCols(), oldColName.L)`.
    let Some(position) = stored.columns.iter_deref().position(|candidate| {
        let candidate = candidate.read();
        !candidate.hidden && candidate.name.lowercase() == wanted
    }) else {
        return Err(DdlPlanError::UnknownColumn {
            column: from.to_owned(),
            table: table.to_owned(),
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
    if wanted == to.go_to_lower() {
        return Ok(None);
    }
    let new_name = to.go_to_lower();
    if new_name == "_tidb_rowid" {
        return Err(coded_refusal(
            tidb_error::mysql::errcode::ErrWrongColumnName,
            format!("Incorrect column name '{new_name}'"),
        ));
    }
    if stored
        .columns
        .iter_deref()
        .any(|candidate| candidate.read().name.lowercase() == new_name)
    {
        return Err(DdlPlanError::DuplicateColumnName(to.to_owned()));
    }
    // Go `checkModifyColumnWithGeneratedColumnsConstraint`.
    if let Some(dependent) = stored.columns.iter_deref().find(|candidate| {
        let candidate = candidate.read();
        candidate.is_generated() && candidate.dependences.contains(&wanted)
    }) {
        return Err(if dependent.read().hidden {
            coded_refusal(
                tidb_error::mysql::errcode::ErrDependentByFunctionalIndex,
                format!(
                    "Column '{from}' has an expression index dependency and cannot be dropped \
                     or renamed"
                ),
            )
        } else {
            coded_refusal(
                tidb_error::mysql::errcode::ErrDependentByGeneratedColumn,
                format!("Column '{from}' has a generated column dependency."),
            )
        });
    }
    check_drop_column_with_partition_constraint(stored, &wanted)?;
    let mut column = stored
        .columns
        .get(position)
        .expect("original column is present")
        .read()
        .clone_like_go();
    column.name = CiString::new(to.to_owned());
    Ok(Some(column))
}

/// One MODIFY / CHANGE COLUMN as Go's `GetModifiableColumnJob` builds it,
/// with the kind `getModifyColumnType` gives the change.
struct PreparedModifyColumn {
    /// The column as the change leaves it, under the old column's id.
    column: tidb_model::ColumnInfo,
    /// How the stored data changes: Go `getModifyColumnType`, before the
    /// VARCHAR -> CHAR adjustment, which [`Self::precheck`] records.
    kind: tidb_executor::ddl::modify_column_type::ModifyColumnType,
    /// Whether Go routes the change through `ModifyTypePrecheck`: the rows
    /// are checked first, and a row that fails makes it a reorganization.
    precheck: bool,
    /// Go `!noReorgDataStrict`: whether the row check covers the value range.
    check_value_range: bool,
}

fn prepare_modify_column(
    stored: &TableInfo,
    table: &str,
    column: &tidb_ast::ColumnDef,
    requested_position: &tidb_ast::ColumnPosition,
    context: &DdlStatementContext,
    rename_from: Option<&str>,
    modify_column_type: Option<tidb_executor::ddl::modify_column_type::ModifyColumnType>,
) -> Result<PreparedModifyColumn, DdlPlanError> {
    use tidb_executor::ddl::modify_column_type::{
        get_modify_column_type, modify_column_type as adjust, no_reorg_data_strict,
        ModifyColumnFacts, ModifyColumnType,
    };
    // A CHANGE locates by the OLD name; a MODIFY by the (unchanged)
    // declared name.
    let wanted = rename_from.unwrap_or(column.name.as_str()).go_to_lower();
    let Some(position) = stored
        .columns
        .iter_deref()
        .position(|candidate| candidate.read().name.lowercase() == wanted)
    else {
        // Go 1054 ErrBadField through the modify path.
        return Err(DdlPlanError::UnknownColumn {
            column: rename_from.unwrap_or(column.name.as_str()).to_owned(),
            table: table.to_owned(),
        });
    };
    // Go `GetModifiableColumnJob` / `onModifyColumn`.
    check_column_referenced_by_partial_condition(
        stored,
        stored
            .columns
            .get(position)
            .expect("the position was just found")
            .read()
            .name
            .original(),
    )?;
    let new_name = column.name.go_to_lower();
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
    // Go `GetModifiableColumnJob` builds the new column from the definition
    // alone, keeping only the old column's id, offset, state, origin default
    // and version: an omitted DEFAULT or COMMENT is gone afterwards.
    let built = crate::table_info_build::build_modified_column(
        column,
        &stored.charset,
        &stored.collate,
        &context.0,
    )
    .map_err(DdlPlanError::Admission)?;
    let admission = |error: tidb_executor::DriverError| {
        let error = error.to_mysql_error();
        DdlPlanError::Admission(DdlAdmissionError::with_code(error.code, error.message))
    };
    // Go: "Copy index related options to the new spec", and a primary key
    // column stays NOT NULL.
    let mut field_type = built.field_type.clone();
    let index_flags = old.field_type.flags()
        & (FieldTypeFlags::PRI_KEY | FieldTypeFlags::UNIQUE_KEY | FieldTypeFlags::MULTIPLE_KEY);
    field_type.add_flags(index_flags);
    if old.field_type.has_flag(FieldTypeFlags::PRI_KEY) {
        field_type.add_flags(FieldTypeFlags::NOT_NULL);
    }
    let indexed = stored.indices.iter_deref().any(|index| {
        index
            .read()
            .columns
            .iter_deref()
            .any(|part| part.read().name.lowercase() == wanted)
    });
    tidb_executor::ddl::check_modify_types(
        &old.field_type,
        &field_type,
        old.name.lowercase(),
        indexed,
    )
    .map_err(admission)?;
    let check_value_range = !no_reorg_data_strict(&old.field_type, &field_type, indexed);
    let mut changed = built;
    changed.id = old.id;
    changed.offset = old.offset;
    changed.state = old.state;
    changed.version = old.version;
    changed.field_type = field_type;
    // Go: "new col's origin default value be the same as the new default
    // value" once the data may change; otherwise the old one stands.
    if check_value_range {
        crate::table_info_build::stamp_modified_origin_default(&mut changed, &context.0)
            .map_err(DdlPlanError::Admission)?;
    } else {
        changed.origin_default_value = old.origin_default_value.clone();
        changed.origin_default_value_bit = old.origin_default_value_bit.clone();
    }
    // Go `modify_column.go:700`: `MODIFY COLUMN b AFTER b` names
    // the column as its own anchor, which Go answers as
    // ErrColumnNotExists on THAT column rather than as a no-op.
    if let tidb_ast::ColumnPosition::After(anchor) = requested_position {
        if anchor.go_to_lower() == wanted {
            return Err(DdlPlanError::UnknownColumn {
                column: rename_from.unwrap_or(column.name.as_str()).to_owned(),
                table: table.to_owned(),
            });
        }
    }
    // Unlike ADD COLUMN, the column is already AT its offset, so the
    // destination is located against that rather than against an
    // appended tail (Go `modify_column.go:704`).
    locate_offset_to_move(position, requested_position, stored)?;
    let facts = ModifyColumnFacts {
        column_indexed: indexed,
        partitioned: stored.partition.is_some(),
        tiflash_replica: stored
            .tiflash_replica
            .as_ref()
            .is_some_and(|replica| replica.read().count > 0),
        strict_sql_mode: context.0.strict(),
    };
    let (kind, precheck) = match modify_column_type {
        Some(kind) => (kind, false),
        None => (
            get_modify_column_type(&old.field_type, &changed.field_type, facts),
            adjust(&old.field_type, &changed.field_type, facts) == ModifyColumnType::Precheck,
        ),
    };
    Ok(PreparedModifyColumn {
        column: changed,
        kind,
        precheck,
        check_value_range,
    })
}

/// Go's final state after a changing column / changing indexes replace the
/// old ones: every index on the column under a new id
/// (`initializeChangingIndexes`, `AllocateIndexID`), and under
/// `ModifyTypeReorg` the column under a new id too (`AllocateColumnID`).
/// Returns the column's id afterwards and each renumbered index as
/// (old id, new id).
fn renumber_changed_column(
    info: &mut TableInfo,
    old_id: i64,
    changed_name: &str,
    kind: tidb_executor::ddl::modify_column_type::ModifyColumnType,
) -> (i64, Vec<(i64, i64)>) {
    use tidb_executor::ddl::modify_column_type::ModifyColumnType;
    let mut renumbered_indexes = Vec::new();
    if matches!(kind, ModifyColumnType::IndexReorg | ModifyColumnType::Reorg) {
        let changed_name = changed_name.go_to_lower();
        let related = info
            .indices
            .iter_deref()
            .filter(|index| {
                index
                    .read()
                    .columns
                    .iter_deref()
                    .any(|part| part.read().name.lowercase() == changed_name)
            })
            .collect::<Vec<_>>();
        for index in related {
            info.max_index_id += 1;
            let mut index = index.write();
            renumbered_indexes.push((index.id, info.max_index_id));
            index.id = info.max_index_id;
        }
    }
    let offset = info
        .columns
        .iter_deref()
        .position(|candidate| candidate.read().id == old_id)
        .expect("the modified column keeps its place in the table");
    if kind == ModifyColumnType::Reorg {
        info.max_column_id += 1;
        let new_id = info.max_column_id;
        info.columns
            .get(offset)
            .expect("the modified column is present")
            .write()
            .id = new_id;
    }
    let new_column_id = info
        .columns
        .get(offset)
        .expect("the modified column is present")
        .read()
        .id;
    (new_column_id, renumbered_indexes)
}

/// Apply an already admitted column by stable ID, preserving offsets shifted
/// by preceding jobs and updating index references with the column rename.
fn apply_prepared_column(
    info: &mut TableInfo,
    mut column: tidb_model::ColumnInfo,
    position: &tidb_ast::ColumnPosition,
    add: bool,
) -> Result<(), DdlPlanError> {
    let offset = if add {
        info.max_column_id += 1;
        column.id = info.max_column_id;
        column.offset = info.columns.len() as i64;
        let offset = info.columns.len();
        info.columns.push_handle_go(Some(GoShared::new(column)));
        offset
    } else {
        let offset = info
            .columns
            .iter_deref()
            .position(|c| c.read().id == column.id)
            .expect("admitted column identity survives disjoint sub-jobs");
        let handle = info.columns.get(offset).expect("column is present");
        let old_name = handle.read().name.clone();
        column.offset = offset as i64;
        // Go `adjustTableInfoAfterModifyColumn`: `updateNewIdxColsNameOffset`
        // runs `UpdateIndexCol` on every index part naming the column (a part
        // keeps its prefix only while the new type is prefixable and wider
        // than it), and the foreign keys and TTL definition follow a rename.
        for index in info.indices.iter_deref() {
            for indexed in index.read().columns.iter_deref() {
                let mut indexed = indexed.write();
                if indexed.name.lowercase() == old_name.lowercase() {
                    indexed.name = column.name.clone();
                    if !column.field_type.code().is_type_prefixable()
                        || column.field_type.flen() <= indexed.length
                    {
                        indexed.length = -1;
                    }
                }
            }
        }
        if old_name.lowercase() != column.name.lowercase() {
            for foreign_key in info.foreign_keys.iter_deref() {
                let foreign_key = foreign_key.read();
                for position in 0..foreign_key.cols.len() {
                    if foreign_key.cols.get(position).lowercase() == old_name.lowercase() {
                        foreign_key.cols.set(position, column.name.clone());
                    }
                }
            }
            if let Some(ttl) = info.ttl_info.as_ref() {
                let mut ttl = ttl.write();
                if ttl.column_name.lowercase() == old_name.lowercase() {
                    ttl.column_name = column.name.clone();
                }
            }
        }
        *handle.write() = column;
        offset
    };
    let destination = locate_offset_to_move(offset, position, info)?;
    move_column_info(info, offset, destination);
    Ok(())
}

/// Typed column jobs and conditional no-ops prepared against one original
/// table. Index execution keeps its existing backfill owner after admission.
struct PreparedClusterColumns {
    columns: BTreeMap<usize, (tidb_model::ColumnInfo, tidb_ast::ColumnPosition, bool)>,
    /// A MODIFY / CHANGE sub-job's data step: its kind, whether it prechecks
    /// the rows, and whether the check covers the value range.
    modify_kinds: BTreeMap<
        usize,
        (
            tidb_executor::ddl::modify_column_type::ModifyColumnType,
            bool,
            bool,
        ),
    >,
    foreign_keys: BTreeMap<usize, CreatedForeignKey>,
    skipped: BTreeMap<usize, String>,
}

fn prepare_cluster_columns(
    catalog: &ClusterCatalog,
    stored: &TableInfo,
    schema: &str,
    table: &str,
    actions: &[AlterColumnAction],
    warnings: &mut Vec<(DdlWarningLevel, u16, String)>,
) -> Result<PreparedClusterColumns, DdlPlanError> {
    let mut prepared = PreparedClusterColumns {
        columns: BTreeMap::new(),
        modify_kinds: BTreeMap::new(),
        foreign_keys: BTreeMap::new(),
        skipped: BTreeMap::new(),
    };
    // Go `AlterTable` names every anonymous key before any spec runs.
    let mut max_foreign_key_id = stored.max_foreign_key_id;
    let mut add_foreign_keys = Vec::new();
    let mut add_columns = Vec::new();
    let mut drop_columns = Vec::new();
    let mut modify_columns = Vec::new();
    let mut position_columns = Vec::new();
    let mut relative_columns = Vec::new();
    let mut add_indexes = Vec::new();
    let mut drop_indexes = Vec::new();
    let mut alter_indexes = Vec::new();
    for (offset, action) in actions.iter().enumerate() {
        let mut skipped = None;
        let mut column_job = None;
        match action {
            AlterColumnAction::Add {
                if_not_exists,
                column,
                position,
                context,
            } => {
                let mut candidate = stored.clone_like_go();
                match apply_add_column(
                    &mut candidate,
                    schema,
                    table,
                    column,
                    position,
                    *if_not_exists,
                    &context.0,
                )? {
                    AlterColumnOutcome::Applied => {
                        let built =
                            tidb_model::column::find_column_info(&candidate.columns, &column.name)
                                .expect("prepared added column")
                                .read()
                                .clone_like_go();
                        add_columns.push(built.name.clone());
                        relative_columns.extend(
                            built
                                .dependences
                                .snapshot()
                                .into_iter()
                                .map(|name| CiString::new(name.to_string())),
                        );
                        column_job = Some((built, position.clone(), true));
                    }
                    AlterColumnOutcome::AlreadySatisfied(detail) => {
                        warnings.push((
                            DdlWarningLevel::Note,
                            1060,
                            format!("Duplicate column name '{}'", column.name),
                        ));
                        skipped = Some(detail);
                    }
                }
            }
            AlterColumnAction::Modify {
                column,
                position,
                context,
                rename_from,
                modify_column_type,
            } => {
                let modified = prepare_modify_column(
                    stored,
                    table,
                    column,
                    position,
                    context,
                    rename_from.as_deref(),
                    *modify_column_type,
                )?;
                prepared.modify_kinds.insert(
                    offset,
                    (modified.kind, modified.precheck, modified.check_value_range),
                );
                let built = modified.column;
                let old_name =
                    CiString::new(rename_from.as_deref().unwrap_or(&column.name).to_owned());
                if built.name.lowercase() == old_name.lowercase() {
                    modify_columns.push(built.name.clone());
                } else {
                    add_columns.push(built.name.clone());
                    drop_columns.push(old_name);
                }
                column_job = Some((built, position.clone(), false));
            }
            AlterColumnAction::Rename { from, to } => {
                if let Some(built) = prepare_rename_column(stored, table, from, to)? {
                    add_columns.push(built.name.clone());
                    drop_columns.push(CiString::new(from.clone()));
                    column_job = Some((built, tidb_ast::ColumnPosition::Default, false));
                } else {
                    skipped = Some(format!("column `{from}` already has the requested name"));
                }
            }
            AlterColumnAction::Drop {
                if_exists,
                column,
                allow_remove_auto_inc,
            } => {
                // Go `DropColumn` for each spec, against the original table.
                if check_is_droppable_column(
                    catalog,
                    schema,
                    stored,
                    column,
                    *if_exists,
                    *allow_remove_auto_inc,
                )? {
                    check_visible_column_cnt(stored, 0, 1)?;
                    drop_columns.push(CiString::new(column.clone()));
                } else {
                    warnings.push((
                        DdlWarningLevel::Note,
                        1091,
                        format!("Can't DROP '{column}'; check that column/key exists"),
                    ));
                    skipped = Some(format!(
                        "column `{column}` does not exist on `{schema}`.`{table}`"
                    ));
                }
            }
            AlterColumnAction::AddIndex {
                if_not_exists,
                index,
                expressions,
                condition,
                context,
            } => {
                let name = settle_added_index_name(stored, index, expressions);
                if find_index(stored, &name).is_some() {
                    if !if_not_exists {
                        return Err(DdlPlanError::DuplicateKeyName(name));
                    }
                    warnings.push((
                        DdlWarningLevel::Note,
                        1061,
                        format!("Duplicate key name '{name}'"),
                    ));
                    skipped = Some(format!(
                        "index `{name}` already exists on `{schema}`.`{table}`"
                    ));
                } else {
                    // Go `fillMultiSchemaInfo`: each expression part's hidden
                    // column, built by the executor against the original
                    // table, makes the columns it reads relative too.
                    if expressions.iter().any(Option::is_some) {
                        let mut candidate = stored.clone_like_go();
                        plan_added_index(
                            &mut candidate,
                            index,
                            expressions,
                            condition.as_ref(),
                            &name,
                            // The sub-job plans the index again, and raises
                            // its warnings then.
                            &context.0.with_isolated_warnings(),
                        )?;
                        for hidden in candidate.columns.iter_deref().skip(stored.columns.len()) {
                            relative_columns.extend(
                                hidden
                                    .read()
                                    .dependences
                                    .snapshot()
                                    .into_iter()
                                    .map(|name| CiString::new(name.to_string())),
                            );
                        }
                    }
                    for (indexed, expression) in index.columns.iter_deref().zip(expressions) {
                        // An expression part's hidden column does not exist
                        // until the plan builds it.
                        if expression.is_some() {
                            continue;
                        }
                        let indexed = indexed.read();
                        if tidb_model::column::find_column_info(
                            &stored.columns,
                            indexed.name.original(),
                        )
                        .is_none()
                        {
                            return Err(DdlPlanError::UnknownIndexColumn {
                                column: indexed.name.original().to_owned(),
                                index: name.clone(),
                            });
                        }
                        relative_columns.push(indexed.name.clone());
                    }
                    add_indexes.push(CiString::new(name));
                }
            }
            AlterColumnAction::DropIndex { if_exists, name } => {
                if let Some(index) = find_index(stored, name) {
                    crate::foreign_key_build::check_index_needed_in_foreign_key(
                        catalog,
                        schema,
                        stored,
                        &index.read(),
                    )
                    .map_err(DdlPlanError::Admission)?;
                }
                if find_index(stored, name).is_none() {
                    if !if_exists {
                        return Err(DdlPlanError::UnknownIndex(name.clone()));
                    }
                    warnings.push((
                        DdlWarningLevel::Note,
                        1091,
                        format!("Can't DROP '{name}'; check that column/key exists"),
                    ));
                    skipped = Some(format!(
                        "index `{name}` does not exist on `{schema}`.`{table}`"
                    ));
                } else {
                    drop_indexes.push(CiString::new(name.clone()));
                }
            }
            AlterColumnAction::AddForeignKey {
                definition,
                context,
            } => {
                let name = match definition.name.as_deref() {
                    Some(name) if !name.is_empty() => name.to_owned(),
                    _ => {
                        max_foreign_key_id += 1;
                        format!("fk_{max_foreign_key_id}")
                    }
                };
                let created =
                    create_foreign_key(catalog, schema, stored, &name, definition, context)?;
                // Go `fillMultiSchemaInfo`: the queued `createIndex` adds an
                // index over the key's columns, the key an `AddForeignKeys`
                // entry.
                if created.needs_index {
                    add_indexes.push(created.fk.name.clone());
                    relative_columns.extend(created.fk.cols.snapshot());
                }
                add_foreign_keys.push(created.fk.cols.snapshot());
                prepared.foreign_keys.insert(offset, created);
            }
            AlterColumnAction::DropForeignKey { name } => {
                // Go `DropForeignKey`.
                if !stored
                    .foreign_keys
                    .iter_deref()
                    .any(|fk| fk.read().name.lowercase() == name.go_to_lower())
                {
                    return Err(foreign_key_not_exists(name));
                }
            }
            AlterColumnAction::AddCheck {
                definition,
                context,
            } => {
                if actions.len() > 1 {
                    let mut candidate = stored.clone_like_go();
                    crate::table_info_build::append_check_constraints(
                        &mut candidate,
                        &[tidb_executor::ddl::check_constraint::CheckConstraintInput {
                            definition: (**definition).clone(),
                            in_column: None,
                        }],
                        tidb_executor::ddl::check_constraint::CheckConstraintBuildMode::Alter,
                        &context.0,
                    )
                    .map_err(DdlPlanError::Admission)?;
                    return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(8200,
                        "Unsupported multi schema change for add check constraint",
                    )));
                }
            }
            // Go `fillMultiSchemaInfo`.
            AlterColumnAction::DropPrimaryKey => {
                drop_indexes.push(CiString::new("PRIMARY".to_owned()));
            }
            AlterColumnAction::RenameIndex { from, to } => {
                add_indexes.push(CiString::new(from.clone()));
                drop_indexes.push(CiString::new(to.clone()));
            }
            AlterColumnAction::IndexVisibility { name, .. } => {
                alter_indexes.push(CiString::new(name.clone()));
            }
            AlterColumnAction::SetDefault { column, .. } => {
                modify_columns.push(CiString::new(column.clone()));
            }
            AlterColumnAction::RebaseAutoId { .. }
            | AlterColumnAction::TableComment { .. }
            | AlterColumnAction::TableCharset { .. } => {}
        }
        if let Some(job) = column_job {
            if let tidb_ast::ColumnPosition::After(name) = &job.1 {
                position_columns.push(CiString::new(name.clone()));
            }
            prepared.columns.insert(offset, job);
        }
        if let Some(detail) = skipped {
            prepared.skipped.insert(offset, detail);
        }
    }
    let names = tidb_model::MultiSchemaInfo {
        add_columns: GoSharedSlice::from_vec(add_columns),
        drop_columns: GoSharedSlice::from_vec(drop_columns),
        modify_columns: GoSharedSlice::from_vec(modify_columns),
        position_columns: GoSharedSlice::from_vec(position_columns),
        relative_columns: GoSharedSlice::from_vec(relative_columns),
        add_indexes: GoSharedSlice::from_vec(add_indexes),
        drop_indexes: GoSharedSlice::from_vec(drop_indexes),
        alter_indexes: GoSharedSlice::from_vec(alter_indexes),
        ..Default::default()
    };
    // Go `checkMultiSchemaInfo`.
    tidb_executor::ddl::check_multi_schema_names(&names).map_err(|error| {
        let error = error.to_mysql_error();
        coded_refusal(error.code, error.message)
    })?;
    check_visible_column_cnt(stored, names.add_columns.len(), names.drop_columns.len())?;
    check_operate_drop_index_use_by_foreign_key(
        stored,
        &names.drop_indexes.snapshot(),
        &add_foreign_keys,
    )?;
    check_add_column_too_many_columns(
        (stored.columns.len() + names.add_columns.len()).saturating_sub(names.drop_columns.len()),
    )?;
    Ok(prepared)
}

/// One sub-action's answer inside an evolving ALTER.
enum AlterColumnOutcome {
    Applied,
    /// The `IF [NOT] EXISTS` no-op, with the sentence `already` reports.
    AlreadySatisfied(String),
}

/// Go `onDropIndex` for the primary key over an EVOLVING `TableInfo`, after
/// `CheckIsDropPrimaryKey`. The index entries go with the definition.
fn apply_drop_primary_key(
    catalog: &ClusterCatalog,
    schema: &str,
    info: &mut TableInfo,
    use_new_collation: bool,
    backfill: &mut Vec<IndexBackfill>,
) -> Result<(), DdlPlanError> {
    // A clustered primary key -- whether it is the int handle (`PKIsHandle`)
    // or a composite one (`IsCommonHandle`) -- is what the rows are STORED
    // under, so dropping it would leave every row unaddressable.
    if info.pk_is_handle || info.is_common_handle {
        return Err(DdlPlanError::Admission(
            crate::table_info_build::DdlAdmissionError::with_code(
                8200,
                "Unsupported drop primary key when the table is using clustered index".to_owned(),
            ),
        ));
    }
    // Go's `PRIMARY` lookup is by the reserved name, and a general index that
    // merely happens to be called `primary` is not one (the #14243 fix).
    let primary = info.indices.iter_deref().find(|index| {
        let index = index.read();
        index.name.lowercase() == "primary" && index.primary
    });
    let Some(primary) = primary else {
        // Go `ErrCantDropFieldOrKey` (1091), the same code DROP INDEX uses
        // for a name that is not there.
        return Err(DdlPlanError::UnknownIndex("PRIMARY".to_owned()));
    };
    let dropped = primary.read().clone_like_go();
    crate::foreign_key_build::check_index_needed_in_foreign_key(catalog, schema, info, &dropped)
        .map_err(DdlPlanError::Admission)?;
    // The removal walk rebuilds the dropped index's entry keys, so its table
    // must still CARRY the index.
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
    // An index whose rows survive its `TableInfo` is invisible garbage that a
    // later index of the same id would read as its own.
    backfill.push(IndexBackfill {
        table: backfill_table,
        index: GoShared::new(dropped),
        use_new_collation,
        operation: IndexBackfillOperation::Drop,
    });
    Ok(())
}

/// Go `ValidateRenameIndex` and `renameIndexes` over an EVOLVING `TableInfo`.
fn apply_rename_index(
    info: &mut TableInfo,
    schema: &str,
    table: &str,
    from: &str,
    to: &str,
) -> Result<AlterColumnOutcome, DdlPlanError> {
    if find_index(info, from).is_none() {
        return Err(DdlPlanError::KeyNotExists {
            index: from.to_owned(),
            table: table.to_owned(),
        });
    }
    // Case-SENSITIVE equality is the no-op test: `inDex` -> `IndEX` is a real
    // rename that only changes the stored spelling.
    if from == to {
        return Ok(AlterColumnOutcome::AlreadySatisfied(format!(
            "index `{from}` on `{schema}`.`{table}` already has that name"
        )));
    }
    let from_lower = from.go_to_lower();
    let to_lower = to.go_to_lower();
    if from_lower != to_lower {
        if let Some(existing) = find_index(info, to) {
            return Err(DdlPlanError::DuplicateKeyName(
                existing.read().name.original().to_owned(),
            ));
        }
    }
    // Go `renameIndexes` walks every index and renames each one whose name
    // matches, rather than stopping at the first.
    //
    // Its two other passes have nothing to rename here: temp indexes exist
    // only during a reorg this node does not run, and the hidden columns of
    // an expression index cannot occur because this node refuses to create
    // or load one.
    for candidate in info.indices.iter_deref() {
        let mut candidate = candidate.write();
        if candidate.name.lowercase() == from_lower {
            candidate.name = CiString::new(to.to_owned());
        }
    }
    Ok(AlterColumnOutcome::Applied)
}

/// Go `validateAlterIndexVisibility` and `setIndexVisibility` over an
/// EVOLVING `TableInfo`.
fn apply_index_visibility(
    info: &mut TableInfo,
    schema: &str,
    table: &str,
    index: &str,
    invisible: bool,
) -> Result<AlterColumnOutcome, DdlPlanError> {
    // The index must exist AND be public, else `ErrKeyNotExists`.
    let found = find_index(info, index)
        .filter(|candidate| candidate.read().state == tidb_model::SchemaState::PUBLIC);
    let Some(found) = found else {
        return Err(DdlPlanError::KeyNotExists {
            index: index.to_owned(),
            table: table.to_owned(),
        });
    };
    // Go's early return: a visibility that already matches finishes the job
    // without touching the table.
    if found.read().invisible == invisible {
        return Ok(AlterColumnOutcome::AlreadySatisfied(format!(
            "index `{index}` on `{schema}`.`{table}` is already {}",
            if invisible { "invisible" } else { "visible" }
        )));
    }
    if invisible && found.read().is_columnar_index() {
        return Err(coded_refusal(
            8200,
            format!(
                "INVISIBLE can not be used in {} INDEX",
                found.read().tp.sql()
            ),
        ));
    }
    let wanted = index.go_to_lower();
    // Go `setIndexVisibility` walks EVERY index and sets each one whose name
    // matches, rather than stopping at the first.
    for candidate in info.indices.iter_deref() {
        let mut candidate = candidate.write();
        if candidate.name.lowercase() == wanted {
            candidate.invisible = invisible;
        }
    }
    Ok(AlterColumnOutcome::Applied)
}

/// Go `onSetDefaultValue` over an EVOLVING `TableInfo`.
fn apply_set_column_default(
    info: &mut TableInfo,
    table: &str,
    column: &str,
    default_value: Option<&tidb_ast::Expr>,
    context: &tidb_executor::StmtContext,
) -> Result<(), DdlPlanError> {
    // Go resolves the target column before it looks at the new default at
    // all, and a non-public one reads as absent.
    let wanted = column.go_to_lower();
    let Some(target) = info.columns.iter_deref().find(|candidate| {
        let candidate = candidate.read();
        candidate.name.lowercase() == wanted && candidate.state == tidb_model::SchemaState::PUBLIC
    }) else {
        return Err(DdlPlanError::UnknownColumn {
            column: column.to_owned(),
            table: table.to_owned(),
        });
    };
    // Go `ErrInvalidAutoRandom`: the shard bits own the column's values, so a
    // default could never be written.
    if default_value.is_some()
        && info.auto_random_bits > 0
        && target.read().field_type.has_flag(FieldTypeFlags::PRI_KEY)
    {
        return Err(DdlPlanError::InvalidAutoRandom(
            "auto_random is incompatible with default".to_owned(),
        ));
    }
    let mut target = target.write();
    crate::table_info_build::set_column_default(column, &mut target, default_value, context)
        .map_err(DdlPlanError::Admission)
}

/// Go refuses an ALTER TABLE charset change that names neither a charset nor
/// a collation before it resolves anything.
fn check_table_charset_named(charset: &str, collate: &str) -> Result<(), DdlPlanError> {
    if charset.is_empty() && collate.is_empty() {
        return Err(DdlPlanError::Admission(
            crate::table_info_build::DdlAdmissionError::with_code(
                1115,
                "Unknown character set: ''".to_owned(),
            ),
        ));
    }
    Ok(())
}

/// Go `checkAlterTableCharset` and `onModifyTableCharsetAndCollate` over an
/// EVOLVING `TableInfo`.
#[allow(clippy::too_many_arguments)]
fn apply_table_charset_and_collate(
    catalog: &ClusterCatalog,
    schema: &str,
    table: &str,
    info: &mut TableInfo,
    charset: &str,
    collate: &str,
    overwrite_columns: bool,
) -> Result<AlterColumnOutcome, DdlPlanError> {
    let database = find_database(catalog, schema)
        .expect("locate_table resolved the database")
        .info
        .clone();
    // Go: an omitted charset keeps the table's own, and an omitted collation
    // becomes that charset's default.
    let to_charset = if charset.is_empty() {
        info.charset.clone()
    } else {
        charset.to_owned()
    };
    let (to_charset, to_collate) = crate::table_info_build::resolve_charset_collation(
        Some(to_charset.as_str()),
        if collate.is_empty() {
            None
        } else {
            Some(collate)
        },
        CATALOG_CHARSET,
        CATALOG_COLLATION,
    )
    .map_err(DdlPlanError::Admission)?;

    // Go `checkAlterTableCharset`'s early return: the table already matches
    // AND, when the columns are in scope, every non-binary column matches too.
    if info.charset == to_charset && info.collate == to_collate {
        let columns_settled = !overwrite_columns
            || info.columns.iter_deref().all(|column| {
                let column = column.read();
                let column_charset = column.field_type.charset_name();
                column_charset == "binary"
                    || (column_charset == to_charset
                        && column.field_type.collation().name() == to_collate)
            });
        if columns_settled {
            return Ok(AlterColumnOutcome::AlreadySatisfied(format!(
                "`{schema}`.`{table}` is already {to_charset}/{to_collate}"
            )));
        }
    }

    // Go resolves the table's ORIGINAL pair against its database before
    // comparing, so a table that never named one is judged by what it
    // actually inherited.
    let (from_charset, from_collate) = crate::table_info_build::resolve_charset_collation(
        if info.charset.is_empty() {
            Some(database.charset.as_str())
        } else {
            Some(info.charset.as_str())
        },
        if info.collate.is_empty() {
            Some(database.collate.as_str())
        } else {
            Some(info.collate.as_str())
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
        "",
    )?;

    if overwrite_columns {
        // Go checks every column BEFORE it rewrites any of them, so a refusal
        // leaves the table exactly as it was.
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
                is_column_with_index(&name, info),
                &name,
            )?;
        }
    }
    info.charset = to_charset.clone();
    info.collate = to_collate.clone();
    if overwrite_columns {
        let collation = tidb_datatype::Collation::from_name(&to_collate)
            .unwrap_or(tidb_datatype::Collation::Binary);
        for column in info.columns.iter_deref() {
            let mut column = column.write();
            // Go `field_types.HasCharset`: a type that holds text takes the
            // new pair, everything else is marked binary.
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
    Ok(AlterColumnOutcome::Applied)
}

/// Go `verifyNoOverflowShardBits`: the row-id allocator's next global id
/// must leave the new shard bits clear (`tables.OverflowShardBits` with the
/// sign bit reserved).
fn verify_no_overflow_shard_bits<S: MetaSnapshot>(
    snapshot: &mut S,
    db_id: i64,
    info: &TableInfo,
    shard_row_id_bits: u64,
) -> Result<(), DdlPlanError> {
    if shard_row_id_bits == 0 {
        return Ok(());
    }
    let schema_id = if info.auto_id_schema_id == 0 {
        db_id
    } else {
        info.auto_id_schema_id
    };
    // The row-id counter holds the id last handed out; `NextGlobalAutoID` is
    // one past it, and an absent key reads as 0.
    let counter = match snapshot.get(&key::auto_table_id_kv_key(schema_id, info.id))? {
        Some(value) => value::parse_int_value(&value)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        None => 0,
    };
    let next_global = counter.wrapping_add(1);
    const ROW_ID_BIT_LENGTH: u64 = 64;
    let mask = ((1u64 << shard_row_id_bits) - 1) << (ROW_ID_BIT_LENGTH - shard_row_id_bits - 1);
    if next_global & (mask as i64) > 0 {
        return Err(coded_refusal(
            tidb_error::mysql::errcode::ErrAutoincReadFailed,
            format!(
                "shard_row_id_bits {shard_row_id_bits} will cause next global auto ID {next_global} overflow"
            ),
        ));
    }
    Ok(())
}

/// The auto-increment counter a multi-schema change has already moved inside
/// its own transaction: the value its executor-side checks read, and the
/// value the sub-jobs before this one left.
#[derive(Clone, Copy)]
struct RebasedCounter {
    original: i64,
    evolved: i64,
}

/// Go `RebaseAutoID` (the executor's `adjustNewBaseToNextGlobalID`, a
/// warning) and `onRebaseAutoIncrementIDType`. Inside a multi-schema change
/// the job adjusts the base again against the counter the earlier sub-jobs
/// left, and Go reports that sub-job warning as a note
/// (`appendMultiChangeWarningsToOwnerCtx`).
#[allow(clippy::too_many_arguments)]
fn rebase_auto_increment_id<S: MetaSnapshot>(
    snapshot: &mut S,
    auto_ids: Option<&std::sync::Arc<crate::auto_id_client::AutoIdClient>>,
    db_id: i64,
    info: &mut TableInfo,
    requested: i64,
    force: bool,
    rebased: &mut Option<RebasedCounter>,
    writes: &mut Vec<BufferMutation>,
    warnings: &mut Vec<(DdlWarningLevel, u16, String)>,
) -> Result<(), DdlPlanError> {
    if force && requested == 0 {
        return Err(DdlPlanError::AutoIdReadFailed);
    }
    let counter_key = crate::cluster_auto_id::auto_id_key_for(db_id, info);
    let service = auto_ids.filter(|_| info.sep_auto_inc()).map(|client| {
        let unsigned = info.is_auto_inc_col_unsigned();
        crate::cluster_auto_id::AutoIdServiceAllocator::new(
            client.clone(),
            if info.auto_id_schema_id == 0 {
                db_id
            } else {
                info.auto_id_schema_id
            },
            info.id,
            unsigned,
            u32::MAX,
        )
    });
    let call = if force {
        tidb_executor::kv_table::AutoIdCall::with_timeout(std::time::Duration::from_secs(30))
    } else {
        tidb_executor::kv_table::AutoIdCall::background()
    };

    // Go `NextGlobalAutoID`: the stored counter holds the id LAST handed out,
    // so the next one to allocate is one past it. An absent key reads as 0,
    // matching Go's `GetInt64`.
    let stored_counter = if let Some(rebased) = rebased {
        rebased.original
    } else if force && service.is_some() {
        0
    } else if let Some(service) = &service {
        service
            .alloc(&call, 0, 1, 1)
            .map_err(|e| DdlPlanError::Encode(e.to_string()))?
            .1
    } else {
        match snapshot.get(&counter_key)? {
            Some(value) => value::parse_int_value(&value)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            None => 0,
        }
    };
    let mut new_base = requested;
    if !force {
        // Go `adjustNewBaseToNextGlobalID` compares as UNSIGNED whatever the
        // column's signedness, and never lets the base move backwards:
        // another node's allocator may already have handed out ids past the
        // requested base.
        let next_global = (stored_counter as u64).wrapping_add(1);
        let adjusted = u64::max(new_base as u64, next_global) as i64;
        if adjusted != new_base {
            warnings.push((
                DdlWarningLevel::Warning,
                1105,
                format!(
                    "Can't reset AUTO_INCREMENT to {new_base} without FORCE option, \
                     using {adjusted} instead"
                ),
            ));
            new_base = adjusted;
        }
        if let Some(rebased) = rebased {
            let next_global = (rebased.evolved as u64).wrapping_add(1);
            let adjusted = u64::max(new_base as u64, next_global) as i64;
            if adjusted != new_base {
                warnings.push((
                    DdlWarningLevel::Note,
                    1105,
                    format!(
                        "Can't reset AUTO_INCREMENT to {new_base} without FORCE option, \
                         using {adjusted} instead"
                    ),
                ));
                new_base = adjusted;
            }
        }
    }
    info.auto_inc_id = new_base;
    // "The next value to allocate is `newBase`", so the counter -- which
    // holds the last id handed out -- becomes one below it. `Rebase` only
    // ever grows, `ForceRebase` sets exactly; the non-force base was already
    // raised to at least the current counter above, so both reduce to one
    // write here.
    let new_end = new_base.wrapping_sub(1);
    let current = rebased.map_or(stored_counter, |rebased| rebased.evolved);
    let evolved = if let Some(service) = &service {
        service
            .rebase(&call, new_end, force)
            .map_err(|e| DdlPlanError::Encode(e.to_string()))?;
        new_end
    } else if force || (new_end as u64) > (current as u64) {
        writes.push(BufferMutation::set(
            counter_key,
            value::encode_int_value(new_end),
        )?);
        new_end
    } else {
        current
    };
    *rebased = Some(RebasedCounter {
        original: stored_counter,
        evolved,
    });
    Ok(())
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
    // Go `AddColumn`, then `checkAndCreateNewColumn`.
    check_add_column_too_many_columns(info.columns.len() + 1)?;
    let wanted = column.name.go_to_lower();
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
    tidb_executor::ddl::check_too_long_identifier(&column.name).map_err(|error| {
        let error = error.to_mysql_error();
        coded_refusal(error.code, error.message)
    })?;
    // An admission refusal already carries Go's own error number (8200 for
    // `ErrUnsupportedDDLOperation`); wrapping it as an encode failure both
    // flattened that to the generic 1105 and prefixed the client's message
    // with "catalog encode failed", which names an internal step the
    // statement never reached.
    let appended = info.columns.len();
    let destination = locate_offset_to_move(appended, position, info)?;
    // Go `checkDependedColExist`: a generated column reads only visible
    // columns.
    let generated_namespace = info
        .columns
        .iter_deref()
        .map(|column| column.read().clone_like_go())
        .filter(|column| !column.hidden)
        .collect::<Vec<_>>();
    let mut added = crate::table_info_build::build_added_column(
        column,
        &info.charset,
        &info.collate,
        context,
        Some(&generated_namespace),
    )
    .map_err(DdlPlanError::Admission)?;
    // Go `verifyColumnGenerationSingle`: a generated column it reads must
    // come before the new column's position.
    if added.is_generated()
        && info.columns.iter_deref().any(|existing| {
            let existing = existing.read();
            existing.is_generated()
                && usize::try_from(existing.offset).is_ok_and(|offset| offset >= destination)
                && added.dependences.contains(existing.name.lowercase())
        })
    {
        return Err(coded_refusal(
            tidb_error::tidb::errcode::ErrGeneratedColumnNonPrior,
            "Generated column can refer only to generated columns defined prior to it.",
        ));
    }
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
            let wanted = name.go_to_lower();
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

fn coded_refusal(code: u16, message: impl Into<String>) -> DdlPlanError {
    DdlPlanError::Admission(DdlAdmissionError::with_code(code, message.into()))
}

/// Go `checkIsDroppableColumn` (`pkg/ddl/executor.go`): one DROP COLUMN as
/// its submitter checks it against the table as it stands. `Ok(false)` is
/// IF EXISTS over a column that is not there, which Go skips with a 1091
/// note.
fn check_is_droppable_column(
    catalog: &ClusterCatalog,
    schema: &str,
    info: &TableInfo,
    column: &str,
    if_exists: bool,
    allow_remove_auto_inc: bool,
) -> Result<bool, DdlPlanError> {
    let col_name = column.go_to_lower();
    // Go `table.FindCol(t.VisibleCols(), colName.L)`: a hidden
    // expression-index column cannot be named.
    let Some(col) = info
        .columns
        .iter_deref()
        .map(|candidate| candidate.read().clone_like_go())
        .find(|candidate| !candidate.hidden && candidate.name.lowercase() == col_name)
    else {
        if if_exists {
            return Ok(false);
        }
        return Err(DdlPlanError::CantDropFieldOrKey(column.to_owned()));
    };
    is_droppable_column(info, column, info.columns.len())?;
    check_drop_column_with_partition_constraint(info, &col_name)?;
    crate::foreign_key_build::check_drop_column_with_foreign_key_constraint(
        catalog, schema, info, &col_name,
    )
    .map_err(DdlPlanError::Admission)?;
    check_drop_column_with_ttl_config(info, &col_name)?;
    // Go `ErrUnsupportedPKHandle`.
    if col.is_pk_handle_column(info) {
        return Err(coded_refusal(8200, "Unsupported drop integer primary key"));
    }
    // Go `ErrCantDropColWithAutoInc`.
    if col.get_flag() & u64::from(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT) != 0
        && !allow_remove_auto_inc
    {
        return Err(coded_refusal(
            8200,
            "Unsupported can't remove column with auto_increment when \
             @@tidb_allow_remove_auto_inc disabled",
        ));
    }
    check_column_referenced_by_partial_condition(info, col.name.original())?;
    Ok(true)
}

/// Go `checkDropColumn` (`pkg/ddl/column.go`): the owner's checks of one
/// DROP COLUMN sub-job against the table the earlier sub-jobs left.
fn check_drop_column(
    catalog: &ClusterCatalog,
    schema: &str,
    info: &TableInfo,
    column: &str,
    column_count: usize,
) -> Result<(), DdlPlanError> {
    let col_name = column.go_to_lower();
    if !info.columns.iter_deref().any(|candidate| {
        let candidate = candidate.read();
        !candidate.hidden && candidate.name.lowercase() == col_name
    }) {
        return Err(coded_refusal(
            tidb_error::mysql::errcode::ErrCantDropFieldOrKey,
            format!("column {column} doesn't exist"),
        ));
    }
    is_droppable_column(info, column, column_count)?;
    // Go `checkDropColumnWithForeignKeyConstraintInOwner`.
    if tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::SeqCst) {
        crate::foreign_key_build::check_drop_column_with_foreign_key_constraint(
            catalog, schema, info, &col_name,
        )
        .map_err(DdlPlanError::Admission)?;
    }
    check_drop_column_with_ttl_config(info, &col_name)
}

/// Go `isDroppableColumn` (`pkg/ddl/column.go`).
///
/// `column_count` is Go's `len(tblInfo.Columns)` at the check, which inside a
/// multi-schema change counts columns other sub-jobs are still adding.
fn is_droppable_column(
    info: &TableInfo,
    column: &str,
    column_count: usize,
) -> Result<(), DdlPlanError> {
    let col_name = column.go_to_lower();
    // Go `hasDependentByGeneratedColumn`, whose `dep` is `colName.L`.
    if let Some(dependent) = info
        .columns
        .iter_deref()
        .find(|candidate| candidate.read().dependences.contains(&col_name))
    {
        return Err(if dependent.read().hidden {
            coded_refusal(
                tidb_error::mysql::errcode::ErrDependentByFunctionalIndex,
                format!(
                    "Column '{col_name}' has an expression index dependency and cannot be \
                     dropped or renamed"
                ),
            )
        } else {
            coded_refusal(
                tidb_error::mysql::errcode::ErrDependentByGeneratedColumn,
                format!("Column '{col_name}' has a generated column dependency."),
            )
        });
    }
    if column_count == 1 {
        return Err(coded_refusal(
            tidb_error::mysql::errcode::ErrCantRemoveAllFields,
            format!(
                "can't drop only column {column} in table {}",
                info.name.original()
            ),
        ));
    }
    // Go `isColumnCanDropWithIndex`: `ErrCantDropColWithIndex`.
    for index in info.indices.iter_deref() {
        let index = index.read();
        if !(index.primary || index.columns.len() > 1 || index.is_columnar_index()) {
            continue;
        }
        if index
            .columns
            .iter_deref()
            .any(|indexed| indexed.read().name.lowercase() == col_name)
        {
            let covered = if index.is_columnar_index() {
                "with Columnar Index covered now"
            } else {
                "with composite index covered or Primary Key covered now"
            };
            return Err(coded_refusal(
                8200,
                format!("can't drop column {col_name} {covered}"),
            ));
        }
    }
    // Go `IsColumnDroppableWithCheckConstraint`: a CHECK that also names
    // another column cannot survive this DROP (3959). A single-column CHECK
    // is allowed; `table.LoadCheckConstraint` removes that now-invalid
    // metadata lazily when the post-DDL table is loaded.
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
        return Err(coded_refusal(error.code, error.message));
    }
    check_column_referenced_by_partial_condition(info, column)
}

/// Go `checkDropColumnWithPartitionConstraint` (`pkg/ddl/executor.go`): a
/// column the partitioning reads (`GetPartitionColumnNames`) cannot go.
fn check_drop_column_with_partition_constraint(
    info: &TableInfo,
    col_name: &str,
) -> Result<(), DdlPlanError> {
    if info.partition.is_none() {
        return Ok(());
    }
    let spec = stored_partition_spec(info)?;
    if spec
        .dependencies
        .iter()
        .any(|name| name.go_to_lower() == col_name)
    {
        return Err(coded_refusal(
            tidb_error::tidb::errcode::ErrDependentByPartitionFunctional,
            format!(
                "Column '{col_name}' has a partitioning function dependency and cannot be \
                 dropped or renamed"
            ),
        ));
    }
    Ok(())
}

/// Go `checkDropColumnWithTTLConfig` (`pkg/ddl/ttl.go`).
fn check_drop_column_with_ttl_config(info: &TableInfo, col_name: &str) -> Result<(), DdlPlanError> {
    if let Some(ttl) = &info.ttl_info {
        if ttl.read().column_name.lowercase() == col_name {
            return Err(coded_refusal(
                tidb_error::tidb::errcode::ErrTTLColumnCannotDrop,
                format!("Cannot drop column '{col_name}': needed in TTL config"),
            ));
        }
    }
    Ok(())
}

/// Go `checkColumnReferencedByPartialCondition` (`pkg/ddl/executor.go`).
fn check_column_referenced_by_partial_condition(
    info: &TableInfo,
    column: &str,
) -> Result<(), DdlPlanError> {
    let col_name = column.go_to_lower();
    for index in info.indices.iter_deref() {
        let index = index.read();
        if index
            .affect_column
            .iter_deref()
            .any(|affected| affected.read().name.lowercase() == col_name)
        {
            return Err(coded_refusal(
                tidb_error::tidb::errcode::ErrModifyColumnReferencedByPartialCondition,
                format!(
                    "Cannot drop, change or modify column '{column}': it is referenced in \
                     partial index '{}'",
                    index.name.original()
                ),
            ));
        }
    }
    Ok(())
}

/// Go `config.TableColumnCountLimit`.
fn table_column_count_limit() -> usize {
    tidb_config::config_tree::config::get_global_config().table_column_count_limit as usize
}

/// Go `checkTableInfoValidExtra` (`create_table.go`) over a table about to be
/// published, as `createTableWithInfoJob` runs it after the existence check:
/// the name, duplicate-column and count checks one port shares with the
/// in-process builder, then `checkTableInfoValid`'s `checkInvisibleIndexOnPK`.
fn check_table_info_valid_extra(info: &TableInfo) -> Result<(), DdlPlanError> {
    let columns = info
        .columns
        .iter_deref()
        .map(|column| column.read().name.original().to_owned())
        .collect::<Vec<_>>();
    tidb_executor::ddl::check_table_info_valid_extra(
        info.name.original(),
        columns.iter().map(String::as_str),
        info.indices.len(),
        table_column_count_limit(),
    )
    .map_err(|error| {
        let error = error.to_mysql_error();
        coded_refusal(error.code, error.message)
    })?;
    check_table_info_valid(info)
}

/// Go `checkTableInfoValid`, which `updateVersionAndTableInfoWithCheck` runs
/// on the table a job step leaves: the observable half is
/// `checkInvisibleIndexOnPK`, since a change can turn an invisible unique
/// index into the implicit primary key. (`TableFromMeta`'s rebuild is the
/// loader's, `build_generated_expr`.)
fn check_table_info_valid(info: &TableInfo) -> Result<(), DdlPlanError> {
    check_invisible_index_on_pk(info)
}

/// Go `checkInvisibleIndexOnPK` (`ddl/executor.go`).
fn check_invisible_index_on_pk(info: &TableInfo) -> Result<(), DdlPlanError> {
    if info.pk_is_handle {
        return Ok(());
    }
    if info
        .get_primary_key()
        .is_some_and(|primary| primary.read().invisible)
    {
        return Err(coded_refusal(
            tidb_error::tidb::errcode::ErrPKIndexCantBeInvisible,
            "A primary key index cannot be invisible",
        ));
    }
    Ok(())
}

/// Go `checkAddColumnTooManyColumns` (`ddl/column.go`).
fn check_add_column_too_many_columns(col_num: usize) -> Result<(), DdlPlanError> {
    if col_num > table_column_count_limit() {
        return Err(coded_refusal(
            tidb_error::mysql::errcode::ErrTooManyFields,
            "Too many columns",
        ));
    }
    Ok(())
}

/// Go `checkVisibleColumnCnt` (`pkg/ddl/executor.go`).
fn check_visible_column_cnt(
    info: &TableInfo,
    add_cnt: usize,
    drop_cnt: usize,
) -> Result<(), DdlPlanError> {
    let visible_column_cnt = info
        .columns
        .iter_deref()
        .filter(|column| !column.read().hidden)
        .count();
    if visible_column_cnt + add_cnt > drop_cnt {
        return Ok(());
    }
    if info.columns.len() > visible_column_cnt {
        // There are only invisible columns.
        return Err(coded_refusal(
            tidb_error::mysql::errcode::ErrTableMustHaveColumns,
            "A table must have at least 1 column",
        ));
    }
    Err(coded_refusal(
        tidb_error::mysql::errcode::ErrCantRemoveAllFields,
        "You can't delete all columns with ALTER TABLE; use DROP TABLE instead",
    ))
}

/// Go `checkOperateDropIndexUseByForeignKey` (`multi_schema_change.go`): an
/// index this ALTER drops cannot be the only one covering a key it adds.
fn check_operate_drop_index_use_by_foreign_key(
    tb_info: &TableInfo,
    drop_indexes: &[CiString],
    add_foreign_keys: &[Vec<CiString>],
) -> Result<(), DdlPlanError> {
    let (dropping_indexes, remain_indexes): (Vec<_>, Vec<_>) =
        tb_info.indices.iter_handles().partition(|index| {
            let index = index.as_ref().expect("nil *IndexInfo in TableInfo.Indices");
            let name = index.read().name.lowercase().to_owned();
            drop_indexes
                .iter()
                .any(|dropped| dropped.lowercase() == name)
        });
    let dropping_indexes = tidb_model::GoSharedPointerSlice::from_handles(dropping_indexes);
    let remain_indexes = tidb_model::GoSharedPointerSlice::from_handles(remain_indexes);
    for cols in add_foreign_keys {
        if let Some(dropping_idx) = tidb_model::index::find_index_by_columns_for_foreign_key(
            tb_info,
            &dropping_indexes,
            cols,
        ) {
            if tidb_model::index::find_index_by_columns_for_foreign_key(
                tb_info,
                &remain_indexes,
                cols,
            )
            .is_none()
            {
                return Err(coded_refusal(
                    tidb_error::mysql::errcode::ErrDropIndexNeededInForeignKey,
                    format!(
                        "Cannot drop index '{}': needed in a foreign key constraint",
                        dropping_idx.read().name
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Go `infoschema.ErrForeignKeyNotExists` (1091).
fn foreign_key_not_exists(name: &str) -> DdlPlanError {
    coded_refusal(
        tidb_error::mysql::errcode::ErrCantDropFieldOrKey,
        format!("Can't DROP '{name}'; check that column/key exists"),
    )
}

/// Go `dropForeignKey` (`foreign_key.go`) over an evolving `TableInfo`.
fn drop_foreign_key(info: &mut TableInfo, name: &str) -> Result<(), DdlPlanError> {
    let Some(position) = info
        .foreign_keys
        .iter_deref()
        .position(|fk| fk.read().name.lowercase() == name.go_to_lower())
    else {
        return Err(foreign_key_not_exists(name));
    };
    let kept = info
        .foreign_keys
        .iter_deref()
        .enumerate()
        .filter(|(offset, _)| *offset != position)
        .map(|(_, fk)| fk.read().clone())
        .collect::<Vec<_>>();
    info.foreign_keys = kept.into();
    Ok(())
}

/// Go `onDropColumn`'s metadata over an evolving `TableInfo`, once the
/// checks above have passed: the column goes, and so does every
/// single-column index on it (`listIndicesWithColumn`).
fn apply_drop_column(info: &mut TableInfo, column: &str) {
    let wanted = column.go_to_lower();
    let dropped_offset = info
        .columns
        .iter_deref()
        .position(|candidate| candidate.read().name.lowercase() == wanted)
        .expect("a checked DROP COLUMN names a column");
    let dropped = info
        .columns
        .iter_deref()
        .nth(dropped_offset)
        .expect("the position was just found")
        .read()
        .clone_like_go();
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
        _ => Err(DdlAdmissionError::with_code(
            GENERIC_ERROR_CODE,
            format!(
                "{what} name `{}` is not a `[schema.]name` path",
                path.join(".")
            ),
        )),
    }
}

fn lower_drop_table(
    drop: &DropTableStmt,
    default_schema: &str,
) -> Result<DdlStatement, DdlAdmissionError> {
    // The session owner has already handled local temporary targets and
    // validated temporary spelling. Only persistent targets reach this route.
    let mut names = Vec::with_capacity(drop.names.len());
    for name in &drop.names {
        names.push(split_name(name, default_schema, "table")?);
    }
    let [name] = drop.names.as_slice() else {
        // The execution owner submits targets individually and defers the
        // combined missing-name error or ordered notes until completion.
        return Ok(DdlStatement::DropTables {
            names,
            if_exists: drop.if_exists,
        });
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
        return Err(DdlAdmissionError::with_code(
            GENERIC_ERROR_CODE,
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
    // Prefix-length key parts (`INDEX(b(2))`) are admitted: the builder
    // stores the length on the `IndexColumn`, the write path cuts entries
    // (`index_prefix_cut::cut_index_value`), and the read path treats cut
    // endpoints as inclusive ranges. The catalog loader never validated the
    // length away — the old refusal here predated that support.
    // The session's `tidb_enable_clustered_index` (server default ON), which
    // the context carries; bootstrap builds under its own mode at its own
    // call site.
    let build = CreateTableBuild::new(create, &schema, context)?;
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
    primary: bool,
    context: &tidb_executor::StmtContext,
) -> Result<DdlStatement, DdlAdmissionError> {
    // go's `ADD PRIMARY KEY` lowers through the CreateIndex statement with
    // `primary = true`; the caller decides, since the AST's own
    // `CREATE INDEX` grammar has no PRIMARY spelling.
    let (schema, table) = split_name(&create.table, default_schema, "table")?;
    // Go `createIndex`'s hypo branch keeps the index in `SessionVars`. The
    // session answers `CREATE INDEX ... TYPE HYPO` itself; the ALTER TABLE
    // spelling has no session arm yet, and must not store a real index.
    if create.options.index_type == Some(tidb_ast::IndexType::HYPO) {
        return Err(DdlAdmissionError::unsupported(
            "a hypothetical index through ALTER TABLE is not supported yet",
        ));
    }
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
    let mut columns = Vec::with_capacity(create.parts.len());
    let mut expressions = Vec::with_capacity(create.parts.len());
    for part in &create.parts {
        match part {
            tidb_ast::IndexPart::Column {
                name, prefix_len, ..
            } => {
                columns.push(IndexColumn {
                    name: CiString::new(name.clone()),
                    // Resolved against the stored table when the change is
                    // planned; the declared prefix travels in `length` and is
                    // validated (and normalized) there, where the column's
                    // type is known.
                    offset: 0,
                    length: prefix_len.unwrap_or(-1),
                    ..IndexColumn::default()
                });
                expressions.push(None);
            }
            // An expression part indexes a hidden generated column the plan
            // builds once the index has its name (`_V$_<index>_<part>`).
            tidb_ast::IndexPart::Expr { expr, .. } => {
                columns.push(IndexColumn {
                    offset: 0,
                    length: -1,
                    ..IndexColumn::default()
                });
                expressions.push(Some(expr.clone()));
            }
        }
    }
    if columns.is_empty() {
        return Err(DdlAdmissionError::with_code(
            GENERIC_ERROR_CODE,
            "CREATE INDEX names no column",
        ));
    }
    Ok(DdlStatement::CreateIndex {
        schema,
        table,
        if_not_exists: create.if_not_exists,
        expressions,
        condition: create.options.condition.clone(),
        context: DdlStatementContext(context.clone()),
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
            primary,
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
    // Go keeps hypothetical indexes in `SessionVars`; the session answers
    // `DROP HYPO INDEX` before any statement reaches the catalog.
    if drop.is_hypo {
        return Err(DdlAdmissionError::unsupported(
            "DROP HYPO INDEX is session state and never reaches the catalog",
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
    /// A source-defined error whose class, code and message must survive the
    /// worker checkpoint. SQL projection belongs at the submitting connection.
    Source(TerrorError),
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
    /// `DROP TABLE a, b` naming tables that are not in the catalog (Go
    /// `ErrBadTable` 1051, every missing name comma-joined).
    UnknownTables(Vec<String>),
    /// Go `ErrCantDropFieldOrKey` (1091): DROP COLUMN/INDEX names something
    /// the table does not have.
    CantDropFieldOrKey(String),
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
            Self::Source(error) => fmt::Display::fmt(error, formatter),
            Self::Catalog(error) => write!(formatter, "{error}"),
            Self::UnknownDatabase(name) => write!(formatter, "Unknown database '{name}'"),
            Self::DatabaseExists(name) => {
                write!(formatter, "Can't create database '{name}'; database exists")
            }
            Self::UnknownTable { schema, table } => {
                write!(formatter, "Unknown table '{schema}.{table}'")
            }
            Self::UnknownTables(names) => {
                write!(formatter, "Unknown table '{}'", names.join(","))
            }
            Self::CantDropFieldOrKey(name) => {
                write!(
                    formatter,
                    "Can't DROP '{name}'; check that column/key exists"
                )
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
            Self::Encode(detail) => write!(formatter, "{detail}"),
            Self::Mutations(error) => write!(formatter, "{error}"),
        }
    }
}

impl DdlPlanError {
    /// The existing SQL contract is owned with the error, so direct execution
    /// and persisted action checkpoints cannot select different error numbers.
    pub(crate) fn mysql_error_code(&self) -> Option<u16> {
        use tidb_error::tidb::errcode;
        Some(match self {
            Self::Source(error) => error.to_sql_error().code,
            Self::UnknownDatabase(_) => errcode::ErrBadDB,
            Self::DatabaseExists(_) => errcode::ErrDBCreateExists,
            Self::UnknownTable { .. } | Self::UnknownTables(_) => errcode::ErrBadTable,
            Self::TableNotExists { .. } => errcode::ErrNoSuchTable,
            Self::TableExists { .. } => errcode::ErrTableExists,
            Self::DuplicateKeyName(_) => errcode::ErrDupKeyName,
            Self::DuplicateColumnName(_) => errcode::ErrDupFieldName,
            Self::UnknownIndexColumn { .. } => errcode::ErrKeyColumnDoesNotExits,
            Self::KeyNotExists { .. } => errcode::ErrKeyDoesNotExist,
            Self::CantDropFieldOrKey(_) | Self::UnknownIndex(_) => errcode::ErrCantDropFieldOrKey,
            Self::UnknownColumn { .. } => errcode::ErrBadField,
            Self::InvalidAutoRandom(_) => errcode::ErrInvalidAutoRandom,
            Self::AutoIdReadFailed => errcode::ErrAutoincReadFailed,
            Self::Admission(error) => error.code,
            Self::Unsupported(_) => errcode::ErrUnknown,
            Self::Catalog(_)
            | Self::Encode(_)
            | Self::Mutations(_)
            | Self::GlobalIdExhausted { .. } => return None,
        })
    }

    /// Durable job error. Coded legacy errors retain their numeric envelope;
    /// uncoded errors follow Go `toTError`'s DDL/CodeUnknown fallback.
    /// Source RFC identities must be carried from their producers, not guessed
    /// from a MySQL number shared by several Go error classes.
    pub fn to_job_error(&self) -> tidb_error::terror::TerrorError {
        if let Self::Source(error) = self {
            return error.clone();
        }
        match self.mysql_error_code() {
            Some(code) => tidb_error::terror::TerrorError::compatible(
                tidb_error::terror::TerrorCode::new(
                    isize::try_from(code).expect("u16 error code fits isize"),
                ),
                self.to_string(),
            ),
            None => unknown_ddl_job_error(self.to_string()),
        }
    }

    /// Converts a direct refusal through the same contract as durable history.
    pub fn to_sql_error(&self) -> tidb_error::mysql::SqlError {
        ddl_job_error_to_sql_error(&self.to_job_error())
    }

    fn is_source_error(&self, prototype: &TerrorError) -> bool {
        matches!(self, Self::Source(error) if prototype.equal(Some(error)))
    }
}

fn unknown_ddl_job_error(message: impl Into<String>) -> tidb_error::terror::TerrorError {
    tidb_error::terror::TerrorError::synthesize(
        tidb_error::terror::TerrorClass::Ddl,
        tidb_error::terror::CODE_UNKNOWN,
        message,
    )
}

/// Projects the durable DDL error envelope onto SQL. Legacy Rust job rows have
/// numeric codes without RFC identities; they must remain readable after restart.
/// SQLSTATE comes from the shared Go MySQL catalog, never a transport-local map.
pub fn ddl_job_error_to_sql_error(
    error: &tidb_error::terror::TerrorError,
) -> tidb_error::mysql::SqlError {
    let code = u16::try_from(error.code().value()).unwrap_or(tidb_error::tidb::errcode::ErrUnknown);
    tidb_error::mysql::SqlError {
        code,
        state: tidb_error::mysql::mysql_state(code),
        message: error.message().to_owned(),
    }
}

impl std::error::Error for DdlPlanError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            Self::Admission(error) => Some(error),
            Self::Catalog(error) => Some(error),
            Self::Mutations(error) => Some(error),
            _ => None,
        }
    }
}

impl From<TerrorError> for DdlPlanError {
    fn from(error: TerrorError) -> Self {
        Self::Source(error)
    }
}

impl From<DdlAdmissionError> for DdlPlanError {
    fn from(error: DdlAdmissionError) -> Self {
        Self::Admission(error)
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
        /// The warnings the statement raises even though it changed nothing:
        /// go notes 1007/1051, errors 1105. A statement can raise several
        /// (`DROP TABLE IF EXISTS a, b` on two missing tables leaves one
        /// 1051 note per table).
        warnings: Vec<(DdlWarningLevel, u16, String)>,
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
    /// Action error for the worker's post-commit retry policy. SQL results
    /// come from durable history, not this step's return value.
    pub run_error: Option<DdlPlanError>,
    /// Error count saved in this checkpoint, including the current action.
    pub error_count: i64,
}

// Action handlers only plan metadata and update the in-memory job. The worker
// owns durable queue updates and cancellation. A handled rollback error may
// accompany metadata that Go deliberately keeps rather than resetting.
struct PersistedDdlActionStep {
    write: DdlWrite,
    update_raw_args: bool,
    run_error: Option<DdlPlanError>,
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
                tidb_executor::ddl::check_constraint::CheckConstraintBuildMode::Alter,
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
                .find(|constraint| constraint.read().name.lowercase() == name.go_to_lower())
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
                .find(|constraint| constraint.read().name.lowercase() == name.go_to_lower())
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
    job.schema_name = schema.go_to_lower().into();
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

/// Schema acknowledgements belong to one owner run, like Go jobContext. A new
/// owner starts without acknowledgements and recovers durable schema state.
#[derive(Clone, Copy, Debug)]
pub struct DdlJobSchemaState {
    /// Go IsMDLEnabled, including the next-generation kernel override.
    pub mdl_enabled: bool,
    /// Last version acknowledged by this owner run.
    pub synced_version: Option<i64>,
    /// Version committed by this run that still needs acknowledgement.
    pub pending_version: Option<i64>,
}

impl DdlJobSchemaState {
    /// Starts a fresh owner run in the selected source mode.
    pub fn new(mdl_enabled: bool) -> Self {
        Self {
            mdl_enabled,
            synced_version: None,
            pending_version: None,
        }
    }
}

/// A persisted job cannot advance while its previous schema publication is
/// unacknowledged. Go's scheduler recovers this barrier before calling a worker.
#[derive(Clone, Debug)]
pub enum PersistedDdlJobPlan {
    /// Release the worker without finishing the job or rewriting its row.
    Paused,
    /// Resume schema synchronization before another action or history write.
    SchemaSync {
        /// Committed version, or the latest nonempty diff on non-MDL recovery.
        version: i64,
        /// MDL-only durable table scope to clean after acknowledgement.
        mdl_info: Option<MdlInfoUpdate>,
        /// Non-MDL recovery republishes the latest version before waiting.
        publish_version: bool,
    },
    /// One action or history transaction, planned from the current active row.
    Step(PersistedDdlJobStep),
}

/// Actions currently integrated into the persisted owner worker. Seed planners
/// outside this set are not made live by the common lifecycle.
pub fn supports_persisted_ddl_job(action: ActionType) -> bool {
    matches!(
        action,
        ActionType::ACTION_ADD_CHECK_CONSTRAINT
            | ActionType::ACTION_DROP_CHECK_CONSTRAINT
            | ActionType::ACTION_ALTER_CHECK_CONSTRAINT
            | ActionType::ACTION_CREATE_SCHEMA
            | ActionType::ACTION_CREATE_TABLE
            | ActionType::ACTION_CREATE_TABLES
            | ActionType::ACTION_RENAME_TABLES
            | ActionType::ACTION_DROP_SCHEMA
            | ActionType::ACTION_DROP_TABLE
    )
}

/// Plans Go's shared worker lifecycle: recover the selected schema barrier,
/// execute one action, then move a completed job to history after synchronization.
/// A restarted owner supplies a fresh schema state and recovers the barrier.
pub fn plan_persisted_ddl_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
    ddl_job_id: i64,
    start_ts: u64,
    schema_state: DdlJobSchemaState,
    load_error_count_limit: &dyn Fn() -> i64,
) -> Result<PersistedDdlJobPlan, DdlPlanError> {
    plan_persisted_ddl_job_with(
        snapshot,
        ddl_job_id,
        start_ts,
        schema_state,
        load_error_count_limit,
        supports_persisted_ddl_job,
        |_snapshot, catalog, active| {
            let action = active.job.type_;
            match action {
                ActionType::ACTION_ADD_CHECK_CONSTRAINT
                | ActionType::ACTION_DROP_CHECK_CONSTRAINT
                | ActionType::ACTION_ALTER_CHECK_CONSTRAINT => {
                    plan_persisted_check_constraint_job_step(catalog, active, start_ts)
                }
                ActionType::ACTION_CREATE_SCHEMA => {
                    plan_persisted_create_schema_job_step(catalog, active)
                }
                ActionType::ACTION_CREATE_TABLE => {
                    plan_persisted_create_table_job_step(catalog, active, start_ts)
                }
                ActionType::ACTION_CREATE_TABLES => {
                    plan_persisted_create_tables_job_step(catalog, active, start_ts)
                }
                ActionType::ACTION_RENAME_TABLES => {
                    plan_persisted_rename_tables_job_step(catalog, active, start_ts)
                }
                ActionType::ACTION_DROP_SCHEMA => {
                    plan_persisted_drop_schema_job_step(catalog, active)
                }
                ActionType::ACTION_DROP_TABLE => {
                    plan_persisted_drop_table_job_step(catalog, active)
                }
                _ => unreachable!("supported action checked above"),
            }
        },
    )
}

// All persisted actions share the queue, MDL recovery and history owner. The
// explicit action predicate keeps seed entrypoints out of live dispatch.
fn plan_persisted_ddl_job_with<S: MetaSnapshot>(
    snapshot: &mut S,
    ddl_job_id: i64,
    start_ts: u64,
    schema_state: DdlJobSchemaState,
    load_error_count_limit: &dyn Fn() -> i64,
    supports_action: impl FnOnce(ActionType) -> bool,
    plan_action: impl FnOnce(
        &mut S,
        &ClusterCatalog,
        &mut crate::ddl_job_table::ActiveDdlJob,
    ) -> Result<PersistedDdlActionStep, DdlPlanError>,
) -> Result<PersistedDdlJobPlan, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    let job_table = crate::ddl_job_table::DdlJobTable::locate(&catalog)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    let mut active = job_table
        .load_by_id(snapshot, ddl_job_id)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?
        .ok_or_else(|| DdlPlanError::Encode(format!("DDL job {ddl_job_id} does not exist")))?;
    if !supports_action(active.job.type_) {
        return Err(DdlPlanError::Encode(format!(
            "unsupported persisted DDL action {}",
            active.job.type_
        )));
    }
    let mdl_info = if schema_state.mdl_enabled {
        let mut info = mdl_info_update(&catalog, active.job.table_id)?;
        info.table_ids = active.table_ids.clone();
        info.omit_owner_id =
            tidb_metadef::is_system_related_db(&active.job.schema_name.to_string().to_lowercase());
        match crate::ddl_systable::SystemTableManager::new(&catalog)
            .get_mdl_version(snapshot, ddl_job_id)
        {
            Ok(version) if Some(version) != schema_state.synced_version => {
                return Ok(PersistedDdlJobPlan::SchemaSync {
                    version,
                    mdl_info: Some(info),
                    publish_version: false,
                });
            }
            Ok(_) => {}
            Err(crate::ddl_systable::SystemTableManagerError::NotFound) => {}
            Err(error) => return Err(DdlPlanError::Encode(error.to_string())),
        }
        Some(info)
    } else {
        // Go waitVersionSyncedWithoutMDL recovers from a fresh snapshot, not
        // from an MDL row or merely the job's possibly older LastSchemaVersion.
        let can_sync = matches!(
            active.job.state,
            JobState::RUNNING | JobState::ROLLINGBACK | JobState::DONE | JobState::ROLLBACK_DONE
        );
        if can_sync {
            if let Some(version) = schema_state.pending_version {
                return Ok(PersistedDdlJobPlan::SchemaSync {
                    version,
                    mdl_info: None,
                    publish_version: false,
                });
            }
            if schema_state.synced_version.is_none() && active.job.last_schema_version > 0 {
                let mut version = catalog.schema_version;
                // Same projection as meta.GetSchemaVersionWithNonEmptyDiff:
                // a separately reserved newest version may not have a diff yet.
                if version > 0 {
                    let bytes = snapshot.get(&key::schema_diff_kv_key(version))?;
                    let diff = bytes
                        .as_deref()
                        .map(value::parse_schema_diff)
                        .transpose()
                        .map_err(|error| DdlPlanError::Encode(error.to_string()))?
                        .flatten();
                    if diff.is_none() {
                        version -= 1;
                    }
                }
                if version > 0 {
                    return Ok(PersistedDdlJobPlan::SchemaSync {
                        version,
                        mdl_info: None,
                        publish_version: true,
                    });
                }
            }
        }
        None
    };
    if matches!(
        active.job.state,
        JobState::DONE | JobState::ROLLBACK_DONE | JobState::CANCELLED
    ) {
        return finish_persisted_ddl_job(snapshot, &catalog, &job_table, &mut active, start_ts)
            .map(PersistedDdlJobPlan::Step);
    }

    // Go processJobPausingRequest runs before action dispatch. The pause
    // signal is control flow, not an action error: keep the prior error and
    // undecoded raw args, and never publish a schema or move to history.
    if active.job.is_paused() {
        return Ok(PersistedDdlJobPlan::Paused);
    }

    if active.job.real_start_ts == 0 {
        active.job.real_start_ts = start_ts;
    }
    if active.job.is_pausing() {
        active.job.state = JobState::PAUSED;
        let mut mutations = Vec::new();
        job_table
            .append_update(&mut active, false, &mut mutations)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
        return Ok(PersistedDdlJobPlan::Step(PersistedDdlJobStep {
            write: job_only_write(ddl_job_id, mutations),
            terminal: false,
            run_error: None,
            error_count: active.job.error_count,
        }));
    }
    let cancelling = active.job.is_cancelling();
    let was_rollingback = active.job.is_rollingback();
    let previous_schema_version = active.job.last_schema_version;
    let outcome = recover_ddl_action(ddl_job_id, || {
        if cancelling {
            plan_cancel_persisted_ddl_job(&catalog, &mut active, start_ts)
        } else {
            if !active.job.is_rollingback() {
                active.job.state = JobState::RUNNING;
            }
            plan_action(snapshot, &catalog, &mut active)
        }
    });
    let panicked = outcome.is_err();
    let mut step = match outcome {
        Ok(Ok(step)) => step,
        Ok(Err(error)) => PersistedDdlActionStep {
            write: job_only_write(ddl_job_id, Vec::new()),
            update_raw_args: !was_rollingback && active.job.is_rollingback(),
            run_error: Some(error),
        },
        Err(()) => {
            // An interrupted planner has no completed metadata write set.
            // Go's recovered runOneJobStep also skips LastSchemaVersion and
            // raw-argument publication after the panicking action call.
            active.job.last_schema_version = previous_schema_version;
            count_ddl_action_panic(&mut active.job, load_error_count_limit);
            job_control_step(&active.job)
        }
    };
    if cancelling {
        step.update_raw_args = active.job.is_rollingback();
    } else if !panicked {
        active.job.last_schema_version = step.write.diff.version;
    }
    if let Some(error) = &step.run_error {
        if cancelling {
            record_ddl_cancellation_error(&mut active.job, error, load_error_count_limit);
        } else {
            count_ddl_action_error(&mut active.job, error, load_error_count_limit);
        }
    }
    // Go resets the action's transaction before handleJobDone on cancellation.
    // This also covers rollback handlers that cancel without a fresh error.
    if active.job.state == JobState::CANCELLED {
        return finish_persisted_ddl_job(snapshot, &catalog, &job_table, &mut active, start_ts)
            .map(PersistedDdlJobPlan::Step);
    }
    // Go transitOneJobStep discards failed forward metadata, but keeps a
    // handled rollback's mutations. The job/error checkpoint survives both.
    if step.run_error.is_some() && !active.job.is_rollingback() && !active.job.is_rollback_done() {
        step.write = job_only_write(ddl_job_id, Vec::new());
    }
    job_table
        .append_update(&mut active, step.update_raw_args, &mut step.write.mutations)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    // Only a new diff publishes a schema version; cancellation is not a
    // publication of whatever catalog version happened to be read.
    step.write.schema_version = step.write.diff.version;
    if step.write.schema_version != 0 {
        step.write.mdl_info_update = mdl_info;
    }
    Ok(PersistedDdlJobPlan::Step(PersistedDdlJobStep {
        write: step.write,
        terminal: false,
        run_error: step.run_error,
        error_count: active.job.error_count,
    }))
}

// Go handleJobDone/finishDDLJob. Cancellation calls this in its current
// transaction after discarding action mutations; DONE/ROLLBACK_DONE reach it
// only after the shared MDL barrier. No action handler writes history itself.
fn finish_persisted_ddl_job<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    job_table: &crate::ddl_job_table::DdlJobTable,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlJobStep, DdlPlanError> {
    if active.job.state == JobState::DONE {
        active.job.state = JobState::SYNCED;
    }
    if let Some(binlog) = active.job.binlog_info.as_ref() {
        binlog.write().finished_ts = start_ts;
    }
    active.job.sequence_number = DDL_HISTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed) + 1;
    // Preserve the submitted raw args, including invalid args on cancellation.
    // Completed jobs have a freshly decoded, empty private Args field.
    let encoded = active
        .job
        .encode(false)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    let mut mutations = Vec::new();
    crate::ddl_history_table::DdlHistoryTable::locate(catalog)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?
        .append_insert_ignore(snapshot, &active.job, &encoded, &mut mutations)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    mutations.push(BufferMutation::set(
        key::ddl_job_history_kv_key(active.job.id),
        encoded,
    )?);
    job_table
        .append_delete(active, &mut mutations)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    Ok(PersistedDdlJobStep {
        write: job_only_write(active.job.id, mutations),
        terminal: true,
        run_error: None,
        error_count: active.job.error_count,
    })
}

/// Plans one CHECK worker step from `mysql.tidb_ddl_job` and current table
/// metadata, matching pinned Go `runOneJobStep` plus the three CHECK action
/// handlers.
///
/// The active row is the operation authority; no statement-local continuation
/// is accepted. A process may therefore disappear after any committed phase
/// and a later owner can call this function with the same job ID.
fn plan_persisted_check_constraint_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let ddl_job_id = active.job.id;

    let stored = checked_job_table(catalog, active, true)?;
    let database = catalog
        .databases
        .iter()
        .find(|db| db.info.id == active.job.schema_id)
        .expect("checked_job_table checked the database");
    let cancelling = active.job.is_cancelling();
    let mut info = stored.clone_like_go();
    let mut validation = None;
    let mut terminal = false;
    let mut schema_changed = true;

    match active.job.type_ {
        ActionType::ACTION_ADD_CHECK_CONSTRAINT => {
            let args = tidb_model::get_add_check_constraint_args(&mut active.job)
                .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
                .ok_or_else(|| DdlPlanError::Encode("ADD CHECK job has nil args".to_owned()))?;
            let constraint_handle = args.read().constraint.get().ok_or_else(|| {
                DdlPlanError::Encode("ADD CHECK job has nil constraint".to_owned())
            })?;
            let mut wanted = constraint_handle.read().name.lowercase().to_owned();
            let mut position = info
                .constraints
                .iter_deref()
                .position(|constraint| constraint.read().name.lowercase() == wanted);

            if let Some(position) = position {
                if info.constraints.get(position).unwrap().read().state == SchemaState::PUBLIC {
                    return Err(cancel_ddl_job(
                        active,
                        DdlPlanError::from(
                            tidb_util::dbterror::CLASS_SCHEMA
                                .new_std(tidb_error::tidb::errcode::ErrDupFieldName)
                                .generate_with_stack_by_args(&[FormatArg::from(
                                    constraint_handle.read().name.original(),
                                )]),
                        ),
                    ));
                }
            }
            // Go checkConstraintNamesNotExists skips the submitted pointer
            // after its first WRITE_ONLY publication, not based on the current
            // metadata state. This also governs cancellation conversion.
            if constraint_handle.read().state != SchemaState::WRITE_ONLY
                && database.tables.iter().any(|table| {
                    table
                        .constraints
                        .iter_deref()
                        .any(|constraint| constraint.read().name.lowercase() == wanted)
                })
            {
                return Err(cancel_ddl_job(
                    active,
                    DdlPlanError::from(
                        tidb_util::dbterror::CLASS_SCHEMA
                            .new_std(tidb_error::tidb::errcode::ErrCheckConstraintDupName)
                            .generate_with_stack_by_args(&[FormatArg::from(wanted.as_str())]),
                    ),
                ));
            }
            if cancelling || active.job.state == JobState::ROLLINGBACK {
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
                    if !cancelling {
                        active.job.state = JobState::ROLLBACK_DONE;
                        terminal = true;
                    }
                } else {
                    return Err(cancelled_ddl_job(active));
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
                        return Err(DdlPlanError::from(
                            tidb_util::dbterror::CLASS_SCHEMA
                                .new_std(tidb_error::tidb::errcode::ErrCheckConstraintDupName)
                                .generate_with_stack_by_args(&[FormatArg::from(
                                    constraint.name.lowercase(),
                                )]),
                        ));
                    }
                    for dependency in &constraint.constraint_cols {
                        if !info.columns.iter_deref().any(|column| {
                            let column = column.read();
                            column.state == SchemaState::PUBLIC
                                && column.name.lowercase() == dependency.lowercase()
                        }) {
                            return Err(DdlPlanError::from(
                                tidb_util::dbterror::ERR_TABLE_CHECK_CONSTRAINT_REFER_UNKNOWN
                                    .generate_with_stack_by_args(&[
                                        FormatArg::from(constraint.name.original()),
                                        FormatArg::from(dependency.original()),
                                    ]),
                            ));
                        }
                    }
                    *constraint_handle.write() = constraint.clone();
                    // Go puts the same ConstraintInfo pointer in job.Args and
                    // TableInfo. Its initial WRITE_ONLY state must survive
                    // argument encoding for subsequent validation/rollback.
                    info.constraints
                        .push_handle_go(Some(constraint_handle.clone()));
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
                            return Err(DdlPlanError::from(
                                tidb_util::dbterror::ERR_INVALID_DDL_STATE
                                    .generate_with_stack_by_args(&[
                                        FormatArg::from("constraint"),
                                        FormatArg::new(
                                            state.to_string(),
                                            state.0.to_string(),
                                            "model.SchemaState",
                                        ),
                                    ]),
                            ));
                        }
                    }
                }
            }
        }
        ActionType::ACTION_DROP_CHECK_CONSTRAINT => {
            let args = tidb_model::get_check_constraint_args(&mut active.job)
                .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
                .ok_or_else(|| DdlPlanError::Encode("DROP CHECK job has nil args".to_owned()))?;
            let wanted = args.read().constraint_name.get().lowercase().to_owned();
            let position = info
                .constraints
                .iter_deref()
                .position(|constraint| constraint.read().name.lowercase() == wanted)
                .ok_or_else(|| {
                    cancel_ddl_job(
                        active,
                        DdlPlanError::from(
                            tidb_util::dbterror::ERR_CONSTRAINT_NOT_FOUND
                                .generate_with_stack_by_args(&[FormatArg::from(
                                    args.read().constraint_name.get().original(),
                                )]),
                        ),
                    )
                })?;
            let state = info
                .constraints
                .get(position)
                .expect("DROP constraint position exists")
                .read()
                .state;
            if cancelling {
                if state == SchemaState::PUBLIC {
                    return Err(cancelled_ddl_job(active));
                }
                active.job.state = JobState::RUNNING;
                return Ok(job_control_step(&active.job));
            }
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
                    _ => {
                        return Err(DdlPlanError::from(
                            tidb_util::dbterror::ERR_INVALID_DDL_JOB.generate_with_stack_by_args(
                                &[
                                    FormatArg::from("constraint"),
                                    FormatArg::new(
                                        info.state.to_string(),
                                        info.state.0.to_string(),
                                        "model.SchemaState",
                                    ),
                                ],
                            ),
                        ));
                    }
                }
            }
        }
        ActionType::ACTION_ALTER_CHECK_CONSTRAINT => {
            let args = tidb_model::get_check_constraint_args(&mut active.job)
                .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
                .ok_or_else(|| DdlPlanError::Encode("ALTER CHECK job has nil args".to_owned()))?;
            let args = args.read();
            let wanted = args.constraint_name.get().lowercase().to_owned();
            let enforced = args.enforced.get();
            let position = info
                .constraints
                .iter_deref()
                .position(|constraint| constraint.read().name.lowercase() == wanted)
                .ok_or_else(|| {
                    cancel_ddl_job(
                        active,
                        DdlPlanError::from(
                            tidb_util::dbterror::ERR_CONSTRAINT_NOT_FOUND
                                .generate_with_stack_by_args(&[FormatArg::from(
                                    args.constraint_name.get().original(),
                                )]),
                        ),
                    )
                })?;
            let handle = info
                .constraints
                .get(position)
                .expect("ALTER constraint position exists");
            let mut constraint = handle.write();
            if cancelling || active.job.state == JobState::ROLLINGBACK {
                if constraint.state == SchemaState::PUBLIC {
                    return Err(cancelled_ddl_job(active));
                } else {
                    constraint.enforced = !enforced;
                    constraint.state = SchemaState::PUBLIC;
                    if !cancelling {
                        active.job.state = JobState::ROLLBACK_DONE;
                        terminal = true;
                    }
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
                    _ => {
                        // Go's enforcing switch has no default mutation/error.
                        // Only checkpoint the unchanged job in this state.
                        schema_changed = false;
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
        mutations.push(BufferMutation::set(
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
        mutations.push(BufferMutation::set(
            key::schema_version_kv_key(),
            value::encode_int_value(schema_version),
        )?);
        mutations.push(BufferMutation::set(
            key::schema_diff_kv_key(schema_version),
            value::serialize_schema_diff(&diff)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?);
        active.job.last_schema_version = schema_version;
        diff
    } else {
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
    }

    Ok(PersistedDdlActionStep {
        write: DdlWrite {
            ddl_job_id,
            mutations,
            schema_version,
            diff,
            created_id: None,
            backfill: Vec::new(),
            exchange_partition_validation: None,
            check_constraint_validation: validation,
            column_reorgs: Vec::new(),
            foreign_key_checks: Vec::new(),
            cache_table_check: None,
            partition_reorg: None,
            mdl_info_update: None,
            exchange_partition_label_swap: None,
            warnings: Vec::new(),
            placement_bundles: Vec::new(),
            placement_rollback_bundles: Vec::new(),
        },
        // Go runOneJobStep re-encodes decoded arguments after any successful
        // action, independently of whether the schema version changed.
        update_raw_args: true,
        run_error: None,
    })
}

/// Plans one execution step of a persisted `ACTION_CREATE_SCHEMA` job.
///
/// Pinned Go `onCreateSchema` (`pkg/ddl/schema.go`): the submitter already
/// packed the full `DBInfo` (name, charset, collate, placement) into the
/// job's raw args and allocated `job.SchemaID`; the owner re-stamps the ID,
/// refuses the job when either the name or the ID is already taken (Go
/// `checkSchemaNotExists` -- that path cancels the job), bumps the schema
/// version with a `CREATE SCHEMA` diff, publishes the database `PUBLIC` in
/// one meta write, and persists DONE for the shared synchronization lifecycle.
/// CREATE DATABASE has no reorg states or backfill.
fn plan_persisted_create_schema_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let ddl_job_id = active.job.id;

    let args = tidb_model::get_create_schema_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| DdlPlanError::Encode("CREATE SCHEMA job has nil args".to_owned()))?;
    let submitted = args
        .read()
        .db_info
        .get()
        .ok_or_else(|| DdlPlanError::Encode("CREATE SCHEMA job has nil db_info".to_owned()))?
        .read()
        .clone();
    let mut db_info = submitted;
    // Go `onCreateSchema`: the owner re-stamps the ID from the job.
    db_info.id = active.job.schema_id;

    let name_taken = find_database(catalog, db_info.name.original()).is_some();
    let id_taken = catalog
        .databases
        .iter()
        .any(|database| database.info.id == db_info.id);
    if name_taken || id_taken {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_util::dbterror::CLASS_SCHEMA
                    .new_std(tidb_error::tidb::errcode::ErrDBCreateExists)
                    .generate(format!(
                        "Can't create database '{}'; database exists",
                        db_info.name.original()
                    )),
            ),
        ));
    }

    let schema_version = catalog.schema_version + 1;
    let mut mutations = Vec::new();
    let diff = {
        db_info.state = SchemaState::PUBLIC;
        mutations.push(BufferMutation::set(
            key::database_kv_key(db_info.id),
            value::serialize_db_info(&db_info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?);
        let diff = SchemaDiff {
            version: schema_version,
            action_type: active.job.type_,
            schema_id: db_info.id,
            ..SchemaDiff::default()
        };
        mutations.push(BufferMutation::set(
            key::schema_version_kv_key(),
            value::encode_int_value(schema_version),
        )?);
        mutations.push(BufferMutation::set(
            key::schema_diff_kv_key(schema_version),
            value::serialize_schema_diff(&diff)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?);
        active.job.last_schema_version = schema_version;
        diff
    };

    active.job.finish_db_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        schema_version,
        Some(GoShared::new(db_info.clone())),
    );

    Ok(PersistedDdlActionStep {
        write: DdlWrite {
            ddl_job_id,
            mutations,
            schema_version,
            diff,
            created_id: Some(db_info.id),
            backfill: Vec::new(),
            exchange_partition_validation: None,
            check_constraint_validation: None,
            column_reorgs: Vec::new(),
            foreign_key_checks: Vec::new(),
            cache_table_check: None,
            partition_reorg: None,
            mdl_info_update: None,
            exchange_partition_label_swap: None,
            warnings: Vec::new(),
            placement_bundles: Vec::new(),
            placement_rollback_bundles: Vec::new(),
        },
        update_raw_args: true,
        run_error: None,
    })
}

/// Plans one execution step of a persisted `ACTION_CREATE_TABLE` job.
///
/// Pinned Go `onCreateTable` fast path (`pkg/ddl/table.go`): the submitter
/// packed the full `TableInfo` into the job's raw args and allocated
/// `job.TableID`; the owner re-stamps the ID, refuses the job when the table
/// already exists in the target schema (Go `checkTableNotExists` cancels),
/// publishes the table `PUBLIC` in one meta write, bumps the schema version
/// with a `CREATE TABLE` diff, and persists DONE. Synchronization and history
/// belong to the shared worker lifecycle.
fn plan_persisted_create_table_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let ddl_job_id = active.job.id;

    let args = tidb_model::get_create_table_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| DdlPlanError::Encode("CREATE TABLE job has nil args".to_owned()))?;
    let submitted = args
        .read()
        .table_info
        .get()
        .ok_or_else(|| DdlPlanError::Encode("CREATE TABLE job has nil table_info".to_owned()))?
        .read()
        .clone();
    let mut table_info = submitted;
    // Go `onCreateTable`: the owner re-stamps the ID from the job.
    table_info.id = active.job.table_id;

    let database = catalog
        .databases
        .iter()
        .find(|database| database.info.id == active.job.schema_id)
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::from(
                    tidb_util::dbterror::CLASS_SCHEMA
                        .new_std(tidb_error::tidb::errcode::ErrBadDB)
                        .generate(format!(
                            "Unknown database '(Schema ID {})'",
                            active.job.schema_id
                        )),
                ),
            )
        })?;
    let name_taken = database
        .tables
        .iter()
        .any(|table| table.name.lowercase() == table_info.name.lowercase());
    let id_taken = database
        .tables
        .iter()
        .any(|table| table.id == table_info.id);
    if name_taken || id_taken {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_util::dbterror::CLASS_SCHEMA
                    .new_std(tidb_error::tidb::errcode::ErrTableExists)
                    .generate(format!(
                        "Table '{}.{}' already exists",
                        database.info.name.original(),
                        table_info.name.original()
                    )),
            ),
        ));
    }

    let schema_version = catalog.schema_version + 1;
    let mut mutations = Vec::new();
    let diff = {
        table_info.state = SchemaState::PUBLIC;
        table_info.update_ts = start_ts;
        mutations.push(BufferMutation::set(
            key::table_kv_key(database.info.id, table_info.id),
            value::serialize_table_info(&table_info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?);
        let diff = SchemaDiff {
            version: schema_version,
            action_type: active.job.type_,
            schema_id: database.info.id,
            table_id: table_info.id,
            ..SchemaDiff::default()
        };
        mutations.push(BufferMutation::set(
            key::schema_version_kv_key(),
            value::encode_int_value(schema_version),
        )?);
        mutations.push(BufferMutation::set(
            key::schema_diff_kv_key(schema_version),
            value::serialize_schema_diff(&diff)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?);
        active.job.last_schema_version = schema_version;
        diff
    };

    active.job.finish_table_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        schema_version,
        Some(GoShared::new(table_info.clone())),
    );

    Ok(PersistedDdlActionStep {
        write: DdlWrite {
            ddl_job_id,
            mutations,
            schema_version,
            diff,
            created_id: None,
            backfill: Vec::new(),
            exchange_partition_validation: None,
            check_constraint_validation: None,
            column_reorgs: Vec::new(),
            foreign_key_checks: Vec::new(),
            cache_table_check: None,
            partition_reorg: None,
            mdl_info_update: None,
            exchange_partition_label_swap: None,
            warnings: Vec::new(),
            placement_bundles: Vec::new(),
            placement_rollback_bundles: Vec::new(),
        },
        update_raw_args: true,
        run_error: None,
    })
}

/// Plans one execution step of a persisted `ACTION_CREATE_TABLES` job (the
/// batch form BR restore submits: one job carrying every restored table).
///
/// Pinned Go `onCreateTables` (`pkg/ddl/create_table.go`): the submitter
/// allocated every `TableInfo.ID` up front and packed the per-table args
/// (`tables: [{table_info, fk_check}, ...]`) into the job's raw args; the
/// owner re-checks the target schema and each table's name/ID against the
/// catalog (any conflict cancels the whole job, Go cancels on the first
/// failing table), publishes every table `PUBLIC` in one meta write, bumps
/// the schema version once with a `CREATE TABLES` diff, and persists DONE for
/// the shared synchronization and history lifecycle.
fn plan_persisted_create_tables_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let ddl_job_id = active.job.id;

    // Pinned Go `GetBatchCreateTableArgs`: the v2 envelope is
    // `{"tables": [{"table_info": <Go TableInfo JSON>, "fk_check": bool}]}`
    // decoded through the same serde surface as the single-table args.
    let args = tidb_model::get_batch_create_table_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| DdlPlanError::Encode("CREATE TABLES job has nil args".to_owned()))?;

    let database = catalog
        .databases
        .iter()
        .find(|database| database.info.id == active.job.schema_id)
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::from(
                    tidb_util::dbterror::CLASS_SCHEMA
                        .new_std(tidb_error::tidb::errcode::ErrBadDB)
                        .generate(format!(
                            "Unknown database '(Schema ID {})'",
                            active.job.schema_id
                        )),
                ),
            )
        })?;

    let handles: Vec<Option<GoShared<tidb_model::CreateTableArgs>>> =
        args.read().tables.get().handles();
    let mut table_infos = Vec::new();
    let mut cancel_reason: Option<String> = None;
    for handle in handles {
        let Some(table_args) = handle else {
            cancel_reason = Some("CREATE TABLES job carries a nil table args entry".to_owned());
            break;
        };
        let Some(info_handle) = table_args.read().table_info.get() else {
            cancel_reason =
                Some("CREATE TABLES job carries a table with nil table_info".to_owned());
            break;
        };
        let mut table_info = info_handle.read().clone();
        // Go `onCreateTables`: the submitter already allocated every
        // `TableInfo.ID` (the batch job's own TableID stays 0); the owner
        // keeps the submitted IDs verbatim.
        let name_taken = database
            .tables
            .iter()
            .any(|table| table.name.lowercase() == table_info.name.lowercase());
        let id_taken = database
            .tables
            .iter()
            .any(|table| table.id == table_info.id);
        if name_taken || id_taken {
            cancel_reason = Some(format!(
                "table '{}.{}' already exists (id {})",
                database.info.name.original(),
                table_info.name.original(),
                table_info.id
            ));
            break;
        }
        table_infos.push(table_info);
    }

    if let Some(reason) = cancel_reason {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_util::dbterror::CLASS_SCHEMA
                    .new_std(tidb_error::tidb::errcode::ErrTableExists)
                    .generate(reason),
            ),
        ));
    }

    let schema_version = catalog.schema_version + 1;
    let mut mutations = Vec::new();
    let first_id = table_infos.first().map(|info| info.id).unwrap_or(0);
    let diff = {
        for info in &mut table_infos {
            info.state = SchemaState::PUBLIC;
            info.update_ts = start_ts;
            mutations.push(BufferMutation::set(
                key::table_kv_key(database.info.id, info.id),
                value::serialize_table_info(info)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?);
        }
        // Go `SetSchemaDiffForCreateTables` (`pkg/ddl/schema_version.go:36`):
        // one `AffectedOption` per created table, each naming its schema and
        // table IDs on both sides -- Go's `applyCreateTables` applies the
        // batch to every peer's infoschema strictly through these options.
        let mut diff = SchemaDiff {
            version: schema_version,
            action_type: active.job.type_,
            schema_id: database.info.id,
            table_id: first_id,
            ..SchemaDiff::default()
        };
        diff.affected_options = GoSharedPointerSlice::from(
            table_infos
                .iter()
                .map(|info| tidb_model::AffectedOption {
                    schema_id: database.info.id,
                    old_schema_id: database.info.id,
                    table_id: info.id,
                    old_table_id: info.id,
                    ..tidb_model::AffectedOption::default()
                })
                .collect::<Vec<_>>(),
        );
        mutations.push(BufferMutation::set(
            key::schema_version_kv_key(),
            value::encode_int_value(schema_version),
        )?);
        mutations.push(BufferMutation::set(
            key::schema_diff_kv_key(schema_version),
            value::serialize_schema_diff(&diff)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?);
        active.job.last_schema_version = schema_version;
        diff
    };

    active.job.state = JobState::DONE;
    active.job.schema_state = SchemaState::PUBLIC;
    // Go onCreateTables uses SetTableInfos, including an empty batch;
    // selecting only the first table also leaves an empty batch RUNNING.
    active
        .job
        .binlog_info
        .as_ref()
        .ok_or_else(|| DdlPlanError::Encode("CREATE TABLES job has nil BinlogInfo".to_owned()))?
        .write()
        .set_table_infos(schema_version, &GoSharedPointerSlice::from(table_infos));

    Ok(PersistedDdlActionStep {
        write: DdlWrite {
            ddl_job_id,
            mutations,
            schema_version,
            diff,
            created_id: None,
            backfill: Vec::new(),
            exchange_partition_validation: None,
            check_constraint_validation: None,
            column_reorgs: Vec::new(),
            foreign_key_checks: Vec::new(),
            cache_table_check: None,
            partition_reorg: None,
            mdl_info_update: None,
            exchange_partition_label_swap: None,
            warnings: Vec::new(),
            placement_bundles: Vec::new(),
            placement_rollback_bundles: Vec::new(),
        },
        update_raw_args: true,
        run_error: None,
    })
}

/// Plans one execution step of a persisted `ACTION_RENAME_TABLES` job.
///
/// Pinned Go `onRenameTables` (`pkg/ddl/table.go:849`), a two-phase job.
/// Phase one (before `StatePublic`) moves every table from its old schema to
/// its new one -- delete the old table KV, stamp `tblInfo.Name =
/// args.NewTableName`, create under the new schema (Go `checkAndRenameTables`)
/// -- and bumps the schema version once with a `RENAME TABLES` diff (Go
/// `SetSchemaDiffForRenameTables`: the first rename rides `TableID` /
/// `SchemaID` / `OldSchemaID`, the rest land in `AffectedOpts`). The job then
/// sits at `StatePublic` in the queue. Phase two bumps the version once more
/// with the diff's old IDs pinned to the new schemas (Go
/// `finishJobRenameTables`), so peers re-resolve the tables, and finishes the
/// job with `FinishMultipleTableJob`. A missing source table cancels the job
/// with the recorded error (Go `GetTableInfoAndCancelFaultJob`); Go resolves
/// the table by name inside the old schema, so a stale job-side ID must not
/// fail the move.
fn plan_persisted_rename_tables_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let ddl_job_id = active.job.id;

    let args = tidb_model::get_rename_tables_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| DdlPlanError::Encode("RENAME TABLES job has nil args".to_owned()))?;
    let args = args.read();
    let rename_infos = args.rename_table_infos.get();
    let first = rename_infos
        .get(0)
        .ok_or_else(|| DdlPlanError::Encode("RENAME TABLES job has no entries".to_owned()))?;
    let first = first.read();

    // Resolve every entry: Go looks the table up by name inside the old
    // schema (`GetTableInfoAndCancelFaultJob`), so a job-side stale ID must
    // not fail a rename whose name still resolves.
    struct Move {
        old_schema_id: i64,
        new_schema_id: i64,
        table: tidb_model::TableInfo,
        new_name: CiString,
        old_table_id: i64,
    }
    let mut resolved: Vec<Move> = Vec::with_capacity(rename_infos.len());
    let mut failure: Option<DdlPlanError> = None;
    // Once phase one has moved the tables (`SchemaState == StatePublic`), the
    // same entries resolve against their NEW schema and NEW names instead (Go
    // `finishJobRenameTables` reloads the tables under `info.NewSchemaID`).
    let phase_two = active.job.schema_state == SchemaState::PUBLIC;
    for index in 0..rename_infos.len() {
        let info = rename_infos
            .get(index)
            .ok_or_else(|| DdlPlanError::Encode("RENAME TABLES job arg is nil".to_owned()))?;
        let info = info.read();
        let old_schema_id = if info.old_schema_id == 0 {
            active.job.schema_id
        } else {
            info.old_schema_id
        };
        let new_schema_id = if info.new_schema_id == 0 {
            old_schema_id
        } else {
            info.new_schema_id
        };
        let (lookup_schema_id, lookup_table_id, lookup_name) = if phase_two {
            (
                new_schema_id,
                info.table_id,
                info.new_table_name.lowercase(),
            )
        } else {
            (
                old_schema_id,
                info.table_id,
                info.old_table_name.lowercase(),
            )
        };
        let table = catalog
            .databases
            .iter()
            .find(|database| database.info.id == lookup_schema_id)
            .and_then(|database| {
                database
                    .tables
                    .iter()
                    .find(|table| {
                        table.id == lookup_table_id || table.name.lowercase() == lookup_name
                    })
                    .cloned()
            });
        let Some(table) = table else {
            failure = Some(DdlPlanError::from(
                tidb_util::dbterror::CLASS_SCHEMA
                    .new_std(tidb_error::tidb::errcode::ErrNoSuchTable)
                    .generate(if phase_two {
                        format!("Table '{}' doesn't exist", info.new_table_name.original())
                    } else {
                        format!("Table '{}' doesn't exist", info.old_table_name.original())
                    }),
            ));
            break;
        };
        if !catalog
            .databases
            .iter()
            .any(|database| database.info.id == new_schema_id)
        {
            failure = Some(DdlPlanError::from(
                tidb_util::dbterror::CLASS_SCHEMA
                    .new_std(tidb_error::tidb::errcode::ErrBadDB)
                    .generate(format!("Unknown database '(Schema ID {new_schema_id})'")),
            ));
            break;
        }
        resolved.push(Move {
            old_schema_id,
            new_schema_id,
            table,
            new_name: info.new_table_name.clone(),
            old_table_id: info.table_id,
        });
    }

    if let Some(error) = failure {
        return Err(cancel_ddl_job(active, error));
    }

    let schema_version = catalog.schema_version;
    let mut mutations = Vec::new();

    if active.job.schema_state != SchemaState::PUBLIC {
        // Phase one: move the tables and bump the version once.
        let mut affected = Vec::with_capacity(resolved.len().saturating_sub(1));
        for (index, move_plan) in resolved.iter().enumerate() {
            let info_handle = rename_infos
                .get(index)
                .ok_or_else(|| DdlPlanError::Encode("RENAME TABLES job arg is nil".to_owned()))?;
            let info = info_handle.read();
            if index > 0 {
                let adjusted_old = if info.old_schema_id_for_schema_diff > 0 {
                    info.old_schema_id_for_schema_diff
                } else {
                    info.old_schema_id
                };
                affected.push(tidb_model::AffectedOption {
                    schema_id: move_plan.new_schema_id,
                    old_schema_id: adjusted_old,
                    table_id: move_plan.table.id,
                    old_table_id: move_plan.table.id,
                    ..tidb_model::AffectedOption::default()
                });
            }
            mutations.push(BufferMutation::delete(key::table_kv_key(
                move_plan.old_schema_id,
                move_plan.table.id,
            ))?);
            let mut table = move_plan.table.clone();
            table.name = move_plan.new_name.clone();
            // Go `checkAndRenameTables`: keep the auto ID anchored to the
            // original schema across a cross-schema rename.
            if table.auto_id_schema_id == 0 && move_plan.new_schema_id != move_plan.old_schema_id {
                table.auto_id_schema_id = move_plan.old_schema_id;
            }
            if move_plan.new_schema_id == table.auto_id_schema_id {
                table.auto_id_schema_id = 0;
            }
            table.update_ts = start_ts;
            mutations.push(BufferMutation::set(
                key::table_kv_key(move_plan.new_schema_id, table.id),
                value::serialize_table_info(&table)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?);
        }
        let _first_guard = &first;
        let schema_version = schema_version + 1;
        let diff = SchemaDiff {
            version: schema_version,
            action_type: active.job.type_,
            schema_id: first.new_schema_id,
            table_id: first.table_id,
            old_schema_id: first.old_schema_id,
            ..SchemaDiff::default()
        };
        mutations.push(BufferMutation::set(
            key::schema_version_kv_key(),
            value::encode_int_value(schema_version),
        )?);
        mutations.push(BufferMutation::set(
            key::schema_diff_kv_key(schema_version),
            value::serialize_schema_diff(&diff)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?);
        active.job.last_schema_version = schema_version;
        active.job.schema_state = SchemaState::PUBLIC;
        return Ok(PersistedDdlActionStep {
            write: DdlWrite {
                ddl_job_id,
                mutations,
                schema_version,
                diff,
                created_id: None,
                backfill: Vec::new(),
                exchange_partition_validation: None,
                check_constraint_validation: None,
                column_reorgs: Vec::new(),
                foreign_key_checks: Vec::new(),
                cache_table_check: None,
                partition_reorg: None,
                mdl_info_update: None,
                exchange_partition_label_swap: None,
                warnings: Vec::new(),
                placement_bundles: Vec::new(),
                placement_rollback_bundles: Vec::new(),
            },
            update_raw_args: true,
            run_error: None,
        });
    }

    // Phase two (Go `finishJobRenameTables`): bump the version once more
    // with every old schema pinned to the new schema, then finish the job
    // over the relocated tables.
    let mut tables: Vec<tidb_model::TableInfo> = Vec::with_capacity(resolved.len());
    for move_plan in resolved.iter() {
        let mut table = move_plan.table.clone();
        table.name = move_plan.new_name.clone();
        table.update_ts = start_ts;
        tables.push(table);
    }
    let schema_version = schema_version + 1;
    let mut diff = SchemaDiff {
        version: schema_version,
        action_type: active.job.type_,
        schema_id: first.new_schema_id,
        table_id: first.table_id,
        old_schema_id: first.new_schema_id,
        ..SchemaDiff::default()
    };
    let mut affected = Vec::with_capacity(resolved.len().saturating_sub(1));
    for move_plan in resolved.iter().skip(1) {
        affected.push(tidb_model::AffectedOption {
            schema_id: move_plan.new_schema_id,
            old_schema_id: move_plan.new_schema_id,
            table_id: move_plan.table.id,
            old_table_id: move_plan.table.id,
            ..tidb_model::AffectedOption::default()
        });
    }
    diff.affected_options = GoSharedPointerSlice::from(affected);
    mutations.push(BufferMutation::set(
        key::schema_version_kv_key(),
        value::encode_int_value(schema_version),
    )?);
    mutations.push(BufferMutation::set(
        key::schema_diff_kv_key(schema_version),
        value::serialize_schema_diff(&diff)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
    )?);
    active.job.last_schema_version = schema_version;
    let published: GoSharedPointerSlice<tidb_model::TableInfo> = GoSharedPointerSlice::from(tables);
    active.job.finish_multiple_table_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        schema_version,
        &published,
    );

    Ok(PersistedDdlActionStep {
        write: DdlWrite {
            ddl_job_id,
            mutations,
            schema_version,
            diff,
            created_id: None,
            backfill: Vec::new(),
            exchange_partition_validation: None,
            check_constraint_validation: None,
            column_reorgs: Vec::new(),
            foreign_key_checks: Vec::new(),
            cache_table_check: None,
            partition_reorg: None,
            mdl_info_update: None,
            exchange_partition_label_swap: None,
            warnings: Vec::new(),
            placement_bundles: Vec::new(),
            placement_rollback_bundles: Vec::new(),
        },
        update_raw_args: true,
        run_error: None,
    })
}

/// Plans one execution step of a persisted `ACTION_DROP_SCHEMA` job.
///
/// Pinned Go `onDropSchema` (`pkg/ddl/schema.go:158`): the job walks the
/// database state machine -- `Public` -> `WriteOnly` -> `DeleteOnly` -> `None`
/// -- bumping the schema version at every transition, then deletes the
/// database meta key (table KVs are left to the delete range) and finishes
/// with `FinishDBJob`. A missing database cancels the job with the recorded
/// error (Go `checkSchemaExistAndCancelNotExistJob`).
fn plan_persisted_drop_schema_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let ddl_job_id = active.job.id;

    let schema_version = catalog.schema_version;
    let mut mutations = Vec::new();

    let Some(database) = catalog
        .databases
        .iter()
        .find(|database| database.info.id == active.job.schema_id)
        .map(|database| database.info.clone())
    else {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_util::dbterror::CLASS_SCHEMA
                    .new_std(tidb_error::tidb::errcode::ErrDBDropExists)
                    .generate("Can't drop database ''; database doesn't exist"),
            ),
        ));
    };

    let mut db_info = database;
    let schema_version = schema_version + 1;
    match db_info.state {
        SchemaState::PUBLIC => db_info.state = SchemaState::WRITE_ONLY,
        SchemaState::WRITE_ONLY => db_info.state = SchemaState::DELETE_ONLY,
        SchemaState::DELETE_ONLY => db_info.state = SchemaState::NONE,
        _ => {}
    }
    let diff = SchemaDiff {
        version: schema_version,
        action_type: active.job.type_,
        schema_id: db_info.id,
        ..SchemaDiff::default()
    };
    if db_info.state != SchemaState::NONE {
        mutations.push(BufferMutation::set(
            key::database_kv_key(db_info.id),
            value::serialize_db_info(&db_info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?);
    } else {
        // Go `metaMut.DropDatabase`: only the database meta key is deleted;
        // the tables' KVs are cleaned by the delete range.
        mutations.push(BufferMutation::delete(key::database_kv_key(db_info.id))?);
        active
            .job
            .finish_db_job(JobState::DONE, SchemaState::NONE, schema_version, None);
    }
    active.job.schema_state = db_info.state;
    mutations.push(BufferMutation::set(
        key::schema_version_kv_key(),
        value::encode_int_value(schema_version),
    )?);
    mutations.push(BufferMutation::set(
        key::schema_diff_kv_key(schema_version),
        value::serialize_schema_diff(&diff)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
    )?);
    active.job.last_schema_version = schema_version;

    Ok(PersistedDdlActionStep {
        write: DdlWrite {
            ddl_job_id,
            mutations,
            schema_version,
            diff,
            created_id: None,
            backfill: Vec::new(),
            exchange_partition_validation: None,
            check_constraint_validation: None,
            column_reorgs: Vec::new(),
            foreign_key_checks: Vec::new(),
            cache_table_check: None,
            partition_reorg: None,
            mdl_info_update: None,
            exchange_partition_label_swap: None,
            warnings: Vec::new(),
            placement_bundles: Vec::new(),
            placement_rollback_bundles: Vec::new(),
        },
        update_raw_args: false,
        run_error: None,
    })
}

/// Plans one execution step of a persisted `ACTION_DROP_TABLE` job.
///
/// Pinned Go `onDropTableOrView` (`pkg/ddl/table.go:62`): the job walks the
/// table state machine -- `Public` -> `WriteOnly` -> `DeleteOnly` -> `None` --
/// bumping the schema version at every transition, then deletes the table meta
/// key and finishes with `FinishTableJob`. A missing table cancels the job
/// with the recorded error (Go `checkTableExistAndCancelNonExistJob`). Each
/// call advances exactly one state; the runner loops until the job lands in
/// history.
fn plan_persisted_drop_table_job_step(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let ddl_job_id = active.job.id;

    let schema_version = catalog.schema_version;
    let mut mutations = Vec::new();

    let mut table = checked_job_table(catalog, active, false)?.clone_like_go();
    let database = catalog
        .databases
        .iter()
        .find(|db| db.info.id == active.job.schema_id)
        .expect("checked_job_table checked the database");

    let schema_version = schema_version + 1;
    match table.state {
        SchemaState::PUBLIC => table.state = SchemaState::WRITE_ONLY,
        SchemaState::WRITE_ONLY => table.state = SchemaState::DELETE_ONLY,
        SchemaState::DELETE_ONLY => table.state = SchemaState::NONE,
        _ => {}
    }
    let diff = SchemaDiff {
        version: schema_version,
        action_type: active.job.type_,
        schema_id: database.info.id,
        table_id: table.id,
        old_table_id: table.id,
        ..SchemaDiff::default()
    };
    if table.state != SchemaState::NONE {
        mutations.push(BufferMutation::set(
            key::table_kv_key(database.info.id, table.id),
            value::serialize_table_info(&table)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
        )?);
    } else {
        mutations.push(BufferMutation::delete(key::table_kv_key(
            database.info.id,
            table.id,
        ))?);
        active.job.finish_table_job(
            JobState::DONE,
            SchemaState::NONE,
            schema_version,
            Some(GoShared::new(table.clone())),
        );
    }
    active.job.schema_state = table.state;
    mutations.push(BufferMutation::set(
        key::schema_version_kv_key(),
        value::encode_int_value(schema_version),
    )?);
    mutations.push(BufferMutation::set(
        key::schema_diff_kv_key(schema_version),
        value::serialize_schema_diff(&diff)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
    )?);
    active.job.last_schema_version = schema_version;

    Ok(PersistedDdlActionStep {
        write: DdlWrite {
            ddl_job_id,
            mutations,
            schema_version,
            diff,
            created_id: None,
            backfill: Vec::new(),
            exchange_partition_validation: None,
            check_constraint_validation: None,
            column_reorgs: Vec::new(),
            foreign_key_checks: Vec::new(),
            cache_table_check: None,
            partition_reorg: None,
            mdl_info_update: None,
            exchange_partition_label_swap: None,
            warnings: Vec::new(),
            placement_bundles: Vec::new(),
            placement_rollback_bundles: Vec::new(),
        },
        update_raw_args: false,
        run_error: None,
    })
}
// Action handlers select cancellation; the shared worker records this fresh
// error, discards action writes, and finalizes the job in the same transaction.
fn cancel_ddl_job(
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    error: impl Into<DdlPlanError>,
) -> DdlPlanError {
    active.job.state = JobState::CANCELLED;
    error.into()
}

fn job_only_write(ddl_job_id: i64, mutations: Vec<BufferMutation>) -> DdlWrite {
    DdlWrite {
        ddl_job_id,
        mutations,
        schema_version: 0,
        diff: SchemaDiff::default(),
        created_id: None,
        backfill: Vec::new(),
        exchange_partition_validation: None,
        check_constraint_validation: None,
        column_reorgs: Vec::new(),
        foreign_key_checks: Vec::new(),
        cache_table_check: None,
        partition_reorg: None,
        mdl_info_update: None,
        exchange_partition_label_swap: None,
        warnings: Vec::new(),
        placement_bundles: Vec::new(),
        placement_rollback_bundles: Vec::new(),
    }
}

fn job_control_step(job: &Job) -> PersistedDdlActionStep {
    PersistedDdlActionStep {
        write: job_only_write(job.id, Vec::new()),
        update_raw_args: false,
        run_error: None,
    }
}

fn cancelled_ddl_job(active: &mut crate::ddl_job_table::ActiveDdlJob) -> DdlPlanError {
    cancel_ddl_job(
        active,
        DdlPlanError::from(
            tidb_util::dbterror::ERR_CANCELLED_DDL_JOB
                .generate(tidb_error::tidb::errname::ErrCancelledDDLJob.raw),
        ),
    )
}

// Go convertJob2RollbackJob, restricted to the actions integrated into this
// worker. Conversion is its own step; an irreversible DROP resumes next time.
fn plan_cancel_persisted_ddl_job(
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let cancel = match active.job.type_ {
        ActionType::ACTION_CREATE_SCHEMA
        | ActionType::ACTION_CREATE_TABLE
        | ActionType::ACTION_CREATE_TABLES => true,
        ActionType::ACTION_RENAME_TABLES => active.job.schema_state == SchemaState::NONE,
        ActionType::ACTION_DROP_SCHEMA => {
            let database = catalog
                .databases
                .iter()
                .find(|db| db.info.id == active.job.schema_id)
                .ok_or_else(|| {
                    cancel_ddl_job(
                        active,
                        DdlPlanError::from(
                            tidb_util::dbterror::CLASS_SCHEMA
                                .new_std(tidb_error::tidb::errcode::ErrDBDropExists)
                                .generate("Can't drop database ''; database doesn't exist"),
                        ),
                    )
                })?;
            database.info.state == SchemaState::PUBLIC
        }
        ActionType::ACTION_DROP_TABLE => {
            let table = checked_job_table(catalog, active, false)?;
            table.state == SchemaState::PUBLIC
        }
        ActionType::ACTION_ADD_CHECK_CONSTRAINT
        | ActionType::ACTION_DROP_CHECK_CONSTRAINT
        | ActionType::ACTION_ALTER_CHECK_CONSTRAINT => {
            return plan_persisted_check_constraint_job_step(catalog, active, start_ts);
        }
        _ => {
            return Err(DdlPlanError::Encode(format!(
                "cancellation for disabled action {} is not integrated",
                active.job.type_
            )))
        }
    };
    if cancel {
        return Err(cancelled_ddl_job(active));
    }
    active.job.state = JobState::RUNNING;
    Ok(job_control_step(&active.job))
}

// Go GetTableInfoAndCancelFaultJob/checkTableExistAndCancelNonExistJob. Keep
// the same object checks on forward CHECK and cancellation paths.
fn checked_job_table<'a>(
    catalog: &'a ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    require_public: bool,
) -> Result<&'a TableInfo, DdlPlanError> {
    let database = catalog
        .databases
        .iter()
        .find(|db| db.info.id == active.job.schema_id)
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::from(
                    tidb_util::dbterror::CLASS_SCHEMA
                        .new_std(tidb_error::tidb::errcode::ErrBadDB)
                        .generate(format!(
                            "Unknown database '(Schema ID {})'",
                            active.job.schema_id
                        )),
                ),
            )
        })?;
    let table = database
        .tables
        .iter()
        .find(|table| table.id == active.job.table_id)
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::from(
                    tidb_util::dbterror::CLASS_SCHEMA
                        .new_std(tidb_error::tidb::errcode::ErrNoSuchTable)
                        .generate(format!(
                            "Table '(Schema ID {}).(Table ID {})' doesn't exist",
                            active.job.schema_id, active.job.table_id
                        )),
                ),
            )
        })?;
    if !active.job.table_name.is_empty()
        && table.name.lowercase() != active.job.table_name.to_string()
    {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_util::dbterror::CLASS_SCHEMA
                    .new_std(tidb_error::tidb::errcode::ErrNoSuchTable)
                    .generate(format!(
                        "Table '{}.{}' doesn't exist",
                        active.job.schema_name, active.job.table_name
                    )),
            ),
        ));
    }
    if require_public && table.state != SchemaState::PUBLIC {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(tidb_util::dbterror::ERR_INVALID_DDL_STATE.generate(format!(
                "table {} is not in public, but {}",
                table.name, table.state
            ))),
        ));
    }
    Ok(table)
}

fn record_ddl_cancellation_error(
    job: &mut Job,
    error: &DdlPlanError,
    load_limit: &dyn Fn() -> i64,
) {
    if job.error.is_none() {
        record_ddl_plan_error(job, error);
    } else {
        job.error_count += 1;
    }
    if error.is_source_error(&tidb_util::dbterror::ERR_CANCELLED_DDL_JOB) {
        let original = job.error.as_ref().unwrap().read().clone();
        if !tidb_util::dbterror::ERR_CANCELLED_DDL_JOB.equal(Some(&original)) {
            // generate retains a source identity and also preserves old Rust
            // history without an RFC code; synthesizing class zero invents one.
            job.error = Some(GoShared::new(original.generate(format!(
                "DDL job rollback, error msg: {}",
                original.message()
            ))));
        }
    } else {
        let limit = load_limit();
        if job.error_count > limit {
            let message =
                format!("rollback DDL job error count exceed the limit {limit}, cancelled it now");
            // Replacing the diagnostic must not count the same failure twice.
            job.error = Some(GoShared::new(unknown_ddl_job_error(message)));
            job.state = JobState::CANCELLED;
        }
    }
}

fn count_ddl_action_error(job: &mut Job, error: &DdlPlanError, load_limit: &dyn Fn() -> i64) {
    record_ddl_plan_error(job, error);
    if !job.is_cancelled() {
        let limit = load_limit();
        if job.error_count > limit && job.is_running() && job.is_rollbackable() {
            job.state = JobState::CANCELLING;
        }
    }
}

// Go worker.countForPanic is distinct from countForError: below the limit it
// keeps the earlier error (including nil), and counts every panic just once.
fn count_ddl_action_panic(job: &mut Job, load_limit: &dyn Fn() -> i64) {
    job.state = if job.is_rollingback() {
        JobState::CANCELLED
    } else {
        JobState::CANCELLING
    };
    job.error_count += 1;
    let limit = load_limit();
    if job.error_count > limit {
        job.error = Some(GoShared::new(unknown_ddl_job_error(format!(
            "panic in handling DDL logic and error count beyond the limitation {limit}, cancelled"
        ))));
        job.state = JobState::CANCELLED;
    }
}

// Only action execution is recoverable here. Transaction setup, queue/history
// I/O and commit are owned by the caller. All action writes are staged until a
// complete plan returns; unwinding drops guards, and the caller discards the
// incomplete outputs before publishing a checkpoint. This is why asserting
// unwind safety for the action's borrowed snapshot/buffer is sound.
pub(crate) fn recover_ddl_action<T>(job_id: i64, action: impl FnOnce() -> T) -> Result<T, ()> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)).map_err(|panic| {
        tidb_util::panic_metrics::PANIC_TOTAL
            .with_label_values(&["ddl-worker"])
            .inc();
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("non-string panic payload");
        eprintln!("{{\"level\":\"error\",\"event\":\"ddl_job_action_panic\",\"job_id\":{job_id},\"error\":{}}}",
            serde_json::to_string(message).unwrap_or_default());
    })
}

fn record_ddl_plan_error(job: &mut Job, error: &DdlPlanError) {
    job.error = Some(GoShared::new(error.to_job_error()));
    job.error_count += 1;
}

/// Outcome of an action that could not complete its staged work.
#[derive(Debug)]
pub enum PersistedDdlJobFailure {
    /// A returned action error follows Go's countForError path.
    Error(DdlPlanError),
    /// An unwinding action follows Go's distinct countForPanic path.
    Panic,
}

/// Plans an action failure against the ORIGINAL action snapshot. The
/// caller commits this job-only write using that same transaction, so a racing
/// pause/cancel causes a write conflict instead of being overwritten.
pub fn plan_persisted_ddl_job_failure<S: MetaSnapshot>(
    snapshot: &mut S,
    ddl_job_id: i64,
    start_ts: u64,
    failure: PersistedDdlJobFailure,
    load_error_count_limit: &dyn Fn() -> i64,
) -> Result<PersistedDdlJobStep, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    let job_table = crate::ddl_job_table::DdlJobTable::locate(&catalog)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    let mut active = job_table
        .load_by_id(snapshot, ddl_job_id)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?
        .ok_or_else(|| DdlPlanError::Encode(format!("DDL job {ddl_job_id} does not exist")))?;
    if active.job.real_start_ts == 0 {
        active.job.real_start_ts = start_ts;
    }
    if !active.job.is_rollingback() {
        active.job.state = JobState::RUNNING;
    }
    let run_error = match failure {
        PersistedDdlJobFailure::Error(error) => {
            if matches!(
                active.job.type_,
                ActionType::ACTION_ADD_CHECK_CONSTRAINT | ActionType::ACTION_ALTER_CHECK_CONSTRAINT
            ) && error.is_source_error(&tidb_util::dbterror::ERR_CHECK_CONSTRAINT_IS_VIOLATED)
            {
                active.job.state = JobState::ROLLINGBACK;
            }
            active.job.last_schema_version = 0;
            count_ddl_action_error(&mut active.job, &error, load_error_count_limit);
            Some(error)
        }
        PersistedDdlJobFailure::Panic => {
            count_ddl_action_panic(&mut active.job, load_error_count_limit);
            None
        }
    };
    if active.job.is_cancelled() {
        return finish_persisted_ddl_job(snapshot, &catalog, &job_table, &mut active, start_ts);
    }
    let mut mutations = Vec::new();
    job_table
        .append_update(&mut active, false, &mut mutations)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    Ok(PersistedDdlJobStep {
        write: job_only_write(ddl_job_id, mutations),
        terminal: false,
        run_error,
        error_count: active.job.error_count,
    })
}

/// One catalog change's complete write set.
#[derive(Clone, Debug)]
pub struct DdlWrite {
    /// Go `model.Job.ID`, allocated after IDs owned by the job.
    pub ddl_job_id: i64,
    /// Every meta-key mutation, in a deterministic order.
    pub mutations: Vec<BufferMutation>,
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
    /// The row checks or rewrites MODIFY / CHANGE COLUMN owes, one per
    /// sub-job whose data changes, in sub-job order.
    pub column_reorgs: Vec<ColumnReorg>,
    /// The existing-row checks the `ADD FOREIGN KEY`s owe, in job order.
    pub foreign_key_checks: Vec<ForeignKeyCheck>,
    /// The table an `ALTER TABLE ... CACHE` owes Go's `checkCacheTableSize`.
    pub cache_table_check: Option<Box<TableInfo>>,
    /// The row move a partition reorganization owes (Go
    /// `doPartitionReorgWork`, then the drop of the replaced data).
    pub partition_reorg: Option<PartitionReorgStep>,
    /// The `mysql.tidb_mdl_info` row that must be replaced atomically with
    /// this schema phase before the owner waits for acknowledgements.
    pub mdl_info_update: Option<MdlInfoUpdate>,
    /// The PD region-label rule swap owed by `EXCHANGE PARTITION`.
    pub exchange_partition_label_swap: Option<ExchangePartitionLabelSwap>,
    /// The warnings the change raises, if any.
    ///
    /// Go carries these as `job.Warning` (and, for the same adjustments made
    /// at admission time, `StmtCtx.AppendWarning`); the client reads them
    /// back with `SHOW WARNINGS`. A change that silently did something other
    /// than what was written would otherwise look like it did exactly what
    /// was written.
    pub warnings: Vec<(DdlWarningLevel, u16, String)>,
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
    /// Go `vardef.EnableCheckConstraint`: whether each table's writable
    /// CHECK constraints are applied to the other's rows.
    pub check_constraints: bool,
}

/// One Go `registerMDLInfo` replacement associated with a committed CHECK
/// schema phase.
#[derive(Clone, Debug)]
pub struct MdlInfoUpdate {
    /// Stored `mysql.tidb_mdl_info` table definition used by the ordinary
    /// clustered-row encoder.
    pub table: Box<TableInfo>,
    /// Tables touched by the job, stored as Go's comma-separated ID list.
    pub table_ids: String,
    /// System-table upgrades must not depend on the owner_id column.
    pub omit_owner_id: bool,
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
            Datum::Bytes(self.table_ids.as_bytes().to_vec()),
        );
        if !self.omit_owner_id {
            values.insert(
                column_id("owner_id")?,
                Datum::Bytes(owner_id.as_bytes().to_vec()),
            );
        }
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
        mutations: &mut Vec<BufferMutation>,
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
    pub fn append_delete_mutations<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        ddl_job_id: i64,
        owner_id: &str,
        mutations: &mut Vec<BufferMutation>,
    ) -> Result<(), DdlPlanError> {
        use crate::mysql_system_tables::{scan_system_table_prefixed, SystemRow, SystemTableView};
        let columns: &[&str] = if self.omit_owner_id {
            &["job_id"]
        } else {
            &["job_id", "owner_id"]
        };
        let view = SystemTableView::project("mysql.tidb_mdl_info", &self.table, columns);
        let pairs = scan_system_table_prefixed(snapshot, &view, &[Datum::Int(ddl_job_id)])
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
        for (key, value) in pairs {
            let row = SystemRow::parse(&view, &key, &value)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            let matches_owner = self.omit_owner_id
                || row
                    .text("owner_id")
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?
                    .as_deref()
                    == Some(owner_id);
            if matches_owner {
                mutations.extend(
                    crate::system_row_write::delete_unindexed_clustered_row(
                        &self.table,
                        &row.into_values(),
                    )
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
                );
            }
        }
        Ok(())
    }
}

/// The names and post-exchange physical IDs Go uses to swap the standalone
/// table and partition label rules in PD; shared with the in-process tier.
pub use tidb_executor::ddl::label_rules::ExchangePartitionLabelSwap;

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
    /// The operation; only creation evaluates rows.
    pub operation: IndexBackfillOperation,
}

/// The data step of a MODIFY / CHANGE COLUMN Go does not finish in metadata
/// (`onModifyColumn` for every kind but `ModifyTypeNoReorg`), staged in the
/// same transaction as the new `TableInfo`.
#[derive(Clone, Debug)]
pub struct ColumnReorg {
    /// The table as its stored rows have it, before the change.
    pub table: Box<TableInfo>,
    /// The id of the column being changed, in [`Self::table`].
    pub column_id: i64,
    /// The table as the change leaves it: the column's new definition and
    /// place, and under
    /// [`tidb_executor::ddl::modify_column_type::ModifyColumnType::Reorg`]
    /// its new id.
    pub after: Box<TableInfo>,
    /// The changed column's id in [`Self::after`].
    pub new_column_id: i64,
    /// Each index rebuilt under a new id: (old id, new id).
    pub renumbered_indexes: Vec<(i64, i64)>,
    /// How the stored data changes, once any precheck has passed.
    pub kind: tidb_executor::ddl::modify_column_type::ModifyColumnType,
    /// Go `ModifyTypePrecheck`: check the rows first, and report a row that
    /// fails so the statement is reorganized instead.
    pub precheck: bool,
    /// Go `!noReorgDataStrict`: whether the row check covers the value range.
    pub check_value_range: bool,
    /// The persisted collation mode captured when this DDL was planned.
    pub use_new_collation: bool,
    /// SQL mode and location the rows are checked and converted under.
    pub context: DdlStatementContext,
    /// The multi-schema sub-job this data step belongs to, `None` for a
    /// single MODIFY / CHANGE; a failed precheck re-plans that one only.
    pub action_offset: Option<usize>,
}

/// The rows a partition reorganization moves, staged in the DDL transaction.
#[derive(Clone, Debug)]
pub struct PartitionReorgStep {
    /// The table as its stored rows have it, before the change.
    pub table: Box<TableInfo>,
    /// What moves where (`tidb_executor::kv_table::PartitionReorg`).
    pub reorg: tidb_executor::kv_table::PartitionReorg,
    /// SQL mode and location the rows are routed under.
    pub context: DdlStatementContext,
    /// The database the table's auto-id counters live under.
    pub db_id: i64,
    /// Go's final state for PARTITION BY / REMOVE PARTITIONING: each counter
    /// copied from the old table id's key to the new one's, as
    /// (from, to). It runs after the rows moved, because a heap row whose
    /// `_tidb_rowid` collides takes a new one from the old counter first.
    pub counter_moves: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Go `checkForeignKeyConstrain`, owed by an `ADD FOREIGN KEY` under
/// `foreign_key_checks = ON` before the key becomes public: the child's
/// existing rows against the parent, in the same transaction.
#[derive(Clone, Debug)]
pub struct ForeignKeyCheck {
    /// The child's database.
    pub schema: String,
    /// The child as the change leaves it, carrying the new key.
    pub child: Box<TableInfo>,
    /// The parent's database and table, `None` for a self reference.
    pub parent: Option<(String, Box<TableInfo>)>,
    /// The new key's name.
    pub name: String,
    /// The persisted collation mode captured when this DDL was planned.
    pub use_new_collation: bool,
    /// The context the rows are read under.
    pub context: DdlStatementContext,
}

/// Existing-row evaluation belongs only to index creation. Removal uses the
/// physical index range, independent of row expressions and SQL mode.
#[derive(Clone, Debug)]
pub enum IndexBackfillOperation {
    /// Build entries under the submitting statement's captured policy.
    Add(DdlStatementContext),
    /// Remove every key owned by the index.
    Drop,
}

/// Column removal owns the implicit deletion of its single-column indexes.
fn plan_removed_column_indexes(
    before: &TableInfo,
    after: &TableInfo,
    use_new_collation: bool,
    backfill: &mut Vec<IndexBackfill>,
) {
    for index in before.indices.iter_handles().flatten() {
        let id = index.read().id;
        if !after
            .indices
            .iter_deref()
            .any(|candidate| candidate.read().id == id)
        {
            backfill.push(IndexBackfill {
                table: Box::new(before.clone_like_go()),
                index: index.clone(),
                use_new_collation,
                operation: IndexBackfillOperation::Drop,
            });
        }
    }
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
/// The cluster catalog's side of a partition reorganization: ids from the
/// global allocator, and the placement policies the snapshot holds.
struct ClusterReorgHost<'a> {
    global_ids: &'a mut GlobalIdAllocator,
    policies: Vec<tidb_model::PolicyInfo>,
    /// An allocator failure, which the trait cannot carry; the plan reports
    /// it once the shared code returns.
    failed: Option<DdlPlanError>,
}

impl tidb_executor::ddl::ReorgHost for ClusterReorgHost<'_> {
    fn allocate_table_id(&mut self) -> i64 {
        match self.global_ids.allocate(1) {
            Ok(ids) => ids[0],
            Err(error) => {
                self.failed.get_or_insert(error);
                0
            }
        }
    }

    fn resolve_placement(
        &self,
        definitions: &mut [tidb_executor::partition_routing::PartitionDef],
    ) -> Result<(), tidb_executor::DriverError> {
        for definition in definitions {
            if let Some(reference) = definition.placement_policy.as_mut() {
                let Some(found) = self
                    .policies
                    .iter()
                    .find(|policy| policy.name.lowercase() == reference.name.lowercase())
                else {
                    return Err(tidb_executor::DriverError::PlacementPolicyNotExists(
                        reference.name.original().to_owned(),
                    ));
                };
                reference.id = found.id;
            }
        }
        Ok(())
    }
}

/// Whether a table option is one of Go's TTL family.
fn is_ttl_table_option(option: &tidb_ast::TableOption) -> bool {
    matches!(
        option,
        tidb_ast::TableOption::Ttl { .. }
            | tidb_ast::TableOption::TtlEnable(_)
            | tidb_ast::TableOption::TtlJobInterval(_)
    )
}

/// Go `CreateSequence`'s `BuildTableInfo(ctx, name, nil, nil, "", "")` with
/// `Sequence` set: a column-less `TableInfo` of the current version.
fn build_sequence_table_info(
    name: &str,
    info: tidb_executor::sequence::SequenceInfo,
    comment: String,
) -> TableInfo {
    TableInfo {
        name: CiString::new(name.to_owned()),
        version: tidb_model::table_info::CURR_LATEST_TABLE_INFO_VERSION,
        sequence: Some(GoShared::new(stored_sequence_info(info, comment))),
        ..TableInfo::default()
    }
}

/// Go `model.SequenceInfo` from the options that decide values.
fn stored_sequence_info(
    info: tidb_executor::sequence::SequenceInfo,
    comment: String,
) -> tidb_model::table::SequenceInfo {
    tidb_model::table::SequenceInfo {
        start: info.start,
        cache: info.cache,
        cycle: info.cycle,
        min_value: info.min_value,
        max_value: info.max_value,
        increment: info.increment,
        cache_value: info.cache_value,
        comment,
    }
}

/// The options of a stored `model.SequenceInfo` that decide values.
fn sequence_options_of(
    stored: &tidb_model::table::SequenceInfo,
) -> tidb_executor::sequence::SequenceInfo {
    tidb_executor::sequence::SequenceInfo {
        start: stored.start,
        increment: stored.increment,
        min_value: stored.min_value,
        max_value: stored.max_value,
        cache_value: stored.cache_value,
        cache: stored.cache,
        cycle: stored.cycle,
    }
}

/// Go `Mutator.sequenceKey` in the database hash: the sequence's counter.
fn sequence_value_kv_key(db_id: i64, sequence_id: i64) -> Vec<u8> {
    tidb_meta::structure::encode_hash_data_key(&key::db_key(db_id), &key::sequence_key(sequence_id))
}

/// Go `createSequenceWithCheck` / `restartSequenceValue`: the counter sits one
/// step outside `value`, so the next congruence seek returns `value` itself.
fn sequence_base_for(increment: i64, value: i64) -> i64 {
    if increment >= 0 {
        value - 1
    } else {
        value + 1
    }
}

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

/// The routing spec of a stored partitioned table, as this node's loader
/// builds it from Go's `PartitionInfo`.
fn stored_partition_spec(
    stored: &TableInfo,
) -> Result<tidb_executor::partition_routing::PartitionSpec, DdlPlanError> {
    use tidb_executor::ddl::StoredPartitionDefinition;

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
            storage_class: (
                definition.storage_class_tier.clone(),
                definition.storage_class_transitions.snapshot(),
            ),
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
    tidb_executor::ddl::partition_spec_from_metadata(
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
    })
}

fn apply_partition_change(
    stored: &TableInfo,
    schema: &str,
    table: &str,
    sql: &str,
) -> Result<tidb_executor::partition_routing::PartitionSpec, DdlPlanError> {
    use tidb_executor::{Catalog, KvColumn, KvTable, TableEntry};

    let spec = stored_partition_spec(stored)?;

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
    kv_table.set_max_column_id(stored.max_column_id);
    kv_table.set_max_index_id(stored.max_index_id);
    kv_table.name = table.to_owned();
    // An added partition resolves its storage class from the table's
    // ENGINE_ATTRIBUTE (`CheckAndUpdateAddedPartitionDefinitions`).
    kv_table.set_engine_attribute(stored.engine_attribute.clone());
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
                "Table to exchange with partition is partitioned: '{}'",
                standalone.name
            ),
        ));
    }
    if standalone.affinity.is_some() || partitioned.affinity.is_some() {
        return Err(exchange_refusal(
            8200,
            "Unsupported EXCHANGE PARTITION of a table with AFFINITY option",
        ));
    }
    if !standalone.foreign_keys.is_empty() {
        return Err(exchange_refusal(
            1740,
            format!(
                "Table to exchange with partition has foreign key references: '{}'",
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
    plan_ddl_with_auto_ids(snapshot, statement, start_ts, use_new_collation, None, 0)
}

/// DDL uses the same process AutoID authority as SQL table writes.
pub fn plan_ddl_with_auto_ids<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
    start_ts: u64,
    use_new_collation: bool,
    auto_ids: Option<&std::sync::Arc<crate::auto_id_client::AutoIdClient>>,
    // The issuing session's `tidb_cdc_write_source`: a change TiCDC
    // replicates skips the BDR submit admission.
    cdc_write_source: u64,
) -> Result<DdlPlan, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    // Go `meta.Mutator.GetBDRRole`, at the plan's own snapshot.
    let bdr_role = snapshot.get(&key::bdr_role_kv_key())?.unwrap_or_default();
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
    let mut column_reorgs: Vec<ColumnReorg> = Vec::new();
    let mut foreign_key_checks: Vec<ForeignKeyCheck> = Vec::new();
    let mut cache_table_check: Option<Box<TableInfo>> = None;
    let mut partition_reorg: Option<PartitionReorgStep> = None;
    let mut exchange_partition_label_swap = None;
    let mut warnings: Vec<(DdlWarningLevel, u16, String)> = Vec::new();
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
                    writes.push(BufferMutation::set(key::policy_kv_key(found.id), encoded)?);
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
                    writes.push(BufferMutation::set(key::policy_kv_key(policy_id), encoded)?);
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
            writes.push(BufferMutation::set(key::policy_kv_key(found.id), encoded)?);
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
            writes.push(BufferMutation::delete(key::policy_kv_key(found.id))?);
            diff.action_type = ActionType::ACTION_DROP_PLACEMENT_POLICY;
            diff.schema_id = found.id;
        }
        DdlStatement::CreateDatabase {
            name,
            if_not_exists,
            charset,
            collate,
            placement_policy,
        } => {
            if let Some(existing) = find_database(&catalog, name) {
                if *if_not_exists {
                    return Ok(DdlPlan::AlreadySatisfied {
                        detail: format!(
                            "database `{}` already exists",
                            existing.info.name.original()
                        ),
                        warnings: vec![(
                            DdlWarningLevel::Note,
                            1007,
                            format!("Can't create database '{}'; database exists", name),
                        )],
                    });
                }
                return Err(DdlPlanError::DatabaseExists(name.clone()));
            }
            // Go `checkTooLongSchema`.
            tidb_executor::ddl::check_too_long_identifier(&name.go_to_lower()).map_err(
                |error| {
                    let error = error.to_mysql_error();
                    coded_refusal(error.code, error.message)
                },
            )?;
            // Go `handleDatabasePlacement`.
            let placement_policy_ref = match placement_policy {
                Some(policy) => check_and_normalize_placement_policy(snapshot, policy)?,
                None => None,
            };
            let db_id = global_ids.allocate(1)?[0];
            created_id = Some(db_id);
            let info = DBInfo {
                id: db_id,
                name: CiString::new(name.clone()),
                charset: charset.clone(),
                collate: collate.clone(),
                placement_policy_ref,
                state: SchemaState::PUBLIC,
                ..DBInfo::default()
            };
            let encoded = value::serialize_db_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(key::database_kv_key(db_id), encoded)?);
            diff.action_type = ActionType::ACTION_CREATE_SCHEMA;
            diff.schema_id = db_id;
            schema_change_events.push((0, SchemaChangeEvent::create_schema(&info)));
        }
        DdlStatement::DropDatabase { name, if_exists } => {
            let Some(database) = find_database(&catalog, name) else {
                if *if_exists {
                    return Ok(already(format!("database `{name}` does not exist")));
                }
                // Go `executor.DropSchema` (pkg/ddl/executor.go:804): a
                // missing database under DROP DATABASE answers
                // `ErrDatabaseDropExists` (1008, HY000), not the 1049 other
                // missing-database paths answer.
                return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                    1008,
                    format!("Can't drop database '{name}'; database doesn't exist"),
                )));
            };
            let db_id = database.info.id;
            // Go `Mutator.DropDatabase` is `HClear(DB:<id>)` then
            // `HDel(DBs, DB:<id>)`: every field of the database's own hash —
            // its `Table:<id>` entries and its per-table ID allocators — goes
            // with it. Only fields this snapshot actually observed are deleted.
            for (raw_key, _) in snapshot.scan_prefix(&key::database_metas_kv_prefix(db_id))? {
                writes.push(BufferMutation::delete(raw_key)?);
            }
            writes.push(BufferMutation::delete(key::database_kv_key(db_id))?);
            diff.action_type = ActionType::ACTION_DROP_SCHEMA;
            diff.schema_id = db_id;
            if !tidb_metadef::is_mem_or_sys_db(&name.go_to_lower()) {
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
            let mut info = stored.clone_like_go();
            apply_drop_primary_key(
                &catalog,
                schema,
                &mut info,
                use_new_collation,
                &mut backfill,
            )?;
            check_table_info_valid(&info)?;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
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
                    CiString::new(format!("{}_chk_{}", table.go_to_lower(), offset + 1));
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
            // Go `createTableWithInfoJob`.
            check_table_info_valid_extra(&info)?;
            info.update_ts = start_ts;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_CREATE_TABLE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
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
            // Go `checkTTLInfoValid`'s foreign-key half, which reads the
            // infoschema the builder does not see.
            if info.ttl_info.is_some()
                && !crate::foreign_key_build::get_table_referred_foreign_keys(
                    &catalog, schema, table,
                )
                .is_empty()
            {
                let error = tidb_executor::DriverError::TtlReferencedByForeignKey.to_mysql_error();
                return Err(coded_refusal(error.code, error.message));
            }
            // Go `CreateTable`: the built table's `checkTableForeignKeysValid`
            // (executor.go:1106) comes before `createTableWithInfoJob`'s
            // existence check, which is followed by `checkTableInfoValidExtra`
            // and, in the job, the owner's `checkTableForeignKeyValidInOwner` +
            // `allocateFKIndexID` (create_table.go:87). This planner runs every
            // stage against the one snapshot it publishes from.
            // A new parent also needs validation against existing children.
            let fk_check = build.context.foreign_key_checks();
            crate::foreign_key_build::check_table_foreign_keys_valid(
                &catalog, schema, &info, fk_check,
            )
            .map_err(DdlPlanError::Admission)?;
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
            check_table_info_valid_extra(&info)?;
            crate::foreign_key_build::check_table_foreign_keys_valid_in_owner(
                &catalog, schema, &mut info, fk_check,
            )
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
            // Go `BuildTableInfoWithStmt`: a non-temporary table with no
            // placement of its own takes the schema's default.
            if info.placement_policy_ref.is_none()
                && info.temp_table_type == tidb_model::TempTableType::NONE
            {
                if let Some(reference) = find_database(&catalog, schema)
                    .and_then(|database| database.info.placement_policy_ref.as_ref())
                {
                    info.placement_policy_ref =
                        Some(tidb_model::GoShared::new(reference.read().clone()));
                }
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
                        DdlPlanError::Admission(DdlAdmissionError::with_code(
                            GENERIC_ERROR_CODE,
                            format!("building placement rules: {error}"),
                        ))
                    })?;
            }
            // Go `createTable` stamps the job transaction's own start timestamp.
            info.update_ts = start_ts;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
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
                writes.push(BufferMutation::set(
                    crate::cluster_auto_id::auto_id_key_for(db_id, &info),
                    value::encode_int_value(info.auto_inc_id - 1),
                )?);
            }
            if info.auto_random_bits > 0 && info.auto_rand_id > 1 {
                writes.push(BufferMutation::set(
                    crate::cluster_auto_id::auto_random_id_key_for(db_id, &info),
                    value::encode_int_value(info.auto_rand_id - 1),
                )?);
            }
            diff.action_type = ActionType::ACTION_CREATE_TABLE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
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
            let existing = find_table(database, name);
            tidb_executor::view::check_view_target(
                schema,
                name,
                *or_replace,
                existing.map(|table| table.view.is_some()),
            )
            .map_err(|error| match error {
                tidb_executor::DriverError::Schema(
                    tidb_executor::SchemaErrorKind::TableExists(_),
                ) => DdlPlanError::TableExists {
                    schema: schema.clone(),
                    table: name.clone(),
                },
                error => {
                    let error = error.to_mysql_error();
                    DdlPlanError::Admission(DdlAdmissionError::with_code(error.code, error.message))
                }
            })?;
            let existing = existing.map(|table| table.id);
            let db_id = database.info.id;
            // Go SetSchemaDiffForCreateView carries both identities for a
            // replacement, so schema consumers retire the previous view.
            if let Some(old_id) = existing {
                writes.push(BufferMutation::delete(key::table_kv_key(db_id, old_id))?);
                diff.old_table_id = old_id;
            }
            let table_id = global_ids.allocate(1)?[0];
            created_id = Some(table_id);
            let mut info = (**info).clone();
            // Go `createTableWithInfoJob` checks a view like any table.
            check_table_info_valid_extra(&info)?;
            info.id = table_id;
            info.update_ts = start_ts;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_CREATE_VIEW;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::DropView { names, if_exists } => {
            let [(schema, name)] = names.as_slice() else {
                return Err(DdlPlanError::Unsupported(
                    "multi-target DROP VIEW must use the ordered DDL completion owner".into(),
                ));
            };
            let stored = find_database(&catalog, schema)
                .and_then(|db| find_table(db, name).map(|table| (db.info.id, table)));
            let Some((db_id, table)) = stored else {
                if *if_exists {
                    return Ok(already_with_warnings(
                        format!("view `{schema}`.`{name}` does not exist"),
                        vec![missing_table_note(schema, name)],
                    ));
                }
                return Err(DdlPlanError::UnknownTables(vec![format!(
                    "{schema}.{name}"
                )]));
            };
            check_cluster_drop_object(schema, name, table, true)?;
            writes.push(BufferMutation::delete(key::table_kv_key(db_id, table.id))?);
            diff.action_type = ActionType::ACTION_DROP_VIEW;
            diff.schema_id = db_id;
            diff.table_id = table.id;
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
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            writes.push(BufferMutation::set(
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
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, stored.id),
                value::serialize_table_info(&info)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?);
            diff.action_type = ActionType::ACTION_MODIFY_TABLE_AUTO_IDCACHE;
            diff.schema_id = db_id;
            diff.table_id = stored.id;
        }
        DdlStatement::SetTiFlashReplica {
            schema,
            table,
            count,
            labels,
            reset_available,
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
            // Go onSetTableFlashReplica retains logical usability when the
            // replica count/labels change, unless restore explicitly resets it.
            let mut info = stored.clone_like_go();
            if *count == 0 {
                info.tiflash_replica = None;
            } else {
                let mut replica = tidb_model::table::TiFlashReplicaInfo::default();
                replica.count = *count;
                replica.location_labels = labels.clone().into();
                replica.available = !reset_available
                    && stored
                        .tiflash_replica
                        .as_ref()
                        .is_some_and(|old| old.read().available);
                info.tiflash_replica = Some(GoShared::new(replica));
            }
            let db_id = database.info.id;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, stored.id),
                value::serialize_table_info(&info)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?);
            diff.action_type = ActionType::ACTION_SET_TI_FLASH_REPLICA;
            diff.schema_id = db_id;
            diff.table_id = stored.id;
            // Placement creation still uses the legacy poll owner here.
            // Its migration to durable DDL and cleanup to GC remains F01.
        }
        DdlStatement::AlterTableSetRegionSplitPolicy {
            schema,
            table,
            target,
            option,
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
            let target = match target {
                tidb_ast::SplitTarget::Table => tidb_executor::ddl::SplitPolicyTarget::Table,
                tidb_ast::SplitTarget::PrimaryKey => {
                    tidb_executor::ddl::SplitPolicyTarget::PrimaryKey
                }
                tidb_ast::SplitTarget::Index(name) => {
                    tidb_executor::ddl::SplitPolicyTarget::Index(name)
                }
            };
            let (index_name, policy) = tidb_executor::ddl::normalize_split_policy(
                target, option, stored,
            )
            .map_err(|error| {
                let error = error.to_mysql_error();
                DdlPlanError::Admission(DdlAdmissionError::with_code(error.code, error.message))
            })?;
            let mut info = stored.clone_like_go();
            crate::table_info_build::set_region_split_policy(&mut info, &index_name, policy);
            let db_id = database.info.id;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, stored.id),
                value::serialize_table_info(&info)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?);
            diff.action_type = ActionType::ACTION_ALTER_TABLE_SET_REGION_SPLIT_POLICY;
            diff.schema_id = db_id;
            diff.table_id = stored.id;
        }
        DdlStatement::UpdateTiFlashReplicaStatus {
            table_id,
            available,
        } => {
            // Go `onUpdateFlashReplicaStatus`: a metadata-only flip. The
            // schema version still advances: this framework publishes every
            // catalog write with a new version, which only churns the reload
            // trigger, never the answers.
            let mut found: Option<(i64, tidb_model::table_info::TableInfo)> = None;
            for database in &catalog.databases {
                for stored in &database.tables {
                    if stored.id == *table_id
                        || stored.get_partition_info().is_some_and(|pi| {
                            pi.read().definitions.with_visible(|definitions| {
                                definitions.iter().any(|p| p.id == *table_id)
                            })
                        })
                    {
                        found = Some((database.info.id, stored.clone_like_go()));
                    }
                }
            }
            let Some((db_id, mut info)) = found else {
                // Go answers ErrTableNotExists and the poller just moves on.
                return Err(DdlPlanError::TableNotExists {
                    schema: String::new(),
                    table: format!("(physical id {table_id})"),
                });
            };
            let Some(replica) = info.tiflash_replica.as_ref() else {
                return Err(DdlPlanError::Unsupported(
                    "the table carries no TiFlash replica to mark".to_owned(),
                ));
            };
            let previous = if info.id == *table_id {
                replica.read().available
            } else {
                replica.read().is_partition_available(*table_id)
            };
            if previous == *available {
                return Ok(DdlPlan::AlreadySatisfied {
                    detail: "TiFlash replica status already matches".to_owned(),
                    warnings: Vec::new(),
                });
            }
            if info.id == *table_id {
                replica.write().available = *available;
            } else {
                let partition = info
                    .get_partition_info()
                    .expect("physical ID resolved in partitions");
                let mut replica = replica.write();
                let mut ids: Vec<_> = replica.available_partition_ids.iter().copied().collect();
                if *available {
                    ids.push(*table_id);
                    replica.available = partition.read().definitions.with_visible(|definitions| {
                        definitions.iter().all(|p| ids.contains(&p.id))
                    });
                } else {
                    ids.retain(|id| id != table_id);
                    replica.available = false;
                }
                replica.available_partition_ids = ids.into();
            }
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, info.id),
                value::serialize_table_info(&info)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?);
            diff.action_type = ActionType::ACTION_UPDATE_TI_FLASH_REPLICA_STATUS;
            diff.schema_id = db_id;
            diff.table_id = info.id;
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
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                value::serialize_table_info(&info)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?);
            let random_key = key::auto_random_table_id_kv_key(db_id, table_id);
            let current = if converting && stored.sep_auto_inc() {
                writes.push(BufferMutation::set(
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
                writes.push(BufferMutation::set(
                    random_key,
                    value::encode_int_value(rebased_random as i64),
                )?);
            }
            if converting {
                let row_id_key = key::auto_table_id_kv_key(db_id, table_id);
                if snapshot.get(&row_id_key)?.is_some() {
                    writes.push(BufferMutation::delete(row_id_key)?);
                }
            }
            diff.action_type = ActionType::ACTION_MODIFY_COLUMN;
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
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
        DdlStatement::ReorganizePartitions {
            schema,
            table,
            action,
            context,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            // The table the loader serves, planned against without reading a
            // row: Go's executor checks and the job's first state read only
            // its metadata.
            let detached = tidb_executor::cluster_storage::ClusterTableStorage::new(
                tidb_executor::cluster_storage::MutationBuffer::new(),
                std::sync::Arc::new(std::sync::Mutex::new(
                    tidb_executor::cluster_storage::SwappableSnapshot::new(),
                )),
            );
            let kv = crate::table_from_meta::cluster_table(
                stored,
                &detached,
                &crate::table_from_meta::AutoIdSource::Unavailable,
            )
            .map_err(|reason| {
                DdlPlanError::Unsupported(format!(
                    "`{schema}`.`{table}` cannot be loaded: {reason}"
                ))
            })?;
            let mut policies = Vec::new();
            for (_, encoded) in snapshot.scan_prefix(&key::policies_kv_prefix())? {
                policies.push(
                    value::parse_policy_info(&encoded)
                        .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
                );
            }
            let mut host = ClusterReorgHost {
                global_ids: &mut global_ids,
                policies,
                failed: None,
            };
            let ctx = &context.0;
            let planned = match action.as_ref() {
                tidb_ast::AlterPartitionAction::Reorganize {
                    names, definitions, ..
                } => tidb_executor::ddl::plan_reorganize_partition(
                    &kv,
                    names,
                    definitions,
                    ctx,
                    &mut host,
                ),
                tidb_ast::AlterPartitionAction::Repartition(partitioning) => {
                    tidb_executor::ddl::plan_alter_table_partitioning(
                        &kv,
                        partitioning,
                        ctx,
                        &mut host,
                    )
                }
                tidb_ast::AlterPartitionAction::RemovePartitioning => {
                    tidb_executor::ddl::plan_remove_partitioning(&kv, &mut host)
                }
                tidb_ast::AlterPartitionAction::Coalesce { count, .. } => {
                    tidb_executor::ddl::plan_hash_partition_management(
                        &kv,
                        tidb_executor::ddl::HashManagement::Coalesce(*count),
                        ctx,
                        &mut host,
                    )
                }
                tidb_ast::AlterPartitionAction::Add { spec, .. } => {
                    let change = match spec {
                        tidb_ast::AddPartitionSpec::Definitions(definitions) => {
                            tidb_executor::ddl::HashManagement::Add {
                                count: 0,
                                definitions,
                            }
                        }
                        tidb_ast::AddPartitionSpec::Count(count) => {
                            tidb_executor::ddl::HashManagement::Add {
                                count: *count,
                                definitions: &[],
                            }
                        }
                    };
                    tidb_executor::ddl::plan_hash_partition_management(&kv, change, ctx, &mut host)
                }
                other => unreachable!("lowered only for a reorganization, got {other:?}"),
            };
            if let Some(failed) = host.failed.take() {
                return Err(failed);
            }
            let planned = planned.map_err(|error| {
                let error = error.to_mysql_error();
                coded_refusal(error.code, error.message)
            })?;
            let reorg = planned.reorg;
            let old_table_id = stored.id;
            let mut info = stored.clone_like_go();
            let old_definitions = stored
                .partition
                .as_ref()
                .filter(|partition| partition.read().enable)
                .map(|partition| partition.read().definitions.snapshot())
                .unwrap_or_default();
            // Go's final state: the definitions the job added replace the ones
            // it dropped ...
            let (dropped, added) = match (&planned.stored, info.partition.as_ref()) {
                (Some(final_metadata), Some(current)) if reorg.new_table_id.is_none() => {
                    let rebuilt = crate::table_info_build::partition_info(final_metadata);
                    let final_definitions = rebuilt.definitions.snapshot();
                    {
                        let mut current = current.write();
                        // Go `tblInfo.Partition.Num = uint64(len(newDefs))`.
                        current.num = final_definitions.len() as u64;
                        current.definitions = final_definitions.clone().into();
                    }
                    let kept =
                        |id: i64| old_definitions.iter().any(|definition| definition.id == id);
                    (
                        old_definitions
                            .iter()
                            .filter(|definition| reorg.source_ids.contains(&definition.id))
                            .map(|definition| definition.clone_like_go())
                            .collect::<Vec<_>>(),
                        final_definitions
                            .into_iter()
                            .filter(|definition| !kept(definition.id))
                            .collect::<Vec<_>>(),
                    )
                }
                (Some(final_metadata), _) => {
                    let rebuilt = crate::table_info_build::partition_info(final_metadata);
                    let added = rebuilt.definitions.snapshot();
                    info.partition = Some(GoShared::new(rebuilt));
                    (old_definitions.clone(), added)
                }
                (None, _) => {
                    info.partition = None;
                    (old_definitions.clone(), Vec::new())
                }
            };
            // ... every recreated index moves to the end under its new id
            // (Go appends the duplicate and drops the original) ...
            for recreated in &reorg.recreated_indexes {
                let Some(position) = info
                    .indices
                    .iter_deref()
                    .position(|index| index.read().id == recreated.old_id)
                else {
                    continue;
                };
                let index = info
                    .indices
                    .get(position)
                    .expect("the position was just found")
                    .read()
                    .clone_like_go();
                info.indices.delete_go(position, position + 1);
                let mut index = index;
                index.id = recreated.new_id;
                index.global = recreated.global;
                index.global_index_version = recreated.global_index_version;
                info.max_index_id = info.max_index_id.max(index.id);
                info.indices.push_go(index);
            }
            // ... and PARTITION BY / REMOVE PARTITIONING give the table its
            // `NewTableID`, carrying the auto-id counters across.
            let mut old_physical_ids = reorg.source_ids.clone();
            let mut counter_moves = Vec::new();
            if let Some(new_table_id) = reorg.new_table_id {
                writes.push(BufferMutation::delete(key::table_kv_key(
                    db_id,
                    old_table_id,
                ))?);
                for key_for in [
                    key::auto_table_id_kv_key as fn(i64, i64) -> Vec<u8>,
                    key::auto_increment_id_kv_key,
                    key::auto_random_table_id_kv_key,
                ] {
                    counter_moves
                        .push((key_for(db_id, old_table_id), key_for(db_id, new_table_id)));
                }
                if old_physical_ids.first() != Some(&old_table_id) {
                    old_physical_ids.push(old_table_id);
                }
                info.id = new_table_id;
            }
            check_table_info_valid(&info)?;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            let new_physical_ids = match reorg.new_table_id {
                Some(new_table_id) if info.partition.is_none() => vec![new_table_id],
                _ => added.iter().map(|definition| definition.id).collect(),
            };
            if !new_physical_ids.is_empty() {
                // Go `buildPlacementAffects`, both lists padded to one length.
                let width = old_physical_ids.len().max(new_physical_ids.len());
                diff.affected_options = GoSharedPointerSlice::from(
                    (0..width)
                        .map(|offset| tidb_model::AffectedOption {
                            old_table_id: old_physical_ids.get(offset).copied().unwrap_or(0),
                            table_id: new_physical_ids.get(offset).copied().unwrap_or(0),
                            ..tidb_model::AffectedOption::default()
                        })
                        .collect::<Vec<_>>(),
                );
            }
            diff.action_type = match action.as_ref() {
                tidb_ast::AlterPartitionAction::Reorganize { .. }
                | tidb_ast::AlterPartitionAction::Coalesce { .. }
                | tidb_ast::AlterPartitionAction::Add { .. } => {
                    ActionType::ACTION_REORGANIZE_PARTITION
                }
                tidb_ast::AlterPartitionAction::Repartition(_) => {
                    ActionType::ACTION_ALTER_TABLE_PARTITIONING
                }
                _ => ActionType::ACTION_REMOVE_PARTITIONING,
            };
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if reorg.new_table_id.is_some() {
                diff.old_table_id = old_table_id;
            }
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
                let event = match diff.action_type {
                    ActionType::ACTION_REORGANIZE_PARTITION => {
                        SchemaChangeEvent::reorganize_partitions(
                            info.clone_like_go(),
                            event_partition_info(added),
                            event_partition_info(dropped),
                        )
                    }
                    ActionType::ACTION_ALTER_TABLE_PARTITIONING => {
                        SchemaChangeEvent::add_partitioning(
                            old_table_id,
                            info.clone_like_go(),
                            event_partition_info(added),
                        )
                    }
                    _ => SchemaChangeEvent::remove_partitioning(
                        old_table_id,
                        info.clone_like_go(),
                        event_partition_info(dropped),
                    ),
                };
                schema_change_events.push((-1, event));
            }
            if let Some(warning) = planned.warning {
                warnings.push((DdlWarningLevel::Warning, 1105, warning.to_owned()));
            }
            if let Some((original, replacement)) = planned.interval_rewrite {
                context.0.rewrite_ddl_job_query(original, replacement);
            }
            partition_reorg = Some(PartitionReorgStep {
                table: Box::new(stored.clone_like_go()),
                reorg,
                context: context.clone(),
                db_id,
                counter_moves,
            });
        }
        DdlStatement::AddPartitions {
            schema, table, sql, ..
        }
        | DdlStatement::DropPartitions { schema, table, sql } => {
            let adding = matches!(statement, DdlStatement::AddPartitions { .. });
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            // Go `AddTablePartitions`: on a HASH/KEY table an ADD is a
            // reorganization of every partition (`hashPartitionManagement`),
            // whichever form it was written in.
            if let DdlStatement::AddPartitions {
                action, context, ..
            } = statement
            {
                let hash_or_key = stored.partition.as_ref().is_some_and(|partition| {
                    let partition = partition.read();
                    partition.enable
                        && matches!(
                            partition.partition_type,
                            tidb_ast::PartitionType::HASH | tidb_ast::PartitionType::KEY
                        )
                });
                if hash_or_key
                    && matches!(action.as_ref(), tidb_ast::AlterPartitionAction::Add { .. })
                {
                    return plan_ddl_with_auto_ids(
                        snapshot,
                        &DdlStatement::ReorganizePartitions {
                            schema: schema.clone(),
                            table: table.clone(),
                            action: action.clone(),
                            context: context.clone(),
                        },
                        start_ts,
                        use_new_collation,
                        auto_ids,
                        cdc_write_source,
                    );
                }
            }
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
                        storage_class_tier: definition.storage_class.0,
                        storage_class_transitions: definition.storage_class.1.into(),
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
            if !adding {
                if let Some(replica) = &info.tiflash_replica {
                    let remaining = info
                        .partition
                        .as_ref()
                        .unwrap()
                        .read()
                        .definitions
                        .snapshot();
                    let mut replica = replica.write();
                    replica.available_partition_ids = replica
                        .available_partition_ids
                        .iter()
                        .copied()
                        .filter(|id| remaining.iter().any(|p| p.id == *id))
                        .collect::<Vec<_>>()
                        .into();
                }
            }
            check_table_info_valid(&info)?;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
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
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
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
            check_constraints,
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
                    check_constraints: *check_constraints,
                });
            }
            exchange_partition_label_swap = Some(ExchangePartitionLabelSwap {
                partitioned_schema: schema.go_to_lower(),
                partitioned_table: stored_partitioned.name.lowercase().to_owned(),
                partition: original_definition.name.lowercase().to_owned(),
                standalone_schema: standalone_schema.go_to_lower(),
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

            writes.push(BufferMutation::delete(key::table_kv_key(
                standalone_db_id,
                original_standalone_id,
            ))?);
            writes.push(BufferMutation::set(
                key::table_kv_key(partitioned_db_id, partitioned_info.id),
                value::serialize_table_info(&partitioned_info)
                    .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
            )?);
            writes.push(BufferMutation::set(
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
                writes.push(BufferMutation::set(
                    partitioned_key,
                    value::encode_int_value(maximum),
                )?);
                writes.push(BufferMutation::set(
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
            warnings = vec![(
                DdlWarningLevel::Warning,
                1105,
                "after the exchange, please analyze related table of the exchange to update statistics"
                    .to_owned(),
            )];
            if !tidb_metadef::is_mem_or_sys_db(&standalone_schema.go_to_lower()) {
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
                        storage_class_tier: definition.storage_class.0,
                        storage_class_transitions: definition.storage_class.1.into(),
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
            if let Some(replica) = &info.tiflash_replica {
                let remaining = info
                    .partition
                    .as_ref()
                    .unwrap()
                    .read()
                    .definitions
                    .snapshot();
                let mut replica = replica.write();
                replica.available = false;
                replica.available_partition_ids = replica
                    .available_partition_ids
                    .iter()
                    .copied()
                    .filter(|id| remaining.iter().any(|p| p.id == *id))
                    .collect::<Vec<_>>()
                    .into();
            }
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_TRUNCATE_TABLE_PARTITION;
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
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
                AlterColumnOutcome::AlreadySatisfied(detail) => {
                    // go's add-column IF NOT EXISTS no-op carries the 1060
                    // note (oracle g-alter: `Note:1060:Duplicate column
                    // name 'd'`).
                    return Ok(already_with_warnings(
                        detail,
                        vec![(
                            crate::real_tikv_ddl::DdlWarningLevel::Note,
                            1060,
                            format!("Duplicate column name '{}'", column.name),
                        )],
                    ));
                }
                AlterColumnOutcome::Applied => {}
            }
            crate::cluster_bdr::admit_add_column(&bdr_role, schema, &column.options)?;
            check_table_info_valid(&info)?;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_ADD_COLUMN;
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
                let added = info
                    .columns
                    .iter_deref()
                    .find(|candidate| {
                        candidate.read().name.lowercase() == column.name.go_to_lower()
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
            allow_remove_auto_inc,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            if !check_is_droppable_column(
                &catalog,
                schema,
                stored,
                column,
                *if_exists,
                *allow_remove_auto_inc,
            )? {
                // go's drop-column IF EXISTS no-op carries the 1091 note
                // (oracle g-alter: `Note:1091:Can't DROP 'no_such';
                // check that column/key exists`).
                return Ok(already_with_warnings(
                    format!("column `{column}` does not exist on `{schema}`.`{table}`"),
                    vec![(
                        crate::real_tikv_ddl::DdlWarningLevel::Note,
                        1091,
                        format!("Can't DROP '{column}'; check that column/key exists"),
                    )],
                ));
            }
            check_visible_column_cnt(stored, 0, 1)?;
            let mut info = stored.clone_like_go();
            apply_drop_column(&mut info, column);
            plan_removed_column_indexes(stored, &info, use_new_collation, &mut backfill);
            check_table_info_valid(&info)?;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
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
            let Some(column) = prepare_rename_column(stored, table, from, to)? else {
                return Ok(already(format!("column `{from}` already has the requested name")));
            };
            let mut info = stored.clone_like_go();
            apply_prepared_column(&mut info, column, &tidb_ast::ColumnPosition::Default, false)?;
            check_table_info_valid(&info)?;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_MODIFY_COLUMN;
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
                let modified = info
                    .columns
                    .iter_deref()
                    .find(|candidate| candidate.read().name.lowercase() == to.go_to_lower())
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
            modify_column_type,
        } => {
            use tidb_executor::ddl::modify_column_type::ModifyColumnType;
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let PreparedModifyColumn {
                column: prepared,
                kind,
                precheck,
                check_value_range,
            } = prepare_modify_column(
                stored,
                table,
                column,
                requested_position,
                context,
                rename_from.as_deref(),
                *modify_column_type,
            )?;
            let old_id = prepared.id;
            let mut info = stored.clone_like_go();
            apply_prepared_column(&mut info, prepared, requested_position, false)?;
            let (new_column_id, renumbered_indexes) =
                renumber_changed_column(&mut info, old_id, &column.name, kind);
            admit_modified_column_bdr(
                &bdr_role,
                schema,
                stored,
                &info,
                column,
                rename_from.as_deref(),
            )?;
            let new_name = column.name.go_to_lower();
            check_table_info_valid(&info)?;
            info.update_ts = start_ts;
            if kind != ModifyColumnType::NoReorg {
                column_reorgs.push(ColumnReorg {
                    table: Box::new(stored.clone_like_go()),
                    column_id: old_id,
                    after: Box::new(info.clone_like_go()),
                    new_column_id,
                    renumbered_indexes,
                    kind,
                    precheck,
                    check_value_range,
                    use_new_collation,
                    context: context.clone(),
                    action_offset: None,
                });
            }
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_MODIFY_COLUMN;
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
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
            let mut prepared =
                prepare_cluster_columns(&catalog, stored, schema, table, actions, &mut warnings)?;
            let mut info = stored.clone_like_go();
            let mut applied = 0usize;
            let mut satisfied = Vec::new();
            let mut enforced_check = None;
            // Go `mergeAddIndex` gathers multiple ADD INDEX sub-jobs into one
            // merged sub-job at the end, preserving every other sub-job's
            // relative order. Execute the equivalent order here so later
            // metadata, backfill snapshots, and notifier sequence IDs agree.
            // Go merges `ActionAddIndex` sub-jobs only; an ADD PRIMARY KEY is
            // `ActionAddPrimaryKey` and keeps its place.
            let is_mergeable_add_index = |action: &AlterColumnAction| matches!(action, AlterColumnAction::AddIndex { index, .. } if !index.primary);
            let add_index_count = actions
                .iter()
                .enumerate()
                .filter(|(offset, action)| {
                    !prepared.skipped.contains_key(offset) && is_mergeable_add_index(action)
                })
                .count();
            // Go `mergeAddIndex` keeps the order when a key is added: the key
            // needs the index queued for it to come first.
            let merge_add_index = add_index_count > 1
                && !actions
                    .iter()
                    .any(|action| matches!(action, AlterColumnAction::AddForeignKey { .. }));
            let mut action_order: Vec<_> = (0..actions.len()).filter(|offset| !prepared.skipped.contains_key(offset)).collect();
            if merge_add_index {
                action_order
                    .sort_by_key(|offset| usize::from(is_mergeable_add_index(&actions[*offset])));
            }
            // Go `onMultiSchemaChange`'s `SubActionTypes`: each sub-job's
            // type in order, the merged ADD INDEX one last; `SubmitBatch`'s
            // BDR admission reads the same list.
            let mut sub_action_types = Vec::with_capacity(action_order.len());
            for offset in &action_order {
                let action_type = match &actions[*offset] {
                    AlterColumnAction::AddIndex { .. }
                        if merge_add_index && is_mergeable_add_index(&actions[*offset]) =>
                    {
                        continue
                    }
                    AlterColumnAction::AddIndex { index, .. } if index.primary => {
                        ActionType::ACTION_ADD_PRIMARY_KEY
                    }
                    AlterColumnAction::AddIndex { .. } => ActionType::ACTION_ADD_INDEX,
                    AlterColumnAction::Add { .. } => ActionType::ACTION_ADD_COLUMN,
                    AlterColumnAction::Modify { .. } | AlterColumnAction::Rename { .. } => {
                        ActionType::ACTION_MODIFY_COLUMN
                    }
                    AlterColumnAction::Drop { .. } => ActionType::ACTION_DROP_COLUMN,
                    AlterColumnAction::DropIndex { .. } => ActionType::ACTION_DROP_INDEX,
                    AlterColumnAction::DropPrimaryKey => ActionType::ACTION_DROP_PRIMARY_KEY,
                    AlterColumnAction::AddForeignKey { .. } => {
                        // The index Go queues for the key's columns comes
                        // first.
                        if prepared
                            .foreign_keys
                            .get(offset)
                            .is_some_and(|created| created.needs_index)
                        {
                            sub_action_types.push(ActionType::ACTION_ADD_INDEX);
                        }
                        ActionType::ACTION_ADD_FOREIGN_KEY
                    }
                    AlterColumnAction::DropForeignKey { .. } => ActionType::ACTION_DROP_FOREIGN_KEY,
                    AlterColumnAction::AddCheck { .. } => ActionType::ACTION_ADD_CHECK_CONSTRAINT,
                    AlterColumnAction::RenameIndex { .. } => ActionType::ACTION_RENAME_INDEX,
                    AlterColumnAction::IndexVisibility { .. } => {
                        ActionType::ACTION_ALTER_INDEX_VISIBILITY
                    }
                    AlterColumnAction::SetDefault { .. } => ActionType::ACTION_SET_DEFAULT_VALUE,
                    AlterColumnAction::RebaseAutoId { .. } => ActionType::ACTION_REBASE_AUTO_ID,
                    AlterColumnAction::TableComment { .. } => {
                        ActionType::ACTION_MODIFY_TABLE_COMMENT
                    }
                    AlterColumnAction::TableCharset { .. } => {
                        ActionType::ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE
                    }
                };
                sub_action_types.push(action_type);
            }
            if merge_add_index {
                sub_action_types.push(ActionType::ACTION_ADD_INDEX);
            }
            let active_action_count = action_order.len();
            // Go's multi-schema worker steps every sub-job through its
            // revertible states before any finishes: each ADD COLUMN's column
            // is in `Columns` by then, while a DROP COLUMN only marks itself
            // non-revertible. A drop's `len(Columns)` check therefore sees the
            // columns added before it (that first step) and every added
            // column less the drops finished before it (its later steps).
            let mut pending_added_columns = action_order
                .iter()
                .filter(|offset| matches!(actions[**offset], AlterColumnAction::Add { .. }))
                .count();
            let mut applied_added_columns = 0usize;
            let mut rebased = None;
            satisfied.extend(prepared.skipped.values().cloned());
            let mut merged_added_indexes = Vec::new();
            for (sequence, action_offset) in action_order.into_iter().enumerate() {
                let action = &actions[action_offset];
                let outcome = match action {
                    AlterColumnAction::Add { .. } | AlterColumnAction::Modify { .. } | AlterColumnAction::Rename { .. } => {
                        let (column, position, add) = prepared.columns.remove(&action_offset)
                            .expect("every active column job was prepared");
                        let name = column.name.clone();
                        let old_id = column.id;
                        // A MODIFY / CHANGE sub-job whose data changes reads
                        // the rows as the sub-jobs before it left them.
                        let reorg = prepared
                            .modify_kinds
                            .get(&action_offset)
                            .copied()
                            .filter(|(kind, ..)| {
                                *kind != tidb_executor::ddl::modify_column_type::ModifyColumnType::NoReorg
                            })
                            .map(|step| (step, info.clone_like_go()));
                        apply_prepared_column(&mut info, column, &position, add)?;
                        if let Some(((kind, precheck, check_value_range), before)) = reorg {
                            let AlterColumnAction::Modify { context, .. } = action else {
                                unreachable!("only a MODIFY / CHANGE sub-job changes data");
                            };
                            let (new_column_id, renumbered_indexes) =
                                renumber_changed_column(&mut info, old_id, name.original(), kind);
                            column_reorgs.push(ColumnReorg {
                                table: Box::new(before),
                                column_id: old_id,
                                after: Box::new(info.clone_like_go()),
                                new_column_id,
                                renumbered_indexes,
                                kind,
                                precheck,
                                check_value_range,
                                use_new_collation,
                                context: context.clone(),
                                action_offset: Some(action_offset),
                            });
                        }
                        if add {
                            pending_added_columns -= 1;
                            applied_added_columns += 1;
                        }
                        match action {
                            AlterColumnAction::Add {
                                column: definition, ..
                            } => {
                                crate::cluster_bdr::admit_add_column(
                                    &bdr_role,
                                    schema,
                                    &definition.options,
                                )?;
                            }
                            AlterColumnAction::Modify {
                                column: definition,
                                rename_from,
                                ..
                            } => {
                                admit_modified_column_bdr(
                                    &bdr_role,
                                    schema,
                                    stored,
                                    &info,
                                    definition,
                                    rename_from.as_deref(),
                                )?;
                            }
                            _ => {}
                        }
                        if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
                            let changed = tidb_model::column::find_column_info(&info.columns, name.original())
                                .expect("applied column is present").read().clone_like_go();
                            let event = if add {
                                SchemaChangeEvent::add_columns(info.clone_like_go(), vec![changed])
                            } else {
                                SchemaChangeEvent::modify_columns(info.clone_like_go(), vec![changed], false)
                            };
                            schema_change_events.push((sequence as i64, event));
                        }
                        AlterColumnOutcome::Applied
                    }
                    AlterColumnAction::Drop { column, .. } => {
                        let column_count = usize::min(
                            stored.columns.len() + applied_added_columns,
                            info.columns.len() + pending_added_columns,
                        );
                        check_drop_column(&catalog, schema, &info, column, column_count)?;
                        let before = info.clone_like_go();
                        apply_drop_column(&mut info, column);
                        plan_removed_column_indexes(
                            &before,
                            &info,
                            use_new_collation,
                            &mut backfill,
                        );
                        AlterColumnOutcome::Applied
                    }
                    AlterColumnAction::AddIndex {
                        index,
                        expressions,
                        condition,
                        context,
                        ..
                    } => {
                        // Names were admitted against the original table, as
                        // Go builds every sub-job from it; offsets now account
                        // for preceding disjoint jobs.
                        let name = settle_added_index_name(stored, index, expressions);
                        warn_missing_region_split_policy(stored, &name, &context.0);
                        let mut added = plan_added_index(
                            &mut info,
                            index,
                            expressions,
                            condition.as_ref(),
                            &name,
                            &context.0,
                        )?;
                        check_partial_index_fast_reorg(schema, &added)?;
                        // The backfill decodes EXISTING rows against the
                        // evolved columns (a bundle-added column reads its
                        // origin default, an expression part its hidden
                        // column) but must not walk the new index.
                        let backfill_table = Box::new(info.clone_like_go());
                        allocate_added_index(&mut info, &mut added);
                        let added = GoShared::new(added);
                        info.indices.push_handle_go(Some(added.clone()));
                        backfill.push(IndexBackfill {
                            table: backfill_table,
                            index: added.clone(),
                            use_new_collation,
                            operation: IndexBackfillOperation::Add(context.clone()),
                        });
                        if merge_add_index {
                            merged_added_indexes.push(added.read().clone_like_go());
                        } else if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
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
                        // Go `checkIndexNeededInForeignKeyInOwner`.
                        if tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::SeqCst)
                        {
                            crate::foreign_key_build::check_index_needed_in_foreign_key(
                                &catalog, schema, &info, &dropped,
                            )
                            .map_err(DdlPlanError::Admission)?;
                        }
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
                            operation: IndexBackfillOperation::Drop,
                        });
                        applied += 1;
                        continue;
                    }
                    AlterColumnAction::AddForeignKey { .. } => {
                        let created = prepared
                            .foreign_keys
                            .remove(&action_offset)
                            .expect("every ADD FOREIGN KEY was prepared");
                        foreign_key_checks.extend(on_create_foreign_key(
                            &catalog,
                            schema,
                            &mut info,
                            &created,
                            use_new_collation,
                            &mut backfill,
                        )?);
                        applied += 1;
                        continue;
                    }
                    AlterColumnAction::DropForeignKey { name } => {
                        // Go `onDropForeignKey`.
                        drop_foreign_key(&mut info, name)?;
                        applied += 1;
                        continue;
                    }
                    AlterColumnAction::AddCheck {
                        definition,
                        context,
                    } => {
                        // Go admits the CHECK against original metadata before
                        // fillMultiSchemaInfo rejects its unsupported job kind.
                        let mut candidate = stored.clone_like_go();
                        let prior_len = candidate.constraints.len();
                        crate::table_info_build::append_check_constraints(
                            &mut candidate,
                            &[tidb_executor::ddl::check_constraint::CheckConstraintInput {
                                definition: (**definition).clone(),
                                in_column: None,
                            }],
                            tidb_executor::ddl::check_constraint::CheckConstraintBuildMode::Alter,
                            &context.0,
                        )
                        .map_err(DdlPlanError::Admission)?;
                        info = candidate;
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
                    AlterColumnAction::DropPrimaryKey => {
                        apply_drop_primary_key(
                            &catalog,
                            schema,
                            &mut info,
                            use_new_collation,
                            &mut backfill,
                        )?;
                        AlterColumnOutcome::Applied
                    }
                    AlterColumnAction::RenameIndex { from, to } => {
                        apply_rename_index(&mut info, schema, table, from, to)?
                    }
                    AlterColumnAction::IndexVisibility { name, invisible } => {
                        apply_index_visibility(&mut info, schema, table, name, *invisible)?
                    }
                    AlterColumnAction::SetDefault {
                        column,
                        default_value,
                        context,
                    } => {
                        apply_set_column_default(
                            &mut info,
                            table,
                            column,
                            default_value.as_deref(),
                            &context.0,
                        )?;
                        AlterColumnOutcome::Applied
                    }
                    AlterColumnAction::RebaseAutoId { new_base, force } => {
                        rebase_auto_increment_id(
                            snapshot,
                            auto_ids,
                            db_id,
                            &mut info,
                            *new_base,
                            *force,
                            &mut rebased,
                            &mut writes,
                            &mut warnings,
                        )?;
                        AlterColumnOutcome::Applied
                    }
                    // Go `onModifyTableComment`.
                    AlterColumnAction::TableComment { comment } => {
                        info.comment = comment.clone();
                        AlterColumnOutcome::Applied
                    }
                    AlterColumnAction::TableCharset {
                        charset,
                        collate,
                        overwrite_columns,
                    } => {
                        check_table_charset_named(charset, collate)?;
                        apply_table_charset_and_collate(
                            &catalog,
                            schema,
                            table,
                            &mut info,
                            charset,
                            collate,
                            *overwrite_columns,
                        )?
                    }
                };
                match outcome {
                    AlterColumnOutcome::Applied => applied += 1,
                    AlterColumnOutcome::AlreadySatisfied(detail) => satisfied.push(detail),
                }
            }
            if !merged_added_indexes.is_empty()
                && !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower())
            {
                let sequence = active_action_count - add_index_count;
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
                return Ok(already_with_warnings(satisfied.join("; "), warnings));
            }
            if let Some((constraint_name, context)) = enforced_check {
                check_constraint_validation = Some(CheckConstraintValidation {
                    table: Box::new(info.clone_like_go()),
                    constraint_name,
                    context,
                });
            }
            check_table_info_valid(&info)?;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_MULTI_SCHEMA_CHANGE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
            diff.sub_action_types = sub_action_types.into();
        }
        DdlStatement::TruncateTable { schema, table } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            // Go `TruncateTable`: a view or a sequence has no rows to
            // truncate and reads as missing.
            if stored.view.is_some() || stored.is_sequence() {
                let database = find_database(&catalog, schema).expect("located above");
                return Err(DdlPlanError::TableNotExists {
                    schema: database.info.name.original().to_owned(),
                    table: stored.name.original().to_owned(),
                });
            }
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
            // The new table ID, which a session holding a lock on the old
            // one moves its lock to (Go `HandleLockTablesOnSuccessSubmit`).
            created_id = Some(new_table_id);
            let mut info = stored.clone_like_go();
            info.id = new_table_id;
            if let Some(replica) = &info.tiflash_replica {
                let mut replica = replica.write();
                replica.available = false;
                replica.available_partition_ids = Default::default();
            }
            if let Some(partition) = &info.partition {
                let partition = partition.read();
                for (ordinal, id) in ids[1..].iter().enumerate() {
                    partition
                        .definitions
                        .update(ordinal, |definition| definition.id = *id);
                }
            }
            info.update_ts = start_ts;
            writes.push(BufferMutation::delete(key::table_kv_key(
                db_id,
                old_table_id,
            ))?);
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
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
                    writes.push(BufferMutation::delete(allocator)?);
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
                        DdlPlanError::Admission(DdlAdmissionError::with_code(
                            GENERIC_ERROR_CODE,
                            format!("building placement rules: {error}"),
                        ))
                    })?;
            }
            diff.action_type = ActionType::ACTION_TRUNCATE_TABLE;
            diff.schema_id = db_id;
            diff.table_id = new_table_id;
            diff.old_table_id = old_table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
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
                    // go `ErrBadTable` demoted to a note: `Unknown table 'd.t'`.
                    return Ok(already_with_warnings(
                        format!("table `{schema}`.`{table}` does not exist"),
                        vec![missing_table_note(schema, table)],
                    ));
                }
                return Err(DdlPlanError::UnknownTables(vec![format!(
                    "{schema}.{table}"
                )]));
            };
            let Some(stored) = find_table(database, table) else {
                if *if_exists {
                    return Ok(already_with_warnings(
                        format!("table `{schema}`.`{table}` does not exist"),
                        vec![missing_table_note(schema, table)],
                    ));
                }
                return Err(DdlPlanError::UnknownTable {
                    schema: schema.clone(),
                    table: table.clone(),
                });
            };
            if !check_cluster_drop_object(schema, table, stored, false)? {
                if *if_exists {
                    return Ok(already_with_warnings(
                        format!("table `{schema}`.`{table}` does not exist"),
                        vec![missing_table_note(schema, table)],
                    ));
                }
                return Err(DdlPlanError::UnknownTables(vec![format!(
                    "{schema}.{table}"
                )]));
            }
            let db_id = database.info.id;
            let table_id = stored.id;
            writes.push(BufferMutation::delete(key::table_kv_key(db_id, table_id))?);
            // Go `GetAutoIDAccessors(dbID, tblID).Del()` removes the three
            // allocator fields with the table; each is deleted only if this
            // snapshot observed it, exactly as `HDel` does.
            for allocator in [
                key::auto_table_id_kv_key(db_id, table_id),
                key::auto_increment_id_kv_key(db_id, table_id),
                key::auto_random_table_id_kv_key(db_id, table_id),
            ] {
                if snapshot.get(&allocator)?.is_some() {
                    writes.push(BufferMutation::delete(allocator)?);
                }
            }
            diff.action_type = ActionType::ACTION_DROP_TABLE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
                schema_change_events
                    .push((-1, SchemaChangeEvent::drop_table(stored.clone_like_go())));
            }
        }
        DdlStatement::AcceptedNoOp {
            schema,
            table,
            warning,
        } => {
            let warnings = warning
                .as_ref()
                .map(|(code, message)| {
                    vec![(
                        crate::real_tikv_ddl::DdlWarningLevel::Warning,
                        *code,
                        message.clone(),
                    )]
                })
                .unwrap_or_default();
            return Ok(already_with_warnings(
                format!("ALTER TABLE `{schema}`.`{table}` changed nothing"),
                warnings,
            ));
        }
        DdlStatement::DropTables { .. } | DdlStatement::DropSequences { .. } => {
            return Err(DdlPlanError::Unsupported(
                "multi-target DROP must use the ordered DDL completion owner".into(),
            ));
        }
        DdlStatement::CreateSequence {
            schema,
            name,
            if_not_exists,
            info,
        } => {
            let Some(database) = find_database(&catalog, schema) else {
                return Err(DdlPlanError::UnknownDatabase(schema.clone()));
            };
            if find_table(database, name).is_some() {
                // A sequence shares the table namespace (Go
                // `checkTableNotExists`); `IF NOT EXISTS` demotes the 1050 to
                // a note (`OnExistIgnore`).
                if *if_not_exists {
                    return Ok(already_with_warnings(
                        format!("`{schema}`.`{name}` already exists"),
                        vec![(
                            DdlWarningLevel::Note,
                            tidb_error::tidb::errcode::ErrTableExists,
                            format!("Table '{schema}.{name}' already exists"),
                        )],
                    ));
                }
                return Err(DdlPlanError::TableExists {
                    schema: schema.clone(),
                    table: name.clone(),
                });
            }
            let db_id = database.info.id;
            let table_id = global_ids.allocate(1)?[0];
            created_id = Some(table_id);
            let mut info = (**info).clone();
            info.id = table_id;
            info.state = SchemaState::PUBLIC;
            info.update_ts = start_ts;
            // Go `createSequenceWithCheck`.
            check_table_info_valid(&info)?;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            // Go `CreateSequenceAndSetSeqValue`.
            let sequence = info
                .sequence
                .as_ref()
                .expect("a CREATE SEQUENCE carries its options")
                .read()
                .clone();
            writes.push(BufferMutation::set(
                sequence_value_kv_key(db_id, table_id),
                value::encode_int_value(sequence_base_for(sequence.increment, sequence.start)),
            )?);
            diff.action_type = ActionType::ACTION_CREATE_SEQUENCE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::AlterSequence {
            schema,
            name,
            if_exists,
            options,
        } => {
            // Go `AlterSequence`, in its order.
            let Some(database) = find_database(&catalog, schema) else {
                return Err(DdlPlanError::UnknownDatabase(schema.clone()));
            };
            let Some(stored) = find_table(database, name) else {
                if *if_exists {
                    return Ok(already_with_warnings(
                        format!("sequence `{schema}`.`{name}` does not exist"),
                        vec![(
                            DdlWarningLevel::Note,
                            tidb_error::tidb::errcode::ErrNoSuchTable,
                            format!("Table '{schema}.{name}' doesn't exist"),
                        )],
                    ));
                }
                return Err(DdlPlanError::TableNotExists {
                    schema: schema.clone(),
                    table: name.clone(),
                });
            };
            let Some(old) = stored.sequence.as_ref().map(|shared| shared.read().clone()) else {
                return Err(coded_refusal(
                    tidb_error::tidb::errcode::ErrWrongObject,
                    format!("'{schema}.{name}' is not SEQUENCE"),
                ));
            };
            let db_id = database.info.id;
            let (options, restart) = tidb_executor::alter_sequence_options(
                sequence_options_of(&old),
                options,
                schema,
                name,
            )
            .map_err(|error| {
                let error = error.to_mysql_error();
                coded_refusal(error.code, error.message)
            })?;
            let altered = stored_sequence_info(options, old.comment.clone());
            // Go `onAlterSequence`'s early return.
            if altered == old && restart.is_none() {
                return Ok(already(format!(
                    "sequence `{schema}`.`{name}` already has those options"
                )));
            }
            let mut info = stored.clone_like_go();
            let increment = altered.increment;
            info.sequence = Some(GoShared::new(altered));
            if let Some(value) = restart {
                // Go `restartSequenceValue`.
                writes.push(BufferMutation::set(
                    sequence_value_kv_key(db_id, info.id),
                    value::encode_int_value(sequence_base_for(increment, value)),
                )?);
            }
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_ALTER_SEQUENCE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::DropSequence { schema, name } => {
            let found = find_database(&catalog, schema).and_then(|database| {
                find_table(database, name).map(|table| (database.info.id, table))
            });
            let Some((db_id, stored)) = found else {
                return Err(coded_refusal(
                    tidb_error::tidb::errcode::ErrUnknownSequence,
                    format!("Unknown SEQUENCE: '{schema}.{name}'"),
                ));
            };
            if !stored.is_sequence() {
                return Err(coded_refusal(
                    tidb_error::tidb::errcode::ErrWrongObject,
                    format!("'{schema}.{name}' is not SEQUENCE"),
                ));
            }
            let table_id = stored.id;
            // Go `Mutator.DropSequence`: the table, its auto-id fields
            // (deleted only if this snapshot observed them, as `HDel` does),
            // and the sequence counter.
            writes.push(BufferMutation::delete(key::table_kv_key(db_id, table_id))?);
            for field in [
                key::auto_table_id_kv_key(db_id, table_id),
                key::auto_increment_id_kv_key(db_id, table_id),
                key::auto_random_table_id_kv_key(db_id, table_id),
                sequence_value_kv_key(db_id, table_id),
            ] {
                if snapshot.get(&field)?.is_some() {
                    writes.push(BufferMutation::delete(field)?);
                }
            }
            diff.action_type = ActionType::ACTION_DROP_SEQUENCE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::RenameTable {
            is_alter,
            from_schema,
            from_table,
            to_schema,
            to_table,
        } => {
            if *is_alter
                && from_schema.eq_ignore_ascii_case(&to_schema)
                && from_table.eq_ignore_ascii_case(&to_table)
            {
                // Go resolves the source before the ALTER identity early return.
                let exists = find_database(&catalog, from_schema)
                    .is_some_and(|database| find_table(database, from_table).is_some());
                if !exists {
                    return Err(DdlPlanError::TableNotExists {
                        schema: from_schema.clone(),
                        table: from_table.clone(),
                    });
                }
                return Ok(already_with_warnings(
                    format!("table `{from_schema}`.`{from_table}` already has its requested name"),
                    Vec::new(),
                ));
            } else {
                let pair = RenameTablePair {
                    from_schema: from_schema.clone(),
                    from_table: from_table.clone(),
                    to_schema: to_schema.clone(),
                    to_table: to_table.clone(),
                };
                plan_rename_tables(
                    &catalog,
                    std::slice::from_ref(&pair),
                    *is_alter,
                    start_ts,
                    &mut writes,
                    &mut diff,
                )?;
            }
        }
        DdlStatement::RenameTables { pairs } => {
            plan_rename_tables(&catalog, pairs, false, start_ts, &mut writes, &mut diff)?;
        }
        DdlStatement::CreateIndex {
            schema,
            table,
            if_not_exists,
            index,
            expressions,
            condition,
            context,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            // go's ADD PRIMARY KEY refuses a SECOND primary as 1068 BEFORE
            // the name-duplicate check -- the existing `PRIMARY` index makes
            // the name check answer 1061 instead (oracle g-alter: a second
            // ADD PRIMARY KEY answers `Multiple primary key defined`).
            // Go `CreatePrimaryKey`: an integer handle is a primary key too.
            if index.primary && (find_index(stored, "PRIMARY").is_some() || stored.pk_is_handle) {
                return Err(DdlPlanError::Admission(
                    crate::table_info_build::DdlAdmissionError::with_code(
                        1068,
                        "Multiple primary key defined",
                    ),
                ));
            }
            // Go `CreatePrimaryKey`: a primary key needs stored generated
            // columns, and an expression part is a virtual one.
            if index.primary && expressions.iter().any(Option::is_some) {
                return Err(coded_refusal(
                    tidb_error::mysql::errcode::ErrFunctionalIndexPrimaryKey,
                    "The primary key cannot be an expression index",
                ));
            }
            let name = settle_added_index_name(stored, index, expressions);
            if let Some(existing) = find_index(stored, &name) {
                let existing = existing.read();
                if *if_not_exists {
                    return Ok(already(format!(
                        "index `{}` already exists on `{schema}`.`{table}`",
                        existing.name.original()
                    )));
                }
                return Err(DdlPlanError::DuplicateKeyName(name));
            }
            let mut info = stored.clone_like_go();
            let mut added = plan_added_index(
                &mut info,
                index,
                expressions,
                condition.as_ref(),
                &name,
                &context.0,
            )?;
            warn_missing_region_split_policy(stored, &name, &context.0);
            check_partial_index_fast_reorg(schema, &added)?;
            // The backfill decodes the EXISTING rows: the table with any hidden
            // column the index adds, without the index itself.
            let backfill_table = Box::new(info.clone_like_go());
            allocate_added_index(&mut info, &mut added);
            let added = GoShared::new(added);
            info.indices.push_handle_go(Some(added.clone()));
            check_table_info_valid(&info)?;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            backfill.push(IndexBackfill {
                table: backfill_table,
                index: added.clone(),
                use_new_collation,
                operation: IndexBackfillOperation::Add(context.clone()),
            });
            diff.action_type = ActionType::ACTION_ADD_INDEX;
            diff.schema_id = db_id;
            diff.table_id = table_id;
            if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
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
        DdlStatement::AddForeignKey {
            schema,
            table,
            definition,
            context,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            // Go `AlterTable` names an anonymous key before any spec runs.
            let name = match definition.name.as_deref() {
                Some(name) if !name.is_empty() => name.to_owned(),
                _ => format!("fk_{}", stored.max_foreign_key_id + 1),
            };
            let created = create_foreign_key(&catalog, schema, stored, &name, definition, context)?;
            let mut info = stored.clone_like_go();
            foreign_key_checks.extend(on_create_foreign_key(
                &catalog,
                schema,
                &mut info,
                &created,
                use_new_collation,
                &mut backfill,
            )?);
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_ADD_FOREIGN_KEY;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::AlterTableCache {
            schema,
            table,
            cache,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let wanted = if *cache {
                tidb_model::TableCacheStatusType::ENABLE
            } else {
                tidb_model::TableCacheStatusType::DISABLE
            };
            // Go returns without a job when the state already holds.
            if stored.table_cache_status_type == wanted {
                return Ok(already(format!(
                    "`{schema}`.`{table}` is already {}",
                    if *cache { "cached" } else { "not cached" }
                )));
            }
            if *cache {
                // Go `AlterTableCache`.
                if tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
                    return Err(coded_refusal(
                        tidb_error::tidb::errcode::ErrUnsupportedDDLOperation,
                        "ALTER table cache for tables in system database is currently unsupported",
                    ));
                }
                if stored.temp_table_type != tidb_model::TempTableType::NONE {
                    return Err(coded_refusal(
                        tidb_error::tidb::errcode::ErrOptOnTemporaryTable,
                        "`alter temporary table cache` is unsupported on temporary tables.",
                    ));
                }
                if stored.partition.is_some() {
                    return Err(coded_refusal(
                        tidb_error::tidb::errcode::ErrOptOnCacheTable,
                        "'partition mode' is unsupported on cache tables.",
                    ));
                }
                // Go `checkCacheTableSize`, over the table's stored bytes.
                cache_table_check = Some(Box::new(stored.clone_like_go()));
                // Go `AlterTableCache`: `replace into mysql.table_cache_meta
                // values (%?, 'NONE', 0, 0)` before the job.
                write_table_cache_meta_row(&catalog, stored.id, &mut writes)?;
            }
            // Go moves through `TableCacheStatusSwitching`, one version each
            // way; the transaction publishes the final state.
            let mut info = stored.clone_like_go();
            info.table_cache_status_type = wanted;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = if *cache {
                ActionType::ACTION_ALTER_CACHE_TABLE
            } else {
                ActionType::ACTION_ALTER_NO_CACHE_TABLE
            };
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::DropForeignKey {
            schema,
            table,
            name,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let mut info = stored.clone_like_go();
            drop_foreign_key(&mut info, name)?;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_DROP_FOREIGN_KEY;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::AddForeignKeyRollback {
            schema,
            table,
            max_column_id,
            max_index_id,
            max_foreign_key_id,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let mut info = stored.clone_like_go();
            info.max_column_id = info.max_column_id.max(*max_column_id);
            info.max_index_id = info.max_index_id.max(*max_index_id);
            info.max_foreign_key_id = info.max_foreign_key_id.max(*max_foreign_key_id);
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            // The rolled-back job keeps its own type.
            diff.action_type = ActionType::ACTION_ADD_FOREIGN_KEY;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::ModifyEngineAttribute {
            schema,
            table,
            engine_attribute,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let admission = |error: tidb_executor::DriverError| {
                let error = error.to_mysql_error();
                DdlPlanError::Admission(DdlAdmissionError::with_code(error.code, error.message))
            };
            let mut info = stored.clone_like_go();
            // Go `onModifyTableEngineAttribute` keeps the written string for
            // SHOW CREATE TABLE, then `onAlterTableStorageClassSettings`
            // re-resolves the table's and every partition's class when the
            // attribute names one.
            info.engine_attribute = engine_attribute.clone();
            if let Some(settings) =
                tidb_executor::ddl::storage_class::storage_class_settings_of(engine_attribute)
                    .map_err(admission)?
            {
                let (tier, transitions) =
                    tidb_executor::ddl::storage_class::build_storage_class_for_table(&settings);
                info.storage_class_tier = tier;
                info.storage_class_transitions = transitions.into();
                let has_definitions = info
                    .partition
                    .as_ref()
                    .is_some_and(|partition| !partition.read().definitions.is_empty());
                if has_definitions {
                    let spec = stored_partition_spec(stored)?;
                    let definitions = spec.definitions.iter().collect::<Vec<_>>();
                    let classes =
                        tidb_executor::ddl::storage_class::build_storage_class_for_partitions(
                            &settings,
                            &spec.kind,
                            &definitions,
                            &tidb_executor::StmtContext::for_query(),
                        )
                        .map_err(admission)?;
                    let partition = info.partition.as_ref().expect("checked above").read();
                    for (ordinal, (tier, transitions)) in classes.into_iter().enumerate() {
                        partition.definitions.update(ordinal, |definition| {
                            definition.storage_class_tier = tier;
                            definition.storage_class_transitions = transitions.into();
                        });
                    }
                }
            }
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_MODIFY_ENGINE_ATTRIBUTE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::LockTables {
            lock_tables,
            unlock_tables,
            session,
        } => {
            let mut changed = Vec::new();
            // `onLockTables`: the session's earlier locks are released first.
            plan_unlock_tables(&catalog, unlock_tables, session, false, &mut changed);
            // Checked once more at the job's first step.
            for lock in lock_tables {
                let info = lock_table_target(&catalog, &mut changed, lock)?;
                let name = info.name.lowercase().to_owned();
                let current = info.lock.as_ref().map(|lock| lock.read().clone());
                tidb_executor::ddl::table_lock::check_table_locked(
                    &name,
                    current.as_ref(),
                    lock.tp,
                    session,
                )
                .map_err(lock_plan_error)?;
            }
            for lock in lock_tables {
                let info = lock_table_target(&catalog, &mut changed, lock)?;
                let name = info.name.lowercase().to_owned();
                let current = info.lock.as_ref().map(|lock| lock.read().clone());
                let next = tidb_executor::ddl::table_lock::lock_table(
                    &name,
                    current.as_ref(),
                    lock.tp,
                    session,
                    start_ts,
                )
                .map_err(lock_plan_error)?;
                info.lock = Some(tidb_model::GoShared::new(next));
            }
            let first = lock_tables.first().ok_or_else(|| {
                DdlPlanError::Admission(DdlAdmissionError::with_code(
                    GENERIC_ERROR_CODE,
                    "LOCK TABLES names no table".to_owned(),
                ))
            })?;
            write_lock_table_infos(
                &mut writes,
                &mut diff,
                changed,
                start_ts,
                ActionType::ACTION_LOCK_TABLE,
                (first.schema_id, first.table_id),
            )?;
        }
        DdlStatement::UnlockTables {
            unlock_tables,
            session,
            is_cleanup,
        } => {
            let mut changed = Vec::new();
            plan_unlock_tables(&catalog, unlock_tables, session, *is_cleanup, &mut changed);
            // Go `unlockTable` answers "no update" for every table: the job
            // finishes without a schema version.
            let Some(first) = changed.first().map(|(db_id, info)| (*db_id, info.id)) else {
                return Ok(already("UNLOCK TABLES held no lock to release".to_owned()));
            };
            write_lock_table_infos(
                &mut writes,
                &mut diff,
                changed,
                start_ts,
                ActionType::ACTION_UNLOCK_TABLE,
                first,
            )?;
        }
        DdlStatement::AlterTtl {
            schema,
            table,
            ttl_info,
            enable,
            job_interval,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let stored_ttl = stored.ttl_info.as_ref().map(|ttl| ttl.read().clone());
            let ttl = match ttl_info {
                // Go `AlterTableTTLInfoOrEnable` validates the new definition
                // on a copy of the table, then `onTTLInfoChange` keeps the
                // stored enable flag and interval the statement left unset.
                Some(ttl) => {
                    let mut candidate = stored.clone_like_go();
                    candidate.ttl_info = Some(GoShared::new((**ttl).clone()));
                    let referred = !crate::foreign_key_build::get_table_referred_foreign_keys(
                        &catalog, schema, table,
                    )
                    .is_empty();
                    crate::table_info_build::check_ttl_info_valid(&candidate, referred).map_err(
                        |error| {
                            let error = error.to_mysql_error();
                            coded_refusal(error.code, error.message)
                        },
                    )?;
                    let mut ttl = (**ttl).clone();
                    if let Some(old) = &stored_ttl {
                        if enable.is_none() {
                            ttl.enable = old.enable;
                        }
                        if job_interval.is_none() {
                            ttl.job_interval = old.job_interval.clone();
                        }
                    }
                    ttl
                }
                None => {
                    let Some(mut ttl) = stored_ttl else {
                        let option = if enable.is_some() {
                            "TTL_ENABLE"
                        } else {
                            "TTL_JOB_INTERVAL"
                        };
                        let error = tidb_executor::DriverError::SetTtlOptionForNonTtlTable(
                            option.to_owned(),
                        )
                        .to_mysql_error();
                        return Err(coded_refusal(error.code, error.message));
                    };
                    if let Some(enable) = enable {
                        ttl.enable = *enable;
                    }
                    if let Some(interval) = job_interval {
                        ttl.job_interval = interval.clone();
                    }
                    ttl
                }
            };
            let mut info = stored.clone_like_go();
            info.ttl_info = Some(GoShared::new(ttl));
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_ALTER_TTLINFO;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::RemoveTtl { schema, table } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            // Go `AlterTableRemoveTTL` submits no job for a table without one.
            if stored.ttl_info.is_none() {
                return Ok(already(format!("`{schema}`.`{table}` has no TTL")));
            }
            let mut info = stored.clone_like_go();
            info.ttl_info = None;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_ALTER_TTLREMOVE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::AlterTablePlacement {
            schema,
            table,
            policy,
        } => {
            // Go `AlterTablePlacement`, then `onAlterTablePlacement`.
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            if stored.temp_table_type != tidb_model::TempTableType::NONE {
                return Err(coded_refusal(
                    8006,
                    "`placement` is unsupported on temporary tables.".to_owned(),
                ));
            }
            let reference = check_and_normalize_placement_policy(snapshot, policy)?;
            let old_enables_placement = stored.placement_policy_ref.is_some();
            let mut info = stored.clone_like_go();
            info.placement_policy_ref = reference;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            // `placement.NewTableBundle`, or an empty bundle when the table
            // had placement before and has none now.
            let policies = load_policies(snapshot)?;
            let bundle = tidb_placement::new_table_bundle(&policies, &info).map_err(|error| {
                DdlPlanError::Admission(DdlAdmissionError::with_code(
                    GENERIC_ERROR_CODE,
                    format!("building placement rules: {error}"),
                ))
            })?;
            match bundle {
                Some(bundle) => placement_bundles.push(bundle),
                None if old_enables_placement => {
                    placement_bundles.push(tidb_placement::new_bundle(table_id))
                }
                None => {}
            }
            diff.action_type = ActionType::ACTION_ALTER_TABLE_PLACEMENT;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::ShardRowId {
            schema,
            table,
            bits,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            // Go `ShardRowID`, in its order.
            if stored.temp_table_type != tidb_model::TempTableType::NONE {
                return Err(coded_refusal(
                    8006,
                    "`shard_row_id_bits` is unsupported on temporary tables.".to_owned(),
                ));
            }
            if *bits == stored.shard_row_id_bits {
                return Ok(already(format!(
                    "`{schema}`.`{table}` already has SHARD_ROW_ID_BITS = {bits}"
                )));
            }
            if *bits > 0 && (stored.pk_is_handle || stored.is_common_handle) {
                return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                    8200,
                    "Unsupported shard_row_id_bits for table with primary key as row id",
                )));
            }
            let mut info = stored.clone_like_go();
            // Go `onShardRowID`: lowering the count only narrows the shard;
            // raising it first checks the next row id still fits, and moves
            // `MaxShardRowIDBits` with it.
            if *bits < info.shard_row_id_bits {
                info.shard_row_id_bits = *bits;
            } else {
                verify_no_overflow_shard_bits(snapshot, db_id, &info, *bits)?;
                info.shard_row_id_bits = *bits;
                info.max_shard_row_id_bits = *bits;
            }
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_SHARD_ROW_ID;
            diff.schema_id = db_id;
            diff.table_id = table_id;
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
            writes.push(BufferMutation::set(
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
            let mut info = stored.clone_like_go();
            if let AlterColumnOutcome::AlreadySatisfied(detail) =
                apply_index_visibility(&mut info, schema, table, index, *invisible)?
            {
                return Ok(already(detail));
            }
            // Go `updateVersionAndTableInfoWithCheck`.
            check_table_info_valid(&info)?;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
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
            crate::foreign_key_build::check_index_needed_in_foreign_key(
                &catalog, schema, stored, &dropped,
            )
            .map_err(DdlPlanError::Admission)?;
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
            check_table_info_valid(&info)?;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
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
                operation: IndexBackfillOperation::Drop,
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
            check_table_charset_named(charset, collate)?;
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let mut info = stored.clone_like_go();
            if let AlterColumnOutcome::AlreadySatisfied(detail) = apply_table_charset_and_collate(
                &catalog,
                schema,
                table,
                &mut info,
                charset,
                collate,
                *overwrite_columns,
            )? {
                return Ok(already(detail));
            }
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
            diff.action_type = ActionType::ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE;
            diff.schema_id = db_id;
            diff.table_id = table_id;
        }
        DdlStatement::ModifySchemaDefaultPlacement {
            name,
            placement_policy,
        } => {
            let Some(database) = find_database(&catalog, name) else {
                return Err(DdlPlanError::UnknownDatabase(name.clone()));
            };
            let reference = check_and_normalize_placement_policy(snapshot, placement_policy)?;
            // Go `onModifySchemaDefaultPlacement`: the same reference
            // finishes the job without touching the database.
            let current = database
                .info
                .placement_policy_ref
                .as_ref()
                .map(|reference| reference.read().clone());
            let wanted = reference.as_ref().map(|reference| reference.read().clone());
            if wanted.is_some() && current == wanted {
                return Ok(already(format!(
                    "database `{name}` already has that placement policy"
                )));
            }
            let mut info = database.info.clone();
            info.placement_policy_ref = reference;
            let db_id = info.id;
            let encoded = value::serialize_db_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(key::database_kv_key(db_id), encoded)?);
            diff.action_type = ActionType::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT;
            diff.schema_id = db_id;
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
            writes.push(BufferMutation::set(key::database_kv_key(db_id), encoded)?);
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
            let mut info = stored.clone_like_go();
            if let AlterColumnOutcome::AlreadySatisfied(detail) =
                apply_rename_index(&mut info, schema, table, from, to)?
            {
                return Ok(already(detail));
            }
            check_table_info_valid(&info)?;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
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
            apply_set_column_default(
                &mut info,
                table,
                column,
                default_value.as_deref(),
                &context.0,
            )?;
            check_table_info_valid(&info)?;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
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
            let warnings = has_primary_key
                .then(|| {
                    format!(
                        "ORDER BY ignored as there is a user-defined clustered index in the table '{table}'"
                    )
                })
                .map(|message| vec![(DdlWarningLevel::Warning, 1105, message)])
                .unwrap_or_default();
            return Ok(DdlPlan::AlreadySatisfied {
                detail: format!("ORDER BY on `{schema}`.`{table}` changes nothing"),
                warnings,
            });
        }
        DdlStatement::RebaseAutoIncrementId {
            schema,
            table,
            new_base,
            force,
        } => {
            let (db_id, stored) = locate_table(&catalog, schema, table)?;
            let mut info = stored.clone_like_go();
            rebase_auto_increment_id(
                snapshot,
                auto_ids,
                db_id,
                &mut info,
                *new_base,
                *force,
                &mut None,
                &mut writes,
                &mut warnings,
            )?;
            info.update_ts = start_ts;
            let table_id = info.id;
            let encoded = value::serialize_table_info(&info)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            writes.push(BufferMutation::set(
                key::table_kv_key(db_id, table_id),
                encoded,
            )?);
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

    // Go `finishDDLJob` writes every finished job into
    // `mysql.tidb_ddl_history`, which is what `ADMIN SHOW DDL JOBS` reads
    // (`fetchShowDDLJobs` over that table). The direct path has no persisted
    // job, so the history rows are synthesized here from the same
    // schema-change events the notifier announces: one row per event, ids
    // contiguous from the job ID, each already SYNCED with its schema
    // PUBLIC.
    if !schema_change_events.is_empty() && diff.schema_id != 0 {
        let history = crate::ddl_history_table::DdlHistoryTable::locate(&catalog)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
        for (offset, (_, event)) in schema_change_events.iter().enumerate() {
            let (schema_name_override, table_name) = match event.action_type() {
                ActionType::ACTION_CREATE_TABLE => (
                    None,
                    Some(event.create_table_info().name.original().to_owned()),
                ),
                ActionType::ACTION_ADD_INDEX => (
                    None,
                    Some(event.add_index_info().0.name.original().to_owned()),
                ),
                ActionType::ACTION_DROP_TABLE => (
                    None,
                    Some(event.drop_table_info().name.original().to_owned()),
                ),
                ActionType::ACTION_TRUNCATE_TABLE => (
                    None,
                    Some(event.truncate_table_info().0.name.original().to_owned()),
                ),
                ActionType::ACTION_MODIFY_COLUMN => (
                    None,
                    Some(event.modify_column_info().0.name.original().to_owned()),
                ),
                ActionType::ACTION_ADD_TABLE_PARTITION => (
                    None,
                    Some(event.add_partition_info().0.name.original().to_owned()),
                ),
                ActionType::ACTION_DROP_TABLE_PARTITION => (
                    None,
                    Some(event.drop_partition_info().0.name.original().to_owned()),
                ),
                ActionType::ACTION_TRUNCATE_TABLE_PARTITION => (
                    None,
                    Some(event.truncate_partition_info().0.name.original().to_owned()),
                ),
                ActionType::ACTION_REORGANIZE_PARTITION => (
                    None,
                    Some(event.reorganize_partition_info().0.name.original().to_owned()),
                ),
                ActionType::ACTION_EXCHANGE_TABLE_PARTITION => (
                    None,
                    Some(event.exchange_partition_info().0.name.original().to_owned()),
                ),
                // Schema-only rows carry an EMPTY table name and their own
                // db name; go's drop-schema history row reads schema_state
                // 'none' (the schema is gone, not public).
                ActionType::ACTION_CREATE_SCHEMA => (
                    Some(event.create_schema_info().name.original().to_owned()),
                    Some(String::new()),
                ),
                ActionType::ACTION_DROP_SCHEMA => (
                    Some(event.drop_schema_info().name.original().to_owned()),
                    Some(String::new()),
                ),
                _ => (None, None),
            };
            let Some(table_name) = table_name else {
                continue;
            };
            let mut job = tidb_model::Job::default();
            job.id = ddl_job_id + offset as i64;
            job.type_ = event.action_type();
            job.state = JobState::SYNCED;
            job.schema_state = if event.action_type() == ActionType::ACTION_DROP_SCHEMA {
                SchemaState::NONE
            } else {
                SchemaState::PUBLIC
            };
            job.schema_id = diff.schema_id;
            job.table_id = diff.table_id;
            job.start_ts = start_ts;
            job.schema_name = schema_name_override
                .unwrap_or_else(|| {
                    catalog
                        .databases
                        .iter()
                        .find(|database| database.info.id == diff.schema_id)
                        .map(|database| database.info.name.original().to_owned())
                        .unwrap_or_default()
                })
                .into();
            job.table_name = table_name.into();
            let encoded = job
                .encode(false)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
            history
                .append_insert_ignore(snapshot, &job, &encoded, &mut writes)
                .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
        }
    }

    // Go's submitter admits the job before it runs.
    let job_schema = catalog
        .databases
        .iter()
        .find(|database| database.info.id == diff.schema_id)
        .map(|database| database.info.name.original().to_owned())
        .or_else(|| match statement {
            DdlStatement::CreateDatabase { name, .. }
            | DdlStatement::ModifySchemaDefaultPlacement { name, .. } => Some(name.clone()),
            _ => None,
        })
        .unwrap_or_default();
    crate::cluster_bdr::admit_submit(&bdr_role, cdc_write_source, &job_schema, statement, &diff)?;

    // The version bump comes last so the write set always ends with the two
    // keys that make the change observable — and the version key is what a
    // concurrent DDL collides with.
    writes.push(BufferMutation::set(
        key::schema_version_kv_key(),
        value::encode_int_value(schema_version),
    )?);
    let encoded_diff = value::serialize_schema_diff(&diff)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    writes.push(BufferMutation::set(
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
        column_reorgs,
        foreign_key_checks,
        cache_table_check,
        partition_reorg,
        mdl_info_update,
        exchange_partition_label_swap,
        warnings,
        placement_bundles,
        placement_rollback_bundles,
    })))
}

/// Go `IsModifyColumnDenied` over a MODIFY / CHANGE COLUMN this plan built:
/// the stored column's type against the built one's.
fn admit_modified_column_bdr(
    bdr_role: &[u8],
    schema: &str,
    stored: &TableInfo,
    built: &TableInfo,
    column: &tidb_ast::ColumnDef,
    rename_from: Option<&str>,
) -> Result<(), DdlPlanError> {
    let field_type_of = |info: &TableInfo, name: &str| {
        tidb_model::column::find_column_info(&info.columns, name)
            .map(|column| column.read().field_type.clone())
    };
    let (Some(old), Some(new)) = (
        field_type_of(stored, rename_from.unwrap_or(&column.name)),
        field_type_of(built, &column.name),
    ) else {
        return Ok(());
    };
    crate::cluster_bdr::admit_modify_column(bdr_role, schema, &new, &old, &column.options)
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
    column_name: &str,
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
            // go `errors.Trace(ErrUnsupportedModifyCollation.GenWithStackByArgs(
            // column, fromCollate, toCollate))`: the message names the
            // COLUMN, then from/to, and the index qualifier.
            format!(
                "Unsupported converting collation of column '{column_name}' from '{from_collate}' to '{to_collate}' when index is defined on it."
            ),
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
    let wanted = name.go_to_lower();
    table.indices.iter_deref().any(|index| {
        index
            .read()
            .columns
            .iter_deref()
            .any(|column| column.read().name.lowercase() == wanted)
    })
}

/// A lock job's refusal as the planner reports it.
fn lock_plan_error(error: tidb_executor::DriverError) -> DdlPlanError {
    let error = error.to_mysql_error();
    DdlPlanError::Admission(DdlAdmissionError::with_code(error.code, error.message))
}

/// The stored table a lock job edits, by Go's `(SchemaID, TableID)`: the
/// copy this job already changed, or a fresh one (Go
/// `GetTableInfoAndCancelFaultJob`).
fn lock_table_target<'a>(
    catalog: &ClusterCatalog,
    changed: &'a mut Vec<(i64, TableInfo)>,
    lock: &tidb_model::TableLockTpInfo,
) -> Result<&'a mut TableInfo, DdlPlanError> {
    if let Some(index) = changed
        .iter()
        .position(|(db_id, info)| *db_id == lock.schema_id && info.id == lock.table_id)
    {
        return Ok(&mut changed[index].1);
    }
    let stored = stored_table_by_ids(catalog, lock.schema_id, lock.table_id).ok_or_else(|| {
        DdlPlanError::Admission(DdlAdmissionError::with_code(
            1146,
            format!(
                "Table '(Schema ID {}).(Table ID {})' doesn't exist",
                lock.schema_id, lock.table_id
            ),
        ))
    })?;
    changed.push((lock.schema_id, stored.clone_like_go()));
    Ok(&mut changed.last_mut().expect("just pushed").1)
}

fn stored_table_by_ids(
    catalog: &ClusterCatalog,
    schema_id: i64,
    table_id: i64,
) -> Option<&TableInfo> {
    catalog
        .databases
        .iter()
        .find(|database| database.info.id == schema_id)?
        .tables
        .iter()
        .find(|table| table.id == table_id)
}

/// Go `ddl.unlockTables`: each table the session holds lets go of it, a
/// table dropped since it was locked skipped.
fn plan_unlock_tables(
    catalog: &ClusterCatalog,
    unlock_tables: &[tidb_model::TableLockTpInfo],
    session: &tidb_model::SessionInfo,
    is_cleanup: bool,
    changed: &mut Vec<(i64, TableInfo)>,
) {
    for unlock in unlock_tables {
        let Some(stored) = stored_table_by_ids(catalog, unlock.schema_id, unlock.table_id) else {
            continue;
        };
        let current = stored.lock.as_ref().map(|lock| lock.read().clone());
        let Some(next) =
            tidb_executor::ddl::table_lock::unlock_table(current.as_ref(), session, is_cleanup)
        else {
            continue;
        };
        let mut info = stored.clone_like_go();
        info.lock = next.map(tidb_model::GoShared::new);
        changed.push((unlock.schema_id, info));
    }
}

/// Writes every table a lock job changed (Go's `updateVersionAndTableInfo`
/// per table), and one diff for the job naming `first` and listing the
/// others as affected options, which `applyDefaultAction` reloads.
fn write_lock_table_infos(
    writes: &mut Vec<BufferMutation>,
    diff: &mut SchemaDiff,
    changed: Vec<(i64, TableInfo)>,
    start_ts: u64,
    action: ActionType,
    first: (i64, i64),
) -> Result<(), DdlPlanError> {
    diff.action_type = action;
    diff.schema_id = first.0;
    diff.table_id = first.1;
    for (db_id, mut info) in changed {
        info.update_ts = start_ts;
        let encoded = value::serialize_table_info(&info)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
        writes.push(BufferMutation::set(
            key::table_kv_key(db_id, info.id),
            encoded,
        )?);
        if (db_id, info.id) != first {
            diff.affected_options.push_go(tidb_model::AffectedOption {
                schema_id: db_id,
                table_id: info.id,
                old_table_id: info.id,
                old_schema_id: db_id,
            });
        }
    }
    Ok(())
}

fn already(detail: String) -> DdlPlan {
    DdlPlan::AlreadySatisfied {
        detail,
        warnings: Vec::new(),
    }
}

/// go's note for one missing table: `Note | 1051 | Unknown table 'd.t'`.
fn missing_table_note(
    schema: &str,
    table: &str,
) -> (crate::real_tikv_ddl::DdlWarningLevel, u16, String) {
    (
        crate::real_tikv_ddl::DdlWarningLevel::Note,
        1051,
        format!("Unknown table '{schema}.{table}'"),
    )
}

/// `already` carrying go's per-table 1051 notes (DROP ... IF EXISTS).
fn already_with_warnings(detail: String, warnings: Vec<(DdlWarningLevel, u16, String)>) -> DdlPlan {
    DdlPlan::AlreadySatisfied { detail, warnings }
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
    is_alter: bool,
    start_ts: u64,
    writes: &mut Vec<BufferMutation>,
    diff: &mut SchemaDiff,
) -> Result<(), DdlPlanError> {
    let database_ids = catalog
        .databases
        .iter()
        .map(|database| (database.info.name.lowercase().to_owned(), database.info.id))
        .collect::<BTreeMap<_, _>>();
    let mut namespace = BTreeMap::new();
    let mut table_locations = BTreeMap::new();
    let mut referred = BTreeMap::<String, Vec<(i64, String)>>::new();
    for database in &catalog.databases {
        for table in &database.tables {
            table_locations.insert(
                table.id,
                table_name_key(database.info.name.lowercase(), table.name.lowercase()),
            );
            for fk in table.foreign_keys.iter_deref() {
                let fk = fk.read();
                if fk.version < tidb_model::table::FK_VERSION1 {
                    continue;
                }
                referred
                    .entry(table_name_key(
                        fk.ref_schema.lowercase(),
                        fk.ref_table.lowercase(),
                    ))
                    .or_default()
                    .push((table.id, fk.name.lowercase().to_owned()));
            }
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

    let mut changed = std::collections::BTreeSet::new();
    let mut results = Vec::with_capacity(pairs.len());
    for pair in pairs {
        let from_schema = pair.from_schema.go_to_lower();
        let to_schema = pair.to_schema.go_to_lower();
        let from_key = table_name_key(&from_schema, &pair.from_table.go_to_lower());
        let to_key = table_name_key(&to_schema, &pair.to_table.go_to_lower());
        let original = catalog.find_table(&from_schema, &pair.from_table);
        tidb_executor::ddl::check_rename(tidb_executor::ddl::RenameAdmission {
            source: (&pair.from_schema, &pair.from_table),
            target: (&pair.to_schema, &pair.to_table),
            source_exists: namespace.contains_key(&from_key),
            target_schema_exists: database_ids.contains_key(&to_schema),
            target_exists: namespace.contains_key(&to_key),
            source_is_view: original.is_some_and(|(_, table)| table.view.is_some()),
            is_alter,
        })
        .map_err(|error| match error {
            tidb_executor::DriverError::Schema(tidb_executor::SchemaErrorKind::UnknownTable(_)) => {
                DdlPlanError::TableNotExists {
                    schema: pair.from_schema.clone(),
                    table: pair.from_table.clone(),
                }
            }
            tidb_executor::DriverError::Schema(tidb_executor::SchemaErrorKind::TableExists(_)) => {
                DdlPlanError::TableExists {
                    schema: pair.to_schema.clone(),
                    table: pair.to_table.clone(),
                }
            }
            other => {
                let error = other.to_mysql_error();
                DdlPlanError::Admission(DdlAdmissionError::with_code(error.code, error.message))
            }
        })?;
        let new_schema_id = database_ids[&to_schema];
        let state = namespace.get(&from_key).expect("admitted source");
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
        changed.insert(table_id);
        table_locations.insert(table_id, to_key.clone());
        namespace.insert(to_key, state);
        if tidb_executor::ddl::rename_changes_fk_reference(&pair.from_table, &pair.to_table) {
            // Go consults the original InfoSchema's referred-key directory,
            // then edits retained child metadata by identity in this transaction.
            if let Some(children) = referred.get(&from_key) {
                let schema_name = &catalog
                    .databases
                    .iter()
                    .find(|database| database.info.id == new_schema_id)
                    .expect("admitted schema")
                    .info
                    .name;
                for (child_id, fk_name) in children {
                    let location = table_locations
                        .get(child_id)
                        .expect("retained child identity");
                    let child = namespace
                        .get_mut(location)
                        .expect("retained child metadata");
                    for fk in child.table.foreign_keys.iter_deref() {
                        let mut fk = fk.write();
                        if fk.name.lowercase() == fk_name {
                            fk.ref_schema = schema_name.clone();
                            fk.ref_table = CiString::new(pair.to_table.clone());
                            changed.insert(*child_id);
                            break;
                        }
                    }
                }
            }
        }
    }

    for state in namespace
        .values()
        .filter(|state| changed.contains(&state.table.id))
    {
        let table_id = state.table.id;
        if state.original_schema_id != state.current_schema_id {
            writes.push(BufferMutation::delete(key::table_kv_key(
                state.original_schema_id,
                table_id,
            ))?);
        }
        let encoded = value::serialize_table_info(&state.table)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
        writes.push(BufferMutation::set(
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
    } else {
        // This direct transaction publishes only once. Go's first rename
        // phase carries the original schema so peers retire the old entry;
        // its later completion-only diff cannot replace that first publication.
        // Remaining pairs retain their written order in affected options.
        diff.action_type = ActionType::ACTION_RENAME_TABLES;
        diff.schema_id = first.new_schema_id;
        diff.table_id = first.table_id;
        diff.old_schema_id = first.old_schema_id;
        diff.affected_options = results[1..]
            .iter()
            .map(|result| AffectedOption {
                schema_id: result.new_schema_id,
                table_id: result.table_id,
                old_table_id: result.table_id,
                old_schema_id: result.old_schema_id,
            })
            .collect::<Vec<_>>()
            .into();
    }
    // Go SetSchemaDiffForMultiInfos deduplicates moved tables, then publishes
    // other loaded FK children at their current schema and unchanged ID.
    let moved: std::collections::BTreeSet<_> =
        results.iter().map(|result| result.table_id).collect();
    for state in namespace
        .values()
        .filter(|state| changed.contains(&state.table.id) && !moved.contains(&state.table.id))
    {
        diff.affected_options.push_go(AffectedOption {
            schema_id: state.current_schema_id,
            old_schema_id: state.current_schema_id,
            table_id: state.table.id,
            old_table_id: state.table.id,
        });
    }
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

/// The name an added index takes: as written, or Go `GetName4AnonymousIndex`
/// against `stored`'s indexes, after the first key column -- or
/// `expression_index` when the first part is an expression
/// (`checkIndexNameAndColumns`). Go names every sub-job of an ALTER against
/// the table as it was before the statement.
fn settle_added_index_name(
    stored: &TableInfo,
    index: &IndexInfo,
    expressions: &[Option<tidb_ast::Expr>],
) -> String {
    if !index.name.original().is_empty() {
        return index.name.original().to_owned();
    }
    let first_column = index.columns.iter_deref().next();
    let prefix = match (expressions.first(), first_column) {
        (Some(Some(_)), _) | (_, None) => "expression_index".to_owned(),
        (_, Some(column)) => column.read().name.original().to_owned(),
    };
    let existing = stored
        .indices
        .iter_deref()
        .map(|index| index.read().name.original().to_owned())
        .collect::<Vec<_>>();
    let existing = existing.iter().map(String::as_str).collect::<Vec<_>>();
    tidb_executor::ddl::get_name_for_anonymous_index(&existing, &prefix, "")
}

/// Go `CreateForeignKey` (`executor.go`) for one ADD FOREIGN KEY, as its
/// submitter checks it against the original table.
struct CreatedForeignKey {
    fk: tidb_model::table::FKInfo,
    /// The parent's database and table, `None` for a self reference.
    parent: Option<(String, String)>,
    /// No index covers the key's columns, so `CreateForeignKey` queues a
    /// `createIndex` named after the key ahead of the key's own job.
    needs_index: bool,
    context: DdlStatementContext,
}

/// Go `CreateForeignKey`: `name` is already settled, as `AlterTable` names
/// an anonymous key `fk_{MaxForeignKeyID+n}` before any spec runs.
fn create_foreign_key(
    catalog: &ClusterCatalog,
    schema: &str,
    stored: &TableInfo,
    name: &str,
    definition: &tidb_ast::ForeignKeyConstraintDefinition,
    context: &DdlStatementContext,
) -> Result<CreatedForeignKey, DdlPlanError> {
    let admission = DdlPlanError::Admission;
    // A temporary table takes no foreign key.
    if stored.temp_table_type != tidb_model::TempTableType::NONE {
        return Err(coded_refusal(
            tidb_error::tidb::errcode::ErrCannotAddForeign,
            "Cannot add foreign key constraint",
        ));
    }
    if definition.reference.match_type == tidb_ast::ForeignKeyMatch::Partial {
        return Err(DdlPlanError::Admission(DdlAdmissionError::unsupported(
            "MATCH PARTIAL is not supported yet",
        )));
    }
    // Go `checkFKDupName`.
    if stored
        .foreign_keys
        .iter_deref()
        .any(|fk| fk.read().name.lowercase() == name.go_to_lower())
    {
        return Err(coded_refusal(
            tidb_error::tidb::errcode::ErrFkDupName,
            format!("Duplicate foreign key constraint name '{name}'"),
        ));
    }
    let (ref_schema, ref_table) = match definition.reference.table.as_deref() {
        Some([ref_schema, ref_table]) => (ref_schema.clone(), ref_table.clone()),
        Some([ref_table]) => (schema.to_owned(), ref_table.clone()),
        _ => (schema.to_owned(), String::new()),
    };
    let fk = crate::foreign_key_build::build_fk_info(
        name,
        &definition.parts,
        &definition.reference,
        &ref_schema,
        stored,
    )
    .map_err(admission)?;
    // Go `checkAddForeignKeyValid`.
    crate::foreign_key_build::check_foreign_key_valid(
        catalog,
        schema,
        stored,
        &fk,
        context.0.foreign_key_checks(),
        false,
    )
    .map_err(admission)?;
    let needs_index = tidb_model::index::find_index_by_columns_for_foreign_key(
        stored,
        &stored.indices,
        &fk.cols.snapshot(),
    )
    .is_none();
    // Go `createIndex`'s duplicate-name check for the key's index.
    if needs_index && find_index(stored, fk.name.original()).is_some() {
        return Err(DdlPlanError::DuplicateKeyName(
            fk.name.original().to_owned(),
        ));
    }
    let self_reference = ref_schema.go_to_lower() == schema.go_to_lower()
        && ref_table.go_to_lower() == stored.name.lowercase();
    Ok(CreatedForeignKey {
        fk,
        parent: (!self_reference).then_some((ref_schema, ref_table)),
        needs_index,
        context: context.clone(),
    })
}

/// Go `onCreateForeignKey` (`foreign_key.go`), after the `createIndex`
/// sub-job `CreateForeignKey` queued when no index covers the key: the key
/// joins the evolving table (`allocateFKIndexID`), and under
/// `foreign_key_checks` the existing rows owe `checkForeignKeyConstrain`
/// before it is public.
fn on_create_foreign_key(
    catalog: &ClusterCatalog,
    schema: &str,
    info: &mut TableInfo,
    created: &CreatedForeignKey,
    use_new_collation: bool,
    backfill: &mut Vec<IndexBackfill>,
) -> Result<Option<ForeignKeyCheck>, DdlPlanError> {
    let mut fk = created.fk.clone();
    if created.needs_index {
        let index = IndexInfo {
            name: fk.name.clone(),
            columns: fk
                .cols
                .snapshot()
                .iter()
                .map(|name| IndexColumn {
                    name: name.clone(),
                    offset: 0,
                    length: -1,
                    ..IndexColumn::default()
                })
                .collect::<Vec<_>>()
                .into(),
            state: SchemaState::PUBLIC,
            tp: tidb_ast::IndexType::BTREE,
            ..IndexInfo::default()
        };
        let mut added = plan_added_index(
            info,
            &index,
            &[],
            None,
            fk.name.original(),
            &created.context.0,
        )?;
        let backfill_table = Box::new(info.clone_like_go());
        info.max_index_id += 1;
        added.id = info.max_index_id;
        added.table = info.name.clone();
        let added = GoShared::new(added);
        info.indices.push_handle_go(Some(added.clone()));
        backfill.push(IndexBackfill {
            table: backfill_table,
            index: added,
            use_new_collation,
            operation: IndexBackfillOperation::Add(created.context.clone()),
        });
    }
    let fk_check = created.context.0.foreign_key_checks();
    // Go `checkAddForeignKeyValidInOwner`.
    crate::foreign_key_build::check_foreign_key_valid(catalog, schema, info, &fk, fk_check, true)
        .map_err(DdlPlanError::Admission)?;
    info.max_foreign_key_id += 1;
    fk.id = info.max_foreign_key_id;
    fk.state = SchemaState::PUBLIC;
    let name = fk.name.original().to_owned();
    info.foreign_keys.push_go(fk);
    if !(fk_check && tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::SeqCst)) {
        return Ok(None);
    }
    let parent = created.parent.as_ref().and_then(|(ref_schema, ref_table)| {
        catalog
            .find_table(ref_schema, ref_table)
            .map(|(_, parent)| (ref_schema.clone(), Box::new(parent.clone_like_go())))
    });
    Ok(Some(ForeignKeyCheck {
        schema: schema.to_owned(),
        child: Box::new(info.clone_like_go()),
        parent,
        name,
        use_new_collation,
        context: created.context.clone(),
    }))
}

/// Go `createIndex`: the job's `ReorgMeta.IsFastReorg` is
/// `vardef.EnableFastReorg`, off for a job on a system database
/// (`initJobReorgMetaFromVariables`), and a partial index needs it.
fn check_partial_index_fast_reorg(schema: &str, index: &IndexInfo) -> Result<(), DdlPlanError> {
    let is_fast_reorg = tidb_vardef::ENABLE_FAST_REORG.load(std::sync::atomic::Ordering::SeqCst)
        && !tidb_metadef::is_system_related_db(&schema.go_to_lower());
    if index.has_condition() && !is_fast_reorg {
        return Err(coded_refusal(
            tidb_error::tidb::errcode::ErrUnsupportedDDLOperation,
            "Unsupported add partial index: add partial index without fast reorg is not supported",
        ));
    }
    Ok(())
}

/// The rest of Go `createIndex` for one added index named `name`, planned
/// against `info`, through the checks the in-process builder runs too:
/// `checkTooLongIndex`; `BuildHiddenColumnInfo` for each expression part, its
/// column appended to `info` with a fresh id as the job worker's
/// `createColumnInfoWithPosition` does, and `checkExpressionIndexAutoIncrement`;
/// `buildIndexColumns`, which resolves and validates every key part;
/// `checkCreateGlobalIndex`; and `validateCommentLength`. Returns the index,
/// complete except for its id.
/// Go `checkAndWarnMissingRegionSplitPolicy`: a new index on a table whose
/// indexes carry a region split policy draws a warning, read against the
/// table as the statement found it.
fn warn_missing_region_split_policy(
    stored: &TableInfo,
    new_index_name: &str,
    context: &tidb_executor::StmtContext,
) {
    if stored
        .indices
        .iter_deref()
        .any(|index| index.read().region_split_policy.is_some())
    {
        context.append_warning_parts(
            tidb_error::tidb::errcode::ErrUnknown,
            &format!(
                "It is recommended to add a region split strategy to the new index \
                 '{new_index_name}' to avoid write hotspots"
            ),
        );
    }
}

fn plan_added_index(
    info: &mut TableInfo,
    index: &IndexInfo,
    expressions: &[Option<tidb_ast::Expr>],
    condition: Option<&tidb_ast::Expr>,
    name: &str,
    context: &tidb_executor::StmtContext,
) -> Result<IndexInfo, DdlPlanError> {
    use tidb_executor::ddl::{IndexedTableColumn, ResolvedKeyPart};
    let admission = |error: tidb_executor::DriverError| {
        let error = error.to_mysql_error();
        DdlPlanError::Admission(DdlAdmissionError::with_code(error.code, error.message))
    };
    tidb_executor::ddl::check_too_long_identifier(name).map_err(admission)?;
    // Go `onCreateIndex`'s `checkTooManyIndexes`, counting the new index.
    tidb_executor::ddl::check_too_many_indexes(info.indices.len() + 1).map_err(admission)?;
    let mut added = index.clone_like_go();
    added.name = CiString::new(name.to_owned());
    if expressions.iter().any(Option::is_some) {
        let parts = added
            .columns
            .iter_deref()
            .zip(expressions)
            .map(|(column, expression)| match expression {
                Some(expr) => tidb_ast::IndexPart::Expr {
                    expr: expr.clone(),
                    desc: false,
                },
                None => {
                    let column = column.read();
                    tidb_ast::IndexPart::Column {
                        name: column.name.original().to_owned(),
                        prefix_len: (column.length != -1).then_some(column.length),
                        desc: false,
                    }
                }
            })
            .collect::<Vec<_>>();
        // Built against the VISIBLE columns, as in process: an expression can
        // never name an earlier index's hidden column.
        let visible = info
            .columns
            .iter_deref()
            .filter(|column| !column.read().hidden)
            .map(|column| {
                let column = column.read();
                (column.name.original().to_owned(), column.field_type.clone())
            })
            .collect::<Vec<_>>();
        let (names, types): (Vec<_>, Vec<_>) = visible.into_iter().unzip();
        let hidden =
            tidb_executor::expression_index::build_hidden_columns_with_like_default_escape(
                name,
                &parts,
                &names,
                &types,
                &context.session_zone(),
                context.like_default_escape(),
            )
            .map_err(admission)?;
        let auto_increment = info.columns.iter_deref().find_map(|column| {
            let column = column.read();
            column
                .field_type
                .has_flag(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT)
                .then(|| column.name.original().to_owned())
        });
        tidb_executor::ddl::check_expression_index_auto_increment(
            name,
            &hidden,
            auto_increment.as_deref(),
            context,
        )
        .map_err(admission)?;
        for (position, hidden) in hidden {
            let mut column =
                crate::table_info_build::hidden_index_column_info(hidden, info.columns.len());
            info.max_column_id += 1;
            column.id = info.max_column_id;
            let part = added
                .columns
                .get(position)
                .expect("a hidden column belongs to a key part");
            {
                let mut part = part.write();
                part.name = column.name.clone();
                part.length = -1;
            }
            info.columns.push_go(column);
        }
    }
    // Go's `IndexColumn.Offset` is a position in `TableInfo.Columns`, and the
    // loader reads it back that way, so it is resolved against the table
    // rather than trusted from the statement.
    let columns = info
        .columns
        .iter_deref()
        .map(|column| column.read().clone_like_go())
        .collect::<Vec<_>>();
    let mut offsets = Vec::with_capacity(added.columns.len());
    let mut resolved = Vec::with_capacity(added.columns.len());
    for (part, expression) in added.columns.iter_deref().zip(
        expressions
            .iter()
            .map(Option::is_some)
            .chain(std::iter::repeat(false)),
    ) {
        let part = part.read();
        let Some(offset) = columns
            .iter()
            .position(|column| column.name.lowercase() == part.name.lowercase())
        else {
            return Err(DdlPlanError::UnknownIndexColumn {
                column: part.name.original().to_owned(),
                index: name.to_owned(),
            });
        };
        offsets.push(offset);
        resolved.push(ResolvedKeyPart {
            offset,
            declared: (part.length != -1).then_some(part.length),
            expression,
        });
    }
    let indexed = columns
        .iter()
        .map(|column| IndexedTableColumn {
            name: column.name.original(),
            field_type: &column.field_type,
            virtual_generated: !column.generated_expr_string.is_empty() && !column.generated_stored,
        })
        .collect::<Vec<_>>();
    let (prefix_lengths, _) = tidb_executor::ddl::build_index_columns(
        &indexed,
        &resolved,
        added.unique,
        tidb_executor::ddl::index_prefix::MAX_INDEX_LENGTH,
    )
    .map_err(admission)?;
    for ((part, offset), length) in added
        .columns
        .iter_deref()
        .zip(&offsets)
        .zip(&prefix_lengths)
    {
        let mut part = part.write();
        part.name = columns[*offset].name.clone();
        part.offset = columns[*offset].offset;
        // The WRITE path cuts entries by this length, so it must be the
        // normalized one `checkIndexColumn` settled.
        part.length = *length;
    }
    let partition_offsets = match info.partition.as_ref() {
        Some(_) => {
            let spec = stored_partition_spec(info)?;
            let mut partition_offsets = Vec::with_capacity(spec.dependencies.len());
            for dependency in &spec.dependencies {
                let Some(offset) = columns
                    .iter()
                    .position(|column| column.name.original().eq_ignore_ascii_case(dependency))
                else {
                    return Err(DdlPlanError::Admission(DdlAdmissionError::with_code(
                        GENERIC_ERROR_CODE,
                        format!(
                            "the partition expression of `{}` reads column `{dependency}`, \
                             which the table does not define",
                            info.name.original()
                        ),
                    )));
                };
                partition_offsets.push(offset);
            }
            Some(partition_offsets)
        }
        None => None,
    };
    tidb_executor::ddl::check_create_global_index(
        name,
        added.unique,
        added.global,
        partition_offsets.as_deref(),
        &indexed,
        &offsets,
        &prefix_lengths,
        context,
    )
    .map_err(admission)?;
    added.comment = tidb_executor::ddl::validate_comment_length(
        &added.comment,
        name,
        tidb_executor::ddl::CommentOwner::Index,
        context,
    )
    .map_err(admission)?;
    crate::table_info_build::build_index_condition(info, &mut added, condition, context)
        .map_err(DdlPlanError::Admission)?;
    Ok(added)
}

/// Gives a planned index its id and table and finishes the table for it, as
/// Go's `onCreateIndex` does for a single ADD INDEX and a multi-schema
/// sub-job alike: `setGlobalIndexVersion`, and for a primary key every key
/// column stamped NOT NULL+PRI (oracle: after `ALTER TABLE pf ADD PRIMARY
/// KEY(b(2))`, SHOW CREATE prints `b` varchar(8) NOT NULL and SHOW INDEX
/// adds the `PRIMARY` row with Sub_part=2).
fn allocate_added_index(info: &mut TableInfo, added: &mut IndexInfo) {
    info.max_index_id += 1;
    added.id = info.max_index_id;
    added.table = info.name.clone();
    set_global_index_version(info, added);
    if !added.primary {
        return;
    }
    let pk_columns: Vec<String> = added
        .columns
        .iter_deref()
        .map(|column| column.read().name.lowercase().to_owned())
        .collect();
    for stored_column in info.columns.iter_deref() {
        let name = stored_column.read().name.lowercase().to_owned();
        if pk_columns.contains(&name) {
            let mut column = stored_column.write();
            let mut field_type = column.field_type.clone();
            field_type.add_flags(
                tidb_datatype::FieldTypeFlags::PRI_KEY | tidb_datatype::FieldTypeFlags::NOT_NULL,
            );
            column.field_type = field_type;
        }
    }
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
    writes: &mut Vec<BufferMutation>,
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
    let needs_row_id = !table.pk_is_handle && !table.is_common_handle;
    let mut row_id = if needs_row_id {
        snapshot
            .get(&row_id_key)?
            .map(|stored| value::parse_int_value(&stored))
            .transpose()
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?
            .unwrap_or(0)
    } else {
        0
    };
    for (sub_job_id, event) in events {
        if needs_row_id {
            row_id = row_id
                .checked_add(1)
                .ok_or(DdlPlanError::GlobalIdExhausted { wanted: i64::MAX })?;
        }
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
        // Go inserts through the table's stored handle layout. Existing clusters
        // can use a clustered primary key even when our bootstrap uses row IDs.
        let mutations = if needs_row_id {
            crate::system_row_write::insert_row(table, row_id, &values)
        } else {
            crate::system_row_write::store_clustered_row(table, None, &values)
        };
        writes.extend(mutations.map_err(|error| DdlPlanError::Encode(error.to_string()))?);
    }
    if needs_row_id {
        writes.push(BufferMutation::set(
            row_id_key,
            value::encode_int_value(row_id),
        )?);
    }
    Ok(())
}

/// Go `AlterTableCache`'s `replace into mysql.table_cache_meta values (%?,
/// 'NONE', 0, 0)`: the cached table's lock row, unlocked.
fn write_table_cache_meta_row(
    catalog: &ClusterCatalog,
    table_id: i64,
    writes: &mut Vec<BufferMutation>,
) -> Result<(), DdlPlanError> {
    let (_, table) = catalog
        .find_table("mysql", "table_cache_meta")
        .ok_or_else(|| DdlPlanError::Encode("mysql.table_cache_meta does not exist".to_owned()))?;
    let column = |name: &str| {
        table
            .cols()
            .iter_deref()
            .find(|column| column.read().name.lowercase() == name)
            .map(|column| column.read().clone_like_go())
            .ok_or_else(|| {
                DdlPlanError::Encode(format!("mysql.table_cache_meta has no column `{name}`"))
            })
    };
    let lock_type = column("lock_type")?;
    let mut values = crate::system_row_write::RowValues::new();
    values.insert(column("tid")?.id, Datum::Int(table_id));
    values.insert(
        lock_type.id,
        Datum::new_enum(
            tidb_datatype::MysqlEnum::new("NONE", 1),
            lock_type.field_type.collation(),
        ),
    );
    values.insert(column("lease")?.id, Datum::Int(0));
    values.insert(column("oldreadlease")?.id, Datum::Int(0));
    writes.extend(
        crate::system_row_write::replace_unindexed_clustered_row(table, &values)
            .map_err(|error| DdlPlanError::Encode(error.to_string()))?,
    );
    Ok(())
}

fn mdl_info_update(catalog: &ClusterCatalog, table_id: i64) -> Result<MdlInfoUpdate, DdlPlanError> {
    let (_, table) = catalog
        .find_table("mysql", "tidb_mdl_info")
        .ok_or_else(|| DdlPlanError::Encode("mysql.tidb_mdl_info does not exist".to_owned()))?;
    Ok(MdlInfoUpdate {
        table: Box::new(table.clone_like_go()),
        table_ids: table_id.to_string(),
        omit_owner_id: false,
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
    let name = name.go_to_lower();
    catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == name)
}

fn find_table<'database>(
    database: &'database crate::cluster_catalog::LoadedDatabase,
    name: &str,
) -> Option<&'database TableInfo> {
    let name = name.go_to_lower();
    database
        .tables
        .iter()
        .find(|table| table.name.lowercase() == name)
}

#[cfg(test)]
mod global_index_version_tests {
    use super::*;

    #[test]
    fn set_global_index_version_matches_go_shape_rules() {
        let previous = tidb_model::index::get_global_index_v1_supported();
        tidb_model::index::set_global_index_v1_supported(true);

        let nullable_column = tidb_model::column::ColumnInfo {
            id: 1,
            offset: 0,
            field_type: FieldType::new(FieldTypeCode::LongLong),
            state: SchemaState::PUBLIC,
            ..Default::default()
        };
        let mut not_null_type = FieldType::new(FieldTypeCode::LongLong);
        not_null_type.add_flags(FieldTypeFlags::NOT_NULL);
        let not_null_column = tidb_model::column::ColumnInfo {
            id: 1,
            offset: 0,
            field_type: not_null_type,
            state: SchemaState::PUBLIC,
            ..Default::default()
        };
        let table = |column| TableInfo {
            columns: vec![column].into(),
            ..TableInfo::default()
        };
        let index = |unique| IndexInfo {
            global: true,
            unique,
            columns: vec![IndexColumn {
                offset: 0,
                ..IndexColumn::default()
            }]
            .into(),
            ..IndexInfo::default()
        };

        // Go assigns V1 to non-unique global indexes even when their columns
        // are NOT NULL, and to unique global indexes whenever a nullable key
        // can contain multiple NULL values across partitions.
        let mut non_unique = index(false);
        set_global_index_version(&table(not_null_column.clone()), &mut non_unique);
        assert_eq!(
            non_unique.global_index_version,
            tidb_model::index::GLOBAL_INDEX_VERSION_V1
        );
        let mut unique_nullable = index(true);
        set_global_index_version(&table(nullable_column.clone()), &mut unique_nullable);
        assert_eq!(
            unique_nullable.global_index_version,
            tidb_model::index::GLOBAL_INDEX_VERSION_V1
        );

        // A unique global index over only NOT NULL columns needs no partition
        // component, and clustered tables use the legacy key format.
        let mut unique_not_null = index(true);
        set_global_index_version(&table(not_null_column.clone()), &mut unique_not_null);
        assert_eq!(
            unique_not_null.global_index_version,
            tidb_model::index::GLOBAL_INDEX_VERSION_LEGACY
        );
        let mut clustered = index(false);
        let clustered_table = TableInfo {
            columns: vec![not_null_column].into(),
            pk_is_handle: true,
            ..TableInfo::default()
        };
        set_global_index_version(&clustered_table, &mut clustered);
        assert_eq!(
            clustered.global_index_version,
            tidb_model::index::GLOBAL_INDEX_VERSION_LEGACY
        );

        tidb_model::index::set_global_index_v1_supported(false);
        let mut unsupported = index(false);
        set_global_index_version(&table(nullable_column), &mut unsupported);
        assert_eq!(
            unsupported.global_index_version,
            tidb_model::index::GLOBAL_INDEX_VERSION_LEGACY
        );
        tidb_model::index::set_global_index_v1_supported(previous);
    }
}

/// Both SQL catalog tiers use the same classic DROP object contract.
fn check_cluster_drop_object(
    schema: &str,
    name: &str,
    table: &TableInfo,
    view: bool,
) -> Result<bool, DdlPlanError> {
    use tidb_executor::ddl::{check_drop_object, DropObjectKind};
    let actual = if table.is_base_table() {
        DropObjectKind::Table
    } else if table.view.is_some() {
        DropObjectKind::View
    } else {
        DropObjectKind::Other
    };
    check_drop_object(
        schema,
        name,
        if view {
            DropObjectKind::View
        } else {
            DropObjectKind::Table
        },
        actual,
        table.table_cache_status_type != tidb_model::TableCacheStatusType::DISABLE,
    )
    .map_err(|error| {
        let error = error.to_mysql_error();
        DdlPlanError::Admission(DdlAdmissionError::with_code(error.code, error.message))
    })
}
