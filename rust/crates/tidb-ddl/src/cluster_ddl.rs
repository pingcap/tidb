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
//! happens in [`lower_ddl_with_context`], before a timestamp is spent or a byte is written.

use tidb_ddl_build::session_context::DdlSessionContext;
use tidb_ddl_build::index::set_global_index_version;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use tidb_ast::CiString;
use tidb_ast::{
    AlterTableStmt, CreateIndexStmt, CreateTableStmt, DatabaseOption, DdlStmt, DropIndexStmt,
    DropTableStmt, IndexConstraintDefinition, IndexConstraintKind, RenameTableStmt, Stmt,
};
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_ddl_notifier::SchemaChangeEvent;
use tidb_error::mysql::FormatArg;
use tidb_error::terror::TerrorError;
use tidb_hack::GoCaseCompare;
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

use tidb_cluster::cluster_catalog::{
    load_cluster_catalog, ClusterCatalog, ClusterCatalogError, MetaSnapshot,
};
use crate::real_tikv_ddl::DdlWarningLevel;
use tidb_hack::GoToLower;
use tidb_ddl_build::column_types::resolve_charset_collation;

pub use tidb_ddl_build::table_info_build::DdlAdmissionError;

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
    context: DdlContext,
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
        context: &DdlContext,
    ) -> Result<Self, DdlAdmissionError> {
        let mb_ctx = context.metabuild_context();
        let mut template = tidb_ddl_build::table_info_build::build_table_info_in_schema(
            create,
            schema,
            CATALOG_CHARSET,
            CATALOG_COLLATION,
            &mb_ctx,
            context.enable_check_constraint(),
        )?;
        rewrite_partition_query_string(context, create, &template)?;
        Self::check_and_split(&mb_ctx, create, &mut template)?;
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
    ///
    /// The template's build already reported the statement's warnings when
    /// the DDL was routed; Go builds the table once, so a rebuild under the
    /// database's charset must not report them a second time.
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
        let mb_ctx = self.context.with_isolated_warnings().metabuild_context();
        let mut info = tidb_ddl_build::table_info_build::build_table_info_in_schema(
            &self.create,
            &self.schema,
            charset,
            collate,
            &mb_ctx,
            self.context.enable_check_constraint(),
        )?;
        Self::check_and_split(&mb_ctx, &self.create, &mut info)?;
        Ok(info)
    }

    /// Go `CreateTable` after `BuildTableInfoWithStmt` and
    /// `rewritePartitionQueryString`: `checkTableInfoValidWithStmt`, then the
    /// region split policies.
    fn check_and_split(
        mb_ctx: &tidb_expr::metabuild::Context,
        create: &CreateTableStmt,
        info: &mut TableInfo,
    ) -> Result<(), DdlAdmissionError> {
        tidb_ddl_build::table_info_build::check_table_info_valid_with_stmt(mb_ctx, info)?;
        tidb_ddl_build::table_info_build::set_create_table_split_policies(create, info)
    }
}

/// Go `rewritePartitionQueryString` (`partition.go:732`): the job of a
/// table written with INTERVAL sugar records, as its query, the partitions
/// the sugar generated.
fn rewrite_partition_query_string(
    context: &dyn DdlSessionContext,
    create: &CreateTableStmt,
    info: &TableInfo,
) -> Result<(), DdlAdmissionError> {
    let Some(interval) = create
        .partitioning
        .as_ref()
        .and_then(|partitioning| partitioning.method.interval.as_ref())
    else {
        return Ok(());
    };
    let Some(partition) = info.partition.as_ref() else {
        return Ok(());
    };
    // The statement text is spliced when the job copies it
    // (`create_table::prepare_create_table_job_submission`), which also
    // refuses a text that does not contain the sugar.
    let sugar = String::from_utf8_lossy(interval.original_text()).into_owned();
    let (mut columns, hidden): (Vec<_>, Vec<_>) = info
        .cols()
        .iter_deref()
        .partition(|column| !column.read().hidden);
    columns.extend(hidden);
    let spec = tidb_cluster::table_from_meta::partition_spec_for(&columns, &partition.read())
        .map_err(DdlAdmissionError::from_mysql_error)?;
    let definitions = tidb_ddl_build::table_partition::append_partition_defs(
        &spec.definitions,
        &spec.kind,
        context.sql_mode().ansi_quotes,
    );
    context.rewrite_ddl_job_query(sugar, definitions.trim_start_matches('\n').to_owned());
    Ok(())
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
    /// Metadata-only MODIFY/CHANGE, admitted by the standalone lowering.
    Modify {
        /// `MODIFY COLUMN IF EXISTS`: a missing column is a note, not a
        /// sub-job (Go `ModifyColumn`'s `spec.IfExists`).
        if_exists: bool,
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
        modify_column_type: Option<tidb_ddl_build::modify_column_type::ModifyColumnType>,
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
    /// `DROP CHECK name` beside another specification: Go's
    /// `DropCheckConstraint` looks the constraint up before
    /// `fillMultiSchemaInfo` refuses its job.
    DropCheck {
        /// The constraint name as written.
        name: String,
        /// The issuing statement's context.
        context: DdlStatementContext,
    },
    /// `ALTER CHECK name [NOT] ENFORCED` beside another specification; see
    /// [`Self::DropCheck`].
    AlterCheck {
        /// The constraint name as written.
        name: String,
        /// The requested enforcement.
        enforced: bool,
        /// The issuing statement's context.
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
    /// A specification Go's `fillMultiSchemaInfo` has no sub-job for, as its
    /// own statement: Go's executor function still runs its checks, and only
    /// the job it builds is refused (`ErrRunMultiSchemaChanges`); one that
    /// builds no job (`REMOVE TTL` on a table without TTL) is a no-op.
    NoSubJob(Box<DdlStatement>),
    /// A specification Go's `AlterTable` loop refuses when it reaches it
    /// (`ErrUnsupportedAlterTableOption` for a table option it has no arm
    /// for), after the specifications before it ran their checks.
    Refused(DdlAdmissionError),
}

/// A `StmtContext` wrapper carrying the Debug the statement enum derives.
#[derive(Clone)]
pub struct DdlStatementContext(pub DdlContext);

/// The session a DDL statement runs in (Go's `sessionctx.Context` as
/// `pkg/ddl` reads it), shared by the jobs the statement builds.
pub type DdlContext = std::sync::Arc<dyn DdlSessionContext>;

impl std::fmt::Debug for DdlStatementContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DdlStatementContext")
    }
}

impl AlterColumnAction {
    fn statement_context(&self) -> Option<&DdlContext> {
        match self {
            Self::Add { context, .. }
            | Self::Modify { context, .. }
            | Self::AddIndex { context, .. }
            | Self::AddForeignKey { context, .. }
            | Self::AddCheck { context, .. }
            | Self::DropCheck { context, .. }
            | Self::AlterCheck { context, .. }
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
    pub fn statement_context(&self) -> Option<&DdlContext> {
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
    /// `ALTER DATABASE ... SET TIFLASH REPLICA`, Go's
    /// `ModifySchemaSetTiFlashReplica`: one `ActionSetTiFlashReplica` job per
    /// table, which the statement's runner submits in turn
    /// ([`alter_schema::schema_tiflash_replica_tables`]).
    ModifySchemaSetTiFlashReplica {
        /// The database name as written.
        name: String,
        /// Requested replica count.
        count: u64,
        /// Placement labels, in written order.
        labels: Vec<String>,
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
        /// `SET HYPO TIFLASH REPLICA`: Go `setHypoTiFlashReplica` records it
        /// in the session instead of submitting a job.
        hypo: bool,
    },
    /// Go `AddResourceGroup` -> the `ActionCreateResourceGroup` job
    /// (`onCreateResourceGroup`): the group the session admitted, given its
    /// ID by the job.
    CreateResourceGroup {
        /// The group, built from the statement's options.
        group: Box<tidb_model::ResourceGroupInfo>,
    },
    /// Go `AlterResourceGroup` -> the `ActionAlterResourceGroup` job.
    AlterResourceGroup {
        /// The existing group with the statement's settings applied.
        group: Box<tidb_model::ResourceGroupInfo>,
    },
    /// Go `DropResourceGroup` -> the `ActionDropResourceGroup` job.
    DropResourceGroup {
        /// The group being dropped.
        group: Box<tidb_model::ResourceGroupInfo>,
    },
    /// Go `AlterTableAffinity` -> the `ActionAlterTableAffinity` job
    /// (`onAlterTableAffinity`): the table's affinity and its PD groups.
    AlterTableAffinity {
        /// Database containing the table.
        schema: String,
        /// Table whose affinity is set.
        table: String,
        /// The level as written; Go parses it after resolving the table.
        level: String,
    },
    /// Go `AlterTableAttributes` -> the `ActionAlterTableAttributes` job
    /// (`onAlterTableAttributes`): the table's region-label rule.
    AlterTableAttributes {
        /// Database containing the table.
        schema: String,
        /// Table whose attributes are set.
        table: String,
        /// The attributes as written.
        attributes: tidb_ast::AttributesSpec,
    },
    /// Go `AlterTablePartitionAttributes` -> the
    /// `ActionAlterTablePartitionAttributes` job
    /// (`onAlterTablePartitionAttributes`): one partition's region-label rule.
    AlterTablePartitionAttributes {
        /// Database containing the table.
        schema: String,
        /// Partitioned table.
        table: String,
        /// The partition as written.
        partition: String,
        /// The attributes, or `None` for `ATTRIBUTES=DEFAULT`.
        attributes: Option<String>,
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
    /// `ALTER TABLE ... DROP PARTITION` (and FIRST PARTITION LESS THAN),
    /// checked by the ordinary partition DDL implementation.
    DropPartitions {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// Canonical SQL for the shared partition DDL implementation.
        sql: String,
        /// The action as written: the partition names in statement order.
        action: Box<tidb_ast::AlterPartitionAction>,
    },
    /// `ALTER TABLE ... TRUNCATE PARTITION`, Go's
    /// `ActionTruncateTablePartition`.
    TruncatePartitions {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// Canonical SQL for the shared partition DDL implementation.
        sql: String,
        /// The action as written: ALL, or the names in statement order.
        action: Box<tidb_ast::AlterPartitionAction>,
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
        modify_column_type: Option<tidb_ddl_build::modify_column_type::ModifyColumnType>,
        /// Go `spec.IfExists`: a missing source column is a 1054 note, not
        /// an error (`executor.ModifyColumn` / `ChangeColumn`).
        if_exists: bool,
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
        /// `DROP HYPO INDEX`: Go `dropHypoIndexFromCtx` removes it from the
        /// session instead of submitting a job.
        hypo: bool,
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
    /// `ALTER TABLE ... PARTITION p PLACEMENT POLICY [=] name`, Go's
    /// `AlterTablePartitionPlacement` -> `ActionAlterTablePartitionPlacement`.
    AlterTablePartitionPlacement {
        /// The resolved database name.
        schema: String,
        /// The table name as written.
        table: String,
        /// The partition name as written.
        partition: String,
        /// The policy as written; `DEFAULT` clears the partition's.
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
            // Go's loop has no case for them.
            DatabaseOption::Encryption(_) | DatabaseOption::SetTiFlashReplica { .. } => {}
        }
    }

    if charset.is_some() || collate.is_some() {
        resolve_charset_collation(
            &[
                (charset.unwrap_or(""), collate.unwrap_or("")),
                (CATALOG_CHARSET, CATALOG_COLLATION),
            ],
            "",
        )
        .map_err(DdlAdmissionError::from_mysql_error)
    } else {
        Ok((CATALOG_CHARSET.to_owned(), CATALOG_COLLATION.to_owned()))
    }
}

/// Admits one parsed statement as a catalog change, or explains why not,
/// under the session that parsed it: admission and metadata persistence
/// observe the same statement context as the parser.
///
/// `None` means the statement is not a DDL this module owns at all, so the
/// caller runs it down its ordinary path. `Err` means it *is* one of the
/// shapes but carries something this node refuses — that refusal is final and
/// happens before any mutation.
pub fn lower_ddl_with_context(
    statement: &Stmt,
    default_schema: &str,
    context: &DdlContext,
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
            // Go `checkMultiSchemaSpecs`: only one SET TIFLASH REPLICA may
            // appear among several specifications.
            if options
                .iter()
                .filter(|option| matches!(option, DatabaseOption::SetTiFlashReplica { .. }))
                .count()
                > 1
            {
                return Err(DdlAdmissionError::from_terror(
                    &tidb_util::dbterror::ERR_RUN_MULTI_SCHEMA_CHANGES
                        .generate_with_stack_by_args(&["set tiflash replica".into()]),
                ));
            }
            // Go `AlterSchema`'s option loop: ENCRYPTION is parsed and
            // ignored; each of the three changes is its own job, run in
            // order by [`split_alter_database`]'s parts.
            let mut charset = None;
            let mut collate = None;
            let mut placement_policy = None;
            let mut tiflash_replica = None;
            for option in options {
                match option {
                    DatabaseOption::CharacterSet(value) => charset = Some(value.as_str()),
                    DatabaseOption::Collate(value) => collate = Some(value.as_str()),
                    DatabaseOption::PlacementPolicy(value) => placement_policy = Some(value),
                    DatabaseOption::SetTiFlashReplica { count, labels } => {
                        tiflash_replica = Some((*count, labels.clone()))
                    }
                    DatabaseOption::Encryption(_) => {}
                }
            }
            let alter_charset = charset.is_some() || collate.is_some();
            if [alter_charset, placement_policy.is_some(), tiflash_replica.is_some()]
                .into_iter()
                .filter(|job| *job)
                .count()
                > 1
            {
                return Ok(None);
            }
            let name = name.clone().unwrap_or_else(|| default_schema.to_owned());
            if let Some((count, labels)) = tiflash_replica {
                return Ok(Some(DdlStatement::ModifySchemaSetTiFlashReplica {
                    name,
                    count,
                    labels,
                }));
            }
            if let Some(policy) = placement_policy {
                return Ok(Some(DdlStatement::ModifySchemaDefaultPlacement {
                    name,
                    placement_policy: policy.clone(),
                }));
            }
            // Go builds no job without a charset, collation or placement.
            if !alter_charset {
                return Ok(Some(DdlStatement::AcceptedNoOp {
                    schema: String::new(),
                    table: String::new(),
                    warning: None,
                }));
            }
            // Go `AlterSchema` walks the options in order: a second CHARACTER
            // SET, or a COLLATE of another character set than the one chosen
            // so far, is `ErrConflictingDeclarations` (1302).
            let mut chosen: Option<String> = None;
            let conflict = |current: &str, other: &str| {
                DdlAdmissionError::from_terror(
                    &tidb_util::dbterror::ERR_CONFLICTING_DECLARATIONS
                        .generate_with_stack_by_args(&[current.into(), other.into()]),
                )
            };
            for option in options {
                match option {
                    DatabaseOption::CharacterSet(value) => match &chosen {
                        None => chosen = Some(value.clone()),
                        Some(current) if current != value => return Err(conflict(current, value)),
                        Some(_) => {}
                    },
                    DatabaseOption::Collate(value) => {
                        let (collation_charset, _) =
                            resolve_charset_collation(
                                &[("", value.as_str()), (CATALOG_CHARSET, CATALOG_COLLATION)],
                                "",
                            )
                            .map_err(DdlAdmissionError::from_mysql_error)?;
                        match &chosen {
                            None => chosen = Some(collation_charset),
                            Some(current) if *current != collation_charset => {
                                return Err(conflict(current, &collation_charset))
                            }
                            Some(_) => {}
                        }
                    }
                    _ => {}
                }
            }
            let (charset, collate) = resolve_charset_collation(
                &[
                    (charset.unwrap_or(""), collate.unwrap_or("")),
                    (CATALOG_CHARSET, CATALOG_COLLATION),
                ],
                "",
            )
            .map_err(DdlAdmissionError::from_mysql_error)?;
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
                return Err(DdlAdmissionError::from_terror(
                    &tidb_util::dbterror::ERR_WRONG_USAGE
                        .generate_with_stack("Incorrect usage of OR REPLACE and IF NOT EXISTS"),
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
        DdlStmt::AlterTable(alter) => {
            let alter = resolve_alter_table_spec(alter, context)?;
            lower_alter_table_catalog(&alter, default_schema, context)
        }
        DdlStmt::RenameTable(rename) => lower_rename_table_stmt(rename, default_schema),
        DdlStmt::TruncateTable(name) => {
            let (schema, table) = split_name(name, default_schema, "table")?;
            Ok(Some(DdlStatement::TruncateTable { schema, table }))
        }
        DdlStmt::CreateSequence(create) => {
            let (schema, name) = split_name(&create.name, default_schema, "sequence")?;
            let (info, comment) = tidb_ddl_build::sequence::build_create_sequence(create, &schema, &name)
                .map_err(DdlAdmissionError::from_mysql_error)?;
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
        DdlPlanError::Admission(DdlAdmissionError::plain(
            error.to_string(),
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
    if name.go_lower_eq("default") {
        return Ok(None);
    }
    let Some(policy) = find_policy(snapshot, name)? else {
        return Err(DdlPlanError::Admission(DdlAdmissionError::from_terror(
            &tidb_error::catalog::infoschema::ERR_PLACEMENT_POLICY_NOT_EXISTS
                .generate_with_stack_by_args(&[name.into()]),
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

/// Go `fillMultiSchemaInfo`'s default arm:
/// `dbterror.ErrRunMultiSchemaChanges.FastGenByArgs(job.Type.String())`.
fn go_unsupported_multi_schema_change(job: &str) -> DdlAdmissionError {
    DdlAdmissionError::from_terror(
        &tidb_util::dbterror::ERR_RUN_MULTI_SCHEMA_CHANGES
            .generate_with_stack_by_args(&[job.into()]),
    )
}

/// The job type Go builds for a specification `fillMultiSchemaInfo` has no
/// sub-job for (its `ActionType.String()`).
fn go_multi_schema_job_without_sub_job(action: &tidb_ast::AlterTableAction) -> Option<&'static str> {
    use tidb_ast::{AlterPartitionAction, AlterTableAction};
    Some(match action {
        AlterTableAction::AlterCheck(_) => "alter check constraint",
        AlterTableAction::DropCheck(_) => "drop check constraint",
        AlterTableAction::SetAttributes(_) => "alter table attributes",
        AlterTableAction::Cache(_) => "alter table cache",
        AlterTableAction::SetTiFlashReplica { .. } => "set tiflash replica",
        AlterTableAction::Partition(partition) => match partition {
            AlterPartitionAction::Add { .. } => "add partition",
            AlterPartitionAction::Drop { .. } => "drop partition",
            AlterPartitionAction::Truncate { .. } => "truncate partition",
            AlterPartitionAction::Exchange { .. } => "exchange partition",
            AlterPartitionAction::Reorganize { .. } | AlterPartitionAction::Coalesce { .. } => {
                "alter table reorganize partition"
            }
            AlterPartitionAction::Repartition { .. } => "alter table partition by",
            AlterPartitionAction::RemovePartitioning => "alter table remove partitioning",
            AlterPartitionAction::SetAttributes { .. } => "alter table partition attributes",
            _ => return None,
        },
        _ => return None,
    })
}

/// Go `executor.AlterTable`'s spec walk (`pkg/ddl/executor.go:1763-1774`,
/// `:1952`): the partition maintenance specs and the table options it has
/// no arm for are refused with their own errors.
fn check_alter_table_spec_refusals(alter: &AlterTableStmt) -> Result<(), DdlAdmissionError> {
    use tidb_ast::{AlterPartitionAction, PartitionMaintenanceOp};
    use tidb_util::dbterror as dbt;
    for action in &alter.actions {
        match action {
            tidb_ast::AlterTableAction::Partition(partition) => {
                let (proto, message): (&tidb_error::terror::TerrorError, &str) = match partition {
                    AlterPartitionAction::MergeFirstPartitionLessThan { .. } => {
                        (&dbt::ERR_GENERAL_UNSUPPORTED_DDL, "Unsupported MERGE FIRST PARTITION")
                    }
                    AlterPartitionAction::SplitMaxValuePartition { .. } => {
                        (&dbt::ERR_GENERAL_UNSUPPORTED_DDL, "Unsupported SPLIT LAST PARTITION")
                    }
                    AlterPartitionAction::Check { .. } => {
                        (&dbt::ERR_UNSUPPORTED_CHECK_PARTITION, "Unsupported check partition")
                    }
                    AlterPartitionAction::Maintain { operation, .. } => match operation {
                        PartitionMaintenanceOp::Rebuild => (
                            &dbt::ERR_UNSUPPORTED_REBUILD_PARTITION,
                            "Unsupported rebuild partition",
                        ),
                        PartitionMaintenanceOp::Optimize => (
                            &dbt::ERR_UNSUPPORTED_OPTIMIZE_PARTITION,
                            "Unsupported optimize partition",
                        ),
                        PartitionMaintenanceOp::Repair => (
                            &dbt::ERR_UNSUPPORTED_REPAIR_PARTITION,
                            "Unsupported repair partition",
                        ),
                    },
                    _ => continue,
                };
                return Err(DdlAdmissionError::from_terror(
                    &proto.generate_with_stack(message),
                ));
            }
            // `STATS_PERSISTENT` and `PRE_SPLIT_REGIONS` fall to the option
            // switch's `default` arm: `ErrUnsupportedAlterTableOption`.
            tidb_ast::AlterTableAction::SetTableOptions { options }
                if options.iter().any(|option| {
                    matches!(
                        option,
                        tidb_ast::TableOption::StatsPersistent
                            | tidb_ast::TableOption::PreSplitRegions(_)
                    )
                }) =>
            {
                return Err(DdlAdmissionError::from_terror(
                    &dbt::ERR_UNSUPPORTED_ALTER_TABLE_OPTION.generate_with_stack_by_args(&[]),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// One specification of a multi-spec `ALTER TABLE` as the single statement
/// Go's executor function for it builds.
fn lower_single_alter_spec(
    alter: &AlterTableStmt,
    action: &tidb_ast::AlterTableAction,
    default_schema: &str,
    context: &DdlContext,
) -> Result<Option<DdlStatement>, DdlAdmissionError> {
    let mut single = alter.clone();
    single.actions = vec![action.clone()];
    lower_alter_table_catalog(&single, default_schema, context)
}

/// Go `ResolveAlterTableSpec`: drops the ignorable LOCK / ALGORITHM specs,
/// splits a grouped `ADD COLUMN (...)` (`resolveAlterTableAddColumns`: one
/// ADD COLUMN spec per column, then one ADD CONSTRAINT spec per table
/// constraint), and resolves the last ALGORITHM against every remaining
/// spec before any of them runs.
fn resolve_alter_table_spec(
    alter: &AlterTableStmt,
    context: &DdlContext,
) -> Result<AlterTableStmt, DdlAdmissionError> {
    use tidb_ast::{AlterTableAction, AlterTableAlgorithm};
    let mut algorithm = AlterTableAlgorithm::Default;
    let mut valid_specs = Vec::with_capacity(alter.actions.len());
    for action in &alter.actions {
        match action {
            AlterTableAction::Algorithm(specified) => algorithm = *specified,
            AlterTableAction::Lock(_) => {}
            AlterTableAction::AddColumns {
                if_not_exists,
                columns,
                constraints,
            } => {
                valid_specs.extend(columns.iter().map(|column| AlterTableAction::AddColumn {
                    if_not_exists: *if_not_exists,
                    column: column.clone(),
                    position: tidb_ast::ColumnPosition::Default,
                }));
                valid_specs.extend(constraints.iter().map(|constraint| match constraint {
                    tidb_ast::TableConstraint::Index(index) => {
                        AlterTableAction::AddIndexConstraint(index.clone())
                    }
                    tidb_ast::TableConstraint::Check(check) => {
                        AlterTableAction::AddCheck(check.clone())
                    }
                    tidb_ast::TableConstraint::ForeignKey(foreign_key) => {
                        AlterTableAction::AddForeignKey(foreign_key.clone())
                    }
                }));
            }
            other => valid_specs.push(other.clone()),
        }
    }
    for spec in &valid_specs {
        resolve_alter_algorithm(spec, algorithm, context)?;
    }
    let mut resolved = alter.clone();
    resolved.actions = valid_specs;
    Ok(resolved)
}

/// Go `ResolveAlterAlgorithm` + `getProperAlgorithm`: ADD CONSTRAINT runs
/// INPLACE, everything else INSTANT. A specified algorithm the spec cannot
/// run is an error when no supported one is at least as cheap, else the
/// 1846 lands as an error-level warning.
fn resolve_alter_algorithm(
    spec: &tidb_ast::AlterTableAction,
    specify: tidb_ast::AlterTableAlgorithm,
    context: &DdlContext,
) -> Result<(), DdlAdmissionError> {
    use tidb_ast::{AlterTableAction, AlterTableAlgorithm};
    fn name(algorithm: AlterTableAlgorithm) -> &'static str {
        match algorithm {
            AlterTableAlgorithm::Default => "DEFAULT",
            AlterTableAlgorithm::Copy => "COPY",
            AlterTableAlgorithm::Inplace => "INPLACE",
            AlterTableAlgorithm::Instant => "INSTANT",
        }
    }
    fn rank(algorithm: AlterTableAlgorithm) -> u8 {
        match algorithm {
            AlterTableAlgorithm::Default => 0,
            AlterTableAlgorithm::Copy => 1,
            AlterTableAlgorithm::Inplace => 2,
            AlterTableAlgorithm::Instant => 3,
        }
    }
    let supported = match spec {
        AlterTableAction::AddIndexConstraint(_)
        | AlterTableAction::AddForeignKey(_)
        | AlterTableAction::AddCheck(_) => AlterTableAlgorithm::Inplace,
        _ => AlterTableAlgorithm::Instant,
    };
    if specify == AlterTableAlgorithm::Default || specify == supported {
        return Ok(());
    }
    let message = format!(
        "ALGORITHM={specify} is not supported. Reason: Cannot alter table by {specify}. Try ALGORITHM={supported}.",
        specify = name(specify),
        supported = name(supported),
    );
    if rank(specify) > rank(supported) {
        return Err(DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_ALTER_OPERATION_NOT_SUPPORTED.generate_with_stack(message),
        ));
    }
    context.append_sql_warn(tidb_util::context::SqlWarn::of(
        DdlWarningLevel::Error,
        &tidb_util::dbterror::ERR_ALTER_OPERATION_NOT_SUPPORTED,
        message,
    ));
    Ok(())
}

fn lower_alter_table_catalog(
    alter: &AlterTableStmt,
    default_schema: &str,
    context: &DdlContext,
) -> Result<Option<DdlStatement>, DdlAdmissionError> {
    check_alter_table_spec_refusals(alter)?;
    // Go `AlterTable` with no valid spec left (only LOCK / ALGORITHM)
    // resolves the table and builds no job.
    if alter.actions.is_empty() {
        let (schema, table) = split_name(&alter.name, default_schema, "table")?;
        return Ok(Some(DdlStatement::AcceptedNoOp {
            schema,
            table,
            warning: None,
        }));
    }
    let [action] = alter.actions.as_slice() else {
        // Go's one ActionMultiSchemaChange job: expressible here exactly when
        // every action has an admitted job the catalog transaction owns.
        let mut actions = Vec::with_capacity(alter.actions.len());
        // Go `CheckStorageClassConflictInAlterTableSpecs`, before any
        // specification runs.
        fn admission(error: impl Into<tidb_error::mysql::MysqlError>) -> DdlAdmissionError {
            DdlAdmissionError::from_mysql_error(error.into())
        }
        tidb_ddl_build::storage_class::check_storage_class_conflict_in_alter_specs(
            alter
                .actions
                .iter()
                .flat_map(|action| match action {
                    tidb_ast::AlterTableAction::SetTableOptions { options } => options.as_slice(),
                    _ => &[],
                }),
        )
        .map_err(admission)?;
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
                            if_exists,
                            column,
                            position,
                            context,
                            rename_from,
                            modify_column_type,
                            ..
                        }) => actions.push(AlterColumnAction::Modify {
                            if_exists,
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
                    let Some(statement) = lower_single_alter_spec(alter, action, default_schema, context)? else {
                        return Ok(None);
                    };
                    actions.push(AlterColumnAction::NoSubJob(Box::new(statement)));
                }
                tidb_ast::AlterTableAction::RemoveTtl(_) => {
                    let Some(statement) = lower_single_alter_spec(alter, action, default_schema, context)? else {
                        return Ok(None);
                    };
                    actions.push(AlterColumnAction::NoSubJob(Box::new(statement)));
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
                            // Go `fillMultiSchemaInfo`'s default arm names the
                            // job type of a table option it has no sub-job for.
                            // Each specification's attribute is validated as
                            // Go builds its job (8270 / 8271); a valid one
                            // has no multi-schema sub-job here yet.
                            tidb_ast::TableOption::EngineAttribute(_)
                            | tidb_ast::TableOption::StorageClass(_) => {
                                tidb_ddl_build::storage_class::engine_attribute_from_table_options(
                                    options,
                                )
                                .map_err(admission)?;
                                return Ok(None);
                            }
                            // Go's `AlterTableAutoIDCache`, `ShardRowID` and
                            // `RebaseAutoID(AutoRandomType)` check, then build
                            // a job `fillMultiSchemaInfo` has no sub-job for.
                            tidb_ast::TableOption::AutoIdCache(_)
                            | tidb_ast::TableOption::ShardRowIdBits(_)
                            | tidb_ast::TableOption::AutoRandomBase(_)
                            | tidb_ast::TableOption::ForceAutoRandomBase(_) => {
                                let spec = tidb_ast::AlterTableAction::SetTableOptions {
                                    options: vec![option.clone()],
                                };
                                let Some(statement) =
                                    lower_single_alter_spec(alter, &spec, default_schema, context)?
                                else {
                                    return Ok(None);
                                };
                                actions.push(AlterColumnAction::NoSubJob(Box::new(statement)));
                            }
                            // Go's loop has empty cases for these: no sub-job.
                            option if is_noop_table_option(option) => {}
                            // Go's `default: err = dbterror.ErrUnsupportedAlterTableOption`.
                            _ => actions.push(AlterColumnAction::Refused(
                                DdlAdmissionError::from_terror(
                                    &tidb_util::dbterror::ERR_UNSUPPORTED_ALTER_TABLE_OPTION
                                        .generate_with_stack_by_args(&[]),
                                ),
                            )),
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
                // Go `AlterTable`'s `ConstraintCheck`: a warning while CHECK is
                // off, else `CreateCheckConstraint`'s job.
                tidb_ast::AlterTableAction::AddCheck(definition) => {
                    // With the switch off Go only warns and builds nothing.
                    // On, `CreateCheckConstraint` admits the constraint
                    // against the table as it stands (1054 for a column a
                    // sibling specification adds) before `fillMultiSchemaInfo`
                    // refuses its job beside another specification; the
                    // multi-schema planner reads the table to do both.
                    if !context.enable_check_constraint() {
                        context.append_warning_parts(1105, "tidb_enable_check_constraint is off");
                    } else {
                        actions.push(AlterColumnAction::AddCheck {
                            definition: Box::new(definition.clone()),
                            context: DdlStatementContext(context.clone()),
                        });
                    }
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
                // Go `AlterTable`: each VALIDATION spec appends its own 8200
                // warning; ENABLE / DISABLE KEYS do nothing (MyISAM only).
                tidb_ast::AlterTableAction::WithValidation => {
                    context.append_sql_warn(tidb_util::context::SqlWarn::raised(
                        DdlWarningLevel::Warning,
                        tidb_util::dbterror::ERR_UNSUPPORTED_ALTER_TABLE_WITH_VALIDATION
                            .generate_with_stack_by_args(&[]),
                    ))
                }
                tidb_ast::AlterTableAction::WithoutValidation => {
                    context.append_sql_warn(tidb_util::context::SqlWarn::raised(
                        DdlWarningLevel::Warning,
                        tidb_util::dbterror::ERR_UNSUPPORTED_ALTER_TABLE_WITHOUT_VALIDATION
                            .generate_with_stack_by_args(&[]),
                    ))
                }
                tidb_ast::AlterTableAction::SetKeysEnabled(_) => {}
                // Go `AlterTable`: `DropCheckConstraint` always runs; ALTER
                // CHECK warns while CHECK is off.
                tidb_ast::AlterTableAction::DropCheck(drop) => {
                    actions.push(AlterColumnAction::DropCheck {
                        name: drop.name.clone(),
                        context: DdlStatementContext(context.clone()),
                    });
                }
                tidb_ast::AlterTableAction::AlterCheck(alter_check) => {
                    if !context.enable_check_constraint() {
                        context.append_warning_parts(1105, "tidb_enable_check_constraint is off");
                    } else {
                        actions.push(AlterColumnAction::AlterCheck {
                            name: alter_check.name.clone(),
                            enforced: alter_check.enforced,
                            context: DdlStatementContext(context.clone()),
                        });
                    }
                }
                // Go `AlterTable` refuses a masking-policy spec beside another
                // before running it.
                tidb_ast::AlterTableAction::MaskingPolicy(_) => actions.push(
                    AlterColumnAction::Refused(go_unsupported_multi_schema_change("masking policy")),
                ),
                // Every other spec runs its own executor function: a job it
                // builds has no sub-job (`DoDDLJob` refuses it), an error is
                // the statement's, and a spec that builds nothing passes.
                other => match lower_single_alter_spec(alter, other, default_schema, context)? {
                    Some(statement) => {
                        actions.push(AlterColumnAction::NoSubJob(Box::new(statement)));
                    }
                    None => match go_multi_schema_job_without_sub_job(other) {
                        Some(job) => actions
                            .push(AlterColumnAction::Refused(go_unsupported_multi_schema_change(job))),
                        // A specification this node cannot run at all.
                        None => return Ok(None),
                    },
                },
            }
        }
        let (schema, table) = split_name(&alter.name, default_schema, "table")?;
        // Go `multiSchemaChange` builds no job without a sub-job.
        if actions.is_empty() {
            return Ok(Some(DdlStatement::AcceptedNoOp {
                schema,
                table,
                warning: None,
            }));
        }
        return Ok(Some(DdlStatement::MultiSchemaChange {
            schema,
            table,
            actions,
        }));
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
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::SetTiFlashReplica {
                schema,
                table,
                count: *count,
                labels: labels.clone(),
                reset_available: false,
                hypo: *hypo,
            }))
        }
        // Go `AlterTable` handles a spec's TTL, TTL_ENABLE and
        // TTL_JOB_INTERVAL once, as one `AlterTableTTLInfoOrEnable`.
        tidb_ast::AlterTableAction::SetTableOptions { options }
            if !options.is_empty() && options.iter().all(is_ttl_table_option) =>
        {
            let ttl_info = tidb_ddl_build::ttl_info_from_options(options)
                .map_err(DdlAdmissionError::from_mysql_error)?;
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
        tidb_ast::AlterTableAction::SetTableOptions { options }
            if !options.is_empty()
                && options.iter().all(|option| {
                    matches!(
                        option,
                        tidb_ast::TableOption::CharacterSet(_) | tidb_ast::TableOption::Collate(_)
                    )
                }) =>
        {
            // Go's `AlterTableOption` loop handles every charset/collate
            // option at once: `GetCharsetAndCollateInTableOption` from the
            // first, then one `AlterTableCharsetAndCollate` job.
            let (charset, collate) = tidb_ddl_build::column_types::get_charset_and_collate_in_table_option(
                0,
                options,
                context.default_collation_for_utf8mb4(),
            )
            .map_err(DdlAdmissionError::from_mysql_error)?;
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::ModifyTableCharsetAndCollate {
                schema,
                table,
                charset,
                collate,
                // Go `NeedToOverwriteColCharset`: only CONVERT TO overwrites
                // the columns, and that is its own action here.
                overwrite_columns: false,
            }))
        }
        tidb_ast::AlterTableAction::SetTableOptions { options } => {
            // Go folds every ENGINE_ATTRIBUTE / STORAGE_CLASS written in one
            // specification into a single job, validating each (8270 / 8271).
            if options.len() > 1
                && options.iter().all(|option| {
                    matches!(
                        option,
                        tidb_ast::TableOption::EngineAttribute(_)
                            | tidb_ast::TableOption::StorageClass(_)
                    )
                })
            {
                let engine_attribute =
                    tidb_ddl_build::storage_class::engine_attribute_from_table_options(options)
                        .map_err(DdlAdmissionError::from_mysql_error)?
                        .expect("the options are all storage-class options");
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return Ok(Some(DdlStatement::ModifyEngineAttribute {
                    schema,
                    table,
                    engine_attribute,
                }));
            }
            let [option] = options.as_slice() else {
                return Ok(None);
            };
            if let tidb_ast::TableOption::AutoIdCache(value) = option {
                let new_cache = value.parse::<u64>().map_err(|_| {
                    DdlAdmissionError::plain(
                        "AUTO_ID_CACHE needs an integer value",
                    )
                })?;
                if new_cache > i64::MAX as u64 {
                    return Err(DdlAdmissionError::plain(
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
            if let tidb_ast::TableOption::EngineAttribute(_)
            | tidb_ast::TableOption::StorageClass(_) = option
            {
                let engine_attribute =
                    tidb_ddl_build::storage_class::engine_attribute_from_table_options(options)
                        .map_err(DdlAdmissionError::from_mysql_error)?
                        .expect("the one option is a storage-class option");
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return Ok(Some(DdlStatement::ModifyEngineAttribute {
                    schema,
                    table,
                    engine_attribute,
                }));
            }
            if let tidb_ast::TableOption::Affinity(level) = option {
                let (schema, table) = split_name(&alter.name, default_schema, "table")?;
                return Ok(Some(DdlStatement::AlterTableAffinity {
                    schema,
                    table,
                    level: level.clone(),
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
                    DdlAdmissionError::plain(
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
                let comment = tidb_ddl_build::validate_comment_length(
                    comment,
                    &tidb_hack::go_to_lower(&table),
                    tidb_ddl_build::CommentOwner::Table,
                    &context.metabuild_context(),
                )
                .map_err(DdlAdmissionError::from_mysql_error)?;
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
                    DdlAdmissionError::plain(
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
                // Options Go's `AlterTable` switch runs that are planned
                // elsewhere (charset/collate, TTL).
                tidb_ast::TableOption::CharacterSet(_)
                | tidb_ast::TableOption::Collate(_)
                | tidb_ast::TableOption::Ttl { .. }
                | tidb_ast::TableOption::TtlEnable(_)
                | tidb_ast::TableOption::TtlJobInterval(_) => return Ok(None),
                // Go `AlterTable`'s option switch default:
                // `dbterror.ErrUnsupportedAlterTableOption`.
                _ => {
                    return Err(DdlAdmissionError::from_terror(
                        &tidb_util::dbterror::ERR_UNSUPPORTED_ALTER_TABLE_OPTION
                            .generate_with_stack_by_args(&[]),
                    ))
                }
            };
            let next = value.parse::<u64>().map_err(|_| {
                DdlAdmissionError::plain(
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
            // Go `GetModifiableColumnJob` handles an AUTO_RANDOM change too
            // (`checkAutoRandom`, then the owner's
            // `checkAndApplyAutoRandomBits`).
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::ModifyColumn {
                schema,
                table,
                column: Box::new(column.clone()),
                position: position.clone(),
                context: DdlStatementContext(context.clone()),
                rename_from: None,
                modify_column_type: None,
                if_exists: *if_exists,
            }))
        }
        tidb_ast::AlterTableAction::ChangeColumn {
            if_exists,
            old_name,
            column,
            position,
        } => {
            let [old] = old_name.as_slice() else {
                return Err(DdlAdmissionError::plain(
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
                if_exists: *if_exists,
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
                    return Err(DdlAdmissionError::from_terror(
                        &tidb_util::dbterror::ERR_UNKNOWN_CHARACTER_SET
                            .generate_with_stack_by_args(&[name.into()]),
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
                return Err(DdlAdmissionError::plain(
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
            action @ (tidb_ast::AlterPartitionAction::Drop { .. }
            | tidb_ast::AlterPartitionAction::FirstPartitionLessThan { .. }),
        ) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::DropPartitions {
                schema,
                table,
                sql: Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::AlterTable(Box::new(
                    alter.clone(),
                ))))
                .restore(),
                action: Box::new(action.clone()),
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
        tidb_ast::AlterTableAction::Partition(
            action @ tidb_ast::AlterPartitionAction::Truncate { .. },
        ) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::TruncatePartitions {
                schema,
                table,
                sql: Stmt::Ddl(tidb_ast::NodeBox::new(DdlStmt::AlterTable(Box::new(
                    alter.clone(),
                ))))
                .restore(),
                action: Box::new(action.clone()),
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
        // Go `AlterTablePartitionOptions`: PLACEMENT POLICY is the one
        // partition option; the last one written wins.
        tidb_ast::AlterTableAction::Partition(tidb_ast::AlterPartitionAction::SetOptions {
            partition,
            options,
        }) => {
            let mut policy = None;
            for option in options {
                match option {
                    tidb_ast::TableOption::PlacementPolicy(name) => policy = Some(name.clone()),
                    _ => {
                        return Err(DdlAdmissionError::plain(
                            "unknown partition option".to_owned(),
                        ))
                    }
                }
            }
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            match policy {
                Some(policy) => Ok(Some(DdlStatement::AlterTablePartitionPlacement {
                    schema,
                    table,
                    partition: partition.clone(),
                    policy,
                })),
                None => Ok(Some(DdlStatement::AcceptedNoOp {
                    schema,
                    table,
                    warning: None,
                })),
            }
        }
        tidb_ast::AlterTableAction::SetAttributes(attributes) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AlterTableAttributes {
                schema,
                table,
                attributes: attributes.clone(),
            }))
        }
        tidb_ast::AlterTableAction::Partition(tidb_ast::AlterPartitionAction::SetAttributes {
            partition,
            attributes,
        }) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AlterTablePartitionAttributes {
                schema,
                table,
                partition: partition.clone(),
                attributes: attributes.clone(),
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
        // Go `AlterTable`: ENABLE / DISABLE KEYS are MyISAM-only; nothing
        // to do.
        tidb_ast::AlterTableAction::SetKeysEnabled(_) => {
            let (schema, table) = split_name(&alter.name, default_schema, "table")?;
            Ok(Some(DdlStatement::AcceptedNoOp {
                schema,
                table,
                warning: None,
            }))
        }
        tidb_ast::AlterTableAction::SetStatsOptions(_) => Err(DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_ALTER_TABLE_OPTION
                .generate_with_stack_by_args(&[]),
        )),
        // Go `AlterTable`'s default arm: the specifications it has no job for.
        tidb_ast::AlterTableAction::Force
        | tidb_ast::AlterTableAction::TablespaceImport(_)
        | tidb_ast::AlterTableAction::Partition(
            tidb_ast::AlterPartitionAction::ImportTablespace { .. }
            | tidb_ast::AlterPartitionAction::DiscardTablespace { .. },
        ) => Err(DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_ALTER_TABLE_SPEC.generate_with_stack_by_args(&[]),
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
        [] => Err(DdlAdmissionError::plain(
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
    context: &DdlContext,
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
        return Err(DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_PRIMARY_KEY.generate_with_stack(
                "Adding clustered primary key is not supported. Please consider adding \
             NONCLUSTERED primary key instead",
            ),
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
            return Err(DdlAdmissionError::from_terror(
                &tidb_util::dbterror::ERR_UNSUPPORTED_ADD_COLUMNAR_INDEX.generate_with_stack(
                    "FULLTEXT index is only supported in starter deployment mode",
                ),
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
        return Err(DdlPlanError::from(
            tidb_error::catalog::infoschema::ERR_COLUMN_NOT_EXISTS.generate_with_stack_by_args(&[
                (from.to_owned()).into(),
                (table.to_owned()).into(),
            ]),
        ));
    };
    // Go asks `IsColumnRenameableWithCheckConstraint` before the
    // same-name early return and before the duplicate-name check.
    if let Some(constraint) = stored.constraints.iter_deref().find(|constraint| {
        tidb_ddl_build::check_constraint::uses_column(&constraint.read(), from)
    }) {
        let constraint = constraint.read();
        let error = tidb_ddl_build::check_constraint::column_dependency_error(
            constraint.name.original(),
            from,
        );
        return Err(DdlPlanError::Admission(DdlAdmissionError::from_terror(
            &error.proto.generate_with_stack(error.message),
        )));
    }
    if wanted == to.go_to_lower() {
        return Ok(None);
    }
    let new_name = to.go_to_lower();
    if new_name == "_tidb_rowid" {
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_WRONG_COLUMN_NAME,
            format!("Incorrect column name '{new_name}'"),
        ));
    }
    if stored
        .columns
        .iter_deref()
        .any(|candidate| candidate.read().name.lowercase() == new_name)
    {
        return Err(DdlPlanError::from(
            tidb_error::catalog::infoschema::ERR_COLUMN_EXISTS
                .generate_with_stack_by_args(&[(to.to_owned()).into()]),
        ));
    }
    // Go `checkModifyColumnWithGeneratedColumnsConstraint`.
    if let Some(dependent) = stored.columns.iter_deref().find(|candidate| {
        let candidate = candidate.read();
        candidate.is_generated() && candidate.dependences.contains(&wanted)
    }) {
        return Err(if dependent.read().hidden {
            coded_refusal(
                &tidb_util::dbterror::ERR_DEPENDENT_BY_FUNCTIONAL_INDEX,
                format!(
                    "Column '{from}' has an expression index dependency and cannot be dropped \
                     or renamed"
                ),
            )
        } else {
            coded_refusal(
                &tidb_util::dbterror::ERR_DEPENDENT_BY_GENERATED_COLUMN,
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

/// Go `checkModifyColumnWithGeneratedColumnsConstraint`: the error a change
/// of `wanted` owes when a generated column (or an expression index's hidden
/// column) reads it, named as written.
fn check_modify_column_with_generated_columns_constraint(
    stored: &TableInfo,
    wanted: &str,
    written: &str,
) -> Option<(&'static TerrorError, String)> {
    let dependent = stored.columns.iter_deref().find(|candidate| {
        let candidate = candidate.read();
        candidate.is_generated() && candidate.dependences.contains(wanted)
    })?;
    let hidden = dependent.read().hidden;
    Some(if hidden {
        (
            &*tidb_util::dbterror::ERR_DEPENDENT_BY_FUNCTIONAL_INDEX,
            format!(
                "Column '{written}' has an expression index dependency and cannot be dropped or \
                 renamed"
            ),
        )
    } else {
        (
            &*tidb_util::dbterror::ERR_DEPENDENT_BY_GENERATED_COLUMN,
            format!("Column '{written}' has a generated column dependency."),
        )
    })
}

/// Go `isGeneratedRelatedColumn`: a change that may rewrite the data may not
/// touch a generated column or one a generated column reads.
fn is_generated_related_column(
    stored: &TableInfo,
    new: &tidb_model::ColumnInfo,
    old: &tidb_model::ColumnInfo,
) -> Result<(), DdlPlanError> {
    let unsupported = |reason: String| {
        coded_refusal(
            &tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN,
            format!("Unsupported modify column: {reason}"),
        )
    };
    if new.is_generated() {
        return Err(unsupported("new column is generated".to_owned()));
    }
    if old.is_generated() {
        return Err(unsupported("old column is generated".to_owned()));
    }
    // Go `hasDependentByGeneratedColumn`, whose `dep` is `colName.L`.
    let name = old.name.lowercase();
    if stored
        .columns
        .iter_deref()
        .any(|candidate| candidate.read().dependences.contains(name))
    {
        return Err(unsupported(format!(
            "oldCol is a dependent column '{name}' for generated column"
        )));
    }
    Ok(())
}

/// Go `checkModifyGeneratedColumn` (`pkg/ddl/generated_column.go`): the five
/// rules a MODIFY / CHANGE of (or to) a generated column must pass.
fn check_modify_generated_column(
    stored: &TableInfo,
    schema: &str,
    position: usize,
    old: &tidb_model::ColumnInfo,
    new: &tidb_model::ColumnInfo,
    new_def: &tidb_ast::ColumnDef,
    requested_position: &tidb_ast::ColumnPosition,
    context: &DdlStatementContext,
) -> Result<(), DdlPlanError> {
    let unsupported_on_generated = |what: &str| {
        coded_refusal(
            &tidb_util::dbterror::ERR_UNSUPPORTED_ON_GENERATED_COLUMN,
            format!("'{what}' is not supported for generated columns."),
        )
    };
    let bad_field = |column: &str| {
        coded_refusal(
            &tidb_util::dbterror::ERR_BAD_FIELD,
            format!("Unknown column '{column}' in 'generated column function'"),
        )
    };
    // Rule 1: the STORED status stays.
    if old.is_virtual_generated() != new.is_virtual_generated() {
        return Err(unsupported_on_generated("Changing the STORED status"));
    }

    // Rule 2: every generated column reads only columns that exist and
    // generated columns before it, with this column at its new place.
    let columns = stored
        .columns
        .iter_deref()
        .map(|column| column.read().clone_like_go())
        .collect::<Vec<_>>();
    let mut generation: BTreeMap<String, (usize, bool, Vec<String>)> = BTreeMap::new();
    for (offset, column) in columns.iter().enumerate() {
        if offset == position {
            // Go `findPositionRelativeColumn` over the table's columns.
            let at = match requested_position {
                tidb_ast::ColumnPosition::Default => offset,
                tidb_ast::ColumnPosition::First => 0,
                tidb_ast::ColumnPosition::After(anchor) => {
                    let anchor = anchor.go_to_lower();
                    let Some(relative) = columns
                        .iter()
                        .find(|candidate| candidate.name.lowercase() == anchor)
                    else {
                        return Err(bad_field(&anchor));
                    };
                    usize::try_from(relative.offset).unwrap_or(0) + 1
                }
            };
            generation.insert(
                new.name.lowercase().to_owned(),
                (
                    at,
                    new.is_generated(),
                    new.dependences
                        .snapshot()
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                ),
            );
        } else {
            generation.insert(
                column.name.lowercase().to_owned(),
                (
                    offset,
                    column.is_generated(),
                    column
                        .dependences
                        .snapshot()
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                ),
            );
        }
    }
    // Go `verifyColumnGeneration`, for every column, changed or not.
    for (offset, column) in columns.iter().enumerate() {
        let name = if offset == position {
            new.name.lowercase()
        } else {
            column.name.lowercase()
        };
        let (at, generated, dependences) = &generation[name];
        if !generated {
            continue;
        }
        for dependency in dependences {
            let Some((dependency_at, dependency_generated, _)) = generation.get(dependency) else {
                return Err(bad_field(dependency));
            };
            if *dependency_generated && at <= dependency_at {
                return Err(coded_refusal(
                    &tidb_util::dbterror::ERR_GENERATED_COLUMN_NON_PRIOR,
                    "Generated column can refer only to generated columns defined prior to it."
                        .to_owned(),
                ));
            }
        }
    }

    if new.is_generated() {
        fn admission(error: impl Into<tidb_error::mysql::MysqlError>) -> DdlPlanError {
            DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error.into()))
        }
        // Rule 3: Go `checkIllegalFn4Generated(newCol.Name.L, typeColumn, ...)`.
        if let Some(tidb_ast::ColumnOption::Generated { expression, .. }) = new_def
            .options
            .iter()
            .find(|option| matches!(option, tidb_ast::ColumnOption::Generated { .. }))
        {
            tidb_ddl_build::expression_index::check_illegal_fn_4_generated(
                new.name.lowercase(),
                tidb_ddl_build::expression_index::GenType::Column,
                expression,
            )
            .map_err(admission)?;
        }
        // Rule 4: Go `findDependedColumnNames` with the qualifier check, and
        // `checkAutoIncrementRef` unless the session allows it.
        let (_, dependences) = tidb_ddl_build::generated_column::find_depended_column_names(
            schema,
            stored.name.lowercase(),
            new_def,
        )
        .map_err(admission)?;
        if !context.0.auto_increment_in_generated() {
            if let Some(auto_increment) = columns
                .iter()
                .find(|column| column.field_type.has_flag(FieldTypeFlags::AUTO_INCREMENT))
            {
                if dependences
                    .iter()
                    .any(|dependency| dependency == auto_increment.name.lowercase())
                {
                    return Err(coded_refusal(
                        &tidb_util::dbterror::ERR_GENERATED_COLUMN_REF_AUTO_INC,
                        format!(
                            "Generated column '{}' cannot refer to auto-increment column.",
                            new_def.name.go_to_lower()
                        ),
                    ));
                }
            }
        }
        // Rule 5: Go `checkIndexOrStored`.
        let indexed = stored.indices.iter_deref().any(|index| {
            index.read().columns.iter_deref().any(|part| {
                let part = part.read();
                part.name.lowercase() == old.name.lowercase()
                    || part.name.lowercase() == new.name.lowercase()
            })
        });
        let covered_by_index = || {
            coded_refusal(
                &tidb_util::dbterror::ERR_UNSUPPORTED_ON_GENERATED_COLUMN,
                "Unsupported modification for generated columns covered by an index".to_owned(),
            )
        };
        if old.generated_expr_string == new.generated_expr_string {
            if old.field_type.equal(&new.field_type) || !indexed {
                return Ok(());
            }
            return Err(covered_by_index());
        }
        if new.generated_stored {
            return Err(unsupported_on_generated("modifying a stored column"));
        }
        if indexed {
            return Err(covered_by_index());
        }
    }
    Ok(())
}

/// One sub-action's answer inside an evolving ALTER.
enum AlterColumnOutcome {
    Applied,
    /// The `IF [NOT] EXISTS` no-op, with the sentence `already` reports.
    AlreadySatisfied(String),
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
        return Err(DdlPlanError::from(
            tidb_error::catalog::infoschema::ERR_KEY_NOT_EXISTS.generate_with_stack_by_args(&[
                (from.to_owned()).into(),
                (table.to_owned()).into(),
            ]),
        ));
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
            return Err(DdlPlanError::from(
                tidb_util::dbterror::ERR_DUP_KEY_NAME.generate_with_stack_by_args(&[(existing
                    .read()
                    .name
                    .original()
                    .to_owned())
                .into()]),
            ));
        }
    }
    // Go `renameIndexes` walks every index and renames each one whose name
    // matches, rather than stopping at the first, then renames the expression
    // index's key parts and hidden columns (`renameExpressionIndexColumnRefs`
    // and `onRenameIndex`'s `renameHiddenColumns`, Issue 51431): a hidden
    // column left under the old name would collide with the next index of
    // that name.
    for candidate in info.indices.iter_deref() {
        let mut candidate = candidate.write();
        if candidate.name.lowercase() == from_lower {
            candidate.name = CiString::new(to.to_owned());
        }
    }
    rename_expression_index_column_refs(info, from, to);
    rename_hidden_columns(info, from, to);
    Ok(AlterColumnOutcome::Applied)
}

/// Go `getExpressionIndexOriginName`: `_V$_<index>_<n>` -> `<index>`.
fn get_expression_index_origin_name(name: &str) -> &str {
    let column_name = name.strip_prefix("_V$_").unwrap_or(name);
    match column_name.rfind('_') {
        Some(position) => &column_name[..position],
        None => column_name,
    }
}

/// Go `strings.Replace(s, from, to, 1)`.
fn replace_first(text: &str, from: &str, to: &str) -> String {
    text.replacen(from, to, 1)
}

/// Go `strings.Replace` on a `CIStr`'s original spelling (its lower-case
/// form follows).
fn rename_ci_first(name: &CiString, from: &str, to: &str) -> CiString {
    CiString::new(replace_first(name.original(), from, to))
}

/// Go `renameExpressionIndexColumnRefs`.
fn rename_expression_index_column_refs(info: &TableInfo, from: &str, to: &str) {
    for index in info.indices.iter_deref() {
        for part in index.read().columns.iter_deref() {
            let mut part = part.write();
            let hidden = info
                .columns
                .get(part.offset as usize)
                .is_some_and(|column| column.read().hidden);
            if hidden && get_expression_index_origin_name(part.name.original()) == from {
                part.name = rename_ci_first(&part.name, from, to);
            }
        }
    }
}

/// Go `renameHiddenColumns`.
fn rename_hidden_columns(info: &TableInfo, from: &str, to: &str) {
    for column in info.columns.iter_deref() {
        let mut column = column.write();
        if column.hidden && get_expression_index_origin_name(column.name.original()) == from {
            column.name = rename_ci_first(&column.name, from, to);
        }
    }
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
        return Err(DdlPlanError::from(
            tidb_error::catalog::infoschema::ERR_KEY_NOT_EXISTS.generate_with_stack_by_args(&[
                (index.to_owned()).into(),
                (table.to_owned()).into(),
            ]),
        ));
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
            &tidb_util::dbterror::ERR_UNSUPPORTED_INDEX_TYPE,
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
    context: &dyn DdlSessionContext,
) -> Result<(), DdlPlanError> {
    // Go resolves the target column before it looks at the new default at
    // all, and a non-public one reads as absent.
    let wanted = column.go_to_lower();
    let Some(target) = info.columns.iter_deref().find(|candidate| {
        let candidate = candidate.read();
        candidate.name.lowercase() == wanted && candidate.state == tidb_model::SchemaState::PUBLIC
    }) else {
        return Err(DdlPlanError::from(
            tidb_error::catalog::infoschema::ERR_COLUMN_NOT_EXISTS.generate_with_stack_by_args(&[
                (column.to_owned()).into(),
                (table.to_owned()).into(),
            ]),
        ));
    };
    // Go `ErrInvalidAutoRandom`: the shard bits own the column's values, so a
    // default could never be written.
    if default_value.is_some()
        && info.auto_random_bits > 0
        && target.read().field_type.has_flag(FieldTypeFlags::PRI_KEY)
    {
        return Err(DdlPlanError::from(
            tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[
                ("auto_random is incompatible with default".to_owned()).into(),
            ]),
        ));
    }
    let mut target = target.write();
    tidb_ddl_build::table_info_build::set_column_default(column, &mut target, default_value, &context.metabuild_context())
        .map_err(DdlPlanError::Admission)
}

/// Go refuses an ALTER TABLE charset change that names neither a charset nor
/// a collation before it resolves anything.
fn check_table_charset_named(charset: &str, collate: &str) -> Result<(), DdlPlanError> {
    if charset.is_empty() && collate.is_empty() {
        return Err(DdlPlanError::Admission(
            tidb_ddl_build::table_info_build::DdlAdmissionError::from_terror(
                &tidb_util::dbterror::ERR_UNKNOWN_CHARACTER_SET
                    .generate_with_stack_by_args(&["".into()]),
            ),
        ));
    }
    Ok(())
}

/// Go `checkAlterTableCharset` and `onModifyTableCharsetAndCollate` over an
/// EVOLVING `TableInfo`.
#[allow(clippy::too_many_arguments)]
/// Go `checkIndexLengthWithNewCharset`: every index's key length, summed over
/// its parts with each column converted to the new charset, must still fit
/// `MaxIndexLength` -- `CONVERT TO CHARACTER SET utf8mb4` quadruples a
/// `varchar(1000) utf8` key part from 3000 to 4000 bytes.
fn check_index_length_with_new_charset(
    info: &TableInfo,
    to_charset: &str,
    to_collate: &str,
) -> Result<(), DdlPlanError> {
    // Copy all columns and replace the charset and collate.
    let columns: Vec<tidb_model::ColumnInfo> = info
        .columns
        .iter_deref()
        .map(|column| {
            let mut column = column.read().clone_like_go();
            if column.field_type.has_charset() {
                column.field_type.set_charset_name(to_charset);
                column.field_type.set_collation_name(to_collate);
            } else {
                column.field_type.set_charset_name("binary");
                column.field_type.set_collation_name("binary");
            }
            column
        })
        .collect();
    for index in info.indices.iter_deref() {
        let index = index.read();
        let parts = index
            .columns
            .iter_deref()
            .map(|part| part.read().clone())
            .collect::<Vec<_>>();
        tidb_ddl_build::index::check_index_prefix_length(
            &columns,
            &parts,
            index.get_columnar_index_type(),
        )
        .map_err(|error| {
            DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error))
        })?;
    }
    Ok(())
}

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
    let (to_charset, to_collate) = resolve_charset_collation(
        &[
            (to_charset.as_str(), collate),
            (CATALOG_CHARSET, CATALOG_COLLATION),
        ],
        "",
    )
    .map_err(DdlPlanError::from_mysql_error)?;

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
    let (from_charset, from_collate) = resolve_charset_collation(
        &[
            (
                if info.charset.is_empty() {
                    database.charset.as_str()
                } else {
                    info.charset.as_str()
                },
                if info.collate.is_empty() {
                    database.collate.as_str()
                } else {
                    info.collate.as_str()
                },
            ),
            (CATALOG_CHARSET, CATALOG_COLLATION),
        ],
        "",
    )
    .map_err(DdlPlanError::from_mysql_error)?;
    check_modify_charset_and_collation(
        &to_charset,
        &to_collate,
        &from_charset,
        &from_collate,
        false,
        "",
    )?;

    if overwrite_columns {
        check_index_length_with_new_charset(info, &to_charset, &to_collate)?;
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
            &tidb_error::catalog::autoid::ERR_AUTOINC_READ_FAILED,
            format!(
                "shard_row_id_bits {shard_row_id_bits} will cause next global auto ID {next_global} overflow"
            ),
        ));
    }
    Ok(())
}

/// Go executor `AddColumn` -> `checkAndCreateNewColumn` against `info`: the
/// column the job adds (no id yet) and where it lands once appended.
/// `Ok(Err(detail))` is IF NOT EXISTS over an existing column.
pub(super) fn build_add_column(
    info: &TableInfo,
    schema: &str,
    table: &str,
    column: &tidb_ast::ColumnDef,
    position: &tidb_ast::ColumnPosition,
    if_not_exists: bool,
    context: &dyn DdlSessionContext,
) -> Result<Result<(tidb_model::ColumnInfo, usize), String>, DdlPlanError> {
    // Go `AddColumn`, then `checkAndCreateNewColumn`.
    check_add_column_too_many_columns(info.columns.len() + 1)?;
    check_unsupported_column_constraint(column, schema, table)?;
    // Go `table.FindCol`: `strings.EqualFold(col.Name.O, name)`, so `s`
    // finds an existing `ſ` here although CREATE TABLE keys by `.L`.
    if info.columns.iter_deref().any(|candidate| {
        tidb_hack::GoCaseCompare::go_equal_fold(candidate.read().name.original(), &column.name)
    }) {
        if if_not_exists {
            return Ok(Err(format!(
                "column `{}` already exists on `{schema}`.`{table}`",
                column.name
            )));
        }
        return Err(DdlPlanError::from(
            tidb_error::catalog::infoschema::ERR_COLUMN_EXISTS
                .generate_with_stack_by_args(&[(column.name.clone()).into()]),
        ));
    }
    tidb_ddl_build::check_too_long_identifier(&column.name)
        .map_err(DdlPlanError::from_mysql_error)?;
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
    let added = tidb_ddl_build::table_info_build::build_added_column(
        column,
        &info.charset,
        &info.collate,
        &context.metabuild_context(),
        context.enable_check_constraint(),
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
            &tidb_util::dbterror::ERR_GENERATED_COLUMN_NON_PRIOR,
            "Generated column can refer only to generated columns defined prior to it.",
        ));
    }
    Ok(Ok((added, destination)))
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
                DdlPlanError::from(
                    tidb_error::catalog::infoschema::ERR_COLUMN_NOT_EXISTS
                        .generate_with_stack_by_args(&[
                            (name.clone()).into(),
                            (info.name.original().to_owned()).into(),
                        ]),
                )
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

/// A column / foreign-key / exchange-partition check's refusal, which crosses
/// the reorg boundary as the wire triple (`LockSqlError`) and so has lost the
/// Go identity it was raised with. The checks raise a closed set of Go errors;
/// this names them, and any other number stays a plain refusal.
pub(crate) fn checked_refusal(code: u16, message: impl Into<String>) -> DdlPlanError {
    use tidb_error::catalog::{kv, table, types};
    let proto: Option<&'static TerrorError> = match code {
        1062 => Some(&kv::ERR_KEY_EXISTS),
        1048 => Some(&table::ERR_COLUMN_CANT_NULL),
        1264 => Some(&types::ERR_WARN_DATA_OUT_OF_RANGE),
        1265 => Some(&types::ERR_TRUNCATED),
        1292 => Some(&types::ERR_TRUNCATED_WRONG_VAL),
        1366 => Some(&table::ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD),
        1406 => Some(&types::ERR_DATA_TOO_LONG),
        1690 => Some(&types::ERR_OVERFLOW),
        1365 => Some(&tidb_error::catalog::expression::ERR_DIVISION_BY_ZERO),
        1451 => Some(&tidb_error::plannererrors::ERR_ROW_IS_REFERENCED2),
        1452 => Some(&tidb_util::dbterror::ERR_NO_REFERENCED_ROW2),
        3819 => Some(&tidb_util::dbterror::ERR_CHECK_CONSTRAINT_IS_VIOLATED),
        _ => None,
    };
    match proto {
        Some(proto) => coded_refusal(proto, message),
        None => DdlPlanError::from_mysql_error(tidb_error::mysql::MysqlError::sql_error(code, message)),
    }
}

/// Go `checkUnsupportedColumnConstraint`: the column options ADD COLUMN
/// refuses, checked before the column's name.
fn check_unsupported_column_constraint(
    column: &tidb_ast::ColumnDef,
    schema: &str,
    table: &str,
) -> Result<(), DdlPlanError> {
    for option in &column.options {
        let constraint = match option {
            tidb_ast::ColumnOption::AutoIncrement => "AUTO_INCREMENT",
            tidb_ast::ColumnOption::InlineKey(key) => match key.kind {
                tidb_ast::InlineKeyKind::Primary { .. } => "PRIMARY KEY",
                tidb_ast::InlineKeyKind::Unique => "UNIQUE KEY",
            },
            tidb_ast::ColumnOption::AutoRandom(_) => {
                // `ErrInvalidAutoRandom.GenWithStackByArgs(AutoRandomAlterAddColumn)`.
                return Err(coded_refusal(
                    &tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM,
                    format!(
                        "Invalid auto random: unsupported add column '{}' constraint AUTO_RANDOM when altering '{schema}.{table}'",
                        column.name
                    ),
                ));
            }
            _ => continue,
        };
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_UNSUPPORTED_ADD_COLUMN,
            format!(
                "unsupported add column '{}' constraint {constraint} when altering '{schema}.{table}'",
                column.name
            ),
        ));
    }
    Ok(())
}

fn coded_refusal(proto: &'static TerrorError, message: impl Into<String>) -> DdlPlanError {
    DdlPlanError::Admission(DdlAdmissionError::from_terror(
        &proto.generate_with_stack(message.into()),
    ))
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
        return Err(DdlPlanError::from(
            tidb_util::dbterror::ERR_CANT_DROP_FIELD_OR_KEY
                .generate_with_stack_by_args(&[(column.to_owned()).into()]),
        ));
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
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_UNSUPPORTED_PK_HANDLE,
            "Unsupported drop integer primary key",
        ));
    }
    // Go `ErrCantDropColWithAutoInc`.
    if col.get_flag() & u64::from(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT) != 0
        && !allow_remove_auto_inc
    {
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_CANT_DROP_COL_WITH_AUTO_INC,
            "Unsupported can't remove column with auto_increment when \
             @@tidb_allow_remove_auto_inc disabled",
        ));
    }
    check_column_referenced_by_partial_condition(info, col.name.original())?;
    Ok(true)
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
                &tidb_util::dbterror::ERR_DEPENDENT_BY_FUNCTIONAL_INDEX,
                format!(
                    "Column '{col_name}' has an expression index dependency and cannot be \
                     dropped or renamed"
                ),
            )
        } else {
            coded_refusal(
                &tidb_util::dbterror::ERR_DEPENDENT_BY_GENERATED_COLUMN,
                format!("Column '{col_name}' has a generated column dependency."),
            )
        });
    }
    if column_count == 1 {
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_CANT_REMOVE_ALL_FIELDS,
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
                &tidb_util::dbterror::ERR_CANT_DROP_COL_WITH_INDEX,
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
            && tidb_ddl_build::check_constraint::uses_column(&constraint, column)
    }) {
        let constraint = constraint.read();
        let error = tidb_ddl_build::check_constraint::column_dependency_error(
            constraint.name.original(),
            column,
        );
        return Err(coded_refusal(error.proto, error.message));
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
            &tidb_util::dbterror::ERR_DEPENDENT_BY_PARTITION_FUNCTIONAL,
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
                &tidb_util::dbterror::ERR_TTL_COLUMN_CANNOT_DROP,
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
                &tidb_util::dbterror::ERR_MODIFY_COLUMN_REFERENCED_BY_PARTIAL_CONDITION,
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

/// Go `checkTableInfoValidExtra`, its `ec` warnings recorded with the job's.
fn check_table_info_valid_extra(
    info: &TableInfo,
    warnings: &mut Vec<tidb_util::context::SqlWarn>,
) -> Result<(), DdlPlanError> {
    tidb_ddl_build::table_info_build::check_table_info_valid_extra(
        &mut |warning| {
            warnings.push(tidb_util::context::SqlWarn::new(DdlWarningLevel::Warning, warning));
        },
        info,
    )?;
    Ok(())
}

/// Go `checkTableInfoValid` (see [`tidb_ddl_build::table_info_build::check_table_info_valid`]).
fn check_table_info_valid(info: &TableInfo) -> Result<(), DdlPlanError> {
    Ok(tidb_ddl_build::table_info_build::check_table_info_valid(info)?)
}

/// Go `checkAddColumnTooManyColumns` (`ddl/column.go`).
fn check_add_column_too_many_columns(col_num: usize) -> Result<(), DdlPlanError> {
    if col_num > table_column_count_limit() {
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_TOO_MANY_FIELDS,
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
            &tidb_util::dbterror::ERR_TABLE_MUST_HAVE_COLUMNS,
            "A table must have at least 1 column",
        ));
    }
    Err(coded_refusal(
        &tidb_util::dbterror::ERR_CANT_REMOVE_ALL_FIELDS,
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
                    &tidb_util::dbterror::ERR_DROP_INDEX_NEEDED_IN_FOREIGN_KEY,
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
        &tidb_util::dbterror::ERR_CANT_DROP_FIELD_OR_KEY,
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
        [object] if default_schema.is_empty() => Err(DdlAdmissionError::from_terror(
            &tidb_error::plannererrors::ERR_NO_DB.generate_with_stack_by_args(&[]),
        )),
        [object] => Ok((default_schema.to_owned(), object.clone())),
        [schema, object] => Ok((schema.clone(), object.clone())),
        _ => Err(DdlAdmissionError::plain(format!(
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
    context: &DdlContext,
) -> Result<DdlStatement, DdlAdmissionError> {
    // LOCAL temporary metadata belongs to the issuing session and must never
    // be persisted by a cluster DDL job. The server routes it through the
    // session executor before lowering; retain this guard for direct callers.
    if create.temporary == tidb_ast::CreateTableTemporary::Local {
        return Err(DdlAdmissionError::plain(
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
    // (`tidb_tablecodec::cut_datum_by_prefix_len`), and the read path treats cut
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
/// The gate is not a taste judgement: [`tidb_cluster::cluster_catalog`]'s bounded
/// loader still refuses a prefix index, so publishing one here would write a
/// `TableInfo` that path cannot serve. The refusal names the unsupported
/// storage shape.
fn lower_create_index(
    create: &CreateIndexStmt,
    default_schema: &str,
    primary: bool,
    context: &DdlContext,
) -> Result<DdlStatement, DdlAdmissionError> {
    // go's `ADD PRIMARY KEY` lowers through the CreateIndex statement with
    // `primary = true`; the caller decides, since the AST's own
    // `CREATE INDEX` grammar has no PRIMARY spelling.
    let (schema, table) = split_name(&create.table, default_schema, "table")?;
    let (index, expressions) = tidb_ddl_build::index::index_template(create, primary)?;
    Ok(DdlStatement::CreateIndex {
        schema,
        table,
        if_not_exists: create.if_not_exists,
        expressions,
        condition: create.options.condition.clone(),
        context: DdlStatementContext(context.clone()),
        index: Box::new(index),
    })
}

fn lower_drop_index(
    drop: &DropIndexStmt,
    default_schema: &str,
) -> Result<DdlStatement, DdlAdmissionError> {
    let (schema, table) = split_name(&drop.table, default_schema, "table")?;
    Ok(DdlStatement::DropIndex {
        schema,
        table,
        index: drop.name.clone(),
        if_exists: drop.if_exists,
        hypo: drop.is_hypo,
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
    Admission(tidb_ddl_build::table_info_build::DdlAdmissionError),
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
            Self::Unsupported(reason) => formatter.write_str(reason),
            Self::Admission(error) => formatter.write_str(&error.reason),
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
    /// A refusal carrying an already classified driver-tier failure.
    pub fn from_mysql_error(error: tidb_error::mysql::MysqlError) -> Self {
        Self::Admission(DdlAdmissionError::from_mysql_error(error))
    }

    /// A refusal Go raises as a plain `errors.Errorf` (1105, no class).
    pub(crate) fn plain_refusal(message: impl Into<String>) -> Self {
        Self::Admission(DdlAdmissionError::plain(message))
    }

    /// The existing SQL contract is owned with the error, so direct execution
    /// and persisted action checkpoints cannot select different error numbers.
    pub(crate) fn mysql_error_code(&self) -> Option<u16> {
        use tidb_error::tidb::errcode;
        Some(match self {
            Self::Source(error) => error.to_sql_error().code,
            Self::Admission(error) => error.code,
            Self::Unsupported(_) => errcode::ErrUnknown,
            Self::Catalog(_)
            | Self::Encode(_)
            | Self::Mutations(_)
            | Self::GlobalIdExhausted { .. } => return None,
        })
    }

    /// The Go `*terror.Error` identity this refusal is raised as, `None` for
    /// the plain errors and the failures that are not Go errors at all.
    #[must_use]
    pub fn identity(&self) -> Option<tidb_error::terror::TerrorIdentity> {
        match self {
            Self::Source(error) => Some(error.identity()),
            Self::Admission(error) => error.identity,
            _ => None,
        }
    }

    /// Durable job error. Coded legacy errors retain their numeric envelope;
    /// uncoded errors follow Go `toTError`'s DDL/CodeUnknown fallback.
    /// Source RFC identities must be carried from their producers, not guessed
    /// from a MySQL number shared by several Go error classes.
    pub fn to_job_error(&self) -> tidb_error::terror::TerrorError {
        if let Self::Source(error) = self {
            return error.clone();
        }
        // The Go definition the refusal was raised as: the job's `err` JSON
        // carries its class, code and RFC identity (`{"class":8,"code":1062,
        // "rfccode":"kv:1062"}`), exactly as Go persists a `*terror.Error`.
        if let Some(identity) = self.identity() {
            return tidb_error::terror::TerrorError::synthesize(
                identity.class(),
                identity.code(),
                self.to_string(),
            );
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

    /// Go `proto.Equal(err)`: whether this is the Go error `proto` raises.
    #[must_use]
    pub fn equal(&self, proto: &TerrorError) -> bool {
        self.identity() == Some(proto.identity())
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
        identity: Some(error.identity()),
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
        warnings: Vec<tidb_util::context::SqlWarn>,
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
    /// A reorganization the owner must run before the job moves on; the
    /// step then commits nothing.
    reorg: Option<add_index::AddIndexReorg>,
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
            tidb_ddl_build::table_info_build::append_check_constraints(
                &mut candidate,
                &[tidb_ddl_build::check_constraint::CheckConstraintInput {
                    definition: (**definition).clone(),
                    in_column: None,
                }],
                tidb_ddl_build::check_constraint::CheckConstraintBuildMode::Alter,
                &context.0.metabuild_context(),
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
                return Err(DdlPlanError::Admission(DdlAdmissionError::from_terror(
                    &tidb_util::dbterror::ERR_CHECK_CONSTRAINT_DUP_NAME
                        .generate_with_stack_by_args(&[constraint.name.original().into()]),
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
                    DdlPlanError::Admission(DdlAdmissionError::from_terror(
                        &tidb_util::dbterror::ERR_CONSTRAINT_NOT_FOUND
                            .generate_with_stack_by_args(&[name.into()]),
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
                    DdlPlanError::Admission(DdlAdmissionError::from_terror(
                        &tidb_util::dbterror::ERR_CONSTRAINT_NOT_FOUND
                            .generate_with_stack_by_args(&[name.into()]),
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
    context: &dyn DdlSessionContext,
) -> Job {
    let mut job = session_job(context, action);
    job.schema_id = schema_id;
    job.table_id = table.id;
    job.schema_name = schema.go_to_lower().into();
    job.table_name = table.name.lowercase().to_owned().into();
    if action == ActionType::ACTION_ADD_CHECK_CONSTRAINT {
        job.priority = context.ddl_reorg_priority();
    }
    job
}

/// The job fields Go's executor sets on every statement's job literal
/// (`Version`, `Type`, `BinlogInfo`, `CDCWriteSource`, `SQLMode`) and those
/// `DoDDLJobWrapper` adds: `TraceInfo` and `setDDLJobQuery`'s query.
pub(crate) fn session_job(context: &dyn DdlSessionContext, action: ActionType) -> Job {
    let mut job = Job::default();
    job.version = get_job_ver_in_use();
    job.type_ = action;
    job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
    job.query = context.ddl_query().into();
    job.sql_mode = context.ddl_sql_mode();
    job.cdc_write_source = context.ddl_cdc_write_source();
    job.trace_info = Some(GoShared::new(TraceInfo {
        session_alias: context.ddl_session_alias().into(),
        trace_id: context.ddl_trace_id().to_vec().into(),
        connection_id: context.ddl_connection_id(),
    }));
    job
}

/// Schema acknowledgements belong to one owner run, like Go jobContext. A new
/// owner starts without acknowledgements and recovers durable schema state.
#[derive(Clone, Debug)]
pub struct DdlJobSchemaState {
    /// Go IsMDLEnabled, including the next-generation kernel override.
    pub mdl_enabled: bool,
    /// Last version acknowledged by this owner run.
    pub synced_version: Option<i64>,
    /// Version committed by this run that still needs acknowledgement.
    pub pending_version: Option<i64>,
    /// The reorganizations and checks this owner run finished since the
    /// job's last committed step, oldest first (Go `reorgCtx.doneCh`, and the
    /// checks Go runs inside the step: a multi-schema step that publishes
    /// every sub-job needs each one's); a new owner reruns them.
    pub reorg: Vec<add_index::ReorgOutcome>,
}

impl DdlJobSchemaState {
    /// Starts a fresh owner run in the selected source mode.
    pub fn new(mdl_enabled: bool) -> Self {
        Self {
            mdl_enabled,
            synced_version: None,
            pending_version: None,
            reorg: Vec::new(),
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
    /// A reorganization the owner runs in its own transactions before it
    /// plans the job again (Go `runReorgJob`).
    Reorg(add_index::AddIndexReorg),
}

pub mod add_column;
pub mod add_index;
pub mod alter_schema;
pub mod alter_table;
pub mod cached_table;
pub mod create_object;
pub mod create_schema;
pub mod create_table;
pub mod drop_column;
pub mod drop_index;
pub mod drop_object;
pub mod exchange_partition;
pub mod foreign_key;
pub mod modify_column;
pub mod multi_schema;
pub mod partition;
pub mod placement_policy;
pub mod rename_table;
pub mod reorganize_partition;
pub mod resource_group;
pub mod table_lock;
pub mod truncate;

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
            | ActionType::ACTION_RENAME_TABLE
            | ActionType::ACTION_RENAME_TABLES
            | ActionType::ACTION_DROP_SCHEMA
            | ActionType::ACTION_DROP_TABLE
            | ActionType::ACTION_DROP_VIEW
            | ActionType::ACTION_DROP_SEQUENCE
            | ActionType::ACTION_ADD_INDEX
            | ActionType::ACTION_ADD_PRIMARY_KEY
            | ActionType::ACTION_DROP_INDEX
            | ActionType::ACTION_DROP_PRIMARY_KEY
            | ActionType::ACTION_TRUNCATE_TABLE
            | ActionType::ACTION_ADD_COLUMN
            | ActionType::ACTION_DROP_COLUMN
            | ActionType::ACTION_MODIFY_COLUMN
            | ActionType::ACTION_MULTI_SCHEMA_CHANGE
            | ActionType::ACTION_ADD_FOREIGN_KEY
            | ActionType::ACTION_CREATE_VIEW
            | ActionType::ACTION_CREATE_SEQUENCE
            | ActionType::ACTION_ALTER_SEQUENCE
            | ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE
            | ActionType::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT
            | ActionType::ACTION_CREATE_PLACEMENT_POLICY
            | ActionType::ACTION_ALTER_PLACEMENT_POLICY
            | ActionType::ACTION_DROP_PLACEMENT_POLICY
            | ActionType::ACTION_CREATE_RESOURCE_GROUP
            | ActionType::ACTION_ALTER_RESOURCE_GROUP
            | ActionType::ACTION_DROP_RESOURCE_GROUP
            | ActionType::ACTION_LOCK_TABLE
            | ActionType::ACTION_UNLOCK_TABLE
            | ActionType::ACTION_ADD_TABLE_PARTITION
            | ActionType::ACTION_DROP_TABLE_PARTITION
            | ActionType::ACTION_TRUNCATE_TABLE_PARTITION
            | ActionType::ACTION_EXCHANGE_TABLE_PARTITION
            | ActionType::ACTION_REORGANIZE_PARTITION
            | ActionType::ACTION_ALTER_TABLE_PARTITIONING
            | ActionType::ACTION_REMOVE_PARTITIONING
    ) || alter_table::ALTER_TABLE_ACTIONS.contains(&action)
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
    let reorg_outcomes = schema_state.reorg.clone();
    plan_persisted_ddl_job_with(
        snapshot,
        ddl_job_id,
        start_ts,
        schema_state,
        load_error_count_limit,
        supports_persisted_ddl_job,
        |snapshot, catalog, active| {
            plan_persisted_action(snapshot, catalog, active, start_ts, &reorg_outcomes)
        },
    )
}

/// Go `runOneJobStep`'s dispatch to the action handler, shared by a job and
/// by a multi-schema change's sub-jobs (run as proxy jobs).
fn plan_persisted_action<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    reorg_outcomes: &[add_index::ReorgOutcome],
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    // A job of its own reads the latest outcome; a multi-schema change hands
    // each sub-job its own.
    let reorg_outcome = reorg_outcomes.last();
    let action = active.job.type_;
    match action {
        ActionType::ACTION_ADD_CHECK_CONSTRAINT
        | ActionType::ACTION_DROP_CHECK_CONSTRAINT
        | ActionType::ACTION_ALTER_CHECK_CONSTRAINT => {
            plan_persisted_check_constraint_job_step(catalog, active, start_ts)
        }
        ActionType::ACTION_CREATE_SCHEMA => plan_persisted_create_schema_job_step(catalog, active),
        ActionType::ACTION_CREATE_TABLE => {
            create_table::plan_persisted_create_table_job_step(snapshot, catalog, active, start_ts)
        }
        ActionType::ACTION_CREATE_TABLES => {
            plan_persisted_create_tables_job_step(catalog, active, start_ts)
        }
        ActionType::ACTION_CREATE_VIEW => {
            create_object::plan_persisted_create_view_job_step(snapshot, catalog, active, start_ts)
        }
        ActionType::ACTION_CREATE_SEQUENCE => {
            create_object::plan_persisted_create_sequence_job_step(catalog, active, start_ts)
        }
        ActionType::ACTION_ALTER_SEQUENCE => {
            create_object::plan_persisted_alter_sequence_job_step(catalog, active, start_ts)
        }
        ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE
        | ActionType::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT => {
            alter_schema::plan_persisted_alter_schema_job_step(snapshot, catalog, active)
        }
        ActionType::ACTION_CREATE_PLACEMENT_POLICY => {
            placement_policy::plan_persisted_create_placement_policy_job_step(
                snapshot, catalog, active,
            )
        }
        ActionType::ACTION_ALTER_PLACEMENT_POLICY => {
            placement_policy::plan_persisted_alter_placement_policy_job_step(
                snapshot, catalog, active,
            )
        }
        ActionType::ACTION_DROP_PLACEMENT_POLICY => {
            placement_policy::plan_persisted_drop_placement_policy_job_step(
                snapshot, catalog, active,
            )
        }
        ActionType::ACTION_CREATE_RESOURCE_GROUP => {
            resource_group::plan_persisted_create_resource_group_job_step(catalog, active)
        }
        ActionType::ACTION_ALTER_RESOURCE_GROUP => {
            resource_group::plan_persisted_alter_resource_group_job_step(catalog, active)
        }
        ActionType::ACTION_DROP_RESOURCE_GROUP => {
            resource_group::plan_persisted_drop_resource_group_job_step(catalog, active)
        }
        ActionType::ACTION_LOCK_TABLE => {
            table_lock::plan_persisted_lock_tables_job_step(catalog, active, start_ts)
        }
        ActionType::ACTION_UNLOCK_TABLE => {
            table_lock::plan_persisted_unlock_tables_job_step(catalog, active, start_ts)
        }
        ActionType::ACTION_ADD_TABLE_PARTITION => {
            partition::plan_persisted_add_table_partition_job_step(
                snapshot, catalog, active, start_ts,
            )
        }
        ActionType::ACTION_DROP_TABLE_PARTITION => {
            partition::plan_persisted_drop_table_partition_job_step(
                snapshot,
                catalog,
                active,
                start_ts,
                reorg_outcome,
            )
        }
        ActionType::ACTION_REORGANIZE_PARTITION
        | ActionType::ACTION_ALTER_TABLE_PARTITIONING
        | ActionType::ACTION_REMOVE_PARTITIONING => {
            reorganize_partition::plan_persisted_reorganize_partition_job_step(
                snapshot,
                catalog,
                active,
                start_ts,
                reorg_outcome,
            )
        }
        ActionType::ACTION_EXCHANGE_TABLE_PARTITION => {
            exchange_partition::plan_persisted_exchange_partition_job_step(
                snapshot,
                catalog,
                active,
                start_ts,
                reorg_outcome,
            )
        }
        ActionType::ACTION_TRUNCATE_TABLE_PARTITION => {
            partition::plan_persisted_truncate_table_partition_job_step(
                snapshot,
                catalog,
                active,
                start_ts,
                reorg_outcome,
            )
        }
        ActionType::ACTION_RENAME_TABLE | ActionType::ACTION_RENAME_TABLES => {
            rename_table::plan_persisted_rename_table_job_step(catalog, active, start_ts)
        }
        ActionType::ACTION_DROP_SCHEMA => {
            plan_persisted_drop_schema_job_step(snapshot, catalog, active)
        }
        ActionType::ACTION_DROP_TABLE
        | ActionType::ACTION_DROP_VIEW
        | ActionType::ACTION_DROP_SEQUENCE => {
            plan_persisted_drop_table_job_step(snapshot, catalog, active)
        }
        ActionType::ACTION_ADD_INDEX | ActionType::ACTION_ADD_PRIMARY_KEY => {
            add_index::plan_persisted_add_index_job_step(
                snapshot,
                catalog,
                active,
                start_ts,
                reorg_outcome,
            )
        }
        ActionType::ACTION_DROP_INDEX | ActionType::ACTION_DROP_PRIMARY_KEY => {
            drop_index::plan_persisted_drop_index_job_step(catalog, active, start_ts)
        }
        ActionType::ACTION_TRUNCATE_TABLE => {
            truncate::plan_persisted_truncate_table_job_step(snapshot, catalog, active, start_ts)
        }
        ActionType::ACTION_ADD_COLUMN => {
            add_column::plan_persisted_add_column_job_step(snapshot, catalog, active, start_ts)
        }
        ActionType::ACTION_DROP_COLUMN => {
            drop_column::plan_persisted_drop_column_job_step(catalog, active, start_ts)
        }
        ActionType::ACTION_MULTI_SCHEMA_CHANGE => {
            multi_schema::plan_persisted_multi_schema_job_step(
                snapshot,
                catalog,
                active,
                start_ts,
                reorg_outcomes,
            )
        }
        ActionType::ACTION_ADD_FOREIGN_KEY => foreign_key::plan_persisted_add_foreign_key_job_step(
            catalog,
            active,
            start_ts,
            reorg_outcome,
        ),
        ActionType::ACTION_MODIFY_COLUMN => modify_column::plan_persisted_modify_column_job_step(
            snapshot,
            catalog,
            active,
            start_ts,
            reorg_outcome,
        ),
        action if alter_table::ALTER_TABLE_ACTIONS.contains(&action) => {
            alter_table::plan_persisted_alter_table_job_step(snapshot, catalog, active, start_ts)
        }
        _ => unreachable!("supported action checked above"),
    }
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
            tidb_metadef::is_system_related_db(&active.job.schema_name.to_string().go_to_lower());
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
    let outcome = recover_ddl_action(|| {
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
    if let Ok(Ok(PersistedDdlActionStep {
        reorg: Some(reorg), ..
    })) = &outcome
    {
        return Ok(PersistedDdlJobPlan::Reorg(reorg.clone()));
    }
    let mut step = match outcome {
        Ok(Ok(step)) => step,
        Ok(Err(error)) => PersistedDdlActionStep {
            write: job_only_write(ddl_job_id, Vec::new()),
            update_raw_args: !was_rollingback && active.job.is_rollingback(),
            run_error: Some(error),
            reorg: None,
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
    // Go `finishDDLJob` -> `addDelRangeJob`: the keys the job left behind,
    // recorded before it moves to history.
    let mut mutations = crate::ddl_delete_range::add_del_range_job_mutations(
        snapshot,
        catalog,
        &active.job,
        start_ts,
    )?;
    append_history_ddl_job(catalog, snapshot, &active.job, encoded, &mut mutations)?;
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
            check_constraint_validation: validation,
            mdl_info_update: None,
            label_rules: Vec::new(),
            warnings: Vec::new(),
            placement_bundles: Vec::new(),
            affinity_groups: crate::ddl_affinity::AffinityGroupUpdate::default(),
            resource_group_update: None,
        },
        // Go runOneJobStep re-encodes decoded arguments after any successful
        // action, independently of whether the schema version changed.
        update_raw_args: true,
        run_error: None,
        reorg: None,
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
            check_constraint_validation: None,
            mdl_info_update: None,
            label_rules: Vec::new(),
            warnings: Vec::new(),
            placement_bundles: Vec::new(),
            affinity_groups: crate::ddl_affinity::AffinityGroupUpdate::default(),
            resource_group_update: None,
        },
        update_raw_args: true,
        run_error: None,
        reorg: None,
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
        let table_info = info_handle.read().clone();
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
    // Go `createTable` for each table of the batch.
    let mut affinity_groups = crate::ddl_affinity::AffinityGroupUpdate {
        create_error_context: Some(CREATE_TABLE_AFFINITY_ERROR),
        ..crate::ddl_affinity::AffinityGroupUpdate::default()
    };
    for info in &table_infos {
        affinity_groups.create_for(info).map_err(|error| {
            cancel_ddl_job(
                active,
                pd_step_error(Some(CREATE_TABLE_AFFINITY_ERROR), error),
            )
        })?;
    }
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
            check_constraint_validation: None,
            mdl_info_update: None,
            label_rules: Vec::new(),
            warnings: Vec::new(),
            placement_bundles: Vec::new(),
            affinity_groups,
            resource_group_update: None,
        },
        update_raw_args: true,
        run_error: None,
        reorg: None,
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
fn plan_persisted_drop_schema_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
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
    let mut affinity_groups = crate::ddl_affinity::AffinityGroupUpdate::default();
    let mut label_rules = LabelRuleSteps::new();
    let schema_version = schema_version + 1;
    if db_info.state == SchemaState::PUBLIC
        && tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::Relaxed)
    {
        // Go `checkDatabaseHasForeignKeyReferredInOwner`.
        let fk_check = tidb_model::get_drop_schema_args(&mut active.job)
            .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
            .is_some_and(|args| args.read().fk_check.get());
        drop_object::check_database_has_foreign_key_referred(catalog, &db_info, fk_check)
            .map_err(|error| cancel_ddl_job(active, error))?;
    }
    if db_info.state == SchemaState::PUBLIC {
        // Go `onDropSchema`: as the schema leaves the public state, every
        // table's and partition's label rule goes, under the job's schema
        // name.
        let tables = catalog
            .databases
            .iter()
            .find(|database| database.info.id == db_info.id)
            .map(|database| {
                database
                    .tables
                    .iter()
                    .map(LabelRuleTable::from_table_info)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        label_rules.push(
            label_rules::drop_schema_label_rules(&active.job.schema_name.to_string(), &tables),
            None,
        )?;
    }
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
        // Go `Mutator.DropDatabase` is `HClear(DB:<id>)` then
        // `HDel(DBs, DB:<id>)`: every field of the database's hash, its
        // tables' metadata and allocators, goes; the tables' rows are left
        // to the delete range.
        for (raw_key, _) in snapshot.scan_prefix(&key::database_metas_kv_prefix(db_info.id))? {
            mutations.push(BufferMutation::delete(raw_key)?);
        }
        mutations.push(BufferMutation::delete(key::database_kv_key(db_info.id))?);
        let tables: Vec<TableInfo> = catalog
            .databases
            .iter()
            .find(|database| database.info.id == db_info.id)
            .map(|database| {
                database
                    .tables
                    .iter()
                    .map(TableInfo::clone_like_go)
                    .collect()
            })
            .unwrap_or_default();
        // Go `onDropSchema`'s `batchDeleteTableAffinityGroups`.
        for table in &tables {
            affinity_groups.delete_for(table, None);
        }
        // Go splits the drop-schema events, 100 tables each (500 past
        // 100,000 tables), under sub-job ids 0, 1, ...
        if !tidb_metadef::is_mem_or_sys_db(&db_info.name.lowercase().go_to_lower()) {
            let tables_per_event = if tables.len() > 100_000 { 500 } else { 100 };
            let events: Vec<(i64, SchemaChangeEvent)> = tables
                .chunks(tables_per_event)
                .enumerate()
                .map(|(sub_job_id, chunk)| {
                    (
                        i64::try_from(sub_job_id).expect("drop-schema event count fits in i64"),
                        SchemaChangeEvent::drop_schema(&db_info, chunk),
                    )
                })
                .collect();
            append_schema_change_mutations(snapshot, catalog, ddl_job_id, &events, &mut mutations)?;
        }
        // Go `getIDs(tables)`: each table's id, then its partitions'.
        let dropped_ids: Vec<i64> = tables
            .iter()
            .flat_map(|table| std::iter::once(table.id).chain(add_index::partition_ids(table)))
            .collect();
        active
            .job
            .fill_finished_args(Some(GoShared::new(tidb_model::DropSchemaArgs {
                all_dropped_table_ids: GoField::new(GoSharedSlice::from_vec(dropped_ids)),
                ..tidb_model::DropSchemaArgs::default()
            })));
        active.job.finish_db_job(
            JobState::DONE,
            SchemaState::NONE,
            schema_version,
            Some(GoShared::new(db_info.clone())),
        );
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
            check_constraint_validation: None,
            mdl_info_update: None,
            label_rules: label_rules.steps,
            warnings: Vec::new(),
            placement_bundles: Vec::new(),
            affinity_groups,
            resource_group_update: None,
        },
        // Go `needUpdateRawArgs`: the finished arguments reach history.
        update_raw_args: active.job.is_done(),
        run_error: None,
        reorg: None,
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
fn plan_persisted_drop_table_job_step<S: MetaSnapshot>(
    snapshot: &mut S,
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
    let mut affinity_groups = crate::ddl_affinity::AffinityGroupUpdate::default();
    if table.state == SchemaState::PUBLIC && active.job.type_ == ActionType::ACTION_DROP_TABLE {
        // Go `checkDropTableHasForeignKeyReferredInOwner`.
        let args = tidb_model::get_drop_table_args(&mut active.job)
            .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
            .unwrap_or_else(|| GoShared::new(tidb_model::DropTableArgs::default()));
        let (ignored, fk_check) = {
            let args = args.read();
            let ignored: Vec<(String, String)> = args
                .identifiers
                .iter()
                .map(|ident| {
                    (
                        ident.schema.original().to_owned(),
                        ident.name.original().to_owned(),
                    )
                })
                .collect();
            (ignored, args.fk_check)
        };
        let schema_name = active.job.schema_name.to_string();
        let table_name = active.job.table_name.to_string();
        if let Some(referred) = crate::foreign_key_build::check_table_has_foreign_key_referred(
            catalog,
            &schema_name,
            &table_name,
            &ignored,
            fk_check && tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::Relaxed),
        ) {
            return Err(cancel_ddl_job(
                active,
                coded_refusal(
                    &tidb_util::dbterror::ERR_TRUNCATE_ILLEGAL_FOREIGN_KEY,
                    format!(
                        "Cannot truncate a table referenced in a foreign key constraint (`{}`.`{}` CONSTRAINT `{}`)",
                        referred.child_schema.original(),
                        referred.child_table.original(),
                        referred.child_fk_name.original()
                    ),
                ),
            ));
        }
    }
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
        let database_id = database.info.id;
        mutations.push(BufferMutation::delete(key::table_kv_key(
            database_id,
            table.id,
        ))?);
        // Go `DropTableOrView` plus `GetAutoIDAccessors(...).Del()` remove
        // the table's allocator fields, and `DropSequence` also its value.
        // Each is deleted only if this snapshot observed it, as `HDel` does.
        let mut allocator_keys = vec![
            key::auto_table_id_kv_key(database_id, table.id),
            key::auto_increment_id_kv_key(database_id, table.id),
            key::auto_random_table_id_kv_key(database_id, table.id),
        ];
        if table.is_sequence() {
            allocator_keys.push(sequence_value_kv_key(database_id, table.id));
        }
        for allocator in allocator_keys {
            if snapshot.get(&allocator)?.is_some() {
                mutations.push(BufferMutation::delete(allocator)?);
            }
        }
        // Go `asyncNotifyEvent(NewDropTableEvent)` for a table.
        if !table.is_sequence()
            && !table.is_view()
            && !tidb_metadef::is_mem_or_sys_db(&database.info.name.lowercase().go_to_lower())
        {
            append_schema_change_mutations(
                snapshot,
                catalog,
                ddl_job_id,
                &[(-1, SchemaChangeEvent::drop_table(table.clone_like_go()))],
                &mut mutations,
            )?;
        }
        // Go `onDropTableOrView`'s `deleteTableAffinityGroupsInPD`.
        affinity_groups.delete_for(&table, None);
        let old_partition_ids = add_index::partition_ids(&table);
        let codec = tidb_ddl_label::CodecV1;
        let schema_name = active.job.schema_name.to_string();
        let mut rule_ids: Vec<String> = table
            .partition
            .as_ref()
            .map(|partition| {
                partition
                    .read()
                    .definitions
                    .snapshot()
                    .iter()
                    .map(|definition| {
                        tidb_ddl_label::new_rule_id(
                            &codec,
                            &schema_name,
                            table.name.lowercase(),
                            definition.name.lowercase(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        rule_ids.push(tidb_ddl_label::new_rule_id(
            &codec,
            &schema_name,
            table.name.lowercase(),
            "",
        ));
        active.job.finish_table_job(
            JobState::DONE,
            SchemaState::NONE,
            schema_version,
            Some(GoShared::new(table.clone())),
        );
        active
            .job
            .fill_finished_args(Some(GoShared::new(tidb_model::DropTableArgs {
                start_key: tidb_model::GoByteSlice(GoSharedSlice::from_vec(
                    tidb_codec::table_key::gen_table_prefix(active.job.table_id),
                )),
                old_partition_ids,
                old_rule_ids: rule_ids,
                ..tidb_model::DropTableArgs::default()
            })));
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
            check_constraint_validation: None,
            mdl_info_update: None,
            label_rules: Vec::new(),
            warnings: Vec::new(),
            placement_bundles: Vec::new(),
            affinity_groups,
            resource_group_update: None,
        },
        // Go `needUpdateRawArgs`: the finished arguments reach history.
        update_raw_args: active.job.is_done(),
        run_error: None,
        reorg: None,
    })
}
// Action handlers select cancellation; the shared worker records this fresh
// error, discards action writes, and finalizes the job in the same transaction.
/// Go `resourcegroup.NewGroupFromOptions(groupInfo.Name.L, settings)`: the
/// group as the resource manager takes it, refusing invalid settings.
fn resource_group_proto(
    group: &tidb_model::ResourceGroupInfo,
) -> Result<tidb_proto::resource_manager::ResourceGroup, DdlPlanError> {
    tidb_ddl_resourcegroup::new_group_from_options(
        group.name.lowercase(),
        group.settings.as_deref(),
    )
    .map_err(|error| {
        DdlPlanError::Admission(DdlAdmissionError::plain(
            error.to_string(),
        ))
    })
}

/// Go `Mutator.AddResourceGroup`'s value: the group's JSON behind the magic
/// byte.
fn serialize_resource_group(
    group: &tidb_model::ResourceGroupInfo,
) -> Result<Vec<u8>, DdlPlanError> {
    serde_json::to_vec(group)
        .map(|json| value::attach_magic_byte(&json))
        .map_err(|error| DdlPlanError::Encode(error.to_string()))
}

/// Go `checkResourceGroupExist`: the stored group of the job's ID.
fn stored_resource_group(
    catalog: &ClusterCatalog,
    group_id: i64,
) -> Result<&tidb_model::ResourceGroupInfo, DdlPlanError> {
    catalog
        .resource_groups
        .iter()
        .find(|group| group.id == group_id)
        .ok_or_else(|| {
            coded_refusal(
                &tidb_error::catalog::infoschema::ERR_RESOURCE_GROUP_NOT_EXISTS,
                format!("Unknown resource group '(Group ID {group_id})'"),
            )
        })
}

/// Go's wrapping of a `createTable` affinity failure.
const CREATE_TABLE_AFFINITY_ERROR: &str = "failed to create table affinity groups in PD";

/// A PD-side step of a job that failed -- an affinity group definition it
/// cannot build, or a label-rule read -- as Go's `errors.Errorf` /
/// `errors.Wrapf`, wrapped by its caller when it wraps one.
fn pd_step_error(context: Option<&str>, error: String) -> DdlPlanError {
    DdlPlanError::Admission(DdlAdmissionError::plain(match context {
            Some(context) => format!("{context}: {error}"),
            None => error,
        },
    ))
}

/// Go `doDDLJob2` outside a multi-schema change: a built job goes through
/// `GenGIDAndInsertJobsWithRetry`'s admission.
fn submit_built_job<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    built: Option<add_index::AddIndexSubmission>,
    start_ts: u64,
    upgrading: bool,
    min_job_id: i64,
) -> Result<Option<add_index::AddIndexSubmission>, DdlPlanError> {
    match built {
        Some(add_index::AddIndexSubmission::Job(spec, warnings)) => {
            let mut specs = [*spec];
            crate::ddl_job_submit::prepare_submit_batch(
                snapshot, catalog, &mut specs, start_ts, upgrading, min_job_id,
            )?;
            let [spec] = specs;
            Ok(Some(add_index::AddIndexSubmission::Job(
                Box::new(spec),
                warnings,
            )))
        }
        other => Ok(other),
    }
}

/// Go `job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible`: a
/// multi-schema sub-job that has not reached its revertible point yet.
fn is_revertible_sub_job(job: &Job) -> bool {
    job.multi_schema_info
        .as_ref()
        .is_some_and(|info| info.read().revertible)
}

/// Go `checkAndMarkNonRevertible`.
fn check_and_mark_non_revertible(job: &mut Job) {
    if is_revertible_sub_job(job) {
        job.mark_non_revertible();
    }
}

/// Go `MultiSchemaInfo.SkipVersion`: a sub-job stepped after the one that
/// published this step's schema version.
fn skips_version(job: &Job) -> bool {
    job.multi_schema_info
        .as_ref()
        .is_some_and(|info| info.read().skip_version)
}

/// Go `asyncNotifyEvent`'s sub-job id: a multi-schema sub-job's sequence,
/// else `noSubJob`.
fn notifier_sub_job_id(job: &Job) -> i64 {
    job.multi_schema_info
        .as_ref()
        .map_or(-1, |info| i64::from(info.read().seq))
}

fn cancel_ddl_job(
    active: &mut crate::ddl_job_table::ActiveDdlJob,
    error: impl Into<DdlPlanError>,
) -> DdlPlanError {
    active.job.state = JobState::CANCELLED;
    error.into()
}

/// A step's write that changes `mutations` and nothing a schema version
/// publishes.
#[must_use]
pub fn job_only_write(ddl_job_id: i64, mutations: Vec<BufferMutation>) -> DdlWrite {
    DdlWrite {
        ddl_job_id,
        mutations,
        schema_version: 0,
        diff: SchemaDiff::default(),
        created_id: None,
        check_constraint_validation: None,
        mdl_info_update: None,
        label_rules: Vec::new(),
        warnings: Vec::new(),
        placement_bundles: Vec::new(),
        affinity_groups: crate::ddl_affinity::AffinityGroupUpdate::default(),
        resource_group_update: None,
    }
}

fn job_control_step(job: &Job) -> PersistedDdlActionStep {
    PersistedDdlActionStep {
        write: job_only_write(job.id, Vec::new()),
        update_raw_args: false,
        run_error: None,
        reorg: None,
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
        | ActionType::ACTION_CREATE_TABLES
        | ActionType::ACTION_CREATE_VIEW
        | ActionType::ACTION_CREATE_SEQUENCE
        | ActionType::ACTION_ALTER_SEQUENCE
        | ActionType::ACTION_CREATE_PLACEMENT_POLICY
        | ActionType::ACTION_ALTER_PLACEMENT_POLICY
        | ActionType::ACTION_DROP_PLACEMENT_POLICY
        | ActionType::ACTION_CREATE_RESOURCE_GROUP
        | ActionType::ACTION_ALTER_RESOURCE_GROUP
        | ActionType::ACTION_DROP_RESOURCE_GROUP
        | ActionType::ACTION_LOCK_TABLE
        | ActionType::ACTION_UNLOCK_TABLE => true,
        // `cancelOnlyNotHandledJob(job, model.StateNone)`.
        ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE
        | ActionType::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT => {
            active.job.schema_state == SchemaState::NONE
        }
        ActionType::ACTION_RENAME_TABLE | ActionType::ACTION_RENAME_TABLES => {
            active.job.schema_state == SchemaState::NONE
        }
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
        ActionType::ACTION_DROP_TABLE
        | ActionType::ACTION_DROP_VIEW
        | ActionType::ACTION_DROP_SEQUENCE => {
            let table = checked_job_table(catalog, active, false)?;
            table.state == SchemaState::PUBLIC
        }
        ActionType::ACTION_ADD_CHECK_CONSTRAINT
        | ActionType::ACTION_DROP_CHECK_CONSTRAINT
        | ActionType::ACTION_ALTER_CHECK_CONSTRAINT => {
            return plan_persisted_check_constraint_job_step(catalog, active, start_ts);
        }
        ActionType::ACTION_ADD_INDEX | ActionType::ACTION_ADD_PRIMARY_KEY => {
            return add_index::plan_cancel_add_index(catalog, active, start_ts);
        }
        ActionType::ACTION_DROP_INDEX | ActionType::ACTION_DROP_PRIMARY_KEY => {
            return drop_index::plan_cancel_drop_index(catalog, active);
        }
        ActionType::ACTION_ADD_COLUMN => {
            return add_column::plan_cancel_add_column(catalog, active, start_ts);
        }
        ActionType::ACTION_DROP_COLUMN => {
            return drop_column::plan_cancel_drop_column(catalog, active);
        }
        ActionType::ACTION_MODIFY_COLUMN => {
            return modify_column::plan_cancel_modify_column(catalog, active);
        }
        ActionType::ACTION_ADD_FOREIGN_KEY => {
            return foreign_key::plan_cancel_add_foreign_key(active);
        }
        ActionType::ACTION_MULTI_SCHEMA_CHANGE => {
            return multi_schema::plan_cancel_multi_schema(active);
        }
        ActionType::ACTION_ADD_TABLE_PARTITION
        | ActionType::ACTION_DROP_TABLE_PARTITION
        | ActionType::ACTION_TRUNCATE_TABLE_PARTITION => {
            return partition::plan_cancel_partition(catalog, active, start_ts);
        }
        ActionType::ACTION_EXCHANGE_TABLE_PARTITION => {
            return exchange_partition::plan_cancel_exchange_partition(catalog, active, start_ts);
        }
        ActionType::ACTION_REORGANIZE_PARTITION
        | ActionType::ACTION_ALTER_TABLE_PARTITIONING
        | ActionType::ACTION_REMOVE_PARTITIONING => {
            return reorganize_partition::plan_cancel_reorganize_partition(
                catalog, active, start_ts,
            );
        }
        action if alter_table::ALTER_TABLE_ACTIONS.contains(&action) => {
            return alter_table::plan_cancel_alter_table(catalog, active);
        }
        // Go `rollingbackTruncateTable`: `cancelOnlyNotHandledJob` at
        // `StateNone`, after `GetTableInfoAndCancelFaultJob`.
        ActionType::ACTION_TRUNCATE_TABLE => {
            checked_job_table(catalog, active, false)?;
            active.job.schema_state == SchemaState::NONE
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
    if error.equal(&tidb_util::dbterror::ERR_CANCELLED_DDL_JOB) {
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
pub(crate) fn recover_ddl_action<T>(action: impl FnOnce() -> T) -> Result<T, ()> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(action)).map_err(|panic| {
        tidb_util::panic_metrics::PANIC_TOTAL
            .with_label_values(&["ddl-worker"])
            .inc();
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("non-string panic payload");
        // Go `tidbutil.Recover(metrics.LabelDDLWorker, fmt.Sprintf("%s
        // runOneJobStep", w), ...)`. Rust runs jobs on no numbered worker,
        // so `funcInfo` names the step without Go's `worker N, tp T` prefix.
        tidb_util::logutil::bg_logger().error(
            "panic in the recoverable goroutine",
            &[
                tidb_log::Field::new("label", tidb_log::Value::Str("ddl-worker".to_owned())),
                tidb_log::Field::new("funcInfo", tidb_log::Value::Str("runOneJobStep".to_owned())),
                tidb_log::Field::new("r", tidb_log::Value::Str(message.to_owned())),
                tidb_log::Field::new(
                    "stack",
                    tidb_log::Value::Str(std::backtrace::Backtrace::force_capture().to_string()),
                ),
            ],
        );
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
            ) && error.equal(&tidb_util::dbterror::ERR_CHECK_CONSTRAINT_IS_VIOLATED)
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
    /// Existing-row validation owed before an enforced CHECK becomes public.
    pub check_constraint_validation: Option<CheckConstraintValidation>,
    /// The `mysql.tidb_mdl_info` row that must be replaced atomically with
    /// this schema phase before the owner waits for acknowledgements.
    pub mdl_info_update: Option<MdlInfoUpdate>,
    /// The region-label rule writes the job step makes (Go
    /// `infosync.PutLabelRule` / `UpdateLabelRules`), in order, delivered
    /// to the node's label manager before the commit and failing the change.
    pub label_rules: Vec<LabelRuleStep>,
    /// The warnings the change raises, if any.
    ///
    /// Go carries these as `job.Warning` (and, for the same adjustments made
    /// at admission time, `StmtCtx.AppendWarning`); the client reads them
    /// back with `SHOW WARNINGS`. A change that silently did something other
    /// than what was written would otherwise look like it did exactly what
    /// was written.
    pub warnings: Vec<tidb_util::context::SqlWarn>,
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
    /// The PD affinity groups the change creates and deletes (Go
    /// `createTableAffinityGroupsInPD`, inside the job before the version is
    /// published, failing it when PD refuses; `deleteTableAffinityGroupsInPD`,
    /// best-effort cleanup).
    pub affinity_groups: crate::ddl_affinity::AffinityGroupUpdate,
    /// The resource-manager write a resource-group job makes before its
    /// version is published (Go `infosync.AddResourceGroup` and its
    /// siblings), failing the change when it fails.
    pub resource_group_update: Option<ResourceGroupUpdate>,
}

/// Go's `infosync` resource-group writes from the resource-group jobs.
#[derive(Clone, Debug)]
pub enum ResourceGroupUpdate {
    /// `onCreateResourceGroup`'s `infosync.AddResourceGroup`.
    Add(Box<tidb_proto::resource_manager::ResourceGroup>),
    /// `onAlterResourceGroup`'s `infosync.ModifyResourceGroup`.
    Modify(Box<tidb_proto::resource_manager::ResourceGroup>),
    /// `onDropResourceGroup`'s `infosync.DeleteResourceGroup`.
    Delete(String),
}

impl ResourceGroupUpdate {
    /// Makes the write; the error is the job's.
    pub fn apply(&self) -> Result<(), String> {
        match self {
            Self::Add(group) => {
                match tidb_domain::resource_manager_client::add_resource_group((**group).clone()) {
                    Ok(()) => Ok(()),
                    // Go: TiDB adds the default group when it bootstraps, so a
                    // keyspace-mode TiDB skips that one's "already exists".
                    Err(error)
                        if error.contains("already exists")
                            && group.name == tidb_model::DEFAULT_RESOURCE_GROUP_NAME =>
                    {
                        Ok(())
                    }
                    Err(error) => Err(error),
                }
            }
            Self::Modify(group) => {
                tidb_domain::resource_manager_client::modify_resource_group((**group).clone())
            }
            Self::Delete(name) => tidb_domain::resource_manager_client::delete_resource_group(name),
        }
    }
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
    ) -> Result<tidb_cluster::system_row_write::RowValues, DdlPlanError> {
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
        let mut values = tidb_cluster::system_row_write::RowValues::new();
        values.insert(column_id("job_id")?, Datum::Int(ddl_job_id));
        values.insert(column_id("version")?, Datum::Int(schema_version));
        values.insert(
            column_id("table_ids")?,
            Datum::Bytes(self.table_ids.as_bytes().to_vec()),
        );
        // Go omits `owner_id` from a system database's `REPLACE INTO`, so
        // the column takes its `DEFAULT ''`.
        let owner_id = if self.omit_owner_id { "" } else { owner_id };
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
        mutations: &mut Vec<BufferMutation>,
    ) -> Result<(), DdlPlanError> {
        let values = self.row_values(ddl_job_id, schema_version, owner_id)?;
        mutations.extend(
            tidb_cluster::system_row_write::replace_unindexed_clustered_row(&self.table, &values)
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
        use tidb_cluster::mysql_system_tables::{scan_system_table_prefixed, SystemRow, SystemTableView};
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
                    tidb_cluster::system_row_write::delete_unindexed_clustered_row(
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
pub use tidb_ddl_build::label_rules::ExchangePartitionLabelSwap;
use tidb_ddl_build::label_rules::{self, LabelRuleTable, LabelRuleWrite};

/// Go's wrapping of a failed label-rule write.
const LABEL_RULES_NOTIFY_ERROR: &str = "failed to notify PD the label rules";

/// One region-label rule write of a job step, with the context Go wraps its
/// failure in (none when Go returns the error as is).
#[derive(Clone, Debug)]
pub struct LabelRuleStep {
    /// The call.
    pub write: LabelRuleWrite,
    /// Go's `errors.Wrapf` context for a failed call.
    pub error_context: Option<&'static str>,
}

/// The label-rule writes of one job step, staged over the node's manager so
/// the step's later reads see its earlier writes, as Go's step, which writes
/// PD call by call, does.
pub(crate) struct LabelRuleSteps {
    staged: tidb_domain::label_manager::StagedLabelRules,
    steps: Vec<LabelRuleStep>,
}

impl LabelRuleSteps {
    fn new() -> Self {
        Self {
            staged: tidb_domain::label_manager::StagedLabelRules::new(
                tidb_domain::label_manager::label_rule_manager(),
            ),
            steps: Vec::new(),
        }
    }

    /// The rules as the step sees them.
    fn manager(&self) -> &dyn tidb_domain::label_manager::LabelRuleManager {
        &self.staged
    }

    fn push(
        &mut self,
        write: LabelRuleWrite,
        error_context: Option<&'static str>,
    ) -> Result<(), DdlPlanError> {
        write
            .apply(&self.staged)
            .map_err(|error| pd_step_error(error_context, error))?;
        self.steps.push(LabelRuleStep {
            write,
            error_context,
        });
        Ok(())
    }

    /// A partition job's label step, from the table before and after it.
    /// Go's `alterTableLabelRule` puts with "failed to notify PD label rule";
    /// `dropLabelRules` and the truncate patch wrap with the plural context.
    fn partition(
        &mut self,
        schema: &str,
        before: &TableInfo,
        after: &TableInfo,
        job: label_rules::PartitionLabelJob,
    ) -> Result<(), DdlPlanError> {
        let writes = label_rules::partition_label_writes(
            self.manager(),
            &schema.go_to_lower(),
            &LabelRuleTable::from_table_info(before),
            &LabelRuleTable::from_table_info(after),
            job,
        )
        .map_err(|error| pd_step_error(None, error))?;
        for write in writes {
            let context = match write {
                LabelRuleWrite::Put(_) => "failed to notify PD label rule",
                LabelRuleWrite::Patch(_) => LABEL_RULES_NOTIFY_ERROR,
            };
            self.push(write, Some(context))?;
        }
        Ok(())
    }
}

pub use tidb_ddl_build::session_pool::ForeignKeyCheck;


/// The candidate table shape whose enforced CHECK must hold for every
/// existing row before its metadata can be published.
#[derive(Clone, Debug)]
pub struct CheckConstraintValidation {
    /// Candidate metadata, including the newly enforced constraint.
    pub table: Box<TableInfo>,
    /// The constraint whose violation Go reports as 3819.
    pub constraint_name: String,
}

/// Go `AlterSchema`: `ModifySchemaCharsetAndCollate`, then
/// `ModifySchemaDefaultPlacement`, then `ModifySchemaSetTiFlashReplica`,
/// each its own job; one ALTER DATABASE per job, in that order. `None` when
/// the statement runs at most one of them, or when `checkMultiSchemaSpecs`
/// refuses it before any.
#[must_use]
pub fn split_alter_database(
    name: &Option<String>,
    options: &[DatabaseOption],
) -> Option<Vec<DdlStmt>> {
    let part = |keep: fn(&DatabaseOption) -> bool| {
        let options: Vec<DatabaseOption> =
            options.iter().filter(|option| keep(option)).cloned().collect();
        (!options.is_empty()).then(|| DdlStmt::AlterDatabase {
            name: name.clone(),
            options,
        })
    };
    let tiflash = |option: &DatabaseOption| matches!(option, DatabaseOption::SetTiFlashReplica { .. });
    if options.iter().filter(|option| tiflash(option)).count() > 1 {
        return None;
    }
    let parts: Vec<DdlStmt> = [
        part(|option| {
            matches!(
                option,
                DatabaseOption::CharacterSet(_) | DatabaseOption::Collate(_)
            )
        }),
        part(|option| matches!(option, DatabaseOption::PlacementPolicy(_))),
        part(tiflash),
    ]
    .into_iter()
    .flatten()
    .collect();
    (parts.len() > 1).then_some(parts)
}

/// Whether a table option is one of Go's TTL family.
/// Go `AlterTable`'s `ast.AlterTableOption` loop over one specification's
/// options, as the single-option `ALTER TABLE` statements it runs, in order:
/// each option is its own job; every CHARSET/COLLATE option is handled once,
/// at the first, and likewise every TTL option; ENGINE and ROW_FORMAT do
/// nothing; the ENGINE_ATTRIBUTE / STORAGE_CLASS options become one job after
/// the loop, and the last PLACEMENT POLICY another after that. `None` when the
/// statement is not one multi-option table-option specification.
#[must_use]
pub fn split_table_option_alter(
    alter: &tidb_ast::AlterTableStmt,
) -> Option<Vec<tidb_ast::AlterTableStmt>> {
    use tidb_ast::TableOption;
    let [tidb_ast::AlterTableAction::SetTableOptions { options }] = alter.actions.as_slice() else {
        return None;
    };
    if options.len() < 2 {
        return None;
    }
    let single = |options: Vec<TableOption>| tidb_ast::AlterTableStmt {
        name: alter.name.clone(),
        actions: vec![tidb_ast::AlterTableAction::SetTableOptions { options }],
    };
    let mut statements = Vec::new();
    let (mut charset_done, mut ttl_done) = (false, false);
    let mut storage_class = Vec::new();
    let mut placement = None;
    for option in options {
        match option {
            TableOption::CharacterSet(_) | TableOption::Collate(_) => {
                if !charset_done {
                    charset_done = true;
                    statements.push(single(
                        options
                            .iter()
                            .filter(|option| {
                                matches!(
                                    option,
                                    TableOption::CharacterSet(_) | TableOption::Collate(_)
                                )
                            })
                            .cloned()
                            .collect(),
                    ));
                }
            }
            option if is_ttl_table_option(option) => {
                if !ttl_done {
                    ttl_done = true;
                    statements.push(single(
                        options
                            .iter()
                            .filter(|option| is_ttl_table_option(option))
                            .cloned()
                            .collect(),
                    ));
                }
            }
            TableOption::Engine(_) | TableOption::RowFormat(_) => {}
            TableOption::EngineAttribute(_) | TableOption::StorageClass(_) => {
                storage_class.push(option.clone());
            }
            TableOption::PlacementPolicy(_) => placement = Some(option.clone()),
            other => statements.push(single(vec![other.clone()])),
        }
    }
    if !storage_class.is_empty() {
        statements.push(single(storage_class));
    }
    if let Some(placement) = placement {
        statements.push(single(vec![placement]));
    }
    if statements.is_empty() {
        // Only no-op options: one of them still resolves the table, as Go
        // does before its loop.
        statements.push(single(vec![options[0].clone()]));
    }
    Some(statements)
}

/// The table options Go's `AlterTableOption` loop accepts with an empty case.
fn is_noop_table_option(option: &tidb_ast::TableOption) -> bool {
    matches!(
        option,
        tidb_ast::TableOption::Engine(_) | tidb_ast::TableOption::RowFormat(_)
    )
}

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
    info: tidb_table::sequence::SequenceInfo,
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
    info: tidb_table::sequence::SequenceInfo,
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
) -> tidb_table::sequence::SequenceInfo {
    tidb_table::sequence::SequenceInfo {
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


/// The routing spec of a stored partitioned table, as this node's loader
/// builds it from Go's `PartitionInfo`.
fn stored_partition_spec(
    stored: &TableInfo,
) -> Result<tidb_table::partition_routing::PartitionSpec, DdlPlanError> {
    let partition = stored.partition.as_ref().ok_or_else(|| {
        DdlPlanError::Admission(DdlAdmissionError::from_terror(
            &tidb_util::dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED
                .generate_with_stack_by_args(&[]),
        ))
    })?;
    let columns = stored.columns.iter_deref().collect::<Vec<_>>();
    tidb_cluster::table_from_meta::partition_spec_for(&columns, &partition.read())
        .map_err(|error| DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error)))
}

/// A definition of the shared partition implementation under the physical
/// id `id`, as Go's model stores it.
fn model_partition_definition(
    definition: tidb_table::partition_routing::PartitionDef,
    id: i64,
) -> tidb_model::partition::PartitionDefinition {
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
}

/// Go `AddTablePartitions` / `DropTablePartition`'s checks over the stored
/// table: the partitioning the change leaves and Go's query rewrite (the
/// FIRST / LAST PARTITION sugar); its notes are left on `context`.
fn apply_partition_change(
    stored: &TableInfo,
    action: &tidb_ast::AlterPartitionAction,
    context: &dyn DdlSessionContext,
) -> Result<tidb_ddl_build::partition_ddl::PartitionChange, DdlPlanError> {
    let partitioned = tidb_ddl_build::partition_ddl::PartitionedTable::new(stored)
        .map_err(|error| DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error)))?;
    tidb_ddl_build::partition_ddl::plan_partition_change(
        &partitioned,
        action,
        &context.metabuild_context(),
    )
    .map_err(|error| DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error)))
}

fn exchange_refusal(proto: &'static TerrorError, message: impl Into<String>) -> DdlPlanError {
    DdlPlanError::Admission(DdlAdmissionError::from_terror(
        &proto.generate_with_stack(message.into()),
    ))
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
        return Err(exchange_refusal(
            &tidb_util::dbterror::ERR_CHECK_NO_SUCH_TABLE,
            "Can't open table",
        ));
    }
    if partitioned.partition.is_none() {
        return Err(exchange_refusal(
            &tidb_util::dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED,
            "Partition management on a not partitioned table is not possible",
        ));
    }
    if standalone.partition.is_some() {
        return Err(exchange_refusal(
            &tidb_util::dbterror::ERR_PARTITION_EXCHANGE_PART_TABLE,
            format!(
                "Table to exchange with partition is partitioned: '{}'",
                standalone.name
            ),
        ));
    }
    if standalone.affinity.is_some() || partitioned.affinity.is_some() {
        return Err(exchange_refusal(
            &tidb_util::dbterror::ERR_GENERAL_UNSUPPORTED_DDL,
            "Unsupported EXCHANGE PARTITION of a table with AFFINITY option",
        ));
    }
    if !standalone.foreign_keys.is_empty() {
        return Err(exchange_refusal(
            &tidb_util::dbterror::ERR_PARTITION_EXCHANGE_FOREIGN_KEY,
            format!(
                "Table to exchange with partition has foreign key references: '{}'",
                standalone.name
            ),
        ));
    }
    if standalone.temp_table_type != tidb_model::TempTableType::NONE {
        return Err(exchange_refusal(
            &tidb_util::dbterror::ERR_PARTITION_EXCHANGE_TEMP_TABLE,
            format!(
                "Table to exchange with partition is temporary: '{}'",
                standalone.name
            ),
        ));
    }
    let different_metadata = || exchange_refusal(
        &tidb_util::dbterror::ERR_TABLES_DIFFERENT_METADATA,
        "Tables have different definitions",
    );
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
                &tidb_util::dbterror::ERR_UNSUPPORTED_ON_GENERATED_COLUMN,
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
                &tidb_util::dbterror::ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION,
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
                &tidb_util::dbterror::ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION,
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
                &tidb_util::dbterror::ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION,
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

/// The statements this node still answers without a DDL job: the ALTER
/// forms Go accepts and changes nothing for. Every other schema change is
/// a persisted job (`plan_persisted_ddl_job_step`).
pub fn plan_ddl<S: MetaSnapshot>(
    snapshot: &mut S,
    statement: &DdlStatement,
) -> Result<DdlPlan, DdlPlanError> {
    let catalog = load_cluster_catalog(snapshot)?;
    // Go `executor.AlterTable` refuses a view or a sequence before any
    // specification is looked at, whichever specification the statement carries.
    if let Some((schema, table)) = alter_table_target(statement) {
        if let Some(stored) = find_database(&catalog, schema).and_then(|db| find_table(db, table)) {
            refuse_non_base_table(stored, schema, table)?;
        }
    }
    match statement {
        DdlStatement::IgnoredCheckConstraint { schema, table } => {
            locate_table(&catalog, schema, table)?;
            Ok(already(format!(
                "CHECK constraint on `{schema}`.`{table}` is discarded while tidb_enable_check_constraint is off"
            )))
        }
        DdlStatement::AcceptedNoOp {
            schema,
            table,
            warning,
        } => {
            // Go `AlterTable` resolves the table before any specification
            // runs, so a no-op ALTER of a missing table is 1146.
            if !table.is_empty() {
                locate_table(&catalog, schema, table)?;
            }
            let warnings = warning
                .as_ref()
                .map(|(code, message)| {
                    vec![tidb_util::context::SqlWarn::with_code(
                        crate::real_tikv_ddl::DdlWarningLevel::Warning,
                        *code,
                        message.clone(),
                    )]
                })
                .unwrap_or_default();
            Ok(already_with_warnings(
                format!("ALTER TABLE `{schema}`.`{table}` changed nothing"),
                warnings,
            ))
        }
        DdlStatement::IgnoredTableOption {
            schema,
            table,
            option,
        } => {
            // Go still resolves the table first, so a missing one is an
            // error rather than a silent success.
            locate_table(&catalog, schema, table)?;
            Ok(already(format!(
                "table option {option} on `{schema}`.`{table}` is accepted and ignored"
            )))
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
                .map(|message| {
                    vec![tidb_util::context::SqlWarn::with_code(
                        DdlWarningLevel::Warning,
                        1105,
                        message,
                    )]
                })
                .unwrap_or_default();
            Ok(DdlPlan::AlreadySatisfied {
                detail: format!("ORDER BY on `{schema}`.`{table}` changes nothing"),
                warnings,
            })
        }
        _ => Err(DdlPlanError::Encode(
            "this schema change must execute through mysql.tidb_ddl_job".to_owned(),
        )),
    }
}

/// Go `ddl.AddHistoryDDLJob`: the job's best-effort copy in
/// `mysql.tidb_ddl_history` (`addHistoryDDLJob2Table`, whose failure Go only
/// logs), then the meta `DDLJobHistory` entry every reader iterates.
fn append_history_ddl_job<S: MetaSnapshot>(
    catalog: &ClusterCatalog,
    snapshot: &mut S,
    job: &Job,
    encoded: Vec<u8>,
    writes: &mut Vec<BufferMutation>,
) -> Result<(), DdlPlanError> {
    let table_copy = crate::ddl_history_table::DdlHistoryTable::locate(catalog)
        .and_then(|history| history.append_insert_ignore(snapshot, job, &encoded, writes));
    if let Err(error) = table_copy {
        tidb_ddl_logutil::ddl_logger().info(
            "failed to add DDL job to history table",
            &[tidb_log::Field::new(
                "error",
                tidb_log::Value::Str(error.to_string()),
            )],
        );
    }
    writes.push(BufferMutation::set(
        key::ddl_job_history_kv_key(job.id),
        encoded,
    )?);
    Ok(())
}

/// The job Go's worker files when a direct change finishes (`finishDDLJob`):
/// the submitter's identity for it (`Job.SchemaName` / `TableName` /
/// `SchemaID` / `TableID`, the statement text, SQL mode and CDC source), the
/// done state Go's `Finish*Job` leaves, and the finished table or schema in
/// `BinlogInfo`. A multi-schema change carries its sub-jobs.
#[allow(clippy::too_many_arguments)]
/// Go `executor.CreateTable` up to the job: the table built from the
/// statement (`BuildTableInfoWithStmt`, with the schema's default placement
/// and every written policy resolved by id), `checkTableForeignKeysValid`,
/// the existence check (a Note under IF NOT EXISTS, answered as `Err(plan)`)
/// and `checkTableInfoValidExtra`. Ids are not assigned yet.
fn build_create_table_info<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    schema: &str,
    table: &str,
    if_not_exists: bool,
    build: &CreateTableBuild,
    warnings: &mut Vec<tidb_util::context::SqlWarn>,
) -> Result<Result<TableInfo, DdlPlan>, DdlPlanError> {
    let Some(database) = find_database(catalog, schema) else {
        return Err(DdlPlanError::from(
            tidb_error::catalog::infoschema::ERR_DATABASE_NOT_EXISTS
                .generate_with_stack_by_args(&[(schema.to_owned()).into()]),
        ));
    };
    // Go's create-table path publishes the TableInfo the builder produced;
    // it does not call `TableInfo.Clone` on the way to
    // `Mutator.CreateTableOrView`. `clone_like_go()` would allocate an empty
    // `Indices` slice even when the builder left it nil, turning Go's
    // persisted `"index_info":null` into `[]`.
    let mut info = build
        .for_database(&database.info.charset, &database.info.collate)
        .map_err(DdlPlanError::Admission)?;
    // Go `checkTTLInfoValid`'s foreign-key half, which reads the infoschema
    // the builder does not see.
    if info.ttl_info.is_some()
        && !crate::foreign_key_build::get_table_referred_foreign_keys(catalog, schema, table)
            .is_empty()
    {
        let error = tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_TTL_REFERENCED_BY_FK
                .generate_with_stack_by_args(&[]),
        );
        return Err(DdlPlanError::from_mysql_error(error));
    }
    // Go `CreateTable`: the built table's `checkTableForeignKeysValid`
    // (executor.go:1106) comes before `createTableWithInfoJob`'s existence
    // check, which is followed by `checkTableInfoValidExtra`.
    let fk_check = build.context.foreign_key_checks();
    crate::foreign_key_build::check_table_foreign_keys_valid(catalog, schema, &info, fk_check)
        .map_err(DdlPlanError::Admission)?;
    if let Some(existing) = find_table(database, table) {
        if if_not_exists {
            // Go `createTableWithInfoJob`: `ErrTableExists` as a note.
            return Ok(Err(already_with_warnings(
                format!(
                    "table `{schema}`.`{}` already exists",
                    existing.name.original()
                ),
                vec![crate::real_tikv_ddl::ddl_note(
                    tidb_error::catalog::infoschema::ERR_TABLE_EXISTS.generate_with_stack_by_args(
                        &[format!("{}.{}", schema, existing.name.original()).into()],
                    ),
                )],
            )));
        }
        return Err(DdlPlanError::from(
            tidb_error::catalog::infoschema::ERR_TABLE_EXISTS.generate_with_stack_by_args(&[
                format!("{}.{}", (schema.to_owned()), (table.to_owned())).into(),
            ]),
        ));
    }
    check_table_info_valid_extra(&info, warnings)?;
    // Go `CreateTableWithInfo` resolves a table's `PLACEMENT POLICY = name`
    // against the policies in the infoschema and refuses an unknown one with
    // `ErrPlacementPolicyNotExists` (8239). The reference records the
    // policy's ID as well as its name, because placement bundles resolve by
    // id.
    let written_policy = build
        .create
        .table_options
        .iter()
        .find_map(|option| match option {
            tidb_ast::TableOption::PlacementPolicy(name) => Some(name.clone()),
            _ => None,
        });
    if let Some(policy_name) = &written_policy {
        // Go `checkAndNormalizePlacementPolicy`: `default` is no policy.
        info.placement_policy_ref = check_and_normalize_placement_policy(snapshot, policy_name)?;
    }
    // Go `handleTablePlacement`: every partition that names a policy of its
    // own resolves it BY id too (`default` clears it).
    if let Some(partition) = info.partition.as_ref() {
        let definitions = partition.read().definitions.snapshot();
        for (index, definition) in definitions.iter().enumerate() {
            let Some(reference) = definition.placement_policy_ref.as_ref() else {
                continue;
            };
            let written = reference.read().name.original().to_owned();
            let resolved = check_and_normalize_placement_policy(snapshot, &written)?;
            partition.read().definitions.update(index, |definition| {
                definition.placement_policy_ref = resolved;
            });
        }
    }
    // Go `BuildTableInfoWithStmt`: a non-temporary table that names no
    // placement takes the schema's default. A written `default` is a
    // reference there (cleared only afterwards), so it does not.
    if written_policy.is_none()
        && info.placement_policy_ref.is_none()
        && info.temp_table_type == tidb_model::TempTableType::NONE
    {
        if let Some(reference) = database.info.placement_policy_ref.as_ref() {
            info.placement_policy_ref = Some(tidb_model::GoShared::new(reference.read().clone()));
        }
    }
    Ok(Ok(info))
}

/// Go `createTable`'s publication once the ids are stamped: the placement
/// bundles (`NewFullTableBundles`, sent before the schema version is
/// published), the affinity groups, the table key at the job transaction's
/// start timestamp and `handleAutoIncID`'s counter seeds.
fn apply_create_table<S: MetaSnapshot>(
    snapshot: &mut S,
    db_id: i64,
    info: &mut TableInfo,
    start_ts: u64,
    writes: &mut Vec<BufferMutation>,
    placement_bundles: &mut Vec<tidb_placement::Bundle>,
    affinity_groups: &mut crate::ddl_affinity::AffinityGroupUpdate,
) -> Result<(), DdlPlanError> {
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
        *placement_bundles =
            tidb_placement::new_full_table_bundles(&policies, info).map_err(|error| {
                DdlPlanError::Admission(DdlAdmissionError::plain(
                    error.to_string(),
                ))
            })?;
    }
    // Go `createTable`'s `createTableAffinityGroupsInPD`, after the
    // placement bundles and wrapped as Go wraps it.
    affinity_groups.create_error_context = Some(CREATE_TABLE_AFFINITY_ERROR);
    affinity_groups
        .create_for(info)
        .map_err(|error| pd_step_error(Some(CREATE_TABLE_AFFINITY_ERROR), error))?;
    info.update_ts = start_ts;
    let encoded = value::serialize_table_info(info)
        .map_err(|error| DdlPlanError::Encode(error.to_string()))?;
    writes.push(BufferMutation::set(
        key::table_kv_key(db_id, info.id),
        encoded,
    )?);
    // Go `handleAutoIncID` seeds the allocator when the table option asks for
    // a first id above 1, to `AutoIncID - 1` (the counter holds the id last
    // handed out). Which key it is, is Go's `SepAutoInc` choice.
    if info.auto_inc_id > 1 {
        writes.push(BufferMutation::set(
            tidb_cluster::cluster_auto_id::auto_id_key_for(db_id, info),
            value::encode_int_value(info.auto_inc_id - 1),
        )?);
    }
    if info.auto_random_bits > 0 && info.auto_rand_id > 1 {
        writes.push(BufferMutation::set(
            tidb_cluster::cluster_auto_id::auto_random_id_key_for(db_id, info),
            value::encode_int_value(info.auto_rand_id - 1),
        )?);
    }
    Ok(())
}

/// Go `onTruncateTable`'s metadata: the table under `new_table_id` (and its
/// partitions under `new_partition_ids`), the old table key and allocators
/// gone, the placement bundles, affinity groups and label rules re-keyed.
/// Returns the new table.
#[allow(clippy::too_many_arguments)]
fn apply_truncate_table<S: MetaSnapshot>(
    snapshot: &mut S,
    schema: &str,
    db_id: i64,
    stored: &TableInfo,
    new_table_id: i64,
    new_partition_ids: &[i64],
    start_ts: u64,
    writes: &mut Vec<BufferMutation>,
    placement_bundles: &mut Vec<tidb_placement::Bundle>,
    affinity_groups: &mut crate::ddl_affinity::AffinityGroupUpdate,
    label_rules: &mut LabelRuleSteps,
) -> Result<TableInfo, DdlPlanError> {
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
    let mut info = stored.clone_like_go();
    info.id = new_table_id;
    if let Some(replica) = &info.tiflash_replica {
        let mut replica = replica.write();
        replica.available = false;
        replica.available_partition_ids = Default::default();
    }
    if let Some(partition) = &info.partition {
        let partition = partition.read();
        for (ordinal, id) in new_partition_ids.iter().enumerate() {
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
        *placement_bundles =
            tidb_placement::new_full_table_bundles(&policies, &info).map_err(|error| {
                DdlPlanError::Admission(DdlAdmissionError::plain(
                    error.to_string(),
                ))
            })?;
    }
    // Go `onTruncateTable`: the new identity's groups first (the job
    // fails when PD refuses), then the old table's, best-effort.
    if info.affinity.is_some() {
        affinity_groups
            .create_for(&info)
            .map_err(|error| pd_step_error(None, error))?;
    }
    if stored.affinity.is_some() {
        affinity_groups.delete_for(stored, None);
    }
    // Go `onTruncateTable`: the rules named for the table and its
    // partitions re-range over the new IDs.
    let label_table = LabelRuleTable::from_table_info(&info);
    let old_rules = label_rules::get_old_label_rules(
        label_rules.manager(),
        &label_table,
        &schema.go_to_lower(),
        &label_table.name,
    )
    .map_err(|error| pd_step_error(Some("failed to get old label rules from PD"), error))?;
    label_rules.push(
        label_rules::update_label_rules(
            &schema.go_to_lower(),
            &label_table,
            &old_rules,
            &[],
            new_table_id,
        ),
        Some("failed to update the label rule to PD"),
    )?;
    Ok(info)
}

/// Go `checkModifyCharsetAndCollation` under `AlterTableCharsetAndCollate`,
/// which rewords the indexed-collation refusal for the column.
fn check_modify_charset_and_collation(
    to_charset: &str,
    to_collate: &str,
    from_charset: &str,
    from_collate: &str,
    need_rewrite_collation_data: bool,
    column_name: &str,
) -> Result<(), DdlPlanError> {
    tidb_ddl_build::modify_column_type::check_modify_charset_and_collation(
        to_charset,
        to_collate,
        from_charset,
        from_collate,
        need_rewrite_collation_data,
    )
    .map_err(|refusal| {
        DdlPlanError::from_mysql_error(refusal.into_error(|| {
            format!(
                "Unsupported converting collation of column '{}' from '{from_collate}' to '{to_collate}' when index is defined on it.",
                column_name.go_to_lower()
            )
        }))
    })
}

fn unsupported_charset_change(proto: &'static TerrorError, reason: String) -> DdlPlanError {
    DdlPlanError::Admission(
        tidb_ddl_build::table_info_build::DdlAdmissionError::from_terror(
            &proto.generate_with_stack(reason),
        ),
    )
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
            &tidb_error::catalog::types::ERR_TOO_BIG_FIELD_LENGTH,
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
fn lock_plan_error(error: tidb_error::mysql::MysqlError) -> DdlPlanError {
    DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error))
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

fn already(detail: String) -> DdlPlan {
    DdlPlan::AlreadySatisfied {
        detail,
        warnings: Vec::new(),
    }
}

/// go's note for one missing table: `Note | 1051 | Unknown table 'd.t'`.
fn missing_table_note(schema: &str, table: &str) -> tidb_util::context::SqlWarn {
    crate::real_tikv_ddl::ddl_note(
        tidb_error::catalog::infoschema::ERR_TABLE_DROP_EXISTS
            .generate_with_stack_by_args(&[format!("{}.{}", schema, table).into()]),
    )
}

/// `already` carrying go's per-table 1051 notes (DROP ... IF EXISTS).
fn already_with_warnings(detail: String, warnings: Vec<tidb_util::context::SqlWarn>) -> DdlPlan {
    DdlPlan::AlreadySatisfied { detail, warnings }
}

/// Resolves `schema`.`table` to its database ID and stored `TableInfo`.
///
/// An index change has no `IF EXISTS` for the TABLE, only for the index, so a
/// missing database or table is always an error here.
/// Go `executor.AlterTable`: a view or a sequence is refused before any
/// specification is looked at (`ErrWrongObject`).
/// The table an `ALTER TABLE` specification names; `None` for every statement
/// that is not one. `CREATE INDEX` and `DROP INDEX` lower to the same index
/// variants as `ALTER TABLE ... ADD/DROP INDEX` but Go does not run them through
/// `AlterTable`'s view refusal (`drop index i on v` is 1091), so they stay out.
fn alter_table_target(statement: &DdlStatement) -> Option<(&str, &str)> {
    use DdlStatement as S;
    match statement {
        S::RebaseAutoRandom { schema, table, .. }
        | S::ModifyAutoIdCache { schema, table, .. }
        | S::SetTiFlashReplica { schema, table, .. }
        | S::AlterTableAttributes { schema, table, .. }
        | S::AlterTablePartitionAttributes { schema, table, .. }
        | S::AlterAutoRandomBits { schema, table, .. }
        | S::AddColumn { schema, table, .. }
        | S::AddCheckConstraint { schema, table, .. }
        | S::DropCheckConstraint { schema, table, .. }
        | S::AlterCheckConstraint { schema, table, .. }
        | S::IgnoredCheckConstraint { schema, table }
        | S::AddPartitions { schema, table, .. }
        | S::DropPartitions { schema, table, .. }
        | S::TruncatePartitions { schema, table, .. }
        | S::ExchangePartition { schema, table, .. }
        | S::DropColumn { schema, table, .. }
        | S::ModifyColumn { schema, table, .. }
        | S::AddForeignKey { schema, table, .. }
        | S::AlterTableCache { schema, table, .. }
        | S::DropForeignKey { schema, table, .. }
        | S::RenameColumn { schema, table, .. }
        | S::MultiSchemaChange { schema, table, .. }
        | S::ReorganizePartitions { schema, table, .. }
        | S::ModifyEngineAttribute { schema, table, .. }
        | S::AlterTtl { schema, table, .. }
        | S::RemoveTtl { schema, table }
        | S::AlterTablePlacement { schema, table, .. }
        | S::AlterTablePartitionPlacement { schema, table, .. }
        | S::ShardRowId { schema, table, .. }
        | S::ModifyTableComment { schema, table, .. }
        | S::AlterIndexVisibility { schema, table, .. }
        | S::DropPrimaryKey { schema, table }
        | S::ModifyTableCharsetAndCollate { schema, table, .. }
        | S::RenameIndex { schema, table, .. }
        | S::SetColumnDefault { schema, table, .. }
        | S::IgnoredTableOption { schema, table, .. }
        | S::OrderByColumns { schema, table }
        | S::RebaseAutoIncrementId { schema, table, .. } => Some((schema, table)),
        S::RenameTable {
            is_alter: true,
            from_schema,
            from_table,
            ..
        } => Some((from_schema, from_table)),
        _ => None,
    }
}

fn refuse_non_base_table(
    stored: &TableInfo,
    schema: &str,
    table: &str,
) -> Result<(), DdlPlanError> {
    if stored.is_view() || stored.is_sequence() {
        return Err(coded_refusal(
            &tidb_util::dbterror::ERR_WRONG_OBJECT,
            format!("'{schema}.{table}' is not BASE TABLE"),
        ));
    }
    Ok(())
}

fn locate_table<'catalog>(
    catalog: &'catalog ClusterCatalog,
    schema: &str,
    table: &str,
) -> Result<(i64, &'catalog TableInfo), DdlPlanError> {
    let Some(database) = find_database(catalog, schema) else {
        return Err(DdlPlanError::from(
            tidb_error::catalog::infoschema::ERR_DATABASE_NOT_EXISTS
                .generate_with_stack_by_args(&[(schema.to_owned()).into()]),
        ));
    };
    let Some(stored) = find_table(database, table) else {
        return Err(DdlPlanError::from(
            tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS.generate_with_stack_by_args(&[
                (schema.to_owned()).into(),
                (table.to_owned()).into(),
            ]),
        ));
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
    tidb_ddl_build::index::get_name_for_anonymous_index(&existing, &prefix, "")
}

/// Go `CreateForeignKey` (`executor.go`) for one ADD FOREIGN KEY, as its
/// submitter checks it against the original table.
struct CreatedForeignKey {
    fk: tidb_model::table::FKInfo,
    /// No index covers the key's columns, so `CreateForeignKey` queues a
    /// `createIndex` named after the key ahead of the key's own job.
    needs_index: bool,
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
            &tidb_error::catalog::infoschema::ERR_CANNOT_ADD_FOREIGN,
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
            &tidb_util::dbterror::ERR_FK_DUP_NAME,
            format!("Duplicate foreign key constraint name '{name}'"),
        ));
    }
    let ref_schema = match definition.reference.table.as_deref() {
        Some([ref_schema, _]) => ref_schema.clone(),
        _ => schema.to_owned(),
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
        return Err(DdlPlanError::from(
            tidb_util::dbterror::ERR_DUP_KEY_NAME.generate_with_stack_by_args(&[(fk
                .name
                .original()
                .to_owned())
            .into()]),
        ));
    }
    Ok(CreatedForeignKey { fk, needs_index })
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
    context: &dyn DdlSessionContext,
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

/// Go `BuildIndexInfo(nil, ...)` as the owner's `checkAndBuildIndexInfo`
/// runs it: the job's index rebuilt against the table with no session, so
/// the statement's checks (global index, comment length) are not repeated
/// and the condition is restored under the default charset.
fn build_index_info(
    info: &TableInfo,
    template: &IndexInfo,
    condition: Option<&tidb_ast::Expr>,
    name: &str,
) -> Result<IndexInfo, DdlPlanError> {
    tidb_ddl_build::check_too_long_identifier(name)
        .map_err(|error| DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error.into())))?;
    let mut added = template.clone_like_go();
    added.name = CiString::new(name.to_owned());
    tidb_ddl_build::index::resolve_index_columns(info, &mut added)
        .map_err(DdlPlanError::Admission)?;
    tidb_ddl_build::table_info_build::build_index_condition(
        info,
        &mut added,
        condition,
        &tidb_expr::metabuild::Context::new([]),
    )
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
        // Go `onCreateIndex` at the public state.
        add_index_column_flag(info, added);
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

/// Go `AddIndexColumnFlag`: a primary key's columns take PRI; any other
/// index flags its first column UNI (a one-column unique index) or MUL.
fn add_index_column_flag(info: &TableInfo, index: &IndexInfo) {
    use tidb_datatype::FieldTypeFlags;
    let flag_column = |offset: i64, flag| {
        if let Some(column) = usize::try_from(offset)
            .ok()
            .and_then(|offset| info.columns.get(offset))
        {
            column.write().field_type.add_flags(flag);
        }
    };
    if index.primary {
        for part in index.columns.iter_deref() {
            flag_column(part.read().offset, FieldTypeFlags::PRI_KEY);
        }
        return;
    }
    let Some(first) = index.columns.get(0) else {
        return;
    };
    let offset = first.read().offset;
    if index.unique && index.columns.len() == 1 {
        flag_column(offset, FieldTypeFlags::UNIQUE_KEY);
    } else {
        flag_column(offset, FieldTypeFlags::MULTIPLE_KEY);
    }
}

/// The table's index of that name, matched the way MySQL matches one:
/// case-insensitively.
fn find_index(table: &TableInfo, name: &str) -> Option<GoShared<IndexInfo>> {
    table
        .indices
        .iter_deref()
        .find(|index| index.read().name.original().go_lower_eq(name))
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
    let (system_database, table) = catalog
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
    // Go's internal INSERT allocates `_tidb_rowid` from the node's
    // allocator, whose reservation commits on its own.
    let allocator = if !table.pk_is_handle && !table.is_common_handle {
        let ids = snapshot.row_ids().ok_or_else(|| {
            DdlPlanError::Encode(format!("mysql.{NOTIFIER_TABLE_NAME} has no row-ID allocator"))
        })?;
        Some(ids.row_allocator_for(system_database.id, table))
    } else {
        None
    };
    for (sub_job_id, event) in events {
        let mut values = tidb_cluster::system_row_write::RowValues::new();
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
        let mutations = match &allocator {
            Some(allocator) => {
                let id = allocator.alloc_next().map_err(|error| {
                    DdlPlanError::Encode(format!(
                        "mysql.{NOTIFIER_TABLE_NAME}'s row-ID allocator: {error}"
                    ))
                })?;
                let row_id = i64::try_from(id)
                    .map_err(|_| DdlPlanError::GlobalIdExhausted { wanted: i64::MAX })?;
                tidb_cluster::system_row_write::insert_row(table, row_id, &values)
            }
            None => tidb_cluster::system_row_write::store_clustered_row(table, None, &values),
        };
        writes.extend(mutations.map_err(|error| DdlPlanError::Encode(error.to_string()))?);
    }
    Ok(())
}

/// Go `AlterTableCache`'s `replace into mysql.table_cache_meta values (%?,
/// 'NONE', 0, 0)`: the cached table's lock row, unlocked.
pub(crate) fn write_table_cache_meta_row(
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
    let mut values = tidb_cluster::system_row_write::RowValues::new();
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
        tidb_cluster::system_row_write::replace_unindexed_clustered_row(table, &values)
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
) -> Option<&'catalog tidb_cluster::cluster_catalog::LoadedDatabase> {
    let name = name.go_to_lower();
    catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == name)
}

fn find_table<'database>(
    database: &'database tidb_cluster::cluster_catalog::LoadedDatabase,
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
    use tidb_ddl_build::table_lifecycle::{check_drop_object, DropObjectKind};
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
    .map_err(|error| DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error)))
}
