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
//! DDL actions follow Go's durable path: submission first inserts the full
//! job envelope in `mysql.tidb_ddl_job`, each owner transaction reloads that
//! envelope and publishes one state transition, and terminal handling moves
//! the job to both Go history stores.
//!
//! Go's executor methods (`crate::executor`) admit statements and call the
//! typed job builders here.

use tidb_ddl_build::plan_error::{unknown_ddl_job_error, DdlPlanError};
use tidb_ddl_build::session_context::DdlSessionContext;
use tidb_ddl_build::index::set_global_index_version;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use tidb_ast::CiString;
use tidb_ast::{
    AlterTableStmt, CreateIndexStmt, CreateTableStmt, DatabaseOption, DdlStmt,
    IndexConstraintDefinition, IndexConstraintKind,
};
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_ddl_notifier::SchemaChangeEvent;
use tidb_mysql::errors::FormatArg;
use tidb_terror::TerrorError;
use tidb_hack::GoCaseCompare;
use tidb_meta::{key, value};
use tidb_metadef::system_tables_def::NOTIFIER_TABLE_NAME;
use tidb_model::action_type::ActionType;
use tidb_model::db::DBInfo;
use tidb_model::index::{IndexColumn, IndexInfo};
use tidb_model::partition::{PartitionDefinition, PartitionInfo};
use tidb_model::schema_diff::{AffectedOption, SchemaDiff};
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;
use tidb_model::{get_job_ver_in_use, AddCheckConstraintArgs, CheckConstraintArgs, GoField, GoShared, GoSharedPointerSlice, HistoryInfo, Job, JobArgsValue, JobState, TraceInfo};
use tidb_goruntime::go_runtime::GoSharedSlice;
use tidb_txnkv::transaction::BufferMutation;

use crate::job_txn::{job_meta_error, JobMeta, JobTxn};
use tidb_infoschema::cluster_catalog::{load_info_schema, MetaSnapshot};
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_context::WarningLevel;
use tidb_hack::GoToLower;
use tidb_ddl_build::column_types::resolve_charset_collation;

use tidb_ddl_build::table_info_build::DdlAdmissionError;

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
    /// Go `executor.CreateTable`: build metadata under the statement context.
    pub fn new(
        create: &CreateTableStmt,
        schema: &str,
        context: &DdlContext,
    ) -> Result<Self, DdlAdmissionError> {
        let mb_ctx = context.metabuild_context();
        let mut template = tidb_ddl_build::table_info_build::build_table_info_with_stmt(
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
        let mut info = tidb_ddl_build::table_info_build::build_table_info_with_stmt(
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
        mb_ctx: &tidb_metabuild::Context,
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
    let spec = tidb_table::table_from_meta::partition_spec_for(&columns, &partition.read())
        .map_err(DdlAdmissionError::from_mysql_error)?;
    let definitions = tidb_ddl_build::table_partition::append_partition_defs(
        &partition.read().definitions.snapshot(),
        &spec.kind,
        context.sql_mode().ansi_quotes,
    );
    context.rewrite_ddl_job_query(sugar, definitions.trim_start_matches('\n').to_owned());
    Ok(())
}

/// One column sub-action of a multi-action `ALTER TABLE`.
/// A vector, inverted (columnar) or full-text index as Go
/// `createColumnarIndex` receives it: the statement's name, key parts and
/// index option, checked against the stored table by the plan.
#[derive(Clone, Debug)]
pub struct ColumnarIndexSpec {
    /// Go `indexName`; empty is anonymous.
    pub index_name: String,
    /// Go `indexPartSpecifications`.
    pub parts: Vec<tidb_ast::IndexPart>,
    /// Go `indexOption`, its `Tp` as `checkIndexGrammar` leaves it.
    pub option: tidb_ast::IndexOption,
}

/// A `StmtContext` wrapper with a `Debug`, for the job builders' derived ones.
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

/// One source/destination pair of a `RENAME TABLE`.
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
pub fn database_charset_collation(
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
    catalog: &SessionExtendedInfoSchema,
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
    for (_, tables) in catalog.list_tables_with_special_attribute(placement_policy_attribute) {
        for table in tables {
            let table = table.read();
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
            &tidb_infoschema::infoschema_errors::ERR_PLACEMENT_POLICY_NOT_EXISTS
                .gen_with_stack_by_args(&[name.into()]),
        )));
    };
    Ok(Some(tidb_model::GoShared::new(tidb_model::PolicyRefInfo {
        id: policy.id,
        name: CiString::new(name),
    })))
}

/// Go `checkAndNormalizePlacementPolicy` over a reference: resolved by its
/// name, `default` meaning none.
fn normalize_placement_policy_ref<S: MetaSnapshot>(
    snapshot: &mut S,
    reference: Option<tidb_model::GoShared<tidb_model::PolicyRefInfo>>,
) -> Result<Option<tidb_model::GoShared<tidb_model::PolicyRefInfo>>, DdlPlanError> {
    match reference {
        Some(reference) => {
            let name = reference.read().name.original().to_owned();
            check_and_normalize_placement_policy(snapshot, &name)
        }
        None => Ok(None),
    }
}

/// Go `handleTablePlacement`: the table's and its partitions' references
/// resolved, or, under `tidb_placement_mode = 'IGNORE'`, removed with a note.
pub(super) fn handle_table_placement<S: MetaSnapshot>(
    snapshot: &mut S,
    context: &dyn DdlSessionContext,
    info: &mut TableInfo,
    warnings: &mut Vec<tidb_context::SqlWarn>,
) -> Result<(), DdlPlanError> {
    if context.placement_mode_ignore() && remove_table_placement(info) {
        warnings.push(placement_ignored_note());
        return Ok(());
    }
    info.placement_policy_ref =
        normalize_placement_policy_ref(snapshot, info.placement_policy_ref.take())?;
    if let Some(partition) = info.partition.as_ref() {
        let definitions = partition.read().definitions.snapshot();
        for (index, definition) in definitions.iter().enumerate() {
            let resolved =
                normalize_placement_policy_ref(snapshot, definition.placement_policy_ref.clone())?;
            partition.read().definitions.update(index, |definition| {
                definition.placement_policy_ref = resolved;
            });
        }
    }
    Ok(())
}

/// Go `handleDatabasePlacement`: under `tidb_placement_mode = IGNORE` the
/// reference is dropped with a note; otherwise it is normalized by name.
pub(super) fn handle_database_placement<S: MetaSnapshot>(
    snapshot: &mut S,
    context: &dyn DdlSessionContext,
    info: &mut tidb_model::DBInfo,
    warnings: &mut Vec<tidb_context::SqlWarn>,
) -> Result<(), DdlPlanError> {
    if info.placement_policy_ref.is_none() {
        return Ok(());
    }
    if context.placement_mode_ignore() {
        info.placement_policy_ref = None;
        warnings.push(placement_ignored_note());
        return Ok(());
    }
    info.placement_policy_ref =
        normalize_placement_policy_ref(snapshot, info.placement_policy_ref.take())?;
    Ok(())
}

/// Go `removeTablePlacement`: whether there was any.
fn remove_table_placement(info: &mut TableInfo) -> bool {
    let mut removed = info.placement_policy_ref.take().is_some();
    if let Some(partition) = info.partition.as_ref() {
        let definitions = partition.read().definitions.snapshot();
        for (index, definition) in definitions.iter().enumerate() {
            if definition.placement_policy_ref.is_some() {
                removed = true;
                partition.read().definitions.update(index, |definition| {
                    definition.placement_policy_ref = None;
                });
            }
        }
    }
    removed
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

/// Go `infoschemacontext.PlacementPolicyAttribute`: the table or one of its
/// partitions names a placement policy.
fn placement_policy_attribute(table: &TableInfo) -> bool {
    table.placement_policy_ref.is_some()
        || table.get_partition_info().is_some_and(|partition| {
            partition
                .read()
                .definitions
                .snapshot()
                .iter()
                .any(|definition| definition.placement_policy_ref.is_some())
        })
}

/// Go `CheckPlacementPolicyNotInUseFromInfoSchema`: whether any table or
/// partition still names this policy.
fn policy_referenced(catalog: &SessionExtendedInfoSchema, policy: &CiString) -> bool {
    catalog
        .list_tables_with_special_attribute(placement_policy_attribute)
        .into_iter()
        .any(|(_, tables)| {
        tables.into_iter().any(|table| {
            let table = table.read();
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

pub(crate) fn placement_settings_from_options(
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
        &tidb_dbterror::ERR_RUN_MULTI_SCHEMA_CHANGES
            .gen_with_stack_by_args(&[job.into()]),
    )
}

/// Go `executor.AlterTable`'s spec walk (`pkg/ddl/executor.go:1763-1774`,
/// `:1952`): the partition maintenance specs and the table options it has
/// no arm for are refused with their own errors.
fn check_alter_table_spec_refusals(alter: &AlterTableStmt) -> Result<(), DdlAdmissionError> {
    use tidb_ast::{AlterPartitionAction, PartitionMaintenanceOp};
    use tidb_dbterror as dbt;
    for action in &alter.actions {
        match action {
            tidb_ast::AlterTableAction::Partition(partition) => {
                let (proto, message): (&tidb_terror::TerrorError, &str) = match partition {
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
                    &proto.gen_with_stack(message),
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
                    &dbt::ERR_UNSUPPORTED_ALTER_TABLE_OPTION.gen_with_stack_by_args(&[]),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Go `ResolveAlterTableSpec`: drops the ignorable LOCK / ALGORITHM specs,
/// splits a grouped `ADD COLUMN (...)` (`resolveAlterTableAddColumns`: one
/// ADD COLUMN spec per column, then one ADD CONSTRAINT spec per table
/// constraint), and resolves the last ALGORITHM against every remaining
/// spec before any of them runs.
pub fn resolve_alter_table_spec(
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
            &tidb_dbterror::ERR_ALTER_OPERATION_NOT_SUPPORTED.gen_with_stack(message),
        ));
    }
    context.append_sql_warn(tidb_context::SqlWarn::of(
        WarningLevel::Error,
        &tidb_dbterror::ERR_ALTER_OPERATION_NOT_SUPPORTED,
        message,
    ));
    Ok(())
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
            tidb_infoschema::infoschema_errors::ERR_COLUMN_NOT_EXISTS.gen_with_stack_by_args(&[
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
        let error = tidb_ddl_build::check_constraint::err_cant_drop_col_with_check_constraint(
            constraint.name.original(),
            from,
        );
        return Err(DdlPlanError::Admission(DdlAdmissionError::from_terror(
            &error.proto.gen_with_stack(error.message),
        )));
    }
    if wanted == to.go_to_lower() {
        return Ok(None);
    }
    let new_name = to.go_to_lower();
    if new_name == "_tidb_rowid" {
        return Err(coded_refusal(
            &tidb_dbterror::ERR_WRONG_COLUMN_NAME,
            format!("Incorrect column name '{new_name}'"),
        ));
    }
    if stored
        .columns
        .iter_deref()
        .any(|candidate| candidate.read().name.lowercase() == new_name)
    {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_COLUMN_EXISTS
                .gen_with_stack_by_args(&[(to.to_owned()).into()]),
        ));
    }
    // Go `checkModifyColumnWithGeneratedColumnsConstraint`.
    if let Some(dependent) = stored.columns.iter_deref().find(|candidate| {
        let candidate = candidate.read();
        candidate.is_generated() && candidate.dependences.contains(&wanted)
    }) {
        return Err(if dependent.read().hidden {
            coded_refusal(
                &tidb_dbterror::ERR_DEPENDENT_BY_FUNCTIONAL_INDEX,
                format!(
                    "Column '{from}' has an expression index dependency and cannot be dropped \
                     or renamed"
                ),
            )
        } else {
            coded_refusal(
                &tidb_dbterror::ERR_DEPENDENT_BY_GENERATED_COLUMN,
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
            &*tidb_dbterror::ERR_DEPENDENT_BY_FUNCTIONAL_INDEX,
            format!(
                "Column '{written}' has an expression index dependency and cannot be dropped or \
                 renamed"
            ),
        )
    } else {
        (
            &*tidb_dbterror::ERR_DEPENDENT_BY_GENERATED_COLUMN,
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
            &tidb_dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN,
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
            &tidb_dbterror::ERR_UNSUPPORTED_ON_GENERATED_COLUMN,
            format!("'{what}' is not supported for generated columns."),
        )
    };
    let bad_field = |column: &str| {
        coded_refusal(
            &tidb_dbterror::ERR_BAD_FIELD,
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
                    &tidb_dbterror::ERR_GENERATED_COLUMN_NON_PRIOR,
                    "Generated column can refer only to generated columns defined prior to it."
                        .to_owned(),
                ));
            }
        }
    }

    if new.is_generated() {
        fn admission(error: impl Into<tidb_terror::MysqlError>) -> DdlPlanError {
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
                        &tidb_dbterror::ERR_GENERATED_COLUMN_REF_AUTO_INC,
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
                &tidb_dbterror::ERR_UNSUPPORTED_ON_GENERATED_COLUMN,
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
    materialized_view::check_materialized_view_group_key_index_constraints(info, from, "RENAME INDEX")
        .map_err(DdlPlanError::from_mysql_error)?;
    if find_index(info, from).is_none() {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_KEY_NOT_EXISTS.gen_with_stack_by_args(&[
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
                tidb_dbterror::ERR_DUP_KEY_NAME.gen_with_stack_by_args(&[(existing
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
    if invisible {
        materialized_view::check_materialized_view_group_key_index_constraints(info, index, "ALTER INDEX INVISIBLE")
            .map_err(DdlPlanError::from_mysql_error)?;
    }
    // The index must exist AND be public, else `ErrKeyNotExists`.
    let found = find_index(info, index)
        .filter(|candidate| candidate.read().state == tidb_model::SchemaState::PUBLIC);
    let Some(found) = found else {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_KEY_NOT_EXISTS.gen_with_stack_by_args(&[
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
            &tidb_dbterror::ERR_UNSUPPORTED_INDEX_TYPE,
            format!(
                "INVISIBLE can not be used in {} INDEX",
                found.read().tp.sql()
            ),
        ));
    }
    set_index_visibility(info, index, invisible);
    Ok(AlterColumnOutcome::Applied)
}

/// Go `setIndexVisibility`: every index of that name, and the temporary
/// index a MODIFY COLUMN builds to replace it (a changing index over a
/// changing column, whose origin name matches case-insensitively; an
/// ordinary index whose name merely has a suffix is not one).
fn set_index_visibility(info: &TableInfo, name: &str, invisible: bool) {
    let wanted = name.go_to_lower();
    for candidate in info.indices.iter_deref() {
        let matches = {
            let read = candidate.read();
            read.name.lowercase() == wanted
                || (read.is_changing()
                    && modify_column::is_temp_index(&read, info)
                    && tidb_mysql::equal_fold(&read.get_changing_origin_name(), name))
        };
        if matches {
            candidate.write().invisible = invisible;
        }
    }
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
            tidb_infoschema::infoschema_errors::ERR_COLUMN_NOT_EXISTS.gen_with_stack_by_args(&[
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
            tidb_dbterror::ERR_INVALID_AUTO_RANDOM.gen_with_stack_by_args(&[
                ("auto_random is incompatible with default".to_owned()).into(),
            ]),
        ));
    }
    let mut target = target.write();
    tidb_ddl_build::table_info_build::alter_column(column, &mut target, default_value, &context.metabuild_context())
        .map_err(DdlPlanError::Admission)
}

/// Go refuses an ALTER TABLE charset change that names neither a charset nor
/// a collation before it resolves anything.
fn check_table_charset_named(charset: &str, collate: &str) -> Result<(), DdlPlanError> {
    if charset.is_empty() && collate.is_empty() {
        return Err(DdlPlanError::Admission(
            tidb_ddl_build::table_info_build::DdlAdmissionError::from_terror(
                &tidb_dbterror::ERR_UNKNOWN_CHARACTER_SET
                    .gen_with_stack_by_args(&["".into()]),
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
    catalog: &SessionExtendedInfoSchema,
    schema: &str,
    table: &str,
    info: &mut TableInfo,
    charset: &str,
    collate: &str,
    overwrite_columns: bool,
) -> Result<AlterColumnOutcome, DdlPlanError> {
    let database = find_database(catalog, schema)
        .expect("locate_table resolved the database")
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
    let database_info = database.read();
    let (from_charset, from_collate) = resolve_charset_collation(
        &[
            (
                if info.charset.is_empty() {
                    database_info.charset.as_str()
                } else {
                    info.charset.as_str()
                },
                if info.collate.is_empty() {
                    database_info.collate.as_str()
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
        let collation = tidb_charset::Collation::from_name(&to_collate)
            .unwrap_or(tidb_charset::Collation::Binary);
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
                    .set_collation(tidb_charset::Collation::Binary);
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
            &tidb_autoid::autoid_errors::ERR_AUTOINC_READ_FAILED,
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
    built_type: Option<&tidb_datatype::FieldType>,
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
            tidb_infoschema::infoschema_errors::ERR_COLUMN_EXISTS
                .gen_with_stack_by_args(&[(column.name.clone()).into()]),
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
        built_type,
        &info.charset,
        &info.collate,
        &context.metabuild_context(),
        context.enable_check_constraint(),
        &generated_namespace,
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
            &tidb_dbterror::ERR_GENERATED_COLUMN_NON_PRIOR,
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
                    tidb_infoschema::infoschema_errors::ERR_COLUMN_NOT_EXISTS
                        .gen_with_stack_by_args(&[
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
/// A reorganization's failure as the job's error: its own Go identity when
/// it carried one, else [`checked_refusal`]'s code table.
pub(crate) fn reorg_failure(
    code: u16,
    message: &str,
    identity: Option<tidb_terror::TerrorIdentity>,
) -> DdlPlanError {
    match identity {
        Some(identity) => DdlPlanError::from(tidb_terror::TerrorError::synthesize(
            identity.class(),
            identity.code(),
            message.to_owned(),
        )),
        None => checked_refusal(code, message.to_owned()),
    }
}

pub(crate) fn checked_refusal(code: u16, message: impl Into<String>) -> DdlPlanError {
    use tidb_table::table_errors as table;
    use tidb_kv::kv_errors as kv;
    use tidb_datatype::types_errors as types;
    let proto: Option<&'static TerrorError> = match code {
        1062 => Some(&kv::ERR_KEY_EXISTS),
        1048 => Some(&table::ERR_COLUMN_CANT_NULL),
        1264 => Some(&types::ERR_WARN_DATA_OUT_OF_RANGE),
        1265 => Some(&types::ERR_TRUNCATED),
        1292 => Some(&types::ERR_TRUNCATED_WRONG_VAL),
        1366 => Some(&table::ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD),
        1406 => Some(&types::ERR_DATA_TOO_LONG),
        1690 => Some(&types::ERR_OVERFLOW),
        1365 => Some(&tidb_expr::expression_errors::ERR_DIVISION_BY_ZERO),
        1451 => Some(&tidb_plannererrors::ERR_ROW_IS_REFERENCED2),
        1452 => Some(&tidb_dbterror::ERR_NO_REFERENCED_ROW2),
        3819 => Some(&tidb_dbterror::ERR_CHECK_CONSTRAINT_IS_VIOLATED),
        3903 => Some(&tidb_expr::expression_errors::ERR_INVALID_JSON_FOR_FUNC_INDEX),
        _ => None,
    };
    match proto {
        Some(proto) => coded_refusal(proto, message),
        None => DdlPlanError::from_mysql_error(tidb_terror::MysqlError::sql_error(code, message)),
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
                    &tidb_dbterror::ERR_INVALID_AUTO_RANDOM,
                    format!(
                        "Invalid auto random: unsupported add column '{}' constraint AUTO_RANDOM when altering '{schema}.{table}'",
                        column.name
                    ),
                ));
            }
            _ => continue,
        };
        return Err(coded_refusal(
            &tidb_dbterror::ERR_UNSUPPORTED_ADD_COLUMN,
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
        &proto.gen_with_stack(message.into()),
    ))
}

/// Go `checkIsDroppableColumn` (`pkg/ddl/executor.go`): one DROP COLUMN as
/// its submitter checks it against the table as it stands. `Ok(false)` is
/// IF EXISTS over a column that is not there, which Go skips with a 1091
/// note.
fn check_is_droppable_column(
    catalog: &SessionExtendedInfoSchema,
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
            tidb_dbterror::ERR_CANT_DROP_FIELD_OR_KEY
                .gen_with_stack_by_args(&[(column.to_owned()).into()]),
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
            &tidb_dbterror::ERR_UNSUPPORTED_PK_HANDLE,
            "Unsupported drop integer primary key",
        ));
    }
    // Go `ErrCantDropColWithAutoInc`.
    if col.get_flag() & u64::from(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT) != 0
        && !allow_remove_auto_inc
    {
        return Err(coded_refusal(
            &tidb_dbterror::ERR_CANT_DROP_COL_WITH_AUTO_INC,
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
                &tidb_dbterror::ERR_DEPENDENT_BY_FUNCTIONAL_INDEX,
                format!(
                    "Column '{col_name}' has an expression index dependency and cannot be \
                     dropped or renamed"
                ),
            )
        } else {
            coded_refusal(
                &tidb_dbterror::ERR_DEPENDENT_BY_GENERATED_COLUMN,
                format!("Column '{col_name}' has a generated column dependency."),
            )
        });
    }
    if column_count == 1 {
        return Err(coded_refusal(
            &tidb_dbterror::ERR_CANT_REMOVE_ALL_FIELDS,
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
                &tidb_dbterror::ERR_CANT_DROP_COL_WITH_INDEX,
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
        let error = tidb_ddl_build::check_constraint::err_cant_drop_col_with_check_constraint(
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
            &tidb_dbterror::ERR_DEPENDENT_BY_PARTITION_FUNCTIONAL,
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
                &tidb_dbterror::ERR_TTL_COLUMN_CANNOT_DROP,
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
                &tidb_dbterror::ERR_MODIFY_COLUMN_REFERENCED_BY_PARTIAL_CONDITION,
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
    tiflash_stores: &dyn Fn() -> Result<u64, String>,
    db_name: &str,
    info: &mut TableInfo,
    warnings: &mut Vec<tidb_context::SqlWarn>,
) -> Result<(), DdlPlanError> {
    tidb_ddl_build::table_info_build::check_table_info_valid_extra(
        &mut |warning| {
            warnings.push(tidb_context::SqlWarn::new(WarningLevel::Warning, warning));
        },
        Some(tiflash_stores),
        db_name,
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
            &tidb_dbterror::ERR_TOO_MANY_FIELDS,
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
            &tidb_dbterror::ERR_TABLE_MUST_HAVE_COLUMNS,
            "A table must have at least 1 column",
        ));
    }
    Err(coded_refusal(
        &tidb_dbterror::ERR_CANT_REMOVE_ALL_FIELDS,
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
                    &tidb_dbterror::ERR_DROP_INDEX_NEEDED_IN_FOREIGN_KEY,
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
        &tidb_dbterror::ERR_CANT_DROP_FIELD_OR_KEY,
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

pub fn split_name(
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
            &tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
        )),
        [object] => Ok((default_schema.to_owned(), object.clone())),
        [schema, object] => Ok((schema.clone(), object.clone())),
        _ => Err(DdlAdmissionError::plain(format!(
            "{what} name `{}` is not a `[schema.]name` path",
            path.join(".")
        ))),
    }
}

/// Go `executor.CreateIndex` / `CreatePrimaryKey`: AST options to the typed job.
pub(crate) fn create_index_job<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &SessionExtendedInfoSchema,
    create: &CreateIndexStmt,
    default_schema: &str,
    primary: bool,
    context: &DdlContext,
    store_count: usize,
    has_null: &mut dyn FnMut(&str, &TableInfo, i64) -> Result<bool, DdlPlanError>,
) -> Result<Option<add_index::AddIndexSubmission>, DdlPlanError> {
    let (schema, table) = split_name(&create.table, default_schema, "table")?;
    let mut build = |index: Box<IndexInfo>,
                     expressions: Vec<Option<tidb_ast::Expr>>,
                     condition: Option<tidb_ast::Expr>,
                     columnar: Option<Box<ColumnarIndexSpec>>| {
        let built = add_index::create_index_with_info(
            snapshot,
            catalog,
            &schema,
            &table,
            &create.if_not_exists,
            &index,
            &expressions,
            &condition,
            &DdlStatementContext(context.clone()),
            &columnar,
            create.options.pre_split_regions.as_ref(),
            store_count,
            context.as_ref(),
        )?;
        if matches!(built, Some(add_index::AddIndexSubmission::Job(..))) {
            add_index::check_primary_key_null_values(catalog, &schema, &table, &index, has_null)?;
        }
        Ok(built)
    };

    // go's `ADD PRIMARY KEY` lowers through the CreateIndex statement with
    // `primary = true`; the caller decides, since the AST's own
    // `CREATE INDEX` grammar has no PRIMARY spelling.

    // Go `checkCreateIndexGrammar` / `checkConstraintGrammar` in the
    // preprocessor.
    let (is_columnar, tp) = tidb_ddl_build::table_info_build::check_index_grammar(
        create.kind,
        &create.options,
        &create.parts,
    )?;
    // Go `createColumnarIndex`: its checks need the stored table.
    if is_columnar {
        let options = &create.options;
        let option = tidb_ast::IndexOption {
            key_block_size: options.key_block_size.unwrap_or(0),
            tp: tp.0,
            comment: options.comment.clone().unwrap_or_default(),
            parser_name: CiString::new(options.parser_name.clone().unwrap_or_default()),
            visibility: match options.visibility {
                None => 0,
                Some(tidb_ast::IndexVisibility::Visible) => 1,
                Some(tidb_ast::IndexVisibility::Invisible) => 2,
            },
            primary_key_tp: match options.primary_key_storage {
                None => 0,
                Some(tidb_ast::PrimaryKeyStorage::Clustered) => 1,
                Some(tidb_ast::PrimaryKeyStorage::NonClustered) => 2,
                Some(tidb_ast::PrimaryKeyStorage::Unknown(value)) => value,
            },
            global: options.global,
            secondary_engine_attr: options
                .secondary_engine_attribute
                .clone()
                .unwrap_or_default(),
            add_columnar_replica_on_demand: options.add_columnar_replica_on_demand,
        };
        return build(
            Box::default(),
            Vec::new(),
            None,
            Some(Box::new(ColumnarIndexSpec {
                index_name: create.name.clone(),
                parts: create.parts.clone(),
                option,
            })),
        );
    }
    let (index, expressions) = tidb_ddl_build::index::index_template(create, primary)?;
    build(
        Box::new(index),
        expressions,
        create.options.condition.clone(),
        None,
    )
}

/// One worker transaction decoded from a persisted active DDL job.
#[derive(Clone, Debug)]
pub struct PersistedDdlJobStep {
    /// The job as the step left it, which Go's `afterWaitSchemaSynced`
    /// call receives once the step's schema version is synchronized.
    pub job: Box<tidb_model::Job>,
    /// SessionExtendedInfoSchema and active-job mutations committed atomically by the worker.
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

/// Go `executor.CreateCheckConstraint` before `doDDLJob2`.
pub(crate) fn create_check_constraint_job(
    catalog: &SessionExtendedInfoSchema,
    schema: &String,
    table: &String,
    definition: &Box<tidb_ast::CheckConstraintDefinition>,
    context: &DdlStatementContext,
) -> Result<tidb_ddl_jobsubmit::JobSpec, DdlPlanError> {
    let (job, args) = {
        let (schema_id, stored) = locate_table(&catalog, schema, table)?;
        let stored = stored.read();
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
        // Go `CreateCheckConstraint` checks the table's own constraints; the
        // owner's `checkConstraintNamesNotExists` lists the schema from meta.
        if stored.constraints.iter_deref().any(|existing| {
            existing.read().name.lowercase() == constraint.name.lowercase()
        }) {
            return Err(DdlPlanError::Admission(DdlAdmissionError::from_terror(
                &tidb_dbterror::ERR_CHECK_CONSTRAINT_DUP_NAME
                    .gen_with_stack_by_args(&[constraint.name.original().into()]),
            )));
        }
        // Go's executor submits StateNone/ID zero. The first owner step
        // allocates the durable constraint ID.
        constraint.id = 0;
        constraint.state = SchemaState::NONE;
        let job = new_check_constraint_job(
            schema_id,
            schema,
            &stored,
            ActionType::ACTION_ADD_CHECK_CONSTRAINT,
            &context.0,
        );
        let args = GoShared::new(AddCheckConstraintArgs {
            constraint: GoField::new(Some(GoShared::new(constraint))),
        });
        (job, JobArgsValue::AddCheckConstraint(Some(args)))
    };
    Ok(tidb_ddl_jobsubmit::JobSpec {
        job,
        args,
        id_allocated: true,
    })
}

/// Go `executor.DropCheckConstraint` before `doDDLJob2`.
pub(crate) fn drop_check_constraint_job(
    catalog: &SessionExtendedInfoSchema,
    schema: &String,
    table: &String,
    name: &String,
    context: &DdlStatementContext,
) -> Result<tidb_ddl_jobsubmit::JobSpec, DdlPlanError> {
    let (job, args) = {
        let (schema_id, stored) = locate_table(&catalog, schema, table)?;
        let stored = stored.read();
        let constraint = stored
            .constraints
            .iter_deref()
            .find(|constraint| constraint.read().name.lowercase() == name.go_to_lower())
            .ok_or_else(|| {
                DdlPlanError::Admission(DdlAdmissionError::from_terror(
                    &tidb_dbterror::ERR_CONSTRAINT_NOT_FOUND.gen_with_stack_by_args(&[name.into()]),
                ))
            })?;
        let job = new_check_constraint_job(
            schema_id,
            schema,
            &stored,
            ActionType::ACTION_DROP_CHECK_CONSTRAINT,
            &context.0,
        );
        let args = GoShared::new(CheckConstraintArgs {
            constraint_name: GoField::new(constraint.read().name.clone()),
            enforced: GoField::new(false),
        });
        (job, JobArgsValue::CheckConstraint(Some(args)))
    };
    Ok(tidb_ddl_jobsubmit::JobSpec {
        job,
        args,
        id_allocated: true,
    })
}

/// Go `executor.AlterCheckConstraint` before `doDDLJob2`.
pub(crate) fn alter_check_constraint_job(
    catalog: &SessionExtendedInfoSchema,
    schema: &String,
    table: &String,
    name: &String,
    enforced: &bool,
    context: &DdlStatementContext,
) -> Result<tidb_ddl_jobsubmit::JobSpec, DdlPlanError> {
    let (job, args) = {
        let (schema_id, stored) = locate_table(&catalog, schema, table)?;
        let stored = stored.read();
        let constraint = stored
            .constraints
            .iter_deref()
            .find(|constraint| constraint.read().name.lowercase() == name.go_to_lower())
            .ok_or_else(|| {
                DdlPlanError::Admission(DdlAdmissionError::from_terror(
                    &tidb_dbterror::ERR_CONSTRAINT_NOT_FOUND.gen_with_stack_by_args(&[name.into()]),
                ))
            })?;
        let job = new_check_constraint_job(
            schema_id,
            schema,
            &stored,
            ActionType::ACTION_ALTER_CHECK_CONSTRAINT,
            &context.0,
        );
        let args = GoShared::new(CheckConstraintArgs {
            constraint_name: GoField::new(constraint.read().name.clone()),
            enforced: GoField::new(*enforced),
        });
        (job, JobArgsValue::CheckConstraint(Some(args)))
    };
    Ok(tidb_ddl_jobsubmit::JobSpec {
        job,
        args,
        id_allocated: true,
    })
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

/// The note Go's placement handlers append under `tidb_placement_mode =
/// 'IGNORE'` (`checkIgnorePlacementDDL`, `handleXxxPlacement`).
pub(crate) const PLACEMENT_IGNORED: &str = "Placement is ignored when TIDB_PLACEMENT_MODE is 'IGNORE'";

pub(crate) fn placement_ignored_note() -> tidb_context::SqlWarn {
    tidb_context::SqlWarn::with_code(
        WarningLevel::Note,
        tidb_errno::errcode::ErrUnknown,
        PLACEMENT_IGNORED,
    )
}

/// Go `checkIgnorePlacementDDL`: under `tidb_placement_mode = 'IGNORE'` a
/// placement statement changes nothing and notes so.
pub(crate) fn check_ignore_placement_ddl(
    context: &dyn DdlSessionContext,
) -> Option<add_index::AddIndexSubmission> {
    context
        .placement_mode_ignore()
        .then(|| add_index::AddIndexSubmission::AlreadySatisfied {
            detail: "placement is ignored".to_owned(),
            warnings: vec![placement_ignored_note()],
        })
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
    /// Go `deliveryJob`'s `jobW`, refreshed by the scheduler between steps.
    pub delivered_job: tidb_model::JobW,
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
    /// What a step reaches through Go's `*worker` (its session pool and
    /// store snapshots), which RECOVER needs. Bootstrap and tests supply
    /// the same session pool as ordinary delivery.
    pub worker: DdlStepWorker,
    /// The statement kv context of the step's transaction, which Go's
    /// `prepareTxn` tags with the job's SQL (`kv.ResourceGroupTagger`).
    /// `None` for a run without a transaction of its own.
    pub step_kv_context: Option<tidb_txnkv::StatementKvContextCell>,
    /// Go `jobContext.infoCache`: the schema a step reads beside its job's
    /// own schemas, which it reads from meta (`jobCtx.metaMut`). `None` reads
    /// the whole schema from the snapshot.
    pub info_cache: Option<std::sync::Arc<tidb_infoschema_issyncer::catalog_watch::InfoCache>>,
}

impl DdlJobSchemaState {
    /// Go `jobScheduler.deliveryJob`: start an owner run with its job and worker.
    pub fn new(mdl_enabled: bool, delivered_job: tidb_model::JobW, worker: DdlStepWorker) -> Self {
        Self {
            delivered_job,
            mdl_enabled,
            synced_version: None,
            pending_version: None,
            reorg: Vec::new(),
            worker,
            step_kv_context: None,
            info_cache: None,
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
        /// The job as its last step left it, which Go's
        /// `afterWaitSchemaSynced` call receives.
        job: Box<tidb_model::Job>,
    },
    /// One action or history transaction, planned from the current active row.
    Step(PersistedDdlJobStep),
    /// A reorganization the owner runs in its own transactions before it
    /// plans the job again (Go `runReorgJob`).
    Reorg(add_index::AddIndexReorg),
}

pub mod add_column;
pub mod add_index;
pub(crate) mod analyze;
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
pub mod flashback_cluster;
pub mod masking_policy;
pub mod materialized_view;
pub mod placement_policy;
pub mod rename_table;
pub mod repair_table;
pub mod reorganize_partition;
pub mod resource_group;
pub mod table_lock;
pub mod table_mode;
pub mod truncate;
mod rollingback;
pub(crate) mod recover_table;
pub use recover_table::{
    get_drop_or_truncate_table_info_from_jobs_by_store, get_gc_safe_point, get_recover_snapshot_ts, DdlStepWorker,
    SnapshotMetaAt,
};

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
            | ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG
            | ActionType::ACTION_ALTER_MATERIALIZED_VIEW_LOG_PURGE
            | ActionType::ACTION_CREATE_MATERIALIZED_VIEW
            | ActionType::ACTION_ALTER_MATERIALIZED_VIEW_REFRESH
            | ActionType::ACTION_ALTER_MATERIALIZED_VIEW_ATTRIBUTES
            | ActionType::ACTION_CREATE_TABLES
            | ActionType::ACTION_RECOVER_TABLE
            | ActionType::ACTION_FLASHBACK_CLUSTER
            | ActionType::ACTION_RECOVER_SCHEMA
            | ActionType::ACTION_RENAME_TABLE
            | ActionType::ACTION_RENAME_TABLES
            | ActionType::ACTION_DROP_SCHEMA
            | ActionType::ACTION_DROP_TABLE
            | ActionType::ACTION_DROP_VIEW
            | ActionType::ACTION_DROP_SEQUENCE
            | ActionType::ACTION_DROP_MATERIALIZED_VIEW_LOG
            | ActionType::ACTION_DROP_MATERIALIZED_VIEW
            | ActionType::ACTION_DROP_MATERIALIZED_VIEW_SHADOW
            | ActionType::ACTION_CREATE_MATERIALIZED_VIEW_SHADOW
            | ActionType::ACTION_MVIEW_REFRESH_OUT_OF_PLACE_CUTOVER
            | ActionType::ACTION_ADD_INDEX
            | ActionType::ACTION_ADD_PRIMARY_KEY
            | ActionType::ACTION_ADD_COLUMNAR_INDEX
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
            | ActionType::ACTION_CREATE_MASKING_POLICY
            | ActionType::ACTION_ALTER_MASKING_POLICY
            | ActionType::ACTION_DROP_MASKING_POLICY
            | ActionType::ACTION_LOCK_TABLE
            | ActionType::ACTION_UNLOCK_TABLE
            | ActionType::ACTION_ALTER_TABLE_MODE
            | ActionType::ACTION_REFRESH_META
            | ActionType::ACTION_REPAIR_TABLE
            | ActionType::ACTION_ADD_TABLE_PARTITION
            | ActionType::ACTION_DROP_TABLE_PARTITION
            | ActionType::ACTION_TRUNCATE_TABLE_PARTITION
            | ActionType::ACTION_EXCHANGE_TABLE_PARTITION
            | ActionType::ACTION_REORGANIZE_PARTITION
            | ActionType::ACTION_ALTER_TABLE_PARTITIONING
            | ActionType::ACTION_REMOVE_PARTITIONING
    ) || alter_table::ALTER_TABLE_ACTIONS.contains(&action)
}

/// Go `worker.transitOneJobStep`: recover the selected schema barrier,
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
    let sessions = &schema_state.worker.clone();
    plan_persisted_ddl_job_with(
        snapshot,
        ddl_job_id,
        start_ts,
        schema_state,
        load_error_count_limit,
        supports_persisted_ddl_job,
        |snapshot, catalog, active| {
            plan_persisted_action(
                snapshot,
                catalog,
                active,
                start_ts,
                &reorg_outcomes,
                sessions,
            )
        },
    )
}

/// Go `runOneJobStep`'s dispatch to the action handler, shared by a job and
/// by a multi-schema change's sub-jobs (run as proxy jobs). The handler runs
/// in the step's transaction over `snapshot` and writes through its
/// `meta.Mutator`; those writes lead the step's mutations, and a failed step
/// leaves none (Go's `transitOneJobStep` discards them).
fn plan_persisted_action<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    reorg_outcomes: &[add_index::ReorgOutcome],
    sessions: &DdlStepWorker,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    run_in_job_txn(snapshot, start_ts, sessions, |txn| {
        dispatch_persisted_action(txn, catalog, active, start_ts, reorg_outcomes, sessions)
    })
}

/// The step's session (Go `w.sess`) from the worker's pool and the step's
/// buffer.
fn step_session(worker: &DdlStepWorker) -> crate::job_txn::StepSession {
    crate::job_txn::StepSession {
        pool: std::sync::Arc::clone(&worker.sess_pool),
        buffer: worker.step_buffer.clone(),
        sql: std::sync::Arc::clone(&worker.step_sql),
        start_mode: worker.start_mode,
        step_ctx: std::sync::Arc::clone(&worker.step_ctx),
    }
}

/// Runs one handler in a step transaction over `snapshot`: its meta writes
/// lead the step's mutations, and a failed handler leaves none.
fn run_in_job_txn(
    snapshot: &mut dyn MetaSnapshot,
    start_ts: u64,
    worker: &DdlStepWorker,
    handler: impl FnOnce(&mut JobTxn<'_>) -> Result<PersistedDdlActionStep, DdlPlanError>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    run_in_job_txn_with(
        snapshot,
        start_ts,
        step_session(worker),
        worker.region_store.clone(),
        handler,
    )
}

/// [`run_in_job_txn`] with the step's session and region store given.
fn run_in_job_txn_with(
    snapshot: &mut dyn MetaSnapshot,
    start_ts: u64,
    session: crate::job_txn::StepSession,
    region_store: Option<std::sync::Arc<dyn tidb_kv::region_map::RegionStore>>,
    handler: impl FnOnce(&mut JobTxn<'_>) -> Result<PersistedDdlActionStep, DdlPlanError>,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let mut txn = JobTxn::new(snapshot, start_ts, session).with_region_store(region_store);
    let mut step = handler(&mut txn)?;
    let mut mutations = txn.into_mutations()?;
    mutations.append(&mut step.write.mutations);
    step.write.mutations = mutations;
    Ok(step)
}

fn dispatch_persisted_action(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    reorg_outcomes: &[add_index::ReorgOutcome],
    sessions: &DdlStepWorker,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    // A job of its own reads the latest outcome; a multi-schema change hands
    // each sub-job its own.
    let reorg_outcome = reorg_outcomes.last();
    let action = active.job.type_;
    match action {
        ActionType::ACTION_ADD_CHECK_CONSTRAINT
        | ActionType::ACTION_DROP_CHECK_CONSTRAINT
        | ActionType::ACTION_ALTER_CHECK_CONSTRAINT => {
            plan_persisted_check_constraint_job_step(txn, active, start_ts)
        }
        ActionType::ACTION_CREATE_SCHEMA => {
            plan_persisted_create_schema_job_step(txn, catalog, active)
        }
        ActionType::ACTION_CREATE_TABLE => create_table::on_create_table(
            txn,
            catalog,
            active,
            start_ts,
        ),
        ActionType::ACTION_MVIEW_REFRESH_OUT_OF_PLACE_CUTOVER => {
            materialized_view::on_refresh_materialized_view_complete_out_of_place_cutover(txn, active, start_ts, sessions)
        }
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW_SHADOW => {
            materialized_view::on_create_materialized_view_shadow(txn, catalog, active, start_ts)
        }
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG => {
            materialized_view::on_create_materialized_view_log(txn, catalog, active, start_ts, sessions)
        }
        ActionType::ACTION_ALTER_MATERIALIZED_VIEW_LOG_PURGE => {
            materialized_view::on_alter_materialized_view_log_purge(txn, active, start_ts)
        }
        ActionType::ACTION_ALTER_MATERIALIZED_VIEW_REFRESH => {
            materialized_view::on_alter_materialized_view_refresh(txn, active, start_ts)
        }
        ActionType::ACTION_ALTER_MATERIALIZED_VIEW_ATTRIBUTES => {
            materialized_view::on_alter_materialized_view_attributes(txn, active, start_ts)
        }
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW => materialized_view::on_create_materialized_view(
            txn,
            catalog,
            active,
            start_ts,
            reorg_outcome,
            sessions,
        ),
        ActionType::ACTION_CREATE_TABLES => {
            plan_persisted_create_tables_job_step(txn, catalog, active, start_ts)
        }
        ActionType::ACTION_RECOVER_TABLE => {
            recover_table::on_recover_table(txn, catalog, active, start_ts, sessions)
        }
        ActionType::ACTION_RECOVER_SCHEMA => {
            recover_table::on_recover_schema(txn, active, start_ts, sessions)
        }
        ActionType::ACTION_FLASHBACK_CLUSTER => {
            flashback_cluster::on_flashback_cluster(txn, catalog, active, sessions)
        }
        ActionType::ACTION_CREATE_VIEW => {
            create_object::on_create_view(txn, active, start_ts)
        }
        ActionType::ACTION_CREATE_SEQUENCE => {
            create_object::on_create_sequence(txn, active, start_ts)
        }
        ActionType::ACTION_ALTER_SEQUENCE => {
            create_object::on_alter_sequence(txn, active, start_ts)
        }
        ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE
        | ActionType::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT => {
            alter_schema::on_modify_schema_charset_and_collate(txn, active)
        }
        ActionType::ACTION_CREATE_PLACEMENT_POLICY => {
            placement_policy::on_create_placement_policy(
                txn, catalog, active,
            )
        }
        ActionType::ACTION_ALTER_PLACEMENT_POLICY => {
            placement_policy::on_alter_placement_policy(
                txn, catalog, active,
            )
        }
        ActionType::ACTION_DROP_PLACEMENT_POLICY => {
            placement_policy::on_drop_placement_policy(
                txn, catalog, active,
            )
        }
        ActionType::ACTION_CREATE_RESOURCE_GROUP => {
            resource_group::on_create_resource_group(txn, active)
        }
        ActionType::ACTION_ALTER_RESOURCE_GROUP => {
            resource_group::on_alter_resource_group(txn, catalog, active)
        }
        ActionType::ACTION_DROP_RESOURCE_GROUP => {
            resource_group::on_drop_resource_group(txn, catalog, active)
        }
        ActionType::ACTION_CREATE_MASKING_POLICY => {
            masking_policy::on_create_masking_policy(txn, catalog, active)
        }
        ActionType::ACTION_ALTER_MASKING_POLICY => {
            masking_policy::on_alter_masking_policy(txn, catalog, active)
        }
        ActionType::ACTION_DROP_MASKING_POLICY => {
            masking_policy::on_drop_masking_policy(txn, active)
        }
        ActionType::ACTION_LOCK_TABLE => {
            table_lock::on_lock_tables(txn, active, start_ts)
        }
        ActionType::ACTION_UNLOCK_TABLE => {
            table_lock::on_unlock_tables(txn, active, start_ts)
        }
        ActionType::ACTION_ADD_TABLE_PARTITION => {
            partition::on_add_table_partition(
                txn,
                active,
                start_ts,
            )
        }
        ActionType::ACTION_DROP_TABLE_PARTITION => {
            partition::on_drop_table_partition(
                txn,
                active,
                start_ts,
                reorg_outcome,
            )
        }
        ActionType::ACTION_REORGANIZE_PARTITION
        | ActionType::ACTION_ALTER_TABLE_PARTITIONING
        | ActionType::ACTION_REMOVE_PARTITIONING => {
            reorganize_partition::on_reorganize_partition(
                txn,
                active,
                start_ts,
                reorg_outcome,
            )
        }
        ActionType::ACTION_EXCHANGE_TABLE_PARTITION => {
            exchange_partition::on_exchange_table_partition(
                txn,
                active,
                reorg_outcome,
            )
        }
        ActionType::ACTION_TRUNCATE_TABLE_PARTITION => {
            partition::on_truncate_table_partition(
                txn,
                active,
                start_ts,
                reorg_outcome,
            )
        }
        ActionType::ACTION_RENAME_TABLE | ActionType::ACTION_RENAME_TABLES => {
            rename_table::on_rename_table(txn, catalog, active, start_ts)
        }
        ActionType::ACTION_ALTER_TABLE_MODE => {
            table_mode::on_alter_table_mode(txn, active, start_ts)
        }
        ActionType::ACTION_REFRESH_META => table_mode::on_refresh_meta(txn, active),
        ActionType::ACTION_REPAIR_TABLE => repair_table::on_repair_table(txn, active, start_ts),
        ActionType::ACTION_DROP_SCHEMA => {
            plan_persisted_drop_schema_job_step(txn, catalog, active)
        }
        ActionType::ACTION_DROP_TABLE
        | ActionType::ACTION_DROP_VIEW
        | ActionType::ACTION_DROP_SEQUENCE
        | ActionType::ACTION_DROP_MATERIALIZED_VIEW_LOG
        | ActionType::ACTION_DROP_MATERIALIZED_VIEW
        | ActionType::ACTION_DROP_MATERIALIZED_VIEW_SHADOW => {
            plan_persisted_drop_table_job_step(txn, catalog, active)
        }
        ActionType::ACTION_ADD_COLUMNAR_INDEX => {
            add_index::on_create_columnar_index(txn, active, start_ts)
        }
        ActionType::ACTION_ADD_INDEX | ActionType::ACTION_ADD_PRIMARY_KEY => {
            add_index::plan_persisted_add_index_job_step(
                txn,
                active,
                start_ts,
                reorg_outcome,
            )
        }
        ActionType::ACTION_DROP_INDEX | ActionType::ACTION_DROP_PRIMARY_KEY => {
            drop_index::on_drop_index(txn, catalog, active, start_ts)
        }
        ActionType::ACTION_TRUNCATE_TABLE => truncate::on_truncate_table(
            txn,
            catalog,
            active,
        ),
        ActionType::ACTION_ADD_COLUMN => {
            add_column::on_add_column(txn, catalog, active, start_ts, sessions)
        }
        ActionType::ACTION_DROP_COLUMN => {
            drop_column::on_drop_column(txn, catalog, active, start_ts)
        }
        ActionType::ACTION_MULTI_SCHEMA_CHANGE => {
            multi_schema::on_multi_schema_change(
                txn,
                catalog,
                active,
                start_ts,
                reorg_outcomes,
                sessions,
            )
        }
        ActionType::ACTION_ADD_FOREIGN_KEY => foreign_key::on_create_foreign_key(
            txn,
            catalog,
            active,
            start_ts,
            reorg_outcome,
        ),
        ActionType::ACTION_MODIFY_COLUMN => modify_column::on_modify_column(
            txn,
            catalog,
            active,
            start_ts,
            reorg_outcome,
        ),
        action if alter_table::ALTER_TABLE_ACTIONS.contains(&action) => {
            alter_table::plan_persisted_alter_table_job_step(txn, catalog, active, start_ts)
        }
        _ => unreachable!("supported action checked above"),
    }
}

/// Go `transitOneJobStepAndWaitSync`: recover the schema barrier before
/// entering `worker.transitOneJobStep` and opening its transaction.
pub fn schema_sync_before_step<S: MetaSnapshot>(
    snapshot: &mut S,
    schema_state: &DdlJobSchemaState,
    job: &tidb_model::Job,
) -> Result<Option<PersistedDdlJobPlan>, DdlPlanError> {
    // Go's barrier reads no schema at the step's snapshot.
    let active = tidb_ddl_systable::ddl_job_table::ActiveDdlJob::from_job(job.clone());
    recover_schema_before_step(snapshot, &active, schema_state, true).map(|(_, sync)| sync)
}

fn recover_schema_before_step<S: MetaSnapshot>(
    snapshot: &mut S,
    active: &tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    schema_state: &DdlJobSchemaState,
    check_barrier: bool,
) -> Result<(Option<MdlInfoUpdate>, Option<PersistedDdlJobPlan>), DdlPlanError> {
    let ddl_job_id = active.job.id;
    let mdl_info = if schema_state.mdl_enabled {
        let info = MdlInfoUpdate {
            omit_owner_id: tidb_metadef::is_system_related_db(&active.job.schema_name.to_string().go_to_lower()),
        };
        if !check_barrier { return Ok((Some(info), None)); }
        // Go `sysTblMgr.GetMDLVer` on the scheduler's session pool.
        let pool = schema_state.worker.sess_pool.sess_pool().ok_or_else(||
            DdlPlanError::Encode("the DDL session pool has no sessions".to_owned()))?;
        let mdl_ver = tidb_ddl_systable::Manager::get_mdl_ver(
            &tidb_ddl_systable::SystemTableManager::new(pool), ddl_job_id,
        );
        match mdl_ver {
            Ok(version) if Some(version) != schema_state.synced_version => {
                return Ok((None, Some(PersistedDdlJobPlan::SchemaSync {
                    version,
                    mdl_info: Some(info),
                    publish_version: false,
                    job: Box::new(active.job.clone()),
                })));
            }
            Ok(_) => {}
            Err(tidb_ddl_systable::SystemTableManagerError::NotFound) => {}
            Err(error) => return Err(DdlPlanError::Encode(error.to_string())),
        }
        Some(info)
    } else {
        if !check_barrier { return Ok((None, None)); }
        // Go waitVersionSyncedWithoutMDL recovers from a fresh snapshot, not
        // from an MDL row or merely the job's possibly older LastSchemaVersion.
        let can_sync = matches!(
            active.job.state,
            JobState::RUNNING | JobState::ROLLINGBACK | JobState::DONE | JobState::ROLLBACK_DONE
        );
        if can_sync {
            if let Some(version) = schema_state.pending_version {
                return Ok((None, Some(PersistedDdlJobPlan::SchemaSync {
                    version,
                    mdl_info: None,
                    publish_version: false,
                    job: Box::new(active.job.clone()),
                })));
            }
            if schema_state.synced_version.is_none() && active.job.last_schema_version > 0 {
                // Go `waitVersionSyncedWithoutMDL`: the fresh snapshot's
                // `GetSchemaVersionWithNonEmptyDiff`.
                let version =
                    tidb_infoschema_issyncer::catalog_reload::get_schema_version_with_non_empty_diff(snapshot)?;
                if version > 0 {
                    return Ok((None, Some(PersistedDdlJobPlan::SchemaSync {
                        version,
                        mdl_info: None,
                        publish_version: true,
                        job: Box::new(active.job.clone()),
                    })));
                }
            }
        }
        None
    };
    Ok((mdl_info, None))
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
        &SessionExtendedInfoSchema,
        &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    ) -> Result<PersistedDdlActionStep, DdlPlanError>,
) -> Result<PersistedDdlJobPlan, DdlPlanError> {
    // Go `transitOneJobStep`: the schema from `jobCtx.infoCache` at the
    // step's version; the handlers read the job's own objects through
    // `jobCtx.metaMut`.
    // The handlers read tables through Go's V2 lookups, so the step plans
    // against the cached image itself rather than a materialized copy.
    let catalog = match &schema_state.info_cache {
        Some(cache) => cache.load_with_ts(snapshot, cache.capacity())?,
        None => std::sync::Arc::new(load_info_schema(snapshot)?),
    };
    let job_w = &schema_state.delivered_job;
    let mut active = tidb_ddl_systable::ddl_job_table::ActiveDdlJob::from_job(
        job_w.job.as_ref().expect("delivered job").read().clone(),
    );
    let result = (|| {
    if !supports_action(active.job.type_) {
        return Err(DdlPlanError::Encode(format!(
            "unsupported persisted DDL action {}",
            active.job.type_
        )));
    }
    let (mdl_info, schema_sync) = recover_schema_before_step(
        snapshot,
        &active,
        &schema_state,
        false,
    )?;
    if let Some(schema_sync) = schema_sync { return Ok(schema_sync); }
    // Go `worker.transitOneJobStep`: compare the exact bytes on w.sess,
    // but execute the delivered object, never a decoded transaction row.
    let session = step_session(&schema_state.worker);
    let sql = format!("select job_meta from mysql.tidb_ddl_job where job_id = {ddl_job_id}");
    let rows = session.execute(start_ts, &sql, "get-job-by-id", &[]).map_err(DdlPlanError::from_mysql_error)?;
    let bytes = rows.first().and_then(|row| row.first())
        .ok_or_else(|| DdlPlanError::Encode("not found".to_owned()))?;
    if bytes.go_bytes() != job_w.bytes.snapshot() {
        return Err(DdlPlanError::Encode("job meta changed by others".to_owned()));
    }
    // Go `transitOneJobStep` -> `prepareTxn`'s `attachTopProfilingInfo`:
    // every step, finishing ones included, runs under the job's SQL.
    crate::job_ctx::attach_top_profiling_info(active.job.id, &active.job.query.to_utf8_lossy_go());
    if let Some(step_kv_context) = &schema_state.step_kv_context {
        // Go `prepareTxn`: `setDDLSourceForDiagnosis`, then the
        // transaction's `kv.RequestSourceType` and `kv.ResourceGroupTagger`.
        let source_type = crate::job_ctx::set_ddl_source_for_diagnosis(active.job.id, active.job.type_);
        step_kv_context.publish(tidb_txnkv::StatementKvContext {
            resource_group_tagger: crate::job_ctx::get_resource_group_tagger_for_top_sql(active.job.id),
            request_source_type: Some(source_type),
            resource_group_name: Some(crate::job_ctx::job_resource_group_name(
                active.job.id,
                active
                    .job
                    .reorg_meta
                    .as_ref()
                    .map(|meta| meta.read().resource_group_name.to_utf8_lossy_go())
                    .as_deref(),
            )),
            ..tidb_txnkv::StatementKvContext::default()
        });
    }
    if matches!(
        active.job.state,
        JobState::DONE | JobState::ROLLBACK_DONE | JobState::CANCELLED
    ) {
        // Go `transitOneJobStep`'s done branch, after `checkBeforeCommit`.
        if active.job.state == JobState::DONE {
            match active.job.type_ {
                ActionType::ACTION_MVIEW_REFRESH_OUT_OF_PLACE_CUTOVER => {
                    materialized_view::cleanup_mview_out_of_place_cutover_after_commit(&active.job, &session, start_ts);
                }
                ActionType::ACTION_TRUNCATE_TABLE => {
                    truncate::cleanup_truncate_table_affinity_groups_after_commit(&mut active.job);
                }
                ActionType::ACTION_ALTER_TABLE_AFFINITY => {
                    alter_table::cleanup_alter_table_affinity_groups_after_commit(&mut active.job);
                }
                _ => {}
            }
        }
        // Go `finishDDLJob`'s `finishRecoverTable` / `finishRecoverSchema` /
        // `finishFlashbackCluster`.
        recover_table::finish_recover(&mut active.job, &schema_state.worker)?;
        flashback_cluster::finish_flashback_cluster(&mut active.job, &schema_state.worker)?;
        return finish_persisted_ddl_job(
            &mut active,
            start_ts,
            &session,
        )
        .map(PersistedDdlJobPlan::Step);
    }
    // Go `transitOneJobStep`, before `runOneJobStep`.
    tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", &mut active.job);

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
        let mutations = Vec::new();
        update_ddl_job_2_table(&session, start_ts, &mut active.job, false)?;
        return Ok(PersistedDdlJobPlan::Step(PersistedDdlJobStep {
            job: Box::new(active.job.clone()),
            write: job_only_write(ddl_job_id, mutations),
            terminal: false,
            run_error: None,
            error_count: active.job.error_count,
        }));
    }
    let cancelling = active.job.is_cancelling();
    let was_rollingback = active.job.is_rollingback();
    let previous_schema_version = active.job.last_schema_version;
    let step_state = snapshot_job_step_state(&active.job);
    let outcome = recover_ddl_action(|| {
        if cancelling {
            run_in_job_txn(snapshot, start_ts, &schema_state.worker, |txn| {
                plan_cancel_persisted_ddl_job(txn, &catalog, &mut active, start_ts)
            })
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
    // Go `transitOneJobStep`, after `runOneJobStep`.
    tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", &mut active.job);
    // Go `transitOneJobStep`: an errored or cancelled step whose handler set
    // `mustRollbackTxnOnError` discards all its mutations, its session's
    // statements and a rollback's included.
    if (step.run_error.is_some() || active.job.state == JobState::CANCELLED)
        && schema_state.worker.step_ctx.must_rollback_txn_on_error.load(std::sync::atomic::Ordering::Acquire)
    {
        step.write = job_only_write(ddl_job_id, Vec::new());
        schema_state.worker.step_buffer.reset();
        if active.job.state == JobState::CANCELLED
            || active.job.is_rollingback()
            || active.job.is_rollback_done()
        {
            // Preserve the sub-job states selected for cancellation or
            // rollback.
            step_state.restore_kv(&mut active.job);
        } else {
            step_state.restore_all(&mut active.job, step.run_error.as_ref());
            step.update_raw_args = false;
        }
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/ddl/afterRollbackTxnForMustRollbackTxnOnError",
            &mut active.job,
        );
    }
    // Go resets the action's transaction before handleJobDone on cancellation.
    // This also covers rollback handlers that cancel without a fresh error.
    if active.job.state == JobState::CANCELLED {
        // Go: the PD groups go before the cancellation commits; if the commit
        // fails, the job runs again and repeats this idempotent cleanup. The
        // step's own writes are discarded, so this reads the metadata the
        // cancellation persists.
        rollingback::delete_affinity_groups_of_cancelled_job(
            &mut JobTxn::new(snapshot, start_ts, step_session(&schema_state.worker)),
            &mut active.job,
        );
        // Go `finishDDLJob`'s `finishRecoverTable` / `finishRecoverSchema` /
        // `finishFlashbackCluster`.
        recover_table::finish_recover(&mut active.job, &schema_state.worker)?;
        flashback_cluster::finish_flashback_cluster(&mut active.job, &schema_state.worker)?;
        return finish_persisted_ddl_job(
            &mut active,
            start_ts,
            &session,
        )
        .map(PersistedDdlJobPlan::Step);
    }
    // Go transitOneJobStep discards failed forward metadata, but keeps a
    // handled rollback's mutations. The job/error checkpoint survives both.
    if step.run_error.is_some() && !active.job.is_rollingback() && !active.job.is_rollback_done() {
        step.write = job_only_write(ddl_job_id, Vec::new());
        // The statements the failed handler ran on `w.sess` go too.
        schema_state.worker.step_buffer.reset();
        // The job's in-memory progress must match the discarded step.
        step_state.restore_all(&mut active.job, step.run_error.as_ref());
        step.update_raw_args = false;
    }
    let mut update_err = update_ddl_job_2_table(&session, start_ts, &mut active.job, step.update_raw_args).err();
    tidb_failpoint::inject_call_ref::<AfterUpdateJobToTable>(
        "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable",
        |f| f(&active.job, &mut update_err),
    );
    if let Some(error) = update_err {
        return Err(error);
    }
    // Only a new diff publishes a schema version; cancellation is not a
    // publication of whatever catalog version happened to be read.
    step.write.schema_version = step.write.diff.version;
    if step.write.schema_version != 0 {
        step.write.mdl_info_update = mdl_info;
    }
    Ok(PersistedDdlJobPlan::Step(PersistedDdlJobStep {
        job: Box::new(active.job.clone()),
        write: step.write,
        terminal: false,
        run_error: step.run_error,
        error_count: active.job.error_count,
    }))
    })();
    *job_w.job.as_ref().expect("delivered job").write() = active.job;
    result
}

pub(crate) use tidb_ddl_jobsubmit::wrap_key_2_string;

/// Go `snapshotJobStepState`: the job's metadata progress, which must match
/// the transaction the step's outcome persists.
struct JobStepState {
    state: JobState,
    schema_state: SchemaState,
    binlog_info: Option<GoShared<tidb_model::HistoryInfo>>,
    resume_reason: Option<GoShared<tidb_model::JobResumeReason>>,
    sub_jobs: Option<Vec<Option<tidb_model::SubJob>>>,
}

fn snapshot_job_step_state(job: &Job) -> JobStepState {
    JobStepState {
        state: job.state,
        schema_state: job.schema_state,
        binlog_info: job
            .binlog_info
            .as_ref()
            .map(|binlog| GoShared::new(binlog.read().clone())),
        resume_reason: job.resume_reason.clone(),
        sub_jobs: job.multi_schema_info.as_ref().map(|info| {
            info.read()
                .sub_jobs
                .handles()
                .into_iter()
                .map(|sub| sub.map(|sub| sub.read().clone_without_args()))
                .collect()
        }),
    }
}

impl JobStepState {
    /// Go `restoreKV`: leaves the sub-job cancellation decisions intact.
    fn restore_kv(&self, job: &mut Job) {
        // ALTER SCHEMA READ ONLY records the attempted change in its schema
        // state so cancellation can run the rollback even after a failed
        // step.
        if job.type_ != ActionType::ACTION_MODIFY_SCHEMA_READ_ONLY {
            job.schema_state = self.schema_state;
        }
        job.binlog_info = self.binlog_info.clone();
        job.resume_reason = self.resume_reason.clone();
        job.last_schema_version = 0;
    }

    /// Go `restoreAll`: also restores completion and sub-job progress so
    /// the step can be retried.
    fn restore_all(&self, job: &mut Job, run_error: Option<&DdlPlanError>) {
        self.restore_kv(job);
        if self.state == JobState::RUNNING && job.is_done() {
            job.state = self.state;
        }
        let (Some(info), Some(saved)) = (job.multi_schema_info.as_ref(), self.sub_jobs.as_ref()) else {
            return;
        };
        let mut saved = saved.clone();
        if matches!(
            run_error,
            Some(DdlPlanError::Source(error))
                if tidb_meta::meta_errors::ERR_DDL_REORG_ELEMENT_NOT_EXIST.equal(Some(error))
        ) {
            // Keep the recovery reset from `getReorgInfo` so the next step
            // recreates the missing handle.
            for (saved, current) in saved.iter_mut().zip(info.read().sub_jobs.handles()) {
                if let (Some(saved), Some(current)) = (saved.as_mut(), current) {
                    if saved.snapshot_version != 0 && current.read().snapshot_version == 0 {
                        saved.snapshot_version = 0;
                    }
                }
            }
        }
        info.write().sub_jobs = tidb_model::GoSharedPointerSlice::from_handles(
            saved.into_iter().map(|sub| sub.map(GoShared::new)).collect(),
        );
    }
}

/// Go's `afterUpdateJobToTable` call-failpoint: the job and the error of its
/// row update, which the test may replace (Go `job, &err`).
pub type AfterUpdateJobToTable = dyn Fn(&Job, &mut Option<DdlPlanError>) + Send + Sync;

/// Go `updateDDLJob2Table(ctx, w.sess, job, updateRawArgs)` on the step's
/// session, opened at the step's start timestamp.
fn update_ddl_job_2_table(
    session: &crate::job_txn::StepSession,
    start_ts: u64,
    job: &mut Job,
    update_raw_args: bool,
) -> Result<(), DdlPlanError> {
    let se = session.sess(start_ts).map_err(DdlPlanError::from_mysql_error)?;
    crate::ddl_job_scheduler::update_ddl_job_2_table(&se, job, update_raw_args)
        .map_err(DdlPlanError::from_mysql_error)
}

// Go handleJobDone/finishDDLJob. Cancellation calls this in its current
// transaction after discarding action mutations; DONE/ROLLBACK_DONE reach it
// only after the shared MDL barrier. No action handler writes history itself.
#[allow(clippy::too_many_arguments)]
fn finish_persisted_ddl_job(
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
    session: &crate::job_txn::StepSession,
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
    // Go `finishDDLJob` -> `addDelRangeJob`: the keys the job left
    // behind, recorded before it moves to history, on a pooled session
    // in their own transactions.
    if crate::ddl_delete_range::job_need_gc(&active.job) {
        crate::ddl_delete_range::add_del_range_job(session.pool.as_ref(), &active.job)?;
    }
    if active.job.type_ == ActionType::ACTION_ALTER_NO_CACHE_TABLE
        && !active.job.is_cancelled()
    {
        session
            .execute(
                start_ts,
                &format!(
                    "delete from mysql.table_cache_meta where tid = {}",
                    active.job.table_id
                ),
                "alter_table_nocache_cleanup",
                &[],
            )
            .map_err(DdlPlanError::from_mysql_error)?;
    }
    // Go `deleteDDLJob`, then `AddHistoryDDLJob`, on `w.sess`.
    session
        .execute(
            start_ts,
            &format!("delete from mysql.tidb_ddl_job where job_id = {}", active.job.id),
            "delete_job",
            &[],
        )
        .map_err(DdlPlanError::from_mysql_error)?;
    add_history_ddl_job(session, start_ts, &active.job, encoded, &mut mutations)?;
    Ok(PersistedDdlJobStep {
        job: Box::new(active.job.clone()),
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
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let ddl_job_id = active.job.id;

    let stored = checked_job_table(txn, active, true)?;
    let stored = stored.read();
    let database = job_schema_with_tables(txn, active.job.schema_id)?
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
                            tidb_dbterror::CLASS_SCHEMA
                                .new_std(tidb_errno::errcode::ErrDupFieldName)
                                .gen_with_stack_by_args(&[FormatArg::from(
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
                && database.read().deprecated_tables.iter_deref().any(|table| {
                    let table = table.read();
                    table
                        .constraints
                        .iter_deref()
                        .any(|constraint| constraint.read().name.lowercase() == wanted)
                })
            {
                return Err(cancel_ddl_job(
                    active,
                    DdlPlanError::from(
                        tidb_dbterror::CLASS_SCHEMA
                            .new_std(tidb_errno::errcode::ErrCheckConstraintDupName)
                            .gen_with_stack_by_args(&[FormatArg::from(wanted.as_str())]),
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
                    if database.read().deprecated_tables.iter_deref().any(|table| {
                        let table = table.read();
                        table
                            .constraints
                            .iter_deref()
                            .any(|existing| existing.read().name.lowercase() == wanted)
                    }) {
                        return Err(DdlPlanError::from(
                            tidb_dbterror::CLASS_SCHEMA
                                .new_std(tidb_errno::errcode::ErrCheckConstraintDupName)
                                .gen_with_stack_by_args(&[FormatArg::from(
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
                                tidb_dbterror::ERR_TABLE_CHECK_CONSTRAINT_REFER_UNKNOWN
                                    .gen_with_stack_by_args(&[
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
                                schema: database.read().name.original().to_owned(),
                                table: Box::new(info.clone_like_go()),
                                constraint_name,
                            });
                            terminal = true;
                        }
                        state => {
                            return Err(DdlPlanError::from(
                                tidb_dbterror::ERR_INVALID_DDL_STATE
                                    .gen_with_stack_by_args(&[
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
                            tidb_dbterror::ERR_CONSTRAINT_NOT_FOUND
                                .gen_with_stack_by_args(&[FormatArg::from(
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
                            tidb_dbterror::ERR_INVALID_DDL_JOB.gen_with_stack_by_args(
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
                            tidb_dbterror::ERR_CONSTRAINT_NOT_FOUND
                                .gen_with_stack_by_args(&[FormatArg::from(
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
                            schema: database.read().name.original().to_owned(),
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

    // Go `updateVersionAndTableInfo`; a step that changed nothing only
    // checkpoints the job.
    let (schema_version, diff) = if schema_changed {
        let database_id = database.read().id;
        let write = add_index::update_version_and_table_info(txn, active, database_id, &info, true, start_ts)?;
        (write.schema_version, write.diff)
    } else {
        (0, SchemaDiff::default())
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
            // The step's meta writes are its transaction's.
            mutations: Vec::new(),
            schema_version,
            diff,
            created_id: None,
            check_constraint_validation: validation,
            mdl_info_update: None,
            label_rules: Vec::new(),
            warnings: Vec::new(),
            placement_bundles: Vec::new(),
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
/// Go `updateSchemaVersion`: the job's next schema version from meta's
/// counter (`setSchemaVersion` -> `GenSchemaVersion`), stamped on `diff`
/// and stored (`SetSchemaDiff`).
pub(super) fn update_schema_version(
    meta: &JobMeta<'_, '_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    mut diff: SchemaDiff,
) -> Result<(i64, SchemaDiff), DdlPlanError> {
    // Go `failpoint.InjectCall("beforeUpdateSchemaVersion", job, &err)`:
    // the callback reads the job and may set the error.
    let mut call = (active.job.clone(), None::<String>);
    tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/beforeUpdateSchemaVersion", &mut call);
    if let Some(error) = call.1 {
        return Err(DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(error)));
    }
    let meta_error = |error| job_meta_error(meta, error);
    let schema_version = meta.gen_schema_version().map_err(meta_error)?;
    diff.version = schema_version;
    meta.set_schema_diff(&diff).map_err(meta_error)?;
    active.job.last_schema_version = schema_version;
    Ok((schema_version, diff))
}

/// Go `onCreateSchema`.
fn plan_persisted_create_schema_job_step(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
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
        .schema_by_id(db_info.id).is_some();
    if name_taken || id_taken {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_dbterror::CLASS_SCHEMA
                    .new_std(tidb_errno::errcode::ErrDBCreateExists)
                    .generate(format!(
                        "Can't create database '{}'; database exists",
                        db_info.name.original()
                    )),
            ),
        ));
    }

    let meta = txn.meta();
    let action_type = active.job.type_;
    let (schema_version, diff) = update_schema_version(
        &meta,
        active,
        SchemaDiff {
            action_type,
            schema_id: db_info.id,
            ..SchemaDiff::default()
        },
    )?;
    // none -> public
    db_info.state = SchemaState::PUBLIC;
    meta.create_database(&db_info)
        .map_err(|error| job_meta_error(&meta, error))?;

    active.job.finish_db_job(
        JobState::DONE,
        SchemaState::PUBLIC,
        schema_version,
        Some(GoShared::new(db_info.clone())),
    );

    Ok(PersistedDdlActionStep {
        write: DdlWrite {
            ddl_job_id,
            // The step's meta writes are its transaction's.
            mutations: Vec::new(),
            schema_version,
            diff,
            created_id: Some(db_info.id),
            check_constraint_validation: None,
            mdl_info_update: None,
            label_rules: Vec::new(),
            warnings: Vec::new(),
            placement_bundles: Vec::new(),
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
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    start_ts: u64,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let ddl_job_id = active.job.id;

    // Pinned Go `GetBatchCreateTableArgs`: the v2 envelope is
    // `{"tables": [{"table_info": <Go TableInfo JSON>, "fk_check": bool}]}`
    // decoded through the same serde surface as the single-table args.
    let args = tidb_model::get_batch_create_table_args(&mut active.job)
        .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
        .ok_or_else(|| DdlPlanError::Encode("CREATE TABLES job has nil args".to_owned()))?;

    let database = job_schema_with_tables(txn, active.job.schema_id)?
        .ok_or_else(|| {
            cancel_ddl_job(
                active,
                DdlPlanError::from(
                    tidb_dbterror::CLASS_SCHEMA
                        .new_std(tidb_errno::errcode::ErrBadDB)
                        .generate(format!(
                            "Unknown database '(Schema ID {})'",
                            active.job.schema_id
                        )),
                ),
            )
        })?;

    let handles: Vec<Option<GoShared<tidb_model::CreateTableArgs>>> =
        args.read().tables.get().handles();
    let mut tables = Vec::new();
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
        tables.push((info_handle, table_args.read().fk_check.get()));
    }

    if let Some(reason) = cancel_reason {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_dbterror::CLASS_SCHEMA
                    .new_std(tidb_errno::errcode::ErrTableExists)
                    .generate(reason),
            ),
        ));
    }

    let database_id = database.read().id;
    let schema = database.read().name.original().to_owned();
    let mut placement_bundles = Vec::new();
    let mut table_infos = Vec::new();
    // Go: a stub job per table runs `createSequenceWithCheck` or
    // `createTable`; any failure cancels the batch.
    for (handle, fk_check) in tables {
        let mut info = handle.read().clone_like_go();
        if info.sequence.is_some() {
            create_object::create_sequence_with_check(txn, active, database_id, &mut info, start_ts)
        } else {
            create_table::create_table(
                txn,
                catalog,
                &database,
                active,
                &mut info,
                fk_check,
                start_ts,
                &mut placement_bundles,
            )
        }
        .map_err(|error| cancel_ddl_job(active, error))?;
        *handle.write() = info.clone_like_go();
        table_infos.push(info);
    }
    let first_id = table_infos.first().map(|info| info.id).unwrap_or(0);
    let meta = txn.meta();
    // Go `SetSchemaDiffForCreateTables` (`pkg/ddl/schema_version.go:36`): one
    // `AffectedOption` per created table, each naming its schema and table
    // IDs on both sides -- Go's `applyCreateTables` applies the batch to
    // every peer's infoschema strictly through these options.
    let action_type = active.job.type_;
    let (schema_version, diff) = update_schema_version(
        &meta,
        active,
        SchemaDiff {
            action_type,
            schema_id: database_id,
            table_id: first_id,
            affected_options: GoSharedPointerSlice::from(
                table_infos
                    .iter()
                    .map(|info| tidb_model::AffectedOption {
                        schema_id: database_id,
                        old_schema_id: database_id,
                        table_id: info.id,
                        old_table_id: info.id,
                        ..tidb_model::AffectedOption::default()
                    })
                    .collect::<Vec<_>>(),
            ),
            ..SchemaDiff::default()
        },
    )?;
    // Go `asyncNotifyEvent` per table, the table's position its sub-job id.
    let mut mutations = Vec::new();
    if !tidb_metadef::is_mem_or_sys_db(&schema.go_to_lower()) {
        let events: Vec<_> = table_infos
            .iter()
            .enumerate()
            .map(|(position, info)| {
                (position as i64, SchemaChangeEvent::create_table(info.clone_like_go()))
            })
            .collect();
        append_schema_change_mutations(txn, ddl_job_id, &events)?;
    }

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
            check_constraint_validation: None,
            mdl_info_update: None,
            label_rules: Vec::new(),
            warnings: Vec::new(),
            placement_bundles,
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
fn plan_persisted_drop_schema_job_step(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let ddl_job_id = active.job.id;
    let mut mutations = Vec::new();

    let Some(database) = job_schema(txn, active.job.schema_id)?
        .map(|database| database.read().clone())
    else {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_dbterror::CLASS_SCHEMA
                    .new_std(tidb_errno::errcode::ErrDBDropExists)
                    .generate("Can't drop database ''; database doesn't exist"),
            ),
        ));
    };

    let mut db_info = database;
    let mut label_rules = LabelRuleSteps::new();
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
        let tables = job_schema_tables(txn, db_info.id)?
            .iter()
            .map(LabelRuleTable::from_table_info)
            .collect::<Vec<_>>();
        label_rules.push(
            label_rules::drop_schema_label_rules(&active.job.schema_name.to_string(), &tables),
            None,
        )?;
    }
    let meta = txn.meta();
    let meta_error = |error| job_meta_error(&meta, error);
    let action_type = active.job.type_;
    let (schema_version, diff) = update_schema_version(
        &meta,
        active,
        SchemaDiff {
            action_type,
            schema_id: db_info.id,
            ..SchemaDiff::default()
        },
    )?;
    match db_info.state {
        SchemaState::PUBLIC => db_info.state = SchemaState::WRITE_ONLY,
        SchemaState::WRITE_ONLY => db_info.state = SchemaState::DELETE_ONLY,
        SchemaState::DELETE_ONLY => db_info.state = SchemaState::NONE,
        _ => {}
    }
    meta.update_database(&db_info).map_err(meta_error)?;
    if db_info.state == SchemaState::NONE {
        // Go `Mutator.DropDatabase` is `HClear(DB:<id>)` then
        // `HDel(DBs, DB:<id>)`: every field of the database's hash, its
        // tables' metadata and allocators, goes; the tables' rows are left
        // to the delete range.
        // Go `metaMut.ListTables(job.SchemaID)`.
        let tables = meta.tables(db_info.id).map_err(meta_error)?;
        meta.drop_database(db_info.id).map_err(meta_error)?;
        // Go `onDropSchema`'s `collectTableAffinityGroupIDs` and
        // `batchDeleteTableAffinityGroups`: best effort, logged.
        let mut affinity_group_ids = std::collections::BTreeSet::new();
        let mut affinity_err = None;
        for table in &tables {
            if table.affinity.is_some() && affinity_err.is_none() {
                affinity_err = crate::ddl_affinity::collect_table_affinity_group_ids(table, &mut affinity_group_ids).err();
            }
        }
        let affinity_err = match affinity_err {
            None => crate::ddl_affinity::batch_delete_table_affinity_groups(&affinity_group_ids).err(),
            error => error,
        };
        if let Some(error) = affinity_err {
            tidb_ddl_logutil::ddl_logger().warn(
                "failed to delete affinity groups for batch tables, but operation will continue",
                &[
                    tidb_log::Field::new("error", tidb_log::Value::Str(error)),
                    tidb_log::Field::new("databaseID", tidb_log::Value::I64(db_info.id)),
                ],
            );
        }
        // Go `onDropSchema`'s `dropMaskingPoliciesByDBName`: a failure is
        // logged and the drop continues.
        let schema_name = active.job.schema_name.to_string();
        if let Err(error) = masking_policy::drop_masking_policies_by_db_name(txn, &schema_name) {
            tidb_ddl_logutil::ddl_logger().warn(
                "failed to delete masking policies for database, but operation will continue",
                &[
                    tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                    tidb_log::Field::new("dbName", tidb_log::Value::Str(schema_name)),
                ],
            );
        }
        // Go master `onDropSchema`: "Refresh and purge records drive MView
        // and MLog maintenance, so their cleanup must roll back DROP DATABASE
        // on failure. Refresh alerts are advisory state, so retaining stale
        // alerts is preferable to blocking DDL."
        for table in tables.iter().filter(|table| table.materialized_view.is_some()) {
            materialized_view::delete_create_materialized_view_refresh_info(txn, table.id)?;
        }
        for table in tables.iter().filter(|table| table.materialized_view.is_some()) {
            if let Err(error) = materialized_view::delete_create_materialized_view_refresh_alert(txn, table.id) {
                tidb_ddl_logutil::ddl_logger().warn(
                    "drop schema: failed to delete materialized view refresh alerts",
                    &[
                        tidb_log::Field::new("schemaName", tidb_log::Value::Str(active.job.schema_name.to_string())),
                        tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                    ],
                );
            }
        }
        for table in tables.iter().filter(|table| table.materialized_view_log.is_some()) {
            materialized_view::delete_materialized_view_log_purge_info(txn, table.id)?;
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
            append_schema_change_mutations(txn, ddl_job_id, &events)?;
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
fn plan_persisted_drop_table_job_step(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
) -> Result<PersistedDdlActionStep, DdlPlanError> {
    let ddl_job_id = active.job.id;
    let mut mutations = Vec::new();

    let mut table = checked_job_table(txn, active, false)?.read().clone_like_go();
    let database = job_schema(txn, active.job.schema_id)?
        .expect("checked_job_table checked the database");

    // Go master: a DROP TABLE of a table with materialized view metadata is
    // refused, as the executor refuses it.
    if table.state == SchemaState::PUBLIC && active.job.type_ == ActionType::ACTION_DROP_TABLE {
        if let Err(error) = materialized_view::check_table_materialized_view_constraints(&table, "DROP TABLE") {
            return Err(cancel_ddl_job(active, DdlPlanError::from_mysql_error(error)));
        }
    }
    // Go master: a log the view's own table got after the executor's check.
    if table.state == SchemaState::PUBLIC && active.job.type_ == ActionType::ACTION_DROP_MATERIALIZED_VIEW {
        if let Err(error) =
            materialized_view::check_base_table_materialized_view_dependency_constraints(&table, "DROP MATERIALIZED VIEW")
        {
            return Err(cancel_ddl_job(active, DdlPlanError::from_mysql_error(error)));
        }
    }
    if table.state == SchemaState::PUBLIC && active.job.type_ == ActionType::ACTION_DROP_TABLE {
        // Go `checkDropTableHasForeignKeyReferredInOwner`.
        let args = tidb_model::get_drop_table_args(&mut active.job)
            .map_err(|error| cancel_ddl_job(active, DdlPlanError::Encode(error.to_string())))?
            .unwrap_or_else(|| GoShared::new(tidb_model::DropTableArgs::default()));
        // DROP TABLE jobs commit independently. Only a self-reference is
        // safe to ignore; another table named in the statement may survive a
        // later job.
        let fk_check = args.read().fk_check;
        let schema_name = active.job.schema_name.to_string();
        let table_name = active.job.table_name.to_string();
        let ignored = [(schema_name.clone(), table_name.clone())];
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
                    &tidb_dbterror::ERR_TRUNCATE_ILLEGAL_FOREIGN_KEY,
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
    if table.state == SchemaState::PUBLIC
        && matches!(active.job.type_, ActionType::ACTION_DROP_TABLE | ActionType::ACTION_DROP_MATERIALIZED_VIEW_LOG)
    {
        materialized_view::check_drop_materialized_view_log_has_no_dependent_mvs(txn, active, &table)?;
    }
    match table.state {
        SchemaState::PUBLIC => table.state = SchemaState::WRITE_ONLY,
        SchemaState::WRITE_ONLY => table.state = SchemaState::DELETE_ONLY,
        SchemaState::DELETE_ONLY => table.state = SchemaState::NONE,
        _ => {}
    }
    // Go master `updateMaterializedViewBaseInfoOnDrop`: the base tables
    // forget the dropped log, in the same version.
    let extra_tables = if table.state != SchemaState::NONE {
        Vec::new()
    } else {
        materialized_view::update_materialized_view_base_info_on_drop(txn, &active.job, &table)?
    };
    // Go `updateVersionAndTableInfo` on every state change, then, as the
    // table reaches none, `DropTableOrView` with `GetAutoIDAccessors(...)
    // .Del()` (`DropSequence` for a sequence, which also drops its value).
    let database_id = database.read().id;
    let action_type = active.job.type_;
    let meta = txn.meta();
    let meta_error = |error| job_meta_error(&meta, error);
    let mut diff = SchemaDiff {
        action_type,
        schema_id: database_id,
        table_id: table.id,
        old_table_id: table.id,
        ..SchemaDiff::default()
    };
    let infos: Vec<(i64, i64)> = extra_tables.iter().map(|(schema, extra)| (*schema, extra.id)).collect();
    materialized_view::set_schema_diff_for_multi_infos(&mut diff, &infos);
    let (schema_version, diff) = update_schema_version(&meta, active, diff)?;
    meta.update_table(database_id, &mut table).map_err(meta_error)?;
    for (extra_schema, mut extra) in extra_tables {
        meta.update_table(extra_schema, &mut extra).map_err(meta_error)?;
    }
    if table.state == SchemaState::NONE {
        if table.is_sequence() {
            meta.drop_sequence(database_id, table.id).map_err(meta_error)?;
        } else {
            meta.drop_table_or_view(database_id, table.id).map_err(meta_error)?;
            meta.auto_ids(database_id, table.id).delete().map_err(meta_error)?;
        }
        // Go `asyncNotifyEvent(NewDropTableEvent)` for a table.
        if !table.is_sequence()
            && !table.is_view()
            && !tidb_metadef::is_mem_or_sys_db(&database.read().name.lowercase().go_to_lower())
        {
            append_schema_change_mutations(txn, ddl_job_id, &[(-1, SchemaChangeEvent::drop_table(table.clone_like_go()))])?;
        }
        // Go `onDropTableOrView`'s `deleteTableAffinityGroupsInPD`: best
        // effort, logged.
        if let Err(error) = crate::ddl_affinity::delete_table_affinity_groups_in_pd(&table, None) {
            tidb_ddl_logutil::ddl_logger().error(
                "failed to delete affinity groups from PD",
                &[
                    tidb_log::Field::new("error", tidb_log::Value::Str(error)),
                    tidb_log::Field::new("tableID", tidb_log::Value::I64(table.id)),
                ],
            );
        }
        // Go master: a dropped log's purge schedule row goes with it.
        if table.materialized_view_log.is_some() {
            materialized_view::delete_materialized_view_log_purge_info(txn, table.id)?;
        }
        // Go master: a dropped view's refresh rows (and, best effort, its
        // alert) go with it.
        if table.materialized_view.is_some() {
            materialized_view::delete_create_materialized_view_refresh_info(txn, table.id)?;
            if let Err(error) = materialized_view::delete_create_materialized_view_refresh_alert(txn, table.id) {
                tidb_ddl_logutil::ddl_logger().warn(
                    "drop table/view: failed to delete materialized view refresh alert",
                    &[
                        tidb_log::Field::new("schemaName", tidb_log::Value::Str(active.job.schema_name.to_string())),
                        tidb_log::Field::new("tableName", tidb_log::Value::Str(table.name.original().to_owned())),
                        tidb_log::Field::new("mviewID", tidb_log::Value::I64(table.id)),
                        tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                    ],
                );
            }
        }
        // Go `onDropTableOrView`'s `dropMaskingPoliciesOnTable`.
        masking_policy::drop_masking_policies_on_table(txn, table.id)?;
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
                    tidb_tablecodec::table_key::gen_table_prefix(active.job.table_id),
                )),
                old_partition_ids,
                old_rule_ids: rule_ids,
                ..tidb_model::DropTableArgs::default()
            })));
    }
    active.job.schema_state = table.state;

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

/// Go `checkResourceGroupExist`: the stored group of the job's ID.
fn stored_resource_group(
    catalog: &SessionExtendedInfoSchema,
    group_id: i64,
) -> Result<&tidb_model::ResourceGroupInfo, DdlPlanError> {
    catalog
        .resource_groups()
        .find(|group| group.id == group_id)
        .ok_or_else(|| {
            coded_refusal(
                &tidb_infoschema::infoschema_errors::ERR_RESOURCE_GROUP_NOT_EXISTS,
                format!("Unknown resource group '(Group ID {group_id})'"),
            )
        })
}

/// Go's wrapping of a `createTable` affinity failure.
const CREATE_TABLE_AFFINITY_ERROR: &str = "failed to create table affinity groups in PD";

/// A PD-side step of a job that failed -- an affinity group definition it
/// cannot build, or a label-rule read -- as Go's `errors.Errorf` /
/// `errors.Wrapf`, wrapped by its caller when it wraps one. The info
/// syncer's `ErrHTTPServiceError` stays that coded error: Go reports a
/// wrapped error by its cause.
fn pd_step_error(context: Option<&str>, error: String) -> DdlPlanError {
    if let Some(coded) = tidb_domain_infosync::pd_http::http_service_error(&error) {
        return DdlPlanError::Source(coded);
    }
    DdlPlanError::Admission(DdlAdmissionError::plain(match context {
            Some(context) => format!("{context}: {error}"),
            None => error,
        },
    ))
}

/// Go `infosync.ConfigureTiFlashPDForTable` in a job step; a PD refusal is
/// the step's error.
pub(crate) fn configure_tiflash_pd_for_table(id: i64, count: u64, labels: &[String]) -> Result<(), DdlPlanError> {
    tidb_domain_infosync::tiflash_manager::configure_tiflash_pd_for_table(id, count, labels)
        .map_err(|error| pd_step_error(None, error))
}

/// Go `infosync.ConfigureTiFlashPDForPartitions` in a job step.
pub(crate) fn configure_tiflash_pd_for_partitions(
    accel: bool,
    definitions: &[tidb_model::PartitionDefinition],
    count: u64,
    labels: &[String],
    table_id: i64,
) -> Result<(), DdlPlanError> {
    let ids: Vec<i64> = definitions.iter().map(|definition| definition.id).collect();
    tidb_domain_infosync::tiflash_manager::configure_tiflash_pd_for_partitions(accel, &ids, count, labels, table_id)
        .map_err(|error| pd_step_error(None, error))
}

/// A table's TiFlash replica count and location labels.
pub(crate) fn tiflash_replica_spec(info: &TableInfo) -> Option<(u64, Vec<String>)> {
    info.tiflash_replica.as_ref().map(|replica| {
        let replica = replica.read();
        (replica.count, replica.location_labels.iter().cloned().collect())
    })
}

/// Go `doDDLJob2` outside a multi-schema change: a built job goes through
/// `GenGIDAndInsertJobsWithRetry`'s admission.
pub(crate) fn submit_built_job<S: MetaSnapshot>(
    snapshot: &mut S,
    built: Option<add_index::AddIndexSubmission>,
    start_ts: u64,
    upgrading: bool,
) -> Result<Option<add_index::AddIndexSubmission>, DdlPlanError> {
    match built {
        Some(add_index::AddIndexSubmission::Job(spec, warnings)) => {
            let mut specs = [*spec];
            tidb_ddl_jobsubmit::prepare_submit_batch(snapshot, &mut specs, start_ts, upgrading)?;
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
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
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

fn cancelled_ddl_job(active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob) -> DdlPlanError {
    cancel_ddl_job(
        active,
        DdlPlanError::from(
            tidb_dbterror::ERR_CANCELLED_DDL_JOB
                .generate(tidb_errno::errname::ErrCancelledDDLJob.raw),
        ),
    )
}

// Go convertJob2RollbackJob. Conversion is its own step; an irreversible
// DROP resumes next time.
fn plan_cancel_persisted_ddl_job(
    txn: &mut JobTxn<'_>,
    catalog: &SessionExtendedInfoSchema,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
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
        | ActionType::ACTION_CREATE_MASKING_POLICY
        | ActionType::ACTION_ALTER_MASKING_POLICY
        | ActionType::ACTION_DROP_MASKING_POLICY
        | ActionType::ACTION_LOCK_TABLE
        | ActionType::ACTION_UNLOCK_TABLE => true,
        // `cancelOnlyNotHandledJob(job, model.StateNone)`.
        ActionType::ACTION_MODIFY_SCHEMA_CHARSET_AND_COLLATE
        | ActionType::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT
        | ActionType::ACTION_REPAIR_TABLE
        | ActionType::ACTION_RECOVER_SCHEMA => {
            active.job.schema_state == SchemaState::NONE
        }
        ActionType::ACTION_RENAME_TABLE | ActionType::ACTION_RENAME_TABLES => {
            active.job.schema_state == SchemaState::NONE
        }
        ActionType::ACTION_DROP_SCHEMA => {
            let database = job_schema(txn, active.job.schema_id)?
                .ok_or_else(|| {
                    cancel_ddl_job(
                        active,
                        DdlPlanError::from(
                            tidb_dbterror::CLASS_SCHEMA
                                .new_std(tidb_errno::errcode::ErrDBDropExists)
                                .generate("Can't drop database ''; database doesn't exist"),
                        ),
                    )
                })?;
            let state = database.read().state;
            state == SchemaState::PUBLIC
        }
        ActionType::ACTION_DROP_TABLE
        | ActionType::ACTION_DROP_VIEW
        | ActionType::ACTION_DROP_SEQUENCE
        | ActionType::ACTION_DROP_MATERIALIZED_VIEW_LOG
        | ActionType::ACTION_DROP_MATERIALIZED_VIEW
        | ActionType::ACTION_DROP_MATERIALIZED_VIEW_SHADOW => {
            let table = checked_job_table(txn, active, false)?;
            let table = table.read();
            table.state == SchemaState::PUBLIC
        }
        ActionType::ACTION_ADD_CHECK_CONSTRAINT
        | ActionType::ACTION_DROP_CHECK_CONSTRAINT
        | ActionType::ACTION_ALTER_CHECK_CONSTRAINT => {
            return plan_persisted_check_constraint_job_step(txn, active, start_ts);
        }
        // Go `rollingbackAddColumanrIndex`: before the reorganization state
        // `convertNotReorgAddIdxJob2RollbackJob`, in it `onCreateColumnarIndex`'s
        // cancelling branch -- both `convertAddIdxJob2RollbackJob` with
        // `ErrCancelledDDLJob`.
        ActionType::ACTION_ADD_INDEX
        | ActionType::ACTION_ADD_PRIMARY_KEY
        | ActionType::ACTION_ADD_COLUMNAR_INDEX => {
            return add_index::rollingback_add_index(txn, active, start_ts);
        }
        ActionType::ACTION_DROP_INDEX | ActionType::ACTION_DROP_PRIMARY_KEY => {
            return drop_index::rollingback_drop_index(catalog, txn, active);
        }
        ActionType::ACTION_ADD_COLUMN => {
            return add_column::rollingback_add_column(txn, active, start_ts);
        }
        ActionType::ACTION_DROP_COLUMN => {
            return drop_column::rollingback_drop_column(catalog, txn, active);
        }
        ActionType::ACTION_MODIFY_COLUMN => {
            return modify_column::rollingback_modify_column(txn, active);
        }
        ActionType::ACTION_ADD_FOREIGN_KEY => {
            return foreign_key::rollingback_add_foreign_key(active);
        }
        ActionType::ACTION_MULTI_SCHEMA_CHANGE => {
            return multi_schema::rolling_back_multi_schema_change(active);
        }
        ActionType::ACTION_ADD_TABLE_PARTITION | ActionType::ACTION_DROP_TABLE_PARTITION => {
            return partition::rollingback_add_table_partition(txn, active, start_ts);
        }
        ActionType::ACTION_TRUNCATE_TABLE_PARTITION => {
            return partition::rollingback_truncate_table_partition(txn, active, start_ts);
        }
        ActionType::ACTION_EXCHANGE_TABLE_PARTITION => {
            return exchange_partition::rollingback_exchange_table_partition(txn, active);
        }
        ActionType::ACTION_REORGANIZE_PARTITION
        | ActionType::ACTION_ALTER_TABLE_PARTITIONING
        | ActionType::ACTION_REMOVE_PARTITIONING => {
            return reorganize_partition::on_rollback_reorganize_partition(
                txn, active, start_ts,
            );
        }
        action if alter_table::ALTER_TABLE_ACTIONS.contains(&action) => {
            return alter_table::convert_job_2_rollback_job(txn, active);
        }
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW => {
            return materialized_view::rollingback_create_materialized_view(active);
        }
        // Go `cancelOnlyNotHandledJob(job, model.StateNone)`.
        ActionType::ACTION_CREATE_MATERIALIZED_VIEW_SHADOW => active.job.schema_state == SchemaState::NONE,
        // Go `rollingbackTruncateTable`: `cancelOnlyNotHandledJob` at
        // `StateNone`, after `GetTableInfoAndCancelFaultJob`.
        ActionType::ACTION_TRUNCATE_TABLE => {
            checked_job_table(txn, active, false)?;
            active.job.schema_state == SchemaState::NONE
        }
        // Go's `default`: the job is cancelled (RECOVER TABLE among them).
        _ => true,
    };
    if cancel {
        return Err(cancelled_ddl_job(active));
    }
    active.job.state = JobState::RUNNING;
    Ok(job_control_step(&active.job))
}

// Go GetTableInfoAndCancelFaultJob/checkTableExistAndCancelNonExistJob over
// `jobCtx.metaMut`. Keep the same object checks on forward CHECK and
// cancellation paths.
fn checked_job_table(
    txn: &mut JobTxn<'_>,
    active: &mut tidb_ddl_systable::ddl_job_table::ActiveDdlJob,
    require_public: bool,
) -> Result<GoShared<TableInfo>, DdlPlanError> {
    let meta = txn.meta();
    let table = match meta.table(active.job.schema_id, active.job.table_id) {
        Ok(table) => table,
        Err(tidb_meta::MetaError::DatabaseNotExists) => {
            return Err(cancel_ddl_job(
                active,
                DdlPlanError::from(
                    tidb_dbterror::CLASS_SCHEMA
                        .new_std(tidb_errno::errcode::ErrBadDB)
                        .generate(format!(
                            "Unknown database '(Schema ID {})'",
                            active.job.schema_id
                        )),
                ),
            ));
        }
        Err(error) => return Err(job_meta_error(&meta, error)),
    };
    let table = table.map(GoShared::new).ok_or_else(|| {
        cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_dbterror::CLASS_SCHEMA
                    .new_std(tidb_errno::errcode::ErrNoSuchTable)
                    .generate(format!(
                        "Table '(Schema ID {}).(Table ID {})' doesn't exist",
                        active.job.schema_id, active.job.table_id
                    )),
            ),
        )
    })?;
    let shared = table;
    let table = shared.read();
    // Check if table name is renamed.
    if !active.job.table_name.is_empty()
        && table.name.lowercase() != active.job.table_name.to_string()
        && active.job.type_ != ActionType::ACTION_REPAIR_TABLE
    {
        return Err(cancel_ddl_job(
            active,
            DdlPlanError::from(
                tidb_dbterror::CLASS_SCHEMA
                    .new_std(tidb_errno::errcode::ErrNoSuchTable)
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
            DdlPlanError::from(tidb_dbterror::ERR_INVALID_DDL_STATE.generate(format!(
                "table {} is not in public, but {}",
                table.name, table.state
            ))),
        ));
    }
    drop(table);
    Ok(shared)
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
    if error.equal(&tidb_dbterror::ERR_CANCELLED_DDL_JOB) {
        let original = job.error.as_ref().unwrap().read().clone();
        if !tidb_dbterror::ERR_CANCELLED_DDL_JOB.equal(Some(&original)) {
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
    if !record_ddl_action_error(job, error) {
        return;
    }
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
        tidb_metrics::panic::PANIC_TOTAL
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
        tidb_logutil::bg_logger().error(
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

/// Go `countForError` up to its error count: the job carries the error, and
/// counts it unless it is an already-started MLog ADD COLUMN waiting for a
/// refresh (Go master `isMLogAddColumnRefreshLockWait`). Whether it counted.
pub(super) fn record_ddl_action_error(job: &mut Job, error: &DdlPlanError) -> bool {
    job.error = Some(GoShared::new(error.to_job_error()));
    if add_column::is_mlog_add_column_refresh_lock_wait(job, error) {
        return false;
    }
    job.error_count += 1;
    true
}

/// Outcome of an action that could not complete its staged work.
#[derive(Debug)]
pub enum PersistedDdlJobFailure {
    /// A returned action error follows Go's countForError path.
    Error(DdlPlanError),
    /// An unwinding action follows Go's distinct countForPanic path.
    Panic,
}

/// Go `worker.runOneJobStep`: checkpoint a failure on the delivered job.
/// The caller commits this job-only write using that same transaction, so a racing
/// pause/cancel causes a write conflict instead of being overwritten.
pub fn plan_persisted_ddl_job_failure(
    ddl_job_id: i64,
    start_ts: u64,
    failure: PersistedDdlJobFailure,
    delivered_job: &tidb_model::Job,
    load_error_count_limit: &dyn Fn() -> i64,
    session: &crate::job_txn::StepSession,
) -> Result<PersistedDdlJobStep, DdlPlanError> {
    let mut active = tidb_ddl_systable::ddl_job_table::ActiveDdlJob::from_job(delivered_job.clone());
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
            ) && error.equal(&tidb_dbterror::ERR_CHECK_CONSTRAINT_IS_VIOLATED)
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
        return finish_persisted_ddl_job(
            &mut active,
            start_ts,
            session,
        );
    }
    let mutations = Vec::new();
    update_ddl_job_2_table(session, start_ts, &mut active.job, false)?;
    Ok(PersistedDdlJobStep {
        job: Box::new(active.job.clone()),
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
    pub warnings: Vec<tidb_context::SqlWarn>,
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
                match tidb_domain_infosync::resource_manager_client::add_resource_group((**group).clone()) {
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
                tidb_domain_infosync::resource_manager_client::modify_resource_group((**group).clone())
            }
            Self::Delete(name) => tidb_domain_infosync::resource_manager_client::delete_resource_group(name),
        }
    }
}

/// Go `checkExchangePartitionRecordValidation` expressed as a data obligation
/// on the final metadata write.
#[derive(Clone, Debug)]
pub struct ExchangePartitionValidation {
    /// Partitioned table before the physical-ID swap (Go `ptbl`).
    pub partitioned: Box<TableInfo>,
    /// Standalone table whose existing rows must all fit the partition (Go
    /// `ntbl`).
    pub standalone: Box<TableInfo>,
    /// Go `pschemaName`: the partitioned table's database, lowercase.
    pub partitioned_schema: String,
    /// Go `nschemaName`: the standalone table's database, lowercase.
    pub standalone_schema: String,
    /// Go `partitionName`.
    pub partition_name: String,
    /// Go `vardef.EnableCheckConstraint`: whether each table's writable
    /// CHECK constraints are applied to the other's rows.
    pub check_constraints: bool,
}

/// One Go `registerMDLInfo` replacement associated with a committed CHECK
/// schema phase.
#[derive(Clone, Debug)]
pub struct MdlInfoUpdate {
    /// System-table upgrades must not depend on the owner_id column.
    pub omit_owner_id: bool,
}

impl MdlInfoUpdate {
    /// Go `registerMDLInfo` on the step's session: the job's `table_ids`
    /// read back from `mysql.tidb_ddl_job`, then `replace into
    /// mysql.tidb_mdl_info` (without `owner_id` for a system database).
    ///
    /// # Errors
    ///
    /// A statement's error, or Go's "can't find ddl job" for a missing row.
    pub fn register(
        &self,
        session: &crate::job_txn::StepSession,
        start_ts: u64,
        ddl_job_id: i64,
        schema_version: i64,
        owner_id: &str,
    ) -> Result<(), DdlPlanError> {
        let rows = session
            .execute(
                start_ts,
                &format!("select table_ids from mysql.tidb_ddl_job where job_id = {ddl_job_id}"),
                "register-mdl-info",
                &[],
            )
            .map_err(DdlPlanError::from_mysql_error)?;
        let Some(ids) = rows.first().and_then(|row| row.first()) else {
            return Err(DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown(format!(
                "can't find ddl job {ddl_job_id}"
            ))));
        };
        let ids = match ids {
            Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
            _ => String::new(),
        };
        let sql = if self.omit_owner_id {
            format!(
                "replace into mysql.tidb_mdl_info (job_id, version, table_ids) values ({ddl_job_id}, {schema_version}, '{ids}')"
            )
        } else {
            format!(
                "replace into mysql.tidb_mdl_info (job_id, version, table_ids, owner_id) values ({ddl_job_id}, {schema_version}, '{ids}', '{owner_id}')"
            )
        };
        session
            .execute(start_ts, &sql, "register-mdl-info", &[])
            .map_err(DdlPlanError::from_mysql_error)?;
        Ok(())
    }

    /// Go `jobScheduler.cleanMDLInfo`'s statement (without `owner_id` for a
    /// system database).
    #[must_use]
    pub fn clean_sql(&self, ddl_job_id: i64, owner_id: &str) -> String {
        if self.omit_owner_id {
            format!("delete from mysql.tidb_mdl_info where job_id = {ddl_job_id}")
        } else {
            format!(
                "delete from mysql.tidb_mdl_info where job_id = {ddl_job_id} and owner_id = '{owner_id}'"
            )
        }
    }

}

/// The names and post-exchange physical IDs Go uses to swap the standalone
/// table and partition label rules in PD; shared with the in-process tier.
use tidb_ddl_build::label_rules::ExchangePartitionLabelSwap;
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
    staged: tidb_domain_infosync::label_manager::StagedLabelRules,
    steps: Vec<LabelRuleStep>,
}

impl LabelRuleSteps {
    fn new() -> Self {
        Self {
            staged: tidb_domain_infosync::label_manager::StagedLabelRules::new(
                tidb_domain_infosync::label_manager::label_rule_manager(),
            ),
            steps: Vec::new(),
        }
    }

    /// The rules as the step sees them.
    fn manager(&self) -> &dyn tidb_domain_infosync::label_manager::LabelRuleManager {
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

use tidb_ddl_build::session_pool::ForeignKeyCheck;


/// The candidate table shape whose enforced CHECK must hold for every
/// existing row before its metadata can be published.
#[derive(Clone, Debug)]
pub struct CheckConstraintValidation {
    /// The table's database (Go `dbInfo.Name`).
    pub schema: String,
    /// Candidate metadata, including the newly enforced constraint.
    pub table: Box<TableInfo>,
    /// The constraint whose violation Go reports as 3819.
    pub constraint_name: String,
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
pub(crate) fn build_sequence_table_info(
    name: &str,
    info: tidb_model::table::SequenceInfo,
    comment: String,
) -> TableInfo {
    TableInfo {
        name: CiString::new(name.to_owned()),
        version: tidb_model::table_info::CURR_LATEST_TABLE_INFO_VERSION,
        sequence: Some(GoShared::new(tidb_model::table::SequenceInfo {
            comment,
            ..info
        })),
        ..TableInfo::default()
    }
}

/// Go `Mutator.sequenceKey` in the database hash: the sequence's counter.
fn sequence_value_kv_key(db_id: i64, sequence_id: i64) -> Vec<u8> {
    tidb_tablecodec::table_key::encode_meta_key(&key::db_key(db_id), &key::sequence_key(sequence_id))
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
) -> Result<tidb_table::partition_routing::PartitionExpr, DdlPlanError> {
    let partition = stored.partition.as_ref().ok_or_else(|| {
        DdlPlanError::Admission(DdlAdmissionError::from_terror(
            &tidb_dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED
                .gen_with_stack_by_args(&[]),
        ))
    })?;
    let columns = stored.columns.iter_deref().collect::<Vec<_>>();
    tidb_table::table_from_meta::partition_spec_for(&columns, &partition.read())
        .map_err(|error| DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error)))
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
        &proto.gen_with_stack(message.into()),
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
            &tidb_dbterror::ERR_CHECK_NO_SUCH_TABLE,
            "Can't open table",
        ));
    }
    if partitioned.partition.is_none() {
        return Err(exchange_refusal(
            &tidb_dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED,
            "Partition management on a not partitioned table is not possible",
        ));
    }
    if standalone.partition.is_some() {
        return Err(exchange_refusal(
            &tidb_dbterror::ERR_PARTITION_EXCHANGE_PART_TABLE,
            format!(
                "Table to exchange with partition is partitioned: '{}'",
                standalone.name
            ),
        ));
    }
    if standalone.affinity.is_some() || partitioned.affinity.is_some() {
        return Err(exchange_refusal(
            &tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL,
            "Unsupported EXCHANGE PARTITION of a table with AFFINITY option",
        ));
    }
    if !standalone.foreign_keys.is_empty() {
        return Err(exchange_refusal(
            &tidb_dbterror::ERR_PARTITION_EXCHANGE_FOREIGN_KEY,
            format!(
                "Table to exchange with partition has foreign key references: '{}'",
                standalone.name
            ),
        ));
    }
    materialized_view::check_exchange_partition_materialized_view_constraints(partitioned, "partitioned table")
        .and_then(|()| {
            materialized_view::check_exchange_partition_materialized_view_constraints(standalone, "non-partitioned table")
        })
        .map_err(DdlPlanError::from_mysql_error)?;
    if standalone.temp_table_type != tidb_model::TempTableType::NONE {
        return Err(exchange_refusal(
            &tidb_dbterror::ERR_PARTITION_EXCHANGE_TEMP_TABLE,
            format!(
                "Table to exchange with partition is temporary: '{}'",
                standalone.name
            ),
        ));
    }
    let different_metadata = || exchange_refusal(
        &tidb_dbterror::ERR_TABLES_DIFFERENT_METADATA,
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
                &tidb_dbterror::ERR_UNSUPPORTED_ON_GENERATED_COLUMN,
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
                &tidb_dbterror::ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION,
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
                &tidb_dbterror::ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION,
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
                &tidb_dbterror::ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION,
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

/// Go `ddl.AddHistoryDDLJob` on the step's session: `addHistoryDDLJob2Table`'s
/// `insert ignore` (whose failure Go only logs), then the meta
/// `DDLJobHistory` entry.
fn add_history_ddl_job(
    session: &crate::job_txn::StepSession,
    start_ts: u64,
    job: &Job,
    encoded: Vec<u8>,
    writes: &mut Vec<BufferMutation>,
) -> Result<(), DdlPlanError> {
    let quote = tidb_mysql::errors::go_quote_string;
    let sql = format!(
        "insert ignore into mysql.tidb_ddl_history(job_id, job_meta, db_name, table_name, schema_ids, table_ids, create_time) values ({}, {}, {}, {}, {}, {}, {})",
        job.id,
        wrap_key_2_string(&encoded),
        quote(&job.schema_name.to_string()),
        quote(&job.table_name.to_string()),
        quote(&job.schema_id.to_string()),
        quote(&job.table_id.to_string()),
        // Go `model.TSConvert2Time(job.StartTS).String()`.
        quote(&tidb_go_time_layout::go_unix_milli_string(
            tidb_model::GoTime::from_tso(job.start_ts).unix_milli(),
        )),
    );
    if let Err(error) = session.execute(start_ts, &sql, "insert_history", &[]) {
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
fn build_create_table_info(
    catalog: &SessionExtendedInfoSchema,
    schema: &str,
    table: &str,
    build: &CreateTableBuild,
) -> Result<TableInfo, DdlPlanError> {
    let Some(database) = find_database(catalog, schema) else {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                .gen_with_stack_by_args(&[(schema.to_owned()).into()]),
        ));
    };
    // Go's create-table path publishes the TableInfo the builder produced;
    // it does not call `TableInfo.Clone` on the way to
    // `Mutator.CreateTableOrView`. `clone_like_go()` would allocate an empty
    // `Indices` slice even when the builder left it nil, turning Go's
    // persisted `"index_info":null` into `[]`.
    let mut info = build
        .for_database(&database.read().charset, &database.read().collate)
        .map_err(DdlPlanError::Admission)?;
    // Go `checkTTLInfoValid`'s foreign-key half, which reads the infoschema
    // the builder does not see.
    if info.ttl_info.is_some()
        && !crate::foreign_key_build::get_table_referred_foreign_keys(catalog, schema, table)
            .is_empty()
    {
        let error = tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_UNSUPPORTED_TTL_REFERENCED_BY_FK
                .gen_with_stack_by_args(&[]),
        );
        return Err(DdlPlanError::from_mysql_error(error));
    }
    // Go `CreateTable`: the built table's `checkTableForeignKeysValid`
    // (executor.go:1106) comes before `createTableWithInfoJob`'s existence
    // check, which is followed by `checkTableInfoValidExtra`.
    let fk_check = build.context.foreign_key_checks();
    crate::foreign_key_build::check_table_foreign_keys_valid(catalog, schema, &info, fk_check)
        .map_err(DdlPlanError::Admission)?;
    // Go `BuildTableInfoWithStmt`: the written `PLACEMENT POLICY`, or else,
    // for a non-temporary table, the schema's default. A written `default`
    // is a reference there (cleared only by `handleTablePlacement`), so it
    // takes no default.
    let written_policy = build
        .create
        .table_options
        .iter()
        .find_map(|option| match option {
            tidb_ast::TableOption::PlacementPolicy(name) => Some(name.clone()),
            _ => None,
        });
    if let Some(name) = written_policy {
        info.placement_policy_ref = Some(tidb_model::GoShared::new(tidb_model::PolicyRefInfo {
            id: 0,
            name: CiString::new(name),
        }));
    } else if info.temp_table_type == tidb_model::TempTableType::NONE {
        if let Some(reference) = database.read().placement_policy_ref.as_ref() {
            info.placement_policy_ref = Some(tidb_model::GoShared::new(reference.read().clone()));
        }
    }
    Ok(info)
}

/// Go `createTable`'s publication once the ids are stamped: the placement
/// bundles (`NewFullTableBundles`, sent before the schema version is
/// published), the affinity groups, the table key at the job transaction's
/// start timestamp and `handleAutoIncID`'s counter seeds.
fn apply_create_table(
    txn: &mut JobTxn<'_>,
    db_id: i64,
    info: &mut TableInfo,
    start_ts: u64,
    placement_bundles: &mut Vec<tidb_placement::Bundle>,
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
        let policies = load_policies(txn)?;
        *placement_bundles =
            tidb_placement::new_full_table_bundles(&policies, info).map_err(|error| {
                DdlPlanError::Admission(DdlAdmissionError::plain(
                    error.to_string(),
                ))
            })?;
    }
    // Go `createTable`'s `createTableAffinityGroupsInPD`, after the
    // placement bundles and wrapped as Go wraps it.
    // A cancelled job's `deleteAffinityGroupsOfCancelledJob` deletes the
    // groups, including groups PD created before it failed.
    crate::ddl_affinity::create_table_affinity_groups_in_pd(info, &[])
        .map_err(|error| pd_step_error(Some(CREATE_TABLE_AFFINITY_ERROR), error))?;
    info.update_ts = start_ts;
    // Go `createTableOrViewWithCheck`.
    let meta = txn.meta();
    meta.create_table_or_view(db_id, info)
        .map_err(|error| job_meta_error(&meta, error))?;
    // Go: "Updating auto id meta kv is done in a separate txn"; here it is
    // the job's, the ids being bound to a table no one reads before the
    // version publishes.
    let meta = txn.meta();
    handle_auto_inc_id(&meta, db_id, info).map_err(|error| job_meta_error(&meta, error))
}

/// Go `handleAutoIncID`: each allocator `NewAllocatorsFromTblInfo` gives the
/// table is rebased, without allocating, to the first id the table option
/// asks for; a rebase only moves a counter forward.
pub(crate) fn handle_auto_inc_id<T: tidb_meta::transaction::MetaTransaction>(
    meta: &tidb_meta::transaction::Mutator<T>,
    db_id: i64,
    info: &TableInfo,
) -> Result<(), tidb_meta::MetaError> {
    let has_row_id = !info.pk_is_handle && !info.is_common_handle;
    let has_auto_inc_id = info.get_auto_increment_col_info().is_some();
    let sep_auto_inc = info.sep_auto_inc();
    let auto_ids = meta.auto_ids(db_id, info.id);
    let row_id = (has_row_id || (has_auto_inc_id && !sep_auto_inc)).then(|| auto_ids.row_id());
    let increment = (has_auto_inc_id && sep_auto_inc).then(|| auto_ids.increment_id(info.version));
    let random = info.contains_auto_random_bits().then(|| auto_ids.random_id());
    let auto_inc_unsigned = info.is_auto_inc_col_unsigned();
    let mut rebases = Vec::new();
    if info.auto_inc_id > 1 {
        let allocator = if sep_auto_inc { &increment } else { &row_id };
        rebases.push((allocator, info.auto_inc_id - 1, auto_inc_unsigned));
    }
    if info.auto_inc_id_extra != 0 {
        rebases.push((&row_id, info.auto_inc_id_extra - 1, auto_inc_unsigned));
    }
    if info.auto_rand_id > 1 {
        rebases.push((&random, info.auto_rand_id - 1, info.is_auto_random_bit_col_unsigned()));
    }
    for (allocator, end, unsigned) in rebases {
        let Some(allocator) = allocator else {
            continue;
        };
        let current = allocator.get()?;
        let behind = if unsigned {
            (current as u64) < (end as u64)
        } else {
            current < end
        };
        if behind {
            allocator.put(end)?;
        }
    }
    tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/handleAutoIncID", &mut ());
    Ok(())
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
            &proto.gen_with_stack(reason),
        ),
    )
}

/// Go `types.IsVarcharTooBigFieldLength`: the declared length is in
/// CHARACTERS, so a wider charset lowers the ceiling.
fn check_varchar_field_length(flen: i64, name: &str, to_charset: &str) -> Result<(), DdlPlanError> {
    const MAX_FIELD_VARCHAR_LENGTH: i64 = 65535;
    let Ok(info) = tidb_charset::get_charset_info(to_charset) else {
        return Ok(());
    };
    let max = MAX_FIELD_VARCHAR_LENGTH / info.maxlen as i64;
    if flen != tidb_datatype::UNSPECIFIED_LENGTH && flen > max {
        return Err(unsupported_charset_change(
            &tidb_datatype::types_errors::ERR_TOO_BIG_FIELD_LENGTH,
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
fn lock_plan_error(error: tidb_terror::MysqlError) -> DdlPlanError {
    DdlPlanError::Admission(DdlAdmissionError::from_mysql_error(error))
}

/// Go `getTableInfo` over `jobCtx.metaMut`, a missing schema or table
/// answered `None`.
fn stored_table_by_ids(
    txn: &mut JobTxn<'_>,
    schema_id: i64,
    table_id: i64,
) -> Result<Option<GoShared<TableInfo>>, DdlPlanError> {
    let meta = txn.meta();
    match meta.table(schema_id, table_id) {
        Ok(table) => Ok(table.map(GoShared::new)),
        Err(tidb_meta::MetaError::DatabaseNotExists) => Ok(None),
        Err(error) => Err(job_meta_error(&meta, error)),
    }
}

/// Go `meta.Mutator.ListTables` over `jobCtx.metaMut`.
fn job_schema_tables(
    txn: &mut JobTxn<'_>,
    schema_id: i64,
) -> Result<Vec<TableInfo>, DdlPlanError> {
    let meta = txn.meta();
    meta.tables(schema_id)
        .map_err(|error| job_meta_error(&meta, error))
}

/// Go `meta.Mutator.GetDatabase` with `ListTables` over `jobCtx.metaMut`:
/// the schema and the tables it stores, in whatever state, as
/// `checkTableNotExistsFromStore` and `checkConstraintNamesNotExists` list
/// them.
fn job_schema_with_tables(
    txn: &mut JobTxn<'_>,
    schema_id: i64,
) -> Result<Option<GoShared<DBInfo>>, DdlPlanError> {
    let meta = txn.meta();
    let meta_error = |error| job_meta_error(&meta, error);
    let Some(mut database) = meta.database(schema_id).map_err(meta_error)? else {
        return Ok(None);
    };
    database.deprecated_tables = meta.tables(schema_id).map_err(meta_error)?.into();
    Ok(Some(GoShared::new(database)))
}

/// Go `meta.Mutator.GetDatabase` over `jobCtx.metaMut`.
fn job_schema(
    txn: &mut JobTxn<'_>,
    schema_id: i64,
) -> Result<Option<GoShared<DBInfo>>, DdlPlanError> {
    let meta = txn.meta();
    meta.database(schema_id)
        .map(|database| database.map(GoShared::new))
        .map_err(|error| job_meta_error(&meta, error))
}

/// go's note for one missing table: `Note | 1051 | Unknown table 'd.t'`.
fn missing_table_note(schema: &str, table: &str) -> tidb_context::SqlWarn {
    crate::real_tikv_ddl::ddl_note(
        tidb_infoschema::infoschema_errors::ERR_TABLE_DROP_EXISTS
            .gen_with_stack_by_args(&[format!("{}.{}", schema, table).into()]),
    )
}

fn refuse_non_base_table(
    stored: &TableInfo,
    schema: &str,
    table: &str,
) -> Result<(), DdlPlanError> {
    if stored.is_view() || stored.is_sequence() {
        return Err(coded_refusal(
            &tidb_dbterror::ERR_WRONG_OBJECT,
            format!("'{schema}.{table}' is not BASE TABLE"),
        ));
    }
    Ok(())
}

fn locate_table<'catalog>(
    catalog: &'catalog SessionExtendedInfoSchema,
    schema: &str,
    table: &str,
) -> Result<(i64, GoShared<TableInfo>), DdlPlanError> {
    let Some(database) = find_database(catalog, schema) else {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                .gen_with_stack_by_args(&[(schema.to_owned()).into()]),
        ));
    };
    let Some((_, stored)) = catalog.table_info_by_name(database.read().name.original(), table) else {
        return Err(DdlPlanError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.gen_with_stack_by_args(&[
                (schema.to_owned()).into(),
                (table.to_owned()).into(),
            ]),
        ));
    };
    let db_id = database.read().id;
    Ok((db_id, stored))
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
    catalog: &SessionExtendedInfoSchema,
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
            &tidb_infoschema::infoschema_errors::ERR_CANNOT_ADD_FOREIGN,
            "Cannot add foreign key constraint",
        ));
    }
    // Go `checkFKDupName`.
    if stored
        .foreign_keys
        .iter_deref()
        .any(|fk| fk.read().name.lowercase() == name.go_to_lower())
    {
        return Err(coded_refusal(
            &tidb_dbterror::ERR_FK_DUP_NAME,
            format!("Duplicate foreign key constraint name '{name}'"),
        ));
    }
    let ref_schema = match definition.reference.table.as_deref() {
        Some([ref_schema, _]) => ref_schema.clone(),
        _ => schema.to_owned(),
    };
    let fk = tidb_ddl_build::foreign_key::build_fk_info(
        name,
        &definition.parts,
        &definition.reference,
        &ref_schema,
        stored,
    )
    .map_err(admission)?;
    // Go `checkAddForeignKeyValid`.
    crate::foreign_key_build::check_table_foreign_key_valid(
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
            tidb_dbterror::ERR_DUP_KEY_NAME.gen_with_stack_by_args(&[(fk
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
            tidb_errno::errcode::ErrUnknown,
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
        &tidb_metabuild::Context::new([]),
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

/// Go `notifier.PubSchemeChangeToStore` for each of `events`: the step's
/// session runs `tableStore.Insert`'s statement in the step's transaction.
/// A run without a session (bootstrap, planner tests) still stages the rows
/// itself, which Go never does.
fn append_schema_change_mutations(
    txn: &JobTxn<'_>,
    ddl_job_id: i64,
    events: &[(i64, SchemaChangeEvent)]) -> Result<(), DdlPlanError> {
    if events.is_empty() {
        return Ok(());
    }
    // Go `asyncNotifyEvent`: "In test environments, we use a channel-based
    // approach to handle DDL events. This maintains compatibility with
    // existing test cases that expect events to be delivered through
    // channels. In production, DDL events are handled by the notifier system
    // instead."
    if tidb_intest::IN_TEST {
        if let Some(ch) = txn.session().pool.ddl_event_ch() {
            for (_, event) in events {
                // Try sending the event to the channel with a backoff strategy
                // to avoid blocking indefinitely. Since most unit tests don't
                // consume events, we make a few attempts and then give up
                // rather than blocking the DDL job forever on a full channel.
                for _ in 0..3 {
                    if ch.try_send(event.clone()).is_ok() {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_micros(2));
                }
                // Go logs this after its loop whether or not the send
                // succeeded.
                tidb_ddl_logutil::ddl_logger().warn(
                    "fail to notify DDL event",
                    &[tidb_log::Field::new("event", tidb_log::Value::Str(event.to_string()))],
                );
            }
        }
    }
    // Go `asyncNotifyEvent`'s failpoint, after its system-database skip
    // (this function's callers skip those).
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/asyncNotifyEventError").is_some() {
        return Err(DdlPlanError::from_mysql_error(tidb_terror::MysqlError::unknown("mock publish event error")));
    }
    let sql = format!(
        "INSERT INTO mysql.{NOTIFIER_TABLE_NAME} (ddl_job_id, sub_job_id, schema_change, processed_by_flag) \
         VALUES (%?, %?, %?, 0)"
    );
    // Go master: the publication's internal SQL can flush earlier step
    // writes, so any later error discards the whole step.
    txn.set_must_rollback_txn_on_error();
    for (sub_job_id, event) in events {
        let event =
            serde_json::to_vec(event).map_err(|error| DdlPlanError::Encode(error.to_string()))?;
        txn.execute(
            &sql,
            "ddl_notifier",
            &[
                tidb_sqlescape::SqlArg::Signed(ddl_job_id),
                tidb_sqlescape::SqlArg::Signed(*sub_job_id),
                tidb_sqlescape::SqlArg::Bytes(Some(&event)),
            ],
        )
        .map_err(DdlPlanError::from_mysql_error)?;
    }
    Ok(())
}

fn event_partition_info(definitions: Vec<PartitionDefinition>) -> PartitionInfo {
    PartitionInfo {
        definitions: definitions.into(),
        ..PartitionInfo::default()
    }
}

fn find_database(catalog: &SessionExtendedInfoSchema, name: &str) -> Option<GoShared<DBInfo>> {
    let name = name.go_to_lower();
    catalog
        .stored_schemas()
        .into_iter()
        .find(|database| database.read().name.lowercase() == name)
}

/// Go `is.TableByName` within an already resolved schema: under V2 the
/// table loads through the bounded cache, never from `DBInfo`'s table list.
fn find_table(
    catalog: &SessionExtendedInfoSchema,
    database: &DBInfo,
    name: &str,
) -> Option<GoShared<TableInfo>> {
    catalog
        .table_info_by_name(database.name.original(), name)
        .map(|(_, table)| table)
}

#[cfg(test)]
mod global_index_version_tests {
    use super::*;

    #[test]
    fn set_global_index_version_matches_go_shape_rules() {
        let previous = tidb_model::index::get_global_index_v_1_supported();
        tidb_model::index::set_global_index_v_1_supported(true);

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

        tidb_model::index::set_global_index_v_1_supported(false);
        let mut unsupported = index(false);
        set_global_index_version(&table(nullable_column), &mut unsupported);
        assert_eq!(
            unsupported.global_index_version,
            tidb_model::index::GLOBAL_INDEX_VERSION_LEGACY
        );
        tidb_model::index::set_global_index_v_1_supported(previous);
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

mod executor_alter;
pub(crate) use executor_alter::{add_materialized_view_log_columns, alter_table_job};
