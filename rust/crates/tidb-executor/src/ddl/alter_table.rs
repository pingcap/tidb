// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `ALTER TABLE`: the dispatcher and the per-action work that changes an
//! existing table's columns and options in place.
//!
//! Inside: [`run_alter_table_in`], which applies the statement's actions in
//! source order on a staged catalog after column/index admission and publishes
//! only after they all succeed;
//! [`prepare_add_column`], [`prepare_modify_column`] and
//! [`prepare_drop_column`], the three column changes, including the read-time
//! `OriginDefaultValue` fill that gives already-written rows a new column's
//! DEFAULT without rewriting their bytes; constraint admission/application
//! in `constraint_changes`; [`prepare_table_options`] for
//! the table-level options an ALTER may set; and the two helpers
//! [`normalize_column_default`] and [`existing_table_charset`] that the
//! column actions share. Each doc comment records the captured TiDB error
//! code (1060, 1090, 1091, 8200).
//!
//! Follows Go `pkg/ddl/executor.go`'s column job admission and
//! `multi_schema_change.go`'s combination checks. Column and index application
//! live in `column_changes` and `index_changes`; their builders reuse the
//! existing type/charset, default, generated-column and storage owners.

use std::collections::HashSet;

use super::column_changes::{self, PreparedColumnChange};
use super::column_types::{field_type_of, NOT_NULL_FLAG};
use super::constraint_changes::{self, PreparedConstraintChange};
use super::index_changes::{self, PreparedIndexChange};
use super::table_constraints::{AUTO_INCREMENT_FLAG, PRI_KEY_FLAG};
use super::{Catalog, ColumnDef, DdlStmt, DriverError, KvColumn, Stmt, TableCharset};
use crate::partition_routing::{PartitionDef, PartitionKind, RangeBound};
use tidb_datatype::{Charset, Collation, Datum, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_hack::GoToLower;

/// Go's worker reaches the revertible AUTO_RANDOM checks before executing
/// non-revertible rebase jobs. Keep both phases outside catalog preparation.
#[derive(Default)]
pub(super) struct PreparedAllocatorChanges {
    pub(super) layouts: Vec<crate::kv_table::PreparedAutoRandomChange>,
    rebases: Vec<PreparedAllocatorRebase>,
}

enum PreparedAllocatorRebase {
    Increment(crate::kv_table::PreparedAutoIdRebase),
    Random(crate::kv_table::PreparedAutoIdRebase),
}

impl PreparedAllocatorChanges {
    fn execute(self) -> Result<(), DriverError> {
        for layout in self.layouts {
            layout.execute().map_err(super::auto_random::rebase_error)?;
        }
        for rebase in self.rebases {
            match rebase {
                PreparedAllocatorRebase::Increment(rebase) => {
                    rebase.execute().map_err(auto_increment_rebase_error)?;
                }
                PreparedAllocatorRebase::Random(rebase) => {
                    rebase.execute().map_err(|error| {
                        super::auto_random::rebase_error(crate::kv_table::AutoRandomError::AutoId(
                            error,
                        ))
                    })?;
                }
            }
        }
        Ok(())
    }
}

/// Go `onRebaseAutoID`'s job warning: without FORCE a request below
/// `NextGlobalAutoID` is raised to it, for auto-increment and auto-random
/// alike.
fn warn_adjusted_rebase(ctx: &crate::StmtContext, rebase: &crate::kv_table::PreparedAutoIdRebase) {
    if rebase.next() != rebase.requested() {
        ctx.append_warning_parts(
            1105,
            &format!(
                "Can't reset AUTO_INCREMENT to {} without FORCE option, using {} instead",
                rebase.requested() as i64,
                rebase.next() as i64
            ),
        );
    }
}

fn auto_increment_rebase_error(error: crate::kv_table::AutoIdError) -> DriverError {
    match error {
        crate::kv_table::AutoIdError::Exhausted => DriverError::AutoincReadFailed,
        crate::kv_table::AutoIdError::OutOfRange { value, type_name } => {
            DriverError::ConstantOverflows { value, type_name }
        }
        crate::kv_table::AutoIdError::Store(detail) => DriverError::AutoIdUnavailable(detail.0),
    }
}

/// Runs an `ALTER TABLE`, applying its actions in source order.
///
/// The rules are captured from TiDB: `ADD COLUMN ... DEFAULT d` gives rows
/// written earlier the value `d` rather than NULL, without rewriting them;
/// `FIRST`/`AFTER` place the column; a duplicate name is 1060, dropping an
/// unknown column is 1091, dropping the last column is 1090, and dropping an
/// integer primary key is TiDB's own 8200.
///
/// Every ALTER action this match does not name is rejected rather than
/// silently accepted, and dropping a column an index uses is rejected rather
/// than leaving the index addressing a column that is gone.
pub fn run_alter_table_in(
    sql: &str,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let stmt = ctx.parse(sql)?;
    let Stmt::Ddl(ddl) = &stmt else {
        return Err(DriverError::unsupported(
            "only ALTER TABLE is supported here",
        ));
    };
    let DdlStmt::AlterTable(alter) = &**ddl else {
        return Err(DriverError::unsupported(
            "only ALTER TABLE is supported here",
        ));
    };
    // Repartition and REORGANIZE require Go's durable reorganization owner. Refuse
    // before applying any action while that owner is unavailable.
    if alter.actions.iter().any(|action| {
        matches!(
            action,
            tidb_ast::AlterTableAction::Partition(
                tidb_ast::AlterPartitionAction::Repartition(_)
                    | tidb_ast::AlterPartitionAction::Reorganize { .. }
            )
        )
    }) {
        return Err(DriverError::unsupported(
            "ALTER TABLE partition reorganization requires its durable DDL owner",
        ));
    }
    // Go owns rollback for the entire multi-schema job. In this synchronous
    // owner, stage the catalog and its copy-on-write row/index storage for
    // every ALTER, including grouped specifications within a single action.
    // Warnings still belong to the statement context on either outcome.
    // Allocators are shared outside that image: prepare their operations and
    // execute only after every action succeeds, never restore shared counters.
    let mut staged = catalog.clone();
    let mut allocators = PreparedAllocatorChanges::default();
    if let Err(error) =
        run_alter_table_in_inner(alter, &mut staged, current_db, ctx, &mut allocators)
    {
        catalog.retain_foreign_key_ids_from(&staged);
        return Err(error);
    }
    allocators.execute()?;
    *catalog = staged;
    Ok(())
}

enum PreparedAlterChange<'a> {
    Column(PreparedColumnChange),
    Constraint(PreparedConstraintChange),
    Index(PreparedIndexChange<'a>),
    Metadata(Vec<PreparedMetadataChange>),
}

struct PreparedAlterAction<'a> {
    change: Option<PreparedAlterChange<'a>>,
    warnings: Vec<(crate::WarnLevel, u16, String)>,
}

impl PreparedAlterAction<'_> {
    fn publish_warnings(&mut self, ctx: &crate::StmtContext) {
        for (level, code, message) in self.warnings.drain(..) {
            ctx.append_leveled(level, code, &message);
        }
    }
}

/// Mirrors Go `checkOperateSameColAndIdx` for one multi-spec ALTER.
///
/// Go turns every specification into a sub-job, collects the affected names
/// by category, and rejects a name that appears in two incompatible
/// categories before any sub-job runs. The synchronous Rust runner used to
/// apply the actions in source order instead, which both exposed a later
/// 1054/1091 and left earlier actions committed. Keep the category ordering
/// (ADD, DROP, POSITION, MODIFY, relative columns, then ADD/DROP/ALTER index)
/// exactly as Go does so the first conflicting name and its 8200 diagnostic
/// are stable.
fn reject_multi_schema_same_column_or_index(
    actions: &[tidb_ast::AlterTableAction],
    changes: &[PreparedAlterAction<'_>],
) -> Result<(), DriverError> {
    let mut add_columns = Vec::new();
    let mut drop_columns = Vec::new();
    let mut position_columns = Vec::new();
    let mut modify_columns = Vec::new();
    let mut relative_columns = Vec::new();
    let mut add_indexes = Vec::new();
    let mut drop_indexes = Vec::new();
    let mut alter_indexes = Vec::new();

    for (action, prepared) in actions.iter().zip(changes) {
        match prepared.change.as_ref() {
            Some(PreparedAlterChange::Index(change)) => match change {
                PreparedIndexChange::Add { name, definition } => {
                    add_indexes.push(name.clone());
                    for part in &definition.parts {
                        match part {
                            tidb_ast::IndexPart::Column { name, .. } => {
                                relative_columns.push(name.clone())
                            }
                            tidb_ast::IndexPart::Expr { expr, .. } => {
                                collect_expression_columns(expr, &mut relative_columns)
                            }
                        }
                    }
                }
                PreparedIndexChange::Drop { name, .. } => drop_indexes.push(name.clone()),
                PreparedIndexChange::Rename { from, to, .. } => {
                    add_indexes.push(from.clone());
                    drop_indexes.push(to.clone());
                }
                PreparedIndexChange::Visibility { name, .. } => alter_indexes.push(name.clone()),
            },
            Some(PreparedAlterChange::Column(change)) => {
                let mut position = None;
                match change {
                    PreparedColumnChange::Add {
                        column,
                        position: requested,
                    } => {
                        add_columns.push(column.name.clone());
                        if let Some(generated) = &column.generated {
                            relative_columns.extend(generated.dependencies.iter().cloned());
                        }
                        position = Some(requested);
                    }
                    PreparedColumnChange::Drop { name, .. } => drop_columns.push(name.clone()),
                    PreparedColumnChange::Modify {
                        old_name,
                        column,
                        position: requested,
                        ..
                    } => {
                        if old_name.go_to_lower() == column.name.go_to_lower() {
                            modify_columns.push(column.name.clone());
                        } else {
                            add_columns.push(column.name.clone());
                            drop_columns.push(old_name.clone());
                        }
                        position = Some(requested);
                    }
                    PreparedColumnChange::Rename { from, to, .. } => {
                        add_columns.push(to.clone());
                        drop_columns.push(from.clone());
                    }
                    PreparedColumnChange::Default { column } => {
                        modify_columns.push(column.name.clone())
                    }
                }
                if let Some(tidb_ast::ColumnPosition::After(name)) = position {
                    position_columns.push(name.clone());
                }
            }
            Some(PreparedAlterChange::Constraint(change)) => {
                if let Some(index) = change.implicit_index() {
                    add_indexes.push(index.name.clone().expect("implicit index is named"));
                    for part in &index.parts {
                        if let tidb_ast::IndexPart::Column { name, .. } = part {
                            relative_columns.push(name.clone());
                        }
                    }
                }
            }
            Some(PreparedAlterChange::Metadata(_)) | None => {}
        }
        if matches!(action, tidb_ast::AlterTableAction::DropPrimaryKey(_)) {
            drop_indexes.push("PRIMARY".to_owned());
        }
    }

    let names = |values: Vec<String>| {
        tidb_model::GoSharedSlice::from_vec(
            values.into_iter().map(tidb_ast::CiString::new).collect(),
        )
    };
    check_multi_schema_names(&tidb_model::MultiSchemaInfo {
        add_columns: names(add_columns),
        drop_columns: names(drop_columns),
        position_columns: names(position_columns),
        modify_columns: names(modify_columns),
        relative_columns: names(relative_columns),
        add_indexes: names(add_indexes),
        drop_indexes: names(drop_indexes),
        alter_indexes: names(alter_indexes),
        ..Default::default()
    })
}

/// Go checkOperateSameColAndIdx, shared by local and cluster admission.
pub fn check_multi_schema_names(info: &tidb_model::MultiSchemaInfo) -> Result<(), DriverError> {
    fn check_names(
        names: &[tidb_ast::CiString],
        add_to_seen: bool,
        seen: &mut HashSet<String>,
        kind: &str,
    ) -> Result<(), DriverError> {
        for name in names {
            let canonical = name.lowercase().to_owned();
            if seen.contains(&canonical) {
                return Err(DriverError::DdlCoded {
                    errno: 8200,
                    message: format!(
                        "Unsupported modify column: operate same {kind} '{canonical}'"
                    ),
                });
            }
            if add_to_seen {
                seen.insert(canonical);
            }
        }
        Ok(())
    }

    let mut columns = HashSet::new();
    check_names(&info.add_columns.snapshot(), true, &mut columns, "column")?;
    check_names(&info.drop_columns.snapshot(), true, &mut columns, "column")?;
    check_names(
        &info.position_columns.snapshot(),
        false,
        &mut columns,
        "column",
    )?;
    check_names(
        &info.modify_columns.snapshot(),
        true,
        &mut columns,
        "column",
    )?;
    check_names(
        &info.relative_columns.snapshot(),
        false,
        &mut columns,
        "column",
    )?;

    let mut indexes = HashSet::new();
    check_names(&info.add_indexes.snapshot(), true, &mut indexes, "index")?;
    check_names(&info.drop_indexes.snapshot(), true, &mut indexes, "index")?;
    check_names(&info.alter_indexes.snapshot(), true, &mut indexes, "index")?;
    Ok(())
}

/// Go checkMultiSchemaInfo validates the final size from admitted jobs.
fn check_prepared_column_count(
    changes: &[PreparedAlterAction<'_>],
    catalog: &Catalog,
    database: &str,
    name: &str,
) -> Result<(), DriverError> {
    let mut added = 0;
    let mut dropped = 0;
    for prepared in changes {
        match prepared.change.as_ref() {
            Some(PreparedAlterChange::Column(PreparedColumnChange::Add { .. })) => added += 1,
            Some(PreparedAlterChange::Column(PreparedColumnChange::Drop { .. })) => dropped += 1,
            _ => {}
        }
    }
    let table = column_changes::table_of(catalog, database, name)?;
    check_visible_column_count(table, added, dropped)?;
    if table.columns().len() + added - dropped > catalog.table_column_count_limit() {
        return Err(DriverError::DdlCoded {
            errno: tidb_error::mysql::errcode::ErrTooManyFields,
            message: "Too many columns".to_owned(),
        });
    }
    Ok(())
}

fn check_visible_column_count(
    table: &crate::KvTable,
    added: usize,
    dropped: usize,
) -> Result<(), DriverError> {
    if table.visible_column_count() + added > dropped {
        return Ok(());
    }
    let (errno, message) = if table.visible_column_count() < table.columns().len() {
        (
            tidb_error::mysql::errcode::ErrTableMustHaveColumns,
            tidb_error::mysql::errname::ErrTableMustHaveColumns.raw,
        )
    } else {
        (
            tidb_error::mysql::errcode::ErrCantRemoveAllFields,
            tidb_error::mysql::errname::ErrCantRemoveAllFields.raw,
        )
    };
    Err(DriverError::DdlCoded {
        errno,
        message: message.to_owned(),
    })
}

/// Collects dependencies while filling multi-schema index jobs.
fn collect_expression_columns(expression: &tidb_ast::Expr, output: &mut Vec<String>) {
    use std::any::Any;
    use tidb_ast::{Visitable, Visitor};

    struct Collector<'a> {
        output: &'a mut Vec<String>,
    }

    impl Visitor for Collector<'_> {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            if let Some(tidb_ast::Expr::Column(path)) = node.downcast_ref::<tidb_ast::Expr>() {
                if let Some(name) = path.last() {
                    self.output.push(name.clone());
                }
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn Any) -> bool {
            true
        }
    }

    let mut expression = expression.clone();
    let mut collector = Collector { output };
    expression.accept(&mut collector);
}

/// Go resolveAlterTableAddColumns expands all columns before constraints.
/// The resulting specifications share the same original-schema admission.
fn resolve_grouped_actions(
    actions: &[tidb_ast::AlterTableAction],
) -> std::borrow::Cow<'_, [tidb_ast::AlterTableAction]> {
    if !actions
        .iter()
        .any(|action| matches!(action, tidb_ast::AlterTableAction::AddColumns { .. }))
    {
        return std::borrow::Cow::Borrowed(actions);
    }
    let mut resolved = Vec::new();
    for action in actions {
        if let tidb_ast::AlterTableAction::AddColumns {
            if_not_exists,
            columns,
            constraints,
        } = action
        {
            resolved.extend(columns.iter().cloned().map(|column| {
                tidb_ast::AlterTableAction::AddColumn {
                    if_not_exists: *if_not_exists,
                    column,
                    position: tidb_ast::ColumnPosition::Default,
                }
            }));
            resolved.extend(
                constraints
                    .iter()
                    .cloned()
                    .map(|constraint| match constraint {
                        tidb_ast::TableConstraint::Index(index) => {
                            tidb_ast::AlterTableAction::AddIndexConstraint(index)
                        }
                        tidb_ast::TableConstraint::ForeignKey(key) => {
                            tidb_ast::AlterTableAction::AddForeignKey(key)
                        }
                        tidb_ast::TableConstraint::Check(check) => {
                            tidb_ast::AlterTableAction::AddCheck(check)
                        }
                    }),
            );
        } else {
            resolved.push(action.clone());
        }
    }
    std::borrow::Cow::Owned(resolved)
}

fn run_alter_table_in_inner(
    alter: &tidb_ast::AlterTableStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    allocators: &mut PreparedAllocatorChanges,
) -> Result<(), DriverError> {
    let (database, name) = crate::driver::split_table_path_pub(&alter.name, current_db)?;
    let (database, name) = (database.to_owned(), name.to_owned());
    if catalog.table_in(&database, &name).is_none() {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{name}"),
        )));
    }
    super::refuse_local_temporary_table_ddl(catalog, &database, &name, "ALTER TABLE")?;
    // Go's ALTER path checks the two guards in THIS order, and the corpus
    // asserts the difference: `ddl/db_integration`'s
    // `TestPlacementOnTemporaryTable` gets 8200 for
    // `alter table <local temp> placement policy='x'` -- the executor's
    // local-temporary refusal above, which fires before any option is looked
    // at -- and 8006 for the same statement over a GLOBAL temporary table,
    // which reaches the DDL package and `ddl/executor.go:646`.
    super::refuse_temporary_table_alter_options(catalog, &database, &name, &alter.actions)?;
    super::table_cache::guard_alter_actions(catalog, &database, &name, &alter.actions)?;

    let mut actions = resolve_grouped_actions(&alter.actions).into_owned();
    // Go getValidAlterTableSpecs filters LOCK before counting specifications.
    actions.retain(|action| !matches!(action, tidb_ast::AlterTableAction::Lock(_)));
    super::storage_class::check_storage_class_conflict_in_alter_specs(actions.iter().flat_map(
        |action| match action {
            tidb_ast::AlterTableAction::SetTableOptions { options } => options.as_slice(),
            _ => &[],
        },
    ))?;
    let mut next_fk_id = column_changes::table_of(catalog, &database, &name)?.max_foreign_key_id();
    for action in &mut actions {
        if let tidb_ast::AlterTableAction::AddForeignKey(definition) = action {
            if definition.name.as_ref().is_none_or(String::is_empty) {
                next_fk_id += 1;
                definition.name = Some(format!("fk_{next_fk_id}"));
            }
        }
    }
    let preparation_ctx = ctx.with_isolated_warnings();
    let mut charset_handled = false;
    let mut changes: Vec<PreparedAlterAction<'_>> = Vec::with_capacity(actions.len());
    for action in actions.iter() {
        let change = if is_metadata_change(action) {
            prepare_metadata_change(
                action,
                catalog,
                &database,
                &name,
                current_db,
                &preparation_ctx,
                actions.len() > 1,
                &mut charset_handled,
            )
            .map(|changes| Some(PreparedAlterChange::Metadata(changes)))
        } else if constraint_changes::is_constraint_change(action) {
            constraint_changes::prepare(
                action,
                catalog,
                &database,
                &name,
                &preparation_ctx,
                actions.len() > 1,
            )
            .map(|change| change.map(PreparedAlterChange::Constraint))
        } else if index_changes::is_index_change(action) {
            index_changes::prepare(
                action,
                catalog,
                &database,
                &name,
                &preparation_ctx,
                actions.len() > 1,
            )
            .map(|change| change.map(PreparedAlterChange::Index))
        } else {
            column_changes::prepare(action, catalog, &database, &name, &preparation_ctx)
                .map(|change| change.map(PreparedAlterChange::Column))
        };
        let warnings = preparation_ctx.take_local_warnings();
        let change = match change {
            Ok(change) => change,
            Err(error) => {
                for prepared in &mut changes {
                    prepared.publish_warnings(ctx);
                }
                for (level, code, message) in warnings {
                    ctx.append_leveled(level, code, &message);
                }
                return Err(error);
            }
        };
        changes.push(PreparedAlterAction { change, warnings });
    }
    // Admission, including its ordered notes, completes before conflict
    // checks and execution. No earlier job can hide a later admission error.
    for prepared in &mut changes {
        prepared.publish_warnings(ctx);
    }
    if actions.len() > 1 {
        reject_multi_schema_same_column_or_index(&actions, &changes)?;
        check_prepared_column_count(&changes, catalog, &database, &name)?;
    }
    reject_drop_index_used_by_added_foreign_key(catalog, &database, &name, &actions)?;
    // Go submits the statement's jobs here -- one per specification, or the
    // sub-jobs of one multi-schema change -- and the submitter's BDR
    // admission refuses before any of them runs.
    let jobs = actions
        .iter()
        .zip(&changes)
        .flat_map(|(action, prepared)| {
            submitted_jobs(action, prepared.change.as_ref(), catalog, &database, &name)
        })
        .collect::<Vec<_>>();
    super::bdr::admit(catalog, ctx.ddl_cdc_write_source(), &database, &jobs)?;
    for (action, prepared) in actions.iter().zip(changes) {
        if let Some(change) = prepared.change {
            match change {
                PreparedAlterChange::Constraint(change) => {
                    change.execute(catalog, &database, &name, ctx)?
                }
                PreparedAlterChange::Index(change) => {
                    change.execute(catalog, &database, &name, ctx)?
                }
                PreparedAlterChange::Column(change) => {
                    change.execute(catalog, &database, &name, ctx, allocators)?
                }
                PreparedAlterChange::Metadata(changes) => {
                    for change in changes {
                        change.execute(catalog, &database, &name, allocators)?;
                    }
                }
            }
            continue;
        }
        if index_changes::is_index_change(action)
            || column_changes::is_column_change(action)
            || constraint_changes::is_constraint_change(action)
        {
            continue;
        }
        match action {
            tidb_ast::AlterTableAction::Cache(mode) => {
                super::table_cache::alter_cache_action(catalog, &database, &name, *mode)?
            }
            tidb_ast::AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Truncate {
                all,
                names,
            }) => truncate_partition_action(catalog, &database, &name, *all, names, ctx)?,
            tidb_ast::AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Drop {
                if_exists,
                names,
            }) => drop_partition_action(catalog, &database, &name, *if_exists, names, ctx)?,
            tidb_ast::AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Add {
                if_not_exists,
                spec,
                ..
            }) => add_partition_action(catalog, &database, &name, *if_not_exists, spec, ctx)?,
            tidb_ast::AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Coalesce {
                count,
                ..
            }) => hash_partition_management(
                catalog,
                &database,
                &name,
                HashManagement::Coalesce(*count),
                ctx,
            )?,
            tidb_ast::AlterTableAction::Partition(tidb_ast::AlterPartitionAction::Exchange {
                partition,
                table,
                with_validation,
            }) => super::exchange_partition::exchange_partition_action(
                catalog,
                &database,
                &name,
                partition,
                table,
                *with_validation,
                current_db,
                ctx,
            )?,
            _ => {
                return Err(DriverError::unsupported(
                    "this ALTER TABLE action is not supported yet",
                ))
            }
        }
    }
    // Go `updateVersionAndTableInfoWithCheck` runs `checkTableInfoValid` on
    // the table each job leaves: a MODIFY that makes an invisible unique key
    // NOT NULL promotes it to an invisible primary key.
    if let Some(crate::TableEntry::Kv(table)) = catalog.table_in(&database, &name) {
        super::indexes::check_invisible_index_on_pk(
            table,
            &table.indexes().iter().collect::<Vec<_>>(),
        )?;
    }
    Ok(())
}

/// The jobs Go's `AlterTable` submits for one specification, for the BDR
/// admission. A specification its builder found nothing to do for (an
/// IF [NOT] EXISTS that matched, an option already in effect) submits none.
fn submitted_jobs(
    action: &tidb_ast::AlterTableAction,
    change: Option<&PreparedAlterChange<'_>>,
    catalog: &Catalog,
    database: &str,
    name: &str,
) -> Vec<super::bdr::SubmittedJob> {
    use super::bdr::SubmittedJob as Job;
    use tidb_ast::{AlterPartitionAction as Partition, AlterTableAction as Action};
    use tidb_model::ActionType as A;
    if let Some(PreparedAlterChange::Metadata(changes)) = change {
        return changes
            .iter()
            .map(|change| {
                Job::new(match change {
                    PreparedMetadataChange::Comment(_) => A::ACTION_MODIFY_TABLE_COMMENT,
                    PreparedMetadataChange::Charset { .. } => {
                        A::ACTION_MODIFY_TABLE_CHARSET_AND_COLLATE
                    }
                    PreparedMetadataChange::Rebase(PreparedAllocatorRebase::Increment(_)) => {
                        A::ACTION_REBASE_AUTO_ID
                    }
                    PreparedMetadataChange::Rebase(PreparedAllocatorRebase::Random(_)) => {
                        A::ACTION_REBASE_AUTO_RANDOM_BASE
                    }
                    PreparedMetadataChange::AutoIdCache(_) => A::ACTION_MODIFY_TABLE_AUTO_IDCACHE,
                    PreparedMetadataChange::Ttl(_) if matches!(action, Action::RemoveTtl(_)) => {
                        A::ACTION_ALTER_TTLREMOVE
                    }
                    PreparedMetadataChange::Ttl(_) => A::ACTION_ALTER_TTLINFO,
                    PreparedMetadataChange::Placement(_) => A::ACTION_ALTER_TABLE_PLACEMENT,
                    PreparedMetadataChange::Affinity(_) => A::ACTION_ALTER_TABLE_AFFINITY,
                    PreparedMetadataChange::EngineAttribute { .. } => {
                        A::ACTION_MODIFY_ENGINE_ATTRIBUTE
                    }
                    PreparedMetadataChange::Rename { .. } => A::ACTION_RENAME_TABLE,
                })
            })
            .collect();
    }
    // The actions this owner runs without a prepared change submit their
    // job unless Go's builder returns early: CACHE on a cached table and
    // NOCACHE on an uncached one do nothing.
    let unprepared = match action {
        Action::Cache(mode) => {
            let cached = matches!(
                catalog.table_in(database, name),
                Some(crate::TableEntry::Kv(table)) if table.is_cached()
            );
            cached == (*mode == tidb_ast::AlterTableCacheMode::NoCache)
        }
        Action::Partition(
            Partition::Truncate { .. }
            | Partition::Drop { .. }
            | Partition::Add { .. }
            | Partition::Coalesce { .. },
        ) => true,
        _ => false,
    };
    if change.is_none() && !unprepared {
        return Vec::new();
    }
    let job = match action {
        Action::AddColumn { .. } | Action::AddColumns { .. } => Job::new(A::ACTION_ADD_COLUMN),
        Action::DropColumn { .. } => Job::new(A::ACTION_DROP_COLUMN),
        Action::DropPrimaryKey(_) => Job::new(A::ACTION_DROP_PRIMARY_KEY),
        // Go `CheckIsDropPrimaryKey`: dropping the index named PRIMARY is
        // dropping the primary key.
        Action::DropIndex { name, .. } if name.eq_ignore_ascii_case("primary") => {
            Job::new(A::ACTION_DROP_PRIMARY_KEY)
        }
        Action::DropIndex { .. } => Job::new(A::ACTION_DROP_INDEX),
        Action::DropForeignKey(_) => Job::new(A::ACTION_DROP_FOREIGN_KEY),
        Action::DropCheck(_) => Job::new(A::ACTION_DROP_CHECK_CONSTRAINT),
        Action::AlterIndexVisibility(_) => Job::new(A::ACTION_ALTER_INDEX_VISIBILITY),
        Action::AlterCheck(_) => Job::new(A::ACTION_ALTER_CHECK_CONSTRAINT),
        Action::AlterColumnDefault(_) => Job::new(A::ACTION_SET_DEFAULT_VALUE),
        Action::RenameIndex(_) => Job::new(A::ACTION_RENAME_INDEX),
        Action::RenameColumn(_) | Action::ModifyColumn { .. } | Action::ChangeColumn { .. } => {
            Job::new(A::ACTION_MODIFY_COLUMN)
        }
        Action::AddIndexConstraint(definition) => match definition.kind {
            tidb_ast::IndexConstraintKind::PrimaryKey => {
                Job::add_index(A::ACTION_ADD_PRIMARY_KEY, true)
            }
            tidb_ast::IndexConstraintKind::Key | tidb_ast::IndexConstraintKind::Index => {
                Job::add_index(A::ACTION_ADD_INDEX, false)
            }
            tidb_ast::IndexConstraintKind::Unique
            | tidb_ast::IndexConstraintKind::UniqueKey
            | tidb_ast::IndexConstraintKind::UniqueIndex => {
                Job::add_index(A::ACTION_ADD_INDEX, true)
            }
            tidb_ast::IndexConstraintKind::Vector | tidb_ast::IndexConstraintKind::Columnar => {
                Job::new(A::ACTION_ADD_COLUMNAR_INDEX)
            }
            tidb_ast::IndexConstraintKind::Fulltext => return Vec::new(),
        },
        Action::AddForeignKey(_) => Job::new(A::ACTION_ADD_FOREIGN_KEY),
        Action::AddCheck(_) => Job::new(A::ACTION_ADD_CHECK_CONSTRAINT),
        Action::Cache(tidb_ast::AlterTableCacheMode::Cache) => {
            Job::new(A::ACTION_ALTER_CACHE_TABLE)
        }
        Action::Cache(tidb_ast::AlterTableCacheMode::NoCache) => {
            Job::new(A::ACTION_ALTER_NO_CACHE_TABLE)
        }
        Action::SetAttributes(_) => Job::new(A::ACTION_ALTER_TABLE_ATTRIBUTES),
        Action::SetStatsOptions(_) => Job::new(A::ACTION_ALTER_TABLE_STATS_OPTIONS),
        Action::SetTiFlashReplica { .. } => Job::new(A::ACTION_SET_TI_FLASH_REPLICA),
        Action::SplitRegion { .. } => Job::new(A::ACTION_ALTER_TABLE_SET_REGION_SPLIT_POLICY),
        Action::Partition(partition) => Job::new(match partition {
            Partition::Add { .. } | Partition::LastPartitionLessThan { .. } => {
                A::ACTION_ADD_TABLE_PARTITION
            }
            Partition::Drop { .. } | Partition::FirstPartitionLessThan { .. } => {
                A::ACTION_DROP_TABLE_PARTITION
            }
            Partition::Truncate { .. } => A::ACTION_TRUNCATE_TABLE_PARTITION,
            Partition::Exchange { .. } => A::ACTION_EXCHANGE_TABLE_PARTITION,
            Partition::Reorganize { .. } | Partition::Coalesce { .. } => {
                A::ACTION_REORGANIZE_PARTITION
            }
            Partition::Repartition(_) => A::ACTION_ALTER_TABLE_PARTITIONING,
            Partition::RemovePartitioning => A::ACTION_REMOVE_PARTITIONING,
            Partition::SetAttributes { .. } => A::ACTION_ALTER_TABLE_PARTITION_ATTRIBUTES,
            Partition::SetOptions { .. } => A::ACTION_ALTER_TABLE_PARTITION_PLACEMENT,
            // Go refuses these before building a job (`ErrGeneralUnsupportedDDL`,
            // `ErrUnsupportedCheckPartition`, ...) or runs none.
            Partition::Check { .. }
            | Partition::ImportTablespace { .. }
            | Partition::DiscardTablespace { .. }
            | Partition::SplitMaxValuePartition { .. }
            | Partition::MergeFirstPartitionLessThan { .. }
            | Partition::Maintain { .. } => return Vec::new(),
        }),
        _ => return Vec::new(),
    };
    vec![job]
}

fn truncate_partition_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    all: bool,
    names: &[String],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let ordinals = {
        let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
            return Err(DriverError::unsupported(
                "ALTER TABLE ... TRUNCATE PARTITION needs a storage-backed table",
            ));
        };
        let Some(partition) = table.partition() else {
            return Err(DriverError::PartitionManagementOnNonpartitioned);
        };
        if all {
            (0..partition.definitions.len()).collect::<Vec<_>>()
        } else {
            let mut ordinals = Vec::with_capacity(names.len());
            for name in names {
                let Some(ordinal) = partition.definitions.iter().position(|definition| {
                    super::table_partition::partition_names_equal(&definition.name, name)
                }) else {
                    // Go `TruncateTablePartition` (`ddl/executor.go:2851`)
                    // passes `name.L` here -- the FOLDED name -- while the
                    // SELECT/DML partition-list errors (`builder.go:6258`,
                    // `show_placement.go:201`) pass `.O` and keep the written
                    // case. The sites genuinely differ, so this one folds and
                    // those stay as they are.
                    return Err(DriverError::UnknownPartition {
                        partition: super::table_partition::go_to_lower(name),
                        table: table_name.to_owned(),
                    });
                };
                // MySQL accepts duplicate names in TRUNCATE PARTITION and
                // truncates that physical partition once.
                if !ordinals.contains(&ordinal) {
                    ordinals.push(ordinal);
                }
            }
            ordinals
        }
    };
    let replacement_ids = ordinals
        .iter()
        .map(|_| catalog.allocate_table_id())
        .collect::<Vec<_>>();
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        unreachable!("the table was resolved before allocating replacement IDs")
    };
    std::sync::Arc::make_mut(table)
        .truncate_partitions(&ordinals, &replacement_ids, ctx)
        .map_err(|error| crate::driver::kv_read_error("truncate partition", error))
}

/// Go's partition-management refusal of a table with AFFINITY
/// (`ErrGeneralUnsupportedDDL`, 8200).
fn refuse_affinity(table: &crate::KvTable, operation: &str) -> Result<(), DriverError> {
    if table.has_affinity() {
        return Err(DriverError::DdlCoded {
            errno: 8200,
            message: format!(
                "Unsupported DDL operation: {operation} of a table with AFFINITY option"
            ),
        });
    }
    Ok(())
}

/// What a HASH/KEY reorganization asks for (Go `hashPartitionManagement`).
enum HashManagement<'a> {
    /// `ADD PARTITION PARTITIONS n` or `ADD PARTITION (definitions)`.
    Add {
        count: u64,
        definitions: &'a [tidb_ast::PartitionDefinition],
    },
    /// `COALESCE PARTITION n`.
    Coalesce(u64),
}

/// Go `hashPartitionManagement` and the `ReorganizePartitions` it calls for
/// a HASH/KEY table: every partition is reorganized into
/// `buildHashPartitionDefinitions`' list -- the existing definitions keep
/// their names, comments and placement policies, the written ones follow,
/// and the rest are named `p{i}` -- whose names must be unique (1517), and
/// every row is re-hashed. Refusals in Go's order: a non-partitioned table
/// (1505), AFFINITY (8200), a COALESCE on another method (1509), a count
/// below one (1515) or one removing the last partition (1508), and a value
/// clause on an added definition (1480).
fn hash_partition_management(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    change: HashManagement<'_>,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let built = {
        let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
            return Err(DriverError::unsupported(
                "ALTER TABLE ... PARTITION needs a storage-backed table",
            ));
        };
        let Some(partition) = table.partition() else {
            return Err(DriverError::PartitionManagementOnNonpartitioned);
        };
        let hash_or_key = matches!(
            partition.kind,
            crate::partition_routing::PartitionKind::Hash
                | crate::partition_routing::PartitionKind::Key
        );
        refuse_affinity(
            table,
            match change {
                HashManagement::Add { .. } => "ADD PARTITION",
                HashManagement::Coalesce(_) => "COALESCE PARTITION",
            },
        )?;
        let existing = &partition.definitions;
        let (count, written) = match change {
            HashManagement::Add { count, definitions } => {
                for definition in definitions {
                    let (method, clause) = match definition.clause {
                        tidb_ast::PartitionDefinitionClause::None => continue,
                        tidb_ast::PartitionDefinitionClause::LessThan(_) => {
                            ("RANGE", "VALUES LESS THAN")
                        }
                        tidb_ast::PartitionDefinitionClause::In(_)
                        | tidb_ast::PartitionDefinitionClause::Default => ("LIST", "VALUES IN"),
                        tidb_ast::PartitionDefinitionClause::History { .. } => {
                            ("SYSTEM_TIME", "VALUES HISTORY")
                        }
                    };
                    return Err(DriverError::PartitionWrongValues { method, clause });
                }
                let added = if definitions.is_empty() {
                    count as usize
                } else {
                    definitions.len()
                };
                (existing.len() + added, definitions)
            }
            HashManagement::Coalesce(count) => {
                // Go's ErrCoalesceOnlyOnHashPartition fires on HASH and KEY
                // alike (oracle g-partition: COALESCE on a KEY table is ok).
                if !hash_or_key {
                    return Err(DriverError::CoalesceOnlyOnHashPartition);
                }
                if count < 1 {
                    return Err(DriverError::CoalescePartitionNoPartition);
                }
                if count as usize >= existing.len() {
                    return Err(DriverError::PartitionDropLast);
                }
                (existing.len() - count as usize, &[][..])
            }
        };
        if count > super::table_partition::MAX_PARTITIONS as usize {
            return Err(DriverError::PartitionTooMany);
        }
        let mut built = Vec::with_capacity(count);
        for ordinal in 0..count {
            if let Some(definition) = existing.get(ordinal) {
                built.push((
                    definition.name.clone(),
                    definition.comment.clone(),
                    definition.placement_policy.clone(),
                ));
            } else if let Some(definition) = written.get(ordinal - existing.len()) {
                built.push((
                    definition.name.clone(),
                    super::table_partition::partition_definition_comment(definition, ctx, false)?,
                    super::table_partition::partition_definition_placement(definition),
                ));
            } else {
                built.push((format!("p{ordinal}"), String::new(), None));
            }
        }
        // Go `checkPartitionNameUnique` over the reorganized definitions.
        let mut seen = std::collections::HashSet::with_capacity(built.len());
        for (name, _, _) in &built {
            if !seen.insert(super::table_partition::go_to_lower(name)) {
                return Err(DriverError::PartitionSameName(name.clone()));
            }
        }
        // Go `handlePartitionPlacement`: each policy must exist.
        for (_, _, policy) in &mut built {
            if let Some(reference) = policy.as_mut() {
                let Some(found) = catalog.policy(reference.name.original()) else {
                    return Err(DriverError::PlacementPolicyNotExists(
                        reference.name.original().to_owned(),
                    ));
                };
                reference.id = found.id;
            }
        }
        let mut definitions = built
            .into_iter()
            .map(
                |(name, comment, placement_policy)| crate::partition_routing::PartitionDef {
                    id: 0,
                    name,
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment,
                    placement_policy,
                    storage_class: Default::default(),
                },
            )
            .collect::<Vec<_>>();
        // Go `buildPartitionDefinitionsInfo` ->
        // `rebuildStorageClassForPartitionDefinitions` over the reorganized
        // list.
        assign_partition_storage_classes(table, &partition.kind, &[], &mut definitions, ctx)?;
        definitions
    };
    // Every partition gets a fresh physical id, as Go's reorganize does.
    let mut definitions = built;
    for definition in &mut definitions {
        definition.id = catalog.allocate_table_id();
    }
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        unreachable!("the table was resolved above")
    };
    std::sync::Arc::make_mut(table)
        .rehash_hash_partitions(definitions, ctx)
        .map_err(|error| crate::driver::kv_read_error("reorganize partition", error))?;
    // Go `ReorganizePartitions` warns on success, which both ADD and
    // COALESCE reach (oracle m24: COALESCE answers ok with this warning).
    ctx.append_warning_parts(
        1105,
        "The statistics of related partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now",
    );
    Ok(())
}

fn drop_partition_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    if_exists: bool,
    names: &[String],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let ordinals = {
        let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
            return Err(DriverError::unsupported(
                "ALTER TABLE ... DROP PARTITION needs a storage-backed table",
            ));
        };
        let Some(partition) = table.partition() else {
            return Err(DriverError::PartitionManagementOnNonpartitioned);
        };
        if !matches!(
            partition.kind,
            PartitionKind::Range { .. }
                | PartitionKind::RangeColumns { .. }
                | PartitionKind::List { .. }
                | PartitionKind::ListColumns { .. }
        ) {
            return Err(DriverError::PartitionOnlyRangeList("DROP"));
        }
        refuse_affinity(table, "DROP PARTITION")?;
        if partition.definitions.len() <= names.len() {
            return Err(DriverError::PartitionDropLast);
        }
        let mut ordinals = Vec::with_capacity(names.len());
        for name in names {
            let Some(ordinal) = partition.definitions.iter().position(|definition| {
                super::table_partition::partition_names_equal(&definition.name, name)
            }) else {
                if if_exists {
                    ctx.append_suppressed(&DriverError::PartitionDropNonexistent);
                    return Ok(());
                }
                return Err(DriverError::PartitionDropNonexistent);
            };
            if ordinals.contains(&ordinal) {
                if if_exists {
                    ctx.append_suppressed(&DriverError::PartitionDropNonexistent);
                    return Ok(());
                }
                return Err(DriverError::PartitionDropNonexistent);
            }
            ordinals.push(ordinal);
        }
        ordinals
    };
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        unreachable!("the table was resolved above")
    };
    std::sync::Arc::make_mut(table)
        .drop_partitions(&ordinals, ctx)
        .map_err(|error| crate::driver::kv_read_error("drop partition", error))
}

fn add_partition_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    if_not_exists: bool,
    spec: &tidb_ast::AddPartitionSpec,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    // Go `AddTablePartitions` refuses a table with AFFINITY once it is known
    // to be partitioned.
    if let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) {
        if table.partition().is_some() {
            refuse_affinity(table, "ADD PARTITION")?;
        }
    }
    // Go `AddTablePartitions`: on a HASH/KEY table an ADD is a reorganize
    // of every partition (`hashPartitionManagement`), whichever form it was
    // written in.
    let hash_or_key = matches!(
        catalog.table_in(database, table_name),
        Some(crate::TableEntry::Kv(table)) if table.partition().is_some_and(|partition| matches!(
            partition.kind,
            PartitionKind::Hash | PartitionKind::Key
        ))
    );
    let definitions = match spec {
        tidb_ast::AddPartitionSpec::Definitions(definitions) if hash_or_key => {
            let change = HashManagement::Add {
                count: 0,
                definitions,
            };
            return hash_partition_management(catalog, database, table_name, change, ctx);
        }
        tidb_ast::AddPartitionSpec::Definitions(definitions) => definitions,
        tidb_ast::AddPartitionSpec::Count(count) => {
            if !hash_or_key {
                return Err(DriverError::unsupported(
                    "ADD PARTITION PARTITIONS n on a non-HASH table is not supported yet",
                ));
            }
            let change = HashManagement::Add {
                count: *count,
                definitions: &[],
            };
            return hash_partition_management(catalog, database, table_name, change, ctx);
        }
    };
    if definitions.is_empty() {
        return Err(DriverError::PartitionsMustBeDefined("LIST"));
    }
    if definitions
        .iter()
        .any(|definition| !definition.options.is_empty() || !definition.sub_partitions.is_empty())
    {
        return Err(DriverError::unsupported(
            "ALTER TABLE ... ADD PARTITION options and subpartitions are not supported yet",
        ));
    }

    let added_kind = {
        let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
            return Err(DriverError::unsupported(
                "ALTER TABLE ... ADD PARTITION needs a storage-backed table",
            ));
        };
        let Some(partition) = table.partition() else {
            return Err(DriverError::PartitionManagementOnNonpartitioned);
        };
        if partition.definitions.len() + definitions.len()
            > super::table_partition::MAX_PARTITIONS as usize
        {
            return Err(DriverError::PartitionTooMany);
        }
        for definition in definitions {
            let duplicate_existing = partition.definitions.iter().any(|old| {
                super::table_partition::partition_names_equal(&old.name, &definition.name)
            });
            let duplicate_added = definitions
                .iter()
                .filter(|candidate| {
                    super::table_partition::partition_names_equal(&candidate.name, &definition.name)
                })
                .count()
                > 1;
            if duplicate_existing || duplicate_added {
                if if_not_exists {
                    ctx.append_suppressed(&DriverError::PartitionSameName(definition.name.clone()));
                    return Ok(());
                }
                return Err(DriverError::PartitionSameName(definition.name.clone()));
            }
        }

        let names = table
            .visible_columns()
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let types = table
            .visible_columns()
            .iter()
            .map(|column| column.field_type.clone())
            .collect::<Vec<_>>();
        match &partition.kind {
            // A table wearing `PartitionTypeNone` is mid-`ALTER ...
            // PARTITION BY`; it has no LIST values for a definition to join.
            PartitionKind::None => {
                return Err(DriverError::unsupported(
                    "ADD PARTITION while the table is being repartitioned".to_owned(),
                ))
            }
            PartitionKind::List {
                values,
                null_partition,
                default_partition,
                unsigned,
            } => {
                if default_partition.is_some() {
                    return Err(DriverError::unsupported(
                        "ADD List partition, already contains DEFAULT partition. Please use REORGANIZE PARTITION instead",
                    ));
                }
                // On the ALTER path the added definitions are checked on
                // their own, so a collision among them is raised here rather
                // than deferred: there is no earlier partition-level check
                // for it to lose to.
                let (added, duplicate) =
                    super::table_partition_list::build_list_values_with_unsigned(
                        definitions,
                        *unsigned,
                        ctx,
                        // Go reaches these builders through the SAME
                        // `buildPartitionDefinitionsInfo` loop a CREATE uses, so
                        // an added partition's name faces the 64-rune rule at the
                        // same point. Per-partition options are refused above, so
                        // there is no comment to validate.
                        &mut |ordinal: usize| {
                            definitions.get(ordinal).map_or(Ok(()), |definition| {
                                super::table_partition::check_too_long_partition_name(
                                    &definition.name,
                                )
                            })
                        },
                    )?;
                if let Some(duplicate) = duplicate {
                    return Err(duplicate);
                }
                let PartitionKind::List {
                    values: added_values,
                    null_partition: added_null,
                    default_partition: added_default,
                    ..
                } = &added
                else {
                    unreachable!()
                };
                if added_values
                    .iter()
                    .any(|(value, _)| values.iter().any(|(old, _)| *old as u64 == *value as u64))
                    || (null_partition.is_some() && added_null.is_some())
                    || (default_partition.is_some() && added_default.is_some())
                {
                    return Err(DriverError::PartitionDuplicateListValue);
                }
                added
            }
            PartitionKind::ListColumns {
                keys,
                default_partition,
                ..
            } => {
                if default_partition.is_some() {
                    return Err(DriverError::unsupported(
                        "ADD List partition, already contains DEFAULT partition. Please use REORGANIZE PARTITION instead",
                    ));
                }
                let columns = partition
                    .dependencies
                    .iter()
                    .map(|name| vec![name.clone()])
                    .collect::<Vec<_>>();
                let (_, added) = super::table_partition_list::build_list_columns_values(
                    &columns,
                    definitions,
                    &names,
                    &types,
                    ctx,
                    // The added definitions are new metadata this statement
                    // is writing, so they face the CREATE-time rules.
                    super::table_partition::PartitionBuildMode::Create,
                    // Go reaches these builders through the SAME
                    // `buildPartitionDefinitionsInfo` loop a CREATE uses, so
                    // an added partition's name faces the 64-rune rule at the
                    // same point. Per-partition options are refused above, so
                    // there is no comment to validate.
                    &mut |ordinal: usize| {
                        definitions.get(ordinal).map_or(Ok(()), |definition| {
                            super::table_partition::check_too_long_partition_name(&definition.name)
                        })
                    },
                )?;
                let PartitionKind::ListColumns {
                    keys: added_keys,
                    default_partition: added_default,
                    ..
                } = &added
                else {
                    unreachable!()
                };
                if added_keys.keys().any(|key| keys.contains_key(key))
                    || (default_partition.is_some() && added_default.is_some())
                {
                    return Err(DriverError::PartitionDuplicateListValue);
                }
                added
            }
            PartitionKind::Range {
                less_than,
                unsigned,
            } => {
                // `ALTER TABLE ... ADD PARTITION` is a WRITTEN clause, so it
                // validates like a CREATE.
                let added = super::table_partition_range::build_range_bounds_with_unsigned(
                    definitions,
                    *unsigned,
                    ctx,
                    super::table_partition::PartitionBuildMode::Create,
                    &mut |ordinal: usize| {
                        definitions.get(ordinal).map_or(Ok(()), |definition| {
                            super::table_partition::check_too_long_partition_name(&definition.name)
                        })
                    },
                )?;
                // Go `checkAddPartitionValue` (`ddl/partition.go:428`) walks
                // EVERY added definition, not just the first. It reads the
                // last EXISTING bound as the running value, refuses outright
                // if that one is already MAXVALUE, and then for each new
                // definition in order: a MAXVALUE at the last position ends
                // the check, a MAXVALUE anywhere else is 1481, and any other
                // bound must be strictly greater than the running value.
                //
                // Comparing only `less_than.last()` against `added.first()`
                // let `ADD PARTITION (p2 VALUES LESS THAN (30), p3 VALUES
                // LESS THAN (20))` through -- Go answers 1493.
                let mut current = match less_than.last() {
                    Some(RangeBound::MaxValue) => {
                        return Err(DriverError::PartitionMaxValueNotLast)
                    }
                    Some(RangeBound::Value(value)) => Some(*value),
                    None => None,
                };
                for (index, bound) in added.iter().enumerate() {
                    match bound {
                        RangeBound::MaxValue => {
                            if index == added.len() - 1 {
                                break;
                            }
                            return Err(DriverError::PartitionMaxValueNotLast);
                        }
                        RangeBound::Value(value) => {
                            if let Some(previous) = current {
                                let increases = if *unsigned {
                                    (*value as u64) > (previous as u64)
                                } else {
                                    *value > previous
                                };
                                if !increases {
                                    return Err(DriverError::PartitionRangeNotIncreasing);
                                }
                            }
                            current = Some(*value);
                        }
                    }
                }
                PartitionKind::Range {
                    less_than: added,
                    unsigned: *unsigned,
                }
            }
            PartitionKind::RangeColumns {
                less_than,
                field_types,
            } => {
                let columns = partition
                    .dependencies
                    .iter()
                    .map(|name| vec![name.clone()])
                    .collect::<Vec<_>>();
                let (_, added_types, added_bounds) =
                    super::table_partition_range::build_range_columns_bounds(
                        &columns,
                        definitions,
                        &names,
                        &types,
                        ctx,
                        super::table_partition::PartitionBuildMode::Create,
                        &mut |ordinal: usize| {
                            definitions.get(ordinal).map_or(Ok(()), |definition| {
                                super::table_partition::check_too_long_partition_name(
                                    &definition.name,
                                )
                            })
                        },
                    )?;
                // Go validates an addition by CONCATENATING it onto the
                // existing definitions and running the whole CREATE-time
                // battery over the result --
                // `CheckAndUpdateAddedPartitionDefinitions`
                // (`ddl/executor.go`) appends
                // `clonePartitionDefinitions(meta.Partition.Definitions)`
                // ahead of the added ones and calls
                // `checkPartitionDefinitionConstraints` on the combined list.
                //
                // RANGE COLUMNS reaches that battery and NOTHING else:
                // `checkAddPartitionValue` runs its increase loop only when
                // `len(meta.Partition.Columns) == 0`, which is the scalar
                // form. So comparing one pair here was the only check this
                // method had, and additions that did not increase among
                // THEMSELVES were accepted.
                let mut combined = less_than.clone();
                combined.extend(added_bounds.iter().cloned());
                super::table_partition_range::check_range_columns_strictly_increasing(
                    &combined,
                    field_types,
                )?;
                PartitionKind::RangeColumns {
                    less_than: added_bounds.clone(),
                    field_types: added_types,
                }
            }
            PartitionKind::Hash | PartitionKind::Key => {
                return Err(DriverError::PartitionOnlyRangeList("ADD"));
            }
        }
    };

    // The added partitions carry the same STORED text a CREATE would give
    // them, because `SHOW CREATE TABLE` prints from it: an empty `InValues`
    // is Go's own marker for a bare `DEFAULT` partition
    // (`ddl/partition.go:5210`), so leaving it empty here would print an
    // added LIST partition as `DEFAULT` and lose the values it was given.
    //
    // Per-partition OPTIONS are refused above, so no comment can reach here.
    let list_field_types = match &added_kind {
        PartitionKind::ListColumns { field_types, .. } => field_types.clone(),
        _ => Vec::new(),
    };
    // The bound TEXT a RANGE addition prints, rendered from the folded bound
    // by the SAME helper a CREATE uses, so the two cannot drift.
    let range_bound_text = |ordinal: usize| match &added_kind {
        PartitionKind::Range {
            less_than,
            unsigned,
        } => less_than
            .get(ordinal)
            .map(|bound| {
                vec![super::table_partition::stored_range_bound_text(
                    *bound, *unsigned,
                )]
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    };
    let mut added_definitions = Vec::with_capacity(definitions.len());
    for (ordinal, definition) in definitions.iter().enumerate() {
        added_definitions.push(PartitionDef {
            // Allocated once every check has passed, as Go's
            // `assignPartitionIDs` runs last.
            id: 0,
            name: definition.name.clone(),
            less_than: range_bound_text(ordinal),
            in_values: super::table_partition::stored_in_values(
                Some(definition),
                &list_field_types,
                ctx,
            )?,
            comment: String::new(),
            // Per-partition OPTIONS are refused on this path, so an added
            // partition names no policy of its own.
            placement_policy: None,
            storage_class: Default::default(),
        });
    }
    // Go `CheckAndUpdateAddedPartitionDefinitions`: the storage classes are
    // resolved over the existing definitions followed by the added ones, and
    // only the added ones take the result.
    {
        let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
            unreachable!("the table was resolved above")
        };
        let partition = table
            .partition()
            .expect("ADD PARTITION was admitted for a partitioned table");
        let existing = partition.definitions.iter().collect::<Vec<_>>();
        assign_partition_storage_classes(
            table,
            &partition.kind,
            &existing,
            &mut added_definitions,
            ctx,
        )?;
    }
    for definition in &mut added_definitions {
        definition.id = catalog.allocate_table_id();
    }
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        unreachable!("the table was resolved above")
    };
    std::sync::Arc::make_mut(table).append_partitions(added_definitions, added_kind, ctx);
    Ok(())
}

/// Go `rebuildStorageClassForPartitions`: resolves the table's
/// ENGINE_ATTRIBUTE over `preceding` followed by `definitions` and stores the
/// result on `definitions`. A table whose attribute names no storage class
/// leaves them unset.
fn assign_partition_storage_classes(
    table: &crate::KvTable,
    kind: &PartitionKind,
    preceding: &[&PartitionDef],
    definitions: &mut [PartitionDef],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let Some(settings) = super::storage_class::storage_class_settings_of(table.engine_attribute())?
    else {
        return Ok(());
    };
    let combined = preceding
        .iter()
        .copied()
        .chain(definitions.iter())
        .collect::<Vec<_>>();
    let classes =
        super::storage_class::build_storage_class_for_partitions(&settings, kind, &combined, ctx)?;
    for (definition, class) in definitions
        .iter_mut()
        .zip(classes.into_iter().skip(preceding.len()))
    {
        definition.storage_class = class;
    }
    Ok(())
}

/// Go's multi-action ALTER validator evaluates a dropped index against an FK
/// added by the same statement before either action is committed. If the
/// existing index is the key the new constraint would rely on, dropping it is
/// refused with 1553 rather than allowing the later ADD to auto-create a
/// replacement index. This is the exact `drop idx_c, add constraint fk_c`
/// shape in `TestAddForeignKey`.
fn reject_drop_index_used_by_added_foreign_key(
    catalog: &Catalog,
    database: &str,
    table_name: &str,
    actions: &[tidb_ast::AlterTableAction],
) -> Result<(), DriverError> {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        return Ok(());
    };
    let drops: Vec<&str> = actions
        .iter()
        .filter_map(|action| match action {
            tidb_ast::AlterTableAction::DropIndex { if_exists: _, name } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    if drops.is_empty() {
        return Ok(());
    }
    let added: Vec<&tidb_ast::ForeignKeyConstraintDefinition> = actions
        .iter()
        .filter_map(|action| match action {
            tidb_ast::AlterTableAction::AddForeignKey(definition) => Some(definition),
            _ => None,
        })
        .collect();
    for index_name in drops {
        let Some(index) = table
            .indexes()
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case(index_name))
        else {
            continue;
        };
        let index_offsets = &index.column_offsets;
        for definition in &added {
            let fk_names = super::indexes::index_part_names(&definition.parts)?;
            let fk_offsets: Vec<usize> = fk_names
                .iter()
                .filter_map(|column| {
                    table
                        .columns
                        .iter()
                        .position(|candidate| candidate.name.eq_ignore_ascii_case(column))
                })
                .collect();
            if !fk_offsets.is_empty() && index_offsets.starts_with(&fk_offsets) {
                return Err(DriverError::DropIndexNeededInForeignKey(
                    index_name.to_owned(),
                ));
            }
        }
    }
    Ok(())
}

/// Values owned by the admitted metadata job, never a whole table snapshot:
/// applying one job must retain sibling column/index changes.
enum PreparedMetadataChange {
    Comment(String),
    Charset {
        target: TableCharset,
        overwrite_columns: bool,
    },
    Rebase(PreparedAllocatorRebase),
    AutoIdCache(u64),
    Ttl(Option<tidb_model::TTLInfo>),
    Placement(Option<tidb_model::PolicyRefInfo>),
    Affinity(Option<tidb_model::TableAffinityInfo>),
    /// Go `ActionModifyEngineAttribute`: the attribute, and the storage
    /// classes it resolves to when it names one.
    EngineAttribute {
        attribute: String,
        table_class: Option<super::storage_class::StorageClass>,
        partition_classes: Option<Vec<super::storage_class::StorageClass>>,
    },
    Rename {
        database: String,
        name: String,
    },
}

impl PreparedMetadataChange {
    fn execute(
        self,
        catalog: &mut Catalog,
        database: &str,
        name: &str,
        allocators: &mut PreparedAllocatorChanges,
    ) -> Result<(), DriverError> {
        match self {
            Self::Rebase(rebase) => allocators.rebases.push(rebase),
            Self::Rename {
                database: to_db,
                name: to_name,
            } => {
                crate::foreign_key::rewrite_table_references(
                    catalog, database, name, &to_db, &to_name,
                );
                catalog.rename_table(database, name, &to_db, &to_name);
            }
            change => {
                let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, name)
                else {
                    return Err(DriverError::unsupported(
                        "ALTER TABLE needs a storage-backed table",
                    ));
                };
                let table = std::sync::Arc::make_mut(table);
                match change {
                    Self::Comment(comment) => table.set_comment(comment),
                    Self::Charset {
                        target,
                        overwrite_columns,
                    } => {
                        table.set_charset(target);
                        if overwrite_columns {
                            for column in table.columns_mut() {
                                if column.field_type.is_character_string() {
                                    column.field_type.set_charset_name(target.charset.name());
                                    column.field_type.set_collation(target.collation);
                                }
                            }
                        }
                    }
                    Self::AutoIdCache(cache) => table
                        .set_auto_id_cache(cache)
                        .map_err(DriverError::unsupported)?,
                    Self::Ttl(info) => table.set_ttl_info(info),
                    Self::Placement(policy) => table.set_placement_policy(policy),
                    Self::Affinity(affinity) => table.set_affinity(affinity),
                    Self::EngineAttribute {
                        attribute,
                        table_class,
                        partition_classes,
                    } => {
                        table.set_engine_attribute(attribute);
                        if let Some(class) = table_class {
                            table.set_storage_class(class);
                        }
                        if let (Some(classes), Some(partition)) =
                            (partition_classes, table.partition_mut())
                        {
                            for (definition, class) in partition.definitions.iter_mut().zip(classes)
                            {
                                definition.storage_class = class;
                            }
                        }
                    }
                    Self::Rebase(_) | Self::Rename { .. } => unreachable!("handled above"),
                }
            }
        }
        Ok(())
    }
}

fn is_metadata_change(action: &tidb_ast::AlterTableAction) -> bool {
    matches!(
        action,
        tidb_ast::AlterTableAction::SetTableOptions { .. }
            | tidb_ast::AlterTableAction::ConvertCharacterSet { .. }
            | tidb_ast::AlterTableAction::RenameTable { .. }
            | tidb_ast::AlterTableAction::RemoveTtl(_)
            | tidb_ast::AlterTableAction::SetKeysEnabled(_)
            | tidb_ast::AlterTableAction::WithValidation
            | tidb_ast::AlterTableAction::WithoutValidation
            | tidb_ast::AlterTableAction::OrderByColumns { .. }
    )
}

fn reject_metadata_multi_job(multi_schema: bool, job: &str) -> Result<(), DriverError> {
    if multi_schema {
        return Err(DriverError::DdlCoded {
            errno: 8200,
            message: format!("Unsupported multi schema change for {job}"),
        });
    }
    Ok(())
}

fn prepare_metadata_change(
    action: &tidb_ast::AlterTableAction,
    catalog: &Catalog,
    database: &str,
    name: &str,
    current_db: &str,
    ctx: &crate::StmtContext,
    multi_schema: bool,
    charset_handled: &mut bool,
) -> Result<Vec<PreparedMetadataChange>, DriverError> {
    let table = column_changes::table_of(catalog, database, name)?;
    match action {
        tidb_ast::AlterTableAction::SetTableOptions { options } => prepare_table_options(
            catalog,
            database,
            name,
            options,
            ctx,
            multi_schema,
            charset_handled,
        ),
        tidb_ast::AlterTableAction::ConvertCharacterSet { charset, collation } => {
            if *charset_handled {
                return Ok(Vec::new());
            }
            let target = prepare_convert_table_charset(
                table,
                charset.as_deref(),
                collation.as_deref(),
                catalog.max_index_length(),
            )?;
            *charset_handled = true;
            Ok(vec![PreparedMetadataChange::Charset {
                target,
                overwrite_columns: true,
            }])
        }
        tidb_ast::AlterTableAction::RemoveTtl(_) => {
            if table.ttl_info().is_none() {
                return Ok(Vec::new());
            }
            reject_metadata_multi_job(multi_schema, "alter table no_ttl")?;
            Ok(vec![PreparedMetadataChange::Ttl(None)])
        }
        tidb_ast::AlterTableAction::RenameTable { new_name } => {
            let (to_db, to_name) = crate::driver::split_table_path_pub(new_name, current_db)?;
            if !super::check_rename(super::RenameAdmission {
                source: (database, name),
                target: (to_db, to_name),
                source_exists: true,
                target_schema_exists: catalog.has_database(to_db),
                target_exists: catalog.table_in(to_db, to_name).is_some(),
                source_is_view: false,
                is_alter: true,
            })? {
                return Ok(Vec::new());
            }
            reject_metadata_multi_job(multi_schema, "rename table")?;
            Ok(vec![PreparedMetadataChange::Rename {
                database: to_db.to_owned(),
                name: to_name.to_owned(),
            }])
        }
        tidb_ast::AlterTableAction::SetKeysEnabled(_) => Ok(Vec::new()),
        tidb_ast::AlterTableAction::WithValidation
        | tidb_ast::AlterTableAction::WithoutValidation => {
            let validation = if matches!(action, tidb_ast::AlterTableAction::WithValidation) {
                "WITH"
            } else {
                "WITHOUT"
            };
            ctx.append_warning_parts(
                8200,
                &format!("ALTER TABLE {validation} VALIDATION is currently unsupported"),
            );
            Ok(Vec::new())
        }
        tidb_ast::AlterTableAction::OrderByColumns { .. } => {
            if table
                .columns
                .iter()
                .any(|column| column.field_type.has_flag(PRI_KEY_FLAG))
            {
                ctx.append_warning_parts(1105, &format!("ORDER BY ignored as there is a user-defined clustered index in the table '{name}'"));
            }
            Ok(Vec::new())
        }
        _ => unreachable!("metadata action selected"),
    }
}

/// Go AlterTable builds each option's job against the original table. The
/// compression precheck precedes every option; charset and TTL are grouped
/// when their first option is visited, while placement follows the loop.
fn prepare_table_options(
    catalog: &Catalog,
    database: &str,
    name: &str,
    options: &[tidb_ast::TableOption],
    ctx: &crate::StmtContext,
    multi_schema: bool,
    charset_handled: &mut bool,
) -> Result<Vec<PreparedMetadataChange>, DriverError> {
    let unsupported = || DriverError::DdlCoded {
        errno: 8200,
        message: "This type of ALTER TABLE is currently unsupported".to_owned(),
    };
    // Go's AlterTableOption arm validates ENGINE_ATTRIBUTE and STORAGE_CLASS
    // before any other option.
    let engine_attribute = super::storage_class::engine_attribute_from_table_options(options)?;
    if options.iter().any(|option| matches!(option, tidb_ast::TableOption::Compression(value) if !value.eq_ignore_ascii_case("none"))) {
        return Err(unsupported());
    }
    let table = column_changes::table_of(catalog, database, name)?;
    let mut changes = Vec::new();
    let mut ttl_handled = false;
    let mut placement = None;
    for option in options {
        match option {
            tidb_ast::TableOption::AutoIncrement(value)
            | tidb_ast::TableOption::ForceAutoIncrement(value) => {
                let next = value.parse::<u64>().map_err(|_| {
                    DriverError::unsupported("AUTO_INCREMENT= needs an integer value")
                })? as i64;
                // Go RebaseAutoID also admits tables without an explicit
                // AUTO_INCREMENT column; the shared row-ID allocator owns
                // the heap handle when SepAutoInc is false.
                let force = matches!(option, tidb_ast::TableOption::ForceAutoIncrement(_));
                let rebase = table
                    .prepare_rebase_auto_increment(next, force)
                    .map_err(auto_increment_rebase_error)?;
                warn_adjusted_rebase(ctx, &rebase);
                changes.push(PreparedMetadataChange::Rebase(
                    PreparedAllocatorRebase::Increment(rebase),
                ));
            }
            tidb_ast::TableOption::AutoRandomBase(value)
            | tidb_ast::TableOption::ForceAutoRandomBase(value) => {
                let next = value.parse::<u64>().map_err(|_| {
                    DriverError::unsupported("AUTO_RANDOM_BASE needs an integer value")
                })? as i64;
                let force = matches!(option, tidb_ast::TableOption::ForceAutoRandomBase(_));
                let rebase = table
                    .prepare_rebase_auto_random(next, force)
                    .map_err(super::auto_random::rebase_error)?;
                warn_adjusted_rebase(ctx, &rebase);
                reject_metadata_multi_job(multi_schema, "rebase auto_random ID")?;
                changes.push(PreparedMetadataChange::Rebase(
                    PreparedAllocatorRebase::Random(rebase),
                ));
            }
            tidb_ast::TableOption::AutoIdCache(value) => {
                let cache = value.parse::<u64>().map_err(|_| {
                    DriverError::unsupported("AUTO_ID_CACHE needs an integer value")
                })?;
                if cache > i64::MAX as u64 {
                    return Err(DriverError::unsupported(
                        "table option auto_id_cache overflows int64",
                    ));
                }
                table
                    .validate_auto_id_cache(cache)
                    .map_err(DriverError::unsupported)?;
                reject_metadata_multi_job(multi_schema, "modify auto id cache")?;
                changes.push(PreparedMetadataChange::AutoIdCache(cache));
            }
            tidb_ast::TableOption::Comment(comment) => {
                changes.push(PreparedMetadataChange::Comment(
                    super::normalize_table_comment(comment, name, ctx)?,
                ))
            }
            // Go `AlterTableAffinity`.
            tidb_ast::TableOption::Affinity(level) => {
                let affinity = super::table_affinity(level)?;
                super::validate_table_affinity(
                    table.temp_table_type() != tidb_model::TempTableType::NONE,
                    table.partition().is_some(),
                    affinity.as_ref(),
                )?;
                changes.push(PreparedMetadataChange::Affinity(affinity));
            }
            tidb_ast::TableOption::CharacterSet(_) | tidb_ast::TableOption::Collate(_) => {
                if !*charset_handled {
                    let target = alter_table_charset_pair(options, table.charset())?;
                    // Preserve the existing supported conversion boundary.
                    if matches!(target.charset, Charset::Utf8 | Charset::Gbk)
                        && target.charset != table.charset().charset
                    {
                        return Err(DriverError::DdlCoded {
                            errno: 8200,
                            message: "unsupported alter table charset operation".to_owned(),
                        });
                    }
                    changes.push(PreparedMetadataChange::Charset {
                        target,
                        overwrite_columns: false,
                    });
                    *charset_handled = true;
                }
            }
            tidb_ast::TableOption::Ttl { .. }
            | tidb_ast::TableOption::TtlEnable(_)
            | tidb_ast::TableOption::TtlJobInterval(_) => {
                if !ttl_handled {
                    let info = prepare_ttl_info_or_enable(catalog, database, name, options)?;
                    reject_metadata_multi_job(multi_schema, "alter table ttl")?;
                    changes.push(PreparedMetadataChange::Ttl(info));
                    ttl_handled = true;
                }
            }
            tidb_ast::TableOption::PlacementPolicy(policy) => placement = Some(policy),
            tidb_ast::TableOption::Engine(_)
            | tidb_ast::TableOption::RowFormat(_)
            | tidb_ast::TableOption::Compression(_)
            | tidb_ast::TableOption::EngineAttribute(_)
            | tidb_ast::TableOption::StorageClass(_) => {}
            _ => return Err(unsupported()),
        }
    }
    // Go `AlterTableEngineAttribute`, after the option loop; its job
    // (`onModifyTableEngineAttribute`) re-resolves the table's and every
    // partition's storage class when the attribute names one.
    if let Some(attribute) = engine_attribute {
        let (table_class, partition_classes) =
            match super::storage_class::storage_class_settings_of(&attribute)? {
                None => (None, None),
                Some(settings) => {
                    let partition_classes = table
                        .partition()
                        .filter(|partition| !partition.definitions.is_empty())
                        .map(|partition| {
                            let definitions = partition.definitions.iter().collect::<Vec<_>>();
                            super::storage_class::build_storage_class_for_partitions(
                                &settings,
                                &partition.kind,
                                &definitions,
                                ctx,
                            )
                        })
                        .transpose()?;
                    (
                        Some(super::storage_class::build_storage_class_for_table(
                            &settings,
                        )),
                        partition_classes,
                    )
                }
            };
        changes.push(PreparedMetadataChange::EngineAttribute {
            attribute,
            table_class,
            partition_classes,
        });
    }
    if let Some(policy) = placement {
        let reference = if policy.eq_ignore_ascii_case("default") {
            None
        } else {
            let found = catalog
                .policy(policy)
                .ok_or_else(|| DriverError::PlacementPolicyNotExists(policy.clone()))?;
            Some(tidb_model::PolicyRefInfo {
                id: found.id,
                name: tidb_ast::CiString::new(policy.clone()),
            })
        };
        reject_metadata_multi_job(multi_schema, "alter table placement")?;
        changes.push(PreparedMetadataChange::Placement(reference));
    }
    Ok(changes)
}

fn alter_table_charset_pair(
    options: &[tidb_ast::TableOption],
    fallback: TableCharset,
) -> Result<TableCharset, DriverError> {
    let mut charset: Option<Charset> = None;
    let mut collation = None;
    for option in options {
        match option {
            tidb_ast::TableOption::CharacterSet(name) => {
                let value = Charset::from_name(name).ok_or(DriverError::DdlCoded {
                    errno: tidb_error::tidb::errcode::ErrUnknownCharacterSet,
                    message: format!("Unknown character set: '{name}'"),
                })?;
                if let Some(previous) = charset {
                    if previous != value {
                        return Err(DriverError::DdlCoded {
                            errno: tidb_error::tidb::errcode::ErrConflictingDeclarations,
                            message: format!(
                                "Conflicting declarations: 'CHARACTER SET {}' and 'CHARACTER SET {}'",
                                previous.name(),
                                value.name()
                            ),
                        });
                    }
                }
                charset = Some(value);
                collation.get_or_insert(value.default_collation());
            }
            tidb_ast::TableOption::Collate(name) => {
                let value = Collation::from_name(name).ok_or(DriverError::DdlCoded {
                    errno: tidb_error::tidb::errcode::ErrUnknownCollation,
                    message: format!("Unknown collation: '{name}'"),
                })?;
                if let Some(charset) = charset {
                    if value.charset() != charset {
                        return Err(DriverError::DdlCoded {
                            errno: tidb_error::tidb::errcode::ErrCollationCharsetMismatch,
                            message: format!(
                                "Collation '{}' is not valid for CHARACTER SET '{}'",
                                value.name(),
                                charset.name()
                            ),
                        });
                    }
                }
                charset.get_or_insert(value.charset());
                collation = Some(value);
            }
            _ => {}
        }
    }
    Ok(TableCharset {
        charset: charset.unwrap_or(fallback.charset),
        collation: collation
            .unwrap_or_else(|| charset.unwrap_or(fallback.charset).default_collation()),
    })
}

/// Go `checkAlterTableCharset` for `CONVERT TO CHARACTER SET`
/// (`needsOverwriteCols`): the table's own charset change, then every
/// index's length under the new charset, the VARCHAR length limit, and each
/// string column's charset/collation change -- an indexed column may not
/// move to an incompatible collation under the new collation framework.
fn prepare_convert_table_charset(
    table: &crate::KvTable,
    charset: Option<&str>,
    collation: Option<&str>,
    max_index_length: i64,
) -> Result<TableCharset, DriverError> {
    let current = table.charset();
    let options = [
        charset.map(|name| tidb_ast::TableOption::CharacterSet(name.to_owned())),
        collation.map(|name| tidb_ast::TableOption::Collate(name.to_owned())),
    ];
    let options: Vec<_> = options.into_iter().flatten().collect();
    let target = alter_table_charset_pair(&options, TableCharset::default())?;
    let (to_charset, to_collation) = (target.charset.name(), target.collation.name());
    if let Some(refusal) = modify_charset_and_collation_refusal(
        to_charset,
        to_collation,
        current.charset.name(),
        current.collation.name(),
        false,
    ) {
        return Err(refusal.into_error(|| unreachable!("no rewrite was asked for")));
    }
    // Go `checkIndexLengthWithNewCharset`.
    let converted: Vec<FieldType> = table
        .columns()
        .iter()
        .map(|column| {
            let mut field_type = column.field_type.clone();
            if field_type.has_charset() {
                field_type.set_charset_name(to_charset);
                field_type.set_collation(target.collation);
            }
            field_type
        })
        .collect();
    for index in table.indexes() {
        let parts = index
            .column_offsets
            .iter()
            .enumerate()
            .map(|(position, offset)| (&converted[*offset], index.prefix_length(position)));
        crate::ddl::index_prefix::check_index_key_length_with_max(
            parts,
            index.column_offsets.len(),
            true,
            true,
            max_index_length,
        )
        .map_err(crate::ddl::index_prefix::driver_error)?;
    }
    for column in table.columns() {
        if column.field_type.code() == FieldTypeCode::Varchar {
            let maximum = 65535 / i64::from(target.charset.maxlen());
            if column.field_type.flen() > maximum {
                return Err(DriverError::TooBigFieldLength {
                    column: column.name.clone(),
                    maximum,
                });
            }
        }
        if !column.field_type.has_charset() || column.field_type.charset_name() == "binary" {
            continue;
        }
        let indexed = table.indexes().iter().any(|index| {
            index
                .column_offsets
                .contains(&column_offset(table, &column.name))
        });
        if let Some(refusal) = modify_charset_and_collation_refusal(
            to_charset,
            to_collation,
            column.field_type.charset_name(),
            column.field_type.collation_name(),
            indexed,
        ) {
            return Err(refusal.into_error(|| {
                format!(
                    "Unsupported converting collation of column '{}' from '{}' to '{}' when index is defined on it.",
                    column.name.go_to_lower(),
                    column.field_type.collation_name(),
                    to_collation
                )
            }));
        }
    }
    Ok(target)
}

fn column_offset(table: &crate::KvTable, name: &str) -> usize {
    table
        .columns()
        .iter()
        .position(|column| column.name == name)
        .expect("the column was read from this table")
}

/// Go `checkModifyCharsetAndCollation`'s two refusals.
enum CharsetChangeRefusal {
    /// `ErrUnsupportedModifyCollation`: an indexed column would need its
    /// entries rewritten for an incompatible collation. The caller words it.
    Collation,
    /// `ErrUnsupportedModifyCharset`, with Go's `modify %s` argument.
    Charset(String),
}

impl CharsetChangeRefusal {
    /// The 8200 error, with the indexed-collation case worded by the caller
    /// as Go's callers reword it.
    fn into_error(self, collation_message: impl FnOnce() -> String) -> DriverError {
        let message = match self {
            Self::Collation => collation_message(),
            Self::Charset(reason) => format!("Unsupported modify {reason}"),
        };
        DriverError::DdlCoded {
            errno: tidb_error::tidb::errcode::ErrUnsupportedDDLOperation,
            message,
        }
    }
}

/// Go `checkModifyCharsetAndCollation`, after its validity check (a built
/// type always carries a valid pair): an indexed column may not move to an
/// incompatible collation under the new collation framework, and only
/// utf8/latin1 to utf8mb4, or a collation change within utf8/utf8mb4, is
/// metadata-only.
fn modify_charset_and_collation_refusal(
    to_charset: &str,
    to_collation: &str,
    from_charset: &str,
    from_collation: &str,
    rewrites_index_data: bool,
) -> Option<CharsetChangeRefusal> {
    if rewrites_index_data
        && tidb_datatype::new_collation_enabled()
        && !compatible_collate(from_collation, to_collation)
    {
        return Some(CharsetChangeRefusal::Collation);
    }
    if matches!(
        (from_charset, to_charset),
        ("utf8", "utf8mb4") | ("utf8", "utf8") | ("utf8mb4", "utf8mb4") | ("latin1", "utf8mb4")
    ) {
        return None;
    }
    if to_charset != from_charset {
        return Some(CharsetChangeRefusal::Charset(format!(
            "charset from {from_charset} to {to_charset}"
        )));
    }
    if to_collation != from_collation {
        return Some(CharsetChangeRefusal::Charset(format!(
            "change collate from {from_collation} to {to_collation}"
        )));
    }
    None
}

/// Go `collate.CompatibleCollate`.
fn compatible_collate(left: &str, right: &str) -> bool {
    let general = |name: &str| matches!(name, "utf8mb4_general_ci" | "utf8_general_ci");
    let bin = |name: &str| matches!(name, "utf8mb4_bin" | "utf8_bin" | "latin1_bin");
    let unicode = |name: &str| matches!(name, "utf8mb4_unicode_ci" | "utf8_unicode_ci");
    (general(left) && general(right))
        || (bin(left) && bin(right))
        || (unicode(left) && unicode(right))
        || left == right
}

/// Go `checkColumnDefaultValue` (`pkg/ddl/add_column.go:1212`), the BLOB /
/// TEXT / JSON arm, shared by every entry point that Go's `SetDefaultValue`
/// serves: CREATE TABLE, ADD COLUMN, MODIFY/CHANGE COLUMN and
/// ALTER COLUMN ... SET DEFAULT.
///
/// It answers Go's `(hasDefaultValue, value)` pair, and is a SEPARATE step
/// from [`normalize_column_default`] (Go's `getDefaultValue` +
/// `checkDefaultValue`), which runs after it over the value returned here.
///
/// Go, verbatim in shape:
///
/// - non-strict `sql_mode` AND an EMPTY-STRING default: warn 1101 and accept.
///   `BLOB`/`LONGBLOB` (which is where `TEXT`/`LONGTEXT` land) additionally
///   report `hasDefaultValue = false`; `JSON`'s default is rewritten to the
///   text `null`. `TINYBLOB`/`MEDIUMBLOB` and their TEXT spellings keep both
///   the default AND `hasDefaultValue`, which is not an oversight here --
///   Go's `if col.GetType() == mysql.TypeBlob || col.GetType() ==
///   mysql.TypeLongBlob` names only those two.
/// - anything else non-NULL on those types: 1101, in every mode.
///
/// Measured against TiDB (`sql_mode=''`):
///
/// ```text
/// create table n1 (c1 text not null default '')       -> `c1` text NOT NULL
/// create table n4 (c1 tinyblob not null default '')   -> `c1` tinyblob NOT NULL DEFAULT ''
/// create table n3 (c1 json not null default '')       -> `c1` json NOT NULL DEFAULT 'null'
/// create table e1 (c1 text default 'x')               -> ERROR 1101
/// ```
///
/// `hasDefaultValue = false` is not "drop the default": Go still STORES the
/// value, and only a NOT NULL column then takes `NoDefaultValueFlag`, which
/// is what makes `SHOW CREATE TABLE` print no DEFAULT clause. A NULLABLE
/// `text DEFAULT ''` keeps printing `DEFAULT ''`. This tier models that flag
/// as "no default recorded", so the pair maps onto `None` for a NOT NULL
/// column and onto the stored value otherwise.
pub fn check_column_default_value(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    ctx: &crate::StmtContext,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<(bool, Datum), DriverError> {
    use tidb_datatype::FieldTypeCode;

    if !value.is_null() && field_type.code() == FieldTypeCode::VectorFloat32 {
        return Err(DriverError::unsupported(format!(
            "VECTOR column '{column}' can't have a literal default. Use expression default instead: ((VEC_FROM_TEXT('...')))"
        )));
    }
    if !value.is_null()
        && ctx.strict()
        && ctx.date_modes().no_zero_date
        && matches!(
            field_type.code(),
            FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp
        )
    {
        let converted = value
            .convert_to_in(field_type, ctx.ddl_default_conversion_flags(), zone)
            .map_err(|_| DriverError::InvalidDefault(column.to_owned()))?;
        if matches!(converted.value, Datum::Time(time) if time.is_zero()) {
            return Err(DriverError::InvalidDefault(column.to_owned()));
        }
    }
    if value.is_null()
        || !matches!(
            field_type.code(),
            FieldTypeCode::Json
                | FieldTypeCode::TinyBlob
                | FieldTypeCode::MediumBlob
                | FieldTypeCode::LongBlob
                | FieldTypeCode::Blob
        )
    {
        return Ok((true, value));
    }
    let empty = matches!(value.as_raw_bytes(), Some(bytes) if bytes.is_empty());
    if ctx.strict() || !empty {
        return Err(DriverError::BlobCantHaveDefault(column.to_owned()));
    }
    let reported = DriverError::BlobCantHaveDefault(column.to_owned()).to_mysql_error();
    ctx.append_warning_parts(reported.code, &reported.message);
    Ok(match field_type.code() {
        FieldTypeCode::Blob | FieldTypeCode::LongBlob => (false, value),
        FieldTypeCode::Json => (true, Datum::new_string("null")),
        _ => (true, value),
    })
}

/// One `ADD COLUMN`.
/// The result of Go `getDefaultValue` -> `checkColumnDefaultValue` ->
/// `setDefaultValueWithBinaryPadding`, before final column flags validate it.
#[derive(Clone, Debug)]
pub struct SettledColumnDefault {
    /// Go `hasDefaultValue`; an empty non-strict BLOB/TEXT default may be
    /// stored while this is false.
    pub has_default: bool,
    /// The exact persisted `ColumnInfo.DefaultValue` string, represented as a
    /// byte-capable Datum, or NULL.
    pub stored: Datum,
}

/// A settled default after Go `checkDefaultValue` has proved that the stored
/// spelling can be read through the column's final type.
#[derive(Clone, Debug)]
pub struct PreparedColumnDefault {
    /// The source `hasDefaultValue` disposition.
    pub has_default: bool,
    /// The exact metadata spelling retained by `ColumnInfo`.
    pub stored: Datum,
}

/// Runs the source storage stages without the final-column-flag validation.
///
/// CREATE TABLE needs this split because Go visits DEFAULT in option order,
/// but checks NULL against NOT NULL / PRIMARY KEY only after every option and
/// table-level key has stamped the final FieldType.
pub fn settle_column_default(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    ctx: &crate::StmtContext,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<SettledColumnDefault, DriverError> {
    let flags = ctx.ddl_default_conversion_flags();
    let stored = column_default_storage_value(value, field_type, column, flags, zone)?;
    let (has_default, stored) = check_column_default_value(stored, field_type, column, ctx, zone)?;
    let stored = timestamp_default_to_utc(stored, field_type, column, flags, zone)?;
    Ok(SettledColumnDefault {
        has_default,
        stored: pad_fixed_width_binary_default(stored, field_type),
    })
}

/// Runs every source stage when the caller already owns the final FieldType.
pub fn prepare_column_default(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    column_info_version: u64,
    ctx: &crate::StmtContext,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<PreparedColumnDefault, DriverError> {
    let settled = settle_column_default(value, field_type, column, ctx, zone)?;
    validate_column_default(
        &settled.stored,
        field_type,
        column,
        column_info_version,
        ctx.ddl_default_conversion_flags(),
        zone,
    )?;
    Ok(PreparedColumnDefault {
        has_default: settled.has_default,
        stored: settled.stored,
    })
}

/// The two persisted faces of one ALTER-written DEFAULT.
///
/// `default` remains computed when future INSERTs must evaluate it again;
/// `origin` is settled once for rows that predate an ADD COLUMN. This is the
/// same `DefaultValue`/`OriginDefaultValue` split used by Go's DDL path.
struct PreparedAlterDefault {
    has_default: bool,
    default: Option<crate::column_default::ColumnDefault>,
    origin: Option<Datum>,
}

fn prepare_alter_column_default(
    default: crate::column_default::ColumnDefault,
    field_type: &FieldType,
    column: &str,
    column_info_version: u64,
    ctx: &crate::StmtContext,
) -> Result<PreparedAlterDefault, DriverError> {
    let zone = &ctx.session_zone();
    match default {
        crate::column_default::ColumnDefault::Value(value) => {
            let prepared =
                prepare_column_default(value, field_type, column, column_info_version, ctx, zone)?;
            if !prepared.has_default && field_type.has_flag(NOT_NULL_FLAG) {
                return Ok(PreparedAlterDefault {
                    has_default: false,
                    default: None,
                    origin: None,
                });
            }
            Ok(PreparedAlterDefault {
                has_default: prepared.has_default,
                default: Some(crate::column_default::ColumnDefault::Value(
                    prepared.stored.clone(),
                )),
                origin: Some(prepared.stored),
            })
        }
        computed @ crate::column_default::ColumnDefault::Computed(_) => {
            let crate::column_default::ColumnDefault::Computed(body) = &computed else {
                unreachable!("the matched default is computed")
            };
            if body.added_origin_safety == crate::column_default::AddedOriginSafety::SequenceDefault
            {
                return Ok(PreparedAlterDefault {
                    has_default: true,
                    default: Some(computed),
                    origin: None,
                });
            }
            let value = tidb_expr::eval_expression_once(&body.expr, ctx)
                .map_err(|error| DriverError::Exec(crate::ExecError::Eval(error)))?;
            let origin =
                prepare_computed_origin(value, field_type, column, column_info_version, ctx, zone)?;
            Ok(PreparedAlterDefault {
                has_default: true,
                default: Some(computed),
                origin: Some(origin),
            })
        }
    }
}

fn prepare_computed_origin(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    column_info_version: u64,
    ctx: &crate::StmtContext,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Datum, DriverError> {
    let flags = ctx.ddl_default_conversion_flags();
    let stored = column_default_storage_value(value, field_type, column, flags, zone)?;
    let stored = timestamp_default_to_utc(stored, field_type, column, flags, zone)?;
    let stored = pad_fixed_width_binary_default(stored, field_type);
    validate_column_default(
        &stored,
        field_type,
        column,
        column_info_version,
        flags,
        zone,
    )?;
    Ok(stored)
}

/// Go `checkDefaultValue`: validate the persisted spelling against the
/// column's final flags and return the typed value an omitted row receives.
pub fn validate_column_default(
    stored: &Datum,
    field_type: &FieldType,
    column: &str,
    column_info_version: u64,
    flags: tidb_datatype::ConversionFlags,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Datum, DriverError> {
    let invalid = || DriverError::InvalidDefault(column.to_owned());
    if stored.is_null() {
        // Inline PRIMARY KEY + DEFAULT NULL was already intercepted by
        // `checkPriKeyConstraint`. At this later `checkDefaultValue` boundary
        // Go checks PRI before NOT NULL, so a table-level key is 1171 even
        // when the column also spelled NOT NULL.
        if field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY) {
            return Err(DriverError::PrimaryCantHaveNull);
        }
        if field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL) {
            return Err(invalid());
        }
        return Ok(Datum::Null);
    }

    let checked = crate::column_default::materialize_stored_literal(
        stored,
        field_type,
        column_info_version,
        flags,
        zone,
    )
    .map_err(|_| invalid())?;
    if checked
        .event
        .as_ref()
        .is_some_and(|event| !crate::driver::conversion_event_is_silent(event))
    {
        return Err(invalid());
    }
    Ok(checked.value)
}

/// Compatibility entrypoint for callers that need only the typed value and
/// have already applied `checkColumnDefaultValue` themselves.
pub fn normalize_column_default(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Datum, DriverError> {
    let stored =
        column_default_storage_value(value, field_type, column, tidb_datatype::STRICT_FLAGS, zone)?;
    let stored = pad_fixed_width_binary_default(stored, field_type);
    validate_column_default(
        &stored,
        field_type,
        column,
        tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
        tidb_datatype::STRICT_FLAGS,
        zone,
    )
}

/// Go `getDefaultValue`: settle one evaluated expression to the exact byte
/// string `ColumnInfo.DefaultValue` stores. This is deliberately distinct
/// from [`validate_column_default`], which returns the typed runtime value.
fn column_default_storage_value(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    flags: tidb_datatype::ConversionFlags,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Datum, DriverError> {
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let invalid = || DriverError::InvalidDefault(column.to_owned());

    // Go handles binary literals before its target-type switch. The three
    // branches are exhaustive and return immediately, so FLOAT/DOUBLE never
    // round their persisted spelling and DECIMAL/TIME/YEAR never retain the
    // literal's raw control bytes.
    if let Datum::BinaryLiteral(literal) | Datum::Bit(literal) = &value {
        if matches!(
            field_type.code(),
            tidb_datatype::FieldTypeCode::Date
                | tidb_datatype::FieldTypeCode::Datetime
                | tidb_datatype::FieldTypeCode::Timestamp
        ) {
            return Err(invalid());
        }
        let bytes = if matches!(
            field_type.code(),
            tidb_datatype::FieldTypeCode::Blob
                | tidb_datatype::FieldTypeCode::TinyBlob
                | tidb_datatype::FieldTypeCode::MediumBlob
                | tidb_datatype::FieldTypeCode::LongBlob
                | tidb_datatype::FieldTypeCode::Json
                | tidb_datatype::FieldTypeCode::VectorFloat32
        ) {
            literal.as_bytes().to_vec()
        } else if matches!(
            field_type.code(),
            tidb_datatype::FieldTypeCode::Bit
                | tidb_datatype::FieldTypeCode::String
                | tidb_datatype::FieldTypeCode::Varchar
                | tidb_datatype::FieldTypeCode::VarString
                | tidb_datatype::FieldTypeCode::Enum
                | tidb_datatype::FieldTypeCode::Set
        ) {
            let (bytes, error) = value
                .binary_string_decoded(flags, field_type.charset_name())
                .into_parts();
            if error.is_some() {
                return Err(invalid());
            }
            bytes
        } else {
            let outcome = literal.to_int();
            if outcome.is_truncated() {
                return Err(invalid());
            }
            outcome.value().to_string().into_bytes()
        };
        return Ok(Datum::new_collation_string(bytes, field_type.collation()));
    }

    let normalized = match field_type.code() {
        tidb_datatype::FieldTypeCode::Tiny
        | tidb_datatype::FieldTypeCode::Short
        | tidb_datatype::FieldTypeCode::Int24
        | tidb_datatype::FieldTypeCode::Long
        | tidb_datatype::FieldTypeCode::LongLong
        | tidb_datatype::FieldTypeCode::Float
        | tidb_datatype::FieldTypeCode::Double => {
            // Go adopts the converted value only when the conversion itself
            // succeeded (`if temp, err := v.ConvertTo(...); err == nil`), and
            // otherwise keeps the original for the check below to report.
            match value.convert_to_in(field_type, flags, zone) {
                Ok(converted) if converted.event.is_none() => converted.value,
                _ => value.clone(),
            }
        }
        tidb_datatype::FieldTypeCode::Enum | tidb_datatype::FieldTypeCode::Set => {
            enum_set_column_default(&value, field_type).ok_or_else(invalid)?
        }
        tidb_datatype::FieldTypeCode::Date
        | tidb_datatype::FieldTypeCode::Datetime
        | tidb_datatype::FieldTypeCode::Timestamp
        | tidb_datatype::FieldTypeCode::Duration => {
            let converted = value
                .convert_to_in(field_type, flags, zone)
                .map_err(|_| invalid())?;
            if converted
                .event
                .as_ref()
                .is_some_and(|event| !crate::driver::conversion_event_is_silent(event))
            {
                return Err(invalid());
            }
            converted.value
        }
        tidb_datatype::FieldTypeCode::Bit => bit_column_default(&value, field_type, column)?,
        _ => value.clone(),
    };
    let bytes = normalized.sql_bytes().map_err(|_| invalid())?;
    Ok(Datum::new_collation_string(bytes, field_type.collation()))
}

/// Go `convertTimestampDefaultValToUTC`: a literal TIMESTAMP is persisted as
/// a UTC wall clock after it has passed the session-zone admission checks.
/// Zero stays zero, and computed `CURRENT_TIMESTAMP` never reaches this path.
fn timestamp_default_to_utc(
    stored: Datum,
    field_type: &FieldType,
    column: &str,
    flags: tidb_datatype::ConversionFlags,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Datum, DriverError> {
    if stored.is_null() || field_type.code() != tidb_datatype::FieldTypeCode::Timestamp {
        return Ok(stored);
    }
    let invalid = || DriverError::InvalidDefault(column.to_owned());
    let converted = stored
        .convert_to_in(field_type, flags, zone)
        .map_err(|_| invalid())?;
    if converted
        .event
        .as_ref()
        .is_some_and(|event| !crate::driver::conversion_event_is_silent(event))
    {
        return Err(invalid());
    }
    let Datum::Time(mut time) = converted.value else {
        return Err(invalid());
    };
    if time.is_zero() {
        return Ok(stored);
    }
    time.convert_time_zone(zone, &tidb_datatype::SessionTimeZone::utc())
        .map_err(|_| invalid())?;
    let bytes = Datum::new_time(time).sql_bytes().map_err(|_| invalid())?;
    Ok(Datum::new_collation_string(bytes, field_type.collation()))
}

/// An `ENUM`/`SET` column's written `DEFAULT`, resolved to a MEMBER of the
/// column's own element list, which is the only thing the column can hold.
///
/// Go `pkg/ddl/add_column.go` reaches the same value by three different
/// routes, and the route is chosen by what the default was WRITTEN as:
///
///  * a hex/bit literal (`DEFAULT 0x61`) is decoded to text in the column's
///    charset and taken verbatim -- `getDefaultValue`'s `KindBinaryLiteral`
///    branch returns before the type switch, so the bytes name the member
///    directly and are never read as a number;
///  * an integer (`DEFAULT 2`) is an INDEX: one-based into the element list
///    for `ENUM` (`getEnumDefaultValue` -> `ParseEnumValue`), a bit mask for
///    `SET` (`ParseSetValue`);
///  * anything else is text matched against the element list under the
///    column's collation, with trailing spaces stripped first for `ENUM`
///    because "trailing spaces are automatically deleted from ENUM member
///    values" (`getEnumDefaultValue` -> `TrimRight` -> `ParseEnumName`), and
///    with the empty string admitted for `SET` as the no-members-set value.
///
/// `None` means no member matches, which the caller reports as 1067. Storing
/// the written literal instead -- the shape this replaced -- leaves a column
/// whose `DEFAULT` is not a value of its own type, so an omitted column on
/// `INSERT` reads something the element list does not contain.
/// Go `setDefaultValueWithBinaryPadding`: a FIXED-width binary column pads its
/// stored `DEFAULT` with NUL bytes out to the declared width, exactly as a
/// value written into `BINARY(n)` is padded. `VARBINARY` and every non-binary
/// charset are variable width and keep the default as written.
///
/// Without it, `BINARY(4) DEFAULT 0x61` records a one-byte default for a
/// column that can only ever hold four.
fn pad_fixed_width_binary_default(value: Datum, field_type: &FieldType) -> Datum {
    if field_type.code() != tidb_datatype::FieldTypeCode::String || !field_type.is_binary_string() {
        return value;
    }
    let width = field_type.flen();
    let Some(bytes) = value.as_raw_bytes() else {
        return value;
    };
    if width < 0 || bytes.len() >= width as usize {
        return value;
    }
    let mut padded = bytes.to_vec();
    padded.resize(width as usize, 0);
    Datum::new_collation_string(padded, field_type.collation())
}

/// A `BIT(n)` column's written `DEFAULT`, settled to the BITS it names.
///
/// Go `pkg/ddl/add_column.go` `getDefaultValue` reaches the same bits from two
/// spellings, and keeps neither of them verbatim:
///
///  * a bit or hex literal (`DEFAULT b'1100110111001'`, `DEFAULT 0x19b9`) is
///    read with `GetBinaryStringDecoded` in the column's charset, which for a
///    `BIT` column is `binary` and so hands back the literal's own bytes;
///  * an INTEGER (`DEFAULT 250`) becomes `NewBinaryLiteralFromUint(v, -1)`,
///    the number's minimal big-endian bytes.
///
/// Go then stores those bytes as the column's `DefaultValue` string, and every
/// surface that prints a default -- `SHOW CREATE TABLE`, `SHOW COLUMNS`,
/// `information_schema.columns` -- renders them with
/// `BinaryLiteral.ToBitLiteralString(true)`, so both spellings print back as
/// `b'11111010'`. Keeping the WRITTEN datum instead prints `DEFAULT '250'`,
/// which re-reads as the three characters `250` and not as the bits.
fn bit_column_default(
    value: &Datum,
    field_type: &FieldType,
    column: &str,
) -> Result<Datum, DriverError> {
    let invalid = || DriverError::InvalidDefault(column.to_owned());
    let bits = match value {
        Datum::BinaryLiteral(_) | Datum::Bit(_) => {
            let (bytes, error) = value
                .binary_string_decoded(tidb_datatype::STRICT_FLAGS, field_type.charset_name())
                .into_parts();
            if error.is_some() {
                return Err(invalid());
            }
            tidb_datatype::BinaryLiteral::from(bytes)
        }
        Datum::Int(_) | Datum::UInt(_) => {
            let number = value
                .as_uint()
                .or_else(|| value.as_int().map(|value| value as u64))
                .ok_or_else(invalid)?;
            tidb_datatype::BinaryLiteral::from_uint(number, None)
        }
        // Go falls through to `v.ToString()` for every other kind and lets
        // the check phase decide, so the written value is kept here too.
        _ => return Ok(value.clone()),
    };
    Ok(Datum::BinaryLiteral(bits))
}

fn enum_set_column_default(value: &Datum, field_type: &FieldType) -> Option<Datum> {
    let collator = field_type.runtime_collator();
    let datum_collation = field_type.collation();
    let is_set = field_type.code() == tidb_datatype::FieldTypeCode::Set;
    let member = match value {
        Datum::BinaryLiteral(_) | Datum::Bit(_) => {
            let (bytes, error) = value
                .binary_string_decoded(
                    tidb_datatype::ConversionFlags::default(),
                    field_type.charset_name(),
                )
                .into_parts();
            if error.is_some() {
                return None;
            }
            bytes
        }
        Datum::Int(index) => {
            if is_set {
                let element_count = field_type.elems().len();
                let upper = if element_count >= i64::BITS as usize {
                    -1
                } else {
                    (1_i64 << element_count).wrapping_sub(1)
                };
                if *index < 1 || *index > upper {
                    return None;
                }
            }
            let index = u64::try_from(*index).ok()?;
            if is_set {
                field_type.with_elems_visible(|elements| {
                    tidb_datatype::parse_set_value(elements, index)
                        .ok()
                        .map(|members| members.name_bytes().to_vec())
                })?
            } else {
                field_type.with_elems_visible(|elements| {
                    tidb_datatype::parse_enum_value(elements, index)
                        .ok()
                        .map(|member| member.name_bytes().to_vec())
                })?
            }
        }
        _ => {
            let mut text = value.sql_bytes().ok()?;
            if is_set {
                field_type.with_elems_visible(|elements| {
                    tidb_datatype::parse_set_name(elements, text.as_slice(), collator)
                        .ok()
                        .map(|members| members.name_bytes().to_vec())
                })?
            } else {
                while text.last() == Some(&b' ') {
                    text.pop();
                }
                field_type.with_elems_visible(|elements| {
                    tidb_datatype::parse_enum_name(elements, text.as_slice(), collator)
                        .ok()
                        .map(|member| member.name_bytes().to_vec())
                })?
            }
        }
    };
    Some(Datum::new_collation_string(member, datum_collation))
}

/// `ALTER TABLE ... MODIFY COLUMN` and `... CHANGE COLUMN`, which differ only
/// in whether the column is also renamed.
///
/// Go runs these as one `ActionModifyColumn` job: it finds the old column by
/// name, checks the new type against the stored data, then swaps the column
/// definition in place, keeping the column id so indexes and handles survive.
///
/// NOT MODELLED (documented, and rejected rather than ignored): a type change
/// on a clustered handle column that requires reorganization (Go 8200
/// "this column has primary key flag"), a BLOB/TEXT column that an index
/// covers (Go 1170), generated columns, and the column options beyond
/// NULL/NOT NULL/DEFAULT/AUTO_INCREMENT that CREATE TABLE also rejects here.
/// A KEY or UNIQUE option lands in that last group, which is Go's rule too:
/// MODIFY may keep a constraint but never ADD one.
///
/// Go's `ErrTooLongKey` (1071) when the new type widens a column an index
/// covers past the key-length limit is checked below for both each key part
/// and the affected index's running byte sum.
/// The existing table's default charset/collation, which a column added or
/// modified by ALTER TABLE inherits just as a CREATE TABLE column does.
fn existing_table_charset(catalog: &Catalog, database: &str, table_name: &str) -> TableCharset {
    match catalog.table_in(database, table_name) {
        Some(crate::TableEntry::Kv(table)) => table.charset(),
        _ => TableCharset::default(),
    }
}

/// Go `checkTypeChangeSupported` (`pkg/types/field_type.go:1569-1603`):
/// five ORIGIN/TARGET type-pair refusals that are unconditional -- each is a
/// TiDB `// TODO: ... not support yet, should fix here after supported`, not
/// a rule about the DATA in any particular row. Reached only when the two
/// types differ (Go's caller, `CheckModifyTypeCompatible`, only calls this in
/// its "different type" branch; the same-type precision/elems checks live
/// beside the caller, not here).
///
/// The five arms, transcribed in Go's own order:
/// 1. `{date/datetime/timestamp, TIME, YEAR, any string type, JSON} -> BIT`.
/// 2. `{date/datetime/timestamp, TIME, YEAR, DECIMAL, FLOAT, DOUBLE, JSON,
///    BIT} -> {ENUM, SET}`. Note the asymmetry with rule 3: TIME/YEAR are
///    origins here but not there, because rule 3's TIME-as-target case is
///    covered by rule 5 instead for DURATION specifically.
/// 3. `{ENUM, SET, BIT, DECIMAL, FLOAT, DOUBLE} -> date/datetime/timestamp`.
///    DURATION and YEAR are deliberately NOT origins of this rule -- rule 5
///    is what refuses DURATION as a target, and YEAR -> date/datetime/
///    timestamp is accepted by Go.
/// 4. TiDB `VECTOR` (`TypeTiDBVectorFloat32`) as EITHER side.
/// 5. `{ENUM, SET, BIT} -> TIME (DURATION)`.
///
/// Everything else this function is asked about is accepted HERE -- the
/// per-row `convert_to` gate in `KvTable::modify_column_with_context` still gets the last
/// word for any row that will not fit the new type.
fn check_type_change_supported(origin: &FieldType, to: &FieldType) -> Result<(), DriverError> {
    let (from_code, to_code) = (origin.code(), to.code());
    if from_code == to_code {
        // Go only reaches `checkTypeChangeSupported` from the "different
        // type" branch of `CheckModifyTypeCompatible`; a same-type MODIFY
        // (e.g. widening a `decimal`'s precision) is judged by other rules
        // this tier applies elsewhere, not by this table.
        return Ok(());
    }

    let is_time_like_origin = |code: FieldTypeCode| {
        code.is_type_time()
            || code == FieldTypeCode::Duration
            || code == FieldTypeCode::Year
            || code.is_string()
            || code == FieldTypeCode::Json
    };
    let refused =
        // Rule 1.
        (is_time_like_origin(from_code) && to_code == FieldTypeCode::Bit)
        // Rule 2.
        || ((from_code.is_type_time()
            || from_code == FieldTypeCode::Duration
            || from_code == FieldTypeCode::Year
            || matches!(
                from_code,
                FieldTypeCode::NewDecimal
                    | FieldTypeCode::Float
                    | FieldTypeCode::Double
                    | FieldTypeCode::Json
                    | FieldTypeCode::Bit
            ))
            && matches!(to_code, FieldTypeCode::Enum | FieldTypeCode::Set))
        // Rule 3.
        || (matches!(
            from_code,
            FieldTypeCode::Enum
                | FieldTypeCode::Set
                | FieldTypeCode::Bit
                | FieldTypeCode::NewDecimal
                | FieldTypeCode::Float
                | FieldTypeCode::Double
        ) && to_code.is_type_time())
        // Rule 4.
        || from_code == FieldTypeCode::VectorFloat32
        || to_code == FieldTypeCode::VectorFloat32
        // Rule 5.
        || (matches!(
            from_code,
            FieldTypeCode::Enum | FieldTypeCode::Set | FieldTypeCode::Bit
        ) && to_code == FieldTypeCode::Duration);

    if refused {
        return Err(DriverError::UnsupportedModifyColumnType {
            from: origin.compact_str(false),
            to: to.compact_str(false),
        });
    }
    Ok(())
}

/// Go `checkAutoRandom`, after `ProcessModifyColumnOptions` and
/// `checkModifyTypes`: the table's AUTO_RANDOM bits count only for its
/// clustered primary key column; adding them needs an AUTO_INCREMENT
/// handle, and an AUTO_RANDOM column keeps its BIGINT type and takes neither
/// AUTO_INCREMENT nor a default.
fn check_auto_random(
    table: &crate::KvTable,
    offset: usize,
    field_type: &FieldType,
    new: Option<crate::kv_table::AutoRandomSpec>,
    wants_auto_increment: bool,
    has_default: bool,
) -> Result<(), DriverError> {
    let invalid = |message: &str| DriverError::InvalidAutoRandom(message.to_owned());
    // Go `isClusteredPKColumn`: the PK-is-handle column or any column of a
    // clustered composite key; converting needs the PK handle itself.
    let handle = table.pk_handle_offset() == Some(offset);
    let clustered = handle || table.common_handle_offsets().contains(&offset);
    let old = table.auto_random().filter(|_| clustered);
    let (old_bits, new_bits) = (
        old.map_or(0, |spec| spec.shard_bits),
        new.map_or(0, |spec| spec.shard_bits),
    );
    match old_bits.cmp(&new_bits) {
        std::cmp::Ordering::Equal => {}
        std::cmp::Ordering::Less => {
            if old_bits == 0 && !(handle && table.auto_increment_offset() == Some(offset)) {
                return Err(invalid(
                    "auto_random can only be converted from auto_increment clustered primary key",
                ));
            }
        }
        std::cmp::Ordering::Greater => {
            return Err(invalid(if new_bits == 0 {
                "adding/dropping/modifying auto_random is not supported"
            } else {
                "decreasing auto_random shard bits is not supported"
            }));
        }
    }
    if old_bits > 0 || new_bits > 0 {
        let origin = &table.columns()[offset].field_type;
        if origin.code() != field_type.code() {
            return Err(invalid(
                "modifying the auto_random column type is not supported",
            ));
        }
        if origin.code() != FieldTypeCode::LongLong {
            return Err(DriverError::InvalidAutoRandom(format!(
                "auto_random option must be defined on `bigint` column, but not on `{}` column",
                tidb_datatype::type_str(origin.code())
            )));
        }
        if wants_auto_increment {
            return Err(invalid("auto_random is incompatible with auto_increment"));
        }
        if has_default {
            return Err(invalid("auto_random is incompatible with default"));
        }
    }
    let range_bits =
        |spec: Option<crate::kv_table::AutoRandomSpec>| spec.map_or(64, |spec| spec.range_bits);
    if range_bits(old) != range_bits(new) {
        return Err(invalid(
            "alter the range bits of auto_random column is not supported",
        ));
    }
    Ok(())
}

/// Go `types.CheckModifyTypeCompatible`'s `canReorg` result for a supported
/// type pair. `checkModifyTypes` uses this bit to distinguish an unsupported
/// metadata-only charset change from a charset change that the row rewrite
/// can perform while converting the type.
fn modify_type_needs_reorganization(origin: &FieldType, to: &FieldType) -> bool {
    if origin.code() == to.code() {
        if matches!(origin.code(), FieldTypeCode::Enum | FieldTypeCode::Set) {
            let old = origin.elems_snapshot();
            let new = to.elems_snapshot();
            if new.len() < old.len() || !new.starts_with(&old) {
                return true;
            }
        }
        if origin.code() == FieldTypeCode::NewDecimal
            && (origin.flen() != to.flen()
                || origin.decimal() != to.decimal()
                || origin.is_unsigned() != to.is_unsigned())
        {
            return true;
        }
    } else if !(origin.code().is_string() && to.code().is_string()
        || origin.code().is_integer_type() && to.code().is_integer_type())
    {
        return true;
    }

    if origin.code().converts_between_char_and_varchar(to.code()) {
        return true;
    }

    let (mut old_flen, mut new_flen) = (origin.flen(), to.flen());
    if origin.code().is_integer_type() && to.code().is_integer_type() {
        old_flen = i64::from(origin.code().default_field_length_and_decimal().0);
        new_flen = i64::from(to.code().default_field_length_and_decimal().0);
    }
    if new_flen > 0 && new_flen != old_flen {
        if new_flen < old_flen {
            return true;
        }
        if origin.code() == FieldTypeCode::String
            && to.code() == FieldTypeCode::String
            && origin.is_binary_string()
            && to.is_binary_string()
        {
            return true;
        }
    }
    (to.decimal() > 0 && to.decimal() < origin.decimal())
        || origin.is_unsigned() != to.is_unsigned()
}

/// Go `checkModifyTypes`' charset/collation step: `checkModifyCharsetAndCollation`
/// with the column's index membership, and the indexed-collation refusal
/// reworded for the column. A type change that reorganizes anyway absorbs
/// either refusal (Go's `ErrUnsupportedModifyCharset.Equal(err)` compares
/// the shared 8200 code, so it matches both) -- except to or from GBK.
fn check_modify_charset_and_collation(
    origin: &FieldType,
    to: &FieldType,
    can_reorganize: bool,
    column_name: &str,
    indexed: bool,
) -> Result<(), DriverError> {
    let Some(refusal) = modify_charset_and_collation_refusal(
        to.charset_name(),
        to.collation_name(),
        origin.charset_name(),
        origin.collation_name(),
        indexed,
    ) else {
        return Ok(());
    };
    let gbk = origin.charset_name() == "gbk" || to.charset_name() == "gbk";
    if !gbk && can_reorganize {
        return Ok(());
    }
    Err(refusal.into_error(|| {
        if gbk {
            format!(
                "Unsupported modifying collation from {} to {}",
                origin.collation_name(),
                to.collation_name()
            )
        } else {
            format!(
                "Unsupported modifying collation of column '{}' from '{}' to '{}' when index is defined on it.",
                column_name.go_to_lower(),
                origin.collation_name(),
                to.collation_name()
            )
        }
    }))
}

fn integer_type_widens(origin: &FieldType, to: &FieldType) -> bool {
    origin.code().is_type_integer()
        && to.code().is_type_integer()
        && to.code().default_length_and_decimal().0 > origin.code().default_length_and_decimal().0
}

fn string_type_extends(origin: &FieldType, to: &FieldType) -> bool {
    let matching_family = match (origin.code(), to.code()) {
        (FieldTypeCode::String, FieldTypeCode::String) => {
            !origin.has_flag(FieldTypeFlags::BINARY)
                && !to.has_flag(FieldTypeFlags::BINARY)
                && origin.charset_name() != "binary"
                && to.charset_name() != "binary"
        }
        (FieldTypeCode::Varchar, FieldTypeCode::Varchar)
        | (FieldTypeCode::VarString, FieldTypeCode::VarString) => true,
        _ => false,
    };
    matching_family && to.flen() > origin.flen()
}

fn time_fsp_extends(origin: &FieldType, to: &FieldType) -> bool {
    origin.code() == to.code()
        && matches!(
            origin.code(),
            FieldTypeCode::Duration | FieldTypeCode::Datetime
        )
        && to.decimal() > origin.decimal()
}

fn enum_or_set_appends(origin: &FieldType, to: &FieldType) -> bool {
    if origin.code() != to.code()
        || !matches!(origin.code(), FieldTypeCode::Enum | FieldTypeCode::Set)
    {
        return false;
    }
    let old = origin.elems_snapshot();
    let new = to.elems_snapshot();
    new.len() >= old.len() && new.starts_with(&old)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PartitionColumnUsage {
    NoFunction,
    ToDays,
    Extract,
    Unsupported,
}

fn collect_partition_column_usage(
    expression: &tidb_ast::Expr,
    target: &str,
    current: PartitionColumnUsage,
    usage: &mut Vec<PartitionColumnUsage>,
) {
    match expression {
        tidb_ast::Expr::Column(path) => {
            if path
                .last()
                .is_some_and(|name| name.eq_ignore_ascii_case(target))
                && !usage.contains(&current)
            {
                usage.push(current);
            }
        }
        tidb_ast::Expr::Func { name, args, .. } => {
            let name = name.to_ascii_lowercase();
            let next = if current == PartitionColumnUsage::Unsupported {
                current
            } else {
                match name.as_str() {
                    "to_days" => PartitionColumnUsage::ToDays,
                    _ => PartitionColumnUsage::Unsupported,
                }
            };
            for argument in args {
                collect_partition_column_usage(argument, target, next, usage);
            }
        }
        tidb_ast::Expr::Extract { value, .. } => {
            let next = if current == PartitionColumnUsage::Unsupported {
                current
            } else {
                PartitionColumnUsage::Extract
            };
            collect_partition_column_usage(value, target, next, usage);
        }
        tidb_ast::Expr::Paren(inner) | tidb_ast::Expr::Unary(_, inner) => {
            collect_partition_column_usage(inner, target, current, usage);
        }
        tidb_ast::Expr::Binary(_, left, right) => {
            collect_partition_column_usage(left, target, current, usage);
            collect_partition_column_usage(right, target, current, usage);
        }
        tidb_ast::Expr::Int(_)
        | tidb_ast::Expr::Decimal(_)
        | tidb_ast::Expr::Float(_)
        | tidb_ast::Expr::Hex(_)
        | tidb_ast::Expr::Bit(_)
        | tidb_ast::Expr::String(_)
        | tidb_ast::Expr::Bool(_)
        | tidb_ast::Expr::Null
        | tidb_ast::Expr::Default(_) => {}
        _ => {
            if !usage.contains(&PartitionColumnUsage::Unsupported) {
                usage.push(PartitionColumnUsage::Unsupported);
            }
        }
    }
}

fn partition_column_change_allowed(
    partition: &crate::partition_routing::PartitionSpec,
    name: &str,
    origin: &FieldType,
    to: &FieldType,
) -> bool {
    let mut allowed_changed_flags = FieldTypeFlags::NO_DEFAULT_VALUE;
    if origin.has_flag(FieldTypeFlags::NOT_NULL) && !to.has_flag(FieldTypeFlags::NOT_NULL) {
        allowed_changed_flags |= FieldTypeFlags::NOT_NULL;
    }
    if origin.eval_type() != to.eval_type()
        || origin.is_unsigned() != to.is_unsigned()
        || origin.charset_name() != to.charset_name()
        || origin.collation_name() != to.collation_name()
        || origin.flags() & !allowed_changed_flags != to.flags() & !allowed_changed_flags
    {
        return false;
    }
    let unchanged = origin.code() == to.code()
        && (origin.code().is_type_integer() || origin.flen() == to.flen())
        && origin.decimal() == to.decimal()
        && origin.elems_snapshot() == to.elems_snapshot();
    if unchanged {
        return true;
    }

    let key_change = || {
        integer_type_widens(origin, to)
            || string_type_extends(origin, to)
            || enum_or_set_appends(origin, to)
    };
    match &partition.kind {
        // NONE routes every row to partition 0 regardless of any column, so
        // no column change can break its routing.
        PartitionKind::None => true,
        PartitionKind::Key => key_change(),
        PartitionKind::RangeColumns { .. } | PartitionKind::ListColumns { .. } => {
            key_change() || time_fsp_extends(origin, to)
        }
        PartitionKind::Hash | PartitionKind::Range { .. } | PartitionKind::List { .. } => {
            let Ok(expression) = tidb_model::generated_expr::parse_expression(&partition.expr_text)
            else {
                return false;
            };
            let mut usage = Vec::new();
            collect_partition_column_usage(
                &expression,
                name,
                PartitionColumnUsage::NoFunction,
                &mut usage,
            );
            !usage.is_empty()
                && usage.into_iter().all(|usage| match usage {
                    PartitionColumnUsage::NoFunction => integer_type_widens(origin, to),
                    PartitionColumnUsage::ToDays => {
                        time_fsp_extends(origin, to) && origin.code() == FieldTypeCode::Datetime
                    }
                    PartitionColumnUsage::Extract => time_fsp_extends(origin, to),
                    PartitionColumnUsage::Unsupported => false,
                })
        }
    }
}

/// What one `MODIFY COLUMN` / `CHANGE COLUMN` action states, plus the session
/// facts it is decided against. Grouped because the old column's own
/// definition is only half the input: the rest is what the STATEMENT says and
/// what the SESSION allows.
pub(super) struct ModifyColumnRequest<'a> {
    pub(super) database: &'a str,
    pub(super) table_name: &'a str,
    /// The column being modified, which `CHANGE COLUMN` may rename.
    pub(super) old_name: &'a str,
    pub(super) def: &'a ColumnDef,
    pub(super) position: &'a tidb_ast::ColumnPosition,
    pub(super) if_exists: bool,
    /// `@@tidb_allow_remove_auto_inc`.
    pub(super) allow_remove_auto_inc: bool,
}

pub(super) fn prepare_modify_column(
    catalog: &mut Catalog,
    request: &ModifyColumnRequest<'_>,
    ctx: &crate::StmtContext,
) -> Result<Option<PreparedColumnChange>, DriverError> {
    let &ModifyColumnRequest {
        database,
        table_name,
        old_name,
        def,
        position,
        if_exists,
        allow_remove_auto_inc,
    } = request;
    let max_index_length = catalog.max_index_length();
    let enum_length_limit = catalog.enable_enum_length_limit();
    let mut field_type = field_type_of(
        def,
        existing_table_charset(catalog, database, table_name),
        enum_length_limit,
        ctx,
    )?;
    let mut default_value = None;
    let mut nullability = None;
    let mut has_null_flag = false;
    for option in &def.options {
        match option {
            tidb_ast::ColumnOption::Default(expr) => {
                default_value = Some(crate::column_default::build_in_context(
                    expr,
                    &field_type,
                    &def.name,
                    ctx,
                )?);
            }
            tidb_ast::ColumnOption::NotNull => nullability = Some(true),
            tidb_ast::ColumnOption::Null => {
                nullability = Some(false);
                // Go retains this independently of the final flag: any
                // explicit NULL on an existing primary-key column is 1171,
                // even when a later NOT NULL adds the flag back.
                has_null_flag = true;
            }
            // AUTO_INCREMENT is legal here as long as the column already has
            // it; the set/remove rules are checked below, once the old column
            // is in hand.
            tidb_ast::ColumnOption::AutoIncrement
            // Whether the generated-ness is allowed to change is a question
            // about the OLD column, so it is asked below once that column is
            // in hand.
            | tidb_ast::ColumnOption::Generated { .. }
            // The old table definition decides whether this is an allowed bit
            // increase or AUTO_INCREMENT conversion, so it is checked below.
            | tidb_ast::ColumnOption::AutoRandom(_)
            // Go `ProcessModifyColumnOptions` handles COMMENT here; the new
            // value is read from the option list below, where an ABSENT one
            // keeps the old column's.
            | tidb_ast::ColumnOption::Comment(_)
            // Go's ProcessModifyColumnOptions lets MODIFY restamp the
            // declared CHARACTER SET/COLLATE through the rebuilt FieldType.
            | tidb_ast::ColumnOption::Collate(_) => {}
            // Go parses REFERENCES on MODIFY but refuses it with the
            // dedicated 8200 reason rather than a generic unsupported option.
            tidb_ast::ColumnOption::Reference(_) => {
                return Err(DriverError::UnsupportedModifyColumn(
                    "can't modify with references",
                ))
            }
            tidb_ast::ColumnOption::OnUpdate(expr) => {
                crate::column_default::validate_on_update_current_timestamp(expr, &field_type)
                    .map_err(|_| DriverError::InvalidOnUpdate(def.name.clone()))?;
                field_type.add_flags(tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW);
            }
            _ => {
                return Err(DriverError::unsupported(
                    "this column option is not supported in ALTER TABLE MODIFY COLUMN",
                ))
            }
        }
    }
    let wants_auto_increment = def
        .options
        .iter()
        .any(|option| matches!(option, tidb_ast::ColumnOption::AutoIncrement));
    let auto_random_option = def.options.iter().find_map(|option| match option {
        tidb_ast::ColumnOption::AutoRandom(option) => Some(option),
        _ => None,
    });

    // Admission reads the whole original catalog: foreign-key counterparts
    // may live in another table or schema. Only the NULL cursor below needs
    // a mutable table borrow; metadata and row rewrites belong to execution.
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        return Err(DriverError::unsupported(
            "ALTER TABLE needs a storage-backed table",
        ));
    };
    let Some(offset) = table
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(old_name))
    else {
        // Go's `IF EXISTS` demotes the missing column rather than silencing
        // it. Captured: `alter table t modify column if exists no_col bigint`
        // leaves `Note | 1054 | Unknown column 'no_col' in 't'`, and the
        // `CHANGE COLUMN` spelling that shares this action leaves the same.
        let missing = DriverError::UnknownColumnInTable {
            column: old_name.to_owned(),
            table: table_name.to_owned(),
        };
        if !if_exists {
            return Err(missing);
        }
        ctx.append_suppressed(&missing);
        return Ok(None);
    };
    // Go `GetModifiableColumnJob`: the new name may not be the reserved
    // handle name.
    if def.name.eq_ignore_ascii_case("_tidb_rowid") {
        return Err(DriverError::WrongColumnName(def.name.to_lowercase()));
    }
    let partition_column = table.partition().is_some_and(|partition| {
        partition
            .dependencies
            .iter()
            .any(|dependency| dependency.eq_ignore_ascii_case(old_name))
    });
    // Go `getModifiableColumnJob` (`pkg/ddl/modify_column.go`) computes
    // `checkModifyColumnWithGeneratedColumnsConstraint` ONCE and then raises
    // it in two different shapes -- the rename arm just below, and the 3106
    // arm further down. The partition question is deliberately NOT asked
    // here: Go's rename-only path asks it, this path does not, and that
    // asymmetry is Go's (`checkPartitionModifiableColumn` polices the type
    // instead).
    let dependent = match table.column_dependent(offset) {
        Some(
            dependent @ (crate::kv_table::ColumnDependent::ExpressionIndex
            | crate::kv_table::ColumnDependent::GeneratedColumn),
        ) => Some(dependent),
        _ => None,
    };
    // A rename onto another column's name is a duplicate, but renaming a
    // column to the name it already has is allowed.
    if !def.name.eq_ignore_ascii_case(old_name) {
        if table
            .columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(&def.name))
        {
            return Err(DriverError::DuplicateColumnName(def.name.clone()));
        }
        // The rename arm raises the dependency error UNWRAPPED: 3108 for a
        // visible generated column, 3837 for the hidden one an expression
        // index was rewritten into.
        if let Some(dependent) = dependent {
            return Err(super::column_dependent_error(dependent, old_name));
        }
        if partition_column {
            return Err(super::column_dependent_error(
                crate::kv_table::ColumnDependent::Partition,
                old_name,
            ));
        }
    }

    // Go `getModifiableColumnJob` asks this HERE: after the rename checks
    // above and BEFORE the index-flag copy and `checkModifyTypes` below.
    // Keeping Go's position is what decides which error a statement that
    // breaks two rules at once reports.
    let original_type = table.columns[offset].field_type.clone();
    crate::foreign_key::check_modify_column(
        catalog,
        database,
        table_name,
        old_name,
        &original_type,
        &field_type,
    )?;
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        unreachable!("the table was found above and nothing here removes it");
    };

    // Go `pkg/ddl/modify_column.go`: the new column is built from the new
    // definition, then the OLD column's index flags are copied onto it and a
    // primary key supplies the NOT NULL state with which option processing
    // starts. NULL/NOT NULL then mutate that state in source order. Without
    // the baseline, `ALTER TABLE mc MODIFY COLUMN a bigint` on a primary key
    // silently makes the key column nullable; without the ordered mutation,
    // `... NOT NULL NULL` incorrectly keeps the first option.
    let old_flags = table.columns[offset].field_type.flags();
    let old_is_primary = old_flags & PRI_KEY_FLAG != 0;
    if old_is_primary {
        field_type.add_flags(PRI_KEY_FLAG | NOT_NULL_FLAG);
    }
    match nullability {
        Some(true) => field_type.add_flags(NOT_NULL_FLAG),
        Some(false) => field_type.del_flags(NOT_NULL_FLAG),
        None => {}
    }
    // Go `checkPriKeyConstraint`, in its exact order. This is deliberately
    // scoped to the copied OLD primary-key flag: MODIFY cannot add a primary
    // key, and ordinary columns do not inherit either error merely because
    // they spell NULL. A NULL default wins with 1067; otherwise any explicit
    // NULL option on the key is 1171, even if NOT NULL followed it.
    if old_is_primary {
        if default_value.as_ref().is_some_and(|default| {
            matches!(
                default,
                crate::column_default::ColumnDefault::Value(value) if value.is_null()
            )
        }) {
            return Err(DriverError::InvalidDefault(def.name.clone()));
        }
        if has_null_flag {
            return Err(DriverError::PrimaryCantHaveNull);
        }
    }
    if partition_column {
        let partition = table
            .partition()
            .expect("partition dependency has an owner");
        let mut partition_candidate = field_type.clone();
        if table.columns[offset]
            .field_type
            .has_flag(FieldTypeFlags::AUTO_INCREMENT)
            && wants_auto_increment
        {
            partition_candidate
                .add_flags(FieldTypeFlags::AUTO_INCREMENT | FieldTypeFlags::NOT_NULL);
        }
        if !partition_column_change_allowed(
            partition,
            old_name,
            &table.columns[offset].field_type,
            &partition_candidate,
        ) {
            return Err(DriverError::UnsupportedModifyColumn(
                "can't change the partitioning column, since it would require reorganize all partitions",
            ));
        }
    }
    // Go `checkModifyTypes` (`pkg/ddl/modify_column.go:2262`), reached right
    // after the index-flag copy above and before the AUTO_INCREMENT checks
    // below -- this is Go's ORDER, not an arbitrary choice: `checkModifyTypes`
    // calls `types.CheckModifyTypeCompatible`, which for a type-changing
    // MODIFY calls `checkTypeChangeSupported` (`pkg/types/field_type.go:1569`)
    // BEFORE any row is read. That location is what makes the refusal fire on
    // an EMPTY table: the per-row `convert_to` gate in `KvTable::modify_column_with_context`
    // below never runs when there are zero rows, so without this table-level
    // check every one of Go's five outright refusals would be silently
    // accepted on an empty table.
    check_type_change_supported(&table.columns[offset].field_type, &field_type)?;
    let can_reorganize =
        modify_type_needs_reorganization(&table.columns[offset].field_type, &field_type);
    // Go `checkModifyTypes`: a change that needs reorganization is refused
    // for any primary key column.
    if can_reorganize
        && table.columns[offset]
            .field_type
            .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)
    {
        return Err(DriverError::UnsupportedModifyColumn(
            "this column has primary key flag",
        ));
    }
    check_modify_charset_and_collation(
        &table.columns[offset].field_type,
        &field_type,
        can_reorganize,
        old_name,
        table
            .indexes()
            .iter()
            .any(|index| index.column_offsets.contains(&offset)),
    )?;
    if let Some(index_name) = table.partial_index_condition_dependency(old_name) {
        return Err(super::indexes::partial_index_column_dependency(
            old_name,
            &index_name,
        ));
    }
    let new_auto_random = auto_random_option
        .map(|option| {
            let shard_bits = option.shard_bits.unwrap_or(5);
            if shard_bits == 0 {
                return Err(DriverError::InvalidAutoRandom(
                    "the value of auto_random should be positive".to_owned(),
                ));
            }
            if shard_bits > 15 {
                return Err(DriverError::InvalidAutoRandom(format!(
                    "max allowed auto_random shard bits is 15, but got {shard_bits} on column `{}`",
                    def.name
                )));
            }
            let range_bits = option.range_bits.unwrap_or(64);
            if !(32..=64).contains(&range_bits) {
                return Err(DriverError::InvalidAutoRandom(format!(
                    "auto_random range bits must be between 32 and 64, but got {range_bits}"
                )));
            }
            let spec = crate::kv_table::AutoRandomSpec {
                offset,
                shard_bits,
                range_bits,
                unsigned: field_type.is_unsigned(),
            };
            Ok(spec)
        })
        .transpose()?;
    // Go, same file: `can't set auto_increment` (8200) for a column that did
    // not have it, and dropping it needs `@@tidb_allow_remove_auto_inc`.
    // Keeping it is the only combination that changes nothing.
    let was_auto_increment = table.auto_increment_offset() == Some(offset);
    let converting_auto_increment = was_auto_increment && new_auto_random.is_some();
    if wants_auto_increment && !was_auto_increment {
        return Err(DriverError::UnsupportedModifyColumn(
            "can't set auto_increment",
        ));
    }
    if wants_auto_increment && default_value.is_some() {
        return Err(DriverError::InvalidDefault(def.name.clone()));
    }
    if was_auto_increment
        && !wants_auto_increment
        && !allow_remove_auto_inc
        && !converting_auto_increment
    {
        return Err(DriverError::UnsupportedModifyColumn(
            "can't remove auto_increment without @@tidb_allow_remove_auto_inc enabled",
        ));
    }
    check_auto_random(
        table,
        offset,
        &field_type,
        new_auto_random,
        wants_auto_increment,
        default_value.is_some(),
    )?;
    if was_auto_increment && wants_auto_increment {
        // Nothing in this tier READS this flag -- the observable
        // AUTO_INCREMENT comes from the table-level offset above, which is
        // why no test can kill this line. It is set so that a column reached
        // through MODIFY carries exactly what the CREATE TABLE path
        // (`ddl.rs`) gives the same column, rather than leaving two spellings
        // of one catalog for the first reader of the flag to trip over.
        field_type.add_flags(AUTO_INCREMENT_FLAG | NOT_NULL_FLAG);
    }
    let drop_auto_increment = was_auto_increment && !wants_auto_increment;
    // Go GetModifiableColumnJob/checkForNullValue: reject existing NULLs
    // before index validation or metadata changes. Only non-TIMESTAMP to
    // TIMESTAMP skips this check (the rewrite substitutes the statement time).
    if old_flags & NOT_NULL_FLAG == 0
        && field_type.flags() & NOT_NULL_FLAG != 0
        && !(original_type.code() != FieldTypeCode::Timestamp
            && field_type.code() == FieldTypeCode::Timestamp)
    {
        let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
            unreachable!("the table was found above");
        };
        let table = std::sync::Arc::make_mut(table);
        let scan_error = |error| DriverError::DdlCoded {
            errno: 1105,
            message: format!("column NULL precheck failed: {error:?}"),
        };
        let mut cursor = table
            .row_cursor_projected_with_context(
                Some(&[offset]),
                None,
                &crate::RowDecodeContext::for_query(ctx),
            )
            .map_err(scan_error)?;
        let memory = ctx.statement_memory();
        while let Some((_, row)) = cursor.next_row().map_err(scan_error)? {
            memory.check()?;
            if row[0].is_null() {
                // Go's SELECT ... LIMIT 1 reports one matching row, not the
                // NULL's physical position in the table.
                return Err(DriverError::DataTruncatedAtRow {
                    column: def.name.go_to_lower(),
                    row: 1,
                });
            }
        }
    }
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        unreachable!("the table was found above");
    };
    // Go `checkIndexInModifiableColumns` (`pkg/ddl/modify_column.go`): every
    // key part over this column is re-validated against the NEW type, under
    // the length that key part will survive with -- which is Go's
    // `UpdateIndexCol` rule, applied by `KvTable::modify_column_with_context` itself.
    //
    // This subsumes the `ErrBlobKeyWithoutLength` refusal it replaces: a key
    // part with no surviving prefix over a new BLOB/TEXT column is exactly
    // Go's 1170. A key part that KEEPS a prefix is legal over the same type,
    // which is why the check has to ask about the length rather than about
    // the type alone.
    for index in table.indexes() {
        for (position, at) in index.column_offsets.iter().enumerate() {
            if *at != offset {
                continue;
            }
            let length = index.prefix_length(position);
            let surviving = (field_type.code().is_type_prefixable() && field_type.flen() > length)
                .then_some(length);
            crate::ddl::index_prefix::key_part_length_with_max(
                &field_type,
                crate::ddl::index_prefix::IndexedColumn::Named(&def.name),
                surviving,
                true,
                max_index_length,
            )?;
        }
    }
    // Go's `checkIndexInModifiableColumns` also re-runs the running sum for
    // each affected index. Rechecking only the changed key part misses a
    // composite index whose parts are individually legal but whose new total
    // exceeds `MAX_INDEX_LENGTH` (1071).
    for index in table.indexes() {
        if !index.column_offsets.contains(&offset) {
            continue;
        }
        let parts = index
            .column_offsets
            .iter()
            .enumerate()
            .map(|(position, column_offset)| {
                let field_type = if *column_offset == offset {
                    &field_type
                } else {
                    &table.columns[*column_offset].field_type
                };
                (field_type, index.prefix_length(position))
            });
        crate::ddl::index_prefix::check_index_key_length_with_max(
            parts,
            index.column_offsets.len(),
            index.unique,
            true,
            max_index_length,
        )
        .map_err(crate::ddl::index_prefix::driver_error)?;
    }

    // The second shape of the dependency error, and the one this tier used to
    // miss entirely. Go raises it here, after the index checks above and
    // regardless of whether the name or even the TYPE changed:
    //
    //     if errG != nil {
    //         // https://github.com/pingcap/tidb/issues/24321
    //         return nil, dbterror.ErrUnsupportedOnGeneratedColumn.
    //             GenWithStackByArgs(errG.Error())
    //     }
    //
    // The argument is the inner error's FULL `Error()` text, class prefix
    // included, so the wire message nests one error inside another. That is
    // not a slip -- it is in the recording verbatim
    // (`tests/integrationtest/r/ddl/column_change.result:12`):
    //
    //     Error 3106 (HY000): '[ddl:3108]Column 'a' has a generated column
    //     dependency.' is not supported for generated columns.
    //
    // Accepting these left the expression index in place over a column whose
    // TYPE had moved out from under it, so a later read used an index whose
    // expression no longer matched the column. Refusing is what Go does; it is
    // not a stand-in for a rewrite Go performs, because Go does not rewrite
    // for MODIFY any more than it does for RENAME.
    let preserve_origin_default = table.columns[offset].field_type == field_type;
    let previous_origin_default = table.columns[offset].origin_default.clone();
    let new_position = column_changes::position(table, position, Some(offset), table_name)?;
    let generated =
        super::generated_modify::build(table, offset, def, &field_type, new_position, ctx)?;
    if let Some(dependent) = dependent {
        return Err(DriverError::UnsupportedOnGeneratedColumn(
            super::column_dependent_error_text(dependent, old_name),
        ));
    }
    // Go `GetModifiableColumnJob`: the column a TTL config reads must stay a
    // time type.
    if table.ttl_info().is_some_and(|info| {
        info.column_name.original().eq_ignore_ascii_case(old_name)
            && !field_type.code().is_type_time()
    }) {
        return Err(DriverError::UnsupportedColumnInTtlConfig(def.name.clone()));
    }
    let prepared_default = match default_value {
        Some(default @ crate::column_default::ColumnDefault::Computed(_))
            if preserve_origin_default =>
        {
            PreparedAlterDefault {
                has_default: true,
                default: Some(default),
                origin: previous_origin_default,
            }
        }
        Some(default) => {
            let mut prepared = prepare_alter_column_default(
                default,
                &field_type,
                &def.name,
                tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                ctx,
            )?;
            if preserve_origin_default {
                prepared.origin = previous_origin_default;
            }
            prepared
        }
        None => PreparedAlterDefault {
            has_default: false,
            default: None,
            origin: previous_origin_default,
        },
    };
    let has_default_value = prepared_default.has_default;
    super::set_no_default_value_flag(&mut field_type, has_default_value);
    table
        .validate_alter_auto_random_spec(new_auto_random, offset)
        .map_err(super::auto_random::rebase_error)?;
    // Go `GetModifiableColumnJob` asks `IsModifyColumnDenied` after
    // `checkAutoRandom`, over the built column's type and the spec's options.
    super::bdr::admit_modify_column(
        catalog,
        database,
        &field_type,
        &table.columns[offset].field_type,
        &def.options,
    )?;
    let column = KvColumn {
        name: def.name.clone(),
        id: table.columns[offset].id,
        field_type,
        column_info_version: table.columns[offset].column_info_version,
        // Go `getModifiableColumnJob` CLONES the old column and then lets
        // `ProcessModifyColumnOptions` overlay only what the spec names, so
        // a MODIFY that does not repeat COMMENT keeps the existing one.
        comment: match super::column_comment_option(&def.options) {
            Some(comment) => super::validate_comment_length(
                &comment,
                &def.name,
                super::CommentOwner::Field,
                ctx,
            )?,
            None => table.columns[offset].comment.clone(),
        },
        generated,
        default_value: prepared_default.default,
        origin_default: prepared_default.origin,
    };
    Ok(Some(PreparedColumnChange::Modify {
        old_name: old_name.to_owned(),
        column,
        position: position.clone(),
        new_auto_random,
        drop_auto_increment,
    }))
}

/// Go `checkUnsupportedColumnConstraint`: the column options ADD COLUMN
/// refuses, checked over every option before anything else is built.
fn check_unsupported_column_constraint(
    def: &ColumnDef,
    database: &str,
    table_name: &str,
) -> Result<(), DriverError> {
    for option in &def.options {
        let constraint = match option {
            tidb_ast::ColumnOption::AutoIncrement => "AUTO_INCREMENT",
            tidb_ast::ColumnOption::InlineKey(key) => match key.kind {
                tidb_ast::InlineKeyKind::Primary { .. } => "PRIMARY KEY",
                tidb_ast::InlineKeyKind::Unique => "UNIQUE KEY",
            },
            tidb_ast::ColumnOption::AutoRandom(_) => {
                return Err(DriverError::InvalidAutoRandom(format!(
                    "unsupported add column '{}' constraint AUTO_RANDOM when altering '{}.{}'",
                    def.name, database, table_name
                )));
            }
            _ => continue,
        };
        // `dbterror.ErrUnsupportedAddColumn.GenWithStack(...)`: 8200 with
        // the formatted text.
        return Err(DriverError::DdlCoded {
            errno: tidb_error::tidb::errcode::ErrUnsupportedDDLOperation,
            message: format!(
                "unsupported add column '{}' constraint {constraint} when altering '{database}.{table_name}'",
                def.name
            ),
        });
    }
    Ok(())
}

pub(super) fn prepare_add_column(
    catalog: &Catalog,
    database: &str,
    table_name: &str,
    def: &ColumnDef,
    position: &tidb_ast::ColumnPosition,
    if_not_exists: bool,
    ctx: &crate::StmtContext,
) -> Result<Option<PreparedColumnChange>, DriverError> {
    let table = column_changes::table_of(catalog, database, table_name)?;
    if table.columns().len() >= catalog.table_column_count_limit() {
        return Err(DriverError::DdlCoded {
            errno: tidb_error::mysql::errcode::ErrTooManyFields,
            message: tidb_error::mysql::errname::ErrTooManyFields.raw.to_owned(),
        });
    }
    check_unsupported_column_constraint(def, database, table_name)?;
    let zone = &ctx.session_zone();
    let mut field_type = field_type_of(
        def,
        existing_table_charset(catalog, database, table_name),
        catalog.enable_enum_length_limit(),
        ctx,
    )?;
    let mut default_value = None;
    let mut not_null = false;
    for option in &def.options {
        match option {
            tidb_ast::ColumnOption::Default(expr) => {
                if crate::column_default::is_sequence_default_expression(expr) {
                    return Err(DriverError::AddColumnSequenceDefault(def.name.clone()));
                }
                default_value = Some(crate::column_default::build_in_context(
                    expr,
                    &field_type,
                    &def.name,
                    ctx,
                )?);
                // Go `removeOnUpdateNowFlag`: only a TIMESTAMP definition's
                // explicit DEFAULT clears a preceding ON UPDATE option.
                if field_type.code() == FieldTypeCode::Timestamp {
                    field_type.del_flags(tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW);
                }
            }
            // Go mutates the flag while visiting options. Replacing this with
            // `any(NotNull)` loses the last-option-wins result of legal forms
            // such as `NOT NULL NULL`.
            tidb_ast::ColumnOption::NotNull => not_null = true,
            tidb_ast::ColumnOption::Null => {
                not_null = false;
                if field_type.code() == FieldTypeCode::Timestamp {
                    field_type.del_flags(tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW);
                }
            }
            tidb_ast::ColumnOption::OnUpdate(expr) => {
                crate::column_default::validate_on_update_current_timestamp(expr, &field_type)
                    .map_err(|_| DriverError::InvalidOnUpdate(def.name.clone()))?;
                field_type.add_flags(tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW);
            }
            // Go `checkAddColumnTooManyColumns`'s neighbour in
            // `pkg/ddl/column.go`: a STORED generated column added by ALTER
            // would have to be backfilled into every existing row, which TiDB
            // refuses outright. Captured: `alter table g add column f int as
            // (a*2) stored` -> `Error|3106|'Adding generated stored column
            // through ALTER TABLE' is not supported for generated columns.`
            // The VIRTUAL form is accepted and computes on read, which is why
            // only this half is refused.
            tidb_ast::ColumnOption::Generated { stored: true, .. } => {
                return Err(DriverError::UnsupportedOnGeneratedColumn(
                    "Adding generated stored column through ALTER TABLE".to_owned(),
                ))
            }
            tidb_ast::ColumnOption::Generated { stored: false, .. } => {}
            tidb_ast::ColumnOption::Check(_) => {
                // Pinned Go `buildColumnAndConstraint` emits this warning
                // while disabled, then `CreateNewColumn` discards the
                // returned inline constraint in both modes.
                if !ctx.enable_check_constraint() {
                    ctx.append_warning_parts(1105, "tidb_enable_check_constraint is off");
                }
            }
            // Go `columnDefToCol`: COMMENT is read below, COLLATE was applied
            // by the type build, and the storage-layout options are accepted
            // and dropped. AUTO_INCREMENT, PRIMARY/UNIQUE and AUTO_RANDOM were
            // refused before this loop.
            tidb_ast::ColumnOption::Comment(_)
            | tidb_ast::ColumnOption::Collate(_)
            | tidb_ast::ColumnOption::ColumnFormat(_)
            | tidb_ast::ColumnOption::Storage(_)
            | tidb_ast::ColumnOption::SecondaryEngineAttribute(_)
            | tidb_ast::ColumnOption::Reference(_)
            | tidb_ast::ColumnOption::MariaDbRowStart
            | tidb_ast::ColumnOption::MariaDbRowEnd
            | tidb_ast::ColumnOption::AutoIncrement
            | tidb_ast::ColumnOption::InlineKey(_)
            | tidb_ast::ColumnOption::AutoRandom(_) => {}
        }
    }
    let generated_expression = def.options.iter().find_map(|option| match option {
        tidb_ast::ColumnOption::Generated { expression, .. } => Some(expression),
        _ => None,
    });
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        return Err(DriverError::unsupported(
            "ALTER TABLE needs a storage-backed table",
        ));
    };
    if table
        .columns
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(&def.name))
    {
        // Go's checkAndCreateNewColumn reports ErrColumnExists after the
        // column definition has passed its own option checks, then lets an
        // individual IF NOT EXISTS guard demote that 1060 to a Note and
        // continue the ALTER (including the grouped ADD COLUMNS form).
        let duplicate = DriverError::DuplicateColumnName(def.name.clone());
        if if_not_exists {
            ctx.append_suppressed(&duplicate);
            return Ok(None);
        }
        return Err(duplicate);
    }
    // Go `checkAndCreateNewColumn`: the name's length, once it is new; then
    // `buildColumnAndConstraint` refuses the reserved handle name.
    super::check_too_long_identifier(&def.name)?;
    if def.name.eq_ignore_ascii_case("_tidb_rowid") {
        return Err(DriverError::WrongColumnName(def.name.to_lowercase()));
    }
    let index = column_changes::position(table, position, None, table_name)?
        .unwrap_or(table.visible_column_count());
    if not_null {
        field_type.add_flags(NOT_NULL_FLAG);
    }
    if let Some(default) = default_value.as_ref() {
        match default.added_origin_safety() {
            crate::column_default::AddedOriginSafety::Safe => {}
            crate::column_default::AddedOriginSafety::UnsafeSystemFunction => {
                return Err(DriverError::BinlogUnsafeSystemFunction);
            }
            crate::column_default::AddedOriginSafety::SequenceDefault => {
                return Err(DriverError::AddColumnSequenceDefault(def.name.clone()));
            }
        }
    }
    let prepared_default = default_value
        .map(|default| {
            prepare_alter_column_default(
                default,
                &field_type,
                &def.name,
                tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                ctx,
            )
        })
        .transpose()?
        .unwrap_or(PreparedAlterDefault {
            has_default: false,
            default: None,
            origin: None,
        });
    let has_default_value = prepared_default.has_default;
    super::set_no_default_value_flag(&mut field_type, has_default_value);
    // Go's `CreateNewColumn` validates generated expressions against the
    // WHOLE current table first (`checkDependedColExist`), then applies the
    // position-sensitive `verifyColumnGenerationSingle` check. Resolving
    // against only `table.columns[..index]` would turn a later generated
    // dependency into 1054 instead of Go's 3107.
    let generated = match generated_expression {
        Some(expression) => {
            let names: Vec<String> = table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect();
            let types: Vec<tidb_datatype::FieldType> = table
                .columns
                .iter()
                .map(|column| column.field_type.clone())
                .collect();
            let generated =
                crate::generated_column::build_added_generated_column_with_like_default_escape(
                    &def.name,
                    expression,
                    false,
                    &names,
                    &types,
                    zone,
                    ctx.like_default_escape(),
                )
                .map_err(crate::ddl::generated_column_error)?;
            // Go `checkAutoIncrementRef` (`add_column.go:233`), named by the
            // column's lower-cased name.
            if !ctx.auto_increment_in_generated() {
                let auto_increment = table.columns.iter().find(|column| {
                    column
                        .field_type
                        .has_flag(tidb_datatype::FieldTypeFlags::AUTO_INCREMENT)
                });
                if auto_increment.is_some_and(|auto_increment| {
                    generated
                        .dependencies
                        .iter()
                        .any(|dependency| dependency.eq_ignore_ascii_case(&auto_increment.name))
                }) {
                    return Err(DriverError::DdlCoded {
                        errno: 3109,
                        message: format!(
                            "Generated column '{}' cannot refer to auto-increment column.",
                            def.name.go_to_lower()
                        ),
                    });
                }
            }
            for dependency in &generated.dependencies {
                let Some(dependency_offset) = table
                    .columns
                    .iter()
                    .position(|column| column.name.eq_ignore_ascii_case(dependency))
                else {
                    // The full-table resolver above already turns this into
                    // Go's 1054, so this is defensive if the resolver ever
                    // gains a non-column dependency form.
                    continue;
                };
                if table.columns[dependency_offset].generated.is_some()
                    && dependency_offset >= index
                {
                    return Err(DriverError::GeneratedColumnNonPrior);
                }
            }
            Some(generated)
        }
        None => None,
    };
    let field_type_for_origin = field_type.clone();
    let column = KvColumn {
        name: def.name.clone(),
        id: 0,
        field_type,
        column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
        // A column being ADDED has no prior comment to keep.
        comment: super::validate_comment_length(
            &super::column_comment_option(&def.options).unwrap_or_default(),
            &def.name,
            super::CommentOwner::Field,
            ctx,
        )?,
        generated,
        default_value: prepared_default.default,
        // Rows written before this column existed read back the default.
        // A NOT NULL column with NO default reads back the TYPE's zero
        // instead of NULL: Go fills the backfill value through
        // `GetColOriginDefaultValueWithoutStrictSQLMode`, whose
        // `getColDefaultValueFromNil` takes the non-strict arm by
        // construction and returns `GetZeroValue`. Captured: after
        // `ALTER TABLE q1 ADD COLUMN cc SET('a','b','c','d') NOT NULL`
        // the pre-existing rows read `''`, and `dd INT NOT NULL` reads
        // `0` -- not NULL, which is why an ordinary UPDATE of such a row
        // does not trip the NOT NULL check.
        origin_default: prepared_default
            .origin
            .or_else(|| not_null.then(|| crate::bad_null::zero_value(&field_type_for_origin))),
    };
    // Go `AddColumn` asks `IsAddColumnDenied` once the column is built and
    // its position checked.
    super::bdr::admit_add_column(catalog, database, &def.options)?;
    Ok(Some(PreparedColumnChange::Add {
        column,
        position: position.clone(),
    }))
}

/// One `DROP COLUMN`.
pub(super) fn prepare_drop_column(
    catalog: &Catalog,
    database: &str,
    table_name: &str,
    column_name: &str,
    if_exists: bool,
    ctx: &crate::StmtContext,
) -> Result<Option<PreparedColumnChange>, DriverError> {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, table_name) else {
        return Err(DriverError::unsupported(
            "ALTER TABLE needs a storage-backed table",
        ));
    };
    // Go `checkIsDroppableColumn` looks in `VisibleCols()`: a hidden
    // expression-index column cannot be named.
    let Some(offset) = table
        .visible_columns()
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(column_name))
    else {
        // Captured: `alter table t drop column if exists no_col` leaves
        // `Note | 1091 | Can't DROP 'no_col'; check that column/key exists` --
        // a DIFFERENT code and text from the MODIFY spelling above, which is
        // why the note carries the suppressed error rather than a shared one.
        let missing = DriverError::UnknownColumnInAlter(column_name.to_owned());
        if !if_exists {
            return Err(missing);
        }
        ctx.append_suppressed(&missing);
        return Ok(None);
    };
    // Go isDroppableColumn retains this action-specific diagnostic before
    // the later combined visible-column count check.
    if table.columns().len() == 1 {
        return Err(DriverError::CannotDropOnlyColumn {
            column: column_name.to_owned(),
            table: table_name.to_owned(),
        });
    }
    // Go `checkDropColumnWithTTLConfig` (`pkg/ddl/ttl.go:152-159`): the column
    // a TTL config names cannot be dropped while the config stands
    // (`ErrTTLColumnCannotDrop`, 8149); the TTL_ENABLE clause must go first.
    if let Some(info) = table.ttl_info() {
        if info.column_name.lowercase() == column_name.to_ascii_lowercase() {
            return Err(DriverError::TtlColumnCannotDrop(column_name.to_owned()));
        }
    }
    // Go `checkIsDroppableColumn` (`pkg/ddl/executor.go`) runs `isDroppableColumn`
    // and then `checkDropColumnWithPartitionConstraint`, which is the pair
    // `column_dependent` answers: with `index idx((a+b))`, `drop column a` is
    // 3837 `Column 'a' has an expression index dependency and cannot be
    // dropped or renamed` (CAPTURED from TiDB); with `c AS (a+1)` it is 3108;
    // with `partition by hash(a)` it is 3855. Without this the drop succeeded
    // and left the expression naming a column that no longer exists.
    if let Some(dependent) = table.column_dependent(offset) {
        return Err(super::column_dependent_error(dependent, column_name));
    }
    // Captured: dropping an integer primary key is TiDB's 8200.
    if table.pk_handle_offset() == Some(offset) {
        return Err(DriverError::UnsupportedDropIntegerPrimaryKey);
    }
    if table.common_handle_offsets().contains(&offset) {
        return Err(DriverError::unsupported(
            "dropping a clustered primary key column is not supported yet",
        ));
    }
    // Captured from TiDB: a COMPOSITE index over the column refuses the drop
    // with 8200, while a single-column index is dropped along with it.
    if table
        .indexes()
        .iter()
        .any(|index| index.column_offsets.len() > 1 && index.column_offsets.contains(&offset))
    {
        return Err(DriverError::CannotDropColumnWithCompositeIndex(
            column_name.to_owned(),
        ));
    }
    // Go `IsColumnDroppableWithCheckConstraint`: a CHECK that also
    // references another column blocks the drop. A CHECK whose sole
    // dependency is this column is allowed and becomes invalid; Go removes
    // it lazily in `table.LoadCheckConstraint` when the new schema is loaded.
    let mut invalid_constraint_ids = Vec::new();
    for info in table.check_constraint_infos() {
        if !super::check_constraint::uses_column(info, column_name) {
            continue;
        }
        if info.constraint_cols.len() > 1 {
            let error =
                super::check_constraint::column_dependency_error(info.name.original(), column_name);
            return Err(DriverError::DdlCoded {
                errno: error.code,
                message: error.message,
            });
        }
        invalid_constraint_ids.push(info.id);
    }
    if let Some(index_name) = table.partial_index_condition_dependency(column_name) {
        return Err(super::indexes::partial_index_column_dependency(
            column_name,
            &index_name,
        ));
    }
    check_visible_column_count(table, 0, 1)?;
    Ok(Some(PreparedColumnChange::Drop {
        id: table.columns[offset].id,
        name: column_name.to_owned(),
        invalid_constraint_ids,
    }))
}

/// Go `AlterTableTTLInfoOrEnable` (`pkg/ddl/executor.go:3851-3903`) plus the
/// `onAlterTTLInfo` merge rules (`pkg/ddl/ttl.go:54-90`): the TTL options of
/// one ALTER form ONE group. A full `TTL=` re-definition is validated like
/// CREATE and inherits the existing `TTL_ENABLE`/`TTL_JOB_INTERVAL` unless
/// this ALTER also carries them; the enable/interval-only forms need an
/// existing config (`ErrSetTTLOptionForNonTTLTable`, 8150).
fn prepare_ttl_info_or_enable(
    catalog: &Catalog,
    database: &str,
    name: &str,
    options: &[tidb_ast::TableOption],
) -> Result<Option<tidb_model::TTLInfo>, DriverError> {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in(database, name) else {
        return Err(DriverError::unsupported(
            "ALTER TABLE needs a storage-backed table",
        ));
    };
    let info = super::ttl_info_from_options(options)?;
    let mut explicit_enable: Option<bool> = None;
    let mut explicit_interval: Option<String> = None;
    for option in options {
        match option {
            tidb_ast::TableOption::TtlEnable(enabled) => explicit_enable = Some(*enabled),
            tidb_ast::TableOption::TtlJobInterval(interval) => {
                explicit_interval = Some(interval.clone());
            }
            _ => {}
        }
    }

    if let Some(mut built) = info {
        // Go runs `checkTTLInfoValid` on the NEW config before the job, in
        // its order: temporary table, clustered FLOAT/DOUBLE primary key,
        // foreign-key referral, then the column.
        if table.temp_table_type() != tidb_model::TempTableType::NONE {
            return Err(DriverError::TempTableNotAllowedWithTTL);
        }
        if table.common_handle_offsets().iter().any(|offset| {
            matches!(
                table.columns[*offset].field_type.code(),
                tidb_datatype::FieldTypeCode::Float | tidb_datatype::FieldTypeCode::Double
            )
        }) {
            return Err(DriverError::UnsupportedPrimaryKeyTypeWithTtl);
        }
        if crate::foreign_key::is_table_referred(catalog, database, name) {
            return Err(DriverError::TtlReferencedByForeignKey);
        }
        validate_ttl_column(table, built.column_name.original())?;
        // The merge rules: an explicit enable/interval wins; otherwise the
        // existing config's survives a re-definition.
        if let Some(current) = table.ttl_info() {
            if explicit_enable.is_none() {
                built.enable = current.enable;
            }
            if explicit_interval.is_none() {
                built.job_interval = current.job_interval.clone();
            }
        }
        return Ok(Some(built));
    }

    let Some(current) = table.ttl_info() else {
        // Go: both enable-only and interval-only refuse on a non-TTL table.
        if explicit_enable.is_some() {
            return Err(DriverError::SetTtlOptionForNonTtlTable(
                "TTL_ENABLE".to_owned(),
            ));
        }
        if let Some(_) = explicit_interval {
            return Err(DriverError::SetTtlOptionForNonTtlTable(
                "TTL_JOB_INTERVAL".to_owned(),
            ));
        }
        return Ok(None);
    };
    let mut updated = current.clone();
    if let Some(enabled) = explicit_enable {
        updated.enable = enabled;
    }
    if let Some(interval) = explicit_interval {
        updated.job_interval = interval;
    }
    Ok(Some(updated))
}

/// Go `checkTTLInfoValid` -> `checkTTLInfoColumnType` (`pkg/ddl/ttl.go
/// :141-149`): the TTL column must exist and be a time type.
fn validate_ttl_column(table: &crate::KvTable, named: &str) -> Result<(), DriverError> {
    match table
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(named))
    {
        None => Err(DriverError::UnknownColumnInTtlConfig(named.to_owned())),
        Some(column) if !column.field_type.code().is_type_time() => {
            Err(DriverError::UnsupportedColumnInTtlConfig(named.to_owned()))
        }
        Some(_) => Ok(()),
    }
}

#[cfg(test)]
mod type_change_gate_tests {
    use super::{
        check_type_change_supported, enum_set_column_default, normalize_column_default, DriverError,
    };
    use tidb_datatype::{
        BinaryLiteral, Datum, FieldType, FieldTypeCode, GoString, SessionTimeZone,
    };

    #[test]
    fn enum_set_defaults_preserve_raw_member_bytes() {
        let mut enum_type = FieldType::new(FieldTypeCode::Enum);
        enum_type.set_elems(vec![GoString::from([0xff])]);
        let value = enum_set_column_default(&Datum::Bytes(vec![0xff]), &enum_type)
            .expect("raw ENUM default matches its declaration");
        match value {
            Datum::String(value) => assert_eq!(value.bytes(), [0xff]),
            other => panic!("expected a string default, got {other:?}"),
        }
        let value = normalize_column_default(
            Datum::new_binary_literal(BinaryLiteral::from(vec![0xff])),
            &enum_type,
            "e",
            &SessionTimeZone::utc(),
        )
        .expect("the raw ENUM member passes final strict validation");
        assert_eq!(value.sql_bytes().unwrap(), [0xff]);

        let mut set_type = FieldType::new(FieldTypeCode::Set);
        set_type.set_elems(vec![GoString::from([0xfe])]);
        let value = enum_set_column_default(&Datum::Bytes(vec![0xfe]), &set_type)
            .expect("raw SET default matches its declaration");
        match value {
            Datum::String(value) => assert_eq!(value.bytes(), [0xfe]),
            other => panic!("expected a string default, got {other:?}"),
        }
        let value = normalize_column_default(
            Datum::new_binary_literal(BinaryLiteral::from(vec![0xfe])),
            &set_type,
            "s",
            &SessionTimeZone::utc(),
        )
        .expect("the raw SET member passes final strict validation");
        assert_eq!(value.sql_bytes().unwrap(), [0xfe]);
    }

    #[test]
    fn enum_set_uint_defaults_use_member_names_not_signed_indexes() {
        let number = 9_223_372_036_854_775_808_u64;
        let member = GoString::from(number.to_string());

        for code in [FieldTypeCode::Enum, FieldTypeCode::Set] {
            let mut field_type = FieldType::new(code);
            field_type.set_elems(vec![member.clone()]);
            let value = enum_set_column_default(&Datum::UInt(number), &field_type)
                .expect("a uint default follows Go's string-name branch");
            match value {
                Datum::String(value) => assert_eq!(value.bytes(), member.as_bytes()),
                other => panic!("expected a string default, got {other:?}"),
            }
        }
    }

    /// Rule 4 (`field_type.go:1591-1594`, `TypeTiDBVectorFloat32` on either
    /// side): VECTOR cannot be changed to or from another type family.
    #[test]
    fn vector_type_is_refused_on_either_side() {
        let vector = FieldType::new(FieldTypeCode::VectorFloat32);
        let int = FieldType::new(FieldTypeCode::Long);

        assert!(matches!(
            check_type_change_supported(&vector, &int),
            Err(DriverError::UnsupportedModifyColumnType { .. })
        ));
        assert!(matches!(
            check_type_change_supported(&int, &vector),
            Err(DriverError::UnsupportedModifyColumnType { .. })
        ));
    }

    /// MUTATION PROBE for all five rules: with the gate neutered (always
    /// `Ok`), every refusal below would incorrectly succeed. This test
    /// documents the probe; it is not itself the neutering -- see the SQL
    /// pins in `tidb-session`'s `tests_alter_column.rs` for the five
    /// empty-table cases this backs.
    #[test]
    fn control_conversion_is_not_refused() {
        let from = FieldType::new(FieldTypeCode::Long);
        let to = FieldType::new(FieldTypeCode::LongLong);
        assert!(check_type_change_supported(&from, &to).is_ok());
    }
}
