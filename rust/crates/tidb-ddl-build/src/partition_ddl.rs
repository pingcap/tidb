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

//! Go `pkg/ddl/executor.go`'s partition management, checked against the
//! table its `TableFromMeta` loads: ADD / DROP PARTITION (and INTERVAL's
//! FIRST / LAST PARTITION), `hashPartitionManagement` (ADD on HASH/KEY,
//! COALESCE), `ReorganizePartitions`, `AlterTablePartitioning` and
//! `RemovePartitioning`. Each answers what the job carries; the owner's job
//! steps apply it.

use tidb_ast::{PartitionDefinition, PartitionMethod, PartitionType, TablePartitioning};
use tidb_datatype::FieldType;
use tidb_error::mysql::MysqlError;
use tidb_hack::GoCaseCompare;
use tidb_model::index::IndexInfo;
use tidb_model::table_info::TableInfo;
use tidb_table::partition_routing::{
    PartitionDef, PartitionKind, PartitionSpec, RangeBound, RangeColumnBound,
};

use crate::table_partition::self;
use tidb_table::partition_metadata::StoredPartitionDefinition;

/// Go `StmtCtx.AppendNote(err)` for a refusal `IF [NOT] EXISTS` turns into a
/// note.
fn note(ctx: &tidb_expr::metabuild::Context, proto: &tidb_error::terror::TerrorError, message: impl Into<String>) {
    ctx.append_note(tidb_context::WarnErr::Terror(proto.generate(message)));
}

/// Go `table.Table` as the partition DDL reads it: `Meta()`, and for a
/// partitioned table the partition expression Go builds from it
/// (`tables.newPartitionExpr`).
pub struct PartitionedTable<'a> {
    meta: &'a TableInfo,
    partition: Option<PartitionSpec>,
}

impl<'a> PartitionedTable<'a> {
    /// Go `TableFromMeta`'s partitioned half over `meta`.
    ///
    /// # Errors
    ///
    /// When the stored partitioning cannot be rebuilt.
    pub fn new(meta: &'a TableInfo) -> Result<Self, MysqlError> {
        let partition = meta
            .get_partition_info()
            .map(|partition| {
                let columns = meta.columns.iter_deref().collect::<Vec<_>>();
                tidb_table::table_from_meta::partition_spec_for(&columns, &partition.read())
            })
            .transpose()?;
        Ok(Self { meta, partition })
    }

    /// Go `Table.Meta()`.
    #[must_use]
    pub fn meta(&self) -> &TableInfo {
        self.meta
    }

    fn partition(&self) -> Option<&PartitionSpec> {
        self.partition.as_ref()
    }

    /// Go `TableInfo.Indices`.
    fn indexes(&self) -> Vec<IndexInfo> {
        self.meta
            .indices
            .iter_deref()
            .map(|index| index.read().clone())
            .collect()
    }
}

/// Go's partition-management refusal of a table with AFFINITY
/// (`ErrGeneralUnsupportedDDL`, 8200).
pub fn refuse_affinity(table: &PartitionedTable<'_>, operation: &str) -> Result<(), MysqlError> {
    if table.meta.affinity.is_some() {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.generate_with_stack_by_args(&[
                format!("{} of a table with AFFINITY option", operation).into(),
            ]),
        ));
    }
    Ok(())
}

/// What a HASH/KEY reorganization asks for (Go `hashPartitionManagement`).
#[derive(Clone, Copy)]
pub enum HashManagement<'a> {
    /// `ADD PARTITION PARTITIONS n` or `ADD PARTITION (definitions)`.
    Add {
        /// The written `PARTITIONS n`, zero for a definition list.
        count: u64,
        /// The written definitions.
        definitions: &'a [tidb_ast::PartitionDefinition],
    },
    /// `COALESCE PARTITION n`.
    Coalesce(u64),
}

/// Go `hashPartitionManagement` and the `ReorganizePartitions` it calls for
/// a HASH/KEY table, planned against `table`: every partition is reorganized
/// into `buildHashPartitionDefinitions`' list -- the existing definitions
/// keep their names, comments and placement policies, the written ones
/// follow, and the rest are named `p{i}` -- whose names must be unique
/// (1517), every row is re-hashed into a fresh physical table, and the global
/// indexes are recreated. Refusals in Go's order: a non-partitioned table
/// (1505), AFFINITY (8200), a COALESCE on another method (1509), a count
/// below one (1515) or one removing the last partition (1508), and a value
/// clause on an added definition (1480).
pub fn plan_hash_partition_management(
    table: &PartitionedTable<'_>,
    change: HashManagement<'_>,
    ctx: &tidb_expr::metabuild::Context,
    host: &mut dyn ReorgHost,
) -> Result<PartitionReorgPlan, MysqlError> {
    let Some(partition) = table.partition() else {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED
                .generate_with_stack_by_args(&[]),
        ));
    };
    let hash_or_key = matches!(
        partition.kind,
        tidb_table::partition_routing::PartitionKind::Hash
            | tidb_table::partition_routing::PartitionKind::Key
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
                    tidb_ast::PartitionDefinitionClause::LessThan(_) => ("RANGE", "LESS THAN"),
                    tidb_ast::PartitionDefinitionClause::In(_)
                    | tidb_ast::PartitionDefinitionClause::Default => ("LIST", "IN"),
                    tidb_ast::PartitionDefinitionClause::History { .. } => {
                        ("SYSTEM_TIME", "HISTORY")
                    }
                };
                return Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_error::catalog::ast::ERR_PARTITION_WRONG_VALUES
                        .generate_with_stack_by_args(&[
                            method.to_string().into(),
                            clause.to_string().into(),
                        ]),
                ));
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
                return Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_dbterror::ERR_COALESCE_ONLY_ON_HASH_PARTITION
                        .generate_with_stack_by_args(&[]),
                ));
            }
            if count < 1 {
                return Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_error::catalog::ast::ERR_COALESCE_PARTITION_NO_PARTITION
                        .generate_with_stack_by_args(&[]),
                ));
            }
            if count as usize >= existing.len() {
                return Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_dbterror::ERR_DROP_LAST_PARTITION.generate_with_stack_by_args(&[]),
                ));
            }
            (existing.len() - count as usize, &[][..])
        }
    };
    if count > crate::table_partition::MAX_PARTITIONS as usize {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_dbterror::ERR_TOO_MANY_PARTITIONS.generate_with_stack_by_args(&[]),
        ));
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
                crate::table_partition::partition_definition_comment(definition, ctx, false)?,
                crate::table_partition::partition_definition_placement(definition),
            ));
        } else {
            built.push((format!("p{ordinal}"), String::new(), None));
        }
    }
    // Go `checkPartitionNameUnique` over the reorganized definitions.
    let mut seen = std::collections::HashSet::with_capacity(built.len());
    for (name, _, _) in &built {
        if !seen.insert(crate::table_partition::go_to_lower(name)) {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_dbterror::ERR_SAME_NAME_PARTITION
                    .generate_with_stack_by_args(&[(name.clone()).to_string().into()]),
            ));
        }
    }
    let mut definitions = built
        .into_iter()
        .map(
            |(name, comment, placement_policy)| tidb_table::partition_routing::PartitionDef {
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
    // Go `handlePartitionPlacement`: each policy must exist.
    host.resolve_placement(&mut definitions)?;
    // Go `buildPartitionDefinitionsInfo` ->
    // `rebuildStorageClassForPartitionDefinitions` over the reorganized list.
    assign_partition_storage_classes(table, &partition.kind, &[], &mut definitions, ctx)?;
    // Every partition gets a fresh physical id, as Go's reorganize does.
    for definition in &mut definitions {
        definition.id = host.allocate_table_id();
    }
    let source_ids = existing.iter().map(|definition| definition.id).collect();
    let key = matches!(partition.kind, tidb_table::partition_routing::PartitionKind::Key);
    let stored = crate::table_partition::StoredPartitionMetadata {
        kind: if key {
            tidb_ast::PartitionType::KEY
        } else {
            tidb_ast::PartitionType::HASH
        },
        enable: true,
        num: definitions.len() as u64,
        expr: if key {
            String::new()
        } else {
            partition.expr_text.clone()
        },
        columns: if key {
            partition.dependencies.clone()
        } else {
            Vec::new()
        },
        is_empty_columns: false,
        definitions: definitions
            .iter()
            .map(
                |definition| tidb_table::partition_metadata::StoredPartitionDefinition {
                    id: definition.id,
                    name: definition.name.clone(),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: definition.comment.clone(),
                    placement_policy: definition.placement_policy.clone(),
                    storage_class: definition.storage_class.clone(),
                },
            )
            .collect(),
    };
    Ok(PartitionReorgPlan {
        source_ids,
        interval_rewrite: None,
        // Go `ReorganizePartitions` warns on success, which both ADD and
        // COALESCE reach (oracle m24: COALESCE answers ok with this warning).
        warning: Some(STATS_OUTDATED_RELATED),
        stored: Some(stored),
    })
}

/// The partitioning an ADD or DROP PARTITION leaves, and Go's job-query
/// rewrite for INTERVAL's FIRST / LAST PARTITION sugar (the written clause and
/// the partitions it stands for).
pub struct PartitionChange {
    /// The table's partitioning after the change.
    pub partition: PartitionSpec,
    /// Go `ctx.SetValue(sessionctx.QueryString, ...)`'s rewrite, as
    /// (written, generated) text.
    pub query_rewrite: Option<(String, String)>,
}

/// Go's partition-change builders over one table, for the DDL job path:
/// `buildAddedPartitionInfo` / `checkAddPartition` /
/// `CheckAndUpdateAddedPartitionDefinitions` for an ADD (and INTERVAL's LAST
/// PARTITION), `CheckDropTablePartition` for a DROP (and INTERVAL's FIRST
/// PARTITION). `table` is the stored table as Go's `TableFromMeta` loads it;
/// added partitions carry no ids yet (Go's `assignGIDsForJobs` draws them).
///
/// # Errors
///
/// The builders' refusals.
pub fn plan_partition_change(
    table: &PartitionedTable<'_>,
    action: &tidb_ast::AlterPartitionAction,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<PartitionChange, MysqlError> {
    let mut partition = table.partition().cloned().ok_or_else(|| {
        MysqlError::from_terror(
            &tidb_dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED
                .generate_with_stack_by_args(&[]),
        )
    })?;
    let mut query_rewrite = None;
    match action {
        tidb_ast::AlterPartitionAction::Add {
            if_not_exists,
            spec,
            ..
        } => {
            if let Some((added, kind)) = plan_add_partitions(table, *if_not_exists, spec, ctx)? {
                append_partitions(&mut partition, added, kind);
            }
        }
        tidb_ast::AlterPartitionAction::LastPartitionLessThan { expr, .. } => {
            let (added, kind, rewrite) = plan_last_partition_less_than(table, expr, ctx)?;
            append_partitions(&mut partition, added, kind);
            query_rewrite = Some(rewrite);
        }
        tidb_ast::AlterPartitionAction::Drop { if_exists, names } => {
            if let Some(ordinals) = plan_drop_partitions(table, *if_exists, names, ctx)? {
                drop_partitions(&mut partition, &ordinals);
            }
        }
        tidb_ast::AlterPartitionAction::FirstPartitionLessThan { expr, .. } => {
            let (ordinals, rewrite) = plan_first_partition_less_than(table, expr, ctx)?;
            drop_partitions(&mut partition, &ordinals);
            query_rewrite = Some(rewrite);
        }
        _ => {
            return Err(tidb_error::mysql::MysqlError::unknown(
                "the job path plans only ADD and DROP PARTITION changes",
            ));
        }
    }
    Ok(PartitionChange {
        partition,
        query_rewrite,
    })
}

/// Go `updateDroppingPartitionInfo`'s effect on the partitioning: the
/// definitions at `ordinals` leave, with every routing entry they owned.
fn drop_partitions(partition: &mut PartitionSpec, ordinals: &[usize]) {
    let mut next = 0usize;
    let remap = (0..partition.definitions.len())
        .map(|old| {
            if ordinals.contains(&old) {
                None
            } else {
                let new = next;
                next += 1;
                Some(new)
            }
        })
        .collect::<Vec<_>>();
    partition.definitions = partition
        .definitions
        .drain(..)
        .enumerate()
        .filter_map(|(old, definition)| remap[old].map(|_| definition))
        .collect();
    match &mut partition.kind {
        // NONE keeps no per-partition structure beside the definitions
        // that were just remapped.
        PartitionKind::None => {}
        PartitionKind::Range { less_than, .. } => {
            *less_than = less_than
                .drain(..)
                .enumerate()
                .filter_map(|(old, bound)| remap[old].map(|_| bound))
                .collect();
        }
        PartitionKind::RangeColumns { less_than, .. } => {
            *less_than = less_than
                .drain(..)
                .enumerate()
                .filter_map(|(old, bound)| remap[old].map(|_| bound))
                .collect();
        }
        PartitionKind::List {
            values,
            null_partition,
            default_partition,
            ..
        } => {
            *values = values
                .drain(..)
                .filter_map(|(value, old)| remap[old].map(|new| (value, new)))
                .collect();
            *null_partition = null_partition.and_then(|old| remap[old]);
            *default_partition = default_partition.and_then(|old| remap[old]);
        }
        PartitionKind::ListColumns {
            values,
            keys,
            default_partition,
            ..
        } => {
            *values = values
                .drain(..)
                .filter_map(|(value, old)| remap[old].map(|new| (value, new)))
                .collect();
            keys.retain(|_, old| {
                let Some(new) = remap[*old] else {
                    return false;
                };
                *old = new;
                true
            });
            *default_partition = default_partition.and_then(|old| remap[old]);
        }
        PartitionKind::Hash | PartitionKind::Key => {
            unreachable!("DDL allows DROP PARTITION only for RANGE/LIST")
        }
    }
}

/// Go `CheckAndUpdateAddedPartitionDefinitions`' result on the partitioning:
/// `definitions` follow the existing ones, `added_kind`'s routing entries
/// renumbered after them.
fn append_partitions(
    partition: &mut PartitionSpec,
    definitions: Vec<PartitionDef>,
    added_kind: PartitionKind,
) {
    let offset = partition.definitions.len();
    match (&mut partition.kind, added_kind) {
        (
            PartitionKind::Range {
                less_than,
                unsigned,
            },
            PartitionKind::Range {
                less_than: added_bounds,
                unsigned: added_unsigned,
            },
        ) => {
            debug_assert_eq!(*unsigned, added_unsigned);
            less_than.extend(added_bounds);
        }
        (
            PartitionKind::RangeColumns {
                less_than,
                field_types,
            },
            PartitionKind::RangeColumns {
                less_than: added_bounds,
                field_types: added_types,
            },
        ) => {
            debug_assert_eq!(*field_types, added_types);
            less_than.extend(added_bounds);
        }
        (
            PartitionKind::List {
                values,
                null_partition,
                default_partition,
                unsigned,
            },
            PartitionKind::List {
                values: added_values,
                null_partition: added_null,
                default_partition: added_default,
                unsigned: added_unsigned,
            },
        ) => {
            debug_assert_eq!(*unsigned, added_unsigned);
            values.extend(
                added_values
                    .into_iter()
                    .map(|(value, owner)| (value, owner + offset)),
            );
            if let Some(owner) = added_null {
                *null_partition = Some(owner + offset);
            }
            if let Some(owner) = added_default {
                *default_partition = Some(owner + offset);
            }
        }
        (
            PartitionKind::ListColumns {
                values,
                keys,
                default_partition,
                field_types,
            },
            PartitionKind::ListColumns {
                values: added_values,
                keys: added_keys,
                default_partition: added_default,
                field_types: added_types,
            },
        ) => {
            debug_assert_eq!(*field_types, added_types);
            values.extend(
                added_values
                    .into_iter()
                    .map(|(value, owner)| (value, owner + offset)),
            );
            keys.extend(
                added_keys
                    .into_iter()
                    .map(|(key, owner)| (key, owner + offset)),
            );
            if let Some(owner) = added_default {
                *default_partition = Some(owner + offset);
            }
        }
        _ => unreachable!("DDL folds added definitions with the existing partition method"),
    }
    partition.definitions.extend(definitions);
}

/// Go `CheckDropTablePartition`: the ordinals of `table`'s partitions that
/// `DROP PARTITION names` removes; `None` when `IF EXISTS` met a missing
/// name (Go's note is left on `ctx`).
pub fn plan_drop_partitions(
    table: &PartitionedTable<'_>,
    if_exists: bool,
    names: &[String],
    ctx: &tidb_expr::metabuild::Context,
) -> Result<Option<Vec<usize>>, MysqlError> {
    let Some(partition) = table.partition() else {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED
                .generate_with_stack_by_args(&[]),
        ));
    };
    if !matches!(
        partition.kind,
        PartitionKind::Range { .. }
            | PartitionKind::RangeColumns { .. }
            | PartitionKind::List { .. }
            | PartitionKind::ListColumns { .. }
    ) {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_dbterror::ERR_ONLY_ON_RANGE_LIST_PARTITION
                .generate_with_stack_by_args(&["DROP".into()]),
        ));
    }
    refuse_affinity(table, "DROP PARTITION")?;
    if partition.definitions.len() <= names.len() {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_dbterror::ERR_DROP_LAST_PARTITION.generate_with_stack_by_args(&[]),
        ));
    }
    let mut ordinals = Vec::with_capacity(names.len());
    for name in names {
        let Some(ordinal) = partition.definitions.iter().position(|definition| {
            crate::table_partition::partition_names_equal(&definition.name, name)
        }) else {
            if if_exists {
                note(
                    ctx,
                    &tidb_dbterror::ERR_DROP_PARTITION_NON_EXISTENT,
                    "Error in list of partitions to DROP",
                );
                return Ok(None);
            }
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_dbterror::ERR_DROP_PARTITION_NON_EXISTENT
                    .generate_with_stack_by_args(&["DROP".into()]),
            ));
        };
        if ordinals.contains(&ordinal) {
            if if_exists {
                note(
                    ctx,
                    &tidb_dbterror::ERR_DROP_PARTITION_NON_EXISTENT,
                    "Error in list of partitions to DROP",
                );
                return Ok(None);
            }
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_dbterror::ERR_DROP_PARTITION_NON_EXISTENT
                    .generate_with_stack_by_args(&["DROP".into()]),
            ));
        }
        ordinals.push(ordinal);
    }
    Ok(Some(ordinals))
}

/// The method Go prints for a RANGE or LIST table's partitioning (`RANGE`,
/// `LIST`); anything else falls to the list spelling the old message used.
fn non_hash_partition_method(table: &PartitionedTable<'_>) -> &'static str {
    table
        .partition()
        .map_or("LIST", |partition| match partition.kind.sql() {
            "RANGE" => "RANGE",
            _ => "LIST",
        })
}

/// Go `AddTablePartitions` for a RANGE or LIST table (`buildAddedPartitionInfo`,
/// `checkAddPartition`, `CheckAndUpdateAddedPartitionDefinitions`): the
/// definitions `ADD PARTITION spec` appends to `table`, without ids, and the
/// partitioning after the append; `None` when `IF NOT EXISTS` met an
/// existing name (Go's note is left on `ctx`).
pub fn plan_add_partitions(
    table: &PartitionedTable<'_>,
    if_not_exists: bool,
    spec: &tidb_ast::AddPartitionSpec,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<Option<(Vec<PartitionDef>, PartitionKind)>, MysqlError> {
    if table.partition().is_some() {
        refuse_affinity(table, "ADD PARTITION")?;
    }
    let definitions = match spec {
        tidb_ast::AddPartitionSpec::Definitions(definitions) => definitions,
        // Go `BuildAddedPartitionInfo`: a RANGE/LIST table needs its
        // definitions written out; a bare count carries none.
        tidb_ast::AddPartitionSpec::Count(_) => {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_error::catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED
                    .generate_with_stack_by_args(&[(non_hash_partition_method(table))
                        .to_string()
                        .into()]),
            ));
        }
    };
    if definitions.is_empty() {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_error::catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED.generate_with_stack_by_args(
                &[(non_hash_partition_method(table)).to_string().into()],
            ),
        ));
    }
    // Go `checkListPartitions` / `buildRangePartitionDefinitions`: a definition
    // without its VALUES clause is 1479 naming the clause the method needs.
    let method = non_hash_partition_method(table);
    if let Some(clause) = match method {
        "LIST" => Some("IN"),
        "RANGE" => Some("LESS THAN"),
        _ => None,
    } {
        if definitions.iter().any(|definition| {
            matches!(definition.clause, tidb_ast::PartitionDefinitionClause::None)
        }) {
            return Err(MysqlError::from_terror(
                &tidb_error::catalog::ast::ERR_PARTITION_REQUIRES_VALUES
                    .generate_with_stack_by_args(&[method.into(), clause.into()]),
            ));
        }
    }
    // Go reads a definition's comment and placement policy and ignores its
    // other options (`ENGINE`, `DATA DIRECTORY`, ...); subpartitions are not
    // ported.
    if definitions
        .iter()
        .any(|definition| !definition.sub_partitions.is_empty())
    {
        return Err(tidb_error::mysql::MysqlError::unknown(
            "ALTER TABLE ... ADD PARTITION subpartitions are not supported yet",
        ));
    }

    let added_kind = {
        let Some(partition) = table.partition() else {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED
                    .generate_with_stack_by_args(&[]),
            ));
        };
        if partition.definitions.len() + definitions.len()
            > crate::table_partition::MAX_PARTITIONS as usize
        {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_dbterror::ERR_TOO_MANY_PARTITIONS.generate_with_stack_by_args(&[]),
            ));
        }
        for definition in definitions {
            let duplicate_existing = partition.definitions.iter().any(|old| {
                crate::table_partition::partition_names_equal(&old.name, &definition.name)
            });
            let duplicate_added = definitions
                .iter()
                .filter(|candidate| {
                    crate::table_partition::partition_names_equal(&candidate.name, &definition.name)
                })
                .count()
                > 1;
            if duplicate_existing || duplicate_added {
                if if_not_exists {
                    note(
                        ctx,
                        &tidb_dbterror::ERR_SAME_NAME_PARTITION,
                        format!("Duplicate partition name {}", definition.name),
                    );
                    return Ok(None);
                }
                return Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_dbterror::ERR_SAME_NAME_PARTITION.generate_with_stack_by_args(&[
                        (definition.name.clone()).to_string().into(),
                    ]),
                ));
            }
        }

        let (names, types) = visible_names_and_types(table);
        match &partition.kind {
            // A table wearing `PartitionTypeNone` is mid-`ALTER ...
            // PARTITION BY`; it has no LIST values for a definition to join.
            PartitionKind::None => {
                return Err(tidb_error::mysql::MysqlError::unknown(
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
                    // Go `dbterror.ErrGeneralUnsupportedDDL` (8200,
                    // "Unsupported %s"), `pkg/ddl/executor.go:2315`.
                    return Err(MysqlError::from_terror(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.generate_with_stack_by_args(&["ADD List partition, already contains DEFAULT partition. Please use REORGANIZE PARTITION instead".into()])));
                }
                // On the ALTER path the added definitions are checked on
                // their own, so a collision among them is raised here rather
                // than deferred: there is no earlier partition-level check
                // for it to lose to.
                let (added, duplicate) =
                    tidb_table::partition_fold::build_list_values_with_unsigned(
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
                                crate::table_partition::check_too_long_partition_name(
                                    &definition.name,
                                )
                            })
                        },
                    )?;
                if let Some(duplicate) = duplicate {
                    return Err(duplicate.into());
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
                    return Err(tidb_error::mysql::MysqlError::from_terror(
                        &tidb_dbterror::ERR_MULTIPLE_DEF_CONST_IN_LIST_PART
                            .generate_with_stack_by_args(&[]),
                    ));
                }
                added
            }
            PartitionKind::ListColumns {
                keys,
                default_partition,
                ..
            } => {
                if default_partition.is_some() {
                    // Go `dbterror.ErrGeneralUnsupportedDDL` (8200,
                    // "Unsupported %s"), `pkg/ddl/executor.go:2315`.
                    return Err(MysqlError::from_terror(&tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.generate_with_stack_by_args(&["ADD List partition, already contains DEFAULT partition. Please use REORGANIZE PARTITION instead".into()])));
                }
                let columns = partition
                    .dependencies
                    .iter()
                    .map(|name| vec![name.clone()])
                    .collect::<Vec<_>>();
                let (_, added) = crate::table_partition_list::build_list_columns_values(
                    &columns,
                    definitions,
                    &names,
                    &types,
                    ctx,
                    // The added definitions are new metadata this statement
                    // is writing, so they face the CREATE-time rules.
                    // Go reaches these builders through the SAME
                    // `buildPartitionDefinitionsInfo` loop a CREATE uses, so
                    // an added partition's name faces the 64-rune rule at the
                    // same point. Per-partition options are refused above, so
                    // there is no comment to validate.
                    &mut |ordinal: usize| {
                        definitions.get(ordinal).map_or(Ok(()), |definition| {
                            crate::table_partition::check_too_long_partition_name(&definition.name)
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
                    return Err(tidb_error::mysql::MysqlError::from_terror(
                        &tidb_dbterror::ERR_MULTIPLE_DEF_CONST_IN_LIST_PART
                            .generate_with_stack_by_args(&[]),
                    ));
                }
                added
            }
            PartitionKind::Range {
                less_than,
                unsigned,
            } => {
                // `ALTER TABLE ... ADD PARTITION` is a WRITTEN clause, so it
                // validates like a CREATE.
                let added = crate::table_partition_range::build_range_bounds_with_unsigned(
                    definitions,
                    *unsigned,
                    ctx,
                    &mut |ordinal: usize| {
                        definitions.get(ordinal).map_or(Ok(()), |definition| {
                            // Go's RANGE loop validates the comment length
                            // (1793) before the name's.
                            crate::table_partition::partition_definition_comment(
                                definition, ctx, true,
                            )?;
                            crate::table_partition::check_too_long_partition_name(&definition.name)
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
                        return Err(tidb_error::mysql::MysqlError::from_terror(
                            &tidb_dbterror::ERR_PARTITION_MAXVALUE
                                .generate_with_stack_by_args(&[]),
                        ))
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
                            return Err(tidb_error::mysql::MysqlError::from_terror(
                                &tidb_dbterror::ERR_PARTITION_MAXVALUE
                                    .generate_with_stack_by_args(&[]),
                            ));
                        }
                        RangeBound::Value(value) => {
                            if let Some(previous) = current {
                                let increases = if *unsigned {
                                    (*value as u64) > (previous as u64)
                                } else {
                                    *value > previous
                                };
                                if !increases {
                                    return Err(tidb_error::mysql::MysqlError::from_terror(
                                        &tidb_dbterror::ERR_RANGE_NOT_INCREASING
                                            .generate_with_stack_by_args(&[]),
                                    ));
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
                    crate::table_partition_range::build_range_columns_bounds(
                        &columns,
                        definitions,
                        &names,
                        &types,
                        ctx,
                        &mut |ordinal: usize| {
                            definitions.get(ordinal).map_or(Ok(()), |definition| {
                                crate::table_partition::check_too_long_partition_name(
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
                crate::table_partition_range::check_range_columns_strictly_increasing(
                    &combined,
                    field_types,
                )?;
                PartitionKind::RangeColumns {
                    less_than: added_bounds.clone(),
                    field_types: added_types,
                }
            }
            PartitionKind::Hash | PartitionKind::Key => {
                return Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_dbterror::ERR_ONLY_ON_RANGE_LIST_PARTITION
                        .generate_with_stack_by_args(&["ADD".into()]),
                ));
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
    let range_bound_text = |ordinal: usize| -> Result<Vec<String>, MysqlError> {
        Ok(match &added_kind {
            PartitionKind::Range {
                less_than,
                unsigned,
            } => less_than
                .get(ordinal)
                .map(|bound| {
                    vec![crate::table_partition::stored_range_bound_text(
                        *bound, *unsigned,
                    )]
                })
                .unwrap_or_default(),
            PartitionKind::RangeColumns {
                less_than,
                field_types,
            } => crate::table_partition::stored_range_columns_bound_text(
                less_than.get(ordinal).map_or(&[][..], Vec::as_slice),
                field_types,
            )?,
            _ => Vec::new(),
        })
    };
    let mut added_definitions = Vec::with_capacity(definitions.len());
    for (ordinal, definition) in definitions.iter().enumerate() {
        added_definitions.push(PartitionDef {
            // Allocated once every check has passed, as Go's
            // `assignPartitionIDs` runs last.
            id: 0,
            name: definition.name.clone(),
            less_than: range_bound_text(ordinal)?,
            in_values: crate::table_partition::stored_in_values(
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
    Ok(Some((added_definitions, added_kind)))
}

/// Go `DropTablePartition` for `FIRST PARTITION LESS THAN (expr)` over
/// `table`: the ordinals of the partitions the INTERVAL names below the new
/// first one. Go's job-query rewrite (the clause becomes the names) is left
/// on `ctx`.
pub fn plan_first_partition_less_than(
    table: &PartitionedTable<'_>,
    bound: &tidb_ast::NodeBox<tidb_ast::Expr>,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<(Vec<usize>, (String, String)), MysqlError> {
    let Some(partition) = table.partition() else {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED
                .generate_with_stack_by_args(&[]),
        ));
    };
    let unsupported = |message: &str| {
        MysqlError::from_terror(
            &tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL
                .generate_with_stack_by_args(&[message.into()]),
        )
    };
    let Some(interval) = crate::partition_interval::interval_from_table(partition, ctx) else {
        return Err(unsupported(
            "FIRST PARTITION, does not seem like an INTERVAL partitioned table",
        ));
    };
    let generated = crate::partition_interval::generate_from_interval(
        &interval.interval(),
        crate::partition_interval::IntervalStatement::DropFirst(bound),
        interval.column.as_ref(),
        partition.definitions.len(),
        ctx,
    )?;
    let null_offset = usize::from(interval.null_partition);
    if generated.is_empty() || generated.len() >= partition.definitions.len() - null_offset {
        return Err(unsupported(
            "FIRST PARTITION, number of partitions does not match",
        ));
    }
    if generated.len() <= 1 {
        return Err(unsupported(
            "FIRST PARTITION, given value does not generate a list of partition names to be dropped",
        ));
    }
    // The last generated partition becomes the new first one.
    let names = partition.definitions[null_offset..null_offset + generated.len() - 1]
        .iter()
        .map(|definition| definition.name.clone())
        .collect::<Vec<_>>();
    let ordinals = plan_drop_partitions(table, false, &names, ctx)?
        .expect("without IF EXISTS a missing partition is an error");
    // Go `DropTablePartition` rewrites the job's query: the FIRST clause
    // becomes the partitions it names.
    let replacement = format!(
        "DROP PARTITION {}",
        names
            .iter()
            .map(|name| crate::table_partition::escape_identifier(name, ctx.get_sql_mode().has_ansi_quotes_mode()))
            .collect::<Vec<_>>()
            .join(", ")
    );
    Ok((
        ordinals,
        (
            String::from_utf8_lossy(bound.original_text()).into_owned(),
            replacement,
        ),
    ))
}

/// Go `AddTablePartitions` for `LAST PARTITION LESS THAN (expr)`
/// (`buildAddedPartitionDefs`) over `table`: the INTERVAL the table was
/// generated with extends it up to the new last bound. Answers what
/// [`plan_add_partitions`] answers; Go's job-query rewrite (the clause
/// becomes the added partitions) is left on `ctx`.
pub fn plan_last_partition_less_than(
    table: &PartitionedTable<'_>,
    bound: &tidb_ast::NodeBox<tidb_ast::Expr>,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<(Vec<PartitionDef>, PartitionKind, (String, String)), MysqlError> {
    let Some(partition) = table.partition() else {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED
                .generate_with_stack_by_args(&[]),
        ));
    };
    refuse_affinity(table, "ADD PARTITION")?;
    let unsupported = |message: &str| {
        MysqlError::from_terror(
            &tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL
                .generate_with_stack_by_args(&[message.into()]),
        )
    };
    match partition.kind {
        PartitionKind::Hash | PartitionKind::Key => {
            return Err(unsupported("LAST PARTITION of HASH/KEY partitioned table"))
        }
        PartitionKind::List { .. } | PartitionKind::ListColumns { .. } => {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_error::catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED
                    .generate_with_stack_by_args(&["LIST".into()]),
            ))
        }
        _ => {}
    }
    let Some(interval) = crate::partition_interval::interval_from_table(partition, ctx) else {
        return Err(unsupported(
            "LAST PARTITION, does not seem like an INTERVAL partitioned table",
        ));
    };
    if interval.maxvalue_partition {
        return Err(unsupported("LAST PARTITION when MAXVALUE partition exists"));
    }
    let definitions = crate::partition_interval::generate_from_interval(
        &interval.interval(),
        crate::partition_interval::IntervalStatement::AddLast(bound),
        interval.column.as_ref(),
        partition.definitions.len(),
        ctx,
    )?;
    let (added_definitions, added_kind) = plan_add_partitions(
        table,
        false,
        &tidb_ast::AddPartitionSpec::Definitions(definitions),
        ctx,
    )?
    .expect("without IF NOT EXISTS an existing partition is an error");
    let text = crate::table_partition::append_partition_defs(
        &added_definitions,
        &added_kind,
        ctx.get_sql_mode().has_ansi_quotes_mode(),
    );
    let rewrite = (
        String::from_utf8_lossy(bound.original_text()).into_owned(),
        format!("ADD PARTITION {}", text.trim_start_matches('\n')),
    );
    Ok((added_definitions, added_kind, rewrite))
}

/// Go `rebuildStorageClassForPartitions`: resolves the table's
/// ENGINE_ATTRIBUTE over `preceding` followed by `definitions` and stores the
/// result on `definitions`. A table whose attribute names no storage class
/// leaves them unset.
fn assign_partition_storage_classes(
    table: &PartitionedTable<'_>,
    kind: &PartitionKind,
    preceding: &[&PartitionDef],
    definitions: &mut [PartitionDef],
    ctx: &tidb_expr::metabuild::Context,
) -> Result<(), MysqlError> {
    let Some(settings) = crate::storage_class::storage_class_settings_of(&table.meta.engine_attribute)?
    else {
        return Ok(());
    };
    let combined = preceding
        .iter()
        .copied()
        .chain(definitions.iter())
        .collect::<Vec<_>>();
    let classes =
        crate::storage_class::build_storage_class_for_partitions(&settings, kind, &combined, ctx)?;
    for (definition, class) in definitions
        .iter_mut()
        .zip(classes.into_iter().skip(preceding.len()))
    {
        definition.storage_class = class;
    }
    Ok(())
}


/// Go `ReorganizePartitions`' success warning.
pub const STATS_OUTDATED_RELATED: &str = "The statistics of related partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now";
const STATS_OUTDATED_NEW: &str = "The statistics of new partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now";

/// Go `TableInfo.Columns`' names and types, the list the partition builders
/// resolve against.
fn visible_names_and_types(table: &PartitionedTable<'_>) -> (Vec<String>, Vec<FieldType>) {
    table
        .meta
        .columns
        .iter_deref()
        .map(|column| {
            let column = column.read();
            (column.name.original().to_owned(), column.field_type.clone())
        })
        .unzip()
}

/// The clustered primary key's column offsets in [`visible_names_and_types`]:
/// Go `GetPkColInfo` for an integer handle, the PRIMARY index's columns for a
/// common handle, none for a heap table.
fn handle_offsets(table: &PartitionedTable<'_>) -> Vec<usize> {
    let meta = table.meta;
    let names = visible_names_and_types(table).0;
    let offset_of = |name: &str| names.iter().position(|candidate| candidate.go_lower_eq(name));
    if meta.pk_is_handle {
        return meta
            .columns
            .iter_deref()
            .find(|column| column.read().field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY))
            .and_then(|column| offset_of(column.read().name.original()))
            .into_iter()
            .collect();
    }
    if meta.is_common_handle {
        if let Some(primary) = meta.indices.iter_deref().find(|index| index.read().primary) {
            return primary
                .read()
                .columns
                .iter_deref()
                .filter_map(|column| offset_of(column.read().name.original()))
                .collect();
        }
    }
    Vec::new()
}

/// What a partition reorganization needs from the catalog that owns the
/// table: the ids Go's submitter allocates, and the placement policies a new
/// definition may name.
pub trait ReorgHost {
    /// Go `genGlobalIDs`, one id.
    fn allocate_table_id(&mut self) -> i64;
    /// Go `handlePartitionPlacement`: every policy a new definition names
    /// must exist, and the reference records its id.
    fn resolve_placement(&self, definitions: &mut [PartitionDef]) -> Result<(), MysqlError>;
}

/// A partition reorganization planned against the table before it: Go's
/// executor checks and the job's first state, and what `onReorganizePartition`
/// leaves behind once the job is done.
pub struct PartitionReorgPlan {
    /// The physical ids the reorganization replaces.
    pub source_ids: Vec<i64>,
    /// Go `rewritePartitionQueryString`: INTERVAL sugar's job query records
    /// the partitions it generated, as (written, generated) text.
    pub interval_rewrite: Option<(String, String)>,
    /// The warning the job raises.
    pub warning: Option<&'static str>,
    /// The partition metadata the table stores afterwards, rendered as Go
    /// stores it (folded COLUMNS bounds, written expression bounds), the new
    /// definitions under the host's placeholder ids; `None` once
    /// partitioning is removed.
    pub stored: Option<crate::table_partition::StoredPartitionMetadata>,
}

/// The method of an existing partitioned table, as the clause that would
/// have created it: what Go's `BuildAddedPartitionInfo` copies into the new
/// `PartitionInfo` (`Type`, `Expr`, `Columns`).
fn existing_method(partition: &PartitionSpec) -> Result<PartitionMethod, MysqlError> {
    let columns = || {
        partition
            .dependencies
            .iter()
            .map(|name| vec![name.clone()])
            .collect::<Vec<_>>()
    };
    let expression = || {
        tidb_model::generated_expr::parse_expression(&partition.expr_text)
            .map(Some)
            .map_err(|error| {
                tidb_error::mysql::MysqlError::from_terror(&tidb_util::misc::syntax_error(
                    error.message,
                ))
            })
    };
    let (kind, expr, columns) = match &partition.kind {
        PartitionKind::Range { .. } => (PartitionType::RANGE, expression()?, Vec::new()),
        PartitionKind::RangeColumns { .. } => (PartitionType::RANGE, None, columns()),
        PartitionKind::List { .. } => (PartitionType::LIST, expression()?, Vec::new()),
        PartitionKind::ListColumns { .. } => (PartitionType::LIST, None, columns()),
        PartitionKind::Hash | PartitionKind::Key | PartitionKind::None => {
            unreachable!("REORGANIZE PARTITION admits RANGE and LIST only")
        }
    };
    Ok(PartitionMethod {
        kind,
        linear: false,
        expr,
        columns,
        key_algorithm: None,
        unit: None,
        limit: 0,
        count: 0,
        interval: None,
    })
}

/// The stored metadata for `spec`, each definition carrying its allocated id.
fn stored_with_ids(
    clause: &TablePartitioning,
    spec: &PartitionSpec,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<table_partition::StoredPartitionMetadata, MysqlError> {
    let mut stored = table_partition::stored_partition_metadata(clause, spec, ctx)?;
    for (definition, built) in stored.definitions.iter_mut().zip(&spec.definitions) {
        definition.id = built.id;
    }
    Ok(stored)
}

fn stored_definition(definition: &PartitionDef) -> StoredPartitionDefinition {
    StoredPartitionDefinition {
        id: definition.id,
        name: definition.name.clone(),
        less_than: definition.less_than.clone(),
        in_values: definition.in_values.clone(),
        comment: definition.comment.clone(),
        placement_policy: definition.placement_policy.clone(),
        storage_class: definition.storage_class.clone(),
    }
}

/// Go `getReplacedPartitionIDs`: the ordinals of the named partitions, with
/// the first and last of them.
fn replaced_partitions(
    partition: &PartitionSpec,
    names: &[String],
) -> Result<(usize, usize, Vec<usize>), MysqlError> {
    let mut ordinals: Vec<usize> = Vec::with_capacity(names.len());
    for name in names {
        let Some(ordinal) = partition
            .definitions
            .iter()
            .position(|definition| table_partition::partition_names_equal(&definition.name, name))
        else {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_WRONG_PARTITION_NAME.generate_with_stack_by_args(&[]),
            ));
        };
        if ordinals.contains(&ordinal) {
            // Go returns the bare error, so the name slot stays unformatted.
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_SAME_NAME_PARTITION,
            ));
        }
        ordinals.push(ordinal);
    }
    let first = *ordinals.iter().min().expect("the grammar requires a name");
    let last = *ordinals.iter().max().expect("the grammar requires a name");
    if matches!(
        partition.kind,
        PartitionKind::Range { .. } | PartitionKind::RangeColumns { .. }
    ) && ordinals.len() != last - first + 1
    {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_GENERAL_UNSUPPORTED_DDL.generate_with_stack_by_args(&[
                "REORGANIZE PARTITION of RANGE; not adjacent partitions".into(),
            ]),
        ));
    }
    ordinals.sort_unstable();
    Ok((first, last, ordinals))
}

/// Go `ReorganizePartitions` and the `checkReorgPartitionDefs` battery,
/// planned against `table`.
pub fn plan_reorganize_partition(
    table: &PartitionedTable<'_>,
    names: &[String],
    definitions: &[PartitionDefinition],
    ctx: &tidb_expr::metabuild::Context,
    host: &mut dyn ReorgHost,
) -> Result<PartitionReorgPlan, MysqlError> {
    let (mut spec, source_ids, new_offset, clause) = {
        let Some(partition) = table.partition() else {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED
                    .generate_with_stack_by_args(&[]),
            ));
        };
        refuse_affinity(table, "REORGANIZE PARTITION")?;
        if !matches!(
            partition.kind,
            PartitionKind::Range { .. }
                | PartitionKind::RangeColumns { .. }
                | PartitionKind::List { .. }
                | PartitionKind::ListColumns { .. }
        ) {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_UNSUPPORTED_REORGANIZE_PARTITION
                    .generate_with_stack_by_args(&[]),
            ));
        }
        let (_, last, dropped) = replaced_partitions(partition, names)?;
        // Go `BuildAddedPartitionInfo`: RANGE and LIST need written
        // definitions.
        if definitions.is_empty() {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_error::catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED
                    .generate_with_stack_by_args(&[(partition.kind.sql()).to_string().into()]),
            ));
        }

        // Go `getReorganizedDefinitions`: RANGE splices the new definitions
        // over the replaced run; LIST puts them where the first replaced
        // definition stood.
        let old = &partition.definitions;
        let mut layout: Vec<Option<&PartitionDef>> = Vec::new();
        let mut written_at = Vec::new();
        let mut placed = false;
        for (ordinal, definition) in old.iter().enumerate() {
            if dropped.contains(&ordinal) {
                if !placed {
                    written_at.push(layout.len());
                    layout.extend(std::iter::repeat_n(None, definitions.len()));
                    placed = true;
                }
                continue;
            }
            layout.push(Some(definition));
        }
        let new_offset = written_at[0];
        let mut written = definitions.iter();
        let ast_definitions = layout
            .iter()
            .map(|slot| match slot {
                Some(definition) => {
                    Ok(
                        tidb_table::partition_metadata::stored_definitions_as_ast(&[stored_definition(
                            definition,
                        )])?
                        .remove(0),
                    )
                }
                None => Ok(written
                    .next()
                    .expect("one slot per written definition")
                    .clone()),
            })
            .collect::<Result<Vec<_>, MysqlError>>()?;
        let clause = TablePartitioning {
            method: existing_method(partition)?,
            subpartition: None,
            definitions: ast_definitions,
            update_indexes: Vec::new(),
        };
        let (names, types) = visible_names_and_types(table);
        let mut ids = layout
            .iter()
            .map(|slot| slot.map_or(0, |definition| definition.id))
            .collect::<std::collections::VecDeque<_>>();
        let mut spec = table_partition::build_partitioning(
            &clause,
            &names,
            &types,
            &table.indexes(),
            &handle_offsets(table),
            &mut || ids.pop_front().unwrap_or(0),
            ctx,
        )?;
        // The surviving definitions keep everything Go keeps on them.
        for (built, slot) in spec.definitions.iter_mut().zip(&layout) {
            if let Some(definition) = slot {
                built.comment.clone_from(&definition.comment);
                built
                    .placement_policy
                    .clone_from(&definition.placement_policy);
                built.storage_class.clone_from(&definition.storage_class);
            }
        }
        // Go `updatePartInfoDefinitionsFromFinalDefinitions`: storage classes
        // resolve over the final list, and only the new definitions take
        // theirs.
        if let Some(classes) =
            crate::storage_class::partition_storage_classes(&table.meta.engine_attribute, &spec, ctx)?
        {
            for (offset, class) in classes.into_iter().enumerate() {
                if layout[offset].is_none() {
                    spec.definitions[offset].storage_class = class;
                }
            }
        }
        // Go `checkReorgPartitionDefs`: unless the last partition is replaced,
        // the new last bound must equal the old one.
        if last != old.len() - 1 {
            let new_last = new_offset + definitions.len() - 1;
            let same_end = match (&partition.kind, &spec.kind) {
                (
                    PartitionKind::Range {
                        less_than,
                        unsigned,
                    },
                    PartitionKind::Range {
                        less_than: built, ..
                    },
                ) => match (less_than[last], built[new_last]) {
                    (
                        tidb_table::partition_routing::RangeBound::Value(old),
                        tidb_table::partition_routing::RangeBound::Value(new),
                    ) => {
                        if *unsigned {
                            old as u64 == new as u64
                        } else {
                            old == new
                        }
                    }
                    (old, new) => old == new,
                },
                (
                    PartitionKind::RangeColumns {
                        less_than,
                        field_types,
                    },
                    PartitionKind::RangeColumns {
                        less_than: built, ..
                    },
                ) => range_columns_bounds_equal(&less_than[last], &built[new_last], field_types)?,
                _ => true,
            };
            if !same_end {
                return Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_dbterror::ERR_RANGE_NOT_INCREASING.generate_with_stack_by_args(&[]),
                ));
            }
        }
        host.resolve_placement(&mut spec.definitions[new_offset..new_offset + definitions.len()])?;
        let source_ids = dropped.iter().map(|ordinal| old[*ordinal].id).collect();
        (spec, source_ids, new_offset, clause)
    };
    for definition in &mut spec.definitions[new_offset..new_offset + definitions.len()] {
        definition.id = host.allocate_table_id();
    }
    let stored = stored_with_ids(&clause, &spec, ctx)?;
    Ok(PartitionReorgPlan {
        source_ids,
        interval_rewrite: None,
        warning: Some(STATS_OUTDATED_RELATED),
        stored: Some(stored),
    })
}

/// Go `checkTwoRangeColumns` in both directions: neither bound is above the
/// other.
fn range_columns_bounds_equal(
    old: &[RangeColumnBound],
    new: &[RangeColumnBound],
    field_types: &[FieldType],
) -> Result<bool, MysqlError> {
    for ((old, new), field_type) in old.iter().zip(new).zip(field_types) {
        match (old, new) {
            (RangeColumnBound::MaxValue, RangeColumnBound::MaxValue) => {}
            (RangeColumnBound::Value(old), RangeColumnBound::Value(new)) => {
                let order =
                    tidb_expr::compare_datums_with_collation(old, new, field_type.collation())
                        .map_err(|_| {
                            tidb_error::mysql::MysqlError::from_terror(
                                &tidb_dbterror::ERR_WRONG_TYPE_COLUMN_VALUE
                                    .generate_with_stack_by_args(&[]),
                            )
                        })?;
                if order != std::cmp::Ordering::Equal {
                    return Ok(false);
                }
            }
            _ => return Ok(false),
        }
    }
    Ok(true)
}

/// Go `AlterTablePartitioning`, planned against `table`: the table is
/// repartitioned as `CREATE TABLE ... PARTITION BY` would partition it, and
/// takes a new table id.
pub fn plan_alter_table_partitioning(
    table: &PartitionedTable<'_>,
    partitioning: &TablePartitioning,
    ctx: &tidb_expr::metabuild::Context,
    host: &mut dyn ReorgHost,
) -> Result<PartitionReorgPlan, MysqlError> {
    let (mut spec, source_ids) = {
        refuse_affinity(table, "ALTER TABLE PARTITIONING")?;
        let (names, types) = visible_names_and_types(table);
        let handle_offsets = handle_offsets(table);
        let mut spec = table_partition::build_partitioning(
            partitioning,
            &names,
            &types,
            &table.indexes(),
            &handle_offsets,
            &mut || 0,
            ctx,
        )?;
        // Go `checkAddPartitionOnTemporaryMode`, inside
        // `checkPartitionDefinitionConstraints`.
        if table.meta.temp_table_type != tidb_model::TempTableType::NONE {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_dbterror::ERR_PARTITION_NO_TEMPORARY.generate_with_stack_by_args(&[]),
            ));
        }
        if table
            .meta
            .indices
            .iter_deref()
            .any(|index| index.read().has_condition())
        {
            return Err(MysqlError::from_terror(
                &tidb_dbterror::ERR_UNSUPPORTED_ADD_PARTIAL_INDEX
                    .generate_with_stack_by_args(&[
                        "partial index is not supported on partitioned table".into(),
                    ]),
            )
            .into());
        }
        crate::storage_class::rebuild_storage_class_for_partitions(
            &table.meta.engine_attribute,
            &mut spec,
            ctx,
        )?;
        host.resolve_placement(&mut spec.definitions)?;
        // Go builds the reorganized table in the job's next state, and a KEY
        // method that resolved no column has no expression to build
        // (`extractPartitionExprColumns`).
        if matches!(spec.kind, PartitionKind::Key) && spec.dependencies.is_empty() {
            return Err(tidb_error::mysql::MysqlError::unknown(
                "expression should not be an empty string".to_owned(),
            ));
        }
        // Go `buildTablePartitionInfo`'s UPDATE INDEXES checks.
        table_partition::apply_update_indexes(partitioning, &table.indexes(), &handle_offsets)?;
        let source_ids = match table.partition() {
            Some(partition) => partition
                .definitions
                .iter()
                .map(|definition| definition.id)
                .collect(),
            None => vec![table.meta.id],
        };
        (spec, source_ids)
    };
    // Go's submitter allocates the partition ids (then the new table id).
    for definition in &mut spec.definitions {
        definition.id = host.allocate_table_id();
    }
    // Go `rewritePartitionQueryString`: INTERVAL sugar's job query records
    // the partitions it generated.
    let interval_rewrite = partitioning.method.interval.as_ref().map(|interval| {
        let definitions = crate::table_partition::append_partition_defs(
            &spec.definitions,
            &spec.kind,
            ctx.get_sql_mode().has_ansi_quotes_mode(),
        );
        (
            String::from_utf8_lossy(interval.original_text()).into_owned(),
            definitions.trim_start_matches('\n').to_owned(),
        )
    });
    let stored = stored_with_ids(partitioning, &spec, ctx)?;
    Ok(PartitionReorgPlan {
        source_ids,
        interval_rewrite,
        warning: Some(STATS_OUTDATED_NEW),
        stored: Some(stored),
    })
}

/// Go `RemovePartitioning`, planned against `table`.
pub fn plan_remove_partitioning(table: &PartitionedTable<'_>) -> Result<PartitionReorgPlan, MysqlError> {
    let Some(partition) = table.partition() else {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED
                .generate_with_stack_by_args(&[]),
        ));
    };
    refuse_affinity(table, "REMOVE PARTITIONING")?;
    let source_ids = partition
        .definitions
        .iter()
        .map(|definition| definition.id)
        .collect::<Vec<_>>();
    Ok(PartitionReorgPlan {
        source_ids,
        interval_rewrite: None,
        warning: None,
        stored: None,
    })
}
