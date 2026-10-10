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

//! Go's partition reorganizations for the in-process owner: `REORGANIZE
//! PARTITION` (`ReorganizePartitions`), `ALTER TABLE ... PARTITION BY`
//! (`AlterTablePartitioning`) and `REMOVE PARTITIONING`
//! (`RemovePartitioning`), each checked as the Go executor and the job's
//! first state check it, then applied as `onReorganizePartition` leaves the
//! table once the job is done.

use tidb_ast::{PartitionDefinition, PartitionMethod, PartitionType, TablePartitioning};
use tidb_datatype::FieldType;

use super::alter_table::refuse_affinity;
use super::table_partition::{self, StoredPartitionDefinition};
use super::{Catalog, DriverError};
use crate::kv_table::{PartitionReorg, RecreatedIndex};
use crate::partition_routing::{PartitionDef, PartitionKind, PartitionSpec, RangeColumnBound};

const STATS_OUTDATED_RELATED: &str = "The statistics of related partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now";
const STATS_OUTDATED_NEW: &str = "The statistics of new partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now";

fn kv_table<'a>(
    catalog: &'a Catalog,
    database: &str,
    table_name: &str,
) -> Result<&'a crate::KvTable, DriverError> {
    match catalog.table_in(database, table_name) {
        Some(crate::TableEntry::Kv(table)) => Ok(table),
        _ => Err(DriverError::unsupported(
            "ALTER TABLE ... PARTITION needs a storage-backed table",
        )),
    }
}

fn visible_names_and_types(table: &crate::KvTable) -> (Vec<String>, Vec<FieldType>) {
    table
        .visible_columns()
        .iter()
        .map(|column| (column.name.clone(), column.field_type.clone()))
        .unzip()
}

fn handle_offsets(table: &crate::KvTable) -> Vec<usize> {
    match table.pk_handle_offset() {
        Some(offset) => vec![offset],
        None => table.common_handle_offsets().to_vec(),
    }
}

/// Go `handlePartitionPlacement`: every policy a new definition names must
/// exist, and the reference records its id.
fn resolve_placement(
    catalog: &Catalog,
    definitions: &mut [PartitionDef],
) -> Result<(), DriverError> {
    for definition in definitions {
        if let Some(reference) = definition.placement_policy.as_mut() {
            let Some(found) = catalog.policy(reference.name.original()) else {
                return Err(DriverError::PlacementPolicyNotExists(
                    reference.name.original().to_owned(),
                ));
            };
            reference.id = found.id;
        }
    }
    Ok(())
}

/// Go `AllocateIndexID` + `setGlobalIndexVersion` for every index
/// `onReorganizePartition` duplicates: the ones global before or after the
/// change, in index order.
fn recreated_indexes(
    table: &crate::KvTable,
    global_after: impl Fn(&crate::KvIndex) -> bool,
) -> Vec<RecreatedIndex> {
    let clustered = table.pk_handle_offset().is_some() || !table.common_handle_offsets().is_empty();
    let mut next_id = table.next_index_id();
    let mut recreated = Vec::new();
    for index in table.indexes() {
        let global = global_after(index);
        if !index.global && !global {
            continue;
        }
        let part_types = index
            .column_offsets
            .iter()
            .map(|offset| table.columns[*offset].field_type.clone())
            .collect::<Vec<_>>();
        recreated.push(RecreatedIndex {
            old_id: index.id,
            new_id: next_id,
            global,
            global_index_version: super::indexes::global_index_version(
                global,
                clustered,
                index.unique,
                &part_types,
            ),
        });
        next_id += 1;
    }
    recreated
}

fn apply_reorg(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    reorg: PartitionReorg,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in(database, table_name) else {
        unreachable!("the table was resolved above")
    };
    std::sync::Arc::make_mut(table)
        .reorganize_partitions(reorg, ctx)
        .map_err(crate::driver::kv_write_error)
}

/// The method of an existing partitioned table, as the clause that would
/// have created it: what Go's `BuildAddedPartitionInfo` copies into the new
/// `PartitionInfo` (`Type`, `Expr`, `Columns`).
fn existing_method(partition: &PartitionSpec) -> Result<PartitionMethod, DriverError> {
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
            .map_err(|error| DriverError::Parse(error.message))
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
) -> Result<(usize, usize, Vec<usize>), DriverError> {
    let mut ordinals: Vec<usize> = Vec::with_capacity(names.len());
    for name in names {
        let Some(ordinal) = partition
            .definitions
            .iter()
            .position(|definition| table_partition::partition_names_equal(&definition.name, name))
        else {
            return Err(super::preprocess::wrong_partition_name());
        };
        if ordinals.contains(&ordinal) {
            // Go returns the bare error, so the name slot stays unformatted.
            return Err(DriverError::DdlCoded {
                errno: tidb_error::mysql::errcode::ErrSameNamePartition,
                message: "Duplicate partition name %-.192s".to_owned(),
            });
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
        return Err(DriverError::DdlCoded {
            errno: 8200,
            message: "Unsupported REORGANIZE PARTITION of RANGE; not adjacent partitions"
                .to_owned(),
        });
    }
    ordinals.sort_unstable();
    Ok((first, last, ordinals))
}

/// Go `ReorganizePartitions` and the `checkReorgPartitionDefs` battery,
/// then the job.
pub(super) fn reorganize_partition_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    names: &[String],
    definitions: &[PartitionDefinition],
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let (mut spec, source_ids, recreated, new_offset) = {
        let table = kv_table(catalog, database, table_name)?;
        let Some(partition) = table.partition() else {
            return Err(DriverError::PartitionManagementOnNonpartitioned);
        };
        refuse_affinity(table, "REORGANIZE PARTITION")?;
        if !matches!(
            partition.kind,
            PartitionKind::Range { .. }
                | PartitionKind::RangeColumns { .. }
                | PartitionKind::List { .. }
                | PartitionKind::ListColumns { .. }
        ) {
            return Err(DriverError::DdlCoded {
                errno: 8200,
                message: "Unsupported reorganize partition".to_owned(),
            });
        }
        let (_, last, dropped) = replaced_partitions(partition, names)?;
        // Go `BuildAddedPartitionInfo`: RANGE and LIST need written
        // definitions.
        if definitions.is_empty() {
            return Err(DriverError::PartitionsMustBeDefined(partition.kind.sql()));
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
                        table_partition::stored_definitions_as_ast(&[stored_definition(
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
            .collect::<Result<Vec<_>, DriverError>>()?;
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
            table.indexes(),
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
            super::storage_class::partition_storage_classes(table.engine_attribute(), &spec, ctx)?
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
                        crate::partition_routing::RangeBound::Value(old),
                        crate::partition_routing::RangeBound::Value(new),
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
                return Err(DriverError::PartitionRangeNotIncreasing);
            }
        }
        resolve_placement(
            catalog,
            &mut spec.definitions[new_offset..new_offset + definitions.len()],
        )?;
        let source_ids = dropped.iter().map(|ordinal| old[*ordinal].id).collect();
        let recreated = recreated_indexes(table, |index| index.global);
        (spec, source_ids, recreated, new_offset)
    };
    for definition in &mut spec.definitions[new_offset..new_offset + definitions.len()] {
        definition.id = catalog.allocate_table_id();
    }
    // Go's reorganized table: the same method over the new definitions only.
    let routing = {
        let table = kv_table(catalog, database, table_name)?;
        let (names, types) = visible_names_and_types(table);
        let partition = table.partition().expect("checked above");
        let kind = match partition.kind {
            PartitionKind::Range { .. } | PartitionKind::RangeColumns { .. } => {
                PartitionType::RANGE
            }
            _ => PartitionType::LIST,
        };
        let columns = match partition.kind {
            PartitionKind::RangeColumns { .. } | PartitionKind::ListColumns { .. } => {
                partition.dependencies.clone()
            }
            _ => Vec::new(),
        };
        let expr_text = if columns.is_empty() {
            partition.expr_text.clone()
        } else {
            String::new()
        };
        let new_definitions = spec.definitions[new_offset..new_offset + definitions.len()]
            .iter()
            .map(stored_definition)
            .collect::<Vec<_>>();
        table_partition::partition_spec_from_metadata(
            kind,
            &expr_text,
            &columns,
            false,
            &new_definitions,
            &[],
            &names,
            &types,
        )?
    };
    apply_reorg(
        catalog,
        database,
        table_name,
        PartitionReorg {
            source_ids,
            partition: Some(spec),
            routing: Some(routing),
            new_table_id: None,
            recreated_indexes: recreated,
        },
        ctx,
    )?;
    ctx.append_warning_parts(1105, STATS_OUTDATED_RELATED);
    Ok(())
}

/// Go `checkTwoRangeColumns` in both directions: neither bound is above the
/// other.
fn range_columns_bounds_equal(
    old: &[RangeColumnBound],
    new: &[RangeColumnBound],
    field_types: &[FieldType],
) -> Result<bool, DriverError> {
    for ((old, new), field_type) in old.iter().zip(new).zip(field_types) {
        match (old, new) {
            (RangeColumnBound::MaxValue, RangeColumnBound::MaxValue) => {}
            (RangeColumnBound::Value(old), RangeColumnBound::Value(new)) => {
                let order =
                    tidb_expr::compare_datums_with_collation(old, new, field_type.collation())
                        .map_err(|_| DriverError::PartitionColumnValueWrongType)?;
                if order != std::cmp::Ordering::Equal {
                    return Ok(false);
                }
            }
            _ => return Ok(false),
        }
    }
    Ok(true)
}

/// Go `AlterTablePartitioning`, then the job: the table is repartitioned as
/// `CREATE TABLE ... PARTITION BY` would partition it, and takes a new
/// table id.
pub(super) fn alter_table_partitioning_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    partitioning: &TablePartitioning,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let (mut spec, source_ids, recreated) = {
        let table = kv_table(catalog, database, table_name)?;
        refuse_affinity(table, "ALTER TABLE PARTITIONING")?;
        let (names, types) = visible_names_and_types(table);
        let handle_offsets = handle_offsets(table);
        let mut spec = table_partition::build_partitioning(
            partitioning,
            &names,
            &types,
            table.indexes(),
            &handle_offsets,
            &mut || 0,
            ctx,
        )?;
        // Go `checkAddPartitionOnTemporaryMode`, inside
        // `checkPartitionDefinitionConstraints`.
        if table.temp_table_type() != tidb_model::TempTableType::NONE {
            return Err(DriverError::PartitionNoTemporary);
        }
        if table
            .indexes()
            .iter()
            .any(|index| table.partial_index_condition_string(index.id).is_some())
        {
            return Err(super::indexes::unsupported_partial_index(
                "partial index is not supported on partitioned table",
            ));
        }
        super::storage_class::rebuild_storage_class_for_partitions(
            table.engine_attribute(),
            &mut spec,
            ctx,
        )?;
        resolve_placement(catalog, &mut spec.definitions)?;
        // Go builds the reorganized table in the job's next state, and a KEY
        // method that resolved no column has no expression to build
        // (`extractPartitionExprColumns`).
        if matches!(spec.kind, PartitionKind::Key) && spec.dependencies.is_empty() {
            return Err(DriverError::DdlCoded {
                errno: tidb_error::mysql::errcode::ErrUnknown,
                message: "expression should not be an empty string".to_owned(),
            });
        }
        let updated =
            table_partition::apply_update_indexes(partitioning, table.indexes(), &handle_offsets)?;
        let recreated = recreated_indexes(table, |index| {
            updated
                .iter()
                .find(|candidate| candidate.id == index.id)
                .is_some_and(|candidate| candidate.global)
        });
        let source_ids = match table.partition() {
            Some(partition) => partition
                .definitions
                .iter()
                .map(|definition| definition.id)
                .collect(),
            None => vec![table.table_id],
        };
        (spec, source_ids, recreated)
    };
    // Go's submitter allocates the partition ids first, then NewTableID.
    for definition in &mut spec.definitions {
        definition.id = catalog.allocate_table_id();
    }
    let new_table_id = catalog.allocate_table_id();
    apply_reorg(
        catalog,
        database,
        table_name,
        PartitionReorg {
            source_ids,
            partition: Some(spec),
            routing: None,
            new_table_id: Some(new_table_id),
            recreated_indexes: recreated,
        },
        ctx,
    )?;
    ctx.append_warning_parts(1105, STATS_OUTDATED_NEW);
    Ok(())
}

/// Go `RemovePartitioning`, then the job: every row moves into the one
/// `CollapsedPartitions` definition, whose id becomes the table's, and every
/// index becomes local.
pub(super) fn remove_partitioning_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let (source_ids, recreated) = {
        let table = kv_table(catalog, database, table_name)?;
        let Some(partition) = table.partition() else {
            return Err(DriverError::PartitionManagementOnNonpartitioned);
        };
        refuse_affinity(table, "REMOVE PARTITIONING")?;
        let source_ids = partition
            .definitions
            .iter()
            .map(|definition| definition.id)
            .collect::<Vec<_>>();
        (source_ids, recreated_indexes(table, |_| false))
    };
    let new_table_id = catalog.allocate_table_id();
    apply_reorg(
        catalog,
        database,
        table_name,
        PartitionReorg {
            source_ids,
            partition: None,
            routing: None,
            new_table_id: Some(new_table_id),
            recreated_indexes: recreated,
        },
        ctx,
    )
}
