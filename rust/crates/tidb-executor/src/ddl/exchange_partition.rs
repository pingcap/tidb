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

//! `ALTER TABLE pt EXCHANGE PARTITION p WITH TABLE nt` over the in-process
//! catalog: Go `ExchangeTablePartition` (`pkg/ddl/executor.go`) for the
//! checks, then the job's `onExchangeTablePartition`
//! (`pkg/ddl/partition.go`) for the placement check, the row validation and
//! the physical-id swap.

use super::{Catalog, DriverError};
use crate::{KvTable, TableEntry};
use tidb_datatype::FieldTypeFlags;
use tidb_hack::GoToLower;

fn coded(errno: u16, message: impl Into<String>) -> DriverError {
    DriverError::DdlCoded {
        errno,
        message: message.into(),
    }
}

/// Go `ErrTablesDifferentMetadata` (1736).
fn different_metadata() -> DriverError {
    coded(1736, "Tables have different definitions")
}

/// Go `ErrPartitionExchangeDifferentOption` (1731).
fn different_option(attribute: String) -> DriverError {
    coded(
        1731,
        format!("Non matching attribute '{attribute}' between partition and table"),
    )
}

/// Go `ErrPartitionExchangeTempTable` (1733).
fn temporary_table(name: &str) -> DriverError {
    coded(
        1733,
        format!("Table to exchange with partition is temporary: '{name}'"),
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exchange_partition_action(
    catalog: &mut Catalog,
    database: &str,
    table_name: &str,
    partition: &str,
    standalone: &[String],
    with_validation: bool,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let (standalone_db, standalone_name) =
        crate::driver::split_table_path_pub(standalone, current_db)?;
    let (standalone_db, standalone_name) = (standalone_db.to_owned(), standalone_name.to_owned());
    // Go checks the session's infoschema for a LOCAL temporary table first,
    // because only the session holds one.
    if let Some(TableEntry::Kv(table)) = catalog.table_in(&standalone_db, &standalone_name) {
        if table.temp_table_type() == tidb_model::TempTableType::LOCAL {
            return Err(temporary_table(&table.name));
        }
    }
    let Some(TableEntry::Kv(partitioned)) = catalog.table_in(database, table_name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{table_name}"),
        )));
    };
    let Some(standalone_entry) = catalog.persistent_table_in(&standalone_db, &standalone_name)
    else {
        if !catalog.has_database(&standalone_db) {
            return Err(DriverError::Schema(
                crate::SchemaErrorKind::UnknownDatabase(standalone_db),
            ));
        }
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{standalone_db}.{standalone_name}"),
        )));
    };
    // Go `checkExchangePartition`.
    let standalone = match standalone_entry {
        TableEntry::Kv(table) => table,
        TableEntry::View(_) | TableEntry::Sequence(_) | TableEntry::Mem(_) => {
            return Err(coded(1177, "Can't open table"));
        }
    };
    let Some(partition_info) = partitioned.partition() else {
        return Err(DriverError::PartitionManagementOnNonpartitioned);
    };
    if standalone.partition().is_some() {
        return Err(coded(
            1732,
            format!(
                "Table to exchange with partition is partitioned: '{}'",
                standalone.name
            ),
        ));
    }
    if standalone.has_affinity() || partitioned.has_affinity() {
        return Err(coded(
            8200,
            "Unsupported DDL operation: EXCHANGE PARTITION of a table with AFFINITY option",
        ));
    }
    if !standalone.foreign_keys().is_empty() {
        return Err(coded(
            1740,
            format!(
                "Table to exchange with partition has foreign key references: '{}'",
                standalone.name
            ),
        ));
    }
    // Go `tables.FindPartitionByName` names the folded partition.
    let partition_name = partition.go_to_lower();
    let Some(ordinal) = partition_info
        .definitions
        .iter()
        .position(|definition| definition.name.go_to_lower() == partition_name)
    else {
        return Err(DriverError::UnknownPartition {
            partition: partition_name,
            table: partitioned.name.clone(),
        });
    };
    check_table_def_compatible(partitioned, standalone)?;
    check_placement_policy(
        standalone.placement_policy(),
        partition_info.definitions[ordinal]
            .placement_policy
            .as_ref()
            .or(partitioned.placement_policy()),
    )?;

    let partitioned_db = database.to_owned();
    let partitioned_name = table_name.to_owned();
    let mut partitioned = partitioned.as_ref().clone();
    let mut standalone = standalone.as_ref().clone();
    if with_validation {
        validate_records(&mut partitioned, ordinal, &mut standalone, ctx)?;
    }
    super::bdr::admit(
        catalog,
        ctx.ddl_cdc_write_source(),
        &standalone_db,
        &[super::bdr::SubmittedJob::new(
            tidb_model::ActionType::ACTION_EXCHANGE_TABLE_PARTITION,
        )],
    )?;
    partitioned
        .exchange_partition(ordinal, &mut standalone, ctx)
        .map_err(|error| crate::driver::kv_read_error("exchange partition", error))?;
    partitioned
        .exchange_auto_ids(&standalone)
        .map_err(|error| DriverError::AutoIdUnavailable(error.0))?;
    *catalog
        .table_mut_in(&partitioned_db, &partitioned_name)
        .expect("the partitioned table was resolved above") =
        TableEntry::Kv(std::sync::Arc::new(partitioned));
    *catalog
        .table_mut_in(&standalone_db, &standalone_name)
        .expect("the exchanged table was resolved above") =
        TableEntry::Kv(std::sync::Arc::new(standalone));
    ctx.append_warning_parts(
        tidb_error::mysql::errcode::ErrUnknown,
        "after the exchange, please analyze related table of the exchange to update statistics",
    );
    Ok(())
}

/// Go `checkTableDefCompatible`: the two tables must agree on every option
/// and column the exchanged rows are stored under, and on the ids their
/// keys are encoded with.
fn check_table_def_compatible(source: &KvTable, target: &KvTable) -> Result<(), DriverError> {
    if target.temp_table_type() != tidb_model::TempTableType::NONE {
        return Err(temporary_table(&target.name));
    }
    let random_bits = |table: &KvTable| {
        table
            .auto_random()
            .map_or((0, 0), |spec| (spec.shard_bits, spec.range_bits))
    };
    let replica = |table: &KvTable| {
        table.tiflash_replica().map(|replica| {
            (
                replica.count,
                replica.available,
                replica.location_labels.iter().cloned().collect::<Vec<_>>(),
            )
        })
    };
    if random_bits(source) != random_bits(target)
        || source.charset() != target.charset()
        || source.shard_row_id_bits() != target.shard_row_id_bits()
        || source.pk_handle_offset().is_some() != target.pk_handle_offset().is_some()
        || source.common_handle_offsets().is_empty() != target.common_handle_offsets().is_empty()
        || replica(source) != replica(target)
    {
        return Err(different_metadata());
    }
    if source.columns().len() != target.columns().len() {
        return Err(different_metadata());
    }
    for (offset, (source_column, target_column)) in
        source.columns().iter().zip(target.columns()).enumerate()
    {
        let virtual_generated = |column: &crate::KvColumn| {
            column
                .generated
                .as_ref()
                .is_some_and(|generated| !generated.stored)
        };
        if virtual_generated(source_column) != virtual_generated(target_column) {
            return Err(coded(
                3106,
                "'Exchanging partitions for non-generated columns' is not supported for generated columns.",
            ));
        }
        let expression = |column: &crate::KvColumn| {
            column
                .generated
                .as_ref()
                .map(|generated| generated.expr_text.clone())
        };
        if source_column.name.go_to_lower() != target_column.name.go_to_lower()
            || source.is_hidden(offset) != target.is_hidden(offset)
            || !field_type_compatible(&source_column.field_type, &target_column.field_type)
            || expression(source_column) != expression(target_column)
        {
            return Err(different_metadata());
        }
        if source_column.id != target_column.id {
            return Err(different_option(format!("column: {}", source_column.name)));
        }
    }
    if source.indexes().len() != target.indexes().len() {
        return Err(different_metadata());
    }
    for source_index in source.indexes() {
        if source_index.global {
            return Err(different_option(format!(
                "global index: {}",
                source_index.name
            )));
        }
        let Some(target_index) = target
            .indexes()
            .iter()
            .rev()
            .find(|index| index.name.eq_ignore_ascii_case(&source_index.name))
        else {
            return Err(different_metadata());
        };
        let primary = |name: &str| name.eq_ignore_ascii_case("PRIMARY");
        if source_index.unique != target_index.unique
            || primary(&source_index.name) != primary(&target_index.name)
            || source_index.column_offsets.len() != target_index.column_offsets.len()
        {
            return Err(different_metadata());
        }
        for position in 0..source_index.column_offsets.len() {
            let name = |table: &KvTable, index: &crate::KvIndex| {
                table.columns()[index.column_offsets[position]]
                    .name
                    .go_to_lower()
            };
            if source_index.prefix_lengths[position] != target_index.prefix_lengths[position]
                || name(source, source_index) != name(target, target_index)
            {
                return Err(different_metadata());
            }
        }
        if source_index.id != target_index.id {
            return Err(different_option(format!("index: {}", source_index.name)));
        }
    }
    Ok(())
}

/// Go `checkFieldTypeCompatible`: a fixed-length type ignores its display
/// width (`int(1)` matches `int(8)`).
fn field_type_compatible(
    left: &tidb_datatype::FieldType,
    right: &tidb_datatype::FieldType,
) -> bool {
    let flags = [
        FieldTypeFlags::UNSIGNED,
        FieldTypeFlags::AUTO_INCREMENT,
        FieldTypeFlags::NOT_NULL,
        FieldTypeFlags::ZEROFILL,
        FieldTypeFlags::BINARY,
        FieldTypeFlags::PRI_KEY,
    ];
    left.code() == right.code()
        && left.decimal() == right.decimal()
        && left.charset_name() == right.charset_name()
        && left.collation_name() == right.collation_name()
        && (left.flen() == right.flen() || left.storage_length() != tidb_datatype::VAR_STORAGE_LEN)
        && flags
            .iter()
            .all(|flag| left.has_flag(*flag) == right.has_flag(*flag))
        && left.elems_snapshot() == right.elems_snapshot()
}

/// Go `checkExchangePartitionPlacementPolicy`: the partition's policy (or
/// the partitioned table's) and the standalone table's must name the same
/// policy.
fn check_placement_policy(
    standalone: Option<&tidb_model::PolicyRefInfo>,
    partition: Option<&tidb_model::PolicyRefInfo>,
) -> Result<(), DriverError> {
    match (standalone, partition) {
        (None, None) => Ok(()),
        (Some(left), Some(right)) if left.name.lowercase() == right.name.lowercase() => Ok(()),
        _ => Err(different_metadata()),
    }
}

/// Go `checkExchangePartitionRecordValidation`: every row of the table
/// exchanged in must belong to the partition and satisfy the partitioned
/// table's CHECK constraints, and every row of the partition must satisfy
/// the standalone table's.
fn validate_records(
    partitioned: &mut KvTable,
    ordinal: usize,
    standalone: &mut KvTable,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let mismatch = || coded(1737, "Found a row that does not match the partition");
    let read_error = |error| crate::driver::kv_read_error("exchange partition", error);
    let kind = &partitioned
        .partition()
        .expect("checked partitioned above")
        .kind;
    if matches!(kind, crate::partition_routing::PartitionKind::Key) {
        return Err(coded(
            8200,
            format!(
                "Unsupported partition type of table {} when exchanging partition",
                partitioned.name
            ),
        ));
    }
    let check_constraints = ctx.enable_check_constraint();
    let standalone_id = standalone.table_id;
    for row in standalone
        .physical_rows(standalone_id, ctx)
        .map_err(read_error)?
    {
        if partitioned
            .exchange_row_mismatches(ordinal, &row, ctx)
            .map_err(read_error)?
        {
            return Err(mismatch());
        }
        if check_constraints {
            violates(partitioned.validate_check_constraints(&row, ctx), mismatch)?;
        }
    }
    if check_constraints {
        let partition_id = partitioned.partition().expect("checked above").definitions[ordinal].id;
        for row in partitioned
            .physical_rows(partition_id, ctx)
            .map_err(read_error)?
        {
            violates(standalone.validate_check_constraints(&row, ctx), mismatch)?;
        }
    }
    Ok(())
}

/// A CHECK constraint that evaluates false is a mismatched row.
fn violates(
    checked: Result<(), crate::kv_table::KvTableError>,
    mismatch: impl Fn() -> DriverError,
) -> Result<(), DriverError> {
    match checked {
        Ok(()) => Ok(()),
        Err(crate::kv_table::KvTableError::CheckConstraintViolated(_)) => Err(mismatch()),
        Err(error) => Err(crate::driver::kv_read_error("exchange partition", error)),
    }
}
