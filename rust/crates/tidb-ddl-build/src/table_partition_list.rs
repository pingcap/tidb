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

//! Scalar `PARTITION BY LIST (expr)` definition folding and validation.
//!
//! Scalar LIST folds one integer expression, while LIST COLUMNS compares typed
//! tuples. Both definition forms are normalized here before runtime routing.

use std::collections::HashMap;

use tidb_ast::{Expr, PartitionDefinition, PartitionDefinitionClause, PartitionValue};
use tidb_datatype::FieldType;

use tidb_table::partition_routing::PartitionKind;
use tidb_terror::MysqlError;

use tidb_table::partition_fold::fold_column_value;

use tidb_hack::GoCaseCompare;

/// Builds `LIST COLUMNS`' typed tuple key set.
pub fn build_list_columns_values(
    columns: &[Vec<String>],
    definitions: &[PartitionDefinition],
    names: &[String],
    types: &[FieldType],
    ctx: &tidb_expr::metabuild::Context,
    definition_tail: &mut dyn FnMut(usize) -> Result<(), MysqlError>,
) -> Result<(Vec<String>, PartitionKind), MysqlError> {
    if columns.is_empty() || definitions.is_empty() {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_error::catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED
                .generate_with_stack_by_args(&["LIST".into()]),
        ));
    }
    let mut dependency_names = Vec::with_capacity(columns.len());
    let mut field_types = Vec::with_capacity(columns.len());
    for path in columns {
        let name = path.last().ok_or_else(|| {
            tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_WRONG_TYPE_COLUMN_VALUE.generate_with_stack_by_args(&[]),
            )
        })?;
        // Go `checkPartitionColumnsUnique`, from
        // `checkPartitionDefinitionConstraints`.
        if dependency_names
                .iter()
                .any(|candidate: &String| candidate.go_lower_eq(name))
        {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_SAME_NAME_PARTITION_FIELD
                    .generate_with_stack_by_args(&[(name.clone()).to_string().into()]),
            ));
        }
        let offset = names
            .iter()
            .position(|candidate| candidate.go_lower_eq(name))
            // Go `checkColumnsPartitionType` (`partition.go:787`) reports a
            // column list's missing name as `ErrFieldNotFoundPart` (1488),
            // not the 1054 the expression path raises.
            .ok_or_else(|| {
                tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_FIELD_NOT_FOUND_PART.generate_with_stack_by_args(&[]),
                )
            })?;
        let field_type = types[offset].clone();
        // Go `checkColumnsPartitionType` (`partition.go:642`).
        if !list_columns_type_allowed(&field_type) {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_NOT_ALLOWED_TYPE_IN_PARTITION
                    .generate_with_stack_by_args(&[(name.clone()).to_string().into()]),
            ));
        }
        dependency_names.push(name.clone());
        field_types.push(field_type);
    }

    let mut values = Vec::new();
    let mut keys = HashMap::new();
    let mut default_partition = None;
    for (ordinal, definition) in definitions.iter().enumerate() {
        match &definition.clause {
            PartitionDefinitionClause::Default => {
                set_default_partition(&mut default_partition, ordinal)?;
            }
            PartitionDefinitionClause::In(items) => {
                for item in items {
                    if matches!(item, PartitionValue::Default) {
                        set_default_partition(&mut default_partition, ordinal)?;
                        continue;
                    }
                    let exprs: &[Expr] = match item {
                        PartitionValue::Expr(expr) if field_types.len() == 1 => {
                            std::slice::from_ref(expr)
                        }
                        PartitionValue::Tuple(exprs) if exprs.len() == field_types.len() => exprs,
                        // Go's DDL validator owns tuple-shape errors and
                        // reports ErrPartitionColumnList (1653), including
                        // the scalar value supplied for a multi-column LIST
                        // COLUMNS key.
                        _ => {
                            return Err(tidb_terror::MysqlError::from_terror(
                                &tidb_error::catalog::ast::ERR_PARTITION_COLUMN_LIST
                                    .generate_with_stack_by_args(&[]),
                            ))
                        }
                    };
                    let tuple = exprs
                        .iter()
                        .zip(&field_types)
                        .map(|(expr, field_type)| fold_column_value(expr, field_type, ctx))
                        .collect::<Result<Vec<_>, _>>()?;
                    let key = tidb_codec::encode_key_in_timezone(
                        &ctx.get_expr_ctx().get_eval_ctx().time_zone(),
                        &tuple,
                    )
                    .map_err(|_| {
                        tidb_terror::MysqlError::from_terror(
                            &tidb_dbterror::ERR_WRONG_TYPE_COLUMN_VALUE
                                .generate_with_stack_by_args(&[]),
                        )
                    })?;
                    // Go `checkListPartitionValue`.
                    if keys.insert(key, ordinal).is_some() {
                        return Err(tidb_terror::MysqlError::from_terror(
                            &tidb_dbterror::ERR_MULTIPLE_DEF_CONST_IN_LIST_PART
                                .generate_with_stack_by_args(&[]),
                        ));
                    }
                    values.push((tuple, ordinal));
                }
            }
            PartitionDefinitionClause::LessThan(_) => {
                return Err(tidb_terror::MysqlError::from_terror(
                    &tidb_error::catalog::ast::ERR_PARTITION_WRONG_VALUES
                        .generate_with_stack_by_args(&["RANGE".into(), "LESS THAN".into()]),
                ));
            }
            _ => {
                return Err(tidb_terror::MysqlError::from_terror(
                    &tidb_error::catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED
                        .generate_with_stack_by_args(&["LIST".into()]),
                ))
            }
        }
        definition_tail(ordinal)?;
    }
    Ok((
        dependency_names,
        PartitionKind::ListColumns {
            values,
            keys,
            default_partition,
            field_types,
        },
    ))
}

fn set_default_partition(
    slot: &mut Option<usize>,
    ordinal: usize,
) -> Result<(), MysqlError> {
    // A second DEFAULT is `ErrMultipleDefConstInListPart` from
    // `formatListPartitionValue` (`ddl/partition.go:2010`), which
    // `checkListPartitionValue` reaches.
    if slot.replace(ordinal).is_some() {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_MULTIPLE_DEF_CONST_IN_LIST_PART
                .generate_with_stack_by_args(&[]),
        ));
    }
    Ok(())
}

pub fn list_columns_type_allowed(field_type: &FieldType) -> bool {
    use tidb_datatype::FieldTypeCode;
    matches!(
        field_type.code(),
        FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Date
            | FieldTypeCode::Datetime
            | FieldTypeCode::Duration
            | FieldTypeCode::Varchar
            | FieldTypeCode::String
    )
}
