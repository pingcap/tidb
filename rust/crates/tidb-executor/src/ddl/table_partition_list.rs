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

use std::collections::{HashMap, HashSet};

use tidb_ast::{Expr, PartitionDefinition, PartitionDefinitionClause, PartitionValue};
use tidb_datatype::{Datum, FieldType};

use crate::partition_routing::PartitionKind;
use crate::DriverError;

/// Builds `LIST COLUMNS`' typed tuple key set.
pub(super) fn build_list_columns_values(
    columns: &[Vec<String>],
    definitions: &[PartitionDefinition],
    names: &[String],
    types: &[FieldType],
    ctx: &crate::StmtContext,
    mode: super::table_partition::PartitionBuildMode,
    definition_tail: &mut dyn FnMut(usize) -> Result<(), DriverError>,
) -> Result<(Vec<String>, PartitionKind), DriverError> {
    if columns.is_empty() || definitions.is_empty() {
        return Err(DriverError::PartitionsMustBeDefined("LIST"));
    }
    let mut dependency_names = Vec::with_capacity(columns.len());
    let mut field_types = Vec::with_capacity(columns.len());
    for path in columns {
        let name = path
            .last()
            .ok_or(DriverError::PartitionColumnValueWrongType)?;
        // Go `checkPartitionColumnsUnique` runs from
        // `checkPartitionDefinitionConstraints`, i.e. CREATE only -- the
        // RANGE COLUMNS twin in this module is gated the same way.
        if mode.validates()
            && dependency_names
                .iter()
                .any(|candidate: &String| candidate.eq_ignore_ascii_case(name))
        {
            return Err(DriverError::PartitionDuplicateField(name.clone()));
        }
        let offset = names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(name))
            // Go `checkColumnsPartitionType` (`partition.go:787`) reports a
            // column list's missing name as `ErrFieldNotFoundPart` (1488),
            // not the 1054 the expression path raises.
            .ok_or(DriverError::PartitionFieldNotFound)?;
        let field_type = types[offset].clone();
        // Go `checkColumnsPartitionType` is CREATE-only (`partition.go:642`).
        if mode.validates() && !list_columns_type_allowed(&field_type) {
            return Err(DriverError::PartitionFieldTypeNotAllowed(name.clone()));
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
                set_default_partition(&mut default_partition, ordinal, mode)?;
            }
            PartitionDefinitionClause::In(items) => {
                for item in items {
                    if matches!(item, PartitionValue::Default) {
                        set_default_partition(&mut default_partition, ordinal, mode)?;
                        continue;
                    }
                    let exprs: &[Expr] = match item {
                        PartitionValue::Expr(expr) if field_types.len() == 1 => {
                            std::slice::from_ref(expr)
                        }
                        PartitionValue::Tuple(exprs) if exprs.len() == field_types.len() => exprs,
                        _ => return Err(DriverError::PartitionColumnValueWrongType),
                    };
                    let tuple = exprs
                        .iter()
                        .zip(&field_types)
                        .map(|(expr, field_type)| fold_column_value(expr, field_type, ctx))
                        .collect::<Result<Vec<_>, _>>()?;
                    let key = tidb_codec::encode_key_in_timezone(&ctx.session_zone(), &tuple)
                        .map_err(|_| DriverError::PartitionColumnValueWrongType)?;
                    // Go's loader fills a map and lets the LAST definition
                    // own a repeated tuple; only `checkListPartitionValue`,
                    // on the CREATE path, refuses it.
                    if keys.insert(key, ordinal).is_some() && mode.validates() {
                        return Err(DriverError::PartitionDuplicateListValue);
                    }
                    values.push((tuple, ordinal));
                }
            }
            PartitionDefinitionClause::LessThan(_) => {
                return Err(DriverError::PartitionWrongValues {
                    method: "RANGE",
                    clause: "VALUES LESS THAN",
                });
            }
            _ => return Err(DriverError::PartitionsMustBeDefined("LIST")),
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
    mode: super::table_partition::PartitionBuildMode,
) -> Result<(), DriverError> {
    // A second DEFAULT is `ErrMultipleDefConstInListPart` from
    // `formatListPartitionValue` (`ddl/partition.go:2010`), which only
    // `checkListPartitionValue` -- the CREATE path -- reaches. The loader
    // takes the last one, as its map would.
    if slot.replace(ordinal).is_some() && mode.validates() {
        return Err(DriverError::PartitionDuplicateListValue);
    }
    Ok(())
}

pub(super) fn fold_column_value(
    expr: &Expr,
    field_type: &FieldType,
    ctx: &crate::StmtContext,
) -> Result<Datum, DriverError> {
    let rewritten = tidb_expr::rewriter::rewrite_expr_resolved(
        expr,
        &tidb_expr::rewriter::ZonedNoResolver::with_like_default_escape(
            ctx.session_zone(),
            ctx.like_default_escape(),
        ),
    )
    .map_err(|_| DriverError::PartitionColumnValueWrongType)?;
    let mut dual = tidb_chunk::chunk::Chunk::new_empty(&[]);
    dual.set_num_virtual_rows(1);
    let value = rewritten
        .eval(ctx, dual.get_row(0))
        .map_err(|_| DriverError::PartitionColumnValueWrongType)?;
    let converted = value
        .convert_to_in(
            field_type,
            ctx.ddl_default_conversion_flags(),
            &ctx.session_zone(),
        )
        .map_err(|_| DriverError::PartitionColumnValueWrongType)?;
    if converted.event.is_some() {
        return Err(DriverError::PartitionColumnValueWrongType);
    }
    Ok(converted.value)
}

pub(super) fn list_columns_type_allowed(field_type: &FieldType) -> bool {
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

/// Folds scalar LIST definitions into exact-value routing metadata.
pub(super) fn build_list_values(
    built: &tidb_expr::expression::Expression,
    definitions: &[PartitionDefinition],
    ctx: &crate::StmtContext,
    definition_tail: &mut dyn FnMut(usize) -> Result<(), DriverError>,
) -> Result<(PartitionKind, Option<DriverError>), DriverError> {
    if definitions.is_empty() {
        return Err(DriverError::PartitionsMustBeDefined("LIST"));
    }
    let unsigned = super::table_partition_range::partition_expression_is_unsigned(built);
    build_list_values_with_unsigned(definitions, unsigned, ctx, definition_tail)
}

pub(super) fn build_list_values_with_unsigned(
    definitions: &[PartitionDefinition],
    unsigned: bool,
    ctx: &crate::StmtContext,
    // Go's definition loop checks THIS definition's name before it reads the
    // NEXT one's values (`ddl/partition.go:1565-1574`).
    definition_tail: &mut dyn FnMut(usize) -> Result<(), DriverError>,
) -> Result<(PartitionKind, Option<DriverError>), DriverError> {
    if definitions.is_empty() {
        return Err(DriverError::PartitionsMustBeDefined("LIST"));
    }
    let mut values = Vec::new();
    let mut seen = HashSet::new();
    let mut null_partition = None;
    let mut default_partition = None;
    // Every 1495 Go raises for LIST comes from `checkListPartitionValue`
    // (`ddl/partition.go:5296` -> `:2010` and `:2064`), which
    // `checkPartitionDefinitionConstraints` reaches only AFTER the
    // name-uniqueness (1517) and partition-count (1499) checks. Refusing
    // inside this loop made a duplicate `VALUES IN` win over a duplicate
    // partition NAME, which Go reports first -- so the collision is carried
    // out to the caller and raised at Go's point instead.
    let mut duplicate: Option<DriverError> = None;

    for (ordinal, definition) in definitions.iter().enumerate() {
        match &definition.clause {
            PartitionDefinitionClause::Default => {
                if default_partition.replace(ordinal).is_some() {
                    duplicate.get_or_insert(DriverError::PartitionDuplicateListValue);
                }
            }
            PartitionDefinitionClause::In(items) => {
                for item in items {
                    if matches!(item, PartitionValue::Default) {
                        if default_partition.replace(ordinal).is_some() {
                            duplicate.get_or_insert(DriverError::PartitionDuplicateListValue);
                        }
                        continue;
                    };
                    let PartitionValue::Expr(expr) = item else {
                        return Err(DriverError::PartitionValuesNotInt(definition.name.clone()));
                    };
                    let value = fold_list_value(expr, &definition.name, unsigned, ctx)?;
                    let Some(bits) = value else {
                        if null_partition.replace(ordinal).is_some() {
                            duplicate.get_or_insert(DriverError::PartitionDuplicateListValue);
                        }
                        continue;
                    };
                    if seen.insert(bits as u64) {
                        values.push((bits, ordinal));
                    } else {
                        duplicate.get_or_insert(DriverError::PartitionDuplicateListValue);
                        // Go's loader fills a map, so the LAST definition to
                        // claim a value owns it. Pushing a second pair would
                        // instead leave the FIRST one to win the search.
                        if let Some(entry) = values
                            .iter_mut()
                            .find(|(value, _)| (*value as u64) == (bits as u64))
                        {
                            entry.1 = ordinal;
                        }
                    }
                }
            }
            PartitionDefinitionClause::LessThan(_) => {
                return Err(DriverError::PartitionWrongValues {
                    method: "RANGE",
                    clause: "VALUES LESS THAN",
                });
            }
            _ => return Err(DriverError::PartitionsMustBeDefined("LIST")),
        }
        definition_tail(ordinal)?;
    }
    let kind = PartitionKind::List {
        values,
        null_partition,
        default_partition,
        unsigned,
    };
    Ok((kind, duplicate))
}

fn fold_list_value(
    expr: &Expr,
    partition: &str,
    unsigned: bool,
    ctx: &crate::StmtContext,
) -> Result<Option<i64>, DriverError> {
    let rewritten = tidb_expr::rewriter::rewrite_expr_resolved(
        expr,
        &tidb_expr::rewriter::ZonedNoResolver::with_like_default_escape(
            ctx.session_zone(),
            ctx.like_default_escape(),
        ),
    )
    .map_err(|_| DriverError::PartitionValuesNotInt(partition.to_owned()))?;
    let mut dual = tidb_chunk::chunk::Chunk::new_empty(&[]);
    dual.set_num_virtual_rows(1);
    match rewritten
        .eval(ctx, dual.get_row(0))
        .map_err(|_| DriverError::PartitionValuesNotInt(partition.to_owned()))?
    {
        Datum::Int(value) => {
            if unsigned && value < 0 {
                Err(DriverError::PartitionConstDomain)
            } else {
                Ok(Some(value))
            }
        }
        Datum::UInt(value) => Ok(Some(value as i64)),
        Datum::Null => Ok(None),
        _ => Err(DriverError::PartitionValuesNotInt(partition.to_owned())),
    }
}


