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
        if dependency_names
            .iter()
            .any(|candidate: &String| candidate.eq_ignore_ascii_case(name))
        {
            return Err(DriverError::PartitionDuplicateField(name.clone()));
        }
        let offset = names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(name))
            .ok_or_else(|| DriverError::UnknownColumnInClause {
                column: name.clone(),
                clause: "partition function".to_owned(),
            })?;
        let field_type = types[offset].clone();
        if !list_columns_type_allowed(&field_type) {
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
                        _ => return Err(DriverError::PartitionColumnValueWrongType),
                    };
                    let tuple = exprs
                        .iter()
                        .zip(&field_types)
                        .map(|(expr, field_type)| fold_column_value(expr, field_type, ctx))
                        .collect::<Result<Vec<_>, _>>()?;
                    let key = tidb_codec::encode_key_in_timezone(&ctx.session_zone(), &tuple)
                        .map_err(|_| DriverError::PartitionColumnValueWrongType)?;
                    if keys.insert(key, ordinal).is_some() {
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

fn set_default_partition(slot: &mut Option<usize>, ordinal: usize) -> Result<(), DriverError> {
    if slot.replace(ordinal).is_some() {
        Err(DriverError::PartitionDuplicateListValue)
    } else {
        Ok(())
    }
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
    partition_expr: &Expr,
    definitions: &[PartitionDefinition],
    names: &[String],
    types: &[FieldType],
    dependencies: &[usize],
    ctx: &crate::StmtContext,
) -> Result<PartitionKind, DriverError> {
    if definitions.is_empty() {
        return Err(DriverError::PartitionsMustBeDefined("LIST"));
    }
    let unsigned = super::table_partition_range::range_expression_is_unsigned(
        partition_expr,
        names,
        types,
        dependencies,
    )?;
    build_list_values_with_unsigned(definitions, unsigned, ctx)
}

pub(super) fn build_list_values_with_unsigned(
    definitions: &[PartitionDefinition],
    unsigned: bool,
    ctx: &crate::StmtContext,
) -> Result<PartitionKind, DriverError> {
    if definitions.is_empty() {
        return Err(DriverError::PartitionsMustBeDefined("LIST"));
    }
    let mut values = Vec::new();
    let mut seen = HashSet::new();
    let mut null_partition = None;
    let mut default_partition = None;

    for (ordinal, definition) in definitions.iter().enumerate() {
        match &definition.clause {
            PartitionDefinitionClause::Default => {
                if default_partition.replace(ordinal).is_some() {
                    return Err(DriverError::PartitionDuplicateListValue);
                }
            }
            PartitionDefinitionClause::In(items) => {
                for item in items {
                    if matches!(item, PartitionValue::Default) {
                        if default_partition.replace(ordinal).is_some() {
                            return Err(DriverError::PartitionDuplicateListValue);
                        }
                        continue;
                    };
                    let PartitionValue::Expr(expr) = item else {
                        return Err(DriverError::PartitionValuesNotInt(definition.name.clone()));
                    };
                    let value = fold_list_value(expr, &definition.name, unsigned, ctx)?;
                    let Some(bits) = value else {
                        if null_partition.replace(ordinal).is_some() {
                            return Err(DriverError::PartitionDuplicateListValue);
                        }
                        continue;
                    };
                    if !seen.insert(bits as u64) {
                        return Err(DriverError::PartitionDuplicateListValue);
                    }
                    values.push((bits, ordinal));
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
    }
    Ok(PartitionKind::List {
        values,
        null_partition,
        default_partition,
        unsigned,
    })
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

/// `SHOW CREATE TABLE`'s scalar LIST definition list.
#[must_use]
pub fn list_definitions_text(
    definitions: &[crate::partition_routing::PartitionDef],
    _values: &[(i64, usize)],
    _null_partition: Option<usize>,
    _default_partition: Option<usize>,
    _unsigned: bool,
) -> String {
    // Go `AppendPartitionDefs` (`ddl/partition.go:5209`) prints a LIST
    // partition from its STORED `InValues`, not from the folded values:
    //
    //   len(InValues) == 0                    -> " DEFAULT"
    //   InValues == [["DEFAULT"]] (EqualFold) -> " DEFAULT"
    //   otherwise                             -> " VALUES IN (...)"
    //
    // Rendering from the folded values lost the written position of `NULL`
    // (it was appended last whatever the user wrote) and dropped the values
    // of a partition that ALSO carried `DEFAULT`, printing a bare `DEFAULT`
    // for `VALUES IN (1, 2, DEFAULT)`.
    let mut out = String::from("\n(");
    for (ordinal, definition) in definitions.iter().enumerate() {
        if ordinal > 0 {
            out.push_str(",\n ");
        }
        out.push_str(&format!("PARTITION `{}`", definition.name));
        let bare_default = definition.in_values.is_empty()
            || (definition.in_values.len() == 1
                && definition.in_values[0].len() == 1
                && definition.in_values[0][0].eq_ignore_ascii_case("DEFAULT"));
        if bare_default {
            out.push_str(" DEFAULT");
        } else {
            out.push_str(" VALUES IN (");
            for (index, tuple) in definition.in_values.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                match tuple.as_slice() {
                    [single] => out.push_str(&super::table_partition::hex_if_non_print(single)),
                    many => {
                        out.push('(');
                        for (position, value) in many.iter().enumerate() {
                            if position > 0 {
                                out.push(',');
                            }
                            out.push_str(&super::table_partition::hex_if_non_print(value));
                        }
                        out.push(')');
                    }
                }
            }
            out.push(')');
        }
        out.push_str(&super::table_partition::partition_comment_text(
            &definition.comment,
        ));
    }
    out.push(')');
    out
}

/// `SHOW CREATE TABLE`'s `LIST COLUMNS` definition list. The values held in
/// metadata are already converted to their declared types, just like Go's
/// `PartitionDefinition.InValues` after `formatListPartitionValue`.
#[must_use]
pub fn list_columns_definitions_text(
    definitions: &[crate::partition_routing::PartitionDef],
    values: &[(Vec<Datum>, usize)],
    default_partition: Option<usize>,
) -> String {
    let mut out = String::from("\n(");
    for (ordinal, definition) in definitions.iter().enumerate() {
        if ordinal > 0 {
            out.push_str(",\n ");
        }
        out.push_str(&format!("PARTITION `{}`", definition.name));
        if default_partition == Some(ordinal) {
            out.push_str(" DEFAULT");
            out.push_str(&super::table_partition::partition_comment_text(
                &definition.comment,
            ));
            continue;
        }
        out.push_str(" VALUES IN (");
        let mut first = true;
        for (tuple, owner) in values {
            if *owner != ordinal {
                continue;
            }
            if !first {
                out.push(',');
            }
            first = false;
            if tuple.len() > 1 {
                out.push('(');
            }
            for (index, value) in tuple.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                let rendered = value
                    .restore_value_expr()
                    .expect("LIST COLUMNS metadata contains a restorable value expression");
                // Go `AppendPartitionDefs` (`ddl/partition.go:5226`) runs
                // every printed `VALUES IN` component through
                // `hexIfNonPrint`.
                out.push_str(&super::table_partition::hex_if_non_print(
                    &String::from_utf8_lossy(&rendered),
                ));
            }
            if tuple.len() > 1 {
                out.push(')');
            }
        }
        out.push(')');
        out.push_str(&super::table_partition::partition_comment_text(
            &definition.comment,
        ));
    }
    out.push(')');
    out
}
