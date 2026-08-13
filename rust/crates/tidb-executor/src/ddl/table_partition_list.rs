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
//! tuples and collations. Only the former belongs in this module.

use std::collections::HashSet;

use tidb_ast::{Expr, PartitionDefinition, PartitionDefinitionClause, PartitionValue};
use tidb_datatype::{Datum, FieldType};

use crate::partition_routing::PartitionKind;
use crate::DriverError;

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
        &tidb_expr::rewriter::ZonedNoResolver(ctx.session_zone()),
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
    values: &[(i64, usize)],
    null_partition: Option<usize>,
    default_partition: Option<usize>,
    unsigned: bool,
) -> String {
    let mut out = String::from("\n(");
    for (ordinal, definition) in definitions.iter().enumerate() {
        if ordinal > 0 {
            out.push_str(",\n ");
        }
        out.push_str(&format!("PARTITION `{}`", definition.name));
        if default_partition == Some(ordinal) {
            out.push_str(" DEFAULT");
            continue;
        }
        out.push_str(" VALUES IN (");
        let mut first = true;
        for (value, owner) in values {
            if *owner != ordinal {
                continue;
            }
            if !first {
                out.push(',');
            }
            first = false;
            if unsigned {
                out.push_str(&(*value as u64).to_string());
            } else {
                out.push_str(&value.to_string());
            }
        }
        if null_partition == Some(ordinal) {
            if !first {
                out.push(',');
            }
            out.push_str("NULL");
        }
        out.push(')');
    }
    out.push(')');
    out
}
