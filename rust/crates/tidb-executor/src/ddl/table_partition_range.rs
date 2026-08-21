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

//! `PARTITION BY RANGE (expr)`: the `VALUES LESS THAN` bounds, folded and
//! checked.
//!
//! Mirrors Go `pkg/ddl/partition.go`'s `buildRangePartitionDefinitions`
//! together with `checkPartitionValuesIsInt` and the `checkRangePartitionValue`
//! that `checkPartitionDefinitionConstraints` runs after it. The result is the
//! bound list [`crate::partition_routing::PartitionKind::Range`] carries, in
//! definition order.
//!
//! # A bound is a VALUE, not an expression
//!
//! Go EVALUATES each bound at `CREATE` (`expression.EvalSimpleAst`) and
//! stores the folded integer, which `SHOW CREATE TABLE` then prints back.
//! Captured: `VALUES LESS THAN (5+20)` is stored and printed as `25`. So a
//! bound cannot depend on a row, and the routing compares integers.
//!
//! # RANGE COLUMNS is a typed tuple method
//!
//! `PARTITION BY RANGE COLUMNS (a, b)` compares tuples with each column's own
//! type and collation (Go's `locateRangeColumnPartition` over
//! `UpperBounds`), so its bounds live alongside the scalar integer bounds but
//! are represented separately below.

use std::cmp::Ordering;

use tidb_ast::{Expr, PartitionDefinition, PartitionDefinitionClause, PartitionValue};
use tidb_datatype::{Datum, FieldType};

use crate::partition_routing::{RangeBound, RangeColumnBound};
use crate::DriverError;

/// The DDL-owned metadata that becomes one routed `RANGE COLUMNS` spec.
pub(super) type RangeColumnsMetadata = (Vec<String>, Vec<FieldType>, Vec<Vec<RangeColumnBound>>);

/// The exclusive upper bounds of a RANGE table's partitions, in definition
/// order, and whether they compare as unsigned.
///
/// `names`/`types` are the table's own columns and `dependencies` the offsets
/// the partition expression reads, which decide the comparison's signedness
/// (Go `isPartExprUnsigned`).
///
/// # Errors
///
/// The captured `CREATE` rejections for a RANGE table: 1480 (a `VALUES IN`
/// clause under RANGE), 1481 (`MAXVALUE` before the last partition), 1492 (no
/// definitions at all), 1493 (bounds that do not strictly increase), 1563 (a
/// negative bound under an unsigned expression), 1659 (a `NULL` bound) and
/// 1697 (a bound that is not an integer).
pub(super) fn build_range_bounds(
    partition_expr: &Expr,
    definitions: &[tidb_ast::PartitionDefinition],
    names: &[String],
    types: &[FieldType],
    dependencies: &[usize],
    ctx: &crate::StmtContext,
    mode: super::table_partition::PartitionBuildMode,
) -> Result<(Vec<RangeBound>, bool), DriverError> {
    // Go `buildPartitionDefinitionsInfo`: a RANGE table with no definitions
    // is 1492, checked before any bound is read.
    if definitions.is_empty() {
        return Err(DriverError::PartitionsMustBeDefined("RANGE"));
    }
    let unsigned = range_expression_is_unsigned(partition_expr, names, types, dependencies)?;
    let bounds = build_range_bounds_with_unsigned(definitions, unsigned, ctx, mode)?;
    Ok((bounds, unsigned))
}

pub(super) fn build_range_bounds_with_unsigned(
    definitions: &[PartitionDefinition],
    unsigned: bool,
    ctx: &crate::StmtContext,
    mode: super::table_partition::PartitionBuildMode,
) -> Result<Vec<RangeBound>, DriverError> {
    if definitions.is_empty() {
        return Err(DriverError::PartitionsMustBeDefined("RANGE"));
    }

    let mut bounds = Vec::with_capacity(definitions.len());
    for (index, definition) in definitions.iter().enumerate() {
        let values = match &definition.clause {
            PartitionDefinitionClause::LessThan(values) => values,
            // Go `PartitionDefinitionClause.Validate`: only RANGE takes
            // `VALUES LESS THAN`, and only LIST takes `VALUES IN`, so the
            // wrong clause under RANGE names LIST as the owner.
            PartitionDefinitionClause::In(_) => {
                return Err(DriverError::PartitionWrongValues {
                    method: "LIST",
                    clause: "VALUES IN",
                })
            }
            _ => return Err(DriverError::PartitionsMustBeDefined("RANGE")),
        };
        // RANGE COLUMNS is the only form with more than one bound per
        // partition, and it is refused before this point.
        let [value] = values.as_slice() else {
            return Err(DriverError::PartitionsMustBeDefined("RANGE"));
        };
        let bound = match value {
            PartitionValue::MaxValue => {
                // Go `checkRangePartitionValue` strips a TRAILING `MAXVALUE`
                // and then reports 1481 for any other one it meets.
                if mode.validates() && index + 1 != definitions.len() {
                    return Err(DriverError::PartitionMaxValueNotLast);
                }
                RangeBound::MaxValue
            }
            PartitionValue::Expr(expr) => {
                RangeBound::Value(fold_range_bound(expr, &definition.name, unsigned, ctx)?)
            }
            PartitionValue::Default | PartitionValue::Tuple(_) => {
                return Err(DriverError::PartitionValuesNotInt(definition.name.clone()))
            }
        };
        bounds.push(bound);
    }
    // Go's strictly-increasing rule is CREATE-only (`ddl/partition.go:1938`).
    // The loader never re-judges bounds it did not write: a table the cluster
    // is serving must not be refused because this node re-derived a verdict
    // Go reached once, at DDL time.
    if mode.validates() {
        check_strictly_increasing(&bounds, unsigned)?;
    }
    Ok(bounds)
}

/// Builds `RANGE COLUMNS`' typed, lexicographic upper-bound tuples.
///
/// Go folds each written bound through the declared column type during DDL,
/// then compares the tuples left-to-right with that column's collation.  The
/// stored model retains `MAXVALUE` as a sentinel, so `(2, MAXVALUE)` is an
/// ordinary lexicographic upper bound rather than a fabricated string or
/// integer maximum.
pub(super) fn build_range_columns_bounds(
    columns: &[Vec<String>],
    definitions: &[PartitionDefinition],
    names: &[String],
    types: &[FieldType],
    ctx: &crate::StmtContext,
    mode: super::table_partition::PartitionBuildMode,
) -> Result<RangeColumnsMetadata, DriverError> {
    if columns.is_empty() || definitions.is_empty() {
        return Err(DriverError::PartitionsMustBeDefined("RANGE"));
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
            // Go `checkPartitionColumnsUnique` is CREATE-only
            // (`ddl/partition.go:4664`).
            if mode.validates() {
                return Err(DriverError::PartitionDuplicateField(name.clone()));
            }
        }
        let offset = names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(name))
            .ok_or_else(|| DriverError::UnknownColumnInClause {
                column: name.clone(),
                clause: "partition function".to_owned(),
            })?;
        let field_type = types[offset].clone();
        if !super::table_partition_list::list_columns_type_allowed(&field_type) {
            // Go `checkColumnsPartitionType` is CREATE-only
            // (`ddl/partition.go:785`).
            if mode.validates() {
                return Err(DriverError::PartitionFieldTypeNotAllowed(name.clone()));
            }
        }
        dependency_names.push(name.clone());
        field_types.push(field_type);
    }

    let mut less_than = Vec::with_capacity(definitions.len());
    for definition in definitions {
        let PartitionDefinitionClause::LessThan(values) = &definition.clause else {
            return match &definition.clause {
                PartitionDefinitionClause::In(_) => Err(DriverError::PartitionWrongValues {
                    method: "LIST",
                    clause: "VALUES IN",
                }),
                _ => Err(DriverError::PartitionsMustBeDefined("RANGE")),
            };
        };
        // Go `ErrPartitionColumnList` is CREATE-only (`ddl/partition.go:5326`);
        // the loader does not re-check arity on metadata it did not write.
        if mode.validates() && values.len() != field_types.len() {
            return Err(DriverError::PartitionColumnValueWrongType);
        }
        let bound = values
            .iter()
            .zip(&field_types)
            .map(|(value, field_type)| match value {
                PartitionValue::MaxValue => Ok(RangeColumnBound::MaxValue),
                PartitionValue::Expr(expr) => {
                    // Go repeats exactly ONE semantic check when loading:
                    // the bound must fold to a CONSTANT
                    // (`tables/partition.go:423`), raising 1563 when it does
                    // not. A bound that reads a column, or anything else the
                    // fold cannot reduce, is not a bound.
                    let value = super::table_partition_list::fold_column_value(
                        expr, field_type, ctx,
                    )
                    .map_err(|error| {
                        if mode.validates() {
                            error
                        } else {
                            DriverError::PartitionConstDomain
                        }
                    })?;
                    // Go `ErrNullInValuesLessThan` is CREATE-only
                    // (`ddl/partition.go:1691`).
                    if mode.validates() && value.is_null() {
                        return Err(DriverError::PartitionNullInValuesLessThan);
                    }
                    Ok(RangeColumnBound::Value(value))
                }
                PartitionValue::Default | PartitionValue::Tuple(_) => {
                    Err(DriverError::PartitionColumnValueWrongType)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        less_than.push(bound);
    }
    for pair in less_than.windows(2) {
        if !range_columns_bound_increases(&pair[0], &pair[1], &field_types)? {
            return Err(DriverError::PartitionRangeNotIncreasing);
        }
    }
    Ok((dependency_names, field_types, less_than))
}

pub(super) fn range_columns_bound_increases(
    previous: &[RangeColumnBound],
    current: &[RangeColumnBound],
    field_types: &[FieldType],
) -> Result<bool, DriverError> {
    for ((previous, current), field_type) in previous.iter().zip(current).zip(field_types) {
        match (previous, current) {
            (RangeColumnBound::MaxValue, _) => return Ok(false),
            (_, RangeColumnBound::MaxValue) => return Ok(true),
            (RangeColumnBound::Value(previous), RangeColumnBound::Value(current)) => {
                let order = tidb_expr::compare_datums_with_collation(
                    current,
                    previous,
                    field_type.collation(),
                )
                .map_err(|_| DriverError::PartitionColumnValueWrongType)?;
                match order {
                    Ordering::Greater => return Ok(true),
                    Ordering::Less => return Ok(false),
                    Ordering::Equal => {}
                }
            }
        }
    }
    Ok(false)
}

/// Go `checkPartitionValuesIsInt` plus the `EvalSimpleAst` fold: one bound as
/// the integer Go stores.
///
/// The two rejections are captured verbatim: a non-integer bound (`'abc'`,
/// `1.5`) is 1697 naming the PARTITION, while a `NULL` bound is 1659 naming
/// the field as the literal text `NULL` -- which reads oddly and is exactly
/// what real TiDB prints.
pub(super) fn fold_range_bound(
    expr: &Expr,
    partition: &str,
    unsigned: bool,
    ctx: &crate::StmtContext,
) -> Result<i64, DriverError> {
    // The fold reads `ctx`, which is the SESSION's, because a bound's VALUE
    // can depend on the session `time_zone` -- Go threads its own
    // `expression.BuildContext` from the statement down to
    // `checkPartitionValuesIsInt`'s `EvalSimpleAst` for exactly this reason.
    //
    // Captured from real TiDB, the same statement in two sessions:
    // `VALUES LESS THAN (UNIX_TIMESTAMP('2020-01-03 15:10:00'))` stores
    // `1578064200` under `+00:00` and `1578035400` under `+08:00`. Folding
    // under a fixed UTC instead put a row real TiDB routes to `p7` into `p9`
    // -- a wrong answer with no error -- which is why this bound was refused
    // outright until the context reached here.
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
    let value = rewritten
        .eval(ctx, dual.get_row(0))
        .map_err(|_| DriverError::PartitionValuesNotInt(partition.to_owned()))?;
    match value {
        Datum::Int(value) => {
            // Go `checkPartitionValuesIsInt`: a NEGATIVE bound under an
            // unsigned partition expression is out of the function's domain.
            if unsigned && value < 0 {
                return Err(DriverError::PartitionConstDomain);
            }
            Ok(value)
        }
        Datum::UInt(value) => Ok(value as i64),
        Datum::Null => Err(DriverError::PartitionFieldTypeNotAllowed("NULL".to_owned())),
        _ => Err(DriverError::PartitionValuesNotInt(partition.to_owned())),
    }
}

/// Go `checkRangePartitionValue`: the bounds must strictly increase, with a
/// trailing `MAXVALUE` exempt because it is above all of them by definition.
fn check_strictly_increasing(bounds: &[RangeBound], unsigned: bool) -> Result<(), DriverError> {
    let mut previous: Option<i64> = None;
    for bound in bounds {
        let RangeBound::Value(value) = *bound else {
            continue;
        };
        if let Some(previous) = previous {
            let increasing = if unsigned {
                (value as u64) > (previous as u64)
            } else {
                value > previous
            };
            if !increasing {
                return Err(DriverError::PartitionRangeNotIncreasing);
            }
        }
        previous = Some(value);
    }
    Ok(())
}

/// Go `isPartExprUnsigned`: whether the partition expression's own result
/// type is unsigned, which decides how every bound and every routed value
/// compares.
///
/// Go asks the BUILT expression for its flag. This tier answers it for the
/// one shape whose answer is unambiguous -- a bare column, which is unsigned
/// exactly when the column is -- and REFUSES an arithmetic expression over an
/// unsigned column rather than guessing which of MySQL's promotion rules
/// applies. Guessing would put a row in the wrong partition silently, which
/// is the failure this whole module exists to prevent.
pub(super) fn range_expression_is_unsigned(
    expr: &Expr,
    names: &[String],
    types: &[FieldType],
    dependencies: &[usize],
) -> Result<bool, DriverError> {
    let reads_unsigned = dependencies
        .iter()
        .any(|offset| types[*offset].is_unsigned());
    if !reads_unsigned {
        return Ok(false);
    }
    if let Expr::Column(path) = unwrap_parentheses(expr) {
        if let Some(offset) = path.last().and_then(|name| {
            names
                .iter()
                .position(|candidate| candidate.eq_ignore_ascii_case(name))
        }) {
            return Ok(types[offset].is_unsigned());
        }
    }
    Err(DriverError::unsupported(
        "PARTITION BY RANGE over an EXPRESSION of an unsigned column is not supported by this \
         node: whether the bounds compare as signed or unsigned would decide which partition a \
         row lands in, and this node will not guess it"
            .to_owned(),
    ))
}

/// The parenthesised expression's subject, since `(a)` partitions on `a`.
fn unwrap_parentheses(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(inner) => unwrap_parentheses(inner),
        other => other,
    }
}

/// The `SHOW CREATE TABLE` tail Go prints for a RANGE table (Go
/// `ddl.AppendPartitionInfo`'s definition-list form).
///
/// Captured verbatim, including the leading newline, the two-space
/// continuation indent of one leading space, and `MAXVALUE` inside its own
/// parentheses:
///
/// ```text
/// PARTITION BY RANGE (`a`)
/// (PARTITION `p0` VALUES LESS THAN (10),
///  PARTITION `p1` VALUES LESS THAN (20),
///  PARTITION `pm` VALUES LESS THAN (MAXVALUE))
/// ```
#[must_use]
pub fn range_definitions_text(
    definitions: &[crate::partition_routing::PartitionDef],
    less_than: &[RangeBound],
    unsigned: bool,
) -> String {
    let mut out = String::from("\n(");
    for (index, definition) in definitions.iter().enumerate() {
        if index > 0 {
            out.push_str(",\n ");
        }
        let bound = match less_than.get(index) {
            Some(RangeBound::MaxValue) | None => "MAXVALUE".to_owned(),
            Some(RangeBound::Value(value)) if unsigned => format!("{}", *value as u64),
            Some(RangeBound::Value(value)) => format!("{value}"),
        };
        out.push_str(&format!(
            "PARTITION `{}` VALUES LESS THAN ({bound}){}",
            definition.name,
            super::table_partition::partition_comment_text(&definition.comment)
        ));
    }
    out.push(')');
    out
}

/// `SHOW CREATE TABLE`'s typed `RANGE COLUMNS` definition list.
#[must_use]
pub fn range_columns_definitions_text(
    definitions: &[crate::partition_routing::PartitionDef],
    less_than: &[Vec<RangeColumnBound>],
) -> String {
    let mut out = String::from("\n(");
    for (index, definition) in definitions.iter().enumerate() {
        if index > 0 {
            out.push_str(",\n ");
        }
        out.push_str(&format!(
            "PARTITION `{}` VALUES LESS THAN (",
            definition.name
        ));
        if let Some(bound) = less_than.get(index) {
            for (component, value) in bound.iter().enumerate() {
                if component > 0 {
                    out.push(',');
                }
                match value {
                    RangeColumnBound::MaxValue => out.push_str("MAXVALUE"),
                    RangeColumnBound::Value(value) => {
                        let rendered = value
                            .restore_value_expr()
                            .expect("RANGE COLUMNS metadata contains restorable values");
                        // Go runs every printed bound through `hexIfNonPrint`
                        // (`ddl/partition.go:5206`), so a value MySQL cannot
                        // quote becomes a `0x...` literal rather than raw
                        // bytes in the DDL.
                        out.push_str(&super::table_partition::hex_if_non_print(
                            &String::from_utf8_lossy(&rendered),
                        ));
                    }
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The captured `SHOW CREATE TABLE` tail, character for character.
    #[test]
    fn the_definition_list_matches_gos_own_spelling() {
        let definitions = ["p0", "p1", "pm"].map(|name| crate::partition_routing::PartitionDef {
            id: 0,
            name: name.to_owned(),
            less_than: Vec::new(),
            in_values: Vec::new(),
            comment: String::new(),
        });
        assert_eq!(
            range_definitions_text(
                &definitions,
                &[
                    RangeBound::Value(10),
                    RangeBound::Value(20),
                    RangeBound::MaxValue
                ],
                false
            ),
            "\n(PARTITION `p0` VALUES LESS THAN (10),\n PARTITION `p1` VALUES LESS THAN (20),\n \
             PARTITION `pm` VALUES LESS THAN (MAXVALUE))"
        );
    }

    /// A trailing `MAXVALUE` is exempt from the increasing rule; one before
    /// the end never reaches it, because 1481 fires first.
    #[test]
    fn only_strictly_increasing_bounds_are_accepted() {
        assert!(check_strictly_increasing(
            &[
                RangeBound::Value(10),
                RangeBound::Value(20),
                RangeBound::MaxValue
            ],
            false
        )
        .is_ok());
        assert!(
            check_strictly_increasing(&[RangeBound::Value(10), RangeBound::Value(5)], false)
                .is_err()
        );
        assert!(
            check_strictly_increasing(&[RangeBound::Value(10), RangeBound::Value(10)], false)
                .is_err()
        );
        // Read as unsigned, the pattern -1 is ABOVE 10 and the same pair is
        // increasing rather than not.
        assert!(
            check_strictly_increasing(&[RangeBound::Value(10), RangeBound::Value(-1)], true)
                .is_ok()
        );
        assert!(
            check_strictly_increasing(&[RangeBound::Value(10), RangeBound::Value(-1)], false)
                .is_err()
        );
    }
}
