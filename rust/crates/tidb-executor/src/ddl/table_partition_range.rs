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
    built: &tidb_expr::expression::Expression,
    definitions: &[tidb_ast::PartitionDefinition],
    ctx: &crate::StmtContext,
    mode: super::table_partition::PartitionBuildMode,
    definition_tail: &mut dyn FnMut(usize) -> Result<(), DriverError>,
) -> Result<(Vec<RangeBound>, bool), DriverError> {
    // Go `buildPartitionDefinitionsInfo`: a RANGE table with no definitions
    // is 1492, checked before any bound is read.
    if definitions.is_empty() {
        return Err(DriverError::PartitionsMustBeDefined("RANGE"));
    }
    let unsigned = partition_expression_is_unsigned(built);
    let bounds =
        build_range_bounds_with_unsigned(definitions, unsigned, ctx, mode, definition_tail)?;
    Ok((bounds, unsigned))
}

pub(super) fn build_range_bounds_with_unsigned(
    definitions: &[PartitionDefinition],
    unsigned: bool,
    ctx: &crate::StmtContext,
    mode: super::table_partition::PartitionBuildMode,
    // Go's definition loop checks THIS definition's comment and name before
    // it reads the NEXT one's values (`ddl/partition.go:1650-1670`), so the
    // two cannot be run as separate passes without changing which error a
    // statement that is wrong twice reports.
    definition_tail: &mut dyn FnMut(usize) -> Result<(), DriverError>,
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
                RangeBound::Value(fold_range_bound(
                    expr,
                    &definition.name,
                    unsigned,
                    ctx,
                    mode,
                )?)
            }
            PartitionValue::Default | PartitionValue::Tuple(_) => {
                return Err(DriverError::PartitionValuesNotInt(definition.name.clone()))
            }
        };
        bounds.push(bound);
        definition_tail(index)?;
    }
    // Go's strictly-increasing rule lives in `checkPartitionByRange`, which
    // `checkPartitionDefinitionConstraints` runs after 1517/1499/1652 -- so
    // the caller raises it, not this folder. It is CREATE-only either way
    // (`ddl/partition.go:1938`): the loader never re-judges bounds it did not
    // write.
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
    definition_tail: &mut dyn FnMut(usize) -> Result<(), DriverError>,
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
            // Go `checkColumnsPartitionType` (`partition.go:787`) reports a
            // column list's missing name as `ErrFieldNotFoundPart` (1488),
            // not the 1054 the expression path raises.
            .ok_or(DriverError::PartitionFieldNotFound)?;
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
    for (index, definition) in definitions.iter().enumerate() {
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
        definition_tail(index)?;
    }
    Ok((dependency_names, field_types, less_than))
}

/// Go `checkPartitionByRange` -> `checkRangeColumnsPartitionValue`
/// (`ddl/partition.go:5296`): the bounds must strictly increase.
///
/// Go reaches it from `checkPartitionDefinitionConstraints`, AFTER the
/// name-uniqueness (1517), partition-count (1499) and duplicate-column
/// (1652) checks -- so a statement wrong in two ways reports the one Go
/// reports. Folding it into the per-definition loop above made 1493 win over
/// all three.
pub(super) fn check_range_columns_strictly_increasing(
    less_than: &[Vec<RangeColumnBound>],
    field_types: &[FieldType],
) -> Result<(), DriverError> {
    for pair in less_than.windows(2) {
        if !range_columns_bound_increases(&pair[0], &pair[1], field_types)? {
            return Err(DriverError::PartitionRangeNotIncreasing);
        }
    }
    Ok(())
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
    mode: super::table_partition::PartitionBuildMode,
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
            // That check lives in `buildPartitionDefinitionsInfo`, so it is
            // CREATE-only -- the loader takes the stored bound as given.
            if mode.validates() && unsigned && value < 0 {
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
pub(super) fn check_strictly_increasing(
    bounds: &[RangeBound],
    unsigned: bool,
) -> Result<(), DriverError> {
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

/// Go `isPartExprUnsigned` (`ddl/partition.go:4875`): whether the partition
/// expression's own result type carries `mysql.UnsignedFlag`.
///
/// Go builds the expression and reads the flag off it -- nothing more -- and
/// on a build FAILURE it logs and answers `false`. It never refuses the
/// table over this question.
///
/// The previous port answered only for a bare column and refused any
/// arithmetic over an unsigned one rather than guess MySQL's promotion
/// rules. But the promotion rules are already ported:
/// `builtin_arithmetic::infer_arithmetic_type_with_context` sets
/// `UNSIGNED` when either operand carries it, with Go's own
/// `NO_UNSIGNED_SUBTRACTION` exception. So the built expression ALREADY
/// knows the answer, and asking it is both exact and unable to refuse --
/// which matters most on the LOAD path, where the refusal made a table a Go
/// cluster serves unreadable here.
pub(super) fn partition_expression_is_unsigned(
    built: &tidb_expr::expression::Expression,
) -> bool {
    built.static_type().is_some_and(FieldType::is_unsigned)
}




#[cfg(test)]
mod tests {
    use super::*;

    /// The captured `SHOW CREATE TABLE` tail, character for character.
    #[test]
    fn the_definition_list_matches_gos_own_spelling() {
        let definitions = [("p0", "10"), ("p1", "20"), ("pm", "MAXVALUE")].map(|(name, bound)| {
            crate::partition_routing::PartitionDef {
                id: 0,
                name: name.to_owned(),
                less_than: vec![bound.to_owned()],
                in_values: Vec::new(),
                comment: String::new(),
                placement_policy: None,
            }
        });
        assert_eq!(
            super::super::table_partition::append_partition_defs(
                &definitions,
                &crate::partition_routing::PartitionKind::Range {
                    less_than: Vec::new(),
                    unsigned: false,
                }
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
