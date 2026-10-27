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
//! bound list [`tidb_table::partition_routing::PartitionKind::Range`] carries, in
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
use tidb_datatype::FieldType;

use tidb_table::partition_routing::{RangeBound, RangeColumnBound};
use tidb_error::mysql::MysqlError;

pub use tidb_table::partition_fold::partition_expression_is_unsigned;
use tidb_hack::GoCaseCompare;

/// The DDL-owned metadata that becomes one routed `RANGE COLUMNS` spec.
pub type RangeColumnsMetadata = (Vec<String>, Vec<FieldType>, Vec<Vec<RangeColumnBound>>);

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
pub fn build_range_bounds(
    built: &tidb_expr::expression::Expression,
    definitions: &[tidb_ast::PartitionDefinition],
    ctx: &tidb_expr::metabuild::Context,
    definition_tail: &mut dyn FnMut(usize) -> Result<(), MysqlError>,
) -> Result<(Vec<RangeBound>, bool), MysqlError> {
    // Go `buildPartitionDefinitionsInfo`: a RANGE table with no definitions
    // is 1492, checked before any bound is read.
    if definitions.is_empty() {
        return Err(tidb_error::mysql::MysqlError::from_terror(&tidb_error::catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED.generate_with_stack_by_args(&["RANGE".into()])));
    }
    let unsigned = partition_expression_is_unsigned(built);
    let bounds =
        build_range_bounds_with_unsigned(definitions, unsigned, ctx, definition_tail)?;
    Ok((bounds, unsigned))
}

pub fn build_range_bounds_with_unsigned(
    definitions: &[PartitionDefinition],
    unsigned: bool,
    ctx: &tidb_expr::metabuild::Context,
    // Go's definition loop checks THIS definition's comment and name before
    // it reads the NEXT one's values (`ddl/partition.go:1650-1670`), so the
    // two cannot be run as separate passes without changing which error a
    // statement that is wrong twice reports.
    definition_tail: &mut dyn FnMut(usize) -> Result<(), MysqlError>,
) -> Result<Vec<RangeBound>, MysqlError> {
    if definitions.is_empty() {
        return Err(tidb_error::mysql::MysqlError::from_terror(&tidb_error::catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED.generate_with_stack_by_args(&["RANGE".into()])));
    }

    let mut bounds = Vec::with_capacity(definitions.len());
    for (index, definition) in definitions.iter().enumerate() {
        let values = match &definition.clause {
            PartitionDefinitionClause::LessThan(values) => values,
            // Go `PartitionDefinitionClause.Validate`: only RANGE takes
            // `VALUES LESS THAN`, and only LIST takes `VALUES IN`, so the
            // wrong clause under RANGE names LIST as the owner.
            PartitionDefinitionClause::In(_) => {
                return Err(tidb_error::mysql::MysqlError::from_terror(&tidb_error::catalog::ast::ERR_PARTITION_WRONG_VALUES.generate_with_stack_by_args(&["LIST".into(), "IN".into()])))
            }
            _ => return Err(tidb_error::mysql::MysqlError::from_terror(&tidb_error::catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED.generate_with_stack_by_args(&["RANGE".into()]))),
        };
        // RANGE COLUMNS is the only form with more than one bound per
        // partition, and it is refused before this point.
        let [value] = values.as_slice() else {
            return Err(tidb_error::mysql::MysqlError::from_terror(&tidb_error::catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED.generate_with_stack_by_args(&["RANGE".into()])));
        };
        let bound = match value {
            PartitionValue::MaxValue => {
                // Go `checkRangePartitionValue` strips a TRAILING `MAXVALUE`
                // and then reports 1481 for any other one it meets.
                if index + 1 != definitions.len() {
                    return Err(tidb_error::mysql::MysqlError::from_terror(
                        &tidb_util::dbterror::ERR_PARTITION_MAXVALUE
                            .generate_with_stack_by_args(&[]),
                    ));
                }
                RangeBound::MaxValue
            }
            PartitionValue::Expr(expr) => {
                RangeBound::Value(fold_range_bound(expr, &definition.name, unsigned, ctx)?)
            }
            PartitionValue::Default | PartitionValue::Tuple(_) => {
                return Err(tidb_error::mysql::MysqlError::from_terror(&tidb_util::dbterror::ERR_VALUES_IS_NOT_INT_TYPE.generate_with_stack_by_args(&[(definition.name.clone()).to_string().into()])))
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
pub fn build_range_columns_bounds(
    columns: &[Vec<String>],
    definitions: &[PartitionDefinition],
    names: &[String],
    types: &[FieldType],
    ctx: &tidb_expr::metabuild::Context,
    definition_tail: &mut dyn FnMut(usize) -> Result<(), MysqlError>,
) -> Result<RangeColumnsMetadata, MysqlError> {
    if columns.is_empty() || definitions.is_empty() {
        return Err(tidb_error::mysql::MysqlError::from_terror(&tidb_error::catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED.generate_with_stack_by_args(&["RANGE".into()])));
    }
    // Go checks the bound tuple arity before resolving the named partition
    // columns.  That ordering is observable for a missing column paired with
    // a malformed tuple: both shapes report ErrPartitionColumnList (1653),
    // rather than the later 1488 missing-field error.
    if definitions.iter().any(|definition| {
        matches!(
            &definition.clause,
            PartitionDefinitionClause::LessThan(values) if values.len() != columns.len()
        )
    }) {
        return Err(tidb_error::mysql::MysqlError::from_terror(&tidb_error::catalog::ast::ERR_PARTITION_COLUMN_LIST.generate_with_stack_by_args(&[])));
    }
    let mut dependency_names = Vec::with_capacity(columns.len());
    let mut field_types = Vec::with_capacity(columns.len());
    for path in columns {
        let name = path
            .last()
            .ok_or_else(|| tidb_error::mysql::MysqlError::from_terror(&tidb_util::dbterror::ERR_WRONG_TYPE_COLUMN_VALUE.generate_with_stack_by_args(&[])))?;
        // Go `checkPartitionColumnsUnique` (`ddl/partition.go:4664`).
        if dependency_names
            .iter()
            .any(|candidate: &String| candidate.go_lower_eq(name))
        {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_SAME_NAME_PARTITION_FIELD
                    .generate_with_stack_by_args(&[(name.clone()).to_string().into()]),
            ));
        }
        let offset = names
            .iter()
            .position(|candidate| candidate.go_lower_eq(name))
            // Go `checkColumnsPartitionType` (`partition.go:787`) reports a
            // column list's missing name as `ErrFieldNotFoundPart` (1488),
            // not the 1054 the expression path raises.
            .ok_or_else(|| tidb_error::mysql::MysqlError::from_terror(&tidb_util::dbterror::ERR_FIELD_NOT_FOUND_PART.generate_with_stack_by_args(&[])))?;
        let field_type = types[offset].clone();
        // Go `checkColumnsPartitionType` (`ddl/partition.go:785`).
        if !super::table_partition_list::list_columns_type_allowed(&field_type) {
            return Err(tidb_error::mysql::MysqlError::from_terror(&tidb_util::dbterror::ERR_NOT_ALLOWED_TYPE_IN_PARTITION.generate_with_stack_by_args(&[(name.clone()).to_string().into()])));
        }
        dependency_names.push(name.clone());
        field_types.push(field_type);
    }

    let mut less_than = Vec::with_capacity(definitions.len());
    for (index, definition) in definitions.iter().enumerate() {
        let PartitionDefinitionClause::LessThan(values) = &definition.clause else {
            return match &definition.clause {
                PartitionDefinitionClause::In(_) => {
                    Err(tidb_error::mysql::MysqlError::from_terror(&tidb_error::catalog::ast::ERR_PARTITION_WRONG_VALUES.generate_with_stack_by_args(&["LIST".into(), "IN".into()])))
                }
                _ => Err(tidb_error::mysql::MysqlError::from_terror(&tidb_error::catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED.generate_with_stack_by_args(&["RANGE".into()]))),
            };
        };
        let bound = values
            .iter()
            .zip(&field_types)
            .map(|(value, field_type)| match value {
                PartitionValue::MaxValue => Ok(RangeColumnBound::MaxValue),
                PartitionValue::Expr(expr) => {
                    let value =
                        super::table_partition_list::fold_range_column_value(expr, field_type, ctx)?;
                    // Go `ErrNullInValuesLessThan` (`ddl/partition.go:1691`).
                    if value.is_null() {
                        return Err(tidb_error::mysql::MysqlError::from_terror(
                            &tidb_util::dbterror::ERR_NULL_IN_VALUES_LESS_THAN
                                .generate_with_stack_by_args(&[]),
                        ));
                    }
                    Ok(RangeColumnBound::Value(value))
                }
                PartitionValue::Default | PartitionValue::Tuple(_) => {
                    Err(tidb_error::mysql::MysqlError::from_terror(&tidb_util::dbterror::ERR_WRONG_TYPE_COLUMN_VALUE.generate_with_stack_by_args(&[])))
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
pub fn check_range_columns_strictly_increasing(
    less_than: &[Vec<RangeColumnBound>],
    field_types: &[FieldType],
) -> Result<(), MysqlError> {
    for pair in less_than.windows(2) {
        if !range_columns_bound_increases(&pair[0], &pair[1], field_types)? {
            return Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_RANGE_NOT_INCREASING.generate_with_stack_by_args(&[]),
            ));
        }
    }
    Ok(())
}

pub fn range_columns_bound_increases(
    previous: &[RangeColumnBound],
    current: &[RangeColumnBound],
    field_types: &[FieldType],
) -> Result<bool, MysqlError> {
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
                .map_err(|_| tidb_error::mysql::MysqlError::from_terror(&tidb_util::dbterror::ERR_WRONG_TYPE_COLUMN_VALUE.generate_with_stack_by_args(&[])))?;
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

/// Go `checkPartitionValuesIsInt` over the shared `EvalSimpleAst` fold
/// ([`tidb_table::partition_fold::fold_range_bound`]): a NEGATIVE bound under
/// an unsigned partition expression is out of the function's domain (1563).
/// The check lives in `buildPartitionDefinitionsInfo`, so the loader, which
/// takes the stored bound as given, does not make it.
pub fn fold_range_bound(
    expr: &Expr,
    partition: &str,
    unsigned: bool,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<i64, MysqlError> {
    match tidb_table::partition_fold::fold_range_bound(expr, partition, ctx)? {
        tidb_table::partition_fold::FoldedRangeBound::Signed(value) if unsigned && value < 0 => {
            Err(tidb_error::mysql::MysqlError::from_terror(&tidb_util::dbterror::ERR_PARTITION_CONST_DOMAIN.generate_with_stack_by_args(&[])))
        }
        folded => Ok(folded.stored()),
    }
}

/// Go `checkRangePartitionValue`: the bounds must strictly increase, with a
/// trailing `MAXVALUE` exempt because it is above all of them by definition.
pub fn check_strictly_increasing(
    bounds: &[RangeBound],
    unsigned: bool,
) -> Result<(), MysqlError> {
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
                return Err(tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::ERR_RANGE_NOT_INCREASING.generate_with_stack_by_args(&[]),
                ));
            }
        }
        previous = Some(value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The captured `SHOW CREATE TABLE` tail, character for character.
    #[test]
    fn the_definition_list_matches_gos_own_spelling() {
        let definitions = [("p0", "10"), ("p1", "20"), ("pm", "MAXVALUE")].map(|(name, bound)| {
            tidb_table::partition_routing::PartitionDef {
                id: 0,
                name: name.to_owned(),
                less_than: vec![bound.to_owned()],
                in_values: Vec::new(),
                comment: String::new(),
                placement_policy: None,
                storage_class: Default::default(),
            }
        });
        assert_eq!(
            super::super::table_partition::append_partition_defs(
                &definitions,
                &tidb_table::partition_routing::PartitionKind::Range {
                    less_than: Vec::new(),
                    unsigned: false,
                },
                false,
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
