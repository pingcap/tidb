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

//! How a partition bound or `VALUES IN` value FOLDS, shared by Go's two
//! partition paths: the DDL's validation of a written clause
//! (`pkg/ddl/partition.go`) and the table layer's rebuild of stored metadata
//! (`pkg/table/tables/partition.go`). The two disagree on what they CHECK;
//! a bound must fold the same way on both, so the folding lives once, here.

use std::collections::HashSet;

use tidb_ast::{Expr, PartitionDefinition, PartitionDefinitionClause, PartitionValue};
use tidb_datatype::{Datum, DatumKind, FieldType, FieldTypeCode};
use tidb_terror::MysqlError;

use crate::partition_routing::PartitionKind;

/// A LIST COLUMNS value folded through its column's type: evaluated as a
/// constant, then converted (Go `EvalSimpleAst` + `ConvertTo`).
pub fn fold_column_value(
    expr: &Expr,
    field_type: &FieldType,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<Datum, MysqlError> {
    let value = eval_column_value(expr, ctx)?;
    convert_column_value(value, field_type, ctx)
}

/// Folds a RANGE COLUMNS bound with Go's source-kind compatibility check.
///
/// `checkAndGetColumnsTypeAndValuesMatch` in `pkg/ddl/partition.go` checks
/// the result of `EvalSimpleAst` before `ConvertTo`.  A conversion that looks
/// harmless to the datatype layer is therefore still rejected when the
/// written literal belongs to a different kind (for example, an integer for
/// a DATETIME column).  LIST COLUMNS uses a different parser path and keeps
/// the broader conversion behavior of [`fold_column_value`].
pub fn fold_range_column_value(
    expr: &Expr,
    field_type: &FieldType,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<Datum, MysqlError> {
    if contains_collate(expr) {
        // Go's `buildRangePartitionDefinitions` runs the partition-expression
        // allowlist over each bound after folding.  COLLATE is not one of the
        // allowed value forms, even though the evaluator can otherwise pass
        // the string through unchanged.
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED
                .generate_with_stack_by_args(&[]),
        ));
    }
    let value = eval_column_value(expr, ctx)?;
    if !range_column_value_kind_allowed(value.kind(), field_type.code()) {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_WRONG_TYPE_COLUMN_VALUE.generate_with_stack_by_args(&[]),
        ));
    }
    convert_column_value(value, field_type, ctx)
}

/// The session context Go's partition DDL evaluates a bound under
/// (`EvalSimpleAst`, `ParseSimpleExpr`): its zone, `LIKE` escape and
/// connection charset, so a string literal takes `collation_connection`.
pub fn partition_value_resolver(
    ctx: &tidb_expr::metabuild::Context,
) -> tidb_expr::rewriter::ZonedNoResolver {
    let (charset, collation) = ctx.get_expr_ctx().get_charset_info();
    tidb_expr::rewriter::ZonedNoResolver::with_like_default_escape(
        ctx.get_expr_ctx().get_eval_ctx().time_zone(),
        ctx.get_expr_ctx().get_eval_ctx().like_default_escape(),
    )
    .with_connection_charset(charset, collation)
}

/// Go's `EvalSimpleAst` hands the rewriter's own error back, so a partition
/// bound naming a column is 1054 ("Unknown column 'a' in 'expression'"), not
/// the value-type refusal the other failures keep.
pub fn partition_value_failure(error: tidb_expr::EvalError, otherwise: MysqlError) -> MysqlError {
    match error {
        tidb_expr::EvalError::UnknownColumnInClause(column, clause) => {
            tidb_terror::MysqlError::from_terror(
                &tidb_plannererrors::ERR_UNKNOWN_COLUMN.generate_with_stack_by_args(&[
                    tidb_hack::go_to_lower(column).into(),
                    clause.to_owned().into(),
                ]),
            )
        }
        _ => otherwise,
    }
}

/// A partition value evaluated as a constant under `ctx` (Go
/// `EvalSimpleAst`).
pub fn eval_column_value(
    expr: &Expr,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<Datum, MysqlError> {
    let rewritten = tidb_expr::rewriter::rewrite_expr_resolved(
        expr,
        &partition_value_resolver(ctx),
    )
    .map_err(|_| {
        tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_WRONG_TYPE_COLUMN_VALUE.generate_with_stack_by_args(&[]),
        )
    })?;
    let mut dual = tidb_chunk::chunk::Chunk::new_empty(&[]);
    dual.set_num_virtual_rows(1);
    rewritten
        .eval(ctx.get_expr_ctx().get_eval_ctx(), dual.get_row(0))
        .map_err(|_| {
            tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_WRONG_TYPE_COLUMN_VALUE.generate_with_stack_by_args(&[]),
            )
        })
}

fn convert_column_value(
    value: Datum,
    field_type: &FieldType,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<Datum, MysqlError> {
    let converted = value
        .convert_to_in(
            field_type,
            ctx.get_expr_ctx().get_eval_ctx().type_ctx_flags(),
            &ctx.get_expr_ctx().get_eval_ctx().time_zone(),
        )
        .map_err(|_| {
            tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_WRONG_TYPE_COLUMN_VALUE.generate_with_stack_by_args(&[]),
            )
        })?;
    if converted.event.is_some() {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_WRONG_TYPE_COLUMN_VALUE.generate_with_stack_by_args(&[]),
        ));
    }
    Ok(converted.value)
}

fn range_column_value_kind_allowed(kind: DatumKind, code: FieldTypeCode) -> bool {
    match code {
        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Duration => {
            matches!(kind, DatumKind::String | DatumKind::Bytes | DatumKind::Null)
        }
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong => {
            matches!(kind, DatumKind::Int | DatumKind::UInt | DatumKind::Null)
        }
        FieldTypeCode::Float | FieldTypeCode::Double => {
            matches!(kind, DatumKind::Float32 | DatumKind::Real | DatumKind::Null)
        }
        FieldTypeCode::String | FieldTypeCode::VarString => matches!(
            kind,
            DatumKind::String | DatumKind::Bytes | DatumKind::Null | DatumKind::BinaryLiteral
        ),
        _ => true,
    }
}

fn contains_collate(expr: &Expr) -> bool {
    match expr {
        Expr::Collate { .. } => true,
        Expr::Paren(inner) | Expr::Unary(_, inner) => contains_collate(inner),
        Expr::Binary(_, left, right) => contains_collate(left) || contains_collate(right),
        Expr::Extract { value, .. } => contains_collate(value),
        Expr::Func { args, .. } => args.iter().any(contains_collate),
        _ => false,
    }
}

/// Folds scalar LIST definitions into exact-value routing metadata.
pub fn build_list_values(
    built: &tidb_expr::expression::Expression,
    definitions: &[PartitionDefinition],
    ctx: &tidb_expr::metabuild::Context,
    definition_tail: &mut dyn FnMut(usize) -> Result<(), MysqlError>,
) -> Result<(PartitionKind, Option<MysqlError>), MysqlError> {
    if definitions.is_empty() {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_ast::ast_errors::ERR_PARTITIONS_MUST_BE_DEFINED
                .generate_with_stack_by_args(&["LIST".into()]),
        ));
    }
    let unsigned = partition_expression_is_unsigned(built);
    build_list_values_with_unsigned(definitions, unsigned, ctx, definition_tail)
}

/// [`build_list_values`] for an expression whose signedness the caller
/// already knows.
pub fn build_list_values_with_unsigned(
    definitions: &[PartitionDefinition],
    unsigned: bool,
    ctx: &tidb_expr::metabuild::Context,
    // Go's definition loop checks THIS definition's name before it reads the
    // NEXT one's values (`ddl/partition.go:1565-1574`).
    definition_tail: &mut dyn FnMut(usize) -> Result<(), MysqlError>,
) -> Result<(PartitionKind, Option<MysqlError>), MysqlError> {
    if definitions.is_empty() {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_ast::ast_errors::ERR_PARTITIONS_MUST_BE_DEFINED
                .generate_with_stack_by_args(&["LIST".into()]),
        ));
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
    let mut duplicate: Option<MysqlError> = None;

    for (ordinal, definition) in definitions.iter().enumerate() {
        match &definition.clause {
            PartitionDefinitionClause::Default => {
                if default_partition.replace(ordinal).is_some() {
                    duplicate.get_or_insert(tidb_terror::MysqlError::from_terror(
                        &tidb_dbterror::ERR_MULTIPLE_DEF_CONST_IN_LIST_PART
                            .generate_with_stack_by_args(&[]),
                    ));
                }
            }
            PartitionDefinitionClause::In(items) => {
                for item in items {
                    if matches!(item, PartitionValue::Default) {
                        if default_partition.replace(ordinal).is_some() {
                            duplicate.get_or_insert(tidb_terror::MysqlError::from_terror(
                                &tidb_dbterror::ERR_MULTIPLE_DEF_CONST_IN_LIST_PART
                                    .generate_with_stack_by_args(&[]),
                            ));
                        }
                        continue;
                    };
                    let PartitionValue::Expr(expr) = item else {
                        return Err(tidb_terror::MysqlError::from_terror(
                            &tidb_dbterror::ERR_VALUES_IS_NOT_INT_TYPE
                                .generate_with_stack_by_args(&[(definition.name.clone())
                                    .to_string()
                                    .into()]),
                        ));
                    };
                    let value = fold_list_value(expr, &definition.name, unsigned, ctx)?;
                    let Some(bits) = value else {
                        if null_partition.replace(ordinal).is_some() {
                            duplicate.get_or_insert(tidb_terror::MysqlError::from_terror(
                                &tidb_dbterror::ERR_MULTIPLE_DEF_CONST_IN_LIST_PART
                                    .generate_with_stack_by_args(&[]),
                            ));
                        }
                        continue;
                    };
                    if seen.insert(bits as u64) {
                        values.push((bits, ordinal));
                    } else {
                        duplicate.get_or_insert(tidb_terror::MysqlError::from_terror(
                            &tidb_dbterror::ERR_MULTIPLE_DEF_CONST_IN_LIST_PART
                                .generate_with_stack_by_args(&[]),
                        ));
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
                return Err(tidb_terror::MysqlError::from_terror(
                    &tidb_ast::ast_errors::ERR_PARTITION_WRONG_VALUES
                        .generate_with_stack_by_args(&["RANGE".into(), "LESS THAN".into()]),
                ));
            }
            _ => {
                return Err(tidb_terror::MysqlError::from_terror(
                    &tidb_ast::ast_errors::ERR_PARTITIONS_MUST_BE_DEFINED
                        .generate_with_stack_by_args(&["LIST".into()]),
                ))
            }
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
    ctx: &tidb_expr::metabuild::Context,
) -> Result<Option<i64>, MysqlError> {
    let rewritten =
        tidb_expr::rewriter::rewrite_expr_resolved(expr, &partition_value_resolver(ctx)).map_err(
            |error| {
                partition_value_failure(
                    error,
                    tidb_terror::MysqlError::from_terror(
                        &tidb_dbterror::ERR_VALUES_IS_NOT_INT_TYPE
                            .generate_with_stack_by_args(&[partition.to_owned().into()]),
                    ),
                )
            },
        )?;
    let mut dual = tidb_chunk::chunk::Chunk::new_empty(&[]);
    dual.set_num_virtual_rows(1);
    match rewritten
        .eval(ctx.get_expr_ctx().get_eval_ctx(), dual.get_row(0))
        .map_err(|_| {
            tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_VALUES_IS_NOT_INT_TYPE
                    .generate_with_stack_by_args(&[partition.to_owned().into()]),
            )
        })? {
        Datum::Int(value) => {
            if unsigned && value < 0 {
                Err(tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_PARTITION_CONST_DOMAIN
                        .generate_with_stack_by_args(&[]),
                ))
            } else {
                Ok(Some(value))
            }
        }
        Datum::UInt(value) => Ok(Some(value as i64)),
        Datum::Null => Ok(None),
        _ => Err(tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_VALUES_IS_NOT_INT_TYPE
                .generate_with_stack_by_args(&[partition.to_owned().into()]),
        )),
    }
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
pub fn partition_expression_is_unsigned(built: &tidb_expr::expression::Expression) -> bool {
    built.static_type().is_some_and(FieldType::is_unsigned)
}

/// One folded RANGE bound, keeping whether it folded signed: Go stores both
/// in the same `int64` slot, and only a SIGNED negative is out of an unsigned
/// expression's domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FoldedRangeBound {
    /// An `int64` result.
    Signed(i64),
    /// A `uint64` result.
    Unsigned(u64),
}

impl FoldedRangeBound {
    /// The value as Go stores it.
    #[must_use]
    pub const fn stored(self) -> i64 {
        match self {
            Self::Signed(value) => value,
            Self::Unsigned(value) => value as i64,
        }
    }
}

/// Go's `EvalSimpleAst` fold of one RANGE bound: a non-integer bound
/// (`'abc'`, `1.5`) is 1697 naming the PARTITION, while a `NULL` bound is
/// 1659 naming the field as the literal text `NULL` -- which reads oddly and
/// is exactly what real TiDB prints.
///
/// The fold reads `ctx`, because a bound's VALUE can depend on the
/// `time_zone`: `UNIX_TIMESTAMP('2020-01-03 15:10:00')` stores `1578064200`
/// under `+00:00` and `1578035400` under `+08:00`.
pub fn fold_range_bound(
    expr: &Expr,
    partition: &str,
    ctx: &tidb_expr::metabuild::Context,
) -> Result<FoldedRangeBound, MysqlError> {
    let rewritten =
        tidb_expr::rewriter::rewrite_expr_resolved(expr, &partition_value_resolver(ctx)).map_err(
            |error| {
                partition_value_failure(
                    error,
                    tidb_terror::MysqlError::from_terror(
                        &tidb_dbterror::ERR_VALUES_IS_NOT_INT_TYPE
                            .generate_with_stack_by_args(&[partition.to_owned().into()]),
                    ),
                )
            },
        )?;
    let mut dual = tidb_chunk::chunk::Chunk::new_empty(&[]);
    dual.set_num_virtual_rows(1);
    let value = rewritten
        .eval(ctx.get_expr_ctx().get_eval_ctx(), dual.get_row(0))
        .map_err(|_| {
            tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_VALUES_IS_NOT_INT_TYPE
                    .generate_with_stack_by_args(&[partition.to_owned().into()]),
            )
        })?;
    match value {
        Datum::Int(value) => Ok(FoldedRangeBound::Signed(value)),
        Datum::UInt(value) => Ok(FoldedRangeBound::Unsigned(value)),
        Datum::Null => Err(tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_NOT_ALLOWED_TYPE_IN_PARTITION
                .generate_with_stack_by_args(&[("NULL".to_owned()).to_string().into()]),
        )),
        _ => Err(tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_VALUES_IS_NOT_INT_TYPE
                .generate_with_stack_by_args(&[partition.to_owned().into()]),
        )),
    }
}
