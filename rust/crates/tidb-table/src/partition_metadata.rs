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

//! Go `pkg/table/tables/partition.go`'s `newPartitionedTable` ->
//! `newPartitionExpr`: a table's routing rebuilt from the partition metadata
//! a cluster STORED.
//!
//! This is the LOAD side of partitioning, and Go keeps it apart from the DDL
//! side (`pkg/ddl/partition.go`): the DDL decides whether a written
//! `PARTITION BY` is legal, the loader rebuilds what was accepted and
//! re-judges almost nothing, so a table a Go cluster serves never becomes
//! unreadable here. Both fold a bound the same way, through
//! [`crate::partition_fold`].

use std::collections::HashMap;

use tidb_ast::{PartitionDefinition, PartitionDefinitionClause, PartitionType, PartitionValue};
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_terror::MysqlError;
use tidb_hack::GoCaseCompare;

use crate::partition_routing::{
    PartitionDef, PartitionKind, PartitionSpec, RangeBound, RangeColumnBound, StorageClass,
};

/// Go's `partitionMaxValue`, stored as that literal word and matched back
/// with `strings.EqualFold`.
pub const PARTITION_MAX_VALUE: &str = "MAXVALUE";

/// One stored partition definition, as Go's `model.PartitionDefinition`
/// carries it across a restart: the physical id, the name, and the bounds as
/// SQL TEXT rather than as folded values.
///
/// Go re-parses that text every time it loads the table (`newPartitionExpr`
/// builds a fresh `parser.New()` for exactly this), because the stored form
/// is the user's own `VALUES LESS THAN (...)` / `VALUES IN (...)` and the
/// folding depends on the table's current column types.
#[derive(Clone, Debug, Default)]
pub struct StoredPartitionDefinition {
    /// Go `PartitionDefinition.ID`: the physical table id.
    pub id: i64,
    /// Go `PartitionDefinition.Name`.
    pub name: String,
    /// Go `PartitionDefinition.LessThan`: one bound per partition column,
    /// with the literal `MAXVALUE` kept as text.
    pub less_than: Vec<String>,
    /// Go `PartitionDefinition.InValues`: one tuple per `VALUES IN` entry,
    /// with the literal `DEFAULT` kept as text in the first component.
    pub in_values: Vec<Vec<String>>,
    /// Go `PartitionDefinition.Comment`.
    pub comment: String,
    /// Go `PartitionDefinition.PlacementPolicyRef`: id AND name.
    pub placement_policy: Option<tidb_model::PolicyRefInfo>,
    /// Go `PartitionDefinition.StorageClassTier` and
    /// `StorageClassTransitions`.
    pub storage_class: StorageClass,
}

/// Go `tables.NewPartitionExprBuildCtx` (`table/tables/partition.go:284`):
/// the FIXED context a metadata load folds bounds under.
///
/// `newPartitionExpr` discards the caller's context and builds this one
/// (`partition.go:306`), which is the whole point: routing must not depend on
/// who loaded the table. Threading the SESSION context through instead meant
/// the same stored table could fold to different bounds under
/// `SET time_zone = '+08:00'` than under UTC -- two nodes, or the same node
/// on two connections, routing the same row to different partitions.
///
/// It is deliberately NON-STRICT: truncation is ignored, zero and invalid
/// dates are accepted. A stored bound was already validated when it was
/// written, so a load that re-imposed CREATE-time strictness could refuse a
/// table the cluster is serving.
fn partition_metadata_context() -> tidb_expr::metabuild::Context {
    // Go `tables.NewPartitionExprBuildCtx()` is an `exprstatic.ExprContext`
    // over that evaluation context; every other metabuild option keeps its
    // `NewContext` default.
    let eval = crate::partition_routing::partition_expr_build_ctx();
    tidb_expr::metabuild::Context::new([tidb_expr::metabuild::with_expr_ctx(
        std::sync::Arc::new(tidb_expr::exprstatic::ExprContext::new([
            tidb_expr::exprstatic::with_eval_ctx(std::sync::Arc::new(eval)),
        ])),
    )])
}

/// Go `dataForRangePruning` (`table/tables/partition.go`): how the LOADER
/// reads a scalar RANGE bound back.
///
/// It does not evaluate an expression. It runs `strconv.ParseInt` on the
/// stored text, retries as `ParseUint` when that overflows, and only then
/// gives up. Re-folding the text as an expression instead meant the loader
/// re-applied the CREATE-time rules -- the unsigned-domain refusal (1563),
/// the must-be-integer refusal (1697) -- to metadata a Go cluster had already
/// accepted, so a table this node could not re-derive a verdict for became
/// unreadable rather than merely unpruned.
fn range_bound_from_stored_text(text: &str) -> Option<RangeBound> {
    if text.eq_ignore_ascii_case(PARTITION_MAX_VALUE) {
        return Some(RangeBound::MaxValue);
    }
    if let Ok(value) = text.parse::<i64>() {
        return Some(RangeBound::Value(value));
    }
    // Go's `ErrRange` retry: a bound above `i64::MAX` is stored as its
    // unsigned spelling and kept in the same slot, reinterpreted.
    if let Ok(value) = text.parse::<u64>() {
        return Some(RangeBound::Value(value as i64));
    }
    None
}

/// Rebuild the AST value clause a stored definition was written from, so the
/// bound folding reads it exactly as it read the original `CREATE TABLE`.
///
/// Go keeps `MAXVALUE` and `DEFAULT` as those literal words in the stored
/// text and matches them case-insensitively (`strings.EqualFold`) before
/// parsing, because neither is an expression.
fn stored_clause(
    definition: &StoredPartitionDefinition,
) -> Result<PartitionDefinitionClause, MysqlError> {
    let parse = |text: &String| {
        tidb_model::generated_expr::parse_expression(text).map_err(|error| {
            tidb_terror::MysqlError::from_terror(&tidb_util::misc::syntax_error(
                error.message,
            ))
        })
    };
    if !definition.less_than.is_empty() {
        let mut values = Vec::with_capacity(definition.less_than.len());
        for bound in &definition.less_than {
            values.push(if bound.eq_ignore_ascii_case("MAXVALUE") {
                PartitionValue::MaxValue
            } else {
                PartitionValue::Expr(parse(bound)?)
            });
        }
        return Ok(PartitionDefinitionClause::LessThan(values));
    }
    if !definition.in_values.is_empty() {
        let mut values = Vec::with_capacity(definition.in_values.len());
        for tuple in &definition.in_values {
            // Go `buildListPartitionValueMap`: `DEFAULT` is recognised on the
            // FIRST component only, and the rest of the tuple is not read.
            if tuple
                .first()
                .is_some_and(|first| first.eq_ignore_ascii_case("DEFAULT"))
            {
                values.push(PartitionValue::Default);
                continue;
            }
            values.push(match tuple.as_slice() {
                [single] => PartitionValue::Expr(parse(single)?),
                many => {
                    PartitionValue::Tuple(many.iter().map(parse).collect::<Result<Vec<_>, _>>()?)
                }
            });
        }
        return Ok(PartitionDefinitionClause::In(values));
    }
    Ok(PartitionDefinitionClause::None)
}

/// The stored definitions as the AST nodes the bound folding reads, which is
/// the shape Go reconstructs when it re-parses `LessThan`/`InValues`.
pub fn stored_definitions_as_ast(
    definitions: &[StoredPartitionDefinition],
) -> Result<Vec<PartitionDefinition>, MysqlError> {
    definitions
        .iter()
        .map(|definition| {
            Ok(PartitionDefinition {
                name: definition.name.clone(),
                clause: stored_clause(definition)?,
                options: Vec::new(),
                sub_partitions: Vec::new(),
            })
        })
        .collect()
}

/// Rebuild a table's routing from the partition metadata a Go cluster
/// STORED, which is Go `newPartitionedTable` -> `newPartitionExpr`
/// (`table/tables/partition.go`).
///
/// Go refuses only a definition list that is empty
/// (`table.ErrUnknownPartition`) and otherwise rebuilds; re-running the DDL
/// checks at load would let this node refuse a table a Go cluster is
/// serving, which is the one outcome a loader must not produce.
///
/// `columns` is Go's `PartitionInfo.Columns`, the explicit column list that
/// KEY and the `COLUMNS` variants carry instead of an expression; Go
/// dispatches on it the same way (`len(partCols) < 1` chooses RANGE over
/// RANGE COLUMNS in `generateRangePartitionExpr`).
/// `overlapping_dropping_partition_indices` is the caller's direct snapshot
/// of `PartitionInfo.GetOverlappingDroppingPartitionIdx` while the complete
/// model metadata is available; the rebuilt spec retains it for static
/// partition pruning during online DROP PARTITION.
///
/// # Errors
///
/// 1735 for an empty definition list, and a parse or fold error when the
/// stored expression or a stored bound no longer resolves against the
/// table's current columns.
pub fn partition_spec_from_metadata(
    kind: PartitionType,
    expr_text: &str,
    columns: &[String],
    is_empty_columns: bool,
    definitions: &[StoredPartitionDefinition],
    overlapping_dropping_partition_indices: &[Option<usize>],
    names: &[String],
    types: &[FieldType],
) -> Result<PartitionSpec, MysqlError> {
    // Go builds its own; the caller's context is deliberately not used here.
    let ctx = &partition_metadata_context();
    // Go `newPartitionedTable` (`tables/partition.go:114`): an empty
    // definition list is refused before the expression is touched -- and with
    // `ErrUnknownPartition` (1735), not the 1504 a CREATE would give for
    // `PARTITIONS 0`. The two paths raise different errors for the same
    // shape because they are different code.
    if definitions.is_empty() {
        return Err(tidb_terror::MysqlError::from_terror(
            &crate::table_errors::ERR_UNKNOWN_PARTITION.generate_with_stack_by_args(&[]),
        ));
    }
    let physical = definitions
        .iter()
        .map(|definition| PartitionDef {
            id: definition.id,
            name: definition.name.clone(),
            less_than: definition.less_than.clone(),
            in_values: definition.in_values.clone(),
            comment: definition.comment.clone(),
            placement_policy: definition.placement_policy.clone(),
            storage_class: definition.storage_class.clone(),
        })
        .collect::<Vec<_>>();
    // The COLUMNS forms name their inputs directly. The builders take the
    // parser's qualified paths, whose last component is the column name.
    let column_paths = columns
        .iter()
        .map(|name| vec![name.clone()])
        .collect::<Vec<_>>();
    let column_expr_text = || {
        columns
            .iter()
            .map(|name| format!("`{name}`"))
            .collect::<Vec<_>>()
            .join(",")
    };
    // The COLUMNS forms route off the stored column list rather than a built
    // expression, so the expression slot holds the same placeholder the DDL
    // path puts there.
    let placeholder = || {
        tidb_expr::expression::Expression::Constant(tidb_expr::expression::Constant::new(
            tidb_datatype::Datum::Null,
            FieldType::new(FieldTypeCode::LongLong),
        ))
    };
    let ast_definitions = || stored_definitions_as_ast(definitions);
    match kind {
        // Go `generateKeyPartitionExpr`: KEY builds no expression at all. The
        // stored column list IS the routing input, hashed by the same
        // `key_partition_index` the DDL-built spec routes through, so the
        // rebuild only has to resolve those names back to the table.
        PartitionType::KEY => {
            for column in columns {
                if !names.iter().any(|candidate| candidate.go_lower_eq(column)) {
                    return Err(tidb_terror::MysqlError::from_terror(
                        &tidb_dbterror::ERR_FIELD_NOT_FOUND_PART
                            .generate_with_stack_by_args(&[]),
                    ));
                }
            }
            Ok(PartitionSpec {
                is_empty_columns,
                kind: PartitionKind::Key,
                expr_text: column_expr_text(),
                expr: placeholder(),
                dependencies: columns.to_vec(),
                definitions: physical,
                overlapping_dropping_partition_indices: overlapping_dropping_partition_indices
                    .to_vec(),
            })
        }
        // Go `generateRangePartitionExpr` / `generateListPartitionExpr` with
        // `partCols` non-empty: the bounds are folded through the named
        // columns' own types.
        PartitionType::RANGE if !columns.is_empty() => {
            let (dependencies, field_types, less_than) =
                load_range_columns_bounds(&column_paths, &ast_definitions()?, names, types, ctx)?;
            Ok(PartitionSpec {
                is_empty_columns,
                kind: PartitionKind::RangeColumns {
                    less_than,
                    field_types,
                },
                expr_text: column_expr_text(),
                expr: placeholder(),
                dependencies,
                definitions: physical,
                overlapping_dropping_partition_indices: overlapping_dropping_partition_indices
                    .to_vec(),
            })
        }
        PartitionType::LIST if !columns.is_empty() => {
            let (dependencies, kind) =
                load_list_columns_values(&column_paths, &ast_definitions()?, names, types, ctx)?;
            Ok(PartitionSpec {
                is_empty_columns,
                kind,
                expr_text: column_expr_text(),
                expr: placeholder(),
                dependencies,
                definitions: physical,
                overlapping_dropping_partition_indices: overlapping_dropping_partition_indices
                    .to_vec(),
            })
        }
        // The expression forms: Go parses the stored `PartitionInfo.Expr` and
        // builds it against the table's own columns
        // (`generateHashPartitionExpr` and the `len(partCols) < 1` arms).
        PartitionType::HASH | PartitionType::RANGE | PartitionType::LIST => {
            let expr =
                tidb_model::generated_expr::parse_expression(expr_text).map_err(|error| {
                    tidb_terror::MysqlError::from_terror(&tidb_util::misc::syntax_error(
                        error.message,
                    ))
                })?;
            // Go keeps the stored `PartitionInfo.Expr` text as it is; restoring
            // the parsed form again would bracket it a second time.
            let (built, dependencies) = build_stored_expression(
                &expr,
                names,
                types,
                &ctx.get_expr_ctx().get_eval_ctx().time_zone(),
                ctx.get_expr_ctx().get_eval_ctx().like_default_escape(),
            )?;
            let expr_text = expr_text.to_owned();
            let kind = match kind {
                PartitionType::RANGE => {
                    // Go's loader reads each bound with `strconv`
                    // (`dataForRangePruning`). Only a bound it cannot parse
                    // that way falls back to folding the stored text as an
                    // expression, which is where Go reaches
                    // `fixOldVersionPartitionInfo`.
                    let stored: Option<Vec<RangeBound>> = definitions
                        .iter()
                        .map(|definition| {
                            definition
                                .less_than
                                .first()
                                .and_then(|text| range_bound_from_stored_text(text))
                        })
                        .collect();
                    // The routing flag stays the EXPRESSION's own signedness.
                    // Go carries a second, text-derived `Unsigned` on
                    // `ForRangePruning`, but that one only steers pruning --
                    // Go routes a row through the built `UpperBounds`
                    // comparisons, which use the expression's real type.
                    let unsigned =
                        crate::partition_fold::partition_expression_is_unsigned(&built);
                    let less_than = match stored {
                        Some(less_than) => less_than,
                        None => load_range_bounds(&ast_definitions()?, ctx)?,
                    };
                    PartitionKind::Range {
                        less_than,
                        unsigned,
                    }
                }
                // Go's LOADER (`generateListPartitionExpr`) never dedups: it
                // fills a map, so a repeated value simply ends up owned by
                // the LAST definition that claims it. `checkListPartitionValue`
                // -- the 1495 -- belongs to CREATE only. Refusing here meant
                // this node could not open a table a Go cluster serves.
                PartitionType::LIST => {
                    crate::partition_fold::build_list_values(
                        &built,
                        &ast_definitions()?,
                        ctx,
                        // Go's loader runs no per-definition name or comment
                        // check.
                        &mut |_| Ok(()),
                    )?
                    .0
                }
                _ => PartitionKind::Hash,
            };
            Ok(PartitionSpec {
                is_empty_columns,
                kind,
                expr_text,
                expr: built,
                dependencies,
                definitions: physical,
                overlapping_dropping_partition_indices: overlapping_dropping_partition_indices
                    .to_vec(),
            })
        }
        // Go `newPartitionExpr` returns `nil, nil` for NONE
        // (`tables/partition.go:313`) and every row routes to index 0
        // (`:1453`). This is the shape a table wears WHILE
        // `ALTER TABLE ... PARTITION BY` reorganises it, so refusing it made
        // an ordinary table unreadable for the length of the reorg -- a Go
        // cluster serves it throughout.
        PartitionType::NONE => Ok(PartitionSpec {
            is_empty_columns,
            kind: PartitionKind::None,
            expr_text: expr_text.to_owned(),
            expr: placeholder(),
            dependencies: Vec::new(),
            definitions: physical,
            overlapping_dropping_partition_indices: overlapping_dropping_partition_indices.to_vec(),
        }),
        other => {
            let name = other.sql();
            Err(tidb_terror::MysqlError::unknown(format!(
                "loading a PARTITION BY {name} table is not supported by this node"
            )))
        }
    }
}


/// Go `generateHashPartitionExpr`'s build of the stored `PartitionInfo.Expr`
/// against the table's own columns: parsed and built, never re-judged (the
/// whitelist and the no-column rule are `checkPartitionFuncValid`'s, which
/// only the DDL runs). Returns the evaluable form and the columns it reads.
fn build_stored_expression(
    expr: &tidb_ast::Expr,
    names: &[String],
    types: &[FieldType],
    zone: &tidb_datatype::SessionTimeZone,
    like_default_escape: u8,
) -> Result<(tidb_expr::expression::Expression, Vec<String>), MysqlError> {
    let resolver = crate::generated_column::TableColumnResolver::with_like_default_escape(
        names,
        types,
        zone.clone(),
        like_default_escape,
    );
    let unknown_column = |name: &str| {
        tidb_terror::MysqlError::from_terror(
            &tidb_plannererrors::ERR_UNKNOWN_COLUMN.generate_with_stack_by_args(&[
                tidb_hack::go_to_lower(tidb_hack::go_to_lower(name)).into(),
                ("partition function".to_owned()).to_string().into(),
            ]),
        )
    };
    let built = tidb_expr::rewriter::rewrite_expr_resolved(expr, &resolver).map_err(|_| {
        match resolver.missing_name() {
            // Go's `extractColumns` quotes the folded name.
            Some(name) => unknown_column(&name),
            None => tidb_terror::MysqlError::unknown(
                "this partition expression is not supported yet".to_owned(),
            ),
        }
    })?;
    // A rewrite can succeed while a branch failed to resolve only if the
    // resolver was never consulted, so the missing name is still the
    // authority on 1054.
    if let Some(name) = resolver.missing_name() {
        return Err(unknown_column(&name));
    }
    Ok((built, resolver.dependency_names()))
}

/// Go `generateRangePartitionExpr`'s bounds when a stored bound is not a
/// plain integer (`fixOldVersionPartitionInfo`): each folded as written, the
/// trailing-`MAXVALUE` and unsigned-domain rules left to the DDL that wrote
/// them.
fn load_range_bounds(
    definitions: &[PartitionDefinition],
    ctx: &tidb_expr::metabuild::Context,
) -> Result<Vec<RangeBound>, MysqlError> {
    if definitions.is_empty() {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_ast::ast_errors::ERR_PARTITIONS_MUST_BE_DEFINED
                .generate_with_stack_by_args(&["RANGE".into()]),
        ));
    }
    let mut bounds = Vec::with_capacity(definitions.len());
    for definition in definitions {
        let values = match &definition.clause {
            PartitionDefinitionClause::LessThan(values) => values,
            PartitionDefinitionClause::In(_) => {
                return Err(tidb_terror::MysqlError::from_terror(
                    &tidb_ast::ast_errors::ERR_PARTITION_WRONG_VALUES
                        .generate_with_stack_by_args(&["LIST".into(), "IN".into()]),
                ))
            }
            _ => {
                return Err(tidb_terror::MysqlError::from_terror(
                    &tidb_ast::ast_errors::ERR_PARTITIONS_MUST_BE_DEFINED
                        .generate_with_stack_by_args(&["RANGE".into()]),
                ))
            }
        };
        let [value] = values.as_slice() else {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_ast::ast_errors::ERR_PARTITIONS_MUST_BE_DEFINED
                    .generate_with_stack_by_args(&["RANGE".into()]),
            ));
        };
        bounds.push(match value {
            PartitionValue::MaxValue => RangeBound::MaxValue,
            PartitionValue::Expr(expr) => RangeBound::Value(
                crate::partition_fold::fold_range_bound(expr, &definition.name, ctx)?.stored(),
            ),
            PartitionValue::Default | PartitionValue::Tuple(_) => {
                return Err(tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_VALUES_IS_NOT_INT_TYPE.generate_with_stack_by_args(
                        &[(definition.name.clone()).to_string().into()],
                    ),
                ))
            }
        });
    }
    Ok(bounds)
}

/// The partition columns of a `COLUMNS` form resolved against the table:
/// `checkColumnsPartitionType`'s missing name (1488) is the one thing a load
/// still refuses; its uniqueness and type rules are the DDL's.
fn load_partition_columns(
    columns: &[Vec<String>],
    names: &[String],
    types: &[FieldType],
) -> Result<(Vec<String>, Vec<FieldType>), MysqlError> {
    let mut dependency_names = Vec::with_capacity(columns.len());
    let mut field_types = Vec::with_capacity(columns.len());
    for path in columns {
        let name = path.last().ok_or_else(|| {
            tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_WRONG_TYPE_COLUMN_VALUE.generate_with_stack_by_args(&[]),
            )
        })?;
        let offset = names
            .iter()
            .position(|candidate| candidate.go_lower_eq(name))
            .ok_or_else(|| {
                tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_FIELD_NOT_FOUND_PART.generate_with_stack_by_args(&[]),
                )
            })?;
        dependency_names.push(name.clone());
        field_types.push(types[offset].clone());
    }
    Ok((dependency_names, field_types))
}

/// Go `generateRangePartitionExpr` with `partCols`
/// (`dataForRangeColumnsPruning`): each stored bound folded through its
/// column's type. Go repeats exactly ONE semantic check when loading: the
/// bound must fold to a CONSTANT (`tables/partition.go:423`), raising 1563
/// when it does not.
fn load_range_columns_bounds(
    columns: &[Vec<String>],
    definitions: &[PartitionDefinition],
    names: &[String],
    types: &[FieldType],
    ctx: &tidb_expr::metabuild::Context,
) -> Result<(Vec<String>, Vec<FieldType>, Vec<Vec<RangeColumnBound>>), MysqlError> {
    if columns.is_empty() || definitions.is_empty() {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_ast::ast_errors::ERR_PARTITIONS_MUST_BE_DEFINED
                .generate_with_stack_by_args(&["RANGE".into()]),
        ));
    }
    let (dependency_names, field_types) = load_partition_columns(columns, names, types)?;
    let mut less_than = Vec::with_capacity(definitions.len());
    for definition in definitions {
        let PartitionDefinitionClause::LessThan(values) = &definition.clause else {
            return match &definition.clause {
                PartitionDefinitionClause::In(_) => {
                    Err(tidb_terror::MysqlError::from_terror(
                        &tidb_ast::ast_errors::ERR_PARTITION_WRONG_VALUES
                            .generate_with_stack_by_args(&["LIST".into(), "IN".into()]),
                    ))
                }
                _ => Err(tidb_terror::MysqlError::from_terror(
                    &tidb_ast::ast_errors::ERR_PARTITIONS_MUST_BE_DEFINED
                        .generate_with_stack_by_args(&["RANGE".into()]),
                )),
            };
        };
        let bound = values
            .iter()
            .zip(&field_types)
            .map(|(value, field_type)| match value {
                PartitionValue::MaxValue => Ok(RangeColumnBound::MaxValue),
                PartitionValue::Expr(expr) => {
                    crate::partition_fold::fold_range_column_value(expr, field_type, ctx)
                        .map(RangeColumnBound::Value)
                        .map_err(|_| {
                            tidb_terror::MysqlError::from_terror(
                                &tidb_dbterror::ERR_PARTITION_CONST_DOMAIN
                                    .generate_with_stack_by_args(&[]),
                            )
                        })
                }
                PartitionValue::Default | PartitionValue::Tuple(_) => {
                    Err(tidb_terror::MysqlError::from_terror(
                        &tidb_dbterror::ERR_WRONG_TYPE_COLUMN_VALUE
                            .generate_with_stack_by_args(&[]),
                    ))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        less_than.push(bound);
    }
    Ok((dependency_names, field_types, less_than))
}

/// Go `generateListPartitionExpr` with `partCols`: the stored tuples folded
/// through the columns' types into a map, where a repeated tuple or a second
/// `DEFAULT` simply ends up owned by the LAST definition that claims it (the
/// 1495 is `checkListPartitionValue`'s, the DDL's).
fn load_list_columns_values(
    columns: &[Vec<String>],
    definitions: &[PartitionDefinition],
    names: &[String],
    types: &[FieldType],
    ctx: &tidb_expr::metabuild::Context,
) -> Result<(Vec<String>, PartitionKind), MysqlError> {
    if columns.is_empty() || definitions.is_empty() {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_ast::ast_errors::ERR_PARTITIONS_MUST_BE_DEFINED
                .generate_with_stack_by_args(&["LIST".into()]),
        ));
    }
    let (dependency_names, field_types) = load_partition_columns(columns, names, types)?;
    let mut values = Vec::new();
    let mut keys = HashMap::new();
    let mut default_partition = None;
    for (ordinal, definition) in definitions.iter().enumerate() {
        match &definition.clause {
            PartitionDefinitionClause::Default => default_partition = Some(ordinal),
            PartitionDefinitionClause::In(items) => {
                for item in items {
                    if matches!(item, PartitionValue::Default) {
                        default_partition = Some(ordinal);
                        continue;
                    }
                    let exprs: &[tidb_ast::Expr] = match item {
                        PartitionValue::Expr(expr) if field_types.len() == 1 => {
                            std::slice::from_ref(expr)
                        }
                        PartitionValue::Tuple(exprs) if exprs.len() == field_types.len() => exprs,
                        _ => {
                            return Err(tidb_terror::MysqlError::from_terror(
                                &tidb_ast::ast_errors::ERR_PARTITION_COLUMN_LIST
                                    .generate_with_stack_by_args(&[]),
                            ))
                        }
                    };
                    let tuple = exprs
                        .iter()
                        .zip(&field_types)
                        .map(|(expr, field_type)| {
                            crate::partition_fold::fold_column_value(expr, field_type, ctx)
                        })
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
                    keys.insert(key, ordinal);
                    values.push((tuple, ordinal));
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
