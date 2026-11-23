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

//! Go `PartitionProcessor.prune`'s choice of pruner by partition method
//! (`PruneHashOrKeyPartition`, `PruneRangePartition`, `PruneListPartition`,
//! the RANGE COLUMNS and LIST COLUMNS pruners) over a partitioned table's
//! conditions, and `partitionpruning.PartitionPruning`'s mapping of a
//! dropping partition onto its overlapping one
//! (`handleDroppingForRange`).

use tidb_expr::expression::Expression;

/// What Go's pruners read off `sctx`: the expression context
/// (`GetExprCtx`, `GetEvalCtx`) and the ranger's session inputs.
pub struct PruneContext<'a> {
    /// Go `GetExprCtx()`'s function construction (`expression.PushDownNot`).
    pub builder: &'a dyn tidb_expr::expr_util::builder::FunctionBuilder,
    /// Go `GetEvalCtx()`.
    pub eval: &'a dyn tidb_exprctx::Columns,
    /// Go `SessionVars.RangeMaxSize`.
    pub range_max_size: i64,
    /// Go `GetRangerCtx().RangeFallbackHandler`; a context with none cannot
    /// build ranges, so the ranger-backed pruners keep every partition.
    pub range_fallback_handler: Option<&'a tidb_context::RangeFallbackHandler>,
    /// The ranger's options (`RegardNULLAsPoint`, `OptPrefixIndexSingleScan`,
    /// the fix controls).
    pub ranger_options: tidb_ranger::detacher::RangerOptions,
}

enum ListColumnsLocated {
    Full,
    Location(super::pruners::ListPartitionLocation),
}

/// Go `PartitionInfo.GetOverlappingDroppingPartitionIdx` as an ordinal, `None`
/// for a definition no read reaches.
fn overlapping_dropping_partition_idx(pi: &tidb_model::partition::PartitionInfo, index: usize) -> Option<usize> {
    usize::try_from(pi.get_overlapping_dropping_partition_idx(index as isize))
        .ok()
        .filter(|replacement| *replacement < pi.definitions.len())
}

fn remap_list_columns_location(
    pi: &tidb_model::partition::PartitionInfo,
    location: super::pruners::ListPartitionLocation,
) -> super::pruners::ListPartitionLocation {
    let mut remapped = super::pruners::ListPartitionLocation::new();
    for (index, mut groups) in location {
        let Some(replacement) = overlapping_dropping_partition_idx(pi, index) else {
            continue;
        };
        if replacement != index {
            groups.clear();
            groups.insert(-1);
        }
        remapped.entry(replacement).or_default().extend(groups);
    }
    remapped
}

fn remap_partition_indices(
    pi: &tidb_model::partition::PartitionInfo,
    partition_names: &[String],
    indices: impl IntoIterator<Item = usize>,
) -> Vec<usize> {
    let definitions = pi.definitions.snapshot();
    let mut used_ids = std::collections::BTreeSet::new();
    indices
        .into_iter()
        .filter_map(|index| overlapping_dropping_partition_idx(pi, index))
        .filter(|index| {
            partition_names.is_empty()
                || partition_names
                    .iter()
                    .any(|name| tidb_ast::CiString::new(name) == definitions[*index].name)
        })
        .filter(|index| used_ids.insert(definitions[*index].id))
        .collect()
}

fn locate_list_columns_condition(
    partition: &tidb_table::partition_routing::PartitionExpr,
    pi: &tidb_model::partition::PartitionInfo,
    condition: &Expression,
    columns: &[tidb_expr::column::Column],
    ctx: &PruneContext<'_>,
) -> Result<ListColumnsLocated, tidb_planner_core_base::PlanError> {
    let tidb_table::partition_routing::PartitionKind::ListColumns {
        values,
        default_partition,
        field_types,
        ..
    } = &partition.kind
    else {
        return Ok(ListColumnsLocated::Full);
    };

    match condition {
        Expression::Constant(constant) => match tidb_expr::truthy_of(&constant.value) {
            Ok(Some(false) | None) => Ok(ListColumnsLocated::Location(Default::default())),
            Ok(Some(true)) | Err(_) => Ok(ListColumnsLocated::Full),
        },
        Expression::ScalarFunction(function) => match function.func_name.lowercase() {
            "and" => locate_list_columns_cnf(partition, pi, function.get_args(), columns, ctx),
            "or" => locate_list_columns_dnf(partition, pi, function.get_args(), columns, ctx),
            _ => {
                let referenced = tidb_expr::simple_expr::extract_columns(condition);
                if referenced.len() != 1 {
                    return Ok(ListColumnsLocated::Full);
                }
                let Some(column_index) = columns
                    .iter()
                    .position(|column| column.id == referenced[0].id)
                else {
                    return Ok(ListColumnsLocated::Full);
                };
                let Some(range_fallback_handler) = ctx.range_fallback_handler else {
                    return Ok(ListColumnsLocated::Full);
                };
                let detached =
                    tidb_ranger::detacher::detach_partition_range_with_fallback_handler(
                        std::slice::from_ref(condition),
                        std::slice::from_ref(&columns[column_index]),
                        &[tidb_datatype::UNSPECIFIED_LENGTH],
                        ctx.range_max_size,
                        range_fallback_handler,
                        ctx.ranger_options,
                    )
                    .map_err(|error| {
                        tidb_planner_core_base::PlanError::internal(format!(
                            "LIST COLUMNS range detachment failed: {error:?}"
                        ))
                    })?;
                let location = super::pruners::list_column_location_for_ranges(
                    &detached.ranges,
                    values,
                    *default_partition,
                    field_types,
                    column_index,
                )
                .map_err(|error| {
                    tidb_planner_core_base::PlanError::internal(format!(
                        "LIST COLUMNS location failed: {error:?}"
                    ))
                })?;
                Ok(location.map_or(ListColumnsLocated::Full, |location| {
                    ListColumnsLocated::Location(remap_list_columns_location(pi, location))
                }))
            }
        },
        Expression::Column(_) | Expression::CorrelatedColumn(_) | Expression::ScalarSubQuery(_) => {
            Ok(ListColumnsLocated::Full)
        }
    }
}

fn locate_list_columns_cnf(
    partition: &tidb_table::partition_routing::PartitionExpr,
    pi: &tidb_model::partition::PartitionInfo,
    conditions: &[Expression],
    columns: &[tidb_expr::column::Column],
    ctx: &PruneContext<'_>,
) -> Result<ListColumnsLocated, tidb_planner_core_base::PlanError> {
    let mut location = None;
    for condition in conditions {
        match locate_list_columns_condition(partition, pi, condition, columns, ctx)? {
            ListColumnsLocated::Full => {}
            ListColumnsLocated::Location(found) => {
                if let Some(current) = &mut location {
                    super::pruners::intersect_list_partition_location(current, &found);
                } else {
                    location = Some(found);
                }
            }
        }
    }
    Ok(location.map_or(ListColumnsLocated::Full, ListColumnsLocated::Location))
}

fn locate_list_columns_dnf(
    partition: &tidb_table::partition_routing::PartitionExpr,
    pi: &tidb_model::partition::PartitionInfo,
    conditions: &[Expression],
    columns: &[tidb_expr::column::Column],
    ctx: &PruneContext<'_>,
) -> Result<ListColumnsLocated, tidb_planner_core_base::PlanError> {
    if conditions.is_empty() {
        return Ok(ListColumnsLocated::Full);
    }
    let mut location = super::pruners::ListPartitionLocation::new();
    for condition in conditions {
        match locate_list_columns_condition(partition, pi, condition, columns, ctx)? {
            ListColumnsLocated::Full => return Ok(ListColumnsLocated::Full),
            ListColumnsLocated::Location(found) => {
                super::pruners::union_list_partition_location(&mut location, found);
            }
        }
    }
    Ok(ListColumnsLocated::Location(location))
}

fn list_columns_pruned_ids(
    partition: &tidb_table::partition_routing::PartitionExpr,
    pi: &tidb_model::partition::PartitionInfo,
    conditions: &[Expression],
    columns: &[tidb_expr::column::Column],
    ctx: &PruneContext<'_>,
) -> Result<Option<Vec<i64>>, tidb_planner_core_base::PlanError> {
    Ok(
        match locate_list_columns_cnf(partition, pi, conditions, columns, ctx)? {
            ListColumnsLocated::Full => None,
            ListColumnsLocated::Location(location) => Some(
                pi.definitions
                    .snapshot()
                    .iter()
                    .enumerate()
                    .filter_map(|(index, definition)| {
                        location.contains_key(&index).then_some(definition.id)
                    })
                    .collect(),
            ),
        },
    )
}

/// Go `partitionpruning.PartitionPruning(ctx, tbl, conds, partitionNames,
/// columns, names)` over the catalog's table `table_id`: the ordinals of the
/// partitions the conditions can reach, in definition order.
///
/// # Errors
/// The table is absent or unpartitioned, or a condition cannot be pruned.

pub fn partition_pruning(
    partition: &tidb_table::partition_routing::PartitionExpr,
    pi: &tidb_model::partition::PartitionInfo,
    conds: &[Expression],
    partition_names: &[String],
    table_columns: &[tidb_expr::column::Column],
    ctx: &PruneContext<'_>,
) -> Result<Vec<usize>, tidb_planner_core_base::PlanError> {
    let mut surviving = (0..pi.definitions.len()).collect::<Vec<_>>();
    // Go's hash pruning has no empty-condition shortcut (only the list pruner
    // does): a full range still reaches the BIT-column branch, which drops
    // the hash values the column's width cannot produce.
    let hash = matches!(
        partition.kind,
        tidb_table::partition_routing::PartitionKind::Hash
    );
    if (conds.is_empty() && !hash) || partition.dependencies.is_empty() {
        return Ok(remap_partition_indices(
            pi,
            partition_names,
            0..pi.definitions.len(),
        ));
    }
    let columns = partition
        .dependencies
        .iter()
        .map(|dependency| {
            let dependency = tidb_ast::CiString::new(dependency);
            table_columns
                .iter()
                .find(|column| {
                    column
                        .orig_name
                        .rsplit('.')
                        .next()
                        .is_some_and(|name| tidb_ast::CiString::new(name) == dependency)
                })
                .cloned()
        })
        .collect::<Option<Vec<_>>>();
    let Some(columns) = columns else {
        return Ok(remap_partition_indices(
            pi,
            partition_names,
            0..pi.definitions.len(),
        ));
    };
    let conditions = conds
        .iter()
        .map(|condition| tidb_expr::expr_util::push_not::push_down_not(condition, ctx.builder))
        .collect::<Vec<_>>();
    if matches!(
        partition.kind,
        tidb_table::partition_routing::PartitionKind::ListColumns { .. }
    ) {
        if let Some(ids) = list_columns_pruned_ids(partition, pi, &conditions, &columns, ctx)? {
            surviving.retain(|index| ids.contains(&pi.definitions.get(*index).id));
        }
        return Ok(remap_partition_indices(
            pi,
            partition_names,
            surviving,
        ));
    }
    // Go `PartitionRangeForCNFExpr` prunes a single-column RANGE COLUMNS
    // table by its predicates, and only a wider one through the ranger.
    if let ([column], tidb_table::partition_routing::PartitionKind::RangeColumns { .. }) =
        (columns.as_slice(), &partition.kind)
    {
        if let Some(ids) = super::pruners::single_range_columns_pruned_ids(
            partition,
            pi,
            &conditions,
            column,
            ctx.builder,
            ctx.eval,
        ) {
            surviving.retain(|index| ids.contains(&pi.definitions.get(*index).id));
        }
        return Ok(remap_partition_indices(
            pi,
            partition_names,
            surviving,
        ));
    }
    // Go `PruneRangePartition` prunes `PARTITION BY RANGE (expr)` by its
    // predicates too (`PartitionRangeForCNFExpr`), never through the ranger.
    if matches!(partition.kind, tidb_table::partition_routing::PartitionKind::Range { .. }) {
        if let [column] = columns.as_slice() {
            if let Some(ids) =
                super::pruners::scalar_range_pruned_ids(partition, pi, &conditions, column, ctx.eval)
            {
                surviving.retain(|index| ids.contains(&pi.definitions.get(*index).id));
            }
        }
        return Ok(remap_partition_indices(
            pi,
            partition_names,
            surviving,
        ));
    }
    let lengths = vec![tidb_datatype::UNSPECIFIED_LENGTH; columns.len()];
    let Some(Ok(detached)) = ctx.range_fallback_handler.map(|range_fallback_handler| {
        tidb_ranger::detacher::detach_partition_range_with_fallback_handler(
            &conditions,
            &columns,
            &lengths,
            ctx.range_max_size,
            range_fallback_handler,
            ctx.ranger_options,
        )
    }) else {
        return Ok(remap_partition_indices(
            pi,
            partition_names,
            0..pi.definitions.len(),
        ));
    };
    let pruned =
        super::pruners::pruned_ids_from_ranger(partition, pi, &detached.ranges, ctx.eval)
            .map_err(|error| tidb_planner_core_base::PlanError::internal(format!("{error:?}")))?;
    if let Some(ids) = pruned {
        surviving.retain(|index| ids.contains(&pi.definitions.get(*index).id));
    }
    Ok(remap_partition_indices(
        pi,
        partition_names,
        surviving,
    ))
}

/// The partitions of `source` its conditions and `PARTITION (...)` names
/// leave: Go `PartitionProcessor.prune` over `ds.Table`'s partition
/// expression and `ds.AllConds`, under the statement's expression and ranger
/// context.
///
/// # Errors
/// A partitioned source with no table behind it, or a pruning failure.
pub fn partition_indices(
    rule_ctx: &tidb_planner_core_operator_logicalop::plan_context::RuleContext<'_>,
    source: &tidb_planner_core_operator_logicalop::DataSource,
) -> Result<Vec<usize>, tidb_planner_core_base::PlanError> {
    let table = source.table.as_ref().ok_or_else(|| {
        tidb_planner_core_base::PlanError::internal("partitioned logical table is absent from the catalog")
    })?;
    let partition = table.partition().ok_or_else(|| {
        tidb_planner_core_base::PlanError::internal("partition processor received an unpartitioned table")
    })?;
    let pi = table.meta().read().get_partition_info().ok_or_else(|| {
        tidb_planner_core_base::PlanError::internal("partition processor received an unpartitioned table")
    })?;
    let pi = pi.read();
    let ctx = PruneContext {
        builder: rule_ctx.builder,
        eval: rule_ctx.eval_context,
        range_max_size: rule_ctx.range_max_size,
        range_fallback_handler: rule_ctx.range_fallback_handler,
        ranger_options: tidb_ranger::detacher::RangerOptions {
            opt_prefix_index_single_scan: rule_ctx.opt_prefix_index_single_scan,
            ..rule_ctx.estimator_options.ranger_options()
        },
    };
    partition_pruning(partition, &pi, &source.all_conds, &source.partition_names, &source.table_columns, &ctx)
}

/// Go `generateHashPartitionExpr` and `MakePartitionByFnCol` each
/// `PlanID.Add(1)` before parsing the partition expression, "to make sure
/// some tests will pass" (their words): a HASH and a plain RANGE table spend
/// one plan id per statically pruned source.
pub fn spend_partition_expression_plan_id(
    rule_ctx: &tidb_planner_core_operator_logicalop::plan_context::RuleContext<'_>,
    source: &tidb_planner_core_operator_logicalop::DataSource,
) {
    let spends = source.table.as_ref().and_then(|table| table.partition()).is_some_and(|partition| {
        matches!(
            partition.kind,
            tidb_table::partition_routing::PartitionKind::Hash | tidb_table::partition_routing::PartitionKind::Range { .. }
        )
    });
    if spends {
        rule_ctx.allocator.alloc();
    }
}

#[cfg(test)]
mod list_columns_pruning_tests {
    use super::*;
    use tidb_model::partition::{PartitionDefinition, PartitionInfo};
    use tidb_table::partition_routing::{PartitionKind, PartitionExpr};
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::{column::Column, constant::Constant, scalar_function::ScalarFunction};


    /// The pruning context of a test statement.
    fn prune_context<'a>(
        builder: &'a tidb_expr::expr_util::builder::RealFunctionBuilder<'a, tidb_sessionctx::StmtContext>,
        context: &'a tidb_sessionctx::StmtContext,
    ) -> PruneContext<'a> {
        PruneContext {
            builder,
            eval: context,
            range_max_size: context.range_max_size(),
            range_fallback_handler: Some(context.range_fallback_handler()),
            ranger_options: tidb_ranger::detacher::RangerOptions::default(),
        }
    }

    fn integer_type() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn column(id: i64, unique_id: i64, index: i64) -> Column {
        let mut column = Column::new(unique_id, integer_type());
        column.id = id;
        column.index = index;
        column
    }

    fn equals(column: &Column, value: i64) -> Expression {
        tidb_expr::new_function_base(
            &tidb_exprctx::NoColumns,
            "eq",
            FieldType::new(FieldTypeCode::Tiny),
            vec![
                Expression::Column(column.clone()),
                Expression::Constant(Constant::new(Datum::Int(value), integer_type())),
            ],
        ).unwrap()
    }

    fn or(arguments: Vec<Expression>) -> Expression {
        tidb_expr::new_function_base(
            &tidb_exprctx::NoColumns,
            "or",
            FieldType::new(FieldTypeCode::Tiny),
            arguments,
        ).unwrap()
    }

    fn list_columns_spec() -> PartitionExpr {
        let field_type = integer_type();
        PartitionExpr {
            kind: PartitionKind::ListColumns {
                values: vec![
                    (vec![Datum::Int(1), Datum::Int(5)], 0),
                    (vec![Datum::Int(1), Datum::Int(6)], 0),
                    (vec![Datum::Int(1), Datum::Int(7)], 1),
                    (vec![Datum::Int(9), Datum::Int(9)], 1),
                ],
                keys: Default::default(),
                default_partition: Some(2),
                field_types: vec![field_type.clone(), field_type.clone()],
            },
            expr: Expression::Constant(Constant::new(Datum::Null, field_type)),
            dependencies: vec!["a".to_owned(), "b".to_owned()],
        }
    }

    /// Go `PartitionInfo` of `LIST COLUMNS (a, b)` with p0, p1 and a DEFAULT
    /// p2, whose `InValues` are `values[ordinal]`.
    fn list_columns_pi(values: [&[&[&str]]; 3]) -> PartitionInfo {
        PartitionInfo {
            partition_type: tidb_ast::PartitionType::LIST,
            enable: true,
            columns: vec![CiString::new("a"), CiString::new("b")].into(),
            definitions: (0..3)
                .map(|ordinal| PartitionDefinition {
                    id: 501 + ordinal as i64,
                    name: CiString::new(format!("p{ordinal}")),
                    in_values: values[ordinal]
                        .iter()
                        .map(|tuple| {
                            tuple
                                .iter()
                                .map(|value| (*value).to_owned())
                                .collect::<Vec<_>>()
                                .into()
                        })
                        .collect::<Vec<_>>()
                        .into(),
                    ..Default::default()
                })
                .collect::<Vec<_>>()
                .into(),
            ..Default::default()
        }
    }

    fn list_columns_definitions() -> PartitionInfo {
        list_columns_pi([&[&["1", "5"], &["1", "6"]], &[&["1", "7"], &["9", "9"]], &[&["DEFAULT"]]])
    }

    /// Go's `DROP PARTITION` write-only state over `pi` with `dropping`.
    fn dropping(mut pi: PartitionInfo, dropping: &[usize]) -> PartitionInfo {
        let definitions = pi.definitions.snapshot();
        pi.ddl_action = tidb_model::ActionType::ACTION_DROP_TABLE_PARTITION;
        pi.ddl_state = tidb_model::SchemaState::WRITE_ONLY;
        pi.dropping_definitions = dropping
            .iter()
            .map(|ordinal| definitions[*ordinal].clone())
            .collect::<Vec<_>>()
            .into();
        pi
    }

    #[test]
    fn list_columns_prunes_each_referenced_column_and_intersects_tuple_groups() {
        let spec = list_columns_spec();
        let pi = list_columns_definitions();
        let columns = vec![column(1, 11, 0), column(2, 12, 1)];
        let context = tidb_sessionctx::StmtContext::for_query();
        let builder = tidb_expr::expr_util::builder::RealFunctionBuilder::new(&context);
        let ctx = prune_context(&builder, &context);

        assert_eq!(
            list_columns_pruned_ids(&spec, &pi, &[equals(&columns[1], 7)], &columns, &ctx)
                .unwrap(),
            Some(vec![502, 503]),
            "a predicate on the second partition column is prunable; Go also retains DEFAULT"
        );
        assert_eq!(
            list_columns_pruned_ids(
                &spec,
                &pi,
                &[equals(&columns[0], 1), equals(&columns[1], 9)],
                &columns,
                &ctx,
            )
            .unwrap(),
            Some(vec![503]),
            "CNF intersection must use tuple-group identity, not just partition identity"
        );
        assert_eq!(
            list_columns_pruned_ids(
                &spec,
                &pi,
                &[or(vec![equals(&columns[1], 6), equals(&columns[0], 9)])],
                &columns,
                &ctx,
            )
            .unwrap(),
            Some(vec![501, 502, 503]),
            "DNF union keeps every located tuple group and Go's DEFAULT group"
        );
    }

    #[test]
    fn dropping_partition_ordinals_are_remapped_before_static_children() {
        // p0 and p1 are dropping, so their reads go to the DEFAULT p2.
        let pi = dropping(list_columns_definitions(), &[0, 1]);

        assert_eq!(
            remap_partition_indices(&pi, &[], [0, 1, 2]),
            vec![2],
            "Go deduplicates every dropping definition that overlaps the same readable partition"
        );
        assert!(
            remap_partition_indices(&pi, &["p0".to_owned()], [0]).is_empty(),
            "the explicit partition name is checked against Go's remapped definition"
        );

        let location = crate::rule_partition_processor::pruners::ListPartitionLocation::from([
            (0, std::collections::BTreeSet::from([0, 1])),
            (1, std::collections::BTreeSet::from([2])),
        ]);
        assert_eq!(
            remap_list_columns_location(&pi, location),
            crate::rule_partition_processor::pruners::ListPartitionLocation::from([(
                2,
                std::collections::BTreeSet::from([-1])
            ),]),
            "a remapped LIST COLUMNS location uses Go's special group and merges duplicates"
        );

        // The dropping p0 is itself the DEFAULT partition: no read reaches it.
        let pi = dropping(
            list_columns_pi([&[&["DEFAULT"]], &[&["1", "7"], &["9", "9"]], &[&["1", "5"]]]),
            &[0],
        );
        assert!(
            remap_partition_indices(&pi, &[], [0]).is_empty(),
            "Go skips a dropping definition with no readable overlap"
        );
    }

    #[test]
    fn partition_pruning_fallback_keeps_explicit_partition_names() {
        let spec = list_columns_spec();
        let pi = list_columns_definitions();
        let mut source = tidb_planner_core_operator_logicalop::DataSource::default();
        source.partition_names = vec!["p1".to_owned()];
        source.all_conds = vec![Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::Tiny),
        ))];
        let context = tidb_sessionctx::StmtContext::for_query();
        let builder = tidb_expr::expr_util::builder::RealFunctionBuilder::new(&context);

        assert_eq!(
            partition_pruning(
                &spec,
                &pi,
                &source.all_conds,
                &source.partition_names,
                &source.table_columns,
                &prune_context(&builder, &context),
            )
            .unwrap(),
            vec![1]
        );
    }
}
