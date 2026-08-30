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

//! Go `pkg/planner/core/rule/rule_partition_processor.go`.

use crate::logical::{
    BaseLogicalPlan, DataSource, LogicalPartitionUnionAll, LogicalPlan, LogicalTableDual,
    LogicalUnionScan,
};
use crate::plan_base::PlanError;

use super::rule::{conds_to_table_dual, LogicalOptRule, RuleContext};
use super::rule_predicate_simplification::apply_predicate_simplification;

fn resolve_index_merge_hints_for_partition(source: &mut DataSource, partition_name: &str) {
    source.index_merge_hints.retain(|hint| {
        hint.partitions.is_empty()
            || hint
                .partitions
                .iter()
                .any(|partition| partition.eq_ignore_ascii_case(partition_name))
    });
}

fn resolve_index_hints_for_partition(
    source: &mut DataSource,
    partition_name: &str,
) -> Result<(), PlanError> {
    source.index_hints.retain(|hint| {
        hint.partitions.is_empty()
            || hint
                .partitions
                .iter()
                .any(|partition| partition.eq_ignore_ascii_case(partition_name))
    });
    let table = crate::plan_builder::catalog::SourceTable {
        table_name: source.table_name.clone(),
        indexes: source.indexes.clone(),
        pk_is_handle: source.pk_is_handle,
        is_common_handle: !source.common_handle_cols.is_empty(),
        ..Default::default()
    };
    let resolution = crate::access_path::apply_table_index_hints(
        &table,
        &source.public_enumerated_paths,
        &source.ast_index_hints,
        &source.index_hints,
        true,
    )?;
    source.enumerated_paths = resolution.paths;
    source.forced_index_ids = resolution.forced_index_ids;
    source.force_keep_order_index_ids = resolution.force_keep_order_index_ids;
    source.force_no_keep_order_index_ids = resolution.force_no_keep_order_index_ids;
    source.force_keep_order_table_path = resolution.force_keep_order_table_path;
    source.force_no_keep_order_table_path = resolution.force_no_keep_order_table_path;
    source.push_down_lookup_index_ids = resolution.push_down_lookup_index_ids;
    Ok(())
}

fn warn_for_unknown_index_merge_partitions(
    ctx: &RuleContext<'_>,
    source: &DataSource,
    used_partition_names: &std::collections::BTreeSet<String>,
) {
    let Some(sink) = ctx.hint_warning_sink else {
        return;
    };
    for hint in &source.index_merge_hints {
        let unknown = unknown_hint_partitions(&hint.partitions, used_partition_names);
        if !unknown.is_empty() {
            sink.set_hint_warning(&format!(
                "unknown partitions ({}) in optimizer hint {}",
                unknown.join(","),
                hint.restored
            ));
        }
    }
}

fn warn_for_unknown_index_partitions(
    ctx: &RuleContext<'_>,
    source: &DataSource,
    used_partition_names: &std::collections::BTreeSet<String>,
) {
    let Some(sink) = ctx.hint_warning_sink else {
        return;
    };
    for hint in &source.index_hints {
        let unknown = unknown_hint_partitions(&hint.partitions, used_partition_names);
        if !unknown.is_empty() {
            sink.set_hint_warning(&format!(
                "unknown partitions ({}) in optimizer hint {}",
                unknown.join(","),
                hint.restored
            ));
        }
    }
}

fn unknown_hint_partitions(
    partitions: &[String],
    used_partition_names: &std::collections::BTreeSet<String>,
) -> Vec<String> {
    partitions
        .iter()
        .filter(|partition| !used_partition_names.contains(&partition.to_ascii_lowercase()))
        .cloned()
        .collect()
}

/// The ranger-backed half of Go's partition processor. The planner owns the
/// tree rewrite; the catalog implementation owns partition expressions and
/// therefore answers which definition ordinals survive.
pub trait PartitionPruning {
    /// Go `PartitionProcessor.prune`, reduced to its surviving definition
    /// ordinals in table-definition order.
    fn partition_indices(&self, source: &DataSource) -> Result<Vec<usize>, PlanError>;
}

fn make_children(
    ctx: &RuleContext<'_>,
    source: DataSource,
    indices: Vec<usize>,
) -> Result<LogicalPlan, PlanError> {
    let schema = source.base.base.schema().cloned();
    let query_block_offset = source.base.base.query_block_offset();
    let mut children = Vec::with_capacity(indices.len());
    let mut used_partition_names = std::collections::BTreeSet::new();
    for index in indices {
        let Some(physical_id) = source.partition_definition_ids.get(index).copied() else {
            continue;
        };
        let mut child = source.clone_shallow();
        child.partition_def_idx = Some(index);
        child.physical_table_id = physical_id;
        if let Some(partition_name) = source.partition_definition_names.get(index) {
            resolve_index_hints_for_partition(&mut child, partition_name)?;
            resolve_index_merge_hints_for_partition(&mut child, partition_name);
            used_partition_names.insert(partition_name.to_ascii_lowercase());
        }
        children.push(LogicalPlan::DataSource(child));
    }
    warn_for_unknown_index_partitions(ctx, &source, &used_partition_names);
    warn_for_unknown_index_merge_partitions(ctx, &source, &used_partition_names);
    Ok(match children.len() {
        0 => {
            let mut dual = LogicalTableDual::new(
                crate::logical::BaseLogicalPlan::new(
                    ctx.allocator,
                    LogicalTableDual::TYPE,
                    query_block_offset,
                ),
                0,
            );
            dual.base.base.set_schema(schema);
            LogicalPlan::TableDual(dual)
        }
        1 => children.pop().expect("one partition child"),
        _ => {
            let mut union = LogicalPartitionUnionAll::new(crate::logical::BaseLogicalPlan::new(
                ctx.allocator,
                LogicalPartitionUnionAll::TYPE,
                query_block_offset,
            ));
            union.union_all.base.base.set_schema(schema);
            union.union_all.base.set_children(children);
            LogicalPlan::PartitionUnionAll(union)
        }
    })
}

fn prune_data_source(
    ctx: &RuleContext<'_>,
    mut source: DataSource,
) -> Result<LogicalPlan, PlanError> {
    if source.partition_definition_ids.is_empty() {
        return Ok(LogicalPlan::DataSource(source));
    }
    source.pushed_down_conds = apply_predicate_simplification(ctx, source.pushed_down_conds, false);
    source.all_conds = apply_predicate_simplification(ctx, source.all_conds, false);
    if let Some(dual) = conds_to_table_dual(
        ctx,
        &source.all_conds,
        source.base.base.schema(),
        source.base.base.query_block_offset(),
    ) {
        return Ok(dual);
    }
    let indices = ctx.partition_pruning.map_or_else(
        || Ok((0..source.partition_definition_ids.len()).collect()),
        |pruning| pruning.partition_indices(&source),
    )?;
    if let Some(marker) = ctx.plan_cache_marker {
        marker.set_skip_plan_cache("Static partition pruning mode");
    }
    make_children(ctx, source, indices)
}

fn rewrite(ctx: &RuleContext<'_>, mut plan: LogicalPlan) -> Result<LogicalPlan, PlanError> {
    if let LogicalPlan::DataSource(source) = plan {
        return prune_data_source(ctx, source);
    }
    if let LogicalPlan::UnionScan(mut union_scan) = plan {
        let children = union_scan.base.take_children();
        if children.len() != 1 {
            let children = children
                .into_iter()
                .map(|child| rewrite(ctx, child))
                .collect::<Result<Vec<_>, _>>()?;
            union_scan.base.set_children(children);
            return Ok(LogicalPlan::UnionScan(union_scan));
        }
        let child = children.into_iter().next().expect("one UnionScan child");
        let LogicalPlan::DataSource(source) = child else {
            union_scan.base.set_children(vec![rewrite(ctx, child)?]);
            return Ok(LogicalPlan::UnionScan(union_scan));
        };
        let pruned = prune_data_source(ctx, source)?;
        if let LogicalPlan::PartitionUnionAll(mut partition_union) = pruned {
            let children = partition_union.union_all.base.take_children();
            let children = children
                .into_iter()
                .map(|child| {
                    let mut branch = LogicalUnionScan::new(
                        BaseLogicalPlan::new(
                            ctx.allocator,
                            LogicalUnionScan::TYPE,
                            partition_union.union_all.base.base.query_block_offset(),
                        ),
                        union_scan.handle_cols.clone(),
                    );
                    branch.conditions.clone_from(&union_scan.conditions);
                    branch.base.set_children(vec![child]);
                    LogicalPlan::UnionScan(branch)
                })
                .collect();
            partition_union.union_all.base.set_children(children);
            return Ok(LogicalPlan::PartitionUnionAll(partition_union));
        }
        union_scan.base.set_children(vec![pruned]);
        return Ok(LogicalPlan::UnionScan(union_scan));
    }
    // Go deliberately does not enter a CTE definition here.
    if matches!(plan, LogicalPlan::CTE(_)) {
        return Ok(plan);
    }
    let children = plan.base_mut().take_children();
    let children = children
        .into_iter()
        .map(|child| rewrite(ctx, child))
        .collect::<Result<Vec<_>, _>>()?;
    plan.set_children(children);
    Ok(plan)
}

/// Go `PartitionProcessor` at logical rule position 17.
#[derive(Debug)]
pub struct PartitionProcessor;

impl LogicalOptRule for PartitionProcessor {
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        match rewrite(ctx, plan.clone()) {
            Ok(rewritten) => Ok((rewritten, false)),
            Err(error) => Err((plan, error)),
        }
    }

    fn name(&self) -> &'static str {
        "partition_processor"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access_path::PossiblePath;
    use crate::logical::data_source::{DataSourceIndexHint, DataSourceIndexMergeHint};
    use crate::plan_builder::catalog::SourceIndex;

    #[test]
    fn index_merge_hints_are_resolved_per_static_partition() {
        let global = DataSourceIndexMergeHint {
            index_names: vec!["idx_global".to_owned()],
            restored: "USE_INDEX_MERGE(`t`, `idx_global`)".to_owned(),
            ..Default::default()
        };
        let p0 = DataSourceIndexMergeHint {
            index_names: vec!["idx_p0".to_owned()],
            partitions: vec!["p0".to_owned()],
            restored: "USE_INDEX_MERGE(`t` PARTITION(`p0`), `idx_p0`)".to_owned(),
        };
        let p1 = DataSourceIndexMergeHint {
            index_names: vec!["idx_p1".to_owned()],
            partitions: vec!["P1".to_owned()],
            restored: "USE_INDEX_MERGE(`t` PARTITION(`P1`), `idx_p1`)".to_owned(),
        };
        let mut source = DataSource {
            index_merge_hints: vec![global.clone(), p0, p1.clone()],
            ..DataSource::default()
        };

        resolve_index_merge_hints_for_partition(&mut source, "p1");
        assert_eq!(source.index_merge_hints, vec![global, p1]);
        assert_eq!(
            unknown_hint_partitions(
                &["p0".to_owned(), "P1".to_owned()],
                &std::collections::BTreeSet::from(["p1".to_owned()]),
            ),
            vec!["p0"],
            "Go warns in written order and matches partition names case-insensitively"
        );
    }

    #[test]
    fn ordinary_index_hints_are_resolved_per_static_partition() {
        let hinted = |name: &str, partitions: &[&str]| DataSourceIndexHint {
            kind: tidb_ast::IndexHintKind::Use,
            index_names: vec![name.to_owned()],
            partitions: partitions.iter().map(|name| (*name).to_owned()).collect(),
            push_down_lookup: false,
            force_keep_order: false,
            force_no_keep_order: false,
            restored: format!("/*+ USE_INDEX(t, {name}) */"),
        };
        let mut source = DataSource {
            table_name: "t".to_owned(),
            indexes: vec![
                SourceIndex {
                    id: 1,
                    name: "idx_all".to_owned(),
                    ..SourceIndex::default()
                },
                SourceIndex {
                    id: 2,
                    name: "idx_p0".to_owned(),
                    ..SourceIndex::default()
                },
                SourceIndex {
                    id: 3,
                    name: "idx_p1".to_owned(),
                    ..SourceIndex::default()
                },
            ],
            public_enumerated_paths: vec![
                PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                },
                PossiblePath::Index { index: 0 },
                PossiblePath::Index { index: 1 },
                PossiblePath::Index { index: 2 },
            ],
            index_hints: vec![
                hinted("idx_all", &[]),
                hinted("idx_p0", &["p0"]),
                hinted("idx_p1", &["P1"]),
            ],
            ..DataSource::default()
        };

        resolve_index_hints_for_partition(&mut source, "p1").expect("paths resolve");
        assert_eq!(
            source.enumerated_paths,
            vec![
                PossiblePath::Index { index: 0 },
                PossiblePath::Index { index: 2 }
            ]
        );
        assert_eq!(
            source.forced_index_ids,
            std::collections::BTreeSet::from([1, 3])
        );
        assert_eq!(source.index_hints.len(), 2);
    }
}
