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
use tidb_planner_core_base::PlanError;

use super::rule::{conds_to_table_dual, LogicalOptRule, RuleContext};
use super::rule_predicate_simplification::apply_predicate_simplification;

// Go `resolveOptimizeHint` keeps a hint for a partition when one of its
// partitions satisfies `p.String() == partitionName.String()`: the names as
// written, byte for byte, although `checkHintsApplicable` reports unknown
// partitions by `.L`. `PARTITION(P1)` therefore raises no warning for a
// partition named `p1`, yet does not apply to it.

fn resolve_index_merge_hints_for_partition(source: &mut DataSource, partition_name: &str) {
    source.index_merge_hints.retain(|hint| {
        hint.partitions.is_empty()
            || hint
                .partitions
                .iter()
                .any(|partition| partition == partition_name)
    });
}

/// The read-from-storage half of Go `resolveOptimizeHint`: a storage
/// preference scoped to partitions survives only on a named partition, and a
/// partition left preferring both stores warns.
fn resolve_storage_hints_for_partition(
    ctx: &RuleContext<'_>,
    source: &mut DataSource,
    partition_name: &str,
) {
    use crate::logical::data_source::{PREFER_TIFLASH, PREFER_TIKV};
    for store in [PREFER_TIKV, PREFER_TIFLASH] {
        if source.prefer_store_type & store == 0 {
            continue;
        }
        let Some(partitions) = source.prefer_partitions.get(&store) else {
            continue;
        };
        if partitions.is_empty() {
            continue;
        }
        source.prefer_store_type ^= store;
        if partitions
            .iter()
            .any(|partition| partition == partition_name)
        {
            source.prefer_store_type |= store;
        }
    }
    if source.prefer_store_type & PREFER_TIFLASH != 0 && source.prefer_store_type & PREFER_TIKV != 0
    {
        if let Some(sink) = ctx.hint_warning_sink {
            // Go `AppendWarning(errors.NewNoStackError(...))`: ErrUnknown.
            sink.set_hint_warning_from_error(
                1105,
                &format!(
                    "hint `read_from_storage` has conflict storage type for the partition {}",
                    tidb_mysql::to_lowercase(partition_name)
                ),
            );
        }
    }
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
                .any(|partition| partition == partition_name)
    });
    let table = tidb_infoschema::table_source::SourceTable {
        table_name: source.table_name.clone(),
        indexes: source.indexes.clone(),
        pk_is_handle: source.pk_is_handle,
        is_common_handle: source.is_common_handle,
        common_handle_version: source.common_handle_version,
        is_temporary: source.is_temporary,
        is_local_temporary: source.is_local_temporary,
        is_cached: source.is_cached,
        has_affinity: source.has_affinity,
        ..Default::default()
    };
    // Go `resolveOptimizeHint` re-runs `getPossibleAccessPaths` with the
    // per-partition-filtered comment hints AND `ds.AstIndexHints`, the
    // table-syntax `USE/FORCE/IGNORE INDEX` list, which has no partition
    // scope. The two lists are disjoint (`index_hints` holds only comment
    // hints), exactly as at the top-level resolution. Dropping the AST list
    // put the table path back on every partition of `t USE INDEX (ia)`.
    let resolution = crate::access_path::apply_table_index_hints(
        &table,
        &source.public_enumerated_paths,
        &source.ast_index_hints,
        &source.index_hints,
        true,
        source.force_no_index_lookup_push_down,
        source.index_lookup_push_down_session,
        source.tikv_in_isolation_read,
        &source.isolation_read_engines_value,
    )?;
    source.derived_access_paths = None;
    source.enumerated_paths = resolution.paths;
    source.forced_index_ids = resolution.forced_index_ids;
    source.force_keep_order_index_ids = resolution.force_keep_order_index_ids;
    source.force_no_keep_order_index_ids = resolution.force_no_keep_order_index_ids;
    source.force_keep_order_table_path = resolution.force_keep_order_table_path;
    source.force_no_keep_order_table_path = resolution.force_no_keep_order_table_path;
    source.index_lookup_push_down_by = resolution.index_lookup_push_down_by;
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
        append_warn_for_unknown_partitions(sink, &hint.restored, &unknown);
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
        append_warn_for_unknown_partitions(sink, &hint.restored, &unknown);
    }
}

/// Go `checkHintsApplicable`'s read-from-storage check: the unknown
/// partitions of both store preferences, in one warning.
fn warn_for_unknown_storage_partitions(
    ctx: &RuleContext<'_>,
    source: &DataSource,
    used_partition_names: &std::collections::BTreeSet<String>,
) {
    use crate::logical::data_source::{PREFER_TIFLASH, PREFER_TIKV};
    let Some(sink) = ctx.hint_warning_sink else {
        return;
    };
    let mut unknown = Vec::new();
    for store in [PREFER_TIKV, PREFER_TIFLASH] {
        if let Some(partitions) = source.prefer_partitions.get(&store) {
            unknown.extend(unknown_hint_partitions(partitions, used_partition_names));
        }
    }
    append_warn_for_unknown_partitions(sink, "read_from_storage", &unknown);
}

/// Go `appendWarnForUnknownPartitions`: `SetHintWarningFromError` of a
/// `fmt.Errorf`, so the warning is ErrUnknown (1105).
fn append_warn_for_unknown_partitions(
    sink: &dyn super::rule::HintWarningSink,
    hint_name: &str,
    unknown_partitions: &[String],
) {
    if unknown_partitions.is_empty() {
        return;
    }
    sink.set_hint_warning_from_error(
        1105,
        &format!(
            "unknown partitions ({}) in optimizer hint {}",
            unknown_partitions.join(","),
            hint_name
        ),
    );
}

fn unknown_hint_partitions(
    partitions: &[String],
    used_partition_names: &std::collections::BTreeSet<String>,
) -> Vec<String> {
    partitions
        .iter()
        .map(|partition| tidb_mysql::to_lowercase(partition))
        .filter(|partition| !used_partition_names.contains(partition))
        .collect()
}

/// The ranger-backed half of Go's partition processor. The planner owns the
/// tree rewrite; the catalog implementation owns partition expressions and
/// therefore answers which definition ordinals survive.
pub trait PartitionPruning {
    /// Go `PartitionProcessor.prune`, reduced to its surviving definition
    /// ordinals in table-definition order.
    fn partition_indices(&self, source: &DataSource) -> Result<Vec<usize>, PlanError>;

    /// Go's static pruner `PlanID.Add(1)`s before it parses the partition
    /// expression (`generateHashPartitionExpr`, `MakePartitionByFnCol`); the
    /// dynamic-mode access object does not run that code. The default spends
    /// nothing.
    fn spend_partition_expression_plan_id(&self, _source: &DataSource) {}
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
        // Go builds each copy with `NewBaseLogicalPlan`, which draws a plan id,
        // and then `SetID(ds.ID())` puts the original back: the id is spent
        // all the same.
        let _ = crate::logical::BaseLogicalPlan::new(
            ctx.allocator,
            "TableScan",
            query_block_offset,
        );
        let mut child = source.clone_shallow();
        child.derived_access_paths = None;
        child.partition_def_idx = Some(index);
        child.physical_table_id = physical_id;
        if let Some(partition_name) = source.partition_definition_names.get(index) {
            resolve_index_hints_for_partition(&mut child, partition_name)?;
            resolve_index_merge_hints_for_partition(&mut child, partition_name);
            resolve_storage_hints_for_partition(ctx, &mut child, partition_name);
            used_partition_names.insert(tidb_mysql::to_lowercase(partition_name));
        }
        children.push(LogicalPlan::DataSource(child));
    }
    warn_for_unknown_index_partitions(ctx, &source, &used_partition_names);
    warn_for_unknown_index_merge_partitions(ctx, &source, &used_partition_names);
    warn_for_unknown_storage_partitions(ctx, &source, &used_partition_names);
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
    source.derived_access_paths = None;
    source.pushed_down_conds =
        apply_predicate_simplification(ctx, source.pushed_down_conds, false, None);
    source.all_conds = apply_predicate_simplification(ctx, source.all_conds, false, None);
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
        |pruning| {
            pruning.spend_partition_expression_plan_id(&source);
            pruning.partition_indices(&source)
        },
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
    use tidb_planner_util::path::PossiblePath;
    use crate::logical::data_source::{DataSourceIndexHint, DataSourceIndexMergeHint};
    use tidb_infoschema::table_source::SourceIndex;

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
        let source = DataSource {
            index_merge_hints: vec![global.clone(), p0, p1.clone()],
            ..DataSource::default()
        };

        // Go `resolveOptimizeHint` compares `p.String() ==
        // partitionName.String()`: the written names, byte for byte.
        let mut exact = source.clone_shallow();
        resolve_index_merge_hints_for_partition(&mut exact, "P1");
        assert_eq!(exact.index_merge_hints, vec![global.clone(), p1]);
        let mut folded = source.clone_shallow();
        resolve_index_merge_hints_for_partition(&mut folded, "p1");
        assert_eq!(folded.index_merge_hints, vec![global]);
        assert_eq!(
            unknown_hint_partitions(
                &["p0".to_owned(), "P1".to_owned()],
                &std::collections::BTreeSet::from(["p1".to_owned()]),
            ),
            vec!["p0"],
            "Go warns in written order and judges unknown partitions by `.L`"
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
            // These are comment hints (`/*+ USE_INDEX(t, ...) */`), which
            // live in `index_hints` only; `ast_index_hints` holds the
            // table-syntax list, empty for this statement.
            ast_index_hints: Vec::new(),
            isolation_read_engines_value: "tikv".to_owned(),
            tikv_in_isolation_read: true,
            ..DataSource::default()
        };

        // `PARTITION(P1)` applies to the partition written `P1` only.
        let mut folded = source.clone_shallow();
        resolve_index_hints_for_partition(&mut folded, "p1").expect("paths resolve");
        assert_eq!(folded.index_hints.len(), 1);
        resolve_index_hints_for_partition(&mut source, "P1").expect("paths resolve");
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

    /// Go `resolveOptimizeHint`'s read-from-storage half: a store preference
    /// scoped to partitions stays only on a partition written exactly as
    /// named; an unscoped one stays everywhere.
    #[test]
    fn storage_hints_are_resolved_per_static_partition() {
        use crate::logical::data_source::{PREFER_TIFLASH, PREFER_TIKV};
        let allocator = tidb_planner_core_base::PlanIdAllocator::new();
        let ctx = crate::logical::rule_tests::test_context(&allocator);
        let source = DataSource {
            prefer_store_type: PREFER_TIKV,
            prefer_partitions: std::collections::BTreeMap::from([(
                PREFER_TIKV,
                vec!["PÉ".to_owned()],
            )]),
            ..DataSource::default()
        };
        for (partition, kept) in [("PÉ", PREFER_TIKV), ("pé", 0), ("p1", 0)] {
            let mut child = source.clone_shallow();
            resolve_storage_hints_for_partition(&ctx, &mut child, partition);
            assert_eq!(child.prefer_store_type, kept, "{partition}");
        }
        let unscoped = DataSource {
            prefer_store_type: PREFER_TIFLASH,
            prefer_partitions: std::collections::BTreeMap::from([(PREFER_TIFLASH, Vec::new())]),
            ..DataSource::default()
        };
        let mut child = unscoped.clone_shallow();
        resolve_storage_hints_for_partition(&ctx, &mut child, "p1");
        assert_eq!(child.prefer_store_type, PREFER_TIFLASH);
    }

    /// Go `resolveAccessPaths` passes `ds.AstIndexHints` back into
    /// `getPossibleAccessPaths` for each partition: a table-syntax
    /// `t USE INDEX (ia)` keeps the table path out of every partition.
    #[test]
    fn table_syntax_index_hints_survive_per_partition_resolution() {
        let mut source = DataSource {
            table_name: "t".to_owned(),
            indexes: vec![SourceIndex {
                id: 1,
                name: "ia".to_owned(),
                ..SourceIndex::default()
            }],
            public_enumerated_paths: vec![
                PossiblePath::Table {
                    is_int_handle: true,
                    primary_index: None,
                },
                PossiblePath::Index { index: 0 },
            ],
            ast_index_hints: vec![tidb_ast::IndexHint {
                kind: tidb_ast::IndexHintKind::Use,
                scope: tidb_ast::IndexHintScope::All,
                indexes: vec!["ia".to_owned()],
            }],
            isolation_read_engines_value: "tikv".to_owned(),
            tikv_in_isolation_read: true,
            ..DataSource::default()
        };

        resolve_index_hints_for_partition(&mut source, "p0").expect("paths resolve");
        assert_eq!(
            source.enumerated_paths,
            vec![PossiblePath::Index { index: 0 }]
        );
        assert_eq!(
            source.forced_index_ids,
            std::collections::BTreeSet::from([1])
        );
    }
}
