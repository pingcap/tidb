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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/planner/core/hint_utils.go`: the optimizer hints that reproduce a
//! physical plan (`GenHintsFromFlatPlan`), used by `EXPLAIN FORMAT='hint'`,
//! the statement summary's `PLAN_HINT` and bindings created from history.

use std::collections::HashSet;

use tidb_ast::{Hint, HintKind, HintTable, LeadingElement};
use tidb_hint::{generate_qb_name, remove_duplicated_hints, NodeType};

use tidb_planner_core_base::LogicalJoinType;
use crate::physical::PhysicalPlan;
use tidb_kv::store_type::StoreType;
use crate::plan_cost_ver2::IndexJoinKind;

/// Table identity the physical plan does not carry itself.
pub trait HintCatalog {
    /// The table's own name for a physical table id.
    fn table_name(&self, table_id: i64) -> Option<String>;

    /// Go `TableInfo.PKIsHandle || TableInfo.IsCommonHandle`.
    fn has_clustered_primary_key(&self, table_id: i64) -> bool;
}

/// Go's `flatOperator.StoreType`: root operators run in TiDB.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Store {
    TiDb,
    TiKv,
    TiFlash,
}

impl Store {
    fn of_reader(store: StoreType) -> Self {
        match store {
            StoreType::TiFlash => Self::TiFlash,
            _ => Self::TiKv,
        }
    }
}

/// Go `GenHintsFromFlatPlan`.
///
/// `block_as_names` is Go's `PlannerSelectBlockAsName`: the alias a derived
/// table's select block was given, indexed by query block offset.
#[must_use]
pub fn gen_hints_from_physical_plan(
    plan: &PhysicalPlan,
    catalog: &dyn HintCatalog,
    block_as_names: &[(String, String)],
) -> Vec<Hint> {
    let (select_plan, node_type) = match plan {
        PhysicalPlan::Dml(dml) => {
            let node_type = match dml.go_operator.as_str() {
                "Update" => NodeType::Update,
                "Delete" => NodeType::Delete,
                _ => NodeType::Select,
            };
            let Some(select) = dml.select_plan.as_deref() else {
                return Vec::new();
            };
            (select, node_type)
        }
        other => (other, NodeType::Select),
    };
    let mut flat = Vec::new();
    let mut ctes = Vec::new();
    flatten(select_plan, Store::TiDb, &mut flat, &mut ctes);

    let context = HintContext {
        catalog,
        block_as_names,
        node_type,
    };
    let mut hints = Vec::new();
    // Go avoids revisiting the joins of one join group while building the
    // leading hint.
    let mut visited_join_ids = HashSet::new();
    for (operator, store) in &flat {
        context.gen_hints_from_single(operator, *store, &mut hints);
        if is_join(operator) {
            if let Some(hint) = context.gen_join_order_hint(operator, &mut visited_join_ids) {
                hints.push(hint);
            }
        }
    }
    for cte in ctes {
        let mut cte_flat = Vec::new();
        let mut nested = Vec::new();
        for part in [Some(&cte.seed_plan), cte.recursive_plan.as_ref()]
            .into_iter()
            .flatten()
        {
            flatten(part, Store::TiDb, &mut cte_flat, &mut nested);
        }
        // The definition itself and every coprocessor operator are skipped.
        for (operator, store) in cte_flat.iter().filter(|(_, store)| *store == Store::TiDb) {
            context.gen_hints_from_single(operator, *store, &mut hints);
            if is_join(operator) {
                if let Some(hint) = context.gen_join_order_hint(operator, &mut visited_join_ids) {
                    hints.push(hint);
                }
            }
        }
    }
    remove_duplicated_hints(&hints)
}

/// Go `FlattenPhysicalPlan(p, false)`'s select-plan order, with each
/// operator's store type.
fn flatten<'a>(
    plan: &'a PhysicalPlan,
    store: Store,
    out: &mut Vec<(&'a PhysicalPlan, Store)>,
    ctes: &mut Vec<&'a crate::physical::PhysicalCTE>,
) {
    out.push((plan, store));
    match plan {
        PhysicalPlan::TableReader(reader) => {
            if let Some(table_plan) = reader.table_plan.as_deref() {
                flatten(table_plan, Store::of_reader(reader.store_type), out, ctes);
            }
        }
        PhysicalPlan::IndexReader(reader) => {
            if let Some(index_plan) = reader.index_plan.as_deref() {
                flatten(index_plan, Store::TiKv, out, ctes);
            }
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            if let Some(index_plan) = reader.index_plan.as_deref() {
                flatten(index_plan, Store::TiKv, out, ctes);
            }
            if let Some(table_plan) = reader.table_plan.as_deref() {
                flatten(table_plan, Store::TiKv, out, ctes);
            }
        }
        PhysicalPlan::IndexMergeReader(reader) => {
            for partial in &reader.partial_plans_raw {
                flatten(partial, Store::TiKv, out, ctes);
            }
            if let Some(table_plan) = reader.table_plan.as_deref() {
                flatten(table_plan, Store::TiKv, out, ctes);
            }
        }
        PhysicalPlan::ShuffleReceiver(receiver) => {
            // The receiver's data source is rendered below it, outside the
            // workers.
            flatten(&receiver.data_source, Store::TiDb, out, ctes);
        }
        PhysicalPlan::CTE(cte) => {
            // Only a CTE executed in TiDB is flattened separately.
            if store == Store::TiDb && !ctes.iter().any(|seen| seen.id_for_storage == cte.id_for_storage)
            {
                ctes.push(cte);
            }
        }
        other => {
            for child in other.children() {
                flatten(child, store, out, ctes);
            }
        }
    }
}

fn is_join(plan: &PhysicalPlan) -> bool {
    matches!(
        plan,
        PhysicalPlan::HashJoin(_)
            | PhysicalPlan::MergeJoin(_)
            | PhysicalPlan::IndexJoin(_)
            | PhysicalPlan::Apply(_)
    )
}

/// Go `PhysicalJoin.GetJoinType()`.
fn join_type(plan: &PhysicalPlan) -> Option<LogicalJoinType> {
    match plan {
        PhysicalPlan::HashJoin(join) => Some(join.join_type),
        PhysicalPlan::MergeJoin(join) => Some(join.join_type),
        PhysicalPlan::IndexJoin(join) => Some(join.join_type),
        PhysicalPlan::Apply(apply) => Some(apply.hash_join.join_type),
        _ => None,
    }
}

const fn is_semi_join(join_type: LogicalJoinType) -> bool {
    matches!(
        join_type,
        LogicalJoinType::Semi
            | LogicalJoinType::AntiSemi
            | LogicalJoinType::LeftOuterSemi
            | LogicalJoinType::AntiLeftOuterSemi
    )
}

fn hint(name: &str, kind: HintKind) -> Hint {
    Hint {
        name: name.to_owned(),
        written_name: None,
        kind,
    }
}

fn plain_table(db: &str, name: &str) -> HintTable {
    HintTable {
        db_name: (!db.is_empty()).then(|| db.to_owned()),
        name: name.to_owned(),
        qb_name: None,
        partitions: Vec::new(),
    }
}

/// The leaf of a pushed-down plan chain, Go's `Plans[0]`.
fn first_leaf(plan: &PhysicalPlan) -> &PhysicalPlan {
    let mut current = plan;
    while let Some(child) = current.children().first() {
        current = child;
    }
    current
}

struct ScanNames {
    db: String,
    table: String,
    keep_order: bool,
    index_name: Option<String>,
    table_id: i64,
}

struct HintContext<'a> {
    catalog: &'a dyn HintCatalog,
    block_as_names: &'a [(String, String)],
    node_type: NodeType,
}

impl HintContext<'_> {
    fn scan_names(&self, scan: &PhysicalPlan) -> Option<ScanNames> {
        match scan {
            PhysicalPlan::TableScan(scan) => Some(ScanNames {
                db: scan.db_name.clone(),
                table: self.table_display_name(scan.table_id, scan.table_as_name.as_deref())?,
                keep_order: scan.keep_order,
                index_name: None,
                table_id: scan.table_id,
            }),
            PhysicalPlan::IndexScan(scan) => Some(ScanNames {
                db: scan.db_name.clone(),
                table: self.table_display_name(scan.table_id, scan.table_as_name.as_deref())?,
                keep_order: scan.keep_order,
                index_name: Some(scan.index_name.clone()),
                table_id: scan.table_id,
            }),
            _ => None,
        }
    }

    /// Go `getTableName`: the alias when there is one.
    fn table_display_name(&self, table_id: i64, as_name: Option<&str>) -> Option<String> {
        match as_name {
            Some(name) if !name.is_empty() => Some(name.to_owned()),
            _ => self.catalog.table_name(table_id),
        }
    }

    fn qb_name(&self, offset: i32) -> Option<String> {
        generate_qb_name(self.node_type, offset).ok()
    }

    /// Go `genHintsFromSingle`.
    fn gen_hints_from_single(&self, plan: &PhysicalPlan, store: Store, res: &mut Vec<Hint>) {
        let Some(qb_name) = self.qb_name(plan.query_block_offset()) else {
            return;
        };
        let qb = || Some(qb_name.clone());
        match plan {
            PhysicalPlan::Limit(_) | PhysicalPlan::TopN(_) => {
                if store == Store::TiKv {
                    res.push(hint("LIMIT_TO_COP", HintKind::Nullary { qb_name: qb() }));
                }
            }
            PhysicalPlan::TableReader(reader) => {
                let Some(table_scan) = reader.table_plan.as_deref().map(first_leaf) else {
                    return;
                };
                let Some(names) = self.scan_names(table_scan) else {
                    return;
                };
                if matches!(reader.store_type, StoreType::TiFlash) {
                    res.push(hint(
                        "READ_FROM_STORAGE",
                        HintKind::ReadFromStorage {
                            qb_name: qb(),
                            groups: vec![(
                                "TIFLASH".to_owned(),
                                vec![plain_table(&names.db, &names.table)],
                            )],
                        },
                    ));
                    return;
                }
                res.push(hint(
                    "USE_INDEX",
                    HintKind::Index {
                        qb_name: qb(),
                        table: plain_table(&names.db, &names.table),
                        indexes: Vec::new(),
                    },
                ));
                if self.catalog.has_clustered_primary_key(names.table_id) {
                    let order = if names.keep_order {
                        "ORDER_INDEX"
                    } else {
                        "NO_ORDER_INDEX"
                    };
                    res.push(hint(
                        order,
                        HintKind::Index {
                            qb_name: qb(),
                            table: plain_table(&names.db, &names.table),
                            indexes: vec!["primary".to_owned()],
                        },
                    ));
                }
            }
            PhysicalPlan::IndexLookUpReader(reader) => {
                if let Some(index_plan) = reader.index_plan.as_deref() {
                    self.push_index_reader_hints(first_leaf(index_plan), &qb_name, res);
                }
            }
            PhysicalPlan::IndexReader(reader) => {
                if let Some(index_plan) = reader.index_plan.as_deref() {
                    self.push_index_reader_hints(first_leaf(index_plan), &qb_name, res);
                }
            }
            PhysicalPlan::IndexMergeReader(reader) => {
                let mut indexes = Vec::new();
                let mut table = String::new();
                for partial in &reader.partial_plans_raw {
                    match self.scan_names(first_leaf(partial)) {
                        Some(ScanNames {
                            index_name: Some(index_name),
                            table: table_name,
                            ..
                        }) => {
                            indexes.push(index_name);
                            table = table_name;
                        }
                        _ => indexes.push("PRIMARY".to_owned()),
                    }
                }
                res.push(hint(
                    "USE_INDEX_MERGE",
                    HintKind::Index {
                        qb_name: qb(),
                        table: plain_table("", &table),
                        indexes,
                    },
                ));
            }
            PhysicalPlan::HashAgg(_) => {
                res.push(hint("HASH_AGG", HintKind::Nullary { qb_name: qb() }));
                if store == Store::TiKv {
                    res.push(hint("AGG_TO_COP", HintKind::Nullary { qb_name: qb() }));
                }
            }
            PhysicalPlan::StreamAgg(_) => {
                res.push(hint("STREAM_AGG", HintKind::Nullary { qb_name: qb() }));
                if store == Store::TiKv {
                    res.push(hint("AGG_TO_COP", HintKind::Nullary { qb_name: qb() }));
                }
            }
            PhysicalPlan::Apply(apply) => {
                if apply.no_decorrelate {
                    let children = plan.children();
                    if children.len() > 1 {
                        let inner_offset = children[1].query_block_offset();
                        if inner_offset > 0 && inner_offset != plan.query_block_offset() {
                            if let Some(inner_qb) = self.qb_name(inner_offset) {
                                res.push(hint(
                                    "NO_DECORRELATE",
                                    HintKind::Nullary {
                                        qb_name: Some(inner_qb),
                                    },
                                ));
                            }
                        }
                    }
                }
            }
            PhysicalPlan::MergeJoin(_) => {
                if let Some(join_hint) = self.gen_join_method_hint(
                    "MERGE_JOIN",
                    plan.query_block_offset(),
                    false,
                    &plan.children().iter().collect::<Vec<_>>(),
                ) {
                    res.push(join_hint);
                }
            }
            PhysicalPlan::HashJoin(join) => {
                let children = plan.children();
                // Semi joins do not support hash_join_[build|probe].
                if is_semi_join(join.join_type) {
                    if let Some(join_hint) = self.gen_join_method_hint(
                        "HASH_JOIN",
                        plan.query_block_offset(),
                        false,
                        &children.iter().collect::<Vec<_>>(),
                    ) {
                        res.push(join_hint);
                    }
                    return;
                }
                let right_is_build = if join.use_outer_to_build {
                    join.inner_child_idx == 0
                } else {
                    join.inner_child_idx != 0
                };
                let (build, probe) = if right_is_build {
                    (&children[1], &children[0])
                } else {
                    (&children[0], &children[1])
                };
                if let Some(join_hint) = self.gen_join_method_hint(
                    "HASH_JOIN_BUILD",
                    plan.query_block_offset(),
                    true,
                    &[build, probe],
                ) {
                    res.push(join_hint);
                } else if let Some(join_hint) = self.gen_join_method_hint(
                    "HASH_JOIN_PROBE",
                    plan.query_block_offset(),
                    true,
                    &[probe, build],
                ) {
                    res.push(join_hint);
                }
            }
            PhysicalPlan::IndexJoin(join) => {
                let name = match join.kind {
                    IndexJoinKind::IndexJoin => "INL_JOIN",
                    IndexJoinKind::IndexHashJoin => "INL_HASH_JOIN",
                    IndexJoinKind::IndexMergeJoin => "INL_MERGE_JOIN",
                };
                let children = plan.children();
                if let Some(join_hint) = self.gen_join_method_hint(
                    name,
                    plan.query_block_offset(),
                    true,
                    &[&children[join.inner_child_idx], &children[1 - join.inner_child_idx]],
                ) {
                    res.push(join_hint);
                }
            }
            _ => {}
        }
    }

    fn push_index_reader_hints(&self, scan: &PhysicalPlan, qb_name: &str, res: &mut Vec<Hint>) {
        let Some(ScanNames {
            db,
            table,
            keep_order,
            index_name: Some(index_name),
            ..
        }) = self.scan_names(scan)
        else {
            return;
        };
        res.push(hint(
            "USE_INDEX",
            HintKind::Index {
                qb_name: Some(qb_name.to_owned()),
                table: plain_table(&db, &table),
                indexes: vec![index_name.clone()],
            },
        ));
        let order = if keep_order {
            "ORDER_INDEX"
        } else {
            "NO_ORDER_INDEX"
        };
        res.push(hint(
            order,
            HintKind::Index {
                qb_name: Some(qb_name.to_owned()),
                table: plain_table(&db, &table),
                indexes: vec![index_name],
            },
        ));
    }

    /// Go `genJoinMethodHintForSinglePhysicalJoin`.
    fn gen_join_method_hint(
        &self,
        name: &str,
        parent_qb_offset: i32,
        only_first_table: bool,
        children: &[&PhysicalPlan],
    ) -> Option<Hint> {
        if parent_qb_offset == -1 {
            return None;
        }
        let (tables, hint_qb_name) = self.gen_hint_tables_for_join_nodes(children, parent_qb_offset);
        let first = tables.iter().flatten().next()?.clone();
        if only_first_table && tables.first().is_none_or(Option::is_none) {
            return None;
        }
        Some(hint(
            name,
            HintKind::Tables {
                qb_name: hint_qb_name,
                tables: vec![first],
            },
        ))
    }

    /// Go `genHintTblForJoinNodes`.
    fn gen_hint_tables_for_join_nodes(
        &self,
        joined_nodes: &[&PhysicalPlan],
        parent_qb_offset: i32,
    ) -> (Vec<Option<HintTable>>, Option<String>) {
        let mut tables: Vec<Option<HintTable>> = Vec::with_capacity(joined_nodes.len());
        let mut offsets: Vec<i32> = Vec::with_capacity(joined_nodes.len());
        let mut guessed = HashSet::new();
        for plan in joined_nodes {
            let Some((offset, guess, table)) =
                self.gen_hint_table_for_single_join_node(plan, parent_qb_offset)
            else {
                offsets.push(-1);
                tables.push(None);
                continue;
            };
            // Two nodes guessed to the same block are likely wrong.
            if guess && !guessed.insert(offset) {
                offsets.push(-1);
                tables.push(None);
                continue;
            }
            offsets.push(offset);
            tables.push(Some(table));
        }
        for (index, table) in tables.iter_mut().enumerate() {
            let Some(table) = table else { continue };
            let offset = offsets[index];
            // The hints go in the first valid place of the statement, so the
            // default block needs no name.
            if (offset <= 1 && self.node_type == NodeType::Select)
                || (offset == 0 && matches!(self.node_type, NodeType::Update | NodeType::Delete))
            {
                continue;
            }
            if let Some(qb_name) = self.qb_name(offset) {
                table.qb_name = Some(qb_name);
            }
        }
        if offsets.contains(&-1) {
            return (tables, None);
        }
        let Some(min_offset) = offsets.iter().copied().min() else {
            return (tables, None);
        };
        if (min_offset > 1 && self.node_type == NodeType::Select)
            || (min_offset > 0 && matches!(self.node_type, NodeType::Update | NodeType::Delete))
        {
            return match self.qb_name(min_offset) {
                Some(qb_name) => (tables, Some(qb_name)),
                None => (Vec::new(), None),
            };
        }
        (tables, None)
    }

    /// Go `genHintTblForSingleJoinNode`.
    fn gen_hint_table_for_single_join_node(
        &self,
        join_node: &PhysicalPlan,
        parent_offset: i32,
    ) -> Option<(i32, bool, HintTable)> {
        let self_offset = join_node.query_block_offset();
        let mut qb_offset = self_offset;
        if qb_offset == -1 {
            return None;
        }
        let mut guess = false;
        let mut names: Option<(String, String)> = None;
        // Sub-queries like `(select * from t) t1` belong to the surrounding block.
        if qb_offset != parent_offset {
            let index = usize::try_from(qb_offset).ok()?;
            let (db, table) = self.block_as_names.get(index)?.clone();
            names = Some((db, table));
            qb_offset = parent_offset;
            // Join reorder sets the offset to -1; guess it from the parent.
            if self_offset > 1 && qb_offset == -1 {
                guess = true;
                qb_offset = self_offset - 1;
            }
        }
        if names.as_ref().is_none_or(|(_, table)| table.is_empty()) {
            guess = false;
            qb_offset = join_node.query_block_offset();
            names = self.extract_table_as_name(join_node);
        }
        let (db, table) = names.filter(|(_, table)| !table.is_empty())?;
        Some((qb_offset, guess, plain_table(&db, &table)))
    }

    /// Go `extractTableAsName`.
    fn extract_table_as_name(&self, plan: &PhysicalPlan) -> Option<(String, String)> {
        if plan.children().len() > 1 {
            return None;
        }
        let leaf_names = |leaf: Option<&PhysicalPlan>| {
            let names = self.scan_names(first_leaf(leaf?))?;
            Some((names.db, names.table))
        };
        match plan {
            PhysicalPlan::TableReader(reader) => leaf_names(reader.table_plan.as_deref()),
            PhysicalPlan::IndexReader(reader) => leaf_names(reader.index_plan.as_deref()),
            PhysicalPlan::IndexLookUpReader(reader) => leaf_names(reader.index_plan.as_deref()),
            PhysicalPlan::Sort(_)
            | PhysicalPlan::Selection(_)
            | PhysicalPlan::UnionScan(_)
            | PhysicalPlan::Projection(_)
            | PhysicalPlan::HashAgg(_)
            | PhysicalPlan::StreamAgg(_) => self.extract_table_as_name(plan.children().first()?),
            _ => None,
        }
    }

    /// Go `genJoinOrderHintFromRootPhysicalJoin`.
    fn gen_join_order_hint(
        &self,
        plan: &PhysicalPlan,
        visited: &mut HashSet<i32>,
    ) -> Option<Hint> {
        if visited.contains(&plan.id()) {
            return None;
        }
        let group = extract_ordered_join_group(plan, visited, 1);
        // Two tables need no join order hint.
        if group.len() <= 2 {
            return None;
        }
        let (tables, hint_qb_name) =
            self.gen_hint_tables_for_join_nodes(&group, plan.query_block_offset());
        if tables.iter().any(Option::is_none) {
            return None;
        }
        Some(hint(
            "LEADING",
            HintKind::Leading {
                qb_name: hint_qb_name,
                elements: tables
                    .into_iter()
                    .flatten()
                    .map(LeadingElement::Table)
                    .collect(),
            },
        ))
    }
}

/// Go `extractOrderedPhysicalJoinGroup`.
fn extract_ordered_join_group<'a>(
    plan: &'a PhysicalPlan,
    visited: &mut HashSet<i32>,
    depth: usize,
) -> Vec<&'a PhysicalPlan> {
    visited.insert(plan.id());
    // A cartesian join breaks the join relationship.
    if let PhysicalPlan::HashJoin(join) = plan {
        if join.equal_conditions.is_empty() && join.na_equal_conditions.is_empty() {
            return Vec::new();
        }
    }
    // The only join types join reorder supports.
    if !matches!(
        join_type(plan),
        Some(LogicalJoinType::Inner | LogicalJoinType::LeftOuter | LogicalJoinType::RightOuter)
    ) {
        return Vec::new();
    }
    let children = plan.children();
    if children.len() < 2 {
        return Vec::new();
    }
    let child0_is_join = is_join(&children[0]);
    let child1_is_join = is_join(&children[1]);
    let mut child_group = Vec::new();
    if child0_is_join {
        child_group = extract_ordered_join_group(&children[0], visited, depth + 1);
    }
    if child1_is_join {
        child_group = extract_ordered_join_group(&children[1], visited, depth + 1);
    }
    // A bushy join is not supported.
    if child0_is_join && child1_is_join {
        return Vec::new();
    }
    // A leaf join starts the group with its two children.
    if !child0_is_join && !child1_is_join {
        return vec![&children[0], &children[1]];
    }
    // A non-leaf join appends its non-join child to the child join's group.
    if child_group.len() < 2 {
        return Vec::new();
    }
    child_group.push(if child0_is_join {
        &children[1]
    } else {
        &children[0]
    });
    child_group
}
