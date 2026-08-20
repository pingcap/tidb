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

//! WHAT ORDER A `FROM` SUBTREE ALREADY PRODUCES, and the merge join a join
//! node can therefore build over it.
//!
//! # The Go this ports
//!
//! `preparePossibleProperties` (`pkg/planner/core/property_cols_prune.go:23`)
//! walks the logical plan bottom-up and asks each node
//! `PreparePossibleProperties(schema, childrenProperties...)` for the column
//! orders its output already carries. Three of those implementations are what
//! this module is:
//!
//!  * `DataSource` (`logical_datasource.go:343`) -- one order per access
//!    path. Only the int-handle branch is ported; see
//!    [`crate::merge_join_plan`] for why the index branch is a separate
//!    increment.
//!  * `LogicalProjection` (`logical_projection.go:334`) -- each child order
//!    rewritten through the projection's BARE-COLUMN expressions, truncated
//!    at the first order column the projection does not carry, and dropped
//!    when nothing survives. A derived table is exactly this node.
//!  * `LogicalJoin` (`logical_join.go:646`) -- see the divergence below.
//!  * `LogicalSelection` (`logical_selection.go:243`) -- passes its child's
//!    orders through unchanged, which is why a `WHERE` inside a derived table
//!    does not stop the propagation here either.
//!
//! `GetMergeJoin` (`physicalop/physical_merge_join.go:50`) then reads the two
//! children's orders off the `LogicalJoin` and requires the join keys to be
//! FULLY covered on the left and matched as a prefix on the right.
//!
//! # CORRECTION (this file used to report something narrower)
//!
//! This module ONCE reported, for a join node, the order that join's own
//! chosen plan produces -- its merge keys when it merged, and NOTHING when it
//! hashed -- rather than Go's union. The reasoning was sound for a one-pass
//! builder: promising an order a hash join then fails to deliver would let a
//! parent merge join silently drop rows.
//!
//! The consequence was measured over the enrolled replay: the bottom join of
//! a three-way tree hashes, therefore reports no order, therefore no parent
//! merge ever forms, therefore no site ever REQUIRES an order -- and the
//! index-join path, which only Go's non-empty property unlocks (`getHashJoins`
//! returns nothing under a non-empty `prop.SortItems`), was reachable at zero
//! sites.
//!
//! Go's escape is TWO PHASES, and this tier now has both:
//!
//!  * PROMISE (logical, this module). `LogicalJoin.PreparePossibleProperties`
//!    reports the UNION of its two children's orders -- `resultProperties :=
//!    leftProperties ++ rightProperties` -- with the null-extended side
//!    dropped for an outer join. It is a claim about which orders the join's
//!    output COULD be produced in, NOT about the plan finally chosen. That
//!    union is what [`possible_properties`] returns again, verbatim.
//!  * VERIFY (physical, [`super::from::build_join`]). Go re-asks a child
//!    through `findBestTask(prop)` and the child either answers with a plan
//!    that satisfies the property or loses on cost. This tier cannot re-ask,
//!    but it CAN check: `build_from` now reports the orders the executor it
//!    just built ACTUALLY produces, and `build_join` keeps its merge plan only
//!    when both children delivered. A promise verification cannot deliver
//!    falls back to the hash join.
//!
//! The verification is not a prediction, so it cannot drift from the build:
//! it reads the built plan's own answer. That is what makes the row-drop
//! hazard the narrowing existed to prevent structurally impossible rather than
//! argued away -- see `from::tests` `a_promise_the_child_cannot_deliver_falls_back`.
//!
//! A merged join's output is sorted by BOTH key lists -- the two are equal in
//! every row it emits -- so both are reported by the DELIVERY side, minus the
//! side an outer join null-extends (Go's own `JoinType` check in the same
//! function).
//!
//! # The residue here is NOT a missing cost comparison -- measured
//!
//! [`merge_join_decision`] answers `Some` whenever `GetMergeJoin` succeeds
//! STRUCTURALLY, where Go builds the merge as one candidate among several and
//! lets `findBestTask`'s `getTaskPlanCost` pick. That gap is real, and the
//! standing reading was that it is what the `join_shape` CASETEST's EXTRA
//! ordered-merge pairs measure. It is not. Every one of the seven was opened
//! against its recording and none is a merge-vs-hash cost decision: in each,
//! TiDB's join TREE puts a different pair of leaves adjacent than this tier's
//! does, so the pair this tier merges is one TiDB never forms at all. The
//! per-pair witnesses, and the two mutation probes that separate the causes,
//! are in [`super::join_reorder`]'s module doc.
//!
//! One of those probes is this function's own: making it return `None`
//! unconditionally takes the `join_shape` 5-tuple from `(229, 144, 88, 80, 7)`
//! to `(229, 69, 88, 0, 0)`. The seven EXTRA pairs go, and all 80 AGREED go
//! with them. A cost gate here would still be Go, but it is not what those
//! seven need, and it cannot reach TiDB's tree from this one.

use super::catalog::split_table_path;
use super::catalog::{Catalog, TableEntry};
use super::predicate_push_down::{offered_conjuncts, Offered};
use crate::merge_join_plan::{MergeJoinKey, MergeJoinPlan};
use std::collections::BTreeSet;
use tidb_ast::{Expr, Join, JoinNode, JoinType, QueryStmt, SelectField, SelectStmt};

/// One relation a `FROM` subtree exposes under a name, and where its columns
/// start within that subtree's row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Relation {
    /// The name a column path qualifies this relation by: a table's alias or
    /// name, or a derived table's alias.
    pub(crate) visible: String,
    /// The relation's column names, in row order.
    pub(crate) columns: Vec<String>,
    /// Where `columns[0]` sits in the subtree's own row.
    pub(crate) offset: usize,
}

/// A relation-qualified column name -- the form a merge key survives column
/// pruning in, since pruning drops columns but never renames a relation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RelColumn {
    /// The relation's visible name.
    pub(crate) relation: String,
    /// The column's own name.
    pub(crate) column: String,
}

/// Go's `PossiblePropertiesInfo` for one `FROM` subtree, in this tier's
/// physical reading (see the module doc).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SideProperties {
    /// The relations the subtree exposes, in row order.
    pub(crate) relations: Vec<Relation>,
    /// The subtree's row width.
    pub(crate) width: usize,
    /// The column orders its output already carries, as offsets into its own
    /// row. Go's `PossiblePropertiesInfo.Orders`.
    pub(crate) orders: Vec<Vec<usize>>,
}

impl SideProperties {
    /// One relation, starting at offset zero.
    fn single(visible: String, columns: Vec<String>, orders: Vec<Vec<usize>>) -> Self {
        let width = columns.len();
        Self {
            relations: vec![Relation {
                visible,
                columns,
                offset: 0,
            }],
            width,
            orders,
        }
    }

    /// The offset within this subtree's row of the column `path` names, or
    /// `None` when no relation answers or when a bare name would answer
    /// twice.
    ///
    /// Ambiguity is refused rather than resolved: Go raises
    /// `ErrAmbiguous` for the same path, and a merge key resolved to the
    /// wrong column would join on data nobody asked about.
    pub(crate) fn offset_of(&self, path: &[String]) -> Option<usize> {
        let (qualifier, name) = match path {
            [name] => (None, name),
            [table, name] => (Some(table), name),
            [_, table, name] => (Some(table), name),
            _ => return None,
        };
        let mut found = None;
        for relation in &self.relations {
            if qualifier.is_some_and(|table| !table.eq_ignore_ascii_case(&relation.visible)) {
                continue;
            }
            let Some(at) = relation
                .columns
                .iter()
                .position(|column| column.eq_ignore_ascii_case(name))
            else {
                continue;
            };
            if found.is_some() {
                return None;
            }
            found = Some(relation.offset + at);
        }
        found
    }

    /// The relation-qualified name of the column at `offset`.
    pub(crate) fn column_at(&self, offset: usize) -> Option<RelColumn> {
        for relation in &self.relations {
            if offset >= relation.offset && offset - relation.offset < relation.columns.len() {
                return Some(RelColumn {
                    relation: relation.visible.clone(),
                    column: relation.columns[offset - relation.offset].clone(),
                });
            }
        }
        None
    }

    /// The two sides of a join, concatenated: the right side's columns follow
    /// the left's, which is the row every join in this tier builds.
    ///
    /// `None` when a relation name would appear twice -- an unaliased
    /// self-join, where no column path can say which side it means.
    fn concat(left: &Self, right: &Self) -> Option<Self> {
        let mut relations = left.relations.clone();
        for relation in &right.relations {
            if relations
                .iter()
                .any(|earlier| earlier.visible.eq_ignore_ascii_case(&relation.visible))
            {
                return None;
            }
            relations.push(Relation {
                visible: relation.visible.clone(),
                columns: relation.columns.clone(),
                offset: relation.offset + left.width,
            });
        }
        Some(Self {
            relations,
            width: left.width + right.width,
            orders: Vec::new(),
        })
    }
}

/// The merge join this join node commits to, with each key named the way it
/// survives column pruning.
pub(crate) struct MergeDecision {
    /// The keys and direction the executor runs on.
    pub(crate) plan: MergeJoinPlan,
    /// The same keys as `(left, right)` relation-qualified names.
    pub(crate) names: Vec<(RelColumn, RelColumn)>,
    /// The complete order the left child must build, including any leading
    /// columns fixed by equality predicates.
    pub(crate) left_required: Vec<usize>,
    /// The complete order the right child must build, including any leading
    /// columns fixed by equality predicates.
    pub(crate) right_required: Vec<usize>,
    /// [`Self::left_required`] named so it survives column pruning.
    pub(crate) left_required_names: Vec<RelColumn>,
    /// [`Self::right_required`] named so it survives column pruning.
    pub(crate) right_required_names: Vec<RelColumn>,
}

/// WHICH of the two questions a properties walk is answering.
///
/// The pair is Go's own split, and the reason the narrowing this module once
/// carried is gone (see the CORRECTION at the top).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Phase {
    /// `PreparePossibleProperties`: which orders this subtree's output COULD
    /// be produced in. A join unions its two children's, because a parent that
    /// wants one of them will re-ask for it.
    Promise,
    /// What the subtree WOULD be built as under the EMPTY property, which is
    /// the only property anything below a derived table is ever asked for. A
    /// join reports the order its own chosen plan produces -- its merge keys
    /// when it merges, and nothing when it hashes.
    ///
    /// This is a conservative LOWER bound: the real build forms its merge
    /// candidates from [`Phase::Promise`] and can therefore deliver MORE than
    /// this says. Under-reporting only ever declines a parent's merge, which
    /// is the safe direction; over-reporting would drop rows.
    Delivered,
}

/// The orders a `FROM` node's output already carries, or `None` when this
/// tier cannot describe the node at all.
///
/// `None` and an empty `orders` are different answers: `None` means no column
/// path can be resolved against the subtree either, so no join above it can
/// name a key.
pub(crate) fn possible_properties(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
    offered: Offered<'_>,
) -> Option<SideProperties> {
    properties(node, catalog, current_db, offered, Phase::Promise)
}

/// [`possible_properties`] in the [`Phase::Delivered`] reading: what the
/// executor `build_from` would build for this node under the EMPTY property
/// actually produces.
pub(crate) fn delivered_properties(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
    offered: Offered<'_>,
) -> Option<SideProperties> {
    properties(node, catalog, current_db, offered, Phase::Delivered)
}

/// Both readings, one walk.
fn properties(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
    offered: Offered<'_>,
    phase: Phase,
) -> Option<SideProperties> {
    match node {
        JoinNode::Table(table_ref) => {
            if table_ref.as_of.is_some() || !table_ref.partitions.is_empty() {
                // A historical read is refused outright below this tier, and
                // a PARTITION restriction reads partition by partition, so
                // the stream is ordered WITHIN each partition and not across
                // them. Go reaches the same answer through
                // `PartitionProcessor`, whose `PartitionUnion` offers no
                // order.
                return None;
            }
            let (database, name) = split_table_path(&table_ref.name, current_db).ok()?;
            let entry = catalog.get_in(database, name)?;
            let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
            let columns: Vec<String> = entry
                .column_list()
                .into_iter()
                .map(|(column, _)| column)
                .collect();
            let orders = table_orders(entry, &columns);
            Some(SideProperties::single(visible, columns, orders))
        }
        // A nested join is offered the SAME `WHERE` conjuncts, which is
        // what `build_from` hands it.
        JoinNode::Join(join) => join_properties(join, catalog, current_db, offered, phase),
        JoinNode::Derived {
            subquery,
            alias,
            lateral,
            column_names,
        } => {
            if *lateral {
                // An Apply, not a join.
                return None;
            }
            derived_properties(
                subquery,
                alias.as_deref()?,
                column_names,
                catalog,
                current_db,
                phase,
            )
        }
    }
}

/// `DataSource.PreparePossibleProperties` for one catalog entry, as offsets
/// into the row `columns` names -- which is the row both this module's
/// properties and [`super::from::build_from`]'s executor lay out.
///
/// Only a real key-value table read WHOLE streams in key order; a view, a
/// memory table and a partitioned table each carry none, and say so while
/// still letting a column path name their columns. A partitioned table reads
/// partition by partition, so its stream is ordered within each partition and
/// not across them -- Go reaches the same answer through `PartitionProcessor`,
/// whose `PartitionUnion` offers no order.
pub(crate) fn table_orders(entry: &TableEntry, columns: &[String]) -> Vec<Vec<usize>> {
    orders_of(entry, columns, crate::merge_join_plan::provided_orders)
}

/// What a whole-table scan of `entry` DELIVERS, in the same row identity
/// [`table_orders`] answers in.
///
/// [`super::from::build_from`]'s leaf reports this and not [`table_orders`].
/// The two agree today and are still separate calls: `table_orders` is the
/// PROMISE a merge join is offered and may grow into Go's index branch, while
/// this is a statement about the `TableFullScan` that was built. Reading the
/// promise on the verify side is not a verification at all -- it agreed with
/// itself by construction, and the module doc's claim that the check "reads
/// the built plan's own answer" was true only by coincidence. See
/// [`crate::merge_join_plan::table_scan_order`] for the row drop that
/// coincidence hid.
pub(crate) fn table_scan_orders(entry: &TableEntry, columns: &[String]) -> Vec<Vec<usize>> {
    orders_of(entry, columns, crate::merge_join_plan::table_scan_order)
}

fn orders_of(
    entry: &TableEntry,
    columns: &[String],
    of_table: fn(&crate::kv_table::KvTable) -> Vec<Vec<usize>>,
) -> Vec<Vec<usize>> {
    let TableEntry::Kv(kv) = entry else {
        return Vec::new();
    };
    if kv.partition().is_some() {
        return Vec::new();
    }
    of_table(kv)
        .into_iter()
        .filter_map(|order| {
            let order: Vec<_> = order
                .into_iter()
                .map_while(|at| {
                    let column = &kv.columns.get(at)?.name;
                    columns
                        .iter()
                        .position(|name| name.eq_ignore_ascii_case(column))
                })
                .collect();
            (!order.is_empty()).then_some(order)
        })
        .collect()
}

/// Whether an executor that delivers `delivered` satisfies a demand for
/// `wanted` -- `wanted` must be a PREFIX of one delivered order.
///
/// A prefix and not a subset: an order `(a, b)` describes rows grouped by `a`
/// first, so a demand for `(b)` alone is not met by it.
pub(crate) fn delivers(delivered: &[Vec<usize>], wanted: &[usize]) -> bool {
    wanted.is_empty() || delivered.iter().any(|order| order.starts_with(wanted))
}

/// The child order a grouped StreamAgg needs, including leading key columns
/// fixed by equality predicates.
///
/// Go's access-path `EqCondCount` removes fixed leading index parts when it
/// matches a physical property. This representation keeps those parts in the
/// scan request -- the record/index walk really delivers them -- and records
/// that the Selection below the aggregate fixes them, so the remaining suffix
/// is ordered by the written group items.
pub(crate) struct AggregationOrder {
    required: tidb_planner::physical_property::PhysicalProperty,
    required_columns: Vec<RelColumn>,
    physical_group_columns: Vec<RelColumn>,
}

impl AggregationOrder {
    /// Re-resolves the order after logical join reorder. Go's property keeps
    /// column `UniqueID`s stable while the join tree moves; this driver uses
    /// row offsets, so the equivalent identity is the relation-qualified
    /// column name captured when the aggregation order was derived.
    pub(crate) fn required_for(
        &self,
        from: &Join,
        catalog: &Catalog,
        current_db: &str,
        offered: Offered<'_>,
    ) -> Option<tidb_planner::physical_property::PhysicalProperty> {
        let source = join_properties(from, catalog, current_db, offered, Phase::Promise)?;
        let (_, desc) = self.required.all_same_order();
        let offsets = self
            .required_columns
            .iter()
            .map(|column| source.offset_of(&[column.relation.clone(), column.column.clone()]));
        Some(child_required_prop(
            offsets.collect::<Option<Vec<_>>>()?.into_iter(),
            desc,
        ))
    }

    /// Whether the committed executor, rather than merely a catalog promise,
    /// delivered the order this grouped aggregation relies on.
    #[must_use]
    pub(crate) fn is_delivered_by(
        &self,
        delivered: &[Vec<usize>],
        scope: &super::FromScope,
    ) -> bool {
        let required_offsets = self
            .required_columns
            .iter()
            .map(|required| {
                scope
                    .tables
                    .iter()
                    .find(|table| table.name.eq_ignore_ascii_case(&required.relation))
                    .and_then(|table| {
                        table
                            .columns
                            .iter()
                            .position(|(column, _)| column.eq_ignore_ascii_case(&required.column))
                            .map(|offset| table.offset + offset)
                    })
            })
            .collect::<Option<Vec<_>>>();
        required_offsets.is_some_and(|required| delivers(delivered, &required))
    }

    /// Group-key offsets in the physical StreamAgg tuple: ordered non-fixed
    /// keys first, then equality-fixed keys. The returned offsets use the
    /// committed (possibly pruned) source scope.
    pub(crate) fn physical_group_offsets(&self, scope: &super::FromScope) -> Option<Vec<usize>> {
        self.physical_group_columns
            .iter()
            .map(|required| {
                scope
                    .tables
                    .iter()
                    .find(|table| table.name.eq_ignore_ascii_case(&required.relation))
                    .and_then(|table| {
                        table
                            .columns
                            .iter()
                            .position(|(column, _)| column.eq_ignore_ascii_case(&required.column))
                            .map(|offset| table.offset + offset)
                    })
            })
            .collect()
    }
}

/// Finds Go's grouped-StreamAgg child property for this query block.
pub(crate) fn aggregation_order(
    select: &SelectStmt,
    from: &Join,
    catalog: &Catalog,
    current_db: &str,
    offered: Offered<'_>,
) -> Option<AggregationOrder> {
    if select.rollup || select.group_by.is_empty() {
        return None;
    }
    let source = join_properties(from, catalog, current_db, offered, Phase::Promise)?;
    let group_offsets: Vec<usize> = select
        .group_by
        .iter()
        .map(|item| match &item.expr {
            Expr::Column(path) => source.offset_of(path),
            _ => None,
        })
        .collect::<Option<_>>()?;
    let mut fixed = BTreeSet::new();
    let mut conjuncts = Vec::new();
    if let Some(predicate) = &select.where_clause {
        crate::plan_trace::collect_and(predicate, &mut conjuncts);
    }
    for conjunct in conjuncts {
        let Expr::Binary(tidb_ast::BinaryOp::Eq, left, right) = conjunct else {
            continue;
        };
        let fixed_path = match (
            strip_aggregation_parens(left),
            strip_aggregation_parens(right),
        ) {
            (Expr::Column(path), value) if aggregation_constant(value) => Some(path),
            (value, Expr::Column(path)) if aggregation_constant(value) => Some(path),
            _ => None,
        };
        if let Some(offset) = fixed_path.and_then(|path| source.offset_of(path)) {
            fixed.insert(offset);
        }
    }

    for order in &source.orders {
        let fixed_prefix = order
            .iter()
            .take_while(|offset| fixed.contains(offset))
            .count();
        // A fixed group key contributes no ordering demand of its own. Go's
        // `EqCondCount` removes it while matching the property, even when the
        // same column is also written in GROUP BY. Counting it a second time
        // would ask `(w,d,o,n)` of an index that already delivers the needed
        // `(w,d,o)` order for `WHERE w=1 GROUP BY w,d,o`.
        let ordered_group_offsets: Vec<usize> = group_offsets
            .iter()
            .copied()
            .filter(|offset| !fixed.contains(offset))
            .collect();
        if !order[fixed_prefix..].starts_with(&ordered_group_offsets) {
            continue;
        }
        let required_offsets = order[..fixed_prefix + ordered_group_offsets.len()].to_vec();
        let required_columns = required_offsets
            .iter()
            .map(|offset| source.column_at(*offset))
            .collect::<Option<Vec<_>>>()?;
        let physical_group_offsets = ordered_group_offsets
            .iter()
            .copied()
            .chain(
                group_offsets
                    .iter()
                    .copied()
                    .filter(|offset| fixed.contains(offset)),
            )
            .collect::<Vec<_>>();
        let physical_group_columns = physical_group_offsets
            .iter()
            .map(|offset| source.column_at(*offset))
            .collect::<Option<Vec<_>>>()?;
        return Some(AggregationOrder {
            required: child_required_prop(required_offsets.iter().copied(), false),
            required_columns,
            physical_group_columns,
        });
    }
    None
}

fn strip_aggregation_parens(mut expr: &Expr) -> &Expr {
    while let Expr::Paren(inner) = expr {
        expr = inner;
    }
    expr
}

fn aggregation_constant(expr: &Expr) -> bool {
    match expr {
        Expr::Paren(inner) => aggregation_constant(inner),
        Expr::Unary(tidb_ast::UnaryOp::Minus | tidb_ast::UnaryOp::Plus, inner) => {
            aggregation_constant(inner)
        }
        Expr::Int(_)
        | Expr::Decimal(_)
        | Expr::Float(_)
        | Expr::Hex(_)
        | Expr::Bit(_)
        | Expr::String(_)
        | Expr::RawString(_)
        | Expr::CharsetString { .. }
        | Expr::Bool(_) => true,
        _ => false,
    }
}

/// [`possible_properties`] for a join node: the two sides' relations
/// concatenated, and the order the join's OWN chosen plan produces.
pub(crate) fn join_properties(
    join: &Join,
    catalog: &Catalog,
    current_db: &str,
    offered: Offered<'_>,
    phase: Phase,
) -> Option<SideProperties> {
    let Some(right_node) = &join.right else {
        // The single-relation wrapper the parser always produces.
        return properties(&join.left, catalog, current_db, offered, phase);
    };
    if join.natural || !join.using.is_empty() {
        // A coalesced join's scope addresses columns by row offset rather
        // than by name, and its output row is not the two sides concatenated.
        return None;
    }
    let left = properties(&join.left, catalog, current_db, offered, phase)?;
    let right = properties(right_node, catalog, current_db, offered, phase)?;
    let mut joined = SideProperties::concat(&left, &right)?;
    let shift = |orders: &[Vec<usize>]| -> Vec<Vec<usize>> {
        orders
            .iter()
            .map(|order| order.iter().map(|at| at + left.width).collect())
            .collect()
    };
    joined.orders = match phase {
        // Go's union, verbatim -- see the CORRECTION at the top of this
        // module for what it replaced and what now makes it safe.
        Phase::Promise => union_orders(join.tp, left.orders.clone(), shift(&right.orders)),
        // The order this join's own chosen plan produces under the EMPTY
        // property, which is the only property a subtree reached this way is
        // asked for. A merge join emits its rows in key order and its two key
        // lists are equal in every row it emits, so both describe the output;
        // a hash join describes none.
        Phase::Delivered => {
            let required = tidb_planner::physical_property::PhysicalProperty::default();
            match decide(join, &left, &right, &joined, &required, offered, None) {
                Some(decision) => union_orders(
                    join.tp,
                    vec![decision.plan.keys.iter().map(|key| key.left).collect()],
                    shift(&[decision.plan.keys.iter().map(|key| key.right).collect()]),
                ),
                None => Vec::new(),
            }
        }
    };
    Some(joined)
}

/// `LogicalJoin.PreparePossibleProperties`'s body: the two children's orders
/// CONCATENATED, minus the side an outer join null-extends.
///
/// Go, verbatim (`logical_join.go:653`):
///
/// ```text
/// if p.JoinType == base.LeftOuterJoin || p.JoinType == base.LeftOuterSemiJoin {
///     rightProperties = nil
/// } else if p.JoinType == base.RightOuterJoin {
///     leftProperties = nil
/// }
/// resultProperties := make([][]*expression.Column, len(leftProperties)+len(rightProperties))
/// ```
///
/// The null-extended side's column carries NULL on the extended rows and is
/// therefore not sorted at all, which is why Go drops it. `JoinType::Cross` is
/// this AST's spelling of the INNER join (see `build_join`'s `JoinKind`
/// mapping), which null-extends neither side.
///
/// The same function serves both phases: the PROMISE unions the two children's
/// own orders, and the DELIVERY of a merged join unions its two key lists,
/// which are equal in every row it emits.
pub(crate) fn union_orders(
    join_type: JoinType,
    left_orders: Vec<Vec<usize>>,
    right_orders: Vec<Vec<usize>>,
) -> Vec<Vec<usize>> {
    let (left_orders, right_orders) = match join_type {
        JoinType::Cross => (left_orders, right_orders),
        JoinType::Left => (left_orders, Vec::new()),
        JoinType::Right => (Vec::new(), right_orders),
    };
    left_orders.into_iter().chain(right_orders).collect()
}

/// [`possible_properties`] for a derived table:
/// `LogicalProjection.PreparePossibleProperties` over its `FROM`.
///
/// The subquery must be a plain, order-preserving `SELECT`. `GROUP BY`,
/// `DISTINCT`, `ORDER BY`, `LIMIT`, a window and a set operation each REPLACE
/// the row order rather than carrying it, and Go gives every one of them its
/// own `PreparePossibleProperties` (`LogicalAggregation`'s group-by
/// permutations, `LogicalSort`'s own items). None of those is ported, so each
/// is refused here rather than described wrongly.
fn derived_properties(
    subquery: &QueryStmt,
    alias: &str,
    column_names: &[String],
    catalog: &Catalog,
    current_db: &str,
    phase: Phase,
) -> Option<SideProperties> {
    let QueryStmt::Select(select) = subquery else {
        return None;
    };
    if !select.group_by.is_empty() {
        return grouped_derived_properties(select, alias, column_names, phase);
    }
    // A derived table's own `WHERE` is the one offered INSIDE it, which is
    // what `run_select_stmt` hands its `FROM`.
    let inner_offered = offered_conjuncts(select.where_clause.as_ref());
    let inner = order_preserving_source(select, catalog, current_db, &inner_offered, phase)?;
    // Go's `oldCols`/`newCols`: the projection's BARE-COLUMN expressions, and
    // where each lands in the projection's own schema.
    let mut columns = Vec::with_capacity(select.fields.fields().len());
    let mut sources = Vec::with_capacity(select.fields.fields().len());
    for field in select.fields.fields() {
        let SelectField::Expr { expr, alias } = field else {
            // A `*` expands against the subquery's own scope, which this
            // decision does not build.
            return None;
        };
        match expr {
            Expr::Column(path) => {
                let name = match alias {
                    Some(alias) => alias.clone(),
                    None => path.last()?.clone(),
                };
                columns.push(name);
                sources.push(inner.offset_of(path));
            }
            // An unaliased expression's result-field name is its SOURCE TEXT
            // in Go, which this decision does not carry. Refusing keeps every
            // name it does resolve a name the statement actually wrote.
            _ => {
                columns.push(alias.clone()?);
                sources.push(None);
            }
        }
    }
    if !column_names.is_empty() {
        // `derived (c1, c2, ...)` renames positionally, and a count mismatch
        // is an error the builder raises later.
        if column_names.len() != columns.len() {
            return None;
        }
        columns = column_names.to_vec();
    }
    Some(SideProperties::single(
        alias.to_owned(),
        columns,
        project_orders(&inner.orders, &sources),
    ))
}

/// Go `LogicalAggregation.PreparePossibleProperties` projected into a
/// derived table's output. A grouped StreamAgg can promise the selected group
/// keys in written order; delivery is reported by the actual materialized
/// SELECT build, so this function never guesses it in the delivered phase.
fn grouped_derived_properties(
    select: &SelectStmt,
    alias: &str,
    column_names: &[String],
    phase: Phase,
) -> Option<SideProperties> {
    if phase == Phase::Delivered
        || select.rollup
        || select.distinct
        || select.having.is_some()
        || select.limit.is_some()
        || !select.windows.is_empty()
    {
        return None;
    }
    let group_paths = select
        .group_by
        .iter()
        .map(|item| match &item.expr {
            Expr::Column(path) => Some(path),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    if !select.order_by.is_empty()
        && (select.order_by.len() != group_paths.len()
            || select
                .order_by
                .iter()
                .zip(&group_paths)
                .any(|(item, group)| {
                    item.desc || !matches!(&item.expr, Expr::Column(path) if path == *group)
                }))
    {
        return None;
    }

    let mut columns = Vec::with_capacity(select.fields.fields().len());
    for field in select.fields.fields() {
        let SelectField::Expr {
            expr,
            alias: field_alias,
        } = field
        else {
            return None;
        };
        columns.push(match expr {
            Expr::Column(path) => field_alias
                .clone()
                .unwrap_or_else(|| path.last().cloned().unwrap_or_default()),
            _ => field_alias.clone()?,
        });
    }
    if !column_names.is_empty() {
        if column_names.len() != columns.len() {
            return None;
        }
        columns = column_names.to_vec();
    }
    let order = group_paths
        .iter()
        .map(|group| {
            select.fields.fields().iter().position(|field| {
                matches!(field, SelectField::Expr { expr: Expr::Column(path), .. } if path == *group)
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(SideProperties::single(
        alias.to_owned(),
        columns,
        vec![order],
    ))
}

/// The source-column name behind a physical join key. Go's projection
/// elimination keeps the original table identity in `MergeJoin` key text,
/// even when SQL name resolution used a base-table or derived-table alias.
/// Returning `None` leaves callers on the ordinary final-scope name.
pub(crate) fn physical_column_trace_name(
    node: &JoinNode,
    column: &RelColumn,
    catalog: &Catalog,
    current_db: &str,
) -> Option<String> {
    let origin = physical_column_origin(node, column, catalog, current_db, true)?;
    Some(format!(
        "{}.{}.{}",
        origin.database.to_lowercase(),
        origin.table.to_lowercase(),
        origin.column.to_lowercase()
    ))
}

/// Whether the base column behind a projection-only output may be NULL.
/// Aggregate/computed outputs have no single base origin and return `None`.
pub(crate) fn physical_column_is_nullable(
    node: &JoinNode,
    column: &RelColumn,
    catalog: &Catalog,
    current_db: &str,
) -> Option<bool> {
    physical_column_origin(node, column, catalog, current_db, false).map(|origin| origin.nullable)
}

/// Whether the source path of a projected join column crossed a grouped
/// derived aggregation. Such keys inherit Go's sorted physical group display;
/// ordinary join keys retain their logical equality order.
pub(crate) fn physical_column_crosses_grouping(
    node: &JoinNode,
    column: &RelColumn,
    catalog: &Catalog,
    current_db: &str,
) -> bool {
    physical_column_origin(node, column, catalog, current_db, true)
        .is_some_and(|origin| origin.crossed_grouping)
}

struct PhysicalColumnOrigin {
    database: String,
    table: String,
    column: String,
    nullable: bool,
    crossed_grouping: bool,
}

fn physical_column_origin(
    node: &JoinNode,
    column: &RelColumn,
    catalog: &Catalog,
    current_db: &str,
    cross_aggregation: bool,
) -> Option<PhysicalColumnOrigin> {
    if let JoinNode::Table(table_ref) = node {
        let (database, name) = split_table_path(&table_ref.name, current_db).ok()?;
        let visible = table_ref.alias.as_deref().unwrap_or(name);
        if !visible.eq_ignore_ascii_case(&column.relation) {
            return None;
        }
        let entry = catalog.get_in(database, name)?;
        if let TableEntry::View(view) = entry {
            let _guard = super::from::ViewDepthGuard::enter(&format!("{database}.{name}")).ok()?;
            let statement = tidb_parser::parse(&view.select_sql).ok()?;
            let tidb_ast::Stmt::Query(query) = statement else {
                return None;
            };
            let QueryStmt::Select(select) = &*query else {
                return None;
            };
            let output = view
                .columns
                .iter()
                .position(|(candidate, _)| candidate.eq_ignore_ascii_case(&column.column))?;
            return select_output_origin(select, output, catalog, database, cross_aggregation);
        }
        let (physical_name, field_type) = entry
            .column_list()
            .into_iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(&column.column))?;
        return Some(PhysicalColumnOrigin {
            database: database.to_owned(),
            table: name.to_owned(),
            column: physical_name,
            nullable: !field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL),
            crossed_grouping: false,
        });
    }
    if let JoinNode::Join(join) = node {
        if let Some(mut origin) =
            physical_column_origin(&join.left, column, catalog, current_db, cross_aggregation)
        {
            if join.tp == JoinType::Right {
                origin.nullable = true;
            }
            return Some(origin);
        }
        return join.right.as_ref().and_then(|right| {
            physical_column_origin(right, column, catalog, current_db, cross_aggregation).map(
                |mut origin| {
                    if join.tp == JoinType::Left {
                        origin.nullable = true;
                    }
                    origin
                },
            )
        });
    }
    let JoinNode::Derived {
        subquery,
        alias: Some(alias),
        lateral: false,
        column_names,
    } = node
    else {
        return None;
    };
    if !alias.eq_ignore_ascii_case(&column.relation) {
        return None;
    }
    let QueryStmt::Select(select) = &**subquery else {
        return None;
    };
    let mut output_names = super::from::derived_field_names(select)?;
    if !column_names.is_empty() {
        if column_names.len() != output_names.len() {
            return None;
        }
        output_names = column_names.to_vec();
    }
    let output = output_names
        .iter()
        .position(|name| name.eq_ignore_ascii_case(&column.column))?;
    select_output_origin(select, output, catalog, current_db, cross_aggregation)
}

fn select_output_origin(
    select: &SelectStmt,
    output: usize,
    catalog: &Catalog,
    current_db: &str,
    cross_aggregation: bool,
) -> Option<PhysicalColumnOrigin> {
    let SelectField::Expr {
        expr: field_expr @ Expr::Column(path),
        ..
    } = select.fields.fields().get(output)?
    else {
        return None;
    };
    if !cross_aggregation
        && !select.group_by.is_empty()
        && !select.group_by.iter().any(|item| &item.expr == field_expr)
    {
        return None;
    }
    let from = select.from.as_ref()?;
    let promised_origin = || {
        let source = join_properties(
            from,
            catalog,
            current_db,
            &offered_conjuncts(select.where_clause.as_ref()),
            Phase::Promise,
        )?;
        source.column_at(source.offset_of(path)?)
    };
    // A projection can retain a directly qualified child column even when
    // the child's complete possible-property model is unavailable (for
    // example, a grouped decorrelation wrapper over a left join). Go's
    // projection elimination still carries that Column's UniqueID through.
    // Resolve that direct path before giving up on the richer property model.
    let direct_origin = || match path.as_slice() {
        [name] => physical_unqualified_column_origin(
            &from.left,
            from.right.as_ref(),
            name,
            catalog,
            current_db,
            cross_aggregation,
        ),
        [.., relation, name] => physical_column_origin(
            &from.left,
            &RelColumn {
                relation: relation.clone(),
                column: name.clone(),
            },
            catalog,
            current_db,
            cross_aggregation,
        )
        .or_else(|| {
            from.right.as_ref().and_then(|right| {
                physical_column_origin(
                    right,
                    &RelColumn {
                        relation: relation.clone(),
                        column: name.clone(),
                    },
                    catalog,
                    current_db,
                    cross_aggregation,
                )
            })
        }),
        _ => None,
    };
    let mut physical = promised_origin()
        .and_then(|origin| {
            physical_column_origin(&from.left, &origin, catalog, current_db, cross_aggregation)
                .or_else(|| {
                    from.right.as_ref().and_then(|right| {
                        physical_column_origin(
                            right,
                            &origin,
                            catalog,
                            current_db,
                            cross_aggregation,
                        )
                    })
                })
        })
        .or_else(direct_origin)?;
    physical.crossed_grouping |= !select.group_by.is_empty();
    Some(physical)
}

/// Resolves one bare projected column against the relations visible in a
/// SELECT's FROM. Every relation that exposes the name counts, including a
/// computed output with no physical origin, so an ambiguous SQL name can
/// never be turned into an arbitrary base-table identity.
fn physical_unqualified_column_origin(
    left: &JoinNode,
    right: Option<&JoinNode>,
    name: &str,
    catalog: &Catalog,
    current_db: &str,
    cross_aggregation: bool,
) -> Option<PhysicalColumnOrigin> {
    let mut matches = Vec::new();
    collect_unqualified_column_origins(
        left,
        name,
        catalog,
        current_db,
        cross_aggregation,
        &mut matches,
    );
    if let Some(right) = right {
        collect_unqualified_column_origins(
            right,
            name,
            catalog,
            current_db,
            cross_aggregation,
            &mut matches,
        );
    }
    if matches.len() != 1 {
        return None;
    }
    matches.pop().flatten()
}

fn collect_unqualified_column_origins(
    node: &JoinNode,
    name: &str,
    catalog: &Catalog,
    current_db: &str,
    cross_aggregation: bool,
    matches: &mut Vec<Option<PhysicalColumnOrigin>>,
) {
    match node {
        JoinNode::Table(table_ref) => {
            let Ok((database, table)) = split_table_path(&table_ref.name, current_db) else {
                return;
            };
            let Some(entry) = catalog.get_in(database, table) else {
                return;
            };
            if entry
                .column_list()
                .iter()
                .any(|(column, _)| column.eq_ignore_ascii_case(name))
            {
                let visible = table_ref.alias.as_deref().unwrap_or(table);
                matches.push(physical_column_origin(
                    node,
                    &RelColumn {
                        relation: visible.to_owned(),
                        column: name.to_owned(),
                    },
                    catalog,
                    current_db,
                    cross_aggregation,
                ));
            }
        }
        JoinNode::Join(join) => {
            collect_unqualified_column_origins(
                &join.left,
                name,
                catalog,
                current_db,
                cross_aggregation,
                matches,
            );
            if let Some(right) = &join.right {
                collect_unqualified_column_origins(
                    right,
                    name,
                    catalog,
                    current_db,
                    cross_aggregation,
                    matches,
                );
            }
        }
        JoinNode::Derived {
            subquery,
            alias: Some(alias),
            lateral: false,
            column_names,
        } => {
            let QueryStmt::Select(select) = &**subquery else {
                return;
            };
            let Some(mut output_names) = super::from::derived_field_names(select) else {
                return;
            };
            if !column_names.is_empty() {
                if column_names.len() != output_names.len() {
                    return;
                }
                output_names = column_names.clone();
            }
            for output_name in output_names
                .into_iter()
                .filter(|output_name| output_name.eq_ignore_ascii_case(name))
            {
                matches.push(physical_column_origin(
                    node,
                    &RelColumn {
                        relation: alias.clone(),
                        column: output_name,
                    },
                    catalog,
                    current_db,
                    cross_aggregation,
                ));
            }
        }
        JoinNode::Derived { .. } => {}
    }
}

/// Each of `orders` rewritten through a projection, where `sources[i]` is the
/// child column the projection's `i`th output reads (and `None` for an output
/// that is not a bare column).
///
/// `LogicalProjection.PreparePossibleProperties`'s inner loop, verbatim:
///
/// ```text
/// for _, col := range childProperty {
///     pos := tmpSchema.ColumnIndex(col)
///     if pos < 0 { break }
///     newChildProperty = append(newChildProperty, newCols[pos])
/// }
/// if len(newChildProperty) != 0 { newProperties = append(...) }
/// ```
///
/// The `break` is load-bearing and is NOT a `continue`: an order `(a, b)`
/// whose `a` the projection drops describes nothing about the projection's
/// output, because rows equal on `b` were only separated by `a`. Truncating
/// leaves the PREFIX that still holds; skipping would claim an order the rows
/// are not in. An order that survives as nothing at all is dropped, which is
/// why the answer can be shorter than `orders`.
fn project_orders(orders: &[Vec<usize>], sources: &[Option<usize>]) -> Vec<Vec<usize>> {
    orders
        .iter()
        .filter_map(|order| {
            let mut projected = Vec::with_capacity(order.len());
            for column in order {
                // Go's `tmpSchema.ColumnIndex`: the FIRST output that reads
                // this column.
                let Some(at) = sources.iter().position(|source| *source == Some(*column)) else {
                    break;
                };
                projected.push(at);
            }
            (!projected.is_empty()).then_some(projected)
        })
        .collect()
}

/// Maps orders the built input actually delivered through a projection's
/// output-to-input column mapping. This is the physical verification sibling
/// of [`project_orders`]'s logical-property use in [`derived_properties`].
pub(crate) fn project_delivered_orders(
    orders: &[Vec<usize>],
    sources: &[Option<usize>],
) -> Vec<Vec<usize>> {
    project_orders(orders, sources)
}

/// The `FROM` of a `SELECT` that carries its source's row order through
/// unchanged, or `None`.
fn order_preserving_source(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    offered: Offered<'_>,
    phase: Phase,
) -> Option<SideProperties> {
    if select.distinct
        || !select.group_by.is_empty()
        || select.having.is_some()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || !select.windows.is_empty()
    {
        return None;
    }
    if select
        .fields
        .fields()
        .iter()
        .any(|field| projected_expr(field).is_some_and(Expr::has_aggregate_flag))
    {
        return None;
    }
    // A `WHERE` is `LogicalSelection`, which passes its child's orders
    // through unchanged.
    join_properties(select.from.as_ref()?, catalog, current_db, offered, phase)
}

/// The expression a select field projects, when it projects one.
fn projected_expr(field: &SelectField) -> Option<&Expr> {
    match field {
        SelectField::Expr { expr, .. } => Some(expr),
        SelectField::Wildcard(_) => None,
    }
}

/// The merge join `GetMergeJoin` would build for this join over these two
/// sides' properties, or `None`.
///
/// `joined` is the two sides concatenated, and is what the key NAMES are read
/// out of -- the offsets themselves cannot be handed to the executor, because
/// column pruning renumbers both sides after the children are built.
pub(crate) fn decide(
    join: &Join,
    left: &SideProperties,
    right: &SideProperties,
    joined: &SideProperties,
    required: &tidb_planner::physical_property::PhysicalProperty,
    offered: Offered<'_>,
    rows: Option<&crate::driver::join_reorder::RowSource>,
) -> Option<MergeDecision> {
    let mut conjuncts = Vec::new();
    if let Some(on) = join.on.as_ref() {
        crate::plan_trace::collect_and(on, &mut conjuncts);
    }
    // Go's `LogicalJoin.PredicatePushDown` moves a `WHERE` equality that
    // spans the two sides into `EqualConditions`, and `GetJoinKeys` -- what
    // `GetMergeJoin` reads -- returns it alongside the ones written in `ON`.
    // The same conjuncts reach the executor through
    // [`super::predicate_push_down::spanning_conjuncts`], and only for an
    // inner, non-coalesced join, which is the gate repeated here. The pairing
    // loop below is itself the "spans both sides" test.
    if join.tp == JoinType::Cross && !join.natural && join.using.is_empty() {
        // A conjunct the `ON` already spells is not counted twice. The two
        // spellings meet whenever a `WHERE` equality became an `ON` -- which
        // is every edge the join reorder rebuilt a tree from
        // (`driver::join_reorder`) -- and the duplicate would ask
        // `GetMergeJoin` to cover the SAME key twice, which no single-column
        // order can do.
        let repeated: Vec<&Expr> = offered
            .iter()
            .filter(|conjunct| !conjuncts.contains(conjunct))
            .collect();
        conjuncts.extend(repeated);
    }
    // Go reads constant columns from the join's functional dependencies, so
    // predicates already pushed into either child still shorten an ordered
    // access path's fixed prefix. `RowSource` is this driver's statement-owned
    // predicate inventory; unlike `offered`, it also retains those leaf-local
    // predicates after decorrelation and join reorder.
    if let Some(rows) = rows {
        for relation in left.relations.iter().chain(&right.relations) {
            if let Some(filters) = rows.filters_for(&relation.visible) {
                conjuncts.extend(filters);
            }
        }
    }
    let mut left_fixed = BTreeSet::new();
    let mut right_fixed = BTreeSet::new();
    for conjunct in &conjuncts {
        let Expr::Binary(tidb_ast::BinaryOp::Eq, lhs, rhs) = conjunct else {
            continue;
        };
        let fixed_path = match (strip_aggregation_parens(lhs), strip_aggregation_parens(rhs)) {
            (Expr::Column(path), value) if aggregation_constant(value) => Some(path),
            (value, Expr::Column(path)) if aggregation_constant(value) => Some(path),
            _ => None,
        };
        let Some(path) = fixed_path else {
            continue;
        };
        if let Some(offset) = left.offset_of(path) {
            left_fixed.insert(offset);
        }
        if let Some(offset) = right.offset_of(path) {
            right_fixed.insert(offset);
        }
    }
    let mut keys = Vec::new();
    for conjunct in conjuncts {
        let Expr::Binary(tidb_ast::BinaryOp::Eq, lhs, rhs) = conjunct else {
            continue;
        };
        let (Expr::Column(left_path), Expr::Column(right_path)) = (&**lhs, &**rhs) else {
            continue;
        };
        // `ON` may write either side first; a key is a key whichever way
        // round it was spelled.
        let pair = match (left.offset_of(left_path), right.offset_of(right_path)) {
            (Some(l), Some(r)) => Some((l, r)),
            _ => match (left.offset_of(right_path), right.offset_of(left_path)) {
                (Some(l), Some(r)) => Some((l, r)),
                _ => None,
            },
        };
        if let Some((l, r)) = pair {
            keys.push(MergeJoinKey { left: l, right: r });
        }
    }
    // A fixed column that is itself a join key remains part of the merge
    // order. Only constant columns BEFORE the first join key are neutral: in
    // `d_w_id=w_id AND w_id=1`, both key columns are fixed but Go still
    // merges on `d_w_id=w_id`; trimming them would erase the join key and
    // incorrectly fall back to an index/hash join.
    for key in &keys {
        left_fixed.remove(&key.left);
        right_fixed.remove(&key.right);
    }
    let left_effective = trim_fixed_prefixes(&left.orders, &left_fixed);
    let right_effective = trim_fixed_prefixes(&right.orders, &right_fixed);
    let plan = crate::merge_join_plan::get_merge_join(
        &keys,
        &left_effective,
        &right_effective,
        required.all_same_order().1,
    )?;
    if !merge_satisfies_required_property(
        join.tp,
        &plan,
        left.width,
        required,
        &left_fixed,
        &right_fixed,
    ) {
        return None;
    }
    let left_keys: Vec<usize> = plan.keys.iter().map(|key| key.left).collect();
    let right_keys: Vec<usize> = plan.keys.iter().map(|key| key.right).collect();
    let left_required = required_order(&left.orders, &left_fixed, &left_keys)?;
    let right_required = required_order(&right.orders, &right_fixed, &right_keys)?;
    let names = plan
        .keys
        .iter()
        .map(|key| {
            Some((
                joined.column_at(key.left)?,
                joined.column_at(key.right + left.width)?,
            ))
        })
        .collect::<Option<Vec<_>>>()?;
    let left_required_names = left_required
        .iter()
        .map(|offset| left.column_at(*offset))
        .collect::<Option<Vec<_>>>()?;
    let right_required_names = right_required
        .iter()
        .map(|offset| right.column_at(*offset))
        .collect::<Option<Vec<_>>>()?;
    Some(MergeDecision {
        plan,
        names,
        left_required,
        right_required,
        left_required_names,
        right_required_names,
    })
}

/// Go `PhysicalMergeJoin.tryToGetChildReqProp`, expressed in this driver's
/// joined-row offsets. A leading constant in `required` is the local
/// equivalent of the access path suffix Go adds for `EqCondCount`: it does
/// not participate in the varying order and is skipped before join-key
/// compatibility is checked.
fn merge_satisfies_required_property(
    join_type: JoinType,
    plan: &MergeJoinPlan,
    left_width: usize,
    required: &tidb_planner::physical_property::PhysicalProperty,
    left_constants: &BTreeSet<usize>,
    right_constants: &BTreeSet<usize>,
) -> bool {
    if required.is_sort_item_empty() {
        return true;
    }
    if !required.all_same_order().0 {
        return false;
    }
    let left_keys = plan
        .keys
        .iter()
        .map(|key| key.left as i64)
        .collect::<Vec<_>>();
    let right_keys = plan
        .keys
        .iter()
        .map(|key| (key.right + left_width) as i64)
        .collect::<Vec<_>>();
    let left_constants = left_constants
        .iter()
        .map(|offset| *offset as i64)
        .collect::<BTreeSet<_>>();
    let right_constants = right_constants
        .iter()
        .map(|offset| (*offset + left_width) as i64)
        .collect::<BTreeSet<_>>();
    let compatible = |keys: &[i64], constants: &BTreeSet<i64>| {
        let mut key_pos = 0;
        for item in &required.sort_items {
            if constants.contains(&item.col) {
                continue;
            }
            let mut matched = false;
            while let Some(key) = keys.get(key_pos) {
                key_pos += 1;
                if item.col == *key {
                    matched = true;
                    break;
                }
                if !constants.contains(key) {
                    return false;
                }
            }
            if !matched {
                return false;
            }
        }
        true
    };
    let match_left = compatible(&left_keys, &left_constants);
    let match_right = compatible(&right_keys, &right_constants);
    (match_left || match_right)
        && !(match_right && join_type == JoinType::Left)
        && !(match_left && join_type == JoinType::Right)
}

/// Removes only the leading columns whose equality predicates make them
/// constant for every row. The remaining suffix is the order a merge join
/// can compare; the original prefix is restored in [`required_order`] so the
/// child still chooses the access path and range that provide it.
fn trim_fixed_prefixes(orders: &[Vec<usize>], fixed: &BTreeSet<usize>) -> Vec<Vec<usize>> {
    orders
        .iter()
        .filter_map(|order| {
            let prefix = order
                .iter()
                .take_while(|offset| fixed.contains(offset))
                .count();
            (prefix < order.len()).then(|| order[prefix..].to_vec())
        })
        .collect()
}

/// Finds the original child order whose fixed-prefix-trimmed suffix starts
/// with `keys`, and returns the whole prefix the child must actually build.
fn required_order(
    orders: &[Vec<usize>],
    fixed: &BTreeSet<usize>,
    keys: &[usize],
) -> Option<Vec<usize>> {
    orders.iter().find_map(|order| {
        let fixed_prefix = order
            .iter()
            .take_while(|offset| fixed.contains(offset))
            .count();
        order[fixed_prefix..]
            .starts_with(keys)
            .then(|| order[..fixed_prefix + keys.len()].to_vec())
    })
}

/// The merge join this join node commits to, read straight off the catalog
/// and the `ON` clause.
pub(crate) fn merge_join_decision(
    join: &Join,
    catalog: &Catalog,
    current_db: &str,
    required: &tidb_planner::physical_property::PhysicalProperty,
    offered: Offered<'_>,
    rows: Option<&crate::driver::join_reorder::RowSource>,
) -> Option<MergeDecision> {
    let right_node = join.right.as_ref()?;
    if join.natural || !join.using.is_empty() {
        return None;
    }
    let left = possible_properties(&join.left, catalog, current_db, offered)?;
    let right = possible_properties(right_node, catalog, current_db, offered)?;
    let joined = SideProperties::concat(&left, &right)?;
    decide(join, &left, &right, &joined, required, offered, rows)
}

/// Go `getEnforcedMergeJoin`: a `MERGE_JOIN` hint may admit a merge plan even
/// when neither child already offers the join-key order. The caller installs
/// a root Sort on every child that does not deliver the returned requirement.
pub(crate) fn enforced_merge_join_decision(
    join: &Join,
    catalog: &Catalog,
    current_db: &str,
    required: &tidb_planner::physical_property::PhysicalProperty,
    offered: Offered<'_>,
) -> Option<MergeDecision> {
    let right_node = join.right.as_ref()?;
    if join.natural || !join.using.is_empty() {
        return None;
    }
    let left = possible_properties(&join.left, catalog, current_db, offered)?;
    let right = possible_properties(right_node, catalog, current_db, offered)?;
    let joined = SideProperties::concat(&left, &right)?;

    let mut conjuncts = Vec::new();
    if let Some(on) = join.on.as_ref() {
        crate::plan_trace::collect_and(on, &mut conjuncts);
    }
    if join.tp == JoinType::Cross {
        let repeated = offered
            .iter()
            .filter(|conjunct| !conjuncts.contains(conjunct))
            .collect::<Vec<_>>();
        conjuncts.extend(repeated);
    }
    let mut keys = Vec::new();
    for conjunct in conjuncts {
        let Expr::Binary(tidb_ast::BinaryOp::Eq, lhs, rhs) = conjunct else {
            continue;
        };
        let (Expr::Column(left_path), Expr::Column(right_path)) = (&**lhs, &**rhs) else {
            continue;
        };
        let pair = match (left.offset_of(left_path), right.offset_of(right_path)) {
            (Some(left), Some(right)) => Some((left, right)),
            _ => match (left.offset_of(right_path), right.offset_of(left_path)) {
                (Some(left), Some(right)) => Some((left, right)),
                _ => None,
            },
        };
        if let Some((left, right)) = pair {
            keys.push(MergeJoinKey { left, right });
        }
    }
    if keys.is_empty() {
        return None;
    }

    let (all_same, desc) = required.all_same_order();
    if !all_same {
        return None;
    }
    let mut front = Vec::new();
    for item in &required.sort_items {
        let column = item.col as usize;
        let at = keys
            .iter()
            .position(|key| key.left == column || key.right + left.width == column)?;
        if !front.contains(&at) {
            front.push(at);
        }
        if (join.tp == JoinType::Left && keys[at].right + left.width == column)
            || (join.tp == JoinType::Right && keys[at].left == column)
        {
            return None;
        }
    }
    let mut ordered = Vec::with_capacity(keys.len());
    ordered.extend(front.iter().map(|at| keys[*at]));
    ordered.extend(
        keys.into_iter()
            .enumerate()
            .filter_map(|(at, key)| (!front.contains(&at)).then_some(key)),
    );
    let names = ordered
        .iter()
        .map(|key| {
            Some((
                joined.column_at(key.left)?,
                joined.column_at(key.right + left.width)?,
            ))
        })
        .collect::<Option<Vec<_>>>()?;
    let left_required = ordered.iter().map(|key| key.left).collect::<Vec<_>>();
    let right_required = ordered.iter().map(|key| key.right).collect::<Vec<_>>();
    let left_required_names = names.iter().map(|(left, _)| left.clone()).collect();
    let right_required_names = names.iter().map(|(_, right)| right.clone()).collect();
    Some(MergeDecision {
        plan: MergeJoinPlan {
            keys: ordered,
            desc,
        },
        names,
        left_required,
        right_required,
        left_required_names,
        right_required_names,
    })
}

/// The property a `SELECT`'s own `FROM` must satisfy so that the `SELECT`'s
/// OUTPUT carries `required` -- the DOWNWARD half of the projection rule
/// [`derived_properties`] already ports upward.
///
/// Go's `PhysicalProjection.exhaustPhysicalPlans` is the source:
///
/// ```text
/// newProp, ok := p.TryToGetChildProp(prop)
/// if !ok { return nil, true, nil }
/// ```
///
/// and `TryToGetChildProp` rewrites each required sort item through the
/// projection's expressions, taking ONLY the ones that are a bare column
/// (`*expression.Column`) and refusing the whole property otherwise. That
/// refusal is why this returns the EMPTY property rather than a partial one:
/// an order the child cannot be asked for is an order the projection must not
/// claim.
///
/// A derived table is that `Projection`, which is what makes this the
/// function `build_derived_source` needs: the order a join above the derived
/// table requires of it is an order its own leaf has to be asked for, and a
/// materialized derived table replays its rows in arrival order, so asking
/// the leaf is the whole of delivering it.
///
/// The `SELECT` clauses that REPLACE the row order rather than carry it are
/// refused by the same list [`order_preserving_source`] holds, for the same
/// reason: `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT` and a window each have
/// their own `exhaustPhysicalPlans`, and none of them hands its child the
/// parent's property.
pub(crate) fn from_required_prop(
    select: &SelectStmt,
    from: &Join,
    required: &tidb_planner::physical_property::PhysicalProperty,
    catalog: &Catalog,
    current_db: &str,
    offered: Offered<'_>,
) -> tidb_planner::physical_property::PhysicalProperty {
    let empty = tidb_planner::physical_property::PhysicalProperty::default();
    if required.is_sort_item_empty() {
        return empty;
    }
    let (all_same, desc) = required.all_same_order();
    if !all_same {
        return empty;
    }
    if select.distinct
        || !select.group_by.is_empty()
        || select.having.is_some()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || !select.windows.is_empty()
    {
        return empty;
    }
    let fields = select.fields.fields();
    if fields
        .iter()
        .any(|field| projected_expr(field).is_some_and(Expr::has_aggregate_flag))
    {
        return empty;
    }
    let Some(source) = join_properties(from, catalog, current_db, offered, Phase::Promise) else {
        return empty;
    };
    let mut offsets = Vec::with_capacity(required.sort_items.len());
    for item in &required.sort_items {
        // `TryToGetChildProp`'s `expression.Column` arm, and its refusal for
        // every other expression shape -- including a `*`, which expands
        // against a scope this walk does not build.
        let Some(SelectField::Expr {
            expr: Expr::Column(path),
            ..
        }) = fields.get(item.col as usize)
        else {
            return empty;
        };
        let Some(offset) = source.offset_of(path) else {
            return empty;
        };
        offsets.push(offset);
    }
    child_required_prop(offsets.into_iter(), desc)
}

/// The property a merge join's child is required to satisfy: its own join
/// keys' order, in the direction the parent asked for.
///
/// `PhysicalMergeJoin.tryToGetChildReqProp`, whose `NewPhysicalProperty(
/// RootTaskType, p.LeftJoinKeys, desc, math.MaxFloat64, false)` this is.
pub(crate) fn child_required_prop(
    keys: impl Iterator<Item = usize>,
    desc: bool,
) -> tidb_planner::physical_property::PhysicalProperty {
    let cols: Vec<i64> = keys.map(|at| at as i64).collect();
    tidb_planner::physical_property::PhysicalProperty::new(
        tidb_planner::physical_property::TaskType::Root,
        &cols,
        desc,
        f64::MAX,
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn side(relations: &[(&str, &[&str])], orders: &[&[usize]]) -> SideProperties {
        let mut offset = 0;
        let relations = relations
            .iter()
            .map(|(visible, columns)| {
                let relation = Relation {
                    visible: (*visible).to_owned(),
                    columns: columns.iter().map(|c| (*c).to_owned()).collect(),
                    offset,
                };
                offset += columns.len();
                relation
            })
            .collect();
        SideProperties {
            relations,
            width: offset,
            orders: orders.iter().map(|o| o.to_vec()).collect(),
        }
    }

    fn path(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|p| (*p).to_owned()).collect()
    }

    #[test]
    fn an_empty_order_requirement_is_always_satisfied() {
        assert!(delivers(&[], &[]));
        assert!(delivers(&[vec![2, 3]], &[]));
        assert!(!delivers(&[], &[2]));
    }

    /// A qualified path picks its relation; a bare name searches every
    /// relation and answers only when exactly one holds it. Go raises
    /// `ErrAmbiguous` for the second case, and a merge key guessed here would
    /// join on a column nobody named.
    #[test]
    fn a_bare_name_resolves_only_when_one_relation_holds_it() {
        let props = side(&[("t1", &["a", "b"]), ("t2", &["a", "c"])], &[]);
        assert_eq!(props.offset_of(&path(&["t1", "a"])), Some(0));
        assert_eq!(props.offset_of(&path(&["t2", "a"])), Some(2));
        assert_eq!(props.offset_of(&path(&["db", "t2", "c"])), Some(3));
        assert_eq!(props.offset_of(&path(&["b"])), Some(1));
        assert_eq!(props.offset_of(&path(&["a"])), None);
        assert_eq!(props.offset_of(&path(&["nosuch"])), None);
    }

    /// Concatenation shifts the right side's offsets by the left's width, and
    /// refuses a name that would then answer for two relations at once.
    #[test]
    fn concat_shifts_the_right_side_and_refuses_a_repeated_name() {
        let left = side(&[("t1", &["a"])], &[&[0]]);
        let right = side(&[("t2", &["a", "b"])], &[&[0]]);
        let joined = SideProperties::concat(&left, &right).expect("distinct names");
        assert_eq!(joined.width, 3);
        assert_eq!(joined.offset_of(&path(&["t2", "b"])), Some(2));
        assert_eq!(
            joined.column_at(1),
            Some(RelColumn {
                relation: "t2".to_owned(),
                column: "a".to_owned(),
            })
        );
        assert!(SideProperties::concat(&left, &left).is_none());
    }

    /// The null-extended side of an outer join offers NO order: its column is
    /// NULL on the extended rows. Go's own three-way check, from the function
    /// this module ports.
    #[test]
    fn an_outer_join_drops_the_null_extended_sides_order() {
        assert_eq!(
            union_orders(JoinType::Cross, vec![vec![0]], vec![vec![3]]),
            vec![vec![0], vec![3]]
        );
        assert_eq!(
            union_orders(JoinType::Left, vec![vec![0]], vec![vec![3]]),
            vec![vec![0]]
        );
        assert_eq!(
            union_orders(JoinType::Right, vec![vec![0]], vec![vec![3]]),
            vec![vec![3]]
        );
    }

    /// The PROMISE is a union and not a pick: a join whose two children each
    /// carry an order reports BOTH, which is what lets a parent merge on
    /// either. Go's `len(leftProperties)+len(rightProperties)`.
    #[test]
    fn the_promise_unions_both_children() {
        assert_eq!(
            union_orders(
                JoinType::Cross,
                vec![vec![0], vec![1]],
                vec![vec![2], vec![3]]
            ),
            vec![vec![0], vec![1], vec![2], vec![3]]
        );
    }

    /// A projection TRUNCATES an order at the first column it does not carry
    /// rather than skipping past it, and drops an order that survives as
    /// nothing. Go's `break`, and the reason it is a `break`: rows equal on
    /// the later column were separated only by the dropped one.
    #[test]
    fn a_projection_truncates_an_order_at_its_first_missing_column() {
        // The child is ordered by (0, 1); the projection carries 1 at output
        // 0 and 0 at output 1.
        let sources = [Some(1), Some(0)];
        assert_eq!(project_orders(&[vec![0, 1]], &sources), vec![vec![1, 0]]);
        // 2 is not projected: the order is cut before it.
        assert_eq!(project_orders(&[vec![0, 2, 1]], &sources), vec![vec![1]]);
        // Cut at the FIRST column leaves nothing, and the order is dropped
        // rather than reported as the suffix that happens to be carried.
        assert_eq!(
            project_orders(&[vec![2, 0]], &sources),
            Vec::<Vec<usize>>::new()
        );
        // A projection that carries no bare column at all carries no order.
        assert_eq!(
            project_orders(&[vec![0]], &[None, None]),
            Vec::<Vec<usize>>::new()
        );
    }

    /// `column_at` answers for every offset the row holds and for none beyond
    /// it -- which is what makes a key that lands outside the join droppable
    /// rather than silently wrong.
    #[test]
    fn column_at_covers_the_row_and_stops_at_its_end() {
        let props = side(&[("t1", &["a"]), ("t2", &["b"])], &[]);
        assert_eq!(props.column_at(0).map(|c| c.column), Some("a".to_owned()));
        assert_eq!(
            props.column_at(1).map(|c| c.relation),
            Some("t2".to_owned())
        );
        assert!(props.column_at(2).is_none());
    }

    /// A leading index column fixed by `column = constant` no longer blocks
    /// a merge on the following key, but the child is still required to build
    /// the complete `(fixed, key)` order so its access path remains truthful.
    #[test]
    fn a_fixed_index_prefix_is_trimmed_for_matching_and_restored_for_building() {
        let fixed = BTreeSet::from([0]);
        let orders = vec![vec![0, 1, 2], vec![3]];
        assert_eq!(
            trim_fixed_prefixes(&orders, &fixed),
            vec![vec![1, 2], vec![3]]
        );
        assert_eq!(required_order(&orders, &fixed, &[1]), Some(vec![0, 1]));
        assert_eq!(required_order(&orders, &fixed, &[2]), None);
    }

    #[test]
    fn a_merge_property_skips_a_required_constant_prefix() {
        let plan = MergeJoinPlan {
            keys: vec![MergeJoinKey { left: 1, right: 0 }],
            desc: false,
        };
        let required = child_required_prop([2, 1].into_iter(), false);
        assert!(merge_satisfies_required_property(
            JoinType::Cross,
            &plan,
            3,
            &required,
            &BTreeSet::from([2]),
            &BTreeSet::new(),
        ));

        let required = child_required_prop([1, 2].into_iter(), false);
        assert!(!merge_satisfies_required_property(
            JoinType::Cross,
            &plan,
            3,
            &required,
            &BTreeSet::new(),
            &BTreeSet::new(),
        ));
    }
}
