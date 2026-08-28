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

//! Mechanical lowering for the shared planner's merge-join receipt.
//!
//! The shared planner owns Go's `preparePossibleProperties`, `GetMergeJoin`,
//! property enforcement, enumeration, and costing. This module keeps only the
//! mechanical boundary that Go's executor builder performs after selection:
//! resolve stable relation-qualified columns to executor offsets, lower any
//! selected Sort enforcer, and verify the built children deliver the received
//! requirements.
//!
//! [`super::from::build_join`] still checks the built children's physical
//! delivery receipts before constructing the selected merge join. A mismatch
//! is an unsupported planner/executor boundary and is rejected; it is never
//! repaired by locally choosing hash join. A merged join reports both equal
//! key orders, except for a side null-extended by an outer join, matching
//! `LogicalJoin.PreparePossibleProperties`.

use super::catalog::split_table_path;
use super::catalog::{Catalog, TableEntry};
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

/// The stable relation/column layout of one `FROM` subtree. Physical order
/// comes from the selected planner tree and is deliberately absent here.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SideProperties {
    /// The relations the subtree exposes, in row order.
    pub(crate) relations: Vec<Relation>,
    /// The subtree's row width.
    pub(crate) width: usize,
}

impl SideProperties {
    /// One relation, starting at offset zero.
    fn single(visible: String, columns: Vec<String>) -> Self {
        let width = columns.len();
        Self {
            relations: vec![Relation {
                visible,
                columns,
                offset: 0,
            }],
            width,
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
    /// The complete left-child order named so it survives column pruning.
    pub(crate) left_required_names: Vec<RelColumn>,
    /// The complete right-child order named so it survives column pruning.
    pub(crate) right_required_names: Vec<RelColumn>,
}

/// Resolves the row identity needed to lower stable planner columns. This is
/// intentionally not Go's `preparePossibleProperties`: the shared logical
/// planner has already run that rule and selected a physical tree.
pub(crate) fn source_layout(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
) -> Option<SideProperties> {
    match node {
        JoinNode::Table(table_ref) => {
            let (database, name) = split_table_path(&table_ref.name, current_db).ok()?;
            let entry = catalog.get_in(database, name)?;
            let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
            let columns: Vec<String> = entry
                .column_list()
                .into_iter()
                .map(|(column, _)| column)
                .collect();
            Some(SideProperties::single(visible, columns))
        }
        JoinNode::Join(join) => join_layout(join, catalog, current_db),
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
            let mut columns = super::from::derived_field_names_query(subquery)?;
            if !column_names.is_empty() {
                if column_names.len() != columns.len() {
                    return None;
                }
                columns = column_names.to_vec();
            }
            Some(SideProperties::single(
                alias.as_deref()?.to_owned(),
                columns,
            ))
        }
    }
}

/// The concatenated row identity of a join. Algorithm and order selection
/// remain exclusively in the shared physical planner.
pub(crate) fn join_layout(
    join: &Join,
    catalog: &Catalog,
    current_db: &str,
) -> Option<SideProperties> {
    let Some(right_node) = &join.right else {
        return source_layout(&join.left, catalog, current_db);
    };
    if join.natural || !join.using.is_empty() {
        return None;
    }
    let left = source_layout(&join.left, catalog, current_db)?;
    let right = source_layout(right_node, catalog, current_db)?;
    SideProperties::concat(&left, &right)
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
#[derive(Clone, Debug)]
pub(crate) struct AggregationOrder {
    required_columns: Vec<RelColumn>,
    fixed_columns: Vec<RelColumn>,
    physical_group_columns: Vec<RelColumn>,
    desc: bool,
    enforced: bool,
}

impl AggregationOrder {
    pub(crate) fn from_planner_columns(
        required_columns: Vec<RelColumn>,
        fixed_columns: Vec<RelColumn>,
        physical_group_columns: Vec<RelColumn>,
        desc: bool,
        enforced: bool,
    ) -> Self {
        Self {
            required_columns,
            fixed_columns,
            physical_group_columns,
            desc,
            enforced,
        }
    }

    /// Re-resolves the order after logical join reorder. Go's property keeps
    /// column `UniqueID`s stable while the join tree moves; this driver uses
    /// row offsets, so the equivalent identity is the relation-qualified
    /// column name captured when the aggregation order was derived.
    pub(crate) fn required_for(
        &self,
        from: &Join,
        catalog: &Catalog,
        current_db: &str,
    ) -> Option<tidb_planner::physical_property::PhysicalProperty> {
        if self.enforced {
            return Some(tidb_planner::physical_property::PhysicalProperty::default());
        }
        let source = join_layout(from, catalog, current_db)?;
        let required_offsets = self
            .required_columns
            .iter()
            .map(|column| source.offset_of(&[column.relation.clone(), column.column.clone()]))
            .collect::<Option<Vec<_>>>()?;
        Some(child_required_prop(required_offsets.into_iter(), self.desc))
    }

    /// The selected physical StreamAgg has a `PhysicalSort` child rather
    /// than an access path that naturally supplies the group order.
    #[must_use]
    pub(crate) const fn enforced_sort_direction(&self) -> Option<bool> {
        if self.enforced {
            Some(self.desc)
        } else {
            None
        }
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
        let fixed_offsets = self
            .fixed_columns
            .iter()
            .filter_map(|required| {
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
            .collect::<BTreeSet<_>>();
        required_offsets.is_some_and(|required| {
            delivered.iter().any(|order| {
                if order.starts_with(&required) {
                    return true;
                }
                let fixed_prefix = order
                    .iter()
                    .take_while(|offset| fixed_offsets.contains(offset))
                    .count();
                order[fixed_prefix..].starts_with(&required)
            })
        })
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
        let source = join_layout(from, catalog, current_db)?;
        source.column_at(source.offset_of(path)?)
    };
    // A projection can retain a directly qualified child column even when
    // the child's complete layout model is unavailable (for
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
/// output-to-input column mapping.
pub(crate) fn project_delivered_orders(
    orders: &[Vec<usize>],
    sources: &[Option<usize>],
) -> Vec<Vec<usize>> {
    project_orders(orders, sources)
}

/// Removes only the leading columns whose equality predicates make them
/// constant for every row. The remaining suffix is the order the shared
/// planner can use when enumerating a merge join.
pub(crate) fn trim_fixed_prefixes(
    orders: &[Vec<usize>],
    fixed: &BTreeSet<usize>,
) -> Vec<Vec<usize>> {
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

/// Lowers the exact merge-key receipt selected by the shared physical planner
/// into this executor's child-relative offsets. This is mechanical identity
/// resolution only; algorithm selection and key ordering remain owned by the
/// shared planner.
pub(crate) fn merge_join_decision_from_planner(
    left_node: &JoinNode,
    right_node: &JoinNode,
    left: &SideProperties,
    right: &SideProperties,
    keys: &[(RelColumn, RelColumn)],
    desc: bool,
    left_requirement: &crate::driver::planner_bridge::JoinChildRequirement,
    right_requirement: &crate::driver::planner_bridge::JoinChildRequirement,
    catalog: &Catalog,
    current_db: &str,
) -> Option<MergeDecision> {
    let mut offsets = Vec::with_capacity(keys.len());
    let mut names = Vec::with_capacity(keys.len());
    for (planner_left, planner_right) in keys {
        let (left_offset, right_offset, left_name, right_name) = match (
            planner_column_offset(left_node, left, planner_left, catalog, current_db),
            planner_column_offset(right_node, right, planner_right, catalog, current_db),
        ) {
            (Some((left_offset, left_name)), Some((right_offset, right_name))) => {
                (left_offset, right_offset, left_name, right_name)
            }
            _ => {
                let (left_offset, left_name) =
                    planner_column_offset(left_node, left, planner_right, catalog, current_db)?;
                let (right_offset, right_name) =
                    planner_column_offset(right_node, right, planner_left, catalog, current_db)?;
                (left_offset, right_offset, left_name, right_name)
            }
        };
        offsets.push(MergeJoinKey {
            left: left_offset,
            right: right_offset,
        });
        names.push((left_name, right_name));
    }
    if offsets.is_empty() {
        return None;
    }
    let (_, left_required_names) = planner_required_offsets(
        left_node,
        left,
        &left_requirement.columns,
        catalog,
        current_db,
    )?;
    let (_, right_required_names) = planner_required_offsets(
        right_node,
        right,
        &right_requirement.columns,
        catalog,
        current_db,
    )?;
    Some(MergeDecision {
        plan: MergeJoinPlan {
            keys: offsets,
            desc,
        },
        names,
        left_required_names,
        right_required_names,
    })
}

fn planner_required_offsets(
    node: &JoinNode,
    side: &SideProperties,
    columns: &[RelColumn],
    catalog: &Catalog,
    current_db: &str,
) -> Option<(Vec<usize>, Vec<RelColumn>)> {
    let pairs = columns
        .iter()
        .map(|column| planner_column_offset(node, side, column, catalog, current_db))
        .collect::<Option<Vec<_>>>()?;
    Some(pairs.into_iter().unzip())
}

/// Translates a selected physical join's child property into the positional
/// property understood by executor construction. This is identity lowering,
/// not another property derivation: the columns and direction come directly
/// from the shared physical node.
pub(crate) fn child_required_prop_from_planner(
    node: &JoinNode,
    side: &SideProperties,
    requirement: &crate::driver::planner_bridge::JoinChildRequirement,
    catalog: &Catalog,
    current_db: &str,
) -> Option<tidb_planner::physical_property::PhysicalProperty> {
    if requirement.enforced_sort {
        return Some(tidb_planner::physical_property::PhysicalProperty::default());
    }
    let (offsets, _) =
        planner_required_offsets(node, side, &requirement.columns, catalog, current_db)?;
    Some(child_required_prop(offsets.into_iter(), requirement.desc))
}

fn planner_column_offset(
    node: &JoinNode,
    side: &SideProperties,
    planner_column: &RelColumn,
    catalog: &Catalog,
    current_db: &str,
) -> Option<(usize, RelColumn)> {
    let direct_path = [
        planner_column.relation.clone(),
        planner_column.column.clone(),
    ];
    if let Some(offset) = side.offset_of(&direct_path) {
        return Some((offset, planner_column.clone()));
    }

    let mut resolved = None;
    for relation in &side.relations {
        for (column_offset, column) in relation.columns.iter().enumerate() {
            let visible = RelColumn {
                relation: relation.visible.clone(),
                column: column.clone(),
            };
            let Some(origin) = physical_column_origin(node, &visible, catalog, current_db, true)
            else {
                continue;
            };
            if !origin.table.eq_ignore_ascii_case(&planner_column.relation)
                || !origin.column.eq_ignore_ascii_case(&planner_column.column)
            {
                continue;
            }
            if resolved.is_some() {
                return None;
            }
            resolved = Some((relation.offset + column_offset, visible));
        }
    }
    resolved
}

/// The property a `SELECT`'s own `FROM` must satisfy so that the selected
/// planner projection carries `required`.
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
/// refused for the same reason: `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`
/// and a window each have
/// their own `exhaustPhysicalPlans`, and none of them hands its child the
/// parent's property.
pub(crate) fn from_required_prop(
    select: &SelectStmt,
    from: &Join,
    required: &tidb_planner::physical_property::PhysicalProperty,
    catalog: &Catalog,
    current_db: &str,
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
        .any(|field| matches!(field, SelectField::Expr { expr, .. } if expr.has_aggregate_flag()))
    {
        return empty;
    }
    let Some(source) = join_layout(from, catalog, current_db) else {
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

    fn side(relations: &[(&str, &[&str])]) -> SideProperties {
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
        let props = side(&[("t1", &["a", "b"]), ("t2", &["a", "c"])]);
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
        let left = side(&[("t1", &["a"])]);
        let right = side(&[("t2", &["a", "b"])]);
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
        let props = side(&[("t1", &["a"]), ("t2", &["b"])]);
        assert_eq!(props.column_at(0).map(|c| c.column), Some("a".to_owned()));
        assert_eq!(
            props.column_at(1).map(|c| c.relation),
            Some("t2".to_owned())
        );
        assert!(props.column_at(2).is_none());
    }

    /// A leading index column fixed by `column = constant` no longer blocks
    /// matching the following key in Go's possible-property preparation.
    #[test]
    fn a_fixed_index_prefix_is_trimmed_for_matching() {
        let fixed = BTreeSet::from([0]);
        let orders = vec![vec![0, 1, 2], vec![3]];
        assert_eq!(
            trim_fixed_prefixes(&orders, &fixed),
            vec![vec![1, 2], vec![3]]
        );
    }
}
