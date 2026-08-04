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
//! # The ONE deliberate divergence, and why it is the safe direction
//!
//! Go's `LogicalJoin.PreparePossibleProperties` reports the UNION of its two
//! children's orders -- `resultProperties := leftProperties ++
//! rightProperties` -- with the null-extended side dropped for an outer join.
//! That is a LOGICAL claim about which orders the join's output COULD be
//! produced in, not about the plan finally chosen: a `PhysicalHashJoin`
//! carries none of them. Go is safe because a parent that requires one of
//! those orders re-asks the child through `findBestTask(prop)`, and the child
//! either answers with a plan that satisfies it or an enforced `Sort` is
//! added.
//!
//! This tier has no such second pass -- `build_join` builds executors
//! bottom-up and a child cannot be re-planned once built. Reporting Go's
//! logical union here would promise an order a hash join then fails to
//! deliver, and the merge executor would silently drop rows. So a join node
//! reports the order ITS OWN chosen plan produces: its merge keys when it
//! merges, and NOTHING when it hashes. That is the same set Go's physical
//! layer ends up with, reached one pass earlier.
//!
//! A merged join's output is sorted by BOTH key lists -- the two are equal in
//! every row it emits -- so both are reported, minus the side an outer join
//! null-extends (Go's own `JoinType` check in the same function).

use super::catalog::split_table_path;
use super::catalog::{Catalog, TableEntry};
use super::predicate_push_down::{offered_conjuncts, Offered};
use crate::merge_join_plan::{MergeJoinKey, MergeJoinPlan};
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
    fn column_at(&self, offset: usize) -> Option<RelColumn> {
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
            let orders = match entry {
                // Only a real key-value table streams in key order; a view or
                // a memory table carries none, and says so while still
                // letting a path name its columns.
                TableEntry::Kv(kv) if kv.partition().is_none() => {
                    crate::merge_join_plan::provided_orders(kv)
                        .into_iter()
                        .filter_map(|order| {
                            order
                                .into_iter()
                                .map(|at| {
                                    let column = &kv.columns.get(at)?.name;
                                    columns
                                        .iter()
                                        .position(|name| name.eq_ignore_ascii_case(column))
                                })
                                .collect::<Option<Vec<usize>>>()
                        })
                        .collect()
                }
                _ => Vec::new(),
            };
            Some(SideProperties::single(visible, columns, orders))
        }
        // A nested join is offered the SAME `WHERE` conjuncts, which is
        // what `build_from` hands it.
        JoinNode::Join(join) => join_properties(join, catalog, current_db, offered),
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
            )
        }
    }
}

/// [`possible_properties`] for a join node: the two sides' relations
/// concatenated, and the order the join's OWN chosen plan produces.
pub(crate) fn join_properties(
    join: &Join,
    catalog: &Catalog,
    current_db: &str,
    offered: Offered<'_>,
) -> Option<SideProperties> {
    let Some(right_node) = &join.right else {
        // The single-relation wrapper the parser always produces.
        return possible_properties(&join.left, catalog, current_db, offered);
    };
    if join.natural || !join.using.is_empty() {
        // A coalesced join's scope addresses columns by row offset rather
        // than by name, and its output row is not the two sides concatenated.
        return None;
    }
    let left = possible_properties(&join.left, catalog, current_db, offered)?;
    let right = possible_properties(right_node, catalog, current_db, offered)?;
    let mut joined = SideProperties::concat(&left, &right)?;
    // The property this join is asked for is the empty one: no caller demands
    // an order from a `FROM` node yet, and the empty property's
    // `AllSameOrder` is Go's ascending answer.
    let required = tidb_planner::physical_property::PhysicalProperty::default();
    if let Some(decision) = decide(join, &left, &right, &joined, &required, offered) {
        let left_order: Vec<usize> = decision.plan.keys.iter().map(|key| key.left).collect();
        let right_order: Vec<usize> = decision
            .plan
            .keys
            .iter()
            .map(|key| key.right + left.width)
            .collect();
        joined.orders = orders_of_merged_join(join.tp, left_order, right_order);
    }
    Some(joined)
}

/// The orders a MERGED join's output carries, given the order its left keys
/// impose and the order its right keys impose.
///
/// A merge join emits its rows in key order and its two key lists are EQUAL in
/// every row it emits, so both describe the output -- except for the side an
/// outer join null-extends, whose column carries NULL on the extended rows and
/// is therefore not sorted at all. That exception is Go's, verbatim, from the
/// same function this module ports (`logical_join.go:653`):
///
/// ```text
/// if p.JoinType == base.LeftOuterJoin || p.JoinType == base.LeftOuterSemiJoin {
///     rightProperties = nil
/// } else if p.JoinType == base.RightOuterJoin {
///     leftProperties = nil
/// }
/// ```
///
/// `JoinType::Cross` is this AST's spelling of the INNER join (see
/// `build_join`'s `JoinKind` mapping), which null-extends neither side.
fn orders_of_merged_join(
    join_type: JoinType,
    left_order: Vec<usize>,
    right_order: Vec<usize>,
) -> Vec<Vec<usize>> {
    match join_type {
        JoinType::Cross => vec![left_order, right_order],
        JoinType::Left => vec![left_order],
        JoinType::Right => vec![right_order],
    }
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
) -> Option<SideProperties> {
    let QueryStmt::Select(select) = subquery else {
        return None;
    };
    // A derived table's own `WHERE` is the one offered INSIDE it, which is
    // what `run_select_stmt` hands its `FROM`.
    let inner_offered = offered_conjuncts(select.where_clause.as_ref());
    let inner = order_preserving_source(select, catalog, current_db, &inner_offered)?;
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

/// The `FROM` of a `SELECT` that carries its source's row order through
/// unchanged, or `None`.
fn order_preserving_source(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    offered: Offered<'_>,
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
    join_properties(select.from.as_ref()?, catalog, current_db, offered)
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
            .copied()
            .filter(|conjunct| !conjuncts.contains(conjunct))
            .collect();
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
    let plan = crate::merge_join_plan::get_merge_join(
        &keys,
        &left.orders,
        &right.orders,
        required.all_same_order().1,
    )?;
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
    Some(MergeDecision { plan, names })
}

/// The merge join this join node commits to, read straight off the catalog
/// and the `ON` clause.
pub(crate) fn merge_join_decision(
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
    decide(join, &left, &right, &joined, required, offered)
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
            orders_of_merged_join(JoinType::Cross, vec![0], vec![3]),
            vec![vec![0], vec![3]]
        );
        assert_eq!(
            orders_of_merged_join(JoinType::Left, vec![0], vec![3]),
            vec![vec![0]]
        );
        assert_eq!(
            orders_of_merged_join(JoinType::Right, vec![0], vec![3]),
            vec![vec![3]]
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
}
