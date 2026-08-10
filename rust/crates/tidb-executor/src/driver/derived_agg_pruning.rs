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

//! Go's `LogicalAggregation.PruneColumns` (`logical_aggregation.go:113`)
//! reaching a derived table. An UNGROUPED aggregation nobody reads a column
//! of computes only the fact that it produced its one row. A GROUPED
//! aggregation also drops an unread selected group carrier while retaining
//! the `GROUP BY` expression that determines its rows.
//!
//! # The Go rule, and why the answer is `count(1)`
//!
//! `PruneColumns` deletes every aggregate whose output column no parent uses
//! and whose arguments have no side effects. When that empties the list it
//! does NOT leave the aggregation empty:
//!
//! ```go
//! // If all the aggregate functions are pruned, we should add an aggregate
//! // function to maintain the info of row numbers.
//! newAgg, err = aggregation.NewAggFuncDesc(..., ast.AggFuncCount,
//!     []expression.Expression{expression.NewOne()}, false)
//! ```
//!
//! -- because an ungrouped aggregation over an EMPTY table still returns one
//! row, and dropping it would change the parent's row count. That surviving
//! `count(1)` names no column, so the `DataSource` beneath it is pruned to
//! nothing and Go's access-path choice takes the narrowest index. It is the
//! whole reason TiDB records
//!
//! ```text
//! explain format = 'plan_tree' select 1 from (select count(c2), count(c3) from t1) k;
//!   Projection      1->Column
//!   └─StreamAgg     funcs:count(Column)->Column
//!     └─IndexReader index:StreamAgg
//!       └─StreamAgg cop[tikv] funcs:count(1)->Column
//!         └─IndexFullScan table:t1, index:c2(c2)
//! ```
//!
//! for a statement whose derived table asks for `c2` AND `c3`: neither is
//! read, so neither is fetched, and the narrow index answers the row count.
//!
//! # What this rewrites, and what it does not
//!
//! The rewrite is the statement-level form of the same step: the derived
//! `SELECT`'s field list becomes the single field `count(1)`. Everything else
//! -- which index then answers it -- is already there
//! ([`super::leaf_demand`] feeding [`crate::access_cost`]), so this file only
//! has to stop asking for columns nobody wants.
//!
//! Refusals, each keeping the statement exactly as written:
//!
//! * a derived subquery that is not a plain `SELECT`, or is `LATERAL`, or
//!   carries an alias column list;
//! * a subquery that is not an UNGROUPED aggregation: a `GROUP BY`,
//!   `HAVING`, `DISTINCT`, `WITH ROLLUP`, `ORDER BY`, `LIMIT`, `WINDOW`,
//!   `WITH` or `VALUES` all make the row count depend on what is computed,
//!   and a `FROM`-less one has no `DataSource` to prune;
//! * any field that is not an aggregate call -- Go prunes those through the
//!   `Projection` above the aggregation, which is a separate rewrite;
//! * `GROUP_CONCAT`, whose separator and per-aggregate `ORDER BY` are a
//!   shape of their own -- Go prunes it like any other unread aggregate, so
//!   this refusal is narrower than Go and costs only a plan;
//! * any argument that is not a plain column reference or literal, which is
//!   this port's conservative reading of Go's `ExprsHasSideEffects`. A
//!   `DISTINCT` aggregate is NOT refused: Go's check is over the ARGUMENTS,
//!   and `select 1 from (select count(distinct c2) from t1) k` really does
//!   read the narrow index in TiDB (captured with `gorun`);
//! * a field list that is ALREADY the single `count(1)` this would write,
//!   so the rewrite is idempotent;
//! * an outer statement that reads any column of the derived table, decided
//!   by [`super::leaf_demand::LeafDemand`] over the outer statement -- the
//!   same walk, and the same over-approximating-in-the-safe-direction
//!   argument, as [`super::outer_join_elimination`].

//! # Which refusals are guards, and which are only narrowings
//!
//! Mutation-probed one at a time, each against the gorun-captured row sets in
//! `crates/tidb-session/src/tests_derived_agg_pruning.rs`. Two are GUARDS --
//! removing either answers a wrong row count:
//!
//! * "every field is an aggregate". `select 1 from (select c2 + 0 from p1) k`
//!   is three rows; the rewrite would make it one. Note that the outer-read
//!   check does NOT also catch this, because the derived column is named
//!   `c2 + 0` and no outer reference collides with that name -- which is why
//!   the test uses that spelling and not `select c2`.
//! * the outer-read check itself. `select k.n from (select count(c2) as n
//!   from p1) k` is 2, because `count(c2)` skips the NULL row.
//!
//! The rest are NARROWINGS: removing them still answers TiDB's rows, because
//! the rewrite only ever replaces the FIELD LIST and every one of these
//! clauses survives it intact. `GROUP BY` still groups (Go prunes a grouped
//! aggregation's unread aggregates too), `HAVING` still tests its own
//! aggregate, `LIMIT` still cuts, the wildcard case is already refused by
//! `derived_field_names` returning `None`, and the idempotence guard only
//! stops a second no-op pass. They are kept because each widening would need
//! its own measured statement to justify it, and the corpus asks for none.

use tidb_ast::{Expr, JoinNode, QueryStmt, SelectField, SelectStmt};
use tidb_datatype::{FieldType, FieldTypeCode};

use super::leaf_demand::LeafDemand;

/// Marks the row-count aggregate Go appends after column pruning removes the
/// last non-FIRST_ROW aggregate from a grouped derived relation. This alias
/// exists only on the optimizer's private AST; the parent proved it does not
/// read the replaced output column before the marker is installed.
const PRUNED_ROW_COUNT_ALIAS: &str = "__tidb_pruned_row_count";

/// Whether this grouped SELECT carries the synthetic row-count aggregate
/// installed by [`prune_unread_grouped_outputs`]. Physical aggregation keeps
/// this state after its FIRST_ROW carriers, matching Go's
/// `LogicalAggregation.PruneColumns` append order.
pub(crate) fn has_pruned_row_count(select: &SelectStmt) -> bool {
    select.fields.fields().iter().any(|field| {
        matches!(
            field,
            SelectField::Expr {
                expr: Expr::Aggregate { name, distinct: false, args },
                alias: Some(alias),
            } if alias == PRUNED_ROW_COUNT_ALIAS
                && name.eq_ignore_ascii_case("count")
                && matches!(args.as_slice(), [Expr::Int(value)] if value == "1")
        )
    })
}

/// Whether this statement's sole relation is a grouped derived SELECT. Used
/// by the caller to render the post-pruning physical expressions that no
/// longer have source-column names.
pub(crate) fn is_single_grouped_derived(select: &SelectStmt) -> bool {
    let Some(from) = &select.from else {
        return false;
    };
    if from.right.is_some() {
        return false;
    }
    matches!(
        &from.left,
        JoinNode::Derived { subquery, .. }
            if matches!(&**subquery, QueryStmt::Select(inner) if !inner.group_by.is_empty())
    )
}

/// The statement with unread derived-aggregation outputs removed, or `None`
/// when no reduction is provable.
pub(crate) fn prune(select: &SelectStmt) -> Option<SelectStmt> {
    let mut rewritten = select.clone();
    prune_select(&mut rewritten).then_some(rewritten)
}

/// Prunes one query block top-down. Its own output has already been reduced
/// by its caller, so references in the surviving projection and join
/// predicates are the exact demand to propagate into the block's derived
/// children.
fn prune_select(select: &mut SelectStmt) -> bool {
    let demand = LeafDemand::of_select(&without_derived_inputs(select));
    select
        .from
        .as_mut()
        .is_some_and(|from| prune_join(from, &demand))
}

fn prune_join(join: &mut tidb_ast::Join, demand: &LeafDemand) -> bool {
    let mut changed = prune_node(&mut join.left, demand);
    if let Some(right) = &mut join.right {
        changed |= prune_node(right, demand);
    }
    changed
}

fn prune_node(node: &mut JoinNode, demand: &LeafDemand) -> bool {
    match node {
        JoinNode::Join(join) => prune_join(join, demand),
        JoinNode::Table(_) => false,
        JoinNode::Derived {
            subquery,
            alias,
            lateral,
            column_names,
        } => {
            let QueryStmt::Select(inner) = &mut **subquery else {
                return false;
            };
            let mut changed = false;
            if let (Some(alias), false, true) =
                (alias.as_deref(), *lateral, column_names.is_empty())
            {
                if let Some(names) = super::from::derived_field_names(inner) {
                    // Only output names matter to `LeafDemand::needed`; the
                    // type is a placeholder until the child is built.
                    let columns = names
                        .into_iter()
                        .map(|name| (name, FieldType::new(FieldTypeCode::LongLong)))
                        .collect::<Vec<_>>();
                    let needed = demand.needed(alias, &columns);
                    let pruned = if is_prunable_ungrouped_aggregation(inner) {
                        needed.is_empty().then(|| {
                            let mut pruned = (**inner).clone();
                            pruned.fields = vec![SelectField::Expr {
                                expr: count_one(),
                                alias: None,
                            }]
                            .into();
                            pruned
                        })
                    } else {
                        prune_unread_grouped_outputs(inner, &needed)
                            .or_else(|| prune_unread_pass_through_columns(inner, &needed))
                    };
                    if let Some(pruned) = pruned {
                        **inner = pruned;
                        changed = true;
                    }
                }
            }
            changed | prune_select(inner)
        }
    }
}

/// Computes one query block's parent demand without descending into the
/// derived inputs whose output is being pruned. Join `ON`/`USING` clauses
/// remain, as do correlated subqueries written in this block's expressions.
fn without_derived_inputs(select: &SelectStmt) -> SelectStmt {
    fn strip_join(join: &mut tidb_ast::Join) {
        strip_node(&mut join.left);
        if let Some(right) = &mut join.right {
            strip_node(right);
        }
    }

    fn strip_node(node: &mut JoinNode) {
        match node {
            JoinNode::Join(join) => strip_join(join),
            JoinNode::Table(_) => {}
            JoinNode::Derived { subquery, .. } => {
                let QueryStmt::Select(inner) = &mut **subquery else {
                    return;
                };
                inner.with = None;
                inner.fields = vec![SelectField::Expr {
                    expr: count_one(),
                    alias: None,
                }]
                .into();
                inner.values.clear();
                inner.from = None;
                inner.where_clause = None;
                inner.group_by.clear();
                inner.rollup = false;
                inner.having = None;
                inner.windows.clear();
                inner.order_by.clear();
                inner.limit = None;
            }
        }
    }

    let mut outer = select.clone();
    if let Some(from) = &mut outer.from {
        strip_join(from);
    }
    outer
}

/// Drops grouped outputs the outer query does not read. The group items remain,
/// so row cardinality and ordering are unchanged; only their exposed
/// `FIRST_ROW` carriers and side-effect-free aggregate outputs disappear.
///
/// Go builds explicit aggregate functions before the source-column
/// `FIRST_ROW` carriers. If pruning removes the last explicit aggregate while
/// carriers remain, `LogicalAggregation.PruneColumns` appends `COUNT(1)` to
/// preserve the empty-input row-count distinction. TPCC condition 11 reaches
/// exactly that state for `customer_count`, so retaining the written COUNT or
/// moving the replacement before the carriers describes a different physical
/// aggregation.
///
/// This remains narrower than Go's general rule: scalar select expressions
/// and aggregate arguments with possible side effects are retained, and
/// DISTINCT/output-order-sensitive shapes are refused.
fn prune_unread_grouped_outputs(select: &SelectStmt, needed: &[usize]) -> Option<SelectStmt> {
    if select.group_by.is_empty()
        || select.distinct
        || select.rollup
        || !select.order_by.is_empty()
        || !select.windows.is_empty()
    {
        return None;
    }

    let mut changed = false;
    let mut saw_non_first_row = false;
    let mut kept_non_first_row = false;
    let mut fields = Vec::with_capacity(select.fields.fields().len());
    for (index, field) in select.fields.fields().iter().enumerate() {
        let is_group_carrier = match field {
            SelectField::Expr {
                expr: expr @ Expr::Column(_),
                alias: _,
            } => select.group_by.iter().any(|item| &item.expr == expr),
            _ => false,
        };
        let aggregate = match field {
            SelectField::Expr {
                expr:
                    Expr::Aggregate {
                        name,
                        distinct: _,
                        args,
                    },
                ..
            } => Some((name, args)),
            _ => None,
        };
        let is_first_row =
            aggregate.is_some_and(|(name, _)| name.eq_ignore_ascii_case("first_row"));
        if aggregate.is_some() && !is_first_row {
            saw_non_first_row = true;
        }
        let removable_aggregate =
            aggregate.is_some_and(|(_, args)| args.iter().all(is_side_effect_free_argument));

        if !needed.contains(&index) && (is_group_carrier || removable_aggregate) {
            changed = true;
        } else {
            kept_non_first_row |= aggregate.is_some() && !is_first_row;
            fields.push(field.clone());
        }
    }
    if saw_non_first_row && !kept_non_first_row {
        fields.push(SelectField::Expr {
            expr: count_one(),
            alias: Some(PRUNED_ROW_COUNT_ALIAS.to_owned()),
        });
        changed = true;
    }
    if !changed || fields.is_empty() {
        return None;
    }

    let mut pruned = select.clone();
    pruned.fields = fields.into();
    Some(pruned)
}

/// Drops unread columns from a projection-only derived relation. The join
/// and filter below it still read every column they require; only values no
/// parent consumes stop crossing the derived-table boundary.
fn prune_unread_pass_through_columns(select: &SelectStmt, needed: &[usize]) -> Option<SelectStmt> {
    if select.from.is_none()
        || select.with.is_some()
        || !select.values.is_empty()
        || select.distinct
        || select.rollup
        || !select.group_by.is_empty()
        || select.having.is_some()
        || !select.windows.is_empty()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || !select.fields.fields().iter().all(|field| {
            matches!(
                field,
                SelectField::Expr {
                    expr: Expr::Column(_),
                    ..
                }
            )
        })
    {
        return None;
    }

    let fields = select
        .fields
        .fields()
        .iter()
        .enumerate()
        .filter(|(index, _)| needed.contains(index))
        .map(|(_, field)| field.clone())
        .collect::<Vec<_>>();
    if fields.is_empty() || fields.len() == select.fields.fields().len() {
        return None;
    }

    let mut pruned = select.clone();
    pruned.fields = fields.into();
    Some(pruned)
}

/// `COUNT(1)`, the aggregate Go appends when it prunes the last one -- the
/// same shape the parser builds for `COUNT(*)`.
fn count_one() -> Expr {
    Expr::Aggregate {
        name: "COUNT".to_owned(),
        distinct: false,
        args: vec![Expr::Int("1".to_owned())],
    }
}

/// Whether `select` is an ungrouped aggregation whose every output is an
/// aggregate this rewrite may drop. See the module doc for each refusal.
fn is_prunable_ungrouped_aggregation(select: &SelectStmt) -> bool {
    if select.from.is_none()
        || select.with.is_some()
        || !select.values.is_empty()
        || !select.windows.is_empty()
        || !select.group_by.is_empty()
        || select.rollup
        || select.having.is_some()
        || select.distinct
        || !select.order_by.is_empty()
        || select.limit.is_some()
    {
        return false;
    }
    let fields = select.fields.fields();
    if fields.is_empty() {
        return false;
    }
    if fields.len() == 1 {
        if let SelectField::Expr { expr, alias: None } = &fields[0] {
            if *expr == count_one() {
                return false;
            }
        }
    }
    fields.iter().all(|field| match field {
        SelectField::Expr { expr, alias: _ } => match expr {
            Expr::Aggregate { args, .. } => args.iter().all(is_side_effect_free_argument),
            _ => false,
        },
        SelectField::Wildcard(_) => false,
    })
}

/// A column reference or a literal: an argument whose evaluation Go's
/// `ExprsHasSideEffects` can never object to.
fn is_side_effect_free_argument(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Column(_)
            | Expr::Int(_)
            | Expr::Decimal(_)
            | Expr::Float(_)
            | Expr::String(_)
            | Expr::Null
            | Expr::Bool(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demand_crosses_pass_through_join_before_pruning_group_carrier() {
        let statement = tidb_parser::parse(
            r#"SELECT t.c1,t.sm,t.smh FROM (
                 SELECT o.c_id,o.c_d_id,o.c_w_id,o.c1,o.sm,h.smh FROM (
                   SELECT c.c_id,c.c_d_id,c.c_w_id,c.c_balance AS c1,
                          SUM(ol.amount) AS sm
                   FROM customer c LEFT JOIN ol
                     ON c.c_id=ol.c_id AND c.c_d_id=ol.c_d_id
                   GROUP BY c.c_d_id,c.c_id,c.c_w_id
                 ) o LEFT JOIN (
                   SELECT SUM(h.amount) AS smh,h.c_d_id,h.c_id FROM history h
                   GROUP BY h.c_d_id,h.c_id
                 ) h ON o.c_d_id=h.c_d_id AND o.c_id=h.c_id
               ) t"#,
        )
        .unwrap();
        let tidb_ast::Stmt::Query(query) = statement else {
            panic!("not a query");
        };
        let QueryStmt::Select(select) = &*query else {
            panic!("not a SELECT");
        };

        let pruned = prune(select).expect("the pass-through projection is reducible");
        let JoinNode::Derived { subquery: top, .. } =
            &pruned.from.as_ref().expect("outer FROM").left
        else {
            panic!("outer source is not derived");
        };
        let QueryStmt::Select(pass_through) = &**top else {
            panic!("outer derived query is not a SELECT");
        };
        assert_eq!(
            super::super::from::derived_field_names(pass_through).unwrap(),
            ["c1", "sm", "smh"]
        );

        let JoinNode::Derived {
            subquery: grouped, ..
        } = &pass_through.from.as_ref().expect("pass-through FROM").left
        else {
            panic!("grouped outer source is not derived");
        };
        let QueryStmt::Select(grouped) = &**grouped else {
            panic!("grouped query is not a SELECT");
        };
        assert_eq!(
            super::super::from::derived_field_names(grouped).unwrap(),
            ["c_id", "c_d_id", "c1", "sm"]
        );
        assert_eq!(grouped.group_by.len(), 3);
    }
}
