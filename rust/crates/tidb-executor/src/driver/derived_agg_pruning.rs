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
//! reaching a derived table: an UNGROUPED aggregation nobody reads a column
//! of computes nothing at all -- only the fact that it produced its one row.
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
//! * a `FROM` that is anything but the one derived table (a join needs the
//!   per-relation parent-column set, which the statement text does not
//!   carry);
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

use tidb_ast::{Expr, JoinNode, QueryStmt, SelectField, SelectStmt};
use tidb_datatype::{FieldType, FieldTypeCode};

use super::leaf_demand::LeafDemand;

/// The statement with its derived table's field list reduced to `count(1)`,
/// or `None` when no reduction is provable.
pub(crate) fn prune(select: &SelectStmt) -> Option<SelectStmt> {
    let from = select.from.as_ref()?;
    if from.right.is_some() {
        return None;
    }
    let JoinNode::Derived {
        subquery,
        alias: Some(alias),
        lateral: false,
        column_names,
    } = &from.left
    else {
        return None;
    };
    if !column_names.is_empty() {
        return None;
    }
    let QueryStmt::Select(inner) = &**subquery else {
        return None;
    };
    if !is_prunable_ungrouped_aggregation(inner) {
        return None;
    }
    // The outer statement must read no column of the derived table. Only the
    // NAMES matter to `LeafDemand::needed`, so the type is a placeholder.
    let names = super::from::derived_field_names(inner)?;
    let columns: Vec<(String, FieldType)> = names
        .into_iter()
        .map(|name| (name, FieldType::new(FieldTypeCode::LongLong)))
        .collect();
    if !LeafDemand::of_select(select)
        .needed(alias, &columns)
        .is_empty()
    {
        return None;
    }

    let mut pruned_inner = inner.clone();
    pruned_inner.fields = vec![SelectField::Expr {
        expr: count_one(),
        alias: None,
    }]
    .into();
    let mut rewritten = select.clone();
    rewritten.from = Some(tidb_ast::Join {
        left: JoinNode::Derived {
            subquery: tidb_ast::NodeBox::new(QueryStmt::Select(pruned_inner)),
            alias: Some(alias.clone()),
            lateral: false,
            column_names: Vec::new(),
        },
        ..from.clone()
    });
    Some(rewritten)
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
