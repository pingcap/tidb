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

//! Go's `rule_join_elimination.go`: an outer join whose null-producing side
//! nobody reads, and which cannot duplicate an outer row, is not a join at
//! all -- the statement reads the outer side alone and never touches the
//! inner table.
//!
//! # The rule, and which half of it is here
//!
//! `OuterJoinEliminator.tryToEliminateOuterJoin` has two independent grounds:
//!
//!  1. no parent column comes from the inner side AND the inner side's join
//!     keys contain a unique key of the inner table, so each outer row keeps
//!     at most one match and the row COUNT is the outer side's own;
//!  2. no parent column comes from the inner side AND every consumer above is
//!     duplicate-agnostic (`DISTINCT`, `max`/`min`, ...), so a duplicated
//!     outer row is unobservable even without the uniqueness.
//!
//! The direct `DISTINCT`/aggregate subset of (2), together with (1), is implemented here. It
//! closes both of the statements this tier diverges on, `explain_easy`'s
//!
//! ```sql
//! select t1.a, t1.b from t1 left outer join t2 on t1.a = t2.a;
//! select distinct t1.a, t1.b from t1 left outer join t2 on t1.a = t2.a;
//! ```
//!
//! -- including the `DISTINCT`, `MAX`, and `MIN` forms when `t2.a` is NOT
//! unique. Grouped and hidden aggregate carriers still need a logical
//! aggregate-column walk and remain outside this statement-shaped rewrite.
//!
//! # Why this runs over the statement rather than a logical plan
//!
//! Go runs the rule as a logical-plan rewrite, carrying `parentCols` down the
//! tree so a join nested three levels deep knows what its own parents read.
//! This tier has no such tree at rule time (see [`super::leaf_demand`] for
//! the same argument about column pruning), so the analysis is over the
//! statement text and is deliberately narrower than Go's in the ways
//! `refusals` below names. Every refusal keeps the join, which is the answer
//! this tier gave before the rule existed -- so a shape this walk cannot
//! prove is a plan that is merely worse, never wrong.
//!
//! # What proves the inner side is unread
//!
//! Not a second reference walk: the CANDIDATE statement -- the one whose
//! `FROM` no longer has the inner table and whose `ON` is gone with it -- is
//! handed to [`super::leaf_demand::LeafDemand`], the same walk the access
//! path costing already trusts, and the inner table is eliminable only when
//! that demand asks it for NO column. `LeafDemand` over-approximates (a bare
//! `c` is charged to every leaf owning a `c`, and a construct it cannot see
//! through charges everything), and here the over-approximation lands on the
//! safe side: it refuses eliminations Go would make, never the reverse.
//!
//! # Refusals
//!
//! * anything but the TOP node of the `FROM` -- a nested join needs the
//!   parent-column set at ITS level, which the statement text does not carry;
//! * `NATURAL`/`USING`, which coalesce column names across the two sides, so
//!   dropping one side changes what a bare name means;
//! * an inner side that is not one plain base table with row storage (a
//!   derived table, a view, a nested join: no unique keys to check);
//! * an inner table carrying index hints, `PARTITION`, `AS OF` or
//!   `TABLESAMPLE`, or a statement carrying `/*+ ... */` hints -- dropping
//!   the table would drop the hint reporting that names it;
//! * an `ON` conjunct that is not `outer_qualified_col = inner_qualified_col`
//!   -- Go tolerates other conjuncts and merely ignores them for key
//!   extraction; refusing them here keeps the key set provably complete
//!   without a reference walk over each conjunct;
//! * `<=>` as a join key, which matches NULLs to NULLs and therefore does not
//!   inherit a nullable unique key's at-most-one-match guarantee (Go excludes
//!   exactly these from `NullableUK`).

use tidb_ast::{Expr, Join, JoinNode, JoinType, SelectField, SelectStmt};

use super::catalog::{Catalog, TableEntry};
use super::leaf_demand::LeafDemand;

/// The statement with its top-level outer join replaced by its outer side, or
/// `None` when no elimination is provable.
pub(crate) fn eliminate(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Option<SelectStmt> {
    // A hint names tables by the shape the statement was written in, and this
    // rewrite changes that shape.
    if !select.hints.is_empty() {
        return None;
    }
    let join = top_join(select.from.as_ref()?)?;
    let right = join.right.as_ref()?;
    if join.natural || !join.using.is_empty() {
        return None;
    }
    let (outer, inner) = match join.tp {
        JoinType::Left => (&join.left, right),
        JoinType::Right => (right, &join.left),
        JoinType::Cross => return None,
    };
    let inner_ref = match inner {
        JoinNode::Table(table_ref) => table_ref,
        _ => return None,
    };
    if !inner_ref.hints.is_empty()
        || !inner_ref.partitions.is_empty()
        || inner_ref.as_of.is_some()
        || inner_ref.sample.is_some()
    {
        return None;
    }
    let (database, name) = split_name(&inner_ref.name, current_db)?;
    let entry = catalog.get_in(database, name)?;
    let visible = inner_ref
        .alias
        .clone()
        .unwrap_or_else(|| name.to_ascii_lowercase());

    // The candidate: the same statement reading only the outer side.
    let mut candidate = select.clone();
    candidate.from = Some(Join {
        left: outer.clone(),
        right: None,
        tp: JoinType::Cross,
        straight: false,
        on: None,
        using: Vec::new(),
        natural: false,
        explicit_parens: false,
    });
    if !LeafDemand::of_select(&candidate)
        .needed(&visible, &entry.column_types())
        .is_empty()
    {
        return None;
    }
    // Go carries SELECT DISTINCT as duplicate-agnostic aggregate columns into
    // `tryToEliminateOuterJoin`. Once no inner column survives, duplicate
    // matches cannot alter the result, even when the inner key is not unique.
    if select.distinct || direct_duplicate_agnostic_aggregate(select) {
        return Some(candidate);
    }

    let keys = inner_join_keys(join.on.as_ref()?, &visible)?;
    if !keys_contain_unique_key(entry, &keys) {
        return None;
    }
    Some(candidate)
}

/// The direct, ungrouped subset of Go `GetDupAgnosticAggCols`: every output
/// is `MAX`/`MIN` or carries `DISTINCT`, so multiplying an outer row through
/// an unread inner relation cannot alter the result.
fn direct_duplicate_agnostic_aggregate(select: &SelectStmt) -> bool {
    if !select.group_by.is_empty() || select.having.is_some() || !select.order_by.is_empty() {
        return false;
    }
    let mut has_aggregate = false;
    for field in select.fields.fields() {
        let SelectField::Expr { expr, .. } = field else {
            return false;
        };
        let Expr::Aggregate { name, distinct, .. } = expr else {
            return false;
        };
        has_aggregate = true;
        if !distinct && !name.eq_ignore_ascii_case("MAX") && !name.eq_ignore_ascii_case("MIN") {
            return false;
        }
    }
    has_aggregate
}

/// The `FROM`'s real join node, unwrapping the single-relation wrapper the
/// comma form parses into (`build_join` unwraps the same one).
fn top_join(from: &Join) -> Option<&Join> {
    match (&from.right, &from.left) {
        (None, JoinNode::Join(nested)) => top_join(nested),
        (None, _) => None,
        (Some(_), _) => Some(from),
    }
}

/// `db.t` / `t` split against the session's current database, lowercased --
/// the same resolution `build_from` performs, minus the error reporting.
fn split_name<'a>(path: &'a [String], current_db: &'a str) -> Option<(&'a str, &'a str)> {
    match path {
        [name] => Some((current_db, name)),
        [database, name] => Some((database, name)),
        _ => None,
    }
}

/// The inner side's columns named by the `ON` clause's equalities, lowercased.
///
/// `None` for any conjunct that is not `qualified_col = qualified_col` with
/// exactly one side qualified by `inner`, which is this port's stricter
/// reading of Go's `extractInnerJoinKeys` (see the module doc).
fn inner_join_keys(on: &Expr, inner: &str) -> Option<Vec<String>> {
    let inner = inner.to_ascii_lowercase();
    let mut keys = Vec::new();
    let mut stack = vec![on];
    while let Some(expr) = stack.pop() {
        match expr {
            Expr::Binary(tidb_ast::BinaryOp::LogicAnd, left, right) => {
                stack.push(left);
                stack.push(right);
            }
            Expr::Binary(tidb_ast::BinaryOp::Eq, left, right) => {
                let left = qualified_column(left)?;
                let right = qualified_column(right)?;
                // Exactly one side is the inner table's, so the other names
                // some other relation and the equality is a join key.
                let key = match (left.0 == inner, right.0 == inner) {
                    (true, false) => left.1,
                    (false, true) => right.1,
                    _ => return None,
                };
                keys.push(key);
            }
            _ => return None,
        }
    }
    (!keys.is_empty()).then_some(keys)
}

/// `(qualifier, column)` of a written `t.c` / `db.t.c`, both lowercased.
fn qualified_column(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::Column(path) => match path.as_slice() {
            [table, column] | [_, table, column] => {
                Some((table.to_ascii_lowercase(), column.to_ascii_lowercase()))
            }
            _ => None,
        },
        _ => None,
    }
}

/// Go's `isInnerJoinKeysContainUniqueKey` + `isInnerJoinKeysContainIndex`:
/// some key of the inner table has all its columns among the join keys.
///
/// The three key kinds this tier stores are the integer handle
/// (`TableInfo.PKIsHandle`), the clustered common handle
/// (`TableInfo.IsCommonHandle`) and a unique index. A prefix index is
/// excluded for the same reason Go's `PointGetPlan` declines one: an entry
/// found by a prefix does not identify the row.
fn keys_contain_unique_key(entry: &TableEntry, keys: &[String]) -> bool {
    let TableEntry::Kv(table) = entry else {
        return false;
    };
    let columns = table.visible_columns();
    let named = |offset: usize| -> bool {
        columns
            .get(offset)
            .is_some_and(|column| keys.contains(&column.name.to_ascii_lowercase()))
    };
    if table.pk_handle_offset().is_some_and(&named) {
        return true;
    }
    let common_handle = table.common_handle_offsets();
    if !common_handle.is_empty() && common_handle.iter().all(|offset| named(*offset)) {
        return true;
    }
    table.indexes().iter().any(|index| {
        index.unique
            && index.visible
            && !index.has_prefix()
            && !index.column_offsets.is_empty()
            && index.column_offsets.iter().all(|offset| named(*offset))
    })
}
