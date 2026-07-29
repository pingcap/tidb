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

//! Materialization of one CTE body, Go `buildWith`/`buildRecursiveCTE` plus
//! `CTEExec`'s iterative producer.
//!
//! Every CTE goes through [`materialize_cte_body`], which is ONE producer
//! rather than a case per shape: a bare `SELECT` body runs once, a
//! `UNION`-bodied one that never names itself runs once through the ordinary
//! set-operation fold, and a self-naming one runs the fixpoint. `RECURSIVE` is
//! a CLAUSE-level flag, not a per-CTE one, so which of those applies is
//! decided by whether the body actually names the CTE -- not by the keyword
//! (captured: `WITH RECURSIVE t AS (SELECT 1)` is valid and non-recursive,
//! while `WITH t AS (SELECT 1 UNION ALL SELECT n+1 FROM t ...)` -- no
//! `RECURSIVE` -- is `ErrNoSuchTable`, because the self-reference resolves to
//! no table at all).
//!
//! The fixpoint, captured against Go with `gorun`:
//!
//! - The blocks split into a LEADING run of non-self-naming seed blocks and a
//!   TRAILING run of recursive ones. Both runs may hold more than one block
//!   (`SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT n+1 FROM t WHERE n<4`
//!   seeds with `{1,2}`; two recursive blocks both fire every round). Any
//!   other interleaving is `ErrCTERecursiveRequiresNonRecursiveFirst`.
//! - Each round evaluates the recursive blocks with the CTE bound to ONLY the
//!   rows the PREVIOUS round added -- the delta, Go's `iterInTbl` -- never the
//!   whole accumulated table. This is required, not an optimization: the
//!   classic counter yields `1,2,3,4,5`, which a whole-table rescan would
//!   double-count.
//! - `UNION` deduplicates against the WHOLE accumulated result every round
//!   (Go's hash table over `resTbl`), which is what makes a cyclic graph
//!   terminate; `UNION ALL` does not, and the same cycle then runs until the
//!   depth bound refuses it.
//! - The bound is `@@cte_max_recursion_depth` rounds (default `1000`), and the
//!   round it REFUSES is the one it reports: a limit of `3` aborts "after 4
//!   iterations", the default aborts "after 1001".
//! - A `LIMIT`/`OFFSET` on the CTE's own definition caps the TOTAL accumulated
//!   row count across every round and stops the fixpoint early, so it can end
//!   a recursion the depth bound would otherwise refuse. A round may overshoot
//!   the target -- the surplus is dropped in the order the blocks produced it.
//!
//! Refused by name rather than approximated, each a genuine Go error this
//! reproduces with Go's own code: a self-join or any subquery/derived-table
//! reference inside a recursive block (`3577` -- Go demands exactly one
//! plain-`FROM` reference), an aggregate or `GROUP BY` (`3575`), the
//! definition's own `ORDER BY` and a block's `DISTINCT` (`1235`), and
//! `EXCEPT`/`INTERSECT` between the seed and recursive parts (`1235`).
//!
//! REFUSED (not modelled): recursive blocks joined by a MIX of `UNION` and
//! `UNION ALL`. Go accepts it, but which modifier then governs the
//! accumulate-and-dedup fold is not something a capture pins down, and
//! guessing wrong is a silently over- or under-deduplicated answer.

use tidb_ast::{Expr, Join, JoinNode, QueryStmt, SelectStmt, SetOp, SetOprStmt, SetOprTermBody};

use super::{
    combine_set_opr, eval_limit_bound, run_select_stmt, run_set_opr_stmt, Catalog, DriverError,
    MemTable, SelectMeta,
};
use crate::StmtContext;

/// Materializes one CTE's body into its columns and rows. `recursive_clause`
/// is the `WITH RECURSIVE` keyword, which only ever PERMITS recursion -- the
/// body itself decides whether any happens.
pub(super) fn materialize_cte_body(
    name: &str,
    column_names: &[String],
    query: &QueryStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &StmtContext,
    recursive_clause: bool,
) -> Result<SelectMeta, DriverError> {
    match query {
        QueryStmt::Select(select) => {
            // A bare `SELECT` body that names itself has no seed to start
            // from, which Go reports before ever resolving the name.
            if recursive_clause && select_self_refs(select, name) > 0 {
                return Err(DriverError::CteRecursiveRequiresUnion(name.to_owned()));
            }
            let (columns, rows) = run_select_stmt(select, catalog, current_db, ctx)?;
            Ok((apply_column_list(columns, column_names)?, rows))
        }
        QueryStmt::SetOpr(set_opr) => {
            let self_refs: usize = set_opr
                .terms
                .iter()
                .map(|term| term_self_refs(&term.body, name))
                .sum();
            if self_refs == 0 {
                // Never names itself, so the ordinary fold applies whether or
                // not the clause said RECURSIVE -- and the definition's own
                // ORDER BY / LIMIT and mixed operators come with it.
                let (columns, rows) = run_set_opr_stmt(set_opr, catalog, current_db, ctx)?;
                return Ok((apply_column_list(columns, column_names)?, rows));
            }
            if !recursive_clause {
                // Without RECURSIVE the self-reference names no table at all.
                return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
                    format!("{current_db}.{name}"),
                )));
            }
            run_fixpoint(name, column_names, set_opr, catalog, current_db, ctx)
        }
    }
}

/// The seed rows, then rounds of the recursive blocks until one adds nothing,
/// the `LIMIT` target is reached, or the depth bound refuses the next round.
fn run_fixpoint(
    name: &str,
    column_names: &[String],
    set_opr: &SetOprStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &StmtContext,
) -> Result<SelectMeta, DriverError> {
    if !set_opr.order_by.is_empty() {
        return Err(DriverError::NotSupportedYet(
            "ORDER BY over UNION in recursive Common Table Expression",
        ));
    }
    let split = split_blocks(name, set_opr)?;
    let target = limit_target(set_opr)?;

    // The seed is the leading non-recursive run, folded by exactly the
    // machinery an ordinary set operation uses.
    let mut seed = SetOprStmt {
        terms: set_opr.terms[..split.recursive_from].to_vec(),
        order_by: Vec::new(),
        limit: None,
        lock: None,
        outer_order_by: Vec::new(),
        outer_limit: None,
        outer_lock: None,
        ..set_opr.clone()
    };
    seed.terms[0].op = None;
    let (columns, mut accumulated) = run_set_opr_stmt(&seed, catalog, current_db, ctx)?;
    // The rename applies to the SEED's columns, before any round runs: a
    // recursive block reads the CTE by its RENAMED column names.
    let columns = apply_column_list(columns, column_names)?;

    // Reaching the target before any round runs (`LIMIT 0`, or a seed that
    // already fills it) is the same check the loop makes, not a special case:
    // it just empties the delta, so no round ever runs.
    if let Some(t) = target {
        if accumulated.len() >= t {
            accumulated.truncate(t);
            return Ok((columns, apply_definition_limit(accumulated, set_opr)?));
        }
    }
    let mut delta = accumulated.clone();
    let mut round: u64 = 0;
    let depth = ctx.cte_max_recursion_depth();
    let mut scratch = catalog.clone();
    while !delta.is_empty() {
        round += 1;
        if round > depth {
            return Err(DriverError::CteMaxRecursionDepth(round));
        }
        // The recursive blocks see the previous round's new rows only.
        scratch.register_mem_in(
            current_db,
            name,
            MemTable {
                columns: columns.clone(),
                rows: delta,
            },
        );
        let mut produced = Vec::new();
        for term in &set_opr.terms[split.recursive_from..] {
            let SetOprTermBody::Select(select) = &term.body else {
                unreachable!("split_blocks rejects a nested recursive block")
            };
            let (_, rows) = run_select_stmt(select, &scratch, current_db, ctx)?;
            produced.extend(rows);
        }
        let grown = combine_set_opr(split.op, accumulated.clone(), produced)?;
        // `combine_set_opr` keeps the accumulated rows as an unchanged prefix
        // under both UNION kinds, so the new suffix is the next delta.
        delta = grown[accumulated.len().min(grown.len())..].to_vec();
        accumulated = grown;
        if let Some(t) = target {
            if accumulated.len() >= t {
                accumulated.truncate(t);
                break;
            }
        }
    }
    Ok((columns, apply_definition_limit(accumulated, set_opr)?))
}

/// Where the recursive blocks begin, and the operator that folds them.
struct BlockSplit {
    recursive_from: usize,
    op: SetOp,
}

/// Validates the seed/recursive split and every recursive block against the
/// same restrictions Go enforces; see this module's doc.
fn split_blocks(name: &str, set_opr: &SetOprStmt) -> Result<BlockSplit, DriverError> {
    let recursive_from = set_opr
        .terms
        .iter()
        .position(|term| term_self_refs(&term.body, name) > 0)
        .expect("caller checked some term names the CTE");
    if recursive_from == 0 {
        return Err(DriverError::CteRecursiveRequiresNonRecursiveFirst(
            name.to_owned(),
        ));
    }
    let mut op: Option<SetOp> = None;
    for term in &set_opr.terms[recursive_from..] {
        // A non-recursive block after a recursive one breaks the "seed blocks
        // first, then recursive ones" shape Go requires.
        if term_self_refs(&term.body, name) == 0 {
            return Err(DriverError::CteRecursiveRequiresNonRecursiveFirst(
                name.to_owned(),
            ));
        }
        let SetOprTermBody::Select(select) = &term.body else {
            return Err(DriverError::Unsupported(
                "a parenthesized nested set operation as a WITH RECURSIVE block",
            ));
        };
        check_recursive_block(name, select)?;
        let this_op = term.op.expect("a term after the first carries an operator");
        match this_op {
            SetOp::Union { .. } => {}
            SetOp::Except { .. } => {
                return Err(DriverError::NotSupportedYet(
                    "EXCEPT between seed part and recursive part, hint: The operator between \
                     seed part and recursive part must bu UNION[DISTINCT] or UNION ALL",
                ))
            }
            SetOp::Intersect { .. } => {
                return Err(DriverError::NotSupportedYet(
                    "INTERSECT between seed part and recursive part, hint: The operator between \
                     seed part and recursive part must bu UNION[DISTINCT] or UNION ALL",
                ))
            }
        }
        match op {
            None => op = Some(this_op),
            Some(previous) if previous == this_op => {}
            Some(_) => {
                return Err(DriverError::Unsupported(
                    "WITH RECURSIVE blocks joined by a mix of UNION and UNION ALL",
                ))
            }
        }
    }
    Ok(BlockSplit {
        recursive_from,
        op: op.expect("the recursive run is non-empty"),
    })
}

/// One recursive block's own restrictions.
fn check_recursive_block(name: &str, select: &SelectStmt) -> Result<(), DriverError> {
    if select.distinct {
        return Err(DriverError::NotSupportedYet(
            "SELECT DISTINCT in recursive query block of Common Table Expression",
        ));
    }
    // Go reports GROUP BY under the same "neither aggregation nor window
    // functions" error an aggregate call gets (captured).
    if !select.group_by.is_empty() || select_aggregates(select) {
        return Err(DriverError::CteRecursiveForbidsAggregation(name.to_owned()));
    }
    // Exactly one plain `FROM` reference: a self-join names it twice, and a
    // derived table or scalar subquery hides it where Go refuses to look.
    if select.from.as_ref().map_or(0, |from| join_refs(from, name)) != 1
        || select_subquery_refs(select, name) > 0
    {
        return Err(DriverError::CteRecursiveForbiddenJoinOrder(name.to_owned()));
    }
    Ok(())
}

/// A CTE's explicit `(c1, c2, ...)` column list, renaming its body's output
/// columns positionally. A width mismatch is Go's `ErrViewWrongList`.
fn apply_column_list(
    mut columns: Vec<(String, tidb_datatype::FieldType)>,
    names: &[String],
) -> Result<Vec<(String, tidb_datatype::FieldType)>, DriverError> {
    if names.is_empty() {
        return Ok(columns);
    }
    if names.len() != columns.len() {
        return Err(DriverError::ViewWrongList);
    }
    for (column, name) in columns.iter_mut().zip(names) {
        column.0 = name.clone();
    }
    Ok(columns)
}

/// The `offset + count` total to accumulate before stopping.
fn limit_target(set_opr: &SetOprStmt) -> Result<Option<usize>, DriverError> {
    let Some(limit) = &set_opr.limit else {
        return Ok(None);
    };
    let count = eval_limit_bound(&limit.count)? as usize;
    let offset = match &limit.offset {
        Some(expr) => eval_limit_bound(expr)? as usize,
        None => 0,
    };
    Ok(Some(offset.saturating_add(count)))
}

/// The definition's own `LIMIT` windowing, applied once at the end exactly as
/// an ordinary `LIMIT` is.
fn apply_definition_limit(
    rows: Vec<Vec<tidb_datatype::Datum>>,
    set_opr: &SetOprStmt,
) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
    let Some(limit) = &set_opr.limit else {
        return Ok(rows);
    };
    let count = eval_limit_bound(&limit.count)? as usize;
    let offset = match &limit.offset {
        Some(expr) => eval_limit_bound(expr)? as usize,
        None => 0,
    };
    Ok(rows.into_iter().skip(offset).take(count).collect())
}

/// How many times `name` is a plain `FROM` table reference in one set-operation
/// block, recursing through a nested block's own terms.
fn term_self_refs(body: &SetOprTermBody, name: &str) -> usize {
    match body {
        SetOprTermBody::Select(select) => select_self_refs(select, name),
        SetOprTermBody::Nested(nested) => nested
            .terms
            .iter()
            .map(|term| term_self_refs(&term.body, name))
            .sum(),
    }
}

/// Every reference `name` gets anywhere in one `SELECT` -- its `FROM` tree,
/// its derived tables, and its expression subqueries. Used to decide whether a
/// body is recursive at all, so it must NOT undercount: a reference the split
/// misses would be resolved against a real table (or nothing) instead.
fn select_self_refs(select: &SelectStmt, name: &str) -> usize {
    select.from.as_ref().map_or(0, |from| join_refs(from, name))
        + derived_refs(select, name)
        + select_subquery_refs(select, name)
}

/// Plain `FROM` table references only -- not a derived table's own body.
fn join_refs(join: &Join, name: &str) -> usize {
    fn node(node: &JoinNode, name: &str) -> usize {
        match node {
            JoinNode::Table(table) => {
                usize::from(table.name.len() == 1 && table.name[0].eq_ignore_ascii_case(name))
            }
            JoinNode::Derived { .. } => 0,
            JoinNode::Join(inner) => join_refs(inner, name),
        }
    }
    node(&join.left, name) + join.right.as_ref().map_or(0, |right| node(right, name))
}

/// References inside a derived table's own body, which Go refuses in a
/// recursive block but which still make the body recursive.
fn derived_refs(select: &SelectStmt, name: &str) -> usize {
    fn node(node: &JoinNode, name: &str) -> usize {
        match node {
            JoinNode::Table(_) => 0,
            JoinNode::Derived { subquery, .. } => query_refs(subquery, name),
            JoinNode::Join(inner) => walk(inner, name),
        }
    }
    fn walk(join: &Join, name: &str) -> usize {
        node(&join.left, name) + join.right.as_ref().map_or(0, |right| node(right, name))
    }
    select.from.as_ref().map_or(0, |from| walk(from, name))
}

fn query_refs(query: &QueryStmt, name: &str) -> usize {
    match query {
        QueryStmt::Select(select) => select_self_refs(select, name),
        QueryStmt::SetOpr(set_opr) => set_opr
            .terms
            .iter()
            .map(|term| term_self_refs(&term.body, name))
            .sum(),
    }
}

/// References from a scalar/`IN`/`EXISTS` subquery written in any expression
/// of `select` -- the "not in any subquery" half of Go's rule.
fn select_subquery_refs(select: &SelectStmt, name: &str) -> usize {
    let mut total = 0;
    let mut visit = |expr: &Expr| total += expr_subquery_refs(expr, name);
    for field in &select.fields {
        if let tidb_ast::SelectField::Expr { expr, .. } = field {
            visit(expr);
        }
    }
    for expr in select.where_clause.iter().chain(select.having.iter()) {
        visit(expr);
    }
    for item in &select.group_by {
        visit(&item.expr);
    }
    for item in &select.order_by {
        visit(&item.expr);
    }
    total
}

fn expr_subquery_refs(expr: &Expr, name: &str) -> usize {
    struct Counter<'a> {
        name: &'a str,
        total: usize,
    }
    impl tidb_ast::Visitor for Counter<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(query) = node.downcast_ref::<QueryStmt>() {
                self.total += query_refs(query, self.name);
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut counter = Counter { name, total: 0 };
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut counter);
    counter.total
}

/// Whether any select field aggregates or calls a window function, which is
/// what Go's `ErrCTERecursiveForbidsAggregation` names.
///
/// The scan stops at a nested query: an aggregate inside a scalar subquery
/// belongs to that subquery, not to this block -- and Go agrees, reporting
/// `3577` (the reference rule) rather than `3575` for `SELECT (SELECT MAX(n)
/// FROM t)+1 FROM t` (captured).
fn select_aggregates(select: &SelectStmt) -> bool {
    struct Found(bool);
    impl tidb_ast::Visitor for Found {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if node.downcast_ref::<QueryStmt>().is_some() {
                return true;
            }
            if let Some(expr) = node.downcast_ref::<Expr>() {
                if matches!(
                    expr,
                    Expr::Aggregate { .. } | Expr::GroupConcat { .. } | Expr::Window { .. }
                ) {
                    self.0 = true;
                    return true;
                }
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    select.fields.iter().any(|field| match field {
        tidb_ast::SelectField::Expr { expr, .. } => {
            let mut found = Found(false);
            let mut owned = expr.clone();
            tidb_ast::Visitable::accept(&mut owned, &mut found);
            found.0
        }
        tidb_ast::SelectField::Wildcard { .. } => false,
    })
}
