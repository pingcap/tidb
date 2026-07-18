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

//! Materialized `WITH` evaluation: `WITH RECURSIVE`, plus any (even
//! non-`RECURSIVE`) CTE whose own body is a `UNION`/`UNION ALL`
//! (`QueryStmt::SetOpr`). Unlike `crate::cte`'s non-recursive `QueryStmt::Select`
//! desugaring (a pure, catalog-free AST rewrite into ordinary derived
//! tables), both cases here need real query EXECUTION — a fixpoint for a
//! genuinely self-referencing body, a single evaluate-and-fold for a
//! non-self-referencing `UNION` body — so this is a `Database` method, not
//! a free function.
//!
//! Design: [`Database::resolve_materialized_with`] materializes every CTE
//! in a `WITH` clause that reached this module (in written order) into a
//! `Relation` (already-computed columns + rows), appended to a growing
//! `Vec<(String, Relation)>` scope; the outer query (with `with` cleared)
//! is then resolved against that FULL scope via `crate::select`'s
//! `select_scoped`/`build_node`, which checks it BEFORE falling through to
//! the real catalog — the SAME "name shadows a real table" behavior
//! non-recursive CTEs already have, just backed by already-computed,
//! FROZEN rows instead of a re-resolvable subquery (`crate::cte`'s own
//! AST-rewrite trick cannot represent that freezing: `JoinNode::Derived`'s
//! `subquery` field, though it CAN now hold either a `QueryStmt::Select` or a
//! `QueryStmt::SetOpr` body — see its own doc — is always RE-EXECUTED fresh at
//! every reference, with no way to pin a fixed set of already-computed
//! rows in its place; a genuinely self-referencing body needs real
//! fixpoint iteration regardless, which the AST-rewrite trick can't
//! express at all).
//!
//! A CTE within a `WITH RECURSIVE` clause need not itself be
//! self-referencing (confirmed via `gorun`: `RECURSIVE` is a CLAUSE-level
//! flag, not per-CTE — `WITH RECURSIVE a AS (SELECT 1 AS n UNION ALL
//! SELECT n+1 FROM a WHERE n<2), b AS (SELECT * FROM a) SELECT * FROM b`
//! is real, valid SQL where `b` never references itself) — such a CTE is
//! simply executed once, the same as a non-recursive CTE would be,
//! [`Database::resolve_materialized_with`]'s own `QueryStmt::Select` arm; a
//! non-self-referencing `UNION`-bodied CTE (whether or not the clause said
//! `RECURSIVE`, and whether or not a SIBLING CTE in the same clause is
//! genuinely recursive) takes the analogous fast path in
//! [`Database::materialize_union_cte`], reusing
//! `crate::setopr::Database::setopr_scoped` — the exact machinery an
//! ordinary top-level `UNION` statement already uses — rather than a
//! separate restricted implementation, so the CTE's own `ORDER BY`/
//! `LIMIT` and mixed `UNION`/`UNION ALL` terms (both confirmed legal via
//! `gorun`) fall out for free. A `UNION`-bodied CTE that DOES
//! self-reference without `RECURSIVE` on the clause is a real error
//! (confirmed via `gorun`: the self-reference resolves to no table at
//! all, [`ExecError::UnknownTable`] — not a silent non-recursive
//! misevaluation).
//!
//! [`Database::materialize_union_cte`]'s self-referencing branch
//! implements the actual fixpoint: `so.terms[0]` is the base/seed term,
//! executed once; `so.terms[1..]` are
//! recursive terms, each iteration evaluating them with the CTE's own name
//! bound to ONLY the PREVIOUS iteration's newly-added rows (the "delta"),
//! NOT the whole accumulated table — confirmed via `gorun` this is
//! REQUIRED, not an optimization: `WITH RECURSIVE cte AS (SELECT 1 AS n
//! UNION ALL SELECT n+1 FROM cte WHERE n<5) SELECT * FROM cte` gives
//! `1,2,3,4,5` under `UNION ALL`, which a naive "recursive term re-scans
//! the WHOLE accumulated table every round" implementation would get
//! WRONG (it would re-derive `2` from `1` a second time once `2` itself
//! joins the table, producing a duplicate `2` that real TiDB never
//! produces). `UNION` deduplicates the WHOLE accumulated result every
//! round (confirmed via `gorun` with a diamond-shaped graph reachability
//! query: a node reached by two different paths appears TWICE under
//! `UNION ALL` but once under plain `UNION`); `combine`
//! (`crate::setopr::combine`, the SAME fold ordinary set operations use for an
//! ordinary top-level `UNION`) does the actual dedup, and its "preserves
//! first-seen order, `lhs` unchanged as a prefix" behavior is exactly what
//! lets the newly-added suffix be sliced off as the next round's delta
//! with no extra bookkeeping. Iteration stops when a round adds zero new
//! rows, or after [`MAX_RECURSION_DEPTH`] rounds — confirmed via `gorun`
//! to match real MySQL/TiDB's own `cte_max_recursion_depth` DEFAULT of
//! `1000` (`n < 1000` succeeds, `n < 1001` errors, for a one-row-per-round
//! counter) — not exposed as a settable session variable here, a
//! narrower scope boundary than real TiDB's own `SET
//! @@cte_max_recursion_depth`.
//!
//! Scope, every boundary confirmed via `gorun` to be a real MySQL/TiDB
//! restriction, not invented: a recursive term must reference its own
//! CTE's name EXACTLY ONCE in its `FROM` tree (a self-join, e.g. `FROM
//! cte c1, cte c2`, is a genuine `ERR`, checked in
//! [`Database::check_recursive_term`]); a recursive term may not contain
//! an aggregate, `DISTINCT`, `GROUP BY`, or `ORDER BY` (all genuine
//! `ERR`s — note this is the recursive TERM's own `ORDER BY`, a separate,
//! narrower restriction from the CTE DEFINITION's own trailing `ORDER
//! BY`, described next); every recursive term must use `UNION`/`UNION
//! ALL` (not `EXCEPT`/`INTERSECT`), and — a boundary this project imposes
//! rather than one independently confirmed against real TiDB for every
//! possible combination — must all share the SAME `ALL`/distinct
//! modifier, since mixing them within one CTE would need a materially
//! more complex per-term fold than the single running `combine` call this
//! implementation uses.
//!
//! A `LIMIT` (with an optional `OFFSET`) on the CTE DEFINITION's own
//! trailing clause (`so.limit`, as opposed to a term's own, rejected
//! above) IS modelled, a genuine early-termination optimization: it caps
//! the TOTAL accumulated row count across every round, confirmed via
//! `gorun` to stop the fixpoint well before a `WHERE` clause alone would
//! (`... WHERE n<1000000 LIMIT 5` stops after 5 rows, not ~1M rounds). A
//! round may overshoot the target by more than one row at once (a
//! self-referencing term with no bounding `WHERE`, or several recursive
//! terms firing the same round) — truncated to the first `target` rows,
//! in the SAME order the terms were just evaluated (confirmed via
//! `gorun` this is the exact subset real TiDB keeps), then iteration
//! stops immediately rather than completing further rounds. A `LIMIT`
//! exceeding the fixpoint's own natural (empty-delta) termination point
//! is simply never reached, a no-op. The CTE DEFINITION's own trailing
//! `ORDER BY`, unlike `LIMIT`, remains a genuine `ERR` in real TiDB
//! (confirmed via `gorun`) and stays rejected here too — ordering an
//! unbounded, still-growing fixpoint mid-iteration has no well-defined
//! per-round meaning the way capping a row COUNT does.

use tidb_ast::{
    Join, JoinNode, QueryStmt, SelectField, SelectStmt, SetOp, SetOprStmt, SetOprTerm,
    SetOprTermBody,
};

use crate::aggregate::expr_has_aggregate;
use crate::catalog::Relation;
use crate::setopr::combine;
use crate::{Database, ExecError, Row};

/// See this module's own doc for why `1000` matches real TiDB's default
/// `cte_max_recursion_depth`, confirmed via `gorun`.
const MAX_RECURSION_DEPTH: usize = 1000;

impl Database {
    /// Resolves a `WITH` clause via materialization: returns the outer
    /// query with `with` cleared, plus the FULL materialized-CTE scope for
    /// `crate::select`'s own `ctes` parameter (`inherited`, from an
    /// enclosing statement — always empty in practice today, since a
    /// `WITH` clause is only recognized directly at the top of a
    /// statement, never nested — followed by this clause's own CTEs, in
    /// written order, so a later one may reference an earlier one). The
    /// caller routes here whenever `with.recursive` is set OR any CTE in
    /// the clause has a `UNION`-bodied (`QueryStmt::SetOpr`) definition — a
    /// non-recursive `QueryStmt::Select`-bodied CTE still goes through
    /// `crate::cte`'s cheaper pure-AST-rewrite path instead.
    pub(crate) fn resolve_materialized_with(
        &self,
        sel: &SelectStmt,
        inherited: &[(String, Relation)],
    ) -> Result<(SelectStmt, Vec<(String, Relation)>), ExecError> {
        let with = sel
            .with
            .as_ref()
            .expect("caller checked sel.with.is_some()");
        let mut ctes: Vec<(String, Relation)> = inherited.to_vec();
        for cte in &with.ctes {
            let mut rel = match cte.query.as_ref() {
                QueryStmt::Select(s) => {
                    let rows = self.select_scoped(s, None, &ctes)?.rows;
                    let cols = self.derived_columns(s, &cte.name, &ctes)?;
                    Relation { cols, rows }
                }
                QueryStmt::SetOpr(so) => {
                    self.materialize_union_cte(&cte.name, so, &ctes, with.recursive)?
                }
            };
            if !cte.columns.is_empty() {
                rel = rename_relation_columns(rel, &cte.name, &cte.columns)?;
            }
            ctes.push((cte.name.clone(), rel));
        }
        let mut result = sel.clone();
        result.with = None;
        Ok((result, ctes))
    }

    /// Materializes a `UNION`/`UNION ALL`-bodied CTE (`name`'s own
    /// definition is `so`). Two genuinely different cases share this one
    /// entry point, distinguished by whether ANY term actually references
    /// `name` itself (checked across every term, seed included):
    ///
    /// - **No self-reference** (confirmed via `gorun` to be legal
    ///   regardless of whether the enclosing clause said `RECURSIVE` —
    ///   `RECURSIVE` is a clause-level flag, and a non-self-referencing
    ///   CTE inside a `WITH RECURSIVE` clause is simply executed once,
    ///   same as this): every term is evaluated exactly once and folded
    ///   via `crate::setopr::Database::setopr_scoped` — the SAME machinery
    ///   an ordinary top-level `UNION` statement already uses, so the
    ///   CTE's own `ORDER BY`/`LIMIT` and mixed `UNION`/`UNION ALL` terms
    ///   (both confirmed legal here via `gorun`) fall out for free with no
    ///   separate restricted code path.
    /// - **Self-reference present**: a genuinely recursive body, which
    ///   requires the enclosing clause to have said `RECURSIVE` — real
    ///   MySQL/TiDB behavior confirmed via `gorun`: without it, the
    ///   self-reference resolves to no table at all (there is no real
    ///   table by that name either), a real `UnknownTable` error, not a
    ///   silent misevaluation. When permitted, runs the standard
    ///   base-term-then-iterate-to-a-fixpoint algorithm — see this
    ///   module's own doc.
    fn materialize_union_cte(
        &self,
        name: &str,
        so: &SetOprStmt,
        prior: &[(String, Relation)],
        recursive_clause: bool,
    ) -> Result<Relation, ExecError> {
        let self_refs: usize = so.terms.iter().map(|t| term_self_refs(t, name)).sum();
        if self_refs == 0 {
            let rows = self.setopr_scoped(so, None, prior)?.rows;
            let cols = self.derived_columns(so.representative_select(), name, prior)?;
            return Ok(Relation { cols, rows });
        }
        if !recursive_clause {
            return Err(ExecError::UnknownTable(name.to_string()));
        }
        if !so.order_by.is_empty() {
            return Err(ExecError::Unsupported(
                "ORDER BY on a WITH RECURSIVE CTE's own definition",
            ));
        }
        // A `LIMIT` on the recursive CTE's own definition is a real
        // early-termination optimization (confirmed via `gorun`: it caps
        // the TOTAL accumulated row count across every round, not any one
        // term's own output — `WITH RECURSIVE cte AS (SELECT 1 AS n UNION
        // ALL SELECT n+1 FROM cte WHERE n<1000000 LIMIT 5) ...` stops
        // after 5 rows despite the `WHERE` clause alone permitting nearly
        // a million rounds). `offset + count` is the target total to
        // accumulate before stopping; the real `crate::order::apply_limit`
        // windowing (skip `offset`, take `count`) is applied once at the
        // very end, exactly as an ordinary `LIMIT` elsewhere.
        let target = so
            .limit
            .as_ref()
            .map(|limit| {
                let offset = limit
                    .offset
                    .as_ref()
                    .map_or(Ok(0), crate::order::const_usize)?;
                Ok::<usize, ExecError>(offset + crate::order::const_usize(&limit.count)?)
            })
            .transpose()?;
        // `parse_select_or_setopr` only ever builds a `SetOprStmt` when a
        // trailing set operator was actually seen, so `so.terms` always
        // has at least two elements — `rest` below is never empty.
        let [seed, rest @ ..] = so.terms.as_slice() else {
            unreachable!("a SetOprStmt always has at least two terms")
        };

        let seed_rows = self.term_rows(seed, None, prior)?;
        let cols = self.derived_columns(seed.body.representative_select(), name, prior)?;

        let mut op: Option<SetOp> = None;
        for term in rest {
            // A recursive term's own self-reference must be a bare
            // `SELECT` directly naming the CTE in its `FROM` (the fixed-
            // point loop below re-evaluates it once per round against a
            // growing `delta` relation) — a parenthesized NESTED set
            // operation as a recursive term is a real but obscure MySQL
            // shape this crate doesn't attempt (deliberately
            // `Unsupported`, not silently wrong), since it would need
            // its own recursive re-evaluation machinery, not just this
            // `SetOprTermBody` split.
            let SetOprTermBody::Select(term_sel) = &term.body else {
                return Err(ExecError::Unsupported(
                    "a WITH RECURSIVE term may not be a parenthesized nested set operation",
                ));
            };
            self.check_recursive_term(term_sel, name)?;
            let this_op = term.op.expect("non-first term has an operator");
            if !matches!(this_op, SetOp::Union { .. }) {
                return Err(ExecError::Unsupported(
                    "WITH RECURSIVE term must use UNION or UNION ALL",
                ));
            }
            match op {
                None => op = Some(this_op),
                Some(prev) if prev == this_op => {}
                Some(_) => {
                    return Err(ExecError::Unsupported(
                        "WITH RECURSIVE terms must share the same UNION kind",
                    ))
                }
            }
        }
        let op = op.expect("rest is non-empty");

        let mut accumulated = seed_rows;
        // Reached the `LIMIT` target before any recursive round even
        // runs (e.g. `LIMIT 0`, or a seed alone already meeting it) —
        // confirmed via `gorun` this short-circuits exactly like the
        // in-loop check below (an empty `delta` skips the `while` loop
        // entirely, so no recursive term ever runs), not a special case.
        let reached_target = target.is_some_and(|t| accumulated.len() >= t);
        if reached_target {
            accumulated.truncate(target.expect("reached_target implies target is Some"));
        }
        let mut delta = if reached_target {
            Vec::new()
        } else {
            accumulated.clone()
        };
        let mut round = 0usize;
        while !delta.is_empty() {
            round += 1;
            if round > MAX_RECURSION_DEPTH {
                return Err(ExecError::Unsupported(
                    "WITH RECURSIVE exceeded the maximum recursion depth (1000)",
                ));
            }
            let working = Relation {
                cols: cols.clone(),
                rows: delta,
            };
            let mut scope = prior.to_vec();
            scope.push((name.to_string(), working));
            let mut candidates: Vec<Row> = Vec::new();
            for term in rest {
                // Already validated as `Select` (never `Nested`) by the
                // loop above.
                let SetOprTermBody::Select(term_sel) = &term.body else {
                    unreachable!("a WITH RECURSIVE term's body was already validated as Select")
                };
                candidates.extend(self.select_scoped(term_sel, None, &scope)?.rows);
            }
            let combined = combine(accumulated.clone(), candidates, op);
            delta = combined[accumulated.len()..].to_vec();
            accumulated = combined;
            // A round may overshoot the target by more than one row (a
            // term with no self-reference in its own WHERE, or several
            // recursive terms firing the same round) — truncated to the
            // first `t` rows (in the SAME order the terms were just
            // evaluated), confirmed via `gorun` this is the exact subset
            // real TiDB keeps, then iteration stops.
            if let Some(t) = target {
                if accumulated.len() >= t {
                    accumulated.truncate(t);
                    break;
                }
            }
        }
        let rows = match &so.limit {
            Some(limit) => crate::order::apply_limit(accumulated, limit)?,
            None => accumulated,
        };
        Ok(Relation { cols, rows })
    }

    /// Validates one recursive term against the SAME restrictions real
    /// TiDB enforces (all confirmed via `gorun`, not assumed) — see this
    /// module's own doc for the exact list.
    fn check_recursive_term(&self, sel: &SelectStmt, name: &str) -> Result<(), ExecError> {
        if sel.distinct {
            return Err(ExecError::Unsupported("DISTINCT in a WITH RECURSIVE term"));
        }
        if !sel.group_by.is_empty() {
            return Err(ExecError::Unsupported("GROUP BY in a WITH RECURSIVE term"));
        }
        if !sel.order_by.is_empty() {
            return Err(ExecError::Unsupported("ORDER BY in a WITH RECURSIVE term"));
        }
        if sel.limit.is_some() {
            return Err(ExecError::Unsupported("LIMIT in a WITH RECURSIVE term"));
        }
        if sel
            .fields
            .iter()
            .any(|f| matches!(f, SelectField::Expr { expr, .. } if expr_has_aggregate(expr)))
        {
            return Err(ExecError::Unsupported("aggregate in a WITH RECURSIVE term"));
        }
        let refs = sel.from.as_ref().map_or(0, |f| count_cte_refs(f, name));
        if refs != 1 {
            return Err(ExecError::Unsupported(
                "a WITH RECURSIVE term must reference its own CTE exactly once",
            ));
        }
        Ok(())
    }
}

/// Counts how many times `name` (case-insensitively, unqualified —
/// matching how a CTE is always referenced) appears as a bare table
/// reference in `j`'s own tree, recursing into a nested join and a
/// derived table's own `FROM` (mirroring `crate::cte`'s own traversal
/// shape) — used to enforce the "exactly once" self-reference rule.
fn count_cte_refs(j: &Join, name: &str) -> usize {
    count_node_refs(&j.left, name) + j.right.as_ref().map_or(0, |r| count_node_refs(r, name))
}

/// A single [`SetOprTerm`]'s own self-reference count — a plain `SELECT`
/// checks its own `FROM`; a parenthesized [`SetOprTermBody::Nested`]
/// term recurses into ITS OWN terms (a self-reference can hide in any
/// branch of an arbitrarily-nested `UNION`). Shared by both places that
/// sum this across a `SetOprStmt`'s terms: the top-level "is this CTE
/// genuinely recursive at all" check, and a nested derived table's own
/// self-reference count below.
fn term_self_refs(term: &SetOprTerm, name: &str) -> usize {
    match &term.body {
        SetOprTermBody::Select(sel) => sel.from.as_ref().map_or(0, |f| count_cte_refs(f, name)),
        SetOprTermBody::Nested(so) => so.terms.iter().map(|t| term_self_refs(t, name)).sum(),
    }
}

fn count_node_refs(node: &JoinNode, name: &str) -> usize {
    match node {
        JoinNode::Table(tr) if tr.name.len() == 1 && tr.name[0].eq_ignore_ascii_case(name) => 1,
        JoinNode::Table(_) => 0,
        // A `QueryStmt::SetOpr`-bodied derived table's self-reference count is
        // the SUM across every term (a self-reference can hide in any
        // branch of the `UNION`) — undercounting here would let a
        // genuinely invalid `WITH RECURSIVE` term wrongly pass the
        // "exactly once" check below.
        JoinNode::Derived { subquery, .. } => match subquery.as_ref() {
            QueryStmt::Select(s) => s.from.as_ref().map_or(0, |f| count_cte_refs(f, name)),
            QueryStmt::SetOpr(so) => so.terms.iter().map(|t| term_self_refs(t, name)).sum(),
        },
        JoinNode::Join(inner) => count_cte_refs(inner, name),
    }
}

/// Applies a CTE's explicit column rename list (`WITH a (m, n) AS ...`) to
/// an already-materialized relation — the SAME contract
/// `crate::cte::rename_columns` enforces (arity must match) for the
/// non-recursive, not-yet-executed case, just applied to `Relation`'s own
/// `Column`s instead of a `SelectStmt`'s field aliases.
fn rename_relation_columns(
    mut rel: Relation,
    cte_name: &str,
    names: &[String],
) -> Result<Relation, ExecError> {
    if rel.cols.len() != names.len() {
        return Err(ExecError::Unsupported("CTE column list arity mismatch"));
    }
    for (col, name) in rel.cols.iter_mut().zip(names) {
        col.name = name.clone();
        col.tables = vec![cte_name.to_string()];
    }
    Ok(rel)
}
