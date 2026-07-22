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

//! Non-recursive `WITH` (common table expression) desugaring: rewrites
//! every `FROM`-clause reference to a CTE name into an equivalent derived
//! table `(query) AS name`, so the executor's EXISTING derived-table
//! machinery (`crate::select`) handles everything else — self-joins,
//! chained CTEs referencing earlier ones, column renaming — for free,
//! with no new execution-time machinery needed at all. Called
//! unconditionally at the top of `crate::select::Database::select`, so
//! every nested subquery/derived-table `SELECT` gets this treatment too
//! (a no-op when there's nothing to desugar).
//!
//! Pure AST rewrite — no catalog access, so these are free functions, not
//! `Database` methods (unlike `crate::subquery`'s `resolve_subqueries`,
//! which actually EXECUTES a subquery and so genuinely needs `&self`).
//!
//! Scope: `RECURSIVE` is rejected ([`ExecError::Unsupported`]) rather
//! than silently misevaluated as non-recursive — a genuinely harder
//! feature (iterative evaluation) deliberately deferred. A CTE reference
//! inside a `WHERE`/`HAVING`/select-list subquery (as opposed to the
//! `FROM` clause) is NOT resolved — only [`rewrite_select_ctes`]'s own
//! `FROM`-tree walk runs; a deliberate, narrow scope boundary matching
//! how [`tidb_ast::SelectStmt::with`] itself is only recognized directly
//! at the top of a statement, not inside a subquery.

use tidb_ast::{Join, JoinNode, QueryStmt, SelectField, SelectStmt};

use crate::ExecError;

/// Rewrites `sel`'s `WITH` clause away entirely — a no-op (returns `sel`
/// cloned, unchanged) when `sel.with` is `None`. Only called for a
/// NON-recursive `WITH` clause whose every CTE has a plain `QueryStmt::Select`
/// body — `crate::select::Database::select_scoped`'s own dispatch routes
/// anything else (`with.recursive`, or any CTE with a `UNION`/`UNION
/// ALL`-bodied, `QueryStmt::SetOpr` definition) to `crate::recursive_cte`
/// instead, as a `Database` method, since a genuinely recursive body needs
/// real query EXECUTION (fixpoint iteration) regardless — even though
/// [`tidb_ast::JoinNode::Derived`]'s own `subquery` field CAN now hold a
/// `QueryStmt::SetOpr` body too (see its own doc), this function's own
/// [`rewrite_join_node_ctes`] deliberately does NOT rewrite CTE
/// references nested inside one (a narrow, documented scope boundary, not
/// an inherited type limitation). Each CTE's OWN query is resolved against only
/// the EARLIER CTEs in the same clause (never itself or a later one),
/// matching non-recursive semantics; a CTE's query is re-resolved fresh
/// at EVERY reference to it (rather than materializing its rows once and
/// sharing them) — observably identical here, since a CTE's query is
/// read-only over tables that don't change mid-statement, the same
/// reasoning this project already applies to an uncorrelated subquery
/// re-executing per outer row.
pub(crate) fn desugar_ctes(sel: &SelectStmt) -> Result<SelectStmt, ExecError> {
    let Some(with) = &sel.with else {
        return Ok(sel.clone());
    };
    debug_assert!(!with.recursive, "caller routes RECURSIVE elsewhere");
    let mut resolved: Vec<(String, SelectStmt)> = Vec::new();
    for cte in &with.ctes {
        // `crate::select::Database::select_scoped`'s own dispatch routes
        // any `WITH` clause containing a `QueryStmt::SetOpr`-bodied CTE to
        // `crate::recursive_cte` instead, so every CTE reaching this
        // AST-rewrite path is guaranteed `QueryStmt::Select`.
        let QueryStmt::Select(query) = cte.query.as_ref() else {
            unreachable!("a UNION-bodied CTE is routed to crate::recursive_cte")
        };
        let mut query = rewrite_select_ctes(query, &resolved)?;
        if !cte.columns.is_empty() {
            query = rename_columns(query, &cte.columns)?;
        }
        resolved.push((cte.name.clone(), query));
    }
    let mut result = rewrite_select_ctes(sel, &resolved)?;
    result.with = None;
    Ok(result)
}

/// Rewrites `sel`'s own `FROM` tree against `ctes` (its `WHERE`/`HAVING`/
/// select-list expressions are deliberately left untouched — see this
/// module's own doc for the scope boundary).
fn rewrite_select_ctes(
    sel: &SelectStmt,
    ctes: &[(String, SelectStmt)],
) -> Result<SelectStmt, ExecError> {
    let mut result = sel.clone();
    if let Some(from) = &result.from {
        result.from = Some(rewrite_join_ctes(from, ctes)?);
    }
    Ok(result)
}

fn rewrite_join_ctes(j: &Join, ctes: &[(String, SelectStmt)]) -> Result<Join, ExecError> {
    Ok(Join {
        left: rewrite_join_node_ctes(&j.left, ctes)?,
        right: j
            .right
            .as_ref()
            .map(|r| rewrite_join_node_ctes(r, ctes))
            .transpose()?,
        ..j.clone()
    })
}

fn rewrite_join_node_ctes(
    node: &JoinNode,
    ctes: &[(String, SelectStmt)],
) -> Result<JoinNode, ExecError> {
    Ok(match node {
        // A CTE is always referenced unqualified (no db-qualification),
        // matching a single-segment table name.
        JoinNode::Table(tr) if tr.name.len() == 1 => {
            match ctes
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(&tr.name[0]))
            {
                Some((_, query)) => JoinNode::Derived {
                    subquery: tidb_ast::NodeBox::new(QueryStmt::Select(Box::new(query.clone()))),
                    alias: Some(tr.alias.clone().unwrap_or_else(|| tr.name[0].clone())),
                    lateral: false,
                    column_names: Vec::new(),
                },
                None => node.clone(),
            }
        }
        // Recurse into an already-derived table's own subquery too (it
        // may itself reference a CTE) — but only when that subquery is
        // itself a plain `QueryStmt::Select` (a `QueryStmt::SetOpr`-bodied derived
        // table's own terms are deliberately left unrewritten here — a
        // CTE reference nested inside a `UNION`-bodied derived table's
        // own branches surfaces as an honest `UnknownTable` at execution
        // instead, a narrow scope boundary matching this module's own
        // established "only the FROM-tree walk runs" doc, not a bug).
        JoinNode::Derived {
            subquery,
            alias,
            lateral,
            column_names,
        } => {
            let mut rewritten = subquery.clone();
            if let QueryStmt::Select(select) = rewritten.as_mut() {
                *select = Box::new(rewrite_select_ctes(select, ctes)?);
            }
            JoinNode::Derived {
                subquery: rewritten,
                alias: alias.clone(),
                lateral: *lateral,
                column_names: column_names.clone(),
            }
        }
        JoinNode::Join(inner) => JoinNode::Join(Box::new(rewrite_join_ctes(inner, ctes)?)),
        other => other.clone(),
    })
}

/// Applies a CTE's explicit column rename list (`WITH a (m, n) AS ...`)
/// by setting each select-list field's OWN alias — the SAME mechanism an
/// ordinary `AS` alias already uses, so it flows through
/// `crate::select::Database::derived_columns` completely unchanged.
fn rename_columns(mut query: SelectStmt, names: &[String]) -> Result<SelectStmt, ExecError> {
    if query.fields.len() != names.len() {
        return Err(ExecError::Unsupported("CTE column list arity mismatch"));
    }
    for (field, name) in query.fields.iter_mut().zip(names) {
        match field {
            SelectField::Expr { alias, .. } => *alias = Some(name.clone()),
            SelectField::Wildcard(_) => {
                return Err(ExecError::Unsupported("CTE column list with wildcard"))
            }
        }
    }
    Ok(query)
}
