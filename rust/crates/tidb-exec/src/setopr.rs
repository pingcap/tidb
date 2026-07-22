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

//! Set-operation execution (`UNION`/`UNION ALL`/`EXCEPT`/`INTERSECT`) for
//! both table-aware terms and the public table-less seed entrypoint.
//!
//! Go routes `ast.SetOprStmt` from `pkg/executor/compiler.go`; physical UNION
//! row production lives under `pkg/executor/unionexec/union.go`, while
//! EXCEPT/INTERSECT and DISTINCT are represented by their planned physical
//! shapes. This leaf owns the seed's common row-set fold so both entrypoints
//! use one implementation without a crate-root behavior path.

use tidb_datatype::Datum;
use tidb_expr::Columns;

use tidb_ast::{QueryStmt, SetOp, SetOprStmt, SetOprTerm, SetOprTermBody, Stmt};

use crate::catalog::Relation;
use crate::error::ExecError;
use crate::order::{apply_limit, cmp_keys, output_index};
use crate::result::{ResultSet, Row};
use crate::select::execute_select;
use crate::session::SessionState;
use crate::Database;

/// Executes a table-less SELECT or set operation.
///
/// This public contract is consumed by the differential result tests. It is
/// stateless, so session variables use source defaults and the wall clock has
/// no value.
pub fn execute(stmt: &Stmt) -> Result<ResultSet, ExecError> {
    match stmt {
        Stmt::Query(query) => match query.as_ref() {
            QueryStmt::Select(select) => execute_select(select, SessionState::default()),
            QueryStmt::SetOpr(setopr) => execute_setopr(setopr),
        },
        _ => Err(ExecError::NotSelect),
    }
}

/// Executes a set operation over table-less synthetic-row terms.
fn execute_setopr(setopr: &SetOprStmt) -> Result<ResultSet, ExecError> {
    if setopr.with.is_some() {
        return Err(ExecError::Unsupported("WITH before set operation"));
    }
    // Statement ORDER BY only reorders an unordered result label. LIMIT
    // changes row count and remains outside this table-less seed boundary.
    if setopr.limit.is_some() {
        return Err(ExecError::Unsupported("set-operation LIMIT"));
    }

    let mut rows = Vec::new();
    for (index, term) in setopr.terms.iter().enumerate() {
        let term_rows = match &term.body {
            SetOprTermBody::Select(select) => execute_select(select, SessionState::default())?.rows,
            SetOprTermBody::Nested(nested) => execute_setopr(nested)?.rows,
        };
        if index == 0 {
            rows = term_rows;
        } else {
            rows = combine(
                rows,
                term_rows,
                term.op.expect("non-first term has an operator"),
            );
        }
    }
    Ok(ResultSet::unordered(rows))
}

impl Database {
    pub(crate) fn setopr(
        &self,
        so: &SetOprStmt,
        outer: Option<&dyn Columns>,
    ) -> Result<ResultSet, ExecError> {
        self.setopr_scoped(so, outer, &[])
    }

    /// Evaluates one [`SetOprTerm`]'s own row set: a plain `SELECT`, or,
    /// for a parenthesized [`SetOprTermBody::Nested`] term, the fully
    /// combined-and-tail-applied result of that nested set operation
    /// (its own `ORDER BY`/`LIMIT` scoped to just that group — see
    /// `SetOprTermBody::Nested`'s own doc).
    pub(crate) fn term_rows(
        &self,
        term: &SetOprTerm,
        outer: Option<&dyn Columns>,
        ctes: &[(String, Relation)],
    ) -> Result<Vec<Row>, ExecError> {
        match &term.body {
            SetOprTermBody::Select(sel) => Ok(self.select_scoped(sel, outer, ctes)?.rows),
            SetOprTermBody::Nested(so) => Ok(self.setopr_scoped(so, outer, ctes)?.rows),
        }
    }

    /// Like [`Database::setopr`], but additionally resolves each term's
    /// `FROM`-clause table references against `ctes` first — the SAME
    /// `ctes` scope `crate::select::Database::select_scoped` accepts.
    /// Reused by `crate::recursive_cte` to materialize a non-recursive
    /// (no self-reference) `UNION`-bodied CTE by simply evaluating its
    /// terms once and folding them, exactly as an ordinary top-level
    /// `UNION` statement already does — no separate restricted code path
    /// needed, so a CTE's own `ORDER BY`/`LIMIT` and mixed `UNION`/`UNION
    /// ALL` terms fall out for free (both confirmed legal via `gorun`).
    pub(crate) fn setopr_scoped(
        &self,
        so: &SetOprStmt,
        outer: Option<&dyn Columns>,
        ctes: &[(String, Relation)],
    ) -> Result<ResultSet, ExecError> {
        if so.with.is_some() {
            return Err(ExecError::Unsupported("WITH before set operation"));
        }
        let mut acc: Vec<Row> = Vec::new();
        for (i, term) in so.terms.iter().enumerate() {
            // Each term's own ORDER BY/LIMIT is handled by `term_rows`;
            // the statement-level ordering below applies to the
            // combined result.
            let rows = self.term_rows(term, outer, ctes)?;
            if i == 0 {
                acc = rows;
            } else {
                acc = combine(acc, rows, term.op.expect("non-first term has an operator"));
            }
        }
        // Statement-level ORDER BY sorts the combined rows by output column;
        // union column identity comes from the first term's select list.
        let ordered = !so.order_by.is_empty();
        if ordered {
            let first = so
                .terms
                .first()
                .map(|t| &t.body.representative_select().fields[..])
                .unwrap_or(&[]);
            let idxs: Vec<(usize, bool)> = so
                .order_by
                .iter()
                .map(|item| Ok((output_index(item, first)?, item.desc)))
                .collect::<Result<_, ExecError>>()?;
            let descs: Vec<bool> = idxs.iter().map(|(_, d)| *d).collect();
            let mut keyed: Vec<(Vec<Datum>, Row)> = acc
                .into_iter()
                .map(|row| {
                    let kv = idxs
                        .iter()
                        .map(|&(i, _)| row.get(i).cloned().unwrap_or(Datum::Null))
                        .collect();
                    (kv, row)
                })
                .collect();
            keyed.sort_by(|a, b| cmp_keys(&a.0, &b.0, &descs));
            acc = keyed.into_iter().map(|(_, r)| r).collect();
        }
        if let Some(limit) = &so.limit {
            acc = apply_limit(acc, limit)?;
        }
        Ok(ResultSet { rows: acc, ordered })
    }
}

/// Folds `rhs` into the accumulated `lhs` per the set operator.
pub(crate) fn combine(lhs: Vec<Row>, rhs: Vec<Row>, op: SetOp) -> Vec<Row> {
    match op {
        SetOp::Union { all: true } => {
            let mut rows = lhs;
            rows.extend(rhs);
            rows
        }
        SetOp::Union { all: false } => {
            let mut rows = lhs;
            rows.extend(rhs);
            dedup(rows)
        }
        SetOp::Except { .. } => dedup(lhs)
            .into_iter()
            .filter(|row| !rhs.contains(row))
            .collect(),
        SetOp::Intersect { .. } => dedup(lhs)
            .into_iter()
            .filter(|row| rhs.contains(row))
            .collect(),
    }
}

fn dedup(rows: Vec<Row>) -> Vec<Row> {
    let mut unique = Vec::with_capacity(rows.len());
    for row in rows {
        if !unique.contains(&row) {
            unique.push(row);
        }
    }
    unique
}
