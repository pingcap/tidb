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

//! `SELECT` execution: building the (possibly joined) input relation, then
//! selection and either row-wise projection (also where window functions
//! are computed and spliced in — see `crate::window`'s own doc) or
//! handing off to `crate::aggregate` for `GROUP BY`/aggregation. Called
//! from `crate::database` (top-level `run`), `crate::subquery`, and
//! `crate::setopr`. `build_join`/`derived_columns`/`select_scoped` also
//! accept a `ctes: &[(String, Relation)]` scope — already-materialized
//! `WITH RECURSIVE` relations from `crate::recursive_cte`, checked before
//! a bare table name falls through to the real catalog (`build_node`'s
//! own `JoinNode::Table` arm) — empty for every ordinary statement, so
//! this costs nothing when no recursive CTE is in scope.

use tidb_ast::{
    Expr, Join, JoinNode, JoinType, QueryStmt, SelectField, SelectStatementKind, SelectStmt,
    WindowDef, WindowOver,
};
use tidb_datatype::Datum;
use tidb_expr::{eval_in, truthy_of, Columns, EvalError};
use tidb_planner::condition_binding::ConditionBindingError;
use tidb_planner::join_condition::{ColumnSpec, JoinSchema};
use tidb_planner::typed_condition::ConditionEvaluationMode;

use crate::aggregate::expr_has_aggregate;
use crate::catalog::{table_key, Column, Relation};
use crate::error::ExecError;
use crate::order::{apply_limit, cmp_keys, resolve_order_keys};
use crate::result::{ResultSet, Row};
use crate::session::RelResolver;
use crate::session::SessionState;
use crate::table_reference::{check_no_as_of, check_no_partition, check_no_table_sample};
use crate::typed_condition_eval::{
    compile_join_condition, compile_using_conditions, evaluate_join_condition,
    evaluate_join_conditions, finalize_outer_row_statuses, merge_outer_row_truth, OuterRowStatus,
    PredicateTruth, TypedConditionEvalError,
};
use crate::Database;

/// Executes one table-less SELECT over its synthetic input row.
pub(crate) fn execute_select(
    select: &SelectStmt,
    session: SessionState,
) -> Result<ResultSet, ExecError> {
    if select.kind == SelectStatementKind::Values {
        return Err(ExecError::Unsupported("VALUES"));
    }
    // A real FROM needs table data; FROM DUAL parses as no table source.
    if select.from.is_some() {
        return Err(ExecError::RequiresTable);
    }
    check_no_into_outfile(&select.into_outfile)?;

    let resolver = RelResolver::new(&[], &[], session);
    if let Some(predicate) = &select.where_clause {
        if !is_truthy(eval_in(predicate, &resolver)?)? {
            return Ok(ResultSet::default());
        }
    }

    let mut row = Row::with_capacity(select.fields.len());
    for field in &select.fields {
        match field {
            SelectField::Wildcard(_) => return Err(ExecError::Wildcard),
            SelectField::Expr { expr, .. } => row.push(eval_in(expr, &resolver)?),
        }
    }
    Ok(ResultSet::unordered(vec![row]))
}

/// Rejects SELECT INTO OUTFILE before ordinary row execution.
///
/// Go plans this statement in `pkg/planner/core/planbuilder.go` and executes
/// its non-result-set sink in `pkg/executor/select_into.go`; returning the
/// ordinary projection would therefore be a wrong-shaped outcome.
pub(crate) fn check_no_into_outfile(into_outfile: &Option<String>) -> Result<(), ExecError> {
    if into_outfile.is_none() {
        Ok(())
    } else {
        Err(ExecError::Unsupported("INTO OUTFILE clause"))
    }
}

impl Database {
    pub(crate) fn select(
        &self,
        sel: &SelectStmt,
        outer: Option<&dyn Columns>,
    ) -> Result<ResultSet, ExecError> {
        self.select_scoped(sel, outer, &[])
    }

    /// Like [`Database::select`], but additionally resolves `FROM`-clause
    /// table references against `ctes` FIRST (an already-materialized
    /// `WITH RECURSIVE` relation from an ENCLOSING scope, checked before
    /// falling through to the real catalog — see `crate::recursive_cte`'s
    /// own doc) before the real catalog. `select` itself is just this with
    /// an empty `ctes` — the vast majority of statements, which reference
    /// no recursive CTE at all, pay nothing extra.
    pub(crate) fn select_scoped(
        &self,
        sel: &SelectStmt,
        outer: Option<&dyn Columns>,
        ctes: &[(String, Relation)],
    ) -> Result<ResultSet, ExecError> {
        if sel.kind == SelectStatementKind::Values {
            // `VALUES` owns rows directly rather than through SELECT's
            // scan/projection shape. Do not let an empty `FROM` fall through
            // to `execute_select` and fabricate a one-row empty result.
            return Err(ExecError::Unsupported("VALUES"));
        }
        // `FOR UPDATE`/`FOR SHARE` changes transaction locking and, for
        // `OF`-qualified forms, the exact table set. The local executor does
        // not model that lock lifecycle, so reject the typed query before a
        // query-source INSERT can mutate its target instead of silently
        // executing an unlocked read.
        if sel.lock.is_some() {
            return Err(ExecError::Unsupported("SELECT locking"));
        }
        check_no_into_outfile(&sel.into_outfile)?;
        // A `WITH RECURSIVE` clause, or any CTE with a `UNION`-bodied
        // definition (even under a plain `WITH`), needs real query
        // EXECUTION (`crate::recursive_cte::Database::resolve_materialized_with`)
        // rather than `crate::cte::desugar_ctes`'s pure, catalog-free AST
        // rewrite (which can only represent a `QueryStmt::Select`-bodied CTE as
        // a derived table) — a no-op when `sel.with` is `None`, so every
        // nested subquery/derived-table `SELECT` pays only a cheap clone,
        // not a real cost.
        let needs_materialize = matches!(&sel.with, Some(with) if with.recursive
            || with.ctes.iter().any(|c| matches!(c.query.as_ref(), QueryStmt::SetOpr(_))));
        let (sel, ctes) = if needs_materialize {
            self.resolve_materialized_with(sel, ctes)?
        } else {
            (crate::cte::desugar_ctes(sel)?, ctes.to_vec())
        };
        // Named windows (`OVER w` / `WINDOW w AS (...)`, see
        // `crate::window`'s own doc) are resolved to an already-merged,
        // fully inline form here too, right alongside `WITH` — every
        // downstream consumer (this file's own window-collection below,
        // `crate::aggregate`'s, `Database::compute_window`,
        // `crate::aggregate::check_columns_pinned`) then only ever sees
        // the resolved shape.
        let sel = crate::window::resolve_named_windows(&sel)?;
        let sel = &sel;
        // select_rows produces rows already in ORDER BY order (if any); LIMIT
        // then truncates the final sequence.
        let mut rows = self.select_rows(sel, outer, &ctes)?;
        if let Some(limit) = &sel.limit {
            rows = apply_limit(rows, limit)?;
        }
        Ok(ResultSet {
            rows,
            ordered: !sel.order_by.is_empty(),
        })
    }

    /// Produces a SELECT's output rows, in `ORDER BY` order when present, but
    /// before `LIMIT`: scan/join, selection, then row-wise projection or
    /// grouping/aggregation. `ORDER BY` is resolved in the input context (a row
    /// for scans, a group for aggregation), so ordering by a non-selected
    /// column, a `*` query, or a position all reduce to the normal case.
    fn select_rows(
        &self,
        sel: &SelectStmt,
        outer: Option<&dyn Columns>,
        ctes: &[(String, Relation)],
    ) -> Result<Vec<Row>, ExecError> {
        let has_aggregate = sel
            .fields
            .iter()
            .any(|f| matches!(f, SelectField::Expr { expr, .. } if expr_has_aggregate(expr)));
        // No FROM (or FROM DUAL) is the single-synthetic-row case; its result is
        // at most one row, so ORDER BY is a no-op. Aggregate projections still
        // must cross the canonical grouping/eval_group path: ordinary
        // `execute_select` deliberately knows only scalar expressions and
        // cannot evaluate an `Expr::Aggregate` leaf.
        let Some(from) = &sel.from else {
            if has_aggregate || !sel.group_by.is_empty() {
                let synthetic = Row::new();
                let mut passing = Vec::with_capacity(1);
                let include = if let Some(predicate) = &sel.where_clause {
                    let resolver =
                        RelResolver::with_outer(&[], &synthetic, outer, self.session_state());
                    let folded = self.resolve_subqueries(predicate, &resolver)?;
                    is_truthy(eval_in(&folded, &resolver)?)?
                } else {
                    true
                };
                if include {
                    passing.push(&synthetic);
                }
                return self.aggregate(sel, &[], &passing, outer);
            }
            return Ok(execute_select(sel, self.session_state())?.rows);
        };
        // Build the (possibly joined) input relation, then filter/group/project.
        let rel = self.build_join(from, ctes)?;
        let session = self.session_state();

        // Selection: keep the rows whose WHERE predicate is truthy. Subqueries in
        // WHERE are resolved per row, so a correlated subquery sees the current
        // row as its outer scope; an uncorrelated one just recomputes the same
        // value each time. This makes correlated and uncorrelated one code path.
        let mut passing: Vec<&Row> = Vec::new();
        for row in &rel.rows {
            if let Some(pred) = &sel.where_clause {
                let cur = RelResolver::with_outer(&rel.cols, row, outer, session.clone());
                let folded = self.resolve_subqueries(pred, &cur)?;
                if !is_truthy(eval_in(&folded, &cur)?)? {
                    continue;
                }
            }
            passing.push(row);
        }

        if sel.group_by.is_empty() && !has_aggregate {
            if sel.having.is_some() {
                return Err(ExecError::Unsupported("HAVING without aggregation"));
            }
            let keys = resolve_order_keys(&sel.order_by, &sel.fields)?;

            // Window functions (`ROW_NUMBER`/`RANK`/`DENSE_RANK` `OVER
            // (...)`, see `crate::window`'s own doc): collect every
            // DISTINCT window-function call across the select list and
            // the (already position-resolved) ORDER BY keys, and compute
            // each one's value for every row NOW, in `passing`'s current
            // (pre-sort) order — a no-op when the query has none, so the
            // ordinary path below is untouched.
            let mut window_exprs: Vec<Expr> = Vec::new();
            for field in &sel.fields {
                if let SelectField::Expr { expr, .. } = field {
                    crate::window::collect_windows_in(expr, &mut window_exprs);
                }
            }
            for (e, _) in &keys {
                crate::window::collect_windows_in(e, &mut window_exprs);
            }
            // `compute_window` operates over `GROUP BY`-style groups; a
            // plain row-wise relation is the degenerate case of one row
            // per group.
            let groups: Vec<Vec<&Row>> = passing.iter().map(|&r| vec![r]).collect();
            let windows = window_exprs
                .into_iter()
                .map(|w| {
                    let Expr::Window { name, args, over } = &w else {
                        unreachable!("collect_windows_in only ever pushes Expr::Window")
                    };
                    let WindowOver::Def(WindowDef { spec, .. }) = over else {
                        unreachable!("resolve_named_windows already normalized every OVER clause")
                    };
                    self.compute_window(name, args, spec, &groups, &rel.cols, outer)
                        .map(|vals| (w.clone(), vals))
                })
                .collect::<Result<Vec<_>, ExecError>>()?;

            // `orig_idx[i]` tracks `passing[i]`'s index into `windows`'
            // value vectors (computed against the PRE-sort order) as rows
            // get reordered by ORDER BY below — a window value must
            // follow its own row through the sort.
            let mut orig_idx: Vec<usize> = (0..passing.len()).collect();

            // ORDER BY the scanned rows (in the row context), then project.
            if !keys.is_empty() {
                let descs: Vec<bool> = keys.iter().map(|(_, d)| *d).collect();
                let mut keyed = Vec::with_capacity(passing.len());
                for (i, &row) in passing.iter().enumerate() {
                    let resolver = RelResolver::new(&rel.cols, row, session.clone());
                    let kv = keys
                        .iter()
                        .map(|(e, _)| -> Result<Datum, ExecError> {
                            let resolved = crate::window::resolve_windows(e, &windows, i);
                            let folded = self.resolve_subqueries(&resolved, &resolver)?;
                            Ok(eval_in(&folded, &resolver)?)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    keyed.push((kv, i, row));
                }
                keyed.sort_by(|a, b| cmp_keys(&a.0, &b.0, &descs));
                orig_idx = keyed.iter().map(|(_, i, _)| *i).collect();
                passing = keyed.into_iter().map(|(_, _, row)| row).collect();
            }

            if windows.is_empty() {
                return passing
                    .iter()
                    .map(|row| self.project_row(&sel.fields, &rel.cols, row, outer))
                    .collect();
            }

            return passing
                .iter()
                .zip(&orig_idx)
                .map(|(row, &i)| {
                    let fields: Vec<SelectField> = sel
                        .fields
                        .iter()
                        .map(|f| match f {
                            SelectField::Expr { expr, alias } => SelectField::Expr {
                                expr: crate::window::resolve_windows(expr, &windows, i),
                                alias: alias.clone(),
                            },
                            SelectField::Wildcard(q) => SelectField::Wildcard(q.clone()),
                        })
                        .collect();
                    self.project_row(&fields, &rel.cols, row, outer)
                })
                .collect();
        }
        self.aggregate(sel, &rel.cols, &passing, outer)
    }

    /// Builds the relation for a `FROM` join node: a bare table, or a join of
    /// two operands.
    fn build_node(
        &self,
        node: &JoinNode,
        ctes: &[(String, Relation)],
    ) -> Result<Relation, ExecError> {
        match node {
            JoinNode::Table(tr) if tr.name.len() == 1 => {
                // An already-materialized `WITH RECURSIVE` relation shadows
                // a real table of the same name, matching the EXISTING
                // non-recursive CTE desugaring's own shadowing behavior
                // (`crate::cte`'s own doc) — checked first, by name,
                // case-insensitively, same convention as everywhere else a
                // CTE name is matched.
                if let Some((_, rel)) = ctes
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case(&tr.name[0]))
                {
                    let qual = tr.alias.clone().unwrap_or_else(|| tr.name[0].clone());
                    let cols = rel
                        .cols
                        .iter()
                        .map(|c| Column {
                            tables: vec![qual.clone()],
                            name: c.name.clone(),
                        })
                        .collect();
                    return Ok(Relation {
                        cols,
                        rows: rel.rows.clone(),
                    });
                }
                self.build_real_table(tr)
            }
            JoinNode::Table(tr) => self.build_real_table(tr),
            JoinNode::Derived {
                subquery,
                alias,
                lateral,
                ..
            } => {
                // `LATERAL` needs the subquery re-evaluated once per row of
                // the tables preceding it, with THAT row's own column
                // values in scope — a correlated, per-outer-row execution
                // model this crate's `Relation`-based join engine doesn't
                // have (`build_join` below builds each side's whole
                // `Relation` independently, once, before joining them).
                // Unconditionally `Unsupported`, the SAME "real semantic
                // effect, no cheap representation" scope cut already
                // applied to `TABLESAMPLE`/`AS OF TIMESTAMP` (see
                // `tidb_ast::JoinNode::Derived::lateral`'s own doc). A
                // non-`LATERAL` derived table has no outer scope at all, so
                // it's unaffected. `subquery` is always `Select` or
                // `SetOpr` (the only two variants `parse_select_or_setopr`
                // can ever produce) — see `tidb_ast::JoinNode::Derived`'s
                // own doc. For a `SetOpr` body, output column NAMING
                // follows the FIRST term only (matching real MySQL/TiDB —
                // a `UNION`'s result columns are named from its first
                // `SELECT`), the SAME convention `crate::recursive_cte`'s
                // own `derived_columns` call sites already use for a
                // `UNION`-bodied CTE.
                if *lateral {
                    return Err(ExecError::Unsupported("LATERAL derived table"));
                }
                // Parsing/restoring a derived table with no alias at all
                // is fully supported (see `tidb_ast::JoinNode::Derived
                // ::alias`'s own doc), but EXECUTING one needs a name to
                // tag its output columns with for qualified-reference
                // resolution (`derived_columns` below) — deliberately
                // `Unsupported` here rather than inventing an untested
                // empty/synthetic qualifier, the SAME "real semantic
                // effect, no cheap representation" scope cut as
                // `LATERAL` just above.
                let Some(alias) = alias else {
                    return Err(ExecError::Unsupported("derived table without an alias"));
                };
                let (rows, repr) = match subquery.as_ref() {
                    QueryStmt::Select(s) => (self.select_scoped(s, None, ctes)?.rows, s.as_ref()),
                    QueryStmt::SetOpr(s) => (
                        self.setopr_scoped(s, None, ctes)?.rows,
                        s.representative_select(),
                    ),
                };
                let cols = self.derived_columns(repr, alias, ctes)?;
                Ok(Relation { cols, rows })
            }
            JoinNode::Join(j) => self.build_join(j, ctes),
        }
    }

    fn build_real_table(&self, tr: &tidb_ast::TableRef) -> Result<Relation, ExecError> {
        check_no_partition(&tr.partitions)?;
        check_no_table_sample(&tr.sample)?;
        check_no_as_of(&tr.as_of)?;
        let key = table_key(&tr.name);
        let table = self
            .tables
            .get(&key)
            .ok_or_else(|| ExecError::UnknownTable(key.clone()))?;
        // Columns are qualified by the alias, or the table name.
        let qual = tr.alias.clone().unwrap_or(key);
        let cols = table
            .cols
            .iter()
            .map(|c| Column {
                tables: vec![qual.clone()],
                name: c.clone(),
            })
            .collect();
        Ok(Relation {
            cols,
            rows: table.rows.clone(),
        })
    }

    /// Derives a derived table's output columns, qualified by its alias: a
    /// field's `AS` alias if given, else a plain column reference's own
    /// name, else an unnamed placeholder (addressable positionally via `*`,
    /// but not by name — a deliberate boundary rather than reproducing
    /// MySQL's expression-to-name stringification). A `*` in the subquery's
    /// own select list expands to its `FROM` relation's own columns — built
    /// the same way the subquery's own execution builds them (redundant
    /// with `select`'s internal build, but this is a seed executor where
    /// correctness, not performance, is what matters), requalified under
    /// this derived table's alias, matching how a real table scan names its
    /// columns.
    pub(crate) fn derived_columns(
        &self,
        subquery: &SelectStmt,
        alias: &str,
        ctes: &[(String, Relation)],
    ) -> Result<Vec<Column>, ExecError> {
        let has_wildcard = subquery
            .fields
            .iter()
            .any(|f| matches!(f, SelectField::Wildcard(_)));
        let source_cols = if has_wildcard {
            let from = subquery.from.as_ref().ok_or(ExecError::Wildcard)?;
            Some(self.build_join(from, ctes)?.cols)
        } else {
            None
        };

        let mut out = Vec::new();
        for f in &subquery.fields {
            match f {
                SelectField::Wildcard(qualifier) => {
                    let cols = source_cols.as_ref().unwrap();
                    for i in wildcard_indices(cols, qualifier)? {
                        out.push(Column {
                            tables: vec![alias.to_string()],
                            name: cols[i].name.clone(),
                        });
                    }
                }
                SelectField::Expr {
                    expr,
                    alias: field_alias,
                } => {
                    let name = field_alias.clone().unwrap_or_else(|| match expr {
                        Expr::Column(path) => path.last().cloned().unwrap_or_default(),
                        _ => String::new(),
                    });
                    out.push(Column {
                        tables: vec![alias.to_string()],
                        name,
                    });
                }
            }
        }
        Ok(out)
    }

    /// Builds the relation for a join, doing a nested-loop join with `ON`
    /// filtering. Supports inner/cross, `LEFT`, and `RIGHT` outer joins.
    pub(crate) fn build_join(
        &self,
        j: &Join,
        ctes: &[(String, Relation)],
    ) -> Result<Relation, ExecError> {
        let left = self.build_node(&j.left, ctes)?;
        let Some(right_node) = &j.right else {
            return Ok(left); // single-table wrapper
        };
        let right = self.build_node(right_node, ctes)?;
        // `NATURAL JOIN` is exactly `JOIN ... USING (<every column name
        // common to both sides>)` (confirmed via `gorun`: coalesced
        // columns, LEFT/RIGHT outer-join NULL-padding, and the
        // LEFT/RIGHT column-order swap all behave identically to an
        // explicit `USING` join) — delegates to the SAME
        // `build_using_join` rather than a separate implementation. Zero
        // common columns degenerates to a plain cross join with no
        // special-casing needed: `build_using_join`'s own row-matching
        // loop over an EMPTY column list is vacuously `true` for every
        // pair, confirmed via `gorun` (`t3 NATURAL JOIN t4` with no
        // shared column names returns the full cartesian product).
        if j.natural {
            let using = if matches!(j.tp, JoinType::Right) {
                natural_join_columns(&right, &left)
            } else {
                natural_join_columns(&left, &right)
            };
            return build_using_join(j, &using, left, right);
        }
        if !j.using.is_empty() {
            return build_using_join(j, &j.using, left, right);
        }
        let mut cols = left.cols.clone();
        cols.extend(right.cols.iter().cloned());
        let schema = join_schema(&left, &right);
        let condition =
            j.on.as_ref()
                .map(|expression| {
                    compile_join_condition(
                        expression,
                        &schema,
                        if matches!(j.tp, JoinType::Left) {
                            ConditionEvaluationMode::OuterMatchStatus
                        } else {
                            ConditionEvaluationMode::JoinFilter
                        },
                    )
                })
                .transpose()
                .map_err(join_condition_error)?;

        let evaluate_candidate =
            |lrow: &Row, rrow: &Row| -> Result<(Row, PredicateTruth), ExecError> {
                let mut combined = lrow.clone();
                combined.extend_from_slice(rrow);
                let truth = match &condition {
                    Some(condition) => evaluate_join_condition(condition, &combined)
                        .map_err(join_condition_error)?,
                    None => PredicateTruth::True,
                };
                Ok((combined, truth))
            };

        let mut rows = Vec::new();
        match j.tp {
            JoinType::Left => {
                let nulls = vec![Datum::Null; right.cols.len()];
                for lrow in &left.rows {
                    let mut status = OuterRowStatus::Unmatched;
                    for rrow in &right.rows {
                        let (row, truth) = evaluate_candidate(lrow, rrow)?;
                        status = merge_outer_row_truth(status, truth);
                        if truth == PredicateTruth::True {
                            rows.push(row);
                        }
                    }
                    if !finalize_outer_row_statuses(&[status]).is_empty() {
                        let mut row = lrow.clone();
                        row.extend_from_slice(&nulls);
                        rows.push(row);
                    }
                }
            }
            JoinType::Right => {
                let nulls = vec![Datum::Null; left.cols.len()];
                for rrow in &right.rows {
                    let mut status = OuterRowStatus::Unmatched;
                    for lrow in &left.rows {
                        let (row, truth) = evaluate_candidate(lrow, rrow)?;
                        status = merge_outer_row_truth(status, truth);
                        if truth == PredicateTruth::True {
                            rows.push(row);
                        }
                    }
                    // UNKNOWN is a nonmatch for RIGHT just as for LEFT; only
                    // TRUE suppresses the nullable-inner row.
                    if !finalize_outer_row_statuses(&[status]).is_empty() {
                        let mut row = nulls.clone();
                        row.extend_from_slice(rrow);
                        rows.push(row);
                    }
                }
            }
            JoinType::Cross => {
                for lrow in &left.rows {
                    for rrow in &right.rows {
                        let (row, truth) = evaluate_candidate(lrow, rrow)?;
                        if truth == PredicateTruth::True {
                            rows.push(row);
                        }
                    }
                }
            }
        }
        Ok(Relation { cols, rows })
    }
}

/// Computes a `NATURAL JOIN`'s own implicit `USING` column list: every
/// name in `outer.cols` that ALSO appears (case-insensitively) in
/// `inner.cols`, in the planner outer child's declaration order — confirmed
/// via `gorun` (a 3-common-column mixed-order probe). For RIGHT the caller
/// passes the original right relation as `outer`, matching Go's mirrored
/// `coalesceCommonColumns` working sets.
fn natural_join_columns(outer: &Relation, inner: &Relation) -> Vec<String> {
    outer
        .cols
        .iter()
        .filter(|column| {
            !column
                .tables
                .iter()
                .any(|table| table == HIDDEN_USING_COLUMN)
        })
        .filter(|lc| {
            inner
                .cols
                .iter()
                .filter(|column| {
                    !column
                        .tables
                        .iter()
                        .any(|table| table == HIDDEN_USING_COLUMN)
                })
                .any(|rc| rc.name.eq_ignore_ascii_case(&lc.name))
        })
        .map(|c| c.name.clone())
        .collect()
}

fn join_schema(left: &Relation, right: &Relation) -> JoinSchema {
    let left_columns = left
        .cols
        .iter()
        .map(|column| {
            let spec = ColumnSpec::with_qualifiers(
                column.name.clone(),
                column
                    .tables
                    .iter()
                    .filter(|table| !table.starts_with('\0'))
                    .cloned(),
                true,
            );
            if column
                .tables
                .iter()
                .any(|table| table == HIDDEN_USING_COLUMN)
            {
                spec.qualified_only()
            } else {
                spec
            }
        })
        .collect::<Vec<_>>();
    let right_columns = right
        .cols
        .iter()
        .map(|column| {
            let spec = ColumnSpec::with_qualifiers(
                column.name.clone(),
                column
                    .tables
                    .iter()
                    .filter(|table| !table.starts_with('\0'))
                    .cloned(),
                true,
            );
            if column
                .tables
                .iter()
                .any(|table| table == HIDDEN_USING_COLUMN)
            {
                spec.qualified_only()
            } else {
                spec
            }
        })
        .collect::<Vec<_>>();
    JoinSchema::new(left_columns, right_columns)
}

fn join_condition_error(error: TypedConditionEvalError) -> ExecError {
    match error {
        TypedConditionEvalError::Evaluation(error) => ExecError::Eval(error),
        TypedConditionEvalError::Batch { source, .. } => join_condition_error(*source),
        TypedConditionEvalError::Binding(ConditionBindingError::UnknownColumn { path })
        | TypedConditionEvalError::Binding(ConditionBindingError::AmbiguousColumn { path }) => {
            ExecError::UnknownColumn(path.join("."))
        }
        TypedConditionEvalError::Binding(ConditionBindingError::InvalidColumnPath) => {
            ExecError::UnknownColumn(String::new())
        }
        TypedConditionEvalError::Binding(ConditionBindingError::UnboundParameterMarker {
            ..
        }) => ExecError::Unsupported("unbound parameter marker in join residual"),
        TypedConditionEvalError::UnsupportedShape(_) => {
            ExecError::Unsupported("join residual expression shape")
        }
        TypedConditionEvalError::RowWidth { .. } => {
            ExecError::Unsupported("join FullSchema row width")
        }
    }
}

/// Builds a `USING (cols...)` join: equivalent to an `ON` join equating each
/// named column pair, but each named column is also **coalesced** into one
/// physical column reachable under both sides' qualifiers (or unqualified) —
/// matching MySQL's `USING` semantics, where `SELECT *` shows the column
/// once, not twice. An outer join's unmatched row is combined with a
/// synthetic all-`NULL` row on the other side, through the same combining
/// step as a real match, so the unmatched case needs no separate NULL-padding
/// logic: the coalesce naturally keeps the present side's value. Also used
/// for `NATURAL JOIN`, via [`natural_join_columns`]'s own computed list
/// (an EMPTY list there — no common columns — degenerates to a plain
/// cross join, needing no special-casing: the row-matching loop below is
/// vacuously `true` for every pair when there are no columns to check).
fn build_using_join(
    j: &Join,
    using: &[String],
    left: Relation,
    right: Relation,
) -> Result<Relation, ExecError> {
    let schema = join_schema(&left, &right);
    let conditions = compile_using_conditions(using, &schema).map_err(join_condition_error)?;
    let pairs: Vec<(usize, usize)> = conditions
        .iter()
        .map(|condition| {
            condition
                .equality_indices()
                .expect("USING compiles only equality conditions")
        })
        .collect();
    // The USING list controls predicate construction, but it does not control
    // the visible output order. TiDB's planner moves common columns to the
    // front in the order they appear in the outer child schema
    // (`buildUsingClause`/`coalesceCommonColumns`), then appends the remaining
    // left and right columns. For RIGHT JOIN the planner mirrors the children
    // first, so the outer child is the original right relation. Keep the
    // source pair lookup in USING order for deterministic validation, and use
    // that outer-child ordered view whenever building columns or rows. This
    // distinction matters for `USING (id, z)` when the left table declares
    // `z` before `id`.
    let mut output_pairs = pairs.clone();
    if matches!(j.tp, JoinType::Right) {
        // The planner canonicalizes RIGHT JOIN as the mirrored LEFT shape
        // before coalescing, so common fields follow the original right
        // child's declaration order.
        output_pairs.sort_unstable_by_key(|(_, right_index)| *right_index);
    } else {
        output_pairs.sort_unstable_by_key(|(left_index, _)| *left_index);
    }
    let left_using: Vec<usize> = pairs.iter().map(|&(li, _)| li).collect();
    let right_using: Vec<usize> = pairs.iter().map(|&(_, ri)| ri).collect();

    // A `RIGHT JOIN` swaps the non-USING column/value order to
    // [right-remaining, left-remaining] — verified against real TiDB, which
    // effectively rewrites `A RIGHT JOIN B USING(x)` as `B LEFT JOIN A
    // USING(x)`. Every other join type keeps [left-remaining,
    // right-remaining]. (A plain `ON` join has no such swap — its column
    // order is always [left, right] — so this asymmetry is specific to the
    // USING coalescing path.)
    let swap = matches!(j.tp, JoinType::Right);

    // Combined columns: the coalesced USING columns first (reachable via
    // either side's qualifiers), then each side's remaining columns.
    let mut cols: Vec<Column> = output_pairs
        .iter()
        .map(|&(li, ri)| {
            let (mut column, source_index) = if swap {
                (right.cols[ri].clone(), ri)
            } else {
                (left.cols[li].clone(), li)
            };
            mark_qualified_order(&mut column, source_index);
            column.name = left.cols[li].name.clone();
            column
        })
        .collect();
    let left_rest: Vec<Column> = left
        .cols
        .iter()
        .enumerate()
        .filter(|(i, _)| !left_using.contains(i))
        .map(|(index, c)| {
            let mut column = c.clone();
            mark_qualified_order(&mut column, index);
            column
        })
        .collect();
    let right_rest: Vec<Column> = right
        .cols
        .iter()
        .enumerate()
        .filter(|(i, _)| !right_using.contains(i))
        .map(|(index, c)| {
            let mut column = c.clone();
            mark_qualified_order(&mut column, index);
            column
        })
        .collect();
    if swap {
        cols.extend(right_rest);
        cols.extend(left_rest);
    } else {
        cols.extend(left_rest);
        cols.extend(right_rest);
    }
    let hidden_using_columns: Vec<Column> = output_pairs
        .iter()
        .map(|&(li, ri)| {
            let (mut column, source_index) = if swap {
                (left.cols[li].clone(), li)
            } else {
                (right.cols[ri].clone(), ri)
            };
            mark_qualified_order(&mut column, source_index);
            column.tables.push(HIDDEN_USING_COLUMN.to_owned());
            column
        })
        .collect();
    cols.extend(hidden_using_columns);

    // Combines one side's raw row (at its original width) with the other's
    // into one output row: a USING column coalesces to whichever side is
    // non-`NULL`, followed by each side's remaining (non-USING) values, in
    // the same order as `cols`.
    let combine_row = |lrow: &Row, rrow: &Row| -> Row {
        let mut row: Row = output_pairs
            .iter()
            .map(|&(li, ri)| {
                let lv = &lrow[li];
                if *lv != Datum::Null {
                    lv.clone()
                } else {
                    rrow[ri].clone()
                }
            })
            .collect();
        let left_vals: Vec<Datum> = lrow
            .iter()
            .enumerate()
            .filter(|(i, _)| !left_using.contains(i))
            .map(|(_, v)| v.clone())
            .collect();
        let right_vals: Vec<Datum> = rrow
            .iter()
            .enumerate()
            .filter(|(i, _)| !right_using.contains(i))
            .map(|(_, v)| v.clone())
            .collect();
        if swap {
            row.extend(right_vals);
            row.extend(left_vals);
        } else {
            row.extend(left_vals);
            row.extend(right_vals);
        }
        row.extend(output_pairs.iter().map(|&(li, ri)| {
            if swap {
                lrow[li].clone()
            } else {
                rrow[ri].clone()
            }
        }));
        row
    };

    // Two rows match when every USING column is equal (never true if either
    // side is `NULL`, matching an equi-join's `ON` semantics).
    let candidate_truth = |lrow: &Row, rrow: &Row| -> Result<PredicateTruth, ExecError> {
        let mut full_row = lrow.clone();
        full_row.extend_from_slice(rrow);
        evaluate_join_conditions(&conditions, &full_row).map_err(join_condition_error)
    };

    let left_nulls = vec![Datum::Null; left.cols.len()];
    let right_nulls = vec![Datum::Null; right.cols.len()];
    let mut rows = Vec::new();
    match j.tp {
        JoinType::Left => {
            for lrow in &left.rows {
                let mut status = OuterRowStatus::Unmatched;
                for rrow in &right.rows {
                    let truth = candidate_truth(lrow, rrow)?;
                    status = merge_outer_row_truth(status, truth);
                    if truth == PredicateTruth::True {
                        rows.push(combine_row(lrow, rrow));
                    }
                }
                if !finalize_outer_row_statuses(&[status]).is_empty() {
                    rows.push(combine_row(lrow, &right_nulls));
                }
            }
        }
        JoinType::Right => {
            for rrow in &right.rows {
                let mut status = OuterRowStatus::Unmatched;
                for lrow in &left.rows {
                    let truth = candidate_truth(lrow, rrow)?;
                    status = merge_outer_row_truth(status, truth);
                    if truth == PredicateTruth::True {
                        rows.push(combine_row(lrow, rrow));
                    }
                }
                if !finalize_outer_row_statuses(&[status]).is_empty() {
                    rows.push(combine_row(&left_nulls, rrow));
                }
            }
        }
        JoinType::Cross => {
            for lrow in &left.rows {
                for rrow in &right.rows {
                    if candidate_truth(lrow, rrow)? == PredicateTruth::True {
                        rows.push(combine_row(lrow, rrow));
                    }
                }
            }
        }
    }
    Ok(Relation { cols, rows })
}

/// Resolves a (possibly qualified) `*` to the 0-based indices of the columns
/// it expands to: all of `cols` for a bare `*`, or only those under the
/// given qualifier (matched by the last path segment, case-insensitively —
/// the same rule [`RelResolver`] uses for a qualified column reference). An
/// unmatched qualifier is an unknown-table error rather than a silent empty
/// expansion.
pub(crate) fn wildcard_indices(
    cols: &[Column],
    qualifier: &[String],
) -> Result<Vec<usize>, ExecError> {
    let Some(qual) = qualifier.last() else {
        return Ok(cols
            .iter()
            .enumerate()
            .filter(|(_, column)| {
                !column
                    .tables
                    .iter()
                    .any(|table| table == HIDDEN_USING_COLUMN)
            })
            .map(|(index, _)| index)
            .collect());
    };
    let mut indices: Vec<usize> = cols
        .iter()
        .enumerate()
        .filter(|(_, c)| c.tables.iter().any(|t| t.eq_ignore_ascii_case(qual)))
        .map(|(i, _)| i)
        .collect();
    indices.sort_by_key(|index| qualified_order(&cols[*index], qual).unwrap_or(*index));
    if indices.is_empty() {
        return Err(ExecError::UnknownTable(qual.clone()));
    }
    Ok(indices)
}

/// Internal relation marker for a redundant USING-side `FullSchema` column.
/// It is never a SQL qualifier: bare wildcards omit it, while the real table
/// qualifier remains available for `right.col` and `right.*`.
const HIDDEN_USING_COLUMN: &str = "\0tidb:hidden-using";
const USING_ORDER_PREFIX: &str = "\0tidb:using-order:";

fn mark_qualified_order(column: &mut Column, index: usize) {
    let qualifiers = column
        .tables
        .iter()
        .filter(|table| !table.starts_with('\0'))
        .cloned()
        .collect::<Vec<_>>();
    for qualifier in qualifiers {
        let prefix = format!("{USING_ORDER_PREFIX}{}:", qualifier.to_ascii_lowercase());
        if !column.tables.iter().any(|table| table.starts_with(&prefix)) {
            column.tables.push(format!("{prefix}{index}"));
        }
    }
}

fn qualified_order(column: &Column, qualifier: &str) -> Option<usize> {
    let prefix = format!("{USING_ORDER_PREFIX}{}:", qualifier.to_ascii_lowercase());
    column
        .tables
        .iter()
        .find_map(|table| table.strip_prefix(&prefix)?.parse().ok())
}

/// MySQL truthiness of a predicate value: a non-zero integer passes; `NULL` and
/// `0` fail; a bare string predicate would need numeric coercion (out of scope).
pub(crate) fn is_truthy(v: Datum) -> Result<bool, ExecError> {
    match v {
        Datum::String(_) => Err(ExecError::Eval(EvalError::Unsupported("string predicate"))),
        // Null is falsy (not an error) here; Int/Decimal share MySQL's
        // nonzero-is-truthy rule via `truthy_of`.
        other => Ok(truthy_of(&other)?.unwrap_or(false)),
    }
}
