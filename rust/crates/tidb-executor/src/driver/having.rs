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

//! `HAVING` for a non-aggregate `SELECT` -- Go's `LogicalSelection` above the
//! select list's `Projection`.
//!
//! The aggregate pipeline has its own `HAVING` stage
//! ([`super::agg_select::run_aggregate_select`]) because there the clause
//! reads the AGGREGATION's output. Everything else -- no `GROUP BY`, no
//! aggregate anywhere -- lands here, where the clause reads the PROJECTION's
//! output. Before this module existed the driver simply DROPPED the clause on
//! that path, so `select a from ht having b > 0` answered rows TiDB rejects
//! with 1054 and `select a, b from ht having (select y from hs where hs.x =
//! ht.b) > 0` answered both rows where TiDB answers one.
//!
//! Go, `buildSelect`:
//!
//! ```go
//! p, projExprs, oldLen, err = b.buildProjection(ctx, p, sel.Fields.Fields, ...)
//! if sel.Having != nil {
//!     b.curClause = havingClause
//!     p, err = b.buildSelection(ctx, p, sel.Having.Expr, havingMap)
//! }
//! ```
//!
//! This tier evaluates the projection LAST, over source rows, so the filter
//! is a `SelectionExec` over source rows with every `HAVING` name replaced by
//! the select field it names. That is a rewrite, not a different plan: the
//! projection is one-to-one with its input here, so filtering before it and
//! filtering after it keep the same rows, and both sit below the sort and the
//! limit exactly as Go's `Selection -> Sort -> Limit` does.

use super::*;

/// Builds the `HAVING` filter over `source`, or reports why the clause cannot
/// name what it names.
///
/// `projected` is the select list with its own correlated subqueries already
/// hoisted (the driver's `projected`), and `scope` the `FROM` scope its
/// wildcards expand against; `current_scope` is the row `source` produces,
/// which this widens by one column per correlated subquery the clause holds.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_plain_having(
    having: &tidb_ast::Expr,
    projected: &[(SelectField, Option<String>)],
    scope: &FromScope,
    current_scope: &mut FromScope,
    mut source: Box<dyn Executor>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Box<dyn Executor>, DriverError> {
    let fields = having_select_fields(projected, scope);
    let outputs = having_outputs(projected, scope);
    // Go's `resolveHavingAndOrderBy` runs over the clause as written, before
    // anything is built, and reports every name the select list lacks.
    let having = resolve_having_names(having, &fields, &ScopeResolver { scope })?;
    // ... and then the names its SUBQUERIES correlate to, which Go resolves
    // later against the projection.
    bind_having_correlations(&having, &outputs, current_scope, catalog, current_db, ctx)?;

    // A correlated subquery becomes an Apply below the filter, appending the
    // column the rewritten predicate reads -- the shape the `WHERE` path
    // builds, and Go's plan for a subquery it cannot fold.
    let mut correlated = None;
    let appended = current_scope.width();
    let predicate = extract_correlated_subquery(
        &having,
        current_scope,
        catalog,
        current_db,
        appended,
        &mut correlated,
        ctx,
    )?;
    if let Some(correlated) = correlated {
        let mut value_type = FieldType::new(FieldTypeCode::LongLong);
        if matches!(correlated.kind, SubqueryKind::Scalar) {
            value_type = subquery_result_type(&correlated, catalog, current_db, ctx)
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
        }
        let inner_scope = current_scope.clone();
        current_scope.tables.push(FromTable {
            name: String::new(),
            database: None,
            columns: vec![(format!("__apply_{appended}"), value_type)],
            offset: appended,
            func_deps: Default::default(),
        });
        let columns: Vec<Column> = current_scope
            .column_list()
            .iter()
            .enumerate()
            .map(|(i, (_, ft))| {
                let mut col = Column::new((i + 1) as i64, ft.clone());
                col.index = i as i64;
                col
            })
            .collect();
        let apply_schema = Schema::new(columns);
        // The callback outlives this borrow of the catalog, so it owns a
        // snapshot (see ApplyExec::new); the context is a handle, so the
        // inner query's warnings reach the statement's one buffer.
        let inner_catalog = catalog.clone();
        let inner_db = current_db.to_owned();
        let inner_ctx = ctx.clone();
        let runner: crate::apply::InnerRunner = Box::new(move |values: &[Datum]| {
            run_correlated_subquery(
                &correlated,
                values,
                &inner_scope,
                &inner_catalog,
                &inner_db,
                &inner_ctx,
            )
            .map_err(|e| match e {
                DriverError::Exec(exec) => exec,
                DriverError::SubqueryReturnsMoreThanOneRow => {
                    ExecError::SubqueryReturnsMoreThanOneRow
                }
                other => ExecError::unsupported(driver_error_text(&other)),
            })
        });
        source = Box::new(crate::apply::ApplyExec::new(
            ExecutorMeta::new(apply_schema, 7, INIT_CAP, MAX_CHUNK_SIZE),
            source,
            runner,
            ctx.statement_memory(),
            // The outer side is the statement's SOURCE, never an aggregation,
            // so Go's deselected-default-row case cannot arise.
            None,
        ));
    }
    let resolver = ScopeResolver {
        scope: current_scope,
    };
    let mut predicate = rewrite_expr_resolved(&predicate, &resolver)
        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
    refine_comparisons(&mut predicate, ctx);
    let schema = source.schema().clone();
    Ok(Box::new(SelectionExec::new(
        ExecutorMeta::new(schema, 1, INIT_CAP, MAX_CHUNK_SIZE),
        vec![predicate],
        source,
        ctx.clone(),
    )))
}

/// The select list as `resolveFromSelectFields` walks it: `(alias, expr)` per
/// field, with `*` and `t.*` already unfolded.
///
/// Go unfolds the star in `buildSelect` BEFORE `resolveHavingAndOrderBy`, so
/// by the time the clause is resolved every field is an ordinary expression.
/// That is why `select * from ht having b > 15` resolves `b` while `select a
/// from ht having b > 15` is 1054.
fn having_select_fields(
    projected: &[(SelectField, Option<String>)],
    scope: &FromScope,
) -> Vec<(Option<String>, tidb_ast::Expr)> {
    let mut fields = Vec::with_capacity(projected.len());
    for (field, _) in projected {
        match field {
            SelectField::Expr { expr, alias } => fields.push((alias.clone(), expr.clone())),
            SelectField::Wildcard(qualifier) => {
                for (table, column) in star_columns(qualifier, scope) {
                    fields.push((None, tidb_ast::Expr::Column(vec![table, column])));
                }
            }
        }
    }
    fields
}

/// The projection's output names, as a `HAVING` subquery's correlations see
/// them (see [`HavingOutput`]).
fn having_outputs(
    projected: &[(SelectField, Option<String>)],
    scope: &FromScope,
) -> Vec<HavingOutput> {
    let mut outputs = Vec::with_capacity(projected.len());
    for (field, name) in projected {
        match field {
            SelectField::Expr { expr, alias } => {
                // Only a plain column field keeps a table qualifier -- Go's
                // `OrigTblName`, which an expression field has none of.
                let table = match expr {
                    tidb_ast::Expr::Column(path) => match path.as_slice() {
                        [.., qualifier, _] => Some(qualifier.clone()),
                        // An unqualified column still belongs to the table it
                        // resolved to, which with one FROM table is that one.
                        [_] => (scope.tables.len() == 1)
                            .then(|| scope.tables[0].name.clone())
                            .filter(|name| !name.is_empty()),
                        [] => None,
                    },
                    _ => None,
                };
                let name = match (alias, name) {
                    (Some(alias), _) => alias.clone(),
                    (None, Some(name)) => name.clone(),
                    (None, None) => continue,
                };
                outputs.push(HavingOutput { name, table });
            }
            SelectField::Wildcard(qualifier) => {
                for (table, column) in star_columns(qualifier, scope) {
                    outputs.push(HavingOutput {
                        name: column,
                        table: (!table.is_empty()).then_some(table),
                    });
                }
            }
        }
    }
    outputs
}

/// `*` / `t.*` as `(table, column)` pairs, in `FROM` order.
fn star_columns(qualifier: &[String], scope: &FromScope) -> Vec<(String, String)> {
    scope
        .tables
        .iter()
        .filter(|table| match qualifier.last() {
            None => true,
            Some(q) => table.name.eq_ignore_ascii_case(q),
        })
        .flat_map(|table| {
            table
                .columns
                .iter()
                .map(|(column, _)| (table.name.clone(), column.clone()))
        })
        .collect()
}

/// Reports every name a `HAVING` subquery correlates to that the projection
/// does not output.
///
/// Go leaves these for the subquery build, which resolves them against the
/// plan `HAVING` sits on: the PROJECTION. A name that plan lacks is
/// `ErrUnknownColumn` naming the `having clause`, which is the rule
/// [`find_having_output`] carries with its captures -- and it is a rule the
/// SOURCE scope cannot express, because the source row still HAS the column
/// (`select a from ht having (select y from hs where hs.x = ht.b) > 0` is
/// 1054 while `select a, b from ht having (...)` answers `1|10`).
///
/// Which names are correlations at all is the subquery's own question, so it
/// is asked with the machinery that knows both scopes.
///
/// DEFERRED: a correlation to a name only an ALIAS gives (`SELECT b AS bb ...
/// HAVING (... = bb)`, which TiDB answers) is refused rather than answered.
/// It never reaches here: `fold_subqueries` runs over the whole statement
/// first, sees a subquery whose names the SOURCE scope cannot resolve, and
/// takes it for an uncorrelated one to evaluate -- so the alias is reported
/// as an unresolved column there. Fixing it means teaching that fold about
/// the projection's names, which is a wider seam than this clause.
fn bind_having_correlations(
    having: &tidb_ast::Expr,
    outputs: &[HavingOutput],
    source_scope: &FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let mut correlated = Vec::new();
    for query in having_subqueries(having) {
        crate::driver::subquery::collect_correlated_columns_query(
            &query,
            source_scope,
            catalog,
            current_db,
            &mut correlated,
            ctx,
        );
    }
    for path in &correlated {
        // `__apply_N` is a placeholder a select-list subquery left behind,
        // not a source column anyone wrote.
        if path.last().is_some_and(|name| name.starts_with("__apply_")) {
            continue;
        }
        if find_having_output(path, outputs).is_none() {
            return Err(unknown_having_column(&path.join(".")));
        }
    }
    Ok(())
}
