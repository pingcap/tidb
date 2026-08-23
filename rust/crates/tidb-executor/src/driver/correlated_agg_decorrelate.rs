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

//! Decorrelation of equality-correlated scalar aggregations.
//!
//! Go's `DecorrelateSolver` has two aggregation arms.  When an outer source
//! has a non-null unique key, the first scalar aggregation can be pulled above
//! a left join (`CanPullUpAgg`/`CanPullUp`): the unique key becomes
//! `GROUP BY`, outer values become `FIRST_ROW`, and the scalar aggregate
//! keeps its empty-input NULL through the left join — whatever columns the
//! subquery correlates on.  When that arm cannot fire — the outer has no
//! non-null key, the Apply carries a pulled-up condition (a HAVING), the
//! outer side is already aggregated, or the aggregate's argument survives
//! NULL-extension (`COUNT(1)`) — the aggregation stays below the join: its
//! correlation keys are appended to its own grouping and the Apply becomes a
//! left join to that grouped relation.  A default-valued aggregate (`COUNT`)
//! under a HAVING additionally moves the pulled-up condition OFF the join
//! into a projection over `IFNULL(count, 0)` (`rule_decorrelate.go`'s
//! `havingConds` branch), because an empty input owes the default while a
//! HAVING-filtered group owes NULL.
//!
//! This module transcribes those arms over the AST because this executor
//! does not retain a separate logical-plan tree.  Its acceptance boundary is
//! deliberately proof-shaped:
//!
//! * SELECT-list arm ([`rewrite_current`]): one base-table outer source with
//!   a non-null primary/unique key, bare-column outer values, and each
//!   rewritten field exactly one non-distinct `SUM(column)` scalar subquery;
//! * SELECT-list HAVING arm ([`rewrite_count_having_fields`]): a
//!   single-argument non-distinct `COUNT` subquery with a HAVING that reads
//!   only the aggregate output and outer columns;
//! * predicate arm ([`rewrite_predicate_aggregate`]): one scalar
//!   `AVG`/`MIN`/`MAX`/`SUM(column)` subquery inside a WHERE conjunct;
//! * every correlation is a column equality, and removing those equalities
//!   leaves no outer reference in the subquery;
//! * the inner `FROM` contains inner joins only.
//!
//! Any clause outside that boundary leaves the statement byte-for-byte
//! unchanged, and unwritten Go arms remain unwritten rather than guessed at:
//! non-equality correlations, the no-HAVING default projection
//! (`IFNULL`-only), the `BIT_AND`/`BIT_OR`/`BIT_XOR` defaults, DISTINCT,
//! LIMIT, windows, locking, and `pruneRedundantApply` all keep their Apply
//! plans.  Go also runs `rule_aggregation_elimination` after decorrelating,
//! collapsing a unique-keyed group to a projection; this tier keeps the
//! grouped aggregate, which changes plan TEXT but no scan choice or row.

use std::collections::{BTreeMap, BTreeSet};

use tidb_ast::{
    BinaryOp, Expr, GroupByItem, Join, JoinNode, JoinType, QueryStmt, SelectField, SelectStmt,
    TableRef,
};
use tidb_datatype::FieldTypeFlags;
use tidb_expr::rewriter::ColumnResolver;

use super::catalog::{Catalog, TableEntry};
use super::from::ScopeResolver;

/// Rewrites every eligible derived SELECT first, then the current SELECT.
/// Returning `None` means no node in the tree changed.
pub(crate) fn rewrite(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<SelectStmt> {
    let mut rewritten = select.clone();
    let mut changed = rewritten
        .from
        .as_mut()
        .is_some_and(|from| rewrite_join(from, catalog, current_db, ctx));
    if let Some(current) = rewrite_current(&rewritten, catalog, current_db, ctx) {
        rewritten = current;
        changed = true;
    }
    if let Some(current) = rewrite_predicate_aggregate(&rewritten, catalog, current_db, ctx) {
        rewritten = current;
        changed = true;
    }
    if let Some(current) = rewrite_count_having_fields(&rewritten, catalog, current_db, ctx) {
        rewritten = current;
        changed = true;
    }
    changed.then_some(rewritten)
}

/// Whether later optimizer rules are looking at the grouped left-join form
/// produced by this module. Derived-table fusion may move that form into a
/// caller after [`rewrite`] returned, so the driver recognizes the invariant
/// shape rather than relying on a transient boolean from the first pass.
pub(crate) fn is_pulled_scalar_sum(select: &SelectStmt) -> bool {
    !select.group_by.is_empty()
        && select
            .from
            .as_ref()
            .is_some_and(|join| join.tp == JoinType::Left)
        && select.fields.fields().iter().any(|field| {
            matches!(
                field,
                SelectField::Expr {
                    expr: Expr::Aggregate { name, .. },
                    ..
                } if name.eq_ignore_ascii_case("SUM")
            )
        })
        && select.fields.fields().iter().all(|field| match field {
            SelectField::Expr { expr, .. } => !super::subquery::expr_has_subquery(expr),
            SelectField::Wildcard(_) => false,
        })
}

/// Whether this SELECT is the Selection/Projection wrapper emitted around a
/// pulled scalar SUM. Recursive derived-table planning sees the rewritten AST
/// after the transient `rewrite` result flag is gone, so the wrapper must be
/// recognized from the same proof-shaped structure as its grouped child.
pub(crate) fn is_pulled_scalar_sum_wrapper(select: &SelectStmt) -> bool {
    let Some(from) = select.from.as_ref() else {
        return false;
    };
    if from.right.is_some() || from.tp != JoinType::Cross || from.on.is_some() {
        return false;
    }
    let JoinNode::Derived {
        subquery,
        alias: Some(alias),
        lateral: false,
        column_names,
    } = &from.left
    else {
        return false;
    };
    if alias != "__decorrelated_pullup_0" || !column_names.is_empty() {
        return false;
    }
    let QueryStmt::Select(grouped) = &**subquery else {
        return false;
    };
    is_pulled_scalar_sum(grouped)
}

fn rewrite_join(
    join: &mut Join,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> bool {
    let mut changed = rewrite_node(&mut join.left, catalog, current_db, ctx);
    if let Some(right) = &mut join.right {
        changed |= rewrite_node(right, catalog, current_db, ctx);
    }
    changed
}

fn rewrite_node(
    node: &mut JoinNode,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> bool {
    match node {
        JoinNode::Derived { subquery, .. } => {
            let QueryStmt::Select(select) = &mut **subquery else {
                return false;
            };
            let Some(rewritten) = rewrite(select, catalog, current_db, ctx) else {
                return false;
            };
            **select = rewritten;
            true
        }
        JoinNode::Join(join) => rewrite_join(join, catalog, current_db, ctx),
        JoinNode::Table(_) => false,
    }
}

#[derive(Clone)]
struct ScalarSum {
    field_index: usize,
    output_name: String,
    output_alias: Option<String>,
    sum: Expr,
    inner: SelectStmt,
    local_conditions: Vec<Expr>,
    correlations: Vec<Correlation>,
}

#[derive(Clone)]
struct Correlation {
    inner: Vec<String>,
    outer_offset: usize,
}

#[derive(Clone)]
struct PredicateAggregate {
    inner: SelectStmt,
    aggregate: Expr,
    scalar: Expr,
    local_conditions: Vec<Expr>,
    correlations: Vec<Correlation>,
}

/// Pulls one scalar aggregation used by a predicate into a grouped derived
/// relation. This is the `LogicalApply -> LogicalAggregation -> Selection`
/// arm of Go's `DecorrelateSolver`: correlation equalities become join keys,
/// those inner keys are appended to GROUP BY, and the scalar expression is
/// rewritten over the aggregate output column.
fn rewrite_predicate_aggregate(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<SelectStmt> {
    let where_clause = select.where_clause.as_ref()?;
    // Scope construction may recursively plan a derived relation. Keep the
    // no-subquery path side-effect free, as the select-list decorrelator does.
    if !super::subquery::expr_has_subquery(where_clause) {
        return None;
    }
    let outer_scope = super::subquery::select_outer_scope(select, catalog, current_db, ctx);
    let relation_alias = "__decorrelated_predicate_0";
    let value_alias = "__decorrelated_value_0";
    let value = Expr::Column(vec![relation_alias.to_owned(), value_alias.to_owned()]);
    let mut predicate = None;
    let mut aggregate = None;
    let mut remaining = Vec::new();
    for conjunct in conjuncts(where_clause) {
        if aggregate.is_none() {
            if let Some((rewritten, found)) = rewrite_one_predicate_subquery(
                conjunct,
                &outer_scope,
                catalog,
                current_db,
                ctx,
                &value,
            ) {
                predicate = Some(rewritten);
                aggregate = Some(found);
                continue;
            }
        }
        remaining.push(conjunct.clone());
    }
    let predicate = predicate?;
    let aggregate = aggregate?;

    // Go unfolds `*` before it builds the WHERE clause, so the relation
    // a decorrelation adds is never an output column. Both arms below need
    // the unfolded list; the pull-up arm additionally reads output names off
    // it, so a `SELECT *` must be expanded before the arm is chosen.
    let mut expanded = select.clone();
    if !super::subquery::expand_unqualified_wildcards(&mut expanded, &outer_scope) {
        return None;
    }
    let select = &expanded;

    if let Some(group_key) = pull_up_group_key(select, &outer_scope) {
        let mut inner_from = aggregate.inner.from.clone()?;
        let residual_local = attach_to_inner_join(
            &mut inner_from,
            combine_and(aggregate.local_conditions.clone()),
        );
        let mut join_conditions = aggregate
            .correlations
            .iter()
            .map(|correlation| {
                Some(Expr::Binary(
                    BinaryOp::Eq,
                    Box::new(Expr::Column(
                        outer_scope.qualified_path(correlation.outer_offset)?,
                    )),
                    Box::new(Expr::Column(correlation.inner.clone())),
                ))
            })
            .collect::<Option<Vec<_>>>()?;
        if let Some(residual) = residual_local {
            join_conditions.push(residual);
        }
        let mut grouped = select.clone();
        grouped.from = Some(Join {
            left: join_node(select.from.clone()?),
            right: Some(join_node(inner_from)),
            tp: JoinType::Left,
            straight: false,
            on: combine_and(join_conditions),
            using: Vec::new(),
            natural: false,
            explicit_parens: false,
        });
        grouped.group_by = group_key
            .into_iter()
            .map(|path| GroupByItem {
                expr: Expr::Column(path),
                desc: None,
            })
            .collect();
        grouped.where_clause = combine_and(remaining);
        grouped.having = None;
        grouped.order_by.clear();
        grouped.limit = None;

        let wrapper_alias = "__decorrelated_pullup_0";
        let aggregate_alias = "__decorrelated_aggregate_0";
        let mut outer_predicate = replace_column(
            &predicate,
            &[relation_alias, value_alias],
            &Expr::Column(vec![wrapper_alias.to_owned(), aggregate_alias.to_owned()]),
        );
        let mut carriers = Vec::<(usize, String, Vec<String>)>::new();
        let mut grouped_fields = grouped.fields.fields().to_vec();
        for path in super::only_full_group_by::bare_columns(&outer_predicate) {
            if matches!(path.as_slice(), [scope, _] if scope.eq_ignore_ascii_case(wrapper_alias)) {
                continue;
            }
            let Some((offset, _, _)) = (ScopeResolver {
                scope: &outer_scope,
            })
            .resolve(&path) else {
                continue;
            };
            if carriers.iter().any(|(have, _, _)| *have == offset) {
                continue;
            }
            carriers.push((offset, format!("__decorrelated_outer_{offset}"), path));
        }
        for (_, alias, path) in &carriers {
            let source = Expr::Column(path.clone());
            outer_predicate = replace_column(
                &outer_predicate,
                &path.iter().map(String::as_str).collect::<Vec<_>>(),
                &Expr::Column(vec![wrapper_alias.to_owned(), alias.clone()]),
            );
            grouped_fields.push(SelectField::Expr {
                expr: source,
                alias: Some(alias.clone()),
            });
        }
        grouped_fields.push(SelectField::Expr {
            expr: aggregate.aggregate,
            alias: Some(aggregate_alias.to_owned()),
        });
        grouped.fields = grouped_fields.into();

        let output_names = super::from::derived_field_names(select)?;
        let mut wrapper = select.clone();
        wrapper.fields = select
            .fields
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| SelectField::Expr {
                expr: Expr::Column(vec![wrapper_alias.to_owned(), output_names[index].clone()]),
                alias: match field {
                    SelectField::Expr { alias, .. } => alias.clone(),
                    SelectField::Wildcard(_) => None,
                },
            })
            .collect::<Vec<_>>()
            .into();
        wrapper.from = Some(Join {
            left: derived_node(grouped, wrapper_alias),
            right: None,
            tp: JoinType::Cross,
            straight: false,
            on: None,
            using: Vec::new(),
            natural: false,
            explicit_parens: false,
        });
        wrapper.where_clause = Some(outer_predicate);
        wrapper.group_by.clear();
        wrapper.having = None;
        return Some(wrapper);
    }

    let mut inner = aggregate.inner;
    inner.where_clause = combine_and(aggregate.local_conditions);
    inner.group_by = aggregate
        .correlations
        .iter()
        .map(|correlation| GroupByItem {
            expr: Expr::Column(correlation.inner.clone()),
            desc: None,
        })
        .collect();
    let mut fields = vec![SelectField::Expr {
        expr: aggregate.aggregate,
        alias: Some(value_alias.to_owned()),
    }];
    fields.extend(
        aggregate
            .correlations
            .iter()
            .enumerate()
            .map(|(index, correlation)| SelectField::Expr {
                expr: Expr::Column(correlation.inner.clone()),
                alias: Some(format!("__decorrelated_key_{index}")),
            }),
    );
    inner.fields = fields.into();

    let on = combine_and(
        aggregate
            .correlations
            .iter()
            .enumerate()
            .map(|(index, correlation)| {
                let outer = outer_scope.qualified_path(correlation.outer_offset)?;
                Some(Expr::Binary(
                    BinaryOp::Eq,
                    Box::new(Expr::Column(outer)),
                    Box::new(Expr::Column(vec![
                        relation_alias.to_owned(),
                        format!("__decorrelated_key_{index}"),
                    ])),
                ))
            })
            .collect::<Option<Vec<_>>>()?,
    );

    let mut rewritten = select.clone();
    remaining.push(predicate);
    rewritten.where_clause = combine_and(remaining);
    rewritten.from = Some(Join {
        left: join_node(select.from.clone()?),
        right: Some(derived_node(inner, relation_alias)),
        tp: JoinType::Left,
        straight: false,
        on,
        using: Vec::new(),
        natural: false,
        explicit_parens: false,
    });
    Some(rewritten)
}

/// The GROUP BY key Go's aggregation pull-up arm uses: the outer source's
/// non-null primary/unique key. Go gates the arm in `LogicalApply.
/// CanPullUpAgg` (`logical_apply.go`) on the Apply carrying no join
/// condition — true here because the scalar predicate stays above — and on
/// `outerPlan.Schema().PKOrUK` being non-empty; `LogicalAggregation.
/// CanPullUp` (`logical_aggregation.go`) additionally requires an ungrouped
/// aggregate whose arguments evaluate to NULL over an all-NULL row, which
/// `scalar_aggregate_expression`'s AVG/MIN/MAX/SUM-of-column shape satisfies
/// (and COUNT, whose constant argument survives NULL-extension, does not
/// reach). Whether the correlation columns cover the key is deliberately NOT
/// asked: Go groups the join by the outer key regardless of what the
/// subquery correlates on.
fn pull_up_group_key(
    select: &SelectStmt,
    outer_scope: &super::from::FromScope,
) -> Option<Vec<Vec<String>>> {
    if !select.group_by.is_empty()
        || select.having.is_some()
        || select.distinct
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.fields.fields().iter().any(|field| match field {
            SelectField::Expr { expr, .. } => expr.has_aggregate_flag(),
            SelectField::Wildcard(_) => true,
        })
    {
        return None;
    }
    let fds = super::funcdep::scope_fd_set(outer_scope, select.from.as_ref(), None);
    let primary_key = fds.primary_key()?;
    primary_key
        .iter()
        .map(|offset| outer_scope.qualified_path(usize::try_from(offset).ok()?))
        .collect()
}

/// One SELECT-list scalar `COUNT` subquery with a HAVING, matched by
/// [`count_having_subquery`].
struct CountHavingSubquery {
    count: Expr,
    alias: Option<String>,
    having: Expr,
    inner: SelectStmt,
    local_conditions: Vec<Expr>,
    correlations: Vec<Correlation>,
}

/// Go's grouped-below aggregation arm for a SELECT-list scalar `COUNT`
/// subquery that carries a HAVING (`rule_decorrelate.go`, the
/// `LogicalApply -> LogicalAggregation -> Selection` arm with a non-empty
/// `defaultValueMap` and pulled-up join conditions).
///
/// Go reaches this shape in three steps. The HAVING becomes a `Selection`
/// above the aggregation, and the solver's Selection arm attaches its
/// decorrelated condition to the Apply as a join condition. The Apply over
/// the aggregation then cannot take the pull-up arm — `CanPullUpAgg`
/// refuses an Apply that carries join conditions — so the equality
/// correlations under the aggregation become join keys, their inner columns
/// join the GROUP BY, and the Apply becomes a left outer join to that
/// grouped relation. Finally, because `COUNT` owes a non-NULL default (0)
/// on an empty subquery input while a HAVING-filtered group owes NULL, the
/// attached conditions are taken OFF the join again (`havingConds`) and
/// applied in a projection instead: the aggregate output reads
/// `IFNULL(count, 0)`, and the scalar value becomes
/// `IF(<having over that default>, IFNULL(count, 0), NULL)`.
///
/// Boundary of this transcription, matching the sibling arms: equality-only
/// correlations, a HAVING that references nothing but the aggregate output
/// (by its alias or by repeating the aggregate) and outer columns, and a
/// single-argument non-distinct `COUNT`. Go's same arm without a HAVING
/// (the `IFNULL`-only projection) and its `BIT_AND`/`BIT_OR`/`BIT_XOR`
/// default values are not taken here; those statements keep their Apply
/// plans.
fn rewrite_count_having_fields(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<SelectStmt> {
    if !plain_outer(select) || !has_count_having_candidate(select) {
        return None;
    }
    let outer_scope = super::subquery::select_outer_scope(select, catalog, current_db, ctx);
    let outer_resolver = ScopeResolver {
        scope: &outer_scope,
    };
    let field_names = super::from::derived_field_names(select)?;
    let mut rewritten = select.clone();
    let mut fields = rewritten.fields.fields().to_vec();
    let mut changed = false;
    let mut round = 0usize;
    for (index, field) in fields.iter_mut().enumerate() {
        let SelectField::Expr { expr, alias } = field else {
            continue;
        };
        let Some(found) = count_having_subquery(expr, &outer_scope, catalog, current_db, ctx)
        else {
            continue;
        };
        let relation_alias = format!("__decorrelated_having_{round}");
        let value_alias = "__decorrelated_value_0";
        // COUNT's empty-input default from Go's `aggDefaultValueMap`.
        let default_value = Expr::Func {
            name: "IFNULL".to_owned(),
            args: vec![
                Expr::Column(vec![relation_alias.clone(), value_alias.to_owned()]),
                Expr::Int("0".to_owned()),
            ],
            origin_position: 0,
        };
        let Some(having) = rewrite_having_over_aggregate(
            &found.having,
            found.alias.as_deref(),
            &found.count,
            &default_value,
            &outer_resolver,
            &found.inner,
            catalog,
            current_db,
            ctx,
        ) else {
            continue;
        };
        let Some(on) = found
            .correlations
            .iter()
            .enumerate()
            .map(|(key, correlation)| {
                Some(Expr::Binary(
                    BinaryOp::Eq,
                    Box::new(Expr::Column(
                        outer_scope.qualified_path(correlation.outer_offset)?,
                    )),
                    Box::new(Expr::Column(vec![
                        relation_alias.clone(),
                        format!("__decorrelated_key_{key}"),
                    ])),
                ))
            })
            .collect::<Option<Vec<_>>>()
            .and_then(combine_and)
        else {
            continue;
        };
        let Some(from) = rewritten.from.clone() else {
            continue;
        };
        let mut grouped = found.inner.clone();
        grouped.having = None;
        grouped.where_clause = combine_and(found.local_conditions.clone());
        grouped.group_by = found
            .correlations
            .iter()
            .map(|correlation| GroupByItem {
                expr: Expr::Column(correlation.inner.clone()),
                desc: None,
            })
            .collect();
        let mut grouped_fields = vec![SelectField::Expr {
            expr: found.count.clone(),
            alias: Some(value_alias.to_owned()),
        }];
        grouped_fields.extend(
            found
                .correlations
                .iter()
                .enumerate()
                .map(|(key, correlation)| SelectField::Expr {
                    expr: Expr::Column(correlation.inner.clone()),
                    alias: Some(format!("__decorrelated_key_{key}")),
                }),
        );
        grouped.fields = grouped_fields.into();
        rewritten.from = Some(Join {
            left: join_node(from),
            right: Some(derived_node(grouped, &relation_alias)),
            tp: JoinType::Left,
            straight: false,
            on: Some(on),
            using: Vec::new(),
            natural: false,
            explicit_parens: false,
        });
        *expr = Expr::Func {
            name: "IF".to_owned(),
            args: vec![having, default_value, Expr::Null],
            origin_position: 0,
        };
        if alias.is_none() {
            // The scalar subquery's output name is the field's own text; the
            // replacement expression restores differently, so the original
            // name is pinned as an alias.
            *alias = Some(field_names[index].clone());
        }
        changed = true;
        round += 1;
    }
    if !changed {
        return None;
    }
    rewritten.fields = fields.into();
    Some(rewritten)
}

/// A side-effect-free gate for the only field shape
/// [`rewrite_count_having_fields`] can rewrite, mirroring
/// [`has_scalar_sum_candidate`]'s reason to exist.
fn has_count_having_candidate(select: &SelectStmt) -> bool {
    select.fields.fields().iter().any(|field| {
        let SelectField::Expr {
            expr: Expr::Subquery(query),
            ..
        } = field
        else {
            return false;
        };
        let QueryStmt::Select(inner) = &**query else {
            return false;
        };
        inner.having.is_some()
            && matches!(
                inner.fields.fields(),
                [SelectField::Expr {
                    expr: Expr::Aggregate {
                        name,
                        distinct: false,
                        args,
                    },
                    ..
                }] if name.eq_ignore_ascii_case("COUNT") && args.len() == 1
            )
    })
}

fn count_having_subquery(
    expr: &Expr,
    outer_scope: &super::from::FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<CountHavingSubquery> {
    let Expr::Subquery(query) = expr else {
        return None;
    };
    let QueryStmt::Select(inner) = &**query else {
        return None;
    };
    if !plain_scalar_inner_ignoring_having(inner) || !inner_joins_only(inner.from.as_ref()?) {
        return None;
    }
    let having = inner.having.clone()?;
    let [SelectField::Expr {
        expr:
            count @ Expr::Aggregate {
                name,
                distinct: false,
                args,
            },
        alias,
    }] = inner.fields.fields()
    else {
        return None;
    };
    if !name.eq_ignore_ascii_case("COUNT") {
        return None;
    }
    let inner_scope = super::subquery::select_outer_scope(inner, catalog, current_db, ctx);
    let inner_resolver = ScopeResolver {
        scope: &inner_scope,
    };
    // The COUNT argument must belong to the inner row or be a literal;
    // an outer reference inside the aggregate is a different Go path.
    match args.as_slice() {
        [Expr::Column(path)] => {
            inner_resolver.resolve(path)?;
        }
        [Expr::Int(_) | Expr::Decimal(_) | Expr::Float(_) | Expr::String(_) | Expr::Bool(_)] => {}
        _ => return None,
    }
    let outer_resolver = ScopeResolver { scope: outer_scope };
    let mut correlations = Vec::new();
    let mut local_conditions = Vec::new();
    for conjunct in conjuncts(inner.where_clause.as_ref()?) {
        if let Some((inner_path, outer_path)) =
            predicate_correlation_equality(conjunct, &inner_resolver, &outer_resolver)
        {
            let (outer_offset, _, _) = outer_resolver.resolve(&outer_path)?;
            let inner_path = normalize_inner_expression(
                &Expr::Column(inner_path),
                &inner_resolver,
                &inner_scope,
            )?;
            let Expr::Column(inner_path) = inner_path else {
                unreachable!()
            };
            correlations.push(Correlation {
                inner: inner_path,
                outer_offset,
            });
        } else {
            local_conditions.push(normalize_inner_expression(
                conjunct,
                &inner_resolver,
                &inner_scope,
            )?);
        }
    }
    if correlations.is_empty() {
        return None;
    }
    // Removing the correlation equalities must leave no outer reference in
    // the grouped relation this arm builds (the HAVING is validated
    // separately, over the outer scope it will live in).
    let mut uncorrelated = (**inner).clone();
    uncorrelated.having = None;
    uncorrelated.fields = vec![SelectField::Expr {
        expr: count.clone(),
        alias: None,
    }]
    .into();
    uncorrelated.where_clause = combine_and(local_conditions.clone());
    let mut remaining = Vec::new();
    super::subquery::collect_correlated_columns_query(
        &QueryStmt::Select(Box::new(uncorrelated)),
        outer_scope,
        catalog,
        current_db,
        &mut remaining,
        ctx,
    );
    if !remaining.is_empty() {
        return None;
    }
    Some(CountHavingSubquery {
        count: count.clone(),
        alias: alias.clone(),
        having,
        inner: (**inner).clone(),
        local_conditions,
        correlations,
    })
}

/// Rewrites a HAVING over the decorrelated aggregate's defaulted output:
/// every reference to the aggregate — its SELECT-list alias or the aggregate
/// expression itself — becomes `replacement` (`IFNULL(count, 0)`), outer
/// columns stay, and anything else refuses the arm.
#[allow(clippy::too_many_arguments)]
fn rewrite_having_over_aggregate(
    having: &Expr,
    alias: Option<&str>,
    aggregate: &Expr,
    replacement: &Expr,
    outer: &ScopeResolver<'_>,
    inner: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<Expr> {
    let inner_scope = super::subquery::select_outer_scope(inner, catalog, current_db, ctx);
    struct Rewrite<'a> {
        alias: Option<&'a str>,
        aggregate: &'a Expr,
        replacement: &'a Expr,
        outer: &'a ScopeResolver<'a>,
        inner: ScopeResolver<'a>,
        valid: bool,
    }

    impl tidb_ast::Visitor for Rewrite<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_mut::<Expr>() else {
                return false;
            };
            if expr == self.aggregate {
                *expr = self.replacement.clone();
                return true;
            }
            match expr {
                Expr::Column(path) => {
                    // HAVING resolves the SELECT-list alias before any table
                    // column, so the alias match must come first.
                    if path.len() == 1
                        && self
                            .alias
                            .is_some_and(|alias| path[0].eq_ignore_ascii_case(alias))
                    {
                        *expr = self.replacement.clone();
                    } else if self.inner.resolve(path).is_some()
                        || self.outer.resolve(path).is_none()
                    {
                        // An inner-row column below the aggregate, or a name
                        // this statement cannot resolve at all: not a shape
                        // this arm hoists.
                        self.valid = false;
                    }
                    true
                }
                Expr::Aggregate { .. } | Expr::GroupConcat { .. } | Expr::Subquery(_) => {
                    self.valid = false;
                    true
                }
                _ => false,
            }
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            self.valid
        }
    }

    let mut rewritten = having.clone();
    let mut visitor = Rewrite {
        alias,
        aggregate,
        replacement,
        outer,
        inner: ScopeResolver {
            scope: &inner_scope,
        },
        valid: true,
    };
    if !tidb_ast::Visitable::accept(&mut rewritten, &mut visitor) || !visitor.valid {
        return None;
    }
    Some(rewritten)
}

fn replace_column(expression: &Expr, path: &[&str], replacement: &Expr) -> Expr {
    struct Replace<'a> {
        path: &'a [&'a str],
        replacement: &'a Expr,
    }

    impl tidb_ast::Visitor for Replace<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(Expr::Column(candidate)) = node.downcast_mut::<Expr>() else {
                return false;
            };
            if candidate.len() == self.path.len()
                && candidate
                    .iter()
                    .zip(self.path)
                    .all(|(candidate, expected)| candidate.eq_ignore_ascii_case(expected))
            {
                *node
                    .downcast_mut::<Expr>()
                    .expect("the expression was matched") = self.replacement.clone();
            }
            true
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut replaced = expression.clone();
    tidb_ast::Visitable::accept(&mut replaced, &mut Replace { path, replacement });
    replaced
}

#[allow(clippy::too_many_arguments)]
fn rewrite_one_predicate_subquery(
    predicate: &Expr,
    outer_scope: &super::from::FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    replacement: &Expr,
) -> Option<(Expr, PredicateAggregate)> {
    struct Rewrite<'a> {
        outer_scope: &'a super::from::FromScope,
        catalog: &'a Catalog,
        current_db: &'a str,
        ctx: &'a crate::StmtContext,
        replacement: &'a Expr,
        aggregate: Option<PredicateAggregate>,
    }

    impl tidb_ast::Visitor for Rewrite<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_mut::<Expr>() else {
                return false;
            };
            if self.aggregate.is_some() {
                return true;
            }
            let Expr::Subquery(query) = expr else {
                return false;
            };
            let Some(aggregate) = predicate_aggregate(
                query,
                self.outer_scope,
                self.catalog,
                self.current_db,
                self.ctx,
                self.replacement,
            ) else {
                return true;
            };
            *expr = aggregate.scalar.clone();
            self.aggregate = Some(aggregate);
            true
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut rewritten = predicate.clone();
    let mut visitor = Rewrite {
        outer_scope,
        catalog,
        current_db,
        ctx,
        replacement,
        aggregate: None,
    };
    tidb_ast::Visitable::accept(&mut rewritten, &mut visitor);
    Some((rewritten, visitor.aggregate?))
}

#[allow(clippy::too_many_arguments)]
fn predicate_aggregate(
    query: &QueryStmt,
    outer_scope: &super::from::FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    replacement: &Expr,
) -> Option<PredicateAggregate> {
    let QueryStmt::Select(inner) = query else {
        return None;
    };
    if !plain_scalar_inner(inner) || !inner_joins_only(inner.from.as_ref()?) {
        return None;
    }
    let [SelectField::Expr { expr, .. }] = inner.fields.fields() else {
        return None;
    };
    let inner_scope = super::subquery::select_outer_scope(inner, catalog, current_db, ctx);
    let inner_resolver = ScopeResolver {
        scope: &inner_scope,
    };
    let outer_resolver = ScopeResolver { scope: outer_scope };
    let (aggregate, scalar) = scalar_aggregate_expression(expr, &inner_resolver, replacement)?;

    let mut correlations = Vec::new();
    let mut local_conditions = Vec::new();
    for conjunct in conjuncts(inner.where_clause.as_ref()?) {
        if let Some((inner_path, outer_path)) =
            predicate_correlation_equality(conjunct, &inner_resolver, &outer_resolver)
        {
            let (outer_offset, _, _) = outer_resolver.resolve(&outer_path)?;
            let inner_path = normalize_inner_expression(
                &Expr::Column(inner_path),
                &inner_resolver,
                &inner_scope,
            )?;
            let Expr::Column(inner_path) = inner_path else {
                unreachable!()
            };
            correlations.push(Correlation {
                inner: inner_path,
                outer_offset,
            });
        } else {
            local_conditions.push(normalize_inner_expression(
                conjunct,
                &inner_resolver,
                &inner_scope,
            )?);
        }
    }
    if correlations.is_empty() {
        return None;
    }

    let mut uncorrelated = (**inner).clone();
    uncorrelated.fields = vec![SelectField::Expr {
        expr: aggregate.clone(),
        alias: None,
    }]
    .into();
    uncorrelated.where_clause = combine_and(local_conditions.clone());
    let mut remaining = Vec::new();
    super::subquery::collect_correlated_columns_query(
        &QueryStmt::Select(Box::new(uncorrelated)),
        outer_scope,
        catalog,
        current_db,
        &mut remaining,
        ctx,
    );
    if !remaining.is_empty() {
        return None;
    }

    Some(PredicateAggregate {
        inner: (**inner).clone(),
        aggregate,
        scalar,
        local_conditions,
        correlations,
    })
}

fn scalar_aggregate_expression(
    expression: &Expr,
    inner: &ScopeResolver<'_>,
    replacement: &Expr,
) -> Option<(Expr, Expr)> {
    struct Replace<'a> {
        inner: &'a ScopeResolver<'a>,
        replacement: &'a Expr,
        aggregate: Option<Expr>,
        valid: bool,
    }

    impl tidb_ast::Visitor for Replace<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_mut::<Expr>() else {
                return false;
            };
            match expr {
                Expr::Aggregate {
                    name,
                    distinct: false,
                    args,
                } if self.aggregate.is_none()
                    && matches!(
                        name.to_ascii_uppercase().as_str(),
                        "AVG" | "MIN" | "MAX" | "SUM"
                    )
                    && matches!(args.as_slice(), [Expr::Column(path)] if self.inner.resolve(path).is_some()) =>
                {
                    self.aggregate = Some(expr.clone());
                    *expr = self.replacement.clone();
                    true
                }
                Expr::Aggregate { .. } | Expr::Column(_) | Expr::Subquery(_) => {
                    self.valid = false;
                    true
                }
                _ => false,
            }
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            self.valid
        }
    }

    let mut scalar = expression.clone();
    let mut visitor = Replace {
        inner,
        replacement,
        aggregate: None,
        valid: true,
    };
    if !tidb_ast::Visitable::accept(&mut scalar, &mut visitor) || !visitor.valid {
        return None;
    }
    Some((visitor.aggregate?, scalar))
}

fn rewrite_current(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<SelectStmt> {
    if !plain_outer(select) || !has_scalar_sum_candidate(select) {
        return None;
    }
    let outer_ref = single_table_ref(select.from.as_ref()?)?;
    let outer_scope = super::subquery::select_outer_scope(select, catalog, current_db, ctx);
    if outer_scope.tables.len() != 1 {
        return None;
    }
    let outer_resolver = ScopeResolver {
        scope: &outer_scope,
    };
    let field_names = super::from::derived_field_names(select)?;

    let mut output_by_offset = BTreeMap::new();
    let mut sums = Vec::new();
    for (field_index, field) in select.fields.fields().iter().enumerate() {
        let SelectField::Expr { expr, alias } = field else {
            return None;
        };
        if let Some(sum) = scalar_sum(
            expr,
            field_index,
            &field_names[field_index],
            alias,
            &outer_scope,
            catalog,
            current_db,
            ctx,
        ) {
            sums.push(sum);
            continue;
        }
        if super::subquery::expr_has_subquery(expr) {
            return None;
        }
        let Expr::Column(path) = expr else {
            return None;
        };
        let (offset, _, _) = outer_resolver.resolve(path)?;
        if output_by_offset
            .insert(offset, field_names[field_index].clone())
            .is_some()
        {
            return None;
        }
    }
    if sums.is_empty() {
        return None;
    }

    let table = table_entry(outer_ref, catalog, current_db)?;
    let TableEntry::Kv(table) = table else {
        return None;
    };
    let unique_key = non_null_unique_key(table)?;
    let first = &sums[0];
    let mut group_offsets = first
        .correlations
        .iter()
        .map(|correlation| correlation.outer_offset)
        .collect::<Vec<_>>();
    for offset in unique_key {
        if !group_offsets.contains(&offset) {
            group_offsets.push(offset);
        }
    }
    let outer_visible = outer_ref
        .alias
        .as_deref()
        .or_else(|| outer_ref.name.last().map(String::as_str))?;
    let outer_columns = table.visible_columns();
    let group_by = group_offsets
        .iter()
        .map(|offset| GroupByItem {
            expr: Expr::Column(vec![
                outer_visible.to_owned(),
                outer_columns[*offset].name.clone(),
            ]),
            desc: None,
        })
        .collect::<Vec<_>>();

    // The first Apply is pulled above a left join and aggregation.
    let mut inner_from = first.inner.from.clone()?;
    let residual_local =
        attach_to_inner_join(&mut inner_from, combine_and(first.local_conditions.clone()));
    let mut outer_on = first
        .correlations
        .iter()
        .map(|correlation| {
            Expr::Binary(
                BinaryOp::Eq,
                Box::new(Expr::Column(vec![
                    outer_visible.to_owned(),
                    outer_columns[correlation.outer_offset].name.clone(),
                ])),
                Box::new(Expr::Column(correlation.inner.clone())),
            )
        })
        .collect::<Vec<_>>();
    if let Some(residual) = residual_local {
        outer_on.push(residual);
    }
    let left = join_node(select.from.clone()?);
    let right = join_node(inner_from);
    let mut pulled = select.clone();
    pulled.from = Some(Join {
        left,
        right: Some(right),
        tp: JoinType::Left,
        straight: false,
        on: combine_and(outer_on),
        using: Vec::new(),
        natural: false,
        explicit_parens: false,
    });
    pulled.group_by = group_by;
    let later = sums
        .iter()
        .skip(1)
        .map(|sum| sum.field_index)
        .collect::<BTreeSet<_>>();
    let mut pulled_fields = Vec::new();
    for (index, field) in select.fields.fields().iter().enumerate() {
        if later.contains(&index) {
            continue;
        }
        if index == first.field_index {
            pulled_fields.push(SelectField::Expr {
                expr: first.sum.clone(),
                alias: first.output_alias.clone(),
            });
        } else {
            pulled_fields.push(field.clone());
        }
    }
    pulled.fields = pulled_fields.into();

    if sums.len() == 1 {
        return Some(pulled);
    }

    // Later Applies see an already-aggregated outer side. Their own SUM stays
    // below the join, grouped by the inner correlation keys.
    let mut current = pulled;
    let mut available = select
        .fields
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, _)| (!later.contains(&index)).then_some(index))
        .collect::<BTreeSet<_>>();
    for (round, sum) in sums.iter().skip(1).enumerate() {
        let outer_alias = format!("__decorrelated_outer_{round}");
        let inner_alias = format!("__decorrelated_sum_{round}");
        let mut grouped_inner = sum.inner.clone();
        grouped_inner.where_clause = combine_and(sum.local_conditions.clone());
        grouped_inner.group_by = sum
            .correlations
            .iter()
            .map(|correlation| GroupByItem {
                expr: Expr::Column(correlation.inner.clone()),
                desc: None,
            })
            .collect();
        let mut inner_fields = vec![SelectField::Expr {
            expr: sum.sum.clone(),
            alias: Some(sum.output_name.clone()),
        }];
        inner_fields.extend(
            sum.correlations
                .iter()
                .map(|correlation| SelectField::Expr {
                    expr: Expr::Column(correlation.inner.clone()),
                    alias: correlation.inner.last().cloned(),
                }),
        );
        grouped_inner.fields = inner_fields.into();

        let on = combine_and(
            sum.correlations
                .iter()
                .map(|correlation| {
                    let outer_name = output_by_offset.get(&correlation.outer_offset)?;
                    let inner_name = correlation.inner.last()?;
                    Some(Expr::Binary(
                        BinaryOp::Eq,
                        Box::new(Expr::Column(vec![inner_alias.clone(), inner_name.clone()])),
                        Box::new(Expr::Column(vec![outer_alias.clone(), outer_name.clone()])),
                    ))
                })
                .collect::<Option<Vec<_>>>()?,
        );
        let from = Join {
            left: derived_node(current, &outer_alias),
            right: Some(derived_node(grouped_inner, &inner_alias)),
            tp: JoinType::Left,
            straight: false,
            on,
            using: Vec::new(),
            natural: false,
            explicit_parens: false,
        };
        available.insert(sum.field_index);
        let mut fields = Vec::with_capacity(available.len());
        for index in available.iter().copied() {
            let from_inner = index == sum.field_index;
            fields.push(SelectField::Expr {
                expr: Expr::Column(vec![
                    if from_inner {
                        inner_alias.clone()
                    } else {
                        outer_alias.clone()
                    },
                    field_names[index].clone(),
                ]),
                alias: Some(field_names[index].clone()),
            });
        }
        let mut pass_through = select.clone();
        pass_through.fields = fields.into();
        pass_through.from = Some(from);
        pass_through.where_clause = None;
        current = pass_through;
    }
    Some(current)
}

/// A side-effect-free gate for the only field shape this rule can rewrite.
/// Scope construction may plan a view or derived relation, so it must happen
/// only after the AST proves that a scalar SUM candidate exists.
fn has_scalar_sum_candidate(select: &SelectStmt) -> bool {
    select.fields.fields().iter().any(|field| {
        let SelectField::Expr {
            expr: Expr::Subquery(query),
            ..
        } = field
        else {
            return false;
        };
        let QueryStmt::Select(inner) = &**query else {
            return false;
        };
        matches!(
            inner.fields.fields(),
            [SelectField::Expr {
                expr: Expr::Aggregate {
                    name,
                    distinct: false,
                    args,
                },
                ..
            }] if name.eq_ignore_ascii_case("SUM") && matches!(args.as_slice(), [Expr::Column(_)])
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn scalar_sum(
    expr: &Expr,
    field_index: usize,
    output_name: &str,
    output_alias: &Option<String>,
    outer_scope: &super::from::FromScope,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<ScalarSum> {
    let Expr::Subquery(query) = expr else {
        return None;
    };
    let QueryStmt::Select(inner) = &**query else {
        return None;
    };
    if !plain_scalar_inner(inner) || !inner_joins_only(inner.from.as_ref()?) {
        return None;
    }
    let [SelectField::Expr {
        expr:
            sum @ Expr::Aggregate {
                name,
                distinct: false,
                args,
            },
        ..
    }] = inner.fields.fields()
    else {
        return None;
    };
    if !name.eq_ignore_ascii_case("SUM") || !matches!(args.as_slice(), [Expr::Column(_)]) {
        return None;
    }
    let inner_scope = super::subquery::select_outer_scope(inner, catalog, current_db, ctx);
    let inner_resolver = ScopeResolver {
        scope: &inner_scope,
    };
    let outer_resolver = ScopeResolver { scope: outer_scope };
    let mut correlations = Vec::new();
    let mut local_conditions = Vec::new();
    for conjunct in conjuncts(inner.where_clause.as_ref()?) {
        if let Some((inner_path, outer_path)) =
            correlation_equality(conjunct, &inner_resolver, &outer_resolver)
        {
            let (outer_offset, _, _) = outer_resolver.resolve(&outer_path)?;
            correlations.push(Correlation {
                inner: inner_path,
                outer_offset,
            });
        } else {
            local_conditions.push(normalize_inner_expression(
                conjunct,
                &inner_resolver,
                &inner_scope,
            )?);
        }
    }
    if correlations.is_empty() {
        return None;
    }
    let mut uncorrelated = (**inner).clone();
    uncorrelated.where_clause = combine_and(local_conditions.clone());
    let mut remaining = Vec::new();
    super::subquery::collect_correlated_columns_query(
        &QueryStmt::Select(Box::new(uncorrelated)),
        outer_scope,
        catalog,
        current_db,
        &mut remaining,
        ctx,
    );
    if !remaining.is_empty() {
        return None;
    }
    // The SUM argument itself must belong to the inner row.
    let Expr::Aggregate { args, .. } = sum else {
        unreachable!()
    };
    let [Expr::Column(argument)] = args.as_slice() else {
        unreachable!()
    };
    inner_resolver.resolve(argument)?;
    Some(ScalarSum {
        field_index,
        output_name: output_name.to_owned(),
        output_alias: output_alias.clone(),
        sum: sum.clone(),
        inner: (**inner).clone(),
        local_conditions,
        correlations,
    })
}

/// Replaces parser spelling with the resolved source spelling Go's logical
/// expression carries after name resolution. This affects only plan text;
/// identifier matching and runtime semantics remain case-insensitive.
fn normalize_inner_expression(
    expression: &Expr,
    resolver: &ScopeResolver<'_>,
    scope: &super::from::FromScope,
) -> Option<Expr> {
    struct Normalize<'a> {
        resolver: &'a ScopeResolver<'a>,
        scope: &'a super::from::FromScope,
        valid: bool,
    }
    impl tidb_ast::Visitor for Normalize<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(Expr::Column(path)) = node.downcast_mut::<Expr>() {
                let Some((offset, _, _)) = self.resolver.resolve(path) else {
                    self.valid = false;
                    return false;
                };
                let Some(qualified) = self.scope.qualified_path(offset) else {
                    self.valid = false;
                    return false;
                };
                *path = qualified;
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut normalized = expression.clone();
    let mut visitor = Normalize {
        resolver,
        scope,
        valid: true,
    };
    tidb_ast::Visitable::accept(&mut normalized, &mut visitor);
    visitor.valid.then_some(normalized)
}

fn correlation_equality(
    expr: &Expr,
    inner: &ScopeResolver<'_>,
    outer: &ScopeResolver<'_>,
) -> Option<(Vec<String>, Vec<String>)> {
    let Expr::Binary(BinaryOp::Eq, left, right) = expr else {
        return None;
    };
    let Expr::Column(left) = &**left else {
        return None;
    };
    let Expr::Column(right) = &**right else {
        return None;
    };
    let classify = |path: &[String]| (inner.resolve(path).is_some(), outer.resolve(path).is_some());
    match (classify(left), classify(right)) {
        ((true, false), (false, true)) => Some((left.clone(), right.clone())),
        ((false, true), (true, false)) => Some((right.clone(), left.clone())),
        _ => None,
    }
}

fn predicate_correlation_equality(
    expr: &Expr,
    inner: &ScopeResolver<'_>,
    outer: &ScopeResolver<'_>,
) -> Option<(Vec<String>, Vec<String>)> {
    let Expr::Binary(BinaryOp::Eq, left, right) = expr else {
        return None;
    };
    let Expr::Column(left) = &**left else {
        return None;
    };
    let Expr::Column(right) = &**right else {
        return None;
    };
    // SQL name resolution searches the current query block before the outer
    // block. An unqualified inner column may therefore also be a valid outer
    // name without becoming correlated.
    let classify = |path: &[String]| {
        let local = inner.resolve(path).is_some();
        (local, !local && outer.resolve(path).is_some())
    };
    match (classify(left), classify(right)) {
        ((true, false), (false, true)) => Some((left.clone(), right.clone())),
        ((false, true), (true, false)) => Some((right.clone(), left.clone())),
        _ => None,
    }
}

fn plain_outer(select: &SelectStmt) -> bool {
    select.with.is_none()
        && select.hints.is_empty()
        && !select.sql_small_result
        && !select.sql_big_result
        && !select.sql_buffer_result
        && !select.sql_no_cache
        && !select.straight_join
        && !select.calc_found_rows
        && !select.distinct
        && !select.all
        && select.values.is_empty()
        && select.group_by.is_empty()
        && !select.rollup
        && select.having.is_none()
        && select.windows.is_empty()
        && select.order_by.is_empty()
        && select.limit.is_none()
        && select.lock.is_none()
        && select.into_outfile.is_none()
}

fn plain_scalar_inner(select: &SelectStmt) -> bool {
    select.having.is_none() && plain_scalar_inner_ignoring_having(select)
}

/// [`plain_scalar_inner`] minus the HAVING refusal, for the one arm that
/// exists to hoist a HAVING ([`rewrite_count_having_fields`]).
fn plain_scalar_inner_ignoring_having(select: &SelectStmt) -> bool {
    select.with.is_none()
        && select.hints.is_empty()
        && !select.distinct
        && select.values.is_empty()
        && select.group_by.is_empty()
        && !select.rollup
        && select.windows.is_empty()
        && select.order_by.is_empty()
        && select.limit.is_none()
        && select.lock.is_none()
        && select.into_outfile.is_none()
        && select.from.is_some()
        && select.where_clause.is_some()
}

fn single_table_ref(join: &Join) -> Option<&TableRef> {
    if join.right.is_some() || join.on.is_some() || !join.using.is_empty() || join.natural {
        return None;
    }
    match &join.left {
        JoinNode::Table(table) => Some(table),
        _ => None,
    }
}

fn table_entry<'a>(
    table: &TableRef,
    catalog: &'a Catalog,
    current_db: &str,
) -> Option<&'a TableEntry> {
    let name = table.name.last()?;
    let database = match table.name.as_slice() {
        [name] if !name.is_empty() => current_db,
        [database, _] => database,
        _ => return None,
    };
    catalog.get_in(database, name)
}

fn non_null_unique_key(table: &crate::KvTable) -> Option<Vec<usize>> {
    let key = table
        .pk_handle_offset()
        .map(|offset| vec![offset])
        .or_else(|| {
            (!table.common_handle_offsets().is_empty())
                .then(|| table.common_handle_offsets().to_vec())
        })
        .or_else(|| {
            table
                .indexes()
                .iter()
                .find(|index| {
                    index.unique
                        && index.name.eq_ignore_ascii_case("PRIMARY")
                        && !index.has_prefix()
                })
                .map(|index| index.column_offsets.clone())
        })
        .or_else(|| {
            table
                .indexes()
                .iter()
                .find(|index| index.unique && !index.has_prefix())
                .map(|index| index.column_offsets.clone())
        })?;
    key.iter()
        .all(|offset| {
            table
                .visible_columns()
                .get(*offset)
                .is_some_and(|column| column.field_type.has_flag(FieldTypeFlags::NOT_NULL))
        })
        .then_some(key)
}

fn inner_joins_only(join: &Join) -> bool {
    if join.tp != JoinType::Cross || join.natural || !join.using.is_empty() {
        return false;
    }
    node_inner_only(&join.left) && join.right.as_ref().is_none_or(node_inner_only)
}

fn node_inner_only(node: &JoinNode) -> bool {
    match node {
        JoinNode::Table(_) => true,
        JoinNode::Join(join) => inner_joins_only(join),
        JoinNode::Derived { .. } => false,
    }
}

fn join_node(join: Join) -> JoinNode {
    if join.right.is_none() && join.on.is_none() && join.using.is_empty() && !join.natural {
        join.left
    } else {
        JoinNode::Join(Box::new(join))
    }
}

fn derived_node(select: SelectStmt, alias: &str) -> JoinNode {
    JoinNode::Derived {
        subquery: tidb_ast::NodeBox::new(QueryStmt::Select(Box::new(select))),
        alias: Some(alias.to_owned()),
        lateral: false,
        column_names: Vec::new(),
    }
}

/// Places inner-local predicates on the inner join itself. A single-table
/// inner has no join node to own them, so they remain part of the outer
/// join's ON condition.
fn attach_to_inner_join(join: &mut Join, conditions: Option<Expr>) -> Option<Expr> {
    let Some(conditions) = conditions else {
        return None;
    };
    if join.right.is_none() && join.on.is_none() && join.using.is_empty() && !join.natural {
        if let JoinNode::Join(inner) = &mut join.left {
            return attach_to_inner_join(inner, Some(conditions));
        }
        return Some(conditions);
    }
    join.on = and(join.on.take(), Some(conditions));
    None
}

fn conjuncts(expr: &Expr) -> Vec<&Expr> {
    let mut result = Vec::new();
    crate::plan_trace::collect_and(expr, &mut result);
    result
}

fn combine_and(mut conditions: Vec<Expr>) -> Option<Expr> {
    let first = conditions.pop()?;
    Some(conditions.into_iter().rev().fold(first, |right, left| {
        Expr::Binary(BinaryOp::LogicAnd, Box::new(left), Box::new(right))
    }))
}

fn and(left: Option<Expr>, right: Option<Expr>) -> Option<Expr> {
    match (left, right) {
        (Some(left), Some(right)) => Some(Expr::Binary(
            BinaryOp::LogicAnd,
            Box::new(left),
            Box::new(right),
        )),
        (Some(expr), None) | (None, Some(expr)) => Some(expr),
        (None, None) => None,
    }
}
