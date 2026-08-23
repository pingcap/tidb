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

//! Go's `tidb_opt_join_reorder_through_proj`: dissolving a `Projection` that
//! sits between a join group and the joins below it.
//!
//! # What Go does, and where
//!
//! `extractJoinGroupImpl` (`rule_join_reorder.go:81`) takes an arm when the
//! node is a `LogicalProjection` and the session variable is ON:
//! `tryInlineProjectionForJoinGroup` extracts the join group from the
//! projection's CHILD and returns it with a `colExprMap` -- one entry per
//! projected column, mapping its `UniqueID` to the expression that defines it.
//! Every join condition the group carries is then rewritten through that map
//! (`SubstituteColsInEqEdges` / `SubstituteColsInExprs`,
//! `rule_join_reorder.go:265`), so a condition written against a DERIVED
//! column becomes a condition over base-table columns and the reorder can
//! attribute it to a leaf. Afterwards `restoreSchemaIfChanged` puts a
//! `Projection` back on top, rebuilding each original output column from the
//! same map.
//!
//! The point is not the projection: it is that `t2` and `t3` stop being one
//! opaque relation. `select ... from t1, (select t2.a as key_a, t2.b * 2 as
//! doubled_b from t2 join t3 on t2.a = t3.a) dt where t1.b = dt.doubled_b`
//! has a group of `{t1, dt}` with the variable OFF and `{t1, t2, t3}` with it
//! ON -- and only the second can reach the tree
//! `r/planner/core/join_reorder_through_projection.result:1319` records.
//!
//! # Why this tier does it as a STATEMENT rewrite
//!
//! Go owns a logical plan, so a dissolved projection is re-materialized above
//! the reordered join and the statement's own output columns keep pointing at
//! it. This tier plans from the AST and resolves a column by NAME against the
//! `FROM` scope ([`crate::driver::from::FromScope`]), so a derived table that
//! dissolves takes its whole name space with it: `dt.doubled_b` is no longer a
//! row anything produces. The restore therefore has to happen in the same
//! place the dissolve does -- in the statement -- which is what this module
//! is. `dt.doubled_b` in the select list, the `WHERE`, the `GROUP BY`, the
//! `HAVING` and the `ORDER BY` is replaced by `t2.b * 2` (keeping `doubled_b`
//! as the output NAME, so the result set is unchanged), and `dt.*` expands to
//! the derived table's own field list. That is `restoreSchemaIfChanged`'s
//! `colExprMap` walk, written over names instead of `UniqueID`s.
//!
//! # When it runs
//!
//! Go has two distinct paths. `ProjectionEliminator`, which runs before join
//! reorder, unconditionally removes a projection whose expressions are all
//! bare columns. A projection that still computes an expression survives that
//! pass and is dissolved by `extractJoinGroupImpl` only when
//! `@@tidb_opt_join_reorder_through_proj` is ON. The same distinction is kept
//! here: identity/pass-through projections are eliminated in a stock session;
//! computed projections retain the session-variable gate.
//!
//! This module briefly carried a second gate of its own -- a POSITIVE
//! `@@tidb_opt_join_reorder_threshold` -- because dissolving a projection is
//! useful only when a reorder can then move the freed relations, and
//! [`crate::driver::join_reorder`] modelled Go's DP solver alone, the arm a
//! positive threshold selects. That gate is gone: the greedy solver, which a
//! DEFAULT threshold selects, is modelled too, so the shape Go inlines for is
//! reachable at every threshold, exactly as in Go. The topic that exercises
//! this runs almost all of its statements at
//! `tidb_opt_join_reorder_through_proj = on` with the threshold left at its
//! default `0`, which is the greedy arm.
//!
//! # What is declined
//!
//! The safety gates are Go's, reached through
//! [`tidb_planner::join_reorder_projection_inline`], which is
//! `rule_join_reorder_projection_inline.go` transcreated: an expression must
//! reference at least one column, be built only from column / scalar-function
//! / constant nodes, and be free of mutable, non-deterministic and correlated
//! behaviour. On top of those, and matching `canInlineProjection`, every
//! expression must depend on exactly ONE leaf of the join below it.
//!
//! Three declines are this rewrite's own, each because a NAME-keyed restore
//! cannot express what a `UniqueID`-keyed one can:
//!
//! * an unqualified `*` that the caller could not expand from the catalog
//!   before this pass, whose output order would otherwise be unknown;
//! * a derived table with two output columns of the same name, which no
//!   qualified reference can tell apart;
//! * a splice that would put two relations with the same visible name in one
//!   scope (`from t1, (select ... from t1 join t2) dt`).
//!
//! A fourth is a `leading` hint. Go dissolves the projection AND still honours
//! the hint, because `generateLeadingJoinGroup` puts the named prefix in
//! `s.leadingJoinGroup` and `constructConnectedJoinTree` joins it first.
//! Nothing downstream here models that prefix, so dissolving would hand the
//! greedy a freedom the statement explicitly withheld. See
//! [`inline`]'s own gate for the recorded witness.
//!
//! # The injected column
//!
//! Dissolving turns `t1.b = dt.doubled_b` into `t1.b = t2.b * 2`, which no join
//! can KEY on -- `crate::hash_join::split_equi` takes a key only when both
//! sides are columns. Go does not leave it that way either: `injectExpr`
//! materializes the computed side as a real column of the branch that owns it,
//! and `r/planner/core/join_reorder_through_projection.result:1319` shows the
//! operator that results, `Projection  t2.a, mul(t2.b, 2)->Column`, sitting
//! between `t2` and the join that keys on it.
//!
//! [`inject_expressions`] spells that `Projection` as the only thing a `FROM`
//! clause can spell it as: a derived table wrapping the leaf, publishing that
//! leaf's own columns plus the computed one. It cannot dissolve back, because
//! [`inline`] runs once over the statement as written and this rewrite happens
//! after the splice that would have consumed it.
//!
//! An OUTER join anywhere in the dissolved subtree is declined too. Go allows
//! one and fences it with `nullExtendedCols` (`canInlineProjection`'s
//! `ExprReferenceSchema` test); declining outright is strictly the more
//! conservative of the two and keeps this rewrite's rule set small.

use std::collections::BTreeMap;

use tidb_ast::{
    BinaryOp, Expr, GroupByItem, Join, JoinNode, JoinType, OrderItem, QueryStmt, SelectField,
    SelectStmt,
};
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_planner::join_reorder_projection_inline::{
    can_inline_projection_basic, ProjectionInlineExpr, ProjectionInlineShape,
};

use crate::driver::catalog::{Catalog, TableEntry};
use crate::driver::{FromScope, FromTable};

/// One dissolved derived table: the alias it answered to, and what each of its
/// output columns is now spelled as. Go's `colExprMap`, keyed by name.
struct Dissolved {
    alias: String,
    fields: Vec<(String, Expr)>,
    /// Base relation qualifiers hidden behind `alias` before this Projection
    /// dissolved. Name resolution runs before optimization in Go, so a query
    /// that wrote one of these qualifiers must not become valid by inlining.
    hidden_qualifiers: Vec<String>,
}

/// Everything the splice accumulated while walking the `FROM` tree.
#[derive(Default)]
struct Splice {
    /// Whether `@@tidb_opt_join_reorder_through_proj` permits computed
    /// projections to dissolve.  Bare-column projections are handled by
    /// Go's earlier `ProjectionEliminator` and do not need this opt-in.
    allow_general_projection: bool,
    /// Whether any dissolved projection computed an expression rather than
    /// merely forwarding a child column. Only this case can create a
    /// non-column join key that needs Go's `injectExpr` equivalent.
    computed_projection_dissolved: bool,
    dissolved: Vec<Dissolved>,
    /// `WHERE` conjuncts lifted out of a dissolved subquery. Go reaches these
    /// through the `Selection` the subquery's own predicate pushdown left
    /// under the projection; here they join the outer `WHERE`, which is sound
    /// because every join in the dissolved subtree is an inner join.
    lifted: Vec<Expr>,
    /// The visible name of every relation now in the spliced scope, in order.
    visible: Vec<String>,
}

impl Splice {
    /// The defining expression of a written path, or `None` when the path
    /// names nothing that dissolved.
    fn lookup(&self, path: &[String]) -> Option<&Expr> {
        let (qualifier, name) = split_path(path)?;
        match qualifier {
            Some(qualifier) => self
                .dissolved
                .iter()
                .find(|d| d.alias.eq_ignore_ascii_case(qualifier))?
                .fields
                .iter()
                .find(|(field, _)| field.eq_ignore_ascii_case(name))
                .map(|(_, expr)| expr),
            None => {
                let mut hit = None;
                for dissolved in &self.dissolved {
                    for (field, expr) in &dissolved.fields {
                        if field.eq_ignore_ascii_case(name) {
                            // An unqualified name two dissolved tables both
                            // own is ambiguous; leaving it alone would keep a
                            // reference to a scope that no longer exists, so
                            // the caller declines instead.
                            if hit.is_some() {
                                return None;
                            }
                            hit = Some(expr);
                        }
                    }
                }
                hit
            }
        }
    }

    /// Whether any dissolved table would answer to `qualifier`.
    fn owns(&self, qualifier: &str) -> bool {
        self.dissolved
            .iter()
            .any(|d| d.alias.eq_ignore_ascii_case(qualifier))
    }

    fn hides(&self, qualifier: &str) -> bool {
        self.dissolved.iter().any(|d| {
            d.hidden_qualifiers
                .iter()
                .any(|hidden| hidden.eq_ignore_ascii_case(qualifier))
        })
    }
}

fn split_path(path: &[String]) -> Option<(Option<&String>, &String)> {
    match path {
        [name] => Some((None, name)),
        [.., qualifier, name] => Some((Some(qualifier), name)),
        [] => None,
    }
}

/// Go's projection-inlining arm of `extractJoinGroup`, plus
/// `restoreSchemaIfChanged`, over one `SELECT`.
///
/// Returns the rewritten statement when at least one derived table dissolved,
/// and `None` when the statement is left exactly as written. Bare-column
/// projections may dissolve at the default settings; computed projections
/// require `@@tidb_opt_join_reorder_through_proj=ON`.
pub(crate) struct InlinedSelect {
    pub(crate) select: SelectStmt,
    pub(crate) computed_output_restored: bool,
}

pub(crate) fn inline(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<InlinedSelect> {
    // A `leading` hint PINS the join order: Go builds the named prefix into
    // `s.leadingJoinGroup` and `constructConnectedJoinTree` joins it first
    // (`rule_join_reorder_greedy.go:56-76`), so it dissolves the projection
    // AND still answers to the hint. Nothing downstream here models that
    // prefix, and dissolving without it hands the greedy a freedom the
    // statement explicitly withheld -- `r/planner/core/
    // join_reorder_through_projection.result:2349` records `leading(t2, t3,
    // t1)` keeping `MergeJoin(t2, t3)` under the injected `Projection`, which
    // is the tree the UNDISSOLVED statement already builds.
    if select
        .hints
        .iter()
        .any(|hint| hint.name.eq_ignore_ascii_case("LEADING"))
    {
        return None;
    }
    let from = select.from.as_ref()?;
    let mut splice = Splice {
        allow_general_projection: ctx.join_reorder_through_proj(),
        ..Splice::default()
    };
    let rewritten_from = splice_join(from, catalog, current_db, &mut splice)?;
    if splice.dissolved.is_empty() {
        return None;
    }
    // Two relations answering to one name is a scope this tier cannot resolve
    // against; Go never meets it because it keys columns by `UniqueID`.
    for (index, name) in splice.visible.iter().enumerate() {
        if splice.visible[index + 1..]
            .iter()
            .any(|other| other.eq_ignore_ascii_case(name))
        {
            return None;
        }
    }

    let mut rewritten = select.clone();
    rewritten.from = Some(rewritten_from);

    let mut fields = Vec::new();
    for (index, field) in select.fields.fields().iter().enumerate() {
        match field {
            SelectField::Wildcard(path) => match split_path(path) {
                // `dt.*` becomes the dissolved table's own field list, each
                // field carrying the name it answered to.
                Some((_, qualifier)) if splice.owns(qualifier) => {
                    let dissolved = splice
                        .dissolved
                        .iter()
                        .find(|d| d.alias.eq_ignore_ascii_case(qualifier))?;
                    for (name, expr) in &dissolved.fields {
                        fields.push(SelectField::Expr {
                            expr: expr.clone(),
                            alias: Some(name.clone()),
                        });
                    }
                }
                Some(_) => fields.push(field.clone()),
                // A bare `*` -- see this module's doc.
                None => return None,
            },
            // The output NAME is Go's `restoreSchemaIfChanged` restoring the
            // original schema: substituting `dt.key_a` for `t2.a` must not
            // rename the result column, so the name the statement as WRITTEN
            // would have displayed is pinned as an explicit alias.
            SelectField::Expr { expr, alias } => fields.push(SelectField::Expr {
                expr: substitute(expr, &splice)?,
                alias: Some(alias.clone().unwrap_or_else(|| {
                    crate::driver::default_field_display_name(&select.fields, index, expr)
                })),
            }),
        }
    }
    let computed_output_restored = fields.iter().any(|field| {
        matches!(
            field,
            SelectField::Expr { expr, .. } if !matches!(expr, Expr::Column(_))
        )
    });
    rewritten.fields = fields.into();

    let mut conjuncts = Vec::new();
    if let Some(where_clause) = &select.where_clause {
        conjuncts.push(substitute(where_clause, &splice)?);
    }
    conjuncts.extend(splice.lifted.iter().cloned());
    rewritten.where_clause = conjuncts
        .into_iter()
        .reduce(|left, right| Expr::Binary(BinaryOp::LogicAnd, Box::new(left), Box::new(right)));

    rewritten.having = match &select.having {
        Some(having) => Some(substitute(having, &splice)?),
        None => None,
    };
    rewritten.group_by = select
        .group_by
        .iter()
        .map(|item| {
            Some(GroupByItem {
                expr: substitute(&item.expr, &splice)?,
                desc: item.desc,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    rewritten.order_by = select
        .order_by
        .iter()
        .map(|item| {
            Some(OrderItem {
                expr: substitute(&item.expr, &splice)?,
                desc: item.desc,
            })
        })
        .collect::<Option<Vec<_>>>()?;

    let computed_projection_dissolved = splice.computed_projection_dissolved;
    let select = if computed_projection_dissolved {
        // A computed output may now appear inside a join equality. Go's
        // `injectExpr` materializes it back into a column on its owning side.
        inject_expressions(rewritten, catalog, current_db)?
    } else {
        // Projection elimination only substituted columns for columns, so it
        // cannot have created a key that needs materialization. In
        // particular, do not route unrelated `column = constant` filters
        // through the injection path.
        rewritten
    };
    Some(InlinedSelect {
        select,
        computed_output_restored,
    })
}

/// Builds the lower aggregation input produced by Go
/// `AggregationPushDownSolver`'s `Aggregation -> Projection` arm.
///
/// Go substitutes only aggregate arguments, aggregate order items, and group
/// items through the child Projection. Its visible Projection and ORDER BY
/// stay above the Aggregation. This driver has no retained logical plan, so
/// source expressions in the select fields are substituted as an execution
/// adapter, while the caller retains the original statement for the visible
/// Projection and trace. In particular, ORDER BY is deliberately left in the
/// output namespace.
pub(crate) struct AggregationInputPushdown {
    pub(crate) select: SelectStmt,
    /// The derived relation's output namespace before optimizer substitution.
    /// ONLY_FULL_GROUP_BY and DISTINCT need only these bindings; physical
    /// expression types continue to come from the flattened source scope.
    pub(crate) semantic_scope: FromScope,
}

pub(crate) fn push_aggregation_inputs_through_projection(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<AggregationInputPushdown> {
    let has_aggregate = !select.group_by.is_empty()
        || select.fields.fields().iter().any(|field| match field {
            SelectField::Expr { expr, .. } => expr.has_aggregate_flag(),
            SelectField::Wildcard(_) => false,
        })
        || select.having.as_ref().is_some_and(Expr::has_aggregate_flag)
        || select
            .order_by
            .iter()
            .any(|item| item.expr.has_aggregate_flag());
    if !has_aggregate || !select.windows.is_empty() {
        return None;
    }

    let from = select.from.as_ref()?;
    if from.right.is_some()
        || from.on.is_some()
        || from.natural
        || !from.using.is_empty()
        || from.straight
    {
        return None;
    }
    let JoinNode::Derived {
        subquery,
        alias,
        lateral: false,
        column_names,
    } = &from.left
    else {
        return None;
    };
    if !column_names.is_empty() {
        return None;
    }
    let alias = alias.as_deref().filter(|alias| !alias.is_empty())?;
    let QueryStmt::Select(child) = &**subquery else {
        return None;
    };
    if child.distinct
        || child.all
        || child.with.is_some()
        || !child.hints.is_empty()
        || child.sql_small_result
        || child.sql_big_result
        || child.sql_buffer_result
        || child.sql_no_cache
        || child.straight_join
        || child.calc_found_rows
        || child.having.is_some()
        || child.limit.is_some()
        || child.lock.is_some()
        || child.into_outfile.is_some()
        || child.rollup
        || !child.group_by.is_empty()
        || !child.order_by.is_empty()
        || !child.windows.is_empty()
        || !child.values.is_empty()
    {
        return None;
    }

    let mut fields = Vec::new();
    for (index, field) in child.fields.fields().iter().enumerate() {
        let SelectField::Expr {
            expr,
            alias: field_alias,
        } = field
        else {
            return None;
        };
        if expr.has_aggregate_flag() {
            return None;
        }
        let shape = shape_of(expr);
        if !shape.is_inlineable()
            || shape.has_mutable_effects()
            || shape.is_non_deterministic()
            || shape.is_correlated()
        {
            return None;
        }
        let name = field_alias.clone().unwrap_or_else(|| {
            crate::driver::default_field_display_name(&child.fields, index, expr)
        });
        if fields
            .iter()
            .any(|(other, _): &(String, Expr)| other.eq_ignore_ascii_case(&name))
        {
            return None;
        }
        fields.push((name, expr.clone()));
    }

    let mut hidden = Splice::default();
    if let Some(child_from) = &child.from {
        collect_visible(&child_from.left, catalog, current_db, &mut hidden)?;
        if let Some(right) = &child_from.right {
            collect_visible(right, catalog, current_db, &mut hidden)?;
        }
    }
    let semantic_scope = FromScope {
        tables: vec![FromTable {
            name: alias.to_owned(),
            database: None,
            columns: fields
                .iter()
                .map(|(name, _)| (name.clone(), FieldType::new(FieldTypeCode::LongLong)))
                .collect(),
            offset: 0,
            func_deps: Default::default(),
        }],
        ..FromScope::for_statement(ctx)
    };
    let splice = Splice {
        dissolved: vec![Dissolved {
            alias: alias.to_owned(),
            fields,
            hidden_qualifiers: hidden.visible,
        }],
        ..Splice::default()
    };

    let mut rewritten = select.clone();
    rewritten.from = child.from.clone();
    let mut output = Vec::with_capacity(select.fields.fields().len());
    for (index, field) in select.fields.fields().iter().enumerate() {
        let SelectField::Expr { expr, alias } = field else {
            return None;
        };
        output.push(SelectField::Expr {
            expr: substitute(expr, &splice)?,
            alias: Some(alias.clone().unwrap_or_else(|| {
                crate::driver::default_field_display_name(&select.fields, index, expr)
            })),
        });
    }
    rewritten.fields = output.into();

    let mut predicates = Vec::new();
    if let Some(predicate) = &select.where_clause {
        predicates.push(substitute(predicate, &splice)?);
    }
    if let Some(predicate) = &child.where_clause {
        predicates.push(predicate.clone());
    }
    rewritten.where_clause = predicates
        .into_iter()
        .reduce(|left, right| Expr::Binary(BinaryOp::LogicAnd, Box::new(left), Box::new(right)));
    rewritten.having = match &select.having {
        Some(having) => Some(substitute(having, &splice)?),
        None => None,
    };
    rewritten.group_by = select
        .group_by
        .iter()
        .map(|item| {
            Some(GroupByItem {
                expr: substitute(&item.expr, &splice)?,
                desc: item.desc,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    rewritten.order_by = select.order_by.clone();
    Some(AggregationInputPushdown {
        select: rewritten,
        semantic_scope,
    })
}

/// Go's `baseSingleGroupJoinOrderSolver.injectExpr` (`rule_join_reorder.go:793`),
/// spelled as a statement rewrite.
///
/// A dissolved projection leaves equalities like `t1.b = t2.b * 2`, which no
/// join can KEY on. Go materializes the computed side as a real column of the
/// branch that owns it -- `LogicalProjection{Exprs: Column2Exprs(schema)}` over
/// that branch, then `AppendExpr` -- and rewrites the edge to name the new
/// column. The recorded plan shows exactly that operator:
/// `r/planner/core/join_reorder_through_projection.result:1319` has
/// `Projection  t2.a, mul(t2.b, 2)->Column` sitting between `t2` and the join
/// that keys on it.
///
/// The `FROM`-clause spelling of "a projection over one leaf, publishing that
/// leaf's own columns plus one more" is a derived table wrapping the leaf. It
/// cannot dissolve back: [`inline`] runs ONCE over the statement as written,
/// and this rewrite happens after the splice that would have consumed it.
///
/// Declines when the equality is not a join edge between two DIFFERENT
/// relations, which keeps the previous refusal for every shape Go's
/// `checkConnection` would not have built an edge from either.
fn inject_expressions(
    mut rewritten: SelectStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Option<SelectStmt> {
    let relations = base_relations(rewritten.from.as_ref()?, catalog, current_db)?;
    let mut injections: Vec<Injection> = Vec::new();
    // `AppendExpr`'s reuse test: the SAME expression on the SAME relation is
    // materialized once, however many edges need it.
    let mut register = |relation: usize, expr: &Expr| -> String {
        if let Some(found) = injections
            .iter()
            .find(|i| i.relation == relation && &i.expr == expr)
        {
            return found.name.clone();
        }
        let name = format!("_inject_{}", injections.len());
        injections.push(Injection {
            relation,
            expr: expr.clone(),
            name: name.clone(),
        });
        name
    };

    // One walk that both decides and rewrites: every `Eq` whose two sides read
    // two different relations and whose operands are not both columns.
    let mut declined = false;
    let mut rewrite = |expr: &mut Expr| {
        let Expr::Binary(BinaryOp::Eq, lhs, rhs) = expr else {
            return;
        };
        let (left_is_column, right_is_column) = (
            matches!(strip(lhs), Expr::Column(_)),
            matches!(strip(rhs), Expr::Column(_)),
        );
        if left_is_column && right_is_column {
            return;
        }
        let (Some(left), Some(right)) = (
            single_relation(lhs, &relations),
            single_relation(rhs, &relations),
        ) else {
            declined = true;
            return;
        };
        if left == right {
            // Not a join edge: `checkConnection` never builds one from a
            // condition both of whose sides live in one relation, so there is
            // no key to preserve and nothing to inject.
            declined = true;
            return;
        }
        if !left_is_column {
            let name = register(left, lhs);
            **lhs = Expr::Column(vec![relations[left].visible.clone(), name]);
        }
        if !right_is_column {
            let name = register(right, rhs);
            **rhs = Expr::Column(vec![relations[right].visible.clone(), name]);
        }
    };
    if let Some(where_clause) = &mut rewritten.where_clause {
        for_each_conjunct_mut(where_clause, &mut rewrite);
    }
    for_each_on_conjunct_mut(rewritten.from.as_mut()?, &mut rewrite);
    if declined {
        return None;
    }
    if injections.is_empty() {
        return Some(rewritten);
    }
    // A wrapped relation publishes ONE MORE column than the statement wrote,
    // so a surviving `t2.*` would expand to a row shape the statement never
    // asked for. Go never meets this: `restoreSchemaIfChanged` rebuilds the
    // original schema by `UniqueID` above the reordered join.
    for field in rewritten.fields.fields() {
        if let SelectField::Wildcard(path) = field {
            let (_, qualifier) = split_path(path)?;
            if injections.iter().any(|i| {
                relations[i.relation]
                    .visible
                    .eq_ignore_ascii_case(qualifier)
            }) {
                return None;
            }
        }
    }
    // A generated name a column of the wrapped table already answers to would
    // shadow it; there is no safe rename, so decline.
    for injection in &injections {
        if relations[injection.relation]
            .columns
            .iter()
            .any(|column| column.eq_ignore_ascii_case(&injection.name))
        {
            return None;
        }
    }
    wrap_relations(rewritten.from.as_mut()?, &relations, &injections)?;
    Some(rewritten)
}

/// One expression Go would have materialized, and where.
struct Injection {
    /// Index into the `relations` list: the branch that owns the expression.
    relation: usize,
    expr: Expr,
    /// The name the wrapper publishes it under. Go's injected column is
    /// anonymous; a `FROM` clause has to spell one.
    name: String,
}

/// One base-table leaf of the spliced `FROM`, as the injection needs it.
struct Relation {
    visible: String,
    columns: Vec<String>,
}

/// Every base-table leaf of the spliced `FROM`, in order.
///
/// `None` declines: a leaf this cannot open by name is one whose columns the
/// resolver below could attribute wrongly, and an injection put on the wrong
/// branch is a wrong key.
fn base_relations(join: &Join, catalog: &Catalog, current_db: &str) -> Option<Vec<Relation>> {
    let mut relations = Vec::new();
    collect_base_relations(join, catalog, current_db, &mut relations)?;
    Some(relations)
}

fn collect_base_relations(
    join: &Join,
    catalog: &Catalog,
    current_db: &str,
    out: &mut Vec<Relation>,
) -> Option<()> {
    let node = |node: &JoinNode, out: &mut Vec<Relation>| -> Option<()> {
        match node {
            JoinNode::Join(inner) => collect_base_relations(inner, catalog, current_db, out),
            JoinNode::Table(table_ref) => {
                if table_ref.as_of.is_some() || !table_ref.partitions.is_empty() {
                    return None;
                }
                let (database, name) =
                    crate::driver::split_table_path(&table_ref.name, current_db).ok()?;
                let TableEntry::Kv(table) = catalog.get_in(database, name)? else {
                    return None;
                };
                out.push(Relation {
                    visible: table_ref.alias.clone().unwrap_or_else(|| name.to_owned()),
                    columns: table
                        .visible_columns()
                        .iter()
                        .map(|column| column.name.clone())
                        .collect(),
                });
                Some(())
            }
            // A derived table that did NOT dissolve is opaque here: its output
            // names are not the catalog's, so an unqualified path could belong
            // to it without this knowing.
            JoinNode::Derived { .. } => None,
        }
    };
    node(&join.left, out)?;
    match &join.right {
        Some(right) => node(right, out),
        None => Some(()),
    }
}

/// Which relation an expression reads, when it reads exactly one.
///
/// `None` for an expression over several relations or over a name this cannot
/// resolve -- Go's `injectExpr` puts the projection on ONE branch, so an
/// expression spanning two has no branch to go on.
fn single_relation(expr: &Expr, relations: &[Relation]) -> Option<usize> {
    let mut hit = None;
    for path in column_paths(expr) {
        let (qualifier, name) = split_path(&path)?;
        let mut found = None;
        for (index, relation) in relations.iter().enumerate() {
            if let Some(qualifier) = qualifier {
                if !relation.visible.eq_ignore_ascii_case(qualifier) {
                    continue;
                }
            }
            if relation
                .columns
                .iter()
                .any(|column| column.eq_ignore_ascii_case(name))
            {
                // An unqualified name two relations own is the statement's own
                // ambiguity error, not an injection decision.
                if found.is_some() {
                    return None;
                }
                found = Some(index);
            }
        }
        let found = found?;
        if hit.is_some_and(|hit| hit != found) {
            return None;
        }
        hit = Some(found);
    }
    hit
}

/// Runs `f` over every top-level `AND` conjunct of an expression, in place.
fn for_each_conjunct_mut(expr: &mut Expr, f: &mut dyn FnMut(&mut Expr)) {
    match expr {
        Expr::Binary(BinaryOp::LogicAnd, left, right) => {
            for_each_conjunct_mut(left, f);
            for_each_conjunct_mut(right, f);
        }
        Expr::Paren(inner) => for_each_conjunct_mut(inner, f),
        other => f(other),
    }
}

/// The same, over every `ON` of a join tree.
fn for_each_on_conjunct_mut(join: &mut Join, f: &mut dyn FnMut(&mut Expr)) {
    if let Some(on) = &mut join.on {
        for_each_conjunct_mut(on, f);
    }
    if let JoinNode::Join(inner) = &mut join.left {
        for_each_on_conjunct_mut(inner, f);
    }
    if let Some(JoinNode::Join(inner)) = &mut join.right {
        for_each_on_conjunct_mut(inner, f);
    }
}

/// Replaces every injected-into leaf with the derived table that publishes its
/// own columns plus the injected expressions -- Go's `LogicalProjection` over
/// that branch, spelled in the `FROM`.
fn wrap_relations(join: &mut Join, relations: &[Relation], injections: &[Injection]) -> Option<()> {
    let mut index = 0usize;
    wrap_node(&mut join.left, relations, injections, &mut index)?;
    if let Some(right) = &mut join.right {
        wrap_node(right, relations, injections, &mut index)?;
    }
    Some(())
}

fn wrap_node(
    node: &mut JoinNode,
    relations: &[Relation],
    injections: &[Injection],
    index: &mut usize,
) -> Option<()> {
    match node {
        JoinNode::Join(inner) => {
            wrap_node(&mut inner.left, relations, injections, index)?;
            if let Some(right) = &mut inner.right {
                wrap_node(right, relations, injections, index)?;
            }
            Some(())
        }
        JoinNode::Table(_) => {
            let position = *index;
            *index += 1;
            let mine: Vec<&Injection> = injections
                .iter()
                .filter(|i| i.relation == position)
                .collect();
            if mine.is_empty() {
                return Some(());
            }
            let relation = &relations[position];
            // `Column2Exprs(p.Schema().Columns)`: the pass-through half, so the
            // branch's own outputs are unchanged, then `AppendExpr` for each
            // injected expression.
            //
            // Go's `PruneColumns` then narrows this projection to the columns
            // read above it, which is what lets its leaf take a COVERING
            // index (`result:1604` reads `IndexFullScan  t2, index:b(b)`
            // under the pruned `Projection  t2.a, t2.b, mul(t2.b,2)`).
            // Publishing the pruned set here was MEASURED and reverted: it
            // closed the covering rows it targeted but re-priced the hash
            // alternative below every parent merge-vs-hash comparison, and
            // this tier's candidate assembly does not yet reproduce Go's
            // node shapes (cop Selection below the projection, the reader's
            // net term) exactly enough for those comparisons to land on Go's
            // side -- `join_reorder_through_projection` went 5 -> 6 recorded
            // divergences, with the narrow-output statements flipping to a
            // whole-hash tree Go does not build. NAMED RESIDUE: Go's
            // `PruneColumns` over this wrapper, blocked on candidate-shape
            // fidelity at the sites `build_join_with_choice` prices.
            let mut fields: Vec<SelectField> = relation
                .columns
                .iter()
                .map(|column| SelectField::Expr {
                    expr: Expr::Column(vec![relation.visible.clone(), column.clone()]),
                    alias: Some(column.clone()),
                })
                .collect();
            for injection in mine {
                fields.push(SelectField::Expr {
                    expr: injection.expr.clone(),
                    alias: Some(injection.name.clone()),
                });
            }
            // The wrapper is built from a PARSED skeleton rather than a
            // struct literal, so a clause this rewrite has no opinion about --
            // `DISTINCT`, `LIMIT`, a lock, a hint -- is whatever an empty
            // statement has, and stays that way when `SelectStmt` grows a
            // field.
            let mut subquery = match tidb_parser::parse("SELECT 1 FROM _").ok()? {
                tidb_ast::Stmt::Query(query) => match &*query {
                    QueryStmt::Select(select) => (**select).clone(),
                    _ => return None,
                },
                _ => return None,
            };
            subquery.fields = fields.into();
            subquery.from = Some(Join {
                left: node.clone(),
                right: None,
                tp: JoinType::Cross,
                straight: false,
                on: None,
                using: Vec::new(),
                natural: false,
                explicit_parens: false,
            });
            *node = JoinNode::Derived {
                subquery: tidb_ast::NodeBox::new(QueryStmt::Select(Box::new(subquery))),
                alias: Some(relation.visible.clone()),
                lateral: false,
                column_names: Vec::new(),
            };
            Some(())
        }
        JoinNode::Derived { .. } => None,
    }
}

fn strip(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(inner) => strip(inner),
        other => other,
    }
}

/// `expr` with every reference to a dissolved column replaced by its defining
/// expression -- Go's `SubstituteColsInExpr`.
///
/// `None` declines: a qualified path naming a dissolved table but no column of
/// it, or an unqualified path two dissolved tables both answer to, cannot be
/// rewritten and must not be left pointing at a scope that no longer exists.
fn substitute(expr: &Expr, splice: &Splice) -> Option<Expr> {
    struct Rewrite<'a> {
        splice: &'a Splice,
        ok: bool,
    }
    impl tidb_ast::Visitor for Rewrite<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(expr) = node.downcast_mut::<Expr>() else {
                return false;
            };
            let Expr::Column(path) = expr else {
                return false;
            };
            match self.splice.lookup(path) {
                Some(defining) => {
                    *expr = defining.clone();
                    // The defining expression is already written against base
                    // relations, so it is not itself a substitution target --
                    // Go's `rewriteExprTree` stops at a replaced node too.
                    true
                }
                None => {
                    // A qualified reference into a table that dissolved must
                    // have resolved; anything else names a surviving relation
                    // and is left alone.
                    if let Some((Some(qualifier), _)) = split_path(path) {
                        if self.splice.owns(qualifier) || self.splice.hides(qualifier) {
                            self.ok = false;
                        }
                    } else if self
                        .splice
                        .dissolved
                        .iter()
                        .filter(|d| {
                            d.fields.iter().any(|(field, _)| {
                                split_path(path)
                                    .is_some_and(|(_, name)| field.eq_ignore_ascii_case(name))
                            })
                        })
                        .count()
                        > 1
                    {
                        self.ok = false;
                    }
                    false
                }
            }
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut owned = expr.clone();
    let mut rewrite = Rewrite { splice, ok: true };
    tidb_ast::Visitable::accept(&mut owned, &mut rewrite);
    rewrite.ok.then_some(owned)
}

/// Replaces every unqualified column with the visible leaf name that uniquely
/// owns it. Go keeps that identity in the column's `UniqueID` when a derived
/// Projection is eliminated; this AST rewrite must make the same identity
/// explicit before the derived alias disappears into a larger name scope.
fn qualify_unique_columns(
    expr: &Expr,
    leaves: &[(String, Vec<String>)],
    owners: &BTreeMap<String, Vec<usize>>,
) -> Option<Expr> {
    struct Qualify<'a> {
        leaves: &'a [(String, Vec<String>)],
        owners: &'a BTreeMap<String, Vec<usize>>,
        ok: bool,
    }
    impl tidb_ast::Visitor for Qualify<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            let Some(Expr::Column(path)) = node.downcast_mut::<Expr>() else {
                return false;
            };
            let [name] = path.as_slice() else {
                return false;
            };
            let Some([owner]) = self
                .owners
                .get(&name.to_ascii_lowercase())
                .map(Vec::as_slice)
            else {
                self.ok = false;
                return true;
            };
            *path = vec![self.leaves[*owner].0.clone(), name.clone()];
            true
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut owned = expr.clone();
    let mut qualify = Qualify {
        leaves,
        owners,
        ok: true,
    };
    tidb_ast::Visitable::accept(&mut owned, &mut qualify);
    qualify.ok.then_some(owned)
}

/// Walks one join spine, replacing every derived table that qualifies with the
/// join below its projection.
fn splice_join(
    join: &Join,
    catalog: &Catalog,
    current_db: &str,
    splice: &mut Splice,
) -> Option<Join> {
    // A parenthesized subtree marks a name-scope boundary Go does not rotate
    // across; leaving it whole keeps this rewrite out of that question.
    if join.natural || !join.using.is_empty() || join.straight || join.tp != JoinType::Cross {
        collect_visible(&join.left, catalog, current_db, splice)?;
        if let Some(right) = &join.right {
            collect_visible(right, catalog, current_db, splice)?;
        }
        return Some(join.clone());
    }
    let left = splice_node(&join.left, catalog, current_db, splice)?;
    let right = match &join.right {
        Some(right) => Some(splice_node(right, catalog, current_db, splice)?),
        None => None,
    };
    let on = match &join.on {
        // The `ON` is rewritten LAST, against everything the two sides
        // dissolved, which is what lets `on t1.a = dt.key_a` survive `dt`.
        Some(on) => Some(substitute(on, splice)?),
        None => None,
    };
    Some(Join {
        left,
        right,
        on,
        ..join.clone()
    })
}

fn splice_node(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
    splice: &mut Splice,
) -> Option<JoinNode> {
    match node {
        JoinNode::Join(inner) => Some(JoinNode::Join(Box::new(splice_join(
            inner, catalog, current_db, splice,
        )?))),
        JoinNode::Derived { .. } => match dissolve(node, catalog, current_db, splice) {
            Some(spliced) => Some(spliced),
            None => {
                collect_visible(node, catalog, current_db, splice)?;
                Some(node.clone())
            }
        },
        JoinNode::Table(_) => {
            collect_visible(node, catalog, current_db, splice)?;
            Some(node.clone())
        }
    }
}

/// Records the visible names one subtree contributes, for the collision check.
fn collect_visible(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
    splice: &mut Splice,
) -> Option<()> {
    match node {
        JoinNode::Join(inner) => {
            collect_visible(&inner.left, catalog, current_db, splice)?;
            if let Some(right) = &inner.right {
                collect_visible(right, catalog, current_db, splice)?;
            }
            Some(())
        }
        _ => {
            let (visible, _) = leaf_names(node, catalog, current_db)?;
            splice.visible.push(visible);
            Some(())
        }
    }
}

/// The name a leaf answers to and the columns it publishes.
fn leaf_names(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
) -> Option<(String, Vec<String>)> {
    match node {
        JoinNode::Table(table_ref) => {
            let (database, name) =
                crate::driver::split_table_path(&table_ref.name, current_db).ok()?;
            let TableEntry::Kv(table) = catalog.get_in(database, name)? else {
                return None;
            };
            let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
            let columns = table
                .visible_columns()
                .iter()
                .map(|column| column.name.clone())
                .collect();
            Some((visible, columns))
        }
        JoinNode::Derived {
            subquery, alias, ..
        } => {
            let alias = alias.as_deref().filter(|alias| !alias.is_empty())?;
            let QueryStmt::Select(select) = &**subquery else {
                return None;
            };
            Some((
                alias.to_owned(),
                crate::driver::from::derived_field_names(select)?,
            ))
        }
        JoinNode::Join(_) => None,
    }
}

/// Go's `tryInlineProjectionForJoinGroup` for one derived table.
///
/// On success the derived node is replaced by the join UNDER its projection,
/// its fields are recorded in `splice`, and its own `WHERE` is lifted.
fn dissolve(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
    splice: &mut Splice,
) -> Option<JoinNode> {
    let JoinNode::Derived {
        subquery,
        alias,
        lateral,
        column_names,
    } = node
    else {
        return None;
    };
    if *lateral || !column_names.is_empty() {
        return None;
    }
    let alias = alias.as_deref().filter(|alias| !alias.is_empty())?;
    let QueryStmt::Select(select) = &**subquery else {
        return None;
    };
    // Anything that changes the row count on its own way up is not a bare
    // `Projection` over a `Join`, which is the ONLY shape Go's arm accepts
    // (`tryInlineProjectionForJoinGroup` requires `proj.Children()[0]` to be a
    // `LogicalJoin`).
    if select.distinct
        || select.with.is_some()
        || select.having.is_some()
        || select.limit.is_some()
        || !select.group_by.is_empty()
        || !select.order_by.is_empty()
        || !select.windows.is_empty()
    {
        return None;
    }
    let from = select.from.as_ref()?;
    if !is_all_inner_join(from) {
        return None;
    }

    // The subtree below this projection is spliced FIRST, so a stacked
    // `Projection -> Join -> Projection -> Join` dissolves bottom-up and this
    // projection's own expressions are rewritten against base relations --
    // Go's bottom-up `colExprMap` propagation (`rule_join_reorder.go:52`).
    let mut inner = Splice {
        allow_general_projection: splice.allow_general_projection,
        ..Splice::default()
    };
    let spliced_from = splice_join(from, catalog, current_db, &mut inner)?;
    for (index, name) in inner.visible.iter().enumerate() {
        if inner.visible[index + 1..]
            .iter()
            .any(|other| other.eq_ignore_ascii_case(name))
        {
            return None;
        }
    }

    // Which leaf owns each column now visible below the projection.
    let mut owner: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    let mut leaves = Vec::new();
    collect_leaf_names(&spliced_from, catalog, current_db, &mut leaves)?;
    for (index, (_, columns)) in leaves.iter().enumerate() {
        for column in columns {
            owner
                .entry(column.to_ascii_lowercase())
                .or_default()
                .push(index);
        }
    }
    let leaf_of = |path: &[String]| -> Option<usize> {
        let (qualifier, name) = split_path(path)?;
        match qualifier {
            Some(qualifier) => leaves.iter().position(|(visible, columns)| {
                visible.eq_ignore_ascii_case(qualifier)
                    && columns.iter().any(|own| own.eq_ignore_ascii_case(name))
            }),
            None => match owner.get(&name.to_ascii_lowercase())?.as_slice() {
                [only] => Some(*only),
                _ => None,
            },
        }
    };

    let mut fields = Vec::new();
    for (index, field) in select.fields.fields().iter().enumerate() {
        let SelectField::Expr {
            expr,
            alias: field_alias,
        } = field
        else {
            // A `*` inside the projection has no field list to restore from.
            return None;
        };
        if expr.has_aggregate_flag() {
            return None;
        }
        let written = expr;
        let expr = qualify_unique_columns(&substitute(expr, &inner)?, &leaves, &owner)?;
        // `canInlineProjection`: every expression must depend on exactly one
        // leaf, since the reorder has to attribute it to one side.
        let paths = column_paths(&expr);
        if paths.is_empty() {
            return None;
        }
        let mut single = None;
        for path in &paths {
            let leaf = leaf_of(path)?;
            if *single.get_or_insert(leaf) != leaf {
                return None;
            }
        }
        let name = field_alias.clone().unwrap_or_else(|| {
            crate::driver::default_field_display_name(&select.fields, index, written)
        });
        fields.push((name, expr));
    }
    // `ProjectionEliminator` runs before join reorder and removes a logical
    // projection whenever every expression is a single column.  That rule is
    // unconditional; `tidb_opt_join_reorder_through_proj` gates only the
    // later inlining of projections that still compute expressions.
    let projection_eliminable = fields
        .iter()
        .all(|(_, expr)| matches!(strip(expr), Expr::Column(_)));
    if !projection_eliminable && !splice.allow_general_projection {
        return None;
    }
    // `canInlineProjectionBasic`, over the shapes Go's own gate is written
    // against.
    let shape = ProjectionInlineShape::new(
        false,
        fields.iter().map(|(_, expr)| shape_of(expr)).collect(),
    );
    if !can_inline_projection_basic(&shape) {
        return None;
    }
    // Two output columns of one name cannot be told apart by a qualified
    // reference once the alias is gone.
    for (index, (name, _)) in fields.iter().enumerate() {
        if fields[index + 1..]
            .iter()
            .any(|(other, _)| other.eq_ignore_ascii_case(name))
        {
            return None;
        }
    }

    let mut lifted = std::mem::take(&mut inner.lifted);
    if let Some(where_clause) = &select.where_clause {
        lifted.push(substitute(where_clause, &inner)?);
    }

    splice.computed_projection_dissolved |=
        inner.computed_projection_dissolved || !projection_eliminable;
    splice.dissolved.extend(inner.dissolved);
    splice.dissolved.push(Dissolved {
        alias: alias.to_owned(),
        fields,
        hidden_qualifiers: inner.visible.clone(),
    });
    splice.lifted.extend(lifted);
    splice.visible.extend(inner.visible);
    Some(JoinNode::Join(Box::new(spliced_from)))
}

/// Every leaf of a spliced subtree, in order.
fn collect_leaf_names(
    join: &Join,
    catalog: &Catalog,
    current_db: &str,
    out: &mut Vec<(String, Vec<String>)>,
) -> Option<()> {
    let push = |node: &JoinNode, out: &mut Vec<(String, Vec<String>)>| match node {
        JoinNode::Join(inner) => collect_leaf_names(inner, catalog, current_db, out),
        other => {
            out.push(leaf_names(other, catalog, current_db)?);
            Some(())
        }
    };
    push(&join.left, out)?;
    match &join.right {
        Some(right) => push(right, out),
        None => Some(()),
    }
}

/// Whether every join in a subtree is an INNER join with no `USING`/`NATURAL`
/// spelling -- see this module's doc for why an outer join is declined.
fn is_all_inner_join(join: &Join) -> bool {
    if join.right.is_none() && join.on.is_none() && join.using.is_empty() && !join.natural {
        return matches!(&join.left, JoinNode::Join(inner) if is_all_inner_join(inner))
            || !matches!(&join.left, JoinNode::Join(_));
    }
    if join.tp != JoinType::Cross || join.natural || !join.using.is_empty() || join.straight {
        return false;
    }
    let ok = |node: &JoinNode| match node {
        JoinNode::Join(inner) => is_all_inner_join(inner),
        _ => true,
    };
    ok(&join.left) && join.right.as_ref().is_none_or(ok)
}

/// Every column path an expression reads.
fn column_paths(expr: &Expr) -> Vec<Vec<String>> {
    struct Collect(Vec<Vec<String>>);
    impl tidb_ast::Visitor for Collect {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(Expr::Column(path)) = node.downcast_ref::<Expr>() {
                self.0.push(path.clone());
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut collector = Collect(Vec::new());
    let mut owned = expr.clone();
    tidb_ast::Visitable::accept(&mut owned, &mut collector);
    collector.0
}

/// The written expression as the shape Go's inlining gate reasons about.
///
/// Everything outside Column / ScalarFunction / Constant is
/// [`ProjectionInlineExpr::Unsupported`], which is Go's own default arm in
/// `isInlineableProjectionExpr`: a node whose evaluation this rule cannot
/// reason about is not inlined, whatever else is true of it.
fn shape_of(expr: &Expr) -> ProjectionInlineExpr {
    let function =
        |name: &str, args: Vec<ProjectionInlineExpr>| ProjectionInlineExpr::ScalarFunction {
            args,
            mutable_effects: is_mutable_effects(name),
            non_deterministic: is_unfoldable(name),
            correlated: false,
        };
    match expr {
        Expr::Column(_) => ProjectionInlineExpr::Column,
        Expr::Int(_)
        | Expr::Decimal(_)
        | Expr::Float(_)
        | Expr::Hex(_)
        | Expr::Bit(_)
        | Expr::String(_)
        | Expr::RawString(_)
        | Expr::Null
        | Expr::Bool(_) => ProjectionInlineExpr::Constant { deferred: false },
        Expr::Paren(inner) => shape_of(inner),
        Expr::Binary(op, left, right) => function(
            &format!("{op:?}").to_ascii_lowercase(),
            vec![shape_of(left), shape_of(right)],
        ),
        Expr::Unary(_, inner) => function("unary", vec![shape_of(inner)]),
        Expr::Func { name, args, .. } => function(
            &name.to_ascii_lowercase(),
            args.iter().map(shape_of).collect(),
        ),
        // Go's expression rewriter lowers the parser's dedicated EXTRACT
        // syntax to an ordinary scalar function before either optimizer rule
        // inspects it.
        Expr::Extract { value, .. } => function("extract", vec![shape_of(value)]),
        Expr::Cast(cast) => function("cast", vec![shape_of(&cast.expr)]),
        // Anything not named above -- a subquery, a user or system variable, a
        // `VALUES()`, a parameter marker, a window call -- is Go's default arm.
        other => ProjectionInlineExpr::Unsupported {
            referenced_columns: column_paths(other).len(),
        },
    }
}

/// Go `unFoldableFunctions` (`pkg/expression/function_traits.go:48`).
pub(super) fn is_unfoldable(name: &str) -> bool {
    matches!(
        name,
        "sysdate"
            | "found_rows"
            | "rand"
            | "uuid"
            | "uuid_v4"
            | "uuid_v7"
            | "sleep"
            | "row"
            | "values"
            | "setvar"
            | "getvar"
            | "getparam"
            | "benchmark"
            | "dayname"
            | "nextval"
            | "lastval"
            | "setval"
            | "any_value"
    )
}

/// Go `mutableEffectsFunctions` (`pkg/expression/function_traits.go:224`).
pub(super) fn is_mutable_effects(name: &str) -> bool {
    matches!(
        name,
        "now"
            | "current_timestamp"
            | "utc_time"
            | "curtime"
            | "current_time"
            | "utc_timestamp"
            | "unix_timestamp"
            | "sysdate"
            | "curdate"
            | "current_date"
            | "utc_date"
            | "rand"
            | "random_bytes"
            | "uuid"
            | "uuid_v4"
            | "uuid_v7"
            | "uuid_short"
            | "sleep"
            | "setvar"
            | "getvar"
            | "any_value"
    )
}
