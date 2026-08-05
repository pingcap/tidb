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
//! Only when `@@tidb_opt_join_reorder_through_proj` is ON, which is Go's own
//! and only gate (`extractJoinGroupImpl`, `rule_join_reorder.go:80`). It is
//! OFF by default, so a stock session never dissolves anything.
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
//! * an unqualified `*` in the select list, whose expansion order and column
//!   names would have to be reconstructed from the dissolved scope;
//! * a derived table with two output columns of the same name, which no
//!   qualified reference can tell apart;
//! * a splice that would put two relations with the same visible name in one
//!   scope (`from t1, (select ... from t1 join t2) dt`).
//!
//! A fourth is this tier's EXECUTOR boundary rather than its resolver's. Go
//! puts an eq-edge back into `col = col` form with
//! `baseSingleGroupJoinOrderSolver.injectExpr`, which materializes
//! `t2.b * 2` as a column of the branch that owns it, so `t1.b = dt.doubled_b`
//! survives the dissolve as a hash/merge/index-join KEY. A rebuilt `FROM`
//! clause has no way to spell that injected column, and
//! `crate::hash_join::split_equi` only takes a key whose two sides are both
//! columns -- so the same join would come out of the dissolve as a nested loop
//! over a residual predicate. Dissolving a projection must not make the plan
//! worse, so an inline that would leave a non-column operand in any equality
//! is declined and the statement keeps the derived table it was written with.
//! Landing the injected column is the next rung, and it is the one that opens
//! the recorded `IndexHashJoin(Projection(t2), t1)` shapes.
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
use tidb_planner::join_reorder_projection_inline::{
    can_inline_projection_basic, ProjectionInlineExpr, ProjectionInlineShape,
};

use crate::driver::catalog::{Catalog, TableEntry};

/// One dissolved derived table: the alias it answered to, and what each of its
/// output columns is now spelled as. Go's `colExprMap`, keyed by name.
struct Dissolved {
    alias: String,
    fields: Vec<(String, Expr)>,
}

/// Everything the splice accumulated while walking the `FROM` tree.
#[derive(Default)]
struct Splice {
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
/// and `None` when the statement is left exactly as written -- which is every
/// statement a stock session runs, since the gate is off by default.
pub(crate) fn inline(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Option<SelectStmt> {
    if !ctx.join_reorder_through_proj() {
        return None;
    }
    let from = select.from.as_ref()?;
    let mut splice = Splice::default();
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

    // Every equality must still read `col = col`; see this module's doc for
    // why an injected column is the prerequisite for anything else.
    let mut equalities = Vec::new();
    if let Some(where_clause) = &rewritten.where_clause {
        crate::plan_trace::collect_and(where_clause, &mut equalities);
    }
    collect_on_conjuncts(rewritten.from.as_ref()?, &mut equalities);
    if equalities.iter().any(|conjunct| {
        matches!(conjunct, Expr::Binary(BinaryOp::Eq, lhs, rhs)
            if !matches!(strip(lhs), Expr::Column(_)) || !matches!(strip(rhs), Expr::Column(_)))
    }) {
        return None;
    }
    Some(rewritten)
}

/// Every `ON` conjunct of a join tree.
fn collect_on_conjuncts<'a>(join: &'a Join, out: &mut Vec<&'a Expr>) {
    if let Some(on) = &join.on {
        crate::plan_trace::collect_and(on, out);
    }
    let walk = |node: &'a JoinNode, out: &mut Vec<&'a Expr>| {
        if let JoinNode::Join(inner) = node {
            collect_on_conjuncts(inner, out);
        }
    };
    walk(&join.left, out);
    if let Some(right) = &join.right {
        walk(right, out);
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
                        if self.splice.owns(qualifier) {
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
    let mut inner = Splice::default();
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
        let expr = substitute(expr, &inner)?;
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

    splice.dissolved.extend(inner.dissolved);
    splice.dissolved.push(Dissolved {
        alias: alias.to_owned(),
        fields,
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
        Expr::Cast(cast) => function("cast", vec![shape_of(&cast.expr)]),
        // Anything not named above -- a subquery, a user or system variable, a
        // `VALUES()`, a parameter marker, a window call -- is Go's default arm.
        other => ProjectionInlineExpr::Unsupported {
            referenced_columns: column_paths(other).len(),
        },
    }
}

/// Go `unFoldableFunctions` (`pkg/expression/function_traits.go:48`).
fn is_unfoldable(name: &str) -> bool {
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
fn is_mutable_effects(name: &str) -> bool {
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
