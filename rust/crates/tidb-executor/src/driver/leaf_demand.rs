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

//! Which columns of each base-table LEAF of a `FROM` the statement reads:
//! Go's `rule_column_pruning.go` delivering every `DataSource` the column set
//! its parents demand, expressed over the statement text.
//!
//! # Why a leaf needs this at all
//!
//! Go costs an access path per `DataSource`, and the single input that decides
//! whether an index path is a SINGLE scan or a double read is
//! `isCoveringIndex(path.IdxCols, ds.schema.Columns)` -- the columns the
//! `DataSource`'s parents still need after pruning. A leaf under a join has
//! exactly the same question to answer as a lone table does, so the same
//! answer has to reach it; without one, every leaf declares that it needs
//! every column, no index ever covers, and `TableFullScan` wins by
//! construction rather than by cost.
//!
//! [`crate::column_prune`] cannot supply it. That module NARROWS a source's
//! output, so it must be exact in both directions and refuses every shape it
//! cannot prove (any subquery, three or more tables, a derived table). This
//! one only feeds the COST MODEL: the source it informs still emits the whole
//! row, so a demand that is too wide costs a covering index as a double read
//! and falls back to the scan that would have run anyway. That asymmetry is
//! what lets this analysis be a name-level over-approximation and stay safe.
//!
//! # The over-approximation, stated
//!
//! A leaf `t` needs its column `c` when ANY of these appears anywhere in the
//! statement -- select list, `WHERE`, `ON`, `GROUP BY`, `HAVING`, `ORDER BY`,
//! `LIMIT`, and INSIDE every subquery of all of them:
//!
//! * `*`, or `t.*` -- every column of `t`;
//! * `t.c` or `db.t.c` -- matched on the leaf's VISIBLE name, which is its
//!   alias when it has one, exactly as a column reference resolves;
//! * a bare `c` -- charged to EVERY leaf owning a column of that name,
//!   because which one it resolves to needs the whole `FROM` scope and this
//!   runs before that scope exists.
//!
//! The bare-name rule is the over-approximation, and it is the safe
//! direction: charging `c` to a leaf that does not own the reference can only
//! make an index look less covering than it is, which costs the leaf the
//! ordinary table scan.
//!
//! A construct this walk cannot see through does not silently narrow the
//! answer: it charges every column of every leaf, and therefore reproduces
//! the behaviour of no analysis at all.
//!
//! The [`Expr`] match is EXHAUSTIVE with no wildcard arm, so a new expression
//! variant is a compile error here rather than a subtree nobody walks -- the
//! same rule [`crate::column_prune`] holds itself to.
//!
//! # The second fact this walk carries
//!
//! [`LeafDemand::forces_index`] is Go's `StmtCtx.GetIndexForce()`, the other
//! statement-wide input [`crate::access_cost::enumerate_paths`] reads. It
//! rides here because it is the same question asked of the same nodes and has
//! the same one consumer; its own doc says why, and what Go does with it.
//!
//! # Where the walk is rooted, and the gap that leaves
//!
//! Both facts are computed from the `SELECT` being planned, so a subquery
//! planned on its own sees its OWN text and everything nested inside it, but
//! not the statement enclosing it. For the column demand that is the safe
//! direction (a leaf under a subquery is charged its own references, and the
//! enclosing statement cannot make it need FEWER). For the index-force flag
//! it is not symmetric: a `USE INDEX` written in the OUTER query does not yet
//! penalize a full scan inside an inner one, where Go's session-scoped flag
//! would. Go raises the flag during `DeriveStats` over the whole logical
//! tree, which this tier has no equivalent of; a `StmtContext` field set once
//! at the statement boundary is what would close it.

use std::collections::{BTreeMap, BTreeSet};

use tidb_ast::{
    Expr, Join, JoinNode, QueryStmt, SelectField, SelectStmt, WindowDef, WindowOver, WindowSpec,
};
use tidb_datatype::FieldType;

/// What an enclosing `SELECT` hands down to every relation of its `FROM`:
/// Go's two pre-physical rules, `rule_predicate_push_down` and
/// `rule_column_pruning`, travelling together because they travel to the same
/// places.
///
/// Both are `None`/empty for a caller that has no statement above the `FROM`
/// -- a subquery built through [`crate::driver::from::build_join`] directly,
/// or a `FROM` with no filter -- which is [`FromDemand::none`].
#[derive(Clone, Copy)]
pub(crate) struct FromDemand<'a> {
    /// The `WHERE` equalities offered to the joins below; see
    /// [`crate::driver::predicate_push_down`].
    pub(crate) offered: crate::driver::predicate_push_down::Offered<'a>,
    /// Per-relation predicates derived by Go's join predicate-pushdown rule.
    pub(crate) pushdown: Option<&'a crate::driver::predicate_push_down::Plan>,
    /// The columns each base-table leaf must still produce, or `None` when
    /// the caller has no statement to compute them from -- which every leaf
    /// reads as "every column", the answer it gave before this existed.
    pub(crate) columns: Option<&'a LeafDemand>,
    /// Every relation of this `FROM` with the row count `derive_stats` gives
    /// it -- the estimate owner the join-strategy search prices its
    /// candidates with. `None` is a `FROM` whose shape
    /// [`crate::driver::join_reorder::row_source`] declines, which the search
    /// reads as "this site cannot be priced" and refuses.
    pub(crate) rows: Option<&'a crate::driver::join_reorder::RowSource>,
    /// The join algorithms the statement NAMED, or `None` when it named none
    /// -- Go's `hintInfo == nil` in `SetPreferredJoinTypeAndOrder`. A join
    /// site reads it before it decides a merge join; see
    /// [`crate::driver::join_method_hints`].
    pub(crate) join_hints: Option<&'a crate::driver::join_method_hints::JoinMethodHints>,
    /// The recursively costed physical strategy for every join of this
    /// `FROM`, when the whole tree was representable by the native planner.
    pub(crate) join_guide: Option<&'a crate::driver::join_search::RecursiveGuide>,
}

impl FromDemand<'_> {
    /// The demand a caller with nothing to offer passes.
    pub(crate) fn none() -> Self {
        FromDemand {
            offered: &[],
            pushdown: None,
            columns: None,
            rows: None,
            join_hints: None,
            join_guide: None,
        }
    }
}

/// The column names a statement reads, keyed the way a `FROM` leaf can be
/// named. See the module doc for the rules and for why they may over-count.
#[derive(Debug, Default, Clone)]
pub(crate) struct LeafDemand {
    /// A bare `*`, or a construct this walk cannot see through: every leaf
    /// needs every column.
    all: bool,
    /// `t.*`, keyed by the lowercased qualifier.
    star_tables: BTreeSet<String>,
    /// Lowercased column names written with no qualifier.
    unqualified: BTreeSet<String>,
    /// Lowercased column names written as `t.c`, keyed by lowercased `t`.
    qualified: BTreeMap<String, BTreeSet<String>>,
    /// Go `StmtCtx.SetIndexForce`, the OTHER statement-wide fact
    /// [`crate::access_cost::enumerate_paths`] reads: whether ANY table of
    /// the statement carries a `USE`/`FORCE INDEX`.
    ///
    /// It rides this walk rather than a second one because it is the same
    /// question asked of the same nodes -- what the whole statement, its
    /// subqueries included, says about the leaves below it -- and it has the
    /// same one consumer. A second exhaustive [`Expr`] match would be a
    /// second traversal that could silently disagree with this one about
    /// which subqueries exist.
    ///
    /// Go raises this from `stats.go`'s `getGeneralAttributesFromPaths` the
    /// moment any `AccessPath` of the statement is `path.Forced`, and
    /// `getTableScanPenalty` then charges EVERY full table scan of the
    /// statement -- including one over a table no hint named. `IGNORE INDEX`
    /// does not force; `USE INDEX ()` does, because Go forces the table path
    /// itself there.
    forces_index: bool,
    /// Named windows visible in each SELECT currently being walked. Nested
    /// queries push their own scope.
    window_scopes: Vec<Vec<(String, WindowDef)>>,
}

impl LeafDemand {
    /// The demand a `SELECT`'s clauses place on the leaves of its own `FROM`.
    pub(crate) fn of_select(select: &SelectStmt) -> Self {
        let mut demand = LeafDemand::default();
        demand.add_select(select);
        demand
    }

    /// The columns referenced above this SELECT's `FROM`, excluding the
    /// relations' own definitions and join predicates. Outer-join elimination
    /// uses this after removing the join: a column read inside the surviving
    /// outer derived table is not a read of an equally named eliminated inner
    /// column.
    pub(crate) fn of_select_parent_clauses(select: &SelectStmt) -> Self {
        let mut demand = LeafDemand::default();
        if demand.reject_select_shape(select) {
            return demand;
        }
        demand.window_scopes.push(select.windows.clone());
        demand.add_select_parent_clauses(select);
        demand.window_scopes.pop();
        demand
    }

    /// The offsets of `columns` this leaf must still produce, given that it is
    /// visible under the name `visible`.
    ///
    /// The offsets are ascending and unique, which is the shape
    /// [`crate::access_cost::enumerate_paths`] reads them in.
    pub(crate) fn needed(&self, visible: &str, columns: &[(String, FieldType)]) -> Vec<usize> {
        let visible = visible.to_ascii_lowercase();
        if self.all || self.star_tables.contains(&visible) {
            return (0..columns.len()).collect();
        }
        let qualified = self.qualified.get(&visible);
        columns
            .iter()
            .enumerate()
            .filter(|(_, (name, _))| {
                let name = name.to_ascii_lowercase();
                self.unqualified.contains(&name)
                    || qualified.is_some_and(|names| names.contains(&name))
            })
            .map(|(offset, _)| offset)
            .collect()
    }

    /// Go `StmtCtx.GetIndexForce()`: see [`LeafDemand::forces_index`].
    pub(crate) const fn statement_forces_an_index(&self) -> bool {
        self.forces_index
    }

    /// Records one written column path in whichever of the two spellings it
    /// used. A path longer than `db.t.c` names nothing this tier resolves.
    fn add_path(&mut self, path: &[String]) {
        match path {
            [name] => {
                self.unqualified.insert(name.to_ascii_lowercase());
            }
            [table, name] | [_, table, name] => {
                self.qualified
                    .entry(table.to_ascii_lowercase())
                    .or_default()
                    .insert(name.to_ascii_lowercase());
            }
            _ => self.all = true,
        }
    }

    /// Records a `*` / `t.*` / `db.t.*` wildcard.
    fn add_wildcard(&mut self, path: &[String]) {
        match path {
            [] => self.all = true,
            [table] | [_, table] => {
                self.star_tables.insert(table.to_ascii_lowercase());
            }
            _ => self.all = true,
        }
    }

    /// Adds every column reference of one `SELECT`, its `FROM` included.
    fn add_select(&mut self, select: &SelectStmt) {
        // A CTE introduces relation names this walk cannot tell apart from
        // the leaves below it, and `VALUES` names columns through machinery
        // outside the expression tree.
        if self.reject_select_shape(select) {
            return;
        }
        self.window_scopes.push(select.windows.clone());
        self.add_select_parent_clauses(select);
        if let Some(join) = &select.from {
            self.add_join(join);
        }
        self.window_scopes.pop();
    }

    fn reject_select_shape(&mut self, select: &SelectStmt) -> bool {
        let rejected = select.with.is_some() || !select.values.is_empty();
        if rejected {
            self.all = true;
        }
        rejected
    }

    fn add_select_parent_clauses(&mut self, select: &SelectStmt) {
        for field in select.fields.fields() {
            match field {
                SelectField::Wildcard(path) => self.add_wildcard(path),
                SelectField::Expr { expr, alias: _ } => self.add_expr(expr),
            }
        }
        for predicate in [select.where_clause.as_ref(), select.having.as_ref()]
            .into_iter()
            .flatten()
        {
            self.add_expr(predicate);
        }
        for item in &select.group_by {
            self.add_expr(&item.expr);
        }
        for item in &select.order_by {
            self.add_expr(&item.expr);
        }
        if let Some(limit) = &select.limit {
            self.add_expr(&limit.count);
            if let Some(offset) = &limit.offset {
                self.add_expr(offset);
            }
        }
    }

    /// Adds the references a `FROM` tree writes itself: every `ON`, every
    /// `USING` name, and every derived table's own subquery.
    fn add_join(&mut self, join: &Join) {
        // A `USING (a, b)` names the columns by bare name on BOTH sides, and
        // `NATURAL` names them by the two sides' shared names -- which this
        // walk cannot enumerate without the catalog.
        if join.natural {
            self.all = true;
        }
        for name in &join.using {
            self.unqualified.insert(name.to_ascii_lowercase());
        }
        if let Some(on) = &join.on {
            self.add_expr(on);
        }
        self.add_join_node(&join.left);
        if let Some(right) = &join.right {
            self.add_join_node(right);
        }
    }

    fn add_join_node(&mut self, node: &JoinNode) {
        match node {
            // A base table writes no expression of its own -- but it is
            // where a `USE`/`FORCE INDEX` is written, and that is a
            // STATEMENT-wide fact (see [`LeafDemand::forces_index`]).
            JoinNode::Table(table_ref) => {
                if table_ref.hints.iter().any(|hint| {
                    // Go: a hint outside `HintForScan` is skipped before its
                    // names are even looked at, so `FOR JOIN` never forces.
                    hint.scope == tidb_ast::IndexHintScope::All
                        && hint.kind != tidb_ast::IndexHintKind::Ignore
                }) {
                    self.forces_index = true;
                }
            }
            JoinNode::Join(join) => self.add_join(join),
            JoinNode::Derived { subquery, .. } => self.add_query(subquery),
        }
    }

    /// Adds a nested query's references, because a correlated one names a
    /// column of THIS statement's leaves.
    fn add_query(&mut self, query: &QueryStmt) {
        match query {
            QueryStmt::Select(select) => self.add_select(select),
            // A set operation's terms are `SelectStmt`s reached through a
            // shape this walk does not model; the fallback names everything.
            QueryStmt::SetOpr(_) => self.all = true,
        }
    }

    fn add_window_over(&mut self, over: &WindowOver) {
        match over {
            WindowOver::Name(name) => {
                let Some(def) = self.named_window(name) else {
                    self.all = true;
                    return;
                };
                self.add_window_def(&def, &mut vec![name.to_ascii_lowercase()]);
            }
            WindowOver::Def(def) => self.add_window_def(def, &mut Vec::new()),
        }
    }

    fn add_window_def(&mut self, def: &WindowDef, seen: &mut Vec<String>) {
        if let Some(base) = &def.base {
            let key = base.to_ascii_lowercase();
            if seen.contains(&key) {
                self.all = true;
                return;
            }
            let Some(base_def) = self.named_window(base) else {
                self.all = true;
                return;
            };
            seen.push(key);
            self.add_window_def(&base_def, seen);
            seen.pop();
        }
        self.add_window_spec(&def.spec);
    }

    fn named_window(&self, name: &str) -> Option<WindowDef> {
        self.window_scopes
            .last()?
            .iter()
            .find_map(|(candidate, def)| candidate.eq_ignore_ascii_case(name).then(|| def.clone()))
    }

    fn add_window_spec(&mut self, spec: &WindowSpec) {
        for expr in &spec.partition_by {
            self.add_expr(expr);
        }
        for item in &spec.order_by {
            self.add_expr(&item.expr);
        }
    }

    /// Adds every column reference inside one expression.
    ///
    /// Exhaustive with no wildcard arm on purpose: see the module doc.
    fn add_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Column(path) => self.add_path(path),
            Expr::Default(Some(path)) => self.add_path(path),
            Expr::MatchAgainst {
                columns,
                against,
                modifier: _,
            } => {
                for path in columns {
                    self.add_path(path);
                }
                self.add_expr(against);
            }

            // Leaves: no column can hide inside them.
            Expr::Int(_)
            | Expr::Decimal(_)
            | Expr::Float(_)
            | Expr::Hex(_)
            | Expr::Bit(_)
            | Expr::String(_)
            | Expr::RawString(_)
            | Expr::CharsetString { .. }
            | Expr::Null
            | Expr::Bool(_)
            | Expr::Default(None)
            | Expr::ParamMarker { .. }
            | Expr::UserVar(_)
            | Expr::SysVar { .. } => {}

            // Go `LogicalWindow.extractUsedCols`: the call arguments plus
            // PARTITION BY / ORDER BY columns are the child's demand.
            Expr::Window { args, over, .. } => {
                for arg in args {
                    self.add_expr(arg);
                }
                self.add_window_over(over);
            }

            // Nested queries: a correlated reference inside one names a
            // column of the leaves this demand is computed for.
            Expr::Subquery(subquery) => self.add_query(subquery),
            Expr::Exists { subquery, not: _ } => self.add_query(subquery),
            Expr::InSubquery {
                expr,
                subquery,
                not: _,
            } => {
                self.add_expr(expr);
                self.add_query(subquery);
            }
            Expr::CompareSubquery {
                op: _,
                left,
                all: _,
                subquery,
            } => {
                self.add_expr(left);
                self.add_query(subquery);
            }

            // Recursions.
            Expr::Paren(inner)
            | Expr::Unary(_, inner)
            | Expr::Assign { value: inner, .. }
            | Expr::CharsetBinary { value: inner, .. }
            | Expr::Interval { value: inner, .. }
            | Expr::Extract { value: inner, .. }
            | Expr::WeightString { expr: inner, .. }
            | Expr::GetFormat { expr: inner, .. }
            | Expr::Is { expr: inner, .. }
            | Expr::ConvertUsing { expr: inner, .. }
            | Expr::Collate { expr: inner, .. } => self.add_expr(inner),
            Expr::Binary(_, left, right)
            | Expr::Position {
                substr: left,
                str: right,
            }
            | Expr::TimestampAdd {
                interval: left,
                expr: right,
                ..
            }
            | Expr::TimestampDiff {
                expr1: left,
                expr2: right,
                ..
            }
            | Expr::Like {
                expr: left,
                pattern: right,
                ..
            }
            | Expr::Regexp {
                expr: left,
                pattern: right,
                ..
            }
            | Expr::MemberOf {
                expr: left,
                array: right,
            } => {
                self.add_expr(left);
                self.add_expr(right);
            }
            Expr::Trim {
                expr,
                remstr,
                direction: _,
            } => {
                self.add_expr(expr);
                if let Some(remstr) = remstr {
                    self.add_expr(remstr);
                }
            }
            Expr::Row(items)
            | Expr::Func { args: items, .. }
            | Expr::GenericFuncCall { args: items, .. }
            | Expr::Aggregate { args: items, .. } => {
                for item in items {
                    self.add_expr(item);
                }
            }
            Expr::GroupConcat { args, order_by, .. } => {
                for arg in args {
                    self.add_expr(arg);
                }
                for item in order_by {
                    self.add_expr(&item.expr);
                }
            }
            Expr::In { expr, list, not: _ } => {
                self.add_expr(expr);
                for item in list {
                    self.add_expr(item);
                }
            }
            Expr::Between {
                expr,
                low,
                high,
                not: _,
            } => {
                self.add_expr(expr);
                self.add_expr(low);
                self.add_expr(high);
            }
            Expr::Case {
                value,
                when_clauses,
                else_clause,
            } => {
                if let Some(value) = value {
                    self.add_expr(value);
                }
                for (condition, result) in when_clauses {
                    self.add_expr(condition);
                    self.add_expr(result);
                }
                if let Some(else_clause) = else_clause {
                    self.add_expr(else_clause);
                }
            }
            Expr::Cast(cast) => self.add_expr(&cast.expr),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::FieldTypeCode;

    fn select_of(sql: &str) -> SelectStmt {
        let stmt = tidb_parser::parse(sql).expect("the test statement parses");
        let tidb_ast::Stmt::Query(query) = stmt else {
            panic!("the test statement is a query");
        };
        let tidb_ast::QueryStmt::Select(select) = &*query else {
            panic!("the test statement is a SELECT");
        };
        (**select).clone()
    }

    fn columns(names: &[&str]) -> Vec<(String, FieldType)> {
        names
            .iter()
            .map(|name| ((*name).to_owned(), FieldType::new(FieldTypeCode::LongLong)))
            .collect()
    }

    fn needed_of(sql: &str, visible: &str, names: &[&str]) -> Vec<usize> {
        LeafDemand::of_select(&select_of(sql)).needed(visible, &columns(names))
    }

    /// The whole point: a leaf under a join is charged only the columns its
    /// own name reaches, so a two-column index can cover it.
    #[test]
    fn a_join_leaf_is_charged_only_its_own_qualified_columns() {
        assert_eq!(
            needed_of(
                "select t1.a, t2.b from t1 join t2 on t1.c = t2.c",
                "t1",
                &["a", "b", "c"]
            ),
            vec![0, 2],
            "t1 contributes a and c; t2.b is charged to t2"
        );
        assert_eq!(
            needed_of(
                "select t1.a, t2.b from t1 join t2 on t1.c = t2.c",
                "t2",
                &["a", "b", "c"]
            ),
            vec![1, 2]
        );
    }

    /// The alias is the name a column reference resolves through, so it is
    /// the name the demand is keyed by.
    #[test]
    fn an_alias_is_the_name_the_demand_is_keyed_by() {
        assert_eq!(
            needed_of(
                "select x.a from t1 x, t2 y where y.b = x.b",
                "x",
                &["a", "b"]
            ),
            vec![0, 1]
        );
        assert_eq!(
            needed_of(
                "select x.a from t1 x, t2 y where y.b = x.b",
                "t1",
                &["a", "b"]
            ),
            Vec::<usize>::new(),
            "the written name no longer reaches an aliased table"
        );
    }

    /// A bare name is charged to every leaf that owns one -- the stated
    /// over-approximation, and the direction that cannot mis-plan.
    #[test]
    fn a_bare_column_name_is_charged_to_every_leaf_that_has_it() {
        assert_eq!(
            needed_of("select a from t1 join t2 on t1.c = t2.c", "t2", &["a", "c"]),
            vec![0, 1],
            "`a` may belong to t2, so t2 is charged for it"
        );
    }

    /// A wildcard names every column, which is exactly the answer no
    /// analysis would have given.
    #[test]
    fn a_wildcard_names_every_column() {
        assert_eq!(
            needed_of(
                "select * from t1 join t2 on t1.c = t2.c",
                "t1",
                &["a", "b", "c"]
            ),
            vec![0, 1, 2]
        );
        assert_eq!(
            needed_of(
                "select t1.* from t1 join t2 on t1.c = t2.c",
                "t1",
                &["a", "b", "c"]
            ),
            vec![0, 1, 2]
        );
        assert_eq!(
            needed_of(
                "select t1.* from t1 join t2 on t1.c = t2.c",
                "t2",
                &["a", "b", "c"]
            ),
            vec![2],
            "the other side of `t1.*` is still charged only its ON column"
        );
    }

    #[test]
    fn rollup_charges_only_grouping_and_aggregate_inputs() {
        let sql = "select t1.a, dt.key_a, dt.sum_b from t1 join (\
            select t2.a as key_a, sum(t3.b) as sum_b \
            from t2 join t3 on t2.a = t3.a \
            group by t2.a with rollup\
          ) dt on t1.a = dt.key_a";
        assert_eq!(needed_of(sql, "t1", &["a", "b", "c"]), vec![0]);
        assert_eq!(needed_of(sql, "t2", &["a", "b", "c"]), vec![0]);
        assert_eq!(needed_of(sql, "t3", &["a", "b", "c"]), vec![0, 1]);
    }

    /// A correlated reference lives inside a subquery, and it names a column
    /// of THIS statement's leaf -- so the walk descends into one.
    #[test]
    fn a_correlated_reference_inside_a_subquery_is_charged_to_its_leaf() {
        assert_eq!(
            needed_of(
                "select c2 = (select c2 from t2 where t1.c1 = t2.c1 order by c1 limit 1) from t1",
                "t1",
                &["c1", "c2", "c3"]
            ),
            vec![0, 1],
            "c1 is reached only through the subquery's correlated equality"
        );
    }

    /// A derived table in the FROM contributes its subquery's references,
    /// and its own alias is charged nothing a base leaf would answer for.
    #[test]
    fn a_derived_table_contributes_its_subquerys_references() {
        assert_eq!(
            needed_of(
                "select t1.a, dt.k from t1, (select t2.a as k from t2 join t3 on t2.b = t3.b) dt \
                 where t1.a = dt.k",
                "t2",
                &["a", "b", "c"]
            ),
            vec![0, 1]
        );
    }

    /// Go's logical window passes only its arguments and ordering columns to
    /// its child. That is what lets `idx(a)` cover `sum(a) over ()` without
    /// pretending the query reads the rest of the table row.
    #[test]
    fn a_window_charges_only_its_arguments_partition_and_order_columns() {
        assert_eq!(
            needed_of(
                "select row_number() over (partition by t1.b) from t1 join t2 on t1.c = t2.c",
                "t1",
                &["a", "b", "c"]
            ),
            vec![1, 2],
        );
        assert_eq!(
            needed_of(
                "select sum(t1.a) over (partition by t1.b order by t1.c) \
                 from t1 join t2 on t1.a = t2.a",
                "t1",
                &["a", "b", "c", "unused"]
            ),
            vec![0, 1, 2],
        );
    }

    #[test]
    fn a_named_window_resolves_its_base_chain_in_the_current_select() {
        assert_eq!(
            needed_of(
                "select sum(t1.a) over w2 from t1 join t2 on t1.a = t2.a \
                 window w1 as (partition by t1.b), w2 as (w1 order by t1.c)",
                "t1",
                &["a", "b", "c", "unused"]
            ),
            vec![0, 1, 2],
        );
    }

    /// `USING` names its columns on both sides by bare name.
    #[test]
    fn using_names_its_columns_on_both_sides() {
        assert_eq!(
            needed_of("select t1.a from t1 join t2 using (c)", "t2", &["a", "c"]),
            vec![1],
            "USING names `c` on both sides; `a` was written qualified to t1"
        );
    }
}
