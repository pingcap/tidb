// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! AST-level inlining for single-consumer, non-recursive CTEs.
//!
//! Go decides this in
//! `pkg/planner/core/logical_plan_builder.go::computeCTEInlineFlag`: without
//! recursion or an explicit override, only `consumerCount == 1` is inlined;
//! zero and multiple consumers remain materialized. The count follows
//! `pkg/planner/core/preprocess.go::UpdateCTEConsumerCount`, resolving an
//! unqualified table name from the innermost visible `WITH` scope. Replacing
//! that table reference with a derived query mirrors
//! `pkg/planner/core/logical_plan_builder.go::buildDataSourceFromCTEMerge`.

use std::any::Any;

use tidb_ast::{
    Cte, JoinNode, QueryStmt, SelectStmt, SetOprStmt, TableRef, Visitable, Visitor, WithClause,
};

#[derive(Clone, Copy)]
enum ScopePhase {
    Definition(usize),
    Body,
}

#[derive(Clone)]
struct Binding {
    name: String,
    target: Option<usize>,
}

struct Scope {
    bindings: Vec<Binding>,
    recursive: bool,
    phase: ScopePhase,
    next_definition: usize,
}

impl Scope {
    fn targets(with: &WithClause, phase: ScopePhase) -> Self {
        Self {
            bindings: with
                .ctes
                .iter()
                .enumerate()
                .map(|(index, cte)| Binding {
                    name: cte.name.clone(),
                    target: Some(index),
                })
                .collect(),
            recursive: with.recursive,
            phase,
            next_definition: 0,
        }
    }

    fn shadows(with: &WithClause) -> Self {
        Self {
            bindings: with
                .ctes
                .iter()
                .map(|cte| Binding {
                    name: cte.name.clone(),
                    target: None,
                })
                .collect(),
            recursive: with.recursive,
            phase: ScopePhase::Definition(0),
            next_definition: 0,
        }
    }

    fn visible_bindings(&self) -> &[Binding] {
        let end = match self.phase {
            ScopePhase::Body => self.bindings.len(),
            ScopePhase::Definition(index) => {
                // A non-recursive declaration sees only earlier declarations.
                // WITH RECURSIVE additionally exposes the one being built.
                index
                    .saturating_add(usize::from(self.recursive))
                    .min(self.bindings.len())
            }
        };
        &self.bindings[..end]
    }
}

#[derive(Clone, Copy)]
enum ResolvedBinding {
    Target(usize),
    Shadow,
}

/// Scope state shared by counting, rewriting, and base-table qualification.
struct QueryScopes {
    scopes: Vec<Scope>,
    select_pushes: Vec<bool>,
    set_opr_pushes: Vec<bool>,
}

impl QueryScopes {
    fn with_outer(with: &WithClause, phase: ScopePhase) -> Self {
        Self {
            scopes: vec![Scope::targets(with, phase)],
            select_pushes: Vec::new(),
            set_opr_pushes: Vec::new(),
        }
    }

    fn enter(&mut self, node: &mut dyn Any) {
        if let Some(select) = node.downcast_mut::<SelectStmt>() {
            let pushed = select.with.as_ref().is_some_and(|with| {
                self.scopes.push(Scope::shadows(with));
                true
            });
            self.select_pushes.push(pushed);
            return;
        }
        if let Some(set_opr) = node.downcast_mut::<SetOprStmt>() {
            let pushed = set_opr.with.as_ref().is_some_and(|with| {
                self.scopes.push(Scope::shadows(with));
                true
            });
            self.set_opr_pushes.push(pushed);
            return;
        }
        if node.is::<Cte>() {
            let scope = self
                .scopes
                .last_mut()
                .expect("a CTE is visited beneath its WITH scope");
            let index = scope.next_definition;
            scope.next_definition += 1;
            scope.phase = ScopePhase::Definition(index);
        }
    }

    fn leave(&mut self, node: &mut dyn Any) {
        if node.is::<WithClause>() {
            self.scopes
                .last_mut()
                .expect("a WITH clause owns a scope")
                .phase = ScopePhase::Body;
            return;
        }
        if node.is::<SelectStmt>() {
            if self
                .select_pushes
                .pop()
                .expect("SELECT scope entries are balanced")
            {
                self.scopes.pop();
            }
            return;
        }
        if node.is::<SetOprStmt>()
            && self
                .set_opr_pushes
                .pop()
                .expect("set-operation scope entries are balanced")
        {
            self.scopes.pop();
        }
    }

    fn resolve(&self, table: &TableRef) -> Option<ResolvedBinding> {
        let [name] = table.name.as_slice() else {
            // Go's preprocess.handleTableName only tries a CTE when the
            // schema is empty; `db.c` is always a base relation.
            return None;
        };
        for scope in self.scopes.iter().rev() {
            for binding in scope.visible_bindings().iter().rev() {
                if binding.name.eq_ignore_ascii_case(name) {
                    return Some(match binding.target {
                        Some(index) => ResolvedBinding::Target(index),
                        None => ResolvedBinding::Shadow,
                    });
                }
            }
        }
        None
    }
}

struct ConsumerCounter<'a> {
    scopes: QueryScopes,
    counts: &'a mut [usize],
}

impl Visitor for ConsumerCounter<'_> {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        self.scopes.enter(node);
        if let Some(table) = node.downcast_mut::<TableRef>() {
            if let Some(ResolvedBinding::Target(index)) = self.scopes.resolve(table) {
                // Equality with one is all computeCTEInlineFlag needs.
                self.counts[index] = self.counts[index].saturating_add(1);
            }
        }
        false
    }

    fn leave(&mut self, node: &mut dyn Any) -> bool {
        self.scopes.leave(node);
        true
    }
}

#[derive(Clone)]
struct InlineDefinition {
    name: String,
    columns: Vec<String>,
    query: tidb_ast::NodeBox<QueryStmt>,
}

struct InlineRewriter<'a> {
    scopes: QueryScopes,
    definitions: &'a [Option<InlineDefinition>],
}

impl Visitor for InlineRewriter<'_> {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        self.scopes.enter(node);
        let Some(join_node) = node.downcast_mut::<JoinNode>() else {
            return false;
        };
        let JoinNode::Table(table) = join_node else {
            return false;
        };
        let Some(ResolvedBinding::Target(index)) = self.scopes.resolve(table) else {
            return false;
        };
        let Some(definition) = &self.definitions[index] else {
            return false;
        };

        // Go's TableSource alias around buildDataSourceFromCTEMerge wins over
        // the CTE name. Its explicit CTE column list renames positionally.
        let alias = table
            .alias
            .as_deref()
            .filter(|alias| !alias.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| definition.name.clone());
        *join_node = JoinNode::Derived {
            subquery: definition.query.clone(),
            alias: Some(alias),
            lateral: false,
            column_names: definition.columns.clone(),
        };

        // The definition was already expanded in its declaration scope. Do
        // not revisit its clone in the consumer's lexical scope.
        true
    }

    fn leave(&mut self, node: &mut dyn Any) -> bool {
        self.scopes.leave(node);
        true
    }
}

/// Go resolves a base table before buildDataSourceFromCTEMerge moves the
/// resulting plan. An AST clone has no resolved table identity, so qualify an
/// unbound declaration-time table with the current database before moving it;
/// otherwise a remaining CTE or nested WITH at the consumer could capture it.
struct BaseTableQualifier<'a> {
    scopes: QueryScopes,
    current_db: &'a str,
}

impl Visitor for BaseTableQualifier<'_> {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        self.scopes.enter(node);
        let Some(table) = node.downcast_mut::<TableRef>() else {
            return false;
        };
        if !self.current_db.is_empty()
            && table.name.len() == 1
            && self.scopes.resolve(table).is_none()
        {
            table.name.insert(0, self.current_db.to_owned());
        }
        false
    }

    fn leave(&mut self, node: &mut dyn Any) -> bool {
        self.scopes.leave(node);
        true
    }
}

fn count_consumers(query: &QueryStmt, with: &WithClause) -> Vec<usize> {
    let mut counts = vec![0usize; with.ctes.len()];
    for (index, cte) in with.ctes.iter().enumerate() {
        let mut body = cte.query.clone();
        body.accept(&mut ConsumerCounter {
            scopes: QueryScopes::with_outer(with, ScopePhase::Definition(index)),
            counts: &mut counts,
        });
    }

    let mut main = query.clone();
    main.accept(&mut ConsumerCounter {
        scopes: QueryScopes::with_outer(with, ScopePhase::Body),
        counts: &mut counts,
    });
    counts
}

fn rewrite_references(
    query: &mut impl Visitable,
    with: &WithClause,
    phase: ScopePhase,
    definitions: &[Option<InlineDefinition>],
) {
    query.accept(&mut InlineRewriter {
        scopes: QueryScopes::with_outer(with, phase),
        definitions,
    });
}

fn qualify_base_tables(
    query: &mut impl Visitable,
    with: &WithClause,
    phase: ScopePhase,
    current_db: &str,
) {
    query.accept(&mut BaseTableQualifier {
        scopes: QueryScopes::with_outer(with, phase),
        current_db,
    });
}

fn take_with(query: &mut QueryStmt) -> Option<WithClause> {
    match query {
        QueryStmt::Select(select) => select.with.take(),
        QueryStmt::SetOpr(set_opr) => set_opr.with.take(),
    }
}

fn put_with(query: &mut QueryStmt, with: WithClause) {
    match query {
        QueryStmt::Select(select) => select.with = Some(with),
        QueryStmt::SetOpr(set_opr) => set_opr.with = Some(with),
    }
}

fn inline_query(query: &QueryStmt, current_db: &str) -> Option<QueryStmt> {
    let mut rewritten = query.clone();
    let with = take_with(&mut rewritten)?;

    // This AST records RECURSIVE at clause granularity. The required Go
    // parity rule keeps the complete recursive clause on materialization.
    if with.recursive {
        return None;
    }

    let counts = count_consumers(&rewritten, &with);
    let inline: Vec<bool> = counts.iter().map(|count| *count == 1).collect();
    if !inline.iter().any(|inline| *inline) {
        return None;
    }

    // Expand declarations left-to-right. A later declaration may consume an
    // earlier inlined one; a materialized declaration retains that rewritten
    // body after the consumed declaration is removed from WITH.
    let mut ctes = with.ctes.clone();
    let mut definitions: Vec<Option<InlineDefinition>> = vec![None; ctes.len()];
    for index in 0..ctes.len() {
        rewrite_references(
            &mut ctes[index].query,
            &with,
            ScopePhase::Definition(index),
            &definitions,
        );
        if inline[index] {
            qualify_base_tables(
                &mut ctes[index].query,
                &with,
                ScopePhase::Definition(index),
                current_db,
            );
            definitions[index] = Some(InlineDefinition {
                name: ctes[index].name.clone(),
                columns: ctes[index].columns.clone(),
                query: ctes[index].query.clone(),
            });
        }
    }

    rewrite_references(&mut rewritten, &with, ScopePhase::Body, &definitions);

    let remaining: Vec<Cte> = ctes
        .into_iter()
        .enumerate()
        .filter_map(|(index, cte)| (!inline[index]).then_some(cte))
        .collect();
    if !remaining.is_empty() {
        put_with(
            &mut rewritten,
            WithClause {
                recursive: false,
                ctes: remaining,
            },
        );
    }
    Some(rewritten)
}

pub(crate) fn inline_select(select: &SelectStmt, current_db: &str) -> Option<SelectStmt> {
    let query = QueryStmt::Select(Box::new(select.clone()));
    match inline_query(&query, current_db)? {
        QueryStmt::Select(select) => Some(*select),
        QueryStmt::SetOpr(_) => unreachable!("a SELECT rewrite preserves its query shape"),
    }
}

pub(crate) fn inline_set_opr(set_opr: &SetOprStmt, current_db: &str) -> Option<SetOprStmt> {
    let query = QueryStmt::SetOpr(Box::new(set_opr.clone()));
    match inline_query(&query, current_db)? {
        QueryStmt::SetOpr(set_opr) => Some(*set_opr),
        QueryStmt::Select(_) => unreachable!("a set operation rewrite preserves its query shape"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::Stmt;

    fn select(sql: &str) -> SelectStmt {
        let Stmt::Query(query) = tidb_parser::parse(sql).expect("query parses") else {
            panic!("expected query")
        };
        let QueryStmt::Select(select) = query.into_inner() else {
            panic!("expected SELECT")
        };
        *select
    }

    #[test]
    fn one_consumer_inlines_but_zero_or_two_materialize() {
        let one = inline_select(
            &select("WITH c AS (SELECT * FROM t) SELECT * FROM c"),
            "test",
        )
        .expect("one consumer is inlined");
        assert!(one.with.is_none());
        assert!(matches!(
            one.from.expect("FROM").left,
            JoinNode::Derived { ref alias, .. } if alias.as_deref() == Some("c")
        ));

        assert!(inline_select(&select("WITH c AS (SELECT * FROM t) SELECT 1"), "test").is_none());
        assert!(inline_select(
            &select(
                "WITH c AS (SELECT * FROM t) \
                 SELECT * FROM c AS x JOIN c AS y ON x.a = y.a",
            ),
            "test",
        )
        .is_none());
    }

    #[test]
    fn dependencies_expand_in_definition_scope_and_keep_names() {
        let rewritten = inline_select(
            &select(
                "WITH a (x) AS (SELECT a FROM t), b AS (SELECT x FROM a) \
                 SELECT q.x FROM b AS q",
            ),
            "test",
        )
        .expect("both single-use CTEs inline");
        assert!(rewritten.with.is_none());
        let JoinNode::Derived {
            subquery, alias, ..
        } = rewritten.from.expect("FROM").left
        else {
            panic!("b becomes a derived table")
        };
        assert_eq!(alias.as_deref(), Some("q"));
        let QueryStmt::Select(b) = &*subquery else {
            panic!("b has a SELECT body")
        };
        assert!(matches!(
            b.from.as_ref().expect("b FROM").left,
            JoinNode::Derived { ref alias, ref column_names, .. }
                if alias.as_deref() == Some("a") && column_names == &["x"]
        ));
    }

    #[test]
    fn nested_with_shadows_outer_names_and_qualified_names_are_base_tables() {
        let rewritten = inline_select(
            &select(
                "WITH c AS (SELECT * FROM t), \
                 d AS (WITH c AS (SELECT 9 AS a) SELECT * FROM c) SELECT * FROM d",
            ),
            "test",
        )
        .expect("d is inlined");
        let remaining = rewritten.with.expect("unused outer c remains materialized");
        assert_eq!(remaining.ctes.len(), 1);
        assert!(remaining.ctes[0].name.eq_ignore_ascii_case("c"));

        assert!(inline_select(
            &select("WITH c AS (SELECT * FROM t) SELECT * FROM test.c"),
            "test",
        )
        .is_none());
    }

    #[test]
    fn moved_definition_keeps_declaration_time_base_binding() {
        let rewritten = inline_select(
            &select("WITH a AS (SELECT x FROM b), b AS (SELECT 9 AS x) SELECT * FROM a"),
            "test",
        )
        .expect("a has exactly one consumer");
        let JoinNode::Derived { subquery, .. } = rewritten.from.expect("FROM").left else {
            panic!("a becomes a derived table")
        };
        let QueryStmt::Select(a) = &*subquery else {
            panic!("a has a SELECT body")
        };
        let JoinNode::Table(table) = &a.from.as_ref().expect("a FROM").left else {
            panic!("a still reads a table")
        };
        assert_eq!(table.name, ["test", "b"]);
    }
}
