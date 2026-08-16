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

//! SEED of Go `pkg/util/hint`, covering `hint_processor.go`'s visitor half:
//! [`HintsSet`] with its collect/bind processors, the statement-node hint
//! extraction and containment checks, the insert-statement duplicate-hint
//! warning, and `nodeType4Stmt`.
//!
//! This walks the real Rust AST: hints live on
//! `SelectStmt`/`UpdateStmt`/`DeleteStmt`/`InsertStmt` and index hints on
//! `TableRef`, and the processors ride `tidb_ast::Visitable` exactly as Go's
//! ride `Accept`. Still open from `hint_processor.go`: `ParseHintsSet` (the
//! parser round-trip), the `Restore*` helpers (they need the AST's per-hint
//! restore exposed), and `CheckBindingFromHistoryComplete`.

use std::any::Any;

use tidb_ast::{
    DeleteStmt, Hint, HintKind, IndexHint, InsertStmt, QueryStmt, SelectStmt, SetOprTermBody,
    UpdateStmt, Visitable, Visitor,
};

/// Go's private `supportedHintNameForInsertStmt`, filled by `init`.
const SUPPORTED_HINTS_FOR_INSERT: [&str; 1] = ["memory_quota"];

/// Go's private `isStmtHint`: statement-level hints survive from every
/// block, not only the first.
fn is_stmt_hint(hint: &Hint) -> bool {
    matches!(
        hint.name.to_lowercase().as_str(),
        "max_execution_time" | "memory_quota" | "resource_group"
    )
}

/// Go `HintsSet`: the hints of one statement, block by block.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HintsSet {
    /// Go `tableHints`: one entry per `SELECT`/`UPDATE`/`DELETE` block, in
    /// traversal order.
    pub table_hints: Vec<Vec<Hint>>,
    /// Go `indexHints`: one entry per table reference, in traversal order.
    pub index_hints: Vec<Vec<IndexHint>>,
}

impl HintsSet {
    /// Go `GetStmtHints`: the first block's hints whole (the source keeps
    /// prior behavior there), then only statement-level hints from the rest.
    #[must_use]
    pub fn get_stmt_hints(&self) -> Vec<Hint> {
        let mut result = Vec::new();
        if let Some(first) = self.table_hints.first() {
            result.extend(first.iter().cloned());
        }
        for block in self.table_hints.iter().skip(1) {
            for hint in block {
                if is_stmt_hint(hint) {
                    result.push(hint.clone());
                }
            }
        }
        result
    }

    /// Go `ContainTableHint`, comparing the display name as the source's
    /// `HintName.String()` does.
    #[must_use]
    pub fn contain_table_hint(&self, hint_name: &str) -> bool {
        self.table_hints
            .iter()
            .flatten()
            .any(|hint| hint.name == hint_name)
    }
}

/// Go's private `containTableHint`, the lowercase-name form.
fn contain_table_hint(hints: &[Hint], hint_name: &str) -> bool {
    hints
        .iter()
        .any(|hint| hint.name.to_lowercase() == hint_name)
}

/// Go `ExtractTableHintsFromStmtNode` for a query (`SELECT` or set
/// operation).
fn extract_from_query(query: &QueryStmt) -> Vec<Hint> {
    match query {
        QueryStmt::Select(select) => select.hints.clone(),
        QueryStmt::SetOpr(set_opr) => {
            let mut result = Vec::new();
            for term in &set_opr.terms {
                if let SetOprTermBody::Select(select) = &term.body {
                    result.extend(extract_from_query(&QueryStmt::Select(select.clone())));
                }
            }
            result
        }
    }
}

/// Go `ExtractTableHintsFromStmtNode` for an `INSERT`: its own hints, the
/// duplicate check, and the statement-level hints of its `SELECT` source.
pub fn extract_table_hints_from_insert(
    insert: &InsertStmt,
    warnings: &mut Vec<String>,
) -> Vec<Hint> {
    check_insert_stmt_hint_duplicated(insert, warnings);
    let mut result = insert.hints.clone();
    if let Some(source) = &insert.source {
        for hint in extract_from_query(source) {
            if is_stmt_hint(&hint) {
                result.push(hint);
            }
        }
    }
    result
}

/// Go `ContainTableHintInStmtNode` for an `INSERT`.
#[must_use]
pub fn insert_contains_table_hint(insert: &InsertStmt, hint_name: &str) -> bool {
    if contain_table_hint(&insert.hints, hint_name) {
        return true;
    }
    let Some(source) = &insert.source else {
        return false;
    };
    contain_table_hint(&extract_from_query(source), hint_name)
}

/// Go's private `checkInsertStmtHintDuplicated`: when the insert carries a
/// supported hint and its `SELECT` source repeats the name, the repeat warns
/// with `ErrWarnConflictingHint` (3126).
fn check_insert_stmt_hint_duplicated(insert: &InsertStmt, warnings: &mut Vec<String>) {
    if insert.hints.is_empty() {
        return;
    }
    let supported = insert
        .hints
        .iter()
        .find(|hint| SUPPORTED_HINTS_FOR_INSERT.contains(&hint.name.to_lowercase().as_str()));
    let Some(supported) = supported else { return };
    let Some(source) = &insert.source else { return };
    let duplicated = extract_from_query(source)
        .into_iter()
        .find(|hint| hint.name.to_lowercase() == supported.name.to_lowercase());
    if let Some(duplicated) = duplicated {
        // Go renders `%s(`%v`)` with the hint's data; MEMORY_QUOTA's data is
        // its byte count.
        let data = match &duplicated.kind {
            HintKind::MemoryQuota { bytes, .. } => bytes.to_string(),
            _ => String::new(),
        };
        warnings.push(format!(
            "Hint {}(`{data}`) is ignored as conflicting/duplicated.",
            duplicated.name
        ));
    }
}

/// Go `NodeType`, reused from the query-block seed.
pub use crate::qb_hint::NodeType;

/// Go's private `nodeType4Stmt` over the statement shapes that carry hints.
/// SQL bind only handles `INSERT INTO SELECT`, so an insert is a select.
#[must_use]
pub fn node_type_for_stmt(node: &dyn Any) -> NodeType {
    if node.is::<SelectStmt>() || node.is::<InsertStmt>() {
        NodeType::Select
    } else if node.is::<UpdateStmt>() {
        NodeType::Update
    } else if node.is::<DeleteStmt>() {
        NodeType::Delete
    } else {
        NodeType::Invalid
    }
}

/// Go's private `hintProcessor`: one visitor for both directions.
struct HintProcessor {
    set: HintsSet,
    bind_hint_to_ast: bool,
    table_counter: usize,
    index_counter: usize,
    block_counter: i64,
}

impl HintProcessor {
    fn on_block_hints(&mut self, hints: &mut Vec<Hint>) {
        if self.bind_hint_to_ast {
            *hints = self
                .set
                .table_hints
                .get(self.table_counter)
                .cloned()
                .unwrap_or_default();
            self.table_counter += 1;
        } else {
            self.set.table_hints.push(hints.clone());
        }
        self.block_counter += 1;
    }
}

impl Visitor for HintProcessor {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        if let Some(select) = node.downcast_mut::<SelectStmt>() {
            self.on_block_hints(&mut select.hints);
        } else if let Some(update) = node.downcast_mut::<UpdateStmt>() {
            self.on_block_hints(&mut update.hints);
        } else if let Some(delete) = node.downcast_mut::<DeleteStmt>() {
            self.on_block_hints(&mut delete.hints);
        } else if let Some(table) = node.downcast_mut::<tidb_ast::TableRef>() {
            // Insert-target tables come before any block; skip them, as the
            // source's blockCounter guard does.
            if self.block_counter == 0 {
                return false;
            }
            if self.bind_hint_to_ast {
                table.hints = self
                    .set
                    .index_hints
                    .get(self.index_counter)
                    .cloned()
                    .unwrap_or_default();
                self.index_counter += 1;
            } else {
                self.set.index_hints.push(table.hints.clone());
            }
        }
        false
    }

    fn leave(&mut self, node: &mut dyn Any) -> bool {
        if node.is::<SelectStmt>() || node.is::<UpdateStmt>() || node.is::<DeleteStmt>() {
            self.block_counter -= 1;
        }
        true
    }
}

/// Go `CollectHint`.
pub fn collect_hint<N: Visitable>(node: &mut N) -> HintsSet {
    let mut processor = HintProcessor {
        set: HintsSet::default(),
        bind_hint_to_ast: false,
        table_counter: 0,
        index_counter: 0,
        block_counter: 0,
    };
    node.accept(&mut processor);
    processor.set
}

/// Go `BindHint`: writes `hints_set`'s hints back onto the statement in the
/// same traversal order; blocks beyond the set's length are cleared.
pub fn bind_hint<N: Visitable>(node: &mut N, hints_set: &HintsSet) {
    let mut processor = HintProcessor {
        set: hints_set.clone(),
        bind_hint_to_ast: true,
        table_counter: 0,
        index_counter: 0,
        block_counter: 0,
    };
    node.accept(&mut processor);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_parser::parse;

    fn select(sql: &str) -> tidb_ast::Stmt {
        parse(sql).expect("parse")
    }

    fn hint_names(hints: &[Hint]) -> Vec<String> {
        hints.iter().map(|hint| hint.name.clone()).collect()
    }

    // Collect gathers per-block table hints and per-table index hints in
    // traversal order; bind writes them back.
    #[test]
    fn collect_and_bind_round_trip() {
        let mut stmt = select(
            "SELECT /*+ HASH_JOIN(t1) */ * FROM t1 USE INDEX (idx_a) \
             WHERE a IN (SELECT /*+ MERGE_JOIN(t2) */ b FROM t2)",
        );
        let set = collect_hint(&mut stmt);
        assert_eq!(set.table_hints.len(), 2);
        assert_eq!(hint_names(&set.table_hints[0]), vec!["HASH_JOIN"]);
        assert_eq!(hint_names(&set.table_hints[1]), vec!["MERGE_JOIN"]);
        assert_eq!(set.index_hints.len(), 2);
        assert_eq!(set.index_hints[0].len(), 1);
        assert!(set.index_hints[1].is_empty());
        assert!(set.contain_table_hint("HASH_JOIN"));
        assert!(!set.contain_table_hint("hash_join"));

        // Binding onto a hint-less statement with the same shape installs
        // the collected hints.
        let mut bare = select("SELECT * FROM t1 WHERE a IN (SELECT b FROM t2)");
        bind_hint(&mut bare, &set);
        let rebound = collect_hint(&mut bare);
        assert_eq!(rebound.table_hints, set.table_hints);
        assert_eq!(rebound.index_hints, set.index_hints);

        // Binding an EMPTY set clears existing hints.
        let mut hinted = select("SELECT /*+ HASH_JOIN(t1) */ * FROM t1");
        bind_hint(&mut hinted, &HintsSet::default());
        let cleared = collect_hint(&mut hinted);
        assert!(cleared.table_hints[0].is_empty());
    }

    // Go `GetStmtHints`: the first block passes whole; later blocks only
    // contribute statement-level hints.
    #[test]
    fn stmt_hints_keep_the_first_block_whole() {
        let mut stmt = select(
            "SELECT /*+ HASH_JOIN(t1) */ * FROM t1 \
             WHERE a IN (SELECT /*+ MERGE_JOIN(t2), MAX_EXECUTION_TIME(100) */ b FROM t2)",
        );
        let set = collect_hint(&mut stmt);
        let stmt_hints = hint_names(&set.get_stmt_hints());
        assert_eq!(stmt_hints, vec!["HASH_JOIN", "MAX_EXECUTION_TIME"]);
    }

    // Go `nodeType4Stmt` over the hint-bearing statement shapes.
    #[test]
    fn node_types_classify_for_sql_bind() {
        let mut stmt = select("SELECT 1");
        let tidb_ast::Stmt::Query(query) = &mut stmt else {
            panic!("expected a query");
        };
        let QueryStmt::Select(select_stmt) = &**query else {
            panic!("expected a select");
        };
        assert_eq!(
            node_type_for_stmt(select_stmt.as_ref() as &dyn Any),
            NodeType::Select
        );
        assert_eq!(node_type_for_stmt(&1_i64), NodeType::Invalid);
    }
}
