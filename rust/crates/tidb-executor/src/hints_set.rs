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

/// Go `RestoreTableOptimizerHint`: the hint's canonical text, lowercased.
#[must_use]
pub fn restore_table_optimizer_hint(hint: &Hint) -> String {
    hint.restore().to_lowercase()
}

/// Go `RestoreIndexHint`: the index hint's canonical text, lowercased. The
/// spelling is the one `TableRef`'s own restore writes.
#[must_use]
pub fn restore_index_hint(hint: &IndexHint) -> String {
    use tidb_ast::{IndexHintKind, IndexHintScope};
    let mut out = String::new();
    out.push_str(match hint.kind {
        IndexHintKind::Use => "USE INDEX",
        IndexHintKind::Force => "FORCE INDEX",
        IndexHintKind::Ignore => "IGNORE INDEX",
    });
    out.push_str(match hint.scope {
        IndexHintScope::All => "",
        IndexHintScope::Join => " FOR JOIN",
        IndexHintScope::OrderBy => " FOR ORDER BY",
        IndexHintScope::GroupBy => " FOR GROUP BY",
    });
    out.push_str(" (");
    for (i, name) in hint.indexes.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push('`');
        out.push_str(name);
        out.push('`');
    }
    out.push(')');
    out.to_lowercase()
}

/// Go `RestoreOptimizerHints`: each hint restored once (first occurrence
/// wins), joined with `, `.
#[must_use]
pub fn restore_optimizer_hints(hints: &[Hint]) -> String {
    let mut seen = std::collections::BTreeSet::new();
    let mut restored = Vec::with_capacity(hints.len());
    for hint in hints {
        let text = restore_table_optimizer_hint(hint);
        if seen.insert(text.clone()) {
            restored.push(text);
        }
    }
    restored.join(", ")
}

impl HintsSet {
    /// Go `HintsSet.Restore`: every table hint then every index hint, in
    /// block order, joined with `, `.
    #[must_use]
    pub fn restore(&self) -> String {
        let mut restored = Vec::new();
        for block in &self.table_hints {
            for hint in block {
                restored.push(restore_table_optimizer_hint(hint));
            }
        }
        for block in &self.index_hints {
            for hint in block {
                restored.push(restore_index_hint(hint));
            }
        }
        restored.join(", ")
    }
}

/// Go's private `bindableChecker`.
struct BindableChecker {
    complete: bool,
    reason: &'static str,
    tables: std::collections::BTreeSet<String>,
}

impl Visitor for BindableChecker {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        if let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() {
            // Go stops at *ast.ExistsSubqueryExpr and *ast.SubqueryExpr; in
            // this AST the subquery node is embedded per variant, so every
            // variant that carries one is the same stop.
            if matches!(
                expr,
                tidb_ast::Expr::Exists { .. }
                    | tidb_ast::Expr::Subquery(_)
                    | tidb_ast::Expr::InSubquery { .. }
                    | tidb_ast::Expr::CompareSubquery { .. }
            ) {
                self.complete = false;
                self.reason = "auto-generated hint for queries with sub queries might not be complete, the plan might change even after creating this binding";
                return true;
            }
        } else if let Some(table) = node.downcast_mut::<tidb_ast::TableRef>() {
            // Faithful to the source's own quirk: membership is checked
            // against the SCHEMA name, but what is inserted is the TABLE
            // name.
            let (schema, name) = match table.name.as_slice() {
                [schema, name] => (schema.clone(), name.clone()),
                [name] => (String::new(), name.clone()),
                _ => (String::new(), String::new()),
            };
            if !self.tables.contains(&schema) {
                self.tables.insert(name);
            }
            if self.tables.len() >= 3 {
                self.complete = false;
                self.reason = "auto-generated hint for queries with more than 3 table join might not be complete, the plan might change even after creating this binding";
                return true;
            }
        }
        false
    }

    fn leave(&mut self, _node: &mut dyn Any) -> bool {
        self.complete
    }
}

/// Go `CheckBindingFromHistoryComplete`: whether an auto-generated binding's
/// AST and hint text are complete enough to bind.
pub fn check_binding_from_history_complete<N: Visitable>(
    node: &mut N,
    hint_str: &str,
) -> (bool, &'static str) {
    if hint_str.contains("tiflash") {
        return (
            false,
            "auto-generated hint for queries accessing TiFlash might not be complete, the plan might change even after creating this binding",
        );
    }
    let mut checker = BindableChecker {
        complete: true,
        reason: "",
        tables: std::collections::BTreeSet::new(),
    };
    node.accept(&mut checker);
    (checker.complete, checker.reason)
}

/// The hint-level `@qb_name` slot, where the variant carries one that
/// REFERENCES a block (the `QB_NAME` hint's own field DEFINES a name and is
/// not this).
fn hint_qb_name_mut(hint: &mut Hint) -> Option<&mut Option<String>> {
    match &mut hint.kind {
        HintKind::Nullary { qb_name }
        | HintKind::Tables { qb_name, .. }
        | HintKind::Leading { qb_name, .. }
        | HintKind::Index { qb_name, .. }
        | HintKind::Bool { qb_name, .. }
        | HintKind::Name { qb_name, .. }
        | HintKind::Keyword { qb_name, .. }
        | HintKind::MemoryQuota { qb_name, .. }
        | HintKind::Number { qb_name, .. }
        | HintKind::ReadFromStorage { qb_name, .. } => Some(qb_name),
        HintKind::SetVar { .. } | HintKind::TimeRange { .. } | HintKind::QbName { .. } => None,
    }
}

fn collect_leading_tables<'h>(
    elements: &'h mut [tidb_ast::LeadingElement],
    tables: &mut Vec<&'h mut tidb_ast::HintTable>,
) {
    for element in elements {
        match element {
            tidb_ast::LeadingElement::Table(table) => tables.push(table),
            tidb_ast::LeadingElement::Group(nested) => collect_leading_tables(nested, tables),
        }
    }
}

/// Go's flat `TableOptimizerHint.Tables` view: every table argument the hint
/// carries, in written order.
fn hint_tables_mut(hint: &mut Hint) -> Vec<&mut tidb_ast::HintTable> {
    let mut result = Vec::new();
    match &mut hint.kind {
        HintKind::Tables { tables, .. } => result.extend(tables.iter_mut()),
        HintKind::Index { table, .. } => result.push(table),
        HintKind::Leading { elements, .. } => collect_leading_tables(elements, &mut result),
        HintKind::ReadFromStorage { groups, .. } => {
            for (_, tables) in groups {
                result.extend(tables.iter_mut());
            }
        }
        HintKind::QbName { views, .. } => result.extend(views.iter_mut()),
        _ => {}
    }
    result
}

/// Go `ParseHintsSet`: parses one statement, collects its hints, and
/// normalizes every hint onto generated query-block names with default
/// databases filled in — the canonical form SQL bindings store.
///
/// Named seed boundaries: the source's charset/collation parameters ride
/// Go's parser options and the returned parser warnings ride its
/// out-of-range filter, neither of which this workspace's parser surface
/// takes yet; and view `QB_NAME` hints are carried through unmodified since
/// the view-hint registry (`handleViewHints`) is not ported — a handler
/// without view hints answers `isHint4View` false, exactly as modeled here.
pub fn parse_hints_set(sql: &str, db: &str) -> Result<(HintsSet, tidb_ast::Stmt), String> {
    let mut stmt = tidb_parser::parse(sql)
        .map_err(|error| format!("bind_sql must be a single statement: {sql}: {error:?}"))?;
    let mut hs = collect_hint(&mut stmt);

    // Drive the query-block handler as Go's Accept does: one offset per
    // SELECT block, and every bare (non-view) QB_NAME hint registered at its
    // block's offset. Go passes a nil warn handler here, so registration
    // warnings are dropped.
    let top_type = match &stmt {
        tidb_ast::Stmt::Query(_) => NodeType::Select,
        tidb_ast::Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Update(_) => NodeType::Update,
            tidb_ast::DmlStmt::Delete(_) => NodeType::Delete,
            tidb_ast::DmlStmt::Insert(_) => NodeType::Select,
            _ => NodeType::Invalid,
        },
        _ => NodeType::Invalid,
    };
    let top_is_write = matches!(top_type, NodeType::Update | NodeType::Delete);
    let select_blocks =
        hs.table_hints.len() - usize::from(top_is_write && !hs.table_hints.is_empty());
    let mut handler = crate::qb_hint::QbHintHandler::new();
    for _ in 0..select_blocks {
        handler.next_select_stmt_offset();
    }
    let mut dropped = crate::qb_hint::HintWarnCollector::default();
    for (i, block) in hs.table_hints.iter().enumerate() {
        let offset = if top_is_write { i as i64 } else { i as i64 + 1 };
        let registrations: Vec<crate::qb_hint::QbHint> = block
            .iter()
            .filter_map(|hint| match &hint.kind {
                HintKind::QbName { qb_name, views } if views.is_empty() => {
                    Some(crate::qb_hint::QbHint {
                        name: crate::qb_hint::HINT_QB_NAME.to_owned(),
                        qb_name: qb_name.to_lowercase(),
                        tables: Vec::new(),
                        restored: String::new(),
                    })
                }
                _ => None,
            })
            .collect();
        handler.check_query_block_hints(&registrations, offset, &mut dropped);
    }

    // Go's normalization loop.
    for (i, block) in hs.table_hints.iter_mut().enumerate() {
        let cur_offset = if top_is_write { i as i64 } else { i as i64 + 1 };
        let mut new_hints = Vec::with_capacity(block.len());
        for mut hint in block.drain(..) {
            if let HintKind::QbName { views, .. } = &hint.kind {
                // A bare QB_NAME defined its block and is dropped; the view
                // form is kept.
                if !views.is_empty() {
                    new_hints.push(hint);
                }
                continue;
            }
            let qb_reference = hint_qb_name_mut(&mut hint)
                .and_then(|slot| slot.clone())
                .map(|name| name.to_lowercase())
                .unwrap_or_default();
            let offset = handler.hint_offset(&qb_reference, cur_offset);
            let table_qb_names: Vec<crate::qb_hint::QbHintTable> = hint_tables_mut(&mut hint)
                .iter()
                .map(|table| crate::qb_hint::QbHintTable {
                    name: table.name.to_lowercase(),
                    qb_name: table.qb_name.as_deref().unwrap_or_default().to_lowercase(),
                })
                .collect();
            if offset < 0 || !handler.check_table_qb_name(&table_qb_names) {
                return Err(format!(
                    "Unknown query block name in hint {}",
                    restore_table_optimizer_hint(&hint)
                ));
            }
            let generated = crate::qb_hint::generate_qb_name(top_type, offset)?;
            if let Some(slot) = hint_qb_name_mut(&mut hint) {
                *slot = Some(generated);
            }
            for table in hint_tables_mut(&mut hint) {
                if table.db_name.as_deref().unwrap_or_default().is_empty() {
                    table.db_name = Some(db.to_owned());
                }
            }
            new_hints.push(hint);
        }
        *block = new_hints;
    }
    Ok((hs, stmt))
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
    // Go `RestoreOptimizerHints` deduplicates on restored text; the restore
    // family lowercases.
    #[test]
    fn restores_lowercase_and_deduplicate() {
        let mut stmt = select("SELECT /*+ HASH_JOIN(t1), HASH_JOIN(t1), MERGE_JOIN(t2) */ 1");
        let set = collect_hint(&mut stmt);
        let hints = &set.table_hints[0];
        assert_eq!(restore_table_optimizer_hint(&hints[0]), "hash_join(`t1`)");
        assert_eq!(
            restore_optimizer_hints(hints),
            "hash_join(`t1`), merge_join(`t2`)"
        );

        let mut indexed = select("SELECT * FROM t1 USE INDEX FOR ORDER BY (Idx_A)");
        let indexed_set = collect_hint(&mut indexed);
        let rendered = indexed_set.restore();
        assert_eq!(rendered, "use index for order by (`idx_a`)");
    }

    // Go `CheckBindingFromHistoryComplete`: tiflash text, subqueries, and
    // three-table joins each make a binding incomplete.
    #[test]
    fn binding_completeness_checks() {
        let mut simple = select("SELECT * FROM t1, t2 WHERE t1.a = t2.a");
        let (complete, reason) =
            check_binding_from_history_complete(&mut simple, "hash_join(`t1`)");
        assert!(complete, "{reason}");

        let (complete, reason) =
            check_binding_from_history_complete(&mut simple, "read_from_storage(tiflash[`t1`])");
        assert!(!complete);
        assert!(reason.contains("TiFlash"));

        let mut subquery = select("SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2)");
        let (complete, reason) = check_binding_from_history_complete(&mut subquery, "");
        assert!(!complete);
        assert!(reason.contains("sub queries"));

        let mut wide = select("SELECT * FROM t1, t2, t3 WHERE t1.a = t2.a AND t2.b = t3.b");
        let (complete, reason) = check_binding_from_history_complete(&mut wide, "");
        assert!(!complete);
        assert!(reason.contains("more than 3 table join"));
    }
    // Go `ParseHintsSet`: hints normalize onto generated block names with
    // default databases filled in; unknown block names fail.
    #[test]
    fn parse_hints_set_normalizes_blocks_and_databases() {
        let (set, _stmt) = parse_hints_set(
            "SELECT /*+ HASH_JOIN(t1) */ * FROM t1 \
             WHERE a IN (SELECT /*+ MERGE_JOIN(test2.t2) */ b FROM t2)",
            "bind_db",
        )
        .unwrap();
        assert_eq!(set.table_hints.len(), 2);
        let restored = set.restore();
        // Block one's hint gains @sel_1 and the default database; block
        // two's keeps its written database and gains @sel_2.
        assert!(
            restored.contains("hash_join(@`sel_1` `bind_db`.`t1`)"),
            "{restored}"
        );
        assert!(
            restored.contains("merge_join(@`sel_2` `test2`.`t2`)"),
            "{restored}"
        );

        // A registered QB_NAME resolves and the bare hint is dropped.
        let (set, _stmt) = parse_hints_set(
            "SELECT * FROM t1 WHERE a IN \
             (SELECT /*+ QB_NAME(qb2), HASH_JOIN(t2) */ b FROM t2)",
            "d",
        )
        .unwrap();
        let restored = set.restore();
        assert!(
            restored.contains("hash_join(@`sel_2` `d`.`t2`)"),
            "{restored}"
        );
        assert!(!restored.contains("qb_name"), "{restored}");

        // An unknown block name is the source's exact error.
        let error =
            parse_hints_set("SELECT /*+ HASH_JOIN(@nope t1) */ * FROM t1", "d").unwrap_err();
        assert!(
            error.starts_with("Unknown query block name in hint "),
            "{error}"
        );
    }
}
