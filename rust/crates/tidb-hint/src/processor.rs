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

use std::any::Any;
use std::collections::HashSet;

use tidb_ast::{
    DmlStmt, Expr, Hint, HintKind, HintTable, IndexHint, IndexHintKind, IndexHintScope,
    LeadingElement, QueryStmt, SetOprStmt, SetOprTermBody, Stmt, TableRef, Visitable as _, Visitor,
};

use crate::{generate_qb_name, HintWarning, NodeType, QBHintHandler};

/// Go `HintsSet`: table optimizer hints and index hints in AST traversal
/// order. The representation is intentionally opaque outside this package.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HintsSet {
    table_hints: Vec<Vec<Hint>>,
    index_hints: Vec<Vec<IndexHint>>,
}

impl HintsSet {
    /// Go `HintsSet.GetStmtHints`.
    #[must_use]
    pub fn stmt_hints(&self) -> Vec<&Hint> {
        let mut result = Vec::new();
        if let Some(first) = self.table_hints.first() {
            result.extend(first);
        }
        for block in self.table_hints.iter().skip(1) {
            result.extend(block.iter().filter(|hint| is_stmt_hint(hint)));
        }
        result
    }

    /// Go `HintsSet.ContainTableHint`.
    #[must_use]
    pub fn contains_table_hint(&self, name: &str) -> bool {
        self.table_hints
            .iter()
            .flatten()
            .any(|hint| hint.name.eq_ignore_ascii_case(name))
    }

    /// Go `HintsSet.Restore`.
    #[must_use]
    pub fn restore(&self) -> String {
        let mut restored = Vec::new();
        restored.extend(
            self.table_hints
                .iter()
                .flatten()
                .map(restore_table_optimizer_hint),
        );
        restored.extend(self.index_hints.iter().flatten().map(restore_index_hint));
        restored.join(", ")
    }
}

fn is_stmt_hint(hint: &Hint) -> bool {
    matches!(
        hint.name.to_ascii_lowercase().as_str(),
        "max_execution_time" | "memory_quota" | "resource_group"
    )
}

struct HintProcessor {
    hints: HintsSet,
    bind_hint_to_ast: bool,
    table_counter: usize,
    index_counter: usize,
    block_counter: usize,
}

impl HintProcessor {
    fn visit_table_hints(&mut self, target: &mut Vec<Hint>) {
        if self.bind_hint_to_ast {
            *target = self
                .hints
                .table_hints
                .get(self.table_counter)
                .cloned()
                .unwrap_or_default();
            self.table_counter += 1;
        } else {
            self.hints.table_hints.push(target.clone());
        }
        self.block_counter += 1;
    }
}

impl Visitor for HintProcessor {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        if let Some(select) = node.downcast_mut::<tidb_ast::SelectStmt>() {
            self.visit_table_hints(&mut select.hints);
            return false;
        }
        if let Some(update) = node.downcast_mut::<tidb_ast::UpdateStmt>() {
            self.visit_table_hints(&mut update.hints);
            return false;
        }
        if let Some(delete) = node.downcast_mut::<tidb_ast::DeleteStmt>() {
            self.visit_table_hints(&mut delete.hints);
            return false;
        }
        if let Some(table) = node.downcast_mut::<tidb_ast::TableRef>() {
            // The target of INSERT is visited before any SELECT/UPDATE/DELETE
            // block and therefore does not consume an index-hint slot.
            if self.block_counter == 0 {
                return false;
            }
            if self.bind_hint_to_ast {
                table.hints = self
                    .hints
                    .index_hints
                    .get(self.index_counter)
                    .cloned()
                    .unwrap_or_default();
                self.index_counter += 1;
            } else {
                self.hints.index_hints.push(table.hints.clone());
            }
        }
        false
    }

    fn leave(&mut self, node: &mut dyn Any) -> bool {
        if node.is::<tidb_ast::SelectStmt>()
            || node.is::<tidb_ast::UpdateStmt>()
            || node.is::<tidb_ast::DeleteStmt>()
        {
            self.block_counter -= 1;
        }
        true
    }
}

/// Go `CollectHint`.
#[must_use]
pub fn collect_hint(statement: &Stmt) -> HintsSet {
    let mut statement = statement.clone();
    let mut processor = HintProcessor {
        hints: HintsSet::default(),
        bind_hint_to_ast: false,
        table_counter: 0,
        index_counter: 0,
        block_counter: 0,
    };
    statement.accept(&mut processor);
    processor.hints
}

/// Go `BindHint`.
pub fn bind_hint(statement: &mut Stmt, hints: &HintsSet) {
    let mut processor = HintProcessor {
        hints: hints.clone(),
        bind_hint_to_ast: true,
        table_counter: 0,
        index_counter: 0,
        block_counter: 0,
    };
    statement.accept(&mut processor);
}

/// Go `ExtractTableHintsFromStmtNode`.
#[must_use]
pub fn extract_table_hints_from_stmt_node(statement: &Stmt) -> (Vec<Hint>, Vec<HintWarning>) {
    let mut warnings = Vec::new();
    let hints = extract_stmt_hints(statement, Some(&mut warnings));
    (hints, warnings)
}

fn extract_stmt_hints(statement: &Stmt, warnings: Option<&mut Vec<HintWarning>>) -> Vec<Hint> {
    match statement {
        Stmt::Query(query) => extract_query_hints(query),
        Stmt::Dml(dml) => extract_dml_hints(dml, warnings),
        _ => Vec::new(),
    }
}

fn extract_query_hints(query: &QueryStmt) -> Vec<Hint> {
    match query {
        QueryStmt::Select(select) => select.hints.clone(),
        QueryStmt::SetOpr(set_operation) => extract_set_operation_hints(set_operation),
    }
}

fn extract_set_operation_hints(set_operation: &SetOprStmt) -> Vec<Hint> {
    let mut result = Vec::new();
    for term in &set_operation.terms {
        match &term.body {
            SetOprTermBody::Select(select) => result.extend(select.hints.clone()),
            SetOprTermBody::Nested(nested) => {
                result.extend(extract_set_operation_hints(nested));
            }
        }
    }
    result
}

fn extract_dml_hints(
    statement: &DmlStmt,
    mut warnings: Option<&mut Vec<HintWarning>>,
) -> Vec<Hint> {
    match statement {
        DmlStmt::With { statement, .. } => extract_dml_hints(statement, warnings),
        DmlStmt::Update(update) => update.hints.clone(),
        DmlStmt::Delete(delete) => delete.hints.clone(),
        DmlStmt::Insert(insert) => {
            let mut result = insert.hints.clone();
            let source_hints = insert
                .source
                .as_deref()
                .map(extract_query_hints)
                .unwrap_or_default();
            if let Some(outer) = insert
                .hints
                .iter()
                .find(|hint| hint.name.eq_ignore_ascii_case("memory_quota"))
            {
                if let Some(inner) = source_hints
                    .iter()
                    .find(|hint| hint.name.eq_ignore_ascii_case(&outer.name))
                {
                    if let Some(warnings) = warnings.as_mut() {
                        warnings.push(HintWarning::conflicting(format!(
                            "{}(`{}`)",
                            inner.name,
                            hint_scalar_value(inner)
                        )));
                    }
                }
            }
            result.extend(source_hints.into_iter().filter(is_stmt_hint));
            result
        }
        _ => Vec::new(),
    }
}

fn hint_scalar_value(hint: &Hint) -> String {
    match &hint.kind {
        HintKind::MemoryQuota { bytes, .. } | HintKind::Number { value: bytes, .. } => {
            bytes.to_string()
        }
        HintKind::Name { name, .. } => name.clone(),
        HintKind::Bool { value, .. } => value.to_string(),
        _ => String::new(),
    }
}

/// Go `ContainTableHintInStmtNode`.
#[must_use]
pub fn contain_table_hint_in_stmt_node(statement: &Stmt, hint_name: &str) -> bool {
    extract_stmt_hints(statement, None)
        .iter()
        .any(|hint| hint.name.eq_ignore_ascii_case(hint_name))
}

/// Go `RestoreIndexHint`.
#[must_use]
pub fn restore_index_hint(hint: &IndexHint) -> String {
    let mut value = match hint.kind {
        IndexHintKind::Use => "use index".to_owned(),
        IndexHintKind::Force => "force index".to_owned(),
        IndexHintKind::Ignore => "ignore index".to_owned(),
    };
    value.push_str(match hint.scope {
        IndexHintScope::All => "",
        IndexHintScope::Join => " for join",
        IndexHintScope::OrderBy => " for order by",
        IndexHintScope::GroupBy => " for group by",
    });
    value.push_str(" (");
    value.push_str(
        &hint
            .indexes
            .iter()
            .map(|name| format!("`{}`", name.replace('`', "``")))
            .collect::<Vec<_>>()
            .join(", "),
    );
    value.push(')');
    value.to_ascii_lowercase()
}

/// Go `nodeType4Stmt`.
#[must_use]
pub fn node_type_for_stmt(statement: &Stmt) -> NodeType {
    match statement {
        Stmt::Query(_) => NodeType::Select,
        Stmt::Dml(dml) => match unwrap_dml(dml) {
            DmlStmt::Insert(_) => NodeType::Select,
            DmlStmt::Update(_) => NodeType::Update,
            DmlStmt::Delete(_) => NodeType::Delete,
            _ => NodeType::Invalid,
        },
        _ => NodeType::Invalid,
    }
}

/// Go `ParseHintsSet`.
pub fn parse_hints_set(
    sql: &str,
    charset: &str,
    collation: &str,
    database: &str,
) -> Result<(HintsSet, Stmt, Vec<String>), String> {
    let (mut statements, warnings) =
        tidb_parser::parse_multi_with_connection_and_warnings(sql, charset, collation)
            .map_err(|error| error.compatibility_message(sql))?;
    if statements.len() != 1 {
        return Err(format!("bind_sql must be a single statement: {sql}"));
    }
    let mut statement = statements.remove(0);
    let mut hints = collect_hint(&statement);
    let query_blocks = QBHintHandler::build(&mut statement);
    let top_node_type = node_type_for_stmt(&statement);
    for (block_offset, block_hints) in hints.table_hints.iter_mut().enumerate() {
        let mut normalized = Vec::with_capacity(block_hints.len());
        let mut current_offset = block_offset as i32 + 1;
        if matches!(top_node_type, NodeType::Delete | NodeType::Update) {
            current_offset -= 1;
        }
        for mut hint in std::mem::take(block_hints) {
            if let HintKind::QbName { views, .. } = &hint.kind {
                if !views.is_empty() {
                    normalized.push(hint);
                }
                continue;
            }
            if query_blocks.is_hint_for_view(&hint) {
                normalized.push(hint);
                continue;
            }
            let offset = query_blocks.hint_offset(hint_qb_name(&hint), current_offset);
            let table_refs = crate::query_block::hint_tables(&hint);
            if offset < 0 || !query_blocks.tables_have_valid_qb_names(&table_refs) {
                return Err(format!(
                    "Unknown query block name in hint {}",
                    restore_table_optimizer_hint(&hint)
                ));
            }
            let qb_name = generate_qb_name(top_node_type, offset)?;
            set_hint_qb_name(&mut hint, qb_name);
            fill_default_database(&mut hint, database);
            normalized.push(hint);
        }
        *block_hints = normalized;
    }
    Ok((
        hints,
        statement,
        warnings
            .into_iter()
            .find(|warning| is_hint_parse_warning(&warning.message))
            .map(|warning| vec![warning.message])
            .unwrap_or_default(),
    ))
}

// Go `extractHintWarns`: binding creation keeps only parser errors and the
// six optimizer-hint diagnostic classes, and returns at most the first one.
// Rust's parser retains their source-compatible text rather than an error
// type, so the bracketed parser code is the authoritative discriminator. The
// standalone hint parser's syntax and numeric-token diagnostics currently
// carry the same Go text without the bracketed prefix and are included by
// their exact source messages.
fn is_hint_parse_warning(message: &str) -> bool {
    const HINT_WARNING_CODES: [&str; 7] = [
        "[parser:1064]",
        "[parser:8061]",
        "[parser:8062]",
        "[parser:8063]",
        "[parser:8064]",
        "[parser:8065]",
        "[parser:8066]",
    ];
    HINT_WARNING_CODES
        .iter()
        .any(|code| message.starts_with(code))
        || message.starts_with("Optimizer hint syntax error at line ")
        || matches!(
            message,
            "Cannot use decimal number"
                | "Cannot use bit-value literal"
                | "Cannot use hexadecimal literal"
                | "integer value is out of range"
        )
}

fn hint_qb_name(hint: &Hint) -> Option<&str> {
    match &hint.kind {
        HintKind::Nullary { qb_name }
        | HintKind::Tables { qb_name, .. }
        | HintKind::Leading { qb_name, .. }
        | HintKind::Index { qb_name, .. }
        | HintKind::Bool { qb_name, .. }
        | HintKind::Name { qb_name, .. }
        | HintKind::Keyword { qb_name, .. }
        | HintKind::MemoryQuota { qb_name, .. }
        | HintKind::Number { qb_name, .. }
        | HintKind::ReadFromStorage { qb_name, .. } => qb_name.as_deref(),
        HintKind::SetVar { .. } | HintKind::TimeRange { .. } | HintKind::QbName { .. } => None,
    }
}

fn set_hint_qb_name(hint: &mut Hint, value: String) {
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
        | HintKind::ReadFromStorage { qb_name, .. } => *qb_name = Some(value),
        HintKind::SetVar { .. } | HintKind::TimeRange { .. } | HintKind::QbName { .. } => {}
    }
}

fn fill_default_database(hint: &mut Hint, database: &str) {
    match &mut hint.kind {
        HintKind::Tables { tables, .. } => fill_tables_database(tables, database),
        HintKind::Leading { elements, .. } => fill_leading_database(elements, database),
        HintKind::Index { table, .. } => fill_table_database(table, database),
        HintKind::QbName { views, .. } => fill_tables_database(views, database),
        HintKind::ReadFromStorage { groups, .. } => {
            for (_, tables) in groups {
                fill_tables_database(tables, database);
            }
        }
        _ => {}
    }
}

fn fill_tables_database(tables: &mut [HintTable], database: &str) {
    for table in tables {
        fill_table_database(table, database);
    }
}

fn fill_leading_database(elements: &mut [LeadingElement], database: &str) {
    for element in elements {
        match element {
            LeadingElement::Table(table) => fill_table_database(table, database),
            LeadingElement::Group(group) => fill_leading_database(group, database),
        }
    }
}

fn fill_table_database(table: &mut HintTable, database: &str) {
    if table.db_name.as_deref().is_none_or(str::is_empty) {
        table.db_name = Some(database.to_owned());
    }
}

fn unwrap_dml(mut statement: &DmlStmt) -> &DmlStmt {
    while let DmlStmt::With {
        statement: inner, ..
    } = statement
    {
        statement = inner;
    }
    statement
}

/// Go `CheckBindingFromHistoryComplete`.
#[must_use]
pub fn check_binding_from_history_complete(statement: &Stmt, hint: &str) -> (bool, String) {
    if hint.contains("tiflash") {
        return (
            false,
            "auto-generated hint for queries accessing TiFlash might not be complete, the plan might change even after creating this binding".to_owned(),
        );
    }
    let mut statement = statement.clone();
    let mut checker = BindableChecker::default();
    statement.accept(&mut checker);
    (checker.complete, checker.reason)
}

struct BindableChecker {
    complete: bool,
    reason: String,
    tables: HashSet<String>,
}

impl Default for BindableChecker {
    fn default() -> Self {
        Self {
            complete: true,
            reason: String::new(),
            tables: HashSet::with_capacity(2),
        }
    }
}

impl Visitor for BindableChecker {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        if let Some(expression) = node.downcast_ref::<Expr>() {
            if matches!(
                expression,
                Expr::Subquery(_)
                    | Expr::Exists { .. }
                    | Expr::InSubquery { .. }
                    | Expr::CompareSubquery { .. }
            ) {
                self.complete = false;
                self.reason = "auto-generated hint for queries with sub queries might not be complete, the plan might change even after creating this binding".to_owned();
                return true;
            }
        }
        if let Some(table) = node.downcast_ref::<TableRef>() {
            let schema = table.name.first().filter(|_| table.name.len() > 1);
            if schema.is_none_or(|schema| !self.tables.contains(schema)) {
                if let Some(name) = table.name.last() {
                    self.tables.insert(name.clone());
                }
            }
            if self.tables.len() >= 3 {
                self.complete = false;
                self.reason = "auto-generated hint for queries with more than 3 table join might not be complete, the plan might change even after creating this binding".to_owned();
                return true;
            }
        }
        false
    }

    fn leave(&mut self, _node: &mut dyn Any) -> bool {
        self.complete
    }
}

/// Go `RestoreTableOptimizerHint`.
#[must_use]
pub fn restore_table_optimizer_hint(hint: &Hint) -> String {
    hint.restore().to_ascii_lowercase()
}

/// Go `RestoreOptimizerHints`, including its first-occurrence-preserving
/// duplicate removal.
#[must_use]
pub fn restore_optimizer_hints(hints: &[Hint]) -> String {
    let mut seen = HashSet::with_capacity(hints.len());
    hints
        .iter()
        .map(restore_table_optimizer_hint)
        .filter(|restored| seen.insert(restored.clone()))
        .collect::<Vec<_>>()
        .join(", ")
}
