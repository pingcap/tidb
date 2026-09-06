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
use std::collections::{HashMap, HashSet};

use tidb_ast::{Hint, HintKind, HintTable, QueryStmt, Stmt, Visitable as _, Visitor};

use crate::{restore_table_optimizer_hint, HintWarning};

const DEFAULT_UPDATE_BLOCK_NAME: &str = "upd_1";
const DEFAULT_DELETE_BLOCK_NAME: &str = "del_1";
const DEFAULT_SELECT_BLOCK_PREFIX: &str = "sel_";

/// Go `NodeType` used by SQL binding query-block normalization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeType {
    /// UPDATE statement.
    Update,
    /// DELETE statement.
    Delete,
    /// SELECT or INSERT ... SELECT statement.
    Select,
    /// Any unsupported statement kind.
    Invalid,
}

/// Go `QBHintBuildState`.
#[derive(Clone, Debug, Default)]
pub struct QBHintBuildState {
    qb_offset_to_hints: HashMap<i32, Vec<Hint>>,
    hint_identities: HashMap<i32, HashSet<usize>>,
    view_qb_name_used: Option<HashSet<String>>,
}

/// The subset of Go's `qbNameMap4View`/`viewHints` that matched one view
/// reference in the outer query block. It remains opaque so callers cannot
/// construct a query-block state that Go's matching pass could not produce.
#[derive(Clone, Debug, Default)]
pub struct ViewHintContext {
    qb_name_to_tables: HashMap<String, Vec<HintTable>>,
    qb_name_to_hints: HashMap<String, Vec<Hint>>,
}

/// Go `QBHintHandler`: AST-derived query-block and view-hint metadata.
#[derive(Clone, Debug, Default)]
pub struct QBHintHandler {
    qb_name_to_select_offset: HashMap<String, i32>,
    view_qb_name_to_table: HashMap<String, Vec<HintTable>>,
    view_qb_name_to_hints: HashMap<String, Vec<Hint>>,
    warnings: Vec<HintWarning>,
    select_stmt_offset: i32,
}

impl QBHintHandler {
    /// Go `NewQBHintHandler(nil)`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds handler metadata while applying Go's view-hint extraction to
    /// the supplied statement AST.
    pub fn build(statement: &mut Stmt) -> Self {
        let mut handler = Self::new();
        statement.accept(&mut handler);
        handler
    }

    /// Builds handler metadata for a query-statement root.
    pub fn build_query(query: &mut QueryStmt) -> Self {
        let mut handler = Self::new();
        query.accept(&mut handler);
        handler
    }

    /// Go `NewBuildState`.
    #[must_use]
    pub fn new_build_state(&self) -> QBHintBuildState {
        QBHintBuildState {
            view_qb_name_used: (!self.view_qb_name_to_table.is_empty())
                .then(|| HashSet::with_capacity(self.view_qb_name_to_table.len())),
            ..QBHintBuildState::default()
        }
    }

    /// Go `MaxSelectStmtOffset`.
    #[must_use]
    pub fn max_select_stmt_offset(&self) -> i32 {
        self.select_stmt_offset
    }

    /// Returns and clears warnings accumulated by the handler.
    pub fn take_warnings(&mut self) -> Vec<HintWarning> {
        std::mem::take(&mut self.warnings)
    }

    fn warn(&mut self, message: impl Into<String>) {
        self.warnings.push(HintWarning::optimizer(message));
    }

    fn check_query_block_hints(&mut self, hints: &[Hint], offset: i32) {
        let mut qb_name = None::<String>;
        for hint in hints {
            let HintKind::QbName {
                qb_name: candidate, ..
            } = &hint.kind
            else {
                continue;
            };
            if let Some(first) = &qb_name {
                self.warn(format!(
                    "There are more than two query names in same query block, using the first one {first}"
                ));
            } else if !candidate.is_empty() {
                // An empty-name qb_name() does not occupy the slot in Go
                // (len(qbName) stays 0), so a later named hint still wins
                // without a warning.
                qb_name = Some(candidate.to_ascii_lowercase());
            }
        }
        let Some(qb_name) = qb_name.filter(|name| !name.is_empty()) else {
            return;
        };
        if self.qb_name_to_select_offset.contains_key(&qb_name) {
            self.warn(format!(
                "Duplicate query block name {qb_name}, only the first one is effective"
            ));
        } else {
            self.qb_name_to_select_offset.insert(qb_name, offset);
        }
    }

    fn handle_view_hints(&mut self, hints: &mut Vec<Hint>, offset: i32) {
        if hints.is_empty() {
            return;
        }
        let mut used = vec![false; hints.len()];
        for (index, hint) in hints.iter_mut().enumerate() {
            let HintKind::QbName { qb_name, views } = &mut hint.kind else {
                continue;
            };
            if views.is_empty() {
                continue;
            }
            used[index] = true;
            let qb_name = qb_name.to_ascii_lowercase();
            if qb_name.is_empty() {
                continue;
            }
            if self.view_qb_name_to_table.contains_key(&qb_name) {
                self.warn(format!(
                    "Duplicate query block name {qb_name} for view's query block hint, only the first one is effective"
                ));
                continue;
            }
            if offset != 1 && views[0].qb_name.is_none() {
                views[0].qb_name = Some(format!("{DEFAULT_SELECT_BLOCK_PREFIX}{offset}"));
            }
            self.view_qb_name_to_table.insert(qb_name, views.clone());
        }

        for (index, hint) in hints.iter().enumerate() {
            if used[index] || matches!(hint.kind, HintKind::QbName { .. }) {
                continue;
            }
            let mut view_name = hint_target_qb_name(hint).map(str::to_ascii_lowercase);
            let mut valid = view_name
                .as_ref()
                .is_some_and(|name| self.view_qb_name_to_table.contains_key(name));
            if view_name.is_none() {
                let tables = hint_tables(hint);
                if let Some(first) = tables.first().and_then(|table| table.qb_name.as_deref()) {
                    let first = first.to_ascii_lowercase();
                    valid = self.view_qb_name_to_table.contains_key(&first);
                    if valid
                        && tables.iter().any(|table| {
                            !table
                                .qb_name
                                .as_deref()
                                .is_some_and(|name| name.eq_ignore_ascii_case(&first))
                        })
                    {
                        self.warn("Only one query block name is allowed in a view hint, otherwise the hint will be invalid");
                        used[index] = true;
                        valid = false;
                    }
                    view_name = Some(first);
                }
            }
            if valid {
                used[index] = true;
                self.view_qb_name_to_hints
                    .entry(view_name.expect("valid view hint has a name"))
                    .or_default()
                    .push(hint.clone());
            }
        }

        let mut index = 0;
        hints.retain(|_| {
            let retain = !used[index];
            index += 1;
            retain
        });
    }

    fn block_offset(&self, block_name: &str) -> i32 {
        let block_name = block_name.to_ascii_lowercase();
        if let Some(offset) = self.qb_name_to_select_offset.get(&block_name) {
            return *offset;
        }
        if block_name == DEFAULT_UPDATE_BLOCK_NAME || block_name == DEFAULT_DELETE_BLOCK_NAME {
            return 0;
        }
        let Some(suffix) = block_name.strip_prefix(DEFAULT_SELECT_BLOCK_PREFIX) else {
            return -1;
        };
        suffix
            .parse::<i32>()
            .ok()
            .filter(|offset| *offset <= self.select_stmt_offset)
            .unwrap_or(-1)
    }

    /// Go `GetHintOffset`.
    #[must_use]
    pub fn hint_offset(&self, qb_name: Option<&str>, current_offset: i32) -> i32 {
        qb_name.map_or(current_offset, |name| self.block_offset(name))
    }

    /// Go `checkTableQBName`.
    #[must_use]
    pub fn tables_have_valid_qb_names(&self, tables: &[&HintTable]) -> bool {
        tables.iter().all(|table| {
            table
                .qb_name
                .as_deref()
                .is_none_or(|name| self.block_offset(name) >= 0)
        })
    }

    /// Go `isHint4View`.
    #[must_use]
    pub fn is_hint_for_view(&self, hint: &Hint) -> bool {
        if let Some(name) = hint_target_qb_name(hint) {
            return self
                .view_qb_name_to_table
                .contains_key(&name.to_ascii_lowercase());
        }
        hint_tables(hint).iter().all(|table| {
            table.qb_name.as_deref().is_some_and(|name| {
                self.view_qb_name_to_table
                    .contains_key(&name.to_ascii_lowercase())
            })
        })
    }

    /// Go `GetCurrentStmtHints`.
    pub fn current_stmt_hints(
        &mut self,
        hints: &[Hint],
        current_offset: i32,
        state: &mut QBHintBuildState,
    ) -> Vec<Hint> {
        for hint in hints {
            if matches!(hint.kind, HintKind::QbName { .. }) {
                continue;
            }
            let offset = self.hint_offset(hint_target_qb_name(hint), current_offset);
            let tables = hint_tables(hint);
            if offset < 0 || !self.tables_have_valid_qb_names(&tables) {
                self.warn(format!(
                    "Hint {} is ignored due to unknown query block name",
                    restore_table_optimizer_hint(hint)
                ));
                continue;
            }
            let identity = std::ptr::from_ref(hint) as usize;
            if state
                .hint_identities
                .entry(offset)
                .or_default()
                .insert(identity)
            {
                state
                    .qb_offset_to_hints
                    .entry(offset)
                    .or_default()
                    .push(hint.clone());
            }
        }
        state
            .qb_offset_to_hints
            .get(&current_offset)
            .cloned()
            .unwrap_or_default()
    }

    /// Go `MarkViewQBNameUsed`.
    pub fn mark_view_qb_name_used(&self, name: &str, state: &mut QBHintBuildState) {
        if let Some(used) = state.view_qb_name_used.as_mut() {
            used.insert(name.to_owned());
        }
    }

    /// Go `HandleUnusedViewHints`.
    #[must_use]
    pub fn unused_view_hint_warnings(&self, state: &QBHintBuildState) -> Vec<String> {
        self.view_qb_name_to_table
            .keys()
            .filter(|name| {
                !state
                    .view_qb_name_used
                    .as_ref()
                    .is_some_and(|used| used.contains(*name))
            })
            .map(|name| {
                format!(
                    "The qb_name hint {name} is unused, please check whether the table list in the qb_name hint {name} is correct"
                )
            })
            .collect()
    }

    /// Go `buildDataSource`'s view-hint matching loop. The first table in a
    /// view QB_NAME path must match the visible view name and the query block
    /// containing that reference; the remaining path belongs to the view
    /// body (and possibly nested views).
    pub fn matching_view_hints(
        &self,
        visible_view_name: &str,
        current_offset: i32,
        state: &mut QBHintBuildState,
    ) -> ViewHintContext {
        let mut context = ViewHintContext::default();
        for (qb_name, path) in &self.view_qb_name_to_table {
            let Some(first) = path.first() else { continue };
            let hint_offset = first
                .qb_name
                .as_deref()
                .map_or(1, |name| self.hint_offset(Some(name), current_offset));
            if !first.name.eq_ignore_ascii_case(visible_view_name) || hint_offset != current_offset
            {
                continue;
            }
            context
                .qb_name_to_tables
                .insert(qb_name.clone(), path[1..].to_vec());
            context.qb_name_to_hints.insert(
                qb_name.clone(),
                self.view_qb_name_to_hints
                    .get(qb_name)
                    .cloned()
                    .unwrap_or_default(),
            );
            self.mark_view_qb_name_used(qb_name, state);
        }
        context
    }

    /// Go `BuildDataSourceFromView`'s query-block-state conversion. Hints
    /// whose view path ends here become ordinary hints at the resolved block;
    /// longer paths stay attached to the nested view named next in the path.
    pub fn for_view_body(
        statement: &mut Stmt,
        inherited: ViewHintContext,
    ) -> (Self, QBHintBuildState) {
        let mut handler = Self::build(statement);
        let mut ordinary_hints = HashMap::new();
        let mut nested_tables = HashMap::new();
        let mut nested_hints = HashMap::new();
        let mut qb_name_offsets = HashMap::new();

        for (qb_name, path) in inherited.qb_name_to_tables {
            let offset = if path.is_empty() {
                1
            } else if path.len() == 1 && path[0].name.is_empty() {
                handler.hint_offset(path[0].qb_name.as_deref(), -1)
            } else {
                nested_tables.insert(qb_name.clone(), path);
                nested_hints.insert(
                    qb_name.clone(),
                    inherited
                        .qb_name_to_hints
                        .get(&qb_name)
                        .cloned()
                        .unwrap_or_default(),
                );
                -1
            };
            if offset != -1 {
                ordinary_hints.insert(
                    offset,
                    inherited
                        .qb_name_to_hints
                        .get(&qb_name)
                        .cloned()
                        .unwrap_or_default(),
                );
                qb_name_offsets.insert(qb_name, offset);
            }
        }

        handler.view_qb_name_to_table = nested_tables;
        handler.view_qb_name_to_hints = nested_hints;
        handler.qb_name_to_select_offset = qb_name_offsets;
        let mut state = handler.new_build_state();
        state.qb_offset_to_hints = ordinary_hints;
        (handler, state)
    }
}

impl Visitor for QBHintHandler {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        if let Some(update) = node.downcast_mut::<tidb_ast::UpdateStmt>() {
            self.check_query_block_hints(&update.hints, 0);
        } else if let Some(delete) = node.downcast_mut::<tidb_ast::DeleteStmt>() {
            self.check_query_block_hints(&delete.hints, 0);
        } else if let Some(select) = node.downcast_mut::<tidb_ast::SelectStmt>() {
            self.select_stmt_offset += 1;
            self.handle_view_hints(&mut select.hints, self.select_stmt_offset);
            self.check_query_block_hints(&select.hints, self.select_stmt_offset);
        } else if node.is::<tidb_ast::ExplainStmt>() || node.is::<tidb_ast::CreateBindingStmt>() {
            return true;
        }
        false
    }

    fn leave(&mut self, _node: &mut dyn Any) -> bool {
        true
    }
}

/// Go `GenerateQBName`.
pub fn generate_qb_name(node_type: NodeType, offset: i32) -> Result<String, String> {
    if offset == 0 {
        return match node_type {
            NodeType::Delete => Ok(DEFAULT_DELETE_BLOCK_NAME.to_owned()),
            NodeType::Update => Ok(DEFAULT_UPDATE_BLOCK_NAME.to_owned()),
            _ => Err(format!(
                "Unexpected NodeType {} when block offset is 0",
                node_type as u8
            )),
        };
    }
    Ok(format!("{DEFAULT_SELECT_BLOCK_PREFIX}{offset}"))
}

fn hint_target_qb_name(hint: &Hint) -> Option<&str> {
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

pub(crate) fn hint_tables(hint: &Hint) -> Vec<&HintTable> {
    let mut tables = Vec::new();
    match &hint.kind {
        HintKind::Tables { tables: source, .. } => tables.extend(source),
        HintKind::Leading { elements, .. } => collect_leading_tables(elements, &mut tables),
        HintKind::Index { table, .. } => tables.push(table),
        HintKind::QbName { views, .. } => tables.extend(views),
        HintKind::ReadFromStorage { groups, .. } => {
            for (_, group) in groups {
                tables.extend(group);
            }
        }
        _ => {}
    }
    tables
}

fn collect_leading_tables<'a>(
    elements: &'a [tidb_ast::LeadingElement],
    tables: &mut Vec<&'a HintTable>,
) {
    for element in elements {
        match element {
            tidb_ast::LeadingElement::Table(table) => tables.push(table),
            tidb_ast::LeadingElement::Group(group) => collect_leading_tables(group, tables),
        }
    }
}
