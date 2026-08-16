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

//! SEED of Go `pkg/util/hint/hint_query_block.go`: the query-block naming
//! and offset resolution that decides which query block a `/*+ ... */` hint
//! applies to.
//!
//! This is a seed, not a completed package. Go's `QBHintHandler` is an AST
//! visitor that also stamps `SelectStmt.QueryBlockOffset` as it walks; this
//! module owns the *resolution* half — the `qb_name` to offset map, the
//! default block names, the `sel_N` grammar and its bound, the view-hint
//! bookkeeping, and every warning those produce — while the caller supplies
//! the offsets. Stamping the offset onto the AST needs a
//! `QueryBlockOffset` field that `tidb_ast::SelectStmt` does not carry yet,
//! and `hint.go` (1275 LOC of `StmtHints`) is untouched.
//!
//! Hints arrive as [`QbHint`], a uniform view of Go's
//! `ast.TableOptimizerHint`. Go has one hint struct with `HintName`,
//! `QBName`, and `Tables` fields; `tidb_ast::Hint` is an enum whose
//! `qb_name` lives per variant, so the bridge is built by the caller rather
//! than assumed here.
//!
//! Name comparisons use the lowercased form throughout, which is what Go's
//! `CIStr.L` holds.

use std::collections::{BTreeMap, BTreeSet};

/// Go `defaultUpdateBlockName`.
pub const DEFAULT_UPDATE_BLOCK_NAME: &str = "upd_1";
/// Go `defaultDeleteBlockName`.
pub const DEFAULT_DELETE_BLOCK_NAME: &str = "del_1";
/// Go `defaultSelectBlockPrefix`.
pub const DEFAULT_SELECT_BLOCK_PREFIX: &str = "sel_";
/// Go's private `hintQBName`.
pub const HINT_QB_NAME: &str = "qb_name";

/// Go `NodeType`: which statement a hint's default query block belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeType {
    /// Go `TypeUpdate`.
    Update,
    /// Go `TypeDelete`.
    Delete,
    /// Go `TypeSelect`.
    Select,
    /// Go `TypeInvalid`.
    Invalid,
}

/// One table argument of a hint, narrowed to what query-block resolution
/// reads (Go `ast.HintTable`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QbHintTable {
    /// The table name, lowercased.
    pub name: String,
    /// The table's own `@query_block` suffix, lowercased; empty when absent.
    pub qb_name: String,
}

/// A uniform view of Go `ast.TableOptimizerHint` for query-block resolution.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QbHint {
    /// The hint name, lowercased (Go `HintName.L`).
    pub name: String,
    /// The hint-level `@query_block` name, lowercased; empty when absent.
    pub qb_name: String,
    /// The hint's table arguments.
    pub tables: Vec<QbHintTable>,
    /// The hint's restored text, used verbatim in the ignore warning. Go
    /// calls `RestoreTableOptimizerHint` at that point.
    pub restored: String,
}

/// Go's private `hintWarnHandler`, collecting the warnings resolution emits.
#[derive(Clone, Debug, Default)]
pub struct HintWarnCollector {
    warnings: Vec<String>,
}

impl HintWarnCollector {
    /// Go `SetHintWarning`.
    pub fn set_hint_warning(&mut self, warning: impl Into<String>) {
        self.warnings.push(warning.into());
    }

    /// The warnings collected so far, in order.
    #[must_use]
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }
}

/// Go `QBHintBuildState`: the per-build runtime state, kept apart from the
/// handler so the handler's AST-derived metadata can be shared.
#[derive(Clone, Debug, Default)]
pub struct QbHintBuildState {
    /// Go `QBOffsetToHints`.
    pub qb_offset_to_hints: BTreeMap<i64, Vec<QbHint>>,
    /// Go `ViewQBNameUsed`; `None` when the handler has no view hints, which
    /// is how Go leaves the map nil in that case.
    pub view_qb_name_used: Option<BTreeSet<String>>,
}

/// Go `QBHintHandler`.
#[derive(Clone, Debug, Default)]
pub struct QbHintHandler {
    /// Go `QBNameToSelOffset`: query-block name to select offset.
    pub qb_name_to_sel_offset: BTreeMap<String, i64>,
    /// Go `ViewQBNameToTable`.
    pub view_qb_name_to_table: BTreeMap<String, Vec<QbHintTable>>,
    /// Go `ViewQBNameToHints`.
    pub view_qb_name_to_hints: BTreeMap<String, Vec<QbHint>>,
    select_stmt_offset: i64,
}

impl QbHintHandler {
    /// Go `NewQBHintHandler`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `MaxSelectStmtOffset`.
    #[must_use]
    pub const fn max_select_stmt_offset(&self) -> i64 {
        self.select_stmt_offset
    }

    /// The offset Go's visitor assigns to the next `SelectStmt` it enters:
    /// it pre-increments, so the first select block is offset 1.
    pub fn next_select_stmt_offset(&mut self) -> i64 {
        self.select_stmt_offset += 1;
        self.select_stmt_offset
    }

    /// Go `NewBuildState`. The used-name set is allocated only when the
    /// handler actually carries view hints.
    #[must_use]
    pub fn new_build_state(&self) -> QbHintBuildState {
        QbHintBuildState {
            qb_offset_to_hints: BTreeMap::new(),
            view_qb_name_used: if self.view_qb_name_to_table.is_empty() {
                None
            } else {
                Some(BTreeSet::new())
            },
        }
    }

    /// Go `checkQueryBlockHints`: records the block's `qb_name`, warning when
    /// a block names itself twice or when a name repeats across blocks.
    ///
    /// Only the first `qb_name` in a block is taken, and the warning quotes
    /// that first name, not the duplicate.
    pub fn check_query_block_hints(
        &mut self,
        hints: &[QbHint],
        offset: i64,
        warnings: &mut HintWarnCollector,
    ) {
        let mut qb_name = String::new();
        for hint in hints {
            if hint.name != HINT_QB_NAME {
                continue;
            }
            if qb_name.is_empty() {
                qb_name.clone_from(&hint.qb_name);
            } else {
                warnings.set_hint_warning(format!(
                    "There are more than two query names in same query block, using the first one {qb_name}"
                ));
            }
        }
        if qb_name.is_empty() {
            return;
        }
        match self.qb_name_to_sel_offset.entry(qb_name) {
            std::collections::btree_map::Entry::Occupied(occupied) => {
                let qb_name = occupied.key();
                warnings.set_hint_warning(format!(
                    "Duplicate query block name {qb_name}, only the first one is effective"
                ));
            }
            std::collections::btree_map::Entry::Vacant(vacant) => {
                vacant.insert(offset);
            }
        }
    }

    /// Go's private `getBlockOffset`: 0 for a top-level update or delete,
    /// the recorded offset for a registered name, and -1 for anything
    /// unknown — including a `sel_N` whose N exceeds the blocks actually
    /// seen.
    #[must_use]
    pub fn block_offset(&self, block_name: &str) -> i64 {
        if let Some(offset) = self.qb_name_to_sel_offset.get(block_name) {
            return *offset;
        }
        if block_name == DEFAULT_UPDATE_BLOCK_NAME || block_name == DEFAULT_DELETE_BLOCK_NAME {
            return 0;
        }
        if let Some(suffix) = block_name.strip_prefix(DEFAULT_SELECT_BLOCK_PREFIX) {
            // Go parses with strconv.ParseInt, which rejects a leading `+`
            // sign only for being unparsable as the bare digits it expects;
            // any parse failure or an out-of-range block is -1.
            return match suffix.parse::<i64>() {
                Ok(level) if level <= self.select_stmt_offset => level,
                _ => -1,
            };
        }
        -1
    }

    /// Go `GetHintOffset`: a named block resolves through the map, an unnamed
    /// hint stays where it was written.
    #[must_use]
    pub fn hint_offset(&self, qb_name: &str, current_offset: i64) -> i64 {
        if qb_name.is_empty() {
            current_offset
        } else {
            self.block_offset(qb_name)
        }
    }

    /// Go's private `checkTableQBName`: every named table must resolve.
    #[must_use]
    pub fn check_table_qb_name(&self, tables: &[QbHintTable]) -> bool {
        tables
            .iter()
            .all(|table| table.qb_name.is_empty() || self.block_offset(&table.qb_name) >= 0)
    }

    /// Go `GetCurrentStmtHints`: bucket every hint by the block it applies to
    /// and return this block's bucket.
    ///
    /// `qb_name` hints are skipped, a hint naming an unresolvable block is
    /// dropped with a warning, and a hint already present in its bucket is
    /// not added twice.
    pub fn current_stmt_hints(
        &self,
        hints: &[QbHint],
        current_offset: i64,
        state: &mut QbHintBuildState,
        warnings: &mut HintWarnCollector,
    ) -> Vec<QbHint> {
        for hint in hints {
            if hint.name == HINT_QB_NAME {
                continue;
            }
            let offset = self.hint_offset(&hint.qb_name, current_offset);
            if offset < 0 || !self.check_table_qb_name(&hint.tables) {
                warnings.set_hint_warning(format!(
                    "Hint {} is ignored due to unknown query block name",
                    hint.restored
                ));
                continue;
            }
            let bucket = state.qb_offset_to_hints.entry(offset).or_default();
            if !bucket.contains(hint) {
                bucket.push(hint.clone());
            }
        }
        state
            .qb_offset_to_hints
            .get(&current_offset)
            .cloned()
            .unwrap_or_default()
    }

    /// Go `MarkViewQBNameUsed`.
    pub fn mark_view_qb_name_used(qb_name: &str, state: &mut QbHintBuildState) {
        if let Some(used) = state.view_qb_name_used.as_mut() {
            used.insert(qb_name.to_owned());
        }
    }

    /// Go's private `handleViewHints`: pulls the view-form `QB_NAME` hints
    /// and the hints that target them out of a block's hint list, returning
    /// what is left on the block.
    ///
    /// Pass one registers each `QB_NAME(name, view...)`: an empty name is
    /// consumed silently, a repeated name warns and keeps the first, and —
    /// when the block is not the first — a first view entry without its own
    /// `@sel_N` is stamped with the block's offset. Pass two routes every
    /// other hint that names a registered view (at hint level, or uniformly
    /// across its tables) into the view-hint registry, warning when tables
    /// mix query blocks. Whatever neither pass consumed stays on the block.
    pub fn handle_view_hints(
        &mut self,
        hints: Vec<QbHint>,
        offset: i64,
        warnings: &mut HintWarnCollector,
    ) -> Vec<QbHint> {
        if hints.is_empty() {
            return hints;
        }
        let mut hints = hints;
        let mut used = vec![false; hints.len()];

        for (i, hint) in hints.iter_mut().enumerate() {
            if hint.name != HINT_QB_NAME || hint.tables.is_empty() {
                continue;
            }
            used[i] = true;
            let qb_name = hint.qb_name.clone();
            if qb_name.is_empty() {
                continue;
            }
            match self.view_qb_name_to_table.entry(qb_name) {
                std::collections::btree_map::Entry::Occupied(occupied) => {
                    let qb_name = occupied.key();
                    warnings.set_hint_warning(format!(
                        "Duplicate query block name {qb_name} for view's query block hint, only the first one is effective"
                    ));
                }
                std::collections::btree_map::Entry::Vacant(vacant) => {
                    if offset != 1 && hint.tables[0].qb_name.is_empty() {
                        hint.tables[0].qb_name = format!("{DEFAULT_SELECT_BLOCK_PREFIX}{offset}");
                    }
                    vacant.insert(hint.tables.clone());
                }
            }
        }

        for (i, hint) in hints.iter().enumerate() {
            if used[i] || hint.name == HINT_QB_NAME {
                continue;
            }
            let mut ok = false;
            let mut qb_name = hint.qb_name.clone();
            if !qb_name.is_empty() {
                ok = self.view_qb_name_to_table.contains_key(&qb_name);
            } else if !hint.tables.is_empty() {
                // Only tables of one query block may share a view hint.
                qb_name = hint.tables[0].qb_name.clone();
                ok = self.view_qb_name_to_table.contains_key(&qb_name);
                if ok && hint.tables.iter().any(|table| table.qb_name != qb_name) {
                    warnings.set_hint_warning(
                        "Only one query block name is allowed in a view hint, otherwise the hint will be invalid",
                    );
                    used[i] = true;
                    ok = false;
                }
            }
            if ok {
                used[i] = true;
                self.view_qb_name_to_hints
                    .entry(qb_name)
                    .or_default()
                    .push(hint.clone());
            }
        }

        hints
            .into_iter()
            .zip(used)
            .filter_map(|(hint, was_used)| (!was_used).then_some(hint))
            .collect()
    }

    /// Go's private `isHint4View`: a hint-level name checks registration
    /// directly; an unnamed hint is a view hint only when EVERY table's
    /// query block is registered.
    #[must_use]
    pub fn is_hint_for_view(&self, hint: &QbHint) -> bool {
        if !hint.qb_name.is_empty() {
            return self.view_qb_name_to_table.contains_key(&hint.qb_name);
        }
        hint.tables
            .iter()
            .all(|table| self.view_qb_name_to_table.contains_key(&table.qb_name))
    }

    /// Go `HandleUnusedViewHints`: replaces the warning list with one entry
    /// per view `qb_name` the build never used. Go truncates the incoming
    /// slice first, so nothing passed in survives.
    #[must_use]
    pub fn handle_unused_view_hints(&self, state: &QbHintBuildState) -> Vec<String> {
        let mut warnings = Vec::new();
        for qb_name in self.view_qb_name_to_table.keys() {
            let used = state
                .view_qb_name_used
                .as_ref()
                .is_some_and(|used| used.contains(qb_name));
            if !used {
                warnings.push(format!(
                    "The qb_name hint {qb_name} is unused, please check whether the table list in the qb_name hint {qb_name} is correct"
                ));
            }
        }
        warnings
    }
}

/// Go `GenerateQBName`: the block name for an offset.
///
/// Offset 0 is the top-level block, which only `UPDATE` and `DELETE` have a
/// name for; a `SELECT` at offset 0 is an error, as is any other node type.
pub fn generate_qb_name(node_type: NodeType, qb_offset: i64) -> Result<String, String> {
    if qb_offset == 0 {
        return match node_type {
            NodeType::Delete => Ok(DEFAULT_DELETE_BLOCK_NAME.to_owned()),
            NodeType::Update => Ok(DEFAULT_UPDATE_BLOCK_NAME.to_owned()),
            other => Err(format!(
                "Unexpected NodeType {} when block offset is 0",
                node_type_code(other)
            )),
        };
    }
    Ok(format!("{DEFAULT_SELECT_BLOCK_PREFIX}{qb_offset}"))
}

/// Go's `NodeType` is an `iota` run; the error message prints the number.
const fn node_type_code(node_type: NodeType) -> i32 {
    match node_type {
        NodeType::Invalid => 0,
        NodeType::Update => 1,
        NodeType::Delete => 2,
        NodeType::Select => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hint(name: &str, qb_name: &str) -> QbHint {
        QbHint {
            name: name.to_owned(),
            qb_name: qb_name.to_owned(),
            tables: Vec::new(),
            restored: format!("{name}(...)"),
        }
    }

    fn qb_name_hint(qb_name: &str) -> QbHint {
        hint(HINT_QB_NAME, qb_name)
    }

    // A block registers its qb_name against the offset it was seen at.
    #[test]
    fn a_query_block_name_maps_to_its_offset() {
        let mut handler = QbHintHandler::new();
        let mut warnings = HintWarnCollector::default();
        assert_eq!(handler.next_select_stmt_offset(), 1);
        handler.check_query_block_hints(&[qb_name_hint("qb1")], 1, &mut warnings);

        assert_eq!(handler.qb_name_to_sel_offset.get("qb1"), Some(&1));
        assert_eq!(handler.block_offset("qb1"), 1);
        assert!(warnings.warnings().is_empty());
    }

    // Two names in one block: the first wins and the warning quotes it.
    #[test]
    fn a_second_name_in_one_block_warns_and_loses() {
        let mut handler = QbHintHandler::new();
        let mut warnings = HintWarnCollector::default();
        handler.check_query_block_hints(
            &[qb_name_hint("first"), qb_name_hint("second")],
            1,
            &mut warnings,
        );

        assert_eq!(handler.block_offset("first"), 1);
        assert_eq!(handler.block_offset("second"), -1);
        assert_eq!(
            warnings.warnings(),
            ["There are more than two query names in same query block, using the first one first"]
        );
    }

    // The same name in two blocks keeps the first block's offset.
    #[test]
    fn a_repeated_name_across_blocks_warns_and_keeps_the_first() {
        let mut handler = QbHintHandler::new();
        let mut warnings = HintWarnCollector::default();
        handler.next_select_stmt_offset();
        handler.check_query_block_hints(&[qb_name_hint("dup")], 1, &mut warnings);
        handler.next_select_stmt_offset();
        handler.check_query_block_hints(&[qb_name_hint("dup")], 2, &mut warnings);

        assert_eq!(handler.block_offset("dup"), 1);
        assert_eq!(
            warnings.warnings(),
            ["Duplicate query block name dup, only the first one is effective"]
        );
    }

    // The default names, the sel_N grammar, and its bound.
    #[test]
    fn default_and_positional_block_names_resolve() {
        let mut handler = QbHintHandler::new();
        assert_eq!(handler.block_offset(DEFAULT_UPDATE_BLOCK_NAME), 0);
        assert_eq!(handler.block_offset(DEFAULT_DELETE_BLOCK_NAME), 0);
        // No select block has been seen, so even sel_1 is out of range.
        assert_eq!(handler.block_offset("sel_1"), -1);

        handler.next_select_stmt_offset();
        handler.next_select_stmt_offset();
        assert_eq!(handler.block_offset("sel_1"), 1);
        assert_eq!(handler.block_offset("sel_2"), 2);
        // Beyond the blocks actually seen.
        assert_eq!(handler.block_offset("sel_3"), -1);
        // Unparsable suffixes and unknown names.
        assert_eq!(handler.block_offset("sel_x"), -1);
        assert_eq!(handler.block_offset("sel_"), -1);
        assert_eq!(handler.block_offset("nope"), -1);
    }

    // A registered name outranks the positional grammar.
    #[test]
    fn a_registered_name_wins_over_the_positional_form() {
        let mut handler = QbHintHandler::new();
        let mut warnings = HintWarnCollector::default();
        handler.next_select_stmt_offset();
        handler.next_select_stmt_offset();
        // Deliberately register `sel_1` pointing at the second block.
        handler.check_query_block_hints(&[qb_name_hint("sel_1")], 2, &mut warnings);
        assert_eq!(handler.block_offset("sel_1"), 2);
    }

    // An unnamed hint stays where written; a named one moves.
    #[test]
    fn hint_offsets_follow_the_query_block_name() {
        let mut handler = QbHintHandler::new();
        let mut warnings = HintWarnCollector::default();
        handler.next_select_stmt_offset();
        handler.next_select_stmt_offset();
        handler.check_query_block_hints(&[qb_name_hint("qb2")], 2, &mut warnings);

        assert_eq!(handler.hint_offset("", 1), 1);
        assert_eq!(handler.hint_offset("qb2", 1), 2);
        assert_eq!(handler.hint_offset("missing", 1), -1);
    }

    // Table-level query-block names must resolve too.
    #[test]
    fn table_query_block_names_are_checked() {
        let mut handler = QbHintHandler::new();
        let mut warnings = HintWarnCollector::default();
        handler.next_select_stmt_offset();
        handler.check_query_block_hints(&[qb_name_hint("qb1")], 1, &mut warnings);

        let unnamed = [QbHintTable::default()];
        assert!(handler.check_table_qb_name(&unnamed));
        let known = [QbHintTable {
            name: "t".to_owned(),
            qb_name: "qb1".to_owned(),
        }];
        assert!(handler.check_table_qb_name(&known));
        let unknown = [QbHintTable {
            name: "t".to_owned(),
            qb_name: "nope".to_owned(),
        }];
        assert!(!handler.check_table_qb_name(&unknown));
    }

    // Hints bucket by block; qb_name hints never bucket, unresolvable ones
    // warn, and a repeat of the same hint is not added twice.
    #[test]
    fn hints_bucket_by_block_and_deduplicate() {
        let mut handler = QbHintHandler::new();
        let mut warnings = HintWarnCollector::default();
        handler.next_select_stmt_offset();
        handler.next_select_stmt_offset();
        handler.check_query_block_hints(&[qb_name_hint("qb2")], 2, &mut warnings);

        let mut state = handler.new_build_state();
        let here = hint("hash_join", "");
        let there = hint("merge_join", "qb2");
        let bogus = hint("stream_agg", "missing");
        let hints = vec![
            here.clone(),
            there.clone(),
            bogus,
            qb_name_hint("qb2"),
            here.clone(),
        ];

        let current = handler.current_stmt_hints(&hints, 1, &mut state, &mut warnings);
        // Only the unnamed hint lands in this block, and only once.
        assert_eq!(current, vec![here]);
        // The named hint went to block 2 instead.
        assert_eq!(state.qb_offset_to_hints.get(&2), Some(&vec![there]));
        assert_eq!(
            warnings.warnings(),
            ["Hint stream_agg(...) is ignored due to unknown query block name"]
        );
    }

    // Without view hints Go leaves the used-name map nil and reports nothing.
    #[test]
    fn a_handler_without_view_hints_has_no_used_set() {
        let handler = QbHintHandler::new();
        let state = handler.new_build_state();
        assert!(state.view_qb_name_used.is_none());
        assert!(handler.handle_unused_view_hints(&state).is_empty());
    }

    // Every view qb_name the build never marked is reported.
    #[test]
    fn unused_view_query_block_names_are_reported() {
        let mut handler = QbHintHandler::new();
        handler
            .view_qb_name_to_table
            .insert("used".to_owned(), Vec::new());
        handler
            .view_qb_name_to_table
            .insert("stale".to_owned(), Vec::new());

        let mut state = handler.new_build_state();
        assert!(state.view_qb_name_used.is_some());
        QbHintHandler::mark_view_qb_name_used("used", &mut state);

        let reported = handler.handle_unused_view_hints(&state);
        assert_eq!(
            reported,
            ["The qb_name hint stale is unused, please check whether the table list in the qb_name hint stale is correct"]
        );
    }

    // Go `GenerateQBName`.
    #[test]
    fn generated_block_names_follow_the_node_type() {
        assert_eq!(generate_qb_name(NodeType::Delete, 0).unwrap(), "del_1");
        assert_eq!(generate_qb_name(NodeType::Update, 0).unwrap(), "upd_1");
        assert_eq!(generate_qb_name(NodeType::Select, 1).unwrap(), "sel_1");
        assert_eq!(generate_qb_name(NodeType::Update, 4).unwrap(), "sel_4");
        assert_eq!(
            generate_qb_name(NodeType::Select, 0).unwrap_err(),
            "Unexpected NodeType 3 when block offset is 0"
        );
        assert_eq!(
            generate_qb_name(NodeType::Invalid, 0).unwrap_err(),
            "Unexpected NodeType 0 when block offset is 0"
        );
    }
    // Go handleViewHints: registration, duplicate warning, offset stamping,
    // routing, and the mixed-block rejection.
    #[test]
    fn view_hints_register_route_and_reject_mixed_blocks() {
        let mut handler = QbHintHandler::new();
        let mut warnings = HintWarnCollector::default();
        let view_table = |qb: &str| QbHintTable {
            name: "v1".to_owned(),
            qb_name: qb.to_owned(),
        };
        let qb_view = |name: &str, tables: Vec<QbHintTable>| QbHint {
            name: HINT_QB_NAME.to_owned(),
            qb_name: name.to_owned(),
            tables,
            restored: String::new(),
        };

        // Registration on a non-first block stamps the bare first entry.
        let left =
            handler.handle_view_hints(vec![qb_view("qv", vec![view_table("")])], 2, &mut warnings);
        assert!(left.is_empty());
        assert_eq!(handler.view_qb_name_to_table["qv"][0].qb_name, "sel_2");

        // A repeated name warns and keeps the first registration.
        let _ = handler.handle_view_hints(
            vec![qb_view("qv", vec![view_table("sel_9")])],
            1,
            &mut warnings,
        );
        assert_eq!(
            warnings.warnings(),
            ["Duplicate query block name qv for view's query block hint, only the first one is effective"]
        );
        assert_eq!(handler.view_qb_name_to_table["qv"][0].qb_name, "sel_2");

        // A hint naming the view at hint level routes into the registry.
        let named = QbHint {
            name: "merge_join".to_owned(),
            qb_name: "qv".to_owned(),
            tables: Vec::new(),
            restored: String::new(),
        };
        let left = handler.handle_view_hints(vec![named.clone()], 1, &mut warnings);
        assert!(left.is_empty());
        assert_eq!(handler.view_qb_name_to_hints["qv"], vec![named.clone()]);
        assert!(handler.is_hint_for_view(&named));

        // Tables mixing query blocks reject the hint with the warning.
        let mixed = QbHint {
            name: "hash_join".to_owned(),
            qb_name: String::new(),
            tables: vec![
                QbHintTable {
                    name: "t1".to_owned(),
                    qb_name: "qv".to_owned(),
                },
                QbHintTable {
                    name: "t2".to_owned(),
                    qb_name: "other".to_owned(),
                },
            ],
            restored: String::new(),
        };
        let left = handler.handle_view_hints(vec![mixed], 1, &mut warnings);
        assert!(left.is_empty());
        assert_eq!(handler.view_qb_name_to_hints["qv"].len(), 1);
        assert!(warnings.warnings()[1].contains("Only one query block name is allowed"));

        // A hint targeting no registered view stays on the block.
        let plain = QbHint {
            name: "hash_agg".to_owned(),
            qb_name: String::new(),
            tables: Vec::new(),
            restored: String::new(),
        };
        assert!(handler.is_hint_for_view(&plain));
        let left = handler.handle_view_hints(vec![plain.clone()], 1, &mut warnings);
        assert_eq!(left, vec![plain]);
    }
}
