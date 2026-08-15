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

//! SQL bindings: `CREATE BINDING`, `DROP BINDING`, `SHOW BINDINGS` and the
//! plan-time match, transcreated from Go's `pkg/bindinfo`.
//!
//! # What a binding is, mechanically
//!
//! A binding attaches the HINTS written in one statement to every later
//! statement that normalizes to the same text. Go stores the pair, and at
//! plan time replaces the incoming statement's hints with the binding's --
//! `hint.BindHint` in `pkg/planner/optimize.go`. The literals stay the
//! incoming statement's own, which is why `in (1)` and `in (1,2,3)` share one
//! binding: `parser.NormalizeDigestForBinding` collapses both in-lists to
//! `in ( ... )`, and this crate's [`tidb_parser::normalize_digest_for_binding`]
//! already ports that rule.
//!
//! Three things drop out of the normalizer that are surprising until measured,
//! and all three were measured here rather than assumed (Go probe against
//! `parser.NormalizeDigestForBinding` on this branch):
//!
//! * `/*+ ... */` comment hints vanish -- `reduceOptimizerHint`.
//! * `USE`/`FORCE`/`IGNORE INDEX (...)` ALSO vanish, in the same function.
//!   So a binding whose hinted SQL is `select * from t use index(kb) ...`
//!   really does match a plain `select * from t ...`, and a query that writes
//!   `ignore index(kb)` itself matches that same binding. Confirmed on real
//!   TiDB through `gorun`: all three spellings answered
//!   `@@last_plan_from_binding = 1` against one binding.
//! * `straight_join` normalizes to `join`.
//!
//! # Session scope is implemented; GLOBAL scope is refused, and why
//!
//! A session binding lives in a map on the session and needs no storage. A
//! GLOBAL binding is a row in `mysql.bind_info` shared by every session, and
//! this tier has no such table: the name is in `tidb-metadef`'s
//! `CREATE_BIND_INFO_TABLE` as DDL text, but no session's catalog contains
//! it. That is measured, not asserted --
//! `select * from mysql.bind_info` in this tier answers `table not found in
//! catalog`, and `tests_binding`'s
//! `global_scope_is_refused_because_the_storage_table_is_absent` pins BOTH
//! halves so the refusal flips the day the table appears.
//!
//! # What a match changes here
//!
//! The binding's hints replace the statement's. Table-level `USE`/`FORCE`/
//! `IGNORE INDEX` are honoured by this tier's own access-path selection
//! (`tidb_executor::index_hints`), so such a binding really does move the
//! plan. `/*+ ... */` comment hints are inert in this tier whether they are
//! written in the query or carried by a binding -- `report_comment_index_hints`
//! only warns -- so a binding carrying them transfers them faithfully and
//! changes nothing, exactly as writing them directly changes nothing. The gap
//! is this tier's comment-hint coverage, not the binding.

use std::any::Any;
use std::collections::BTreeMap;

use tidb_ast::{Stmt, Visitable as _, Visitor};
use tidb_executor::DriverError;

/// Go `utilparser.RestoreWithDefaultDB`: the statement's canonical text with
/// every unqualified table name qualified by `default_db`. Delegates to the
/// complete `pkg/util/parser` port; Go additionally passes `node.Text()` so
/// `SimpleCases` can keep the user's own formatting, but this AST does not
/// retain the statement's raw text, so the full-restore path is always taken
/// (same output as before this delegation).
pub(crate) fn restore_with_default_db(stmt: &Stmt, default_db: &str) -> String {
    tidb_parser::util_parser::restore_with_default_db(stmt, default_db, "")
}

/// Go `utilparser.RestoreWithoutDB`: the same text with every schema
/// qualifier ERASED, which is what makes a binding portable across databases.
pub(crate) fn restore_without_db(stmt: &Stmt) -> String {
    tidb_parser::util_parser::restore_without_db(stmt)
}

/// Go `bindinfo.NormalizeStmtForBinding`, the with-DB half: the normalized
/// text and its digest.
pub(crate) fn normalize_with_db(stmt: &Stmt, default_db: &str) -> (String, String) {
    let (normalized, digest) =
        tidb_parser::normalize_digest_for_binding(&restore_with_default_db(stmt, default_db));
    (normalized, digest.as_str().to_owned())
}

/// Go `bindinfo.NormalizeStmtForBinding`, the no-DB half: the digest a match
/// is decided on.
pub(crate) fn no_db_digest(stmt: &Stmt) -> String {
    let (_, digest) = tidb_parser::normalize_digest_for_binding(&restore_without_db(stmt));
    digest.as_str().to_owned()
}

/// Go's `hint.HintsSet`: the table-level optimizer hints of each statement
/// block and the index hints of each table name, both in traversal order.
///
/// Two parallel lists rather than a tree because that is the whole trick:
/// collecting from one statement and assigning to another in the SAME order
/// transfers hints between two statements that differ only in literals.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct HintsSet {
    table_hints: Vec<Vec<tidb_ast::Hint>>,
    index_hints: Vec<Vec<tidb_ast::IndexHint>>,
}

/// Go's `hintProcessor`, both directions. `bind_to_ast` false collects
/// (`CollectHint`), true assigns (`BindHint`).
struct HintProcessor {
    set: HintsSet,
    bind_to_ast: bool,
    table_counter: usize,
    index_counter: usize,
    block_counter: usize,
}

impl Visitor for HintProcessor {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        if let Some(select) = node.downcast_mut::<tidb_ast::SelectStmt>() {
            if self.bind_to_ast {
                select.hints = self
                    .set
                    .table_hints
                    .get(self.table_counter)
                    .cloned()
                    .unwrap_or_default();
                self.table_counter += 1;
            } else {
                self.set.table_hints.push(select.hints.clone());
            }
            self.block_counter += 1;
            return false;
        }
        if let Some(table_ref) = node.downcast_mut::<tidb_ast::TableRef>() {
            // Go's `hp.blockCounter == 0` guard: a table name reached outside
            // any SELECT/UPDATE/DELETE block (an INSERT's target) is not an
            // index-hint site and must not consume a slot.
            if self.block_counter == 0 {
                return false;
            }
            if self.bind_to_ast {
                table_ref.hints = self
                    .set
                    .index_hints
                    .get(self.index_counter)
                    .cloned()
                    .unwrap_or_default();
                self.index_counter += 1;
            } else {
                self.set.index_hints.push(table_ref.hints.clone());
            }
        }
        false
    }

    fn leave(&mut self, node: &mut dyn Any) -> bool {
        if node.is::<tidb_ast::SelectStmt>() {
            self.block_counter = self.block_counter.saturating_sub(1);
        }
        true
    }
}

/// Go `hint.CollectHint`.
pub(crate) fn collect_hints(stmt: &Stmt) -> HintsSet {
    let mut stmt = stmt.clone();
    let mut processor = HintProcessor {
        set: HintsSet::default(),
        bind_to_ast: false,
        table_counter: 0,
        index_counter: 0,
        block_counter: 0,
    };
    stmt.accept(&mut processor);
    processor.set
}

/// Go `hint.BindHint`: replaces the statement's hints with the set's, block
/// for block. A block beyond the set's length has its hints CLEARED, which is
/// Go's `setTableHints4StmtNode(in, nil)` -- the binding decides every block's
/// hints, not just the ones it has something to say about.
pub(crate) fn bind_hints(stmt: &mut Stmt, set: &HintsSet) {
    let mut processor = HintProcessor {
        set: set.clone(),
        bind_to_ast: true,
        table_counter: 0,
        index_counter: 0,
        block_counter: 0,
    };
    stmt.accept(&mut processor);
}

/// Collects `(schema, table)` in traversal order, lowercased, which is Go's
/// `bindinfo.CollectTableNames` plus the `.L` its comparisons use.
struct TableNameCollector {
    names: Vec<(String, String)>,
}

impl Visitor for TableNameCollector {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        if let Some(table_ref) = node.downcast_mut::<tidb_ast::TableRef>() {
            let (schema, table) = match table_ref.name.as_slice() {
                [table] => (String::new(), table.clone()),
                [schema, table] => (schema.clone(), table.clone()),
                _ => return false,
            };
            self.names
                .push((schema.to_lowercase(), table.to_lowercase()));
        }
        false
    }

    fn leave(&mut self, _node: &mut dyn Any) -> bool {
        true
    }
}

/// Go `bindinfo.CollectTableNames`.
pub(crate) fn collect_table_names(stmt: &Stmt) -> Vec<(String, String)> {
    let mut stmt = stmt.clone();
    let mut collector = TableNameCollector { names: Vec::new() };
    stmt.accept(&mut collector);
    collector.names
}

/// Every `TableRef` in traversal order, keeping the name path and alias AS
/// WRITTEN.
///
/// [`collect_table_names`] lowercases and drops the alias because Go's
/// binding matcher compares `.L` names; the privilege collector
/// ([`crate::table_privilege`]) needs the written spelling for the error
/// message and the alias to place a multi-table `UPDATE`/`DELETE` target, so
/// it reads the same nodes through this.
pub(crate) fn collect_table_refs(stmt: &Stmt) -> Vec<(Vec<String>, Option<String>)> {
    struct Collector {
        refs: Vec<(Vec<String>, Option<String>)>,
    }
    impl Visitor for Collector {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            if let Some(table_ref) = node.downcast_mut::<tidb_ast::TableRef>() {
                self.refs
                    .push((table_ref.name.clone(), table_ref.alias.clone()));
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn Any) -> bool {
            true
        }
    }
    let mut stmt = stmt.clone();
    let mut collector = Collector { refs: Vec::new() };
    stmt.accept(&mut collector);
    collector.refs
}

/// The written name path of every `TableRef`, which is the row-source list
/// Go's `buildDataSource` walks.
pub(crate) fn collect_table_paths(stmt: &Stmt) -> Vec<Vec<String>> {
    collect_table_refs(stmt)
        .into_iter()
        .map(|(path, _)| path)
        .collect()
}

/// Every name a `WITH` clause anywhere in `stmt` defines.
///
/// A CTE is REFERENCED through the ordinary table grammar, so it parses as a
/// `TableRef` -- but it resolves to the query that defined it, not to a
/// stored table, and Go's `buildDataSource` is never reached for one. The
/// privilege collector subtracts these so `WITH c AS (...) SELECT * FROM c`
/// does not demand `SELECT` on a table named `c`.
pub(crate) fn collect_cte_names(stmt: &Stmt) -> Vec<String> {
    struct Collector {
        names: Vec<String>,
    }
    impl Visitor for Collector {
        fn enter(&mut self, node: &mut dyn Any) -> bool {
            if let Some(cte) = node.downcast_mut::<tidb_ast::Cte>() {
                self.names.push(cte.name.clone());
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn Any) -> bool {
            true
        }
    }
    let mut stmt = stmt.clone();
    let mut collector = Collector { names: Vec::new() };
    stmt.accept(&mut collector);
    collector.names
}

/// Go's `bindinfo.Binding`, minus the fields only a stored global binding
/// has (`PlanDigest` from a captured plan, `SourceHistory`, the usage
/// counters `mysql.bind_info` carries).
#[derive(Debug, Clone)]
pub(crate) struct Binding {
    /// The NORMALIZED origin statement, DB-qualified. This is the text
    /// `SHOW BINDINGS` prints first, not the SQL the user typed.
    pub(crate) original_sql: String,
    /// The hinted statement, restored and DB-qualified.
    pub(crate) bind_sql: String,
    /// Go `GetDefaultDB`: the schema the origin statement resolves against.
    pub(crate) db: String,
    /// `enabled` for every binding this tier creates; `SET BINDING DISABLED`
    /// is not modelled, so the column is constant rather than absent.
    pub(crate) status: &'static str,
    pub(crate) charset: String,
    pub(crate) collation: String,
    /// Go `bindinfo.SourceManual`.
    pub(crate) source: &'static str,
    /// Digest of [`Self::original_sql`], and the store's own key.
    pub(crate) sql_digest: String,
    pub(crate) create_time: String,
    pub(crate) update_time: String,
    /// Digest of the HINTED statement restored WITHOUT its schema, which is
    /// Go's `noDBDigestFromBinding` -- it parses `BindSQL`, not the origin.
    /// Equal to the origin's own no-DB digest whenever the pair is legal,
    /// because the only legal difference between them is hints and the
    /// normalizer erases those.
    pub(crate) no_db_digest: String,
    pub(crate) table_names: Vec<(String, String)>,
    pub(crate) hints: HintsSet,
}

/// Go's `sessionBindingHandle`: one binding per normalized origin statement,
/// replaced wholesale when the same statement is bound again.
///
/// A `BTreeMap` rather than Go's `map`: `SHOW BINDINGS` sorts by update time
/// then create time, and both are equal for two bindings created in the same
/// millisecond, so the map's own order is the tie-break. Go's is random;
/// ordering by digest at least makes this tier's answer reproducible.
#[derive(Debug, Default)]
pub(crate) struct SessionBindings {
    bindings: BTreeMap<String, Binding>,
}

impl SessionBindings {
    /// Go `CreateSessionBinding`: replaces any binding for the same
    /// normalized statement.
    pub(crate) fn create(&mut self, binding: Binding) {
        self.bindings.insert(binding.sql_digest.clone(), binding);
    }

    /// Go `DropSessionBinding`. Deleting an absent digest is not an error
    /// there and is not one here (measured: a `DROP SESSION BINDING` for a
    /// statement with no binding answers OK on real TiDB).
    pub(crate) fn drop_digest(&mut self, digest: &str) -> bool {
        self.bindings.remove(digest).is_some()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    /// Go `fetchShowBind`'s order: descending update time, then descending
    /// create time.
    pub(crate) fn all_sorted(&self) -> Vec<&Binding> {
        let mut all: Vec<&Binding> = self.bindings.values().collect();
        all.sort_by(|left, right| {
            right
                .update_time
                .cmp(&left.update_time)
                .then_with(|| right.create_time.cmp(&left.create_time))
        });
        all
    }

    /// Go `MatchSessionBinding`: every binding sharing the statement's no-DB
    /// digest is a candidate, and `crossDBMatchBindings` picks among them.
    pub(crate) fn match_statement(
        &self,
        no_db_digest: &str,
        table_names: &[(String, String)],
        current_db: &str,
    ) -> Option<&Binding> {
        let candidates = self
            .bindings
            .values()
            .filter(|binding| binding.no_db_digest == no_db_digest);
        cross_db_match(candidates, table_names, current_db)
    }
}

/// Go `crossDBMatchBindings`, with `EnableFuzzyBinding` at its default OFF:
/// a binding whose table list contains a `*` wildcard schema is skipped, so
/// the "fewest wildcards" tie-break can only ever select a zero-wildcard
/// binding. Kept in this shape anyway because the wildcard schema is written
/// by `prepareHints` for a cross-DB binding, and this tier creates none.
fn cross_db_match<'a>(
    candidates: impl Iterator<Item = &'a Binding>,
    table_names: &[(String, String)],
    current_db: &str,
) -> Option<&'a Binding> {
    let mut least_wildcards = table_names.len() + 1;
    let mut matched = None;
    for binding in candidates {
        // Go `Binding.IsBindingEnabled`: `using` is the legacy spelling of
        // `enabled`; `disabled` (and everything else) never matches.
        if binding.status != STATUS_ENABLED && binding.status != STATUS_USING {
            continue;
        }
        let Some(wildcards) =
            cross_db_match_table_names(current_db, table_names, &binding.table_names)
        else {
            continue;
        };
        // Cross-DB bindings are off by default (`tidb_opt_enable_fuzzy_binding`).
        if wildcards > 0 {
            continue;
        }
        if wildcards < least_wildcards {
            least_wildcards = wildcards;
            matched = Some(binding);
        }
    }
    matched
}

/// Go `crossDBMatchBindingTableName`. `None` is Go's `matched == false`.
fn cross_db_match_table_names(
    current_db: &str,
    stmt_tables: &[(String, String)],
    binding_tables: &[(String, String)],
) -> Option<usize> {
    if stmt_tables.len() != binding_tables.len() {
        return None;
    }
    let current_db = current_db.to_lowercase();
    let mut wildcards = 0;
    for ((stmt_schema, stmt_table), (binding_schema, binding_table)) in
        stmt_tables.iter().zip(binding_tables)
    {
        if stmt_table != binding_table {
            return None;
        }
        if binding_schema == "*" {
            wildcards += 1;
        }
        if binding_schema == stmt_schema
            || (stmt_schema.is_empty() && *binding_schema == current_db)
            || binding_schema == "*"
        {
            continue;
        }
        return None;
    }
    Some(wildcards)
}

/// Go `bindinfo.StatusEnabled`.
pub(crate) const STATUS_ENABLED: &str = "enabled";
/// Go `bindinfo.StatusDisabled`: listed by `SHOW BINDINGS`, skipped by the
/// plan-time match.
pub(crate) const STATUS_DISABLED: &str = "disabled";
/// Go `bindinfo.StatusUsing`, the pre-v6 spelling of `enabled`, still
/// accepted by `SET BINDING DISABLED`'s status guard and by the match.
pub(crate) const STATUS_USING: &str = "using";
/// Go `bindinfo.SourceManual`.
pub(crate) const SOURCE_MANUAL: &str = "manual";

/// Go `utilparser.GetDefaultDB`: the schema the FIRST table name in the
/// statement carries, or the session's current database when it carries
/// none.
pub(crate) fn default_db_of(stmt: &Stmt, current_db: &str) -> String {
    collect_table_names(stmt)
        .into_iter()
        .find_map(|(schema, _)| (!schema.is_empty()).then_some(schema))
        .unwrap_or_else(|| current_db.to_lowercase())
}

/// Go's `preprocessor`'s binding check: the origin and the hinted statement
/// must normalize to the SAME text once the hints are erased.
///
/// The message is Go's own, verbatim, including its `originSQL:`/`hintedSQL:`
/// spelling with no space after the colon (captured from real TiDB:
/// `Error|1105|hinted sql and origin sql don't match when hinted sql erase
/// the hint info, after erase hint info, originSQL:..., hintedSQL:...`).
pub(crate) fn check_origin_matches_hinted(
    origin_normalized: &str,
    hinted_normalized: &str,
) -> Result<(), DriverError> {
    if origin_normalized == hinted_normalized {
        return Ok(());
    }
    Err(DriverError::BindingHintedSqlMismatch {
        origin: origin_normalized.to_owned(),
        hinted: hinted_normalized.to_owned(),
    })
}
