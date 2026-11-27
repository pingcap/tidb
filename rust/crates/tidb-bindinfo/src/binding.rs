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
//! # Ownership
//!
//! Session bindings live in a private map. Global bindings are persisted in
//! `mysql.bind_info`; cluster sessions match a node-owned committed cache,
//! refreshed independently of user transactions. Standalone in-memory
//! sessions read their shared catalog. This module supplies matching and
//! decoding, not a complete transcreation claim for `pkg/bindinfo`.
//!
//! # What a match changes here
//!
//! The binding's hints replace the statement's. Both table-level index hints
//! and `/*+ ... */` optimizer hints then enter the same planner hint path as
//! hints written directly in the statement.

use std::any::Any;
use std::collections::BTreeMap;

use tidb_ast::{Stmt, Visitable as _, Visitor};
use tidb_driver_error::DriverError;
use tidb_hack::GoCaseCompare;
use tidb_hack::go_to_lower;

use tidb_hint::HintsSet;

/// Go `utilparser.RestoreWithDefaultDB`: the statement's canonical text with
/// every unqualified table name qualified by `default_db`. Delegates to the
/// complete `pkg/util/parser` port; Go additionally passes `node.Text()` so
/// `SimpleCases` can keep the user's own formatting, but this AST does not
/// retain the statement's raw text, so the full-restore path is always taken
/// (same output as before this delegation).
pub fn restore_with_default_db(stmt: &Stmt, default_db: &str) -> String {
    tidb_parser::util_parser::restore_with_default_db(stmt, default_db, "")
}

/// Go `utilparser.RestoreWithoutDB`: the same text with every schema
/// qualifier ERASED, which is what makes a binding portable across databases.
pub fn restore_without_db(stmt: &Stmt) -> String {
    tidb_parser::util_parser::restore_without_db(stmt)
}

/// Go `bindinfo.NormalizeStmtForBinding`, the with-DB half: the normalized
/// text and its digest.
pub fn normalize_stmt_for_binding(stmt: &Stmt, default_db: &str) -> (String, String) {
    let (normalized, digest) =
        tidb_parser::normalize_digest_for_binding(&restore_with_default_db(stmt, default_db));
    (normalized, digest.as_str().to_owned())
}

/// Go `bindinfo.NormalizeStmtForBinding`, the no-DB half: the digest a match
/// is decided on.
pub fn no_db_digest(stmt: &Stmt) -> String {
    let (_, digest) = tidb_parser::normalize_digest_for_binding(&restore_without_db(stmt));
    digest.as_str().to_owned()
}

/// Go `hint.CollectHint`.
pub fn collect_hints(stmt: &Stmt) -> HintsSet {
    tidb_hint::collect_hint(stmt)
}

/// Go `hint.BindHint`: replaces the statement's hints with the set's, block
/// for block. A block beyond the set's length has its hints CLEARED, which is
/// Go's `setTableHints4StmtNode(in, nil)` -- the binding decides every block's
/// hints, not just the ones it has something to say about.
pub fn bind_hints(stmt: &mut Stmt, set: &HintsSet) {
    tidb_hint::bind_hint(stmt, set);
}

/// Collects `(schema, table)` in traversal order, lowercased, which is Go's
/// `bindinfo.CollectTableNames` plus the `.L` its comparisons use.
struct TableNameCollector {
    names: Vec<(String, String)>,
}

impl TableNameCollector {
    fn push(&mut self, path: &[String]) {
        let (schema, table) = match path {
            [table] => (String::new(), table.clone()),
            [schema, table] => (schema.clone(), table.clone()),
            _ => return,
        };
        self.names.push((go_to_lower(schema), go_to_lower(table)));
    }
}

impl Visitor for TableNameCollector {
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        if let Some(table_ref) = node.downcast_mut::<tidb_ast::TableRef>() {
            let path = table_ref.name.clone();
            self.push(&path);
        }
        false
    }

    fn leave(&mut self, node: &mut dyn Any) -> bool {
        // Go `InsertStmt.Accept` visits the SELECT source, then the target
        // `TableName`, which this AST keeps as a bare path.
        if let Some(insert) = node.downcast_mut::<tidb_ast::InsertStmt>() {
            let path = insert.table.clone();
            self.push(&path);
        }
        true
    }
}

/// Go `bindinfo.CollectTableNames`.
pub fn collect_table_names(stmt: &Stmt) -> Vec<(String, String)> {
    let mut stmt = stmt.clone();
    let mut collector = TableNameCollector { names: Vec::new() };
    stmt.accept(&mut collector);
    collector.names
}

/// Go's shared binding, including stored plan digest and live usage times.
///
/// `Default` is derived so [`crate::binding_cache`]'s ported Go tests can
/// build the same partially-filled literals they do (`&Binding{BindSQL: ...,
/// SQLDigest: ...}`); Go's zero value leaves every other field empty, and so
/// does this one.
#[derive(Debug, Clone, Default)]
pub struct Binding {
    /// The NORMALIZED origin statement, DB-qualified. This is the text
    /// `SHOW BINDINGS` prints first, not the SQL the user typed.
    pub original_sql: String,
    /// The hinted statement, restored and DB-qualified.
    pub bind_sql: String,
    /// Go `GetDefaultDB`: the schema the origin statement resolves against.
    pub db: String,
    /// `enabled` for every binding this tier creates; `SET BINDING DISABLED`
    /// is not modelled, so the column is constant rather than absent.
    pub status: &'static str,
    pub charset: String,
    pub collation: String,
    /// Go `bindinfo.SourceManual`.
    pub source: &'static str,
    /// Digest of [`Self::original_sql`], and the store's own key.
    pub sql_digest: String,
    pub plan_digest: String,
    pub usage: std::sync::Arc<std::sync::Mutex<BindingUsageTimes>>,
    pub create_time: String,
    pub update_time: String,
    /// Digest of the HINTED statement restored WITHOUT its schema, which is
    /// Go's `noDBDigestFromBinding` -- it parses `BindSQL`, not the origin.
    /// Equal to the origin's own no-DB digest whenever the pair is legal,
    /// because the only legal difference between them is hints and the
    /// normalizer erases those.
    pub no_db_digest: String,
    pub table_names: Vec<(String, String)>,
    pub hints: HintsSet,
}

/// Mutable usage belongs to a shared binding, so reloads of the same version
/// retain activity recorded by planners holding that binding reference.
#[derive(Debug, Default)]
pub struct BindingUsageTimes {
    last_used_at: Option<chrono::DateTime<chrono::Utc>>,
    last_saved_at: Option<chrono::DateTime<chrono::Utc>>,
}


impl Binding {
    /// Go UpdateLastUsedAt on a successful global match.
    pub fn mark_used(&self) {
        self.usage.lock().unwrap().last_used_at = Some(chrono::Utc::now());
    }
    /// Snapshot fields required by Go's batched usage writer.
    pub fn usage_snapshot(&self) -> crate::binding_utils::UsageInfo {
        let usage = self.usage.lock().unwrap();
        crate::binding_utils::UsageInfo {
            sql_digest: self.sql_digest.clone(),
            plan_digest: self.plan_digest.clone(),
            last_used_at: usage.last_used_at,
            last_saved_at: usage.last_saved_at,
        }
    }
    /// Publish a successful storage write without losing newer planner usage.
    pub fn mark_usage_saved(&self, saved: chrono::DateTime<chrono::Utc>) {
        self.usage.lock().unwrap().last_saved_at = Some(saved);
    }
    /// Decode the stored binding projection used by both catalog and node loaders.
    /// Invalid SQL and builtin rows are ignored; tombstones reach cache resolution.
    pub fn from_storage_row(
        row: &[tidb_datatype::Datum],
        mode: tidb_lexer::SqlMode,
    ) -> Option<Self> {
        let text = |index: usize| tidb_datatype::datum_text::sysvar_datum_text(row.get(index)?);
        let status = match text(3)?.as_str() {
            STATUS_ENABLED => STATUS_ENABLED,
            STATUS_USING => STATUS_USING,
            STATUS_DISABLED => STATUS_DISABLED,
            STATUS_DELETED => STATUS_DELETED,
            _ => return None,
        };
        let bind_sql = text(1)?;
        let hinted = tidb_parser::parse_with_sql_mode(&bind_sql, mode).ok()?;
        Some(Self {
            original_sql: text(0)?,
            bind_sql,
            db: text(2)?,
            status,
            charset: text(6).unwrap_or_default(),
            collation: text(7).unwrap_or_default(),
            source: crate::binding_utils::binding_source(
                text(8).unwrap_or_default().as_str(),
            ),
            sql_digest: text(9)?,
            plan_digest: text(10).unwrap_or_default(),
            usage: Default::default(),
            create_time: text(4).unwrap_or_default(),
            update_time: text(5).unwrap_or_default(),
            no_db_digest: no_db_digest(&hinted),
            table_names: collect_table_names(&hinted),
            hints: collect_hints(&hinted),
        })
    }
}

/// Go's `sessionBindingHandle`: one binding per normalized origin statement,
/// replaced wholesale when the same statement is bound again.
///
/// A `BTreeMap` rather than Go's `map`: `SHOW BINDINGS` sorts by update time
/// then create time, and both are equal for two bindings created in the same
/// millisecond, so the map's own order is the tie-break. Go's is random;
/// ordering by digest at least makes this tier's answer reproducible.
#[derive(Debug, Default)]
pub struct SessionBindings {
    bindings: BTreeMap<String, Binding>,
}

impl SessionBindings {
    /// Go `CreateSessionBinding`: replaces any binding for the same
    /// normalized statement.
    pub fn create(&mut self, binding: Binding) {
        self.bindings.insert(binding.sql_digest.clone(), binding);
    }

    /// Go `DropSessionBinding`. Deleting an absent digest is not an error
    /// there and is not one here (measured: a `DROP SESSION BINDING` for a
    /// statement with no binding answers OK on real TiDB).
    pub fn drop_session_binding(&mut self, digest: &str) -> bool {
        self.bindings.remove(digest).is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    /// Go `GetAllSessionBindings`.
    pub fn get_all_session_bindings(&self) -> Vec<&Binding> {
        self.bindings.values().collect()
    }

    /// Go `fetchShowBind`'s order: descending update time, then descending
    /// create time.
    pub fn all_sorted(&self) -> Vec<&Binding> {
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
    pub fn match_session_binding(
        &self,
        no_db_digest: &str,
        table_names: &[(String, String)],
        current_db: &str,
        fuzzy_enabled: bool,
    ) -> Option<&Binding> {
        let candidates = self
            .bindings
            .values()
            .filter(|binding| binding.no_db_digest == no_db_digest);
        cross_db_match_bindings(candidates, table_names, current_db, fuzzy_enabled)
    }
}

/// Go `crossDBMatchBindings`: `fuzzy_enabled` is
/// `@@tidb_opt_enable_fuzzy_binding` (default OFF). A `*`-schema binding is
/// CREATED regardless of the switch but MATCHES only under it -- measured:
/// the same wildcard binding answers `@@last_plan_from_binding` 1 with the
/// switch on and 0 with it off, flipping live with `SET`. Among the
/// candidates the fewest-wildcards binding wins, Go's own tie-break.
pub fn cross_db_match_bindings<'a>(
    candidates: impl Iterator<Item = &'a Binding>,
    table_names: &[(String, String)],
    current_db: &str,
    fuzzy_enabled: bool,
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
        if wildcards > 0 && !fuzzy_enabled {
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
    let current_db = go_to_lower(current_db);
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
pub const STATUS_ENABLED: &str = "enabled";
/// Go `bindinfo.StatusDisabled`: listed by `SHOW BINDINGS`, skipped by the
/// plan-time match.
pub const STATUS_DISABLED: &str = "disabled";
pub const STATUS_DELETED: &str = "deleted";
/// Go `bindinfo.StatusUsing`, the pre-v6 spelling of `enabled`, still
/// accepted by `SET BINDING DISABLED`'s status guard and by the match.
pub const STATUS_USING: &str = "using";
/// Go `bindinfo.StatusBuiltin` indicates the binding is a builtin record for
/// internal locking purpose. It is also the status for the builtin binding.
pub const STATUS_BUILTIN: &str = "builtin";
/// Go `bindinfo.SourceManual`.
pub const SOURCE_MANUAL: &str = "manual";
/// Go `bindinfo.SourceHistory`: a binding created `FROM HISTORY`.
pub const SOURCE_HISTORY: &str = "history";

/// Go `utilparser.GetDefaultDB`: the schema the FIRST table name in the
/// statement carries, or the session's current database when it carries
/// none.
pub fn get_default_db(stmt: &Stmt, current_db: &str) -> String {
    collect_table_names(stmt)
        .into_iter()
        .find_map(|(schema, _)| (!schema.is_empty()).then_some(schema))
        .unwrap_or_else(|| go_to_lower(current_db))
}

/// Go's `preprocessor`'s binding check: the origin and the hinted statement
/// must normalize to the SAME text once the hints are erased.
///
/// The message is Go's own, verbatim, including its `originSQL:`/`hintedSQL:`
/// spelling with no space after the colon (captured from real TiDB:
/// `Error|1105|hinted sql and origin sql don't match when hinted sql erase
/// the hint info, after erase hint info, originSQL:..., hintedSQL:...`).
pub fn check_origin_matches_hinted(
    origin_normalized: &str,
    hinted_normalized: &str,
) -> Result<(), DriverError> {
    if origin_normalized == hinted_normalized {
        return Ok(());
    }
    Err(DriverError::from(tidb_terror::MysqlError::unknown(format!("hinted sql and origin sql don't match when hinted sql erase the hint info, after erase hint info, originSQL:{}, hintedSQL:{}", origin_normalized.to_owned(), hinted_normalized.to_owned()))))
}

/// Persisted binding projection. Cluster sessions match against a node-owned
/// committed cache; standalone in-memory sessions read their local catalog.
pub const BIND_INFO_COLUMNS: &str = "original_sql, bind_sql, default_db, status, create_time, \
     update_time, charset, collation, source, sql_digest";

/// Go `bindinfo.mayHaveSQLBinding`: INSERT/REPLACE value forms have no
/// binding-capable SELECT source. Keep the filter at the matcher boundary so
/// stored bindings remain visible to administrative operations while ordinary
/// DML and EXPLAIN do not report a false match.
#[must_use]
pub fn may_have_sql_binding(stmt: &Stmt) -> bool {
    match stmt {
        // REPLACE is an InsertStmt too; bindings support only its SELECT form.
        Stmt::Dml(dml) => match &**dml {
            tidb_ast::DmlStmt::Insert(insert) => insert.source.is_some(),
            _ => true,
        },
        // An EXPLAIN without a statement (EXPLAIN <plan_digest>) keeps the old
        // behavior and is no early-negative filter.
        Stmt::Admin(admin) => match &**admin {
            tidb_ast::AdminStmt::Explain(explain) => explain.statement().is_none_or(may_have_sql_binding),
            _ => true,
        },
        _ => true,
    }
}

#[cfg(test)]
mod may_have_sql_binding_tests {
    use super::*;

    /// Go `TestMayHaveSQLBinding`.
    #[test]
    fn test_may_have_sql_binding() {
        assert!(may_have_sql_binding(&tidb_parser::parse("explain '0123456789abcdef'").unwrap()));
        for (sql, want) in [
            ("insert into t values (1)", false),
            ("insert into t values (1) on duplicate key update a = values(a)", false),
            ("insert into t set a = 1", false),
            ("replace into t values (1)", false),
            ("explain insert into t values (1)", false),
            ("insert into t select * from s", true),
            ("replace into t select * from s", true),
            ("explain insert into t select * from s", true),
            ("select * from t", true),
            ("update t set a = 1", true),
            ("delete from t where a = 1", true),
        ] {
            let stmt = tidb_parser::parse(sql).unwrap();
            assert_eq!(may_have_sql_binding(&stmt), want, "{sql}");
        }
    }
}

/// What Go `matchSQLBindingCore` reads off `sctx`.
pub trait BindingMatchContext {
    /// Go `SessionVars`' session binding handle.
    fn session_bindings(&self) -> &SessionBindings;
    /// Go `GetBindingHandle(sctx)`'s cache: `None` when the session reads
    /// `mysql.bind_info` itself (outside any domain), `Some(None)` for Go's
    /// nil handle.
    fn global_binding_cache(&self) -> Option<Option<crate::binding_cache::SharedBindingCache>>;
    /// The global bindings read from `mysql.bind_info`.
    ///
    /// # Errors
    ///
    /// The read's failure.
    fn load_global_bindings(&mut self) -> Result<SessionBindings, DriverError>;
    /// Go `SessionVars.CurrentDB`.
    fn current_db(&self) -> &str;
    /// Go `EnableFuzzyBinding`.
    fn fuzzy_binding_enabled(&self) -> bool;
    /// Go `vardef.EnableBindingUsage`.
    fn binding_usage_enabled(&self) -> bool;
}

/// Go `matchSQLBindingCore`: the session binding, else the global one,
/// matching the statement's no-DB digest and tables -- the matched
/// binding's hints and bind SQL.
pub fn match_sql_binding_core<C: BindingMatchContext + ?Sized>(
    ctx: &mut C,
    stmt: &Stmt,
) -> Option<(tidb_hint::HintsSet, String)> {
    let source = ctx.global_binding_cache();
    let global = source.clone().flatten().map(|cache| cache.load());
    let has_global = match (&source, &global) {
        (_, Some(cache)) => cache.size() != 0,
        // A session outside any domain reads `mysql.bind_info` itself;
        // Go's nil handle matches no global binding.
        (source, None) => source.is_none(),
    };
    if ctx.session_bindings().is_empty() && !has_global {
        return None;
    }
    let no_db_digest = no_db_digest(stmt);
    let table_names = collect_table_names(stmt);
    // Go `crossDBMatchBindings` reads `EnableFuzzyBinding` at MATCH
    // time, so flipping the switch changes what an existing wildcard
    // binding does without touching the row.
    let fuzzy_enabled = ctx.fuzzy_binding_enabled();
    let current_db = ctx.current_db().to_owned();
    // Session bindings shadow global ones, which is Go's order:
    // `getBindingFromSession` first, the domain handle only on a miss.
    let (hints, bind_sql) =
        match ctx.session_bindings().match_session_binding(&no_db_digest, &table_names, &current_db, fuzzy_enabled) {
            Some(matched) => (matched.hints.clone(), matched.bind_sql.clone()),
            None if global.is_some() => {
                let matched =
                    global.as_ref()?.matching_binding(&no_db_digest, &table_names, &current_db, fuzzy_enabled)?;
                if ctx.binding_usage_enabled() {
                    matched.mark_used();
                }
                (matched.hints.clone(), matched.bind_sql.clone())
            }
            None if source.is_some() => return None,
            None => {
                let global = ctx.load_global_bindings().ok()?;
                let matched = global.match_session_binding(&no_db_digest, &table_names, &current_db, fuzzy_enabled)?;
                (matched.hints.clone(), matched.bind_sql.clone())
            }
        };
    Some((hints, bind_sql))
}
