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

//! Go `pkg/bindinfo`, covering the single file `utils.go`.
//!
//! LABELING: a **COMPLETE port of one file**, and therefore still a **SEED for
//! the package**. `pkg/bindinfo` holds `binding.go`, `binding_handle.go`,
//! `binding_operator.go`, `binding_match.go`, `capture.go`,
//! `session_handle.go` and more; this module and [`crate::binding_cache`]
//! claim two of them. Two files do not make `pkg/bindinfo` transcreated.
//!
//! # What this file is
//!
//! `utils.go` is the storage-facing half of the binding subsystem: it turns a
//! statement plus a hint string into the SQL text that gets STORED as a
//! binding ([`generate_binding_sql`]), reads bindings back out of
//! `mysql.bind_info` ([`read_bindings_from_storage`],
//! [`new_binding_from_storage`]), and writes each binding's last-used
//! timestamp back ([`update_binding_usage_info_to_storage`]).
//!
//! The one genuinely tricky piece is [`generate_binding_sql`]: the hint has to
//! be spliced into the RESTORED text at the right keyword, and for a `WITH`
//! query the right keyword is the first `SELECT` *after* the CTE prefix, not
//! the one inside it.
//!
//! # Boundaries
//!
//! Everything `utils.go` does against a session is funnelled through one
//! narrow trait, [`InternalSqlRunner`], which stands in for four Go seams:
//!
//! * `// boundary:` `pkg/sessionctx.Context` + `util/sqlexec.SQLExecutor` /
//!   `RestrictedSQLExecutor` -- Go's `exec` (line 74) and `execRows`
//!   (line 80) become [`InternalSqlRunner::exec`] and
//!   [`InternalSqlRunner::exec_rows`].
//! * `// boundary:` `pkg/planner/core/resolve.ResultField` -- `execRows`
//!   returns `(rows, fields, err)` and no caller in this file reads `fields`,
//!   so the field list is dropped.
//! * `// boundary:` `pkg/util/chunk.Row` -- a storage row becomes
//!   `&[tidb_datatype::Datum]`, read through [`crate::datum_text`], which is
//!   what [`crate::Session`]'s own `mysql.bind_info` reader already uses.
//! * `// boundary:` `pkg/util.DestroyableSessionPool` -- `callWithSCtx`
//!   (line 41) borrows a session, optionally wraps it in a pessimistic
//!   transaction, and destroys rather than recycles it on error. This tier has
//!   ONE session and no pool, so [`call_with_runner`] keeps only the
//!   transaction wrapper (`BEGIN PESSIMISTIC` / `COMMIT` / `ROLLBACK`) and
//!   drops Get/Put/Destroy.
//!
//! # Narrowings
//!
//! * **`hint.HintsSet` / `hint.BindHint`** resolve to [`crate::binding`]'s own
//!   port of the same Go type, not to `pkg/util/hint` through `tidb-executor`.
//!   Same function, closer at hand.
//! * **`RestoreDBForBinding` (`binding.go:464`)** is
//!   [`crate::binding::restore_with_default_db`] -- already ported, reused
//!   rather than duplicated. Go passes `node.Text()` so its `SimpleCases` fast
//!   path can echo the user's own spelling; this AST does not retain raw text,
//!   so the full-restore path always runs.
//! * **The `WITH` branch of `GenerateBindingSQL` (lines 112-125).** Go
//!   restores the `With` clause on its own to learn where it ends, then
//!   replaces the first `SELECT` after that offset.
//!   `tidb_ast::WithClause`'s restore is `pub(crate)` and unreachable from
//!   here, so this port instead splices at the first `SELECT` that is at paren
//!   depth zero and outside any quoted token -- the same position, since every
//!   CTE body is parenthesised. The non-`WITH` branches keep Go's own naive
//!   `strings.Index`, verbatim.
//! * **`Binding.PlanDigest` and `Binding.UsageInfo`.** [`Binding`] carries
//!   neither (see its own doc), so [`new_binding_from_storage`] drops the
//!   `plan_digest` column, and the usage writer operates on a standalone
//!   [`BindingUsage`] record instead of Go's `atomic.Pointer[time.Time]`
//!   fields hanging off the binding.
//! * **`Binding.Status` / `Binding.Source` are `&'static str`** in this crate,
//!   so a storage row carrying an unrecognised status is SKIPPED (Go keeps the
//!   raw text) and an unrecognised source falls back to `manual`. This matches
//!   [`crate::Session`]'s existing `mysql.bind_info` loader.
//! * **`time.Since`** becomes an explicit `now` parameter on
//!   [`should_update_binding`] and [`update_binding_usage_info_to_storage`],
//!   so the interval rule is testable without a clock.
//! * **`%?`** is kept verbatim in the generated SQL: it is TiDB's
//!   internal-SQL placeholder spelling, and keeping it makes the statement
//!   text byte-comparable with Go. An implementation of
//!   [`InternalSqlRunner`] bridging to `tidb_executor::bind_parameters` must
//!   translate it to `?`.
//! * **`bindingLogger()`** (line 87) and every `zap` call are dropped by name;
//!   this workspace has no structured logger at this seam. `terror.Log` on the
//!   rollback error is dropped with it.
//! * **`intest.Assert`** is dropped; the assertion it makes (a batched usage
//!   record always has a `LastUsedAt`) is a type invariant here instead.
//!
//! # Skipped
//!
//! * **`getBindingPlanDigest` (lines 316-347).** It needs
//!   `CalculatePlanDigest` and a planner that emits plan digests; this tier
//!   emits none, which is also why [`Binding`] has no `plan_digest` field and
//!   why `SHOW BINDINGS` prints an empty `Plan_digest` column.
//! * **`hasParam` (`binding.go:560`)**, reached only from
//!   `getBindingPlanDigest`, goes with it.

use std::time::Duration;

use chrono::{DateTime, Utc};
use tidb_ast::{DmlStmt, QueryStmt, Stmt};
use tidb_datatype::Datum;
use tidb_executor::DriverError;

use crate::binding::{Binding, HintsSet, STATUS_DISABLED, STATUS_ENABLED, STATUS_USING};

/// Go `bindinfo.BuiltinPseudoSQL4BindLock` (`binding_handle.go:34`). Declared
/// in another file of the package; inlined here because
/// [`read_bindings_from_storage`] compares against it.
pub const BUILTIN_PSEUDO_SQL_4_BIND_LOCK: &str = "builtin_pseudo_sql_for_bind_lock";

/// Go `bindinfo.LockBindInfoSQL` (`binding_handle.go:37`), issued by
/// `lockBindInfoTable` (`binding_operator.go:267`) -- both in other files of
/// the package, inlined here because this file's usage writer calls them.
pub const LOCK_BIND_INFO_SQL: &str =
    "UPDATE mysql.bind_info SET source= 'builtin' WHERE original_sql= 'builtin_pseudo_sql_for_bind_lock'";

/// Go `UpdateBindingUsageInfoBatchSize` (line 175).
pub const UPDATE_BINDING_USAGE_INFO_BATCH_SIZE: usize = 100;

/// Go `MaxWriteInterval` (line 177): how long a binding may go unsaved before
/// a use forces a write.
pub const MAX_WRITE_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

/// The columns `readBindingsFromStorage` selects (line 146), in order.
pub const BINDING_STORAGE_COLUMNS: &str = "original_sql, bind_sql, default_db, status, create_time,\n       update_time, charset, collation, source, sql_digest, plan_digest";

/// `// boundary:` one internal SQL statement against the session, standing in
/// for `sessionctx.Context` + `util/sqlexec`. See the module header.
pub trait InternalSqlRunner {
    /// Go `exec` (line 74): run a statement, keep only the affected-row count
    /// (no caller in this file reads the returned `RecordSet`).
    fn exec(&mut self, sql: &str, args: &[Datum]) -> Result<u64, DriverError>;

    /// Go `execRows` (line 80): run a query, keep only the rows (no caller in
    /// this file reads the `[]*resolve.ResultField`).
    fn exec_rows(&mut self, sql: &str, args: &[Datum]) -> Result<Vec<Vec<Datum>>, DriverError>;
}

/// Go `callWithSCtx` (lines 41-71), minus the session pool.
///
/// With `wrap_txn` the body runs inside `BEGIN PESSIMISTIC` and is committed
/// on success or rolled back on failure; the rollback's own error is
/// swallowed, as Go swallows it into `terror.Log`.
pub fn call_with_runner<R, T>(
    runner: &mut R,
    wrap_txn: bool,
    body: impl FnOnce(&mut R) -> Result<T, DriverError>,
) -> Result<T, DriverError>
where
    R: InternalSqlRunner + ?Sized,
{
    if !wrap_txn {
        return body(runner);
    }
    runner.exec("BEGIN PESSIMISTIC", &[])?;
    match body(runner) {
        Ok(value) => {
            runner.exec("COMMIT", &[])?;
            Ok(value)
        }
        Err(err) => {
            let _ = runner.exec("ROLLBACK", &[]);
            Err(err)
        }
    }
}

/// Go `GenerateBindingSQL` (lines 91-143): the statement's own hints are
/// ERASED and `plan_hint` is injected in their place, producing the text that
/// gets stored as `bind_sql`.
///
/// Returns `""` for a statement kind Go's switch does not cover (Go logs
/// "unexpected statement type when generating bind SQL" and returns `""`), and
/// for the two shapes noted in the module header.
#[must_use]
pub fn generate_binding_sql(stmt: &Stmt, plan_hint: &str, default_db: &str) -> String {
    // "We need to evolve plan based on the current sql, not the original sql
    // which may have different parameters. So here we would remove the hint
    // and inject the current best plan hint."
    let mut stmt = stmt.clone();
    crate::binding::bind_hints(&mut stmt, &HintsSet::default());
    let bind_sql = crate::binding::restore_with_default_db(&stmt, default_db);
    if bind_sql.is_empty() {
        return String::new();
    }

    // Go's `switch` is over the statement node itself. A `WITH ... <DML>` is
    // one wrapper node in this AST and a `With` field on the DML node in Go,
    // so unwrapping it lands on the same case Go takes.
    let mut node: &DmlStmt;
    match &stmt {
        Stmt::Dml(dml) => {
            node = dml;
            while let DmlStmt::With { statement, .. } = node {
                node = statement;
            }
            match node {
                DmlStmt::Delete(_) => splice_after_keyword(&bind_sql, "DELETE", plan_hint),
                DmlStmt::Update(_) => splice_after_keyword(&bind_sql, "UPDATE", plan_hint),
                // Go slices from `REPLACE`/`INSERT` to drop a possible
                // `explain` prefix, but then injects the hint at the first
                // `SELECT` -- an `INSERT ... VALUES` therefore gets no hint at
                // all, which is Go's behaviour, not an omission here.
                DmlStmt::Insert(insert) => {
                    let head = if insert.replace { "REPLACE" } else { "INSERT" };
                    let Some(start) = bind_sql.find(head) else {
                        return String::new();
                    };
                    replace_first(&bind_sql[start..], "SELECT", &hinted("SELECT", plan_hint))
                }
                DmlStmt::With { .. } => unreachable!("the loop above unwraps every With node"),
                _ => String::new(),
            }
        }
        Stmt::Query(query) => match query.as_ref() {
            QueryStmt::Select(select) if select.with.is_some() => {
                let Some(with_idx) = bind_sql.find("WITH") else {
                    return String::new();
                };
                // Go computes this offset by restoring the CTE prefix and
                // measuring it; see the module header for why this scans
                // instead.
                let Some(select_idx) = top_level_select_index(&bind_sql[with_idx..]) else {
                    return String::new();
                };
                let tail = &bind_sql[with_idx + select_idx..];
                let mut out = bind_sql[with_idx..with_idx + select_idx].to_owned();
                out.push_str(&replace_first(tail, "SELECT", &hinted("SELECT", plan_hint)));
                out
            }
            QueryStmt::Select(_) => splice_after_keyword(&bind_sql, "SELECT", plan_hint),
            // Go's switch has no `*ast.SetOprStmt` case.
            QueryStmt::SetOpr(_) => String::new(),
        },
        _ => String::new(),
    }
}

/// Go's per-case body: drop everything before `keyword` (a possible `explain`
/// prefix) and inject the hint at its first occurrence.
///
/// Go indexes unconditionally and would panic on a missing keyword; returning
/// `""` here is the same answer its `default` branch gives for a statement it
/// cannot handle.
fn splice_after_keyword(bind_sql: &str, keyword: &str, plan_hint: &str) -> String {
    let Some(start) = bind_sql.find(keyword) else {
        return String::new();
    };
    replace_first(&bind_sql[start..], keyword, &hinted(keyword, plan_hint))
}

/// Go's `fmt.Sprintf("%s /*+ %s*/", keyword, planHint)` -- no space before the
/// closing `*/`, exactly as written there.
fn hinted(keyword: &str, plan_hint: &str) -> String {
    format!("{keyword} /*+ {plan_hint}*/")
}

/// Go `strings.Replace(s, old, new, 1)`.
fn replace_first(haystack: &str, needle: &str, replacement: &str) -> String {
    match haystack.find(needle) {
        Some(at) => {
            let mut out = haystack[..at].to_owned();
            out.push_str(replacement);
            out.push_str(&haystack[at + needle.len()..]);
            out
        }
        None => haystack.to_owned(),
    }
}

/// The byte offset of the first `SELECT` at parenthesis depth zero and outside
/// any quoted token: the start of the query a `WITH` clause prefixes.
///
/// Restored SQL spells keywords in upper case and quotes identifiers with
/// backticks, so the scan only has to respect `` ` ``, `'` and `"` (with
/// backslash escapes inside the latter two) and paren nesting.
fn top_level_select_index(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut quote: Option<u8> = None;
    let mut i = 0usize;
    while i < bytes.len() {
        let byte = bytes[i];
        if let Some(open) = quote {
            if byte == b'\\' && open != b'`' {
                i += 2;
                continue;
            }
            if byte == open {
                quote = None;
            }
            i += 1;
            continue;
        }
        match byte {
            b'`' | b'\'' | b'"' => quote = Some(byte),
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'S' if depth == 0 && text[i..].starts_with("SELECT") => return Some(i),
            _ => {}
        }
        i += 1;
    }
    None
}

/// Go's `selectStmt` in `readBindingsFromStorage` (lines 146-148).
#[must_use]
pub fn read_bindings_sql(condition: &str) -> String {
    format!("SELECT {BINDING_STORAGE_COLUMNS} FROM mysql.bind_info\n       {condition}")
}

/// Go `readBindingsFromStorage` (lines 145-171).
///
/// The builtin lock row is skipped, and a row whose `bind_sql` no longer
/// parses is skipped too -- that is Go's `prepareHints` failure path
/// (`binding.go:364`, another file of the package), which logs and drops the
/// row. Here the same parse is what derives the matcher's inputs.
pub fn read_bindings_from_storage<R>(
    runner: &mut R,
    condition: &str,
    args: &[Datum],
) -> Result<Vec<Binding>, DriverError>
where
    R: InternalSqlRunner + ?Sized,
{
    let sql = read_bindings_sql(condition);
    call_with_runner(runner, false, |runner| {
        let rows = runner.exec_rows(&sql, args)?;
        let mut bindings = Vec::with_capacity(rows.len());
        for row in &rows {
            if row.first().and_then(crate::datum_text).as_deref()
                == Some(BUILTIN_PSEUDO_SQL_4_BIND_LOCK)
            {
                continue;
            }
            let Some(binding) = new_binding_from_storage(row) else {
                continue;
            };
            bindings.push(binding);
        }
        Ok(bindings)
    })
}

/// Go `newBindingFromStorage` (lines 294-314), plus the `prepareHints`
/// derivation `readBindingsFromStorage` runs immediately after it.
///
/// `None` is Go's "failed to generate bind record from data row": a missing
/// column, an unrepresentable status, or a `bind_sql` that no longer parses.
#[must_use]
pub fn new_binding_from_storage(row: &[Datum]) -> Option<Binding> {
    let text = |index: usize| row.get(index).and_then(crate::datum_text);
    // "For compatibility, the 'Using' status binding will be converted to the
    // 'Enabled' status binding."
    let status = match text(3)?.as_str() {
        s if s == STATUS_ENABLED || s == STATUS_USING => STATUS_ENABLED,
        s if s == STATUS_DISABLED => STATUS_DISABLED,
        _ => return None,
    };
    let original_sql = text(0)?;
    let bind_sql = text(1)?;
    let stmt = tidb_parser::parse(&bind_sql).ok()?;
    Some(Binding {
        original_sql,
        // Go lowercases the schema on the way out of storage as well as in.
        db: text(2)?.to_lowercase(),
        status,
        create_time: text(4).unwrap_or_default(),
        update_time: text(5).unwrap_or_default(),
        charset: text(6).unwrap_or_default(),
        collation: text(7).unwrap_or_default(),
        source: binding_source(text(8).unwrap_or_default().as_str()),
        sql_digest: text(9).unwrap_or_default(),
        no_db_digest: crate::binding::no_db_digest(&stmt),
        table_names: crate::binding::collect_table_names(&stmt),
        hints: crate::binding::collect_hints(&stmt),
        bind_sql,
    })
}

/// The `source` column narrowed to this crate's `&'static str` set. Go keeps
/// the raw column text; an unrecognised value falls back to `manual`, which is
/// the source every binding this tier creates carries anyway.
fn binding_source(source: &str) -> &'static str {
    match source {
        "capture" => "capture",
        "evolve" => "evolve",
        "history" => "history",
        "builtin" => "builtin",
        _ => crate::binding::SOURCE_MANUAL,
    }
}

/// One binding's usage counters, Go's `Binding.UsageInfo` (`binding.go:117`)
/// lifted out of the binding -- see the module narrowings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingUsage {
    /// Go `Binding.SQLDigest`.
    pub sql_digest: String,
    /// Go `Binding.PlanDigest`; `""` means the stored column is `NULL`.
    pub plan_digest: String,
    /// Go `UsageInfo.LastUsedAt`: `None` when the binding was never used.
    pub last_used_at: Option<DateTime<Utc>>,
    /// Go `UsageInfo.LastSavedAt`: `None` when it was never written back.
    pub last_saved_at: Option<DateTime<Utc>>,
}

/// Go `shouldUpdateBinding` (lines 216-224).
///
/// A binding that has never been saved is always written. Otherwise it is
/// written only once [`MAX_WRITE_INTERVAL`] has passed since the last save AND
/// it has been used in between.
#[must_use]
pub fn should_update_binding(
    now: DateTime<Utc>,
    last_saved: Option<DateTime<Utc>>,
    last_used: DateTime<Utc>,
) -> bool {
    let Some(last_saved) = last_saved else {
        // "If it has never been written before, it will be written."
        return true;
    };
    let elapsed = now
        .signed_duration_since(last_saved)
        .to_std()
        .unwrap_or_default();
    elapsed >= MAX_WRITE_INTERVAL && last_used > last_saved
}

/// One row of a usage-write batch: Go passes whole `*Binding`s, but the
/// statements only ever read these three values off them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct UsageWrite {
    sql_digest: String,
    plan_digest: String,
    last_used: DateTime<Utc>,
}

/// Go `updateBindingUsageInfoToStorage` (lines 180-214): batches the bindings
/// that are due a write and flushes every
/// [`UPDATE_BINDING_USAGE_INFO_BATCH_SIZE`] of them.
///
/// Returns Go's `cnt`, which there only feeds a log line. Bindings that were
/// written have their `last_saved_at` advanced to `now`, as Go's
/// `UpdateLastSavedAt` does.
pub fn update_binding_usage_info_to_storage<R>(
    runner: &mut R,
    usages: &mut [BindingUsage],
    now: DateTime<Utc>,
) -> Result<usize, DriverError>
where
    R: InternalSqlRunner + ?Sized,
{
    let mut to_write: Vec<usize> = Vec::with_capacity(UPDATE_BINDING_USAGE_INFO_BATCH_SIZE);
    let mut written: Vec<usize> = Vec::new();
    let mut count = 0usize;
    for index in 0..usages.len() {
        let Some(last_used) = usages[index].last_used_at else {
            continue;
        };
        if should_update_binding(now, usages[index].last_saved_at, last_used) {
            to_write.push(index);
            count += 1;
        }
        if to_write.len() == UPDATE_BINDING_USAGE_INFO_BATCH_SIZE {
            flush_usage_batch(runner, usages, &to_write, now)?;
            written.append(&mut to_write);
        }
    }
    if !to_write.is_empty() {
        flush_usage_batch(runner, usages, &to_write, now)?;
        written.append(&mut to_write);
    }
    for index in written {
        usages[index].last_saved_at = Some(now);
    }
    Ok(count)
}

/// Go `updateBindingUsageInfoToStorageInternal` (lines 226-254).
fn flush_usage_batch<R>(
    runner: &mut R,
    usages: &[BindingUsage],
    batch: &[usize],
    now: DateTime<Utc>,
) -> Result<(), DriverError>
where
    R: InternalSqlRunner + ?Sized,
{
    let writes: Vec<UsageWrite> = batch
        .iter()
        .map(|&index| UsageWrite {
            sql_digest: usages[index].sql_digest.clone(),
            plan_digest: usages[index].plan_digest.clone(),
            last_used: usages[index].last_used_at.unwrap_or(now),
        })
        .collect();
    call_with_runner(runner, true, |runner| {
        runner.exec(LOCK_BIND_INFO_SQL, &[])?;
        // "lockBindInfoTable is to prefetch the rows and lock them, it is good
        // for performance when there are many bindings to update with multi
        // tidb nodes."
        runner.exec(&add_lock_for_binds_sql(&writes), &[])?;
        for write in &writes {
            runner.exec(
                &save_binding_usage_sql(&write.plan_digest),
                &[
                    Datum::new_string(format_last_used(write.last_used)),
                    Datum::new_string(write.sql_digest.clone()),
                ],
            )?;
        }
        Ok(())
    })
}

/// Go `addLockForBinds` (lines 256-276).
///
/// Note the tuple ORDER: the `IN` list names `(plan_digest, sql_digest)` but
/// each literal is built as `('<sqlDigest>', <planDigest>)`. That inversion is
/// Go's, reproduced verbatim rather than "fixed" -- the statement exists to
/// take locks, and correcting it here would make this port stop matching what
/// TiDB actually sends.
fn add_lock_for_binds_sql(writes: &[UsageWrite]) -> String {
    let condition: Vec<String> = writes
        .iter()
        .map(|write| {
            if write.plan_digest.is_empty() {
                format!("('{}',NULL)", write.sql_digest)
            } else {
                format!("('{}','{}')", write.sql_digest, write.plan_digest)
            }
        })
        .collect();
    format!(
        "select 1 from mysql.bind_info use index(digest_index) where (plan_digest, sql_digest) in ({}) for update",
        condition.join(" , ")
    )
}

/// Go `saveBindingUsage`'s statement (lines 278-292). The `plan_digest`
/// predicate is `IS NULL` for an empty digest and an inline literal otherwise,
/// exactly as Go builds it.
#[must_use]
pub fn save_binding_usage_sql(plan_digest: &str) -> String {
    let mut sql = "UPDATE mysql.bind_info USE INDEX(digest_index) SET last_used_date = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE) WHERE sql_digest = %?".to_owned();
    if plan_digest.is_empty() {
        sql.push_str(" AND plan_digest IS NULL");
    } else {
        sql.push_str(&format!(" AND plan_digest = '{plan_digest}'"));
    }
    sql
}

/// Go `ts.UTC().Format(types.TimeFormat)` (line 279); `types.TimeFormat` is
/// `time.DateTime`, i.e. `2006-01-02 15:04:05`.
#[must_use]
pub fn format_last_used(ts: DateTime<Utc>) -> String {
    ts.format("%Y-%m-%d %H:%M:%S").to_string()
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    /// A recording [`InternalSqlRunner`]: the boundary has no real
    /// implementation in this tier, so the tests drive it directly.
    #[derive(Default)]
    struct RecordingRunner {
        statements: RefCell<Vec<String>>,
        rows: Vec<Vec<Datum>>,
    }

    impl InternalSqlRunner for RecordingRunner {
        fn exec(&mut self, sql: &str, args: &[Datum]) -> Result<u64, DriverError> {
            let rendered: Vec<String> = args
                .iter()
                .map(|arg| crate::datum_text(arg).unwrap_or_default())
                .collect();
            self.statements.borrow_mut().push(if rendered.is_empty() {
                sql.to_owned()
            } else {
                format!("{sql} :: [{}]", rendered.join(", "))
            });
            Ok(1)
        }

        fn exec_rows(
            &mut self,
            sql: &str,
            _args: &[Datum],
        ) -> Result<Vec<Vec<Datum>>, DriverError> {
            self.statements.borrow_mut().push(sql.to_owned());
            Ok(self.rows.clone())
        }
    }

    fn parse(sql: &str) -> Stmt {
        tidb_parser::parse(sql).expect("test SQL parses")
    }

    /// Go `TestExtractTableName` (`binding_cache_test.go:173`).
    ///
    /// Go collects with `CollectTableNames` and renders each `*ast.TableName`
    /// with `format.RestoreKeyWordLowercase` only -- no quoting and no default
    /// DB -- which for these statements is just the bare table name. The
    /// equivalent here is [`crate::binding::collect_table_names`], whose pairs
    /// are already lower-cased.
    #[test]
    fn extract_table_name() {
        let cases: [(&str, &[&str]); 6] = [
            (
                "select /*+ HASH_JOIN(t1, t2) */ * from t1 t1 join t1 t2 on t1.a=t2.a where t1.b is not null;",
                &["t1", "t1"],
            ),
            ("select * from t", &["t"]),
            ("select * from t1, t2, t3;", &["t1", "t2", "t3"]),
            ("select * from t1 where t1.a > (select max(a) from t2);", &["t1", "t2"]),
            (
                "select * from t1 where t1.a > (select max(a) from t2 where t2.a > (select max(a) from t3));",
                &["t1", "t2", "t3"],
            ),
            (
                "select a,b,c,d,* from t1 where t1.a > (select max(a) from t2 where t2.a > (select max(a) from t3));",
                &["t1", "t2", "t3"],
            ),
        ];
        for (sql, tables) in cases {
            let names: Vec<String> = crate::binding::collect_table_names(&parse(sql))
                .into_iter()
                .map(|(schema, table)| {
                    if schema.is_empty() {
                        table
                    } else {
                        format!("{schema}.{table}")
                    }
                })
                .collect();
            assert_eq!(names, tables, "sql: {sql}");
        }
    }

    /// New coverage: `GenerateBindingSQL`'s restore shape and hint injection
    /// for every statement kind Go's switch names. Go's own coverage of this
    /// function is testkit-based (`binding_operator_test.go`).
    #[test]
    fn generate_binding_sql_injects_the_hint_per_statement_kind() {
        let hint = "use_index(@`sel_1` `test`.`t` )";
        let cases = [
            (
                "select a from t where a > 1",
                "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ `a` FROM `test`.`t` WHERE `a` > 1",
            ),
            (
                "delete from t where a = 1",
                "DELETE /*+ use_index(@`sel_1` `test`.`t` )*/ FROM `test`.`t` WHERE `a` = 1",
            ),
            (
                "update t set a = 1 where b = 2",
                "UPDATE /*+ use_index(@`sel_1` `test`.`t` )*/ `test`.`t` SET `a`=1 WHERE `b` = 2",
            ),
            (
                "insert into t select * from t2",
                "INSERT INTO `t` SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t2`",
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(
                generate_binding_sql(&parse(sql), hint, "test"),
                expected,
                "sql: {sql}"
            );
        }
    }

    /// New coverage: the statement's OWN hints are erased before the plan hint
    /// is injected -- Go's `hint.BindHint(stmtNode, &hint.HintsSet{})`.
    #[test]
    fn generate_binding_sql_erases_the_statements_own_hints() {
        let sql = "select /*+ use_index(t, k) */ a from t where a > 1";
        assert_eq!(
            generate_binding_sql(&parse(sql), "hash_join(`t`)", "test"),
            "SELECT /*+ hash_join(`t`)*/ `a` FROM `test`.`t` WHERE `a` > 1"
        );
    }

    /// New coverage: the `WITH` branch splices at the query's own `SELECT`,
    /// NOT at the one inside the CTE body -- the whole point of Go's
    /// `withEnd` arithmetic (lines 115-124).
    #[test]
    fn generate_binding_sql_skips_the_cte_body_select() {
        let sql = "with c as (select * from t2) select * from c join t3 on 1";
        assert_eq!(
            generate_binding_sql(&parse(sql), "hash_join(`c`)", "test"),
            "WITH `c` AS (SELECT * FROM `test`.`t2`) SELECT /*+ hash_join(`c`)*/ * FROM `c` JOIN `test`.`t3` ON 1"
        );
    }

    /// New coverage: `REPLACE` takes Go's `IsReplace` branch, and a
    /// `VALUES`-form insert gets NO hint because Go injects at `SELECT`.
    #[test]
    fn generate_binding_sql_handles_replace_and_valueless_inserts() {
        assert_eq!(
            generate_binding_sql(&parse("replace into t select * from t2"), "h", "test"),
            "REPLACE INTO `t` SELECT /*+ h*/ * FROM `test`.`t2`"
        );
        assert_eq!(
            generate_binding_sql(&parse("insert into t values (1)"), "h", "test"),
            "INSERT INTO `t` VALUES (1)"
        );
    }

    /// New coverage: statement kinds Go's switch does not name answer `""`.
    #[test]
    fn generate_binding_sql_declines_unhandled_statements() {
        assert_eq!(
            generate_binding_sql(&parse("select 1 union select 2"), "h", "test"),
            ""
        );
        assert_eq!(generate_binding_sql(&parse("show tables"), "h", "test"), "");
    }

    /// New coverage: DB restoration and last-semicolon handling, the two
    /// properties `RestoreDBForBinding` contributes to every stored binding.
    /// A trailing `;` is part of the user's text, never of the restored
    /// statement (Go reaches the same state through `eraseLastSemicolon`).
    #[test]
    fn generate_binding_sql_qualifies_schemas_and_drops_the_last_semicolon() {
        // Unqualified names take the default DB.
        assert_eq!(
            generate_binding_sql(&parse("select * from t"), "h", "test"),
            "SELECT /*+ h*/ * FROM `test`.`t`"
        );
        // An explicit schema is kept as written, not overwritten.
        assert_eq!(
            generate_binding_sql(&parse("select * from d.t"), "h", "test"),
            "SELECT /*+ h*/ * FROM `d`.`t`"
        );
        // A trailing semicolon leaves no trace.
        assert_eq!(
            generate_binding_sql(&parse("select * from t;"), "h", "test"),
            generate_binding_sql(&parse("select * from t"), "h", "test")
        );
        // No default DB: names stay unqualified.
        assert_eq!(
            generate_binding_sql(&parse("select * from t"), "h", ""),
            "SELECT /*+ h*/ * FROM `t`"
        );
    }

    /// New coverage: `newBindingFromStorage`'s three observable rules --
    /// `using` becomes `enabled`, the schema is lower-cased, and the derived
    /// matcher inputs come from parsing `bind_sql`.
    #[test]
    fn new_binding_from_storage_normalizes_status_and_schema() {
        let row: Vec<Datum> = [
            "select * from `test` . `t`",
            "SELECT /*+ use_index(`t` ) */ * FROM `test`.`t`",
            "TeSt",
            "using",
            "2026-01-01 00:00:00.000000",
            "2026-01-02 00:00:00.000000",
            "utf8mb4",
            "utf8mb4_bin",
            "capture",
            "digest-1",
            "plan-1",
        ]
        .into_iter()
        .map(|text| Datum::new_string(text.to_owned()))
        .collect();

        let binding = new_binding_from_storage(&row).expect("the row maps to a binding");
        assert_eq!(
            binding.status, STATUS_ENABLED,
            "`using` is stored as `enabled`"
        );
        assert_eq!(binding.db, "test");
        assert_eq!(binding.source, "capture");
        assert_eq!(binding.sql_digest, "digest-1");
        assert_eq!(binding.create_time, "2026-01-01 00:00:00.000000");
        assert_eq!(
            binding.table_names,
            vec![("test".to_owned(), "t".to_owned())]
        );
        assert_eq!(
            binding.no_db_digest,
            crate::binding::no_db_digest(&parse(&binding.bind_sql))
        );

        // A status this crate cannot represent drops the row, as does an
        // unparsable bind_sql.
        let mut broken = row.clone();
        broken[3] = Datum::new_string("deleted".to_owned());
        assert!(new_binding_from_storage(&broken).is_none());
        let mut unparsable = row;
        unparsable[1] = Datum::new_string("NOT A STATEMENT".to_owned());
        assert!(new_binding_from_storage(&unparsable).is_none());
    }

    /// New coverage: `readBindingsFromStorage` builds Go's statement and skips
    /// the builtin lock row.
    #[test]
    fn read_bindings_from_storage_skips_the_lock_row() {
        let row = |original_sql: &str| -> Vec<Datum> {
            [
                original_sql,
                "SELECT * FROM `test`.`t`",
                "test",
                "enabled",
                "2026-01-01 00:00:00.000000",
                "2026-01-01 00:00:00.000000",
                "utf8mb4",
                "utf8mb4_bin",
                "manual",
                "digest-1",
                "",
            ]
            .into_iter()
            .map(|text| Datum::new_string(text.to_owned()))
            .collect()
        };
        let mut runner = RecordingRunner {
            rows: vec![
                row(BUILTIN_PSEUDO_SQL_4_BIND_LOCK),
                row("select * from `test` . `t`"),
            ],
            ..RecordingRunner::default()
        };

        let bindings =
            read_bindings_from_storage(&mut runner, "ORDER BY update_time, create_time", &[])
                .unwrap();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].original_sql, "select * from `test` . `t`");
        assert_eq!(
            runner.statements.borrow().as_slice(),
            [read_bindings_sql("ORDER BY update_time, create_time")]
        );
        // No transaction: Go passes wrapTxn=false here.
        assert!(!runner.statements.borrow()[0].contains("BEGIN"));
    }

    /// New coverage: `shouldUpdateBinding`'s three branches (lines 216-224).
    #[test]
    fn should_update_binding_follows_the_write_interval() {
        let at = |secs: i64| DateTime::from_timestamp(secs, 0).unwrap();
        let now = at(1_000_000);
        // Never saved -> always written.
        assert!(should_update_binding(now, None, at(0)));
        // Saved recently -> not written, even though it was used since.
        let recent = now - chrono::TimeDelta::hours(1);
        assert!(!should_update_binding(now, Some(recent), now));
        // Saved long ago but unused since -> not written.
        let old = now - chrono::TimeDelta::hours(7);
        assert!(!should_update_binding(now, Some(old), old));
        // Saved long ago and used since -> written.
        assert!(should_update_binding(
            now,
            Some(old),
            old + chrono::TimeDelta::seconds(1)
        ));
        // Exactly at the interval boundary is inclusive (Go's `>=`).
        let boundary = now - chrono::TimeDelta::hours(6);
        assert!(should_update_binding(
            now,
            Some(boundary),
            boundary + chrono::TimeDelta::seconds(1)
        ));
    }

    /// New coverage: the usage writer's statement sequence, including the
    /// pessimistic transaction wrapper and the inverted lock tuple.
    #[test]
    fn update_binding_usage_info_writes_the_go_statement_sequence() {
        let now = DateTime::from_timestamp(1_767_225_600, 0).unwrap();
        let mut usages = vec![
            BindingUsage {
                sql_digest: "sd1".to_owned(),
                plan_digest: String::new(),
                last_used_at: Some(now),
                last_saved_at: None,
            },
            BindingUsage {
                sql_digest: "sd2".to_owned(),
                plan_digest: "pd2".to_owned(),
                last_used_at: Some(now),
                last_saved_at: None,
            },
            // Never used -> never written.
            BindingUsage {
                sql_digest: "sd3".to_owned(),
                plan_digest: String::new(),
                last_used_at: None,
                last_saved_at: None,
            },
            // Saved a minute ago -> not due.
            BindingUsage {
                sql_digest: "sd4".to_owned(),
                plan_digest: String::new(),
                last_used_at: Some(now),
                last_saved_at: Some(now - chrono::TimeDelta::minutes(1)),
            },
        ];

        let mut runner = RecordingRunner::default();
        let count = update_binding_usage_info_to_storage(&mut runner, &mut usages, now).unwrap();
        assert_eq!(count, 2);

        let last_used = format_last_used(now);
        assert_eq!(last_used, "2026-01-01 00:00:00");
        assert_eq!(
            runner.statements.borrow().as_slice(),
            [
                "BEGIN PESSIMISTIC".to_owned(),
                LOCK_BIND_INFO_SQL.to_owned(),
                "select 1 from mysql.bind_info use index(digest_index) where (plan_digest, sql_digest) in (('sd1',NULL) , ('sd2','pd2')) for update".to_owned(),
                format!("{} :: [{last_used}, sd1]", save_binding_usage_sql("")),
                format!("{} :: [{last_used}, sd2]", save_binding_usage_sql("pd2")),
                "COMMIT".to_owned(),
            ]
        );

        // Written bindings advance their last-saved watermark; the others do
        // not, so a second pass writes nothing.
        assert_eq!(usages[0].last_saved_at, Some(now));
        assert_eq!(usages[1].last_saved_at, Some(now));
        assert_eq!(usages[2].last_saved_at, None);
        assert_eq!(
            usages[3].last_saved_at,
            Some(now - chrono::TimeDelta::minutes(1))
        );
        let mut second = RecordingRunner::default();
        assert_eq!(
            update_binding_usage_info_to_storage(&mut second, &mut usages, now).unwrap(),
            0
        );
        assert!(second.statements.borrow().is_empty());
    }

    /// New coverage: the batch boundary at
    /// [`UPDATE_BINDING_USAGE_INFO_BATCH_SIZE`] -- Go flushes mid-loop and
    /// then again for the remainder.
    #[test]
    fn update_binding_usage_info_flushes_every_batch() {
        let now = DateTime::from_timestamp(1_767_225_600, 0).unwrap();
        let mut usages: Vec<BindingUsage> = (0..UPDATE_BINDING_USAGE_INFO_BATCH_SIZE + 1)
            .map(|i| BindingUsage {
                sql_digest: format!("sd{i}"),
                plan_digest: String::new(),
                last_used_at: Some(now),
                last_saved_at: None,
            })
            .collect();

        let mut runner = RecordingRunner::default();
        let count = update_binding_usage_info_to_storage(&mut runner, &mut usages, now).unwrap();
        assert_eq!(count, UPDATE_BINDING_USAGE_INFO_BATCH_SIZE + 1);
        let statements = runner.statements.borrow();
        // Two transactions: one full batch, then the single leftover.
        assert_eq!(
            statements
                .iter()
                .filter(|s| *s == "BEGIN PESSIMISTIC")
                .count(),
            2
        );
        assert_eq!(statements.iter().filter(|s| *s == "COMMIT").count(), 2);
    }

    /// New coverage: `callWithSCtx`'s rollback path (lines 59-66).
    #[test]
    fn call_with_runner_rolls_back_on_error() {
        let mut runner = RecordingRunner::default();
        let err = call_with_runner(&mut runner, true, |_runner| {
            Err::<(), _>(DriverError::unsupported("boom"))
        })
        .unwrap_err();
        assert!(format!("{err}").contains("boom"), "{err}");
        assert_eq!(
            runner.statements.borrow().as_slice(),
            ["BEGIN PESSIMISTIC".to_owned(), "ROLLBACK".to_owned()]
        );
    }
}
