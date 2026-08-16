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

//! The `CREATE BINDING` / `DROP BINDING` / `SHOW BINDINGS` arms of
//! [`crate::Session::dispatch_admin_stmt`], plus the plan-time match every
//! `SELECT` runs through. The mechanics they build on live in
//! [`crate::binding`]; read that module's doc first.

use tidb_ast::{
    BindingScope, CreateBindingSource, CreateBindingStmt, DropBindingStmt, DropBindingTarget,
    ShowBindingsStmt, Stmt,
};
use tidb_datatype::Datum;
use tidb_executor::DriverError;

use crate::binding::{self, Binding, SOURCE_MANUAL, STATUS_ENABLED};
use crate::{Session, StmtOutput};

/// A GLOBAL binding is a ROW in `mysql.bind_info` (bootstrapped by
/// `crate::bootstrap`), written and read with the same statements Go's
/// `bindingOperator` issues -- CREATE marks the older rows for the same
/// normalized statement `deleted` and inserts a fresh `enabled` row, DROP is
/// an UPDATE to `deleted`, and both SHOW and the plan-time match read the
/// live rows back and re-derive digests/table names/hints by parsing the
/// stored SQL, exactly as Go's cache load does. There is deliberately no
/// second in-memory copy to drift from the table: with one process, the
/// table IS the cache.
const BIND_INFO_COLUMNS: &str = "original_sql, bind_sql, default_db, status, create_time, \
     update_time, charset, collation, source, sql_digest";

impl Session {
    /// Go `SQLBindExec.setBindingStatus[ByDigest]` over
    /// `bindingOperator.SetBindingStatus`: one UPDATE flipping the row
    /// between `enabled` and `disabled`, guarded by the CURRENT status --
    /// disabling accepts `using` (the legacy enabled spelling) and
    /// `enabled`, enabling accepts only `disabled`. Zero rows touched is
    /// not an error but the measured warning.
    pub(crate) fn set_binding_stmt(
        &mut self,
        set: &tidb_ast::SetBindingStmt,
    ) -> Result<StmtOutput, DriverError> {
        let digest = match &set.target {
            tidb_ast::SetBindingTarget::Statement(target) => {
                let current_db = self.current_db.clone();
                let (_, digest) = binding::normalize_with_db(target.origin.as_ref(), &current_db);
                digest
            }
            tidb_ast::SetBindingTarget::SqlDigest(digest) => digest.clone(),
        };
        let (new_status, old_status0, old_status1) = match set.status {
            tidb_ast::BindingStatus::Enabled => (
                STATUS_ENABLED,
                binding::STATUS_DISABLED,
                binding::STATUS_DISABLED,
            ),
            tidb_ast::BindingStatus::Disabled => (
                binding::STATUS_DISABLED,
                binding::STATUS_USING,
                STATUS_ENABLED,
            ),
        };
        self.gc_global_bindings()?;
        let now = self.global_binding_timestamp();
        let changed = self.bind_info_exec(
            "UPDATE mysql.bind_info SET status = ?, update_time = ? \
             WHERE sql_digest = ? AND update_time < ? AND status IN (?, ?)",
            &[
                Datum::new_string(new_status.to_owned()),
                Datum::new_string(now.clone()),
                Datum::new_string(digest),
                Datum::new_string(now),
                Datum::new_string(old_status0.to_owned()),
                Datum::new_string(old_status1.to_owned()),
            ],
        )?;
        if changed == 0 {
            // Go wraps `errors.NewNoStackError(...)` as an ordinary 1105.
            self.append_warning(
                crate::WarningLevel::Warning,
                1105,
                "There are no bindings can be set the status. Please check the SQL text".to_owned(),
            );
        }
        Ok(StmtOutput::Affected(0))
    }

    /// Go `SQLBindExec`'s create half, for `CREATE [SESSION] BINDING FOR
    /// <origin> USING <hinted>`.
    pub(crate) fn create_binding_stmt(
        &mut self,
        create: &CreateBindingStmt,
    ) -> Result<StmtOutput, DriverError> {
        let target = match &create.source {
            CreateBindingSource::Statement { target } => target,
            // `FROM HISTORY USING PLAN DIGEST` reads a captured plan out of
            // `information_schema.statements_summary`, which this tier does
            // not record. Refused rather than answered from nothing.
            CreateBindingSource::History { .. } => {
                return Err(DriverError::unsupported(
                    "CREATE BINDING FROM HISTORY needs a recorded statement summary, \
                     which this tier does not keep",
                ))
            }
        };
        let Some(hinted) = &target.hinted else {
            // The grammar guarantees `USING` for CREATE; a missing one is a
            // parser invariant break, not a user error.
            return Err(DriverError::unsupported(
                "CREATE BINDING without USING is not a statement this tier models",
            ));
        };
        let origin = target.origin.as_ref();
        let hinted = hinted.as_ref();

        let current_db = self.current_db.clone();
        // A cross-DB binding (`*.t`) belongs to NO schema: Go stores an
        // empty `Default_db` (measured), and the `*` survives normalization.
        let wildcard = binding::collect_table_names(origin)
            .iter()
            .any(|(schema, _)| schema == "*");
        let db = if wildcard {
            String::new()
        } else {
            binding::default_db_of(origin, &current_db)
        };
        let (original_sql, sql_digest) = binding::normalize_with_db(origin, &current_db);
        let (hinted_normalized, _) = binding::normalize_with_db(hinted, &current_db);
        // Go's preprocessor check: erasing the hints must leave two identical
        // statements.
        binding::check_origin_matches_hinted(&original_sql, &hinted_normalized)?;
        // Go `checkBindingValidation` runs `EXPLAIN FORMAT='hint'` over the
        // hinted SQL, so a binding naming a table or index that does not
        // exist fails at CREATE time with that statement's own error (1146 /
        // 1176, captured from real TiDB). Planning the hinted statement here
        // raises the same errors from the same catalog. A `*` schema names
        // no plannable table, and Go accepts the statement without the check
        // (measured, switch on or off), so the wildcard form skips it.
        if !wildcard {
            self.validate_binding_statement(hinted)?;
        }

        let bind_sql = binding::restore_with_default_db(hinted, &current_db);
        let now = self.binding_timestamp();
        let binding = Binding {
            original_sql,
            bind_sql,
            db,
            status: STATUS_ENABLED,
            charset: self.binding_charset(),
            collation: self.binding_collation(),
            source: SOURCE_MANUAL,
            sql_digest,
            create_time: now.clone(),
            update_time: now,
            no_db_digest: binding::no_db_digest(hinted),
            table_names: binding::collect_table_names(origin),
            hints: binding::collect_hints(hinted),
        };
        if create.scope == BindingScope::Global {
            self.create_global_binding(&binding)?;
        } else {
            self.session_bindings.create(binding);
        }
        Ok(StmtOutput::Affected(0))
    }

    /// Go `bindingOperator.CreateBinding`'s storage half: older rows for the
    /// same normalized statement become `deleted`, then the new row is
    /// inserted `enabled`. The timestamps print with SIX fractional digits
    /// (`types.NewTime(..., 6)` there, against the session handle's 3).
    fn create_global_binding(&mut self, binding: &Binding) -> Result<(), DriverError> {
        self.gc_global_bindings()?;
        let now = self.global_binding_timestamp();
        self.bind_info_exec(
            "UPDATE mysql.bind_info SET status = ?, update_time = ? \
             WHERE original_sql = ? AND update_time < ?",
            &[
                Datum::new_string("deleted".to_owned()),
                Datum::new_string(now.clone()),
                Datum::new_string(binding.original_sql.clone()),
                Datum::new_string(now.clone()),
            ],
        )?;
        self.bind_info_exec(
            &format!(
                "INSERT INTO mysql.bind_info({BIND_INFO_COLUMNS}) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            ),
            &[
                Datum::new_string(binding.original_sql.clone()),
                Datum::new_string(binding.bind_sql.clone()),
                // Go lowercases the schema on the way in
                // (`strings.ToLower(binding.Db)`).
                Datum::new_string(binding.db.to_lowercase()),
                Datum::new_string(binding.status.to_owned()),
                Datum::new_string(now.clone()),
                Datum::new_string(now),
                Datum::new_string(binding.charset.clone()),
                Datum::new_string(binding.collation.clone()),
                Datum::new_string(binding.source.to_owned()),
                Datum::new_string(binding.sql_digest.clone()),
            ],
        )?;
        Ok(())
    }

    /// Go `SQLBindExec`'s drop half. Dropping a binding that is not there is
    /// NOT an error (measured on real TiDB: `DROP SESSION BINDING FOR <sql>`
    /// with no such binding answers OK), so the affected count is the only
    /// thing that distinguishes the two outcomes.
    pub(crate) fn drop_binding_stmt(
        &mut self,
        drop: &DropBindingStmt,
    ) -> Result<StmtOutput, DriverError> {
        let digests: Vec<String> = match &drop.target {
            DropBindingTarget::Statement(target) => {
                let current_db = self.current_db.clone();
                let (_, digest) = binding::normalize_with_db(target.origin.as_ref(), &current_db);
                vec![digest]
            }
            DropBindingTarget::SqlDigests(values) => values
                .iter()
                .map(|value| match value {
                    tidb_ast::BindingValue::String(text) => Ok(text.clone()),
                    // A `@var` digest is read at execution time from the
                    // session's user variables; refusing is better than
                    // silently dropping nothing.
                    tidb_ast::BindingValue::UserVar(name) => self
                        .user_vars
                        .borrow()
                        .get(&name.to_ascii_lowercase())
                        .and_then(crate::datum_text)
                        .ok_or_else(|| DriverError::unsupported("sql digest is null")),
                })
                .collect::<Result<Vec<_>, _>>()?,
        };
        if drop.scope == BindingScope::Global {
            // Go `bindingOperator.DropBinding`: a drop is an UPDATE to
            // `deleted`, and the affected count is the answer.
            self.gc_global_bindings()?;
            let now = self.global_binding_timestamp();
            let mut dropped = 0u64;
            for digest in &digests {
                dropped += self.bind_info_exec(
                    "UPDATE mysql.bind_info SET status = ?, update_time = ? \
                     WHERE sql_digest = ? AND update_time < ? AND status != ?",
                    &[
                        Datum::new_string("deleted".to_owned()),
                        Datum::new_string(now.clone()),
                        Datum::new_string(digest.clone()),
                        Datum::new_string(now.clone()),
                        Datum::new_string("deleted".to_owned()),
                    ],
                )?;
            }
            return Ok(StmtOutput::Affected(dropped));
        }
        let dropped = digests
            .iter()
            .filter(|digest| self.session_bindings.drop_digest(digest))
            .count();
        Ok(StmtOutput::Affected(dropped as u64))
    }

    /// Go `fetchShowBind`, session scope.
    /// Go `ShowExec.fetchShowBindingCacheStatus`: the cache count is the
    /// enabled/`using` bindings the handle holds, the table count is
    /// `count(*)` over the same statuses, and the usage is the sum of
    /// `Binding.size()` -- each string field's byte length plus two 16-byte
    /// `types.Time` stamps (the `ID` a fresh binding carries is empty).
    /// This tier's cache IS the table, so the two counts agree by
    /// construction; on real TiDB they differ only while the cache lags a
    /// peer's write. Captured: a fresh store answers
    /// `0|0|0 Bytes|64 MB`, one binding `1|1|156 Bytes|64 MB`.
    pub(crate) fn binding_cache_status_stmt(&mut self) -> Result<StmtOutput, DriverError> {
        use crate::binding::STATUS_USING;
        let loaded = self.load_global_bindings()?;
        let all = loaded.all_sorted();
        // A DISABLED row counts in NEITHER column but still holds cache
        // memory (captured: disabling flips `1|1|156 Bytes` to
        // `0|0|157 Bytes` -- one byte more, `disabled` over `enabled`).
        let live = all
            .iter()
            .filter(|binding| binding.status == STATUS_ENABLED || binding.status == STATUS_USING)
            .count();
        let usage: i64 = all
            .iter()
            .map(|binding| crate::binding_cache::binding_size(binding))
            .sum();
        let quota = self
            .vars
            .get_system("tidb_mem_quota_binding_cache")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(64 << 20);
        let long = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let text = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let count = i64::try_from(live).unwrap_or(i64::MAX);
        Ok(StmtOutput::Rows {
            columns: vec![
                ("bindings_in_cache".to_owned(), long()),
                ("bindings_in_table".to_owned(), long()),
                ("memory_usage".to_owned(), text()),
                ("memory_quota".to_owned(), text()),
            ],
            rows: vec![vec![
                Datum::Int(count),
                Datum::Int(count),
                Datum::new_string(tidb_util::memory::format_bytes(usage)),
                Datum::new_string(tidb_util::memory::format_bytes(quota)),
            ]],
        })
    }

    pub(crate) fn show_bindings_stmt(
        &mut self,
        show: &ShowBindingsStmt,
    ) -> Result<StmtOutput, DriverError> {
        let text = || tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let columns = [
            "Original_sql",
            "Bind_sql",
            "Default_db",
            "Status",
            "Create_time",
            "Update_time",
            "Charset",
            "Collation",
            "Source",
            "Sql_digest",
            "Plan_digest",
        ]
        .into_iter()
        .map(|name| (name.to_owned(), text()))
        .collect();
        let global;
        let listed = if show.scope == BindingScope::Global {
            global = self.load_global_bindings()?;
            global.all_sorted()
        } else {
            self.session_bindings.all_sorted()
        };
        let rows = listed
            .into_iter()
            .map(|binding| {
                vec![
                    Datum::Bytes(binding.original_sql.clone().into_bytes()),
                    Datum::Bytes(binding.bind_sql.clone().into_bytes()),
                    Datum::Bytes(binding.db.clone().into_bytes()),
                    Datum::Bytes(binding.status.as_bytes().to_vec()),
                    Datum::Bytes(binding.create_time.clone().into_bytes()),
                    Datum::Bytes(binding.update_time.clone().into_bytes()),
                    Datum::Bytes(binding.charset.clone().into_bytes()),
                    Datum::Bytes(binding.collation.clone().into_bytes()),
                    Datum::Bytes(binding.source.as_bytes().to_vec()),
                    Datum::Bytes(binding.sql_digest.clone().into_bytes()),
                    // Go leaves `Plan_digest` empty for a manually created
                    // binding; only `FROM HISTORY` fills it.
                    Datum::Bytes(Vec::new()),
                ]
            })
            .collect();
        Ok(StmtOutput::Rows { columns, rows })
    }

    /// Go `planner.optimize`'s binding step: replaces the statement's hints
    /// with the matched binding's and records the match for
    /// `@@last_plan_from_binding`.
    ///
    /// Returns the statement to plan. The caller keeps the original when
    /// nothing matched, so a session with no bindings pays one map-emptiness
    /// test and nothing else.
    pub(crate) fn bind_statement_hints(&mut self, stmt: &Stmt) -> Option<Stmt> {
        // The Go shape is a cache-size test before any digest is computed;
        // here the global "cache" is the table itself, so its side of the
        // test is a row count (see [`Self::has_global_binding_rows`]).
        if self.session_bindings.is_empty() && !self.has_global_binding_rows() {
            return None;
        }
        // Go gates the whole step on `SessionVars.UsePlanBaselines`.
        if !self.session_bool("tidb_use_plan_baselines", true) {
            return None;
        }
        let no_db_digest = binding::no_db_digest(stmt);
        let table_names = binding::collect_table_names(stmt);
        // Go `crossDBMatchBindings` reads `EnableFuzzyBinding` at MATCH
        // time, so flipping the switch changes what an existing wildcard
        // binding does without touching the row.
        let fuzzy_enabled = self.session_bool("tidb_opt_enable_fuzzy_binding", false);
        // Session bindings shadow global ones, which is Go's order in
        // `planner.optimize`: `getBindingFromSession` first, the domain
        // handle only on a miss.
        let hints = match self.session_bindings.match_statement(
            &no_db_digest,
            &table_names,
            &self.current_db,
            fuzzy_enabled,
        ) {
            Some(matched) => matched.hints.clone(),
            None => {
                let global = self.load_global_bindings().ok()?;
                global
                    .match_statement(&no_db_digest, &table_names, &self.current_db, fuzzy_enabled)?
                    .hints
                    .clone()
            }
        };
        let mut bound = stmt.clone();
        binding::bind_hints(&mut bound, &hints);
        self.found_in_binding = true;
        Some(bound)
    }

    /// Whether `@@last_plan_from_binding` should report a hit, which is the
    /// PRECEDING statement's outcome (Go `PrevFoundInBinding`).
    pub(crate) fn last_plan_from_binding(&self) -> bool {
        self.prev_found_in_binding
    }

    /// Go `checkBindingValidation`'s effect: the hinted statement must plan
    /// against the current catalog. Go reaches it through `EXPLAIN
    /// FORMAT='hint'`; this runs the same planner entry `EXPLAIN` runs.
    fn validate_binding_statement(&mut self, hinted: &Stmt) -> Result<(), DriverError> {
        let Stmt::Query(query) = hinted else {
            return Ok(());
        };
        let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
            return Ok(());
        };
        let current_db = self.current_db.clone();
        let ctx = self.statement_context(false);
        let select = select.clone();
        self.with_catalog_mut(|catalog| {
            tidb_executor::explain_select_stmt(
                &select,
                catalog,
                &current_db,
                &ctx,
                tidb_executor::ExplainFormat::Brief,
            )
            .map(|_| ())
        })
    }

    /// Go stores `types.NewTime(FromGoTime(now.In(tz)), TypeTimestamp, 3)`,
    /// which prints with three fractional digits.
    fn binding_timestamp(&self) -> String {
        let zone = self.session_time_zone();
        let (seconds, nanos, offset) = self.statement_clock(&zone);
        let at = chrono::DateTime::from_timestamp(seconds, nanos)
            .unwrap_or_else(chrono::Utc::now)
            .naive_utc()
            + chrono::Duration::seconds(i64::from(offset));
        at.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
    }

    /// Go `SessionVars.GetCharsetInfo`'s first half.
    fn binding_charset(&self) -> String {
        self.vars
            .get_system("character_set_connection")
            .unwrap_or_else(|_| "utf8mb4".to_owned())
    }

    /// Go `SessionVars.GetCharsetInfo`'s second half.
    fn binding_collation(&self) -> String {
        self.vars
            .get_system("collation_connection")
            .unwrap_or_else(|_| "utf8mb4_bin".to_owned())
    }

    /// Whether `mysql.bind_info` can hold a user binding at all: any row
    /// beyond the builtin lock row (`crate::bootstrap`). Go answers the same
    /// question from its binding cache's size; this tier's cache IS the
    /// table, and a ROW COUNT reads no row bytes, which keeps the per-
    /// statement fast path free of `bind_info` decodes -- measured by the
    /// column-prune probes, which would otherwise see this table's columns
    /// under every statement.
    ///
    /// Rows marked `deleted` keep the gate open; the load skips them. That
    /// is the correct cost shape: a catalog where global bindings were NEVER
    /// used pays one integer compare, one that used them pays the read.
    ///
    /// The count must not touch the storage seam: iterating keys shows up in
    /// the storage-op counters the plan-cache tests measure statements by.
    /// `KvTable::len` is a stored size, and every row writes exactly one
    /// record key plus one key per index (NULLs are indexed), so more keys
    /// than one row's worth means a second row exists.
    fn has_global_binding_rows(&mut self) -> bool {
        self.with_catalog_mut(|catalog| {
            Ok(match catalog.table_in("mysql", "bind_info") {
                Some(tidb_executor::TableEntry::Kv(table)) => {
                    table.len() > 1 + table.indexes().len()
                }
                _ => false,
            })
        })
        .unwrap_or(false)
    }

    /// [`Self::binding_timestamp`] at SIX fractional digits: the global
    /// operator stores `types.NewTime(..., 6)` where the session handle uses
    /// 3, and `bind_info.update_time` is a `TIMESTAMP(6)` column.
    fn global_binding_timestamp(&self) -> String {
        self.global_binding_timestamp_offset(0)
    }

    /// [`Self::global_binding_timestamp`] shifted by `seconds` -- the GC
    /// cutoff is "now minus ten leases" in the same TIMESTAMP(6) spelling
    /// the rows store, so the comparison happens in one text domain.
    fn global_binding_timestamp_offset(&self, seconds: i64) -> String {
        let zone = self.session_time_zone();
        let (clock_seconds, nanos, offset) = self.statement_clock(&zone);
        let at = chrono::DateTime::from_timestamp(clock_seconds, nanos)
            .unwrap_or_else(chrono::Utc::now)
            .naive_utc()
            + chrono::Duration::seconds(i64::from(offset) + seconds);
        at.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
    }

    /// Go `bindingOperator.GCBinding`: `deleted` tombstones whose
    /// `update_time` is older than TEN LEASES (`bindinfo.Lease` = 3s) are
    /// physically removed, so every peer's cache has long acknowledged the
    /// drop. Go's owner runs it on a timer; this tier has no background
    /// loop, so every global-binding WRITE sweeps first -- the same rows
    /// are gone at the same observable ages, without a goroutine to port.
    fn gc_global_bindings(&mut self) -> Result<(), DriverError> {
        let cutoff = self.global_binding_timestamp_offset(-30);
        self.bind_info_exec(
            "DELETE FROM mysql.bind_info WHERE status = ? AND update_time < ?",
            &[
                Datum::new_string("deleted".to_owned()),
                Datum::new_string(cutoff),
            ],
        )?;
        Ok(())
    }

    /// One parameterized write against `mysql.bind_info`, the same shape as
    /// Go's `exec(sctx, sql, args...)` in `binding_operator.go`. The
    /// parameters go through [`tidb_executor::bind_parameters`] -- the
    /// prepared-statement binder -- so a bound SQL text (which contains
    /// quotes freely) can never break out of its literal.
    fn bind_info_exec(&mut self, sql: &str, params: &[Datum]) -> Result<u64, DriverError> {
        let text = tidb_executor::bind_parameters(sql, params, self.scanner_sql_mode())?;
        let ctx = self.statement_context(true);
        self.with_catalog_mut(|catalog| match text.trim_start().get(..6) {
            Some(word) if word.eq_ignore_ascii_case("INSERT") => {
                tidb_executor::run_insert_in(&text, catalog, "mysql", &ctx)
            }
            Some(word) if word.eq_ignore_ascii_case("DELETE") => {
                tidb_executor::run_delete_in(&text, catalog, "mysql", &ctx)
            }
            _ => tidb_executor::run_update_in(&text, catalog, "mysql", &ctx),
        })
    }

    /// Reads the live global bindings back out of `mysql.bind_info`,
    /// re-deriving the matcher's inputs by PARSING the stored `bind_sql` --
    /// which is exactly how Go's `LoadFromStorageToCache` rebuilds its cache
    /// rows, and what keeps this tier from needing a second copy that could
    /// drift from the table. `builtin` and `deleted` rows are skipped as Go
    /// skips them.
    fn load_global_bindings(&mut self) -> Result<binding::SessionBindings, DriverError> {
        if !self.has_global_binding_rows() {
            return Ok(binding::SessionBindings::default());
        }
        let ctx = self.statement_context(false);
        let (_, rows) = self.with_catalog_mut(|catalog| {
            tidb_executor::run_select_meta_in(
                &format!("SELECT {BIND_INFO_COLUMNS} FROM mysql.bind_info"),
                catalog,
                "mysql",
                &ctx,
            )
        })?;
        let mut loaded = binding::SessionBindings::default();
        for row in rows {
            let text = |index: usize| crate::datum_text(row.get(index)?);
            let Some(status) = text(3) else { continue };
            // Live rows only: `SHOW GLOBAL BINDINGS` lists `disabled` rows
            // (measured) and the match filter skips them by status; the
            // `builtin` lock row and `deleted` tombstones never load, as in
            // Go's cache.
            let status = match status.as_str() {
                s if s == STATUS_ENABLED => STATUS_ENABLED,
                s if s == binding::STATUS_USING => binding::STATUS_USING,
                s if s == binding::STATUS_DISABLED => binding::STATUS_DISABLED,
                _ => continue,
            };
            let (Some(original_sql), Some(bind_sql), Some(db), Some(sql_digest)) =
                (text(0), text(1), text(2), text(9))
            else {
                continue;
            };
            // A row whose bind_sql no longer parses matches nothing; Go
            // drops such rows from the cache with a warning.
            let Ok(hinted) = tidb_parser::parse_with_sql_mode(&bind_sql, self.scanner_sql_mode())
            else {
                continue;
            };
            loaded.create(Binding {
                original_sql,
                bind_sql,
                db,
                status,
                charset: text(6).unwrap_or_default(),
                collation: text(7).unwrap_or_default(),
                source: SOURCE_MANUAL,
                sql_digest,
                create_time: text(4).unwrap_or_default(),
                update_time: text(5).unwrap_or_default(),
                no_db_digest: binding::no_db_digest(&hinted),
                table_names: binding::collect_table_names(&hinted),
                hints: binding::collect_hints(&hinted),
            });
        }
        Ok(loaded)
    }
}
