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

//! A `table.MutateContext` with no session behind it.
//!
//! Go builds these over `exprstatic` wherever the table layer is driven
//! without a session: Lightning's `litTableMutateContext`
//! (`pkg/lightning/backend/kv/context.go`) and the DDL reorganization's
//! `reorgTableMutateContext` over `newReorgExprCtxWithReorgMeta`
//! (`pkg/ddl/reorg.go`). Here the system-table writer uses one too, where Go
//! writes `mysql.*` rows through an internal session's `INSERT`.

use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use tidb_datatype::{ConversionFlags, DateModes, Datum, SessionTimeZone};
use tidb_exprctx::Columns;
use tidb_exprctx::ErrorLevel;
use tidb_kv::keys_read::KeysReadBudget;
use tidb_variable::mem_quota::StatementMemory;
use tidb_kv::staged_writes::StagedWrites;
use tidb_context::WarningLevel;

use super::MutateContext;

/// What every clone of one context shares: Go's statement-owned buffers.
#[derive(Default)]
struct Shared {
    staged_writes: Arc<StagedWrites>,
    keys_read: KeysReadBudget,
    memory: StatementMemory,
    warnings: Mutex<Vec<tidb_context::SqlWarn>>,
    row_id_shard: OnceLock<u64>,
}

/// A statement context with no session behind it.
#[derive(Clone)]
pub struct StaticMutateContext {
    shared: Arc<Shared>,
    /// The complete SQL mode a reorganization derives its levels from.
    sql_mode: tidb_mysql::SqlMode,
    /// Go `SQLMode.HasStrictMode()`.
    strict: bool,
    /// Go `ErrCtx`'s level for truncation.
    truncate: ErrorLevel,
    /// Go `ErrCtx`'s level for a zero divisor.
    division_by_zero: ErrorLevel,
    /// The SQL mode's three date bits.
    date_modes: DateModes,
    /// Whether this is a reorganization's context.
    reorg: bool,
    /// Go `exprstatic.WithLocation`.
    zone: SessionTimeZone,
    /// Go `exprstatic.WithCurrentTime`: read once, in [`Self::zone`].
    clock: Option<Arc<OnceLock<(i64, u32, i32)>>>,
    /// Go `collate.NewCollationEnabled()`.
    new_collation_enabled: bool,
}

impl StaticMutateContext {
    /// A fresh statement of a default session in UTC: strict, truncation and
    /// zero divisors as warnings, no date-mode bits, no clock, the process's
    /// collation switch.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shared: Arc::default(),
            sql_mode: default_sql_mode(),
            strict: true,
            truncate: ErrorLevel::Warn,
            division_by_zero: ErrorLevel::Warn,
            date_modes: DateModes::default(),
            reorg: false,
            zone: SessionTimeZone::utc(),
            clock: None,
            new_collation_enabled: tidb_datatype::new_collation_enabled(),
        }
    }

    /// Go `newReorgTableMutateContext(newReorgExprCtxWithReorgMeta(meta))`:
    /// the job's SQL mode (`reorgTypeFlagsWithSQLMode`,
    /// `reorgErrLevelsWithSQLMode`) and location, the current time read in
    /// that location.
    #[must_use]
    pub fn for_reorg_meta(sql_mode: i64, zone: SessionTimeZone) -> Self {
        Self {
            sql_mode: tidb_mysql::SqlMode(sql_mode),
            zone,
            clock: Some(Arc::default()),
            ..Self::new()
        }
        .reorganized()
    }

    /// This context under its SQL mode's reorganization levels.
    fn reorganized(&self) -> Self {
        let mode = self.sql_mode;
        let strict = mode.has_strict_mode();
        let truncate = if strict {
            ErrorLevel::Error
        } else {
            ErrorLevel::Warn
        };
        Self {
            strict,
            truncate,
            division_by_zero: if mode.has_error_for_division_by_zero_mode() {
                truncate
            } else {
                ErrorLevel::Ignore
            },
            date_modes: DateModes {
                no_zero_date: mode.has_no_zero_date_mode(),
                no_zero_in_date: mode.has_no_zero_in_date_mode(),
                allow_invalid_dates: mode.has_allow_invalid_dates_mode(),
            },
            reorg: true,
            ..self.clone()
        }
    }

    fn warnings(&self) -> std::sync::MutexGuard<'_, Vec<tidb_context::SqlWarn>> {
        self.shared
            .warnings
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for StaticMutateContext {
    fn default() -> Self {
        Self::new()
    }
}

/// TiDB's `DefaultSQLMode`.
fn default_sql_mode() -> tidb_mysql::SqlMode {
    tidb_mysql::get_sql_mode(tidb_mysql::DefaultSQLMode).unwrap_or(tidb_mysql::SqlMode(0))
}

/// The statement clock Go's `GetStmtTimestamp` reads: `timestamp` (the
/// session's `@@timestamp`) or the wall clock, with `zone`'s offset at that
/// instant, as `(seconds, nanoseconds, offset seconds)`.
#[must_use]
pub fn resolve_statement_clock(timestamp: Option<f64>, zone: &SessionTimeZone) -> (i64, u32, i32) {
    use chrono::TimeZone;

    let utc = chrono::Utc::now();
    let (seconds, nanos) = match timestamp {
        #[expect(clippy::cast_possible_truncation, reason = "Go's int64(seconds)")]
        #[expect(clippy::cast_sign_loss, reason = "@@timestamp's MinValue is 0")]
        Some(timestamp) => (
            timestamp.trunc() as i64,
            (timestamp.fract() * 1e9) as u32 % 1_000_000_000,
        ),
        None => (utc.timestamp(), utc.timestamp_subsec_nanos()),
    };
    let offset = match zone {
        SessionTimeZone::Local => {
            let at = chrono::DateTime::from_timestamp(seconds, nanos)
                .unwrap_or(utc)
                .naive_utc();
            chrono::Offset::fix(&chrono::Local.offset_from_utc_datetime(&at)).local_minus_utc()
        }
        SessionTimeZone::Fixed { offset_secs, .. } => *offset_secs,
        SessionTimeZone::Named(zone) => {
            let at = chrono::DateTime::from_timestamp(seconds, nanos)
                .unwrap_or(utc)
                .naive_utc();
            chrono::Offset::fix(&zone.offset_from_utc_datetime(&at)).local_minus_utc()
        }
    };
    (seconds, nanos, offset)
}

impl Columns for StaticMutateContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn context_id(&self) -> u64 {
        Arc::as_ptr(&self.shared) as usize as u64
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        ("utf8mb4", "utf8mb4_bin")
    }

    fn now(&self) -> Option<(i64, u32, i32)> {
        let clock = self.clock.as_ref()?;
        Some(*clock.get_or_init(|| resolve_statement_clock(None, &self.zone)))
    }

    fn time_zone(&self) -> SessionTimeZone {
        self.zone.clone()
    }

    fn division_by_zero_level(&self) -> ErrorLevel {
        self.division_by_zero
    }

    fn truncate_level(&self) -> ErrorLevel {
        self.truncate
    }

    fn strict_sql_mode(&self) -> bool {
        self.strict
    }

    /// Go `ResetContextOfStmt`'s flags for a statement that reads, or the
    /// reorganization's.
    fn type_flags(&self) -> ConversionFlags {
        if self.reorg {
            return self.reorg_default_conversion_flags();
        }
        ConversionFlags::default()
            .with_ignore_zero_date_err(true)
            .with_ignore_truncate_err(self.truncate == ErrorLevel::Ignore)
            .with_truncate_as_warning(self.truncate == ErrorLevel::Warn)
            .with_ignore_invalid_date_err(self.date_modes.allow_invalid_dates)
            .with_ignore_zero_in_date_err(true)
            .with_allow_negative_to_unsigned(true)
    }

    fn append_warning(&self, code: u16, message: &str) {
        self.append_leveled(WarningLevel::Warning, code, message);
    }

    fn append_note(&self, code: u16, message: &str) {
        self.append_leveled(WarningLevel::Note, code, message);
    }

    fn warning_count(&self) -> usize {
        self.warnings().len()
    }

    fn truncate_warnings(&self, bookmark: usize) {
        self.warnings().truncate(bookmark);
    }

    fn take_warnings_since(&self, bookmark: usize) -> Vec<tidb_context::SqlWarn> {
        self.warnings().drain(bookmark..).collect()
    }

    fn append_sql_warn(&self, warning: tidb_context::SqlWarn) {
        self.warnings().push(warning);
    }

    fn append_error_warning(&self, error: &tidb_mysql::errors::SqlError) {
        self.append_sql_warn(tidb_context::SqlWarn::from_error(
            WarningLevel::Warning,
            error.code,
            error.message.clone(),
            error.identity,
        ));
    }

    fn date_modes(&self) -> DateModes {
        self.date_modes
    }
}

impl MutateContext for StaticMutateContext {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Go `util.GetTypeFlagsForInsert`.
    fn write_conversion_flags(&self) -> ConversionFlags {
        tidb_util::misc::write_date_flags(
            tidb_datatype::DEFAULT_STATEMENT_FLAGS
                .with_allow_negative_to_unsigned(false)
                .with_ignore_truncate_err(self.truncate == ErrorLevel::Ignore)
                .with_truncate_as_warning(self.truncate == ErrorLevel::Warn),
            self.date_modes,
            self.strict,
        )
    }

    fn strict(&self) -> bool {
        self.strict
    }

    fn ignore_err(&self) -> bool {
        false
    }

    fn new_collation_enabled(&self) -> bool {
        self.new_collation_enabled
    }

    /// Go's static contexts carry no statistics support.
    fn update_table_delta(&self, _physical_table_id: i64, _delta: i64, _count: i64) {}

    fn staged_writes(&self) -> &StagedWrites {
        &self.shared.staged_writes
    }

    fn staged_writes_handle(&self) -> Arc<StagedWrites> {
        Arc::clone(&self.shared.staged_writes)
    }

    fn pessimistic_transaction(&self) -> bool {
        false
    }

    fn pessimistic_check_in_prewrite(&self) -> bool {
        false
    }

    fn optimistic_transaction(&self) -> bool {
        false
    }

    fn writes_untouched_indices(&self) -> bool {
        false
    }

    /// Go `NewRowIDShardGenerator` under `tidb_shard_allocate_step`'s
    /// unbounded default: one shard for the whole context.
    fn next_row_id_shard(&self, _count: u64) -> u64 {
        *self
            .shared
            .row_id_shard
            .get_or_init(|| u64::from(tidb_fastrand::uint32()))
    }

    /// Go `DefTiDBTmpTableMaxSize`.
    fn tmp_table_max_size(&self) -> i64 {
        64 << 20
    }

    fn statement_memory(&self) -> StatementMemory {
        self.shared.memory.clone()
    }

    /// A leader read in the global scope, under the default resource group.
    fn snapshot_read_options(&self, _avg_row_bytes: f64) -> tidb_txnkv::SnapshotReadOptions {
        tidb_txnkv::SnapshotReadOptions {
            read_replica_scope: "global".to_owned(),
            resource_group_name: Some("default".to_owned()),
            busy_threshold_ns: 1_000_000_000,
            ..tidb_txnkv::SnapshotReadOptions::default()
        }
    }

    /// Go `reorgTypeFlagsWithSQLMode`.
    fn reorg_default_conversion_flags(&self) -> ConversionFlags {
        tidb_datatype::STRICT_FLAGS
            .with_truncate_as_warning(!self.strict)
            .with_ignore_invalid_date_err(self.date_modes.allow_invalid_dates)
            .with_ignore_zero_in_date_err(!self.strict || self.date_modes.allow_invalid_dates)
            .with_cast_time_to_year_through_concat(true)
    }

    fn take_warnings(&self) -> Vec<tidb_context::SqlWarn> {
        std::mem::take(&mut *self.warnings())
    }

    fn append_leveled(&self, level: WarningLevel, code: u16, message: &str) {
        self.append_sql_warn(tidb_context::SqlWarn::with_code(level, code, message));
    }


    fn query_default_conversion_flags(&self) -> ConversionFlags {
        tidb_datatype::DEFAULT_STATEMENT_FLAGS
            .with_truncate_as_warning(true)
            .with_ignore_zero_in_date_err(true)
            .with_ignore_invalid_date_err(self.date_modes.allow_invalid_dates)
    }

    fn show_default_conversion_flags(&self) -> ConversionFlags {
        tidb_datatype::DEFAULT_STATEMENT_FLAGS
            .with_ignore_truncate_err(true)
            .with_ignore_zero_in_date_err(true)
            .with_ignore_invalid_date_err(self.date_modes.allow_invalid_dates)
    }

    fn keys_read(&self) -> &KeysReadBudget {
        &self.shared.keys_read
    }

    /// Go `newReorgExprCtxWithReorgMeta` under this context's SQL mode.
    fn for_reorg(&self) -> Arc<dyn MutateContext> {
        Arc::new(self.reorganized())
    }

    fn with_new_collation_enabled(&self, enabled: bool) -> Arc<dyn MutateContext> {
        Arc::new(Self {
            new_collation_enabled: enabled,
            ..self.clone()
        })
    }

    fn to_shared(&self) -> Arc<dyn MutateContext> {
        Arc::new(self.clone())
    }

    fn session_zone(&self) -> SessionTimeZone {
        self.zone.clone()
    }
}
