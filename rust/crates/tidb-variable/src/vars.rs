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

//! Session system and user variables.
//!
//! Go keeps every system variable in one process-wide registry
//! (`variable.GetSysVar`) holding a scope, a default value, and validation,
//! and each session keeps the values it has overridden. `SET` on a name the
//! registry does not know is `ErrUnknownSystemVar` (1193), and reading `@@x`
//! for an unknown name is `ErrUnknownSystemVariable` too.
//!
//! The registry itself is [`crate::sysvar`], which holds all 965 entries
//! captured from Go's own `GetSysVars()`, and the value validation Go's
//! `ValidateFromType` performs.
//!
//! GLOBAL scope: a session reads and writes the GLOBAL and INSTANCE tiers
//! through its `GlobalVarsAccessor` ([`crate::GlobalVarAccessor`], Go's
//! interface), which the session installs when it is created -- the node's
//! accessor over the domain's sysvar cache (`tidb_domain::global_sysvars`).
//! A brand new session copies the cache's session view once, at connect
//! ([`SessionVars::seed_from_globals`], Go
//! `loadCommonGlobalVariablesIfNeeded`) -- so a session opened AFTER a
//! `SET GLOBAL` sees the new value as its session default, while sessions
//! already open do not. A `SkipInit` variable is not copied: the session
//! reads its GLOBAL value at first use and keeps it.
//!
//! Per-variable effects that need the complete session remain at the session
//! layer: for example, `variables.rs` performs autocommit's OFF-to-ON commit,
//! while this module keeps Go's typed autocommit status in lockstep with the
//! normalized variable value.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use tidb_fixcontrol::OptimizerFixControl;

use crate::sysvar::{alias_of, get_sys_var, SysVarDef, ValidationError, SCOPE_SESSION};

/// Names a validation refusal the way the variable error surface reports it:
/// Go's `ErrWrongTypeForVar` (1232), `ErrWrongValueForVar` (1231) and the
/// bare `errors.Errorf` a `Validation` closure may raise.
///
/// One function rather than one per call site, because the three writers
/// (SESSION, GLOBAL/INSTANCE, and the read-only refusal in
/// `Session::check_max_allowed_packet_scope`) must agree: the same rejected
/// value has to name the same error whichever tier was being written.
pub fn validation_var_error(name: &str, value: &str, error: ValidationError) -> VarError {
    match error {
        ValidationError::WrongType => tidb_terror::MysqlError::from_terror(
            &crate::variable_errors::ERR_WRONG_TYPE_FOR_VAR
                .generate_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
        ),
        ValidationError::WrongValue => tidb_terror::MysqlError::from_terror(
            &crate::variable_errors::ERR_WRONG_VALUE_FOR_VAR.generate_with_stack_by_args(&[
                (name.to_ascii_lowercase()).into(),
                (value.to_owned()).into(),
            ]),
        ),
        ValidationError::SqlError(error) => tidb_terror::MysqlError::from_sql_error(error),
        ValidationError::Refused(message) => tidb_terror::MysqlError::unknown(message),
    }
}

/// Process-owned PD setting consumer. Detached cluster validation images do
/// not publish to this owner; committed reloads retain the installed binding.
/// Publication runs under the global image lock; implementations must not
/// reenter global-variable APIs from this callback.
pub trait PdRegionPolicy: std::fmt::Debug + Send + Sync {
    /// Applies Go EnableFollowerHandle to the process PD client.
    fn set_follower_handle(&self, enabled: bool);
    /// Applies Go EnableTSOFollowerProxy to the same process PD client.
    fn set_tso_follower_proxy(&self, _enabled: bool) {}
    /// Applies the maximum additional collection wait to the shared PD owner.
    fn set_tso_batch_wait(&self, _wait: std::time::Duration) {}
    /// Applies Go `opt.TSOClientRPCConcurrency` to the same process PD client.
    ///
    /// Go's `setPDClientDynamicOption` does not forward the variable's text:
    /// `TiDBTSOClientRPCMode` is an enum whose three choices each name a
    /// concurrency (`pkg/domain/domain_sysvars.go:92-106`), and the mapped
    /// integer is what reaches the client. [`tso_rpc_concurrency`] performs
    /// that mapping so both publication paths send the same number.
    fn set_tso_rpc_concurrency(&self, _concurrency: isize) {}
}

/// Go `variable.SetGlobalResourceControl`: registered by the domain
/// (`domain_sysvars.go` `setGlobalResourceControl`, which calls the driver's
/// `EnableGlobalResourceControlFunc` / `DisableGlobalResourceControlFunc`,
/// client-go's `tikv.EnableResourceControl` / `DisableResourceControl`), and
/// called when `tidb_enable_resource_control` changes.
pub static SET_GLOBAL_RESOURCE_CONTROL: RwLock<Option<Arc<dyn Fn(bool) + Send + Sync>>> =
    RwLock::new(None);

/// Storage-oracle authority installed once by the process session factory.
pub trait ExternalTimestampProvider: std::fmt::Debug + Send + Sync {
    /// Reads the current timestamp from the storage authority.
    fn get(&self) -> Result<u64, String>;
    /// Updates the timestamp under that authority's monotonicity rules.
    fn set(&self, ts: u64) -> Result<(), String>;
}

/// One read-mostly image of the node-wide variable tables.
///
/// [`GlobalSysvars::get`] used to answer through the authoritative
/// `Mutex<HashMap<String, String>>`: per statement execution that cost a lock,
/// a SipHash probe over a ~40-character name, and an owned String clone --
/// times the dozen-plus variables `Session::statement_context_ignoring`
/// re-reads every statement. Go pays none of that: its `SetSysVar` writes a
/// typed object once and every read is a field load. This image is the rust
/// equivalent at table granularity: rebuilt wholesale whenever a mutation
/// lands (`SET GLOBAL`, cluster loads), read lock-free otherwise, with each
/// slot holding an immutable `Arc<str>` so a read clones only the `Arc`.
/// Slot `i` mirrors registry entry `i`; the owning tier is static per
/// variable, so one flat table serves both maps.
#[derive(Debug, Clone)]
pub struct ResolvedGlobals {
    pub values: std::boxed::Box<[Option<Arc<str>>]>,
    /// Go's process-wide typed `vardef.OOMAction` atomic.
    pub oom_action: tidb_vardef::OomAction,
    /// Go's process-wide typed `vardef.EnableTmpStorageOnOOM` atomic.
    pub tmp_storage_on_oom: bool,
    /// Go's `config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8` atomic
    /// (`sysvar.go`, the `tidb_check_mb4_value_in_utf8` instance hook).
    pub check_mb4_value_in_utf8: bool,
    /// Go's process-wide typed `vardef.EnableCheckConstraint` atomic.
    pub enable_check_constraint: bool,
}

impl Default for ResolvedGlobals {
    fn default() -> Self {
        Self {
            values: std::boxed::Box::default(),
            oom_action: tidb_vardef::OomAction::Cancel,
            tmp_storage_on_oom: true,
            check_mb4_value_in_utf8: true,
            enable_check_constraint: false,
        }
    }
}

impl ResolvedGlobals {
    /// Writes one slot by name; `None` records "at the registry default".
    pub fn note(&mut self, name: &str, value: Option<&str>) {
        if let Some(index) = crate::sysvar::sys_var_index_lookup(name) {
            if let Some(slot) = self.values.get_mut(index) {
                *slot = value.map(Arc::from);
            }
        }
    }
}

pub use crate::VarError;

impl crate::session_vars::SystemVars for SessionVars {
    fn get_system(&self, name: &str) -> Option<String> {
        SessionVars::get_system(self, name).ok()
    }
}

/// A session's system-variable state: the variables it has overridden (Go's
/// `SessionVars.systems`) plus the shared GLOBAL table.
///
/// User variables are NOT here. They live in one place -- the shared UserVars owner
/// [`crate::Session`] lends to every statement context -- because `@x := expr`
/// writes them from inside expression evaluation, mid-row; see
/// `tidb_sessionctx::StmtContext`'s `user_vars` field.
/// go `GetSessionVar`'s SysTimestamp hook: an explicit override that is not
/// the default (`'0'`, empty) reads back verbatim; anything else reads the
/// STATEMENT time, formatted like strconv.FormatFloat(v, 'f', -1, 64).
fn timestamp_hook_value(override_value: Option<&str>) -> String {
    if let Some(value) = override_value {
        if !value.is_empty() && value != "0" {
            return value.to_owned();
        }
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = now.as_secs() as f64 + now.subsec_nanos() as f64 / 1e9;
    format_double(seconds)
}

/// Go `txnIsolationLevelOneShotState`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TxnIsolationLevelOneShotState {
    /// Go `oneShotDef`: `tx_isolation_one_shot` is not set.
    Def,
    /// Go `oneShotSet`: set by the current statement.
    Set,
    /// Go `oneShotUse`: the level of the transaction that comes next.
    Use,
}

#[derive(Clone, Debug)]
pub struct SessionVars {
    systems: HashMap<String, String>,
    /// Go `StmtCtx.AppendWarning` as the sysvar hooks reach it: what a SET's
    /// `Validation` / `SetSession` / `SetGlobal` hooks appended, which the
    /// session moves onto the statement (see `crate::set_hooks`).
    pub(crate) stmt_warnings: Vec<tidb_context::SqlWarn>,
    /// Go's typed `ServerStatusAutocommit` bit.
    ///
    /// The normalized system-variable text remains the SQL read authority;
    /// transaction, planner-cache, process-list, and wire-status consumers
    /// read this field exactly as Go reads `SessionVars.status`.
    autocommit: bool,
    /// Go's typed `SessionVars.SQLMode`, maintained by the sql_mode sysvar's
    /// `SetSession` hook and read directly by parser and executor consumers.
    sql_mode: tidb_mysql::SqlMode,
    /// Go's typed `SessionVars.MaxAllowedPacket`, maintained by the sysvar's
    /// `SetSession` hook and read directly by wire and builtin consumers.
    max_allowed_packet: u64,
    /// Go's typed `SessionVars.MaxKeysRead`, maintained by the
    /// `tidb_max_keys_read` `SetSession` hook. The Go accessor returns this
    /// value only while a SELECT statement is active; callers pass that
    /// statement-shape bit to [`Self::max_keys_read`].
    max_keys_read: u64,
    /// Go's typed `SessionVars.MaxExecutionTime`, maintained by the
    /// `max_execution_time` `SetSession` hook and read by statement contexts
    /// as a millisecond deadline (zero means unlimited).
    max_execution_time: u64,
    /// Go's typed `SessionVars.TimeZone`, resolved once by the time-zone
    /// `SetSession` hook rather than reparsed by every statement.
    time_zone: tidb_datatype::SessionTimeZone,
    /// Go SnapshotTS: resolved by SET in that session's location, not on each read.
    snapshot_ts: u64,
    txn_read_ts: u64,
    txn_read_ts_used: bool,
    /// Go `SessionVars.txnIsolationLevelOneShot`: the level `SET TRANSACTION
    /// ISOLATION LEVEL` (the `tx_isolation_one_shot` hook) gives the next
    /// transaction, and where it stands in its one-shot lifecycle.
    txn_isolation_level_one_shot: (TxnIsolationLevelOneShotState, String),
    /// Go's typed `SessionVars.SelectLimit`, maintained by the
    /// `sql_select_limit` `SetSession` hook. `u64::MAX` is the unlimited
    /// default and any smaller value caps top-level SELECT/set results.
    select_limit: u64,
    /// Go's typed `SessionVars.SelectivityFactor`, maintained by the
    /// `tidb_opt_selectivity_factor` `SetSession` hook.
    selectivity_factor: f64,
    /// Go's typed `SessionVars.MultiStatementMode`: OFF=0, ON=1, WARN=2.
    /// The normalized enum value drives COM_QUERY multi-statement admission.
    multi_statement_mode: u8,
    /// Go's typed `SessionVars.EnablePreparedPlanCache`, maintained by the
    /// `tidb_enable_prepared_plan_cache` sysvar's `SetSession` hook.
    enable_prepared_plan_cache: bool,
    /// Go's typed `SessionVars.SharedLockPromotion`, maintained by the
    /// `tidb_enable_shared_lock_promotion` sysvar's `SetSession` hook.
    shared_lock_promotion: bool,
    /// Go's typed `SessionVars.EnableWindowFunction`, maintained by the
    /// `tidb_enable_window_function` sysvar's `SetSession` hook.
    enable_window_function: bool,
    /// Go's typed `SessionVars.TiFlashMaxBytesBeforeExternalJoin`, maintained
    /// by the corresponding TiFlash threshold `SetSession` hook.
    ti_flash_max_bytes_before_ext_join: i64,
    /// Go's typed `SessionVars.TiFlashMaxBytesBeforeExternalGroupBy`.
    ti_flash_max_bytes_before_ext_agg: i64,
    /// Go's typed `SessionVars.TiFlashMaxBytesBeforeExternalSort`.
    ti_flash_max_bytes_before_ext_sort: i64,
    /// Go's typed `SessionVars.TiFlashMemQuotaQueryPerNode`.
    ti_flash_mem_quota_query_per_node: i64,
    /// Go's typed `SessionVars.TiFlashQuerySpillRatio`.
    ti_flash_query_spill_ratio: f64,
    /// Go's typed `SessionVars.PessimisticTransactionFairLocking`.
    pessimistic_transaction_fair_locking: bool,
    /// Go's typed `SessionVars.BulkDMLEnabled`, maintained by
    /// `tidb_dml_type`'s SetSession hook.
    bulk_dml_enabled: bool,
    /// Go's typed replica-read selection, maintained by the
    /// `tidb_replica_read` SetSession hook.
    replica_read: tidb_txnkv::ReplicaReadType,
    /// Go's typed `SessionVars.AnalyzeStoreBatchSize`, maintained by the
    /// `tidb_analyze_store_batch_size` SetSession hook. Zero disables
    /// Analyze store batching.
    analyze_store_batch_size: i64,
    /// Go's typed `SessionVars.QueryCopStoreLimit`, maintained by the
    /// `tidb_query_cop_store_limit` SetSession hook. Zero disables the
    /// per-store query limiter.
    query_cop_store_limit: i64,
    /// Bumped by every mutation of `systems`, so a caller can cache what it
    /// PARSES out of the raw text -- chiefly the optimizer's cost environment
    /// -- and re-derive only when a `SET`
    /// actually happened. Go holds the same products as typed fields on
    /// `SessionVars` updated by each variable's `SetSession` hook; a
    /// generation stamp buys that read cost without a hook per variable.
    /// Session-scoped reads never consult the shared globals
    /// (`get_system`'s fallback is the static default), so this counter
    /// alone is a complete invalidation key for them.
    generation: u64,
    /// Parsed authority kept in lockstep with the raw system-variable text.
    optimizer_fix_control: OptimizerFixControl,
    /// The session-tier read-mostly image (`ResolvedGlobals`, same shape as
    /// the global one): rebuilt whenever `systems` mutates, consulted by
    /// [`Self::get_system`] ahead of the authoritative map so a statement's
    /// dozens of variable reads cost one fixed-seed probe plus an `Arc` slot
    /// check instead of a SipHash probe and a String clone apiece.
    session_resolved: ResolvedGlobals,
    /// Go `SessionVars.GlobalVarsAccessor`: the GLOBAL and INSTANCE tiers,
    /// which the session installs when it is created (Go `createSession`
    /// sets it to the session itself). `None` until then, Go's nil.
    pub(crate) globals: Option<Arc<dyn crate::GlobalVarAccessor>>,
    /// The GLOBAL values this session read for variables `SkipInit` left
    /// out of its copy; see [`Self::unseeded_global_value`].
    first_read_globals: FirstReadGlobals,
    /// Go `SessionVars.InMultiStmts`: the running COM_QUERY text holds more
    /// than one statement, which keeps the non-prepared plan cache out.
    /// (Go `SessionVars.InMultiStmts`.)
    pub in_multi_stmts: bool,
    /// Go `SessionVars.SysWarningCount` / `SysErrorCount`: the PREVIOUS
    /// statement's counts, which is what `@@warning_count` and
    /// `@@error_count` report.
    ///
    /// `ResetContextOfStmt` snapshots them from the outgoing
    /// `StmtCtx.NumErrorWarnings()` at every statement start, whatever the
    /// incoming statement is -- unlike the buffer above, which only the
    /// statements that REPORT warnings inherit. Reading the buffer instead
    /// answered `0` for every statement that is not one of those three.
    /// (Go `SessionVars.SysWarningCount`.)
    pub sys_warning_count: usize,
    /// Go `SessionVars.SysErrorCount`.
    pub sys_error_count: usize,
    /// Go `SessionVars.User` in its two spellings: the matched grant
    /// identity `CURRENT_USER()` reports and the login identity `USER()`
    /// reports. Empty until a front end authenticates one.
    /// (Go `SessionVars.User (rendered)`.)
    pub current_user: Option<String>,
    /// Go `SessionVars.User.AuthUsername@AuthHostname (rendered)`.
    pub login_user: Option<String>,
    /// Go `SessionVars.ActiveRoles`: the `(user, host)` of each role `SET
    /// ROLE` activated for this session.
    pub active_roles: std::sync::Arc<Vec<(String, String)>>,
    /// Go `SessionVars.ConnectionID`, which `CONNECTION_ID()` reports.
    /// `None` for a session with no connection identity, where the builtin
    /// answers NULL like `CURRENT_USER()` does for an unauthenticated one.
    /// (Go `SessionVars.ConnectionID`.)
    pub connection_id: Option<u64>,
    /// Go `SessionVars.ClientCapability & mysql.ClientFoundRows`: negotiated
    /// once by the front end and copied into every statement context.
    /// (Go `SessionVars.ClientCapability & ClientFoundRows`.)
    pub client_found_rows: bool,
    /// Go `SessionVars.PrevLastInsertID`: the id `LAST_INSERT_ID()` reports,
    /// which only a statement that ALLOCATED an auto value updates.
    /// (Go `SessionVars.LastInsertID`.)
    pub last_insert_id: u64,
    /// Go `SessionVars.InSetSessionStatesStmt`: the running statement is
    /// `SET SESSION_STATES`, which keeps the previous `ROW_COUNT()`.
    pub in_set_session_states_stmt: bool,
    /// The id the last statement allocated, which the OK packet carries and
    /// which is 0 for a statement that allocated nothing.
    /// (Go `SessionVars.StmtCtx.InsertID`.)
    pub statement_insert_id: u64,
    /// Go `StmtCtx.PrevAffectedRows`, which is all `ROW_COUNT()` reports: the
    /// preceding statement's affected rows, `-1` after a SELECT, `0`
    /// otherwise. Derived once at the statement boundary from
    /// [`Session::statement_kind`], so the function and the OK packet cannot
    /// disagree about what the statement did.
    /// (Go `SessionVars.PrevAffectedRows / StmtCtx.PrevAffectedRows`.)
    pub prev_row_count: i64,
    /// Go `SessionVars.LastFoundRows`: the row count of the last result set
    /// drained to EOF. Non-result statements leave it unchanged.
    /// (Go `SessionVars.LastFoundRows`.)
    pub last_found_rows: u64,
    /// Go `SessionVars.CurrentDBChanged`: a `USE` switched databases since
    /// the last slow-log entry, which then prints `use <db>;`.
    /// (Go `SessionVars.the CurrentDB change flag`.)
    pub current_db_changed: bool,
    /// Go `SessionVars.PrevStmt`: the previous statement's loggable text
    /// (`GetTextToLog(false)`), which a slow `COMMIT` prints as `Prev_stmt`.
    /// (Go `SessionVars.PrevStmt`.)
    pub prev_stmt_text: Option<String>,
    /// Go `SessionVars.FoundInPlanCache`: whether the statement RUNNING now
    /// found its plan in the cache. Reset for every statement.
    /// (Go `SessionVars.FoundInPlanCache`.)
    pub found_in_plan_cache: bool,
    /// Go `SessionVars.PrevFoundInPlanCache`, which is what
    /// `@@last_plan_from_cache` reads -- the PRECEDING statement's value,
    /// promoted at the statement boundary, since the reading `SELECT` is
    /// itself never cacheable and would otherwise always answer 0.
    /// (Go `SessionVars.PrevFoundInPlanCache`.)
    pub prev_found_in_plan_cache: bool,
    /// Go `StmtCtx.useChunkAlloc` for the running statement: its result was
    /// produced through a valid reusable chunk allocator.
    /// (Go `SessionVars.UseChunkAlloc`.)
    pub use_chunk_alloc: bool,
    /// Go `SessionVars.preUseChunkAlloc`, promoted from the previous
    /// statement by `ExchangeChunkStatus`; `@@last_sql_use_alloc` reads it.
    /// (Go `SessionVars.PreUseChunkAlloc`.)
    pub pre_use_chunk_alloc: bool,
    /// Go `SessionVars.CurrentDB`: the schema an unqualified name resolves in.
    /// Empty means no database is selected, which is Go's `ErrNoDB` case.
    /// (Go `SessionVars.CurrentDB`.)
    pub current_db: String,
    /// Go `session.sandboxMode`: this connection logged in with an EXPIRED
    /// password while the server allowed it, so it may run nothing but the
    /// `SET PASSWORD` / `ALTER USER` that fixes the password. Set by the
    /// front end from the login's verdict, cleared by the statement that
    /// stores a new password.
    /// (Go `SessionVars.InSandBoxMode`.)
    pub sandbox_mode: bool,
    /// Go `SessionVars.FoundInBinding`: whether the statement RUNNING now
    /// took its hints from a binding.
    /// (Go `SessionVars.FoundInBinding`.)
    pub found_in_binding: bool,
    /// Go `SessionVars.PrevFoundInBinding`, which is what
    /// `@@last_plan_from_binding` reads -- the PRECEDING statement's value,
    /// promoted at the statement boundary for the same reason
    /// `@@last_plan_from_cache` is.
    /// (Go `SessionVars.PrevFoundInBinding`.)
    pub prev_found_in_binding: bool,
    /// Go `SessionVars.ResourceGroupName`: the connection's selected resource
    /// group. A statement-level `RESOURCE_GROUP` hint may override this value
    /// for one statement, but never mutates it.
    /// (Go `SessionVars.ResourceGroupName`.)
    pub resource_group: String,
    /// Go `SessionVars.InRestrictedSQL`.
    pub restricted_sql: bool,
    /// Go `SessionVars.TLSConnectionState`: the connection's negotiated TLS
    /// state, `None` for a connection without TLS.
    pub tls_connection_state: Option<TlsConnectionState>,
    /// Go `SessionVars.CompressionAlgorithm`: `mysql.CompressionNone`,
    /// `CompressionZlib` or `CompressionZstd`.
    pub compression_algorithm: u8,
    /// Go `SessionVars.CompressionLevel`: the zstd level the client asked for.
    pub compression_level: i32,
    /// Go `SessionVars.KeysExamined`: the keys this session's coprocessor
    /// reads processed, the `tidb_keys_examined` status.
    pub keys_examined: u64,
    /// Go `SessionVars.preparedStmtID`.
    pub prepared_statement_id: u32,
    /// Go `SessionVars.LastTxnInfo` (`pkg/sessionctx/variable/session.go:1467`):
    /// client-go's `TxnInfo` JSON for the last transaction that ACTIVATED --
    /// full (with `commit_ts`) after a commit, start-only otherwise, and
    /// untouched by a statement that never took a timestamp (`SELECT 1`).
    /// Empty until the first one, as Go's is.
    /// (Go `SessionVars.LastTxnInfo`.)
    pub last_txn_info: String,
    /// Go `SessionVars.LastQueryInfo`, exposed by the read-only
    /// `tidb_last_query_info` getter.
    pub last_query_info: tidb_sessionstates::QueryInfo,
    /// Go `SessionVars.LastDDLInfo`: the query and sequence number of the
    /// last DDL job this session waited for.
    pub last_ddl_info: tidb_sessionstates::LastDDLInfo,
}

/// Go `GetSessionOrGlobalSystemVar`'s lazy population of `s.systems` for a
/// `SkipInit` variable, keyed by registry index. Reads go through `&self`,
/// so the first value read is recorded behind a lock.
#[derive(Debug, Default)]
struct FirstReadGlobals(Mutex<HashMap<usize, String>>);

impl Clone for FirstReadGlobals {
    fn clone(&self) -> Self {
        Self(Mutex::new(
            self.0
                .lock()
                .expect("first-read globals lock poisoned")
                .clone(),
        ))
    }
}

/// Resolves the validated `time_zone` text into the statement-facing zone
/// type. Go's `timeutil.ParseTimeZone` preserves the original `+HH:MM` name
/// on the session location even though the DAGR request later sends an empty
/// name for fixed offsets; retaining that text here keeps both behaviors.
fn resolve_session_time_zone_value(written: &str) -> tidb_datatype::SessionTimeZone {
    use tidb_datatype::SessionTimeZone;

    if !written.eq_ignore_ascii_case("SYSTEM") {
        if let Ok(zone) = written.parse::<chrono_tz::Tz>() {
            return SessionTimeZone::Named(zone);
        }
        if let Some(rest) = written.strip_prefix(['+', '-']) {
            let negative = written.starts_with('-');
            let mut parts = rest.split(':');
            let hours: i32 = parts.next().unwrap_or_default().parse().unwrap_or(-1);
            let minutes: i32 = parts.next().unwrap_or("0").parse().unwrap_or(-1);
            if hours >= 0 && (0..60).contains(&minutes) {
                let offset = hours * 3600 + minutes * 60;
                let bounded = if negative {
                    offset <= 12 * 3600 + 59 * 60
                } else {
                    offset <= 14 * 3600
                };
                if bounded {
                    return SessionTimeZone::Fixed {
                        name: written.to_owned(),
                        offset_secs: if negative { -offset } else { offset },
                    };
                }
            }
        }
    }

    // SYSTEM is TiDB's process-wide SystemLocation, not an offset snapshot.
    // Preserve a resolved IANA zone (and therefore DST), with the process
    // local zone as the same fallback Go uses.
    match tidb_timeutil::system_location() {
        tidb_timeutil::TimeZone::Local => SessionTimeZone::Local,
        tidb_timeutil::TimeZone::Named(zone) => SessionTimeZone::Named(zone),
        tidb_timeutil::TimeZone::Fixed { name, offset_secs } => {
            SessionTimeZone::Fixed { name, offset_secs }
        }
    }
}

/// Go `crypto/tls.ConnectionState`, the fields TiDB reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TlsConnectionState {
    /// Go `Version`.
    pub version: u16,
    /// Go `CipherSuite`.
    pub cipher_suite: u16,
}

impl Default for SessionVars {
    fn default() -> Self {
        Self {
            stmt_warnings: Vec::new(),
            last_txn_info: String::new(),
            last_query_info: tidb_sessionstates::QueryInfo::default(),
            last_ddl_info: tidb_sessionstates::LastDDLInfo::default(),
            resource_group: "default".to_owned(),
            restricted_sql: false,
            tls_connection_state: None,
            compression_algorithm: tidb_mysql::consts::CompressionNone,
            compression_level: 0,
            keys_examined: 0,
            prepared_statement_id: 0,
            in_multi_stmts: false,
            sys_warning_count: 0,
            sys_error_count: 0,
            current_user: None,
            active_roles: std::sync::Arc::default(),
            login_user: None,
            connection_id: None,
            client_found_rows: false,
            last_insert_id: 0,
            in_set_session_states_stmt: false,
            statement_insert_id: 0,
            prev_row_count: 0,
            last_found_rows: 0,
            current_db_changed: false,
            prev_stmt_text: None,
            found_in_plan_cache: false,
            prev_found_in_plan_cache: false,
            use_chunk_alloc: false,
            pre_use_chunk_alloc: tidb_vardef::defaults::DEF_TIDB_USE_ALLOC,
            current_db: String::new(),
            sandbox_mode: false,
            found_in_binding: false,
            prev_found_in_binding: false,
            systems: HashMap::new(),
            autocommit: true,
            sql_mode: tidb_mysql::get_sql_mode(tidb_mysql::DefaultSQLMode)
                .expect("the compiled default SQL mode is valid"),
            max_allowed_packet: 64 << 20,
            max_keys_read: 0,
            max_execution_time: 0,
            time_zone: resolve_session_time_zone_value("SYSTEM"),
            snapshot_ts: 0,
            txn_read_ts: 0,
            txn_read_ts_used: false,
            txn_isolation_level_one_shot: (TxnIsolationLevelOneShotState::Def, String::new()),
            select_limit: u64::MAX,
            selectivity_factor: tidb_vardef::defaults::DEF_OPT_SELECTIVITY_FACTOR,
            multi_statement_mode: 0,
            enable_prepared_plan_cache: tidb_vardef::defaults::DEF_TIDB_ENABLE_PREP_PLAN_CACHE,
            shared_lock_promotion: tidb_vardef::defaults::DEF_TIDB_ENABLE_SHARED_LOCK_PROMOTION,
            enable_window_function: tidb_vardef::defaults::DEF_ENABLE_WINDOW_FUNCTION,
            ti_flash_max_bytes_before_ext_join:
                tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_JOIN,
            ti_flash_max_bytes_before_ext_agg:
                tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY,
            ti_flash_max_bytes_before_ext_sort:
                tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_SORT,
            ti_flash_mem_quota_query_per_node:
                tidb_vardef::defaults::DEF_TIFLASH_MEM_QUOTA_QUERY_PER_NODE,
            ti_flash_query_spill_ratio: tidb_vardef::defaults::DEF_TIFLASH_QUERY_SPILL_RATIO,
            pessimistic_transaction_fair_locking: false,
            bulk_dml_enabled: false,
            replica_read: tidb_txnkv::ReplicaReadType::Leader,
            analyze_store_batch_size: tidb_vardef::defaults::DEF_TIDB_ANALYZE_STORE_BATCH_SIZE,
            query_cop_store_limit: tidb_vardef::defaults::DEF_TIDB_QUERY_COP_STORE_LIMIT,
            generation: 0,
            optimizer_fix_control: OptimizerFixControl::default(),
            session_resolved: ResolvedGlobals::default(),
            globals: None,
            first_read_globals: FirstReadGlobals::default(),
        }
    }
}

impl SessionVars {
    /// Go `GetSession`'s `tidb_last_txn_info` getter.
    pub fn last_txn_info_value(&self) -> String {
        self.last_txn_info.clone()
    }

    /// Go `GetSession`'s `tidb_last_query_info` getter:
    /// `json.Marshal(s.LastQueryInfo)`.
    pub fn last_query_info_value(&self) -> String {
        tidb_stringutil::go_json_marshal(&self.last_query_info)
    }

    /// Go `GetSession`'s `tidb_last_ddl_info` getter:
    /// `json.Marshal(s.LastDDLInfo)`.
    pub fn last_ddl_info_value(&self) -> String {
        tidb_stringutil::go_json_marshal(&self.last_ddl_info)
    }

    /// Restore only Go's typed SnapshotTS after failed storage/schema validation.
    /// SetExecutor leaves the system-variable string as assigned.
    pub fn restore_snapshot_ts(&mut self, ts: u64) {
        self.snapshot_ts = ts;
    }

    /// Go TxnReadTS: one selection, consumed when the selected transaction ends.
    pub fn txn_read_ts(&self) -> u64 {
        self.txn_read_ts
    }

    pub fn set_txn_read_ts(&mut self, ts: u64) {
        self.txn_read_ts = ts;
        self.txn_read_ts_used = false;
    }

    pub fn use_txn_read_ts(&mut self) -> u64 {
        self.txn_read_ts_used = true;
        self.txn_read_ts
    }

    pub fn cleanup_txn_read_ts(&mut self) -> bool {
        if self.txn_read_ts_used && self.txn_read_ts != 0 {
            self.set_txn_read_ts(0);
            return true;
        }
        false
    }

    /// Go `SessionVars.IsolationLevelForNewTxn`: the level a transaction
    /// entered now runs at -- a `SET TRANSACTION ISOLATION LEVEL` level
    /// (just set inside a transaction, or waiting outside one), else
    /// `transaction_isolation`.
    pub fn isolation_level_for_new_txn(&self, in_txn: bool) -> String {
        let (state, value) = &self.txn_isolation_level_one_shot;
        let pending = if in_txn {
            TxnIsolationLevelOneShotState::Set
        } else {
            TxnIsolationLevelOneShotState::Use
        };
        if *state == pending && !value.is_empty() {
            return value.clone();
        }
        self.get_system("transaction_isolation").unwrap_or_default()
    }

    /// The `SET TRANSACTION ISOLATION LEVEL` level Go `IsIsolation` adopts
    /// for a transaction context that has none yet (`oneShotUse`).
    pub fn txn_isolation_level_one_shot_in_use(&self) -> Option<&str> {
        let (state, value) = &self.txn_isolation_level_one_shot;
        (*state == TxnIsolationLevelOneShotState::Use && !value.is_empty())
            .then_some(value.as_str())
    }

    /// Go `SetTxnIsolationLevelOneShotStateForNextTxn`, run by every
    /// `CommitTxn` (`doCommitWithRetry`): a level set by `SET TRANSACTION`
    /// waits for the next transaction, and that transaction's end clears it.
    pub fn set_txn_isolation_level_one_shot_state_for_next_txn(&mut self) {
        let one_shot = &mut self.txn_isolation_level_one_shot;
        match one_shot.0 {
            TxnIsolationLevelOneShotState::Def => {}
            TxnIsolationLevelOneShotState::Set => one_shot.0 = TxnIsolationLevelOneShotState::Use,
            TxnIsolationLevelOneShotState::Use => {
                one_shot.0 = TxnIsolationLevelOneShotState::Def;
                one_shot.1.clear();
            }
        }
    }

    /// The snapshot selected by SET tidb_snapshot; zero clears the pin.
    pub fn snapshot_ts(&self) -> u64 {
        self.snapshot_ts
    }

    /// A session with every variable at its registry default and its own,
    /// unshared global table (used by tests and any standalone session that
    /// has no factory to share one with).
    #[must_use]
    pub fn new() -> Self {
        SessionVars::default()
    }

    /// Points this session at a shared GLOBAL accessor and snapshots
    /// its current overrides into the session's own copy -- Go's rule that a
    /// session's variables are copied from the global tier once, at connect.
    /// A `SET GLOBAL` another session runs AFTER this call is invisible to
    /// this session's plain `@@x` (only to `@@global.x`), exactly as MySQL
    /// documents.
    pub fn seed_from_globals(
        &mut self,
        globals: Arc<dyn crate::GlobalVarAccessor>,
    ) -> Result<(), VarError> {
        let mut systems = self.systems.clone();
        // Go `loadCommonGlobalVariablesIfNeeded`: the deep copy of the sysvar
        // cache's session view (`Domain.GetSessionCache`), which holds every
        // variable `SkipInit` does not exclude -- session scope, not noop,
        // not one of Go's `skipInit` variables -- at its GLOBAL value or,
        // for a session-only variable, its registry value.
        for (name, value) in globals.session_cache() {
            systems.insert(name, value);
        }
        let raw = systems
            .get(tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL)
            .cloned()
            .unwrap_or_else(|| {
                get_sys_var(tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL)
                    .map_or_else(String::new, |def| def.value.to_owned())
            });
        let optimizer_fix_control = OptimizerFixControl::parse(&raw)
            .map_err(|error| tidb_terror::MysqlError::unknown(error.to_string()))?
            .0;
        let autocommit = Self::autocommit_from_systems(&systems);
        let sql_mode = Self::sql_mode_from_systems(&systems)
            .map_err(|error| tidb_terror::MysqlError::unknown(error.to_string()))?;
        let max_allowed_packet = Self::max_allowed_packet_from_systems(&systems)?;
        let max_keys_read = Self::max_keys_read_from_systems(&systems);
        let max_execution_time = Self::max_execution_time_from_systems(&systems);
        let time_zone = Self::time_zone_from_systems(&systems);
        let select_limit = Self::select_limit_from_systems(&systems);
        let selectivity_factor = Self::selectivity_factor_from_systems(&systems);
        let multi_statement_mode = Self::multi_statement_mode_from_systems(&systems);
        let enable_prepared_plan_cache = Self::prepared_plan_cache_from_systems(&systems);
        let shared_lock_promotion = Self::shared_lock_promotion_from_systems(&systems);
        let enable_window_function = Self::enable_window_function_from_systems(&systems);
        let ti_flash_max_bytes_before_ext_join =
            Self::ti_flash_max_bytes_before_ext_join_from_systems(&systems);
        let ti_flash_max_bytes_before_ext_agg =
            Self::ti_flash_max_bytes_before_ext_agg_from_systems(&systems);
        let ti_flash_max_bytes_before_ext_sort =
            Self::ti_flash_max_bytes_before_ext_sort_from_systems(&systems);
        let ti_flash_mem_quota_query_per_node =
            Self::ti_flash_mem_quota_query_per_node_from_systems(&systems);
        let ti_flash_query_spill_ratio = Self::ti_flash_query_spill_ratio_from_systems(&systems);
        let pessimistic_transaction_fair_locking =
            Self::pessimistic_transaction_fair_locking_from_systems(&systems);
        let bulk_dml_enabled = Self::bulk_dml_enabled_from_systems(&systems);
        let replica_read = Self::replica_read_from_systems(&systems);
        let analyze_store_batch_size = Self::analyze_store_batch_size_from_systems(&systems);
        let query_cop_store_limit = Self::query_cop_store_limit_from_systems(&systems);
        // Commit all authorities only after the inherited fix-control
        // row has been accepted. A stale/foreign cluster row can therefore
        // refuse the connection without partially reseeding this session.
        self.systems = systems;
        self.globals = Some(globals);
        self.first_read_globals = FirstReadGlobals::default();
        self.optimizer_fix_control = optimizer_fix_control;
        self.autocommit = autocommit;
        self.sql_mode = sql_mode;
        self.max_allowed_packet = max_allowed_packet;
        self.max_keys_read = max_keys_read;
        self.max_execution_time = max_execution_time;
        self.time_zone = time_zone;
        self.select_limit = select_limit;
        self.selectivity_factor = selectivity_factor;
        self.multi_statement_mode = multi_statement_mode;
        self.enable_prepared_plan_cache = enable_prepared_plan_cache;
        self.shared_lock_promotion = shared_lock_promotion;
        self.enable_window_function = enable_window_function;
        self.ti_flash_max_bytes_before_ext_join = ti_flash_max_bytes_before_ext_join;
        self.ti_flash_max_bytes_before_ext_agg = ti_flash_max_bytes_before_ext_agg;
        self.ti_flash_max_bytes_before_ext_sort = ti_flash_max_bytes_before_ext_sort;
        self.ti_flash_mem_quota_query_per_node = ti_flash_mem_quota_query_per_node;
        self.ti_flash_query_spill_ratio = ti_flash_query_spill_ratio;
        self.pessimistic_transaction_fair_locking = pessimistic_transaction_fair_locking;
        self.bulk_dml_enabled = bulk_dml_enabled;
        self.replica_read = replica_read;
        self.analyze_store_batch_size = analyze_store_batch_size;
        self.query_cop_store_limit = query_cop_store_limit;
        self.session_resolved = Self::build_session_image(&self.systems);
        // The wholesale replacement above is a mutation like any other; the
        // parsed-product caches keyed on `generation` must not survive it.
        self.generation += 1;
        Ok(())
    }

    fn autocommit_from_systems(systems: &HashMap<String, String>) -> bool {
        systems.get("autocommit").map_or(true, |value| {
            value.eq_ignore_ascii_case("ON") || value == "1"
        })
    }

    fn sql_mode_from_systems(
        systems: &HashMap<String, String>,
    ) -> Result<tidb_mysql::SqlMode, tidb_mysql::InvalidSqlMode> {
        tidb_mysql::get_sql_mode(
            systems
                .get("sql_mode")
                .map_or(tidb_mysql::DefaultSQLMode, String::as_str),
        )
    }

    fn max_allowed_packet_from_systems(systems: &HashMap<String, String>) -> Result<u64, VarError> {
        systems
            .get("max_allowed_packet")
            .map_or("67108864", String::as_str)
            .parse::<u64>()
            .map_err(|error| tidb_terror::MysqlError::unknown(error.to_string()))
    }

    fn max_keys_read_from_systems(systems: &HashMap<String, String>) -> u64 {
        systems
            .get("tidb_max_keys_read")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    }

    fn max_execution_time_from_systems(systems: &HashMap<String, String>) -> u64 {
        systems
            .get("max_execution_time")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0)
    }

    fn time_zone_from_systems(systems: &HashMap<String, String>) -> tidb_datatype::SessionTimeZone {
        resolve_session_time_zone_value(systems.get("time_zone").map_or("SYSTEM", String::as_str))
    }

    fn select_limit_from_systems(systems: &HashMap<String, String>) -> u64 {
        systems
            .get("sql_select_limit")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(u64::MAX)
    }

    fn selectivity_factor_from_systems(systems: &HashMap<String, String>) -> f64 {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_OPT_SELECTIVITY_FACTOR)
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_OPT_SELECTIVITY_FACTOR)
    }

    fn multi_statement_mode_from_systems(systems: &HashMap<String, String>) -> u8 {
        match systems.get("tidb_multi_statement_mode").map(String::as_str) {
            Some("ON") => 1,
            Some("WARN") => 2,
            _ => 0,
        }
    }

    fn prepared_plan_cache_from_systems(systems: &HashMap<String, String>) -> bool {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_ENABLE_PREP_PLAN_CACHE)
            .map_or(
                tidb_vardef::defaults::DEF_TIDB_ENABLE_PREP_PLAN_CACHE,
                |value| value == "ON",
            )
    }

    fn shared_lock_promotion_from_systems(systems: &HashMap<String, String>) -> bool {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_ENABLE_SHARED_LOCK_PROMOTION)
            .map_or(
                tidb_vardef::defaults::DEF_TIDB_ENABLE_SHARED_LOCK_PROMOTION,
                |value| value == "ON",
            )
    }

    fn enable_window_function_from_systems(systems: &HashMap<String, String>) -> bool {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_ENABLE_WINDOW_FUNCTION)
            .map_or(tidb_vardef::defaults::DEF_ENABLE_WINDOW_FUNCTION, |value| {
                value.eq_ignore_ascii_case("ON") || value == "1"
            })
    }

    fn ti_flash_max_bytes_before_ext_join_from_systems(systems: &HashMap<String, String>) -> i64 {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_JOIN)
    }

    fn ti_flash_max_bytes_before_ext_agg_from_systems(systems: &HashMap<String, String>) -> i64 {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY)
    }

    fn ti_flash_max_bytes_before_ext_sort_from_systems(systems: &HashMap<String, String>) -> i64 {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_SORT)
    }

    fn ti_flash_mem_quota_query_per_node_from_systems(systems: &HashMap<String, String>) -> i64 {
        systems
            .get(tidb_vardef::tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIFLASH_MEM_QUOTA_QUERY_PER_NODE)
    }

    fn ti_flash_query_spill_ratio_from_systems(systems: &HashMap<String, String>) -> f64 {
        systems
            .get(tidb_vardef::tidb_vars::TIFLASH_QUERY_SPILL_RATIO)
            .and_then(|value| value.parse::<f64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIFLASH_QUERY_SPILL_RATIO)
    }

    fn pessimistic_transaction_fair_locking_from_systems(
        systems: &HashMap<String, String>,
    ) -> bool {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING)
            .is_some_and(|value| value.eq_ignore_ascii_case("ON") || value == "1")
    }

    fn bulk_dml_enabled_from_systems(systems: &HashMap<String, String>) -> bool {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_DML_TYPE)
            .is_some_and(|value| value.eq_ignore_ascii_case("bulk"))
    }

    fn replica_read_from_systems(
        systems: &HashMap<String, String>,
    ) -> tidb_txnkv::ReplicaReadType {
        match systems
            .get(tidb_vardef::tidb_vars::TIDB_REPLICA_READ)
            .map(String::as_str)
        {
            Some(value) if value.eq_ignore_ascii_case("follower") => {
                tidb_txnkv::ReplicaReadType::Follower
            }
            Some(value) if value.eq_ignore_ascii_case("leader-and-follower") => {
                tidb_txnkv::ReplicaReadType::Mixed
            }
            Some(value) if value.eq_ignore_ascii_case("closest-replicas") => {
                tidb_txnkv::ReplicaReadType::Closest
            }
            Some(value) if value.eq_ignore_ascii_case("closest-adaptive") => {
                tidb_txnkv::ReplicaReadType::ClosestAdaptive
            }
            Some(value) if value.eq_ignore_ascii_case("learner") => {
                tidb_txnkv::ReplicaReadType::Learner
            }
            Some(value) if value.eq_ignore_ascii_case("prefer-leader") => {
                tidb_txnkv::ReplicaReadType::PreferLeader
            }
            _ => tidb_txnkv::ReplicaReadType::Leader,
        }
    }

    fn analyze_store_batch_size_from_systems(systems: &HashMap<String, String>) -> i64 {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_ANALYZE_STORE_BATCH_SIZE)
    }

    fn query_cop_store_limit_from_systems(systems: &HashMap<String, String>) -> i64 {
        systems
            .get(tidb_vardef::tidb_vars::TIDB_QUERY_COP_STORE_LIMIT)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_QUERY_COP_STORE_LIMIT)
    }

    /// Go `SessionVars.IsAutocommit`, backed by its typed server-status bit.
    #[must_use]
    pub const fn is_autocommit(&self) -> bool {
        self.autocommit
    }

    /// Restore the autocommit status bit separately from the sysvar string, as Go does.
    pub fn restore_autocommit_status(&mut self, status: u32) {
        self.autocommit = status & 2 != 0;
    }

    /// Go `SessionVars.SQLMode`, parsed once when its sysvar changes.
    #[must_use]
    pub const fn sql_mode(&self) -> tidb_mysql::SqlMode {
        self.sql_mode
    }

    /// Go `SessionVars.MaxAllowedPacket`, parsed by the sysvar hook when the
    /// session copy changes rather than by each consumer.
    #[must_use]
    pub const fn max_allowed_packet(&self) -> u64 {
        self.max_allowed_packet
    }

    /// Go `SessionVars.GetMaxKeysRead`: `tidb_max_keys_read` limits index
    /// lookup work only inside a SELECT. DML and all non-SELECT statement
    /// contexts observe the zero (unlimited) sentinel even when the session
    /// has configured a positive value.
    #[must_use]
    pub const fn max_keys_read(&self, in_select_stmt: bool) -> u64 {
        if in_select_stmt {
            self.max_keys_read
        } else {
            0
        }
    }

    /// Go `SessionVars.MaxExecutionTime`, in milliseconds. A zero value
    /// preserves TiDB's unlimited-deadline sentinel.
    #[must_use]
    pub const fn max_execution_time(&self) -> u64 {
        self.max_execution_time
    }

    /// Go `SessionVars.TimeZone`, resolved from `SET time_zone` and retained
    /// until the next mutation or session-image reseed.
    #[must_use]
    pub fn session_time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.time_zone.clone()
    }

    /// Go `SessionVars.SelectLimit`, where `u64::MAX` means unlimited.
    #[must_use]
    pub const fn select_limit(&self) -> u64 {
        self.select_limit
    }

    /// Go `SessionVars.SelectivityFactor`, used by cardinality fallback
    /// estimation for predicates without statistics.
    #[must_use]
    pub const fn selectivity_factor(&self) -> f64 {
        self.selectivity_factor
    }

    /// Go `SessionVars.MultiStatementMode`: OFF=0 refuses a multi-statement
    /// COM_QUERY without the client capability, ON=1 admits it, and WARN=2
    /// admits it while deferring warning 8130 to the final statement.
    #[must_use]
    pub const fn multi_statement_mode(&self) -> u8 {
        self.multi_statement_mode
    }

    /// Go `SessionVars.EnablePreparedPlanCache`, updated when its normalized
    /// ON/OFF sysvar changes rather than looked up by each execution.
    #[must_use]
    pub const fn prepared_plan_cache_enabled(&self) -> bool {
        self.enable_prepared_plan_cache
    }

    /// Go `SessionVars.SharedLockPromotion`, which makes `FOR SHARE` use the
    /// exclusive-lock path instead of the no-op shared-lock path.
    #[must_use]
    pub const fn shared_lock_promotion_enabled(&self) -> bool {
        self.shared_lock_promotion
    }

    /// Go `SessionVars.EnableWindowFunction`, updated by the normalized
    /// `tidb_enable_window_function` bool sysvar.
    #[must_use]
    pub const fn window_function_enabled(&self) -> bool {
        self.enable_window_function
    }

    /// Go `SessionVars.BuildParserConfig`: the switches every statement this
    /// session parses runs under, plus the scanner encodings of
    /// `SessionVars.GetParseParams` (`CharsetConnection` from
    /// `character_set_connection`, `CharsetClient` from
    /// `character_set_client`, whose read error is Go's empty name).
    #[must_use]
    pub fn build_parser_config(&self) -> tidb_parser::ParserConfig {
        static CLIENT: crate::sysvar::StaticSysVarIndex =
            crate::sysvar::StaticSysVarIndex::new("character_set_client");
        static CONNECTION: crate::sysvar::StaticSysVarIndex =
            crate::sysvar::StaticSysVarIndex::new("character_set_connection");
        let charset_client = self.system_value_at(CLIENT.get()).unwrap_or_default();
        let charset_connection = self.system_value_at(CONNECTION.get()).unwrap_or_default();
        tidb_parser::ParserConfig {
            enable_window_function: self.enable_window_function,
            enable_strict_double_type_check: self
                .system_value(tidb_vardef::tidb_vars::TIDB_ENABLE_STRICT_DOUBLE_TYPE_CHECK)
                .map_or(
                    tidb_vardef::defaults::DEF_ENABLE_STRICT_DOUBLE_TYPE_CHECK,
                    |value| value.eq_ignore_ascii_case("ON") || value == "1",
                ),
            charset_client: tidb_charset::find_encoding(&charset_client),
            charset_connection: tidb_charset::find_encoding(&charset_connection),
        }
    }

    /// Go `SessionVars.BuildParserConfig()` alone: the session's parser
    /// switches over a fresh parser's default scanner encodings, which is
    /// what a parser the server makes for itself -- a stored view body's,
    /// `BuildDataSourceFromView` -- reads with.
    pub fn build_parser_switches(&self) -> tidb_parser::ParserConfig {
        let config = self.build_parser_config();
        tidb_parser::ParserConfig {
            enable_window_function: config.enable_window_function,
            enable_strict_double_type_check: config.enable_strict_double_type_check,
            ..tidb_parser::ParserConfig::default()
        }
    }

    /// Go `SessionVars.TiFlashMaxBytesBeforeExternalJoin`.
    #[must_use]
    pub const fn ti_flash_max_bytes_before_ext_join(&self) -> i64 {
        self.ti_flash_max_bytes_before_ext_join
    }

    /// Go `SessionVars.TiFlashMaxBytesBeforeExternalGroupBy`.
    #[must_use]
    pub const fn ti_flash_max_bytes_before_ext_agg(&self) -> i64 {
        self.ti_flash_max_bytes_before_ext_agg
    }

    /// Go `SessionVars.TiFlashMaxBytesBeforeExternalSort`.
    #[must_use]
    pub const fn ti_flash_max_bytes_before_ext_sort(&self) -> i64 {
        self.ti_flash_max_bytes_before_ext_sort
    }

    /// Go `SessionVars.TiFlashMemQuotaQueryPerNode`.
    #[must_use]
    pub const fn ti_flash_mem_quota_query_per_node(&self) -> i64 {
        self.ti_flash_mem_quota_query_per_node
    }

    /// Go `SessionVars.TiFlashQuerySpillRatio`.
    #[must_use]
    pub const fn ti_flash_query_spill_ratio(&self) -> f64 {
        self.ti_flash_query_spill_ratio
    }

    /// Go `SessionVars.PessimisticTransactionFairLocking`.
    #[must_use]
    pub const fn pessimistic_transaction_fair_locking_enabled(&self) -> bool {
        self.pessimistic_transaction_fair_locking
    }

    /// Go `SessionVars.BulkDMLEnabled`, set by `tidb_dml_type`.
    #[must_use]
    pub const fn bulk_dml_enabled(&self) -> bool {
        self.bulk_dml_enabled
    }

    /// Go `SessionVars.BatchInsert`, set by `tidb_batch_insert`. Read from
    /// the table rather than cached: only a failed autocommit commit asks.
    #[must_use]
    pub fn batch_insert(&self) -> bool {
        self.get_system(tidb_vardef::tidb_vars::TIDB_BATCH_INSERT)
            .map(|value| value.eq_ignore_ascii_case("ON") || value == "1")
            .unwrap_or(tidb_vardef::defaults::DEF_BATCH_INSERT)
    }

    /// Go `SessionVars.PessimisticTransactionFairLocking`, set by
    /// `tidb_pessimistic_txn_fair_locking`.
    #[must_use]
    pub const fn pessimistic_transaction_fair_locking(&self) -> bool {
        self.pessimistic_transaction_fair_locking
    }

    /// Go `SessionVars.GetReplicaRead`, the typed KV replica-read mode.
    #[must_use]
    pub const fn replica_read(&self) -> tidb_txnkv::ReplicaReadType {
        self.replica_read
    }

    /// Go `SessionVars.AnalyzeStoreBatchSize`, where zero disables Analyze
    /// store batching.
    #[must_use]
    pub const fn analyze_store_batch_size(&self) -> i64 {
        self.analyze_store_batch_size
    }

    /// Go `SessionVars.QueryCopStoreLimit`, where zero disables query-level
    /// per-store coprocessor request limiting.
    #[must_use]
    pub const fn query_cop_store_limit(&self) -> i64 {
        self.query_cop_store_limit
    }

    /// Updates ONE registry-indexed slot of the session image after the
    /// authoritative map changed. Statement-scoped save/restore pairs call
    /// this a handful of times each, so a whole-image rebuild here would run
    /// per statement -- the exact cost the image exists to avoid.
    ///
    /// `None` restores the registry default (the map no longer holds `name`).
    fn note_system_change(&mut self, name: &str) {
        let resolved = match self.systems.get(name) {
            Some(value) => Some(Arc::from(value.as_str())),
            None => None,
        };
        if let Some(index) = crate::sysvar::sys_var_index_lookup(name) {
            if let Some(slot) = self.session_resolved.values.get_mut(index) {
                *slot = resolved;
            }
        }
    }

    /// The session-tier twin of [`GlobalSysvars::refresh_resolved`]: one flat
    /// registry-indexed table of the current `systems` overrides.
    fn build_session_image(systems: &HashMap<String, String>) -> ResolvedGlobals {
        let mut slots: Vec<Option<Arc<str>>> =
            std::vec::Vec::with_capacity(crate::sysvar::SYS_VARS.len());
        slots.resize_with(crate::sysvar::SYS_VARS.len(), || None);
        for (name, value) in systems {
            if let Some(index) = crate::sysvar::sys_var_index_lookup(name) {
                slots[index] = Some(Arc::from(value.as_str()));
            }
        }
        ResolvedGlobals {
            values: slots.into(),
            ..ResolvedGlobals::default()
        }
    }

    /// Reads a system variable the way `SELECT @@name` does.
    ///
    /// A variable with SESSION scope answers from this session's own copy, and
    /// one with GLOBAL scope only answers from the shared table -- Go's
    /// `GetSessionOrGlobalSystemVar` falls through to `GetGlobalSystemVar`
    /// exactly when `sv.HasSessionScope()` is false. Without that fall-through
    /// a global-only variable would report the registry default forever, no
    /// matter what `SET GLOBAL` wrote (captured: after
    /// `set @@global.tidb_mem_oom_action='LOG'`, `select @@tidb_mem_oom_action`
    /// reports `LOG`).
    /// The mutation stamp for [`SessionVars`]-derived caches; see the field.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Reads a system variable without cloning its bytes when the session owns
    /// the value or the registry supplies a static default. This is Rust's
    /// equivalent of Go copying a string header out of `SessionVars.systems`.
    pub fn system_value(&self, name: &str) -> Result<Cow<'_, str>, VarError> {
        let Some(index) = crate::sysvar::sys_var_index_lookup(name) else {
            return Err(tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_UNKNOWN_SYSTEM_VAR
                    .generate_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
            ));
        };
        self.system_value_at(index)
    }

    /// A boolean variable's session value (`ON`, `1`, `TRUE`, `YES`), or
    /// `default` for a name the registry does not know.
    pub fn system_bool(&self, name: &str, default: bool) -> bool {
        match self.system_value(name) {
            Ok(value) => ["ON", "1", "TRUE", "YES"]
                .iter()
                .any(|on| value.eq_ignore_ascii_case(on)),
            Err(_) => default,
        }
    }

    /// [`Self::system_value`] for a caller that already holds the variable's
    /// registry index (see [`crate::sysvar::StaticSysVarIndex`]), which skips
    /// only the name-to-index hash probe. Every other step, and the error a
    /// node-tier read can raise, is what [`Self::system_value`] does.
    pub fn system_value_at(&self, index: usize) -> Result<Cow<'_, str>, VarError> {
        let def = &crate::sysvar::SYS_VARS[index];
        // An INSTANCE-scoped variable has no session copy either, and its
        // node-wide value is the only one there is: without this arm a
        // `SET GLOBAL tidb_general_log = 1` would store a value that
        // `SELECT @@tidb_general_log` never consults. A NONE-scope variable
        // (`port`, `socket`) reads the same node tier, which is where the
        // startup `set_global_vars` push (Go `variable.SetSysVar`) lives.
        if !def.has_session_scope() {
            return self
                .globals()
                .get_by_registry_index(index)
                .map(|value| Cow::Owned(global_get_hook(def.name, value)));
        }
        if let Some(value) = self.session_resolved.values.get(index) {
            if let Some(value) = value.as_ref() {
                return Ok(Cow::Borrowed(value.as_ref()));
            }
            if self.session_resolved.values.len() == crate::sysvar::SYS_VARS.len() {
                // A full-length image is current by construction -- every
                // `systems` mutation republishes it before returning.
                if def.name == "timestamp" {
                    return Ok(Cow::Owned(timestamp_hook_value(
                        self.systems.get(def.name).map(String::as_str),
                    )));
                }
                return self
                    .unseeded_global_value(index)
                    .unwrap_or_else(|| Ok(crate::sysvar::effective_default_value(def)));
            }
        }
        // The registry name is the lowercase key `systems` is written under,
        // so the caller's spelling needs no second lowercasing here.
        if def.name == "timestamp" {
            return Ok(Cow::Owned(timestamp_hook_value(
                self.systems.get(def.name).map(String::as_str),
            )));
        }
        if let Some(value) = self.systems.get(def.name) {
            return Ok(Cow::Borrowed(value.as_str()));
        }
        self.unseeded_global_value(index)
            .unwrap_or_else(|| Ok(crate::sysvar::effective_default_value(def)))
    }

    /// Go `GetSessionOrGlobalSystemVar` for a variable `SkipInit` left out of
    /// the session: its first session read takes the GLOBAL value and keeps
    /// it (Go stores it into `s.systems`), so a later `SET GLOBAL` does not
    /// change what this session reads.
    fn unseeded_global_value(&self, index: usize) -> Option<Result<Cow<'_, str>, VarError>> {
        let def = &crate::sysvar::SYS_VARS[index];
        if !(def.skip_init() && def.has_global_scope()) {
            return None;
        }
        let mut first_reads = self
            .first_read_globals
            .0
            .lock()
            .expect("first-read globals lock poisoned");
        if let Some(value) = first_reads.get(&index) {
            return Some(Ok(Cow::Owned(value.clone())));
        }
        Some(self.globals().get_by_registry_index(index).map(|value| {
            let value = global_get_hook(def.name, value);
            first_reads.insert(index, value.clone());
            Cow::Owned(value)
        }))
    }

    /// Go `vardef.EnableNoopVariables`: the GLOBAL
    /// `tidb_enable_noop_variables` switch.
    #[must_use]
    pub fn noop_variables_enabled(&self) -> bool {
        self.get_global(tidb_vardef::tidb_vars::TIDB_ENABLE_NOOP_VARIABLES)
            .map_or(
                tidb_vardef::defaults::DEF_TIDB_ENABLE_NOOP_VARIABLES,
                |value| value.eq_ignore_ascii_case("ON"),
            )
    }

    pub fn get_system(&self, name: &str) -> Result<String, VarError> {
        self.system_value(name).map(Cow::into_owned)
    }

    /// An explicit `SET timestamp = x` override, which pins every statement's
    /// clock. Reading `@@timestamp` itself (Go's `GetSession` hook) answers
    /// the CURRENT statement's time when no override is set, so a value read
    /// through [`Self::get_system`] must never be kept past one statement.
    pub fn timestamp_override(&self) -> Option<f64> {
        self.systems
            .get("timestamp")
            .filter(|value| !value.is_empty() && value.as_str() != "0")
            .and_then(|value| value.parse::<f64>().ok())
    }

    /// Returns the value that Go's `GetSessionStatesSystemVar` would encode
    /// while migrating this session, together with whether the entry should
    /// be kept in the session-state image.
    ///
    /// Session state is deliberately based on cached session overrides, not
    /// on `get_system`'s registry-default fallback: an untouched variable is
    /// omitted so the destination session can use its own default.  Go's
    /// timestamp state hook also omits an override equal to the default,
    /// `max_allowed_packet` is always emitted through its typed getter, and
    /// the last-insert-id/identity hooks are explicitly never migrated.
    pub fn get_session_states_system_var(&self, name: &str) -> Result<(String, bool), VarError> {
        let Some(index) = crate::sysvar::sys_var_index_lookup(name) else {
            return Err(tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_UNKNOWN_SYSTEM_VAR
                    .generate_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
            ));
        };
        let definition = &crate::sysvar::SYS_VARS[index];
        let key = crate::sysvar::lowered_if_needed(name);

        // Go's GetStateValue hooks intentionally omit these values even when
        // their ordinary session getter exposes a value.
        if matches!(key.as_ref(), "last_insert_id" | "identity") {
            return Ok((String::new(), false));
        }

        // Go's max_allowed_packet definition has a GetSession hook, so its
        // current typed value is serialized even before an explicit session
        // override exists.
        if key == "max_allowed_packet" {
            return Ok((self.max_allowed_packet.to_string(), true));
        }

        let first_read = self
            .first_read_globals
            .0
            .lock()
            .expect("first-read globals lock poisoned")
            .get(&index)
            .cloned();
        let Some(value) = self.systems.get(key.as_ref()).or(first_read.as_ref()) else {
            return Ok((String::new(), false));
        };

        // Go's timestamp hook only migrates a non-default explicit override;
        // an assignment of the default is equivalent to no session state.
        if key == "timestamp" && value == definition.value {
            return Ok((String::new(), false));
        }

        Ok((value.clone(), true))
    }

    /// A snapshot of the session overrides `name` (and its alias) currently
    /// hold, for a statement-scoped write to put back afterwards.
    ///
    /// `None` for a name with no override records the ABSENCE, so restoring it
    /// leaves the variable tracking the registry default rather than pinning
    /// it to the default's text.
    #[must_use]
    pub fn snapshot_system(&self, name: &str) -> Vec<(String, Option<String>)> {
        let key = name.to_ascii_lowercase();
        let mut keys = vec![key.clone()];
        if let Some(other) = alias_of(&key) {
            keys.push(other.to_owned());
        }
        keys.into_iter()
            .map(|key| {
                let previous = self.systems.get(&key).cloned();
                (key, previous)
            })
            .collect()
    }

    /// Puts back what [`Self::snapshot_system`] recorded.
    pub fn restore_system(&mut self, snapshot: Vec<(String, Option<String>)>) {
        if snapshot.is_empty() {
            return;
        }
        let mut restores_sql_mode = false;
        let mut restores_max_allowed_packet = false;
        let mut restores_max_keys_read = false;
        let mut restores_max_execution_time = false;
        let mut restores_time_zone = false;
        let mut restores_select_limit = false;
        let mut restores_selectivity_factor = false;
        let mut restores_multi_statement_mode = false;
        let mut restores_prepared_plan_cache = false;
        let mut restores_shared_lock_promotion = false;
        let mut restores_window_function = false;
        let mut restores_ti_flash_max_bytes_before_ext_join = false;
        let mut restores_ti_flash_max_bytes_before_ext_agg = false;
        let mut restores_ti_flash_max_bytes_before_ext_sort = false;
        let mut restores_ti_flash_mem_quota_query_per_node = false;
        let mut restores_ti_flash_query_spill_ratio = false;
        let mut restores_pessimistic_transaction_fair_locking = false;
        let mut restores_bulk_dml_enabled = false;
        let mut restores_replica_read = false;
        let mut restores_analyze_store_batch_size = false;
        for (key, previous) in snapshot {
            restores_sql_mode |= key == "sql_mode";
            restores_max_allowed_packet |= key == "max_allowed_packet";
            restores_max_keys_read |= key == "tidb_max_keys_read";
            restores_max_execution_time |= key == "max_execution_time";
            restores_time_zone |= key == "time_zone";
            restores_select_limit |= key == "sql_select_limit";
            restores_selectivity_factor |=
                key == tidb_vardef::tidb_vars::TIDB_OPT_SELECTIVITY_FACTOR;
            restores_multi_statement_mode |= key == "tidb_multi_statement_mode";
            restores_prepared_plan_cache |=
                key == tidb_vardef::tidb_vars::TIDB_ENABLE_PREP_PLAN_CACHE;
            restores_shared_lock_promotion |=
                key == tidb_vardef::tidb_vars::TIDB_ENABLE_SHARED_LOCK_PROMOTION;
            restores_window_function |= key == tidb_vardef::tidb_vars::TIDB_ENABLE_WINDOW_FUNCTION;
            restores_ti_flash_max_bytes_before_ext_join |=
                key == tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN;
            restores_ti_flash_max_bytes_before_ext_agg |=
                key == tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY;
            restores_ti_flash_max_bytes_before_ext_sort |=
                key == tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT;
            restores_ti_flash_mem_quota_query_per_node |=
                key == tidb_vardef::tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE;
            restores_ti_flash_query_spill_ratio |=
                key == tidb_vardef::tidb_vars::TIFLASH_QUERY_SPILL_RATIO;
            restores_pessimistic_transaction_fair_locking |=
                key == tidb_vardef::tidb_vars::TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING;
            restores_bulk_dml_enabled |= key == tidb_vardef::tidb_vars::TIDB_DML_TYPE;
            restores_replica_read |= key == tidb_vardef::tidb_vars::TIDB_REPLICA_READ;
            restores_analyze_store_batch_size |=
                key == tidb_vardef::tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE;
            match previous {
                Some(value) => {
                    self.session_resolved
                        .note(key.as_str(), Some(value.as_str()));
                    self.systems.insert(key, value);
                }
                None => {
                    self.session_resolved.note(key.as_str(), None);
                    self.systems.remove(&key);
                }
            };
        }
        self.generation += 1;
        self.autocommit = Self::autocommit_from_systems(&self.systems);
        if restores_sql_mode {
            self.sql_mode = Self::sql_mode_from_systems(&self.systems)
                .expect("a saved SQL mode was validated before it was stored");
        }
        if restores_max_allowed_packet {
            self.max_allowed_packet = Self::max_allowed_packet_from_systems(&self.systems)
                .expect("a saved max_allowed_packet was validated before it was stored");
        }
        if restores_max_keys_read {
            self.max_keys_read = Self::max_keys_read_from_systems(&self.systems);
        }
        if restores_max_execution_time {
            self.max_execution_time = Self::max_execution_time_from_systems(&self.systems);
        }
        if restores_time_zone {
            self.time_zone = Self::time_zone_from_systems(&self.systems);
        }
        if restores_select_limit {
            self.select_limit = Self::select_limit_from_systems(&self.systems);
        }
        if restores_selectivity_factor {
            self.selectivity_factor = Self::selectivity_factor_from_systems(&self.systems);
        }
        if restores_multi_statement_mode {
            self.multi_statement_mode = Self::multi_statement_mode_from_systems(&self.systems);
        }
        if restores_prepared_plan_cache {
            self.enable_prepared_plan_cache = Self::prepared_plan_cache_from_systems(&self.systems);
        }
        if restores_shared_lock_promotion {
            self.shared_lock_promotion = Self::shared_lock_promotion_from_systems(&self.systems);
        }
        if restores_window_function {
            self.enable_window_function = Self::enable_window_function_from_systems(&self.systems);
        }
        if restores_ti_flash_max_bytes_before_ext_join {
            self.ti_flash_max_bytes_before_ext_join =
                Self::ti_flash_max_bytes_before_ext_join_from_systems(&self.systems);
        }
        if restores_ti_flash_max_bytes_before_ext_agg {
            self.ti_flash_max_bytes_before_ext_agg =
                Self::ti_flash_max_bytes_before_ext_agg_from_systems(&self.systems);
        }
        if restores_ti_flash_max_bytes_before_ext_sort {
            self.ti_flash_max_bytes_before_ext_sort =
                Self::ti_flash_max_bytes_before_ext_sort_from_systems(&self.systems);
        }
        if restores_ti_flash_mem_quota_query_per_node {
            self.ti_flash_mem_quota_query_per_node =
                Self::ti_flash_mem_quota_query_per_node_from_systems(&self.systems);
        }
        if restores_ti_flash_query_spill_ratio {
            self.ti_flash_query_spill_ratio =
                Self::ti_flash_query_spill_ratio_from_systems(&self.systems);
        }
        if restores_pessimistic_transaction_fair_locking {
            self.pessimistic_transaction_fair_locking =
                Self::pessimistic_transaction_fair_locking_from_systems(&self.systems);
        }
        if restores_bulk_dml_enabled {
            self.bulk_dml_enabled = Self::bulk_dml_enabled_from_systems(&self.systems);
        }
        if restores_replica_read {
            self.replica_read = Self::replica_read_from_systems(&self.systems);
        }
        if restores_analyze_store_batch_size {
            self.analyze_store_batch_size =
                Self::analyze_store_batch_size_from_systems(&self.systems);
        }
        self.refresh_optimizer_fix_control();
    }

    /// Reads `@@global.name`: always the shared table's live value, never
    /// this session's own copy. Go's `ErrIncorrectGlobalLocalVar` (1238) when
    /// the variable has no GLOBAL scope at all to read.
    pub fn get_global(&self, name: &str) -> Result<String, VarError> {
        self.globals().get_for_global_scope(name)
    }

    /// Go `SessionVars.GlobalVarsAccessor`, which the session installs when
    /// it is created; reading the GLOBAL tier before then is Go's nil
    /// dereference.
    fn globals(&self) -> &dyn crate::GlobalVarAccessor {
        self.globals
            .as_deref()
            .expect("SessionVars.GlobalVarsAccessor is installed by the session")
    }

    /// Shares the currently selected global accessor, not its values. Account
    /// writes may temporarily swap this owner for a scratch validation table.
    pub fn global_sysvars(&self) -> Arc<dyn crate::GlobalVarAccessor> {
        Arc::clone(
            self.globals
                .as_ref()
                .expect("SessionVars.GlobalVarsAccessor is installed by the session"),
        )
    }

    pub fn global_sysvar_accessor(&self) -> Arc<dyn crate::GlobalSysvarAccessor> {
        self.global_sysvars()
    }

    /// Reads Go's two process-wide typed statement-memory settings without
    /// converting their GLOBAL sysvar text on every statement.
    pub fn statement_memory_policy(&self) -> (tidb_vardef::OomAction, bool) {
        self.globals().statement_memory_policy()
    }

    /// Go's process-wide `vardef.CheckMb4ValueInUTF8` switch, typed once per
    /// GLOBAL publication.
    pub fn check_mb4_value_in_utf8(&self) -> bool {
        self.globals().check_mb4_value_in_utf8()
    }

    /// Go's process-wide `vardef.EnableCheckConstraint` switch, typed once per
    /// GLOBAL publication.
    pub fn enable_check_constraint(&self) -> bool {
        self.globals().enable_check_constraint()
    }

    /// Go's process-wide resource-control enable switch, published by the
    /// GLOBAL sysvar hook and consumed by statement hint admission.
    pub fn resource_control_enabled(&self) -> bool {
        self.globals().resource_control_enabled()
    }

    /// Go's process-wide resource-control strict-mode switch.
    pub fn resource_control_strict_mode(&self) -> bool {
        self.globals().resource_control_strict_mode()
    }

    /// Sets a session system variable, validating the value as Go's
    /// `ValidateFromType` does: the stored value is the normalized one, and
    /// an out-of-range value is clamped exactly as Go clamps it.
    ///
    /// A read-only variable is Go's `ErrIncorrectGlobalLocalVar`. A
    /// GLOBAL-only variable is Go's `ErrGlobalVariable` (1229): `SET
    /// SESSION`/plain `SET` cannot touch it, only `SET GLOBAL` can.
    ///
    /// Answers whether the value was clamped: Go's clamping checks
    /// (`checkUInt64SystemVar` and friends, plus the per-variable `Validation`
    /// closures modelled in [`sysvar`]) do not fail the statement, they append
    /// `ErrTruncatedWrongValue` (1292) to it, so the caller with the statement
    /// context in hand is the one that can report it.
    pub fn set_system(&mut self, name: &str, value: String) -> Result<bool, VarError> {
        let def = get_sys_var(name).ok_or_else(|| {
            tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_UNKNOWN_SYSTEM_VAR
                    .generate_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
            )
        })?;
        if def.is_read_only() {
            return Err(tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_INCORRECT_SCOPE.generate_with_stack_by_args(&[
                    (name.to_ascii_lowercase()).into(),
                    "read only".into(),
                ]),
            ));
        }
        // Go's `InternalSessionVariable` marker hides this name from explicit
        // `SET SESSION` while retaining its unqualified/internal read path.
        // Keep the SQL setter from bypassing that visibility rule.
        if def.is_internal_session_variable() {
            return Err(tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_UNKNOWN_SYSTEM_VAR
                    .generate_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
            ));
        }
        if !def.has_session_scope() {
            return Err(tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_GLOBAL_VARIABLE
                    .generate_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
            ));
        }
        // Go validates siblings through typed SessionVars fields. The textual
        // snapshot value can differ after SetExecutor rolls back a failed SET.
        let lookup = |sibling: &str| {
            if sibling.eq_ignore_ascii_case("tidb_snapshot") {
                Some(self.snapshot_ts.to_string())
            } else {
                self.get_system(sibling).ok()
            }
        };
        let validated = def
            .validate_in_scope_with_lookup(&value, SCOPE_SESSION, Some(&lookup))
            .map_err(|error| validation_var_error(name, &value, error))?;
        let key = name.to_ascii_lowercase();
        // Go's `tidb_enforce_mpp` Validation refuses ON while the session's
        // `allowMPPExecution` gate is OFF. The value is type-normalized before
        // this closure runs, so numeric/boolean spellings take the same path
        // as an explicit ON. Keep the refusal in the session writer rather
        // than the registry (it depends on another session variable).
        if key == "tidb_enforce_mpp"
            && validated.value == "ON"
            && self
                .get_system("tidb_allow_mpp")
                .map_or(true, |allow| allow != "ON")
        {
            return Err(tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_WRONG_VALUE_FOR_VAR
                    .generate_with_stack_by_args(&[
                        ("tidb_enforce_mpp".to_owned()).into(),
                        ("1' but tidb_allow_mpp is 0, please activate tidb_allow_mpp at first."
                            .to_owned())
                        .into(),
                    ]),
            ));
        }
        // The session Validation uses the same GLOBAL historical-stats
        // prerequisite as the shared writer above.
        if key == "tidb_enable_plan_replayer_continuous_capture"
            && validated.value == "ON"
            && self
                .get_system("tidb_enable_historical_stats")
                .map_or(true, |value| value != "ON")
        {
            return Err(tidb_terror::MysqlError::unknown("tidb_enable_historical_stats should be enabled before enabling tidb_enable_plan_replayer_continuous_capture"
                    .to_owned()));
        }
        self.set_session_from_hook(key, validated.value)?;
        Ok(validated.truncated)
    }

    /// Go `SessionVars.SetSystemVarWithoutValidation`: stores `value` for a
    /// registered session variable through its session hook only. No
    /// `Validation` closure, scope check or read-only check runs, so a
    /// caller that already holds a canonical value (Go `session.SetCollation`
    /// seeding the handshake collation) writes it exactly as Go does.
    pub fn set_system_var_without_validation(
        &mut self,
        name: &str,
        value: String,
    ) -> Result<(), VarError> {
        if get_sys_var(name).is_none() {
            return Err(tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_UNKNOWN_SYSTEM_VAR
                    .generate_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
            ));
        }
        self.set_session_from_hook(name.to_ascii_lowercase(), value)
    }

    /// Go `SysVar.SetSessionFromHook`: the variable's `SetSession` hook, the
    /// stored value, and the same value for its alias. Shared by the
    /// validating [`Self::set_system`] and
    /// [`Self::set_system_var_without_validation`], so both writers keep the
    /// typed fields and derived variables in lockstep.
    fn set_session_from_hook(&mut self, key: String, value: String) -> Result<(), VarError> {
        let parsed_fix_control = if key == tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL {
            Some(
                OptimizerFixControl::parse(&value)
                    .map_err(|error| tidb_terror::MysqlError::unknown(error.to_string()))?
                    .0,
            )
        } else {
            None
        };
        let parsed_sql_mode = if key == "sql_mode" {
            Some(
                tidb_mysql::get_sql_mode(&value)
                    .map_err(|error| tidb_terror::MysqlError::unknown(error.to_string()))?,
            )
        } else {
            None
        };
        let parsed_snapshot_ts = if key == "tidb_snapshot" || key == "tx_read_ts" {
            Some(parse_read_timestamp(
                &value,
                &self.time_zone,
                key == "tidb_snapshot",
            )?)
        } else {
            None
        };
        if key == tidb_vardef::tidb_vars::TIDB_DML_TYPE
            && !value.eq_ignore_ascii_case("standard")
            && !value.eq_ignore_ascii_case("bulk")
        {
            return Err(tidb_terror::MysqlError::unknown(format!(
                "unsupport DML type: {}",
                value
            )));
        }
        // Go's charset/collation hooks keep each pair in lockstep. A charset
        // write selects that charset's default collation (except
        // `character_set_client`/`character_set_results`), while a collation
        // write mirrors its owning charset into the corresponding charset
        // variable. Keep the derived names in the session image so all later
        // reads observe the same side effects as Go's `SessionVars.systems`.
        let charset_default_collation = match key.as_str() {
            "character_set_connection" => tidb_charset::get_charset_info(&value)
                .ok()
                .map(|charset| ("collation_connection", charset.default_collation)),
            "character_set_database" => tidb_charset::get_charset_info(&value)
                .ok()
                .map(|charset| ("collation_database", charset.default_collation)),
            "character_set_server" => tidb_charset::get_charset_info(&value)
                .ok()
                .map(|charset| ("collation_server", charset.default_collation)),
            _ => None,
        };
        let collation_charset = match key.as_str() {
            "collation_connection" => tidb_charset::get_collation_by_name(&value)
                .ok()
                .map(|collation| ("character_set_connection", collation.charset_name)),
            "collation_database" => tidb_charset::get_collation_by_name(&value)
                .ok()
                .map(|collation| ("character_set_database", collation.charset_name)),
            "collation_server" => tidb_charset::get_collation_by_name(&value)
                .ok()
                .map(|collation| ("character_set_server", collation.charset_name)),
            _ => None,
        };
        // Go `SetSessionFromHook`: the alias takes the SAME stored value, with
        // its own validation skipped -- `tx_isolation` and
        // `transaction_isolation` are one value under two spellings.
        if let Some(other) = alias_of(&key) {
            self.systems.insert(other.to_owned(), value.clone());
        }
        self.systems.insert(key.clone(), value.clone());
        if key == "tx_isolation_one_shot" {
            self.txn_isolation_level_one_shot = (TxnIsolationLevelOneShotState::Set, value.clone());
        }
        if let Some(ts) = parsed_snapshot_ts {
            if key == "tidb_snapshot" {
                self.snapshot_ts = ts;
                if !value.is_empty() {
                    self.set_txn_read_ts(0);
                }
            } else {
                self.set_txn_read_ts(ts);
                if !value.is_empty() {
                    self.snapshot_ts = 0;
                }
            }
        }
        self.note_system_change(&key);
        if let Some(other) = alias_of(&key) {
            self.note_system_change(other);
        }
        if let Some((name, value)) = charset_default_collation {
            self.systems.insert(name.to_owned(), value);
            self.note_system_change(name);
        }
        if let Some((name, value)) = collation_charset {
            self.systems.insert(name.to_owned(), value);
            self.note_system_change(name);
        }
        if key == "autocommit" {
            self.autocommit = value == "ON";
        }
        if let Some(parsed) = parsed_sql_mode {
            self.sql_mode = parsed;
        }
        if key == "max_allowed_packet" {
            self.max_allowed_packet = value
                .parse::<u64>()
                .expect("max_allowed_packet validation stores unsigned decimal bytes");
        }
        if key == "tidb_max_keys_read" {
            self.max_keys_read = value
                .parse::<u64>()
                .expect("tidb_max_keys_read validation stores unsigned decimal keys");
        }
        if key == "max_execution_time" {
            self.max_execution_time = value
                .parse::<u64>()
                .expect("max_execution_time validation stores unsigned decimal milliseconds");
        }
        if key == "time_zone" {
            self.time_zone = resolve_session_time_zone_value(&value);
        }
        if key == "sql_select_limit" {
            self.select_limit = value
                .parse::<u64>()
                .expect("sql_select_limit validation stores unsigned decimal rows");
        }
        if key == tidb_vardef::tidb_vars::TIDB_OPT_SELECTIVITY_FACTOR {
            self.selectivity_factor = value
                .parse::<f64>()
                .expect("selectivity factor validation stores a decimal fraction");
        }
        if key == "tidb_multi_statement_mode" {
            self.multi_statement_mode = Self::multi_statement_mode_from_systems(&self.systems);
        }
        if key == tidb_vardef::tidb_vars::TIDB_ENABLE_PREP_PLAN_CACHE {
            self.enable_prepared_plan_cache = value == "ON";
        }
        if key == tidb_vardef::tidb_vars::TIDB_ENABLE_SHARED_LOCK_PROMOTION {
            self.shared_lock_promotion = value == "ON";
        }
        if key == tidb_vardef::tidb_vars::TIDB_ENABLE_WINDOW_FUNCTION {
            self.enable_window_function = value == "ON";
        }
        if key == tidb_vardef::tidb_vars::TIDB_QUERY_COP_STORE_LIMIT {
            self.query_cop_store_limit = value
                .parse::<i64>()
                .expect("query cop store limit validation stores an integer");
        }
        if key == tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN {
            self.ti_flash_max_bytes_before_ext_join = value
                .parse::<i64>()
                .expect("TiFlash external join threshold validation stores signed bytes");
        }
        if key == tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY {
            self.ti_flash_max_bytes_before_ext_agg = value
                .parse::<i64>()
                .expect("TiFlash external group-by threshold validation stores signed bytes");
        }
        if key == tidb_vardef::tidb_vars::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT {
            self.ti_flash_max_bytes_before_ext_sort = value
                .parse::<i64>()
                .expect("TiFlash external sort threshold validation stores signed bytes");
        }
        if key == tidb_vardef::tidb_vars::TIFLASH_MEM_QUOTA_QUERY_PER_NODE {
            self.ti_flash_mem_quota_query_per_node = value
                .parse::<i64>()
                .expect("TiFlash per-node quota validation stores signed bytes");
        }
        if key == tidb_vardef::tidb_vars::TIFLASH_QUERY_SPILL_RATIO {
            self.ti_flash_query_spill_ratio = value
                .parse::<f64>()
                .expect("TiFlash spill ratio validation stores a decimal fraction");
        }
        if key == tidb_vardef::tidb_vars::TIDB_PESSIMISTIC_TRANSACTION_FAIR_LOCKING {
            self.pessimistic_transaction_fair_locking = value == "ON";
        }
        if key == tidb_vardef::tidb_vars::TIDB_DML_TYPE {
            self.bulk_dml_enabled = value.eq_ignore_ascii_case("bulk");
        }
        if key == tidb_vardef::tidb_vars::TIDB_REPLICA_READ {
            self.replica_read = Self::replica_read_from_systems(&self.systems);
        }
        if key == tidb_vardef::tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE {
            self.analyze_store_batch_size = value
                .parse::<i64>()
                .expect("analyze store batch size validation stores an integer");
        }
        self.generation += 1;
        if let Some(parsed) = parsed_fix_control {
            self.optimizer_fix_control = parsed;
        }
        Ok(())
    }

    /// `SET GLOBAL name = value`: writes only the shared table, never this
    /// session's own `@@name`. Go's `ErrLocalVariable` (1228) when the
    /// variable is SESSION-only, so there is no global copy to set.
    pub fn set_global(&mut self, name: &str, value: String) -> Result<bool, VarError> {
        self.globals()
            .set_with_time_zone(name, value, &self.time_zone)
    }

    /// `SET INSTANCE name = value`: writes the node-local tier, never this
    /// session's own copy (an instance-scoped variable has none).
    pub fn set_instance(&mut self, name: &str, value: String) -> Result<bool, VarError> {
        self.globals().set_instance(name, value)
    }

    /// Points this session at a different shared GLOBAL table for one
    /// statement, answering the one it was using -- the GLOBAL-tier twin
    /// of [`crate::privilege::PrivilegeRegistry`]'s `swap_privileges`. A
    /// convergence node validates one `SET GLOBAL` against a scratch table
    /// read from the cluster before persisting it, and must be able to put
    /// the live table back unconditionally if the statement fails.
    pub fn swap_globals(
        &mut self,
        globals: Option<Arc<dyn crate::GlobalVarAccessor>>,
    ) -> Option<Arc<dyn crate::GlobalVarAccessor>> {
        if let (Some(current), Some(incoming)) = (&self.globals, &globals) {
            if let Some(provider) = current.external_timestamp_provider() {
                incoming.set_external_timestamp_provider(provider);
            }
        }
        std::mem::replace(&mut self.globals, globals)
    }

    /// `SET GLOBAL name = DEFAULT`.
    pub fn reset_global(&mut self, name: &str) -> Result<(), VarError> {
        if name.eq_ignore_ascii_case("tidb_external_ts") {
            return self.set_global(name, "0".into()).map(|_| ());
        }
        self.globals().reset(name)
    }

    /// `SET INSTANCE name = DEFAULT`.
    pub fn reset_instance(&mut self, name: &str) -> Result<(), VarError> {
        self.globals().reset_instance(name)
    }

    /// Go's `SET x = DEFAULT`.
    ///
    /// DEFAULT is not "forget the override". Go `SetExecutor.getVarValue`
    /// RESOLVES it to a string --
    /// `variable.GlobalSystemVariableInitialValue(sysVar.Name, sysVar.Value)`
    /// -- and then writes that string through the ordinary session-set path.
    /// The difference is the whole point of that call: four variables ship a
    /// registry value that a real install never runs with
    /// (`tidb_row_format_version` is `1` in the struct and `2` everywhere
    /// else), so clearing the override answers a value no TiDB reports.
    /// Captured on a v8.5.6 playground: `SET tidb_row_format_version =
    /// DEFAULT; SELECT @@tidb_row_format_version` reads `2` on a Go node.
    /// Go `GetGlobalSysVar` as a SESSION `SET x = DEFAULT` reads it: the
    /// stored GLOBAL value, or -- when nothing was ever stored -- the value
    /// bootstrap writes into `mysql.global_variables`
    /// (`globalSystemVariableInitialValue`). A variable without a global scope
    /// answers its compiled default.
    pub fn global_value_for_session_default(&self, name: &str) -> Result<String, VarError> {
        let def = get_sys_var(name).ok_or_else(|| {
            tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_UNKNOWN_SYSTEM_VAR
                    .generate_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
            )
        })?;
        let default = crate::sysvar::effective_default(def);
        if !def.has_global_scope() {
            return Ok(default);
        }
        if let Some(stored) = self.globals().overrides().get(def.name) {
            return Ok(global_get_hook(def.name, stored.clone()));
        }
        Ok(global_system_variable_initial_value(def))
    }

    pub fn reset_system(&mut self, name: &str) -> Result<(), VarError> {
        let def = get_sys_var(name).ok_or_else(|| {
            tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_UNKNOWN_SYSTEM_VAR
                    .generate_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
            )
        })?;
        let value = global_system_variable_initial_value(def);
        self.set_system(name, value).map(|_truncated| ())
    }

    /// Go `SetExecutor.setCharset` (`pkg/executor/set.go`): `SET NAMES`
    /// when `is_set_names`, otherwise `SET CHARACTER SET`. `DEFAULT` arrives
    /// as Go's `mysql.DefaultCharset` with no collation.
    ///
    /// Without a collation, `utf8mb4` takes the session's
    /// `default_collation_for_utf8mb4` and any other charset its default
    /// collation. A named collation resolves through Go's
    /// `collate.GetCollationByName` (unknown, or unsupported by the new
    /// collation framework, is 1273) and must belong to `charset` (1253).
    /// `SET NAMES` then writes the three `SetNamesVariables` and
    /// `collation_connection`; `SET CHARACTER SET` writes the client and
    /// results charsets and takes the connection pair from the GLOBAL
    /// database charset and collation.
    pub fn set_charset(
        &mut self,
        charset: &str,
        collation: Option<&str>,
        is_set_names: bool,
    ) -> Result<(), VarError> {
        // Go's parser validates the charset name (`charset.GetCharsetInfo`)
        // and answers 1115 `Unknown character set: '...'`; this parser
        // defers that check here, which is what makes `SET NAMES anyword` a
        // runtime error.
        let Ok(charset_info) = tidb_charset::get_charset_info(charset) else {
            return Err(tidb_terror::MysqlError::from_sql_error(
                tidb_mysql::errors::SqlError::new(
                    tidb_mysql::errors::errcode::ErrUnknownCharacterSet,
                    &[tidb_mysql::errors::FormatArg::from(charset)],
                ),
            ));
        };
        let collation = match collation {
            None if charset == "utf8mb4" => self.get_system("default_collation_for_utf8mb4")?,
            None => charset_info.default_collation,
            Some(name) => {
                let collation = crate::sysvar::supported_collation(name, name)
                    .map_err(VarError::from_sql_error)?;
                if collation.charset_name != charset {
                    return Err(tidb_terror::MysqlError::from_sql_error(
                        tidb_mysql::errors::SqlError::new(
                            tidb_mysql::errors::errcode::ErrCollationCharsetMismatch,
                            &[
                                tidb_mysql::errors::FormatArg::from(collation.name.as_str()),
                                tidb_mysql::errors::FormatArg::from(charset),
                            ],
                        ),
                    ));
                }
                name.to_owned()
            }
        };
        if is_set_names {
            for name in [
                "character_set_client",
                "character_set_connection",
                "character_set_results",
            ] {
                self.set_system(name, charset.to_owned())?;
            }
            self.set_system("collation_connection", collation)?;
            return Ok(());
        }
        // Set charset statement, see also
        // https://dev.mysql.com/doc/refman/8.0/en/set-character-set.html.
        for name in ["character_set_client", "character_set_results"] {
            self.set_system(name, charset.to_owned())?;
        }
        let charset_database = self.get_global("character_set_database")?;
        let collation_database = self.get_global("collation_database")?;
        self.set_system("character_set_connection", charset_database)?;
        self.set_system("collation_connection", collation_database)?;
        Ok(())
    }

    /// The parsed `tidb_opt_fix_control` map used by planner consumers.
    #[must_use]
    pub fn optimizer_fix_control(&self) -> &OptimizerFixControl {
        &self.optimizer_fix_control
    }

    fn refresh_optimizer_fix_control(&mut self) {
        let raw = self
            .get_system(tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL)
            .unwrap_or_default();
        self.optimizer_fix_control = OptimizerFixControl::parse(&raw)
            .map(|(parsed, _warnings)| parsed)
            .expect(
                "tidb_opt_fix_control session state comes only from validated writes or trusted global rows",
            );
    }
}

/// Go `strconv.FormatFloat(value, 'f', -1, 64)`.
fn format_double(value: f64) -> String {
    tidb_goruntime::go_strconv::format_float(value, b'f', -1, 64)
}


/// Go parseTSFromNumberOrTime; tx_read_ts deliberately permits datetime syntax only.
pub fn parse_read_timestamp(
    value: &str,
    zone: &tidb_datatype::SessionTimeZone,
    numeric: bool,
) -> Result<u64, VarError> {
    if value.is_empty() {
        return Ok(0);
    }
    if numeric {
        if let Ok(ts) = value.parse::<u64>() {
            return Ok(ts);
        }
    }
    let invalid = || {
        tidb_terror::MysqlError::from_sql_error(tidb_mysql::errors::SqlError::new_f(
            tidb_mysql::errors::errcode::ErrTruncatedWrongValue,
            "Incorrect datetime value: '%s'",
            &[],
            &[tidb_mysql::errors::FormatArg::from(value)],
        ))
    };
    let time = tidb_datatype::parse_time(
        value,
        tidb_datatype::TimeType::Timestamp,
        6,
        false,
        false,
        false,
        zone,
    )
    .map_err(|_| invalid())?;
    let instant = time
        .time
        .core_time()
        .to_datetime(zone)
        .map_err(|_| invalid())?;
    Ok((instant.timestamp_millis() as u64) << 18)
}


/// Go `variable.GlobalSystemVariableInitialValue(sv.Name, sv.Value)`: the
/// value bootstrap seeds into `mysql.global_variables` and `SET x = DEFAULT`
/// resolves to.
///
/// The environment is the stock one. `store_is_tikv` is a PROCESS fact in Go
/// (`config.GetGlobalConfig().Store`) that this tier is not told, so
/// `tidb_enable_async_commit`/`tidb_enable_1pc` resolve to their non-TiKV-store
/// defaults here; every other override is unconditional.
pub fn global_system_variable_initial_value(def: &SysVarDef) -> String {
    tidb_vardef::global_sysvar_initial::global_system_variable_initial_value(
        def.name,
        &crate::sysvar::effective_default(def),
        tidb_vardef::global_sysvar_initial::GlobalSysvarEnvironment {
            store_is_tikv: false,
            in_test: false,
            next_gen: false,
        },
    )
}

/// Go's `GetGlobal` hooks that report something other than the stored text:
/// an LDAP bind password reads back as `vardef.MaskPwd` once it is set.
pub fn global_get_hook(name: &str, value: String) -> String {
    match name {
        "authentication_ldap_sasl_bind_root_pwd" | "authentication_ldap_simple_bind_root_pwd"
            if !value.is_empty() =>
        {
            "******".to_owned()
        }
        _ => value,
    }
}

/// Go `SessionVars.GetRelatedTableForMDL().Store(id, ...)`: records one stored
/// table id on the statement's metadata-lock map.
pub type MdlRelatedTableRecorder = std::sync::Arc<dyn Fn(i64) + Send + Sync>;
