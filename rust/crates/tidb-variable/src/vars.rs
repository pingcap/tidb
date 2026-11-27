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
                .gen_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
        ),
        ValidationError::WrongValue => tidb_terror::MysqlError::from_terror(
            &crate::variable_errors::ERR_WRONG_VALUE_FOR_VAR.gen_with_stack_by_args(&[
                (name.to_ascii_lowercase()).into(),
                (value.to_owned()).into(),
            ]),
        ),
        ValidationError::WrongValueWith(given) => tidb_terror::MysqlError::from_terror(
            &crate::variable_errors::ERR_WRONG_VALUE_FOR_VAR
                .gen_with_stack_by_args(&[(name.to_ascii_lowercase()).into(), given.into()]),
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
    /// Go `domain.setLowResolutionTSOUpdateInterval`
    /// (`tidb_low_resolution_tso_update_interval`): how often the store's
    /// oracle refreshes its low-resolution timestamp.
    fn set_low_resolution_tso_update_interval(&self, _interval: std::time::Duration) {}
    /// Applies Go `opt.EnableRouterClient` (`tidb_enable_batch_query_region`)
    /// to the same process PD client.
    fn set_enable_router_client(&self, _enabled: bool) {}
}

/// Go `variable.SetGlobalResourceControl`, called when
/// `tidb_enable_resource_control` changes: the domain's
/// `setGlobalResourceControl` (`domain_sysvars.go`) over the driver's
/// `EnableGlobalResourceControlFunc` / `DisableGlobalResourceControlFunc`,
/// which `tikv_driver.go`'s `init` binds to client-go's
/// `tikv.EnableResourceControl` / `DisableResourceControl` for the whole
/// process.
pub fn set_global_resource_control(enable: bool) {
    if enable {
        tikv_client::enable_resource_control();
    } else {
        tikv_client::disable_resource_control();
    }
}

/// The storage authority behind Go's `variable.SetExternalTimestamp` /
/// `GetExternalTimestamp`, which the domain fills
/// ([`crate::domain_slots::set_external_timestamp_funcs`]).
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

/// The transaction timestamp visible to one SQL session: Go
/// `SessionVars.TxnCtx.StartTS`, which `tidb_current_ts` answers.
///
/// The handle is shared with statement contexts because an autocommit
/// transaction opens lazily on its first storage read, after expression
/// evaluation has already been wired. Publishing through this handle lets a
/// later expression in that same statement observe the timestamp without
/// allocating one for constant-only statements.
#[derive(Clone, Debug, Default)]
pub struct CurrentTso(Arc<std::sync::atomic::AtomicU64>);

impl CurrentTso {
    /// Publishes the timestamp of the transaction that just opened.
    pub fn publish(&self, tso: u64) {
        self.0.store(tso, std::sync::atomic::Ordering::Release);
    }

    /// Clears the completed transaction from the session.
    pub fn clear(&self) {
        self.publish(0);
    }

    /// Returns the signed value `TIDB_CURRENT_TSO()` exposes.
    #[must_use]
    pub fn value(&self) -> i64 {
        i64::try_from(self.0.load(std::sync::atomic::Ordering::Acquire)).unwrap_or(i64::MAX)
    }
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
    /// Go `SessionVars.EnableDDLAnalyzeExecOpt` is a internal flag to notify
    /// internal session whether we do ddl analyze: such an ANALYZE also
    /// covers the indexes still in write-reorganization.
    pub enable_ddl_analyze_exec_opt: bool,
    /// Go's typed `SessionVars.SQLMode`, maintained by the sql_mode sysvar's
    /// `SetSession` hook and read directly by parser and executor consumers.
    sql_mode: tidb_mysql::SqlMode,
    /// Go's typed `SessionVars.MaxAllowedPacket`, maintained by the sysvar's
    /// `SetSession` hook and read directly by wire and builtin consumers.
    max_allowed_packet: u64,
    /// Go's `ServerStatusInTrans` bit (`InTxn` / `SetInTxn`).
    in_txn: bool,
    /// Go `SessionVars.ExplicitRequestSourceType` (`tidb_request_source_type`).
    explicit_request_source_type: String,
    /// Go `SessionVars.ScatterRegion` (`tidb_scatter_region`).
    scatter_region: String,
    /// Go `SessionVars.EnablePlanReplayedContinuesCapture`.
    enable_plan_replayed_continues_capture: bool,
    /// Go `SessionVars.EnablePlanReplayerCapture`.
    enable_plan_replayer_capture: bool,
    /// Go `SessionVars.GroupConcatMaxLen`.
    group_concat_max_len: u64,
    /// Go `SessionVars.PartitionPruneMode`.
    partition_prune_mode: String,
    /// Go `SessionVars.EnableINLJoinInnerMultiPattern`.
    enable_inl_join_inner_multi_pattern: bool,
    /// Go `SessionVars.TiFlashReplicaRead`.
    tiflash_replica_read: tidb_tiflash::ReplicaRead,
    /// Go `SessionVars.SessionAlias`.
    session_alias: String,
    /// Go `SessionVars.SlowLogRules.RawRules` while its `SlowLogRules` is
    /// set: the session's unbound slow-log rules, encoded; empty for none.
    slow_log_rules: String,
    /// Go's typed `SessionVars.MaxKeysRead`, maintained by the
    /// `tidb_max_keys_read` `SetSession` hook. The Go accessor returns this
    /// value only while a SELECT statement is active; callers pass that
    /// statement-shape bit to [`Self::max_keys_read`].
    max_keys_read: u64,
    /// Go's typed `SessionVars.MaxExecutionTime`, maintained by the
    /// `max_execution_time` `SetSession` hook and read by statement contexts
    /// as a millisecond deadline (zero means unlimited).
    max_execution_time: u64,
    /// Go `SessionVars.DMLMaxExecutionTime`: the timeout for transactional
    /// DML statements and COMMIT, in milliseconds; zero disables it.
    dml_max_execution_time: u64,
    /// Go `SessionVars.InNonTransactionalDML`: a non-transactional DML and
    /// its internal statements are running.
    pub in_non_transactional_dml: bool,
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
    /// Go's typed `SessionVars.EnableMView`, maintained by the
    /// `tidb_mview_enable` sysvar's `SetSession` hook.
    enable_mview: bool,
    /// Go master's typed `SessionVars.EnableFullOuterJoin`, maintained by
    /// the `tidb_enable_full_outer_join` sysvar's `SetSession` hook.
    enable_full_outer_join: bool,
    /// Go's typed `SessionVars.EnableRedactLog`, written by both the
    /// `SetSession` and the `SetGlobal` hook of `tidb_redact_log`.
    enable_redact_log: String,
    /// Go's typed `SessionVars.TiFlashMaxBytesBeforeExternalJoin`, maintained
    /// by the corresponding TiFlash threshold `SetSession` hook.
    ti_flash_max_bytes_before_ext_join: i64,
    /// Go's typed `SessionVars.TiFlashMaxBytesBeforeExternalGroupBy`.
    ti_flash_max_bytes_before_ext_agg: i64,
    /// Go's typed `SessionVars.TiFlashMaxBytesBeforeExternalSort`.
    ti_flash_max_bytes_before_ext_sort: i64,
    /// Go's typed `SessionVars.TiFlashMemQuotaQueryPerNode`.
    ti_flash_mem_quota_query_per_node: i64,
    /// Go's typed `SessionVars.MemQuotaQuery`: the byte quota each
    /// statement's tracker takes when it starts (`tidb_mem_quota_query`'s
    /// `SetSession`; Go tests assign it directly).
    pub mem_quota_query: i64,
    /// Go's typed `SessionVars.CopTiFlashConcurrencyFactor`, which the cost
    /// model reads (`tidb_opt_tiflash_concurrency_factor`'s `SetSession`); a
    /// `skipInit` variable, so reading it never stores the global into the
    /// session as `GetSessionOrGlobalSystemVar` would.
    pub cop_tiflash_concurrency_factor: f64,
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
    /// Go `SessionVars.CommonGlobalLoaded`: whether the session has taken its
    /// copy of the global tier ([`Self::load_common_global_variables_if_needed`]).
    common_global_loaded: bool,
    /// The GLOBAL values this session read for variables `SkipInit` left
    /// out of its copy; see [`Self::unseeded_global_value`].
    first_read_globals: FirstReadGlobals,
    /// Go `len(SessionVars.PreparedStmts)`: the prepared statements this
    /// session holds in [`PREPARED_STMT_COUNT`].
    prepared_stmt_entries: i64,
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
    /// Go `TxnCtx.StartTS`: the open transaction's start timestamp, `0`
    /// when none is open.
    pub current_tso: CurrentTso,
    /// Go `SessionVars.Rng`: the session's `RAND()` generator, seeded from
    /// the wall clock (`mathutil.NewWithTime()`) and by `rand_seed1` /
    /// `rand_seed2`'s `SetSession`; statement contexts share it.
    pub rng: Arc<tidb_mathutil::MysqlRng>,
    /// Go `StmtCtx.StmtType == "Set"`: the running statement is a user's
    /// `SET`, which some variables' `Validation` refuses more of than a
    /// programmatic `SetSystemVar` (`max_allowed_packet`'s SESSION scope).
    pub stmt_type_is_set: bool,
    /// Go `SessionVars.SysErrorCount`.
    pub sys_error_count: usize,
    /// Go `SessionVars.User` in its two spellings: the matched grant
    /// identity `CURRENT_USER()` reports and the login identity `USER()`
    /// reports. Empty until a front end authenticates one.
    /// (Go `SessionVars.User (rendered)`.)
    pub current_user: Option<String>,
    /// Go `SessionVars.User.AuthPlugin`.
    pub auth_plugin: String,
    /// Go `SessionVars.ConnectionInfo`.
    pub connection_info: Option<std::sync::Arc<crate::connection_info::ConnectionInfo>>,
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
    /// Go `SessionVars.ClientCapability`: the capability word the front end
    /// negotiated (`cc.capability`).
    pub client_capability: u32,
    /// Go `SessionVars.EnableRowLevelChecksum`: `NewSessionVars` copies
    /// `vardef.EnableRowLevelChecksum` when the session is created.
    pub enable_row_level_checksum: bool,
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
    /// Go master `SessionVars.InMViewMaintenance`: the session runs a
    /// materialized view's maintenance (build, refresh, log purge), whose
    /// restricted SQL may write the view, its log and its shadow table.
    pub in_mview_maintenance: bool,
    /// Go `SessionVars.RequestSourceType`: the source the session's requests
    /// carry, the running statement's label (`session.setRequestSource`).
    pub request_source_type: String,
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
    /// Go `SessionVars.LastCommitTS`: retained across read-only transactions.
    pub last_commit_ts: u64,
    /// Go `SessionVars.LastQueryInfo`, exposed by the read-only
    /// `tidb_last_query_info` getter.
    pub last_query_info: tidb_sessionstates::QueryInfo,
    /// Go `SessionVars.LastDDLInfo`: the query and sequence number of the
    /// last DDL job this session waited for.
    pub last_ddl_info: tidb_sessionstates::LastDDLInfo,
    /// Go `SessionVars.LastPlanReplayerToken`, read by the read-only
    /// `tidb_last_plan_replayer_token`.
    pub last_plan_replayer_token: String,
    /// Go `SessionVars.InPlanReplayer`, set while the plan replayer runs its
    /// EXPLAINs.
    pub in_plan_replayer: bool,
    /// Go `SessionVars.PlanReplayerFinishedTaskKey`: the keys this
    /// session's continuous capture already sent.
    pub plan_replayer_finished_task_key: std::collections::HashSet<tidb_replayer::PlanReplayerTaskKey>,
    /// Go `SessionVars.DefaultCollationForUTF8MB4`, maintained by `default_collation_for_utf8mb4`'s `SetSession`.
    pub(crate) default_collation_for_utf8mb4: String,
    /// Go `SessionVars.AllowWriteRowID`, maintained by `tidb_opt_write_row_id`'s `SetSession`.
    pub(crate) allow_write_row_id: bool,
    /// Go `SessionVars.EnableStableResultMode`, maintained by `tidb_enable_ordered_result_mode`'s `SetSession`.
    pub(crate) enable_stable_result_mode: bool,
    /// Go `SessionVars.SysdateIsNow`, maintained by `tidb_sysdate_is_now`'s `SetSession`.
    pub(crate) sysdate_is_now: bool,
    /// Go `SessionVars.SkipASCIICheck`, maintained by `tidb_skip_ascii_check`'s `SetSession`.
    pub(crate) skip_ascii_check: bool,
    /// Go `SessionVars.SkipUTF8Check`, maintained by `tidb_skip_utf8_check`'s `SetSession`.
    pub(crate) skip_utf8_check: bool,
    /// Go `SessionVars.AllowAutoRandExplicitInsert`, maintained by `allow_auto_random_explicit_insert`'s `SetSession`.
    pub(crate) allow_auto_rand_explicit_insert: bool,
    /// Go `SessionVars.ShardAllocateStep`, maintained by `tidb_shard_allocate_step`'s `SetSession`.
    pub(crate) shard_allocate_step: u64,
    /// Go `SessionVars.EnableNoBackslashEscapesInLike`, maintained by `tidb_enable_no_backslash_escapes_in_like`'s `SetSession`.
    pub(crate) enable_no_backslash_escapes_in_like: bool,
    /// Go `SessionVars.DivPrecisionIncrement`, maintained by `div_precision_increment`'s `SetSession`.
    pub(crate) div_precision_increment: u32,
    /// Go `SessionVars.WindowingUseHighPrecision`, maintained by `windowing_use_high_precision`'s `SetSession`.
    pub(crate) windowing_use_high_precision: bool,
    /// Go `SessionVars.EnablePipelinedWindowExec`, maintained by `tidb_enable_pipelined_window_function`'s `SetSession`.
    pub(crate) enable_pipelined_window_exec: bool,
    /// Go `SessionVars.CTEMaxRecursionDepth`, maintained by `cte_max_recursion_depth`'s `SetSession`.
    pub(crate) cte_max_recursion_depth: i64,
    /// Go `SessionVars.TiDBOptJoinReorderThreshold`, maintained by `tidb_opt_join_reorder_threshold`'s `SetSession`.
    pub(crate) tidb_opt_join_reorder_threshold: i32,
    /// Go `SessionVars.DefaultStrMatchSelectivity`, maintained by `tidb_default_string_match_selectivity`'s `SetSession`.
    pub(crate) default_str_match_selectivity: f64,
    /// Go `SessionVars.EnablePseudoForOutdatedStats`, maintained by `tidb_enable_pseudo_for_outdated_stats`'s `SetSession`.
    pub(crate) enable_pseudo_for_outdated_stats: bool,
    /// Go `SessionVars.StatsLoadSyncWait`, maintained by `tidb_stats_load_sync_wait`'s `SetSession`.
    pub(crate) stats_load_sync_wait: u64,
    /// Go `SessionVars.OptIndexPruneThreshold`, maintained by `tidb_opt_index_prune_threshold`'s `SetSession`.
    pub(crate) opt_index_prune_threshold: i32,
    /// Go `SessionVars.RangeMaxSize`, maintained by `tidb_opt_range_max_size`'s `SetSession`.
    pub(crate) range_max_size: i64,
    /// Go `SessionVars.RangeMaxCount` (master #70768), maintained by
    /// `tidb_opt_range_max_count`'s `SetSession`.
    pub(crate) range_max_count: i64,
    /// Go `SessionVars.OptPrefixIndexSingleScan`, maintained by `tidb_opt_prefix_index_single_scan`'s `SetSession`.
    pub(crate) opt_prefix_index_single_scan: bool,
    /// Go `SessionVars.allowPreferRangeScan`, maintained by `tidb_opt_prefer_range_scan`'s `SetSession`.
    pub(crate) allow_prefer_range_scan: bool,
    /// Go `SessionVars.OptimizerUseInvisibleIndexes`, maintained by `tidb_opt_use_invisible_indexes`'s `SetSession`.
    pub(crate) optimizer_use_invisible_indexes: bool,
    /// Go `SessionVars.AlwaysKeepJoinKey`, maintained by `tidb_opt_always_keep_join_key`'s `SetSession`.
    pub(crate) always_keep_join_key: bool,
    /// Go `SessionVars.AllowAggPushDown`, maintained by `tidb_opt_agg_push_down`'s `SetSession`.
    pub(crate) allow_agg_push_down: bool,
    /// Go `SessionVars.Enable3StageDistinctAgg`, maintained by `tidb_opt_three_stage_distinct_agg`'s `SetSession`.
    pub(crate) enable_3_stage_distinct_agg: bool,
    /// Go `SessionVars.Enable3StageMultiDistinctAgg`, maintained by `tidb_opt_enable_three_stage_multi_distinct_agg`'s `SetSession`.
    pub(crate) enable_3_stage_multi_distinct_agg: bool,
    /// Go `SessionVars.TiFlashPreAggMode`, maintained by `tiflash_hashagg_preaggregation_mode`'s `SetSession`.
    pub(crate) tiflash_pre_agg_mode: String,
    /// Go `SessionVars.OptPartialOrderedIndexForTopN`, maintained by `tidb_opt_partial_ordered_index_for_topn`'s `SetSession`.
    pub(crate) opt_partial_ordered_index_for_topn: String,
    /// Go `SessionVars.EnableUnsafeSubstitute`, maintained by `tidb_enable_unsafe_substitute`'s `SetSession`.
    pub(crate) enable_unsafe_substitute: bool,
    /// Go `SessionVars.enableForceInlineCTE`, maintained by `tidb_opt_force_inline_cte`'s `SetSession`.
    pub(crate) enable_force_inline_cte: bool,
    /// Go `SessionVars.EnableMPPSharedCTEExecution`, maintained by `tidb_opt_enable_mpp_shared_cte_execution`'s `SetSession`.
    pub(crate) enable_mpp_shared_cte_execution: bool,
    /// Go `SessionVars.EnableAdvancedJoinHint`, maintained by `tidb_opt_advanced_join_hint`'s `SetSession`.
    pub(crate) enable_advanced_join_hint: bool,
    /// Go `SessionVars.EnableSemiJoinRewrite`, maintained by `tidb_opt_enable_semi_join_rewrite`'s `SetSession`.
    pub(crate) enable_semi_join_rewrite: bool,
    /// Go `SessionVars.EnableAlternativeLogicalPlans`, maintained by `tidb_opt_enable_alternative_logical_plans`'s `SetSession`.
    pub(crate) enable_alternative_logical_plans: bool,
    /// Go `SessionVars.ExplainNonEvaledSubQuery`, maintained by `tidb_opt_enable_non_eval_scalar_subquery`'s `SetSession`.
    pub(crate) explain_non_evaled_sub_query: bool,
    /// Go `SessionVars.allowInSubqToJoinAndAgg`, maintained by `tidb_opt_insubq_to_join_and_agg`'s `SetSession`.
    pub(crate) set_allow_in_subq_to_join_and_agg: bool,
    /// Go `SessionVars.EnableNoDecorrelateInSelect`, maintained by `tidb_opt_enable_no_decorrelate_in_select`'s `SetSession`.
    pub(crate) enable_no_decorrelate_in_select: bool,
    /// Go `SessionVars.EnableSkewDistinctAgg`, maintained by `tidb_opt_skew_distinct_agg`'s `SetSession`.
    pub(crate) enable_skew_distinct_agg: bool,
    /// Go `SessionVars.EnableAdvancedJoinReorder`, maintained by `tidb_opt_enable_advanced_join_reorder`'s `SetSession`.
    pub(crate) enable_advanced_join_reorder: bool,
    /// Go `SessionVars.ConstraintCheckInPlace`, maintained by `tidb_constraint_check_in_place`'s `SetSession`.
    pub(crate) constraint_check_in_place: bool,
    /// Go `SessionVars.OptOrderingIdxSelRatio`, maintained by `tidb_opt_ordering_index_selectivity_ratio`'s `SetSession`.
    pub(crate) opt_ordering_idx_sel_ratio: f64,
    /// Go `SessionVars.IndexJoinMaxScanRowsRatio`, maintained by `tidb_opt_index_join_max_scan_rows_ratio`'s `SetSession`.
    pub(crate) index_join_max_scan_rows_ratio: f64,
    /// Go `SessionVars.OptOrderingIdxSelThresh`, maintained by `tidb_opt_ordering_index_selectivity_threshold`'s `SetSession`.
    pub(crate) opt_ordering_idx_sel_thresh: f64,
    /// Go `SessionVars.AllowProjectionPushDown`, maintained by `tidb_opt_projection_push_down`'s `SetSession`.
    pub(crate) allow_projection_push_down: bool,
    /// Go `SessionVars.AllowDistinctAggPushDown`, maintained by `tidb_opt_distinct_agg_push_down`'s `SetSession`.
    pub(crate) allow_distinct_agg_push_down: bool,
    /// Go `SessionVars.enableCascadesPlanner`, maintained by `tidb_enable_cascades_planner`'s `SetSession`.
    pub(crate) enable_cascades_planner: bool,
    /// Go `SessionVars.DisableHashJoin (negated)`, maintained by `tidb_opt_enable_hash_join`'s `SetSession`.
    pub(crate) enable_hash_join: bool,
    /// Go `SessionVars.OptimizerEnableNAAJ`, maintained by `tidb_enable_null_aware_anti_join`'s `SetSession`.
    pub(crate) optimizer_enable_naaj: bool,
    /// Go `SessionVars.LimitPushDownThreshold`, maintained by `tidb_opt_limit_push_down_threshold`'s `SetSession`.
    pub(crate) limit_push_down_threshold: u64,
    /// Go `SessionVars.ReadStaleness`, maintained by `tidb_read_staleness`'s `SetSession`.
    pub(crate) read_staleness: i64,
    /// Go `SessionVars.IndexLookUpPushDownPolicy`, maintained by `tidb_index_lookup_pushdown_policy`'s `SetSession`.
    pub(crate) index_lookup_push_down_policy: String,
    /// Go `SessionVars.TiDBOptJoinReorderThroughProj`, maintained by `tidb_opt_join_reorder_through_proj`'s `SetSession`.
    pub(crate) tidb_opt_join_reorder_through_proj: bool,
    /// Go `SessionVars.TiDBOptJoinReorderThroughSel`, maintained by `tidb_opt_join_reorder_through_sel`'s `SetSession`.
    pub(crate) tidb_opt_join_reorder_through_sel: bool,
    /// Go `SessionVars.EnableOuterJoinReorder`, maintained by `tidb_enable_outer_join_reorder`'s `SetSession`.
    pub(crate) enable_outer_join_reorder: bool,
    /// Go `SessionVars.enableIndexMerge`, maintained by `tidb_enable_index_merge`'s `SetSession`.
    pub(crate) enable_index_merge: bool,
    /// Go `SessionVars.SkipMissingPartitionStats`, maintained by `tidb_skip_missing_partition_stats`'s `SetSession`.
    pub(crate) skip_missing_partition_stats: bool,
    /// Go `SessionVars.OptimizerEnableNewOnlyFullGroupByCheck`, maintained by `tidb_enable_new_only_full_group_by_check`'s `SetSession`.
    pub(crate) optimizer_enable_new_only_full_group_by_check: bool,
    /// Go `SessionVars.EnableAutoIncrementInGenerated`, maintained by `tidb_enable_auto_increment_in_generated`'s `SetSession`.
    pub(crate) enable_auto_increment_in_generated: bool,
    /// Go `SessionVars.PrimaryKeyRequired`, maintained by `sql_require_primary_key`'s `SetSession`.
    pub(crate) primary_key_required: bool,
    /// Go `SessionVars.TMPTableSize`, maintained by `tidb_tmp_table_max_size`'s `SetSession`.
    pub(crate) tmp_table_size: i64,
    /// Go `SessionVars.RemoveOrderbyInSubquery`, maintained by `tidb_remove_orderby_in_subquery`'s `SetSession`.
    pub(crate) remove_orderby_in_subquery: bool,
    /// Go `SessionVars.AnalyzeVersion`, maintained by `tidb_analyze_version`'s `SetSession`.
    pub(crate) analyze_version: i64,
    /// Go `SessionVars.EnabledRateLimitAction`, maintained by `tidb_enable_rate_limit_action`'s `SetSession`.
    pub(crate) enabled_rate_limit_action: bool,
    /// Go `SessionVars.LockWaitTimeout`, maintained by `innodb_lock_wait_timeout`'s `SetSession`.
    pub(crate) lock_wait_timeout: u64,
    /// Go `SessionVars.ReplicaClosestReadThreshold`, maintained by `tidb_adaptive_closest_read_threshold`'s `SetSession`.
    pub(crate) replica_closest_read_threshold: i64,
    /// Go `SessionVars.LoadBasedReplicaReadThreshold`, maintained by `tidb_load_based_replica_read_threshold`'s `SetSession`.
    pub(crate) load_based_replica_read_threshold: i64,
    /// Go `SessionVars.TiKVClientReadTimeout`, maintained by `tikv_client_read_timeout`'s `SetSession`.
    pub(crate) tikv_client_read_timeout: u64,
    /// Go `SessionVars.IsolationReadEngines`, maintained by `tidb_isolation_read_engines`'s `SetSession`.
    pub(crate) isolation_read_engines: String,
    /// Go `SessionVars.InitChunkSize`, maintained by `tidb_init_chunk_size`'s `SetSession`.
    pub(crate) init_chunk_size: usize,
    /// Go `SessionVars.MaxChunkSize`, maintained by `tidb_max_chunk_size`'s `SetSession`.
    pub(crate) max_chunk_size: usize,
    /// Go `SessionVars.MetricSchemaStep`, maintained by `tidb_metric_query_step`'s `SetSession`.
    pub(crate) metric_schema_step: i64,
    /// Go `SessionVars.MetricSchemaRangeDuration`, maintained by `tidb_metric_query_range_duration`'s `SetSession`.
    pub(crate) metric_schema_range_duration: i64,
    /// Go `SessionVars.EnableParallelApply`, maintained by `tidb_enable_parallel_apply`'s `SetSession`.
    pub(crate) enable_parallel_apply: bool,
    /// Go `SessionVars.AllowDeriveTopN`, maintained by `tidb_opt_derive_topn`'s `SetSession`.
    pub(crate) allow_derive_topn: bool,
    /// Go `SessionVars.MemQuotaApplyCache`, maintained by `tidb_mem_quota_apply_cache`'s `SetSession`.
    pub(crate) mem_quota_apply_cache: i64,
    /// Go `SessionVars.ExecutorConcurrency`, maintained by `tidb_executor_concurrency`'s `SetSession`.
    pub(crate) executor_concurrency: usize,
    /// Go `SessionVars.PipelinedDMLConfig`, maintained by `tidb_pipelined_dml_resource_policy`'s `SetSession`.
    pub(crate) pipelined_dml_config: crate::session_vars::PipelinedDmlConfig,
    /// Go `SessionVars.hashAggPartialConcurrency`, maintained by `tidb_hashagg_partial_concurrency`'s `SetSession`.
    pub(crate) hash_agg_partial_concurrency: i64,
    /// Go `SessionVars.hashAggFinalConcurrency`, maintained by `tidb_hashagg_final_concurrency`'s `SetSession`.
    pub(crate) hash_agg_final_concurrency: i64,
    /// Go `SessionVars.DDLReorgPriority`, maintained by `tidb_ddl_reorg_priority`'s `SetSession`.
    pub(crate) ddl_reorg_priority: i32,
    /// Go `SessionVars.MemArbitrator.WaitAverse`, maintained by `tidb_mem_arbitrator_wait_averse`'s `SetSession`.
    pub(crate) mem_arbitrator_wait_averse: Option<bool>,
    /// Go `SessionVars.MemArbitrator.QueryReserved`, maintained by `tidb_mem_arbitrator_query_reserved`'s `SetSession`.
    pub(crate) mem_arbitrator_query_reserved: i64,
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
        // Go `timeutil.ParseTimeZone`: an IANA name, else a `+HH:MM[:SS]`
        // offset in Go's duration grammar (seconds kept, `+00:00:30`).
        match tidb_timeutil::parse_time_zone(written) {
            Ok(tidb_timeutil::TimeZone::Named(zone)) => return SessionTimeZone::Named(zone),
            Ok(tidb_timeutil::TimeZone::Fixed { offset_secs, .. }) => {
                return SessionTimeZone::Fixed {
                    name: written.to_owned(),
                    offset_secs,
                };
            }
            Ok(tidb_timeutil::TimeZone::Local) | Err(_) => {}
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
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsConnectionState {
    /// Go `Version`.
    pub version: u16,
    /// Go `CipherSuite`.
    pub cipher_suite: u16,
    /// Go `PeerCertificates`, represented by each certificate's DER `Raw` bytes.
    pub peer_certificates: Vec<Vec<u8>>,
}

/// Go `NewSessionVars`' `systems`: only `character_set_connection` and
/// `collation_connection`, at `charset.GetDefaultCharsetAndCollate()` -- two
/// `skipInit` variables a session reads (the parser's connection charset)
/// before anything loads the global tier.
fn new_session_systems() -> HashMap<String, String> {
    let (charset, collation) = tidb_charset::charset::get_default_charset_and_collate();
    HashMap::from([
        ("character_set_connection".to_owned(), charset.to_owned()),
        ("collation_connection".to_owned(), collation.to_owned()),
    ])
}

impl Default for SessionVars {
    fn default() -> Self {
        Self {
            stmt_warnings: Vec::new(),
            last_txn_info: String::new(),
            last_commit_ts: 0,
            last_plan_replayer_token: String::new(),
            in_plan_replayer: false,
            plan_replayer_finished_task_key: std::collections::HashSet::new(),
            last_query_info: tidb_sessionstates::QueryInfo::default(),
            last_ddl_info: tidb_sessionstates::LastDDLInfo::default(),
            resource_group: "default".to_owned(),
            restricted_sql: false,
            in_mview_maintenance: false,
            request_source_type: String::new(),
            tls_connection_state: None,
            compression_algorithm: tidb_mysql::consts::CompressionNone,
            compression_level: 0,
            keys_examined: 0,
            prepared_statement_id: 0,
            in_multi_stmts: false,
            sys_warning_count: 0,
            current_tso: CurrentTso::default(),
            rng: Arc::new(tidb_mathutil::MysqlRng::new_with_time()),
            stmt_type_is_set: false,
            sys_error_count: 0,
            current_user: None,
            auth_plugin: String::new(),
            connection_info: None,
            active_roles: std::sync::Arc::default(),
            login_user: None,
            connection_id: None,
            client_capability: 0,
            enable_row_level_checksum: tidb_vardef::ENABLE_ROW_LEVEL_CHECKSUM
                .load(std::sync::atomic::Ordering::SeqCst),
            last_insert_id: 0,
            in_set_session_states_stmt: false,
            statement_insert_id: 0,
            prev_row_count: 0,
            last_found_rows: 0,
            current_db_changed: false,
            prev_stmt_text: None,
            found_in_plan_cache: false,
            prev_found_in_plan_cache: false,
            pre_use_chunk_alloc: tidb_vardef::defaults::DEF_TIDB_USE_ALLOC,
            current_db: String::new(),
            sandbox_mode: false,
            found_in_binding: false,
            prev_found_in_binding: false,
            systems: new_session_systems(),
            autocommit: true,
            enable_ddl_analyze_exec_opt: false,
            sql_mode: tidb_mysql::get_sql_mode(tidb_mysql::DefaultSQLMode)
                .expect("the compiled default SQL mode is valid"),
            max_allowed_packet: 64 << 20,
            in_txn: false,
            explicit_request_source_type: String::new(),
            scatter_region: tidb_vardef::defaults::DEF_TIDB_SCATTER_REGION.to_owned(),
            enable_plan_replayed_continues_capture: false,
            enable_plan_replayer_capture: tidb_vardef::defaults::DEF_TIDB_ENABLE_PLAN_REPLAYER_CAPTURE,
            group_concat_max_len: tidb_vardef::defaults::DEF_GROUP_CONCAT_MAX_LEN,
            default_collation_for_utf8mb4: crate::vars_typed::registry_default_collation_for_utf8mb4(),
            allow_write_row_id: crate::vars_typed::registry_allow_write_row_id(),
            enable_stable_result_mode: crate::vars_typed::registry_enable_stable_result_mode(),
            sysdate_is_now: crate::vars_typed::registry_sysdate_is_now(),
            skip_ascii_check: crate::vars_typed::registry_skip_ascii_check(),
            skip_utf8_check: crate::vars_typed::registry_skip_utf8_check(),
            allow_auto_rand_explicit_insert: crate::vars_typed::registry_allow_auto_rand_explicit_insert(),
            shard_allocate_step: crate::vars_typed::registry_shard_allocate_step(),
            enable_no_backslash_escapes_in_like: crate::vars_typed::registry_enable_no_backslash_escapes_in_like(),
            div_precision_increment: crate::vars_typed::registry_div_precision_increment(),
            windowing_use_high_precision: crate::vars_typed::registry_windowing_use_high_precision(),
            enable_pipelined_window_exec: crate::vars_typed::registry_enable_pipelined_window_exec(),
            cte_max_recursion_depth: crate::vars_typed::registry_cte_max_recursion_depth(),
            tidb_opt_join_reorder_threshold: crate::vars_typed::registry_tidb_opt_join_reorder_threshold(),
            default_str_match_selectivity: crate::vars_typed::registry_default_str_match_selectivity(),
            enable_pseudo_for_outdated_stats: crate::vars_typed::registry_enable_pseudo_for_outdated_stats(),
            stats_load_sync_wait: crate::vars_typed::registry_stats_load_sync_wait(),
            opt_index_prune_threshold: crate::vars_typed::registry_opt_index_prune_threshold(),
            range_max_size: crate::vars_typed::registry_range_max_size(),
            range_max_count: tidb_vardef::defaults::DEF_TIDB_OPT_RANGE_MAX_COUNT,
            opt_prefix_index_single_scan: crate::vars_typed::registry_opt_prefix_index_single_scan(),
            allow_prefer_range_scan: crate::vars_typed::registry_allow_prefer_range_scan(),
            optimizer_use_invisible_indexes: crate::vars_typed::registry_optimizer_use_invisible_indexes(),
            always_keep_join_key: crate::vars_typed::registry_always_keep_join_key(),
            allow_agg_push_down: crate::vars_typed::registry_allow_agg_push_down(),
            enable_3_stage_distinct_agg: crate::vars_typed::registry_enable_3_stage_distinct_agg(),
            enable_3_stage_multi_distinct_agg: crate::vars_typed::registry_enable_3_stage_multi_distinct_agg(),
            tiflash_pre_agg_mode: crate::vars_typed::registry_tiflash_pre_agg_mode(),
            opt_partial_ordered_index_for_topn: crate::vars_typed::registry_opt_partial_ordered_index_for_topn(),
            enable_unsafe_substitute: crate::vars_typed::registry_enable_unsafe_substitute(),
            enable_force_inline_cte: crate::vars_typed::registry_enable_force_inline_cte(),
            enable_mpp_shared_cte_execution: crate::vars_typed::registry_enable_mpp_shared_cte_execution(),
            enable_advanced_join_hint: crate::vars_typed::registry_enable_advanced_join_hint(),
            enable_semi_join_rewrite: crate::vars_typed::registry_enable_semi_join_rewrite(),
            enable_alternative_logical_plans: crate::vars_typed::registry_enable_alternative_logical_plans(),
            explain_non_evaled_sub_query: crate::vars_typed::registry_explain_non_evaled_sub_query(),
            set_allow_in_subq_to_join_and_agg: crate::vars_typed::registry_set_allow_in_subq_to_join_and_agg(),
            enable_no_decorrelate_in_select: crate::vars_typed::registry_enable_no_decorrelate_in_select(),
            enable_skew_distinct_agg: crate::vars_typed::registry_enable_skew_distinct_agg(),
            enable_advanced_join_reorder: crate::vars_typed::registry_enable_advanced_join_reorder(),
            constraint_check_in_place: crate::vars_typed::registry_constraint_check_in_place(),
            opt_ordering_idx_sel_ratio: crate::vars_typed::registry_opt_ordering_idx_sel_ratio(),
            index_join_max_scan_rows_ratio: crate::vars_typed::registry_index_join_max_scan_rows_ratio(),
            opt_ordering_idx_sel_thresh: crate::vars_typed::registry_opt_ordering_idx_sel_thresh(),
            allow_projection_push_down: crate::vars_typed::registry_allow_projection_push_down(),
            allow_distinct_agg_push_down: crate::vars_typed::registry_allow_distinct_agg_push_down(),
            enable_cascades_planner: crate::vars_typed::registry_enable_cascades_planner(),
            enable_hash_join: crate::vars_typed::registry_enable_hash_join(),
            optimizer_enable_naaj: crate::vars_typed::registry_optimizer_enable_naaj(),
            limit_push_down_threshold: crate::vars_typed::registry_limit_push_down_threshold(),
            read_staleness: crate::vars_typed::registry_read_staleness(),
            index_lookup_push_down_policy: crate::vars_typed::registry_index_lookup_push_down_policy(),
            tidb_opt_join_reorder_through_proj: crate::vars_typed::registry_tidb_opt_join_reorder_through_proj(),
            tidb_opt_join_reorder_through_sel: crate::vars_typed::registry_tidb_opt_join_reorder_through_sel(),
            enable_outer_join_reorder: crate::vars_typed::registry_enable_outer_join_reorder(),
            enable_index_merge: crate::vars_typed::registry_enable_index_merge(),
            skip_missing_partition_stats: crate::vars_typed::registry_skip_missing_partition_stats(),
            optimizer_enable_new_only_full_group_by_check: crate::vars_typed::registry_optimizer_enable_new_only_full_group_by_check(),
            enable_auto_increment_in_generated: crate::vars_typed::registry_enable_auto_increment_in_generated(),
            primary_key_required: crate::vars_typed::registry_primary_key_required(),
            tmp_table_size: crate::vars_typed::registry_tmp_table_size(),
            remove_orderby_in_subquery: crate::vars_typed::registry_remove_orderby_in_subquery(),
            analyze_version: crate::vars_typed::registry_analyze_version(),
            enabled_rate_limit_action: crate::vars_typed::registry_enabled_rate_limit_action(),
            lock_wait_timeout: crate::vars_typed::registry_lock_wait_timeout(),
            replica_closest_read_threshold: crate::vars_typed::registry_replica_closest_read_threshold(),
            load_based_replica_read_threshold: crate::vars_typed::registry_load_based_replica_read_threshold(),
            tikv_client_read_timeout: crate::vars_typed::registry_tikv_client_read_timeout(),
            isolation_read_engines: crate::vars_typed::registry_isolation_read_engines(),
            init_chunk_size: crate::vars_typed::registry_init_chunk_size(),
            max_chunk_size: crate::vars_typed::registry_max_chunk_size(),
            metric_schema_step: crate::vars_typed::registry_metric_schema_step(),
            metric_schema_range_duration: crate::vars_typed::registry_metric_schema_range_duration(),
            enable_parallel_apply: crate::vars_typed::registry_enable_parallel_apply(),
            allow_derive_topn: crate::vars_typed::registry_allow_derive_topn(),
            mem_quota_apply_cache: crate::vars_typed::registry_mem_quota_apply_cache(),
            executor_concurrency: crate::vars_typed::registry_executor_concurrency(),
            pipelined_dml_config: crate::vars_typed::registry_pipelined_dml_config(),
            hash_agg_partial_concurrency: crate::vars_typed::registry_hash_agg_partial_concurrency(),
            hash_agg_final_concurrency: crate::vars_typed::registry_hash_agg_final_concurrency(),
            ddl_reorg_priority: crate::vars_typed::registry_ddl_reorg_priority(),
            mem_arbitrator_wait_averse: crate::vars_typed::registry_mem_arbitrator_wait_averse(),
            mem_arbitrator_query_reserved: crate::vars_typed::registry_mem_arbitrator_query_reserved(),
            partition_prune_mode: tidb_vardef::defaults::DEF_TIDB_PARTITION_PRUNE_MODE.to_owned(),
            enable_inl_join_inner_multi_pattern: tidb_vardef::defaults::DEF_TIDB_ENABLE_INL_JOIN_MULTI_PATTERN,
            tiflash_replica_read: tidb_tiflash::get_tiflash_replica_read_by_str(
                tidb_vardef::defaults::DEF_TIFLASH_REPLICA_READ,
            ),
            session_alias: String::new(),
            slow_log_rules: String::new(),
            max_keys_read: 0,
            max_execution_time: 0,
            dml_max_execution_time: 0,
            in_non_transactional_dml: false,
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
            enable_mview: tidb_vardef::defaults::DEF_TIDB_MVIEW_ENABLE,
            enable_full_outer_join: tidb_vardef::defaults::DEF_TIDB_ENABLE_FULL_OUTER_JOIN,
            enable_redact_log: tidb_vardef::defaults::DEF_TIDB_REDACT_LOG.to_owned(),
            ti_flash_max_bytes_before_ext_join:
                tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_JOIN,
            ti_flash_max_bytes_before_ext_agg:
                tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY,
            ti_flash_max_bytes_before_ext_sort:
                tidb_vardef::defaults::DEF_TIFLASH_MAX_BYTES_BEFORE_EXTERNAL_SORT,
            ti_flash_mem_quota_query_per_node:
                tidb_vardef::defaults::DEF_TIFLASH_MEM_QUOTA_QUERY_PER_NODE,
            mem_quota_query: tidb_vardef::defaults::DEF_TIDB_MEM_QUOTA_QUERY,
            cop_tiflash_concurrency_factor: tidb_vardef::defaults::DEF_OPT_TIFLASH_CONCURRENCY_FACTOR,
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
            common_global_loaded: false,
            first_read_globals: FirstReadGlobals::default(),
            prepared_stmt_entries: 0,
        }
    }
}

/// Go `variable.PreparedStmtCount`: the prepared statements every session of
/// this process holds, which `max_prepared_stmt_count` bounds.
pub static PREPARED_STMT_COUNT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

impl SessionVars {
    /// Go `SessionVars.AddPreparedStmt`: one more prepared statement, refused
    /// with `ErrMaxPreparedStmtCountReached` when it would pass the GLOBAL
    /// `max_prepared_stmt_count` (-1: unbounded).
    ///
    /// # Errors
    ///
    /// `ErrMaxPreparedStmtCountReached`.
    pub fn add_prepared_stmt(&mut self) -> Result<(), tidb_terror::TerrorError> {
        use std::sync::atomic::Ordering;
        let max = self
            .globals
            .as_ref()
            .and_then(|globals| globals.get("max_prepared_stmt_count").ok())
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_MAX_PREPARED_STMT_COUNT);
        let count = PREPARED_STMT_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
        if max >= 0 && count > max {
            PREPARED_STMT_COUNT.fetch_sub(1, Ordering::SeqCst);
            return Err(crate::variable_errors::ERR_MAX_PREPARED_STMT_COUNT_REACHED
                .gen_with_stack_by_args(&[max.into()]));
        }
        self.prepared_stmt_entries += 1;
        Ok(())
    }

    /// Go `SessionVars.RemovePreparedStmt` for a statement this session held.
    pub fn remove_prepared_stmt(&mut self) {
        if self.prepared_stmt_entries > 0 {
            self.prepared_stmt_entries -= 1;
            PREPARED_STMT_COUNT.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Go `SessionVars.WithdrawAllPreparedStmt`, at session close.
    pub fn withdraw_all_prepared_stmt(&mut self) {
        let entries = std::mem::take(&mut self.prepared_stmt_entries);
        PREPARED_STMT_COUNT.fetch_sub(entries, std::sync::atomic::Ordering::SeqCst);
    }

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

    /// [`Self::set_global_vars_accessor`] and then
    /// [`Self::load_common_global_variables_if_needed`] at once.
    pub fn seed_from_globals(
        &mut self,
        globals: Arc<dyn crate::GlobalVarAccessor>,
    ) -> Result<(), VarError> {
        self.set_global_vars_accessor(globals);
        self.load_common_global_variables_if_needed()
    }

    /// Go `createSession`'s `sessionVars.GlobalVarsAccessor = s`: the GLOBAL
    /// tier this session reads `@@global.x` from and, at its first statement,
    /// copies its own variables from.
    pub fn set_global_vars_accessor(&mut self, globals: Arc<dyn crate::GlobalVarAccessor>) {
        self.globals = Some(globals);
        self.first_read_globals = FirstReadGlobals::default();
        self.common_global_loaded = false;
    }

    /// Go `session.loadCommonGlobalVariablesIfNeeded`, run as a statement
    /// starts: once per session, the session's own copy of the global tier --
    /// Go's rule that a session's variables are copied from the global tier
    /// once. A `SET GLOBAL` another session runs AFTER this copy is invisible
    /// to this session's plain `@@x` (only to `@@global.x`), exactly as MySQL
    /// documents. A variable the session already set (the handshake's
    /// character set, an internal session's settings) keeps its value.
    pub fn load_common_global_variables_if_needed(&mut self) -> Result<(), VarError> {
        if self.common_global_loaded {
            return Ok(());
        }
        let Some(globals) = self.globals.clone() else {
            return Ok(());
        };
        self.common_global_loaded = true;
        // Deep copy the sysvar cache's session view (`Domain.GetSessionCache`),
        // which holds every variable `SkipInit` does not exclude -- session
        // scope, not noop, not one of Go's `skipInit` variables -- at its
        // GLOBAL value or, for a session-only variable, its registry value.
        // Each one not already held goes through its `SetSession`
        // (`SetSystemVarWithRelaxedValidation`), which keeps the typed
        // session state, and a charset and its collation, in lockstep.
        for (name, value) in globals.session_cache() {
            if self.systems.contains_key(&name) {
                continue;
            }
            match self.set_system_var_with_relaxed_validation(&name, value) {
                Ok(()) => {}
                // sessionCache is stale; sysvar has likely been unregistered
                Err(error) if error.code == tidb_errno::errcode::ErrUnknownSystemVariable => {}
                Err(error) => return Err(error),
            }
        }
        // when client set Capability Flags CLIENT_INTERACTIVE, init
        // wait_timeout with interactive_timeout
        if self.client_capability & tidb_mysql::consts::ClientInteractive != 0 {
            if let Some(interactive) = self.systems.get("interactive_timeout").cloned() {
                self.set_system_var("wait_timeout", interactive)?;
            }
        }
        self.session_resolved = Self::build_session_image(&self.systems);
        self.generation += 1;
        Ok(())
    }

    /// Go `SessionVars.SetSystemVarWithRelaxedValidation`: the value as
    /// `ValidateWithRelaxedValidation` normalizes it, through the
    /// variable's `SetSession`.
    ///
    /// # Errors
    ///
    /// An unknown variable, or the variable's `SetSession` refusal.
    pub fn set_system_var_with_relaxed_validation(&mut self, name: &str, value: String) -> Result<(), VarError> {
        let Some(def) = get_sys_var(name) else {
            return Err(tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_UNKNOWN_SYSTEM_VAR
                    .gen_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
            ));
        };
        let value = def.validate_with_relaxed_validation(&value, SCOPE_SESSION);
        self.set_session_from_hook(def.name.to_owned(), value)
    }

























    fn replica_read_from_value(value: Option<&str>) -> tidb_txnkv::ReplicaReadType {
        match value
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



    /// Go `SessionVars.InTxn`: the `ServerStatusInTrans` bit, which the
    /// session sets as it enters a transaction and clears as it ends one,
    /// and `autocommit`'s `SetSession` clears on the OFF to ON transition.
    #[must_use]
    pub const fn in_txn(&self) -> bool {
        self.in_txn
    }

    /// Go `SessionVars.SetInTxn`.
    pub fn set_in_txn(&mut self, in_txn: bool) {
        self.in_txn = in_txn;
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

    /// Go `getSessionFactoryInternal`'s `sessionVars.CommonGlobalLoaded =
    /// true` on a pooled internal session: the global tier is never copied
    /// in, so its typed `SQLMode` keeps `NewSessionVars`'s zero value
    /// (`mysql.ModeNone`) while `@@sql_mode` still reads its default.
    pub fn mark_common_global_loaded(&mut self) {
        self.common_global_loaded = true;
        self.sql_mode = tidb_mysql::ModeNone;
    }

    /// Go `SessionVars.MaxAllowedPacket`, parsed by the sysvar hook when the
    /// session copy changes rather than by each consumer.
    #[must_use]
    pub const fn max_allowed_packet(&self) -> u64 {
        self.max_allowed_packet
    }

    /// Go `SessionVars.ExplicitRequestSourceType`.
    #[must_use]
    pub fn explicit_request_source_type(&self) -> &str {
        &self.explicit_request_source_type
    }

    /// Go `SessionVars.ScatterRegion`.
    #[must_use]
    pub fn scatter_region(&self) -> &str {
        &self.scatter_region
    }

    /// Go `SessionVars.EnablePlanReplayedContinuesCapture`.
    #[must_use]
    pub const fn enable_plan_replayed_continues_capture(&self) -> bool {
        self.enable_plan_replayed_continues_capture
    }

    /// Go `SessionVars.EnablePlanReplayerCapture`.
    #[must_use]
    pub const fn enable_plan_replayer_capture(&self) -> bool {
        self.enable_plan_replayer_capture
    }

    /// Go `SessionVars.GroupConcatMaxLen`.
    #[must_use]
    pub const fn group_concat_max_len(&self) -> u64 {
        self.group_concat_max_len
    }

    /// Go `SessionVars.PartitionPruneMode.Load()`.
    #[must_use]
    pub fn partition_prune_mode(&self) -> &str {
        &self.partition_prune_mode
    }

    /// Go `SessionVars.EnableINLJoinInnerMultiPattern`.
    #[must_use]
    pub const fn enable_inl_join_inner_multi_pattern(&self) -> bool {
        self.enable_inl_join_inner_multi_pattern
    }

    /// Go `SessionVars.TiFlashReplicaRead`.
    #[must_use]
    pub const fn tiflash_replica_read(&self) -> tidb_tiflash::ReplicaRead {
        self.tiflash_replica_read
    }

    /// Go `SessionVars.SessionAlias`.
    #[must_use]
    pub fn session_alias(&self) -> &str {
        &self.session_alias
    }

    /// Go `SessionVars.SlowLogRules`'s `RawRules`, empty when unset.
    #[must_use]
    pub fn slow_log_rules(&self) -> &str {
        &self.slow_log_rules
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

    /// Go `SessionVars.DMLMaxExecutionTime`, in milliseconds; zero disables
    /// the transactional DML and COMMIT timeout.
    #[must_use]
    pub const fn dml_max_execution_time(&self) -> u64 {
        self.dml_max_execution_time
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
    pub const fn enable_prepared_plan_cache(&self) -> bool {
        self.enable_prepared_plan_cache
    }

    /// Go `SessionVars.SharedLockPromotion`, which makes `FOR SHARE` use the
    /// exclusive-lock path instead of the no-op shared-lock path.
    #[must_use]
    pub const fn shared_lock_promotion(&self) -> bool {
        self.shared_lock_promotion
    }

    /// Go `SessionVars.EnableWindowFunction`, updated by the normalized
    /// `tidb_enable_window_function` bool sysvar.
    #[must_use]
    pub const fn enable_window_function(&self) -> bool {
        self.enable_window_function
    }

    /// Go `SessionVars.EnableMView`, updated by the normalized
    /// `tidb_mview_enable` bool sysvar.
    #[must_use]
    pub const fn enable_mview(&self) -> bool {
        self.enable_mview
    }

    /// Go master `SessionVars.EnableFullOuterJoin`, updated by the normalized
    /// `tidb_enable_full_outer_join` bool sysvar.
    #[must_use]
    pub const fn enable_full_outer_join(&self) -> bool {
        self.enable_full_outer_join
    }

    /// Go `SessionVars.EnableRedactLog`.
    #[must_use]
    pub fn enable_redact_log(&self) -> &str {
        &self.enable_redact_log
    }

    /// Go `TiDBRedactLog`'s `SetGlobal` hook: the session that sets the
    /// GLOBAL value takes it as its own; Go master's refresh observers set it
    /// on their borrowed session (`setMVRefreshObserveRedaction`).
    pub fn set_enable_redact_log(&mut self, value: String) {
        self.enable_redact_log = value;
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
    pub const fn tiflash_max_bytes_before_external_join(&self) -> i64 {
        self.ti_flash_max_bytes_before_ext_join
    }

    /// Go `SessionVars.TiFlashMaxBytesBeforeExternalGroupBy`.
    #[must_use]
    pub const fn tiflash_max_bytes_before_external_group_by(&self) -> i64 {
        self.ti_flash_max_bytes_before_ext_agg
    }

    /// Go `SessionVars.TiFlashMaxBytesBeforeExternalSort`.
    #[must_use]
    pub const fn tiflash_max_bytes_before_external_sort(&self) -> i64 {
        self.ti_flash_max_bytes_before_ext_sort
    }

    /// Go `SessionVars.TiFlashMemQuotaQueryPerNode`.
    #[must_use]
    pub const fn tiflash_mem_quota_query_per_node(&self) -> i64 {
        self.ti_flash_mem_quota_query_per_node
    }

    /// Go `SessionVars.TiFlashQuerySpillRatio`.
    #[must_use]
    pub const fn tiflash_query_spill_ratio(&self) -> f64 {
        self.ti_flash_query_spill_ratio
    }

    /// Go `SessionVars.BulkDMLEnabled`, set by `tidb_dml_type`.
    /// Go `SessionVars.UseLowResolutionTSO`: a user connection's
    /// `tidb_low_resolution_tso`.
    #[must_use]
    pub fn use_low_resolution_tso(&self) -> bool {
        !self.restricted_sql
            && self.connection_id.is_some_and(|id| id > 0)
            && self
                .get_system(tidb_vardef::tidb_vars::TIDB_LOW_RESOLUTION_TSO)
                .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value))
    }

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

    /// Go `SessionVars.EnableLazyCursorFetch`, set by
    /// `tidb_enable_lazy_cursor_fetch` (`TiDBOptOn`). Read from the table
    /// rather than cached: only a cursor-mode `COM_STMT_EXECUTE` asks.
    #[must_use]
    pub fn enable_lazy_cursor_fetch(&self) -> bool {
        self.get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_LAZY_CURSOR_FETCH)
            .map(|value| value.eq_ignore_ascii_case("ON") || value == "1")
            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_ENABLE_LAZY_CURSOR_FETCH)
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
            std::vec::Vec::with_capacity(crate::sysvar::sys_var_count());
        slots.resize_with(crate::sysvar::sys_var_count(), || None);
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
                    .gen_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
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
        let def = crate::sysvar::sys_var_at(index);
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
                .and_then(|value| global_get_hook(def, value).map(Cow::Owned));
        }
        // Go `GetSessionOrGlobalSystemVar`: a variable with a `GetSession`
        // hook answers from it ("shortcut to the getter").
        if let Some(get_session) = def.get_session {
            return get_session(self).map(Cow::Owned);
        }
        self.stored_system_value_at(index)
    }

    /// The value the session's variable table holds for a session-scoped
    /// variable (its own copy, else the global or default it reads), without
    /// a `GetSession` hook's answer: what Go's `s.systems` / global tier give.
    fn stored_system_value_at(&self, index: usize) -> Result<Cow<'_, str>, VarError> {
        let def = crate::sysvar::sys_var_at(index);
        if let Some(value) = self.session_resolved.values.get(index) {
            if let Some(value) = value.as_ref() {
                return Ok(Cow::Borrowed(value.as_ref()));
            }
            if self.session_resolved.values.len() == crate::sysvar::sys_var_count() {
                // A full-length image is current by construction -- every
                // `systems` mutation republishes it before returning.
                return self
                    .unseeded_global_value(index)
                    .unwrap_or_else(|| Ok(crate::sysvar::effective_default_value(def)));
            }
        }
        // The registry name is the lowercase key `systems` is written under,
        // so the caller's spelling needs no second lowercasing here.
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
        let def = crate::sysvar::sys_var_at(index);
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
        Some(self.globals().get_by_registry_index(index).and_then(|value| {
            let value = global_get_hook(def, value)?;
            first_reads.insert(index, value.clone());
            Ok(Cow::Owned(value))
        }))
    }

    /// Go `vardef.EnableNoopVariables`: the GLOBAL
    /// `tidb_enable_noop_variables` switch.
    #[must_use]
    pub fn enable_noop_variables(&self) -> bool {
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

    /// Go `GetSessionStatesSystemVar`: the value a session-state migration
    /// encodes for `name`, and whether to encode it -- the variable's
    /// `GetStateValue` if it has one, else its `GetSession`, else only the
    /// session's cached value (no global or default fallback, so an
    /// untouched variable is omitted and the destination keeps its own).
    pub fn get_session_states_system_var(&self, name: &str) -> Result<(String, bool), VarError> {
        let Some(index) = crate::sysvar::sys_var_index_lookup(name) else {
            return Err(tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_UNKNOWN_SYSTEM_VAR
                    .gen_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
            ));
        };
        let definition = crate::sysvar::sys_var_at(index);
        // Call GetStateValue first if it exists. Otherwise, call GetSession.
        if let Some(get_state_value) = definition.get_state_value {
            return get_state_value(self);
        }
        if let Some(get_session) = definition.get_session {
            return get_session(self).map(|value| (value, true));
        }
        // Only get the cached value. No need to check the global or default value.
        let key = crate::sysvar::lowered_if_needed(name);
        let first_read = self
            .first_read_globals
            .0
            .lock()
            .expect("first-read globals lock poisoned")
            .get(&index)
            .cloned();
        Ok(match self.systems.get(key.as_ref()).or(first_read.as_ref()) {
            Some(value) => (value.clone(), true),
            None => (String::new(), false),
        })
    }

    /// Go `timestamp`'s `GetStateValue`: an explicit override that is not
    /// the default.
    pub(crate) fn get_state_timestamp(&self) -> Result<(String, bool), VarError> {
        Ok(match self.systems.get("timestamp") {
            Some(timestamp) => (timestamp.clone(), timestamp != tidb_vardef::defaults::DEF_TIMESTAMP),
            None => (String::new(), false),
        })
    }

    /// Go `last_insert_id` / `identity`'s `GetStateValue`: never migrated.
    pub(crate) fn get_state_never(&self) -> Result<(String, bool), VarError> {
        Ok((String::new(), false))
    }

    /// Go `tx_isolation_one_shot`'s `GetStateValue`.
    pub(crate) fn get_state_tx_isolation_one_shot(&self) -> Result<(String, bool), VarError> {
        let (state, value) = &self.txn_isolation_level_one_shot;
        Ok(if *state == TxnIsolationLevelOneShotState::Def {
            (String::new(), false)
        } else {
            (value.clone(), true)
        })
    }

    /// Go `rand_seed1`'s `GetStateValue`.
    pub(crate) fn get_state_rand_seed1(&self) -> Result<(String, bool), VarError> {
        Ok((self.rng.get_seed1().to_string(), true))
    }

    /// Go `rand_seed2`'s `GetStateValue`.
    pub(crate) fn get_state_rand_seed2(&self) -> Result<(String, bool), VarError> {
        Ok((self.rng.get_seed2().to_string(), true))
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

    /// Puts back what [`Self::snapshot_system`] recorded: Go's
    /// `SetSystemVar(name, old)` of a statement-scoped write, whose
    /// `SetSession` brings the typed session state back with the value. An
    /// absent override is removed again, and the hook sees the value the
    /// variable then reads.
    pub fn restore_system(&mut self, snapshot: Vec<(String, Option<String>)>) {
        if snapshot.is_empty() {
            return;
        }
        for (key, previous) in snapshot {
            match previous {
                Some(value) => {
                    self.session_resolved.note(key.as_str(), Some(value.as_str()));
                    self.systems.insert(key.clone(), value);
                }
                None => {
                    self.session_resolved.note(key.as_str(), None);
                    self.systems.remove(&key);
                }
            }
            if let Some(hook) = get_sys_var(&key).and_then(|def| def.set_session) {
                // The stored value, not a `GetSession` hook's answer: that
                // hook reads the typed field the overlay set, so it would
                // hand the overlay back (Go `SetSystemVar(name, old)`
                // restores the value read before the overlay).
                let index = crate::sysvar::sys_var_index_lookup(&key)
                    .expect("a snapshot names a registered variable");
                let value = self
                    .stored_system_value_at(index)
                    .expect("a snapshot names a session variable")
                    .into_owned();
                hook(self, &value).expect("a restored value was validated before it was stored");
            }
        }
        self.generation += 1;
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
                    .gen_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
            )
        })?;
        if def.is_read_only() {
            return Err(tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_INCORRECT_SCOPE.gen_with_stack_by_args(&[
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
                    .gen_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
            ));
        }
        if !def.has_session_scope() {
            return Err(tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_GLOBAL_VARIABLE
                    .gen_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
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
                    .gen_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
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
        // Go `SetSessionFromHook`: the variable's `SetSession` first (a
        // refusal stores nothing), then the value under its name and alias.
        if let Some(hook) = get_sys_var(&key).and_then(|def| def.set_session) {
            hook(self, &value)?;
        }
        // Go `SetSessionFromHook`: the alias takes the SAME stored value, with
        // its own validation skipped -- `tx_isolation` and
        // `transaction_isolation` are one value under two spellings.
        if let Some(other) = alias_of(&key) {
            self.systems.insert(other.to_owned(), value.clone());
        }
        self.systems.insert(key.clone(), value.clone());
        self.note_system_change(&key);
        if let Some(other) = alias_of(&key) {
            self.note_system_change(other);
        }
        self.generation += 1;
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
                    .gen_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
            )
        })?;
        let default = crate::sysvar::effective_default(def);
        if !def.has_global_scope() {
            return Ok(default);
        }
        if let Some(value) = def.get_global_from_hook() {
            return value;
        }
        if let Some(stored) = self.globals().overrides().get(def.name) {
            return Ok(stored.clone());
        }
        Ok(global_system_variable_initial_value(def))
    }

    pub fn reset_system(&mut self, name: &str) -> Result<(), VarError> {
        let def = get_sys_var(name).ok_or_else(|| {
            tidb_terror::MysqlError::from_terror(
                &crate::variable_errors::ERR_UNKNOWN_SYSTEM_VAR
                    .gen_with_stack_by_args(&[(name.to_ascii_lowercase()).into()]),
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
    Ok(tikv_client::oracle::compose_ts(instant.timestamp_millis(), 0))
}


/// Go `variable.GlobalSystemVariableInitialValue(sv.Name, sv.Value)`: the
/// value bootstrap seeds into `mysql.global_variables` and `SET x = DEFAULT`
/// resolves to.
///
/// `store_is_tikv` is a PROCESS fact in Go (`config.GetGlobalConfig().Store`)
/// that this tier is not told, so `tidb_enable_async_commit`/`tidb_enable_1pc`
/// resolve to their non-TiKV-store defaults here. `in_test` is Go's
/// `intest.InTest`: a test build bootstraps `tidb_mem_oom_action = LOG` and
/// `tidb_enable_auto_analyze = OFF`.
pub fn global_system_variable_initial_value(def: &SysVarDef) -> String {
    tidb_vardef::global_sysvar_initial::global_system_variable_initial_value(
        def.name,
        &crate::sysvar::effective_default(def),
        tidb_vardef::global_sysvar_initial::GlobalSysvarEnvironment {
            store_is_tikv: false,
            in_test: tidb_intest::IN_TEST,
            next_gen: false,
        },
    )
}

/// Go `SysVar.GetGlobalFromHook`: the variable's `GetGlobal` hook, else the
/// stored GLOBAL value.
pub fn global_get_hook(def: &SysVarDef, value: String) -> Result<String, VarError> {
    def.get_global_from_hook().unwrap_or(Ok(value))
}

/// Go `SessionVars.GetRelatedTableForMDL().Store(id, ...)`: records one stored
/// table id on the statement's metadata-lock map.
pub type MdlRelatedTableRecorder = std::sync::Arc<dyn Fn(i64) + Send + Sync>;

/// Go `SysVar.GetSession` bodies (`sysvar.go`): what each of these variables
/// answers from the session's state.
impl SessionVars {
    /// Go `timestamp`'s `GetSession`: an explicit non-default value reads
    /// back verbatim, else the statement's time.
    pub(crate) fn get_session_timestamp(&self) -> Result<String, VarError> {
        Ok(timestamp_hook_value(self.systems.get("timestamp").map(String::as_str)))
    }

    /// Go `warning_count`'s `GetSession`: `s.SysWarningCount`.
    pub(crate) fn get_session_warning_count(&self) -> Result<String, VarError> {
        Ok(self.sys_warning_count.to_string())
    }

    /// Go `error_count`'s `GetSession`: `s.SysErrorCount`.
    pub(crate) fn get_session_error_count(&self) -> Result<String, VarError> {
        Ok(self.sys_error_count.to_string())
    }

    /// Go `last_insert_id`'s and `identity`'s `GetSession`:
    /// `s.StmtCtx.PrevLastInsertID`.
    pub(crate) fn get_session_last_insert_id(&self) -> Result<String, VarError> {
        Ok(self.last_insert_id.to_string())
    }

    /// Go `tidb_current_ts`'s `GetSession`: `s.TxnCtx.StartTS`.
    pub(crate) fn get_session_tidb_current_ts(&self) -> Result<String, VarError> {
        Ok(self.current_tso.value().to_string())
    }

    /// Go `tidb_last_txn_info`'s `GetSession`: `s.LastTxnInfo`.
    pub(crate) fn get_session_tidb_last_txn_info(&self) -> Result<String, VarError> {
        Ok(self.last_txn_info_value())
    }

    /// Go `tidb_last_query_info`'s `GetSession`: `json.Marshal(s.LastQueryInfo)`.
    pub(crate) fn get_session_tidb_last_query_info(&self) -> Result<String, VarError> {
        Ok(self.last_query_info_value())
    }

    /// Go `last_plan_from_cache`'s `GetSession`: `s.PrevFoundInPlanCache`.
    pub(crate) fn get_session_last_plan_from_cache(&self) -> Result<String, VarError> {
        Ok(bool_to_on_off(self.prev_found_in_plan_cache).to_owned())
    }

    /// Go `last_plan_from_binding`'s `GetSession`: `s.PrevFoundInBinding`.
    pub(crate) fn get_session_last_plan_from_binding(&self) -> Result<String, VarError> {
        Ok(bool_to_on_off(self.prev_found_in_binding).to_owned())
    }

    /// Go `rand_seed1`'s `SetSession`: `s.Rng.SetSeed1(uint32(
    /// tidbOptPositiveInt32(val, 0)))` (validation has already clamped the
    /// value into `0..=MaxInt32`).
    pub(crate) fn set_session_rand_seed1(&mut self, value: &str) -> Result<(), VarError> {
        self.rng.set_seed1(value.parse::<u32>().unwrap_or(0));
        Ok(())
    }

    /// Go `rand_seed2`'s `SetSession`.
    pub(crate) fn set_session_rand_seed2(&mut self, value: &str) -> Result<(), VarError> {
        self.rng.set_seed2(value.parse::<u32>().unwrap_or(0));
        Ok(())
    }

    /// Go `rand_seed1`'s and `rand_seed2`'s `GetSession`: always `"0"`; the
    /// seed SET feeds the session's generator (`s.Rng`).
    pub(crate) fn get_session_rand_seed(&self) -> Result<String, VarError> {
        Ok("0".to_owned())
    }

    /// Go `tidb_last_ddl_info`'s `GetSession`: `json.Marshal(s.LastDDLInfo)`.
    pub(crate) fn get_session_tidb_last_ddl_info(&self) -> Result<String, VarError> {
        Ok(self.last_ddl_info_value())
    }

    /// Go `tidb_last_plan_replayer_token`'s `GetSession`:
    /// `s.LastPlanReplayerToken`.
    pub(crate) fn get_session_tidb_last_plan_replayer_token(&self) -> Result<String, VarError> {
        Ok(self.last_plan_replayer_token.clone())
    }

    /// Go `last_sql_use_alloc`'s `GetSession`: `s.preUseChunkAlloc`.
    pub(crate) fn get_session_last_sql_use_alloc(&self) -> Result<String, VarError> {
        Ok(bool_to_on_off(self.pre_use_chunk_alloc).to_owned())
    }

    /// Go `s.systems[name]`: the value stored for a session variable, which a
    /// `GetSession` hook may answer differently.
    #[must_use]
    pub fn stored_session_value(&self, name: &str) -> Option<&str> {
        self.systems.get(name).map(String::as_str)
    }
}

/// Go `BoolToOnOff`.
pub(crate) fn bool_to_on_off(value: bool) -> &'static str {
    if value {
        "ON"
    } else {
        "OFF"
    }
}

/// Go `SysVar.SetSession` bodies: each variable's typed session field, set
/// from the value about to be stored (`sysvar.go`).
impl SessionVars {
    /// Go's charset `SetSession`: the charset's default collation becomes
    /// the paired collation variable's value.
    fn pair_default_collation(&mut self, collation_var: &'static str, value: &str) {
        if let Ok(charset) = tidb_charset::get_charset_info(value) {
            self.systems.insert(collation_var.to_owned(), charset.default_collation.to_owned());
            self.note_system_change(collation_var);
        }
    }

    /// Go's collation `SetSession`: the collation's charset becomes the
    /// paired charset variable's value.
    fn pair_owning_charset(&mut self, charset_var: &'static str, value: &str) {
        if let Ok(collation) = tidb_charset::get_collation_by_name(value) {
            self.systems.insert(charset_var.to_owned(), collation.charset_name.to_owned());
            self.note_system_change(charset_var);
        }
    }

    /// Go `character_set_connection`'s `SetSession`.
    pub(crate) fn set_session_character_set_connection(&mut self, value: &str) -> Result<(), VarError> {
        self.pair_default_collation("collation_connection", value);
        Ok(())
    }

    /// Go `character_set_database`'s `SetSession`.
    pub(crate) fn set_session_character_set_database(&mut self, value: &str) -> Result<(), VarError> {
        self.pair_default_collation("collation_database", value);
        Ok(())
    }

    /// Go `character_set_server`'s `SetSession`.
    pub(crate) fn set_session_character_set_server(&mut self, value: &str) -> Result<(), VarError> {
        self.pair_default_collation("collation_server", value);
        Ok(())
    }

    /// Go `collation_connection`'s `SetSession`.
    pub(crate) fn set_session_collation_connection(&mut self, value: &str) -> Result<(), VarError> {
        self.pair_owning_charset("character_set_connection", value);
        Ok(())
    }

    /// Go `collation_database`'s `SetSession`.
    pub(crate) fn set_session_collation_database(&mut self, value: &str) -> Result<(), VarError> {
        self.pair_owning_charset("character_set_database", value);
        Ok(())
    }

    /// Go `collation_server`'s `SetSession`.
    pub(crate) fn set_session_collation_server(&mut self, value: &str) -> Result<(), VarError> {
        self.pair_owning_charset("character_set_server", value);
        Ok(())
    }

    /// Go `tx_isolation_one_shot`'s `SetSession`.
    pub(crate) fn set_session_tx_isolation_one_shot(&mut self, value: &str) -> Result<(), VarError> {
        self.txn_isolation_level_one_shot = (TxnIsolationLevelOneShotState::Set, value.to_owned());
        Ok(())
    }

    /// Go `tidb_snapshot`'s `SetSession`.
    pub(crate) fn set_session_tidb_snapshot(&mut self, value: &str) -> Result<(), VarError> {
        let ts = parse_read_timestamp(value, &self.time_zone, true)?;
        self.snapshot_ts = ts;
        if !value.is_empty() {
            self.set_txn_read_ts(0);
        }
        Ok(())
    }

    /// Go `tx_read_ts`'s `SetSession`.
    pub(crate) fn set_session_tx_read_ts(&mut self, value: &str) -> Result<(), VarError> {
        let ts = parse_read_timestamp(value, &self.time_zone, false)?;
        self.set_txn_read_ts(ts);
        if !value.is_empty() {
            self.snapshot_ts = 0;
        }
        Ok(())
    }

    /// Go `sql_mode`'s `SetSession`.
    pub(crate) fn set_session_sql_mode(&mut self, value: &str) -> Result<(), VarError> {
        self.sql_mode = tidb_mysql::get_sql_mode(value)
            .map_err(|error| tidb_terror::MysqlError::unknown(error.to_string()))?;
        Ok(())
    }

    /// Go `tidb_opt_fix_control`'s `SetSession`.
    pub(crate) fn set_session_tidb_opt_fix_control(&mut self, value: &str) -> Result<(), VarError> {
        self.optimizer_fix_control = OptimizerFixControl::parse(value)
            .map_err(|error| tidb_terror::MysqlError::unknown(error.to_string()))?
            .0;
        Ok(())
    }

    /// Go `tidb_dml_type`'s `SetSession`.
    pub(crate) fn set_session_tidb_dml_type(&mut self, value: &str) -> Result<(), VarError> {
        if !value.eq_ignore_ascii_case("standard") && !value.eq_ignore_ascii_case("bulk") {
            return Err(tidb_terror::MysqlError::unknown(format!("unsupport DML type: {value}")));
        }
        self.bulk_dml_enabled = value.eq_ignore_ascii_case("bulk");
        Ok(())
    }

    /// Go `autocommit`'s `SetSession`.
    pub(crate) fn set_session_autocommit(&mut self, value: &str) -> Result<(), VarError> {
        let is_autocommit = tidb_vardef::option_values::tidb_opt_on(value);
        // Implicitly commit the possible ongoing transaction if mode is
        // changed from off to on.
        if !self.autocommit && is_autocommit {
            self.set_in_txn(false);
        }
        self.autocommit = is_autocommit;
        Ok(())
    }

    /// Go `max_allowed_packet`'s `SetSession`.
    pub(crate) fn set_session_max_allowed_packet(&mut self, value: &str) -> Result<(), VarError> {
        self.max_allowed_packet = value.parse::<u64>().expect("max_allowed_packet validation stores unsigned decimal bytes");
        Ok(())
    }

    /// Go `max_allowed_packet`'s `GetSession`.
    pub(crate) fn get_session_max_allowed_packet(&self) -> Result<String, VarError> {
        Ok(self.max_allowed_packet.to_string())
    }

    /// Go `tidb_request_source_type`'s `SetSession`.
    pub(crate) fn set_session_tidb_request_source_type(&mut self, value: &str) -> Result<(), VarError> {
        value.clone_into(&mut self.explicit_request_source_type);
        Ok(())
    }

    /// Go `tidb_request_source_type`'s `GetSession`.
    pub(crate) fn get_session_tidb_request_source_type(&self) -> Result<String, VarError> {
        Ok(self.explicit_request_source_type.clone())
    }

    /// Go `tidb_scatter_region`'s `SetSession`.
    pub(crate) fn set_session_tidb_scatter_region(&mut self, value: &str) -> Result<(), VarError> {
        value.clone_into(&mut self.scatter_region);
        Ok(())
    }

    /// Go `tidb_scatter_region`'s `GetSession`.
    pub(crate) fn get_session_tidb_scatter_region(&self) -> Result<String, VarError> {
        Ok(self.scatter_region.clone())
    }

    /// Go `tidb_enable_plan_replayer_continuous_capture`'s `SetSession`.
    pub(crate) fn set_session_tidb_enable_plan_replayer_continuous_capture(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_plan_replayed_continues_capture = tidb_vardef::option_values::tidb_opt_on(value);
        Ok(())
    }

    /// Go `tidb_enable_plan_replayer_continuous_capture`'s `GetSession`.
    pub(crate) fn get_session_tidb_enable_plan_replayer_continuous_capture(&self) -> Result<String, VarError> {
        Ok(bool_to_on_off(self.enable_plan_replayed_continues_capture).to_owned())
    }

    /// Go `tidb_enable_plan_replayer_capture`'s `SetSession`.
    pub(crate) fn set_session_tidb_enable_plan_replayer_capture(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_plan_replayer_capture = tidb_vardef::option_values::tidb_opt_on(value);
        Ok(())
    }

    /// Go `tidb_enable_plan_replayer_capture`'s `GetSession`.
    pub(crate) fn get_session_tidb_enable_plan_replayer_capture(&self) -> Result<String, VarError> {
        Ok(bool_to_on_off(self.enable_plan_replayer_capture).to_owned())
    }

    /// Go `group_concat_max_len`'s `SetSession`.
    pub(crate) fn set_session_group_concat_max_len(&mut self, value: &str) -> Result<(), VarError> {
        self.group_concat_max_len = value
            .parse::<u64>()
            .expect("group_concat_max_len validation stores an unsigned decimal");
        Ok(())
    }

    /// Go `group_concat_max_len`'s `GetSession`.
    pub(crate) fn get_session_group_concat_max_len(&self) -> Result<String, VarError> {
        Ok(self.group_concat_max_len.to_string())
    }

    /// Go `tidb_partition_prune_mode`'s `GetSession`.
    pub(crate) fn get_session_tidb_partition_prune_mode(&self) -> Result<String, VarError> {
        Ok(self.partition_prune_mode.clone())
    }

    /// Go `tidb_partition_prune_mode`'s `SetSession` store.
    pub fn store_partition_prune_mode(&mut self, mode: String) {
        self.partition_prune_mode = mode;
    }

    /// Go `tidb_enable_inl_join_inner_multi_pattern`'s `SetSession`.
    pub(crate) fn set_session_tidb_enable_inl_join_inner_multi_pattern(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_inl_join_inner_multi_pattern = tidb_vardef::option_values::tidb_opt_on(value);
        Ok(())
    }

    /// Go `tidb_enable_inl_join_inner_multi_pattern`'s `GetSession`.
    pub(crate) fn get_session_tidb_enable_inl_join_inner_multi_pattern(&self) -> Result<String, VarError> {
        Ok(bool_to_on_off(self.enable_inl_join_inner_multi_pattern).to_owned())
    }

    /// Go `tiflash_replica_read`'s `SetSession`.
    pub(crate) fn set_session_tiflash_replica_read(&mut self, value: &str) -> Result<(), VarError> {
        self.tiflash_replica_read = tidb_tiflash::get_tiflash_replica_read_by_str(value);
        Ok(())
    }

    /// Go `tiflash_replica_read`'s `GetSession`.
    pub(crate) fn get_session_tiflash_replica_read(&self) -> Result<String, VarError> {
        Ok(tidb_tiflash::get_tiflash_replica_read(self.tiflash_replica_read).to_owned())
    }

    /// Go `tidb_session_alias`'s `SetSession`.
    pub(crate) fn set_session_tidb_session_alias(&mut self, value: &str) -> Result<(), VarError> {
        value.clone_into(&mut self.session_alias);
        Ok(())
    }

    /// Go `tidb_session_alias`'s `GetSession`.
    pub(crate) fn get_session_tidb_session_alias(&self) -> Result<String, VarError> {
        Ok(self.session_alias.clone())
    }

    /// Go `tidb_slow_log_rules`'s `SetSession`: `ParseSessionSlowLogRules`.
    pub(crate) fn set_session_tidb_slow_log_rules(&mut self, value: &str) -> Result<(), VarError> {
        self.slow_log_rules = crate::slow_log_rules::session_rules(value).map_err(VarError::unknown)?;
        Ok(())
    }

    /// Go `tidb_slow_log_rules`'s `GetSession`.
    pub(crate) fn get_session_tidb_slow_log_rules(&self) -> Result<String, VarError> {
        Ok(self.slow_log_rules.clone())
    }

    /// Go `tidb_max_keys_read`'s `SetSession`.
    pub(crate) fn set_session_tidb_max_keys_read(&mut self, value: &str) -> Result<(), VarError> {
        self.max_keys_read = value.parse::<u64>().expect("tidb_max_keys_read validation stores unsigned decimal keys");
        Ok(())
    }

    /// Go `tidb_dml_max_execution_time`'s `SetSession`.
    pub(crate) fn set_session_tidb_dml_max_execution_time(&mut self, value: &str) -> Result<(), VarError> {
        let timeout_ms = crate::varsutil::tidb_opt_positive_int32(value, tidb_vardef::defaults::DEF_TIDB_DML_MAX_EXECUTION_TIME);
        self.dml_max_execution_time = u64::try_from(timeout_ms).unwrap_or(0);
        Ok(())
    }

    /// Go `max_execution_time`'s `SetSession`.
    pub(crate) fn set_session_max_execution_time(&mut self, value: &str) -> Result<(), VarError> {
        self.max_execution_time = value.parse::<u64>().expect("max_execution_time validation stores unsigned decimal milliseconds");
        Ok(())
    }

    /// Go `time_zone`'s `SetSession`.
    pub(crate) fn set_session_time_zone(&mut self, value: &str) -> Result<(), VarError> {
        self.time_zone = resolve_session_time_zone_value(value);
        Ok(())
    }

    /// Go `sql_select_limit`'s `SetSession`.
    pub(crate) fn set_session_sql_select_limit(&mut self, value: &str) -> Result<(), VarError> {
        self.select_limit = value.parse::<u64>().expect("sql_select_limit validation stores unsigned decimal rows");
        Ok(())
    }

    /// Go `tidb_opt_selectivity_factor`'s `SetSession`.
    pub(crate) fn set_session_tidb_opt_selectivity_factor(&mut self, value: &str) -> Result<(), VarError> {
        self.selectivity_factor = value.parse::<f64>().expect("selectivity factor validation stores a decimal fraction");
        Ok(())
    }

    /// Go `tidb_multi_statement_mode`'s `SetSession`.
    pub(crate) fn set_session_tidb_multi_statement_mode(&mut self, value: &str) -> Result<(), VarError> {
        self.multi_statement_mode = match value {
            "ON" => 1,
            "WARN" => 2,
            _ => 0,
        };
        Ok(())
    }

    /// Go `tidb_enable_prepared_plan_cache`'s `SetSession`.
    pub(crate) fn set_session_tidb_enable_prepared_plan_cache(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_prepared_plan_cache = value == "ON";
        Ok(())
    }

    /// Go `tidb_enable_shared_lock_promotion`'s `SetSession`.
    pub(crate) fn set_session_tidb_enable_shared_lock_promotion(&mut self, value: &str) -> Result<(), VarError> {
        self.shared_lock_promotion = value == "ON";
        Ok(())
    }

    /// Go `tidb_enable_window_function`'s `SetSession`.
    pub(crate) fn set_session_tidb_enable_window_function(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_window_function = value == "ON";
        Ok(())
    }

    /// Go `tidb_mview_enable`'s `SetSession`.
    pub(crate) fn set_session_tidb_mview_enable(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_mview = value == "ON";
        Ok(())
    }

    /// Go `tidb_enable_full_outer_join`'s `SetSession`.
    pub(crate) fn set_session_tidb_enable_full_outer_join(&mut self, value: &str) -> Result<(), VarError> {
        self.enable_full_outer_join = value == "ON";
        Ok(())
    }

    /// Go `tidb_redact_log`'s `SetSession`.
    pub(crate) fn set_session_tidb_redact_log(&mut self, value: &str) -> Result<(), VarError> {
        value.clone_into(&mut self.enable_redact_log);
        Ok(())
    }

    /// Go `tidb_constraint_check_in_place_pessimistic`'s `SetSession`.
    pub(crate) fn set_session_tidb_constraint_check_in_place_pessimistic(&mut self, value: &str) -> Result<(), VarError> {
        // Go counts every lazy setting.
        if !tidb_vardef::option_values::tidb_opt_on(value) {
            tidb_metrics::session::LAZY_PESSIMISTIC_UNIQUE_CHECK_SET_COUNT.inc();
        }
        Ok(())
    }

    /// Go `tidb_query_cop_store_limit`'s `SetSession`.
    pub(crate) fn set_session_tidb_query_cop_store_limit(&mut self, value: &str) -> Result<(), VarError> {
        self.query_cop_store_limit = value.parse::<i64>().expect("query cop store limit validation stores an integer");
        Ok(())
    }

    /// Go `tidb_max_bytes_before_tiflash_external_join`'s `SetSession`.
    pub(crate) fn set_session_tidb_max_bytes_before_tiflash_external_join(&mut self, value: &str) -> Result<(), VarError> {
        self.ti_flash_max_bytes_before_ext_join = value.parse::<i64>().expect("TiFlash external join threshold validation stores signed bytes");
        Ok(())
    }

    /// Go `tidb_max_bytes_before_tiflash_external_group_by`'s `SetSession`.
    pub(crate) fn set_session_tidb_max_bytes_before_tiflash_external_group_by(&mut self, value: &str) -> Result<(), VarError> {
        self.ti_flash_max_bytes_before_ext_agg = value.parse::<i64>().expect("TiFlash external group-by threshold validation stores signed bytes");
        Ok(())
    }

    /// Go `tidb_max_bytes_before_tiflash_external_sort`'s `SetSession`.
    pub(crate) fn set_session_tidb_max_bytes_before_tiflash_external_sort(&mut self, value: &str) -> Result<(), VarError> {
        self.ti_flash_max_bytes_before_ext_sort = value.parse::<i64>().expect("TiFlash external sort threshold validation stores signed bytes");
        Ok(())
    }

    /// Go `tiflash_mem_quota_query_per_node`'s `SetSession`.
    /// Go `tidb_mem_quota_query`'s `SetSession`: `s.MemQuotaQuery =
    /// TidbOptInt64(val, DefTiDBMemQuotaQuery)`; the session tracker's limit
    /// (`s.MemTracker.SetBytesLimit`) is taken from it when each statement
    /// starts.
    pub(crate) fn set_session_tidb_opt_tiflash_concurrency_factor(
        &mut self,
        value: &str,
    ) -> Result<(), VarError> {
        self.cop_tiflash_concurrency_factor = crate::varsutil::tidb_opt_float64(
            value,
            tidb_vardef::defaults::DEF_OPT_TIFLASH_CONCURRENCY_FACTOR,
        );
        Ok(())
    }

    /// Go `TiDBSchemaCacheSize`'s `SetGlobal`: a size that differs from the
    /// live one goes through `ChangeSchemaCacheSize` (the domain's, here the
    /// node-registered function) before it is published.
    pub(crate) fn set_global_tidb_schema_cache_size(&mut self, value: &str) -> Result<(), VarError> {
        let bytes = crate::varsutil::parse_byte_size(value)
            .map(|(bytes, _)| bytes)
            .or_else(|| value.parse::<u64>().ok());
        if bytes != Some(tidb_vardef::SCHEMA_CACHE_SIZE.load(std::sync::atomic::Ordering::Acquire)) {
            if let Some(globals) = self.globals.clone() {
                globals.run_set_global_hook(tidb_vardef::tidb_vars::TIDB_SCHEMA_CACHE_SIZE, value)?;
            }
        }
        // Go: `vardef.SchemaCacheSize.Store(bt)`,
        // `vardef.SchemaCacheSizeOriginText.Store(str)`.
        tidb_vardef::SCHEMA_CACHE_SIZE.store(
            bytes.unwrap_or(tidb_vardef::defaults::DEF_TIDB_SCHEMA_CACHE_SIZE as u64),
            std::sync::atomic::Ordering::SeqCst,
        );
        *tidb_vardef::SCHEMA_CACHE_SIZE_ORIGIN_TEXT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = value.to_owned();
        Ok(())
    }

    /// Go `TiDBRedactLog`'s `SetGlobal`: `s.EnableRedactLog = val`.
    pub(crate) fn set_global_tidb_redact_log(&mut self, value: &str) -> Result<(), VarError> {
        self.set_enable_redact_log(value.to_owned());
        // NOTE: switch of errors is a singleton, thus we can not set it for
        // different sessions (Go `errors.RedactLogEnabled.Store(val)`).
        tidb_mysql::errors::set_redaction_mode(match value {
            "ON" => tidb_mysql::errors::RedactionMode::Enabled,
            "MARKER" => tidb_mysql::errors::RedactionMode::Marker,
            _ => tidb_mysql::errors::RedactionMode::Disabled,
        });
        Ok(())
    }

    pub(crate) fn set_session_tidb_mem_quota_query(&mut self, value: &str) -> Result<(), VarError> {
        self.mem_quota_query =
            crate::varsutil::tidb_opt_int64(value, tidb_vardef::defaults::DEF_TIDB_MEM_QUOTA_QUERY);
        Ok(())
    }

    pub(crate) fn set_session_tiflash_mem_quota_query_per_node(&mut self, value: &str) -> Result<(), VarError> {
        self.ti_flash_mem_quota_query_per_node = value.parse::<i64>().expect("TiFlash per-node quota validation stores signed bytes");
        Ok(())
    }

    /// Go `tiflash_query_spill_ratio`'s `SetSession`.
    pub(crate) fn set_session_tiflash_query_spill_ratio(&mut self, value: &str) -> Result<(), VarError> {
        self.ti_flash_query_spill_ratio = value.parse::<f64>().expect("TiFlash spill ratio validation stores a decimal fraction");
        Ok(())
    }

    /// Go `tidb_pessimistic_txn_fair_locking`'s `SetSession`.
    pub(crate) fn set_session_tidb_pessimistic_txn_fair_locking(&mut self, value: &str) -> Result<(), VarError> {
        self.pessimistic_transaction_fair_locking = value == "ON";
        Ok(())
    }

    /// Go `tidb_replica_read`'s `SetSession`.
    pub(crate) fn set_session_tidb_replica_read(&mut self, value: &str) -> Result<(), VarError> {
        self.replica_read = Self::replica_read_from_value(Some(value));
        Ok(())
    }

    /// Go `tidb_analyze_store_batch_size`'s `SetSession`.
    pub(crate) fn set_session_tidb_analyze_store_batch_size(&mut self, value: &str) -> Result<(), VarError> {
        self.analyze_store_batch_size = value.parse::<i64>().expect("analyze store batch size validation stores an integer");
        Ok(())
    }
}

