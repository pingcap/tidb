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

//! The session: the single entry point that owns catalog state and runs SQL
//! statements through the wired parse -> plan -> execute pipeline.
//!
//! This is the seam of Go's `pkg/session` `session.ExecuteStmt`: one object a
//! client holds, dispatching each statement kind to its executor path.
//!
//! SEED SCOPE: [`Session::run`] dispatches `SELECT` (rows), `INSERT` (affected
//! count), and `CREATE TABLE` over the session's [`Catalog`]. DEFERRED
//! (documented): transactions (autocommit is implicit and immediate --
//! `BEGIN`/`COMMIT`/`ROLLBACK` land with the txnkv integration), session
//! variables (`SET`), prepared statements, the MySQL wire protocol, privileges,
//! and every other statement kind. Statements are currently parsed twice (once
//! here for dispatch, once in the driver's runner) -- a wiring simplification
//! to remove when the driver's runners take parsed statements.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_ast::Stmt;
use tidb_datatype::{Datum, FieldType};
use tidb_executor::{Catalog, DriverError, MysqlRng};
use tidb_executor::{SchemaErrorKind, DEFAULT_DATABASE};
pub use tidb_planner::txn_mode::{
    txn_mode_for_begin, txn_mode_for_statement, SessionTxnMode, StatementTxnModeInputs,
    OPTIMISTIC_TXN_MODE, PESSIMISTIC_TXN_MODE,
};

/// Go `approxParseSQLTokenCnt`: estimates the token count used to reserve
/// parser memory with the global memory arbitrator.
///
/// This is intentionally not the SQL lexer. It preserves Go's cheap byte
/// scan, including its core-DML admission rule, comment skipping, ten-byte
/// keyword buffer, and treating quoted strings/identifiers as one token.
#[must_use]
pub fn approx_parse_sql_token_count(sql: &str) -> i64 {
    const CORE: u8 = 1;
    const BYPASS: u8 = 2;
    const SELECT: u8 = 4;

    fn key_token(keyword: &[u8]) -> u8 {
        match keyword {
            b"select" => SELECT,
            b"from" | b"insert" | b"update" | b"delete" | b"replace" => CORE,
            b"explain" | b"desc" | b"analyze" => BYPASS,
            _ => 0,
        }
    }

    let bytes = sql.as_bytes();
    let mut token_count = 0_i64;
    let mut in_word = false;
    let mut keyword = [0_u8; 10];
    let mut keyword_len = 0_usize;
    let mut hit_core_token = false;
    let mut has_select = false;
    let mut index = 0_usize;

    while index < bytes.len() {
        let original = bytes[index];
        let folded = original.to_ascii_lowercase();
        if folded.is_ascii_lowercase() || folded.is_ascii_digit() || folded == b'_' {
            in_word = true;
            if !hit_core_token && keyword_len < keyword.len() {
                keyword[keyword_len] = folded;
                keyword_len += 1;
            }
            index += 1;
            continue;
        }

        if in_word {
            in_word = false;
            token_count += 1;
            if !hit_core_token {
                let token = key_token(&keyword[..keyword_len]);
                if token & SELECT != 0 {
                    has_select = true;
                } else if token & CORE != 0 {
                    hit_core_token = true;
                } else if token & BYPASS == 0 && !has_select {
                    return 0;
                }
                keyword_len = 0;
            }
        }

        if original == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index += 2;
            while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/') {
                index += 1;
            }
            index = (index + 2).min(bytes.len());
            continue;
        }
        if original == b'-' && bytes.get(index + 1) == Some(&b'-') {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            index += usize::from(index < bytes.len());
            continue;
        }
        if original == b'#' {
            index += 1;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            index += usize::from(index < bytes.len());
            continue;
        }
        if original == b'\'' || original == b'"' {
            let quote = original;
            index += 1;
            while index < bytes.len() && bytes[index] != quote {
                if bytes[index] == b'\\' && index + 1 < bytes.len() {
                    index += 1;
                }
                index += 1;
            }
            index += usize::from(index < bytes.len());
            token_count += 1;
            continue;
        }
        if original == b'`' {
            index += 1;
            while index < bytes.len() && bytes[index] != b'`' {
                if bytes[index] == b'\\' && index + 1 < bytes.len() {
                    index += 1;
                }
                index += 1;
            }
            index += usize::from(index < bytes.len());
            token_count += 1;
            continue;
        }
        if original == b'?' {
            token_count += 1;
        }
        index += 1;
    }

    if in_word {
        token_count += 1;
    }
    if hit_core_token {
        token_count
    } else {
        0
    }
}

/// Go `approxCompilePlanTokenCnt`: estimates the token count of normalized
/// SQL used to reserve optimizer memory.
#[must_use]
pub fn approx_compile_plan_token_count(sql: &str, has_select: bool) -> i64 {
    const FROM: &str = "from";

    let mut token_count = 0_i64;
    let mut token_len = 0_usize;
    let mut has_select_from = false;
    for (index, character) in sql.char_indices() {
        if character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || matches!(character, '_' | '`' | '.')
        {
            token_len += character.len_utf8();
            continue;
        }
        if token_len > 0 {
            token_count += 1;
            if has_select
                && !has_select_from
                && token_len == FROM.len()
                && &sql[index - token_len..index] == FROM
            {
                has_select_from = true;
            }
            token_len = 0;
        }
        if character == '?' {
            token_count += 1;
        }
    }
    if token_len > 0 {
        token_count += 1;
    }
    if has_select && !has_select_from {
        0
    } else {
        token_count
    }
}

/// The result of running one statement.
#[derive(Debug, PartialEq)]
pub enum StmtResult {
    /// A query's result rows.
    Rows(Vec<Vec<Datum>>),
    /// A DML statement's affected-row count.
    Affected(u64),
    /// A DDL statement completed (`false` = `IF NOT EXISTS` no-op).
    Done(bool),
}

/// The result of running one statement, with wire-facing column metadata.
///
/// [`StmtResult::Rows`] loses column names/types; a server front end needs one
/// `(name, type)` per result column to build protocol column definitions, so
/// [`Session::run_with_columns`] returns this richer shape instead.
#[derive(Debug, PartialEq)]
pub enum StmtOutput {
    /// A query's result columns and rows.
    Rows {
        /// One `(display name, field type)` per output column.
        columns: Vec<(String, FieldType)>,
        /// The result rows (one `Datum` per column).
        rows: Vec<Vec<Datum>>,
    },
    /// A DML statement's affected-row count.
    Affected(u64),
    /// A DDL statement completed (`false` = `IF NOT EXISTS` no-op).
    Done(bool),
}

/// The statement-owned policy a server needs to retain an eager result set.
///
/// It is captured before `SET_VAR` overlays are restored, so a prepared
/// cursor uses the same quota, OOM action, temporary-storage decision, and
/// chunk bound as the statement that produced its rows.
#[derive(Clone)]
pub struct ResultMaterializationAuthority {
    memory: tidb_executor::StatementMemory,
    init_chunk_size: usize,
    max_chunk_size: usize,
}

impl ResultMaterializationAuthority {
    /// Builds a cursor-retention policy captured by a non-pipeline session.
    ///
    /// Production callers should pass the statement's actual memory policy
    /// and the statement's chunk-size bounds; this constructor exists so an
    /// external server-session implementation can support prepared cursors
    /// without a crate-private back door.
    #[must_use]
    pub fn new(
        memory: tidb_executor::StatementMemory,
        init_chunk_size: usize,
        max_chunk_size: usize,
    ) -> Self {
        Self {
            memory,
            init_chunk_size,
            max_chunk_size,
        }
    }

    /// Consumes the authority into its retained memory policy and chunk bounds.
    #[must_use]
    pub fn into_parts(self) -> (tidb_executor::StatementMemory, usize, usize) {
        (self.memory, self.init_chunk_size, self.max_chunk_size)
    }
}

/// A process-wide catalog shared by every session, as Go's domain-owned
/// `infoschema` is shared by every session of a TiDB instance.
pub type SharedCatalog = Arc<Mutex<Catalog>>;

/// Go `domainMap`: one domain-level catalog authority per storage UUID.
///
/// A server can use this registry when opening sessions for more than one
/// keyspace. Looking up `None` preserves the plugin-facing Go contract: reuse
/// any available domain, or return [`NoAvailableDomain`] when the process has
/// not opened one yet.
#[derive(Default)]
pub struct DomainMap {
    domains: Mutex<HashMap<String, SharedCatalog>>,
}

/// A nil-store lookup found no domain that could be reused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NoAvailableDomain;

impl std::fmt::Display for NoAvailableDomain {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("can not find available domain for a nil store")
    }
}

impl std::error::Error for NoAvailableDomain {}

impl DomainMap {
    /// Gets or creates the shared catalog for `store_uuid`.
    ///
    /// `None` never creates an entry. It returns an existing one when
    /// available, matching the enterprise-plugin compatibility path in Go.
    pub fn get(&self, store_uuid: Option<&str>) -> Result<SharedCatalog, NoAvailableDomain> {
        let mut domains = self
            .domains
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(store_uuid) = store_uuid else {
            return domains.values().next().cloned().ok_or(NoAvailableDomain);
        };
        Ok(Arc::clone(
            domains.entry(store_uuid.to_owned()).or_default(),
        ))
    }
}

/// A session: runs statements against a catalog shared with its peers.
///
/// Go sessions borrow the process's schema state rather than owning private
/// copies, so a table one connection creates is visible to the others. This
/// mirrors that with a shared, mutex-guarded catalog; the statement-level lock
/// stands in for Go's schema-version/lease machinery, which is a separate
/// tier (documented deferral).
pub struct Session {
    catalog: SharedCatalog,
    /// One connection-wide memory/disk tracker pair. Every statement gets a
    /// fresh child below these roots, so an open cursor remains counted when
    /// the client starts its next command.
    session_memory: tidb_executor::SessionMemory,
    /// The open transaction, if any.
    txn: Option<Transaction>,
    /// The session's system and user variables.
    vars: SessionVars,
    /// The warnings the last statement produced, which Go keeps in
    /// `StmtCtx.warnings` and `SHOW WARNINGS` reads.
    warnings: Vec<SqlWarning>,
    /// Go `StatementContext.InShowWarning`: set for exactly the statements
    /// that inherit the buffer, and the reason `WarningCount()` reports 0 for
    /// them. See [`Session::wire_warning_count`].
    in_show_warning: bool,
    /// Go `SessionVars.User` in its two spellings: the matched grant
    /// identity `CURRENT_USER()` reports and the login identity `USER()`
    /// reports. Empty until a front end authenticates one.
    current_user: Option<String>,
    login_user: Option<String>,
    /// Go `SessionVars.ActiveRoles`: the roles this session has activated,
    /// which every privilege check widens through and which `CURRENT_ROLE()`
    /// reports. A fresh session starts with its account's DEFAULT roles
    /// (Go activates them in `Auth`); `SET ROLE` replaces the set wholesale.
    active_roles: Vec<privilege::Account>,
    /// Go `SessionVars.ConnectionID`, which `CONNECTION_ID()` reports.
    /// `None` for a session with no connection identity, where the builtin
    /// answers NULL like `CURRENT_USER()` does for an unauthenticated one.
    connection_id: Option<u64>,
    /// Go `SessionVars.PrevLastInsertID`: the id `LAST_INSERT_ID()` reports,
    /// which only a statement that ALLOCATED an auto value updates.
    last_insert_id: u64,
    /// The id the last statement allocated, which the OK packet carries and
    /// which is 0 for a statement that allocated nothing.
    statement_insert_id: u64,
    /// Go `StmtCtx.AddSetVarHintRestore`: the session overrides a `SET_VAR`
    /// hint overwrote for the duration of ONE statement, put back when that
    /// statement finishes whether it succeeded or failed.
    set_var_hint_restore: Vec<(String, Option<String>)>,
    /// Go `StmtCtx.PrevAffectedRows`, which is all `ROW_COUNT()` reports: the
    /// preceding statement's affected rows, `-1` after a SELECT, `0`
    /// otherwise. Derived once at the statement boundary from
    /// [`Session::statement_kind`], so the function and the OK packet cannot
    /// disagree about what the statement did.
    prev_row_count: i64,
    /// Go `SessionVars.LastFoundRows`: the row count of the last result set
    /// drained to EOF. Non-result statements leave it unchanged.
    last_found_rows: u64,
    /// The class of the statement currently running, which decides what
    /// `ROW_COUNT()` reports next (Go's `StmtCtx.InSelectStmt` /
    /// `InInsertStmt` / `InUpdateStmt` / `InDeleteStmt` bits, read by
    /// `ResetContextOfStmt`). It is recorded even for a statement that ends
    /// in an error, because Go's bits survive the failure too.
    statement_kind: StatementKind,
    /// Go `StmtCtx.LastInsertID`/`LastInsertIDSet`: the id the RUNNING
    /// statement publishes. The session owns the cell and lends it to every
    /// [`tidb_executor::StmtContext`] the statement builds, so an allocating
    /// INSERT and `LAST_INSERT_ID(expr)` write one place, not two.
    published_last_insert_id: Rc<Cell<Option<u64>>>,
    /// Go `SessionVars.RetryInfo`'s auto-increment half: the ids the statement
    /// running now has assigned, kept across a write-conflict replay so the
    /// replay writes the ids the losing attempt picked. It lives on the
    /// session because the retry loop is above the statement -- each attempt
    /// builds its own `StmtContext`, and this is the one thing that has to
    /// cross between them. See `tidb_executor::RetryAutoIds`.
    retry_auto_ids: Rc<RefCell<tidb_executor::RetryAutoIds>>,
    /// Go `SessionVars.RowIDShardGenerator`: retains one random shard for
    /// `@@tidb_shard_allocate_step` generated IDs across statement contexts.
    row_id_shards: Rc<RefCell<tidb_executor::RowIdShardGenerator>>,
    /// The session's non-prepared plan cache
    /// (`tidb_enable_non_prepared_plan_cache`). See
    /// [`non_prepared_plan_cache`] for what it does and does not store.
    non_prepared_plan_cache: non_prepared_plan_cache::NonPreparedPlanCache,
    /// Go `SessionVars.FoundInPlanCache`: whether the statement RUNNING now
    /// found its plan in the cache. Reset for every statement.
    found_in_plan_cache: bool,
    /// Go `SessionVars.PrevFoundInPlanCache`, which is what
    /// `@@last_plan_from_cache` reads -- the PRECEDING statement's value,
    /// promoted at the statement boundary, since the reading `SELECT` is
    /// itself never cacheable and would otherwise always answer 0.
    prev_found_in_plan_cache: bool,
    /// Go `SessionVars.userVars`: this session's user variables, keyed
    /// lowercased, each holding a TYPED value (`SetUserVarVal` stores a
    /// `types.Datum`, which is why `SET @i = 5` and `SET @s = '5'` differ).
    ///
    /// The session owns the map and lends the handle to every statement
    /// context, because `@x := expr` writes it from INSIDE expression
    /// evaluation -- once per row, visible to the next select-list item.
    user_vars: Rc<RefCell<HashMap<String, Datum>>>,
    /// Go `SessionVars.SequenceState`: the last value THIS SESSION took from
    /// each sequence, keyed by lowercase `db.name`, which is what `LASTVAL`
    /// reports. It is SESSION state, not the sequence's stored counter -- a
    /// fresh session reads `NULL` from a sequence other sessions have advanced
    /// (captured: `lastval` before any `nextval` is `<nil>`).
    sequence_last_values: Rc<RefCell<HashMap<String, i64>>>,
    /// Go `SessionVars.CurrentDB`: the schema an unqualified name resolves in.
    /// Empty means no database is selected, which is Go's `ErrNoDB` case.
    current_db: String,
    /// This connection's registration in the server's process list, which the
    /// front end installs. `None` for a session with no server front; such a
    /// session still answers `SHOW PROCESSLIST` -- with the single row it can
    /// honestly report, itself.
    process: Option<process::ProcessGuard>,
    /// Whether this session holds the `PROCESS` privilege, which decides
    /// what `SHOW PROCESSLIST` and `information_schema.PROCESSLIST` let it
    /// see (Go `hasPriv(ctx, mysql.ProcessPriv)`).
    ///
    /// This is a direct test/front-end override. Normal SQL authorization
    /// reads the shared [`privilege::PrivilegeRegistry`] below, whose
    /// `GRANT PROCESS ON *.*` path is shared by every session.
    has_process_priv: bool,
    /// The server's account/global-privilege registry, shared by every
    /// session a front end opens (see [`privilege::PrivilegeRegistry`]).
    /// `None` for a session with no front end (unit tests, internal use),
    /// which is why every check through it falls back to the pre-existing
    /// bit above rather than treating an absent registry as "no privilege".
    privileges: Option<privilege::PrivilegeRegistry>,
    /// Go's process-wide `privileges.SkipWithGrant` admission copied onto
    /// this connection by the front end. The registry remains attached for
    /// account/role storage, while authorization readers treat the session
    /// as unrestricted.
    privilege_bypassed: bool,
    /// Whether this connection completed a TLS handshake (or an equivalent
    /// trusted gateway assertion). Go keeps the same fact in
    /// `SessionVars.TLSConnectionState`; `SET GLOBAL
    /// require_secure_transport=ON` needs it to avoid locking every current
    /// plaintext administrator out of the server.
    secure_transport: bool,
    /// Go `session.sandboxMode`: this connection logged in with an EXPIRED
    /// password while the server allowed it, so it may run nothing but the
    /// `SET PASSWORD` / `ALTER USER` that fixes the password. Set by the
    /// front end from the login's verdict, cleared by the statement that
    /// stores a new password.
    sandbox_mode: bool,
    /// Go `SessionVars.Rng`: the generator unseeded `RAND()` advances, shared
    /// across every statement of this session (unlike constant `RAND(N)`,
    /// which owns a fresh per-statement generator -- see `StmtContext`).
    rand: Rc<MysqlRng>,
    /// Go `SessionVars.PreparedStmtNameToID` / `PreparedStmts`: the SQL-level
    /// prepared statements this session holds. Per-session and not shared: a
    /// peer over the same catalog holds its own.
    prepared_statements: prepared_statements::PreparedStore,
    /// Go's `sessionBindingHandle` (`pkg/bindinfo/session_handle.go`): the
    /// SQL bindings created with `CREATE [SESSION] BINDING`. Session-scoped
    /// and unshared, exactly as Go's is; GLOBAL bindings would need
    /// `mysql.bind_info`, which this tier has no catalog entry for. See
    /// [`binding`].
    session_bindings: binding::SessionBindings,
    /// Go `SessionVars.FoundInBinding`: whether the statement RUNNING now
    /// took its hints from a binding.
    found_in_binding: bool,
    /// Go `SessionVars.PrevFoundInBinding`, which is what
    /// `@@last_plan_from_binding` reads -- the PRECEDING statement's value,
    /// promoted at the statement boundary for the same reason
    /// `@@last_plan_from_cache` is.
    prev_found_in_binding: bool,
}

impl Default for Session {
    /// A session on its own empty catalog, with `test` selected as a fresh
    /// TiDB connection has.
    ///
    /// This is the ONE place a [`Session`] is built. Everything a front end
    /// installs afterwards -- a shared catalog, an identity, a process
    /// registration, a privilege registry, a globals table -- arrives through a
    /// setter or through struct-update over this, so a field added to `Session`
    /// has exactly one place that must name it.
    fn default() -> Self {
        Session {
            catalog: SharedCatalog::default(),
            session_memory: tidb_executor::SessionMemory::new(
                tidb_util::memory::DEF_MEM_QUOTA_QUERY,
                tidb_executor::OomAction::Cancel,
                0,
            ),
            txn: None,
            vars: SessionVars::new(),
            warnings: Vec::new(),
            in_show_warning: false,
            current_user: None,
            login_user: None,
            active_roles: Vec::new(),
            connection_id: None,
            last_insert_id: 0,
            statement_insert_id: 0,
            set_var_hint_restore: Vec::new(),
            prev_row_count: 0,
            last_found_rows: 0,
            statement_kind: StatementKind::Other,
            published_last_insert_id: Rc::default(),
            retry_auto_ids: Rc::default(),
            row_id_shards: Rc::default(),
            non_prepared_plan_cache: non_prepared_plan_cache::NonPreparedPlanCache::default(),
            found_in_plan_cache: false,
            prev_found_in_plan_cache: false,
            user_vars: Rc::default(),
            sequence_last_values: Rc::default(),
            current_db: DEFAULT_DATABASE.to_owned(),
            process: None,
            has_process_priv: false,
            privileges: None,
            privilege_bypassed: false,
            secure_transport: false,
            sandbox_mode: false,
            rand: new_time_seeded_rand(),
            prepared_statements: prepared_statements::PreparedStore::default(),
            session_bindings: binding::SessionBindings::default(),
            found_in_binding: false,
            prev_found_in_binding: false,
        }
    }
}

/// Go `mathutil.NewWithTime()`: seeds a session's unseeded-`RAND()` generator
/// from the wall clock, which is what makes two sessions' `RAND()` sequences
/// differ without either being told to.
fn new_time_seeded_rand() -> Rc<MysqlRng> {
    Rc::new(MysqlRng::new_with_time())
}

pub use tidb_executor::TxnErrorKind;

mod account;
mod admin_check_arm;
mod analyze_arm;
mod binding;
mod binding_arm;
mod classify;
mod dispatch;
mod explain_arm;
mod identity;
pub mod infoschema;
mod non_prepared_plan_cache;
mod noop;
mod prepared_statements;
mod stmt_ctx;
mod table_privilege;
mod txn;
mod variables;
mod warnings;
pub(crate) use classify::{statement_kind_of, StatementKind};
pub use classify::{StmtKind, StoredStateChange};
pub(crate) use txn::Transaction;
pub(crate) use variables::datum_text;
pub use warnings::{SqlWarning, WarningLevel};
pub(crate) use warnings::{CHECK_CONSTRAINT_IS_OFF_CODE, CHECK_CONSTRAINT_IS_OFF_MESSAGE};
pub mod privilege;
pub mod process;
mod process_arm;
mod show;
pub mod sysvar;
pub mod vars;
pub use vars::{GlobalSysvars, SessionVars, VarError};

impl Session {
    /// A fresh session with its own empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Session::default()
    }

    /// Go `SessionVars.CurrentDB`. Empty when no database is selected.
    #[must_use]
    pub fn current_database(&self) -> &str {
        &self.current_db
    }

    /// Go `clientConn.useDB`: selects a schema outside the statement path.
    ///
    /// The connection front end reaches this for the handshake's initial
    /// database and for `COM_INIT_DB`, which Go both route through `useDB`.
    /// Taking the name directly instead of re-rendering `use \`name\`` keeps
    /// backquotes and other identifier syntax out of the picture entirely.
    /// The process-list row is refreshed for the same reason a statement
    /// refreshes it: a peer's `SHOW PROCESSLIST` reports the schema now
    /// selected.
    pub fn select_database(&mut self, name: &str) -> Result<(), DriverError> {
        self.use_database(name)?;
        if let Some(guard) = &self.process {
            guard
                .registry()
                .statement_finished(guard.id(), &self.current_db, &self.status_text());
        }
        Ok(())
    }

    /// Selects NO schema, which is the state a connection that authenticated
    /// without an initial database is in: Go's `SessionVars.CurrentDB` is
    /// empty and every unqualified name is `ErrNoDB` (`Error 1046`) until a
    /// `USE` runs.
    ///
    /// [`Session::default`] selects `test` because that is what a `mysql`
    /// client's own default gives a fresh connection; a front end whose
    /// handshake carried no schema at all -- or a harness replaying one --
    /// needs to say so, and this is the one way to.
    pub fn deselect_database(&mut self) {
        self.current_db = String::new();
    }

    /// Go `executeUse`: an unknown schema is `ErrDatabaseNotExists`, and the
    /// switch also updates `collation_database`.
    fn use_database(&mut self, name: &str) -> Result<(), DriverError> {
        // Go `executeUse` (`executor/simple.go` around line 608) refuses an
        // invisible schema with 1044 BEFORE it looks the schema up, so a
        // schema the account cannot see is never distinguishable from one
        // that does not exist.
        self.require_visible_database(name)?;
        let exists = self.with_catalog_mut(|catalog| Ok(catalog.has_database(name)))?;
        if !exists {
            return Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(
                name.to_owned(),
            )));
        }
        self.current_db = name.to_owned();
        Ok(())
    }

    /// The current database, or Go's `ErrNoDB` when none is selected.
    fn require_current_database(&self) -> Result<&str, DriverError> {
        if self.current_db.is_empty() {
            return Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected));
        }
        Ok(&self.current_db)
    }

    /// Go `LAST_INSERT_ID()`: the first id the most recent ALLOCATING
    /// statement handed out. A statement that allocated nothing -- an explicit
    /// auto value, a table with no auto column, an UPDATE -- leaves it as it
    /// was, which is what MySQL and TiDB both do.
    #[must_use]
    pub fn last_insert_id(&self) -> u64 {
        self.last_insert_id
    }

    /// The auto-increment ids the running statement has assigned, which a
    /// caller that RUNS THE STATEMENT AGAIN rewinds between attempts and
    /// clears when the statement is finally over. See
    /// `tidb_executor::RetryAutoIds`.
    #[must_use]
    pub fn retry_auto_ids(&self) -> &Rc<RefCell<tidb_executor::RetryAutoIds>> {
        &self.retry_auto_ids
    }

    /// The id the last statement allocated, which the OK packet reports and
    /// which is 0 when the statement allocated nothing.
    #[must_use]
    pub fn statement_insert_id(&self) -> u64 {
        self.statement_insert_id
    }

    /// The session's variables.
    #[must_use]
    pub fn vars(&self) -> &SessionVars {
        &self.vars
    }

    /// Installs the immutable build identity captured by the SQL server.
    pub fn set_version_info(&mut self, version_info: tidb_util::versioninfo::VersionInfo) {
        self.vars.set_version_info(version_info);
    }

    /// The live `@@wait_timeout` used by the MySQL connection before reading
    /// its next command packet.
    ///
    /// The registry validates this as an unsigned seconds value before it can
    /// enter the session, so parsing here cannot depend on client input shape.
    #[must_use]
    pub fn wait_timeout(&self) -> Duration {
        let seconds = self
            .vars
            .get_system("wait_timeout")
            .expect("wait_timeout is a registered session variable")
            .parse::<u64>()
            .expect("wait_timeout validation stores unsigned decimal seconds");
        Duration::from_secs(seconds)
    }

    /// A session sharing `catalog` with its peers.
    ///
    /// Every other field comes from [`Session::default`], which is the crate's
    /// one `Session` construction site -- see its doc.
    #[must_use]
    pub fn with_catalog(catalog: SharedCatalog) -> Self {
        Session {
            catalog,
            ..Session::default()
        }
    }

    /// Installs the server-owned spill policy for every statement created by
    /// this session.
    pub fn set_spill_storage(&mut self, storage: Arc<tidb_util::disk::SpillStorage>) {
        self.session_memory.set_spill_storage(storage);
    }

    /// Installs the server-owned process memory arbitrator for statements this
    /// session starts after the authority becomes available.
    pub fn set_mem_arbitrator(&mut self, arbitrator: Arc<tidb_util::memory::MemArbitrator>) {
        self.session_memory.set_mem_arbitrator(arbitrator);
    }

    /// The shared catalog handle, for opening a peer session over the same
    /// schema state.
    #[must_use]
    pub fn shared_catalog(&self) -> SharedCatalog {
        Arc::clone(&self.catalog)
    }

    /// The number of `?` markers a statement carries, which
    /// `COM_STMT_PREPARE` reports to the client.
    pub fn parameter_count(&self, sql: &str) -> Result<usize, DriverError> {
        tidb_executor::parameter_count(sql, self.scanner_sql_mode())
    }

    /// Runs one statement with its prepared-statement parameters bound.
    ///
    /// Go installs the execute-time values on the parsed statement's own
    /// markers; this tier reaches execution through SQL text, so the markers
    /// become literals and the statement is restored before it runs. A byte
    /// string that is not UTF-8 becomes a hex literal, so no value is lost in
    /// that round trip.
    pub fn run_with_params(
        &mut self,
        sql: &str,
        params: &[Datum],
    ) -> Result<StmtOutput, DriverError> {
        // The count is checked even when no values were sent, so a statement
        // with an unbound marker is Go's ErrWrongParamCount rather than a
        // parse-time surprise deeper in.
        if params.is_empty() && self.parameter_count(sql)? == 0 {
            return self.run_with_columns(sql);
        }
        let bound = tidb_executor::bind_parameters(sql, params, self.scanner_sql_mode())?;
        self.run_with_columns(&bound)
    }

    /// Runs one parameterized statement and returns the exact policy needed
    /// to retain a row result after the statement completes.
    ///
    /// The authority is present only for [`StmtOutput::Rows`]. It is captured
    /// inside the statement lifecycle before `SET_VAR` restoration; a server
    /// must not reconstruct it from post-statement session variables.
    pub fn run_with_params_and_result_authority(
        &mut self,
        sql: &str,
        params: &[Datum],
    ) -> Result<(StmtOutput, Option<ResultMaterializationAuthority>), DriverError> {
        if params.is_empty() && self.parameter_count(sql)? == 0 {
            return self.run_with_columns_internal(sql, true);
        }
        let bound = tidb_executor::bind_parameters(sql, params, self.scanner_sql_mode())?;
        self.run_with_columns_internal(&bound, true)
    }

    /// Runs one SQL statement (Go `session.ExecuteStmt`): parses, dispatches by
    /// statement kind, and executes over the session catalog.
    pub fn run(&mut self, sql: &str) -> Result<StmtResult, DriverError> {
        Ok(match self.run_with_columns(sql)? {
            StmtOutput::Rows { rows, .. } => StmtResult::Rows(rows),
            StmtOutput::Affected(count) => StmtResult::Affected(count),
            StmtOutput::Done(created) => StmtResult::Done(created),
        })
    }

    /// Like [`Session::run`], but a query result also carries its column
    /// metadata (`(name, type)` per column) for wire-protocol fronts.
    ///
    /// Captured from TiDB: a statement that fails leaves its own error in the
    /// warning buffer as an `Error`-level row, so `SHOW WARNINGS` right after
    /// a failure reports it.
    pub fn run_with_columns(&mut self, sql: &str) -> Result<StmtOutput, DriverError> {
        self.run_with_columns_internal(sql, false)
            .map(|(output, _)| output)
    }

    fn run_with_columns_internal(
        &mut self,
        sql: &str,
        capture_result_authority: bool,
    ) -> Result<(StmtOutput, Option<ResultMaterializationAuthority>), DriverError> {
        self.check_sandbox_mode(sql)?;
        // A statement is visible to a peer's SHOW PROCESSLIST for exactly as
        // long as it runs, which is why the process list is updated here --
        // the one door every statement of this session goes through -- rather
        // than in one front end's command loop.
        if let Some(guard) = &self.process {
            guard
                .registry()
                .statement_started(guard.id(), sql, &self.status_text());
        }
        // Go's `ResetContextOfStmt` promotes the PRECEDING statement's
        // publication into the `Prev*` fields the next statement reads, so
        // the promotion happens at the boundary, once, for every statement.
        self.statement_kind = StatementKind::Other;
        self.published_last_insert_id.set(None);
        // Go promotes `FoundInPlanCache` into `PrevFoundInPlanCache` in
        // `ResetContextOfStmt`, at the same boundary as the other `Prev*`
        // fields above -- which is why `select @@last_plan_from_cache`
        // reports the PRECEDING statement rather than itself.
        self.prev_found_in_plan_cache = std::mem::take(&mut self.found_in_plan_cache);
        // Go promotes `FoundInBinding` at the same boundary, which is why
        // `select @@last_plan_from_binding` reports the statement BEFORE it
        // rather than itself (that SELECT matches no binding of its own).
        self.prev_found_in_binding = std::mem::take(&mut self.found_in_binding);
        let result = self.execute_statement(sql);
        let result_authority =
            if capture_result_authority && matches!(&result, Ok(StmtOutput::Rows { .. })) {
                let init_chunk_size = self
                    .vars
                    .get_system(tidb_vardef::tidb_vars::TIDB_INIT_CHUNK_SIZE)
                    .ok()
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(32);
                let max_chunk_size = self
                    .vars
                    .get_system(tidb_vardef::tidb_vars::TIDB_MAX_CHUNK_SIZE)
                    .ok()
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(1024);
                Some(ResultMaterializationAuthority::new(
                    self.statement_context(false).statement_memory(),
                    init_chunk_size,
                    max_chunk_size,
                ))
            } else {
                None
            };
        // Go `ExecStmt` puts a `SET_VAR` hint's variables back when the
        // statement finishes, from the restore list the optimizer built --
        // which is why an overlay survives neither a successful statement nor
        // a failing one.
        let restore = std::mem::take(&mut self.set_var_hint_restore);
        self.vars.restore_system(restore);
        self.publish_statement_status(&result);
        if let Some(guard) = &self.process {
            guard
                .registry()
                .statement_finished(guard.id(), &self.current_db, &self.status_text());
        }
        if let Err(error) = &result {
            let reported = error.clone().to_mysql_error();
            self.append_warning(WarningLevel::Error, reported.code, reported.message);
        }
        result.map(|output| (output, result_authority))
    }
}

#[cfg(test)]
mod session_source_tests {
    use super::{
        approx_compile_plan_token_count, approx_parse_sql_token_count, DomainMap, NoAvailableDomain,
    };

    // Go pkg/session/tidb_test.go::TestDomapHandleNil.
    #[test]
    fn test_domap_handle_nil() {
        let domains = DomainMap::default();
        assert!(matches!(domains.get(None), Err(NoAvailableDomain)));

        let opened = domains.get(Some("store-a")).unwrap();
        let available = domains.get(None).unwrap();
        assert!(std::sync::Arc::ptr_eq(&opened, &available));
    }

    // Go pkg/session/session_test.go::TestMemArbitratorSession.
    #[test]
    fn test_mem_arbitrator_session() {
        assert_eq!(
            approx_parse_sql_token_count(
                "/*select * from **/SELECT x FROM `t\\`` # abc \nwhere a = 1.23 and b = 'abc\"d\\'e' -- abc \nand c_1_2 in \"abc'd\\\"e\" # (1,2,3)\n"
            ),
            15
        );
        assert_eq!(approx_parse_sql_token_count("select @@version @a"), 0);
        assert_eq!(approx_parse_sql_token_count("set @a=1"), 0);
        assert_eq!(approx_parse_sql_token_count("desc analyze table t"), 0);
        assert_eq!(approx_parse_sql_token_count("analyze table t"), 0);
        assert_eq!(
            approx_parse_sql_token_count("/*select * from **/explain show warnings"),
            0
        );
        assert_eq!(
            approx_parse_sql_token_count("/*select * from **/desc show columns from t"),
            0
        );
        assert_eq!(approx_parse_sql_token_count("insert into t values 1"), 5);
        assert_eq!(approx_parse_sql_token_count("update t set a=1"), 5);
        assert_eq!(approx_parse_sql_token_count("delete from t where a=1"), 6);
        assert_eq!(approx_parse_sql_token_count("replace into t values 1"), 5);
        assert_eq!(
            approx_parse_sql_token_count("prepare stmt1 from 'select * from t where a=? and b=?'"),
            0
        );
        assert_eq!(
            approx_parse_sql_token_count("execute stmt1 using @a,@b,@c"),
            0
        );
        let normalized = "select * from `a_1`.`b_2` where c1 = ? and c2 = ?";
        assert_eq!(approx_parse_sql_token_count(normalized), 10);
        assert_eq!(approx_compile_plan_token_count(normalized, true), 9);
        assert_eq!(
            approx_compile_plan_token_count("select @@version @a", true),
            0
        );
        assert_eq!(
            approx_compile_plan_token_count("select @@version @a", false),
            3
        );
    }
}

#[cfg(test)]
mod tests_admin_check;
#[cfg(test)]
mod tests_alter_column;
#[cfg(test)]
mod tests_analyze;
#[cfg(test)]
mod tests_auto_increment;
#[cfg(test)]
mod tests_auto_random;
#[cfg(test)]
mod tests_bad_null;
#[cfg(test)]
mod tests_binding;
#[cfg(test)]
mod tests_cast_int_truncation;
#[cfg(test)]
mod tests_charset;
#[cfg(test)]
mod tests_coalesced_joins;
#[cfg(test)]
mod tests_collation;
#[cfg(test)]
mod tests_column_defaults;
#[cfg(test)]
mod tests_column_prune;
#[cfg(test)]
mod tests_compare_refinement;
#[cfg(test)]
mod tests_core;
#[cfg(test)]
mod tests_datetime_year_compare;
#[cfg(test)]
mod tests_deadlock_history;
#[cfg(test)]
mod tests_derived_agg_pruning;
#[cfg(test)]
mod tests_dml_lock_keys;
#[cfg(test)]
mod tests_eval_bool;
#[cfg(test)]
mod tests_explain;
#[cfg(test)]
mod tests_explain_derived;
mod tests_explain_merge_join;
mod tests_expression_indexes;
#[cfg(test)]
mod tests_fix_control;
#[cfg(test)]
mod tests_foreign_key;
mod tests_generated_columns;
#[cfg(test)]
mod tests_global_vars;
#[cfg(test)]
mod tests_grants;
#[cfg(test)]
mod tests_harvested_relation_engine;
#[cfg(test)]
mod tests_in_list_full_evaluation;
#[cfg(test)]
mod tests_index_hints;
mod tests_index_key_length;
#[cfg(test)]
mod tests_join_predicate_placement;
#[cfg(test)]
mod tests_json;
#[cfg(test)]
mod tests_mem_quota;
mod tests_multi_table_dml;
#[cfg(test)]
mod tests_non_prepared_plan_cache;
#[cfg(test)]
mod tests_outer_join_elimination;
#[cfg(test)]
mod tests_partition;
#[cfg(test)]
mod tests_planner_core_rewriter;
#[cfg(test)]
mod tests_prepared_statements;
#[cfg(test)]
mod tests_read_cast;
#[cfg(test)]
mod tests_recursive_cte;
#[cfg(test)]
mod tests_savepoint;
mod tests_sequence;
#[cfg(test)]
mod tests_show;
#[cfg(test)]
mod tests_sql_mode_scanner;
#[cfg(test)]
mod tests_statement_rollback;
mod tests_subquery;
#[cfg(test)]
mod tests_support;
#[cfg(test)]
mod tests_sysbench_access;
#[cfg(test)]
mod tests_system_schemas;
#[cfg(test)]
mod tests_timestamp_range;
#[cfg(test)]
mod tests_timezone_storage;
#[cfg(test)]
mod tests_topn;
#[cfg(test)]
mod tests_union_scan;
#[cfg(test)]
mod tests_user_vars;
#[cfg(test)]
mod tests_views;
#[cfg(test)]
mod tests_window;
#[cfg(test)]
mod tests_write_conversion;
#[cfg(test)]
mod tests_zero_date;
