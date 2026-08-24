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
//! The registry itself is [`crate::sysvar`], which holds all 948 entries
//! captured from Go's own `GetSysVars()`, and the value validation Go's
//! `ValidateFromType` performs.
//!
//! GLOBAL scope: [`GlobalSysvars`] is a shared, `Arc`-backed table every
//! session a [`crate::Session`] factory opens holds a clone of -- Go's one
//! process-wide `GlobalVarAccessor` (kept in-memory here rather than
//! persisted to `mysql.GLOBAL_VARIABLES` in TiKV, so these globals reset on
//! process restart, exactly like [`crate::privilege::PrivilegeRegistry`] and
//! [`crate::process::ProcessRegistry`] do for accounts and connections).
//! `SET GLOBAL x = v` writes only into this shared table; the setting
//! session's own `@@x` is untouched (MySQL's rule: a live session keeps
//! whatever it already has). A brand new session snapshots the current
//! globals into its own session copy once, at connect
//! ([`SessionVars::seed_from_globals`]) -- so a session opened AFTER the
//! `SET GLOBAL` sees the new value as its session default, while sessions
//! already open do not.
//!
//! NOT MODELLED (documented): the per-variable `Validation` and
//! `SetSession` closures such as autocommit's implicit commit, and the
//! removed-variable list Go silently accepts.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tidb_planner::fix_control::OptimizerFixControl;
use tidb_util::versioninfo::VersionInfo;

use crate::sysvar::{
    alias_of, get_sys_var, SysVarDef, ValidationError, SCOPE_GLOBAL, SCOPE_INSTANCE, SCOPE_SESSION,
};

/// Names a validation refusal the way the variable error surface reports it:
/// Go's `ErrWrongTypeForVar` (1232), `ErrWrongValueForVar` (1231) and the
/// bare `errors.Errorf` a `Validation` closure may raise.
///
/// One function rather than one per call site, because the three writers
/// (SESSION, GLOBAL/INSTANCE, and the read-only refusal in
/// `Session::check_max_allowed_packet_scope`) must agree: the same rejected
/// value has to name the same error whichever tier was being written.
pub(crate) fn validation_var_error(name: &str, value: &str, error: ValidationError) -> VarError {
    match error {
        ValidationError::WrongType => VarError::WrongTypeForVar(name.to_ascii_lowercase()),
        ValidationError::WrongValue => {
            VarError::WrongValueForVar(name.to_ascii_lowercase(), value.to_owned())
        }
        ValidationError::WrongValueOf(part) => {
            VarError::WrongValueForVar(name.to_ascii_lowercase(), part)
        }
        ValidationError::Refused(message) => VarError::ValidationRefused(message),
    }
}

/// The shared GLOBAL-scope value table every session of one
/// [`crate::Session`] factory holds a clone of. In-memory only: Go persists
/// this tier to `mysql.GLOBAL_VARIABLES`, so a real cluster survives a
/// restart with its `SET GLOBAL` values and this one does not -- the same
/// documented gap the account/privilege/process registries carry.
#[derive(Clone, Debug)]
pub struct GlobalSysvars {
    values: Arc<Mutex<HashMap<String, String>>>,
    /// The INSTANCE tier: Go `vardef.ScopeInstance`. A per-node value, held
    /// beside the global one rather than in it because the two tiers differ
    /// in exactly one way that matters -- Go persists `ScopeGlobal` to
    /// `mysql.GLOBAL_VARIABLES` and holds `ScopeInstance` in this process's
    /// own memory (`SetInstanceSysVar` writes an `atomic` in `vardef`, never
    /// a row). Keeping them in one map would make [`Self::overrides`] offer
    /// a node-local value to the cluster writer as if it were cluster state.
    ///
    /// 28 entries have this scope alone; the six that carry
    /// `ScopeGlobal|ScopeInstance` live in `values`, matching Go's
    /// `setSysVariable`, which sends anything `IsGlobal` to
    /// `SetGlobalSysVar`.
    instances: Arc<Mutex<HashMap<String, String>>>,
    /// Ordered INSTANCE-only writes made against a cluster transaction's
    /// scratch table. Cluster rows never contain this tier, so the writer must
    /// replay these mutations into the live node only after the statement's
    /// durable GLOBAL half has committed.
    instance_mutations: Option<Arc<Mutex<Vec<InstanceMutation>>>>,
    /// Scratch registries validate cluster writes before commit. They must not
    /// publish process-wide runtime settings until [`Self::replace_from`]
    /// makes the committed state live.
    publishes_runtime_settings: bool,
}

#[derive(Clone, Debug)]
enum InstanceMutation {
    Set(String, String),
    Reset(String),
}

impl Default for GlobalSysvars {
    fn default() -> Self {
        Self {
            values: Arc::default(),
            instances: Arc::default(),
            instance_mutations: None,
            publishes_runtime_settings: true,
        }
    }
}

impl GlobalSysvars {
    /// A fresh registry with every variable at its default (Go's state on a
    /// cluster nobody has ever run `SET GLOBAL` against).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The tier a variable's node-wide value lives in. A variable that has
    /// GLOBAL scope at all is cluster state even when it ALSO has instance
    /// scope; only the instance-only ones are node-local.
    ///
    /// Every read and every write goes through here, so a value can never be
    /// written to one tier and looked for in the other -- the failure mode
    /// that makes a `SET` a silent no-op.
    fn store(&self, def: &'static SysVarDef) -> &Arc<Mutex<HashMap<String, String>>> {
        if def.has_global_scope() {
            &self.values
        } else {
            &self.instances
        }
    }

    /// Reads a node-wide value (GLOBAL or INSTANCE tier), falling back to the
    /// registry default.
    pub fn get(&self, name: &str) -> Result<String, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        let lowered = crate::sysvar::lowered_if_needed(name);
        Ok(self
            .store(def)
            .lock()
            .expect("global sysvar lock poisoned")
            .get(lowered.as_ref())
            .cloned()
            .unwrap_or_else(|| crate::sysvar::effective_default(def)))
    }

    /// Validates and writes a global value, visible to every session that
    /// reads `@@global.name` or opens AFTER this call.
    ///
    /// Answers whether Go's validation clamped the value, which the caller
    /// turns into `ErrTruncatedWrongValue` (1292) on the statement.
    pub fn set(&self, name: &str, value: String) -> Result<bool, VarError> {
        // Go `validateScope` (`variable.go:265`): `SET GLOBAL` is admitted by
        // `sv.HasGlobalScope() || sv.HasInstanceScope()`, so `SET GLOBAL
        // tidb_general_log = 1` is legal and lands in the instance tier.
        self.write(name, value, SCOPE_GLOBAL)
    }

    /// `SET INSTANCE name = value`, and the destination Go's legacy routing
    /// sends an unqualified `SET` on an instance-scoped variable to.
    ///
    /// Go `validateScope`: `scope == ScopeInstance && !sv.HasInstanceScope()`
    /// is `errLocalVariable` (1228), the same error a `SET GLOBAL` on a
    /// session-only variable gets.
    pub fn set_instance(&self, name: &str, value: String) -> Result<bool, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if !def.is_read_only() && !def.has_instance_scope() {
            return Err(VarError::SessionOnlyVariable(name.to_ascii_lowercase()));
        }
        self.write(name, value, SCOPE_INSTANCE)
    }

    /// Go `variable.SetSysVar`: the STARTUP write `setGlobalVars`
    /// (`cmd/tidb-server/main.go:1105`) pushes config-derived values with —
    /// it replaces the registry default directly, with no scope or read-only
    /// validation, which is how read-only NONE-scope variables like `port`,
    /// `socket` and `hostname` get their per-process values. Runtime `SET`
    /// statements never come through here.
    pub fn set_startup(&self, name: &str, value: String) {
        let Some(def) = get_sys_var(name) else {
            return;
        };
        self.store(def)
            .lock()
            .expect("global sysvar lock poisoned")
            .insert(name.to_ascii_lowercase(), value);
    }

    fn write(&self, name: &str, value: String, scope: u8) -> Result<bool, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.is_read_only() {
            return Err(VarError::ReadOnlyVariable(name.to_ascii_lowercase()));
        }
        if !def.has_global_scope() && !def.has_instance_scope() {
            return Err(VarError::SessionOnlyVariable(name.to_ascii_lowercase()));
        }
        let validated = def
            .validate_in_scope(&value, scope)
            .map_err(|error| validation_var_error(name, &value, error))?;
        let key = name.to_ascii_lowercase();
        if key == tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL {
            OptimizerFixControl::parse(&validated.value)
                .map_err(|error| VarError::ValidationRefused(error.to_string()))?;
        }
        let stored_value = validated.value;
        if self.publishes_runtime_settings && Self::is_memory_arbitration_setting(&key) {
            tidb_util::memory::validate_process_memory_setting(&key, &stored_value)
                .map_err(VarError::ValidationRefused)?;
        }
        {
            let mut values = self.store(def).lock().expect("global sysvar lock poisoned");
            if let Some(other) = alias_of(&key) {
                values.insert(other.to_owned(), stored_value.clone());
            }
            values.insert(key.clone(), stored_value.clone());
        }
        if key == tidb_vardef::tidb_vars::TIDB_REDACT_LOG {
            self.publish_redaction_mode();
        }
        if !def.has_global_scope() {
            self.record_instance_mutation(InstanceMutation::Set(key.clone(), stored_value));
        }
        if key == tidb_vardef::tidb_vars::TIDB_COMMITTER_CONCURRENCY {
            self.publish_committer_concurrency();
        }
        self.publish_memory_arbitration_setting(&key);
        Ok(validated.truncated)
    }

    /// Restores the registry default (`SET GLOBAL x = DEFAULT`).
    pub fn reset(&self, name: &str) -> Result<(), VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.is_read_only() {
            return Err(VarError::ReadOnlyVariable(name.to_ascii_lowercase()));
        }
        if !def.has_global_scope() && !def.has_instance_scope() {
            return Err(VarError::SessionOnlyVariable(name.to_ascii_lowercase()));
        }
        let key = name.to_ascii_lowercase();
        self.store(def)
            .lock()
            .expect("global sysvar lock poisoned")
            .remove(&key);
        if !def.has_global_scope() {
            self.record_instance_mutation(InstanceMutation::Reset(key.clone()));
        }
        if name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::TIDB_COMMITTER_CONCURRENCY) {
            self.publish_committer_concurrency();
        }
        if name.eq_ignore_ascii_case(tidb_vardef::tidb_vars::TIDB_REDACT_LOG) {
            self.publish_redaction_mode();
        }
        self.publish_memory_arbitration_setting(&key);
        Ok(())
    }

    /// Restores an INSTANCE-scoped value to its registry default, after the
    /// same scope validation as [`Self::set_instance`].
    pub fn reset_instance(&self, name: &str) -> Result<(), VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.is_read_only() {
            return Err(VarError::ReadOnlyVariable(name.to_ascii_lowercase()));
        }
        if !def.has_instance_scope() {
            return Err(VarError::SessionOnlyVariable(name.to_ascii_lowercase()));
        }
        let key = name.to_ascii_lowercase();
        self.store(def)
            .lock()
            .expect("global sysvar lock poisoned")
            .remove(&key);
        if !def.has_global_scope() {
            self.record_instance_mutation(InstanceMutation::Reset(key));
        }
        self.publish_memory_arbitration_setting(name);
        Ok(())
    }

    /// Overwrites this table's values from a cluster's stored
    /// `mysql.global_variables` rows, ahead of any session opening.
    ///
    /// Unlike [`Self::set`], this does not validate: a stored row already
    /// passed that validation when some earlier `SET GLOBAL` (Go's or this
    /// node's own) wrote it, so re-validating it here could only reject a
    /// value this node's own registry has since drifted from. A name this
    /// registry does not recognize is skipped rather than refused, the same
    /// forward/backward-compatibility stance `tidb_exec::cluster_privilege_load`
    /// takes on a column or privilege name the running version does not
    /// know.
    pub fn load_from_cluster<I: IntoIterator<Item = (String, String)>>(&self, rows: I) {
        let mut loaded_committer_concurrency = false;
        let mut loaded_redaction_mode = false;
        let mut loaded_memory_arbitration = false;
        for (name, value) in rows {
            let key = name.to_ascii_lowercase();
            if let Some(def) = get_sys_var(&key) {
                loaded_committer_concurrency |=
                    key == tidb_vardef::tidb_vars::TIDB_COMMITTER_CONCURRENCY;
                loaded_redaction_mode |= key == tidb_vardef::tidb_vars::TIDB_REDACT_LOG;
                loaded_memory_arbitration |= Self::is_memory_arbitration_setting(&key);
                self.store(def)
                    .lock()
                    .expect("global sysvar lock poisoned")
                    .insert(key, value);
            }
        }
        if loaded_committer_concurrency {
            self.publish_committer_concurrency();
        }
        if loaded_redaction_mode {
            self.publish_redaction_mode();
        }
        if loaded_memory_arbitration {
            self.publish_memory_arbitration_settings();
        }
    }

    /// Every variable this table currently overrides from its default, for
    /// [`SessionVars::seed_from_globals`] and `SHOW GLOBAL VARIABLES`.
    pub fn overrides(&self) -> HashMap<String, String> {
        self.values
            .lock()
            .expect("global sysvar lock poisoned")
            .clone()
    }

    /// A fresh table seeded from a cluster's stored
    /// `mysql.global_variables` rows -- the scratch copy a convergence node
    /// validates one `SET GLOBAL` against before persisting it, the same
    /// shape [`crate::privilege::PrivilegeRegistry`]'s cluster scratch table
    /// takes for an account statement.
    #[must_use]
    pub fn from_cluster_rows<I: IntoIterator<Item = (String, String)>>(rows: I) -> Self {
        let table = Self {
            instance_mutations: Some(Arc::default()),
            publishes_runtime_settings: false,
            ..Self::default()
        };
        table.load_from_cluster(rows);
        table
    }

    /// Publishes `fresh`'s GLOBAL values into every existing clone of this
    /// table while retaining this node's INSTANCE-only overrides.
    ///
    /// Mirrors [`crate::privilege::PrivilegeRegistry::replace_from`]: this
    /// `mysql.global_variables` never contains the process-local INSTANCE
    /// tier. A startup or periodic cluster rebuild must therefore replace the
    /// persisted GLOBAL image without clearing settings written through
    /// `SET INSTANCE` on this node.
    pub fn replace_from(&self, fresh: &Self) {
        *self.values.lock().expect("global sysvar lock poisoned") =
            std::mem::take(&mut *fresh.values.lock().expect("global sysvar lock poisoned"));
        self.publish_committer_concurrency();
        self.publish_redaction_mode();
        self.publish_memory_arbitration_settings();
    }

    /// Publishes only the named GLOBAL variables from `fresh`.
    ///
    /// The cluster writer uses this as its post-commit fallback when the
    /// durable whole-image reread fails. Applying only the statement's own
    /// changed names cannot erase a disjoint concurrent `SET GLOBAL`, while
    /// `set`/`reset` retain validation aliases and runtime-setting hooks.
    pub fn publish_global_changes_from(&self, fresh: &Self, changed: &[String]) {
        let desired = fresh.overrides();
        for name in changed {
            if get_sys_var(name).is_none() {
                // A newer peer may have stored a sysvar this binary does not
                // know. The cluster loader deliberately skips such rows; a
                // post-commit fallback must do the same instead of turning a
                // confirmed durable change into a process panic.
                continue;
            }
            match desired.get(name).cloned() {
                Some(value) => {
                    self.set(name, value)
                        .expect("the committed scratch table contains validated sysvars");
                }
                None => {
                    self.reset(name)
                        .expect("the committed plan contains registered sysvars");
                }
            }
        }
    }

    /// Replays the INSTANCE-only part of one committed cluster statement into
    /// this node's live table. Mutations retain source order so repeated
    /// assignments to one name have the same last-assignment-wins behavior as
    /// the statement that produced the scratch table.
    pub fn publish_instance_changes_from_if(
        &self,
        fresh: &Self,
        mut should_publish: impl FnMut(&str) -> bool,
    ) -> Vec<String> {
        let Some(mutations) = &fresh.instance_mutations else {
            return Vec::new();
        };
        let mutations = std::mem::take(
            &mut *mutations
                .lock()
                .expect("instance sysvar mutation lock poisoned"),
        );
        let mut published = Vec::new();
        for mutation in mutations {
            let name = match &mutation {
                InstanceMutation::Set(name, _) | InstanceMutation::Reset(name) => name,
            };
            if !should_publish(name) {
                continue;
            }
            match mutation {
                InstanceMutation::Set(name, value) => {
                    self.set_instance(&name, value)
                        .expect("the committed scratch table contains validated instance sysvars");
                    published.push(name);
                }
                InstanceMutation::Reset(name) => {
                    self.reset_instance(&name)
                        .expect("the committed scratch table contains registered instance sysvars");
                    published.push(name);
                }
            }
        }
        published
    }

    fn record_instance_mutation(&self, mutation: InstanceMutation) {
        if let Some(mutations) = &self.instance_mutations {
            mutations
                .lock()
                .expect("instance sysvar mutation lock poisoned")
                .push(mutation);
        }
    }

    fn publish_committer_concurrency(&self) {
        if !self.publishes_runtime_settings {
            return;
        }
        let value = self
            .values
            .lock()
            .expect("global sysvar lock poisoned")
            .get(tidb_vardef::tidb_vars::TIDB_COMMITTER_CONCURRENCY)
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(tidb_tikvutil::DEFAULT_COMMITTER_CONCURRENCY);
        tidb_tikvutil::set_committer_concurrency(value);
    }

    fn publish_redaction_mode(&self) {
        if !self.publishes_runtime_settings {
            return;
        }
        let value = self
            .values
            .lock()
            .expect("global sysvar lock poisoned")
            .get(tidb_vardef::tidb_vars::TIDB_REDACT_LOG)
            .cloned()
            .unwrap_or_else(|| {
                crate::sysvar::effective_default(
                    get_sys_var(tidb_vardef::tidb_vars::TIDB_REDACT_LOG)
                        .expect("tidb_redact_log is registered"),
                )
            });
        tidb_util::redact::set_redact_mode(&value);
    }

    fn is_memory_arbitration_setting(name: &str) -> bool {
        matches!(
            name,
            tidb_vardef::tidb_vars::TIDB_SERVER_MEMORY_LIMIT
                | tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_MODE
                | tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_SOFT_LIMIT
        )
    }

    fn publish_memory_arbitration_setting(&self, name: &str) {
        if self.publishes_runtime_settings && Self::is_memory_arbitration_setting(name) {
            if let Ok(value) = self.get(name) {
                let _ = tidb_util::memory::apply_process_memory_setting(name, &value);
            }
        }
    }

    fn publish_memory_arbitration_settings(&self) {
        for name in [
            tidb_vardef::tidb_vars::TIDB_SERVER_MEMORY_LIMIT,
            tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_MODE,
            tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_SOFT_LIMIT,
        ] {
            self.publish_memory_arbitration_setting(name);
        }
    }
}

/// Why a variable statement failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VarError {
    /// Go `ErrUnknownSystemVar` (1193).
    UnknownSystemVariable(String),
    /// Go `ErrIncorrectGlobalLocalVar` (1238): the variable is read-only.
    ReadOnlyVariable(String),
    /// Go `ErrWrongTypeForVar` (1232).
    WrongTypeForVar(String),
    /// Go `ErrWrongValueForVar` (1231).
    WrongValueForVar(String, String),
    /// Go `ErrLocalVariable` (1228): `SET GLOBAL` named a SESSION-only
    /// variable.
    SessionOnlyVariable(String),
    /// Go `ErrGlobalVariable` (1229): `SET SESSION` named a GLOBAL-only
    /// variable.
    GlobalOnlyVariable(String),
    /// Go `ErrIncorrectGlobalLocalVar` (1238), the read-side form: `SELECT
    /// @@global.x` named a SESSION-only variable (there is no GLOBAL copy to
    /// read).
    NoGlobalCopy(String),
    /// Go `ErrIncorrectScope` (1238), the expression rewriter's explicit read
    /// scope validation. The second field is Go's allowed-scope text, such as
    /// `GLOBAL` or `SESSION or GLOBAL`.
    IncorrectScope(String, &'static str),
    /// A `SysVar.Validation` closure's own `errors.Errorf`, whose wording is
    /// the whole error (Go gives it no code, so it reports as 1105).
    ValidationRefused(String),
}

/// A session's system-variable state: the variables it has overridden (Go's
/// `SessionVars.systems`) plus the shared GLOBAL table.
///
/// User variables are NOT here. They live in one place -- the shared map
/// [`crate::Session`] lends to every statement context -- because `@x := expr`
/// writes them from inside expression evaluation, mid-row; see
/// `tidb_executor::StmtContext`'s `user_vars` field.
#[derive(Clone, Debug, Default)]
pub struct SessionVars {
    systems: HashMap<String, String>,
    /// Bumped by every mutation of `systems`, so a caller can cache what it
    /// PARSES out of the raw text -- the scanner's `sql_mode` bits, the
    /// optimizer's cost environment -- and re-derive only when a `SET`
    /// actually happened. Go holds the same products as typed fields on
    /// `SessionVars` updated by each variable's `SetSession` hook; a
    /// generation stamp buys that read cost without a hook per variable.
    /// Session-scoped reads never consult the shared globals
    /// (`get_system`'s fallback is the static default), so this counter
    /// alone is a complete invalidation key for them.
    generation: u64,
    /// Parsed authority kept in lockstep with the raw system-variable text.
    optimizer_fix_control: OptimizerFixControl,
    /// The shared GLOBAL-scope table this session's factory holds. Cloning a
    /// [`GlobalSysvars`] is cheap (one `Arc` bump), so every session shares
    /// the same underlying map.
    globals: GlobalSysvars,
    /// Immutable server identity captured when this connection opened.
    version_info: VersionInfo,
}

impl SessionVars {
    /// A session with every variable at its registry default and its own,
    /// unshared global table (used by tests and any standalone session that
    /// has no factory to share one with).
    #[must_use]
    pub fn new() -> Self {
        SessionVars::default()
    }

    /// Points this session at a shared [`GlobalSysvars`] table and snapshots
    /// its current overrides into the session's own copy -- Go's rule that a
    /// session's variables are copied from the global tier once, at connect.
    /// A `SET GLOBAL` another session runs AFTER this call is invisible to
    /// this session's plain `@@x` (only to `@@global.x`), exactly as MySQL
    /// documents.
    pub fn seed_from_globals(&mut self, globals: GlobalSysvars) -> Result<(), VarError> {
        let mut systems = self.systems.clone();
        for (name, value) in globals.overrides() {
            // Only a variable this session can actually hold a session copy
            // of inherits the global value; Go's `NewSessionVars` walks the
            // same `HasSessionScope` guard when copying `GlobalVarsAccessor`
            // into a fresh session.
            if get_sys_var(&name).is_some_and(|def| def.has_session_scope()) {
                systems.insert(name, value);
            }
        }
        let raw = systems
            .get(tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL)
            .cloned()
            .unwrap_or_else(|| {
                get_sys_var(tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL)
                    .map_or_else(String::new, |def| def.value.to_owned())
            });
        let optimizer_fix_control = OptimizerFixControl::parse(&raw)
            .map_err(|error| VarError::ValidationRefused(error.to_string()))?
            .0;
        // Commit all three authorities only after the inherited fix-control
        // row has been accepted. A stale/foreign cluster row can therefore
        // refuse the connection without partially reseeding this session.
        self.systems = systems;
        self.globals = globals;
        self.optimizer_fix_control = optimizer_fix_control;
        // The wholesale replacement above is a mutation like any other; the
        // parsed-product caches keyed on `generation` must not survive it.
        self.generation += 1;
        Ok(())
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

    pub fn get_system(&self, name: &str) -> Result<String, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.name == "version_comment" {
            return Ok(self.version_info.version_comment());
        }
        if def.name == "version" {
            return Ok(self.version_info.server_version.clone());
        }
        // An INSTANCE-scoped variable has no session copy either, and its
        // node-wide value is the only one there is: without this arm a
        // `SET GLOBAL tidb_general_log = 1` would store a value that
        // `SELECT @@tidb_general_log` never consults. A NONE-scope variable
        // (`port`, `socket`) reads the same node tier, which is where the
        // startup `set_global_vars` push (Go `variable.SetSysVar`) lives.
        if !def.has_session_scope() {
            return self.globals.get(name);
        }
        let lowered = crate::sysvar::lowered_if_needed(name);
        Ok(self
            .systems
            .get(lowered.as_ref())
            .cloned()
            .unwrap_or_else(|| crate::sysvar::effective_default(def)))
    }

    /// Installs the immutable build identity supplied by the server startup.
    pub fn set_version_info(&mut self, version_info: VersionInfo) {
        self.version_info = version_info;
    }

    /// The immutable build/config identity returned by `TIDB_VERSION()`.
    #[must_use]
    pub(crate) fn tidb_info(&self) -> String {
        tidb_util::printer::get_tidb_info(&self.version_info)
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
        for (key, previous) in snapshot {
            match previous {
                Some(value) => self.systems.insert(key, value),
                None => self.systems.remove(&key),
            };
        }
        self.generation += 1;
        self.refresh_optimizer_fix_control();
    }

    /// Reads `@@global.name`: always the shared table's live value, never
    /// this session's own copy. Go's `ErrIncorrectGlobalLocalVar` (1238) when
    /// the variable has no GLOBAL scope at all to read.
    pub fn get_global(&self, name: &str) -> Result<String, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        // Go's read path for `@@global.x` does not run `validateScope`; an
        // instance-scoped variable answers `SELECT @@global.max_connections`,
        // which some drivers ask for at connect.
        if !def.has_global_scope() && !def.has_instance_scope() {
            // A NONE-scope variable (`port`, `socket`) has exactly ONE
            // value, the node's own, and Go answers it for `@@global.x` and
            // `SHOW GLOBAL VARIABLES` alike (`GetScopeNoneSystemVar`). Only
            // a SESSION-only variable has no global copy to read (Go's
            // ErrIncorrectGlobalLocalVar).
            if def.has_session_scope() {
                return Err(VarError::NoGlobalCopy(name.to_ascii_lowercase()));
            }
            return self.globals.get(name);
        }
        self.globals.get(name)
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
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.is_read_only() {
            return Err(VarError::ReadOnlyVariable(name.to_ascii_lowercase()));
        }
        if !def.has_session_scope() {
            return Err(VarError::GlobalOnlyVariable(name.to_ascii_lowercase()));
        }
        let validated = def
            .validate_in_scope(&value, SCOPE_SESSION)
            .map_err(|error| validation_var_error(name, &value, error))?;
        let key = name.to_ascii_lowercase();
        let parsed_fix_control = if key == tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL {
            Some(
                OptimizerFixControl::parse(&validated.value)
                    .map_err(|error| VarError::ValidationRefused(error.to_string()))?
                    .0,
            )
        } else {
            None
        };
        // Go `SetSessionFromHook`: the alias takes the SAME stored value, with
        // its own validation skipped -- `tx_isolation` and
        // `transaction_isolation` are one value under two spellings.
        if let Some(other) = alias_of(&key) {
            self.systems
                .insert(other.to_owned(), validated.value.clone());
        }
        self.systems.insert(key, validated.value);
        self.generation += 1;
        if let Some(parsed) = parsed_fix_control {
            self.optimizer_fix_control = parsed;
        }
        Ok(validated.truncated)
    }

    /// `SET GLOBAL name = value`: writes only the shared table, never this
    /// session's own `@@name`. Go's `ErrLocalVariable` (1228) when the
    /// variable is SESSION-only, so there is no global copy to set.
    pub fn set_global(&mut self, name: &str, value: String) -> Result<bool, VarError> {
        self.globals.set(name, value)
    }

    /// `SET INSTANCE name = value`: writes the node-local tier, never this
    /// session's own copy (an instance-scoped variable has none).
    pub fn set_instance(&mut self, name: &str, value: String) -> Result<bool, VarError> {
        self.globals.set_instance(name, value)
    }

    /// Points this session at a different shared GLOBAL table for one
    /// statement, answering the one it was using -- the `GlobalSysvars` twin
    /// of [`crate::privilege::PrivilegeRegistry`]'s `swap_privileges`. A
    /// convergence node validates one `SET GLOBAL` against a scratch table
    /// read from the cluster before persisting it, and must be able to put
    /// the live table back unconditionally if the statement fails.
    pub fn swap_globals(&mut self, globals: GlobalSysvars) -> GlobalSysvars {
        std::mem::replace(&mut self.globals, globals)
    }

    /// `SET GLOBAL name = DEFAULT`.
    pub fn reset_global(&mut self, name: &str) -> Result<(), VarError> {
        self.globals.reset(name)
    }

    /// `SET INSTANCE name = DEFAULT`.
    pub fn reset_instance(&mut self, name: &str) -> Result<(), VarError> {
        self.globals.reset_instance(name)
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
    ///
    /// The environment is the stock one. `store_is_tikv` is a PROCESS fact in
    /// Go (`config.GetGlobalConfig().Store`) that this tier is not told, so
    /// `tidb_enable_async_commit`/`tidb_enable_1pc` resolve to their
    /// non-TiKV-store defaults here; every other override is unconditional.
    pub fn reset_system(&mut self, name: &str) -> Result<(), VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        let default = crate::sysvar::effective_default(def);
        let value = tidb_vardef::global_sysvar_initial::global_system_variable_initial_value(
            &name.to_ascii_lowercase(),
            &default,
            tidb_vardef::global_sysvar_initial::GlobalSysvarEnvironment {
                store_is_tikv: false,
                in_test: false,
                next_gen: false,
            },
        );
        self.set_system(name, value).map(|_truncated| ())
    }

    /// Go `SET NAMES <charset>`: sets the three client character-set
    /// variables together, plus the connection collation.
    pub fn set_names(&mut self, charset: &str, collation: Option<&str>) -> Result<(), VarError> {
        for name in [
            "character_set_client",
            "character_set_connection",
            "character_set_results",
        ] {
            self.set_system(name, charset.to_owned())?;
        }
        if let Some(collation) = collation {
            self.set_system("collation_connection", collation.to_owned())?;
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_come_from_the_registry() {
        let vars = SessionVars::new();
        assert_eq!(vars.get_system("autocommit").unwrap(), "ON");
        assert_eq!(vars.get_system("character_set_client").unwrap(), "utf8mb4");
        assert_eq!(
            vars.get_system("transaction_isolation").unwrap(),
            "REPEATABLE-READ"
        );
        assert_eq!(vars.get_system("max_allowed_packet").unwrap(), "67108864");
        // The lookup is case-insensitive, as Go's is after lowercasing.
        assert_eq!(vars.get_system("AUTOCOMMIT").unwrap(), "ON");
    }

    #[test]
    fn sem_enable_and_disable_change_new_session_defaults() {
        struct DisableSemOnDrop;

        impl Drop for DisableSemOnDrop {
            fn drop(&mut self) {
                tidb_util::sem::disable();
            }
        }

        tidb_util::sem::disable();
        let _reset = DisableSemOnDrop;

        tidb_util::sem::enable();
        let enabled = SessionVars::new();
        assert_eq!(
            enabled.get_system("tidb_enable_enhanced_security").unwrap(),
            "ON"
        );
        assert_eq!(enabled.get_system("hostname").unwrap(), "localhost");

        let mut recovery = crate::Session::new();
        recovery.set_user("recovery@%".to_owned(), "recovery@127.0.0.1".to_owned());
        recovery.attach_privileges(crate::privilege::PrivilegeRegistry::default());
        assert!(recovery.sem_hides_status_var("tidb_gc_leader_desc"));
        recovery.enable_privilege_bypass();
        assert!(
            !recovery.sem_hides_status_var("tidb_gc_leader_desc"),
            "skip-grant-table satisfies RESTRICTED_STATUS_ADMIN verification",
        );

        tidb_util::sem::disable();
        let disabled = SessionVars::new();
        assert_eq!(
            disabled
                .get_system("tidb_enable_enhanced_security")
                .unwrap(),
            "OFF"
        );
    }

    #[test]
    fn an_unknown_system_variable_is_rejected_both_ways() {
        let mut vars = SessionVars::new();
        assert_eq!(
            vars.get_system("nope"),
            Err(VarError::UnknownSystemVariable("nope".to_owned()))
        );
        assert_eq!(
            vars.set_system("nope", "1".to_owned()),
            Err(VarError::UnknownSystemVariable("nope".to_owned()))
        );
    }

    #[test]
    fn a_loaded_row_overrides_and_an_absent_one_still_falls_back_to_default() {
        let globals = GlobalSysvars::new();
        globals.load_from_cluster([("AUTOCOMMIT".to_owned(), "OFF".to_owned())]);
        // The seeded row overrides, case-insensitively, exactly like `set`.
        assert_eq!(globals.get("autocommit").unwrap(), "OFF");
        // A variable the fixture never mentioned still falls back to the
        // registry default -- `load_from_cluster` only overwrites names it is
        // given.
        assert_eq!(globals.get("max_allowed_packet").unwrap(), "67108864");
    }

    #[test]
    fn a_loaded_row_naming_an_unknown_variable_is_skipped_not_refused() {
        let globals = GlobalSysvars::new();
        // Bypasses `set`'s validation on purpose (a stored row already passed
        // it once), but an unrecognized name from an older/newer cluster must
        // still not panic or poison every other loaded row.
        globals.load_from_cluster([
            ("not_a_real_variable".to_owned(), "x".to_owned()),
            ("autocommit".to_owned(), "OFF".to_owned()),
        ]);
        assert_eq!(
            globals.get("not_a_real_variable"),
            Err(VarError::UnknownSystemVariable(
                "not_a_real_variable".to_owned()
            ))
        );
        assert_eq!(globals.get("autocommit").unwrap(), "OFF");
    }

    #[test]
    fn a_read_only_variable_cannot_be_set() {
        let mut vars = SessionVars::new();
        assert!(vars.get_system("version").unwrap().starts_with("8.0.11-"));
        assert_eq!(
            vars.set_system("version", "9".to_owned()),
            Err(VarError::ReadOnlyVariable("version".to_owned()))
        );
    }

    #[test]
    fn set_overrides_the_default_and_set_names_moves_three_variables() {
        let mut vars = SessionVars::new();
        vars.set_system("autocommit", "OFF".to_owned()).unwrap();
        assert_eq!(vars.get_system("autocommit").unwrap(), "OFF");

        vars.set_names("latin1", Some("latin1_bin")).unwrap();
        for name in [
            "character_set_client",
            "character_set_connection",
            "character_set_results",
        ] {
            assert_eq!(vars.get_system(name).unwrap(), "latin1", "{name}");
        }
        assert_eq!(
            vars.get_system("collation_connection").unwrap(),
            "latin1_bin"
        );
        // SET NAMES leaves the server-side character set alone.
        assert_eq!(vars.get_system("character_set_server").unwrap(), "utf8mb4");
    }

    #[test]
    fn set_global_writes_the_shared_table_not_this_sessions_own_copy() {
        let mut vars = SessionVars::new();
        // `autocommit` (scope 3: both SESSION and GLOBAL) is the case Go
        // documents: `SET GLOBAL` never moves the setting session's own
        // `@@autocommit`.
        vars.set_global("autocommit", "OFF".to_owned()).unwrap();
        assert_eq!(vars.get_system("autocommit").unwrap(), "ON");
        assert_eq!(vars.get_global("autocommit").unwrap(), "OFF");
    }

    #[test]
    fn set_global_redact_log_updates_the_process_redaction_authority() {
        let mut vars = SessionVars::new();
        vars.set_global("tidb_redact_log", "MARKER".to_owned())
            .unwrap();
        assert!(tidb_util::redact::need_redact());
        assert_eq!(tidb_util::redact::value("secret"), "?");

        vars.globals.reset("tidb_redact_log").unwrap();
        assert!(!tidb_util::redact::need_redact());

        let scratch =
            GlobalSysvars::from_cluster_rows([("tidb_redact_log".to_owned(), "MARKER".to_owned())]);
        assert!(!tidb_util::redact::need_redact());
        vars.globals.replace_from(&scratch);
        assert!(tidb_util::redact::need_redact());

        vars.set_global("tidb_redact_log", "OFF".to_owned())
            .unwrap();
        assert!(!tidb_util::redact::need_redact());

        vars.globals
            .load_from_cluster([("tidb_redact_log".to_owned(), "ON".to_owned())]);
        assert!(tidb_util::redact::need_redact());
        vars.globals.reset("tidb_redact_log").unwrap();
    }

    #[test]
    fn global_memory_arbitration_settings_update_the_running_process_authority() {
        struct NoopRecorder;

        impl tidb_util::memory::RecordMemState for NoopRecorder {
            fn load(&self) -> Result<Option<tidb_util::memory::RuntimeMemStateV1>, String> {
                Ok(None)
            }

            fn store(&self, _: &tidb_util::memory::RuntimeMemStateV1) -> Result<(), String> {
                Ok(())
            }
        }

        let arbitrator =
            tidb_util::memory::MemArbitrator::new(1 << 30, 4, 4, 64 << 10, Box::new(NoopRecorder));
        let _registration = tidb_util::memory::install_process_arbitrator(&arbitrator);
        let globals = GlobalSysvars::new();

        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_MODE,
                "priority".to_owned(),
            )
            .unwrap();
        assert_eq!(
            arbitrator.work_mode(),
            tidb_util::memory::ArbitratorWorkMode::Priority
        );

        globals
            .set(
                tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_SOFT_LIMIT,
                "0.5".to_owned(),
            )
            .unwrap();
        assert_eq!(arbitrator.soft_limit(), 512 << 20);

        globals
            .set_instance(
                tidb_vardef::tidb_vars::TIDB_SERVER_MEMORY_LIMIT,
                "2GiB".to_owned(),
            )
            .unwrap();
        assert_eq!(arbitrator.limit_u64(), 2 << 30);

        globals
            .reset(tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_MODE)
            .unwrap();
        assert_eq!(
            arbitrator.work_mode(),
            tidb_util::memory::ArbitratorWorkMode::Disable
        );
    }

    #[test]
    fn set_global_on_a_session_only_variable_is_1228() {
        let mut vars = SessionVars::new();
        // `error_count` is SESSION-only (scope 2): Go's `ErrLocalVariable`.
        assert_eq!(
            vars.set_global("debug_sync", "x".to_owned()),
            Err(VarError::SessionOnlyVariable("debug_sync".to_owned()))
        );
    }

    #[test]
    fn replacing_a_cluster_global_image_preserves_instance_only_values() {
        let globals = GlobalSysvars::new();
        globals
            .set_instance("tidb_general_log", "ON".to_owned())
            .expect("the instance-only variable sets");
        globals
            .set("autocommit", "OFF".to_owned())
            .expect("the old global override sets");

        let fresh = GlobalSysvars::from_cluster_rows([(
            "require_secure_transport".to_owned(),
            "ON".to_owned(),
        )]);
        globals.replace_from(&fresh);

        assert_eq!(globals.get("tidb_general_log").as_deref(), Ok("ON"));
        assert_eq!(globals.get("require_secure_transport").as_deref(), Ok("ON"));
        assert_eq!(globals.get("autocommit").as_deref(), Ok("ON"));
    }

    #[test]
    fn set_session_on_a_global_only_variable_is_1229() {
        let mut vars = SessionVars::new();
        // `default_password_lifetime` is GLOBAL-only (scope 1): Go's
        // `ErrGlobalVariable`.
        assert_eq!(
            vars.set_system("default_password_lifetime", "5".to_owned()),
            Err(VarError::GlobalOnlyVariable(
                "default_password_lifetime".to_owned()
            ))
        );
    }

    #[test]
    fn reading_global_on_a_session_only_variable_is_1238() {
        let vars = SessionVars::new();
        assert_eq!(
            vars.get_global("debug_sync"),
            Err(VarError::NoGlobalCopy("debug_sync".to_owned()))
        );
    }

    #[test]
    fn a_new_session_inherits_the_global_but_a_live_session_does_not() {
        let globals = GlobalSysvars::new();

        // A session already open BEFORE the GLOBAL changes keeps its own
        // value -- MySQL's rule that the session copy is made once, at
        // connect.
        let mut live = SessionVars::new();
        live.seed_from_globals(globals.clone()).unwrap();
        globals.set("autocommit", "OFF".to_owned()).unwrap();
        assert_eq!(live.get_system("autocommit").unwrap(), "ON");

        // `SET GLOBAL` again, then a brand NEW session opens: it inherits
        // the value that was live at ITS connect time.
        globals.set("autocommit", "OFF".to_owned()).unwrap();
        let mut fresh = SessionVars::new();
        fresh.seed_from_globals(globals).unwrap();
        assert_eq!(fresh.get_system("autocommit").unwrap(), "OFF");

        // Neither session's inherited copy is the live table: further
        // session-only `SET` traffic on one does not leak to the other.
        live.set_system("autocommit", "OFF".to_owned()).unwrap();
        fresh.set_system("autocommit", "ON".to_owned()).unwrap();
        assert_eq!(live.get_system("autocommit").unwrap(), "OFF");
        assert_eq!(fresh.get_system("autocommit").unwrap(), "ON");
    }
}
