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

use crate::sysvar::{get_sys_var, ValidationError};

/// The shared GLOBAL-scope value table every session of one
/// [`crate::Session`] factory holds a clone of. In-memory only: Go persists
/// this tier to `mysql.GLOBAL_VARIABLES`, so a real cluster survives a
/// restart with its `SET GLOBAL` values and this one does not -- the same
/// documented gap the account/privilege/process registries carry.
#[derive(Clone, Debug, Default)]
pub struct GlobalSysvars {
    values: Arc<Mutex<HashMap<String, String>>>,
}

impl GlobalSysvars {
    /// A fresh registry with every variable at its default (Go's state on a
    /// cluster nobody has ever run `SET GLOBAL` against).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads a global value, falling back to the registry default.
    pub fn get(&self, name: &str) -> Result<String, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        Ok(self
            .values
            .lock()
            .expect("global sysvar lock poisoned")
            .get(&name.to_ascii_lowercase())
            .cloned()
            .unwrap_or_else(|| def.value.to_owned()))
    }

    /// Validates and writes a global value, visible to every session that
    /// reads `@@global.name` or opens AFTER this call.
    pub fn set(&self, name: &str, value: String) -> Result<(), VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.is_read_only() {
            return Err(VarError::ReadOnlyVariable(name.to_ascii_lowercase()));
        }
        if !def.has_global_scope() {
            return Err(VarError::SessionOnlyVariable(name.to_ascii_lowercase()));
        }
        let validated = def.validate(&value).map_err(|error| match error {
            ValidationError::WrongType => VarError::WrongTypeForVar(name.to_ascii_lowercase()),
            ValidationError::WrongValue => {
                VarError::WrongValueForVar(name.to_ascii_lowercase(), value.clone())
            }
        })?;
        self.values
            .lock()
            .expect("global sysvar lock poisoned")
            .insert(name.to_ascii_lowercase(), validated.value);
        Ok(())
    }

    /// Restores the registry default (`SET GLOBAL x = DEFAULT`).
    pub fn reset(&self, name: &str) -> Result<(), VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.is_read_only() {
            return Err(VarError::ReadOnlyVariable(name.to_ascii_lowercase()));
        }
        self.values
            .lock()
            .expect("global sysvar lock poisoned")
            .remove(&name.to_ascii_lowercase());
        Ok(())
    }

    /// Every variable this table currently overrides from its default, for
    /// [`SessionVars::seed_from_globals`] and `SHOW GLOBAL VARIABLES`.
    fn overrides(&self) -> HashMap<String, String> {
        self.values
            .lock()
            .expect("global sysvar lock poisoned")
            .clone()
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
}

/// A session's variable state: the system variables it has overridden and its
/// user variables (Go's `SessionVars.systems` and `userVars`).
#[derive(Clone, Debug, Default)]
pub struct SessionVars {
    systems: HashMap<String, String>,
    users: HashMap<String, Option<String>>,
    /// The shared GLOBAL-scope table this session's factory holds. Cloning a
    /// [`GlobalSysvars`] is cheap (one `Arc` bump), so every session shares
    /// the same underlying map.
    globals: GlobalSysvars,
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
    pub fn seed_from_globals(&mut self, globals: GlobalSysvars) {
        for (name, value) in globals.overrides() {
            // Only a variable this session can actually hold a session copy
            // of inherits the global value; Go's `NewSessionVars` walks the
            // same `HasSessionScope` guard when copying `GlobalVarsAccessor`
            // into a fresh session.
            if get_sys_var(&name).is_some_and(|def| def.has_session_scope()) {
                self.systems.insert(name, value);
            }
        }
        self.globals = globals;
    }

    /// Reads a system variable, falling back to the registry default.
    pub fn get_system(&self, name: &str) -> Result<String, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        Ok(self
            .systems
            .get(&name.to_ascii_lowercase())
            .cloned()
            .unwrap_or_else(|| def.value.to_owned()))
    }

    /// Reads `@@global.name`: always the shared table's live value, never
    /// this session's own copy. Go's `ErrIncorrectGlobalLocalVar` (1238) when
    /// the variable has no GLOBAL scope at all to read.
    pub fn get_global(&self, name: &str) -> Result<String, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if !def.has_global_scope() {
            return Err(VarError::NoGlobalCopy(name.to_ascii_lowercase()));
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
    pub fn set_system(&mut self, name: &str, value: String) -> Result<(), VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.is_read_only() {
            return Err(VarError::ReadOnlyVariable(name.to_ascii_lowercase()));
        }
        if !def.has_session_scope() {
            return Err(VarError::GlobalOnlyVariable(name.to_ascii_lowercase()));
        }
        let validated = def.validate(&value).map_err(|error| match error {
            ValidationError::WrongType => VarError::WrongTypeForVar(name.to_ascii_lowercase()),
            ValidationError::WrongValue => {
                VarError::WrongValueForVar(name.to_ascii_lowercase(), value.clone())
            }
        })?;
        self.systems
            .insert(name.to_ascii_lowercase(), validated.value);
        Ok(())
    }

    /// `SET GLOBAL name = value`: writes only the shared table, never this
    /// session's own `@@name`. Go's `ErrLocalVariable` (1228) when the
    /// variable is SESSION-only, so there is no global copy to set.
    pub fn set_global(&mut self, name: &str, value: String) -> Result<(), VarError> {
        self.globals.set(name, value)
    }

    /// `SET GLOBAL name = DEFAULT`.
    pub fn reset_global(&mut self, name: &str) -> Result<(), VarError> {
        self.globals.reset(name)
    }

    /// Clears a session override so the registry default applies again
    /// (Go's `SET x = DEFAULT`).
    pub fn reset_system(&mut self, name: &str) -> Result<(), VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.is_read_only() {
            return Err(VarError::ReadOnlyVariable(name.to_ascii_lowercase()));
        }
        self.systems.remove(&name.to_ascii_lowercase());
        Ok(())
    }

    /// Reads a user variable. An unset one is NULL, as in MySQL -- never an
    /// error, unlike a system variable.
    #[must_use]
    pub fn get_user(&self, name: &str) -> Option<String> {
        self.users
            .get(&name.to_ascii_lowercase())
            .cloned()
            .flatten()
    }

    /// Sets a user variable; `None` is Go's `UnsetUserVar` for a NULL value.
    pub fn set_user(&mut self, name: &str, value: Option<String>) {
        self.users.insert(name.to_ascii_lowercase(), value);
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
    fn set_global_on_a_session_only_variable_is_1228() {
        let mut vars = SessionVars::new();
        // `error_count` is SESSION-only (scope 2): Go's `ErrLocalVariable`.
        assert_eq!(
            vars.set_global("debug_sync", "x".to_owned()),
            Err(VarError::SessionOnlyVariable("debug_sync".to_owned()))
        );
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
        live.seed_from_globals(globals.clone());
        globals.set("autocommit", "OFF".to_owned()).unwrap();
        assert_eq!(live.get_system("autocommit").unwrap(), "ON");

        // `SET GLOBAL` again, then a brand NEW session opens: it inherits
        // the value that was live at ITS connect time.
        globals.set("autocommit", "OFF".to_owned()).unwrap();
        let mut fresh = SessionVars::new();
        fresh.seed_from_globals(globals);
        assert_eq!(fresh.get_system("autocommit").unwrap(), "OFF");

        // Neither session's inherited copy is the live table: further
        // session-only `SET` traffic on one does not leak to the other.
        live.set_system("autocommit", "OFF".to_owned()).unwrap();
        fresh.set_system("autocommit", "ON".to_owned()).unwrap();
        assert_eq!(live.get_system("autocommit").unwrap(), "OFF");
        assert_eq!(fresh.get_system("autocommit").unwrap(), "ON");
    }

    #[test]
    fn an_unset_user_variable_is_null_rather_than_an_error() {
        let mut vars = SessionVars::new();
        assert_eq!(vars.get_user("@x"), None);
        vars.set_user("@x", Some("1".to_owned()));
        assert_eq!(vars.get_user("@X"), Some("1".to_owned()));
        vars.set_user("@x", None);
        assert_eq!(vars.get_user("@x"), None);
    }
}
