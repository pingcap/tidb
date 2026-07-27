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
//! NOT MODELLED (documented): GLOBAL scope persistence (a `SET GLOBAL` here
//! changes nothing outside the session), the per-variable `Validation` and
//! `SetSession` closures such as autocommit's implicit commit, and the
//! removed-variable list Go silently accepts.

use std::collections::HashMap;

use crate::sysvar::{get_sys_var, ValidationError};

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
}

/// A session's variable state: the system variables it has overridden and its
/// user variables (Go's `SessionVars.systems` and `userVars`).
#[derive(Clone, Debug, Default)]
pub struct SessionVars {
    systems: HashMap<String, String>,
    users: HashMap<String, Option<String>>,
}

impl SessionVars {
    /// A session with every variable at its registry default.
    #[must_use]
    pub fn new() -> Self {
        SessionVars::default()
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

    /// Sets a session system variable, validating the value as Go's
    /// `ValidateFromType` does: the stored value is the normalized one, and
    /// an out-of-range value is clamped exactly as Go clamps it.
    ///
    /// A read-only variable is Go's `ErrIncorrectGlobalLocalVar`.
    pub fn set_system(&mut self, name: &str, value: String) -> Result<(), VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.is_read_only() {
            return Err(VarError::ReadOnlyVariable(name.to_ascii_lowercase()));
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
    fn an_unset_user_variable_is_null_rather_than_an_error() {
        let mut vars = SessionVars::new();
        assert_eq!(vars.get_user("@x"), None);
        vars.set_user("@x", Some("1".to_owned()));
        assert_eq!(vars.get_user("@X"), Some("1".to_owned()));
        vars.set_user("@x", None);
        assert_eq!(vars.get_user("@x"), None);
    }
}
