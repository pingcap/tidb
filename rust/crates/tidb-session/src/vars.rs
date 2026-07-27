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
//! PARTIAL REGISTRY (documented, and the reason a divergence is possible):
//! Go's table has well over a thousand entries; the ones below are
//! transcreated exactly -- name, scope, and default value read from
//! `pkg/sessionctx/variable/sysvar.go` and the constants it references -- and
//! cover what a connecting MySQL client reads and sets. A variable real TiDB
//! knows but this table does not yet list is rejected with 1193 where TiDB
//! would accept it. That is a visible, honest failure rather than a silently
//! wrong answer, and porting the rest of the table is its own unit.
//!
//! NOT MODELLED (documented): per-variable validation and typing (Go rejects
//! an out-of-range or non-enum value), GLOBAL scope persistence (a `SET
//! GLOBAL` here changes nothing outside the session), `SetSession` hooks such
//! as autocommit's implicit commit, and the removed-variable list Go silently
//! accepts.

use std::collections::HashMap;

/// Go `vardef.ScopeFlag`: where a system variable can be read and written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    /// Go `ScopeNone`: a read-only server property.
    None,
    /// Go `ScopeGlobal | ScopeSession`.
    GlobalAndSession,
    /// Go `ScopeSession`.
    Session,
}

/// One entry of Go's system-variable registry.
struct SysVarDef {
    name: &'static str,
    scope: Scope,
    value: &'static str,
}

/// Go `mysql.DefaultCharset`.
const DEFAULT_CHARSET: &str = "utf8mb4";
/// Go `mysql.DefaultCollationName`.
const DEFAULT_COLLATION: &str = "utf8mb4_bin";
/// Go `mysql.DefaultSQLMode`.
const DEFAULT_SQL_MODE: &str =
    "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,\
ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION";
/// Go `mysql.ServerVersion`: the MySQL compatibility version, a separator,
/// and the TiDB release version.
const SERVER_VERSION: &str = "8.0.11-v8.4.0-this-is-a-placeholder";
/// Go's `version_comment` for the Apache-licensed community edition.
const VERSION_COMMENT: &str =
    "TiDB Server (Apache License 2.0) Community Edition, MySQL 8.0 compatible";
/// Go `config.DefMaxAllowedPacket` (64 << 20).
const DEF_MAX_ALLOWED_PACKET: &str = "67108864";
/// Go `vardef.DefWaitTimeout`.
const DEF_WAIT_TIMEOUT: &str = "28800";

/// The transcreated slice of Go's registry (see the module doc).
const SYS_VARS: &[SysVarDef] = &[
    SysVarDef {
        name: "autocommit",
        scope: Scope::GlobalAndSession,
        value: "ON",
    },
    SysVarDef {
        name: "character_set_client",
        scope: Scope::GlobalAndSession,
        value: DEFAULT_CHARSET,
    },
    SysVarDef {
        name: "character_set_connection",
        scope: Scope::GlobalAndSession,
        value: DEFAULT_CHARSET,
    },
    SysVarDef {
        name: "character_set_results",
        scope: Scope::GlobalAndSession,
        value: DEFAULT_CHARSET,
    },
    SysVarDef {
        name: "character_set_server",
        scope: Scope::GlobalAndSession,
        value: DEFAULT_CHARSET,
    },
    SysVarDef {
        name: "collation_connection",
        scope: Scope::GlobalAndSession,
        value: DEFAULT_COLLATION,
    },
    SysVarDef {
        name: "collation_server",
        scope: Scope::GlobalAndSession,
        value: DEFAULT_COLLATION,
    },
    SysVarDef {
        name: "sql_mode",
        scope: Scope::GlobalAndSession,
        value: DEFAULT_SQL_MODE,
    },
    SysVarDef {
        name: "time_zone",
        scope: Scope::GlobalAndSession,
        value: "SYSTEM",
    },
    SysVarDef {
        name: "max_allowed_packet",
        scope: Scope::GlobalAndSession,
        value: DEF_MAX_ALLOWED_PACKET,
    },
    SysVarDef {
        name: "transaction_isolation",
        scope: Scope::GlobalAndSession,
        value: "REPEATABLE-READ",
    },
    SysVarDef {
        name: "tx_isolation",
        scope: Scope::GlobalAndSession,
        value: "REPEATABLE-READ",
    },
    SysVarDef {
        name: "wait_timeout",
        scope: Scope::GlobalAndSession,
        value: DEF_WAIT_TIMEOUT,
    },
    SysVarDef {
        name: "interactive_timeout",
        scope: Scope::GlobalAndSession,
        value: "28800",
    },
    // Go `ScopeNone`: read-only server properties.
    SysVarDef {
        name: "version",
        scope: Scope::None,
        value: SERVER_VERSION,
    },
    SysVarDef {
        name: "version_comment",
        scope: Scope::None,
        value: VERSION_COMMENT,
    },
];

/// Looks a system variable up by name, case-insensitively as Go's
/// `GetSysVar` does after lowercasing.
fn sys_var(name: &str) -> Option<&'static SysVarDef> {
    SYS_VARS
        .iter()
        .find(|candidate| candidate.name.eq_ignore_ascii_case(name))
}

/// Why a variable statement failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VarError {
    /// Go `ErrUnknownSystemVar` (1193).
    UnknownSystemVariable(String),
    /// Go `ErrIncorrectGlobalLocalVar` (1238): the variable is read-only.
    ReadOnlyVariable(String),
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
        let def = sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        Ok(self
            .systems
            .get(&name.to_ascii_lowercase())
            .cloned()
            .unwrap_or_else(|| def.value.to_owned()))
    }

    /// Sets a session system variable.
    ///
    /// A `ScopeNone` variable is read-only, which Go reports as
    /// `ErrIncorrectGlobalLocalVar`. Value validation and typing are not
    /// modelled (see the module doc), so any string is stored.
    pub fn set_system(&mut self, name: &str, value: String) -> Result<(), VarError> {
        let def = sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.scope == Scope::None {
            return Err(VarError::ReadOnlyVariable(name.to_ascii_lowercase()));
        }
        self.systems.insert(name.to_ascii_lowercase(), value);
        Ok(())
    }

    /// Clears a session override so the registry default applies again
    /// (Go's `SET x = DEFAULT`).
    pub fn reset_system(&mut self, name: &str) -> Result<(), VarError> {
        let def = sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.scope == Scope::None {
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
