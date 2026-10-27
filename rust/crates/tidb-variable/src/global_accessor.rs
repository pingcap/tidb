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

//! Go `variable.GlobalVarAccessor` (`pkg/sessionctx/variable/variable.go`)
//! and its test double `MockGlobalAccessor` (`mock_globalaccessor.go`).
//!
//! `SessionVars.GlobalVarsAccessor` is how a session reads and writes the
//! GLOBAL and INSTANCE tiers. `variable` only names the interface: the
//! session installs itself (`session.go` `createSession`), which answers
//! from the domain's sysvar cache (`tidb_domain::global_sysvars`), and a mock
//! context installs the mock.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::sysvar::{get_sys_var, sys_var_index_lookup, SYS_VARS};
use crate::vars::ExternalTimestampProvider;
use crate::VarError;

/// Go `variable.GlobalVarAccessor`, with the typed reads this tier serves
/// from the same object where Go loads `vardef` atomics.
pub trait GlobalVarAccessor: crate::GlobalSysvarAccessor + std::fmt::Debug {
    /// Go `GetGlobalSysVar`.
    ///
    /// # Errors
    /// An unknown variable.
    fn get(&self, name: &str) -> Result<String, VarError>;

    /// Go `GetGlobalSysVar` as `@@global.x` reads it: 1238 for a variable
    /// with no GLOBAL (or INSTANCE) scope.
    ///
    /// # Errors
    /// An unknown variable, or one with no global copy.
    fn get_for_global_scope(&self, name: &str) -> Result<String, VarError>;

    /// The node-tier value of the registry's `index`th variable.
    ///
    /// # Errors
    /// What reading it reports.
    fn get_by_registry_index(&self, index: usize) -> Result<String, VarError>;

    /// Go `SetGlobalSysVar`, validated under `zone`; `true` when the stored
    /// value changed.
    ///
    /// # Errors
    /// Go's scope and validation errors.
    fn set_with_time_zone(
        &self,
        name: &str,
        value: String,
        zone: &tidb_datatype::SessionTimeZone,
    ) -> Result<bool, VarError>;

    /// Go `SetInstanceSysVar`.
    ///
    /// # Errors
    /// Go's scope and validation errors.
    fn set_instance(&self, name: &str, value: String) -> Result<bool, VarError>;

    /// `SET GLOBAL name = DEFAULT`.
    ///
    /// # Errors
    /// Go's scope errors.
    fn reset(&self, name: &str) -> Result<(), VarError>;

    /// `SET INSTANCE name = DEFAULT`.
    ///
    /// # Errors
    /// Go's scope errors.
    fn reset_instance(&self, name: &str) -> Result<(), VarError>;

    /// Go `Domain.GetSessionCache`: the values a new session starts from.
    fn session_cache(&self) -> HashMap<String, String>;

    /// The stored `mysql.global_variables` rows.
    fn overrides(&self) -> HashMap<String, String>;

    /// Go's typed `vardef.OOMAction` and `vardef.EnableTmpStorageOnOOM`.
    fn statement_memory_policy(&self) -> (tidb_vardef::OomAction, bool);

    /// Go's `CheckMb4ValueInUTF8` instance setting.
    fn check_mb4_value_in_utf8(&self) -> bool;

    /// Go's typed `vardef.EnableCheckConstraint`.
    fn enable_check_constraint(&self) -> bool;

    /// Go's typed `vardef.EnableResourceControl`.
    fn resource_control_enabled(&self) -> bool;

    /// Go's typed `vardef.EnableResourceControlStrictMode`.
    fn resource_control_strict_mode(&self) -> bool;

    /// The storage authority behind `tidb_external_ts`, when installed.
    fn external_timestamp_provider(&self) -> Option<Arc<dyn ExternalTimestampProvider>>;

    /// Installs the storage authority behind `tidb_external_ts`.
    fn set_external_timestamp_provider(&self, provider: Arc<dyn ExternalTimestampProvider>);
}

/// Go `MockGlobalAccessor` from `NewMockGlobalAccessor4Tests`: every
/// variable at its registry default, `SET GLOBAL` validated and stored in
/// the mock's own map. It runs no `SetGlobal` hook and keeps no INSTANCE
/// tier (Go's mock applies the hook and stores nothing for INSTANCE).
#[derive(Debug, Default)]
pub struct MockGlobalAccessor {
    vals: Mutex<HashMap<String, String>>,
    external_timestamp: RwLock<Option<Arc<dyn ExternalTimestampProvider>>>,
}

impl MockGlobalAccessor {
    /// Go `NewMockGlobalAccessor4Tests`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            vals: Mutex::new(
                SYS_VARS
                    .iter()
                    .map(|def| (def.name.to_owned(), crate::sysvar::effective_default(def)))
                    .collect(),
            ),
            external_timestamp: RwLock::default(),
        }
    }

    fn value(&self, name: &str) -> Result<String, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        Ok(self
            .vals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(def.name)
            .cloned()
            .unwrap_or_else(|| crate::sysvar::effective_default(def)))
    }

    fn store(&self, name: &str, value: String) -> Result<bool, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        let mut vals = self
            .vals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let changed = vals.get(def.name) != Some(&value);
        vals.insert(def.name.to_owned(), value);
        Ok(changed)
    }
}

impl crate::GlobalSysvarAccessor for MockGlobalAccessor {
    fn get_global_sysvar(&self, name: &str) -> Option<String> {
        self.value(name).ok()
    }
}

impl GlobalVarAccessor for MockGlobalAccessor {
    fn get(&self, name: &str) -> Result<String, VarError> {
        self.value(name)
    }

    fn get_for_global_scope(&self, name: &str) -> Result<String, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if !def.has_global_scope() && !def.has_instance_scope() && def.has_session_scope() {
            return Err(VarError::NoGlobalCopy(name.to_ascii_lowercase()));
        }
        self.value(name)
    }

    fn get_by_registry_index(&self, index: usize) -> Result<String, VarError> {
        self.value(SYS_VARS[index].name)
    }

    fn set_with_time_zone(
        &self,
        name: &str,
        value: String,
        _zone: &tidb_datatype::SessionTimeZone,
    ) -> Result<bool, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        let value = def
            .validate_in_scope(&value, crate::sysvar::SCOPE_GLOBAL)
            .map(|validated| validated.value)
            .map_err(|error| crate::vars::validation_var_error(name, &value, error))?;
        self.store(name, value)
    }

    fn set_instance(&self, name: &str, _value: String) -> Result<bool, VarError> {
        get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        Ok(false)
    }

    fn reset(&self, name: &str) -> Result<(), VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        self.store(name, crate::sysvar::effective_default(def)).map(|_| ())
    }

    fn reset_instance(&self, name: &str) -> Result<(), VarError> {
        get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        Ok(())
    }

    fn session_cache(&self) -> HashMap<String, String> {
        self.vals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|(name, _)| {
                sys_var_index_lookup(name)
                    .is_some_and(|index| SYS_VARS[index].has_session_scope())
            })
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect()
    }

    fn overrides(&self) -> HashMap<String, String> {
        HashMap::new()
    }

    fn statement_memory_policy(&self) -> (tidb_vardef::OomAction, bool) {
        (tidb_vardef::OomAction::Cancel, true)
    }

    fn check_mb4_value_in_utf8(&self) -> bool {
        true
    }

    fn enable_check_constraint(&self) -> bool {
        false
    }

    fn resource_control_enabled(&self) -> bool {
        tidb_vardef::defaults::DEF_TIDB_ENABLE_RESOURCE_CONTROL
    }

    fn resource_control_strict_mode(&self) -> bool {
        tidb_vardef::defaults::DEF_TIDB_RESOURCE_CONTROL_STRICT_MODE
    }

    fn external_timestamp_provider(&self) -> Option<Arc<dyn ExternalTimestampProvider>> {
        self.external_timestamp
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn set_external_timestamp_provider(&self, provider: Arc<dyn ExternalTimestampProvider>) {
        *self
            .external_timestamp
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(provider);
    }
}
