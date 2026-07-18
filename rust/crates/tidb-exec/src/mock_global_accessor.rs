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

//! Dependency-closed global-system-variable test accessor from
//! `pkg/sessionctx/variable/mock_globalaccessor.go`.
//!
//! TiDB's mock accessor has two intentionally different modes. The ordinary
//! constructor reads the process-wide registry and returns an empty value for
//! unknown names; the test-suite constructor snapshots defaults and rejects
//! unknown names. This leaf keeps that map/validation boundary typed. Live
//! `SessionVars`, system-variable hooks, context cancellation, and SQL error
//! construction remain outside this module.

use std::collections::BTreeMap;
use std::fmt;

/// The source name of TiDB's default authentication plugin variable.
pub const DEFAULT_AUTH_PLUGIN: &str = "default_authentication_plugin";
/// The source table key used by the GC lifetime test hook.
pub const TIKV_GC_LIFE_TIME: &str = "tikv_gc_life_time";
/// The default value returned by TiDB's GC lifetime variable.
pub const DEFAULT_GC_LIFE_TIME: &str = "10m0s";

const DEFAULT_AUTH_PLUGIN_VALUE: &str = "mysql_native_password";
const AUTH_PLUGIN_VALUES: &[&str] = &[
    "mysql_native_password",
    "caching_sha2_password",
    "tidb_sm3_password",
    "authentication_ldap_sasl",
    "authentication_ldap_simple",
];

/// Errors at the mock global-accessor boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MockGlobalAccessorError {
    /// The test-suite map does not contain this variable.
    UnknownSystemVariable(String),
    /// A known variable rejected the supplied value.
    InvalidValue {
        /// The system-variable name whose setter rejected the value.
        name: String,
        /// The rejected source-form value.
        value: String,
    },
    /// The table-value hook only supports the source GC lifetime key.
    UnsupportedTableValue(String),
}

impl fmt::Display for MockGlobalAccessorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownSystemVariable(name) => {
                write!(f, "unknown system variable: {name}")
            }
            Self::InvalidValue { name, value } => {
                write!(f, "invalid value for {name}: {value}")
            }
            Self::UnsupportedTableValue(name) => {
                write!(f, "unsupported TiDB table value: {name}")
            }
        }
    }
}

impl std::error::Error for MockGlobalAccessorError {}

/// A small source-shaped mock implementation of `GlobalVarAccessor`.
#[derive(Clone, Debug, Default)]
pub struct MockGlobalAccessor {
    values: BTreeMap<String, String>,
    test_suite: bool,
}

impl MockGlobalAccessor {
    /// Creates the ordinary accessor. Unknown names return an empty value.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates the test-suite accessor with all dependency-closed defaults.
    #[must_use]
    pub fn for_tests() -> Self {
        let mut values = BTreeMap::new();
        values.insert(
            DEFAULT_AUTH_PLUGIN.to_owned(),
            DEFAULT_AUTH_PLUGIN_VALUE.to_owned(),
        );
        Self {
            values,
            test_suite: true,
        }
    }

    /// Returns a global value, preserving ordinary and test-suite mode.
    pub fn get_global_sys_var(&self, name: &str) -> Result<String, MockGlobalAccessorError> {
        if !self.test_suite {
            return Ok(match name {
                DEFAULT_AUTH_PLUGIN => DEFAULT_AUTH_PLUGIN_VALUE.to_owned(),
                _ => String::new(),
            });
        }
        self.values
            .get(name)
            .cloned()
            .ok_or_else(|| MockGlobalAccessorError::UnknownSystemVariable(name.to_owned()))
    }

    /// Validates and stores one global system variable.
    pub fn set_global_sys_var(
        &mut self,
        name: &str,
        value: &str,
    ) -> Result<(), MockGlobalAccessorError> {
        self.ensure_known(name)?;
        if name == DEFAULT_AUTH_PLUGIN && !AUTH_PLUGIN_VALUES.contains(&value) {
            return Err(MockGlobalAccessorError::InvalidValue {
                name: name.to_owned(),
                value: value.to_owned(),
            });
        }
        self.values.insert(name.to_owned(), value.to_owned());
        Ok(())
    }

    /// Stores a global value without invoking the source validation hook.
    pub fn set_global_sys_var_only(
        &mut self,
        name: &str,
        value: &str,
    ) -> Result<(), MockGlobalAccessorError> {
        self.ensure_known(name)?;
        self.values.insert(name.to_owned(), value.to_owned());
        Ok(())
    }

    /// Returns the one table value supported by the source test accessor.
    pub fn get_tidb_table_value(&self, name: &str) -> Result<String, MockGlobalAccessorError> {
        if name == TIKV_GC_LIFE_TIME {
            Ok(DEFAULT_GC_LIFE_TIME.to_owned())
        } else {
            Err(MockGlobalAccessorError::UnsupportedTableValue(
                name.to_owned(),
            ))
        }
    }

    /// Returns whether this accessor is in test-suite snapshot mode.
    #[must_use]
    pub const fn is_test_suite(&self) -> bool {
        self.test_suite
    }

    fn ensure_known(&self, name: &str) -> Result<(), MockGlobalAccessorError> {
        if name == DEFAULT_AUTH_PLUGIN {
            Ok(())
        } else {
            Err(MockGlobalAccessorError::UnknownSystemVariable(
                name.to_owned(),
            ))
        }
    }
}
