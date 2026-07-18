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

//! Transaction-scope metadata translated from `pkg/kv/txn_scope_var.go`.
//!
//! The Go constructors read process configuration and, in the default path,
//! therefore depend on `pkg/config`. This leaf keeps that boundary explicit:
//! callers pass the already resolved configuration value to
//! [`TxnScopeVar::from_configured_scope`]. The scope value is otherwise an
//! immutable pair of strings, with no PD client, timestamp oracle, or request
//! context hidden behind it.

/// Scope used by PD for globally synchronized timestamps.
pub const GLOBAL_TXN_SCOPE: &str = "global";

/// SQL-visible scope for a transaction that uses a local timestamp oracle.
pub const LOCAL_TXN_SCOPE: &str = "local";

/// The two strings carried by TiDB's `TxnScopeVar`.
///
/// `var_value` is the value exposed by `@@txn_scope`; `txn_scope` is the value
/// passed to the timestamp oracle. For a local variable these values differ:
/// the first is always `local`, while the second is the configured zone label.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TxnScopeVar {
    var_value: String,
    txn_scope: String,
}

impl TxnScopeVar {
    /// Creates the global scope (`global`, `global`).
    #[must_use]
    pub fn new_global() -> Self {
        Self::new(GLOBAL_TXN_SCOPE, GLOBAL_TXN_SCOPE)
    }

    /// Creates a local scope with the exact configured oracle scope.
    ///
    /// Go does not validate or normalize this string in
    /// `NewLocalTxnScopeVar`, so arbitrary labels are preserved verbatim.
    #[must_use]
    pub fn new_local(txn_scope: impl Into<String>) -> Self {
        Self::new(LOCAL_TXN_SCOPE, txn_scope)
    }

    /// Creates the default value from `config.GetTxnScopeFromConfig()`.
    ///
    /// The Go implementation chooses global only for the exact global
    /// constant; every other configured value becomes a local variable with
    /// that same value as its oracle scope. Passing the resolved config value
    /// keeps configuration ownership in the caller.
    #[must_use]
    pub fn from_configured_scope(configured_scope: &str) -> Self {
        if configured_scope == GLOBAL_TXN_SCOPE {
            Self::new_global()
        } else {
            Self::new_local(configured_scope)
        }
    }

    fn new(var_value: impl Into<String>, txn_scope: impl Into<String>) -> Self {
        Self {
            var_value: var_value.into(),
            txn_scope: txn_scope.into(),
        }
    }

    /// Returns the SQL-visible `@@txn_scope` value.
    #[must_use]
    pub fn var_value(&self) -> &str {
        &self.var_value
    }

    /// Returns the scope passed to the timestamp oracle.
    #[must_use]
    pub fn txn_scope(&self) -> &str {
        &self.txn_scope
    }
}

#[cfg(test)]
mod tests {
    use super::{TxnScopeVar, GLOBAL_TXN_SCOPE, LOCAL_TXN_SCOPE};

    #[test]
    fn global_and_local_constructors_preserve_the_source_pair() {
        let global = TxnScopeVar::new_global();
        assert_eq!(global.var_value(), GLOBAL_TXN_SCOPE);
        assert_eq!(global.txn_scope(), GLOBAL_TXN_SCOPE);

        let local = TxnScopeVar::new_local("zone-a");
        assert_eq!(local.var_value(), LOCAL_TXN_SCOPE);
        assert_eq!(local.txn_scope(), "zone-a");
    }

    #[test]
    fn configured_scope_only_selects_global_on_exact_match() {
        let global = TxnScopeVar::from_configured_scope(GLOBAL_TXN_SCOPE);
        assert_eq!(global, TxnScopeVar::new_global());

        for configured in ["zone-a", LOCAL_TXN_SCOPE, "", "unknown-zone"] {
            let local = TxnScopeVar::from_configured_scope(configured);
            assert_eq!(local.var_value(), LOCAL_TXN_SCOPE);
            assert_eq!(local.txn_scope(), configured);
        }
    }

    #[test]
    fn scope_metadata_is_immutable_and_compares_by_values() {
        let first = TxnScopeVar::new_local(String::from("zone-a"));
        let second = TxnScopeVar::new_local("zone-a");
        assert_eq!(first, second);
        assert_ne!(first, TxnScopeVar::new_local("zone-b"));
        assert_eq!(first.var_value(), LOCAL_TXN_SCOPE);
        assert_eq!(first.txn_scope(), "zone-a");
    }
}
