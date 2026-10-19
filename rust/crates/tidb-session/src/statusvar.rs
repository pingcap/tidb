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

//! Go `pkg/sessionctx/variable/statusvar.go`'s statistics registry: the
//! process-wide status-variable providers `SHOW STATUS` collects from, such
//! as the GC worker's `tidb_gc_*` rows.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, RwLock};

use crate::sysvar::{SCOPE_GLOBAL, SCOPE_SESSION};

/// Go `DefaultStatusVarScopeFlag`.
pub const DEFAULT_STATUS_VAR_SCOPE_FLAG: u8 = SCOPE_GLOBAL | SCOPE_SESSION;

/// Go `Statistics`.
pub trait Statistics: Send + Sync {
    /// Go `GetScope`: the scope of one of the provider's statuses.
    fn get_scope(&self, status: &str) -> u8;
    /// Go `Stats`: the provider's statuses by name.
    fn stats(&self) -> Result<BTreeMap<String, String>, String>;
}

/// Go `StatusVal`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatusVal {
    /// Go `Scope`.
    pub scope: u8,
    /// Go `Value`, rendered.
    pub value: String,
}

static STATISTICS_LIST: LazyLock<RwLock<Vec<Arc<dyn Statistics>>>> =
    LazyLock::new(|| RwLock::new(Vec::new()));

/// Go `RegisterStatistics`.
pub fn register_statistics(statistics: Arc<dyn Statistics>) {
    STATISTICS_LIST
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(statistics);
}

/// Go `UnregisterStatistics`.
pub fn unregister_statistics(statistics: &Arc<dyn Statistics>) {
    let mut list = STATISTICS_LIST
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(index) = list
        .iter()
        .rposition(|registered| Arc::ptr_eq(registered, statistics))
    {
        list.swap_remove(index);
    }
}

/// Go `GetStatusVars`: every registered provider's statuses; a later
/// provider's status replaces an earlier one of the same name.
pub fn get_status_vars() -> Result<BTreeMap<String, StatusVal>, String> {
    let list = STATISTICS_LIST
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut status_vars = BTreeMap::new();
    for statistics in list.iter() {
        for (name, value) in statistics.stats()? {
            let scope = statistics.get_scope(&name);
            status_vars.insert(name, StatusVal { scope, value });
        }
    }
    Ok(status_vars)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(&'static str, &'static str, u8);

    impl Statistics for Fixed {
        fn get_scope(&self, _status: &str) -> u8 {
            self.2
        }

        fn stats(&self) -> Result<BTreeMap<String, String>, String> {
            Ok(BTreeMap::from([(self.0.to_owned(), self.1.to_owned())]))
        }
    }

    #[test]
    fn registered_statistics_are_collected_until_unregistered() {
        let provider: Arc<dyn Statistics> =
            Arc::new(Fixed("statusvar_test_status", "1", SCOPE_SESSION));
        register_statistics(Arc::clone(&provider));
        assert_eq!(
            Some(&StatusVal {
                scope: SCOPE_SESSION,
                value: "1".to_owned()
            }),
            get_status_vars().unwrap().get("statusvar_test_status")
        );
        unregister_statistics(&provider);
        assert!(!get_status_vars()
            .unwrap()
            .contains_key("statusvar_test_status"));
    }
}
