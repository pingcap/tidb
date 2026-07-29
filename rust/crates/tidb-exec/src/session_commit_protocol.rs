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

//! Which commit protocols this node's sessions are allowed to attempt.

use tidb_txnkv::transaction::CommitProtocol;

use crate::global_sysvar_initial::{
    global_system_variable_initial_value, GlobalSysvarEnvironment, ENABLE_1PC, ENABLE_ASYNC_COMMIT,
    ON,
};

/// `@@tidb_enable_async_commit` and `@@tidb_enable_1pc` as a TiKV-backed
/// cluster actually bootstraps them.
///
/// Both variables carry `OFF` in the registry, which is what a mock-store TiDB
/// runs with. Go `GlobalSystemVariableInitialValue` overrides both to `ON` when
/// the configured store is TiKV, and that overridden value is what bootstrap
/// writes into `mysql.global_variables` — so on a real cluster both protocols
/// are on by default. This node has no `SET`-able session variable store, so it
/// reads the bootstrap value directly, exactly as it does for
/// `@@tidb_pessimistic_txn_fair_locking`.
#[must_use]
pub fn session_commit_protocol() -> CommitProtocol {
    let environment = GlobalSysvarEnvironment {
        store_is_tikv: true,
        in_test: false,
        next_gen: false,
    };
    CommitProtocol {
        async_commit: global_system_variable_initial_value(ENABLE_ASYNC_COMMIT, "OFF", environment)
            == ON,
        one_pc: global_system_variable_initial_value(ENABLE_1PC, "OFF", environment) == ON,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A TiKV-backed node runs with both protocols on, and the registry default
    /// alone would have said otherwise.
    #[test]
    fn a_tikv_backed_node_enables_both_faster_commit_protocols() {
        let protocol = session_commit_protocol();
        assert!(protocol.async_commit);
        assert!(protocol.one_pc);

        let mock_store = GlobalSysvarEnvironment {
            store_is_tikv: false,
            in_test: false,
            next_gen: false,
        };
        assert_eq!(
            global_system_variable_initial_value(ENABLE_ASYNC_COMMIT, "OFF", mock_store),
            "OFF"
        );
        assert_eq!(
            global_system_variable_initial_value(ENABLE_1PC, "OFF", mock_store),
            "OFF"
        );
    }
}
