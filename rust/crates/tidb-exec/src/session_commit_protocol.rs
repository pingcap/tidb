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

//! Transaction activation options and the separate bootstrap storage defaults.

use tidb_txnkv::transaction::CommitProtocol;

use tidb_vardef::global_sysvar_initial::{
    global_system_variable_initial_value, GlobalSysvarEnvironment, ENABLE_1PC, ENABLE_ASYNC_COMMIT,
    ON,
};

/// Options copied once when a SQL transaction becomes active (Go
/// `SetOptionsOnTxnActive`). The native client owns their commit-time meaning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionTransactionOptions {
    /// Session permissions for native commit-protocol eligibility checks.
    pub commit_protocol: CommitProtocol,
    /// Session assertion checking level, fixed for this transaction.
    pub assertion_level: tidb_proto::KvrpcAssertionLevel,
}

impl SessionTransactionOptions {
    /// Applies the activation snapshot before the transaction is exposed.
    pub fn apply<C, L, T>(
        self,
        transaction: &mut tidb_txnkv::transaction::RealOptimisticTransaction<C, L, T>,
    ) {
        transaction.set_commit_protocol(self.commit_protocol);
        transaction.set_assertion_level(self.assertion_level);
    }
}

// Low-level storage and bootstrap callers have no SQL session. Preserve their
// explicit protocol selection and the native client's assertion default.
impl From<CommitProtocol> for SessionTransactionOptions {
    fn from(commit_protocol: CommitProtocol) -> Self {
        Self {
            commit_protocol,
            assertion_level: tidb_proto::KvrpcAssertionLevel::Off,
        }
    }
}

/// Bootstrap permissions for low-level TiKV storage operations without a SQL
/// session. SQL transaction activation must use its session variable snapshot.
#[must_use]
pub fn bootstrap_commit_protocol() -> CommitProtocol {
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
        let protocol = bootstrap_commit_protocol();
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
