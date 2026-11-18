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

//! Go `pkg/sessiontxn/isolation/optimistic.go`.

use super::ProviderSessionVars;
use crate::EnterNewTxnType;

/// Go `isOptimisticTxnRetryable`: the `TxnCtx.CouldRetry` an optimistic
/// transaction gets when it activates (`OptimisticTxnContextProvider.
/// onTxnActive`).
#[must_use]
pub fn is_optimistic_txn_retryable(
    vars: &dyn ProviderSessionVars,
    tp: EnterNewTxnType,
    is_pipelined: bool,
) -> bool {
    if tp == EnterNewTxnType::Default {
        return false;
    }
    if is_pipelined {
        return false;
    }
    // If retry limit is 0, the transaction could not retry.
    if vars.retry_limit() == 0 {
        return false;
    }
    // When `@@tidb_snapshot` is set, it is a ready-only statement and will
    // not cause the errors that should retry a transaction in optimistic mode.
    if vars.snapshot_ts() != 0 {
        return false;
    }
    // If the session is not InTxn, it is an auto-committed transaction. The
    // auto-committed transaction could always retry.
    if !vars.in_txn() {
        return true;
    }
    // The internal transaction could always retry.
    if vars.in_restricted_sql() {
        return true;
    }
    // If the retry is enabled, the transaction could retry.
    !vars.disable_txn_auto_retry()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    struct Vars {
        retry_limit: i64,
        snapshot_ts: u64,
        in_txn: bool,
        in_restricted_sql: bool,
        disable_txn_auto_retry: bool,
    }

    impl ProviderSessionVars for Vars {
        fn retry_limit(&self) -> i64 {
            self.retry_limit
        }
        fn snapshot_ts(&self) -> u64 {
            self.snapshot_ts
        }
        fn in_txn(&self) -> bool {
            self.in_txn
        }
        fn in_restricted_sql(&self) -> bool {
            self.in_restricted_sql
        }
        fn disable_txn_auto_retry(&self) -> bool {
            self.disable_txn_auto_retry
        }
    }

    /// Every branch of Go's `isOptimisticTxnRetryable`, in its order.
    #[test]
    fn is_optimistic_txn_retryable_follows_gos_branches() {
        let autocommit = Vars {
            retry_limit: 10,
            snapshot_ts: 0,
            in_txn: false,
            in_restricted_sql: false,
            disable_txn_auto_retry: true,
        };
        let before = EnterNewTxnType::BeforeStmt;
        assert!(is_optimistic_txn_retryable(&autocommit, before, false));
        assert!(!is_optimistic_txn_retryable(&autocommit, EnterNewTxnType::Default, false));
        assert!(!is_optimistic_txn_retryable(&autocommit, before, true));
        assert!(!is_optimistic_txn_retryable(&Vars { retry_limit: 0, ..autocommit }, before, false));
        assert!(!is_optimistic_txn_retryable(&Vars { snapshot_ts: 7, ..autocommit }, before, false));

        let explicit = Vars { in_txn: true, ..autocommit };
        let begin = EnterNewTxnType::WithBeginStmt;
        assert!(!is_optimistic_txn_retryable(&explicit, begin, false));
        assert!(is_optimistic_txn_retryable(&Vars { in_restricted_sql: true, ..explicit }, begin, false));
        assert!(is_optimistic_txn_retryable(&Vars { disable_txn_auto_retry: false, ..explicit }, begin, false));
    }
}
