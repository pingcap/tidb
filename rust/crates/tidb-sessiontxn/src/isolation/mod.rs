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

//! Go `pkg/sessiontxn/isolation`: the providers of each isolation level.

pub mod optimistic;

/// The `*variable.SessionVars` fields Go's providers read.
pub trait ProviderSessionVars {
    /// Go `SessionVars.RetryLimit` (`@@tidb_retry_limit`).
    fn retry_limit(&self) -> i64;
    /// Go `SessionVars.SnapshotTS` (`@@tidb_snapshot`), 0 when unset.
    fn snapshot_ts(&self) -> u64;
    /// Go `SessionVars.InTxn()`.
    fn in_txn(&self) -> bool;
    /// Go `SessionVars.InRestrictedSQL`.
    fn in_restricted_sql(&self) -> bool;
    /// Go `SessionVars.DisableTxnAutoRetry`.
    fn disable_txn_auto_retry(&self) -> bool;
}
