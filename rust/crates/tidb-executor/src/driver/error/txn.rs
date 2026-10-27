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

/// Why a transaction statement failed (Go `kv.ErrWriteConflict` and friends).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxnErrorKind {
    /// The catalog moved under the transaction, so committing would discard
    /// another session's writes.
    WriteConflict,
    /// An `AS OF TIMESTAMP` expression Go's `CalculateAsOfTsExpr` would
    /// refuse -- NULL, unparsable, or a TSO from before 2013-01-01
    /// (`pkg/sessiontxn/staleread/util.go:41-80`). Renders as `ErrAsOf`
    /// (8135), "invalid as of timestamp: %s".
    AsOf(String),
    /// The storage route was stale or temporarily unavailable. The SQL layer
    /// exposes this as `ErrRegionUnavailable` (9005); `pkg/kv` deliberately
    /// excludes that code from automatic transaction replay.
    RegionUnavailable,
}
