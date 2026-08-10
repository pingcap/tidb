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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnErrorKind {
    /// The catalog moved under the transaction, so committing would discard
    /// another session's writes.
    WriteConflict,
    /// The storage route was stale or temporarily unavailable. Go exposes
    /// this as `ErrRegionUnavailable` (9005), which an autocommit statement
    /// may replay after refreshing its route.
    RegionUnavailable,
}
