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

//! Go `pkg/ddl/backfilling.go`'s result of one backfill batch.

/// What one reorganization batch did (Go `backfillTaskContext`).
#[derive(Debug, Default)]
pub struct ReorgBatch {
    /// Where the next batch starts.
    pub next_key: Vec<u8>,
    /// Whether the range is exhausted.
    pub done: bool,
    /// Rows (or temporary entries) scanned.
    pub scanned: u64,
    /// Entries written.
    pub added: u64,
    /// Row keys the batch locks (Go `txn.LockKeys` in the backfill), for the
    /// caller to commit as lock-only mutations with the batch.
    pub locks: Vec<Vec<u8>>,
    /// A column rewrite's per-row warnings (Go `backfillTaskContext.warnings`
    /// and `warningsCount`): each code's first warning and how often it
    /// recurred, for the job's `ReorgMeta`.
    pub warnings: Vec<(u16, String, i64)>,
}
