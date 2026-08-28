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

//! Gap tests for the `recordSet` and shared-lock-key halves of Go
//! `pkg/executor/adapter_internal_test.go`. Go's `recordSet`
//! (`pkg/executor/adapter.go:100`) wraps a live `Executor` behind an
//! `ExecStmt`; this tier has no `ExecStmt`-coupled result set at all (the
//! DistSQL row stream is the sibling crate's `tidb-exec::distsql_recordset`),
//! and the shared-lock promotion helper is unported.

/// Go `pkg/executor/adapter_internal_test.go:133::TestRecordSetNewChunkAfterFinish`:
/// a finished `recordSet` (executor already cleared by `Close`) still returns
/// non-nil chunks shaped by its schema -- one `mysql.TypeLonglong` column for
/// both a nil allocator and an explicit one (`recordSet.NewChunk`,
/// `pkg/executor/adapter.go:240`).
#[test]
#[ignore = "go-parity-gap: Go's ExecStmt-coupled recordSet (pkg/executor/adapter.go:100/:240) is unported; the tier's row stream is tidb-exec::distsql_recordset with no chunk-allocator seam"]
fn record_set_new_chunk_after_finish_matches_schema() {}

/// Go `pkg/executor/adapter_internal_test.go:145::TestRecordSetNextAfterFinish`:
/// `Next` over an already-finished record set returns
/// `exeerrors.ErrQueryInterrupted` (`pkg/executor/adapter.go:166 Next`,
/// `:192 ErrQueryInterrupted` arm), not a silent empty chunk.
#[test]
#[ignore = "go-parity-gap: recordSet.Next (pkg/executor/adapter.go:166/:192) and the ErrQueryInterrupted finish-guard are unported on this tier"]
fn record_set_next_after_finish_returns_query_interrupted() {}

/// Go `pkg/executor/adapter_internal_test.go:166::TestMoveWrittenSharedLockKeysToExclusive`
/// (table-driven, 3 cases): `moveWrittenSharedLockKeysToExclusive`
/// (`pkg/executor/adapter.go:1335`) takes the membuf RLock once per call
/// whenever shared keys exist, promotes shared keys that are already written
/// in the transaction buffer (membuf `GetLocal` succeeds) to exclusive keys,
/// deduplicates them against the incoming exclusive set, leaves unwritten
/// shared keys shared, and on a `GetLocal` error returns
/// `(nil, nil, err)`.
#[test]
#[ignore = "go-parity-gap: moveWrittenSharedLockKeysToExclusive (pkg/executor/adapter.go:1335) is unported; shared-lock keys here exist only as reported stats (tidb-exec LockKeysDetails)"]
fn move_written_shared_lock_keys_to_exclusive_promotes_written_keys() {}
