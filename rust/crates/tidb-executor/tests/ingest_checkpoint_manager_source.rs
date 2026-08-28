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

//! Port ledger for `pkg/ddl/ingest/checkpoint_test.go` (pkg/ddl.part7 items
//! 364-366 of the local enumeration: `TestCheckpointManager`,
//! `TestCheckpointManagerUpdateReorg`, `TestCheckpointManagerResumeReorg`).
//! All three drive `ingest.CheckpointManager` over a mockstore session pool
//! and a `mysql.tidb_ddl_reorg` row; the crate has no checkpoint carrier, so
//! they stay documentary gap ports.

/// GO PORT of `pkg/ddl/ingest/checkpoint_test.go:53 TestCheckpointManager`.
///
/// Re-derived contract: a chunk's end key only becomes "processed" through
/// the two-gate watermark pipeline — `UpdateChunk(taskID, keys, last)` and
/// `FinishChunk` fill per-task checkpoint state, `AdvanceWatermark(false)`
/// runs `afterFlush` which pops the contiguous prefix of tasks whose
/// `lastBatchRead` is set and `writtenKeys == totalKeys` and
/// `chunksFinished == chunksTotal` into `flushedKeyLowWatermark`
/// (checkpoint.go:402-418), and `AdvanceWatermark(true)` additionally runs
/// `afterImport`+`updateCheckpoint` promoting the flushed prefix into
/// `importedKeyLowWatermark` (:420-437); `IsKeyProcessed(end)` answers from
/// the IMPORTED watermark only (checkpoint.go:293-300). Hence in the Go
/// test: a flushed-but-not-imported key still reads unprocessed; task 0
/// finishing while task 1 is unfinished keeps both unprocessed until the
/// contiguous prefix completes; out-of-order finishes (4 then 3 then 2)
/// advance only when the prefix closes; and a chunk whose wrong row count is
/// corrected by a second UpdateChunk+FinishChunk pair needs both finished
/// chunks counted before the key is processed.
#[test]
#[ignore = "go-parity-gap: no Rust carrier for ingest CheckpointManager chunk/watermark state machine (pkg/ddl/ingest/checkpoint.go:293-437) or its mockstore session pool"]
fn checkpoint_manager_watermark_marks_keys_only_after_imported_prefix() {}

/// GO PORT of `pkg/ddl/ingest/checkpoint_test.go:131
/// TestCheckpointManagerUpdateReorg`.
///
/// Re-derived contract: closing the manager flushes the global checkpoint to
/// the `mysql.tidb_ddl_reorg` row as a `JobReorgMeta` JSON blob whose
/// `Checkpoint` carries `GlobalKeyCount`/`LocalKeyCount` (both 100 = the
/// finished chunk's keys), `GlobalSyncKey`/`LocalSyncKey` (both the chunk's
/// end key `{'1','9'}`), and `TS` = the PD client's composed timestamp
/// `oracle.ComposeTS(13, 35)` taken from the injected `mockGetTSClient`
/// (pts=12, lts=34, so the first GetTS after init returns 13/35).
#[test]
#[ignore = "go-parity-gap: no Rust carrier for checkpoint persistence to mysql.tidb_ddl_reorg (pkg/ddl/ingest/checkpoint.go updateCheckpoint) nor oracle.ComposeTS in the gate closure"]
fn checkpoint_manager_persists_sync_keys_counts_and_ts_to_reorg_table() {}

/// GO PORT of `pkg/ddl/ingest/checkpoint_test.go:171
/// TestCheckpointManagerResumeReorg`.
///
/// Re-derived contract: with a persisted `ReorgCheckpoint` (GlobalSyncKey
/// `{'1','9'}`, LocalSyncKey `{'2','9'}`, TS 123456) in `mysql.tidb_ddl_reorg`
/// and an EMPTY local folder, the manager trusts only the global half:
/// `IsKeyProcessed({'1','9'})` true, `IsKeyProcessed({'2','9'})` false,
/// `TotalKeyCount()` 0, `NextStartKey()` = `{'1','9'}`, `GetImportTS()`
/// 123456; once the folder contains a data file, the local checkpoint is
/// usable too, so `{'2','9'}` is processed and `TotalKeyCount()` rises to the
/// local 100 with `NextStartKey()` = `{'2','9'}` — local progress is only
/// honored when the local store dir actually has content.
#[test]
#[ignore = "go-parity-gap: no Rust carrier for checkpoint resume (pkg/ddl/ingest/checkpoint.go resumeOrInitCheckpoint) or its local-folder liveness probe"]
fn checkpoint_manager_resume_trusts_local_data_only_when_folder_is_live() {}
