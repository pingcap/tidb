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

//! Gap tests for Go `pkg/executor/compact_table_test.go` (:58/:78/:98/:119/:160):
//! `ALTER TABLE ... COMPACT [TIFLASH REPLICA]`. Go drives the executor
//! against a mocked TiFlash RPC handler (`newCompactRequestMocker`, the
//! kvrpcpb CompactRequest/CompactResponse pair) and asserts the WARNINGS the
//! executor surfaces per store. This tier has no TiFlash compact RPC and no
//! ALTER TABLE COMPACT statement, so the five contracts are recorded as
//! gaps. Go's warning shape is
//! `Warning 1105 compact on store <store> failed: <reason>`.

/// Go `pkg/executor/compact_table_test.go:58::TestCompactTableTooBusy`: a
/// `ErrTooManyPendingTasks` compact error surfaces as
/// `compact on store tiflash0 failed: store is too busy`.
#[test]
#[ignore = "go-parity-gap: no ALTER TABLE COMPACT executor or TiFlash CompactRequest mock on this tier"]
fn compact_table_too_busy_warns_store_is_too_busy() {}

/// Go `pkg/executor/compact_table_test.go:78::TestCompactTableInProgress`: a
/// `ErrCompactInProgress` error surfaces as `table is compacting in
/// progress`.
#[test]
#[ignore = "go-parity-gap: no ALTER TABLE COMPACT executor or TiFlash CompactRequest mock on this tier"]
fn compact_table_in_progress_warns_compacting_in_progress() {}

/// Go `pkg/executor/compact_table_test.go:98::TestCompactTableInternalError`: a
/// `ErrInvalidStartKey` error surfaces as `internal error (check logs for
/// details)`.
#[test]
#[ignore = "go-parity-gap: no ALTER TABLE COMPACT executor or TiFlash CompactRequest mock on this tier"]
fn compact_table_internal_error_warns_without_details() {}

/// Go `pkg/executor/compact_table_test.go:119::TestCompactTableNoRemaining`:
/// when every response answers `HasRemaining: false`, one request round per
/// compact statement suffices (`alter table t compact tiflash replica` and
/// later `alter table test.t compact` each hit the handler exactly once,
/// with `StartKey` empty and physical/logical table ids equal for a
/// non-partitioned table) and no warnings are produced.
#[test]
#[ignore = "go-parity-gap: no ALTER TABLE COMPACT executor or TiFlash CompactRequest mock on this tier"]
fn compact_table_no_remaining_needs_one_request_round() {}

/// Go `pkg/executor/compact_table_test.go:160::TestCompactTableHasRemaining`:
/// `HasRemaining: true` responses chain the follow-up request from the
/// previous `CompactedEndKey` (empty -> 0xFF -> 0xFF 0x20), continuing until
/// a response closes with `HasRemaining: false`.
#[test]
#[ignore = "go-parity-gap: no ALTER TABLE COMPACT executor or TiFlash CompactRequest mock on this tier"]
fn compact_table_has_remaining_follows_compacted_end_keys() {}
