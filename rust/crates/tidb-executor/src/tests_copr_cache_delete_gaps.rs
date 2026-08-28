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

//! Gap tests for Go `pkg/executor/copr_cache_test.go` and
//! `pkg/executor/delete_test.go`. These tests require the Go mock store's
//! coprocessor cache and the session transaction/foreign-key executors; the
//! Rust executor owns neither those external seams nor the corresponding SQL
//! warning and lock surfaces.

/// Go `pkg/executor/copr_cache_test.go:35::TestIntegrationCopCache`.
/// The Go test installs the unistore cache failpoint and uses
/// `explain analyze` to distinguish cache misses, hits, and a disabled cache
/// (`pkg/executor/copr_cache_test.go:69-95`); the request path is owned by
/// the Go coprocessor client rather than this crate.
#[test]
#[ignore = "go-parity-gap: unistore coprocessor-cache failpoints, region splitting, and explain RPC counters are unported"]
fn integration_cop_cache_tracks_hits_and_disabled_cache() {}

/// Go `pkg/executor/delete_test.go:27::TestDeleteLockKey`.
/// `DeleteExec::deleteSingleTableByChunk` (`pkg/executor/delete.go:93`) and
/// `removeRow` (`:300`) must hold the deleted primary/unique keys so a
/// concurrent pessimistic insert waits until commit across six key layouts.
#[test]
#[ignore = "go-parity-gap: cross-session pessimistic delete locks and transaction commit waits are unported"]
fn delete_lock_key_blocks_conflicting_inserts() {}

/// Go `pkg/executor/delete_test.go:111::TestDeleteIgnoreWithFK`.
/// `onRemoveRowForFK` (`pkg/executor/delete.go:319`) must convert parent-row
/// foreign-key failures into warnings for `DELETE IGNORE`, including joined
/// and batched deletes, while deleting unconstrained rows.
#[test]
#[ignore = "go-parity-gap: foreign-key enforcement, DELETE IGNORE warnings, and batch DML are unported"]
fn delete_ignore_with_foreign_key_reports_warnings() {}
