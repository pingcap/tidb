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

//! Gap tests for Go executor detachment. The Go `Detach` helper
//! (`pkg/executor/detach.go:31`) clones supported executor trees and moves
//! their session-independent state behind a detachable record set
//! (`pkg/executor/adapter.go:300`). The Rust crate deliberately documents
//! `Detach` as deferred (`src/lib.rs` crate scope): its pull executor has no
//! detachable record-set/session cursor contract.

/// Go `pkg/executor/detach_integration_test.go:35::TestDetachAllContexts`.
/// Detaching a table query must clone the executor and all children, allow
/// the session to run another statement, and preserve the detached rows.
#[test]
#[ignore = "go-parity-gap: detachable record sets, executor-tree cloning, and session cursor state are unported"]
fn detach_all_contexts_preserves_rows_after_session_reuse() {}

/// Go `pkg/executor/detach_integration_test.go:75::TestAfterDetachSessionCanExecute`.
/// A detached 10,000-row record set can drain concurrently with new queries
/// on the original session (`pkg/executor/detach_integration_test.go:93-123`).
#[test]
#[ignore = "go-parity-gap: concurrent detached record-set draining and session reuse are unported"]
fn after_detach_session_can_execute_concurrently() {}

/// Go `pkg/executor/detach_integration_test.go:126::TestDetachWithParam`.
/// Detachment must capture bound parameter state so later statements with a
/// different parameter shape cannot change the detached range.
#[test]
#[ignore = "go-parity-gap: detached prepared-parameter state is not exposed by the Rust executor"]
fn detach_with_param_keeps_original_range() {}

/// Go `pkg/executor/detach_integration_test.go:176::TestDetachIndexReaderAndIndexLookUp`.
/// `Detach` must preserve both index-reader rows and index-lookup rows while
/// their source session is reused (`pkg/executor/detach.go:67-88`).
#[test]
#[ignore = "go-parity-gap: Go index-reader/index-lookup detachable executors and record sets are unported"]
fn detach_index_reader_and_index_lookup_preserves_rows() {}

/// Go `pkg/executor/detach_integration_test.go:235::TestDetachSelection`.
/// Selection detachment freezes supported user-variable values but rejects an
/// optional-property expression such as `found_rows()`.
#[test]
#[ignore = "go-parity-gap: session expression properties and detachable SelectionExec are unported"]
fn detach_selection_freezes_supported_state_and_rejects_optional_property() {}

/// Go `pkg/executor/detach_integration_test.go:313::TestDetachProjection`.
/// Projection detachment freezes user variables and statement time, rejects
/// `setvar`, and preserves projected rows after session mutation.
#[test]
#[ignore = "go-parity-gap: session variables/time capture and detachable ProjectionExec are unported"]
fn detach_projection_freezes_statement_state() {}

/// Go `pkg/executor/detach_test.go:31::TestDetachExecutor`.
/// `Detach` rejects a generic executor, clones a table reader, rejects an
/// unsupported child, and recursively clones a supported table-reader child
/// (`pkg/executor/detach.go:109-160`).
#[test]
#[ignore = "go-parity-gap: Go-private executor types and the Detach tree-cloning API are unported"]
fn detach_executor_accepts_only_supported_executor_trees() {}
