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

//! Ports for `pkg/planner/extstore/extstore_test.go` — items 1180–1185 of
//! `pkg/planner.part20` (all 1278 `Test*`/`Benchmark*` declarations under
//! `pkg/planner/` on `origin/master`, sorted by file then line, chunked by
//! 60). `extstore_test.go:31 TestMain` (item 1180) is bootstrap-only
//! (`testsetup.SetupForCommonTest` + goleak ignore list) and is recorded as
//! skipped-reason in the batch receipt.
//!
//! The Go package under test owns external-storage plumbing: `NewExtStorage`
//! (an `objstore.Storage` over `file://` URIs and namespaces) and the
//! global/local storage-root selection (`getLocalPathDirName` probing a
//! writeable `<log-dir>/replayer` subdir; `GetGlobalExtStorage` caching the
//! chosen root as a process-wide store). The Rust workspace has no extstore
//! module — the nearest surface is tidb-domain's `DumpFileStorage` trait
//! boundary (`plan_replayer.rs`), which models only `WalkDir`/`DeleteFile`
//! for plan-replayer GC and none of this package's behavior — so all five
//! tests are documentary `#[ignore]` ports.

/// GO PORT of `pkg/planner/extstore/extstore_test.go:40 TestExtStorage`.
///
/// Contract: `NewExtStorage(ctx, "file://"+tempDir, "test_namespace")`
/// returns a non-nil store whose `URI()` contains both the temp dir and the
/// namespace (:42-50). Full file lifecycle over that store: WriteFile +
/// ReadFile round-trip (`hello world`), FileExists true, WalkDir reports the
/// file with its exact byte size, DeleteFile + FileExists false, the
/// Create/Write/Close writer path persisted as `test writer`, the Open/Read
/// reader path returning those 11 bytes, Rename moving the file, and
/// DeleteFiles removing a batch of two (:52-124). A second `URI()` read
/// still contains tempDir + namespace (:126-129), and Close releases the
/// store (:131).
///
/// go-parity-gap: no Rust extstore module exists (NewExtStorage/objstore
/// unported).
#[test]
#[ignore = "go-parity-gap: extstore NewExtStorage file-store lifecycle has no Rust module"]
fn ext_storage_file_roundtrip_and_lifecycle() {}

/// GO PORT of `pkg/planner/extstore/extstore_test.go:142
/// TestGetLocalPathDirNameWithWritePerm`.
///
/// Contract: with global config `Log.File.Filename = /var/log/tidb/tidb.log`
/// and `TempDir = /tmp/tidb`, over an in-memory FS where
/// `/var/log/tidb/replayer` exists and is writable, `getLocalPathDirName`
/// returns `/var/log/tidb` — the log dir wins as storage root when its
/// replayer subdir is writable (:148-163).
///
/// go-parity-gap: getLocalPathDirName + the afero mem-FS probing surface are
/// unported.
#[test]
#[ignore = "go-parity-gap: extstore local-path selection has no Rust module"]
fn get_local_path_dir_name_prefers_writable_log_dir() {}

/// GO PORT of `pkg/planner/extstore/extstore_test.go:162
/// TestGetLocalPathDirNameWithoutWritePerm`.
///
/// Contract: same config, but the FS is wrapped read-only
/// (`afero.NewReadOnlyFs`), so the log dir cannot be written; the function
/// falls back to exactly `config.GetGlobalConfig().TempDir` (:168-182).
///
/// go-parity-gap: same unported surface.
#[test]
#[ignore = "go-parity-gap: extstore local-path fallback has no Rust module"]
fn get_local_path_dir_name_falls_back_to_temp_dir() {}

/// GO PORT of `pkg/planner/extstore/extstore_test.go:182
/// TestGetGlobalExtStorageWithWritePerm`.
///
/// Contract: with `CloudStorageURI` empty and the config log dir pointing
/// into a writable temp-tree (`<tmp>/log/tidb.log`), after resetting the
/// global store (`SetGlobalExtStorageForTest(nil)` + clearing
/// `testLocalPathFS`), `GetGlobalExtStorage(ctx)` returns a live store whose
/// URI contains `<tmp>/log` — the log dir is used as the global external
/// storage root when writable (:184-220).
///
/// go-parity-gap: GetGlobalExtStorage process-wide cache has no Rust module.
#[test]
#[ignore = "go-parity-gap: extstore global storage root has no Rust module"]
fn get_global_ext_storage_uses_log_dir_when_writable() {}

/// GO PORT of `pkg/planner/extstore/extstore_test.go:221
/// TestGetGlobalExtStorageWithoutWritePerm`.
///
/// Contract: same reset, but the FS is read-only; `GetGlobalExtStorage(ctx)`
/// returns a store whose URI contains `config.GetGlobalConfig().TempDir` —
/// the temp dir is the fallback global storage root (:223-260).
///
/// go-parity-gap: same unported surface.
#[test]
#[ignore = "go-parity-gap: extstore global storage fallback has no Rust module"]
fn get_global_ext_storage_falls_back_to_temp_dir() {}
