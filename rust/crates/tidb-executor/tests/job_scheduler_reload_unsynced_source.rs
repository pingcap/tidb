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

//! Port ledger for `pkg/ddl/job_scheduler_test.go` (pkg/ddl.part7 items
//! 401-402 of the local enumeration). Both Go tests are gomock unit tests
//! over the owner-side scheduler internals; the crate carries no scheduler,
//! so both are documentary gap ports.

/// GO PORT of `pkg/ddl/job_scheduler_test.go:36 TestMustReloadSchemas`.
///
/// Re-derived contract (pkg/ddl/job_scheduler.go:460-473): `mustReloadSchemas`
/// calls `schemaLoader.Reload()` in a loop — returning on the first nil
/// error, warning-and-retrying on error with `schedulerLoopRetryInterval`
/// between attempts (the test shrinks it to 10ms), and returning when the
/// scheduler context is cancelled while waiting. The Go mock scripts: direct
/// success; one error then success; and an error whose `Do` cancels the
/// context, requiring the cancel to win the retry loop.
#[test]
#[ignore = "go-parity-gap: jobScheduler.mustReloadSchemas (pkg/ddl/job_scheduler.go:460-473) and its MockSchemaLoader have no Rust carrier"]
fn must_reload_schemas_retries_until_success_or_context_cancel() {}

/// GO PORT of `pkg/ddl/job_scheduler_test.go:65 TestUnSyncedJobTracker`.
///
/// Re-derived contract (pkg/ddl/ddl.go:293-320): `newUnSyncedJobTracker`
/// starts empty; `addUnSynced(1)` makes `isUnSynced(1)` true;
/// `removeUnSynced(1)` makes it false again — a job-ID set guarded by an
/// RWMutex (plus a `onceMap` for first-run-on-owner detection that the test
/// does not touch).
#[test]
#[ignore = "go-parity-gap: unSyncedJobTracker (pkg/ddl/ddl.go:293-320) has no Rust carrier; no transcreated DDL job-sync state exists"]
fn un_synced_job_tracker_tracks_job_ids_until_removed() {}
