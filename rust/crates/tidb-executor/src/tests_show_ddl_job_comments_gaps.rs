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

//! Gap tests for Go `pkg/executor/show_ddl_jobs_test.go` (items 590-591).
//! Both test pure helpers of the SHOW ADMIN DDL JOBS renderer:
//! `showCommentsFromJob` (pkg/executor/show_ddl_jobs.go:302) and
//! `showCommentsFromSubjob` (:362). Neither is transcreated on this tier —
//! the crate's `ddl_running_jobs.rs` ports the job SCHEDULER's lock table,
//! not the SHOW renderer — and the Go tests self-skip on the nextgen
//! kernel (`kerneltype.IsNextGen()`), while this snapshot builds classic.

/// Go `pkg/executor/show_ddl_jobs_test.go:26::TestShowCommentsFromJob`:
/// `showCommentsFromJob` composes the Comments column — empty without
/// ReorgMeta; analyzing/analyze_failed/analyze_timeout labels from
/// `AnalyzeState`; for adding-index jobs the reorg type label (`txn`,
/// `txn-merge`, `ingest`) plus `DXF`/`cloud` when dist-reorg/cloud; for any
/// reorg job the non-default tuning labels `thread=`, `batch_size=`,
/// `max_write_speed=`, `service_scope=`, `max_node_count=` in that order,
/// omitting values equal to the vardef defaults.
#[test]
#[ignore = "go-parity-gap: showCommentsFromJob (pkg/executor/show_ddl_jobs.go:302) is not transcreated; the SHOW ADMIN DDL JOBS renderer is out of this tier's scope"]
fn show_comments_from_job_composes_reorg_labels_in_order() {}

/// Go
/// `pkg/executor/show_ddl_jobs_test.go:115::TestShowCommentsFromSubJob`:
/// `showCommentsFromSubjob` returns empty for `ReorgTypeNone`, otherwise
/// the reorg-type label plus `DXF` only when useDXF, plus `cloud` only when
/// BOTH useDXF and useCloud (`ingest` / `ingest, DXF` / `ingest, DXF,
/// cloud` / plain `ingest` for useCloud without DXF).
#[test]
#[ignore = "go-parity-gap: showCommentsFromSubjob (pkg/executor/show_ddl_jobs.go:362) is not transcreated"]
fn show_comments_from_subjob_appends_dxf_and_cloud_flags() {}
