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

//! Gap tests for Go `pkg/executor/distribute_table_test.go`. The Go cases
//! exercise `DistributeTableExec` (`pkg/executor/distribute.go:40`), the
//! SHOW distribution readers (`pkg/executor/show.go:2207` and `:2621`), and a
//! replaceable PD HTTP client. This Rust crate has no PD scheduler-job
//! executor, distribution virtual table, or SHOW statement session surface.

/// Go `pkg/executor/distribute_table_test.go:86::TestShowDistributionJobs`.
/// `fetchShowDistributionJobs` (`pkg/executor/show.go:2621`) formats mocked
/// PD scheduler jobs and applies `job_id` filters consistently with
/// `SHOW DISTRIBUTION JOB`.
#[test]
#[ignore = "go-parity-gap: PD scheduler-job HTTP mocks and SHOW distribution jobs are unported"]
fn show_distribution_jobs_formats_and_filters_pd_jobs() {}

/// Go `pkg/executor/distribute_table_test.go:125::TestDistributeTable`.
/// `DistributeTableExec::distributeTable` (`pkg/executor/distribute.go:142`)
/// builds PD range-scheduler inputs for full and selected partitions and
/// rejects invalid engine/rule combinations.
#[test]
#[ignore = "go-parity-gap: PD range scheduler creation, partition key ranges, and DISTRIBUTE TABLE are unported"]
fn distribute_table_builds_partition_scheduler_jobs() {}

/// Go `pkg/executor/distribute_table_test.go:227::TestShowTableDistributions`.
/// `fetchShowDistributions` (`pkg/executor/show.go:2207`) queries mocked PD
/// region distributions and renders one row for each table or partition.
#[test]
#[ignore = "go-parity-gap: PD region-distribution HTTP queries and SHOW TABLE DISTRIBUTIONS are unported"]
fn show_table_distributions_renders_table_and_partition_rows() {}

/// Go `pkg/executor/distribute_table_test.go:289::TestCancelDistributionJob`.
/// `CancelDistributionJobExec::Next` (`pkg/executor/distribute.go:225`) maps
/// a missing PD scheduler job to an error and succeeds for an existing job.
#[test]
#[ignore = "go-parity-gap: PD scheduler-job cancellation and session executor errors are unported"]
fn cancel_distribution_job_reports_missing_and_existing_jobs() {}
