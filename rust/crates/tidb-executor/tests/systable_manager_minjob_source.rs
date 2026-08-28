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

//! Ports of Go `pkg/ddl/systable` (master): `manager_test.go:34
//! TestManager` and `min_job_id_test.go:27 TestRefreshMinJobID`. The
//! `pkg/ddl/systable` package (the system-table manager over
//! `mysql.tidb_ddl_job`/`mysql.tidb_mdl_info`) is not transcreated in this
//! tier, so each test is recorded as an explicit gap with the contract
//! re-derived from the Go source. Nothing is approximated.

/// Go `TestManager` (`pkg/ddl/systable/manager_test.go:34`, subtests
/// GetJobByID / GetMDLVer / GetMinJobID / HasFlashbackClusterJob): the
/// manager reads `mysql.tidb_ddl_job` and `mysql.tidb_mdl_info` --
/// `GetJobByID(9999)` misses with `systable.ErrNotFound` until a job row is
/// inserted, then returns the job with ID 9999; `GetMDLVer` reads the
/// `tidb_mdl_info` version (123) for a job; `GetMinJobID` returns the
/// SMALLEST pending job id (0 on an empty table, 123456 after the insert,
/// still 123456 when asked from a lower bound of 123456, 0 from 123457);
/// `HasFlashbackClusterJob` is true iff a job row with
/// `model.ActionFlashbackCluster` exists outside the exclusion bound (true
/// for bounds 0 and 123, false for 124).
// go-parity-gap: no `pkg/ddl/systable` carrier, no mysql.tidb_ddl_job /
// tidb_mdl_info system tables in this tier.
#[test]
#[ignore]
fn systable_manager_answers_job_mdl_and_min_id_queries() {
}

/// Go `TestRefreshMinJobID` (`pkg/ddl/systable/min_job_id_test.go:27`): the
/// refresher's `refresh` monotonically raises the cached minimum -- a
/// `GetMinJobID(0)` mock returning 1 stores 1; a later call seeded with 1
/// returning 100 stores 100; and a call returning 0 ("all jobs done") does
/// NOT move the cached value backwards (still 100).
// go-parity-gap: no MinJobIDRefresher carrier (`pkg/ddl/systable` not
// transcreated; the Go test drives it through a gomock Manager).
#[test]
#[ignore]
fn min_job_id_refresher_never_moves_backwards() {
}
