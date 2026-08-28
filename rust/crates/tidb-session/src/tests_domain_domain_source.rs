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

//! Port of `pkg/domain/domain_test.go` (origin/master), part 1's share:
//! `TestInfo` (:99), `TestStatWorkRecoverFromPanic` (:225),
//! `TestUpdateExternalWorkloadTTLJobEnableOnlyFromMaster` (:268),
//! `TestShouldStartTTLJobManagerWithExternalWorkloadRole` (:282),
//! `TestLoadSysVarCacheLoopReappliesStmtSummaryInternalQuery` (:336),
//! `TestClosestReplicaReadChecker` (:413), and `TestIsAnalyzeTableSQL`
//! (:621).
//!
//! `TestInfo` is `t.Skip`ped upstream itself ("TestInfo will hang
//! currently") and additionally needs an embedded etcd cluster; it is
//! recorded as skipped-upstream in the batch receipt, not as a Rust test.
//! The rest bind the `Domain` root (`pkg/domain/domain.go`), which is
//! screened and deliberately unported (see `tidb_domain`'s crate doc), so
//! each is a documentary ignored gap.

#![cfg(test)]

/// Go `pkg/domain/domain_test.go:225::TestStatWorkRecoverFromPanic`: with a
/// zero stats lease, `gcStatsWorker` (domain.go:2259) and `autoAnalyzeWorker`
/// (domain.go:2352) panic and are recovered, driving
/// `metrics.PanicCounter{LabelDomain}` to exactly 2; then
/// `GetScope("status")` is `DefaultStatusVarScopeFlag`,
/// `ExpiredTimeStamp4PC` (domain.go:284) round-trips a parsed timestamp, and
/// `isClose` (domain.go:473) flips only after `Close`.
// go-parity-gap: the Domain root and the metrics panic-counter wiring are
// not transcreated.
#[test]
#[ignore = "go-parity-gap: Domain root (stats workers, close lifecycle, \
           metrics) is not transcreated"]
fn stat_work_recover_from_panic() {}

/// Go
/// `pkg/domain/domain_test.go:268::TestUpdateExternalWorkloadTTLJobEnableOnlyFromMaster`:
/// `updateExternalWorkloadTTLJobEnable` (domain_sysvars.go:129) forwards the
/// enable value to the external-workload manager only when its role is
/// `RoleMaster`; a `RoleTTLTaskWorker` manager's `UpdateTTLJobEnable` is
/// never called.
// go-parity-gap: domain_sysvars.go's Domain method and the external
// workload-manager interface are not transcreated.
#[test]
#[ignore = "go-parity-gap: Domain.updateExternalWorkloadTTLJobEnable is not \
           transcreated"]
fn update_external_workload_ttl_job_enable_only_from_master() {}

/// Go
/// `pkg/domain/domain_test.go:282::TestShouldStartTTLJobManagerWithExternalWorkloadRole`:
/// `shouldStartTTLJobManager` (domain.go:2933) is true by default, false
/// with a `RoleMaster` manager, true with a `RoleTTLTaskWorker` manager; and
/// when the manager is nil it falls back to the global config's
/// `ExternalWorkload.Enable`/`Role`, where both master and ttl-worker roles
/// answer false.
// go-parity-gap: Domain.shouldStartTTLJobManager and the external-workload
// config surface are not transcreated.
#[test]
#[ignore = "go-parity-gap: Domain.shouldStartTTLJobManager is not \
           transcreated"]
fn should_start_ttl_job_manager_with_external_workload_role() {}

/// Go
/// `pkg/domain/domain_test.go:336::TestLoadSysVarCacheLoopReappliesStmtSummaryInternalQuery`
/// (regression coverage for issue #69913): `LoadSysVarCacheLoop`
/// (domain.go:1489) and `rebuildSysVarCache` must invoke the
/// `tidb_stmt_summary_internal_query` sysvar's `SetGlobal` callback on EVERY
/// rebuild — even when the applied value stays `OFF` — because the callback
/// runs the internal-statement-summary cleanup path
/// (`stmtsummaryv2.SetEnableInternalQuery(false)`).
// go-parity-gap: the Domain sysvar-cache rebuild loop and Go's mutable
// sysvar-registry entry swap are not transcreated.
#[test]
#[ignore = "go-parity-gap: Domain.LoadSysVarCacheLoop + sysvar registry \
           swap are not transcreated"]
fn load_sys_var_cache_loop_reapplies_stmt_summary_internal_query() {}

/// Go `pkg/domain/domain_test.go:413::TestClosestReplicaReadChecker`:
/// `checkReplicaRead` (domain.go:1024) over a scripted PD client and mocked
/// `GetAllServerInfo`/`GetServerInfo` failpoints enables
/// `tidb_enable_adaptive_replica_read` exactly when some live TiDB server's
/// zone label matches a store's zone label, disables it when none match, and
/// propagates the PD error without touching the flag.
// go-parity-gap: Domain.checkReplicaRead and the serverinfo/infosync
// failpoint seams are not transcreated.
#[test]
#[ignore = "go-parity-gap: Domain.checkReplicaRead is not transcreated"]
fn closest_replica_read_checker() {}

/// Go `pkg/domain/domain_test.go:621::TestIsAnalyzeTableSQL`:
/// `isAnalyzeTableSQL` (domain.go:2465) accepts `analyze table ...` in any
/// case, with surrounding spaces, after a plain/multi-line comment, and
/// directly after a hint comment with no space.
// go-parity-gap: domain.go's isAnalyzeTableSQL helper is not transcreated.
#[test]
#[ignore = "go-parity-gap: domain.go:2465 isAnalyzeTableSQL is not \
           transcreated"]
fn is_analyze_table_sql() {}
