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

//! Source-backed carriers for the final 11-test `pkg/session.part6` slice.
//!
//! These Go tests exercise private TiDB session/domain, DistSQL resource-control,
//! RU-v2, storage-bootstrap, binding-upgrade, and upgrade-registry seams. The
//! current Rust `tidb-session` API does not expose those complete compositions,
//! so each declaration is retained as an explicit ignored parity carrier rather
//! than approximated with a different behavior.

#![cfg(test)]

/// `pkg/session/tidb_test.go:33::TestDomapHandleNil`.
///
/// The test checks the nil-store branch of `domainMap.Get` in
/// `pkg/session/tidb.go:68-89`, including the enterprise-plugin no-panic contract.
#[test]
#[ignore = "go-parity-gap: Go domainMap nil-store handling is private and the Rust session has no Domain map equivalent"]
fn test_domap_handle_nil() {}

/// `pkg/session/tidb_test.go:41::TestSysSessionPoolGoroutineLeak`.
///
/// The test exercises the Go internal-session pool at
/// `pkg/session/session.go:1298` and restricted execution cleanup, including the
/// async timestamp-worker lifecycle.
// go-parity-gap: Go sysSessionPool/restricted-session worker cleanup is not exposed by Rust tidb-session.
#[test]
#[ignore = "go-parity-gap: Go sysSessionPool and async timestamp-worker lifecycle are not transcreated"]
fn test_sys_session_pool_goroutine_leak() {}

/// `pkg/session/tidb_test.go:70::TestRUV2SessionParserTotalDoesNotLeakAcrossStandaloneParse`.
///
/// The test pins parser pending-count reset and transfer at
/// `pkg/session/session.go:1981-2038`, RU-v2 statement assignment at
/// `pkg/session/session.go:2450-2458`, bypass classification at
/// `pkg/session/session.go:2768-2792`, and restricted-session restoration at
/// `pkg/session/session.go:2233-2249`.
// go-parity-gap: Go RU-v2 metrics, internal-source contexts, prepared ANALYZE bypass, and restricted-session restoration are not transcreated.
#[test]
#[ignore = "go-parity-gap: Go RU-v2 session accounting and internal execution contexts are not transcreated"]
fn test_ruv2_session_parser_total_does_not_leak_across_standalone_parse() {}

/// `pkg/session/tidb_test.go:153::TestCrossKSSessionDistSQLCtxDoesNotExposeTypedNilRUReporter`.
///
/// The test checks the cross-keyspace Domain branch in
/// `pkg/session/session.go:3453-3465` while constructing the DistSQL context.
// go-parity-gap: Go cross-keyspace Domain and DistSQL RU reporter wiring are not exposed by Rust tidb-session.
#[test]
#[ignore = "go-parity-gap: Go cross-keyspace Domain/DistSQL RU reporter wiring is not transcreated"]
fn test_cross_ks_session_dist_sql_ctx_does_not_expose_typed_nil_ru_reporter() {}

/// `pkg/session/tidb_test.go:167::TestDistSQLCtxPagingSizeBytesRequiresHardCappedResourceGroup`.
///
/// The test pins the resource-control and burst-limit checks in
/// `pkg/session/session.go:3467-3473`, reached through the DistSQL context
/// construction at `pkg/session/session.go:3453-3550`.
// go-parity-gap: Go resource-group catalog state and DistSQL paging-size policy are not transcreated.
#[test]
#[ignore = "go-parity-gap: Go resource-control resource groups and DistSQL paging policy are not transcreated"]
fn test_dist_sql_ctx_paging_size_bytes_requires_hard_capped_resource_group() {}

/// `pkg/session/tidb_test.go:199::TestRUV2MetricsIsolatedPerStatementInExplicitTxn`.
///
/// The test checks that each statement receives fresh RU-v2 metrics through
/// `pkg/session/session.go:2450-2458`, including explicit-transaction statement
/// execution and failpoint-controlled retry accounting.
// go-parity-gap: Go RU-v2 metric identity and TiKV failpoint-driven retry accounting are not transcreated.
#[test]
#[ignore = "go-parity-gap: Go RU-v2 per-statement metric identity and TiKV retry failpoints are not transcreated"]
fn test_ruv2_metrics_isolated_per_statement_in_explicit_txn() {}

/// `pkg/session/upgrade_backfill_test.go:32::TestUpgradeToVer259BackfillsIgnoreInlistPlanDigest`.
///
/// The test drives the versioned bootstrap chain into
/// `pkg/session/upgrade_def.go:2140-2142` and verifies the persisted global
/// variable after reboot.
// go-parity-gap: Go versioned BootstrapSession and persisted global-variable backfill are not transcreated.
#[test]
#[ignore = "go-parity-gap: Go versioned bootstrap and global-variable persistence are not transcreated"]
fn test_upgrade_to_ver259_backfills_ignore_inlist_plan_digest() {}

/// `pkg/session/upgrade_backfill_test.go:97::TestUpgradeToVer262RefreshesBindingDigest`.
///
/// The test drives persisted binding rows through the normalization, duplicate,
/// invalid-row, and cleanup logic in `pkg/session/upgrade_def.go:2165-2265`.
// go-parity-gap: Go bind_info storage, binding normalization, and versioned upgrade execution are not transcreated.
#[test]
#[ignore = "go-parity-gap: Go bind_info storage and upgradeToVer262 binding-digest migration are not transcreated"]
fn test_upgrade_to_ver262_refreshes_binding_digest() {}

/// `pkg/session/upgrade_backfill_test.go:247::TestUpgradeToVer261BackfillsDefaultStringMatchSelectivity`.
///
/// The test drives the versioned bootstrap chain into
/// `pkg/session/upgrade_def.go:2148-2150` and verifies the persisted default.
// go-parity-gap: Go versioned BootstrapSession and persisted global-variable backfill are not transcreated.
#[test]
#[ignore = "go-parity-gap: Go versioned bootstrap and global-variable persistence are not transcreated"]
fn test_upgrade_to_ver261_backfills_default_string_match_selectivity() {}

/// `pkg/session/upgrade_backfill_test.go:312::TestUpgradeToVer263BackfillsAnalyzeDefaultOptions`.
///
/// The test drives the versioned bootstrap chain into
/// `pkg/session/upgrade_def.go:2267-2271` and verifies both persisted defaults.
// go-parity-gap: Go versioned BootstrapSession and persisted global-variable backfill are not transcreated.
#[test]
#[ignore = "go-parity-gap: Go versioned bootstrap and global-variable persistence are not transcreated"]
fn test_upgrade_to_ver263_backfills_analyze_default_options() {}

/// `pkg/session/upgrade_test.go:52::TestUpgradeToVerFunctionsCheck`.
///
/// The test validates the private ordered registry at
/// `pkg/session/upgrade_def.go:530-536,700-720`, including function names and
/// the `currentBootstrapVersion` endpoint.
// go-parity-gap: Rust does not expose Go's private versioned upgrade-function registry or function identity.
#[test]
#[ignore = "go-parity-gap: Go private upgradeToVerFunctions registry and function identity are not transcreated"]
fn test_upgrade_to_ver_functions_check() {}
