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

//! Port of the `pkg/domain/infosync` package tests (origin/master) that
//! belong to part 1: `info_test.go`'s `TestPutBundlesRetry` (:53),
//! `TestTiFlashManager` (:116), `TestInfoSyncerMarshal` (:216),
//! `TestSetKeyspaceConfig` (:279),
//! `TestSetKeyspaceConfigWithoutPDHTTPClient` (:312), and
//! `TestSetKeyspaceConfigPropagatesPDHTTPError` (:321), plus
//! `label_manager_test.go`'s `TestFilterLabelRulesByKeyspace` (:27). The two
//! `TestMain`s (:41, and globalconfigsync's) are goleak harnesses recorded
//! as skipped-reason.
//!
//! `TestInfoSyncerMarshal` is ported for real in
//! [`crate::tests_domain_serverinfo_info_source`] — it pins the wire format
//! of `pkg/domain/serverinfo/info.go`, which IS transcreated. The rest of
//! `info_test.go` drives `GlobalInfoSyncerInit` (info.go:152),
//! `GetRuleBundle` (info.go:480), `SetKeyspaceConfig` (info.go:343), and the
//! TiFlash manager (`MustGetTiFlashProgressWithCircuitBreaker`
//! info.go:391) — the whole `infosync` package is an unported PD/etcd
// surface — and `filterRulesByKeyspace` (label_manager.go:140) belongs to
//! the same package.

#![cfg(test)]

/// Go `pkg/domain/infosync/info_test.go:53::TestPutBundlesRetry`: after
/// `GlobalInfoSyncerInit`, putting a placement bundle retries until PD
/// accepts, and a later `GetRuleBundle(info.go:480)` of a never-stored id
/// answers an empty bundle without error.
// go-parity-gap: the infosync package (PD HTTP placement-bundle client) is
// not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/infosync is not transcreated"]
fn put_bundles_retry() {}

/// Go `pkg/domain/infosync/info_test.go:116::TestTiFlashManager`: over a
/// mock TiFlash + PD HTTP client, placement rules set for a table come back
/// from `GetTiFlashGroupRules("tiflash")` with the
/// `table-<id>-r`/count/labels shape, `ConfigureTiFlashPDForTable` and
/// `ConfigureTiFlashPDForPartitions` accumulate rules per table/partition,
/// and the circuit-breaker subtest requires a hung progress HTTP request to
/// be CANCELED when the breaker fires.
// go-parity-gap: the infosync TiFlash manager is not transcreated.
#[test]
#[ignore = "go-parity-gap: infosync TiFlash manager is not transcreated"]
fn tiflash_manager() {}

/// Go `pkg/domain/infosync/info_test.go:279::TestSetKeyspaceConfig`:
/// `SetKeyspaceConfig` forwards the config/precondition maps verbatim to
/// `UpdateKeyspaceConfig` on the PD HTTP client, keyed by the keyspace name.
// go-parity-gap: infosync's SetKeyspaceConfig (info.go:343) is not
// transcreated.
#[test]
#[ignore = "go-parity-gap: infosync SetKeyspaceConfig is not transcreated"]
fn set_keyspace_config() {}

/// Go
/// `pkg/domain/infosync/info_test.go:312::TestSetKeyspaceConfigWithoutPDHTTPClient`:
/// with no PD HTTP client configured, `SetKeyspaceConfig` errors with
/// "pd http cli is nil".
// go-parity-gap: infosync's SetKeyspaceConfig (info.go:343) is not
// transcreated.
#[test]
#[ignore = "go-parity-gap: infosync SetKeyspaceConfig is not transcreated"]
fn set_keyspace_config_without_pd_http_client() {}

/// Go
/// `pkg/domain/infosync/info_test.go:321::TestSetKeyspaceConfigPropagatesPDHTTPError`:
/// an error returned by the PD HTTP client propagates out of
/// `SetKeyspaceConfig` unchanged.
// go-parity-gap: infosync's SetKeyspaceConfig (info.go:343) is not
// transcreated.
#[test]
#[ignore = "go-parity-gap: infosync SetKeyspaceConfig is not transcreated"]
fn set_keyspace_config_propagates_pd_http_error() {}

/// Go
/// `pkg/domain/infosync/label_manager_test.go:27::TestFilterLabelRulesByKeyspace`:
/// `filterRulesByKeyspace` (label_manager.go:140) keeps every rule under a
/// V1 codec; under a V2 codec for keyspace 42 it keeps only rules whose ID
/// carries the `keyspace/42/` prefix (and, on the classic kernel, keeps all
// of them).
// go-parity-gap: infosync's label-manager filter + pkg/ddl/label.Rule are
// not transcreated.
#[test]
#[ignore = "go-parity-gap: infosync filterRulesByKeyspace is not \
           transcreated"]
fn filter_label_rules_by_keyspace() {}
