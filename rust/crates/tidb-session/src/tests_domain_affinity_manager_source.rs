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

//! Port of `pkg/domain/affinity/manager_test.go` (origin/master): the eleven
//! tests of the PD-affinity manager, `pdManager` (manager.go:38) —
//! `CreateAffinityGroupsIfNotExists` (manager.go:59), `GetAffinityGroups`
//! (manager.go:84), and the pure helpers `shouldUseGetAllAffinityGroups`
//! (manager.go:140), `affinityGroupIDsEscapedQueryLen` (manager.go:144),
//! `isPDHTTPStatusError` (manager.go:173) with the caps
//! `maxAffinityGroupIDsQueryLen = 4096` / `maxAffinityGroupIDsCount = 100`
//! (manager.go:45-47).
//!
//! The package is a PD HTTP client wrapper with no Rust home yet, so every
//! port below is a documentary ignored gap. The contracts they pin, for the
//! batch that lands the transcreation:
//!
//! - create-if-not-exists sends `skipExistCheck` (one option) and NEVER
//!   probes with `GetAffinityGroups` when the create succeeds;
//! - a 409 from the create falls back to `GetAffinityGroups(ids)` and
//!   re-creates only the MISSING groups with no options;
//! - an `ErrHTTPServiceError` terrified as a domain-class error also falls
//!   back the same way, but any other status (e.g. 500) propagates
//!   unchanged with no fallback;
//! - `GetAffinityGroups` filters the response to the REQUESTED ids (the
//!   mock returning extra groups must not leak them);
//! - when the requested ids' URL-escaped query would exceed 4096 (1365
//!   `/` chars, each escaping to `%2F`), the call falls back to
//!   `GetAllAffinityGroups` and filters locally;
//! - a 400 (unsupported ids query) and a service error both fall back to
//!   `GetAllAffinityGroups` + local filtering.

#![cfg(test)]

/// Go
/// `pkg/domain/affinity/manager_test.go:64::TestCreateAffinityGroupsIfNotExistsUseSkipExistCheck`.
// go-parity-gap: pkg/domain/affinity is not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/affinity is not transcreated"]
fn create_affinity_groups_if_not_exists_use_skip_exist_check() {}

/// Go
/// `pkg/domain/affinity/manager_test.go:79::TestCreateAffinityGroupsIfNotExistsFallbackWhenSkipExistRejected`.
// go-parity-gap: pkg/domain/affinity is not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/affinity is not transcreated"]
fn create_affinity_groups_if_not_exists_fallback_when_skip_exist_rejected() {}

/// Go
/// `pkg/domain/affinity/manager_test.go:100::TestCreateAffinityGroupsIfNotExistsFallbackForHTTPServiceError`.
// go-parity-gap: pkg/domain/affinity is not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/affinity is not transcreated"]
fn create_affinity_groups_if_not_exists_fallback_for_http_service_error() {}

/// Go
/// `pkg/domain/affinity/manager_test.go:121::TestCreateAffinityGroupsIfNotExistsDoNotFallbackForNonCompatibilityError`.
// go-parity-gap: pkg/domain/affinity is not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/affinity is not transcreated"]
fn create_affinity_groups_if_not_exists_do_not_fallback_for_non_compatibility_error() {}

/// Go
/// `pkg/domain/affinity/manager_test.go:138::TestGetAffinityGroupsFilterDirectResponse`.
// go-parity-gap: pkg/domain/affinity is not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/affinity is not transcreated"]
fn get_affinity_groups_filter_direct_response() {}

/// Go
/// `pkg/domain/affinity/manager_test.go:161::TestGetAffinityGroupsFallbackByEscapedQueryLen`.
// go-parity-gap: pkg/domain/affinity is not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/affinity is not transcreated"]
fn get_affinity_groups_fallback_by_escaped_query_len() {}

/// Go
/// `pkg/domain/affinity/manager_test.go:183::TestGetAffinityGroupsFallbackWhenIDsQueryUnsupported`.
// go-parity-gap: pkg/domain/affinity is not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/affinity is not transcreated"]
fn get_affinity_groups_fallback_when_ids_query_unsupported() {}

/// Go
/// `pkg/domain/affinity/manager_test.go:205::TestGetAffinityGroupsFallbackForHTTPServiceError`.
// go-parity-gap: pkg/domain/affinity is not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/affinity is not transcreated"]
fn get_affinity_groups_fallback_for_http_service_error() {}

/// Go
/// `pkg/domain/affinity/manager_test.go:227::TestAffinityGroupIDsEscapedQueryLenBoundary`:
/// 1364 `/` ids escape to exactly `maxAffinityGroupIDsQueryLen` (4096); 1365
/// of them to 4099 (`len + 3`).
// go-parity-gap: pkg/domain/affinity is not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/affinity is not transcreated"]
fn affinity_group_ids_escaped_query_len_boundary() {}

/// Go
/// `pkg/domain/affinity/manager_test.go:232::TestShouldUseGetAllAffinityGroupsByIDCount`:
/// `maxAffinityGroupIDsCount + 1` (101) ids force the get-all form.
// go-parity-gap: pkg/domain/affinity is not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/affinity is not transcreated"]
fn should_use_get_all_affinity_groups_by_id_count() {}

/// Go
/// `pkg/domain/affinity/manager_test.go:240::TestIsPDHTTPStatusErrorMatchByCodeOnly`:
/// the matcher reads the numeric status out of the PD error text, so a
/// mangled status TEXT still matches 400 and does not match 409.
// go-parity-gap: pkg/domain/affinity is not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/affinity is not transcreated"]
fn is_pd_http_status_error_match_by_code_only() {}
