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

//! Port of `pkg/domain/db_test.go` (origin/master): `TestDomainSession`
//! (:40), `TestNormalSessionPool` (:72), `TestAbnormalSessionPool` (:105),
//! `TestTetchAllSchemasWithTables` (:136, the upstream typo for "Fetch"),
//! and `TestFetchAllSchemasWithTablesWithFailpoint` (:166).
//!
//! Every one of them boots a full `session.BootstrapSession(store)` Domain
//! over a mock store and then drives live SQL sessions, the server's
//! internal session pool, or `Domain.FetchAllSchemasWithTables`
//! (test_helper.go:56) over a real meta snapshot. None of that stack —
//! `Domain`, the session bootstrap, the sys-session pool, the meta reader —
//! is transcreated, so each port below is a documentary ignored gap.

#![cfg(test)]

/// Go `pkg/domain/db_test.go:40::TestDomainSession`: a bootstrapped Domain
/// survives `CREATE ROLE` (privilege-cache notify) and a global binding
/// create over an index hint.
// go-parity-gap: needs session.BootstrapSession and the Domain composition
// root (pkg/domain/domain.go), neither of which is transcreated.
#[test]
#[ignore = "go-parity-gap: Domain + session bootstrap are not transcreated"]
fn domain_session() {}

/// Go `pkg/domain/db_test.go:72::TestNormalSessionPool`: after
/// `GlobalInfoSyncerInit` and a `server.NewServer`, a session taken from
/// `Domain.SysSessionPool()` (domain.go:1287) is visible to the server's
/// `InternalSessionExists`, and putting it back hides it again.
// go-parity-gap: needs the Domain root, the sys session pool, and the
// server's session manager — all unported.
#[test]
#[ignore = "go-parity-gap: Domain/sys-session-pool/server session manager \
           are not transcreated"]
fn normal_session_pool() {}

/// Go `pkg/domain/db_test.go:105::TestAbnormalSessionPool`: with the
/// `mockSessionPoolReturnError` failpoint enabled, `pool.Get()` errors and
/// the (nil) session is not registered with the server.
// go-parity-gap: needs the Domain root and the failpoint-injected session
// pool — unported.
#[test]
#[ignore = "go-parity-gap: Domain + failpoint-injected session pool are not \
           transcreated"]
fn abnormal_session_pool() {}

/// Go `pkg/domain/db_test.go:136::TestTetchAllSchemasWithTables`:
/// `FetchAllSchemasWithTables` (test_helper.go:56) over a max-timestamp meta
/// reader reports the 3 bootstrapped databases, and 5 after two more
/// databases (one holding two tables) are created through SQL.
// go-parity-gap: needs a live store/meta snapshot and the Domain root —
// unported.
#[test]
#[ignore = "go-parity-gap: meta reader + Domain root are not transcreated"]
fn tetch_all_schemas_with_tables() {}

/// Go `pkg/domain/db_test.go:166::TestFetchAllSchemasWithTablesWithFailpoint`:
/// same call over 1003 databases succeeds; with the
/// `issyncer/failed-fetch-schemas-with-tables` failpoint enabled the call
/// errors with exactly "failpoint: failed to fetch schemas with tables" and
/// a nil result.
// go-parity-gap: needs the store/meta stack, the Domain root, and the
// issyncer failpoint — unported.
#[test]
#[ignore = "go-parity-gap: store/meta stack + issyncer failpoint are not \
           transcreated"]
fn fetch_all_schemas_with_tables_with_failpoint() {}
