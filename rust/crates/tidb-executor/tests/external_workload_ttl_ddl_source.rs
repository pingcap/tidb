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

//! Port ledger for `pkg/ddl/external_workload_ttl_test.go:173
//! TestExternalWorkloadTTLDDLIntegration` (`pkg/ddl.part6` batch b105, item
//! 321 of the pkg/ddl enumeration).
//!
//! The Go test boots a full session through
//! `BootstrapSessionWithExternalWorkloadManager` with a
//! `recordingExternalWorkloadManager` whose register/delete hooks either
//! record table ids or fail with `context.DeadlineExceeded`, then runs TTL
//! DDL against it. Neither the external-workload manager hook nor the
//! session bootstrap that installs it exists in this tier.

/// GO PORT of `pkg/ddl/external_workload_ttl_test.go:173
/// TestExternalWorkloadTTLDDLIntegration`.
///
/// Re-derived contract, per subtest (external_workload_ttl_test.go:174-300):
/// * "create table registration failure aborts ddl" -- a `CREATE TABLE ...
///   TTL=...` whose external-workload REGISTRATION fails aborts the DDL with
///   the deadline error, the table does not exist afterwards, and the
///   manager recorded no registrations;
/// * "create table with foreign key registers ttl" -- a child table with a
///   FK and a TTL clause registers exactly its own id;
/// * "drop table deletes ttl metadata" -- `DROP TABLE` deletes the recorded
///   TTL registration for the dropped id;
/// * "drop table delete failure aborts ddl" -- a failing DELETE aborts the
///   drop (table still present) with nothing recorded as deleted;
/// * "drop and flashback database sync ttl metadata" -- dropping a database
///   deletes TTL registrations for every table in it (enabled AND
///   TTL_ENABLE='OFF' alike) and empties the active set, while
///   `flashback database` re-registers only the ENABLED tables and keeps
///   each table's `TTL_ENABLE` option in its show-create;
/// * "drop database delete failure restores ttl registrations" -- a failing
///   delete during DROP DATABASE aborts the drop and RESTORES the deleted
///   registrations.
#[test]
#[ignore = "go-parity-gap: BootstrapSessionWithExternalWorkloadManager and the external-workload TTL register/delete hooks around TTL DDL are not transcreated"]
fn external_workload_ttl_ddl_registers_and_deletes_through_the_manager() {}
