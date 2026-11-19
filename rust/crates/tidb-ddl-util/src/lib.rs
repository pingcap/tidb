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

//! Go `pkg/ddl/util/util.go`'s constants: the etcd paths DDL and the
//! domain share, and the session TTL. Go's functions in that package live
//! with their callers' ports.

/// Go `DDLOwnerKey`: the ddl owner path that is saved to etcd.
pub const DDL_OWNER_KEY: &str = "/tidb/ddl/fg/owner";
/// Go `DDLAllSchemaVersions`: the path on etcd that is used to store all
/// servers current schema versions.
pub const DDL_ALL_SCHEMA_VERSIONS: &str = "/tidb/ddl/all_schema_versions";
/// Go `DDLAllSchemaVersionsByJob`: the path on etcd that is used to store
/// all servers current schema versions, by job.
pub const DDL_ALL_SCHEMA_VERSIONS_BY_JOB: &str = "/tidb/ddl/all_schema_by_job_versions";
/// Go `DDLGlobalSchemaVersion`: the path on etcd that is used to store the
/// latest schema versions.
pub const DDL_GLOBAL_SCHEMA_VERSION: &str = "/tidb/ddl/global_schema_version";
/// Go `AddingDDLJobNotifyKey`: the key that notifies a new DDL job.
pub const ADDING_DDL_JOB_NOTIFY_KEY: &str = "/tidb/ddl/add_ddl_job_general";
/// Go `ServerGlobalState`: the path on etcd that is used to store the
/// server global state.
pub const SERVER_GLOBAL_STATE: &str = "/tidb/server/global_state";
/// Go `SessionTTL`: the etcd session's TTL in seconds.
pub const SESSION_TTL: i64 = 90;

/// Go `emulatorGCEnable`: whether the delete-range emulator works off the
/// recorded ranges. The default is enabled; some unit tests stop it.
static EMULATOR_GC_ENABLE: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(1);

/// Go `EmulatorGCEnable`: enables emulator GC. It exports for testing.
pub fn emulator_gc_enable() {
    EMULATOR_GC_ENABLE.store(1, std::sync::atomic::Ordering::SeqCst);
}

/// Go `EmulatorGCDisable`: disables emulator GC. It exports for testing.
pub fn emulator_gc_disable() {
    EMULATOR_GC_ENABLE.store(0, std::sync::atomic::Ordering::SeqCst);
}

/// Go `IsEmulatorGCEnable`: whether emulator GC is enabled. It exports for
/// testing.
#[must_use]
pub fn is_emulator_gc_enable() -> bool {
    EMULATOR_GC_ENABLE.load(std::sync::atomic::Ordering::SeqCst) == 1
}
