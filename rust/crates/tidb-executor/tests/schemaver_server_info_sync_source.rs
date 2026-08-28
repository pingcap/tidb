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

//! Ports of the `pkg/ddl/schemaver` (master) slice owned by this batch.
//!
//! Carrier status, precisely: the workspace crate `tidb-schemaver` IS a
//! whole-crate port of `pkg/ddl/schemaver`'s `syncer.go`/`mem_syncer.go`
//! (`rust/crates/tidb-schemaver/src/lib.rs` mapping table), and its in-crate
//! tests already pin five of the seven Go tests in this slice:
//!
//! | Go test (master) | tidb-schemaver in-crate test |
//! | --- | --- |
//! | `pkg/ddl/schemaver/syncer_nokit_test.go:36::TestNodeVersions` | `test_node_versions` (`src/etcd_syncer.rs:1333`) |
//! | `pkg/ddl/schemaver/syncer_nokit_test.go:68::TestDecodeJobVersionEvent` | `test_decode_job_version_event` (`src/etcd_syncer.rs:1370`) |
//! | `pkg/ddl/schemaver/syncer_nokit_test.go:92::TestSyncJobSchemaVerLoop` | `test_sync_job_schema_ver_loop` (`src/etcd_syncer.rs:1389`) |
//! | `pkg/ddl/schemaver/syncer_test.go:42::TestSyncerSimple` | `test_syncer_simple` (`src/etcd_syncer.rs:1488`) |
//! | `pkg/ddl/schemaver/syncer_test.go:168::TestPutKVToEtcdMono` | `test_put_kv_to_etcd_mono` (`src/etcd_syncer.rs:1639`) |
//!
//! `tidb-schemaver` is not a dependency of this gate crate, and the two
//! remaining Go tests exercise symbols that no Rust crate carries yet, so
//! they are recorded below as `#[ignore]` gaps with the contracts re-derived
//! from the Go source. Nothing is approximated.

/// Go `TestCalculateUpdatedMap`
/// (`pkg/ddl/schemaver/syncer_nokit_test.go:198`): `calculateUpdatedMap`
/// classifies the server-info map for an owner sync -- all-fresh servers
/// (distinct IPs, no assumed keyspace) give `len == 3` and
/// `SyncSummary{ServerCount: 3}`; an `AssumedKeyspace: "a"` entry still
/// counts as a server but is ALSO counted in `AssumedServerCount`; servers
/// sharing one IP with distinct `StartTimestamp`s collapse to the newest
/// (`len == 1`), and a stale-and-assumed mix keeps `AssumedServerCount: 1`
/// only when the surviving entry is assumed (the last row:
/// b assumed collapses into a, summary is `{1, 0}`).
// go-parity-gap: `calculateUpdatedMap`/`SyncSummary` are not transcreated
// anywhere in this workspace (`grep calculate_updated rust/crates` is
// empty); master added them after the snapshot `tidb-schemaver` was ported
// from.
#[test]
#[ignore]
fn schemaver_calculate_updated_map_classifies_and_collapses_servers() {
}

/// Go `TestGetServersForISSync`
/// (`pkg/ddl/schemaver/syncer_nokit_test.go:232`): with a mocked
/// `domain/serverinfo` store holding two `ks1` servers and one assumed-
/// into-system keyspace server, `getServersForISSync(ctx, false)` returns
/// the CLASSIC shape (all 3) while NEXTGEN returns only the 2 real ones
/// (`IsAssumed()` false for each); with `checkAssumedSvr = true` all 3 come
/// back. The test constructs a CLASSIC or NEXTGEN syncer per
/// `kerneltype.IsClassic()`.
// go-parity-gap: `getServersForISServer`/`serverinfo.Syncer`/the kernel-type
// switch and the `mockGetAllServerInfo` failpoint seam are not transcreated.
#[test]
#[ignore]
fn schemaver_get_servers_for_is_sync_filters_assumed_keyspaces() {
}
