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

//! Port ledger for the job-version half of `pkg/ddl/ddl_test.go`
//! (`pkg/ddl.part6` batch b105, items 303-304 of the pkg/ddl enumeration).
//!
//! Both Go tests drive process-global switches that the DDL owner flips from
//! cluster observation; the switches themselves ARE transcreated
//! (`tidb_model::job_enums::{get_job_ver_in_use, set_job_ver_in_use}`,
//! `tidb_model::index::{get,set}_global_index_v1_supported`), but the
//! OBSERVER that decides them -- `d.detectAndUpdateJobVersion`
//! (pkg/ddl/ddl.go:975-1021) with `detectAndUpdateJobVersionOnce`
//! (ddl.go:1023) polling the cluster's `ServerInfo` versions through the
//! `domain/serverinfo/mockGetAllServerInfo` failpoint -- is not, so the tests
//! have no carrier here. The kernel-type side conditions are separately
//! pinned by `crates/tidb-metadef/tests/model_parity_source.rs:191
//! global_index_v1_supported_for_next_gen` and `:199
//! job_ver_in_use_matches_kernel_type` (ports of pkg/meta/model's own tests).

/// GO PORT of `pkg/ddl/ddl_test.go:475 TestDetectAndUpdateJobVersion`.
///
/// Re-derived contract (ddl.go:975-1059): `detectAndUpdateJobVersion` starts
/// from `JobVersion1` + `GlobalIndexV1Supported=false`, then, in the UT
/// address space without the v1 force-args flag, promotes the in-use version
/// to `JobVersion2` and sets the global-index-v1 flag. With live server
/// infos: every node at >= 8.4.0 promotes the job version; the global-index
/// flag additionally needs >= 8.5.6; an unknown version, an invalid version,
/// a pre-8.4.0 node, or a mixed upgrade fleet keeps v1 and keeps the flag
/// off (the seven-iteration background loop of ddl_test.go:537-573, driven
/// through the `afterDetectAndUpdateJobVersionOnce` failpoint).
#[test]
#[ignore = "go-parity-gap: detectAndUpdateJobVersion/detectAndUpdateJobVersionOnce (pkg/ddl/ddl.go:975-1059) and the serverinfo polling they read are not transcreated, so the version/flag observer has no Rust carrier"]
fn detect_and_update_job_version_promotes_by_cluster_versions() {}

/// GO PORT of `pkg/ddl/ddl_test.go:586 TestSetGlobalIndexVersionFlag`.
///
/// Re-derived contract (pkg/ddl/index.go:358-384): `setGlobalIndexVersion`
/// stamps `idxInfo.GlobalIndexVersion = 0` and, when the process-global
/// `GetGlobalIndexV1Supported()` is off, stops there. With the flag ON, a
/// GLOBAL index on a NON-clustered table gets `GlobalIndexVersionV1` (1)
/// exactly when the key needs the partition id in it: a non-unique index
/// always, a unique index when any of its columns is NULL-able
/// (`getNullColInfos`, index.go:368-375). The versioned flag decides
/// `GenIndexKey`'s key encoding for global indexes.
#[test]
#[ignore = "go-parity-gap: setGlobalIndexVersion (pkg/ddl/index.go:358-384) is not transcreated -- no Rust DDL path stamps IndexInfo.global_index_version yet"]
fn set_global_index_version_stamps_v1_by_clustered_and_nullability() {}
