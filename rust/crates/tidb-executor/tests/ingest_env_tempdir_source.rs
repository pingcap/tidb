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

//! Port ledger for `pkg/ddl/ingest/env_test.go` (pkg/ddl.part7 items 367-368).
//! Both Go tests exercise the ingest sort-path environment; neither carrier
//! exists on the Rust side, so both are documentary gap ports.

/// GO PORT of `pkg/ddl/ingest/env_test.go:29 TestGenLightningDataDir`.
///
/// Re-derived contract (pkg/ddl/ingest/env.go:94-125): the ingest sort path
/// is `${TempDir}/tmp_ddl-{Port}` — `GetIngestTempDataDir` joins the global
/// config's TempDir with the `/tmp_ddl-` suffix plus the server port, and
/// `GenIngestTempDataDir` MkdirAll's it with mode 0o700 (tolerating an
/// already-existing dir) and returns it. The Go test swaps the global config
/// (TempDir to a temp dir, Port to 5678) and requires exactly
/// `tmpDir + "/tmp_ddl-5678"`.
#[test]
#[ignore = "go-parity-gap: no Rust carrier for ingest GetIngestTempDataDir/GenIngestTempDataDir (pkg/ddl/ingest/env.go:94-125) nor a global config swap hook for TempDir/Port"]
fn gen_ingest_temp_data_dir_formats_temp_dir_tmp_ddl_port() {}

/// GO PORT of `pkg/ddl/ingest/env_test.go:43 TestLitBackendCtxMgr`.
///
/// Re-derived contract (pkg/ddl/ingest/env.go:127-175): `CleanUpTempDir`
/// lists the sort path, decodes each subdirectory name as a backend job tag
/// (`decodeBackendTag`), asks `mysql.tidb_ddl_job` which of those job IDs are
/// still processing, and removes only the directories of non-processing
/// jobs; unreadable/undecodable entries are skipped, and an unknown path is
/// a no-op rather than an error. The Go test seeds `mysql.tidb_ddl_job` with
/// job 100 processing and 101 not, and requires 100's dir to survive until
/// its row is deleted while 101's dir goes away on the second pass.
#[test]
#[ignore = "go-parity-gap: CleanUpTempDir (pkg/ddl/ingest/env.go:127-175) needs a live mysql.tidb_ddl_job session surface and backend-tag encoding, none transcreated"]
fn cleanup_temp_dir_removes_only_dirs_of_non_processing_jobs() {}
