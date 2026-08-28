// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap tests for the checksum/post-processing halves of Go
//! `pkg/executor/importer/importer_testkit_test.go` plus
//! `GetTargetNodeCPUCnt`. Go source: `pkg/executor/importer/table_import.go`
//! and `import.go`.

/// Go `pkg/executor/importer/importer_testkit_test.go:68::TestVerifyChecksum`:
/// `VerifyChecksum` (`table_import.go:932`) skips the remote check when the
/// local checksum already matches, otherwise runs
/// `RemoteChecksumTableBySQL` (:972, `ADMIN CHECKSUM TABLE` via the session)
/// and fails with `common.ErrChecksumMismatch` on mismatch, restores the
/// session's dist-SQL scan concurrency, honors cancellation of a slow remote
/// checksum, and is a no-op when the plan's checksum is `off`.
#[test]
#[ignore = "go-parity-gap: VerifyChecksum/RemoteChecksumTableBySQL (table_import.go:932/:972) need a session + ADMIN CHECKSUM executor; unported"]
fn import_verify_checksum_compares_local_and_remote_and_restores_concurrency() {}

/// Go `pkg/executor/importer/importer_testkit_test.go:178::TestGetTargetNodeCpuCnt`
/// (classic only): `GetTargetNodeCPUCnt` (`import.go:2029`) returns the mocked
/// CPU count for query sources and for file sources with dist-task disabled,
/// rejects an invalid S3 path with `ErrLoadDataInvalidURI`, and with dist-task
/// enabled returns the DXF node resource's CPU count
/// (`storage.SetNodeResource`).
#[test]
#[ignore = "go-parity-gap: GetTargetNodeCPUCnt (import.go:2029) needs the DXF node-resource registry and cpu failpoint; unported"]
fn import_target_node_cpu_count_follows_dist_task_mode() {}

/// Go `pkg/executor/importer/importer_testkit_test.go:217::TestPostProcess`:
/// `PostProcess` (`table_import.go:848`) verifies the per-group KV checksum
/// (failing with `common.ErrChecksumMismatch` before any write), then
/// verifies the table, updates mysql.stats_meta through ANALYZE-style
/// bookkeeping, and removes the import sort directory on success.
#[test]
#[ignore = "go-parity-gap: PostProcess (table_import.go:848) needs the session, stats, and checksum-group machinery; unported"]
fn import_post_process_verifies_group_checksum_before_cleanup() {}
