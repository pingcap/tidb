// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap tests for Go `pkg/executor/importer/table_import_test.go` and the
//! select-source cleanup half of `table_import_testkit_test.go`: local-sort
//! directory management, subtask planning math, and the PD region-split size
//! fetch. Go source: `pkg/executor/importer/table_import.go`.

/// Go `pkg/executor/importer/table_import_test.go:35::TestPrepareSortDir`:
/// `prepareSortDir` (`table_import.go:133`) returns
/// `<TempDir>/import-<port>/<id>`, creating the parent when missing, replacing
/// a file that occupies the parent path, leaving both intact when they exist,
/// and REMOVING a stale same-named sort dir before recreating it.
#[test]
#[ignore = "go-parity-gap: prepareSortDir (table_import.go:133) and GetImportRootDir (table_import.go:1073) are unported"]
fn import_prepare_sort_dir_layout_and_stale_dir_removal() {}

/// Go `pkg/executor/importer/table_import_test.go:89::TestCalculateSubtaskCnt`:
/// `calculateSubtaskCnt` (`table_import.go:429`) is
/// `ceil(TotalRealSize/MaxEngineSize)` without cloud storage (bounded by
/// execute-node count + 1: 749/500/3 -> 1, 750/500/4 -> 2, 100/30/7 -> 3) and
/// one subtask per engine-size slice per node WITH cloud storage (500/500/2 ->
/// 2, 1250/500/6 -> 6, 100/30/2 -> 4, 400/99/3 -> 6, 500/100/5 -> 5).
#[test]
#[ignore = "go-parity-gap: calculateSubtaskCnt (table_import.go:429) is unported"]
fn import_calculate_subtask_count_matches_cloud_and_local_slicing() {}

/// Go `pkg/executor/importer/table_import_test.go:135::TestCalculateSubtaskCntUsesRealSizeNotFileSize`:
/// regression pin: subtask planning uses the DECODED footprint
/// (`TotalRealSize`), not on-disk bytes (`TotalFileSize`) -- 1500 real/100
/// file with engine size 500 plans 3 subtasks, and the mirrored
/// `getAdjustedMaxEngineSize` (:464) returns 500/100 accordingly.
#[test]
#[ignore = "go-parity-gap: calculateSubtaskCnt/getAdjustedMaxEngineSize (table_import.go:429/:464) are unported"]
fn import_subtask_count_uses_real_size_not_compressed_file_size() {}

/// Go `pkg/executor/importer/table_import_test.go:154::TestLoadDataControllerGetAdjustedMaxEngineSize`:
/// `getAdjustedMaxEngineSize` (`table_import.go:464`) splits the engine size
/// across subtasks: without cloud storage `ceil(TotalRealSize/nodes)` bounded
/// by the engine size (749/500/3 -> 749, 750/500/4 -> 375, 100/30/7 -> 34);
/// with cloud storage it divides by the subtask count (500/500/2 -> 250,
/// 750/500/4 -> 188, 1250/500/6 -> 209, 100/30/2 -> 25, 400/99/3 -> 67).
#[test]
#[ignore = "go-parity-gap: getAdjustedMaxEngineSize (table_import.go:464) is unported"]
fn import_adjusted_max_engine_size_divides_across_subtasks() {}

/// Go `pkg/executor/importer/table_import_test.go:218::TestGetRegionSplitSizeKeys`:
/// `GetRegionSplitSizeKeys` (`table_import.go:171`) propagates a PD client
/// construction error verbatim ("mock error") and wraps a client-level failure
/// with "get region split size and keys failed" (via the
/// `NewClientWithAPIContext` injection seam, `table_import.go:186`).
#[test]
#[ignore = "go-parity-gap: GetRegionSplitSizeKeys (table_import.go:171) needs the PD client seam; unported"]
fn import_region_split_size_keys_wraps_pd_failures() {}

/// Go `pkg/executor/importer/table_import_testkit_test.go:51::TestImportFromSelectCleanup`:
/// a failed `IMPORT INTO ... FROM SELECT` (failpoint `mockImportFromSelectErr`,
/// `table_import.go:785`) leaves the import root directory empty after the
/// table importer is closed: no sort temp files survive.
#[test]
#[ignore = "go-parity-gap: the table importer for SELECT sources (NewTableImporterForTest, table_import.go:295) and its cleanup are unported"]
fn import_from_select_failure_cleans_the_import_directory() {}
