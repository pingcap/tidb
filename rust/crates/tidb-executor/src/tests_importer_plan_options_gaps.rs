// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap tests for Go `pkg/executor/importer/import_test.go`: the
//! `Plan`/`LoadDataController` option machinery. None of
//! `pkg/executor/importer` exists on this tier (no `Plan`, no
//! `LoadDataController`, no Lightning config/backend seam), so each test here
//! records the exact Go behavior that a future port must satisfy. Go source:
//! `pkg/executor/importer/import.go` unless noted.

/// Go `pkg/executor/importer/import_test.go:71::TestInitDefaultOptions`:
/// `Plan.initDefaultOptions` (`import.go:739`) defaults thread count to 2 for
/// a query source (5 on a second call after the CPU-based clamp), thread count
/// 1 for a file source with `CloudStorageURI` set, zero disk quota,
/// required checksum, unlimited write speed, no split-file, 10000 max recorded
/// errors, error on-dup, detached off, utf8mb4 charset, and appends `/dxf/` to
/// the cloud storage URI. Engine size default comes from `getDefMaxEngineSize`
/// (`import.go:760`): 500GiB classic / 100GiB nextgen.
#[test]
#[ignore = "go-parity-gap: importer Plan.initDefaultOptions (import.go:739) is unported; no Plan type on this tier"]
fn import_plan_init_default_options_pins_thread_quota_checksum_and_uri_defaults() {}

/// Go `pkg/executor/importer/import_test.go:108::TestPlanUseNewCollate`:
/// `Plan.GetUseNewCollateOrDefault` (`import.go:360`) returns the stored flag
/// or the passed default when unset; `setUseNewCollate` (:369) writes it; the
/// flag round-trips through `json.Marshal` as `"use_new_collate":false`.
#[test]
#[ignore = "go-parity-gap: importer Plan collate flag and its JSON contract are unported"]
fn import_plan_use_new_collate_flag_round_trips_through_json() {}

/// Go `pkg/executor/importer/import_test.go:129::TestInitOptionsPositiveCase`:
/// `Plan.initOptions` (`import.go:767`) maps every `WITH` option: charset,
/// field terminators/enclosures/escapes, `NULL` definition, line terminator,
/// skip-rows, disk-quota (`100gib` -> 100<<30), checksum `optional`,
/// thread count clamped to GOMAXPROCS, write speed `200mib` -> 200<<20,
/// split-file, record-errors 123, detached, disable-tikv-import-mode,
/// max-engine-size, and the cloud-storage-URI override chain
/// (global var < `cloud_storage_uri` option; `azure://`/`azblob://`/`gs://`
/// accepted; empty string forces local sort), plus `on_duplicate_key`
/// capture/error. The option names come from the `IMPORT INTO` grammar.
#[test]
#[ignore = "go-parity-gap: importer Plan.initOptions (import.go:767) is unported; no WITH-option evaluation surface"]
fn import_plan_init_options_maps_every_with_option() {}

/// Go `pkg/executor/importer/import_test.go:248::TestInitOptionsDisallowOnDuplicateKeyWithLocalSort`:
/// `on_duplicate_key='capture'` without a cloud storage URI fails with
/// `ErrLoadDataUnsupportedOption` naming the option and "local sort"
/// (`import.go`, the capture-needs-cloud branch guarded by the same errors as
/// import.go:804/:812).
#[test]
#[ignore = "go-parity-gap: importer Plan.initOptions (import.go:767) is unported; ErrLoadDataUnsupportedOption has no Rust error"]
fn import_plan_rejects_on_duplicate_key_capture_with_local_sort() {}

/// Go `pkg/executor/importer/import_test.go:279::TestAdjustOptions`:
/// `Plan.adjustOptions` (`import.go:1046`) clamps an oversized thread count to
/// the target node CPU count (doubled for a query source) and forces
/// `DisableTiKVImportMode` on when a cloud storage URI is present; write speed
/// is never adjusted.
#[test]
#[ignore = "go-parity-gap: importer Plan.adjustOptions (import.go:1046) is unported"]
fn import_plan_adjust_options_clamps_threads_and_disables_tikv_import_mode_for_cloud() {}

/// Go `pkg/executor/importer/import_test.go:302::TestGetConflictHandlingMode`:
/// `Plan.GetOnDupKeyMode` (`import.go:351`) defaults to `OnDupKeyModeError`
/// and reflects an explicitly stored capture/error mode otherwise.
#[test]
#[ignore = "go-parity-gap: importer Plan.GetOnDupKeyMode (import.go:351) is unported"]
fn import_plan_conflict_handling_mode_defaults_to_error() {}

/// Go `pkg/executor/importer/import_test.go:313::TestAdjustDiskQuota` (in
/// `table_import.go`): `adjustDiskQuota` (`table_import.go:820`) clamps the
/// quota to 80% of the sort directory's free space (failpoint
/// `GetStorageSize` = 2048 -> 1638) and never returns less than 1 byte.
#[test]
#[ignore = "go-parity-gap: adjustDiskQuota (table_import.go:820) needs the lightning GetStorageSize failpoint and a local store; unported"]
fn import_disk_quota_is_clamped_to_eighty_percent_of_free_space() {}

/// Go `pkg/executor/importer/import_test.go:325::TestASTArgsFromStmt`:
/// `ASTArgsFromStmt` (`import.go:624`) re-parses the statement text and keeps
/// the column list and SET assignments verbatim, including a non-ASCII column
/// name (`é`) parsed with a latin1 charset.
#[test]
#[ignore = "go-parity-gap: importer ASTArgsFromStmt (import.go:624) is unported (the statement itself parses via tidb-ast, but no ASTArgs extraction exists)"]
fn import_ast_args_round_trip_columns_and_assignments() {}

/// Go `pkg/executor/importer/import_test.go:349::TestInitParameters`:
/// `Plan.initParameters` (`import.go:1069`) builds the job's recorded
/// parameters, redacting secret-looking query parameters (`sas-token`,
/// `sas_token`) to `xxxxxx` in both the file location and the
/// `cloud_storage_uri` option value, and stringifying option values (`thread`
/// -> `"3"`, flag options kept by name).
#[test]
#[ignore = "go-parity-gap: importer Plan.initParameters (import.go:1069) with its redaction table is unported"]
fn import_plan_init_parameters_redacts_secrets_and_stringifies_options() {}

/// Go `pkg/executor/importer/import_test.go:389::TestGetLocalBackendCfg`:
/// `LoadDataController.getLocalBackendCfg` builds a lightning local-backend
/// config from the PD address and store dir, disables automatic compactions,
/// zero RaftKV2 switch duration, and the default switch interval when
/// `IsRaftKV2` is set.
#[test]
#[ignore = "go-parity-gap: lightning local backend config seam is unported; no getLocalBackendCfg equivalent"]
fn import_local_backend_cfg_pins_pd_addr_store_dir_and_raftkv2_interval() {}

/// Go `pkg/executor/importer/import_test.go:439::TestImportPlanParquetLocation`:
/// the parquet writer location follows the session time zone (named zone kept
/// as-is, fixed offset rendered `+08:00`/`-06:00`, a NAMED fixed zone like
/// `UTC+8` rejected with "invalid location UTC+8"), and a legacy task-meta
/// JSON without `LocationID` falls back to UTC
/// (`LoadDataController.ParquetLocation`, `import.go:695`; `NewImportPlan`,
/// `import.go:541`).
#[test]
#[ignore = "go-parity-gap: parquet data-file pipeline (InitDataFiles/ParquetLocation, import.go:695/:1464) is unported"]
fn import_parquet_location_follows_session_timezone_with_utc_fallback() {}

/// Go `pkg/executor/importer/import_test.go:550::TestEstimateFormatSizeExpansionRatio`:
/// `estimateFormatSizeExpansionRatio` (`import.go:1338`) returns 1.0 for
/// row-oriented sources and, for parquet, the sampled compression ratio
/// clamped to at least 1.0 relative to the physical file size.
#[test]
#[ignore = "go-parity-gap: estimateFormatSizeExpansionRatio (import.go:1338) needs the parquet sampler; unported"]
fn import_format_size_expansion_ratio_is_identity_for_rows_clamped_for_parquet() {}

/// Go `pkg/executor/importer/import_test.go:594::TestInitCompressedFiles`:
/// `LoadDataController.InitDataFiles` (`import.go:1464`) globs compressed
/// sources, reports FileSize from storage and RealSize from the decompressed
/// sample (failpoint `SampleFileCompressPercentage`), and tolerates a sampled
/// ratio above 100% (250) for many empty archives. Go itself skips the whole
/// test when run as root ("it cannot run as root").
#[test]
#[ignore = "go-parity-gap: InitDataFiles (import.go:1464) and the mydump SampleFileCompressPercentage failpoint are unported"]
fn import_init_data_files_measures_compressed_and_real_sizes() {}

/// Go `pkg/executor/importer/import_test.go:653::TestSupportedSuffixForServerDisk`
/// (classic only): `InitDataFiles` rejects suffix-less or unknown-suffix
/// server-disk paths with `ErrLoadDataInvalidURI`, accepts `.csv`/`.csv.gz`,
/// distinguishes permission-denied (`ErrLoadDataCantRead`) from missing files,
/// honors glob character classes (`[12]`, `[2-3]`), and auto-detects the data
/// format from the double extension (csv/sql/parquet x
/// gz/zstd/zst/snappy, case-insensitive). Go also skips when run as root.
#[test]
#[ignore = "go-parity-gap: InitDataFiles URI/suffix validation and format auto-detection are unported"]
fn import_server_disk_paths_validate_suffixes_permissions_and_globs() {}

/// Go `pkg/executor/importer/import_test.go:802::TestGetDataSourceType`:
/// `getDataSourceType` (`import.go:2018`) reports `DataSourceTypeQuery` when
/// the plan has a select plan and `DataSourceTypeFile` otherwise.
#[test]
#[ignore = "go-parity-gap: importer Plan/getDataSourceType (import.go:2018) is unported"]
fn import_data_source_type_is_query_only_with_a_select_plan() {}

/// Go `pkg/executor/importer/import_test.go:808::TestParseFileType`:
/// `parseFileType` (`import.go:1704`) maps the (double) file extension,
/// case-insensitively, stripping one compression suffix, to csv/sql/parquet,
/// defaulting to csv for unknown or missing extensions (`.hidden.sql.gz` is
/// sql; `file.gz` and `document.txt.gz` are csv).
#[test]
#[ignore = "go-parity-gap: parseFileType (import.go:1704) is unported; newLoadDataParser error pin half needs the lightning parser stack"]
fn import_parse_file_type_maps_double_extensions_case_insensitively() {}

/// Go `pkg/executor/importer/import_test.go:865::TestGetDefMaxEngineSize`:
/// `getDefMaxEngineSize` (`import.go:760`) is 500GiB on the classic kernel and
/// 100GiB on nextgen (this snapshot builds classic, so 500GiB is the pinned
/// value).
#[test]
#[ignore = "go-parity-gap: getDefMaxEngineSize (import.go:760) is unported"]
fn import_default_max_engine_size_is_kernel_dependent() {}
