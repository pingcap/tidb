// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap tests for Go `pkg/executor/importer/precheck_test.go` and
//! `sampler_test.go`: requirement prechecks and the KV-size samplers that
//! estimate index/data ratio before an import. Go source:
//! `pkg/executor/importer/precheck.go` and `sampler.go`.

/// Go `pkg/executor/importer/precheck_test.go:112::TestCheckRequirements`:
/// `LoadDataController.checkRequirements` (`precheck.go:62`, wrapped by
/// `CheckRequirements`/:52 and `CheckRequirementsBeforeInitDataFiles`/:58)
/// fails with `ErrLoadDataPreCheckFailed` when another active job exists on
/// the target table ("there is active job on the target table already"), when
/// the source file size is zero, when the target table is non-empty, or when
/// the table is missing ("doesn't exist"); the before-init variant skips the
/// file-size check when `DisablePrecheck` is set; `CancelJob` clears the
/// active-job gate.
#[test]
#[ignore = "go-parity-gap: the precheck chain (precheck.go:52/:58/:62) needs the job table and a session; unported"]
fn import_check_requirements_gates_on_active_jobs_size_and_empty_table() {}

/// Go `pkg/executor/importer/sampler_test.go:168::TestSampleIndexSizeRatio`:
/// `SampleFileImportKVSize` (`sampler.go:108`) over generated CSV/SQL sources
/// reproduces Go's measured index/data ratios per (files, rows-per-file,
/// row-size, schema) case (e.g. 0.287 for the simple table, 1.151 with four
/// indexes, 0.087 for longer rows, keyspace codec 0.308), closes the parser
/// when `getParser` errors (`kvSizeSampler.getParser`, `sampler.go:224`), and
/// sizes SQL sources by consumed bytes rather than buffered progress.
#[test]
#[ignore = "go-parity-gap: the kvSizeSampler (sampler.go:90/:108/:224) and its CSV/SQL parsing stack are unported"]
fn import_kv_size_sampler_estimates_index_to_data_ratio() {}

/// Go `pkg/executor/importer/sampler_test.go:287::TestSampleIndexSizeRatioVeryLongRows`:
/// the same sampler with very long rows (rows larger than the read block
/// size) still produces the expected ratio, exercising the
/// long-line-splitting path of the CSV reader.
#[test]
#[ignore = "go-parity-gap: the kvSizeSampler (sampler.go:108) and its long-row CSV reader path are unported"]
fn import_kv_size_sampler_handles_very_long_rows() {}
