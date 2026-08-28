// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap tests for Go `pkg/executor/importer/chunk_process_testkit_test.go` and
//! the chunk-processing halves of `importer_testkit_test.go`: the file/query
//! chunk processors that encode rows into KV pairs and deliver them to engine
//! writers. The whole processor stack (`pkg/executor/importer/chunk_process.go`
//! plus the Lightning backend it drives) is unported on this tier.

/// Go `pkg/executor/importer/chunk_process_testkit_test.go:60::TestFileChunkProcess`:
/// `NewFileChunkProcessor` (`chunk_process.go:432`) over a CSV parser and
/// table-KV encoder: processed rows/KV counts match the file, the checksum
/// collector accumulates the encoded bytes, disk-quota locking pauses the
/// deliver loop, an index-writer error aborts `Process` with "index write
/// error", and the writer's `IsSynced`/status contract holds at completion
/// (`baseChunkProcessor.Process`, `chunk_process.go:402`).
#[test]
#[ignore = "go-parity-gap: the chunk processor stack (chunk_process.go:402/:432) plus lightning backend writers and metrics are unported"]
fn import_file_chunk_processor_encodes_csv_and_propagates_index_writer_errors() {}

/// Go `pkg/executor/importer/chunk_process_testkit_test.go:278::TestNewIndexRouteWriterFactoryErr`:
/// `NewIndexRouteWriter` (`chunk_process.go:627`) surfaces the factory error
/// from `AppendRows` when the per-index writer cannot be created.
#[test]
#[ignore = "go-parity-gap: NewIndexRouteWriter (chunk_process.go:627) and its simplesst writer factory are unported"]
fn import_index_route_writer_propagates_factory_errors_from_append_rows() {}

/// Go `pkg/executor/importer/importer_testkit_test.go:312::TestProcessChunkWith`:
/// `ProcessChunkWithWriter` (`engine_process.go:72`) over a real store: the
/// `skip_rows` option is honored, scanned-row counts and per-chunk KV
/// checksums match the CSV content, and the keyspace prefix length feeds the
/// row-ID accounting (`getParser`, `table_import.go:352`; `SetSelectedChunkCh`,
/// `table_import.go:712`, drives the query side).
#[test]
#[ignore = "go-parity-gap: ProcessChunkWithWriter (engine_process.go:72) needs a full table importer + lightning backend; unported"]
fn import_process_chunk_respects_skip_rows_and_tracks_checksums() {}

/// Go `pkg/executor/importer/importer_testkit_test.go:426::TestPopulateChunks`:
/// `LoadDataController.PopulateChunks` (`table_import.go:480`) splits a
/// 3-file glob into one data engine per file plus an empty index engine
/// (engine id `common.IndexEngineID`), with `__max_engine_size` capping chunk
/// sizes.
#[test]
#[ignore = "go-parity-gap: PopulateChunks (table_import.go:480) and the engine-id layout are unported"]
fn import_populate_chunks_splits_globs_into_per_file_engines() {}
