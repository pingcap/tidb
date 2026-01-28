# TiDB lightning Test Case Map (pkg/lightning)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/lightning/backend

### Tests
- `pkg/lightning/backend/backend_test.go` - lightning/backend: Tests open/close import cleanup engine.

## pkg/lightning/backend/external

### Tests
- `pkg/lightning/backend/external/bench_test.go` - lightning/backend/external: Tests compare writer.
- `pkg/lightning/backend/external/byte_reader_test.go` - lightning/backend/external: Tests byte reader.
- `pkg/lightning/backend/external/codec_test.go` - lightning/backend/external: Tests range property codec.
- `pkg/lightning/backend/external/concurrent_reader_test.go` - lightning/backend/external: Tests concurrent read.
- `pkg/lightning/backend/external/engine_test.go` - lightning/backend/external: Tests memory ingest data.
- `pkg/lightning/backend/external/file_test.go` - lightning/backend/external: Tests add key-value maintain range property.
- `pkg/lightning/backend/external/iter_test.go` - lightning/backend/external: Tests merge KV iter.
- `pkg/lightning/backend/external/merge_test.go` - lightning/backend/external: Tests split data files.
- `pkg/lightning/backend/external/misc_bench_test.go` - lightning/backend/external: Tests misc bench.
- `pkg/lightning/backend/external/onefile_writer_test.go` - lightning/backend/external: Tests onefile writer basic.
- `pkg/lightning/backend/external/reader_test.go` - lightning/backend/external: Tests read all data basic.
- `pkg/lightning/backend/external/sort_test.go` - lightning/backend/external: Tests global sort local basic.
- `pkg/lightning/backend/external/split_test.go` - lightning/backend/external: Tests general properties.
- `pkg/lightning/backend/external/util_test.go` - lightning/backend/external: Tests seek props offsets.
- `pkg/lightning/backend/external/writer_test.go` - lightning/backend/external: Tests writer.

## pkg/lightning/backend/kv

### Tests
- `pkg/lightning/backend/kv/allocator_test.go` - lightning/backend/kv: Tests allocator.
- `pkg/lightning/backend/kv/base_test.go` - lightning/backend/kv: Tests log KV convert failed.
- `pkg/lightning/backend/kv/context_test.go` - lightning/backend/kv: Tests lit expression context.
- `pkg/lightning/backend/kv/kv2sql_test.go` - lightning/backend/kv: Tests iter raw index keys clustered PK.
- `pkg/lightning/backend/kv/session_internal_test.go` - lightning/backend/kv: Tests KV mem buf interweave alloc and recycle.
- `pkg/lightning/backend/kv/sql2kv_test.go` - lightning/backend/kv: Tests marshal.

## pkg/lightning/backend/local

### Tests
- `pkg/lightning/backend/local/checksum_test.go` - lightning/backend/local: Tests do checksum.
- `pkg/lightning/backend/local/compress_test.go` - lightning/backend/local: Tests gzip compressor.
- `pkg/lightning/backend/local/disk_quota_test.go` - lightning/backend/local: Tests check disk quota.
- `pkg/lightning/backend/local/duplicate_test.go` - lightning/backend/local: Tests build dup task.
- `pkg/lightning/backend/local/engine_mgr_test.go` - lightning/backend/local: Tests engine manager.
- `pkg/lightning/backend/local/engine_test.go` - lightning/backend/local: Tests get engine size when import.
- `pkg/lightning/backend/local/iterator_test.go` - lightning/backend/local: Tests dup detect iterator.
- `pkg/lightning/backend/local/job_worker_test.go` - lightning/backend/local: Tests region job base worker.
- `pkg/lightning/backend/local/local_check_test.go` - lightning/backend/local: Tests check requirements TiFlash.
- `pkg/lightning/backend/local/local_test.go` - lightning/backend/local: Tests next key.
- `pkg/lightning/backend/local/localhelper_test.go` - lightning/backend/local: Tests store write limiter.
- `pkg/lightning/backend/local/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/lightning/backend/local/rate_limiter_test.go` - lightning/backend/local: Tests concurrency limit.
- `pkg/lightning/backend/local/region_job_test.go` - lightning/backend/local: Tests convert PB error to error.

## pkg/lightning/backend/tidb

### Tests
- `pkg/lightning/backend/tidb/tidb_test.go` - lightning/backend/tidb: Tests write rows replace on dup.

## pkg/lightning/checkpoints

### Tests
- `pkg/lightning/checkpoints/checkpoints_file_test.go` - lightning/checkpoints: Tests get.
- `pkg/lightning/checkpoints/checkpoints_sql_test.go` - lightning/checkpoints: Tests normal operations.
- `pkg/lightning/checkpoints/checkpoints_test.go` - lightning/checkpoints: Tests merge status checkpoint.
- `pkg/lightning/checkpoints/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/lightning/common

### Tests
- `pkg/lightning/common/common_test.go` - lightning/common: Tests alloc global auto ID.
- `pkg/lightning/common/errors_test.go` - lightning/common: Tests normalize error.
- `pkg/lightning/common/key_adapter_test.go` - lightning/common: Tests noop key adapter.
- `pkg/lightning/common/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/lightning/common/once_error_test.go` - lightning/common: Tests once error.
- `pkg/lightning/common/pause_test.go` - lightning/common: Tests pause.
- `pkg/lightning/common/retry_test.go` - lightning/common: Tests is retryable error.
- `pkg/lightning/common/security_test.go` - lightning/common: Tests get JSON insecure.
- `pkg/lightning/common/storage_test.go` - lightning/common: Tests get storage size.
- `pkg/lightning/common/util_test.go` - lightning/common: Tests dir not exist.

## pkg/lightning/config

### Tests
- `pkg/lightning/config/bytesize_test.go` - lightning/config: Tests byte size TOML decode.
- `pkg/lightning/config/config_test.go` - lightning/config: Tests adjust PD addr and port.
- `pkg/lightning/config/configlist_test.go` - lightning/config: Tests normal push pop.

## pkg/lightning/duplicate

### Tests
- `pkg/lightning/duplicate/detector_test.go` - lightning/duplicate: Tests detector.
- `pkg/lightning/duplicate/internal_test.go` - lightning/duplicate: Tests internal key.
- `pkg/lightning/duplicate/worker_test.go` - lightning/duplicate: Tests gen split key.

## pkg/lightning/errormanager

### Tests
- `pkg/lightning/errormanager/errormanager_test.go` - lightning/errormanager: Tests init.
- `pkg/lightning/errormanager/resolveconflict_test.go` - lightning/errormanager: Tests replace conflict multiple keys non-clustered PK.

## pkg/lightning/log

### Tests
- `pkg/lightning/log/filter_test.go` - lightning/log: Tests filter.
- `pkg/lightning/log/log_test.go` - lightning/log: Tests config adjust.

## pkg/lightning/membuf

### Tests
- `pkg/lightning/membuf/buffer_test.go` - lightning/membuf: Tests buffer pool.
- `pkg/lightning/membuf/limiter_test.go` - lightning/membuf: Tests limiter.

## pkg/lightning/metric

### Tests
- `pkg/lightning/metric/metric_test.go` - lightning/metric: Tests read counter.

## pkg/lightning/mydump

### Tests
- `pkg/lightning/mydump/charset_convertor_test.go` - lightning/mydump: Tests charset converter.
- `pkg/lightning/mydump/csv_parser_test.go` - lightning/mydump: Tests TPCH.
- `pkg/lightning/mydump/loader_test.go` - lightning/mydump: Tests loader.
- `pkg/lightning/mydump/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/lightning/mydump/parquet_parser_test.go` - lightning/mydump: Tests parquet parser.
- `pkg/lightning/mydump/parser_test.go` - lightning/mydump: Tests read row.
- `pkg/lightning/mydump/reader_test.go` - lightning/mydump: Tests export statement no trailing newline.
- `pkg/lightning/mydump/region_test.go` - lightning/mydump: Tests table region.
- `pkg/lightning/mydump/router_test.go` - lightning/mydump: Tests route parser.
- `pkg/lightning/mydump/schema_import_test.go` - lightning/mydump: Tests schema importer.

## pkg/lightning/tikv

### Tests
- `pkg/lightning/tikv/local_sst_writer_test.go` - lightning/tikv: Tests integration test.
- `pkg/lightning/tikv/tikv_test.go` - lightning/tikv: Tests for all stores.

## pkg/lightning/verification

### Tests
- `pkg/lightning/verification/checksum_test.go` - lightning/verification: Tests checksum.

## pkg/lightning/worker

### Tests
- `pkg/lightning/worker/worker_test.go` - lightning/worker: Tests apply recycle.
