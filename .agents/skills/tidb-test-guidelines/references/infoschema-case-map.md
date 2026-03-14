# TiDB infoschema Test Case Map (pkg/infoschema)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/infoschema

### Tests
- `pkg/infoschema/bench_test.go` - Tests infoschema overhead.
- `pkg/infoschema/builder_test.go` - Tests get kept allocators.
- `pkg/infoschema/infoschema_nokit_test.go` - Tests infoschema add/delete.
- `pkg/infoschema/infoschema_test.go` - Tests basic.
- `pkg/infoschema/infoschema_v2_test.go` - Tests V2 basic.
- `pkg/infoschema/infoschemav2_cache_test.go` - Tests infoschema cache.
- `pkg/infoschema/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/infoschema/metrics_schema_test.go` - Tests metrics schema definition.
- `pkg/infoschema/sieve_test.go` - Tests get and set.
- `pkg/infoschema/tables_test.go` - Tests TiFlash store detection.

## pkg/infoschema/internal

### Tests
- `pkg/infoschema/internal/sizer_test.go` - Tests size.

## pkg/infoschema/issyncer

### Tests
- `pkg/infoschema/issyncer/deferfn_test.go` - Tests defer func.
- `pkg/infoschema/issyncer/loader_test.go` - Tests load from TS.
- `pkg/infoschema/issyncer/syncer_test.go` - Tests syncer skip MDL check.

## pkg/infoschema/isvalidator

### Tests
- `pkg/infoschema/isvalidator/validator_test.go` - Tests schema validator.

## pkg/infoschema/perfschema

### Tests
- `pkg/infoschema/perfschema/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/infoschema/perfschema/tables_test.go` - Tests predefined tables.

### Testdata
- `pkg/infoschema/perfschema/testdata/test.pprof`
- `pkg/infoschema/perfschema/testdata/tikv.cpu.profile`

## pkg/infoschema/test/cachetest

### Tests
- `pkg/infoschema/test/cachetest/cache_test.go` - Tests new cache.
- `pkg/infoschema/test/cachetest/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/infoschema/test/clustertablestest

### Tests
- `pkg/infoschema/test/clustertablestest/cluster_tables_test.go` - Tests for cluster server info.
- `pkg/infoschema/test/clustertablestest/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/infoschema/test/clustertablestest/tables_test.go` - Tests info schema field value.

## pkg/infoschema/test/infoschemav2test

### Tests
- `pkg/infoschema/test/infoschemav2test/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/infoschema/test/infoschemav2test/v2_test.go` - Tests special schemas.
