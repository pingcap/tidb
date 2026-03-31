# TiDB table Test Case Map (pkg/table)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/table

### Tests
- `pkg/table/column_test.go` - Tests column string.
- `pkg/table/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/table/table_test.go` - Tests error code.

## pkg/table/tables

### Tests
- `pkg/table/tables/bench_test.go` - Tests add record in pipelined DML.
- `pkg/table/tables/cache_test.go` - Tests cache table basic scan.
- `pkg/table/tables/index_test.go` - Tests multi column common handle.
- `pkg/table/tables/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/table/tables/mutation_checker_test.go` - Tests compare index data.
- `pkg/table/tables/state_remote_test.go` - Tests state remote.
- `pkg/table/tables/tables_test.go` - Tests basic.

## pkg/table/tables/test/partition

### Tests
- `pkg/table/tables/test/partition/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/table/tables/test/partition/partition_test.go` - Tests partition add record.

## pkg/table/tblctx

### Tests
- `pkg/table/tblctx/buffers_test.go` - Tests encode row.

## pkg/table/tblsession

### Tests
- `pkg/table/tblsession/table_test.go` - Tests session mutate context fields.

## pkg/table/temptable

### Tests
- `pkg/table/temptable/ddl_test.go` - Tests add local temporary table.
- `pkg/table/temptable/interceptor_test.go` - Tests get key accessed table ID.
- `pkg/table/temptable/main_test.go` - Configures default goleak settings and registers testdata.
