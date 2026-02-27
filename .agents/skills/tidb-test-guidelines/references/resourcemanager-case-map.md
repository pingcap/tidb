# TiDB resourcemanager Test Case Map (pkg/resourcemanager)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/resourcemanager

### Tests
- `pkg/resourcemanager/schedule_test.go` - Tests scheduler overload too much.

## pkg/resourcemanager/pool/spool

### Tests
- `pkg/resourcemanager/pool/spool/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/resourcemanager/pool/spool/spool_test.go` - Tests release when running pool.

## pkg/resourcemanager/pool/workerpool

### Tests
- `pkg/resourcemanager/pool/workerpool/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/resourcemanager/pool/workerpool/workpool_test.go` - Tests worker pool.

## pkg/resourcemanager/util

### Tests
- `pkg/resourcemanager/util/shard_pool_map_test.go` - Tests shard pool map.