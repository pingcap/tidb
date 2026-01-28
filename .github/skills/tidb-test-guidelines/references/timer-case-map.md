# TiDB timer Test Case Map (pkg/timer)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/timer

### Tests
- `pkg/timer/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/timer/store_intergartion_test.go` - Tests mem timer store integration.

## pkg/timer/api

### Tests
- `pkg/timer/api/client_test.go` - Tests get timer option.
- `pkg/timer/api/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/timer/api/schedule_policy_test.go` - Tests interval policy.
- `pkg/timer/api/store_test.go` - Tests field optional.
- `pkg/timer/api/timer_test.go` - Tests timer validate.

## pkg/timer/runtime

### Tests
- `pkg/timer/runtime/cache_test.go` - Tests cache update.
- `pkg/timer/runtime/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/timer/runtime/runtime_test.go` - Tests runtime start/stop.
- `pkg/timer/runtime/worker_test.go` - Tests worker start/stop.

## pkg/timer/tablestore

### Tests
- `pkg/timer/tablestore/sql_test.go` - Tests build insert timer SQL.
