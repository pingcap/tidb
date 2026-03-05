# TiDB TTL Test Case Map (pkg/ttl)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/ttl/cache

### Tests
- `pkg/ttl/cache/base_test.go` - Tests base cache.
- `pkg/ttl/cache/infoschema_test.go` - Tests infoschema cache.
- `pkg/ttl/cache/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/ttl/cache/split_test.go` - Tests split TTL scan ranges with signed int.
- `pkg/ttl/cache/table_test.go` - Tests new TTL table.
- `pkg/ttl/cache/task_test.go` - Tests row to TTL task.
- `pkg/ttl/cache/ttlstatus_test.go` - Tests TTL status cache.

## pkg/ttl/client

### Tests
- `pkg/ttl/client/command_test.go` - Tests command client.

## pkg/ttl/metrics

### Tests
- `pkg/ttl/metrics/metrics_test.go` - Tests phase tracer.

## pkg/ttl/session

### Tests
- `pkg/ttl/session/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/ttl/session/session_test.go` - Tests session run in txn.
- `pkg/ttl/session/sysvar_test.go` - Tests sys var TTL job enable.

## pkg/ttl/sqlbuilder

### Tests
- `pkg/ttl/sqlbuilder/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/ttl/sqlbuilder/sql_test.go` - Tests escape.

## pkg/ttl/ttlworker

### Tests
- `pkg/ttl/ttlworker/del_test.go` - Tests TTL del retry buffer.
- `pkg/ttl/ttlworker/job_manager_integration_test.go` - Tests with session.
- `pkg/ttl/ttlworker/job_manager_test.go` - Tests ready for lock heartbeat timeout job tables.
- `pkg/ttl/ttlworker/scan_integration_test.go` - Tests cancel while scan.
- `pkg/ttl/ttlworker/scan_test.go` - Tests scan worker schedule.
- `pkg/ttl/ttlworker/session_integration_test.go` - Tests get session with fault.
- `pkg/ttl/ttlworker/session_test.go` - Tests execute SQL with check.
- `pkg/ttl/ttlworker/task_manager_integration_test.go` - Tests parallel lock new task.
- `pkg/ttl/ttlworker/task_manager_test.go` - Tests resize workers.
- `pkg/ttl/ttlworker/timer_sync_test.go` - Tests TTL manual trigger one timer.
- `pkg/ttl/ttlworker/timer_test.go` - Tests TTL timer hook prepare.
