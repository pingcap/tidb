# TiDB DistSQL Test Case Map (pkg/distsql)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/distsql

### Tests
- `pkg/distsql/bench_test.go` - Tests large chunk responses for select.
- `pkg/distsql/context_test.go` - Tests context.
- `pkg/distsql/distsql_test.go` - Tests normal select.
- `pkg/distsql/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/distsql/request_builder_test.go` - Tests table handles to KV ranges.
- `pkg/distsql/select_result_test.go` - Tests coprocessor runtime-stat updates, including exact plan-ID row summaries, last-plan scan-detail ownership/provenance counts, multi-response aggregation, plan-local Cop/CopStream RPC access, independent expected-task coverage, and close-time stats-only handling that cannot replay stale summaries.

## pkg/distsql/context

### Tests
- `pkg/distsql/context/context_test.go` - Tests context detach.
