# TiDB telemetry Test Case Map (pkg/telemetry)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/telemetry

### Tests
- `pkg/telemetry/data_feature_usage_test.go` - Tests txn usage info.
- `pkg/telemetry/data_window_test.go` - Tests builtin functions usage.
- `pkg/telemetry/main_test.go` - Configures default goleak settings and registers testdata.