# TiDB metrics Test Case Map (pkg/metrics)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/metrics

### Tests
- `pkg/metrics/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/metrics/metrics_internal_test.go` - Tests retained labels.
- `pkg/metrics/metrics_test.go` - Tests metrics.

## pkg/metrics/common

### Tests
- `pkg/metrics/common/wrapper_test.go` - Tests get merged const labels.
