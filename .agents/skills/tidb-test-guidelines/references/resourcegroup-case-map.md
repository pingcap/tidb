# TiDB resourcegroup Test Case Map (pkg/resourcegroup)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/resourcegroup/runaway

### Tests
- `pkg/resourcegroup/runaway/checker_test.go` - Tests concurrent reset and check thresholds.
- `pkg/resourcegroup/runaway/record_test.go` - Tests record key.

## pkg/resourcegroup/tests

### Tests
- `pkg/resourcegroup/tests/resource_group_test.go` - Tests resource group basic.