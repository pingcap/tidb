# TiDB testkit Test Case Map (pkg/testkit)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/testkit

### Tests
- `pkg/testkit/db_driver_test.go` - Tests mock DB.
- `pkg/testkit/testkit_test.go` - Tests multi statement in testkit.

### Testdata
- `pkg/testkit/testdata/BUILD.bazel`
- `pkg/testkit/testdata/testdata.go`

## pkg/testkit/testfork

### Tests
- `pkg/testkit/testfork/fork_test.go` - Tests fork sub test.

## pkg/testkit/testutil

### Tests
- `pkg/testkit/testutil/require_test.go` - Tests compare unordered string.
