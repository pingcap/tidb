# TiDB owner Test Case Map (pkg/owner)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/owner

### Tests
- `pkg/owner/fail_test.go` - Tests fail new session.
- `pkg/owner/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/owner/manager_test.go` - Tests force to be owner.