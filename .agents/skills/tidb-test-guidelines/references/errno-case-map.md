# TiDB errno Test Case Map (pkg/errno)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/errno

### Tests
- `pkg/errno/errname_test.go` - Tests error codes have messages.
- `pkg/errno/infoschema_test.go` - Tests infoschema copy safety.
- `pkg/errno/main_test.go` - Configures default goleak settings and registers testdata.
