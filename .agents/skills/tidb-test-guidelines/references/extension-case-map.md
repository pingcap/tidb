# TiDB extension Test Case Map (pkg/extension)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/extension

### Tests
- `pkg/extension/auth_test.go` - Tests auth plugin.
- `pkg/extension/bootstrap_test.go` - Tests bootstrap.
- `pkg/extension/event_listener_test.go` - Tests extension stmt events.
- `pkg/extension/function_test.go` - Tests extension func ctx.
- `pkg/extension/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/extension/registry_test.go` - Tests setup extensions.