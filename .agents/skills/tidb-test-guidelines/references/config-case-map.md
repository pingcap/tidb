# TiDB config Test Case Map (pkg/config)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/config

### Tests
- `pkg/config/config_test.go` - Tests atomic bool unmarshal.
- `pkg/config/config_util_test.go` - Tests clone config.
- `pkg/config/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/config/store_test.go` - Tests store type.

## pkg/config/kerneltype

### Tests
- `pkg/config/kerneltype/type_test.go` - Tests kernel type.
