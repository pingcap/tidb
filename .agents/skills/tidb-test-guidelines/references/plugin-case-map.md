# TiDB plugin Test Case Map (pkg/plugin)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/plugin

### Tests
- `pkg/plugin/const_test.go` - Tests const to string.
- `pkg/plugin/helper_test.go` - Tests plugin declare.
- `pkg/plugin/integration_test.go` - Tests audit log normal.
- `pkg/plugin/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/plugin/plugin_test.go` - Tests load static registered plugin.
- `pkg/plugin/spi_test.go` - Tests export manifest.

## pkg/plugin/conn_ip_example

### Tests
- `pkg/plugin/conn_ip_example/conn_ip_example_test.go` - Tests load plugin.
- `pkg/plugin/conn_ip_example/main_test.go` - Configures default goleak settings and registers testdata.