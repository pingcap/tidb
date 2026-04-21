# TiDB KV Test Case Map (pkg/kv)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/kv

### Tests
- `pkg/kv/checker_test.go` - Tests is request type supported.
- `pkg/kv/error_test.go` - Tests error.
- `pkg/kv/fault_injection_test.go` - Tests fault injection basic.
- `pkg/kv/interface_mock_test.go` - Tests interface mock.
- `pkg/kv/key_test.go` - Tests partial next.
- `pkg/kv/kv_test.go` - Tests resource group tag encoding.
- `pkg/kv/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/kv/mock_test.go` - Tests interface.
- `pkg/kv/option_test.go` - Tests set CDC write source.
- `pkg/kv/txn_test.go` - Tests backoff.
- `pkg/kv/utils_test.go` - Tests increment int64.
- `pkg/kv/version_test.go` - Tests version.
