# TiDB tablecodec Test Case Map (pkg/tablecodec)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/tablecodec

### Tests
- `pkg/tablecodec/bench_test.go` - Tests encode row key with handle.
- `pkg/tablecodec/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/tablecodec/tablecodec_test.go` - Tests table codec.

## pkg/tablecodec/rowindexcodec

### Tests
- `pkg/tablecodec/rowindexcodec/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/tablecodec/rowindexcodec/rowindexcodec_test.go` - Tests get key kind.