# TiDB structure Test Case Map (pkg/structure)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/structure

### Tests
- `pkg/structure/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/structure/structure_test.go` - Tests structure stringer.
