# TiDB sessionctx Test Case Map (pkg/sessionctx)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/sessionctx

### Tests
- `pkg/sessionctx/context_test.go` - Tests basic ctx type to string.
- `pkg/sessionctx/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/sessionctx/sessionstates

### Tests
- `pkg/sessionctx/sessionstates/session_states_test.go` - Tests grammar.
- `pkg/sessionctx/sessionstates/session_token_test.go` - Tests set cert and key.

## pkg/sessionctx/stmtctx

### Tests
- `pkg/sessionctx/stmtctx/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/sessionctx/stmtctx/stmtctx_test.go` - Tests cop tasks details.

## pkg/sessionctx/vardef

### Tests
- `pkg/sessionctx/vardef/runtime_test.go` - Tests read-only var in next-gen.
- `pkg/sessionctx/vardef/tidb_vars_test.go` - Tests MDL enabled in next-gen.

## pkg/sessionctx/variable

### Tests
- `pkg/sessionctx/variable/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/sessionctx/variable/mock_globalaccessor_test.go` - Tests mock API.
- `pkg/sessionctx/variable/nextgen_test.go` - Tests TiDB pessimistic transaction fair locking.
- `pkg/sessionctx/variable/removed_test.go` - Tests removed option.
- `pkg/sessionctx/variable/statusvar_test.go` - Tests status vars.
- `pkg/sessionctx/variable/sysvar_test.go` - Tests SQL select limit.
- `pkg/sessionctx/variable/varsutil_test.go` - Tests TiDB opt on.

## pkg/sessionctx/variable/tests

### Tests
- `pkg/sessionctx/variable/tests/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/sessionctx/variable/tests/session_test.go` - Tests set system variable.
- `pkg/sessionctx/variable/tests/variable_test.go` - Tests sys var.

## pkg/sessionctx/variable/tests/slowlog

### Tests
- `pkg/sessionctx/variable/tests/slowlog/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/sessionctx/variable/tests/slowlog/slow_log_test.go` - Tests slow log field accessor.
