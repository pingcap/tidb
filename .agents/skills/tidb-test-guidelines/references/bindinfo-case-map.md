# TiDB bindinfo Test Case Map (pkg/bindinfo)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/bindinfo

### Tests
- `pkg/bindinfo/binding_auto_test.go` - Tests plan generation with stmt ctx.
- `pkg/bindinfo/binding_cache_test.go` - Tests cross-DB binding cache.
- `pkg/bindinfo/binding_operator_test.go` - Tests binding cache.
- `pkg/bindinfo/binding_plan_evolution_test.go` - Tests rule-based plan performance predictor.
- `pkg/bindinfo/binding_plan_generation_test.go` - Tests fix control adjustments.
- `pkg/bindinfo/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/bindinfo/session_handle_test.go` - Tests global and session binding both exist.

### Testdata
- `pkg/bindinfo/testdata/binding_auto_suite_in.json`
- `pkg/bindinfo/testdata/binding_auto_suite_out.json`

## pkg/bindinfo/tests

### Tests
- `pkg/bindinfo/tests/bind_test.go` - Tests prepare cache with binding.
- `pkg/bindinfo/tests/bind_usage_info_test.go` - Tests bind usage info.
- `pkg/bindinfo/tests/cross_db_binding_test.go` - Tests cross-DB binding basics.
- `pkg/bindinfo/tests/main_test.go` - Configures default goleak settings and registers testdata.
