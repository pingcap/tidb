# TiDB types Test Case Map (pkg/types)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/types

### Tests
- `pkg/types/benchmark_test.go` - Tests default type for value.
- `pkg/types/binary_literal_test.go` - Tests binary literal.
- `pkg/types/compare_test.go` - Tests compare.
- `pkg/types/const_test.go` - Tests get SQL mode.
- `pkg/types/context_test.go` - Tests with new flags.
- `pkg/types/convert_test.go` - Tests convert type.
- `pkg/types/core_time_test.go` - Tests week behavior.
- `pkg/types/datum_test.go` - Tests datum.
- `pkg/types/enum_test.go` - Tests enum.
- `pkg/types/errors_test.go` - Tests error.
- `pkg/types/etc_test.go` - Tests is type.
- `pkg/types/export_test.go` - Tests export.
- `pkg/types/field_type_test.go` - Tests field type.
- `pkg/types/format_test.go` - Tests time format method.
- `pkg/types/fsp_test.go` - Tests check fsp.
- `pkg/types/helper_test.go` - Tests str to int.
- `pkg/types/json_binary_functions_test.go` - Tests decode escaped unicode.
- `pkg/types/json_binary_test.go` - Tests binary JSON marshal/unmarshal.
- `pkg/types/json_path_expr_test.go` - Tests contains any asterisk.
- `pkg/types/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/types/mydecimal_benchmark_test.go` - Tests round.
- `pkg/types/mydecimal_test.go` - Tests from int.
- `pkg/types/overflow_test.go` - Tests add.
- `pkg/types/set_test.go` - Tests set.
- `pkg/types/time_test.go` - Tests time encoding.
- `pkg/types/vector_test.go` - Tests vector endianness.

## pkg/types/parser_driver

### Tests
- `pkg/types/parser_driver/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/types/parser_driver/value_expr_test.go` - Tests value expr restore.
