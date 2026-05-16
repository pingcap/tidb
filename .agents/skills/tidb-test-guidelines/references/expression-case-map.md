# TiDB expression Test Case Map (pkg/expression)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/expression

### Tests
- `pkg/expression/bench_test.go` - expression: Tests vectorized execute.
- `pkg/expression/builtin_arithmetic_test.go` - expression: Tests set flen decimal for real or decimal.
- `pkg/expression/builtin_arithmetic_vec_test.go` - expression: Tests vectorized builtin arithmetic func.
- `pkg/expression/builtin_cast_bench_test.go` - expression: Tests cast int as int row.
- `pkg/expression/builtin_cast_test.go` - expression: Tests cast functions.
- `pkg/expression/builtin_cast_vec_test.go` - expression: Tests vectorized builtin cast eval one vec.
- `pkg/expression/builtin_compare_test.go` - expression: Tests compare function with refine.
- `pkg/expression/builtin_compare_vec_generated_test.go` - expression: Tests vectorized generated builtin compare eval one vec.
- `pkg/expression/builtin_compare_vec_test.go` - expression: Tests vectorized builtin compare eval one vec.
- `pkg/expression/builtin_control_test.go` - expression: Tests case when.
- `pkg/expression/builtin_control_vec_generated_test.go` - expression: Tests vectorized builtin control eval one vec generated.
- `pkg/expression/builtin_encryption_test.go` - expression: Tests SQL decode.
- `pkg/expression/builtin_encryption_vec_test.go` - expression: Tests vectorized builtin encryption func.
- `pkg/expression/builtin_grouping_test.go` - expression: Tests grouping.
- `pkg/expression/builtin_ilike_test.go` - expression: Tests ilike.
- `pkg/expression/builtin_info_test.go` - expression: Tests database.
- `pkg/expression/builtin_info_vec_test.go` - expression: Tests vectorized builtin info func.
- `pkg/expression/builtin_json_test.go` - expression: Tests JSON type.
- `pkg/expression/builtin_json_vec_test.go` - expression: Tests vectorized builtin JSON func.
- `pkg/expression/builtin_like_test.go` - expression: Tests like.
- `pkg/expression/builtin_like_vec_test.go` - expression: Tests vectorized builtin like func.
- `pkg/expression/builtin_math_test.go` - expression: Tests abs.
- `pkg/expression/builtin_math_vec_test.go` - expression: Tests vectorized builtin math eval one vec.
- `pkg/expression/builtin_miscellaneous_test.go` - expression: Tests inet aton.
- `pkg/expression/builtin_miscellaneous_vec_test.go` - expression: Tests vectorized builtin miscellaneous eval one vec.
- `pkg/expression/builtin_op_test.go` - expression: Tests unary.
- `pkg/expression/builtin_op_vec_test.go` - expression: Tests vectorized builtin op func.
- `pkg/expression/builtin_other_test.go` - expression: Tests bit count.
- `pkg/expression/builtin_other_vec_generated_test.go` - expression: Tests vectorized builtin other eval one vec generated.
- `pkg/expression/builtin_other_vec_test.go` - expression: Tests vectorized builtin other func.
- `pkg/expression/builtin_regexp_test.go` - expression: Tests regexp like.
- `pkg/expression/builtin_regexp_vec_const_test.go` - expression: Tests vectorized builtin regexp for constants.
- `pkg/expression/builtin_string_test.go` - expression: Tests length and octet length.
- `pkg/expression/builtin_string_vec_generated_test.go` - expression: Tests vectorized generated builtin string eval one vec.
- `pkg/expression/builtin_string_vec_test.go` - expression: Tests vectorized builtin string eval one vec.
- `pkg/expression/builtin_test.go` - expression: Tests is null func.
- `pkg/expression/builtin_time_test.go` - expression: Tests date.
- `pkg/expression/builtin_time_vec_generated_test.go` - expression: Tests vectorized builtin time eval one vec generated.
- `pkg/expression/builtin_time_vec_test.go` - expression: Tests vectorized builtin time eval one vec.
- `pkg/expression/builtin_vec_vec_test.go` - expression: Tests vectorized builtin vec func.
- `pkg/expression/builtin_vectorized_test.go` - expression: Tests mock vec plus int.
- `pkg/expression/collation_test.go` - expression: Tests collation hash equals.
- `pkg/expression/column_test.go` - expression: Tests column.
- `pkg/expression/constant_test.go` - expression: Tests constant propagation.
- `pkg/expression/distsql_builtin_test.go` - expression: Tests PB to expr.
- `pkg/expression/evaluator_test.go` - expression: Tests sleep.
- `pkg/expression/expr_to_pb_test.go` - expression: Tests constant to PB.
- `pkg/expression/expression_test.go` - expression: Tests new values func.
- `pkg/expression/function_traits_test.go` - expression: Tests unfoldable funcs.
- `pkg/expression/grouping_sets_test.go` - expression: Tests group sets target one.
- `pkg/expression/helper_test.go` - expression: Tests get time value.
- `pkg/expression/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/expression/scalar_function_test.go` - expression: Tests expression semantic equal.
- `pkg/expression/schema_test.go` - expression: Tests schema clone.
- `pkg/expression/simple_rewriter_test.go` - expression: Tests find field name.
- `pkg/expression/typeinfer_test.go` - expression: Tests infer type.
- `pkg/expression/util_test.go` - expression: Tests base builtin.

## pkg/expression/aggregation

### Tests
- `pkg/expression/aggregation/agg_to_pb_test.go` - expression/aggregation: Tests agg func to PB.
- `pkg/expression/aggregation/aggregation_test.go` - expression/aggregation: Tests avg.
- `pkg/expression/aggregation/base_func_test.go` - expression/aggregation: Tests clone.
- `pkg/expression/aggregation/bench_test.go` - expression/aggregation: Tests create context.
- `pkg/expression/aggregation/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/expression/aggregation/util_test.go` - expression/aggregation: Tests distinct.

## pkg/expression/exprctx

### Tests
- `pkg/expression/exprctx/context_override_test.go` - expression/exprctx: Tests ctx with handle truncate err level.
- `pkg/expression/exprctx/optional_test.go` - expression/exprctx: Tests optional prop key set.

## pkg/expression/expropt

### Tests
- `pkg/expression/expropt/optional_test.go` - expression/expropt: Tests optional eval prop providers.

## pkg/expression/exprstatic

### Tests
- `pkg/expression/exprstatic/evalctx_test.go` - expression/exprstatic: Tests new static eval ctx.
- `pkg/expression/exprstatic/exprctx_test.go` - expression/exprstatic: Tests new static expr ctx.

## pkg/expression/integration_test

### Tests
- `pkg/expression/integration_test/integration_test.go` - expression/integration_test: Tests FTS parser.
- `pkg/expression/integration_test/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/expression/sessionexpr

### Tests
- `pkg/expression/sessionexpr/sessionctx_test.go` - expression/sessionexpr: Tests session eval context basic.

## pkg/expression/test/constantpropagation

### Tests
- `pkg/expression/test/constantpropagation/constant_propagation_test.go` - expression/test/constantpropagation: Tests constant propagation.
- `pkg/expression/test/constantpropagation/main_test.go` - Configures default goleak settings and registers testdata.

## pkg/expression/test/multivaluedindex

### Tests
- `pkg/expression/test/multivaluedindex/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/expression/test/multivaluedindex/multi_valued_index_test.go` - expression/test/multivaluedindex: Tests write multi-valued index.
