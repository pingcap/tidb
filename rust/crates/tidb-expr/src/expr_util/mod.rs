// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SEED of Go `pkg/expression`, covering the expression-tree UTILITIES that
//! every logical optimization rule in `pkg/planner/core` calls: column
//! extraction, column substitution, CNF/DNF normal-form handling, constant
//! helpers, NOT push-down, and the tree predicates that guard a rewrite.
//!
//! `pkg/expression` is far too large to complete in one unit. This module is
//! explicitly a SEED, and its boundaries are named below rather than implied.
//!
//! # Go sources ported here
//!
//! - `pkg/expression/util.go` -- the bulk of this module.
//! - `pkg/expression/expression.go` -- only the normal-form and null-evaluation
//!   helpers that live there rather than in `util.go`:
//!   `splitNormalFormItems` (`:920`), `SplitCNFItems` (`:939`),
//!   `SplitDNFItems` (`:945`), `extractBinaryOpItems` (`:851`),
//!   `FlattenDNFConditions` (`:865`), `FlattenCNFConditions` (`:871`),
//!   `EvaluateExprWithNull` (`:953`) with `evaluateExprWithNull` (`:964`) and
//!   `evaluateExprWithNullInNullRejectCheck` (`:993`).
//! - `pkg/expression/schema.go:134` `ExprFromSchema` -- pulled in because
//!   `DeriveRelaxedFiltersFromDNF` is unwritable without it and `schema.rs`
//!   does not have it.
//! - `pkg/expression/constant_fold.go` -- the public `FoldConstant` entry
//!   point and its four special handlers; see [`fold`].
//! - `pkg/expression/explain.go` -- the NORMALIZED half only; see
//!   [`explain_normalized`].
//! - `pkg/expression/function_traits.go:48` `unFoldableFunctions` and `:224`
//!   `mutableEffectsFunctions` -- the two trait tables that
//!   `IsRuntimeConstExpr`, `CheckNonDeterministic`, `IsMutableEffectsExpr` and
//!   `IsImmutableFunc` are defined against.
//! - `pkg/expression/collation.go:177` `CollationStrictnessGroup` and `:192`
//!   `CollationStrictness` -- the two tables `checkCollationStrictness` reads.
//!
//! # Already ported elsewhere in this crate -- re-exported, NOT duplicated
//!
//! `simple_expr.rs` reached `util.go` first for three extractors and the two
//! condition composers. They are re-exported from here so a downstream planner
//! rule has one import path, and no second implementation exists:
//!
//! - Go `ExtractColumns` (`util.go:127`) -> [`crate::simple_expr::extract_columns`]
//! - Go `ExtractCorColumns` (`util.go:140`) -> [`crate::simple_expr::extract_cor_columns`]
//! - Go `ExtractColumnsFromExpressions` (`util.go:164`) ->
//!   [`crate::simple_expr::extract_columns_from_expressions`]
//! - Go `ComposeCNFCondition` (`expression.go:842`) ->
//!   [`crate::simple_expr::compose_cnf_condition`]
//! - Go `ComposeDNFCondition` (`expression.go:847`) ->
//!   [`crate::simple_expr::compose_dnf_condition`]
//!
//! `constant_fold.rs` already holds the rewriter-tier fold (`foldConstant`'s
//! bottom-up walk as the AST rewriter needs it, plus `folds_to_constant` /
//! `folded_value`). [`fold::fold_constant`] here is the PUBLIC `FoldConstant`
//! entry point over a built `Expression` tree, which that file does not
//! expose; it does not re-derive the walk, it drives the same predicates.
//!
//! # Boundaries (this is a seed, not the package)
//!
//! - Go `NewFunction` / `NewFunctionInternal`: every ported symbol that
//!   REBUILDS a function -- `ColumnSubstituteImpl`, `SubstituteCorCol2Constant`,
//!   `PushDownNot`, `EliminateNoPrecisionLossCast`, `EvaluateExprWithNull`,
//!   `PopRowFirstArg`, `BuildNotNullExpr` -- takes a [`FunctionBuilder`]
//!   rather than hard-wiring construction. Two are supplied:
//!   [`RealFunctionBuilder`] routes to the crate's real
//!   [`crate::new_function`], giving Go's argument type inference, arity
//!   checking, registry dispatch and post-construction folding, and is what
//!   callers with an evaluation context should use;
//!   [`PreservingFunctionBuilder`] is the context-free fallback that keeps the
//!   caller-supplied result type verbatim, for callers that only need the tree
//!   SHAPE. The rewrite rules themselves are identical under both.
//! - `// boundary:` Go `BuildCastFunctionWithCheck`. Go routes `cast` to a
//!   DEDICATED builder, which `new_function` explicitly refuses; that builder
//!   is not in this crate yet. [`FunctionBuilder::build_cast`] therefore
//!   constructs the cast node directly with the target type preserved, which
//!   is the whole of what `ColumnSubstituteImpl`'s cast arm reads off it.
//! - `// boundary:` Go `intset.FastIntSet`. Not in the workspace. Column-id
//!   sets use `BTreeSet<i64>`, which has the same membership semantics and a
//!   deterministic iteration order; only the small-set bitmap optimization is
//!   lost.
//! - `// boundary:` Go `sync.Pool`. `GetUniqueIDToColumnMap` /
//!   `PutUniqueIDToColumnMap` (`util.go:199`, `:204`) are an allocator pool
//!   around `map[int64]*Column` and are NOT ported: they carry no semantics,
//!   and a `BTreeMap` reused through `&mut` covers the one caller shape they
//!   serve (see [`extract::extract_columns_map_from_expressions_with_reused_map`]).
//! - `// boundary:` Go `Expression.Equal(ctx, e)`. `expression.rs` documents
//!   its `Expression::equal` as context-FREE: columns compare by `UniqueID`,
//!   while constants and scalar functions conservatively report `false`
//!   because Go compares them through a collator and a per-function
//!   `equal(ctx, ...)`. [`predicates::contains`] and
//!   [`extract::extract_constant_eq_columns_or_scalar`] inherit exactly that
//!   narrowing and no other.
//! - `// boundary:` Go `driver.ParamMarkerExpr`. It lives in
//!   `pkg/types/parser_driver`, above this crate. [`param::ParamMarkerValue`]
//!   is the two-field view (`Datum`, `Order`) that `ParamMarkerExpression`
//!   reads, so no driver type is duplicated.
//! - `// boundary:` `types.InferParamTypeFromDatum`. Not yet in
//!   `tidb-datatype`; [`param::param_marker_expression`] takes the inferred
//!   type as an argument rather than guessing one.
//!
//! # Not ported, and why
//!
//! - `Filter` (`util.go:82`) -- a generic slice filter; `Iterator::filter` is
//!   the Rust spelling and carries no TiDB semantics.
//! - `SQLDigestTextRetriever` (`util.go:1899`-`:2049`) -- runs real
//!   `SELECT`s through `expropt::SQLExecutor` against
//!   `information_schema`/`cluster_statements_summary`. It is a SQL client,
//!   not an expression utility, and belongs with the `expropt` unit.
//! - `ExecBinaryParam` and its `binary*` decoders (`util.go:2117`-`:2356`) --
//!   the MySQL binary-protocol parameter decoder, keyed on
//!   `param.BinaryParam` from `pkg/server`. It is protocol code that happens
//!   to produce `Expression`s; it belongs with the server-protocol unit.
//! - `GetFormatBytes` (`util.go:1804`) / `GetFormatNanoTime` (`:1843`) -- the
//!   bodies of the `FORMAT_BYTES` / `FORMAT_NANO_TIME` builtins, filed under
//!   `util.go` only by accident. They belong with `builtin_string.go`.
//! - `locateStringWithCollation` (`util.go:821`), `timeZone2int` (`:846`),
//!   `getValidPrefix` (`:735`) -- likewise builtin bodies (`LOCATE`,
//!   `CONVERT_TZ`, numeric-prefix parsing), not planner utilities.
//! - `symmetricOp` (`util.go:893`) -- an `opcode.Op` table with no reader in
//!   `util.go`; it is consumed by the ranger, and lands with it.
//! - `ExprsToStringsForDisplay` (`util.go:2050`) and the CONTEXT-DEPENDENT
//!   half of `explain.go` -- both need `Expression.StringWithCtx`, redaction
//!   modes and `Datum.TruncatedStringify`, none of which exist in the
//!   workspace yet. See [`explain_normalized`] for the half that does not.
//! - `ParamMarkerInPrepareChecker` (`util.go:1397`) -- an `ast.Visitor` over
//!   `driver.ParamMarkerExpr`, blocked by the same driver boundary as
//!   `ParamMarkerExpression` and carrying no logic beyond `!v.InExecute`.

pub mod builder;
pub mod explain_normalized;
pub mod extract;
pub mod fold;
pub mod normal_form;
pub mod param;
pub mod predicates;
pub mod push_not;
pub mod substitute;
pub mod traits;

#[cfg(test)]
mod tests;

pub use explain_normalized::{
    column_explain_info_normalized, explain_normalized_info, explain_normalized_info_4_in_list,
    sorted_explain_normalized_expression_list, sorted_explain_normalized_scalar_func_list,
};
pub use fold::{fold_constant, fold_constant_with, FoldOptions};
pub use param::{
    construct_position_expr, datum_to_constant, get_int_from_constant, get_string_from_constant,
    param_marker_expression, pos_from_position_expr, ConstantReadError, ParamMarkerValue,
    PositionExpr,
};
pub use predicates::{
    check_args_not_multi_column_row, check_func_in_expr, check_non_deterministic,
    const_expr_consider_plan_cache, contain_correlated_column, contain_mutable_const,
    contain_outer_not, contain_virtual_column, contains, disable_parse_json_flag_4_expr,
    expr_has_set_var_or_sleep, exprs_has_side_effects, get_expr_inside_is_truth, get_func_arg,
    get_row_len, get_uint64_from_constant, has_column_with_condition, is_const_null,
    is_immutable_func, is_mutable_effects_expr, is_runtime_const_expr,
    maybe_over_optimized_4_plan_cache, projection_benefits_from_pushed_down, remove_dup_exprs,
    remove_mutable_const,
};
pub use push_not::{
    eliminate_no_precision_loss_cast, no_precision_loss_cast_compatible, push_down_not,
};
pub use substitute::{
    build_not_null_expr, column_substitute, column_substitute_all, column_substitute_impl,
    evaluate_expr_with_null, pop_row_first_arg, substitute_cor_col_2_constant, SubstituteError,
    SubstituteOptions, SubstituteOutcome,
};

pub use normal_form::{
    derive_relaxed_filters_from_dnf, expr_from_schema, extract_filters_from_dnfs,
    flatten_cnf_conditions, flatten_dnf_conditions, split_cnf_items, split_dnf_items,
};

pub use builder::{
    FunctionBuildError, FunctionBuilder, PreservingFunctionBuilder, RealFunctionBuilder,
};
pub use extract::{
    extract_all_columns_from_expressions, extract_all_columns_from_expressions_in_used_slices,
    extract_column_set, extract_columns_and_cor_columns_from_expressions,
    extract_columns_from_col_op_col, extract_columns_map_from_expressions,
    extract_columns_map_from_expressions_with_reused_map, extract_columns_set_from_expressions,
    extract_constant_eq_columns_or_scalar, extract_cor_columns_from_expressions,
    extract_dependent_columns, extract_equivalence_columns, filter_out_in_place, find_upper_bound,
    is_col_op_col, set_expr_column_in_operand,
};
pub use traits::{is_mutable_effects_function, is_unfoldable_function};

// Re-exports of the `util.go` / `expression.go` surface that `simple_expr.rs`
// reached first. These are ALIASES, not reimplementations.
pub use crate::simple_expr::{
    compose_cnf_condition, compose_dnf_condition, extract_columns,
    extract_columns_from_expressions, extract_cor_columns,
};
