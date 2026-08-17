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

//! Go `pkg/expression/aggregation`: the aggregate-function DESCRIPTOR that the
//! planner builds and the executor consumes.
//!
//! The Go package has two halves. This module ports the DESCRIPTOR half --
//! the part that is shared vocabulary between planner and executor -- and
//! deliberately leaves the EVALUATOR half where the workspace already put it.
//!
//! # Ported (per Go file)
//!
//! | Go file | Go symbol | line | Rust |
//! | --- | --- | --- | --- |
//! | `base_func.go` | `baseFuncDesc` | 34 | [`BaseFuncDesc`] |
//! | `base_func.go` | `newBaseFuncDesc` | 44 | [`BaseFuncDesc::new`] |
//! | `base_func.go` | `baseFuncDesc.Equals` | 66 | [`BaseFuncDesc::equals`] |
//! | `base_func.go` | `baseFuncDesc.equal` | 88 | [`BaseFuncDesc::equal`] |
//! | `base_func.go` | `baseFuncDesc.clone` | 100 | `#[derive(Clone)]` |
//! | `base_func.go` | `baseFuncDesc.TypeInfer` | 122 | [`BaseFuncDesc::type_infer`] |
//! | `base_func.go` | `typeInfer4Count` | 161 | `type_infer_4_count` |
//! | `base_func.go` | `typeInfer4ApproxCountDistinct` | 170 | inline |
//! | `base_func.go` | `typeInfer4ApproxPercentile` | 174 | `type_infer_4_approx_percentile` |
//! | `base_func.go` | `typeInfer4Sum` | 215 | `type_infer_4_sum` |
//! | `base_func.go` | `typeInfer4SumInt` | 238 | `type_infer_4_sum_int` |
//! | `base_func.go` | `TypeInfer4AvgSum` | 258 | [`BaseFuncDesc::type_infer_4_avg_sum`] |
//! | `base_func.go` | `TypeInfer4FinalCount` | 277 | [`BaseFuncDesc::type_infer_4_final_count`] |
//! | `base_func.go` | `typeInfer4Avg` | 283 | `type_infer_4_avg` |
//! | `base_func.go` | `typeInfer4GroupConcat` | 310 | `type_infer_4_group_concat` |
//! | `base_func.go` | `typeInfer4MaxMin` | 345 | `type_infer_4_max_min` |
//! | `base_func.go` | `typeInfer4BitFuncs` | 369 | `type_infer_4_bit_funcs` |
//! | `base_func.go` | `typeInfer4JsonArrayAgg` | 376 | `type_infer_4_json_array_agg` |
//! | `base_func.go` | `typeInfer4JsonObjectAgg` | 381 | `type_infer_4_json_object_agg` |
//! | `base_func.go` | `typeInfer4NumberFuncs` | 388 | `type_infer_4_number_funcs` |
//! | `base_func.go` | `typeInfer4CumeDist` | 394 | `type_infer_4_cume_dist` |
//! | `base_func.go` | `typeInfer4Ntile` | 400 | `type_infer_4_ntile` |
//! | `base_func.go` | `typeInfer4PercentRank` | 407 | `type_infer_4_percent_rank` |
//! | `base_func.go` | `typeInfer4LeadLag` | 413 | `type_infer_4_lead_lag` |
//! | `base_func.go` | `typeInfer4PopOrSamp` | 423 | `type_infer_4_pop_or_samp` |
//! | `base_func.go` | `GetDefaultValue` | 447 | [`BaseFuncDesc::get_default_value`] |
//! | `base_func.go` | `noNeedCastAggFuncs` | 465 | `NO_NEED_CAST_AGG_FUNCS` |
//! | `base_func.go` | `WrapCastForAggArgs` | 478 | [`BaseFuncDesc::wrap_cast_for_agg_args`] |
//! | `descriptor.go` | `AggFuncDesc` | 33 | [`AggFuncDesc`] |
//! | `descriptor.go` | `NewAggFuncDesc` | 47 | [`AggFuncDesc::new`] |
//! | `descriptor.go` | `NewAggFuncDescForWindowFunc` | 56 | [`AggFuncDesc::new_for_window_func`] |
//! | `descriptor.go` | `AggFuncDesc.Equals` | 75 | [`AggFuncDesc::equals`] |
//! | `descriptor.go` | `AggFuncDesc.Equal` | 122 | [`AggFuncDesc::equal`] |
//! | `descriptor.go` | `AggFuncDesc.Clone` | 138 | `#[derive(Clone)]` |
//! | `descriptor.go` | `AggFuncDesc.Split` | 152 | [`AggFuncDesc::split`] |
//! | `descriptor.go` | `EvalNullValueInOuterJoin` | 240 | [`AggFuncDesc::eval_null_value_in_outer_join`] |
//! | `descriptor.go` | `evalNullValueInOuterJoin4Count` | 289 | inline |
//! | `descriptor.go` | `evalNullValueInOuterJoin4Sum` | 303 | inline |
//! | `descriptor.go` | `evalNullValueInOuterJoin4BitAnd` | 314 | inline |
//! | `descriptor.go` | `evalNullValueInOuterJoin4BitOr` | 325 | inline |
//! | `descriptor.go` | `UpdateNotNullFlag4RetType` | 337 | [`AggFuncDesc::update_not_null_flag_4_ret_type`] |
//! | `aggregation.go` | `AggFunctionMode` | 116 | [`AggFunctionMode`] |
//! | `aggregation.go` | `AggFunctionMode.ToString` | 137 | [`AggFunctionMode::as_str`] |
//! | `aggregation.go` | `NeedCount` | 197 | [`need_count`] |
//! | `aggregation.go` | `NeedValue` | 202 | [`need_value`] |
//! | `aggregation.go` | `IsAllFirstRow` | 213 | [`is_all_first_row`] |
//! | `aggregation.go` | `CheckAggPushDown` | 223 | [`check_agg_push_down`] |
//! | `aggregation.go` | `checkVectorAggPushDown` | 254 | `check_vector_agg_push_down` |
//! | `aggregation.go` | `CheckAggPushFlash` | 268 | [`check_agg_push_flash`] |
//! | `explain.go` | `ExplainAggFunc` | 26 | [`explain_agg_func_normalized`] (half, see below) |
//! | `window_func.go` | `WindowFuncDesc` | 27 | [`WindowFuncDesc`] |
//! | `window_func.go` | `NewWindowFuncDesc` | 32 | [`WindowFuncDesc::new`] |
//! | `window_func.go` | `noFrameWindowFuncs` | 85 | `NO_FRAME_WINDOW_FUNCS` |
//! | `window_func.go` | `UseDefaultFrame` | 108 | [`use_default_frame`] |
//! | `window_func.go` | `NeedFrame` | 113 | [`need_frame`] |
//! | `window_func.go` | `WindowFuncDesc.Clone` | 119 | `#[derive(Clone)]` |
//! | `builtin_cast.go` | `WrapWithCastAs*` | 2666-2886 | [`wrap_cast`] (SEED, see below) |
//!
//! The name constants are `pkg/parser/ast/functions.go:820-856` (aggregate)
//! and `:950-970` (window), reproduced verbatim in [`names`].
//!
//! # COMPLETE vs SEED
//!
//! This module is a **SEED** of `pkg/expression/aggregation`, not a complete
//! package claim. The package's evaluator half -- the `Aggregation` interface
//! (`aggregation.go:32`), `aggFunction` (`:154`), `AggEvaluateContext`
//! (`:107`), `NewDistAggFunc` (`:51`), `GetAggFunc` (`descriptor.go:264`) and
//! the eleven per-kind files (`avg.go`, `sum.go`, `sum_int.go`, `count.go`,
//! `concat.go`, `first_row.go`, `max_min.go`, `bit_and.go`, `bit_or.go`,
//! `bit_xor.go`, `util.go`'s `distinctChecker`/`calculateSum`) -- is NOT here.
//! It is not missing from the workspace: it already lives in
//! `tidb-executor`'s `hash_agg.rs` and `tidb-exec`'s `aggregate/runtime/`,
//! ported from `pkg/executor/aggfuncs` (the production evaluator) rather than
//! from this package's mock-coprocessor twin. Re-porting it here would be a
//! third evaluator, which is exactly what this module exists to prevent.
//!
//! [`wrap_cast`] is likewise a SEED: it holds only the `WrapWithCastAs*`
//! family of `pkg/expression/builtin_cast.go`, because
//! `WrapCastForAggArgs`/`typeInfer4BitFuncs`/`typeInfer4JsonObjectAgg` cannot
//! be ported without it. It makes no claim on `builtin_cast.go`.
//!
//! # Relationship to the executor's existing aggregate types
//!
//! Three aggregate vocabularies already exist in the workspace. None is
//! edited by this module; here is exactly how they relate.
//!
//! 1. **`tidb-executor::hash_agg::{AggFunc, AggKind, BitOp}`** -- the RUNTIME
//!    shape. `AggKind` is a closed enum whose variants carry what the FOLD
//!    needs (`GroupConcat{separator}`, `Bit(BitOp)`, `Variance{sample,sqrt}`,
//!    `JsonArrayAgg{value_type}`, `ApproxPercentile(Option<i64>)`), and
//!    `AggFunc` splits Go's one `Args` slice into `arg`/`extra_args` plus a
//!    denormalized `arg_orig_name`. It is NOT a competing descriptor: it is
//!    Go's `AggFuncDesc` after `buildAggFunc` has already resolved the name to
//!    a concrete `aggfuncs.AggFunc` -- a stage BELOW this one. [`AggFuncDesc`]
//!    keeps Go's own shape: the name as a lowercase string, ONE flat `args`
//!    vector, the inferred `ret_type`, [`AggFunctionMode`], `has_distinct`,
//!    and `order_by_items`.
//!
//!    A follow-up that unifies them would: (a) add
//!    `TryFrom<&AggFuncDesc> for AggFunc` in `tidb-executor`, deriving
//!    `AggKind` from `desc.name()` plus `desc.ret_type()` -- every variant
//!    payload is recoverable (`separator` is the last `args` element for
//!    `group_concat`, `sample`/`sqrt` follow from the four variance names,
//!    `value_type` is `args[0].static_type()`, `ApproxPercentile`'s percent is
//!    the constant `args[1]`); (b) delete `AggFunc::{distinct, order_by}`,
//!    reading `desc.has_distinct` / `desc.order_by_items` instead; and (c)
//!    keep `AggKind::FinalCount` and `arg_orig_name`, which have NO Go
//!    `AggFuncDesc` counterpart -- `FinalCount` is this crate's spelling of
//!    the mode dimension ([`AggFunctionMode::Final`] on a `count`) and
//!    `arg_orig_name` is display state that Go reads back off `*Column`.
//!    Only (c) is a genuine shape difference, and it is a superset, not a
//!    conflict.
//!
//! 2. **`tidb-executor::driver::agg_build`** -- an AST-driven partial port of
//!    `typeInfer4Sum` / `typeInfer4Avg` / `TypeInfer4AvgSum` /
//!    `group_concat_result_type` fused with the select-field rewrite. Its
//!    inference arms are the same Go source lines this module ports, so it is
//!    a genuine DUPLICATE of [`BaseFuncDesc::type_infer`]'s sum/avg/
//!    group-concat arms and should be replaced by calls into
//!    [`BaseFuncDesc::type_infer`] once that crate can depend on this one. It
//!    covers fewer kinds (no window functions, no `approx_percentile`, no
//!    `json_*`) and does not implement `WrapCastForAggArgs`.
//!
//! 3. **`tidb-planner::aggregation_descriptor::AggFuncDesc<A, R, O>`** -- a
//!    GENERIC identity shell (name/args/ret/mode/distinct/order-by) whose type
//!    parameters exist because the planner had no concrete `Expression`-typed
//!    descriptor to point at. This module supplies exactly that type. The
//!    intended end state is `LogicalAggregation` holding
//!    `Vec<AggFuncDesc>` from here, with the planner's generic shell reduced
//!    to the hashing/identity adapter it is used for. That crate is NOT
//!    touched by this batch.
//!
//! # Narrowings, each named
//!
//! - **`RetTp` is a value, not a nullable pointer.** Go's `baseFuncDesc.RetTp`
//!   is `*types.FieldType` and both `Equals` and `Hash64` branch on nil.
//!   `newBaseFuncDesc` always assigns it before returning, so the nil branch
//!   is reachable only for a hand-built descriptor. [`BaseFuncDesc::ret_type`]
//!   is a plain `FieldType`; the nil arms of `Equals`/`Hash64` are therefore
//!   not reproduced. This also makes `NewAggFuncDescForWindowFunc`'s
//!   `desc.RetTp == nil` safety check (`descriptor.go:57`) unreachable, and
//!   [`AggFuncDesc::new_for_window_func`] documents it as such.
//! - **`Hash64` is not ported.** Go's `base.Hasher` lives in
//!   `pkg/planner/cascades/base` and hashes through `Expression.Hash64`,
//!   which this crate does not have (only `HashCode`). The identity that
//!   `TestAggFuncDesc` pins is reproduced through [`AggFuncDesc::equals`]
//!   instead, which discriminates on exactly the same six fields.
//! - **`MemoryUsage` is not ported** (`descriptor.go:389`,
//!   `base_func.go:519`). It sums `pkg/util/size` constants plus
//!   `Expression.MemoryUsage`, which this crate does not implement.
//! - **`StringWithCtx` is not ported** (`descriptor.go:98`,
//!   `base_func.go:110`). It needs `Expression.StringWithCtx` and the redact
//!   modes -- the same boundary [`crate::expr_util`] already names for
//!   `ExprsToStringsForDisplay`.
//! - **`ExplainAggFunc`'s context-dependent half is not ported.** Only the
//!   `normalized == true` branch has a Rust counterpart
//!   ([`crate::expr_util::explain_normalized_info`]); the `ExplainInfo(ctx)`
//!   branch shares the `StringWithCtx` boundary above. Its `show-agg-mode`
//!   failpoint becomes an explicit `show_mode` parameter, since the workspace
//!   has no failpoint injection.
//! - **`AggFuncDesc.Equal(ctx, other)` compares arguments context-free.**
//!   Go's `Expression.Equal(ctx, e)` evaluates constants through a collator;
//!   [`Expression::equal`](crate::expression::Expression::equal) is the
//!   context-free part of it and answers `false` for constants and scalar
//!   functions. [`AggFuncDesc::equal`] inherits exactly that narrowing and no
//!   other. [`AggFuncDesc::equals`] (structural, Go's `base.Equals`) has no
//!   such gap and is the one to prefer.
//! - **`OrderByItems` is `Vec<ByItems>` defined HERE**, not imported from
//!   `pkg/planner/util`. `util.ByItems` is `{Expr Expression; Desc bool}` and
//!   the planner crate depends on this one, so the type cannot come from
//!   there without a cycle. `tidb-planner::by_item::ByItem` is a
//!   display-string shell, not an `Expression` holder, so it is not the same
//!   type either.
//! - **`WrapWithCastAsDecimal`'s constant-refinement tail is dropped**
//!   (`builtin_cast.go:2836`-`:2845`): it calls `EvalDecimal` on the built
//!   cast node and narrows flen/decimal to the result's actual precision.
//!   See [`wrap_cast`] for why.
//! - **`WrapWithCastAsString`'s `CoercibilityExplicit` branch reads the
//!   expression's FIELD TYPE charset**, not a separate `collationInfo`; see
//!   [`wrap_cast`].
//! - **`ConstLevel == ConstStrict` stands in for Go's
//!   `ConstLevel() != ConstNone`** in `typeInfer4ApproxPercentile`'s constant
//!   check, and the percentage argument is read from a `Constant` node rather
//!   than through `EvalInt`'s implicit conversion; see
//!   [`BaseFuncDesc::type_infer`].
//!
//! # Skipped, with reasons
//!
//! - **`agg_to_pb.go` in full** (`GetTiPBExpr`, `AggFuncToPBExpr`,
//!   `AggFunctionModeToPB`, `PBAggFuncModeToAggFuncMode`,
//!   `PBExprToAggFuncDesc`, and `window_func.go`'s `WindowFuncToPBExpr`).
//!   `tidb-proto`'s `select.proto` is a deliberately dependency-closed
//!   PROJECTION of `tipb`: its `ExprType` enum has five members
//!   (`Null`/`Int64`/`String`/`ColumnRef`/`ScalarFunc`) and carries NONE of
//!   the ~25 aggregate/window `ExprType` values these functions switch on.
//!   `tipb.AggFunctionMode` and `tipb.ByItem` are absent entirely, and
//!   `tipb.Expr` in that projection has no `agg_func_mode`/`order_by` fields.
//!   Porting would require extending `tidb-proto`, which this batch does not
//!   own. [`AggFunctionMode::ordinal`] exposes the wire discriminant so the
//!   conversion is a one-liner once the proto grows.
//! - **`window_func.go`'s `CanPushDownToTiFlash`** -- needs
//!   `expression.CanExprsPushDown` over a `PushDownContext` (client +
//!   converter), a distsql-layer object this crate does not model.
//! - **`util.go`'s `distinctChecker`/`calculateSum`** and `bench_test.go` --
//!   evaluator half, see COMPLETE vs SEED above.

use crate::expression::Expression;
use crate::infer_pushdown::{is_push_down_enabled, PushDownStore};
use std::collections::HashMap;
use tidb_datatype::FieldTypeCode;

mod base_func;
mod descriptor;
mod explain;
pub mod names;
mod window_func;
pub mod wrap_cast;

#[cfg(test)]
mod tests;

pub use base_func::{AggDescError, BaseFuncDesc};
pub use descriptor::{AggFuncDesc, ByItems};
pub use explain::explain_agg_func_normalized;
pub use window_func::{need_frame, use_default_frame, WindowFrameDefault, WindowFuncDesc};

/// Go `AggFunctionMode` (`aggregation.go:116`): the aggregate's execution
/// stage.
///
/// ```text
/// |-----------------|--------------|--------------|
/// | AggFunctionMode | input        | output       |
/// |-----------------|--------------|--------------|
/// | CompleteMode    | origin data  | final result |
/// | FinalMode       | partial data | final result |
/// | Partial1Mode    | origin data  | partial data |
/// | Partial2Mode    | partial data | partial data |
/// | DedupMode       | origin data  | origin data  |
/// |-----------------|--------------|--------------|
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum AggFunctionMode {
    /// Go `CompleteMode`, the zero value.
    #[default]
    Complete,
    /// Go `FinalMode`.
    Final,
    /// Go `Partial1Mode`.
    Partial1,
    /// Go `Partial2Mode`.
    Partial2,
    /// Go `DedupMode`.
    Dedup,
}

impl AggFunctionMode {
    /// Go `AggFunctionMode.ToString` (`aggregation.go:137`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Final => "final",
            Self::Partial1 => "partial1",
            Self::Partial2 => "partial2",
            Self::Dedup => "deduplicate",
        }
    }

    /// The source `iota` discriminant, which is also the value
    /// `AggFunctionMode(*expr.AggFuncMode)` reads off the wire
    /// (`aggregation.go:63`).
    #[must_use]
    pub const fn ordinal(self) -> i32 {
        match self {
            Self::Complete => 0,
            Self::Final => 1,
            Self::Partial1 => 2,
            Self::Partial2 => 3,
            Self::Dedup => 4,
        }
    }

    /// The inverse of [`AggFunctionMode::ordinal`]. `None` for a discriminant
    /// the source enum does not define.
    #[must_use]
    pub const fn from_ordinal(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Complete),
            1 => Some(Self::Final),
            2 => Some(Self::Partial1),
            3 => Some(Self::Partial2),
            4 => Some(Self::Dedup),
            _ => None,
        }
    }
}

/// Go `NeedCount` (`aggregation.go:197`): whether the aggregate records a
/// count in its partial state.
#[must_use]
pub fn need_count(name: &str) -> bool {
    name == names::COUNT || name == names::AVG
}

/// Go `NeedValue` (`aggregation.go:202`): whether the aggregate records a
/// value in its partial state.
#[must_use]
pub fn need_value(name: &str) -> bool {
    matches!(
        name,
        names::SUM
            | names::SUM_INT
            | names::AVG
            | names::FIRST_ROW
            | names::MAX
            | names::MIN
            | names::GROUP_CONCAT
            | names::BIT_OR
            | names::BIT_AND
            | names::BIT_XOR
            | names::APPROX_PERCENTILE
    )
}

/// Go `IsAllFirstRow` (`aggregation.go:213`).
#[must_use]
pub fn is_all_first_row(agg_funcs: &[AggFuncDesc]) -> bool {
    agg_funcs.iter().all(|f| f.name() == names::FIRST_ROW)
}

/// The static type of an argument, defaulting to the `NULL` type for a node
/// with no `RetType` (Go's nil pointer would panic; this crate's
/// `static_type` returns `None`).
fn arg_type_code(expr: &Expression) -> FieldTypeCode {
    expr.static_type().map_or(FieldTypeCode::Null, |t| t.code())
}

/// Go `checkVectorAggPushDown` (`aggregation.go:254`): an aggregate over a
/// `VECTOR` column is only pushable when it is one of the four
/// value-agnostic kinds.
fn check_vector_agg_push_down(agg_func: &AggFuncDesc) -> bool {
    match agg_func.name() {
        names::COUNT | names::MIN | names::MAX | names::FIRST_ROW => true,
        _ => agg_func
            .args()
            .first()
            .is_none_or(|arg| arg_type_code(arg) != FieldTypeCode::VectorFloat32),
    }
}

/// Go `CheckAggPushFlash` (`aggregation.go:268`).
#[must_use]
pub fn check_agg_push_flash(agg_func: &AggFuncDesc) -> bool {
    if agg_func
        .args()
        .iter()
        .any(|arg| arg_type_code(arg) == FieldTypeCode::Duration)
    {
        return false;
    }
    match agg_func.name() {
        names::COUNT
        | names::MIN
        | names::MAX
        | names::FIRST_ROW
        | names::APPROX_COUNT_DISTINCT => true,
        // TiFlash has no CastJsonAsReal / CastJsonAsString.
        names::SUM | names::SUM_INT | names::AVG | names::GROUP_CONCAT => agg_func
            .args()
            .first()
            .is_none_or(|arg| arg_type_code(arg) != FieldTypeCode::Json),
        _ => false,
    }
}

/// Go `CheckAggPushDown` (`aggregation.go:223`): whether the aggregate may be
/// pushed to `store`.
///
/// `blacklist` is the atomically published `expr_pushdown_blacklist` map that
/// [`is_push_down_enabled`] reads; Go reaches it through a package-level
/// variable, which this crate makes an explicit parameter (the same shape
/// [`crate::infer_pushdown`] already uses).
#[must_use]
pub fn check_agg_push_down(
    agg_func: &AggFuncDesc,
    store: PushDownStore,
    blacklist: &HashMap<String, u32>,
) -> bool {
    if !agg_func.order_by_items.is_empty() && agg_func.name() != names::GROUP_CONCAT {
        return false;
    }
    if agg_func.name() == names::APPROX_PERCENTILE {
        return false;
    }
    if store != PushDownStore::TiFlash && agg_func.name() == names::APPROX_COUNT_DISTINCT {
        // approx_count_distinct is only pushable to TiFlash today.
        return false;
    }
    if !check_vector_agg_push_down(agg_func) {
        return false;
    }
    let ret = match store {
        PushDownStore::TiFlash => check_agg_push_flash(agg_func),
        // TiKV does not support group_concat.
        PushDownStore::TiKv => agg_func.name() != names::GROUP_CONCAT,
        _ => true,
    };
    // No `strings.ToLower` here: `BaseFuncDesc::new` already lower-cased it.
    ret && is_push_down_enabled(blacklist, agg_func.name(), store)
}
