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

//! Go `pkg/expression/aggregation/agg_to_pb.go`'s plan-to-PB half:
//! `GetTiPBExpr`, `AggFuncToPBExpr`, `AggFunctionModeToPB` and
//! `PBAggFuncModeToAggFuncMode`. Go asks the `kv.Client` whether the
//! request type is supported; this port's expression lowering decides from
//! its pushdown catalog (register row X01), so only a name with no PB type
//! is refused here.

use tidb_expr::pushdown_catalog::{to_pb_field_type_with_check, PushDownContext};
use tidb_kv::store_type::StoreType;
use tidb_proto::tipb;

use crate::names;
use crate::{AggFuncDesc, AggFunctionMode, BaseFuncDesc, ByItems};

impl BaseFuncDesc {
    /// Go `GetTiPBExpr`: the PB type of the function, `Null` when it has
    /// none; a window function's only when `try_window_desc`.
    #[must_use]
    pub fn get_tipb_expr(&self, try_window_desc: bool) -> tipb::ExprType {
        use tipb::ExprType;
        let tp = match self.name.as_str() {
            names::COUNT => ExprType::Count,
            names::APPROX_COUNT_DISTINCT => ExprType::ApproxCountDistinct,
            names::FIRST_ROW => ExprType::First,
            names::GROUP_CONCAT => ExprType::GroupConcat,
            names::MAX => ExprType::Max,
            names::MIN => ExprType::Min,
            names::SUM => ExprType::Sum,
            names::SUM_INT => ExprType::SumInt,
            names::AVG => ExprType::Avg,
            names::BIT_OR => ExprType::AggBitOr,
            names::BIT_XOR => ExprType::AggBitXor,
            names::BIT_AND => ExprType::AggBitAnd,
            names::VAR_POP => ExprType::VarPop,
            names::JSON_ARRAYAGG => ExprType::JsonArrayAgg,
            names::JSON_OBJECTAGG => ExprType::JsonObjectAgg,
            names::STDDEV_POP => ExprType::StddevPop,
            names::VAR_SAMP => ExprType::VarSamp,
            names::STDDEV_SAMP => ExprType::StddevSamp,
            _ => ExprType::Null,
        };

        if tp != ExprType::Null || !try_window_desc {
            return tp;
        }

        match self.name.as_str() {
            names::ROW_NUMBER => ExprType::RowNumber,
            names::RANK => ExprType::Rank,
            names::DENSE_RANK => ExprType::DenseRank,
            names::CUME_DIST => ExprType::CumeDist,
            names::PERCENT_RANK => ExprType::PercentRank,
            names::NTILE => ExprType::Ntile,
            names::LEAD => ExprType::Lead,
            names::LAG => ExprType::Lag,
            names::FIRST_VALUE => ExprType::FirstValue,
            names::LAST_VALUE => ExprType::LastValue,
            names::NTH_VALUE => ExprType::NthValue,
            _ => tp,
        }
    }
}

impl AggFuncDesc {
    /// Go `AggFuncDesc.StringWithCtx`.
    #[must_use]
    pub fn string_with_ctx(&self, ctx: &dyn PushDownContext) -> String {
        let mut buffer = self.base.name.clone();
        buffer.push('(');
        if self.has_distinct {
            buffer.push_str("distinct ");
        }
        let args: Vec<String> = self.base.args.iter().map(|arg| ctx.expr_string(arg)).collect();
        buffer.push_str(&args.join(", "));
        if !self.order_by_items.is_empty() {
            buffer.push_str(" order by ");
        }
        let order_by: Vec<String> = self.order_by_items.iter().map(|item| by_items_string(ctx, item)).collect();
        buffer.push_str(&order_by.join(", "));
        buffer.push(')');
        buffer
    }
}

/// Go `ByItems.StringWithCtx`.
fn by_items_string(ctx: &dyn PushDownContext, item: &ByItems) -> String {
    if item.desc {
        format!("{} true", ctx.expr_string(&item.expr))
    } else {
        ctx.expr_string(&item.expr)
    }
}

/// Go `AggFuncToPBExpr`: converts aggregate function to pb.
///
/// # Errors
/// The function, an argument or its type has no PB form.
pub fn agg_func_to_pb_expr(
    ctx: &dyn PushDownContext,
    agg_func: &AggFuncDesc,
    store_type: StoreType,
) -> Result<tipb::Expr, String> {
    let tp = agg_func.base.get_tipb_expr(false);
    if tp == tipb::ExprType::Null {
        return Err("select request is not supported by client".to_owned());
    }

    let mut children = Vec::with_capacity(agg_func.base.args.len());
    for arg in &agg_func.base.args {
        let pb_arg = ctx
            .expr_to_pb(arg)
            .ok_or_else(|| agg_func.string_with_ctx(ctx) + " can't be converted to PB.")?;
        children.push(pb_arg);
    }
    let ret_type = to_pb_field_type_with_check(&agg_func.base.ret_type, store_type)?;

    let mut expr = tipb::Expr {
        tp: Some(tp as i32),
        children,
        // gogoproto's non-nullable `sig` is always sent.
        sig: Some(tipb::ScalarFuncSig::Unspecified as i32),
        field_type: Some(ret_type),
        has_distinct: Some(agg_func.has_distinct),
        agg_func_mode: Some(agg_function_mode_to_pb(agg_func.mode) as i32),
        ..tipb::Expr::default()
    };
    if tp == tipb::ExprType::GroupConcat {
        let mut order_by = Vec::with_capacity(agg_func.order_by_items.len());
        for arg in &agg_func.order_by_items {
            let pb_arg = ctx
                .expr_to_pb(&arg.expr)
                .ok_or_else(|| agg_func.string_with_ctx(ctx) + " can't be converted to PB.")?;
            order_by.push(tipb::ByItem { expr: Some(pb_arg), desc: Some(arg.desc), ..tipb::ByItem::default() });
        }
        // encode GroupConcatMaxLen
        let mut val = Vec::new();
        tidb_codec::encode_uint(&mut val, ctx.group_concat_max_len());
        expr.val = Some(val);
        expr.order_by = order_by;
    }
    Ok(expr)
}

/// Go `AggFunctionModeToPB`.
#[must_use]
pub fn agg_function_mode_to_pb(mode: AggFunctionMode) -> tipb::AggFunctionMode {
    match mode {
        AggFunctionMode::Complete => tipb::AggFunctionMode::CompleteMode,
        AggFunctionMode::Final => tipb::AggFunctionMode::FinalMode,
        AggFunctionMode::Partial1 => tipb::AggFunctionMode::Partial1Mode,
        AggFunctionMode::Partial2 => tipb::AggFunctionMode::Partial2Mode,
        AggFunctionMode::Dedup => tipb::AggFunctionMode::DedupMode,
    }
}

/// Go `PBAggFuncModeToAggFuncMode`: the default mode of the aggregate
/// function is `Partial1Mode`.
#[must_use]
pub fn pb_agg_func_mode_to_agg_func_mode(pb_mode: Option<tipb::AggFunctionMode>) -> AggFunctionMode {
    match pb_mode {
        Some(tipb::AggFunctionMode::CompleteMode) => AggFunctionMode::Complete,
        Some(tipb::AggFunctionMode::FinalMode) => AggFunctionMode::Final,
        Some(tipb::AggFunctionMode::Partial2Mode) => AggFunctionMode::Partial2,
        Some(tipb::AggFunctionMode::DedupMode) => AggFunctionMode::Dedup,
        Some(tipb::AggFunctionMode::Partial1Mode) | None => AggFunctionMode::Partial1,
    }
}
