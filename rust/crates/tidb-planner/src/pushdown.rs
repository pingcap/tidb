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

//! The planner's TiKV push-down admission: the scoped slice of
//! `pkg/expression/infer_pushdown.go` its planner consumers read.
//!
//! Go asks `expression.CanExprsPushDown(ctx, exprs, kv.TiKV)` before
//! pushing a Projection (and the TopN machinery) onto a cop task. The full
//! file also carries the TiFlash/TiDB store gates, the pb-code conversion
//! checks, and the `tidb_opt_disable_expression_pushdown_blacklist` map;
//! this module ports what decides the TIKV answer for the ported
//! expression shapes.
//!
//! # Narrowings, each naming its Go symbol
//!
//! * `canExprPushDown`'s `Column`/`Constant`/`CorrelatedColumn` arms answer
//!   through pb-conversion probes (`columnToPBExpr`/`conOrCorColToPBExpr`),
//!   which fail only for shapes the ported expression tree does not carry
//!   (virtual generated columns, unconvertible types); they answer true
//!   here.
//! * The pb-code-CONDITIONAL function arms — `UnixTimestamp`, `Round`,
//!   `Rand`, `Conv`, `Extract`, `Regexp*` and kin — answer FALSE
//!   conservatively: an under-push only loses a candidate Go would price,
//!   while a wrong true would push an expression the store cannot run.
//! * `DefaultExprPushDownBlacklist` defaults empty and its sysvar is
//!   unported; the blacklist check is skipped as Go skips it when empty.
//! * `canFuncBePushed`'s TiFlash/TiDB halves and `canEnumPushdownPreliminarily`
//!   narrow with their tiers.

use tidb_expr::expression::Expression;

/// The unconditional arms of Go `scalarExprSupportedByTiKV`
/// (`infer_pushdown.go:186`), transcribed name for name. The commented-out
/// names in Go's switch are NOT here, exactly as they are not there.
const TIKV_SUPPORTED: &[&str] = &[
    // op functions.
    "and",
    "or",
    "xor",
    "not",
    "bitand",
    "bitor",
    "bitxor",
    "bitneg",
    "leftshift",
    "rightshift",
    "unaryminus",
    // compare functions.
    "lt",
    "le",
    "eq",
    "ne",
    "ge",
    "gt",
    "nulleq",
    "in",
    "isnull",
    "like",
    "istrue",
    "istrue_with_null",
    "isfalse",
    // arithmetical functions.
    "pi",
    "plus",
    "minus",
    "mul",
    "div",
    "abs",
    "mod",
    "intdiv",
    // math functions.
    "ceil",
    "ceiling",
    "floor",
    "sqrt",
    "sign",
    "ln",
    "log",
    "log2",
    "log10",
    "exp",
    "pow",
    "power",
    "sin",
    "asin",
    "cos",
    "acos",
    "atan",
    "atan2",
    "cot",
    "radians",
    "degrees",
    "crc32",
    // control flow functions.
    "case_when",
    "if",
    "ifnull",
    "coalesce",
    // string functions.
    "upper",
    "lower",
    "length",
    "bit_length",
    "concat",
    "concat_ws",
    "replace",
    "ascii",
    "hex",
    "reverse",
    "ltrim",
    "rtrim",
    "strcmp",
    "space",
    "elt",
    "field",
    "from_binary",
    "to_binary",
    "mid",
    "substring",
    "substr",
    "char_length",
    "right",
    // json functions.
    "json_type",
    "json_extract",
    "json_object",
    "json_array",
    "json_merge",
    "json_set",
    "json_insert",
    "json_replace",
    "json_remove",
    "json_length",
    "json_merge_patch",
    "json_unquote",
    "json_contains",
    "json_valid",
    "json_memberof",
    "json_array_append",
    // vector functions.
    "vec_dims",
    "vec_l1_distance",
    "vec_l2_distance",
    "vec_negative_inner_product",
    "vec_cosine_distance",
    "vec_l2_norm",
    "vec_as_text",
    // date functions.
    "date",
    "week",
    "datediff",
    "monthname",
    "makedate",
    "time_to_sec",
    "maketime",
    "date_format",
    "date_add",
    "adddate",
    "date_sub",
    "subdate",
    "hour",
    "minute",
    "second",
    "microsecond",
    "month",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "weekofyear",
    "year",
    "from_days",
    "period_add",
    "period_diff",
    "timestampdiff",
    "from_unixtime",
    "sysdate",
    // encryption functions.
    "md5",
    "sha1",
    "sha2",
    "uncompressed_length",
    "cast",
    // misc functions.
    "uuid",
    "uuid_version",
    "uuid_timestamp",
];

/// Go `CanExprsPushDown(ctx, exprs, kv.TiKV)` (`infer_pushdown.go:589`)
/// over the ported expression shapes: every expression must be pushable,
/// and a scalar function's ARGUMENTS recurse (`canScalarFuncPushDown`'s
/// parameter check).
#[must_use]
pub fn can_exprs_push_down_tikv(exprs: &[Expression]) -> bool {
    exprs.iter().all(can_expr_push_down_tikv)
}

fn can_expr_push_down_tikv(expr: &Expression) -> bool {
    match expr {
        Expression::Column(_) | Expression::Constant(_) | Expression::CorrelatedColumn(_) => true,
        Expression::ScalarFunction(func) => {
            TIKV_SUPPORTED.contains(&func.func_name.lowercase())
                && can_exprs_push_down_tikv(&func.args)
        }
    }
}

#[cfg(test)]
mod tests {
    // Go's coverage lives in expression integration suites
    // (`TestExprPushDownToTiKV` is testkit-bound); these pin the transcribed
    // table's decision shape.

    use super::*;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::Constant;
    use tidb_expr::scalar_function::ScalarFunction;

    fn func(name: &str, args: Vec<Expression>) -> Expression {
        let mut sf = ScalarFunction::default();
        sf.func_name = tidb_ast::CiString::new(name.to_owned());
        sf.args = args;
        Expression::ScalarFunction(sf)
    }

    #[test]
    fn the_supported_table_admits_and_the_conditional_arms_refuse() {
        let col = Expression::Column(Column::new(1, FieldType::new(FieldTypeCode::LongLong)));
        let konst = Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::LongLong),
        ));
        // eq(col, const): admitted, arguments recursed.
        assert!(can_exprs_push_down_tikv(&[func(
            "eq",
            vec![col.clone(), konst.clone()]
        )]));
        // A conditional arm (`Round`, pb-code gated in Go) refuses
        // conservatively.
        assert!(!can_exprs_push_down_tikv(&[func(
            "round",
            vec![col.clone()]
        )]));
        // An admitted function with a refused ARGUMENT refuses whole.
        assert!(!can_exprs_push_down_tikv(&[func(
            "eq",
            vec![func("round", vec![col]), konst]
        )]));
    }
}
