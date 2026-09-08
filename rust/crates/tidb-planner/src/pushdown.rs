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

//! TiKV expression push-down admission.
//!
//! Go `canExprPushDown` first proves that an expression has a protobuf
//! representation, and `canScalarFuncPushDown` then applies the source-owned
//! TiKV policy recursively. Rust keeps that policy in
//! [`tidb_expr::infer_pushdown`] and concrete signatures in
//! [`tidb_expr::pushdown_catalog`]. Keeping a second function-name whitelist in
//! the planner let these answers drift (notably for Go's PbCode-dependent
//! `ROUND` arm), so this module now only composes the two shared receipts.

use tidb_expr::expression::Expression;

/// Go `CanExprsPushDown(ctx, exprs, kv.TiKV)` over the expression families
/// represented by the shared TiPB scalar catalog.
#[must_use]
pub fn can_exprs_push_down_tikv(exprs: &[Expression]) -> bool {
    exprs.iter().all(can_expr_push_down_tikv)
}

fn can_expr_push_down_tikv(expr: &Expression) -> bool {
    use tidb_expr::infer_pushdown::{scalar_expr_supported_by_tikv, PushDownPolicy};
    use tidb_expr::pushdown_catalog::{PbScalar, ScalarFuncSig};

    let Expression::ScalarFunction(function) = expr else {
        // Go probes columns/constants/correlated columns through PbConverter.
        // Every such node represented by this physical expression tree has
        // already passed the expression rewriter's type construction.
        return true;
    };
    if !function.args.iter().all(can_expr_push_down_tikv) {
        return false;
    }
    let name = function.func_name.lowercase();
    // Go builds every cast under the single name `ast.Cast`, which
    // `scalarExprSupportedByTiKV` admits unconditionally
    // (`infer_pushdown.go:246`). The dedicated-cast transcreation names each
    // target type (`cast_decimal`, `cast_signed`, ...), so map that family
    // back to Go's name before the shared policy sees it. Without this the
    // policy answers "not pushable" for `not(isnull(cast_decimal(col)))` and
    // the derived NOT NULL filter is left above the projection that defines
    // the cast instead of inside the cop reader.
    let policy_name = if name.starts_with("cast_") {
        "cast"
    } else {
        name.as_ref()
    };
    let signature = match tidb_expr::pushdown_catalog::from_expression(expr) {
        Some(PbScalar::Call { signature, .. }) => signature.sig,
        _ => ScalarFuncSig::Unspecified,
    };
    // Go's conditional arms read PbCode. A name-only answer for one would be
    // speculation, so require the shared signature catalog to resolve it.
    if signature == ScalarFuncSig::Unspecified
        && matches!(
            policy_name,
            "if" | "ifnull"
                | "case"
                | "unix_timestamp"
                | "conv"
                | "round"
                | "rand"
                | "regexp"
                | "regexp_like"
                | "regexp_substr"
                | "regexp_instr"
                | "regexp_replace"
        )
    {
        return false;
    }
    scalar_expr_supported_by_tikv(&PushDownPolicy::new(policy_name, signature))
}

#[cfg(test)]
mod tests {
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
    fn admission_is_the_exact_tikv_scalar_catalog_receipt() {
        let col = Expression::Column(Column::new(1, FieldType::new(FieldTypeCode::LongLong)));
        let one = Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::LongLong),
        ));

        assert!(can_exprs_push_down_tikv(&[func(
            "eq",
            vec![col.clone(), one.clone()]
        )]));
        // Go admits only the one-argument RoundInt/RoundReal/RoundDec PbCodes.
        assert!(can_exprs_push_down_tikv(&[func(
            "round",
            vec![col.clone()]
        )]));
        assert!(!can_exprs_push_down_tikv(&[func(
            "round",
            vec![col.clone(), one]
        )]));
        // Go rejects a scalar name unless it resolves to a concrete TiKV
        // protobuf signature; conditional functions are no exception.
        assert!(!can_exprs_push_down_tikv(&[func("if", vec![col.clone()])]));
        assert!(!can_exprs_push_down_tikv(&[func("tan", vec![col])]));
    }

    /// Go builds every cast under the single name `ast.Cast`, which
    /// `scalarExprSupportedByTiKV` admits unconditionally
    /// (`pkg/expression/infer_pushdown.go:246`). Rust's dedicated-cast
    /// transcreation names each target type, so `cast_decimal` must answer
    /// like `cast`; otherwise a derived `not(isnull(cast(col)))` filter is
    /// left above the projection that defines the cast instead of inside the
    /// cop reader.
    #[test]
    fn a_dedicated_cast_name_answers_like_go_cast() {
        let col = Expression::Column(Column::new(1, FieldType::new(FieldTypeCode::NewDecimal)));
        let cast = func("cast_decimal", vec![col.clone()]);
        assert!(can_exprs_push_down_tikv(std::slice::from_ref(&cast)));
        assert!(can_exprs_push_down_tikv(&[func(
            "not",
            vec![func("isnull", vec![cast])]
        )]));
        // The mapping is name-prefix only; a name outside the cast family is
        // still decided by the shared policy.
        assert!(!can_exprs_push_down_tikv(&[func("castaway", vec![col])]));
    }
}
