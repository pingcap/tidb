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

//! Shared logical and store-specific expression push-down admission.
//!
//! Go `canExprPushDown` first proves that an expression has a protobuf
//! representation, and `canScalarFuncPushDown` then applies the source-owned
//! store policy recursively. Rust keeps that policy in
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

/// Go `CanExprsPushDown(ctx, exprs, kv.TiFlash)` over the expression families
/// represented by the shared TiPB scalar catalog.
#[must_use]
pub fn can_exprs_push_down_tiflash(exprs: &[Expression]) -> bool {
    exprs.iter().all(can_expr_push_down_tiflash)
}

/// Shared admission for the logical datasource and store-specific consumers.
/// Go uses Unspecified before deriving any ordinary or index-merge ranges.
#[must_use]
pub fn can_exprs_push_down(
    exprs: &[Expression],
    store: tidb_expr::infer_pushdown::PushDownStore,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
) -> bool {
    exprs
        .iter()
        .all(|expr| can_expr_push_down(expr, store, blacklist))
}

/// Go SplitSelCondsWithVirtualColumn followed by PushDownExprs for scan filters.
/// The second list remains on the root task until reader conversion.
pub fn split_scan_filters(
    filters: Vec<Expression>,
    store: tidb_expr::infer_pushdown::PushDownStore,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
) -> (Vec<Expression>, Vec<Expression>) {
    let (ordinary, mut root) = split_virtual_column_filters(filters);
    let (pushed, rejected): (Vec<_>, Vec<_>) = ordinary.into_iter().partition(|condition| {
        can_exprs_push_down(std::slice::from_ref(condition), store, blacklist)
    });
    root.extend(rejected);
    (pushed, root)
}

/// Go SplitSelCondsWithVirtualColumn, kept separate for index/table phase order.
pub fn split_virtual_column_filters(
    filters: Vec<Expression>,
) -> (Vec<Expression>, Vec<Expression>) {
    filters.into_iter().partition(|condition| {
        !tidb_expr::simple_expr::extract_columns(condition)
            .iter()
            .any(|column| column.virtual_expr.is_some())
    })
}

fn can_expr_push_down_tikv(expr: &Expression) -> bool {
    can_expr_push_down(
        expr,
        tidb_expr::infer_pushdown::PushDownStore::TiKv,
        &Default::default(),
    )
}

fn can_expr_push_down_tiflash(expr: &Expression) -> bool {
    can_expr_push_down(
        expr,
        tidb_expr::infer_pushdown::PushDownStore::TiFlash,
        &Default::default(),
    )
}

fn can_expr_push_down(
    expr: &Expression,
    store: tidb_expr::infer_pushdown::PushDownStore,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
) -> bool {
    use tidb_expr::infer_pushdown::{
        can_function_be_pushed, is_push_down_enabled, PushDownPolicy, PushDownStore,
    };
    use tidb_expr::pushdown_catalog::{PbScalar, ScalarFuncSig};

    let Expression::ScalarFunction(function) = expr else {
        // Go PbConverter.columnToPBExpr checks column types independently of
        // the requested engine, including correlated columns.
        if matches!(
            expr,
            Expression::Column(_) | Expression::CorrelatedColumn(_)
        ) {
            if let Some(field_type) = expr.static_type() {
                use tidb_datatype::FieldTypeCode;
                return match field_type.code() {
                    FieldTypeCode::Enum => {
                        is_push_down_enabled(blacklist, "enum", PushDownStore::Unspecified)
                    }
                    FieldTypeCode::Bit => {
                        is_push_down_enabled(blacklist, "bit", PushDownStore::TiKv)
                    }
                    FieldTypeCode::Set | FieldTypeCode::Geometry | FieldTypeCode::Unspecified => {
                        false
                    }
                    _ => true,
                };
            }
        }
        return true;
    };
    if !function
        .args
        .iter()
        .all(|arg| can_expr_push_down(arg, store, blacklist))
    {
        return false;
    }
    let name = function.func_name.lowercase();
    // Dedicated Rust casts represent Go's single ast.Cast function family.
    let policy_name = if name.starts_with("cast_") {
        "cast"
    } else {
        name.as_ref()
    };
    let signature = match tidb_expr::pushdown_catalog::from_expression(expr) {
        Some(PbScalar::Call { signature, .. }) => signature.sig,
        _ => ScalarFuncSig::Unspecified,
    };
    if store == PushDownStore::TiKv
        && signature == ScalarFuncSig::Unspecified
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
    let mut policy = PushDownPolicy::new(policy_name, signature);
    if policy_name == "cast" {
        policy.source_type = function.args.first().and_then(Expression::static_type);
        policy.return_type = expr.static_type();
    }
    if matches!(
        policy_name,
        "regexp" | "regexp_like" | "regexp_substr" | "regexp_instr" | "regexp_replace"
    ) {
        (policy.charset, policy.collation) = function.collation.charset_and_collation();
    }
    can_function_be_pushed(&policy, store, blacklist)
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
    fn datasource_admission_preserves_unspecified_store_blacklist_semantics() {
        use tidb_expr::infer_pushdown::{blacklist_store_mask, PushDownStore};
        let column = Expression::Column(Column::new(1, FieldType::new(FieldTypeCode::LongLong)));
        let one = Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::LongLong),
        ));
        let predicate = func("gt", vec![column, one]);
        let mut blacklist = Default::default();
        for name in ["gt", "gt.gtint"] {
            blacklist =
                std::collections::HashMap::from([(name.into(), blacklist_store_mask("tikv"))]);
            assert!(can_exprs_push_down(
                std::slice::from_ref(&predicate),
                PushDownStore::Unspecified,
                &blacklist
            ));
            assert!(!can_exprs_push_down(
                std::slice::from_ref(&predicate),
                PushDownStore::TiKv,
                &blacklist
            ));
            blacklist.insert(name.into(), blacklist_store_mask("tikv,tiflash,tidb"));
            assert!(!can_exprs_push_down(
                std::slice::from_ref(&predicate),
                PushDownStore::Unspecified,
                &blacklist
            ));
        }
        let enum_column = Expression::Column(Column::new(2, FieldType::new(FieldTypeCode::Enum)));
        blacklist.clear();
        blacklist.insert("enum".into(), blacklist_store_mask("tikv"));
        assert!(can_exprs_push_down(
            std::slice::from_ref(&enum_column),
            PushDownStore::TiKv,
            &blacklist
        ));
        blacklist.insert("enum".into(), blacklist_store_mask("tikv,tiflash,tidb"));
        assert!(!can_exprs_push_down(
            &[enum_column],
            PushDownStore::Unspecified,
            &blacklist
        ));
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

    #[test]
    fn tiflash_admission_uses_the_shared_signature_policy() {
        let col = Expression::Column(Column::new(1, FieldType::new(FieldTypeCode::LongLong)));
        let one = Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::LongLong),
        ));
        assert!(can_exprs_push_down_tiflash(&[func(
            "eq",
            vec![col.clone(), one.clone()],
        )]));
        assert!(can_exprs_push_down_tiflash(&[func(
            "if",
            vec![col.clone(), one.clone(), one.clone()],
        )]));
        // TiFlash does not admit TiKV-only signatures such as RAND, and an
        // unresolved function is rejected without a catalog signature.
        assert!(!can_exprs_push_down_tiflash(&[func(
            "rand",
            vec![col.clone()]
        )]));
        assert!(!can_exprs_push_down_tiflash(&[func("tan", vec![col])]));
    }
}
