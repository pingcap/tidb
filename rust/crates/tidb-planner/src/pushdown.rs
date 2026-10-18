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
    let (pushed, rejected) = push_down_exprs(ordinary, store, blacklist);
    root.extend(rejected);
    (pushed, root)
}

/// Go `expression.PushDownExprs`: the filters `store` can evaluate, then the
/// ones it cannot. Unlike [`split_scan_filters`] it keeps a virtual-column
/// filter: an index that covers the hidden column evaluates it.
pub fn push_down_exprs(
    filters: Vec<Expression>,
    store: tidb_expr::infer_pushdown::PushDownStore,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
) -> (Vec<Expression>, Vec<Expression>) {
    filters.into_iter().partition(|condition| {
        can_exprs_push_down(std::slice::from_ref(condition), store, blacklist)
    })
}

/// Go `extractFiltersForIndexMerge`: a filter TiKV can evaluate is pushed,
/// one only another engine can evaluate stays for the root task, and one no
/// engine can evaluate is dropped -- predicate push-down's `kv.UnSpecified`
/// split left it in the Selection above the DataSource, which evaluates it.
pub fn extract_filters_for_index_merge(
    filters: Vec<Expression>,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
) -> (Vec<Expression>, Vec<Expression>) {
    use tidb_expr::infer_pushdown::PushDownStore;

    let mut pushed = Vec::new();
    let mut remaining = Vec::new();
    for expr in filters {
        if can_exprs_push_down(std::slice::from_ref(&expr), PushDownStore::TiKv, blacklist) {
            pushed.push(expr);
        } else if can_exprs_push_down(
            std::slice::from_ref(&expr),
            PushDownStore::Unspecified,
            blacklist,
        ) {
            remaining.push(expr);
        }
    }
    (pushed, remaining)
}

/// Go `BuildIndexMergeTableScan`'s filter split: `extractFiltersForIndexMerge`,
/// then `SplitSelCondsWithVirtualColumn` over the pushed filters, whose
/// virtual-column conditions join the root ones after the engine residue.
pub fn split_index_merge_table_filters(
    filters: Vec<Expression>,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
) -> (Vec<Expression>, Vec<Expression>) {
    let (pushed, mut remaining) = extract_filters_for_index_merge(filters, blacklist);
    let (pushed, virtual_remaining) = split_virtual_column_filters(pushed);
    remaining.extend(virtual_remaining);
    (pushed, remaining)
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

/// [`can_exprs_push_down`] that also reports, as Go's `canScalarFuncPushDown`
/// does through its `PushDownContext`, the outermost scalar function that
/// cannot be pushed: `report` receives Go's warning text. Go checks a
/// function before its arguments and stops at the first refusal.
pub fn can_exprs_push_down_reporting(
    exprs: &[Expression],
    store: tidb_expr::infer_pushdown::PushDownStore,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
    report: &dyn Fn(String),
) -> bool {
    exprs
        .iter()
        .all(|expr| can_expr_push_down_reported(expr, store, blacklist, Some(report)))
}

fn can_expr_push_down(
    expr: &Expression,
    store: tidb_expr::infer_pushdown::PushDownStore,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
) -> bool {
    can_expr_push_down_reported(expr, store, blacklist, None)
}

fn can_expr_push_down_reported(
    expr: &Expression,
    store: tidb_expr::infer_pushdown::PushDownStore,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
    report: Option<&dyn Fn(String)>,
) -> bool {
    use tidb_expr::infer_pushdown::{is_push_down_enabled, PushDownStore};

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
    // Go `canScalarFuncPushDown`: the function itself first, with the
    // refusal warning, then each argument.
    let signature = match tidb_expr::pushdown_catalog::from_expression(expr) {
        Some(tidb_expr::pushdown_catalog::PbScalar::Call { signature, .. }) => signature.sig,
        Some(tidb_expr::pushdown_catalog::PbScalar::Cast { signature, .. }) => signature,
        _ => tidb_expr::pushdown_catalog::ScalarFuncSig::Unspecified,
    };
    if !scalar_function_can_be_pushed(expr, function, signature, store, blacklist) {
        if let Some(report) = report {
            let storage = match store {
                PushDownStore::TiKv => "tikv",
                PushDownStore::TiFlash => "tiflash",
                PushDownStore::TiDb => "tidb",
                _ => "storage layer",
            };
            report(format!(
                "Scalar function '{}'(signature: {}, return type: {}) is not supported to push down to {storage} now.",
                function.func_name.lowercase(),
                signature.as_str_name(),
                expr.static_type().map_or_else(String::new, |field_type| {
                    field_type.compact_str(tidb_datatype::STRICT_INTEGER_DISPLAY_WIDTH)
                }),
            ));
        }
        return false;
    }
    function
        .args
        .iter()
        .all(|arg| can_expr_push_down_reported(arg, store, blacklist, report))
}

/// Go `canFuncBePushed` and the PbCode check of `canScalarFuncPushDown`, for
/// the function alone.
fn scalar_function_can_be_pushed(
    expr: &Expression,
    function: &tidb_expr::scalar_function::ScalarFunction,
    signature: tidb_expr::pushdown_catalog::ScalarFuncSig,
    store: tidb_expr::infer_pushdown::PushDownStore,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
) -> bool {
    use tidb_expr::infer_pushdown::{can_function_be_pushed, PushDownPolicy, PushDownStore};
    use tidb_expr::pushdown_catalog::ScalarFuncSig;

    let name = function.func_name.lowercase();
    // Dedicated Rust casts represent Go's single ast.Cast function family.
    let policy_name = if name.starts_with("cast_") {
        "cast"
    } else {
        name.as_ref()
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
                // Go's `builtinPeriodAddSig`/`builtinPeriodDiffSig` never call
                // `setPbCode`, so `canScalarFuncPushDown` refuses their
                // unspecified signature although the name is listed.
                | "period_add"
                | "period_diff"
        )
    {
        return false;
    }
    // A cast between eval types with no `CastXAsY` signature (to or from a
    // vector, say) has no pb code in Go and refuses itself and its parent.
    if store == PushDownStore::TiKv && policy_name == "cast" {
        if let (Some(from), Some(to)) = (
            function.args.first().and_then(Expression::static_type),
            expr.static_type(),
        ) {
            if !tidb_expr::pushdown_catalog::cast_pair_has_signature(
                from.eval_type(),
                to.eval_type(),
            ) {
                return false;
            }
        }
    }
    let mut policy = PushDownPolicy::new(policy_name, signature);
    if policy_name == "conv" {
        policy.conv_casts_hybrid_or_binary_literal = function
            .args
            .first()
            .is_some_and(tidb_expr::scalar_function::conv_reads_bits);
    }
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

    /// Go `canScalarFuncPushDown` checks a function before its arguments:
    /// a refused outer function is the one reported, and the walk stops
    /// there. A pushable outer function reports its refused argument.
    #[test]
    fn a_refused_outer_function_is_reported_before_its_arguments() {
        use tidb_expr::infer_pushdown::PushDownStore;
        let column = Expression::Column(Column::new(1, FieldType::new(FieldTypeCode::LongLong)));
        let reported = std::cell::RefCell::new(Vec::new());
        let report = |message: String| reported.borrow_mut().push(message);
        let inner = func("json_overlaps", vec![column.clone(), column.clone()]);
        let outer = func("json_overlaps", vec![inner.clone(), column.clone()]);
        assert!(!can_exprs_push_down_reporting(
            &[outer],
            PushDownStore::Unspecified,
            &Default::default(),
            &report,
        ));
        assert_eq!(reported.borrow().len(), 1);
        reported.borrow_mut().clear();
        let pushable = func("isnull", vec![inner]);
        assert!(!can_exprs_push_down_reporting(
            &[pushable],
            PushDownStore::Unspecified,
            &Default::default(),
            &report,
        ));
        let messages = reported.borrow();
        assert_eq!(messages.len(), 1);
        assert!(
            messages[0].starts_with("Scalar function 'json_overlaps'(signature: Unspecified"),
            "{messages:?}"
        );
        assert!(messages[0].ends_with("is not supported to push down to storage layer now."));
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
        let cast = Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("cast_decimal"),
            FieldType::new(FieldTypeCode::NewDecimal),
            vec![col.clone()],
        ));
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
