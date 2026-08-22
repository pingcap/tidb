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

//! What `mysql.expr_pushdown_blacklist` refuses: Go `IsPushDownEnabled`, and
//! the walk over one predicate that `PushDownExprs` performs with it.
//!
//! # Why refusing a push changes the PLAN, not just where a filter runs
//!
//! Go filters a `DataSource`'s predicates through `PushDownExprs` BEFORE any
//! access path is derived (`DataSource.PredicatePushDown`:
//! `ds.PushedDownConds, predicates = expression.PushDownExprs(...)`), and the
//! ranger only ever sees `PushedDownConds`. A refused condition therefore
//! does not merely fall back to a root `Selection` -- it never reaches the
//! range builder, so the index whose leading column it constrained stops
//! being a candidate at all. That is why blacklisting `enum` turns
//!
//! ```text
//! IndexLookUp / IndexRangeScan idx(b,a) range:["a","a"]
//! ```
//!
//! into a root `Selection` over a `TableFullScan`, which is what TiDB records
//! for `tests/integrationtest/t/black_list.test`.
//!
//! # Where Go reads it, and whether this tier does
//!
//! Go has eight `IsPushDownEnabled` call sites. Five are answered here:
//!
//!  * `canFuncBePushed` -- a scalar function's own name, asked at both of
//!    Go's stores ([`blacklist_admits`]);
//!  * `columnToPBExpr`'s `enum` (under `kv.UnSpecified`) and `bit` (under
//!    `kv.TiKV`) arms, whose asymmetry is Go's own;
//!  * `DataSource.PredicatePushDown`, the one that decides the PLAN;
//!  * `find_best_task`'s index and table filter split;
//!  * `CheckAggPushDown`'s aggregate-name check ([`aggregate_admits`]).
//!
//! Three are NOT, and each for a stated reason:
//!
//!  * `exhaust_physical_plans.go:1891` (join type) and
//!    `physical_window.go:359` (window function) ask under `kv.TiFlash`.
//!    There is no TiFlash tier here, so there is no plan for them to change.
//!  * `canFuncBePushed`'s SECOND lookup, `name.signature` -- an operator may
//!    disable `lt.ltint` without disabling `lt`. It needs the resolved `tipb`
//!    signature, which this tier does not carry for an arbitrary predicate,
//!    so such a row is loaded and then has no effect. A row naming the plain
//!    function name behaves exactly as Go's.
//!
//! Two more differences worth naming:
//!
//!  * `mysql.opt_rule_blacklist` is honoured for `predicate_push_down` alone
//!    (see `crate::driver::access`'s `pushed_down_conds`). Go's
//!    `isLogicalRuleDisabled` gates all ~30 entries of `optRuleList`; this
//!    tier is not rule-structured, so each further name needs its own
//!    equivalent found first. Any other name is loaded and ignored.
//!  * Go stamps `ExprPushDownBlackListReloadTimeStamp` on every reload and
//!    mixes it into the plan-cache key, so a reload invalidates cached plans.
//!    This tier's plan cache stores no reusable plan (see
//!    `tidb_session::non_prepared_plan_cache`), so there is none to
//!    invalidate; the stamp lands with the plan object.
//!
//! # Scope
//!
//! Only the blacklist's own verdicts live here. `columnToPBExpr` also refuses
//! `SET`, `GEOMETRY` and unspecified-typed columns UNCONDITIONALLY, and
//! `canFuncBePushed` first consults the whole `scalarExprSupportedByTiKV`
//! whitelist; this tier answers both elsewhere and by its own route
//! ([`crate::predicate_pushdown`]'s shape list). Everything here
//! short-circuits on an empty blacklist, so a session that never ran `ADMIN
//! RELOAD` takes exactly the path it took before.

use tidb_expr::infer_pushdown::{is_push_down_enabled, ExprPushDownBlacklist, PushDownStore};
use tidb_expr::rewriter::ColumnResolver;
use tidb_expr::expression::Expression;

/// Go `ast.TypeStr(mysql.TypeBit)`, the name `columnToPBExpr` looks a BIT
/// column up under.
const BIT_TYPE_NAME: &str = "bit";

/// Go `columnToPBExpr`'s `mysql.TypeEnum` arm, whose blacklist key is the
/// bare word rather than a function name.
const ENUM_TYPE_NAME: &str = "enum";

/// Go `expression.PushDownExprs` for ONE condition, restricted to the
/// blacklist's verdicts.
///
/// `store` is the one Go's caller passes: `kv.UnSpecified` at
/// `DataSource.PredicatePushDown`, `kv.TiKV` at `find_best_task`'s index and
/// table filter split.
pub(crate) fn blacklist_admits(
    condition: &tidb_ast::Expr,
    resolver: &impl ColumnResolver,
    ctx: &crate::StmtContext,
    store: PushDownStore,
) -> bool {
    let blacklist = ctx.expr_pushdown_blacklist();
    if blacklist.is_empty() {
        return true;
    }
    let Ok(rewritten) = tidb_expr::rewriter::rewrite_expr_resolved(condition, resolver) else {
        // A condition this tier cannot even resolve is not one the blacklist
        // has an opinion about; whatever refuses it does so on its own.
        return true;
    };
    admits(&rewritten, blacklist, store)
}

/// Go `CheckAggPushDown`'s last line: `ret = IsPushDownEnabled(aggFunc.Name,
/// storeType)`, asked of every function in a pushed aggregate, plus the
/// argument walk `CheckAggCanPushCop` performs beside it.
///
/// The store is `kv.TiKV`: this is the coprocessor stage, which is the only
/// one this tier pushes an aggregate into.
pub(crate) fn aggregate_admits(
    aggregate: &crate::remote_scan::PushdownPartialAggregate,
    ctx: &crate::StmtContext,
) -> bool {
    use crate::remote_scan::{PushdownAggregateKind, PushdownPartialAggregate as Agg};
    let blacklist = ctx.expr_pushdown_blacklist();
    if blacklist.is_empty() {
        return true;
    }
    // Go's `aggFunc.Name`, already lower-case by `newBaseFuncDesc`.
    let name_of = |kind: PushdownAggregateKind| match kind {
        PushdownAggregateKind::Count => "count",
        PushdownAggregateKind::Sum => "sum",
        PushdownAggregateKind::Min => "min",
        PushdownAggregateKind::Max => "max",
    };
    let admits_name =
        |name: &str| is_push_down_enabled(blacklist, name, PushDownStore::TiKv);
    let admits_arg = |input: Option<&Expression>| {
        input.is_none_or(|expr| admits(expr, blacklist, PushDownStore::TiKv))
    };
    match aggregate {
        // `GroupBy` is a one-column `SELECT DISTINCT` with no aggregate
        // function at all, so there is no name to ask about; its group key is
        // a scan column, which the column arm below would answer for.
        Agg::GroupBy { .. } => true,
        Agg::Count { .. } => admits_name("count"),
        Agg::Sum { .. } => admits_name("sum"),
        Agg::GroupBySum { .. } => admits_name("sum"),
        Agg::Grouped { functions, .. } => functions
            .iter()
            .all(|function| admits_name(name_of(function.kind)) && admits_arg(function.input.as_ref())),
        Agg::Global { functions } => functions
            .iter()
            .all(|function| admits_name(name_of(function.kind)) && admits_arg(function.input.as_ref())),
    }
}

/// The walk itself, over the typed tree Go walks.
fn admits(expr: &Expression, blacklist: &ExprPushDownBlacklist, store: PushDownStore) -> bool {
    match expr {
        // Go `columnToPBExpr`, whose two blacklist-driven refusals are by
        // COLUMN TYPE rather than by any function name. `enum` is asked under
        // `kv.UnSpecified` and `bit` under `kv.TiKV`, which is Go's own
        // asymmetry and not a transcription slip.
        Expression::Column(column) => {
            let Some(field_type) = column.ret_type.as_ref() else {
                return true;
            };
            match field_type.code() {
                tidb_datatype::FieldTypeCode::Enum => {
                    is_push_down_enabled(blacklist, ENUM_TYPE_NAME, PushDownStore::Unspecified)
                }
                tidb_datatype::FieldTypeCode::Bit => {
                    is_push_down_enabled(blacklist, BIT_TYPE_NAME, PushDownStore::TiKv)
                }
                _ => true,
            }
        }
        Expression::Constant(_) | Expression::CorrelatedColumn(_) => true,
        // Go `canFuncBePushed`'s two lookups: the function's own name, then
        // `name.signature` so an operator may disable one signature without
        // disabling the function. This tier resolves no `tipb` signature for
        // an arbitrary AST predicate, so only the first lookup is asked --
        // the second could not name anything the first does not.
        Expression::ScalarFunction(function) => {
            if !is_push_down_enabled(blacklist, function.func_name.lowercase(), store) {
                return false;
            }
            function
                .args
                .iter()
                .all(|arg| admits(arg, blacklist, store))
        }
    }
}
