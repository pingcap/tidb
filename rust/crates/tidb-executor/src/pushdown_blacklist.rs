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
