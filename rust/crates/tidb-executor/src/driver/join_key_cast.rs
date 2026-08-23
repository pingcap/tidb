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

//! Go's join-key type-cast chain, read off a join's rewritten conditions.
//!
//! # What Go does, in order
//!
//! For `t_int.id = t_varchar.id`, three Go stages cooperate:
//!
//! 1. `LogicalJoin.updateEQCond` (`logical_join.go:1754`): the equality's
//!    comparison type is DOUBLE, so `NewFunctionInternal` wraps both sides in
//!    `cast(... as double)`; neither side is then a bare column, so each is
//!    materialized into a child Projection via `AppendExpr`, which allocates
//!    one `AllocPlanColumnID` per side. The equality becomes
//!    `eq(proj_col_L, proj_col_R)` over the two new DOUBLE columns.
//! 2. `ruleutil.BuildKeyInfoPortal(p)` runs right after (`logical_join.go:300`)
//!    and recurses the join's whole subtree; `LogicalProjection.BuildKeyInfo`
//!    calls `buildSchemaByExprs` (`logical_projection.go:505`), which
//!    allocates ONE MORE id per non-column expression per projection -- one
//!    per cast, so another two ids for the pair above.
//! 3. `JoinKeyTypeCastRewriter` (`rule_join_key_type_cast.go`), the very next
//!    rule after predicate pushdown: when one cast source is a SIGNED,
//!    non-BIGINT integer column and the other a string column, the DOUBLE
//!    equality is rewritten to an INTEGER one. The int side re-publishes its
//!    bare column under the column's own `UniqueID`; the string side appends
//!    `cast(str AS SIGNED)` under ONE freshly allocated id -- the `Column#N`
//!    the recorded plans print -- and a guard `Selection`
//!    `eq(cast(cast(str, bigint), double), cast(str, double))` lands below
//!    the string-side projection, filtering values ('1.5') whose integer cast
//!    is not their numeric value.
//!
//! The stream arithmetic is pinned by `r/planner/core/join_key_type_cast`:
//! `t_uint join t_varchar` (stage 3 skipped -- unsigned) prints the stage-1
//! columns as `Column#8`/`Column#9`, and every stage-3 statement prints the
//! rewritten string cast as `Column#12` (`Column#13` for `t_mixed_*`, whose
//! extra rowid handles shift the sources) -- all reproduced exactly by
//! "sources, then 2 ids per mismatched equality, then 2 more for the portal,
//! then 1 per rewritten equality".
//!
//! # What this tier does with it
//!
//! This driver plans from the AST and addresses columns by offset, so a
//! logical child Projection cannot be injected without shifting every offset
//! after it. The rewrite is therefore carried as a PLAN-LOCAL computed key:
//! [`analyze`] recognizes the rewritable equalities, and the index-join path
//! ([`crate::join::IndexProbeCast`]) computes `cast(str AS SIGNED)` per outer
//! row -- refusing rows the guard rejects -- instead of reading a
//! materialized column. The hash statements keep their existing residual
//! evaluation (`crate::hash_join::has_cross_side_equality` still marks their
//! keyed order); only the index-join strategy needs the integer key, because
//! only it probes a handle with the value. NAMED RESIDUE: Go's rewrite also
//! turns the hash join's key into the integer pair; this tier's hash path
//! still evaluates the original double equality, which agrees on every value
//! the guard admits.
//!
//! The plan-column id stream, by contrast, is modelled for EVERY mismatched
//! equality whether or not a rewrite is used -- Go allocates on the way to
//! the decision, not on its outcome, and a later `Column#N` in the same
//! statement (an aggregate, another join) is numbered after these.

use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::aggregation::wrap_cast::{wrap_with_cast_as_int, wrap_with_cast_as_real};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::new_function::new_function;
use tidb_expr::Columns;

use crate::executor::ExecError;

/// One cross-side equality Go's `updateEQCond` would materialize through
/// child projections, and -- when [`Self::rewrite`] is `Some` -- rewrite to
/// an integer equality.
pub(crate) struct MismatchedEquality {
    /// Joined-row offset of the integer-side column.
    pub(crate) int_offset: usize,
    /// Joined-row offset of the string-side column.
    pub(crate) str_offset: usize,
    /// The stage-3 rewrite, absent when `classifyCastPair` declines
    /// (unsigned, BIGINT, or a non-string partner).
    pub(crate) rewrite: Option<RewrittenEquality>,
}

/// Go `rule_join_key_type_cast.go`'s product for one eligible equality.
pub(crate) struct RewrittenEquality {
    /// `CAST(str AS SIGNED)` over a ONE-COLUMN row holding the string value.
    pub(crate) cast: Expression,
    /// `eq(cast(cast(str, bigint), double), cast(str, double))` over the same
    /// one-column row.
    pub(crate) guard: Expression,
    /// The string column's own type: the one-column row's layout.
    pub(crate) str_type: FieldType,
}

/// Everything the join-level cast chain says about one join's conditions.
pub(crate) struct JoinKeyCoercions {
    /// In conjunct order.
    pub(crate) mismatched: Vec<MismatchedEquality>,
}

impl JoinKeyCoercions {
    /// How many equalities get the stage-1 double-cast projection pair.
    pub(crate) fn double_cast_pairs(&self) -> usize {
        self.mismatched.len()
    }

    /// Indices (within `mismatched`) of the stage-3 rewritten equalities, in
    /// the order Go's rule allocates their string-cast columns.
    pub(crate) fn rewritten(&self) -> Vec<usize> {
        self.mismatched
            .iter()
            .enumerate()
            .filter_map(|(at, eq)| eq.rewrite.as_ref().map(|_| at))
            .collect()
    }
}

/// The column beneath one side of the equality, seen through at most one
/// REAL-valued cast -- Go's `findCastInProj` accepts exactly a
/// `cast`-to-`ETReal` of a bare column, which is both the implicit
/// `updateEQCond` wrap and a user-written `cast(col as double)`.
fn cast_source_column(expr: &Expression) -> Option<&Column> {
    match expr {
        Expression::Column(column) => Some(column),
        Expression::ScalarFunction(f)
            if f.func_name.lowercase().starts_with("cast")
                && f.args.len() == 1
                && f.ret_type
                    .as_ref()
                    .is_some_and(|tp| tp.eval_type() == EvalType::Real) =>
        {
            match &f.args[0] {
                Expression::Column(column) => Some(column),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Go `classifyCastPair`'s `isSignedInt`: `ETInt`, not UNSIGNED, and not
/// BIGINT -- rewriting `VARCHAR = BIGINT` would change the observable result
/// once values cross DOUBLE's exact-integer boundary.
fn is_rewritable_signed_int(tp: &FieldType) -> bool {
    tp.eval_type() == EvalType::Int
        && !tp.has_flag(FieldTypeFlags::UNSIGNED)
        && tp.code() != FieldTypeCode::LongLong
}

/// Recognizes the mismatched cross-side equalities of one join's conditions.
///
/// `left_width` splits the joined row exactly as
/// [`crate::hash_join::split_equi`] splits it. Only `eq` counts -- Go's rule
/// skips `<=>` because the guard's `=` would filter the NULLs that `<=>`
/// must match.
pub(crate) fn analyze(
    conditions: &[Expression],
    left_width: usize,
    ctx: &impl Columns,
) -> JoinKeyCoercions {
    let mut mismatched = Vec::new();
    for conjunct in conditions
        .iter()
        .flat_map(crate::hash_join::split_conjuncts)
    {
        let Expression::ScalarFunction(f) = conjunct else {
            continue;
        };
        if f.func_name.lowercase() != "eq" || f.args.len() != 2 {
            continue;
        }
        let (Some(a), Some(b)) = (
            cast_source_column(&f.args[0]),
            cast_source_column(&f.args[1]),
        ) else {
            continue;
        };
        let (Ok(a_index), Ok(b_index)) = (usize::try_from(a.index), usize::try_from(b.index))
        else {
            continue;
        };
        // One column from each side, like `equi_key`'s split.
        if (a_index < left_width) == (b_index < left_width) {
            continue;
        }
        let (Some(a_type), Some(b_type)) = (a.ret_type.as_ref(), b.ret_type.as_ref()) else {
            continue;
        };
        // The pair Go's `NewFunctionInternal` casts to DOUBLE on both sides:
        // an integer-family column against a string-family one. Same-type
        // pairs (`name = name`) stay bare columns and allocate nothing;
        // other coerced pairs (decimal/time against string) are NAMED
        // RESIDUE -- nothing in the pinned corpus reaches them through this
        // chain.
        let (int_at, str_at) = match (a_type.eval_type(), b_type.eval_type()) {
            (EvalType::Int, EvalType::String) => ((a_index, a_type), (b_index, b_type)),
            (EvalType::String, EvalType::Int) => ((b_index, b_type), (a_index, a_type)),
            _ => continue,
        };
        let ((int_offset, int_type), (str_offset, str_type)) = (int_at, str_at);
        let rewrite = is_rewritable_signed_int(int_type)
            .then(|| build_rewrite(int_type, str_type, ctx))
            .flatten();
        mismatched.push(MismatchedEquality {
            int_offset,
            str_offset,
            rewrite,
        });
    }
    JoinKeyCoercions { mismatched }
}

/// Builds Go's `castIntExpr` and guard over a one-column row `[str]`.
pub(crate) fn build_rewrite(
    int_type: &FieldType,
    str_type: &FieldType,
    ctx: &impl Columns,
) -> Option<RewrittenEquality> {
    let str_column = || {
        let mut column = Column::new(0, str_type.clone());
        column.index = 0;
        Expression::Column(column)
    };
    // Go: `expression.WrapWithCastAsInt(exprCtx, strCol, intCol.RetType)`.
    let cast = wrap_with_cast_as_int(str_column(), Some(int_type)).ok()?;
    // Go: `eq(WrapWithCastAsReal(castIntExpr), WrapWithCastAsReal(strCol))`.
    let guard_left = wrap_with_cast_as_real(cast.clone()).ok()?;
    let guard_right = wrap_with_cast_as_real(str_column()).ok()?;
    let guard = new_function(
        ctx,
        "eq",
        FieldType::new(FieldTypeCode::Tiny),
        vec![guard_left, guard_right],
    )
    .ok()?;
    Some(RewrittenEquality {
        cast,
        guard,
        str_type: str_type.clone(),
    })
}

/// Evaluates one outer row's computed probe key: `None` when the guard
/// rejects the value (or either evaluation yields NULL), `Some(v)` with
/// `v = CAST(str AS SIGNED)` otherwise.
///
/// The guard is Go's `Selection` below the string-side projection, folded
/// into key computation: an inner join emits nothing for an outer row with
/// no key, which is exactly what the dropped row produced.
pub(crate) fn computed_probe_key(
    cast: &Expression,
    guard: &Expression,
    str_type: &FieldType,
    value: &Datum,
    ctx: &impl Columns,
) -> Result<Option<Datum>, ExecError> {
    if matches!(value, Datum::Null) {
        return Ok(None);
    }
    let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(std::slice::from_ref(str_type), 1);
    chunk.append_datum(0, value);
    let row = chunk.get_row(0);
    let admitted = guard.eval(ctx, row).map_err(ExecError::Eval)?;
    if tidb_expr::truthy_of(&admitted)? != Some(true) {
        return Ok(None);
    }
    let key = cast.eval(ctx, row).map_err(ExecError::Eval)?;
    Ok(match key {
        Datum::Null => None,
        other => Some(other),
    })
}
