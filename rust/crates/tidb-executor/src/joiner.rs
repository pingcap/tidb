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

//! `pkg/executor/join/joiner.go`: the per-join-type row-matching strategies.
//!
//! This is the file that encodes WHAT a join type emits, separately from HOW
//! the rows are paired up. Every pairing engine in `pkg/executor/join` -- the
//! v1 hash join, the merge join, the three index-lookup joins -- hands each
//! (outer row, candidate inner rows) pair to a [`Joiner`] and lets the joiner
//! decide what reaches the output chunk. The contract is Go's, quoted at the
//! trait:
//!
//! ```text
//! hasMatch, hasNull := false, false
//! for innerIter.Current() != innerIter.End() {
//!     matched, isNull, err := j.TryToMatchInners(outer, innerIter, chk)
//!     hasMatch = hasMatch || matched
//!     hasNull  = hasNull  || isNull
//! }
//! if !hasMatch {
//!     j.OnMissMatch(hasNull, outer, chk)
//! }
//! ```
//!
//! The `hasNull` channel is the whole reason this is not a boolean: for
//! `AntiSemiJoin`, `LeftOuterSemiJoin` and `AntiLeftOuterSemiJoin` a miss
//! because the condition was FALSE and a miss because it was NULL produce
//! different output rows, and only the joiner that evaluated the condition
//! knows which happened.
//!
//! SEED SCOPE. `pkg/executor/join` is a large package; this file is
//! `joiner.go` and nothing else. LANDED here: the [`Joiner`] trait, its nine
//! implementations (`semiJoiner`, `antiSemiJoiner`, `nullAwareAntiSemiJoiner`,
//! `leftOuterSemiJoiner`, `antiLeftOuterSemiJoiner`,
//! `nullAwareAntiLeftOuterSemiJoiner`, `leftOuterJoiner`, `rightOuterJoiner`,
//! `innerJoiner`), the shared `baseJoiner` state and its
//! `makeJoinRowToChunk`/`makeShallowJoinRow`/`filter`/
//! `filterAndCheckOuterRowStatus`/`Clone` helpers, `NewJoiner`, `JoinerType`,
//! `outerRowStatusFlag` and `NAAJType`. NOT landed by this file (each is its
//! own Go file and its own unit): `base_join_probe.go`, `hash_join_v1.go`,
//! `hash_join_v2.go`, `hash_join_base.go`, `hash_join_spill*.go`,
//! `merge_join.go`, `index_lookup_*join.go`, the per-type `*_join_probe.go`
//! v2 probes, `hash_table_v1.go`, `hash_join_stats.go`, `concurrent_map.go`.
//! `hash_table_v2.go`, `join_row_table.go`, `row_table_builder.go`,
//! `join_table_meta.go` and `tagged_ptr.go` already live in `tidb-exec`.
//!
//! REUSE. The pairing engine this crate already ships is
//! [`crate::join::JoinExec`], which inlines a fused subset of these semantics
//! ([`crate::join::JoinKind`], `emit_outer_row`) for the five join kinds its
//! driver can build. That executor is untouched: this module is the faithful
//! `joiner.go` surface, and it is the one a v2 probe or a full `hash_join_v1`
//! port would be written against. The two are deliberately not merged yet --
//! folding `JoinExec` onto `Joiner` changes the wired engine's behaviour and
//! is a separate, testable step.
//!
//! NARROWINGS, each named:
//!
//! - `sessionctx.Context` is replaced by a [`Columns`] evaluation context,
//!   which is the same substitution [`crate::join::JoinExec`] makes. The
//!   session-variable reads Go does inside the joiner
//!   (`MaxChunkSize`, `InitChunkSize`, `EnableVectorizedExpression`) become
//!   explicit constructor arguments, because no `SessionVars` is reachable
//!   from `Columns`.
//! - Go's `makeShallowJoinRow` builds a `chunk.MutRow` that SHALLOW-copies the
//!   two source rows. [`BaseJoiner::make_shallow_join_row`] appends them into
//!   a reused scratch [`Chunk`] instead, which copies the cells. Same row,
//!   same evaluation result; strictly a per-row allocation cost, because
//!   `MutRow::shallow_copy_partial_row` needs `&mut Chunk` on the SOURCE and
//!   an inner row here is borrowed from an iterator.
//! - `expression.VectorizedFilter` / `VectorizedFilterConsiderNull` are
//!   replaced by [`row_based_filter`], a port of Go's own
//!   `rowBasedFilter` (`pkg/expression/chunk_executor.go:465`) -- the branch
//!   Go itself takes when the filter is not vectorizable. It is filter-major
//!   and skips already-deselected rows, exactly as Go's is, so the
//!   `isNull` accumulation matches. Go's vectorized fast path is not ported;
//!   it is a performance path with the same result. Within `rowBasedFilter`,
//!   Go has two branches, `EvalInt` for an `ETInt` filter and `EvalBool` for
//!   everything else, and they do NOT agree on `isNull` (the `EvalBool`
//!   branch reports NULL only for an EQ-from-IN condition, and otherwise
//!   leaves the previous row's flag in place). This port implements the
//!   `EvalInt` branch's rule uniformly -- a NULL result sets `is_null` and
//!   deselects -- because a join condition is `ETInt` and so takes that
//!   branch in Go.
//! - `Joiner::join_type` replaces Go's `JoinerType` type switch: Rust has no
//!   equivalent of a type switch over trait objects, so each implementation
//!   reports its own type. Go's function maps every non-listed joiner to
//!   `InnerJoin`; here the two null-aware joiners report their own
//!   (`AntiSemiJoin` / `AntiLeftOuterSemiJoin`) rather than Go's `InnerJoin`
//!   fallthrough, which is a Go artifact of the switch listing only the
//!   non-null-aware types.
//! - Go's `TryToMatchInners(..., opt ...NAAJType)` variadic becomes a single
//!   [`NAAJType`] argument. `NAAJType::Unknown` is Go's zero value; Go would
//!   panic on `opt[0]` with no argument, whereas
//!   `nullAwareAntiLeftOuterSemiJoiner::on_match` here emits nothing for
//!   `Unknown` (its `switch` has no default arm).
//! - `logutil.BgLogger().Debug("InlineProjection", ...)` in `NewJoiner` is
//!   dropped: there is no logger on this path.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_util::{
    copy_selected_join_rows_direct, copy_selected_join_rows_with_same_outer_rows,
};
use tidb_chunk::iterator::LendingIterator;
use tidb_chunk::mutrow::MutRow;
use tidb_chunk::row::Row;
use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;
use tidb_expr::Columns;

use crate::executor::ExecError;

/// Go `plannerbase.JoinType` (`pkg/planner/core/base`), restricted to the
/// values `NewJoiner` accepts.
///
/// It is declared here rather than imported because the planner's own type is
/// not part of this crate's dependency set; the two are kept in sync by name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType {
    /// Go `InnerJoin`.
    Inner,
    /// Go `LeftOuterJoin`.
    LeftOuter,
    /// Go `RightOuterJoin`.
    RightOuter,
    /// Go `SemiJoin`: emit the outer row once when some inner row matches.
    SemiJoin,
    /// Go `AntiSemiJoin`: emit the outer row when NO inner row matches.
    AntiSemiJoin,
    /// Go `LeftOuterSemiJoin`: emit the outer row plus a 0/1/NULL flag.
    LeftOuterSemiJoin,
    /// Go `AntiLeftOuterSemiJoin`: the same with the flag negated.
    AntiLeftOuterSemiJoin,
}

/// Go `outerRowStatusFlag`: per-outer-row verdict of `TryToMatchOuters`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OuterRowStatusFlag {
    /// Go `outerRowUnmatched`: the condition was FALSE.
    Unmatched,
    /// Go `outerRowMatched`.
    Matched,
    /// Go `outerRowHasNull`: the condition was NULL, which is not the same as
    /// unmatched for the anti/left-outer-semi types.
    HasNull,
}

/// Go `NAAJType`: join detail only used by the null-aware
/// `AntiLeftOuterSemiJoin`, describing the nullness of the two NA-EQ keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum NAAJType {
    /// Go `Unknown`: the default value.
    #[default]
    Unknown,
    /// Go `LeftHasNullRightNotNull`.
    LeftHasNullRightNotNull,
    /// Go `LeftHasNullRightHasNull`.
    LeftHasNullRightHasNull,
    /// Go `LeftNotNullRightNotNull`.
    LeftNotNullRightNotNull,
    /// Go `LeftNotNullRightHasNull`.
    LeftNotNullRightHasNull,
}

/// Go `expression.IsEQCondFromIn` (`pkg/expression/expression.go:325`): an EQ
/// scalar function at least one of whose columns carries `InOperand`, i.e. one
/// rewritten from an `IN` subquery.
///
/// [`eval_bool`] treats a NULL from such a condition as "unknown" rather than
/// as an immediate FALSE, which is what lets an anti-semi join distinguish
/// "no matching row" from "cannot tell".
fn is_eq_cond_from_in(expr: &Expression) -> bool {
    let Expression::ScalarFunction(sf) = expr else {
        return false;
    };
    if sf.func_name.lowercase() != "eq" {
        return false;
    }
    fn any_in_operand(expr: &Expression) -> bool {
        match expr {
            Expression::Column(column) => column.in_operand,
            Expression::ScalarFunction(sf) => sf.get_args().iter().any(any_in_operand),
            Expression::Constant(_) | Expression::CorrelatedColumn(_) => false,
        }
    }
    sf.get_args().iter().any(any_in_operand)
}

/// Go `expression.EvalBool` (`pkg/expression/expression.go:348`): evaluate a
/// CNF condition list over one row, returning `(matched, has_null)`.
///
/// A NULL short-circuits to `(false, false)` UNLESS the condition is an
/// EQ rewritten from `IN`, in which case it records `has_null` and keeps
/// going -- Go's comment explains why: a later condition may still prove the
/// whole list false, and `false` is a stronger answer than `unknown`.
///
/// # Errors
/// Propagates an expression evaluation failure.
pub fn eval_bool<C: Columns>(
    ctx: &C,
    exprs: &[Expression],
    row: Row<'_>,
) -> Result<(bool, bool), ExecError> {
    let mut has_null = false;
    for expr in exprs {
        let data = expr.eval(ctx, row)?;
        if matches!(data, Datum::Null) {
            if !is_eq_cond_from_in(expr) {
                return Ok((false, false));
            }
            has_null = true;
            continue;
        }
        if tidb_expr::truthy_of(&data)? != Some(true) {
            return Ok((false, false));
        }
    }
    if has_null {
        return Ok((false, true));
    }
    Ok((true, false))
}

/// Go `expression.rowBasedFilter` (`pkg/expression/chunk_executor.go:465`):
/// fill `selected` (and, when asked, `is_null`) for every row of `input`.
///
/// Filter-major, and a row already deselected by an earlier filter is skipped
/// -- both are load-bearing, because they decide whether a row that was NULL
/// under one filter and FALSE under a later one reports `is_null`.
///
/// See the module header for the one deliberate difference from Go: the
/// `EvalInt` branch's NULL rule is applied to every filter.
fn row_based_filter<C: Columns>(
    ctx: &C,
    conditions: &[Expression],
    input: &Chunk,
    selected: &mut Vec<bool>,
    mut is_null: Option<&mut Vec<bool>>,
) -> Result<(), ExecError> {
    let num_rows = input.num_rows();
    selected.clear();
    selected.resize(num_rows, true);
    if let Some(nulls) = is_null.as_deref_mut() {
        nulls.clear();
        nulls.resize(num_rows, false);
    }
    for filter in conditions {
        for index in 0..num_rows {
            if !selected[index] {
                continue;
            }
            let value = filter.eval(ctx, input.get_row(index))?;
            let null_result = matches!(value, Datum::Null);
            let truth = !null_result && tidb_expr::truthy_of(&value)? == Some(true);
            selected[index] = selected[index] && truth;
            if let Some(nulls) = is_null.as_deref_mut() {
                nulls[index] = nulls[index] || null_result;
            }
        }
    }
    Ok(())
}

/// Go `baseJoiner.makeJoinRowToChunk`: append `lhs` then `rhs` into `chk`.
///
/// Go's comment (and issue 5771) explains the order: the whole-row append must
/// come first so the chunk's virtual row count is incremented exactly once.
fn make_join_row_to_chunk(
    chk: &mut Chunk,
    lhs: Row<'_>,
    rhs: Row<'_>,
    l_used: Option<&[usize]>,
    r_used: Option<&[usize]>,
) {
    let l_wide = chk.append_row_by_col_idxs(lhs, l_used);
    chk.append_partial_row_by_col_idxs(l_wide, rhs, r_used);
}

/// Go `baseJoiner`: the state every join type shares.
pub struct BaseJoiner<C: Columns> {
    ctx: C,
    /// Go `conditions`: the non-equality `ON` predicates ("other conditions").
    conditions: Vec<Expression>,
    /// Go `defaultInner`: the all-NULL (or default) inner row an outer join
    /// pads with. Only built for `LeftOuterJoin`/`RightOuterJoin`.
    default_inner: Option<MutRow>,
    /// Go `outerIsRight`: whether the OUTER side is the join's right child.
    outer_is_right: bool,
    /// Go `chk`: the scratch chunk the filtering join types build their
    /// candidate rows in before `filter` copies the survivors out. Present
    /// only for inner/outer joins that HAVE conditions.
    chk: Option<Chunk>,
    /// Go `shallowRow`: the one-row scratch a semi-family joiner evaluates
    /// its conditions against.
    shallow_row: Option<Chunk>,
    /// Go `selected` / `isNull`: reused filter output buffers.
    selected: Vec<bool>,
    is_null: Vec<bool>,
    /// Go `maxChunkSize` (`SessionVars.MaxChunkSize`).
    max_chunk_size: usize,
    /// Go `lUsed`/`rUsed`: the inline projection. `None` is Go's nil (every
    /// column is used); `Some(&[])` is Go's non-nil empty slice (no column
    /// is), and the distinction is load-bearing in `append_row_by_col_idxs`.
    l_used: Option<Vec<usize>>,
    r_used: Option<Vec<usize>>,
}

impl<C: Columns + Clone> BaseJoiner<C> {
    /// Go `baseJoiner.initDefaultInner`.
    fn init_default_inner(&mut self, inner_types: &[FieldType], default_inner: &[Datum]) {
        let mut mutable_row = MutRow::from_types(inner_types);
        mutable_row.set_datums(&default_inner[..inner_types.len()]);
        self.default_inner = Some(mutable_row);
    }

    /// Go `baseJoiner.makeShallowJoinRow`: place `inner` and `outer` into the
    /// scratch row in the join's own left-then-right order.
    ///
    /// It deliberately ignores `l_used`/`r_used`: a column the output prunes
    /// may still be read by a condition.
    fn make_shallow_join_row(&mut self, is_right_join: bool, inner: Row<'_>, outer: Row<'_>) {
        let (first, second) = if is_right_join {
            (inner, outer)
        } else {
            (outer, inner)
        };
        let scratch = self
            .shallow_row
            .as_mut()
            .expect("a semi-family joiner always builds its shallow row");
        scratch.reset();
        scratch.append_partial_row(0, first);
        scratch.append_partial_row(first.len(), second);
    }

    /// The scratch row built by the last [`Self::make_shallow_join_row`].
    fn shallow_row(&self) -> Row<'_> {
        self.shallow_row
            .as_ref()
            .expect("a semi-family joiner always builds its shallow row")
            .get_row(0)
    }

    /// Go `baseJoiner.filter`: filter the candidate rows built from ONE outer
    /// row and many inner rows, copying the survivors into `output`.
    ///
    /// Returns whether the outer row matched anything.
    ///
    /// Go passes `lUsed`/`rUsed` explicitly, but every call site passes
    /// `j.lUsed`/`j.rUsed` and only ever calls this when `conditions` is
    /// non-empty (Go's "reach here, chkForJoin is j.chk"), so the input is
    /// always `self.chk` and the projection is always the joiner's own.
    fn filter(&mut self, output: &mut Chunk, outer_col_len: usize) -> Result<bool, ExecError> {
        let input = self
            .chk
            .take()
            .expect("filter runs only on the conditional path, which owns chk");
        let result = self.filter_inner(input, output, outer_col_len);
        result.map(|(matched, input)| {
            self.chk = Some(input);
            matched
        })
    }

    fn filter_inner(
        &mut self,
        mut input: Chunk,
        output: &mut Chunk,
        outer_col_len: usize,
    ) -> Result<(bool, Chunk), ExecError> {
        row_based_filter(
            &self.ctx,
            &self.conditions,
            &input,
            &mut self.selected,
            None,
        )?;

        let mut outer_col_len = outer_col_len;
        let (mut inner_col_offset, mut outer_col_offset) = (0, input.num_cols() - outer_col_len);
        let mut inner_col_len = input.num_cols() - outer_col_len;
        if !self.outer_is_right {
            inner_col_offset = outer_col_len;
            outer_col_offset = 0;
        }

        let l_used = self.l_used.clone();
        let r_used = self.r_used.clone();
        let mut pruned = None;
        if l_used.is_some() || r_used.is_some() {
            let l_size = if self.outer_is_right {
                outer_col_offset
            } else {
                inner_col_offset
            };
            let l_used = l_used.as_deref().unwrap_or_default();
            let r_used = r_used.as_deref().unwrap_or_default();
            let mut used = Vec::with_capacity(l_used.len() + r_used.len());
            used.extend_from_slice(l_used);
            used.extend(r_used.iter().map(|index| index + l_size));
            pruned = Some(input.prune(&used));

            inner_col_offset = 0;
            outer_col_offset = l_used.len();
            inner_col_len = l_used.len();
            outer_col_len = r_used.len();
            if !self.outer_is_right {
                inner_col_offset = l_used.len();
                outer_col_offset = 0;
                std::mem::swap(&mut inner_col_len, &mut outer_col_len);
            }
        }

        let source = pruned.as_ref().unwrap_or(&input);
        let matched = copy_selected_join_rows_with_same_outer_rows(
            source,
            inner_col_offset,
            inner_col_len,
            outer_col_offset,
            outer_col_len,
            &self.selected,
            output,
        )
        .map_err(ExecError::internal)?;
        Ok((matched, input))
    }

    /// Go `baseJoiner.filterAndCheckOuterRowStatus`: filter the candidate rows
    /// built from MANY outer rows and one inner row, recording each outer
    /// row's verdict.
    fn filter_and_check_outer_row_status(
        &mut self,
        output: &mut Chunk,
        inner_cols_len: usize,
        outer_row_status: &mut [OuterRowStatusFlag],
    ) -> Result<(), ExecError> {
        let mut input = self
            .chk
            .take()
            .expect("this path runs only when conditions exist, which owns chk");
        let result =
            self.filter_and_check_inner(&mut input, output, inner_cols_len, outer_row_status);
        self.chk = Some(input);
        result
    }

    fn filter_and_check_inner(
        &mut self,
        input: &mut Chunk,
        output: &mut Chunk,
        inner_cols_len: usize,
        outer_row_status: &mut [OuterRowStatusFlag],
    ) -> Result<(), ExecError> {
        row_based_filter(
            &self.ctx,
            &self.conditions,
            input,
            &mut self.selected,
            Some(&mut self.is_null),
        )?;
        for index in 0..self.selected.len().min(outer_row_status.len()) {
            if self.is_null[index] {
                outer_row_status[index] = OuterRowStatusFlag::HasNull;
            } else if !self.selected[index] {
                outer_row_status[index] = OuterRowStatusFlag::Unmatched;
            }
        }

        let l_used = self.l_used.clone();
        let r_used = self.r_used.clone();
        let mut pruned = None;
        if l_used.is_some() || r_used.is_some() {
            let l_size = if self.outer_is_right {
                inner_cols_len
            } else {
                input.num_cols() - inner_cols_len
            };
            let l_used = l_used.as_deref().unwrap_or_default();
            let r_used = r_used.as_deref().unwrap_or_default();
            let mut used = Vec::with_capacity(l_used.len() + r_used.len());
            used.extend_from_slice(l_used);
            used.extend(r_used.iter().map(|index| index + l_size));
            pruned = Some(input.prune(&used));
        }
        let source = pruned.as_ref().unwrap_or(&*input);
        copy_selected_join_rows_direct(source, &self.selected, output)
            .map_err(ExecError::internal)?;
        Ok(())
    }

    /// Go `baseJoiner.Clone`.
    fn clone_base(&self) -> BaseJoiner<C> {
        BaseJoiner {
            ctx: self.ctx.clone(),
            conditions: self.conditions.clone(),
            default_inner: self.default_inner.clone(),
            outer_is_right: self.outer_is_right,
            chk: self.chk.clone(),
            shallow_row: self.shallow_row.clone(),
            selected: Vec::with_capacity(self.selected.len()),
            is_null: Vec::with_capacity(self.is_null.len()),
            max_chunk_size: self.max_chunk_size,
            l_used: self.l_used.clone(),
            r_used: self.r_used.clone(),
        }
    }

    /// Go `len(j.conditions) == 0`, the shared body of
    /// `isSemiJoinWithoutCondition` for every semi-family joiner.
    fn has_no_condition(&self) -> bool {
        self.conditions.is_empty()
    }

    /// The joiner's `MaxChunkSize`, kept because Go's `baseJoiner` carries it
    /// and a probe that batches by it needs to read it back.
    #[must_use]
    pub fn max_chunk_size(&self) -> usize {
        self.max_chunk_size
    }
}

/// Go `Joiner`: generates join results according to the join type.
///
/// NOTE, as in Go: this is **not** thread-safe.
pub trait Joiner {
    /// Go `TryToMatchInners`: join one outer row with a batch of inner rows.
    ///
    /// Returns `(matched, is_null)`. `matched` is false when `inners` was
    /// empty or every joined row was filtered out; `is_null` distinguishes a
    /// FALSE condition from a NULL one and is always false for the join types
    /// that do not care.
    ///
    /// # Errors
    /// Propagates condition-evaluation and chunk-copy failures.
    fn try_to_match_inners(
        &mut self,
        outer: Row<'_>,
        inners: &mut LendingIterator<'_>,
        chk: &mut Chunk,
        opt: NAAJType,
    ) -> Result<(bool, bool), ExecError>;

    /// Go `TryToMatchOuters`: join a batch of outer rows with one inner row,
    /// used when the hash table was built on the outer side.
    ///
    /// `outer_row_status` is cleared and refilled, one entry per consumed
    /// outer row.
    ///
    /// # Errors
    /// Propagates condition-evaluation and chunk-copy failures.
    fn try_to_match_outers(
        &mut self,
        outers: &mut LendingIterator<'_>,
        inner: Row<'_>,
        chk: &mut Chunk,
        outer_row_status: &mut Vec<OuterRowStatusFlag>,
    ) -> Result<(), ExecError>;

    /// Go `OnMissMatch`: handle an outer row that matched nothing, per the
    /// join type's rule (see the table in Go's interface comment).
    fn on_miss_match(&mut self, has_null: bool, outer: Row<'_>, chk: &mut Chunk);

    /// Go `isSemiJoinWithoutCondition`: when true, one matching inner row
    /// settles the outer row, so the caller may stop probing early.
    fn is_semi_join_without_condition(&self) -> bool;

    /// Go `JoinerType` (`joiner.go:120`), as a method: see the module header
    /// for why the type switch could not be ported as a free function.
    fn join_type(&self) -> JoinType;

    /// Go `Clone`: deep copy.
    fn clone_joiner(&self) -> Box<dyn Joiner>;

    /// The capacity of Go `baseJoiner.chk`, the scratch chunk the filtering
    /// inner/outer joiners build candidate rows in; `None` when this joiner
    /// has no such chunk (every semi-family type, and an inner/outer joiner
    /// with no conditions).
    ///
    /// Go's `TestJoinerOtherConditionChunkUsesInitChunkSize` reaches into the
    /// struct from inside the package. Rust has no equivalent for a trait
    /// object, so the accessor is on the trait; nothing but that test reads
    /// it.
    fn scratch_chunk_capacity(&self) -> Option<usize> {
        None
    }
}

/// How many more rows the caller's output chunk wants, Go's
/// `chk.RequiredRows() - chk.NumRows()`.
fn num_to_append(chk: &Chunk) -> usize {
    chk.required_rows().saturating_sub(chk.num_rows())
}

// ---------------------------------------------------------------------------
// semiJoiner (`joiner.go:363`)
// ---------------------------------------------------------------------------

/// Go `semiJoiner`: `EXISTS`. Emits the outer row once, on the first match.
pub struct SemiJoiner<C: Columns> {
    base: BaseJoiner<C>,
}

impl<C: Columns + Clone + 'static> Joiner for SemiJoiner<C> {
    fn try_to_match_inners(
        &mut self,
        outer: Row<'_>,
        inners: &mut LendingIterator<'_>,
        chk: &mut Chunk,
        _opt: NAAJType,
    ) -> Result<(bool, bool), ExecError> {
        if inners.is_empty() {
            return Ok((false, false));
        }
        if self.base.conditions.is_empty() {
            chk.append_row_by_col_idxs(outer, self.base.l_used.as_deref());
            inners.reach_end();
            return Ok((true, false));
        }
        while let Some(inner) = inners.current() {
            self.base
                .make_shallow_join_row(self.base.outer_is_right, inner, outer);
            // For a semi join a NULL condition is safely treated as FALSE, so
            // Go ignores `EvalBool`'s nullness here and so does this.
            let (matched, _) = eval_bool(
                &self.base.ctx,
                &self.base.conditions,
                self.base.shallow_row(),
            )?;
            if matched {
                chk.append_row_by_col_idxs(outer, self.base.l_used.as_deref());
                inners.reach_end();
                return Ok((true, false));
            }
            inners.next_row();
        }
        Ok((false, false))
    }

    fn try_to_match_outers(
        &mut self,
        outers: &mut LendingIterator<'_>,
        inner: Row<'_>,
        chk: &mut Chunk,
        outer_row_status: &mut Vec<OuterRowStatusFlag>,
    ) -> Result<(), ExecError> {
        outer_row_status.clear();
        let mut budget = num_to_append(chk);
        if self.base.conditions.is_empty() {
            while budget > 0 {
                let Some(outer) = outers.current() else { break };
                chk.append_row_by_col_idxs(outer, self.base.l_used.as_deref());
                outer_row_status.push(OuterRowStatusFlag::Matched);
                outers.next_row();
                budget -= 1;
            }
            return Ok(());
        }
        while budget > 0 {
            let Some(outer) = outers.current() else { break };
            self.base
                .make_shallow_join_row(self.base.outer_is_right, inner, outer);
            let (matched, _) = eval_bool(
                &self.base.ctx,
                &self.base.conditions,
                self.base.shallow_row(),
            )?;
            if matched {
                outer_row_status.push(OuterRowStatusFlag::Matched);
                let outer = outers.current().expect("still positioned on a row");
                chk.append_row_by_col_idxs(outer, self.base.l_used.as_deref());
            } else {
                outer_row_status.push(OuterRowStatusFlag::Unmatched);
            }
            outers.next_row();
            budget -= 1;
        }
        Ok(())
    }

    fn on_miss_match(&mut self, _has_null: bool, _outer: Row<'_>, _chk: &mut Chunk) {}

    fn is_semi_join_without_condition(&self) -> bool {
        self.base.has_no_condition()
    }

    fn join_type(&self) -> JoinType {
        JoinType::SemiJoin
    }

    fn clone_joiner(&self) -> Box<dyn Joiner> {
        Box::new(SemiJoiner {
            base: self.base.clone_base(),
        })
    }
}

// ---------------------------------------------------------------------------
// nullAwareAntiSemiJoiner (`joiner.go:455`)
// ---------------------------------------------------------------------------

/// Go `nullAwareAntiSemiJoiner`: `NOT IN` with a null-aware key. Its
/// conditions carry only the inner filters, so a NULL there is just a
/// non-match; the NA-EQ nullness has already been resolved by the bucket the
/// caller drew `inners` from.
pub struct NullAwareAntiSemiJoiner<C: Columns> {
    base: BaseJoiner<C>,
}

impl<C: Columns + Clone + 'static> Joiner for NullAwareAntiSemiJoiner<C> {
    fn try_to_match_inners(
        &mut self,
        outer: Row<'_>,
        inners: &mut LendingIterator<'_>,
        _chk: &mut Chunk,
        _opt: NAAJType,
    ) -> Result<(bool, bool), ExecError> {
        // Step 1: inner rows come from the NULL bucket OR the same-key bucket;
        // no rows means no match.
        if inners.is_empty() {
            return Ok((false, false));
        }
        // Step 2: with no other condition every inner row is valid, so the
        // right side is non-empty and the probe row is refused.
        if self.base.conditions.is_empty() {
            inners.reach_end();
            return Ok((true, false));
        }
        while let Some(inner) = inners.current() {
            self.base
                .make_shallow_join_row(self.base.outer_is_right, inner, outer);
            let (valid, _) = eval_bool(
                &self.base.ctx,
                &self.base.conditions,
                self.base.shallow_row(),
            )?;
            // For `x NOT IN (y set)`, one x found in y settles it: refuse the
            // probe row and append nothing.
            if valid {
                inners.reach_end();
                return Ok((true, false));
            }
            inners.next_row();
        }
        Ok((false, false))
    }

    fn try_to_match_outers(
        &mut self,
        _outers: &mut LendingIterator<'_>,
        _inner: Row<'_>,
        _chk: &mut Chunk,
        _outer_row_status: &mut Vec<OuterRowStatusFlag>,
    ) -> Result<(), ExecError> {
        // Go: "todo: use the Outer build." -- it returns the caller's slice
        // untouched, so this leaves `outer_row_status` alone too.
        Ok(())
    }

    fn on_miss_match(&mut self, _has_null: bool, outer: Row<'_>, chk: &mut Chunk) {
        chk.append_row_by_col_idxs(outer, self.base.l_used.as_deref());
    }

    fn is_semi_join_without_condition(&self) -> bool {
        self.base.has_no_condition()
    }

    fn join_type(&self) -> JoinType {
        JoinType::AntiSemiJoin
    }

    fn clone_joiner(&self) -> Box<dyn Joiner> {
        Box::new(NullAwareAntiSemiJoiner {
            base: self.base.clone_base(),
        })
    }
}

// ---------------------------------------------------------------------------
// antiSemiJoiner (`joiner.go:504`)
// ---------------------------------------------------------------------------

/// Go `antiSemiJoiner`: `NOT EXISTS`. Emits the outer row only when nothing
/// matched AND nothing was NULL.
pub struct AntiSemiJoiner<C: Columns> {
    base: BaseJoiner<C>,
}

impl<C: Columns + Clone + 'static> Joiner for AntiSemiJoiner<C> {
    fn try_to_match_inners(
        &mut self,
        outer: Row<'_>,
        inners: &mut LendingIterator<'_>,
        _chk: &mut Chunk,
        _opt: NAAJType,
    ) -> Result<(bool, bool), ExecError> {
        if inners.is_empty() {
            return Ok((false, false));
        }
        if self.base.conditions.is_empty() {
            inners.reach_end();
            return Ok((true, false));
        }
        let mut has_null = false;
        while let Some(inner) = inners.current() {
            self.base
                .make_shallow_join_row(self.base.outer_is_right, inner, outer);
            let (matched, is_null) = eval_bool(
                &self.base.ctx,
                &self.base.conditions,
                self.base.shallow_row(),
            )?;
            if matched {
                inners.reach_end();
                return Ok((true, false));
            }
            has_null = has_null || is_null;
            inners.next_row();
        }
        Ok((false, has_null))
    }

    fn try_to_match_outers(
        &mut self,
        outers: &mut LendingIterator<'_>,
        inner: Row<'_>,
        chk: &mut Chunk,
        outer_row_status: &mut Vec<OuterRowStatusFlag>,
    ) -> Result<(), ExecError> {
        outer_row_status.clear();
        let mut budget = num_to_append(chk);
        if self.base.conditions.is_empty() {
            // Go walks the WHOLE iterator here, ignoring `numToAppend`,
            // because no row is appended to `chk` on this path.
            while outers.current().is_some() {
                outer_row_status.push(OuterRowStatusFlag::Matched);
                outers.next_row();
            }
            return Ok(());
        }
        while budget > 0 {
            let Some(outer) = outers.current() else { break };
            self.base
                .make_shallow_join_row(self.base.outer_is_right, inner, outer);
            let (matched, is_null) = eval_bool(
                &self.base.ctx,
                &self.base.conditions,
                self.base.shallow_row(),
            )?;
            outer_row_status.push(if matched {
                OuterRowStatusFlag::Matched
            } else if is_null {
                OuterRowStatusFlag::HasNull
            } else {
                OuterRowStatusFlag::Unmatched
            });
            outers.next_row();
            budget -= 1;
        }
        Ok(())
    }

    fn on_miss_match(&mut self, has_null: bool, outer: Row<'_>, chk: &mut Chunk) {
        if !has_null {
            chk.append_row_by_col_idxs(outer, self.base.l_used.as_deref());
        }
    }

    fn is_semi_join_without_condition(&self) -> bool {
        self.base.has_no_condition()
    }

    fn join_type(&self) -> JoinType {
        JoinType::AntiSemiJoin
    }

    fn clone_joiner(&self) -> Box<dyn Joiner> {
        Box::new(AntiSemiJoiner {
            base: self.base.clone_base(),
        })
    }
}

// ---------------------------------------------------------------------------
// leftOuterSemiJoiner (`joiner.go:576`)
// ---------------------------------------------------------------------------

/// Go `leftOuterSemiJoiner`: the outer row plus a flag column -- 1 on match,
/// 0 on a FALSE miss, NULL on a miss that saw NULL.
pub struct LeftOuterSemiJoiner<C: Columns> {
    base: BaseJoiner<C>,
}

impl<C: Columns + Clone> LeftOuterSemiJoiner<C> {
    /// Go `leftOuterSemiJoiner.onMatch`.
    fn on_match(&self, outer: Row<'_>, chk: &mut Chunk) {
        let l_wide = chk.append_row_by_col_idxs(outer, self.base.l_used.as_deref());
        chk.append_int64(l_wide, 1);
    }
}

impl<C: Columns + Clone + 'static> Joiner for LeftOuterSemiJoiner<C> {
    fn try_to_match_inners(
        &mut self,
        outer: Row<'_>,
        inners: &mut LendingIterator<'_>,
        chk: &mut Chunk,
        _opt: NAAJType,
    ) -> Result<(bool, bool), ExecError> {
        if inners.is_empty() {
            return Ok((false, false));
        }
        if self.base.conditions.is_empty() {
            self.on_match(outer, chk);
            inners.reach_end();
            return Ok((true, false));
        }
        let mut has_null = false;
        while let Some(inner) = inners.current() {
            // Go passes a literal `false` here, not `j.outerIsRight`: a
            // left-outer-semi join's outer side is always the left one.
            self.base.make_shallow_join_row(false, inner, outer);
            let (matched, is_null) = eval_bool(
                &self.base.ctx,
                &self.base.conditions,
                self.base.shallow_row(),
            )?;
            if matched {
                self.on_match(outer, chk);
                inners.reach_end();
                return Ok((true, false));
            }
            has_null = has_null || is_null;
            inners.next_row();
        }
        Ok((false, has_null))
    }

    fn try_to_match_outers(
        &mut self,
        outers: &mut LendingIterator<'_>,
        inner: Row<'_>,
        chk: &mut Chunk,
        outer_row_status: &mut Vec<OuterRowStatusFlag>,
    ) -> Result<(), ExecError> {
        outer_row_status.clear();
        let mut budget = num_to_append(chk);
        if self.base.conditions.is_empty() {
            while budget > 0 {
                let Some(outer) = outers.current() else { break };
                self.on_match(outer, chk);
                outer_row_status.push(OuterRowStatusFlag::Matched);
                outers.next_row();
                budget -= 1;
            }
            return Ok(());
        }
        while budget > 0 {
            let Some(outer) = outers.current() else { break };
            self.base.make_shallow_join_row(false, inner, outer);
            let (matched, is_null) = eval_bool(
                &self.base.ctx,
                &self.base.conditions,
                self.base.shallow_row(),
            )?;
            if matched {
                let outer = outers.current().expect("still positioned on a row");
                self.on_match(outer, chk);
                outer_row_status.push(OuterRowStatusFlag::Matched);
            } else if is_null {
                outer_row_status.push(OuterRowStatusFlag::HasNull);
            } else {
                outer_row_status.push(OuterRowStatusFlag::Unmatched);
            }
            outers.next_row();
            budget -= 1;
        }
        Ok(())
    }

    fn on_miss_match(&mut self, has_null: bool, outer: Row<'_>, chk: &mut Chunk) {
        let l_wide = chk.append_row_by_col_idxs(outer, self.base.l_used.as_deref());
        if has_null {
            chk.append_null(l_wide);
        } else {
            chk.append_int64(l_wide, 0);
        }
    }

    fn is_semi_join_without_condition(&self) -> bool {
        self.base.has_no_condition()
    }

    fn join_type(&self) -> JoinType {
        JoinType::LeftOuterSemiJoin
    }

    fn clone_joiner(&self) -> Box<dyn Joiner> {
        Box::new(LeftOuterSemiJoiner {
            base: self.base.clone_base(),
        })
    }
}

// ---------------------------------------------------------------------------
// nullAwareAntiLeftOuterSemiJoiner (`joiner.go:658`)
// ---------------------------------------------------------------------------

/// Go `nullAwareAntiLeftOuterSemiJoiner`: null-aware `NOT IN` in the
/// left-outer-semi shape. Unlike [`AntiLeftOuterSemiJoiner`], its conditions
/// hold only the inner filters, so `EvalBool`'s nullness is never consulted --
/// the NA-EQ nullness arrives as [`NAAJType`] instead.
pub struct NullAwareAntiLeftOuterSemiJoiner<C: Columns> {
    base: BaseJoiner<C>,
}

impl<C: Columns + Clone> NullAwareAntiLeftOuterSemiJoiner<C> {
    /// Go `nullAwareAntiLeftOuterSemiJoiner.onMatch`.
    fn on_match(&self, outer: Row<'_>, chk: &mut Chunk, opt: NAAJType) {
        match opt {
            NAAJType::LeftNotNullRightNotNull => {
                // Neither side is null: `x NOT IN (x...)` -> (rhs, 0).
                let l_wide = chk.append_row_by_col_idxs(outer, self.base.l_used.as_deref());
                chk.append_int64(l_wide, 0);
            }
            NAAJType::LeftNotNullRightHasNull
            | NAAJType::LeftHasNullRightHasNull
            | NAAJType::LeftHasNullRightNotNull => {
                // Either `x NOT IN (null...)` or a null left key: -> (rhs, null).
                let l_wide = chk.append_row_by_col_idxs(outer, self.base.l_used.as_deref());
                chk.append_null(l_wide);
            }
            // Go's switch has no default arm; `Unknown` emits nothing.
            NAAJType::Unknown => {}
        }
    }
}

impl<C: Columns + Clone + 'static> Joiner for NullAwareAntiLeftOuterSemiJoiner<C> {
    fn try_to_match_inners(
        &mut self,
        outer: Row<'_>,
        inners: &mut LendingIterator<'_>,
        chk: &mut Chunk,
        opt: NAAJType,
    ) -> Result<(bool, bool), ExecError> {
        if inners.is_empty() {
            return Ok((false, false));
        }
        if self.base.conditions.is_empty() {
            // No inner filter means every inner row is a valid source.
            self.on_match(outer, chk, opt);
            inners.reach_end();
            return Ok((true, false));
        }
        while let Some(inner) = inners.current() {
            self.base.make_shallow_join_row(false, inner, outer);
            let (valid, _) = eval_bool(
                &self.base.ctx,
                &self.base.conditions,
                self.base.shallow_row(),
            )?;
            if valid {
                self.on_match(outer, chk, opt);
                inners.reach_end();
                return Ok((true, false));
            }
            inners.next_row();
        }
        Ok((false, false))
    }

    fn try_to_match_outers(
        &mut self,
        _outers: &mut LendingIterator<'_>,
        _inner: Row<'_>,
        _chk: &mut Chunk,
        outer_row_status: &mut Vec<OuterRowStatusFlag>,
    ) -> Result<(), ExecError> {
        // Go: "todo:" -- it returns a nil slice, which is this clear.
        outer_row_status.clear();
        Ok(())
    }

    fn on_miss_match(&mut self, _has_null: bool, outer: Row<'_>, chk: &mut Chunk) {
        // Reaching here means no short path fired: `null/x NOT IN (empty set)`
        // or `x NOT IN (non-empty set without x and without null)`.
        let l_wide = chk.append_row_by_col_idxs(outer, self.base.l_used.as_deref());
        chk.append_int64(l_wide, 1);
    }

    fn is_semi_join_without_condition(&self) -> bool {
        self.base.has_no_condition()
    }

    fn join_type(&self) -> JoinType {
        JoinType::AntiLeftOuterSemiJoin
    }

    fn clone_joiner(&self) -> Box<dyn Joiner> {
        Box::new(NullAwareAntiLeftOuterSemiJoiner {
            base: self.base.clone_base(),
        })
    }
}

// ---------------------------------------------------------------------------
// antiLeftOuterSemiJoiner (`joiner.go:746`)
// ---------------------------------------------------------------------------

/// Go `antiLeftOuterSemiJoiner`: the outer row plus a flag -- 0 on match, 1 on
/// a FALSE miss, NULL on a miss that saw NULL. The negation of
/// [`LeftOuterSemiJoiner`].
pub struct AntiLeftOuterSemiJoiner<C: Columns> {
    base: BaseJoiner<C>,
}

impl<C: Columns + Clone> AntiLeftOuterSemiJoiner<C> {
    /// Go `antiLeftOuterSemiJoiner.onMatch`.
    fn on_match(&self, outer: Row<'_>, chk: &mut Chunk) {
        let l_wide = chk.append_row_by_col_idxs(outer, self.base.l_used.as_deref());
        chk.append_int64(l_wide, 0);
    }
}

impl<C: Columns + Clone + 'static> Joiner for AntiLeftOuterSemiJoiner<C> {
    fn try_to_match_inners(
        &mut self,
        outer: Row<'_>,
        inners: &mut LendingIterator<'_>,
        chk: &mut Chunk,
        _opt: NAAJType,
    ) -> Result<(bool, bool), ExecError> {
        if inners.is_empty() {
            return Ok((false, false));
        }
        if self.base.conditions.is_empty() {
            self.on_match(outer, chk);
            inners.reach_end();
            return Ok((true, false));
        }
        let mut has_null = false;
        while let Some(inner) = inners.current() {
            self.base.make_shallow_join_row(false, inner, outer);
            let (matched, is_null) = eval_bool(
                &self.base.ctx,
                &self.base.conditions,
                self.base.shallow_row(),
            )?;
            if matched {
                self.on_match(outer, chk);
                inners.reach_end();
                return Ok((true, false));
            }
            has_null = has_null || is_null;
            inners.next_row();
        }
        Ok((false, has_null))
    }

    fn try_to_match_outers(
        &mut self,
        outers: &mut LendingIterator<'_>,
        inner: Row<'_>,
        chk: &mut Chunk,
        outer_row_status: &mut Vec<OuterRowStatusFlag>,
    ) -> Result<(), ExecError> {
        outer_row_status.clear();
        let mut budget = num_to_append(chk);
        if self.base.conditions.is_empty() {
            while budget > 0 {
                let Some(outer) = outers.current() else { break };
                self.on_match(outer, chk);
                outer_row_status.push(OuterRowStatusFlag::Matched);
                outers.next_row();
                budget -= 1;
            }
            return Ok(());
        }
        while budget > 0 {
            let Some(outer) = outers.current() else { break };
            self.base.make_shallow_join_row(false, inner, outer);
            let (matched, is_null) = eval_bool(
                &self.base.ctx,
                &self.base.conditions,
                self.base.shallow_row(),
            )?;
            if matched {
                let outer = outers.current().expect("still positioned on a row");
                self.on_match(outer, chk);
                outer_row_status.push(OuterRowStatusFlag::Matched);
            } else if is_null {
                outer_row_status.push(OuterRowStatusFlag::HasNull);
            } else {
                outer_row_status.push(OuterRowStatusFlag::Unmatched);
            }
            outers.next_row();
            budget -= 1;
        }
        Ok(())
    }

    fn on_miss_match(&mut self, has_null: bool, outer: Row<'_>, chk: &mut Chunk) {
        let l_wide = chk.append_row_by_col_idxs(outer, self.base.l_used.as_deref());
        if has_null {
            chk.append_null(l_wide);
        } else {
            chk.append_int64(l_wide, 1);
        }
    }

    fn is_semi_join_without_condition(&self) -> bool {
        self.base.has_no_condition()
    }

    fn join_type(&self) -> JoinType {
        JoinType::AntiLeftOuterSemiJoin
    }

    fn clone_joiner(&self) -> Box<dyn Joiner> {
        Box::new(AntiLeftOuterSemiJoiner {
            base: self.base.clone_base(),
        })
    }
}

// ---------------------------------------------------------------------------
// leftOuterJoiner / rightOuterJoiner / innerJoiner (`joiner.go:833`+)
// ---------------------------------------------------------------------------

/// Which side each of the three row-producing joiners puts first, and whether
/// the placement depends on `outer_is_right`.
///
/// Go writes the three `makeJoinRowToChunk` call sites out longhand; they
/// differ only here, so one enum keeps the three bodies from drifting.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RowOrder {
    /// Go `leftOuterJoiner`: `makeJoinRowToChunk(chk, outer, inner)`.
    OuterFirst,
    /// Go `rightOuterJoiner`: `makeJoinRowToChunk(chk, inner, outer)`.
    InnerFirst,
    /// Go `innerJoiner`: inner first iff `outerIsRight`.
    ByOuterSide,
}

/// The shared body of the three row-producing joiners.
///
/// `leftOuterJoiner`, `rightOuterJoiner` and `innerJoiner` have textually
/// identical `TryToMatchInners`/`TryToMatchOuters` in Go apart from the
/// argument order captured by [`RowOrder`], so they share one implementation
/// and differ only in `OnMissMatch`, `isSemiJoinWithoutCondition` and their
/// join type.
struct RowJoiner<C: Columns> {
    base: BaseJoiner<C>,
    order: RowOrder,
}

impl<C: Columns + Clone> RowJoiner<C> {
    fn lhs_first(&self) -> bool {
        match self.order {
            RowOrder::OuterFirst => true,
            RowOrder::InnerFirst => false,
            RowOrder::ByOuterSide => !self.base.outer_is_right,
        }
    }

    fn try_to_match_inners(
        &mut self,
        outer: Row<'_>,
        inners: &mut LendingIterator<'_>,
        chk: &mut Chunk,
    ) -> Result<(bool, bool), ExecError> {
        if inners.is_empty() {
            return Ok((false, false));
        }
        let outer_first = self.lhs_first();
        let conditional = !self.base.conditions.is_empty();
        let (l_used, r_used) = if conditional {
            (None, None)
        } else {
            (self.base.l_used.clone(), self.base.r_used.clone())
        };
        let mut budget = num_to_append(chk);
        let outer_len = outer.len();

        let mut scratch = if conditional {
            let mut scratch = self
                .base
                .chk
                .take()
                .expect("a conditional inner/outer joiner owns chk");
            scratch.reset();
            Some(scratch)
        } else {
            None
        };
        {
            let target: &mut Chunk = scratch.as_mut().unwrap_or(chk);
            loop {
                if budget == 0 {
                    break;
                }
                let Some(inner) = inners.current() else { break };
                if outer_first {
                    make_join_row_to_chunk(
                        target,
                        outer,
                        inner,
                        l_used.as_deref(),
                        r_used.as_deref(),
                    );
                } else {
                    make_join_row_to_chunk(
                        target,
                        inner,
                        outer,
                        l_used.as_deref(),
                        r_used.as_deref(),
                    );
                }
                inners.next_row();
                budget -= 1;
            }
        }
        if let Some(scratch) = scratch {
            self.base.chk = Some(scratch);
        }
        if !conditional {
            return Ok((true, false));
        }
        let matched = self.base.filter(chk, outer_len)?;
        Ok((matched, false))
    }

    fn try_to_match_outers(
        &mut self,
        outers: &mut LendingIterator<'_>,
        inner: Row<'_>,
        chk: &mut Chunk,
        outer_row_status: &mut Vec<OuterRowStatusFlag>,
    ) -> Result<(), ExecError> {
        let outer_first = self.lhs_first();
        let conditional = !self.base.conditions.is_empty();
        let (l_used, r_used) = if conditional {
            (None, None)
        } else {
            (self.base.l_used.clone(), self.base.r_used.clone())
        };
        let budget = num_to_append(chk);
        let inner_len = inner.len();
        let mut cursor = 0;

        let mut scratch = if conditional {
            let mut scratch = self
                .base
                .chk
                .take()
                .expect("a conditional inner/outer joiner owns chk");
            scratch.reset();
            Some(scratch)
        } else {
            None
        };
        {
            let target: &mut Chunk = scratch.as_mut().unwrap_or(chk);
            while cursor < budget {
                let Some(outer) = outers.current() else { break };
                if outer_first {
                    make_join_row_to_chunk(
                        target,
                        outer,
                        inner,
                        l_used.as_deref(),
                        r_used.as_deref(),
                    );
                } else {
                    make_join_row_to_chunk(
                        target,
                        inner,
                        outer,
                        l_used.as_deref(),
                        r_used.as_deref(),
                    );
                }
                outers.next_row();
                cursor += 1;
            }
        }
        if let Some(scratch) = scratch {
            self.base.chk = Some(scratch);
        }
        outer_row_status.clear();
        outer_row_status.resize(cursor, OuterRowStatusFlag::Matched);
        if !conditional {
            return Ok(());
        }
        self.base
            .filter_and_check_outer_row_status(chk, inner_len, outer_row_status)
    }
}

/// Go `leftOuterJoiner`.
pub struct LeftOuterJoiner<C: Columns> {
    inner: RowJoiner<C>,
}

impl<C: Columns + Clone + 'static> Joiner for LeftOuterJoiner<C> {
    fn try_to_match_inners(
        &mut self,
        outer: Row<'_>,
        inners: &mut LendingIterator<'_>,
        chk: &mut Chunk,
        _opt: NAAJType,
    ) -> Result<(bool, bool), ExecError> {
        self.inner.try_to_match_inners(outer, inners, chk)
    }

    fn try_to_match_outers(
        &mut self,
        outers: &mut LendingIterator<'_>,
        inner: Row<'_>,
        chk: &mut Chunk,
        outer_row_status: &mut Vec<OuterRowStatusFlag>,
    ) -> Result<(), ExecError> {
        self.inner
            .try_to_match_outers(outers, inner, chk, outer_row_status)
    }

    fn on_miss_match(&mut self, _has_null: bool, outer: Row<'_>, chk: &mut Chunk) {
        let base = &self.inner.base;
        let l_wide = chk.append_row_by_col_idxs(outer, base.l_used.as_deref());
        let default_inner = base
            .default_inner
            .as_ref()
            .expect("an outer joiner always builds its default inner row");
        chk.append_partial_row_by_col_idxs(l_wide, default_inner.to_row(), base.r_used.as_deref());
    }

    fn is_semi_join_without_condition(&self) -> bool {
        false
    }

    fn join_type(&self) -> JoinType {
        JoinType::LeftOuter
    }

    fn clone_joiner(&self) -> Box<dyn Joiner> {
        Box::new(LeftOuterJoiner {
            inner: RowJoiner {
                base: self.inner.base.clone_base(),
                order: self.inner.order,
            },
        })
    }

    fn scratch_chunk_capacity(&self) -> Option<usize> {
        self.inner.base.chk.as_ref().map(Chunk::capacity)
    }
}

/// Go `rightOuterJoiner`.
pub struct RightOuterJoiner<C: Columns> {
    inner: RowJoiner<C>,
}

impl<C: Columns + Clone + 'static> Joiner for RightOuterJoiner<C> {
    fn try_to_match_inners(
        &mut self,
        outer: Row<'_>,
        inners: &mut LendingIterator<'_>,
        chk: &mut Chunk,
        _opt: NAAJType,
    ) -> Result<(bool, bool), ExecError> {
        self.inner.try_to_match_inners(outer, inners, chk)
    }

    fn try_to_match_outers(
        &mut self,
        outers: &mut LendingIterator<'_>,
        inner: Row<'_>,
        chk: &mut Chunk,
        outer_row_status: &mut Vec<OuterRowStatusFlag>,
    ) -> Result<(), ExecError> {
        self.inner
            .try_to_match_outers(outers, inner, chk, outer_row_status)
    }

    fn on_miss_match(&mut self, _has_null: bool, outer: Row<'_>, chk: &mut Chunk) {
        let base = &self.inner.base;
        let default_inner = base
            .default_inner
            .as_ref()
            .expect("an outer joiner always builds its default inner row");
        let l_wide = chk.append_row_by_col_idxs(default_inner.to_row(), base.l_used.as_deref());
        chk.append_partial_row_by_col_idxs(l_wide, outer, base.r_used.as_deref());
    }

    fn is_semi_join_without_condition(&self) -> bool {
        false
    }

    fn join_type(&self) -> JoinType {
        JoinType::RightOuter
    }

    fn clone_joiner(&self) -> Box<dyn Joiner> {
        Box::new(RightOuterJoiner {
            inner: RowJoiner {
                base: self.inner.base.clone_base(),
                order: self.inner.order,
            },
        })
    }

    fn scratch_chunk_capacity(&self) -> Option<usize> {
        self.inner.base.chk.as_ref().map(Chunk::capacity)
    }
}

/// Go `innerJoiner`.
pub struct InnerJoiner<C: Columns> {
    inner: RowJoiner<C>,
}

impl<C: Columns + Clone + 'static> Joiner for InnerJoiner<C> {
    fn try_to_match_inners(
        &mut self,
        outer: Row<'_>,
        inners: &mut LendingIterator<'_>,
        chk: &mut Chunk,
        _opt: NAAJType,
    ) -> Result<(bool, bool), ExecError> {
        self.inner.try_to_match_inners(outer, inners, chk)
    }

    fn try_to_match_outers(
        &mut self,
        outers: &mut LendingIterator<'_>,
        inner: Row<'_>,
        chk: &mut Chunk,
        outer_row_status: &mut Vec<OuterRowStatusFlag>,
    ) -> Result<(), ExecError> {
        self.inner
            .try_to_match_outers(outers, inner, chk, outer_row_status)
    }

    fn on_miss_match(&mut self, _has_null: bool, _outer: Row<'_>, _chk: &mut Chunk) {}

    fn is_semi_join_without_condition(&self) -> bool {
        false
    }

    fn join_type(&self) -> JoinType {
        JoinType::Inner
    }

    fn clone_joiner(&self) -> Box<dyn Joiner> {
        Box::new(InnerJoiner {
            inner: RowJoiner {
                base: self.inner.base.clone_base(),
                order: self.inner.order,
            },
        })
    }

    fn scratch_chunk_capacity(&self) -> Option<usize> {
        self.inner.base.chk.as_ref().map(Chunk::capacity)
    }
}

/// The session-variable reads Go's `NewJoiner` performs through
/// `ctx.GetSessionVars()`.
///
/// They are an explicit argument because [`Columns`] carries no `SessionVars`;
/// see the module header.
#[derive(Clone, Copy, Debug)]
pub struct JoinerChunkSizes {
    /// Go `SessionVars.InitChunkSize`.
    pub init_chunk_size: usize,
    /// Go `SessionVars.MaxChunkSize`.
    pub max_chunk_size: usize,
}

/// Go `NewJoiner` (`joiner.go:138`).
///
/// `children_used` is Go's `childrenUsed [][]int`: `None` is a nil slice
/// (every column is output), `Some([left, right])` an inline projection. The
/// two vectors keep their original order, because a join schema may rely on a
/// reversed order of the child's schema.
#[must_use]
// Go's `NewJoiner` takes nine parameters; folding them into a struct would
// hide which argument is which Go one, so the shape is kept.
#[allow(clippy::too_many_arguments)]
pub fn new_joiner<C: Columns + Clone + 'static>(
    ctx: C,
    join_type: JoinType,
    outer_is_right: bool,
    default_inner: &[Datum],
    filter: Vec<Expression>,
    lhs_col_types: &[FieldType],
    rhs_col_types: &[FieldType],
    children_used: Option<(Vec<usize>, Vec<usize>)>,
    is_na: bool,
    sizes: JoinerChunkSizes,
) -> Box<dyn Joiner> {
    let (l_used, r_used) = match children_used {
        Some((left, right)) => (Some(left), Some(right)),
        None => (None, None),
    };
    let mut base = BaseJoiner {
        ctx,
        conditions: filter,
        default_inner: None,
        outer_is_right,
        chk: None,
        shallow_row: None,
        selected: Vec::with_capacity(tidb_chunk::chunk::INITIAL_CAPACITY),
        is_null: Vec::with_capacity(tidb_chunk::chunk::INITIAL_CAPACITY),
        max_chunk_size: sizes.max_chunk_size,
        l_used,
        r_used,
    };

    if matches!(join_type, JoinType::LeftOuter | JoinType::RightOuter) {
        let inner_col_types = if outer_is_right {
            lhs_col_types
        } else {
            rhs_col_types
        };
        base.init_default_inner(inner_col_types, default_inner);
    }

    // The shallow row's type list is NOT the output column list: an inline
    // projection may prune a column the conditions still read.
    let shallow_row_type: Vec<FieldType> = lhs_col_types
        .iter()
        .chain(rhs_col_types.iter())
        .cloned()
        .collect();

    match join_type {
        JoinType::SemiJoin => {
            base.shallow_row = Some(Chunk::new_with_capacity(&shallow_row_type, 1));
            Box::new(SemiJoiner { base })
        }
        JoinType::AntiSemiJoin => {
            base.shallow_row = Some(Chunk::new_with_capacity(&shallow_row_type, 1));
            if is_na {
                Box::new(NullAwareAntiSemiJoiner { base })
            } else {
                Box::new(AntiSemiJoiner { base })
            }
        }
        JoinType::LeftOuterSemiJoin => {
            base.shallow_row = Some(Chunk::new_with_capacity(&shallow_row_type, 1));
            Box::new(LeftOuterSemiJoiner { base })
        }
        JoinType::AntiLeftOuterSemiJoin => {
            base.shallow_row = Some(Chunk::new_with_capacity(&shallow_row_type, 1));
            if is_na {
                Box::new(NullAwareAntiLeftOuterSemiJoiner { base })
            } else {
                Box::new(AntiLeftOuterSemiJoiner { base })
            }
        }
        JoinType::LeftOuter | JoinType::RightOuter | JoinType::Inner => {
            if !base.conditions.is_empty() {
                base.chk = Some(Chunk::new(
                    &shallow_row_type,
                    sizes.init_chunk_size,
                    sizes.max_chunk_size,
                ));
            }
            let order = match join_type {
                JoinType::LeftOuter => RowOrder::OuterFirst,
                JoinType::RightOuter => RowOrder::InnerFirst,
                _ => RowOrder::ByOuterSide,
            };
            let inner = RowJoiner { base, order };
            match join_type {
                JoinType::LeftOuter => Box::new(LeftOuterJoiner { inner }),
                JoinType::RightOuter => Box::new(RightOuterJoiner { inner }),
                _ => Box::new(InnerJoiner { inner }),
            }
        }
    }
}

#[cfg(test)]
mod tests;
