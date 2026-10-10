// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `index_join_path.go`: how one inner access path serves an index join.
//!
//! `indexJoinPathBuild` decides, for the path's index columns, which leading
//! columns the outer join keys and the inner EQ/IN predicates pin, whether
//! the next column takes a static range or a per-outer-row comparison range,
//! and builds the template ranges whose join-key slots the executor fills
//! for every outer row. The same build reruns on plan-cache reuse
//! (`mutableIndexJoinRange.Rebuild`) without the range memory limit.

use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;

use crate::physical::{IndexJoinCompareFilters, IndexJoinCompareOp};
use crate::plan_base::PlanError;
use crate::ranger::points::ExpressionEvaluator;
use crate::ranger::types::{Range, Ranges};

/// Go `indexJoinPathInfo`: what the join tells the inner path.
#[derive(Clone, Copy)]
pub(crate) struct IndexJoinPathInfo<'a> {
    /// Go `joinOtherConditions`: candidates for the last column's
    /// per-outer-row comparison range.
    pub other_conditions: &'a [Expression],
    /// Go `outerJoinKeys`.
    pub outer_join_keys: &'a [Column],
    /// Go `innerJoinKeys`.
    pub inner_join_keys: &'a [Column],
    /// Go `innerPushedConditions`: the inner DataSource's pushed conditions.
    pub inner_pushed_conditions: &'a [Expression],
    /// Go `innerSchema`.
    pub inner_schema: &'a Schema,
}

/// The range-building environment of one `indexJoinPathBuild` call.
#[derive(Clone, Copy)]
pub(crate) struct IndexJoinRangeEnv<'a> {
    /// Go `RangeMaxSize`; zero in rebuild mode ("When rebuilding ranges for
    /// plan cache, we don't restrict range mem limit").
    pub range_max_size: i64,
    /// Go `StmtCtx.RecordRangeFallback`'s sink.
    pub fallback_handler: Option<&'a tidb_util::context::RangeFallbackHandler>,
    /// Go `RangerContext.OptPrefixIndexSingleScan`.
    pub opt_prefix_index_single_scan: bool,
    /// The statement's constant evaluator.
    pub evaluate: &'a ExpressionEvaluator<'a>,
    /// Go `sctx.SetSkipPlanCache` from the ranger, when the statement uses
    /// the plan cache.
    pub plan_cache_marker: Option<&'a dyn crate::logical::rule::PlanCacheMarker>,
}

impl IndexJoinRangeEnv<'_> {
    fn record_range_fallback(&self) {
        if let Some(handler) = self.fallback_handler {
            handler.record_range_fallback(self.range_max_size);
        }
    }
}

/// Go `indexJoinPathTmp`.
#[derive(Default)]
struct IndexJoinPathTmp {
    cur_possible_used_keys: Vec<Column>,
    cur_not_used_index_cols: Vec<Column>,
    cur_not_used_col_lens: Vec<i64>,
    /// The inner key offset of each index column, `-1` when none.
    cur_idx_off2_key_off: Vec<i64>,
}

/// Go `indexJoinTmpRange`.
#[derive(Default)]
struct IndexJoinTmpRange {
    ranges: Ranges,
    empty_range: bool,
    key_cnt_in_range: usize,
    eq_and_in_cnt_in_range: usize,
    next_col_in_range: bool,
    extra_col_in_range: bool,
}

/// Go `indexJoinPathResult`, without the skyline candidate and the NDV the
/// caller derives from the DataSource.
#[derive(Clone, Debug)]
pub(crate) struct IndexJoinPathCore {
    /// Go `chosenAccess`: the static predicates the template ranges absorb.
    pub chosen_access: Vec<Expression>,
    /// Go `chosenRemained`: the predicates the inner scan still evaluates.
    pub chosen_remained: Vec<Expression>,
    /// Go `chosenRanges`: the template ranges, a NULL placeholder in every
    /// join-key slot (and in the comparison-range slot).
    pub ranges: Ranges,
    /// Go `usedColsLen`: the template width.
    pub used_cols_len: usize,
    /// The `usedColsLen` argument of `indexJoinPathConstructResult`, which
    /// `getIndexCandidateForIndexJoin` reads as `indexJoinCols`.
    pub index_join_cols: usize,
    /// The width of the equality prefix the NDV estimate covers.
    pub eq_used_cols_len: usize,
    /// Go `idxOff2KeyOff`.
    pub idx_off2_key_off: Vec<i64>,
    /// Go `lastColManager`.
    pub last_col_manager: Option<IndexJoinCompareFilters>,
}

/// What `indexJoinPathBuild` answers for one path.
pub(crate) enum IndexJoinPathOutcome {
    /// Go `(nil, false, nil)`: the path cannot drive this index join.
    NotApplicable,
    /// Go's `emptyRange`: the inner predicates contradict, so no path can.
    EmptyRange,
    /// Go `(result, false, nil)`.
    Built(IndexJoinPathCore),
}

/// Go `indexJoinPathBuild` (`index_join_path.go:166`).
pub(crate) fn index_join_path_build(
    info: IndexJoinPathInfo<'_>,
    idx_cols: &[Column],
    idx_col_lens: &[i64],
    env: IndexJoinRangeEnv<'_>,
) -> Result<IndexJoinPathOutcome, PlanError> {
    if idx_cols.is_empty() {
        return Ok(IndexJoinPathOutcome::NotApplicable);
    }
    let mut tmp = index_join_path_tmp_init(info, idx_cols, idx_col_lens);
    let extraction = crate::ranger::detacher::extract_eq_and_in_condition_in(
        info.inner_pushed_conditions,
        &tmp.cur_not_used_index_cols,
        &tmp.cur_not_used_col_lens,
        true,
        env.evaluate,
    );
    if let (true, Some(marker)) = (extraction.parameters_overwritten, env.plan_cache_marker) {
        marker.set_skip_plan_cache("some parameters may be overwritten");
    }
    if extraction.empty_range {
        return Ok(IndexJoinPathOutcome::EmptyRange);
    }
    let not_key_eq_and_in = extraction.accesses;
    let mut remained = extraction.filters;
    let range_filter_candidates = extraction
        .new_conditions
        .into_iter()
        .map(std::borrow::Cow::into_owned)
        .collect::<Vec<_>>();
    let (not_key_eq_and_in, remained_eq_and_in) =
        index_join_path_remove_useless_eq_in(&mut tmp, idx_cols, not_key_eq_and_in);
    let matched_key_cnt = tmp.cur_possible_used_keys.len();
    // "If no join key is matched while join keys actually are not empty. We
    // don't choose index join for now."
    if matched_key_cnt == 0 && !info.inner_join_keys.is_empty() {
        return Ok(IndexJoinPathOutcome::NotApplicable);
    }
    let mut accesses = not_key_eq_and_in.clone();
    remained = crate::ranger::detacher::append_conditions_if_not_exist(remained, &remained_eq_and_in);
    let mut last_col_pos = matched_key_cnt + not_key_eq_and_in.len();
    if last_col_pos == 0 {
        return Ok(IndexJoinPathOutcome::NotApplicable);
    }
    // "If all the index columns are covered by eq/in conditions, we don't
    // need to consider other conditions anymore."
    if last_col_pos == idx_cols.len() {
        if matched_key_cnt == 0 {
            return Ok(IndexJoinPathOutcome::NotApplicable);
        }
        remained.extend(range_filter_candidates);
        let tmp_range = index_join_path_build_tmp_range(
            &tmp,
            matched_key_cnt,
            &not_key_eq_and_in,
            None,
            false,
            env,
        )?;
        if tmp_range.empty_range {
            return Ok(IndexJoinPathOutcome::EmptyRange);
        }
        if tmp_range.key_cnt_in_range == 0 {
            return Ok(IndexJoinPathOutcome::NotApplicable);
        }
        (last_col_pos, accesses, remained) =
            index_join_path_update_tmp_range(&mut tmp, &tmp_range, accesses, remained);
        return Ok(IndexJoinPathOutcome::Built(index_join_path_construct_result(
            &tmp,
            tmp_range.ranges,
            accesses,
            remained,
            None,
            false,
            last_col_pos,
        )));
    }
    let last_possible_col = &idx_cols[last_col_pos];
    let (mut last_col_manager, last_col_access) = index_join_path_build_col_manager(
        info,
        last_possible_col,
        last_col_pos,
        idx_col_lens[last_col_pos],
    );
    // "If the column manager holds no expression, then we fallback to find
    // whether there're useful normal filters."
    if last_col_access.is_empty() {
        if matched_key_cnt == 0 {
            return Ok(IndexJoinPathOutcome::NotApplicable);
        }
        let (mut col_accesses, mut col_remained) = crate::ranger::detacher::detach_conds_for_column(
            &range_filter_candidates,
            last_possible_col,
            env.opt_prefix_index_single_scan,
        );
        let mut next_col_range = None;
        if !col_accesses.is_empty() {
            let field_type = last_possible_col
                .ret_type
                .clone()
                .unwrap_or_else(|| tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong));
            let built = crate::ranger::ranger::build_column_range_in(
                &col_accesses,
                &field_type,
                idx_col_lens[last_col_pos],
                env.range_max_size,
                env.evaluate,
            )
            .map_err(|error| PlanError::internal(format!("index join range: {error:?}")))?;
            let used = built.access_conds.to_vec();
            let left = built.remained_conds.to_vec();
            let ranges = built.ranges;
            if left.is_empty() {
                next_col_range = Some(ranges);
            } else {
                // Go `buildColumnRange` records the fallback itself.
                env.record_range_fallback();
                col_remained.extend(left);
            }
            col_accesses = used;
        }
        let tmp_range = index_join_path_build_tmp_range(
            &tmp,
            matched_key_cnt,
            &not_key_eq_and_in,
            next_col_range.as_ref(),
            false,
            env,
        )?;
        if tmp_range.empty_range {
            return Ok(IndexJoinPathOutcome::EmptyRange);
        }
        if tmp_range.key_cnt_in_range == 0 {
            return Ok(IndexJoinPathOutcome::NotApplicable);
        }
        (last_col_pos, accesses, remained) =
            index_join_path_update_tmp_range(&mut tmp, &tmp_range, accesses, remained);
        remained.extend(col_remained);
        let mut last_col_is_range = false;
        if tmp_range.next_col_in_range {
            if idx_col_lens[last_col_pos] != tidb_datatype::UNSPECIFIED_LENGTH {
                remained.extend(col_accesses.iter().cloned());
            }
            accesses.extend(col_accesses);
            last_col_pos += 1;
            last_col_is_range = true;
        } else {
            remained.extend(col_accesses);
        }
        return Ok(IndexJoinPathOutcome::Built(index_join_path_construct_result(
            &tmp,
            tmp_range.ranges,
            accesses,
            remained,
            None,
            last_col_is_range,
            last_col_pos,
        )));
    }
    let tmp_range = index_join_path_build_tmp_range(
        &tmp,
        matched_key_cnt,
        &not_key_eq_and_in,
        None,
        true,
        env,
    )?;
    if tmp_range.empty_range {
        return Ok(IndexJoinPathOutcome::EmptyRange);
    }
    (last_col_pos, accesses, remained) =
        index_join_path_update_tmp_range(&mut tmp, &tmp_range, accesses, remained);
    remained.extend(range_filter_candidates);
    let mut last_col_is_range = false;
    if tmp_range.extra_col_in_range {
        accesses.extend(last_col_access);
        last_col_pos += 1;
        last_col_is_range = true;
    } else {
        if tmp_range.key_cnt_in_range == 0 {
            return Ok(IndexJoinPathOutcome::NotApplicable);
        }
        last_col_manager = None;
    }
    Ok(IndexJoinPathOutcome::Built(index_join_path_construct_result(
        &tmp,
        tmp_range.ranges,
        accesses,
        remained,
        last_col_manager,
        last_col_is_range,
        last_col_pos,
    )))
}

/// Go `indexJoinPathTmpInit`: map each index column to its inner join key,
/// dropping a string key whose collation cannot serve the index's.
fn index_join_path_tmp_init(
    info: IndexJoinPathInfo<'_>,
    idx_cols: &[Column],
    col_lens: &[i64],
) -> IndexJoinPathTmp {
    let mut tmp = IndexJoinPathTmp {
        cur_idx_off2_key_off: vec![-1; idx_cols.len()],
        ..IndexJoinPathTmp::default()
    };
    for (i, idx_col) in idx_cols.iter().enumerate() {
        if let Some(key_off) = info
            .inner_join_keys
            .iter()
            .position(|key| key.unique_id == idx_col.unique_id)
        {
            tmp.cur_idx_off2_key_off[i] = key_off as i64;
            // "Don't use the join columns if their collations are unmatched
            // and the new collation is enabled."
            if let Some(outer_key) = info.outer_join_keys.get(key_off) {
                if tidb_datatype::new_collation_enabled()
                    && is_string_column(idx_col)
                    && is_string_column(outer_key)
                {
                    let compatible = tidb_expr::collation_derive::check_and_derive_collation_from_exprs(
                        "equal",
                        tidb_datatype::EvalType::Int,
                        &[Expression::Column(idx_col.clone()), Expression::Column(outer_key.clone())],
                    )
                    .is_ok_and(|derived| {
                        tidb_datatype::compatible_collate(
                            idx_col.ret_type.as_ref().map_or("", |ft| ft.collation_name()),
                            &derived.collation,
                        )
                    });
                    if !compatible {
                        tmp.cur_idx_off2_key_off[i] = -1;
                    }
                }
            }
            continue;
        }
        tmp.cur_not_used_index_cols.push(idx_col.clone());
        tmp.cur_not_used_col_lens.push(col_lens[i]);
    }
    tmp
}

fn is_string_column(column: &Column) -> bool {
    column.ret_type.as_ref().is_some_and(|ft| ft.code().is_string())
}

/// Go `indexJoinPathRemoveUselessEQIn`: an EQ/IN column after a gap that is
/// neither a key nor pinned cannot access, so it and every later key slot are
/// dropped.
fn index_join_path_remove_useless_eq_in(
    tmp: &mut IndexJoinPathTmp,
    idx_cols: &[Column],
    mut not_key_eq_and_in: Vec<Expression>,
) -> (Vec<Expression>, Vec<Expression>) {
    tmp.cur_possible_used_keys = Vec::with_capacity(idx_cols.len());
    let mut not_key_col_pos = 0;
    for idx_col_pos in 0..idx_cols.len() {
        if tmp.cur_idx_off2_key_off[idx_col_pos] != -1 {
            tmp.cur_possible_used_keys.push(idx_cols[idx_col_pos].clone());
            continue;
        }
        if not_key_col_pos < not_key_eq_and_in.len()
            && tmp.cur_not_used_index_cols[not_key_col_pos].unique_id
                == idx_cols[idx_col_pos].unique_id
        {
            not_key_col_pos += 1;
            continue;
        }
        for off in &mut tmp.cur_idx_off2_key_off[idx_col_pos + 1..] {
            *off = -1;
        }
        let remained = not_key_eq_and_in.split_off(not_key_col_pos);
        return (not_key_eq_and_in, remained);
    }
    (not_key_eq_and_in, Vec::new())
}

/// Go `indexJoinPathBuildColManager`: the join's other conditions that
/// compare the next index column with an outer-only expression, normalized
/// to `col op arg`.
fn index_join_path_build_col_manager(
    info: IndexJoinPathInfo<'_>,
    next_col: &Column,
    next_col_offset: usize,
    col_length: i64,
) -> (Option<IndexJoinCompareFilters>, Vec<Expression>) {
    let mut ops = Vec::new();
    let mut args = Vec::new();
    let mut last_col_accesses = Vec::new();
    for filter in info.other_conditions {
        let Expression::ScalarFunction(function) = filter else {
            continue;
        };
        let name = function.func_name.lowercase();
        if !matches!(name, "le" | "lt" | "ge" | "gt") {
            continue;
        }
        let [left, right] = function.args.as_slice() else {
            continue;
        };
        let (op, another_arg) = if left
            .as_column()
            .is_some_and(|column| column.unique_id == next_col.unique_id)
        {
            (compare_op(name), right)
        } else if right
            .as_column()
            .is_some_and(|column| column.unique_id == next_col.unique_id)
        {
            // "The column manager always build expression in the form of col
            // op arg1. So we need use the symmetric one of the current
            // function."
            (compare_op(symmetric_op(name)), left)
        } else {
            continue;
        };
        let affected = tidb_expr::simple_expr::extract_columns(another_arg);
        if affected.is_empty() || affected.iter().any(|column| info.inner_schema.contains(column)) {
            continue;
        }
        last_col_accesses.push(filter.clone());
        ops.push(op);
        args.push(another_arg.clone());
    }
    let manager = (!ops.is_empty()).then(|| IndexJoinCompareFilters {
        target_col: next_col.clone(),
        target_index_offset: next_col_offset,
        col_length,
        ops,
        args,
    });
    (manager, last_col_accesses)
}

fn compare_op(name: &str) -> IndexJoinCompareOp {
    match name {
        "ge" => IndexJoinCompareOp::Ge,
        "gt" => IndexJoinCompareOp::Gt,
        "lt" => IndexJoinCompareOp::Lt,
        _ => IndexJoinCompareOp::Le,
    }
}

/// Go `symmetricOp`.
fn symmetric_op(name: &str) -> &'static str {
    match name {
        "lt" => "gt",
        "ge" => "le",
        "gt" => "lt",
        _ => "ge",
    }
}

/// Go `indexJoinPathBuildTmpRange`: the template ranges over the key and
/// EQ/IN prefix, extended by a static next-column range or an empty
/// comparison-range slot, cut short where the range memory limit is hit.
fn index_join_path_build_tmp_range(
    tmp: &IndexJoinPathTmp,
    matched_key_cnt: usize,
    eq_and_in_funcs: &[Expression],
    next_col_range: Option<&Ranges>,
    have_extra_col: bool,
    env: IndexJoinRangeEnv<'_>,
) -> Result<IndexJoinTmpRange, PlanError> {
    let point_length = matched_key_cnt + eq_and_in_funcs.len();
    let mut ranges: Ranges = vec![Range::default()];
    let (mut i, mut j) = (0, 0);
    while i + j < point_length {
        if tmp.cur_idx_off2_key_off[i + j] != -1 {
            // "This position is occupied by join key."
            let fallback;
            (ranges, fallback) = append_tail_template_range(ranges, env.range_max_size);
            if fallback {
                env.record_range_fallback();
                return Ok(IndexJoinTmpRange {
                    ranges,
                    key_cnt_in_range: i,
                    eq_and_in_cnt_in_range: j,
                    ..IndexJoinTmpRange::default()
                });
            }
            i += 1;
        } else {
            let exprs = std::slice::from_ref(&eq_and_in_funcs[j]);
            let field_type = tmp.cur_not_used_index_cols[j]
                .ret_type
                .clone()
                .unwrap_or_else(|| tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong));
            let built = crate::ranger::ranger::build_column_range_in(
                exprs,
                &field_type,
                tmp.cur_not_used_col_lens[j],
                env.range_max_size,
                env.evaluate,
            )
            .map_err(|error| PlanError::internal(format!("index join range: {error:?}")))?;
            if built.ranges.is_empty() {
                return Ok(IndexJoinTmpRange {
                    empty_range: true,
                    ..IndexJoinTmpRange::default()
                });
            }
            if !built.remained_conds.is_empty() {
                // Go `buildColumnRange` records the fallback itself.
                env.record_range_fallback();
                return Ok(IndexJoinTmpRange {
                    ranges,
                    key_cnt_in_range: i,
                    eq_and_in_cnt_in_range: j,
                    ..IndexJoinTmpRange::default()
                });
            }
            let fallback;
            (ranges, fallback) = crate::ranger::ranger::append_ranges_to_point_ranges(
                ranges,
                &built.ranges,
                env.range_max_size,
            );
            if fallback {
                env.record_range_fallback();
                return Ok(IndexJoinTmpRange {
                    ranges,
                    key_cnt_in_range: i,
                    eq_and_in_cnt_in_range: j,
                    ..IndexJoinTmpRange::default()
                });
            }
            j += 1;
        }
    }
    if let Some(next_col_range) = next_col_range.filter(|ranges| !ranges.is_empty()) {
        let fallback;
        (ranges, fallback) = crate::ranger::ranger::append_ranges_to_point_ranges(
            ranges,
            next_col_range,
            env.range_max_size,
        );
        if fallback {
            env.record_range_fallback();
        }
        return Ok(IndexJoinTmpRange {
            ranges,
            key_cnt_in_range: matched_key_cnt,
            eq_and_in_cnt_in_range: eq_and_in_funcs.len(),
            next_col_in_range: !fallback,
            ..IndexJoinTmpRange::default()
        });
    }
    if have_extra_col {
        let fallback;
        (ranges, fallback) = append_tail_template_range(ranges, env.range_max_size);
        if fallback {
            env.record_range_fallback();
        }
        return Ok(IndexJoinTmpRange {
            ranges,
            key_cnt_in_range: matched_key_cnt,
            eq_and_in_cnt_in_range: eq_and_in_funcs.len(),
            extra_col_in_range: !fallback,
            ..IndexJoinTmpRange::default()
        });
    }
    Ok(IndexJoinTmpRange {
        ranges,
        key_cnt_in_range: matched_key_cnt,
        eq_and_in_cnt_in_range: eq_and_in_funcs.len(),
        ..IndexJoinTmpRange::default()
    })
}

/// Go `appendTailTemplateRange`: an empty datum appended to every range,
/// refused when the estimate exceeds `range_max_size`.
fn append_tail_template_range(mut ranges: Ranges, range_max_size: i64) -> (Ranges, bool) {
    if range_max_size > 0
        && crate::ranger::types::ranges_mem_usage(&ranges)
            + (crate::ranger::types::EMPTY_DATUM_SIZE * 2 + 16) * ranges.len() as i64
            > range_max_size
    {
        return (ranges, true);
    }
    for range in &mut ranges {
        range.low_val.push(tidb_datatype::Datum::Null);
        range.high_val.push(tidb_datatype::Datum::Null);
        range.collators.push(tidb_datatype::Collation::Binary);
    }
    (ranges, false)
}

/// Go `indexJoinPathUpdateTmpRange`: truncate the keys and accesses to what
/// the template ranges actually cover.
fn index_join_path_update_tmp_range(
    tmp: &mut IndexJoinPathTmp,
    tmp_range: &IndexJoinTmpRange,
    mut accesses: Vec<Expression>,
    remained: Vec<Expression>,
) -> (usize, Vec<Expression>, Vec<Expression>) {
    let last_col_pos = tmp_range.key_cnt_in_range + tmp_range.eq_and_in_cnt_in_range;
    tmp.cur_possible_used_keys.truncate(tmp_range.key_cnt_in_range);
    for off in tmp.cur_idx_off2_key_off.iter_mut().skip(last_col_pos) {
        *off = -1;
    }
    let dropped = accesses.split_off(tmp_range.eq_and_in_cnt_in_range.min(accesses.len()));
    let remained = crate::ranger::detacher::append_conditions_if_not_exist(remained, &dropped);
    (last_col_pos, accesses, remained)
}

/// Go `indexJoinPathConstructResult`, minus the candidate and NDV the caller
/// derives from the DataSource.
fn index_join_path_construct_result(
    tmp: &IndexJoinPathTmp,
    ranges: Ranges,
    accesses: Vec<Expression>,
    remained: Vec<Expression>,
    last_col_manager: Option<IndexJoinCompareFilters>,
    last_col_is_range: bool,
    used_cols_len: usize,
) -> IndexJoinPathCore {
    IndexJoinPathCore {
        used_cols_len: ranges.first().map_or(0, |range| range.low_val.len()),
        ranges,
        chosen_access: accesses,
        chosen_remained: remained,
        index_join_cols: used_cols_len,
        eq_used_cols_len: used_cols_len - usize::from(last_col_is_range),
        idx_off2_key_off: tmp.cur_idx_off2_key_off.clone(),
        last_col_manager,
    }
}

/// Go `indexJoinPathGetRangeInfoAndMaxOneRow`'s second answer: a unique
/// path whose every column the template uses, with no trailing range
/// access, reads at most one row per probe.
pub(crate) fn index_join_path_max_one_row(
    core: &IndexJoinPathCore,
    unique: bool,
    full_idx_col_count: usize,
) -> bool {
    if !unique || core.used_cols_len != full_idx_col_count {
        return false;
    }
    match core.chosen_access.last() {
        None => true,
        Some(Expression::ScalarFunction(function)) => function.func_name.lowercase() == "eq",
        Some(_) => false,
    }
}

/// The inputs `mutableIndexJoinRange.Rebuild` reruns `indexJoinPathBuild`
/// with when a cached plan is reused with new parameters.
#[derive(Clone, Debug)]
pub struct IndexJoinPathRebuild {
    pub(crate) idx_cols: Vec<Column>,
    pub(crate) idx_col_lens: Vec<i64>,
    pub(crate) pushed_conditions: Vec<Expression>,
    pub(crate) other_conditions: Vec<Expression>,
    pub(crate) outer_join_keys: Vec<Column>,
    pub(crate) inner_join_keys: Vec<Column>,
    pub(crate) inner_schema: Schema,
    pub(crate) opt_prefix_index_single_scan: bool,
}

impl IndexJoinPathRebuild {
    pub(crate) fn new(
        info: IndexJoinPathInfo<'_>,
        idx_cols: &[Column],
        idx_col_lens: &[i64],
        opt_prefix_index_single_scan: bool,
    ) -> Self {
        Self {
            idx_cols: idx_cols.to_vec(),
            idx_col_lens: idx_col_lens.to_vec(),
            pushed_conditions: info.inner_pushed_conditions.to_vec(),
            other_conditions: info.other_conditions.to_vec(),
            outer_join_keys: info.outer_join_keys.to_vec(),
            inner_join_keys: info.inner_join_keys.to_vec(),
            inner_schema: info.inner_schema.clone(),
            opt_prefix_index_single_scan,
        }
    }

    /// The conditions whose parameters a reuse binds.
    pub(crate) fn conditions_mut(&mut self) -> [&mut Vec<Expression>; 2] {
        [&mut self.pushed_conditions, &mut self.other_conditions]
    }

    /// Go `mutableIndexJoinRange.Rebuild`: rerun the build in rebuild mode.
    /// `None` is Go's refusal: the path no longer builds, or its ranges are
    /// empty.
    pub(crate) fn rebuild(
        &self,
        evaluate: &ExpressionEvaluator<'_>,
    ) -> Result<(Option<Ranges>, Option<String>), PlanError> {
        /// Go's `SetSkipPlanCache` during the rebuild, which ends the hit.
        #[derive(Default)]
        struct Recorded(std::cell::RefCell<Option<String>>);
        impl crate::logical::rule::PlanCacheMarker for Recorded {
            fn set_skip_plan_cache(&self, reason: &str) {
                self.0.borrow_mut().get_or_insert_with(|| reason.to_owned());
            }
        }
        let recorded = Recorded::default();
        let info = IndexJoinPathInfo {
            other_conditions: &self.other_conditions,
            outer_join_keys: &self.outer_join_keys,
            inner_join_keys: &self.inner_join_keys,
            inner_pushed_conditions: &self.pushed_conditions,
            inner_schema: &self.inner_schema,
        };
        let env = IndexJoinRangeEnv {
            range_max_size: 0,
            fallback_handler: None,
            opt_prefix_index_single_scan: self.opt_prefix_index_single_scan,
            evaluate,
            plan_cache_marker: Some(&recorded),
        };
        let ranges = match index_join_path_build(info, &self.idx_cols, &self.idx_col_lens, env)? {
            IndexJoinPathOutcome::Built(core) if !core.ranges.is_empty() => Some(core.ranges),
            _ => None,
        };
        Ok((ranges, recorded.0.into_inner()))
    }
}

#[cfg(test)]
mod tests {
    //! Ports of Go `TestIndexJoinAnalyzeLookUpFilters` and
    //! `TestRangeFallbackForAnalyzeLookUpFilters`
    //! (`exhaust_physical_plans_test.go`).

    use super::*;
    use std::sync::Arc;
    use tidb_datatype::{
        FieldName, FieldNameMetadata, FieldType, FieldTypeCode, IdentifierMetadata,
        UNSPECIFIED_LENGTH,
    };
    use tidb_util::context::{PlanCacheTracker, RangeFallbackHandler, StaticWarnHandler, WarnHandler};

    struct Fixture {
        ds_schema: Schema,
        ds_names: Vec<FieldName>,
        join_schema: Schema,
        join_names: Vec<FieldName>,
        idx_col_lens: Vec<i64>,
    }

    fn name(table: &str, column: &str) -> FieldName {
        FieldName::new(FieldNameMetadata {
            database: IdentifierMetadata::new("test"),
            table: IdentifierMetadata::new(table),
            column: IdentifierMetadata::new(column),
            ..FieldNameMetadata::default()
        })
    }

    /// Go `prepareForAnalyzeLookUpFilters`.
    fn fixture() -> Fixture {
        let long = || FieldType::new(FieldTypeCode::LongLong);
        let varchar = |collation: &str| {
            FieldType::new(FieldTypeCode::Varchar)
                .with_collation_name(collation)
                .with_flen(UNSPECIFIED_LENGTH)
        };
        let ds = [
            ("a", long()),
            ("b", long()),
            ("c", varchar("utf8mb4_bin")),
            ("d", long()),
            ("c_ascii", varchar("ascii_bin")),
        ];
        let outer = [("e", long()), ("f", long()), ("g", varchar("utf8mb4_bin")), ("h", long())];
        let mut id = 0;
        let mut columns = |items: &[(&str, FieldType)], table: &str| {
            items
                .iter()
                .map(|(column, ty)| {
                    id += 1;
                    (Column::new(id, ty.clone()), name(table, column))
                })
                .unzip::<_, _, Vec<_>, Vec<_>>()
        };
        let (ds_columns, ds_names) = columns(&ds, "t");
        let (outer_columns, outer_names) = columns(&outer, "t1");
        let mut join_columns = ds_columns.clone();
        join_columns.extend(outer_columns);
        let mut join_names = ds_names.clone();
        join_names.extend(outer_names);
        Fixture {
            ds_schema: Schema::new(ds_columns),
            ds_names,
            join_schema: Schema::new(join_columns),
            join_names,
            idx_col_lens: vec![UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH, 2, UNSPECIFIED_LENGTH, 2],
        }
    }

    /// Go `rewriteSimpleExpr`: one expression, flattened when it is an AND.
    fn rewrite(text: &str, schema: &Schema, names: &[FieldName]) -> Vec<Expression> {
        if text.is_empty() {
            return Vec::new();
        }
        let options = tidb_expr::simple_expr::BuildOptions::new()
            .with_input_schema_and_names(schema.clone(), names.to_vec());
        let expr = tidb_expr::simple_expr::parse_simple_expr(
            &tidb_expr::rewriter::NoResolver,
            text,
            &options,
        )
        .expect("builds");
        match &expr {
            Expression::ScalarFunction(function) if function.func_name.lowercase() == "and" => {
                tidb_expr::expr_util::normal_form::flatten_cnf_conditions(function)
            }
            _ => vec![expr],
        }
    }

    /// Go `StringWithCtx` for the shapes these cases build.
    fn render(expr: &Expression) -> String {
        match expr {
            Expression::Column(column) => format!("Column#{}", column.unique_id),
            Expression::CorrelatedColumn(column) => format!("Column#{}", column.column.unique_id),
            Expression::Constant(constant) => match &constant.value {
                tidb_datatype::Datum::Int(value) => value.to_string(),
                tidb_datatype::Datum::UInt(value) => value.to_string(),
                other => String::from_utf8_lossy(&other.go_bytes()).into_owned(),
            },
            Expression::ScalarFunction(function) => format!(
                "{}({})",
                function.func_name.lowercase(),
                function.args.iter().map(render).collect::<Vec<_>>().join(", ")
            ),
        }
    }

    fn render_list(exprs: &[Expression]) -> String {
        format!("[{}]", exprs.iter().map(render).collect::<Vec<_>>().join(" "))
    }

    fn render_ranges(ranges: &[Range]) -> String {
        format!(
            "[{}]",
            ranges.iter().map(Range::to_display_string).collect::<Vec<_>>().join(" ")
        )
    }

    fn render_manager(manager: Option<&IndexJoinCompareFilters>) -> String {
        manager.map_or_else(
            || "<nil>".to_owned(),
            |manager| {
                manager
                    .ops
                    .iter()
                    .zip(&manager.args)
                    .map(|(op, arg)| {
                        format!(
                            "{}(Column#{}, {})",
                            op.go_name(),
                            manager.target_col.unique_id,
                            render(arg)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            },
        )
    }

    struct Case<'a> {
        inner_keys: &'a [usize],
        pushed: &'a str,
        others: &'a str,
    }

    struct Output {
        ranges: String,
        idx_off2_key_off: String,
        accesses: String,
        remained: String,
        compare_filters: String,
    }

    /// Go `testAnalyzeLookUpFilters`: `None` result fields print as Go's zero
    /// `indexJoinPathResult`.
    fn analyze(
        fixture: &Fixture,
        case: &Case<'_>,
        range_max_size: i64,
        handler: Option<&RangeFallbackHandler>,
    ) -> (Output, Ranges) {
        let pushed = rewrite(case.pushed, &fixture.ds_schema, &fixture.ds_names);
        let others = rewrite(case.others, &fixture.join_schema, &fixture.join_names);
        let keys = case
            .inner_keys
            .iter()
            .map(|offset| fixture.ds_schema.columns[*offset].clone())
            .collect::<Vec<_>>();
        let info = IndexJoinPathInfo {
            other_conditions: &others,
            outer_join_keys: &keys,
            inner_join_keys: &keys,
            inner_pushed_conditions: &pushed,
            inner_schema: &fixture.ds_schema,
        };
        let env = IndexJoinRangeEnv {
            range_max_size,
            fallback_handler: handler,
            opt_prefix_index_single_scan: true,
            evaluate: &crate::ranger::points::evaluate_static,
            plan_cache_marker: None,
        };
        let outcome = index_join_path_build(
            info,
            &fixture.ds_schema.columns,
            &fixture.idx_col_lens,
            env,
        )
        .expect("builds");
        match outcome {
            IndexJoinPathOutcome::Built(core) => (
                Output {
                    ranges: render_ranges(&core.ranges),
                    idx_off2_key_off: format!(
                        "[{}]",
                        core.idx_off2_key_off
                            .iter()
                            .map(i64::to_string)
                            .collect::<Vec<_>>()
                            .join(" ")
                    ),
                    accesses: render_list(&core.chosen_access),
                    remained: render_list(&core.chosen_remained),
                    compare_filters: render_manager(core.last_col_manager.as_ref()),
                },
                core.ranges,
            ),
            _ => (
                Output {
                    ranges: "[]".to_owned(),
                    idx_off2_key_off: "[]".to_owned(),
                    accesses: "[]".to_owned(),
                    remained: "[]".to_owned(),
                    compare_filters: "<nil>".to_owned(),
                },
                Vec::new(),
            ),
        }
    }

    fn assert_output(actual: &Output, expected: [&str; 5], message: &str) {
        assert_eq!(actual.ranges, expected[0], "ranges: {message}");
        assert_eq!(actual.idx_off2_key_off, expected[1], "idxOff2KeyOff: {message}");
        assert_eq!(actual.accesses, expected[2], "accesses: {message}");
        assert_eq!(actual.remained, expected[3], "remained: {message}");
        assert_eq!(actual.compare_filters, expected[4], "compareFilters: {message}");
    }

    #[test]
    fn index_join_analyze_look_up_filters() {
        let fixture = fixture();
        let cases: &[(Case<'_>, [&str; 5])] = &[
            // Join key not continuous and no pushed filter to match.
            (
                Case { inner_keys: &[0, 2], pushed: "", others: "" },
                ["[[NULL,NULL]]", "[0 -1 -1 -1 -1]", "[]", "[]", "<nil>"],
            ),
            // Join key and pushed eq filter not continuous.
            (
                Case { inner_keys: &[2], pushed: "a = 1", others: "" },
                ["[]", "[]", "[]", "[]", "<nil>"],
            ),
            // Keys are continuous.
            (
                Case { inner_keys: &[1], pushed: "a = 1", others: "" },
                ["[[1 NULL,1 NULL]]", "[-1 0 -1 -1 -1]", "[eq(Column#1, 1)]", "[]", "<nil>"],
            ),
            // Keys are continuous and there're correlated filters.
            (
                Case { inner_keys: &[1], pushed: "a = 1", others: "c > g and c < concat(g, \"ab\")" },
                [
                    "[[1 NULL NULL,1 NULL NULL]]",
                    "[-1 0 -1 -1 -1]",
                    "[eq(Column#1, 1) gt(Column#3, Column#8) lt(Column#3, concat(Column#8, ab))]",
                    "[]",
                    "gt(Column#3, Column#8) lt(Column#3, concat(Column#8, ab))",
                ],
            ),
            // cast function won't be involved.
            (
                Case { inner_keys: &[1], pushed: "a = 1", others: "c > g and c < g + 10" },
                [
                    "[[1 NULL NULL,1 NULL NULL]]",
                    "[-1 0 -1 -1 -1]",
                    "[eq(Column#1, 1) gt(Column#3, Column#8)]",
                    "[]",
                    "gt(Column#3, Column#8)",
                ],
            ),
            // Can deal with prefix index correctly.
            (
                Case { inner_keys: &[1], pushed: "a = 1 and c > 'a' and c < 'aaaaaa'", others: "" },
                [
                    "[(1 NULL \"a\",1 NULL \"aa\"]]",
                    "[-1 0 -1 -1 -1]",
                    "[eq(Column#1, 1) gt(Column#3, a) lt(Column#3, aaaaaa)]",
                    "[gt(Column#3, a) lt(Column#3, aaaaaa)]",
                    "<nil>",
                ],
            ),
            (
                Case {
                    inner_keys: &[1, 2, 3],
                    pushed: "a = 1 and c_ascii > 'a' and c_ascii < 'aaaaaa'",
                    others: "",
                },
                [
                    "[(1 NULL NULL NULL \"a\",1 NULL NULL NULL \"aa\"]]",
                    "[-1 0 1 2 -1]",
                    "[eq(Column#1, 1) gt(Column#5, a) lt(Column#5, aaaaaa)]",
                    "[gt(Column#5, a) lt(Column#5, aaaaaa)]",
                    "<nil>",
                ],
            ),
            // Can generate correct ranges for in functions.
            (
                Case { inner_keys: &[1], pushed: "a in (1, 2, 3) and c in ('a', 'b', 'c')", others: "" },
                [
                    "[[1 NULL \"a\",1 NULL \"a\"] [1 NULL \"b\",1 NULL \"b\"] [1 NULL \"c\",1 NULL \"c\"] [2 NULL \"a\",2 NULL \"a\"] [2 NULL \"b\",2 NULL \"b\"] [2 NULL \"c\",2 NULL \"c\"] [3 NULL \"a\",3 NULL \"a\"] [3 NULL \"b\",3 NULL \"b\"] [3 NULL \"c\",3 NULL \"c\"]]",
                    "[-1 0 -1 -1 -1]",
                    "[in(Column#1, 1, 2, 3) in(Column#3, a, b, c)]",
                    "[in(Column#3, a, b, c)]",
                    "<nil>",
                ],
            ),
            // Can generate correct ranges for in functions with correlated filters.
            (
                Case {
                    inner_keys: &[1],
                    pushed: "a in (1, 2, 3) and c in ('a', 'b', 'c')",
                    others: "d > h and d < h + 100",
                },
                [
                    "[[1 NULL \"a\" NULL,1 NULL \"a\" NULL] [1 NULL \"b\" NULL,1 NULL \"b\" NULL] [1 NULL \"c\" NULL,1 NULL \"c\" NULL] [2 NULL \"a\" NULL,2 NULL \"a\" NULL] [2 NULL \"b\" NULL,2 NULL \"b\" NULL] [2 NULL \"c\" NULL,2 NULL \"c\" NULL] [3 NULL \"a\" NULL,3 NULL \"a\" NULL] [3 NULL \"b\" NULL,3 NULL \"b\" NULL] [3 NULL \"c\" NULL,3 NULL \"c\" NULL]]",
                    "[-1 0 -1 -1 -1]",
                    "[in(Column#1, 1, 2, 3) in(Column#3, a, b, c) gt(Column#4, Column#9) lt(Column#4, plus(Column#9, 100))]",
                    "[in(Column#3, a, b, c)]",
                    "gt(Column#4, Column#9) lt(Column#4, plus(Column#9, 100))",
                ],
            ),
            // Join keys are not continuous and the pushed key connect the key
            // but not eq/in functions.
            (
                Case { inner_keys: &[0, 2], pushed: "b > 1", others: "" },
                ["[(NULL 1,NULL +inf]]", "[0 -1 -1 -1 -1]", "[gt(Column#2, 1)]", "[]", "<nil>"],
            ),
            (
                Case { inner_keys: &[1], pushed: "a = 1 and c > 'a' and c < '一二三'", others: "" },
                [
                    "[(1 NULL \"a\",1 NULL \"一二\"]]",
                    "[-1 0 -1 -1 -1]",
                    "[eq(Column#1, 1) gt(Column#3, a) lt(Column#3, 一二三)]",
                    "[gt(Column#3, a) lt(Column#3, 一二三)]",
                    "<nil>",
                ],
            ),
        ];
        for (i, (case, expected)) in cases.iter().enumerate() {
            let (output, _) = analyze(&fixture, case, 0, None);
            assert_output(&output, *expected, &format!("test case: {i}"));
        }
    }

    #[test]
    fn range_fallback_for_analyze_look_up_filters() {
        let fixture = fixture();
        let cases: &[(Case<'_>, &[[&str; 5]])] = &[
            (
                Case { inner_keys: &[1, 3], pushed: "a in (1, 3) and c in ('aaa', 'bbb')", others: "" },
                &[
                    [
                        "[[1 NULL \"aa\" NULL,1 NULL \"aa\" NULL] [1 NULL \"bb\" NULL,1 NULL \"bb\" NULL] [3 NULL \"aa\" NULL,3 NULL \"aa\" NULL] [3 NULL \"bb\" NULL,3 NULL \"bb\" NULL]]",
                        "[-1 0 -1 1 -1]",
                        "[in(Column#1, 1, 3) in(Column#3, aaa, bbb)]",
                        "[in(Column#3, aaa, bbb)]",
                        "<nil>",
                    ],
                    [
                        "[[1 NULL \"aa\",1 NULL \"aa\"] [1 NULL \"bb\",1 NULL \"bb\"] [3 NULL \"aa\",3 NULL \"aa\"] [3 NULL \"bb\",3 NULL \"bb\"]]",
                        "[-1 0 -1 -1 -1]",
                        "[in(Column#1, 1, 3) in(Column#3, aaa, bbb)]",
                        "[in(Column#3, aaa, bbb)]",
                        "<nil>",
                    ],
                    [
                        "[[1 NULL,1 NULL] [3 NULL,3 NULL]]",
                        "[-1 0 -1 -1 -1]",
                        "[in(Column#1, 1, 3)]",
                        "[in(Column#3, aaa, bbb)]",
                        "<nil>",
                    ],
                    ["[]", "[]", "[]", "[]", "<nil>"],
                ],
            ),
            // haveExtraCol.
            (
                Case { inner_keys: &[0], pushed: "b in (1, 3, 5)", others: "c > g and c < concat(g, 'aaa')" },
                &[
                    [
                        "[[NULL 1 NULL,NULL 1 NULL] [NULL 3 NULL,NULL 3 NULL] [NULL 5 NULL,NULL 5 NULL]]",
                        "[0 -1 -1 -1 -1]",
                        "[in(Column#2, 1, 3, 5) gt(Column#3, Column#8) lt(Column#3, concat(Column#8, aaa))]",
                        "[]",
                        "gt(Column#3, Column#8) lt(Column#3, concat(Column#8, aaa))",
                    ],
                    [
                        "[[NULL 1,NULL 1] [NULL 3,NULL 3] [NULL 5,NULL 5]]",
                        "[0 -1 -1 -1 -1]",
                        "[in(Column#2, 1, 3, 5)]",
                        "[]",
                        "<nil>",
                    ],
                    ["[[NULL,NULL]]", "[0 -1 -1 -1 -1]", "[]", "[in(Column#2, 1, 3, 5)]", "<nil>"],
                ],
            ),
            // nextColRange.
            (
                Case { inner_keys: &[1], pushed: "a in (1, 3) and c > 'aaa' and c < 'bbb'", others: "" },
                &[
                    [
                        "[[1 NULL \"aa\",1 NULL \"bb\"] [3 NULL \"aa\",3 NULL \"bb\"]]",
                        "[-1 0 -1 -1 -1]",
                        "[in(Column#1, 1, 3) gt(Column#3, aaa) lt(Column#3, bbb)]",
                        "[gt(Column#3, aaa) lt(Column#3, bbb)]",
                        "<nil>",
                    ],
                    [
                        "[[1 NULL,1 NULL] [3 NULL,3 NULL]]",
                        "[-1 0 -1 -1 -1]",
                        "[in(Column#1, 1, 3)]",
                        "[gt(Column#3, aaa) lt(Column#3, bbb)]",
                        "<nil>",
                    ],
                ],
            ),
        ];
        for (case, outputs) in cases {
            let mut range_max_size = 0;
            for (i, expected) in outputs.iter().enumerate() {
                let warnings = Arc::new(StaticWarnHandler::new(0));
                let tracker = Arc::new(PlanCacheTracker::new(warnings.clone()));
                let handler = RangeFallbackHandler::new(tracker, warnings.clone());
                let (output, ranges) = analyze(&fixture, case, range_max_size, Some(&handler));
                assert_output(&output, *expected, &format!("{} output {i}", case.pushed));
                let fell_back = warnings.copy_warnings().iter().any(|warning| {
                    warning
                        .err
                        .to_string()
                        .contains("'tidb_opt_range_max_size' exceeded when building ranges")
                });
                assert_eq!(fell_back, i > 0, "{} output {i}", case.pushed);
                range_max_size = crate::ranger::types::ranges_mem_usage(&ranges) - 1;
            }
        }

        // "building ranges doesn't have mem limit under rebuild mode"
        let pushed = rewrite("b in (1, 3) and d in (2, 4)", &fixture.ds_schema, &fixture.ds_names);
        let keys = vec![fixture.ds_schema.columns[0].clone(), fixture.ds_schema.columns[2].clone()];
        let rebuild = IndexJoinPathRebuild {
            idx_cols: fixture.ds_schema.columns.clone(),
            idx_col_lens: fixture.idx_col_lens.clone(),
            pushed_conditions: pushed,
            other_conditions: Vec::new(),
            outer_join_keys: keys.clone(),
            inner_join_keys: keys,
            inner_schema: fixture.ds_schema.clone(),
            opt_prefix_index_single_scan: true,
        };
        let (ranges, skip_plan_cache) = rebuild
            .rebuild(&crate::ranger::points::evaluate_static)
            .expect("rebuilds");
        assert_eq!(skip_plan_cache, None);
        let ranges = ranges.expect("a range");
        assert_eq!(
            render_ranges(&ranges),
            "[[NULL 1 NULL 2,NULL 1 NULL 2] [NULL 1 NULL 4,NULL 1 NULL 4] [NULL 3 NULL 2,NULL 3 NULL 2] [NULL 3 NULL 4,NULL 3 NULL 4]]"
        );
        assert!(crate::ranger::types::ranges_mem_usage(&ranges) > 1);
    }
}
