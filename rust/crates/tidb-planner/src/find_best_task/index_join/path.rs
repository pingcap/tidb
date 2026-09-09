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

//! Go `index_join_path.go`: index/common-handle lookup range analysis.
//! Candidate skyline/NDV selection and cached-plan ownership are separate
//! from this builder; neither is approximated by ordinary scan ranges.

use crate::{
    physical::ColWithCmpFuncManager,
    physical_property::IndexJoinRuntimeProp,
    plan_base::PlanError,
    ranger::{
        detacher::{
            append_conditions_if_not_exist, detach_conds_for_column, extract_eq_and_in_condition_in,
        },
        points::ConstantEvaluator,
        ranger::{append_ranges_to_point_ranges, build_column_range_in},
        types::{ranges_mem_usage, Range, Ranges, EMPTY_DATUM_SIZE},
    },
};
use tidb_datatype::{Collation, Datum, EvalType, FieldType};
use tidb_expr::{
    collation_derive::check_and_derive_collation_from_exprs, column::Column,
    expression::Expression, schema::Schema, simple_expr::extract_columns_from_expressions,
};

mod cached;
pub use cached::IndexJoinRangeTemplate;

/// Range-bearing portion of Go `indexJoinPathResult`. The caller must estimate
/// equality-prefix NDV from original table statistics, not filtered scan stats.
#[derive(Clone, Debug)]
pub struct IndexJoinPathRanges {
    /// Placeholder join slots and current constant ranges.
    pub ranges: Ranges,
    /// Immutable chosen-path definitions when these ranges depend on cached parameters.
    pub range_template: Option<std::sync::Arc<IndexJoinRangeTemplate>>,
    /// Chosen access predicates, including outer-dependent final bounds.
    pub accesses: Vec<Expression>,
    /// Inner predicates still needing Selection evaluation.
    pub remained: Vec<Expression>,
    /// Index-column offset to original join-key offset; -1 means not a key.
    pub index_to_key: Vec<i64>,
    /// Final-column expressions evaluated for each outer row.
    pub compare_filters: Option<ColWithCmpFuncManager>,
    /// Whether the last used column is an inequality range, not EQ/IN.
    pub last_col_is_range: bool,
}

impl IndexJoinPathRanges {
    /// Go `usedColsLen`.
    pub fn used_columns(&self) -> usize {
        self.ranges.first().map_or(0, Range::width)
    }

    /// Prefix passed to Go `EstimateColsNDVWithMatchedLen`.
    pub fn equality_columns(&self) -> usize {
        self.used_columns() - usize::from(self.last_col_is_range)
    }

    /// Go `getBestIndexJoinPathResultByProp` inverts the selected key mapping.
    pub fn key_to_index(&self, key_count: usize) -> Vec<i64> {
        let mut offsets = vec![-1; key_count];
        for (index, &key) in self.index_to_key.iter().enumerate() {
            if key >= 0 {
                offsets[key as usize] = index as i64;
            }
        }
        offsets
    }

    /// Go `indexJoinPathGetRangeInfoAndMaxOneRow`: a final IN predicate is not
    /// treated as EQ, even when its current value produces just one point.
    pub fn max_one_row(&self, unique: bool, full_index_columns: usize) -> bool {
        unique
            && self.used_columns() == full_index_columns
            && self.accesses.last().is_none_or(|expr| {
                matches!(expr, Expression::ScalarFunction(f) if f.func_name.lowercase() == "eq")
            })
    }
}

/// Borrowed source definitions plus the current ranger context. The same
/// analysis serves initial planning and parameter-dependent range rebuilding.
pub struct IndexJoinPathRangeBuilder<'a> {
    /// Go AccessPath.IdxCols, including usable common-handle columns.
    pub columns: &'a [Column],
    /// Go AccessPath.IdxColLens; -1 denotes a full column.
    pub lengths: &'a [i64],
    /// Go indexJoinPathInfo's keys and join predicates.
    pub lookup: &'a IndexJoinRuntimeProp,
    /// Inner schema, used to reject bounds depending on inner row values.
    pub inner_schema: &'a Schema,
    /// Go DataSource.PushedDownConds, before ordinary range detachment.
    pub pushed_conditions: &'a [Expression],
    /// Current statement parameter/deferred constant evaluation.
    pub eval_constant: &'a ConstantEvaluator<'a>,
    /// Session RangeMaxSize, ignored in rebuild mode.
    pub range_max_size: i64,
    /// Go StmtContext.RecordRangeFallback, including rejected paths.
    pub record_range_fallback: &'a dyn Fn(i64),
    /// Session RegardNULLAsPoint.
    pub regard_null_as_point: bool,
    /// Session OptPrefixIndexSingleScan.
    pub opt_prefix_index_single_scan: bool,
}

struct Prefix {
    index_to_key: Vec<i64>,
    non_key_columns: Vec<Column>,
    non_key_lengths: Vec<i64>,
}

struct Template {
    ranges: Ranges,
    keys: usize,
    constants: usize,
    tail: bool,
}

impl IndexJoinPathRangeBuilder<'_> {
    /// Go `indexJoinPathNewMutableRange`: retain definitions only when cache
    /// reuse can change a condition that contributed to the chosen ranges.
    pub fn build_for_plan_cache(&self) -> Result<(Option<IndexJoinPathRanges>, bool), PlanError> {
        let (mut result, empty) = self.build(false)?;
        if let Some(result) = &mut result {
            if tidb_expr::expr_util::maybe_over_optimized_4_plan_cache(true, &result.accesses) {
                result.range_template =
                    Some(std::sync::Arc::new(IndexJoinRangeTemplate::capture(self)));
            }
        }
        Ok((result, empty))
    }

    /// Go `indexJoinPathBuild`. The trailing bool distinguishes a proven
    /// empty conjunction from an index that cannot supply a lookup prefix.
    pub fn build(
        &self,
        rebuild_mode: bool,
    ) -> Result<(Option<IndexJoinPathRanges>, bool), PlanError> {
        if self.columns.is_empty() {
            return Ok((None, false));
        }
        let mut prefix = self.init_prefix()?;
        let extracted = extract_eq_and_in_condition_in(
            self.pushed_conditions,
            &prefix.non_key_columns,
            &prefix.non_key_lengths,
            self.regard_null_as_point,
            self.eval_constant,
        );
        if extracted.empty_range {
            return Ok((None, true));
        }
        let mut accesses = extracted.accesses;
        let mut remained = extracted.filters;
        let range_candidates = extracted.new_conditions;
        let (mut keys, mut constants) = (0, 0);
        // Remove every join key/static equality after the first prefix gap.
        for (index, column) in self.columns.iter().enumerate() {
            if prefix.index_to_key[index] >= 0 {
                keys += 1;
            } else if constants < accesses.len()
                && prefix.non_key_columns[constants].unique_id == column.unique_id
            {
                constants += 1;
            } else {
                prefix.index_to_key[index + 1..].fill(-1);
                remained = append_conditions_if_not_exist(remained, &accesses[constants..]);
                accesses.truncate(constants);
                break;
            }
        }
        let point_width = keys + accesses.len();
        if point_width == 0 || (keys == 0 && !self.lookup.inner_join_keys().is_empty()) {
            return Ok((None, false));
        }
        let quota = if rebuild_mode { 0 } else { self.range_max_size };
        let all_columns = point_width == self.columns.len();
        let mut manager = None;
        let mut tail_access = Vec::new();
        let mut tail_ranges = Vec::new();
        if all_columns {
            if keys == 0 {
                return Ok((None, false));
            }
            remained.extend(range_candidates);
        } else {
            let (comparisons, expressions) = self.build_col_manager(point_width);
            if !expressions.is_empty() {
                manager = Some(comparisons);
                tail_access = expressions;
                remained.extend(range_candidates);
            } else {
                if keys == 0 {
                    return Ok((None, false));
                }
                let (access, mut filters) = detach_conds_for_column(
                    &range_candidates,
                    &self.columns[point_width],
                    self.opt_prefix_index_single_scan,
                );
                if !access.is_empty() {
                    let built = build_column_range_in(
                        &access,
                        column_type(&self.columns[point_width])?,
                        self.lengths[point_width],
                        quota,
                        self.eval_constant,
                    )
                    .map_err(|err| {
                        PlanError::internal(format!("IndexJoin column range: {err:?}"))
                    })?;
                    if built.remained_conds.is_empty() {
                        tail_ranges = built.ranges;
                        tail_access = access;
                    } else {
                        (self.record_range_fallback)(quota);
                        filters.extend(access);
                    }
                }
                // Go appends column residuals after unused prefix equalities.
                // Hold them separately until quota fallback updates the prefix.
                let template =
                    self.build_template(&prefix, keys, &accesses, &tail_ranges, false, quota)?;
                let Some(template) = template else {
                    return Ok((None, true));
                };
                if template.keys == 0 {
                    return Ok((None, false));
                }
                update_prefix(&mut prefix, &template, &mut accesses, &mut remained);
                remained.extend(filters);
                if template.tail {
                    if self.lengths[template.keys + template.constants] != -1 {
                        remained.extend(tail_access.iter().cloned());
                    }
                    accesses.extend(tail_access);
                } else {
                    remained.extend(tail_access);
                }
                return Ok((
                    Some(finish(prefix, template, accesses, remained, None)),
                    false,
                ));
            }
        }
        let template = self.build_template(
            &prefix,
            keys,
            &accesses,
            &tail_ranges,
            manager.is_some(),
            quota,
        )?;
        let Some(template) = template else {
            return Ok((None, true));
        };
        if template.keys == 0 && !template.tail {
            return Ok((None, false));
        }
        update_prefix(&mut prefix, &template, &mut accesses, &mut remained);
        if template.tail {
            accesses.extend(tail_access);
        } else {
            manager = None;
        }
        Ok((
            Some(finish(prefix, template, accesses, remained, manager)),
            false,
        ))
    }

    fn init_prefix(&self) -> Result<Prefix, PlanError> {
        let mut prefix = Prefix {
            index_to_key: Vec::with_capacity(self.columns.len()),
            non_key_columns: Vec::new(),
            non_key_lengths: Vec::new(),
        };
        for (index, column) in self.columns.iter().enumerate() {
            let key = self
                .lookup
                .inner_join_keys()
                .iter()
                .position(|key| key.unique_id == column.unique_id);
            let Some(key) = key else {
                prefix.index_to_key.push(-1);
                prefix.non_key_columns.push(column.clone());
                prefix.non_key_lengths.push(self.lengths[index]);
                continue;
            };
            let outer = &self.lookup.outer_join_keys()[key];
            let mut offset = key as i64;
            if tidb_datatype::new_collation_enabled()
                && column_type(column)?.is_string()
                && column_type(outer)?.is_string()
            {
                let derived = check_and_derive_collation_from_exprs(
                    "equal",
                    EvalType::Int,
                    &[
                        Expression::Column(column.clone()),
                        Expression::Column(outer.clone()),
                    ],
                )
                .map_err(|err| PlanError::internal(format!("IndexJoin key collation: {err:?}")))?;
                if !tidb_datatype::compatible_collate(
                    column_type(column)?.collation_name(),
                    &derived.collation,
                ) {
                    offset = -1;
                }
            }
            // An incompatible join key is not reclassified as a static key.
            prefix.index_to_key.push(offset);
        }
        Ok(prefix)
    }

    fn build_col_manager(&self, position: usize) -> (ColWithCmpFuncManager, Vec<Expression>) {
        let column = &self.columns[position];
        let mut manager = ColWithCmpFuncManager {
            target_col: column.clone(),
            col_length: self.lengths[position],
            op_types: Vec::new(),
            op_args: Vec::new(),
            affected_col_schema: Schema::default(),
        };
        let mut accesses = Vec::new();
        for expression in self.lookup.other_conditions() {
            let Expression::ScalarFunction(function) = expression else {
                continue;
            };
            let op = function.func_name.lowercase();
            let symmetric = match op {
                "lt" => "gt",
                "le" => "ge",
                "gt" => "lt",
                "ge" => "le",
                _ => continue,
            };
            let [left, right] = function.args.as_slice() else {
                continue;
            };
            let (op, arg) = if matches!(left, Expression::Column(c) if c.unique_id == column.unique_id)
            {
                (op, right)
            } else if matches!(right, Expression::Column(c) if c.unique_id == column.unique_id) {
                (symmetric, left)
            } else {
                continue;
            };
            let affected = extract_columns_from_expressions(std::slice::from_ref(arg), None);
            if affected.is_empty() || affected.iter().any(|col| self.inner_schema.contains(col)) {
                continue;
            }
            manager.append_new_expr(op.to_owned(), arg.clone(), &affected);
            accesses.push(expression.clone());
        }
        (manager, accesses)
    }

    fn build_template(
        &self,
        prefix: &Prefix,
        keys: usize,
        accesses: &[Expression],
        tail_ranges: &Ranges,
        extra: bool,
        quota: i64,
    ) -> Result<Option<Template>, PlanError> {
        let mut template = Template {
            ranges: vec![Range::default()],
            keys: 0,
            constants: 0,
            tail: false,
        };
        while template.keys + template.constants < keys + accesses.len() {
            if prefix.index_to_key[template.keys + template.constants] >= 0 {
                if !append_template_slot(&mut template.ranges, quota) {
                    (self.record_range_fallback)(quota);
                    return Ok(Some(template));
                }
                template.keys += 1;
            } else {
                let index = template.constants;
                let built = build_column_range_in(
                    &accesses[index..index + 1],
                    column_type(&prefix.non_key_columns[index])?,
                    prefix.non_key_lengths[index],
                    quota,
                    self.eval_constant,
                )
                .map_err(|err| PlanError::internal(format!("IndexJoin equality range: {err:?}")))?;
                if built.ranges.is_empty() {
                    return Ok(None);
                }
                if !built.remained_conds.is_empty() {
                    (self.record_range_fallback)(quota);
                    return Ok(Some(template));
                }
                let (ranges, fallback) =
                    append_ranges_to_point_ranges(template.ranges, &built.ranges, quota);
                template.ranges = ranges;
                if fallback {
                    (self.record_range_fallback)(quota);
                    return Ok(Some(template));
                }
                template.constants += 1;
            }
        }
        if !tail_ranges.is_empty() {
            let (ranges, fallback) =
                append_ranges_to_point_ranges(template.ranges, tail_ranges, quota);
            template.ranges = ranges;
            if fallback {
                (self.record_range_fallback)(quota);
            }
            template.tail = !fallback;
        } else if extra {
            template.tail = append_template_slot(&mut template.ranges, quota);
            if !template.tail {
                (self.record_range_fallback)(quota);
            }
        }
        Ok(Some(template))
    }
}

fn column_type(column: &Column) -> Result<&FieldType, PlanError> {
    column
        .ret_type
        .as_ref()
        .ok_or_else(|| PlanError::internal("IndexJoin column type missing"))
}

fn append_template_slot(ranges: &mut Ranges, quota: i64) -> bool {
    if quota > 0
        && ranges_mem_usage(ranges) + (EMPTY_DATUM_SIZE * 2 + 16) * ranges.len() as i64 > quota
    {
        return false;
    }
    for range in ranges {
        range.low_val.push(Datum::Null);
        range.high_val.push(Datum::Null);
        // Go uses nil here: these placeholders are not compared until filled.
        range.collators.push(Collation::Binary);
    }
    true
}

fn update_prefix(
    prefix: &mut Prefix,
    template: &Template,
    accesses: &mut Vec<Expression>,
    remained: &mut Vec<Expression>,
) {
    prefix.index_to_key[template.keys + template.constants..].fill(-1);
    *remained =
        append_conditions_if_not_exist(std::mem::take(remained), &accesses[template.constants..]);
    accesses.truncate(template.constants);
}

fn finish(
    prefix: Prefix,
    template: Template,
    accesses: Vec<Expression>,
    remained: Vec<Expression>,
    compare_filters: Option<ColWithCmpFuncManager>,
) -> IndexJoinPathRanges {
    IndexJoinPathRanges {
        ranges: template.ranges,
        range_template: None,
        accesses,
        remained,
        index_to_key: prefix.index_to_key,
        compare_filters,
        last_col_is_range: template.tail,
    }
}

#[cfg(test)]
mod tests;
