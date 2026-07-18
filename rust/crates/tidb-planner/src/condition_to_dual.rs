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

//! Constant-condition to TableDual decisions from
//! `pkg/planner/core/operator/logicalop/expression_util.go`.
//!
//! The Go helpers inspect typed expressions, statement context, and plan-cache
//! state. This leaf keeps their dependency-closed control flow over normalized
//! condition truth tokens; expression coercion, plan construction/schema, and
//! optimizer integration remain explicit external boundaries.

/// Normalized result of the source constant-expression check.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConditionTruth {
    /// The expression is not a constant.
    NonConstant,
    /// The constant evaluates to SQL NULL.
    Null,
    /// The constant converts to boolean false (including numeric zero).
    False,
    /// The constant converts to boolean true.
    True,
    /// Statement-context conversion failed.
    ConversionError,
}

/// Reports whether a normalized condition is source-constant-false.
#[must_use]
pub const fn is_const_false(condition: ConditionTruth) -> bool {
    matches!(condition, ConditionTruth::Null | ConditionTruth::False)
}

/// Applies the source `Conds2TableDual` decision.
///
/// A NULL condition has precedence over list length. The source suppresses
/// all dual construction when the plan-cache guard says the expression may be
/// over-optimized. Otherwise only a single constant-false/NULL condition can
/// produce a dual; empty or multi-condition lists remain unchanged.
#[must_use]
pub fn conds_to_table_dual(
    conditions: &[ConditionTruth],
    may_be_over_optimized_for_plan_cache: bool,
) -> bool {
    if conditions.is_empty() {
        return false;
    }

    if conditions.contains(&ConditionTruth::Null) {
        return !may_be_over_optimized_for_plan_cache;
    }

    if conditions.len() != 1 || may_be_over_optimized_for_plan_cache {
        return false;
    }

    is_const_false(conditions[0])
}

#[cfg(test)]
mod tests {
    use super::{conds_to_table_dual, is_const_false, ConditionTruth};

    #[test]
    fn null_and_false_are_const_false_but_true_is_not() {
        assert!(is_const_false(ConditionTruth::Null));
        assert!(is_const_false(ConditionTruth::False));
        assert!(!is_const_false(ConditionTruth::True));
        assert!(!is_const_false(ConditionTruth::NonConstant));
        assert!(!is_const_false(ConditionTruth::ConversionError));
    }

    #[test]
    fn single_false_or_null_builds_a_dual() {
        assert!(conds_to_table_dual(&[ConditionTruth::False], false));
        assert!(conds_to_table_dual(&[ConditionTruth::Null], false));
    }

    #[test]
    fn empty_and_multi_condition_lists_remain_unchanged() {
        assert!(!conds_to_table_dual(&[], false));
        assert!(!conds_to_table_dual(
            &[ConditionTruth::True, ConditionTruth::False],
            false
        ));
        assert!(conds_to_table_dual(
            &[ConditionTruth::Null, ConditionTruth::True],
            false
        ));
    }

    #[test]
    fn plan_cache_guard_suppresses_all_dual_construction() {
        assert!(!conds_to_table_dual(&[ConditionTruth::False], true));
        assert!(!conds_to_table_dual(&[ConditionTruth::Null], true));
        assert!(!conds_to_table_dual(
            &[ConditionTruth::Null, ConditionTruth::False],
            true
        ));
    }
}
