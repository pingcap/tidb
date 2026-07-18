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

//! Window-frame metadata from
//! `pkg/planner/core/operator/logicalop/logical_window.go`.
//!
//! This leaf ports the handwritten FrameBound and WindowFrame Hash64/Equals
//! contracts, plus FrameBound cloning. The normalized expression adapter keeps
//! column identity and caller-supplied compare-function address tokens while
//! leaving arbitrary expression evaluation, function-pointer identity,
//! session/type context, and LogicalWindow plan execution as external
//! boundaries.

use crate::hash_equaler::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};

/// Normalized column expression identity used by frame bounds.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FrameExprIdentity {
    id: i64,
    unique_id: i64,
    index: i64,
    type_fingerprint: Option<u64>,
}

impl FrameExprIdentity {
    /// Creates a column-shaped expression identity without a type fingerprint.
    #[must_use]
    pub const fn new(id: i64, unique_id: i64, index: i64) -> Self {
        Self {
            id,
            unique_id,
            index,
            type_fingerprint: None,
        }
    }

    /// Creates a column-shaped expression identity with normalized type data.
    #[must_use]
    pub const fn with_type_fingerprint(
        id: i64,
        unique_id: i64,
        index: i64,
        type_fingerprint: u64,
    ) -> Self {
        Self {
            id,
            unique_id,
            index,
            type_fingerprint: Some(type_fingerprint),
        }
    }
}

/// A normalized caller-owned compare-function address token.
pub type CompareFunctionIdentity = String;

/// Source-shaped FrameBound identity and Hash64/Equals fields.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FrameBoundIdentity {
    frame_type: i64,
    unbounded: bool,
    num: u64,
    calc_funcs: Option<Vec<FrameExprIdentity>>,
    compare_cols: Option<Vec<FrameExprIdentity>>,
    cmp_funcs: Option<Vec<CompareFunctionIdentity>>,
    cmp_data_type: i64,
    explicit_range: bool,
}

impl FrameBoundIdentity {
    /// Creates a normalized FrameBound identity in source field order.
    // Keep the independent generated fields explicit so the handwritten
    // Hash64/Equals contract remains reviewable at the port boundary.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        frame_type: i64,
        unbounded: bool,
        num: u64,
        calc_funcs: Option<Vec<FrameExprIdentity>>,
        compare_cols: Option<Vec<FrameExprIdentity>>,
        cmp_funcs: Option<Vec<CompareFunctionIdentity>>,
        cmp_data_type: i64,
        explicit_range: bool,
    ) -> Self {
        Self {
            frame_type,
            unbounded,
            num,
            calc_funcs,
            compare_cols,
            cmp_funcs,
            cmp_data_type,
            explicit_range,
        }
    }

    /// Computes handwritten Go FrameBound Hash64 field order.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
        hash_frame_bound(&mut hasher, self);
        hasher.sum64()
    }

    /// Compares handwritten Go FrameBound Equals fields.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }

    /// Clones the identity while retaining nil-versus-empty optional slices.
    #[must_use]
    pub fn clone_identity(&self) -> Self {
        self.clone()
    }
}

/// Source-shaped WindowFrame identity and handwritten Hash64/Equals fields.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct WindowFrameIdentity {
    frame_type: i64,
    start: Option<FrameBoundIdentity>,
    end: Option<FrameBoundIdentity>,
}

impl WindowFrameIdentity {
    /// Creates a normalized WindowFrame identity.
    #[must_use]
    pub fn new(
        frame_type: i64,
        start: Option<FrameBoundIdentity>,
        end: Option<FrameBoundIdentity>,
    ) -> Self {
        Self {
            frame_type,
            start,
            end,
        }
    }

    /// Computes handwritten Go WindowFrame Hash64 field order.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_int(self.frame_type);
        if let Some(start) = &self.start {
            hasher.hash_byte(NOT_NIL_FLAG);
            hash_frame_bound(&mut hasher, start);
        } else {
            hasher.hash_byte(NIL_FLAG);
            // The Go implementation expects End to be non-nil when Start is
            // nil and hashes it without a second pointer marker. Invalid
            // nil-end input remains outside this normalized adapter boundary.
            if let Some(end) = &self.end {
                hash_frame_bound(&mut hasher, end);
            }
        }
        hasher.sum64()
    }

    /// Compares handwritten Go WindowFrame Equals fields.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

fn hash_frame_bound(hasher: &mut impl Hasher, bound: &FrameBoundIdentity) {
    hasher.hash_int(bound.frame_type);
    hasher.hash_bool(bound.unbounded);
    hasher.hash_uint64(bound.num);
    hash_exprs(hasher, bound.calc_funcs.as_deref());
    hash_exprs(hasher, bound.compare_cols.as_deref());
    hash_cmp_funcs(hasher, bound.cmp_funcs.as_deref());
    hasher.hash_int64(bound.cmp_data_type);
    hasher.hash_bool(bound.explicit_range);
}

fn hash_exprs(hasher: &mut impl Hasher, expressions: Option<&[FrameExprIdentity]>) {
    match expressions {
        Some(expressions) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_int(expressions.len() as i64);
            for expression in expressions {
                hash_expr(hasher, expression);
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_cmp_funcs(hasher: &mut impl Hasher, functions: Option<&[CompareFunctionIdentity]>) {
    match functions {
        Some(functions) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_int(functions.len() as i64);
            for function in functions {
                // Go hashes fmt.Sprintf("%p", compareFunc); callers provide
                // that stable address-shaped token at this boundary.
                hasher.hash_string(function);
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_expr(hasher: &mut impl Hasher, expression: &FrameExprIdentity) {
    match expression.type_fingerprint {
        Some(fingerprint) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_uint64(fingerprint);
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
    hasher.hash_int64(expression.id);
    hasher.hash_int64(expression.unique_id);
    hasher.hash_int(expression.index);
}

#[cfg(test)]
mod tests {
    use super::{FrameBoundIdentity, FrameExprIdentity, WindowFrameIdentity};

    fn column(index: i64, unique_id: i64) -> FrameExprIdentity {
        // The Go anchor's columns share the same type and differ by index and
        // UniqueID; ID remains zero in both values.
        FrameExprIdentity::with_type_fingerprint(0, unique_id, index, 1)
    }

    fn base_bound() -> FrameBoundIdentity {
        FrameBoundIdentity::new(
            0,
            true,
            1,
            Some(vec![column(0, 0)]),
            Some(vec![column(0, 0)]),
            Some(vec!["mock-func".to_owned()]),
            1,
            false,
        )
    }

    fn assert_differs(first: &FrameBoundIdentity, second: &FrameBoundIdentity) {
        assert_ne!(first.hash64(), second.hash64());
        assert!(!first.equals(second));
    }

    #[test]
    fn source_test_frame_bound_matching_hash_and_identity() {
        let first = base_bound();
        let second = base_bound();

        assert_eq!(first.hash64(), second.hash64());
        assert!(first.equals(&second));
    }

    #[test]
    fn source_test_frame_bound_scalar_fields_change_hash_and_equality() {
        let first = base_bound();

        let mut second = base_bound();
        second.frame_type = 1;
        assert_differs(&first, &second);

        second.frame_type = 0;
        second.unbounded = false;
        assert_differs(&first, &second);

        second.unbounded = true;
        second.num = 2;
        assert_differs(&first, &second);

        second.num = 1;
        second.cmp_data_type = 2;
        assert_differs(&first, &second);

        second.cmp_data_type = 1;
        second.explicit_range = true;
        assert_differs(&first, &second);
    }

    #[test]
    fn source_test_frame_bound_expression_lists_change_hash_and_equality() {
        let first = base_bound();
        let mut second = base_bound();

        second.calc_funcs = Some(vec![column(1, 1)]);
        assert_differs(&first, &second);

        second.calc_funcs = Some(vec![column(0, 0)]);
        second.compare_cols = Some(vec![column(1, 1)]);
        assert_differs(&first, &second);
    }

    #[test]
    fn source_test_frame_bound_compare_function_tokens_change_hash_and_equality() {
        let first = base_bound();
        let mut second = base_bound();
        second.cmp_funcs = Some(vec!["mock-func-2".to_owned()]);
        assert_differs(&first, &second);

        second.cmp_funcs = Some(vec!["mock-func".to_owned()]);
        assert_eq!(first.hash64(), second.hash64());
        assert!(first.equals(&second));
    }

    #[test]
    fn source_test_frame_bound_clone_preserves_nil_slices() {
        let original = FrameBoundIdentity::new(
            0,
            true,
            1,
            None,
            None,
            Some(vec!["mock-func".to_owned()]),
            1,
            false,
        );
        let cloned = original.clone_identity();

        assert!(cloned.calc_funcs.is_none());
        assert!(cloned.compare_cols.is_none());
        assert_eq!(original.hash64(), cloned.hash64());
        assert!(original.equals(&cloned));
    }

    #[test]
    fn source_test_window_frame_matching_and_type_change() {
        let bound = base_bound();
        let first = WindowFrameIdentity::new(1, Some(bound.clone()), Some(bound.clone()));
        let mut second = first.clone();

        assert_eq!(first.hash64(), second.hash64());
        assert!(first.equals(&second));

        second.frame_type = 2;
        assert_ne!(first.hash64(), second.hash64());
        assert!(!first.equals(&second));
    }

    #[test]
    fn source_window_frame_hashes_start_but_not_end_when_start_is_present() {
        let bound = base_bound();
        let first = WindowFrameIdentity::new(1, Some(bound.clone()), Some(bound.clone()));
        let mut different_end = first.clone();
        different_end.end.as_mut().expect("base end").num = 2;

        assert_eq!(first.hash64(), different_end.hash64());
        assert!(!first.equals(&different_end));
    }
}
