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

//! LogicalProjection identity from
//! `pkg/planner/core/operator/logicalop/logical_projection.go` and its
//! generated Hash64/Equals implementation.
//!
//! The source identity hashes the Projection plan tag, output schema, ordered
//! expression list, CalculateNoDelay, and Proj4Expand. This leaf preserves
//! that field order over normalized column adapters; arbitrary expression
//! metadata/evaluation, projection rewrites/pruning, plan context, and runtime
//! execution remain explicit external boundaries.

use crate::hash_equaler::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};

/// Normalized expression-column identity used by Projection schema/expressions.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProjectionColumnIdentity {
    id: i64,
    unique_id: i64,
    index: i64,
    type_fingerprint: Option<u64>,
}

impl ProjectionColumnIdentity {
    /// Creates a column identity without a caller-supplied type fingerprint.
    #[must_use]
    pub const fn new(id: i64, unique_id: i64, index: i64) -> Self {
        Self {
            id,
            unique_id,
            index,
            type_fingerprint: None,
        }
    }

    /// Creates a column identity with a normalized type fingerprint.
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

/// Minimal LogicalProjection identity and generated Hash64/Equals fields.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LogicalProjectionIdentity {
    schema: Option<Vec<ProjectionColumnIdentity>>,
    exprs: Option<Vec<ProjectionColumnIdentity>>,
    calculate_no_delay: bool,
    proj4_expand: bool,
}

impl LogicalProjectionIdentity {
    /// Creates an identity from source-owned schema, expressions, and flags.
    #[must_use]
    pub fn new(
        schema: Option<Vec<ProjectionColumnIdentity>>,
        exprs: Option<Vec<ProjectionColumnIdentity>>,
        calculate_no_delay: bool,
        proj4_expand: bool,
    ) -> Self {
        Self {
            schema,
            exprs,
            calculate_no_delay,
            proj4_expand,
        }
    }

    /// Returns the optional output schema.
    #[must_use]
    pub fn schema(&self) -> Option<&[ProjectionColumnIdentity]> {
        self.schema.as_deref()
    }

    /// Returns the optional ordered expression list.
    #[must_use]
    pub fn exprs(&self) -> Option<&[ProjectionColumnIdentity]> {
        self.exprs.as_deref()
    }

    /// Returns CalculateNoDelay.
    #[must_use]
    pub const fn calculate_no_delay(&self) -> bool {
        self.calculate_no_delay
    }

    /// Returns Proj4Expand.
    #[must_use]
    pub const fn proj4_expand(&self) -> bool {
        self.proj4_expand
    }

    /// Computes generated Hash64 in source field order.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_string("Projection");
        hash_columns(&mut hasher, self.schema.as_deref(), false);
        hash_columns(&mut hasher, self.exprs.as_deref(), true);
        hasher.hash_bool(self.calculate_no_delay);
        hasher.hash_bool(self.proj4_expand);
        hasher.sum64()
    }

    /// Compares generated Hash64/Equals identity fields.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

fn hash_columns(
    hasher: &mut impl Hasher,
    columns: Option<&[ProjectionColumnIdentity]>,
    include_length: bool,
) {
    match columns {
        Some(columns) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            if include_length {
                hasher.hash_int(columns.len() as i64);
            }
            for column in columns {
                hash_column(hasher, column);
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_column(hasher: &mut impl Hasher, column: &ProjectionColumnIdentity) {
    match column.type_fingerprint {
        Some(fingerprint) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_uint64(fingerprint);
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
    hasher.hash_int64(column.id);
    hasher.hash_int64(column.unique_id);
    hasher.hash_int(column.index);
}

#[cfg(test)]
mod tests {
    use super::{LogicalProjectionIdentity, ProjectionColumnIdentity};

    fn base() -> LogicalProjectionIdentity {
        LogicalProjectionIdentity::new(
            Some(vec![ProjectionColumnIdentity::new(1, 0, 0)]),
            Some(vec![ProjectionColumnIdentity::new(2, 0, 0)]),
            false,
            false,
        )
    }

    #[test]
    fn source_test_matching_projection_has_equal_hash_and_identity() {
        let first = base();
        let second = base();

        assert_eq!(first.hash64(), second.hash64());
        assert!(first.equals(&second));
    }

    #[test]
    fn source_test_empty_and_nil_exprs_are_distinct() {
        let first = base();
        let empty_exprs =
            LogicalProjectionIdentity::new(first.schema.clone(), Some(Vec::new()), false, false);
        let nil_exprs = LogicalProjectionIdentity::new(first.schema.clone(), None, false, false);

        assert_ne!(first.hash64(), empty_exprs.hash64());
        assert!(!first.equals(&empty_exprs));
        assert_ne!(first.hash64(), nil_exprs.hash64());
        assert!(!first.equals(&nil_exprs));
    }

    #[test]
    fn source_test_calculate_no_delay_changes_hash_and_equality() {
        let first = base();
        let second =
            LogicalProjectionIdentity::new(first.schema.clone(), first.exprs.clone(), true, false);

        assert_ne!(first.hash64(), second.hash64());
        assert!(!first.equals(&second));
    }

    #[test]
    fn source_test_proj4_expand_changes_hash_and_equality() {
        let first = base();
        let second =
            LogicalProjectionIdentity::new(first.schema.clone(), first.exprs.clone(), false, true);

        assert_ne!(first.hash64(), second.hash64());
        assert!(!first.equals(&second));
    }

    #[test]
    fn source_test_schema_and_expression_columns_are_identity_fields() {
        let first = base();
        let changed_schema = LogicalProjectionIdentity::new(
            Some(vec![ProjectionColumnIdentity::new(3, 0, 0)]),
            first.exprs.clone(),
            false,
            false,
        );
        let changed_expr = LogicalProjectionIdentity::new(
            first.schema.clone(),
            Some(vec![ProjectionColumnIdentity::new(4, 0, 0)]),
            false,
            false,
        );

        assert_ne!(first.hash64(), changed_schema.hash64());
        assert!(!first.equals(&changed_schema));
        assert_ne!(first.hash64(), changed_expr.hash64());
        assert!(!first.equals(&changed_expr));
    }

    #[test]
    fn source_test_normalized_type_fingerprint_is_hashed() {
        let first = LogicalProjectionIdentity::new(
            Some(vec![ProjectionColumnIdentity::with_type_fingerprint(
                1, 0, 0, 10,
            )]),
            Some(vec![ProjectionColumnIdentity::with_type_fingerprint(
                2, 0, 0, 20,
            )]),
            false,
            false,
        );
        let second = LogicalProjectionIdentity::new(
            Some(vec![ProjectionColumnIdentity::with_type_fingerprint(
                1, 0, 0, 11,
            )]),
            Some(vec![ProjectionColumnIdentity::with_type_fingerprint(
                2, 0, 0, 21,
            )]),
            false,
            false,
        );

        assert_ne!(first.hash64(), second.hash64());
        assert!(!first.equals(&second));
    }
}
