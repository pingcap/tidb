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

//! LogicalExpand identity from
//! `pkg/planner/core/operator/logicalop/logical_expand.go` and its generated
//! Hash64/Equals implementation.
//!
//! The source identity hashes the Expand plan tag, embedded output schema,
//! ordered grouping columns/expressions, DistinctSize, nested grouping sets,
//! level expressions, GID, and GPos. This leaf preserves that field order over
//! normalized column adapters; arbitrary expression variants, FieldType and
//! collation metadata, grouping-name/ID maps, plan context/schema propagation,
//! and optimizer/runtime execution remain explicit external boundaries.

use crate::hash_equaler::{new_hash_equaler, Hasher, NIL_FLAG, NOT_NIL_FLAG};

/// Normalized column identity used by LogicalExpand schema and expressions.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ExpandColumnIdentity {
    id: i64,
    unique_id: i64,
    index: i64,
    type_fingerprint: Option<u64>,
}

impl ExpandColumnIdentity {
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

/// A grouping-expression slice. `None` and `Some(Vec::new())` retain the
/// source nil-versus-empty equality distinction; both hash as length zero,
/// matching generated Go code, which hashes only `len` for nested slices.
pub type ExpandGroupingExprs = Option<Vec<ExpandColumnIdentity>>;

/// One grouping set, preserving its source nil-versus-empty state.
pub type ExpandGroupingSet = Option<Vec<ExpandGroupingExprs>>;

/// All rollup grouping sets, preserving nil-versus-empty state at every level.
pub type ExpandGroupingSets = Option<Vec<ExpandGroupingSet>>;

/// One level projection, preserving its source nil-versus-empty state.
pub type ExpandLevelExpr = Option<Vec<ExpandColumnIdentity>>;

/// All level projections, preserving nil-versus-empty state at each level.
pub type ExpandLevelExprs = Option<Vec<ExpandLevelExpr>>;

/// Minimal LogicalExpand identity and generated Hash64/Equals fields.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LogicalExpandIdentity {
    schema: Option<Vec<ExpandColumnIdentity>>,
    distinct_group_by_col: Option<Vec<ExpandColumnIdentity>>,
    distinct_gby_exprs: Option<Vec<ExpandColumnIdentity>>,
    distinct_size: i64,
    rollup_grouping_sets: ExpandGroupingSets,
    level_exprs: ExpandLevelExprs,
    gid: Option<ExpandColumnIdentity>,
    gpos: Option<ExpandColumnIdentity>,
}

impl LogicalExpandIdentity {
    /// Creates an identity from normalized LogicalExpand fields.
    // Keep the generated Go field order visible at this boundary; collapsing
    // these independent source fields into an opaque builder would hide the
    // Hash64/Equals contract this leaf is porting.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        schema: Option<Vec<ExpandColumnIdentity>>,
        distinct_group_by_col: Option<Vec<ExpandColumnIdentity>>,
        distinct_gby_exprs: Option<Vec<ExpandColumnIdentity>>,
        distinct_size: i64,
        rollup_grouping_sets: ExpandGroupingSets,
        level_exprs: ExpandLevelExprs,
        gid: Option<ExpandColumnIdentity>,
        gpos: Option<ExpandColumnIdentity>,
    ) -> Self {
        Self {
            schema,
            distinct_group_by_col,
            distinct_gby_exprs,
            distinct_size,
            rollup_grouping_sets,
            level_exprs,
            gid,
            gpos,
        }
    }

    /// Computes generated Hash64 in source field order.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = new_hash_equaler();
        hasher.hash_string("Expand");
        hash_schema(&mut hasher, self.schema.as_deref());
        hash_columns(&mut hasher, self.distinct_group_by_col.as_deref());
        hash_columns(&mut hasher, self.distinct_gby_exprs.as_deref());
        hasher.hash_int64(self.distinct_size);
        hash_rollup_grouping_sets(&mut hasher, self.rollup_grouping_sets.as_ref());
        hash_level_exprs(&mut hasher, self.level_exprs.as_ref());
        hash_column_option(&mut hasher, self.gid.as_ref());
        hash_column_option(&mut hasher, self.gpos.as_ref());
        hasher.sum64()
    }

    /// Compares generated Hash64/Equals identity fields.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

fn hash_schema(hasher: &mut impl Hasher, columns: Option<&[ExpandColumnIdentity]>) {
    match columns {
        Some(columns) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            for column in columns {
                hash_column(hasher, column);
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_columns(hasher: &mut impl Hasher, columns: Option<&[ExpandColumnIdentity]>) {
    match columns {
        Some(columns) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_int(columns.len() as i64);
            for column in columns {
                hash_column(hasher, column);
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_column_option(hasher: &mut impl Hasher, column: Option<&ExpandColumnIdentity>) {
    match column {
        Some(column) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hash_column(hasher, column);
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_column(hasher: &mut impl Hasher, column: &ExpandColumnIdentity) {
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

fn hash_rollup_grouping_sets(
    hasher: &mut impl Hasher,
    grouping_sets: Option<&Vec<ExpandGroupingSet>>,
) {
    match grouping_sets {
        Some(grouping_sets) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_int(grouping_sets.len() as i64);
            for grouping_set in grouping_sets {
                // Generated Go hashes nested nil and empty slices identically
                // by writing only len(one), while Equals distinguishes them.
                hasher.hash_int(
                    grouping_set
                        .as_ref()
                        .map_or(0, |grouping_exprs| grouping_exprs.len())
                        as i64,
                );
                if let Some(grouping_exprs) = grouping_set {
                    for grouping_expr in grouping_exprs {
                        hasher.hash_int(grouping_expr.as_ref().map_or(0, Vec::len) as i64);
                        if let Some(columns) = grouping_expr {
                            for column in columns {
                                hash_column(hasher, column);
                            }
                        }
                    }
                }
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

fn hash_level_exprs(hasher: &mut impl Hasher, level_exprs: Option<&Vec<ExpandLevelExpr>>) {
    match level_exprs {
        Some(level_exprs) => {
            hasher.hash_byte(NOT_NIL_FLAG);
            hasher.hash_int(level_exprs.len() as i64);
            for level_expr in level_exprs {
                // As with RollupGroupingSets, nested nil and empty slices hash
                // by length only and differ only in generated Equals.
                hasher.hash_int(level_expr.as_ref().map_or(0, Vec::len) as i64);
                if let Some(columns) = level_expr {
                    for column in columns {
                        hash_column(hasher, column);
                    }
                }
            }
        }
        None => hasher.hash_byte(NIL_FLAG),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ExpandColumnIdentity, ExpandGroupingSets, ExpandLevelExprs, LogicalExpandIdentity,
    };

    fn col(id: i64) -> ExpandColumnIdentity {
        // Mirrors the Go anchor's columns: ID differs, while UniqueID and
        // Index remain at their zero values.
        ExpandColumnIdentity::new(id, 0, 0)
    }

    fn base() -> LogicalExpandIdentity {
        LogicalExpandIdentity::new(
            None,
            Some(vec![col(1)]),
            Some(vec![col(1)]),
            1,
            None,
            None,
            Some(col(1)),
            Some(col(1)),
        )
    }

    fn assert_differs(first: &LogicalExpandIdentity, second: &LogicalExpandIdentity) {
        assert_ne!(first.hash64(), second.hash64());
        assert!(!first.equals(second));
    }

    #[test]
    fn source_test_matching_expand_has_equal_hash_and_identity() {
        let first = base();
        let second = base();

        assert_eq!(first.hash64(), second.hash64());
        assert!(first.equals(&second));
    }

    #[test]
    fn source_test_group_by_columns_change_hash_and_equality() {
        let first = base();
        let mut second = base();
        second.distinct_group_by_col = Some(vec![col(2)]);
        assert_differs(&first, &second);
    }

    #[test]
    fn source_test_group_by_expressions_change_hash_and_equality() {
        let first = base();
        let mut second = base();
        second.distinct_gby_exprs = Some(vec![col(2)]);
        assert_differs(&first, &second);
    }

    #[test]
    fn source_test_distinct_size_changes_hash_and_equality() {
        let first = base();
        let mut second = base();
        second.distinct_size = 2;
        assert_differs(&first, &second);
    }

    #[test]
    fn source_test_nested_grouping_sets_and_levels_change_hash_and_equality() {
        let first = base();
        let mut second = base();
        let rollup: ExpandGroupingSets = Some(vec![Some(vec![Some(vec![col(1)])])]);
        second.rollup_grouping_sets = rollup;
        assert_differs(&first, &second);

        second.rollup_grouping_sets = None;
        let levels: ExpandLevelExprs = Some(vec![Some(vec![col(1)])]);
        second.level_exprs = levels;
        assert_differs(&first, &second);
    }

    #[test]
    fn source_test_gid_and_gpos_change_hash_and_equality() {
        let first = base();
        let mut second = base();
        second.gid = Some(col(2));
        assert_differs(&first, &second);

        second.gid = Some(col(1));
        second.gpos = Some(col(2));
        assert_differs(&first, &second);
    }

    #[test]
    fn source_test_schema_type_and_nested_nil_state_are_identity_inputs() {
        let first = base();
        let mut second = base();
        second.schema = Some(vec![ExpandColumnIdentity::with_type_fingerprint(
            1, 11, 0, 7,
        )]);
        assert_differs(&first, &second);

        let mut nil_nested = base();
        nil_nested.rollup_grouping_sets = Some(vec![None]);
        let mut empty_nested = base();
        empty_nested.rollup_grouping_sets = Some(vec![Some(Vec::new())]);
        assert_ne!(nil_nested, empty_nested);
        assert_eq!(nil_nested.hash64(), empty_nested.hash64());
    }
}
