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

//! Go `util.AccessPath`'s helpers that need the operator's scan metadata:
//! `SplitCorColAccessCondFromFilters`, `OnlyPointRange`, and the resolved
//! TiKV table-scan descriptor a table reader is explained by.

use tidb_ranger::Range;

/// Go AccessPath.SplitCorColAccessCondFromFilters. Extends an equality
/// prefix through correlated equalities, optionally ending in a range.
/// The caller also records Go's correlated-subquery plan-cache exclusion.
pub fn split_correlated_access_conditions(
    filters: &[tidb_expr::expression::Expression],
    columns: &[tidb_expr::column::Column],
    lengths: &[i64],
    eq_or_in_count: usize,
) -> (
    Vec<tidb_expr::expression::Expression>,
    Vec<tidb_expr::expression::Expression>,
) {
    use tidb_expr::expression::Expression;
    fn matches(
        expr: &Expression,
        col: &tidb_expr::column::Column,
        constant: bool,
        range: bool,
    ) -> bool {
        let Expression::ScalarFunction(fun) = expr else {
            return false;
        };
        if if range {
            !["lt", "le", "gt", "ge"].contains(&fun.func_name.lowercase())
        } else {
            fun.func_name.lowercase() != "eq"
        } {
            return false;
        }
        let [left, right] = fun.args.as_slice() else {
            return false;
        };
        for (column, value) in [(left, right), (right, left)] {
            let Expression::Column(column) = column else {
                continue;
            };
            if column.ret_type.as_ref().is_some_and(|tp| {
                tp.eval_type() == tidb_datatype::EvalType::String
                    && !tidb_datatype::compatible_collate(
                        fun.collation.charset_and_collation().1,
                        tp.collation_name(),
                    )
            }) {
                return false;
            }
            if column.unique_id == col.unique_id
                && if constant {
                    matches!(value, Expression::Constant(_))
                } else {
                    matches!(value, Expression::CorrelatedColumn(_))
                }
            {
                return true;
            }
        }
        false
    }
    let mut access = Vec::new();
    let mut used = vec![false; filters.len()];
    for i in eq_or_in_count..columns.len() {
        let mut matched = false;
        for (j, filter) in filters.iter().enumerate() {
            if used[j] {
                continue;
            }
            let constant = matches(filter, &columns[i], true, false);
            if i == eq_or_in_count && constant {
                return (Vec::new(), filters.to_vec());
            }
            if constant || matches(filter, &columns[i], false, false) {
                access.push(filter.clone());
                if lengths[i] == -1 {
                    used[j] = true;
                }
                matched = true;
                break;
            }
        }
        if !matched {
            if let Some((_, filter)) = filters
                .iter()
                .enumerate()
                .find(|(j, filter)| !used[*j] && matches(filter, &columns[i], false, true))
            {
                access.push(filter.clone());
            }
            break;
        }
    }
    let remained = filters
        .iter()
        .zip(used)
        .filter_map(|(expr, used)| (!used).then(|| expr.clone()))
        .collect();
    (access, remained)
}

/// The access-path identity needed by Go `AccessPath.OnlyPointRange`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PointRangePath {
    /// A signed integer table-handle path, whose point may be SQL `NULL`.
    IntHandle,
    /// An ordinary index path, which must fully match every index column.
    Index {
        /// Number of columns in the source index definition.
        column_count: usize,
    },
}

/// Reports whether every source range is a point accepted by this path.
///
/// This is Go `AccessPath.OnlyPointRange`: integer handles use
/// `Range.IsPointNullable`, while ordinary indexes use
/// `Range.IsPointNonNullable` and additionally require every range to cover
/// all index columns.
#[must_use]
pub fn only_point_ranges(path: PointRangePath, ranges: &[Range]) -> bool {
    ranges.iter().all(|range| match path {
        PointRangePath::IntHandle => range.is_point(true),
        PointRangePath::Index { column_count } => {
            range.is_point(false) && range.high_val.len() == column_count
        }
    })
}

/// Source-resolved table-scan kind returned by Go's
/// `PhysicalTableScan.IsFullScan` decision.
///
/// Cluster-table and index-lookup child identities are outside the bounded
/// ordinary TiKV table path, so only the full/range branches are admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolvedTableScanKind {
    /// Every source range is a full range for the table handle type.
    Full,
    /// At least one source range restricts the table handle domain.
    Range,
    /// The table side of an index lookup reads rows by handles emitted by
    /// the index side. Go names this `TableRowIDScan` regardless of the
    /// source table's ordinary handle-range classification.
    RowId,
}

impl ResolvedTableScanKind {
    /// Returns the exact Go plan-codec identity for this bounded scan kind.
    #[must_use]
    pub const fn plan_type(self) -> &'static str {
        match self {
            Self::Full => "TableFullScan",
            Self::Range => "TableRangeScan",
            Self::RowId => "TableRowIDScan",
        }
    }
}

/// Source statement-context policy for `PhysicalTableScan.ExplainID`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableScanExplainIdSuffix {
    /// Normal Go explain output appends the physical plan ID.
    IncludePlanId,
    /// `StmtCtx.IgnoreExplainIDSuffix` suppresses the physical plan ID.
    Omit,
}

/// Authoritative source descriptor for one ordinary TiKV table scan.
///
/// Go derives table identity, common-handle state, range kind, and explain-ID
/// policy from the same table/range/session objects used to build the physical
/// scan. Keeping them together prevents caller-proposed protobuf metadata from
/// silently naming another table or dropping reader metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedTableDescriptor {
    table_id: i64,
    is_common_handle: bool,
    scan_kind: ResolvedTableScanKind,
    explain_id_suffix: TableScanExplainIdSuffix,
}

impl ResolvedTableDescriptor {
    /// Creates one source-resolved bounded table descriptor.
    #[must_use]
    pub const fn new(
        table_id: i64,
        is_common_handle: bool,
        scan_kind: ResolvedTableScanKind,
        explain_id_suffix: TableScanExplainIdSuffix,
    ) -> Self {
        Self {
            table_id,
            is_common_handle,
            scan_kind,
            explain_id_suffix,
        }
    }

    /// Returns the authoritative table identity.
    #[must_use]
    pub const fn table_id(self) -> i64 {
        self.table_id
    }

    /// Returns whether the source table uses a common handle.
    #[must_use]
    pub const fn is_common_handle(self) -> bool {
        self.is_common_handle
    }

    /// Returns the source full/range classification.
    #[must_use]
    pub const fn scan_kind(self) -> ResolvedTableScanKind {
        self.scan_kind
    }

    /// Returns the source explain-ID suffix policy.
    #[must_use]
    pub const fn explain_id_suffix(self) -> TableScanExplainIdSuffix {
        self.explain_id_suffix
    }
}

#[cfg(test)]
mod tests {
    use tidb_datatype::Datum;

    use super::{only_point_ranges, PointRangePath};
    use tidb_ranger::Range;

    fn range(low: Datum, high: Datum) -> Range {
        Range {
            collators: vec![tidb_charset::Collation::Binary; 1],
            low_val: vec![low],
            high_val: vec![high],
            low_exclude: false,
            high_exclude: false,
        }
    }

    #[test]
    fn test_only_point_range() {
        // pkg/planner/util/path_test.go::TestOnlyPointRange is the source of
        // truth for this table, including its asymmetric NULL treatment.
        let null_point_range = range(Datum::Null, Datum::Null);
        let one_point_range = range(Datum::new_int(1), Datum::new_int(1));
        let one_to_two_range = range(Datum::new_int(1), Datum::new_int(2));

        assert!(only_point_ranges(
            PointRangePath::IntHandle,
            &[null_point_range.clone(), one_point_range.clone()],
        ));
        assert!(!only_point_ranges(
            PointRangePath::IntHandle,
            &[one_point_range.clone(), one_to_two_range.clone()],
        ));

        let one_column_index = PointRangePath::Index { column_count: 1 };
        assert!(only_point_ranges(
            one_column_index,
            std::slice::from_ref(&one_point_range),
        ));
        assert!(!only_point_ranges(
            one_column_index,
            &[null_point_range, one_point_range.clone()],
        ));
        assert!(!only_point_ranges(
            one_column_index,
            &[one_point_range.clone(), one_to_two_range],
        ));

        assert!(!only_point_ranges(
            PointRangePath::Index { column_count: 2 },
            &[one_point_range],
        ));
    }
}

// ---------------------------------------------------------------------------
// Go `getPossibleAccessPaths` (`planbuilder.go:1320`): the ENUMERATION step.
// ---------------------------------------------------------------------------
