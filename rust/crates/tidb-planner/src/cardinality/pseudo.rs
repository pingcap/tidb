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

//! Dependency-closed pseudo cardinality arithmetic from
//! `pkg/planner/cardinality/pseudo.go`.
//!
//! The Go implementation also inspects planner context, expression columns,
//! histograms, indexes, and MySQL Datum comparison.  Those owners are not
//! available in the seed planner, so this module keeps only the deterministic
//! equality/less/between/range formulas.  Callers supply already-normalized
//! scalar bounds and prefix-equality lengths; no statistics/catalog/session
//! facade is invented here.

/// Source pseudo divisor for equality predicates and one value's average
/// frequency.
pub const PSEUDO_EQUAL_RATE: f64 = 1_000.0;
/// Source pseudo divisor for one-sided less/greater predicates.
pub const PSEUDO_LESS_RATE: f64 = 3.0;
/// Source pseudo divisor for a bounded between predicate.
pub const PSEUDO_BETWEEN_RATE: f64 = 40.0;

/// A normalized range-bound kind from TiDB's Datum/ranger domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PseudoBoundKind {
    /// The accompanying scalar value is a concrete bound.
    Value,
    /// SQL NULL lower bound.
    Null,
    /// The smallest non-NULL value marker.
    MinNotNull,
    /// The largest value marker.
    MaxValue,
}

/// A signed integer pseudo range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SignedIntRange {
    /// Concrete low value (ignored for `Null`/`MinNotNull`).
    pub low: i64,
    /// Concrete high value (ignored for `MaxValue`).
    pub high: i64,
    /// Kind of the low bound.
    pub low_kind: PseudoBoundKind,
    /// Kind of the high bound.
    pub high_kind: PseudoBoundKind,
}

impl SignedIntRange {
    /// Creates a normalized signed range.
    #[must_use]
    pub const fn new(
        low: i64,
        high: i64,
        low_kind: PseudoBoundKind,
        high_kind: PseudoBoundKind,
    ) -> Self {
        Self {
            low,
            high,
            low_kind,
            high_kind,
        }
    }
}

/// An unsigned integer pseudo range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnsignedIntRange {
    /// Concrete low value (ignored for `Null`/`MinNotNull`).
    pub low: u64,
    /// Concrete high value (ignored for `MaxValue`).
    pub high: u64,
    /// Kind of the low bound.
    pub low_kind: PseudoBoundKind,
    /// Kind of the high bound.
    pub high_kind: PseudoBoundKind,
}

impl UnsignedIntRange {
    /// Creates a normalized unsigned range.
    #[must_use]
    pub const fn new(
        low: u64,
        high: u64,
        low_kind: PseudoBoundKind,
        high_kind: PseudoBoundKind,
    ) -> Self {
        Self {
            low,
            high,
            low_kind,
            high_kind,
        }
    }
}

/// A scalar numeric pseudo range for the generic Datum-range formula.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ScalarRange {
    /// Numeric low value supplied by the caller.
    pub low: f64,
    /// Numeric high value supplied by the caller.
    pub high: f64,
    /// Kind of the low bound.
    pub low_kind: PseudoBoundKind,
    /// Kind of the high bound.
    pub high_kind: PseudoBoundKind,
}

impl ScalarRange {
    /// Creates a normalized scalar range.
    #[must_use]
    pub const fn new(
        low: f64,
        high: f64,
        low_kind: PseudoBoundKind,
        high_kind: PseudoBoundKind,
    ) -> Self {
        Self {
            low,
            high,
            low_kind,
            high_kind,
        }
    }
}

/// A composite-index pseudo range after the source has computed its equal
/// prefix length.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexRange {
    /// Per-column normalized bounds, in index order.
    pub columns: Vec<ScalarRange>,
    /// Number of leading columns equal to a concrete value.
    pub equal_prefix_len: usize,
    /// Whether the low bound is exclusive.
    pub low_exclude: bool,
    /// Whether the high bound is exclusive.
    pub high_exclude: bool,
}

impl IndexRange {
    /// Creates a source-shaped composite index range.
    #[must_use]
    pub fn new(
        columns: Vec<ScalarRange>,
        equal_prefix_len: usize,
        low_exclude: bool,
        high_exclude: bool,
    ) -> Self {
        Self {
            columns,
            equal_prefix_len,
            low_exclude,
            high_exclude,
        }
    }
}

/// Returns the pseudo average count for one equality value.
#[must_use]
pub fn pseudo_avg_count_per_value(table_row_count: f64) -> f64 {
    table_row_count / PSEUDO_EQUAL_RATE
}

/// Returns the equality estimate for a table with no histogram.
#[must_use]
pub fn pseudo_equal_count(table_row_count: f64) -> f64 {
    pseudo_avg_count_per_value(table_row_count)
}

/// Returns the one-sided less/greater estimate for a table with no histogram.
#[must_use]
pub fn pseudo_less_count(table_row_count: f64) -> f64 {
    table_row_count / PSEUDO_LESS_RATE
}

/// Returns the bounded between estimate for a table with no histogram.
#[must_use]
pub fn pseudo_between_count(table_row_count: f64) -> f64 {
    table_row_count / PSEUDO_BETWEEN_RATE
}

fn clamp_to_table(row_count: f64, table_row_count: f64) -> f64 {
    if row_count > table_row_count {
        table_row_count
    } else {
        row_count
    }
}

/// Estimates signed integer ranges using the source pseudo rates.
#[must_use]
pub fn pseudo_row_count_by_signed_int_ranges(
    ranges: &[SignedIntRange],
    table_row_count: f64,
) -> f64 {
    let mut row_count = 0.0;
    for range in ranges {
        let low = if matches!(
            range.low_kind,
            PseudoBoundKind::Null | PseudoBoundKind::MinNotNull
        ) {
            i64::MIN
        } else {
            range.low
        };
        let high = if range.high_kind == PseudoBoundKind::MaxValue {
            i64::MAX
        } else {
            range.high
        };
        let mut count = if low == i64::MIN && high == i64::MAX {
            table_row_count
        } else if low == i64::MIN || high == i64::MAX {
            pseudo_less_count(table_row_count)
        } else if low == high {
            1.0
        } else {
            pseudo_between_count(table_row_count)
        };
        let width = high.wrapping_sub(low);
        if width > 0 && count > width as f64 {
            count = width as f64;
        }
        row_count += count;
    }
    clamp_to_table(row_count, table_row_count)
}

/// Estimates unsigned integer ranges using the source pseudo rates.
#[must_use]
pub fn pseudo_row_count_by_unsigned_int_ranges(
    ranges: &[UnsignedIntRange],
    table_row_count: f64,
) -> f64 {
    let mut row_count = 0.0;
    for range in ranges {
        let low = if matches!(
            range.low_kind,
            PseudoBoundKind::Null | PseudoBoundKind::MinNotNull
        ) {
            0
        } else {
            range.low
        };
        let high = if range.high_kind == PseudoBoundKind::MaxValue {
            u64::MAX
        } else {
            range.high
        };
        let mut count = if low == 0 && high == u64::MAX {
            table_row_count
        } else if low == 0 || high == u64::MAX {
            pseudo_less_count(table_row_count)
        } else if low == high {
            1.0
        } else {
            pseudo_between_count(table_row_count)
        };
        let width = high.wrapping_sub(low);
        if width > 0 && count > width as f64 {
            count = width as f64;
        }
        row_count += count;
    }
    clamp_to_table(row_count, table_row_count)
}

/// Estimates generic normalized scalar ranges using equality/less/between
/// markers.  The caller performs any source Datum/collation comparison before
/// constructing this numeric range; no comparison/session error is invented.
#[must_use]
pub fn pseudo_row_count_by_scalar_ranges(ranges: &[ScalarRange], table_row_count: f64) -> f64 {
    let mut row_count = 0.0;
    for range in ranges {
        let count = if range.low_kind == PseudoBoundKind::Null
            && range.high_kind == PseudoBoundKind::MaxValue
        {
            table_row_count
        } else if range.low_kind == PseudoBoundKind::MinNotNull {
            let null_count = pseudo_equal_count(table_row_count);
            if range.high_kind == PseudoBoundKind::MaxValue {
                table_row_count - null_count
            } else {
                pseudo_less_count(table_row_count) - null_count
            }
        } else if range.high_kind == PseudoBoundKind::MaxValue {
            pseudo_less_count(table_row_count)
        } else if range.low == range.high {
            pseudo_equal_count(table_row_count)
        } else {
            pseudo_between_count(table_row_count)
        };
        row_count += count;
    }
    clamp_to_table(row_count, table_row_count)
}

/// Estimates composite-index ranges after the caller supplies equal-prefix
/// lengths.  `unique_columns` corresponds to Go's `colsLen`; `None` means the
/// index is not known to be unique.
#[must_use]
pub fn pseudo_row_count_by_index_ranges(
    ranges: &[IndexRange],
    table_row_count: f64,
    unique_columns: Option<usize>,
) -> f64 {
    if table_row_count == 0.0 {
        return 0.0;
    }
    let mut total_count = 0.0;
    for range in ranges {
        if range.columns.is_empty() {
            continue;
        }
        let mut prefix_len = range.equal_prefix_len;
        if unique_columns == Some(prefix_len) && !range.low_exclude && !range.high_exclude {
            total_count += 1.0;
            continue;
        }
        prefix_len = prefix_len.min(range.columns.len() - 1);
        let row_count = pseudo_row_count_by_scalar_ranges(
            &range.columns[prefix_len..=prefix_len],
            table_row_count,
        );
        let mut count = row_count;
        for _ in 0..prefix_len {
            count /= 100.0;
        }
        total_count += count;
    }
    if total_count > table_row_count {
        table_row_count / PSEUDO_LESS_RATE
    } else {
        total_count
    }
}

/// Which arm of `pseudoSelectivity`'s function-name switch a predicate takes.
///
/// Go switches on `fun.FuncName.L` at `pkg/planner/cardinality/pseudo.go:54`.
/// Only two arms exist; everything else leaves `minFactor` untouched.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PseudoFunctionKind {
    /// `ast.EQ`, `ast.NullEQ`, `ast.In`.
    Equality,
    /// `ast.GE`, `ast.GT`, `ast.LE`, `ast.LT`.
    ///
    /// The source carries a `FIXME: To resolve the between case.` here: a
    /// `BETWEEN` that ranger split into `>=` and `<=` charges `1/3` once, not
    /// the `1/40` a bounded range would deserve.
    Ordering,
    /// Any other function name.
    Other,
}

/// The column a pseudo predicate resolved to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PseudoColumn {
    /// Lowercased column name, the key of the source's `colExists` map.
    pub lower_name: String,
    /// Whether `mysql.HasUniKeyFlag(col.Info.GetFlag())` holds.
    pub unique_key_flag: bool,
}

/// One CNF item as `pseudoSelectivity` sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PseudoPredicate {
    /// Not a `*expression.ScalarFunction`, or `getConstantColumnID` returned
    /// `unknownColumnID`. The source `continue`s before the switch, so this
    /// contributes nothing at all.
    Unresolved,
    /// A scalar function over one column and one constant. `column` is `None`
    /// when `coll.GetCol(colID)` returned nil -- note the source updates
    /// `minFactor` for an equality **before** that nil check, so a missing
    /// column still lowers the factor.
    Resolved {
        /// Which switch arm the function name takes.
        kind: PseudoFunctionKind,
        /// The resolved column, or `None` when the collection has no entry.
        column: Option<PseudoColumn>,
    },
}

/// One index as the source's `ForEachIndexImmutable` walk sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PseudoIndex {
    /// Whether `idx.Info.Unique` holds.
    pub unique: bool,
    /// Lowercased index column names, in index order.
    pub column_lower_names: Vec<String>,
}

/// Whole-expression pseudo selectivity, Go `pseudoSelectivity`
/// (`pkg/planner/cardinality/pseudo.go:40-97`).
///
/// `Selectivity` takes this path when there are more than 63 conditions or
/// when the collection has neither column nor index statistics
/// (`selectivity.go:69-73`) -- that is, on tables that have not been analyzed,
/// which is the common case rather than the exotic one.
///
/// Three source behaviors are load-bearing and reproduced exactly:
///
/// * The unique-key shortcut returns `1.0 / RealtimeCount` **immediately**,
///   abandoning every other condition's contribution. Real TiDB confirms it:
///   on a 10000-pseudo-row unanalyzed table, 64 `a != k` conditions estimate
///   8000.00 rows, and the same 64 plus `d = 7` on a `UNIQUE` column estimate
///   1.00.
/// * `minFactor` only ever moves down, and the equality arm charges
///   `1/pseudoEqualRate` even when the column has no statistics entry.
/// * The composite-index check requires **every** index column to appear in
///   `colExists`; a prefix match is not enough. The source's `firstMatch`
///   variable only gates a statistics-load side effect and has no effect on
///   the returned number, so it is not modelled here.
///
/// `selectivity_factor` is the session's `SelectivityFactor` (0.8 by default).
#[must_use]
pub fn pseudo_selectivity(
    predicates: &[PseudoPredicate],
    indexes: &[PseudoIndex],
    realtime_count: i64,
    selectivity_factor: f64,
) -> f64 {
    let mut min_factor = selectivity_factor;
    let mut col_exists: Vec<&str> = Vec::new();

    for predicate in predicates {
        let PseudoPredicate::Resolved { kind, column } = predicate else {
            continue;
        };
        match kind {
            PseudoFunctionKind::Equality => {
                min_factor = min_factor.min(1.0 / PSEUDO_EQUAL_RATE);
                let Some(column) = column else {
                    continue;
                };
                if !col_exists.contains(&column.lower_name.as_str()) {
                    col_exists.push(&column.lower_name);
                }
                if column.unique_key_flag {
                    return 1.0 / realtime_count as f64;
                }
            }
            PseudoFunctionKind::Ordering => {
                min_factor = min_factor.min(1.0 / PSEUDO_LESS_RATE);
            }
            PseudoFunctionKind::Other => {}
        }
    }

    if col_exists.is_empty() {
        return min_factor;
    }

    let has_unique_key = indexes.iter().any(|index| {
        index.unique
            && index
                .column_lower_names
                .iter()
                .all(|name| col_exists.contains(&name.as_str()))
    });

    if has_unique_key {
        return 1.0 / realtime_count as f64;
    }
    min_factor
}
