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

//! Which partitions a read has to touch: `PARTITION (p)` selection and RANGE
//! pruning.
//!
//! Mirrors Go `pkg/planner/core/rule/rule_partition_processor.go` -- its
//! `PruneRangePartition`/`PruneUseBinarySearch` for the pruning and its
//! `FindByName`/`resolveOptimizeHint` for the explicit selection. Both answer
//! the SAME question and are therefore one module: they each narrow the set
//! of physical table ids a scan reads, and the scan applies whichever
//! narrowing it was handed.
//!
//! # Pruning is a RANGE question, answered by the crate's range algebra
//!
//! Go prunes with `pkg/util/ranger`, and so does this: the caller hands over
//! the [`tidb_executor::index_range`](crate::index_range) intervals it
//! already built for the partition expression's column, and this module maps
//! each interval onto the partition ordinals it can intersect. There is no
//! second range implementation here -- the point algebra, the exclusivity
//! and the NULL handling all belong to `index_range` and are read, not
//! reimplemented.
//!
//! # Why pruning may only ever read a SUPERSET
//!
//! A partition dropped in error is a scan that misses matching rows and
//! returns FEWER rows with no error at all -- one of this project's silent
//! wrong answers. Every uncertainty here therefore resolves to "keep the
//! partition": an expression the caller could not turn into ranges, a NULL
//! that could be in the lowest partition, a bound this tier cannot compare.
//! Reading a superset costs time; reading a subset costs correctness.
//!
//! # NOT pruned here
//!
//! HASH. Go prunes a hash-partitioned table for an EQUALITY on the partition
//! expression (`getUsedHashPartitions`), which this tier does not do: it
//! scans every hash partition and lets the `WHERE` above filter. That is a
//! performance gap, never a wrong answer, and it is stated rather than
//! hidden.

use crate::kv_table::IndexRange;
use crate::partition_routing::{PartitionKind, PartitionSpec, RangeBound};
use tidb_datatype::Datum;

/// Go `FindByName` over an explicit `PARTITION (p, ...)` list: the physical
/// ids those partitions occupy, in the table's own definition order.
///
/// The order is the TABLE's rather than the clause's because the ids are one
/// ascending block and every downstream key range is built ascending; the
/// clause's order carries no meaning in Go either.
///
/// # Errors
///
/// The name, when the table has no such partition -- Go's
/// `ErrUnknownPartition` (1735), captured as
/// `Unknown partition 'nosuch' in table 'ok1'`.
pub fn ids_for_selected_partitions(
    spec: &PartitionSpec,
    names: &[String],
) -> Result<Vec<i64>, String> {
    for name in names {
        if spec.definition_named(name).is_none() {
            return Err(name.clone());
        }
    }
    Ok(spec
        .definitions
        .iter()
        .filter(|def| {
            names
                .iter()
                .any(|name| name.eq_ignore_ascii_case(&def.name))
        })
        .map(|def| def.id)
        .collect())
}

/// Go `PruneRangePartition`: the physical ids a scan restricted by `ranges`
/// over the partition expression must read, or `None` when nothing can be
/// pruned.
///
/// `ranges` are the [`IndexRange`] intervals the RANGER built for the
/// partition expression's OWN value -- the same intervals a single-column
/// index on it would take, and the caller must pass the ranger's output
/// rather than a list it assembled. `None` back means "read everything",
/// which is what an unprunable table and a method without pruning reduce to;
/// an unprunable PREDICATE never reaches here, because the ranger answers
/// `None` for it one level up.
///
/// An EMPTY `ranges` is therefore the ranger's contradictory `WHERE` -- the
/// same reading [`crate::table_access::TableAccess::accept_handle_ranges`]
/// gives it -- and prunes every partition away. Reading it as "no
/// restriction" instead would turn `a >= 10 AND a < 10` into a full scan.
///
/// # The NULL partition is never pruned away by a range
///
/// A RANGE table stores every NULL in its LOWEST partition, whatever its
/// bound says. `index_range`'s intervals describe the values a predicate
/// ADMITS, and a predicate that admits no NULL still cannot prove the lowest
/// partition is empty of rows it wants -- the partition holds real values
/// too. So the lowest partition is dropped only when the ranges prove no
/// value below its bound qualifies, which is exactly what the interval test
/// below asks.
#[must_use]
pub fn pruned_ids(spec: &PartitionSpec, ranges: &[IndexRange]) -> Option<Vec<i64>> {
    let PartitionKind::Range {
        less_than,
        unsigned,
    } = &spec.kind
    else {
        return None;
    };
    let mut kept = Vec::with_capacity(spec.definitions.len());
    for (index, definition) in spec.definitions.iter().enumerate() {
        let low = if index == 0 {
            None
        } else {
            match less_than[index - 1] {
                RangeBound::Value(value) => Some(value),
                // A `MAXVALUE` below the last position is 1481 at CREATE, so
                // this is unreachable; keeping the partition is the safe
                // reading if it ever were.
                RangeBound::MaxValue => None,
            }
        };
        let high = match less_than[index] {
            RangeBound::Value(value) => Some(value),
            RangeBound::MaxValue => None,
        };
        if ranges
            .iter()
            .any(|range| range_meets_partition(range, low, high, *unsigned))
        {
            kept.push(definition.id);
        }
    }
    Some(kept)
}

/// Whether one `index_range` interval can hold a value this partition
/// stores, given the partition's half-open value window `[low, high)`.
///
/// `low`/`high` of `None` are unbounded (the first partition below, a
/// `MAXVALUE` partition above). Anything the interval cannot be compared
/// against -- a non-integer bound, a bound this tier cannot read -- answers
/// `true`, because an unreadable bound proves nothing about emptiness.
fn range_meets_partition(
    range: &IndexRange,
    low: Option<i64>,
    high: Option<i64>,
    unsigned: bool,
) -> bool {
    // A range whose low end is at or above the partition's EXCLUSIVE upper
    // bound admits no value this partition stores. An exclusive low end
    // admits strictly more than its datum, so this test is conservative
    // there by exactly one value -- it keeps a partition that `(9, ...]`
    // against a bound of `10` cannot in fact reach. A superset is the side
    // this module errs on deliberately.
    //
    // A low end that is not an integer -- NULL, `MinNotNull`, a string --
    // proves nothing and keeps the partition.
    if let (Some(high), Some(value)) = (high, interval_low(range)) {
        if !less(value, high, unsigned) {
            return false;
        }
    }
    // A range whose high end is below the partition's INCLUSIVE lower bound
    // admits no value this partition stores. Here the exclusive case is
    // exact: `[..., low)` admits nothing at or above `low`.
    if let (Some(low), Some((value, exclusive))) = (low, interval_high(range)) {
        let below = if exclusive {
            !less(low, value, unsigned)
        } else {
            less(value, low, unsigned)
        };
        if below {
            return false;
        }
    }
    true
}

/// `a < b`, read with the partition expression's own signedness.
fn less(a: i64, b: i64, unsigned: bool) -> bool {
    if unsigned {
        (a as u64) < (b as u64)
    } else {
        a < b
    }
}

/// The interval's low endpoint as an integer, or `None` for an endpoint this
/// tier cannot compare against a partition bound.
fn interval_low(range: &IndexRange) -> Option<i64> {
    integer_endpoint(range.low.first())
}

/// The interval's high endpoint and whether it is EXCLUSIVE, as
/// [`interval_low`].
fn interval_high(range: &IndexRange) -> Option<(i64, bool)> {
    integer_endpoint(range.high.first()).map(|value| (value, range.high_exclusive))
}

/// One endpoint datum as the 64-bit pattern a partition bound compares
/// against, or `None` when it is not an integer at all.
///
/// `MinNotNull`/`MaxValue` are the range algebra's own infinities and NULL is
/// below every value; none of them bounds a partition, so each answers
/// `None` and keeps the partition.
fn integer_endpoint(value: Option<&Datum>) -> Option<i64> {
    match value? {
        Datum::Int(value) => Some(*value),
        Datum::UInt(value) => Some(*value as i64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_routing::PartitionDef;

    fn range_table() -> PartitionSpec {
        PartitionSpec {
            kind: PartitionKind::Range {
                less_than: vec![
                    RangeBound::Value(10),
                    RangeBound::Value(20),
                    RangeBound::MaxValue,
                ],
                unsigned: false,
            },
            expr_text: "`a`".to_owned(),
            expr: tidb_expr::expression::Expression::Constant(
                tidb_expr::expression::Constant::new(
                    Datum::Int(0),
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                ),
            ),
            dependencies: vec!["a".to_owned()],
            definitions: vec![
                PartitionDef {
                    id: 101,
                    name: "p0".to_owned(),
                },
                PartitionDef {
                    id: 102,
                    name: "p1".to_owned(),
                },
                PartitionDef {
                    id: 103,
                    name: "pm".to_owned(),
                },
            ],
        }
    }

    fn interval(low: Datum, low_exclusive: bool, high: Datum, high_exclusive: bool) -> IndexRange {
        IndexRange {
            low: vec![low],
            high: vec![high],
            low_exclusive,
            high_exclusive,
        }
    }

    /// The captured queries, as the partitions each must read: `a < 10` is
    /// `p0` alone, `a = 10` is `p1` alone, `a >= 20` is `pm` alone.
    #[test]
    fn range_pruning_keeps_exactly_the_partitions_the_predicate_can_match() {
        let spec = range_table();
        // a < 10
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::MinNotNull, false, Datum::Int(10), true)]
            ),
            Some(vec![101])
        );
        // a = 10
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(10), false, Datum::Int(10), false)]
            ),
            Some(vec![102])
        );
        // a >= 20
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(20), false, Datum::MaxValue, false)]
            ),
            Some(vec![103])
        );
        // a >= 9 AND a <= 20 spans all three.
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(9), false, Datum::Int(20), false)]
            ),
            Some(vec![101, 102, 103])
        );
    }

    /// The boundary rows, one interval each: `a = 9` and `a = 10` must not
    /// land in the same partition, and `a = 19`/`a = 20` must not either.
    #[test]
    fn the_boundary_values_prune_to_different_partitions() {
        let spec = range_table();
        for (value, expected) in [(9_i64, 101_i64), (10, 102), (19, 102), (20, 103)] {
            assert_eq!(
                pruned_ids(
                    &spec,
                    &[interval(Datum::Int(value), false, Datum::Int(value), false)]
                ),
                Some(vec![expected]),
                "a = {value}"
            );
        }
    }

    /// An endpoint this tier cannot compare against a bound prunes nothing
    /// and reads the whole table, while the ranger's own EMPTY range list is
    /// the contradictory `WHERE` and prunes everything away.
    #[test]
    fn an_inexpressible_restriction_prunes_nothing() {
        let spec = range_table();
        assert_eq!(pruned_ids(&spec, &[]), Some(Vec::new()));
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(
                    Datum::Null,
                    false,
                    Datum::Bytes(b"x".to_vec()),
                    false
                )]
            ),
            Some(vec![101, 102, 103])
        );
    }

    /// A HASH table is never pruned by this module, and says so by answering
    /// `None` rather than by returning every id.
    #[test]
    fn a_hash_table_is_not_pruned_here() {
        let mut spec = range_table();
        spec.kind = PartitionKind::Hash;
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(1), false, Datum::Int(1), false)]
            ),
            None
        );
    }

    /// `PARTITION (p)` resolves case-insensitively, keeps the TABLE's order,
    /// and names the partition it could not find.
    #[test]
    fn an_explicit_selection_resolves_to_its_ids() {
        let spec = range_table();
        assert_eq!(
            ids_for_selected_partitions(&spec, &["PM".to_owned(), "p0".to_owned()]),
            Ok(vec![101, 103])
        );
        assert_eq!(
            ids_for_selected_partitions(&spec, &["nosuch".to_owned()]),
            Err("nosuch".to_owned())
        );
    }
}
