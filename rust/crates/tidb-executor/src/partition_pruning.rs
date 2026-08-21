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

/// The physical ids a scan restricted by `ranges` over the partition
/// expression must read, or `None` when nothing can be pruned. RANGE maps
/// intervals onto definition bounds; HASH maps point and short integer
/// intervals through the same router used by writes.
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
    match &spec.kind {
        // Every row is in partition 0, so there is nothing to prune away.
        PartitionKind::None => None,
        PartitionKind::Hash => prune_hash_ids(spec, ranges),
        PartitionKind::Key => Some(prune_key_ids(spec, ranges)),
        PartitionKind::Range {
            less_than,
            unsigned,
        } => Some(prune_range_ids(spec, ranges, less_than, *unsigned)),
        PartitionKind::RangeColumns {
            less_than,
            field_types,
        } => Some(prune_range_columns_ids(
            spec,
            ranges,
            less_than,
            field_types,
        )),
        PartitionKind::List {
            values,
            null_partition,
            default_partition,
            unsigned,
        } => Some(prune_list_ids(
            spec,
            ranges,
            values,
            *null_partition,
            *default_partition,
            *unsigned,
        )),
        PartitionKind::ListColumns {
            values,
            default_partition,
            ..
        } => Some(prune_list_columns_ids(
            spec,
            ranges,
            values,
            *default_partition,
        )),
    }
}

/// Go's KEY pruner can use an exact range only when every partition column is
/// constrained.  A partial key predicate may hash into every partition, so it
/// remains a full scan.
fn prune_key_ids(spec: &PartitionSpec, ranges: &[IndexRange]) -> Vec<i64> {
    let mut used = vec![false; spec.definitions.len()];
    for range in ranges {
        if !range.is_point(true) || range.low.len() != spec.dependencies.len() {
            return spec
                .definitions
                .iter()
                .map(|definition| definition.id)
                .collect();
        }
        let Ok(ordinal) =
            crate::partition_routing::key_partition_index_for_tuple(&range.low, spec.num())
        else {
            return spec
                .definitions
                .iter()
                .map(|definition| definition.id)
                .collect();
        };
        used[ordinal] = true;
    }
    spec.definitions
        .iter()
        .zip(used)
        .filter_map(|(definition, used)| used.then_some(definition.id))
        .collect()
}

/// A typed ranger point has one exact RANGE COLUMNS destination, so retain
/// only that partition.  More complex tuple intervals remain a full scan
/// until the ranger exposes enough normalized endpoint information to prove
/// their intersection without risking a false negative.
fn prune_range_columns_ids(
    spec: &PartitionSpec,
    ranges: &[IndexRange],
    less_than: &[Vec<crate::partition_routing::RangeColumnBound>],
    field_types: &[tidb_datatype::FieldType],
) -> Vec<i64> {
    let mut used = vec![false; spec.definitions.len()];
    for range in ranges {
        if !range.is_point(true) || range.low.len() != field_types.len() {
            return spec
                .definitions
                .iter()
                .map(|definition| definition.id)
                .collect();
        }
        let Ok(ordinal) = crate::partition_routing::range_columns_partition_index_for_tuple(
            &range.low,
            less_than,
            field_types,
        ) else {
            return spec
                .definitions
                .iter()
                .map(|definition| definition.id)
                .collect();
        };
        if let Some(slot) = used.get_mut(ordinal) {
            *slot = true;
        }
    }
    spec.definitions
        .iter()
        .zip(used)
        .filter_map(|(definition, used)| used.then_some(definition.id))
        .collect()
}

/// Go `ForListColumnPruning.LocateRanges`: a tuple belongs when its prefix
/// key intersects a ranger interval. The ranger may constrain only the first
/// N partition columns, so comparisons intentionally use each interval's
/// prefix length rather than requiring a full tuple equality.
fn prune_list_columns_ids(
    spec: &PartitionSpec,
    ranges: &[IndexRange],
    values: &[(Vec<Datum>, usize)],
    default_partition: Option<usize>,
) -> Vec<i64> {
    let mut used = vec![false; spec.definitions.len()];
    for range in ranges {
        for (tuple, ordinal) in values {
            if *ordinal < used.len() && tuple_in_range(tuple, range) {
                used[*ordinal] = true;
            }
        }
    }
    // Go adds DEFAULT to every `LocateRanges` result: gaps in any predicate
    // range may contain a tuple it owns.
    if let Some(ordinal) = default_partition.filter(|ordinal| *ordinal < used.len()) {
        used[ordinal] = true;
    }
    spec.definitions
        .iter()
        .zip(used)
        .filter_map(|(definition, used)| used.then_some(definition.id))
        .collect()
}

fn tuple_in_range(tuple: &[Datum], range: &IndexRange) -> bool {
    let width = range.low.len().min(range.high.len()).min(tuple.len());
    if width == 0 {
        return true;
    }
    let value = match tidb_codec::encode_key(&tuple[..width]) {
        Ok(value) => value,
        Err(_) => return true,
    };
    let low = match tidb_codec::encode_key(&range.low[..width]) {
        Ok(value) => value,
        Err(_) => return true,
    };
    let high = match tidb_codec::encode_key(&range.high[..width]) {
        Ok(value) => value,
        Err(_) => return true,
    };
    let lower_ok = if range.low_exclusive {
        value > low
    } else {
        value >= low
    };
    let upper_ok = if range.high_exclusive {
        value < high
    } else {
        value <= high
    };
    lower_ok && upper_ok
}

/// Go `ForListPruning.LocatePartitionByRange`: retain the definitions owning
/// values inside any ranger interval. A DEFAULT definition is always retained
/// because an interval can include values that have no explicit owner.
fn prune_list_ids(
    spec: &PartitionSpec,
    ranges: &[IndexRange],
    values: &[(i64, usize)],
    null_partition: Option<usize>,
    default_partition: Option<usize>,
    unsigned: bool,
) -> Vec<i64> {
    let mut used = vec![false; spec.definitions.len()];
    for range in ranges {
        if interval_is_null_point(range) {
            if let Some(ordinal) = null_partition.filter(|ordinal| *ordinal < used.len()) {
                used[ordinal] = true;
            }
            continue;
        }
        let Some((low, high)) = scalar_interval(range) else {
            return spec
                .definitions
                .iter()
                .map(|definition| definition.id)
                .collect();
        };
        for (value, ordinal) in values {
            if *ordinal < used.len() && scalar_in_interval(*value, low, high, unsigned) {
                used[*ordinal] = true;
            }
        }
        if interval_includes_null(range) {
            if let Some(ordinal) = null_partition.filter(|ordinal| *ordinal < used.len()) {
                used[ordinal] = true;
            }
        }
    }
    if let Some(ordinal) = default_partition.filter(|ordinal| *ordinal < used.len()) {
        used[ordinal] = true;
    }
    spec.definitions
        .iter()
        .zip(used)
        .filter_map(|(definition, used)| used.then_some(definition.id))
        .collect()
}

/// A scalar range endpoint: its value, whether the endpoint is EXCLUSIVE,
/// and whether the value is UNSIGNED in its own right.
///
/// That third component is the one Go carries and this module used to throw
/// away. Go compares a partition bound against a query constant with
/// `types.CompareInt(bound, boundUnsigned, value, valueUnsigned)`
/// (`pkg/planner/core/rule/rule_partition_processor.go:938`), where the two
/// flags come from DIFFERENT places -- the bound's from the partitioning
/// column's type, the constant's from its own. Folding them into one flag
/// makes `a < 18446744073709551615` over a SIGNED column prune partitions
/// that hold matching rows.
type ScalarRangeEndpoint = (Option<i64>, bool, bool);
type ScalarInterval = (ScalarRangeEndpoint, ScalarRangeEndpoint);

/// One scalar ranger interval. `None` means an endpoint has a type that the
/// partition expression cannot compare, so its safe pruning answer is every
/// partition.
fn scalar_interval(range: &IndexRange) -> Option<ScalarInterval> {
    fn endpoint(value: Option<&Datum>) -> Option<(Option<i64>, bool)> {
        match value? {
            Datum::Int(value) => Some((Some(*value), false)),
            // Go keeps `HasUnsignedFlag(constExpr.GetType())` for the
            // constant; the datum's own kind is that flag here.
            Datum::UInt(value) => Some((Some(*value as i64), true)),
            Datum::Null | Datum::MinNotNull | Datum::MaxValue => Some((None, false)),
            _ => None,
        }
    }
    let (low, low_unsigned) = endpoint(range.low.first())?;
    let (high, high_unsigned) = endpoint(range.high.first())?;
    Some((
        (low, range.low_exclusive, low_unsigned),
        (high, range.high_exclusive, high_unsigned),
    ))
}

use std::cmp::Ordering;

/// Go `types.CompareInt` (`pkg/types/compare.go`): compare two 64-bit
/// integers that may independently be signed or unsigned.
///
/// The two mixed cases are the whole point. When only one side is unsigned,
/// Go decides by whether the signed side is negative or the unsigned side
/// exceeds `i64::MAX`; casting both to one type instead answers the opposite
/// way for exactly the values that matter.
fn compare_int(left: i64, left_unsigned: bool, right: i64, right_unsigned: bool) -> Ordering {
    match (left_unsigned, right_unsigned) {
        (true, true) => (left as u64).cmp(&(right as u64)),
        (true, false) => {
            if right < 0 || (left as u64) > i64::MAX as u64 {
                Ordering::Greater
            } else {
                left.cmp(&right)
            }
        }
        (false, true) => {
            if left < 0 || (right as u64) > i64::MAX as u64 {
                Ordering::Less
            } else {
                left.cmp(&right)
            }
        }
        (false, false) => left.cmp(&right),
    }
}

fn scalar_in_interval(
    value: i64,
    low: ScalarRangeEndpoint,
    high: ScalarRangeEndpoint,
    unsigned: bool,
) -> bool {
    // `value` is a stored LIST value, so its signedness is the partition
    // column's; each endpoint carries its own. Go compares the two with
    // `types.CompareInt` rather than casting both to one type.
    let lower_ok = low.0.is_none_or(|bound| {
        let order = compare_int(value, unsigned, bound, low.2);
        if low.1 {
            order == Ordering::Greater
        } else {
            order != Ordering::Less
        }
    });
    let upper_ok = high.0.is_none_or(|bound| {
        let order = compare_int(value, unsigned, bound, high.2);
        if high.1 {
            order == Ordering::Less
        } else {
            order != Ordering::Greater
        }
    });
    lower_ok && upper_ok
}

/// `NULL` is selected when the range starts at inclusive `NULL`: this covers
/// both the `IS NULL` point and `IndexRange::full()`. Ordinary comparisons
/// start at `MinNotNull` and therefore cannot select it.
fn interval_includes_null(range: &IndexRange) -> bool {
    matches!(range.low.first(), Some(Datum::Null)) && !range.low_exclusive
}

fn interval_is_null_point(range: &IndexRange) -> bool {
    matches!(range.low.first(), Some(Datum::Null))
        && !range.low_exclusive
        && matches!(range.high.first(), Some(Datum::Null))
        && !range.high_exclusive
}

fn prune_range_ids(
    spec: &PartitionSpec,
    ranges: &[IndexRange],
    less_than: &[RangeBound],
    unsigned: bool,
) -> Vec<i64> {
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
        if ranges.iter().any(|range| {
            (index == 0 && interval_includes_null(range))
                || (!interval_is_null_point(range)
                    && range_meets_partition(range, low, high, unsigned))
        }) {
            kept.push(definition.id);
        }
    }
    kept
}

/// Go `getUsedHashPartitions` for the admitted bare-integer partition
/// expression. Points use the table router's conversion and modulus rule.
/// A finite integer interval is enumerated only when its width is smaller
/// than the partition count; wider or non-integer intervals conservatively
/// keep the full scan.
fn prune_hash_ids(spec: &PartitionSpec, ranges: &[IndexRange]) -> Option<Vec<i64>> {
    let mut used = vec![false; spec.definitions.len()];
    for range in ranges {
        if range.is_point(true) {
            let value = range.high.first()?;
            let index = crate::partition_routing::hash_partition_index(value, spec.num()).ok()?;
            used[index] = true;
            continue;
        }
        if !mark_short_hash_range(range, spec.num(), &mut used) {
            return None;
        }
    }
    Some(
        spec.definitions
            .iter()
            .zip(used)
            .filter_map(|(definition, used)| used.then_some(definition.id))
            .collect(),
    )
}

fn mark_short_hash_range(range: &IndexRange, partitions: u64, used: &mut [bool]) -> bool {
    let (Some(low), Some(high)) = (range.low.first(), range.high.first()) else {
        return false;
    };
    match (low, high) {
        (Datum::Int(low), Datum::Int(high)) => {
            let low = if range.low_exclusive {
                low.wrapping_add(1)
            } else {
                *low
            };
            let high = if range.high_exclusive {
                high.wrapping_sub(1)
            } else {
                *high
            };
            let width = if high < low {
                0
            } else {
                high.wrapping_sub(low) as u64
            };
            if width >= partitions {
                return false;
            }
            for offset in 0..=width {
                let value = Datum::Int(low.wrapping_add(offset as i64));
                let Ok(index) = crate::partition_routing::hash_partition_index(&value, partitions)
                else {
                    return false;
                };
                used[index] = true;
            }
            true
        }
        (Datum::UInt(low), Datum::UInt(high)) => {
            let low = if range.low_exclusive {
                low.wrapping_add(1)
            } else {
                *low
            };
            let high = if range.high_exclusive {
                high.wrapping_sub(1)
            } else {
                *high
            };
            let width = high.saturating_sub(low);
            if width >= partitions {
                return false;
            }
            for offset in 0..=width {
                let value = Datum::UInt(low.wrapping_add(offset));
                let Ok(index) = crate::partition_routing::hash_partition_index(&value, partitions)
                else {
                    return false;
                };
                used[index] = true;
            }
            true
        }
        _ => false,
    }
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
    // `high`/`low` are PARTITION bounds, so they carry the partitioning
    // column's signedness; the range endpoints carry their own.
    if let (Some(high), Some((value, value_unsigned))) = (high, interval_low(range)) {
        if compare_int(value, value_unsigned, high, unsigned) != Ordering::Less {
            return false;
        }
    }
    // A range whose high end is below the partition's INCLUSIVE lower bound
    // admits no value this partition stores. Here the exclusive case is
    // exact: `[..., low)` admits nothing at or above `low`.
    if let (Some(low), Some((value, exclusive, value_unsigned))) = (low, interval_high(range)) {
        let below = if exclusive {
            compare_int(low, unsigned, value, value_unsigned) != Ordering::Less
        } else {
            compare_int(value, value_unsigned, low, unsigned) == Ordering::Less
        };
        if below {
            return false;
        }
    }
    true
}

/// `a < b`, read with the partition expression's own signedness.
/// The interval's low endpoint as an integer, or `None` for an endpoint this
/// tier cannot compare against a partition bound.
fn interval_low(range: &IndexRange) -> Option<(i64, bool)> {
    integer_endpoint(range.low.first())
}

/// The interval's high endpoint and whether it is EXCLUSIVE, as
/// [`interval_low`].
fn interval_high(range: &IndexRange) -> Option<(i64, bool, bool)> {
    integer_endpoint(range.high.first())
        .map(|(value, unsigned)| (value, range.high_exclusive, unsigned))
}

/// One endpoint datum as the 64-bit pattern a partition bound compares
/// against, or `None` when it is not an integer at all.
///
/// `MinNotNull`/`MaxValue` are the range algebra's own infinities and NULL is
/// below every value; none of them bounds a partition, so each answers
/// `None` and keeps the partition.
/// The endpoint's value AND whether it is unsigned in its own right.
///
/// Collapsing `Datum::UInt(v)` to `v as i64` and dropping the flag is what
/// made a constant above `i64::MAX` read as a negative number and prune the
/// wrong partitions.
fn integer_endpoint(value: Option<&Datum>) -> Option<(i64, bool)> {
    match value? {
        Datum::Int(value) => Some((*value, false)),
        Datum::UInt(value) => Some((*value as i64, true)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_routing::PartitionDef;

    fn range_table() -> PartitionSpec {
        PartitionSpec {
            is_empty_columns: false,
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
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                },
                PartitionDef {
                    id: 102,
                    name: "p1".to_owned(),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                },
                PartitionDef {
                    id: 103,
                    name: "pm".to_owned(),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                },
            ],
        }
    }

    fn list_table() -> PartitionSpec {
        PartitionSpec {
            is_empty_columns: false,
            kind: PartitionKind::List {
                values: vec![(1, 0), (3, 0), (5, 1)],
                null_partition: Some(1),
                default_partition: Some(2),
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
                    id: 201,
                    name: "p0".to_owned(),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                },
                PartitionDef {
                    id: 202,
                    name: "pn".to_owned(),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                },
                PartitionDef {
                    id: 203,
                    name: "pd".to_owned(),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
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
        assert_eq!(
            pruned_ids(&spec, &[interval(Datum::Null, false, Datum::Null, false)]),
            Some(vec![101])
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

    /// LIST keeps exact owners for points/ranges, keeps its NULL owner only
    /// for an `IS NULL` range, and always retains DEFAULT for a possible gap.
    #[test]
    fn list_pruning_matches_gos_explicit_and_default_owners() {
        let spec = list_table();
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(1), false, Datum::Int(1), false)]
            ),
            Some(vec![201, 203])
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(2), false, Datum::Int(3), false)]
            ),
            Some(vec![201, 203])
        );
        assert_eq!(
            pruned_ids(&spec, &[interval(Datum::Null, false, Datum::Null, false)]),
            Some(vec![202, 203])
        );

        let mut without_default = spec.clone();
        if let PartitionKind::List {
            default_partition, ..
        } = &mut without_default.kind
        {
            *default_partition = None;
        }
        assert_eq!(
            pruned_ids(&without_default, &[IndexRange::full()]),
            Some(vec![201, 202]),
            "a full range must retain the explicit NULL owner"
        );
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

    /// A HASH table narrows point and short integer ranges to the partitions
    /// those values route into. A wider range stays a full scan.
    #[test]
    fn hash_pruning_keeps_only_the_partitions_the_values_route_into() {
        let mut spec = range_table();
        spec.kind = PartitionKind::Hash;
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(1), false, Datum::Int(1), false)]
            ),
            Some(vec![102])
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[
                    interval(Datum::Int(1), false, Datum::Int(1), false),
                    interval(Datum::Int(-2), false, Datum::Int(-2), false),
                ]
            ),
            Some(vec![102, 103])
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(3), false, Datum::Int(4), false)]
            ),
            Some(vec![101, 102])
        );
        assert_eq!(
            pruned_ids(&spec, &[interval(Datum::Int(0), true, Datum::Int(2), true)]),
            Some(vec![102])
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(0), false, Datum::Int(2), false)]
            ),
            Some(vec![101, 102, 103])
        );
        assert_eq!(
            pruned_ids(&spec, &[interval(Datum::Null, false, Datum::Null, false)]),
            Some(vec![101])
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(
                    Datum::UInt(u64::MAX),
                    false,
                    Datum::UInt(u64::MAX),
                    false,
                )]
            ),
            Some(vec![102])
        );
        assert_eq!(pruned_ids(&spec, &[]), Some(Vec::new()));
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(0), false, Datum::Int(3), false)]
            ),
            None
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::MinNotNull, false, Datum::MaxValue, false,)]
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

    /// Go `types.CompareInt` (`pkg/types/compare.go`), case by case.
    ///
    /// The expectations are read off Go's own branches, not off this port:
    /// with both sides unsigned it compares as `uint64`; with both signed as
    /// `int64`; and when exactly one side is unsigned Go answers by whether
    /// the signed side is negative or the unsigned side exceeds `i64::MAX`,
    /// only falling through to a signed comparison when neither holds.
    #[test]
    fn compare_int_matches_gos_four_signedness_cases() {
        // (false, false): plain signed.
        assert_eq!(compare_int(-5, false, 3, false), Ordering::Less);
        assert_eq!(compare_int(3, false, 3, false), Ordering::Equal);

        // (true, true): both read as unsigned, so the bit pattern of -1 is
        // the largest value there is.
        assert_eq!(compare_int(-1, true, 3, true), Ordering::Greater);

        // (false, true): a NEGATIVE signed side is always smaller...
        assert_eq!(compare_int(-5, false, 3, true), Ordering::Less);
        // ...and so is any signed value when the unsigned side is above
        // i64::MAX. This is the case that made pruning wrong: the bound 10
        // is BELOW the constant 18446744073709551615, whose bit pattern as
        // i64 is -1.
        assert_eq!(compare_int(10, false, -1, true), Ordering::Less);
        // Neither special condition holds, so it is an ordinary comparison.
        assert_eq!(compare_int(10, false, 3, true), Ordering::Greater);

        // (true, false): the mirror image.
        assert_eq!(compare_int(3, true, -5, false), Ordering::Greater);
        assert_eq!(compare_int(-1, true, 10, false), Ordering::Greater);
        assert_eq!(compare_int(3, true, 10, false), Ordering::Less);
    }

    /// The whole point of carrying the second flag: a partition bound and a
    /// query constant can disagree about signedness, and casting both to one
    /// type answers backwards for exactly the values that matter.
    #[test]
    fn a_constant_above_i64_max_does_not_prune_a_signed_partition() {
        // `PARTITION BY RANGE (a)` over a SIGNED column with bound 10, and
        // the predicate `a < 18446744073709551615`, whose constant arrives as
        // an unsigned datum. Go keeps every partition, because the bound is
        // below the constant.
        let bound_below_constant = compare_int(10, false, -1, true);
        assert_eq!(bound_below_constant, Ordering::Less);

        // Reading the constant with the COLUMN's signedness instead -- what
        // this module used to do -- reaches the opposite verdict and would
        // prune a partition holding matching rows.
        let collapsed = 10_i64.cmp(&-1_i64);
        assert_eq!(collapsed, Ordering::Greater);
        assert_ne!(bound_below_constant, collapsed);
    }
}
