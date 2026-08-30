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
//! # Pruning consumes the ranger's intervals over Go's pruning columns
//!
//! The caller hands over [`IndexRange`] intervals built for the columns read
//! by the partition expression. As pinned Go does, scalar RANGE pruning
//! evaluates the full expression for points and for the supported monotone
//! functions, HASH and scalar LIST evaluate points, RANGE COLUMNS compares
//! ranger tuples directly, and LIST COLUMNS recursively combines per-column
//! tuple-group locations in the planner bridge.
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
use std::collections::{BTreeMap, BTreeSet};
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
/// expression's pruning columns must read, or `None` when nothing can be
/// pruned. RANGE maps evaluated expression intervals onto definition bounds;
/// HASH maps point and short integer intervals through Go's pruning rules.
///
/// `ranges` are the [`IndexRange`] intervals the ranger built for Go's
/// pruning columns, and the caller must pass the ranger's output rather than
/// a list it assembled. `None` back means "read everything",
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
pub fn pruned_ids(
    spec: &PartitionSpec,
    ranges: &[IndexRange],
    ctx: &impl tidb_expr::Columns,
) -> Result<Option<Vec<i64>>, tidb_expr::EvalError> {
    Ok(match &spec.kind {
        // Every row is in partition 0, so there is nothing to prune away.
        PartitionKind::None => None,
        PartitionKind::Hash => prune_hash_ids(spec, ranges, ctx),
        PartitionKind::Key => Some(prune_key_ids(spec, ranges)),
        PartitionKind::Range {
            less_than,
            unsigned,
        } => Some(prune_range_ids(spec, ranges, less_than, *unsigned, ctx)),
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
            ctx,
        )?),
        // Go's LIST COLUMNS pruner does not detach one composite range over
        // all partition columns. It recursively locates every single-column
        // predicate and intersects its VALUES IN tuple-group identities.
        // The planner bridge owns that predicate tree, so this range-only
        // entry point must not invent a second, incompatible pruning path.
        PartitionKind::ListColumns { .. } => None,
    })
}

/// Go `getUsedKeyPartitions`: exact tuples route directly; a short integer
/// interval over one KEY column is enumerated while its width is smaller than
/// the partition count. A partial multi-column key may hash anywhere and
/// remains a full scan.
fn prune_key_ids(spec: &PartitionSpec, ranges: &[IndexRange]) -> Vec<i64> {
    let mut used = vec![false; spec.definitions.len()];
    for range in ranges {
        if range.is_point(true) && range.low.len() == spec.dependencies.len() {
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
            continue;
        }
        if spec.dependencies.len() == 1 && mark_short_key_range(range, spec.num(), &mut used) {
            continue;
        }
        return spec
            .definitions
            .iter()
            .map(|definition| definition.id)
            .collect();
    }
    spec.definitions
        .iter()
        .zip(used)
        .filter_map(|(definition, used)| used.then_some(definition.id))
        .collect()
}

fn mark_short_key_range(range: &IndexRange, partitions: u64, used: &mut [bool]) -> bool {
    let (Some(low), Some(high)) = (range.low.first(), range.high.first()) else {
        return false;
    };
    let (low, high, unsigned) = match (low, high) {
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
            (low as u64, high as u64, false)
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
            (low, high, true)
        }
        _ => return false,
    };
    let width = if unsigned {
        high.saturating_sub(low)
    } else {
        let low = low as i64;
        let high = high as i64;
        if high < low {
            0
        } else {
            high.wrapping_sub(low) as u64
        }
    };
    if width >= partitions {
        return false;
    }
    for offset in 0..=width {
        let value = if unsigned {
            Datum::UInt(low.wrapping_add(offset))
        } else {
            Datum::Int((low as i64).wrapping_add(offset as i64))
        };
        let Ok(ordinal) =
            crate::partition_routing::key_partition_index_for_tuple(&[value], partitions)
        else {
            continue;
        };
        used[ordinal] = true;
    }
    true
}

/// Go `multiColumnRangeColumnsPruner`: map every ranger tuple interval onto
/// the half-open RANGE COLUMNS partitions it intersects. The ranger has
/// already normalized the predicate into these endpoints; pruning only does
/// the two source `sort.Search` operations over the partition bounds.
fn prune_range_columns_ids(
    spec: &PartitionSpec,
    ranges: &[IndexRange],
    less_than: &[Vec<crate::partition_routing::RangeColumnBound>],
    field_types: &[tidb_datatype::FieldType],
) -> Vec<i64> {
    if less_than.len() != spec.definitions.len()
        || less_than
            .iter()
            .any(|bound| bound.len() != field_types.len())
    {
        return spec
            .definitions
            .iter()
            .map(|definition| definition.id)
            .collect();
    }

    let mut used = vec![false; spec.definitions.len()];
    for range in ranges {
        if range.low.len() > field_types.len() || range.high.len() > field_types.len() {
            return spec
                .definitions
                .iter()
                .map(|definition| definition.id)
                .collect();
        }

        let Ok(start) = first_range_columns_bound(less_than, |bound| {
            range_columns_min_cmp(bound, &range.low, field_types, range.low_exclusive)
        }) else {
            return spec
                .definitions
                .iter()
                .map(|definition| definition.id)
                .collect();
        };
        let Ok(mut end) = first_range_columns_bound(less_than, |bound| {
            range_columns_max_cmp(bound, &range.high, field_types, range.high_exclusive)
        }) else {
            return spec
                .definitions
                .iter()
                .map(|definition| definition.id)
                .collect();
        };
        if end < less_than.len() {
            end += 1;
        }
        for slot in used.iter_mut().take(end).skip(start) {
            *slot = true;
        }
    }
    spec.definitions
        .iter()
        .zip(used)
        .filter_map(|(definition, used)| used.then_some(definition.id))
        .collect()
}

fn first_range_columns_bound(
    bounds: &[Vec<crate::partition_routing::RangeColumnBound>],
    mut compare: impl FnMut(
        &[crate::partition_routing::RangeColumnBound],
    ) -> Result<bool, crate::partition_routing::RoutingError>,
) -> Result<usize, crate::partition_routing::RoutingError> {
    let mut low = 0;
    let mut high = bounds.len();
    while low < high {
        let middle = low + (high - low) / 2;
        if compare(&bounds[middle])? {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    Ok(low)
}

fn range_columns_min_cmp(
    bound: &[crate::partition_routing::RangeColumnBound],
    low: &[Datum],
    field_types: &[tidb_datatype::FieldType],
    low_exclusive: bool,
) -> Result<bool, crate::partition_routing::RoutingError> {
    for ((bound, value), field_type) in bound.iter().zip(low).zip(field_types) {
        match compare_range_column_bound(bound, value, field_type)? {
            Ordering::Greater => return Ok(true),
            Ordering::Less => return Ok(false),
            Ordering::Equal => {}
        }
    }
    if low.len() < bound.len() {
        if low_exclusive {
            return Ok(false);
        }
        if matches!(
            bound[low.len()],
            crate::partition_routing::RangeColumnBound::MaxValue
        ) {
            return Ok(true);
        }
        let field_type = &field_types[low.len()];
        if !field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL) {
            return Ok(true);
        }
        return Ok(!range_column_bound_is_type_minimum(
            &bound[low.len()],
            field_type,
        ));
    }
    Ok(false)
}

fn range_columns_max_cmp(
    bound: &[crate::partition_routing::RangeColumnBound],
    high: &[Datum],
    field_types: &[tidb_datatype::FieldType],
    high_exclusive: bool,
) -> Result<bool, crate::partition_routing::RoutingError> {
    for ((bound, value), field_type) in bound.iter().zip(high).zip(field_types) {
        match compare_range_column_bound(bound, value, field_type)? {
            Ordering::Greater => return Ok(true),
            Ordering::Less => return Ok(false),
            Ordering::Equal => {}
        }
    }
    if high.len() < bound.len()
        && matches!(
            bound[high.len()],
            crate::partition_routing::RangeColumnBound::MaxValue
        )
    {
        return Ok(true);
    }
    Ok(high_exclusive)
}

fn compare_range_column_bound(
    bound: &crate::partition_routing::RangeColumnBound,
    endpoint: &Datum,
    field_type: &tidb_datatype::FieldType,
) -> Result<Ordering, crate::partition_routing::RoutingError> {
    let crate::partition_routing::RangeColumnBound::Value(bound) = bound else {
        return Ok(Ordering::Greater);
    };
    match endpoint {
        Datum::MinNotNull => Ok(if matches!(bound, Datum::Null) {
            Ordering::Less
        } else {
            Ordering::Greater
        }),
        Datum::MaxValue => Ok(Ordering::Less),
        _ => tidb_expr::compare_datums_with_collation(bound, endpoint, field_type.collation())
            .map_err(crate::partition_routing::RoutingError::Eval),
    }
}

fn range_column_bound_is_type_minimum(
    bound: &crate::partition_routing::RangeColumnBound,
    field_type: &tidb_datatype::FieldType,
) -> bool {
    let crate::partition_routing::RangeColumnBound::Value(bound) = bound else {
        return false;
    };
    match field_type.eval_type() {
        tidb_datatype::EvalType::Int if field_type.is_unsigned() => {
            matches!(bound, Datum::UInt(0) | Datum::Int(0))
        }
        tidb_datatype::EvalType::Int => match field_type.code() {
            tidb_datatype::FieldTypeCode::Tiny
            | tidb_datatype::FieldTypeCode::Short
            | tidb_datatype::FieldTypeCode::Int24
            | tidb_datatype::FieldTypeCode::Long
            | tidb_datatype::FieldTypeCode::LongLong
            | tidb_datatype::FieldTypeCode::Enum => matches!(
                bound,
                Datum::Int(value)
                    if *value == tidb_datatype::integer_signed_lower_bound(field_type.code())
            ),
            _ => false,
        },
        tidb_datatype::EvalType::Datetime | tidb_datatype::EvalType::Timestamp => {
            matches!(bound, Datum::Time(value) if value.is_zero())
        }
        tidb_datatype::EvalType::String => bound.as_raw_bytes().is_some_and(<[u8]>::is_empty),
        _ => false,
    }
}

/// Go `ListPartitionLocation`: each partition maps to the VALUES IN tuple
/// groups still compatible with one predicate. Group `-1` is Go's special
/// DEFAULT identity, which deliberately intersects only another DEFAULT.
pub(crate) type ListPartitionLocation = BTreeMap<usize, BTreeSet<isize>>;

/// Go `ForListColumnPruning.LocatePartition`/`LocateRanges` for one LIST
/// COLUMNS component. The caller performs Go's recursive CNF/DNF traversal
/// and combines these tuple-group locations with intersection/union.
///
/// `None` means the comparison cannot safely prune and must become a full
/// scan. An empty map is a proven contradiction for this predicate.
pub(crate) fn list_column_location_for_ranges(
    ranges: &[IndexRange],
    values: &[(Vec<Datum>, usize)],
    default_partition: Option<usize>,
    field_types: &[tidb_datatype::FieldType],
    column_index: usize,
) -> Result<Option<ListPartitionLocation>, tidb_expr::EvalError> {
    let Some(field_type) = field_types.get(column_index) else {
        return Ok(None);
    };
    let mut group_counts = BTreeMap::<usize, isize>::new();
    let groups = values
        .iter()
        .map(|(tuple, ordinal)| {
            let group = group_counts.entry(*ordinal).or_default();
            let result = (tuple.get(column_index), *ordinal, *group);
            *group += 1;
            result
        })
        .collect::<Vec<_>>();
    if groups.iter().any(|(value, _, _)| value.is_none()) {
        return Ok(None);
    }

    let mut location = ListPartitionLocation::new();
    for range in ranges {
        if range.low.len() != 1 || range.high.len() != 1 {
            return Ok(None);
        }
        let point = range.is_point(true);
        let mut range_location = ListPartitionLocation::new();
        for (value, ordinal, group) in &groups {
            let Some(value) = *value else {
                return Ok(None);
            };
            let matches = if point {
                tidb_expr::compare_datums_with_collation(
                    value,
                    &range.high[0],
                    field_type.collation(),
                )? == Ordering::Equal
            } else {
                tuple_in_range(
                    std::slice::from_ref(value),
                    range,
                    std::slice::from_ref(field_type),
                )?
            };
            if matches {
                range_location.entry(*ordinal).or_default().insert(*group);
            }
        }
        if let Some(default) = default_partition {
            // Go excludes DEFAULT only for an explicitly-owned point on a
            // one-column LIST COLUMNS table. A missing point, every range,
            // and every point on a multi-column table may still be DEFAULT.
            if !point || range_location.is_empty() || field_types.len() > 1 {
                range_location.entry(default).or_default().insert(-1);
            }
        }
        union_list_partition_location(&mut location, range_location);
    }
    Ok(Some(location))
}

pub(crate) fn union_list_partition_location(
    location: &mut ListPartitionLocation,
    other: ListPartitionLocation,
) {
    for (partition, groups) in other {
        location.entry(partition).or_default().extend(groups);
    }
}

pub(crate) fn intersect_list_partition_location(
    location: &mut ListPartitionLocation,
    other: &ListPartitionLocation,
) {
    location.retain(|partition, groups| {
        let Some(other_groups) = other.get(partition) else {
            return false;
        };
        groups.retain(|group| other_groups.contains(group));
        !groups.is_empty()
    });
}

fn tuple_in_range(
    tuple: &[Datum],
    range: &IndexRange,
    field_types: &[tidb_datatype::FieldType],
) -> Result<bool, tidb_expr::EvalError> {
    let width = range.low.len().min(range.high.len()).min(tuple.len());
    if width == 0 {
        return Ok(true);
    }
    if width > field_types.len() {
        return Err(tidb_expr::EvalError::Unsupported(
            "LIST COLUMNS range wider than its field types",
        ));
    }
    let low = tuple_endpoint_order(&tuple[..width], &range.low[..width], &field_types[..width])?;
    let high = tuple_endpoint_order(&tuple[..width], &range.high[..width], &field_types[..width])?;
    let lower_ok = if range.low_exclusive {
        low == Ordering::Greater
    } else {
        low != Ordering::Less
    };
    let upper_ok = if range.high_exclusive {
        high == Ordering::Less
    } else {
        high != Ordering::Greater
    };
    Ok(lower_ok && upper_ok)
}

fn tuple_endpoint_order(
    tuple: &[Datum],
    endpoint: &[Datum],
    field_types: &[tidb_datatype::FieldType],
) -> Result<Ordering, tidb_expr::EvalError> {
    for ((value, endpoint), field_type) in tuple.iter().zip(endpoint).zip(field_types) {
        let order = match endpoint {
            Datum::MinNotNull => {
                if matches!(value, Datum::Null) {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            Datum::MaxValue => Ordering::Less,
            _ => tidb_expr::compare_datums_with_collation(value, endpoint, field_type.collation())?,
        };
        if order != Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(Ordering::Equal)
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
    ctx: &impl tidb_expr::Columns,
) -> Result<Vec<i64>, tidb_expr::EvalError> {
    let full = || {
        spec.definitions
            .iter()
            .map(|definition| definition.id)
            .collect::<Vec<_>>()
    };
    let mut used = vec![false; spec.definitions.len()];
    for range in ranges {
        if range.high.len() != spec.dependencies.len() || range.is_full() {
            return Ok(full());
        }
        if range.is_point(true) {
            let row = tidb_chunk::mutrow::MutRow::from_datums(&range.high);
            let value = spec.expr.eval(ctx, row.to_row())?;
            let (value, is_null) = list_pruning_integer(&value)?;
            let ordinal = if is_null {
                null_partition.or(default_partition)
            } else {
                values
                    .iter()
                    .find_map(|(candidate, ordinal)| (*candidate == value).then_some(*ordinal))
                    .or(default_partition)
            };
            if let Some(ordinal) = ordinal.filter(|ordinal| *ordinal < used.len()) {
                used[ordinal] = true;
            }
            continue;
        }
        if spec.expr.as_column().is_none() {
            return Ok(full());
        }
        let Some((low, high)) = scalar_interval(range) else {
            return Ok(full());
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
        if let Some(ordinal) = default_partition.filter(|ordinal| *ordinal < used.len()) {
            used[ordinal] = true;
        }
    }
    Ok(spec
        .definitions
        .iter()
        .zip(used)
        .filter_map(|(definition, used)| used.then_some(definition.id))
        .collect())
}

fn list_pruning_integer(value: &Datum) -> Result<(i64, bool), tidb_expr::EvalError> {
    Ok(match value {
        Datum::Null => (0, true),
        Datum::Int(value) => (*value, false),
        Datum::UInt(value) => (*value as i64, false),
        Datum::Bit(value) | Datum::BinaryLiteral(value) => (value.to_int().value() as i64, false),
        _ => {
            return Err(tidb_expr::EvalError::Unsupported(
                "LIST partition expression did not evaluate as integer",
            ));
        }
    })
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
    ctx: &impl tidb_expr::Columns,
) -> Vec<i64> {
    let transformed = if spec.expr.as_column().is_some() {
        ranges.to_vec()
    } else if ranges.iter().all(|range| range.is_point(true)) {
        match ranges
            .iter()
            .map(|range| evaluate_range_partition_point(spec, range, ctx))
            .collect::<Option<Vec<_>>>()
        {
            Some(ranges) => ranges,
            None => {
                return spec
                    .definitions
                    .iter()
                    .map(|definition| definition.id)
                    .collect();
            }
        }
    } else {
        let Some(mode) = range_partition_monotone_mode(&spec.expr) else {
            return spec
                .definitions
                .iter()
                .map(|definition| definition.id)
                .collect();
        };
        match ranges
            .iter()
            .map(|range| transform_monotone_range(spec, range, mode, ctx))
            .collect::<Option<Vec<_>>>()
        {
            Some(ranges) => ranges,
            None => {
                return spec
                    .definitions
                    .iter()
                    .map(|definition| definition.id)
                    .collect();
            }
        }
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
        if transformed.iter().any(|range| {
            (index == 0 && interval_includes_null(range))
                || (!interval_is_null_point(range)
                    && range_meets_partition(range, low, high, unsigned))
        }) {
            kept.push(definition.id);
        }
    }
    kept
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RangeMonotoneMode {
    Strict,
    NonStrict,
}

fn range_partition_monotone_mode(
    expression: &tidb_expr::expression::Expression,
) -> Option<RangeMonotoneMode> {
    use tidb_expr::expression::Expression;

    let Expression::ScalarFunction(function) = expression else {
        return expression.as_column().map(|_| RangeMonotoneMode::Strict);
    };
    let name = function.func_name.lowercase();
    let args = function.get_args();
    match name {
        "year" | "to_days" if matches!(args.first(), Some(Expression::Column(_))) => {
            Some(RangeMonotoneMode::NonStrict)
        }
        "unix_timestamp" | "plus" | "minus"
            if matches!(args.first(), Some(Expression::Column(_))) =>
        {
            Some(RangeMonotoneMode::Strict)
        }
        "floor" => {
            let Some(Expression::ScalarFunction(unix_timestamp)) = args.first() else {
                return None;
            };
            (unix_timestamp.func_name.lowercase() == "unix_timestamp"
                && matches!(
                    unix_timestamp.get_args().first(),
                    Some(Expression::Column(_))
                ))
            .then_some(RangeMonotoneMode::NonStrict)
        }
        "extract" => extract_partition_monotone_mode(args),
        _ => None,
    }
}

fn extract_partition_monotone_mode(
    args: &[tidb_expr::expression::Expression],
) -> Option<RangeMonotoneMode> {
    use tidb_datatype::FieldTypeCode;
    use tidb_expr::expression::Expression;

    let [Expression::Constant(unit), value] = args else {
        return None;
    };
    let unit = unit.value.as_raw_bytes()?;
    let column = match value {
        Expression::Column(column) => column,
        Expression::ScalarFunction(cast)
            if cast.func_name.lowercase() == "cast"
                && cast.get_static_type()?.code() == FieldTypeCode::Duration
                && matches!(cast.get_args().first(), Some(Expression::Column(_))) =>
        {
            cast.get_args().first()?.as_column()?
        }
        _ => return None,
    };
    let code = column.get_static_type()?.code();
    let unit = std::str::from_utf8(unit).ok()?.to_ascii_uppercase();
    let monotone = match code {
        FieldTypeCode::Date | FieldTypeCode::Datetime => {
            matches!(unit.as_str(), "YEAR" | "YEAR_MONTH")
        }
        FieldTypeCode::Duration => matches!(
            unit.as_str(),
            "HOUR" | "HOUR_MINUTE" | "HOUR_SECOND" | "HOUR_MICROSECOND"
        ),
        _ => false,
    };
    monotone.then_some(RangeMonotoneMode::NonStrict)
}

fn evaluate_range_partition_point(
    spec: &PartitionSpec,
    range: &IndexRange,
    ctx: &impl tidb_expr::Columns,
) -> Option<IndexRange> {
    if range.high.len() != spec.dependencies.len() {
        return None;
    }
    let row = tidb_chunk::mutrow::MutRow::from_datums(&range.high);
    let value = spec.expr.eval(ctx, row.to_row()).ok()?;
    range_partition_integer(value).map(|value| IndexRange {
        low: vec![value.clone()],
        high: vec![value],
        low_exclusive: false,
        high_exclusive: false,
    })
}

fn transform_monotone_range(
    spec: &PartitionSpec,
    range: &IndexRange,
    mode: RangeMonotoneMode,
    ctx: &impl tidb_expr::Columns,
) -> Option<IndexRange> {
    if range.low.len() != 1 || range.high.len() != 1 || spec.dependencies.len() != 1 {
        return None;
    }
    let low = evaluate_range_partition_endpoint(spec, &range.low[0], ctx)?;
    let high = evaluate_range_partition_endpoint(spec, &range.high[0], ctx)?;
    Some(IndexRange {
        low: vec![low],
        high: vec![high],
        low_exclusive: range.low_exclusive && mode == RangeMonotoneMode::Strict,
        high_exclusive: range.high_exclusive && mode == RangeMonotoneMode::Strict,
    })
}

fn evaluate_range_partition_endpoint(
    spec: &PartitionSpec,
    value: &Datum,
    ctx: &impl tidb_expr::Columns,
) -> Option<Datum> {
    if matches!(value, Datum::Null | Datum::MinNotNull | Datum::MaxValue) {
        return Some(value.clone());
    }
    let row = tidb_chunk::mutrow::MutRow::from_datums(std::slice::from_ref(value));
    range_partition_integer(spec.expr.eval(ctx, row.to_row()).ok()?)
}

fn range_partition_integer(value: Datum) -> Option<Datum> {
    match value {
        Datum::Null | Datum::Int(_) | Datum::UInt(_) => Some(value),
        Datum::Bit(value) | Datum::BinaryLiteral(value) => {
            Some(Datum::UInt(value.to_int().value()))
        }
        _ => None,
    }
}

/// Go `getUsedHashPartitions`. Points evaluate the complete partition
/// expression before applying the table router's conversion and modulus.
/// Non-point enumeration is restricted to Go's bare integer-column branch;
/// wider ranges keep the full scan, except BIT columns whose declared width
/// proves that only the first `2^flen` hash values can occur.
fn prune_hash_ids(
    spec: &PartitionSpec,
    ranges: &[IndexRange],
    ctx: &impl tidb_expr::Columns,
) -> Option<Vec<i64>> {
    let mut used = vec![false; spec.definitions.len()];
    for range in ranges {
        if range.is_point(true) {
            if range.high.len() != spec.dependencies.len() {
                return None;
            }
            let row = tidb_chunk::mutrow::MutRow::from_datums(&range.high);
            let Ok(value) = spec.expr.eval(ctx, row.to_row()) else {
                // Pinned Go skips a point whose partition expression cannot
                // be evaluated; another ranger point may still be usable.
                continue;
            };
            let Ok(index) = crate::partition_routing::hash_partition_index(&value, spec.num())
            else {
                continue;
            };
            used[index] = true;
            continue;
        }

        let Some(column) = spec.expr.as_column() else {
            return None;
        };
        let field_type = column.get_static_type()?;
        if field_type.eval_type() != tidb_datatype::EvalType::Int {
            return None;
        }
        if mark_short_hash_range(range, spec.num(), field_type, &mut used) {
            continue;
        }
        if field_type.code() == tidb_datatype::FieldTypeCode::Bit
            && field_type.flen() > 0
            && field_type.flen() < 13
        {
            let possible_values = 1_usize << field_type.flen();
            if possible_values < used.len() {
                used[..possible_values].fill(true);
                continue;
            }
        }
        return None;
    }
    Some(
        spec.definitions
            .iter()
            .zip(used)
            .filter_map(|(definition, used)| used.then_some(definition.id))
            .collect(),
    )
}

fn mark_short_hash_range(
    range: &IndexRange,
    partitions: u64,
    field_type: &tidb_datatype::FieldType,
    used: &mut [bool],
) -> bool {
    let (Some(low), Some(high)) = (range.low.first(), range.high.first()) else {
        return false;
    };
    let (Some(mut low), Some(mut high)) = (
        hash_pruning_integer(low, field_type),
        hash_pruning_integer(high, field_type),
    ) else {
        return false;
    };
    if range.low_exclusive {
        low = low.wrapping_add(1);
    }
    if range.high_exclusive {
        high = high.wrapping_sub(1);
    }
    let width = if field_type.is_unsigned() {
        if (high as u64) < low as u64 {
            0
        } else {
            (high as u64) - (low as u64)
        }
    } else if high < low {
        0
    } else {
        high.wrapping_sub(low) as u64
    };
    if width >= partitions {
        return false;
    }
    for offset in 0..=width {
        let value = low.wrapping_add(offset as i64);
        let index = (value % partitions as i64).unsigned_abs() as usize;
        used[index] = true;
    }
    true
}

fn hash_pruning_integer(value: &Datum, field_type: &tidb_datatype::FieldType) -> Option<i64> {
    match value {
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => None,
        Datum::Int(value) => Some(*value),
        Datum::UInt(value) => Some(*value as i64),
        Datum::Bit(value) | Datum::BinaryLiteral(value)
            if field_type.code() == tidb_datatype::FieldTypeCode::Bit =>
        {
            Some(value.to_int().value() as i64)
        }
        _ => None,
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
    // bound admits no value this partition stores. Go's `GT` branch searches
    // with `data.C+1`, because these range bounds are discrete integer values;
    // apply the same wrapping increment to an exclusive ranger endpoint.
    //
    // A low end that is not an integer -- NULL, `MinNotNull`, a string --
    // proves nothing and keeps the partition.
    // `high`/`low` are PARTITION bounds, so they carry the partitioning
    // column's signedness; the range endpoints carry their own.
    if let (Some(high), Some((value, exclusive, value_unsigned))) = (high, interval_low(range)) {
        let value = if exclusive {
            value.wrapping_add(1)
        } else {
            value
        };
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
fn interval_low(range: &IndexRange) -> Option<(i64, bool, bool)> {
    integer_endpoint(range.low.first())
        .map(|(value, unsigned)| (value, range.low_exclusive, unsigned))
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

    fn pruned_ids(spec: &PartitionSpec, ranges: &[IndexRange]) -> Option<Vec<i64>> {
        super::pruned_ids(spec, ranges, &tidb_expr::NoColumns).expect("partition pruning")
    }

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
            expr: {
                let mut column = tidb_expr::column::Column::new(
                    1,
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                );
                column.index = 0;
                tidb_expr::expression::Expression::Column(column)
            },
            dependencies: vec!["a".to_owned()],
            definitions: vec![
                PartitionDef {
                    id: 101,
                    name: "p0".to_owned(),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                    placement_policy: None,
                },
                PartitionDef {
                    id: 102,
                    name: "p1".to_owned(),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                    placement_policy: None,
                },
                PartitionDef {
                    id: 103,
                    name: "pm".to_owned(),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                    placement_policy: None,
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
                    placement_policy: None,
                },
                PartitionDef {
                    id: 202,
                    name: "pn".to_owned(),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                    placement_policy: None,
                },
                PartitionDef {
                    id: 203,
                    name: "pd".to_owned(),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                    placement_policy: None,
                },
            ],
        }
    }

    fn range_columns_table() -> PartitionSpec {
        use crate::partition_routing::RangeColumnBound::{MaxValue, Value};

        let field_types = vec![
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        ];
        PartitionSpec {
            is_empty_columns: false,
            kind: PartitionKind::RangeColumns {
                less_than: vec![
                    vec![Value(Datum::Int(1)), Value(Datum::Int(10))],
                    vec![Value(Datum::Int(2)), Value(Datum::Int(5))],
                    vec![Value(Datum::Int(2)), MaxValue],
                    vec![MaxValue, MaxValue],
                ],
                field_types: field_types.clone(),
            },
            expr_text: String::new(),
            expr: tidb_expr::expression::Expression::Constant(
                tidb_expr::expression::Constant::new(Datum::Int(0), field_types[0].clone()),
            ),
            dependencies: vec!["a".to_owned(), "b".to_owned()],
            definitions: (0..4)
                .map(|ordinal| PartitionDef {
                    id: 301 + ordinal,
                    name: format!("p{ordinal}"),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                    placement_policy: None,
                })
                .collect(),
        }
    }

    #[test]
    fn list_column_locations_keep_go_point_collation_and_default_rules() {
        let field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Varchar)
            .with_collation(tidb_datatype::Collation::Utf8Mb4GeneralCi);
        let values = vec![
            (vec![Datum::new_string("a")], 0),
            (vec![Datum::new_string("b")], 1),
        ];
        let explicit = list_column_location_for_ranges(
            &[interval(
                Datum::new_string("A"),
                false,
                Datum::new_string("A"),
                false,
            )],
            &values,
            Some(2),
            std::slice::from_ref(&field_type),
            0,
        )
        .expect("point comparison succeeds")
        .expect("a comparable point");
        assert_eq!(explicit.keys().copied().collect::<Vec<_>>(), vec![0]);

        let gap = list_column_location_for_ranges(
            &[interval(
                Datum::new_string("z"),
                false,
                Datum::new_string("z"),
                false,
            )],
            &values,
            Some(2),
            std::slice::from_ref(&field_type),
            0,
        )
        .expect("point comparison succeeds")
        .expect("a comparable point");
        assert_eq!(gap.keys().copied().collect::<Vec<_>>(), vec![2]);
    }

    fn interval(low: Datum, low_exclusive: bool, high: Datum, high_exclusive: bool) -> IndexRange {
        IndexRange {
            low: vec![low],
            high: vec![high],
            low_exclusive,
            high_exclusive,
        }
    }

    fn tuple_interval(
        low: Vec<Datum>,
        low_exclusive: bool,
        high: Vec<Datum>,
        high_exclusive: bool,
    ) -> IndexRange {
        IndexRange {
            low,
            high,
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
        // a > 9 AND a <= 10 admits only integer 10, so Go's GT `C+1`
        // search excludes p0.
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(9), true, Datum::Int(10), false)]
            ),
            Some(vec![102])
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

    #[test]
    fn range_pruning_evaluates_go_supported_partition_functions() {
        use tidb_ast::CiString;
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::{
            column::Column, constant::Constant, expression::Expression,
            scalar_function::ScalarFunction,
        };

        let mut spec = range_table();
        let field_type = FieldType::new(FieldTypeCode::LongLong);
        let mut column = Column::new(1, field_type.clone());
        column.index = 0;
        spec.expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            field_type.clone(),
            vec![
                Expression::Column(column.clone()),
                Expression::Constant(Constant::new(Datum::Int(1), field_type.clone())),
            ],
        ));

        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(9), false, Datum::Int(9), false)]
            ),
            Some(vec![102]),
            "RANGE(a + 1) must compare the evaluated value at an exact point"
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(8), false, Datum::Int(9), false)]
            ),
            Some(vec![101, 102]),
            "Go transforms both endpoints of a strictly monotone partition function"
        );

        spec.expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("mod"),
            field_type.clone(),
            vec![
                Expression::Column(column),
                Expression::Constant(Constant::new(Datum::Int(2), field_type)),
            ],
        ));
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(21), false, Datum::Int(21), false)]
            ),
            Some(vec![101]),
            "Go evaluates equality constants even for a non-monotone function"
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(20), false, Datum::Int(21), false)]
            ),
            Some(vec![101, 102, 103]),
            "Go cannot prune a non-point predicate through a non-monotone function"
        );

        let datetime_type = FieldType::new(FieldTypeCode::Datetime);
        let mut datetime_column = Column::new(1, datetime_type);
        datetime_column.index = 0;
        spec.kind = PartitionKind::Range {
            less_than: vec![
                RangeBound::Value(2007),
                RangeBound::Value(2008),
                RangeBound::MaxValue,
            ],
            unsigned: false,
        };
        spec.expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("year"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![Expression::Column(datetime_column)],
        ));
        let march_2007 = Datum::Time(
            tidb_datatype::Time::new(
                tidb_datatype::CoreTime::from_date(2007, 3, 8, 0, 0, 0, 0),
                tidb_datatype::TimeType::DateTime,
                0,
            )
            .unwrap(),
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::MinNotNull, false, march_2007, true)]
            ),
            Some(vec![101, 102]),
            "Go relaxes < to <= through a non-strict monotone function"
        );
    }

    #[test]
    fn range_columns_pruning_maps_tuple_intervals_with_gos_bound_searches() {
        let spec = range_columns_table();

        assert_eq!(
            pruned_ids(
                &spec,
                &[tuple_interval(
                    vec![Datum::Int(1), Datum::Int(10)],
                    false,
                    vec![Datum::Int(1), Datum::Int(10)],
                    false,
                )],
            ),
            Some(vec![302]),
            "a point on a LESS THAN boundary belongs to the next partition"
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[tuple_interval(
                    vec![Datum::Int(1), Datum::Int(9)],
                    false,
                    vec![Datum::Int(2), Datum::Int(5)],
                    true,
                )],
            ),
            Some(vec![301, 302]),
            "an exclusive high boundary must not add its destination partition"
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[tuple_interval(
                    vec![Datum::Int(2), Datum::Int(5)],
                    false,
                    vec![Datum::MaxValue, Datum::MaxValue],
                    false,
                )],
            ),
            Some(vec![303, 304]),
            "the lower boundary and MAXVALUE endpoint retain the final partitions"
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[
                    tuple_interval(
                        vec![Datum::Int(0), Datum::Int(0)],
                        false,
                        vec![Datum::Int(0), Datum::Int(0)],
                        false,
                    ),
                    tuple_interval(
                        vec![Datum::Int(2), Datum::Int(6)],
                        false,
                        vec![Datum::Int(2), Datum::Int(6)],
                        false,
                    ),
                ],
            ),
            Some(vec![301, 303]),
            "multiple ranger ranges union their partition spans"
        );
    }

    #[test]
    fn range_columns_pruning_uses_each_partition_columns_collation() {
        use crate::partition_routing::RangeColumnBound::{MaxValue, Value};
        use tidb_datatype::{Collation, FieldType, FieldTypeCode, StringDatum};

        let string = |text: &str, collation| {
            Datum::String(StringDatum::new(text.as_bytes().to_vec(), collation))
        };
        let mut spec = range_columns_table();
        let field_types = vec![
            FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Utf8Mb40900AiCi),
            FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Utf8Mb4UnicodeCi),
        ];
        spec.kind = PartitionKind::RangeColumns {
            less_than: vec![
                vec![
                    Value(string("i", Collation::Utf8Mb40900AiCi)),
                    Value(string("i", Collation::Utf8Mb4UnicodeCi)),
                ],
                vec![MaxValue, MaxValue],
            ],
            field_types,
        };
        spec.definitions.truncate(2);

        assert_eq!(
            pruned_ids(
                &spec,
                &[tuple_interval(
                    vec![
                        string("I", Collation::Utf8Mb40900AiCi),
                        string("I", Collation::Utf8Mb4UnicodeCi),
                    ],
                    false,
                    vec![
                        string("I", Collation::Utf8Mb40900AiCi),
                        string("I", Collation::Utf8Mb4UnicodeCi),
                    ],
                    false,
                )],
            ),
            Some(vec![302]),
            "case-insensitive equality with the bound routes to the next partition"
        );
    }

    /// LIST points keep their exact owner (DEFAULT only owns a point gap),
    /// while intervals also retain DEFAULT because they may contain gaps.
    #[test]
    fn list_pruning_matches_gos_explicit_and_default_owners() {
        let mut spec = list_table();
        let mut column = tidb_expr::column::Column::new(
            1,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        column.index = 0;
        spec.expr = tidb_expr::expression::Expression::Column(column);
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(1), false, Datum::Int(1), false)]
            ),
            Some(vec![201])
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
            Some(vec![202])
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
            Some(vec![201, 202, 203]),
            "a full ranger interval must retain every definition"
        );
    }

    #[test]
    fn list_point_pruning_evaluates_the_partition_expression() {
        use tidb_ast::CiString;
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::{
            column::Column, constant::Constant, expression::Expression,
            scalar_function::ScalarFunction,
        };

        let mut spec = list_table();
        let field_type = FieldType::new(FieldTypeCode::LongLong);
        let mut column = Column::new(1, field_type.clone());
        column.index = 0;
        spec.expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            field_type.clone(),
            vec![
                Expression::Column(column),
                Expression::Constant(Constant::new(Datum::Int(1), field_type)),
            ],
        ));

        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(0), false, Datum::Int(0), false)]
            ),
            Some(vec![201]),
            "LIST(a + 1) must locate a=0 through expression value 1"
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(1), false, Datum::Int(1), false)]
            ),
            Some(vec![203]),
            "the DEFAULT partition owns a point whose expression value is absent"
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(0), false, Datum::Int(1), false)]
            ),
            Some(vec![201, 202, 203]),
            "Go declines non-point pruning for a compound LIST expression"
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
        let mut column = tidb_expr::column::Column::new(
            1,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        column.index = 0;
        spec.expr = tidb_expr::expression::Expression::Column(column);
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

    #[test]
    fn hash_point_pruning_evaluates_the_partition_expression() {
        use tidb_ast::CiString;
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::{
            column::Column, constant::Constant, expression::Expression,
            scalar_function::ScalarFunction,
        };

        let mut spec = range_table();
        spec.kind = PartitionKind::Hash;
        let field_type = FieldType::new(FieldTypeCode::LongLong);
        let mut column = Column::new(1, field_type.clone());
        column.index = 0;
        spec.expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            field_type.clone(),
            vec![
                Expression::Column(column),
                Expression::Constant(Constant::new(Datum::Int(1), field_type)),
            ],
        ));

        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(1), false, Datum::Int(1), false)]
            ),
            Some(vec![103]),
            "HASH(a + 1) must route a=1 through expression value 2"
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(1), false, Datum::Int(2), false)]
            ),
            None,
            "Go enumerates non-point ranges only for a bare integer column"
        );
    }

    #[test]
    fn hash_bit_range_uses_the_columns_finite_value_domain() {
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::{column::Column, expression::Expression};

        let mut spec = range_table();
        spec.kind = PartitionKind::Hash;
        let mut column = Column::new(1, FieldType::new(FieldTypeCode::Bit).with_flen(1));
        column.index = 0;
        spec.expr = Expression::Column(column);

        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::MinNotNull, false, Datum::MaxValue, false,)]
            ),
            Some(vec![101, 102]),
            "BIT(1) has only hash values 0 and 1 even with three partitions"
        );
    }

    #[test]
    fn key_pruning_enumerates_short_single_integer_ranges() {
        let mut spec = range_table();
        spec.kind = PartitionKind::Key;

        let expected_for = |values: &[i64]| {
            let mut used = vec![false; spec.definitions.len()];
            for value in values {
                let ordinal = crate::partition_routing::key_partition_index_for_tuple(
                    &[Datum::Int(*value)],
                    spec.num(),
                )
                .expect("integer key routing");
                used[ordinal] = true;
            }
            spec.definitions
                .iter()
                .zip(used)
                .filter_map(|(definition, used)| used.then_some(definition.id))
                .collect::<Vec<_>>()
        };

        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(3), false, Datum::Int(4), false)]
            ),
            Some(expected_for(&[3, 4]))
        );
        assert_eq!(
            pruned_ids(&spec, &[interval(Datum::Int(0), true, Datum::Int(2), true)]),
            Some(expected_for(&[1]))
        );
        assert_eq!(
            pruned_ids(
                &spec,
                &[interval(Datum::Int(0), false, Datum::Int(3), false)]
            ),
            Some(vec![101, 102, 103]),
            "Go falls back to FullRange when interval width reaches partition count"
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
