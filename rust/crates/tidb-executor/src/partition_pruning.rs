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
use tidb_hack::GoCaseCompare;

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
        .filter(|def| names.iter().any(|name| name.go_lower_eq(&def.name)))
        .map(|def| def.id)
        .collect())
}

/// The physical ids a scan restricted by `ranges` over the partition
/// expression's pruning columns must read, or `None` when nothing can be
/// pruned. RANGE maps evaluated expression intervals onto definition bounds;
/// HASH maps point and short integer intervals through Go's pruning rules.
///
/// `ranger_ranges` are the ranger's own intervals over Go's pruning columns,
/// including their per-column collators. `None` back means "read everything",
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
/// below asks. Unlike an executor scan range, the ranger range carries the
/// comparison collator for every tuple position; RANGE COLUMNS pruning must
/// retain that metadata through `minCmp`/`maxCmp`.
pub fn pruned_ids_from_ranger(
    spec: &PartitionSpec,
    ranger_ranges: &[tidb_planner::ranger::types::Range],
    ctx: &impl tidb_expr::Columns,
) -> Result<Option<Vec<i64>>, tidb_expr::EvalError> {
    let ranges = ranger_ranges
        .iter()
        .map(|range| IndexRange {
            low: range.low_val.clone(),
            high: range.high_val.clone(),
            low_exclusive: range.low_exclude,
            high_exclusive: range.high_exclude,
        })
        .collect::<Vec<_>>();
    let collations = ranger_ranges
        .iter()
        .map(|range| range.collators.clone())
        .collect::<Vec<_>>();
    pruned_ids_impl(spec, &ranges, &collations, ctx)
}

fn pruned_ids_impl(
    spec: &PartitionSpec,
    ranges: &[IndexRange],
    range_collations: &[Vec<tidb_datatype::Collation>],
    ctx: &impl tidb_expr::Columns,
) -> Result<Option<Vec<i64>>, tidb_expr::EvalError> {
    Ok(match &spec.kind {
        // Every row is in partition 0, so there is nothing to prune away.
        PartitionKind::None => None,
        PartitionKind::Hash => prune_hash_ids(spec, ranges, ctx),
        PartitionKind::Key => Some(prune_key_ids(spec, ranges)),
        // Go prunes `PARTITION BY RANGE` by the statement's predicates, not by
        // ranger intervals: see [`scalar_range_pruned_ids`].
        PartitionKind::Range { .. } => None,
        PartitionKind::RangeColumns {
            less_than,
            field_types,
        } => Some(prune_range_columns_ids(
            spec,
            ranges,
            less_than,
            field_types,
            range_collations,
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
    range_collations: &[Vec<tidb_datatype::Collation>],
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
    for (range_index, range) in ranges.iter().enumerate() {
        if range.low.len() > field_types.len() || range.high.len() > field_types.len() {
            return spec
                .definitions
                .iter()
                .map(|definition| definition.id)
                .collect();
        }
        let Some(collations) = range_collations
            .get(range_index)
            .filter(|collations| collations.len() >= range.low.len().max(range.high.len()))
        else {
            return spec
                .definitions
                .iter()
                .map(|definition| definition.id)
                .collect();
        };

        let Ok(start) = first_range_columns_bound(less_than, |bound| {
            range_columns_min_cmp(
                bound,
                &range.low,
                field_types,
                collations.as_slice(),
                range.low_exclusive,
            )
        }) else {
            return spec
                .definitions
                .iter()
                .map(|definition| definition.id)
                .collect();
        };
        let Ok(mut end) = first_range_columns_bound(less_than, |bound| {
            range_columns_max_cmp(
                bound,
                &range.high,
                collations.as_slice(),
                range.high_exclusive,
            )
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
    collations: &[tidb_datatype::Collation],
    low_exclusive: bool,
) -> Result<bool, crate::partition_routing::RoutingError> {
    for ((bound, value), collation) in bound.iter().zip(low).zip(collations) {
        match compare_range_column_bound(bound, value, *collation)? {
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
    collations: &[tidb_datatype::Collation],
    high_exclusive: bool,
) -> Result<bool, crate::partition_routing::RoutingError> {
    for ((bound, value), collation) in bound.iter().zip(high).zip(collations) {
        match compare_range_column_bound(bound, value, *collation)? {
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
    collation: tidb_datatype::Collation,
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
        _ => tidb_expr::compare_datums_with_collation(bound, endpoint, collation)
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum RangeMonotoneMode {
    Strict,
    NonStrict,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_routing::PartitionDef;

    fn pruned_ids(spec: &PartitionSpec, ranges: &[IndexRange]) -> Option<Vec<i64>> {
        let field_types = match &spec.kind {
            PartitionKind::RangeColumns { field_types, .. } => Some(field_types.as_slice()),
            _ => None,
        };
        let ranger_ranges = ranges
            .iter()
            .map(|range| {
                let width = range.low.len().max(range.high.len());
                tidb_planner::ranger::types::Range {
                    low_val: range.low.clone(),
                    high_val: range.high.clone(),
                    collators: field_types.map_or_else(
                        || vec![tidb_datatype::Collation::Binary; width],
                        |field_types| {
                            field_types
                                .iter()
                                .take(width)
                                .map(tidb_datatype::FieldType::collation)
                                .collect()
                        },
                    ),
                    low_exclude: range.low_exclusive,
                    high_exclude: range.high_exclusive,
                }
            })
            .collect::<Vec<_>>();
        super::pruned_ids_from_ranger(spec, &ranger_ranges, &tidb_expr::NoColumns)
            .expect("partition pruning")
    }

    fn range_table() -> PartitionSpec {
        PartitionSpec {
            overlapping_dropping_partition_indices: Vec::new(),
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
                    storage_class: Default::default(),
                },
                PartitionDef {
                    id: 102,
                    name: "p1".to_owned(),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                    placement_policy: None,
                    storage_class: Default::default(),
                },
                PartitionDef {
                    id: 103,
                    name: "pm".to_owned(),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                    placement_policy: None,
                    storage_class: Default::default(),
                },
            ],
        }
    }

    fn range_table_with_bounds(bounds: Vec<RangeBound>) -> PartitionSpec {
        let field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        let mut column = tidb_expr::column::Column::new(1, field_type);
        column.index = 0;
        PartitionSpec {
            overlapping_dropping_partition_indices: Vec::new(),
            is_empty_columns: false,
            kind: PartitionKind::Range {
                unsigned: false,
                less_than: bounds.clone(),
            },
            expr_text: "`a`".to_owned(),
            expr: tidb_expr::expression::Expression::Column(column),
            dependencies: vec!["a".to_owned()],
            definitions: (0..bounds.len())
                .map(|ordinal| PartitionDef {
                    id: ordinal as i64,
                    name: format!("p{ordinal}"),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                    placement_policy: None,
                    storage_class: Default::default(),
                })
                .collect(),
        }
    }

    fn list_table() -> PartitionSpec {
        PartitionSpec {
            overlapping_dropping_partition_indices: Vec::new(),
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
                    storage_class: Default::default(),
                },
                PartitionDef {
                    id: 202,
                    name: "pn".to_owned(),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                    placement_policy: None,
                    storage_class: Default::default(),
                },
                PartitionDef {
                    id: 203,
                    name: "pd".to_owned(),
                    less_than: Vec::new(),
                    in_values: Vec::new(),
                    comment: String::new(),
                    placement_policy: None,
                    storage_class: Default::default(),
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
            overlapping_dropping_partition_indices: Vec::new(),
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
                    storage_class: Default::default(),
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

    fn part_column(code: tidb_datatype::FieldTypeCode) -> tidb_expr::column::Column {
        let mut column = tidb_expr::column::Column::new(1, tidb_datatype::FieldType::new(code));
        column.id = 1;
        column.index = 0;
        column
    }

    /// `name(column, value)`, or `name(value, column)` when `flipped`.
    fn condition(
        name: &str,
        column: &tidb_expr::column::Column,
        value: Datum,
        flipped: bool,
    ) -> tidb_expr::expression::Expression {
        use tidb_expr::expression::Expression;
        // A time constant has the column's own time type, as Go's comparison
        // cast leaves it.
        let value_type = match &value {
            Datum::Time(_) => column.ret_type.clone().expect("a typed column"),
            _ => tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        };
        let mut args = vec![
            Expression::Column(column.clone()),
            Expression::Constant(tidb_expr::constant::Constant::new(value, value_type)),
        ];
        if flipped {
            args.reverse();
        }
        Expression::ScalarFunction(tidb_expr::scalar_function::ScalarFunction::new(
            tidb_ast::CiString::new(name),
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            args,
        ))
    }

    fn cond(name: &str, column: &tidb_expr::column::Column, value: i64) -> tidb_expr::expression::Expression {
        condition(name, column, Datum::Int(value), false)
    }

    fn is_null(column: &tidb_expr::column::Column) -> tidb_expr::expression::Expression {
        tidb_expr::expression::Expression::ScalarFunction(
            tidb_expr::scalar_function::ScalarFunction::new(
                tidb_ast::CiString::new("isnull"),
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                vec![tidb_expr::expression::Expression::Column(column.clone())],
            ),
        )
    }

    fn logic(name: &str, args: Vec<tidb_expr::expression::Expression>) -> tidb_expr::expression::Expression {
        tidb_expr::expression::Expression::ScalarFunction(
            tidb_expr::scalar_function::ScalarFunction::new(
                tidb_ast::CiString::new(name),
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                args,
            ),
        )
    }

    fn range_pruned(
        spec: &PartitionSpec,
        column: &tidb_expr::column::Column,
        conditions: &[tidb_expr::expression::Expression],
    ) -> Option<Vec<i64>> {
        scalar_range_pruned_ids(spec, conditions, column, &tidb_expr::NoColumns)
    }

    /// The captured queries, as the partitions each must read: `a < 10` is
    /// `p0` alone, `a = 10` is `p1` alone, `a >= 20` is `pm` alone.
    #[test]
    fn range_pruning_keeps_exactly_the_partitions_the_predicate_can_match() {
        let spec = range_table();
        let a = part_column(tidb_datatype::FieldTypeCode::LongLong);
        assert_eq!(range_pruned(&spec, &a, &[cond("lt", &a, 10)]), Some(vec![101]));
        assert_eq!(range_pruned(&spec, &a, &[cond("eq", &a, 10)]), Some(vec![102]));
        assert_eq!(range_pruned(&spec, &a, &[cond("ge", &a, 20)]), Some(vec![103]));
        // a >= 9 AND a <= 20 spans all three.
        assert_eq!(
            range_pruned(&spec, &a, &[cond("ge", &a, 9), cond("le", &a, 20)]),
            Some(vec![101, 102, 103])
        );
        // a > 9 AND a <= 10 admits only integer 10, so Go's GT `C+1`
        // search excludes p0.
        assert_eq!(
            range_pruned(&spec, &a, &[cond("gt", &a, 9), cond("le", &a, 10)]),
            Some(vec![102])
        );
        assert_eq!(range_pruned(&spec, &a, &[is_null(&a)]), Some(vec![101]));
    }

    /// Go keeps every partition for a predicate its pruner does not know:
    /// `a != 10`, `a not in (...)`, and an OR with an unknown branch.
    #[test]
    fn range_pruning_ignores_predicates_go_cannot_prune_by() {
        let spec = range_table();
        let a = part_column(tidb_datatype::FieldTypeCode::LongLong);
        let all = Some(vec![101, 102, 103]);
        assert_eq!(range_pruned(&spec, &a, &[cond("ne", &a, 10)]), all);
        assert_eq!(
            range_pruned(
                &spec,
                &a,
                &[logic("or", vec![cond("ne", &a, 10), cond("lt", &a, 5)])]
            ),
            all
        );
    }

    /// The boundary rows: `a = 9` and `a = 10` must not land in the same
    /// partition, and `a = 19`/`a = 20` must not either.
    #[test]
    fn the_boundary_values_prune_to_different_partitions() {
        let spec = range_table();
        let a = part_column(tidb_datatype::FieldTypeCode::LongLong);
        for (value, expected) in [(9_i64, 101_i64), (10, 102), (19, 102), (20, 103)] {
            assert_eq!(
                range_pruned(&spec, &a, &[cond("eq", &a, value)]),
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
            constant::Constant, expression::Expression, scalar_function::ScalarFunction,
        };

        let mut spec = range_table();
        let field_type = FieldType::new(FieldTypeCode::LongLong);
        let a = part_column(FieldTypeCode::LongLong);
        spec.expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            field_type.clone(),
            vec![
                Expression::Column(a.clone()),
                Expression::Constant(Constant::new(Datum::Int(1), field_type.clone())),
            ],
        ));
        assert_eq!(
            range_pruned(&spec, &a, &[cond("eq", &a, 9)]),
            Some(vec![102]),
            "RANGE(a + 1) must compare the evaluated value at an exact point"
        );
        assert_eq!(
            range_pruned(&spec, &a, &[cond("ge", &a, 8), cond("le", &a, 9)]),
            Some(vec![101, 102]),
            "Go evaluates each bound through a strictly monotone partition function"
        );

        spec.expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("mod"),
            field_type.clone(),
            vec![
                Expression::Column(a.clone()),
                Expression::Constant(Constant::new(Datum::Int(2), field_type)),
            ],
        ));
        assert_eq!(
            range_pruned(&spec, &a, &[cond("eq", &a, 21)]),
            Some(vec![101]),
            "Go evaluates equality constants even for a non-monotone function"
        );
        assert_eq!(
            range_pruned(&spec, &a, &[cond("ge", &a, 20), cond("le", &a, 21)]),
            Some(vec![101, 102, 103]),
            "Go cannot prune a non-point predicate through a non-monotone function"
        );

        let datetime = part_column(FieldTypeCode::Datetime);
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
            vec![Expression::Column(datetime.clone())],
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
            range_pruned(
                &spec,
                &datetime,
                &[condition("lt", &datetime, march_2007, false)]
            ),
            Some(vec![101, 102]),
            "Go relaxes < to <= through a non-strict monotone function"
        );
    }

    #[test]
    fn range_pruning_matches_go_monotone_datetime_and_timestamp_cases() {
        use tidb_ast::CiString;
        use tidb_datatype::{CoreTime, FieldType, FieldTypeCode, Time, TimeType};
        use tidb_expr::{expression::Expression, scalar_function::ScalarFunction};

        let datetime_column = part_column(FieldTypeCode::Datetime);
        let mut datetime_spec =
            range_table_with_bounds(vec![RangeBound::Value(733_108), RangeBound::Value(733_132)]);
        datetime_spec.expr_text = "to_days(`d`)".to_owned();
        datetime_spec.expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("to_days"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![Expression::Column(datetime_column.clone())],
        ));
        datetime_spec.dependencies = vec!["d".to_owned()];
        let datetime = |year, month, day| {
            Datum::Time(
                Time::new(
                    CoreTime::from_date(year, month, day, 0, 0, 0, 0),
                    TimeType::DateTime,
                    0,
                )
                .unwrap(),
            )
        };
        assert_eq!(
            range_pruned(
                &datetime_spec,
                &datetime_column,
                &[condition("lt", &datetime_column, datetime(2000, 3, 8), false)],
            ),
            Some(vec![0])
        );
        assert_eq!(
            range_pruned(
                &datetime_spec,
                &datetime_column,
                &[condition("gt", &datetime_column, datetime(2018, 3, 8), false)],
            ),
            Some(Vec::new())
        );

        let timestamp_column = part_column(FieldTypeCode::Timestamp);
        let mut timestamp_spec = range_table_with_bounds(vec![
            RangeBound::Value(1_199_145_600),
            RangeBound::Value(1_207_008_000),
            RangeBound::Value(1_262_304_000),
            RangeBound::MaxValue,
        ]);
        timestamp_spec.expr_text = "unix_timestamp(`report_updated`)".to_owned();
        timestamp_spec.expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("unix_timestamp"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![Expression::Column(timestamp_column.clone())],
        ));
        timestamp_spec.dependencies = vec!["report_updated".to_owned()];
        let may_2008 = Datum::Time(
            Time::new(
                CoreTime::from_date(2008, 5, 1, 0, 0, 0, 0),
                TimeType::Timestamp,
                0,
            )
            .unwrap(),
        );
        assert_eq!(
            range_pruned(
                &timestamp_spec,
                &timestamp_column,
                &[condition("gt", &timestamp_column, may_2008, false)],
            ),
            Some(vec![2, 3])
        );
    }

    /// Go `TestPartitionRangeForExpr` (`rule_partition_pruning_test.go:243`).
    #[test]
    fn range_pruning_matches_go_partition_range_for_expr_matrix() {
        let spec = range_table_with_bounds(vec![
            RangeBound::Value(4),
            RangeBound::Value(7),
            RangeBound::Value(11),
            RangeBound::Value(14),
            RangeBound::Value(17),
            RangeBound::MaxValue,
        ]);
        let a = part_column(tidb_datatype::FieldTypeCode::LongLong);
        let cases: Vec<(&str, Vec<tidb_expr::expression::Expression>, Vec<i64>)> = vec![
            ("a < 2 and a > 10", vec![cond("lt", &a, 2), cond("gt", &a, 10)], vec![]),
            ("a > 3", vec![cond("gt", &a, 3)], vec![1, 2, 3, 4, 5]),
            ("a < 3", vec![cond("lt", &a, 3)], vec![0]),
            ("a >= 11", vec![cond("ge", &a, 11)], vec![3, 4, 5]),
            ("a > 11", vec![cond("gt", &a, 11)], vec![3, 4, 5]),
            ("a < 11", vec![cond("lt", &a, 11)], vec![0, 1, 2]),
            ("a = 16", vec![cond("eq", &a, 16)], vec![4]),
            ("a > 66", vec![cond("gt", &a, 66)], vec![5]),
            (
                "a > 2 and a < 10",
                vec![cond("gt", &a, 2), cond("lt", &a, 10)],
                vec![0, 1, 2],
            ),
            (
                "a < 2 or a >= 15",
                vec![logic("or", vec![cond("lt", &a, 2), cond("ge", &a, 15)])],
                vec![0, 4, 5],
            ),
            ("a is null", vec![is_null(&a)], vec![0]),
            (
                "12 > a",
                vec![condition("gt", &a, Datum::Int(12), true)],
                vec![0, 1, 2, 3],
            ),
            (
                "4 <= a",
                vec![condition("le", &a, Datum::Int(4), true)],
                vec![1, 2, 3, 4, 5],
            ),
        ];
        for (predicate, conditions, expected) in cases {
            assert_eq!(range_pruned(&spec, &a, &conditions), Some(expected), "{predicate}");
        }
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

    #[test]
    fn range_columns_pruning_uses_the_ranger_interval_collator() {
        use crate::partition_routing::RangeColumnBound::{MaxValue, Value};
        use tidb_datatype::{Collation, FieldType, FieldTypeCode};

        let field_type =
            FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Utf8Mb4GeneralCi);
        let mut spec = range_columns_table();
        spec.kind = PartitionKind::RangeColumns {
            less_than: vec![vec![Value(Datum::new_string("b"))], vec![MaxValue]],
            field_types: vec![field_type],
        };
        spec.definitions.truncate(2);
        let range = tidb_planner::ranger::types::Range {
            low_val: vec![Datum::new_string("B")],
            high_val: vec![Datum::new_string("B")],
            collators: vec![Collation::Utf8Mb4Bin],
            low_exclude: false,
            high_exclude: false,
        };

        assert_eq!(
            pruned_ids_from_ranger(&spec, &[range], &tidb_expr::NoColumns).unwrap(),
            Some(vec![301]),
            "Go minCmp/maxCmp compare this binary-collation point before the lowercase bound"
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

/// Go `PartitionRange`: the half-open definition-index interval
/// `[start, end)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PartitionRange {
    start: usize,
    end: usize,
}

/// Go `PartitionRangeOR`: an OR of [`PartitionRange`]s.
#[derive(Clone, Debug, PartialEq, Eq)]
struct PartitionRangeOr(Vec<PartitionRange>);

impl PartitionRangeOr {
    /// Go `GetFullRange`.
    fn full(end: usize) -> Self {
        Self(vec![PartitionRange { start: 0, end }])
    }

    /// Go `IntersectionRange`.
    fn intersection_range(self, start: usize, end: usize) -> Self {
        Self(
            self.0
                .into_iter()
                .filter_map(|range| {
                    let (new_start, new_end) = (range.start.max(start), range.end.min(end));
                    (new_end > new_start).then_some(PartitionRange {
                        start: new_start,
                        end: new_end,
                    })
                })
                .collect(),
        )
    }

    /// Go `Union`.
    fn union(mut self, other: Self) -> Self {
        self.0.extend(other.0);
        self.simplify()
    }

    /// Go `simplify`: sort by start and merge overlapping ranges.
    fn simplify(mut self) -> Self {
        if self.0.is_empty() {
            return self;
        }
        self.0.sort_by_key(|range| range.start);
        let mut merged: Vec<PartitionRange> = Vec::with_capacity(self.0.len());
        for current in self.0 {
            match merged.last_mut() {
                Some(last) if current.start <= last.end => last.end = last.end.max(current.end),
                _ => merged.push(current),
            }
        }
        Self(merged)
    }

    /// Go `Intersection`.
    fn intersection(self, other: Self) -> Self {
        if let [only] = self.0.as_slice() {
            return other.intersection_range(only.start, only.end);
        }
        if let [only] = other.0.as_slice() {
            return self.intersection_range(only.start, only.end);
        }
        let (wide, narrow) = if self.0.len() > other.0.len() {
            (self, other)
        } else {
            (other, self)
        };
        let mut result = Vec::with_capacity(narrow.0.len());
        for range in narrow.0 {
            result.extend(wide.clone().intersection_range(range.start, range.end).0);
        }
        Self(result).simplify()
    }
}

/// Go `sort.Search`: the smallest index in `[0, n)` where `predicate`
/// holds, or `n`.
fn go_sort_search(n: usize, mut predicate: impl FnMut(usize) -> bool) -> usize {
    let (mut low, mut high) = (0, n);
    while low < high {
        let middle = low + (high - low) / 2;
        if predicate(middle) {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    low
}

/// Go `RangeColumnsPruner` for a table partitioned by ONE column. Go prunes
/// that shape by its predicates rather than through the ranger
/// (`PartitionRangeForCNFExpr`, `rule_partition_processor.go:1372`): the
/// ranger does not yet handle a predicate whose collation differs from the
/// column's, which `partitionRangeForExpr` must refuse to prune.
struct SingleRangeColumnsPruner<'a, B, C> {
    less_than: &'a [Vec<crate::partition_routing::RangeColumnBound>],
    bound_type: &'a tidb_datatype::FieldType,
    column: &'a tidb_expr::column::Column,
    builder: &'a B,
    ctx: &'a C,
}

impl<B, C> SingleRangeColumnsPruner<'_, B, C>
where
    B: tidb_expr::expr_util::builder::FunctionBuilder,
    C: tidb_expr::Columns,
{
    fn full(&self) -> PartitionRangeOr {
        PartitionRangeOr::full(self.less_than.len())
    }

    fn is_part_col(&self, expr: &tidb_expr::expression::Expression) -> bool {
        matches!(expr, tidb_expr::expression::Expression::Column(column) if column.id == self.column.id)
    }

    /// Go `PartitionRangeForCNFExpr`.
    fn range_for_cnf(
        &self,
        exprs: &[tidb_expr::expression::Expression],
        mut result: PartitionRangeOr,
    ) -> PartitionRangeOr {
        for expr in exprs {
            result = self.range_for_expr(expr, result);
        }
        result
    }

    /// Go `PartitionRangeForExpr`.
    fn range_for_expr(
        &self,
        expr: &tidb_expr::expression::Expression,
        result: PartitionRangeOr,
    ) -> PartitionRangeOr {
        if let tidb_expr::expression::Expression::ScalarFunction(op) = expr {
            match op.func_name.lowercase() {
                "and" => return self.range_for_cnf(&op.args, result),
                "or" if op.args.len() == 2 => {
                    // Go `partitionRangeForOrExpr`.
                    let left = self.range_for_expr(&op.args[0], self.full());
                    let right = self.range_for_expr(&op.args[1], self.full());
                    return result.intersection(left.union(right));
                }
                "in" => return result.intersection(self.range_for_in(&op.args)),
                _ => {}
            }
        }
        match self.partition_range_for_expr(expr) {
            Some((start, end)) => result.intersection_range(start, end),
            None => result,
        }
    }

    /// Go `partitionRangeColumnForInExpr`.
    fn range_for_in(&self, args: &[tidb_expr::expression::Expression]) -> PartitionRangeOr {
        let Some(column) = args.first().filter(|first| self.is_part_col(first)) else {
            return self.full();
        };
        let mut result = PartitionRangeOr(Vec::new());
        for arg in &args[1..] {
            let tidb_expr::expression::Expression::Constant(constant) = arg else {
                return self.full();
            };
            // "for safety, only support string,int and datetime now"
            match &constant.value {
                Datum::Int(_) | Datum::UInt(_) | Datum::Time(_) | Datum::Bytes(_) | Datum::String(_) => {}
                Datum::Null => continue,
                _ => return self.full(),
            }
            let Ok(eq) = self.builder.new_function("eq", None, vec![column.clone(), arg.clone()]) else {
                return self.full();
            };
            let Some((start, end)) = self.partition_range_for_expr(&eq) else {
                return self.full();
            };
            result.0.push(PartitionRange { start, end });
        }
        result.simplify()
    }

    /// Go `RangeColumnsPruner.partitionRangeForExpr`; `None` is Go's
    /// `ok == false`.
    fn partition_range_for_expr(
        &self,
        expr: &tidb_expr::expression::Expression,
    ) -> Option<(usize, usize)> {
        let tidb_expr::expression::Expression::ScalarFunction(op) = expr else {
            return None;
        };
        let length = self.less_than.len();
        let mut op_name = match op.func_name.lowercase() {
            name @ ("eq" | "lt" | "gt" | "le" | "ge" | "nulleq") => name,
            "isnull" => {
                // NULL sorts before every value: the first partition.
                return op.args.first().filter(|arg| self.is_part_col(arg)).map(|_| (0, 1));
            }
            _ => return None,
        };
        let [left, right] = op.args.as_slice() else {
            return None;
        };
        let constant = match (left, right) {
            (tidb_expr::expression::Expression::Column(_), tidb_expr::expression::Expression::Constant(constant)) => constant,
            (tidb_expr::expression::Expression::Constant(constant), tidb_expr::expression::Expression::Column(_)) => {
                // Go `opposite`.
                op_name = match op_name {
                    "lt" => "gt",
                    "gt" => "lt",
                    "le" => "ge",
                    "ge" => "le",
                    same => same,
                };
                constant
            }
            _ => return Some((0, length)),
        };
        let column = if matches!(left, tidb_expr::expression::Expression::Column(_)) { left } else { right };
        if !self.is_part_col(column) {
            return None;
        }
        if op_name == "nulleq" {
            if constant.value.is_null() {
                return Some((0, 1));
            }
            op_name = "eq";
        }
        // "If different collation, we can only prune if the expression is
        // binary collation and an EQ": otherwise every partition stays. Go
        // reads the comparison's own collation (`expr.CharsetAndCollation()`);
        // a comparison rebuilt over numbers or times derives none and keeps
        // its result type's.
        let expr_collation = op.derived_collation().name();
        let column_collation = self
            .column
            .ret_type
            .as_ref()
            .map_or("", tidb_datatype::FieldType::collation_name);
        if expr_collation != column_collation
            && (op_name != "eq" || !tidb_datatype::is_bin_collation(expr_collation))
        {
            return Some((0, length));
        }
        Some(self.prune_use_binary_search(op_name, constant))
    }

    /// Go `RangeColumnsPruner.pruneUseBinarySearch`.
    fn prune_use_binary_search(
        &self,
        op_name: &str,
        constant: &tidb_expr::constant::Constant,
    ) -> (usize, usize) {
        let length = self.less_than.len();
        let mut failed = false;
        let mut is_null = false;
        let mut compare = |ith: usize, op: &str| -> bool {
            let crate::partition_routing::RangeColumnBound::Value(bound) = &self.less_than[ith][0]
            else {
                return true;
            };
            let bound = tidb_expr::expression::Expression::Constant(
                tidb_expr::constant::Constant::new(bound.clone(), self.bound_type.clone()),
            );
            // Go `NewFunctionBase` (no constant folding), then
            // `expr.SetCharsetAndCollation(partition column's)`: the
            // comparison runs under the column's collation.
            let mut ret_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
            if let Some(column_type) = self.column.ret_type.as_ref() {
                ret_type.set_charset_name(column_type.charset_name());
                ret_type.set_collation_name(column_type.collation_name());
            }
            let function = tidb_expr::expression::Expression::ScalarFunction(
                tidb_expr::scalar_function::ScalarFunction::new(
                    tidb_ast::CiString::new(op),
                    ret_type,
                    vec![bound, tidb_expr::expression::Expression::Constant(constant.clone())],
                ),
            );
            match tidb_expr::eval_expression_once(&function, self.ctx) {
                Ok(Datum::Null) => {
                    is_null = true;
                    false
                }
                Ok(value) => {
                    is_null = false;
                    matches!(value, Datum::Int(v) if v > 0) || matches!(value, Datum::UInt(v) if v > 0)
                }
                Err(_) => {
                    failed = true;
                    true
                }
            }
        };
        let (start, mut end) = match op_name {
            "eq" => {
                let position = go_sort_search(length, |i| compare(i, "gt"));
                (position, position + 1)
            }
            "lt" => (0, go_sort_search(length, |i| compare(i, "ge")) + 1),
            "ge" | "gt" => (go_sort_search(length, |i| compare(i, "gt")), length),
            "le" => (0, go_sort_search(length, |i| compare(i, "gt")) + 1),
            _ => (0, length),
        };
        if failed || is_null {
            return (0, length);
        }
        end = end.min(length);
        (start, end)
    }
}

/// Go `LessThanDataInt` with the pruned column's own signedness.
struct ScalarRangePruner<'a, C> {
    less_than: &'a [RangeBound],
    /// Go `mysql.HasUnsignedFlag(col.GetStaticType().GetFlag())`.
    unsigned: bool,
    column: &'a tidb_expr::column::Column,
    /// Go `RangePruner.PartFn`: the partition expression when it is a
    /// function of the column, `None` for a bare column.
    part_fn: Option<&'a tidb_expr::expression::Expression>,
    /// Go `RangePruner.Monotonous`; `None` is `MonotoneModeInvalid`.
    monotone: Option<RangeMonotoneMode>,
    ctx: &'a C,
}

/// One `f(x) op const` condition, Go `DataForPrune`.
struct DataForPrune {
    op: &'static str,
    value: i64,
    unsigned: bool,
}

impl<C: tidb_expr::Columns> ScalarRangePruner<'_, C> {
    fn full(&self) -> PartitionRangeOr {
        PartitionRangeOr::full(self.less_than.len())
    }

    fn is_part_col(&self, expr: &tidb_expr::expression::Expression) -> bool {
        matches!(expr, tidb_expr::expression::Expression::Column(column) if column.id == self.column.id)
    }

    /// Go `PartitionRangeForCNFExpr`.
    fn range_for_cnf(
        &self,
        exprs: &[tidb_expr::expression::Expression],
        mut result: PartitionRangeOr,
    ) -> PartitionRangeOr {
        for expr in exprs {
            result = self.range_for_expr(expr, result);
        }
        result
    }

    /// Go `PartitionRangeForExpr`.
    fn range_for_expr(
        &self,
        expr: &tidb_expr::expression::Expression,
        result: PartitionRangeOr,
    ) -> PartitionRangeOr {
        if let tidb_expr::expression::Expression::ScalarFunction(op) = expr {
            match op.func_name.lowercase() {
                "and" => return self.range_for_cnf(&op.args, result),
                "or" if op.args.len() == 2 => {
                    // Go `partitionRangeForOrExpr`.
                    let left = self.range_for_expr(&op.args[0], self.full());
                    let right = self.range_for_expr(&op.args[1], self.full());
                    return result.intersection(left.union(right));
                }
                "in" => return result.intersection(self.range_for_in(&op.args)),
                _ => {}
            }
        }
        match self.partition_range_for_expr(expr) {
            Some((start, end)) => result.intersection_range(start, end),
            None => result,
        }
    }

    /// Go `partitionRangeForInExpr`.
    fn range_for_in(&self, args: &[tidb_expr::expression::Expression]) -> PartitionRangeOr {
        use tidb_expr::expression::Expression;
        if !args.first().is_some_and(|first| self.is_part_col(first)) {
            return self.full();
        }
        let mut result = PartitionRangeOr(Vec::new());
        for arg in &args[1..] {
            let Expression::Constant(constant) = arg else {
                return self.full();
            };
            if constant.value.is_null() {
                continue;
            }
            let evaluated = match self.part_fn {
                // Go `replaceColumnWithConst`: fn(col) becomes fn(const).
                Some(part_fn) => {
                    let replaced = replace_column_with_const(part_fn, arg);
                    self.eval_int(&replaced)
                }
                None => self.eval_int(arg),
            };
            // An error is Go's `return pruner.fullRange()`; a NULL
            // value keeps the zero Go reads from `EvalInt`.
            let Some((value, unsigned)) = evaluated else {
                return self.full();
            };
            let (start, end) = self.prune_use_binary_search(&DataForPrune {
                op: "eq",
                value: value.unwrap_or(0),
                unsigned,
            });
            result.0.push(PartitionRange { start, end });
        }
        result.simplify()
    }

    /// Go `RangePruner.partitionRangeForExpr`; `None` is Go's `ok == false`.
    fn partition_range_for_expr(
        &self,
        expr: &tidb_expr::expression::Expression,
    ) -> Option<(usize, usize)> {
        if let tidb_expr::expression::Expression::Constant(constant) = expr {
            // A constant false expression.
            if matches!(constant.value, Datum::Int(0) | Datum::UInt(0)) {
                return Some((0, 0));
            }
        }
        let data = self.extract_data_for_prune(expr)?;
        Some(self.prune_use_binary_search(&data))
    }

    /// Go `RangePruner.extractDataForPrune`: the expression has to be
    /// `f(x) op const`.
    fn extract_data_for_prune(
        &self,
        expr: &tidb_expr::expression::Expression,
    ) -> Option<DataForPrune> {
        use tidb_expr::expression::Expression;
        let Expression::ScalarFunction(op) = expr else {
            return None;
        };
        let mut op_name = match op.func_name.lowercase() {
            "eq" => "eq",
            "lt" => "lt",
            "gt" => "gt",
            "le" => "le",
            "ge" => "ge",
            "nulleq" => "nulleq",
            "isnull" => {
                return match op.args.first() {
                    Some(arg) if self.is_part_col(arg) => Some(DataForPrune {
                        op: "isnull",
                        value: 0,
                        unsigned: false,
                    }),
                    _ => None,
                };
            }
            _ => return None,
        };
        let [left, right] = op.args.as_slice() else {
            return None;
        };
        let constant = if self.is_part_col(left) {
            matches!(right, Expression::Constant(_)).then_some(right)
        } else if self.is_part_col(right) {
            matches!(left, Expression::Constant(_)).then(|| {
                // Go `opposite`.
                op_name = match op_name {
                    "lt" => "gt",
                    "gt" => "lt",
                    "le" => "ge",
                    "ge" => "le",
                    same => same,
                };
                left
            })
        } else {
            None
        }?;
        // Current expression is `col op const`.
        let evaluated = match self.part_fn {
            Some(part_fn) => {
                // If the partition function is not monotone, only EQ
                // condition can be pruning.
                if self.monotone.is_none() && op_name != "eq" {
                    return None;
                }
                // When the PartFn is not strict monotonous, `<` relaxes to
                // `<=` and `>` to `>=`: `col < '2020-02-11 17:34:11'` does
                // not imply `to_days(col) < to_days(2020-02-11 17:34:11)`.
                if self.monotone == Some(RangeMonotoneMode::NonStrict) {
                    op_name = match op_name {
                        "lt" => "le",
                        "gt" => "ge",
                        same => same,
                    };
                }
                self.eval_int(&replace_column_with_const(part_fn, constant))?
            }
            None => self.eval_int(constant)?,
        };
        match evaluated {
            (Some(value), unsigned) => Some(DataForPrune {
                op: if op_name == "nulleq" { "eq" } else { op_name },
                value,
                unsigned,
            }),
            // Mark it as IsNull, which `prune_use_binary_search` handles.
            (None, _) if op_name == "nulleq" => Some(DataForPrune {
                op: "isnull",
                value: 0,
                unsigned: false,
            }),
            (None, _) => None,
        }
    }

    /// Go `Expression.EvalInt` over an empty row: the value (`None` for SQL
    /// NULL) and whether the expression's type is unsigned; `None` is Go's
    /// error.
    fn eval_int(&self, expr: &tidb_expr::expression::Expression) -> Option<(Option<i64>, bool)> {
        let unsigned = expr
            .static_type()
            .is_some_and(tidb_datatype::FieldType::is_unsigned);
        match tidb_expr::eval_expression_once(expr, self.ctx).ok()? {
            Datum::Null => Some((None, unsigned)),
            Datum::Int(value) => Some((Some(value), unsigned)),
            Datum::UInt(value) => Some((Some(value as i64), unsigned)),
            _ => None,
        }
    }

    /// Go `PruneUseBinarySearch` (`lessThan.compare` with MAXVALUE greater
    /// than everything).
    fn prune_use_binary_search(&self, data: &DataForPrune) -> (usize, usize) {
        let length = self.less_than.len();
        let compare = |ith: usize, value: i64| -> Ordering {
            match self.less_than[ith] {
                RangeBound::MaxValue => Ordering::Greater,
                RangeBound::Value(bound) => compare_int(bound, self.unsigned, value, data.unsigned),
            }
        };
        let (start, end) = match data.op {
            "eq" => {
                let position = go_sort_search(length, |i| compare(i, data.value) == Ordering::Greater);
                (position, position + 1)
            }
            "lt" => {
                let position = go_sort_search(length, |i| compare(i, data.value) != Ordering::Less);
                (0, position + 1)
            }
            "ge" => (
                go_sort_search(length, |i| compare(i, data.value) == Ordering::Greater),
                length,
            ),
            // Although `value + 1` may overflow, the answer stays correct.
            "gt" => (
                go_sort_search(length, |i| {
                    compare(i, data.value.wrapping_add(1)) == Ordering::Greater
                }),
                length,
            ),
            "le" => {
                let position = go_sort_search(length, |i| compare(i, data.value) == Ordering::Greater);
                (0, position + 1)
            }
            "isnull" => (0, 1),
            _ => (0, length),
        };
        (start, end.min(length))
    }
}

/// Go `replaceColumnWithConst`: `fn(col)` becomes `fn(const)`.
fn replace_column_with_const(
    part_fn: &tidb_expr::expression::Expression,
    constant: &tidb_expr::expression::Expression,
) -> tidb_expr::expression::Expression {
    use tidb_expr::expression::Expression;
    let mut replaced = part_fn.clone();
    let Expression::ScalarFunction(function) = &mut replaced else {
        return replaced;
    };
    // The partition function may be floor(unix_timestamp(ts)) instead of a
    // simple fn(col).
    if function.func_name.lowercase() == "floor" {
        if let Some(Expression::ScalarFunction(inner)) = function.args.first_mut() {
            if inner.func_name.lowercase() == "unix_timestamp" {
                inner.args[0] = constant.clone();
                return replaced;
            }
        }
    } else if function.func_name.lowercase() == "extract" {
        // `extract(unit, col)` carries the column second, possibly under the
        // time-to-duration cast Go adds.
        if let Some(Expression::ScalarFunction(cast)) = function.args.get_mut(1) {
            if is_time_as_duration_cast(cast) {
                cast.args[0] = constant.clone();
                return replaced;
            }
        }
        function.args[1] = constant.clone();
        return replaced;
    }
    function.args[0] = constant.clone();
    replaced
}

/// Go `expr.Function.PbCode() == tipb.ScalarFuncSig_CastTimeAsDuration`.
fn is_time_as_duration_cast(function: &tidb_expr::scalar_function::ScalarFunction) -> bool {
    function.func_name.lowercase().starts_with("cast")
        && function
            .get_static_type()
            .is_some_and(|ty| ty.code() == tidb_datatype::FieldTypeCode::Duration)
        && function.args.len() == 1
}

/// Go `MakePartitionByFnCol` for the pruned column: whether `spec.expr` is a
/// bare column or a function of it, and the monotone mode Go assigns.
/// `None` is Go's `col == nil` (no pruning).
fn partition_fn_of_column(
    spec: &PartitionSpec,
) -> Option<(Option<&tidb_expr::expression::Expression>, Option<RangeMonotoneMode>)> {
    use tidb_expr::expression::Expression;
    let function = match &spec.expr {
        Expression::Column(_) => return Some((None, None)),
        Expression::ScalarFunction(function) => function,
        _ => return None,
    };
    let args = function.get_args();
    match function.func_name.lowercase() {
        "floor" => {
            // Special handling for floor(unix_timestamp(ts)).
            if let Some(Expression::ScalarFunction(inner)) = args.first() {
                if inner.func_name.lowercase() == "unix_timestamp" {
                    if let [Expression::Column(_)] = inner.get_args() {
                        return Some((Some(&spec.expr), Some(RangeMonotoneMode::NonStrict)));
                    }
                }
            }
        }
        "extract" => {
            if let [Expression::Constant(unit), value] = args {
                let column = match value {
                    Expression::Column(column) => Some(column),
                    Expression::ScalarFunction(cast) if is_time_as_duration_cast(cast) => {
                        cast.get_args().first().and_then(Expression::as_column)
                    }
                    _ => None,
                };
                if let (Some(column), Some(unit)) = (column, unit.value.as_raw_bytes()) {
                    let unit = std::str::from_utf8(unit).ok()?;
                    use tidb_datatype::FieldTypeCode::{Date, Datetime, Duration};
                    match column.get_static_type().map(tidb_datatype::FieldType::code) {
                        // Only YEAR and YEAR_MONTH are monotonic; the rest
                        // wrap around.
                        Some(Date | Datetime) => {
                            let monotone = matches!(unit, "YEAR" | "YEAR_MONTH");
                            return Some((
                                Some(&spec.expr),
                                monotone.then_some(RangeMonotoneMode::NonStrict),
                            ));
                        }
                        Some(Duration) => {
                            let monotone = matches!(
                                unit,
                                "HOUR" | "HOUR_MINUTE" | "HOUR_SECOND" | "HOUR_MICROSECOND"
                            );
                            return Some((
                                Some(&spec.expr),
                                monotone.then_some(RangeMonotoneMode::NonStrict),
                            ));
                        }
                        _ => {}
                    }
                }
            }
        }
        _ => {}
    }
    // `fn(col, ...)`: one column argument, and it is the first parameter.
    if spec.dependencies.len() != 1 || !matches!(args.first(), Some(Expression::Column(_))) {
        return None;
    }
    let monotone = match function.func_name.lowercase() {
        "year" | "to_days" => Some(RangeMonotoneMode::NonStrict),
        // `unix_timestamp` is strict; plus and minus only when the form is
        // fn(column, const).
        "unix_timestamp" | "plus" | "minus" => Some(RangeMonotoneMode::Strict),
        _ => None,
    };
    Some((Some(&spec.expr), monotone))
}

/// Go `PruneRangePartition` for `PARTITION BY RANGE (expr)`: the physical ids
/// `conditions` can reach, or `None` when Go cannot prune this table.
pub(crate) fn scalar_range_pruned_ids(
    spec: &PartitionSpec,
    conditions: &[tidb_expr::expression::Expression],
    column: &tidb_expr::column::Column,
    ctx: &impl tidb_expr::Columns,
) -> Option<Vec<i64>> {
    let PartitionKind::Range { less_than, .. } = &spec.kind else {
        return None;
    };
    if less_than.len() != spec.definitions.len() {
        return None;
    }
    let (part_fn, monotone) = partition_fn_of_column(spec)?;
    let pruner = ScalarRangePruner {
        less_than,
        unsigned: column
            .ret_type
            .as_ref()
            .is_some_and(tidb_datatype::FieldType::is_unsigned),
        column,
        part_fn,
        monotone,
        ctx,
    };
    let ranges = pruner.range_for_cnf(conditions, pruner.full());
    Some(
        ranges
            .0
            .iter()
            .flat_map(|range| spec.definitions[range.start..range.end].iter().map(|definition| definition.id))
            .collect(),
    )
}

/// Go `pruneRangeColumnsPartition` for a single-column RANGE COLUMNS table:
/// the physical ids `conditions` can reach.
pub(crate) fn single_range_columns_pruned_ids(
    spec: &PartitionSpec,
    conditions: &[tidb_expr::expression::Expression],
    column: &tidb_expr::column::Column,
    builder: &impl tidb_expr::expr_util::builder::FunctionBuilder,
    ctx: &impl tidb_expr::Columns,
) -> Option<Vec<i64>> {
    let PartitionKind::RangeColumns {
        less_than,
        field_types,
    } = &spec.kind
    else {
        return None;
    };
    let [bound_type] = field_types.as_slice() else {
        return None;
    };
    if less_than.len() != spec.definitions.len() || less_than.iter().any(|bound| bound.len() != 1) {
        return None;
    }
    let pruner = SingleRangeColumnsPruner {
        less_than,
        bound_type,
        column,
        builder,
        ctx,
    };
    let ranges = pruner.range_for_cnf(conditions, pruner.full());
    Some(
        ranges
            .0
            .iter()
            .flat_map(|range| spec.definitions[range.start..range.end].iter().map(|definition| definition.id))
            .collect(),
    )
}
