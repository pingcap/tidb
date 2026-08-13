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

//! Which physical table one row of a partitioned table is stored in.
//!
//! This is Go `pkg/table/tables/partition.go`'s `locatePartition` family,
//! reduced to the one question the storage layer asks: given a row, which
//! partition's id does its record key carry? In Go a partition IS a physical
//! table -- `PartitionDefinition.ID` is the `t{id}` a record key is written
//! under -- and that is exactly the shape kept here, so a partitioned table
//! is one [`crate::KvTable`] whose rows are spread over N key prefixes
//! rather than one.
//!
//! # Why the unpartitioned table is not a special case
//!
//! [`PartitionSpec`] is what a table has INSTEAD of nothing; every key the
//! table writes goes through [`PartitionSpec::locate`] when it has one and
//! through the table id when it does not, which is the same statement as
//! "an unpartitioned table is a table with one partition whose id is its
//! own". The routing code therefore has no "is this partitioned" branch
//! inside it -- the branch is the `Option`, taken once at the key site.
//!
//! # NOT MODELLED here (each refused at DDL, see [`crate::ddl::table_partition`])
//!
//! Subpartitioning and every `ALTER TABLE ... PARTITION` action. HASH, KEY,
//! scalar RANGE, RANGE COLUMNS, scalar LIST, and LIST COLUMNS are routed; the
//! rest are refused.

use std::cmp::Ordering;
use std::collections::HashMap;

use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;

/// One partition's exclusive upper bound under RANGE: Go
/// `model.PartitionDefinition.LessThan[0]`, already constant-folded to the
/// integer Go stores.
///
/// Captured: `VALUES LESS THAN (5+20)` is stored -- and printed back by
/// `SHOW CREATE TABLE` -- as `25`, so the bound is a VALUE here and not an
/// expression.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeBound {
    /// A folded integer bound, read as a 64-bit pattern; whether it compares
    /// as signed or unsigned is [`PartitionKind::Range`]'s `unsigned`.
    Value(i64),
    /// `MAXVALUE`, which only the LAST partition may carry (1481).
    MaxValue,
}

/// One component of a `RANGE COLUMNS` upper-bound tuple.
///
/// `MAXVALUE` is an ordered sentinel rather than an integer stand-in: the
/// tuple `(2, MAXVALUE)` admits every `(2, x)` while `(MAXVALUE, anything)`
/// admits every row.  Keeping it in the value model makes DDL validation,
/// routing and pruning use the same lexicographic comparison.
#[derive(Clone, Debug, PartialEq)]
pub enum RangeColumnBound {
    /// A folded bound value, converted to its declared column type at DDL.
    Value(Datum),
    /// Go's `MAXVALUE`, above every concrete value in this tuple position.
    MaxValue,
}

/// The partition method a table was created with.
///
/// Go's `ast.PartitionType` has six values; this tier stores the methods it
/// can route. The rest never reach a `PartitionSpec` because DDL refuses them.
#[derive(Clone, Debug, PartialEq)]
pub enum PartitionKind {
    /// Go `ast.PartitionTypeHash`: `PARTITION BY HASH (expr) PARTITIONS n`.
    Hash,
    /// Go `ast.PartitionTypeKey`: the ordered partition columns are encoded
    /// through `Datum.ToHashKey` and fed to IEEE CRC32.
    Key,
    /// Go `ast.PartitionTypeRange` without `COLUMNS`:
    /// `PARTITION BY RANGE (expr) (PARTITION p VALUES LESS THAN (v), ...)`.
    Range {
        /// Go `ForRangePruning.LessThan`/`MaxValue`: each partition's
        /// EXCLUSIVE upper bound, in definition order and strictly
        /// increasing (1493). Always the same length as
        /// [`PartitionSpec::definitions`].
        less_than: Vec<RangeBound>,
        /// Whether the partition expression's type is unsigned, which is
        /// Go's `isPartExprUnsigned` and decides how a bound compares.
        unsigned: bool,
    },
    /// Go `PARTITION BY RANGE COLUMNS`: typed lexicographic upper bounds.
    RangeColumns {
        /// One exclusive upper-bound tuple per partition, in definition
        /// order. `MAXVALUE` is represented per component, not as a datum.
        less_than: Vec<Vec<RangeColumnBound>>,
        /// The declared type and collation for each tuple component.
        field_types: Vec<FieldType>,
    },
    /// Go `ast.PartitionTypeList` without `COLUMNS`.
    List {
        /// Folded non-NULL values and their owning partition ordinals.
        values: Vec<(i64, usize)>,
        /// The partition that explicitly owns `NULL`, if any.
        null_partition: Option<usize>,
        /// The `DEFAULT` partition, if any.
        default_partition: Option<usize>,
        /// Whether the partition expression is unsigned.
        unsigned: bool,
    },
    /// Go `ast.PartitionTypeList` with `COLUMNS`: typed, normalized tuples.
    ListColumns {
        /// Each fully converted `VALUES IN` tuple and its owner ordinal.
        values: Vec<(Vec<Datum>, usize)>,
        /// Encoded normalized tuples indexed as Go's list-columns pruner
        /// does. Routing must be a lookup, not a scan of every definition.
        keys: HashMap<Vec<u8>, usize>,
        /// The catch-all `DEFAULT` partition, if written.
        default_partition: Option<usize>,
        /// The declared types of each tuple position.
        field_types: Vec<tidb_datatype::FieldType>,
    },
}

impl PartitionKind {
    /// The method's name as `SHOW CREATE TABLE` prints it (Go
    /// `PartitionType.String()`).
    #[must_use]
    pub const fn sql(&self) -> &'static str {
        match self {
            PartitionKind::Hash => "HASH",
            PartitionKind::Key => "KEY",
            PartitionKind::Range { .. } => "RANGE",
            PartitionKind::RangeColumns { .. } => "RANGE",
            PartitionKind::List { .. } => "LIST",
            PartitionKind::ListColumns { .. } => "LIST",
        }
    }
}

/// One partition: Go `model.PartitionDefinition`, reduced to the two facts a
/// row's key and `SHOW CREATE TABLE` need.
#[derive(Clone, Debug)]
pub struct PartitionDef {
    /// Go `PartitionDefinition.ID`: the PHYSICAL table id this partition's
    /// record keys are written under.
    pub id: i64,
    /// Go `PartitionDefinition.Name`, as written (`p0`, `p1`, ...).
    pub name: String,
}

/// A table's partitioning: Go `model.PartitionInfo` plus the built expression
/// Go keeps beside it as `PartitionExpr`.
#[derive(Clone, Debug)]
pub struct PartitionSpec {
    /// The method.
    pub kind: PartitionKind,
    /// Go `PartitionInfo.Expr`: the partition expression in Go's own restored
    /// spelling, which is the text `SHOW CREATE TABLE` prints back.
    pub expr_text: String,
    /// The evaluable form, whose `Column` nodes index [`Self::dependencies`]
    /// -- the same convention a generated column uses
    /// (`crate::generated_column::GeneratedColumn::expr`), and for the same
    /// reason: an `ALTER TABLE` that moves a column must not be able to
    /// re-point the routing at a different column, which would send new rows
    /// to a different partition than the existing ones with no error.
    pub expr: Expression,
    /// Go `PartitionInfo.Columns`: the NAMES of the columns the expression
    /// reads, in the order [`Self::expr`] indexes them. A projected scan must
    /// decode these or routing would evaluate over holes.
    pub dependencies: Vec<String>,
    /// The partitions, in definition order. Never empty: Go's
    /// `checkNoHashPartitions` rejects `PARTITIONS 0` with 1504.
    pub definitions: Vec<PartitionDef>,
}

/// A row that no partition accepts, or an expression that failed to evaluate.
#[derive(Clone, Debug)]
pub enum RoutingError {
    /// Go `table.ErrNoPartitionForGivenValue` (1526): `Table has no partition
    /// for value %s`. HASH never raises it -- every value maps into
    /// `0..num` -- but RANGE and LIST will, so the error lives with the
    /// routing rather than with one method.
    NoPartitionForValue(String),
    /// Go `types.ErrOverflow` (1690), raised by the `ConvertTo(TypeLonglong)`
    /// inside `locateHashPartition`: a value above `i64::MAX` has no signed
    /// reading, so the row is REJECTED rather than routed by a clamped one.
    ///
    /// CAPTURED from real TiDB: on
    /// `create table tc (a bit(64), b int) partition by hash(a) partitions 3`,
    /// `insert into tc values(b'1111...1' /* 64 ones */, 1)` answers
    /// `[types:1690]constant 18446744073709551615 overflows bigint`.
    ValueOverflowsBigint(String),
    /// The partition expression could not be evaluated over this row.
    Eval(tidb_expr::EvalError),
    /// A LIST COLUMNS value could not be normalized into its declared field
    /// type or codec key. Stored rows normally make this unreachable.
    Conversion(String),
}

impl PartitionSpec {
    /// Go `PartitionInfo.Num`: how many partitions the table has.
    #[must_use]
    pub fn num(&self) -> u64 {
        self.definitions.len() as u64
    }

    /// The physical table ids this table's rows can live under, ascending.
    ///
    /// Ids are allocated as one contiguous ascending block at `CREATE TABLE`,
    /// so this is also the key ORDER the partitions' record ranges appear in.
    #[must_use]
    pub fn physical_ids(&self) -> Vec<i64> {
        self.definitions.iter().map(|def| def.id).collect()
    }

    /// The partition named `name`, matched case-insensitively as MySQL
    /// matches partition names.
    #[must_use]
    pub fn definition_named(&self, name: &str) -> Option<&PartitionDef> {
        self.definitions
            .iter()
            .find(|def| def.name.eq_ignore_ascii_case(name))
    }

    /// Go `locatePartition`: the physical table id `row` belongs in.
    ///
    /// `columns` is the table's CURRENT column list, which is where the
    /// dependency names become offsets; `ctx` is the statement's evaluation
    /// context, so the expression is evaluated under the SQL mode of the
    /// statement that writes the row.
    ///
    /// # Errors
    ///
    /// [`RoutingError::Eval`] when the partition expression fails.
    pub fn locate<S: crate::generated_column::GeneratedColumnSlot>(
        &self,
        row: &[Datum],
        columns: &[S],
        ctx: &impl tidb_expr::Columns,
    ) -> Result<i64, RoutingError> {
        let index = self.locate_index(row, columns, ctx)?;
        Ok(self.definitions[index].id)
    }

    /// [`PartitionSpec::locate`] as the partition's ORDINAL -- Go's
    /// `GetPartitionIdxByRow`, the number `EXPLAIN`'s access object indexes
    /// `Partition.Definitions` with.
    ///
    /// # Errors
    ///
    /// The same [`RoutingError`]s [`PartitionSpec::locate`] raises.
    pub fn locate_ordinal<S: crate::generated_column::GeneratedColumnSlot>(
        &self,
        row: &[Datum],
        columns: &[S],
        ctx: &impl tidb_expr::Columns,
    ) -> Result<usize, RoutingError> {
        self.locate_index(row, columns, ctx)
    }

    /// [`PartitionSpec::locate`] as the partition's ORDINAL, which is what
    /// the per-method rules are written in terms of.
    fn locate_index<S: crate::generated_column::GeneratedColumnSlot>(
        &self,
        row: &[Datum],
        columns: &[S],
        ctx: &impl tidb_expr::Columns,
    ) -> Result<usize, RoutingError> {
        match &self.kind {
            PartitionKind::Key => key_partition_index(row, columns, &self.dependencies, self.num()),
            PartitionKind::RangeColumns {
                less_than,
                field_types,
            } => range_columns_partition_index(
                row,
                columns,
                &self.dependencies,
                field_types,
                less_than,
                ctx,
            ),
            PartitionKind::ListColumns {
                keys,
                default_partition,
                field_types,
                ..
            } => list_columns_partition_index(
                row,
                columns,
                &self.dependencies,
                field_types,
                keys,
                *default_partition,
                ctx,
            ),
            kind => {
                let value = crate::generated_column::eval_over_dependencies(
                    &self.expr,
                    &self.dependencies,
                    columns,
                    row,
                    ctx,
                )
                .map_err(RoutingError::Eval)?;
                match kind {
                    PartitionKind::Hash => hash_partition_index(&value, self.num()),
                    PartitionKind::Key => unreachable!("matched above"),
                    PartitionKind::Range {
                        less_than,
                        unsigned,
                    } => range_partition_index(&value, less_than, *unsigned),
                    PartitionKind::RangeColumns { .. } => unreachable!("matched above"),
                    PartitionKind::List {
                        values,
                        null_partition,
                        default_partition,
                        unsigned,
                    } => list_partition_index(
                        &value,
                        values,
                        *null_partition,
                        *default_partition,
                        *unsigned,
                    ),
                    PartitionKind::ListColumns { .. } => unreachable!("matched above"),
                }
            }
        }
    }
}

/// Go `ForKeyPruning.LocateKeyPartition`: hash the stored values in partition
/// column order.  This intentionally does not run `convert_to_in`; Go hashes
/// the row datum after normal table coercion, so converting a second time can
/// change a collation key or turn a stored value into a warning.
fn key_partition_index<S: crate::generated_column::GeneratedColumnSlot>(
    row: &[Datum],
    columns: &[S],
    names: &[String],
    num: u64,
) -> Result<usize, RoutingError> {
    let mut tuple = Vec::with_capacity(names.len());
    for name in names {
        let offset = crate::generated_column::offset_of(columns, name).ok_or_else(|| {
            RoutingError::Conversion(format!("partition column '{name}' no longer exists"))
        })?;
        tuple.push(row.get(offset).cloned().unwrap_or(Datum::Null));
    }
    key_partition_index_for_tuple(&tuple, num)
}

/// The pure KEY router is shared by write routing and exact point pruning.
pub(crate) fn key_partition_index_for_tuple(
    tuple: &[Datum],
    num: u64,
) -> Result<usize, RoutingError> {
    debug_assert!(num > 0, "DDL rejects PARTITIONS 0 before routing");
    let mut hash = crc32fast::Hasher::new();
    for value in tuple {
        if value.is_null() {
            hash.update(&[0]);
        } else {
            let key = value
                .to_hash_key()
                .map_err(|error| RoutingError::Conversion(error.to_string()))?;
            hash.update(&key);
        }
    }
    Ok((hash.finalize() as u64 % num) as usize)
}

fn range_columns_partition_index<S: crate::generated_column::GeneratedColumnSlot>(
    row: &[Datum],
    columns: &[S],
    names: &[String],
    field_types: &[FieldType],
    less_than: &[Vec<RangeColumnBound>],
    ctx: &impl tidb_expr::Columns,
) -> Result<usize, RoutingError> {
    let tuple = partition_column_tuple(row, columns, names, field_types, ctx)?;
    range_columns_partition_index_for_tuple(&tuple, less_than, field_types)
}

/// The one conversion boundary shared by LIST COLUMNS and RANGE COLUMNS.
/// A stored table row reaches this through the same declared types that DDL
/// used to fold its definition values, so the comparison never mixes an
/// untyped literal with a stored column datum.
fn partition_column_tuple<S: crate::generated_column::GeneratedColumnSlot>(
    row: &[Datum],
    columns: &[S],
    names: &[String],
    field_types: &[FieldType],
    ctx: &impl tidb_expr::Columns,
) -> Result<Vec<Datum>, RoutingError> {
    let mut tuple = Vec::with_capacity(names.len());
    for (name, field_type) in names.iter().zip(field_types) {
        let offset = crate::generated_column::offset_of(columns, name).ok_or_else(|| {
            RoutingError::Conversion(format!("partition column '{name}' no longer exists"))
        })?;
        let converted = row
            .get(offset)
            .unwrap_or(&Datum::Null)
            .convert_to_in(
                field_type,
                tidb_datatype::ConversionFlags::default(),
                &ctx.time_zone(),
            )
            .map_err(|error| RoutingError::Conversion(error.to_string()))?;
        if converted.event.is_some() {
            return Err(RoutingError::Conversion(format!(
                "partition column '{name}' does not convert exactly"
            )));
        }
        tuple.push(converted.value);
    }
    Ok(tuple)
}

pub(crate) fn range_columns_partition_index_for_tuple(
    tuple: &[Datum],
    less_than: &[Vec<RangeColumnBound>],
    field_types: &[FieldType],
) -> Result<usize, RoutingError> {
    // Go's upper-bound expression returns SQL NULL for a NULL partition
    // value; locateRangeColumnPartition treats that as the lowest partition.
    if tuple.iter().any(Datum::is_null) {
        return Ok(0);
    }
    for (ordinal, bound) in less_than.iter().enumerate() {
        if range_columns_tuple_cmp(tuple, bound, field_types)? == Ordering::Less {
            return Ok(ordinal);
        }
    }
    Err(RoutingError::NoPartitionForValue(
        "from column_list".to_owned(),
    ))
}

pub(crate) fn range_columns_tuple_cmp(
    tuple: &[Datum],
    bound: &[RangeColumnBound],
    field_types: &[FieldType],
) -> Result<Ordering, RoutingError> {
    if tuple.len() != bound.len() || tuple.len() != field_types.len() {
        return Err(RoutingError::Conversion(
            "RANGE COLUMNS tuple arity changed".to_owned(),
        ));
    }
    for ((value, bound), field_type) in tuple.iter().zip(bound).zip(field_types) {
        match bound {
            RangeColumnBound::MaxValue => return Ok(Ordering::Less),
            RangeColumnBound::Value(bound) => {
                let order =
                    tidb_expr::compare_datums_with_collation(value, bound, field_type.collation())
                        .map_err(RoutingError::Eval)?;
                if order != Ordering::Equal {
                    return Ok(order);
                }
            }
        }
    }
    Ok(Ordering::Equal)
}

fn list_columns_partition_index<S: crate::generated_column::GeneratedColumnSlot>(
    row: &[Datum],
    columns: &[S],
    names: &[String],
    field_types: &[tidb_datatype::FieldType],
    keys: &HashMap<Vec<u8>, usize>,
    default_partition: Option<usize>,
    ctx: &impl tidb_expr::Columns,
) -> Result<usize, RoutingError> {
    let tuple = partition_column_tuple(row, columns, names, field_types, ctx)?;
    let key = tidb_codec::encode_key_in_timezone(&ctx.time_zone(), &tuple)
        .map_err(|error| RoutingError::Conversion(error.to_string()))?;
    if let Some(ordinal) = keys.get(&key) {
        return Ok(*ordinal);
    }
    default_partition
        .ok_or_else(|| RoutingError::NoPartitionForValue("from column_list".to_owned()))
}

/// Go `ForListPruning.LocatePartition`: an exact folded-value match, then
/// `NULL`/`DEFAULT`, otherwise 1526.
fn list_partition_index(
    value: &Datum,
    values: &[(i64, usize)],
    null_partition: Option<usize>,
    default_partition: Option<usize>,
    unsigned: bool,
) -> Result<usize, RoutingError> {
    let bits = match value {
        Datum::Int(value) => *value,
        Datum::UInt(value) => *value as i64,
        Datum::Null => {
            return null_partition
                .or(default_partition)
                .ok_or_else(|| RoutingError::NoPartitionForValue("NULL".to_owned()))
        }
        other => return Err(RoutingError::NoPartitionForValue(format!("{other:?}"))),
    };
    if let Some((_, ordinal)) = values.iter().find(|(candidate, _)| *candidate == bits) {
        return Ok(*ordinal);
    }
    default_partition.ok_or_else(|| {
        RoutingError::NoPartitionForValue(if unsigned {
            format!("{}", bits as u64)
        } else {
            format!("{bits}")
        })
    })
}

/// Go `locateRangePartition`: the ordinal a range-partitioned row lands in.
///
/// The rule is `sort.Search` for the FIRST partition whose exclusive upper
/// bound is strictly greater than the value, with two edges Go states
/// explicitly and this unit captured:
///
/// * NULL goes to the LOWEST partition -- `pos = 0` unconditionally, even
///   when that partition's bound would exclude any real value. Captured on a
///   table with no `MAXVALUE` at all: `INSERT ... VALUES (NULL, 1)` succeeds
///   and `PARTITION (p0)` returns the row.
/// * a value past the last bound has NO partition, which is
///   `ErrNoPartitionForGivenValue` (1526) rather than a silent drop.
///   Captured: `25` into bounds `10, 20` is rejected.
///
/// `MAXVALUE` is greater than every value, so it always terminates the
/// search; 1481 keeps it in the last position, which is why a bound BELOW it
/// is never skipped.
fn range_partition_index(
    value: &Datum,
    less_than: &[RangeBound],
    unsigned: bool,
) -> Result<usize, RoutingError> {
    let bits = match value {
        Datum::Int(value) => *value,
        Datum::UInt(value) => *value as i64,
        // NULL -- and anything else the expression produced, which DDL's
        // integer rule (1659/1697) leaves as NULL only -- takes the lowest
        // partition.
        _ => return Ok(0),
    };
    let found = less_than
        .iter()
        .position(|bound| range_bound_exceeds(*bound, bits, unsigned));
    found.ok_or_else(|| {
        RoutingError::NoPartitionForValue(if unsigned {
            format!("{}", bits as u64)
        } else {
            format!("{bits}")
        })
    })
}

/// Whether a partition's exclusive upper bound admits `value`, which is Go's
/// `ranges.Compare(i, ret, unsigned) > 0`.
fn range_bound_exceeds(bound: RangeBound, value: i64, unsigned: bool) -> bool {
    match bound {
        RangeBound::MaxValue => true,
        RangeBound::Value(bound) if unsigned => (bound as u64) > (value as u64),
        RangeBound::Value(bound) => bound > value,
    }
}

/// Go `locateHashPartition`: the ordinal a hash-partitioned row lands in.
///
/// Captured from real TiDB (`partition by hash(a) partitions 4`): `0 -> p0`,
/// `1 -> p1`, `3 -> p3`, `4 -> p0`, `-1 -> p1`, `-3 -> p3`, `-7 -> p3`,
/// `NULL -> p0`. So the rule is `|v| mod n` with NULL treated as zero -- Go's
/// `ret = ret % numParts; if ret < 0 { ret = -ret }`, and its `isNull`
/// early-return of `0`.
///
/// The value is read as its 64-bit PATTERN, not as a number: Go's `EvalInt`
/// hands an unsigned column's value back as `int64`, which is why
/// `18446744073709551615` (bit pattern `-1`) lands in `p1` and
/// `9223372036854775809` (pattern `-9223372036854775807`) lands in `p3` --
/// both captured.
///
/// `-(i64::MIN)` cannot overflow here because `v % n` already lies strictly
/// between `-n` and `n`, and `n` is at most Go's 8192 partitions.
///
/// # A `BIT` column is NOT already an integer
///
/// Go's column fast path reads the datum's kind and CONVERTS anything that is
/// not `KindInt64`/`KindUint64` with `ConvertTo(TypeLonglong)` before taking
/// the modulus. `BIT(n)` is an admitted partition column and its value
/// arrives as [`Datum::Bit`], so treating a non-integer kind as zero routes
/// EVERY row of such a table into `p0` -- a silent wrong answer, since
/// `SELECT ... PARTITION (p1)` then returns nothing. CAPTURED from real TiDB
/// on `bit(8) ... partition by hash(a) partitions 3` holding `0,1,2,3`:
/// `p0` reads `0,3`, `p1` reads `1`, `p2` reads `2` -- plain `v mod 3` over
/// the bits' integer value.
///
/// # Errors
///
/// [`RoutingError::ValueOverflowsBigint`] when the conversion has no exact
/// signed reading, which is Go returning `ConvertTo`'s error from
/// `locateHashPartition` and failing the write.
pub(crate) fn hash_partition_index(value: &Datum, num: u64) -> Result<usize, RoutingError> {
    let bits = match value {
        Datum::Int(value) => *value,
        Datum::UInt(value) => *value as i64,
        // Go's `ConvertTo` returns NULL unchanged and `GetInt64` reads it as
        // 0, which is the captured `NULL -> p0`.
        Datum::Null => 0,
        other => {
            let rejected = || RoutingError::ValueOverflowsBigint(overflow_text(other));
            let converted = other
                .convert_to(
                    &tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                    tidb_datatype::ConversionFlags::from_bits(0),
                )
                .map_err(|_| rejected())?;
            if converted.event.is_some() {
                return Err(rejected());
            }
            match converted.value {
                Datum::Int(value) => value,
                _ => 0,
            }
        }
    };
    Ok((bits % num as i64).unsigned_abs() as usize)
}

/// The value as Go's `types.overflow` prints it inside 1690.
///
/// A `BIT` value has no signed reading to print -- the whole reason it
/// overflowed -- so it is named by the UNSIGNED integer its bits spell, which
/// is what the capture shows (`constant 18446744073709551615 overflows
/// bigint`, not `-1`).
fn overflow_text(value: &Datum) -> String {
    match value {
        Datum::Bit(literal) | Datum::BinaryLiteral(literal) => literal.to_int().value().to_string(),
        other => format!("{other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every hash routing this unit captured from real TiDB, as the rule
    /// rather than as the SQL: `|v| mod n`, NULL to the first partition.
    #[test]
    fn hash_routing_matches_the_captured_go_answers() {
        for (value, expected) in [
            (0_i64, 0_usize),
            (1, 1),
            (3, 3),
            (4, 0),
            (-1, 1),
            (-3, 3),
            (-4, 0),
            (-7, 3),
            (i64::MIN, 0),
        ] {
            assert_eq!(
                hash_partition_index(&Datum::Int(value), 4).unwrap(),
                expected,
                "hash({value}) over 4 partitions"
            );
        }
        assert_eq!(hash_partition_index(&Datum::Null, 4).unwrap(), 0);
        // The unsigned captures, which only agree if the value is read as a
        // 64-bit pattern: u64::MAX is -1, and 9223372036854775809 is
        // -9223372036854775807.
        assert_eq!(hash_partition_index(&Datum::UInt(u64::MAX), 4).unwrap(), 1);
        assert_eq!(
            hash_partition_index(&Datum::UInt(9_223_372_036_854_775_809), 4).unwrap(),
            3
        );
    }

    /// A `BIT` column's value is CONVERTED before the modulus, which is Go's
    /// `locateHashPartition` column path.
    ///
    /// CAPTURED from real TiDB on `create table tb (a bit(8), b int)
    /// partition by hash(a) partitions 3` holding `(0,0),(1,1),(2,2),(3,3)`:
    /// `select b from tb partition (p0)` reads `0,3`, `(p1)` reads `1`,
    /// `(p2)` reads `2`. Reading the datum's kind and defaulting to zero --
    /// what this did before -- puts all four in `p0` and makes `partition
    /// (p1)` return nothing.
    #[test]
    fn a_bit_column_routes_by_its_integer_value() {
        for (bits, expected) in [(0_u8, 0_usize), (1, 1), (2, 2), (3, 0)] {
            let value = Datum::Bit(tidb_datatype::BinaryLiteral::from_uint(
                u64::from(bits),
                None,
            ));
            assert_eq!(
                hash_partition_index(&value, 3).unwrap(),
                expected,
                "hash(bit {bits}) over 3 partitions"
            );
        }
    }

    /// A `BIT(64)` value above `i64::MAX` has no signed reading, so Go's
    /// `ConvertTo` errors and the row is REJECTED.
    ///
    /// CAPTURED from real TiDB: `insert into tc values(b'1{64}', 1)` on
    /// `bit(64) ... partition by hash(a) partitions 3` answers
    /// `[types:1690]constant 18446744073709551615 overflows bigint`.
    #[test]
    fn a_bit_value_with_no_signed_reading_is_rejected() {
        let value = Datum::Bit(tidb_datatype::BinaryLiteral::from_uint(u64::MAX, None));
        match hash_partition_index(&value, 3) {
            Err(RoutingError::ValueOverflowsBigint(text)) => {
                assert_eq!(text, "18446744073709551615");
            }
            other => panic!("expected 1690, got {other:?}"),
        }
    }

    /// Every RANGE routing this unit captured from real TiDB, as the rule.
    ///
    /// The capture is `partition by range(a) (p0 < 10, p1 < 20, pm <
    /// MAXVALUE)` over `-1, 5, 9, 10, 19, 20, 100, NULL`, read back one
    /// partition at a time with `SELECT ... PARTITION (p)`. `9` and `10`
    /// straddle the first boundary, which is the off-by-one this asserts.
    #[test]
    fn range_routing_matches_the_captured_go_answers() {
        let bounds = [
            RangeBound::Value(10),
            RangeBound::Value(20),
            RangeBound::MaxValue,
        ];
        for (value, expected) in [
            (-1_i64, 0_usize),
            (5, 0),
            (9, 0),
            (10, 1),
            (19, 1),
            (20, 2),
            (100, 2),
            (i64::MAX, 2),
        ] {
            assert_eq!(
                range_partition_index(&Datum::Int(value), &bounds, false).expect("routed"),
                expected,
                "range({value}) over 10/20/MAXVALUE"
            );
        }
        assert_eq!(
            range_partition_index(&Datum::Null, &bounds, false).expect("routed"),
            0,
            "NULL takes the lowest partition"
        );
    }

    /// Without `MAXVALUE` a value past the last bound has NO partition, and
    /// NULL still lands in the lowest one. Both captured: `INSERT (25)` is
    /// rejected while `INSERT (NULL)` succeeds and reads back from `p0`.
    #[test]
    fn a_range_table_without_maxvalue_refuses_the_value_it_cannot_place() {
        let bounds = [RangeBound::Value(10), RangeBound::Value(20)];
        assert!(matches!(
            range_partition_index(&Datum::Int(25), &bounds, false),
            Err(RoutingError::NoPartitionForValue(ref value)) if value == "25"
        ));
        assert_eq!(
            range_partition_index(&Datum::Null, &bounds, false).expect("routed"),
            0
        );
        assert_eq!(
            range_partition_index(&Datum::Int(5), &bounds, false).expect("routed"),
            0
        );
    }

    /// An unsigned partition expression compares its bounds as unsigned, so
    /// a value whose 64-bit pattern is negative is ABOVE every signed bound
    /// rather than below it.
    #[test]
    fn an_unsigned_range_compares_its_bounds_unsigned() {
        let bounds = [RangeBound::Value(10), RangeBound::MaxValue];
        assert_eq!(
            range_partition_index(&Datum::UInt(u64::MAX), &bounds, true).expect("routed"),
            1
        );
        // The same 64-bit pattern read as SIGNED is -1, which is below 10.
        assert_eq!(
            range_partition_index(&Datum::UInt(u64::MAX), &bounds, false).expect("routed"),
            0
        );
    }

    /// A partition count that is not a power of two takes the same rule; this
    /// is the `hash(a+b) partitions 3` capture (`1+1 -> p2`, `1+2 -> p0`,
    /// `-1 + -1 -> p2`).
    #[test]
    fn hash_routing_is_not_a_power_of_two_special_case() {
        assert_eq!(hash_partition_index(&Datum::Int(2), 3).unwrap(), 2);
        assert_eq!(hash_partition_index(&Datum::Int(3), 3).unwrap(), 0);
        assert_eq!(hash_partition_index(&Datum::Int(-2), 3).unwrap(), 2);
    }
}
