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
//! RANGE, LIST and KEY partitioning, subpartitioning, and every `ALTER TABLE
//! ... PARTITION` action. Only HASH is routed, so only HASH is accepted.

use tidb_datatype::{Datum, FieldType};
use tidb_expr::expression::Expression;

/// The partition method a table was created with.
///
/// Go's `ast.PartitionType` has six values; this tier stores the one it can
/// route. The rest never reach a `PartitionSpec` because DDL refuses them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionKind {
    /// Go `ast.PartitionTypeHash`: `PARTITION BY HASH (expr) PARTITIONS n`.
    Hash,
}

impl PartitionKind {
    /// The method's name as `SHOW CREATE TABLE` prints it (Go
    /// `PartitionType.String()`).
    #[must_use]
    pub const fn sql(self) -> &'static str {
        match self {
            PartitionKind::Hash => "HASH",
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
    /// The evaluable form, whose `Column` nodes index the row by column
    /// OFFSET -- the same convention a generated column uses, so the row a
    /// write builds is already the evaluation row.
    pub expr: Expression,
    /// The column offsets the expression reads. A projected scan must decode
    /// these or routing would evaluate over holes.
    pub dependencies: Vec<usize>,
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
    /// The partition expression could not be evaluated over this row.
    Eval(tidb_expr::EvalError),
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
    /// `types` is the table's column type list, which the evaluator needs to
    /// materialize the row; `ctx` is the statement's evaluation context, so
    /// the expression is evaluated under the SQL mode of the statement that
    /// writes the row.
    ///
    /// # Errors
    ///
    /// [`RoutingError::Eval`] when the partition expression fails.
    pub fn locate(
        &self,
        row: &[Datum],
        types: &[FieldType],
        ctx: &impl tidb_expr::Columns,
    ) -> Result<i64, RoutingError> {
        let index = self.locate_index(row, types, ctx)?;
        Ok(self.definitions[index].id)
    }

    /// [`PartitionSpec::locate`] as the partition's ORDINAL, which is what
    /// the per-method rules are written in terms of.
    fn locate_index(
        &self,
        row: &[Datum],
        types: &[FieldType],
        ctx: &impl tidb_expr::Columns,
    ) -> Result<usize, RoutingError> {
        match self.kind {
            PartitionKind::Hash => {
                let value = crate::generated_column::eval_over_row(&self.expr, types, row, ctx)
                    .map_err(RoutingError::Eval)?;
                Ok(hash_partition_index(&value, self.num()))
            }
        }
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
fn hash_partition_index(value: &Datum, num: u64) -> usize {
    let bits = match value {
        Datum::Int(value) => *value,
        Datum::UInt(value) => *value as i64,
        // NULL, and any value the expression produced that is not an
        // integer, route to the first partition: DDL admits only integer
        // partition expressions (1659), so the non-integer case here is NULL
        // arriving as some other kind.
        _ => return 0,
    };
    (bits % num as i64).unsigned_abs() as usize
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
                hash_partition_index(&Datum::Int(value), 4),
                expected,
                "hash({value}) over 4 partitions"
            );
        }
        assert_eq!(hash_partition_index(&Datum::Null, 4), 0);
        // The unsigned captures, which only agree if the value is read as a
        // 64-bit pattern: u64::MAX is -1, and 9223372036854775809 is
        // -9223372036854775807.
        assert_eq!(hash_partition_index(&Datum::UInt(u64::MAX), 4), 1);
        assert_eq!(
            hash_partition_index(&Datum::UInt(9_223_372_036_854_775_809), 4),
            3
        );
    }

    /// A partition count that is not a power of two takes the same rule; this
    /// is the `hash(a+b) partitions 3` capture (`1+1 -> p2`, `1+2 -> p0`,
    /// `-1 + -1 -> p2`).
    #[test]
    fn hash_routing_is_not_a_power_of_two_special_case() {
        assert_eq!(hash_partition_index(&Datum::Int(2), 3), 2);
        assert_eq!(hash_partition_index(&Datum::Int(3), 3), 0);
        assert_eq!(hash_partition_index(&Datum::Int(-2), 3), 2);
    }
}
