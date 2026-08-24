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

//! `pkg/executor/aggregate` `HashAggExec`: the complete serial
//! `unparallelExec` path plus a bounded worker slice for one direct integer
//! group key with `COUNT`/`FINAL_COUNT`/integer `FIRST_ROW` aggregates and the
//! pushed-down decimal `AVG(partial_count, partial_sum)` shape used by TPC-H
//! q17.
//!
//! Aggregates ported (from `pkg/executor/aggfuncs`): `COUNT` (NULL inputs
//! skipped; `COUNT(*)` counts rows), `SUM` (NULL inputs skipped; an all-NULL /
//! empty group sums to NULL), and `FIRST_ROW` (the planner's carrier for
//! group-by columns in the output). `COUNT(a, b, ...)` (only reachable as
//! `COUNT(DISTINCT a, b, ...)` -- the parser rejects the non-DISTINCT form,
//! `pkg/parser` `expr_func_parser.go`'s `parseAggregateFuncCall`) counts a row
//! only when every argument is non-NULL, and its own DISTINCT dedupes over
//! the whole argument tuple rather than a single column (Go
//! `count4MultiArgs`). Groups emit in first-seen order, matching
//! Go's `groupKeys` insertion order. The no-group-by/no-data case emits ONE
//! empty group (`SELECT COUNT(c) FROM t` on empty `t` is `[0]`), while
//! group-by/no-data emits none -- exactly Go's `unparallelExec` rule.
//!
//! `MIN`/`MAX` compare with the shared datum ordering and skip NULL inputs
//! (Go `maxMin4*.UpdatePartialResult`); `AVG` keeps Go's sum/count pair and
//! divides at finalize, returning DECIMAL for integer/decimal inputs and
//! DOUBLE for real ones (Go `typeInfer4Avg` + `baseAvg*.AppendFinalResult2Chunk`),
//! with the same `div_precision_increment` the `/` operator uses. `DISTINCT`
//! de-duplicates a function's inputs per group on the datum hash key, as Go's
//! `*4Distinct*` variants do with their `valueSet`.
//!
//! `BIT_AND`/`BIT_OR`/`BIT_XOR` fold in the unsigned 64-bit domain and seed
//! with the operator's IDENTITY, so an empty or all-NULL group yields that
//! identity rather than NULL (Go `func_bitfuncs.go`); the variance family
//! (`VAR_POP`/`VAR_SAMP`/`STDDEV_POP`/`STDDEV_SAMP`) shares Go's one
//! incremental accumulator (`func_varpop.go`'s `calculateIntermediate`),
//! reproduced operation-for-operation so the floating-point result matches.
//!
//! `JSON_ARRAYAGG`/`JSON_OBJECTAGG` build a JSON document per group (NULL
//! inputs kept as JSON `null`, a repeated object key overwritten by the last
//! row), `APPROX_COUNT_DISTINCT` counts distinct encoded argument tuples and
//! `APPROX_PERCENTILE` ranks the group's values -- see each [`AggKind`]
//! variant for the exact Go rule and its captured edges.
//!
//! The Go parallel partial/final worker pipeline is transcreated in
//! [`parallel`] for the exactly-mergeable aggregate shapes; DISTINCT,
//! order-sensitive/float-domain aggregates, computed keys that cannot share
//! evaluation across threads, and spill rounds stay on the complete serial
//! path.
//!
//! `APPROX_COUNT_DISTINCT` ports Go's `BJKST` sketch
//! (`func_count_distinct.go`'s `partialResult4ApproxCountDistinct`, see
//! [`crate::approx_count_distinct`]) over the FarmHash `Hash64` of each
//! row's encoded argument tuple (`func_count_distinct.go`'s
//! `evalAndEncode`/`appendInt64`/etc, ported in
//! [`crate::farmhash`]), so results match Go's exactly, including above the
//! 65536-distinct-value threshold where the sketch stops being exact and
//! starts extrapolating. Only the encodings for `INT`/`REAL`/`DECIMAL`/
//! `STRING`/`BINARY`/vector arguments are byte-identical to Go's; `TIME`,
//! `DURATION`, and `JSON` arguments fall back to the datum's generic hash
//! key, which dedupes correctly but does not hash identically to Go's raw
//! struct layout / recursive `BinaryJSON.HashValue` -- a documented,
//! narrower divergence than before.

use crate::agg_spill::AggSpillDiskAction;

mod parallel;
mod spill;

use crate::approx_count_distinct::ApproxCountDistinctSketch;
use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::hash_join::{FastBytesMap, IdentityU64Hasher, fast_bytes_fingerprint};
use crate::mem_quota::StatementMemory;
use spill::new_group_bytes;

#[doc(inline)]
pub use parallel::HashAggContext;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_in_disk::DataInDiskByChunks;
use tidb_codec::{
    NIL_FLAG, UVARINT_FLAG, VARINT_FLAG, encode_bytes, encode_compact_bytes, encode_uvarint,
    encode_varint,
};
use tidb_datatype::{
    BinaryJSON, BinaryJSONValue, Collation, Datum, Decimal, EvalType, FieldType, FieldTypeCode,
    MAX_DECIMAL_SCALE, TimeType, UNSPECIFIED_LENGTH,
};
use tidb_expr::Columns;
use tidb_expr::compare_datums;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_util::disk;
use tidb_util::memory::{ActionOnExceed, ArcAction, Tracker};
use tidb_util::selection::{Selectable, select};
use tidb_util::set::MemorySet;

struct DatumSelection<'a>(&'a mut [Datum]);

impl Selectable for DatumSelection<'_> {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn less(&self, i: usize, j: usize) -> bool {
        compare_datums(&self.0[i], &self.0[j]).unwrap_or(Ordering::Equal) == Ordering::Less
    }

    fn swap(&mut self, i: usize, j: usize) {
        self.0.swap(i, j);
    }
}

/// The aggregate function kinds this seed supports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AggKind {
    /// `COUNT(expr)` / `COUNT(*)` (no argument).
    Count,
    /// Root final stage for a pushed partial `COUNT`: add the per-region
    /// counts instead of counting the number of partial rows.
    FinalCount,
    /// `SUM(expr)`.
    Sum,
    /// `FIRST_ROW(expr)`: the first row's value (the planner's group-column
    /// carrier).
    FirstRow,
    /// `MIN(expr)`.
    Min,
    /// `MAX(expr)`.
    Max,
    /// `AVG(expr)`.
    Avg,
    /// `GROUP_CONCAT([DISTINCT] arg [ORDER BY ...] [SEPARATOR sep])`, whose
    /// separator travels with the kind because it is part of the aggregate
    /// rather than an argument.
    GroupConcat {
        /// The text placed between rows; Go defaults it to a comma.
        separator: String,
    },
    /// `BIT_AND`/`BIT_OR`/`BIT_XOR`: a 64-bit fold whose empty-group result
    /// is the operator's IDENTITY rather than NULL (Go's `func_bitfuncs.go`).
    Bit(BitOp),
    /// The variance family, all four of which share Go's one incremental
    /// accumulator (`func_varpop.go`) and differ only in the divisor and
    /// whether a square root is taken: `VAR_POP`/`VARIANCE`, `VAR_SAMP`,
    /// `STDDEV_POP`/`STDDEV`/`STD`, `STDDEV_SAMP`.
    Variance {
        /// `true` for the SAMPLE forms, which divide by `count - 1` and are
        /// NULL for a single row.
        sample: bool,
        /// `true` for the `STDDEV*` forms, which take the square root.
        sqrt: bool,
    },
    /// `JSON_ARRAYAGG(v)`: every row's value in ARRIVAL order, NULL rows
    /// included as JSON `null` (Go `func_json_arrayagg.go` appends the
    /// converted datum unconditionally). An empty group is SQL NULL.
    /// `value_type` is the argument's own field type, needed to tag a
    /// BINARY-charset value's JSON `Opaque` wrapping with the source
    /// column's exact MySQL type code.
    JsonArrayAgg {
        /// The VALUE argument's static field type.
        value_type: FieldType,
    },
    /// `JSON_OBJECTAGG(k, v)`: a JSON object keyed by the stringified first
    /// argument. A repeated key keeps the LAST row's value (Go's
    /// `MemAwareMap.SetExt` overwrites), a NULL key fails the statement with
    /// 3158, a BINARY-charset key fails with 3144, and a NULL value stores
    /// JSON `null`. `value_type` is the VALUE argument's own field type, for
    /// the same `Opaque` type-code tagging `JsonArrayAgg` needs.
    ///
    /// `key_is_binary` is a STATIC property of the KEY argument's own field
    /// type (Go: `e.args[0].GetType(sctx).GetCharset() == charset.CharsetBin`),
    /// not a runtime check on the evaluated key datum: an ordinary string
    /// LITERAL key can also evaluate to a byte-backed datum in this
    /// evaluator (the chunk rewriter's own representation choice, not a
    /// charset), so only the argument's declared type can tell a genuinely
    /// BINARY-charset key apart from that coincidence.
    JsonObjectAgg {
        /// The VALUE argument's static field type.
        value_type: FieldType,
        /// Whether the KEY argument's static field type is BINARY-charset.
        key_is_binary: bool,
    },
    /// `APPROX_COUNT_DISTINCT(a[, b, ...])`: Go's BJKST sketch
    /// ([`ApproxCountDistinctSketch`]) over the FarmHash `Hash64` of the
    /// encoded argument tuple, skipping any row with a NULL argument. Below
    /// `uniquesHashMaxSize` (65536) distinct values the sketch keeps every
    /// hash and the answer is the exact distinct count; above that
    /// threshold Go starts discarding hashes and extrapolating the true
    /// cardinality, which this sketch reproduces bit for bit.
    ApproxCountDistinct,
    /// `APPROX_PERCENTILE(v, pct)`: the value at ordinal rank
    /// `ceil(pct / 100 * N)` among the group's non-NULL values (Go
    /// `func_percentile.go`'s `percentile`), which is a real element rather
    /// than an interpolation. `None` is Go's `basePercentile` fallback: for
    /// an argument whose eval type is none of INT/REAL/DECIMAL/DATETIME/
    /// DURATION (a string column, say) `buildApproxPercentile` returns the
    /// base function, which appends NULL for every group.
    ApproxPercentile(Option<i64>),
}

/// Which bitwise fold a [`AggKind::Bit`] performs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BitOp {
    /// `BIT_AND`, whose identity is all ones.
    And,
    /// `BIT_OR`, whose identity is zero.
    Or,
    /// `BIT_XOR`, whose identity is zero.
    Xor,
}

/// One aggregate: a kind plus its argument (`None` only for `COUNT(*)`).
#[derive(Clone, Debug)]
pub struct AggFunc {
    /// The aggregate kind.
    pub kind: AggKind,
    /// The argument expression; `None` means `COUNT(*)`.
    pub arg: Option<Expression>,
    /// `GROUP_CONCAT(a, b, ...)`'s arguments past the first: Go concatenates
    /// every argument per row (like `CONCAT`) before joining the rows with
    /// the separator, and drops the row when ANY argument is NULL. Empty for
    /// every other aggregate.
    pub extra_args: Vec<Expression>,
    /// Go `AggFuncDesc.HasDistinct`: whether repeated input values are counted
    /// once per group.
    pub distinct: bool,
    /// `GROUP_CONCAT`'s own `ORDER BY`, which orders the rows WITHIN the
    /// concatenation -- a separate scope from the query's `ORDER BY`. Each
    /// entry is an expression over the source row and its descending flag.
    pub order_by: Vec<(Expression, bool)>,
    /// Go `Column.OrigName` of the FIRST argument -- `db.table.column` --
    /// which is the only thing the 1260 truncation message renders and which
    /// the rewritten `Expression` no longer carries. Empty for every
    /// aggregate but `GROUP_CONCAT`, and for a computed `GROUP_CONCAT`
    /// argument, where Go prints an internal plan column id instead.
    pub arg_orig_name: String,
}

/// The narrow aggregate shape that can be evaluated without crossing the
/// non-`Send` session/expression context boundary.  It covers the two q13
/// hash aggregations (`COUNT` and the `FIRST_ROW` group-key carrier) while
/// leaving computed expressions, DISTINCT, and order-sensitive aggregates on
/// the complete serial implementation.
#[derive(Clone)]
enum ParallelIntAggSpec {
    Count(Option<usize>),
    FinalCount {
        column: usize,
        unsigned: bool,
    },
    /// Final stage of a pushed-down decimal AVG: `arg` is the partial count
    /// and the one extra argument is the partial decimal sum.
    FinalAvgDecimal {
        count_column: usize,
        count_unsigned: bool,
        sum_column: usize,
    },
    FirstRow {
        column: usize,
        field_type: FieldType,
    },
}

#[derive(Clone)]
enum DirectStringAgg {
    Count(Option<usize>),
    FinalCount(usize),
    Sum(usize),
    FirstRow { column: usize, field_type: FieldType },
}

const DIRECT_STRING_MAX_KEY_BYTES: usize = 192;

type DirectStringBucketMap<V> =
    HashMap<u64, V, BuildHasherDefault<IdentityU64Hasher>>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum ParallelIntKey {
    Null,
    Signed(i64),
    Unsigned(u64),
}

/// Go's hash aggregation uses a cheap FNV-family bucket hash. The default
/// Rust `HashMap` hasher is SipHash, which is needlessly expensive for the
/// fixed-width integer keys in the bounded worker path. The enum's derived
/// `Hash` implementation calls the typed `Hasher` methods below, so this
/// preserves the variant distinction without materializing an encoded key.
#[derive(Default)]
struct ParallelIntHasher {
    hash: u64,
    initialized: bool,
}

impl Hasher for ParallelIntHasher {
    fn finish(&self) -> u64 {
        if self.initialized {
            self.hash
        } else {
            0xcbf29ce484222325
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = if self.initialized {
            self.hash
        } else {
            self.initialized = true;
            0xcbf29ce484222325
        };
        for byte in bytes {
            hash = hash.wrapping_mul(0x100000001b3);
            hash ^= u64::from(*byte);
        }
        self.hash = hash;
    }

    fn write_u8(&mut self, value: u8) {
        self.write(&[value]);
    }

    fn write_i64(&mut self, value: i64) {
        self.write(&value.to_ne_bytes());
    }

    fn write_u64(&mut self, value: u64) {
        self.write(&value.to_ne_bytes());
    }
}

type ParallelIntMap<V> = HashMap<ParallelIntKey, V, BuildHasherDefault<ParallelIntHasher>>;

impl ParallelIntKey {
    fn from_row(row: tidb_chunk::row::Row<'_>, column: usize, unsigned: bool) -> Self {
        if row.is_null(column) {
            Self::Null
        } else if unsigned {
            Self::Unsigned(row.get_uint64(column))
        } else {
            Self::Signed(row.get_int64(column))
        }
    }

    fn as_datum(self) -> Datum {
        match self {
            Self::Null => Datum::Null,
            Self::Signed(value) => Datum::Int(value),
            Self::Unsigned(value) => Datum::UInt(value),
        }
    }
}

struct ParallelIntGroup {
    first_seq: usize,
    /// Inline storage for the common one-to-two-aggregate case: q18's
    /// subquery opens 1.5M groups, and three heap allocations per group
    /// showed up as a top jemalloc cost. `SmallVec` keeps them inline.
    counts: smallvec::SmallVec<[i64; 2]>,
    decimal_sums: smallvec::SmallVec<[Option<ParallelDecimalSum>; 1]>,
    first_rows: smallvec::SmallVec<[Option<Datum>; 1]>,
}

/// A partial decimal SUM in the bounded integer aggregate. TPC-H q17's
/// pushed-down AVG has one fixed fractional scale and fits comfortably in an
/// `i128`; keeping that representation avoids constructing a heap-backed
/// [`Decimal`] for every partial row. Mixed-scale values and coefficients that
/// overflow `i128` retain the exact Decimal fallback.
#[derive(Clone)]
enum ParallelDecimalSum {
    Fixed { coefficient: i128, scale: u32 },
    Decimal(Decimal),
}

impl ParallelDecimalSum {
    fn from_my_decimal(value: &tidb_datatype::MyDecimal) -> Self {
        value
            .to_i128_scaled()
            .map(|(coefficient, scale)| Self::Fixed { coefficient, scale })
            .unwrap_or_else(|| Self::Decimal(Decimal::from_my_decimal(value)))
    }

    fn add_my_decimal(self, value: &tidb_datatype::MyDecimal) -> Self {
        let incoming = value.to_i128_scaled();
        match (self, incoming) {
            (Self::Fixed { coefficient, scale }, Some((rhs, rhs_scale))) if scale == rhs_scale => {
                coefficient
                    .checked_add(rhs)
                    .map(|coefficient| Self::Fixed { coefficient, scale })
                    .unwrap_or_else(|| {
                        let current = Decimal::from_scaled_i128(coefficient, scale);
                        Self::Decimal(current.add(&Decimal::from_my_decimal(value)))
                    })
            }
            (Self::Fixed { coefficient, scale }, _) => {
                let current = Decimal::from_scaled_i128(coefficient, scale);
                Self::Decimal(current.add(&Decimal::from_my_decimal(value)))
            }
            (Self::Decimal(current), _) => {
                Self::Decimal(current.add(&Decimal::from_my_decimal(value)))
            }
        }
    }

    fn add(self, incoming: Self) -> Self {
        match (self, incoming) {
            (
                Self::Fixed { coefficient, scale },
                Self::Fixed {
                    coefficient: rhs,
                    scale: rhs_scale,
                },
            ) if scale == rhs_scale => coefficient
                .checked_add(rhs)
                .map(|coefficient| Self::Fixed { coefficient, scale })
                .unwrap_or_else(|| {
                    let current = Decimal::from_scaled_i128(coefficient, scale);
                    let incoming = Decimal::from_scaled_i128(rhs, rhs_scale);
                    Self::Decimal(current.add(&incoming))
                }),
            (current, incoming) => {
                let current = current.into_decimal();
                let incoming = incoming.into_decimal();
                Self::Decimal(current.add(&incoming))
            }
        }
    }

    fn into_decimal(self) -> Decimal {
        match self {
            Self::Fixed { coefficient, scale } => Decimal::from_scaled_i128(coefficient, scale),
            Self::Decimal(value) => value,
        }
    }

    fn scale(&self) -> u32 {
        match self {
            Self::Fixed { scale, .. } => *scale,
            Self::Decimal(value) => value.scale(),
        }
    }
}

struct ParallelIntCountGroup {
    first_seq: usize,
    count: i64,
}

/// One row's aggregate input. `GROUP_CONCAT(DISTINCT ...)` needs the rendered
/// value and its per-argument collation key separately.
struct AggInput {
    value: Option<Datum>,
    distinct_key: Option<Vec<u8>>,
    /// A DECIMAL cell's `(signed coefficient, scale)` read straight from the
    /// chunk, set only for a non-DISTINCT SUM over a bare DECIMAL column.
    /// The fold consumes this INSTEAD of `value`, skipping the Datum and its
    /// `Decimal` build entirely.
    decimal_coefficient: Option<(i128, u32)>,
}

impl AggFunc {
    /// An aggregate without the `DISTINCT` modifier.
    #[must_use]
    pub fn new(kind: AggKind, arg: Option<Expression>) -> Self {
        AggFunc {
            kind,
            arg,
            extra_args: Vec::new(),
            order_by: Vec::new(),
            distinct: false,
            arg_orig_name: String::new(),
        }
    }
}

/// One group's partial results, in agg-func order.
enum Partial {
    Count(i64),
    FinalCount(i64),
    /// `None` until the first non-NULL input (an empty sum is NULL). Go
    /// sums an integer or decimal argument exactly, in the decimal domain --
    /// `SUM` over a BIGINT column is a DECIMAL in MySQL.
    SumDecimal(Option<Decimal>),
    /// Fixed-scale decimal SUM accumulator. The common DECIMAL(15,2) fold
    /// (TPC-H's revenue sums) keeps only the signed i128 coefficient until
    /// finalization: one checked add per row instead of a `Decimal` build
    /// plus its digit parse. A differing-scale or overflowing input falls
    /// back to [`Partial::SumDecimal`] via [`Partial::materialize_sum_fast`].
    SumDecimalFast {
        sum: i128,
        scale: u32,
    },
    SumReal(Option<f64>),
    /// `None` until the first row is seen.
    FirstRow(Option<Datum>),
    /// `MIN`/`MAX`: the extreme seen so far, `None` while every input was NULL.
    MaxMin {
        value: Option<Datum>,
        is_max: bool,
    },
    /// `AVG` over integer/decimal inputs: Go's exact decimal sum plus count.
    AvgDecimal {
        sum: Decimal,
        count: i64,
    },
    /// Fixed-scale AVG accumulator. The common DECIMAL(15,2) path keeps only
    /// the signed coefficient until finalization, avoiding one Decimal value
    /// allocation per input row.
    AvgDecimalFast {
        sum: i128,
        scale: u32,
        count: i64,
    },
    /// `AVG` over real inputs: Go's `partialResult4AvgFloat64`.
    AvgReal {
        sum: f64,
        count: i64,
    },
    /// `GROUP_CONCAT`: the values seen so far, in row order. Go keeps the
    /// same buffer and joins it when the group is finalized.
    GroupConcat {
        /// Each contributed value with the sort key its row produced; the
        /// key is empty when the aggregate has no ORDER BY of its own.
        values: Vec<(Vec<u8>, Vec<Datum>)>,
        separator: String,
    },
    /// `BIT_AND`/`BIT_OR`/`BIT_XOR`: the fold so far, seeded with the
    /// operator's identity so an all-NULL (or empty) group still yields it.
    Bit {
        acc: u64,
        op: BitOp,
    },
    /// The variance family's shared accumulator, Go's
    /// `partialResult4VarPopFloat64`: the running count, sum and the
    /// incremental sum of squared deviations.
    Variance {
        count: i64,
        sum: f64,
        variance: f64,
        sample: bool,
        sqrt: bool,
    },
    /// `JSON_ARRAYAGG`: the converted values in arrival order, plus the
    /// value argument's field type for `Opaque` type-code tagging.
    JsonArrayAgg(Vec<BinaryJSON>, FieldType),
    /// `JSON_OBJECTAGG`: the object built so far, the value argument's field
    /// type, and whether the key argument is BINARY-charset. A `BTreeMap`
    /// both keeps the last write per key (Go's map overwrite) and hands the
    /// encoder the bytewise-sorted key order it needs.
    JsonObjectAgg(BTreeMap<String, BinaryJSON>, FieldType, bool),
    /// `APPROX_COUNT_DISTINCT`: the BJKST sketch folding the group's encoded
    /// argument tuples.
    ApproxCountDistinct(ApproxCountDistinctSketch),
    /// `APPROX_PERCENTILE`: the group's non-NULL values, ranked at finalize.
    /// `percent` is `None` for Go's always-NULL fallback build.
    ApproxPercentile {
        values: Vec<Datum>,
        percent: Option<i64>,
    },
}

/// One aggregate's per-group state: its partial result plus, for `DISTINCT`,
/// the input values already folded in (Go's `valueSet`).
///
/// `collation` is the ARGUMENT expression's derived collation, which Go
/// reads off `AggFuncDesc.RetTp` (`aggfuncs/builder.go:460-468` builds
/// `collate.GetCollator(RetTp.GetCollate())` for MIN/MAX, and the DISTINCT
/// value set encodes under the same collator). It is not the column's
/// collation: a computed argument such as `UPPER(s)` derives its own, which
/// is why the aggregate cannot re-read the datum and has to be told.
struct AggState {
    partial: Partial,
    seen: Option<MemorySet<Vec<u8>>>,
    collation: tidb_datatype::Collation,
}

impl AggState {
    fn new(func: &AggFunc) -> AggState {
        let collation = func
            .arg
            .as_ref()
            .map_or(tidb_datatype::Collation::DEFAULT, expr_collation);
        AggState {
            partial: Partial::new(&func.kind),
            seen: func.distinct.then(MemorySet::new),
            collation,
        }
    }

    /// Folds one row's input in, skipping values this group has already seen
    /// when the function is `DISTINCT`.
    ///
    /// Go keys its `valueSet` on the codec encoding of the evaluated argument
    /// UNDER ITS OWN COLLATION -- the same `codec.HashChunkSelected` call the
    /// group key uses -- so `COUNT(DISTINCT UPPER(s))` over a `_ci` column
    /// folds `a`/`A` together exactly as `COUNT(DISTINCT s)` does. A datum
    /// with no hash key (Go's encode error) fails the statement rather than
    /// silently counting twice.
    /// Returns the bytes this row ADDED to the group's state, which the
    /// aggregation reports to its memory tracker (Go's `UpdatePartialResult`
    /// returns the same `memDelta`).
    fn update(
        &mut self,
        value: Option<Datum>,
        extra: &[Datum],
        sort_key: Vec<Datum>,
        distinct_key: Option<Vec<u8>>,
    ) -> Result<i64, ExecError> {
        let mut delta: i64 = 0;
        if let Some(seen) = &mut self.seen {
            let datum = value.clone().unwrap_or(Datum::Null);
            if datum != Datum::Null {
                let key = if let Some(key) = distinct_key {
                    key
                } else {
                    match datum.as_raw_bytes() {
                        Some(_) => group_key_part(&self.collation, &datum),
                        None => datum
                            .to_hash_key()
                            .map_err(|_| ExecError::unsupported("DISTINCT over this datum kind"))?,
                    }
                };
                let key_bytes = i64::try_from(key.len()).unwrap_or(i64::MAX);
                let (map_delta, inserted) = seen.insert(key);
                if !inserted {
                    return Ok(0);
                }
                delta += key_bytes + map_delta;
            }
        }
        delta += self.partial.retained_row_bytes(value.as_ref());
        self.partial
            .update(value, extra, sort_key, self.collation)?;
        Ok(delta)
    }

    /// Folds one DECIMAL cell coefficient into the fixed-scale SUM
    /// accumulator. Returns `false` when this state cannot take it (DISTINCT
    /// or an i128 overflow) and the caller must use the complete path.
    /// Folds a DECIMAL cell coefficient (read straight from the chunk)
    /// into this state. Returns `false` when the caller must replay the row
    /// through the complete path.
    fn partial_update_with_coefficient(&mut self, coefficient: i128, scale: u32) -> bool {
        self.partial.update_with_coefficient(coefficient, scale)
    }

    fn update_sum_decimal_fast(&mut self, coefficient: i128, scale: u32) -> bool {
        if self.seen.is_some() {
            return false;
        }
        match &mut self.partial {
            Partial::SumDecimal(None) => {
                self.partial = Partial::SumDecimalFast {
                    sum: coefficient,
                    scale,
                };
                true
            }
            Partial::SumDecimalFast {
                sum,
                scale: current_scale,
            } if *current_scale == scale => match sum.checked_add(coefficient) {
                Some(total) => {
                    *sum = total;
                    true
                }
                None => false,
            },
            _ => false,
        }
    }

    fn update_avg_decimal_fast(&mut self, coefficient: i128, scale: u32, count: i64) -> bool {
        if self.seen.is_some() || count < 0 {
            return false;
        }
        match &mut self.partial {
            Partial::AvgDecimal { count: current, .. } if *current == 0 => {
                self.partial = Partial::AvgDecimalFast {
                    sum: coefficient,
                    scale,
                    count,
                };
                true
            }
            Partial::AvgDecimalFast {
                sum,
                scale: current_scale,
                count: current,
            } if *current_scale == scale => {
                let Some(total) = sum.checked_add(coefficient) else {
                    return false;
                };
                *sum = total;
                *current = current.wrapping_add(count);
                true
            }
            _ => false,
        }
    }

    /// Updates the scalar COUNT accumulator without materializing its input
    /// datum. Returns false for DISTINCT or a non-COUNT state so the caller
    /// can use the complete aggregate path.
    fn update_count_fast(&mut self, input_is_non_null: bool) -> bool {
        if self.seen.is_some() {
            return false;
        }
        let Partial::Count(count) = &mut self.partial else {
            return false;
        };
        if input_is_non_null {
            *count += 1;
        }
        true
    }

    fn update_final_count_fast(&mut self, value: i64) -> bool {
        if self.seen.is_some() {
            return false;
        }
        let Partial::FinalCount(total) = &mut self.partial else {
            return false;
        };
        *total = total.wrapping_add(value);
        true
    }

    /// Folds a fixed-scale DECIMAL MIN/MAX by comparing coefficients directly
    /// and only materializing a Decimal when a group first receives (or
    /// replaces) its extremum.
    fn update_decimal_max_min_fast(&mut self, coefficient: i128, scale: u32) -> bool {
        if self.seen.is_some() {
            return false;
        }
        let Partial::MaxMin { value, is_max } = &mut self.partial else {
            return false;
        };
        match value {
            None => {
                *value = Some(Datum::Decimal(Decimal::from_scaled_i128(
                    coefficient,
                    scale,
                )));
                true
            }
            Some(Datum::Decimal(current)) => {
                let Some((current_coefficient, current_scale)) = current.coefficient_i128() else {
                    return false;
                };
                if current_scale != scale {
                    return false;
                }
                let improves = if *is_max {
                    coefficient > current_coefficient
                } else {
                    coefficient < current_coefficient
                };
                if improves {
                    *value = Some(Datum::Decimal(Decimal::from_scaled_i128(
                        coefficient,
                        scale,
                    )));
                }
                true
            }
            Some(_) => false,
        }
    }

    fn has_first_row(&self) -> bool {
        self.seen.is_none() && matches!(self.partial, Partial::FirstRow(Some(_)))
    }
}

/// The bytes one `GROUP_CONCAT` input contributes, which Go produces by
/// casting the argument to a string.
/// The derived collation of a group-by expression.
fn expr_collation(expr: &Expression) -> tidb_datatype::Collation {
    tidb_expr::collation_derive::collation_of_node(expr)
}

/// One group-key part: a STRING key is the collation's SORT KEY, so two values
/// that the collation calls equal land in the same group.
///
/// Go's `HashGroupKey` calls `codec.HashChunkSelected` with the column's own
/// collator, which encodes `collator.Key(value)` rather than the raw bytes --
/// that is what makes `GROUP BY ci_col` over `a, A, b, B` produce two groups
/// (and `utf8mb4_bin`'s PAD SPACE key group `'a'` with `'a  '`). Every
/// non-string datum keeps its ordinary hash code.
pub(crate) fn append_group_key_part(
    collation: &tidb_datatype::Collation,
    datum: &Datum,
    output: &mut Vec<u8>,
) {
    match datum.as_raw_bytes() {
        Some(bytes) => {
            encode_compact_bytes(output, &collation.key(bytes));
        }
        None => tidb_codec::Encoder::new(true).hash_code(output, datum),
    }
}

/// The allocation-free counterpart of [`append_group_key_part`] for a typed
/// integer chunk column. This writes exactly the same datum hash code without
/// first constructing `Datum::Int`/`Datum::UInt` for every input row.
fn append_integer_group_key_part(
    row: tidb_chunk::row::Row<'_>,
    index: usize,
    unsigned: bool,
    output: &mut Vec<u8>,
) {
    if row.is_null(index) {
        output.push(NIL_FLAG);
    } else if unsigned {
        output.push(UVARINT_FLAG);
        encode_uvarint(output, row.get_uint64(index));
    } else {
        output.push(VARINT_FLAG);
        encode_varint(output, row.get_int64(index));
    }
}

pub(crate) fn group_key_part(collation: &tidb_datatype::Collation, datum: &Datum) -> Vec<u8> {
    let mut output = Vec::new();
    append_group_key_part(collation, datum, &mut output);
    output
}

/// Go `SessionVars.GroupConcatMaxLen` as this statement sees it.
fn group_concat_max_len<C: Columns>(ctx: &C) -> u64 {
    match ctx.sysvar(None, "group_concat_max_len") {
        Some(Datum::UInt(value)) => value,
        Some(Datum::Int(value)) if value >= 0 => value as u64,
        // Go `DefGroupConcatMaxLen`.
        _ => 1024,
    }
}

/// The argument text Go's 1260 message names, which is
/// `Expression.StringWithCtx` of `args[0]`: a table column prints as its
/// `OrigName` (`test.g.s`), and anything else prints as `Column#<id>` --
/// Go's aggregate reads a PROJECTED column, so a computed argument shows an
/// internal plan id this tier does not mint. The bare-column case, which is
/// the one a user can predict, is exact.
fn group_concat_arg_text(func: &AggFunc) -> String {
    if func.arg_orig_name.is_empty() {
        match &func.arg {
            Some(Expression::Column(column)) => format!("Column#{}", column.unique_id),
            _ => "Column#0".to_owned(),
        }
    } else {
        func.arg_orig_name.clone()
    }
}

fn group_concat_bytes(value: &Datum) -> Result<Vec<u8>, ExecError> {
    Ok(match value {
        Datum::Bytes(bytes) => bytes.clone(),
        Datum::String(text) => text.bytes().to_vec(),
        Datum::Int(number) => number.to_string().into_bytes(),
        Datum::UInt(number) => number.to_string().into_bytes(),
        Datum::Real(number) => number.to_string().into_bytes(),
        Datum::Decimal(number) => number.to_string().into_bytes(),
        _ => {
            return Err(ExecError::unsupported(
                "GROUP_CONCAT over this datum kind is not yet supported",
            ));
        }
    })
}

/// The value Go's real aggregate implementations consume.
///
/// `WrapCastForAggArgs` chooses `EvalReal` whenever `SUM`/`AVG` inferred a
/// DOUBLE result. That includes character, temporal, and duration arguments,
/// not merely FLOAT columns. Keep the conversion at the aggregate boundary so
/// hash and stream aggregation use the same rule.
fn real_aggregate_value(value: &Datum, function: &'static str) -> Result<f64, ExecError> {
    value
        .to_f64()
        .map(|converted| converted.value)
        .map_err(|_| {
            ExecError::unsupported(format!(
                "{function} over this datum kind is not yet supported"
            ))
        })
}

/// One `APPROX_COUNT_DISTINCT` argument's contribution to the hashed tuple,
/// Go `func_count_distinct.go`'s `evalAndEncode`: each argument type has its
/// own raw encoding (a fixed-width native-endian copy of the scalar for
/// `INT`/`REAL`, `MyDecimal.ToHashKey` for `DECIMAL`, a collation sort key
/// wrapped in `codec.EncodeCompactBytes` for `STRING`/`BINARY`, the vector's
/// wire serialization for `VECTOR`) rather than the generic datum hash key
/// `COUNT(DISTINCT ...)` uses, because these bytes feed FarmHash directly
/// and the sketch only matches Go's numbers if the hash INPUT matches too.
///
/// `TIME` encodes as Go's `appendTime`/`WriteTime`: a 16-byte struct-style
/// layout (year as big-endian u16, then month/day/hour/minute/second as raw
/// bytes, a zero pad byte, microsecond as big-endian u32, the MySQL type
/// code, the fsp, and two zero pad bytes) rather than the datum's generic
/// hash key, so large-cardinality sketches over DATE/DATETIME/TIMESTAMP
/// columns extrapolate identically to Go's.
///
/// `DURATION` encodes as Go's `appendDuration`: a raw 16-byte copy of the
/// Go `Duration` struct, which is `{ Duration int64 /* ns */; Fsp int }` --
/// two 8-byte little-endian fields with no padding, because Go's `int` is
/// 8 bytes on every platform TiDB ships.
///
/// `JSON` encodes via `BinaryJSON.HashValue`, a recursive type-tagged
/// traversal that folds integers into doubles when no precision is lost (so
/// `3` and `3.0` collide) and recurses into arrays/objects so structurally
/// equal values hash equal.
pub(crate) fn approx_count_distinct_encode(datum: &Datum) -> Result<Vec<u8>, ExecError> {
    let unsupported = || ExecError::unsupported("APPROX_COUNT_DISTINCT over this datum kind");
    Ok(match datum {
        Datum::Int(value) => value.to_le_bytes().to_vec(),
        // Go's `arg.EvalInt` returns the column's stored int64 bit pattern
        // regardless of signedness, so an unsigned argument encodes to the
        // same 8 raw bytes as a signed one with that bit pattern.
        Datum::UInt(value) => value.to_le_bytes().to_vec(),
        Datum::Real(value) | Datum::Float32(value) => value.to_le_bytes().to_vec(),
        Datum::Decimal(value) => value.to_hash_key().map_err(|_| unsupported())?.0,
        Datum::String(text) => {
            let collation = datum.collation().unwrap_or(Collation::Binary);
            let key = collation.immutable_key(text.bytes());
            let mut encoded = Vec::new();
            encode_compact_bytes(&mut encoded, &key);
            encoded
        }
        Datum::Bytes(bytes) => {
            let collation = datum.collation().unwrap_or(Collation::Binary);
            let key = collation.immutable_key(bytes);
            let mut encoded = Vec::new();
            encode_compact_bytes(&mut encoded, &key);
            encoded
        }
        Datum::VectorFloat32(vector) => {
            let mut encoded = Vec::new();
            vector.serialize_to(&mut encoded);
            encoded
        }
        Datum::Time(time) => {
            let core = time.core_time();
            let mut encoded = [0u8; 16];
            encoded[0..2].copy_from_slice(&(core.year() as u16).to_be_bytes());
            encoded[2] = core.month();
            encoded[3] = core.day();
            encoded[4] = core.hour();
            encoded[5] = core.minute();
            encoded[6] = core.second();
            // encoded[7] is Go's struct padding byte, left zero.
            encoded[8..12].copy_from_slice(&core.microsecond().to_be_bytes());
            encoded[12] = match time.kind() {
                TimeType::Date => mysql_type::DATE,
                TimeType::DateTime => mysql_type::DATETIME,
                TimeType::Timestamp => mysql_type::TIMESTAMP,
            };
            encoded[13] = time.fsp();
            // encoded[14..16] are Go's trailing struct padding bytes, left zero.
            encoded.to_vec()
        }
        Datum::Duration(duration) => {
            let mut encoded = Vec::with_capacity(16);
            encoded.extend_from_slice(&duration.nanoseconds().to_le_bytes());
            encoded.extend_from_slice(&duration.fsp().to_le_bytes());
            encoded
        }
        Datum::Json(json) => json.hash_value().map_err(|_| unsupported())?,
        other => other.to_hash_key().map_err(|_| unsupported())?,
    })
}

/// MySQL column type codes as embedded in the encoded TIME/DATETIME/
/// TIMESTAMP byte tuple above (`pkg/parser/mysql/type.go`).
mod mysql_type {
    pub(super) const TIMESTAMP: u8 = 7;
    pub(super) const DATE: u8 = 10;
    pub(super) const DATETIME: u8 = 12;
}

impl Partial {
    fn new(kind: &AggKind) -> Partial {
        match kind {
            AggKind::Count => Partial::Count(0),
            AggKind::FinalCount => Partial::FinalCount(0),
            // The sum's domain is chosen lazily from the first non-NULL input.
            AggKind::Sum => Partial::SumDecimal(None),
            AggKind::GroupConcat { separator } => Partial::GroupConcat {
                values: Vec::new(),
                separator: separator.clone(),
            },
            AggKind::FirstRow => Partial::FirstRow(None),
            AggKind::Min => Partial::MaxMin {
                value: None,
                is_max: false,
            },
            AggKind::Max => Partial::MaxMin {
                value: None,
                is_max: true,
            },
            // As with SUM, the domain is chosen from the first non-NULL input:
            // Go picks it from the inferred return type, which is DECIMAL for
            // integer/decimal arguments and DOUBLE for real ones.
            AggKind::Avg => Partial::AvgDecimal {
                sum: Decimal::from_int(0),
                count: 0,
            },
            AggKind::Bit(op) => Partial::Bit {
                acc: match op {
                    BitOp::And => u64::MAX,
                    BitOp::Or | BitOp::Xor => 0,
                },
                op: *op,
            },
            AggKind::Variance { sample, sqrt } => Partial::Variance {
                count: 0,
                sum: 0.0,
                variance: 0.0,
                sample: *sample,
                sqrt: *sqrt,
            },
            AggKind::JsonArrayAgg { value_type } => {
                Partial::JsonArrayAgg(Vec::new(), value_type.clone())
            }
            AggKind::JsonObjectAgg {
                value_type,
                key_is_binary,
            } => Partial::JsonObjectAgg(BTreeMap::new(), value_type.clone(), *key_is_binary),
            AggKind::ApproxCountDistinct => {
                Partial::ApproxCountDistinct(ApproxCountDistinctSketch::new())
            }
            AggKind::ApproxPercentile(percent) => Partial::ApproxPercentile {
                values: Vec::new(),
                percent: *percent,
            },
        }
    }

    fn materialize_sum_fast(&mut self) {
        if let Partial::SumDecimalFast { sum, scale } = self {
            *self = Partial::SumDecimal(Some(Decimal::from_scaled_i128(*sum, *scale)));
        }
    }

    fn materialize_avg_fast(&mut self) {
        let replacement = match self {
            Partial::AvgDecimalFast { sum, scale, count } => {
                Some((Decimal::from_scaled_i128(*sum, *scale), *count))
            }
            _ => None,
        };
        if let Some((sum, count)) = replacement {
            *self = Partial::AvgDecimal { sum, count };
        }
    }

    /// Folds one DECIMAL cell coefficient into the fixed-scale SUM
    /// accumulator. Returns `false` when this state cannot take it (an i128
    /// overflow) and the caller must replay via the complete path.
    fn update_with_coefficient(&mut self, coefficient: i128, scale: u32) -> bool {
        match &mut *self {
            Partial::SumDecimal(None) => {
                *self = Partial::SumDecimalFast {
                    sum: coefficient,
                    scale,
                };
                true
            }
            Partial::SumDecimalFast {
                sum,
                scale: current_scale,
            } if *current_scale == scale => match sum.checked_add(coefficient) {
                Some(total) => {
                    *sum = total;
                    true
                }
                None => {
                    self.materialize_sum_fast();
                    false
                }
            },
            _ => false,
        }
    }

    fn update(
        &mut self,
        value: Option<Datum>,
        extra: &[Datum],
        sort_key: Vec<Datum>,
        collation: tidb_datatype::Collation,
    ) -> Result<(), ExecError> {
        if matches!(
            self,
            Partial::AvgDecimal { .. } | Partial::AvgDecimalFast { .. } | Partial::AvgReal { .. }
        ) && !extra.is_empty()
        {
            if extra.len() != 1 {
                return Err(ExecError::unsupported(
                    "final AVG requires one partial sum column",
                ));
            }
            let sum = &extra[0];
            if matches!(sum, Datum::Null) {
                return Ok(());
            }
            let count = match value {
                None => {
                    return Err(ExecError::unsupported(
                        "final AVG requires a partial count column",
                    ));
                }
                Some(Datum::Null) => return Ok(()),
                Some(Datum::Int(count)) if count >= 0 => count,
                Some(Datum::UInt(count)) => i64::try_from(count)
                    .map_err(|_| ExecError::unsupported("partial AVG count exceeds i64"))?,
                Some(_) => {
                    return Err(ExecError::unsupported(
                        "final AVG requires integer partial counts",
                    ));
                }
            };
            match self {
                Partial::AvgDecimal {
                    sum: destination,
                    count: destination_count,
                } => {
                    let addend = match sum {
                        Datum::Int(value) => Some(Decimal::from_int(*value)),
                        Datum::UInt(value) => Some(Decimal::from_uint(*value)),
                        Datum::Decimal(value) => Some(value.clone()),
                        _ => None,
                    };
                    if let Some(addend) = addend {
                        *destination = destination.add(&addend);
                        *destination_count = destination_count.wrapping_add(count);
                    } else {
                        let accumulated =
                            real_aggregate_value(&Datum::Decimal(destination.clone()), "AVG")?;
                        *self = Partial::AvgReal {
                            sum: accumulated + real_aggregate_value(sum, "AVG")?,
                            count: destination_count.wrapping_add(count),
                        };
                    }
                }
                Partial::AvgReal {
                    sum: destination,
                    count: destination_count,
                } => {
                    *destination += real_aggregate_value(sum, "AVG")?;
                    *destination_count = destination_count.wrapping_add(count);
                }
                Partial::AvgDecimalFast {
                    sum: destination,
                    scale: destination_scale,
                    count: destination_count,
                } => {
                    let addend = match sum {
                        Datum::Decimal(value) => value.coefficient_i128(),
                        Datum::Int(value) => Some((i128::from(*value), 0)),
                        Datum::UInt(value) => i128::try_from(*value).ok().map(|v| (v, 0)),
                        _ => None,
                    };
                    if let Some((coefficient, scale)) = addend {
                        if scale == *destination_scale {
                            if let Some(total) = destination.checked_add(coefficient) {
                                *destination = total;
                                *destination_count = destination_count.wrapping_add(count);
                                return Ok(());
                            }
                        }
                    }
                    let current = Decimal::from_scaled_i128(*destination, *destination_scale);
                    *self = Partial::AvgDecimal {
                        sum: current,
                        count: *destination_count,
                    };
                    if let Partial::AvgDecimal {
                        sum: destination,
                        count: destination_count,
                    } = self
                    {
                        let addend = match sum {
                            Datum::Int(value) => Decimal::from_int(*value),
                            Datum::UInt(value) => Decimal::from_uint(*value),
                            Datum::Decimal(value) => value.clone(),
                            _ => {
                                let accumulated = real_aggregate_value(
                                    &Datum::Decimal(destination.clone()),
                                    "AVG",
                                )?;
                                *self = Partial::AvgReal {
                                    sum: accumulated + real_aggregate_value(sum, "AVG")?,
                                    count: destination_count.wrapping_add(count),
                                };
                                return Ok(());
                            }
                        };
                        *destination = destination.add(&addend);
                        *destination_count = destination_count.wrapping_add(count);
                    }
                }
                _ => unreachable!("final AVG was checked above"),
            }
            return Ok(());
        }
        // A caller that cannot keep using the fixed-scale fast path falls
        // back to the ordinary Decimal state before entering this match.
        self.materialize_avg_fast();
        match (self, value) {
            // Go appends the converted value for EVERY row, so a NULL input
            // lands in the array as JSON `null` rather than being skipped.
            (Partial::JsonArrayAgg(..), None) => {
                return Err(ExecError::unsupported("JSON_ARRAYAGG requires an argument"));
            }
            (Partial::JsonArrayAgg(entries, value_type), Some(input)) => {
                entries.push(json_value(&input, value_type)?)
            }
            (Partial::JsonObjectAgg(..), None | Some(Datum::Null)) => {
                return Err(ExecError::JsonDocumentNullKey);
            }
            // A BINARY-charset key (Go: `e.args[0].GetType(sctx).GetCharset()
            // == charset.CharsetBin`) fails the statement with 3144 before
            // the value is even evaluated -- a STATIC property of the key
            // argument's declared type, checked here rather than by
            // inspecting the evaluated datum (see `key_is_binary`'s own doc
            // for why the datum kind alone cannot tell this apart from an
            // ordinary string literal).
            (Partial::JsonObjectAgg(_, _, true), Some(_)) => {
                return Err(ExecError::InvalidJsonCharset {
                    charset: "binary".to_owned(),
                });
            }
            (Partial::JsonObjectAgg(entries, value_type, false), Some(key)) => {
                let value = extra.first().cloned().unwrap_or(Datum::Null);
                entries.insert(json_object_key(&key)?, json_value(&value, value_type)?);
            }
            // A row with a NULL argument (or, for the multi-argument form, a
            // NULL in ANY argument, which the caller has already collapsed to
            // one NULL) never reaches the sketch.
            (Partial::ApproxCountDistinct(_), None) => {
                return Err(ExecError::unsupported(
                    "APPROX_COUNT_DISTINCT requires an argument",
                ));
            }
            (Partial::ApproxCountDistinct(_), Some(Datum::Null)) => {}
            // The caller (the group-fold loop below) has already encoded the
            // row's argument tuple the way Go's `evalAndEncode` does; this
            // just feeds those bytes through FarmHash into the sketch.
            (Partial::ApproxCountDistinct(sketch), Some(Datum::Bytes(encoded))) => {
                sketch.insert(&encoded);
            }
            (Partial::ApproxCountDistinct(_), Some(_)) => {
                return Err(ExecError::unsupported(
                    "APPROX_COUNT_DISTINCT requires a pre-encoded argument tuple",
                ));
            }
            (Partial::ApproxPercentile { .. }, None) => {
                return Err(ExecError::unsupported(
                    "APPROX_PERCENTILE requires an argument",
                ));
            }
            (Partial::ApproxPercentile { .. }, Some(Datum::Null)) => {}
            (Partial::ApproxPercentile { values, .. }, Some(input)) => values.push(input),
            // COUNT(*): every row counts; COUNT(expr): NULL skipped.
            (Partial::Count(n), None) => *n += 1,
            (Partial::Count(_), Some(Datum::Null)) => {}
            (Partial::Count(n), Some(_)) => *n += 1,
            (Partial::FinalCount(_), None | Some(Datum::Null)) => {}
            (Partial::FinalCount(total), Some(Datum::Int(value))) => *total += value,
            (Partial::FinalCount(total), Some(Datum::UInt(value))) => {
                *total += i64::try_from(value)
                    .map_err(|_| ExecError::unsupported("partial COUNT exceeds i64"))?;
            }
            (Partial::FinalCount(_), Some(_)) => {
                return Err(ExecError::unsupported(
                    "final COUNT requires integer partial results",
                ));
            }
            (this @ Partial::SumDecimalFast { .. }, None | Some(Datum::Null)) => {
                // A NULL input contributes nothing to the accumulator.
                let _ = this;
            }
            (state @ Partial::SumDecimalFast { .. }, Some(input)) => {
                // Fold by coefficient when the input's scale matches; any
                // other shape materializes the exact Decimal state and this
                // row replays through the ordinary SumDecimal fold.
                let (current_sum, current_scale) = match state {
                    Partial::SumDecimalFast { sum, scale } => (*sum, *scale),
                    _ => unreachable!("matched arm"),
                };
                let coefficient = match input {
                    Datum::Int(ref v) => Some((i128::from(*v), 0u32)),
                    Datum::UInt(ref v) => i128::try_from(*v).ok().map(|unsigned| (unsigned, 0u32)),
                    Datum::Decimal(ref d) => d.coefficient_i128(),
                    _ => None,
                };
                let folded = coefficient.and_then(|(addend, addend_scale)| {
                    if addend_scale == current_scale {
                        current_sum.checked_add(addend)
                    } else {
                        None
                    }
                });
                if let Some(total) = folded {
                    match state {
                        Partial::SumDecimalFast { sum, .. } => *sum = total,
                        _ => {}
                    }
                } else {
                    let materialized = Decimal::from_scaled_i128(current_sum, current_scale);
                    let replayed = match input {
                        Datum::Int(ref v) => Decimal::from_int(*v).add(&materialized),
                        Datum::UInt(ref v) => Decimal::from_uint(*v).add(&materialized),
                        Datum::Decimal(ref d) => d.add(&materialized),
                        ref other => {
                            return Err(ExecError::unsupported(format!(
                                "SUM over {other:?} is not supported"
                            )));
                        }
                    };
                    *state = Partial::SumDecimal(Some(replayed));
                }
            }
            (Partial::SumDecimal(_) | Partial::SumReal(_), None) => {
                return Err(ExecError::unsupported("SUM requires an argument"));
            }
            (Partial::SumDecimal(_) | Partial::SumReal(_), Some(Datum::Null)) => {}
            (this @ Partial::SumDecimal(None), Some(input))
                if !matches!(input, Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_)) =>
            {
                // Go's `calculateSum` selects DOUBLE for every
                // non-integer/non-decimal input.
                *this = Partial::SumReal(Some(real_aggregate_value(&input, "SUM")?));
            }
            (Partial::SumDecimal(acc), Some(input)) => {
                let addend = match input {
                    Datum::Int(v) => Decimal::from_int(v),
                    Datum::UInt(v) => Decimal::from_uint(v),
                    Datum::Decimal(d) => d,
                    _ => {
                        return Err(ExecError::unsupported(
                            "SUM over this datum kind is not yet supported",
                        ));
                    }
                };
                *acc = Some(match acc.take() {
                    Some(sum) => sum.add(&addend),
                    None => addend,
                });
            }
            (Partial::SumReal(acc), Some(input)) => {
                *acc = Some(acc.unwrap_or(0.0) + real_aggregate_value(&input, "SUM")?);
            }
            // Go `builtinGroupConcat`: a NULL input contributes nothing at
            // all, and every other value is stringified before it is joined.
            (Partial::GroupConcat { .. }, None) => {
                return Err(ExecError::unsupported("GROUP_CONCAT requires an argument"));
            }
            (Partial::GroupConcat { .. }, Some(Datum::Null)) => {}
            (Partial::GroupConcat { values, .. }, Some(input)) => {
                values.push((group_concat_bytes(&input)?, sort_key));
            }
            (Partial::FirstRow(slot), value) => {
                if slot.is_none() {
                    slot.replace(value.unwrap_or(Datum::Null));
                }
            }
            (Partial::MaxMin { .. }, None) => {
                return Err(ExecError::unsupported("MIN/MAX requires an argument"));
            }
            (Partial::MaxMin { .. }, Some(Datum::Null)) => {}
            (Partial::MaxMin { value, is_max }, Some(input)) => match value {
                None => *value = Some(input),
                Some(current) => {
                    // Go `aggfuncs/builder.go:460-468`: MIN/MAX order under
                    // `collate.GetCollator(RetTp.GetCollate())`, so over a
                    // `utf8mb4_general_ci` column holding 'a','B','A' the
                    // answers are max=B, min=a -- NOT the binary a/A
                    // (captured from TiDB).
                    let ordering =
                        tidb_expr::compare_datums_with_collation(&input, current, collation)?;
                    if (*is_max && ordering == Ordering::Greater)
                        || (!*is_max && ordering == Ordering::Less)
                    {
                        *value = Some(input);
                    }
                }
            },
            (
                Partial::AvgDecimal { .. }
                | Partial::AvgDecimalFast { .. }
                | Partial::AvgReal { .. },
                None,
            ) => return Err(ExecError::unsupported("AVG requires an argument")),
            (
                Partial::AvgDecimal { .. }
                | Partial::AvgDecimalFast { .. }
                | Partial::AvgReal { .. },
                Some(Datum::Null),
            ) => {}
            (this @ Partial::AvgDecimal { count: 0, .. }, Some(input))
                if !matches!(input, Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_)) =>
            {
                // `calculateSum` returns a float datum for every
                // non-integer/non-decimal input, and AVG keeps that domain.
                *this = Partial::AvgReal {
                    sum: real_aggregate_value(&input, "AVG")?,
                    count: 1,
                };
            }
            (Partial::AvgDecimal { sum, count }, Some(input)) => {
                let addend = match input {
                    Datum::Int(v) => Decimal::from_int(v),
                    Datum::UInt(v) => Decimal::from_uint(v),
                    Datum::Decimal(d) => d,
                    _ => {
                        return Err(ExecError::unsupported(
                            "AVG over this datum kind is not yet supported",
                        ));
                    }
                };
                *sum = sum.add(&addend);
                *count += 1;
            }
            (this @ Partial::AvgDecimalFast { .. }, Some(input)) => {
                let _ = (this, input, extra, sort_key, collation);
                unreachable!("fast AVG is materialized before the update match");
            }
            // Go's bit functions cast the argument to `UNSIGNED BIGINT` and
            // skip NULL, so an all-NULL group keeps the identity.
            (Partial::Bit { .. }, None) => {
                return Err(ExecError::unsupported(
                    "BIT_AND/BIT_OR/BIT_XOR requires an argument",
                ));
            }
            (Partial::Bit { .. }, Some(Datum::Null)) => {}
            (Partial::Bit { acc, op }, Some(input)) => {
                let bits = datum_bits(&input)?;
                match op {
                    BitOp::And => *acc &= bits,
                    BitOp::Or => *acc |= bits,
                    BitOp::Xor => *acc ^= bits,
                }
            }
            (Partial::Variance { .. }, None) => {
                return Err(ExecError::unsupported(
                    "the variance/stddev family requires an argument",
                ));
            }
            (Partial::Variance { .. }, Some(Datum::Null)) => {}
            (
                Partial::Variance {
                    count,
                    sum,
                    variance,
                    ..
                },
                Some(input),
            ) => {
                // Go `varPop4Float64.UpdatePartialResult` +
                // `calculateIntermediate`, kept operation-for-operation so
                // the floating-point result is bit-identical.
                let value = input
                    .to_f64()
                    .map_err(|_| {
                        ExecError::unsupported("the variance/stddev family over this datum kind")
                    })?
                    .value;
                *count += 1;
                *sum += value;
                if *count > 1 {
                    let t = *count as f64 * value - *sum;
                    *variance += (t * t) / ((*count * (*count - 1)) as f64);
                }
            }
            (Partial::AvgReal { sum, count }, Some(input)) => {
                *sum += real_aggregate_value(&input, "AVG")?;
                *count += 1;
            }
        }
        Ok(())
    }

    /// The bytes this partial GROWS BY when it takes one more row, which is
    /// the aggregate half of Go's `memDelta`.
    ///
    /// Only the variants that RETAIN their inputs grow per row; the scalar
    /// folds (`COUNT`, `SUM`, `AVG`, `MIN`/`MAX`, the bit and variance
    /// families) hold a fixed-size accumulator and are already paid for by
    /// [`new_group_bytes`].
    ///
    /// DIVERGENCE (named): Go's `memDelta` also carries each accumulator's own
    /// bookkeeping. The DISTINCT value-set table is charged by [`MemorySet`];
    /// the `APPROX_COUNT_DISTINCT` sketch's rehash is represented by its
    /// retained hash payload. The payload is the term that grows without
    /// bound.
    fn retained_row_bytes(&self, value: Option<&Datum>) -> i64 {
        let retains = matches!(
            self,
            Partial::GroupConcat { .. }
                | Partial::JsonArrayAgg(..)
                | Partial::JsonObjectAgg(..)
                | Partial::ApproxPercentile { .. }
        );
        if !retains {
            return 0;
        }
        let payload = value.map_or(0, tidb_datatype::Datum::estimated_mem_usage);
        i64::try_from(payload + size_of::<Datum>()).unwrap_or(i64::MAX)
    }

    fn finish(
        &self,
        order_by: &[(Expression, bool)],
        div_precision_increment: u32,
    ) -> Result<Datum, ExecError> {
        Ok(match self {
            Partial::Count(n) | Partial::FinalCount(n) => Datum::Int(*n),
            // An empty group is SQL NULL, not the empty document `[]`/`{}`.
            Partial::JsonArrayAgg(entries, _) if entries.is_empty() => Datum::Null,
            Partial::JsonArrayAgg(entries, _) => encode_json(BinaryJSONValue::Array(
                entries
                    .iter()
                    .cloned()
                    .map(BinaryJSONValue::Binary)
                    .collect(),
            ))?,
            Partial::JsonObjectAgg(entries, _, _) if entries.is_empty() => Datum::Null,
            Partial::JsonObjectAgg(entries, _, _) => encode_json(BinaryJSONValue::Object(
                entries
                    .iter()
                    .map(|(key, value)| (key.clone(), BinaryJSONValue::Binary(value.clone())))
                    .collect(),
            ))?,
            Partial::ApproxCountDistinct(sketch) => Datum::Int(sketch.fixed_size() as i64),
            // Go `percentile`: ordinal rank `k = min(ceil(N * pct/100), N)`,
            // then the k-th smallest value ITSELF (`selection.Select`), so an
            // even-sized group returns a real element rather than the mean of
            // the middle two.
            Partial::ApproxPercentile { values, percent } => {
                let Some(percent) = percent.filter(|_| !values.is_empty()) else {
                    return Ok(Datum::Null);
                };
                let mut values = values.clone();
                let rank = ((values.len() as f64 * (percent as f64 / 100.0)).ceil() as usize)
                    .clamp(1, values.len());
                let index = select(&mut DatumSelection(&mut values), rank)
                    .expect("nonempty percentile input must produce a selected index");
                values[index].clone()
            }
            // An empty group concatenates to NULL, not an empty string.
            Partial::GroupConcat { values, .. } if values.is_empty() => Datum::Null,
            Partial::GroupConcat { values, separator } => {
                // Go sorts the collected rows by the aggregate's own ORDER BY
                // before joining them; without one the rows keep arrival
                // order, which MySQL documents as undefined.
                let mut values = values.clone();
                if !order_by.is_empty() {
                    // Go `buildGroupConcat` builds one collator per byItem
                    // from that item's own `RetType`, so `GROUP_CONCAT(s
                    // ORDER BY s)` over a `_ci` column holding 'b','A','a','B'
                    // yields `a,A,b,B` rather than the byte order `A,B,a,b`
                    // (captured from TiDB).
                    let collations: Vec<tidb_datatype::Collation> = order_by
                        .iter()
                        .map(|(expr, _)| expr_collation(expr))
                        .collect();
                    values.sort_by(|left, right| {
                        for (position, (_, desc)) in order_by.iter().enumerate() {
                            let (Some(a), Some(b)) = (left.1.get(position), right.1.get(position))
                            else {
                                continue;
                            };
                            let collation = collations
                                .get(position)
                                .copied()
                                .unwrap_or(tidb_datatype::Collation::DEFAULT);
                            let ordering =
                                tidb_expr::compare_datums_with_collation(a, b, collation)
                                    .unwrap_or(Ordering::Equal);
                            if ordering != Ordering::Equal {
                                return if *desc { ordering.reverse() } else { ordering };
                            }
                        }
                        Ordering::Equal
                    });
                }
                let mut joined = Vec::new();
                for (index, (value, _)) in values.iter().enumerate() {
                    if index > 0 {
                        joined.extend_from_slice(separator.as_bytes());
                    }
                    joined.extend_from_slice(value);
                }
                Datum::Bytes(joined)
            }
            Partial::SumDecimal(None) | Partial::SumReal(None) => Datum::Null,
            Partial::SumDecimal(Some(v)) => Datum::Decimal(v.clone()),
            Partial::SumDecimalFast { sum, scale } => {
                Datum::Decimal(Decimal::from_scaled_i128(*sum, *scale))
            }
            Partial::SumReal(Some(v)) => Datum::Real(*v),
            Partial::FirstRow(v) => v.clone().unwrap_or(Datum::Null),
            Partial::MaxMin { value, .. } => value.clone().unwrap_or(Datum::Null),
            // Go divides the exact sum by the count with the session's
            // div_precision_increment, the same rule the `/` operator follows.
            Partial::AvgDecimal { count: 0, .. } | Partial::AvgReal { count: 0, .. } => Datum::Null,
            Partial::AvgDecimal { sum, count } => {
                let divisor = Decimal::from_int(*count);
                let target_scale = sum.scale() + div_precision_increment;
                match sum.true_div(&divisor, target_scale) {
                    Some(quotient) => Datum::Decimal(quotient),
                    None => Datum::Null,
                }
            }
            Partial::AvgDecimalFast { sum, scale, count } => {
                if *count == 0 {
                    Datum::Null
                } else {
                    let sum = Decimal::from_scaled_i128(*sum, *scale);
                    let divisor = Decimal::from_int(*count);
                    let target_scale = sum.scale() + div_precision_increment;
                    match sum.true_div(&divisor, target_scale) {
                        Some(quotient) => Datum::Decimal(quotient),
                        None => Datum::Null,
                    }
                }
            }
            Partial::AvgReal { sum, count } => Datum::Real(sum / *count as f64),
            // Go `typeInfer4BitFuncs` marks the column `UnsignedFlag`, and
            // `func_bitfuncs.go`'s `AppendFinalResult2Chunk` does
            // `AppendUint64`, so the fold is printed as the unsigned value:
            // `BIT_AND` over an all-NULL group is `18446744073709551615`,
            // not `-1` (captured from TiDB).
            Partial::Bit { acc, .. } => Datum::UInt(*acc),
            // Go: population variance divides by `count`, sample variance by
            // `count - 1` and is NULL for a single row; both are NULL for an
            // empty (or all-NULL) input.
            Partial::Variance {
                count,
                variance,
                sample,
                sqrt,
                ..
            } => {
                let divisor = if *sample { *count - 1 } else { *count };
                if divisor <= 0 {
                    Datum::Null
                } else {
                    let value = variance / divisor as f64;
                    Datum::Real(if *sqrt { value.sqrt() } else { value })
                }
            }
        })
    }
}

/// One JSON aggregate input as a JSON value, Go's `getRealJSONValue` followed
/// by `CreateBinaryJSONWithCheck`'s per-element conversion: a NULL datum is
/// JSON `null`, a DECIMAL becomes a DOUBLE, a JSON column's value is carried
/// through unchanged, and a BINARY-charset string (`value_type`'s charset is
/// `binary`) wraps into a JSON `Opaque` tagged with `value_type`'s own MySQL
/// type code rather than becoming a JSON string.
fn json_value(value: &Datum, value_type: &FieldType) -> Result<BinaryJSON, ExecError> {
    value
        .to_mysql_json_with_source_type(value_type)
        .map_err(|_| ExecError::unsupported("this datum kind is not a JSON value"))
}

/// `JSON_OBJECTAGG`'s member name: Go reads the key argument with
/// `EvalString`, so a non-string key is stringified (`JSON_OBJECTAGG(id, v)`
/// keys the object with `"1"`, `"2"`, ...).
fn json_object_key(value: &Datum) -> Result<String, ExecError> {
    value
        .sql_string()
        .map_err(|_| ExecError::unsupported("this datum kind is not a JSON member name"))
}

/// Encodes a finished JSON aggregate, Go's `CreateBinaryJSONWithCheck`.
fn encode_json(value: BinaryJSONValue) -> Result<Datum, ExecError> {
    BinaryJSON::from_typed_value(&value)
        .map(Datum::Json)
        .map_err(|_| ExecError::unsupported("this JSON document cannot be encoded"))
}

/// The 64 bits one `BIT_AND`/`BIT_OR`/`BIT_XOR` input contributes: Go casts
/// the argument to `UNSIGNED BIGINT`, so a negative integer folds as its
/// two's-complement pattern.
fn datum_bits(value: &Datum) -> Result<u64, ExecError> {
    Ok(match value {
        Datum::UInt(bits) => *bits,
        other => {
            other
                .to_i64()
                .map_err(|_| ExecError::unsupported("BIT_AND/BIT_OR/BIT_XOR over this datum kind"))?
                .value as u64
        }
    })
}

/// Folds one aggregate over an explicit value list, returning its result.
///
/// This is the same accumulate-then-finish path a GROUP BY group takes, reached
/// without a group key so a WINDOW FRAME can aggregate an arbitrary slice of a
/// partition (see `crate::window`). Reusing it is what keeps SUM's
/// integers-summed-in-the-decimal-domain rule, AVG's `div_precision_increment`
/// division and MIN/MAX's datum comparison identical between the two callers.
/// `collation` is the argument expression's derived collation, which is what
/// keeps a windowed `MAX(ci_col)` agreeing with the grouped one.
/// `None` stands for `COUNT(*)`'s absent argument; every other aggregate takes
/// `Some(value)`, with `Some(Datum::Null)` for a NULL input. Each item pairs
/// that first argument with the values of any further arguments, exactly the
/// pair the GROUP BY path builds per source row.
pub(crate) fn aggregate_rows(
    kind: &AggKind,
    rows: impl IntoIterator<Item = (Option<Datum>, Vec<Datum>)>,
    div_precision_increment: u32,
    collation: tidb_datatype::Collation,
    output_type: &FieldType,
) -> Result<Datum, ExecError> {
    let mut partial = Partial::new(kind);
    for (value, extra) in rows {
        partial.update(value, &extra, Vec::new(), collation)?;
    }
    let value = partial.finish(&[], div_precision_increment)?;
    Ok(round_avg_result(kind, output_type, value))
}

/// Go `baseAvgDecimal.AppendFinalResult2Chunk` rounds `DecimalDiv`'s hidden
/// base-1e9 fraction words to the inferred AVG return scale before appending
/// the value. Keeping that step here makes HashAgg, StreamAgg, and window AVG
/// share the same result contract.
fn round_avg_result(kind: &AggKind, output_type: &FieldType, value: Datum) -> Datum {
    let Datum::Decimal(value) = value else {
        return value;
    };
    if !matches!(kind, AggKind::Avg) {
        return Datum::Decimal(value);
    }
    let scale = if output_type.decimal() == UNSPECIFIED_LENGTH {
        MAX_DECIMAL_SCALE
    } else {
        output_type.decimal()
    };
    Datum::Decimal(value.round_to_scale(scale as i32))
}

/// Finishes one aggregate state, including GROUP_CONCAT's statement warning
/// and byte limit. Both hash and stream aggregation use this exact path so a
/// physical algorithm choice cannot change an aggregate's value semantics.
fn finish_agg_value<C: Columns>(
    state: &mut AggState,
    func: &AggFunc,
    output_type: &FieldType,
    ctx: &C,
    truncated: &mut bool,
) -> Result<Datum, ExecError> {
    let mut value = state
        .partial
        .finish(&func.order_by, ctx.div_precision_increment())?;
    value = round_avg_result(&func.kind, output_type, value);
    if let Datum::Bytes(joined) = &mut value {
        if matches!(func.kind, AggKind::GroupConcat { .. }) {
            let max_len = group_concat_max_len(ctx);
            if max_len > 0 && joined.len() as u64 > max_len {
                joined.truncate(max_len as usize);
                if !*truncated {
                    *truncated = true;
                    let text = group_concat_arg_text(func);
                    ctx.handle_group_concat_cut(&format!(
                        "Some rows were cut by GROUPCONCAT({text})"
                    ))?;
                }
            }
        }
    }
    Ok(value)
}

/// Go `StreamAggExec` for a global aggregate (an empty group-by list).
///
/// A global aggregate is already one ordered group, so no hash table or row
/// materialization is needed: each child row is folded into the one vector of
/// partial results and that vector emits exactly once, including for empty
/// input. The driver selects this executor only for Go plan shapes that choose
/// StreamAgg; grouped input continues through [`HashAggExec`].
pub struct StreamAggExec<C: Columns> {
    meta: ExecutorMeta,
    agg_funcs: Vec<AggFunc>,
    child: Box<dyn Executor>,
    ctx: C,
    child_chunk: Chunk,
    states: Vec<AggState>,
    truncated: Vec<bool>,
    emitted: bool,
    child_returned_empty: bool,
}

impl<C: Columns> StreamAggExec<C> {
    /// Builds a one-group streaming aggregation over `child`.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        agg_funcs: Vec<AggFunc>,
        child: Box<dyn Executor>,
        ctx: C,
    ) -> Self {
        let child_chunk = child.new_chunk();
        let states = agg_funcs.iter().map(AggState::new).collect();
        let truncated = vec![false; agg_funcs.len()];
        Self {
            meta,
            agg_funcs,
            child,
            ctx,
            child_chunk,
            states,
            truncated,
            emitted: false,
            child_returned_empty: true,
        }
    }

    fn update_row(
        agg_funcs: &[AggFunc],
        ctx: &C,
        states: &mut [AggState],
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<(), ExecError> {
        for index in 0..agg_funcs.len() {
            let func = &agg_funcs[index];
            let mut extra_values = Vec::new();
            let input = eval_agg_input(func, ctx, row, &mut extra_values)?;
            let mut sort_key = Vec::with_capacity(func.order_by.len());
            for (expr, _) in &func.order_by {
                sort_key.push(expr.eval(ctx, row)?);
            }
            if let Some((coefficient, scale)) = input.decimal_coefficient {
                if states[index].partial_update_with_coefficient(coefficient, scale) {
                    continue;
                }
            }
            states[index].update(input.value, &extra_values, sort_key, input.distinct_key)?;
        }
        Ok(())
    }
}

impl<C: Columns> Executor for StreamAggExec<C> {
    fn agg_tree_input_empty(&self) -> bool {
        self.child_returned_empty
    }

    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.child_chunk.reset();
        self.states = self.agg_funcs.iter().map(AggState::new).collect();
        self.truncated.fill(false);
        self.emitted = false;
        self.child_returned_empty = true;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.emitted {
            return Ok(());
        }
        // A global COUNT(*)/COUNT(1) only needs the exact child cardinality.
        // Go's stream aggregate uses this route for a count-only parent,
        // avoiding materialization when the child is a derived join/UNION.
        let count_all_rows = self.agg_funcs.len() == 1
            && matches!(self.agg_funcs[0].kind, AggKind::Count)
            && !self.agg_funcs[0].distinct
            && self.agg_funcs[0].extra_args.is_empty()
            && self.agg_funcs[0].order_by.is_empty()
            && match self.agg_funcs[0].arg.as_ref() {
                None => true,
                Some(Expression::Constant(constant)) => {
                    matches!(constant.value, Datum::Int(1) | Datum::UInt(1))
                }
                Some(_) => false,
            };
        let mut counted = false;
        if count_all_rows {
            if let Some(count) = self.child.row_count()? {
                let count = i64::try_from(count)
                    .map_err(|_| ExecError::unsupported("COUNT exceeds signed BIGINT"))?;
                self.states = vec![AggState::new(&self.agg_funcs[0])];
                self.states[0].partial = Partial::Count(count);
                self.child_returned_empty = count == 0;
                counted = true;
            }
        }
        if !counted {
            loop {
                self.child.next(&mut self.child_chunk)?;
                let rows = self.child_chunk.num_rows();
                if rows == 0 {
                    break;
                }
                self.child_returned_empty = false;
                for row_index in 0..rows {
                    Self::update_row(
                        &self.agg_funcs,
                        &self.ctx,
                        &mut self.states,
                        self.child_chunk.get_row(row_index),
                    )?;
                }
                self.child_chunk.reset();
            }
        }
        if self.agg_funcs.is_empty() {
            req.set_num_virtual_rows(1);
        } else {
            for index in 0..self.agg_funcs.len() {
                let value = finish_agg_value(
                    &mut self.states[index],
                    &self.agg_funcs[index],
                    &self.meta.ret_field_types()[index],
                    &self.ctx,
                    &mut self.truncated[index],
                )?;
                req.append_datum(index, &value);
            }
        }
        self.emitted = true;
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.states.clear();
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

/// Go `StreamAggExec` for input already ordered by every `GROUP BY` item.
///
/// Unlike [`HashAggExec`], this operator holds one group at a time. A change
/// in the encoded group key finishes the current aggregate states before the
/// first row of the next group is folded. The driver constructs it only after
/// the child reports the required physical order, so equal keys are contiguous
/// and a completed group can never reappear later in the stream.
pub struct GroupedStreamAggExec<C: Columns> {
    meta: ExecutorMeta,
    group_by: Vec<Expression>,
    agg_funcs: Vec<AggFunc>,
    /// Output column for each aggregate state. This separates TiKV's
    /// function-first physical state order from the aggregation schema order
    /// Go exposes without an extra Projection.
    output_positions: Vec<usize>,
    child: Box<dyn Executor>,
    ctx: C,
    child_chunk: Chunk,
    child_at: usize,
    states: Vec<AggState>,
    truncated: Vec<bool>,
    current_key: Option<Vec<u8>>,
    child_done: bool,
    child_returned_empty: bool,
}

impl<C: Columns> GroupedStreamAggExec<C> {
    /// Builds a grouped streaming aggregation over an already ordered child.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        group_by: Vec<Expression>,
        agg_funcs: Vec<AggFunc>,
        output_positions: Vec<usize>,
        child: Box<dyn Executor>,
        ctx: C,
    ) -> Self {
        debug_assert!(!group_by.is_empty());
        debug_assert_eq!(agg_funcs.len(), output_positions.len());
        debug_assert!(
            output_positions
                .iter()
                .copied()
                .all(|position| position < agg_funcs.len())
        );
        debug_assert!((0..agg_funcs.len()).all(|position| output_positions.contains(&position)));
        let child_chunk = child.new_chunk();
        let states = agg_funcs.iter().map(AggState::new).collect();
        let truncated = vec![false; agg_funcs.len()];
        Self {
            meta,
            group_by,
            agg_funcs,
            output_positions,
            child,
            ctx,
            child_chunk,
            child_at: 0,
            states,
            truncated,
            current_key: None,
            child_done: false,
            child_returned_empty: true,
        }
    }

    fn group_key(&self, row: tidb_chunk::row::Row<'_>) -> Result<Vec<u8>, ExecError> {
        let mut key = Vec::new();
        for expr in &self.group_by {
            let datum = expr.eval(&self.ctx, row)?;
            key.extend_from_slice(&group_key_part(&expr_collation(expr), &datum));
            key.push(0xff);
        }
        Ok(key)
    }

    fn emit_current(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        for index in 0..self.states.len() {
            let output_position = self.output_positions[index];
            let value = finish_agg_value(
                &mut self.states[index],
                &self.agg_funcs[index],
                &self.meta.ret_field_types()[output_position],
                &self.ctx,
                &mut self.truncated[index],
            )?;
            req.append_datum(output_position, &value);
        }
        Ok(())
    }
}

impl<C: Columns> Executor for GroupedStreamAggExec<C> {
    fn agg_tree_input_empty(&self) -> bool {
        self.child_returned_empty
    }

    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.child_chunk.reset();
        self.child_at = 0;
        self.states = self.agg_funcs.iter().map(AggState::new).collect();
        self.truncated.fill(false);
        self.current_key = None;
        self.child_done = false;
        self.child_returned_empty = true;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        let cap = self.meta.max_chunk_size();
        while req.num_rows() < cap {
            if self.child_done {
                if self.current_key.take().is_some() {
                    self.emit_current(req)?;
                }
                break;
            }
            if self.child_at >= self.child_chunk.num_rows() {
                self.child_chunk.reset();
                self.child.next(&mut self.child_chunk)?;
                self.child_at = 0;
                if self.child_chunk.num_rows() == 0 {
                    self.child_done = true;
                    continue;
                }
            }

            let key = self.group_key(self.child_chunk.get_row(self.child_at))?;
            if self
                .current_key
                .as_ref()
                .is_some_and(|current| current != &key)
            {
                self.emit_current(req)?;
                self.states = self.agg_funcs.iter().map(AggState::new).collect();
            }
            self.current_key = Some(key);
            let row = self.child_chunk.get_row(self.child_at);
            StreamAggExec::<C>::update_row(&self.agg_funcs, &self.ctx, &mut self.states, row)?;
            self.child_at += 1;
            self.child_returned_empty = false;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.states.clear();
        self.current_key = None;
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

/// Go `HashAggExec` (serial `unparallelExec`): hash aggregation over the
/// child's rows, in ROUNDS when the statement's memory quota forces a spill.
///
/// # The round structure, which is Go's and only visible under spill
///
/// Without a spill there is exactly one round: drain the child, fold every row
/// into its group, emit the groups. Under a spill (see [`crate::agg_spill`]
/// for the trigger) a round STOPS OPENING NEW GROUPS -- each row whose group
/// this round does not already hold is written back to a spill file untouched
/// -- finishes and emits the groups it does hold, releases them, and starts
/// the next round by re-reading the rows it deferred. A group is therefore
/// opened, completed and emitted inside ONE round, which is why this arm never
/// has to merge two partial results for the same key.
///
/// The observable consequence, and it is Go's: the groups of a spilled
/// aggregation come out in a different ORDER than the same aggregation
/// unspilled (rounds, then first-seen within a round). A `GROUP BY` without an
/// `ORDER BY` guarantees no order in either engine.
/// The `HashAggContext` bound carries the parallel-pipeline capability
/// declaration every context must make (see [`hash_agg::parallel`]).
pub struct HashAggExec<C: HashAggContext> {
    meta: ExecutorMeta,
    group_by: Vec<Expression>,
    /// Present when every GROUP BY expression is a resolved integer column.
    /// Go's vectorized hash aggregation reads those typed chunk cells
    /// directly; retaining the shape here avoids one Datum construction per
    /// key and input row while keeping computed/mixed keys on the evaluator.
    integer_group_columns: Option<Vec<(usize, bool)>>,
    agg_funcs: Vec<AggFunc>,
    child: Box<dyn Executor>,
    ctx: C,
    child_chunk: Chunk,
    /// Go `HashAggExec.IsChildReturnEmpty`: whether the last run saw NO input
    /// row at all -- read back through [`Executor::agg_tree_input_empty`].
    child_returned_empty: bool,

    // --- Go's `unparallelExec` state machine ---
    /// Group key -> index into `ordered`. Go's `groupSet` + `partialResultMap`.
    groups: FastBytesMap<usize>,
    /// Scratch storage for the current row's encoded group key. The key is
    /// moved into `groups` only when a new group is opened; repeated rows
    /// reuse this allocation instead of allocating one `Vec` per row.
    group_key_buffer: Vec<u8>,
    /// Compact bucket index for the one-column binary string fast path. The
    /// complete key stays in `direct_string_keys` and is compared after every
    /// bucket hit, so a fingerprint collision cannot merge SQL groups.
    direct_string_buckets: DirectStringBucketMap<usize>,
    direct_string_collisions: DirectStringBucketMap<Vec<usize>>,
    direct_string_keys: Vec<Vec<u8>>,
    group_collations: Vec<tidb_datatype::Collation>,
    /// The open groups' states, in first-seen order (Go's `groupKeys`). Group
    /// `g` occupies `g * agg_funcs.len()..(g + 1) * agg_funcs.len()` so the
    /// hot path does not allocate one inner `Vec` per group.
    ordered: Vec<AggState>,
    group_count: usize,
    /// Go `cursor4GroupKey`: how many of `ordered` this round has emitted.
    cursor: usize,
    /// Go `prepared`: the current round's groups are complete and emitting.
    prepared: bool,
    /// Go `executed`: every row, including every spilled one, is accounted for.
    executed: bool,
    /// The per-GROUP_CONCAT "already warned" sentinel. It lives here rather
    /// than in a round because MySQL emits exactly ONE 1260 per function per
    /// STATEMENT, and a spilled aggregation emits its groups over several
    /// rounds (`func_group_concat.go:56-60`).
    truncated: Vec<bool>,

    // --- spill (`agg_spill.go`'s `AggSpillDiskAction` arm) ---
    /// The statement's budget: this operator's tracker hangs off it and its
    /// quota is what the spill action and the cancellation both watch.
    memory: StatementMemory,
    /// Go `HashAggExec.memTracker`.
    tracker: Arc<Tracker>,
    /// Go `HashAggExec.diskTracker`.
    disk_tracker: Arc<disk::Tracker>,
    /// Go `HashAggExec.inSpillMode`, raised by the action.
    in_spill_mode: Arc<AtomicBool>,
    /// The action registered on the session tracker's SOFT-limit slot, kept so
    /// `close` can finish it (Go `Close`: `e.spillAction.SetFinished()`).
    spill_action: Option<Arc<AggSpillDiskAction>>,
    /// Go `HashAggExec.dataInDisk`, created on the first spill.
    data_in_disk: Option<DataInDiskByChunks>,
    /// Go `HashAggExec.tmpChkForSpill`.
    tmp_chk_for_spill: Chunk,
    /// Go `numOfSpilledChks`: chunks spilled as of the last round boundary.
    num_of_spilled_chks: usize,
    /// Go `offsetOfSpilledChks`: the next spilled chunk to re-read.
    offset_of_spilled_chks: usize,
    /// Go `isChildDrained`.
    is_child_drained: bool,
    /// Output rows produced by the bounded parallel integer fast path.
    /// Flattened row-major output; one inner `Vec` per group made q13 spend
    /// most of its time in the allocator while dropping 150k rows.
    parallel_output: Vec<Datum>,
    parallel_output_width: usize,
    parallel_output_cursor: usize,
    parallel_output_active: bool,
    parallel_agg_windows: usize,
    /// The parallel partial/final worker pipeline is engaged for this Open
    /// (Go `parallelExecValid`). Decided once per Open; `execute` never
    /// re-decides mid-run.
    pipeline_mode: bool,
    /// Resolved worker counts for the current Open (diagnostics).
    pipeline_partial_concurrency: usize,
    pipeline_final_concurrency: usize,
    /// Test/diagnostic override standing in for SET concurrency variables.
    pipeline_concurrency_override: Option<(usize, usize)>,
    /// Diagnostics shared with the pipeline's workers while it runs.
    pipeline_stats: Option<Arc<parallel::PipelineStats>>,
}

impl<C: HashAggContext> HashAggExec<C> {
    /// Builds a hash aggregation of `agg_funcs` over `child`, grouped by
    /// `group_by` (empty for a global aggregate).
    ///
    /// `memory` is the statement's budget, required for the same reason
    /// [`crate::sort::SortExec::new`] requires it: a call site must not be
    /// able to build an UNACCOUNTED group table by omitting it.
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        group_by: Vec<Expression>,
        agg_funcs: Vec<AggFunc>,
        child: Box<dyn Executor>,
        ctx: C,
        memory: StatementMemory,
    ) -> Self {
        let child_chunk = child.new_chunk();
        let tmp_chk_for_spill = child.new_chunk();
        let tracker = memory.operator_tracker(meta.id());
        let disk_tracker = memory.operator_disk_tracker(meta.id());
        let truncated = vec![false; agg_funcs.len()];
        let group_collations = group_by.iter().map(expr_collation).collect();
        let integer_group_columns = group_by
            .iter()
            .map(|expr| {
                let column = expr.as_column()?;
                let index = usize::try_from(column.index).ok()?;
                let field_type = column.get_static_type()?;
                (field_type.eval_type() == EvalType::Int)
                    .then_some((index, field_type.is_unsigned()))
            })
            .collect::<Option<Vec<_>>>();
        HashAggExec {
            meta,
            group_by,
            integer_group_columns,
            agg_funcs,
            child,
            ctx,
            child_chunk,
            child_returned_empty: true,
            groups: FastBytesMap::default(),
            group_key_buffer: Vec::new(),
            direct_string_buckets: DirectStringBucketMap::default(),
            direct_string_collisions: DirectStringBucketMap::default(),
            direct_string_keys: Vec::new(),
            group_collations,
            ordered: Vec::new(),
            group_count: 0,
            cursor: 0,
            prepared: false,
            executed: false,
            truncated,
            memory,
            tracker,
            disk_tracker,
            in_spill_mode: Arc::new(AtomicBool::new(false)),
            spill_action: None,
            data_in_disk: None,
            tmp_chk_for_spill,
            num_of_spilled_chks: 0,
            offset_of_spilled_chks: 0,
            is_child_drained: false,
            parallel_output: Vec::new(),
            parallel_output_width: 0,
            parallel_output_cursor: 0,
            parallel_output_active: false,
            parallel_agg_windows: 0,
            pipeline_mode: false,
            pipeline_partial_concurrency: 1,
            pipeline_final_concurrency: 1,
            pipeline_concurrency_override: None,
            pipeline_stats: None,
        }
    }

    /// Number of bounded batches processed by the integer aggregate worker
    /// path. Exposed for regression tests and local performance diagnostics.
    #[cfg(test)]
    pub(crate) fn parallel_agg_windows(&self) -> usize {
        self.parallel_agg_windows
    }

    fn parallel_int_agg_specs(&self) -> Option<(usize, bool, Vec<ParallelIntAggSpec>)> {
        // Keep this path deliberately small: q13 and the common pushed-down
        // hash aggregate use one direct integer key and only COUNT/FIRST_ROW.
        // The bounded worker path does not participate in round-based spill.
        // Keep low-quota statements on the serial implementation where the
        // tracker and, when enabled, spill action enforce Go's memory
        // contract. This applies even when temporary storage is disabled:
        // that shape must still reach the statement's 8175 cancellation.
        // Normal 1 GiB query budgets (including TPC-H) have ample headroom for
        // this bounded worker path.
        if (self.memory.quota() > 0 && self.memory.quota() < 256 * 1024 * 1024)
            || self.group_by.len() != 1
        {
            return None;
        }
        if self.agg_funcs.is_empty() {
            return None;
        }
        let group_column = self.group_by[0].as_column()?;
        let group_index = usize::try_from(group_column.index).ok()?;
        let group_type = group_column.get_static_type()?;
        if group_type.eval_type() != EvalType::Int {
            return None;
        }
        let mut specs = Vec::with_capacity(self.agg_funcs.len());
        for f in &self.agg_funcs {
            if f.distinct
                || !f.order_by.is_empty()
                || (!f.extra_args.is_empty() && !matches!(f.kind, AggKind::Avg))
            {
                return None;
            }
            match &f.kind {
                AggKind::Count => {
                    let column = match &f.arg {
                        None => None,
                        Some(expr) => Some(usize::try_from(expr.as_column()?.index).ok()?),
                    };
                    specs.push(ParallelIntAggSpec::Count(column));
                }
                AggKind::FinalCount => {
                    let expr = f.arg.as_ref()?.as_column()?;
                    let field_type = expr.get_static_type()?;
                    if field_type.eval_type() != EvalType::Int {
                        return None;
                    }
                    specs.push(ParallelIntAggSpec::FinalCount {
                        column: usize::try_from(expr.index).ok()?,
                        unsigned: field_type.is_unsigned(),
                    });
                }
                AggKind::Avg => {
                    let count_expr = f.arg.as_ref()?.as_column()?;
                    let count_type = count_expr.get_static_type()?;
                    if count_type.eval_type() != EvalType::Int || f.extra_args.len() != 1 {
                        return None;
                    }
                    let sum_expr = f.extra_args[0].as_column()?;
                    let sum_type = sum_expr.get_static_type()?;
                    if sum_type.code() != tidb_datatype::FieldTypeCode::NewDecimal {
                        return None;
                    }
                    specs.push(ParallelIntAggSpec::FinalAvgDecimal {
                        count_column: usize::try_from(count_expr.index).ok()?,
                        count_unsigned: count_type.is_unsigned(),
                        sum_column: usize::try_from(sum_expr.index).ok()?,
                    });
                }
                AggKind::FirstRow => {
                    let expr = f.arg.as_ref()?.as_column()?;
                    let field_type = expr.get_static_type()?;
                    if field_type.eval_type() != EvalType::Int {
                        return None;
                    }
                    specs.push(ParallelIntAggSpec::FirstRow {
                        column: usize::try_from(expr.index).ok()?,
                        field_type: field_type.clone(),
                    });
                }
                _ => return None,
            }
        }
        Some((group_index, group_type.is_unsigned(), specs))
    }

    /// Returns the direct binary string GROUP BY shape used by Web3Bench.
    /// Binary collations can use the source bytes as the hash key; all other
    /// collations stay on the complete collation-aware expression path.
    fn direct_string_group_column(&self) -> Option<(usize, tidb_datatype::Collation)> {
        if self.group_by.len() != 1 {
            return None;
        }
        let column = self.group_by[0].as_column()?;
        let offset = usize::try_from(column.index).ok()?;
        let field_type = column.get_static_type()?;
        if !matches!(
            field_type.code(),
            FieldTypeCode::String | FieldTypeCode::VarString | FieldTypeCode::Varchar
        ) {
            return None;
        }
        let collation = expr_collation(&self.group_by[0]);
        let bytes_per_char = match collation {
            tidb_datatype::Collation::Binary => 1,
            tidb_datatype::Collation::Utf8Mb4Bin
            | tidb_datatype::Collation::Utf8Mb40900Bin => 4,
            _ => return None,
        };
        if field_type
            .flen()
            .checked_mul(bytes_per_char)
            .is_none_or(|width| width < 0 || width as usize > DIRECT_STRING_MAX_KEY_BYTES)
        {
            return None;
        }
        matches!(
            collation,
            tidb_datatype::Collation::Binary
                | tidb_datatype::Collation::Utf8Mb4Bin
                | tidb_datatype::Collation::Utf8Mb40900Bin
        )
        .then_some((offset, collation))
    }

    fn direct_string_group_index(&self, fingerprint: u64, key: &[u8]) -> Option<usize> {
        let primary = *self.direct_string_buckets.get(&fingerprint)?;
        if self.direct_string_keys[primary] == key {
            return Some(primary);
        }
        self.direct_string_collisions
            .get(&fingerprint)
            .and_then(|candidates| {
                candidates
                    .iter()
                    .copied()
                    .find(|index| self.direct_string_keys[*index] == key)
            })
    }

    fn open_direct_string_group(&mut self, fingerprint: u64) -> (usize, i64) {
        let idx = self.group_count;
        let capacity = self.group_key_buffer.capacity();
        let key = std::mem::replace(
            &mut self.group_key_buffer,
            Vec::with_capacity(capacity),
        );
        let bytes = new_group_bytes(key.len(), self.agg_funcs.len());
        self.direct_string_keys.push(key);
        match self.direct_string_buckets.entry(fingerprint) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(idx);
            }
            std::collections::hash_map::Entry::Occupied(_) => {
                self.direct_string_collisions
                    .entry(fingerprint)
                    .or_default()
                    .push(idx);
            }
        }
        self.ordered
            .extend(self.agg_funcs.iter().map(AggState::new));
        self.group_count += 1;
        (idx, bytes)
    }

    /// Reserve the direct group's hot-path containers once the first input
    /// chunk arrives.  The pushed-down Web3Bench aggregate receives a bounded
    /// partial result (about 80K groups here); growing three independent
    /// vectors/maps one bucket at a time otherwise adds several rehash and
    /// relocation rounds before the final TopN can run.
    fn reserve_direct_string_groups(&mut self) {
        if self.group_count != 0
            || !self.direct_string_buckets.is_empty()
            || !self.direct_string_keys.is_empty()
        {
            return;
        }
        let estimate = self
            .child
            .row_count()
            .ok()
            .flatten()
            .unwrap_or(0)
            .clamp(1024, 131_072) as usize;
        self.direct_string_buckets.reserve(estimate);
        self.direct_string_collisions.reserve(estimate / 64);
        self.direct_string_keys.reserve(estimate);
        self.ordered
            .reserve(estimate.saturating_mul(self.agg_funcs.len()));
    }

    /// The Web3Bench grouped shape has one binary string key and only
    /// column-based COUNT/SUM functions.  Keep this descriptor narrow: the
    /// columnar fold below deliberately bypasses expression evaluation, so a
    /// computed argument, DISTINCT, or an order-sensitive aggregate must stay
    /// on the complete Go-compatible path.
    fn direct_string_aggregate_specs(&self) -> Option<Vec<DirectStringAgg>> {
        if self.agg_funcs.is_empty() {
            return None;
        }
        self.agg_funcs
            .iter()
            .map(|func| {
                if func.distinct || !func.extra_args.is_empty() || !func.order_by.is_empty() {
                    return None;
                }
                match func.kind {
                    AggKind::Count => {
                        let column = match func.arg.as_ref() {
                            None => None,
                            Some(expr) => Some(usize::try_from(expr.as_column()?.index).ok()?),
                        };
                        Some(DirectStringAgg::Count(column))
                    }
                    AggKind::FinalCount => {
                        let column = func.arg.as_ref()?.as_column()?;
                        if column.get_static_type()?.is_unsigned() {
                            return None;
                        }
                        Some(DirectStringAgg::FinalCount(
                            usize::try_from(column.index).ok()?,
                        ))
                    }
                    AggKind::Sum => {
                        let column = func.arg.as_ref()?.as_column()?;
                        let field_type = column.get_static_type()?;
                        (field_type.code() == FieldTypeCode::NewDecimal).then_some(
                            DirectStringAgg::Sum(usize::try_from(column.index).ok()?),
                        )
                    }
                    AggKind::FirstRow => {
                        let column = func.arg.as_ref()?.as_column()?;
                        Some(DirectStringAgg::FirstRow {
                            column: usize::try_from(column.index).ok()?,
                            field_type: column.get_static_type()?.clone(),
                        })
                    }
                    _ => None,
                }
            })
            .collect()
    }

    /// Returns the narrow global `COUNT(DISTINCT string_column)` shape. The
    /// generic aggregate path evaluates a `Datum` and re-encodes it for every
    /// row; a binary string column can use its chunk bytes directly instead.
    fn direct_global_count_distinct_column(
        &self,
    ) -> Option<(usize, tidb_datatype::Collation)> {
        if !self.group_by.is_empty() || self.agg_funcs.len() != 1 {
            return None;
        }
        let function = &self.agg_funcs[0];
        if !matches!(function.kind, AggKind::Count)
            || !function.distinct
            || !function.extra_args.is_empty()
            || !function.order_by.is_empty()
        {
            return None;
        }
        let column = function.arg.as_ref()?.as_column()?;
        let offset = usize::try_from(column.index).ok()?;
        let field_type = column.get_static_type()?;
        if !matches!(
            field_type.code(),
            FieldTypeCode::String
                | FieldTypeCode::VarString
                | FieldTypeCode::Varchar
                | FieldTypeCode::TinyBlob
                | FieldTypeCode::MediumBlob
                | FieldTypeCode::LongBlob
                | FieldTypeCode::Blob
        ) {
            return None;
        }
        let collation = expr_collation(function.arg.as_ref()?);
        matches!(
            collation,
            tidb_datatype::Collation::Binary
                | tidb_datatype::Collation::Utf8Mb4Bin
                | tidb_datatype::Collation::Utf8Mb40900Bin
        )
        .then_some((offset, collation))
    }

    /// Drains a global binary-string DISTINCT COUNT without constructing one
    /// `Datum`/hash encoding per input row. NULLs are excluded by COUNT;
    /// `utf8mb4_bin` keeps the same PAD SPACE normalization as the grouped
    /// direct-string path above.
    fn execute_direct_global_count_distinct(
        &mut self,
        offset: usize,
        collation: tidb_datatype::Collation,
    ) -> Result<(), ExecError> {
        self.ordered.clear();
        self.ordered
            .extend(self.agg_funcs.iter().map(AggState::new));
        self.group_count = 1;
        loop {
            self.child_chunk.reset();
            self.child.next(&mut self.child_chunk)?;
            let rows = self.child_chunk.num_rows();
            if rows == 0 {
                break;
            }
            self.child_returned_empty = false;
            let column = self.child_chunk.column(offset);
            for row_index in 0..rows {
                let physical_row = self
                    .child_chunk
                    .sel()
                    .map_or(row_index, |selection| selection[row_index]);
                if column.is_null(physical_row) {
                    continue;
                }
                let raw = column.get_bytes(physical_row);
                let raw = raw.as_ref();
                let key = if matches!(collation, tidb_datatype::Collation::Utf8Mb4Bin) {
                    let len = raw
                        .iter()
                        .rposition(|byte| *byte != b' ')
                        .map_or(0, |index| index + 1);
                    raw[..len].to_vec()
                } else {
                    raw.to_vec()
                };
                let key_len = i64::try_from(key.len()).unwrap_or(i64::MAX);
                let (map_delta, inserted) = self.ordered[0]
                    .seen
                    .as_mut()
                    .expect("direct DISTINCT COUNT always owns a seen set")
                    .insert(key);
                if inserted {
                    if let Partial::Count(count) = &mut self.ordered[0].partial {
                        *count += 1;
                    }
                    self.tracker.consume(key_len + map_delta);
                }
            }
        }
        self.executed = true;
        self.prepared = true;
        Ok(())
    }

    /// Folds a direct binary string grouping key without constructing a
    /// Datum/collation key for every row. The leading tag keeps NULL distinct
    /// from the empty string; UTF8MB4 binary retains MySQL PAD SPACE.
    fn fold_direct_string_group(
        &mut self,
        chunk: &Chunk,
        rows: usize,
        offset: usize,
        collation: tidb_datatype::Collation,
    ) -> Result<Vec<usize>, ExecError> {
        self.reserve_direct_string_groups();
        let mut deferred = Vec::new();
        for row_index in 0..rows {
            let row = chunk.get_row(row_index);
            self.group_key_buffer.clear();
            if row.is_null(offset) {
                self.group_key_buffer.push(0);
            } else {
                let bytes = row.get_bytes(offset);
                let bytes = bytes.as_ref();
                let bytes = if matches!(collation, tidb_datatype::Collation::Utf8Mb4Bin) {
                    let len = bytes
                        .iter()
                        .rposition(|byte| *byte != b' ')
                        .map_or(0, |index| index + 1);
                    &bytes[..len]
                } else {
                    bytes
                };
                if bytes.len() > DIRECT_STRING_MAX_KEY_BYTES {
                    return Err(ExecError::unsupported(
                        "direct string aggregate key exceeds its declared width",
                    ));
                }
                self.group_key_buffer.push(1);
                self.group_key_buffer.extend_from_slice(bytes);
            }
            let fingerprint = fast_bytes_fingerprint(&self.group_key_buffer);
            let idx = match self
                .direct_string_group_index(fingerprint, &self.group_key_buffer)
            {
                Some(idx) => idx,
                None => {
                    if self.in_spill_mode.load(SeqCst) && self.group_count != 0 {
                        deferred.push(row_index);
                        continue;
                    }
                    let (idx, bytes) = self.open_direct_string_group(fingerprint);
                    self.tracker.consume(bytes);
                    idx
                }
            };
            let delta = self.update_group(idx, row)?;
            if delta != 0 {
                self.tracker.consume(delta);
            }
        }
        Ok(deferred)
    }

    /// Columnar counterpart of [`Self::fold_direct_string_group`].  Go's
    /// vectorized aggregate updates read COUNT null bits and DECIMAL
    /// coefficients directly from the chunk columns; doing the same avoids
    /// constructing a `Row`, evaluating two column expressions, and decoding
    /// a decimal for every Web3Bench transaction.
    fn fold_direct_string_group_columnar(
        &mut self,
        chunk: &Chunk,
        rows: usize,
        offset: usize,
        collation: tidb_datatype::Collation,
        specs: &[DirectStringAgg],
    ) -> Result<Vec<usize>, ExecError> {
        self.reserve_direct_string_groups();
        let values = specs
            .iter()
            .map(|spec| match spec {
                DirectStringAgg::Count(Some(index))
                | DirectStringAgg::FinalCount(index)
                | DirectStringAgg::Sum(index)
                | DirectStringAgg::FirstRow { column: index, .. } => {
                    Some(chunk.column(*index))
                }
                DirectStringAgg::Count(None) => None,
            })
            .collect::<Vec<_>>();
        let key_column = chunk.column(offset);
        // The normal statement budget is far above the spill threshold. In
        // that case defer the tracker walk until the chunk boundary instead
        // of traversing the global memory-arbitrator tree for every group.
        // Small quotas retain per-group accounting so spill/cancellation
        // still observes Go's mid-chunk behavior.
        let batch_tracking = self.memory.quota() == 0 || self.memory.quota() >= 256 * 1024 * 1024;
        let mut pending_tracker_bytes = 0;
        let mut deferred = Vec::new();
        for row_index in 0..rows {
            let physical_row = chunk.sel().map_or(row_index, |selection| selection[row_index]);
            self.group_key_buffer.clear();
            if key_column.is_null(physical_row) {
                self.group_key_buffer.push(0);
            } else {
                let bytes = key_column.get_bytes(physical_row);
                let bytes = bytes.as_ref();
                let bytes = if matches!(collation, tidb_datatype::Collation::Utf8Mb4Bin) {
                    let len = bytes
                        .iter()
                        .rposition(|byte| *byte != b' ')
                        .map_or(0, |index| index + 1);
                    &bytes[..len]
                } else {
                    bytes
                };
                if bytes.len() > DIRECT_STRING_MAX_KEY_BYTES {
                    return Err(ExecError::unsupported(
                        "direct string aggregate key exceeds its declared width",
                    ));
                }
                self.group_key_buffer.push(1);
                self.group_key_buffer.extend_from_slice(bytes);
            }
            let fingerprint = fast_bytes_fingerprint(&self.group_key_buffer);
            let idx = match self
                .direct_string_group_index(fingerprint, &self.group_key_buffer)
            {
                Some(idx) => idx,
                None => {
                    if self.in_spill_mode.load(SeqCst) && self.group_count != 0 {
                        deferred.push(row_index);
                        continue;
                    }
                    let (idx, bytes) = self.open_direct_string_group(fingerprint);
                    if batch_tracking {
                        pending_tracker_bytes += bytes;
                    } else {
                        self.tracker.consume(bytes);
                    }
                    idx
                }
            };
            let group_offset = idx * self.agg_funcs.len();
            let mut delta = 0;
            for (function_index, spec) in specs.iter().enumerate() {
                let state = &mut self.ordered[group_offset + function_index];
                match spec {
                    DirectStringAgg::Count(column) => {
                        let non_null = column.is_none_or(|_column| {
                            !values[function_index]
                                .as_ref()
                                .expect("COUNT column installed")
                                .is_null(physical_row)
                        });
                        if !state.update_count_fast(non_null) {
                            return Err(ExecError::unsupported(
                                "direct string COUNT state is not a plain count",
                            ));
                        }
                    }
                    DirectStringAgg::FinalCount(_column) => {
                        let source = values[function_index]
                            .as_ref()
                            .expect("final COUNT column installed");
                        if source.is_null(physical_row) {
                            continue;
                        }
                        let value = source.get_int64(physical_row);
                        if !state.update_final_count_fast(value) {
                            return Err(ExecError::unsupported(
                                "direct string final COUNT state is not a final count",
                            ));
                        }
                    }
                    DirectStringAgg::Sum(_column) => {
                        let source = values[function_index]
                            .as_ref()
                            .expect("SUM column installed");
                        if source.is_null(physical_row) {
                            continue;
                        }
                        if let Some((coefficient, scale)) =
                            source.get_my_decimal_i128_scaled(physical_row)
                        {
                            if state.update_sum_decimal_fast(coefficient, scale) {
                                continue;
                            }
                        }
                        // Overflow or a mixed representation is rare but must
                        // retain Go's exact fallback semantics.
                        let decimal = source.get_my_decimal(physical_row);
                        let value = Datum::Decimal(Decimal::from_my_decimal(&decimal));
                        delta += state.update(Some(value), &[], Vec::new(), None)?;
                    }
                    DirectStringAgg::FirstRow { column, field_type } => {
                        if !state.has_first_row() {
                            let row = chunk.get_row(row_index);
                            let value = row.get_datum(*column, field_type);
                            delta += state.update(Some(value), &[], Vec::new(), None)?;
                        }
                    }
                }
            }
            if delta != 0 {
                if batch_tracking {
                    pending_tracker_bytes += delta;
                } else {
                    self.tracker.consume(delta);
                }
            }
        }
        if pending_tracker_bytes != 0 {
            self.tracker.consume(pending_tracker_bytes);
        }
        Ok(deferred)
    }

    fn parallel_int_agg_chunk(
        chunks: Vec<(Chunk, usize)>,
        group_column: usize,
        group_unsigned: bool,
        specs: &[ParallelIntAggSpec],
    ) -> Result<ParallelIntMap<ParallelIntGroup>, ExecError> {
        let mut groups = ParallelIntMap::default();
        let needs_counts = specs.iter().any(|spec| {
            matches!(
                spec,
                ParallelIntAggSpec::Count(_)
                    | ParallelIntAggSpec::FinalCount { .. }
                    | ParallelIntAggSpec::FinalAvgDecimal { .. }
            )
        });
        let needs_decimal_sums = specs
            .iter()
            .any(|spec| matches!(spec, ParallelIntAggSpec::FinalAvgDecimal { .. }));
        let needs_first_rows = specs
            .iter()
            .any(|spec| matches!(spec, ParallelIntAggSpec::FirstRow { .. }));
        for (chunk, sequence) in chunks {
            // A Go chunk worker keeps typed column pointers for the whole
            // fetch window. Looking the column up through `Row::get_*` for
            // every cell needlessly repeats the column-slot read path (and,
            // for promoted aliases, its read lock). Hold the source columns
            // once per chunk and only vary the row offset in the hot loop.
            let group_values = chunk.column(group_column);
            let spec_values = specs
                .iter()
                .map(|spec| {
                    let column = match spec {
                        ParallelIntAggSpec::Count(Some(column))
                        | ParallelIntAggSpec::FinalCount { column, .. }
                        | ParallelIntAggSpec::FinalAvgDecimal {
                            count_column: column,
                            ..
                        }
                        | ParallelIntAggSpec::FirstRow { column, .. } => Some(*column),
                        ParallelIntAggSpec::Count(None) => None,
                    };
                    column.map(|column| chunk.column(column))
                })
                .collect::<Vec<_>>();
            let sum_values = specs
                .iter()
                .map(|spec| match spec {
                    ParallelIntAggSpec::FinalAvgDecimal { sum_column, .. } => {
                        Some(chunk.column(*sum_column))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            for row_index in 0..chunk.num_rows() {
                // Typed columns are indexed physically, while `num_rows` and
                // `get_row` are logical and map through `sel`. Go's chunk
                // workers use the selected physical row for every vectorized
                // argument, so preserve that mapping on this direct-column
                // fast path as well.
                let physical_row = chunk.sel().map_or(row_index, |sel| sel[row_index]);
                let key = if group_values.is_null(physical_row) {
                    ParallelIntKey::Null
                } else if group_unsigned {
                    ParallelIntKey::Unsigned(group_values.get_uint64(physical_row))
                } else {
                    ParallelIntKey::Signed(group_values.get_int64(physical_row))
                };
                let entry = groups.entry(key).or_insert_with(|| ParallelIntGroup {
                    first_seq: sequence + row_index,
                    // Most plans use only one aggregate state family. Avoid
                    // allocating the other two vectors for every local copy
                    // of every group (q13 opens 150k customer groups in each
                    // worker map and needs COUNT state only).
                    counts: if needs_counts {
                        smallvec::smallvec![0; specs.len()]
                    } else {
                        smallvec::SmallVec::new()
                    },
                    decimal_sums: if needs_decimal_sums {
                        smallvec::smallvec![None; specs.len()]
                    } else {
                        smallvec::SmallVec::new()
                    },
                    first_rows: if needs_first_rows {
                        smallvec::smallvec![None; specs.len()]
                    } else {
                        smallvec::SmallVec::new()
                    },
                });
                for (index, spec) in specs.iter().enumerate() {
                    match spec {
                        ParallelIntAggSpec::Count(column) => {
                            if column.is_none_or(|_| {
                                !spec_values[index]
                                    .as_ref()
                                    .expect("COUNT column installed")
                                    .is_null(physical_row)
                            }) {
                                entry.counts[index] = entry.counts[index].wrapping_add(1);
                            }
                        }
                        ParallelIntAggSpec::FinalCount {
                            column: _,
                            unsigned,
                        } => {
                            let values = spec_values[index]
                                .as_ref()
                                .expect("FINAL_COUNT column installed");
                            if values.is_null(physical_row) {
                                continue;
                            }
                            let value = if *unsigned {
                                i64::try_from(values.get_uint64(physical_row)).unwrap_or(i64::MAX)
                            } else {
                                values.get_int64(physical_row)
                            };
                            entry.counts[index] = entry.counts[index].wrapping_add(value);
                        }
                        ParallelIntAggSpec::FinalAvgDecimal {
                            count_column: _,
                            count_unsigned,
                            sum_column: _,
                        } => {
                            let count_values = spec_values[index]
                                .as_ref()
                                .expect("AVG count column installed");
                            let sum_values = sum_values[index]
                                .as_ref()
                                .expect("AVG sum column installed");
                            if count_values.is_null(physical_row)
                                || sum_values.is_null(physical_row)
                            {
                                continue;
                            }
                            let count = if *count_unsigned {
                                i64::try_from(count_values.get_uint64(physical_row)).map_err(
                                    |_| {
                                        ExecError::unsupported(
                                            "partial AVG count exceeds i64 in parallel aggregate",
                                        )
                                    },
                                )?
                            } else {
                                count_values.get_int64(physical_row)
                            };
                            if count < 0 {
                                return Err(ExecError::unsupported(
                                    "partial AVG count is negative in parallel aggregate",
                                ));
                            }
                            let sum = sum_values.get_my_decimal(physical_row);
                            entry.counts[index] = entry.counts[index].wrapping_add(count);
                            entry.decimal_sums[index] =
                                Some(match entry.decimal_sums[index].take() {
                                    Some(current) => current.add_my_decimal(&sum),
                                    None => ParallelDecimalSum::from_my_decimal(&sum),
                                });
                        }
                        ParallelIntAggSpec::FirstRow { column, field_type } => {
                            if entry.first_rows[index].is_none() {
                                let row = chunk.get_row(row_index);
                                entry.first_rows[index] = Some(row.get_datum(*column, field_type));
                            }
                        }
                    }
                }
            }
        }
        Ok(groups)
    }

    fn parallel_int_count_chunk(
        chunks: Vec<(Chunk, usize)>,
        group_column: usize,
        group_unsigned: bool,
        spec: &ParallelIntAggSpec,
    ) -> ParallelIntMap<ParallelIntCountGroup> {
        let mut groups = ParallelIntMap::default();
        for (chunk, sequence) in chunks {
            for row_index in 0..chunk.num_rows() {
                let row = chunk.get_row(row_index);
                let key = ParallelIntKey::from_row(row, group_column, group_unsigned);
                let entry = groups.entry(key).or_insert(ParallelIntCountGroup {
                    first_seq: sequence + row_index,
                    count: 0,
                });
                let increment = match spec {
                    ParallelIntAggSpec::Count(None) => 1,
                    ParallelIntAggSpec::Count(Some(column)) => i64::from(!row.is_null(*column)),
                    ParallelIntAggSpec::FinalCount { column, unsigned } => {
                        if row.is_null(*column) {
                            0
                        } else if *unsigned {
                            i64::try_from(row.get_uint64(*column)).unwrap_or(i64::MAX)
                        } else {
                            row.get_int64(*column)
                        }
                    }
                    _ => unreachable!("the scalar count path accepts only COUNT states"),
                };
                entry.count = entry.count.wrapping_add(increment);
            }
        }
        groups
    }

    fn finish_parallel_int_agg(
        &mut self,
        groups: ParallelIntMap<ParallelIntGroup>,
        specs: &[ParallelIntAggSpec],
    ) {
        let mut groups: Vec<_> = groups.into_iter().collect();
        groups.sort_by_key(|(_, group)| group.first_seq);
        let output_types = self.meta.ret_field_types().to_vec();
        let div_precision_increment = self.ctx.div_precision_increment();
        self.parallel_output.clear();
        self.parallel_output
            .reserve(groups.len().saturating_mul(specs.len()));
        for (key, mut group) in groups {
            for (index, spec) in specs.iter().enumerate() {
                self.parallel_output.push(match spec {
                    ParallelIntAggSpec::Count(_) | ParallelIntAggSpec::FinalCount { .. } => {
                        Datum::Int(group.counts[index])
                    }
                    ParallelIntAggSpec::FinalAvgDecimal { .. } => {
                        let value = if group.counts[index] == 0 {
                            Datum::Null
                        } else if let Some(sum) = group.decimal_sums[index].take() {
                            let divisor = Decimal::from_int(group.counts[index]);
                            let target_scale = sum.scale() + div_precision_increment;
                            sum.into_decimal()
                                .true_div(&divisor, target_scale)
                                .map(Datum::Decimal)
                                .unwrap_or(Datum::Null)
                        } else {
                            Datum::Null
                        };
                        round_avg_result(&AggKind::Avg, &output_types[index], value)
                    }
                    ParallelIntAggSpec::FirstRow { .. } => group.first_rows[index]
                        .clone()
                        .unwrap_or_else(|| key.as_datum()),
                });
            }
        }
        self.parallel_output_width = specs.len();
        self.parallel_output_cursor = 0;
        self.parallel_output_active = true;
    }

    fn finish_parallel_int_count_agg(&mut self, groups: ParallelIntMap<ParallelIntCountGroup>) {
        let mut groups: Vec<_> = groups.into_iter().collect();
        groups.sort_by_key(|(_, group)| group.first_seq);
        self.parallel_output.clear();
        self.parallel_output.reserve(groups.len());
        self.parallel_output
            .extend(groups.into_iter().map(|(_, group)| Datum::Int(group.count)));
        self.parallel_output_width = 1;
        self.parallel_output_cursor = 0;
        self.parallel_output_active = true;
    }

    /// Go's inner loop of `execute`: fold `chunk`'s rows into their groups,
    /// returning the bytes the group table grew by and the rows this round
    /// refused to open a group for (Go's `sel`).
    fn fold_chunk(&mut self, chunk: &Chunk, rows: usize) -> Result<Vec<usize>, ExecError> {
        if let Some((offset, collation)) = self.direct_string_group_column() {
            if let Some(specs) = self.direct_string_aggregate_specs() {
                return self.fold_direct_string_group_columnar(
                    chunk, rows, offset, collation, &specs,
                );
            }
            return self.fold_direct_string_group(chunk, rows, offset, collation);
        }
        let mut sel: Vec<usize> = Vec::new();
        for r in 0..rows {
            let row = chunk.get_row(r);
            self.group_key_buffer.clear();
            if let Some(columns) = &self.integer_group_columns {
                for &(index, unsigned) in columns {
                    append_integer_group_key_part(row, index, unsigned, &mut self.group_key_buffer);
                    self.group_key_buffer.push(0xff);
                }
            } else {
                for (expr, collation) in self.group_by.iter().zip(&self.group_collations) {
                    let datum = expr.eval(&self.ctx, row)?;
                    append_group_key_part(collation, &datum, &mut self.group_key_buffer);
                    self.group_key_buffer.push(0xff); // separator, as key parts are length-coded
                }
            }
            let idx = match self.groups.get(&self.group_key_buffer) {
                Some(&idx) => idx,
                None => {
                    // Go: a round in spill mode opens no new group -- but it
                    // must open the FIRST one, or a round could make no
                    // progress at all and the aggregation would not terminate.
                    if self.in_spill_mode.load(SeqCst) && self.group_count != 0 {
                        sel.push(r);
                        continue;
                    }
                    let idx = self.group_count;
                    let capacity = self.group_key_buffer.capacity();
                    let key =
                        std::mem::replace(&mut self.group_key_buffer, Vec::with_capacity(capacity));
                    let key_len = key.len();
                    self.groups.insert(key, idx);
                    self.ordered
                        .extend(self.agg_funcs.iter().map(AggState::new));
                    self.group_count += 1;
                    // Consumed HERE, not at the end of the chunk: Go consumes
                    // inside `getPartialResults`, per group, so the spill
                    // action fires PART WAY THROUGH a chunk and the rest of
                    // that chunk is already deferred. Accumulating a whole
                    // chunk's groups into one `Consume` would let a single
                    // call jump clean over the quota between the soft limit
                    // that spills and the hard limit that cancels.
                    self.tracker
                        .consume(new_group_bytes(key_len, self.agg_funcs.len()));
                    idx
                }
            };
            let delta = self.update_group(idx, row)?;
            self.tracker.consume(delta);
        }
        Ok(sel)
    }

    /// Folds one row into group `idx`, reporting the bytes the group grew by.
    fn update_group(
        &mut self,
        idx: usize,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<i64, ExecError> {
        let mut delta = 0;
        let group_offset = idx * self.agg_funcs.len();
        for c in 0..self.agg_funcs.len() {
            let f = &self.agg_funcs[c];
            let state = &mut self.ordered[group_offset + c];
            // Go's typed COUNT implementations only inspect the source
            // column's NULL bitmap. Keep COUNT(*) and the common
            // COUNT(column) path equally direct.
            if matches!(f.kind, AggKind::Count)
                && !f.distinct
                && f.extra_args.is_empty()
                && f.order_by.is_empty()
            {
                let input_is_non_null = match f.arg.as_ref() {
                    None => Some(true),
                    Some(expr) => expr.as_column().and_then(|column| {
                        usize::try_from(column.index)
                            .ok()
                            .map(|index| !row.is_null(index))
                    }),
                };
                if input_is_non_null.is_some_and(|present| state.update_count_fast(present)) {
                    continue;
                }
            }
            // Go's FIRST_ROW returns before evaluating its argument once the
            // group owns a value. This matters for the second q13 aggregation,
            // where nearly every input row revisits an existing group.
            if matches!(f.kind, AggKind::FirstRow) && state.has_first_row() {
                continue;
            }
            if matches!(f.kind, AggKind::Min | AggKind::Max)
                && !f.distinct
                && f.extra_args.is_empty()
                && f.order_by.is_empty()
            {
                let decimal_column =
                    f.arg
                        .as_ref()
                        .and_then(Expression::as_column)
                        .filter(|column| {
                            column.get_static_type().is_some_and(|ty| {
                                ty.code() == tidb_datatype::FieldTypeCode::NewDecimal
                            })
                        });
                if let Some(column) = decimal_column {
                    if row.is_null(column.index as usize) {
                        continue;
                    }
                    if let Some((coefficient, scale)) =
                        row.get_my_decimal(column.index as usize).to_i128_scaled()
                    {
                        if state.update_decimal_max_min_fast(coefficient, scale) {
                            continue;
                        }
                    }
                }
            }
            // A fixed-scale DECIMAL AVG can accumulate the raw MyDecimal
            // coefficient directly. The normal expression path remains the
            // fallback for computed, mixed-scale, or non-decimal arguments.
            if matches!(f.kind, AggKind::Avg) {
                let column = f.arg.as_ref().and_then(Expression::as_column);
                let decimal_column = column.filter(|column| {
                    column
                        .get_static_type()
                        .is_some_and(|ty| ty.code() == tidb_datatype::FieldTypeCode::NewDecimal)
                });
                if let Some(column) = decimal_column {
                    let count = if f.extra_args.is_empty() {
                        Some(1)
                    } else if f.extra_args.len() == 1 {
                        match f.extra_args[0].eval(&self.ctx, row)? {
                            Datum::Int(value) if value >= 0 => Some(value),
                            Datum::UInt(value) => i64::try_from(value).ok(),
                            Datum::Null => Some(0),
                            _ => None,
                        }
                    } else {
                        None
                    };
                    if let Some(count) = count {
                        if count == 0 {
                            continue;
                        }
                        if row.is_null(column.index as usize) {
                            continue;
                        }
                        if let Some((coefficient, scale)) =
                            row.get_my_decimal(column.index as usize).to_i128_scaled()
                        {
                            if state.update_avg_decimal_fast(coefficient, scale, count) {
                                continue;
                            }
                            state.partial.materialize_avg_fast();
                        }
                    }
                }
            }
            // A fixed-scale DECIMAL SUM folds the raw cell coefficient the
            // same way MIN/MAX and AVG do: no Datum, no `Decimal` build per
            // row. A differing scale or an overflow falls back to the
            // complete path, which materializes and replays the row.
            if matches!(f.kind, AggKind::Sum)
                && !f.distinct
                && f.extra_args.is_empty()
                && f.order_by.is_empty()
            {
                let column = f.arg.as_ref().and_then(Expression::as_column);
                let decimal_column = column.filter(|column| {
                    column
                        .get_static_type()
                        .is_some_and(|ty| ty.code() == tidb_datatype::FieldTypeCode::NewDecimal)
                });
                if let Some(column) = decimal_column {
                    if row.is_null(column.index as usize) {
                        continue;
                    }
                    if let Some((coefficient, scale)) =
                        row.get_my_decimal(column.index as usize).to_i128_scaled()
                    {
                        if state.update_sum_decimal_fast(coefficient, scale) {
                            continue;
                        }
                        state.partial.materialize_sum_fast();
                    }
                }
            }
            // Final AVG rows carry the partial count in `arg` and the
            // partial sum in one extra column. Keep that one extra datum on
            // the stack: allocating a fresh Vec for every partial row is a
            // measurable cost in TPC-H q17's 1.17M-row inner aggregation.
            if matches!(f.kind, AggKind::Avg) && f.extra_args.len() == 1 {
                let value = f
                    .arg
                    .as_ref()
                    .map(|expr| expr.eval(&self.ctx, row))
                    .transpose()?;
                let extra = [f.extra_args[0].eval(&self.ctx, row)?];
                let input = AggInput {
                    value,
                    distinct_key: None,
                    decimal_coefficient: None,
                };
                let sort_key = Vec::new();
                delta += state.update(input.value, &extra, sort_key, input.distinct_key)?;
                continue;
            }
            let mut extra_values: Vec<Datum> = Vec::new();
            let input = eval_agg_input(f, &self.ctx, row, &mut extra_values)?;
            // GROUP_CONCAT's own ORDER BY is evaluated over the same source
            // row that produced the value, so the key travels with it.
            let mut sort_key = Vec::with_capacity(f.order_by.len());
            for (expr, _) in &f.order_by {
                sort_key.push(expr.eval(&self.ctx, row)?);
            }
            if let Some((coefficient, scale)) = input.decimal_coefficient {
                if state.partial_update_with_coefficient(coefficient, scale) {
                    continue;
                }
            }
            delta += state.update(input.value, &extra_values, sort_key, input.distinct_key)?;
        }
        Ok(delta)
    }

    /// Appends group `idx`'s final values to `req`.
    fn emit_group(&mut self, idx: usize, req: &mut Chunk) -> Result<(), ExecError> {
        let group_offset = idx * self.agg_funcs.len();
        for c in 0..self.agg_funcs.len() {
            let value = finish_agg_value(
                &mut self.ordered[group_offset + c],
                &self.agg_funcs[c],
                &self.meta.ret_field_types()[c],
                &self.ctx,
                &mut self.truncated[c],
            )?;
            req.append_datum(c, &value);
        }
        Ok(())
    }
}

impl<C: HashAggContext> Executor for HashAggExec<C> {
    /// Go `aggExecutorTreeInputEmpty`'s one true answer: the walk exists to
    /// find an aggregation and read its `IsChildReturnEmpty`.
    fn agg_tree_input_empty(&self) -> bool {
        self.child_returned_empty
    }

    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.child_chunk.reset();
        self.tmp_chk_for_spill.reset();
        self.child_returned_empty = true;
        self.groups.clear();
        self.group_key_buffer.clear();
        self.direct_string_buckets.clear();
        self.direct_string_collisions.clear();
        self.direct_string_keys.clear();
        self.ordered.clear();
        self.group_count = 0;
        self.cursor = 0;
        self.prepared = false;
        self.executed = false;
        for flag in &mut self.truncated {
            *flag = false;
        }
        if let Some(in_disk) = &mut self.data_in_disk {
            in_disk.close();
        }
        self.data_in_disk = None;
        self.num_of_spilled_chks = 0;
        self.offset_of_spilled_chks = 0;
        self.is_child_drained = false;
        self.parallel_output.clear();
        self.parallel_output_width = 0;
        self.parallel_output_cursor = 0;
        self.parallel_output_active = false;
        self.parallel_agg_windows = 0;
        self.pipeline_mode = false;
        #[cfg(test)]
        {
            self.pipeline_concurrency_override = None;
        }
        self.pipeline_stats = None;
        self.in_spill_mode.store(false, SeqCst);
        // Go `HashAggExec.Open` -> `e.memTracker.Reset()`: an aggregation
        // re-opened by an Apply's inner side must not keep charging for the
        // groups it has just dropped.
        self.tracker.replace_bytes_used(0);
        let pipeline_counts = self.pipeline_eligibility();
        if let Some((partial_concurrency, final_concurrency)) = pipeline_counts {
            // The bounded integer fast path keeps its historical priority
            // (and its Open-time spill registration) over the pipeline.
            if <C as HashAggContext>::PARALLEL_WORKERS_MAY_EVAL
                && self.parallel_int_agg_specs().is_none()
            {
                // Go `initForParallelExec`: worker counts resolved from the
                // session variables; the pipeline takes this aggregation over.
                // The parallel spill helper is not ported yet (see
                // `hash_agg::parallel`'s module docs): NO soft-limit action is
                // registered in this mode, so a quota overrun surfaces as the
                // 8175 cancellation instead of a spill.
                self.pipeline_stats = Some(Arc::new(parallel::PipelineStats::new(
                    partial_concurrency,
                    final_concurrency,
                )));
                self.pipeline_mode = true;
                self.pipeline_partial_concurrency = partial_concurrency;
                self.pipeline_final_concurrency = final_concurrency;
                return Ok(());
            }
        }
        // Go `initForUnparallelExec`: a FRESH action per Open (so `spillTimes`
        // starts over), registered on the SOFT-limit slot, and only when
        // `tidb_enable_tmp_storage_on_oom` is on -- with it off an overrun goes
        // straight to the 8175 cancellation on the hard-limit slot.
        if let Some(action) = self.spill_action.take() {
            action.set_finished();
        }
        if self.memory.tmp_storage_on_oom() {
            let (action, in_spill_mode) = AggSpillDiskAction::new(&self.tracker);
            self.in_spill_mode = in_spill_mode;
            self.memory
                .session_tracker()
                .fallback_old_and_set_new_action_for_soft_limit(Arc::clone(&action) as ArcAction);
            self.spill_action = Some(action);
        }
        Ok(())
    }

    /// Go `unparallelExec`: emit the current round's groups until the chunk is
    /// full, and when they run out, run the next round.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        loop {
            if self.parallel_output_active {
                while self.parallel_output_cursor * self.parallel_output_width
                    < self.parallel_output.len()
                {
                    let start = self.parallel_output_cursor * self.parallel_output_width;
                    let row = &self.parallel_output[start..start + self.parallel_output_width];
                    for (column, value) in row.iter().enumerate() {
                        req.append_datum(column, value);
                    }
                    self.parallel_output_cursor += 1;
                    if req.is_full() {
                        return Ok(());
                    }
                }
                self.parallel_output_active = false;
                continue;
            }
            if self.prepared {
                while self.cursor < self.group_count {
                    if self.agg_funcs.is_empty() {
                        // Go: `chk.SetNumVirtualRows(chk.NumRows() + 1)` -- a
                        // group with no aggregate columns is still a row.
                        req.set_num_virtual_rows(req.num_rows() + 1);
                    }
                    self.emit_group(self.cursor, req)?;
                    self.cursor += 1;
                    if req.is_full() {
                        return Ok(());
                    }
                }
                self.reset_spill_mode();
            }
            if self.executed {
                return Ok(());
            }
            // A global COUNT(*)/COUNT(1) can consume an exact child
            // cardinality directly. This is the same shortcut as Go's
            // aggregate executor and lets a derived join answer COUNT
            // without materializing its joined rows.
            if self.group_by.is_empty()
                && self.agg_funcs.len() == 1
                && matches!(self.agg_funcs[0].kind, AggKind::Count)
                && !self.agg_funcs[0].distinct
                && self.agg_funcs[0].extra_args.is_empty()
                && self.agg_funcs[0].order_by.is_empty()
            {
                let counts_all_rows = match self.agg_funcs[0].arg.as_ref() {
                    None => true,
                    Some(Expression::Constant(constant)) => {
                        matches!(constant.value, Datum::Int(1) | Datum::UInt(1))
                    }
                    Some(_) => false,
                };
                if counts_all_rows {
                    if let Some(count) = self.child.row_count()? {
                        let count = i64::try_from(count)
                            .map_err(|_| ExecError::unsupported("COUNT exceeds signed BIGINT"))?;
                        let mut state = AggState::new(&self.agg_funcs[0]);
                        state.partial = Partial::Count(count);
                        self.ordered.push(state);
                        self.group_count = 1;
                        self.child_returned_empty = count == 0;
                        self.executed = true;
                        self.prepared = true;
                        continue;
                    }
                }
            }
            if let Some((offset, collation)) = self.direct_global_count_distinct_column() {
                self.execute_direct_global_count_distinct(offset, collation)?;
                continue;
            }
            self.execute()?;
            // No group-by and no data: one empty group, so a global COUNT is 0.
            // (The pipeline synthesizes its own defaults row; its output is
            // already staged in `parallel_output`.)
            if self.group_count == 0 && self.group_by.is_empty() && !self.pipeline_mode {
                self.ordered
                    .extend(self.agg_funcs.iter().map(AggState::new));
                self.group_count = 1;
            }
            self.prepared = true;
        }
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.groups.clear();
        self.direct_string_buckets.clear();
        self.direct_string_collisions.clear();
        self.direct_string_keys.clear();
        self.ordered.clear();
        self.group_count = 0;
        self.parallel_output.clear();
        self.parallel_output_width = 0;
        self.parallel_output_cursor = 0;
        self.parallel_output_active = false;
        self.pipeline_mode = false;
        self.pipeline_stats = None;
        if let Some(in_disk) = &mut self.data_in_disk {
            in_disk.close();
        }
        self.data_in_disk = None;
        if let Some(action) = self.spill_action.take() {
            action.set_finished();
        }
        self.tracker.replace_bytes_used(0);
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

/// The value one aggregate function takes from `row`, with any EXTRA argument
/// values it keeps separate. Go evaluates these inside
/// `UpdatePartialResult`, per function; this port evaluates them once at the
/// call site and hands the result to [`AggState::update`].
fn eval_agg_input<C: Columns>(
    f: &AggFunc,
    ctx: &C,
    row: tidb_chunk::row::Row<'_>,
    extra_values: &mut Vec<Datum>,
) -> Result<AggInput, ExecError> {
    // A non-DISTINCT SUM over a bare DECIMAL column reads the raw MyDecimal
    // words directly and returns the i128 coefficient -- skipping the Datum
    // materialization whose NewDecimal arm converts via an ASCII digit
    // round trip (`Decimal::from_my_decimal`). The fold consumes
    // `decimal_coefficient` without building a `Decimal`.
    if matches!(f.kind, AggKind::Sum)
        && !f.distinct
        && f.extra_args.is_empty()
        && f.order_by.is_empty()
    {
        if let Some(Expression::Column(column)) = &f.arg {
            let index = usize::try_from(column.index)
                .map_err(|_| ExecError::unsupported("a SUM argument column has no valid offset"))?;
            if !row.is_null(index) {
                let is_decimal = column
                    .get_static_type()
                    .is_some_and(|ty| ty.code() == tidb_datatype::FieldTypeCode::NewDecimal);
                if is_decimal {
                    if let Some((coefficient, scale)) = row
                        .chunk()
                        .expect("row has chunk")
                        .column(index)
                        .get_my_decimal(row.idx())
                        .to_i128_scaled()
                    {
                        return Ok(AggInput {
                            value: None,
                            distinct_key: None,
                            decimal_coefficient: Some((coefficient, scale)),
                        });
                    }
                }
            } else {
                return Ok(AggInput {
                    value: Some(Datum::Null),
                    distinct_key: None,
                    decimal_coefficient: None,
                });
            }
        }
    }
    let (value, distinct_key) = if f.extra_args.is_empty()
        && !matches!(
            f.kind,
            AggKind::ApproxCountDistinct | AggKind::GroupConcat { .. }
        ) {
        (
            match &f.arg {
                Some(expr) => Some(expr.eval(ctx, row)?),
                None => None,
            },
            None,
        )
    } else if matches!(f.kind, AggKind::JsonObjectAgg { .. } | AggKind::Avg) {
        // JSON_OBJECTAGG keeps key/value separate. Final-mode AVG uses the
        // same representation for Go's `(partial count, partial sum)` pair.
        for expr in &f.extra_args {
            extra_values.push(expr.eval(ctx, row)?);
        }
        (
            match &f.arg {
                Some(expr) => Some(expr.eval(ctx, row)?),
                None => None,
            },
            None,
        )
    } else if matches!(f.kind, AggKind::Count) {
        // `COUNT(a, b, ...)` / `COUNT(DISTINCT a, b, ...)`:
        // Go's `count4MultiArgs.UpdatePartialResult` skips the
        // row as soon as ANY argument is NULL (a row counts
        // only when EVERY argument is non-NULL). DISTINCT
        // dedupes over the whole tuple, so the per-argument
        // hash keys are length-prefixed and concatenated
        // (rather than joined with a fixed separator byte)
        // so no argument's encoding can bleed into the next
        // and manufacture a false collision or split.
        let mut tuple_key = Some(Vec::new());
        for expr in f.arg.iter().chain(f.extra_args.iter()) {
            let datum = expr.eval(ctx, row)?;
            if datum == Datum::Null {
                tuple_key = None;
                break;
            }
            if let Some(buf) = &mut tuple_key {
                let key = datum
                    .to_hash_key()
                    .map_err(|_| ExecError::unsupported("COUNT over this datum kind"))?;
                buf.extend_from_slice(&(key.len() as u64).to_be_bytes());
                buf.extend_from_slice(&key);
            }
        }
        (Some(tuple_key.map_or(Datum::Null, Datum::Bytes)), None)
    } else if matches!(f.kind, AggKind::ApproxCountDistinct) {
        // `APPROX_COUNT_DISTINCT(a, b, ...)`: Go's
        // `approxCountDistinctOriginal.UpdatePartialResult`
        // (`evalAndEncode`) skips the row as soon as ANY
        // argument is NULL, exactly like COUNT's tuple. But
        // unlike COUNT's hash key, each argument's raw
        // per-type encoding is appended straight onto the
        // buffer with NO length prefix between arguments --
        // reproduced as-is (including the theoretical
        // cross-argument collision this allows for
        // variable-width encodings) because the sketch's
        // hash input has to match Go byte for byte.
        let mut tuple_key = Some(Vec::new());
        for expr in f.arg.iter().chain(f.extra_args.iter()) {
            let datum = expr.eval(ctx, row)?;
            if datum == Datum::Null {
                tuple_key = None;
                break;
            }
            if let Some(buf) = &mut tuple_key {
                buf.extend_from_slice(&approx_count_distinct_encode(&datum)?);
            }
        }
        (Some(tuple_key.map_or(Datum::Null, Datum::Bytes)), None)
    } else {
        // Multi-argument GROUP_CONCAT: Go's `groupConcat`
        // update loop stringifies and concatenates every
        // argument per row, and skips the row entirely as
        // soon as ANY argument evaluates to NULL. DISTINCT
        // then dedupes over this concatenated value.
        let mut concatenated = Some(Vec::new());
        let mut distinct_key = f.distinct.then(Vec::new);
        for expr in f.arg.iter().chain(f.extra_args.iter()) {
            let datum = expr.eval(ctx, row)?;
            if datum == Datum::Null {
                concatenated = None;
                distinct_key = None;
                break;
            }
            if let Some(buf) = &mut concatenated {
                let rendered = group_concat_bytes(&datum)?;
                buf.extend_from_slice(&rendered);
                if let Some(key) = &mut distinct_key {
                    encode_bytes(key, &expr_collation(expr).immutable_key(&rendered));
                }
            }
        }
        (
            Some(concatenated.map_or(Datum::Null, Datum::Bytes)),
            distinct_key,
        )
    };
    Ok(AggInput {
        value,
        distinct_key,
        decimal_coefficient: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::NoColumns;
    use tidb_expr::column::Column;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    /// A test-only source emitting one prebuilt chunk then EOF.
    struct OneChunkSource {
        meta: ExecutorMeta,
        data: Option<Chunk>,
    }
    impl Executor for OneChunkSource {
        fn open(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            if let Some(data) = self.data.take() {
                for r in 0..data.num_rows() {
                    req.append_row(data.get_row(r));
                }
            }
            Ok(())
        }
        fn close(&mut self) -> Result<(), ExecError> {
            Ok(())
        }
        fn schema(&self) -> &Schema {
            self.meta.schema()
        }
        fn ret_field_types(&self) -> &[FieldType] {
            self.meta.ret_field_types()
        }
        fn init_cap(&self) -> usize {
            self.meta.init_cap()
        }
        fn max_chunk_size(&self) -> usize {
            self.meta.max_chunk_size()
        }
        fn new_chunk(&self) -> Chunk {
            self.meta.new_chunk()
        }
    }

    fn col(index: i64) -> Expression {
        let mut c = Column::new(index + 1, long());
        c.index = index;
        Expression::Column(c)
    }

    fn typed_col(index: i64, field_type: FieldType) -> Expression {
        let mut c = Column::new(index + 1, field_type);
        c.index = index;
        Expression::Column(c)
    }

    fn source(rows: &[(i64, Option<i64>)]) -> Box<dyn Executor> {
        // Two long columns: group key, value (None = NULL).
        let fields = vec![long(), long()];
        let mut data = Chunk::new_with_capacity(&fields, rows.len().max(1));
        for (g, v) in rows {
            data.append_int64(0, *g);
            match v {
                Some(v) => data.append_int64(1, *v),
                None => data.append_null(1),
            }
        }
        let mut cols = Vec::new();
        for i in 0..2 {
            let mut c = Column::new(i + 1, long());
            c.index = i;
            cols.push(c);
        }
        Box::new(OneChunkSource {
            meta: ExecutorMeta::new(Schema::new(cols), 0, rows.len().max(1), 1024),
            data: Some(data),
        })
    }

    fn out_meta(n: usize) -> ExecutorMeta {
        let mut cols = Vec::new();
        for i in 0..n {
            let mut c = Column::new((i + 1) as i64, long());
            c.index = i as i64;
            cols.push(c);
        }
        ExecutorMeta::new(Schema::new(cols), 1, 4, 1024)
    }

    /// An output schema whose column types are given, so a decimal result
    /// lands in a decimal cell.
    fn out_meta_typed(types: &[FieldType]) -> ExecutorMeta {
        let mut cols = Vec::new();
        for (i, t) in types.iter().enumerate() {
            let mut c = Column::new((i + 1) as i64, t.clone());
            c.index = i as i64;
            cols.push(c);
        }
        ExecutorMeta::new(Schema::new(cols), 1, 4, 1024)
    }

    #[test]
    fn integer_group_key_fast_path_matches_generic_datum_encoding() {
        let types = [long(), long().with_unsigned(true)];
        let mut chunk = Chunk::new_with_capacity(&types, 2);
        chunk.append_int64(0, -7);
        chunk.append_uint64(1, 7);
        chunk.append_null(0);
        chunk.append_null(1);

        for row_index in 0..chunk.num_rows() {
            let row = chunk.get_row(row_index);
            for (column, field_type) in types.iter().enumerate() {
                let mut fast = Vec::new();
                append_integer_group_key_part(row, column, field_type.is_unsigned(), &mut fast);

                let mut generic = Vec::new();
                append_group_key_part(
                    &Collation::DEFAULT,
                    &row.get_datum(column, field_type),
                    &mut generic,
                );
                assert_eq!(fast, generic, "row {row_index}, column {column}");
            }
        }
    }

    #[test]
    fn parallel_integer_aggregate_maps_logical_rows_through_selection() {
        let types = [long(), long()];
        let mut chunk = Chunk::new_with_capacity(&types, 3);
        for (group, value) in [(10, None), (20, Some(2)), (30, Some(3))] {
            chunk.append_int64(0, group);
            match value {
                Some(value) => chunk.append_int64(1, value),
                None => chunk.append_null(1),
            }
        }
        chunk.set_sel(Some(vec![2, 0]));

        let specs = [ParallelIntAggSpec::Count(Some(1))];
        let mut groups =
            HashAggExec::<NoColumns>::parallel_int_agg_chunk(vec![(chunk, 0)], 0, false, &specs)
                .unwrap();

        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups.remove(&ParallelIntKey::Signed(30)).unwrap().counts,
            smallvec::SmallVec::<[i64; 2]>::from_slice(&[1])
        );
        assert_eq!(
            groups.remove(&ParallelIntKey::Signed(10)).unwrap().counts,
            smallvec::SmallVec::<[i64; 2]>::from_slice(&[0])
        );
    }

    #[test]
    fn bounded_integer_aggregate_keeps_low_quota_on_accounted_serial_path() {
        let mut exec = HashAggExec::new(
            out_meta(1),
            vec![col(0)],
            vec![AggFunc::new(AggKind::Count, Some(col(1)))],
            source(&[(1, Some(1))]),
            NoColumns,
            StatementMemory::new(1 << 20, crate::mem_quota::OomAction::Cancel, 42)
                .with_tmp_storage_on_oom(false),
        );

        assert!(exec.parallel_int_agg_specs().is_none());
    }

    fn decimal() -> FieldType {
        FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal)
    }

    fn binary_varchar() -> FieldType {
        FieldType::new(FieldTypeCode::Varchar)
            .with_flen(42)
            .with_collation(Collation::Binary)
    }

    fn binary_string_source(values: &[Option<&[u8]>]) -> Box<dyn Executor> {
        let field_type = binary_varchar();
        let mut data = Chunk::new_with_capacity(std::slice::from_ref(&field_type), values.len());
        for value in values {
            match value {
                Some(value) => data.append_bytes(0, value),
                None => data.append_null(0),
            }
        }
        let mut column = Column::new(1, field_type.clone());
        column.index = 0;
        Box::new(OneChunkSource {
            meta: ExecutorMeta::new(Schema::new(vec![column]), 0, values.len(), 1024),
            data: Some(data),
        })
    }

    #[test]
    fn global_binary_count_distinct_uses_direct_bytes() {
        let field_type = binary_varchar();
        let agg = AggFunc {
            kind: AggKind::Count,
            arg: Some(typed_col(0, field_type)),
            extra_args: Vec::new(),
            distinct: true,
            order_by: Vec::new(),
            arg_orig_name: String::new(),
        };
        let exec = HashAggExec::new(
            out_meta(1),
            vec![],
            vec![agg],
            binary_string_source(&[Some(b"a"), Some(b"b"), Some(b"a"), None]),
            NoColumns,
            StatementMemory::default(),
        );
        assert!(exec.direct_global_count_distinct_column().is_some());
        assert_eq!(run(exec), vec![vec![Datum::Int(2)]]);
    }

    #[test]
    fn grouped_binary_strings_use_exact_compact_buckets() {
        let field_type = binary_varchar();
        let output_types = [field_type.clone(), long()];
        let mut first_row = AggFunc::new(AggKind::FirstRow, Some(typed_col(0, field_type.clone())));
        first_row.distinct = true;
        let mut exec = HashAggExec::new(
            out_meta_typed(&output_types),
            vec![typed_col(0, field_type.clone())],
            vec![first_row, AggFunc::new(AggKind::Count, None)],
            binary_string_source(&[Some(b"alpha"), Some(b"beta"), Some(b"alpha"), None]),
            NoColumns,
            StatementMemory::default(),
        );
        assert!(exec.direct_string_group_column().is_some());

        exec.open().unwrap();
        let mut output = exec.new_chunk();
        output.set_required_rows(1, exec.max_chunk_size());
        exec.next(&mut output).unwrap();
        assert_eq!(exec.direct_string_keys.len(), 3);
        assert!(exec.groups.is_empty());
        let mut rows = Vec::new();
        loop {
            rows.extend((0..output.num_rows()).map(|index| {
                let row = output.get_row(index);
                vec![
                    row.get_datum(0, &output_types[0]),
                    row.get_datum(1, &output_types[1]),
                ]
            }));
            output.set_required_rows(exec.max_chunk_size() as isize, exec.max_chunk_size());
            exec.next(&mut output).unwrap();
            if output.num_rows() == 0 {
                break;
            }
        }
        assert_eq!(
            rows,
            vec![
                vec![
                    Datum::String(tidb_datatype::StringDatum::new(
                        b"alpha".to_vec(),
                        Collation::Binary,
                    )),
                    Datum::Int(2),
                ],
                vec![
                    Datum::String(tidb_datatype::StringDatum::new(
                        b"beta".to_vec(),
                        Collation::Binary,
                    )),
                    Datum::Int(1),
                ],
                vec![Datum::Null, Datum::Int(1)],
            ]
        );
        exec.close().unwrap();
    }

    fn decimal_with_shape(flen: i64, scale: i64) -> FieldType {
        let mut field_type = decimal();
        field_type.set_flen(flen);
        field_type.set_decimal(scale);
        field_type
    }

    fn decimal_source(rows: &[Option<&str>], field_type: &FieldType) -> Box<dyn Executor> {
        let mut data =
            Chunk::new_with_capacity(std::slice::from_ref(field_type), rows.len().max(1));
        for value in rows {
            match value {
                Some(value) => data.append_datum(
                    0,
                    &Datum::Decimal(tidb_datatype::Decimal::from_literal(value)),
                ),
                None => data.append_null(0),
            }
        }
        let mut column = Column::new(1, field_type.clone());
        column.index = 0;
        Box::new(OneChunkSource {
            meta: ExecutorMeta::new(Schema::new(vec![column]), 0, rows.len().max(1), 1024),
            data: Some(data),
        })
    }

    fn final_decimal_avg_source(groups: i64) -> Box<dyn Executor> {
        let decimal_type = decimal_with_shape(20, 2);
        let fields = vec![long(), long(), decimal_type.clone()];
        let mut data = Chunk::new_with_capacity(&fields, (groups as usize) * 2);
        for group in 0..groups {
            data.append_int64(0, group);
            data.append_int64(1, 2);
            data.append_datum(
                2,
                &Datum::Decimal(tidb_datatype::Decimal::from_literal("3.00")),
            );
            data.append_int64(0, group);
            data.append_int64(1, 1);
            data.append_datum(
                2,
                &Datum::Decimal(tidb_datatype::Decimal::from_literal("4.00")),
            );
        }
        let columns = fields
            .into_iter()
            .enumerate()
            .map(|(index, field_type)| {
                let mut column = Column::new((index + 1) as i64, field_type);
                column.index = index as i64;
                column
            })
            .collect();
        Box::new(OneChunkSource {
            meta: ExecutorMeta::new(Schema::new(columns), 0, (groups as usize) * 2, 1024),
            data: Some(data),
        })
    }

    fn run_typed(mut exec: HashAggExec<NoColumns>, types: &[FieldType]) -> Vec<Vec<Datum>> {
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        let mut out = Vec::new();
        loop {
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for r in 0..req.num_rows() {
                let row = req.get_row(r);
                out.push(
                    (0..req.num_cols())
                        .map(|c| row.get_datum(c, &types[c]))
                        .collect(),
                );
            }
        }
        exec.close().unwrap();
        out
    }

    #[test]
    fn min_max_skip_nulls_and_report_null_for_an_all_null_group() {
        let agg = HashAggExec::new(
            out_meta(2),
            vec![],
            vec![
                AggFunc::new(AggKind::Min, Some(col(1))),
                AggFunc::new(AggKind::Max, Some(col(1))),
            ],
            source(&[(1, Some(30)), (1, None), (1, Some(10)), (1, Some(20))]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(run(agg), vec![vec![Datum::Int(10), Datum::Int(30)]]);

        let agg = HashAggExec::new(
            out_meta(2),
            vec![],
            vec![
                AggFunc::new(AggKind::Min, Some(col(1))),
                AggFunc::new(AggKind::Max, Some(col(1))),
            ],
            source(&[(1, None), (1, None)]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(run(agg), vec![vec![Datum::Null, Datum::Null]]);
    }

    #[test]
    fn hash_agg_honors_each_output_chunks_required_rows() {
        let rows: Vec<(i64, Option<i64>)> = (0..10).map(|value| (value, Some(value))).collect();
        let mut exec = HashAggExec::new(
            out_meta(1),
            vec![col(0)],
            vec![AggFunc::new(AggKind::FirstRow, Some(col(0)))],
            source(&rows),
            NoColumns,
            StatementMemory::default(),
        );
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        let mut output = Vec::new();
        for (requested, expected) in [(1_usize, 1_usize), (5, 5), (3, 3), (10, 1)] {
            req.set_required_rows(requested as isize, exec.max_chunk_size());
            exec.next(&mut req).unwrap();
            assert_eq!(req.num_rows(), expected);
            output.extend((0..req.num_rows()).map(|row| req.get_row(row).get_int64(0)));
        }
        assert_eq!(output, (0..10).collect::<Vec<_>>());
        exec.close().unwrap();
    }

    #[test]
    fn integer_count_agg_uses_parallel_worker_window() {
        let rows: Vec<(i64, Option<i64>)> = (0..10_000).map(|value| (value, Some(1))).collect();
        let mut exec = HashAggExec::new(
            out_meta(2),
            vec![col(0)],
            vec![
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
                AggFunc::new(AggKind::Count, Some(col(1))),
            ],
            source(&rows),
            NoColumns,
            StatementMemory::default(),
        );
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        let mut output = Vec::new();
        loop {
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for row in 0..req.num_rows() {
                output.push((req.get_row(row).get_int64(0), req.get_row(row).get_int64(1)));
            }
        }
        assert_eq!(output.len(), rows.len());
        assert!(
            exec.parallel_agg_windows() > 0,
            "large integer hash aggregates must use the parallel worker path"
        );
        exec.close().unwrap();
    }

    #[test]
    fn final_decimal_avg_uses_parallel_worker_window() {
        let decimal_input = decimal_with_shape(20, 2);
        let decimal_output = decimal_with_shape(24, 6);
        let mut avg = AggFunc::new(AggKind::Avg, Some(typed_col(1, long())));
        avg.extra_args.push(typed_col(2, decimal_input));
        let output_types = [decimal_output, long()];
        let mut exec = HashAggExec::new(
            out_meta_typed(&output_types),
            vec![typed_col(0, long())],
            vec![avg, AggFunc::new(AggKind::FirstRow, Some(col(0)))],
            final_decimal_avg_source(10_000),
            NoColumns,
            StatementMemory::default(),
        );

        exec.open().unwrap();
        let mut req = exec.new_chunk();
        let mut row_count = 0;
        loop {
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for row_index in 0..req.num_rows() {
                let row = req.get_row(row_index);
                assert_eq!(
                    String::from_utf8(row.get_my_decimal(0).to_string_bytes()).unwrap(),
                    "2.333333"
                );
                assert_eq!(row.get_int64(1), row_count);
                row_count += 1;
            }
        }
        assert_eq!(row_count, 10_000);
        assert!(
            exec.parallel_agg_windows() > 0,
            "q17-shaped final decimal AVG must use the parallel worker path"
        );
        exec.close().unwrap();
    }

    #[test]
    fn integer_count_agg_keeps_all_null_group() {
        let rows = vec![(0, None), (2, Some(7)), (0, None)];
        let agg = HashAggExec::new(
            out_meta(2),
            vec![col(0)],
            vec![
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
                AggFunc::new(AggKind::Count, Some(col(1))),
            ],
            source(&rows),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(
            run(agg),
            vec![
                vec![Datum::Int(0), Datum::Int(0)],
                vec![Datum::Int(2), Datum::Int(1)]
            ]
        );
    }

    #[test]
    fn nested_integer_count_agg_keeps_zero_count_groups() {
        let inner = HashAggExec::new(
            out_meta(1),
            vec![col(0)],
            vec![AggFunc::new(AggKind::Count, Some(col(1)))],
            source(&[(0, None), (1, Some(7)), (2, None), (1, Some(8))]),
            NoColumns,
            StatementMemory::default(),
        );
        let outer = HashAggExec::new(
            out_meta(2),
            vec![col(0)],
            vec![
                AggFunc::new(AggKind::Count, None),
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
            ],
            Box::new(inner),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(
            run(outer),
            vec![
                vec![Datum::Int(2), Datum::Int(0)],
                vec![Datum::Int(1), Datum::Int(2)]
            ]
        );
    }

    #[test]
    fn nested_integer_count_agg_keeps_late_zero_count_groups_across_windows() {
        let rows = (0..150_000i64)
            .map(|key| (key, (key < 100_000).then_some(1)))
            .collect::<Vec<_>>();
        let inner = HashAggExec::new(
            out_meta(1),
            vec![col(0)],
            vec![AggFunc::new(AggKind::Count, Some(col(1)))],
            source(&rows),
            NoColumns,
            StatementMemory::default(),
        );
        let outer = HashAggExec::new(
            out_meta(2),
            vec![col(0)],
            vec![
                AggFunc::new(AggKind::Count, None),
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
            ],
            Box::new(inner),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(
            run(outer),
            vec![
                vec![Datum::Int(100_000), Datum::Int(1)],
                vec![Datum::Int(50_000), Datum::Int(0)]
            ]
        );
    }

    /// Go divides AVG's exact sum by the count with div_precision_increment,
    /// so an integer average carries four fraction digits. The expectations
    /// are `types.DecimalDiv(sum, count, _, 4)` output from the Go
    /// implementation in this repository.
    #[test]
    fn avg_over_integers_is_decimal_scaled_by_the_precision_increment() {
        // Go `typeInfer4Avg` gives AVG(BIGINT) a DECIMAL return scale equal to
        // div_precision_increment, which is four in this test context.
        let types = [decimal_with_shape(15, 4)];
        for (values, want) in [
            (vec![1i64, 2, 3], "2.0000"),
            (vec![1, 2, 4], "2.3333"),
            (vec![1, 0, 0], "0.3333"),
        ] {
            let rows: Vec<(i64, Option<i64>)> = values.iter().map(|v| (1, Some(*v))).collect();
            let agg = HashAggExec::new(
                out_meta_typed(&types),
                vec![],
                vec![AggFunc::new(AggKind::Avg, Some(col(1)))],
                source(&rows),
                NoColumns,
                StatementMemory::default(),
            );
            assert_eq!(
                run_typed(agg, &types),
                vec![vec![Datum::Decimal(tidb_datatype::Decimal::from_literal(
                    want
                ))]],
                "AVG of {values:?}"
            );
        }
    }

    #[test]
    fn avg_over_fixed_scale_decimals_uses_the_go_return_scale_and_skips_nulls() {
        let input_type = decimal_with_shape(15, 2);
        let output_type = decimal_with_shape(19, 6);
        let agg = HashAggExec::new(
            out_meta_typed(std::slice::from_ref(&output_type)),
            vec![],
            vec![AggFunc::new(
                AggKind::Avg,
                Some(typed_col(0, input_type.clone())),
            )],
            decimal_source(
                &[Some("1.00"), None, Some("2.00"), Some("4.00")],
                &input_type,
            ),
            NoColumns,
            StatementMemory::default(),
        );

        let result = run_typed(agg, std::slice::from_ref(&output_type));
        let Datum::Decimal(result) = &result[0][0] else {
            panic!("AVG(DECIMAL) must return DECIMAL");
        };
        assert_eq!(result.to_string(), "2.333333");
        assert_eq!(result.storage_string(), "2.333333");
        assert_eq!(result.declared_shape(), Some((19, 6)));
    }

    #[test]
    fn avg_of_an_all_null_group_is_null() {
        let types = [decimal()];
        let agg = HashAggExec::new(
            out_meta_typed(&types),
            vec![],
            vec![AggFunc::new(AggKind::Avg, Some(col(1)))],
            source(&[(1, None)]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(run_typed(agg, &types), vec![vec![Datum::Null]]);
    }

    /// DISTINCT folds repeated inputs once per group, and the de-duplication
    /// is per group, not global.
    #[test]
    fn distinct_folds_repeats_within_each_group() {
        let mut count = AggFunc::new(AggKind::Count, Some(col(1)));
        count.distinct = true;
        let mut sum = AggFunc::new(AggKind::Sum, Some(col(1)));
        sum.distinct = true;
        // SUM lands in a decimal cell, which is the domain Go sums in.
        let agg = HashAggExec::new(
            out_meta_typed(&[long(), long(), decimal()]),
            vec![col(0)],
            vec![AggFunc::new(AggKind::FirstRow, Some(col(0))), count, sum],
            source(&[
                (1, Some(5)),
                (1, Some(5)),
                (1, Some(7)),
                (1, None),
                (2, Some(5)),
            ]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(
            run_typed(agg, &[long(), long(), decimal()]),
            vec![
                // Group 1 sees 5,5,7,NULL: two distinct non-NULL values.
                vec![
                    Datum::Int(1),
                    Datum::Int(2),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(12))
                ],
                // Group 2's own 5 is not folded into group 1's.
                vec![
                    Datum::Int(2),
                    Datum::Int(1),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(5))
                ],
            ]
        );
    }

    #[test]
    fn distinct_memory_delta_includes_value_set_growth() {
        let mut count = AggFunc::new(AggKind::Count, None);
        count.distinct = true;
        let mut state = AggState::new(&count);
        let mut retained_key_bytes = 0_i64;
        let mut total_delta = 0_i64;
        for byte in 0..128_u8 {
            let value = Datum::Bytes(vec![byte; 32]);
            retained_key_bytes +=
                i64::try_from(group_key_part(&tidb_datatype::Collation::DEFAULT, &value).len())
                    .unwrap();
            total_delta += state.update(Some(value), &[], Vec::new(), None).unwrap();
        }
        assert!(
            total_delta > retained_key_bytes,
            "DISTINCT must charge both retained keys and value-set table growth"
        );
        assert_eq!(
            state
                .update(Some(Datum::Bytes(vec![127; 32])), &[], Vec::new(), None)
                .unwrap(),
            0,
            "a duplicate grows neither the retained payload nor the table"
        );
    }

    fn run(mut exec: HashAggExec<NoColumns>) -> Vec<Vec<Datum>> {
        exec.open().unwrap();
        let mut req = exec.new_chunk();
        let mut out = Vec::new();
        loop {
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for r in 0..req.num_rows() {
                let row = req.get_row(r);
                out.push(
                    (0..req.num_cols())
                        .map(|c| row.get_datum(c, &long()))
                        .collect(),
                );
            }
        }
        exec.close().unwrap();
        out
    }

    #[test]
    fn global_count_and_sum_with_nulls() {
        // Values: 10, NULL, 30 -> COUNT(v)=2 (NULL skipped), COUNT(*)=3, SUM=40.
        let agg = HashAggExec::new(
            out_meta_typed(&[long(), long(), decimal()]),
            vec![],
            vec![
                AggFunc::new(AggKind::Count, Some(col(1))),
                AggFunc::new(AggKind::Count, None),
                AggFunc::new(AggKind::Sum, Some(col(1))),
            ],
            source(&[(1, Some(10)), (1, None), (1, Some(30))]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(
            run_typed(agg, &[long(), long(), decimal()]),
            vec![vec![
                Datum::Int(2),
                Datum::Int(3),
                Datum::Decimal(tidb_datatype::Decimal::from_int(40))
            ]]
        );
    }

    #[test]
    fn group_by_emits_first_seen_order() {
        // Groups 2, 1 in first-seen order; FIRST_ROW carries the key.
        let agg = HashAggExec::new(
            out_meta_typed(&[long(), decimal()]),
            vec![col(0)],
            vec![
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
                AggFunc::new(AggKind::Sum, Some(col(1))),
            ],
            source(&[(2, Some(5)), (1, Some(7)), (2, Some(6))]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(
            run_typed(agg, &[long(), decimal()]),
            vec![
                vec![
                    Datum::Int(2),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(11))
                ],
                vec![
                    Datum::Int(1),
                    Datum::Decimal(tidb_datatype::Decimal::from_int(7))
                ],
            ]
        );
    }

    #[test]
    fn empty_input_rules() {
        // No group-by + no data: one row, COUNT 0, SUM NULL.
        let agg = HashAggExec::new(
            out_meta(2),
            vec![],
            vec![
                AggFunc::new(AggKind::Count, Some(col(1))),
                AggFunc::new(AggKind::Sum, Some(col(1))),
            ],
            source(&[]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(run(agg), vec![vec![Datum::Int(0), Datum::Null]]);

        // Group-by + no data: empty result.
        let agg = HashAggExec::new(
            out_meta(2),
            vec![col(0)],
            vec![
                AggFunc::new(AggKind::FirstRow, Some(col(0))),
                AggFunc::new(AggKind::Count, Some(col(1))),
            ],
            source(&[]),
            NoColumns,
            StatementMemory::default(),
        );
        assert_eq!(run(agg), Vec::<Vec<Datum>>::new());
    }

    #[test]
    fn approx_percentile_uses_ordinal_selection() {
        let percentile = Partial::ApproxPercentile {
            values: vec![Datum::Int(9), Datum::Int(1), Datum::Int(5), Datum::Int(3)],
            percent: Some(50),
        };
        assert_eq!(percentile.finish(&[], 4).unwrap(), Datum::Int(3));

        let maximum = Partial::ApproxPercentile {
            values: vec![Datum::Int(2), Datum::Int(8), Datum::Int(4)],
            percent: Some(100),
        };
        assert_eq!(maximum.finish(&[], 4).unwrap(), Datum::Int(8));

        let empty = Partial::ApproxPercentile {
            values: Vec::new(),
            percent: Some(50),
        };
        assert_eq!(empty.finish(&[], 4).unwrap(), Datum::Null);
    }

    /// `JSON_ARRAYAGG`/`JSON_OBJECTAGG` over a BINARY-charset value: `Opaque`
    /// wrapping tagged with the source column's own MySQL type code, and a
    /// BINARY-charset key failing with 3144. Every expected JSON text is
    /// captured verbatim from a real TiDB server (`zz_dump_opaque_test.go`,
    /// `TestZZDumpOpaque`).
    mod json_agg_opaque {
        use super::*;
        use tidb_datatype::FieldTypeCode;

        fn varbinary() -> FieldType {
            FieldType::new(FieldTypeCode::Varchar).with_collation(tidb_datatype::Collation::Binary)
        }

        /// `SELECT JSON_ARRAYAGG(c_varbin) FROM t` over one row holding
        /// `"ab"` = `["base64:type15:YWI="]`.
        #[test]
        fn json_arrayagg_wraps_binary_charset_value_as_opaque() {
            let mut partial = Partial::JsonArrayAgg(Vec::new(), varbinary());
            partial
                .update(
                    Some(Datum::new_bytes(*b"ab")),
                    &[],
                    Vec::new(),
                    tidb_datatype::Collation::DEFAULT,
                )
                .unwrap();
            let result = partial.finish(&[], 4).unwrap();
            let Datum::Json(json) = result else {
                panic!("expected a JSON datum, got {result:?}");
            };
            assert_eq!(json.to_string(), r#"["base64:type15:YWI="]"#);
        }

        /// `SELECT JSON_OBJECTAGG('k', c_varbin) FROM t` over one row
        /// holding `"ab"` = `{"k": "base64:type15:YWI="}`.
        #[test]
        fn json_objectagg_wraps_binary_charset_value_as_opaque() {
            let mut partial = Partial::JsonObjectAgg(BTreeMap::new(), varbinary(), false);
            partial
                .update(
                    Some(Datum::new_string("k")),
                    &[Datum::new_bytes(*b"ab")],
                    Vec::new(),
                    tidb_datatype::Collation::DEFAULT,
                )
                .unwrap();
            let result = partial.finish(&[], 4).unwrap();
            let Datum::Json(json) = result else {
                panic!("expected a JSON datum, got {result:?}");
            };
            assert_eq!(json.to_string(), r#"{"k": "base64:type15:YWI="}"#);
        }

        /// `SELECT JSON_OBJECTAGG(c_varbin, 1) FROM t`: a BINARY-charset KEY
        /// fails with 3144, captured message `Cannot create a JSON value
        /// from a string with CHARACTER SET 'binary'.`.
        #[test]
        fn json_objectagg_binary_charset_key_is_error_3144() {
            let mut partial = Partial::JsonObjectAgg(BTreeMap::new(), long(), true);
            let err = partial
                .update(
                    Some(Datum::new_bytes(*b"ab")),
                    &[Datum::Int(1)],
                    Vec::new(),
                    tidb_datatype::Collation::DEFAULT,
                )
                .unwrap_err();
            assert!(matches!(
                err,
                ExecError::InvalidJsonCharset { charset } if charset == "binary"
            ));
        }
    }

    /// `APPROX_COUNT_DISTINCT` argument-encoding tests: golden counts
    /// captured from Go (`TestZZDumpApxEnc`,
    /// `pkg/executor/zz_dump_apxenc_test.go`) that exercise the TIME,
    /// DURATION, and JSON encodings ported into `approx_count_distinct_encode`.
    mod approx_count_distinct_encoding {
        use super::*;
        use tidb_datatype::{CoreTime, MySqlDuration, Time, TimeType};

        /// Feeds `values` through `approx_count_distinct_encode` and the
        /// BJKST sketch exactly as `Partial::ApproxCountDistinct` does, and
        /// returns the resulting distinct count.
        fn distinct_count(values: &[Datum]) -> u64 {
            let mut sketch = ApproxCountDistinctSketch::new();
            for value in values {
                let encoded = approx_count_distinct_encode(value).unwrap();
                sketch.insert(&encoded);
            }
            sketch.fixed_size()
        }

        fn date(y: u16, m: u8, d: u8) -> Datum {
            let core = CoreTime::from_date(y, m, d, 0, 0, 0, 0);
            Datum::Time(Time::new(core, TimeType::Date, 0).unwrap())
        }

        fn datetime_micros(base_micros: u32, offset: u32) -> Datum {
            // 2000-01-01 00:00:00 plus `offset` microseconds, matching the
            // Go capture's `date_add('2000-01-01 00:00:00', interval i
            // microsecond)` -- carried by hand into hour/minute/second so
            // CoreTime's fields never overflow their bit width.
            let total = base_micros as u64 + offset as u64;
            let micros = (total % 1_000_000) as u32;
            let total_seconds = total / 1_000_000;
            let second = (total_seconds % 60) as u8;
            let total_minutes = total_seconds / 60;
            let minute = (total_minutes % 60) as u8;
            let total_hours = total_minutes / 60;
            let hour = (total_hours % 24) as u8;
            let day = 1 + (total_hours / 24) as u8;
            let core = CoreTime::from_date(2000, 1, day, hour, minute, second, micros);
            Datum::Time(Time::new(core, TimeType::DateTime, 6).unwrap())
        }

        fn duration(nanoseconds: i64, fsp: i64) -> Datum {
            Datum::Duration(MySqlDuration::from_nanoseconds(nanoseconds, fsp).unwrap())
        }

        fn json(value: &BinaryJSONValue) -> Datum {
            Datum::Json(BinaryJSON::from_typed_value(value).unwrap())
        }

        // Go: `insert into t_date values (1,'2020-01-01'),(2,'2020-01-01'),
        // (3,'2020-01-02')` -> ZZDUMP date_small = 2.
        #[test]
        fn date_dedup_matches_go() {
            let values = [date(2020, 1, 1), date(2020, 1, 1), date(2020, 1, 2)];
            assert_eq!(distinct_count(&values), 2);
        }

        // Go: `insert into t_dur values (1,'01:02:03.400000'),
        // (2,'01:02:03.400000'),(3,'11:22:33.000001')` -> ZZDUMP
        // dur_small = 2.
        #[test]
        fn duration_dedup_matches_go() {
            let a = (3600 + 2 * 60 + 3) as i64 * 1_000_000_000 + 400_000_000;
            let b = (11 * 3600 + 22 * 60 + 33) as i64 * 1_000_000_000 + 1_000;
            let values = [duration(a, 6), duration(a, 6), duration(b, 6)];
            assert_eq!(distinct_count(&values), 2);
        }

        // Go: `insert into t_json values (1,'3'),(2,'3.0'),(3,'[1,2]'),
        // (4,'[1,2]'),(5,'{"a":1,"b":2}')` -> ZZDUMP json_small = 3 (the
        // integer and the equal-valued double collide, the array pair
        // collides, the object is its own distinct value).
        #[test]
        fn json_dedup_matches_go() {
            let array = || {
                BinaryJSONValue::Array(vec![BinaryJSONValue::Int64(1), BinaryJSONValue::Int64(2)])
            };
            let object = BinaryJSONValue::Object(BTreeMap::from([
                ("a".to_owned(), BinaryJSONValue::Int64(1)),
                ("b".to_owned(), BinaryJSONValue::Int64(2)),
            ]));
            let values = [
                json(&BinaryJSONValue::Int64(3)),
                json(&BinaryJSONValue::Float64(3.0)),
                json(&array()),
                json(&array()),
                json(&object),
            ];
            assert_eq!(distinct_count(&values), 3);
        }

        // Go: `insert into t_dt` with 75000 rows of
        // `date_add('2000-01-01 00:00:00', interval i microsecond)` for
        // `i` in `0..75000` -> ZZDUMP dt_large = 74710 (the BJKST sketch's
        // extrapolated estimate once the exact-count threshold is
        // exceeded). This is the encoding this module ports: without the
        // 16-byte `appendTime` layout, TIME arguments would fall back to a
        // different byte representation and this sketch would diverge from
        // Go's for exactly this reason.
        #[test]
        fn datetime_large_cardinality_matches_go_estimate() {
            let n: u32 = 75_000;
            let values: Vec<Datum> = (0..n).map(|i| datetime_micros(0, i)).collect();
            assert_eq!(distinct_count(&values), 74_710);
        }

        // A mixed-type multi-argument tuple: Go encodes each argument in
        // turn into one shared byte buffer per row before hashing, so two
        // rows with the same (INT, DATETIME, JSON) triple must encode to
        // the same bytes.
        //
        // Go: `insert into t_mixed values (1,1,'2020-01-01 00:00:00','1'),
        // (2,1,'2020-01-01 00:00:00','1'),(3,2,'2020-01-01 00:00:00','1')`
        // -> ZZDUMP mixed_small = 2.
        #[test]
        fn mixed_tuple_dedup_matches_go() {
            fn tuple_bytes(a: i64, b: Datum, c: Datum) -> Vec<u8> {
                let mut encoded = approx_count_distinct_encode(&Datum::Int(a)).unwrap();
                encoded.extend(approx_count_distinct_encode(&b).unwrap());
                encoded.extend(approx_count_distinct_encode(&c).unwrap());
                encoded
            }
            let dt = || datetime_micros(0, 0);
            let mut sketch = ApproxCountDistinctSketch::new();
            for row in [
                tuple_bytes(1, dt(), json(&BinaryJSONValue::Int64(1))),
                tuple_bytes(1, dt(), json(&BinaryJSONValue::Int64(1))),
                tuple_bytes(2, dt(), json(&BinaryJSONValue::Int64(1))),
            ] {
                sketch.insert(&row);
            }
            assert_eq!(sketch.fixed_size(), 2);
        }
    }
}
