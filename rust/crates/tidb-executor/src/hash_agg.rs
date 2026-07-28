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

//! `pkg/executor/aggregate` `HashAggExec`, serial path (`unparallelExec`):
//! group the child's rows by the group-by expressions and fold each group
//! through the aggregate functions.
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
//! DEFERRED (documented): the parallel partial/final worker pipeline, spill,
//! memory tracking; and a BINARY-charset JSON aggregate argument, which Go
//! wraps in a JSON `Opaque`;
//! SUM-over-integer's DECIMAL result domain -- this seed accumulates integer
//! sums in `i64` and reports overflow as an error rather than widening to
//! decimal (Go returns DECIMAL; lands with the layout-faithful MyDecimal);
//! and Go's `Round(retTp.GetDecimal())` display step on the AVG result.
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

use crate::approx_count_distinct::ApproxCountDistinctSketch;
use crate::executor::{ExecError, Executor, ExecutorMeta};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use tidb_chunk::chunk::Chunk;
use tidb_codec::encode_compact_bytes;
use tidb_datatype::{BinaryJSON, BinaryJSONValue, Collation, Datum, Decimal, FieldType, TimeType};
use tidb_expr::compare_datums;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

/// The aggregate function kinds this seed supports.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AggKind {
    /// `COUNT(expr)` / `COUNT(*)` (no argument).
    Count,
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
        }
    }
}

/// One group's partial results, in agg-func order.
enum Partial {
    Count(i64),
    /// `None` until the first non-NULL input (an empty sum is NULL). Go
    /// sums an integer or decimal argument exactly, in the decimal domain --
    /// `SUM` over a BIGINT column is a DECIMAL in MySQL.
    SumDecimal(Option<Decimal>),
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
struct AggState {
    partial: Partial,
    seen: Option<HashSet<Vec<u8>>>,
}

impl AggState {
    fn new(func: &AggFunc) -> AggState {
        AggState {
            partial: Partial::new(&func.kind),
            seen: func.distinct.then(HashSet::new),
        }
    }

    /// Folds one row's input in, skipping values this group has already seen
    /// when the function is `DISTINCT`.
    ///
    /// Go keys its `valueSet` on the codec encoding of the evaluated argument;
    /// the datum hash key is that same encoding. A datum with no hash key
    /// (Go's encode error) fails the statement rather than silently counting
    /// twice.
    fn update(
        &mut self,
        value: Option<Datum>,
        extra: &[Datum],
        sort_key: Vec<Datum>,
    ) -> Result<(), ExecError> {
        if let Some(seen) = &mut self.seen {
            let datum = value.clone().unwrap_or(Datum::Null);
            if datum != Datum::Null {
                let key = datum
                    .to_hash_key()
                    .map_err(|_| ExecError::Unsupported("DISTINCT over this datum kind"))?;
                if !seen.insert(key) {
                    return Ok(());
                }
            }
        }
        self.partial.update(value, extra, sort_key)
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
fn group_key_part(collation: &tidb_datatype::Collation, datum: &Datum) -> Vec<u8> {
    match datum.as_raw_bytes() {
        Some(bytes) => {
            let mut encoded = Vec::new();
            encode_compact_bytes(&mut encoded, &collation.key(bytes));
            encoded
        }
        None => tidb_codec::hash_code(datum),
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
            return Err(ExecError::Unsupported(
                "GROUP_CONCAT over this datum kind is not yet supported",
            ))
        }
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
    let unsupported = || ExecError::Unsupported("APPROX_COUNT_DISTINCT over this datum kind");
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
            encoded.extend_from_slice(&(i64::from(duration.fsp())).to_le_bytes());
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

    fn update(
        &mut self,
        value: Option<Datum>,
        extra: &[Datum],
        sort_key: Vec<Datum>,
    ) -> Result<(), ExecError> {
        match (self, value) {
            // Go appends the converted value for EVERY row, so a NULL input
            // lands in the array as JSON `null` rather than being skipped.
            (Partial::JsonArrayAgg(..), None) => {
                return Err(ExecError::Unsupported("JSON_ARRAYAGG requires an argument"))
            }
            (Partial::JsonArrayAgg(entries, value_type), Some(input)) => {
                entries.push(json_value(&input, value_type)?)
            }
            (Partial::JsonObjectAgg(..), None | Some(Datum::Null)) => {
                return Err(ExecError::JsonDocumentNullKey)
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
                })
            }
            (Partial::JsonObjectAgg(entries, value_type, false), Some(key)) => {
                let value = extra.first().cloned().unwrap_or(Datum::Null);
                entries.insert(json_object_key(&key)?, json_value(&value, value_type)?);
            }
            // A row with a NULL argument (or, for the multi-argument form, a
            // NULL in ANY argument, which the caller has already collapsed to
            // one NULL) never reaches the sketch.
            (Partial::ApproxCountDistinct(_), None) => {
                return Err(ExecError::Unsupported(
                    "APPROX_COUNT_DISTINCT requires an argument",
                ))
            }
            (Partial::ApproxCountDistinct(_), Some(Datum::Null)) => {}
            // The caller (the group-fold loop below) has already encoded the
            // row's argument tuple the way Go's `evalAndEncode` does; this
            // just feeds those bytes through FarmHash into the sketch.
            (Partial::ApproxCountDistinct(sketch), Some(Datum::Bytes(encoded))) => {
                sketch.insert(&encoded);
            }
            (Partial::ApproxCountDistinct(_), Some(_)) => {
                return Err(ExecError::Unsupported(
                    "APPROX_COUNT_DISTINCT requires a pre-encoded argument tuple",
                ))
            }
            (Partial::ApproxPercentile { .. }, None) => {
                return Err(ExecError::Unsupported(
                    "APPROX_PERCENTILE requires an argument",
                ))
            }
            (Partial::ApproxPercentile { .. }, Some(Datum::Null)) => {}
            (Partial::ApproxPercentile { values, .. }, Some(input)) => values.push(input),
            // COUNT(*): every row counts; COUNT(expr): NULL skipped.
            (Partial::Count(n), None) => *n += 1,
            (Partial::Count(_), Some(Datum::Null)) => {}
            (Partial::Count(n), Some(_)) => *n += 1,
            (Partial::SumDecimal(_) | Partial::SumReal(_), None) => {
                return Err(ExecError::Unsupported("SUM requires an argument"))
            }
            (Partial::SumDecimal(_) | Partial::SumReal(_), Some(Datum::Null)) => {}
            (this @ Partial::SumDecimal(None), Some(Datum::Real(v))) => {
                // First non-NULL input is real: the sum's domain is real.
                *this = Partial::SumReal(Some(v));
            }
            (Partial::SumDecimal(acc), Some(input)) => {
                let addend = match input {
                    Datum::Int(v) => Decimal::from_int(v),
                    Datum::UInt(v) => Decimal::from_uint(v),
                    Datum::Decimal(d) => d,
                    _ => {
                        return Err(ExecError::Unsupported(
                            "SUM over this datum kind is not yet supported",
                        ))
                    }
                };
                *acc = Some(match acc.take() {
                    Some(sum) => sum.add(&addend),
                    None => addend,
                });
            }
            (Partial::SumReal(acc), Some(Datum::Real(v))) => {
                *acc = Some(acc.unwrap_or(0.0) + v);
            }
            (Partial::SumReal(acc), Some(Datum::Int(v))) => {
                *acc = Some(acc.unwrap_or(0.0) + v as f64);
            }
            (Partial::SumReal(_), Some(_)) => {
                return Err(ExecError::Unsupported(
                    "SUM over this datum kind is not yet supported",
                ))
            }
            // Go `builtinGroupConcat`: a NULL input contributes nothing at
            // all, and every other value is stringified before it is joined.
            (Partial::GroupConcat { .. }, None) => {
                return Err(ExecError::Unsupported("GROUP_CONCAT requires an argument"))
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
                return Err(ExecError::Unsupported("MIN/MAX requires an argument"))
            }
            (Partial::MaxMin { .. }, Some(Datum::Null)) => {}
            (Partial::MaxMin { value, is_max }, Some(input)) => match value {
                None => *value = Some(input),
                Some(current) => {
                    let ordering = compare_datums(&input, current)?;
                    if (*is_max && ordering == Ordering::Greater)
                        || (!*is_max && ordering == Ordering::Less)
                    {
                        *value = Some(input);
                    }
                }
            },
            (Partial::AvgDecimal { .. } | Partial::AvgReal { .. }, None) => {
                return Err(ExecError::Unsupported("AVG requires an argument"))
            }
            (Partial::AvgDecimal { .. } | Partial::AvgReal { .. }, Some(Datum::Null)) => {}
            (this @ Partial::AvgDecimal { .. }, Some(Datum::Real(v))) => {
                // First non-NULL input is real: Go's return type is DOUBLE.
                let Partial::AvgDecimal { count, .. } = this else {
                    unreachable!()
                };
                debug_assert_eq!(*count, 0, "the domain is fixed by the first input");
                *this = Partial::AvgReal { sum: v, count: 1 };
            }
            (Partial::AvgDecimal { sum, count }, Some(input)) => {
                let addend = match input {
                    Datum::Int(v) => Decimal::from_int(v),
                    Datum::UInt(v) => Decimal::from_uint(v),
                    Datum::Decimal(d) => d,
                    _ => {
                        return Err(ExecError::Unsupported(
                            "AVG over this datum kind is not yet supported",
                        ))
                    }
                };
                *sum = sum.add(&addend);
                *count += 1;
            }
            // Go's bit functions cast the argument to `UNSIGNED BIGINT` and
            // skip NULL, so an all-NULL group keeps the identity.
            (Partial::Bit { .. }, None) => {
                return Err(ExecError::Unsupported(
                    "BIT_AND/BIT_OR/BIT_XOR requires an argument",
                ))
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
                return Err(ExecError::Unsupported(
                    "the variance/stddev family requires an argument",
                ))
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
                        ExecError::Unsupported("the variance/stddev family over this datum kind")
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
                let addend = match input {
                    Datum::Real(v) => v,
                    Datum::Int(v) => v as f64,
                    _ => {
                        return Err(ExecError::Unsupported(
                            "AVG over this datum kind is not yet supported",
                        ))
                    }
                };
                *sum += addend;
                *count += 1;
            }
        }
        Ok(())
    }

    fn finish(&self, order_by: &[(Expression, bool)]) -> Result<Datum, ExecError> {
        Ok(match self {
            Partial::Count(n) => Datum::Int(*n),
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
                let mut sorted = values.clone();
                sorted
                    .sort_by(|left, right| compare_datums(left, right).unwrap_or(Ordering::Equal));
                let rank = (sorted.len() as f64 * (percent as f64 / 100.0)).ceil() as usize;
                let index = rank.clamp(1, sorted.len()) - 1;
                sorted[index].clone()
            }
            // An empty group concatenates to NULL, not an empty string.
            Partial::GroupConcat { values, .. } if values.is_empty() => Datum::Null,
            Partial::GroupConcat { values, separator } => {
                // Go sorts the collected rows by the aggregate's own ORDER BY
                // before joining them; without one the rows keep arrival
                // order, which MySQL documents as undefined.
                let mut values = values.clone();
                if !order_by.is_empty() {
                    values.sort_by(|left, right| {
                        for (position, (_, desc)) in order_by.iter().enumerate() {
                            let (Some(a), Some(b)) = (left.1.get(position), right.1.get(position))
                            else {
                                continue;
                            };
                            let ordering =
                                tidb_expr::compare_datums(a, b).unwrap_or(Ordering::Equal);
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
            Partial::SumReal(Some(v)) => Datum::Real(*v),
            Partial::FirstRow(v) => v.clone().unwrap_or(Datum::Null),
            Partial::MaxMin { value, .. } => value.clone().unwrap_or(Datum::Null),
            // Go divides the exact sum by the count with the session's
            // div_precision_increment, the same rule the `/` operator follows.
            Partial::AvgDecimal { count: 0, .. } | Partial::AvgReal { count: 0, .. } => Datum::Null,
            Partial::AvgDecimal { sum, count } => {
                let divisor = Decimal::from_int(*count);
                let target_scale = sum.scale() + DIV_PRECISION_INCREMENT;
                match sum.true_div(&divisor, target_scale) {
                    Some(quotient) => Datum::Decimal(quotient),
                    None => Datum::Null,
                }
            }
            Partial::AvgReal { sum, count } => Datum::Real(sum / *count as f64),
            // Go's result column is a SIGNED `BIGINT` holding the unsigned
            // fold's bit pattern, which is why `BIT_AND` over an all-NULL
            // group prints `-1` rather than `18446744073709551615`
            // (captured from TiDB).
            Partial::Bit { acc, .. } => Datum::Int(*acc as i64),
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
        .map_err(|_| ExecError::Unsupported("this datum kind is not a JSON value"))
}

/// `JSON_OBJECTAGG`'s member name: Go reads the key argument with
/// `EvalString`, so a non-string key is stringified (`JSON_OBJECTAGG(id, v)`
/// keys the object with `"1"`, `"2"`, ...).
fn json_object_key(value: &Datum) -> Result<String, ExecError> {
    value
        .sql_string()
        .map_err(|_| ExecError::Unsupported("this datum kind is not a JSON member name"))
}

/// Encodes a finished JSON aggregate, Go's `CreateBinaryJSONWithCheck`.
fn encode_json(value: BinaryJSONValue) -> Result<Datum, ExecError> {
    BinaryJSON::from_typed_value(&value)
        .map(Datum::Json)
        .map_err(|_| ExecError::Unsupported("this JSON document cannot be encoded"))
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
                .map_err(|_| ExecError::Unsupported("BIT_AND/BIT_OR/BIT_XOR over this datum kind"))?
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
/// `None` stands for `COUNT(*)`'s absent argument; every other aggregate takes
/// `Some(value)`, with `Some(Datum::Null)` for a NULL input. Each item pairs
/// that first argument with the values of any further arguments, exactly the
/// pair the GROUP BY path builds per source row.
pub(crate) fn aggregate_rows(
    kind: &AggKind,
    rows: impl IntoIterator<Item = (Option<Datum>, Vec<Datum>)>,
) -> Result<Datum, ExecError> {
    let mut partial = Partial::new(kind);
    for (value, extra) in rows {
        partial.update(value, &extra, Vec::new())?;
    }
    partial.finish(&[])
}

/// Go's default `div_precision_increment`, the scale AVG's division adds over
/// its sum (`typeInfer4Avg` sets the result's decimals to it).
const DIV_PRECISION_INCREMENT: u32 = 4;

/// Go `HashAggExec` (serial): hash aggregation over the child's rows.
pub struct HashAggExec<C: Columns> {
    meta: ExecutorMeta,
    group_by: Vec<Expression>,
    agg_funcs: Vec<AggFunc>,
    child: Box<dyn Executor>,
    ctx: C,
    child_chunk: Chunk,
    emitted: bool,
}

impl<C: Columns> HashAggExec<C> {
    /// Builds a hash aggregation of `agg_funcs` over `child`, grouped by
    /// `group_by` (empty for a global aggregate).
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        group_by: Vec<Expression>,
        agg_funcs: Vec<AggFunc>,
        child: Box<dyn Executor>,
        ctx: C,
    ) -> Self {
        let child_chunk = child.new_chunk();
        HashAggExec {
            meta,
            group_by,
            agg_funcs,
            child,
            ctx,
            child_chunk,
            emitted: false,
        }
    }
}

impl<C: Columns> Executor for HashAggExec<C> {
    fn open(&mut self) -> Result<(), ExecError> {
        self.child.open()?;
        self.child_chunk.reset();
        self.emitted = false;
        Ok(())
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        req.reset();
        if self.emitted {
            return Ok(());
        }
        // Group key (encoded bytes) -> partials; emission in first-seen order.
        let mut groups: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut ordered: Vec<Vec<AggState>> = Vec::new();
        loop {
            self.child.next(&mut self.child_chunk)?;
            let rows = self.child_chunk.num_rows();
            if rows == 0 {
                break;
            }
            for r in 0..rows {
                let row = self.child_chunk.get_row(r);
                // Encode the group key from the evaluated group-by datums.
                let mut key = Vec::new();
                for expr in &self.group_by {
                    let datum = expr.eval(&self.ctx, row)?;
                    key.extend_from_slice(&group_key_part(&expr_collation(expr), &datum));
                    key.push(0xff); // separator, as key parts are length-coded
                }
                let idx = match groups.get(&key) {
                    Some(&idx) => idx,
                    None => {
                        let idx = ordered.len();
                        groups.insert(key, idx);
                        ordered.push(self.agg_funcs.iter().map(AggState::new).collect());
                        idx
                    }
                };
                for (f, state) in self.agg_funcs.iter().zip(ordered[idx].iter_mut()) {
                    let mut extra_values: Vec<Datum> = Vec::new();
                    let value = if f.extra_args.is_empty()
                        && !matches!(f.kind, AggKind::ApproxCountDistinct)
                    {
                        match &f.arg {
                            Some(expr) => Some(expr.eval(&self.ctx, row)?),
                            None => None,
                        }
                    } else if matches!(f.kind, AggKind::JsonObjectAgg { .. }) {
                        // `JSON_OBJECTAGG(key, value)` is the one aggregate
                        // whose two arguments stay SEPARATE: the key is
                        // stringified into a member name and the value keeps
                        // its own type, so neither can be folded into the
                        // other the way COUNT's tuple or GROUP_CONCAT's
                        // concatenation is.
                        for expr in &f.extra_args {
                            extra_values.push(expr.eval(&self.ctx, row)?);
                        }
                        match &f.arg {
                            Some(expr) => Some(expr.eval(&self.ctx, row)?),
                            None => None,
                        }
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
                            let datum = expr.eval(&self.ctx, row)?;
                            if datum == Datum::Null {
                                tuple_key = None;
                                break;
                            }
                            if let Some(buf) = &mut tuple_key {
                                let key = datum.to_hash_key().map_err(|_| {
                                    ExecError::Unsupported("COUNT over this datum kind")
                                })?;
                                buf.extend_from_slice(&(key.len() as u64).to_be_bytes());
                                buf.extend_from_slice(&key);
                            }
                        }
                        Some(tuple_key.map_or(Datum::Null, Datum::Bytes))
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
                            let datum = expr.eval(&self.ctx, row)?;
                            if datum == Datum::Null {
                                tuple_key = None;
                                break;
                            }
                            if let Some(buf) = &mut tuple_key {
                                buf.extend_from_slice(&approx_count_distinct_encode(&datum)?);
                            }
                        }
                        Some(tuple_key.map_or(Datum::Null, Datum::Bytes))
                    } else {
                        // Multi-argument GROUP_CONCAT: Go's `groupConcat`
                        // update loop stringifies and concatenates every
                        // argument per row, and skips the row entirely as
                        // soon as ANY argument evaluates to NULL. DISTINCT
                        // then dedupes over this concatenated value.
                        let mut concatenated = Some(Vec::new());
                        for expr in f.arg.iter().chain(f.extra_args.iter()) {
                            let datum = expr.eval(&self.ctx, row)?;
                            if datum == Datum::Null {
                                concatenated = None;
                                break;
                            }
                            if let Some(buf) = &mut concatenated {
                                buf.extend_from_slice(&group_concat_bytes(&datum)?);
                            }
                        }
                        Some(concatenated.map_or(Datum::Null, Datum::Bytes))
                    };
                    // GROUP_CONCAT's own ORDER BY is evaluated over the same
                    // source row that produced the value, so the key travels
                    // with it into the group.
                    let mut sort_key = Vec::with_capacity(f.order_by.len());
                    for (expr, _) in &f.order_by {
                        sort_key.push(expr.eval(&self.ctx, row)?);
                    }
                    state.update(value, &extra_values, sort_key)?;
                }
            }
        }
        // No group-by and no data: one empty group, so a global COUNT is 0.
        if ordered.is_empty() && self.group_by.is_empty() {
            ordered.push(self.agg_funcs.iter().map(AggState::new).collect());
        }
        for states in &ordered {
            for (c, state) in states.iter().enumerate() {
                let order_by = self
                    .agg_funcs
                    .get(c)
                    .map_or(&[][..], |func| func.order_by.as_slice());
                req.append_datum(c, &state.partial.finish(order_by)?);
            }
        }
        self.emitted = true;
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::NoColumns;

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

    fn decimal() -> FieldType {
        FieldType::new(tidb_datatype::FieldTypeCode::NewDecimal)
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
        );
        assert_eq!(run(agg), vec![vec![Datum::Null, Datum::Null]]);
    }

    /// Go divides AVG's exact sum by the count with div_precision_increment,
    /// so an integer average carries four fraction digits. The expectations
    /// are `types.DecimalDiv(sum, count, _, 4)` output from the Go
    /// implementation in this repository.
    #[test]
    fn avg_over_integers_is_decimal_scaled_by_the_precision_increment() {
        let types = [decimal()];
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
    fn avg_of_an_all_null_group_is_null() {
        let types = [decimal()];
        let agg = HashAggExec::new(
            out_meta_typed(&types),
            vec![],
            vec![AggFunc::new(AggKind::Avg, Some(col(1)))],
            source(&[(1, None)]),
            NoColumns,
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
        );
        assert_eq!(run(agg), Vec::<Vec<Datum>>::new());
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
                .update(Some(Datum::new_bytes(*b"ab")), &[], Vec::new())
                .unwrap();
            let result = partial.finish(&[]).unwrap();
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
                )
                .unwrap();
            let result = partial.finish(&[]).unwrap();
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
                .update(Some(Datum::new_bytes(*b"ab")), &[Datum::Int(1)], Vec::new())
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
