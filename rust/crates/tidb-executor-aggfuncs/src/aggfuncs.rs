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

//! Go `pkg/executor/aggfuncs` helpers shared by the `func_*` ports, and the
//! aggregate descriptions (`AggFunc`, `AggKind`) the driver lowers into a
//! pushed-down aggregation (`builder`).

pub mod builder;

use tidb_driver_error::ExecError;

use tidb_charset::Collation;
use tidb_datatype::CollationCollator;
use tidb_codec::encode_compact_bytes;
use tidb_datatype::{BinaryJSON, BinaryJSONValue, Datum, FieldType, TimeType};
use tidb_expr::expression::Expression;
use tidb_exprctx::Columns;

/// Go's four `approx_count_distinct` signatures (`aggfuncs/builder.go`
/// `buildApproxCountDistinct`): whether the rows carry serialized partial
/// sketches (Partial2/Final) and whether the output is one (Partial1/Partial2).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ApproxCountDistinctSig {
    /// The argument is a `Serialize`d partial result to merge.
    pub merge_input: bool,
    /// The result is the `Serialize`d sketch rather than its count.
    pub sketch_output: bool,
}

impl ApproxCountDistinctSig {
    /// Go's choice by the descriptor's mode and result type: a BIGINT result
    /// selects by mode alone; a string result (a partial state column) is
    /// Partial1 for Complete/Partial1 and Partial2 for Partial2/Final.
    pub fn of(mode: tidb_expression_aggregation::AggFunctionMode, string_result: bool) -> Option<Self> {
        use tidb_expression_aggregation::AggFunctionMode as Mode;
        let (merge_input, sketch_output) = match (mode, string_result) {
            (Mode::Complete, false) => (false, false),
            (Mode::Partial1, false) | (Mode::Complete | Mode::Partial1, true) => (false, true),
            (Mode::Partial2, _) | (Mode::Final, true) => (true, true),
            (Mode::Final, false) => (true, false),
            _ => return None,
        };
        Some(Self {
            merge_input,
            sketch_output,
        })
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
    /// `MIN_COUNT`: count rows tied at the minimum argument.
    MinCount,
    /// `MAX_COUNT`: count rows tied at the maximum argument.
    MaxCount,
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
    /// ([`PartialResult4ApproxCountDistinct`]) over the FarmHash `Hash64` of the
    /// encoded argument tuple, skipping any row with a NULL argument. Below
    /// `uniquesHashMaxSize` (65536) distinct values the sketch keeps every
    /// hash and the answer is the exact distinct count; above that
    /// threshold Go starts discarding hashes and extrapolating the true
    /// cardinality, which this sketch reproduces bit for bit. The signature
    /// says whether the input rows are serialized sketches and whether the
    /// output is one (Go's Original/Partial1/Partial2/Final).
    ApproxCountDistinct(ApproxCountDistinctSig),
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

/// The bytes one `GROUP_CONCAT` input contributes, which Go produces by
/// casting the argument to a string.
/// The derived collation of a group-by expression.
pub fn expr_collation(expr: &Expression) -> tidb_charset::Collation {
    tidb_expr::collation_derive::collation_of_node(expr)
}

/// Go `SessionVars.GroupConcatMaxLen` as this statement sees it.
pub fn group_concat_max_len<C: Columns + ?Sized>(ctx: &C) -> u64 {
    match ctx.sysvar(None, "group_concat_max_len") {
        Some(Datum::UInt(value)) => value,
        Some(Datum::Int(value)) if value >= 0 => value as u64,
        // Go `DefGroupConcatMaxLen`.
        _ => 1024,
    }
}

pub fn group_concat_bytes(value: &Datum) -> Result<Vec<u8>, ExecError> {
    // Go's `builtinGroupConcat.writeValue` formats every non-`KindBytes`
    // value through its datum value (`fmt %v`), covering temporal, duration,
    // ENUM/SET, BIT, JSON, and vector values in addition to scalar/string
    // cases. Reuse the shared byte-authoritative conversion for those textual
    // forms so binary string values keep their original octets.
    // Go's `writeValue` formats `KindBinaryLiteral`/`KindMysqlBit` through
    // `fmt %v` on `BinaryLiteral`, whose String method emits the `0x...`
    // spelling (it is not the decimal value used by numeric coercion).
    if let Datum::BinaryLiteral(value) | Datum::Bit(value) = value {
        return Ok(value.to_string().into_bytes());
    }
    value.sql_bytes().map_err(|_| {
        ExecError::unsupported("GROUP_CONCAT over this datum kind is not yet supported")
    })
}

/// One argument's contribution to the hashed tuple of `APPROX_COUNT_DISTINCT`
/// and of the generic `COUNT(DISTINCT ...)` (Go `countOriginalWithDistinct`),
/// Go `func_count_distinct.go`'s `evalAndEncode`: each argument type has its
/// own raw encoding (a fixed-width native-endian copy of the scalar for
/// `INT`/`REAL`, `MyDecimal.ToHashKey` for `DECIMAL`, a collation sort key
/// wrapped in `codec.EncodeCompactBytes` for `STRING`/`BINARY`, the vector's
/// wire serialization for `VECTOR`). The sketch's bytes feed FarmHash
/// directly, so it only matches Go's numbers if the hash INPUT matches too.
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
/// equal values hash equal. String and byte values use the aggregate
/// expression's collator, matching Go's per-argument `evalAndEncode` input;
/// the caller must not infer it from the materialized datum alone.
pub fn approx_count_distinct_encode(
    datum: &Datum,
    collation: Collation,
) -> Result<Vec<u8>, ExecError> {
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
            let key = collation.immutable_key(text.bytes());
            let mut encoded = Vec::new();
            encode_compact_bytes(&mut encoded, &key);
            encoded
        }
        Datum::Bytes(bytes) => {
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
    pub const TIMESTAMP: u8 = 7;
    pub const DATE: u8 = 10;
    pub const DATETIME: u8 = 12;
}

/// Encodes a finished JSON aggregate, Go's `CreateBinaryJSONWithCheck`.
pub fn create_binary_json_with_check(value: BinaryJSONValue) -> Result<Datum, ExecError> {
    BinaryJSON::from_typed_value(&value)
        .map(Datum::Json)
        .map_err(|_| ExecError::unsupported("this JSON document cannot be encoded"))
}
