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

//! Go `pkg/executor/aggfuncs`' aggregate and window function states
//! (`aggfuncs.go` and the `func_*.go` files): the descriptors the builder
//! lowers (`builder`), their typed inputs (`input`), partial results and
//! the sliding-window states.

use tidb_datatype::CollationCollator;

pub mod builder;
pub mod input;
pub mod window_extremum;
pub mod window_numeric;

use input::AggInputMode;

use crate::approx_count_distinct::ApproxCountDistinctSketch;
use tidb_driver_error::ExecError;

use std::cmp::Ordering;
use std::collections::BTreeMap;
use tidb_chunk::chunk::Chunk;
use tidb_codec::{encode_bytes, encode_compact_bytes};
use tidb_goruntime::GoString;
use tidb_datatype::{BinaryJSON, BinaryJSONValue, Datum, Decimal, EvalType, FieldType, FieldTypeCode, TimeType, MAX_DECIMAL_SCALE, UNSPECIFIED_LENGTH};
use tidb_charset::Collation;
use tidb_expr::compare_datums;
use tidb_expr::expression::Expression;
use tidb_exprctx::Columns;
use tidb_selection::{select, Selectable};
use tidb_set::{
    Float64SetWithMemoryUsage, Int64SetWithMemoryUsage, StringSetWithMemoryUsage,
};

pub struct DatumSelection<'a>(&'a mut [Datum]);

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
    /// ([`ApproxCountDistinctSketch`]) over the FarmHash `Hash64` of the
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

/// One row's aggregate input. `GROUP_CONCAT(DISTINCT ...)` needs the rendered
/// value and its per-argument collation key separately.
pub struct AggInput {
    pub value: Option<Datum>,
    pub distinct_key: Option<Vec<u8>>,
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

/// Go window processors retain partial results while advancing a frame.
/// COUNT and BIT_XOR support inverse updates, SUM/AVG follow Go's precision
/// policy, and supported MIN/MAX types retain a monotonic deque. Aggregates
/// without a sliding implementation retain the complete-frame evaluator.
pub struct WindowAggState {
    pub state: AggState,
    pub input: AggInputMode,
    pub previous: Option<(usize, usize)>,
    pub numeric: Option<window_numeric::NumericWindowState>,
    pub extremum: Option<window_extremum::ExtremumWindowState>,
}

impl WindowAggState {
    pub fn new(func: &AggFunc) -> Self {
        Self {
            state: AggState::new(func),
            input: AggInputMode::new(func),
            previous: None,
            numeric: None,
            extremum: None,
        }
    }

    pub fn reset(&mut self, func: &AggFunc) {
        self.state.reset(func);
        self.previous = None;
        self.numeric = None;
        self.extremum = None;
    }

    pub fn value<C: Columns>(
        &mut self,
        func: &AggFunc,
        ctx: &C,
        chunk: &impl crate::frame_rows::FrameRows,
        start: usize,
        end: usize,
        output_type: &tidb_datatype::FieldType,
    ) -> Result<Datum, ExecError> {
        self.update_frame(func, ctx, chunk, start, end, output_type)?;
        self.finish(func, ctx, output_type)
    }

    pub fn update_frame<C: Columns>(
        &mut self,
        func: &AggFunc,
        ctx: &C,
        chunk: &impl crate::frame_rows::FrameRows,
        start: usize,
        end: usize,
        output_type: &tidb_datatype::FieldType,
    ) -> Result<(), ExecError> {
        if matches!(func.kind, AggKind::Min | AggKind::Max)
            && window_extremum::supports(output_type)
        {
            return self
                .extremum
                .get_or_insert_with(Default::default)
                .update_frame(func, ctx, chunk, start, end, output_type);
        }
        if matches!(func.kind, AggKind::Sum | AggKind::Avg)
            && !func.distinct
            && func.extra_args.is_empty()
        {
            return self
                .numeric
                .get_or_insert_with(|| window_numeric::NumericWindowState::new(output_type))
                .update_frame(func, ctx, chunk, start, end, output_type);
        }
        let input = self.input;
        let fold = |state: &mut AggState, start, end| {
            chunk.visit_chunks(start..end, |chunk, range| {
                let bound = input.bind(chunk);
                for index in range {
                    bound.update(func, ctx, state, chunk.get_row(index))?;
                }
                Ok(())
            })
        };
        let sliding_inverse = matches!(func.kind, AggKind::Count | AggKind::Bit(BitOp::Xor))
            && !func.distinct
            && func.extra_args.is_empty();
        if let Some((last_start, last_end)) = self.previous.filter(|_| sliding_inverse) {
            // countOriginal* and bitXorUint64 evaluate departures before arrivals.
            // Use the same typed evaluator for both, including expression errors
            // and NULL handling. Disjoint frames still visit the gap twice,
            // just as Go's two loops do.
            let mut departing = AggState::new(func);
            fold(
                &mut departing,
                last_start,
                chunk.sliding_end(last_start, start),
            )?;
            chunk.check_sliding_end(last_start, start)?;
            match (&mut self.state.partial, departing.partial) {
                (Partial::Count(count), Partial::Count(removed)) => {
                    *count = count.wrapping_sub(removed);
                }
                (Partial::Bit { acc, .. }, Partial::Bit { acc: removed, .. }) => {
                    *acc ^= removed;
                }
                _ => unreachable!("inverse window state is COUNT or BIT_XOR"),
            }
            fold(&mut self.state, last_end, chunk.sliding_end(last_end, end))?;
            chunk.check_sliding_end(last_end, end)?;
        } else {
            self.state.reset(func);
            fold(&mut self.state, start, end)?;
        }
        // Go initializes sliding state only after a nonempty frame.
        if start < end || self.previous.is_some() {
            self.previous = Some((start, end));
        }
        Ok(())
    }

    pub fn finish<C: Columns>(
        &mut self,
        func: &AggFunc,
        ctx: &C,
        output_type: &FieldType,
    ) -> Result<Datum, ExecError> {
        if let Some(state) = &mut self.numeric {
            return state.finish(func, ctx, output_type);
        }
        if let Some(state) = &self.extremum {
            return state.finish(output_type);
        }
        let mut truncated = false;
        finish_agg_value(&mut self.state, func, output_type, ctx, &mut truncated)
    }
}

/// One group's partial results, in agg-func order.
pub enum Partial {
    Count(i64),
    /// `COUNT(DISTINCT x)` over one integer argument: Go's
    /// `partialResult4CountDistinctInt`, the values themselves in an
    /// `Int64Set` (`baseCountDistinct4Int`: `EvalInt`, skip NULL, insert;
    /// merged by set union, finished as the set's size). Every other DISTINCT
    /// keeps the encoded-key set on the state and, in the parallel partial
    /// phase, its retained inputs.
    CountDistinctInt(Int64SetWithMemoryUsage),
    /// `COUNT(DISTINCT x)` over one real argument: Go's
    /// `partialResult4CountDistinctReal`, backed by a float64 map. The set
    /// preserves Go's signed-zero aliasing and non-equal NaN keys.
    CountDistinctReal(Float64SetWithMemoryUsage),
    /// `COUNT(DISTINCT x)` over one string argument: Go's
    /// `partialResult4CountDistinctString`, retaining only the collation key
    /// set and merging it by set union in final workers.
    CountDistinctString(StringSetWithMemoryUsage),
    /// `COUNT(DISTINCT x)` over one DECIMAL argument: Go's
    /// `partialResult4CountDistinctDecimal`, keyed by `MyDecimal.ToHashKey`.
    CountDistinctDecimal(StringSetWithMemoryUsage),
    /// `COUNT(DISTINCT x)` over one duration argument: Go's
    /// `partialResult4CountDistinctDuration`, keyed by the raw duration bits.
    CountDistinctDuration(Int64SetWithMemoryUsage),
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
    /// Fixed-scale DECIMAL `MIN`/`MAX`: Go's `partialResult4MaxMinDecimal`
    /// keeps the extremum as a `MyDecimal` value in place, compared by
    /// `Compare` and appended with `AppendMyDecimal`. The signed i128
    /// coefficient plays that role here: one comparison per row instead of
    /// a `Decimal` build (its digit string) and a datum compare, and no heap
    /// object per group. A differing-scale or non-i128 input materializes
    /// the state into [`Partial::MaxMin`] via [`Partial::materialize_max_min_fast`].
    MaxMinDecimalFast {
        value: i128,
        scale: u32,
        is_max: bool,
    },
    /// `MIN_COUNT`/`MAX_COUNT`: the selected extreme plus the number of rows
    /// tied at that extreme. The value is retained for partial merges while
    /// finalization returns only the count.
    MaxMinCount {
        value: Option<Datum>,
        count: i64,
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
        /// A DISTINCT aggregate: Go's `partialResult4VarPopDistinctFloat64`
        /// keeps the distinct values and computes the variance in two passes
        /// at the end instead of folding incrementally.
        distinct: Option<Box<DistinctVariance>>,
    },
    /// `JSON_ARRAYAGG`: the converted values in arrival order, plus the
    /// value argument's field type for `Opaque` type-code tagging.
    /// The field type is boxed: it carries strings, and this state is one of
    /// many per group.
    JsonArrayAgg(Vec<BinaryJSON>, Box<FieldType>),
    /// `JSON_OBJECTAGG`: the object built so far, the value argument's field
    /// type, and whether the key argument is BINARY-charset. A `BTreeMap`
    /// both keeps the last write per key (Go's map overwrite) and hands the
    /// encoder the bytewise-sorted key order it needs.
    JsonObjectAgg(BTreeMap<String, BinaryJSON>, Box<FieldType>, bool),
    /// `APPROX_COUNT_DISTINCT`: the BJKST sketch folding the group's encoded
    /// argument tuples (or serialized partial sketches), and its signature.
    ApproxCountDistinct(ApproxCountDistinctSketch, ApproxCountDistinctSig),
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
/// Go `calculateDistinctFloat64Variance`'s input: the distinct values of one
/// group (`set.Float64SetWithMemoryUsage`, equal by value, so `-0` and `0`
/// are one) in arrival order.
#[derive(Default)]
pub struct DistinctVariance {
    pub values: Vec<f64>,
    pub seen: std::collections::HashSet<u64>,
}

impl DistinctVariance {
    pub fn insert(&mut self, value: f64) {
        // `-0.0 == 0.0` in Go's float64 map key.
        let bits = if value == 0.0 { 0 } else { value.to_bits() };
        if self.seen.insert(bits) {
            self.values.push(value);
        }
    }

    /// Go `calculateDistinctFloat64Variance`: the count and the sum of the
    /// squared deviations from the mean, in two passes.
    fn two_pass(&self) -> (i64, f64) {
        let count = self.values.len() as i64;
        if count == 0 {
            return (0, 0.0);
        }
        let mut sum = 0.0;
        for value in &self.values {
            sum += value;
        }
        let mean = sum / count as f64;
        let mut variance = 0.0;
        for value in &self.values {
            let diff = value - mean;
            variance += diff * diff;
        }
        (count, variance)
    }
}

pub struct AggState {
    pub partial: Partial,
    /// Boxed: only a DISTINCT aggregate owns a set, and every other state
    /// stays small enough that a group's states share cache lines (Go's
    /// partial results are exactly as large as their value).
    pub seen: Option<Box<StringSetWithMemoryUsage>>,
    /// Original inputs retained only by the parallel partial phase for a
    /// DISTINCT aggregate. Go's distinct partial results retain the values
    /// themselves (not merely the folded scalar) so final workers can union
    /// worker-local sets before producing COUNT/SUM/AVG/GROUP_CONCAT. The
    /// serial path needs only `seen` and leaves this allocation absent.
    pub distinct_inputs: Option<Vec<DistinctInput>>,
    pub collation: tidb_charset::Collation,
}

pub struct DistinctInput {
    pub key: Vec<u8>,
    pub value: Option<Datum>,
    pub extra: Vec<Datum>,
    pub sort_key: Vec<Datum>,
}

/// The fixed-scale coefficient a SUM accumulator can be seeded with, or
/// `None` for an input that must keep the exact `Decimal` path.
///
/// The scales here are the ones [`Partial::SumDecimalFast`] already folds and
/// finalizes: an integer contributes at scale 0, and a decimal only when its
/// visible scale is its storage scale, so rebuilding the result from the
/// coefficient cannot publish hidden division digits.
pub fn sum_seed_coefficient(input: &Datum) -> Option<(i128, u32)> {
    match input {
        Datum::Int(v) => Some((i128::from(*v), 0)),
        Datum::UInt(v) => i128::try_from(*v).ok().map(|value| (value, 0)),
        Datum::Decimal(d) => d.fold_coefficient_i128(),
        _ => None,
    }
}

/// The collation an aggregate's state works under: its argument's derived
/// collation (Go `aggfuncs/builder.go` reads it off `AggFuncDesc.RetTp`
/// once when the function is built).
pub fn state_collation(func: &AggFunc) -> tidb_charset::Collation {
    func.arg
        .as_ref()
        .map_or(tidb_charset::Collation::DEFAULT, expr_collation)
}

/// Go `buildCount`: a single-argument `COUNT(DISTINCT)` over an integer
/// (`types.ETInt`) argument is `countOriginalWithDistinct4Int`, whose partial
/// result is the typed value set rather than an encoded-key set.
pub fn count_distinct_int(func: &AggFunc) -> bool {
    matches!(func.kind, AggKind::Count)
        && func.distinct
        && func.extra_args.is_empty()
        && func.order_by.is_empty()
        && func
            .arg
            .as_ref()
            .and_then(Expression::static_type)
            .is_some_and(|ty| ty.eval_type() == EvalType::Int)
}

/// Go `buildCount` selects `countOriginalWithDistinct4Real` for one REAL
/// argument. Keep the plan predicate separate from the integer variant so the
/// partial state can use Go's float64 map equality instead of encoded datums.
pub fn count_distinct_real(func: &AggFunc) -> bool {
    matches!(func.kind, AggKind::Count)
        && func.distinct
        && func.extra_args.is_empty()
        && func.order_by.is_empty()
        && func
            .arg
            .as_ref()
            .and_then(Expression::static_type)
            .is_some_and(|ty| ty.eval_type() == EvalType::Real)
}

/// Go `buildCount` selects the dedicated string set for one ETString
/// argument. Hybrid types (ENUM/SET without the integer flag) still evaluate
/// through the expression path, but their resulting string key uses the same
/// set semantics.
pub fn count_distinct_string(func: &AggFunc) -> bool {
    matches!(func.kind, AggKind::Count)
        && func.distinct
        && func.extra_args.is_empty()
        && func.order_by.is_empty()
        && func
            .arg
            .as_ref()
            .and_then(Expression::static_type)
            .is_some_and(|ty| ty.eval_type() == EvalType::String)
}

/// Go `buildCount` selects the dedicated decimal set for one ETDecimal
/// argument. The direct-column fast path additionally requires NewDecimal,
/// while expression evaluation remains responsible for any compatible value
/// representation.
pub fn count_distinct_decimal(func: &AggFunc) -> bool {
    matches!(func.kind, AggKind::Count)
        && func.distinct
        && func.extra_args.is_empty()
        && func.order_by.is_empty()
        && func
            .arg
            .as_ref()
            .and_then(Expression::static_type)
            .is_some_and(|ty| {
                ty.eval_type() == EvalType::Decimal && ty.code() == FieldTypeCode::NewDecimal
            })
}

/// Go `buildCount` selects the dedicated duration set for one ETDuration
/// argument.
pub fn count_distinct_duration(func: &AggFunc) -> bool {
    matches!(func.kind, AggKind::Count)
        && func.distinct
        && func.extra_args.is_empty()
        && func.order_by.is_empty()
        && func
            .arg
            .as_ref()
            .and_then(Expression::static_type)
            .is_some_and(|ty| ty.eval_type() == EvalType::Duration)
}

pub fn count_distinct_typed(func: &AggFunc) -> bool {
    count_distinct_int(func)
        || count_distinct_real(func)
        || count_distinct_string(func)
        || count_distinct_decimal(func)
        || count_distinct_duration(func)
}

impl AggState {
    pub fn new(func: &AggFunc) -> AggState {
        Self::with_collation(func, state_collation(func))
    }

    /// `new` with the collation already derived, for the executors that
    /// open a state per group and derive each function's collation once.
    pub fn with_collation(func: &AggFunc, collation: tidb_charset::Collation) -> AggState {
        AggState {
            partial: Partial::for_func(func),
            seen: (func.distinct && !count_distinct_typed(func))
                .then(|| Box::new(StringSetWithMemoryUsage::new([]).0)),
            distinct_inputs: None,
            collation,
        }
    }

    /// Go `ResetPartialResult`: the state is reused for the next group
    /// instead of being reallocated (the collation is derived once).
    pub fn reset(&mut self, func: &AggFunc) {
        self.partial = Partial::for_func(func);
        if self.seen.is_some() {
            self.seen = Some(Box::new(StringSetWithMemoryUsage::new([]).0));
        }
        if let Some(inputs) = &mut self.distinct_inputs {
            inputs.clear();
        }
    }

    /// Go's parallel partial aggregate retains each DISTINCT input so final
    /// workers can merge sets rather than adding already-folded scalars.
    pub fn new_parallel(func: &AggFunc) -> AggState {
        Self::new_parallel_with(func, state_collation(func))
    }

    pub fn new_parallel_with(func: &AggFunc, collation: tidb_charset::Collation) -> AggState {
        let mut state = Self::with_collation(func, collation);
        state.distinct_inputs = (func.distinct && !count_distinct_typed(func)).then(Vec::new);
        state
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
    pub fn update(
        &mut self,
        value: Option<Datum>,
        extra: &[Datum],
        sort_key: Vec<Datum>,
        distinct_key: Option<Vec<u8>>,
    ) -> Result<i64, ExecError> {
        let mut delta: i64 = 0;
        if let Partial::CountDistinctInt(set) = &mut self.partial {
            // Go `baseCountDistinct4Int.UpdatePartialResult`: `EvalInt`, skip
            // NULL, insert (an unsigned value contributes its bits).
            match value {
                Some(Datum::Int(value)) => delta += set.insert(value),
                Some(Datum::UInt(value)) => delta += set.insert(value as i64),
                Some(Datum::Null) => {}
                None => return Err(ExecError::unsupported("COUNT requires an argument")),
                Some(other) => {
                    return Err(ExecError::unsupported(format!(
                        "COUNT(DISTINCT) over {other:?} is not an integer"
                    )))
                }
            }
            return Ok(delta);
        }
        if matches!(self.partial, Partial::CountDistinctReal(_)) {
            match value {
                Some(Datum::Real(value)) | Some(Datum::Float32(value)) => {
                    if let Partial::CountDistinctReal(set) = &mut self.partial {
                        set.insert(value);
                    }
                }
                Some(Datum::Null) => {}
                None => return Err(ExecError::unsupported("COUNT requires an argument")),
                Some(other) => {
                    return Err(ExecError::unsupported(format!(
                        "COUNT(DISTINCT) over {other:?} is not real"
                    )))
                }
            }
            return Ok(0);
        }
        if matches!(self.partial, Partial::CountDistinctString(_)) {
            match value {
                Some(Datum::String(value)) => {
                    let key = self.collation.key(value.bytes());
                    if let Partial::CountDistinctString(set) = &mut self.partial {
                        let key_bytes = i64::try_from(key.len()).unwrap_or(i64::MAX);
                        let key = GoString::from_bytes(key);
                        if !set.contains(&key) {
                            delta += set.insert(key) + key_bytes;
                        }
                    }
                }
                Some(Datum::Bytes(value)) => {
                    let key = self.collation.key(&value);
                    if let Partial::CountDistinctString(set) = &mut self.partial {
                        let key_bytes = i64::try_from(key.len()).unwrap_or(i64::MAX);
                        let key = GoString::from_bytes(key);
                        if !set.contains(&key) {
                            delta += set.insert(key) + key_bytes;
                        }
                    }
                }
                Some(Datum::Null) => {}
                None => return Err(ExecError::unsupported("COUNT requires an argument")),
                Some(other) => {
                    return Err(ExecError::unsupported(format!(
                        "COUNT(DISTINCT) over {other:?} is not string"
                    )))
                }
            }
            return Ok(delta);
        }
        if matches!(self.partial, Partial::CountDistinctDecimal(_)) {
            match value {
                Some(Datum::Decimal(value)) => {
                    let key = value
                        .to_hash_key()
                        .map_err(|_| ExecError::unsupported("COUNT(DISTINCT) over decimal"))?
                        .0;
                    if let Partial::CountDistinctDecimal(set) = &mut self.partial {
                        let key_bytes = i64::try_from(key.len()).unwrap_or(i64::MAX);
                        let key = GoString::from_bytes(key);
                        if !set.contains(&key) {
                            delta += set.insert(key) + key_bytes;
                        }
                    }
                }
                Some(Datum::Null) => {}
                None => return Err(ExecError::unsupported("COUNT requires an argument")),
                Some(other) => {
                    return Err(ExecError::unsupported(format!(
                        "COUNT(DISTINCT) over {other:?} is not decimal"
                    )))
                }
            }
            return Ok(delta);
        }
        if matches!(self.partial, Partial::CountDistinctDuration(_)) {
            match value {
                Some(Datum::Duration(value)) => {
                    if let Partial::CountDistinctDuration(set) = &mut self.partial {
                        delta += set.insert(value.nanoseconds());
                    }
                }
                Some(Datum::Null) => {}
                None => return Err(ExecError::unsupported("COUNT requires an argument")),
                Some(other) => {
                    return Err(ExecError::unsupported(format!(
                        "COUNT(DISTINCT) over {other:?} is not duration"
                    )))
                }
            }
            return Ok(delta);
        }
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
                let key = GoString::from_bytes(key);
                if seen.contains(&key) {
                    return Ok(0);
                }
                let retained_key = self
                    .distinct_inputs
                    .is_some()
                    .then(|| key.as_bytes().to_vec());
                let map_delta = seen.insert(key);
                delta += key_bytes + map_delta;
                if let Some(inputs) = &mut self.distinct_inputs {
                    inputs.push(DistinctInput {
                        key: retained_key.expect("parallel DISTINCT retains its key"),
                        value: value.clone(),
                        extra: extra.to_vec(),
                        sort_key: sort_key.clone(),
                    });
                }
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
        if matches!(
            self.partial,
            Partial::MaxMin { .. } | Partial::MaxMinDecimalFast { .. }
        ) {
            return self.update_decimal_max_min_fast(coefficient, scale);
        }
        self.partial.update_with_coefficient(coefficient, scale)
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
    /// Go `baseCountDistinct4Int.UpdatePartialResult` on a column cell read
    /// straight from the chunk (`None` for a NULL cell). Answers the bytes
    /// the set grew by, or `None` when this state is not the typed set.
    fn update_count_distinct_int_fast(&mut self, value: Option<i64>) -> Option<i64> {
        let Partial::CountDistinctInt(set) = &mut self.partial else {
            return None;
        };
        Some(value.map_or(0, |value| set.insert(value)))
    }

    /// Go `baseCountDistinct4Real.UpdatePartialResult` on a REAL cell read
    /// straight from the chunk. A float set intentionally treats every NaN
    /// insertion as a new key, just like Go's `map[float64]`.
    fn update_count_distinct_real_fast(&mut self, value: Option<f64>) -> Option<i64> {
        let Partial::CountDistinctReal(set) = &mut self.partial else {
            return None;
        };
        Some(value.map_or(0, |value| set.insert(value)))
    }

    /// Go `baseCountDistinct4String.UpdatePartialResult` on a directly read
    /// column key. The caller has already applied the aggregate argument's
    /// collation, so the state only needs to insert the owned bytes.
    fn update_count_distinct_string_fast(&mut self, key: Vec<u8>) -> Option<i64> {
        let Partial::CountDistinctString(set) = &mut self.partial else {
            return None;
        };
        let key_bytes = i64::try_from(key.len()).unwrap_or(i64::MAX);
        Some(set.insert(GoString::from_bytes(key)) + key_bytes)
    }

    /// Go `baseCountDistinct4Decimal.UpdatePartialResult` on a directly read
    /// `MyDecimal.ToHashKey` value.
    fn update_count_distinct_decimal_fast(&mut self, key: Vec<u8>) -> Option<i64> {
        let Partial::CountDistinctDecimal(set) = &mut self.partial else {
            return None;
        };
        let key_bytes = i64::try_from(key.len()).unwrap_or(i64::MAX);
        Some(set.insert(GoString::from_bytes(key)) + key_bytes)
    }

    /// Go `baseCountDistinct4Duration.UpdatePartialResult` on the raw duration
    /// value. Keeping the primitive value avoids constructing an encoded key
    /// and a retained datum for every row.
    fn update_count_distinct_duration_fast(&mut self, value: Option<i64>) -> Option<i64> {
        let Partial::CountDistinctDuration(set) = &mut self.partial else {
            return None;
        };
        Some(value.map_or(0, |value| set.insert(value)))
    }

    /// Go `countPartial4Int.UpdatePartialResult`: a FINAL `COUNT` adds the
    /// partial count it is handed, read straight from its integer column.
    ///
    /// Returns `false` when this state cannot take the value (a DISTINCT set,
    /// or a partial count past `i64`) and the caller must use the complete
    /// path.
    fn update_final_count_fast(&mut self, value: i64) -> bool {
        if self.seen.is_some() {
            return false;
        }
        let Partial::FinalCount(total) = &mut self.partial else {
            return false;
        };
        let Some(sum) = total.checked_add(value) else {
            return false;
        };
        *total = sum;
        true
    }

    /// Folds a fixed-width integer into Go's typed `maxMin4Int`/
    /// `maxMin4Uint` state without materializing a [`Datum`] for every row.
    /// Returning `false` leaves the exact datum path responsible for any
    /// representation that does not match the planned signedness.
    fn update_integer_fast(&mut self, value: i64, unsigned: bool, is_max: bool) -> bool {
        if self.seen.is_some() {
            return false;
        }
        let Partial::MaxMin {
            value: current,
            is_max: state_is_max,
        } = &mut self.partial
        else {
            return false;
        };
        if *state_is_max != is_max {
            return false;
        }
        if unsigned {
            let value = value as u64;
            match current {
                None => *current = Some(Datum::UInt(value)),
                Some(Datum::UInt(current)) => {
                    let improves = if is_max {
                        value > *current
                    } else {
                        value < *current
                    };
                    if improves {
                        *current = value;
                    }
                }
                Some(_) => return false,
            }
        } else {
            match current {
                None => *current = Some(Datum::Int(value)),
                Some(Datum::Int(current)) => {
                    let improves = if is_max {
                        value > *current
                    } else {
                        value < *current
                    };
                    if improves {
                        *current = value;
                    }
                }
                Some(_) => return false,
            }
        }
        true
    }

    /// Folds a direct REAL cell into Go's `sum4Float64` state without
    /// materializing a [`Datum`] or evaluating the expression tree. The lazy
    /// `SumDecimal(None)` seed is the initial representation for every SUM;
    /// a REAL input switches it to the floating-point domain on the first
    /// non-NULL row, exactly as `calculateSum` does.
    fn update_sum_real_fast(&mut self, value: f64) -> bool {
        if self.seen.is_some() {
            return false;
        }
        match &mut self.partial {
            Partial::SumDecimal(None) => {
                self.partial = Partial::SumReal(Some(value));
                true
            }
            Partial::SumReal(sum) => {
                *sum = Some(sum.unwrap_or(0.0) + value);
                true
            }
            _ => false,
        }
    }

    /// Folds a direct REAL cell into Go's `avgOriginal4Float64` state. The
    /// first row changes the lazy decimal seed to the REAL accumulator; later
    /// rows stay in primitive `f64`/count storage.
    fn update_avg_real_fast(&mut self, value: f64) -> bool {
        if self.seen.is_some() {
            return false;
        }
        match &mut self.partial {
            Partial::AvgDecimal { count: 0, .. } => {
                self.partial = Partial::AvgReal {
                    sum: value,
                    count: 1,
                };
                true
            }
            Partial::AvgReal { sum, count } => {
                *sum += value;
                *count += 1;
                true
            }
            _ => false,
        }
    }

    /// Folds a direct FLOAT/DOUBLE cell into Go's typed `maxMin4Float32`/
    /// `maxMin4Float64` state. The datum is materialized only for the first
    /// value or when an extremum improves, preserving the result's original
    /// FLOAT-versus-DOUBLE representation.
    fn update_real_fast(&mut self, value: f64, float32: bool, is_max: bool) -> bool {
        if self.seen.is_some() {
            return false;
        }
        let Partial::MaxMin {
            value: current,
            is_max: state_is_max,
        } = &mut self.partial
        else {
            return false;
        };
        if *state_is_max != is_max {
            return false;
        }
        match current {
            None => {
                *current = Some(if float32 {
                    Datum::Float32(value)
                } else {
                    Datum::Real(value)
                });
            }
            Some(Datum::Float32(current)) if float32 => {
                let improves = if is_max {
                    value > *current
                } else {
                    value < *current
                };
                if improves {
                    *current = value;
                }
            }
            Some(Datum::Real(current)) if !float32 => {
                let improves = if is_max {
                    value > *current
                } else {
                    value < *current
                };
                if improves {
                    *current = value;
                }
            }
            Some(_) => return false,
        }
        true
    }

    /// Folds a direct string cell into Go's `maxMin4String` state. The
    /// collation compares the borrowed column bytes; ownership is copied only
    /// for the first value or when the extremum changes, just as Go's
    /// `stringutil.Copy` path does.
    fn update_string_fast(
        &mut self,
        value: &[u8],
        collation: Collation,
        is_max: bool,
    ) -> Option<i64> {
        if self.seen.is_some() {
            return None;
        }
        let Partial::MaxMin {
            value: current,
            is_max: state_is_max,
        } = &mut self.partial
        else {
            return None;
        };
        if *state_is_max != is_max {
            return None;
        }
        match current {
            None => {
                *current = Some(Datum::Bytes(value.to_vec()));
                Some(value.len() as i64)
            }
            Some(current) => {
                let Some(current_bytes) = current.as_raw_bytes() else {
                    return None;
                };
                let ordering = collation.compare(value, current_bytes);
                let improves = (is_max && ordering == Ordering::Greater)
                    || (!is_max && ordering == Ordering::Less);
                if improves {
                    let old_len = current_bytes.len() as i64;
                    *current = Datum::Bytes(value.to_vec());
                    Some(value.len() as i64 - old_len)
                } else {
                    Some(0)
                }
            }
        }
    }

    /// Folds a direct DATE/DATETIME/TIMESTAMP cell into Go's `maxMin4Time`
    /// state. `Time` is copyable, so the hot loop performs only the packed
    /// decode and comparison and never evaluates the expression tree.
    fn update_time_fast(&mut self, value: tidb_datatype::Time, is_max: bool) -> bool {
        if self.seen.is_some() {
            return false;
        }
        let Partial::MaxMin {
            value: current,
            is_max: state_is_max,
        } = &mut self.partial
        else {
            return false;
        };
        if *state_is_max != is_max {
            return false;
        }
        match current {
            None => *current = Some(Datum::Time(value)),
            Some(Datum::Time(current)) => {
                let ordering = value.compare(*current);
                let improves = (is_max && ordering == Ordering::Greater)
                    || (!is_max && ordering == Ordering::Less);
                if improves {
                    *current = value;
                }
            }
            Some(_) => return false,
        }
        true
    }

    /// Folds a direct TIME cell into Go's `maxMin4Duration` state using the
    /// packed nanosecond value and preserving the column's fractional-second
    /// precision on the stored datum.
    fn update_duration_fast(&mut self, value: tidb_datatype::MySqlDuration, is_max: bool) -> bool {
        if self.seen.is_some() {
            return false;
        }
        let Partial::MaxMin {
            value: current,
            is_max: state_is_max,
        } = &mut self.partial
        else {
            return false;
        };
        if *state_is_max != is_max {
            return false;
        }
        match current {
            None => *current = Some(Datum::Duration(value)),
            Some(Datum::Duration(current)) => {
                let ordering = value.compare(*current);
                let improves = (is_max && ordering == Ordering::Greater)
                    || (!is_max && ordering == Ordering::Less);
                if improves {
                    *current = value;
                }
            }
            Some(_) => return false,
        }
        true
    }

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

    /// Folds a fixed-scale DECIMAL MIN/MAX cell into
    /// [`Partial::MaxMinDecimalFast`] (Go `maxMin4Decimal.UpdatePartialResult`:
    /// `input.Compare(&p.val)`, then `p.val = *input`). Returns `false` when
    /// this state cannot take the coefficient -- DISTINCT, a scale that
    /// differs from the accumulated one, or a state already materialized as
    /// a datum -- and the caller must replay the row through the complete
    /// path; a fast state is materialized first so that replay compares
    /// exact decimals.
    fn update_decimal_max_min_fast(&mut self, coefficient: i128, scale: u32) -> bool {
        if self.seen.is_some() {
            return false;
        }
        match &mut self.partial {
            Partial::MaxMin {
                value: None,
                is_max,
            } => {
                self.partial = Partial::MaxMinDecimalFast {
                    value: coefficient,
                    scale,
                    is_max: *is_max,
                };
                true
            }
            Partial::MaxMinDecimalFast {
                value,
                scale: current_scale,
                is_max,
            } if *current_scale == scale => {
                let improves = if *is_max {
                    coefficient > *value
                } else {
                    coefficient < *value
                };
                if improves {
                    *value = coefficient;
                }
                true
            }
            Partial::MaxMinDecimalFast { .. } => {
                self.partial.materialize_max_min_fast();
                false
            }
            _ => false,
        }
    }

    fn has_first_row(&self) -> bool {
        self.seen.is_none() && matches!(self.partial, Partial::FirstRow(Some(_)))
    }
}

/// The bytes one `GROUP_CONCAT` input contributes, which Go produces by
/// casting the argument to a string.
/// The derived collation of a group-by expression.
pub fn expr_collation(expr: &Expression) -> tidb_charset::Collation {
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
pub fn append_group_key_part(
    collation: &tidb_charset::Collation,
    datum: &Datum,
    output: &mut Vec<u8>,
) {
    match datum.as_raw_bytes() {
        Some(bytes) => {
            encode_compact_bytes(output, &collation.key(bytes));
        }
        None => output.extend_from_slice(&tidb_codec::hash_code(datum)),
    }
}

pub fn group_key_part(collation: &tidb_charset::Collation, datum: &Datum) -> Vec<u8> {
    let mut output = Vec::new();
    append_group_key_part(collation, datum, &mut output);
    output
}

/// Go `SessionVars.GroupConcatMaxLen` as this statement sees it.
pub fn group_concat_max_len<C: Columns>(ctx: &C) -> u64 {
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
pub fn group_concat_arg_text(func: &AggFunc) -> String {
    if func.arg_orig_name.is_empty() {
        match &func.arg {
            Some(Expression::Column(column)) => format!("Column#{}", column.unique_id),
            _ => "Column#0".to_owned(),
        }
    } else {
        func.arg_orig_name.clone()
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

/// The value Go's real aggregate implementations consume.
///
/// `WrapCastForAggArgs` chooses `EvalReal` whenever `SUM`/`AVG` inferred a
/// DOUBLE result. That includes character, temporal, and duration arguments,
/// not merely FLOAT columns. Keep the conversion at the aggregate boundary so
/// hash and stream aggregation use the same rule.
pub fn real_aggregate_value(value: &Datum, function: &'static str) -> Result<f64, ExecError> {
    value
        .to_f64()
        .map(|converted| converted.value)
        .map_err(|_| {
            ExecError::unsupported(format!(
                "{function} over this datum kind is not yet supported"
            ))
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

impl Partial {
    /// The initial state for `func`: [`Self::new`] for its kind, or the typed
    /// integer DISTINCT set (see [`count_distinct_int`]).
    fn for_func(func: &AggFunc) -> Partial {
        if count_distinct_int(func) {
            return Partial::CountDistinctInt(
                Int64SetWithMemoryUsage::new(std::iter::empty::<i64>()).0,
            );
        }
        if count_distinct_real(func) {
            return Partial::CountDistinctReal(
                Float64SetWithMemoryUsage::new(std::iter::empty::<f64>()).0,
            );
        }
        if count_distinct_string(func) {
            return Partial::CountDistinctString(
                StringSetWithMemoryUsage::new(std::iter::empty::<GoString>()).0,
            );
        }
        if count_distinct_decimal(func) {
            return Partial::CountDistinctDecimal(
                StringSetWithMemoryUsage::new(std::iter::empty::<GoString>()).0,
            );
        }
        if count_distinct_duration(func) {
            return Partial::CountDistinctDuration(
                Int64SetWithMemoryUsage::new(std::iter::empty::<i64>()).0,
            );
        }
        let mut partial = Partial::new(&func.kind);
        if func.distinct {
            if let Partial::Variance { distinct, .. } = &mut partial {
                *distinct = Some(Box::default());
            }
        }
        partial
    }

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
            AggKind::MinCount => Partial::MaxMinCount {
                value: None,
                count: 0,
                is_max: false,
            },
            AggKind::MaxCount => Partial::MaxMinCount {
                value: None,
                count: 0,
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
                distinct: None,
            },
            AggKind::JsonArrayAgg { value_type } => {
                Partial::JsonArrayAgg(Vec::new(), Box::new(value_type.clone()))
            }
            AggKind::JsonObjectAgg {
                value_type,
                key_is_binary,
            } => Partial::JsonObjectAgg(
                BTreeMap::new(),
                Box::new(value_type.clone()),
                *key_is_binary,
            ),
            AggKind::ApproxCountDistinct(sig) => {
                Partial::ApproxCountDistinct(ApproxCountDistinctSketch::new(), *sig)
            }
            AggKind::ApproxPercentile(percent) => Partial::ApproxPercentile {
                values: Vec::new(),
                percent: *percent,
            },
        }
    }

    pub fn materialize_sum_fast(&mut self) {
        if let Partial::SumDecimalFast { sum, scale } = self {
            *self = Partial::SumDecimal(Some(Decimal::from_scaled_i128(*sum, *scale)));
        }
    }

    /// Replaces a fixed-scale MIN/MAX accumulator by the exact decimal state
    /// every datum-path rule (compare, merge, spill) understands.
    pub fn materialize_max_min_fast(&mut self) {
        if let Partial::MaxMinDecimalFast {
            value,
            scale,
            is_max,
        } = self
        {
            *self = Partial::MaxMin {
                value: Some(Datum::Decimal(Decimal::from_scaled_i128(*value, *scale))),
                is_max: *is_max,
            };
        }
    }

    pub fn materialize_avg_fast(&mut self) {
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

    pub fn update(
        &mut self,
        value: Option<Datum>,
        extra: &[Datum],
        sort_key: Vec<Datum>,
        collation: tidb_charset::Collation,
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
        self.materialize_max_min_fast();
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
                // Evaluation-origin: Go's warning buffer stays EMPTY beside
                // the fatal 3158 (oracle g-group: `JSON_OBJECTAGG(NULL, 1)`).
                return Err(ExecError::Mysql(
                    tidb_terror::MysqlError::from_terror(
                        &tidb_datatype::types_errors::ERR_JSON_DOCUMENT_NULL_KEY
                            .generate_with_stack_by_args(&[]),
                    )
                    .from_evaluation(),
                ));
            }
            // A BINARY-charset key (Go: `e.args[0].GetType(sctx).GetCharset()
            // == charset.CharsetBin`) fails the statement with 3144 before
            // the value is even evaluated -- a STATIC property of the key
            // argument's declared type, checked here rather than by
            // inspecting the evaluated datum (see `key_is_binary`'s own doc
            // for why the datum kind alone cannot tell this apart from an
            // ordinary string literal).
            (Partial::JsonObjectAgg(_, _, true), Some(_)) => {
                return Err(ExecError::Mysql(
                    tidb_terror::MysqlError::from_terror(
                        &tidb_datatype::types_errors::ERR_INVALID_JSON_CHARSET
                            .generate_with_stack_by_args(&[("binary".to_owned()).into()]),
                    ),
                ));
            }
            (Partial::JsonObjectAgg(entries, value_type, false), Some(key)) => {
                let value = extra.first().cloned().unwrap_or(Datum::Null);
                entries.insert(json_object_key(&key)?, json_value(&value, value_type)?);
            }
            // A row with a NULL argument (or, for the multi-argument form, a
            // NULL in ANY argument, which the caller has already collapsed to
            // one NULL) never reaches the sketch.
            (Partial::ApproxCountDistinct(..), None) => {
                return Err(ExecError::unsupported(
                    "APPROX_COUNT_DISTINCT requires an argument",
                ));
            }
            (Partial::ApproxCountDistinct(..), Some(Datum::Null)) => {}
            // Go `approxCountDistinctPartial2.UpdatePartialResult`: the input
            // is a partial result `Serialize`d by a Partial1/Partial2 below.
            (Partial::ApproxCountDistinct(sketch, sig), Some(partial)) if sig.merge_input => {
                let bytes = partial.as_raw_bytes().ok_or_else(|| {
                    ExecError::unsupported("APPROX_COUNT_DISTINCT partial result is not a string")
                })?;
                sketch.read_and_merge(bytes).map_err(ExecError::internal)?;
            }
            // The caller (the group-fold loop below) has already encoded the
            // row's argument tuple the way Go's `evalAndEncode` does; this
            // just feeds those bytes through FarmHash into the sketch.
            (Partial::ApproxCountDistinct(sketch, _), Some(Datum::Bytes(encoded))) => {
                sketch.insert(&encoded);
            }
            (Partial::ApproxCountDistinct(..), Some(_)) => {
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
            // Go's `sum4Decimal` accumulates into a `MyDecimal` and adds its
            // base-1e9 words; the fixed-scale i128 coefficient plays that role
            // here, and [`Partial::SumDecimalFast`] already folds every later
            // row of the group that way. Only the SEEDING reached this arm, so
            // a SUM that got here built a `Decimal` per row and added it --
            // and a `Decimal`'s coefficient is an ASCII digit string, so that
            // add parsed one string and formatted another, per row.
            //
            // What gets here is an INTEGER sum. A decimal input already
            // carries a coefficient off `eval_agg_input`, so it seeds through
            // `update_with_coefficient` before this match; that is why this
            // arm does not move TPC-H, whose decimal aggregates are also
            // largely pushed to the coprocessor. `SUM` over a BIGINT has no
            // such coefficient and fell here for every row of the group.
            (this @ Partial::SumDecimal(None), Some(ref input))
                if sum_seed_coefficient(input).is_some() =>
            {
                // Recomputed rather than carried out of the guard: a match
                // guard cannot bind. It costs one extra coefficient read on
                // the FIRST row of a group only, and every decimal that
                // reaches here already failed to produce a coefficient
                // upstream (`eval_agg_input`), so in practice this re-reads an
                // integer.
                let (sum, scale) = sum_seed_coefficient(input).expect("checked by the guard");
                *this = Partial::SumDecimalFast { sum, scale };
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
            (Partial::MaxMinDecimalFast { .. }, _) => {
                return Err(ExecError::internal(
                    "a fixed-scale MIN/MAX state reached the datum fold unmaterialized",
                ));
            }
            (Partial::CountDistinctInt(_), _) => {
                return Err(ExecError::internal(
                    "a typed COUNT(DISTINCT) state is folded by its aggregate state",
                ));
            }
            (Partial::CountDistinctReal(_), None | Some(Datum::Null)) => {}
            (Partial::CountDistinctReal(set), Some(Datum::Real(value)))
            | (Partial::CountDistinctReal(set), Some(Datum::Float32(value))) => {
                set.insert(value);
            }
            (Partial::CountDistinctReal(_), Some(_)) => {
                return Err(ExecError::unsupported(
                    "COUNT(DISTINCT) over this real value is not supported",
                ));
            }
            (Partial::CountDistinctString(_), _) => {
                return Err(ExecError::internal(
                    "a typed COUNT(DISTINCT) string state is folded by its aggregate state",
                ));
            }
            (Partial::CountDistinctDecimal(_), _) => {
                return Err(ExecError::internal(
                    "a typed COUNT(DISTINCT) decimal state is folded by its aggregate state",
                ));
            }
            (Partial::CountDistinctDuration(_), _) => {
                return Err(ExecError::internal(
                    "a typed COUNT(DISTINCT) duration state is folded by its aggregate state",
                ));
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
                    let ordering = crate::compare_extreme(&input, current, collation)?;
                    if (*is_max && ordering == Ordering::Greater)
                        || (!*is_max && ordering == Ordering::Less)
                    {
                        *value = Some(input);
                    }
                }
            },
            (Partial::MaxMinCount { .. }, None) => {
                return Err(ExecError::unsupported(
                    "MIN_COUNT/MAX_COUNT requires an argument",
                ));
            }
            (Partial::MaxMinCount { .. }, Some(Datum::Null)) => {}
            (
                Partial::MaxMinCount {
                    value,
                    count,
                    is_max,
                },
                Some(input),
            ) => match value {
                None => {
                    *value = Some(input);
                    *count = 1;
                }
                Some(current) => {
                    let ordering =
                        tidb_expr::compare_datums_with_collation(&input, current, collation)?;
                    if (*is_max && ordering == Ordering::Greater)
                        || (!*is_max && ordering == Ordering::Less)
                    {
                        *current = input;
                        *count = 1;
                    } else if ordering == Ordering::Equal {
                        *count += 1;
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
                    distinct,
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
                if let Some(distinct) = distinct {
                    distinct.insert(value);
                    return Ok(());
                }
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
    /// bookkeeping. The DISTINCT value-set table is charged by
    /// [`StringSetWithMemoryUsage`];
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

    /// [`Partial::finish`] for a state that is about to be dropped: the
    /// retained value is MOVED into the result instead of copied.
    ///
    /// Go's `AppendFinalResult2Chunk` writes the partial result's own value
    /// into the output chunk -- `chk.AppendBytes(p.val)` -- because the
    /// partial result dies with the group and has nothing to protect. Here
    /// `finish` takes `&self` and had to clone, which on a group-by carrying
    /// wide text is the copy itself: finalizing TPC-H Q10, whose `FirstRow`
    /// partials hold `c_name`, `c_address` and `c_comment` (25, 40 and 117
    /// bytes a group), spent 3.0% of the node's samples in `Datum::clone`
    /// under this call, the single largest `memmove` caller in the query.
    ///
    /// Only a caller that owns the state and drops it may claim: a claimed
    /// state reads as an empty one. `FinalWorker::run` qualifies -- it takes each
    /// group by value out of the map and drops it at the end of the iteration
    /// -- and passes `claim: true` for that reason; every other finalize path
    /// passes `false` and keeps the copy.
    fn finish_claiming(
        &mut self,
        order_by: &[(Expression, bool)],
        div_precision_increment: u32,
    ) -> Result<Datum, ExecError> {
        match self {
            Partial::FirstRow(value) | Partial::MaxMin { value, .. } => {
                Ok(value.take().unwrap_or(Datum::Null))
            }
            Partial::SumDecimal(value) => Ok(value.take().map_or(Datum::Null, Datum::Decimal)),
            // Every other state either finishes into a fresh value already
            // (the counts, the fixed-scale coefficients) or needs the whole
            // collection to produce one.
            other => other.finish(order_by, div_precision_increment),
        }
    }

    pub fn finish(
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
            // Go `approxCountDistinctPartial1.AppendFinalResult2Chunk`
            // outputs the serialized sketch; Original/Final output the count.
            Partial::ApproxCountDistinct(sketch, sig) if sig.sketch_output => {
                Datum::Bytes(sketch.serialize())
            }
            Partial::ApproxCountDistinct(sketch, _) => Datum::Int(sketch.fixed_size() as i64),
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
                let index = select(&mut DatumSelection(&mut values), rank as isize);
                values[index as usize].clone()
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
                    let collations: Vec<tidb_charset::Collation> = order_by
                        .iter()
                        .map(|(expr, _)| expr_collation(expr))
                        .collect();
                    // Go `topNRows.Less` in ascending terms; a comparison
                    // error ends it as "not less" either way.
                    let compare = |left: &(Vec<u8>, Vec<Datum>), right: &(Vec<u8>, Vec<Datum>)| {
                        for (position, (_, desc)) in order_by.iter().enumerate() {
                            let (Some(a), Some(b)) = (left.1.get(position), right.1.get(position))
                            else {
                                continue;
                            };
                            let collation = collations
                                .get(position)
                                .copied()
                                .unwrap_or(tidb_charset::Collation::DEFAULT);
                            let Ok(ordering) =
                                tidb_expr::compare_datums_with_collation(a, b, collation)
                            else {
                                return Ordering::Equal;
                            };
                            if ordering != Ordering::Equal {
                                return if *desc { ordering.reverse() } else { ordering };
                            }
                        }
                        Ordering::Equal
                    };
                    // Go `topNRows.tryToAdd` pushes each row, in arrival
                    // order, onto a heap that keeps the greatest on top, and
                    // `concat` then runs `sort.Sort(sort.Reverse(h))` -- an
                    // unstable pdqsort over the heap's layout. Rows the ORDER
                    // BY ties (`abc`/`Abc` under general_ci) therefore come out
                    // in that schedule's order, not their arrival order.
                    let mut heap = Vec::with_capacity(values.len());
                    for value in values {
                        heap.push(value);
                        let last = heap.len() - 1;
                        tidb_goruntime::go_heap::up(&mut heap, last, &mut |a, b| {
                            compare(a, b) == Ordering::Greater
                        });
                    }
                    tidb_goruntime::go_pdqsort::go_sort_func_by(&mut heap, |a, b| compare(a, b));
                    values = heap;
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
            Partial::MaxMinDecimalFast { value, scale, .. } => {
                Datum::Decimal(Decimal::from_scaled_i128(*value, *scale))
            }
            Partial::CountDistinctInt(set) => {
                Datum::Int(i64::try_from(set.len()).unwrap_or(i64::MAX))
            }
            Partial::CountDistinctReal(set) => {
                Datum::Int(i64::try_from(set.len()).unwrap_or(i64::MAX))
            }
            Partial::CountDistinctString(set) | Partial::CountDistinctDecimal(set) => {
                Datum::Int(i64::try_from(set.len()).unwrap_or(i64::MAX))
            }
            Partial::CountDistinctDuration(set) => {
                Datum::Int(i64::try_from(set.len()).unwrap_or(i64::MAX))
            }
            Partial::MaxMinCount { count, .. } => Datum::Int(*count),
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
                distinct,
                ..
            } => {
                let (count, variance) = match distinct {
                    Some(distinct) => distinct.two_pass(),
                    None => (*count, *variance),
                };
                let (count, variance) = (&count, &variance);
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
/// Go `getRealJSONValue` (`aggfuncs/func_json_objectagg.go`), shared by
/// `JSON_ARRAYAGG` and `JSON_OBJECTAGG`: a binary literal, BIT or bytes datum
/// becomes an opaque value carrying the argument's MySQL type; so does a
/// binary-charset string, resized to `flen` for `BINARY(n)` (its trailing
/// zeros included); any other datum is its JSON value.
pub fn json_value(value: &Datum, value_type: &FieldType) -> Result<BinaryJSON, ExecError> {
    let bytes = match value {
        Datum::BinaryLiteral(literal) | Datum::Bit(literal) => Some(literal.as_bytes().to_vec()),
        Datum::Bytes(bytes) => Some(bytes.clone()),
        Datum::String(text) if value_type.charset_name() == "binary" => {
            let mut bytes = text.bytes().to_vec();
            if value_type.code() == tidb_datatype::FieldTypeCode::String {
                bytes.resize(usize::try_from(value_type.flen()).unwrap_or(0), 0);
            }
            Some(bytes)
        }
        _ => None,
    };
    let converted = match bytes {
        Some(bytes) => {
            BinaryJSON::from_typed_value(&BinaryJSONValue::Opaque(tidb_datatype::Opaque {
                type_code: value_type.code().mysql_type(),
                bytes,
            }))
        }
        None => value
            .to_mysql_json()
            .map_err(|_| tidb_datatype::BinaryJSONError::InvalidBinary),
    };
    converted.map_err(|_| ExecError::unsupported("this datum kind is not a JSON value"))
}

/// `JSON_OBJECTAGG`'s member name: Go reads the key argument with
/// `EvalString`, so a non-string key is stringified (`JSON_OBJECTAGG(id, v)`
/// keys the object with `"1"`, `"2"`, ...).
pub fn json_object_key(value: &Datum) -> Result<String, ExecError> {
    value
        .sql_string()
        .map_err(|_| ExecError::unsupported("this datum kind is not a JSON member name"))
}

/// Encodes a finished JSON aggregate, Go's `CreateBinaryJSONWithCheck`.
pub fn encode_json(value: BinaryJSONValue) -> Result<Datum, ExecError> {
    BinaryJSON::from_typed_value(&value)
        .map(Datum::Json)
        .map_err(|_| ExecError::unsupported("this JSON document cannot be encoded"))
}

/// The 64 bits one `BIT_AND`/`BIT_OR`/`BIT_XOR` input contributes: Go casts
/// the argument to `UNSIGNED BIGINT`, so a negative integer folds as its
/// two's-complement pattern.
pub fn datum_bits(value: &Datum) -> Result<u64, ExecError> {
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

/// Go `baseAvgDecimal.AppendFinalResult2Chunk` rounds `DecimalDiv`'s hidden
/// base-1e9 fraction words to the inferred AVG return scale before appending
/// the value. Keeping that step here makes HashAgg, StreamAgg, and window AVG
/// share the same result contract.
pub fn round_avg_result(kind: &AggKind, output_type: &FieldType, value: Datum) -> Datum {
    let Datum::Decimal(value) = value else {
        return value;
    };
    if matches!(kind, AggKind::Sum)
        && output_type.code() == FieldTypeCode::NewDecimal
        && output_type.decimal() != UNSPECIFIED_LENGTH
        && value.scale() > output_type.decimal() as u32
    {
        // Go `baseSumAggFunc.AppendFinalResult2Chunk` writes the accumulated
        // decimal through the SUM result's own FieldType, so the appended
        // value carries the inferred scale. Division feeds SUM full-precision
        // quotients on purpose (UnspecifiedLength into MyDecimal.Div), which
        // makes this final rounding the only place that scale becomes real --
        // without it a `SUM(a/b)` datum keeps 9+ fractional digits where Go
        // holds 6 (TPC-DS q66 comparisons).
        return Datum::Decimal(value.round_to_scale(output_type.decimal() as i32));
    }
    // Go `maxMin4Decimal.AppendFinalResult2Chunk` rounds the extremum to the
    // result scale (`MaxDecimalScale` when unspecified), half up, so a
    // division's hidden fraction digits never reach the client.
    if matches!(kind, AggKind::Min | AggKind::Max) {
        let scale = if output_type.decimal() == UNSPECIFIED_LENGTH {
            MAX_DECIMAL_SCALE
        } else {
            output_type.decimal()
        };
        return Datum::Decimal(value.round_to_scale(scale as i32));
    }
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
pub fn finish_agg_value<C: Columns>(
    state: &mut AggState,
    func: &AggFunc,
    output_type: &FieldType,
    ctx: &C,
    truncated: &mut bool,
) -> Result<Datum, ExecError> {
    finish_agg_value_claiming(
        state,
        func,
        output_type,
        ctx,
        &mut || {
            let first = !*truncated;
            *truncated = true;
            first
        },
        false,
    )
}

/// [`finish_agg_value`] with the GROUP_CONCAT "warn once" sentinel behind
/// `claim_truncation`, which returns whether this call is the first to
/// truncate. Go's `groupConcat` shares one `truncated *int32` across every
/// final worker and claims it with a compare-and-swap; the parallel
/// pipeline passes that atomic claim, the serial executors a plain flag.
pub fn finish_agg_value_claiming<C: Columns>(
    state: &mut AggState,
    func: &AggFunc,
    output_type: &FieldType,
    ctx: &C,
    claim_truncation: &mut dyn FnMut() -> bool,
    claim_value: bool,
) -> Result<Datum, ExecError> {
    let mut value = if claim_value {
        state
            .partial
            .finish_claiming(&func.order_by, ctx.div_precision_increment())?
    } else {
        state
            .partial
            .finish(&func.order_by, ctx.div_precision_increment())?
    };
    value = round_avg_result(&func.kind, output_type, value);
    if let Datum::Bytes(joined) = &mut value {
        if matches!(func.kind, AggKind::GroupConcat { .. }) {
            let max_len = group_concat_max_len(ctx);
            if max_len > 0 && joined.len() as u64 > max_len {
                joined.truncate(max_len as usize);
                if claim_truncation() {
                    let text = group_concat_arg_text(func);
                    tidb_expr::expression_errors::handle_group_concat_cut(ctx, &format!(
                        "Some rows were cut by GROUPCONCAT({text})"
                    ))?;
                }
            }
        }
    }
    Ok(value)
}

/// Finishes one aggregate state straight into column `column` of `req`.
///
/// A fixed-scale DECIMAL SUM appends its MyDecimal cell from the i128
/// accumulator (Go `sum4Decimal.AppendFinalResult2Chunk`:
/// `chk.AppendMyDecimal(&p.val)`), the cell `finish_agg_value` plus
/// `Chunk::append_datum` would build through the digit-string `Decimal`;
/// every other state takes that path. The SUM's own rounding to a narrower
/// output scale (`round_avg_result`) stays on the Datum path.
pub fn append_finished_agg_value_claiming<C: Columns>(
    state: &mut AggState,
    func: &AggFunc,
    output_type: &FieldType,
    ctx: &C,
    claim_truncation: &mut dyn FnMut() -> bool,
    claim_value: bool,
    req: &mut Chunk,
    column: usize,
) -> Result<(), ExecError> {
    let fixed_scale_cell = match &state.partial {
        Partial::SumDecimalFast { sum, scale } if matches!(func.kind, AggKind::Sum) => {
            Some((*sum, *scale))
        }
        // Go `maxMin4Decimal.AppendFinalResult2Chunk` rounds to the result
        // type's scale before `AppendMyDecimal`; a coefficient already at or
        // under that scale rounds to itself.
        Partial::MaxMinDecimalFast { value, scale, .. }
            if matches!(func.kind, AggKind::Min | AggKind::Max) =>
        {
            Some((*value, *scale))
        }
        _ => None,
    };
    if let Some((coefficient, scale)) = fixed_scale_cell {
        if output_type.code() == FieldTypeCode::NewDecimal
            && (output_type.decimal() == UNSPECIFIED_LENGTH
                || scale <= output_type.decimal() as u32)
        {
            if let Some(cell) =
                tidb_datatype::MyDecimal::from_scaled_i128(coefficient, scale, scale)
            {
                req.append_my_decimal(column, &cell);
                return Ok(());
            }
        }
    }
    let value =
        finish_agg_value_claiming(state, func, output_type, ctx, claim_truncation, claim_value)?;
    req.append_datum(column, &value);
    Ok(())
}

/// [`append_finished_agg_value_claiming`] with a plain truncation flag.
pub fn append_finished_agg_value<C: Columns>(
    state: &mut AggState,
    func: &AggFunc,
    output_type: &FieldType,
    ctx: &C,
    truncated: &mut bool,
    req: &mut Chunk,
    column: usize,
) -> Result<(), ExecError> {
    append_finished_agg_value_claiming(
        state,
        func,
        output_type,
        ctx,
        &mut || {
            let first = !*truncated;
            *truncated = true;
            first
        },
        false,
        req,
        column,
    )
}

/// The value one aggregate function takes from `row`, with any EXTRA argument
/// values it keeps separate. Go evaluates these inside
/// `UpdatePartialResult`, per function; this port evaluates them once at the
/// call site and hands the result to [`AggState::update`].
pub fn eval_agg_input<C: Columns>(
    f: &AggFunc,
    ctx: &C,
    row: tidb_chunk::row::Row<'_>,
    extra_values: &mut Vec<Datum>,
) -> Result<AggInput, ExecError> {
    // Go `countOriginalWithDistinct` serves every COUNT(DISTINCT) the typed
    // single-argument sets do not; it goes through the tuple arm below.
    let count_distinct_encoded =
        matches!(f.kind, AggKind::Count) && f.distinct && !count_distinct_typed(f);
    let (value, distinct_key) = if f.extra_args.is_empty()
        && !count_distinct_encoded
        && !matches!(
            f.kind,
            AggKind::ApproxCountDistinct(ApproxCountDistinctSig {
                merge_input: false,
                ..
            }) | AggKind::GroupConcat { .. }
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
        // `COUNT(a, b, ...)` / `COUNT(DISTINCT a, b, ...)`: Go's
        // `count4MultiArgs` and `countOriginalWithDistinct` skip the row as
        // soon as ANY argument is NULL (a row counts only when EVERY
        // argument is non-NULL). DISTINCT keys the row by `evalAndEncode` of
        // every argument, appended with no separator, so JSON `3` and `3.0`
        // collide through `HashValue` exactly as in Go.
        let mut tuple_key = Some(Vec::new());
        for expr in f.arg.iter().chain(f.extra_args.iter()) {
            let datum = expr.eval(ctx, row)?;
            if datum == Datum::Null {
                tuple_key = None;
                break;
            }
            if f.distinct {
                if let Some(buf) = &mut tuple_key {
                    buf.extend_from_slice(&approx_count_distinct_encode(
                        &datum,
                        expr_collation(expr),
                    )?);
                }
            }
        }
        let distinct_key = tuple_key.clone().filter(|_| f.distinct);
        (
            Some(tuple_key.map_or(Datum::Null, Datum::Bytes)),
            distinct_key,
        )
    } else if matches!(f.kind, AggKind::ApproxCountDistinct(_)) {
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
                buf.extend_from_slice(&approx_count_distinct_encode(&datum, expr_collation(expr))?);
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
    })
}
