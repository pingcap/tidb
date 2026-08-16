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

//! Canonical signed partial state shared by every typed `COUNT` evaluator,
//! plus the EXACT-DISTINCT half of Go
//! `pkg/executor/aggfuncs/func_count_distinct.go`.
//!
//! LABELING. This is a COMPLETE port of ONE HALF of ONE FILE of the large
//! `pkg/executor/aggfuncs` package -- it is therefore SEED evidence for that
//! package, not a transcreated package claim. The half ported here is the
//! exact-set `COUNT(DISTINCT ...)` machinery, Go lines 33-621: the
//! `DefPartialResult4*` size block, `partialResult4CountDistinct{Int,Real,
//! Decimal,Duration,String}`, `partialResult4CountWithDistinct`, their
//! `baseCountDistinct4*` update/reset/finalize bodies, the
//! `countPartialWithDistinct*` merges, `evalAndEncode`, `appendInt64`,
//! `appendFloat64`, `appendDecimal`, `WriteTime`, `appendTime`, and
//! `appendDuration`.
//!
//! The OTHER half of that same file -- `intHash64`, the
//! `uniquesHash*` constants, `partialResult4ApproxCountDistinct`, and the
//! whole BJKST `APPROX_COUNT_DISTINCT` sketch (Go lines 622-1007) -- is
//! ALREADY ported at `tidb-executor`'s `approx_count_distinct` module and is
//! deliberately NOT duplicated here. `dgryski/go-farm` is likewise already
//! ported as `tidb-executor`'s `farmhash`; the exact half needs no hashing at
//! all (only `APPROX_COUNT_DISTINCT` calls `farm.Hash64`), so no new
//! dependency edge is introduced.
//!
//! Boundaries and narrowings, each named:
//! - Go's states are driven by `UpdatePartialResult(sctx, rowsInGroup,
//!   PartialResult)`, which calls `e.args[i].EvalInt/EvalReal/...` over
//!   `chunk.Row`s. This tier owns no `AggFunc` descriptor, so the states
//!   here take already-evaluated values (an `Option<T>` per row, where
//!   `None` is Go's `isNull` skip) and, for the multi-argument path,
//!   already-evaluated [`Datum`]s. Expression evaluation itself stays with
//!   the caller; `AllocPartialResult`/`ResetPartialResult`/
//!   `AppendFinalResult2Chunk`/`MergePartialResult` become
//!   `new`/`reset`/`result`/`merge_from`.
//! - `memDelta` accounting is NOT carried. Every Go `Insert` delta comes
//!   from `hack.MemAwareMap`, which reads the live Go runtime's swiss-map
//!   header (`Used`, group size, `approxSize`) through `unsafe`; the
//!   `DefPartialResult4CountDistinct*Size` constants are likewise
//!   `unsafe.Sizeof` of those Go map headers. Neither number is
//!   reproducible from Rust, so `partial_state_size` reports this crate's
//!   own `size_of` (the convention [`CountState`] already established) and
//!   the mem-delta return values are dropped.
//! - `evalAndEncode` switches on `arg.GetType(sctx).EvalType()`; here it
//!   switches on the [`Datum`] variant instead, which is the same partition
//!   for every type the Go branch list covers. `Datum::UInt` joins the
//!   `ETInt` branch (Go's `EvalInt` returns the same `int64` bit pattern for
//!   unsigned arguments), and `Datum::Float32` joins `ETReal` (Go widens
//!   `TypeFloat` through `EvalReal`). ENUM/SET/BIT/binary-literal datums are
//!   rejected rather than guessed at: Go reaches the `ETString` branch for
//!   them through `EvalString`, which this tier has no descriptor to apply.
//! - `appendInt64`/`appendFloat64`/`appendDuration` write raw Go struct
//!   memory through `unsafe.Pointer`, so their bytes are native-endian and
//!   `appendDuration` emits `types.Duration`'s `{int64 Duration; int Fsp}`
//!   layout. [`append_int64`], [`append_float64`], and [`append_duration`]
//!   use `to_ne_bytes` and the same two-word layout, which is byte-identical
//!   on every platform TiDB builds for.
//! - Go's `set.Float64SetWithMemoryUsage` is a `map[float64]struct{}`, whose
//!   key equality is `==`: `+0.0` and `-0.0` collide, and NaN never equals
//!   itself, so every NaN inserted becomes an unreachable extra entry that
//!   still counts. [`CountDistinctRealState`] reproduces both rules exactly
//!   (see its field docs) rather than using an ordered bit key.
//! - `hack.String`/`stringutil.Copy`, which only launder Go's string/[]byte
//!   aliasing, have no Rust counterpart and are dropped; the owned `Vec<u8>`
//!   keys here already carry their own storage.
//! - Go's `set.StringSet` is a hash map with no iteration order;
//!   `BTreeSet<Vec<u8>>` is used throughout so `merge_from` and any dump of
//!   the set are deterministic. Only cardinality is observable, so this
//!   cannot change a result.

use std::collections::BTreeSet;

use tidb_datatype::{
    BinaryJSON, Collation, Datum, Decimal, FieldTypeCode, MySqlDuration, Time, TimeType,
    VectorFloat32,
};

use crate::ExecError;

/// Go `partialResult4Count` is one signed 64-bit counter for every input type.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CountState {
    value: i64,
}

/// Source typed-int `COUNT(DISTINCT)` set, colocated with the count runtime.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CountDistinctIntState {
    values: BTreeSet<i64>,
}

impl CountDistinctIntState {
    /// Creates an empty exact-integer DISTINCT set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            values: BTreeSet::new(),
        }
    }
    /// Inserts every non-NULL integer into the exact DISTINCT set.
    pub fn update(&mut self, values: &[Option<i64>]) {
        self.values.extend(values.iter().flatten().copied());
    }
    /// Unions another exact DISTINCT set into this state.
    pub fn merge_from(&mut self, source: &Self) {
        self.values.extend(source.values.iter().copied());
    }
    /// Drops all remembered values without replacing the state owner.
    pub fn reset(&mut self) {
        self.values.clear();
    }
    /// Returns the number of distinct non-NULL integers seen.
    #[must_use]
    pub fn result(&self) -> i64 {
        self.values.len() as i64
    }
    /// Returns the current exact-set cardinality as a collection size.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }
    /// Reports whether the exact DISTINCT set contains no values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    /// Returns the fixed structural width, excluding set allocations.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

impl CountState {
    /// Creates the source zero state.
    #[must_use]
    pub const fn new() -> Self {
        Self { value: 0 }
    }

    /// Resets the partial result.
    pub fn reset(&mut self) {
        self.value = 0;
    }

    /// Adds one for a non-NULL original value.
    pub fn update(&mut self, value: &Datum) {
        if !value.is_null() {
            self.value = self.value.wrapping_add(1);
        }
    }

    /// Adds a count produced by a partial executor.
    pub fn add_partial(&mut self, value: i64) {
        self.value = self.value.wrapping_add(value);
    }

    /// Merges a source partial result into this destination.
    pub fn merge_from(&mut self, source: &Self) {
        self.add_partial(source.value);
    }

    /// Slides by removing outgoing values before adding incoming values.
    pub fn slide(&mut self, outgoing: &[Datum], incoming: &[Datum]) {
        for value in outgoing {
            if !value.is_null() {
                self.value = self.value.wrapping_sub(1);
            }
        }
        for value in incoming {
            self.update(value);
        }
    }

    /// Returns the signed result.
    #[must_use]
    pub const fn result(self) -> i64 {
        self.value
    }

    /// Returns Go's fixed partial-result width.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        std::mem::size_of::<i64>()
    }
}

/// Go `partialResult4CountDistinctReal` with `baseCountDistinct4Real`'s
/// `UpdatePartialResult` / `AppendFinalResult2Chunk` and
/// `countPartialWithDistinct4Real.MergePartialResult`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CountDistinctRealState {
    /// Non-NaN keys, held as bits with `-0.0` folded onto `+0.0` because a
    /// Go `map[float64]` compares keys with `==`.
    values: BTreeSet<u64>,
    /// NaN insertions. A Go map key never equals NaN, so `Exist` is always
    /// false and every insert appends another unreachable entry that `Count`
    /// still returns.
    nan_entries: usize,
}

impl CountDistinctRealState {
    /// Go `baseCountDistinct4Real.AllocPartialResult`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            values: BTreeSet::new(),
            nan_entries: 0,
        }
    }

    /// Go's `map[float64]` key: `-0.0` and `+0.0` are the same key.
    fn key(value: f64) -> u64 {
        if value == 0.0 {
            0.0f64.to_bits()
        } else {
            value.to_bits()
        }
    }

    /// Inserts every non-NULL real; `None` is Go's `isNull` skip.
    pub fn update(&mut self, values: &[Option<f64>]) {
        for value in values.iter().flatten().copied() {
            if value.is_nan() {
                self.nan_entries += 1;
            } else {
                self.values.insert(Self::key(value));
            }
        }
    }

    /// Go `countPartialWithDistinct4Real.MergePartialResult`.
    pub fn merge_from(&mut self, source: &Self) {
        self.values.extend(source.values.iter().copied());
        self.nan_entries += source.nan_entries;
    }

    /// Go `baseCountDistinct4Real.ResetPartialResult`.
    pub fn reset(&mut self) {
        self.values.clear();
        self.nan_entries = 0;
    }

    /// Go `int64(p.valSet.Count())`.
    #[must_use]
    pub fn result(&self) -> i64 {
        self.len() as i64
    }

    /// The set cardinality Go's `Count` returns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len() + self.nan_entries
    }

    /// Reports whether the DISTINCT set contains no values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The fixed structural width, excluding set allocations.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Go `partialResult4CountDistinctDecimal`: a set over `MyDecimal.ToHashKey`,
/// so numerically equal decimals written at different scales collapse.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CountDistinctDecimalState {
    values: BTreeSet<Vec<u8>>,
}

impl CountDistinctDecimalState {
    /// Go `baseCountDistinct4Decimal.AllocPartialResult`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            values: BTreeSet::new(),
        }
    }

    /// Go `baseCountDistinct4Decimal.UpdatePartialResult`.
    ///
    /// Go propagates `ToHashKey`'s error out of the update; so does this.
    pub fn update(&mut self, values: &[Option<Decimal>]) -> Result<(), ExecError> {
        for value in values.iter().flatten() {
            let (key, _warning) = value
                .to_hash_key()
                .map_err(|_| ExecError::Unsupported("COUNT(DISTINCT) decimal hash key"))?;
            self.values.insert(key);
        }
        Ok(())
    }

    /// Go `countPartialWithDistinct4Decimal.MergePartialResult`.
    pub fn merge_from(&mut self, source: &Self) {
        self.values.extend(source.values.iter().cloned());
    }

    /// Go `baseCountDistinct4Decimal.ResetPartialResult`.
    pub fn reset(&mut self) {
        self.values.clear();
    }

    /// Go `int64(p.valSet.Count())`.
    #[must_use]
    pub fn result(&self) -> i64 {
        self.values.len() as i64
    }

    /// The set cardinality Go's `Count` returns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Reports whether the DISTINCT set contains no values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// The fixed structural width, excluding set allocations.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Go `partialResult4CountDistinctDuration`: an int64 set keyed on
/// `input.Duration` alone, so `'01:00:00'` at fsp 0 and at fsp 6 are ONE
/// distinct value.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CountDistinctDurationState {
    values: BTreeSet<i64>,
}

impl CountDistinctDurationState {
    /// Go `baseCountDistinct4Duration.AllocPartialResult`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            values: BTreeSet::new(),
        }
    }

    /// Go `baseCountDistinct4Duration.UpdatePartialResult`.
    pub fn update(&mut self, values: &[Option<MySqlDuration>]) {
        self.values
            .extend(values.iter().flatten().map(|value| value.nanoseconds()));
    }

    /// Go `countPartialWithDistinct4Duration.MergePartialResult`.
    pub fn merge_from(&mut self, source: &Self) {
        self.values.extend(source.values.iter().copied());
    }

    /// Go `baseCountDistinct4Duration.ResetPartialResult`.
    pub fn reset(&mut self) {
        self.values.clear();
    }

    /// Go `int64(p.valSet.Count())`.
    #[must_use]
    pub fn result(&self) -> i64 {
        self.values.len() as i64
    }

    /// The set cardinality Go's `Count` returns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Reports whether the DISTINCT set contains no values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// The fixed structural width, excluding set allocations.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Go `partialResult4CountDistinctString`: a set over the argument
/// collation's sort key, so DISTINCT follows the column's collation rather
/// than raw bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CountDistinctStringState {
    /// Go's `collate.GetCollator(e.args[0].GetType(sctx).GetCollate())`,
    /// resolved once per group in `UpdatePartialResult`.
    collation: Collation,
    values: BTreeSet<Vec<u8>>,
}

impl CountDistinctStringState {
    /// Go `baseCountDistinct4String.AllocPartialResult`, given the argument
    /// collation Go resolves at update time.
    #[must_use]
    pub const fn new(collation: Collation) -> Self {
        Self {
            collation,
            values: BTreeSet::new(),
        }
    }

    /// Go `baseCountDistinct4String.UpdatePartialResult`, whose key is
    /// `collator.Key(input)`.
    pub fn update(&mut self, values: &[Option<&[u8]>]) {
        for value in values.iter().flatten() {
            self.values.insert(self.collation.key(value));
        }
    }

    /// Go `countPartialWithDistinct4String.MergePartialResult`.
    ///
    /// Go unions already-collated keys, so no re-keying happens here either
    /// and a mismatched source collation would be Go's bug too.
    pub fn merge_from(&mut self, source: &Self) {
        self.values.extend(source.values.iter().cloned());
    }

    /// Go `baseCountDistinct4String.ResetPartialResult`.
    pub fn reset(&mut self) {
        self.values.clear();
    }

    /// Go `int64(p.valSet.Count())`.
    #[must_use]
    pub fn result(&self) -> i64 {
        self.values.len() as i64
    }

    /// The set cardinality Go's `Count` returns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Reports whether the DISTINCT set contains no values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// The fixed structural width, excluding set allocations.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Go `partialResult4CountWithDistinct` with `baseCountDistinct4MultiArgs`:
/// the group-key path taken by `COUNT(DISTINCT a, b, ...)` and by any single
/// argument whose type has no dedicated set (`DATE`/`DATETIME`/`TIMESTAMP`,
/// `JSON`, `VECTOR`). The key is the concatenated [`eval_and_encode`] output.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CountWithDistinctState {
    values: BTreeSet<Vec<u8>>,
}

impl CountWithDistinctState {
    /// Go `baseCountDistinct4MultiArgs.AllocPartialResult`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            values: BTreeSet::new(),
        }
    }

    /// Go `baseCountDistinct4MultiArgs.UpdatePartialResult` for ONE row.
    ///
    /// Returns whether the row produced a new distinct key. Go stops
    /// encoding at the first NULL argument (`hasNull`) and skips the row
    /// entirely, which is why a partially built key is never inserted.
    pub fn update_row(
        &mut self,
        row: &[Datum],
        collations: &[Collation],
    ) -> Result<bool, ExecError> {
        let mut encoded = Vec::new();
        for (index, value) in row.iter().enumerate() {
            let collation = collations.get(index).copied().unwrap_or(Collation::Binary);
            if eval_and_encode(&mut encoded, value, collation)? {
                // hasNull: abandon the row.
                return Ok(false);
            }
        }
        Ok(self.values.insert(encoded))
    }

    /// Go `baseCountDistinct4MultiArgs.UpdatePartialResult` over a group.
    pub fn update(
        &mut self,
        rows: &[Vec<Datum>],
        collations: &[Collation],
    ) -> Result<(), ExecError> {
        for row in rows {
            self.update_row(row, collations)?;
        }
        Ok(())
    }

    /// Inserts an already-encoded key, Go's `p.valSet.Insert(encodedString)`.
    pub fn insert_encoded(&mut self, key: Vec<u8>) -> bool {
        self.values.insert(key)
    }

    /// Go `countPartialWithDistinct.MergePartialResult`.
    pub fn merge_from(&mut self, source: &Self) {
        self.values.extend(source.values.iter().cloned());
    }

    /// Go `baseCountDistinct4MultiArgs.ResetPartialResult`.
    pub fn reset(&mut self) {
        self.values.clear();
    }

    /// Go `int64(p.valSet.Count())`.
    #[must_use]
    pub fn result(&self) -> i64 {
        self.values.len() as i64
    }

    /// The set cardinality Go's `Count` returns.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Reports whether the DISTINCT set contains no values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// The fixed structural width, excluding set allocations.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Go `evalAndEncode`: appends one already-evaluated value's group-key bytes.
///
/// Returns Go's `isNull`; the caller abandons the row when it is true.
pub fn eval_and_encode(
    encoded: &mut Vec<u8>,
    value: &Datum,
    collation: Collation,
) -> Result<bool, ExecError> {
    match value {
        Datum::Null => return Ok(true),
        // types.ETInt. Go's `EvalInt` hands unsigned arguments back as the
        // same int64 bit pattern.
        Datum::Int(value) => append_int64(encoded, *value),
        Datum::UInt(value) => append_int64(encoded, *value as i64),
        // types.ETReal. `Datum::Float32` is Go's `KindFloat32`, which
        // `EvalReal` widens to float64 before this switch sees it.
        Datum::Real(value) | Datum::Float32(value) => append_float64(encoded, *value),
        // types.ETDecimal.
        Datum::Decimal(value) => append_decimal(encoded, value)?,
        // types.ETTimestamp, types.ETDatetime.
        Datum::Time(value) => append_time(encoded, *value),
        // types.ETDuration.
        Datum::Duration(value) => append_duration(encoded, *value),
        // types.ETJson.
        Datum::Json(value) => append_json(encoded, value)?,
        // types.ETVectorFloat32.
        Datum::VectorFloat32(value) => append_vector_float32(encoded, value),
        // types.ETString.
        Datum::String(value) => {
            tidb_codec::encode_compact_bytes(encoded, &collation.immutable_key(value.bytes()));
        }
        Datum::Bytes(value) => {
            tidb_codec::encode_compact_bytes(encoded, &collation.immutable_key(value));
        }
        // Go's `default:` -- `errors.Errorf("unsupported column type for
        // encode %d", tp)`.
        _ => {
            return Err(ExecError::Unsupported(
                "COUNT(DISTINCT) unsupported column type for encode",
            ))
        }
    }
    Ok(false)
}

/// Go `appendInt64`.
pub fn append_int64(encoded: &mut Vec<u8>, value: i64) {
    encoded.extend_from_slice(&value.to_ne_bytes());
}

/// Go `appendFloat64`.
pub fn append_float64(encoded: &mut Vec<u8>, value: f64) {
    encoded.extend_from_slice(&value.to_ne_bytes());
}

/// Go `appendDecimal`.
pub fn append_decimal(encoded: &mut Vec<u8>, value: &Decimal) -> Result<(), ExecError> {
    let (hash, _warning) = value
        .to_hash_key()
        .map_err(|_| ExecError::Unsupported("COUNT(DISTINCT) decimal hash key"))?;
    encoded.extend_from_slice(&hash);
    Ok(())
}

/// Go `WriteTime`: writes `t` into a 16-byte `buf`, leaving no byte untouched.
pub fn write_time(buf: &mut [u8; 16], value: Time) {
    let core = value.core_time();
    buf[0..2].copy_from_slice(&(core.year() as u16).to_be_bytes());
    buf[2] = core.month();
    buf[3] = core.day();
    buf[4] = core.hour();
    buf[5] = core.minute();
    buf[6] = core.second();
    buf[8..12].copy_from_slice(&core.microsecond().to_be_bytes());
    // Go's `t.Type()`: `mysql.TypeDate` / `TypeDatetime` / `TypeTimestamp`.
    buf[12] = match value.kind() {
        TimeType::Date => FieldTypeCode::Date,
        TimeType::DateTime => FieldTypeCode::Datetime,
        TimeType::Timestamp => FieldTypeCode::Timestamp,
    }
    .mysql_type();
    buf[13] = value.fsp();

    buf[7] = 0;
    buf[14] = 0;
    buf[15] = 0;
}

/// Go `appendTime`.
pub fn append_time(encoded: &mut Vec<u8>, value: Time) {
    let mut buf = [0u8; 16];
    write_time(&mut buf, value);
    encoded.extend_from_slice(&buf);
}

/// Go `appendDuration`: the raw `types.Duration` struct memory, which is
/// `{int64 Duration; int Fsp}` -- 16 native-endian bytes.
pub fn append_duration(encoded: &mut Vec<u8>, value: MySqlDuration) {
    encoded.extend_from_slice(&value.nanoseconds().to_ne_bytes());
    encoded.extend_from_slice(&value.fsp().to_ne_bytes());
}

/// Go's `types.ETJson` arm, `val.HashValue(encodedBytes)`.
pub fn append_json(encoded: &mut Vec<u8>, value: &BinaryJSON) -> Result<(), ExecError> {
    let hash = value
        .hash_value()
        .map_err(|_| ExecError::Unsupported("COUNT(DISTINCT) JSON hash value"))?;
    encoded.extend_from_slice(&hash);
    Ok(())
}

/// Go's `types.ETVectorFloat32` arm, `val.SerializeTo(encodedBytes)`.
pub fn append_vector_float32(encoded: &mut Vec<u8>, value: &VectorFloat32) {
    value.serialize_to(encoded);
}
