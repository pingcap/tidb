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

//! Go `pkg/param`: the binary-protocol parameter (`BinaryParam`) and the
//! value a prepared statement's execution binds from it (Go `ExecArgs`).

/// One split binary parameter: the raw value bytes plus the type tags that
/// select how a later stage interprets them.
///
/// This mirrors Go `param.BinaryParam`. `val` holds the raw little-endian /
/// text bytes exactly as they arrived, except that the string group is decoded
/// from the connection's client charset to UTF-8 as Go does.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinaryParam {
    /// The MySQL field type declared for this parameter (a `TYPE_*` code).
    pub tp: u8,
    /// Whether the parameter's type pair set the unsigned flag bit.
    pub is_unsigned: bool,
    /// Whether the value is SQL `NULL` (a declared `TypeNull` or a
    /// length-encoded NULL marker; the NULL-bitmap path uses `tp == TYPE_NULL`).
    pub is_null: bool,
    /// The raw value bytes sliced from the execute packet.
    pub val: Vec<u8>,
}

/// A prepared statement's parameter value as the execution binds it: what
/// Go's `ExecArgs` decodes a [`BinaryParam`] into.
///
/// A signed integer width interprets to one `i64` (Go `ExecBinaryParam`
/// sign-extends `int8`/`int16`/`int32`/`int64`); a string parameter carries its
/// raw length-encoded bytes.
///
/// The string and BLOB families stay apart because Go binds them to
/// different datums: `TypeVarchar`/`TypeString`/... become a string datum
/// (`types.NewDatum(string)`, the default `utf8mb4_bin` collation) while
/// `TypeBlob`/`TypeTinyBlob`/... become a bytes datum (`NewBytesDatum`, the
/// binary collation). A string parameter that arrived as binary bytes
/// compared with a `utf8mb4` column in the binary collation, which kept the
/// predicate out of the coprocessor.
#[derive(Clone, Debug, PartialEq)]
pub enum PreparedValue {
    /// A signed integer parameter, sign-extended to 64 bits from its wire width.
    SignedLongLong(i64),
    /// A string-family parameter's raw bytes (utf8 for the configured node).
    String(Vec<u8>),
    /// A BLOB-family parameter's raw bytes, Go's `NewBytesDatum` arm.
    Bytes(Vec<u8>),
    /// An unsigned integer parameter, widened to 64 bits.
    UnsignedLongLong(u64),
    /// A `FLOAT` parameter, widened to `f64` the way Go widens `float32`.
    Float(f32),
    /// A `DOUBLE` parameter.
    Double(f64),
    /// A `DECIMAL` parameter's digits, which Go parses with
    /// `MyDecimal.FromString`.
    Decimal(Vec<u8>),
    /// A parameter the execute packet marked NULL in its bitmap.
    Null,
    /// A temporal parameter, rendered the way Go renders it before parsing:
    /// `binaryDate`/`binaryDateTime`/`binaryTimestamp` produce the date-time
    /// text and `binaryDuration` the day/time span text.
    Temporal(String),
}
