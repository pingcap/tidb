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

//! TiDB's dependency-closed mem-comparable scalar and datum-key codec.
//!
//! This crate translates the production formats consumed by row handles. It
//! intentionally contains no opaque fixture-only handle representation.

mod bytes;
mod column;
mod datum;
mod decimal;
mod duration;
mod error;
mod float;
mod json;
mod number;
mod row_decoder;
mod row_encoder;
mod row_index;
mod row_layout;
pub mod table_key;
mod temporal;
mod value;

pub use bytes::{
    decode_bytes, decode_bytes_desc, decode_compact_bytes, encode_bytes, encode_bytes_desc,
    encode_bytes_ext, encode_compact_bytes, encoded_bytes_len,
};
pub use column::{
    decode_column_datums, decode_columns, ColumnCodecError, ColumnLayout, RawColumn,
    TypedColumnError,
};
pub use datum::{
    cut_one, decode_one, decode_range, encode_key, peek_one_len, Encoder, BYTES_FLAG, DECIMAL_FLAG,
    FLOAT_FLAG, INT_FLAG, MAX_FLAG, NIL_FLAG, UINT_FLAG,
};
pub use decimal::{
    decimal_encoded_len, decode_decimal, encode_decimal_fixed, inspect_decimal, DecimalWireMetadata,
};
pub use duration::{
    decode_duration, encode_duration, RawDuration, RawDurationParts, MAX_DURATION_FSP,
};
pub use error::CodecError;
pub use float::{decode_float, decode_float_desc, encode_float, encode_float_desc};
pub use json::{
    decode_json, peek_json_len, RawJson, RawJsonTemporal, RawJsonTemporalKind,
    JSON_TYPE_CODE_ARRAY, JSON_TYPE_CODE_DATE, JSON_TYPE_CODE_DATETIME, JSON_TYPE_CODE_DURATION,
    JSON_TYPE_CODE_FLOAT64, JSON_TYPE_CODE_INT64, JSON_TYPE_CODE_LITERAL, JSON_TYPE_CODE_OBJECT,
    JSON_TYPE_CODE_OPAQUE, JSON_TYPE_CODE_STRING, JSON_TYPE_CODE_TIMESTAMP, JSON_TYPE_CODE_UINT64,
};
pub use number::{
    decode_cmp_uint_to_int, decode_comparable_uvarint, decode_comparable_varint, decode_int,
    decode_int_desc, decode_uint, decode_uint_desc, decode_uvarint, decode_varint,
    encode_comparable_uvarint, encode_comparable_varint, encode_int, encode_int_desc,
    encode_int_to_cmp_uint, encode_uint, encode_uint_desc, encode_uvarint, encode_varint,
};
pub use row_decoder::{decode_raw_int, decode_raw_uint, RawRowValue, RowDecodeError, RowDecoder};
pub use row_encoder::{
    encode_raw_int, encode_raw_row, encode_raw_uint, RawRowColumn, RowEncodeError,
};
pub use row_index::{
    decode_table_id, encode_row_key, gen_table_record_prefix, get_key_kind, KeyKind,
};
pub use row_layout::{
    is_new_format, is_row_key, ColumnLookup, RowChecksum, RowCodecError, RowHeader, RowLayout,
    CHECKSUM_FLAG_EXTRA, CHECKSUM_VERSION_MASK, ROW_CODEC_VERSION, ROW_FLAG_CHECKSUM,
    ROW_FLAG_LARGE, ROW_HEADER_LEN,
};
pub use temporal::{decode_packed_time, encode_packed_time};
pub use value::{
    decode_default_row, decode_default_rows, decode_value, RawValue, VALUE_BYTES_FLAG,
    VALUE_COMPACT_BYTES_FLAG, VALUE_DECIMAL_FLAG, VALUE_DURATION_FLAG, VALUE_FLOAT_FLAG,
    VALUE_INT_FLAG, VALUE_JSON_FLAG, VALUE_MAX_FLAG, VALUE_NIL_FLAG, VALUE_UINT_FLAG,
    VALUE_UVARINT_FLAG, VALUE_VARINT_FLAG, VALUE_VECTOR_FLOAT32_FLAG,
};

#[cfg(test)]
mod tests;
