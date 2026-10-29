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

//! Go `pkg/util/rowcodec`: TiDB's row format v2 (Rust).

mod row_decoder;
mod row_encoder;
mod row_layout;
mod rowcodec;

pub use row_decoder::{decode_raw_int, decode_raw_uint, RawRowValue, RowDecodeError, RowDecoder};
pub use row_encoder::{encode_raw_int, encode_raw_row, encode_raw_uint, RawRowColumn};
pub use row_layout::{
    is_new_format, is_row_key, ColumnLookup, RowChecksum, RowCodecError, RowHeader, RowLayout,
    CHECKSUM_FLAG_EXTRA, CHECKSUM_VERSION_MASK, ROW_CODEC_VERSION, ROW_FLAG_CHECKSUM,
    ROW_FLAG_LARGE, ROW_HEADER_LEN,
};
pub use rowcodec::{
    calculate_raw_checksum, decode_row_to_datums, decode_row_to_map, decode_row_to_old_bytes,
    encode_row, encode_row_with_checksum, field_type_from_column, remove_keyspace_prefix,
    ColumnInfo, DatumColumn, DecodeRowOptions, DecodedRow, Handle, RowChecksumPolicy, RowData,
    RowPackageError,
};
