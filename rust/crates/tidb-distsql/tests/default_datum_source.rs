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

//! Source checks for typed scalar materialization from a `TypeDefault` chunk.

use tidb_codec::{encode_compact_bytes, encode_float, encode_int};
use tidb_datatype::Datum;
use tidb_distsql::{decode_chunk, ChunkDecodeError};
use tidb_proto::{Chunk, EncodeType};

#[test]
fn default_chunk_materializes_codec_decode_one_scalar_rows() {
    // Go sources: pkg/util/codec/codec.go::DecodeOne and
    // pkg/util/codec/codec_test.go::TestDecodeRange.  Rows have no RowMeta;
    // the caller supplies the schema column count just like DecodeRange.
    let mut rows_data = vec![0, 8, 2, 9, 0xac, 0x02];
    rows_data.extend_from_slice(&[5]);
    encode_float(&mut rows_data, 1.25);
    rows_data.extend_from_slice(&[2]);
    encode_compact_bytes(&mut rows_data, b"abc");
    rows_data.extend_from_slice(&[3]);
    encode_int(&mut rows_data, -7);
    rows_data.extend_from_slice(&[0, 9, 0]);

    let chunk = Chunk {
        rows_data: Some(rows_data),
        rows_meta: Vec::new(),
    };
    let raw = decode_chunk(&chunk, EncodeType::TypeDefault).unwrap();
    let rows = raw.decode_default_datums(4).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Datum::Null);
    assert_eq!(rows[0][1], Datum::new_int(1));
    assert_eq!(rows[0][2], Datum::new_uint(300));
    assert_eq!(rows[0][3], Datum::new_real(1.25));
    assert_eq!(rows[1][0], Datum::new_bytes(b"abc"));
    assert_eq!(rows[1][1], Datum::new_int(-7));
    assert_eq!(rows[1][2], Datum::Null);
    assert_eq!(rows[1][3], Datum::new_uint(0));
}

#[test]
fn default_chunk_preserves_explicit_unsupported_datum_boundary() {
    let chunk = Chunk {
        rows_data: Some(vec![tidb_codec::VALUE_DURATION_FLAG; 9]),
        rows_meta: Vec::new(),
    };
    let raw = decode_chunk(&chunk, EncodeType::TypeDefault).unwrap();
    assert_eq!(
        raw.decode_default_datums(1),
        Err(ChunkDecodeError::DefaultCodec(
            tidb_codec::CodecError::UnsupportedValueTag(tidb_codec::VALUE_DURATION_FLAG)
        ))
    );
}
