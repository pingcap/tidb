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

//! Source checks for the bounded native CHBlock/tipb chunk envelope.
//!
//! `pkg/executor/internal/mpp/local_mpp_coordinator.go` selects CHBlock only
//! for non-root TiFlash dispatch.  The source `pkg/distsql/select_result.go`
//! has no native decoder, so these tests assert byte/metadata preservation and
//! an explicit typed-decoding boundary rather than inventing ClickHouse
//! schema or Datum semantics.

use tidb_distsql::{decode_ch_block, ChunkDecodeError};
use tidb_proto::{Chunk, EncodeType, RowMeta};

#[test]
fn chblock_envelope_preserves_payload_and_row_metadata() {
    let chunk = Chunk {
        rows_data: Some(b"native-chblock".to_vec()),
        rows_meta: vec![
            RowMeta {
                handle: Some(41),
                length: Some(6),
            },
            RowMeta {
                handle: None,
                length: Some(8),
            },
        ],
    };

    let decoded = decode_ch_block(&chunk).expect("source-shaped CHBlock envelope");
    assert_eq!(decoded.payload(), b"native-chblock");
    assert_eq!(decoded.row_count(), 2);
    assert_eq!(decoded.row(0).and_then(|row| row.handle), Some(41));
    assert_eq!(decoded.row(0).map(|row| row.data), Some(&b"native"[..]));
    assert_eq!(decoded.row(1).map(|row| row.data), Some(&b"-chblock"[..]));
}

#[test]
fn chblock_empty_metadata_keeps_opaque_payload_and_rejects_native_guessing() {
    let chunk = Chunk {
        rows_data: Some(vec![0xde, 0xad, 0xbe, 0xef]),
        rows_meta: Vec::new(),
    };

    let decoded = decode_ch_block(&chunk).expect("metadata-free CHBlock remains opaque");
    assert_eq!(decoded.payload(), &[0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(decoded.row_count(), 0);
    assert_eq!(
        decoded.decode_native(),
        Err(ChunkDecodeError::UnsupportedTypedRowDecoding {
            encode_type: EncodeType::TypeChBlock,
        })
    );
}

#[test]
fn chblock_envelope_reuses_row_length_validation_and_rejects_other_encodings() {
    let malformed = Chunk {
        rows_data: Some(b"x".to_vec()),
        rows_meta: vec![RowMeta {
            handle: None,
            length: Some(2),
        }],
    };
    assert_eq!(
        decode_ch_block(&malformed),
        Err(ChunkDecodeError::RowLengthExceedsData {
            row_index: 0,
            length: 2,
            remaining: 1,
        })
    );

    let raw_type_chunk = tidb_distsql::decode_chunk(&malformed, EncodeType::TypeChunk)
        .expect_err("the malformed row is still rejected before type wrapping");
    assert_eq!(
        raw_type_chunk,
        ChunkDecodeError::RowLengthExceedsData {
            row_index: 0,
            length: 2,
            remaining: 1,
        }
    );
}
