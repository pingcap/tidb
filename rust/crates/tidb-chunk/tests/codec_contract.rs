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

//! Public semantic contract for accepted `pkg/util/chunk/codec.go`.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::codec::{estimate_type_width, Codec, CodecDecodeError, Decoder};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

const GO_VECTORS: &str =
    include_str!("../../../difftests/chunk-tests/fixtures/chunk_codec_vectors.tsv");

fn field(code: FieldTypeCode) -> FieldType {
    FieldType::new(code)
}

fn fixture_fields(name: &str) -> Vec<FieldType> {
    use FieldTypeCode as C;
    match name {
        "int64_with_null" | "no_nulls" => vec![field(C::LongLong)],
        "varchar_with_null" | "all_null_two_bitmap_bytes" => vec![field(C::Varchar)],
        "zero_rows" => vec![field(C::LongLong), field(C::Varchar)],
        "all_shapes" => [
            C::Tiny,
            C::Float,
            C::Double,
            C::Year,
            C::Duration,
            C::NewDecimal,
            C::Datetime,
            C::Varchar,
            C::Blob,
            C::Json,
            C::Enum,
            C::Set,
            C::Bit,
        ]
        .into_iter()
        .map(field)
        .collect(),
        other => panic!("unknown codec fixture {other}"),
    }
}

fn decode_hex(text: &str) -> Vec<u8> {
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII fixture"), 16)
                .expect("hex fixture")
        })
        .collect()
}

fn codec_source(rows: usize) -> (Vec<FieldType>, Chunk) {
    let fields = vec![
        field(FieldTypeCode::LongLong),
        field(FieldTypeCode::Varchar),
    ];
    let mut chunk = Chunk::new_with_capacity(&fields, rows);
    for row in 0..rows {
        if row % 5 == 1 {
            chunk.append_null(0);
        } else {
            chunk.append_int64(0, row as i64 * 10);
        }
        if row % 4 == 2 {
            chunk.append_null(1);
        } else {
            chunk.append_string(1, format!("value-{row}"));
        }
    }
    (fields, chunk)
}

#[test]
fn wire_image_matches_go_and_checked_decode_preserves_suffix() {
    for line in GO_VECTORS.lines() {
        let (name, hex) = line.split_once('\t').expect("name and hex fixture");
        let fields = fixture_fields(name);
        let image = decode_hex(hex);
        let codec = Codec::new(fields.clone());
        let mut decoded = Chunk::new_empty(&fields);
        assert!(codec
            .try_decode_to_chunk(&image, &mut decoded)
            .expect("accepted Go image")
            .is_empty());
        assert_eq!(codec.encode(&decoded), image, "{name}");
    }

    let (fields, source) = codec_source(3);
    let codec = Codec::new(fields.clone());
    let mut image = codec.encode(&source);
    image.extend_from_slice(&[0xde, 0xad]);
    let mut target = Chunk::new_empty(&fields);
    assert_eq!(
        codec
            .try_decode_to_chunk(&image, &mut target)
            .expect("valid image and suffix"),
        [0xde, 0xad]
    );

    assert_eq!(
        codec.try_decode_to_chunk(&[1, 2, 3], &mut target),
        Err(CodecDecodeError::Truncated {
            ordinal: 0,
            section: "header",
            needed: 8,
            remaining: 3,
        })
    );
}

#[test]
fn decoder_batches_and_reuses_remaining_rows() {
    let (fields, source) = codec_source(19);
    let image = Codec::new(fields.clone()).encode(&source);
    let mut decoder = Decoder::new(Chunk::new_with_capacity(&fields, 0), fields.clone());
    decoder.reset(&image);

    let mut first = Chunk::new(&fields, 3, 5);
    for value in [-3_i64, -2, -1] {
        first.append_int64(0, value);
        first.append_string(1, "prefix");
    }
    decoder.decode(&mut first);
    assert_eq!(first.num_rows(), 11);
    assert_eq!(decoder.remained_rows(), 11);

    let mut second = Chunk::new(&fields, 8, 8);
    decoder.decode(&mut second);
    assert_eq!(decoder.remained_rows(), 3);

    let mut remainder = Chunk::new(&fields, 0, 64);
    decoder.reuse_intermediate_chunk(&mut remainder);
    assert!(decoder.is_finished());
    assert_eq!(remainder.num_rows(), 3);
    assert_eq!(remainder.required_rows(), 64);
    for row in 0..3 {
        for (column, field_type) in fields.iter().enumerate() {
            assert_eq!(
                remainder.get_row(row).get_datum(column, field_type),
                source.get_row(row + 16).get_datum(column, field_type)
            );
        }
    }
}

#[test]
fn type_width_table_matches_the_accepted_owner() {
    use FieldTypeCode as C;
    assert_eq!(estimate_type_width(&field(C::LongLong).with_flen(2_000)), 8);
    for (flen, expected) in [
        (-1, 32),
        (31, 31),
        (32, 32),
        (33, 32),
        (999, 515),
        (2_000, 516),
    ] {
        assert_eq!(
            estimate_type_width(&field(C::Varchar).with_flen(flen)),
            expected
        );
    }
    assert_eq!(estimate_type_width(&field(C::Date)), 8);
    assert_eq!(estimate_type_width(&field(C::NewDate)), 32);
}

#[test]
fn benchmark_workloads_keep_codec_semantics_live() {
    let (fields, source) = codec_source(1_024);
    let codec = Codec::new(fields.clone());
    let image = codec.encode(&source);
    let mut decoded_rows = 0usize;
    let mut encoded_bytes = 0usize;
    for _ in 0..64 {
        encoded_bytes += codec.encode(&source).len();
        let mut target = Chunk::new_empty(&fields);
        assert!(codec.decode_to_chunk(&image, &mut target).is_empty());
        decoded_rows += target.num_rows();
    }
    assert_eq!(decoded_rows, 64 * 1_024);
    assert_eq!(encoded_bytes, 64 * image.len());
    assert_eq!(source.get_row(0).get_datum(0, &fields[0]), Datum::Int(0));
    println!(
        "{}",
        r#"LOCKDOWN_OBSERVATION {"boundary_observations":[{"input":"1,024 fixed and variable rows encoded and decoded for 64 deterministic repetitions","name":"codec-workload-results","observed":"every decode returns 1,024 rows and every encode returns the accepted byte length"},{"input":"accepted unsafe slice reinterpretation and all-not-null static initialization","name":"runtime-storage-adaptation","observed":"Rust emits the same native-endian offsets and all-one bitmap bytes through safe typed storage"},{"input":"accepted testing.B loops, timer reset and allocation behavior","name":"benchmark-runtime-excluded","observed":"no TiDB result depends on Go benchmark-runner mechanics"}],"conclusion":"Rust preserves the deterministic encode, decode, DecodeToChunk, fixed/variable and null-bitmap results exercised by the accepted codec benchmarks; unsafe slice reinterpretation, static initialization mechanics and testing.B timing are intentionally not reproduced.","probe_id":"CHUNK-CODEC-RUNTIME-ADAPTATION","schema":"go-package-lockdown-runtime-observation-v1","source_commit":"665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f"}"#
    );
}
