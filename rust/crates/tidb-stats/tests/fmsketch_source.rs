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

//! Source-backed tests for the raw-hash FM sketch boundary.
//!
//! These tests use the original Go tests' direct `insertHashValue` and merge
//! scenarios, then pin the encoded-datum and tipb protobuf boundaries owned
//! by the adjacent codec layer.

use chrono::Utc;
use tidb_datatype::Datum;
use tidb_stats::{
    decode_fm_sketch, encode_fm_sketch, fm_sketch_from_proto, fm_sketch_to_proto, hash_datum,
    hash_row, insert_encoded_row, insert_encoded_value, insert_row_value, insert_value, FmSketch,
    FmSketchCodecError, FmSketchProto, MAX_SKETCH_SIZE,
};

#[test]
fn source_threshold_advances_mask_and_retains_zero_suffixes() {
    let mut sketch = FmSketch::new(2);
    sketch.insert_hash(1);
    sketch.insert_hash(2);
    assert_eq!(sketch.mask(), 0);
    assert_eq!(sketch.len(), 2);
    assert_eq!(sketch.ndv(), 2);

    // The third hash crosses maxSize.  Go advances mask 0 -> 1 and keeps the
    // even values (2 and 4), so the estimate remains 2 * 2.
    sketch.insert_hash(4);
    assert_eq!(sketch.mask(), 1);
    assert_eq!(sketch.len(), 2);
    assert!(sketch.contains(2));
    assert!(sketch.contains(4));
    assert!(!sketch.contains(1));
    assert_eq!(sketch.ndv(), 4);
}

#[test]
fn source_duplicate_insert_still_advances_when_threshold_is_zero() {
    // The Go method inserts into the map and checks len on every admitted
    // value; a duplicate therefore still advances a zero-sized sketch.
    let mut sketch = FmSketch::new(0);
    sketch.insert_hash(0);
    assert_eq!(sketch.mask(), 1);
    sketch.insert_hash(0);
    assert_eq!(sketch.mask(), 3);
    assert_eq!(sketch.len(), 1);
    assert_eq!(sketch.ndv(), 4);
}

#[test]
fn source_merge_raises_mask_filters_destination_and_replays_source() {
    let mut destination = FmSketch::new(10);
    destination.insert_hash(1);
    destination.insert_hash(2);

    let mut source = FmSketch::new(1);
    source.insert_hash(1);
    source.insert_hash(2);
    assert_eq!(source.mask(), 1);
    assert!(source.contains(2));
    assert!(!source.contains(1));

    destination.merge(&source);
    assert_eq!(destination.mask(), 1);
    assert_eq!(destination.len(), 1);
    assert!(destination.contains(2));
    assert_eq!(destination.ndv(), 2);
}

#[test]
fn source_copy_and_memory_shape_are_independent() {
    let mut sketch = FmSketch::new(MAX_SKETCH_SIZE);
    sketch.insert_hashes([7, 9, 7]);
    let clone = sketch.clone();

    assert_eq!(clone.mask(), sketch.mask());
    assert_eq!(clone.len(), sketch.len());
    assert_eq!(clone.ndv(), sketch.ndv());
    assert_eq!(sketch.memory_usage(), 16 + 8 * sketch.len() as u64);

    sketch.insert_hash(11);
    assert_eq!(clone.len(), 2);
    assert!(!clone.contains(11));
}

#[test]
fn source_proto_nil_empty_and_raw_state_boundaries_match() {
    assert_eq!(fm_sketch_to_proto(None), FmSketchProto::default());
    assert!(fm_sketch_from_proto(None).is_none());
    let proto = FmSketchProto {
        mask: 3,
        hashset: vec![8, 8, 12],
    };
    let restored = fm_sketch_from_proto(Some(&proto)).unwrap();
    assert_eq!(restored.mask(), 3);
    assert_eq!(restored.max_size(), 0);
    assert_eq!(restored.sorted_hashes(), [8, 12]);
}

#[test]
fn source_wire_round_trip_and_packed_repeated_values_match() {
    assert_eq!(encode_fm_sketch(None), None);
    assert_eq!(decode_fm_sketch(None).unwrap(), None);
    assert_eq!(
        encode_fm_sketch(Some(&FmSketch::new(8))),
        Some(vec![0x08, 0x00])
    );
    let decoded_empty = decode_fm_sketch(Some(&[])).unwrap().unwrap();
    assert!(decoded_empty.is_empty());
    assert_eq!(decoded_empty.max_size(), MAX_SKETCH_SIZE);
    let sketch = FmSketch::from_raw_parts(1, 5, [2, 4, 6]);
    let bytes = encode_fm_sketch(Some(&sketch)).unwrap();
    let decoded = decode_fm_sketch(Some(&bytes)).unwrap().unwrap();
    assert_eq!(decoded.mask(), 1);
    assert_eq!(decoded.max_size(), MAX_SKETCH_SIZE);
    assert_eq!(decoded.sorted_hashes(), [2, 4, 6]);

    let packed = [0x08, 0x01, 0x12, 0x03, 0x02, 0x04, 0x06, 0x18, 0x09];
    assert_eq!(
        decode_fm_sketch(Some(&packed))
            .unwrap()
            .unwrap()
            .sorted_hashes(),
        [2, 4, 6]
    );
}

#[test]
fn source_wire_rejects_malformed_inputs() {
    assert_eq!(
        decode_fm_sketch(Some(&[0x08])).unwrap_err(),
        FmSketchCodecError::Truncated
    );
    assert_eq!(
        decode_fm_sketch(Some(&[0x0b])).unwrap_err(),
        FmSketchCodecError::InvalidWireType
    );
    assert_eq!(
        decode_fm_sketch(Some(&[
            0x08, 0x82, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02,
        ]))
        .unwrap_err(),
        FmSketchCodecError::VarintOverflow
    );
    for wrong_known_wire in [
        &[0x0a, 0x00][..],
        &[0x15, 0x00, 0x00, 0x00, 0x00][..],
        &[0x00, 0x00][..],
        &[0x1c][..],
    ] {
        assert_eq!(
            decode_fm_sketch(Some(wrong_known_wire)).unwrap_err(),
            FmSketchCodecError::InvalidWireType
        );
    }
    let balanced_unknown_group = [0x1b, 0x20, 0x01, 0x1c];
    assert!(decode_fm_sketch(Some(&balanced_unknown_group)).is_ok());
}

#[test]
fn source_encoded_value_and_row_hash_the_same_stream() {
    let mut value = FmSketch::new(8);
    insert_encoded_value(&mut value, b"abc");
    let mut row = FmSketch::new(8);
    insert_encoded_row(&mut row, [b"a".as_slice(), b"bc".as_slice()]);
    assert_eq!(value, row);
    assert_eq!(value.len(), 1);
}

#[test]
fn source_typed_insert_value_and_row_own_the_datum_encoding() {
    let values = [Datum::Int(1), Datum::Bytes(b"abc".to_vec())];
    let encoded_first = tidb_codec::encode_value(&values[..1]).unwrap();
    assert_eq!(
        hash_datum(&Utc, &values[0]).unwrap(),
        tidb_stats::hash_bytes(&encoded_first).h1
    );

    let encoded_row = tidb_codec::encode_value(&values).unwrap();
    assert_eq!(
        hash_row(&Utc, &values).unwrap(),
        tidb_stats::hash_bytes(&encoded_row).h1
    );

    let mut typed_value = FmSketch::new(8);
    insert_value(&mut typed_value, &Utc, &values[0]).unwrap();
    let mut encoded_value = FmSketch::new(8);
    insert_encoded_value(&mut encoded_value, &encoded_first);
    assert_eq!(typed_value, encoded_value);

    let mut typed_row = FmSketch::new(8);
    insert_row_value(&mut typed_row, &Utc, &values).unwrap();
    let mut encoded_row_sketch = FmSketch::new(8);
    insert_encoded_value(&mut encoded_row_sketch, &encoded_row);
    assert_eq!(typed_row, encoded_row_sketch);
}
