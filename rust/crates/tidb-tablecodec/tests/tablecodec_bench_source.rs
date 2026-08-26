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

//! Ports of the benchmark functions from Go `pkg/tablecodec/bench_test.go`
//! (plus `BenchmarkHasTablePrefix`/`BenchmarkEncodeValue` from
//! `tablecodec_test.go`). Each benchmark loop becomes a fixed-iteration Rust
//! test so the exercised code paths stay pinned even though cargo measures
//! them separately via `benches/tablecodec.rs`.

use tidb_codec::table_key::{
    decode_row_key, encode_row_key_with_handle, RecordHandle,
};
use tidb_codec::{encode_key, Encoder};
use tidb_datatype::{BinaryLiteral, Collation, Datum, Decimal, MysqlEnum, MysqlSet};
use tidb_tablecodec::{
    decode_handle_in_index_value, encode_handle_in_unique_index_value, encode_table_value,
    COMMON_HANDLE_FLAG, INDEX_VERSION_FLAG,
};
use tidb_tablecodec::table_key::{gen_table_prefix, TABLE_PREFIX, RECORD_ROW_KEY_LEN};
use tidb_txnkv::IntHandle;

/// Go `time.UTC`, matching the zones used by the Go benchmarks' stmtctx.
const UTC: tidb_datatype::SessionTimeZone = tidb_datatype::SessionTimeZone::Named(chrono_tz::UTC);

const ITERATIONS: usize = 100;

fn int_record(value: i64) -> RecordHandle {
    RecordHandle::Int(value)
}

/// Mirrors Go `kv.Key.PrefixNext` (`Next()` then a trailing zero byte): bytes
/// equal to 0xff roll over to 0x00 while scanning backwards, the first lesser
/// byte increments, and the result grows by one trailing 0x00.
fn prefix_next(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    for byte in next.iter_mut().rev() {
        if *byte == 0xff {
            *byte = 0;
        } else {
            *byte += 1;
            next.push(0);
            return next;
        }
    }
    next
}

/// Source: `bench_test.go::BenchmarkEncodeRowKeyWithHandle`.
#[test]
fn benchmark_encode_row_key_with_handle() {
    for _ in 0..ITERATIONS {
        let key = encode_row_key_with_handle(100, &int_record(100));
        assert_eq!(key.len(), RECORD_ROW_KEY_LEN);
    }
}

/// Source: `bench_test.go::BenchmarkEncodeEndKey` — both bounds of the handle
/// range are encoded each iteration.
#[test]
fn benchmark_encode_end_key() {
    for _ in 0..ITERATIONS {
        let start = encode_row_key_with_handle(100, &int_record(100));
        let end = encode_row_key_with_handle(100, &int_record(101));
        assert!(start < end);
    }
}

/// Source: `bench_test.go::BenchmarkEncodeRowKeyWithPrefixNex`. The Go
/// benchmark compares the cost of `PrefixNext` against direct re-encoding;
/// here we pin its ordering contract (`Next()` then a trailing zero byte),
/// which lands strictly after the start key but is not itself a valid
/// row-key re-encoding of the next handle.
#[test]
fn benchmark_encode_row_key_with_prefix_nex() {
    for _ in 0..ITERATIONS {
        let start = encode_row_key_with_handle(100, &int_record(100));
        let stepped = prefix_next(&start);
        assert!(stepped > start);
        assert_ne!(stepped, encode_row_key_with_handle(100, &int_record(101)));
    }
}

/// Source: `bench_test.go::BenchmarkDecodeRowKey`.
#[test]
fn benchmark_decode_row_key() {
    let row_key = encode_row_key_with_handle(100, &int_record(100));
    for _ in 0..ITERATIONS {
        assert_eq!(decode_row_key(&row_key).unwrap(), int_record(100));
    }
}

/// Source: `bench_test.go::BenchmarkDecodeIndexKeyIntHandle` — handle values
/// greater than 255 exercise the multi-byte memory path.
#[test]
fn benchmark_decode_index_key_int_handle() {
    let idx_val = encode_handle_in_unique_index_value(&IntHandle::new(256).into(), false);
    for _ in 0..ITERATIONS {
        assert_eq!(
            decode_handle_in_index_value(&idx_val)
                .unwrap()
                .int_value(),
            Some(256)
        );
    }
}

/// Source: `bench_test.go::BenchmarkDecodeIndexKeyCommonHandle` — an index
/// version 1 value carrying a two-column common handle.
#[test]
fn benchmark_decode_index_key_common_handle() {
    let mut idx_val = vec![0, INDEX_VERSION_FLAG, 1];
    let encoded = encode_key(&[Datum::Int(1), Datum::Int(2)]).unwrap();
    idx_val.push(COMMON_HANDLE_FLAG);
    idx_val.extend_from_slice(&(encoded.len() as u16).to_be_bytes());
    idx_val.extend_from_slice(&encoded);
    let handle = decode_handle_in_index_value(&idx_val).unwrap();
    assert!(matches!(
        handle,
        tidb_txnkv::Handle::Common(_)
    ), "expected common handle, got {handle:?}");
    for _ in 0..ITERATIONS {
        decode_handle_in_index_value(&idx_val).unwrap();
    }
}

/// Source: `tablecodec_test.go::BenchmarkHasTablePrefix` and
/// `BenchmarkHasTablePrefixBuiltin` — the plain prefix check the package
/// helper is benchmarked against.
#[test]
fn benchmark_has_table_prefix() {
    let key = b"foobar";
    assert!(!key.starts_with(TABLE_PREFIX));
    let table_key = gen_table_prefix(1);
    assert!(table_key.starts_with(TABLE_PREFIX));
}

/// Source: `tablecodec_test.go::BenchmarkEncodeValue` — one value per row
/// kind the Go benchmark covers.
#[test]
fn benchmark_encode_value() {
    let row = [
        Datum::Int(100),
        Datum::new_bytes(b"abc"),
        Datum::Decimal(Decimal::from_literal("1")),
        Datum::Enum(MysqlEnum::new("a", 0), Collation::Binary),
        Datum::Set(MysqlSet::new("a", 0), Collation::Binary),
        Datum::Bit(BinaryLiteral::from(vec![100])),
        Datum::Float32(1.5),
    ];
    for datum in &row {
        let encoded = encode_table_value(Some(&UTC), datum).unwrap();
        assert!(!encoded.is_empty());
    }
    // The encoder flag plumbing the Go benchmark relies on stays usable.
    assert!(Encoder::new(true).use_new_collation());
}