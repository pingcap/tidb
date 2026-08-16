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

//! Transcreation of Go `pkg/ttl/cache`'s tests (`base_test.go`,
//! `split_test.go`, `table_test.go`, `task_test.go`, `ttlstatus_test.go`,
//! `infoschema_test.go`).
//!
//! Six of the seven Go test files import `testkit`. The tests whose subject is
//! the cache's own arithmetic are ported here with their expected values
//! byte-for-byte; where Go reaches a value through a live server, the fixture
//! builds the same value directly. `mockPDClient`/`mockTiKVStore` become
//! [`MockRegionCache`], which implements the crate's
//! [`RegionCache`](tidb_ttl::cache::table::RegionCache) boundary the way Go's
//! mock PD answers `ScanRegions`.
//!
//! Skipped, with exactly what each would need:
//! - `TestTableEvalTTLExpireTime` is ported only in the half that does not need
//!   a live session; the boundary is named on the test itself.
//! - `TestInsertIntoTTLTask` — asserts the round trip of
//!   `codec.EncodeKey`-encoded scan ranges through `mysql.tidb_ttl_task`. The
//!   encoding is now reachable (`tidb-codec` is a dependency); the round trip
//!   still needs a live store, and `InsertIntoTTLTask` itself is still
//!   unported.
//! - `TestTTLStatusCache` and `TestInfoSchemaCache` — both build a mock server
//!   (`server.CreateMockServer`/`CreateMockConn`), run DDL and DML, and assert
//!   the caches pick the changes up. They need a live TiDB plus the real
//!   `infoschema.InfoSchema`.
//! - `TestMergeRegion` and `TestRegionDisappearDuringSplitRange` — both drive a
//!   real `tikv.RegionCache` through concurrent region merges. They need
//!   `client-go`'s region cache, not the boundary trait.
//! - `TestSplitTTLScanRangesWithBytes`'s and `TestNoTTLSplitSupportTables`'s
//!   table lists are built here from `TableInfo` values rather than from DDL,
//!   because `createTTLTable` runs `CREATE TABLE` on a live store.

use std::time::Duration;

use chrono::TimeZone as _;

use tidb_datatype::{CoreTime, Datum, FieldType, FieldTypeCode, Time, TimeType};
use tidb_model::table::TTLInfo;
use tidb_model::{ColumnInfo, GoShared, TableInfo};

use tidb_ast::CiString;
use tidb_codec::{decode_cmp_uint_to_int, encode_key};
use tidb_tablecodec::table_key::{
    encode_row_key, encode_row_key_with_handle, gen_table_prefix, gen_table_record_prefix,
    RecordHandle,
};
use tidb_ttl::cache::base::BaseCache;
use tidb_ttl::cache::table::{
    eval_expire_time, set_mock_expire_time, MockExpireTimeKey, TimeUnitType,
};
use tidb_ttl::cache::table::{
    get_ascii_prefix_datum_from_bytes, get_next_bytes_handle_datum,
    get_next_int_datum_from_common_handle, get_next_int_handle, new_physical_table, KeyLocation,
    PhysicalTable, RegionCache, ScanRange,
};
use tidb_ttl::cache::task::{
    peek_waiting_ttl_task, row_to_ttl_task, select_from_ttl_task_with_id,
    select_from_ttl_task_with_job_id, SqlArg, TaskStatus, SELECT_FROM_TTL_TASK,
};
use tidb_ttl::cache::ttlstatus::{
    row_to_table_status, select_from_ttl_table_status_with_id, JobStatus,
};
use tidb_ttl::session::{ResultRow, TtlSession};
use tidb_txnkv::Key;

// -------------------------------------------------------------------------
// base_test.go
// -------------------------------------------------------------------------

/// Go `TestBaseCache`.
#[test]
fn test_base_cache() {
    let mut base_cache = BaseCache::new(Duration::from_nanos(1));
    std::thread::sleep(Duration::from_micros(1));

    assert!(base_cache.should_update());

    base_cache.mark_updated();
    base_cache.set_interval(Duration::from_secs(3600));
    assert!(!base_cache.should_update());
}

// -------------------------------------------------------------------------
// split_test.go: the pure key-decoding tests
// -------------------------------------------------------------------------

/// Go `TestGetASCIIPrefixDatumFromBytes`.
#[test]
fn test_get_ascii_prefix_datum_from_bytes() {
    let cases: Vec<(Vec<u8>, &str)> = vec![
        (Vec::new(), ""),
        (vec![], ""),
        (vec![0], ""),
        (vec![1], ""),
        (vec![8], ""),
        (vec![9], "\t"),
        (vec![10], "\n"),
        (vec![11], ""),
        (vec![12], ""),
        (vec![13], "\r"),
        (vec![14], ""),
        (vec![0x19], ""),
        (vec![0x20], " "),
        (vec![0x21], "!"),
        (vec![0x7D], "}"),
        (vec![0x7E], "~"),
        (vec![0x7F], ""),
        (vec![0xFF], ""),
        (vec![0x0, b'a', b'b'], ""),
        (vec![0xFF, b'a', b'b'], ""),
        (vec![b'0', b'1', 0x0, b'a', b'b'], "01"),
        (vec![b'0', b'1', 0x15, b'a', b'b'], "01"),
        (vec![b'0', b'1', 0xFF, b'a', b'b'], "01"),
        (vec![b'a', b'b', 0x0], "ab"),
        (vec![b'a', b'b', 0x15], "ab"),
        (vec![b'a', b'b', 0xFF], "ab"),
        (
            b"ab\rcd\tef\nAB!~GH()tt ;;".to_vec(),
            "ab\rcd\tef\nAB!~GH()tt ;;",
        ),
        ("中文".as_bytes().to_vec(), ""),
        ("cn中文".as_bytes().to_vec(), "cn"),
        ("😀".as_bytes().to_vec(), ""),
        ("emoji😀".as_bytes().to_vec(), "emoji"),
    ];

    for (i, (bs, expected)) in cases.iter().enumerate() {
        let d = get_ascii_prefix_datum_from_bytes(bs);
        let Datum::String(text) = &d else {
            panic!("i: {i}, expected a string datum, got {d:?}");
        };
        assert_eq!(text.bytes(), expected.as_bytes(), "i: {i}, bs: {bs:?}");
    }
}

/// Go `TestGetNextIntHandle`.
#[test]
fn test_get_next_int_handle() {
    let tbl_id: i64 = 7;
    let record_prefix = gen_table_record_prefix(tbl_id);
    let row_key = |handle: i64| encode_row_key_with_handle(tbl_id, &RecordHandle::Int(handle));
    let appended = |handle: i64, byte: u8| {
        let mut key = row_key(handle);
        key.push(byte);
        key
    };
    let cmp = decode_cmp_uint_to_int;

    let cases: Vec<(Vec<u8>, Option<i64>)> = vec![
        (row_key(0), Some(0)),
        (row_key(3), Some(3)),
        (row_key(i64::MAX), Some(i64::MAX)),
        (row_key(i64::MIN), Some(i64::MIN)),
        (appended(7, 0), Some(8)),
        (appended(i64::MAX, 0), None),
        (appended(i64::MIN, 0), Some(i64::MIN + 1)),
        (Vec::new(), Some(i64::MIN)),
        (record_prefix.clone(), Some(i64::MIN)),
        (gen_table_record_prefix(tbl_id - 1), Some(i64::MIN)),
        (
            Key::from_bytes(gen_table_prefix(tbl_id))
                .prefix_next()
                .into_bytes(),
            None,
        ),
        (encode_row_key(tbl_id, &[0]), Some(cmp(0))),
        (
            encode_row_key(tbl_id, &[0, 1, 2, 3]),
            Some(cmp(0x0001_0203_0000_0000)),
        ),
        (
            encode_row_key(tbl_id, &[8, 1, 2, 3]),
            Some(cmp(0x0801_0203_0000_0000)),
        ),
        (
            encode_row_key(tbl_id, &[0, 1, 2, 3, 4, 5, 6, 7, 0]),
            Some(cmp(0x0001_0203_0405_0607) + 1),
        ),
        (
            encode_row_key(tbl_id, &[8, 1, 2, 3, 4, 5, 6, 7, 0]),
            Some(cmp(0x0801_0203_0405_0607) + 1),
        ),
        (encode_row_key(tbl_id, &[0xff; 8]), Some(i64::MAX)),
        (
            encode_row_key(tbl_id, &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0]),
            None,
        ),
    ];

    for (i, (key, expected)) in cases.iter().enumerate() {
        assert_eq!(
            get_next_int_handle(key, &record_prefix),
            *expected,
            "case {i}"
        );
    }
}

/// Go `TestGetNextBytesHandleDatum`.
#[test]
fn test_get_next_bytes_handle_datum() {
    let tbl_id: i64 = 7;
    let record_prefix = gen_table_record_prefix(tbl_id);
    let build_handle_bytes = |data: &[u8]| {
        encode_key(&[Datum::new_bytes(data.to_vec())]).expect("a bytes datum is always encodable")
    };
    let build_row_key = |handle_bytes: &[u8]| encode_row_key(tbl_id, handle_bytes);
    let build_bytes_row_key = |data: &[u8]| build_row_key(&build_handle_bytes(data));
    let with_last = |data: &[u8], byte: u8| {
        let mut key = build_bytes_row_key(data);
        let last = key.len() - 1;
        key[last] = byte;
        key
    };
    let with_nth_from_end = |data: &[u8], offset: usize, byte: u8| {
        let mut key = build_bytes_row_key(data);
        let index = key.len() - offset;
        key[index] = byte;
        key
    };
    let appended = |data: &[u8], byte: u8| {
        let mut key = build_bytes_row_key(data);
        key.push(byte);
        key
    };
    let binary_data_start_pos = record_prefix.len() + 1;
    let truncated = |data: &[u8], extra: usize| {
        let key = build_bytes_row_key(data);
        key[..binary_data_start_pos + extra].to_vec()
    };

    // `None` is Go's `isNull: true`.
    let cases: Vec<(Vec<u8>, Option<Vec<u8>>)> = vec![
        (build_bytes_row_key(&[]), Some(vec![])),
        (build_bytes_row_key(&[1, 2, 3]), Some(vec![1, 2, 3])),
        (build_bytes_row_key(&[1, 2, 3, 0]), Some(vec![1, 2, 3, 0])),
        (
            build_bytes_row_key(&[1, 2, 3, 4, 5, 6, 7, 8]),
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        ),
        (
            build_bytes_row_key(&[1, 2, 3, 4, 5, 6, 7, 8, 9]),
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
        ),
        (
            build_bytes_row_key(&[1, 2, 3, 4, 5, 6, 7, 8, 0]),
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 0]),
        ),
        (
            appended(&[1, 2, 3, 4, 5, 6, 7, 8, 0], 0),
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 0, 0]),
        ),
        (
            appended(&[1, 2, 3, 4, 5, 6, 7, 8, 0], 1),
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 0, 0]),
        ),
        (Vec::new(), Some(vec![])),
        (record_prefix.clone(), Some(vec![])),
        (gen_table_record_prefix(tbl_id - 1), Some(vec![])),
        (
            Key::from_bytes(gen_table_prefix(tbl_id))
                .prefix_next()
                .into_bytes(),
            None,
        ),
        (build_row_key(&[0]), Some(vec![])),
        (build_row_key(&[1]), Some(vec![])),
        (build_row_key(&[2]), None),
        // recordPrefix + bytesFlag + [0]
        (truncated(&[], 1), Some(vec![])),
        // recordPrefix + bytesFlag + [0, 0, 0, 0, 0, 0, 0, 0]
        (truncated(&[], 8), Some(vec![])),
        // recordPrefix + bytesFlag + [1]
        (truncated(&[1, 2, 3], 1), Some(vec![1])),
        // recordPrefix + bytesFlag + [1, 2, 3]
        (truncated(&[1, 2, 3], 3), Some(vec![1, 2, 3])),
        // recordPrefix + bytesFlag + [1, 2, 3, 0]
        (truncated(&[1, 2, 3], 4), Some(vec![1, 2, 3])),
        // recordPrefix + bytesFlag + [1, 2, 3, 0, 0, 0, 0, 0, 247]
        (with_last(&[1, 2, 3], 247), Some(vec![1, 2, 3])),
        // recordPrefix + bytesFlag + [1, 2, 3, 0, 0, 0, 0, 0, 0]
        (with_last(&[1, 2, 3], 0), Some(vec![1, 2, 3])),
        // recordPrefix + bytesFlag + [1..8, 254, 9, 0 x7, 248]
        (
            with_nth_from_end(&[1, 2, 3, 4, 5, 6, 7, 8, 9], 10, 254),
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        ),
        (
            with_nth_from_end(&[1, 2, 3, 4, 5, 6, 7, 0, 9], 10, 254),
            Some(vec![1, 2, 3, 4, 5, 6, 7, 0]),
        ),
        (
            with_nth_from_end(&[1, 2, 3, 4, 5, 6, 7, 0, 9], 10, 253),
            Some(vec![1, 2, 3, 4, 5, 6, 7]),
        ),
        (
            with_last(&[1, 2, 3, 4, 5, 6, 7, 8, 9], 247),
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
        ),
        (
            with_last(&[1, 2, 3, 4, 5, 6, 7, 8, 9], 0),
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
        ),
        (
            {
                let key = build_bytes_row_key(&[1, 2, 3, 4, 5, 6, 7, 8]);
                key[..key.len() - 1].to_vec()
            },
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        ),
    ];

    for (i, (key, expected)) in cases.iter().enumerate() {
        let d = get_next_bytes_handle_datum(key, &record_prefix);
        match expected {
            None => assert!(
                matches!(d, Datum::Null),
                "case {i}: expected null, got {d:?}"
            ),
            Some(result) => {
                let Datum::Bytes(bytes) = &d else {
                    panic!("case {i}: expected a bytes datum, got {d:?}");
                };
                assert_eq!(bytes, result, "case {i}");
            }
        }
    }
}

/// Go `TestGetNextIntDatumFromCommonHandle`.
#[test]
fn test_get_next_int_datum_from_common_handle() {
    let tbl_id: i64 = 7;
    let record_prefix = gen_table_record_prefix(tbl_id);
    let encode = |datums: &[Datum]| encode_row_key(tbl_id, &common_handle(datums));
    let truncate_one = |datums: &[Datum]| {
        let key = encode(datums);
        key[..key.len() - 1].to_vec()
    };
    let with_flag = |flag: u8| {
        let mut key = record_prefix.clone();
        key.push(flag);
        key
    };

    let cases: Vec<(Vec<u8>, Datum, bool)> = vec![
        (encode(&[Datum::new_int(0)]), Datum::new_int(0), false),
        (encode(&[Datum::new_int(1)]), Datum::new_int(1), false),
        (encode(&[Datum::new_int(1024)]), Datum::new_int(1024), false),
        (
            encode(&[Datum::new_int(i64::MAX)]),
            Datum::new_int(i64::MAX),
            false,
        ),
        (
            encode(&[Datum::new_int(i64::MAX / 2)]),
            Datum::new_int(i64::MAX / 2),
            false,
        ),
        (encode(&[Datum::new_int(-1)]), Datum::new_int(-1), false),
        (
            encode(&[Datum::new_int(-1024)]),
            Datum::new_int(-1024),
            false,
        ),
        (
            encode(&[Datum::new_int(i64::MIN)]),
            Datum::new_int(i64::MIN),
            false,
        ),
        (
            encode(&[Datum::new_int(i64::MIN / 2)]),
            Datum::new_int(i64::MIN / 2),
            false,
        ),
        (
            truncate_one(&[Datum::new_int(i64::MAX)]),
            Datum::new_int(i64::MAX - 0xFF),
            false,
        ),
        (
            encode(&[Datum::new_int(i64::MAX), Datum::new_int(0)]),
            Datum::Null,
            false,
        ),
        (
            encode(&[Datum::new_int(i64::MAX - 1), Datum::new_int(0)]),
            Datum::new_int(i64::MAX),
            false,
        ),
        (
            encode(&[Datum::new_int(123), Datum::new_int(0)]),
            Datum::new_int(124),
            false,
        ),
        (
            encode(&[Datum::new_int(-123), Datum::new_int(0)]),
            Datum::new_int(-122),
            false,
        ),
        (
            encode(&[Datum::new_int(i64::MIN), Datum::new_int(0)]),
            Datum::new_int(i64::MIN + 1),
            false,
        ),
        (encode(&[Datum::new_uint(0)]), Datum::new_uint(0), true),
        (encode(&[Datum::new_uint(1)]), Datum::new_uint(1), true),
        (
            encode(&[Datum::new_uint(1024)]),
            Datum::new_uint(1024),
            true,
        ),
        (
            encode(&[Datum::new_uint(i64::MAX as u64)]),
            Datum::new_uint(i64::MAX as u64),
            true,
        ),
        (
            encode(&[Datum::new_uint(i64::MAX as u64 + 1)]),
            Datum::new_uint(i64::MAX as u64 + 1),
            true,
        ),
        (
            encode(&[Datum::new_uint(u64::MAX)]),
            Datum::new_uint(u64::MAX),
            true,
        ),
        (
            truncate_one(&[Datum::new_uint(u64::MAX)]),
            Datum::new_uint(u64::MAX - 0xFF),
            true,
        ),
        (
            encode(&[Datum::new_uint(u64::MAX), Datum::new_int(0)]),
            Datum::Null,
            false,
        ),
        (
            encode(&[Datum::new_uint(u64::MAX - 1), Datum::new_int(0)]),
            Datum::new_uint(u64::MAX),
            true,
        ),
        (
            encode(&[Datum::new_uint(123), Datum::new_int(0)]),
            Datum::new_uint(124),
            true,
        ),
        (
            encode(&[Datum::new_uint(0), Datum::new_int(0)]),
            Datum::new_uint(1),
            true,
        ),
        (Vec::new(), Datum::new_int(i64::MIN), false),
        (Vec::new(), Datum::new_uint(0), true),
        (record_prefix.clone(), Datum::new_int(i64::MIN), false),
        (record_prefix.clone(), Datum::new_uint(0), true),
        // 3 is encoded intFlag
        (with_flag(3), Datum::new_int(i64::MIN), false),
        (with_flag(3), Datum::new_uint(0), true),
        // 4 is encoded uintFlag
        (with_flag(4), Datum::Null, false),
        (with_flag(4), Datum::new_uint(0), true),
        // 5
        (with_flag(5), Datum::Null, false),
        (with_flag(5), Datum::Null, true),
    ];

    for (i, (key, expected, unsigned)) in cases.iter().enumerate() {
        let d = get_next_int_datum_from_common_handle(key, &record_prefix, *unsigned);
        assert_eq!(&d, expected, "case {i}");
    }
}

// -------------------------------------------------------------------------
// split_test.go: the region-splitting family
// -------------------------------------------------------------------------

/// Go's `mockPDClient` plus `mockTiKVStore`, reduced to the one method
/// `splitRawKeyRanges` calls.
///
/// Go layers a real `tikv.RegionCache` over the mock PD; the cache is a
/// pass-through for `LocateKeyRange`, so this answers exactly what Go's
/// `ScanRegions` answers: an implicit head region below the first added one, an
/// implicit tail region above the last, and the overlap filter.
#[derive(Default)]
struct MockRegionCache {
    regions: Vec<(Vec<u8>, Vec<u8>)>,
}

impl MockRegionCache {
    fn clear_regions(&mut self) {
        self.regions.clear();
    }

    fn add_region(&mut self, start: Vec<u8>, end: Vec<u8>) {
        assert!(end > start);
        self.regions.push((start, end));
        self.regions.sort();
    }

    fn add_region_begin_with_table_prefix(&mut self, table_id: i64, handle_end: &[u8]) {
        let start = gen_table_prefix(table_id);
        let end = encode_row_key(table_id, handle_end);
        self.add_region(start, end);
    }

    fn add_region_end_with_table_prefix(&mut self, handle_start: &[u8], table_id: i64) {
        let start = encode_row_key(table_id, handle_start);
        let end = gen_table_prefix(table_id + 1);
        self.add_region(start, end);
    }

    fn add_region_with_table_prefix(&mut self, table_id: i64, start: &[u8], end: &[u8]) {
        self.add_region(
            encode_row_key(table_id, start),
            encode_row_key(table_id, end),
        );
    }

    fn batch_add_int_handle_regions(
        &mut self,
        table_id: i64,
        region_cnt: i64,
        region_size: i64,
        offset: i64,
    ) -> i64 {
        let mut end = 0;
        for i in 0..region_cnt {
            let start = offset + i * region_size;
            end = start + region_size;
            self.add_region_with_table_prefix(table_id, &encode_int(start), &encode_int(end));
        }
        end
    }
}

impl RegionCache for MockRegionCache {
    fn locate_key_range(
        &self,
        key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<KeyLocation>, tidb_ttl::cache::CacheError> {
        if self.regions.is_empty() {
            return Ok(vec![KeyLocation {
                start_key: Vec::new(),
                end_key: vec![0xFF, 0xFF],
            }]);
        }

        let mut regions = vec![(Vec::new(), self.regions[0].0.clone())];
        regions.extend(self.regions.iter().cloned());
        regions.push((
            self.regions[self.regions.len() - 1].1.clone(),
            vec![0xFF, 0xFF, 0xFF],
        ));

        Ok(regions
            .into_iter()
            .filter(|(start, end)| start.as_slice() < end_key && end.as_slice() > key)
            .map(|(start_key, end_key)| KeyLocation { start_key, end_key })
            .collect())
    }
}

/// Go `codec.EncodeInt(nil, value)`, whose Rust form appends into a buffer.
fn encode_int(value: i64) -> Vec<u8> {
    let mut buffer = Vec::new();
    tidb_codec::encode_int(&mut buffer, value);
    buffer
}

fn int_handle(value: i64) -> Vec<u8> {
    encode_int(value)
}

fn common_handle(datums: &[Datum]) -> Vec<u8> {
    encode_key(datums).expect("a TTL handle datum is always encodable")
}

fn bytes_handle(data: &[u8]) -> Vec<u8> {
    common_handle(&[Datum::new_bytes(data.to_vec())])
}

/// A TTL table with a single clustered key column of the given type.
fn ttl_table(id: i64, key_type: FieldType) -> PhysicalTable {
    PhysicalTable {
        id,
        schema: CiString::new("test"),
        key_columns: vec![ColumnInfo {
            name: CiString::new("id"),
            field_type: key_type.clone(),
            ..Default::default()
        }],
        key_column_types: vec![key_type],
        ..Default::default()
    }
}

/// A TTL table with a two-column clustered primary key, as Go's
/// `create2PKTTLTable` builds.
fn ttl_table_2pk(id: i64, key_type: FieldType) -> PhysicalTable {
    let mut tbl = ttl_table(id, key_type);
    let second = ColumnInfo {
        name: CiString::new("id2"),
        field_type: FieldType::new(FieldTypeCode::Long),
        ..Default::default()
    };
    tbl.key_column_types.push(second.field_type.clone());
    tbl.key_columns.push(second);
    tbl
}

fn signed(code: FieldTypeCode) -> FieldType {
    FieldType::new(code)
}

fn unsigned(code: FieldTypeCode) -> FieldType {
    let mut ft = FieldType::new(code);
    ft.set_flags(tidb_mysql::UnsignedFlag as u32);
    ft
}

fn binary_string(code: FieldTypeCode) -> FieldType {
    let mut ft = FieldType::new(code);
    ft.set_flags(tidb_mysql::BinaryFlag as u32);
    ft.set_charset_name("binary");
    ft.set_collation_name("binary");
    ft
}

fn text_string(code: FieldTypeCode, charset: &str, collation: &str) -> FieldType {
    let mut ft = FieldType::new(code);
    ft.set_charset_name(charset);
    ft.set_collation_name(collation);
    ft
}

/// Go's `checkRange`.
fn check_range(range: &ScanRange, start: &Datum, end: &Datum, msg: &str) {
    if matches!(start, Datum::Null) {
        assert!(range.start.is_empty(), "{msg}: expected no start bound");
    } else {
        assert_eq!(range.start.len(), 1, "{msg}");
        assert_eq!(&range.start[0], start, "{msg}");
    }

    if matches!(end, Datum::Null) {
        assert!(range.end.is_empty(), "{msg}: expected no end bound");
    } else {
        assert_eq!(range.end.len(), 1, "{msg}");
        assert_eq!(&range.end[0], end, "{msg}");
    }
}

/// Go `TestSplitTTLScanRangesWithSignedInt`.
#[test]
fn test_split_ttl_scan_ranges_with_signed_int() {
    let tbls = vec![
        ttl_table(11, signed(FieldTypeCode::Tiny)),
        ttl_table(12, signed(FieldTypeCode::Short)),
        ttl_table(13, signed(FieldTypeCode::Int24)),
        ttl_table(14, signed(FieldTypeCode::Long)),
        ttl_table(15, signed(FieldTypeCode::LongLong)),
        // "no clustered" — the `_tidb_rowid` extra handle column.
        ttl_table(16, ColumnInfo::new_extra_handle_col_info().field_type),
    ];

    let mut store = MockRegionCache::default();
    for tbl in &tbls {
        // test only one region
        store.clear_regions();
        let ranges = tbl.split_scan_ranges(Some(&store), 4).unwrap();
        assert_eq!(ranges.len(), 1);
        check_range(&ranges[0], &Datum::Null, &Datum::Null, "one region");

        // test share regions with other table
        store.clear_regions();
        store.add_region(gen_table_prefix(tbl.id - 1), gen_table_prefix(tbl.id + 1));
        let ranges = tbl.split_scan_ranges(Some(&store), 4).unwrap();
        assert_eq!(ranges.len(), 1);
        check_range(&ranges[0], &Datum::Null, &Datum::Null, "shared region");

        // test one table has multiple regions
        store.clear_regions();
        store.add_region_begin_with_table_prefix(tbl.id, &int_handle(0));
        let end = store.batch_add_int_handle_regions(tbl.id, 8, 100, 0);
        store.add_region_end_with_table_prefix(&int_handle(end), tbl.id);
        let ranges = tbl.split_scan_ranges(Some(&store), 4).unwrap();
        assert_eq!(ranges.len(), 4);
        check_range(&ranges[0], &Datum::Null, &Datum::new_int(200), "multi 0");
        check_range(
            &ranges[1],
            &Datum::new_int(200),
            &Datum::new_int(500),
            "multi 1",
        );
        check_range(
            &ranges[2],
            &Datum::new_int(500),
            &Datum::new_int(700),
            "multi 2",
        );
        check_range(&ranges[3], &Datum::new_int(700), &Datum::Null, "multi 3");

        // test one table has multiple regions and one table region across 0
        store.clear_regions();
        store.add_region_begin_with_table_prefix(tbl.id, &int_handle(-350));
        let end = store.batch_add_int_handle_regions(tbl.id, 8, 100, -350);
        store.add_region_end_with_table_prefix(&int_handle(end), tbl.id);
        let ranges = tbl.split_scan_ranges(Some(&store), 5).unwrap();
        assert_eq!(ranges.len(), 5);
        check_range(&ranges[0], &Datum::Null, &Datum::new_int(-250), "cross 0");
        check_range(
            &ranges[1],
            &Datum::new_int(-250),
            &Datum::new_int(-50),
            "cross 1",
        );
        check_range(
            &ranges[2],
            &Datum::new_int(-50),
            &Datum::new_int(150),
            "cross 2",
        );
        check_range(
            &ranges[3],
            &Datum::new_int(150),
            &Datum::new_int(350),
            "cross 3",
        );
        check_range(&ranges[4], &Datum::new_int(350), &Datum::Null, "cross 4");
    }
}

/// Go `TestSplitTTLScanRangesWithUnsignedInt`.
#[test]
fn test_split_ttl_scan_ranges_with_unsigned_int() {
    let tbls = vec![
        ttl_table(21, unsigned(FieldTypeCode::Tiny)),
        ttl_table(22, unsigned(FieldTypeCode::Short)),
        ttl_table(23, unsigned(FieldTypeCode::Int24)),
        ttl_table(24, unsigned(FieldTypeCode::Long)),
        ttl_table(25, unsigned(FieldTypeCode::LongLong)),
    ];

    let max_int64_plus_1 = i64::MAX as u64 + 1;
    let mut store = MockRegionCache::default();
    for tbl in &tbls {
        store.clear_regions();
        let ranges = tbl.split_scan_ranges(Some(&store), 4).unwrap();
        assert_eq!(ranges.len(), 1);
        check_range(&ranges[0], &Datum::Null, &Datum::Null, "one region");

        store.clear_regions();
        store.add_region(gen_table_prefix(tbl.id - 1), gen_table_prefix(tbl.id + 1));
        let ranges = tbl.split_scan_ranges(Some(&store), 4).unwrap();
        assert_eq!(ranges.len(), 1);
        check_range(&ranges[0], &Datum::Null, &Datum::Null, "shared region");

        // [MinInt64, a) [a, b) [b, 0) [0, c) [c, d) [d, MaxInt64]
        store.clear_regions();
        store.add_region_begin_with_table_prefix(tbl.id, &int_handle(-200));
        let end = store.batch_add_int_handle_regions(tbl.id, 4, 100, -200);
        store.add_region_end_with_table_prefix(&int_handle(end), tbl.id);
        let ranges = tbl.split_scan_ranges(Some(&store), 6).unwrap();
        assert_eq!(ranges.len(), 6);
        check_range(
            &ranges[0],
            &Datum::new_uint(max_int64_plus_1),
            &Datum::new_uint(u64::MAX - 199),
            "u 0",
        );
        check_range(
            &ranges[1],
            &Datum::new_uint(u64::MAX - 199),
            &Datum::new_uint(u64::MAX - 99),
            "u 1",
        );
        check_range(
            &ranges[2],
            &Datum::new_uint(u64::MAX - 99),
            &Datum::Null,
            "u 2",
        );
        check_range(&ranges[3], &Datum::Null, &Datum::new_uint(100), "u 3");
        check_range(
            &ranges[4],
            &Datum::new_uint(100),
            &Datum::new_uint(200),
            "u 4",
        );
        check_range(
            &ranges[5],
            &Datum::new_uint(200),
            &Datum::new_uint(max_int64_plus_1),
            "u 5",
        );

        // [MinInt64, a) [a, b) [b, c) [c, d) [d, MaxInt64], b < 0 < c
        store.clear_regions();
        store.add_region_begin_with_table_prefix(tbl.id, &int_handle(-150));
        let end = store.batch_add_int_handle_regions(tbl.id, 3, 100, -150);
        store.add_region_end_with_table_prefix(&int_handle(end), tbl.id);
        let ranges = tbl.split_scan_ranges(Some(&store), 5).unwrap();
        assert_eq!(ranges.len(), 6);
        check_range(
            &ranges[0],
            &Datum::new_uint(max_int64_plus_1),
            &Datum::new_uint(u64::MAX - 149),
            "v 0",
        );
        check_range(
            &ranges[1],
            &Datum::new_uint(u64::MAX - 149),
            &Datum::new_uint(u64::MAX - 49),
            "v 1",
        );
        check_range(
            &ranges[2],
            &Datum::new_uint(u64::MAX - 49),
            &Datum::Null,
            "v 2",
        );
        check_range(&ranges[3], &Datum::Null, &Datum::new_uint(50), "v 3");
        check_range(
            &ranges[4],
            &Datum::new_uint(50),
            &Datum::new_uint(150),
            "v 4",
        );
        check_range(
            &ranges[5],
            &Datum::new_uint(150),
            &Datum::new_uint(max_int64_plus_1),
            "v 5",
        );
    }
}

/// Go `TestSplitTTLScanRangesCommonHandleSignedInt`.
#[test]
fn test_split_ttl_scan_ranges_common_handle_signed_int() {
    let tbls = vec![
        ttl_table_2pk(31, signed(FieldTypeCode::LongLong)),
        ttl_table_2pk(32, signed(FieldTypeCode::Long)),
    ];

    let mut store = MockRegionCache::default();
    for tbl in &tbls {
        store.clear_regions();
        let ranges = tbl.split_scan_ranges(Some(&store), 4).unwrap();
        assert_eq!(ranges.len(), 1);
        check_range(&ranges[0], &Datum::Null, &Datum::Null, "one region");

        store.clear_regions();
        store.add_region(gen_table_prefix(tbl.id - 1), gen_table_prefix(tbl.id + 1));
        let ranges = tbl.split_scan_ranges(Some(&store), 4).unwrap();
        assert_eq!(ranges.len(), 1);
        check_range(&ranges[0], &Datum::Null, &Datum::Null, "shared region");

        store.clear_regions();
        store.add_region_begin_with_table_prefix(tbl.id, &common_handle(&[Datum::new_int(-21)]));
        store.add_region_with_table_prefix(
            tbl.id,
            &common_handle(&[Datum::new_int(-21)]),
            &common_handle(&[Datum::new_int(-19), Datum::new_int(0)]),
        );
        store.add_region_with_table_prefix(
            tbl.id,
            &common_handle(&[Datum::new_int(-19), Datum::new_int(0)]),
            &common_handle(&[Datum::new_int(2)]),
        );
        store.add_region_end_with_table_prefix(&common_handle(&[Datum::new_int(2)]), tbl.id);
        let ranges = tbl.split_scan_ranges(Some(&store), 4).unwrap();
        assert_eq!(ranges.len(), 4);
        check_range(&ranges[0], &Datum::Null, &Datum::new_int(-21), "ch 0");
        check_range(
            &ranges[1],
            &Datum::new_int(-21),
            &Datum::new_int(-18),
            "ch 1",
        );
        check_range(&ranges[2], &Datum::new_int(-18), &Datum::new_int(2), "ch 2");
        check_range(&ranges[3], &Datum::new_int(2), &Datum::Null, "ch 3");
    }
}

/// Go `TestSplitTTLScanRangesCommonHandleUnsignedInt`.
#[test]
fn test_split_ttl_scan_ranges_common_handle_unsigned_int() {
    let tbls = vec![
        ttl_table_2pk(41, unsigned(FieldTypeCode::LongLong)),
        ttl_table_2pk(42, unsigned(FieldTypeCode::Long)),
    ];

    let big = i64::MAX as u64 + 9;
    let mut store = MockRegionCache::default();
    for tbl in &tbls {
        store.clear_regions();
        let ranges = tbl.split_scan_ranges(Some(&store), 4).unwrap();
        assert_eq!(ranges.len(), 1);
        check_range(&ranges[0], &Datum::Null, &Datum::Null, "one region");

        store.clear_regions();
        store.add_region(gen_table_prefix(tbl.id - 1), gen_table_prefix(tbl.id + 1));
        let ranges = tbl.split_scan_ranges(Some(&store), 4).unwrap();
        assert_eq!(ranges.len(), 1);
        check_range(&ranges[0], &Datum::Null, &Datum::Null, "shared region");

        store.clear_regions();
        store.add_region_begin_with_table_prefix(tbl.id, &common_handle(&[Datum::new_uint(9)]));
        store.add_region_with_table_prefix(
            tbl.id,
            &common_handle(&[Datum::new_uint(9)]),
            &common_handle(&[Datum::new_uint(23), Datum::new_uint(0)]),
        );
        store.add_region_with_table_prefix(
            tbl.id,
            &common_handle(&[Datum::new_uint(23), Datum::new_uint(0)]),
            &common_handle(&[Datum::new_uint(big)]),
        );
        store.add_region_end_with_table_prefix(&common_handle(&[Datum::new_uint(big)]), tbl.id);
        let ranges = tbl.split_scan_ranges(Some(&store), 4).unwrap();
        assert_eq!(ranges.len(), 4);
        check_range(&ranges[0], &Datum::Null, &Datum::new_uint(9), "chu 0");
        check_range(
            &ranges[1],
            &Datum::new_uint(9),
            &Datum::new_uint(24),
            "chu 1",
        );
        check_range(
            &ranges[2],
            &Datum::new_uint(24),
            &Datum::new_uint(big),
            "chu 2",
        );
        check_range(&ranges[3], &Datum::new_uint(big), &Datum::Null, "chu 3");
    }
}

/// Go `TestSplitTTLScanRangesWithBytes`.
#[test]
fn test_split_ttl_scan_ranges_with_bytes() {
    struct Case {
        name: &'static str,
        region_edges: Vec<Vec<u8>>,
        split_cnt: i64,
        binary_expected: Vec<(Datum, Datum)>,
        string_expected: Vec<(Datum, Datum)>,
    }

    let bytes = |data: &[u8]| Datum::new_bytes(data.to_vec());
    let text = |data: &str| Datum::new_string(data.as_bytes().to_vec());

    let cases = vec![
        Case {
            name: "2 regions with binary split",
            region_edges: vec![bytes_handle(&[1, 2, 3])],
            split_cnt: 4,
            binary_expected: vec![
                (Datum::Null, bytes(&[1, 2, 3])),
                (bytes(&[1, 2, 3]), Datum::Null),
            ],
            string_expected: vec![(Datum::Null, Datum::Null)],
        },
        Case {
            name: "6 regions with binary split",
            region_edges: vec![
                bytes_handle(&[1, 2, 3]),
                bytes_handle(&[1, 2, 3, 4]),
                bytes_handle(&[1, 2, 3, 4, 5]),
                bytes_handle(&[1, 2, 4]),
                bytes_handle(&[1, 2, 5]),
            ],
            split_cnt: 4,
            binary_expected: vec![
                (Datum::Null, bytes(&[1, 2, 3, 4])),
                (bytes(&[1, 2, 3, 4]), bytes(&[1, 2, 4])),
                (bytes(&[1, 2, 4]), bytes(&[1, 2, 5])),
                (bytes(&[1, 2, 5]), Datum::Null),
            ],
            string_expected: vec![(Datum::Null, Datum::Null)],
        },
        Case {
            name: "2 regions with utf8 split",
            region_edges: vec![bytes_handle("中文".as_bytes())],
            split_cnt: 4,
            binary_expected: vec![
                (Datum::Null, bytes("中文".as_bytes())),
                (bytes("中文".as_bytes()), Datum::Null),
            ],
            string_expected: vec![(Datum::Null, Datum::Null)],
        },
        Case {
            name: "several regions with mixed split",
            region_edges: vec![
                bytes_handle(b"abc"),
                bytes_handle(b"ab\x7f0"),
                bytes_handle(b"ab\xff0"),
                bytes_handle(b"ac\x001"),
                bytes_handle(b"ad\x0a1"),
                bytes_handle(b"ad23"),
                bytes_handle(b"ad230\xff"),
                bytes_handle(b"befh"),
                bytes_handle("中文".as_bytes()),
            ],
            split_cnt: 10,
            binary_expected: vec![
                (Datum::Null, bytes(b"abc")),
                (bytes(b"abc"), bytes(b"ab\x7f0")),
                (bytes(b"ab\x7f0"), bytes(b"ab\xff0")),
                (bytes(b"ab\xff0"), bytes(b"ac\x001")),
                (bytes(b"ac\x001"), bytes(b"ad\x0a1")),
                (bytes(b"ad\x0a1"), bytes(b"ad23")),
                (bytes(b"ad23"), bytes(b"ad230\xff")),
                (bytes(b"ad230\xff"), bytes(b"befh")),
                (bytes(b"befh"), bytes("中文".as_bytes())),
                (bytes("中文".as_bytes()), Datum::Null),
            ],
            string_expected: vec![
                (Datum::Null, text("abc")),
                (text("abc"), text("ac")),
                (text("ac"), text("ad\n1")),
                (text("ad\n1"), text("ad23")),
                (text("ad23"), text("ad230")),
                (text("ad230"), text("befh")),
                (text("befh"), Datum::Null),
            ],
        },
    ];

    // Go's table list, rebuilt from field types: binary(32), char(32) binary,
    // varchar(32) binary, bit(32), 2PK binary(32), varbinary(32), utf8mb4_bin,
    // utf8_bin, and a 2PK utf8mb4_0900_bin.
    let tbls = vec![
        ttl_table(51, binary_string(FieldTypeCode::String)),
        ttl_table(52, binary_string(FieldTypeCode::String)),
        ttl_table(53, binary_string(FieldTypeCode::Varchar)),
        ttl_table(54, FieldType::new(FieldTypeCode::Bit)),
        ttl_table_2pk(55, binary_string(FieldTypeCode::String)),
        ttl_table(56, binary_string(FieldTypeCode::VarString)),
        ttl_table(
            57,
            text_string(FieldTypeCode::String, "utf8mb4", "utf8mb4_bin"),
        ),
        ttl_table(58, text_string(FieldTypeCode::String, "utf8", "utf8_bin")),
        ttl_table_2pk(
            59,
            text_string(FieldTypeCode::String, "utf8mb4", "utf8mb4_0900_bin"),
        ),
    ];

    let mut store = MockRegionCache::default();
    for tbl in &tbls {
        for case in &cases {
            store.clear_regions();
            assert!(!case.region_edges.is_empty());
            for (i, edge) in case.region_edges.iter().enumerate() {
                if i == 0 {
                    store.add_region_begin_with_table_prefix(tbl.id, edge);
                } else {
                    store.add_region_with_table_prefix(tbl.id, &case.region_edges[i - 1], edge);
                }
            }
            store.add_region_end_with_table_prefix(
                &case.region_edges[case.region_edges.len() - 1],
                tbl.id,
            );
            let ranges = tbl.split_scan_ranges(Some(&store), case.split_cnt).unwrap();

            let key_tp = &tbl.key_column_types[0];
            let expected = if key_tp.code().mysql_type() == tidb_mysql::TypeBit
                || tidb_mysql::has_binary_flag(key_tp.flags() as usize)
            {
                &case.binary_expected
            } else {
                &case.string_expected
            };

            assert_eq!(
                ranges.len(),
                expected.len(),
                "tbl: {}, case: {}",
                tbl.id,
                case.name
            );
            for (i, range) in ranges.iter().enumerate() {
                check_range(
                    range,
                    &expected[i].0,
                    &expected[i].1,
                    &format!("tbl: {}, case: {}, i: {i}", tbl.id, case.name),
                );
            }
        }
    }
}

/// Go `TestNoTTLSplitSupportTables`.
#[test]
fn test_no_ttl_split_support_tables() {
    let tbls = vec![
        ttl_table(61, FieldType::new(FieldTypeCode::NewDecimal)),
        ttl_table(62, FieldType::new(FieldTypeCode::Date)),
        ttl_table(63, FieldType::new(FieldTypeCode::Datetime)),
        ttl_table(64, FieldType::new(FieldTypeCode::Timestamp)),
        ttl_table(
            65,
            text_string(FieldTypeCode::Varchar, "utf8mb4", "utf8mb4_general_ci"),
        ),
        ttl_table(
            66,
            text_string(FieldTypeCode::Varchar, "utf8mb4", "utf8mb4_0900_ai_ci"),
        ),
    ];

    let mut store = MockRegionCache::default();
    for tbl in &tbls {
        store.clear_regions();
        let ranges = tbl.split_scan_ranges(Some(&store), 4).unwrap();
        assert_eq!(ranges.len(), 1);
        check_range(&ranges[0], &Datum::Null, &Datum::Null, "one region");

        store.clear_regions();
        store.add_region(gen_table_prefix(tbl.id - 1), gen_table_prefix(tbl.id + 1));
        let ranges = tbl.split_scan_ranges(Some(&store), 4).unwrap();
        assert_eq!(ranges.len(), 1);
        check_range(&ranges[0], &Datum::Null, &Datum::Null, "shared region");

        store.clear_regions();
        store.add_region_begin_with_table_prefix(tbl.id, &bytes_handle(&[1, 2, 3]));
        store.add_region_with_table_prefix(
            tbl.id,
            &bytes_handle(&[1, 2, 3]),
            &bytes_handle(&[1, 2, 3, 4]),
        );
        store.add_region_end_with_table_prefix(&bytes_handle(&[1, 2, 3, 4]), tbl.id);
        let ranges = tbl.split_scan_ranges(Some(&store), 3).unwrap();
        assert_eq!(ranges.len(), 1);
        check_range(&ranges[0], &Datum::Null, &Datum::Null, "multi region");
    }
}

/// A store that is not a `tikv.Storage` yields one full range — Go's failed
/// type assertion.
#[test]
fn test_split_scan_ranges_without_a_tikv_store() {
    let tbl = ttl_table(71, signed(FieldTypeCode::LongLong));
    let ranges = tbl.split_scan_ranges(None, 4).unwrap();
    assert_eq!(ranges.len(), 1);
    check_range(&ranges[0], &Datum::Null, &Datum::Null, "non-tikv store");

    // `splitCnt <= 1` short-circuits the same way.
    let store = MockRegionCache::default();
    let ranges = tbl.split_scan_ranges(Some(&store), 1).unwrap();
    assert_eq!(ranges.len(), 1);
}

// -------------------------------------------------------------------------
// table_test.go
// -------------------------------------------------------------------------

fn column(id: i64, offset: i64, name: &str, field_type: FieldType) -> GoShared<ColumnInfo> {
    GoShared::new(ColumnInfo {
        id,
        offset,
        name: CiString::new(name),
        field_type,
        state: tidb_model::SchemaState::PUBLIC,
        ..Default::default()
    })
}

/// Go `TestNewTTLTable`, with the tables built directly rather than through
/// `CREATE TABLE` on a live store.
///
/// Go's `require.Same` on `TimeColumn` and `TableInfo` asserts Go pointer
/// identity; `TableInfo` keeps it here, while the key/time columns are owned
/// values (see the module's narrowing note), so those become value equality.
#[test]
fn test_new_ttl_table() {
    let time_col = column(2, 1, "t", FieldType::new(FieldTypeCode::Datetime));
    let ttl_info = GoShared::new(TTLInfo {
        column_name: CiString::new("t"),
        interval_expr_str: "2".to_owned(),
        interval_time_unit: 0,
        enable: true,
        job_interval: String::new(),
    });

    // `create table ttl1(a int, t datetime) ttl = t + interval 2 hour`:
    // no clustered key, so `_tidb_rowid` is the key column.
    let a_col = column(1, 0, "a", FieldType::new(FieldTypeCode::Long));
    let tbl = GoShared::new(TableInfo {
        id: 101,
        name: CiString::new("ttl1"),
        state: tidb_model::SchemaState::PUBLIC,
        columns: tidb_model::GoSharedPointerSlice::from_handles(vec![
            Some(a_col.clone()),
            Some(time_col.clone()),
        ]),
        ttl_info: Some(ttl_info.clone()),
        ..Default::default()
    });

    let ttl_tbl = new_physical_table(CiString::new("test"), &tbl, CiString::new("")).unwrap();
    assert_eq!(ttl_tbl.schema.original(), "test");
    assert!(ttl_tbl.table_info_ptr_eq(&tbl));
    assert_eq!(ttl_tbl.id, 101);
    assert_eq!(ttl_tbl.partition.lowercase(), "");
    assert!(ttl_tbl.partition_def.is_none());
    assert_eq!(ttl_tbl.key_columns.len(), 1);
    assert_eq!(ttl_tbl.key_column_types.len(), 1);
    assert_eq!(
        ttl_tbl.key_columns[0].name.lowercase(),
        tidb_model::column::EXTRA_HANDLE_NAME
    );
    assert_eq!(ttl_tbl.time_column.as_ref().unwrap().name.lowercase(), "t");
    assert_eq!(ttl_tbl.full_name(), "test.ttl1");

    // `create table ttl2(id int primary key, t datetime) ttl = ...`:
    // `pk_is_handle`, so the primary-key column is the key column.
    let id_col = column(1, 0, "id", {
        let mut ft = FieldType::new(FieldTypeCode::Long);
        ft.set_flags(tidb_mysql::PriKeyFlag as u32);
        ft
    });
    let tbl = GoShared::new(TableInfo {
        id: 102,
        name: CiString::new("ttl2"),
        state: tidb_model::SchemaState::PUBLIC,
        pk_is_handle: true,
        columns: tidb_model::GoSharedPointerSlice::from_handles(vec![
            Some(id_col),
            Some(time_col.clone()),
        ]),
        ttl_info: Some(ttl_info.clone()),
        ..Default::default()
    });
    let ttl_tbl = new_physical_table(CiString::new("test"), &tbl, CiString::new("")).unwrap();
    assert_eq!(ttl_tbl.key_columns.len(), 1);
    assert_eq!(ttl_tbl.key_columns[0].name.lowercase(), "id");

    // A table with no TTL info is rejected, as `t1` is in Go.
    let tbl = GoShared::new(TableInfo {
        id: 103,
        name: CiString::new("t1"),
        state: tidb_model::SchemaState::PUBLIC,
        columns: tidb_model::GoSharedPointerSlice::from_handles(vec![Some(a_col)]),
        ..Default::default()
    });
    let err = new_physical_table(CiString::new("test"), &tbl, CiString::new("")).unwrap_err();
    assert_eq!(err.to_string(), "table 'test.t1' is not a ttl table");

    // A TTL column that is not public is rejected too.
    let tbl = GoShared::new(TableInfo {
        id: 104,
        name: CiString::new("ttl3"),
        state: tidb_model::SchemaState::PUBLIC,
        columns: tidb_model::GoSharedPointerSlice::from_handles(vec![]),
        ttl_info: Some(ttl_info),
        ..Default::default()
    });
    let err = new_physical_table(CiString::new("test"), &tbl, CiString::new("")).unwrap_err();
    assert_eq!(
        err.to_string(),
        "time column 't' is not public in ttl table 'test.ttl3'"
    );
}

/// `ValidateKeyPrefix`, which `sqlbuilder` leans on.
#[test]
fn test_validate_key_prefix() {
    let tbl = ttl_table_2pk(81, signed(FieldTypeCode::LongLong));
    assert!(tbl.validate_key_prefix(&[Datum::new_int(1)]).is_ok());
    assert!(tbl
        .validate_key_prefix(&[Datum::new_int(1), Datum::new_int(2)])
        .is_ok());
    let err = tbl
        .validate_key_prefix(&[Datum::new_int(1), Datum::new_int(2), Datum::new_int(3)])
        .unwrap_err();
    assert_eq!(err.to_string(), "invalid key length: 3, expected 2");
}

// -------------------------------------------------------------------------
// task_test.go / ttlstatus_test.go
// -------------------------------------------------------------------------

/// A scripted result row.
struct MockRow {
    cells: Vec<Option<Cell>>,
}

enum Cell {
    Int(i64),
    Text(String),
    Bytes(Vec<u8>),
    Datetime(Time),
}

fn datetime(text: &str) -> Time {
    // A fixed, valid datetime; the exact instant is irrelevant to these
    // assertions, only that the column round-trips as a `Time`.
    let _ = text;
    Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap()
}

impl ResultRow for MockRow {
    fn is_null(&self, col_idx: usize) -> bool {
        self.cells.get(col_idx).is_none_or(Option::is_none)
    }
    fn get_int64(&self, col_idx: usize) -> i64 {
        match &self.cells[col_idx] {
            Some(Cell::Int(v)) => *v,
            other => panic!("column {col_idx} is not an int: {}", other.is_some()),
        }
    }
    fn get_string(&self, col_idx: usize) -> String {
        match &self.cells[col_idx] {
            Some(Cell::Text(v)) => v.clone(),
            _ => panic!("column {col_idx} is not text"),
        }
    }
    fn get_bytes(&self, col_idx: usize) -> Vec<u8> {
        match &self.cells[col_idx] {
            Some(Cell::Bytes(v)) => v.clone(),
            _ => panic!("column {col_idx} is not bytes"),
        }
    }
    fn get_time(&self, col_idx: usize) -> Time {
        match &self.cells[col_idx] {
            Some(Cell::Datetime(v)) => *v,
            _ => panic!("column {col_idx} is not a datetime"),
        }
    }
}

/// Go `TestRowToTTLTask`'s decoding half.
///
/// Go inserts a row through a live session and reads it back; the decoder is
/// what `task.go` owns, so the row is supplied directly. The `scan_range_*`
/// columns stay memcomparable-encoded here (see the module boundary), so the
/// assertion is on the raw bytes Go would then decode.
#[test]
fn test_row_to_ttl_task() {
    let now = datetime("2026-01-01 00:00:00");

    // Go's first assertion: NULL scan ranges.
    let row = MockRow {
        cells: vec![
            Some(Cell::Text("test-job".to_owned())),
            Some(Cell::Int(1)),
            Some(Cell::Int(1)),
            None,
            None,
            Some(Cell::Datetime(now)),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(Cell::Datetime(now)),
        ],
    };
    let task = row_to_ttl_task(&row).unwrap();
    assert_eq!(task.job_id, "test-job");
    assert_eq!(task.table_id, 1);
    assert_eq!(task.scan_id, 1);
    assert!(task.scan_range_start.is_none());
    assert!(task.scan_range_end.is_none());
    assert_eq!(task.expire_time, Some(now));
    assert_eq!(task.created_time, Some(now));
    assert_eq!(task.status, TaskStatus::default());

    // Go's second assertion: the ranges are updated to encoded `1` and `2`.
    let range_start = encode_key(&[Datum::new_int(1)]).expect("an int datum is always encodable");
    let range_end = encode_key(&[Datum::new_int(2)]).expect("an int datum is always encodable");
    let row = MockRow {
        cells: vec![
            Some(Cell::Text("test-job".to_owned())),
            Some(Cell::Int(1)),
            Some(Cell::Int(1)),
            Some(Cell::Bytes(range_start.clone())),
            Some(Cell::Bytes(range_end.clone())),
            Some(Cell::Datetime(now)),
            Some(Cell::Text("owner".to_owned())),
            Some(Cell::Text("addr".to_owned())),
            Some(Cell::Datetime(now)),
            // An empty status column defaults to "waiting".
            Some(Cell::Text(String::new())),
            Some(Cell::Datetime(now)),
            Some(Cell::Text("{\"total_rows\":3}".to_owned())),
            Some(Cell::Datetime(now)),
        ],
    };
    let task = row_to_ttl_task(&row).unwrap();
    assert_eq!(task.scan_range_start, Some(range_start));
    assert_eq!(task.scan_range_end, Some(range_end));
    assert_eq!(task.owner_id, "owner");
    assert_eq!(task.owner_addr, "addr");
    assert_eq!(task.status, TaskStatus(TaskStatus::WAITING.to_owned()));
    assert_eq!(task.state.as_deref(), Some("{\"total_rows\":3}"));

    // A non-NULL but empty range column stays unset, as Go's comment notes.
    let row = MockRow {
        cells: vec![
            Some(Cell::Text("j".to_owned())),
            Some(Cell::Int(1)),
            Some(Cell::Int(1)),
            Some(Cell::Bytes(Vec::new())),
            Some(Cell::Bytes(Vec::new())),
            Some(Cell::Datetime(now)),
            None,
            None,
            None,
            Some(Cell::Text("running".to_owned())),
            None,
            None,
            Some(Cell::Datetime(now)),
        ],
    };
    let task = row_to_ttl_task(&row).unwrap();
    assert!(task.scan_range_start.is_none());
    assert!(task.scan_range_end.is_none());
    assert_eq!(task.status, TaskStatus(TaskStatus::RUNNING.to_owned()));
}

/// `RowToTableStatus`'s per-column extraction, the part of
/// `TestTTLStatusCache`'s table-driven half that does not need a server.
#[test]
fn test_row_to_table_status() {
    let now = datetime("2026-01-01 00:00:00");
    let mut cells: Vec<Option<Cell>> = (0..17).map(|_| None).collect();
    cells[0] = Some(Cell::Int(1));
    cells[1] = Some(Cell::Int(2));
    cells[2] = Some(Cell::Text("test str".to_owned()));
    cells[3] = Some(Cell::Text("test job id".to_owned()));
    cells[4] = Some(Cell::Datetime(now));
    cells[7] = Some(Cell::Text("summary".to_owned()));
    cells[15] = Some(Cell::Text(String::new()));
    let row = MockRow { cells };

    let status = row_to_table_status(&row).unwrap();
    assert_eq!(status.table_id, 1);
    assert_eq!(status.parent_table_id, 2);
    assert_eq!(status.table_statistics, "test str");
    assert_eq!(status.last_job_id, "test job id");
    assert_eq!(status.last_job_start_time, Some(now));
    assert!(status.last_job_finish_time.is_none());
    assert_eq!(status.last_job_summary, "summary");
    // An empty status column defaults to "waiting".
    assert_eq!(
        status.current_job_status,
        JobStatus(JobStatus::WAITING.to_owned())
    );
    assert!(status.current_job_status_update_time.is_none());
}

/// The statement text and arguments of `task.go`'s builders, byte-exact.
#[test]
fn test_ttl_task_statements() {
    let (sql, args) = select_from_ttl_task_with_job_id("test-job");
    assert_eq!(sql, format!("{SELECT_FROM_TTL_TASK} WHERE job_id = %?"));
    assert_eq!(args, vec![SqlArg::Str("test-job".to_owned())]);

    let (sql, args) = select_from_ttl_task_with_id("test-job", 7);
    assert_eq!(
        sql,
        format!("{SELECT_FROM_TTL_TASK} WHERE job_id = %? AND scan_id = %?")
    );
    assert_eq!(
        args,
        vec![SqlArg::Str("test-job".to_owned()), SqlArg::Int(7)]
    );

    let (sql, args) = peek_waiting_ttl_task("2026-01-01 00:00:00");
    // Spelled out rather than re-derived, so the suffix is checked against Go's
    // literal and not against the same expression that produced it.
    assert_eq!(
        &sql[SELECT_FROM_TTL_TASK.len()..],
        " WHERE status = 'waiting' OR (owner_hb_time < %? AND status = 'running') ORDER BY created_time ASC"
    );
    assert!(sql.starts_with(SELECT_FROM_TTL_TASK));
    assert_eq!(args, vec![SqlArg::Str("2026-01-01 00:00:00".to_owned())]);

    assert!(SELECT_FROM_TTL_TASK.starts_with("SELECT LOW_PRIORITY\n\tjob_id,"));
    assert!(SELECT_FROM_TTL_TASK.ends_with("\tcreated_time FROM mysql.tidb_ttl_task"));

    let (sql, args) = select_from_ttl_table_status_with_id(9);
    assert!(sql.ends_with(" WHERE table_id = %?"));
    assert_eq!(args, vec![9]);
}

// -------------------------------------------------------------------------
// table_test.go: expiry evaluation
// -------------------------------------------------------------------------

/// Go's `time.ParseInLocation(time.DateTime, text, loc)`.
///
/// Go resolves an ambiguous local time (a fall-back repeated hour) to the
/// FIRST of the two instants, which is `earliest()` here.
fn parse_in_location<Tz: chrono::TimeZone>(text: &str, zone: &Tz) -> chrono::DateTime<Tz> {
    let naive = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
        .expect("the fixture text is a valid datetime");
    zone.from_local_datetime(&naive)
        .earliest()
        .expect("the fixture instant exists in the fixture zone")
}

/// Go's `t.Format(time.DateTime)`.
fn format_date_time<Tz: chrono::TimeZone>(value: &chrono::DateTime<Tz>) -> String
where
    Tz::Offset: std::fmt::Display,
{
    value.format("%Y-%m-%d %H:%M:%S").to_string()
}

/// Go `TestEvalTTLExpireTime`.
///
/// Go's `require.Same(t, tz, tm.Location())` asserts that the returned
/// `time.Time` kept the argument's `*time.Location` pointer; `chrono` has no
/// pointer to compare, so each case asserts the equivalent
/// `tm.timezone() == tz`.
#[test]
fn test_eval_ttl_expire_time() {
    let tz_shanghai = chrono_tz::Asia::Shanghai;
    let tz_berlin = chrono_tz::Europe::Berlin;

    let epoch = |zone: chrono_tz::Tz| zone.timestamp_millis_opt(0).single().unwrap();

    let tm = eval_expire_time(&epoch(tz_shanghai), "1", TimeUnitType::Day).unwrap();
    assert_eq!(tm.timestamp(), -86400);
    assert_eq!(format_date_time(&tm), "1969-12-31 08:00:00");
    assert_eq!(tm.timezone(), tz_shanghai);

    let tm = eval_expire_time(&epoch(tz_berlin), "1", TimeUnitType::Day).unwrap();
    assert_eq!(tm.timestamp(), -86400);
    assert_eq!(
        format_date_time(&tm.with_timezone(&tz_berlin)),
        "1969-12-31 01:00:00"
    );
    assert_eq!(tm.timezone(), tz_berlin);

    let tm = eval_expire_time(&epoch(tz_shanghai), "3", TimeUnitType::Month).unwrap();
    assert_eq!(
        format_date_time(&tm.with_timezone(&tz_shanghai)),
        "1969-10-01 08:00:00"
    );
    assert_eq!(tm.timezone(), tz_shanghai);

    let tm = eval_expire_time(&epoch(tz_berlin), "3", TimeUnitType::Month).unwrap();
    assert_eq!(
        format_date_time(&tm.with_timezone(&tz_berlin)),
        "1969-10-01 01:00:00"
    );
    assert_eq!(tm.timezone(), tz_berlin);

    // test cases for daylight saving time.
    // When local standard time was about to reach Sunday, 10 March 2024,
    // 02:00:00 clocks were turned forward 1 hour to Sunday, 10 March 2024,
    // 03:00:00 local daylight time instead.
    let tz_los_angeles = chrono_tz::America::Los_Angeles;
    let now = parse_in_location("2024-03-11 19:49:59", &tz_los_angeles);
    let tm = eval_expire_time(&now, "90", TimeUnitType::Minute).unwrap();
    assert_eq!(format_date_time(&tm), "2024-03-11 18:19:59");
    assert_eq!(tm.timezone(), tz_los_angeles);

    // across day light-saving time
    let now = parse_in_location("2024-03-10 03:01:00", &tz_los_angeles);
    let tm = eval_expire_time(&now, "90", TimeUnitType::Minute).unwrap();
    assert_eq!(format_date_time(&tm), "2024-03-10 00:31:00");
    assert_eq!(tm.timezone(), tz_los_angeles);

    let now = parse_in_location("2024-03-10 04:01:00", &tz_los_angeles);
    let tm = eval_expire_time(&now, "90", TimeUnitType::Minute).unwrap();
    assert_eq!(format_date_time(&tm), "2024-03-10 01:31:00");
    assert_eq!(tm.timezone(), tz_los_angeles);

    let now = parse_in_location("2024-11-03 03:00:00", &tz_los_angeles);
    let tm = eval_expire_time(&now, "90", TimeUnitType::Minute).unwrap();
    assert_eq!(format_date_time(&tm), "2024-11-03 01:30:00");
    assert_eq!(tm.timezone(), tz_los_angeles);
    // 2024-11-03 01:30:00 in America/Los_Angeles has two related time points:
    // 2024-11-03 01:30:00 -0700 PDT
    // 2024-11-03 01:30:00 -0800 PST
    // We must use the earlier one to avoid deleting some unexpected rows.
    assert_eq!(now.timestamp() - tm.timestamp(), 5400);

    // time should be truncated to second to make the result simple
    let now = chrono::Utc
        .from_local_datetime(
            &chrono::NaiveDateTime::parse_from_str(
                "2023-01-02 15:00:01.986542",
                "%Y-%m-%d %H:%M:%S%.f",
            )
            .unwrap(),
        )
        .single()
        .unwrap();
    let tm = eval_expire_time(&now, "1", TimeUnitType::Day).unwrap();
    assert_eq!(
        tm.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
        "2023-01-01 15:00:01.000000"
    );
    assert_eq!(tm.timezone(), chrono::Utc);

    // test for string interval format
    let tm = eval_expire_time(&epoch(tz_berlin), "'1:3'", TimeUnitType::HourMinute).unwrap();
    assert_eq!(
        format_date_time(&tm.with_timezone(&chrono::Utc)),
        "1969-12-31 22:57:00"
    );
    assert_eq!(tm.timezone(), tz_berlin);
}

/// A `sessionctx.Context` whose only scripted answer is the global
/// `time_zone`, which is all `(*PhysicalTable).EvalExpireTime` reads.
struct GlobalTimeZoneContext {
    global_time_zone: &'static str,
    killer: tidb_util::sqlkiller::SqlKiller,
}

impl GlobalTimeZoneContext {
    fn new(global_time_zone: &'static str) -> Self {
        Self {
            global_time_zone,
            killer: tidb_util::sqlkiller::SqlKiller::default(),
        }
    }
}

impl tidb_ttl::session::SessionContext for GlobalTimeZoneContext {
    type Row = NoRow;
    type Store = ();
    type InfoSchema = ();

    fn get_store(&self) {}
    fn get_latest_info_schema(&self) {}
    fn get_txn_info_schema(&self) {}

    fn execute_internal(
        &self,
        _sql: &str,
        _args: &[Datum],
    ) -> Result<Option<Vec<NoRow>>, tidb_ttl::session::SessionError> {
        Ok(None)
    }

    fn get_global_system_var(
        &self,
        _name: &str,
    ) -> Result<String, tidb_ttl::session::SessionError> {
        Ok(self.global_time_zone.to_owned())
    }

    fn get_session_or_global_system_var(
        &self,
        _name: &str,
    ) -> Result<String, tidb_ttl::session::SessionError> {
        Ok(self.global_time_zone.to_owned())
    }

    fn time_zone(&self) -> Option<tidb_util::timeutil::TimeZone> {
        None
    }

    fn location(&self) -> tidb_util::timeutil::TimeZone {
        tidb_util::timeutil::parse_time_zone(self.global_time_zone).unwrap()
    }

    fn sql_killer(&self) -> &tidb_util::sqlkiller::SqlKiller {
        &self.killer
    }
}

/// A `chunk.Row` this fixture never reads.
struct NoRow;

impl ResultRow for NoRow {
    fn is_null(&self, _col_idx: usize) -> bool {
        true
    }
    fn get_int64(&self, _col_idx: usize) -> i64 {
        0
    }
    fn get_string(&self, _col_idx: usize) -> String {
        String::new()
    }
    fn get_bytes(&self, _col_idx: usize) -> Vec<u8> {
        Vec::new()
    }
    fn get_time(&self, _col_idx: usize) -> Time {
        Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap()
    }
}

/// A TTL table whose `TTLInfo` carries one interval expression and unit.
fn ttl_interval_table(id: i64, interval: &str, unit: TimeUnitType) -> PhysicalTable {
    let time_col = column(2, 1, "t", FieldType::new(FieldTypeCode::Datetime));
    let tbl = GoShared::new(TableInfo {
        id,
        name: CiString::new("t"),
        state: tidb_model::SchemaState::PUBLIC,
        columns: tidb_model::GoSharedPointerSlice::from_handles(vec![
            Some(column(1, 0, "a", FieldType::new(FieldTypeCode::Long))),
            Some(time_col),
        ]),
        ttl_info: Some(GoShared::new(TTLInfo {
            column_name: CiString::new("t"),
            interval_expr_str: interval.to_owned(),
            interval_time_unit: unit as i64,
            enable: true,
            job_interval: String::new(),
        })),
        ..Default::default()
    });
    new_physical_table(CiString::new("test"), &tbl, CiString::new("")).unwrap()
}

/// The session-independent half of Go `TestTableEvalTTLExpireTime`.
///
/// `// boundary:` the other half. Go drives the global zone with
/// `SET @@global.time_zone = '+02:00'` on a live `testkit` session and closes
/// by asserting `select @@time_zone` still reports the SESSION zone
/// (`Asia/Tokyo`) — that the global-zone read did not leak into the session.
/// Both need a real server. What is asserted here is everything
/// `(*PhysicalTable).EvalExpireTime` itself owns: that it computes against the
/// GLOBAL zone rather than the `now` argument's, that it returns in the `now`
/// argument's zone, that a string-format interval works, and that the mock
/// override short-circuits it.
#[test]
fn test_table_eval_ttl_expire_time() {
    // the global timezone set to +02:00
    let tz1 = chrono::FixedOffset::east_opt(2 * 3600).unwrap();
    let se = TtlSession::new(GlobalTimeZoneContext::new("+02:00"), Box::new(|| {}));

    // `create table test.t(a int, t datetime) ttl = `t` + interval 1 month`
    let ttl_tbl = ttl_interval_table(101, "1", TimeUnitType::Month);

    // the timezone of now argument is set to -02:00
    let tz2 = chrono::FixedOffset::east_opt(-2 * 3600).unwrap();
    let now = parse_in_location("1999-02-28 23:00:00", &tz2);
    let tm = ttl_tbl
        .eval_expire_time(&MockExpireTimeKey::default(), &se, &now)
        .unwrap();
    // The expired time should be calculated according to the global time zone
    assert_eq!(
        format_date_time(&tm.with_timezone(&tz1)),
        "1999-02-01 03:00:00"
    );
    // The location of the expired time should be the same as the input
    // argument `now`
    assert_eq!(tm.timezone(), tz2);

    // should support a string format interval
    // `create table test.t2(a int, t datetime) ttl = `t` + interval '1:3' hour_minute`
    let ttl_tbl2 = ttl_interval_table(102, "'1:3'", TimeUnitType::HourMinute);
    let now = parse_in_location("2020-01-01 15:00:00", &tz1);
    let tm = ttl_tbl2
        .eval_expire_time(&MockExpireTimeKey::default(), &se, &now)
        .unwrap();
    assert_eq!(format_date_time(&tm), "2020-01-01 13:57:00");
    assert_eq!(tm.timezone(), tz1);

    // Go `SetMockExpireTime`: the context value short-circuits every read
    // above, including the session's.
    let mocked = parse_in_location("2021-06-07 08:09:10", &tz1);
    let ctx = set_mock_expire_time(&MockExpireTimeKey::default(), mocked);
    let tm = ttl_tbl2.eval_expire_time(&ctx, &se, &now).unwrap();
    assert_eq!(format_date_time(&tm), "2021-06-07 08:09:10");
}
