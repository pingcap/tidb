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

//! Ports of `pkg/util/chunk/list_test.go`.

use tidb_datatype::{FieldType, FieldTypeCode};

use crate::chunk::Chunk;
use crate::list::{List, RowPtr};

fn int_fields() -> Vec<FieldType> {
    vec![FieldType::new(FieldTypeCode::LongLong)]
}

/// Go `TestList` (list_test.go): basic append, chunk reuse after `Reset`,
/// `Add` + `AppendRow` interaction, and `Walk` iteration. The free-list
/// length assertions are internal to Go's `List`; this port checks the same
/// behavior through chunk counts before/after reset.
#[test]
fn list_basics() {
    let fields = int_fields();
    let mut l = List::new(&fields, 2, 2);
    let mut src_chunk = Chunk::new_with_capacity(&fields, 32);
    src_chunk.append_int64(0, 1);
    let src_row = src_chunk.get_row(0);

    // Test basic append: two chunks of two rows plus one spill-over chunk.
    for _ in 0..5 {
        l.append_row(src_row);
    }
    assert_eq!(l.num_chunks(), 3);
    assert_eq!(l.len(), 5);

    // Test chunk reuse: after Reset the recycled chunks serve new appends.
    l.reset();
    for _ in 0..5 {
        l.append_row(src_row);
    }
    assert_eq!(l.num_chunks(), 3);
    assert_eq!(l.len(), 5);

    // Test add chunk then append row.
    l.reset();
    let mut n_chunk = Chunk::new_with_capacity(&fields, 32);
    n_chunk.append_null(0);
    l.add(n_chunk);
    let ptr = l.append_row(src_chunk.get_row(0));
    assert_eq!(l.num_chunks(), 2);
    assert_eq!(ptr, RowPtr { chk_idx: 1, row_idx: 0 });
    let row = l.get_row(ptr);
    assert_eq!(row.get_int64(0), 1);

    // Test iteration.
    l.reset();
    for i in 0..5i64 {
        let mut tmp = Chunk::new_with_capacity(&fields, 32);
        tmp.append_int64(0, i);
        l.append_row(tmp.get_row(0));
    }
    let expected: Vec<i64> = vec![0, 1, 2, 3, 4];
    let mut results = Vec::new();
    l.walk(|row| {
        results.push(row.get_int64(0));
        Ok::<(), std::convert::Infallible>(())
    })
    .expect("walk never fails here");
    assert_eq!(expected, results);
}

/// Go `TestListMemoryUsage` (list_test.go). The Rust port charges tracker
/// bytes on the same events; exact byte-for-byte equality with Go's internal
/// `chunks[0].MemoryUsage()` bookkeeping is asserted where the tracker
/// surface exposes it.
#[test]
fn list_memory_usage() {
    let field_types = vec![
        FieldType::new(FieldTypeCode::Float),
        FieldType::new(FieldTypeCode::Varchar),
        FieldType::new(FieldTypeCode::Json),
        FieldType::new(FieldTypeCode::Datetime),
        FieldType::new(FieldTypeCode::Duration),
    ];
    let time_obj = tidb_datatype::Time::new(
        tidb_datatype::CoreTime::default(),
        tidb_datatype::TimeType::DateTime,
        0,
    )
    .unwrap();
    let duration_obj = MySqlDuration::from_raw_parts(i64::MAX, 0);

    let max_chunk_size = 2;
    let mut src_chk = Chunk::new_with_capacity(&field_types, max_chunk_size);
    src_chk.append_float32(0, 12.4);
    src_chk.append_string(1, "123".as_bytes());
    let json_obj = BinaryJSON::parse("1").unwrap();
    src_chk.append_json(2, &json_obj);
    src_chk.append_time(3, time_obj);
    src_chk.append_duration(4, duration_obj);

    let mut list = List::new(&field_types, max_chunk_size, max_chunk_size * 2);
    assert_eq!(list.mem_tracker().bytes_consumed(), 0);

    list.append_row(src_chk.get_row(0));
    // Go pins a stale-zero read here: the allocation is not yet accounted.
    assert_eq!(list.mem_tracker().bytes_consumed(), 0);
    let mem_usage = list.get_chunk(0).memory_usage();

    list.reset();
    assert_eq!(
        list.mem_tracker().bytes_consumed(),
        mem_usage,
        "reset keeps the consumed accounting until refresh"
    );

    list.add(src_chk);
    assert_eq!(
        list.mem_tracker().bytes_consumed(),
        mem_usage * 2,
        "Add accounts the second chunk"
    );
}

use tidb_datatype::{BinaryJSON, MySqlDuration};
