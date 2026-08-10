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

//! Public contract for `pkg/util/chunk/list.go`.

use std::panic::{catch_unwind, AssertUnwindSafe};

use tidb_chunk::chunk::Chunk;
use tidb_chunk::list::{List, RowPtr, ROW_PTR_SIZE};
use tidb_datatype::{FieldType, FieldTypeCode};

fn int_chunk(fields: &[FieldType], values: &[i64]) -> Chunk {
    let mut chunk = Chunk::new_with_capacity(fields, values.len().max(1));
    for value in values {
        chunk.append_int64(0, *value);
    }
    chunk
}

#[test]
fn list_public_contract() {
    let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
    let mut list = tidb_chunk::list::List::new(&fields, 2, 2);

    assert_eq!(ROW_PTR_SIZE, 8);
    assert!(list.is_empty());
    assert_eq!(list.len(), 0);
    assert_eq!(list.num_chunks(), 0);
    assert_eq!(list.field_types(), fields);
    assert_eq!(list.mem_tracker().bytes_consumed(), 0);
    let mut empty_walk_calls = 0;
    list.walk(|_| {
        empty_walk_calls += 1;
        Ok::<(), ()>(())
    })
    .expect("an empty walk succeeds");
    assert_eq!(empty_walk_calls, 0);

    let source = int_chunk(&fields, &[11, 22, 33]);
    let first = list.append_row(source.get_row(0));
    let second = list.append_row(source.get_row(1));
    let third = list.append_row(source.get_row(2));
    assert_eq!(first, RowPtr::new(0, 0));
    assert_eq!(second, RowPtr::new(0, 1));
    assert_eq!(third, RowPtr::new(1, 0));
    assert_eq!(list.len(), 3);
    assert_eq!(list.num_chunks(), 2);
    assert_eq!(list.num_rows_of_chunk(0), 2);
    assert_eq!(list.num_rows_of_chunk(1), 1);
    assert_eq!(list.get_row(second).get_int64(0), 22);
    assert!(list.mem_tracker().bytes_consumed() > 0);

    let mut all_rows = Vec::new();
    list.walk(|row| {
        all_rows.push(row.get_int64(0));
        Ok::<(), ()>(())
    })
    .expect("a complete walk succeeds");
    assert_eq!(all_rows, [11, 22, 33]);

    let mut visited = Vec::new();
    let stopped = list.walk(|row| {
        visited.push(row.get_int64(0));
        if visited.len() == 2 {
            Err("stop")
        } else {
            Ok(())
        }
    });
    assert_eq!(stopped, Err("stop"));
    assert_eq!(visited, [11, 22]);

    list.get_chunk_mut(1).column_mut(0).set_null(0, true);
    assert!(!list.get_row(first).is_null(0));
    assert!(list.get_row(third).is_null(0));

    let mut added = Chunk::new_with_capacity(&fields, 4);
    added.append_int64(0, 44);
    list.add(added);
    assert_eq!(list.num_chunks(), 3);
    assert_eq!(list.len(), 4);
    let after_added_chunk = list.append_row(source.get_row(0));
    assert_eq!(after_added_chunk, RowPtr::new(3, 0));
    assert_eq!(list.get_row(after_added_chunk).get_int64(0), 11);

    let charged_before_reset = list.mem_tracker().bytes_consumed();
    list.reset();
    assert!(list.is_empty());
    assert_eq!(list.num_chunks(), 0);
    assert!(list.mem_tracker().bytes_consumed() > charged_before_reset);

    let charged_after_reset = list.mem_tracker().bytes_consumed();
    let reused = list.alloc_chunk();
    assert_eq!(reused.num_rows(), 0);
    assert!(list.mem_tracker().bytes_consumed() < charged_after_reset);
    list.clear();
    assert_eq!(list.mem_tracker().bytes_consumed(), 0);

    let mut empty_rejection = List::new(&fields, 1, 1);
    let panic = catch_unwind(AssertUnwindSafe(|| {
        empty_rejection.add(Chunk::new_with_capacity(&fields, 1));
    }));
    assert!(panic.is_err());
}
