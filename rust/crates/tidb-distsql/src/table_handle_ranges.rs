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

//! Sorted table handles to half-open KV ranges, translated from
//! `pkg/distsql/request_builder.go::TableHandlesToKVRanges`.

use tidb_codec::{encode_int, encode_row_key};
use tidb_txnkv::{Handle, Key};

use crate::RequestKeyRange;

/// Converts sorted handles into canonical row-key ranges and row-count hints.
///
/// Adjacent integer handles in one physical table are coalesced. Common
/// handles remain point ranges because byte adjacency does not imply logical
/// row adjacency. The source requires a sorted, same-domain input; the typed
/// `Handle` enum removes interface assertions while preserving that contract.
#[must_use]
pub fn table_handles_to_kv_ranges(
    mut table_id: i64,
    handles: &[Handle],
) -> (Vec<RequestKeyRange>, Vec<usize>) {
    let mut ranges = Vec::with_capacity(handles.len());
    let mut hints = Vec::with_capacity(handles.len());
    let mut index = 0;

    while index < handles.len() {
        let logical = logical_handle(&handles[index], &mut table_id);
        if let Handle::Common(common) = logical {
            let start_key = encode_row_key(table_id, common.encoded());
            let end_handle = Key::from_bytes(common.encoded()).next();
            ranges.push(RequestKeyRange {
                start_key: start_key.into(),
                end_key: encode_row_key(table_id, end_handle.as_bytes()).into(),
            });
            hints.push(1);
            index += 1;
            continue;
        }

        let mut end = index + 1;
        while end < handles.len() {
            let previous = handles[end - 1]
                .int_value()
                .expect("sorted integer-handle run");
            if previous == i64::MAX {
                break;
            }
            if let Handle::Partition(partition) = &handles[end] {
                if partition.partition_id() != table_id {
                    break;
                }
            }
            let current = handles[end].int_value().expect("sorted integer-handle run");
            if current != previous + 1 {
                break;
            }
            end += 1;
        }

        let low = handles[index]
            .int_value()
            .expect("integer range starts with integer handle");
        let high = handles[end - 1]
            .int_value()
            .expect("integer range ends with integer handle");
        let mut low_encoded = Vec::with_capacity(8);
        encode_int(&mut low_encoded, low);
        let mut high_encoded = Vec::with_capacity(8);
        encode_int(&mut high_encoded, high);
        let high_exclusive = Key::from_bytes(high_encoded).prefix_next();
        ranges.push(RequestKeyRange {
            start_key: encode_row_key(table_id, &low_encoded).into(),
            end_key: encode_row_key(table_id, high_exclusive.as_bytes()).into(),
        });
        hints.push(end - index);
        index = end;
    }

    (ranges, hints)
}

fn logical_handle<'a>(handle: &'a Handle, table_id: &mut i64) -> &'a Handle {
    match handle {
        Handle::Partition(partition) => {
            *table_id = partition.partition_id();
            partition.inner()
        }
        handle => handle,
    }
}
