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

use tidb_codec::encode_int_to_cmp_uint;
use tidb_tablecodec::table_key::encode_row_key;
use tidb_txnkv::{Handle, KeyRange};

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
) -> (Vec<KeyRange>, Vec<usize>) {
    let mut ranges = Vec::with_capacity(handles.len());
    let mut hints = Vec::with_capacity(handles.len());
    let mut index = 0;

    while index < handles.len() {
        let logical = logical_handle(&handles[index], &mut table_id);
        if let Handle::Common(common) = logical {
            let start_key = encode_row_key(table_id, common.encoded());
            // Key.Next appends a zero byte to the common handle's encoding.
            let mut end_key = Vec::with_capacity(start_key.len() + 1);
            end_key.extend_from_slice(&start_key);
            end_key.push(0);
            ranges.push(KeyRange {
                start_key: start_key.into(),
                end_key: end_key.into(),
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
        let low_encoded = encode_int_to_cmp_uint(low).to_be_bytes();
        let end_key = if let Some(next) = high.checked_add(1) {
            encode_row_key(table_id, &encode_int_to_cmp_uint(next).to_be_bytes())
        } else {
            // Key.PrefixNext appends a zero byte when the handle is all 0xff.
            let mut high_exclusive = [0xff; 9];
            high_exclusive[8] = 0;
            encode_row_key(table_id, &high_exclusive)
        };
        ranges.push(KeyRange {
            start_key: encode_row_key(table_id, &low_encoded).into(),
            end_key: end_key.into(),
        });
        hints.push(end - index);
        index = end;
    }

    (ranges, hints)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_txnkv::{Handle, IntHandle};

    #[test]
    fn duplicate_handles_keep_go_point_ranges_and_hints() {
        // Go `TableHandlesToKVRanges` (`pkg/distsql/request_builder.go:626-674`)
        // receives a sorted handle list and does not collapse equal entries.
        // The duplicate point ranges preserve one table row per non-unique
        // index occurrence; only strictly consecutive handles are coalesced.
        let handles = [
            Handle::from(IntHandle::new(5)),
            Handle::from(IntHandle::new(5)),
            Handle::from(IntHandle::new(7)),
            Handle::from(IntHandle::new(8)),
        ];
        let (ranges, hints) = table_handles_to_kv_ranges(91, &handles);
        assert_eq!(ranges.len(), 3);
        assert_eq!(hints, vec![1, 1, 2]);
        assert_eq!(ranges[0], ranges[1]);
    }

    #[test]
    fn max_integer_handle_keeps_the_zero_suffix_at_the_exclusive_end() {
        let handles = [
            Handle::from(IntHandle::new(i64::MAX - 1)),
            Handle::from(IntHandle::new(i64::MAX)),
        ];
        let (ranges, hints) = table_handles_to_kv_ranges(91, &handles);
        assert_eq!(hints, [2]);
        assert_eq!(
            ranges[0].end_key.as_bytes(),
            encode_row_key(91, &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0])
        );
    }
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
