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

use std::collections::BTreeMap;

use tidb_txnkv::Key;

/// Go `kv.MemBuffer.SnapshotGetter()` / `SnapshotIter` as `UnionScanExec`
/// reads them: the transaction's buffered entries of the scanned tables as
/// the statement found them -- copied when the executor is built, before the
/// statement writes anything, so `INSERT INTO t SELECT ... FROM t` reads the
/// rows it started from. A tombstone is an empty value, as in the buffer.
///
/// Go's snapshot is the buffer's first staging, which a statement opens as it
/// starts. This buffer also stages user savepoints, so its first staging is
/// not the statement's; the copy taken at the statement's start is.
#[derive(Debug, Default)]
pub struct MemBufferSnapshot {
    pub entries: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemBufferSnapshot {
    /// Go `kv.Getter.Get`: `None` is `kv.ErrNotExist`; a tombstone is
    /// `Some(empty)`.
    pub fn get(&self, key: &Key) -> Option<Vec<u8>> {
        self.entries.get(key.as_bytes()).cloned()
    }

    /// Go `SnapshotIter(start, end)` (or `SnapshotIterReverse(end, start)`),
    /// as the ordered pairs it yields, tombstones included.
    pub fn entries(&self, start: &Key, end: &Key, reverse: bool) -> Vec<(Key, Vec<u8>)> {
        if start.as_bytes() >= end.as_bytes() {
            return Vec::new();
        }
        let range = self
            .entries
            .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
            .map(|(key, value)| (Key::from_bytes(key.clone()), value.clone()));
        if reverse {
            range.rev().collect()
        } else {
            range.collect()
        }
    }

    /// Whether the copy holds nothing at all.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
