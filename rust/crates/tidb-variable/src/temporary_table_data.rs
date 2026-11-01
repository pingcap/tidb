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

//! Go `pkg/sessionctx/variable/session.go`'s `TemporaryTableData`: the
//! committed rows of the session's LOCAL temporary tables, outside any
//! transaction (`SessionVars.TemporaryTableData`).

use std::collections::HashMap;

use tidb_kv::storage::{MemTableStorage, StagingHandle, StorageError, StorageIterator, TableStorage};
use tidb_txnkv::Key;

/// Go `TemporaryTableData`: a MemBuffer of temporary-table keys plus each
/// table's size in it.
#[derive(Clone, Debug, Default)]
pub struct TemporaryTableData {
    mem_buffer: MemTableStorage,
    tbl_size: HashMap<i64, i64>,
}

impl TemporaryTableData {
    /// Go `NewTemporaryTableData`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `kv.Retriever.Get`: the value stored under `key`, which may be the
    /// empty value a deletion leaves.
    pub fn get(&mut self, key: &Key) -> Result<Vec<u8>, StorageError> {
        self.mem_buffer.get(key)
    }

    /// Go `kv.Retriever.Iter`.
    pub fn iter(
        &mut self,
        start: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Box<dyn StorageIterator>, StorageError> {
        self.mem_buffer.iter(start, upper_bound)
    }

    /// Go `Staging`.
    pub fn staging(&mut self) -> StagingHandle {
        self.mem_buffer.staging()
    }

    /// Go `Release`.
    pub fn release(&mut self, handle: StagingHandle) {
        self.mem_buffer.release(handle);
    }

    /// Go `Cleanup`.
    pub fn cleanup(&mut self, handle: StagingHandle) {
        self.mem_buffer.cleanup(handle);
    }

    /// Go `GetTableSize`.
    #[must_use]
    pub fn get_table_size(&self, tbl_id: i64) -> i64 {
        self.tbl_size.get(&tbl_id).copied().unwrap_or(0)
    }

    /// Go `DeleteTableKey`. Like Go's `MemBuffer.Delete`, the key keeps an
    /// empty value, which readers treat as absent.
    pub fn delete_table_key(&mut self, tbl_id: i64, k: Key) -> Result<(), StorageError> {
        let buffer_size = self.size();
        let result = self.mem_buffer.set(k, Vec::new());
        self.update_tbl_size(tbl_id, buffer_size);
        result
    }

    /// Go `SetTableKey`.
    pub fn set_table_key(&mut self, tbl_id: i64, k: Key, val: Vec<u8>) -> Result<(), StorageError> {
        let buffer_size = self.size();
        let result = self.mem_buffer.set(k, val);
        self.update_tbl_size(tbl_id, buffer_size);
        result
    }

    fn size(&self) -> i64 {
        i64::try_from(self.mem_buffer.size_bytes()).unwrap_or(i64::MAX)
    }

    fn update_tbl_size(&mut self, tbl_id: i64, before_size: i64) {
        let delta = self.size() - before_size;
        let size = self.get_table_size(tbl_id) + delta;
        self.tbl_size.insert(tbl_id, size);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(bytes: &[u8]) -> Key {
        Key::from_bytes(bytes.to_vec())
    }

    #[test]
    fn table_sizes_follow_set_and_delete() {
        let mut data = TemporaryTableData::new();
        data.set_table_key(1, key(b"t1_a"), vec![1, 2, 3]).unwrap();
        data.set_table_key(2, key(b"t2_a"), vec![1]).unwrap();
        let one = data.get_table_size(1);
        assert!(one > 0);
        assert!(data.get_table_size(2) > 0);
        assert_eq!(data.get_table_size(3), 0);
        data.delete_table_key(1, key(b"t1_a")).unwrap();
        assert!(data.get_table_size(1) < one);
        assert!(data.get(&key(b"t1_a")).unwrap().is_empty());
    }

    #[test]
    fn a_cleaned_up_stage_leaves_the_data_unchanged() {
        let mut data = TemporaryTableData::new();
        data.set_table_key(1, key(b"t1_a"), vec![1]).unwrap();
        let stage = data.staging();
        data.set_table_key(1, key(b"t1_b"), vec![2]).unwrap();
        data.cleanup(stage);
        assert!(matches!(data.get(&key(b"t1_b")), Err(StorageError::NotFound)));
        let stage = data.staging();
        data.set_table_key(1, key(b"t1_c"), vec![3]).unwrap();
        data.release(stage);
        assert_eq!(data.get(&key(b"t1_c")).unwrap(), vec![3]);
    }
}
