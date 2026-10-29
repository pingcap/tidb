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

//! A scripted cluster snapshot over the cluster storage seam, for the tests
//! of the layers built on it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tidb_txnkv::Key;

use crate::cluster_storage::*;
use tidb_kv::cluster_storage::{ClusterSnapshot, SnapshotPairs};
use tidb_kv::storage::StorageError;

/// A snapshot that answers from a fixed map and counts what the storage
/// asked it for, so a test can prove a read never reached the cluster.
#[derive(Debug, Default)]
pub struct MockSnapshot {
    pub data: BTreeMap<Vec<u8>, Vec<u8>>,
    pub gets: Vec<Vec<u8>>,
    pub scans: Vec<(Vec<u8>, Vec<u8>)>,
    /// Every pair this snapshot handed back, summed over all scans.
    pub rows_read: usize,
    pub fail_with: Option<StorageError>,
    pub index_gets: usize,
    pub fail_index_get: Option<(usize, StorageError)>,
}

impl ClusterSnapshot for MockSnapshot {
    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        if tidb_tablecodec::table_key::is_index_key(key.as_bytes()) {
            self.index_gets += 1;
            if self
                .fail_index_get
                .as_ref()
                .is_some_and(|(at, _)| *at == self.index_gets)
            {
                return Err(self.fail_index_get.take().unwrap().1);
            }
        }
        if let Some(error) = self.fail_with.clone() {
            return Err(error);
        }
        self.gets.push(key.as_bytes().to_vec());
        Ok(self.data.get(key.as_bytes()).cloned())
    }

    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        if let Some(error) = self.fail_with.clone() {
            return Err(error);
        }
        self.scans
            .push((start.as_bytes().to_vec(), end.as_bytes().to_vec()));
        let pairs: SnapshotPairs = self
            .data
            .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
            .take(limit.unwrap_or(usize::MAX))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        // What the cluster actually served, which is the cost a scan pays
        // whether or not the caller goes on to consume it.
        self.rows_read += pairs.len();
        Ok(pairs)
    }
}

pub fn key(bytes: &[u8]) -> Key {
    Key::from_bytes(bytes.to_vec())
}

pub fn storage(
    pairs: &[(&[u8], &[u8])],
) -> (
    ClusterTableStorage,
    Arc<Mutex<MockSnapshot>>,
    MutationBuffer,
) {
    let snapshot = Arc::new(Mutex::new(MockSnapshot {
        data: pairs
            .iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect(),
        ..MockSnapshot::default()
    }));
    let buffer = MutationBuffer::new();
    let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&snapshot) as _;
    (
        ClusterTableStorage::new(buffer.clone(), handle),
        snapshot,
        buffer,
    )
}
