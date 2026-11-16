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

//! Go `meta.NewReader(storage.GetSnapshot(kv.NewVersion(ts)))`: the
//! catalog at a timestamp, as `tidb_meta`'s `Mutator` over a read-only
//! snapshot.

use std::sync::Mutex;

use tidb_meta::MetaError;
use tidb_structure::transaction::{RawRangeVisitor, RawTransaction};

use crate::glue::{KvSnapshot, Storage};

/// A snapshot as Go's read-only meta transaction.
pub struct SnapshotTxn {
    snapshot: Mutex<Box<dyn KvSnapshot>>,
    ts: u64,
}

/// Go `meta.Reader` over a snapshot.
pub type MetaReader = tidb_meta::transaction::Mutator<SnapshotTxn>;

/// Go `meta.NewReader(storage.GetSnapshot(kv.NewVersion(ts)))`.
#[must_use]
pub fn new_reader(storage: &dyn Storage, ts: u64) -> MetaReader {
    tidb_meta::transaction::Mutator::new(SnapshotTxn {
        snapshot: Mutex::new(storage.get_snapshot(ts)),
        ts,
    })
}

fn storage_err(e: &crate::errors::Error) -> MetaError {
    MetaError::Storage(e.to_string())
}

/// The end of a prefix scan: the prefix with its last byte incremented.
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.last_mut() {
        if *last < 0xff {
            *last += 1;
            return end;
        }
        end.pop();
    }
    Vec::new()
}

impl RawTransaction for SnapshotTxn {
    type Error = MetaError;

    fn get(&mut self, key: &[u8]) -> tidb_meta::Result<Option<Vec<u8>>> {
        self.snapshot
            .lock()
            .unwrap()
            .get(key)
            .map_err(|e| storage_err(&e))
    }

    fn set(&mut self, _key: Vec<u8>, _value: Vec<u8>) -> tidb_meta::Result<()> {
        Err(MetaError::Storage(
            "a meta reader does not write".to_owned(),
        ))
    }

    fn delete(&mut self, _key: &[u8]) -> tidb_meta::Result<()> {
        Err(MetaError::Storage(
            "a meta reader does not write".to_owned(),
        ))
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> tidb_meta::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.snapshot
            .lock()
            .unwrap()
            .scan(prefix, &prefix_end(prefix), None)
            .map_err(|e| storage_err(&e))
    }

    fn iterate_range(
        &mut self,
        start: &[u8],
        end: &[u8],
        visit: &mut RawRangeVisitor<'_, Self::Error>,
    ) -> tidb_meta::Result<()> {
        let pairs = self
            .snapshot
            .lock()
            .unwrap()
            .scan(start, end, None)
            .map_err(|e| storage_err(&e))?;
        for (k, v) in pairs {
            visit(&k, &v)?;
        }
        Ok(())
    }
}

impl tidb_meta::transaction::MetaTransaction for SnapshotTxn {
    fn start_ts(&self) -> u64 {
        self.ts
    }

    /// Go `NewReader` applies no transaction configuration.
    fn configure_meta_mutator(&mut self) {}
}
