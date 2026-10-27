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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Go `kv.Request.MaxKeysRead` with its statement-wide `MaxKeysReadCounter`:
/// every coprocessor read of one statement adds the keys its scans processed
/// (`ScanDetail.ProcessedKeys`, which unistore reports as its scans' row
/// count) to one counter, and a read that takes the total past a nonzero
/// limit fails with `ErrMaxKeysReadExceeded` (8274). The same counter is the
/// statement's share of `tidb_keys_examined`.
#[derive(Clone, Debug, Default)]
pub struct KeysReadBudget {
    read: Arc<AtomicU64>,
    max: u64,
}

impl KeysReadBudget {
    /// A budget over the statement's shared counter; `max` 0 is unlimited.
    #[must_use]
    pub fn new(read: Arc<AtomicU64>, max: u64) -> Self {
        Self { read, max }
    }

    /// Go `DistSQLContext.MaxKeysRead`; 0 is unlimited.
    #[must_use]
    pub fn max(&self) -> u64 {
        self.max
    }

    /// Go `DistSQLContext.MaxKeysReadCounter`: the statement-wide accumulator
    /// every coprocessor iterator of the statement adds to.
    #[must_use]
    pub fn counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.read)
    }

    /// The keys this statement's reads have processed so far.
    #[must_use]
    pub fn read(&self) -> u64 {
        self.read.load(Ordering::Relaxed)
    }

    /// Adds processed keys and checks the limit, as `copIterator.Next` does
    /// once each response's scan detail arrives.
    pub fn add(&self, keys: u64) -> Result<(), crate::storage::StorageError> {
        let total = self
            .read
            .fetch_add(keys, Ordering::Relaxed)
            .saturating_add(keys);
        if self.max > 0 && total > self.max {
            return Err(crate::storage::StorageError::Sql(
                tidb_error::mysql::MysqlError::from_terror(
                    &tidb_util::dbterror::exeerrors::ERR_MAX_KEYS_READ_EXCEEDED
                        .generate_with_stack_by_args(&[]),
                ),
            ));
        }
        Ok(())
    }
}
