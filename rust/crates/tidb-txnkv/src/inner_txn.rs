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

//! Inner-transaction start-timestamp tracking translated from `pkg/kv/txn.go`.
//!
//! This leaf owns the source's synchronized set and minimum-selection rule.
//! The TiDB implementation additionally emits long-running-transaction logs
//! from the timestamp's physical clock; that logging and the global server
//! registry stay outside `tidb-txnkv`, which has no oracle or session runtime.

use std::collections::BTreeSet;
use std::ops::Bound::Excluded;
use std::sync::Mutex;

/// Synchronized set of inner-transaction start timestamps.
///
/// `pkg/kv/txn.go` stores timestamps in a mutex-protected map. A set is the
/// source-shaped representation because the map values are all empty structs;
/// ordering the set makes minimum selection explicit without changing the
/// returned value.
#[derive(Debug, Default)]
pub struct InnerTxnStartTsBox {
    timestamps: Mutex<BTreeSet<u64>>,
}

impl InnerTxnStartTsBox {
    /// Creates an empty timestamp box.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an inner transaction start timestamp.
    pub fn store_inner_txn_ts(&self, start_ts: u64) {
        self.timestamps
            .lock()
            .expect("inner transaction timestamp mutex poisoned")
            .insert(start_ts);
    }

    /// Removes an inner transaction start timestamp.
    pub fn delete_inner_txn_ts(&self, start_ts: u64) {
        self.timestamps
            .lock()
            .expect("inner transaction timestamp mutex poisoned")
            .remove(&start_ts);
    }

    /// Reports whether a timestamp is currently tracked.
    #[must_use]
    pub fn contains(&self, start_ts: u64) -> bool {
        self.timestamps
            .lock()
            .expect("inner transaction timestamp mutex poisoned")
            .contains(&start_ts)
    }

    /// Returns the smallest tracked timestamp strictly above `lower_limit`
    /// and strictly below `current_min`.
    ///
    /// This is the pure selection part of Go's `getMinStartTS`. The Go method
    /// also calls `PrintLongTimeInternalTxn` for every tracked timestamp; the
    /// timestamp-to-wall-clock conversion and logging remain an explicit
    /// server/oracle integration boundary.
    #[must_use]
    pub fn get_min_start_ts(&self, lower_limit: u64, current_min: u64) -> u64 {
        // Go's loop simply finds no value when the strict upper bound is at or
        // below the lower bound. Avoid constructing an invalid BTreeSet range
        // for that ordinary boundary case.
        if current_min <= lower_limit {
            return current_min;
        }
        let timestamps = self
            .timestamps
            .lock()
            .expect("inner transaction timestamp mutex poisoned");
        timestamps
            .range((Excluded(lower_limit), Excluded(current_min)))
            .next()
            .copied()
            .unwrap_or(current_min)
    }
}

/// Pure equivalent of Go's `GetMinInnerTxnStartTS` once the global box has
/// been made an explicit dependency.
#[must_use]
pub fn get_min_inner_txn_start_ts(
    timestamps: &InnerTxnStartTsBox,
    lower_limit: u64,
    current_min: u64,
) -> u64 {
    timestamps.get_min_start_ts(lower_limit, current_min)
}
