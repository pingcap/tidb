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

//! Go `variable.TableDelta` and the transaction's `TableDeltaMap` it fills
//! (`TransactionContext.UpdateDeltaForTable`); the statistics usage collector
//! accumulates the same map per session.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::SystemTime;

/// Go `variable.TableDelta`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TableDelta {
    /// The table row-count change.
    pub delta: i64,
    /// The number of modified rows.
    pub count: i64,
    /// The first time represented by this accumulated delta.
    pub init_time: Option<SystemTime>,
}

impl TableDelta {
    /// Go `TableDelta.MergeFrom`.
    pub fn merge_from(&mut self, incoming: Self) {
        self.delta = self.delta.wrapping_add(incoming.delta);
        self.count = self.count.wrapping_add(incoming.count);
        if self.init_time.is_none()
            || incoming
                .init_time
                .is_some_and(|incoming| self.init_time.is_some_and(|current| incoming < current))
        {
            self.init_time = incoming.init_time;
        }
    }
}

/// Go `TableDeltaMap`.
#[derive(Default)]
pub struct TableDeltaMap {
    delta: Mutex<HashMap<i64, TableDelta>>,
}

impl TableDeltaMap {
    /// Go `NewTableDeltaMap`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `TableDeltaMap.Reset`.
    pub fn reset(&self) {
        self.delta
            .lock()
            .expect("table delta lock poisoned")
            .clear();
    }

    /// Go `TableDeltaMap.GetDeltaAndReset`.
    pub fn get_delta_and_reset(&self) -> HashMap<i64, TableDelta> {
        std::mem::take(&mut *self.delta.lock().expect("table delta lock poisoned"))
    }

    /// Go `TableDeltaMap.Update`.
    pub fn update(&self, id: i64, delta: i64, count: i64) {
        debug_assert!(id > 0, "table ID should be greater than 0");
        let mut values = self.delta.lock().expect("table delta lock poisoned");
        let item = values.entry(id).or_default();
        item.delta = item.delta.wrapping_add(delta);
        item.count = item.count.wrapping_add(count);
    }

    /// Go `TableDeltaMap.Merge`.
    pub fn merge(&self, incoming: HashMap<i64, TableDelta>) {
        if incoming.is_empty() {
            return;
        }
        let mut values = self.delta.lock().expect("table delta lock poisoned");
        for (id, incoming) in incoming {
            values.entry(id).or_default().merge_from(incoming);
        }
    }

    /// Go `TransactionContext.GetCurrentSavepoint`'s table-delta clone.
    pub fn snapshot(&self) -> HashMap<i64, TableDelta> {
        self.delta
            .lock()
            .expect("table delta lock poisoned")
            .clone()
    }

    /// Go `TransactionContext.RestoreBySavepoint`'s table-delta restore.
    pub fn restore(&self, snapshot: HashMap<i64, TableDelta>) {
        *self.delta.lock().expect("table delta lock poisoned") = snapshot;
    }
}
