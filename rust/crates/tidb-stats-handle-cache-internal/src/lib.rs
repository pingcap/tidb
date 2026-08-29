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

//! Go `pkg/statistics/handle/cache/internal`.

use std::sync::Arc;

use tidb_stats::Table;

/// Go `StatsCacheInner`.
#[allow(clippy::len_without_is_empty)] // The pinned Go interface has Len, but no IsEmpty.
pub trait StatsCacheInner {
    /// Go `Get`.
    fn get(&self, table_id: i64) -> Option<Arc<Table>>;

    /// Go `Put`.
    fn put(&self, table_id: i64, table: Arc<Table>) -> bool;

    /// Go `Del`.
    fn del(&self, table_id: i64);

    /// Go `Cost`.
    fn cost(&self) -> i64;

    /// Go `Values`.
    fn values(&self) -> Vec<Arc<Table>>;

    /// Go `Len`.
    fn len(&self) -> usize;

    /// Go `Copy`.
    fn copy(&self) -> Box<dyn StatsCacheInner>;

    /// Go `SetCapacity`.
    fn set_capacity(&self, capacity: i64);

    /// Go `Close`.
    fn close(&self);

    /// Go `TriggerEvict`.
    fn trigger_evict(&self);

    /// Go `WaitForAsyncUpdates`.
    fn wait_for_async_updates(&self);
}
