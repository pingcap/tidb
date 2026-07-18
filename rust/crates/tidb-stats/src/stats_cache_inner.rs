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

//! Generic statistics-cache interface from
//! `pkg/statistics/handle/cache/internal/inner.go`.
//!
//! The Go interface is coupled to `*statistics.Table` and to cache-specific
//! memory/admission implementations.  This Rust boundary keeps the table
//! value opaque and transfers ownership on `put`; concrete caches decide how
//! to derive [`cost`](StatsCacheInner::cost), admit/evict entries, and make
//! asynchronous writes visible. `copy` returns a boxed trait object, matching
//! the source interface's dynamic cache field.

/// Cache operations required by the statistics handle.
///
/// `V` is the caller-selected table representation.  The trait deliberately
/// keeps values opaque so map, LFU, and future cache implementations can share
/// the source-shaped lifecycle without importing `statistics::Table`.
pub trait StatsCacheInner<V: 'static> {
    /// Returns the cached value for a table ID, if present.
    fn get(&self, tid: i64) -> Option<&V>;

    /// Inserts or replaces the value for a table ID.
    fn put(&mut self, tid: i64, value: V) -> bool;

    /// Removes a value by table ID.
    fn del(&mut self, tid: i64);

    /// Returns implementation-owned aggregate memory cost.
    fn cost(&self) -> i64;

    /// Returns all cached values in implementation-defined order.
    fn values(&self) -> Vec<&V>;

    /// Returns the number of cached values.
    fn len(&self) -> usize;

    /// Returns whether the cache contains no values.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an independent cache copy with the same visible state.
    fn copy(&self) -> Box<dyn StatsCacheInner<V>>;

    /// Sets the implementation's capacity policy.
    fn set_capacity(&mut self, capacity: i64);

    /// Closes cache-owned resources.
    fn close(&mut self);

    /// Requests implementation-specific eviction.
    fn trigger_evict(&mut self);

    /// Waits until buffered asynchronous writes are visible to later gets.
    fn wait_for_async_updates(&self);
}
