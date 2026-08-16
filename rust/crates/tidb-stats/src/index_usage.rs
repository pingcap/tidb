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

//! Index-usage collection from
//! `pkg/statistics/handle/usage/indexusage/collector.go`.
//!
//! Complete for that file: the seven percentage-access buckets, one-sample
//! construction, the node-global collector and its worker lifecycle, the
//! per-session collector with `Report`/`Flush`, the per-statement de-duplicating
//! collector, and schema-driven garbage collection.
//!
//! Concurrency mapping. The source's `sync.Pool` of delta maps is a Go
//! allocation optimization with no observable behavior, so deltas are allocated
//! directly here. The goroutine and channels behind `StartWorker` are already
//! transcreated in [`crate::usage_collector`], which backs its
//! `GlobalCollector`/`SessionCollector` with a `std::thread` worker over a
//! mutex/condvar queue; `start_worker` and `close` on this collector delegate to
//! it, so callers and tests drive the worker exactly as the Go tests do.
//! `sync.RWMutex` becomes `std::sync::RwLock` and `sync.Mutex` becomes
//! `std::sync::Mutex`; because Go shares one `*SessionIndexUsageCollector`
//! between a session and its statement collector, the Rust statement collector
//! holds that session collector behind `Arc<Mutex<..>>`.
//!
//! Boundaries: the source's `GCIndexUsage` takes a `meta/model.TableInfo`
//! lookup, and `tidb-model` is not a dependency of this crate, so the lookup is
//! narrowed to the index IDs the source actually reads.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::SystemTime;

use crate::index_usage_key::IndexUsageKey;
use crate::usage_collector::{GlobalCollector, SessionCollector};

/// Source `GlobalIndexID`: the table ID/index ID pair keying one sample.
///
/// The identical identity is already transcreated as [`IndexUsageKey`].
pub type GlobalIndexId = IndexUsageKey;

/// Source `indexUsage`: accumulated samples keyed by table and index ID.
pub type IndexUsageMap = HashMap<IndexUsageKey, IndexUsageSample>;

/// Percentage boundaries used by index-usage samples.
pub const INDEX_USAGE_BUCKET_BOUNDS: [f64; 6] = [0.0, 0.01, 0.1, 0.2, 0.5, 1.0];

/// Number of percentage-access buckets in an index-usage sample.
pub const INDEX_USAGE_BUCKET_COUNT: usize = INDEX_USAGE_BUCKET_BOUNDS.len() + 1;

/// A single index-usage observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexUsageSample {
    /// Wall-clock time at which the observation was created.
    pub last_used_at: SystemTime,
    /// Number of queries attributed to the index by this observation.
    pub query_total: u64,
    /// Number of KV requests attributed to the index.
    pub kv_req_total: u64,
    /// Number of rows scanned through the index.
    pub row_access_total: u64,
    /// One-hot percentage-access bucket for this observation.
    pub percentage_access: [u64; INDEX_USAGE_BUCKET_COUNT],
}

impl Default for IndexUsageSample {
    /// The source's zero `Sample`, whose `LastUsedAt` never wins a merge.
    ///
    /// Go's zero `time.Time` is year 1, which `SystemTime` cannot portably
    /// represent; the epoch is used instead and orders identically against the
    /// wall-clock stamps this module produces.
    fn default() -> Self {
        Self {
            last_used_at: SystemTime::UNIX_EPOCH,
            query_total: 0,
            kv_req_total: 0,
            row_access_total: 0,
            percentage_access: [0; INDEX_USAGE_BUCKET_COUNT],
        }
    }
}

impl IndexUsageSample {
    /// Merges another observation into this one.
    ///
    /// This is the source `indexUsage.updateByKey` value merge without the
    /// surrounding map, mutex, or collector channel.
    pub fn merge(&mut self, other: &Self) {
        self.query_total = self.query_total.wrapping_add(other.query_total);
        self.kv_req_total = self.kv_req_total.wrapping_add(other.kv_req_total);
        self.row_access_total = self.row_access_total.wrapping_add(other.row_access_total);
        for (current, incoming) in self
            .percentage_access
            .iter_mut()
            .zip(other.percentage_access.iter())
        {
            *current = current.wrapping_add(*incoming);
        }
        if self.last_used_at < other.last_used_at {
            self.last_used_at = other.last_used_at;
        }
    }
}

/// Maps a scanned-row percentage to TiDB's percentage-access bucket.
///
/// Values outside the source's explicit ranges intentionally retain the Go
/// zero-value bucket behavior. In particular, `NaN` and percentages greater
/// than one are not clamped or assigned a new bucket.
#[must_use]
pub fn index_usage_access_bucket(percentage: f64) -> usize {
    if percentage == 0.0 {
        return 0;
    }

    let mut bucket = 0;
    for index in 1..INDEX_USAGE_BUCKET_BOUNDS.len() {
        if percentage >= INDEX_USAGE_BUCKET_BOUNDS[index - 1]
            && percentage < INDEX_USAGE_BUCKET_BOUNDS[index]
        {
            bucket = index;
            break;
        }
    }
    if percentage == 1.0 {
        bucket = INDEX_USAGE_BUCKET_BOUNDS.len();
    }
    bucket
}

/// Constructs one index-usage observation and records its scan-percentage
/// bucket.
#[must_use]
pub fn new_index_usage_sample(
    query_total: u64,
    kv_req_total: u64,
    row_access: u64,
    table_total_rows: u64,
) -> IndexUsageSample {
    let mut percentage_access = [0; INDEX_USAGE_BUCKET_COUNT];
    let bucket = if table_total_rows > 0 {
        index_usage_access_bucket(row_access as f64 / table_total_rows as f64)
    } else {
        INDEX_USAGE_BUCKET_BOUNDS.len()
    };
    percentage_access[bucket] = 1;

    IndexUsageSample {
        last_used_at: SystemTime::now(),
        query_total,
        kv_req_total,
        row_access_total: row_access,
        percentage_access,
    }
}

/// Source `indexUsage.updateByKey`: folds one sample into the map entry.
fn update_by_key(map: &mut IndexUsageMap, id: IndexUsageKey, sample: &IndexUsageSample) {
    map.entry(id).or_default().merge(sample);
}

/// Source `indexUsage.merge`: folds every entry of `delta` into `map`.
fn merge_map(map: &mut IndexUsageMap, delta: &IndexUsageMap) {
    for (id, sample) in delta {
        update_by_key(map, *id, sample);
    }
}

/// Records index usage for the whole node.
///
/// Source `Collector`.
pub struct IndexUsageCollector {
    index_usage: Arc<RwLock<IndexUsageMap>>,
    collector: GlobalCollector<IndexUsageMap>,
}

impl IndexUsageCollector {
    /// Creates an index-usage collector. Source `NewCollector`.
    #[must_use]
    pub fn new() -> Self {
        let index_usage = Arc::new(RwLock::new(IndexUsageMap::new()));
        let merge_target = Arc::clone(&index_usage);
        let collector = GlobalCollector::new(move |delta: IndexUsageMap| {
            let mut usage = merge_target
                .write()
                .expect("index usage collector lock poisoned");
            merge_map(&mut usage, &delta);
        });

        Self {
            index_usage,
            collector,
        }
    }

    /// Returns the accumulated usage of one index.
    ///
    /// Source `Collector.GetIndexUsage`, which likewise reports the zero sample
    /// for an index the collector has never seen.
    #[must_use]
    pub fn get_index_usage(&self, table_id: i64, index_id: i64) -> IndexUsageSample {
        self.index_usage
            .read()
            .expect("index usage collector lock poisoned")
            .get(&IndexUsageKey::new(table_id, index_id))
            .cloned()
            .unwrap_or_default()
    }

    /// Copies the whole accumulated map.
    ///
    /// The source's tests read the unexported `indexUsage` field directly;
    /// this snapshot is the equivalent that survives the crate boundary.
    #[must_use]
    pub fn index_usage_snapshot(&self) -> IndexUsageMap {
        self.index_usage
            .read()
            .expect("index usage collector lock poisoned")
            .clone()
    }

    /// Creates a session collector attached to this global collector.
    ///
    /// Source `Collector.SpawnSessionCollector`.
    #[must_use]
    pub fn spawn_session_collector(&self) -> SessionIndexUsageCollector {
        SessionIndexUsageCollector {
            index_usage: IndexUsageMap::new(),
            collector: self.collector.spawn_session(),
        }
    }

    /// Starts the background merge worker. Source `Collector.StartWorker`.
    pub fn start_worker(&self) {
        self.collector.start_worker();
    }

    /// Closes the background merge worker. Source `Collector.Close`.
    pub fn close(&self) {
        self.collector.close();
    }

    /// Deletes the usage of indexes that no longer exist.
    ///
    /// Source `Collector.GCIndexUsage`. The source takes a lookup returning
    /// `*model.TableInfo`; `tidb-model` is not a dependency of this crate, so
    /// the lookup is narrowed to the only field the source reads — the IDs of
    /// the table's indexes. `None` marks a table that no longer exists.
    // boundary: pkg/meta/model.TableInfo (Indices[i].ID) is passed as index IDs.
    pub fn gc_index_usage<F>(&self, table_meta_lookup: F)
    where
        F: Fn(i64) -> Option<Vec<i64>>,
    {
        // The source keeps one mutex rather than splitting it, because every
        // operation guarded by it is infrequent.
        let mut usage = self
            .index_usage
            .write()
            .expect("index usage collector lock poisoned");
        usage.retain(|key, _| {
            table_meta_lookup(key.table_id)
                .is_some_and(|index_ids| index_ids.contains(&key.index_id))
        });
    }
}

impl Default for IndexUsageCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Collects index usage for one session.
///
/// Source `SessionIndexUsageCollector`.
pub struct SessionIndexUsageCollector {
    index_usage: IndexUsageMap,
    collector: SessionCollector<IndexUsageMap>,
}

impl SessionIndexUsageCollector {
    /// Records one sample against a table/index pair.
    ///
    /// Source `SessionIndexUsageCollector.Update`.
    pub fn update(&mut self, table_id: i64, index_id: i64, sample: &IndexUsageSample) {
        update_by_key(
            &mut self.index_usage,
            IndexUsageKey::new(table_id, index_id),
            sample,
        );
    }

    /// Returns this session's pending usage for one index.
    ///
    /// Source `SessionIndexUsageCollector.GetIndexUsageForTest`.
    #[must_use]
    pub fn get_index_usage(&self, table_id: i64, index_id: i64) -> Option<IndexUsageSample> {
        self.index_usage
            .get(&IndexUsageKey::new(table_id, index_id))
            .cloned()
    }

    /// Offers this session's pending usage to the global collector without
    /// blocking; the delta is kept when the collector cannot accept it.
    ///
    /// Source `SessionIndexUsageCollector.Report`.
    pub fn report(&mut self) {
        if self.index_usage.is_empty() {
            return;
        }
        // Go's `select` with a `default` arm leaves the value untouched when the
        // channel is full; sending by value cannot, so the retained copy stands
        // in for the value the source never gave away.
        let delta = std::mem::take(&mut self.index_usage);
        if !self.collector.send_delta(delta.clone()) {
            self.index_usage = delta;
        }
    }

    /// Hands this session's pending usage to the global collector, blocking
    /// until it is accepted.
    ///
    /// Source `SessionIndexUsageCollector.Flush`.
    pub fn flush(&mut self) {
        if self.index_usage.is_empty() {
            return;
        }
        self.collector
            .send_delta_sync(std::mem::take(&mut self.index_usage));
    }
}

/// De-duplicates indexes within one statement before recording `query_total`.
///
/// Source `StmtIndexUsageCollector`.
pub struct StmtIndexUsageCollector {
    state: Mutex<HashSet<IndexUsageKey>>,
    session_collector: Arc<Mutex<SessionIndexUsageCollector>>,
}

impl StmtIndexUsageCollector {
    /// Creates a statement collector feeding one session collector.
    ///
    /// Source `NewStmtIndexUsageCollector`.
    #[must_use]
    pub fn new(session_collector: Arc<Mutex<SessionIndexUsageCollector>>) -> Self {
        Self {
            state: Mutex::new(HashSet::new()),
            session_collector,
        }
    }

    /// Records a sample, forcing `query_total` to one on the statement's first
    /// use of the index and to zero afterwards.
    ///
    /// Source `StmtIndexUsageCollector.Update`. The lock is held because
    /// executors with multiple workers can close concurrently.
    pub fn update(&self, table_id: i64, index_id: i64, sample: &IndexUsageSample) {
        let mut recorded_index = self
            .state
            .lock()
            .expect("statement index usage lock poisoned");

        let mut sample = sample.clone();
        sample.query_total =
            u64::from(recorded_index.insert(IndexUsageKey::new(table_id, index_id)));

        self.session_collector
            .lock()
            .expect("session index usage lock poisoned")
            .update(table_id, index_id, &sample);
    }

    /// Forgets the indexes recorded so far. Source
    /// `StmtIndexUsageCollector.Reset`.
    pub fn reset(&self) {
        self.state
            .lock()
            .expect("statement index usage lock poisoned")
            .clear();
    }
}
