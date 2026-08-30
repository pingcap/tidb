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

//! Go `pkg/statistics/handle/usage/indexusage`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use chrono::{DateTime, TimeZone, Utc};
use tidb_model::TableInfo;
use tidb_stats_handle_usage_collector::{GlobalCollector, SessionCollector};

/// Go `GlobalIndexID`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct GlobalIndexId {
    /// Table ID.
    pub table_id: i64,
    /// Index ID.
    pub index_id: i64,
}

/// Go `Sample`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Sample {
    /// Go `LastUsedAt`.
    pub last_used_at: DateTime<Utc>,
    /// Go `QueryTotal`.
    pub query_total: u64,
    /// Go `KvReqTotal`.
    pub kv_req_total: u64,
    /// Go `RowAccessTotal`.
    pub row_access_total: u64,
    /// Go `PercentageAccess`.
    pub percentage_access: [u64; 7],
}

impl Default for Sample {
    fn default() -> Self {
        Self {
            last_used_at: Utc
                .with_ymd_and_hms(1, 1, 1, 0, 0, 0)
                .single()
                .expect("Go zero time is representable"),
            query_total: 0,
            kv_req_total: 0,
            row_access_total: 0,
            percentage_access: [0; 7],
        }
    }
}

const BUCKET_BOUND: [f64; 6] = [0.0, 0.01, 0.1, 0.2, 0.5, 1.0];

fn get_index_usage_access_bucket(percentage: f64) -> usize {
    if percentage == 0.0 {
        return 0;
    }
    let mut bucket = 0;
    for index in 1..BUCKET_BOUND.len() {
        if percentage >= BUCKET_BOUND[index - 1] && percentage < BUCKET_BOUND[index] {
            bucket = index;
            break;
        }
    }
    if percentage == 1.0 {
        bucket = BUCKET_BOUND.len();
    }
    bucket
}

/// Go `NewSample`.
#[must_use]
pub fn new_sample(
    query_total: u64,
    kv_req_total: u64,
    row_access: u64,
    table_total_rows: u64,
) -> Sample {
    let mut percentage_access = [0; 7];
    let bucket = if table_total_rows > 0 {
        get_index_usage_access_bucket(row_access as f64 / table_total_rows as f64)
    } else {
        BUCKET_BOUND.len()
    };
    percentage_access[bucket] = 1;
    Sample {
        last_used_at: Utc::now(),
        query_total,
        kv_req_total,
        row_access_total: row_access,
        percentage_access,
    }
}

type IndexUsage = HashMap<GlobalIndexId, Sample>;

fn index_usage_pool() -> &'static Mutex<Vec<IndexUsage>> {
    static POOL: OnceLock<Mutex<Vec<IndexUsage>>> = OnceLock::new();
    POOL.get_or_init(|| Mutex::new(Vec::new()))
}

fn take_index_usage() -> IndexUsage {
    index_usage_pool()
        .lock()
        .expect("index usage pool lock poisoned")
        .pop()
        .unwrap_or_default()
}

fn recycle_index_usage(delta: Arc<IndexUsage>) {
    let Ok(mut delta) = Arc::try_unwrap(delta) else {
        return;
    };
    delta.clear();
    index_usage_pool()
        .lock()
        .expect("index usage pool lock poisoned")
        .push(delta);
}

fn update_by_key(usage: &mut IndexUsage, id: GlobalIndexId, sample: Sample) {
    let item = usage.entry(id).or_default();
    item.query_total = item.query_total.wrapping_add(sample.query_total);
    item.kv_req_total = item.kv_req_total.wrapping_add(sample.kv_req_total);
    item.row_access_total = item.row_access_total.wrapping_add(sample.row_access_total);
    for (current, incoming) in item
        .percentage_access
        .iter_mut()
        .zip(sample.percentage_access)
    {
        *current = current.wrapping_add(incoming);
    }
    if item.last_used_at < sample.last_used_at {
        item.last_used_at = sample.last_used_at;
    }
}

fn merge(usage: &mut IndexUsage, delta: &IndexUsage) {
    for (id, sample) in delta {
        update_by_key(usage, *id, sample.clone());
    }
}

/// Go `Collector`.
pub struct Collector {
    collector: GlobalCollector<Arc<IndexUsage>>,
    index_usage: Arc<RwLock<IndexUsage>>,
}

impl Collector {
    /// Go `NewCollector`.
    #[must_use]
    pub fn new() -> Self {
        let index_usage = Arc::new(RwLock::new(take_index_usage()));
        let target = Arc::clone(&index_usage);
        let collector = GlobalCollector::new(move |delta: Arc<IndexUsage>| {
            merge(
                &mut target.write().expect("index usage lock poisoned"),
                &delta,
            );
            recycle_index_usage(delta);
        });
        Self {
            collector,
            index_usage,
        }
    }

    /// Go `Collector.GetIndexUsage`.
    #[must_use]
    pub fn get_index_usage(&self, table_id: i64, index_id: i64) -> Sample {
        self.index_usage
            .read()
            .expect("index usage lock poisoned")
            .get(&GlobalIndexId { table_id, index_id })
            .cloned()
            .unwrap_or_default()
    }

    /// Go `Collector.SpawnSessionCollector`.
    #[must_use]
    pub fn spawn_session_collector(&self) -> SessionIndexUsageCollector {
        SessionIndexUsageCollector {
            index_usage: Arc::new(take_index_usage()),
            collector: self.collector.spawn_session(),
        }
    }

    /// Go `Collector.StartWorker`.
    pub fn start_worker(&self) {
        self.collector.start_worker();
    }

    /// Go `Collector.Close`.
    pub fn close(&self) {
        self.collector.close();
    }

    /// Go `Collector.GCIndexUsage`.
    pub fn gc_index_usage(&self, table_meta_lookup: impl Fn(i64) -> Option<Arc<TableInfo>>) {
        self.index_usage
            .write()
            .expect("index usage lock poisoned")
            .retain(|id, _| {
                table_meta_lookup(id.table_id).is_some_and(|table| {
                    table
                        .indices
                        .iter_deref()
                        .any(|index| index.read().id == id.index_id)
                })
            });
    }
}

/// Go `SessionIndexUsageCollector`.
pub struct SessionIndexUsageCollector {
    index_usage: Arc<IndexUsage>,
    collector: SessionCollector<Arc<IndexUsage>>,
}

impl SessionIndexUsageCollector {
    /// Go `SessionIndexUsageCollector.Update`.
    pub fn update(&mut self, table_id: i64, index_id: i64, sample: Sample) {
        update_by_key(
            Arc::get_mut(&mut self.index_usage)
                .expect("pending index usage is unexpectedly shared"),
            GlobalIndexId { table_id, index_id },
            sample,
        );
    }

    /// Go `SessionIndexUsageCollector.Report`.
    pub fn report(&mut self) {
        if self.index_usage.is_empty() {
            return;
        }
        if self.collector.send_delta(Arc::clone(&self.index_usage)) {
            self.index_usage = Arc::new(take_index_usage());
        }
    }

    /// Go `SessionIndexUsageCollector.Flush`.
    pub fn flush(&mut self) {
        if self.index_usage.is_empty() {
            return;
        }
        self.collector
            .send_delta_sync(Arc::clone(&self.index_usage));
        self.index_usage = Arc::new(take_index_usage());
    }
}

/// Go `StmtIndexUsageCollector`.
pub struct StmtIndexUsageCollector {
    recorded_index: Mutex<HashSet<GlobalIndexId>>,
    session_collector: Arc<Mutex<SessionIndexUsageCollector>>,
}

impl StmtIndexUsageCollector {
    /// Go `NewStmtIndexUsageCollector`.
    #[must_use]
    pub fn new(session_collector: Arc<Mutex<SessionIndexUsageCollector>>) -> Self {
        Self {
            recorded_index: Mutex::new(HashSet::new()),
            session_collector,
        }
    }

    /// Go `StmtIndexUsageCollector.Update`.
    pub fn update(&self, table_id: i64, index_id: i64, mut sample: Sample) {
        let mut recorded_index = self
            .recorded_index
            .lock()
            .expect("statement index usage lock poisoned");
        sample.query_total = u64::from(recorded_index.insert(GlobalIndexId { table_id, index_id }));
        self.session_collector
            .lock()
            .expect("session index usage lock poisoned")
            .update(table_id, index_id, sample);
    }

    /// Go `StmtIndexUsageCollector.Reset`.
    pub fn reset(&self) {
        self.recorded_index
            .lock()
            .expect("statement index usage lock poisoned")
            .clear();
    }
}

#[cfg(test)]
mod tests;
