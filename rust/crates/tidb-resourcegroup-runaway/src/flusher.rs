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

//! Go `flusher.go`: a buffer of records written in one statement when it
//! reaches its threshold or its ticker fires.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::record::{exec_rc_restricted_sql, Param};
use crate::SessionPool;

type MergeFn<K, V> = Box<dyn Fn(&mut HashMap<K, V>, K, V) + Send>;
type GenSqlFn<K, V> = Box<dyn Fn(&HashMap<K, V>) -> (String, Vec<Param>) + Send>;

/// Go `batchFlusher`.
pub(crate) struct BatchFlusher<K, V> {
    name: String,
    buffer: HashMap<K, V>,
    /// Go `ticker`: the interval and the next tick.
    interval: Duration,
    next_tick: Instant,
    threshold: usize,
    last_flush_time: Option<Instant>,
    merge_fn: MergeFn<K, V>,
    gen_sql: GenSqlFn<K, V>,
    pool: Arc<dyn SessionPool>,
}

impl<K: Eq + Hash, V> BatchFlusher<K, V> {
    /// Go `newBatchFlusher`.
    pub(crate) fn new(
        name: &str,
        interval: Duration,
        threshold: usize,
        merge_fn: impl Fn(&mut HashMap<K, V>, K, V) + Send + 'static,
        gen_sql: impl Fn(&HashMap<K, V>) -> (String, Vec<Param>) + Send + 'static,
        pool: Arc<dyn SessionPool>,
    ) -> Self {
        Self {
            name: name.to_owned(),
            buffer: HashMap::with_capacity(threshold),
            interval,
            next_tick: Instant::now() + interval,
            threshold,
            last_flush_time: None,
            merge_fn: Box::new(merge_fn),
            gen_sql: Box::new(gen_sql),
            pool,
        }
    }

    /// Go `ticker.C`'s next delivery.
    pub(crate) fn next_tick(&self) -> Instant {
        self.next_tick
    }

    /// Go's `<-f.tickerCh()` case: the tick is consumed (a `time.Ticker`
    /// drops the ticks a slow reader misses) and the buffer flushed.
    pub(crate) fn tick(&mut self, now: Instant) {
        while self.next_tick <= now {
            self.next_tick += self.interval;
        }
        self.flush();
    }

    /// Go `stop`.
    pub(crate) fn stop(&mut self) {
        let name = self.name.clone();
        let field = || tidb_log::Field::new("name", tidb_log::Value::Str(name.clone()));
        tidb_logutil::bg_logger().info("flushing remaining records before stop", &[field()]);
        self.flush();
        tidb_logutil::bg_logger().info("stopped flusher", &[field()]);
    }

    /// Go `add`.
    pub(crate) fn add(&mut self, key: K, value: V) {
        tidb_metrics::server::TIDB_SERVER_RUNAWAY_FLUSHER_ADD_TOTAL.with_label_values(&[&self.name]).inc();
        (self.merge_fn)(&mut self.buffer, key, value);
        if self.buffer.len() >= self.threshold {
            self.flush();
        }
    }

    /// Go `flush`.
    pub(crate) fn flush(&mut self) {
        let batch_size = self.buffer.len();
        if batch_size == 0 {
            // Reset lastFlushTime so the next real flush after an idle period
            // does not record the idle gap as flush interval.
            self.last_flush_time = None;
            return;
        }
        let now = Instant::now();
        if let Some(last) = self.last_flush_time {
            tidb_metrics::server::TIDB_SERVER_RUNAWAY_FLUSHER_INTERVAL_SECONDS
                .with_label_values(&[&self.name])
                .observe(now.duration_since(last).as_secs_f64());
        }
        let start = Instant::now();
        let succeeded = self.flush_fn();
        let duration = start.elapsed();
        tidb_metrics::server::TIDB_SERVER_RUNAWAY_FLUSHER_BATCH_SIZE
            .with_label_values(&[&self.name])
            .observe(batch_size as f64);
        tidb_metrics::server::TIDB_SERVER_RUNAWAY_FLUSHER_DURATION_SECONDS
            .with_label_values(&[&self.name])
            .observe(duration.as_secs_f64());
        let result = if succeeded { tidb_metrics::server::labels::OK } else { tidb_metrics::server::labels::ERROR };
        tidb_metrics::server::TIDB_SERVER_RUNAWAY_FLUSHER_TOTAL.with_label_values(&[&self.name, result]).inc();
        self.last_flush_time = Some(now);
        self.buffer = HashMap::with_capacity(self.threshold);
    }

    /// Go `flushFn`.
    fn flush_fn(&self) -> bool {
        let count = self.buffer.len();
        if count == 0 {
            return true;
        }
        let (sql, params) = (self.gen_sql)(&self.buffer);
        if let Err(error) = exec_rc_restricted_sql(self.pool.as_ref(), &sql, &params) {
            tidb_logutil::bg_logger().error(
                "batch flush failed",
                &[
                    tidb_log::Field::new("name", tidb_log::Value::Str(self.name.clone())),
                    tidb_log::Field::new("count", tidb_log::Value::I64(count as i64)),
                    tidb_log::Field::new("error", tidb_log::Value::Str(error)),
                ],
            );
            return false;
        }
        true
    }
}
