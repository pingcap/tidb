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

//! Worker pool from Go `pkg/lightning/worker`.

use std::sync::Arc;
use std::time::Instant;

use crossbeam_channel::{bounded, Receiver, Sender};

use crate::lightning_metric::{from_context, Metrics};

/// A worker managed by a [`Pool`].
pub struct Worker {
    /// The worker identifier.
    pub id: i64,
}

/// A fixed-size pool of reusable workers.
pub struct Pool {
    workers_tx: Sender<Arc<Worker>>,
    workers_rx: Receiver<Arc<Worker>>,
    name: String,
    metrics: Option<Arc<Metrics>>,
}

impl Pool {
    /// Creates a worker pool with `limit` workers.
    pub fn new(context: &tikv_client::trace::TraceContext, limit: isize, name: String) -> Self {
        let capacity = usize::try_from(limit).expect("negative worker limit");
        let (workers_tx, workers_rx) = bounded(capacity);
        for id in 0..capacity {
            workers_tx
                .send(Arc::new(Worker {
                    id: (id as isize).wrapping_add(1) as i64,
                }))
                .unwrap_or_else(|_| unreachable!("worker channel is connected"));
        }
        let metrics = from_context(context);
        if let Some(metrics) = &metrics {
            metrics
                .idle_workers_gauge
                .with_label_values(&[name.as_str()])
                .set(limit as f64);
        }
        Self {
            workers_tx,
            workers_rx,
            name,
            metrics,
        }
    }

    /// Takes a worker from the pool, blocking until one is available.
    pub fn apply(&self) -> Arc<Worker> {
        let start = Instant::now();
        let worker = self
            .workers_rx
            .recv()
            .unwrap_or_else(|_| unreachable!("worker channel is connected"));
        if let Some(metrics) = &self.metrics {
            metrics
                .idle_workers_gauge
                .with_label_values(&[self.name.as_str()])
                .set(self.workers_rx.len() as f64);
            metrics
                .apply_worker_seconds_histogram
                .with_label_values(&[self.name.as_str()])
                .observe(start.elapsed().as_secs_f64());
        }
        worker
    }

    /// Returns a worker to the pool, blocking until the pool accepts it.
    pub fn recycle(&self, worker: Option<Arc<Worker>>) {
        let worker = worker.unwrap_or_else(|| panic!("invalid restore worker"));
        self.workers_tx
            .send(worker)
            .unwrap_or_else(|_| unreachable!("worker channel is connected"));
        if let Some(metrics) = &self.metrics {
            metrics
                .idle_workers_gauge
                .with_label_values(&[self.name.as_str()])
                .set(self.workers_rx.len() as f64);
        }
    }

    /// Reports whether a buffered worker is immediately available.
    pub fn has_worker(&self) -> bool {
        !self.workers_rx.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_recycle() {
        let pool = Pool::new(
            &tikv_client::trace::TraceContext::default(),
            3,
            "test".to_owned(),
        );
        let (w1, w2, w3) = (pool.apply(), pool.apply(), pool.apply());
        assert_eq!(w1.id, 1);
        assert_eq!(w2.id, 2);
        assert_eq!(w3.id, 3);
        assert!(!pool.has_worker());
        pool.recycle(Some(Arc::clone(&w3)));
        assert!(pool.has_worker());
        assert!(Arc::ptr_eq(&w3, &pool.apply()));
        pool.recycle(Some(Arc::clone(&w2)));
        assert!(Arc::ptr_eq(&w2, &pool.apply()));
        pool.recycle(Some(Arc::clone(&w1)));
        assert!(Arc::ptr_eq(&w1, &pool.apply()));
        assert!(!pool.has_worker());
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| pool.recycle(None)))
            .unwrap_err();
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"invalid restore worker")
        );
    }
}
