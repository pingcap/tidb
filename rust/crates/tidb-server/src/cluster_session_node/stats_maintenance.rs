// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go Domain.gcStatsWorker: one lifetime for statistics GC, health metrics and
//! memory-driven cache eviction, after initial statistics publication.

use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::{ClusterSessionFactory, UsageWorkerStop};
use crate::node_config::StatsLease;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tick {
    Gc,
    Health,
    Memory,
}

/// Retained beside the node, outside the factory it serves. Holding a factory
/// reference here as well as in the worker ensures its last drop never occurs
/// on this worker while shutdown is joining it.
pub(crate) struct StatsMaintenanceWorker {
    stop: Arc<UsageWorkerStop>,
    thread: Option<JoinHandle<()>>,
    factory: Option<Arc<ClusterSessionFactory>>,
}

impl StatsMaintenanceWorker {
    pub(crate) fn start(
        factory: &Arc<ClusterSessionFactory>,
        stats_lease: StatsLease,
        schema_lease: Duration,
        initialization: tidb_exec::stats_watch::AsyncStatsLoaderInit,
    ) -> Result<Option<Self>, String> {
        let StatsLease::Positive(lease) = stats_lease else {
            return Ok(None);
        };
        let intervals = [
            lease
                .checked_mul(100)
                .ok_or("statistics GC interval overflows")?,
            lease
                .checked_mul(20)
                .ok_or("statistics health interval overflows")?,
            Duration::from_millis(300), // memory.ReadMemInterval
        ];
        let active = Arc::clone(factory);
        let mut worker = Self::spawn_after_init(
            intervals,
            move || initialization.is_complete(),
            move |tick| match tick {
                Tick::Gc => {
                    if active
                        .stats_owner
                        .as_ref()
                        .is_some_and(|owner| owner.is_owner())
                    {
                        // This existing owner also checks the auto-analyze window
                        // after errors, matching the Go worker's continuation.
                        if let Err(error) = active.gc_stats(lease, schema_lease) {
                            eprintln!("GC stats failed: {error}");
                        }
                    }
                }
                Tick::Health => active.stats.update_stats_healthy_metrics(),
                Tick::Memory => {
                    tidb_util::memory::force_read_mem_stats();
                    active.stats.trigger_evict();
                }
            },
        )?;
        worker.factory = Some(Arc::clone(factory));
        Ok(Some(worker))
    }

    #[cfg(test)]
    fn spawn(
        intervals: [Duration; 3],
        tick: impl FnMut(Tick) + Send + 'static,
    ) -> Result<Self, String> {
        Self::spawn_after_init(intervals, || true, tick)
    }

    fn spawn_after_init(
        intervals: [Duration; 3],
        initialized: impl Fn() -> bool + Send + 'static,
        mut tick: impl FnMut(Tick) + Send + 'static,
    ) -> Result<Self, String> {
        let now = Instant::now();
        let mut deadlines = [now; 3];
        for (deadline, interval) in deadlines.iter_mut().zip(intervals) {
            if interval.is_zero() {
                return Err("statistics maintenance interval must be positive".to_owned());
            }
            *deadline = now
                .checked_add(interval)
                .ok_or("statistics maintenance deadline overflows")?;
        }
        let stop = Arc::new(UsageWorkerStop {
            stopped: Mutex::new(false),
            wake: Condvar::new(),
        });
        let running = Arc::clone(&stop);
        let thread = std::thread::Builder::new()
            .name("statistics-maintenance".to_owned())
            .spawn(move || {
                while !initialized() {
                    // Shutdown remains independent of a blocked statistics read.
                    if running.wait(Duration::from_millis(10)) {
                        return;
                    }
                }
                let start = Instant::now();
                for (deadline, interval) in deadlines.iter_mut().zip(intervals) {
                    *deadline = start + interval;
                }
                // Go recovers a panic at the worker boundary, retiring this
                // worker without aborting the server or restarting a partial pass.
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| loop {
                    let next = *deadlines.iter().min().unwrap();
                    if running.wait(next.saturating_duration_since(Instant::now())) {
                        return;
                    }
                    for (index, kind) in [Tick::Gc, Tick::Health, Tick::Memory]
                        .into_iter()
                        .enumerate()
                    {
                        let now = Instant::now();
                        if now < deadlines[index] {
                            continue;
                        }
                        if running.wait(Duration::ZERO) {
                            return;
                        }
                        // Drop missed ticks while retaining the ticker's phase,
                        // like time.Ticker when a GC pass runs longer than a lease.
                        let elapsed = now.duration_since(deadlines[index]).as_nanos();
                        let remaining =
                            intervals[index].as_nanos() - elapsed % intervals[index].as_nanos();
                        deadlines[index] = now
                            + Duration::new(
                                (remaining / 1_000_000_000) as u64,
                                (remaining % 1_000_000_000) as u32,
                            );
                        tick(kind);
                    }
                }));
                if result.is_err() {
                    eprintln!("statistics maintenance worker panicked");
                }
            })
            .map_err(|error| format!("start statistics maintenance: {error}"))?;
        Ok(Self {
            stop,
            thread: Some(thread),
            factory: None,
        })
    }

    pub(crate) fn shutdown(&mut self) {
        *self
            .stop
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.stop.wake.notify_all();
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                eprintln!("statistics maintenance worker failed during shutdown");
            }
        }
        if let Some(factory) = self.factory.take() {
            if let Some(owner) = &factory.stats_owner {
                owner.close();
            }
        }
    }
}

impl Drop for StatsMaintenanceWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn stats_maintenance_waits_for_initialization_and_close_interrupts_that_wait() {
        let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let active = Arc::clone(&ready);
        let (events, received) = mpsc::channel();
        let worker = StatsMaintenanceWorker::spawn_after_init(
            [Duration::from_millis(1); 3],
            move || active.load(std::sync::atomic::Ordering::Acquire),
            move |tick| {
                events.send(tick).unwrap();
            },
        )
        .unwrap();
        assert!(received.recv_timeout(Duration::from_millis(30)).is_err());
        ready.store(true, std::sync::atomic::Ordering::Release);
        assert_eq!(
            received.recv_timeout(Duration::from_secs(2)).unwrap(),
            Tick::Gc
        );
        drop(worker);

        let worker = StatsMaintenanceWorker::spawn_after_init(
            [Duration::from_secs(3600); 3],
            || false,
            |_| panic!("initialization is blocked"),
        )
        .unwrap();
        let start = Instant::now();
        drop(worker);
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn stats_maintenance_drives_all_tickers_and_joins_active_work() {
        let (events, received) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let mut first = true;
        let worker = StatsMaintenanceWorker::spawn([Duration::from_millis(1); 3], move |tick| {
            events.send(tick).unwrap();
            if first {
                first = false;
                blocked.recv().unwrap();
            }
        })
        .unwrap();
        assert_eq!(
            received.recv_timeout(Duration::from_secs(2)).unwrap(),
            Tick::Gc
        );
        let (finished, joined) = mpsc::channel();
        let closer = std::thread::spawn(move || {
            drop(worker);
            finished.send(()).unwrap();
        });
        assert!(joined.recv_timeout(Duration::from_millis(20)).is_err());
        release.send(()).unwrap();
        joined.recv_timeout(Duration::from_secs(2)).unwrap();
        closer.join().unwrap();
        assert!(
            received.try_recv().is_err(),
            "close suppresses ticks queued behind active GC"
        );

        let (events, received) = mpsc::channel();
        let mut worker =
            StatsMaintenanceWorker::spawn([Duration::from_millis(1); 3], move |tick| {
                events.send(tick).unwrap();
            })
            .unwrap();
        assert_eq!(
            received.recv_timeout(Duration::from_secs(2)).unwrap(),
            Tick::Gc
        );
        assert_eq!(
            received.recv_timeout(Duration::from_secs(2)).unwrap(),
            Tick::Health
        );
        assert_eq!(
            received.recv_timeout(Duration::from_secs(2)).unwrap(),
            Tick::Memory
        );
        worker.shutdown();
        worker.shutdown();
    }

    #[test]
    fn stats_maintenance_close_wakes_long_timers_and_recovers_worker_panics() {
        let worker = StatsMaintenanceWorker::spawn([Duration::from_secs(3600); 3], |_| {
            panic!("no tick before close");
        })
        .unwrap();
        let before = Instant::now();
        drop(worker);
        assert!(before.elapsed() < Duration::from_secs(1));
        let (entered, receiver) = mpsc::channel();
        let worker = StatsMaintenanceWorker::spawn([Duration::from_millis(1); 3], move |_| {
            entered.send(()).unwrap();
            panic!("injected GC panic");
        })
        .unwrap();
        receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        drop(worker);
    }
}
