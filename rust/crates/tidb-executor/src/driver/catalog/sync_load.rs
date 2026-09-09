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

//! Go `pkg/statistics/handle/syncload`: per-item synchronous statistics load
//! coordination, including global singleflight, urgent/expired queues, and
//! the one-retry worker contract.

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, LazyLock, Mutex, Weak,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::{StatisticsCache, StatisticsItemLoader};

const RETRY_COUNT: usize = 1;

/// Go `GetSyncLoadConcurrencyByCPU`.
#[must_use]
pub(crate) fn concurrency_by_cpu() -> usize {
    let cores = std::thread::available_parallelism().map_or(1, usize::from);
    match cores {
        0..=8 => 5,
        9..=16 => 6,
        17..=32 => 8,
        _ => 10,
    }
}

/// One result delivered by Go's `singleflight.DoChan`.
///
/// A transport error comes from queue admission or the singleflight
/// operation's own timer. An item result means a worker consumed the task;
/// its optional load error is diagnostic and does not make
/// `SyncWaitStatsLoad` fail.
#[derive(Clone, Debug)]
pub(crate) enum SyncLoadOutcome {
    TransportError(String),
    Item {
        item: tidb_model::TableItemID,
        error: Option<String>,
    },
}

struct NeededItemTask {
    item: tidb_model::StatsLoadItem,
    resource_group: String,
    to_timeout: Instant,
    result: mpsc::SyncSender<SyncLoadOutcome>,
    loader: Arc<dyn StatisticsItemLoader>,
    cache: Weak<StatisticsCache>,
}

type Listener = mpsc::SyncSender<SyncLoadOutcome>;

static GLOBAL_SINGLEFLIGHT: LazyLock<Mutex<HashMap<String, Vec<Listener>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// One statistics handle's synchronous-load queues and workers.
struct SyncLoadPool {
    needed_items: mpsc::SyncSender<NeededItemTask>,
    _needed_items_receiver: Arc<Mutex<mpsc::Receiver<NeededItemTask>>>,
    _timeout_items_receiver: Arc<Mutex<mpsc::Receiver<NeededItemTask>>>,
    stop: Arc<AtomicBool>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl SyncLoadPool {
    fn new(concurrency: usize, queue_size: usize, retry_backoff: Duration) -> Arc<Self> {
        let (needed_tx, needed_rx) = mpsc::sync_channel(queue_size);
        let (timeout_tx, timeout_rx) = mpsc::sync_channel(queue_size);
        let needed_rx = Arc::new(Mutex::new(needed_rx));
        let timeout_rx = Arc::new(Mutex::new(timeout_rx));
        let stop = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let needed_rx = Arc::clone(&needed_rx);
            let timeout_rx = Arc::clone(&timeout_rx);
            let timeout_tx = timeout_tx.clone();
            let stop = Arc::clone(&stop);
            workers.push(std::thread::spawn(move || {
                worker_loop(needed_rx, timeout_rx, timeout_tx, retry_backoff, stop);
            }));
        }
        Arc::new(Self {
            needed_items: needed_tx,
            _needed_items_receiver: needed_rx,
            _timeout_items_receiver: timeout_rx,
            stop,
            workers,
        })
    }
}

impl Drop for SyncLoadPool {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

pub(crate) struct SyncLoadService {
    pool: Arc<SyncLoadPool>,
    loader: Arc<dyn StatisticsItemLoader>,
    cache: Weak<StatisticsCache>,
}

impl SyncLoadService {
    pub(super) fn new(
        loader: Arc<dyn StatisticsItemLoader>,
        cache: Weak<StatisticsCache>,
    ) -> Arc<Self> {
        let performance = tidb_config::config_tree::config::get_global_config().performance;
        let concurrency = match performance.stats_load_concurrency {
            configured if configured < 0 => 0,
            0 => concurrency_by_cpu(),
            configured => usize::try_from(configured).unwrap_or(usize::MAX),
        };
        let lease = serde_json::from_value::<tidb_config::configtypes::Duration>(
            serde_json::Value::String(performance.stats_lease),
        )
        .ok()
        .and_then(|duration| u64::try_from(duration.0).ok())
        .map(Duration::from_nanos)
        .filter(|duration| !duration.is_zero())
        .unwrap_or(Duration::from_secs(3));
        let pool = SyncLoadPool::new(concurrency, performance.stats_load_queue_size, lease / 10);
        Self::with_pool(loader, cache, pool)
    }

    fn with_settings(
        loader: Arc<dyn StatisticsItemLoader>,
        cache: Weak<StatisticsCache>,
        concurrency: usize,
        queue_size: usize,
        retry_backoff: Duration,
    ) -> Arc<Self> {
        Self::with_pool(
            loader,
            cache,
            SyncLoadPool::new(concurrency, queue_size, retry_backoff),
        )
    }

    fn with_pool(
        loader: Arc<dyn StatisticsItemLoader>,
        cache: Weak<StatisticsCache>,
        pool: Arc<SyncLoadPool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            pool,
            loader,
            cache,
        })
    }

    pub(crate) fn request(
        self: &Arc<Self>,
        items: &[tidb_model::StatsLoadItem],
        resource_group: &str,
        timeout: Duration,
    ) -> Vec<mpsc::Receiver<SyncLoadOutcome>> {
        items
            .iter()
            .map(|item| self.request_one(*item, resource_group, timeout))
            .collect()
    }

    fn request_one(
        self: &Arc<Self>,
        item: tidb_model::StatsLoadItem,
        resource_group: &str,
        timeout: Duration,
    ) -> mpsc::Receiver<SyncLoadOutcome> {
        let (listener_tx, listener_rx) = mpsc::sync_channel(1);
        let key = item.key();
        {
            let mut flights = GLOBAL_SINGLEFLIGHT
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(listeners) = flights.get_mut(&key) {
                listeners.push(listener_tx);
                return listener_rx;
            }
            flights.insert(key.clone(), vec![listener_tx]);
        }

        let service = Arc::clone(self);
        let resource_group = resource_group.to_owned();
        std::thread::spawn(move || {
            let outcome = service.run_flight(item, resource_group, timeout);
            let mut flights = GLOBAL_SINGLEFLIGHT
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let listeners = flights.remove(&key).unwrap_or_default();
            for listener in listeners {
                let _ = listener.send(outcome.clone());
            }
        });
        listener_rx
    }

    fn run_flight(
        &self,
        item: tidb_model::StatsLoadItem,
        resource_group: String,
        timeout: Duration,
    ) -> SyncLoadOutcome {
        let now = Instant::now();
        let deadline = now.checked_add(timeout).unwrap_or(now);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let mut task = NeededItemTask {
            item,
            resource_group,
            to_timeout: deadline,
            result: result_tx,
            loader: Arc::clone(&self.loader),
            cache: self.cache.clone(),
        };
        loop {
            match self.pool.needed_items.try_send(task) {
                Ok(()) => break,
                Err(mpsc::TrySendError::Full(returned)) => {
                    task = returned;
                    if Instant::now() >= deadline {
                        return SyncLoadOutcome::TransportError(
                            "sync load stats channel is full and timeout sending task to channel"
                                .to_owned(),
                        );
                    }
                    std::thread::sleep(Duration::from_micros(100));
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    return SyncLoadOutcome::TransportError(
                        "sync load stats channel closed unexpectedly".to_owned(),
                    );
                }
            }
        }
        wait_for_task_result(result_rx, deadline)
    }
}

fn wait_for_task_result(
    result_rx: mpsc::Receiver<SyncLoadOutcome>,
    deadline: Instant,
) -> SyncLoadOutcome {
    let remaining = deadline.saturating_duration_since(Instant::now());
    match result_rx.recv_timeout(remaining) {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            SyncLoadOutcome::TransportError("sync load took too long to return".to_owned())
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            // Go's request goroutine retains task.ResultCh even when an expired
            // task is dropped because the timeout queue is full. In that case
            // only the original request timer completes the request.
            std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
            SyncLoadOutcome::TransportError("sync load took too long to return".to_owned())
        }
    }
}

fn worker_loop(
    needed_rx: Arc<Mutex<mpsc::Receiver<NeededItemTask>>>,
    timeout_rx: Arc<Mutex<mpsc::Receiver<NeededItemTask>>>,
    timeout_tx: mpsc::SyncSender<NeededItemTask>,
    retry_backoff: Duration,
    stop: Arc<AtomicBool>,
) {
    while let Some(task) = drain_task(&needed_rx, &timeout_rx, &timeout_tx, &stop) {
        handle_task(task, retry_backoff);
    }
}

fn drain_task(
    needed_rx: &Mutex<mpsc::Receiver<NeededItemTask>>,
    timeout_rx: &Mutex<mpsc::Receiver<NeededItemTask>>,
    timeout_tx: &mpsc::SyncSender<NeededItemTask>,
    stop: &AtomicBool,
) -> Option<NeededItemTask> {
    loop {
        if stop.load(Ordering::Acquire) {
            return None;
        }
        if let Ok(task) = needed_rx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .try_recv()
        {
            if Instant::now() > task.to_timeout {
                let _ = timeout_tx.try_send(task);
                continue;
            }
            return Some(task);
        }
        if let Ok(task) = timeout_rx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .try_recv()
        {
            if let Ok(urgent) = needed_rx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .try_recv()
            {
                let _ = timeout_tx.try_send(task);
                return Some(urgent);
            }
            return Some(task);
        }
        match needed_rx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv_timeout(Duration::from_millis(10))
        {
            Ok(task) if Instant::now() > task.to_timeout => {
                let _ = timeout_tx.try_send(task);
            }
            Ok(task) => return Some(task),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return None,
        }
    }
}

fn handle_task(task: NeededItemTask, retry_backoff: Duration) {
    let mut error = None;
    for retry in 0..=RETRY_COUNT {
        let loaded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            task.loader
                .load_items(std::slice::from_ref(&task.item), &task.resource_group)
        }))
        .unwrap_or_else(|panic| {
            let message = panic.downcast_ref::<&str>().map_or_else(
                || {
                    panic
                        .downcast_ref::<String>()
                        .cloned()
                        .unwrap_or_else(|| "unknown panic".to_owned())
                },
                |message| (*message).to_owned(),
            );
            Err(format!("stats loading panicked: {message}"))
        });
        match loaded {
            Ok(tables) => {
                if let Some(cache) = task.cache.upgrade() {
                    let mut values = cache
                        .values
                        .write()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    for (table_id, statistics) in tables {
                        values.insert(table_id, statistics);
                    }
                }
                error = None;
                break;
            }
            Err(load_error) => {
                error = Some(load_error);
                if retry < RETRY_COUNT {
                    let jitter = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_or(0, |now| u64::from(now.subsec_nanos()) % 500);
                    std::thread::sleep(retry_backoff + Duration::from_micros(jitter));
                }
            }
        }
    }
    let _ = task.result.send(SyncLoadOutcome::Item {
        item: task.item.table_item_id,
        error,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct TestLoader {
        calls: std::sync::atomic::AtomicUsize,
        failures_remaining: std::sync::atomic::AtomicUsize,
        panics_remaining: std::sync::atomic::AtomicUsize,
        delay: Duration,
    }

    impl StatisticsItemLoader for TestLoader {
        fn load_items(
            &self,
            _items: &[tidb_model::StatsLoadItem],
            _resource_group: &str,
        ) -> Result<Vec<(i64, Arc<crate::access_cost::TableStatistics>)>, String> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if !self.delay.is_zero() {
                std::thread::sleep(self.delay);
            }
            if self
                .panics_remaining
                .try_update(
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok()
            {
                panic!("injected statistics load panic");
            }
            if self
                .failures_remaining
                .try_update(
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok()
            {
                return Err("injected statistics load failure".to_owned());
            }
            Ok(Vec::new())
        }
    }

    struct BlockingLoader {
        started: mpsc::SyncSender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        finished: Arc<AtomicBool>,
    }

    impl StatisticsItemLoader for BlockingLoader {
        fn load_items(
            &self,
            _items: &[tidb_model::StatsLoadItem],
            _resource_group: &str,
        ) -> Result<Vec<(i64, Arc<crate::access_cost::TableStatistics>)>, String> {
            self.started.send(()).expect("test receives worker start");
            self.release
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .recv()
                .expect("test releases worker");
            self.finished.store(true, Ordering::Release);
            Ok(Vec::new())
        }
    }

    fn item(id: i64, full_load: bool) -> tidb_model::StatsLoadItem {
        tidb_model::StatsLoadItem {
            table_item_id: tidb_model::TableItemID {
                table_id: 90_000 + id,
                id,
                is_index: false,
                is_sync_load_failed: false,
            },
            full_load,
        }
    }

    fn task(id: i64, to_timeout: Instant) -> NeededItemTask {
        let (result, _receiver) = mpsc::sync_channel(1);
        NeededItemTask {
            item: item(id, true),
            resource_group: "rg".to_owned(),
            to_timeout,
            result,
            loader: Arc::new(TestLoader::default()),
            cache: Weak::new(),
        }
    }

    fn service(loader: Arc<TestLoader>) -> (Arc<StatisticsCache>, Arc<SyncLoadService>) {
        let cache = Arc::new(StatisticsCache::default());
        let service =
            SyncLoadService::with_settings(loader, Arc::downgrade(&cache), 1, 8, Duration::ZERO);
        (cache, service)
    }

    #[test]
    fn production_services_own_independent_worker_pools() {
        let first_cache = Arc::new(StatisticsCache::default());
        let second_cache = Arc::new(StatisticsCache::default());
        let first = SyncLoadService::new(
            Arc::new(TestLoader::default()),
            Arc::downgrade(&first_cache),
        );
        let second = SyncLoadService::new(
            Arc::new(TestLoader::default()),
            Arc::downgrade(&second_cache),
        );

        assert!(!Arc::ptr_eq(&first.pool, &second.pool));
    }

    #[test]
    fn dropping_a_worker_pool_waits_for_an_active_worker() {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let finished = Arc::new(AtomicBool::new(false));
        let loader = Arc::new(BlockingLoader {
            started: started_tx,
            release: Mutex::new(release_rx),
            finished: Arc::clone(&finished),
        });
        let pool = SyncLoadPool::new(1, 1, Duration::ZERO);
        let (result, _receiver) = mpsc::sync_channel(1);
        pool.needed_items
            .send(NeededItemTask {
                item: item(100, true),
                resource_group: "rg".to_owned(),
                to_timeout: Instant::now() + Duration::from_secs(1),
                result,
                loader,
                cache: Weak::new(),
            })
            .expect("queue task");
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker starts task");
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            release_tx.send(()).expect("release worker");
        });

        drop(pool);
        let finished_when_drop_returned = finished.load(Ordering::Acquire);
        releaser.join().expect("releaser exits");
        assert!(finished_when_drop_returned);
    }

    #[test]
    fn cpu_concurrency_matches_go_thresholds_for_the_current_machine() {
        assert!(matches!(concurrency_by_cpu(), 5 | 6 | 8 | 10));
    }

    #[test]
    fn concurrent_identical_requests_share_one_load() {
        let loader = Arc::new(TestLoader {
            delay: Duration::from_millis(30),
            ..TestLoader::default()
        });
        let (_cache, service) = service(Arc::clone(&loader));
        let first = service.request(&[item(101, true)], "rg", Duration::from_secs(1));
        let second = service.request(&[item(101, true)], "rg", Duration::from_secs(1));

        assert!(matches!(
            first[0].recv_timeout(Duration::from_secs(1)).unwrap(),
            SyncLoadOutcome::Item { error: None, .. }
        ));
        assert!(matches!(
            second[0].recv_timeout(Duration::from_secs(1)).unwrap(),
            SyncLoadOutcome::Item { error: None, .. }
        ));
        assert_eq!(loader.calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn worker_retries_one_failure_or_panic_once() {
        for (id, fail, panic) in [(102, 1, 0), (103, 0, 1)] {
            let loader = Arc::new(TestLoader {
                failures_remaining: std::sync::atomic::AtomicUsize::new(fail),
                panics_remaining: std::sync::atomic::AtomicUsize::new(panic),
                ..TestLoader::default()
            });
            let (_cache, service) = service(Arc::clone(&loader));
            let receivers = service.request(&[item(id, true)], "rg", Duration::from_secs(1));
            assert!(matches!(
                receivers[0].recv_timeout(Duration::from_secs(1)).unwrap(),
                SyncLoadOutcome::Item { error: None, .. }
            ));
            assert_eq!(loader.calls.load(std::sync::atomic::Ordering::Relaxed), 2);
        }
    }

    #[test]
    fn second_failure_is_reported_as_an_item_result() {
        let loader = Arc::new(TestLoader {
            failures_remaining: std::sync::atomic::AtomicUsize::new(2),
            ..TestLoader::default()
        });
        let (_cache, service) = service(Arc::clone(&loader));
        let receivers = service.request(&[item(104, true)], "rg", Duration::from_secs(1));
        assert!(matches!(
            receivers[0].recv_timeout(Duration::from_secs(1)).unwrap(),
            SyncLoadOutcome::Item { error: Some(_), .. }
        ));
        assert_eq!(loader.calls.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    #[test]
    fn full_needed_queue_times_out_admission() {
        let loader = Arc::new(TestLoader::default());
        let cache = Arc::new(StatisticsCache::default());
        let service =
            SyncLoadService::with_settings(loader, Arc::downgrade(&cache), 0, 1, Duration::ZERO);
        service
            .pool
            .needed_items
            .send(task(105, Instant::now() + Duration::from_secs(1)))
            .expect("fill the sole queue slot");
        let rejected = service.request(&[item(106, true)], "rg", Duration::from_millis(2));
        assert!(matches!(
            rejected[0]
                .recv_timeout(Duration::from_secs(1))
                .unwrap(),
            SyncLoadOutcome::TransportError(error) if error.contains("channel is full")
        ));
    }

    #[test]
    fn urgent_queue_preempts_an_expired_task() {
        let (needed_tx, needed_rx) = mpsc::sync_channel(2);
        let (timeout_tx, timeout_rx) = mpsc::sync_channel(2);
        timeout_tx
            .send(task(107, Instant::now()))
            .expect("expired task");
        needed_tx
            .send(task(108, Instant::now() + Duration::from_secs(1)))
            .expect("urgent task");

        let stop = AtomicBool::new(false);
        let drained = drain_task(
            &Mutex::new(needed_rx),
            &Mutex::new(timeout_rx),
            &timeout_tx,
            &stop,
        )
        .expect("one task");
        assert_eq!(drained.item, item(108, true));
    }

    #[test]
    fn dropped_expired_task_waits_for_the_singleflight_timer() {
        let (sender, receiver) = mpsc::sync_channel(1);
        drop(sender);
        let started = Instant::now();
        let outcome = wait_for_task_result(receiver, started + Duration::from_millis(20));
        assert!(started.elapsed() >= Duration::from_millis(15));
        assert!(matches!(
            outcome,
            SyncLoadOutcome::TransportError(error)
                if error == "sync load took too long to return"
        ));
    }
}
