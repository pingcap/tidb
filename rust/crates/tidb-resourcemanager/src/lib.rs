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

//! Local instance resource manager from `pkg/resourcemanager`.
//!
//! Ported whole against the baseline, including the CPU scheduling surface
//! Go gets from `pkg/util/cpu` (usage EMA observer, GOMAXPROCS install),
//! which lives in `tidb_util::cpu` here.

pub mod pool;
pub mod poolmanager;
pub mod scheduler;
pub mod spool;
pub mod util;
pub mod workerpool;

use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

use scheduler::{Command, CpuScheduler, Scheduler};
use util::{
    Component, GoroutinePool, PoolContainer, ShardPoolMap, MAX_OVERCLOCK_COUNT,
    MIN_SCHEDULER_INTERVAL_NANOS,
};

/// Returns a random UUID name used by source tests.
pub fn random_name() -> String {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).expect("the OS entropy source is available");
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

/// Local instance resource manager.
pub struct ResourceManager {
    pool_map: Arc<RwLock<Arc<ShardPoolMap>>>,
    schedulers: Arc<Vec<Box<dyn Scheduler>>>,
    cpu_observer: Arc<tidb_util::cpu::Observer>,
    lifecycle: Mutex<Lifecycle>,
}

struct Lifecycle {
    stopped: bool,
    threads: Vec<ManagedThread>,
}

struct ManagedThread {
    stop: Option<crossbeam_channel::Sender<()>>,
    thread: JoinHandle<()>,
}

impl ResourceManager {
    /// Creates a resource manager.
    pub fn new() -> Self {
        Self {
            pool_map: Arc::new(RwLock::new(Arc::new(ShardPoolMap::new()))),
            schedulers: Arc::new(vec![Box::new(CpuScheduler::new())]),
            cpu_observer: Arc::new(tidb_util::cpu::Observer::new()),
            lifecycle: Mutex::new(Lifecycle {
                stopped: false,
                threads: Vec::new(),
            }),
        }
    }

    /// Starts CPU observation and 100 ms scheduling.
    pub fn start(&self) {
        let cpu_observer = Arc::clone(&self.cpu_observer);
        let cpu_start = std::thread::spawn(move || cpu_observer.start());
        let pool_map = Arc::clone(&self.pool_map);
        let schedulers = Arc::clone(&self.schedulers);
        let (stop, receiver) = crossbeam_channel::bounded(1);
        let keepalive = stop.clone();
        let ticker = crossbeam_channel::tick(Duration::from_millis(100));
        let thread = std::thread::spawn(move || {
            let _keepalive = keepalive;
            loop {
                crossbeam_channel::select! {
                    recv(receiver) -> _ => return,
                    recv(ticker) -> _ => schedule(&pool_map, &schedulers),
                }
            }
        });
        let mut lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if lifecycle.stopped {
            let _ = stop.send(());
            let _ = cpu_start.join();
            let _ = thread.join();
            return;
        }
        lifecycle.threads.push(ManagedThread {
            stop: None,
            thread: cpu_start,
        });
        lifecycle.threads.push(ManagedThread {
            stop: Some(stop),
            thread,
        });
    }

    /// Stops observation and scheduling.
    pub fn stop(&self) {
        self.cpu_observer.stop();
        let threads = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(!lifecycle.stopped, "close of closed resource manager");
            lifecycle.stopped = true;
            std::mem::take(&mut lifecycle.threads)
        };
        for ManagedThread { stop, thread } in threads {
            if let Some(stop) = stop {
                let _ = stop.send(());
            }
            let _ = thread.join();
        }
    }

    /// Registers a pool.
    pub fn register(
        &self,
        pool: Arc<dyn GoroutinePool>,
        name: String,
        component: Component,
    ) -> Result<(), &'static str> {
        self.register_pool(name, Arc::new(PoolContainer { pool, component }))
    }

    fn register_pool(&self, name: String, pool: Arc<PoolContainer>) -> Result<(), &'static str> {
        self.pool_map
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .add(name, pool)
    }

    /// Unregisters a pool.
    pub fn unregister(&self, name: &str) {
        self.pool_map
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .delete(name);
    }

    /// Resets the pool registry for tests.
    pub fn reset(&self) {
        *self
            .pool_map
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(ShardPoolMap::new());
    }

    /// Executes one scheduler command.
    pub fn exec(&self, pool: &PoolContainer, command: Command) {
        exec(pool, command);
    }
}

fn schedule(pool_map: &RwLock<Arc<ShardPoolMap>>, schedulers: &[Box<dyn Scheduler>]) {
    pool_map
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter(|pool| {
            if pool.component != Component::DistTask {
                exec(pool, schedule_pool(schedulers, pool));
            }
        });
}

fn schedule_pool(schedulers: &[Box<dyn Scheduler>], pool: &PoolContainer) -> Command {
    if pool.pool.running() == 0 {
        return Command::Hold;
    }
    for scheduler in schedulers {
        let command = scheduler.tune(pool.component, pool.pool.as_ref());
        if command == Command::Hold {
            continue;
        }
        if command == Command::Downclock
            && (pool.pool.cap() == 1 || pool.pool.running() > pool.pool.cap())
        {
            continue;
        }
        return command;
    }
    Command::Hold
}

fn exec(pool: &PoolContainer, command: Command) {
    if command == Command::Hold {
        return;
    }
    let elapsed = elapsed_nanos(pool.pool.last_tuner_ts());
    let minimum = MIN_SCHEDULER_INTERVAL_NANOS.load(std::sync::atomic::Ordering::Relaxed);
    if elapsed <= minimum as i128 {
        return;
    }
    let concurrency = pool.pool.cap();
    match command {
        Command::Downclock => {
            let next = concurrency.wrapping_sub(1);
            tracing::debug!(
                category = "resource manager",
                origin_concurrency = concurrency,
                concurrency = next,
                name = pool.pool.name(),
                "downclock goroutine pool"
            );
            pool.pool.tune(next);
        }
        Command::Overclock => {
            let next = concurrency.wrapping_add(1);
            if next
                > pool
                    .pool
                    .origin_concurrency()
                    .wrapping_add(MAX_OVERCLOCK_COUNT.load(std::sync::atomic::Ordering::Relaxed))
            {
                return;
            }
            tracing::debug!(
                category = "resource manager",
                origin_concurrency = concurrency,
                concurrency = next,
                name = pool.pool.name(),
                "overclock goroutine pool"
            );
            pool.pool.tune(next);
        }
        Command::Hold => {}
    }
}

fn elapsed_nanos(since: SystemTime) -> i128 {
    match SystemTime::now().duration_since(since) {
        Ok(elapsed) => elapsed.as_nanos() as i128,
        Err(error) => -(error.duration().as_nanos() as i128),
    }
}

/// Returns the process-global resource manager.
pub fn instance_resource_manager() -> &'static ResourceManager {
    static INSTANCE: OnceLock<ResourceManager> = OnceLock::new();
    INSTANCE.get_or_init(ResourceManager::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::MockGPool;

    #[test]
    fn test_scheduler_overload_too_much() {
        let manager = ResourceManager::new();
        let pool: Arc<dyn GoroutinePool> = Arc::new(MockGPool::new("test", 1));
        let container = PoolContainer {
            pool,
            component: Component::Ddl,
        };
        manager.exec(&container, Command::Overclock);
        assert_eq!(2, container.pool.cap());
        manager.exec(&container, Command::Overclock);
        assert_eq!(2, container.pool.cap());
    }
}
