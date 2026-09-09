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

//! Scheduler commands from `pkg/resourcemanager/scheduler`.

use std::sync::atomic::Ordering;
use std::time::SystemTime;

use crate::util::{Component, GoroutinePool, MIN_SCHEDULER_INTERVAL_NANOS};

/// Pool tuning command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum Command {
    /// Reduce concurrency.
    Downclock = 0,
    /// Keep concurrency unchanged.
    Hold = 1,
    /// Increase concurrency.
    Overclock = 2,
}

/// Pool scheduler.
pub trait Scheduler: Send + Sync {
    /// Chooses one tuning command.
    fn tune(&self, component: Component, pool: &dyn GoroutinePool) -> Command;
}

/// CPU-usage scheduler.
pub struct CpuScheduler;

impl CpuScheduler {
    /// Creates a CPU scheduler.
    pub fn new() -> Self {
        Self
    }
}

impl Scheduler for CpuScheduler {
    fn tune(&self, _component: Component, pool: &dyn GoroutinePool) -> Command {
        let elapsed = elapsed_nanos(pool.last_tuner_ts());
        let minimum = MIN_SCHEDULER_INTERVAL_NANOS.load(Ordering::Relaxed);
        if elapsed < minimum as i128 {
            return Command::Hold;
        }
        let (value, unsupported) = tidb_util::cpu::get_cpu_usage();
        if unsupported {
            return Command::Hold;
        }
        if value < 0.5 {
            Command::Overclock
        } else if value > 0.7 {
            Command::Downclock
        } else {
            Command::Hold
        }
    }
}

fn elapsed_nanos(since: SystemTime) -> i128 {
    match SystemTime::now().duration_since(since) {
        Ok(elapsed) => elapsed.as_nanos() as i128,
        Err(error) => -(error.duration().as_nanos() as i128),
    }
}
