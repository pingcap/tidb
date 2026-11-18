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

//! Go master `pkg/mvservice/backpressure.go`.

use tidb_timer::go_time::SECOND;

/// Go `defaultBackpressureCPUThreshold`.
pub const DEFAULT_BACKPRESSURE_CPU_THRESHOLD: f64 = 0.8;
/// Go `defaultBackpressureMemThreshold`.
pub const DEFAULT_BACKPRESSURE_MEM_THRESHOLD: f64 = 0.8;
/// Go `defaultTaskBackpressureDelay` (100ms), in nanoseconds.
pub const DEFAULT_TASK_BACKPRESSURE_DELAY: i64 = SECOND / 10;

/// Go `TaskBackpressureController`: whether task execution should wait.
pub trait TaskBackpressureController: Send + Sync {
    /// Go `ShouldBackpressure`: whether workers should stop picking new
    /// tasks for a while, and how long to wait before retrying.
    fn should_backpressure(&self) -> (bool, i64);
}

/// Go `CPUMemBackpressureController`: backpressure while CPU or memory
/// usage is over its threshold. Thresholds are fractions in [0, 1]; a
/// threshold <= 0 disables its check.
pub struct CpuMemBackpressureController {
    /// Go `CPUThreshold`.
    pub cpu_threshold: f64,
    /// Go `MemThreshold`.
    pub mem_threshold: f64,
    /// Go `Delay`.
    pub delay: i64,
    pub(crate) get_cpu_usage: Box<dyn Fn() -> (f64, bool) + Send + Sync>,
    pub(crate) get_mem_used: Box<dyn Fn() -> Result<u64, String> + Send + Sync>,
    pub(crate) get_mem_total: Box<dyn Fn() -> u64 + Send + Sync>,
}

impl CpuMemBackpressureController {
    /// Go `NewCPUMemBackpressureController`: Go `cpu.GetCPUUsage`,
    /// `memory.InstanceMemUsed` and `memory.GetMemTotalIgnoreErr` as probes.
    #[must_use]
    pub fn new(cpu_threshold: f64, mem_threshold: f64, delay: i64) -> Self {
        Self {
            cpu_threshold,
            mem_threshold,
            delay,
            get_cpu_usage: Box::new(tidb_cpu::get_cpu_usage),
            get_mem_used: Box::new(|| Ok(u64::try_from(tidb_memory::read_mem_stats().heap_inuse).unwrap_or(0))),
            get_mem_total: Box::new(|| tidb_memory::mem_total().unwrap_or(0)),
        }
    }

    /// Go `backpressureDelay`: the configured delay or the package default.
    fn backpressure_delay(&self) -> i64 {
        if self.delay > 0 {
            return self.delay;
        }
        DEFAULT_TASK_BACKPRESSURE_DELAY
    }
}

impl TaskBackpressureController for CpuMemBackpressureController {
    fn should_backpressure(&self) -> (bool, i64) {
        if self.cpu_threshold > 0.0 {
            let (usage, unsupported) = (self.get_cpu_usage)();
            if !unsupported && usage >= self.cpu_threshold {
                return (true, self.backpressure_delay());
            }
        }
        if self.mem_threshold > 0.0 {
            let total = (self.get_mem_total)();
            if total > 0 {
                if let Ok(used) = (self.get_mem_used)() {
                    if used as f64 / total as f64 >= self.mem_threshold {
                        return (true, self.backpressure_delay());
                    }
                }
            }
        }
        (false, 0)
    }
}
