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


//! Go `pkg/util/gctuner`'s settings: the GOGC tuner's bounds and the memory
//! limit tuner's GC-trigger percentage, which `tidb_gogc_tuner_max_value`,
//! `tidb_gogc_tuner_min_value` and `tidb_server_memory_limit_gc_trigger`
//! set and read back (`SetMaxGCPercent` / `MaxGCPercent`, ...).
//!
//! Go's tuners act on the Go runtime's collector (`debug.SetGCPercent`,
//! `debug.SetMemoryLimit`). This process has no collector, so the values are
//! the variables' process state and nothing else; there is nothing for them
//! to tune.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::SeqCst};

/// Go `defaultMaxGCPercent`.
const DEFAULT_MAX_GC_PERCENT: u32 = 500;
/// Go `defaultMinGCPercent`.
const DEFAULT_MIN_GC_PERCENT: u32 = 100;
/// Go `vardef.DefTiDBServerMemoryLimitGCTrigger`, the percentage the
/// variable's `SetGlobal` stores when the sysvar cache is first built.
const DEFAULT_MEMORY_LIMIT_PERCENTAGE: f64 = 0.7;

static MAX_GC_PERCENT: AtomicU32 = AtomicU32::new(DEFAULT_MAX_GC_PERCENT);
static MIN_GC_PERCENT: AtomicU32 = AtomicU32::new(DEFAULT_MIN_GC_PERCENT);
static MEMORY_LIMIT_PERCENTAGE: AtomicU64 = AtomicU64::new(DEFAULT_MEMORY_LIMIT_PERCENTAGE.to_bits());

/// Go `gctuner.SetMaxGCPercent`.
pub fn set_max_gc_percent(percent: u32) {
    MAX_GC_PERCENT.store(percent, SeqCst);
}

/// Go `gctuner.MaxGCPercent`.
#[must_use]
pub fn max_gc_percent() -> u32 {
    MAX_GC_PERCENT.load(SeqCst)
}

/// Go `gctuner.SetMinGCPercent`.
pub fn set_min_gc_percent(percent: u32) {
    MIN_GC_PERCENT.store(percent, SeqCst);
}

/// Go `gctuner.MinGCPercent`.
#[must_use]
pub fn min_gc_percent() -> u32 {
    MIN_GC_PERCENT.load(SeqCst)
}

/// Go `gctuner.GlobalMemoryLimitTuner.SetPercentage`.
pub fn set_memory_limit_percentage(percentage: f64) {
    MEMORY_LIMIT_PERCENTAGE.store(percentage.to_bits(), SeqCst);
}

/// Go `gctuner.GlobalMemoryLimitTuner.GetPercentage`.
#[must_use]
pub fn memory_limit_percentage() -> f64 {
    f64::from_bits(MEMORY_LIMIT_PERCENTAGE.load(SeqCst))
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_defaults_are_gos() {
        assert_eq!(super::memory_limit_percentage(), 0.7);
        assert_eq!((super::max_gc_percent(), super::min_gc_percent()), (500, 100));
    }
}
