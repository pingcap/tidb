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


//! Go `executor.GlobalAnalyzeMemoryTracker` (`pkg/executor/select.go`): the
//! ancestor of every ANALYZE job's memory tracker, whose limit is
//! `tidb_mem_quota_analyze` and whose action is `globalPanicOnExceed`.
//!
//! Go keeps the tracker in `executor` and registers its `SetBytesLimit` /
//! `GetBytesLimit` into `variable.SetMemQuotaAnalyze` /
//! `GetMemQuotaAnalyze` from `executor`'s `init`, because `variable` cannot
//! import `executor`. The same cycle holds here, and the tracker lives beside
//! the memory package's other process-wide state so the variable's hooks read
//! it directly, with no registration step a process could miss.

use std::sync::{Arc, LazyLock};

use crate::action::{ActionOnExceed, ArcAction, BaseOomAction, DEF_PANIC_PRIORITY};
use crate::tracker::{
    Tracker, LABEL_FOR_GLOBAL_ANALYZE_MEMORY, LABEL_FOR_GLOBAL_STORAGE,
};

/// Go `memory.LabelForAnalyzeMemory`: the label of each ANALYZE statement's
/// memory tracker.
pub const LABEL_FOR_ANALYZE_MEMORY: i64 = -24;

/// Go `memory.LabelForGlobalMemory`.
pub const LABEL_FOR_GLOBAL_MEMORY: i64 = -12;

/// Go `globalPanicStorageExceed`.
pub const GLOBAL_PANIC_STORAGE_EXCEED: &str = "Out Of Quota For Local Temporary Space!";
/// Go `globalPanicMemoryExceed`.
pub const GLOBAL_PANIC_MEMORY_EXCEED: &str = "Out Of Global Memory Limit!";
/// Go `globalPanicAnalyzeMemoryExceed`.
pub const GLOBAL_PANIC_ANALYZE_MEMORY_EXCEED: &str = "Out Of Global Analyze Memory Limit!";

/// Go `globalPanicOnExceed`: panics with the quota the tracker's label names.
#[derive(Default)]
pub struct GlobalPanicOnExceed {
    base: BaseOomAction,
    mutex: std::sync::Mutex<()>,
}

impl ActionOnExceed for GlobalPanicOnExceed {
    fn action(&self, t: &Arc<Tracker>) {
        let _guard = self.mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let msg = match t.label() {
            LABEL_FOR_GLOBAL_STORAGE => GLOBAL_PANIC_STORAGE_EXCEED,
            LABEL_FOR_GLOBAL_MEMORY => GLOBAL_PANIC_MEMORY_EXCEED,
            LABEL_FOR_GLOBAL_ANALYZE_MEMORY => GLOBAL_PANIC_ANALYZE_MEMORY_EXCEED,
            _ => "Out of Unknown Resource Quota!",
        };
        std::panic::panic_any(msg.to_owned());
    }
    fn set_fallback(&self, a: Option<ArcAction>) {
        self.base.set_fallback(a);
    }
    fn get_fallback(&self) -> Option<ArcAction> {
        self.base.get_fallback()
    }
    fn get_priority(&self) -> i64 {
        DEF_PANIC_PRIORITY
    }
    fn set_finished(&self) {
        self.base.set_finished();
    }
    fn is_finished(&self) -> bool {
        self.base.is_finished()
    }
}

/// Go `GlobalAnalyzeMemoryTracker = memory.NewTracker(
/// memory.LabelForGlobalAnalyzeMemory, -1)` with `globalPanicOnExceed`.
pub static GLOBAL_ANALYZE_MEMORY_TRACKER: LazyLock<Arc<Tracker>> = LazyLock::new(|| {
    let tracker = Tracker::new(LABEL_FOR_GLOBAL_ANALYZE_MEMORY, -1);
    tracker.set_action_on_exceed(Some(Arc::new(GlobalPanicOnExceed::default())));
    tracker
});

/// Go `variable.SetMemQuotaAnalyze` (`GlobalAnalyzeMemoryTracker.SetBytesLimit`).
pub fn set_mem_quota_analyze(quota: i64) {
    GLOBAL_ANALYZE_MEMORY_TRACKER.set_bytes_limit(quota);
}

/// Go `variable.GetMemQuotaAnalyze` (`GlobalAnalyzeMemoryTracker.GetBytesLimit`).
#[must_use]
pub fn get_mem_quota_analyze() -> i64 {
    GLOBAL_ANALYZE_MEMORY_TRACKER.get_bytes_limit()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `globalPanicOnExceed.Action`: an ANALYZE tracker under the global
    /// one panics with the analyze quota's text once the limit is reached.
    #[test]
    fn exceeding_the_global_analyze_limit_panics_with_gos_text() {
        let global = Tracker::new(LABEL_FOR_GLOBAL_ANALYZE_MEMORY, 100);
        global.set_action_on_exceed(Some(Arc::new(GlobalPanicOnExceed::default())));
        let job = Tracker::new(LABEL_FOR_ANALYZE_MEMORY, -1);
        job.attach_to(&global);
        job.consume(99);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.consume(1))).unwrap_err();
        assert_eq!(panic.downcast_ref::<String>().map(String::as_str), Some(GLOBAL_PANIC_ANALYZE_MEMORY_EXCEED));
        assert_eq!(global.bytes_consumed(), 100);
    }
}
