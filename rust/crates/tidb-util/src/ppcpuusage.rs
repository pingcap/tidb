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

//! Complete transcreation of Go `pkg/util/ppcpuusage` (`cpuusages.go`).
//!
//! A [`Mutex`] serializes the per-SQL state, TiDB time is accepted only for
//! the current SQL ID, and TiKV time is always accumulated. The SQL ID wraps
//! to zero.

use std::sync::Mutex;
use std::time::Duration;

/// Records TiDB/TiKV CPU usages.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CpuUsages {
    /// CPU time spent in TiDB.
    pub tidb_cpu_time: Duration,
    /// CPU time spent in TiKV.
    pub tikv_cpu_time: Duration,
}

impl CpuUsages {
    /// Resets all CPU times to 0.
    pub fn reset(&mut self) {
        self.tikv_cpu_time = Duration::ZERO;
        self.tidb_cpu_time = Duration::ZERO;
    }
}

#[derive(Default)]
struct Inner {
    sql_id: u64,
    cpu_usages: CpuUsages,
}

/// Records a SQL ID and its CPU usages.
#[derive(Default)]
pub struct SqlCpuUsages {
    inner: Mutex<Inner>,
}

impl SqlCpuUsages {
    /// Sets the CPU-usages value.
    pub fn set_cpu_usages(&self, usage: CpuUsages) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cpu_usages = usage;
    }

    /// Merges TiDB CPU time into self when `sql_id` matches.
    ///
    /// The ID is checked here because TiDB CPU time can only be collected by
    /// the profiler now, and is updated from concurrent threads.
    pub fn merge_tidb_cpu_time(&self, sql_id: u64, d: Duration) {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inner.sql_id == sql_id {
            inner.cpu_usages.tidb_cpu_time += d;
        }
    }

    /// Merges TiKV CPU time into self.
    ///
    /// No SQL-ID check is needed because TiKV CPU time is updated in
    /// executors now.
    pub fn merge_tikv_cpu_time(&self, d: Duration) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cpu_usages
            .tikv_cpu_time += d;
    }

    /// Returns the TiDB/TiKV CPU times.
    #[must_use]
    pub fn get_cpu_usages(&self) -> CpuUsages {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cpu_usages
    }

    /// Allocates a new ID; restarts from 0 when it exceeds the `u64` limit.
    pub fn alloc_new_sql_id(&self) -> u64 {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.sql_id = inner.sql_id.wrapping_add(1);
        inner.sql_id
    }

    /// Resets the TiDB/TiKV CPU times to 0.
    pub fn reset_cpu_times(&self) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cpu_usages
            .reset();
    }
}
