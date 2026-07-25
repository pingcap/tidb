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

//! Complete transcreation of Go `pkg/util/ppcpuusage` (`cpuusages.go`):
//! per-SQL TiDB/TiKV CPU-time accounting.
//!
//! Go embeds a `sync.Mutex` in `SQLCPUUsages` and guards every method; the
//! Rust equal is a [`Mutex`] around the mutable state, keeping every method
//! `&self` exactly as Go's pointer receivers are shared. `time.Duration`
//! maps to [`Duration`]. Go's `sqlID++` on a `uint64` wraps on overflow
//! ("will restart from 0 when exceeds uint64 max limit"), so the Rust
//! counter uses `wrapping_add`.
//!
//! The Go package ships no test; the tests below pin its observable
//! contract (sql-ID-gated TiDB merge, ungated TiKV merge, reset, and the
//! wrap-around) rather than leaving the port unverified.

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
        self.inner.lock().unwrap().cpu_usages = usage;
    }

    /// Merges TiDB CPU time into self when `sql_id` matches.
    ///
    /// The ID is checked here because TiDB CPU time can only be collected by
    /// the profiler now, and is updated from concurrent threads.
    pub fn merge_tidb_cpu_time(&self, sql_id: u64, d: Duration) {
        let mut inner = self.inner.lock().unwrap();
        if inner.sql_id == sql_id {
            inner.cpu_usages.tidb_cpu_time += d;
        }
    }

    /// Merges TiKV CPU time into self.
    ///
    /// No SQL-ID check is needed because TiKV CPU time is updated in
    /// executors now.
    pub fn merge_tikv_cpu_time(&self, d: Duration) {
        self.inner.lock().unwrap().cpu_usages.tikv_cpu_time += d;
    }

    /// Returns the TiDB/TiKV CPU times.
    #[must_use]
    pub fn get_cpu_usages(&self) -> CpuUsages {
        self.inner.lock().unwrap().cpu_usages
    }

    /// Allocates a new ID; restarts from 0 when it exceeds the `u64` limit.
    pub fn alloc_new_sql_id(&self) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        inner.sql_id = inner.sql_id.wrapping_add(1);
        inner.sql_id
    }

    /// Resets the TiDB/TiKV CPU times to 0.
    pub fn reset_cpu_times(&self) {
        self.inner.lock().unwrap().cpu_usages.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::{CpuUsages, SqlCpuUsages};
    use std::time::Duration;

    #[test]
    fn merge_is_gated_by_sql_id() {
        let c = SqlCpuUsages::default();
        let id = c.alloc_new_sql_id();
        assert_eq!(id, 1);

        // A merge for the current ID lands; a stale ID's merge is dropped.
        c.merge_tidb_cpu_time(id, Duration::from_millis(5));
        c.merge_tidb_cpu_time(id + 1, Duration::from_millis(7));
        // TiKV time is never gated.
        c.merge_tikv_cpu_time(Duration::from_millis(11));

        let usages = c.get_cpu_usages();
        assert_eq!(usages.tidb_cpu_time, Duration::from_millis(5));
        assert_eq!(usages.tikv_cpu_time, Duration::from_millis(11));
    }

    #[test]
    fn set_reset_and_accumulate() {
        let c = SqlCpuUsages::default();
        c.set_cpu_usages(CpuUsages {
            tidb_cpu_time: Duration::from_secs(1),
            tikv_cpu_time: Duration::from_secs(2),
        });
        let id = c.alloc_new_sql_id();
        c.merge_tidb_cpu_time(id, Duration::from_secs(1));
        c.merge_tikv_cpu_time(Duration::from_secs(1));
        assert_eq!(
            c.get_cpu_usages(),
            CpuUsages {
                tidb_cpu_time: Duration::from_secs(2),
                tikv_cpu_time: Duration::from_secs(3),
            }
        );

        c.reset_cpu_times();
        assert_eq!(c.get_cpu_usages(), CpuUsages::default());
    }

    #[test]
    fn alloc_wraps_at_u64_max() {
        let c = SqlCpuUsages::default();
        c.inner.lock().unwrap().sql_id = u64::MAX;
        // Go: `c.sqlID++` on uint64 — "will restart from 0" past the limit.
        assert_eq!(c.alloc_new_sql_id(), 0);
        assert_eq!(c.alloc_new_sql_id(), 1);
    }
}
