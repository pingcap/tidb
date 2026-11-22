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

//! Go `pkg/planner/core/plan_cost_ver1.go`'s session inputs: the version-1
//! cost factors `SessionVars` keeps (`GetCPUFactor`, `GetScanFactor`, ...).
//! The per-operator bodies walk the physical plan in
//! [`crate::find_best_task::coster`], beside version 2's.

/// Go `SessionVars`' version-1 cost factors (`tidb_opt_*_factor`).
#[derive(Clone, Debug, PartialEq)]
pub struct Ver1Factors {
    /// `tidb_opt_cpu_factor`.
    pub cpu: f64,
    /// `tidb_opt_copcpu_factor`.
    pub cop_cpu: f64,
    /// `tidb_opt_network_factor`.
    pub network: f64,
    /// `tidb_opt_scan_factor`.
    pub scan: f64,
    /// `tidb_opt_desc_factor`.
    pub desc_scan: f64,
    /// `tidb_opt_seek_factor`.
    pub seek: f64,
    /// `tidb_opt_memory_factor`.
    pub memory: f64,
    /// `tidb_opt_disk_factor`.
    pub disk: f64,
    /// `tidb_opt_concurrency_factor`.
    pub concurrency: f64,
    /// `tidb_opt_tiflash_concurrency_factor` (Go `CopTiFlashConcurrencyFactor`).
    pub cop_tiflash_concurrency: f64,
}

impl Default for Ver1Factors {
    fn default() -> Self {
        use tidb_vardef::defaults as d;
        Self {
            cpu: d::DEF_OPT_CPU_FACTOR,
            cop_cpu: d::DEF_OPT_COP_CPU_FACTOR,
            network: d::DEF_OPT_NETWORK_FACTOR,
            scan: d::DEF_OPT_SCAN_FACTOR,
            desc_scan: d::DEF_OPT_DESC_SCAN_FACTOR,
            seek: d::DEF_OPT_SEEK_FACTOR,
            memory: d::DEF_OPT_MEMORY_FACTOR,
            disk: d::DEF_OPT_DISK_FACTOR,
            concurrency: d::DEF_OPT_CONCURRENCY_FACTOR,
            cop_tiflash_concurrency: d::DEF_OPT_TIFLASH_CONCURRENCY_FACTOR,
        }
    }
}

impl Ver1Factors {
    /// Go `GetNetworkFactor(tbl)`: a temporary table's reads cost nothing.
    #[must_use]
    pub fn network(&self, is_temporary: bool) -> f64 {
        if is_temporary { 0.0 } else { self.network }
    }

    /// Go `GetScanFactor(tbl)`.
    #[must_use]
    pub fn scan(&self, is_temporary: bool) -> f64 {
        if is_temporary { 0.0 } else { self.scan }
    }

    /// Go `GetDescScanFactor(tbl)`.
    #[must_use]
    pub fn desc_scan(&self, is_temporary: bool) -> f64 {
        if is_temporary { 0.0 } else { self.desc_scan }
    }

    /// Go `GetSeekFactor(tbl)`.
    #[must_use]
    pub fn seek(&self, is_temporary: bool) -> f64 {
        if is_temporary { 0.0 } else { self.seek }
    }
}
