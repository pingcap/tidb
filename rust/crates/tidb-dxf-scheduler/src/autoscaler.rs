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

//! Go `autoscaler.go`: the resources a task needs, by its data size. Not
//! ported yet: `CalcMaxNodeCountByStoresNum` (a PD store listing; DDL ingest
//! and `IMPORT INTO` call it).

use tidb_dxf::schstatus::TuneFactors;

/// Go `baseCores`: node counts take an 8c machine as the baseline.
const BASE_CORES: f64 = 8.0;
/// Go `baseDataSize`: each node handles at least 2 subtasks of 100GiB each;
/// every additional 200 GiB adds a node.
const BASE_DATA_SIZE: f64 = (200_i64 << 30) as f64;
/// Go `baseSizePerConc`: to make small tasks fast, importing 200 GiB fully
/// uses an 8c node, so every additional 25 GiB adds a slot.
const BASE_SIZE_PER_CONC: f64 = (25_i64 << 30) as f64;
/// Go `maxNodeCountLimitForAddIndex`.
const MAX_NODE_COUNT_LIMIT_FOR_ADD_INDEX: f64 = 30.0;
/// Go `maxNodeCountLimitForImportInto`: importing 100TiB within 24 hours
/// needs about 32 8c nodes in previous performance tests.
const MAX_NODE_COUNT_LIMIT_FOR_IMPORT_INTO: i64 = 32;
/// Go `maxDistSQLConcurrencyPerCore` (256/8: 256 on an 8c machine is fast
/// enough for checksum).
const MAX_DIST_SQL_CONCURRENCY_PER_CORE: i64 = 32;

/// Go `ResourceCalc`: the resources a DXF task needs, in nextgen.
#[derive(Debug, Clone)]
pub struct ResourceCalc {
    /// The input data size in bytes: the source data of import-into, the
    /// table of add-index.
    data_size: i64,
    /// The CPU cores of each execution node.
    node_cpu: isize,
    /// The ratio of index KV size to data KV size, sampled and estimated for
    /// import-into; always 0 for add-index.
    index_size_ratio: f64,
    /// The tuning factors.
    factors: TuneFactors,
}

impl ResourceCalc {
    /// Go `NewRCCalcForAddIndex`.
    #[must_use]
    pub fn new_for_add_index(data_size: i64, node_cpu: isize, factors: &TuneFactors) -> Self {
        Self::new(data_size, node_cpu, 0.0, factors)
    }

    /// Go `NewRCCalc`.
    #[must_use]
    pub fn new(data_size: i64, node_cpu: isize, index_size_ratio: f64, factors: &TuneFactors) -> Self {
        Self { data_size, node_cpu, index_size_ratio, factors: factors.clone() }
    }

    /// Go `CalcMaxNodeCountForAddIndex`.
    #[must_use]
    pub fn calc_max_node_count_for_add_index(&self) -> isize {
        let size = self.get_amplified_data_size();
        let limit = self.factors.amplify_factor * MAX_NODE_COUNT_LIMIT_FOR_ADD_INDEX;
        self.calc_max_node_count_by_size(size, limit)
    }

    /// Go `CalcMaxNodeCountForImportInto`.
    #[must_use]
    pub fn calc_max_node_count_for_import_into(&self) -> isize {
        let size = self.get_amplified_data_size();
        let limit = self.factors.amplify_factor * MAX_NODE_COUNT_LIMIT_FOR_IMPORT_INTO as f64;
        self.calc_max_node_count_by_size(size, limit)
    }

    fn get_amplified_data_size(&self) -> i64 {
        (self.factors.amplify_factor * (1.0 + self.index_size_ratio) * self.data_size as f64) as i64
    }

    fn calc_max_node_count_by_size(&self, size: i64, limit: f64) -> isize {
        if self.node_cpu <= 0 {
            return 0;
        }
        let r = BASE_CORES / self.node_cpu as f64;
        let mut node_cnt = size as f64 * r / BASE_DATA_SIZE;
        node_cnt = node_cnt.min(limit * r);
        node_cnt = node_cnt.max(1.0);
        node_cnt.round() as isize
    }

    /// Go `CalcRequiredSlots`: the required slots by the data size.
    #[must_use]
    pub fn calc_required_slots(&self) -> isize {
        let size = self.get_amplified_data_size();
        if size <= 0 {
            return 4;
        }
        let mut slots = size as f64 / BASE_SIZE_PER_CONC;
        slots = slots.min(self.node_cpu as f64);
        slots = slots.max(1.0);
        slots.round() as isize
    }
}

/// Go `GetExecCPUNode`: the CPU cores of the nodes of the target scope.
///
/// # Errors
/// No task manager is set, or the query failed.
pub fn get_exec_cpu_node() -> tidb_dxf_storage::Result<isize> {
    let mgr = match tidb_dxf_storage::get_dxf_svc_task_mgr() {
        Ok(mgr) => mgr,
        Err(error) => {
            if tidb_intest::IN_TEST {
                tidb_logutil::bg_logger()
                    .warn("failed to get DXFSvcTaskMgr in test mode, returning default CPU count", &[]);
                return Ok(tidb_cpu::get_cpu_count() as isize);
            }
            return Err(error);
        }
    };
    mgr.get_cpu_count_of_node_by_role(&tidb_dxf_handle::get_target_scope())
}

/// Go `CalcDistSQLConcurrency`: the DistSQL concurrency by the thread count,
/// the max node count and each node's CPU cores. Up to one node, it is the
/// task concurrency times `DefDistSQLScanConcurrency`; beyond, it grows
/// linearly up to `maxDistSQLConcurrencyPerCore` times the node CPU.
#[must_use]
pub fn calc_dist_sql_concurrency(thread_cnt: isize, max_node_cnt: isize, node_cpu: isize) -> isize {
    let def = tidb_vardef::defaults::DEF_DIST_SQL_SCAN_CONCURRENCY as isize;
    if max_node_cnt <= 1 {
        return thread_cnt * def;
    }

    let start = def * node_cpu;
    let interval = node_cpu * (MAX_DIST_SQL_CONCURRENCY_PER_CORE as isize - def);
    let total_step_count = MAX_NODE_COUNT_LIMIT_FOR_IMPORT_INTO as isize - 1;
    let step_count = total_step_count.min(max_node_cnt - 1);
    (start as f64 + interval as f64 * step_count as f64 / total_step_count as f64) as isize
}

#[cfg(test)]
mod tests {
    use tidb_dxf::schstatus::{get_default_tune_factors, TuneFactors};

    use super::{calc_dist_sql_concurrency, ResourceCalc};

    const GIB: i64 = 1 << 30;
    const MIB: i64 = 1 << 20;
    const TIB: i64 = 1 << 40;

    /// Go `TestCalcMaxNodeCountByTableSize`.
    #[test]
    fn test_calc_max_node_count_by_table_size() {
        let tests = [
            (0, 8, 1),
            (10, 0, 0),
            (320 * GIB + 100, 4, 3),
            (100 * TIB, 4, 60),
            (10 * GIB, 8, 1),
            (200 * GIB, 8, 1),
            (800 * GIB, 8, 4),
            (1100 * GIB, 8, 6),
            (200 * TIB, 8, 30),
            (200 * GIB, 16, 1),
            (600 * GIB, 16, 2),
            (1200 * GIB, 16, 3),
            (4 * TIB, 16, 10),
            (6 * TIB, 16, 15),
            (10 * TIB, 16, 15),
        ];
        for (i, (table_size, cores, expected)) in tests.into_iter().enumerate() {
            let cal = ResourceCalc::new(table_size, cores, 0.0, &get_default_tune_factors());
            assert_eq!(cal.calc_max_node_count_for_add_index(), expected, "case-{i} tableSize:{table_size} cores:{cores}");
        }
    }

    /// Go `TestCalcMaxNodeCountByDataSize`.
    #[test]
    fn test_calc_max_node_count_by_data_size() {
        let tests = [
            (0, 8, 1),
            (10, 0, 0),
            (320 * GIB + 100, 4, 3),
            (100 * TIB, 4, 64),
            (10 * GIB, 8, 1),
            (200 * GIB, 8, 1),
            (800 * GIB, 8, 4),
            (1100 * GIB - 100, 8, 5),
            (1100 * GIB, 8, 6),
            (200 * TIB, 8, 32),
            (200 * GIB, 16, 1),
            (600 * GIB, 16, 2),
            (1200 * GIB, 16, 3),
            (4 * TIB, 16, 10),
            (6 * TIB, 16, 15),
            (10 * TIB, 16, 16),
            (100 * TIB, 16, 16),
        ];
        for (i, (data_size, cores, expected)) in tests.into_iter().enumerate() {
            let calc = ResourceCalc::new(data_size, cores, 0.0, &get_default_tune_factors());
            assert_eq!(calc.calc_max_node_count_for_import_into(), expected, "case-{i} dataSize:{data_size} cores:{cores}");
        }
    }

    /// Go `TestCalcRequiredSlotsByDataSize`.
    #[test]
    fn test_calc_required_slots_by_data_size() {
        let tests = [
            (0, 5, 4),
            (-100, 3, 4),
            (24 * GIB, 5, 1),
            (25 * GIB, 5, 1),
            (25 * GIB, 1, 1),
            (50 * GIB, 4, 2),
            (100 * GIB, 3, 3),
            (37 * GIB + 512 * MIB, 8, 2),
            (50 * GIB, 8, 2),
            (100 * GIB, 8, 4),
            (50 * GIB, 10, 2),
            (75 * GIB, 4, 3),
            (25 * 1000 * GIB, 16, 16),
            (1, 5, 1),
        ];
        for (data_size, cores, expected) in tests {
            let calc = ResourceCalc::new(data_size, cores, 0.0, &get_default_tune_factors());
            assert_eq!(calc.calc_required_slots(), expected, "dataSize:{data_size} cores:{cores}");
        }
    }

    /// Go `TestCalcDistSQLConcurrency`.
    #[test]
    fn test_calc_dist_sql_concurrency() {
        // (concurrency, nodeCount, nodeCoreCount, expected)
        let tests = [
            // on 8c machine
            (1, 1, 8, 15),
            (3, 1, 8, 45),
            (7, 1, 8, 105),
            (8, 1, 8, 120),
            (8, 2, 8, 124),
            (8, 5, 8, 137),
            (8, 32, 8, 256),
            (8, 33, 8, 256),
            (8, 50, 8, 256),
            // on 16c machine
            (1, 1, 16, 15),
            (7, 1, 16, 105),
            (16, 1, 16, 240),
            (16, 5, 16, 275),
            (16, 32, 16, 512),
            (16, 33, 16, 512),
            (16, 50, 16, 512),
            // on 32c machine
            (1, 1, 32, 15),
            (7, 1, 32, 105),
            (32, 1, 32, 480),
            (32, 5, 32, 550),
            (32, 32, 32, 1024),
            (32, 33, 32, 1024),
        ];
        for (i, (c, n, nc, expected)) in tests.into_iter().enumerate() {
            assert_eq!(calc_dist_sql_concurrency(c, n, nc), expected, "case-{i}");
        }
    }

    /// Go `TestIndexSizeRatio`.
    #[test]
    fn test_index_size_ratio() {
        let tests = [(100 * GIB, 8, 0.0, 4, 1), (100 * GIB, 8, 1.0, 8, 1), (100 * GIB, 8, 1.5, 8, 1), (100 * GIB, 8, 2.0, 8, 2)];
        for (i, (data_size, cores, index_size_ratio, expected_c, expected_n)) in tests.into_iter().enumerate() {
            let calc = ResourceCalc::new(data_size, cores, index_size_ratio, &get_default_tune_factors());
            assert_eq!(calc.calc_required_slots(), expected_c, "case-{i}");
            assert_eq!(calc.calc_max_node_count_for_import_into(), expected_n, "case-{i}");
        }
    }

    /// Go `TestTuneFactors`.
    #[test]
    fn test_tune_factors() {
        let tests = [
            (100 * GIB, 8, 1.0, 4, 1, 1),
            (1000 * GIB, 8, 1.0, 8, 5, 5),
            (1000 * GIB, 8, 1.5, 8, 8, 8),
            (1000 * GIB, 8, 2.0, 8, 10, 10),
            (1000 * GIB, 8, 10.0, 8, 50, 50),
            (100 * TIB, 8, 2.0, 8, 64, 60),
            (100 * TIB, 8, 5.0, 8, 160, 150),
        ];
        for (i, (data_size, cores, amplify_factor, expected_c, import_n, add_index_n)) in tests.into_iter().enumerate() {
            let calc = ResourceCalc::new(data_size, cores, 0.0, &TuneFactors { amplify_factor });
            assert_eq!(calc.calc_required_slots(), expected_c, "case-{i}");
            assert_eq!(calc.calc_max_node_count_for_import_into(), import_n, "case-{i}");
            assert_eq!(calc.calc_max_node_count_for_add_index(), add_index_n, "case-{i}");
        }
    }
}
