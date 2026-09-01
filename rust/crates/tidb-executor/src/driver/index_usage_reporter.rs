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

//! Go `pkg/executor/internal/exec/indexusage.go`.

use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::schema::Schema;
use tidb_stats_handle_usage_indexusage::{new_sample, StmtIndexUsageCollector};

use crate::access_cost::TableStatistics;
use crate::executor::{ExecError, Executor};
use crate::kv_table::KvTable;

/// Go `IndexUsageReporter`: common reporting rules for coprocessor and point
/// readers. Request and row counters are supplied by their authoritative
/// execution owners.
pub(crate) struct IndexUsageReporter<'a> {
    collector: Option<&'a Arc<StmtIndexUsageCollector>>,
}

impl<'a> IndexUsageReporter<'a> {
    pub(crate) fn new(collector: Option<&'a Arc<StmtIndexUsageCollector>>) -> Self {
        Self { collector }
    }

    /// Go `ReportCopIndexUsageForHandle`.
    pub(crate) fn report_cop_for_handle(
        &self,
        table: &KvTable,
        stats: Option<&TableStatistics>,
        kv_requests: u64,
        accessed_rows: u64,
    ) {
        let Some(index_id) = cluster_index_id(table) else {
            return;
        };
        self.report_cop_for_table(table, stats, index_id, kv_requests, accessed_rows);
    }

    /// Go `ReportCopIndexUsageForTable`.
    pub(crate) fn report_cop_for_table(
        &self,
        table: &KvTable,
        stats: Option<&TableStatistics>,
        index_id: i64,
        kv_requests: u64,
        accessed_rows: u64,
    ) {
        self.report_cop(table.table_id, index_id, stats, kv_requests, accessed_rows);
    }

    /// Go `ReportCopIndexUsage`.
    pub(crate) fn report_cop(
        &self,
        logical_table_id: i64,
        index_id: i64,
        stats: Option<&TableStatistics>,
        kv_requests: u64,
        accessed_rows: u64,
    ) {
        let Some(collector) = self.collector else {
            return;
        };
        let Some(table_rows) = real_table_row_count(stats) else {
            return;
        };
        if kv_requests == 0 && accessed_rows == 0 {
            return;
        }
        collector.update(
            logical_table_id,
            index_id,
            new_sample(0, kv_requests, accessed_rows, table_rows),
        );
    }

    /// Go `ReportPointGetIndexUsageForHandle`.
    pub(crate) fn report_point_for_handle(
        &self,
        table: &KvTable,
        stats: Option<&TableStatistics>,
        kv_requests: u64,
        rows: u64,
    ) {
        let Some(index_id) = cluster_index_id(table) else {
            return;
        };
        self.report_point_for_table(table, stats, index_id, kv_requests, rows);
    }

    /// Go `ReportPointGetIndexUsage`.
    pub(crate) fn report_point_for_table(
        &self,
        table: &KvTable,
        stats: Option<&TableStatistics>,
        index_id: i64,
        kv_requests: u64,
        rows: u64,
    ) {
        let Some(collector) = self.collector else {
            return;
        };
        let table_rows = real_table_row_count(stats).unwrap_or(i32::MAX as u64);
        collector.update(
            table.table_id,
            index_id,
            new_sample(0, kv_requests, rows, table_rows),
        );
    }
}

fn real_table_row_count(stats: Option<&TableStatistics>) -> Option<u64> {
    let stats = stats?;
    if stats.pseudo {
        return None;
    }
    Some(stats.row_count as u64)
}

/// Go `getClusterIndexID`.
fn cluster_index_id(table: &KvTable) -> Option<i64> {
    if table.pk_handle_offset().is_some() {
        return Some(0);
    }
    if table.common_handle_offsets().is_empty() {
        return None;
    }
    Some(
        table
            .indexes()
            .iter()
            .find(|index| index.clustered_primary)
            .map_or(0, |index| index.id),
    )
}

#[derive(Clone, Copy)]
pub(super) enum PointCommand {
    Get,
    BatchGet,
}

/// Go point readers report from `Close` after reading SnapshotRuntimeStats.
pub(super) struct PointIndexUsageExec {
    child: Box<dyn Executor>,
    table: KvTable,
    stats: Option<Arc<TableStatistics>>,
    collector: Option<Arc<StmtIndexUsageCollector>>,
    index_id: Option<i64>,
    command: PointCommand,
    before: (u64, u64),
    rows: u64,
    reported: bool,
}

impl PointIndexUsageExec {
    pub(super) fn new(
        child: Box<dyn Executor>,
        table: KvTable,
        stats: Option<Arc<TableStatistics>>,
        collector: Option<Arc<StmtIndexUsageCollector>>,
        index_id: Option<i64>,
        command: PointCommand,
    ) -> Self {
        Self {
            child,
            table,
            stats,
            collector,
            index_id,
            command,
            before: (0, 0),
            rows: 0,
            reported: false,
        }
    }

    fn report(&mut self) {
        if self.reported {
            return;
        }
        self.reported = true;
        let after = self.table.point_rpc_counts();
        let kv_requests = match self.command {
            PointCommand::Get => after.0.wrapping_sub(self.before.0),
            PointCommand::BatchGet => after.1.wrapping_sub(self.before.1),
        };
        let reporter = IndexUsageReporter::new(self.collector.as_ref());
        match self.index_id {
            Some(index_id) => reporter.report_point_for_table(
                &self.table,
                self.stats.as_deref(),
                index_id,
                kv_requests,
                self.rows,
            ),
            None => reporter.report_point_for_handle(
                &self.table,
                self.stats.as_deref(),
                kv_requests,
                self.rows,
            ),
        }
    }
}

impl Executor for PointIndexUsageExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.before = self.table.point_rpc_counts();
        self.rows = 0;
        self.reported = false;
        self.child.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        self.child.next(req)?;
        self.rows = self.rows.wrapping_add(req.num_rows() as u64);
        Ok(())
    }

    fn close(&mut self) -> Result<(), ExecError> {
        let result = self.child.close();
        self.report();
        result
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.child.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.child.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.child.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.child.new_chunk()
    }
}

/// Go coprocessor readers report the scan plan's task/row totals at close.
pub(super) struct CopIndexUsageExec {
    child: Box<dyn Executor>,
    table: KvTable,
    stats: Option<Arc<TableStatistics>>,
    collector: Option<Arc<StmtIndexUsageCollector>>,
    index_id: Option<i64>,
    reported: bool,
}

impl CopIndexUsageExec {
    pub(super) fn new(
        child: Box<dyn Executor>,
        table: KvTable,
        stats: Option<Arc<TableStatistics>>,
        collector: Option<Arc<StmtIndexUsageCollector>>,
        index_id: Option<i64>,
    ) -> Self {
        Self {
            child,
            table,
            stats,
            collector,
            index_id,
            reported: false,
        }
    }

    fn report(&mut self) {
        if self.reported {
            return;
        }
        self.reported = true;
        let Some((kv_requests, accessed_rows)) = self
            .child
            .table_access()
            .and_then(|access| access.cop_count_and_rows())
        else {
            return;
        };
        let reporter = IndexUsageReporter::new(self.collector.as_ref());
        match self.index_id {
            Some(index_id) => reporter.report_cop_for_table(
                &self.table,
                self.stats.as_deref(),
                index_id,
                kv_requests,
                accessed_rows,
            ),
            None => reporter.report_cop_for_handle(
                &self.table,
                self.stats.as_deref(),
                kv_requests,
                accessed_rows,
            ),
        }
    }
}

impl Executor for CopIndexUsageExec {
    fn open(&mut self) -> Result<(), ExecError> {
        self.reported = false;
        self.child.open()
    }

    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        self.child.next(req)
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.report();
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.child.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.child.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.child.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.child.new_chunk()
    }

    fn table_access(&mut self) -> Option<&mut dyn crate::table_access::TableAccess> {
        self.child.table_access()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tidb_stats_handle_usage_indexusage::{Collector, StmtIndexUsageCollector};

    use super::*;

    fn collectors() -> (
        Collector,
        Arc<Mutex<tidb_stats_handle_usage_indexusage::SessionIndexUsageCollector>>,
        Arc<StmtIndexUsageCollector>,
    ) {
        let global = Collector::new();
        global.start_worker();
        let session = Arc::new(Mutex::new(global.spawn_session_collector()));
        let statement = Arc::new(StmtIndexUsageCollector::new(Arc::clone(&session)));
        (global, session, statement)
    }

    #[test]
    fn source_go_indexusage_test_reporter_rules() {
        let (global, session, statement) = collectors();
        let reporter = IndexUsageReporter::new(Some(&statement));
        let real = TableStatistics {
            row_count: 100,
            ..TableStatistics::default()
        };
        let pseudo = TableStatistics {
            pseudo: true,
            row_count: 100,
            ..TableStatistics::default()
        };

        reporter.report_point_for_table(&KvTable::new(1, vec![]), Some(&real), 2, 1, 2024);
        reporter.report_cop(1, 2, Some(&real), 1, 2024);
        reporter.report_cop(1, 2, Some(&pseudo), 1, 2024);
        reporter.report_cop(1, 2, Some(&real), 0, 0);
        session.lock().expect("session collector").flush();
        global.close();

        let usage = global.get_index_usage(1, 2);
        assert_eq!(usage.query_total, 1);
        assert_eq!(usage.kv_req_total, 2);
        assert_eq!(usage.row_access_total, 4048);
    }

    #[test]
    fn point_get_without_real_stats_uses_smallest_nonzero_bucket() {
        let (global, session, statement) = collectors();
        let reporter = IndexUsageReporter::new(Some(&statement));
        reporter.report_point_for_table(&KvTable::new(1, vec![]), None, 2, 1, 1);
        session.lock().expect("session collector").flush();
        global.close();

        let usage = global.get_index_usage(1, 2);
        assert_eq!(usage.percentage_access, [0, 1, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn common_handle_without_primary_uses_zero_index_id() {
        let (global, session, statement) = collectors();
        let reporter = IndexUsageReporter::new(Some(&statement));
        let mut table = KvTable::new(1, vec![]);
        table.set_common_handle_offsets(vec![0]);

        reporter.report_point_for_handle(
            &table,
            Some(&TableStatistics {
                row_count: 100,
                ..TableStatistics::default()
            }),
            1,
            1,
        );
        session.lock().expect("session collector").flush();
        global.close();

        assert_eq!(global.get_index_usage(1, 0).query_total, 1);
    }
}
