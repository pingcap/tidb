// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The executor stats Go hands `RuntimeStatsColl.RegisterStats` from an
//! executor's `Close`, as groups of the plan's `RootRuntimeStats`.

use tidb_execdetails::RuntimeStats;
use tidb_executor_internal_exec::Executor;

/// Go `executor.runtimeStatsWithSnapshot`: a point reader's snapshot RPCs.
pub(crate) struct RuntimeStatsWithSnapshot {
    snapshot: Option<tikv_client::SnapshotRuntimeStats>,
}

impl RuntimeStatsWithSnapshot {
    pub(crate) fn new(snapshot: &tikv_client::SnapshotRuntimeStats) -> Self {
        Self {
            snapshot: Some(snapshot.clone()),
        }
    }
}

impl RuntimeStats for RuntimeStatsWithSnapshot {
    fn string(&self) -> String {
        self.snapshot.as_ref().map(ToString::to_string).unwrap_or_default()
    }

    fn merge(&mut self, other: &dyn RuntimeStats) {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return;
        };
        let Some(theirs) = &other.snapshot else {
            return;
        };
        match &self.snapshot {
            Some(mine) => mine.merge(theirs),
            None => self.snapshot = Some(theirs.clone()),
        }
    }

    fn clone_box(&self) -> Box<dyn RuntimeStats> {
        Box::new(Self {
            snapshot: self.snapshot.clone(),
        })
    }

    fn tp(&self) -> i32 {
        tidb_execdetails::TP_RUNTIME_STATS_WITH_SNAPSHOT
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Go `join.joinRuntimeStats` as an apply's `Close` registers it: its
/// concurrency and whether its inner-result cache was used.
pub(crate) struct ApplyJoinRuntimeStats {
    pub(crate) snapshot: crate::apply::native::ApplyRuntimeSnapshot,
}

impl RuntimeStats for ApplyJoinRuntimeStats {
    /// Go `joinRuntimeStats.String`: `RuntimeStatsWithConcurrencyInfo`'s
    /// text (`ParallelNestedLoopApplyExec`'s `Concurrency:N`,
    /// `NestedLoopApplyExec`'s `concurrency:0`, which prints OFF), then the
    /// cache.
    fn string(&self) -> String {
        let snapshot = &self.snapshot;
        let concurrency = tidb_execdetails::RuntimeStatsWithConcurrencyInfo::default();
        let name = if snapshot.concurrency > 1 { "Concurrency" } else { "concurrency" };
        let num = if snapshot.concurrency > 1 { snapshot.concurrency } else { 0 };
        concurrency.set_concurrency_info(vec![tidb_execdetails::ConcurrencyInfo::new(
            name,
            i64::try_from(num).unwrap_or(i64::MAX),
        )]);
        let mut text = concurrency.string();
        if snapshot.enabled {
            let ratio = if snapshot.accesses == 0 {
                0.0
            } else {
                snapshot.hits as f64 / snapshot.accesses as f64
            };
            text.push_str(&format!(", cache:ON, cacheHitRatio:{:.3}%", ratio * 100.0));
        } else {
            text.push_str(", cache:OFF");
        }
        text
    }

    /// Go's embedded `RuntimeStatsWithConcurrencyInfo.Merge`, a no-op.
    fn merge(&mut self, _other: &dyn RuntimeStats) {}

    fn clone_box(&self) -> Box<dyn RuntimeStats> {
        Box::new(Self {
            snapshot: self.snapshot,
        })
    }

    fn tp(&self) -> i32 {
        tidb_execdetails::TP_JOIN_RUNTIME_STATS
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// An executor whose `Close` registers a stats group on its plan in the
/// statement's `RuntimeStatsColl`, as Go's `PointGetExecutor.Close` /
/// `BatchPointGetExec.Close` do with their snapshot stats.
pub(crate) struct RegisterStatsOnClose {
    child: Box<dyn Executor>,
    register: Option<Box<dyn FnOnce() + Send>>,
}

impl RegisterStatsOnClose {
    /// `child`, registering `stats()` under `plan_id` when it closes, or
    /// `child` itself when the statement collects no runtime stats.
    pub(crate) fn wrap(
        child: Box<dyn Executor>,
        ctx: &crate::StmtContext,
        plan_id: i64,
        stats: impl FnOnce() -> Box<dyn RuntimeStats> + Send + 'static,
    ) -> Box<dyn Executor> {
        let Some(source) = ctx.executor_runtime_stats_source().cloned() else {
            return child;
        };
        Box::new(Self {
            child,
            register: Some(Box::new(move || source.register_stats(plan_id, stats()))),
        })
    }
}

impl Executor for RegisterStatsOnClose {
    fn open(&mut self) -> Result<(), crate::ExecError> {
        self.child.open()
    }

    fn next(&mut self, req: &mut tidb_chunk::chunk::Chunk) -> Result<(), crate::ExecError> {
        self.child.next(req)
    }

    /// Go's deferred `RegisterStats`: after the executor's own `Close`.
    fn close(&mut self) -> Result<(), crate::ExecError> {
        let result = self.child.close();
        if let Some(register) = self.register.take() {
            register();
        }
        result
    }

    fn schema(&self) -> &tidb_expr::schema::Schema {
        self.child.schema()
    }

    fn ret_field_types(&self) -> &[tidb_datatype::FieldType] {
        self.child.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.child.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.child.max_chunk_size()
    }

    fn new_chunk(&self) -> tidb_chunk::chunk::Chunk {
        self.child.new_chunk()
    }

    fn new_first_chunk(&self) -> tidb_chunk::chunk::Chunk {
        self.child.new_first_chunk()
    }

    fn table_access(&mut self) -> Option<&mut dyn tidb_executor_internal_exec::table_access::TableAccess> {
        self.child.table_access()
    }

    fn agg_tree_input_empty(&self) -> bool {
        self.child.agg_tree_input_empty()
    }
}
