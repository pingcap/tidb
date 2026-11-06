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

//! `pkg/executor/internal/exec/executor.go`'s RU v2 executor metrics: which
//! executors `exec.Next` records (`ruv2ExecutorMetricByType`), the per-call
//! IO accumulator a parent's children report their output into
//! (`nextIOAcc`), and the recording itself (`addRUV2ExecutorMetricCached`).
//!
//! Go carries the accumulator in the `Next` call's `context`, which worker
//! goroutines inherit; here it is the calling thread's, and a thread that
//! runs children on an executor's behalf adopts the spawner's
//! ([`current_next_io_acc`], [`with_next_io_acc`]).

use std::cell::RefCell;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use tidb_execdetails::ruv2_metrics::{ExecutorMetricRecorder, RUV2Metrics};

/// Go `ruv2ExecutorMetric`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ruv2ExecutorMetric {
    pub level: i64,
    pub label: &'static str,
    pub use_cells: bool,
}

/// Go `ruv2ExecutorMetricByType`, by Go executor type (`*pkg.Type`).
#[must_use]
pub fn ruv2_executor_metric_by_type(exec_type: &str) -> Option<Ruv2ExecutorMetric> {
    let (level, label, use_cells) = match exec_type {
        "*executor.BatchPointGetExec" => (1, "BatchPointGetExec", true),
        "*executor.PointGetExecutor" => (1, "PointGetExecutor", true),
        "*executor.LimitExec" => (1, "LimitExec", true),
        "*aggregate.HashAggExec" => (2, "HashAggExec", false),
        "*executor.ExpandExec" => (2, "ExpandExec", false),
        "*executor.IndexLookUpExecutor" => (2, "IndexLookUpExecutor", false),
        "*executor.IndexReaderExecutor" => (2, "IndexReaderExecutor", false),
        "*executor.MemTableReaderExec" => (2, "MemTableReaderExec", false),
        "*executor.ProjectionExec" => (2, "ProjectionExec", true),
        "*executor.SelectionExec" => (2, "SelectionExec", false),
        "*executor.SelectLockExec" => (2, "SelectLockExec", true),
        "*executor.TableDualExec" => (2, "TableDualExec", false),
        "*executor.TableReaderExecutor" => (2, "TableReaderExecutor", false),
        "*executor.UnionScanExec" => (2, "UnionScanExec", false),
        "*windows.WindowExec" | "*windows.PipelinedWindowExec" | "*windows.OrderedWindowExec" => {
            (2, "WindowExec", false)
        }
        "*join.HashJoinV1Exec" => (2, "HashJoinV1Exec", false),
        "*join.HashJoinV2Exec" => (2, "HashJoinV2Exec", false),
        "*join.IndexLookUpJoin" => (2, "IndexLookUpJoin", true),
        "*join.IndexLookUpMergeJoin" => (2, "IndexLookUpMergeJoin", true),
        "*join.IndexNestedLoopHashJoin" => (2, "IndexNestedLoopHashJoin", true),
        "*join.MergeJoinExec" => (2, "MergeJoinExec", false),
        "*sortexec.TopNExec" => (2, "TopNExec", true),
        "*aggregate.StreamAggExec" => (3, "StreamAggExec", false),
        "*sortexec.SortExec" => (3, "SortExec", true),
        _ => return None,
    };
    Some(Ruv2ExecutorMetric {
        level,
        label,
        use_cells,
    })
}

/// Go `nextIOAcc`: the rows and cells an executor's children returned during
/// its `Next`.
#[derive(Debug, Default)]
pub struct NextIoAcc {
    in_rows: AtomicI64,
    in_cells: AtomicI64,
}

impl NextIoAcc {
    /// Go `nextIOAcc.addInput`.
    pub fn add_input(&self, rows: usize, cols: usize) {
        self.in_rows.fetch_add(rows as i64, Ordering::Relaxed);
        self.in_cells.fetch_add(calc_cell_count(rows, cols), Ordering::Relaxed);
    }

    /// The rows the children returned.
    #[must_use]
    pub fn in_rows(&self) -> i64 {
        self.in_rows.load(Ordering::Relaxed)
    }

    /// The cells the children returned.
    #[must_use]
    pub fn in_cells(&self) -> i64 {
        self.in_cells.load(Ordering::Relaxed)
    }
}

/// Go `calcCellCount`.
#[must_use]
pub fn calc_cell_count(rows: usize, cols: usize) -> i64 {
    (rows as i64).saturating_mul(cols as i64)
}

thread_local! {
    /// Go `ctx.Value(nextIOAccKey)` for the running `Next`.
    static NEXT_IO_ACC: RefCell<Option<Arc<NextIoAcc>>> = const { RefCell::new(None) };
}

/// The accumulator of the `Next` running on this thread, which a child's
/// `Next` reports its output into.
#[must_use]
pub fn current_next_io_acc() -> Option<Arc<NextIoAcc>> {
    NEXT_IO_ACC.with(|acc| acc.borrow().clone())
}

/// Restores the thread's previous accumulator when dropped.
pub struct NextIoAccGuard {
    previous: Option<Arc<NextIoAcc>>,
}

impl Drop for NextIoAccGuard {
    fn drop(&mut self) {
        let previous = self.previous.take();
        NEXT_IO_ACC.with(|acc| *acc.borrow_mut() = previous);
    }
}

/// Go `context.WithValue(ctx, nextIOAccKey, acc)` for the code that runs on
/// this thread until the guard drops.
#[must_use]
pub fn with_next_io_acc(acc: Option<Arc<NextIoAcc>>) -> NextIoAccGuard {
    let previous = NEXT_IO_ACC.with(|current| current.replace(acc));
    NextIoAccGuard { previous }
}

/// Go `addRUV2ExecutorMetricCached`.
pub fn add_ruv2_executor_metric_cached(
    metrics: &RUV2Metrics,
    info: Ruv2ExecutorMetric,
    recorder: &ExecutorMetricRecorder,
    in_rows: i64,
    out_rows: i64,
    in_cells: i64,
    out_cells: i64,
) {
    let delta = if info.use_cells {
        in_cells + out_cells
    } else {
        in_rows + out_rows
    };
    if delta == 0 {
        return;
    }
    if recorder.available() {
        recorder.record(metrics, delta);
        return;
    }
    metrics.add_executor_metric(info.level, info.label, delta);
}

/// Go `exec.Next`'s RU v2 bookkeeping for one executor: the metrics it
/// records into (`ruv2NextCacheState`), `None` when its type is not recorded
/// or the statement bypasses RU v2.
#[derive(Clone)]
pub struct Ruv2NextCache {
    pub metrics: Arc<RUV2Metrics>,
    pub info: Ruv2ExecutorMetric,
    pub recorder: ExecutorMetricRecorder,
}

impl Ruv2NextCache {
    /// Go `populateRUV2NextCache`.
    #[must_use]
    pub fn new(exec_type: &str, metrics: Option<&Arc<RUV2Metrics>>) -> Option<Self> {
        let info = ruv2_executor_metric_by_type(exec_type)?;
        let metrics = metrics.filter(|metrics| !metrics.bypass())?;
        Some(Self {
            metrics: Arc::clone(metrics),
            info,
            recorder: tidb_execdetails::ruv2_metrics::resolve_executor_metric(info.level, info.label),
        })
    }
}

/// Go `exec.Next` around one call of an executor with `child_count`
/// children: the children report into a local accumulator, the output goes
/// to the parent's, and a recorded executor adds its rows or cells.
pub fn next_with_ruv2<E>(
    cache: Option<&Ruv2NextCache>,
    child_count: usize,
    req: &mut tidb_chunk::chunk::Chunk,
    next: impl FnOnce(&mut tidb_chunk::chunk::Chunk) -> Result<(), E>,
) -> Result<(), E> {
    let parent = current_next_io_acc();
    // Go `needNextIOAcc`.
    let local = ((cache.is_some() || parent.is_some()) && child_count > 0)
        .then(|| Arc::new(NextIoAcc::default()));
    let guard = with_next_io_acc(local.clone());
    let result = next(req);
    drop(guard);
    result?;
    let (out_rows, out_cols) = (req.num_rows(), req.num_cols());
    if let Some(parent) = parent {
        parent.add_input(out_rows, out_cols);
    }
    if let Some(cache) = cache {
        let (in_rows, in_cells) = local.map_or((0, 0), |acc| (acc.in_rows(), acc.in_cells()));
        add_ruv2_executor_metric_cached(
            &cache.metrics,
            cache.info,
            &cache.recorder,
            in_rows,
            out_rows as i64,
            in_cells,
            calc_cell_count(out_rows, out_cols),
        );
    }
    Ok(())
}

/// A closure run on another thread on behalf of the calling `Next`, as Go's
/// goroutines inherit their spawner's `context`: it adopts the caller's
/// profiler labels (`tidb_cpuprofile::labels::inherit`) and IO accumulator.
pub fn inherit_goroutine<R>(f: impl FnOnce() -> R) -> impl FnOnce() -> R {
    let acc = current_next_io_acc();
    let f = tidb_cpuprofile::labels::inherit(f);
    move || {
        let _acc = with_next_io_acc(acc);
        f()
    }
}
