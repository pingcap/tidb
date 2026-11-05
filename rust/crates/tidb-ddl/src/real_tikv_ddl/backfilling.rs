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

//! Go `pkg/ddl/backfilling.go`'s transactional backfill
//! (`writePhysicalTableRecord`, `loadTableRanges`, `getBatchTasks`,
//! `doneTaskKeeper`) over `backfilling_txn_executor.go`'s
//! `txnBackfillExecutor`: a physical table's range is cut at its regions,
//! each region range is a task, `tidb_ddl_reorg_worker_cnt` workers run the
//! tasks' batches, and the reorg handle advances over the tasks done in
//! order.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::bounded;
use tidb_kv::region_map::RegionStore;

use super::{ClusterDdlError, LockSqlError};

/// Go `backfillTaskChanSize`.
pub(super) const BACKFILL_TASK_CHAN_SIZE: usize = 128;
/// Go `maxBackfillWorkerSize`.
const MAX_BACKFILL_WORKER_SIZE: usize = 16;
/// Go `util.DefaultMaxRetries`.
const DEFAULT_MAX_RETRIES: usize = 30;
/// Go `util.RetryInterval`.
const RETRY_INTERVAL: Duration = Duration::from_millis(500);
/// Go `UpdateDDLJobReorgCfgInterval`.
const UPDATE_DDL_JOB_REORG_CFG_INTERVAL: Duration = Duration::from_secs(2);

/// One batch's outcome, as a backfill worker's `BackfillData` reports it.
pub(super) enum BatchStep {
    /// The batch committed; `done` when its task range is exhausted.
    Advanced {
        next_key: Vec<u8>,
        done: bool,
        added: u64,
        batch_warnings: Vec<(u16, String, i64)>,
    },
    /// The job is no longer runnable (Go `isReorgRunnable`).
    Interrupted,
    /// The reorganization's own SQL failure.
    Failed(LockSqlError),
}

/// How a physical table's backfill ended.
pub(super) enum TableRecordOutcome {
    /// Every task is done; the handle is past `end`.
    Done,
    /// The step's time ran out; the handle names where to resume.
    Progress,
    /// The job was cancelled or paused.
    Interrupted,
    /// A batch failed.
    Failed(LockSqlError),
}

/// What [`write_physical_table_record`] runs its tasks with.
pub(super) struct BackfillExecutor<'a> {
    /// Go `jobCtx.store`'s region cache; `None` is Go's non-TiKV storage.
    pub region_store: Option<&'a dyn RegionStore>,
    /// The physical table.
    pub physical_id: i64,
    /// Go `ReorgMeta.GetConcurrency`.
    pub concurrency: usize,
    /// Go `ReorgMeta.GetBatchSize`.
    pub batch_size: usize,
    /// Go `reorgInfo.UpdateConfigFromSysTbl`: the running job's concurrency
    /// and batch size now, `None` when the job cannot be altered or read.
    pub reload_config: &'a (dyn Fn() -> Option<(usize, usize)> + Sync),
    /// Go `getActualEndKey`: row workers shrink a task to the rows it has
    /// (`GetRangeEndKey`); the temp-index merge keeps the region end.
    pub shrink_end_key: bool,
    /// Go `BackfillData` for one batch of `[start, end)` of at most the
    /// given rows.
    pub backfill_data: &'a (dyn Fn(&[u8], &[u8], usize) -> Result<BatchStep, ClusterDdlError> + Sync),
    /// Go `GetRangeEndKey`.
    pub range_end_key: &'a (dyn Fn(&[u8], &[u8]) -> Result<Vec<u8>, ClusterDdlError> + Sync),
    /// Go `reorgInfo.UpdateReorgMeta(nextKey, sessPool)`; a failure is only
    /// logged.
    pub update_reorg_meta: &'a (dyn Fn(&[u8]) + Sync),
    /// When this step stops and hands the job back (`ErrWaitReorgTimeout`).
    pub deadline: Instant,
}

/// Go `reorgBackfillTask`.
struct ReorgBackfillTask {
    id: usize,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
}

/// Go `backfillResult`.
struct BackfillResult {
    task_id: usize,
    added_count: u64,
    next_key: Vec<u8>,
    warnings: Vec<(u16, String, i64)>,
    /// Whether the task's range is exhausted; a task the deadline stopped
    /// part way is not.
    done: bool,
    err: Option<TableRecordOutcome>,
}

/// Go `doneTaskKeeper`: the next key past every task done in id order.
struct DoneTaskKeeper {
    done_task_next_key: HashMap<usize, Vec<u8>>,
    current: usize,
    next_key: Vec<u8>,
}

impl DoneTaskKeeper {
    /// Go `newDoneTaskKeeper`.
    fn new(start: Vec<u8>) -> Self {
        Self {
            done_task_next_key: HashMap::new(),
            current: 0,
            next_key: start,
        }
    }

    /// Go `doneTaskKeeper.updateNextKey`.
    fn update_next_key(&mut self, done_task_id: usize, next: Vec<u8>) {
        if done_task_id == self.current {
            self.current += 1;
            self.next_key = next;
            while let Some(next) = self.done_task_next_key.remove(&self.current) {
                self.current += 1;
                self.next_key = next;
            }
            return;
        }
        self.done_task_next_key.insert(done_task_id, next);
    }
}

fn invalid_split_region_ranges(message: String) -> ClusterDdlError {
    ClusterDdlError::Backfill(
        tidb_dbterror::ERR_INVALID_SPLIT_REGION_RANGES
            .gen_with_stack_by_args(&[message.into()])
            .to_string(),
    )
}

/// Go `validateAndFillRanges`.
fn validate_and_fill_ranges(
    ranges: &mut [(Vec<u8>, Vec<u8>)],
    start_key: &[u8],
    end_key: &[u8],
) -> Result<(), ClusterDdlError> {
    if ranges.is_empty() {
        return Err(invalid_split_region_ranges(format!(
            "cannot find region in range [{}, {}]",
            hex::encode(start_key),
            hex::encode(end_key)
        )));
    }
    let last = ranges.len() - 1;
    for i in 0..ranges.len() {
        if i == 0 {
            let s = &ranges[i].0;
            if s.is_empty() || s.as_slice() < start_key {
                ranges[i].0 = start_key.to_vec();
            } else if s.as_slice() > start_key {
                return Err(invalid_split_region_ranges(format!(
                    "get empty range at the beginning of ranges, expected {}, but got {}",
                    hex::encode(start_key),
                    hex::encode(s)
                )));
            }
        }
        if i == last {
            let e = &ranges[i].1;
            // We don't need to check the end key because a limit may set
            // before scanning regions.
            if e.is_empty() || e.as_slice() > end_key {
                ranges[i].1 = end_key.to_vec();
            }
        }
        if ranges[i].0.is_empty() || ranges[i].1.is_empty() {
            return Err(invalid_split_region_ranges(
                "get empty start/end key in the middle of ranges".to_owned(),
            ));
        }
        if i > 0 && ranges[i - 1].1 != ranges[i].0 {
            return Err(invalid_split_region_ranges(format!(
                "ranges are not continuous, last end key {}, next start key {}",
                hex::encode(&ranges[i - 1].1),
                hex::encode(&ranges[i].0)
            )));
        }
    }
    Ok(())
}

fn info(message: &str, fields: Vec<tidb_log::Field>) {
    tidb_ddl_logutil::ddl_logger().info(message, &fields);
}

fn hex_field(key: &str, value: &[u8]) -> tidb_log::Field {
    tidb_log::Field::new(key, tidb_log::Value::Str(hex::encode(value)))
}

/// Go `loadTableRanges` load table key ranges from PD between given start
/// key and end key. It returns up to `limit` ranges.
pub(super) fn load_table_ranges(
    region_store: Option<&dyn RegionStore>,
    physical_id: i64,
    start_key: &[u8],
    end_key: &[u8],
    limit: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ClusterDdlError> {
    let pid = tidb_log::Field::new("physicalTableID", tidb_log::Value::I64(physical_id));
    if start_key.is_empty() && end_key.is_empty() {
        info("load empty range", vec![pid]);
        return Ok(Vec::new());
    }
    let Some(region_store) = region_store else {
        // Only support split ranges in tikv.Storage now.
        info("load table ranges failed, unsupported storage", vec![pid]);
        return Ok(vec![(start_key.to_vec(), end_key.to_vec())]);
    };
    let mut attempt = 0;
    let ranges = loop {
        info(
            "load table ranges from PD",
            vec![pid.clone(), hex_field("start key", start_key), hex_field("end key", end_key)],
        );
        let mut ranges = region_store
            .batch_load_regions_with_key_range(start_key, end_key, limit)
            .map_err(ClusterDdlError::Backfill)?;
        // Go `RunWithRetry`: an invalid split is retried, a PD error is not.
        match validate_and_fill_ranges(&mut ranges, start_key, end_key) {
            Ok(()) => break ranges,
            Err(error) => {
                attempt += 1;
                if attempt >= DEFAULT_MAX_RETRIES {
                    return Err(error);
                }
                std::thread::sleep(RETRY_INTERVAL);
            }
        }
    };
    info(
        "load table ranges from PD done",
        vec![
            pid,
            hex_field("range start", &ranges[0].0),
            hex_field("range end", &ranges[ranges.len() - 1].1),
            tidb_log::Field::new("range count", tidb_log::Value::I64(ranges.len() as i64)),
        ],
    );
    Ok(ranges)
}

/// Go `backfillWorker.handleBackfillTask`: the task's batches, each in its
/// own transaction, until its range is exhausted, the job stops being
/// runnable, or the step's deadline passes.
fn handle_backfill_task(
    exec: &BackfillExecutor<'_>,
    task: ReorgBackfillTask,
    batch_cnt: usize,
    stop: &AtomicBool,
) -> BackfillResult {
    let mut result = BackfillResult {
        task_id: task.id,
        added_count: 0,
        next_key: task.start_key.clone(),
        warnings: Vec::new(),
        done: false,
        err: None,
    };
    let mut start = task.start_key;
    loop {
        if Instant::now() >= exec.deadline {
            stop.store(true, Ordering::SeqCst);
        }
        if stop.load(Ordering::SeqCst) {
            return result;
        }
        match (exec.backfill_data)(&start, &task.end_key, batch_cnt) {
            Ok(BatchStep::Advanced {
                next_key,
                done,
                added,
                batch_warnings,
            }) => {
                result.added_count += added;
                merge_warnings(&mut result.warnings, batch_warnings);
                result.next_key = next_key.clone();
                start = next_key;
                if done {
                    result.done = true;
                    return result;
                }
            }
            Ok(BatchStep::Interrupted) => {
                result.err = Some(TableRecordOutcome::Interrupted);
                return result;
            }
            Ok(BatchStep::Failed(error)) => {
                result.err = Some(TableRecordOutcome::Failed(error));
                return result;
            }
            Err(error) => {
                result.err = Some(TableRecordOutcome::Failed(LockSqlError::from(
                    tidb_terror::MysqlError::unknown(error.to_string()),
                )));
                return result;
            }
        }
    }
}

/// Go `mergeWarnings`: each code's first warning, counted.
pub(super) fn merge_warnings(total: &mut Vec<(u16, String, i64)>, part: Vec<(u16, String, i64)>) {
    for (code, message, count) in part {
        match total.iter_mut().find(|(seen, _, _)| *seen == code) {
            Some((_, _, seen_count)) => *seen_count += count,
            None => total.push((code, message, count)),
        }
    }
}

/// Go `writePhysicalTableRecord` over the txn backfill executor: the
/// workers run the region tasks of `[start_key, end_key)`, the result loop
/// counts their rows and warnings into `total` / `warnings` and moves the
/// reorg handle past the tasks done in order (every `4 x workers` results,
/// and when the step ends). Returns how it ended and the handle's key.
pub(super) fn write_physical_table_record(
    exec: &BackfillExecutor<'_>,
    start_key: &[u8],
    end_key: &[u8],
    total: &mut i64,
    warnings: &mut Vec<(u16, String, i64)>,
) -> Result<(TableRecordOutcome, Vec<u8>), ClusterDdlError> {
    // Go `txnBackfillExecutor.expectedWorkerSize`.
    let expected_worker_size = |concurrency: usize| concurrency.clamp(1, MAX_BACKFILL_WORKER_SIZE);
    let stop = AtomicBool::new(false);
    let generated = AtomicBool::new(false);
    // The workers still running (Go `txnBackfillExecutor.wg`).
    let live = AtomicUsize::new(0);
    // Go `backfillCtx.batchCnt`, which each worker takes up per task.
    let batch_cnt = AtomicUsize::new(exec.batch_size.max(1));
    let generate_error: Mutex<Option<ClusterDdlError>> = Mutex::new(None);
    let mut keeper = DoneTaskKeeper::new(start_key.to_vec());
    let mut partial: HashMap<usize, Vec<u8>> = HashMap::new();
    let mut failure: Option<TableRecordOutcome> = None;
    let mut all_done = true;
    std::thread::scope(|scope| {
        let (task_tx, task_rx) = bounded::<ReorgBackfillTask>(BACKFILL_TASK_CHAN_SIZE);
        let (result_tx, result_rx) = bounded::<BackfillResult>(BACKFILL_TASK_CHAN_SIZE);
        let (stop, generated, batch_cnt, live) = (&stop, &generated, &batch_cnt, &live);
        // Go `backfillWorker.run`; a worker `adjustWorkerSize` closes leaves
        // after its task.
        let spawn_worker = |result_tx: crossbeam_channel::Sender<BackfillResult>| {
            let task_rx = task_rx.clone();
            let closed = Arc::new(AtomicBool::new(false));
            let worker_closed = Arc::clone(&closed);
            live.fetch_add(1, Ordering::SeqCst);
            scope.spawn(tidb_cpuprofile::labels::inherit(move || {
                struct Done<'a>(&'a AtomicUsize);
                impl Drop for Done<'_> {
                    fn drop(&mut self) {
                        self.0.fetch_sub(1, Ordering::SeqCst);
                    }
                }
                let _done = Done(live);
                while !worker_closed.load(Ordering::SeqCst) {
                    let Ok(task) = task_rx.recv() else {
                        return;
                    };
                    // A stopped step drains the tasks it will not run:
                    // `handle_backfill_task` returns them unstarted.
                    let result = handle_backfill_task(exec, task, batch_cnt.load(Ordering::SeqCst), stop);
                    let failed = result.err.is_some();
                    if result_tx.send(result).is_err() || failed {
                        stop.store(true, Ordering::SeqCst);
                    }
                }
            }));
            closed
        };
        let mut workers: Vec<Arc<AtomicBool>> = (0..expected_worker_size(exec.concurrency))
            .map(|_| spawn_worker(result_tx.clone()))
            .collect();
        // The sender new workers get, held until the workers have run every
        // task (Go's `doneCh`, closed after `exec.close(false)` waits for
        // them, ends the worker-count goroutine).
        let mut spawn_tx = Some(result_tx);
        // Go's generate task goroutine.
        let generate_error = &generate_error;
        let worker_cnt = workers.len();
        scope.spawn(tidb_cpuprofile::labels::inherit(move || {
            let (mut start, end) = (start_key.to_vec(), end_key.to_vec());
            let mut task_id = 0;
            loop {
                let kv_ranges = match load_table_ranges(
                    exec.region_store,
                    exec.physical_id,
                    &start,
                    &end,
                    BACKFILL_TASK_CHAN_SIZE,
                ) {
                    Ok(ranges) => ranges,
                    Err(error) => {
                        *generate_error.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
                        stop.store(true, Ordering::SeqCst);
                        break;
                    }
                };
                if kv_ranges.is_empty() {
                    break;
                }
                info(
                    "start backfill workers to reorg record",
                    vec![
                        tidb_log::Field::new("workerCnt", tidb_log::Value::I64(worker_cnt as i64)),
                        tidb_log::Field::new("regionCnt", tidb_log::Value::I64(kv_ranges.len() as i64)),
                        hex_field("startKey", &start),
                        hex_field("endKey", &end),
                    ],
                );
                // Go `sendTasks` over `getBatchTasks`.
                let mut sent = true;
                for (range_start, range_end) in &kv_ranges {
                    if stop.load(Ordering::SeqCst) {
                        sent = false;
                        break;
                    }
                    let end_key = if exec.shrink_end_key {
                        // Go `getActualEndKey`: a failure keeps the region end.
                        (exec.range_end_key)(range_start, range_end).unwrap_or_else(|_| range_end.clone())
                    } else {
                        range_end.clone()
                    };
                    let task = ReorgBackfillTask {
                        id: task_id,
                        start_key: range_start.clone(),
                        end_key,
                    };
                    task_id += 1;
                    if task_tx.send(task).is_err() {
                        sent = false;
                        break;
                    }
                }
                start = kv_ranges[kv_ranges.len() - 1].1.clone();
                if !sent || start >= end {
                    break;
                }
            }
            // Go `exec.close(false)`: the workers finish the queued tasks.
            drop(task_tx);
            generated.store(true, Ordering::SeqCst);
        }));
        // Go's process result goroutine, with the worker-count goroutine's
        // ticker.
        let mut cnt = 0;
        let mut last_config = Instant::now();
        loop {
            if spawn_tx.is_some() && generated.load(Ordering::SeqCst) && live.load(Ordering::SeqCst) == 0 {
                spawn_tx = None;
            }
            if spawn_tx.is_some() && last_config.elapsed() >= UPDATE_DDL_JOB_REORG_CFG_INTERVAL {
                last_config = Instant::now();
                if let Some((concurrency, batch_size)) = (exec.reload_config)() {
                    batch_cnt.store(batch_size.max(1), Ordering::SeqCst);
                    // Go `adjustWorkerSize`.
                    let target = expected_worker_size(concurrency);
                    while workers.len() < target {
                        let result_tx = spawn_tx.clone().expect("held while tasks are generated");
                        workers.push(spawn_worker(result_tx));
                    }
                    while workers.len() > target {
                        workers.pop().expect("more workers than the target").store(true, Ordering::SeqCst);
                    }
                }
            }
            let result = match result_rx.recv_timeout(UPDATE_DDL_JOB_REORG_CFG_INTERVAL / 4) {
                Ok(result) => result,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            };
            cnt += 1;
            *total += result.added_count as i64;
            merge_warnings(warnings, result.warnings);
            if let Some(err) = result.err {
                tidb_ddl_logutil::ddl_logger().warn(
                    "backfill worker failed",
                    &[
                        tidb_log::Field::new("total added count", tidb_log::Value::I64(*total)),
                        hex_field("start key", start_key),
                        hex_field("result next key", &result.next_key),
                    ],
                );
                failure.get_or_insert(err);
                stop.store(true, Ordering::SeqCst);
                continue;
            }
            if !result.done {
                all_done = false;
                partial.insert(result.task_id, result.next_key);
                continue;
            }
            keeper.update_next_key(result.task_id, result.next_key);
            if cnt % (workers.len().max(1) * 4) == 0 {
                (exec.update_reorg_meta)(&keeper.next_key);
            }
        }
    });
    if let Some(error) = generate_error.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner) {
        return Err(error);
    }
    // A task stopped part way resumes where it stopped.
    let next_key = partial.remove(&keeper.current).unwrap_or(keeper.next_key);
    match failure {
        Some(TableRecordOutcome::Failed(error)) => Ok((TableRecordOutcome::Failed(error), next_key)),
        Some(_) => {
            (exec.update_reorg_meta)(&next_key);
            Ok((TableRecordOutcome::Interrupted, next_key))
        }
        None if stop.load(Ordering::SeqCst) || !all_done || next_key.as_slice() < end_key => {
            (exec.update_reorg_meta)(&next_key);
            Ok((TableRecordOutcome::Progress, next_key))
        }
        None => {
            info(
                "backfill workers successfully processed",
                vec![
                    tidb_log::Field::new("total added count", tidb_log::Value::I64(*total)),
                    hex_field("start key", start_key),
                ],
            );
            (exec.update_reorg_meta)(&next_key);
            Ok((TableRecordOutcome::Done, next_key))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestDoneTaskKeeper`.
    #[test]
    fn done_task_keeper_advances_over_tasks_done_in_order() {
        let mut keeper = DoneTaskKeeper::new(b"a".to_vec());
        keeper.update_next_key(0, b"b".to_vec());
        keeper.update_next_key(1, b"c".to_vec());
        assert_eq!(keeper.next_key, b"c");
        keeper.update_next_key(4, b"f".to_vec());
        keeper.update_next_key(3, b"e".to_vec());
        assert_eq!(keeper.next_key, b"c");
        keeper.update_next_key(2, b"d".to_vec());
        assert_eq!(keeper.next_key, b"f");
        assert_eq!(keeper.current, 5);
    }

    /// Go `validateAndFillRanges`: the first range starts at the backfill's
    /// start, the last ends at its end, and the ranges must touch.
    #[test]
    fn validate_and_fill_ranges_clamps_and_checks_continuity() {
        let mut ranges = vec![(Vec::new(), b"c".to_vec()), (b"c".to_vec(), Vec::new())];
        validate_and_fill_ranges(&mut ranges, b"b", b"x").unwrap();
        assert_eq!(ranges, [(b"b".to_vec(), b"c".to_vec()), (b"c".to_vec(), b"x".to_vec())]);
        let mut gap = vec![(b"a".to_vec(), b"c".to_vec()), (b"d".to_vec(), b"z".to_vec())];
        assert!(validate_and_fill_ranges(&mut gap, b"b", b"x")
            .unwrap_err()
            .to_string()
            .contains("ranges are not continuous"));
        let mut late = vec![(b"c".to_vec(), b"z".to_vec())];
        assert!(validate_and_fill_ranges(&mut late, b"b", b"x")
            .unwrap_err()
            .to_string()
            .contains("get empty range at the beginning of ranges"));
    }

    /// Go's executor: the region tasks run on the workers, each over its own
    /// range, and the handle ends past the last task; a failing task stops
    /// the rest and fails the backfill.
    #[test]
    fn workers_run_every_region_task_and_stop_on_a_failure() {
        #[derive(Debug)]
        struct Regions;
        impl RegionStore for Regions {
            fn load_regions_in_key_range(
                &self,
                start: &[u8],
                _end: &[u8],
            ) -> Result<Vec<tidb_kv::region_map::RegionMeta>, String> {
                let bounds: [(&[u8], &[u8]); 4] = [(b"", b"c"), (b"c", b"f"), (b"f", b"k"), (b"k", b"")];
                Ok(bounds
                    .iter()
                    .filter(|(_, end)| end.is_empty() || *end > start)
                    .map(|(start, end)| tidb_kv::region_map::RegionMeta {
                        start_key: start.to_vec(),
                        end_key: end.to_vec(),
                        ..tidb_kv::region_map::RegionMeta::default()
                    })
                    .collect())
            }
            fn split_regions(&self, _: Vec<Vec<u8>>, _: bool, _: Option<i64>) -> (Vec<u64>, Option<String>) {
                (Vec::new(), None)
            }
            fn wait_scatter_region_finish(&self, _: u64, _: i64) -> Result<(), String> {
                Ok(())
            }
            fn check_region_in_scattering(&self, _: u64) -> Result<bool, String> {
                Ok(false)
            }
        }
        let ran = Mutex::new(Vec::new());
        let threads = Mutex::new(std::collections::HashSet::new());
        let backfill = |start: &[u8], end: &[u8], _batch: usize| {
            ran.lock().unwrap().push((start.to_vec(), end.to_vec()));
            threads.lock().unwrap().insert(std::thread::current().id());
            std::thread::sleep(Duration::from_millis(20));
            if start == b"f" && end == b"j" {
                return Ok(BatchStep::Failed(LockSqlError::from(tidb_terror::MysqlError::unknown("dup"))));
            }
            Ok(BatchStep::Advanced { next_key: end.to_vec(), done: true, added: 1, batch_warnings: Vec::new() })
        };
        // Row tasks shrink to the last row (`GetRangeEndKey`).
        let range_end = |_start: &[u8], end: &[u8]| Ok(if end == b"k" { b"j".to_vec() } else { end.to_vec() });
        let updates = Mutex::new(Vec::new());
        let update = |key: &[u8]| updates.lock().unwrap().push(key.to_vec());
        let exec = BackfillExecutor {
            region_store: Some(&Regions),
            physical_id: 1,
            concurrency: 4,
            batch_size: 128,
            reload_config: &|| None,
            shrink_end_key: false,
            backfill_data: &backfill,
            range_end_key: &range_end,
            update_reorg_meta: &update,
            deadline: Instant::now() + Duration::from_secs(60),
        };
        let (mut total, mut warnings) = (0, Vec::new());
        let (outcome, next) = write_physical_table_record(&exec, b"a", b"z", &mut total, &mut warnings).unwrap();
        assert!(matches!(outcome, TableRecordOutcome::Done));
        assert_eq!(next, b"z");
        assert_eq!(total, 4);
        let mut ranges = ran.lock().unwrap().clone();
        ranges.sort();
        assert_eq!(
            ranges,
            [
                (b"a".to_vec(), b"c".to_vec()),
                (b"c".to_vec(), b"f".to_vec()),
                (b"f".to_vec(), b"k".to_vec()),
                (b"k".to_vec(), b"z".to_vec())
            ]
        );
        assert!(threads.lock().unwrap().len() > 1, "the tasks ran on several workers");
        assert_eq!(updates.lock().unwrap().last().unwrap(), b"z");

        let failing = BackfillExecutor { shrink_end_key: true, concurrency: 1, ..exec };
        ran.lock().unwrap().clear();
        let (outcome, next) = write_physical_table_record(&failing, b"a", b"z", &mut total, &mut warnings).unwrap();
        assert!(matches!(outcome, TableRecordOutcome::Failed(_)));
        // The handle stays past the tasks done before the failed one.
        assert_eq!(next, b"f");
        assert!(!ran.lock().unwrap().iter().any(|(start, _)| start == b"k"), "no task runs after a failure");
    }

    /// Go's worker-count goroutine: every `UpdateDDLJobReorgCfgInterval`
    /// the job's concurrency and batch size are read again, the workers
    /// resized (`adjustWorkerSize`), and later tasks run batches of the new
    /// size.
    #[test]
    fn workers_and_batch_size_follow_the_job_while_running() {
        #[derive(Debug)]
        struct Regions;
        impl RegionStore for Regions {
            fn load_regions_in_key_range(
                &self,
                start: &[u8],
                _end: &[u8],
            ) -> Result<Vec<tidb_kv::region_map::RegionMeta>, String> {
                Ok((b'a'..=b'l')
                    .map(|key| (vec![key], vec![key + 1]))
                    .filter(|(_, end)| end.as_slice() > start)
                    .map(|(start_key, end_key)| tidb_kv::region_map::RegionMeta {
                        start_key,
                        end_key,
                        ..tidb_kv::region_map::RegionMeta::default()
                    })
                    .collect())
            }
            fn split_regions(&self, _: Vec<Vec<u8>>, _: bool, _: Option<i64>) -> (Vec<u64>, Option<String>) {
                (Vec::new(), None)
            }
            fn wait_scatter_region_finish(&self, _: u64, _: i64) -> Result<(), String> {
                Ok(())
            }
            fn check_region_in_scattering(&self, _: u64) -> Result<bool, String> {
                Ok(false)
            }
        }
        let seen = Mutex::new(Vec::new());
        let running = AtomicUsize::new(0);
        let peak_late = AtomicUsize::new(0);
        let begun = Instant::now();
        let backfill = |_start: &[u8], end: &[u8], batch: usize| {
            let now = running.fetch_add(1, Ordering::SeqCst) + 1;
            if begun.elapsed() > Duration::from_millis(2300) {
                peak_late.fetch_max(now, Ordering::SeqCst);
            }
            std::thread::sleep(Duration::from_millis(400));
            running.fetch_sub(1, Ordering::SeqCst);
            seen.lock().unwrap().push(batch);
            Ok(BatchStep::Advanced { next_key: end.to_vec(), done: true, added: 1, batch_warnings: Vec::new() })
        };
        let range_end = |_: &[u8], end: &[u8]| Ok(end.to_vec());
        let update = |_: &[u8]| {};
        let exec = BackfillExecutor {
            region_store: Some(&Regions),
            physical_id: 1,
            concurrency: 1,
            batch_size: 128,
            reload_config: &|| Some((4, 7)),
            shrink_end_key: false,
            backfill_data: &backfill,
            range_end_key: &range_end,
            update_reorg_meta: &update,
            deadline: Instant::now() + Duration::from_secs(60),
        };
        let (mut total, mut warnings) = (0, Vec::new());
        let (outcome, next) = write_physical_table_record(&exec, b"a", b"m", &mut total, &mut warnings).unwrap();
        assert!(matches!(outcome, TableRecordOutcome::Done));
        assert_eq!(next, b"m");
        assert_eq!(total, 12);
        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen[0], 128);
        assert_eq!(*seen.last().unwrap(), 7);
        assert!(peak_late.load(Ordering::SeqCst) > 1, "the workers grew to the job's concurrency");
    }

    /// A step whose time runs out mid-task hands the job back with the
    /// handle where that task stopped (Go's `ErrWaitReorgTimeout` with the
    /// reorg meta last updated), so the next step resumes there.
    #[test]
    fn a_deadline_mid_task_resumes_where_the_task_stopped() {
        let backfill = |start: &[u8], _end: &[u8], _batch: usize| {
            std::thread::sleep(Duration::from_millis(80));
            let mut next = start.to_vec();
            next.push(b'5');
            Ok(BatchStep::Advanced { next_key: next, done: false, added: 2, batch_warnings: Vec::new() })
        };
        let range_end = |_: &[u8], end: &[u8]| Ok(end.to_vec());
        let updates = Mutex::new(Vec::new());
        let update = |key: &[u8]| updates.lock().unwrap().push(key.to_vec());
        let exec = BackfillExecutor {
            region_store: None,
            physical_id: 1,
            concurrency: 2,
            batch_size: 128,
            reload_config: &|| None,
            shrink_end_key: true,
            backfill_data: &backfill,
            range_end_key: &range_end,
            update_reorg_meta: &update,
            deadline: Instant::now() + Duration::from_millis(40),
        };
        let (mut total, mut warnings) = (0, Vec::new());
        let (outcome, next) = write_physical_table_record(&exec, b"a", b"z", &mut total, &mut warnings).unwrap();
        assert!(matches!(outcome, TableRecordOutcome::Progress));
        assert_eq!(next, b"a5");
        assert_eq!(total, 2);
        assert_eq!(updates.lock().unwrap().last().unwrap(), b"a5");
    }
}
