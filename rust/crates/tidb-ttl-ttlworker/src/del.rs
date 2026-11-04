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

//! Go `pkg/ttl/ttlworker/del.go`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::Receiver;
use tidb_datatype::Datum;
use tidb_logutil::bg_logger;
use tidb_timer::go_time::GoTime;
use tidb_ttl::cache::table::PhysicalTable;
use tidb_ttl::sql_builder::build_delete_sql;
use tidb_ttl_client::context::Context;
use tidb_ttl_metrics::{
    new_delete_worker_phase_tracer, PhaseTracer, PhaseTracerExt, DELETE_ERROR_DURATION,
    DELETE_SUCCESS_DURATION, PHASE_IDLE, PHASE_OTHER, PHASE_WAIT_TOKEN,
};

use crate::scan::TtlStatistics;
use crate::session::{new_table_session, with_session, SessionPool};
use crate::worker::{BaseWorker, Worker, WorkerStatus};
use crate::{field, int_field, Result, Session};

/// Go `delMaxRetry`.
pub const DEL_MAX_RETRY: usize = 3;
/// Go `delRetryBufferSize`.
pub const DEL_RETRY_BUFFER_SIZE: usize = 128;
/// Go `delRetryInterval`.
pub const DEL_RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// Go `rate.Limiter` with a burst of one, as `defaultDelRateLimiter` uses
/// it: a token bucket whose `Wait` blocks until a token is available.
struct RateLimiter {
    /// Tokens per second; `None` is `rate.Inf`.
    limit: Option<f64>,
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    /// Go `rate.NewLimiter(rate.Inf, 1)`.
    fn new() -> Self {
        Self {
            limit: None,
            tokens: 1.0,
            last: Instant::now(),
        }
    }

    fn advance(&self, now: Instant) -> f64 {
        match self.limit {
            None => 1.0,
            Some(limit) => {
                let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
                (self.tokens + elapsed * limit).min(1.0)
            }
        }
    }

    /// Go `SetLimit`: tokens accrue at the old rate up to now first.
    fn set_limit(&mut self, limit: Option<f64>) {
        let now = Instant::now();
        self.tokens = self.advance(now);
        self.last = now;
        self.limit = limit;
    }

    /// Go `reserveN(now, 1, ...)`: the wait before the reserved token.
    fn reserve(&mut self) -> Duration {
        let Some(limit) = self.limit else {
            return Duration::ZERO;
        };
        let now = Instant::now();
        let tokens = self.advance(now) - 1.0;
        self.tokens = tokens;
        self.last = now;
        if tokens >= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(-tokens / limit)
        }
    }

    /// Go `Reservation.Cancel`: returns an unused token.
    fn cancel_reservation(&mut self) {
        if self.limit.is_some() {
            self.tokens = (self.tokens + 1.0).min(1.0);
        }
    }
}

/// Go `delRateLimiter`.
pub trait DelRateLimiter: Send + Sync {
    /// Go `WaitDelToken`.
    fn wait_del_token(&self, ctx: &Context) -> Result<()>;
}

/// Go `defaultDelRateLimiter`.
pub struct DefaultDelRateLimiter {
    /// Go `limiter`; its limit ranges over `[1.0, +Inf]`, `+Inf` when
    /// `tidb_ttl_delete_rate_limit` is `0`.
    limiter: Mutex<RateLimiter>,
    /// Go `limit`: the variable's value the limiter was set from; `0` means
    /// unlimited.
    limit: AtomicI64,
}

/// Go `newDelRateLimiter`.
pub fn new_del_rate_limiter() -> DefaultDelRateLimiter {
    DefaultDelRateLimiter {
        limiter: Mutex::new(RateLimiter::new()),
        limit: AtomicI64::new(0),
    }
}

/// Go `globalDelRateLimiter`.
pub static GLOBAL_DEL_RATE_LIMITER: LazyLock<DefaultDelRateLimiter> = LazyLock::new(new_del_rate_limiter);

impl DelRateLimiter for DefaultDelRateLimiter {
    fn wait_del_token(&self, ctx: &Context) -> Result<()> {
        let mut limit = self.limit.load(Ordering::SeqCst);
        if tidb_vardef::TTL_DELETE_RATE_LIMIT.load(Ordering::SeqCst) != limit {
            limit = self.reset();
        }

        debug_assert!(limit >= 0);
        if limit <= 0 {
            return ctx.err().map_or(Ok(()), |err| Err(err.to_string()));
        }

        let wait = self.limiter.lock().unwrap_or_else(std::sync::PoisonError::into_inner).reserve();
        if wait.is_zero() {
            return Ok(());
        }
        if ctx.sleep(wait) {
            self.limiter
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .cancel_reservation();
            return Err(ctx.err().map(|err| err.to_string()).unwrap_or_default());
        }
        Ok(())
    }
}

impl DefaultDelRateLimiter {
    /// Go `reset`.
    fn reset(&self) -> i64 {
        let mut limiter = self.limiter.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let new_limit = tidb_vardef::TTL_DELETE_RATE_LIMIT.load(Ordering::SeqCst);
        if new_limit != self.limit.load(Ordering::SeqCst) {
            self.limit.store(new_limit, Ordering::SeqCst);
            // When `TTLDeleteRateLimit > 0`, use the setting as the rate limit.
            // Otherwise, use `rate.Inf` to make it unlimited.
            limiter.set_limit((new_limit > 0).then_some(new_limit as f64));
        }
        new_limit
    }
}

/// Go `ttlDeleteTask`.
#[derive(Clone)]
pub struct TtlDeleteTask {
    /// Go `jobID`.
    pub job_id: String,
    /// Go `scanID`.
    pub scan_id: i64,
    /// Go `tbl`.
    pub tbl: PhysicalTable,
    /// Go `expire`.
    pub expire: GoTime,
    /// Go `rows`.
    pub rows: Vec<Vec<Datum>>,
    /// Go `statistics`.
    pub statistics: Arc<TtlStatistics>,
}

impl TtlDeleteTask {
    /// Go `taskLogger`'s fields.
    fn task_log_fields(&self) -> Vec<tidb_log::Field> {
        vec![
            field("jobID", self.job_id.clone()),
            int_field("scanID", self.scan_id),
            int_field("tableID", self.tbl.id),
            field("table", self.tbl.full_name()),
        ]
    }

    /// Go `doDelete`: deletes the rows in batches, returning the rows to
    /// retry.
    pub fn do_delete(
        &self,
        ctx: &Context,
        tracer: Option<&PhaseTracer>,
        limiter: &dyn DelRateLimiter,
        raw_se: &Session,
    ) -> Vec<Vec<Datum>> {
        let restore_phase = tracer.phase();
        tracer.enter_phase(PHASE_OTHER);

        let mut retry_rows: Vec<Vec<Datum>> = Vec::new();
        let mut left_rows: &[Vec<Datum>] = &self.rows;
        let se = new_table_session(raw_se, &self.tbl, &self.expire);
        while !left_rows.is_empty() && ctx.err().is_none() {
            let max_batch = tidb_vardef::TTL_DELETE_BATCH_SIZE.load(Ordering::SeqCst);
            let del_batch;
            if (left_rows.len() as i64) < max_batch {
                del_batch = left_rows;
                left_rows = &[];
            } else {
                let (batch, rest) = left_rows.split_at(max_batch as usize);
                del_batch = batch;
                left_rows = rest;
            }

            let sql = match build_delete_sql(&self.tbl, del_batch, self.expire.unix()) {
                Ok(sql) => sql,
                Err(err) => {
                    self.statistics.inc_error_rows(del_batch.len());
                    let mut fields = self.task_log_fields();
                    fields.push(field("error", err.to_string()));
                    bg_logger().warn("build delete SQL in TTL failed", &fields);
                    break;
                }
            };

            tracer.enter_phase(PHASE_WAIT_TOKEN);
            if let Err(err) = limiter.wait_del_token(ctx) {
                tracer.enter_phase(PHASE_OTHER);
                let mut fields = self.task_log_fields();
                fields.push(field("error", err));
                fields.push(int_field("waitDelRowCnt", del_batch.len() as i64));
                bg_logger().info("wait TTL delete rate limiter interrupted", &fields);
                retry_rows.extend_from_slice(del_batch);
                continue;
            }
            tracer.enter_phase(PHASE_OTHER);

            let sql_start = Instant::now();
            let (_, need_retry, err) = se.execute_sql_with_check(tracer, &sql);
            let sql_interval = sql_start.elapsed();
            if let Some(err) = err {
                DELETE_ERROR_DURATION.observe(sql_interval.as_secs_f64());
                let mut fields = self.task_log_fields();
                fields.push(field("error", err));
                fields.push(field("SQL", sql));
                fields.push(tidb_log::Field::new("needRetry", tidb_log::Value::Bool(need_retry)));
                bg_logger().warn("delete SQL in TTL failed", &fields);

                if need_retry {
                    retry_rows.extend_from_slice(del_batch);
                } else {
                    self.statistics.inc_error_rows(del_batch.len());
                }
                continue;
            }

            DELETE_SUCCESS_DURATION.observe(sql_interval.as_secs_f64());
            self.statistics.inc_success_rows(del_batch.len());
        }
        // Go's deferred append of the rows the loop never reached.
        retry_rows.extend_from_slice(left_rows);
        tracer.enter_phase(&restore_phase);
        retry_rows
    }
}

/// Go `ttlDelRetryItem`.
pub struct TtlDelRetryItem {
    /// Go `task`.
    pub task: TtlDeleteTask,
    /// Go `retryCnt`.
    pub retry_cnt: usize,
    /// Go `inTime`.
    pub in_time: Instant,
}

/// Go `ttlDelRetryBuffer`.
pub struct TtlDelRetryBuffer {
    list: VecDeque<TtlDelRetryItem>,
    max_size: usize,
    max_retry: usize,
    retry_interval: Duration,
    get_time: Box<dyn Fn() -> Instant + Send>,
}

/// Go `newTTLDelRetryBuffer`.
pub fn new_ttl_del_retry_buffer() -> TtlDelRetryBuffer {
    TtlDelRetryBuffer {
        list: VecDeque::new(),
        max_size: DEL_RETRY_BUFFER_SIZE,
        max_retry: DEL_MAX_RETRY,
        retry_interval: DEL_RETRY_INTERVAL,
        get_time: Box::new(Instant::now),
    }
}

impl TtlDelRetryBuffer {
    /// Go `Len`.
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Whether the buffer holds no item.
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// The buffer's bounds and clock, as Go's tests assign the fields.
    pub fn configure(
        &mut self,
        max_size: usize,
        max_retry: usize,
        retry_interval: Duration,
        get_time: Box<dyn Fn() -> Instant + Send>,
    ) {
        self.max_size = max_size;
        self.max_retry = max_retry;
        self.retry_interval = retry_interval;
        self.get_time = get_time;
    }

    /// The buffered items, front first.
    pub fn items(&self) -> impl Iterator<Item = &TtlDelRetryItem> {
        self.list.iter()
    }

    /// Go `RecordTaskResult`.
    pub fn record_task_result(&mut self, task: &TtlDeleteTask, retry_rows: Vec<Vec<Datum>>) {
        self.record_retry_item(task, retry_rows, 0);
    }

    /// Go `DoRetry`: retries the due items, and answers how long until the
    /// next is due.
    pub fn do_retry(&mut self, mut do_fn: impl FnMut(&TtlDelRetryItem) -> Vec<Vec<Datum>>) -> Duration {
        let l = self.list.len();
        // When `retryInterval==0`, to avoid the infinite retries, limit the max loop to the buffer length.
        // It means one item only has one chance to retry in one `DoRetry` invoking.
        for _ in 0..l {
            let Some(item) = self.list.front() else {
                break;
            };
            let now = (self.get_time)();
            let interval = now.saturating_duration_since(item.in_time);
            if interval < self.retry_interval {
                return self.retry_interval - interval;
            }

            let item = self.list.pop_front().expect("the front item exists");
            let retry_rows = do_fn(&item);
            if !retry_rows.is_empty() {
                self.record_retry_item(&item.task, retry_rows, item.retry_cnt + 1);
            }
        }
        self.retry_interval
    }

    /// Go `SetRetryInterval`.
    pub fn set_retry_interval(&mut self, interval: Duration) {
        self.retry_interval = interval;
    }

    /// Go `Drain`: counts every buffered row as an error.
    pub fn drain(&mut self) {
        for item in &self.list {
            item.task.statistics.inc_error_rows(item.task.rows.len());
        }
        self.list.clear();
    }

    /// Go `recordRetryItem`.
    fn record_retry_item(&mut self, task: &TtlDeleteTask, retry_rows: Vec<Vec<Datum>>, retry_cnt: usize) -> bool {
        if retry_rows.is_empty() {
            return false;
        }

        if retry_cnt >= self.max_retry {
            let mut fields = task.task_log_fields();
            fields.push(int_field("rowCnt", retry_rows.len() as i64));
            fields.push(int_field("retryCnt", retry_cnt as i64));
            bg_logger().warn("discard TTL rows that has failed more than maxRetry times", &fields);
            task.statistics.inc_error_rows(retry_rows.len());
            return false;
        }

        while !self.list.is_empty() && self.list.len() >= self.max_size {
            let item = self.list.pop_front().expect("the front item exists");
            let mut fields = task.task_log_fields();
            fields.push(int_field("rowCnt", retry_rows.len() as i64));
            fields.push(int_field("bufferSize", self.list.len() as i64 + 1));
            bg_logger().warn("discard TTL rows because the retry buffer is full", &fields);
            item.task.statistics.inc_error_rows(item.task.rows.len());
        }

        let mut new_task = task.clone();
        new_task.rows = retry_rows;
        self.list.push_back(TtlDelRetryItem {
            task: new_task,
            in_time: (self.get_time)(),
            retry_cnt,
        });
        true
    }
}

/// Go `ttlDeleteWorker`.
pub struct TtlDeleteWorker {
    base: Arc<BaseWorker<(), ()>>,
}

/// Go `newDeleteWorker`.
pub fn new_delete_worker(del_ch: Receiver<TtlDeleteTask>, sess_pool: Arc<dyn SessionPool>) -> Arc<TtlDeleteWorker> {
    let base = BaseWorker::new(());
    let loop_base = Arc::downgrade(&base);
    base.set_loop(Box::new(move || {
        let Some(base) = loop_base.upgrade() else {
            return Ok(());
        };
        delete_worker_loop(&base, &del_ch, sess_pool.as_ref())
    }));
    Arc::new(TtlDeleteWorker { base })
}

/// Go `(*ttlDeleteWorker).loop`.
fn delete_worker_loop(
    base: &BaseWorker<(), ()>,
    del_ch: &Receiver<TtlDeleteTask>,
    sess_pool: &dyn SessionPool,
) -> Result<()> {
    let tracer = new_delete_worker_phase_tracer();
    let tracer = Some(&tracer);
    tracer.enter_phase(PHASE_OTHER);
    let mut retry_buffer = new_ttl_del_retry_buffer();
    let result = with_session(sess_pool, &mut |se| {
        loop_with_session(base, tracer, del_ch, &mut retry_buffer, &*GLOBAL_DEL_RATE_LIMITER, se)
    });
    tracer.end_phase();
    bg_logger().info("ttlDeleteWorker loop exited.", &[]);
    result
}

/// Go `loopWithSession`.
pub fn loop_with_session<M, S>(
    base: &BaseWorker<M, S>,
    tracer: Option<&PhaseTracer>,
    del_ch: &Receiver<TtlDeleteTask>,
    retry_buffer: &mut TtlDelRetryBuffer,
    limiter: &dyn DelRateLimiter,
    se: &Session,
) -> Result<()>
where
    M: Send + 'static,
    S: Send + 'static,
{
    let ctx = base.ctx.clone();
    let mut next_retry = Instant::now() + retry_buffer.retry_interval;
    while base.status() == WorkerStatus::Running {
        tracer.enter_phase(PHASE_IDLE);
        crossbeam_channel::select! {
            recv(ctx.done()) -> _ => break,
            recv(crossbeam_channel::at(next_retry)) -> _ => {
                tracer.enter_phase(PHASE_OTHER);
                let next_interval = retry_buffer.do_retry(|item| item.task.do_delete(&ctx, tracer, limiter, se));
                next_retry = Instant::now() + next_interval;
            }
            recv(del_ch) -> task => {
                tracer.enter_phase(PHASE_OTHER);
                let Ok(task) = task else {
                    break;
                };
                let retry_rows = task.do_delete(&ctx, tracer, limiter, se);
                retry_buffer.record_task_result(&task, retry_rows);
            }
        }
    }

    // Have a final try to delete all rows in retry buffer while the worker stops
    // to avoid leaving any TTL rows undeleted when shrinking the delete worker.
    if !retry_buffer.is_empty() {
        let start = Instant::now();
        bg_logger().info(
            "try to delete TTL rows in del worker buffer immediately because the worker is going to stop",
            &[int_field("bufferLen", retry_buffer.len() as i64)],
        );
        let (retry_ctx, cancel) = Context::with_timeout(&Context::background(), Duration::from_secs(10));
        retry_buffer.set_retry_interval(Duration::ZERO);
        retry_buffer.do_retry(|item| item.task.do_delete(&retry_ctx, tracer, limiter, se));
        cancel.cancel();
        bg_logger().info(
            "delete TTL rows in del worker buffer finished",
            &[tidb_log::Field::new(
                "duration",
                tidb_log::Value::Duration(start.elapsed().as_nanos() as i64),
            )],
        );
    }

    // drain retry buffer to make sure the statistics are correct
    if !retry_buffer.is_empty() {
        bg_logger().warn(
            "some TTL rows are still in the buffer while the worker is going to stop, mark them as error",
            &[int_field("bufferLen", retry_buffer.len() as i64)],
        );
        retry_buffer.drain();
    }
    Ok(())
}

impl Worker for TtlDeleteWorker {
    fn start(&self) {
        self.base.start();
    }

    fn stop(&self) {
        self.base.stop();
    }

    fn status(&self) -> WorkerStatus {
        self.base.status()
    }

    fn error(&self) -> Option<String> {
        self.base.error()
    }

    fn wait_stopped(&self, ctx: &Context, timeout: Duration) -> std::result::Result<(), tidb_ttl_client::CtxError> {
        self.base.wait_stopped(ctx, timeout)
    }
}
