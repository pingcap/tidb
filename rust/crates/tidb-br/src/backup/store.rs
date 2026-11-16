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

//! Go `br/pkg/backup/store.go`: the backup push-down to one store, over a
//! streaming `Backup` RPC per batch of sub-ranges.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel::{select, Sender};
use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_proto::backup::{self, BackupRequest, BackupResponse};
use tonic::transport::Channel;

use super::limit::ResourceConcurrentLimiter;
use crate::errors::{self, Result};
use crate::pdclient::PdClient;
use crate::utils::backoff::new_backup_sst_backoff_strategy;
use crate::utils::retry::with_retry;
use crate::utils::storewatch::{Callback, Watcher};

/// Go `BackupRetryPolicy`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BackupRetryPolicy {
    /// Go `One`: retry this store.
    pub one: u64,
    /// Go `All`: restart every store.
    pub all: bool,
}

/// Go `ResponseAndStore`.
#[derive(Clone, Debug)]
pub struct ResponseAndStore {
    /// Go `Resp`.
    pub resp: BackupResponse,
    /// Go `StoreID`.
    pub store_id: u64,
}

/// A gRPC backup client.
pub type BackupClient = backup::backup_client::BackupClient<Channel>;

/// Go `BackupSender`.
pub trait BackupSender: Send + Sync {
    /// Go `SendAsync`: backs up `request` on the store, sending each
    /// response to `resp_tx` and dropping it when done.
    #[allow(clippy::too_many_arguments)]
    fn send_async(
        &self,
        ctx: &Context,
        round: u64,
        store_id: u64,
        limiter: Arc<ResourceConcurrentLimiter>,
        request: BackupRequest,
        concurrency: usize,
        cli: BackupClient,
        resp_tx: Sender<ResponseAndStore>,
        state_notifier: Sender<BackupRetryPolicy>,
    );
}

/// Go `MainBackupSender`.
pub struct MainBackupSender {
    /// The runtime the RPCs run on.
    pub runtime: Arc<tokio::runtime::Runtime>,
}

impl BackupSender for MainBackupSender {
    fn send_async(
        &self,
        ctx: &Context,
        round: u64,
        store_id: u64,
        limiter: Arc<ResourceConcurrentLimiter>,
        request: BackupRequest,
        concurrency: usize,
        cli: BackupClient,
        resp_tx: Sender<ResponseAndStore>,
        state_notifier: Sender<BackupRetryPolicy>,
    ) {
        let ctx = ctx.clone();
        let runtime = Arc::clone(&self.runtime);
        std::thread::spawn(move || {
            let res = start_backup(
                &ctx,
                &runtime,
                store_id,
                &limiter,
                request,
                cli,
                concurrency,
                &resp_tx,
            );
            if let Err(err) = res {
                if matches!(
                    errors::cause(err.as_ref()).downcast_ref::<tidb_gocontext::CtxError>(),
                    Some(tidb_gocontext::CtxError::Canceled)
                ) {
                    tidb_log::info(
                        "store backup cancelled",
                        &[
                            Field::new("round", Value::U64(round)),
                            Field::new("storeID", Value::U64(store_id)),
                        ],
                    );
                } else {
                    tidb_log::error(
                        "store backup failed",
                        &[
                            Field::new("round", Value::U64(round)),
                            Field::new("storeID", Value::U64(store_id)),
                            Field::new("error", Value::Str(err.to_string())),
                        ],
                    );
                    select! {
                        recv(ctx.done()) -> _ => {}
                        send(state_notifier, BackupRetryPolicy { one: store_id, all: false }) -> _ => {}
                    }
                }
            }
            tidb_log::debug(
                "store backup goroutine exits",
                &[Field::new("store", Value::U64(store_id))],
            );
            drop(resp_tx);
        });
    }
}

/// Go `TimeoutOneResponse`.
pub static TIMEOUT_ONE_RESPONSE: Mutex<Duration> = Mutex::new(Duration::from_secs(3600));

/// Go `timeoutRecv`: cancels the store's backup when no response arrives
/// within the timeout.
struct TimeoutRecv {
    refresh: Option<Sender<()>>,
    worker: Option<std::thread::JoinHandle<()>>,
    cancel: tidb_gocontext::CancelCauseFunc,
}

impl TimeoutRecv {
    /// Go `Refresh`.
    fn refresh(&self) {
        if let Some(tx) = &self.refresh {
            let _ = tx.try_send(());
        }
    }

    /// Go `Stop`.
    fn stop(&mut self) {
        self.refresh.take();
        if let Some(w) = self.worker.take() {
            let _ = w.join();
        }
        self.cancel.cancel(None);
    }
}

/// Go `StartTimeoutRecv`.
fn start_timeout_recv(ctx: &Context, timeout: Duration, store_id: u64) -> (Context, TimeoutRecv) {
    let (cctx, cancel) = Context::with_cancel_cause(ctx);
    let (tx, rx) = crossbeam_channel::bounded::<()>(0);
    let parent = ctx.clone();
    let cancel_w = cancel.clone();
    let worker = std::thread::spawn(move || loop {
        select! {
            recv(parent.done()) -> _ => return,
            recv(rx) -> r => if r.is_err() { return },
            default(timeout) => {
                tidb_log::warn(
                    "wait backup response timeout, cancel the backup",
                    &[Field::new("timeout", Value::Duration(timeout.as_nanos() as i64)), Field::new("storeID", Value::U64(store_id))],
                );
                cancel_w.cancel(Some(errors::errorf("receive a backup response timeout")));
            }
        }
    });
    (
        cctx,
        TimeoutRecv {
            refresh: Some(tx),
            worker: Some(worker),
            cancel,
        },
    )
}

/// The error of a done context (its cause when it has one).
fn ctx_error(ctx: &Context) -> errors::Error {
    ctx.cause().map_or_else(
        || Arc::new(ctx.err().unwrap_or(tidb_gocontext::CtxError::Canceled)) as errors::Error,
        |c| c,
    )
}

/// Go `doSendBackup`: one streaming `Backup` RPC, each response to
/// `resp_fn`.
fn do_send_backup(
    ctx: &Context,
    runtime: &tokio::runtime::Runtime,
    client: &BackupClient,
    limiter: &ResourceConcurrentLimiter,
    req: &BackupRequest,
    resp_fn: &mut dyn FnMut(BackupResponse) -> Result<()>,
) -> Result<()> {
    tidb_failpoint::inject(
        "github.com/pingcap/tidb/br/pkg/backup/hint-backup-start",
        |v| {
            tidb_log::info(
                "failpoint hint-backup-start injected, process will notify the shell.",
                &[],
            );
            let file = v.string();
            if !file.is_empty() {
                let _ = std::fs::File::create(file);
            }
            std::thread::sleep(Duration::from_secs(3));
        },
    );
    let size = req.sub_ranges.len() as i64 + 1;
    limiter.acquire(size);
    let mut cli = client.clone();
    let request = req.clone();
    let res = runtime.block_on(async move { cli.backup(request).await });
    limiter.release(size);
    let mut stream =
        match tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/backup/reset-retryable-error")
            .map(|v| v.string())
        {
            Some(s) if s == "Unavailable" => {
                return Err(errors::grpc(tonic::Status::unavailable(
                    "Unavailable error",
                )))
            }
            Some(s) if s == "Internal" => {
                return Err(errors::grpc(tonic::Status::internal("Internal error")))
            }
            _ => {
                if tidb_failpoint::eval(
                    "github.com/pingcap/tidb/br/pkg/backup/reset-not-retryable-error",
                )
                .is_some_and(|v| v.bool())
                {
                    return Err(errors::grpc(tonic::Status::unknown(
                        "Your server was haunted hence doesn't work, meow :3",
                    )));
                }
                res.map_err(errors::grpc)?.into_inner()
            }
        };
    loop {
        if ctx.err().is_some() {
            return Err(ctx_error(ctx));
        }
        let next = runtime.block_on(async {
            tokio::time::timeout(Duration::from_millis(200), stream.message()).await
        });
        match next {
            Err(_) => continue,
            Ok(Err(status)) => return Err(errors::grpc(status)),
            Ok(Ok(None)) => {
                tidb_log::debug("backup streaming finish", &[]);
                return Ok(());
            }
            Ok(Ok(Some(resp))) => {
                tidb_log::debug(
                    "range backed up",
                    &[Field::new(
                        "api-version",
                        Value::I64(i64::from(resp.api_version)),
                    )],
                );
                resp_fn(resp)?;
            }
        }
    }
}

/// A response error a failpoint injects.
fn inject_response_error(resp: &mut BackupResponse) {
    for name in [
        "backup-timeout-error",
        "backup-storage-error",
        "tikv-rw-error",
    ] {
        if let Some(v) =
            tidb_failpoint::eval(&format!("github.com/pingcap/tidb/br/pkg/backup/{name}"))
        {
            resp.error = Some(backup::Error {
                msg: v.string(),
                detail: None,
            });
        }
    }
    if let Some(v) = tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/backup/tikv-region-error")
    {
        resp.error = Some(backup::Error {
            msg: String::new(),
            detail: Some(backup::error::Detail::RegionError(
                tidb_proto::errorpb::Error {
                    message: v.string(),
                    ..Default::default()
                },
            )),
        });
    }
}

/// Go `startBackup`: the request split across `concurrency` workers, each
/// retried with the backup SST strategy; the first failure stops the rest.
#[allow(clippy::too_many_arguments)]
pub(crate) fn start_backup(
    pctx: &Context,
    runtime: &tokio::runtime::Runtime,
    store_id: u64,
    limiter: &ResourceConcurrentLimiter,
    backup_req: BackupRequest,
    backup_cli: BackupClient,
    concurrency: usize,
    resp_tx: &Sender<ResponseAndStore>,
) -> Result<()> {
    if pctx.err().is_some() {
        return Err(ctx_error(pctx));
    }
    let reqs = split_backup_req_ranges(backup_req, concurrency);
    tidb_log::info(
        "starting backup to the corresponding store",
        &[
            Field::new("storeID", Value::U64(store_id)),
            Field::new("requestCount", Value::I64(reqs.len() as i64)),
            Field::new("concurrency", Value::U64(concurrency as u64)),
        ],
    );
    let timeout = *TIMEOUT_ONE_RESPONSE.lock().unwrap();
    let (ctx, mut timerecv) = start_timeout_recv(pctx, timeout, store_id);
    let (ectx, ecancel) = Context::with_cancel(&ctx);
    let queue = Mutex::new(reqs.into_iter().enumerate().collect::<Vec<_>>());
    let first: Mutex<Option<errors::Error>> = Mutex::new(None);
    std::thread::scope(|scope| {
        for _ in 0..concurrency.max(1) {
            scope.spawn(|| loop {
                let Some((req_index, bk_req)) = queue.lock().unwrap().pop() else { return };
                if ectx.err().is_some() {
                    return;
                }
                let mut retry: i64 = -1;
                let res = with_retry(
                    &ectx,
                    || {
                        retry += 1;
                        if retry > 1 {
                            tidb_log::info(
                                "retry backup to store",
                                &[
                                    Field::new("storeID", Value::U64(store_id)),
                                    Field::new("retry", Value::I64(retry)),
                                    Field::new("reqIndex", Value::I64(req_index as i64)),
                                ],
                            );
                        }
                        do_send_backup(&ectx, runtime, &backup_cli, limiter, &bk_req, &mut |mut resp| {
                            inject_response_error(&mut resp);
                            select! {
                                recv(ectx.done()) -> _ => Err(ctx_error(&ectx)),
                                send(resp_tx, ResponseAndStore { resp, store_id }) -> r => {
                                    r.map_err(|_| errors::errorf("backup response channel closed"))?;
                                    timerecv.refresh();
                                    Ok(())
                                }
                            }
                        })
                    },
                    &mut new_backup_sst_backoff_strategy(),
                );
                if let Err(err) = res {
                    first.lock().unwrap().get_or_insert(err);
                    ecancel.cancel();
                    return;
                }
            });
        }
    });
    timerecv.stop();
    first.into_inner().unwrap().map_or(Ok(()), Err)
}

/// Go `ObserveStoreChangesAsync`: every 30 seconds, tells the backup loop
/// to restart every store after a store restarted or disconnected, or to
/// start a newly registered store.
pub fn observe_store_changes_async(
    ctx: &Context,
    notifier: Sender<BackupRetryPolicy>,
    pd: Arc<dyn PdClient>,
) {
    let ctx = ctx.clone();
    std::thread::spawn(move || {
        let send_all = std::cell::Cell::new(false);
        let new_join: std::cell::RefCell<std::collections::BTreeSet<u64>> = Default::default();
        let cb = Callback {
            on_new_store_registered: Some(Box::new(|s: &tidb_proto::metapb::Store| {
                new_join.borrow_mut().insert(s.id);
            })),
            on_disconnect: Some(Box::new(|_| send_all.set(true))),
            on_reboot: Some(Box::new(|_| send_all.set(true))),
        };
        let mut watcher = Watcher::new(pd.as_ref(), cb);
        if let Err(err) = watcher.step(&ctx) {
            tidb_log::warn(
                "failed to watch store changes at beginning, ignore it",
                &[Field::new("error", Value::Str(err.to_string()))],
            );
        }
        let mut interval = Duration::from_secs(30);
        if let Some(v) =
            tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/backup/backup-store-change-tick")
        {
            if v.bool() {
                interval = Duration::from_millis(100);
            }
            tidb_log::info(
                "failpoint backup-store-change-tick injected.",
                &[Field::new(
                    "interval",
                    Value::Duration(interval.as_nanos() as i64),
                )],
            );
        }
        let tick = crossbeam_channel::tick(interval);
        let notify = |p: BackupRetryPolicy| {
            select! {
                recv(ctx.done()) -> _ => {}
                send(notifier, p) -> _ => {}
            }
        };
        loop {
            select! {
                recv(ctx.done()) -> _ => return,
                recv(tick) -> _ => {
                    send_all.set(false);
                    new_join.borrow_mut().clear();
                    tidb_log::debug("check store changes", &[Field::new("interval", Value::Duration(interval.as_nanos() as i64))]);
                    if let Err(err) = watcher.step(&ctx) {
                        tidb_log::warn("failed to watch store changes, ignore it", &[Field::new("error", Value::Str(err.to_string()))]);
                    }
                    if send_all.get() {
                        tidb_log::info("detect some store(s) restarted or disconnected, notify with all stores", &[]);
                        notify(BackupRetryPolicy { one: 0, all: true });
                    } else {
                        let ids: Vec<u64> = new_join.borrow().iter().copied().collect();
                        for store_id in ids {
                            tidb_log::info("detect a new registered store, notify with this store", &[Field::new("storeID", Value::U64(store_id))]);
                            notify(BackupRetryPolicy { one: store_id, all: false });
                        }
                    }
                }
            }
        }
    });
}

/// Go `SplitBackupReqRanges`: the sub-ranges in `count` nearly equal
/// requests, earlier ones taking the remainder.
#[must_use]
pub fn split_backup_req_ranges(req: BackupRequest, count: usize) -> Vec<BackupRequest> {
    let range_count = req.sub_ranges.len();
    if range_count == 0 || count <= 1 {
        return vec![req];
    }
    let step = range_count / count;
    let over = range_count - count * step;
    let mut out = Vec::with_capacity(count);
    let mut start = 0;
    for i in 0..count {
        let mut next = start + step;
        if i < over {
            next += 1;
        } else if next == start {
            break;
        }
        let mut split = req.clone();
        split.sub_ranges = req.sub_ranges[start..next].to_vec();
        out.push(split);
        start = next;
    }
    out
}
