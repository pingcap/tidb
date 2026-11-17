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

//! Go `br/pkg/checkpoint`: the checkpoint runner that records finished
//! ranges and table checksums so an interrupted task can resume.
//!
//! This holds `checkpoint.go` (the runner, its flush loop and the
//! checkpoint file codec), `external_storage.go` (checkpoint files and the
//! lock in the backup storage), `ticker.go`, and `backup.go`. The restore
//! side (`restore.go`, `storage.go`, `manager.go`) comes with the restore
//! client; `log_restore.go` is PiTR only.
//!
//! Go writes these files with `encoding/json`; [`gojson`] writes the same
//! bytes, field order included.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel::{bounded, select, Receiver, Sender};
use sha2::{Digest, Sha256};
use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_proto::backup::CipherInfo;

use crate::errors::{self, Result};
use crate::metautil::Storage;

pub mod backup;
pub mod gojson;
pub mod manager;
pub mod restore;
pub mod storage;

/// Go `CheckpointDir`.
pub const CHECKPOINT_DIR: &str = "checkpoints";
/// Go `MaxChecksumTotalCost`.
pub const MAX_CHECKSUM_TOTAL_COST: f64 = 60.0;
/// Go `defaultTickDurationForFlush`.
pub(crate) const DEFAULT_TICK_DURATION_FOR_FLUSH: Duration = Duration::from_secs(30);
/// Go `defaultTickDurationForChecksum`.
pub(crate) const DEFAULT_TICK_DURATION_FOR_CHECKSUM: Duration = Duration::from_secs(5);
/// Go `defaultTickDurationForLock`.
const DEFAULT_TICK_DURATION_FOR_LOCK: Duration = Duration::from_secs(4 * 60);
/// Go `defaultRetryDuration`.
pub(crate) const DEFAULT_RETRY_DURATION: Duration = Duration::from_secs(3);
/// Go `lockTimeToLive`.
const LOCK_TIME_TO_LIVE: Duration = Duration::from_secs(5 * 60);

/// Go `flushPath`.
#[derive(Clone, Copy, Debug)]
pub struct FlushPath {
    /// Go `CheckpointDataDir`.
    pub checkpoint_data_dir: &'static str,
    /// Go `CheckpointChecksumDir`.
    pub checkpoint_checksum_dir: &'static str,
    /// Go `CheckpointLockPath`.
    pub checkpoint_lock_path: &'static str,
}

/// Go `RangeGroup`: one group key's checkpointed values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeGroup<K, V> {
    /// Go `GroupKey`.
    pub group_key: K,
    /// Go `Group`.
    pub group: Vec<V>,
}

/// Go `CheckpointMessage`.
#[derive(Clone, Debug)]
pub struct CheckpointMessage<K, V> {
    /// Go `GroupKey`.
    pub group_key: K,
    /// Go `Group`.
    pub group: Vec<V>,
}

/// Go `ChecksumItem`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ChecksumItem {
    /// Go `TableID`.
    pub table_id: i64,
    /// Go `Crc64xor`.
    pub crc64xor: u64,
    /// Go `TotalKvs`.
    pub total_kvs: u64,
    /// Go `TotalBytes`.
    pub total_bytes: u64,
}

/// Go `GlobalTimer`: PD's TSO.
pub trait GlobalTimer: Send + Sync {
    /// Go `GetTS`.
    ///
    /// # Errors
    /// PD's error.
    fn get_ts(&self, ctx: &Context) -> Result<(i64, i64)>;
}

impl<T: crate::pdclient::PdClient + ?Sized> GlobalTimer for T {
    fn get_ts(&self, ctx: &Context) -> Result<(i64, i64)> {
        crate::pdclient::PdClient::get_ts(self, ctx)
    }
}

/// Go `checkpointStorage`.
pub trait CheckpointStorage: Send + Sync {
    /// Go `flushCheckpointData`.
    ///
    /// # Errors
    /// The write error.
    fn flush_checkpoint_data(&self, ctx: &Context, data: &[u8]) -> Result<()>;
    /// Go `flushCheckpointChecksum`.
    ///
    /// # Errors
    /// The write error.
    fn flush_checkpoint_checksum(&self, ctx: &Context, data: &[u8]) -> Result<()>;
    /// Go `updateLock`.
    ///
    /// # Errors
    /// The lock is held by another task, or a storage error.
    fn update_lock(&self, ctx: &Context) -> Result<()>;
    /// Go `close`.
    fn close(&self);
}

/// Go `valueMarshaler`.
pub type ValueMarshaler<K, V> = Arc<dyn Fn(&RangeGroup<K, V>) -> Result<Vec<u8>> + Send + Sync>;

/// The runner's shared error (Go `err` / `errCh`).
struct RunnerError {
    sent: Mutex<Option<errors::Error>>,
    tx: Sender<errors::Error>,
    rx: Receiver<errors::Error>,
}

/// Parses one flushed range group.
pub type GroupParser<K, V> = dyn Fn(&[u8]) -> Result<RangeGroup<K, V>>;

/// Go `CheckpointRunner`.
pub struct CheckpointRunner<K, V> {
    append_tx: Sender<CheckpointMessage<K, V>>,
    checksum_tx: Sender<ChecksumItem>,
    done_tx: Sender<bool>,
    err: Arc<RunnerError>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
    storage: Arc<dyn CheckpointStorage>,
}

/// The state the main loop owns.
struct Loop<K, V> {
    meta: HashMap<K, RangeGroup<K, V>>,
    checksum: Vec<ChecksumItem>,
    marshaler: ValueMarshaler<K, V>,
    storage: Arc<dyn CheckpointStorage>,
    cipher: Option<CipherInfo>,
}

/// One message to the flush worker.
enum FlushMsg<K, V> {
    Meta(HashMap<K, RangeGroup<K, V>>),
    Checksum(Vec<ChecksumItem>),
    Lock,
}

/// Go `ChecksumItems` JSON.
fn checksum_items_json(items: &[ChecksumItem]) -> Vec<u8> {
    let mut out = String::from(r#"{"checksum-items":"#);
    if items.is_empty() {
        out.push_str("[]");
    } else {
        out.push('[');
        for (i, c) in items.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&format!(
                r#"{{"table-id":{},"crc64-xor":{},"total-kvs":{},"total-bytes":{}}}"#,
                c.table_id, c.crc64xor, c.total_kvs, c.total_bytes
            ));
        }
        out.push(']');
    }
    out.push('}');
    out.into_bytes()
}

impl<K, V> Loop<K, V>
where
    K: Clone + Eq + Hash + Send + 'static,
    V: Clone + Send + 'static,
{
    /// Go `doChecksumFlush`.
    fn do_checksum_flush(&self, ctx: &Context, items: &[ChecksumItem]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let content = checksum_items_json(items);
        let checksum = Sha256::digest(&content).to_vec();
        let data = format!(
            r#"{{"content":{},"checksum":{},"dure-time":{}}}"#,
            gojson::bytes(&content),
            gojson::bytes(&checksum),
            crate::summary::now_dure_time().as_nanos()
        );
        self.storage
            .flush_checkpoint_checksum(ctx, data.as_bytes())?;
        if tidb_failpoint::eval(
            "github.com/pingcap/tidb/br/pkg/checkpoint/failed-after-checkpoint-flushes-checksum",
        )
        .is_some()
        {
            return Err(errors::errorf(
                "failpoint: failed after checkpoint flushes checksum",
            ));
        }
        Ok(())
    }

    /// Go `doFlush`: each group's values, encrypted, with their checksum.
    fn do_flush(&self, ctx: &Context, meta: &HashMap<K, RangeGroup<K, V>>) -> Result<()> {
        if meta.is_empty() {
            return Ok(());
        }
        let mut metas = Vec::with_capacity(meta.len());
        for group in meta.values() {
            if group.group.is_empty() {
                continue;
            }
            let content = (self.marshaler)(group)?;
            let (encrypted, iv) =
                crate::metautil::encrypt(&content, self.cipher.as_ref()).map_err(errors::errorf)?;
            let checksum = Sha256::digest(&content).to_vec();
            metas.push(format!(
                r#"{{"RangeGroupsEncriptedData":{},"Checksum":{},"CipherIv":{},"Size":{}}}"#,
                gojson::bytes(&encrypted),
                gojson::bytes(&checksum),
                gojson::opt_bytes(&iv),
                content.len()
            ));
        }
        if !metas.is_empty() {
            let data = format!(
                r#"{{"dure-time":{},"range-group-metas":[{}]}}"#,
                crate::summary::now_dure_time().as_nanos(),
                metas.join(",")
            );
            self.storage.flush_checkpoint_data(ctx, data.as_bytes())?;
        }
        if tidb_failpoint::eval(
            "github.com/pingcap/tidb/br/pkg/checkpoint/failed-after-checkpoint-flushes",
        )
        .is_some()
        {
            return Err(errors::errorf("failpoint: failed after checkpoint flushes"));
        }
        Ok(())
    }
}

/// Go `dispatcherTicker`: no ticks for a non-positive duration.
fn dispatcher_ticker(d: Duration) -> Receiver<std::time::Instant> {
    if d > Duration::ZERO {
        crossbeam_channel::tick(d)
    } else {
        crossbeam_channel::never()
    }
}

impl<K, V> CheckpointRunner<K, V>
where
    K: Clone + Eq + Hash + Send + 'static,
    V: Clone + Send + 'static,
{
    /// Go `newCheckpointRunner` then `startCheckpointMainLoop`.
    pub(crate) fn start(
        ctx: &Context,
        storage: Arc<dyn CheckpointStorage>,
        cipher: Option<CipherInfo>,
        marshaler: ValueMarshaler<K, V>,
        ticks: [Duration; 4],
    ) -> Self {
        let [mut flush, mut checksum, mut lock, retry] = ticks;
        if tidb_failpoint::eval(
            "github.com/pingcap/tidb/br/pkg/checkpoint/checkpoint-more-quickly-flush",
        )
        .is_some()
        {
            checksum = Duration::from_secs(1);
            flush = Duration::from_secs(3);
            if lock > Duration::ZERO {
                lock = Duration::from_secs(1);
            }
            tidb_log::info(
                "adjust the tick duration for flush or lock",
                &[
                    Field::new("flush", Value::Duration(flush.as_nanos() as i64)),
                    Field::new("checksum", Value::Duration(checksum.as_nanos() as i64)),
                    Field::new("lock", Value::Duration(lock.as_nanos() as i64)),
                ],
            );
        }
        let (append_tx, append_rx) = bounded::<CheckpointMessage<K, V>>(0);
        let (checksum_tx, checksum_rx) = bounded::<ChecksumItem>(0);
        let (done_tx, done_rx) = bounded::<bool>(1);
        let (err_tx, err_rx) = bounded(1);
        let err = Arc::new(RunnerError {
            sent: Mutex::new(None),
            tx: err_tx,
            rx: err_rx,
        });
        let mut state = Loop {
            meta: HashMap::new(),
            checksum: Vec::new(),
            marshaler,
            storage: Arc::clone(&storage),
            cipher,
        };
        let ctx = ctx.clone();
        let loop_err = Arc::clone(&err);
        let worker = std::thread::Builder::new()
            .name("br-checkpoint".into())
            .spawn(move || {
                let (cctx, cancel) = Context::with_cancel(&ctx);
                let (flush_tx, flush_rx) = bounded::<FlushMsg<K, V>>(0);
                let (ferr_tx, ferr_rx) = bounded::<errors::Error>(1);
                let flusher = {
                    let state = Loop {
                        meta: HashMap::new(),
                        checksum: Vec::new(),
                        marshaler: Arc::clone(&state.marshaler),
                        storage: Arc::clone(&state.storage),
                        cipher: state.cipher.clone(),
                    };
                    let cctx = cctx.clone();
                    std::thread::spawn(move || flush_loop(&cctx, &state, &flush_rx, &ferr_tx, retry))
                };
                let flush_tick = crossbeam_channel::tick(flush);
                let checksum_tick = crossbeam_channel::tick(checksum);
                let lock_tick = dispatcher_ticker(lock);
                let send = |msg: FlushMsg<K, V>| -> Result<()> {
                    select! {
                        recv(ctx.done()) -> _ => Err(Arc::new(ctx.err().unwrap_or(tidb_gocontext::CtxError::Canceled))),
                        recv(ferr_rx) -> e => Err(e.unwrap_or_else(|_| errors::errorf("checkpoint flush worker exited"))),
                        send(flush_tx, msg) -> _ => Ok(()),
                    }
                };
                loop {
                    select! {
                        recv(ctx.done()) -> _ => {
                            loop_err.send(Arc::new(ctx.err().unwrap_or(tidb_gocontext::CtxError::Canceled)));
                            break;
                        }
                        recv(lock_tick) -> _ => {
                            if let Err(e) = send(FlushMsg::Lock) { loop_err.send(e); break; }
                        }
                        recv(checksum_tick) -> _ => {
                            let items = std::mem::take(&mut state.checksum);
                            if let Err(e) = send(FlushMsg::Checksum(items)) { loop_err.send(e); break; }
                        }
                        recv(flush_tick) -> _ => {
                            let meta = std::mem::take(&mut state.meta);
                            if let Err(e) = send(FlushMsg::Meta(meta)) { loop_err.send(e); break; }
                        }
                        recv(append_rx) -> msg => {
                            let Ok(msg) = msg else { break };
                            state.meta
                                .entry(msg.group_key.clone())
                                .or_insert_with(|| RangeGroup { group_key: msg.group_key.clone(), group: Vec::new() })
                                .group
                                .extend(msg.group);
                        }
                        recv(checksum_rx) -> msg => {
                            if let Ok(msg) = msg { state.checksum.push(msg); }
                        }
                        recv(done_rx) -> flush => {
                            tidb_log::info("stop checkpoint runner", &[]);
                            if flush.unwrap_or(false) {
                                let meta = std::mem::take(&mut state.meta);
                                if let Err(e) = send(FlushMsg::Meta(meta)) {
                                    tidb_log::error("failed to flush checkpoint meta", &[Field::new("error", Value::Str(e.to_string()))]);
                                } else {
                                    let items = std::mem::take(&mut state.checksum);
                                    if let Err(e) = send(FlushMsg::Checksum(items)) {
                                        tidb_log::error("failed to flush checkpoint checksum", &[Field::new("error", Value::Str(e.to_string()))]);
                                    }
                                }
                            }
                            break;
                        }
                        recv(ferr_rx) -> e => {
                            if let Ok(e) = e { loop_err.send(e); }
                            break;
                        }
                    }
                }
                drop(flush_tx);
                let _ = flusher.join();
                cancel.cancel();
            })
            .expect("spawn the checkpoint runner");
        Self {
            append_tx,
            checksum_tx,
            done_tx,
            err,
            worker: Mutex::new(Some(worker)),
            storage,
        }
    }

    /// The runner's error after its loop exited (Go reading `r.err` once
    /// `errCh` is closed).
    fn sent_error(&self, what: &str) -> errors::Error {
        let err = self
            .err
            .sent
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| errors::errorf("checkpoint runner exited"));
        errors::annotate(err, what.to_owned())
    }

    /// Go `FlushChecksum`.
    ///
    /// # Errors
    /// `ctx`'s error or the runner's.
    pub fn flush_checksum(
        &self,
        ctx: &Context,
        table_id: i64,
        crc64xor: u64,
        total_kvs: u64,
        total_bytes: u64,
    ) -> Result<()> {
        self.flush_checksum_item(
            ctx,
            ChecksumItem {
                table_id,
                crc64xor,
                total_kvs,
                total_bytes,
            },
        )
    }

    /// Go `FlushChecksumItem`.
    ///
    /// # Errors
    /// `ctx`'s error or the runner's.
    pub fn flush_checksum_item(&self, ctx: &Context, item: ChecksumItem) -> Result<()> {
        select! {
            recv(ctx.done()) -> _ => Err(errors::annotate(
                Arc::new(ctx.err().unwrap_or(tidb_gocontext::CtxError::Canceled)),
                "failed to append checkpoint checksum item",
            )),
            recv(self.err.rx) -> e => Err(e.unwrap_or_else(|_| self.sent_error("[checkpoint] Checksum: failed to append checkpoint checksum item"))),
            send(self.checksum_tx, item) -> r => r.map_err(|_| self.sent_error("[checkpoint] Checksum: failed to append checkpoint checksum item")),
        }
    }

    /// Go `Append`.
    ///
    /// # Errors
    /// `ctx`'s error or the runner's.
    pub fn append(&self, ctx: &Context, message: CheckpointMessage<K, V>) -> Result<()> {
        select! {
            recv(ctx.done()) -> _ => Err(errors::annotate(
                Arc::new(ctx.err().unwrap_or(tidb_gocontext::CtxError::Canceled)),
                "failed to append checkpoint message",
            )),
            recv(self.err.rx) -> e => Err(e.unwrap_or_else(|_| self.sent_error("[checkpoint] Append: failed to append checkpoint message"))),
            send(self.append_tx, message) -> r => r.map_err(|_| self.sent_error("[checkpoint] Append: failed to append checkpoint message")),
        }
    }

    /// Go `WaitForFinish`: stops the runner, flushing what it holds first
    /// when `flush`.
    pub fn wait_for_finish(&self, _ctx: &Context, flush: bool) {
        if self.done_tx.try_send(flush).is_err() {
            tidb_log::warn(
                "not the first close the checkpoint runner",
                &[Field::new("category", Value::Str("checkpoint".into()))],
            );
        }
        if let Some(worker) = self.worker.lock().unwrap().take() {
            let _ = worker.join();
        }
        self.storage.close();
    }
}

impl RunnerError {
    /// Go `sendError`: keeps the first error and closes the channel.
    fn send(&self, err: errors::Error) {
        let mut sent = self.sent.lock().unwrap();
        if sent.is_some() {
            tidb_log::error(
                "errCh is blocked",
                &[Field::new("error", Value::Str(err.to_string()))],
            );
            return;
        }
        tidb_log::error(
            "send the error",
            &[
                Field::new("category", Value::Str("checkpoint".into())),
                Field::new("error", Value::Str(err.to_string())),
            ],
        );
        *sent = Some(err.clone());
        let _ = self.tx.try_send(err);
    }
}

/// Go `startCheckpointFlushLoop`'s worker, with Go's `flusher`: a failed
/// flush is kept and retried every `retry`, and retried once more when the
/// runner stops.
fn flush_loop<K, V>(
    ctx: &Context,
    state: &Loop<K, V>,
    rx: &Receiver<FlushMsg<K, V>>,
    err_tx: &Sender<errors::Error>,
    retry: Duration,
) where
    K: Clone + Eq + Hash + Send + 'static,
    V: Clone + Send + 'static,
{
    let mut incomplete_metas: Vec<HashMap<K, RangeGroup<K, V>>> = Vec::new();
    let mut incomplete_checksums: Vec<Vec<ChecksumItem>> = Vec::new();
    let retry_tick = crossbeam_channel::tick(retry);
    loop {
        select! {
            recv(ctx.done()) -> _ => {
                if let Some(e) = ctx.err() {
                    let _ = err_tx.try_send(Arc::new(e));
                }
                return;
            }
            recv(rx) -> msg => match msg {
                Err(_) => {
                    for meta in &incomplete_metas {
                        if let Err(e) = state.do_flush(ctx, meta) {
                            tidb_log::warn("failed to retry to flush checkpoint data", &[Field::new("error", Value::Str(e.to_string()))]);
                        }
                    }
                    for items in &incomplete_checksums {
                        if let Err(e) = state.do_checksum_flush(ctx, items) {
                            tidb_log::warn("failed to retry to flush checkpoint checksum", &[Field::new("error", Value::Str(e.to_string()))]);
                        }
                    }
                    tidb_log::info("stop checkpoint flush worker", &[]);
                    return;
                }
                Ok(FlushMsg::Meta(meta)) => {
                    if let Err(e) = state.do_flush(ctx, &meta) {
                        tidb_log::warn("failed to flush checkpoint data", &[Field::new("error", Value::Str(e.to_string()))]);
                        incomplete_metas.push(meta);
                    }
                }
                Ok(FlushMsg::Checksum(items)) => {
                    if let Err(e) = state.do_checksum_flush(ctx, &items) {
                        tidb_log::warn("failed to flush checkpoint checksum", &[Field::new("error", Value::Str(e.to_string()))]);
                        incomplete_checksums.push(items);
                    }
                }
                Ok(FlushMsg::Lock) => {
                    if let Err(e) = state.storage.update_lock(ctx) {
                        let _ = err_tx.try_send(errors::annotate(e, "failed to update checkpoint lock."));
                        return;
                    }
                }
            },
            recv(retry_tick) -> _ => {
                if let Some(meta) = incomplete_metas.last() {
                    if let Err(e) = state.do_flush(ctx, meta) {
                        tidb_log::warn("failed to retry to flush checkpoint data", &[Field::new("error", Value::Str(e.to_string()))]);
                        continue;
                    }
                    incomplete_metas.pop();
                } else if let Some(items) = incomplete_checksums.last() {
                    if let Err(e) = state.do_checksum_flush(ctx, items) {
                        tidb_log::warn("failed to retry to flush checkpoint checksum", &[Field::new("error", Value::Str(e.to_string()))]);
                        continue;
                    }
                    incomplete_checksums.pop();
                }
            }
        }
    }
}

/// Go `parseCheckpointData`: each group of a checkpoint file through `f`.
/// A file that does not parse, or a group whose checksum does not match, is
/// skipped as in Go.
pub(crate) fn parse_checkpoint_data<K, V>(
    content: &[u8],
    past_dure_time: &mut Duration,
    cipher: Option<&CipherInfo>,
    parse_group: &GroupParser<K, V>,
    f: &mut dyn FnMut(&K, V) -> Result<()>,
) -> Result<()> {
    let Ok(data) = serde_json::from_slice::<serde_json::Value>(content) else {
        tidb_log::error("failed to unmarshal the checkpoint data info, skip it", &[]);
        return Ok(());
    };
    let dure = Duration::from_nanos(data["dure-time"].as_u64().unwrap_or(0));
    if dure > *past_dure_time {
        *past_dure_time = dure;
    }
    for meta in data["range-group-metas"]
        .as_array()
        .cloned()
        .unwrap_or_default()
    {
        let field = |k: &str| gojson::parse_bytes(&meta[k]);
        let decrypted = crate::utils::decrypt(
            &field("RangeGroupsEncriptedData")?,
            cipher,
            &field("CipherIv")?,
        )
        .map_err(errors::errorf)?;
        if field("Checksum")? != Sha256::digest(&decrypted).to_vec() {
            tidb_log::error("checkpoint checksum info's checksum mismatch, skip it", &[]);
            continue;
        }
        let group = parse_group(&decrypted)?;
        for v in group.group {
            f(&group.group_key, v)?;
        }
    }
    Ok(())
}

/// Go `walkCheckpointFile`: every `.cpt` file under `sub_dir`; answers the
/// longest recorded run time.
///
/// # Errors
/// A storage, decrypt or `f` error.
pub fn walk_checkpoint_file<K, V>(
    ctx: &Context,
    s: &Storage,
    cipher: Option<&CipherInfo>,
    sub_dir: &str,
    parse_group: &GroupParser<K, V>,
    f: &mut dyn FnMut(&K, V) -> Result<()>,
) -> Result<Duration> {
    let mut past = Duration::ZERO;
    let mut paths = Vec::new();
    s.walk_dir(
        ctx,
        Some(&tidb_objstore::storeapi::WalkOption {
            sub_dir: sub_dir.into(),
            ..Default::default()
        }),
        &mut |path, _| {
            if path.ends_with(".cpt") {
                paths.push(path.to_owned());
            }
            Ok(())
        },
    )
    .map_err(|e| errors::errorf(e.to_string()))?;
    for path in paths {
        let content = s
            .read_file(ctx, &path)
            .map_err(|e| errors::errorf(e.to_string()))?;
        parse_checkpoint_data(&content, &mut past, cipher, parse_group, f)?;
    }
    Ok(past)
}

/// Go `parseCheckpointChecksum`: a file that does not parse or does not
/// match its checksum is skipped.
pub(crate) fn parse_checkpoint_checksum(
    data: &[u8],
    out: &mut HashMap<i64, ChecksumItem>,
    past: &mut Duration,
) -> Result<()> {
    let Ok(info) = serde_json::from_slice::<serde_json::Value>(data) else {
        tidb_log::error(
            "failed to unmarshal the checkpoint checksum info, skip it",
            &[],
        );
        return Ok(());
    };
    let content = gojson::parse_bytes(&info["content"])?;
    if gojson::parse_bytes(&info["checksum"])? != Sha256::digest(&content).to_vec() {
        tidb_log::error("checkpoint checksum info's checksum mismatch, skip it", &[]);
        return Ok(());
    }
    let dure = Duration::from_nanos(info["dure-time"].as_u64().unwrap_or(0));
    if dure > *past {
        *past = dure;
    }
    let items: serde_json::Value =
        serde_json::from_slice(&content).map_err(|e| errors::errorf(e.to_string()))?;
    for c in items["checksum-items"]
        .as_array()
        .cloned()
        .unwrap_or_default()
    {
        let item = ChecksumItem {
            table_id: c["table-id"].as_i64().unwrap_or(0),
            crc64xor: c["crc64-xor"].as_u64().unwrap_or(0),
            total_kvs: c["total-kvs"].as_u64().unwrap_or(0),
            total_bytes: c["total-bytes"].as_u64().unwrap_or(0),
        };
        out.insert(item.table_id, item);
    }
    Ok(())
}

/// Go `loadCheckpointChecksum`.
///
/// # Errors
/// A storage error.
pub fn load_checkpoint_checksum(
    ctx: &Context,
    s: &Storage,
    sub_dir: &str,
) -> Result<(HashMap<i64, ChecksumItem>, Duration)> {
    let mut paths = Vec::new();
    s.walk_dir(
        ctx,
        Some(&tidb_objstore::storeapi::WalkOption {
            sub_dir: sub_dir.into(),
            ..Default::default()
        }),
        &mut |path, _| {
            paths.push(path.to_owned());
            Ok(())
        },
    )
    .map_err(|e| errors::errorf(e.to_string()))?;
    let mut out = HashMap::new();
    let mut past = Duration::ZERO;
    for path in paths {
        let data = s
            .read_file(ctx, &path)
            .map_err(|e| errors::errorf(e.to_string()))?;
        parse_checkpoint_checksum(&data, &mut out, &mut past)?;
    }
    Ok((out, past))
}

/// Go `removeCheckpointData`: the checkpoint files under `sub_dir`, four at
/// a time; fails after 16 failed deletes.
///
/// # Errors
/// A storage error, or too many failed deletes.
pub fn remove_checkpoint_data(ctx: &Context, s: &Storage, sub_dir: &str) -> Result<()> {
    let mut names = Vec::new();
    let mut size = 0_i64;
    s.walk_dir(
        ctx,
        Some(&tidb_objstore::storeapi::WalkOption {
            sub_dir: sub_dir.into(),
            ..Default::default()
        }),
        &mut |path, sz| {
            if path.ends_with(".cpt") || path.ends_with(".meta") || path.ends_with(".lock") {
                names.push(path.to_owned());
                size += sz;
            }
            Ok(())
        },
    )
    .map_err(|e| errors::errorf(e.to_string()))?;
    tidb_log::info(
        "start to remove checkpoint data",
        &[
            Field::new("checkpoint task", Value::Str(sub_dir.into())),
            Field::new("remove-count", Value::I64(names.len() as i64)),
            Field::new("remove-size", Value::I64(size)),
        ],
    );
    let failed = std::sync::atomic::AtomicI64::new(0);
    let queue = Mutex::new(names);
    let first_err: Mutex<Option<errors::Error>> = Mutex::new(None);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| loop {
                if first_err.lock().unwrap().is_some() {
                    return;
                }
                let Some(name) = queue.lock().unwrap().pop() else {
                    return;
                };
                if let Err(e) = s.delete_file(ctx, &name) {
                    tidb_log::warn(
                        "failed to remove the file",
                        &[
                            Field::new("filename", Value::Str(name.clone())),
                            Field::new("error", Value::Str(e.to_string())),
                        ],
                    );
                    if failed.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 >= 16 {
                        first_err.lock().unwrap().get_or_insert_with(|| {
                            errors::annotate(
                                errors::errorf(e.to_string()),
                                "failed to delete too many files",
                            )
                        });
                        return;
                    }
                }
            });
        }
    });
    if let Some(e) = first_err.into_inner().unwrap() {
        return Err(e);
    }
    tidb_log::info(
        "all the checkpoint data has been removed",
        &[Field::new("checkpoint task", Value::Str(sub_dir.into()))],
    );
    Ok(())
}

/// Go `externalCheckpointStorage`: checkpoint files and the lock in the
/// task's own storage.
pub struct ExternalCheckpointStorage {
    flush_path: FlushPath,
    storage: Storage,
    lock_id: Mutex<u64>,
    timer: Option<Arc<dyn GlobalTimer>>,
}

/// Go `CheckpointLock`.
#[derive(serde::Serialize, serde::Deserialize)]
struct CheckpointLock {
    #[serde(rename = "lock-id")]
    lock_id: u64,
    #[serde(rename = "expire-at")]
    expire_at: i64,
}

impl ExternalCheckpointStorage {
    /// Go `newExternalCheckpointStorage`: takes the lock when there is a
    /// timer.
    ///
    /// # Errors
    /// The lock is held, or a storage or PD error.
    pub fn new(
        ctx: &Context,
        storage: Storage,
        timer: Option<Arc<dyn GlobalTimer>>,
        flush_path: FlushPath,
    ) -> Result<Self> {
        let s = Self {
            flush_path,
            storage,
            lock_id: Mutex::new(0),
            timer,
        };
        if s.timer.is_some() {
            s.initial_lock(ctx)?;
        }
        Ok(s)
    }

    fn write(&self, ctx: &Context, dir: &str, data: &[u8]) -> Result<()> {
        let name = format!("{dir}/{}.cpt", uuid::Uuid::new_v4().simple());
        self.storage
            .write_file(ctx, &name, data)
            .map_err(|e| errors::errorf(e.to_string()))
    }

    /// Go `getTS`, retried with the aggressive PD strategy.
    fn get_ts(&self, ctx: &Context) -> Result<(i64, i64)> {
        let timer = self.timer.as_ref().expect("a lock needs a timer");
        let mut ts = (0, 0);
        let mut retry = 0;
        crate::utils::retry::with_retry(
            ctx,
            || match timer.get_ts(ctx) {
                Ok(v) => {
                    ts = v;
                    Ok(())
                }
                Err(e) => {
                    retry += 1;
                    tidb_log::info(
                        "failed to get ts",
                        &[
                            Field::new("retry", Value::I64(retry)),
                            Field::new("error", Value::Str(e.to_string())),
                        ],
                    );
                    Err(e)
                }
            },
            &mut crate::utils::backoff::new_aggressive_pd_backoff_strategy(),
        )?;
        Ok(ts)
    }

    /// Go `flushLock`.
    fn flush_lock(&self, ctx: &Context, p: i64) -> Result<()> {
        let lock = CheckpointLock {
            lock_id: *self.lock_id.lock().unwrap(),
            expire_at: p + LOCK_TIME_TO_LIVE.as_millis() as i64,
        };
        tidb_log::debug(
            "start to flush the checkpoint lock",
            &[
                Field::new("lock-at", Value::I64(p)),
                Field::new("expire-at", Value::I64(lock.expire_at)),
            ],
        );
        let data = serde_json::to_vec(&lock).map_err(|e| errors::errorf(e.to_string()))?;
        self.storage
            .write_file(ctx, self.flush_path.checkpoint_lock_path, &data)
            .map_err(|e| errors::errorf(e.to_string()))
    }

    /// Go `checkLockFile`.
    fn check_lock_file(&self, ctx: &Context, now: i64) -> Result<()> {
        let data = self
            .storage
            .read_file(ctx, self.flush_path.checkpoint_lock_path)
            .map_err(|e| errors::errorf(e.to_string()))?;
        let lock: CheckpointLock =
            serde_json::from_slice(&data).map_err(|e| errors::errorf(e.to_string()))?;
        let lock_id = *self.lock_id.lock().unwrap();
        if lock.expire_at <= now {
            if lock.lock_id > lock_id {
                return Err(errors::errorf(format!(
                    "There are another BR({}) running after but setting lock before this one({lock_id}). Please check whether the BR is running. If not, you can retry.",
                    lock.lock_id
                )));
            }
            if lock.lock_id == lock_id {
                tidb_log::warn(
                    "The lock has expired.",
                    &[
                        Field::new("expire-at(ms)", Value::I64(lock.expire_at)),
                        Field::new("now(ms)", Value::I64(now)),
                    ],
                );
            }
        } else if lock.lock_id != lock_id {
            let uri = self.storage.uri();
            return Err(errors::errorf(format!(
                "The existing lock will expire in {} seconds. There may be another BR({}) running. If not, you can wait for the lock to expire, or delete the file `{}/{}` manually.",
                (lock.expire_at - now) / 1000,
                lock.lock_id,
                uri.trim_end_matches('/'),
                self.flush_path.checkpoint_lock_path.trim_start_matches('/')
            )));
        }
        Ok(())
    }

    /// Go `initialLock`: the lock ID is a fresh TSO; the lock is checked
    /// again 3 seconds after it is written.
    fn initial_lock(&self, ctx: &Context) -> Result<()> {
        let (p, l) = self.get_ts(ctx)?;
        *self.lock_id.lock().unwrap() = crate::pdclient::compose_ts(p, l);
        let exist = self
            .storage
            .file_exists(ctx, self.flush_path.checkpoint_lock_path)
            .map_err(|e| errors::errorf(e.to_string()))?;
        if exist {
            self.check_lock_file(ctx, p)?;
        }
        self.flush_lock(ctx, p)?;
        std::thread::sleep(Duration::from_secs(3));
        self.check_lock_file(ctx, p)
    }
}

impl CheckpointStorage for ExternalCheckpointStorage {
    fn flush_checkpoint_data(&self, ctx: &Context, data: &[u8]) -> Result<()> {
        self.write(ctx, self.flush_path.checkpoint_data_dir, data)
    }

    fn flush_checkpoint_checksum(&self, ctx: &Context, data: &[u8]) -> Result<()> {
        self.write(ctx, self.flush_path.checkpoint_checksum_dir, data)
    }

    /// Go `updateLock`.
    fn update_lock(&self, ctx: &Context) -> Result<()> {
        let (p, _) = self.get_ts(ctx)?;
        self.check_lock_file(ctx, p)?;
        self.flush_lock(ctx, p)?;
        if tidb_failpoint::eval(
            "github.com/pingcap/tidb/br/pkg/checkpoint/failed-after-checkpoint-updates-lock",
        )
        .is_some()
        {
            return Err(errors::errorf(
                "failpoint: failed after checkpoint updates lock",
            ));
        }
        Ok(())
    }

    fn close(&self) {}
}

/// The default tick durations: flush, checksum, lock, retry.
pub(crate) const DEFAULT_TICKS: [Duration; 4] = [
    DEFAULT_TICK_DURATION_FOR_FLUSH,
    DEFAULT_TICK_DURATION_FOR_CHECKSUM,
    DEFAULT_TICK_DURATION_FOR_LOCK,
    DEFAULT_RETRY_DURATION,
];

#[cfg(test)]
mod tests;
