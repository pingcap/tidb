// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `conflictedkv/deleter.go`: deletes the KVs of conflicted rows from
//! the cluster.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_dxf_execute::Collector;
use tidb_executor::importer::kv_encode::TableKVEncoder;
use tidb_gocontext::Context;
use tidb_lightning_backend_kv::Pairs;
use tidb_table::tables::TableCommon;
use tidb_txnkv::kv_storage::Storage;
use tidb_txnkv::transaction::{BufferMutation, OptimisticCommitOutcome, TransactionCause};

use crate::handler::{is_retryable, store_call, store_error, BaseHandler, ConflictKVHandler, EncodedRowHandler, StoreError};
use crate::handler::{LazyRefreshedSnapshot, PairReceiver, TrafficRecorder};
use crate::{other, Result};

/// Go `storeOpMinBackoff`, `storeOpMaxBackoff` and `storeOpMaxRetryCnt`:
/// (1+2+4+8)*0.1s + (10-4)*1s = 7.5s.
const STORE_OP_MIN_BACKOFF: i64 = 100_000_000;
const STORE_OP_MAX_BACKOFF: i64 = 1_000_000_000;
const STORE_OP_MAX_RETRY_CNT: usize = 10;

/// Go `BufferedKeySizeLimit`: the key bytes buffered before a delete,
/// within client-go's big transaction limits. A variable for tests.
pub static BUFFERED_KEY_SIZE_LIMIT: AtomicUsize = AtomicUsize::new(2 << 20);
/// Go `BufferedKeyCountLimit`.
pub static BUFFERED_KEY_COUNT_LIMIT: AtomicUsize = AtomicUsize::new(9600);

/// Go `Deleter`'s `EncodedRowHandler` part: gathers the existing keys of
/// each row and sends them to the delete loop in batches.
struct DeleteSink {
    keys_tx: Option<SyncSender<Vec<Vec<u8>>>>,
    snapshot: LazyRefreshedSnapshot,
    buffered_keys: Vec<Vec<u8>>,
    buf_size: usize,
}

/// Go `Deleter`.
pub struct Deleter {
    handler: ConflictKVHandler<DeleteSink>,
    store: Arc<dyn Storage>,
    traffic_rec: Option<Arc<dyn TrafficRecorder>>,
}

impl Deleter {
    /// Go `NewDeleter`.
    #[must_use]
    pub fn new(
        target_tbl: TableCommon,
        store: Arc<dyn Storage>,
        keyspace: &[u8],
        kv_group: &str,
        encoder: TableKVEncoder,
        progress_collector: Option<Arc<dyn Collector>>,
        traffic_rec: Option<Arc<dyn TrafficRecorder>>,
    ) -> Self {
        let sink = DeleteSink {
            keys_tx: None,
            snapshot: LazyRefreshedSnapshot::new(Arc::clone(&store), traffic_rec.clone()),
            buffered_keys: Vec::new(),
            buf_size: 0,
        };
        // Go `store.GetCodec()`.
        let base = BaseHandler::new(target_tbl, kv_group, keyspace, Some(encoder), progress_collector);
        let handler = ConflictKVHandler::for_kv_group(base, &store, traffic_rec.clone(), None, sink);
        Self { handler, store, traffic_rec }
    }

    /// Go `Run`: the handler on this thread, the deletes on another.
    ///
    /// # Errors
    /// A KV could not be handled or a delete failed.
    pub fn run(mut self, ctx: &Context, rx: &PairReceiver) -> Result<()> {
        let (ctx, cancel) = Context::with_cancel(ctx);
        let (tx, keys_rx) = sync_channel::<Vec<Vec<u8>>>(0);
        self.handler.row_handler.keys_tx = Some(tx);
        let store = Arc::clone(&self.store);
        let traffic = self.traffic_rec.clone();
        std::thread::scope(|scope| {
            let delete_loop = scope.spawn(|| {
                let r = delete_loop(&ctx, store.as_ref(), traffic.as_deref(), &keys_rx);
                if r.is_err() {
                    cancel.cancel();
                }
                // The handler's next send fails once the loop is gone.
                drop(keys_rx);
                r
            });
            let mut result = self.handler.pre_run().and_then(|()| self.handler.run(&ctx, rx));
            let close = self.handler.close(&ctx);
            result = result.and(close);
            let send = self.handler.row_handler.send_keys_to_delete(&ctx);
            result = result.and(send);
            self.handler.row_handler.keys_tx = None;
            if result.is_err() {
                cancel.cancel();
            }
            let loop_result = delete_loop.join().unwrap_or_else(|_| Err(other("delete loop panicked")));
            // The loop's error is the cause of a failed send.
            loop_result.and(result)
        })
    }
}

/// Go `deleteLoop`.
fn delete_loop(
    ctx: &Context,
    store: &dyn Storage,
    traffic: Option<&dyn TrafficRecorder>,
    keys_rx: &Receiver<Vec<Vec<u8>>>,
) -> Result<()> {
    while let Ok(keys) = keys_rx.recv() {
        delete_keys_with_retry(ctx, store, traffic, &keys)?;
    }
    Ok(())
}

fn retry<F: FnMut() -> Result<()>>(ctx: &Context, mut f: F, retryable: fn(&crate::Error) -> bool) -> Result<()> {
    let mut backoffer = tidb_backoff::new_exponential(STORE_OP_MIN_BACKOFF, 2.0, STORE_OP_MAX_BACKOFF);
    tidb_dxf_handle::run_with_retry(ctx, STORE_OP_MAX_RETRY_CNT, &mut backoffer, &tidb_logutil::bg_logger(), |_| {
        match f() {
            Ok(()) => (true, Ok(())),
            Err(e) => (retryable(&e), Err(e)),
        }
    })
    .map_err(|e| match e {
        tidb_dxf_handle::RetryError::Func(e) => e,
        e => other(e.to_string()),
    })
}

/// Go `deleteKeysWithRetry`. The KVs of one row are deleted by one
/// deleter, but a write conflict is retried anyway, as in Go.
fn delete_keys_with_retry(ctx: &Context, store: &dyn Storage, traffic: Option<&dyn TrafficRecorder>, keys: &[Vec<u8>]) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    retry(ctx, || delete_buffered_keys(store, traffic, keys), is_retryable)
}

/// Go `deleteBufferedKeys`: the keys deleted in one transaction.
fn delete_buffered_keys(store: &dyn Storage, traffic: Option<&dyn TrafficRecorder>, keys: &[Vec<u8>]) -> Result<()> {
    if let Some(rec) = traffic {
        rec.inc_cluster_write_bytes(keys.iter().map(|k| k.len() as u64).sum());
    }
    let txn = store.begin().map_err(store_error)?;
    let mutations = keys
        .iter()
        .map(|k| BufferMutation::delete(k.clone()).map_err(|e| other(format!("{e:?}"))))
        .collect::<Result<Vec<_>>>()?;
    match txn.commit(mutations, &store_call()).map_err(store_error)? {
        OptimisticCommitOutcome::Committed(_) => Ok(()),
        OptimisticCommitOutcome::RolledBack(r) => Err(Arc::new(StoreError {
            message: format!("{:?}", r.cause),
            retryable: retryable_cause(&r.cause),
        })),
        outcome => Err(Arc::new(StoreError { message: format!("{outcome:?}"), retryable: false })),
    }
}

/// Go `IsTxnRetryableError || common.IsRetryableError` of a rolled-back
/// commit: a write conflict, a lock, a region or transport failure.
fn retryable_cause(cause: &TransactionCause) -> bool {
    matches!(
        cause,
        TransactionCause::WriteConflict { .. }
            | TransactionCause::Lock { .. }
            | TransactionCause::Region { .. }
            | TransactionCause::Transport { .. }
            | TransactionCause::BackoffExhausted { .. }
            | TransactionCause::Timestamp { .. }
    )
}

impl EncodedRowHandler for DeleteSink {
    /// Go `HandleEncodedRow`.
    fn handle_encoded_row(&mut self, ctx: &Context, _: &[u8], _: &[Datum], pairs: &Pairs) -> Result<()> {
        self.gather_and_delete_keys_with_retry(ctx, pairs)
    }
}

impl DeleteSink {
    /// Go `gatherAndDeleteKeysWithRetry`.
    fn gather_and_delete_keys_with_retry(&mut self, ctx: &Context, pairs: &Pairs) -> Result<()> {
        let keys: Vec<Vec<u8>> = pairs.pairs.iter().map(|p| p.key.clone()).collect();
        let mut gathered = Vec::new();
        retry(
            ctx,
            || {
                gathered = self.snapshot.batch_get(&keys)?;
                Ok(())
            },
            is_retryable,
        )?;
        // Go `gatherKeysToDelete`: only the keys that exist. The KVs of a
        // data KV group or a plain unique index never overlap between two
        // conflicted KVs, and an MV index's KVs of one row go to one
        // deleter, so a stale snapshot is fine.
        for (k, _) in gathered {
            self.buf_size += k.len();
            self.buffered_keys.push(k);
        }
        if self.buf_size >= BUFFERED_KEY_SIZE_LIMIT.load(Ordering::Relaxed)
            || self.buffered_keys.len() >= BUFFERED_KEY_COUNT_LIMIT.load(Ordering::Relaxed)
        {
            return self.send_keys_to_delete(ctx);
        }
        Ok(())
    }

    /// Go `sendKeysToDelete`.
    fn send_keys_to_delete(&mut self, ctx: &Context) -> Result<()> {
        if self.buffered_keys.is_empty() {
            return Ok(());
        }
        if let Some(e) = ctx.err() {
            return Err(other(e.to_string()));
        }
        let keys = std::mem::take(&mut self.buffered_keys);
        self.buf_size = 0;
        let tx = self.keys_tx.as_ref().ok_or_else(|| other("deleter is not running"))?;
        tx.send(keys).map_err(|_| other(ctx.err().map_or_else(|| "delete loop exited".to_owned(), |e| e.to_string())))
    }
}
