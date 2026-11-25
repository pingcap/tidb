// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `conflictedkv/handler.go`: the handlers of one KV group's conflicted
//! KVs, which re-encode each conflicted row.

use std::borrow::Cow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tidb_datatype::Datum;
use tidb_dxf_execute::{Collector, NoopCollector};
use tidb_executor_importer::kv_encode::TableKVEncoder;
use tidb_gocontext::Context;
use tidb_ingestor_simplesst::KVPair;
use tidb_lightning_backend_kv::Pairs;
use tidb_table::tables::{TableCommon, TableHandle};
use tidb_txnkv::kv_storage::{Storage, Transaction};

use crate::row_handle::KeyFilter;
use crate::{other, table_handle, Result};

/// Go `snapshotRefreshInterval`.
const SNAPSHOT_REFRESH_INTERVAL: Duration = Duration::from_secs(15);

/// Go `BufferedHandleLimit`: the index handles buffered before their rows
/// are read. A variable for tests, like Go's.
pub static BUFFERED_HANDLE_LIMIT: AtomicUsize = AtomicUsize::new(256);

/// The deadline of one store call.
pub(crate) fn store_call() -> tidb_txnkv::UnaryCallContext {
    tidb_txnkv::UnaryCallContext::with_timeout(Duration::from_secs(60))
}

/// Go `TrafficRecorder`: the best-effort traffic between TiDB and TiKV, for
/// metering.
pub trait TrafficRecorder: Send + Sync {
    /// Go `IncClusterReadBytes`.
    fn inc_cluster_read_bytes(&self, bytes: u64);
    /// Go `IncClusterWriteBytes`.
    fn inc_cluster_write_bytes(&self, bytes: u64);
}

/// Go's `*metering.Recorder`, which the step executors pass as the
/// handlers' `TrafficRecorder`.
impl TrafficRecorder for tidb_dxf_metering::Recorder {
    fn inc_cluster_read_bytes(&self, bytes: u64) {
        tidb_dxf_metering::Recorder::inc_cluster_read_bytes(self, bytes);
    }

    fn inc_cluster_write_bytes(&self, bytes: u64) {
        tidb_dxf_metering::Recorder::inc_cluster_write_bytes(self, bytes);
    }
}

/// Go's `chan *simplesst.KVPair` that several handlers receive from.
pub type PairReceiver = crossbeam_channel::Receiver<KVPair>;

/// The next pair, or `None` once the sender is gone.
#[must_use]
pub fn recv(rx: &PairReceiver) -> Option<KVPair> {
    rx.recv().ok()
}

/// Go `EncodedRowHandler`: what is done with a re-encoded conflicted row.
pub trait EncodedRowHandler {
    /// Go `HandleEncodedRow`.
    ///
    /// # Errors
    /// The row could not be handled.
    fn handle_encoded_row(&mut self, ctx: &Context, row_key: &[u8], row: &[Datum], pairs: &Pairs) -> Result<()>;
}

/// Go `BaseHandler`.
pub struct BaseHandler {
    target_table: TableCommon,
    kv_group: String,
    /// Go `codec.GetKeyspace()` of the handler's `tikv.Codec`: the prefix
    /// the sorted files' keys carry, which `DecodeKey` removes.
    keyspace: Vec<u8>,
    encoder: Option<TableKVEncoder>,
    collector: Arc<dyn Collector>,
}

impl BaseHandler {
    /// Go `NewBaseHandler`, with the store's codec as its keyspace.
    #[must_use]
    pub fn new(
        target_table: TableCommon,
        kv_group: &str,
        keyspace: &[u8],
        encoder: Option<TableKVEncoder>,
        collector: Option<Arc<dyn Collector>>,
    ) -> Self {
        Self {
            target_table,
            kv_group: kv_group.to_owned(),
            keyspace: keyspace.to_vec(),
            encoder,
            collector: collector.unwrap_or_else(|| Arc::new(NoopCollector)),
        }
    }

    /// Go `tikv.Codec.DecodeKey`: a key of the keyspace without its prefix;
    /// without a keyspace (API V1) the key itself.
    fn decode_key(&self, key: &[u8]) -> Result<Vec<u8>> {
        if self.keyspace.is_empty() || key.is_empty() {
            return Ok(key.to_vec());
        }
        key.strip_prefix(self.keyspace.as_slice())
            .map(<[u8]>::to_vec)
            .ok_or_else(|| other("given key does not belong to the keyspace"))
    }

    /// Go `encodeAndHandleRow`: the row of a data KV re-encoded, then
    /// handled.
    fn encode_and_handle_row(
        &mut self,
        ctx: &Context,
        row_key: &[u8],
        handle: &TableHandle,
        val: Cow<'_, [u8]>,
        row_handler: &mut dyn EncodedRowHandler,
    ) -> Result<()> {
        let encoder = self.encoder.as_mut().ok_or_else(|| other("conflict KV handler has no encoder"))?;
        let offsets = self.target_table.visible_offsets();
        let context = encoder.base().session().context();
        let decoded = match val {
            Cow::Borrowed(value) => {
                self.target_table.decode_raw_row_data(context, handle, &offsets, value)
            }
            Cow::Owned(value) => {
                self.target_table.decode_owned_raw_row_data(context, handle, &offsets, value)
            }
        }
        .map_err(|e| other(e.into_mysql_error().to_string()))?;
        let auto_row_id = if self.target_table.meta().read().has_clustered_index() {
            0
        } else {
            handle.int_value().unwrap_or(0)
        };
        let pairs = encoder.encode(&decoded, auto_row_id).map_err(|e| other(e.to_string()))?;
        row_handler.handle_encoded_row(ctx, row_key, &decoded, &pairs)
    }
}

/// Go `IndexKVHandler`'s own fields.
pub(crate) struct IndexKV {
    snapshot: LazyRefreshedSnapshot,
    row_key_filter: Option<KeyFilter>,
    /// `len(targetIdx.Columns)`, set by `pre_run`.
    index_columns: usize,
    buffered_rows: Vec<(Vec<u8>, TableHandle)>,
}

/// Go `DataKVHandler` or `IndexKVHandler`.
pub(crate) enum KVHandlerKind {
    Data,
    Index(Box<IndexKV>),
}

/// Go's `BaseHandler` with its `KVHandler` (`DataKVHandler` or
/// `IndexKVHandler`) and `EncodedRowHandler` `R`.
pub struct ConflictKVHandler<R: EncodedRowHandler> {
    base: BaseHandler,
    kind: KVHandlerKind,
    /// The `EncodedRowHandler`.
    pub row_handler: R,
}

impl<R: EncodedRowHandler> ConflictKVHandler<R> {
    /// Go `NewDataKVHandler`.
    pub fn new_data(base: BaseHandler, row_handler: R) -> Self {
        Self { base, kind: KVHandlerKind::Data, row_handler }
    }

    /// Go `NewIndexKVHandler`.
    pub fn new_index(base: BaseHandler, snapshot: LazyRefreshedSnapshot, filter: Option<KeyFilter>, row_handler: R) -> Self {
        let kind = KVHandlerKind::Index(Box::new(IndexKV {
            snapshot,
            row_key_filter: filter,
            index_columns: 0,
            buffered_rows: Vec::new(),
        }));
        Self { base, kind, row_handler }
    }

    /// Go's handler for `kv_group`: the data one for the data KV group,
    /// else an index one with its own snapshot.
    pub fn for_kv_group(
        base: BaseHandler,
        store: &Arc<dyn Storage>,
        traffic: Option<Arc<dyn TrafficRecorder>>,
        filter: Option<KeyFilter>,
        row_handler: R,
    ) -> Self {
        if base.kv_group == tidb_ingestor_globalsort::DATA_KV_GROUP {
            Self::new_data(base, row_handler)
        } else {
            Self::new_index(base, LazyRefreshedSnapshot::new(Arc::clone(store), traffic), filter, row_handler)
        }
    }

    /// Go `PreRun`: an index handler finds its index.
    ///
    /// # Errors
    /// The KV group names no index of the table.
    pub fn pre_run(&mut self) -> Result<()> {
        let KVHandlerKind::Index(h) = &mut self.kind else { return Ok(()) };
        let index_id = tidb_ingestor_globalsort::kv_group_2_index_id(&self.base.kv_group).map_err(|e| other(e.to_string()))?;
        let meta = self.base.target_table.meta();
        let meta = meta.read();
        let Some(index) = meta.indices.iter_deref().find(|i| i.read().id == index_id) else {
            // should not happen
            return Err(other(format!("index {index_id} in table {}", meta.name.original())));
        };
        h.index_columns = index.read().columns.len();
        Ok(())
    }

    /// Go `Run`: every pair from `rx`, each reported as processed.
    ///
    /// # Errors
    /// A pair could not be handled, or `ctx` ended.
    pub fn run(&mut self, ctx: &Context, rx: &PairReceiver) -> Result<()> {
        while let Some(pair) = recv(rx) {
            if let Some(e) = ctx.err() {
                return Err(other(e.to_string()));
            }
            self.handle_pair(ctx, &pair.key, Cow::Owned(pair.value))?;
            self.base.collector.processed(1, 0);
        }
        Ok(())
    }

    /// Go `DataKVHandler.Handle` / `IndexKVHandler.Handle`.
    ///
    /// # Errors
    /// The key does not decode, or the row could not be handled.
    pub fn handle(&mut self, ctx: &Context, kv: &KVPair) -> Result<()> {
        self.handle_pair(ctx, &kv.key, Cow::Borrowed(&kv.value))
    }

    fn handle_pair(&mut self, ctx: &Context, key: &[u8], value: Cow<'_, [u8]>) -> Result<()> {
        // Go `h.codec.DecodeKey(kv.Key)`: the sorted files hold the store's
        // encoded keys.
        let key = self.base.decode_key(key)?;
        match &mut self.kind {
            KVHandlerKind::Data => {
                let handle = match tidb_tablecodec::table_key::decode_row_key(&key).map_err(|e| other(e.to_string()))? {
                    tidb_tablecodec::table_key::RecordHandle::Int(h) => TableHandle::Int(h),
                    tidb_tablecodec::table_key::RecordHandle::Common(h) => TableHandle::Common(h),
                    tidb_tablecodec::table_key::RecordHandle::Partition { .. } => {
                        return Err(other("partition handle in a row key"));
                    }
                };
                self.base.encode_and_handle_row(ctx, &key, &handle, value, &mut self.row_handler)
            }
            KVHandlerKind::Index(h) => {
                // The table ID of the key, for a partitioned table.
                let table_id = tidb_tablecodec::table_key::decode_table_id(&key);
                if table_id == 0 {
                    // should not happen
                    return Err(other(format!("invalid table ID in key {}", tidb_redact::key(&key))));
                }
                let handle = tidb_tablecodec::decode_index_handle(&key, &value, h.index_columns)
                    .map_err(|e| other(e.to_string()))?
                    .ok_or_else(|| other("no handle in index key or value"))?;
                // The filter and the snapshot need the data row key.
                let row_key = encode_data_row_key(table_id, &handle);
                let handle = table_handle(&handle);
                if h.row_key_filter.as_ref().is_some_and(|f| f.is_handled_globally(&row_key)) {
                    return Ok(());
                }
                h.buffered_rows.push((row_key, handle));
                if h.buffered_rows.len() >= BUFFERED_HANDLE_LIMIT.load(Ordering::Relaxed) {
                    return self.handle_buffered_handles(ctx);
                }
                Ok(())
            }
        }
    }

    /// Go `handleBufferedHandles`: the buffered rows that exist in the
    /// cluster, each handled once.
    fn handle_buffered_handles(&mut self, ctx: &Context) -> Result<()> {
        let KVHandlerKind::Index(h) = &mut self.kind else { return Ok(()) };
        if h.buffered_rows.is_empty() {
            return Ok(());
        }
        let keys: Vec<Vec<u8>> = h.buffered_rows.iter().map(|(k, _)| k.clone()).collect();
        let handles: std::collections::HashMap<Vec<u8>, TableHandle> = h.buffered_rows.iter().cloned().collect();
        let res = h.snapshot.batch_get(&keys)?;
        for (row_key, val) in res {
            // Two keys of an MV index can name the same row.
            if h.row_key_filter.as_ref().is_some_and(|f| f.is_handled_locally(&row_key)) {
                continue;
            }
            let Some(handle) = handles.get(&row_key) else { continue };
            // A failed row stays buffered, so `close` retries it.
            self.base.encode_and_handle_row(ctx, &row_key, handle, Cow::Owned(val), &mut self.row_handler)?;
            // A row of the data KV group is always recorded; an index KV
            // group's rows may repeat, so only the first one counts. The set is
            // in memory: past its bound the checksum is skipped.
            if let Some(f) = h.row_key_filter.as_mut() {
                f.add_local(&row_key);
            }
        }
        h.buffered_rows.clear();
        Ok(())
    }

    /// Go `IndexKVHandler.Close` / `BaseHandler.Close`: the buffered
    /// handles are handled, and the encoder is released.
    ///
    /// # Errors
    /// The buffered handles could not be handled.
    pub fn close(&mut self, ctx: &Context) -> Result<()> {
        let result = self.handle_buffered_handles(ctx);
        self.base.encoder = None;
        result
    }

    /// The handler's key filter, once it is done.
    pub fn take_key_filter(&mut self) -> Option<KeyFilter> {
        match &mut self.kind {
            KVHandlerKind::Index(h) => h.row_key_filter.take(),
            KVHandlerKind::Data => None,
        }
    }
}

/// Go `encodeDataRowKey`: a partition handle's key is under its partition,
/// like Go's `EncodeRecordKey`.
pub(crate) fn encode_data_row_key(table_id: i64, handle: &tidb_txnkv::Handle) -> Vec<u8> {
    let prefix = tidb_tablecodec::table_key::gen_table_record_prefix(table_id);
    tidb_tablecodec::table_key::encode_record_key(&prefix, &record_handle(handle))
}

fn record_handle(handle: &tidb_txnkv::Handle) -> tidb_tablecodec::table_key::RecordHandle {
    use tidb_tablecodec::table_key::RecordHandle;
    match handle {
        tidb_txnkv::Handle::Int(h) => RecordHandle::Int(h.value()),
        tidb_txnkv::Handle::Common(h) => RecordHandle::Common(h.encoded().to_vec()),
        tidb_txnkv::Handle::Partition(p) => {
            RecordHandle::Partition { partition_id: p.partition_id(), handle: Box::new(record_handle(p.inner())) }
        }
    }
}

/// Go `LazyRefreshedSnapshot`: a snapshot at a version refreshed at most
/// every 15 seconds, so it does not fall behind the GC safe point.
///
/// A stale version is fine: two conflicted KVs of one KV group whose rows
/// conflict with each other are both recorded, so a row found here is not
/// deleted by another handler of the same group, and the groups are
/// handled one by one.
pub struct LazyRefreshedSnapshot {
    store: Arc<dyn Storage>,
    snapshot: Option<Box<dyn Transaction>>,
    last_refresh_time: Instant,
    traffic_rec: Option<Arc<dyn TrafficRecorder>>,
}

impl LazyRefreshedSnapshot {
    /// Go `NewLazyRefreshedSnapshot`.
    #[must_use]
    pub fn new(store: Arc<dyn Storage>, traffic_rec: Option<Arc<dyn TrafficRecorder>>) -> Self {
        Self { store, snapshot: None, last_refresh_time: Instant::now(), traffic_rec }
    }

    fn refresh_as_needed(&mut self) -> Result<&mut Box<dyn Transaction>> {
        if self.snapshot.is_none() || self.last_refresh_time.elapsed() >= SNAPSHOT_REFRESH_INTERVAL {
            let snapshot = self.store.begin_read_only().map_err(|e| other(e.to_string()))?;
            self.snapshot = Some(snapshot);
            self.last_refresh_time = Instant::now();
        }
        Ok(self.snapshot.as_mut().expect("refreshed above"))
    }

    /// Go `BatchGet`: the keys that exist, with their values.
    ///
    /// # Errors
    /// The snapshot could not be read.
    pub fn batch_get(&mut self, keys: &[Vec<u8>]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let res = self.refresh_as_needed()?.batch_get(keys, &store_call()).map_err(store_error)?;
        if let Some(rec) = &self.traffic_rec {
            rec.inc_cluster_read_bytes(res.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum());
        }
        Ok(res)
    }
}

/// A store error, keeping whether Go would retry it.
#[derive(Debug)]
pub(crate) struct StoreError {
    pub(crate) message: String,
    pub(crate) retryable: bool,
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for StoreError {}

/// Go `common.IsRetryableError` of a store read: PD, region and backoff
/// failures retry; a store error by its MySQL code.
pub(crate) fn store_error(e: tidb_txnkv::transaction::OptimisticCoordinatorError) -> crate::Error {
    use tidb_txnkv::transaction::OptimisticCoordinatorError as E;
    let retryable = match &e {
        E::Storage(kv) => tidb_txnkv::is_txn_retryable_error(Some(kv)) || tidb_lightning_common::is_retryable_error(kv),
        E::Timestamp(_) | E::SnapshotGet(_) | E::SnapshotBackoff { .. } => true,
        _ => false,
    };
    Arc::new(StoreError { message: e.to_string(), retryable })
}

/// Whether a store error is retryable.
pub(crate) fn is_retryable(e: &crate::Error) -> bool {
    match e.downcast_ref::<StoreError>() {
        Some(s) => s.retryable,
        None => tidb_lightning_common::is_retryable_error(e.as_ref()),
    }
}
