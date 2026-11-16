// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/collect_conflicts.go`: the collect-conflicts
//! step, which counts, checksums and records the conflicted rows of each
//! KV group.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::{Arc, Mutex, RwLock};

use tidb_dxf::{StepResource, Subtask};
use tidb_dxf_execute::{FrameworkInfoSlot, StepExecFrameworkInfo, StepExecutor, SubtaskSummary};
use tidb_dxf_importinto_conflictedkv::{
    BoundedKeySet, CollectResult, Collector, PairReceiver, BUFFERED_HANDLE_LIMIT,
};
use tidb_executor::importer::table_import::TableImporter;
use tidb_gocontext::Context;

type StepResult<T> = tidb_dxf_execute::Result<T>;
use tidb_ingestor_engineapi::ConflictInfo;
use tidb_ingestor_simplesst::KVPair;
use tidb_log::{Field, Value};

use crate::conflict_resolution::{conflict_collector, create_encoders, kv_store};
use crate::proto::{self, CollectConflictsStepMeta, Checksum, TaskMeta};
use crate::task_executor::{new_table_importer, step_err};
use crate::{plain, Error};

/// Go `collectConflictsStepExecutor`.
pub(crate) struct CollectConflictsStepExecutor {
    pub(crate) info: FrameworkInfoSlot,
    pub(crate) task_id: i64,
    pub(crate) meta: TaskMeta,
    pub(crate) table_importer: Mutex<Option<Arc<TableImporter>>>,
    pub(crate) summary: Arc<SubtaskSummary>,
}

impl StepExecFrameworkInfo for CollectConflictsStepExecutor {
    fn framework_info_slot(&self) -> &FrameworkInfoSlot {
        &self.info
    }
}

/// Go's per-subtask fields.
struct SubtaskState {
    subtask_id: i64,
    size_of_row_keys_from_index: Arc<AtomicI64>,
    size_limit_of_row_keys_from_index: i64,
    size_of_conflict_row_files: Arc<AtomicI64>,
    result: CollectResult,
    /// One conflicted row can have conflicted KVs in several unique
    /// indexes, so its checksum is taken once: the rows handled in earlier
    /// index KV groups. The groups are handled one by one for this.
    shared_row_key_set: Arc<RwLock<BoundedKeySet>>,
}

impl CollectConflictsStepExecutor {
    fn importer(&self) -> Result<Arc<TableImporter>, Error> {
        self.table_importer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| plain("collect conflicts step executor is not initialized"))
    }

    /// Go `RunSubtask`.
    fn run(&self, ctx: &Context, subtask: &mut Subtask) -> Result<(), Error> {
        let ti = self.importer()?;
        let obj_store = crate::planner::sort_store(ctx, &self.meta.plan)?;
        let mut st_meta: CollectConflictsStepMeta = proto::unmarshal(&subtask.meta)?;
        st_meta.read_external(ctx, obj_store.as_ref())?;
        let res = self.get_resource();
        let mut state = reset_for_new_subtask(subtask.base.id, &res, ti.key_space());
        let concurrency = res.cpu.capacity().max(1) as usize;
        for (kv_group, ci) in st_meta.infos.infos().clone() {
            let mut result = self.collect_conflicts_of_kv_group(ctx, &ti, &obj_store, concurrency, &kv_group, &ci, &mut state);
            tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/dxf/importinto/afterCollectOneKVGroup", &mut result);
            result?;
        }
        on_finished(subtask, st_meta, &state);
        Ok(())
    }

    /// Go `collectConflictsOfKVGroup`.
    #[allow(clippy::too_many_arguments)]
    fn collect_conflicts_of_kv_group(
        &self,
        ctx: &Context,
        ti: &TableImporter,
        obj_store: &Arc<dyn tidb_objstore::storeapi::Storage>,
        mut concurrency: usize,
        kv_group: &str,
        ci: &ConflictInfo,
        state: &mut SubtaskState,
    ) -> Result<(), Error> {
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/dxf/importinto/forceHandleConflictsBySingleThread").is_some() {
            concurrency = 1;
        }
        let task = tidb_lightning_log::begin_logutil_task(
            tidb_logutil::bg_logger().with_fields(&[
                Field::new("kvGroup", Value::Str(kv_group.to_owned())),
                Field::new("duplicates", Value::U64(ci.count)),
                Field::new("file-count", Value::I64(ci.files.len() as i64)),
                Field::new("concurrency", Value::I64(concurrency as i64)),
            ]),
            "collect conflicts of kv group",
        );
        let result = (|| {
            let target_idx = get_kv_group_index_info(ti, kv_group)?;
            let encoders = create_encoders(concurrency, ti)?;
            let store = kv_store()?;
            let progress = conflict_collector(&self.summary);
            let (ctx, cancel) = Context::with_cancel(ctx);
            let (pair_rx, reader) = tidb_ingestor_globalsort::read_kv_files_async(&ctx, Arc::clone(obj_store), ci.files.clone());
            let (handler_rxs, dispatch) = create_conflict_handler_channels(pair_rx, concurrency, target_idx.as_ref());
            let mut first_err: Option<Error> = None;
            let mut local_sets = Vec::new();
            std::thread::scope(|scope| {
                let mut handles = Vec::new();
                for (rx, encoder) in handler_rxs.into_iter().zip(encoders) {
                    let filename_prefix = tidb_dxf_importinto_conflictpath::new_file_name_prefix(self.task_id, state.subtask_id);
                    let local_set = BoundedKeySet::new(
                        Arc::clone(&state.size_of_row_keys_from_index),
                        state.size_limit_of_row_keys_from_index,
                    );
                    let mut collector = Collector::new(
                        ti.controller.table.clone(),
                        Arc::clone(obj_store),
                        &store,
                        ti.key_space(),
                        filename_prefix,
                        kv_group,
                        encoder,
                        Arc::clone(&state.shared_row_key_set),
                        local_set,
                        Arc::clone(&state.size_of_conflict_row_files),
                        Some(Arc::clone(&progress)),
                        None,
                    );
                    let (ctx, cancel) = (&ctx, &cancel);
                    handles.push(scope.spawn(move || {
                        let run = collector.run(ctx, &rx);
                        drop(rx);
                        let close = collector.close(ctx);
                        let result = run.and(close);
                        if result.is_err() {
                            cancel.cancel();
                        }
                        (result, collector.take_local_set(), collector.collect_result().clone())
                    }));
                }
                let dispatcher = dispatch.map(|(input, senders)| {
                    let (ctx, cancel) = (&ctx, &cancel);
                    let columns = target_idx.as_ref().map_or(0, |i| i.columns.len());
                    scope.spawn(move || {
                        let r = dispatch_mv_index_kv_pairs(ctx, &input, senders, columns);
                        if r.is_err() {
                            cancel.cancel();
                        }
                        r
                    })
                });
                for h in handles {
                    let (result, local_set, collect_result) =
                        h.join().unwrap_or_else(|_| (Err(plain("collector panicked")), None, CollectResult::new(&[])));
                    if let Err(e) = result {
                        first_err.get_or_insert(e);
                    }
                    local_sets.extend(local_set);
                    state.result.merge(&collect_result);
                }
                if let Some(d) = dispatcher {
                    if let Err(e) = d.join().unwrap_or_else(|_| Err(plain("dispatcher panicked"))) {
                        first_err.get_or_insert(e);
                    }
                }
            });
            let read = reader.join().unwrap_or_else(|_| Err("reader panicked".into()));
            if let Some(e) = first_err {
                return Err(e);
            }
            read.map_err(crate::planner::global_sort_error)?;
            if let Some(e) = ctx.err() {
                return Err(plain(e.to_string()));
            }
            let mut shared = state.shared_row_key_set.write().unwrap_or_else(std::sync::PoisonError::into_inner);
            for s in local_sets {
                shared.merge(s);
            }
            Ok(())
        })();
        task.end(tidb_log::Level::Error, result.as_ref().err().map(|e| e.as_ref() as &(dyn std::error::Error + 'static)), &[]);
        result
    }
}

/// Go `resetForNewSubtask`. Only one subtask runs today; there may be more
/// later, each with its own state. Half the subtask memory is for the row
/// keys of index conflicts.
fn reset_for_new_subtask(subtask_id: i64, res: &StepResource, keyspace: &[u8]) -> SubtaskState {
    let size_of_row_keys_from_index = Arc::new(AtomicI64::new(0));
    let size_limit = res.mem.capacity() / 2;
    SubtaskState {
        subtask_id,
        shared_row_key_set: Arc::new(RwLock::new(BoundedKeySet::new(Arc::clone(&size_of_row_keys_from_index), size_limit))),
        size_of_row_keys_from_index,
        size_limit_of_row_keys_from_index: size_limit,
        size_of_conflict_row_files: Arc::new(AtomicI64::new(0)),
        result: CollectResult::new(keyspace),
    }
}

/// Go `onFinished`: the result goes into the subtask meta.
fn on_finished(subtask: &mut Subtask, mut meta: CollectConflictsStepMeta, state: &SubtaskState) {
    let r = &state.result;
    let bound_exceeded =
        state.shared_row_key_set.read().unwrap_or_else(std::sync::PoisonError::into_inner).bound_exceeded();
    tidb_logutil::bg_logger().info(
        "collected conflict row info",
        &[
            Field::new("count", Value::I64(r.row_count)),
            Field::new("checksum", Value::Str(r.checksum.to_string())),
            Field::new("targetFiles", Value::Array(r.filenames.iter().cloned().map(Value::Str).collect())),
            Field::new("fileSize", Value::Str(tidb_config_configtypes::bytes_size(r.total_file_size as f64))),
            Field::new("rowKeySetLimitExceeded", Value::Bool(bound_exceeded)),
        ],
    );
    meta.checksum = Some(Checksum::from_kv_checksum(&r.checksum));
    meta.conflicted_row_count = r.row_count;
    meta.conflicted_row_filenames = Some(r.filenames.clone());
    meta.conflicted_row_recording_capped = r.row_recording_capped;
    meta.too_many_conflicts_from_index = bound_exceeded;
    subtask.meta = meta.marshal();
}

/// Go `getKVGroupIndexInfo`: the index of an index KV group, `None` for the
/// data KV group.
pub(crate) fn get_kv_group_index_info(ti: &TableImporter, kv_group: &str) -> Result<Option<tidb_model::IndexInfo>, Error> {
    if kv_group == tidb_ingestor_globalsort::DATA_KV_GROUP {
        return Ok(None);
    }
    let index_id = tidb_ingestor_globalsort::kv_group_2_index_id(kv_group).map_err(crate::planner::global_sort_error)?;
    let meta = ti.controller.table.meta();
    let meta = meta.read();
    match meta.indices.iter_deref().find(|i| i.read().id == index_id) {
        Some(index) => Ok(Some(index.read().clone())),
        // should not happen
        None => Err(plain(format!(
            "index {index_id} from KV group {kv_group:?} not found in table {}",
            meta.name.original()
        ))),
    }
}

/// The input of [`dispatch_mv_index_kv_pairs`] and the handlers' senders.
pub(crate) type Dispatch = (PairReceiver, Vec<SyncSender<KVPair>>);

/// Go `createConflictHandlerChannels`: every handler reads `pair_rx`,
/// except for an MV index with more than one handler. A row can have
/// several unique KVs of an MV index, so each row's KVs go to one handler,
/// each with a buffer of one batch so a busy handler does not hold up the
/// others.
pub(crate) fn create_conflict_handler_channels(
    pair_rx: std::sync::mpsc::Receiver<KVPair>,
    concurrency: usize,
    target_idx: Option<&tidb_model::IndexInfo>,
) -> (Vec<PairReceiver>, Option<Dispatch>) {
    let shared: PairReceiver = Arc::new(Mutex::new(pair_rx));
    let need_dispatch = concurrency > 1 && target_idx.is_some_and(|i| i.mv_index);
    if !need_dispatch {
        return ((0..concurrency).map(|_| Arc::clone(&shared)).collect(), None);
    }
    let (senders, receivers): (Vec<_>, Vec<_>) = (0..concurrency)
        .map(|_| {
            let (tx, rx) = sync_channel(BUFFERED_HANDLE_LIMIT.load(Ordering::Relaxed));
            (tx, Arc::new(Mutex::new(rx)))
        })
        .unzip();
    (receivers, Some((shared, senders)))
}

/// Go `dispatchMVIndexKVPairs`: each index KV to the handler of its row's
/// handle, by `crc32(handle) % handlers`.
pub(crate) fn dispatch_mv_index_kv_pairs(
    ctx: &Context,
    pair_rx: &PairReceiver,
    handler_txs: Vec<SyncSender<KVPair>>,
    index_columns: usize,
) -> Result<(), Error> {
    while let Some(pair) = tidb_dxf_importinto_conflictedkv::recv(pair_rx) {
        if let Some(e) = ctx.err() {
            return Err(plain(e.to_string()));
        }
        let handle = tidb_tablecodec::decode_index_handle(&pair.key, &pair.value, index_columns)
            .map_err(|e| plain(e.to_string()))?
            .ok_or_else(|| plain("no handle in index key or value"))?;
        let idx = crc32fast::hash(&handle.encoded()) as usize % handler_txs.len();
        if handler_txs[idx].send(pair).is_err() {
            // The handler is gone: it failed and canceled the context.
            return Err(plain(ctx.err().map_or_else(|| "conflict handler exited".to_owned(), |e| e.to_string())));
        }
    }
    Ok(())
}

impl StepExecutor for CollectConflictsStepExecutor {
    /// Go `Init`.
    fn init(&self, ctx: &Context) -> StepResult<()> {
        let ti = new_table_importer(ctx, self.task_id, &self.meta).map_err(step_err)?;
        *self.table_importer.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(ti));
        Ok(())
    }

    fn run_subtask(&self, ctx: &Context, subtask: &mut Subtask) -> StepResult<()> {
        self.run(ctx, subtask).map_err(step_err)
    }

    fn realtime_summary(&self) -> Option<Arc<SubtaskSummary>> {
        Some(self.summary.clone())
    }

    fn reset_summary(&self) {
        self.summary.reset();
    }

    /// Go `Cleanup`.
    fn cleanup(&self, _: &Context) -> StepResult<()> {
        if let Some(ti) = self.table_importer.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take() {
            ti.controller.close();
            ti.close();
        }
        Ok(())
    }

    fn task_meta_modified(&self, _: &Context, _: &[u8]) -> StepResult<()> {
        Ok(())
    }

    fn resource_modified(&self, _: &Context, _: &StepResource) -> StepResult<()> {
        Ok(())
    }
}
