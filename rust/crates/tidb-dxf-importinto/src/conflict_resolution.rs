// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/conflict_resolution.go`: the conflict-resolution
//! step, which deletes the KVs of the conflicted rows from the cluster.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use tidb_dxf::{StepResource, Subtask};
use tidb_dxf_execute::{FrameworkInfoSlot, StepExecFrameworkInfo, StepExecutor, SubtaskSummary};
use tidb_dxf_importinto_conflictedkv::Deleter;
use tidb_executor::importer::kv_encode::TableKVEncoder;
use tidb_executor::importer::table_import::TableImporter;
use tidb_gocontext::Context;

type StepResult<T> = tidb_dxf_execute::Result<T>;
use tidb_ingestor_engineapi::ConflictInfo;
use tidb_log::{Field, Value};

use crate::collect_conflicts::{create_conflict_handler_channels, dispatch_mv_index_kv_pairs, get_kv_group_index_info};
use crate::proto::{self, ConflictResolutionStepMeta, TaskMeta};
use crate::task_executor::{new_table_importer, step_err};
use crate::{plain, Error};

/// Go `conflictResolutionStepExecutor`.
pub(crate) struct ConflictResolutionStepExecutor {
    pub(crate) info: FrameworkInfoSlot,
    pub(crate) task_id: i64,
    pub(crate) meta: TaskMeta,
    pub(crate) table_importer: Mutex<Option<Arc<TableImporter>>>,
    pub(crate) summary: Arc<SubtaskSummary>,
}

impl StepExecFrameworkInfo for ConflictResolutionStepExecutor {
    fn framework_info_slot(&self) -> &FrameworkInfoSlot {
        &self.info
    }
}

/// Go's `execute.Collector` of the conflict steps: the conflicted KVs
/// handled are the processed units.
struct ConflictCollector(Arc<SubtaskSummary>);

impl tidb_dxf_execute::Collector for ConflictCollector {
    fn accepted(&self, _: i64) {}
    fn processed(&self, processed_conflict_kvs: i64, _: i64) {
        self.0.processed.fetch_add(processed_conflict_kvs, Ordering::SeqCst);
    }
}

/// The progress collector of a conflict step.
pub(crate) fn conflict_collector(summary: &Arc<SubtaskSummary>) -> Arc<dyn tidb_dxf_execute::Collector> {
    Arc::new(ConflictCollector(Arc::clone(summary)))
}

/// The store the conflict steps read and delete through.
pub(crate) fn kv_store() -> Result<Arc<dyn tidb_txnkv::kv_storage::Storage>, Error> {
    crate::import_store()?.kv_storage().ok_or_else(|| plain("the store of this instance has no TiKV"))
}

impl ConflictResolutionStepExecutor {
    fn importer(&self) -> Result<Arc<TableImporter>, Error> {
        self.table_importer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| plain("conflict resolution step executor is not initialized"))
    }

    /// Go `RunSubtask`. The KV groups are resolved one by one; since the
    /// collection is a separate step, they could run in parallel later.
    fn run(&self, ctx: &Context, subtask: &mut Subtask) -> Result<(), Error> {
        let ti = self.importer()?;
        let recorded = crate::planner::new_obj_store_with_recording(
            ctx,
            &self.meta.plan,
            &self.summary,
            self.get_meter_recorder(),
        )?;
        let obj_store = Arc::clone(&recorded.store);
        let mut step_meta: ConflictResolutionStepMeta = proto::unmarshal(&subtask.meta)?;
        step_meta.read_external(ctx, obj_store.as_ref())?;
        let concurrency = self.get_resource().cpu.capacity().max(1) as usize;
        for (kv_group, ci) in step_meta.infos.infos().clone() {
            let mut result = self.resolve_conflicts_of_kv_group(ctx, &ti, &obj_store, concurrency, &kv_group, &ci);
            tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/dxf/importinto/afterResolveOneKVGroup", &mut result);
            result?;
        }
        Ok(())
    }

    /// Go `resolveConflictsOfKVGroup`.
    fn resolve_conflicts_of_kv_group(
        &self,
        ctx: &Context,
        ti: &TableImporter,
        obj_store: &Arc<dyn tidb_objstore::storeapi::Storage>,
        mut concurrency: usize,
        kv_group: &str,
        ci: &ConflictInfo,
    ) -> Result<(), Error> {
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/dxf/importinto/forceHandleConflictsBySingleThread").is_some() {
            concurrency = 1;
        }
        let task = tidb_lightning_log::begin_logutil_task(
            tidb_logutil::bg_logger().with_fields(&[
                Field::new("kvGroup", Value::Str(kv_group.to_owned())),
                Field::new("duplicates", Value::U64(ci.count)),
                Field::new("fileCount", Value::I64(ci.files.len() as i64)),
                Field::new("concurrency", Value::I64(concurrency as i64)),
            ]),
            "resolve conflicts of kv group",
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
            std::thread::scope(|scope| {
                let mut handles = Vec::new();
                for (rx, encoder) in handler_rxs.into_iter().zip(encoders) {
                    let deleter = Deleter::new(
                        ti.controller.table.clone(),
                        Arc::clone(&store),
                        ti.key_space(),
                        kv_group,
                        encoder,
                        Some(Arc::clone(&progress)),
                        Some(self.get_meter_recorder() as Arc<dyn tidb_dxf_importinto_conflictedkv::TrafficRecorder>),
                    );
                    let (ctx, cancel) = (&ctx, &cancel);
                    handles.push(scope.spawn(move || {
                        let r = deleter.run(ctx, &rx);
                        if r.is_err() {
                            cancel.cancel();
                        }
                        r
                    }));
                }
                if let Some((input, senders)) = dispatch {
                    let (ctx, cancel) = (&ctx, &cancel);
                    let columns = target_idx.as_ref().map_or(0, |i| i.columns.len());
                    handles.push(scope.spawn(move || {
                        let r = dispatch_mv_index_kv_pairs(ctx, &input, senders, columns);
                        if r.is_err() {
                            cancel.cancel();
                        }
                        r
                    }));
                }
                for h in handles {
                    if let Err(e) = h.join().unwrap_or_else(|_| Err(plain("conflict handler panicked"))) {
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
            Ok(())
        })();
        task.end(tidb_log::Level::Error, result.as_ref().err().map(|e| e.as_ref() as &(dyn std::error::Error + 'static)), &[]);
        result
    }
}

/// Go `createEncoders`: every encoder is made before any runs, as Go must
/// to keep the generated column expressions from racing.
pub(crate) fn create_encoders(concurrency: usize, ti: &TableImporter) -> Result<Vec<TableKVEncoder>, Error> {
    (0..concurrency).map(|_| ti.get_kv_encoder_for_dup_resolve().map_err(plain)).collect()
}

impl StepExecutor for ConflictResolutionStepExecutor {
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
