// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/clean_up.go`: the cleanup of a finished import
//! task before it moves to history. It redacts the task meta, switches the
//! table back to normal mode, and deletes the global sort files.

use std::collections::BTreeMap;
use std::sync::Arc;

use tidb_dxf::Task;
use tidb_dxf_scheduler::{BatchCleaner, Cleaner};
use tidb_gocontext::Context;

use crate::proto::TaskMeta;
use crate::{plain, Error};

/// Go `ImportCleaner`.
struct ImportCleaner;

/// Go `init`'s `RegisterCleanerFactory`.
pub(crate) fn register() {
    tidb_dxf_scheduler::register_cleaner_factory(
        tidb_dxf::IMPORT_INTO,
        Arc::new(|| Arc::new(ImportCleaner) as Arc<dyn Cleaner>),
    );
}

impl Cleaner for ImportCleaner {
    /// Go `Clean`.
    fn clean(&self, ctx: &Context, task: &mut Task) -> Result<(), Error> {
        self.batch_clean(ctx, std::slice::from_mut(task))
    }

    fn as_batch_cleaner(&self) -> Option<&dyn BatchCleaner> {
        Some(self)
    }

    fn as_expired_file_cleaner(&self) -> Option<&dyn tidb_dxf_scheduler::ExpiredFileCleaner> {
        Some(self)
    }
}

impl tidb_dxf_scheduler::ExpiredFileCleaner for ImportCleaner {
    /// Go `CleanExpiredFiles`: the conflict row files past their retention.
    fn clean_expired_files(
        &self,
        ctx: &Context,
        task_info_getter: &dyn tidb_dxf_storage::TaskCleanupInfoGetter,
        cloud_storage_uri: &str,
    ) -> Result<(), Error> {
        tidb_dxf_importinto_conflictrows::clean_conflict_row_files(ctx, task_info_getter, cloud_storage_uri)
            .map_err(|e| plain(e.to_string()))
    }
}

/// Go `cleanFileGroup`: the tasks sharing one global sort store.
struct CleanFileGroup {
    non_partitioned_dirs: Vec<String>,
    task_ids: Vec<i64>,
}

impl BatchCleaner for ImportCleaner {
    /// Go `BatchClean`. A next-gen kernel also sends the metering data of
    /// succeeded global-sort tasks here; metering is not ported.
    fn batch_clean(&self, ctx: &Context, tasks: &mut [Task]) -> Result<(), Error> {
        let mut file_groups: BTreeMap<String, CleanFileGroup> = BTreeMap::new();
        for task in tasks.iter_mut() {
            let mut meta = TaskMeta::from_json(&task.meta)?;
            let cloud_storage_uri = meta.plan.cloud_storage_uri.clone();
            let is_global_sort = meta.plan.is_global_sort();
            redact_sensitive_info(task, &mut meta);
            clean_table_mode(ctx, &meta)?;
            if let Some(e) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/dxf/importinto/mockCleanupError") {
                if e.bool() {
                    return Err(plain("mock cleanup error"));
                }
            }
            if is_global_sort {
                let group = file_groups
                    .entry(cloud_storage_uri)
                    .or_insert_with(|| CleanFileGroup { non_partitioned_dirs: Vec::new(), task_ids: Vec::new() });
                group.non_partitioned_dirs.push(task.base.id.to_string());
                group.task_ids.push(task.base.id);
            }
        }
        for (uri, group) in &file_groups {
            clean_external_files(ctx, uri, group)?;
        }
        Ok(())
    }
}

/// Go `redactSensitiveInfo`: drops the statement and redacts the URLs.
pub(crate) fn redact_sensitive_info(task: &mut Task, meta: &mut TaskMeta) {
    meta.stmt = String::new();
    meta.plan.path = tidb_ast::redact_url(&meta.plan.path);
    if !meta.plan.cloud_storage_uri.is_empty() {
        meta.plan.cloud_storage_uri = tidb_ast::redact_url(&meta.plan.cloud_storage_uri);
    }
    task.meta = meta.to_json();
}

/// Go `cleanTableMode`: on a classic kernel, the table back to normal
/// mode, unless it no longer exists.
fn clean_table_mode(ctx: &Context, meta: &TaskMeta) -> Result<(), Error> {
    if tidb_config_kerneltype::is_next_gen() {
        return Ok(());
    }
    let Some(table) = meta.plan.table_info.as_ref() else { return Ok(()) };
    let manager = tidb_dxf_storage::get_task_manager().map_err(|e| Arc::new(e) as Error)?;
    let target = tidb_model::AlterTableModeTarget {
        schema_id: meta.plan.db_id,
        table_id: table.id,
        target_mode: tidb_model::TableMode::NORMAL,
        ..Default::default()
    };
    let result = manager.with_new_txn(ctx, &mut |se| {
        se.sql_server().get_runtime().alter_table_mode(ctx, &target).map_err(tidb_dxf_storage::StorageError::Other)
    });
    match result {
        Ok(()) => Ok(()),
        // Go `infoschema.ErrTableNotExists`: "Table '%s.%s' doesn't exist".
        Err(e) if e.to_string().contains("doesn't exist") => {
            tidb_logutil::bg_logger().warn(
                "table not found during import cleanup, skip altering table mode",
                &[tidb_log::Field::new("tableID", tidb_log::Value::I64(table.id))],
            );
            Ok(())
        }
        Err(e) => Err(Arc::new(e)),
    }
}

/// Go `cleanExternalFiles`.
fn clean_external_files(ctx: &Context, uri: &str, group: &CleanFileGroup) -> Result<(), Error> {
    let task = tidb_lightning_log::begin_logutil_task(
        tidb_logutil::bg_logger().with_fields(&[tidb_log::Field::new("task-ids", tidb_log::Value::Str(format!("{:?}", group.task_ids)))]),
        "cleanup global sorted data",
    );
    let store = match tidb_executor::importer::get_sort_store(ctx, uri) {
        Ok(store) => store,
        Err(e) => {
            tidb_logutil::bg_logger()
                .warn("failed to create store", &[tidb_log::Field::new("error", tidb_log::Value::Str(e.to_string()))]);
            return Err(crate::driver(e));
        }
    };
    let dirs: Vec<&str> = group.non_partitioned_dirs.iter().map(String::as_str).collect();
    let result = tidb_ingestor_globalsort::clean_up_files(ctx, store.as_ref(), &dirs);
    task.end(tidb_log::Level::Info, None, &[]);
    result.map_err(|e| {
        tidb_logutil::bg_logger().warn(
            "failed to clean up files of tasks",
            &[tidb_log::Field::new("error", tidb_log::Value::Str(e.to_string()))],
        );
        crate::planner::global_sort_error(e)
    })
}
