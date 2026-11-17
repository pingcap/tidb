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

//! Go `br/pkg/restore/restorer.go`: the SST restorers over a file importer.
//! `SimpleRestorer` (raw/txn restore) and `PipelineRestorerWrapper` (log
//! restore's split pipeline) are not ported.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_proto::backup::File;

use crate::checkpoint::restore::{
    append_ranges_for_restore, new_checkpoint_range_key_item, RestoreRunner,
};
use crate::errors::Result;
use crate::restore_utils::rewrite_rule::RewriteRules;
use crate::utils::worker_pool::{ErrGroup, WorkerPool};

/// Go `BackupFileSet`: the files of one table and their rewrite rules.
#[derive(Clone, Debug, Default)]
pub struct BackupFileSet {
    /// Go `TableID`.
    pub table_id: i64,
    /// Go `SSTFiles`.
    pub sst_files: Vec<Arc<File>>,
    /// Go `RewriteRules`.
    pub rewrite_rules: Option<Arc<RewriteRules>>,
}

/// Go `BatchBackupFileSet`.
pub type BatchBackupFileSet = Vec<BackupFileSet>;

/// Go `zapBatchBackupFileSetMarshaler`: the log fields of a batch.
#[must_use]
pub fn zap_batch_backup_file_set(batch: &[BackupFileSet]) -> Field {
    let (mut total, mut kvs, mut bytes, mut size) = (0, 0_u64, 0_u64, 0_u64);
    let mut names = Vec::new();
    for fs in batch {
        for f in &fs.sst_files {
            total += 1;
            names.push(f.name.clone());
            kvs += f.total_kvs;
            bytes += f.total_bytes;
            size += f.size;
        }
    }
    Field::new(
        "fileset",
        Value::Str(format!("{{total: {total}, files: {names:?}, totalKVs: {kvs}, totalBytes: {bytes}, totalSize: {size}}}")),
    )
}

/// Go `NewFileSet`.
#[must_use]
pub fn new_file_set(files: Vec<Arc<File>>, rules: Option<Arc<RewriteRules>>) -> BackupFileSet {
    BackupFileSet {
        table_id: 0,
        sst_files: files,
        rewrite_rules: rules,
    }
}

/// Go `SstRestorer`.
pub trait SstRestorer: Send + Sync {
    /// Go `GoRestore`: starts importing the batches; `on_progress` counts the
    /// batches done.
    ///
    /// # Errors
    /// The restorer's context is done.
    fn go_restore(
        &self,
        on_progress: Arc<dyn Fn(i64) + Send + Sync>,
        batch_file_sets: Vec<BatchBackupFileSet>,
    ) -> Result<()>;
    /// Go `WaitUntilFinish`.
    ///
    /// # Errors
    /// The first import error.
    fn wait_until_finish(&self) -> Result<()>;
    /// Go `Close`.
    ///
    /// # Errors
    /// The importer's close error.
    fn close(&self) -> Result<()>;
}

/// Go `FileImporter`.
pub trait FileImporter: Send + Sync {
    /// Go `Import`.
    ///
    /// # Errors
    /// The import error after its retries.
    fn import(&self, ctx: &Context, file_sets: &[BackupFileSet]) -> Result<()>;
    /// Go `Close`.
    ///
    /// # Errors
    /// The connection close error.
    fn close(&self) -> Result<()>;
}

/// Go `BalancedFileImporter`.
pub trait BalancedFileImporter: FileImporter {
    /// Go `PauseForBackpressure`: waits while every store's tokens are taken.
    fn pause_for_backpressure(&self);
}

/// Go `GetFileRangeKey`: the file name up to its last `_`.
///
/// # Panics
/// A name without `_`, as Go's.
#[must_use]
pub fn get_file_range_key(f: &str) -> &str {
    match f.rfind('_') {
        Some(idx) => &f[..idx],
        None => panic!("invalid backup data file name: '{f}'"),
    }
}

/// Go `MultiTablesRestorer`: each batch imported by the worker pool after
/// the stores have room, its range keys checkpointed.
pub struct MultiTablesRestorer<I: BalancedFileImporter + 'static> {
    eg: ErrGroup,
    ectx: Context,
    worker_pool: WorkerPool,
    file_importer: Arc<I>,
    checkpoint_runner: Option<Arc<RestoreRunner>>,
    state: Mutex<(usize, Instant)>,
}

impl<I: BalancedFileImporter + 'static> MultiTablesRestorer<I> {
    /// Go `NewMultiTablesRestorer`.
    #[must_use]
    pub fn new(
        ctx: &Context,
        file_importer: Arc<I>,
        worker_pool: WorkerPool,
        checkpoint_runner: Option<Arc<RestoreRunner>>,
    ) -> Self {
        let (eg, ectx) = ErrGroup::with_context(ctx);
        Self {
            eg,
            ectx,
            worker_pool,
            file_importer,
            checkpoint_runner,
            state: Mutex::new((0, Instant::now())),
        }
    }
}

impl<I: BalancedFileImporter + 'static> SstRestorer for MultiTablesRestorer<I> {
    fn go_restore(
        &self,
        on_progress: Arc<dyn Fn(i64) + Send + Sync>,
        batch_file_sets: Vec<BatchBackupFileSet>,
    ) -> Result<()> {
        *self.state.lock().unwrap() = (0, Instant::now());
        for (i, batch) in batch_file_sets.into_iter().enumerate() {
            if let Some(err) = self.ectx.err() {
                tidb_log::warn(
                    "Restoring encountered error and already stopped, give up remained files.",
                    &[Field::new("error", Value::Str(err.to_string()))],
                );
                break;
            }
            self.state.lock().unwrap().0 += batch.iter().map(|s| s.sst_files.len()).sum::<usize>();
            self.file_importer.pause_for_backpressure();
            let (importer, ectx, runner, on_progress) = (
                Arc::clone(&self.file_importer),
                self.ectx.clone(),
                self.checkpoint_runner.clone(),
                Arc::clone(&on_progress),
            );
            self.worker_pool.apply_on_error_group(&self.eg, move || {
                let file_start = Instant::now();
                importer.import(&ectx, &batch)?;
                if let Some(runner) = &runner {
                    for group in &batch {
                        let keys: BTreeSet<&str> = group
                            .sst_files
                            .iter()
                            .map(|f| get_file_range_key(&f.name))
                            .collect();
                        for key in keys {
                            append_ranges_for_restore(
                                &ectx,
                                runner,
                                &new_checkpoint_range_key_item(group.table_id, key),
                            )?;
                        }
                    }
                }
                tidb_log::info(
                    "import files done",
                    &[
                        Field::new("sn", Value::I64(i as i64)),
                        Field::new(
                            "take",
                            Value::Duration(file_start.elapsed().as_nanos() as i64),
                        ),
                    ],
                );
                on_progress(1);
                Ok(())
            });
        }
        self.ectx.err().map_or(Ok(()), |e| Err(Arc::new(e)))
    }

    fn wait_until_finish(&self) -> Result<()> {
        if let Err(err) = self.eg.wait() {
            let reason = if crate::errors::is_context_canceled(err.as_ref()) {
                crate::summary::FailureReason::Canceled
            } else {
                crate::summary::FailureReason::Error(err.to_string())
            };
            crate::summary::collect_failure_unit("file", reason);
            tidb_log::error(
                "restore files failed",
                &[Field::new("error", Value::Str(err.to_string()))],
            );
            return Err(err);
        }
        let (count, start) = *self.state.lock().unwrap();
        let elapsed = start.elapsed();
        tidb_log::info(
            "Restore Stage Duration",
            &[
                Field::new("stage", Value::Str("restore files".into())),
                Field::new("take", Value::Duration(elapsed.as_nanos() as i64)),
            ],
        );
        crate::summary::collect_duration("restore files", elapsed);
        crate::summary::collect_success_unit(
            "files",
            count as i64,
            crate::summary::SuccessArg::Cost(elapsed),
        );
        Ok(())
    }

    fn close(&self) -> Result<()> {
        self.file_importer.close()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    use super::*;
    use crate::errors;

    /// Go `FakeBalancedFileImporter`.
    #[derive(Default)]
    struct FakeBalancedFileImporter {
        has_error: bool,
        unblock_count: AtomicUsize,
    }

    impl FileImporter for FakeBalancedFileImporter {
        fn import(&self, _: &Context, _: &[BackupFileSet]) -> Result<()> {
            if self.has_error {
                return Err(errors::errorf("import error"));
            }
            Ok(())
        }
        fn close(&self) -> Result<()> {
            Ok(())
        }
    }

    impl BalancedFileImporter for FakeBalancedFileImporter {
        fn pause_for_backpressure(&self) {
            self.unblock_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Go `createSampleBatchFileSets`.
    fn sample_batch() -> BatchBackupFileSet {
        let f = |name: &str, kvs| {
            Arc::new(File {
                name: name.into(),
                total_kvs: kvs,
                ..Default::default()
            })
        };
        vec![
            BackupFileSet {
                table_id: 1001,
                sst_files: vec![f("file1.sst", 10), f("file2.sst", 20)],
                rewrite_rules: None,
            },
            BackupFileSet {
                table_id: 1002,
                sst_files: vec![f("file3.sst", 15)],
                rewrite_rules: None,
            },
        ]
    }

    /// Go `TestMultiTablesRestorerRestoreSuccess`.
    #[test]
    fn multi_tables_restorer_restore_success() {
        let importer = Arc::new(FakeBalancedFileImporter::default());
        let r = MultiTablesRestorer::new(
            &Context::background(),
            Arc::clone(&importer),
            WorkerPool::new(2, "multi-tables-restorer"),
            None,
        );
        let progress = Arc::new(AtomicI64::new(0));
        let p = Arc::clone(&progress);
        r.go_restore(
            Arc::new(move |n| {
                p.fetch_add(n, Ordering::SeqCst);
            }),
            vec![sample_batch(), sample_batch()],
        )
        .unwrap();
        r.wait_until_finish().unwrap();
        assert_eq!(progress.load(Ordering::SeqCst), 2);
        assert_eq!(importer.unblock_count.load(Ordering::SeqCst), 2);
    }

    /// Go `TestMultiTablesRestorerRestoreWithImportError`.
    #[test]
    fn multi_tables_restorer_restore_with_import_error() {
        let importer = Arc::new(FakeBalancedFileImporter {
            has_error: true,
            ..Default::default()
        });
        let r = MultiTablesRestorer::new(
            &Context::background(),
            importer,
            WorkerPool::new(2, "multi-tables-restorer"),
            None,
        );
        let _ = r.go_restore(Arc::new(|_| {}), vec![sample_batch()]);
        let err = r.wait_until_finish().unwrap_err();
        assert!(err.to_string().contains("import error"), "{err}");
    }

    /// Go `TestMultiTablesRestorerRestoreWithContextCancel`.
    #[test]
    fn multi_tables_restorer_restore_with_context_cancel() {
        let (ctx, cancel) = Context::with_cancel(&Context::background());
        let importer = Arc::new(FakeBalancedFileImporter::default());
        let r = MultiTablesRestorer::new(
            &ctx,
            importer,
            WorkerPool::new(2, "multi-tables-restorer"),
            None,
        );
        cancel.cancel();
        let err = r
            .go_restore(Arc::new(|_| {}), vec![sample_batch()])
            .unwrap_err();
        assert_eq!(
            errors::cause(err.as_ref()).downcast_ref::<tidb_gocontext::CtxError>(),
            Some(&tidb_gocontext::CtxError::Canceled)
        );
    }

    /// Go `GetFileRangeKey`.
    #[test]
    fn file_range_key_is_the_name_up_to_its_last_underscore() {
        assert_eq!(get_file_range_key("1_2_3_write.sst"), "1_2_3");
    }
}
