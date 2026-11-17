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


//! Go `restore/snap_client/pitr_collector.go`: a snapshot restore into a
//! cluster with a running log backup. Log backup cannot back ingested SSTs,
//! so the restored SSTs are copied into the task's storage under
//! `v1/ext_backups/<name>/` with their metadata, and a migration names them,
//! so a later point-in-time restore ingests them too.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use prost::Message as _;
use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_objstore::storeapi::{CopySpec, Storage};
use tidb_proto::backup::{File, IngestedSsTs, RewrittenTableId, StorageBackend};

use crate::errors::{self, Result, ERR_INVALID_ARGUMENT, ERR_UNSUPPORTED_OPERATION};
use crate::pdclient::PdClient;
use crate::restore::restorer::BackupFileSet;
use crate::streamhelper::MetaDataClient;
use crate::utils::worker_pool::{ErrGroup, WorkerPool};

/// Go `DefaultMaxConcurrentCopy`.
pub const DEFAULT_MAX_CONCURRENT_COPY: usize = 1024;

fn objstore_err(e: tidb_objstore::Error) -> errors::Error {
    errors::errorf(e.to_string())
}

/// Go `pitrCollector.tso`.
type TsoFn = Box<dyn Fn(&Context) -> Result<u64> + Send + Sync>;

/// Go `persistCall`.
type PersistCall = (Context, crossbeam_channel::Sender<Result<()>>);

/// Go `ingestedSSTsMeta`: the state persisted as the extra backup meta.
#[derive(Default)]
struct IngestedSstsMeta {
    msg: IngestedSsTs,
    rewrites: BTreeMap<i64, i64>,
}

impl IngestedSstsMeta {
    /// Go `toProtoMessage`.
    fn to_proto_message(&self) -> IngestedSsTs {
        let mut msg = self.msg.clone();
        for (&old, &new) in &self.rewrites {
            msg.rewritten_tables.push(RewrittenTableId { ancestor_upstream: old, upstream: new });
        }
        msg
    }
}

/// The shared state of Go `pitrCollector`.
struct Inner {
    task_storage: Arc<dyn Storage>,
    restore_storage: Arc<dyn Storage>,
    name: String,
    restore_uuid: Mutex<uuid::Uuid>,
    operation_context: crate::operation::Context,
    ingested_sst_meta: Mutex<IngestedSstsMeta>,
    /// Go `migrationPrepared` under `prepareMigLock`.
    migration_prepared: Mutex<bool>,
    writer: Mutex<Option<crossbeam_channel::Sender<PersistCall>>>,
    conc_control: WorkerPool,
    tso: TsoFn,
    restore_success: Box<dyn Fn() -> bool + Send + Sync>,
}

/// Go `pitrCollector`. A disabled collector (no log backup task) does
/// nothing.
#[derive(Clone, Default)]
pub struct PitrCollector {
    inner: Option<Arc<Inner>>,
}

/// Go `PiTRCollDep`.
pub struct PitrCollDep {
    /// Go `PDCli`.
    pub pd_cli: Arc<dyn PdClient>,
    /// Go `EtcdCli`, as the metadata client over it.
    pub etcd_cli: MetaDataClient,
    /// Go `Storage`: the backup being restored.
    pub storage: StorageBackend,
    /// Go `OperationContext`.
    pub operation_context: crate::operation::Context,
    max_copy_concurrency: usize,
}

impl PitrCollDep {
    /// The dependencies; the copy concurrency comes from
    /// [`Self::load_max_copy_concurrency`].
    #[must_use]
    pub fn new(
        pd_cli: Arc<dyn PdClient>,
        etcd_cli: MetaDataClient,
        storage: StorageBackend,
        operation_context: crate::operation::Context,
    ) -> Self {
        Self { pd_cli, etcd_cli, storage, operation_context, max_copy_concurrency: 0 }
    }

    /// Go `LoadMaxCopyConcurrency`: the per-store concurrency times the
    /// stores.
    ///
    /// # Errors
    /// PD's error.
    pub fn load_max_copy_concurrency(&mut self, ctx: &Context, max_conc_per_tikv: usize) -> Result<()> {
        let stores = self.pd_cli.get_all_stores(ctx, false)?;
        self.max_copy_concurrency = max_conc_per_tikv * stores.len();
        tidb_log::info(
            "Load max copy concurrency",
            &[
                Field::new("maxCopyConcurrency", Value::U64(self.max_copy_concurrency as u64)),
                Field::new("stores", Value::U64(stores.len() as u64)),
            ],
        );
        Ok(())
    }
}

/// A collector over the given parts, for tests (Go builds the struct).
#[cfg(test)]
fn new_for_test(
    task_storage: Arc<dyn Storage>,
    restore_storage: Arc<dyn Storage>,
    name: String,
    restore_uuid: uuid::Uuid,
    operation_context: crate::operation::Context,
    tso: TsoFn,
    restore_success: Box<dyn Fn() -> bool + Send + Sync>,
) -> PitrCollector {
    let inner = Arc::new(Inner {
        task_storage,
        restore_storage,
        name,
        restore_uuid: Mutex::new(restore_uuid),
        operation_context,
        ingested_sst_meta: Mutex::default(),
        migration_prepared: Mutex::new(false),
        writer: Mutex::new(None),
        conc_control: WorkerPool::new(DEFAULT_MAX_CONCURRENT_COPY, "copy-sst"),
        tso,
        restore_success,
    });
    inner.go_persister();
    inner.reset_committing();
    PitrCollector { inner: Some(inner) }
}

/// Go `newPiTRColl`: enabled when one log backup task runs.
///
/// # Errors
/// More than one task, or a storage or PD error.
pub fn new_pitr_coll(ctx: &Context, deps: &PitrCollDep) -> Result<PitrCollector> {
    let ts = deps.etcd_cli.get_all_tasks()?;
    if ts.len() > 1 {
        let names: Vec<String> = ts.iter().map(|t| format!("{:?}", t.info.name)).collect();
        return Err(errors::annotatef(
            &ERR_INVALID_ARGUMENT,
            format!(
                "more than one task found, pitr collector doesn't support that, tasks are: []string{{{}}}",
                names.join(", ")
            ),
        ));
    }
    let Some(task) = ts.into_iter().next() else { return Ok(PitrCollector::default()) };
    let task_storage = tidb_objstore::create(ctx, &task.info.storage.unwrap_or_default(), false).map_err(objstore_err)?;
    let pd = Arc::clone(&deps.pd_cli);
    let tso = move |ctx: &Context| -> Result<u64> {
        let (l, o) = pd.get_ts(ctx)?;
        Ok(crate::pdclient::compose_ts(l, o))
    };
    let t = tso(ctx)?;
    let restore_storage = tidb_objstore::create(ctx, &deps.storage, false).map_err(objstore_err)?;
    let max_conc = if deps.max_copy_concurrency == 0 { DEFAULT_MAX_CONCURRENT_COPY } else { deps.max_copy_concurrency };
    let inner = Arc::new(Inner {
        task_storage,
        restore_storage,
        name: format!("backup-{t:016X}"),
        restore_uuid: Mutex::new(uuid::Uuid::nil()),
        operation_context: deps.operation_context.clone(),
        ingested_sst_meta: Mutex::default(),
        migration_prepared: Mutex::new(false),
        writer: Mutex::new(None),
        conc_control: WorkerPool::new(max_conc, "copy-sst"),
        tso: Box::new(tso),
        restore_success: Box::new(crate::summary::succeed),
    });
    // Go `init`.
    inner.go_persister();
    inner.reset_committing();
    Ok(PitrCollector { inner: Some(inner) })
}

impl Inner {
    /// Go `outputPath`.
    fn output_path(&self, segs: &[&str]) -> String {
        let mut parts = vec!["v1", "ext_backups", self.name.as_str()];
        parts.extend_from_slice(segs);
        tidb_objstore::gopath::join(&parts)
    }

    /// Go `metaPath`.
    fn meta_path(&self) -> String {
        self.output_path(&["extbackupmeta"])
    }

    /// Go `sstPath`.
    fn sst_path(&self, name: &str) -> String {
        self.output_path(&["sst_files", name])
    }

    /// Go `goPersister`: the only writer of the meta path, folding the
    /// calls queued behind one write into it.
    fn go_persister(self: &Arc<Self>) {
        let (hnd, rx) = crossbeam_channel::bounded::<PersistCall>(2048);
        let this = Arc::downgrade(self);
        std::thread::Builder::new()
            .name("pitr-coll-persister".into())
            .spawn(move || {
                while let Ok(new_call) = rx.recv() {
                    let mut cs = vec![new_call];
                    while let Ok(c) = rx.try_recv() {
                        cs.push(c);
                    }
                    let err = match this.upgrade() {
                        Some(this) => this.do_persist_extra_backup_meta(&cs[0].0),
                        None => Err(errors::errorf("the collector is gone")),
                    };
                    for (_, cb) in cs {
                        let _ = cb.try_send(err.clone());
                    }
                }
            })
            .expect("spawn the persister");
        *self.writer.lock().unwrap() = Some(hnd);
    }

    /// Go `resetCommitting`.
    fn reset_committing(&self) {
        let uuid = *self.restore_uuid.lock().unwrap();
        let mut meta = self.ingested_sst_meta.lock().unwrap();
        *meta = IngestedSstsMeta::default();
        meta.msg.files_prefix_hint = self.sst_path("");
        meta.msg.finished = false;
        meta.msg.backup_uuid = uuid.as_bytes().to_vec();
    }

    /// Go `doPersistExtraBackupMeta`: only the persister calls it.
    fn do_persist_extra_backup_meta(&self, ctx: &Context) -> Result<()> {
        let begin = Instant::now();
        let bs = self.ingested_sst_meta.lock().unwrap().to_proto_message().encode_to_vec();
        tidb_log::info(
            "Persisting extra backup meta.",
            &[
                Field::new("uuid", Value::Str(self.restore_uuid.lock().unwrap().to_string())),
                Field::new("path", Value::Str(self.meta_path())),
                Field::new("takes", Value::Str(format!("{:?}", begin.elapsed()))),
            ],
        );
        self.task_storage
            .write_file(ctx, &self.meta_path(), &bs)
            .map_err(|e| errors::annotate(objstore_err(e), format!("failed to put content to meta to {}", self.meta_path())))
    }

    /// Go `persistExtraBackupMeta` / `persisterHandle.write`: everything
    /// recorded before the call is persisted once it returns.
    fn persist_extra_backup_meta(&self, ctx: &Context) -> Result<()> {
        // Buffered, so a fast persister cannot reply before we wait.
        let (tx, rx) = crossbeam_channel::bounded(1);
        let hnd = self.writer.lock().unwrap().clone().ok_or_else(|| errors::errorf("the persister is closed"))?;
        hnd.send((ctx.clone(), tx)).map_err(|_| errors::errorf("the persister is closed"))?;
        crossbeam_channel::select! {
            recv(rx) -> r => r.unwrap_or_else(|_| Err(errors::errorf("[unreachable] A channel excepted to be never closed was closed."))),
            recv(ctx.done()) -> _ => Err(Arc::new(ctx.err().unwrap_or(tidb_gocontext::CtxError::Canceled))),
        }
    }

    /// Go `putSST`.
    fn put_sst(&self, ctx: &Context, f: &File) -> Result<()> {
        tidb_failpoint::inject_call("github.com/pingcap/tidb/br/pkg/restore/snap_client/put-sst", &mut ());
        let mut f = f.clone();
        let out = self.sst_path(&f.name);
        let Some(copier) = self.task_storage.as_copier() else {
            return Err(errors::annotatef(
                &ERR_INVALID_ARGUMENT,
                format!("storage {} does not support copying", self.task_storage.uri()),
            ));
        };
        let spec = CopySpec { from: f.name.clone(), to: out.clone() };
        let copy_start = Instant::now();
        copier.copy_from(ctx, self.restore_storage.as_ref(), &spec).map_err(|e| {
            errors::annotate(
                objstore_err(e),
                format!(
                    "failed to copy sst file {} to {}, you may check whether permissions are granted in both {} and {}, and the two storages are provided by the same cloud vendor",
                    spec.from,
                    spec.to,
                    self.restore_storage.uri(),
                    self.task_storage.uri()
                ),
            )
        })?;
        tidb_log::info(
            "Copy SST to log backup storage success.",
            &[
                Field::new("file", Value::Str(f.name.clone())),
                Field::new("takes", Value::Str(format!("{:?}", copy_start.elapsed()))),
            ],
        );
        f.name = out;
        self.ingested_sst_meta.lock().unwrap().msg.files.push(f);
        Ok(())
    }

    /// Go `putRewriteRule`.
    fn put_rewrite_rule(&self, old_id: i64, new_id: i64) -> Result<()> {
        let mut meta = self.ingested_sst_meta.lock().unwrap();
        if let Some(&old_val) = meta.rewrites.get(&old_id) {
            if old_val != new_id {
                return Err(errors::annotatef(
                    &ERR_INVALID_ARGUMENT,
                    format!("pitr coll rewrite rule conflict: we had {old_id} -> {old_val}, but you want rewrite to {new_id}"),
                ));
            }
        }
        meta.rewrites.insert(old_id, new_id);
        Ok(())
    }

    /// Go `prepareMig`: the meta path published as a migration, after the
    /// meta is durable (a retried restore uses a new path and cannot repair
    /// an object an earlier migration names).
    fn prepare_mig(&self, ctx: &Context) -> Result<()> {
        self.reset_committing();
        self.persist_extra_backup_meta(ctx)?;
        let est = crate::stream::stream_metas::migration_extension(Arc::clone(&self.task_storage))
            .with_operation_context(self.operation_context.clone());
        let mut m = crate::stream::stream_metas::new_migration();
        m.ingested_sst_paths.push(self.meta_path());
        est.append_migration(ctx, &m)
            .map(|_| ())
            .map_err(|e| errors::annotate(e, format!("failed to add the extra backup at path {}", self.meta_path())))
    }

    /// Go `prepareMigIfNeeded`.
    fn prepare_mig_if_needed(&self, ctx: &Context) -> Result<()> {
        let mut prepared = self.migration_prepared.lock().unwrap();
        if *prepared {
            return Ok(());
        }
        self.prepare_mig(ctx)?;
        // A failed attempt doesn't let later batches skip publication.
        *prepared = true;
        Ok(())
    }

    /// Go `commit`.
    fn commit(&self, ctx: &Context) -> Result<u64> {
        self.ingested_sst_meta.lock().unwrap().msg.finished = true;
        let ts = (self.tso)(ctx)?;
        self.ingested_sst_meta.lock().unwrap().msg.as_if_ts = ts;
        self.persist_extra_backup_meta(ctx)?;
        Ok(ts)
    }
}

/// Go `verifyCompatibilityFor`.
fn verify_compatibility_for(file_set: &BackupFileSet) -> Result<()> {
    let Some(rules) = &file_set.rewrite_rules else { return Ok(()) };
    if !rules.new_keyspace.is_empty() {
        return Err(errors::annotatef(
            &ERR_UNSUPPORTED_OPERATION,
            "keyspace rewriting isn't supported when log backup enabled",
        ));
    }
    for (i, r) in rules.data.iter().enumerate() {
        if r.new_timestamp > 0 {
            return Err(errors::annotatef(
                &ERR_UNSUPPORTED_OPERATION,
                format!("rewrite rule #{i}: rewrite timestamp isn't supported when log backup enabled"),
            ));
        }
        if r.ignore_after_timestamp > 0 || r.ignore_before_timestamp > 0 {
            return Err(errors::annotatef(
                &ERR_UNSUPPORTED_OPERATION,
                format!("rewrite rule #{i}: truncating timestamp isn't supported when log backup enabled"),
            ));
        }
    }
    Ok(())
}

/// Go `onBatch`'s wait-done closure.
pub type WaitDone = Box<dyn FnOnce() -> Result<()> + Send>;

impl PitrCollector {
    /// Go `enabled`.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Sets Go `restoreUUID`.
    pub fn set_restore_uuid(&self, id: uuid::Uuid) {
        if let Some(inner) = &self.inner {
            *inner.restore_uuid.lock().unwrap() = id;
        }
    }

    /// The task-storage directory this restore writes, for tests and logs.
    #[must_use]
    pub fn output_path(&self) -> String {
        self.inner.as_ref().map(|i| i.output_path(&[])).unwrap_or_default()
    }

    /// Go `onBatch`: starts copying a batch's SSTs and recording its table
    /// rewrites; the returned closure waits for them and persists the meta.
    ///
    /// # Errors
    /// The migration cannot be published, or the batch is incompatible.
    pub fn on_batch(&self, ctx: &Context, file_sets: &[BackupFileSet]) -> Result<Option<WaitDone>> {
        let Some(inner) = &self.inner else { return Ok(None) };
        inner.prepare_mig_if_needed(ctx)?;
        let begin = Instant::now();
        let (eg, ectx) = ErrGroup::with_context(ctx);
        let mut file_count = 0;
        for file_set in file_sets {
            verify_compatibility_for(file_set)?;
            for file in &file_set.sst_files {
                file_count += 1;
                let (inner, ectx, file) = (Arc::clone(inner), ectx.clone(), Arc::clone(file));
                inner.conc_control.clone().apply_on_error_group(&eg, move || {
                    inner.put_sst(&ectx, &file).map_err(|e| errors::annotate(e, format!("failed to put sst {}", file.name)))
                });
            }
            for hint in file_set.rewrite_rules.iter().flat_map(|r| r.table_id_remap_hint.iter()) {
                let (inner, origin, rewritten) = (Arc::clone(inner), hint.origin, hint.rewritten);
                inner.conc_control.clone().apply_on_error_group(&eg, move || {
                    inner.put_rewrite_rule(origin, rewritten).map_err(|e| {
                        errors::annotate(e, format!("failed to put rewrite rule of {origin} -> {rewritten}"))
                    })
                });
            }
        }
        let (inner, ctx) = (Arc::clone(inner), ctx.clone());
        Ok(Some(Box::new(move || {
            if let Err(err) = eg.wait() {
                tidb_log::warn("Failed to upload SSTs for future PiTR.", &[Field::new("error", Value::Str(err.to_string()))]);
                return Err(err);
            }
            tidb_log::info(
                "Uploaded a batch of SSTs for future PiTR.",
                &[
                    Field::new("take", Value::Str(format!("{:?}", begin.elapsed()))),
                    Field::new("file-count", Value::U64(file_count)),
                ],
            );
            inner
                .persist_extra_backup_meta(&ctx)
                .map_err(|e| errors::annotate(e, "failed to persist backup meta when finishing batch"))
        })))
    }

    /// Go `close`: commits the SSTs once the restore succeeded, else leaves
    /// half-finished metadata.
    ///
    /// # Errors
    /// The meta cannot be persisted.
    pub fn close(&self) -> Result<()> {
        let Some(inner) = &self.inner else { return Ok(()) };
        let (cx, cancel) = Context::with_timeout(&Context::background(), Duration::from_secs(30));
        let res = if (inner.restore_success)() {
            match inner.commit(&cx) {
                Ok(commit_ts) => {
                    tidb_log::info(
                        "Log backup SSTs are committed.",
                        &[
                            Field::new("commitTS", Value::U64(commit_ts)),
                            Field::new("committedTo", Value::Str(inner.output_path(&[]))),
                        ],
                    );
                    Ok(())
                }
                Err(err) => Err(errors::annotate(err, "failed to commit pitrCollector")),
            }
        } else {
            tidb_log::warn(
                "Backup not success, put a half-finished metadata to the log backup.",
                &[Field::new("uuid", Value::Str(inner.restore_uuid.lock().unwrap().to_string()))],
            );
            inner.persist_extra_backup_meta(&cx).map_err(|e| errors::annotate(e, "failed to persist the meta"))
        };
        cancel.cancel();
        // Go `defer c.writerRoutine.close()`.
        inner.writer.lock().unwrap().take();
        res
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use super::*;
    use crate::restore_utils::{RewriteRules, TableIDRemap};

    fn tmp() -> (Arc<dyn Storage>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        (Arc::new(tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap()), dir)
    }

    /// Go `pitrCollectorT`.
    struct CollT {
        coll: PitrCollector,
        task: Arc<dyn Storage>,
        restore: Arc<dyn Storage>,
        tso_cnt: Arc<AtomicU64>,
        success: Arc<AtomicBool>,
        uuid: uuid::Uuid,
        op: crate::operation::Context,
        cx: Context,
        _dirs: (tempfile::TempDir, tempfile::TempDir),
    }

    /// Go `newPiTRCollForTest`.
    fn new_coll_for_test(name: &str) -> CollT {
        new_coll_with_task_storage(name, None)
    }

    /// Wraps the task storage of a test collector.
    type WrapStorage<'a> = &'a dyn Fn(Arc<dyn Storage>) -> Arc<dyn Storage>;

    fn new_coll_with_task_storage(name: &str, wrap: Option<WrapStorage<'_>>) -> CollT {
        let ((task, d1), (restore, d2)) = (tmp(), tmp());
        let task = wrap.map_or(Arc::clone(&task), |w| w(task));
        let mut op = crate::operation::Context::new("test pitr collector");
        op.set_hint_field("restore_id", "789");
        let (tso_cnt, success) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicBool::new(false)));
        let uuid = uuid::Uuid::new_v4();
        let mut t = CollT {
            coll: PitrCollector::default(),
            task,
            restore,
            tso_cnt,
            success,
            uuid,
            op,
            cx: Context::background(),
            _dirs: (d1, d2),
        };
        t.open(format!("test-{name}"));
        t
    }

    impl CollT {
        fn open(&mut self, name: String) {
            let (tso_cnt, success) = (Arc::clone(&self.tso_cnt), Arc::clone(&self.success));
            self.coll = new_for_test(
                Arc::clone(&self.task),
                Arc::clone(&self.restore),
                name,
                self.uuid,
                self.op.clone(),
                Box::new(move |_| Ok(tso_cnt.fetch_add(1, Ordering::SeqCst) + 1)),
                Box::new(move || success.load(Ordering::SeqCst)),
            );
        }

        /// Go `Reopen`.
        fn reopen(&mut self, name: &str) {
            self.success.store(false, Ordering::SeqCst);
            let n = self.tso_cnt.fetch_add(1, Ordering::SeqCst) + 1;
            self.open(format!("test-{name}-{n}"));
        }

        /// Go `StartRestoreBatch`.
        fn start_restore_batch(&self, fs: &[BackupFileSet]) -> Result<Option<WaitDone>> {
            for b in fs {
                for file in &b.sst_files {
                    self.restore.write_file(&self.cx, &file.name, b"something").map_err(objstore_err)?;
                }
            }
            self.coll.on_batch(&self.cx, fs)
        }

        fn must_start_restore_batch(&self, fs: &[BackupFileSet]) -> WaitDone {
            self.start_restore_batch(fs).unwrap().unwrap()
        }

        /// Go `Done`.
        fn done(&self) {
            self.coll.close().unwrap();
        }

        /// Go `ExtFullBkups`.
        fn ext_full_bkups(&self) -> Vec<IngestedSsTs> {
            let migs = crate::stream::stream_metas::migration_extension(Arc::clone(&self.task)).load(&self.cx).unwrap();
            let mut res = Vec::new();
            for m in migs.list_all() {
                for pth in &m.ingested_sst_paths {
                    let content = self.task.read_file(&self.cx, pth).unwrap();
                    res.push(IngestedSsTs::decode(content.as_slice()).unwrap());
                }
            }
            res
        }

        fn inner(&self) -> &Arc<Inner> {
            self.coll.inner.as_ref().unwrap()
        }

        /// Go `RequireCopied`.
        fn require_copied(&self, e: &IngestedSsTs, files: &[&str]) {
            let mut got: Vec<String> = e.files.iter().map(|f| f.name.clone()).collect();
            let mut want: Vec<String> = files.iter().map(|f| self.inner().sst_path(f)).collect();
            got.sort();
            want.sort();
            assert_eq!(got, want);
        }

        /// Go `RequireRewrite`.
        fn require_rewrite(&self, e: &IngestedSsTs, rules: &[(i64, i64)]) {
            let mut got: Vec<(i64, i64)> = e.rewritten_tables.iter().map(|r| (r.ancestor_upstream, r.upstream)).collect();
            let mut want = rules.to_vec();
            got.sort_unstable();
            want.sort_unstable();
            assert_eq!(got, want);
        }
    }

    /// Go `backupFileSet`.
    fn backup_file_set(files: &[&str], remaps: &[(i64, i64)]) -> BackupFileSet {
        BackupFileSet {
            table_id: 0,
            sst_files: files.iter().map(|n| Arc::new(File { name: (*n).to_owned(), ..Default::default() })).collect(),
            rewrite_rules: Some(Arc::new(RewriteRules {
                table_id_remap_hint: remaps.iter().map(|&(origin, rewritten)| TableIDRemap { origin, rewritten }).collect(),
                ..Default::default()
            })),
        }
    }

    /// Go `TestCollAFile`.
    #[test]
    fn test_coll_a_file() {
        let coll = new_coll_for_test("TestCollAFile");
        let complete = coll.must_start_restore_batch(&[backup_file_set(&["foo.txt"], &[])]);
        complete().unwrap();
        coll.success.store(true, Ordering::SeqCst);
        coll.done();
        let exts = coll.ext_full_bkups();
        assert_eq!(exts.len(), 1);
        coll.require_copied(&exts[0], &["foo.txt"]);
        assert!(exts[0].finished);
        assert_eq!(exts[0].backup_uuid, coll.uuid.as_bytes().to_vec());
    }

    /// Go `TestCollManyFileAndRewriteRules`.
    #[test]
    fn test_coll_many_file_and_rewrite_rules() {
        let coll = new_coll_for_test("TestCollManyFileAndRewriteRules");
        let batch = [
            backup_file_set(&["foo.txt"], &[]),
            backup_file_set(&["bar.txt"], &[(1, 10)]),
            backup_file_set(&["baz.txt"], &[(2, 20)]),
            backup_file_set(&["quux.txt"], &[(3, 21)]),
        ];
        coll.must_start_restore_batch(&batch)().unwrap();
        coll.success.store(true, Ordering::SeqCst);
        coll.done();
        let exts = coll.ext_full_bkups();
        assert_eq!(exts.len(), 1);
        coll.require_copied(&exts[0], &["foo.txt", "bar.txt", "baz.txt", "quux.txt"]);
        coll.require_rewrite(&exts[0], &[(1, 10), (2, 20), (3, 21)]);
        assert!(exts[0].finished);
        assert_eq!(exts[0].backup_uuid, coll.uuid.as_bytes().to_vec());
    }

    /// Go `TestReopen`: each reopened collector publishes its own meta;
    /// only the successful one is finished.
    #[test]
    fn test_reopen() {
        let mut coll = new_coll_for_test("TestReopen");
        let batch1 = [backup_file_set(&["foo.txt"], &[]), backup_file_set(&["bar.txt"], &[(1, 10)])];
        let batch2 = [backup_file_set(&["baz.txt"], &[(2, 20)])];
        let batch3 = [backup_file_set(&["quux.txt"], &[(3, 21)])];
        coll.must_start_restore_batch(&batch1)().unwrap();
        coll.done();
        let exts = coll.ext_full_bkups();
        assert_eq!(exts.len(), 1);
        coll.require_copied(&exts[0], &["foo.txt", "bar.txt"]);
        coll.require_rewrite(&exts[0], &[(1, 10)]);
        assert!(!exts[0].finished);

        coll.reopen("TestReopen");
        coll.must_start_restore_batch(&batch2)().unwrap();
        let exts = coll.ext_full_bkups();
        assert_eq!(exts.len(), 2);
        coll.require_copied(&exts[1], &["baz.txt"]);
        coll.require_rewrite(&exts[1], &[(2, 20)]);
        assert!(!exts[1].finished);
        coll.inner().writer.lock().unwrap().take();

        coll.reopen("TestReopen");
        coll.must_start_restore_batch(&batch3)().unwrap();
        coll.success.store(true, Ordering::SeqCst);
        coll.done();
        let exts = coll.ext_full_bkups();
        assert_eq!(exts.len(), 3);
        coll.require_copied(&exts[2], &["quux.txt"]);
        coll.require_rewrite(&exts[2], &[(3, 21)]);
        assert!(exts[2].finished);
        assert_eq!(exts[2].backup_uuid, coll.uuid.as_bytes().to_vec());
    }

    /// Go `TestConflict`.
    #[test]
    fn test_conflict() {
        let coll = new_coll_for_test("TestConflict");
        let batch = [backup_file_set(&["foo.txt"], &[(1, 10)]), backup_file_set(&["foo.txt"], &[(1, 11)])];
        let cb = coll.coll.on_batch(&coll.cx, &batch).unwrap().unwrap();
        assert!(cb().is_err());
        coll.done();
    }

    /// Go `TestPiTRCollectorPrepareMigWritesOperationMetadata`: the append
    /// lock carries this operation's id and hints.
    #[test]
    fn test_prepare_mig_writes_operation_metadata() {
        let writes = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let w = Arc::clone(&writes);
        let wrap = move |s: Arc<dyn Storage>| -> Arc<dyn Storage> { Arc::new(CaptureStorage { inner: s, writes: Arc::clone(&w), fail_write_path: Mutex::new(String::new()) }) };
        let coll = new_coll_with_task_storage("TestPiTRCollectorPrepareMigWritesOperationMetadata", Some(&wrap));
        coll.inner().prepare_mig(&coll.cx).unwrap();
        let metas: Vec<tidb_objstore::locking::LockMeta> = writes
            .lock()
            .unwrap()
            .iter()
            .filter_map(|b| serde_json::from_slice(b).ok())
            .filter(|m: &tidb_objstore::locking::LockMeta| m.lock_type == crate::operation::LOCK_RESOURCE_MIGRATION_APPEND)
            .collect();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].owner_id, coll.op.operation_id);
        assert!(metas[0].hint.contains("operation_started_at="), "{}", metas[0].hint);
        assert!(metas[0].hint.contains("restore_id=789"), "{}", metas[0].hint);
        coll.done();
    }

    /// Go `TestPiTRCollectorPrepareMigWritesOperationMetadata/retry failed
    /// preparation on the next batch`: a failed publication is retried.
    #[test]
    fn test_retry_failed_preparation_on_the_next_batch() {
        let holder: Arc<Mutex<Option<Arc<CaptureStorage>>>> = Arc::default();
        let h = Arc::clone(&holder);
        let wrap = move |s: Arc<dyn Storage>| -> Arc<dyn Storage> {
            let c = Arc::new(CaptureStorage { inner: s, writes: Arc::default(), fail_write_path: Mutex::new(String::new()) });
            *h.lock().unwrap() = Some(Arc::clone(&c));
            c
        };
        let coll = new_coll_with_task_storage("retry", Some(&wrap));
        let storage = holder.lock().unwrap().clone().unwrap();
        *storage.fail_write_path.lock().unwrap() = coll.inner().meta_path();
        let batch = [backup_file_set(&[], &[(1, 10)])];
        let err = coll.start_restore_batch(&batch).err().unwrap();
        assert!(err.to_string().contains("injected metadata write failure"), "{err}");
        assert!(coll.ext_full_bkups().is_empty());
        storage.fail_write_path.lock().unwrap().clear();
        coll.must_start_restore_batch(&batch)().unwrap();
        let ext = coll.ext_full_bkups();
        assert_eq!(ext.len(), 1);
        coll.require_rewrite(&ext[0], &[(1, 10)]);
        coll.must_start_restore_batch(&[backup_file_set(&[], &[(2, 20)])])().unwrap();
        let ext = coll.ext_full_bkups();
        assert_eq!(ext.len(), 1);
        coll.require_rewrite(&ext[0], &[(1, 10), (2, 20)]);
        coll.done();
    }

    /// Go `lockCaptureStorage`: records the writes and fails one path.
    struct CaptureStorage {
        inner: Arc<dyn Storage>,
        writes: Arc<Mutex<Vec<Vec<u8>>>>,
        fail_write_path: Mutex<String>,
    }

    impl Storage for CaptureStorage {
        fn write_file(&self, ctx: &Context, name: &str, data: &[u8]) -> tidb_objstore::error::Result<()> {
            if *self.fail_write_path.lock().unwrap() == name {
                return Err(tidb_objstore::Error::other("injected metadata write failure"));
            }
            self.writes.lock().unwrap().push(data.to_vec());
            self.inner.write_file(ctx, name, data)
        }
        fn read_file(&self, ctx: &Context, name: &str) -> tidb_objstore::error::Result<Vec<u8>> {
            self.inner.read_file(ctx, name)
        }
        fn file_exists(&self, ctx: &Context, name: &str) -> tidb_objstore::error::Result<bool> {
            self.inner.file_exists(ctx, name)
        }
        fn delete_file(&self, ctx: &Context, name: &str) -> tidb_objstore::error::Result<()> {
            self.inner.delete_file(ctx, name)
        }
        fn open(
            &self,
            ctx: &Context,
            path: &str,
            option: Option<&tidb_objstore::storeapi::ReaderOption>,
        ) -> tidb_objstore::error::Result<Box<dyn tidb_objstore::objectio::Reader>> {
            self.inner.open(ctx, path, option)
        }
        fn delete_files(&self, ctx: &Context, names: &[String]) -> tidb_objstore::error::Result<()> {
            self.inner.delete_files(ctx, names)
        }
        fn walk_dir(
            &self,
            ctx: &Context,
            opt: Option<&tidb_objstore::storeapi::WalkOption>,
            f: &mut dyn FnMut(&str, i64) -> tidb_objstore::error::Result<()>,
        ) -> tidb_objstore::error::Result<()> {
            self.inner.walk_dir(ctx, opt, f)
        }
        fn uri(&self) -> String {
            self.inner.uri()
        }
        fn create(
            &self,
            ctx: &Context,
            path: &str,
            option: Option<&tidb_objstore::storeapi::WriterOption>,
        ) -> tidb_objstore::error::Result<Box<dyn tidb_objstore::objectio::Writer>> {
            self.inner.create(ctx, path, option)
        }
        fn rename(&self, ctx: &Context, old_file_name: &str, new_file_name: &str) -> tidb_objstore::error::Result<()> {
            self.inner.rename(ctx, old_file_name, new_file_name)
        }
        fn presign_file(&self, ctx: &Context, file_name: &str, expire: Duration) -> tidb_objstore::error::Result<String> {
            self.inner.presign_file(ctx, file_name, expire)
        }
        fn as_copier(&self) -> Option<&dyn tidb_objstore::storeapi::Copier> {
            self.inner.as_copier()
        }
    }
}
