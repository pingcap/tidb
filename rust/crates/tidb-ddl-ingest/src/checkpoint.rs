// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/ingest/checkpoint.go`: local and imported low watermarks.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::Duration;

/// Go `CheckpointStorage`.
pub trait CheckpointStorage: Send + Sync {
    /// Go `CheckpointStorage.LoadCheckpoint`.
    fn load_checkpoint(&self) -> Result<Option<ReorgCheckpoint>>;
    /// Go `CheckpointStorage.SaveCheckpoint`.
    fn save_checkpoint(&self, checkpoint: &ReorgCheckpoint) -> Result<()>;
}
/// Go `pd.Client.GetTS`, composed by `oracle.ComposeTS`.
pub trait TimestampOracle: Send + Sync {
    /// Go `GetTS`; returns the composed timestamp.
    fn get_ts(&self) -> Result<u64>;
}
impl<F: Fn() -> Result<u64> + Send + Sync> TimestampOracle for F {
    fn get_ts(&self) -> Result<u64> {
        self()
    }
}
/// Go `JobReorgMeta`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct JobReorgMeta {
    /// Go `Checkpoint`.
    pub reorg_checkpoint: Option<ReorgCheckpoint>,
}
/// Go `ReorgCheckpoint`. Go encodes `kv.Key` byte slices as base64.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ReorgCheckpoint {
    /// Go `LocalSyncKey`.
    #[serde(with = "key_json")]
    pub local_sync_key: Vec<u8>,
    /// Go `LocalKeyCount`.
    pub local_key_count: i64,
    /// Go `GlobalSyncKey`.
    #[serde(with = "key_json")]
    pub global_sync_key: Vec<u8>,
    /// Go `GlobalKeyCount`.
    pub global_key_count: i64,
    /// Go `InstanceAddr`.
    pub instance_addr: String,
    /// Go `PhysicalID`.
    pub physical_id: i64,
    /// Go `TS`, for the next engine import.
    pub ts: u64,
    /// Go `Version`.
    pub version: i64,
}
mod key_json {
    use super::*;
    use base64::Engine;
    pub fn serialize<S: serde::Serializer>(
        key: &[u8],
        s: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        if key.is_empty() {
            s.serialize_none()
        } else {
            s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(key))
        }
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> std::result::Result<Vec<u8>, D::Error> {
        let value = Option::<String>::deserialize(d)?;
        value
            .map(|s| {
                base64::engine::general_purpose::STANDARD
                    .decode(s)
                    .map_err(serde::de::Error::custom)
            })
            .transpose()
            .map(Option::unwrap_or_default)
    }
}
/// Go `JobCheckpointVersionCurrent`.
pub const JOB_CHECKPOINT_VERSION_CURRENT: i64 = 1;
#[derive(Default)]
struct TaskCheckpoint {
    total_keys: i64,
    written_keys: i64,
    end_key: Vec<u8>,
    last_batch_read: bool,
    chunks_total: usize,
    chunks_finished: usize,
}
#[derive(Default)]
struct State {
    checkpoints: BTreeMap<usize, TaskCheckpoint>,
    min_task_id_finished: usize,
    dirty: bool,
    local_data_is_valid: bool,
    persisted: ReorgCheckpoint,
}
struct Inner {
    state: Mutex<State>,
    storage: Arc<dyn CheckpointStorage>,
    oracle: Arc<dyn TimestampOracle>,
}
enum Update {
    Save(mpsc::SyncSender<()>),
    Stop,
}
/// Go `CheckpointManager`. The updater serializes checkpoint writes; writers
/// may complete chunks out of order, but only a contiguous prefix advances.
pub struct CheckpointManager {
    inner: Arc<Inner>,
    updater: mpsc::Sender<Update>,
    thread: Mutex<Option<JoinHandle<()>>>,
}
impl CheckpointManager {
    /// Go `newCheckpointManagerWithStorage` and `resumeOrInitCheckpoint`.
    pub fn new(
        storage: Arc<dyn CheckpointStorage>,
        physical_id: i64,
        local_store_dir: &Path,
        instance_addr: String,
        oracle: Arc<dyn TimestampOracle>,
    ) -> Result<Self> {
        let mut state = State::default();
        state.persisted.physical_id = physical_id;
        state.persisted.instance_addr = instance_addr.clone();
        state.persisted.version = JOB_CHECKPOINT_VERSION_CURRENT;
        match storage.load_checkpoint()? {
            Some(cp) if cp.physical_id == physical_id => {
                state.persisted.global_sync_key = cp.global_sync_key;
                state.persisted.global_key_count = cp.global_key_count;
                state.persisted.ts = cp.ts;
                let not_empty = std::fs::read_dir(local_store_dir)
                    .ok()
                    .and_then(|mut entries| entries.next())
                    .is_some();
                if not_empty && (cp.instance_addr.is_empty() || cp.instance_addr == instance_addr) {
                    state.local_data_is_valid = true;
                    state.persisted.local_sync_key = cp.local_sync_key;
                    state.persisted.local_key_count = cp.local_key_count;
                }
            }
            Some(_) => (), // Go returns without allocating for a physical-ID mismatch.
            None => {
                state.persisted.ts = oracle.get_ts()?;
                storage.save_checkpoint(&state.persisted)?;
            }
        }
        let inner = Arc::new(Inner {
            state: Mutex::new(state),
            storage,
            oracle,
        });
        let (updater, rx) = mpsc::channel();
        let worker = inner.clone();
        let thread = std::thread::Builder::new()
            .name("ingest-checkpoint".into())
            .spawn(move || {
                // Go `updateCheckpointLoop`.
                if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/ingest/checkpointLoopExit")
                    .is_some()
                {
                    return;
                }
                loop {
                    match rx.recv_timeout(Duration::from_secs(10)) {
                        Ok(Update::Save(done)) => {
                            let _ = update_checkpoint_impl(&worker);
                            let _ = done.send(());
                        }
                        Ok(Update::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            if worker.state.lock().unwrap().dirty {
                                let _ = update_checkpoint_impl(&worker);
                            }
                        }
                    }
                }
            })?;
        Ok(Self {
            inner,
            updater,
            thread: Mutex::new(Some(thread)),
        })
    }
    /// Go `CheckpointManager.IsKeyProcessed`.
    pub fn is_key_processed(&self, end: &[u8]) -> bool {
        let s = self.inner.state.lock().unwrap();
        (!s.persisted.global_sync_key.is_empty() && end <= s.persisted.global_sync_key.as_slice())
            || (s.local_data_is_valid
                && !s.persisted.local_sync_key.is_empty()
                && end <= s.persisted.local_sync_key.as_slice())
    }
    /// Go `CheckpointManager.NextStartKey`.
    pub fn next_start_key(&self) -> Vec<u8> {
        let s = self.inner.state.lock().unwrap();
        if s.local_data_is_valid && !s.persisted.local_sync_key.is_empty() {
            s.persisted.local_sync_key.clone()
        } else {
            s.persisted.global_sync_key.clone()
        }
    }
    /// Go `CheckpointManager.TotalKeyCount`.
    pub fn total_key_count(&self) -> i64 {
        let s = self.inner.state.lock().unwrap();
        s.persisted.local_key_count
            + s.checkpoints
                .values()
                .map(|cp| cp.written_keys)
                .sum::<i64>()
    }
    /// Go `CheckpointManager.AddChunk`.
    pub fn add_chunk(&self, task_id: usize, end: Vec<u8>) {
        self.inner.state.lock().unwrap().checkpoints.insert(
            task_id,
            TaskCheckpoint {
                end_key: end,
                ..Default::default()
            },
        );
    }
    /// Go `CheckpointManager.UpdateChunk`.
    pub fn update_chunk(&self, task_id: usize, delta: i64, last: bool) {
        let mut s = self.inner.state.lock().unwrap();
        let cp = s
            .checkpoints
            .get_mut(&task_id)
            .expect("unregistered ingest task");
        cp.total_keys += delta;
        cp.last_batch_read = last;
        cp.chunks_total += 1;
    }
    /// Go `CheckpointManager.FinishChunk`.
    pub fn finish_chunk(&self, task_id: usize, delta: i64) {
        let mut s = self.inner.state.lock().unwrap();
        if let Some(cp) = s.checkpoints.get_mut(&task_id) {
            cp.written_keys += delta;
            cp.chunks_finished += 1;
        }
    }
    /// Go `CheckpointManager.AdvanceWatermark`.
    pub fn advance_watermark(&self, imported: bool) -> Result<()> {
        {
            let s = self.inner.state.lock().unwrap();
            if s.checkpoints.is_empty() && s.min_task_id_finished == 0 {
                return Ok(());
            }
        }
        self.after_flush();
        if imported {
            self.after_import()?;
            self.update_checkpoint()?;
        }
        Ok(())
    }
    /// Go `CheckpointManager.afterFlush`.
    fn after_flush(&self) {
        let mut s = self.inner.state.lock().unwrap();
        loop {
            let id = s.min_task_id_finished;
            let Some(cp) = s.checkpoints.get(&id) else {
                break;
            };
            if !cp.last_batch_read
                || cp.written_keys < cp.total_keys
                || cp.chunks_finished < cp.chunks_total
            {
                break;
            }
            let cp = s.checkpoints.remove(&id).unwrap();
            s.min_task_id_finished += 1;
            s.persisted.local_sync_key = cp.end_key;
            s.persisted.local_key_count += cp.total_keys;
            s.dirty = true;
        }
    }
    /// Go `CheckpointManager.afterImport`.
    fn after_import(&self) -> Result<()> {
        let new_ts = self.inner.oracle.get_ts();
        if tidb_failpoint::eval(
            "github.com/pingcap/tidb/pkg/ddl/ingest/mockAfterImportAllocTSFailed",
        )
        .is_some()
        {
            return Err(Error::Other("mock err".into()));
        }
        let new_ts = new_ts?;
        let mut s = self.inner.state.lock().unwrap();
        if s.persisted.global_sync_key > s.persisted.local_sync_key {
            return Err(Error::Other("flushed key is less than imported key".into()));
        }
        s.persisted.global_sync_key = s.persisted.local_sync_key.clone();
        s.persisted.global_key_count = s.persisted.local_key_count;
        if s.persisted.ts < new_ts {
            s.persisted.ts = new_ts;
        }
        s.dirty = true;
        Ok(())
    }
    /// Go `CheckpointManager.GetImportTS`.
    pub fn get_import_ts(&self) -> u64 {
        self.inner.state.lock().unwrap().persisted.ts
    }
    /// Go `CheckpointManager.updateCheckpoint`. Go waits for the updater;
    /// persistence errors are logged there, not returned to the caller.
    pub fn update_checkpoint(&self) -> Result<()> {
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/ingest/checkpointLoopExit")
            .is_some()
        {
            return Err(Error::Other(
                "failpoint triggered so can't update checkpoint".into(),
            ));
        }
        let (done, wait) = mpsc::sync_channel(0);
        self.updater
            .send(Update::Save(done))
            .map_err(|_| Error::Other("context canceled".into()))?;
        wait.recv()
            .map_err(|_| Error::Other("context canceled".into()))
    }
    /// Go `CheckpointManager.Close`.
    pub fn close(&self) {
        let mut worker = self.thread.lock().unwrap();
        if let Some(thread) = worker.take() {
            let _ = self.update_checkpoint();
            let _ = self.updater.send(Update::Stop);
            let _ = thread.join();
        }
    }
}
impl Drop for CheckpointManager {
    fn drop(&mut self) {
        self.close();
    }
}
/// Go `CheckpointManager.updateCheckpointImpl`.
fn update_checkpoint_impl(inner: &Inner) -> Result<()> {
    let cp = inner.state.lock().unwrap().persisted.clone();
    inner.storage.save_checkpoint(&cp)?;
    inner.state.lock().unwrap().dirty = false;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Default)]
    struct Storage(Mutex<Option<ReorgCheckpoint>>);
    impl CheckpointStorage for Storage {
        fn load_checkpoint(&self) -> Result<Option<ReorgCheckpoint>> {
            Ok(self.0.lock().unwrap().clone())
        }
        fn save_checkpoint(&self, cp: &ReorgCheckpoint) -> Result<()> {
            *self.0.lock().unwrap() = Some(cp.clone());
            Ok(())
        }
    }
    fn oracle() -> Arc<dyn TimestampOracle> {
        let counter = std::sync::atomic::AtomicU64::new(0);
        Arc::new(move || {
            let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(((12 + n) << 18) | (34 + n))
        })
    }
    fn manager() -> (tempfile::TempDir, Arc<Storage>, CheckpointManager) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("test-file"), "").unwrap();
        let storage = Arc::new(Storage::default());
        let mgr = CheckpointManager::new(storage.clone(), 1, dir.path(), "node".into(), oracle())
            .unwrap();
        (dir, storage, mgr)
    }
    /// Go `checkpoint_test.go:TestCheckpointManager`, including all batch and
    /// out-of-order completion checks. Persistence is exercised by the updater.
    #[test]
    fn test_checkpoint_manager() {
        let (_dir, _storage, m) = manager();
        m.add_chunk(0, b"09".to_vec());
        m.add_chunk(1, b"19".to_vec());
        m.update_chunk(0, 100, false);
        assert!(!m.is_key_processed(b"09"));
        m.finish_chunk(0, 100);
        m.advance_watermark(false).unwrap();
        assert!(!m.is_key_processed(b"09"));
        m.update_chunk(0, 100, true);
        m.finish_chunk(0, 100);
        m.advance_watermark(false).unwrap();
        assert!(!m.is_key_processed(b"09"));
        m.finish_chunk(1, 0);
        m.advance_watermark(true).unwrap();
        assert!(m.is_key_processed(b"09"));
        m.update_chunk(1, 50, false);
        m.update_chunk(1, 50, true);
        m.finish_chunk(1, 50);
        m.advance_watermark(true).unwrap();
        assert!(m.is_key_processed(b"09"));
        assert!(!m.is_key_processed(b"19"));
        m.finish_chunk(1, 50);
        m.advance_watermark(true).unwrap();
        assert!(m.is_key_processed(b"09"));
        assert!(m.is_key_processed(b"19"));
        for i in 2..=4 {
            m.add_chunk(i, format!("{i}9").into_bytes());
            m.update_chunk(i, 100, true);
        }
        m.finish_chunk(4, 100);
        m.advance_watermark(true).unwrap();
        m.finish_chunk(3, 100);
        m.advance_watermark(true).unwrap();
        assert!(!m.is_key_processed(b"29"));
        assert!(!m.is_key_processed(b"39"));
        m.finish_chunk(2, 100);
        m.advance_watermark(true).unwrap();
        for key in [b"29", b"39", b"49"] {
            assert!(m.is_key_processed(key));
        }
        m.add_chunk(5, b"59".to_vec());
        m.update_chunk(5, 100, false);
        m.update_chunk(5, 0, true);
        m.finish_chunk(5, 100);
        m.advance_watermark(true).unwrap();
        m.finish_chunk(5, 0);
        m.advance_watermark(true).unwrap();
        assert!(m.is_key_processed(b"59"));
    }
    /// Go `checkpoint_test.go:TestCheckpointManagerUpdateReorg`. The storage
    /// boundary receives the same JSON and composed PD timestamp as Go's table.
    #[test]
    fn test_checkpoint_manager_update_reorg() {
        let (_dir, storage, m) = manager();
        m.add_chunk(0, b"19".to_vec());
        m.update_chunk(0, 100, true);
        m.finish_chunk(0, 100);
        m.advance_watermark(true).unwrap();
        m.close();
        let cp = storage.load_checkpoint().unwrap().unwrap();
        assert_eq!(cp.global_key_count, 100);
        assert_eq!(cp.local_key_count, 100);
        assert_eq!(cp.global_sync_key, b"19");
        assert_eq!(cp.local_sync_key, b"19");
        assert_eq!(cp.ts, (13 << 18) | 35);
        let raw = serde_json::to_value(JobReorgMeta {
            reorg_checkpoint: Some(cp),
        })
        .unwrap();
        assert_eq!(raw["reorg_checkpoint"]["global_sync_key"], "MTk=");
    }
    /// Go `checkpoint_test.go:TestCheckpointManagerResumeReorg`.
    #[test]
    fn test_checkpoint_manager_resume_reorg() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage(Mutex::new(Some(ReorgCheckpoint {
            local_sync_key: b"29".to_vec(),
            local_key_count: 100,
            global_sync_key: b"19".to_vec(),
            global_key_count: 200,
            physical_id: 1,
            instance_addr: "node".into(),
            version: 1,
            ts: 123456,
        }))));
        let m = CheckpointManager::new(storage.clone(), 1, dir.path(), "node".into(), oracle())
            .unwrap();
        assert!(m.is_key_processed(b"19"));
        assert!(!m.is_key_processed(b"29"));
        assert_eq!(m.total_key_count(), 0);
        assert_eq!(m.next_start_key(), b"19");
        assert_eq!(m.get_import_ts(), 123456);
        std::fs::write(dir.path().join("test-file"), "").unwrap();
        let m2 = CheckpointManager::new(storage, 1, dir.path(), "node".into(), oracle()).unwrap();
        assert!(m2.is_key_processed(b"19"));
        assert!(m2.is_key_processed(b"29"));
        assert_eq!(m2.total_key_count(), 100);
        assert_eq!(m2.next_start_key(), b"29");
        assert_eq!(m2.get_import_ts(), 123456);
    }
}

/// Go `NormalCheckpointStorage`, persisted in `mysql.tidb_ddl_reorg`.
pub struct NormalCheckpointStorage {
    /// Go `sessPool`.
    pub sess_pool: Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
    /// Go `jobID`.
    pub job_id: i64,
    /// Go `physicalID`.
    pub physical_id: i64,
}
impl CheckpointStorage for NormalCheckpointStorage {
    fn load_checkpoint(&self) -> Result<Option<ReorgCheckpoint>> {
        let mut session = self
            .sess_pool
            .open_sql_session()
            .map_err(|e| Error::Other(e.to_string()))?;
        session.begin().map_err(|e| Error::Other(e.to_string()))?;
        let result = (|| {
            let rows=session.execute(&format!("select reorg_meta from mysql.tidb_ddl_reorg where job_id = {} and ele_type = x'5f6964785f';",self.job_id),&[]).map_err(|e|Error::Other(e.to_string()))?;
            if rows.is_empty() || rows[0][0].is_null() {
                return Ok(None);
            }
            let meta: JobReorgMeta = serde_json::from_slice(rows[0][0].go_bytes())?;
            Ok(meta
                .reorg_checkpoint
                .filter(|cp| cp.physical_id == self.physical_id))
        })();
        match result {
            Ok(cp) => {
                session.commit().map_err(|e| Error::Other(e.to_string()))?;
                Ok(cp)
            }
            Err(e) => {
                session.rollback();
                Err(e)
            }
        }
    }
    fn save_checkpoint(&self, checkpoint: &ReorgCheckpoint) -> Result<()> {
        let mut session = self
            .sess_pool
            .open_sql_session()
            .map_err(|e| Error::Other(e.to_string()))?;
        session.begin().map_err(|e| Error::Other(e.to_string()))?;
        let result = (|| {
            let raw = serde_json::to_vec(&JobReorgMeta {
                reorg_checkpoint: Some(checkpoint.clone()),
            })?;
            session.execute(&format!("update mysql.tidb_ddl_reorg set reorg_meta = x'{}' where job_id = {} and ele_type = x'5f6964785f';",hex::encode(raw),self.job_id),&[]).map_err(|e|Error::Other(e.to_string()))?;
            Ok(())
        })();
        match result {
            Ok(()) => session.commit().map_err(|e| Error::Other(e.to_string())),
            Err(e) => {
                session.rollback();
                Err(e)
            }
        }
    }
}
/// Go `DistTaskCheckpointStorage`, persisted in DXF subtask checkpoints.
pub struct DistTaskCheckpointStorage {
    /// Go `subtaskID`.
    pub subtask_id: i64,
    /// Go `getFunc`; absent before the storage owner is supplied.
    pub get_func: Option<Arc<dyn Fn(i64) -> Result<String> + Send + Sync>>,
    /// Go `updateFunc`.
    pub update_func: Option<Arc<dyn Fn(i64, &ReorgCheckpoint) -> Result<()> + Send + Sync>>,
}
impl CheckpointStorage for DistTaskCheckpointStorage {
    fn load_checkpoint(&self) -> Result<Option<ReorgCheckpoint>> {
        let Some(get) = &self.get_func else {
            return Ok(None);
        };
        let json = get(self.subtask_id)?;
        if json.is_empty() || json == "{}" {
            return Ok(None);
        }
        Ok(Some(serde_json::from_str(&json)?))
    }
    fn save_checkpoint(&self, checkpoint: &ReorgCheckpoint) -> Result<()> {
        self.update_func
            .as_ref()
            .map_or(Ok(()), |update| update(self.subtask_id, checkpoint))
    }
}
