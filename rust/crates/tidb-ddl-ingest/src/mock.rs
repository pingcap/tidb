// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/ingest/mock.go`: transactional backend used only by test injection.
use crate::{
    Error, Result,
    backend::{BackendCtx, UnregisterOpt},
    checkpoint::CheckpointManager,
    engine::{IngestEngine, IngestWriter},
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tidb_gocontext::Context;
use tidb_store_driver_txn::{
    cluster_storage::MutationBuffer,
    cluster_table_storage::SessionTransaction,
    transaction::{RealOptimisticTransactionOpener, StoreClient},
};
/// Go `MockBackendCtx`.
pub struct MockBackendCtx<S: StoreClient> {
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    transaction: Mutex<Option<SessionTransaction<S>>>,
    buffer: MutationBuffer,
    checkpoint: Option<Arc<CheckpointManager>>,
}
impl<S: StoreClient> MockBackendCtx<S> {
    /// Go `NewMockBackendCtx`, bound to the test session's native transaction tier.
    pub fn new(
        opener: Arc<RealOptimisticTransactionOpener<S>>,
        timeout: Duration,
        checkpoint: Option<Arc<CheckpointManager>>,
    ) -> Self {
        Self {
            opener,
            timeout,
            transaction: Mutex::new(None),
            buffer: MutationBuffer::new(),
            checkpoint,
        }
    }
}
impl<S: StoreClient> BackendCtx for MockBackendCtx<S> {
    fn register(
        &self,
        index_ids: &[i64],
        _: &[bool],
        _: &tidb_model::TableInfo,
    ) -> Result<Vec<Arc<dyn IngestEngine>>> {
        let transaction = SessionTransaction::begin(
            self.opener.clone(),
            self.timeout,
            tidb_store_driver_txn::session_commit_protocol::SessionTransactionOptions::from(
                tidb_store_driver_txn::session_commit_protocol::bootstrap_commit_protocol(),
            ),
        )
        .map_err(|e| Error::Other(e.to_string()))?;
        self.buffer.reset();
        transaction.bind_mutation_buffer(&self.buffer);
        *self.transaction.lock().unwrap() = Some(transaction);
        let lock = Arc::new(Mutex::new(()));
        Ok(index_ids
            .iter()
            .map(|_| {
                Arc::new(MockEngineInfo {
                    buffer: self.buffer.clone(),
                    lock: lock.clone(),
                }) as Arc<dyn IngestEngine>
            })
            .collect())
    }
    fn finish_and_unregister_engines(&self, _: &Context, _: UnregisterOpt) -> Result<()> {
        if let Some(transaction) = self.transaction.lock().unwrap().take() {
            if let Err(error) = transaction.commit(&self.buffer) {
                tidb_log::warn(
                    "mock backend ctx unregister",
                    &[tidb_log::Field::new(
                        "error",
                        tidb_log::Value::Str(error.message.clone()),
                    )],
                );
            }
        }
        Ok(())
    }
    fn ingest_if_quota_exceeded(&self, _: &Context, id: usize, count: i64) -> Result<()> {
        if let Some(cp) = &self.checkpoint {
            cp.finish_chunk(id, count);
        }
        Ok(())
    }
    fn ingest(&self, _: &Context) -> Result<()> {
        self.checkpoint
            .as_ref()
            .map_or(Ok(()), |cp| cp.advance_watermark(true))
    }
    fn get_local_backend(&self) -> Option<Arc<tidb_ingestor_ingestctrl::local::Backend>> {
        None
    }
    fn next_start_key(&self) -> Vec<u8> {
        self.checkpoint
            .as_ref()
            .map_or_else(Vec::new, |cp| cp.next_start_key())
    }
    fn total_key_count(&self) -> i64 {
        self.checkpoint
            .as_ref()
            .map_or(0, |cp| cp.total_key_count())
    }
    fn add_chunk(&self, id: usize, end: Vec<u8>) {
        if let Some(cp) = &self.checkpoint {
            cp.add_chunk(id, end);
        }
    }
    fn update_chunk(&self, id: usize, count: i64, done: bool) {
        if let Some(cp) = &self.checkpoint {
            cp.update_chunk(id, count, done);
        }
    }
    fn finish_chunk(&self, id: usize, count: i64) {
        if let Some(cp) = &self.checkpoint {
            cp.finish_chunk(id, count);
        }
    }
    fn get_import_ts(&self) -> u64 {
        self.checkpoint.as_ref().map_or(0, |cp| cp.get_import_ts())
    }
    fn advance_watermark(&self, imported: bool) -> Result<()> {
        self.checkpoint
            .as_ref()
            .map_or(Ok(()), |cp| cp.advance_watermark(imported))
    }
    fn collect_remote_duplicate_rows(
        &self,
        _: &Context,
        _: i64,
        _: &tidb_model::TableInfo,
    ) -> Result<()> {
        Ok(())
    }
    fn get_disk_usage(&self) -> u64 {
        0
    }
    fn close(&self) {
        if let Some(cp) = &self.checkpoint {
            cp.close();
        }
    }
}
/// Go `MockEngineInfo`.
pub struct MockEngineInfo {
    buffer: MutationBuffer,
    lock: Arc<Mutex<()>>,
}
impl IngestEngine for MockEngineInfo {
    fn flush(&self) -> Result<()> {
        Ok(())
    }
    fn close(&self, _: bool) -> Result<()> {
        Ok(())
    }
    fn create_writer(self: Arc<Self>, _: usize, _: usize) -> Result<Box<dyn IngestWriter>> {
        Ok(Box::new(MockWriter { engine: self }))
    }
}
/// Go `MockWriter`.
pub struct MockWriter {
    engine: Arc<MockEngineInfo>,
}
impl IngestWriter for MockWriter {
    fn write_row(&mut self, key: &[u8], value: &[u8], _: &[u8]) -> Result<()> {
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/ddl/ingest/onMockWriterWriteRow",
            &mut (),
        );
        let _lock = self.engine.lock.lock().unwrap();
        self.engine
            .buffer
            .set(tidb_txnkv::Key::from_bytes(key.to_vec()), value.to_vec())
            .map_err(|e| Error::Other(e.to_string()))?;
        if let Some(hook) = MOCK_EXEC_AFTER_WRITE_ROW.lock().unwrap().as_ref() {
            hook();
        }
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/ddl/ingest/afterMockWriterWriteRow",
            &mut (),
        );
        Ok(())
    }
    fn written_bytes(&self) -> i64 {
        0
    }
}

/// Go `MockExecAfterWriteRow`: a test hook the mock writer calls after each
/// row it writes.
pub static MOCK_EXEC_AFTER_WRITE_ROW: std::sync::Mutex<Option<Box<dyn Fn() + Send>>> =
    std::sync::Mutex::new(None);
