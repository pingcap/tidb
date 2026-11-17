// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/ingest/engine.go`.
use crate::{Error, Result, mem_root::MemRootImpl};
use std::collections::HashSet;
use std::sync::{Arc, Mutex, RwLock};
use tidb_ingestor_ingestctrl::{engine::Engine, local::Backend};

/// Go `engineInfo`.
pub struct EngineInfo {
    /// Go `indexID`.
    pub index_id: i64,
    /// Go `unique`.
    pub unique: bool,
    /// Go `uuid`.
    pub uuid: [u8; 16],
    engine: Arc<Engine>,
    backend: Arc<Backend>,
    mem_root: Arc<MemRootImpl>,
    flush_lock: RwLock<()>,
    opened: Mutex<bool>,
    writer_cache: Mutex<HashSet<usize>>,
}
impl EngineInfo {
    /// Go `newEngineInfo`.
    pub fn new(
        index_id: i64,
        unique: bool,
        uuid: [u8; 16],
        engine: Arc<Engine>,
        backend: Arc<Backend>,
        mem_root: Arc<MemRootImpl>,
    ) -> Self {
        Self {
            index_id,
            unique,
            uuid,
            engine,
            backend,
            mem_root,
            flush_lock: RwLock::new(()),
            opened: Mutex::new(true),
            writer_cache: Mutex::new(HashSet::new()),
        }
    }
    /// Go `engineInfo.Flush` and the caller's `flushLock`.
    pub fn flush(&self) -> Result<()> {
        let _guard = self.flush_lock.write().unwrap();
        if *self.opened.lock().unwrap() {
            self.engine.flush()?;
        }
        Ok(())
    }
    /// Go `engineInfo.Close`.
    pub fn close(&self, cleanup: bool) -> Result<()> {
        let _guard = self.flush_lock.write().unwrap();
        let mut opened = self.opened.lock().unwrap();
        if !*opened {
            return Ok(());
        }
        self.writer_cache.lock().unwrap().clear();
        match self.engine.close() {
            Ok(()) => *opened = false,
            Err(error) => tidb_log::warn(
                "close ingest engine failed",
                &[tidb_log::Field::new(
                    "error",
                    tidb_log::Value::Str(error.to_string()),
                )],
            ),
        }
        // Go defers cleanup even when closing writers or the engine fails.
        if cleanup {
            if let Err(error) = self.backend.cleanup_engine(self.uuid) {
                tidb_log::warn(
                    "clean ingest engine failed",
                    &[tidb_log::Field::new(
                        "error",
                        tidb_log::Value::Str(error.to_string()),
                    )],
                );
            }
        }
        Ok(())
    }
    /// Go `engineInfo.CreateWriter` / `newWriterContext`.
    pub fn create_writer(
        self: &Arc<Self>,
        id: usize,
        mem_cache_size: usize,
    ) -> Result<WriterContext> {
        self.mem_root.refresh_consumption();
        let mut cached = self.writer_cache.lock().unwrap();
        if !self
            .mem_root
            .check_consume(std::mem::size_of::<WriterContext>() as i64)
            || (!cached.contains(&id) && !self.mem_root.check_consume(mem_cache_size as i64))
        {
            return Err(Error::Database(
                tidb_dbterror::ERR_INGEST_FAILED.fast_gen_by_args(&["memory used up".into()]),
            ));
        }
        cached.insert(id);
        Ok(WriterContext {
            engine: self.clone(),
            id,
            written_bytes: 0,
        })
    }
    /// Go engine generation included in the current flush.
    pub fn flushed_generation(&self) -> Option<u64> {
        self.engine.flushed_generation()
    }
}
/// Go `writerContext`.
pub struct WriterContext {
    engine: Arc<EngineInfo>,
    id: usize,
    written_bytes: i64,
}
impl WriterContext {
    /// Go `writerContext.WriteRow` and `LockForWrite` around the append.
    pub fn write_row(&mut self, key: &[u8], value: &[u8], handle: &[u8]) -> Result<()> {
        let _guard = self.engine.flush_lock.read().unwrap();
        self.written_bytes += (key.len() + value.len()) as i64;
        self.engine
            .engine
            .append_rows(self.id, &[(key.to_vec(), value.to_vec(), handle.to_vec())])?;
        Ok(())
    }
    /// Go `writerContext.WrittenBytes`.
    pub fn written_bytes(&self) -> i64 {
        self.written_bytes
    }
}

/// Go `Engine`, implemented by the native local engine and Go's test backend.
pub trait IngestEngine: Send + Sync {
    /// Go `Engine.Flush`.
    fn flush(&self) -> Result<()>;
    /// Go `Engine.Close`.
    fn close(&self, cleanup: bool) -> Result<()>;
    /// Go `Engine.CreateWriter`.
    fn create_writer(
        self: Arc<Self>,
        id: usize,
        mem_cache_size: usize,
    ) -> Result<Box<dyn IngestWriter>>;
}
/// Go `Writer`.
pub trait IngestWriter: Send {
    /// Go `Writer.WriteRow`.
    fn write_row(&mut self, key: &[u8], value: &[u8], handle: &[u8]) -> Result<()>;
    /// Go `Writer.WrittenBytes`.
    fn written_bytes(&self) -> i64;
    /// Go `(*simplesst.Writer).Close`: only an external writer flushes and
    /// reports its files when it closes.
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
impl IngestEngine for EngineInfo {
    fn flush(&self) -> Result<()> {
        EngineInfo::flush(self)
    }
    fn close(&self, cleanup: bool) -> Result<()> {
        EngineInfo::close(self, cleanup)
    }
    fn create_writer(
        self: Arc<Self>,
        id: usize,
        mem_cache_size: usize,
    ) -> Result<Box<dyn IngestWriter>> {
        Ok(Box::new(EngineInfo::create_writer(
            &self,
            id,
            mem_cache_size,
        )?))
    }
}
impl IngestWriter for WriterContext {
    fn write_row(&mut self, key: &[u8], value: &[u8], handle: &[u8]) -> Result<()> {
        WriterContext::write_row(self, key, value, handle)
    }
    fn written_bytes(&self) -> i64 {
        WriterContext::written_bytes(self)
    }
}
