//! Go `local.go`'s `Backend` methods behind `backend.Backend`: what the
//! lightning engine manager (and so IMPORT INTO) drives the local backend
//! through. Engines are keyed by their UUID's bytes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_lightning_backend as be;
use uuid::Uuid;

use crate::engine::Engine;
use crate::local::Backend;

/// Go `defaultRetryBackoffTime`.
const DEFAULT_RETRY_BACKOFF_TIME: Duration = Duration::from_secs(3);

static NEXT_WRITER_ID: AtomicUsize = AtomicUsize::new(1 << 32);

fn err(e: crate::Error) -> be::Error {
    let retryable = matches!(&e, crate::Error::Ingest(_)) && !tidb_ingestor_errdef::is_kv_disk_full_error(&e);
    be::Error { retryable, ..be::Error::from_cause(e) }
}

/// Go `localWriter`: one writer's rows into the engine.
struct LocalWriter {
    engine: Arc<Engine>,
    writer_id: usize,
    closed: bool,
}

impl be::EngineWriter for LocalWriter {
    fn append_rows(&mut self, _ctx: &Context, _column_names: &[String], rows: be::Rows) -> be::Result<()> {
        let rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = rows.into_kv_pairs().into_iter().map(|kv| (kv.key, kv.val, kv.row_id)).collect();
        self.engine.append_rows(self.writer_id, &rows).map_err(err)
    }

    fn is_synced(&self) -> bool {
        self.closed
    }

    fn close(&mut self, _ctx: &Context) -> be::Result<be::ChunkFlushStatus> {
        if !self.closed {
            self.engine.close_writer(self.writer_id).map_err(err)?;
            self.closed = true;
        }
        Ok(be::ChunkFlushStatus { flushed: true })
    }
}

impl Backend {
    fn engine_of(&self, id: Uuid) -> Option<Arc<Engine>> {
        self.engine(*id.as_bytes()).ok()
    }
}

impl be::Backend for Backend {
    fn close(&self) {
        let _ = Backend::close(self);
    }

    fn retry_import_delay(&self) -> Duration {
        DEFAULT_RETRY_BACKOFF_TIME
    }

    fn should_post_process(&self) -> bool {
        true
    }

    fn open_engine(&self, _ctx: &Context, config: &be::EngineConfig, engine_uuid: Uuid) -> be::Result<()> {
        let engine = Backend::open_engine(self, *engine_uuid.as_bytes(), config.ts).map_err(err)?;
        if engine.get_ts() == 0 && config.ts > 0 {
            engine.set_ts(config.ts).map_err(err)?;
        }
        Ok(())
    }

    /// Go `engineManager.closeEngine`: the global-sort engine of an external
    /// config; otherwise flushes the local engine and allocates its commit TS
    /// (`allocateTSIfNotExists`).
    fn close_engine(&self, _ctx: &Context, config: &be::EngineConfig, engine_uuid: Uuid) -> be::Result<()> {
        if let Some(external) = &config.external {
            return self.close_external_engine(*engine_uuid.as_bytes(), external, config.ts).map_err(err);
        }
        let Some(engine) = self.engine_of(engine_uuid) else { return Ok(()) };
        engine.flush().map_err(err)?;
        if engine.get_ts() == 0 {
            let ts = self.importer().get_ts().map_err(err)?;
            engine.set_ts(ts).map_err(err)?;
        }
        Ok(())
    }

    /// Go `Backend.ImportEngine`.
    fn import_engine(&self, ctx: &Context, engine_uuid: Uuid, region_split_size: i64, region_split_keys: i64) -> be::Result<()> {
        Backend::import_engine(
            self,
            ctx,
            *engine_uuid.as_bytes(),
            u64::try_from(region_split_size).unwrap_or(0),
            usize::try_from(region_split_keys).unwrap_or(0),
        )
        .map_err(err)
    }

    fn cleanup_engine(&self, _ctx: &Context, engine_uuid: Uuid) -> be::Result<()> {
        Backend::cleanup_engine(self, *engine_uuid.as_bytes()).map_err(err)
    }

    fn flush_engine(&self, _ctx: &Context, engine_uuid: Uuid) -> be::Result<()> {
        match self.engine_of(engine_uuid) {
            Some(engine) => engine.flush().map_err(err),
            None => Err(be::Error::other(format!("engine '{engine_uuid}' not found"))),
        }
    }

    fn flush_all_engines(&self, _ctx: &Context) -> be::Result<()> {
        Backend::flush_all_engines(self).map_err(err)
    }

    fn local_writer(&self, _ctx: &Context, _cfg: &be::LocalWriterConfig, engine_uuid: Uuid) -> be::Result<Box<dyn be::EngineWriter>> {
        let engine = self
            .engine_of(engine_uuid)
            .ok_or_else(|| be::Error::other(format!("could not find engine for {engine_uuid}")))?;
        Ok(Box::new(LocalWriter { engine, writer_id: NEXT_WRITER_ID.fetch_add(1, Ordering::SeqCst), closed: false }))
    }
}
