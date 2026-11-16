//! Go `pkg/executor/importer/table_import.go`'s `TableImporter`: the
//! engines, encoders and parsers one import of one table runs on, over the
//! local backend.

use std::sync::Arc;

use tidb_gocontext::Context;
use tidb_lightning_backend::{ClosedEngine, EngineConfig, EngineManager, OpenedEngine, TableInfo};
use tidb_lightning_backend_encode::{EncodingConfig, SessionOptions};
use tidb_lightning_backend_kv::PanickingAllocators;
use tidb_lightning_mydump::{Compression, Parser, SourceFileMeta, SourceType};

use super::chunk_process::Chunk;
use super::kv_encode::TableKVEncoder;
use super::LoadDataController;

/// Go `common.IndexEngineID`.
pub const INDEX_ENGINE_ID: i32 = -1;

/// Go `TableImporter`.
pub struct TableImporter {
    /// Go's embedded `LoadDataController`.
    pub controller: LoadDataController,
    /// Go `id`.
    pub id: String,
    /// Go `backend`.
    pub backend: Arc<tidb_ingestor_ingestctrl::local::Backend>,
    /// Go `tableInfo`.
    pub table_info: TableInfo,
    /// Go `keyspace`.
    pub keyspace: Vec<u8>,
    /// Go `encTable`'s allocators.
    pub allocators: PanickingAllocators,
    /// Go `regionSplitSize`.
    pub region_split_size: i64,
    /// Go `regionSplitKeys`.
    pub region_split_keys: i64,
}

fn source_type_of(t: i32) -> SourceType {
    match t {
        x if x == SourceType::SQL as i32 => SourceType::SQL,
        x if x == SourceType::CSV as i32 => SourceType::CSV,
        x if x == SourceType::Parquet as i32 => SourceType::Parquet,
        _ => SourceType::Ignore,
    }
}

fn compression_of(c: i32) -> Compression {
    match c {
        1 => Compression::GZ,
        2 => Compression::LZ4,
        3 => Compression::ZStd,
        4 => Compression::XZ,
        5 => Compression::LZO,
        6 => Compression::Snappy,
        _ => Compression::None,
    }
}

impl Chunk {
    /// Go `toSourceFileMeta`.
    #[must_use]
    pub fn to_source_file_meta(&self) -> SourceFileMeta {
        SourceFileMeta {
            path: self.path.clone(),
            r#type: source_type_of(self.r#type),
            compression: compression_of(self.compression),
            file_size: self.file_size,
            parquet_meta: self.parquet_meta.clone(),
            ..SourceFileMeta::default()
        }
    }
}

impl LoadDataController {
    /// Go `FullTableName`: `` `db`.`table` ``.
    #[must_use]
    pub fn full_table_name(&self) -> String {
        format!(
            "`{}`.`{}`",
            self.plan.db_name.replace('`', "``"),
            self.table.meta().read().name.original().replace('`', "``")
        )
    }

    /// Go `getParser`: the chunk's parser, past the ignored lines of a
    /// chunk at the start of its file, else at the chunk's offset.
    pub fn get_parser(&self, ctx: &Context, chunk: &Chunk) -> Result<Box<dyn Parser + Send>, String> {
        let store = self.data_store.clone().ok_or_else(|| "the import has no data store".to_owned())?;
        let file_meta = chunk.to_source_file_meta();
        let mut parser = super::new_load_data_parser(ctx, &self.plan, &self.generate_csv_config(), &store, &file_meta)?;
        if chunk.offset == 0 {
            super::handle_skip_n_rows(parser.as_mut(), self.plan.ignore_lines).map_err(|e| e.to_string())?;
            parser.set_row_id(chunk.prev_row_id_max);
        } else {
            parser.set_pos(chunk.offset, chunk.prev_row_id_max).map_err(|e| e.to_string())?;
        }
        Ok(parser)
    }

    /// Go `getKVEncoder`.
    pub fn get_kv_encoder(&self, chunk: &Chunk, allocators: PanickingAllocators) -> Result<TableKVEncoder, String> {
        let cfg = EncodingConfig {
            session_options: SessionOptions {
                sql_mode: self.plan.sql_mode,
                timestamp: chunk.timestamp,
                sys_vars: self.plan.important_sys_vars.clone(),
                auto_random_seed: chunk.prev_row_id_max,
                ..SessionOptions::default()
            },
            path: chunk.path.clone(),
            table: self.table.meta().read().clone(),
            use_identity_auto_row_id: false,
        };
        TableKVEncoder::new(&cfg, self, allocators).map_err(|e| e.to_string())
    }
}

impl TableImporter {
    /// Go `getKVEncoder`.
    pub fn get_kv_encoder(&self, chunk: &Chunk) -> Result<TableKVEncoder, String> {
        self.controller.get_kv_encoder(chunk, self.allocators.clone())
    }

    /// Go `GetKVEncoderForDupResolve`.
    pub fn get_kv_encoder_for_dup_resolve(&self) -> Result<TableKVEncoder, String> {
        let cfg = EncodingConfig {
            session_options: SessionOptions {
                sql_mode: self.controller.plan.sql_mode,
                sys_vars: self.controller.plan.important_sys_vars.clone(),
                ..SessionOptions::default()
            },
            path: String::new(),
            table: self.controller.table.meta().read().clone(),
            use_identity_auto_row_id: true,
        };
        TableKVEncoder::new_for_dup_resolve(&cfg, &self.controller, self.allocators.clone()).map_err(|e| e.to_string())
    }

    /// Go `GetKeySpace`.
    #[must_use]
    pub fn key_space(&self) -> &[u8] {
        &self.keyspace
    }

    fn engine_manager(&self) -> EngineManager<'_> {
        EngineManager::new(self.backend.as_ref())
    }

    /// Go `OpenIndexEngine`. The Rust local engine has no compaction, so
    /// Go's compaction threshold is not computed.
    pub fn open_index_engine(&self, ctx: &Context, engine_id: i32) -> Result<OpenedEngine<'_>, String> {
        let cfg = EngineConfig { table_info: self.table_info.clone(), ..EngineConfig::default() };
        self.engine_manager()
            .open_engine(ctx, &cfg, &self.controller.full_table_name(), engine_id)
            .map_err(|e| e.to_string())
    }

    /// Go `OpenDataEngine`.
    pub fn open_data_engine(&self, ctx: &Context, engine_id: i32) -> Result<OpenedEngine<'_>, String> {
        let cfg = EngineConfig { table_info: self.table_info.clone(), ..EngineConfig::default() };
        self.engine_manager()
            .open_engine(ctx, &cfg, &self.controller.full_table_name(), engine_id)
            .map_err(|e| e.to_string())
    }

    /// Go `ImportAndCleanup`: imports and cleans the engine; returns the
    /// data engine's imported KV count.
    pub fn import_and_cleanup(&self, ctx: &Context, closed: &ClosedEngine<'_>) -> Result<i64, String> {
        let import_err = closed.import(ctx, self.region_split_size, self.region_split_keys).err();
        let kv_count = if closed.id() == INDEX_ENGINE_ID {
            0
        } else {
            self.backend.get_imported_kv_count(*closed.uuid().as_bytes())
        };
        let cleanup_err = closed.cleanup(ctx).err();
        match (import_err, cleanup_err) {
            (None, None) => Ok(kv_count),
            (Some(a), None) | (None, Some(a)) => Err(a.to_string()),
            (Some(a), Some(b)) => Err(format!("{a}; {b}")),
        }
    }

    /// Go `Close`.
    pub fn close(&self) {
        let _ = self.backend.close();
    }

    /// Go `Allocators`.
    #[must_use]
    pub fn allocators(&self) -> &PanickingAllocators {
        &self.allocators
    }
}
