//! Go `pkg/executor/importer/engine_process.go`: one chunk processed into a
//! pair of engines.

use tidb_gocontext::Context;
use tidb_lightning_backend::{EngineWriter, LocalWriterConfig, OpenedEngine};
use tidb_lightning_verification::KvGroupChecksum;

use super::chunk_process::{Chunk, ChunkError, ChunkProcessor, ParserEncodeReader};
use super::table_import::TableImporter;

/// The rows a chunk processor reads: Go `TableImporter.DataSourceType`.
pub enum ChunkSource<'a> {
    /// Go `DataSourceTypeFile`: the chunk's file range.
    File,
    /// Go `DataSourceTypeQuery`: `TableImporter.chunkCh`.
    Query(&'a crossbeam_channel::Receiver<super::chunk_process::QueryChunk>),
}

/// Go `ProcessChunk`: opens the engines' writers and processes the chunk.
pub fn process_chunk(
    ctx: &Context,
    chunk: &Chunk,
    ti: &TableImporter,
    data_engine: &OpenedEngine<'_>,
    index_engine: &OpenedEngine<'_>,
    group_checksum: &mut KvGroupChecksum,
    collector: Option<&dyn tidb_dxf_execute::Collector>,
) -> Result<(), ChunkError> {
    process_chunk_from(ctx, chunk, &ChunkSource::File, ti, data_engine, index_engine, group_checksum, collector)
}

/// Go `ProcessChunk` over either data source.
#[allow(clippy::too_many_arguments)]
pub fn process_chunk_from(
    ctx: &Context,
    chunk: &Chunk,
    source: &ChunkSource<'_>,
    ti: &TableImporter,
    data_engine: &OpenedEngine<'_>,
    index_engine: &OpenedEngine<'_>,
    group_checksum: &mut KvGroupChecksum,
    collector: Option<&dyn tidb_dxf_execute::Collector>,
) -> Result<(), ChunkError> {
    let meta = ti.controller.table.meta().read().clone();
    let has_auto_increment_auto_id = tidb_lightning_common::table_has_auto_row_id(&meta)
        && meta.auto_random_bits == 0
        && meta.shard_row_id_bits == 0
        && meta.get_partition_info().is_none();
    let data_cfg = LocalWriterConfig { is_kv_sorted: has_auto_increment_auto_id, ..LocalWriterConfig::default() };
    let mut data_writer = data_engine.local_writer(ctx, &data_cfg).map_err(|e| e.to_string())?;
    let mut index_writer = index_engine.local_writer(ctx, &LocalWriterConfig::default()).map_err(|e| e.to_string())?;
    let result = match source {
        ChunkSource::File => {
            process_chunk_with_writer(ctx, chunk, ti, data_writer.as_mut(), index_writer.as_mut(), group_checksum, collector)
        }
        ChunkSource::Query(chunk_ch) => process_query_chunk_with_writer(
            ctx,
            chunk,
            (*chunk_ch).clone(),
            ti,
            data_writer.as_mut(),
            index_writer.as_mut(),
            group_checksum,
            collector,
        ),
    };
    let _ = data_writer.close(ctx);
    let _ = index_writer.close(ctx);
    result
}

/// Go `ProcessChunkWithWriter` for a file source.
pub fn process_chunk_with_writer(
    ctx: &Context,
    chunk: &Chunk,
    ti: &TableImporter,
    data_writer: &mut dyn EngineWriter,
    index_writer: &mut dyn EngineWriter,
    group_checksum: &mut KvGroupChecksum,
    collector: Option<&dyn tidb_dxf_execute::Collector>,
) -> Result<(), ChunkError> {
    let mut encoder = ti.get_kv_encoder(chunk)?;
    let mut parser = ti.controller.get_parser(ctx, chunk)?;
    let reader = ParserEncodeReader { parser: parser.as_mut(), end_offset: chunk.end_offset, filename: chunk.get_key() };
    ChunkProcessor::new(
        chunk.get_key(),
        Box::new(reader),
        chunk.offset,
        &mut encoder,
        ti.key_space(),
        data_writer,
        index_writer,
        collector,
    )
    .process(ctx, group_checksum)
}

/// Go `ProcessChunkWithWriter` for a query source (`newQueryChunkProcessor`).
#[allow(clippy::too_many_arguments)]
pub fn process_query_chunk_with_writer(
    ctx: &Context,
    chunk: &Chunk,
    chunk_ch: crossbeam_channel::Receiver<super::chunk_process::QueryChunk>,
    ti: &TableImporter,
    data_writer: &mut dyn EngineWriter,
    index_writer: &mut dyn EngineWriter,
    group_checksum: &mut KvGroupChecksum,
    collector: Option<&dyn tidb_dxf_execute::Collector>,
) -> Result<(), ChunkError> {
    let mut encoder = ti.get_kv_encoder(chunk)?;
    ChunkProcessor::new(
        "import-from-select".to_owned(),
        Box::new(super::chunk_process::QueryChunkEncodeReader::new(chunk_ch)),
        -1,
        &mut encoder,
        ti.key_space(),
        data_writer,
        index_writer,
        collector,
    )
    .process(ctx, group_checksum)
}
