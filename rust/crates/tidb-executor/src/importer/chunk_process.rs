//! Go `pkg/executor/importer/chunk_process.go` and `engine_process.go`:
//! a chunk's rows read, encoded into KV pairs and delivered to the data and
//! index engine writers, with the chunk's KV checksums.
//!
//! As in Go, encoding and delivery run concurrently, joined by a bounded
//! queue of `maxKVQueueSize` batches.

use std::sync::mpsc;

use tidb_datatype::Datum;
use tidb_gocontext::Context;
use tidb_lightning_backend::EngineWriter;
use tidb_lightning_mydump::{CsvError, Parser};
use tidb_lightning_verification::{KvGroupChecksum, KvPair};

use super::kv_encode::TableKVEncoder;

/// Go `maxKVQueueSize`.
const MAX_KV_QUEUE_SIZE: usize = 32;
/// Go `DefaultMinDeliverBytes`.
pub const DEFAULT_MIN_DELIVER_BYTES: u64 = 96 * 1024;
/// Go `DefaultMinDeliverRowCnt`.
pub const DEFAULT_MIN_DELIVER_ROW_CNT: usize = 4096;

/// Go `Chunk`: one region of a data file, as a subtask carries it.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Chunk {
    /// Go `Path`.
    pub path: String,
    /// Go `FileSize`.
    pub file_size: i64,
    /// Go `Offset`.
    pub offset: i64,
    /// Go `EndOffset`.
    pub end_offset: i64,
    /// Go `PrevRowIDMax`.
    #[serde(rename = "PrevRowIDMax")]
    pub prev_row_id_max: i64,
    /// Go `RowIDMax`.
    #[serde(rename = "RowIDMax")]
    pub row_id_max: i64,
    /// Go `Type`, a `mydump.SourceType`.
    pub r#type: i32,
    /// Go `Compression`, a `mydump.Compression`.
    pub compression: i32,
    /// Go `Timestamp`.
    pub timestamp: i64,
    /// Go `ParquetMeta`.
    pub parquet_meta: tidb_lightning_mydump::ParquetFileMeta,
}

impl Chunk {
    /// Go `GetKey`.
    #[must_use]
    pub fn get_key(&self) -> String {
        format!("{}:{}", self.path, self.offset)
    }

    /// Go `GetSize`: a Parquet chunk's size is its file's.
    #[must_use]
    pub fn get_size(&self) -> i64 {
        if self.r#type == tidb_lightning_mydump::SourceType::Parquet as i32 {
            return self.file_size;
        }
        self.end_offset - self.offset
    }
}

/// A chunk failure: Go's error value, its chain kept (`errors.Annotatef`
/// over `ErrEncodeKV.Wrap(cause)`), so the DXF task error keeps the outer
/// error code and `common.IsRetryableError` sees the cause.
#[derive(Clone, Debug)]
pub struct ChunkError(pub std::sync::Arc<dyn std::error::Error + Send + Sync>);

/// A Go error without a code (`errors.New`).
#[derive(Debug)]
struct TextError(String);

impl std::fmt::Display for TextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TextError {}

impl From<String> for ChunkError {
    fn from(text: String) -> Self {
        Self(std::sync::Arc::new(TextError(text)))
    }
}

impl std::fmt::Display for ChunkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Go `common.ErrEncodeKV.Wrap(err).GenWithStackByArgs(file, offset)`.
pub(crate) fn encode_kv_error(
    cause: std::sync::Arc<dyn std::error::Error + Send + Sync>,
    file: &str,
    offset: i64,
) -> tidb_terror::TerrorError {
    tidb_lightning_common::errors::ERR_ENCODE_KV.wrap(cause).gen_with_stack_by_args(&[file.into(), offset.into()])
}

/// Go `rowToEncode`.
struct RowToEncode {
    row: Vec<Datum>,
    row_id: i64,
    /// The scanned file offset after the row, or -1 for a query source.
    end_offset: i64,
    start_pos: i64,
}

/// Go `encodeReaderFn`: the next row, or `None` when the chunk is done.
pub trait EncodeReader: Send {
    /// Reads the next row.
    fn read_row(&mut self, ctx: &Context) -> Result<Option<(Vec<Datum>, i64, i64, i64)>, ChunkError>;
}

/// Go `parserEncodeReader`: rows of a file chunk up to `end_offset`.
pub struct ParserEncodeReader<'a> {
    /// Go `parser`.
    pub parser: &'a mut (dyn Parser + Send),
    /// Go `endOffset`.
    pub end_offset: i64,
    /// Go `filename`: the chunk key errors name.
    pub filename: String,
}

impl EncodeReader for ParserEncodeReader<'_> {
    fn read_row(&mut self, _ctx: &Context) -> Result<Option<(Vec<Datum>, i64, i64, i64)>, ChunkError> {
        let (read_pos, _) = self.parser.pos();
        if read_pos >= self.end_offset {
            return Ok(None);
        }
        let result = self.parser.read_row();
        let curr_offset = self.parser.scanned_pos().unwrap_or(0);
        match result {
            Ok(()) => {}
            Err(CsvError::Eof) => return Ok(None),
            Err(e) => {
                let cause = std::sync::Arc::new(TextError(e.to_string()));
                return Err(ChunkError(std::sync::Arc::new(encode_kv_error(cause, &self.filename, read_pos))));
            }
        }
        let last = self.parser.last_row();
        Ok(Some((last.row.clone(), last.row_id, curr_offset, read_pos)))
    }
}

/// Go `QueryChunk`: a batch of rows a query source produced.
#[derive(Clone, Debug)]
pub struct QueryChunk {
    /// Go `Rows`.
    pub rows: Vec<Vec<Datum>>,
    /// Go `RowIDOffset`.
    pub row_id_offset: i64,
}

/// Go `queryChunkEncodeReader`. Every worker of an import reads the same
/// channel.
pub struct QueryChunkEncodeReader {
    /// Go `chunkCh`.
    pub chunk_ch: crossbeam_channel::Receiver<QueryChunk>,
    curr: Option<QueryChunk>,
    cursor: usize,
}

impl QueryChunkEncodeReader {
    /// Go `queryRowEncodeReader`.
    #[must_use]
    pub fn new(chunk_ch: crossbeam_channel::Receiver<QueryChunk>) -> Self {
        Self { chunk_ch, curr: None, cursor: 0 }
    }
}

impl EncodeReader for QueryChunkEncodeReader {
    fn read_row(&mut self, ctx: &Context) -> Result<Option<(Vec<Datum>, i64, i64, i64)>, ChunkError> {
        while self.curr.as_ref().is_none_or(|c| self.cursor >= c.rows.len()) {
            loop {
                if let Some(e) = ctx.err() {
                    return Err(e.to_string().into());
                }
                match self.chunk_ch.recv_timeout(std::time::Duration::from_millis(50)) {
                    Ok(chunk) => {
                        self.curr = Some(chunk);
                        self.cursor = 0;
                        break;
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return Ok(None),
                }
            }
        }
        let chunk = self.curr.as_ref().expect("a chunk");
        let row = chunk.rows[self.cursor].clone();
        self.cursor += 1;
        Ok(Some((row, chunk.row_id_offset + self.cursor as i64, -1, -1)))
    }
}

/// Go `encodedKVGroupBatch`.
pub struct EncodedKvGroupBatch {
    /// Go `dataKVs`.
    pub data_kvs: Vec<KvPair>,
    /// Index ID to its pairs, in index ID order.
    pub index_kvs: std::collections::BTreeMap<i64, Vec<KvPair>>,
    /// Go `groupChecksum`.
    pub group_checksum: KvGroupChecksum,
}

impl EncodedKvGroupBatch {
    pub(crate) fn new(keyspace: &[u8], count: usize) -> Self {
        Self {
            data_kvs: Vec::with_capacity(count),
            index_kvs: std::collections::BTreeMap::new(),
            group_checksum: KvGroupChecksum::with_keyspace(keyspace),
        }
    }

    /// Go `add`.
    pub(crate) fn add(&mut self, kvs: tidb_lightning_backend_kv::Pairs) -> Result<i64, String> {
        let mut kv_bytes = 0;
        for pair in kvs.pairs {
            kv_bytes += (pair.key.len() + pair.val.len()) as i64;
            if tidb_tablecodec::is_record_key(&pair.key) {
                self.group_checksum.update_one_data_kv(&pair);
                self.data_kvs.push(pair);
            } else {
                let index_id = tidb_tablecodec::table_key::decode_index_id(&pair.key).map_err(|e| format!("{e:?}"))?;
                self.group_checksum.update_one_index_kv(index_id, &pair);
                self.index_kvs.entry(index_id).or_default().push(pair);
            }
        }
        Ok(kv_bytes)
    }
}

/// Go `chunkEncoder` + `dataDeliver` + `baseChunkProcessor.Process`.
pub struct ChunkProcessor<'a> {
    /// Go `chunkName`.
    pub chunk_name: String,
    /// Go `readFn`.
    pub reader: Box<dyn EncodeReader + 'a>,
    /// The chunk's start offset (-1 for a query source).
    pub offset: i64,
    /// Go `encoder`.
    pub encoder: &'a mut TableKVEncoder,
    /// Go `keyspace`.
    pub keyspace: &'a [u8],
    /// Go `dataWriter`.
    pub data_writer: &'a mut dyn EngineWriter,
    /// Go `indexWriter`.
    pub index_writer: &'a mut dyn EngineWriter,
    /// Go `collector`.
    pub collector: Option<&'a dyn tidb_dxf_execute::Collector>,
    /// Go `MinDeliverBytes`.
    pub min_deliver_bytes: u64,
    /// Go `MinDeliverRowCnt`.
    pub min_deliver_row_cnt: usize,
}

impl<'a> ChunkProcessor<'a> {
    /// Go `NewFileChunkProcessor` / `newQueryChunkProcessor`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chunk_name: String,
        reader: Box<dyn EncodeReader + 'a>,
        offset: i64,
        encoder: &'a mut TableKVEncoder,
        keyspace: &'a [u8],
        data_writer: &'a mut dyn EngineWriter,
        index_writer: &'a mut dyn EngineWriter,
        collector: Option<&'a dyn tidb_dxf_execute::Collector>,
    ) -> Self {
        Self {
            chunk_name,
            reader,
            offset,
            encoder,
            keyspace,
            data_writer,
            index_writer,
            collector,
            min_deliver_bytes: DEFAULT_MIN_DELIVER_BYTES,
            min_deliver_row_cnt: DEFAULT_MIN_DELIVER_ROW_CNT,
        }
    }

    /// Go `baseChunkProcessor.Process`: the chunk's merged KV checksum is
    /// added to `group_checksum`.
    pub fn process(self, ctx: &Context, group_checksum: &mut KvGroupChecksum) -> Result<(), ChunkError> {
        let (eg_ctx, cancel) = Context::with_cancel(ctx);
        let (tx, rx) = mpsc::sync_channel::<EncodedKvGroupBatch>(MAX_KV_QUEUE_SIZE);
        let Self { chunk_name, mut reader, mut offset, encoder, keyspace, data_writer, index_writer, collector, min_deliver_bytes, min_deliver_row_cnt } = self;
        let mut chunk_checksum = KvGroupChecksum::with_keyspace(keyspace);
        let (enc_result, deliver_result) = std::thread::scope(|scope| {
            let deliver_ctx = eg_ctx.clone();
            let cancel_deliver = cancel.clone();
            let deliver = scope.spawn(move || {
                let result = deliver_loop(&deliver_ctx, rx, data_writer, index_writer);
                if result.is_err() {
                    cancel_deliver.cancel();
                }
                result
            });
            let enc = encode_loop(
                &eg_ctx,
                &chunk_name,
                reader.as_mut(),
                &mut offset,
                encoder,
                keyspace,
                tx,
                collector,
                min_deliver_bytes,
                min_deliver_row_cnt,
                &mut chunk_checksum,
            );
            if enc.is_err() {
                cancel.cancel();
            }
            (enc, deliver.join().unwrap_or_else(|_| Err("deliver loop panicked".to_owned())))
        });
        group_checksum.add(&chunk_checksum);
        // Go's errgroup returns the first failure; a cancellation the other
        // loop caused is not the cause.
        match (enc_result, deliver_result) {
            (Err(e), Ok(())) => Err(e),
            (Ok(()), Err(d)) => Err(d.into()),
            (Err(e), Err(d)) => Err(if e.to_string() == "context canceled" { d.into() } else { e }),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

/// Go `chunkEncoder.encodeLoop`.
#[allow(clippy::too_many_arguments)]
fn encode_loop(
    ctx: &Context,
    chunk_name: &str,
    reader: &mut dyn EncodeReader,
    offset: &mut i64,
    encoder: &mut TableKVEncoder,
    keyspace: &[u8],
    tx: mpsc::SyncSender<EncodedKvGroupBatch>,
    collector: Option<&dyn tidb_dxf_execute::Collector>,
    min_deliver_bytes: u64,
    min_deliver_row_cnt: usize,
    group_checksum: &mut KvGroupChecksum,
) -> Result<(), ChunkError> {
    let mut row_batch: Vec<tidb_lightning_backend_kv::Pairs> = Vec::with_capacity(min_deliver_row_cnt);
    let mut row_batch_byte_size = 0u64;
    let mut curr_offset = 0i64;
    let mut row_number = 0usize;
    let mut record_send_reset = |row_batch: &mut Vec<tidb_lightning_backend_kv::Pairs>, curr_offset: i64| -> Result<(), String> {
        if row_batch.is_empty() {
            return Ok(());
        }
        let mut delta = 0;
        if curr_offset >= 0 {
            delta = curr_offset - *offset;
            *offset = curr_offset;
        }
        if let Some(c) = collector {
            c.accepted(delta);
        }
        let row_count = row_batch.len();
        let mut batch = EncodedKvGroupBatch::new(keyspace, row_count);
        let mut total_kv_bytes = 0;
        for kvs in row_batch.drain(..) {
            total_kv_bytes += batch.add(kvs)?;
        }
        group_checksum.add(&batch.group_checksum);
        send(ctx, &tx, batch)?;
        if let Some(c) = collector {
            c.processed(total_kv_bytes, row_count as i64);
        }
        Ok(())
    };
    loop {
        let Some((row, row_id, end_offset, start_pos)) = reader.read_row(ctx)? else { break };
        row_number += 1;
        let data = RowToEncode { row, row_id, end_offset, start_pos };
        let kvs = encoder.encode(&data.row, data.row_id);
        curr_offset = data.end_offset;
        let kvs = kvs.map_err(|e| {
            let cause: std::sync::Arc<dyn std::error::Error + Send + Sync> = match e {
                tidb_lightning_backend_kv::EncodeError::Terror(e) => std::sync::Arc::new(e),
                other => std::sync::Arc::new(TextError(other.to_string())),
            };
            let wrapped = encode_kv_error(cause, chunk_name, data.start_pos);
            ChunkError(std::sync::Arc::new(tidb_terror::annotate(
                std::sync::Arc::new(wrapped),
                format!("when encoding {row_number}-th data row in this chunk"),
            )))
        })?;
        row_batch_byte_size += tidb_lightning_backend_encode::Row::size(&kvs);
        row_batch.push(kvs);
        if row_batch_byte_size >= min_deliver_bytes || row_batch.len() >= min_deliver_row_cnt {
            record_send_reset(&mut row_batch, curr_offset)?;
            row_batch_byte_size = 0;
        }
    }
    Ok(record_send_reset(&mut row_batch, curr_offset)?)
}

/// Go `dataDeliver.sendEncodedData`.
fn send(ctx: &Context, tx: &mpsc::SyncSender<EncodedKvGroupBatch>, mut batch: EncodedKvGroupBatch) -> Result<(), String> {
    loop {
        match tx.try_send(batch) {
            Ok(()) => return Ok(()),
            Err(mpsc::TrySendError::Full(b)) => {
                if let Some(e) = ctx.err() {
                    return Err(e.to_string());
                }
                batch = b;
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                return Err(ctx.err().map_or_else(|| "deliver loop stopped".to_owned(), |e| e.to_string()));
            }
        }
    }
}

/// Go `dataDeliver.deliverLoop`.
fn deliver_loop(
    ctx: &Context,
    rx: mpsc::Receiver<EncodedKvGroupBatch>,
    data_writer: &mut dyn EngineWriter,
    index_writer: &mut dyn EngineWriter,
) -> Result<(), String> {
    loop {
        let batch = match rx.recv_timeout(std::time::Duration::from_millis(50)) {
            Ok(batch) => batch,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Some(e) = ctx.err() {
                    return Err(e.to_string());
                }
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        };
        data_writer.append_rows(ctx, &[], batch.data_kvs).map_err(|e| e.to_string())?;
        let index_kvs: Vec<KvPair> = batch.index_kvs.into_values().flatten().collect();
        index_writer.append_rows(ctx, &[], index_kvs).map_err(|e| e.to_string())?;
    }
}

/// Go `WriterFactory`: the global-sort writer of an index.
pub type WriterFactory = Box<dyn FnMut(i64) -> Result<tidb_ingestor_simplesst::Writer, String> + Send>;

/// Go `IndexRouteWriter`: with global sort, routes each index KV to its
/// index's writer, made on first use.
pub struct IndexRouteWriter {
    writers: std::collections::HashMap<i64, tidb_ingestor_simplesst::Writer>,
    writer_factory: WriterFactory,
}

impl IndexRouteWriter {
    /// Go `NewIndexRouteWriter`.
    #[must_use]
    pub fn new(writer_factory: WriterFactory) -> Self {
        Self { writers: std::collections::HashMap::new(), writer_factory }
    }
}

impl EngineWriter for IndexRouteWriter {
    /// Go `AppendRows`. Go gets the pairs grouped by index ID; here they
    /// come flattened, so the ID is decoded from each key.
    fn append_rows(&mut self, ctx: &Context, _: &[String], rows: Vec<KvPair>) -> tidb_lightning_backend::Result<()> {
        for item in rows {
            let index_id = tidb_tablecodec::table_key::decode_index_id(&item.key)
                .map_err(|e| tidb_lightning_backend::Error::other(format!("{e:?}")))?;
            let writer = match self.writers.entry(index_id) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(e) => {
                    e.insert((self.writer_factory)(index_id).map_err(tidb_lightning_backend::Error::other)?)
                }
            };
            writer
                .write_row(ctx, &item.key, &item.val)
                .map_err(|e| tidb_lightning_backend::Error::other(e.to_string()))?;
        }
        Ok(())
    }

    fn is_synced(&self) -> bool {
        true
    }

    /// Go `Close`: closes every writer, returning the first failure.
    fn close(&mut self, ctx: &Context) -> tidb_lightning_backend::Result<tidb_lightning_backend::ChunkFlushStatus> {
        let mut first_err = None;
        for writer in self.writers.values_mut() {
            if let Err(e) = writer.close(ctx) {
                tidb_log::error("close index writer failed", &[tidb_log::Field::new("error", tidb_log::Value::Str(e.to_string()))]);
                first_err.get_or_insert(e);
            }
        }
        match first_err {
            Some(e) => Err(tidb_lightning_backend::Error::other(e.to_string())),
            None => Ok(tidb_lightning_backend::ChunkFlushStatus { flushed: true }),
        }
    }
}
