// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `byte_reader.go` and `concurrent_reader.go`: structured reads over a
//! storage reader, which can switch to reading large ranges of the file
//! concurrently.

use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_objstore::objectio::Reader as ObjReader;
use tidb_objstore::storeapi::{ReaderOption, Storage};

use crate::{Error, Result, UNEXPECTED_EOF};

/// Go `ConcurrentReaderBufferSizePerConc`: 8 MB.
pub static CONCURRENT_READER_BUFFER_SIZE_PER_CONC: AtomicUsize = AtomicUsize::new(8 * 1024 * 1024);

/// Sets [`CONCURRENT_READER_BUFFER_SIZE_PER_CONC`], as Go tests assign it.
pub fn set_concurrent_reader_buffer_size_per_conc(size: usize) {
    CONCURRENT_READER_BUFFER_SIZE_PER_CONC.store(size, Ordering::SeqCst);
}

pub(crate) fn concurrent_reader_buffer_size_per_conc() -> usize {
    CONCURRENT_READER_BUFFER_SIZE_PER_CONC.load(Ordering::SeqCst)
}

/// Go `concurrentReaderTotalConcurrency`.
pub(crate) const CONCURRENT_READER_TOTAL_CONCURRENCY: i64 = 256;

/// Go `size.GB`.
const GB: usize = 1 << 30;

/// Go `util.DefaultMaxRetries` and `util.RetryInterval` (ms).
const DEFAULT_MAX_RETRIES: u64 = 30;
const RETRY_INTERVAL_MS: u64 = 500;

fn io_err(e: &std::io::Error) -> Error {
    Error::Storage(tidb_objstore::Error::other(e.to_string()))
}

/// Go `openStoreReaderAndSeek`.
pub(crate) fn open_store_reader_and_seek(
    ctx: &Context,
    store: &dyn Storage,
    name: &str,
    init_file_offset: u64,
    prefetch_size: usize,
) -> Result<Box<dyn ObjReader>> {
    let opt = ReaderOption {
        start_offset: Some(i64::try_from(init_file_offset).unwrap_or(i64::MAX)),
        end_offset: None,
        prefetch_size,
    };
    Ok(store.open(ctx, name, Some(&opt))?)
}

/// Go `concurrentFileReader`.
struct ConcurrentFileReader {
    ctx: Context,
    concurrency: usize,
    read_buffer_size: usize,
    storage: Arc<dyn Storage>,
    name: String,
    offset: i64,
    file_size: i64,
}

impl ConcurrentFileReader {
    /// Go `read`: fills up to `concurrency` buffers from the current offset,
    /// concurrently; returns each buffer's filled length.
    fn read(&mut self, bufs: &mut [Vec<u8>]) -> Result<Vec<usize>> {
        if self.offset >= self.file_size {
            return Err(Error::Eof);
        }
        let mut jobs = Vec::new();
        for buf in bufs.iter_mut().take(self.concurrency) {
            if self.offset >= self.file_size {
                break;
            }
            let end = (self.read_buffer_size as i64).min(self.file_size - self.offset) as usize;
            jobs.push((self.offset, &mut buf[..end]));
            self.offset += end as i64;
        }
        let lens: Vec<usize> = jobs.iter().map(|(_, b)| b.len()).collect();
        let results: Vec<Result<()>> = std::thread::scope(|scope| {
            let handles: Vec<_> = jobs
                .into_iter()
                .map(|(offset, buf)| {
                    let (ctx, storage, name) = (&self.ctx, &self.storage, &self.name);
                    scope.spawn(move || {
                        let len = buf.len();
                        tidb_objstore::read_data_in_range(ctx, storage.as_ref(), name, offset, buf)
                            .map(|_| ())
                            .map_err(|e| {
                                Error::from(e)
                                    .annotate(format!("offset: {offset}, readSize: {len}"))
                            })
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| {
                    h.join()
                        .unwrap_or_else(|_| Err(Error::Other("read panicked".into())))
                })
                .collect()
        });
        for r in results {
            r?;
        }
        Ok(lens)
    }
}

/// Go `byteReader.concurrentReader`.
#[derive(Default)]
struct ConcurrentState {
    store: Option<Arc<dyn Storage>>,
    filename: String,
    concurrency: usize,
    buf_size_per_conc: usize,
    now: bool,
    expected: bool,
    large_buf: Vec<Vec<u8>>,
    large_lens: Vec<usize>,
    reader: Option<ConcurrentFileReader>,
    reload_cnt: usize,
}

/// Go `byteReader`.
pub(crate) struct ByteReader {
    ctx: Context,
    pub(crate) storage_reader: Box<dyn ObjReader>,
    /// Go `smallBuf`, and the length its last fill left.
    small_buf: Vec<u8>,
    small_len: usize,
    /// Go `curBufIdx` / `curBufOffset` over `curBuf`, which is the small
    /// buffer or the concurrent reader's large ones.
    cur_idx: usize,
    cur_offset: usize,
    /// The flattened result of a read that spans buffers.
    aux: Vec<u8>,
    concurrent: ConcurrentState,
    /// Go `mergeSortReadCounter`.
    pub(crate) merge_sort_read_counter: bool,
}

impl ByteReader {
    /// Go `newByteReader`: the first fill happens now, as the open already
    /// issued a GET.
    pub(crate) fn new(
        ctx: &Context,
        storage_reader: Box<dyn ObjReader>,
        buf_size: usize,
    ) -> Result<Self> {
        let mut r = Self {
            ctx: ctx.clone(),
            storage_reader,
            small_buf: vec![0; buf_size],
            small_len: buf_size,
            cur_idx: 0,
            cur_offset: 0,
            aux: Vec::new(),
            concurrent: ConcurrentState::default(),
            merge_sort_read_counter: false,
        };
        match r.reload() {
            Ok(()) => Ok(r),
            Err(e) => {
                let _ = r.close();
                Err(e)
            }
        }
    }

    fn buf_count(&self) -> usize {
        if self.concurrent.now {
            self.concurrent.large_lens.len()
        } else {
            1
        }
    }

    fn buf(&self, idx: usize) -> &[u8] {
        if self.concurrent.now {
            &self.concurrent.large_buf[idx][..self.concurrent.large_lens[idx]]
        } else {
            &self.small_buf[..self.small_len]
        }
    }

    /// Go `enableConcurrentRead`.
    pub(crate) fn enable_concurrent_read(
        &mut self,
        store: Arc<dyn Storage>,
        filename: &str,
        concurrency: usize,
        buf_size_per_conc: usize,
    ) {
        self.concurrent.store = Some(store);
        self.concurrent.filename = filename.to_owned();
        self.concurrent.concurrency = concurrency;
        self.concurrent.buf_size_per_conc = buf_size_per_conc;
    }

    /// Go `switchConcurrentMode`.
    pub(crate) fn switch_concurrent_mode(&mut self, use_concurrent: bool) -> Result<()> {
        if self.concurrent.store.is_none() {
            tidb_log::warn("concurrent reader is not enabled, skip switching", &[]);
            return Ok(());
        }
        // Set before reload; a concurrent reader is created lazily there.
        self.concurrent.expected = use_concurrent;
        if use_concurrent || !self.concurrent.now {
            return Ok(());
        }
        // Turn it off now to release memory. The large buffers were fully
        // loaded unless at the end of file, where no switch follows.
        let (reload_cnt, offset_in_old_buf) = self.close_concurrent_reader();
        let large_buf_size = self.concurrent.buf_size_per_conc * self.concurrent.concurrency;
        let delta = offset_in_old_buf as i64 + (reload_cnt as i64 - 1) * large_buf_size as i64;
        self.storage_reader
            .seek(SeekFrom::Current(delta))
            .map_err(|e| io_err(&e))?;
        match self.reload() {
            Err(e) if e.is_eof() => Ok(()),
            other => other,
        }
    }

    /// Go `switchToConcurrentReader`: only called once the storage reader's
    /// buffered data is used up, so its position is where to continue.
    fn switch_to_concurrent_reader(&mut self) -> Result<()> {
        let curr_offset = self
            .storage_reader
            .stream_position()
            .map_err(|e| io_err(&e))?;
        let file_size = self.storage_reader.get_file_size()?;
        let c = &mut self.concurrent;
        c.reader = Some(ConcurrentFileReader {
            ctx: self.ctx.clone(),
            concurrency: c.concurrency,
            read_buffer_size: c.buf_size_per_conc,
            storage: Arc::clone(c.store.as_ref().expect("enabled")),
            name: c.filename.clone(),
            offset: curr_offset as i64,
            file_size,
        });
        c.large_buf = (0..c.concurrency)
            .map(|_| vec![0; c.buf_size_per_conc])
            .collect();
        c.large_lens = Vec::new();
        self.cur_offset = 0;
        c.now = true;
        Ok(())
    }

    /// Go `readNBytes`: the next `n` bytes, valid until the next call.
    pub(crate) fn read_n_bytes(&mut self, n: usize) -> Result<&[u8]> {
        if n == 0 {
            return Err(Error::Other(format!(
                "illegal n ({n}) when reading from external storage"
            )));
        }
        if n > GB {
            return Err(Error::Other(format!(
                "read {n} bytes from external storage, exceed max limit {GB}"
            )));
        }
        if self.cur_idx < self.buf_count() {
            let len = self.buf(self.cur_idx).len();
            if self.cur_offset + n <= len {
                let (idx, start) = (self.cur_idx, self.cur_offset);
                self.cur_offset += n;
                if self.cur_offset == len {
                    self.cur_idx += 1;
                    self.cur_offset = 0;
                }
                return Ok(&self.buf(idx)[start..start + n]);
            }
        }
        let mut aux = std::mem::take(&mut self.aux);
        aux.clear();
        aux.reserve(n);
        let mut has_read = self.take_into(&mut aux, n) > 0;
        while aux.len() < n {
            if let Err(e) = self.reload() {
                if e.is_eof() && has_read {
                    // EOF is only allowed when nothing was read.
                    return Err(Error::UnexpectedEof(format!(
                        "file: {}: {UNEXPECTED_EOF}",
                        self.concurrent.filename
                    )));
                }
                return Err(e);
            }
            let rest = n - aux.len();
            has_read = self.take_into(&mut aux, rest) > 0 || has_read;
        }
        self.aux = aux;
        Ok(&self.aux)
    }

    /// Go `next`: appends up to `n` buffered bytes to `out`.
    fn take_into(&mut self, out: &mut Vec<u8>, mut n: usize) -> usize {
        let mut taken = 0;
        while self.cur_idx < self.buf_count() && n > 0 {
            let (idx, offset) = (self.cur_idx, self.cur_offset);
            let len = self.buf(idx).len();
            if offset + n <= len {
                out.extend_from_slice(&self.buf(idx)[offset..offset + n]);
                taken += n;
                self.cur_offset += n;
                if self.cur_offset == len {
                    self.cur_idx += 1;
                    self.cur_offset = 0;
                }
                break;
            }
            out.extend_from_slice(&self.buf(idx)[offset..]);
            taken += len - offset;
            n -= len - offset;
            self.cur_idx += 1;
            self.cur_offset = 0;
        }
        taken
    }

    /// Go `reload`.
    fn reload(&mut self) -> Result<()> {
        let result = self.reload_inner();
        if self.merge_sort_read_counter {
            let size: usize = (0..self.buf_count()).map(|i| self.buf(i).len()).sum();
            tidb_metrics::globalsort::MERGE_SORT_READ_BYTES.inc_by(size as f64);
        }
        result
    }

    fn reload_inner(&mut self) -> Result<()> {
        if !self.concurrent.now && self.concurrent.expected {
            tidb_log::info(
                "switch reader mode",
                &[tidb_log::Field::new(
                    "use concurrent mode",
                    tidb_log::Value::Bool(true),
                )],
            );
            self.switch_to_concurrent_reader()?;
        }
        if self.concurrent.now {
            self.concurrent.reload_cnt += 1;
            let c = &mut self.concurrent;
            let lens = c
                .reader
                .as_mut()
                .expect("concurrent reader")
                .read(&mut c.large_buf)?;
            c.large_lens = lens;
            self.cur_idx = 0;
            self.cur_offset = 0;
            return Ok(());
        }
        // Go `util.RunWithRetry(util.DefaultMaxRetries, util.RetryInterval, ...)`.
        let mut last = Ok(());
        for i in 1..=DEFAULT_MAX_RETRIES {
            let (retryable, result) = self.read_from_storage_reader();
            if result.is_ok() || !retryable {
                return result;
            }
            last = result;
            std::thread::sleep(Duration::from_millis(RETRY_INTERVAL_MS * i));
        }
        last
    }

    /// Go `readFromStorageReader`: `io.ReadFull` into the small buffer.
    fn read_from_storage_reader(&mut self) -> (bool, Result<()>) {
        let cap = self.small_buf.len();
        let mut n = 0;
        while n < cap {
            match self.storage_reader.read(&mut self.small_buf[n..cap]) {
                Ok(0) => break,
                Ok(m) => n += m,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => {
                    tidb_log::warn(
                        "other error during read",
                        &[tidb_log::Field::new(
                            "error",
                            tidb_log::Value::Str(e.to_string()),
                        )],
                    );
                    return (false, Err(io_err(&e)));
                }
            }
        }
        if n == 0 {
            // Go `io.EOF`: move past the buffer, so later reads see EOF too.
            self.cur_idx = 1;
            return (false, Err(Error::Eof));
        }
        // A short read is the last batch.
        self.small_len = n;
        self.cur_idx = 0;
        self.cur_offset = 0;
        (false, Ok(()))
    }

    /// Go `closeConcurrentReader`.
    fn close_concurrent_reader(&mut self) -> (usize, usize) {
        let c = &mut self.concurrent;
        let unread: usize = c.large_lens.len().saturating_sub(self.cur_idx);
        tidb_log::info(
            "drop data in closeConcurrentReader",
            &[
                tidb_log::Field::new("reloadCnt", tidb_log::Value::I64(c.reload_cnt as i64)),
                tidb_log::Field::new(
                    "dropBytes",
                    tidb_log::Value::I64(
                        (c.buf_size_per_conc * unread) as i64 - self.cur_offset as i64,
                    ),
                ),
                tidb_log::Field::new("curBufIdx", tidb_log::Value::I64(self.cur_idx as i64)),
            ],
        );
        c.large_buf = Vec::new();
        c.large_lens = Vec::new();
        c.now = false;
        let reload_cnt = std::mem::take(&mut c.reload_cnt);
        let offset_in_old = self.cur_offset + self.cur_idx * c.buf_size_per_conc;
        // Go `curBuf = [][]byte{r.smallBuf}`: the small buffer is consumed.
        self.cur_idx = 1;
        self.cur_offset = 0;
        (reload_cnt, offset_in_old)
    }

    /// Go `Close`.
    pub(crate) fn close(&mut self) -> Result<()> {
        if self.concurrent.now {
            self.close_concurrent_reader();
        }
        Ok(self.storage_reader.close()?)
    }
}
