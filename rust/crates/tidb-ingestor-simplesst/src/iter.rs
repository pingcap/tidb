// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `iter.go`: k-way merges over sorted readers, of KV pairs from data
//! files and of range properties from stat files.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::{Arc, Mutex};

use tidb_gocontext::Context;
use tidb_goruntime::go_heap;
use tidb_objstore::storeapi::Storage;

use crate::byte_reader::{
    concurrent_reader_buffer_size_per_conc, CONCURRENT_READER_TOTAL_CONCURRENCY,
};
use crate::codec::RangeProperty;
use crate::kv_reader::{KVReader, StatsReader};
use crate::writer::{str_field, MultipleFilesStat, MAX_MERGE_SORT_OVERLAP_THRESHOLD};
use crate::{Error, Result};

/// Go `heapElem`. Elements own their bytes, so Go's `cloneInnerFields` has
/// nothing to do.
pub(crate) trait HeapElem {
    fn sort_key(&self) -> &[u8];
    fn len(&self) -> usize;
}

/// Go `sortedReader`.
pub(crate) trait SortedReader {
    type Elem: HeapElem;
    fn path(&self) -> String;
    fn next(&mut self) -> Result<Self::Elem>;
    fn switch_concurrent_mode(&mut self, use_concurrent: bool) -> Result<()>;
    fn close(&mut self) -> Result<()>;
}

/// Go `readerOpenerFn`.
type ReaderOpener<R> = Box<dyn FnOnce() -> Result<R> + Send>;

/// Readers, `None` when empty, with their first elements.
type OpenedReaders<R> = (Vec<Option<R>>, Vec<Option<<R as SortedReader>::Elem>>);

/// Go `mergeHeapElem`.
struct MergeHeapElem<T> {
    elem: T,
    reader_idx: usize,
}

/// Go `mergeHeap.Less`.
fn heap_less<T: HeapElem>(a: &MergeHeapElem<T>, b: &MergeHeapElem<T>) -> bool {
    a.elem.sort_key() < b.elem.sort_key()
}

/// Go `heap.Push`.
fn heap_push<T: HeapElem>(h: &mut Vec<MergeHeapElem<T>>, x: MergeHeapElem<T>) {
    h.push(x);
    let n = h.len() - 1;
    go_heap::up(h, n, &mut heap_less);
}

/// Go `openAndGetFirstElem`: opens the readers concurrently and reads each
/// one's first element. Readers that are empty come back as `None`.
fn open_and_get_first_elem<R>(openers: Vec<ReaderOpener<R>>) -> Result<OpenedReaders<R>>
where
    R: SortedReader + Send,
{
    let opened: Vec<Result<R>> = if openers.len() == 1 {
        openers.into_iter().map(|f| f()).collect()
    } else {
        std::thread::scope(|scope| {
            let handles: Vec<_> = openers.into_iter().map(|f| scope.spawn(f)).collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap_or_else(|p| std::panic::resume_unwind(p)))
                .collect()
        })
    };
    let mut readers = Vec::with_capacity(opened.len());
    let mut first_err = None;
    for r in opened {
        match r {
            Ok(r) => readers.push(Some(r)),
            Err(e) if e.is_eof() => readers.push(None),
            Err(e) => {
                readers.push(None);
                first_err.get_or_insert(e);
            }
        }
    }
    let close_readers = |readers: &mut Vec<Option<R>>| {
        for r in readers.iter_mut().flatten() {
            let _ = r.close();
        }
    };
    if let Some(e) = first_err {
        close_readers(&mut readers);
        return Err(e);
    }
    let mut elements = Vec::with_capacity(readers.len());
    for j in 0..readers.len() {
        let Some(rd) = readers[j].as_mut() else {
            elements.push(None);
            continue;
        };
        match rd.next() {
            Ok(e) => elements.push(Some(e)),
            Err(e) if e.is_eof() => {
                let _ = rd.close();
                readers[j] = None;
                elements.push(None);
            }
            Err(e) => {
                close_readers(&mut readers);
                return Err(e);
            }
        }
    }
    Ok((readers, elements))
}

/// Go `mergeIter`: merges sorted readers through a min-heap. With
/// `check_hotspot`, a reader yielding over half of a period's elements
/// switches to concurrent reading.
pub(crate) struct MergeIter<R: SortedReader> {
    h: Vec<MergeHeapElem<R::Elem>>,
    readers: Vec<Option<R>>,
    curr: Option<R::Elem>,
    last_reader_idx: Option<usize>,
    err: Option<Error>,
    check_hotspot: bool,
    hotspot_map: HashMap<usize, usize>,
    check_hotspot_cnt: usize,
    check_hotspot_period: usize,
    /// Go starts it at 0 (the zero value), not -1.
    last_hotspot_idx: Option<usize>,
}

impl<R: SortedReader + Send> MergeIter<R> {
    /// Go `newMergeIter`.
    fn new(openers: Vec<ReaderOpener<R>>, check_hotspot: bool) -> Result<Self> {
        if openers.is_empty() {
            return Err(Error::Other("no reader openers".to_owned()));
        }
        let (readers, first_elements) = open_and_get_first_elem(openers)?;
        let mut h = Vec::with_capacity(readers.len());
        let (mut sample_key_size, mut sample_key_cnt) = (0, 0);
        for (j, e) in first_elements.into_iter().enumerate() {
            let Some(e) = e else { continue };
            sample_key_size += e.len();
            sample_key_cnt += 1;
            h.push(MergeHeapElem {
                elem: e,
                reader_idx: j,
            });
        }
        // Check about every 32 MB of data.
        let check_hotspot_period = if sample_key_size == 0 || sample_key_size / sample_key_cnt == 0
        {
            10000
        } else {
            1000.max((32 << 20) / (sample_key_size / sample_key_cnt))
        };
        go_heap::init(&mut h, &mut heap_less);
        Ok(Self {
            h,
            readers,
            curr: None,
            last_reader_idx: None,
            err: None,
            check_hotspot,
            hotspot_map: HashMap::new(),
            check_hotspot_cnt: 0,
            check_hotspot_period,
            last_hotspot_idx: Some(0),
        })
    }

    /// Go `close`: the first close error.
    fn close(&mut self) -> Result<()> {
        let mut first_err = None;
        for rd in self.readers.iter_mut() {
            let Some(r) = rd.as_mut() else { continue };
            if let Err(e) = r.close() {
                tidb_log::warn(
                    "failed to close reader",
                    &[
                        str_field("path", &r.path()),
                        str_field("error", &e.to_string()),
                    ],
                );
                first_err.get_or_insert(e);
            }
            *rd = None;
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Go `next`: whether an element is in `curr`, and the reader closed on
    /// the way.
    fn next(&mut self) -> (Option<usize>, bool) {
        let mut close_reader_idx = None;
        if let Some(last) = self.last_reader_idx {
            if self.check_hotspot {
                *self.hotspot_map.entry(last).or_default() += 1;
                self.check_hotspot_cnt += 1;
                if self.check_hotspot_cnt == self.check_hotspot_period {
                    // At most one reader can pass half of the period.
                    self.last_hotspot_idx = self
                        .hotspot_map
                        .iter()
                        .find(|(_, cnt)| **cnt > self.check_hotspot_period / 2)
                        .map(|(idx, _)| *idx);
                    for (idx, rd) in self.readers.iter_mut().enumerate() {
                        let Some(rd) = rd.as_mut() else { continue };
                        if let Err(e) =
                            rd.switch_concurrent_mode(self.last_hotspot_idx == Some(idx))
                        {
                            self.err = Some(e);
                            return (close_reader_idx, false);
                        }
                    }
                    self.check_hotspot_cnt = 0;
                    self.hotspot_map.clear();
                }
            }
            let rd = self.readers[last].as_mut().expect("last reader is open");
            match rd.next() {
                Ok(e) => heap_push(
                    &mut self.h,
                    MergeHeapElem {
                        elem: e,
                        reader_idx: last,
                    },
                ),
                Err(e) if e.is_eof() => {
                    if let Err(close_err) = rd.close() {
                        tidb_log::warn(
                            "failed to close reader",
                            &[
                                str_field("path", &rd.path()),
                                str_field("error", &close_err.to_string()),
                            ],
                        );
                    }
                    self.readers[last] = None;
                    self.hotspot_map.remove(&last);
                    close_reader_idx = Some(last);
                }
                Err(e) => {
                    tidb_log::error(
                        "failed to read next element",
                        &[
                            str_field("path", &rd.path()),
                            str_field("error", &e.to_string()),
                        ],
                    );
                    self.err = Some(e);
                    return (close_reader_idx, false);
                }
            }
        }
        self.last_reader_idx = None;
        let Some(curr) = go_heap::pop(&mut self.h, &mut heap_less) else {
            return (close_reader_idx, false);
        };
        self.curr = Some(curr.elem);
        self.last_reader_idx = Some(curr.reader_idx);
        (close_reader_idx, true)
    }
}

/// Go `limitSizeMergeIter`: keeps the sum of the open readers' weights
/// within `limit`, opening more as readers drain.
pub(crate) struct LimitSizeMergeIter<R: SortedReader> {
    iter: MergeIter<R>,
    reader_openers: Vec<Option<ReaderOpener<R>>>,
    weights: Vec<i64>,
    next_reader_idx: usize,
    weight_sum: i64,
    limit: i64,
}

impl<R: SortedReader + Send> LimitSizeMergeIter<R> {
    /// Go `newLimitSizeMergeIter`.
    fn new(reader_openers: Vec<ReaderOpener<R>>, weights: Vec<i64>, limit: i64) -> Result<Self> {
        if limit <= 0 {
            return Err(Error::Other(format!("limit must be positive, got {limit}")));
        }
        let (mut end, mut cur) = (0, 0);
        while end < weights.len() {
            if cur + weights[end] > limit {
                break;
            }
            cur += weights[end];
            end += 1;
        }
        let mut reader_openers: Vec<Option<ReaderOpener<R>>> =
            reader_openers.into_iter().map(Some).collect();
        let first = reader_openers[..end]
            .iter_mut()
            .map(|o| o.take().expect("unopened"))
            .collect();
        let iter = MergeIter::new(first, false)?;
        let mut ret = Self {
            iter,
            reader_openers,
            weights,
            weight_sum: cur,
            next_reader_idx: end,
            limit,
        };
        for (i, rd) in ret.iter.readers.iter().enumerate() {
            if rd.is_none() {
                ret.weight_sum -= ret.weights[i];
            }
        }
        if let Err(e) = ret.try_open_more_readers() {
            let _ = ret.iter.close();
            return Err(e);
        }
        Ok(ret)
    }

    /// Go `tryOpenMoreReaders`.
    fn try_open_more_readers(&mut self) -> Result<()> {
        while self.next_reader_idx < self.reader_openers.len() {
            let weight = self.weights[self.next_reader_idx];
            if self.weight_sum + weight > self.limit {
                return Ok(());
            }
            let opener = self.reader_openers[self.next_reader_idx]
                .take()
                .expect("unopened");
            self.next_reader_idx += 1;
            let (mut new_readers, mut first_elements) = open_and_get_first_elem(vec![opener])?;
            let new_reader_idx = self.iter.readers.len();
            let new_reader = new_readers.pop().expect("one reader");
            let is_open = new_reader.is_some();
            self.iter.readers.push(new_reader);
            if !is_open {
                continue;
            }
            let e = first_elements
                .pop()
                .flatten()
                .expect("first element of an open reader");
            self.iter.h.push(MergeHeapElem {
                elem: e,
                reader_idx: new_reader_idx,
            });
            let last = self.iter.h.len() - 1;
            go_heap::fix(&mut self.iter.h, last, &mut heap_less);
            self.weight_sum += weight;
        }
        Ok(())
    }

    /// Go `next`.
    fn next(&mut self) -> (bool, Option<usize>) {
        let (close_reader_idx, mut ok) = self.iter.next();
        let Some(closed) = close_reader_idx else {
            return (ok, close_reader_idx);
        };
        let drained = !ok && self.iter.h.is_empty();
        self.weight_sum -= self.weights[closed];
        if let Err(e) = self.try_open_more_readers() {
            self.iter.err = Some(e);
            return (false, close_reader_idx);
        }
        if drained && !self.iter.h.is_empty() {
            (_, ok) = self.iter.next();
        }
        (ok, close_reader_idx)
    }
}

/// Go `KVPair`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KVPair {
    /// Go `Key`.
    pub key: Vec<u8>,
    /// Go `Value`.
    pub value: Vec<u8>,
}

impl HeapElem for KVPair {
    fn sort_key(&self) -> &[u8] {
        &self.key
    }

    fn len(&self) -> usize {
        self.key.len() + self.value.len()
    }
}

/// Go `kvReaderProxy`.
struct KvReaderProxy {
    p: String,
    r: KVReader,
}

impl SortedReader for KvReaderProxy {
    type Elem = KVPair;

    fn path(&self) -> String {
        self.p.clone()
    }

    fn next(&mut self) -> Result<KVPair> {
        let (key, value) = self.r.next_kv()?;
        Ok(KVPair { key, value })
    }

    fn switch_concurrent_mode(&mut self, use_concurrent: bool) -> Result<()> {
        self.r.switch_concurrent_mode(use_concurrent)
    }

    fn close(&mut self) -> Result<()> {
        self.r.close()
    }
}

/// Go `MergeKVIter`: merges KV pairs from data files.
pub struct MergeKVIter {
    iter: MergeIter<KvReaderProxy>,
    input_size: i64,
}

impl MergeKVIter {
    /// Go `NewMergeKVIter`: reads `paths[i]` from `paths_start_offset[i]`.
    /// `reader_memory_size` bounds the concurrent reader of a hotspot file.
    ///
    /// # Errors
    /// A file could not be opened or read.
    pub fn new(
        ctx: &Context,
        paths: &[String],
        paths_start_offset: &[u64],
        ex_storage: Arc<dyn Storage>,
        read_buffer_size: usize,
        check_hotspot: bool,
        reader_memory_size: i64,
    ) -> Result<Self> {
        let concurrency = get_concurrent_reader_concurrency(reader_memory_size);
        let input_size = Arc::new(AtomicI64::new(0));
        let openers: Vec<ReaderOpener<KvReaderProxy>> = paths
            .iter()
            .zip(paths_start_offset)
            .map(|(path, start)| {
                let (ctx, store, path, start, input_size) = (
                    ctx.clone(),
                    Arc::clone(&ex_storage),
                    path.clone(),
                    *start,
                    Arc::clone(&input_size),
                );
                Box::new(move || {
                    let mut rd =
                        KVReader::new(&ctx, &path, store.as_ref(), start, read_buffer_size)?;
                    let file_size = match rd.get_file_size() {
                        Ok(s) => s,
                        Err(e) => {
                            let _ = rd.close();
                            return Err(e);
                        }
                    };
                    input_size.fetch_add(file_size - start as i64, Ordering::SeqCst);
                    rd.byte_reader.merge_sort_read_counter = true;
                    if concurrency > 0 {
                        rd.enable_concurrent_read(
                            store,
                            &path,
                            concurrency,
                            concurrent_reader_buffer_size_per_conc(),
                        );
                    }
                    Ok(KvReaderProxy { p: path, r: rd })
                }) as ReaderOpener<KvReaderProxy>
            })
            .collect();
        let iter = MergeIter::new(openers, check_hotspot && concurrency > 0)?;
        Ok(Self {
            iter,
            input_size: input_size.load(Ordering::SeqCst),
        })
    }

    /// Go `InputSize`: bytes left to read when the iterator was created.
    #[must_use]
    pub fn input_size(&self) -> i64 {
        self.input_size
    }

    /// Go `Error`.
    #[must_use]
    pub fn error(&self) -> Option<&Error> {
        self.iter.err.as_ref()
    }

    /// Go `Next`.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        self.iter.next().1
    }

    /// Go `Key`.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.iter.curr.as_ref().expect("current pair").key
    }

    /// Go `Value`.
    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.iter.curr.as_ref().expect("current pair").value
    }

    /// Go `Close`.
    ///
    /// # Errors
    /// A reader failed to close.
    pub fn close(&mut self) -> Result<()> {
        self.iter.close()
    }
}

/// Go `getConcurrentReaderConcurrency`.
fn get_concurrent_reader_concurrency(reader_memory_size: i64) -> usize {
    let concurrency = (reader_memory_size / concurrent_reader_buffer_size_per_conc() as i64).max(0);
    concurrency.min(CONCURRENT_READER_TOTAL_CONCURRENCY) as usize
}

impl HeapElem for RangeProperty {
    fn sort_key(&self) -> &[u8] {
        &self.first_key
    }

    fn len(&self) -> usize {
        // 24 for the offset, size and keys.
        self.first_key.len() + self.last_key.len() + 24
    }
}

/// Go `statReaderProxy`.
struct StatReaderProxy {
    p: String,
    r: StatsReader,
}

impl SortedReader for StatReaderProxy {
    type Elem = RangeProperty;

    fn path(&self) -> String {
        self.p.clone()
    }

    fn next(&mut self) -> Result<RangeProperty> {
        self.r.next_prop()
    }

    fn switch_concurrent_mode(&mut self, _: bool) -> Result<()> {
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        self.r.close()
    }
}

/// Go `errMergePropBaseIterClosed`.
fn err_merge_prop_base_iter_closed() -> Error {
    Error::Other("mergePropBaseIter is closed".to_owned())
}

/// A pre-opened stat reader, or the error opening it.
type PreOpened = Receiver<Result<StatReaderProxy>>;

/// Go `mergePropBaseIter`: merges the properties of one
/// [`MultipleFilesStat`], opening up to twice the overlap ahead.
struct MergePropBaseIter {
    iter: LimitSizeMergeIter<StatReaderProxy>,
    close_reader_flag: Option<Arc<AtomicBool>>,
    /// Go `closeCh`: dropping the receiver stops the pre-opening.
    pre_open: Arc<Mutex<Option<Receiver<PreOpened>>>>,
    /// Go `wg`.
    producer: Option<std::thread::JoinHandle<()>>,
}

impl MergePropBaseIter {
    /// Go `newMergePropBaseIter`.
    fn new(
        ctx: &Context,
        multi_stat: &MultipleFilesStat,
        ex_storage: &Arc<dyn Storage>,
    ) -> Result<Self> {
        let files = multi_stat.filenames.len();
        let limit = if multi_stat.max_overlapping_num <= 0 {
            files as i64
        } else {
            multi_stat.max_overlapping_num + 1
        };
        let limit = limit.min(files as i64) as usize;
        let pre_open_limit = (limit * 2).min(files);
        let (pre_open_tx, pre_open_rx) = sync_channel::<PreOpened>(pre_open_limit - limit);
        let pre_open = Arc::new(Mutex::new(Some(pre_open_rx)));

        let paths: Vec<String> = multi_stat.filenames.iter().map(|f| f[1].clone()).collect();
        let producer = {
            let (ctx, store, paths) =
                (ctx.clone(), Arc::clone(ex_storage), paths[limit..].to_vec());
            std::thread::spawn(move || {
                let mut openers = Vec::new();
                for path in paths {
                    let (async_tx, async_rx) = sync_channel(1);
                    let (ctx, store) = (ctx.clone(), Arc::clone(&store));
                    openers.push(std::thread::spawn(move || {
                        let rd = StatsReader::new(&ctx, store.as_ref(), &path, 250 * 1024)
                            .map(|r| StatReaderProxy { p: path, r });
                        // The iterator was closed: close what was opened.
                        if let Err(std::sync::mpsc::SendError(Ok(mut rd))) = async_tx.send(rd) {
                            let _ = SortedReader::close(&mut rd);
                        }
                    }));
                    // A dropped receiver closes the buffered readers too.
                    if pre_open_tx.send(async_rx).is_err() {
                        break;
                    }
                }
                drop(pre_open_tx);
                for h in openers {
                    let _ = h.join();
                }
            })
        };

        let mut openers: Vec<ReaderOpener<StatReaderProxy>> = Vec::with_capacity(files);
        for path in &paths[..limit] {
            let (ctx, store, path) = (ctx.clone(), Arc::clone(ex_storage), path.clone());
            openers.push(Box::new(move || {
                let r = StatsReader::new(&ctx, store.as_ref(), &path, 250 * 1024)?;
                Ok(StatReaderProxy { p: path, r })
            }));
        }
        for _ in limit..files {
            let pre_open = Arc::clone(&pre_open);
            openers.push(Box::new(move || {
                let guard = pre_open.lock().expect("lock");
                let rx = guard.as_ref().ok_or_else(err_merge_prop_base_iter_closed)?;
                let async_rx = rx.recv().map_err(|_| err_merge_prop_base_iter_closed())?;
                async_rx
                    .recv()
                    .map_err(|_| err_merge_prop_base_iter_closed())?
            }));
        }
        let weights = vec![1; openers.len()];
        match LimitSizeMergeIter::new(openers, weights, limit as i64) {
            Ok(iter) => Ok(Self {
                iter,
                close_reader_flag: None,
                pre_open,
                producer: Some(producer),
            }),
            Err(e) => {
                *pre_open.lock().expect("lock") = None;
                let _ = producer.join();
                Err(e)
            }
        }
    }
}

impl SortedReader for MergePropBaseIter {
    type Elem = RangeProperty;

    fn path(&self) -> String {
        "mergePropBaseIter".to_owned()
    }

    fn next(&mut self) -> Result<RangeProperty> {
        let (ok, close_reader_idx) = self.iter.next();
        if let (Some(flag), Some(_)) = (&self.close_reader_flag, close_reader_idx) {
            flag.store(true, Ordering::SeqCst);
        }
        if !ok {
            return Err(self.iter.iter.err.take().unwrap_or(Error::Eof));
        }
        Ok(self.iter.iter.curr.clone().expect("current property"))
    }

    fn switch_concurrent_mode(&mut self, _: bool) -> Result<()> {
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        *self.pre_open.lock().expect("lock") = None;
        if let Some(h) = self.producer.take() {
            let _ = h.join();
        }
        self.iter.iter.close()
    }
}

/// Go `MergePropIter`: merges the range properties of many
/// [`MultipleFilesStat`]s.
pub struct MergePropIter {
    iter: LimitSizeMergeIter<MergePropBaseIter>,
    base_close_reader_flag: Arc<AtomicBool>,
}

impl MergePropIter {
    /// Go `NewMergePropIter`: sorts `multi_stat` by min key.
    ///
    /// # Errors
    /// A stat file could not be opened or read.
    pub fn new(
        ctx: &Context,
        multi_stat: &mut [MultipleFilesStat],
        ex_storage: Arc<dyn Storage>,
    ) -> Result<Self> {
        tidb_goruntime::go_pdqsort::go_sort_func_by(multi_stat, |a, b| a.min_key.cmp(&b.min_key));
        let flag = Arc::new(AtomicBool::new(false));
        let openers: Vec<ReaderOpener<MergePropBaseIter>> = multi_stat
            .iter()
            .map(|m| {
                let (ctx, store, m, flag) = (
                    ctx.clone(),
                    Arc::clone(&ex_storage),
                    m.clone(),
                    Arc::clone(&flag),
                );
                Box::new(move || {
                    let mut base = MergePropBaseIter::new(&ctx, &m, &store)?;
                    base.close_reader_flag = Some(flag);
                    Ok(base)
                }) as ReaderOpener<MergePropBaseIter>
            })
            .collect();
        let weights = multi_stat.iter().map(|m| m.max_overlapping_num).collect();
        let iter = LimitSizeMergeIter::new(openers, weights, MAX_MERGE_SORT_OVERLAP_THRESHOLD * 2)?;
        Ok(Self {
            iter,
            base_close_reader_flag: flag,
        })
    }

    /// Go `Error`.
    #[must_use]
    pub fn error(&self) -> Option<&Error> {
        self.iter.iter.err.as_ref()
    }

    /// Go `Next`.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        self.base_close_reader_flag.store(false, Ordering::SeqCst);
        self.iter.next().0
    }

    /// Go `GetBaseIterCloseReaderFlag`: whether the last [`Self::next`]
    /// closed a stat file.
    #[must_use]
    pub fn get_base_iter_close_reader_flag(&self) -> bool {
        self.base_close_reader_flag.load(Ordering::SeqCst)
    }

    /// Go `CurrProperty`.
    #[must_use]
    pub fn curr_property(&self) -> &RangeProperty {
        self.iter.iter.curr.as_ref().expect("current property")
    }

    /// Go `ReaderIndex`: the [`MultipleFilesStat`] and the file within it
    /// the current property came from.
    #[must_use]
    pub fn reader_index(&self) -> (usize, usize) {
        let idx = self.iter.iter.last_reader_idx.expect("current property");
        let base = self.iter.iter.readers[idx]
            .as_ref()
            .expect("open base iterator");
        (
            idx,
            base.iter.iter.last_reader_idx.expect("current property"),
        )
    }

    /// Go `Close`.
    ///
    /// # Errors
    /// A reader failed to close.
    pub fn close(&mut self) -> Result<()> {
        self.iter.iter.close()
    }
}
