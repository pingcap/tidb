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

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{
    AtomicBool, AtomicI32, AtomicI64, AtomicUsize, Ordering as AtomicOrdering,
};
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use tidb_log::{Field, Logger, Value};

use super::{Error, ExternalSorter, Iterator, Result, Writer};

const SST_FILE_SUFFIX: &str = ".sst";
const TMP_FILE_SUFFIX: &str = ".tmp";
const DEFAULT_KV_STATS_BUCKET_SIZE: usize = 1 << 20;
const DISK_SORTER_SORTED_FILE: &str = "sorted";
const DISK_SORTER_STATE_WRITING: i32 = 0;
const DISK_SORTER_STATE_SORTING: i32 = 1;
const DISK_SORTER_STATE_SORTED: i32 = 2;
const RUN_MAGIC: &[u8; 8] = b"TDBXSRT1";
const RUN_HEADER_LEN: u64 = 24;
const RUN_BLOCK_SIZE: usize = 4 << 10;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct FileMetadata {
    file_num: i64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    last_key: Vec<u8>,
    kv_stats: KvStats,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct KvStats {
    #[serde(rename = "histogram")]
    histogram: Option<Vec<KvStatsBucket>>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct KvStatsBucket {
    #[serde(rename = "size")]
    size: usize,
    #[serde(rename = "upperBound")]
    upper_bound: Vec<u8>,
}

#[derive(Debug)]
struct KvStatsCollector {
    bucket_size: usize,
    buckets: Vec<KvStatsBucket>,
    current_size: usize,
    last_key: Vec<u8>,
}

impl KvStatsCollector {
    fn new(bucket_size: usize) -> Self {
        Self {
            bucket_size,
            buckets: Vec::new(),
            current_size: 0,
            last_key: Vec::new(),
        }
    }

    fn add(&mut self, key: &[u8], value: &[u8]) {
        self.current_size += key.len() + value.len();
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        if self.current_size >= self.bucket_size {
            self.add_bucket();
        }
    }

    fn add_bucket(&mut self) {
        self.buckets.push(KvStatsBucket {
            size: self.current_size,
            upper_bound: self.last_key.clone(),
        });
        self.current_size = 0;
    }

    fn finish(mut self) -> KvStats {
        if self.current_size > 0 {
            self.add_bucket();
        }
        KvStats {
            histogram: (!self.buckets.is_empty()).then_some(self.buckets),
        }
    }
}

fn make_filename(dirname: &Path, file_num: i64) -> PathBuf {
    dirname.join(format!("{file_num:06}{SST_FILE_SUFFIX}"))
}

fn parse_filename(path: &Path) -> Option<i64> {
    let filename = path.file_name()?.to_str()?;
    let number = filename.strip_suffix(SST_FILE_SUFFIX)?;
    number.parse().ok()
}

struct SstWriter {
    writer: Option<BufWriter<File>>,
    file_num: i64,
    tmp_path: PathBuf,
    destination_path: PathBuf,
    count: u64,
    write_offset: u64,
    current_block_size: usize,
    block_index: Vec<RunBlockIndex>,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    stats: Option<KvStatsCollector>,
    error: Option<Error>,
    closed: bool,
}

impl SstWriter {
    fn new(dirname: &Path, file_num: i64, bucket_size: usize) -> Result<Self> {
        let destination_path = make_filename(dirname, file_num);
        let mut temporary_name = destination_path.as_os_str().to_os_string();
        temporary_name.push(TMP_FILE_SUFFIX);
        let tmp_path = PathBuf::from(temporary_name);
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&tmp_path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(RUN_MAGIC)?;
        writer.write_all(&0_u64.to_be_bytes())?;
        writer.write_all(&0_u64.to_be_bytes())?;
        Ok(Self {
            writer: Some(writer),
            file_num,
            tmp_path,
            destination_path,
            count: 0,
            write_offset: RUN_HEADER_LEN,
            current_block_size: 0,
            block_index: Vec::new(),
            first_key: Vec::new(),
            last_key: Vec::new(),
            stats: Some(KvStatsCollector::new(bucket_size)),
            error: None,
            closed: false,
        })
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.closed {
            return Err(Error::message("sst writer is closed"));
        }
        if self.error.is_some() {
            return Err(Error::message("sst writer has an earlier error"));
        }
        if self.count > 0 && self.last_key.as_slice() >= key {
            let error = Error::message("keys must be added in strictly increasing order");
            self.error = Some(Error::message(error.to_string()));
            return Err(error);
        }
        let key_len = u32::try_from(key.len()).map_err(|_| Error::message("key is too large"))?;
        let value_len =
            u32::try_from(value.len()).map_err(|_| Error::message("value is too large"))?;
        if self.current_block_size == 0 {
            self.block_index.push(RunBlockIndex {
                first_key: key.to_vec(),
                offset: self.write_offset,
                record_index: self.count,
            });
        }
        let writer = self.writer.as_mut().expect("open writer");
        if let Err(error) = (|| -> std::io::Result<()> {
            writer.write_all(&key_len.to_be_bytes())?;
            writer.write_all(&value_len.to_be_bytes())?;
            writer.write_all(key)?;
            writer.write_all(value)?;
            Ok(())
        })() {
            let error = Error::from(error);
            self.error = Some(Error::message(error.to_string()));
            return Err(error);
        }
        if self.count == 0 {
            self.first_key.extend_from_slice(key);
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.stats
            .as_mut()
            .expect("unfinished stats")
            .add(key, value);
        let record_size = 8 + key.len() + value.len();
        self.write_offset += record_size as u64;
        self.current_block_size += record_size;
        if self.current_block_size >= RUN_BLOCK_SIZE {
            self.current_block_size = 0;
        }
        self.count += 1;
        Ok(())
    }

    fn close(&mut self) -> Result<FileMetadata> {
        if self.closed {
            return Err(Error::message("sst writer is closed"));
        }
        self.closed = true;
        if let Some(error) = self.error.take() {
            self.writer.take();
            let _ = fs::remove_file(&self.tmp_path);
            return Err(error);
        }
        let stats = self.stats.take().expect("unfinished stats").finish();
        let encoded_stats = serde_json::to_vec(&stats)?;
        let mut writer = self.writer.take().expect("open writer");
        let footer_offset = self.write_offset;
        let result = (|| -> std::io::Result<()> {
            writer.write_all(&(encoded_stats.len() as u64).to_be_bytes())?;
            writer.write_all(&encoded_stats)?;
            writer.write_all(&(self.block_index.len() as u64).to_be_bytes())?;
            for block in &self.block_index {
                write_bytes(&mut writer, &block.first_key)?;
                writer.write_all(&block.offset.to_be_bytes())?;
                writer.write_all(&block.record_index.to_be_bytes())?;
            }
            write_bytes(&mut writer, &self.first_key)?;
            write_bytes(&mut writer, &self.last_key)?;
            writer.flush()?;
            writer.seek(SeekFrom::Start(RUN_MAGIC.len() as u64))?;
            writer.write_all(&self.count.to_be_bytes())?;
            writer.write_all(&footer_offset.to_be_bytes())?;
            writer.flush()?;
            Ok(())
        })();
        drop(writer);
        if let Err(error) = result {
            let _ = fs::remove_file(&self.tmp_path);
            return Err(error.into());
        }
        if let Err(error) = fs::rename(&self.tmp_path, &self.destination_path) {
            return Err(error.into());
        }
        let mut end_key = self.last_key.clone();
        end_key.push(0);
        Ok(FileMetadata {
            file_num: self.file_num,
            start_key: self.first_key.clone(),
            end_key,
            last_key: self.last_key.clone(),
            kv_stats: stats,
        })
    }
}

#[derive(Clone, Debug)]
struct RunBlockIndex {
    first_key: Vec<u8>,
    offset: u64,
    record_index: u64,
}

#[derive(Debug)]
struct RunReader {
    path: PathBuf,
    count: u64,
    blocks: Vec<RunBlockIndex>,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    stats: KvStats,
}

impl RunReader {
    fn open(path: &Path) -> Result<Self> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut magic = [0_u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != RUN_MAGIC {
            return Err(Error::message("invalid external-sort run magic"));
        }
        let count = read_u64(&mut reader)?;
        let footer_offset = read_u64(&mut reader)?;
        reader.seek(SeekFrom::Start(footer_offset))?;
        let stats_len = read_u64(&mut reader)? as usize;
        let mut encoded_stats = vec![0; stats_len];
        reader.read_exact(&mut encoded_stats)?;
        let stats = serde_json::from_slice(&encoded_stats)?;
        let block_count = read_u64(&mut reader)? as usize;
        let mut blocks = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            blocks.push(RunBlockIndex {
                first_key: read_bytes(&mut reader)?,
                offset: read_u64(&mut reader)?,
                record_index: read_u64(&mut reader)?,
            });
        }
        let first_key = read_bytes(&mut reader)?;
        let last_key = read_bytes(&mut reader)?;
        Ok(Self {
            path: path.to_path_buf(),
            count,
            blocks,
            first_key,
            last_key,
            stats,
        })
    }
}

fn write_bytes(writer: &mut impl Write, value: &[u8]) -> std::io::Result<()> {
    let length = u32::try_from(value.len())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "value is too large"))?;
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(value)
}

fn read_bytes(reader: &mut impl Read) -> std::io::Result<Vec<u8>> {
    let length = read_u32(reader)? as usize;
    let mut value = vec![0; length];
    reader.read_exact(&mut value)?;
    Ok(value)
}

fn read_u32(reader: &mut impl Read) -> std::io::Result<u32> {
    let mut bytes = [0_u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> std::io::Result<u64> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_be_bytes(bytes))
}

#[derive(Debug)]
struct ReaderPool {
    dirname: PathBuf,
    readers: Mutex<HashMap<i64, (Arc<RunReader>, usize)>>,
}

impl ReaderPool {
    fn new(dirname: PathBuf) -> Arc<Self> {
        Arc::new(Self {
            dirname,
            readers: Mutex::new(HashMap::new()),
        })
    }

    fn get(self: &Arc<Self>, file_num: i64) -> Result<ReaderLease> {
        {
            let mut readers = self
                .readers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some((reader, refs)) = readers.get_mut(&file_num) {
                *refs += 1;
                return Ok(ReaderLease {
                    file_num,
                    reader: Arc::clone(reader),
                    pool: Arc::clone(self),
                    released: false,
                });
            }
        }
        let opened = Arc::new(RunReader::open(&make_filename(&self.dirname, file_num))?);
        let reader = {
            let mut readers = self
                .readers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some((reader, refs)) = readers.get_mut(&file_num) {
                *refs += 1;
                Arc::clone(reader)
            } else {
                readers.insert(file_num, (Arc::clone(&opened), 1));
                opened
            }
        };
        Ok(ReaderLease {
            file_num,
            reader,
            pool: Arc::clone(self),
            released: false,
        })
    }

    fn release(&self, file_num: i64) {
        let mut readers = self
            .readers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (_, refs) = readers.get_mut(&file_num).unwrap_or_else(|| {
            panic!("sstReaderPool: unref a reader that does not exist: {file_num}")
        });
        *refs -= 1;
        if *refs == 0 {
            readers.remove(&file_num);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.readers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }
}

#[derive(Debug)]
struct ReaderLease {
    file_num: i64,
    reader: Arc<RunReader>,
    pool: Arc<ReaderPool>,
    released: bool,
}

impl ReaderLease {
    fn release(&mut self) {
        if !self.released {
            self.pool.release(self.file_num);
            self.released = true;
        }
    }
}

impl Drop for ReaderLease {
    fn drop(&mut self) {
        self.release();
    }
}

struct SstIter {
    lease: Option<ReaderLease>,
    file: Option<BufReader<File>>,
    file_offset: u64,
    record_index: Option<u64>,
    next_offset: u64,
    key: Vec<u8>,
    value: Vec<u8>,
    error: Option<Error>,
}

impl SstIter {
    fn new(lease: ReaderLease) -> Result<Self> {
        let file = BufReader::new(File::open(&lease.reader.path)?);
        Ok(Self {
            lease: Some(lease),
            file: Some(file),
            file_offset: 0,
            record_index: None,
            next_offset: RUN_HEADER_LEN,
            key: Vec::new(),
            value: Vec::new(),
            error: None,
        })
    }

    fn invalidate(&mut self) -> bool {
        self.record_index = None;
        self.key.clear();
        self.value.clear();
        false
    }

    fn read_record(&mut self, offset: u64, record_index: u64) -> bool {
        self.error = None;
        let result = (|| -> std::io::Result<()> {
            let file = self.file.as_mut().expect("open iterator");
            if self.file_offset != offset {
                file.seek(SeekFrom::Start(offset))?;
                self.file_offset = offset;
            }
            let key_length = read_u32(file)? as usize;
            let value_length = read_u32(file)? as usize;
            self.key.resize(key_length, 0);
            self.value.resize(value_length, 0);
            file.read_exact(&mut self.key)?;
            file.read_exact(&mut self.value)?;
            self.next_offset = offset + 8 + key_length as u64 + value_length as u64;
            self.file_offset = self.next_offset;
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.record_index = Some(record_index);
                true
            }
            Err(error) => {
                self.error = Some(error.into());
                self.invalidate()
            }
        }
    }
}

impl Iterator for SstIter {
    fn seek(&mut self, key: &[u8]) -> bool {
        let (count, block) = {
            let reader = &self.lease.as_ref().expect("open iterator").reader;
            if reader.count == 0 {
                return self.invalidate();
            }
            let block_index = reader
                .blocks
                .partition_point(|block| block.first_key.as_slice() <= key)
                .saturating_sub(1);
            (reader.count, reader.blocks[block_index].clone())
        };
        if !self.read_record(block.offset, block.record_index) {
            return false;
        }
        while self.key.as_slice() < key {
            let index = self.record_index.expect("valid record");
            if index + 1 >= count || !self.read_record(self.next_offset, index + 1) {
                return self.invalidate();
            }
        }
        true
    }

    fn first(&mut self) -> bool {
        let count = self.lease.as_ref().expect("open iterator").reader.count;
        if count == 0 {
            return self.invalidate();
        }
        self.read_record(RUN_HEADER_LEN, 0)
    }

    fn next(&mut self) -> bool {
        let Some(record_index) = self.record_index else {
            return false;
        };
        let count = self.lease.as_ref().expect("open iterator").reader.count;
        if record_index + 1 >= count {
            return self.invalidate();
        }
        self.read_record(self.next_offset, record_index + 1)
    }

    fn last(&mut self) -> bool {
        let (count, block) = {
            let reader = &self.lease.as_ref().expect("open iterator").reader;
            if reader.count == 0 {
                return self.invalidate();
            }
            (
                reader.count,
                reader.blocks.last().expect("non-empty run").clone(),
            )
        };
        if !self.read_record(block.offset, block.record_index) {
            return false;
        }
        while self.record_index.expect("valid record") + 1 < count {
            let index = self.record_index.expect("valid record");
            if !self.read_record(self.next_offset, index + 1) {
                return false;
            }
        }
        true
    }

    fn valid(&self) -> bool {
        self.error.is_none() && self.record_index.is_some()
    }

    fn error(&self) -> Option<&Error> {
        self.error.as_ref()
    }

    fn unsafe_key(&self) -> &[u8] {
        &self.key
    }

    fn unsafe_value(&self) -> &[u8] {
        &self.value
    }

    fn close(&mut self) -> Result<()> {
        self.record_index = None;
        self.key.clear();
        self.value.clear();
        self.file.take();
        self.lease.take();
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HeapNode {
    key: Vec<u8>,
    file_index: usize,
}

impl Ord for HeapNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.file_index.cmp(&other.file_index))
    }
}

impl PartialOrd for HeapNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

type OpenIterator = Arc<dyn Fn(&FileMetadata) -> Result<Box<dyn Iterator>> + Send + Sync + 'static>;

struct MergingIter {
    heap: BinaryHeap<Reverse<HeapNode>>,
    opened: HashMap<usize, Box<dyn Iterator>>,
    ordered_files: Vec<FileMetadata>,
    next_file_index: usize,
    open_iterator: OpenIterator,
    error: Option<Error>,
}

impl MergingIter {
    fn new(ordered_files: Vec<FileMetadata>, open_iterator: OpenIterator) -> Self {
        assert!(
            ordered_files
                .windows(2)
                .all(|window| window[0].start_key <= window[1].start_key),
            "newMergingIter: orderedFiles are not ordered by start key"
        );
        Self {
            heap: BinaryHeap::new(),
            opened: HashMap::new(),
            ordered_files,
            next_file_index: 0,
            open_iterator,
            error: None,
        }
    }

    fn push_current(&mut self, file_index: usize, iterator: Box<dyn Iterator>) {
        self.heap.push(Reverse(HeapNode {
            key: iterator.unsafe_key().to_vec(),
            file_index,
        }));
        self.opened.insert(file_index, iterator);
    }

    fn close_opened(&mut self) -> Result<()> {
        self.heap.clear();
        let mut first_error = None;
        for (_, mut iterator) in self.opened.drain() {
            if let Err(error) = iterator.close() {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn maybe_open_next_files(&mut self) -> bool {
        while self.next_file_index < self.ordered_files.len() {
            let file = &self.ordered_files[self.next_file_index];
            if self
                .heap
                .peek()
                .is_some_and(|node| node.0.key.as_slice() < file.start_key.as_slice())
            {
                break;
            }
            let file_index = self.next_file_index;
            let mut iterator = match (self.open_iterator)(file) {
                Ok(iterator) => iterator,
                Err(error) => {
                    self.error = Some(error);
                    return false;
                }
            };
            if iterator.first() {
                self.push_current(file_index, iterator);
            } else {
                if let Some(error) = iterator.error() {
                    self.error = Some(Error::message(error.to_string()));
                }
                if let Err(error) = iterator.close() {
                    if self.error.is_none() {
                        self.error = Some(error);
                    }
                }
                if self.error.is_some() {
                    return false;
                }
            }
            self.next_file_index += 1;
        }
        !self.heap.is_empty()
    }

    fn advance_file(&mut self, file_index: usize) -> bool {
        let mut iterator = self
            .opened
            .remove(&file_index)
            .expect("heap item has an iterator");
        if iterator.next() {
            self.push_current(file_index, iterator);
        } else {
            if let Some(error) = iterator.error() {
                self.error = Some(Error::message(error.to_string()));
            }
            if let Err(error) = iterator.close() {
                if self.error.is_none() {
                    self.error = Some(error);
                }
            }
            if self.error.is_some() {
                return false;
            }
        }
        self.maybe_open_next_files()
    }
}

impl Iterator for MergingIter {
    fn seek(&mut self, key: &[u8]) -> bool {
        self.error = None;
        self.heap.clear();
        let old_opened = std::mem::take(&mut self.opened);
        let mut reused = HashSet::new();
        for (index, mut iterator) in old_opened {
            let file = &self.ordered_files[index];
            if self.error.is_none()
                && key >= file.start_key.as_slice()
                && key < file.end_key.as_slice()
                && iterator.seek(key)
            {
                reused.insert(index);
                self.push_current(index, iterator);
                continue;
            }
            if self.error.is_none() {
                if let Some(error) = iterator.error() {
                    self.error = Some(Error::message(error.to_string()));
                }
            }
            if let Err(error) = iterator.close() {
                if self.error.is_none() {
                    self.error = Some(error);
                }
            }
        }
        if self.error.is_some() {
            return false;
        }
        self.next_file_index = self.ordered_files.len();
        for index in 0..self.ordered_files.len() {
            let file = &self.ordered_files[index];
            if file.start_key.as_slice() > key {
                self.next_file_index = index;
                break;
            }
            if reused.contains(&index) {
                continue;
            }
            if file.end_key.as_slice() <= key {
                continue;
            }
            let mut iterator = match (self.open_iterator)(file) {
                Ok(iterator) => iterator,
                Err(error) => {
                    self.error = Some(error);
                    return false;
                }
            };
            if iterator.seek(key) {
                self.push_current(index, iterator);
            } else {
                if let Some(error) = iterator.error() {
                    self.error = Some(Error::message(error.to_string()));
                }
                if let Err(error) = iterator.close() {
                    if self.error.is_none() {
                        self.error = Some(error);
                    }
                }
                if self.error.is_some() {
                    return false;
                }
            }
        }
        self.maybe_open_next_files()
    }

    fn first(&mut self) -> bool {
        self.seek(&[])
    }

    fn next(&mut self) -> bool {
        self.error = None;
        let Some(current_key) = self.heap.peek().map(|node| node.0.key.clone()) else {
            return false;
        };
        while self
            .heap
            .peek()
            .is_some_and(|node| node.0.key == current_key)
        {
            let Reverse(node) = self.heap.pop().expect("checked heap");
            if !self.advance_file(node.file_index) && self.error.is_some() {
                return false;
            }
        }
        self.valid()
    }

    fn last(&mut self) -> bool {
        self.error = None;
        if let Err(error) = self.close_opened() {
            self.error = Some(error);
            return false;
        }
        self.next_file_index = self.ordered_files.len();
        let mut indexes: Vec<_> = (0..self.ordered_files.len()).collect();
        indexes.sort_by(|left, right| {
            self.ordered_files[*right]
                .last_key
                .cmp(&self.ordered_files[*left].last_key)
        });
        for index in indexes {
            let file = &self.ordered_files[index];
            let mut iterator = match (self.open_iterator)(file) {
                Ok(iterator) => iterator,
                Err(error) => {
                    self.error = Some(error);
                    return false;
                }
            };
            if iterator.last() {
                self.push_current(index, iterator);
                break;
            }
            if let Some(error) = iterator.error() {
                self.error = Some(Error::message(error.to_string()));
            }
            if let Err(error) = iterator.close() {
                if self.error.is_none() {
                    self.error = Some(error);
                }
            }
            if self.error.is_some() {
                return false;
            }
        }
        self.valid()
    }

    fn valid(&self) -> bool {
        self.error.is_none() && !self.heap.is_empty()
    }

    fn error(&self) -> Option<&Error> {
        self.error.as_ref()
    }

    fn unsafe_key(&self) -> &[u8] {
        &self.heap.peek().expect("iterator is invalid").0.key
    }

    fn unsafe_value(&self) -> &[u8] {
        let index = self.heap.peek().expect("iterator is invalid").0.file_index;
        self.opened
            .get(&index)
            .expect("heap item has an iterator")
            .unsafe_value()
    }

    fn close(&mut self) -> Result<()> {
        self.error = None;
        self.close_opened()
    }
}

/// Optional parameters for [`DiskSorter`].
#[derive(Clone)]
pub struct DiskSorterOptions {
    /// Maximum number of concurrent compactions.
    pub concurrency: usize,
    /// Per-writer in-memory byte buffer size.
    pub writer_buffer_size: usize,
    /// Overlap depth which triggers compaction.
    pub compaction_threshold: usize,
    /// Maximum number of files in one compaction group.
    pub max_compaction_depth: usize,
    /// Approximate maximum bytes in one compaction range.
    pub max_compaction_size: usize,
    /// Logger used for compaction diagnostics.
    pub logger: Option<Logger>,
}

impl Default for DiskSorterOptions {
    fn default() -> Self {
        Self {
            concurrency: std::thread::available_parallelism().map_or(1, usize::from),
            writer_buffer_size: 128 << 20,
            compaction_threshold: 16,
            max_compaction_depth: 64,
            max_compaction_size: 512 << 20,
            logger: Some(tidb_log::l()),
        }
    }
}

impl DiskSorterOptions {
    fn adjust(&mut self) {
        if self.concurrency == 0 {
            self.concurrency = std::thread::available_parallelism().map_or(1, usize::from);
        }
        if self.writer_buffer_size == 0 {
            self.writer_buffer_size = 128 << 20;
        }
        if self.compaction_threshold == 0 {
            self.compaction_threshold = 16;
        }
        if self.max_compaction_depth < 2 {
            self.max_compaction_depth = 64;
        }
        if self.max_compaction_size == 0 {
            self.max_compaction_size = 512 << 20;
        }
        if self.logger.is_none() {
            self.logger = Some(tidb_log::l());
        }
    }
}

struct DiskSorterInner {
    options: DiskSorterOptions,
    dirname: PathBuf,
    _owned_directory: Option<tempfile::TempDir>,
    reader_pool: Arc<ReaderPool>,
    id_allocator: AtomicI64,
    state: AtomicI32,
    pending_files: Mutex<Vec<FileMetadata>>,
    ordered_files: RwLock<Vec<FileMetadata>>,
}

/// Disk-backed implementation of [`ExternalSorter`].
#[derive(Clone)]
pub struct DiskSorter {
    inner: Arc<DiskSorterInner>,
}

/// Opens or recovers a disk sorter rooted at `dirname`.
pub fn open_disk_sorter(
    dirname: impl AsRef<Path>,
    options: Option<DiskSorterOptions>,
) -> Result<DiskSorter> {
    DiskSorter::open(dirname, options)
}

impl DiskSorter {
    fn open(dirname: impl AsRef<Path>, options: Option<DiskSorterOptions>) -> Result<Self> {
        let mut options = options.unwrap_or_default();
        options.adjust();
        let requested_directory = dirname.as_ref();
        let owned_directory = if requested_directory.as_os_str().is_empty() {
            Some(tempfile::tempdir()?)
        } else {
            None
        };
        let dirname = owned_directory.as_ref().map_or_else(
            || requested_directory.to_path_buf(),
            |dir| dir.path().to_path_buf(),
        );
        if owned_directory.is_none() {
            create_sort_directory(&dirname)?;
        }
        let reader_pool = ReaderPool::new(dirname.clone());
        let sorter = Self {
            inner: Arc::new(DiskSorterInner {
                options,
                dirname,
                _owned_directory: owned_directory,
                reader_pool,
                id_allocator: AtomicI64::new(0),
                state: AtomicI32::new(DISK_SORTER_STATE_WRITING),
                pending_files: Mutex::new(Vec::new()),
                ordered_files: RwLock::new(Vec::new()),
            }),
        };
        sorter.initialize()?;
        Ok(sorter)
    }

    fn initialize(&self) -> Result<()> {
        let mut file_numbers = Vec::new();
        for entry in fs::read_dir(&self.inner.dirname)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(TMP_FILE_SUFFIX))
            {
                let _ = fs::remove_file(path);
                continue;
            }
            let Some(file_num) = parse_filename(&path) else {
                continue;
            };
            self.inner
                .id_allocator
                .fetch_max(file_num, AtomicOrdering::SeqCst);
            file_numbers.push(file_num);
        }
        let files = Mutex::new(Vec::with_capacity(file_numbers.len()));
        let first_error = Mutex::new(None);
        let canceled = AtomicBool::new(false);
        std::thread::scope(|scope| {
            for file_num in file_numbers {
                let files = &files;
                let first_error = &first_error;
                let canceled = &canceled;
                scope.spawn(move || {
                    if canceled.load(AtomicOrdering::Relaxed) {
                        return;
                    }
                    match self.read_file_metadata(file_num) {
                        Ok(file) => files
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .push(file),
                        Err(error) => {
                            let mut slot = first_error
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            if slot.is_none() {
                                *slot = Some(error);
                            }
                            drop(slot);
                            canceled.store(true, AtomicOrdering::Relaxed);
                        }
                    }
                });
            }
        });
        if let Some(error) = first_error
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            return Err(error);
        }
        let mut files = files
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if fs::metadata(self.inner.dirname.join(DISK_SORTER_SORTED_FILE)).is_ok() {
            files.sort_by(|left, right| left.start_key.cmp(&right.start_key));
            *self
                .inner
                .ordered_files
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = files;
            self.inner
                .state
                .store(DISK_SORTER_STATE_SORTED, AtomicOrdering::SeqCst);
        } else {
            *self
                .inner
                .pending_files
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = files;
            self.inner
                .state
                .store(DISK_SORTER_STATE_WRITING, AtomicOrdering::SeqCst);
        }
        Ok(())
    }

    fn read_file_metadata(&self, file_num: i64) -> Result<FileMetadata> {
        let lease = self.inner.reader_pool.get(file_num)?;
        let reader = &lease.reader;
        let start_key = reader.first_key.clone();
        let last_key = reader.last_key.clone();
        let mut end_key = last_key.clone();
        end_key.push(0);
        Ok(FileMetadata {
            file_num,
            start_key,
            end_key,
            last_key,
            kv_stats: reader.stats.clone(),
        })
    }

    fn merging_iterator(&self, files: Vec<FileMetadata>) -> MergingIter {
        let pool = Arc::clone(&self.inner.reader_pool);
        MergingIter::new(
            files,
            Arc::new(move |file| {
                Ok(Box::new(SstIter::new(pool.get(file.file_num)?)?) as Box<dyn Iterator>)
            }),
        )
    }

    fn do_sort(&self, canceled: &AtomicBool) -> Result<()> {
        let mut pending = self
            .inner
            .pending_files
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pending.is_empty() {
            return Ok(());
        }
        let mut ordered = std::mem::take(&mut *pending);
        ordered.sort_by(|left, right| left.start_key.cmp(&right.start_key));
        *self
            .inner
            .ordered_files
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = ordered;

        loop {
            let files = {
                let ordered = self
                    .inner
                    .ordered_files
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                pick_compaction_files(
                    &ordered,
                    self.inner.options.compaction_threshold,
                    self.inner.options.logger.as_ref(),
                )
            };
            if files.is_empty() {
                return Ok(());
            }
            self.compact_files(canceled, files)?;
        }
    }

    fn compact_files(&self, canceled: &AtomicBool, mut files: Vec<FileMetadata>) -> Result<()> {
        let groups = split_compaction_files(&mut files, self.inner.options.max_compaction_depth);
        let compactions: Vec<_> = groups
            .iter()
            .flat_map(|group| build_compactions(group, self.inner.options.max_compaction_size))
            .collect();
        if let Some(logger) = &self.inner.options.logger {
            logger.info(
                "total compactions",
                &[Field::new("count", Value::I64(compactions.len() as i64))],
            );
        }

        let mut references = HashMap::<i64, usize>::new();
        for file in &files {
            references.insert(file.file_num, 0);
        }
        for compaction in &compactions {
            for file in &compaction.overlap_files {
                *references.get_mut(&file.file_num).expect("selected file") += 1;
            }
        }

        let references = Mutex::new(references);
        let removed = Mutex::new(HashSet::new());
        let outputs = Mutex::new(Vec::with_capacity(compactions.len()));
        let first_error = Mutex::new(None);
        let next = AtomicUsize::new(0);
        let abort = AtomicBool::new(false);
        let worker_count = self.inner.options.concurrency.min(compactions.len()).max(1);
        std::thread::scope(|scope| {
            for _ in 0..worker_count {
                scope.spawn(|| loop {
                    if abort.load(AtomicOrdering::Relaxed) {
                        return;
                    }
                    let index = next.fetch_add(1, AtomicOrdering::Relaxed);
                    let Some(compaction) = compactions.get(index) else {
                        return;
                    };
                    match self.run_compaction(canceled, &abort, compaction) {
                        Ok(output) => {
                            outputs
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .push(output);
                            let mut references = references
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            for file in &compaction.overlap_files {
                                let count = references
                                    .get_mut(&file.file_num)
                                    .expect("compaction reference");
                                *count -= 1;
                                if *count == 0 {
                                    let _ = fs::remove_file(make_filename(
                                        &self.inner.dirname,
                                        file.file_num,
                                    ));
                                    removed
                                        .lock()
                                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                                        .insert(file.file_num);
                                }
                            }
                        }
                        Err(error) => {
                            abort.store(true, AtomicOrdering::Relaxed);
                            let mut slot = first_error
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            if slot.is_none() {
                                *slot = Some(error);
                            }
                            return;
                        }
                    }
                });
            }
        });
        if let Some(error) = first_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            return Err(error);
        }

        let removed = removed
            .into_inner()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut ordered = self
            .inner
            .ordered_files
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ordered.extend(
            outputs
                .into_inner()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        ordered.retain(|file| !removed.contains(&file.file_num));
        ordered.sort_by(|left, right| left.start_key.cmp(&right.start_key));
        Ok(())
    }

    fn run_compaction(
        &self,
        canceled: &AtomicBool,
        group_canceled: &AtomicBool,
        compaction: &Compaction,
    ) -> Result<FileMetadata> {
        if let Some(logger) = &self.inner.options.logger {
            logger.debug(
                "run compaction",
                &[
                    Field::new("startKey", Value::Binary(compaction.start_key.clone())),
                    Field::new("endKey", Value::Binary(compaction.end_key.clone())),
                    Field::new(
                        "fileCount",
                        Value::I64(compaction.overlap_files.len() as i64),
                    ),
                ],
            );
        }
        let file_num = self.inner.id_allocator.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        let mut writer =
            SstWriter::new(&self.inner.dirname, file_num, DEFAULT_KV_STATS_BUCKET_SIZE)?;
        let mut iterator = self.merging_iterator(compaction.overlap_files.clone());
        let write_result = (|| {
            let mut iterations = 0_usize;
            let mut valid = iterator.seek(&compaction.start_key);
            while valid {
                if iterator.unsafe_key() >= compaction.end_key.as_slice() {
                    break;
                }
                iterations += 1;
                if iterations.is_multiple_of(1000)
                    && (canceled.load(AtomicOrdering::Relaxed)
                        || group_canceled.load(AtomicOrdering::Relaxed))
                {
                    return Err(Error::canceled());
                }
                writer.set(iterator.unsafe_key(), iterator.unsafe_value())?;
                valid = iterator.next();
            }
            if let Some(error) = iterator.error() {
                return Err(Error::message(error.to_string()));
            }
            Ok(())
        })();
        let _ = iterator.close();
        if let Err(error) = write_result {
            let _ = writer.close();
            return Err(error);
        }
        writer.close()
    }
}

fn create_sort_directory(path: &Path) -> std::io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o755);
    }
    builder.create(path)
}

#[derive(Clone, Debug)]
struct KeyValue {
    key: Vec<u8>,
    value: Vec<u8>,
}

struct DiskSorterWriter {
    sorter: Arc<DiskSorterInner>,
    key_values: Vec<KeyValue>,
    buffered_bytes: usize,
}

impl DiskSorterWriter {
    fn flush_inner(&mut self) -> Result<()> {
        if self.key_values.is_empty() {
            return Ok(());
        }
        let file_num = self
            .sorter
            .id_allocator
            .fetch_add(1, AtomicOrdering::SeqCst)
            + 1;
        let mut writer =
            SstWriter::new(&self.sorter.dirname, file_num, DEFAULT_KV_STATS_BUCKET_SIZE)?;
        self.key_values
            .sort_unstable_by(|left, right| left.key.cmp(&right.key));
        let mut last_key: &[u8] = &[];
        for key_value in &self.key_values {
            if last_key == key_value.key.as_slice() {
                continue;
            }
            if let Err(error) = writer.set(&key_value.key, &key_value.value) {
                let _ = writer.close();
                return Err(error);
            }
            last_key = &key_value.key;
        }
        let metadata = writer.close()?;
        self.sorter
            .pending_files
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(metadata);
        self.key_values.clear();
        self.buffered_bytes = 0;
        Ok(())
    }
}

impl Writer for DiskSorterWriter {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.buffered_bytes + key.len() + value.len() > self.sorter.options.writer_buffer_size {
            self.flush_inner()?;
        }
        self.key_values.push(KeyValue {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        self.buffered_bytes += key.len() + value.len();
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.flush_inner()
    }

    fn close(&mut self) -> Result<()> {
        self.flush_inner()
    }
}

impl ExternalSorter for DiskSorter {
    fn new_writer(&self, _canceled: &AtomicBool) -> Result<Box<dyn Writer>> {
        if self.inner.state.load(AtomicOrdering::SeqCst) > DISK_SORTER_STATE_WRITING {
            return Err(Error::message(
                "diskSorter started sorting, cannot write more data",
            ));
        }
        Ok(Box::new(DiskSorterWriter {
            sorter: Arc::clone(&self.inner),
            key_values: Vec::new(),
            buffered_bytes: 0,
        }))
    }

    fn sort(&self, canceled: &AtomicBool) -> Result<()> {
        if self.inner.state.load(AtomicOrdering::SeqCst) == DISK_SORTER_STATE_SORTED {
            return Ok(());
        }
        self.inner
            .state
            .store(DISK_SORTER_STATE_SORTING, AtomicOrdering::SeqCst);
        self.do_sort(canceled)?;
        self.inner
            .state
            .store(DISK_SORTER_STATE_SORTED, AtomicOrdering::SeqCst);
        File::create(self.inner.dirname.join(DISK_SORTER_SORTED_FILE))?;
        Ok(())
    }

    fn is_sorted(&self) -> bool {
        self.inner.state.load(AtomicOrdering::SeqCst) == DISK_SORTER_STATE_SORTED
    }

    fn new_iterator(&self, _canceled: &AtomicBool) -> Result<Box<dyn Iterator>> {
        if !self.is_sorted() {
            return Err(Error::message("diskSorter is not sorted"));
        }
        let files = self
            .inner
            .ordered_files
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        Ok(Box::new(self.merging_iterator(files)))
    }

    fn close(&self) -> Result<()> {
        Ok(())
    }

    fn close_and_cleanup(&self) -> Result<()> {
        match fs::remove_dir_all(&self.inner.dirname) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Compaction {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    overlap_files: Vec<FileMetadata>,
}

fn pick_compaction_files(
    all_files: &[FileMetadata],
    compaction_threshold: usize,
    logger: Option<&Logger>,
) -> Vec<FileMetadata> {
    if all_files.is_empty() {
        return Vec::new();
    }
    let mut events = Vec::<(Vec<u8>, isize)>::with_capacity(all_files.len() * 2);
    for file in all_files {
        events.push((file.start_key.clone(), 1));
        events.push((file.end_key.clone(), -1));
    }
    events.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let mut grouped = Vec::<(Vec<u8>, isize)>::new();
    for (key, delta) in events {
        if let Some((last_key, last_delta)) = grouped.last_mut() {
            if *last_key == key {
                *last_delta += delta;
                continue;
            }
        }
        grouped.push((key, delta));
    }
    let mut depth = 0_isize;
    let mut max_depth = 0_isize;
    let mut intervals = Vec::with_capacity(grouped.len());
    for (key, delta) in grouped {
        depth += delta;
        max_depth = max_depth.max(depth);
        intervals.push((key, depth));
    }
    if max_depth < compaction_threshold as isize {
        return Vec::new();
    }

    let files: Vec<_> = all_files
        .iter()
        .filter(|file| {
            let minimum = intervals.partition_point(|(key, _)| key < &file.start_key);
            let maximum = intervals.partition_point(|(key, _)| key < &file.end_key);
            intervals[minimum..maximum]
                .iter()
                .any(|(_, depth)| *depth >= compaction_threshold as isize)
        })
        .cloned()
        .collect();
    if let Some(logger) = logger {
        logger.info(
            "max overlap depth reached the compaction threshold, pick files to compact",
            &[
                Field::new("maxDepth", Value::I64(max_depth as i64)),
                Field::new("threshold", Value::I64(compaction_threshold as i64)),
                Field::new("fileCount", Value::I64(files.len() as i64)),
            ],
        );
    }
    files
}

fn split_compaction_files(
    files: &mut [FileMetadata],
    max_compaction_depth: usize,
) -> Vec<Vec<FileMetadata>> {
    files.sort_by(|left, right| left.start_key.cmp(&right.start_key));
    let mut groups = Vec::<Vec<FileMetadata>>::new();
    let mut current_group = vec![files[0].clone()];
    let mut maximum_end_key = files[0].end_key.clone();
    for file in &files[1..] {
        if file.start_key >= maximum_end_key {
            groups.push(std::mem::take(&mut current_group));
        }
        current_group.push(file.clone());
        if file.end_key > maximum_end_key {
            maximum_end_key.clone_from(&file.end_key);
        }
    }
    if !current_group.is_empty() {
        groups.push(current_group);
    }

    let mut final_groups = Vec::new();
    for group in groups {
        let subgroup_count = (group.len() - 1) / max_compaction_depth + 1;
        let subgroup_size = (group.len() - 1) / subgroup_count + 1;
        final_groups.extend(group.chunks(subgroup_size).map(<[FileMetadata]>::to_vec));
    }
    final_groups
}

fn build_compactions(files: &[FileMetadata], max_compaction_size: usize) -> Vec<Compaction> {
    let start_key = files
        .iter()
        .map(|file| file.start_key.as_slice())
        .min()
        .expect("non-empty compaction files")
        .to_vec();
    let end_key = files
        .iter()
        .map(|file| file.end_key.as_slice())
        .max()
        .expect("non-empty compaction files")
        .to_vec();
    let mut buckets: Vec<_> = files
        .iter()
        .flat_map(|file| {
            file.kv_stats
                .histogram
                .as_deref()
                .unwrap_or_default()
                .iter()
                .cloned()
        })
        .collect();
    if buckets.is_empty() {
        let mut overlap_files = files.to_vec();
        overlap_files.sort_by(|left, right| left.start_key.cmp(&right.start_key));
        return vec![Compaction {
            start_key,
            end_key,
            overlap_files,
        }];
    }

    buckets.sort_by(|left, right| left.upper_bound.cmp(&right.upper_bound));
    let mut merged_buckets = Vec::<KvStatsBucket>::with_capacity(buckets.len());
    for bucket in buckets {
        if let Some(previous) = merged_buckets.last_mut() {
            if previous.upper_bound == bucket.upper_bound {
                previous.size += bucket.size;
                continue;
            }
        }
        merged_buckets.push(bucket);
    }

    let mut range_start = start_key;
    let mut key_value_size = 0_usize;
    let mut compactions = Vec::new();
    for (index, bucket) in merged_buckets.iter().enumerate() {
        if index + 1 == merged_buckets.len() {
            compactions.push(Compaction {
                start_key: range_start,
                end_key: end_key.clone(),
                overlap_files: Vec::new(),
            });
            break;
        }
        key_value_size += bucket.size;
        if key_value_size >= max_compaction_size {
            compactions.push(Compaction {
                start_key: range_start,
                end_key: bucket.upper_bound.clone(),
                overlap_files: Vec::new(),
            });
            range_start = bucket.upper_bound.clone();
            key_value_size = 0;
        }
    }

    for compaction in &mut compactions {
        compaction.overlap_files = files
            .iter()
            .filter(|file| {
                !(file.end_key <= compaction.start_key || file.start_key >= compaction.end_key)
            })
            .cloned()
            .collect();
        compaction
            .overlap_files
            .sort_by(|left, right| left.start_key.cmp(&right.start_key));
    }
    compactions
}

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;
    use std::sync::Barrier;

    use super::*;

    fn no_cancel() -> AtomicBool {
        AtomicBool::new(false)
    }

    fn random_key_values(count: usize, key_range: usize, value_range: usize) -> Vec<KeyValue> {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        (0..count)
            .map(|index| {
                let key_size = (next() as usize % (key_range - 4)) + 4;
                let value_size = next() as usize % value_range;
                let mut key = vec![0; key_size];
                let mut value = vec![0; value_size];
                for byte in &mut key[..key_size - 4] {
                    *byte = next() as u8;
                }
                for byte in &mut value {
                    *byte = next() as u8;
                }
                key[key_size - 4..].copy_from_slice(&(index as u32).to_be_bytes());
                KeyValue { key, value }
            })
            .collect()
    }

    fn collect(iterator: &mut dyn Iterator) -> Result<Vec<KeyValue>> {
        let mut output = Vec::new();
        let mut valid = iterator.first();
        while valid {
            output.push(KeyValue {
                key: iterator.unsafe_key().to_vec(),
                value: iterator.unsafe_value().to_vec(),
            });
            valid = iterator.next();
        }
        if let Some(error) = iterator.error() {
            return Err(Error::message(error.to_string()));
        }
        Ok(output)
    }

    fn common_test(sorter: &DiskSorter) {
        let canceled = no_cancel();
        let mut writer = sorter.new_writer(&canceled).unwrap();
        let mut key_values = random_key_values(1_000, 256, 1_024);
        for key_value in &key_values {
            writer.put(&key_value.key, &key_value.value).unwrap();
        }
        writer.close().unwrap();
        assert!(sorter.new_iterator(&canceled).is_err());
        sorter.sort(&canceled).unwrap();
        let mut iterator = sorter.new_iterator(&canceled).unwrap();
        let actual = collect(iterator.as_mut()).unwrap();
        key_values.sort_by(|left, right| left.key.cmp(&right.key));
        assert_eq!(actual.len(), key_values.len());
        for (actual, expected) in actual.iter().zip(&key_values) {
            assert_eq!(actual.key, expected.key);
            assert_eq!(actual.value, expected.value);
        }
        iterator.close().unwrap();
    }

    fn common_parallel_test(sorter: &DiskSorter) {
        const WRITERS: usize = 10;
        let canceled = Arc::new(no_cancel());
        let key_values = Arc::new(random_key_values(10_000, 256, 1_024));
        let barrier = Arc::new(Barrier::new(WRITERS));
        std::thread::scope(|scope| {
            for writer_index in 0..WRITERS {
                let sorter = sorter.clone();
                let canceled = Arc::clone(&canceled);
                let key_values = Arc::clone(&key_values);
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    let mut writer = sorter.new_writer(&canceled).unwrap();
                    barrier.wait();
                    for (index, key_value) in key_values.iter().enumerate() {
                        if index % WRITERS == writer_index {
                            writer.put(&key_value.key, &key_value.value).unwrap();
                        }
                    }
                    writer.close().unwrap();
                });
            }
        });
        sorter.sort(&canceled).unwrap();
        let mut iterator = sorter.new_iterator(&canceled).unwrap();
        let actual = collect(iterator.as_mut()).unwrap();
        let mut expected = key_values.as_ref().clone();
        expected.sort_by(|left, right| left.key.cmp(&right.key));
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(&expected) {
            assert_eq!(actual.key, expected.key);
            assert_eq!(actual.value, expected.value);
        }
        iterator.close().unwrap();
    }

    fn test_options() -> DiskSorterOptions {
        DiskSorterOptions {
            writer_buffer_size: 32 * 1_024,
            compaction_threshold: 4,
            max_compaction_depth: 4,
            logger: None,
            ..Default::default()
        }
    }

    #[test]
    fn disk_sorter_common() {
        let directory = tempfile::tempdir().unwrap();
        let sorter = DiskSorter::open(directory.path(), Some(test_options())).unwrap();
        common_test(&sorter);
        sorter.close().unwrap();
    }

    #[test]
    fn disk_sorter_common_parallel() {
        let directory = tempfile::tempdir().unwrap();
        let sorter = DiskSorter::open(directory.path(), Some(test_options())).unwrap();
        common_parallel_test(&sorter);
        sorter.close().unwrap();
    }

    #[test]
    fn disk_sorter_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let canceled = no_cancel();
        let mut key_values = random_key_values(2_000, 256, 1_024);
        let (first, second) = key_values.split_at(1_000);

        let sorter = DiskSorter::open(directory.path(), Some(test_options())).unwrap();
        let mut writer = sorter.new_writer(&canceled).unwrap();
        for key_value in first {
            writer.put(&key_value.key, &key_value.value).unwrap();
        }
        writer.close().unwrap();
        sorter.close().unwrap();

        let sorter = DiskSorter::open(directory.path(), Some(test_options())).unwrap();
        assert!(!sorter.is_sorted());
        let mut writer = sorter.new_writer(&canceled).unwrap();
        for key_value in second {
            writer.put(&key_value.key, &key_value.value).unwrap();
        }
        writer.close().unwrap();
        key_values.sort_by(|left, right| left.key.cmp(&right.key));
        sorter.sort(&canceled).unwrap();
        assert!(sorter.is_sorted());
        let mut iterator = sorter.new_iterator(&canceled).unwrap();
        let actual = collect(iterator.as_mut()).unwrap();
        assert_eq!(actual.len(), key_values.len());
        for (actual, expected) in actual.iter().zip(&key_values) {
            assert_eq!(actual.key, expected.key);
            assert_eq!(actual.value, expected.value);
        }
        iterator.close().unwrap();
        sorter.close().unwrap();

        let sorter = DiskSorter::open(directory.path(), Some(test_options())).unwrap();
        assert!(sorter.is_sorted());
        let mut iterator = sorter.new_iterator(&canceled).unwrap();
        let actual = collect(iterator.as_mut()).unwrap();
        assert_eq!(actual.len(), key_values.len());
        for (actual, expected) in actual.iter().zip(&key_values) {
            assert_eq!(actual.key, expected.key);
            assert_eq!(actual.value, expected.value);
        }
        iterator.close().unwrap();
    }

    #[test]
    fn kv_stats_collector() {
        let key_values = [
            (b"aa".as_slice(), b"11".as_slice()),
            (b"bb".as_slice(), b"22".as_slice()),
            (b"cc".as_slice(), b"33".as_slice()),
            (b"dd".as_slice(), b"44".as_slice()),
            (b"ee".as_slice(), b"55".as_slice()),
        ];
        let cases = [
            (
                0,
                vec![
                    (4, b"aa".to_vec()),
                    (4, b"bb".to_vec()),
                    (4, b"cc".to_vec()),
                    (4, b"dd".to_vec()),
                    (4, b"ee".to_vec()),
                ],
            ),
            (
                4,
                vec![
                    (4, b"aa".to_vec()),
                    (4, b"bb".to_vec()),
                    (4, b"cc".to_vec()),
                    (4, b"dd".to_vec()),
                    (4, b"ee".to_vec()),
                ],
            ),
            (
                7,
                vec![
                    (8, b"bb".to_vec()),
                    (8, b"dd".to_vec()),
                    (4, b"ee".to_vec()),
                ],
            ),
            (50, vec![(20, b"ee".to_vec())]),
        ];
        for (bucket_size, expected) in cases {
            let mut collector = KvStatsCollector::new(bucket_size);
            for (key, value) in key_values {
                collector.add(key, value);
            }
            let actual: Vec<_> = collector
                .finish()
                .histogram
                .unwrap_or_default()
                .into_iter()
                .map(|bucket| (bucket.size, bucket.upper_bound))
                .collect();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn make_filename() {
        let directory = Path::new("/tmp");
        assert_eq!(
            super::make_filename(directory, 1),
            Path::new("/tmp/000001.sst")
        );
        assert_eq!(
            super::make_filename(directory, 123),
            Path::new("/tmp/000123.sst")
        );
        assert_eq!(
            super::make_filename(directory, 666_666),
            Path::new("/tmp/666666.sst")
        );
        assert_eq!(
            super::make_filename(directory, 7_777_777),
            Path::new("/tmp/7777777.sst")
        );
    }

    #[test]
    fn parse_filename() {
        for (name, expected) in [
            ("/tmp/1.sst", Some(1)),
            ("/tmp/123.sst", Some(123)),
            ("/tmp/000001.sst", Some(1)),
            ("/tmp/000123.sst", Some(123)),
            ("/tmp/666666.sst", Some(666_666)),
            ("/tmp/7777777.sst", Some(7_777_777)),
            ("/tmp/123.sst.tmp", None),
            (DISK_SORTER_SORTED_FILE, None),
        ] {
            assert_eq!(super::parse_filename(Path::new(name)), expected);
        }
    }

    fn write_sst_file(
        directory: &Path,
        file_num: i64,
        key_values: &[(&[u8], &[u8])],
    ) -> FileMetadata {
        let mut writer = SstWriter::new(directory, file_num, 8).unwrap();
        for (key, value) in key_values {
            writer.set(key, value).unwrap();
        }
        writer.close().unwrap()
    }

    #[test]
    fn sst_writer() {
        let directory = tempfile::tempdir().unwrap();
        let metadata = write_sst_file(
            directory.path(),
            13,
            &[
                (b"aa", b"11"),
                (b"bb", b"22"),
                (b"cc", b"33"),
                (b"dd", b"44"),
                (b"ee", b"55"),
            ],
        );
        assert!(super::make_filename(directory.path(), 13).is_file());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
        assert_eq!(metadata.file_num, 13);
        assert_eq!(metadata.start_key, b"aa");
        assert_eq!(metadata.end_key, b"ee\0");
        assert_eq!(metadata.last_key, b"ee");
        assert_eq!(
            metadata.kv_stats.histogram,
            Some(vec![
                KvStatsBucket {
                    size: 8,
                    upper_bound: b"bb".to_vec()
                },
                KvStatsBucket {
                    size: 8,
                    upper_bound: b"dd".to_vec()
                },
                KvStatsBucket {
                    size: 4,
                    upper_bound: b"ee".to_vec()
                },
            ])
        );
    }

    #[test]
    fn sst_writer_empty() {
        let directory = tempfile::tempdir().unwrap();
        let metadata = write_sst_file(directory.path(), 13, &[]);
        assert!(super::make_filename(directory.path(), 13).is_file());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
        assert!(metadata.start_key.is_empty());
        assert_eq!(metadata.end_key, [0]);
        assert!(metadata.last_key.is_empty());
        assert_eq!(metadata.kv_stats, KvStats::default());
    }

    #[test]
    fn sst_writer_error() {
        let directory = tempfile::tempdir().unwrap();
        let mut writer = SstWriter::new(directory.path(), 13, 0).unwrap();
        writer.set(b"bb", b"11").unwrap();
        assert!(writer.set(b"aa", b"22").is_err());
        assert!(writer.close().is_err());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn sst_reader_pool() {
        let directory = tempfile::tempdir().unwrap();
        write_sst_file(directory.path(), 1, &[]);
        let pool = ReaderPool::new(directory.path().to_path_buf());
        let first = pool.get(1).unwrap();
        let second = pool.get(1).unwrap();
        assert!(Arc::ptr_eq(&first.reader, &second.reader));
        drop(first);
        assert_eq!(pool.len(), 1);
        drop(second);
        assert_eq!(pool.len(), 0);
        assert!(std::panic::catch_unwind(AssertUnwindSafe(|| pool.release(1))).is_err());
    }

    #[test]
    fn sst_reader_pool_parallel() {
        let directory = tempfile::tempdir().unwrap();
        for file_num in 1..=3 {
            write_sst_file(directory.path(), file_num, &[]);
        }
        let pool = ReaderPool::new(directory.path().to_path_buf());
        std::thread::scope(|scope| {
            for index in 0..17 {
                let pool = Arc::clone(&pool);
                scope.spawn(move || {
                    for _ in 0..10_000 {
                        drop(pool.get(index % 3 + 1).unwrap());
                    }
                });
            }
        });
        assert_eq!(pool.len(), 0);
    }

    #[test]
    fn sst_iter() {
        let directory = tempfile::tempdir().unwrap();
        write_sst_file(
            directory.path(),
            1,
            &[
                (b"aa", b"11"),
                (b"bb", b"22"),
                (b"cc", b"33"),
                (b"dd", b"44"),
                (b"ee", b"55"),
            ],
        );
        let pool = ReaderPool::new(directory.path().to_path_buf());
        let mut iterator = SstIter::new(pool.get(1).unwrap()).unwrap();
        assert!(iterator.seek(b"bc"));
        assert_eq!(iterator.unsafe_key(), b"cc");
        assert_eq!(iterator.unsafe_value(), b"33");
        assert!(iterator.first());
        assert_eq!(iterator.unsafe_key(), b"aa");
        assert_eq!(iterator.unsafe_value(), b"11");
        assert!(iterator.next());
        assert_eq!(iterator.unsafe_key(), b"bb");
        assert_eq!(iterator.unsafe_value(), b"22");
        assert!(iterator.last());
        assert_eq!(iterator.unsafe_key(), b"ee");
        assert_eq!(iterator.unsafe_value(), b"55");
        assert!(!iterator.next());
        assert!(!iterator.valid());
        iterator.close().unwrap();
    }

    #[test]
    fn merging_iter() {
        let directory = tempfile::tempdir().unwrap();
        let mut files = vec![
            write_sst_file(
                directory.path(),
                1,
                &[
                    (b"a0", b"va0"),
                    (b"a1", b"va1"),
                    (b"e0", b"ve0"),
                    (b"e1", b"ve1"),
                ],
            ),
            write_sst_file(
                directory.path(),
                2,
                &[
                    (b"b0", b"vb0"),
                    (b"b1", b"vb1"),
                    (b"d0", b"vd0"),
                    (b"d1", b"vd1"),
                ],
            ),
            write_sst_file(
                directory.path(),
                3,
                &[
                    (b"c0", b"vc0"),
                    (b"c1", b"vc1"),
                    (b"g0", b"vg0"),
                    (b"g1", b"vg1"),
                ],
            ),
            write_sst_file(
                directory.path(),
                4,
                &[(b"f0", b"vf0"), (b"f1", b"vf1"), (b"h1", b"vh1")],
            ),
            write_sst_file(
                directory.path(),
                5,
                &[(b"f0", b"vf0"), (b"f2", b"vf2"), (b"h0", b"vh0")],
            ),
            write_sst_file(
                directory.path(),
                6,
                &[
                    (b"i0", b"vi0"),
                    (b"i1", b"vi1"),
                    (b"j0", b"vj0"),
                    (b"j1", b"vj1"),
                ],
            ),
        ];
        files.sort_by(|left, right| left.start_key.cmp(&right.start_key));
        let pool = ReaderPool::new(directory.path().to_path_buf());
        let open_pool = Arc::clone(&pool);
        let mut iterator = MergingIter::new(
            files,
            Arc::new(move |file| {
                Ok(Box::new(SstIter::new(open_pool.get(file.file_num)?)?) as Box<dyn Iterator>)
            }),
        );
        let actual = collect(&mut iterator).unwrap();
        let expected = [
            ("a0", "va0"),
            ("a1", "va1"),
            ("b0", "vb0"),
            ("b1", "vb1"),
            ("c0", "vc0"),
            ("c1", "vc1"),
            ("d0", "vd0"),
            ("d1", "vd1"),
            ("e0", "ve0"),
            ("e1", "ve1"),
            ("f0", "vf0"),
            ("f1", "vf1"),
            ("f2", "vf2"),
            ("g0", "vg0"),
            ("g1", "vg1"),
            ("h0", "vh0"),
            ("h1", "vh1"),
            ("i0", "vi0"),
            ("i1", "vi1"),
            ("j0", "vj0"),
            ("j1", "vj1"),
        ];
        assert_eq!(actual.len(), expected.len());
        for (actual, (key, value)) in actual.iter().zip(expected) {
            assert_eq!(actual.key, key.as_bytes());
            assert_eq!(actual.value, value.as_bytes());
        }
        assert!(iterator.seek(b""));
        assert_eq!(iterator.unsafe_key(), b"a0");
        assert!(!iterator.seek(b"k"));
        assert!(iterator.error().is_none());
        for key in expected.iter().rev().map(|(key, _)| key.as_bytes()) {
            assert!(iterator.seek(key));
            assert_eq!(iterator.unsafe_key(), key);
        }
        assert!(iterator.last());
        assert_eq!(iterator.unsafe_key(), b"j1");
        assert_eq!(iterator.unsafe_value(), b"vj1");
        assert!(!iterator.next());
        assert!(iterator.error().is_none());
        assert!(iterator.first());
        assert_eq!(iterator.unsafe_key(), b"a0");
        iterator.close().unwrap();
        assert_eq!(pool.len(), 0);
    }

    fn metadata(file_num: i64, start: &str, end: &str) -> FileMetadata {
        FileMetadata {
            file_num,
            start_key: start.as_bytes().to_vec(),
            end_key: end.as_bytes().to_vec(),
            ..Default::default()
        }
    }

    #[test]
    fn pick_compaction_files() {
        let cases = vec![
            (
                vec![
                    metadata(1, "a", "b"),
                    metadata(2, "b", "c"),
                    metadata(3, "c", "d"),
                ],
                2,
                vec![],
            ),
            (
                vec![
                    metadata(1, "a", "b"),
                    metadata(2, "b", "d"),
                    metadata(3, "c", "e"),
                ],
                2,
                vec![2, 3],
            ),
            (
                vec![
                    metadata(1, "a", "c"),
                    metadata(2, "b", "f"),
                    metadata(3, "d", "g"),
                    metadata(4, "e", "i"),
                    metadata(5, "h", "j"),
                ],
                2,
                vec![1, 2, 3, 4, 5],
            ),
            (
                vec![
                    metadata(1, "a", "c"),
                    metadata(2, "b", "f"),
                    metadata(3, "d", "g"),
                    metadata(4, "e", "i"),
                    metadata(5, "h", "j"),
                ],
                3,
                vec![2, 3, 4],
            ),
            (
                vec![
                    metadata(1, "a", "c"),
                    metadata(2, "b", "f"),
                    metadata(3, "d", "g"),
                    metadata(4, "e", "i"),
                    metadata(5, "h", "j"),
                ],
                4,
                vec![],
            ),
        ];
        for (files, threshold, expected) in cases {
            let mut actual: Vec<_> = super::pick_compaction_files(&files, threshold, None)
                .into_iter()
                .map(|file| file.file_num)
                .collect();
            actual.sort_unstable();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn split_compaction_files() {
        let mut files = vec![
            metadata(1, "a", "c"),
            metadata(2, "b", "f"),
            metadata(3, "d", "g"),
            metadata(4, "e", "i"),
            metadata(5, "h", "j"),
        ];
        let numbers = |groups: Vec<Vec<FileMetadata>>| {
            groups
                .into_iter()
                .map(|group| {
                    group
                        .into_iter()
                        .map(|file| file.file_num)
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            numbers(super::split_compaction_files(&mut files.clone(), 5)),
            vec![vec![1, 2, 3, 4, 5]]
        );
        assert_eq!(
            numbers(super::split_compaction_files(&mut files, 4)),
            vec![vec![1, 2, 3], vec![4, 5]]
        );

        let mut separate = vec![
            metadata(1, "a", "c"),
            metadata(2, "b", "f"),
            metadata(3, "d", "e"),
            metadata(4, "g", "i"),
            metadata(5, "h", "j"),
        ];
        assert_eq!(
            numbers(super::split_compaction_files(&mut separate, 3)),
            vec![vec![1, 2, 3], vec![4, 5]]
        );
    }

    fn with_buckets(
        file_num: i64,
        start: &[u8],
        end: &[u8],
        buckets: &[(usize, &[u8])],
    ) -> FileMetadata {
        FileMetadata {
            file_num,
            start_key: start.to_vec(),
            end_key: end.to_vec(),
            kv_stats: KvStats {
                histogram: Some(
                    buckets
                        .iter()
                        .map(|(size, key)| KvStatsBucket {
                            size: *size,
                            upper_bound: key.to_vec(),
                        })
                        .collect(),
                ),
            },
            ..Default::default()
        }
    }

    #[test]
    fn build_compactions() {
        let first = with_buckets(
            1,
            b"a",
            b"e\0",
            &[(20, b"b"), (23, b"c"), (17, b"d"), (25, b"e")],
        );
        let second = with_buckets(2, b"c", b"g\0", &[(21, b"d"), (22, b"f"), (20, b"g")]);
        let cases = vec![
            (
                vec![metadata(1, "a", "c"), metadata(2, "b", "d")],
                20,
                vec![vec![1, 2]],
            ),
            (
                vec![with_buckets(
                    1,
                    b"a",
                    b"e\0",
                    &[(20, b"b"), (23, b"c"), (21, b"d"), (25, b"e")],
                )],
                20,
                vec![vec![1], vec![1], vec![1], vec![1]],
            ),
            (vec![first.clone()], 50, vec![vec![1], vec![1]]),
            (vec![first, second], 50, vec![vec![1, 2], vec![1, 2]]),
        ];
        for (files, maximum_size, expected) in cases {
            let actual = super::build_compactions(&files, maximum_size);
            assert_eq!(actual.len(), expected.len());
            for (compaction, expected_files) in actual.iter().zip(expected) {
                assert_eq!(compaction.overlap_files.len(), expected_files.len());
                for (file, expected_file_num) in compaction.overlap_files.iter().zip(expected_files)
                {
                    assert_eq!(file.file_num, expected_file_num);
                }
            }
        }
    }
}
