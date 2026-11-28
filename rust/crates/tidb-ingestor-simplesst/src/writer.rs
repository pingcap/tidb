// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `writer.go`: the multi-file writer, its builder and summaries, range
//! properties, and the multi-file statistics merge sort plans with.

use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tidb_gocontext::Context;
use tidb_ingestor_engineapi::{ConflictInfo, OnDuplicateKey};
use tidb_lightning_membuf::{Buffer, Pool, SliceLocation};
use tidb_objstore::objectio::Writer as ObjWriter;
use tidb_objstore::storeapi::{Storage, WriterOption};

use crate::codec::{encode_multi_props, RangeProperty};
use crate::file::{KeyValueStore, DUP_SUFFIX, LENGTH_BYTES, STAT_SUFFIX};
use crate::util::{
    get_max_overlapping, remove_duplicates, remove_duplicates_more_than_two, Endpoint, EndpointTp,
};
use crate::{Error, Result};

/// Go `MultiFileStatNum`: files per [`MultipleFilesStat`].
pub const MULTI_FILE_STAT_NUM: usize = 500;
/// Go `DefaultPropSizeDist`: 1 MB.
pub const DEFAULT_PROP_SIZE_DIST: u64 = 1 << 20;
/// Go `DefaultPropKeysDist`: 8K keys.
pub const DEFAULT_PROP_KEYS_DIST: u64 = 8 * 1024;
/// Go `maxUploadWorkersPerThread`.
pub(crate) const MAX_UPLOAD_WORKERS_PER_THREAD: usize = 4;
/// Go `partitionHeader`.
const PARTITION_HEADER: &str = "p";
/// Go `maxMergeSortOverlapThreshold`.
pub const MAX_MERGE_SORT_OVERLAP_THRESHOLD: i64 = 4000;
/// Go `MaxMergeSortFileCountStep`.
pub const MAX_MERGE_SORT_FILE_COUNT_STEP: usize = 4000;
/// Go `MergeSortMaxSubtaskTargetFiles`.
pub const MERGE_SORT_MAX_SUBTASK_TARGET_FILES: usize = 16;
/// Go `MinUploadPartSize`: 5 MiB, the minimum for S3 and GCS.
pub const MIN_UPLOAD_PART_SIZE: i64 = 5 << 20;
/// Go `DefaultMemSizeLimit`: 256 MB.
pub const DEFAULT_MEM_SIZE_LIMIT: u64 = 256 << 20;
/// Go `DefaultBlockSize`: 16 MiB.
pub const DEFAULT_BLOCK_SIZE: usize = 16 << 20;
/// Go `flushKVsRetryTimes`.
const FLUSH_KVS_RETRY_TIMES: usize = 3;

/// Go `commonGetAdjustCount`.
fn common_get_adjust_count(is_overlap_threshold: bool, concurrency: i64) -> i64 {
    let concurrency = if concurrency <= 0 {
        tidb_log::error("concurrency is less than 0 or equal to 0, set to 1", &[]);
        1
    } else {
        concurrency
    };
    const MAX_OPENED_CONN_PER_CORE: i64 = 250;
    let cnt = MAX_OPENED_CONN_PER_CORE * concurrency;
    if is_overlap_threshold {
        cnt.min(MAX_MERGE_SORT_OVERLAP_THRESHOLD)
    } else {
        cnt.min(MAX_MERGE_SORT_FILE_COUNT_STEP as i64)
    }
}

/// Go `GetAdjustedMergeSortOverlapThreshold`.
#[must_use]
pub fn get_adjusted_merge_sort_overlap_threshold(concurrency: i64) -> i64 {
    common_get_adjust_count(true, concurrency)
}

/// Go `GetAdjustedMergeSortFileCountStep`.
#[must_use]
pub fn get_adjusted_merge_sort_file_count_step(concurrency: i64) -> usize {
    common_get_adjust_count(false, concurrency) as usize
}

/// Go `GetAdjustedBlockSize`: a block size over 1.1 times the buffer size
/// becomes the buffer size, so many small writers do not OOM.
#[must_use]
pub fn get_adjusted_block_size(total_buf_size: u64, def_block_size: usize) -> usize {
    let aligned = tidb_lightning_membuf::get_aligned_size(total_buf_size, def_block_size as u64);
    if aligned as f64 / total_buf_size as f64 > 1.1 {
        return total_buf_size as usize;
    }
    def_block_size
}

/// Go `RangePropertiesCollector`.
#[derive(Clone, Debug, Default)]
pub struct RangePropertiesCollector {
    pub(crate) props: Vec<RangeProperty>,
    pub(crate) curr_prop: RangeProperty,
    prop_size_dist: u64,
    prop_keys_dist: u64,
}

impl RangePropertiesCollector {
    /// Go `NewRangePropertiesCollector`.
    #[must_use]
    pub fn new(prop_size_dist: u64, prop_keys_dist: u64) -> Self {
        Self {
            props: Vec::with_capacity(1024),
            curr_prop: RangeProperty::default(),
            prop_size_dist,
            prop_keys_dist,
        }
    }

    /// Go `CurrProp`.
    #[must_use]
    pub fn curr_prop(&self) -> &RangeProperty {
        &self.curr_prop
    }

    /// Go `onNextEncodedData`: `size` is the file size after `data`.
    pub(crate) fn on_next_encoded_data(&mut self, data: &[u8], size: u64) {
        let key_len =
            u64::from_be_bytes(data[..LENGTH_BYTES].try_into().expect("8 bytes")) as usize;
        let key = &data[2 * LENGTH_BYTES..2 * LENGTH_BYTES + key_len];
        if self.curr_prop.first_key.is_empty() {
            self.curr_prop.first_key = key.to_vec();
        }
        self.curr_prop.last_key = key.to_vec();
        self.curr_prop.size += (data.len() - 2 * LENGTH_BYTES) as u64;
        self.curr_prop.keys += 1;
        if self.curr_prop.size >= self.prop_size_dist || self.curr_prop.keys >= self.prop_keys_dist
        {
            self.props.push(self.curr_prop.clone());
            self.curr_prop.first_key = Vec::new();
            self.curr_prop.offset = size;
            self.curr_prop.keys = 0;
            self.curr_prop.size = 0;
        }
    }

    /// Go `onFileEnd`.
    pub(crate) fn on_file_end(&mut self) {
        if self.curr_prop.keys > 0 {
            self.props.push(self.curr_prop.clone());
        }
    }

    /// Go `Reset`.
    pub fn reset(&mut self) {
        self.props.clear();
        self.curr_prop = RangeProperty::default();
    }

    /// Go `Encode`.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(1024);
        encode_multi_props(&mut b, &self.props);
        b
    }
}

/// Go `WriterSummary`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriterSummary {
    /// Go `WriterID`.
    pub writer_id: String,
    /// Go `GroupOffset`.
    pub group_offset: i64,
    /// Go `Seq`.
    pub seq: i64,
    /// Go `Min`: inclusive; empty when nothing was written.
    pub min: Vec<u8>,
    /// Go `Max`: inclusive.
    pub max: Vec<u8>,
    /// Go `TotalSize`: duplicates may be excluded, per `onDup`.
    pub total_size: u64,
    /// Go `TotalCnt`.
    pub total_cnt: u64,
    /// Go `KVFileCount`.
    pub kv_file_count: i64,
    /// Go `MultipleFilesStats`.
    pub multiple_files_stats: Vec<MultipleFilesStat>,
    /// Go `ConflictInfo`.
    pub conflict_info: ConflictInfo,
}

/// Go `OnWriterCloseFunc`.
pub type OnWriterCloseFunc = Arc<dyn Fn(&WriterSummary) + Send + Sync>;

/// Go `tikv.Codec.EncodeKey`, applied to every key a writer gets.
pub type KeyCodec = Arc<dyn Fn(&[u8]) -> Vec<u8> + Send + Sync>;

/// Go `WriterBuilder`.
#[derive(Clone)]
pub struct WriterBuilder {
    group_offset: i64,
    mem_size_limit: u64,
    block_size: usize,
    prop_size_dist: u64,
    prop_keys_dist: u64,
    on_close: OnWriterCloseFunc,
    tikv_codec: Option<KeyCodec>,
    on_dup: OnDuplicateKey,
}

impl Default for WriterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl WriterBuilder {
    /// Go `NewWriterBuilder`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            group_offset: 0,
            mem_size_limit: DEFAULT_MEM_SIZE_LIMIT,
            block_size: DEFAULT_BLOCK_SIZE,
            prop_size_dist: DEFAULT_PROP_SIZE_DIST,
            prop_keys_dist: DEFAULT_PROP_KEYS_DIST,
            on_close: Arc::new(|_| {}),
            tikv_codec: None,
            on_dup: OnDuplicateKey::Ignore,
        }
    }

    /// Go `SetMemorySizeLimit`: a writer flushes a file once its buffered
    /// data exceeds this; for a one-file writer it is the buffer size.
    #[must_use]
    pub fn set_memory_size_limit(mut self, size: u64) -> Self {
        self.mem_size_limit = size;
        self
    }

    /// Go `SetPropSizeDistance`.
    #[must_use]
    pub fn set_prop_size_distance(mut self, dist: u64) -> Self {
        self.prop_size_dist = dist;
        self
    }

    /// Go `SetPropKeysDistance`.
    #[must_use]
    pub fn set_prop_keys_distance(mut self, dist: u64) -> Self {
        self.prop_keys_dist = dist;
        self
    }

    /// Go `SetOnCloseFunc`.
    #[must_use]
    pub fn set_on_close_func(mut self, on_close: OnWriterCloseFunc) -> Self {
        self.on_close = on_close;
        self
    }

    /// Go `SetBlockSize`.
    #[must_use]
    pub fn set_block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }

    /// Go `SetGroupOffset`.
    #[must_use]
    pub fn set_group_offset(mut self, offset: i64) -> Self {
        self.group_offset = offset;
        self
    }

    /// Go `SetTiKVCodec`.
    #[must_use]
    pub fn set_tikv_codec(mut self, codec: Option<KeyCodec>) -> Self {
        self.tikv_codec = codec;
        self
    }

    /// Go `SetOnDup`.
    #[must_use]
    pub fn set_on_dup(mut self, on_dup: OnDuplicateKey) -> Self {
        self.on_dup = on_dup;
        self
    }

    fn new_buffer(&self) -> Buffer {
        let pool = Arc::new(Pool::new([
            tidb_lightning_membuf::with_block_num(0),
            tidb_lightning_membuf::with_block_size(self.block_size as isize),
        ]));
        pool.new_buffer([tidb_lightning_membuf::with_buffer_memory_limit(
            self.mem_size_limit,
        )])
    }

    /// Go `Build`: the writer's files are under `{prefix}/{writer_id}`.
    #[must_use]
    pub fn build(&self, store: Arc<dyn Storage>, prefix: &str, writer_id: &str) -> Writer {
        let filename_prefix = tidb_objstore::gopath::join(&[prefix, writer_id]);
        Writer {
            rc: RangePropertiesCollector::new(self.prop_size_dist, self.prop_keys_dist),
            store,
            kv_buffer: self.new_buffer(),
            kv_locations: Vec::new(),
            common_prefix: Vec::new(),
            kv_size: 0,
            current_seq: 0,
            rnd: tidb_go_rand::Rand::new(get_hash(&filename_prefix)),
            filename_prefix,
            writer_id: writer_id.to_owned(),
            group_offset: self.group_offset,
            on_close: Arc::clone(&self.on_close),
            on_dup: self.on_dup,
            closed: false,
            batch_size: 0,
            multi_file_stats: Vec::new(),
            file_min_keys: Vec::with_capacity(MULTI_FILE_STAT_NUM),
            file_max_keys: Vec::with_capacity(MULTI_FILE_STAT_NUM),
            min_key: Vec::new(),
            max_key: Vec::new(),
            total_size: 0,
            total_cnt: 0,
            kv_file_count: 0,
            tikv_codec: self.tikv_codec.clone(),
            conflict_info: ConflictInfo::default(),
        }
    }

    /// Go `BuildOneFile`: one data file and one stat file under
    /// `{prefix}/{writer_id}`.
    #[must_use]
    pub fn build_one_file(
        &self,
        store: Arc<dyn Storage>,
        prefix: &str,
        writer_id: &str,
    ) -> crate::OneFileWriter {
        let filename_prefix = tidb_objstore::gopath::join(&[prefix, writer_id]);
        crate::onefile_writer::OneFileWriter::new(
            store,
            self.new_buffer(),
            RangePropertiesCollector::new(self.prop_size_dist, self.prop_keys_dist),
            tidb_go_rand::Rand::new(get_hash(&filename_prefix)),
            filename_prefix,
            writer_id.to_owned(),
            Arc::clone(&self.on_close),
            self.on_dup,
        )
    }
}

/// Go encoding/json of a `[]byte` (`kv.Key`): standard base64, `null` for
/// an empty key.
pub mod go_bytes {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    /// Serializes `value`.
    ///
    /// # Errors
    /// The serializer failed.
    pub fn serialize<S: Serializer>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        if value.is_empty() {
            serializer.serialize_none()
        } else {
            serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(value))
        }
    }

    /// Deserializes a `[]byte`.
    ///
    /// # Errors
    /// The value is not base64 or null.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        Option::<String>::deserialize(deserializer)?.map_or(Ok(Vec::new()), |text| {
            base64::engine::general_purpose::STANDARD
                .decode(text)
                .map_err(serde::de::Error::custom)
        })
    }
}

/// Go `MultipleFilesStat`: statistics of up to [`MULTI_FILE_STAT_NUM`]
/// files, used to estimate how much the data overlaps.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MultipleFilesStat {
    /// Go `MinKey`.
    #[serde(rename = "min-key", with = "go_bytes")]
    pub min_key: Vec<u8>,
    /// Go `MaxKey`.
    #[serde(rename = "max-key", with = "go_bytes")]
    pub max_key: Vec<u8>,
    /// Go `Filenames`: `[data file, stat file]` pairs sorted by the data
    /// file's first key.
    #[serde(rename = "filenames", deserialize_with = "null_as_empty")]
    pub filenames: Vec<[String; 2]>,
    /// Go `MaxOverlappingNum`.
    #[serde(rename = "max-overlapping-num")]
    pub max_overlapping_num: i64,
}

/// Go decodes a JSON `null` slice as nil.
fn null_as_empty<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Vec<[String; 2]>, D::Error> {
    Ok(Option::<Vec<[String; 2]>>::deserialize(d)?.unwrap_or_default())
}

impl MultipleFilesStat {
    /// Go `Build`: sorts `start_keys` together with the file names.
    pub fn build(&mut self, start_keys: &mut [Vec<u8>], end_keys: &[Vec<u8>]) {
        if start_keys.is_empty() {
            return;
        }
        self.min_key = start_keys[0].clone();
        self.max_key = end_keys[0].clone();
        for i in 1..start_keys.len() {
            if self.min_key > start_keys[i] {
                self.min_key = start_keys[i].clone();
            }
            if self.max_key < end_keys[i] {
                self.max_key = end_keys[i].clone();
            }
        }
        // Go `sort.Sort(&startKeysAndFiles{startKeys, m.Filenames})`.
        // `sort.Sort` and `slices.SortFunc` are generated from one template,
        // so they compare and swap alike.
        let mut paired: Vec<(Vec<u8>, [String; 2])> = start_keys
            .iter_mut()
            .map(std::mem::take)
            .zip(std::mem::take(&mut self.filenames))
            .collect();
        tidb_goruntime::go_pdqsort::go_sort_func_by(&mut paired, |a, b| a.0.cmp(&b.0));
        let mut filenames = Vec::with_capacity(paired.len());
        for (slot, (key, files)) in start_keys.iter_mut().zip(paired) {
            *slot = key;
            filenames.push(files);
        }
        self.filenames = filenames;

        let mut points = Vec::with_capacity(start_keys.len() * 2);
        for k in start_keys {
            points.push(Endpoint {
                key: k.clone(),
                tp: EndpointTp::InclusiveStart,
                weight: 1,
            });
        }
        for k in end_keys {
            points.push(Endpoint {
                key: k.clone(),
                tp: EndpointTp::InclusiveEnd,
                weight: 1,
            });
        }
        self.max_overlapping_num = get_max_overlapping(&mut points);
    }
}

/// Go `GetMaxOverlappingTotal`: the overlapping level assuming the worst
/// overlap of `stats`.
#[must_use]
pub fn get_max_overlapping_total(stats: &[MultipleFilesStat]) -> i64 {
    let mut points = Vec::with_capacity(stats.len() * 2);
    for s in stats {
        points.push(Endpoint {
            key: s.min_key.clone(),
            tp: EndpointTp::InclusiveStart,
            weight: s.max_overlapping_num,
        });
    }
    for s in stats {
        points.push(Endpoint {
            key: s.max_key.clone(),
            tp: EndpointTp::InclusiveEnd,
            weight: s.max_overlapping_num,
        });
    }
    get_max_overlapping(&mut points)
}

/// Go `Writer`: buffers KV pairs and, whenever its buffer is full, sorts
/// them into a new data file and stat file.
/// Go `keyHintBytes`.
const KEY_HINT_BYTES: usize = 8;

/// Go `condenseLocation` (4239a9df1b5): a buffered KV's location with an
/// eight-byte sort hint, so full 64-bit integer handles (including
/// AUTO_RANDOM and SHARD_ROW_ID_BITS keys, whose high shard bits and low row
/// IDs are separated by zero bytes) compare in registers without reading the
/// buffer. Go drops the slice length (its buffer reads to the block end);
/// Rust's buffer hands out bounded slices, so the location keeps it.
#[derive(Clone, Copy, Default)]
struct CondenseLocation {
    key_hint: u64,
    loc: SliceLocation,
}

impl std::ops::Deref for CondenseLocation {
    type Target = SliceLocation;

    fn deref(&self) -> &SliceLocation {
        &self.loc
    }
}

pub struct Writer {
    store: Arc<dyn Storage>,
    writer_id: String,
    group_offset: i64,
    current_seq: i64,
    filename_prefix: String,
    rnd: tidb_go_rand::Rand,
    rc: RangePropertiesCollector,
    kv_buffer: Buffer,
    kv_locations: Vec<CondenseLocation>,
    kv_size: i64,
    /// Go `commonPrefix`: the common prefix of all keys written since the
    /// last flush.
    common_prefix: Vec<u8>,
    on_close: OnWriterCloseFunc,
    on_dup: OnDuplicateKey,
    closed: bool,
    batch_size: u64,
    multi_file_stats: Vec<MultipleFilesStat>,
    file_min_keys: Vec<Vec<u8>>,
    file_max_keys: Vec<Vec<u8>>,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    total_size: u64,
    total_cnt: u64,
    kv_file_count: i64,
    tikv_codec: Option<KeyCodec>,
    conflict_info: ConflictInfo,
}

/// The data and stat paths and their writers.
type StorageWriters = (String, String, Box<dyn ObjWriter>, Box<dyn ObjWriter>);

/// Go `getKeyByLoc` over an encoded block.
fn key_of(block: &[u8]) -> &[u8] {
    let key_len = u64::from_be_bytes(block[..LENGTH_BYTES].try_into().expect("8 bytes")) as usize;
    &block[2 * LENGTH_BYTES..2 * LENGTH_BYTES + key_len]
}

/// Go `getValueByLoc` over an encoded block.
fn value_of(block: &[u8]) -> &[u8] {
    let key_len = u64::from_be_bytes(block[..LENGTH_BYTES].try_into().expect("8 bytes")) as usize;
    &block[2 * LENGTH_BYTES + key_len..]
}

/// Go `ErrFoundDuplicateKeys.FastGenByArgs(key, value)`.
pub fn found_duplicate_keys(key: &[u8], value: &[u8]) -> Error {
    Error::FoundDuplicateKeys {
        key: key.to_vec(),
        value: value.to_vec(),
        error: tidb_lightning_common::errors::ERR_FOUND_DUPLICATE_KEYS.generate(format!(
            "found duplicate key '{}', value '{}'",
            hex::encode(key),
            hex::encode(value)
        )),
    }
}

impl Writer {
    /// Go `WriteRow`.
    ///
    /// # Errors
    /// A full buffer failed to flush, or the pair is larger than a block.
    pub fn write_row(&mut self, ctx: &Context, key: &[u8], val: &[u8]) -> Result<()> {
        let encoded;
        let key = match &self.tikv_codec {
            Some(codec) => {
                encoded = codec(key);
                encoded.as_slice()
            }
            None => key,
        };
        let length = key.len() + val.len() + LENGTH_BYTES * 2;
        let (mut data, mut loc) = self
            .kv_buffer
            .alloc_bytes_with_slice_location(length as isize);
        if data.is_none() {
            self.flush_kvs(ctx, false)?;
            (data, loc) = self
                .kv_buffer
                .alloc_bytes_with_slice_location(length as isize);
        }
        let Some(data) = data else {
            return Err(Error::Other(format!(
                "failed to allocate kv buffer: {length}"
            )));
        };
        crate::file::encode_to_buf(&mut data.as_mut_slice(), key, val);
        self.update_common_prefix(key);
        self.kv_locations.push(CondenseLocation { key_hint: 0, loc });
        self.kv_size += (key.len() + val.len()) as i64;
        self.batch_size += length as u64;
        Ok(())
    }

    /// Go `WrittenBytes`.
    #[must_use]
    pub fn written_bytes(&self) -> i64 {
        self.total_size as i64
    }

    /// Go `Close`: flushes the buffer and reports the summary.
    ///
    /// # Errors
    /// The writer was closed already, or the last flush failed.
    pub fn close(&mut self, ctx: &Context) -> Result<()> {
        if self.closed {
            return Err(Error::Other(format!(
                "writer {} has been closed",
                self.writer_id
            )));
        }
        self.closed = true;
        let result = self.flush_kvs(ctx, true);
        self.kv_buffer.destroy();
        result?;
        tidb_log::info(
            "close writer",
            &[
                str_field("writerID", &self.writer_id),
                str_field("minKey", &hex::encode(&self.min_key)),
                str_field("maxKey", &hex::encode(&self.max_key)),
                int_field("kv-file-count", self.kv_file_count),
                int_field("dup-file-count", self.conflict_info.files.len() as i64),
                str_field(
                    "total-size",
                    &tidb_config_configtypes::bytes_size(self.total_size as f64),
                ),
                int_field("total-kv-cnt", self.total_cnt as i64),
            ],
        );
        self.kv_locations = Vec::new();
        (self.on_close)(&WriterSummary {
            writer_id: self.writer_id.clone(),
            group_offset: self.group_offset,
            seq: self.current_seq,
            min: self.min_key.clone(),
            max: self.max_key.clone(),
            total_size: self.total_size,
            total_cnt: self.total_cnt,
            kv_file_count: self.kv_file_count,
            multiple_files_stats: self.multi_file_stats.clone(),
            conflict_info: self.conflict_info.clone(),
        });
        Ok(())
    }

    /// Go `recordMinMax`.
    fn record_min_max(&mut self, new_min: &[u8], new_max: &[u8]) {
        if self.min_key.is_empty() || new_min < self.min_key.as_slice() {
            self.min_key = new_min.to_vec();
        }
        if self.max_key.is_empty() || new_max > self.max_key.as_slice() {
            self.max_key = new_max.to_vec();
        }
    }

    fn key_at(&self, loc: &SliceLocation) -> Vec<u8> {
        key_of(&self.kv_buffer.get_slice(loc).as_slice()).to_vec()
    }

    /// Go `updateCommonPrefix`. An empty prefix is valid once keys with
    /// different leading bytes arrive.
    fn update_common_prefix(&mut self, key: &[u8]) {
        if self.kv_locations.is_empty() {
            self.common_prefix = key.to_vec();
            return;
        }
        if self.common_prefix.is_empty() {
            return;
        }
        let common_len = self.common_prefix.len().min(key.len());
        let differs = (0..common_len).find(|&i| self.common_prefix[i] != key[i]);
        self.common_prefix.truncate(differs.unwrap_or(common_len));
    }

    /// Go `updateKeyHints`: fills every buffered KV's hint and answers
    /// whether hint comparisons completely determine key order and
    /// equality -- all keys have the same length and their suffix beyond
    /// the common prefix is at most eight bytes (every IntHandle / BIGINT /
    /// AUTO_RANDOM record key). Zero-padded variable-length suffixes ("x" vs
    /// "x\x00") are not exact, so equal hints fall back to the suffixes.
    fn update_key_hints(&mut self) -> bool {
        if self.kv_locations.is_empty() {
            return true;
        }
        let prefix_len = self.common_prefix.len();
        let mut first_key_len = None;
        let mut exact_hints = true;
        for i in 0..self.kv_locations.len() {
            let key = self.key_at(&self.kv_locations[i]);
            match first_key_len {
                None => {
                    first_key_len = Some(key.len());
                    if key.len() - prefix_len > KEY_HINT_BYTES {
                        exact_hints = false;
                    }
                }
                Some(len) if len != key.len() => exact_hints = false,
                Some(_) => {}
            }
            let suffix = &key[prefix_len..];
            let mut hint = [0u8; KEY_HINT_BYTES];
            let n = suffix.len().min(KEY_HINT_BYTES);
            hint[..n].copy_from_slice(&suffix[..n]);
            self.kv_locations[i].key_hint = u64::from_be_bytes(hint);
        }
        exact_hints
    }

    /// Go `flushKVs`.
    fn flush_kvs(&mut self, ctx: &Context, from_close: bool) -> Result<()> {
        if self.kv_locations.is_empty() {
            return Ok(());
        }
        let sort_start = Instant::now();
        let mut dup_found = false;
        let mut dup_loc = CondenseLocation::default();
        // Go `sortKVs`: the hints first; with exact hints they decide
        // alone, else equal hints fall back to the suffixes beyond the
        // common prefix.
        let exact_hints = self.update_key_hints();
        let prefix_len = self.common_prefix.len();
        {
            let mut compare = |location: &CondenseLocation, other: &CondenseLocation, a: &[u8], b: &[u8]| {
                let res = match location.key_hint.cmp(&other.key_hint) {
                    std::cmp::Ordering::Equal if exact_hints => std::cmp::Ordering::Equal,
                    std::cmp::Ordering::Equal => key_of(a)[prefix_len..].cmp(&key_of(b)[prefix_len..]),
                    ordering => return ordering,
                };
                if res.is_eq() && !dup_found {
                    dup_found = true;
                    dup_loc = *location;
                }
                res
            };
            if exact_hints {
                tidb_goruntime::go_pdqsort::go_sort_func_by(&mut self.kv_locations, |i, j| compare(i, j, &[], &[]));
            } else if let Some(slices) = self.kv_buffer.try_borrow_slices() {
                tidb_goruntime::go_pdqsort::go_sort_func_by(&mut self.kv_locations, |i, j| {
                    compare(i, j, slices.get_slice(i), slices.get_slice(j))
                });
            } else {
                let buffer = &self.kv_buffer;
                tidb_goruntime::go_pdqsort::go_sort_func_by(&mut self.kv_locations, |i, j| {
                    let (a, b) = (buffer.get_slice(i), buffer.get_slice(j));
                    let (a, b) = (a.as_slice(), b.as_slice());
                    compare(i, j, &a, &b)
                });
            }
        }
        let sort_duration = sort_start.elapsed();
        observe_write("sort", sort_duration, self.batch_size);

        let batch_kv_cnt = self.kv_locations.len();
        let mut dup_locs = Vec::new();
        let mut dup_cnt = 0;
        if dup_found {
            let buffer = &self.kv_buffer;
            let key_getter =
                |loc: &CondenseLocation| key_of(&buffer.get_slice(loc).as_slice()).to_vec();
            match self.on_dup {
                OnDuplicateKey::Ignore => {}
                OnDuplicateKey::Record => {
                    // Without a global view, keep 2 of each key so a later
                    // step still finds them.
                    let (out, removed, total) = remove_duplicates_more_than_two(
                        std::mem::take(&mut self.kv_locations),
                        key_getter,
                    );
                    (self.kv_locations, dup_locs, dup_cnt) = (out, removed, total);
                    self.kv_size = self.re_calculate_kv_size();
                }
                OnDuplicateKey::Remove => {
                    let (out, _, total) = remove_duplicates(
                        std::mem::take(&mut self.kv_locations),
                        key_getter,
                        false,
                    );
                    (self.kv_locations, dup_cnt) = (out, total);
                    self.kv_size = self.re_calculate_kv_size();
                }
                OnDuplicateKey::Error => {
                    let block = buffer.get_slice(&dup_loc);
                    let block = block.as_slice();
                    return Err(found_duplicate_keys(key_of(&block), value_of(&block)));
                }
            }
        }

        let write_start = Instant::now();
        let mut files = (String::new(), String::new(), String::new());
        // With Record, no kept pair means no recorded pair either.
        if !self.kv_locations.is_empty() {
            let mut result = Ok(files.clone());
            for i in 0..FLUSH_KVS_RETRY_TIMES {
                result = self.flush_sorted_kvs(ctx, &dup_locs);
                if result.is_ok() || ctx.err().is_some() {
                    break;
                }
                if let Err(e) = &result {
                    tidb_log::warn(
                        "flush sorted kv failed",
                        &[
                            str_field("error", &e.to_string()),
                            int_field("retry-count", i as i64),
                        ],
                    );
                }
            }
            files = result?;
        }
        let write_duration = write_start.elapsed();
        tidb_log::info(
            "flush kv",
            &[
                int_field("bytes", self.batch_size as i64),
                int_field("kv-cnt", batch_kv_cnt as i64),
                str_field("writer-id", &self.writer_id),
                str_field("on-dup", &self.on_dup.to_string()),
                int_field("dup-cnt", dup_cnt as i64),
                int_field("recorded-dup-cnt", dup_locs.len() as i64),
                int_field("write-time-ms", write_duration.as_millis() as i64),
            ],
        );
        observe_write("sort_and_write", sort_start.elapsed(), self.batch_size);

        // The 500-file statistics.
        if !self.kv_locations.is_empty() {
            self.total_cnt += self.kv_locations.len() as u64;
            self.total_size += self.kv_size as u64;
            self.kv_file_count += 1;
            let min_key = self.key_at(&self.kv_locations[0]);
            let max_key = self.key_at(&self.kv_locations[self.kv_locations.len() - 1]);
            self.record_min_max(&min_key, &max_key);
            self.add_new_kv_file_2_multi_file_stats(&files.0, &files.1, min_key, max_key);
        }
        if from_close && !self.multi_file_stats.is_empty() {
            let last = self.multi_file_stats.len() - 1;
            self.multi_file_stats[last].build(&mut self.file_min_keys, &self.file_max_keys);
        }
        if !dup_locs.is_empty() {
            self.conflict_info.merge(&ConflictInfo {
                count: dup_locs.len() as u64,
                files: vec![files.2],
            });
        }
        self.kv_locations.clear();
        self.kv_size = 0;
        self.common_prefix.clear();
        self.kv_buffer.reset();
        self.batch_size = 0;
        self.current_seq += 1;
        Ok(())
    }

    /// Go `addNewKVFile2MultiFileStats`.
    fn add_new_kv_file_2_multi_file_stats(
        &mut self,
        data_file: &str,
        stat_file: &str,
        min_key: Vec<u8>,
        max_key: Vec<u8>,
    ) {
        let l = self.multi_file_stats.len();
        if l == 0 || self.multi_file_stats[l - 1].filenames.len() == MULTI_FILE_STAT_NUM {
            if l > 0 {
                self.multi_file_stats[l - 1].build(&mut self.file_min_keys, &self.file_max_keys);
            }
            self.multi_file_stats.push(MultipleFilesStat {
                filenames: Vec::with_capacity(MULTI_FILE_STAT_NUM),
                ..MultipleFilesStat::default()
            });
            self.file_min_keys.clear();
            self.file_max_keys.clear();
        }
        let last = self.multi_file_stats.len() - 1;
        self.multi_file_stats[last]
            .filenames
            .push([data_file.to_owned(), stat_file.to_owned()]);
        self.file_min_keys.push(min_key);
        self.file_max_keys.push(max_key);
    }

    /// Go `flushSortedKVs`: the data, stat and dup file paths.
    fn flush_sorted_kvs(
        &mut self,
        ctx: &Context,
        dup_locs: &[CondenseLocation],
    ) -> Result<(String, String, String)> {
        let write_start = Instant::now();
        let (data_file, stat_file, mut data_writer, mut stat_writer) =
            self.create_storage_writer(ctx)?;
        let written = (|| -> Result<()> {
            self.rc.reset();
            let mut kv_store = KeyValueStore::new(ctx, data_writer.as_mut(), Some(&mut self.rc));
            for loc in &self.kv_locations {
                kv_store.add_encoded_data(&self.kv_buffer.get_slice(loc).as_slice())?;
            }
            kv_store.finish();
            Ok(())
        })();
        if let Err(e) = written {
            let _ = data_writer.close(ctx);
            let _ = stat_writer.close(ctx);
            return Err(e);
        }
        let encoded_stat = self.rc.encode();
        if let Err(e) = stat_writer.write(ctx, &encoded_stat) {
            let _ = data_writer.close(ctx);
            let _ = stat_writer.close(ctx);
            return Err(e.into());
        }
        if let Err(e) = data_writer.close(ctx) {
            let _ = stat_writer.close(ctx);
            return Err(e.into());
        }
        stat_writer.close(ctx)?;
        let dup_path = if dup_locs.is_empty() {
            String::new()
        } else {
            self.write_dup_kvs(ctx, dup_locs)?
        };
        let write_duration = write_start.elapsed();
        tidb_log::info(
            "flush sorted kv",
            &[
                int_field("bytes", self.batch_size as i64),
                int_field("stat-size", encoded_stat.len() as i64),
                int_field("write-time-ms", write_duration.as_millis() as i64),
            ],
        );
        observe_write("write", write_duration, self.batch_size);
        Ok((data_file, stat_file, dup_path))
    }

    /// Go `writeDupKVs`.
    fn write_dup_kvs(&mut self, ctx: &Context, locs: &[CondenseLocation]) -> Result<String> {
        let (dup_path, mut dup_writer) = self.create_dup_writer(ctx)?;
        let written = (|| -> Result<()> {
            let mut dup_store = KeyValueStore::new(ctx, dup_writer.as_mut(), None);
            for loc in locs {
                dup_store.add_encoded_data(&self.kv_buffer.get_slice(loc).as_slice())?;
            }
            dup_store.finish();
            Ok(())
        })();
        if let Err(e) = written {
            let _ = dup_writer.close(ctx);
            return Err(e);
        }
        dup_writer.close(ctx)?;
        Ok(dup_path)
    }

    /// Go `reCalculateKVSize`.
    fn re_calculate_kv_size(&self) -> i64 {
        self.kv_locations
            .iter()
            .map(|loc| i64::from(loc.length) - 2 * LENGTH_BYTES as i64)
            .sum()
    }

    /// Go `createStorageWriter`: each path draws its own partition prefix.
    fn create_storage_writer(&mut self, ctx: &Context) -> Result<StorageWriters> {
        let seq = self.current_seq.to_string();
        let data_path = tidb_objstore::gopath::join(&[&self.get_partitioned_prefix(), &seq]);
        let opt = WriterOption {
            concurrency: 20,
            part_size: MIN_UPLOAD_PART_SIZE,
        };
        let data_writer = self.store.create(ctx, &data_path, Some(&opt))?;
        let stat_path =
            tidb_objstore::gopath::join(&[&(self.get_partitioned_prefix() + STAT_SUFFIX), &seq]);
        match self.store.create(ctx, &stat_path, Some(&opt)) {
            Ok(stat_writer) => Ok((data_path, stat_path, data_writer, stat_writer)),
            Err(e) => {
                let mut data_writer = data_writer;
                let _ = data_writer.close(ctx);
                Err(e.into())
            }
        }
    }

    /// Go `createDupWriter`.
    fn create_dup_writer(&mut self, ctx: &Context) -> Result<(String, Box<dyn ObjWriter>)> {
        let seq = self.current_seq.to_string();
        let path =
            tidb_objstore::gopath::join(&[&(self.get_partitioned_prefix() + DUP_SUFFIX), &seq]);
        let opt = WriterOption {
            concurrency: 20,
            part_size: MIN_UPLOAD_PART_SIZE,
        };
        let writer = self.store.create(ctx, &path, Some(&opt))?;
        Ok((path, writer))
    }

    fn get_partitioned_prefix(&mut self) -> String {
        rand_partitioned_prefix(&self.filename_prefix, &mut self.rnd)
    }
}

pub(crate) fn str_field(key: &'static str, value: &str) -> tidb_log::Field {
    tidb_log::Field::new(key, tidb_log::Value::Str(value.to_owned()))
}

pub(crate) fn int_field(key: &'static str, value: i64) -> tidb_log::Field {
    tidb_log::Field::new(key, tidb_log::Value::I64(value))
}

/// Go `GlobalSortWriteToCloudStorageDuration` / `Rate` for one phase.
fn observe_write(phase: &str, duration: std::time::Duration, bytes: u64) {
    let secs = duration.as_secs_f64();
    tidb_metrics::globalsort::WRITE_TO_CLOUD_STORAGE_DURATION
        .with_label_values(&[phase])
        .observe(secs);
    tidb_metrics::globalsort::WRITE_TO_CLOUD_STORAGE_RATE
        .with_label_values(&[phase])
        .observe(bytes as f64 / 1024.0 / 1024.0 / secs);
}

/// Go `randPartitionedPrefix`: `p` and 8 binary digits of a random byte, in
/// front of `prefix`, so object storage can partition the keys.
pub(crate) fn rand_partitioned_prefix(prefix: &str, rnd: &mut tidb_go_rand::Rand) -> String {
    let partition = format!("{PARTITION_HEADER}{:08b}", rnd.intn(256));
    tidb_objstore::gopath::join(&[&partition, prefix])
}

/// Go `IsValidPartition`.
#[must_use]
pub fn is_valid_partition(input: &[u8]) -> bool {
    input.len() == 9
        && input[0] == PARTITION_HEADER.as_bytes()[0]
        && input[1..].iter().all(|c| *c == b'0' || *c == b'1')
}

/// Go `getHash`: FNV-1a 64 as `int64`.
pub(crate) fn get_hash(s: &str) -> i64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h as i64
}

/// Go `EngineWriter`: a [`Writer`] as a Lightning engine writer.
pub struct EngineWriter {
    w: Writer,
}

impl EngineWriter {
    /// Go `NewEngineWriter`.
    #[must_use]
    pub fn new(w: Writer) -> Self {
        Self { w }
    }
}

impl tidb_lightning_backend::EngineWriter for EngineWriter {
    fn append_rows(
        &mut self,
        ctx: &Context,
        _: &[String],
        rows: tidb_lightning_backend::Rows,
    ) -> tidb_lightning_backend::Result<()> {
        for kv in rows.into_kv_pairs() {
            self.w
                .write_row(ctx, &kv.key, &kv.val)
                .map_err(tidb_lightning_backend::Error::from_cause)?;
        }
        Ok(())
    }

    fn is_synced(&self) -> bool {
        // Only used when saving checkpoints.
        true
    }

    fn close(
        &mut self,
        ctx: &Context,
    ) -> tidb_lightning_backend::Result<tidb_lightning_backend::ChunkFlushStatus> {
        self.w
            .close(ctx)
            .map_err(tidb_lightning_backend::Error::from_cause)?;
        // Go returns a nil status, which callers treat as nothing to wait for.
        Ok(tidb_lightning_backend::ChunkFlushStatus { flushed: true })
    }
}

/// Go master `pkg/ingestor/simplesst/writer_test.go`'s
/// `TestWriterKeyHints` (4239a9df1b5).
#[cfg(test)]
mod key_hint_tests {
    use std::sync::Mutex;

    use super::*;
    use crate::kv_reader::KVReader;

    const AUTO_RANDOM_PREFIX: &[u8] = b"x\x00\x00\x01t\x80\x00\x00\x00\x00\x00\x00\x17_r";

    fn auto_random_key(handle: u64) -> Vec<u8> {
        let mut key = AUTO_RANDOM_PREFIX.to_vec();
        key.extend_from_slice(&handle.to_be_bytes());
        key
    }

    fn new_writer(store: &Arc<dyn Storage>, on_dup: OnDuplicateKey, prefix: &str) -> (Writer, Arc<Mutex<Option<WriterSummary>>>) {
        let summary = Arc::new(Mutex::new(None));
        let sink = Arc::clone(&summary);
        let writer = WriterBuilder::new()
            .set_block_size(4096)
            .set_memory_size_limit(4096)
            .set_on_dup(on_dup)
            .set_on_close_func(Arc::new(move |s: &WriterSummary| *sink.lock().unwrap() = Some(s.clone())))
            .build(Arc::clone(store), prefix, "0");
        (writer, summary)
    }

    /// Go `readKVFile`.
    fn read_kv_file(store: &Arc<dyn Storage>, name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        let ctx = Context::background();
        let mut reader = KVReader::new(&ctx, name, store.as_ref(), 0, 1024).unwrap();
        let mut kvs = Vec::new();
        loop {
            match reader.next_kv() {
                Ok(kv) => kvs.push(kv),
                Err(Error::Eof) => break,
                Err(error) => panic!("{error}"),
            }
        }
        reader.close().unwrap();
        kvs
    }

    fn kv(key: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let mut value = b"v:".to_vec();
        value.extend_from_slice(key);
        (key.to_vec(), value)
    }

    fn mem_store() -> Arc<dyn Storage> {
        Arc::new(tidb_objstore::MemStorage::new())
    }

    #[test]
    fn test_writer_key_hints() {
        let ctx = Context::background();
        let cases: Vec<(&str, Vec<Vec<u8>>, &[u8], bool, OnDuplicateKey)> = vec![
            ("no-common-prefix", vec![b"z".to_vec(), b"a".to_vec(), b"m".to_vec()], b"", true, OnDuplicateKey::Error),
            ("empty-key", vec![b"b".to_vec(), b"".to_vec(), b"a".to_vec()], b"", false, OnDuplicateKey::Error),
            (
                "short-suffixes-trailing-zero",
                vec![
                    b"prefix/x\x00".to_vec(),
                    b"prefix/x".to_vec(),
                    b"prefix/x\x00\x00".to_vec(),
                    b"prefix/x\x00\x00\x00\x00\x00\x00\x00\x00a".to_vec(),
                ],
                b"prefix/x",
                false,
                // Zero-padded hints collide at 0, so Error checks they still
                // fall back to the full key comparison.
                OnDuplicateKey::Error,
            ),
            (
                "eight-byte-hint-collisions",
                vec![
                    b"prefix/01234567z".to_vec(),
                    b"prefix/01234567a".to_vec(),
                    b"prefix/z".to_vec(),
                    b"prefix/01234567m".to_vec(),
                ],
                b"prefix/",
                false,
                OnDuplicateKey::Error,
            ),
            (
                "auto-random-int-handles",
                vec![
                    auto_random_key(0x80400000_05f5e0e4),
                    auto_random_key(0xffc00000_00000001),
                    auto_random_key(0x80400000_00000002),
                    auto_random_key(0xc0000000_00000010),
                    auto_random_key(0x80400000_000001d5),
                ],
                AUTO_RANDOM_PREFIX,
                true,
                OnDuplicateKey::Error,
            ),
            (
                "exact-hint-duplicates-ignore",
                vec![
                    auto_random_key(0x80400000_00000003),
                    auto_random_key(0x80400000_00000001),
                    auto_random_key(0x80400000_00000001),
                    auto_random_key(0xffc00000_00000002),
                ],
                AUTO_RANDOM_PREFIX,
                true,
                OnDuplicateKey::Ignore,
            ),
        ];
        for (name, keys, prefix, exact_hints, on_dup) in cases {
            let store = mem_store();
            let (mut writer, summary) = new_writer(&store, on_dup, "/key-hints");
            let mut expected_size = 0u64;
            for _ in 0..2 {
                for key in &keys {
                    let mut value = b"value:".to_vec();
                    value.extend_from_slice(key);
                    writer.write_row(&ctx, key, &value).unwrap();
                    expected_size += (key.len() * 2 + b"value:".len()) as u64;
                }
                assert_eq!(writer.common_prefix, prefix, "{name}");
                assert_eq!(writer.update_key_hints(), exact_hints, "{name}");
                writer.flush_kvs(&ctx, false).unwrap();
                assert!(writer.common_prefix.is_empty(), "{name}");
            }
            writer.close(&ctx).unwrap();
            let summary = summary.lock().unwrap().clone().unwrap();
            assert_eq!(summary.total_size, expected_size, "{name}");
            assert_eq!(writer.written_bytes() as u64, expected_size, "{name}");
            let mut expected_keys = keys.clone();
            expected_keys.sort();
            for files in &summary.multiple_files_stats[0].filenames {
                let read: Vec<Vec<u8>> = read_kv_file(&store, &files[0]).into_iter().map(|(key, value)| {
                    assert_eq!(value, [b"value:".as_slice(), &key].concat(), "{name}");
                    key
                }).collect();
                assert_eq!(read, expected_keys, "{name}");
            }
        }

        // exact-hint-duplicate-modes
        let dup_keys = [
            auto_random_key(0xffc00000_00000009),
            auto_random_key(0x80400000_00000005),
            auto_random_key(0x80400000_00000005),
        ];
        let (mut err_writer, _) = new_writer(&mem_store(), OnDuplicateKey::Error, "/dup-err");
        for key in &dup_keys {
            err_writer.write_row(&ctx, key, b"v").unwrap();
        }
        let error = err_writer.flush_kvs(&ctx, false).expect_err("a duplicate");
        assert!(matches!(error, Error::FoundDuplicateKeys { .. }), "{error}");
        err_writer.kv_buffer.destroy();

        let (mut rm_writer, summary) = new_writer(&mem_store(), OnDuplicateKey::Remove, "/dup-rm");
        for key in &dup_keys {
            rm_writer.write_row(&ctx, key, b"v").unwrap();
        }
        rm_writer.close(&ctx).unwrap();
        let summary = summary.lock().unwrap().clone().unwrap();
        assert_eq!(summary.total_cnt, 1);
        assert_eq!(summary.min, dup_keys[0]);
        assert_eq!(summary.max, dup_keys[0]);

        let (mut rec_writer, summary) = new_writer(&mem_store(), OnDuplicateKey::Record, "/dup-rec");
        for key in dup_keys.iter().chain([auto_random_key(0x80400000_00000005)].iter()) {
            rec_writer.write_row(&ctx, key, b"v").unwrap();
        }
        rec_writer.close(&ctx).unwrap();
        let summary = summary.lock().unwrap().clone().unwrap();
        assert_eq!(summary.total_cnt, 3);
        assert_eq!(summary.conflict_info.count, 1);

        // fallback-hint-collision-duplicate-modes: keys share the eight-byte
        // hint "01234567" beyond "prefix/" while one is a true duplicate.
        let dup_keys: [&[u8]; 4] = [b"prefix/01234567z", b"prefix/01234567a", b"prefix/z", b"prefix/01234567a"];
        let (mut err_writer, _) = new_writer(&mem_store(), OnDuplicateKey::Error, "/fallback-dup-err");
        for key in dup_keys {
            err_writer.write_row(&ctx, key, &kv(key).1).unwrap();
        }
        assert!(!err_writer.update_key_hints());
        let error = err_writer.flush_kvs(&ctx, false).expect_err("a duplicate").to_string();
        assert!(error.contains(&hex::encode("prefix/01234567a")), "{error}");
        assert!(error.contains(&hex::encode("v:prefix/01234567a")), "{error}");
        assert!(matches!(err_writer.close(&ctx), Err(Error::FoundDuplicateKeys { .. })));

        let rm_store = mem_store();
        let (mut rm_writer, summary) = new_writer(&rm_store, OnDuplicateKey::Remove, "/fallback-dup-rm");
        for key in dup_keys {
            rm_writer.write_row(&ctx, key, &kv(key).1).unwrap();
        }
        assert!(!rm_writer.update_key_hints());
        rm_writer.close(&ctx).unwrap();
        let summary = summary.lock().unwrap().clone().unwrap();
        assert_eq!(summary.total_cnt, 2);
        assert_eq!(
            read_kv_file(&rm_store, &summary.multiple_files_stats[0].filenames[0][0]),
            vec![kv(b"prefix/01234567z"), kv(b"prefix/z")]
        );

        let rec_store = mem_store();
        let (mut rec_writer, summary) = new_writer(&rec_store, OnDuplicateKey::Record, "/fallback-dup-rec");
        for key in dup_keys.iter().chain([b"prefix/01234567a".as_slice()].iter()) {
            rec_writer.write_row(&ctx, key, &kv(key).1).unwrap();
        }
        assert!(!rec_writer.update_key_hints());
        rec_writer.close(&ctx).unwrap();
        let summary = summary.lock().unwrap().clone().unwrap();
        assert_eq!(summary.total_cnt, 4);
        assert_eq!(summary.conflict_info.count, 1);
        assert_eq!(summary.conflict_info.files.len(), 1);
        assert_eq!(
            read_kv_file(&rec_store, &summary.multiple_files_stats[0].filenames[0][0]),
            vec![kv(b"prefix/01234567a"), kv(b"prefix/01234567a"), kv(b"prefix/01234567z"), kv(b"prefix/z")]
        );
        assert_eq!(read_kv_file(&rec_store, &summary.conflict_info.files[0]), vec![kv(b"prefix/01234567a")]);

        let ign_store = mem_store();
        let (mut ign_writer, summary) = new_writer(&ign_store, OnDuplicateKey::Ignore, "/fallback-dup-ign");
        for key in dup_keys {
            ign_writer.write_row(&ctx, key, &kv(key).1).unwrap();
        }
        assert!(!ign_writer.update_key_hints());
        ign_writer.close(&ctx).unwrap();
        let summary = summary.lock().unwrap().clone().unwrap();
        assert_eq!(summary.total_cnt, 4);
        assert_eq!(
            read_kv_file(&ign_store, &summary.multiple_files_stats[0].filenames[0][0]),
            vec![kv(b"prefix/01234567a"), kv(b"prefix/01234567a"), kv(b"prefix/01234567z"), kv(b"prefix/z")]
        );
    }
}
