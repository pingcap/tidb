// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/engine.go` and `localWriter` in `local.go`.
//! Immutable sorted runs use the existing native external sorter. A flush
//! freezes a generation; imports remove only that generation's files, leaving
//! rows arriving during import for the next flush.
pub use crate::iterator::EngineIterator;
use crate::{Error, Result};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tidb_extsort::{DiskSorter, DiskSorterOptions, ExternalSorter, open_disk_sorter};

/// Go `common.DupDetectKeyAdapter.Encode`.
pub fn encode_duplicate_key(key: &[u8], row_id: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    tidb_codec::encode_bytes(&mut encoded, key);
    encoded.extend_from_slice(row_id);
    encoded.extend_from_slice(&(row_id.len() as u16).to_be_bytes());
    encoded
}
/// Go `common.DupDetectKeyAdapter.Decode`.
pub fn decode_duplicate_key(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < 2 {
        return Err(Error::Other("insufficient bytes to decode value".into()));
    }
    let tail =
        u16::from_be_bytes(data[data.len() - 2..].try_into().unwrap()).wrapping_add(2) as usize;
    if data.len() < tail {
        return Err(Error::Other("insufficient bytes to decode value".into()));
    }
    tidb_codec::decode_bytes(&data[..data.len() - tail])
        .map(|(_, key)| key)
        .map_err(|e| Error::Other(e.to_string()))
}
struct State {
    runs: BTreeMap<u64, DiskSorter>,
    active: Option<(u64, DiskSorter)>,
    writers: HashMap<usize, Box<dyn tidb_extsort::Writer>>,
    next_run: u64,
    closed: bool,
    buffered_bytes: u64,
    ingest_error: Option<String>,
    region_split_keys: Option<Vec<Vec<u8>>>,
    region_split_size: u64,
}
/// Go `Engine`: sorted index data on local disk, with a stable import TS.
/// Storage differs from Go: `tidb-extsort` disk runs replace Pebble.
pub struct Engine {
    path: PathBuf,
    duplicate_detection: bool,
    writer_cache_size: usize,
    state: Mutex<State>,
    ts: AtomicU64,
    importing: AtomicBool,
    imported_size: AtomicU64,
    imported_count: AtomicU64,
    /// Go `Writer.tikvCodec`: every key `AppendRows` receives is encoded
    /// with it before the engine stores it; `None` stores keys as given.
    tikv_codec: Option<tidb_ingestor_simplesst::KeyCodec>,
}
impl Engine {
    /// Go `engineManager.OpenEngine`: reopen durable runs and retain their TS.
    pub fn open(
        path: PathBuf,
        ts: u64,
        duplicate_detection: bool,
        writer_cache_size: usize,
    ) -> Result<Self> {
        std::fs::create_dir_all(&path)?;
        let mut runs = BTreeMap::new();
        let mut next_run = 0;
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(id) = name
                .strip_prefix("run-")
                .and_then(|id| id.parse::<u64>().ok())
            else {
                continue;
            };
            let sorter = open_disk_sorter(entry.path(), Some(options(writer_cache_size)))?;
            sorter.sort(&AtomicBool::new(false))?;
            runs.insert(id, sorter);
            next_run = next_run.max(id + 1);
        }
        let meta = path.join("import-ts");
        let ts = match std::fs::read_to_string(&meta) {
            Ok(value) => value
                .parse()
                .map_err(|_| Error::Other("invalid engine import timestamp".into()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::fs::write(&meta, ts.to_string())?;
                ts
            }
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            path,
            duplicate_detection,
            writer_cache_size,
            state: Mutex::new(State {
                runs,
                active: None,
                writers: HashMap::new(),
                next_run,
                closed: false,
                buffered_bytes: 0,
                ingest_error: None,
                region_split_keys: None,
                region_split_size: 0,
            }),
            ts: AtomicU64::new(ts),
            importing: AtomicBool::new(false),
            imported_size: AtomicU64::new(0),
            imported_count: AtomicU64::new(0),
            tikv_codec: None,
        })
    }

    /// Go `openLocalWriter`'s `tikvCodec`: the codec the engine's writers
    /// encode each key with.
    #[must_use]
    pub fn with_tikv_codec(mut self, codec: Option<tidb_ingestor_simplesst::KeyCodec>) -> Self {
        self.tikv_codec = codec;
        self
    }
    /// Go `localWriter.AppendRows`, retaining RowID for duplicate detection.
    pub fn append_rows(
        &self,
        writer_id: usize,
        rows: &[(Vec<u8>, Vec<u8>, Vec<u8>)],
    ) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        if s.closed {
            return Err(Error::Other("engine is closed".into()));
        }
        if s.active.is_none() {
            let id = s.next_run;
            s.next_run += 1;
            s.active = Some((
                id,
                open_disk_sorter(
                    self.path.join(format!("run-{id}")),
                    Some(options(self.writer_cache_size)),
                )?,
            ));
        }
        if !s.writers.contains_key(&writer_id) {
            let writer = s
                .active
                .as_ref()
                .unwrap()
                .1
                .new_writer(&AtomicBool::new(false))?;
            s.writers.insert(writer_id, writer);
        }
        for (key, value, row_id) in rows {
            // Go `Writer.AppendRows`: `tikvCodec.EncodeKey` first, then the
            // engine's key adapter.
            let key = match &self.tikv_codec {
                Some(codec) => std::borrow::Cow::Owned(codec(key)),
                None => std::borrow::Cow::Borrowed(key.as_slice()),
            };
            let encoded;
            let stored = if self.duplicate_detection {
                encoded = encode_duplicate_key(&key, row_id);
                encoded.as_slice()
            } else {
                &key[..]
            };
            s.writers.get_mut(&writer_id).unwrap().put(stored, value)?;
            s.buffered_bytes += (stored.len() + value.len()) as u64;
        }
        Ok(())
    }
    /// Go `localWriter.Close`: the writer's buffered rows reach the engine's
    /// current run; [`Self::flush`] seals the run.
    pub fn close_writer(&self, writer_id: usize) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        if let Some(mut writer) = s.writers.remove(&writer_id) {
            writer.close()?;
        }
        Ok(())
    }

    /// Whether the engine holds no flushed or buffered rows (Go
    /// `KVStatistics` reporting zero size).
    pub fn is_empty(&self) -> Result<bool> {
        if self.state.lock().unwrap().buffered_bytes > 0 {
            return Ok(false);
        }
        Ok(self.new_iter(&[], &[])?.next_pair()?.is_none())
    }

    /// Go `Engine.Flush`: all acknowledged rows become durable sorted data.
    pub fn flush(&self) -> Result<()> {
        let mut s = self.state.lock().unwrap();
        if s.closed {
            return Ok(());
        }
        for writer in s.writers.values_mut() {
            writer.close()?;
        }
        s.writers.clear();
        if let Some((id, sorter)) = s.active.clone() {
            sorter.sort(&AtomicBool::new(false))?;
            s.runs.insert(id, sorter);
        }
        s.active = None;
        s.region_split_keys = None;
        s.buffered_bytes = 0;
        Ok(())
    }
    /// Go `Engine.NewIter`: snapshot flushed generations, merged by encoded key.
    pub fn new_iter(&self, start: &[u8], end: &[u8]) -> Result<EngineIterator> {
        let s = self.state.lock().unwrap();
        let mut cursors = Vec::new();
        let seek = if self.duplicate_detection && !start.is_empty() {
            encode_duplicate_key(start, &[])
        } else {
            start.to_vec()
        };
        for sorter in s.runs.values() {
            let mut it = sorter.new_iterator(&AtomicBool::new(false))?;
            if seek.is_empty() {
                it.first();
            } else {
                it.seek(&seek);
            }
            cursors.push(it);
        }
        Ok(EngineIterator::new(
            cursors,
            self.duplicate_detection,
            end.to_vec(),
        ))
    }

    /// Go `Engine.GetFirstAndLastKey` for the half-open range.
    pub fn get_first_and_last_key(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut it = self.new_iter(start, end)?;
        it.ignore_duplicates();
        let Some((first, _)) = it.next_pair()? else {
            return Ok(None);
        };
        let mut last = first.clone();
        while let Some((key, _)) = it.next_pair()? {
            last = key;
        }
        Ok(Some((first, last)))
    }
    /// The engine's directory, named by its UUID.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Go `Engine.GetTS`.
    pub fn get_ts(&self) -> u64 {
        self.ts.load(Ordering::SeqCst)
    }
    /// Go `Backend.SetTSBeforeImportEngine`.
    pub fn set_ts(&self, ts: u64) -> Result<()> {
        let pending = self.path.join("import-ts.tmp");
        let mut file = std::fs::File::create(&pending)?;
        use std::io::Write;
        write!(file, "{ts}")?;
        file.sync_all()?;
        std::fs::rename(pending, self.path.join("import-ts"))?;
        self.ts.store(ts, Ordering::SeqCst);
        Ok(())
    }
    /// Go `Engine`'s imported generation boundary.
    pub fn flushed_generation(&self) -> Option<u64> {
        self.state
            .lock()
            .unwrap()
            .runs
            .last_key_value()
            .map(|(id, _)| *id)
    }
    /// Go `ResetEngineSkipAllocTS`: remove imported runs, keep unflushed rows.
    pub fn reset_imported(&self, generation: Option<u64>) -> Result<()> {
        let Some(generation) = generation else {
            return Ok(());
        };
        let mut s = self.state.lock().unwrap();
        s.region_split_keys = None;
        s.closed = false;
        let ids = s
            .runs
            .range(..=generation)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in ids {
            s.runs[&id].close_and_cleanup()?;
            s.runs.remove(&id);
        }
        Ok(())
    }
    /// Go `Engine.finishWrite`: stop writes but retain readable sorted data.
    pub fn finish_write(&self) -> Result<()> {
        self.flush()?;
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        match &state.ingest_error {
            Some(error) => Err(Error::Other(error.clone())),
            None => Ok(()),
        }
    }
    /// Go `Engine.setError`: retain the first ingest failure.
    pub fn set_error(&self, error: String) {
        self.state.lock().unwrap().ingest_error.get_or_insert(error);
    }
    /// Go `Engine.TotalMemorySize`; extsort owns writer buffers instead of Pebble memtables.
    pub fn total_memory_size(&self) -> u64 {
        self.state
            .lock()
            .unwrap()
            .writers
            .values()
            .map(|writer| writer.memory_size() as u64)
            .sum()
    }
    /// Go `Engine.ImportedStatistics`.
    pub fn imported_statistics(&self) -> (u64, u64) {
        (
            self.imported_size.load(Ordering::SeqCst),
            self.imported_count.load(Ordering::SeqCst),
        )
    }
    /// Go `Engine.Finish`.
    pub fn finish(&self, bytes: u64, count: u64) {
        self.imported_size.fetch_add(bytes, Ordering::SeqCst);
        self.imported_count.fetch_add(count, Ordering::SeqCst);
    }
    /// Go `Engine.KVStatistics` over the durable sorted runs.
    pub fn kv_statistics(&self) -> Result<(u64, u64)> {
        let properties = self.get_size_properties()?;
        Ok((
            properties.total_size,
            properties.index_handles.values().map(|p| p.keys).sum(),
        ))
    }
    /// Go `engineManager.resetEngine`; extsort generations are removed and
    /// recreated instead of deleting and reopening a Pebble DB.
    pub fn reset(&self, ts: u64) -> Result<()> {
        self.flush()?;
        let mut state = self.state.lock().unwrap();
        for sorter in state.runs.values() {
            sorter.close_and_cleanup()?;
        }
        state.runs.clear();
        state.active = None;
        state.writers.clear();
        state.closed = false;
        state.ingest_error = None;
        state.buffered_bytes = 0;
        self.imported_size.store(0, Ordering::SeqCst);
        self.imported_count.store(0, Ordering::SeqCst);
        self.set_ts(ts)
    }
    /// Go `Engine.Close`; cleanup is owned by the backend.
    pub fn close(&self) -> Result<()> {
        self.flush()?;
        let mut s = self.state.lock().unwrap();
        s.closed = true;
        for sorter in s.runs.values() {
            sorter.close()?;
        }
        Ok(())
    }
    /// Go `Backend.CleanupEngine`.
    pub fn cleanup(&self) -> Result<()> {
        self.close()?;
        match std::fs::remove_dir_all(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
    /// Go `EngineFileSize`.
    pub fn file_size(&self, uuid: [u8; 16]) -> crate::disk_quota::EngineFileSize {
        crate::disk_quota::EngineFileSize {
            uuid,
            disk_size: directory_size(&self.path) as i64,
            mem_size: self.total_memory_size() as i64,
            is_importing: self.importing.load(Ordering::SeqCst),
        }
    }
    /// Go `Engine.isImporting`.
    pub fn set_importing(&self, value: bool) {
        self.importing.store(value, Ordering::SeqCst);
    }
}
/// Go `engineManager.openEngine (extsort options replace Pebble options)`.
fn options(writer_buffer_size: usize) -> DiskSorterOptions {
    DiskSorterOptions {
        writer_buffer_size,
        concurrency: 4,
        ..Default::default()
    }
}
/// Go `Engine.getEngineFileSize`.
fn directory_size(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .map(|entries| {
            entries
                .filter_map(std::result::Result::ok)
                .map(|e| match e.file_type() {
                    Ok(t) if t.is_dir() => directory_size(&e.path()),
                    _ => e.metadata().map(|m| m.len()).unwrap_or(0),
                })
                .sum()
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `Writer.AppendRows`: each key is encoded with the writer's
    /// `tikvCodec` (the keyspace prefix) before the engine stores it, so the
    /// engine's range, split keys and region jobs are in the store's encoded
    /// key space; under duplicate detection the row id goes after the
    /// encoded key.
    #[test]
    fn append_rows_stores_codec_encoded_keys() {
        let codec: tidb_ingestor_simplesst::KeyCodec = std::sync::Arc::new(|key| [b"x\0\0\x01".as_slice(), key].concat());
        for duplicate_detection in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let engine = Engine::open(dir.path().join("engine"), 1, duplicate_detection, 1024)
                .unwrap()
                .with_tikv_codec(Some(std::sync::Arc::clone(&codec)));
            engine
                .append_rows(0, &[b"a", b"c"].map(|k| (k.to_vec(), k.to_vec(), b"r".to_vec())))
                .unwrap();
            engine.flush().unwrap();
            assert_eq!(
                engine.get_key_range().unwrap(),
                Some((b"x\0\0\x01a".to_vec(), b"x\0\0\x01c\0".to_vec()))
            );
        }
    }

    /// Go `engine_test.go:TestGetFirstAndLastKey`.
    #[test]
    fn test_get_first_and_last_key() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(dir.path().join("engine"), 1, false, 1024).unwrap();
        engine
            .append_rows(
                0,
                &[b"a", b"c", b"e"].map(|k| (k.to_vec(), k.to_vec(), vec![])),
            )
            .unwrap();
        engine.flush().unwrap();
        for (start, end, expected) in [
            ("", "", Some(("a", "e"))),
            ("b", "d", Some(("c", "c"))),
            ("b", "f", Some(("c", "e"))),
            ("y", "z", None),
            ("e", "", Some(("e", "e"))),
        ] {
            assert_eq!(
                engine
                    .get_first_and_last_key(start.as_bytes(), end.as_bytes())
                    .unwrap(),
                expected.map(|(a, b)| (a.as_bytes().to_vec(), b.as_bytes().to_vec()))
            );
        }
    }
    /// Go `testMergeSSTs`.
    fn merge_ssts(duplicate: bool) {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(dir.path().join("engine"), 1, false, 128).unwrap();
        for i in 0..5 {
            let i = if duplicate && i == 4 { 0 } else { i };
            let rows = (0..10)
                .map(|j| {
                    let mut key = vec![0; 16];
                    key[..8].copy_from_slice(&((i * 100 + j) as u64).to_be_bytes());
                    (key, vec![], vec![])
                })
                .collect::<Vec<_>>();
            engine.append_rows(0, &rows).unwrap();
            engine.flush().unwrap();
        }
        let mut iter = engine.new_iter(&[], &[]).unwrap();
        let (mut count, mut bytes) = (0, 0);
        while let Some((k, v)) = iter.next_pair().unwrap() {
            count += 1;
            bytes += k.len() + v.len();
        }
        assert_eq!(
            (count, bytes),
            if duplicate { (40, 640) } else { (50, 800) }
        );
    }
    /// Go `local_test.go:TestMergeSSTs`.
    #[test]
    fn test_merge_ssts() {
        merge_ssts(false);
    }
    /// Go `local_test.go:TestMergeSSTsDuplicated`.
    #[test]
    fn test_merge_ssts_duplicated() {
        merge_ssts(true);
    }
}

#[cfg(test)]
mod writer_tests {
    use super::*;
    /// Go `local_test.go:TestEngineLocalWriter`, its three ordering cases and
    /// 20,000 distinct 16-byte keys with 128-byte values.
    #[test]
    fn test_engine_local_writer() {
        for ordering in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let engine = Engine::open(dir.path().join("engine"), 1, false, 1024).unwrap();
            let value = (0u64..16).flat_map(u64::to_be_bytes).collect::<Vec<_>>();
            let mut rows = (1u64..=20000)
                .map(|i| {
                    let mut key = Vec::new();
                    key.extend_from_slice(&((i * 7919) % 1000).to_be_bytes());
                    key.extend_from_slice(&i.to_be_bytes());
                    (key, value.clone(), vec![])
                })
                .collect::<Vec<_>>();
            if ordering == 1 {
                rows.sort_by(|a, b| a.0.cmp(&b.0));
            }
            if ordering == 2 {
                rows[..12000].sort_by(|a, b| a.0.cmp(&b.0));
            }
            let slices = if ordering == 2 {
                vec![&rows[..6000], &rows[12000..], &rows[6000..12000]]
            } else {
                vec![&rows[..6000], &rows[6000..12000], &rows[12000..]]
            };
            for rows in slices {
                engine.append_rows(0, rows).unwrap();
            }
            engine.flush().unwrap();
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            let mut it = engine.new_iter(&[], &[]).unwrap();
            let mut size = 0;
            for (key, value, _) in &rows {
                let pair = it.next_pair().unwrap().unwrap();
                assert_eq!(&pair.0, key);
                assert_eq!(&pair.1, value);
                size += pair.0.len() + pair.1.len();
            }
            assert!(it.next_pair().unwrap().is_none());
            assert_eq!(size, 144 * 20000);
        }
    }
}

/// Go `defaultPropSizeIndexDistance`.
pub const DEFAULT_PROP_SIZE_INDEX_DISTANCE: u64 = 4 << 20;
/// Go `defaultPropKeysIndexDistance`.
pub const DEFAULT_PROP_KEYS_INDEX_DISTANCE: u64 = 40 * 1024;
/// Go `engineMetaKey`.
pub const ENGINE_META_KEY: &[u8] = b"\0meta";
/// Go `rangeProperty`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RangeProperty {
    /// Go `Key`.
    pub key: Vec<u8>,
    /// Go `rangeOffsets.Size`.
    pub size: u64,
    /// Go `rangeOffsets.Keys`.
    pub keys: u64,
}
/// Go `rangeProperties.Encode`.
pub fn encode_range_properties(properties: &[RangeProperty]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for p in properties {
        bytes.extend_from_slice(&(p.key.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&p.key);
        bytes.extend_from_slice(&p.size.to_be_bytes());
        bytes.extend_from_slice(&p.keys.to_be_bytes());
    }
    bytes
}
/// Go `decodeRangeProperties`, with the engine's key adapter.
pub fn decode_range_properties(
    mut data: &[u8],
    duplicate_detection: bool,
) -> Result<Vec<RangeProperty>> {
    let mut properties = Vec::new();
    while !data.is_empty() {
        if data.len() < 4 {
            return Err(Error::Other("unexpected EOF".into()));
        }
        let len = u32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
        data = &data[4..];
        if data.len() < len + 16 {
            return Err(Error::Other("unexpected EOF".into()));
        }
        let key = &data[..len];
        let size = u64::from_be_bytes(data[len..len + 8].try_into().unwrap());
        let keys = u64::from_be_bytes(data[len + 8..len + 16].try_into().unwrap());
        data = &data[len + 16..];
        if key != ENGINE_META_KEY {
            let key = if duplicate_detection {
                decode_duplicate_key(key)?
            } else {
                key.to_vec()
            };
            properties.push(RangeProperty { key, size, keys });
        }
    }
    Ok(properties)
}
/// Go `RangePropertiesCollector`. extsort computes these from sorted runs,
/// replacing Pebble's table-property callback without changing the encoding.
pub struct RangePropertiesCollector {
    props: Vec<RangeProperty>,
    last_offsets: (u64, u64),
    current_offsets: (u64, u64),
    last_key: Vec<u8>,
    prop_size_idx_distance: u64,
    prop_keys_idx_distance: u64,
}
impl Default for RangePropertiesCollector {
    /// Go `newRangePropertiesCollector`.
    fn default() -> Self {
        Self::new(
            DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            DEFAULT_PROP_KEYS_INDEX_DISTANCE,
        )
    }
}
impl RangePropertiesCollector {
    /// Go `newRangePropertiesCollector`, with injectable test distances.
    pub fn new(size_distance: u64, keys_distance: u64) -> Self {
        Self {
            props: Vec::new(),
            last_offsets: (0, 0),
            current_offsets: (0, 0),
            last_key: Vec::new(),
            prop_size_idx_distance: size_distance,
            prop_keys_idx_distance: keys_distance,
        }
    }
    /// Go `RangePropertiesCollector.insertNewPoint`.
    fn insert_new_point(&mut self, key: &[u8]) {
        self.last_offsets = self.current_offsets;
        self.props.push(RangeProperty {
            key: key.to_vec(),
            size: self.current_offsets.0,
            keys: self.current_offsets.1,
        });
    }
    /// Go `RangePropertiesCollector.Add`, for a Pebble Set / extsort KV.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if key == ENGINE_META_KEY {
            return;
        }
        self.current_offsets.0 += (key.len() + value.len()) as u64;
        self.current_offsets.1 += 1;
        if self.last_key.is_empty()
            || self.current_offsets.0 - self.last_offsets.0 >= self.prop_size_idx_distance
            || self.current_offsets.1 - self.last_offsets.1 >= self.prop_keys_idx_distance
        {
            self.insert_new_point(key);
        }
        self.last_key = key.to_vec();
    }
    /// Go `RangePropertiesCollector.Finish`.
    pub fn finish(&mut self) -> Vec<RangeProperty> {
        if self.current_offsets != self.last_offsets {
            self.insert_new_point(&self.last_key.clone());
        }
        self.props.clone()
    }
    /// Go `RangePropertiesCollector.Name`.
    pub fn name(&self) -> &'static str {
        "tikv.range_index"
    }
}
/// Go `sizeProperties`.
#[derive(Default)]
pub struct SizeProperties {
    /// Go `totalSize`.
    pub total_size: u64,
    /// Go `indexHandles`, ordered by key.
    pub index_handles: BTreeMap<Vec<u8>, RangeProperty>,
}
impl SizeProperties {
    /// Go `sizeProperties.add`: combine offsets at the same split key.
    pub fn add(&mut self, item: RangeProperty) {
        let old = self
            .index_handles
            .entry(item.key.clone())
            .or_insert_with(|| RangeProperty {
                key: item.key.clone(),
                ..Default::default()
            });
        old.size += item.size;
        old.keys += item.keys;
    }
    /// Go `sizeProperties.addAll`: cumulative file offsets become additive deltas.
    pub fn add_all(&mut self, properties: &[RangeProperty]) {
        let mut previous = (0, 0);
        for p in properties {
            self.add(RangeProperty {
                key: p.key.clone(),
                size: p.size - previous.0,
                keys: p.keys - previous.1,
            });
            previous = (p.size, p.keys);
        }
        self.total_size += previous.0;
    }
}
impl Engine {
    /// Go `getSizeProperties`; the storage substitution reads extsort runs
    /// instead of Pebble SST table properties.
    pub fn get_size_properties(&self) -> Result<SizeProperties> {
        let state = self.state.lock().unwrap();
        let mut result = SizeProperties::default();
        for sorter in state.runs.values() {
            let mut iter = sorter.new_iterator(&AtomicBool::new(false))?;
            let mut collector = RangePropertiesCollector::default();
            iter.first();
            while iter.valid() {
                collector.add(iter.unsafe_key(), iter.unsafe_value());
                iter.next();
            }
            if let Some(error) = iter.error() {
                return Err(Error::Other(error.to_string()));
            }
            iter.close()?;
            let properties = decode_range_properties(
                &encode_range_properties(&collector.finish()),
                self.duplicate_detection,
            )?;
            result.add_all(&properties);
        }
        Ok(result)
    }
    /// Go `Engine.GetKeyRange`.
    pub fn get_key_range(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .get_first_and_last_key(&[], &[])?
            .map(|(first, last)| (first, crate::local::next_key(&last))))
    }
    /// Go `Engine.getRegionSplitKeys`.
    pub fn get_region_split_keys(&self, size_limit: u64, keys_limit: u64) -> Result<Vec<Vec<u8>>> {
        let Some((first, end)) = self.get_key_range()? else {
            return Ok(Vec::new());
        };
        let properties = self.get_size_properties()?;
        let ranges = crate::local::split_range_by_size_props(
            (first, end),
            &properties,
            size_limit,
            keys_limit,
        );
        let mut keys: Vec<_> = ranges.iter().map(|range| range.0.clone()).collect();
        if let Some(range) = ranges.last() {
            keys.push(range.1.clone());
        }
        let mut state = self.state.lock().unwrap();
        state.region_split_keys = Some(keys.clone());
        state.region_split_size = size_limit;
        Ok(keys)
    }
    /// Go `Engine.LoadIngestData`, delivering bounded local ranges. Rust borrows
    /// this engine for the scoped pipeline instead of an engineapi interface.
    pub fn load_ingest_data(
        &self,
        ctx: &tidb_gocontext::Context,
        mut send: impl FnMut((Vec<u8>, Vec<u8>)) -> Result<()>,
    ) -> Result<()> {
        let (cached, size) = {
            let state = self.state.lock().unwrap();
            (state.region_split_keys.clone(), state.region_split_size)
        };
        let keys = load_ingest_data_keys(cached, size, |size, keys| {
            self.get_region_split_keys(size, keys)
        })?;
        for range in keys.windows(2) {
            if let Some(error) = ctx.err() {
                return Err(Error::Source(std::sync::Arc::new(error)));
            }
            send((range[0].clone(), range[1].clone()))?;
        }
        Ok(())
    }
}

/// Go `Engine` as `common.IngestData`: region jobs read the engine itself.
impl tidb_ingestor_engineapi::IngestData for Engine {
    fn get_first_and_last_key(
        &self,
        lower: &[u8],
        upper: &[u8],
    ) -> std::result::Result<Option<tidb_ingestor_engineapi::Pair>, tidb_ingestor_engineapi::Error> {
        Engine::get_first_and_last_key(self, lower, upper).map_err(|error| Box::new(error) as _)
    }

    fn new_iter<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> std::result::Result<Box<dyn tidb_ingestor_engineapi::ForwardIter + 'a>, tidb_ingestor_engineapi::Error> {
        Ok(Box::new(Engine::new_iter(self, lower, upper).map_err(|error| Box::new(error) as tidb_ingestor_engineapi::Error)?))
    }

    fn get_ts(&self) -> u64 {
        Engine::get_ts(self)
    }

    fn finish(&self, total_bytes: i64, total_count: i64) {
        Engine::finish(self, total_bytes as u64, total_count as u64);
    }
}

impl tidb_ingestor_engineapi::ForwardIter for EngineIterator {
    fn next_pair(&mut self) -> std::result::Result<Option<tidb_ingestor_engineapi::Pair>, tidb_ingestor_engineapi::Error> {
        EngineIterator::next_pair(self).map_err(|error| Box::new(error) as _)
    }
}

/// Go `Engine.LoadIngestData`'s cached-range decision and default job-size limit.
/// `get_keys` is Go's `getRegionSplitKeys` / `getSizePropertiesFn` boundary.
fn load_ingest_data_keys(
    cached: Option<Vec<Vec<u8>>>,
    region_split_size: u64,
    get_keys: impl FnOnce(u64, u64) -> Result<Vec<Vec<u8>>>,
) -> Result<Vec<Vec<u8>>> {
    if region_split_size <= 2 * 96 * 1024 * 1024 {
        if let Some(keys) = cached {
            return Ok(keys);
        }
    }
    get_keys(96 * 1024 * 1024, 960_000)
}

#[cfg(test)]
mod property_tests {
    use super::*;
    /// Go `TestRangeProperties`, including merged files and both key-count limits.
    #[test]
    fn test_range_properties() {
        let d = DEFAULT_PROP_SIZE_INDEX_DISTANCE;
        let k = DEFAULT_PROP_KEYS_INDEX_DISTANCE;
        let cases = [
            ("a", 0, 1),
            ("b", d / 8, 1),
            ("c", d / 4, 1),
            ("d", d / 2, 1),
            ("e", d / 8, 1),
            ("f", d / 4, 1),
            ("g", d / 2, 1),
            ("h", d / 8, 1),
            ("i", d / 4, 1),
            ("j", d / 2, 1),
            ("k", d / 2, 1),
            ("l", 0, k / 2),
            ("m", 0, k / 2),
            ("n", 1, k),
            ("o", 1, 1),
        ];
        let mut collector = RangePropertiesCollector::default();
        for (key, size, count) in cases {
            let value = vec![0; size as usize];
            for _ in 0..count {
                collector.add(key.as_bytes(), &value);
            }
        }
        let encoded = encode_range_properties(&collector.finish());
        let props = decode_range_properties(&encoded, false).unwrap();
        assert_eq!(props.len(), 7);
        assert_eq!(props[0].key, b"a");
        assert_eq!(props[6].key, b"o");
        for n in 1..encoded.len() {
            if !props
                .iter()
                .scan(0, |offset, p| {
                    *offset += 20 + p.key.len();
                    Some(*offset)
                })
                .any(|offset| offset == n)
            {
                assert!(decode_range_properties(&encoded[..n], false).is_err());
            }
        }
        let props2: Vec<_> = [
            ("b", d + 10, k / 2),
            ("h", d * 3 / 2, k * 3 / 2),
            ("k", d * 3, k * 7 / 4),
            ("mm", d * 5, k * 2),
            ("q", d * 7, k * 9 / 4 + 10),
            ("y", d * 7 + 100, k * 9 / 4 + 1010),
        ]
        .into_iter()
        .map(|(key, size, keys)| RangeProperty {
            key: key.as_bytes().to_vec(),
            size,
            keys,
        })
        .collect();
        let mut sizes = SizeProperties::default();
        sizes.add_all(&props);
        sizes.add_all(&props2);
        let expected = [
            ("a", 1, 1),
            ("b", d + 10, k / 2),
            ("e", d + 4, 4),
            ("h", d / 2 - 10, k),
            ("i", d * 9 / 8 + 4, 4),
            ("k", d * 5 / 2 + 2, k / 4 + 2),
            ("m", k, k),
            ("mm", d * 2, k / 4),
            ("n", k * 2, k),
            ("o", 2, 1),
            ("q", d * 2, k / 4 + 10),
            ("y", 100, 1000),
        ];
        assert_eq!(sizes.index_handles.len(), expected.len());
        for (got, (key, size, keys)) in sizes.index_handles.values().zip(expected) {
            assert_eq!(
                got,
                &RangeProperty {
                    key: key.as_bytes().to_vec(),
                    size,
                    keys
                }
            );
        }
        for (limit, expected) in [
            (k * 5 / 2, vec!["a", "e", "k", "mm", "q", "z"]),
            (k, vec!["a", "e", "h", "k", "m", "mm", "n", "q", "z"]),
        ] {
            let ranges = crate::local::split_range_by_size_props(
                (b"a".to_vec(), b"z".to_vec()),
                &sizes,
                d * 2,
                limit,
            );
            assert_eq!(
                ranges,
                expected
                    .windows(2)
                    .map(|pair| (pair[0].as_bytes().to_vec(), pair[1].as_bytes().to_vec()))
                    .collect::<Vec<_>>()
            );
        }
    }
    /// Go `TestRangePropertiesWithPebble`; extsort replaces Pebble storage.
    #[test]
    fn test_range_properties_with_pebble() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(dir.path().join("e"), 1, false, 1024).unwrap();
        let mut collector = RangePropertiesCollector::default();
        for i in 0u64..1000 {
            let key = i.to_be_bytes().to_vec();
            let value = vec![0; (i % 50) as usize];
            collector.add(&key, &value);
            engine.append_rows(0, &[(key, value, vec![])]).unwrap();
        }
        engine.flush().unwrap();
        let mut expected = SizeProperties::default();
        expected.add_all(&collector.finish());
        let got = engine.get_size_properties().unwrap();
        assert_eq!(got.total_size, expected.total_size);
        assert_eq!(got.index_handles, expected.index_handles);
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    /// Go `TestGetEngineSizeWhenImport`; querying size must not block on the
    /// import state and must report that state.
    #[test]
    fn test_get_engine_size_when_import() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(dir.path().join("engine"), 1, false, 1024).unwrap();
        engine.set_importing(true);
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let size = engine.file_size([1; 16]);
                    assert_eq!(size.uuid, [1; 16]);
                    assert!(size.is_importing);
                })
                .join()
                .unwrap();
        });
        engine.set_importing(false);
        engine.close().unwrap();
    }
    /// Go `TestIngestSSTWithClosedEngine`; extsort run ingestion goes through
    /// AppendRows/Flush instead of Pebble's ingestion of SST file paths.
    #[test]
    fn test_ingest_sst_with_closed_engine() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(dir.path().join("engine"), 1, false, 1024).unwrap();
        let rows: Vec<_> = (0..10)
            .map(|i| (format!("key{i}").into_bytes(), vec![], vec![]))
            .collect();
        engine.append_rows(0, &rows).unwrap();
        engine.flush().unwrap();
        engine.close().unwrap();
        assert_eq!(
            engine.append_rows(0, &rows).unwrap_err().to_string(),
            "engine is closed"
        );
    }
    /// Go `TestIterOutputHasUniqueMemorySpace`; Rust owns every returned pair,
    /// so releasing/closing the iterator cannot invalidate earlier outputs.
    #[test]
    fn test_iter_output_has_unique_memory_space() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(dir.path().join("engine"), 1, false, 1024).unwrap();
        let rows: Vec<_> = [b'a', b'c', b'e', b'g']
            .map(|k| (vec![k], vec![k], vec![]))
            .into();
        engine.append_rows(0, &rows).unwrap();
        engine.flush().unwrap();
        let mut iter = engine.new_iter(&[], &[]).unwrap();
        let first = [
            iter.next_pair().unwrap().unwrap(),
            iter.next_pair().unwrap().unwrap(),
        ];
        let next = [
            iter.next_pair().unwrap().unwrap(),
            iter.next_pair().unwrap().unwrap(),
        ];
        assert!(iter.next_pair().unwrap().is_none());
        iter.close().unwrap();
        assert_eq!(first, [(vec![b'a'], vec![b'a']), (vec![b'c'], vec![b'c'])]);
        assert_eq!(next, [(vec![b'e'], vec![b'e']), (vec![b'g'], vec![b'g'])]);
    }
    /// Go `TestTotalMemoryConsume`'s phases, with extsort allocations replacing
    /// Go's manual buffers. Unlike Go's skipped heap calibration, this checks
    /// the actual writer's allocation accounting through spill and flush.
    #[test]
    fn test_total_memory_consume() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(dir.path().join("engine"), 1, false, 64).unwrap();
        assert_eq!(engine.total_memory_size(), 0);
        for i in 0..1000 {
            engine
                .append_rows(
                    0,
                    &[(format!("key{i:04}").into_bytes(), vec![0; 16], vec![])],
                )
                .unwrap();
        }
        assert!(engine.total_memory_size() > 0);
        assert!(
            engine.total_memory_size() < 1024,
            "spilled KV bytes are disk data, not resident memory"
        );
        engine.flush().unwrap();
        assert_eq!(engine.total_memory_size(), 0);
    }
    /// Go `TestLocalIngestLoop`: four concurrent producers, 500 entries each,
    /// interleaved flushes, final size/count and the durable completion boundary.
    /// Extsort flush completion replaces Pebble's SST metadata sequence.
    #[test]
    fn test_local_ingest_loop() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(dir.path().join("engine"), 42, false, 1024).unwrap();
        let total = AtomicU64::new(0);
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let engine = &engine;
                let total = &total;
                scope.spawn(move || {
                    for index in 0..500 {
                        let key = ((worker * 500 + index) as u64).to_be_bytes().to_vec();
                        let value = vec![worker as u8; 1 + index % 50];
                        total.fetch_add((key.len() + value.len()) as u64, Ordering::SeqCst);
                        engine.append_rows(worker, &[(key, value, vec![])]).unwrap();
                        if index % 10 == 9 {
                            engine.flush().unwrap();
                        }
                    }
                });
            }
        });
        engine.finish_write().unwrap();
        assert_eq!(
            engine.kv_statistics().unwrap(),
            (total.load(Ordering::SeqCst), 2000)
        );
        let generation = engine.flushed_generation();
        engine.close().unwrap();
        let reopened = Engine::open(dir.path().join("engine"), 0, false, 1024).unwrap();
        assert_eq!(reopened.flushed_generation(), generation);
        assert_eq!(
            reopened.kv_statistics().unwrap(),
            (total.load(Ordering::SeqCst), 2000)
        );
    }

    /// Go `TestSplitRangeAgain4BigRegion`, including its mockGetSizeProperties:
    /// 20 properties of 50 MiB produce ten jobs inside a single 1 GiB region.
    #[test]
    fn test_split_range_again4_big_region() {
        let mut properties = SizeProperties::default();
        for i in 1..=10u8 {
            for key in [vec![i], vec![i, 1]] {
                properties.add(RangeProperty {
                    key,
                    size: 50 * 1024 * 1024,
                    keys: 100_000,
                });
            }
        }
        let keys = load_ingest_data_keys(Some(vec![vec![1], vec![11]]), 1 << 30, |size, count| {
            assert_eq!((size, count), (96 * 1024 * 1024, 960_000));
            let ranges = crate::local::split_range_by_size_props(
                (vec![1], vec![10, 1, 0]),
                &properties,
                size,
                count,
            );
            let mut keys: Vec<_> = ranges.iter().map(|r| r.0.clone()).collect();
            keys.push(ranges.last().unwrap().1.clone());
            Ok(keys)
        })
        .unwrap();
        assert_eq!(keys.len(), 11);
        for (i, key) in keys[..10].iter().enumerate() {
            assert_eq!(key, &vec![i as u8 + 1]);
        }
        assert_eq!(keys[10], vec![10, 1, 0]);
        let cached = vec![vec![1], vec![11]];
        assert_eq!(
            load_ingest_data_keys(Some(cached.clone()), 96 * 1024 * 1024, |_, _| panic!(
                "cached ordinary ranges must be reused"
            ))
            .unwrap(),
            cached
        );
    }
}
