// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/engine.go` and `localWriter` in `local.go`.
//! Immutable sorted runs use the existing native external sorter. A flush
//! freezes a generation; imports remove only that generation's files, leaving
//! rows arriving during import for the next flush.
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
}
/// Go `Engine`: sorted index data on local disk, with a stable import TS.
pub struct Engine {
    path: PathBuf,
    duplicate_detection: bool,
    writer_cache_size: usize,
    state: Mutex<State>,
    ts: AtomicU64,
    importing: AtomicBool,
    imported_kvs: std::sync::atomic::AtomicI64,
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
            }),
            ts: AtomicU64::new(ts),
            importing: AtomicBool::new(false),
            imported_kvs: std::sync::atomic::AtomicI64::new(0),
        })
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
            let encoded;
            let stored = if self.duplicate_detection {
                encoded = encode_duplicate_key(key, row_id);
                encoded.as_slice()
            } else {
                key.as_slice()
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
        Ok(EngineIterator {
            cursors,
            duplicate_detection: self.duplicate_detection,
            end: end.to_vec(),
            last_encoded: None,
            last_key: None,
        })
    }
    /// Go `Engine.GetFirstAndLastKey` for the half-open range.
    pub fn get_first_and_last_key(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut it = self.new_iter(start, end)?;
        let Some((first, _)) = it.next_pair()? else {
            return Ok(None);
        };
        let mut last = first.clone();
        while let Some((key, _)) = it.next_pair()? {
            last = key;
        }
        Ok(Some((first, last)))
    }
    /// Go `Engine.importedKVCount`: the KV pairs ingested so far.
    pub fn imported_kvs(&self) -> i64 {
        self.imported_kvs.load(Ordering::SeqCst)
    }

    pub(crate) fn add_imported_kvs(&self, n: i64) {
        self.imported_kvs.fetch_add(n, Ordering::SeqCst);
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
            mem_size: self.state.lock().unwrap().buffered_bytes as i64,
            is_importing: self.importing.load(Ordering::SeqCst),
        }
    }
    /// Go `Engine.isImporting`.
    pub fn set_importing(&self, value: bool) {
        self.importing.store(value, Ordering::SeqCst);
    }
}
fn options(writer_buffer_size: usize) -> DiskSorterOptions {
    DiskSorterOptions {
        writer_buffer_size,
        concurrency: 4,
        ..Default::default()
    }
}
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
/// Go `EngineIterator`, skipping identical storage keys and detecting distinct
/// handles for the same unique index key.
pub struct EngineIterator {
    cursors: Vec<Box<dyn tidb_extsort::Iterator>>,
    duplicate_detection: bool,
    end: Vec<u8>,
    last_encoded: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
}
impl EngineIterator {
    /// Go `EngineIterator.Next/Key/Value` as one owned pair.
    pub fn next_pair(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            for it in &self.cursors {
                if let Some(e) = it.error() {
                    return Err(Error::Other(e.to_string()));
                }
            }
            // For repeated identical keys the newest run wins, like Pebble Set.
            let selected = self
                .cursors
                .iter()
                .enumerate()
                .filter(|(_, it)| it.valid())
                .min_by(|(ia, a), (ib, b)| a.unsafe_key().cmp(b.unsafe_key()).then(ib.cmp(ia)))
                .map(|(i, _)| i);
            let Some(i) = selected else {
                return Ok(None);
            };
            let encoded = self.cursors[i].unsafe_key().to_vec();
            let value = self.cursors[i].unsafe_value().to_vec();
            self.cursors[i].next();
            if self.last_encoded.as_deref() == Some(encoded.as_slice()) {
                continue;
            }
            let key = if self.duplicate_detection {
                decode_duplicate_key(&encoded)?
            } else {
                encoded.clone()
            };
            if !self.end.is_empty() && key >= self.end {
                return Ok(None);
            }
            self.last_encoded = Some(encoded);
            if self.duplicate_detection && self.last_key.as_deref() == Some(key.as_slice()) {
                return Err(Error::Duplicate { key, value });
            }
            self.last_key = Some(key.clone());
            return Ok(Some((key, value)));
        }
    }
}
impl Drop for EngineIterator {
    fn drop(&mut self) {
        for it in &mut self.cursors {
            let _ = it.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
