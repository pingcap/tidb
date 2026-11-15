// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/iterator.go`; owned Rust buffers replace membuf,
//! and merging extsort run cursors replaces the Pebble iterator.
use crate::{
    Error, Result,
    engine::{Engine, decode_duplicate_key},
};
use std::sync::Arc;

/// Go `pebbleIter` / `dupDetectIter`, returning owned pairs as Go's buffered
/// Key/Value do. Duplicate collection uses an extsort Engine in place of dupDB.
pub struct EngineIterator {
    cursors: Vec<Box<dyn tidb_extsort::Iterator>>,
    duplicate_detection: bool,
    end: Vec<u8>,
    last_encoded: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    last_value: Vec<u8>,
    duplicate_db: Option<Arc<Engine>>,
    recorded_first: bool,
    failed: Option<(Vec<u8>, Vec<u8>)>,
    exhausted: bool,
    ignore_duplicates: bool,
}
impl EngineIterator {
    /// Go `Engine.newKVIter`, over already-positioned disk cursors.
    pub(crate) fn new(
        cursors: Vec<Box<dyn tidb_extsort::Iterator>>,
        duplicate_detection: bool,
        end: Vec<u8>,
    ) -> Self {
        Self {
            cursors,
            duplicate_detection,
            end,
            last_encoded: None,
            last_key: None,
            last_value: Vec::new(),
            duplicate_db: None,
            recorded_first: false,
            failed: None,
            exhausted: false,
            ignore_duplicates: false,
        }
    }
    /// Go `dupDetectIter.First/Last`: boundary lookup does not scan duplicates.
    pub(crate) fn ignore_duplicates(&mut self) {
        self.ignore_duplicates = true;
    }
    /// Go `newDupDetectIter`, `DupDetectOpt.ErrorOnDup=false`.
    pub fn collect_duplicates(mut self, duplicate_db: Arc<Engine>) -> Self {
        self.duplicate_db = Some(duplicate_db);
        self
    }
    /// Go `dupDBIter`: decode storage keys but retain every duplicate row.
    pub fn next_duplicate_pair(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let Some((encoded, value)) = self.next_encoded_pair()? else {
            return Ok(None);
        };
        self.last_encoded = Some(encoded.clone());
        let key = decode_duplicate_key(&encoded)?;
        if !self.end.is_empty() && key >= self.end {
            self.exhausted = true;
            return Ok(None);
        }
        Ok(Some((key, value)))
    }
    /// Go `pebbleIter.Next`, replacing Pebble's merge with extsort cursors.
    fn next_encoded_pair(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.exhausted {
            return Ok(None);
        }
        loop {
            for it in &self.cursors {
                if let Some(e) = it.error() {
                    return Err(Error::Other(e.to_string()));
                }
            }
            // Pebble Set: newest run wins for an identical encoded key.
            let selected = self
                .cursors
                .iter()
                .enumerate()
                .filter(|(_, it)| it.valid())
                .min_by(|(ia, a), (ib, b)| a.unsafe_key().cmp(b.unsafe_key()).then(ib.cmp(ia)))
                .map(|(i, _)| i);
            let Some(i) = selected else {
                self.exhausted = true;
                return Ok(None);
            };
            let encoded = self.cursors[i].unsafe_key().to_vec();
            let value = self.cursors[i].unsafe_value().to_vec();
            self.cursors[i].next();
            if self.last_encoded.as_ref() == Some(&encoded) {
                continue;
            }
            return Ok(Some((encoded, value)));
        }
    }
    /// Go `dupDetectIter.Next` / `pebbleIter.Next/Key/Value` as one owned pair.
    pub fn next_pair(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if let Some((key, value)) = &self.failed {
            return Err(Error::Duplicate {
                key: key.clone(),
                value: value.clone(),
            });
        }
        while let Some((encoded, value)) = self.next_encoded_pair()? {
            let key = if self.duplicate_detection {
                decode_duplicate_key(&encoded)?
            } else {
                encoded.clone()
            };
            if !self.end.is_empty() && key >= self.end {
                self.exhausted = true;
                return Ok(None);
            }
            if self.duplicate_detection && self.last_key.as_ref() == Some(&key) {
                if self.ignore_duplicates {
                    self.last_encoded = Some(encoded);
                    continue;
                }
                if let Some(db) = &self.duplicate_db {
                    if !self.recorded_first {
                        db.append_rows(
                            0,
                            &[(
                                self.last_encoded.clone().unwrap(),
                                self.last_value.clone(),
                                vec![],
                            )],
                        )?;
                        self.recorded_first = true;
                    }
                    db.append_rows(0, &[(encoded.clone(), value.clone(), vec![])])?;
                    self.last_encoded = Some(encoded);
                    continue;
                }
                self.failed = Some((key.clone(), value.clone()));
                return Err(Error::Duplicate { key, value });
            }
            self.last_encoded = Some(encoded);
            self.last_key = Some(key.clone());
            self.last_value = value.clone();
            self.recorded_first = false;
            return Ok(Some((key, value)));
        }
        Ok(None)
    }
    /// Go `dupDetectIter.Close`, flushing the duplicate batch before iterators close.
    pub fn close(&mut self) -> Result<()> {
        let mut error = self.duplicate_db.as_ref().and_then(|db| db.flush().err());
        for it in &mut self.cursors {
            if let Err(e) = it.close() {
                if error.is_none() {
                    error = Some(e.into());
                }
            }
        }
        self.cursors.clear();
        self.exhausted = true;
        error.map_or(Ok(()), Err)
    }
}
impl Drop for EngineIterator {
    /// Go `dupDetectIter.Close` on scope exit.
    fn drop(&mut self) {
        let _ = self.close();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::encode_duplicate_key;
    /// Go `TestKeyAdapterEncoding`, integer and common handle.
    #[test]
    fn test_key_adapter_encoding() {
        for row in [
            ((1u64) ^ (1 << 63)).to_be_bytes().to_vec(),
            b"mock_common_handle".to_vec(),
        ] {
            assert_eq!(
                decode_duplicate_key(&encode_duplicate_key(&[1, 2, 3], &row)).unwrap(),
                [1, 2, 3]
            );
        }
    }
    /// Go `TestDupDetectIterator`: 20 unique, 20 pairs, 10 triples; all 70
    /// duplicate rows must be persisted, including the first member of each group.
    #[test]
    fn test_dup_detect_iterator() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(dir.path().join("kv"), 1, true, 1024).unwrap();
        let duplicates =
            Arc::new(Engine::open(dir.path().join("duplicates"), 1, false, 1024).unwrap());
        let mut rows = Vec::new();
        let mut expected = Vec::new();
        for i in 0u64..50 {
            let count = if i < 20 {
                1
            } else if i < 40 {
                2
            } else {
                3
            };
            for j in 0u64..count {
                let key = i.to_be_bytes().to_vec();
                let val = vec![i as u8, j as u8];
                let row = (i * 3 + j).to_be_bytes().to_vec();
                rows.push((key.clone(), val.clone(), row));
                if count > 1 {
                    expected.push((key, val));
                }
            }
        }
        rows.reverse();
        engine.append_rows(0, &rows).unwrap();
        engine.flush().unwrap();
        let mut iter = engine
            .new_iter(&[], &[])
            .unwrap()
            .collect_duplicates(duplicates.clone());
        for i in 0u64..50 {
            assert_eq!(
                iter.next_pair().unwrap().unwrap(),
                (i.to_be_bytes().to_vec(), vec![i as u8, 0])
            );
        }
        assert!(iter.next_pair().unwrap().is_none());
        iter.close().unwrap();
        let mut iter = duplicates.new_iter(&[], &[]).unwrap();
        let mut actual = Vec::new();
        while let Some(pair) = iter.next_duplicate_pair().unwrap() {
            actual.push(pair);
        }
        assert_eq!(actual, expected);
        let mut bounded = engine
            .new_iter(&20u64.to_be_bytes(), &21u64.to_be_bytes())
            .unwrap();
        assert!(bounded.next_pair().unwrap().is_some());
        assert!(matches!(bounded.next_pair(), Err(Error::Duplicate { .. })));
        assert!(matches!(bounded.next_pair(), Err(Error::Duplicate { .. })));
    }
}
