// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/engineapi`: what the local and the external (global
//! sort) engines share with the ingest backend.

use serde::{Deserialize, Serialize};

/// Go `Range`: `[start, end)`, keys not encoded by duplicate detection.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Range {
    /// Go `Start`.
    pub start: Vec<u8>,
    /// Go `End`: exclusive.
    pub end: Vec<u8>,
}

/// Go `ConflictInfo`: the recorded conflict KV pairs (PK or UK) and the
/// files, in the sorted KV format, that hold them.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ConflictInfo {
    /// Go `Count`.
    #[serde(skip_serializing_if = "is_zero")]
    pub count: u64,
    /// Go `Files`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<String>,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

impl ConflictInfo {
    /// Go `Merge`.
    pub fn merge(&mut self, other: &ConflictInfo) {
        self.count += other.count;
        self.files.extend(other.files.iter().cloned());
    }
}

/// Go `OnDuplicateKey`: what global sort does with a duplicate key.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum OnDuplicateKey {
    /// Go `OnDuplicateKeyIgnore`: keep every duplicate.
    #[default]
    Ignore = 0,
    /// Go `OnDuplicateKeyRecord`: record duplicates to the external store.
    /// The encode and merge steps only see local data, so they keep 2 of
    /// each key and record the rest.
    Record = 1,
    /// Go `OnDuplicateKeyRemove`: drop duplicates silently.
    Remove = 2,
    /// Go `OnDuplicateKeyError`.
    Error = 3,
}

impl OnDuplicateKey {
    /// The Go integer value.
    #[must_use]
    pub fn from_i64(v: i64) -> Self {
        match v {
            1 => Self::Record,
            2 => Self::Remove,
            3 => Self::Error,
            _ => Self::Ignore,
        }
    }
}

impl std::fmt::Display for OnDuplicateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Ignore => "ignore",
            Self::Record => "record",
            Self::Remove => "remove",
            Self::Error => "error",
        })
    }
}

impl Serialize for OnDuplicateKey {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_i64(*self as i64)
    }
}

impl<'de> Deserialize<'de> for OnDuplicateKey {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Self::from_i64(i64::deserialize(d)?))
    }
}

/// The errors of an engine or its ingest data, keeping the Go error a
/// caller tests for (such as `ErrFoundDuplicateKeys`).
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A key and its value.
pub type Pair = (Vec<u8>, Vec<u8>);

/// Go `ForwardIter` over [`IngestData`]: the pairs in key order.
pub trait ForwardIter {
    /// Go `Next` (after `First`) with `Valid`, `Key` and `Value`: the next
    /// pair, or `None` at the end.
    ///
    /// # Errors
    /// The data could not be read.
    fn next_pair(&mut self) -> Result<Option<Pair>, Error>;
}

/// Go `IngestData`: the KV pairs a region job writes and ingests. The keys
/// are not encoded by the TiKV codec.
pub trait IngestData: Send + Sync {
    /// Go `GetFirstAndLastKey` in `[lower, upper)`; an empty bound is
    /// unbounded. `None` when there is no pair in the range.
    ///
    /// # Errors
    /// The data could not be read.
    fn get_first_and_last_key(&self, lower: &[u8], upper: &[u8]) -> Result<Option<Pair>, Error>;
    /// Go `NewIter` over `[lower, upper)`.
    ///
    /// # Errors
    /// The data could not be read.
    fn new_iter<'a>(&'a self, lower: &[u8], upper: &[u8]) -> Result<Box<dyn ForwardIter + 'a>, Error>;
    /// Go `GetTS`: the commit TS of the pairs.
    fn get_ts(&self) -> u64;
    /// Go `Finish`: counts pairs written and ingested.
    fn finish(&self, total_bytes: i64, total_count: i64);
}

/// Go `DataAndRanges`: data, and the sorted ranges of it to ingest as
/// separate jobs.
pub struct DataAndRanges {
    /// Go `Data`.
    pub data: std::sync::Arc<dyn IngestData>,
    /// Go `SortedRanges`.
    pub sorted_ranges: Vec<Range>,
}

/// Go `Engine`: the data the ingest backend writes into TiKV. `Any` is Go's
/// type assertion to the concrete engine (`e.(*globalsort.Engine)`).
pub trait Engine: Send + std::any::Any {
    /// Go `ID`.
    fn id(&self) -> String;
    /// Go `LoadIngestData`: hands every batch of loaded data to `out`. Go
    /// sends them to a channel the region job generator reads; here `out`
    /// is called on the loading thread, so a batch is ingested before the
    /// next one is loaded.
    ///
    /// # Errors
    /// Loading failed, or `out` did.
    fn load_ingest_data(
        &mut self,
        ctx: &tidb_gocontext::Context,
        out: &mut dyn FnMut(DataAndRanges) -> Result<(), Error>,
    ) -> Result<(), Error>;
    /// Go `KVStatistics`: total size and count.
    fn kv_statistics(&self) -> (i64, i64);
    /// Go `ImportedStatistics`: size and count written and ingested.
    fn imported_statistics(&self) -> (i64, i64);
    /// Go `ConflictInfo`.
    fn conflict_info(&self) -> ConflictInfo;
    /// Go `GetKeyRange`: `[start, end)`.
    ///
    /// # Errors
    /// The range could not be read.
    fn get_key_range(&self) -> Result<(Vec<u8>, Vec<u8>), Error>;
    /// Go `GetRegionSplitKeys`: the keys to split regions at, including the
    /// start and end keys.
    ///
    /// # Errors
    /// The keys could not be read.
    fn get_region_split_keys(&self) -> Result<Vec<Vec<u8>>, Error>;
    /// Go `Close`.
    ///
    /// # Errors
    /// Releasing the engine failed.
    fn close(&mut self) -> Result<(), Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go's JSON of `ConflictInfo` (`omitempty`) and `OnDuplicateKey` (an
    /// int), and `Merge`.
    #[test]
    fn conflict_info_json_and_merge() {
        let mut a = ConflictInfo::default();
        assert_eq!(serde_json::to_string(&a).unwrap(), "{}");
        a.merge(&ConflictInfo {
            count: 2,
            files: vec!["f1".into()],
        });
        a.merge(&ConflictInfo {
            count: 1,
            files: vec!["f2".into()],
        });
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            r#"{"count":3,"files":["f1","f2"]}"#
        );
        assert_eq!(serde_json::to_string(&OnDuplicateKey::Remove).unwrap(), "2");
        assert_eq!(OnDuplicateKey::Record.to_string(), "record");
    }
}
