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
