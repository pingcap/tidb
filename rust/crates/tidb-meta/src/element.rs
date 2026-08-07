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

//! Go `meta.Element` (`pkg/meta/meta.go`): which column or index a DDL
//! backfill job is currently reorganizing, stored as a fixed 13-byte value in
//! the DDL reorg record.
//!
//! It is an on-disk contract, not a convenience: a Go node that resumes a
//! reorg job written by this node reads these exact bytes back, so the
//! five-byte prefix, the big-endian id, and the 13-byte length are all fixed.

use std::fmt;

/// Go `elementKeyLen`.
pub const ELEMENT_KEY_LEN: usize = 5;
/// Go `EncodeElement`'s buffer: the 5-byte prefix plus a big-endian `int64`.
const ELEMENT_LEN: usize = ELEMENT_KEY_LEN + 8;

/// Go `ColumnElementKey`.
pub const COLUMN_ELEMENT_KEY: &[u8] = b"_col_";
/// Go `IndexElementKey`.
pub const INDEX_ELEMENT_KEY: &[u8] = b"_idx_";
/// Go `ElementKeyType`; kept as owned bytes because Go's defined slice type
/// accepts arbitrary mutable byte sequences.
pub type ElementKeyType = Vec<u8>;

/// Which valid kind of object a backfill element names.
///
/// Go's stored `Element.TypeKey` remains raw bytes and can carry invalid
/// values until decode. This enum is therefore only a convenience for the two
/// canonical values; [`Element`] itself preserves the full Go byte domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ElementKind {
    /// Go `ColumnElementKey`, `_col_`.
    Column,
    /// Go `IndexElementKey`, `_idx_`.
    Index,
}

impl ElementKind {
    /// The five prefix bytes this kind writes.
    #[must_use]
    pub fn type_key(self) -> &'static [u8] {
        match self {
            Self::Column => COLUMN_ELEMENT_KEY,
            Self::Index => INDEX_ELEMENT_KEY,
        }
    }

    /// Builds an element with this canonical Go type key.
    #[must_use]
    pub fn element(self, id: i64) -> Element {
        Element {
            id,
            type_key: self.type_key().to_vec(),
        }
    }
}

/// Go `meta.Element`: a backfill job's object kind and id.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Element {
    /// Go `Element.ID`.
    pub id: i64,
    /// Go `Element.TypeKey`, including invalid short, long, or non-UTF-8 keys.
    pub type_key: ElementKeyType,
}

impl Element {
    /// Go `Element.EncodeElement`: the 5-byte type key then the id big-endian.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![0; ELEMENT_LEN];
        let copied = self.type_key.len().min(ELEMENT_KEY_LEN);
        bytes[..copied].copy_from_slice(&self.type_key[..copied]);
        bytes[ELEMENT_KEY_LEN..].copy_from_slice(&(self.id as u64).to_be_bytes());
        bytes
    }

    /// The exact bytes returned by Go `Element.String`.
    ///
    /// Go strings may contain invalid UTF-8, so the byte-returning form is the
    /// lossless source contract. [`fmt::Display`] is exact for valid UTF-8 and
    /// uses Rust's replacement character only when a formatter cannot carry
    /// Go's invalid string bytes.
    #[must_use]
    pub fn string_bytes(&self) -> Vec<u8> {
        let mut value = format!("ID:{},TypeKey:", self.id).into_bytes();
        value.extend_from_slice(&self.type_key);
        value
    }

    /// Go `meta.DecodeElement`.
    ///
    /// Go reads the id as a `uint64` and casts, so a negative id round-trips.
    pub fn decode(bytes: &[u8]) -> Result<Self, ElementError> {
        if bytes.len() < ELEMENT_LEN {
            return Err(ElementError::Length(bytes.to_vec()));
        }
        let (prefix, rest) = bytes.split_at(ELEMENT_KEY_LEN);
        if prefix != INDEX_ELEMENT_KEY && prefix != COLUMN_ELEMENT_KEY {
            return Err(ElementError::Prefix(prefix.to_vec()));
        }
        let id = u64::from_be_bytes(rest[..8].try_into().expect("eight bytes remain")) as i64;
        Ok(Element {
            id,
            type_key: prefix.to_vec(),
        })
    }
}

impl fmt::Display for Element {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&String::from_utf8_lossy(&self.string_bytes()))
    }
}

/// Go `DecodeElement`'s two `errors.Errorf` messages, spelled the same way --
/// they reach an operator through a stalled DDL job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ElementError {
    /// Go: `invalid encoded element %q length %d`.
    Length(Vec<u8>),
    /// Go: `invalid encoded element key prefix %q`.
    Prefix(Vec<u8>),
}

impl fmt::Display for ElementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Length(bytes) => write!(
                formatter,
                "invalid encoded element {} length {}",
                GoQuoted(bytes),
                bytes.len()
            ),
            Self::Prefix(prefix) => write!(
                formatter,
                "invalid encoded element key prefix {}",
                GoQuoted(prefix)
            ),
        }
    }
}

impl std::error::Error for ElementError {}

/// Go's `%q` verb over bytes, for the ASCII-or-escape range an element prefix
/// can hold. (The general form, with UTF-8 and `\u` escapes, lives in
/// `tidb_txnkv`'s `key_ranges`; an element prefix is five bytes copied out of
/// a fixed table or zero-padded, so this covers it.)
struct GoQuoted<'a>(&'a [u8]);

impl fmt::Display for GoQuoted<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("\"")?;
        for byte in self.0 {
            match byte {
                b'\\' => formatter.write_str("\\\\")?,
                b'"' => formatter.write_str("\\\"")?,
                0x20..=0x7e => write!(formatter, "{}", *byte as char)?,
                _ => write!(formatter, "\\x{byte:02x}")?,
            }
        }
        formatter.write_str("\"")
    }
}
