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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Dependency-closed MVCC metadata from the unistore TiKV test store.
//!
//! This module owns the stable timestamp and write-CF byte contracts in
//! `pkg/store/mockstore/unistore/tikv/mvcc/{mvcc,tikv}.go`.  The lock header is
//! represented as safe semantic metadata so callers can inspect request
//! fields without copying Go's `unsafe.Sizeof` layout.  It deliberately does
//! not own Badger/RocksDB, lock storage, kvproto requests, timestamp-oracle
//! calls, lock resolution, or a transaction protocol.

use std::fmt;

use tidb_codec::{decode_uint_desc, decode_uvarint, encode_uint_desc, encode_uvarint, CodecError};

/// User metadata bytes used by the in-memory lock store for a present lock.
pub const LOCK_USER_META_NONE: [u8; 1] = [0];
/// User metadata bytes used by the in-memory lock store to delete a lock.
pub const LOCK_USER_META_DELETE: [u8; 1] = [2];
/// The maximum short value length used by the source lock encoder.
pub const SHORT_VALUE_MAX_LEN: usize = 64;
/// Prefix for an inline value in a write-CF or lock-CF record.
pub const SHORT_VALUE_PREFIX: u8 = b'v';
/// Prefix for a pessimistic lock's for-update timestamp in lock-CF bytes.
pub const FOR_UPDATE_PREFIX: u8 = b'f';
/// Prefix for a large transaction's minimum commit timestamp in lock-CF bytes.
pub const MIN_COMMIT_TS_PREFIX: u8 = b'm';

/// Errors returned while decoding stable MVCC metadata bytes.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MvccMetadataError {
    /// The write-CF record is empty or uses an unknown write type.
    InvalidWriteCfValue,
    /// The metadata buffer has no user key to carry the extra-status marker.
    EmptyKey,
    /// A DB user-metadata buffer is not exactly the source's sixteen bytes.
    InvalidUserMetaLength(usize),
    /// A codec primitive rejected the encoded timestamp.
    Codec(CodecError),
}

impl fmt::Display for MvccMetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidWriteCfValue => formatter.write_str("invalid write CF value"),
            Self::EmptyKey => {
                formatter.write_str("cannot encode extra transaction status for an empty key")
            }
            Self::InvalidUserMetaLength(length) => {
                write!(
                    formatter,
                    "invalid DB user metadata length {length}, expected 16"
                )
            }
            Self::Codec(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for MvccMetadataError {}

impl From<CodecError> for MvccMetadataError {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}

/// The four write-CF type bytes accepted by `ParseWriteCFValue`.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum WriteType {
    /// A lock record.
    Lock = b'L',
    /// A rollback record.
    Rollback = b'R',
    /// A delete write.
    Delete = b'D',
    /// A put write.
    Put = b'P',
}

impl TryFrom<u8> for WriteType {
    type Error = MvccMetadataError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            b'L' => Ok(Self::Lock),
            b'R' => Ok(Self::Rollback),
            b'D' => Ok(Self::Delete),
            b'P' => Ok(Self::Put),
            _ => Err(MvccMetadataError::InvalidWriteCfValue),
        }
    }
}

impl From<WriteType> for u8 {
    fn from(value: WriteType) -> Self {
        value as u8
    }
}

/// The four lock-CF type bytes emitted by `EncodeLockCFValue`.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub enum LockType {
    /// A put lock.
    Put = b'P',
    /// A delete lock.
    Delete = b'D',
    /// An optimistic lock-only record.
    Lock = b'L',
    /// A pessimistic lock.
    Pessimistic = b'S',
}

impl TryFrom<u8> for LockType {
    type Error = MvccMetadataError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            b'P' => Ok(Self::Put),
            b'D' => Ok(Self::Delete),
            b'L' => Ok(Self::Lock),
            b'S' => Ok(Self::Pessimistic),
            _ => Err(MvccMetadataError::InvalidWriteCfValue),
        }
    }
}

impl From<LockType> for u8 {
    fn from(value: LockType) -> Self {
        value as u8
    }
}

/// A decoded write-CF record.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WriteCfValue {
    /// The record's operation byte.
    pub write_type: WriteType,
    /// The transaction start timestamp encoded after the operation byte.
    pub start_ts: u64,
    /// The source's raw bytes remaining after the start timestamp.
    ///
    /// `pkg/store/mockstore/unistore/tikv/mvcc/tikv.go` assigns the decoder's
    /// remainder directly to `ShortVal`; it therefore includes the `v` prefix
    /// and one-byte length when the record carries an inline value.
    pub short_value: Vec<u8>,
}

/// Encodes one write-CF record using TiDB's source byte order.
#[must_use]
pub fn encode_write_cf_value(write_type: WriteType, start_ts: u64, short_value: &[u8]) -> Vec<u8> {
    let mut data = vec![u8::from(write_type)];
    encode_uvarint(&mut data, start_ts);
    if !short_value.is_empty() {
        data.push(SHORT_VALUE_PREFIX);
        // The source stores this as a byte and documents a 64-byte bound.
        // Keeping the cast preserves Go's byte conversion for callers that
        // have not yet crossed the source's ShortValueMaxLen guard.
        data.push(short_value.len() as u8);
        data.extend_from_slice(short_value);
    }
    data
}

/// Decodes one write-CF record, preserving the source remainder semantics.
pub fn parse_write_cf_value(data: &[u8]) -> Result<WriteCfValue, MvccMetadataError> {
    let (&write_byte, payload) = data
        .split_first()
        .ok_or(MvccMetadataError::InvalidWriteCfValue)?;
    let write_type = WriteType::try_from(write_byte)?;
    let (short_value, start_ts) = decode_uvarint(payload)?;
    Ok(WriteCfValue {
        write_type,
        start_ts,
        short_value: short_value.to_vec(),
    })
}

/// The fixed sixteen-byte user metadata attached to a committed MVCC value.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DbUserMeta([u8; 16]);

impl DbUserMeta {
    /// Creates source-shaped little-endian start and commit timestamp bytes.
    #[must_use]
    pub fn new(start_ts: u64, commit_ts: u64) -> Self {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&start_ts.to_le_bytes());
        bytes[8..].copy_from_slice(&commit_ts.to_le_bytes());
        Self(bytes)
    }

    /// Reads the transaction start timestamp.
    #[must_use]
    pub fn start_ts(self) -> u64 {
        u64::from_le_bytes(self.0[..8].try_into().expect("fixed metadata width"))
    }

    /// Reads the commit timestamp; zero denotes a rollback marker.
    #[must_use]
    pub fn commit_ts(self) -> u64 {
        u64::from_le_bytes(self.0[8..].try_into().expect("fixed metadata width"))
    }

    /// Returns the exact bytes stored in Badger's user metadata field.
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }

    /// Copies a validated Badger user-metadata buffer into the typed value.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, MvccMetadataError> {
        let array = bytes
            .try_into()
            .map_err(|_| MvccMetadataError::InvalidUserMetaLength(bytes.len()))?;
        Ok(Self(array))
    }
}

/// Encodes the extra transaction-status key used by rollback and op-lock rows.
pub fn encode_extra_txn_status_key(
    key: &[u8],
    start_ts: u64,
) -> Result<Vec<u8>, MvccMetadataError> {
    let (&first, rest) = key.split_first().ok_or(MvccMetadataError::EmptyKey)?;
    let mut encoded = Vec::with_capacity(key.len() + 8);
    encoded.push(first.wrapping_add(1));
    encoded.extend_from_slice(rest);
    encode_uint_desc(&mut encoded, start_ts);
    Ok(encoded)
}

/// Decodes the user key portion of an extra transaction-status key.
#[must_use]
pub fn decode_extra_txn_status_key(extra_key: &[u8]) -> Option<Vec<u8>> {
    if extra_key.len() <= 9 {
        return None;
    }
    let mut key = extra_key[..extra_key.len() - 8].to_vec();
    key[0] = key[0].wrapping_sub(1);
    Some(key)
}

/// Decodes the descending timestamp suffix from a versioned key.
pub fn decode_key_ts(encoded_key: &[u8]) -> Result<u64, MvccMetadataError> {
    let suffix = encoded_key
        .get(encoded_key.len().saturating_sub(8)..)
        .filter(|suffix| suffix.len() == 8)
        .ok_or(CodecError::InsufficientBytes)?;
    let (_, ts) = decode_uint_desc(suffix)?;
    Ok(ts)
}

/// Safe semantic representation of the source `mvcc.Lock` header and payload.
///
/// This carries no wire-layout promise. `primary_len` and `secondary_count`
/// expose the derived header values that `MarshalBinary` would write, while
/// serialization, kvproto conversion, and lock-store ownership remain open.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MvccLockMetadata {
    /// Transaction start timestamp.
    pub start_ts: u64,
    /// Pessimistic for-update timestamp.
    pub for_update_ts: u64,
    /// Minimum commit timestamp for large transactions.
    pub min_commit_ts: u64,
    /// Lock lifetime in milliseconds.
    pub ttl: u32,
    /// Source lock operation encoded in lock-CF bytes.
    pub lock_type: LockType,
    /// Whether an older version exists for this lock.
    pub has_old_version: bool,
    /// Primary lock key.
    pub primary: Vec<u8>,
    /// Whether async commit secondaries are present.
    pub use_async_commit: bool,
    /// Async-commit secondary keys.
    pub secondaries: Vec<Vec<u8>>,
    /// Value carried by the lock, when the operation has one.
    pub value: Vec<u8>,
}

impl MvccLockMetadata {
    /// Returns the source `uint16` primary-key length field.
    #[must_use]
    pub fn primary_len(&self) -> usize {
        self.primary.len()
    }

    /// Returns the source `uint32` secondary-key count field.
    #[must_use]
    pub fn secondary_count(&self) -> usize {
        self.secondaries.len()
    }
}
