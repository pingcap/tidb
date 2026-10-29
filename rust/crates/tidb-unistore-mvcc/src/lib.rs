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

//! Go `pkg/store/mockstore/unistore/tikv/mvcc` — the MVCC value encodings the
//! unistore engine is built from.
//!
//! LANDS AS A COMPLETE PACKAGE, three files to three modules:
//! * this file — `mvcc.go` (188 lines): the lock record, the DB user meta,
//!   and the extra-transaction-status key.
//! * [`db_writer`] — `db_writer.go` (69): the writer/batch/latch seams and
//!   the engine bundle.
//! * [`tikv`] — `tikv.go` (129): the TiKV-shaped write-CF and lock-CF value
//!   encodings `load_dump` reads and writes.
//!
//! # The lock wire format is Go's MEMORY layout
//!
//! Go marshals a lock by `unsafe.Pointer`-copying `LockHdr` into the buffer
//! (`mvcc.go:88`), so the format is the Go struct layout, padding included:
//! `StartTS@0, ForUpdateTS@8, MinCommitTS@16, TTL@24, Op@28, HasOldVer@29,
//! PrimaryLen@30, UseAsyncCommit@32, [3 pad], SecondaryNum@36`, 40 bytes.
//! These bytes never leave the process in Go either — they live in the
//! lockstore — but `load_dump` persists them, so the layout is reproduced
//! here field-for-field at those exact offsets rather than re-derived.
//! A test pins every offset.
//!
//! # Narrowings
//!
//! * `kvrpcpb.LockInfo` / `kvrpcpb.Op` are the real
//!   [`tidb_proto::KvrpcLockInfo`] / [`tidb_proto::KvrpcOp`].
//! * `badger.DB` / `badger.Txn` (the committed-data engine) are unported;
//!   [`db_writer::DBBundle`] and [`db_writer::DBSnapshot`] carry the engine
//!   GENERICALLY so the bundle's shape survives without inventing one.
//! * `y.SafeCopy` is a plain owned copy.

pub mod db_writer;
pub mod tikv;

use tidb_proto::{KvrpcLockInfo, KvrpcOp};

/// Go `defaultEndian`: little-endian, the only endianness this package uses.
const LOCK_HDR_SIZE: usize = 40;

/// Go `DBUserMeta` (`mvcc.go:31`): `startTS ++ commitTS`, little-endian,
/// 16 bytes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DbUserMeta(pub Vec<u8>);

impl DbUserMeta {
    /// Go `NewDBUserMeta` (`mvcc.go:154`).
    #[must_use]
    pub fn new(start_ts: u64, commit_ts: u64) -> Self {
        let mut meta = Vec::with_capacity(16);
        meta.extend_from_slice(&start_ts.to_le_bytes());
        meta.extend_from_slice(&commit_ts.to_le_bytes());
        Self(meta)
    }

    /// Go `DBUserMeta.CommitTS` (`mvcc.go:162`). Panics on a short buffer
    /// exactly where Go's slice index does.
    #[must_use]
    pub fn commit_ts(&self) -> u64 {
        u64::from_le_bytes(self.0[8..16].try_into().expect("16-byte user meta"))
    }

    /// Go `DBUserMeta.StartTS` (`mvcc.go:167`).
    #[must_use]
    pub fn start_ts(&self) -> u64 {
        u64::from_le_bytes(self.0[..8].try_into().expect("16-byte user meta"))
    }
}

/// Go `LockHdr` (`mvcc.go:54`): the fixed-size head of a lock record.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LockHdr {
    /// `StartTS`.
    pub start_ts: u64,
    /// `ForUpdateTS`.
    pub for_update_ts: u64,
    /// `MinCommitTS`.
    pub min_commit_ts: u64,
    /// `TTL`.
    pub ttl: u32,
    /// `Op`, a raw `kvrpcpb.Op` byte.
    pub op: u8,
    /// `HasOldVer`.
    pub has_old_ver: bool,
    /// `PrimaryLen`.
    pub primary_len: u16,
    /// `UseAsyncCommit`.
    pub use_async_commit: bool,
    /// `SecondaryNum`.
    pub secondary_num: u32,
}

/// Go `Lock` (`mvcc.go:69`): the in-memory MVCC lock record.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Lock {
    /// The embedded `LockHdr`.
    pub hdr: LockHdr,
    /// `Primary`.
    pub primary: Vec<u8>,
    /// `Value`.
    pub value: Vec<u8>,
    /// `Secondaries`.
    pub secondaries: Vec<Vec<u8>>,
}

impl Lock {
    /// Go `Lock.MarshalBinary` (`mvcc.go:77`): the header at Go's memory
    /// layout, then primary, then `(len ++ key)` per secondary, then the
    /// value.
    #[must_use]
    pub fn marshal_binary(&self) -> Vec<u8> {
        let mut length = LOCK_HDR_SIZE + self.primary.len() + self.value.len();
        if self.hdr.secondary_num > 0 {
            for secondary in &self.secondaries {
                length += 2 + secondary.len();
            }
        }
        let mut buf = vec![0_u8; LOCK_HDR_SIZE];
        buf.reserve(length - LOCK_HDR_SIZE);
        buf[0..8].copy_from_slice(&self.hdr.start_ts.to_le_bytes());
        buf[8..16].copy_from_slice(&self.hdr.for_update_ts.to_le_bytes());
        buf[16..24].copy_from_slice(&self.hdr.min_commit_ts.to_le_bytes());
        buf[24..28].copy_from_slice(&self.hdr.ttl.to_le_bytes());
        buf[28] = self.hdr.op;
        buf[29] = u8::from(self.hdr.has_old_ver);
        buf[30..32].copy_from_slice(&self.hdr.primary_len.to_le_bytes());
        buf[32] = u8::from(self.hdr.use_async_commit);
        // Bytes 33-35 are Go's struct padding, zero.
        buf[36..40].copy_from_slice(&self.hdr.secondary_num.to_le_bytes());
        buf.extend_from_slice(&self.primary);
        if self.hdr.secondary_num > 0 {
            for secondary in &self.secondaries {
                buf.extend_from_slice(
                    &u16::try_from(secondary.len())
                        .expect("secondary key length fits u16, as Go's cast assumes")
                        .to_le_bytes(),
                );
                buf.extend_from_slice(secondary);
            }
        }
        buf.extend_from_slice(&self.value);
        buf
    }

    /// Go `Lock.ToLockInfo` (`mvcc.go:105`).
    #[must_use]
    pub fn to_lock_info(&self, key: Vec<u8>) -> KvrpcLockInfo {
        KvrpcLockInfo {
            primary_lock: self.primary.clone(),
            lock_version: self.hdr.start_ts,
            key,
            lock_ttl: u64::from(self.hdr.ttl),
            lock_type: i32::from(self.hdr.op),
            lock_for_update_ts: self.hdr.for_update_ts,
            use_async_commit: self.hdr.use_async_commit,
            min_commit_ts: self.hdr.min_commit_ts,
            secondaries: self.secondaries.clone(),
            ..KvrpcLockInfo::default()
        }
    }

    /// Go `Lock.String` (`mvcc.go:120`).
    #[must_use]
    pub fn describe(&self) -> String {
        format!(
            "Lock {{ Type: {}, StartTS: {},  ForUpdateTS: {}, Primary: {}, UseAsyncCommit: {} }}",
            KvrpcOp::try_from(i32::from(self.hdr.op))
                .map_or_else(|_| format!("{}", self.hdr.op), |op| format!("{op:?}")),
            self.hdr.start_ts,
            self.hdr.for_update_ts,
            // Go `hex.EncodeToString`, inlined: the workspace carries no hex crate.
            self.primary
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            self.hdr.use_async_commit,
        )
    }
}

/// Go `DecodeLock` (`mvcc.go:34`): the header from its fixed offsets, the
/// primary sized by `PrimaryLen`, `SecondaryNum` length-prefixed secondaries,
/// and everything after them the value.
#[must_use]
pub fn decode_lock(data: &[u8]) -> Lock {
    let hdr = LockHdr {
        start_ts: u64::from_le_bytes(data[0..8].try_into().expect("lock header")),
        for_update_ts: u64::from_le_bytes(data[8..16].try_into().expect("lock header")),
        min_commit_ts: u64::from_le_bytes(data[16..24].try_into().expect("lock header")),
        ttl: u32::from_le_bytes(data[24..28].try_into().expect("lock header")),
        op: data[28],
        has_old_ver: data[29] != 0,
        primary_len: u16::from_le_bytes(data[30..32].try_into().expect("lock header")),
        use_async_commit: data[32] != 0,
        secondary_num: u32::from_le_bytes(data[36..40].try_into().expect("lock header")),
    };
    let lock_buf = &data[LOCK_HDR_SIZE..];
    let mut cursor = usize::from(hdr.primary_len);
    let primary = lock_buf[..cursor].to_vec();
    let mut secondaries = Vec::new();
    if hdr.secondary_num > 0 {
        secondaries.reserve(hdr.secondary_num as usize);
        for _ in 0..hdr.secondary_num {
            let key_len = usize::from(u16::from_le_bytes(
                lock_buf[cursor..cursor + 2]
                    .try_into()
                    .expect("secondary length"),
            ));
            cursor += 2;
            secondaries.push(lock_buf[cursor..cursor + key_len].to_vec());
            cursor += key_len;
        }
    }
    let value = lock_buf[cursor..].to_vec();
    Lock {
        hdr,
        primary,
        value,
        secondaries,
    }
}

/// Go `LockUserMetaNoneByte` / `LockUserMetaDeleteByte` (`mvcc.go:132`).
pub const LOCK_USER_META_NONE_BYTE: u8 = 0;
/// See [`LOCK_USER_META_NONE_BYTE`].
pub const LOCK_USER_META_DELETE_BYTE: u8 = 2;
/// Go `LockUserMetaNone` (`mvcc.go:138`).
pub const LOCK_USER_META_NONE: &[u8] = &[LOCK_USER_META_NONE_BYTE];
/// Go `LockUserMetaDelete`.
pub const LOCK_USER_META_DELETE: &[u8] = &[LOCK_USER_META_DELETE_BYTE];

/// Go `DecodeKeyTS` (`mvcc.go:144`): the descending-encoded TS in a key's
/// last eight bytes. Panics on a decode failure exactly as Go does.
#[must_use]
pub fn decode_key_ts(buf: &[u8]) -> u64 {
    let ts_bin = &buf[buf.len() - 8..];
    let (_, ts) = tidb_codec::decode_uint_desc(ts_bin).expect("a desc-encoded key TS");
    ts
}

/// Go `EncodeExtraTxnStatusKey` (`mvcc.go:173`): clone the key, append the
/// DESCENDING start TS, then increment the FIRST byte of the whole result —
/// Go's quirk, which shifts the key into the next keyspace prefix so extra
/// status records sort apart from data. Only used for Rollback and `Op_Lock`.
#[must_use]
pub fn encode_extra_txn_status_key(key: &[u8], start_ts: u64) -> Vec<u8> {
    let mut ret = key.to_vec();
    tidb_codec::encode_uint_desc(&mut ret, start_ts);
    ret[0] = ret[0].wrapping_add(1);
    ret
}

/// Go `DecodeExtraTxnStatusKey` (`mvcc.go:181`): `nil` — here empty — for
/// anything nine bytes or shorter, Go's guard included.
#[must_use]
pub fn decode_extra_txn_status_key(extra_key: &[u8]) -> Vec<u8> {
    if extra_key.len() <= 9 {
        return Vec::new();
    }
    let mut key = extra_key[..extra_key.len() - 8].to_vec();
    key[0] = key[0].wrapping_sub(1);
    key
}

#[cfg(test)]
mod tests {
    // All WRITTEN: the Go subpackage ships no tests of its own — its behavior
    // is exercised through `tikv/mvcc_test.go`, which tests the STORE above
    // it. These pin the encodings that store batch will sit on.
    use super::*;

    fn full_lock() -> Lock {
        Lock {
            hdr: LockHdr {
                start_ts: 400_001,
                for_update_ts: 400_005,
                min_commit_ts: 400_009,
                ttl: 3000,
                op: KvrpcOp::Put as i32 as u8,
                has_old_ver: true,
                primary_len: 3,
                use_async_commit: true,
                secondary_num: 2,
            },
            primary: b"pk1".to_vec(),
            value: b"the value".to_vec(),
            secondaries: vec![b"sk1".to_vec(), b"sk_two".to_vec()],
        }
    }

    #[test]
    fn a_lock_roundtrips_through_gos_memory_layout() {
        let lock = full_lock();
        let decoded = decode_lock(&lock.marshal_binary());
        assert_eq!(decoded, lock);
    }

    #[test]
    fn the_header_bytes_sit_at_gos_struct_offsets() {
        // `unsafe.Sizeof(LockHdr{})` is 40, with padding at 33-35; the wire
        // format IS that layout, so every offset is pinned.
        let buf = full_lock().marshal_binary();
        assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 400_001);
        assert_eq!(u64::from_le_bytes(buf[8..16].try_into().unwrap()), 400_005);
        assert_eq!(u64::from_le_bytes(buf[16..24].try_into().unwrap()), 400_009);
        assert_eq!(u32::from_le_bytes(buf[24..28].try_into().unwrap()), 3000);
        assert_eq!(buf[28], KvrpcOp::Put as i32 as u8);
        assert_eq!(buf[29], 1);
        assert_eq!(u16::from_le_bytes(buf[30..32].try_into().unwrap()), 3);
        assert_eq!(buf[32], 1);
        assert_eq!(&buf[33..36], &[0, 0, 0], "Go's struct padding");
        assert_eq!(u32::from_le_bytes(buf[36..40].try_into().unwrap()), 2);
        assert_eq!(&buf[40..43], b"pk1");
    }

    #[test]
    fn a_lock_without_secondaries_skips_their_encoding() {
        let mut lock = full_lock();
        lock.hdr.secondary_num = 0;
        lock.secondaries = Vec::new();
        let decoded = decode_lock(&lock.marshal_binary());
        assert_eq!(decoded, lock);
        assert!(decoded.secondaries.is_empty());
    }

    #[test]
    fn user_meta_is_start_then_commit_little_endian() {
        let meta = DbUserMeta::new(77, 99);
        assert_eq!(meta.start_ts(), 77);
        assert_eq!(meta.commit_ts(), 99);
        assert_eq!(meta.0.len(), 16);
    }

    #[test]
    fn the_extra_status_key_bumps_the_first_byte_and_comes_back() {
        // `EncodeExtraTxnStatusKey`: append DESC ts, then `ret[0]++` — the
        // whole key shifts one keyspace prefix up so status records sort
        // apart from data.
        let key = b"txyz".to_vec();
        let encoded = encode_extra_txn_status_key(&key, 12_345);
        assert_eq!(encoded[0], b't' + 1);
        assert_eq!(decode_key_ts(&encoded), 12_345);
        assert_eq!(decode_extra_txn_status_key(&encoded), key);
    }

    #[test]
    fn a_nine_byte_extra_key_decodes_to_nothing() {
        // Go's `len(extraKey) <= 9` guard answers nil.
        assert!(decode_extra_txn_status_key(&[0_u8; 9]).is_empty());
        assert!(decode_extra_txn_status_key(&[0_u8; 4]).is_empty());
    }

    #[test]
    fn write_cf_parse_keeps_the_short_value_prefix_go_does_not_strip() {
        // `ParseWriteCFValue` reads type + uvarint and calls EVERYTHING after
        // the uvarint `ShortVal` — including the `('v', len)` prefix
        // `EncodeWriteCFValue` wrote. Parse and encode are NOT inverses in
        // Go, and must not become inverses here.
        let encoded = tikv::encode_write_cf_value(tikv::WRITE_TYPE_PUT, 42, b"abc");
        let parsed = tikv::parse_write_cf_value(&encoded).expect("valid");
        assert_eq!(parsed.write_type, tikv::WRITE_TYPE_PUT);
        assert_eq!(parsed.start_ts, 42);
        assert_eq!(parsed.short_val, b"\x76\x03abc", "prefix retained");
    }

    #[test]
    fn write_cf_rejects_what_go_rejects() {
        assert_eq!(
            tikv::parse_write_cf_value(&[]),
            Err(tikv::ERR_INVALID_WRITE_CF_VALUE)
        );
        assert_eq!(
            tikv::parse_write_cf_value(b"X\x01"),
            Err(tikv::ERR_INVALID_WRITE_CF_VALUE)
        );
    }

    #[test]
    fn lock_cf_splits_long_values_out() {
        let mut lock = full_lock();
        lock.hdr.for_update_ts = 0;
        lock.hdr.min_commit_ts = 0;
        let (short_encoded, long) = tikv::encode_lock_cf_value(&lock);
        assert!(long.is_empty(), "a 9-byte value stays inline");
        assert!(short_encoded.starts_with(&[tikv::LOCK_TYPE_PUT]));

        lock.value = vec![7_u8; tikv::SHORT_VALUE_MAX_LEN + 1];
        let (encoded, long) = tikv::encode_lock_cf_value(&lock);
        assert_eq!(long, lock.value, "65 bytes go to the long value");
        assert!(!encoded.windows(2).any(|w| w == [b'v', 65]));
    }

    #[test]
    fn lock_cf_suffixes_appear_only_when_their_ts_is_set() {
        let mut lock = full_lock();
        lock.value = Vec::new();
        let (with_both, _) = tikv::encode_lock_cf_value(&lock);
        lock.hdr.for_update_ts = 0;
        lock.hdr.min_commit_ts = 0;
        let (with_neither, _) = tikv::encode_lock_cf_value(&lock);
        assert!(
            with_both.len() > with_neither.len() + 17,
            "two 9-byte suffixes"
        );
        assert!(!with_neither.contains(&b'f'));
        assert!(!with_neither.contains(&b'm'));
    }

    #[test]
    #[should_panic(expected = "invalid lock op")]
    fn an_unknown_lock_op_panics_with_gos_message() {
        let mut lock = full_lock();
        lock.hdr.op = 250;
        let _ = tikv::encode_lock_cf_value(&lock);
    }

    #[test]
    fn to_lock_info_carries_the_real_proto() {
        let info = full_lock().to_lock_info(b"the-key".to_vec());
        assert_eq!(info.lock_version, 400_001);
        assert_eq!(info.lock_ttl, 3000);
        assert_eq!(info.lock_type, i32::from(KvrpcOp::Put as i32 as u8));
        assert_eq!(info.secondaries.len(), 2);
        assert!(info.use_async_commit);
    }
}
