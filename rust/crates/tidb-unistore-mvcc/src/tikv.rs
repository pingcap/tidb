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

//! Go `tikv.go`: the TiKV-shaped write-CF and lock-CF value encodings, which
//! `load_dump` persists and restores.

use tidb_proto::KvrpcOp;

use super::Lock;

/// Go `WriteType` and its four values (`tikv.go:31-38`).
pub type WriteType = u8;
/// `WriteTypeLock`.
pub const WRITE_TYPE_LOCK: WriteType = b'L';
/// `WriteTypeRollback`.
pub const WRITE_TYPE_ROLLBACK: WriteType = b'R';
/// `WriteTypeDelete`.
pub const WRITE_TYPE_DELETE: WriteType = b'D';
/// `WriteTypePut`.
pub const WRITE_TYPE_PUT: WriteType = b'P';

/// Go `WriteCFValue` (`tikv.go:40`).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WriteCfValue {
    /// `Type`.
    pub write_type: WriteType,
    /// `StartTS`.
    pub start_ts: u64,
    /// `ShortVal`.
    pub short_val: Vec<u8>,
}

/// Go `errInvalidWriteCFValue`, kept as its exact text.
pub const ERR_INVALID_WRITE_CF_VALUE: &str = "invalid write CF value";

/// Go `ParseWriteCFValue` (`tikv.go:48`).
///
/// One Go quirk is preserved by shape: Go returns `ShortVal` as whatever
/// bytes FOLLOW the uvarint — it does not check the `v` short-value prefix
/// [`EncodeWriteCFValue`] writes, so the parsed `ShortVal` of an encoded
/// short value still CONTAINS the `('v', len)` prefix. Parse and encode are
/// not inverses in Go, and are not inverses here.
pub fn parse_write_cf_value(data: &[u8]) -> Result<WriteCfValue, &'static str> {
    if data.is_empty() {
        return Err(ERR_INVALID_WRITE_CF_VALUE);
    }
    let write_type = data[0];
    match write_type {
        WRITE_TYPE_PUT | WRITE_TYPE_DELETE | WRITE_TYPE_LOCK | WRITE_TYPE_ROLLBACK => {}
        _ => return Err(ERR_INVALID_WRITE_CF_VALUE),
    }
    let (short_val, start_ts) =
        tidb_codec::decode_uvarint(&data[1..]).map_err(|_| ERR_INVALID_WRITE_CF_VALUE)?;
    Ok(WriteCfValue {
        write_type,
        start_ts,
        short_val: short_val.to_vec(),
    })
}

/// Go `shortValuePrefix` / `forUpdatePrefix` / `minCommitTsPrefix`
/// (`tikv.go:63-65`).
const SHORT_VALUE_PREFIX: u8 = b'v';
const FOR_UPDATE_PREFIX: u8 = b'f';
const MIN_COMMIT_TS_PREFIX: u8 = b'm';

/// Go `ShortValueMaxLen` (`tikv.go:68`).
pub const SHORT_VALUE_MAX_LEN: usize = 64;

/// Go `EncodeWriteCFValue` (`tikv.go:71`): type byte, uvarint start TS, and
/// — only when the short value is non-empty — `('v', len, bytes)`.
#[must_use]
pub fn encode_write_cf_value(write_type: WriteType, start_ts: u64, short_val: &[u8]) -> Vec<u8> {
    let mut data = vec![write_type];
    tidb_codec::encode_uvarint(&mut data, start_ts);
    if !short_val.is_empty() {
        data.push(SHORT_VALUE_PREFIX);
        data.push(
            u8::try_from(short_val.len()).expect("short value fits a byte, as Go's cast assumes"),
        );
        data.extend_from_slice(short_val);
    }
    data
}

/// Go `LockType` and its four values (`tikv.go:121-129`).
pub type LockType = u8;
/// `LockTypePut`.
pub const LOCK_TYPE_PUT: LockType = b'P';
/// `LockTypeDelete`.
pub const LOCK_TYPE_DELETE: LockType = b'D';
/// `LockTypeLock`.
pub const LOCK_TYPE_LOCK: LockType = b'L';
/// `LockTypePessimistic`.
pub const LOCK_TYPE_PESSIMISTIC: LockType = b'S';

/// Go `EncodeLockCFValue` (`tikv.go:84`): `(lock-cf bytes, long value)`.
///
/// The lock's op maps to a lock-type byte — any other op PANICS, Go's own
/// `panic("invalid lock op")`. A value longer than [`SHORT_VALUE_MAX_LEN`]
/// is returned separately as the long value instead of being inlined.
#[must_use]
pub fn encode_lock_cf_value(lock: &Lock) -> (Vec<u8>, Vec<u8>) {
    let mut data = Vec::new();
    let op = i32::from(lock.hdr.op);
    if op == KvrpcOp::Put as i32 {
        data.push(LOCK_TYPE_PUT);
    } else if op == KvrpcOp::Del as i32 {
        data.push(LOCK_TYPE_DELETE);
    } else if op == KvrpcOp::Lock as i32 {
        data.push(LOCK_TYPE_LOCK);
    } else if op == KvrpcOp::PessimisticLock as i32 {
        data.push(LOCK_TYPE_PESSIMISTIC);
    } else {
        panic!("invalid lock op");
    }
    let mut long_value = Vec::new();
    tidb_codec::encode_compact_bytes(&mut data, &lock.primary);
    tidb_codec::encode_uvarint(&mut data, lock.hdr.start_ts);
    tidb_codec::encode_uvarint(&mut data, u64::from(lock.hdr.ttl));
    if lock.value.len() <= SHORT_VALUE_MAX_LEN {
        if !lock.value.is_empty() {
            data.push(SHORT_VALUE_PREFIX);
            data.push(u8::try_from(lock.value.len()).expect("short value fits a byte"));
            data.extend_from_slice(&lock.value);
        }
    } else {
        // Go `y.SafeCopy(nil, lock.Value)`: a plain owned copy.
        long_value = lock.value.clone();
    }
    if lock.hdr.for_update_ts > 0 {
        data.push(FOR_UPDATE_PREFIX);
        tidb_codec::encode_uint(&mut data, lock.hdr.for_update_ts);
    }
    if lock.hdr.min_commit_ts > 0 {
        data.push(MIN_COMMIT_TS_PREFIX);
        tidb_codec::encode_uint(&mut data, lock.hdr.min_commit_ts);
    }
    (data, long_value)
}
