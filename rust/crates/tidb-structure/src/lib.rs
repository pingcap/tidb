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

//! Go `pkg/structure`: the string/hash/list data model TiDB layers over a raw
//! KV transaction. [`TxStructure`] owns the operations, under one key prefix
//! (Go `NewStructure(reader, readWriter, prefix)`); the free functions are its
//! key codec for a caller holding only the prefix (Go `NewStructure(nil, nil,
//! prefix).EncodeStringDataKey(...)`).
//!
//! Layout (Go `pkg/structure/type.go`), under the structure's prefix:
//!
//! ```text
//! string data: prefix + EncodeBytes(key) + EncodeUint('s')
//! hash meta:   prefix + EncodeBytes(key) + EncodeUint('H')
//! hash data:   prefix + EncodeBytes(key) + EncodeUint('h') + EncodeBytes(field)
//! list meta:   prefix + EncodeBytes(key) + EncodeUint('L')
//! list data:   prefix + EncodeBytes(key) + EncodeUint('l') + EncodeInt(index)
//! ```
//!
//! Errors: [`StructureError`] is what this package raises; everything else
//! (the transaction's own failures, a visitor's error) passes through as the
//! transaction's [`RawTransaction::Error`], as Go returns the `error` it got.

pub mod error;
pub mod structure_errors;
pub mod transaction;

use tidb_codec::{decode_bytes, decode_int, decode_uint, encode_bytes, encode_int, encode_uint};

pub use crate::error::StructureError;
use crate::transaction::{RawKvIterator, RawRangeVisitor, RawTransaction};

/// Go `structure.StringMeta`.
pub const STRING_META: u8 = b'S';
/// Go `structure.StringData`.
pub const STRING_DATA: u8 = b's';
/// Go `structure.HashMeta`.
pub const HASH_META: u8 = b'H';
/// Go `structure.HashData`.
pub const HASH_DATA: u8 = b'h';
/// Go `structure.ListMeta`.
pub const LIST_META: u8 = b'L';
/// Go `structure.ListData`.
pub const LIST_DATA: u8 = b'l';

/// Encodes `prefix` + `EncodeBytes(key)` + `EncodeUint(flag)`, the shared
/// head of every structure key.
fn encode_head_with(prefix: &[u8], key: &[u8], flag: u8) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(prefix.len() + key.len() + 24);
    encoded.extend_from_slice(prefix);
    encode_bytes(&mut encoded, key);
    encode_uint(&mut encoded, u64::from(flag));
    encoded
}

/// Splits `prefix` + `EncodeBytes(key)` + `EncodeUint(flag)` back apart,
/// checking the flag. Returns the decoded key and the bytes after the flag.
fn decode_head_with<'a>(
    prefix: &[u8],
    encoded: &'a [u8],
    expected: u8,
) -> Result<(Vec<u8>, &'a [u8]), StructureError> {
    let tail = encoded.strip_prefix(prefix).ok_or(StructureError::InvalidKeyPrefix)?;
    let (tail, key) = decode_bytes(tail).map_err(|_| StructureError::MalformedKey)?;
    let (tail, flag) = decode_uint(tail).map_err(|_| StructureError::MalformedKey)?;
    if flag != u64::from(expected) {
        // Go reports the low byte of the decoded uint as a character.
        #[expect(clippy::cast_possible_truncation, reason = "Go formats %c of the byte")]
        return Err(StructureError::UnexpectedTypeFlag(flag as u8));
    }
    Ok((key, tail))
}

/// Go `TxStructure.decodeHashDataKey`, under an arbitrary prefix.
fn decode_hash_data_key_with(
    prefix: &[u8],
    encoded: &[u8],
) -> Result<(Vec<u8>, Vec<u8>), StructureError> {
    let (key, tail) = decode_head_with(prefix, encoded, HASH_DATA)?;
    let (_, field) = decode_bytes(tail).map_err(|_| StructureError::MalformedKey)?;
    Ok((key, field))
}

/// Go `TxStructure.EncodeStringDataKey` under `prefix`.
#[must_use]
pub fn encode_string_data_key(prefix: &[u8], key: &[u8]) -> Vec<u8> {
    encode_head_with(prefix, key, STRING_DATA)
}

/// Go `TxStructure.decodeStringDataKey` under `prefix`.
pub fn decode_string_data_key(prefix: &[u8], encoded: &[u8]) -> Result<Vec<u8>, StructureError> {
    decode_head_with(prefix, encoded, STRING_DATA).map(|(key, _)| key)
}

/// Go `TxStructure.EncodeHashMetaKey` under `prefix` (written by v5.1 and
/// earlier).
#[must_use]
pub fn encode_hash_meta_key(prefix: &[u8], key: &[u8]) -> Vec<u8> {
    encode_head_with(prefix, key, HASH_META)
}

/// Go `TxStructure.encodeListMetaKey` under `prefix`.
#[must_use]
pub fn encode_list_meta_key(prefix: &[u8], key: &[u8]) -> Vec<u8> {
    encode_head_with(prefix, key, LIST_META)
}

/// Go `TxStructure.encodeListDataKey` under `prefix`.
#[must_use]
pub fn encode_list_data_key(prefix: &[u8], key: &[u8], index: i64) -> Vec<u8> {
    let mut encoded = encode_head_with(prefix, key, LIST_DATA);
    encode_int(&mut encoded, index);
    encoded
}

/// Decodes [`encode_list_data_key`] back into its key and index.
pub fn decode_list_data_key(prefix: &[u8], encoded: &[u8]) -> Result<(Vec<u8>, i64), StructureError> {
    let (key, tail) = decode_head_with(prefix, encoded, LIST_DATA)?;
    let (_, index) = decode_int(tail).map_err(|_| StructureError::MalformedKey)?;
    Ok((key, index))
}

/// Go writes every structure scalar with `strconv.FormatInt`.
#[must_use]
pub fn encode_int_value(value: i64) -> Vec<u8> {
    value.to_string().into_bytes()
}

/// Go `TxStructure.GetInt64`: `strconv.ParseInt` over the stored bytes.
///
/// Go returns 0 for a missing key before it ever parses; a caller that has no
/// value should not call this.
pub fn parse_int_value(value: &[u8]) -> Result<i64, StructureError> {
    std::str::from_utf8(value)
        .map_err(|_| StructureError::InvalidIntValue)?
        .parse()
        .map_err(|_| StructureError::InvalidIntValue)
}

/// Go `kv.Key.PrefixNext`: the smallest key strictly greater than every key
/// carrying `key` as a prefix.
fn prefix_next(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    for byte in next.iter_mut().rev() {
        *byte = byte.wrapping_add(1);
        if *byte != 0 {
            return next;
        }
    }
    let mut next = key.to_vec();
    next.push(0);
    next
}

/// Callback shape for Go `IterateHashWithBoundedKey`: hash key, field, value.
pub type BoundedHashVisitor<'a, E> = dyn FnMut(&[u8], &[u8], &[u8]) -> Result<(), E> + 'a;

/// Go `structure.HashPair`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HashPair {
    /// Decoded hash field.
    pub field: Vec<u8>,
    /// Stored value.
    pub value: Vec<u8>,
}

/// Go `structure.listMeta`: the valid index range `[l_index, r_index)`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ListMeta {
    l_index: i64,
    r_index: i64,
}

impl ListMeta {
    fn value(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.l_index.to_be_bytes());
        buf.extend_from_slice(&self.r_index.to_be_bytes());
        buf
    }

    fn is_empty(self) -> bool {
        self.l_index >= self.r_index
    }
}

/// Go `structure.adjustIndex`.
fn adjust_index(index: i64, minv: i64, maxv: i64) -> i64 {
    if index >= 0 {
        index + minv
    } else {
        index + maxv
    }
}

/// Go `structure.TxStructure`: string, hash, and list operations over one raw
/// transaction and one key prefix.
///
/// Go's `NewStructure(reader, readWriter, prefix)` carries two handles so a
/// snapshot-backed structure can pass a nil `readWriter`; every mutation on it
/// fails with `ErrWriteOnSnapshot`. Rust cannot alias one transaction behind
/// two mutable handles, so [`TxStructure::read_only`] models the nil
/// `readWriter` instead.
pub struct TxStructure<'a, T> {
    transaction: &'a mut T,
    prefix: Vec<u8>,
    writable: bool,
}

impl<'a, T: RawTransaction> TxStructure<'a, T> {
    /// Go `NewStructure(txn, txn, prefix)`: reads and writes one transaction.
    pub fn new(transaction: &'a mut T, prefix: &[u8]) -> Self {
        Self {
            transaction,
            prefix: prefix.to_vec(),
            writable: true,
        }
    }

    /// Go `NewStructure(snapshot, nil, prefix)`: mutations fail with
    /// `ErrWriteOnSnapshot`.
    pub fn read_only(transaction: &'a mut T, prefix: &[u8]) -> Self {
        Self {
            transaction,
            prefix: prefix.to_vec(),
            writable: false,
        }
    }

    fn require_writable(&self) -> Result<(), StructureError> {
        if self.writable {
            Ok(())
        } else {
            Err(StructureError::WriteOnSnapshot)
        }
    }

    fn string_data_key(&self, key: &[u8]) -> Vec<u8> {
        encode_head_with(&self.prefix, key, STRING_DATA)
    }

    fn hash_data_key(&self, key: &[u8], field: &[u8]) -> Vec<u8> {
        let mut encoded = encode_head_with(&self.prefix, key, HASH_DATA);
        encode_bytes(&mut encoded, field);
        encoded
    }

    fn hash_data_key_prefix(&self, key: &[u8]) -> Vec<u8> {
        encode_head_with(&self.prefix, key, HASH_DATA)
    }

    fn list_meta_key(&self, key: &[u8]) -> Vec<u8> {
        encode_head_with(&self.prefix, key, LIST_META)
    }

    fn list_data_key(&self, key: &[u8], index: i64) -> Vec<u8> {
        let mut encoded = encode_head_with(&self.prefix, key, LIST_DATA);
        encode_int(&mut encoded, index);
        encoded
    }

    /// Go `TxStructure.EncodeStringDataKey` under this structure's prefix.
    pub fn encode_string_data_key(&self, key: &[u8]) -> Vec<u8> {
        self.string_data_key(key)
    }

    /// Go `TxStructure.EncodeHashMetaKey` under this structure's prefix
    /// (written by v5.1 and earlier; exported for tests).
    pub fn encode_hash_meta_key(&self, key: &[u8]) -> Vec<u8> {
        encode_head_with(&self.prefix, key, HASH_META)
    }

    /// Go `TxStructure.EncodeHashDataKey` under this structure's prefix.
    pub fn encode_hash_data_key(&self, key: &[u8], field: &[u8]) -> Vec<u8> {
        self.hash_data_key(key, field)
    }

    // String operations (Go `pkg/structure/string.go`).

    /// Go `TxStructure.Set`.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), T::Error> {
        self.require_writable()?;
        self.transaction
            .set(self.string_data_key(key), value.to_vec())
    }

    /// Go `TxStructure.Get`: a missing key is `None`, not an error.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, T::Error> {
        self.transaction.get(&self.string_data_key(key))
    }

    /// Go `TxStructure.GetInt64`: a missing key is zero.
    pub fn get_int64(&mut self, key: &[u8]) -> Result<i64, T::Error> {
        Ok(self.get(key)?.map_or(Ok(0), |stored| parse_int_value(&stored))?)
    }

    /// Go `TxStructure.Inc` via `kv.IncInt64`: read, add, write back as
    /// decimal ASCII, and return the incremented value.
    pub fn inc(&mut self, key: &[u8], step: i64) -> Result<i64, T::Error> {
        self.require_writable()?;
        let encoded = self.string_data_key(key);
        let current = self
            .transaction
            .get(&encoded)?
            .map_or(Ok(0), |stored| parse_int_value(&stored))?;
        let next = current.wrapping_add(step);
        self.transaction
            .set(encoded, encode_int_value(next))?;
        Ok(next)
    }

    /// Go `TxStructure.Iterate`: every string key in `[key, upper_bound)`.
    pub fn iterate(
        &mut self,
        key: &[u8],
        upper_bound: &[u8],
        visit: &mut RawRangeVisitor<'_, T::Error>,
    ) -> Result<(), T::Error> {
        let start = self.string_data_key(key);
        let end = self.string_data_key(upper_bound);
        let prefix = self.prefix.clone();
        self.transaction
            .iterate_range(&start, &end, &mut |encoded, stored| {
                let (decoded, _) = decode_head_with(&prefix, encoded, STRING_DATA)?;
                visit(&decoded, stored)
            })
    }

    /// Go `TxStructure.Clear`: deleting a missing key succeeds.
    pub fn clear(&mut self, key: &[u8]) -> Result<(), T::Error> {
        self.require_writable()?;
        self.transaction.delete(&self.string_data_key(key))
    }

    // Hash operations (Go `pkg/structure/hash.go`).

    /// Go `TxStructure.HSet`: the write is skipped when the new value equals
    /// the stored one, which is also how writing nil over nothing succeeds.
    pub fn hset(&mut self, key: &[u8], field: &[u8], value: &[u8]) -> Result<(), T::Error> {
        self.require_writable()?;
        let encoded = self.hash_data_key(key, field);
        let old = self.transaction.get(&encoded)?;
        if old.as_deref().unwrap_or_default() == value {
            return Ok(());
        }
        self.transaction.set(encoded, value.to_vec())
    }

    /// Go `TxStructure.HGet`: a missing field is `None`, not an error.
    pub fn hget(&mut self, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>, T::Error> {
        self.transaction.get(&self.hash_data_key(key, field))
    }

    /// Go `TxStructure.EncodeHashAutoIDKeyValue`.
    pub fn encode_hash_auto_id_key_value(
        &self,
        key: &[u8],
        field: &[u8],
        value: i64,
    ) -> (Vec<u8>, Vec<u8>) {
        (
            self.hash_data_key(key, field),
            encode_int_value(value),
        )
    }

    /// Go `TxStructure.HInc`.
    pub fn hinc(&mut self, key: &[u8], field: &[u8], step: i64) -> Result<i64, T::Error> {
        self.require_writable()?;
        let encoded = self.hash_data_key(key, field);
        let old = self.transaction.get(&encoded)?;
        let base = old
            .as_deref()
            .map_or(Ok(0), parse_int_value)?
            .wrapping_add(step);
        let next = encode_int_value(base);
        if old.as_deref().unwrap_or_default() != next.as_slice() {
            self.transaction.set(encoded, next)?;
        }
        Ok(base)
    }

    /// Go `TxStructure.HGetInt64`: a missing field is zero.
    pub fn hget_int64(&mut self, key: &[u8], field: &[u8]) -> Result<i64, T::Error> {
        Ok(self.hget(key, field)?.map_or(Ok(0), |stored| parse_int_value(&stored))?)
    }

    /// Go `TxStructure.HDel`: only stored fields are deleted.
    pub fn hdel(&mut self, key: &[u8], fields: &[&[u8]]) -> Result<(), T::Error> {
        self.require_writable()?;
        for field in fields {
            let encoded = self.hash_data_key(key, field);
            if self.transaction.get(&encoded)?.is_some() {
                self.transaction.delete(&encoded)?;
            }
        }
        Ok(())
    }

    /// Go `TxStructure.HKeys`.
    pub fn hkeys(&mut self, key: &[u8]) -> Result<Vec<Vec<u8>>, T::Error> {
        let mut keys = Vec::new();
        self.iterate_hash(key, &mut |field, _| {
            keys.push(field.to_vec());
            Ok(())
        })?;
        Ok(keys)
    }

    /// Go `TxStructure.HGetAll`.
    pub fn hget_all(&mut self, key: &[u8]) -> Result<Vec<HashPair>, T::Error> {
        let mut pairs = Vec::new();
        self.iterate_hash(key, &mut |field, stored| {
            pairs.push(HashPair {
                field: field.to_vec(),
                value: stored.to_vec(),
            });
            Ok(())
        })?;
        Ok(pairs)
    }

    /// Go `TxStructure.HGetIter`.
    pub fn hget_iter(
        &mut self,
        key: &[u8],
        visit: &mut dyn FnMut(HashPair) -> Result<(), T::Error>,
    ) -> Result<(), T::Error> {
        self.iterate_hash(key, &mut |field, stored| {
            visit(HashPair {
                field: field.to_vec(),
                value: stored.to_vec(),
            })
        })
    }

    /// Go `TxStructure.HGetLen`.
    pub fn hget_len(&mut self, key: &[u8]) -> Result<u64, T::Error> {
        let mut length: u64 = 0;
        self.iterate_hash(key, &mut |_, _| {
            length += 1;
            Ok(())
        })?;
        Ok(length)
    }

    /// Go `TxStructure.HGetLastN`: the latest `num` pairs, newest first.
    pub fn hget_last_n(&mut self, key: &[u8], num: usize) -> Result<Vec<HashPair>, T::Error> {
        let mut pairs = Vec::with_capacity(num);
        let data_prefix = self.hash_data_key_prefix(key);
        let mut iterator = self.transaction.reverse_scan_prefix(&data_prefix, None)?;
        while iterator.valid() {
            let (_, field) = decode_hash_data_key_with(&self.prefix, iterator.key())?;
            pairs.push(HashPair {
                field,
                value: iterator.value().to_vec(),
            });
            if pairs.len() >= num {
                break;
            }
            iterator.next()?;
        }
        Ok(pairs)
    }

    /// Go `TxStructure.HClear`: collects the stored fields, then deletes them.
    pub fn hclear(&mut self, key: &[u8]) -> Result<(), T::Error> {
        let mut keys = Vec::new();
        let data_prefix = self.hash_data_key_prefix(key);
        self.iterate_hash(key, &mut |field, _| {
            let mut encoded = data_prefix.clone();
            encode_bytes(&mut encoded, field);
            keys.push(encoded);
            Ok(())
        })?;
        // Go has no snapshot guard here and nil-panics when the first key is
        // deleted through its absent readWriter. Preserve that impossible
        // shape rather than turning it into a Rust-only recoverable error.
        if !keys.is_empty() {
            self.require_writable()
                .expect("TxStructure.HClear requires a writable readWriter");
        }
        for encoded in keys {
            self.transaction.delete(&encoded)?;
        }
        Ok(())
    }

    /// Go `TxStructure.IterateHash`: every field of one hash, in field order.
    pub fn iterate_hash(&mut self, key: &[u8], visit: &mut RawRangeVisitor<'_, T::Error>) -> Result<(), T::Error> {
        let data_prefix = self.hash_data_key_prefix(key);
        let end = prefix_next(&data_prefix);
        let prefix = self.prefix.clone();
        self.transaction
            .iterate_range(&data_prefix, &end, &mut |encoded, stored| {
                let (_, field) = decode_hash_data_key_with(&prefix, encoded)?;
                visit(&field, stored)
            })
    }

    /// Go `TxStructure.IterateHashWithBoundedKey`: every decodable hash entry
    /// with a hash key in `[start, end)`; undecodable keys are skipped.
    pub fn iterate_hash_with_bounded_key(
        &mut self,
        start: &[u8],
        end: &[u8],
        visit: &mut BoundedHashVisitor<'_, T::Error>,
    ) -> Result<(), T::Error> {
        let range_start = self.hash_data_key_prefix(start);
        let range_end = self.hash_data_key_prefix(end);
        let prefix = self.prefix.clone();
        self.transaction
            .iterate_range(&range_start, &range_end, &mut |encoded, stored| {
                match decode_hash_data_key_with(&prefix, encoded) {
                    Ok((key, field)) => visit(&key, &field, stored),
                    Err(_) => Ok(()),
                }
            })
    }

    /// Go `structure.NewHashReverseIter`.
    pub fn hash_reverse_iter(&mut self, key: &[u8]) -> Result<ReverseHashIterator<T::Error>, T::Error> {
        self.new_hash_reverse_iter(key, None)
    }

    /// Go `structure.NewHashReverseIterBeginWithField`.
    pub fn hash_reverse_iter_begin_with_field(
        &mut self,
        key: &[u8],
        field: &[u8],
    ) -> Result<ReverseHashIterator<T::Error>, T::Error> {
        self.new_hash_reverse_iter(key, Some(field))
    }

    fn new_hash_reverse_iter(
        &mut self,
        key: &[u8],
        field: Option<&[u8]>,
    ) -> Result<ReverseHashIterator<T::Error>, T::Error> {
        let data_prefix = self.hash_data_key_prefix(key);
        // Go starts at PrefixNext of either the whole hash prefix or the
        // encoded begin field; the raw contract expresses the same bound as
        // an inclusive upper key.
        let upper = match field {
            None | Some(&[]) => None,
            Some(field) => Some(self.hash_data_key(key, field)),
        };
        let iterator = self
            .transaction
            .reverse_scan_prefix(&data_prefix, upper.as_deref())?;
        Ok(ReverseHashIterator {
            iterator,
            prefix: self.prefix.clone(),
            field: Vec::new(),
        })
    }

    // List operations (Go `pkg/structure/list.go`).

    /// Go `TxStructure.LPush`.
    pub fn lpush(&mut self, key: &[u8], values: &[&[u8]]) -> Result<(), T::Error> {
        self.list_push(key, true, values)
    }

    /// Go `TxStructure.RPush`.
    pub fn rpush(&mut self, key: &[u8], values: &[&[u8]]) -> Result<(), T::Error> {
        self.list_push(key, false, values)
    }

    fn list_push(&mut self, key: &[u8], left: bool, values: &[&[u8]]) -> Result<(), T::Error> {
        self.require_writable()?;
        if values.is_empty() {
            return Ok(());
        }
        let meta_key = self.list_meta_key(key);
        let mut meta = self.load_list_meta(&meta_key)?;
        for value in values {
            let index = if left {
                meta.l_index -= 1;
                meta.l_index
            } else {
                let index = meta.r_index;
                meta.r_index += 1;
                index
            };
            self.transaction
                .set(self.list_data_key(key, index), value.to_vec())?;
        }
        self.transaction.set(meta_key, meta.value())
    }

    /// Go `TxStructure.LPop`: `None` when the list is empty.
    pub fn lpop(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, T::Error> {
        self.list_pop(key, true)
    }

    /// Go `TxStructure.RPop`: `None` when the list is empty.
    pub fn rpop(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, T::Error> {
        self.list_pop(key, false)
    }

    fn list_pop(&mut self, key: &[u8], left: bool) -> Result<Option<Vec<u8>>, T::Error> {
        self.require_writable()?;
        let meta_key = self.list_meta_key(key);
        let mut meta = self.load_list_meta(&meta_key)?;
        if meta.is_empty() {
            return Ok(None);
        }
        let index = if left {
            let index = meta.l_index;
            meta.l_index += 1;
            index
        } else {
            meta.r_index -= 1;
            meta.r_index
        };
        let data_key = self.list_data_key(key, index);
        // Go propagates kv.ErrNotExist here: the meta said this entry exists.
        let data = self
            .transaction
            .get(&data_key)?
            .ok_or(StructureError::KeyNotExist)?;
        self.transaction.delete(&data_key)?;
        if meta.is_empty() {
            self.transaction.delete(&meta_key)?;
        } else {
            self.transaction.set(meta_key, meta.value())?;
        }
        Ok(Some(data))
    }

    /// Go `TxStructure.LLen`.
    pub fn llen(&mut self, key: &[u8]) -> Result<i64, T::Error> {
        let meta_key = self.list_meta_key(key);
        let meta = self.load_list_meta(&meta_key)?;
        Ok(meta.r_index - meta.l_index)
    }

    /// Go `TxStructure.LGetAll`: every element, in order from right to left.
    pub fn lget_all(&mut self, key: &[u8]) -> Result<Vec<Vec<u8>>, T::Error> {
        let meta_key = self.list_meta_key(key);
        let meta = self.load_list_meta(&meta_key)?;
        if meta.is_empty() {
            return Ok(Vec::new());
        }
        #[expect(clippy::cast_sign_loss, reason = "a non-empty meta has r > l")]
        let mut elements = Vec::with_capacity((meta.r_index - meta.l_index) as usize);
        let mut index = meta.r_index - 1;
        while index >= meta.l_index {
            let element = self
                .transaction
                .get(&self.list_data_key(key, index))?
                .ok_or(StructureError::KeyNotExist)?;
            elements.push(element);
            index -= 1;
        }
        Ok(elements)
    }

    /// Go `TxStructure.LIndex`: `None` when the adjusted index is out of
    /// range; negative indexes count from the right.
    pub fn lindex(&mut self, key: &[u8], index: i64) -> Result<Option<Vec<u8>>, T::Error> {
        let meta_key = self.list_meta_key(key);
        let meta = self.load_list_meta(&meta_key)?;
        if meta.is_empty() {
            return Ok(None);
        }
        let index = adjust_index(index, meta.l_index, meta.r_index);
        if index >= meta.l_index && index < meta.r_index {
            Ok(Some(
                self.transaction
                    .get(&self.list_data_key(key, index))?
                    .ok_or(StructureError::KeyNotExist)?,
            ))
        } else {
            Ok(None)
        }
    }

    /// Go `TxStructure.LSet`: an out-of-range adjusted index is an error.
    pub fn lset(&mut self, key: &[u8], index: i64, value: &[u8]) -> Result<(), T::Error> {
        self.require_writable()?;
        let meta_key = self.list_meta_key(key);
        let meta = self.load_list_meta(&meta_key)?;
        if meta.is_empty() {
            return Ok(());
        }
        let index = adjust_index(index, meta.l_index, meta.r_index);
        if index >= meta.l_index && index < meta.r_index {
            self.transaction
                .set(self.list_data_key(key, index), value.to_vec())
        } else {
            Err(StructureError::InvalidListIndex(index).into())
        }
    }

    /// Go `TxStructure.LClear`.
    pub fn lclear(&mut self, key: &[u8]) -> Result<(), T::Error> {
        self.require_writable()?;
        let meta_key = self.list_meta_key(key);
        let meta = self.load_list_meta(&meta_key)?;
        if meta.is_empty() {
            return Ok(());
        }
        for index in meta.l_index..meta.r_index {
            self.transaction.delete(&self.list_data_key(key, index))?;
        }
        self.transaction.delete(&meta_key)
    }

    /// Go `TxStructure.loadListMeta`: a missing meta is empty, a stored meta
    /// that is not sixteen bytes is `ErrInvalidListMetaData`.
    fn load_list_meta(&mut self, meta_key: &[u8]) -> Result<ListMeta, T::Error> {
        let Some(stored) = self.transaction.get(meta_key)? else {
            return Ok(ListMeta::default());
        };
        let Some((l_bytes, r_bytes)) = stored
            .split_first_chunk::<8>()
            .and_then(|(l, rest)| Some((l, <&[u8; 8]>::try_from(rest).ok()?)))
        else {
            return Err(StructureError::InvalidListMetaData.into());
        };
        Ok(ListMeta {
            l_index: i64::from_be_bytes(*l_bytes),
            r_index: i64::from_be_bytes(*r_bytes),
        })
    }
}

/// Go `structure.ReverseHashIterator`: latest-first iteration over one hash.
///
/// Mirroring Go, construction does not decode the first entry: `key()` is
/// empty until the first `next()`.
pub struct ReverseHashIterator<E> {
    iterator: Box<dyn RawKvIterator<Error = E>>,
    prefix: Vec<u8>,
    field: Vec<u8>,
}

impl<E: From<StructureError>> ReverseHashIterator<E> {
    /// Go `ReverseHashIterator.Valid`.
    pub fn valid(&self) -> bool {
        self.iterator.valid()
    }

    /// Go `ReverseHashIterator.Key`: the decoded field of the entry `next`
    /// last landed on.
    pub fn key(&self) -> &[u8] {
        &self.field
    }

    /// Go `ReverseHashIterator.Value`.
    pub fn value(&self) -> &[u8] {
        self.iterator.value()
    }

    /// Go `ReverseHashIterator.Next`.
    #[expect(
        clippy::should_implement_trait,
        reason = "Go's Next is fallible mid-iteration; Iterator::next cannot be"
    )]
    pub fn next(&mut self) -> Result<(), E> {
        self.iterator.next()?;
        if self.iterator.valid() {
            let (_, field) = decode_hash_data_key_with(&self.prefix, self.iterator.key())?;
            self.field = field;
        }
        Ok(())
    }
}
