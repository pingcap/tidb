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

//! Go `pkg/lightning/verification` lands as a complete package: the key-value
//! checksum that lightning and the physical-import path compute over ingested data
//! (`checksum.go`), with all four of the package's test functions.
//!
//! A [`KvChecksum`] accumulates three quantities over a stream of key-value
//! pairs: the total byte count (including one keyspace prefix per pair), the
//! pair count, and the XOR of each pair's CRC-64/ECMA over
//! `keyspace || key || value`. Because the combining operation is XOR, two
//! checksums merge with [`KvChecksum::add`] and unmerge with
//! [`KvChecksum::sub`] regardless of the order the pairs arrived in — which is
//! what lets a distributed import verify itself. [`KvGroupChecksum`] keeps one
//! such checksum per KV group: the data (record) rows under
//! [`DATA_KV_GROUP_ID`], and one group per index ID.
//!
//! This module is deliberately *not* placed under `crate::checksum`, which is
//! the unrelated Go package `pkg/util/checksum` — CRC-32 block framing for
//! positional spill-file reads. The two share only the word "checksum".
//!
//! # Narrowings and boundaries
//!
//! - **`pkg/lightning/common.KvPair`** is inlined as [`KvPair`]. Go's struct
//!   carries a third field, `RowID`, which exists only to disambiguate pairs
//!   equal in key and value; this package never reads it.
//! - **CRC-64/ECMA.** Go uses `hash/crc64` with `crc64.MakeTable(crc64.ECMA)`.
//!   `tidb-exec`'s `slow_log_parse` already carries a verified bit-at-a-time
//!   implementation of the same polynomial, but `tidb-util` sits below
//!   `tidb-exec` and cannot depend on it, so [`crc64_ecma_update`] mirrors that
//!   approach here. It is `crc64.Update`, the resumable form, since
//!   `UpdateOne` continues a keyspace-seeded CRC across the key and the value;
//!   `crc64.Checksum(data)` is `crc64_ecma_update(0, data)`.
//! - **`zapcore.ObjectMarshaler`.** Go's `MarshalLogObject` exists so a
//!   checksum can be logged as a nested object. Both implementations become
//!   [`KvChecksum::log_value`] and [`KvGroupChecksum::log_value`], which build
//!   the same nested `tidb_log::Value::Object`.
//! - **Map iteration order.** Go ranges over `map[int64]*KVChecksum`, whose
//!   order is randomized; the merged results (`Add`, `MergedChecksum`, the
//!   sums) are order-independent, and `MarshalLogObject`'s field order is not.
//!   A `BTreeMap` here makes the log output deterministic in index-ID order.

use std::collections::BTreeMap;

use tidb_log::{Field, Value};

/// Go `common.KvPair`, narrowed to the two fields this package reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KvPair {
    /// Go `KvPair.Key`.
    pub key: Vec<u8>,
    /// Go `KvPair.Val`.
    pub val: Vec<u8>,
}

impl KvPair {
    /// One pair from borrowed bytes.
    #[must_use]
    pub fn new(key: &[u8], val: &[u8]) -> Self {
        Self {
            key: key.to_vec(),
            val: val.to_vec(),
        }
    }
}

/// Go `crc64.ECMA` (the ECMA-182 reflected polynomial).
const CRC64_ECMA: u64 = 0xC96C_5795_D787_0F42;

/// Go `crc64.Update(crc, ecmaTable, data)`. Passing `0` gives
/// `crc64.Checksum(data, ecmaTable)`.
#[must_use]
pub fn crc64_ecma_update(crc: u64, data: &[u8]) -> u64 {
    let mut crc = !crc;
    for &byte in data {
        crc ^= u64::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ CRC64_ECMA
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// Go `KVChecksum`: the checksum of a collection of key-value pairs. The
/// default value is the checksum of empty content and zero keyspace.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KvChecksum {
    base: u64,
    prefix_len: usize,
    bytes: u64,
    kvs: u64,
    checksum: u64,
}

impl KvChecksum {
    /// Go `NewKVChecksum`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `NewKVChecksumWithKeyspace`.
    #[must_use]
    pub fn with_keyspace(keyspace: &[u8]) -> Self {
        Self {
            base: crc64_ecma_update(0, keyspace),
            prefix_len: keyspace.len(),
            ..Self::default()
        }
    }

    /// Go `MakeKVChecksum`.
    #[must_use]
    pub fn make(bytes: u64, kvs: u64, checksum: u64) -> Self {
        Self {
            bytes,
            kvs,
            checksum,
            ..Self::default()
        }
    }

    /// Go `MakeKVChecksumWithKeyspace`.
    #[must_use]
    pub fn make_with_keyspace(keyspace: &[u8], bytes: u64, kvs: u64, checksum: u64) -> Self {
        Self {
            base: crc64_ecma_update(0, keyspace),
            prefix_len: keyspace.len(),
            bytes,
            kvs,
            checksum,
        }
    }

    /// Go `KVChecksum.UpdateOne`.
    pub fn update_one(&mut self, kv: &KvPair) {
        let mut sum = crc64_ecma_update(self.base, &kv.key);
        sum = crc64_ecma_update(sum, &kv.val);

        self.bytes = self
            .bytes
            .wrapping_add((self.prefix_len + kv.key.len() + kv.val.len()) as u64);
        self.kvs += 1;
        self.checksum ^= sum;
    }

    /// Go `KVChecksum.Update`.
    pub fn update(&mut self, kvs: &[KvPair]) {
        let mut checksum = 0_u64;
        let mut kv_num = 0_u64;
        let mut bytes = 0_usize;

        for pair in kvs {
            let mut sum = crc64_ecma_update(self.base, &pair.key);
            sum = crc64_ecma_update(sum, &pair.val);
            checksum ^= sum;
            kv_num += 1;
            bytes += self.prefix_len;
            bytes += pair.key.len() + pair.val.len();
        }

        self.bytes = self.bytes.wrapping_add(bytes as u64);
        self.kvs = self.kvs.wrapping_add(kv_num);
        self.checksum ^= checksum;
    }

    /// Go `KVChecksum.Add`.
    pub fn add(&mut self, other: &Self) {
        self.bytes = self.bytes.wrapping_add(other.bytes);
        self.kvs = self.kvs.wrapping_add(other.kvs);
        self.checksum ^= other.checksum;
    }

    /// Go `KVChecksum.Sub`.
    pub fn sub(&mut self, other: &Self) {
        self.bytes = self.bytes.wrapping_sub(other.bytes);
        self.kvs = self.kvs.wrapping_sub(other.kvs);
        self.checksum ^= other.checksum;
    }

    /// Go `KVChecksum.Sum`.
    #[must_use]
    pub const fn sum(&self) -> u64 {
        self.checksum
    }

    /// Go `KVChecksum.SumSize`: the total size of the key-value pairs.
    #[must_use]
    pub const fn sum_size(&self) -> u64 {
        self.bytes
    }

    /// Go `KVChecksum.SumKVS`: the total number of key-value pairs.
    #[must_use]
    pub const fn sum_kvs(&self) -> u64 {
        self.kvs
    }

    /// Go `KVChecksum.MarshalLogObject`.
    #[must_use]
    pub fn log_value(&self) -> Value {
        Value::Object(vec![
            Field::new("cksum", Value::U64(self.checksum)),
            Field::new("size", Value::U64(self.bytes)),
            Field::new("kvs", Value::U64(self.kvs)),
        ])
    }
}

impl serde::Serialize for KvChecksum {
    /// Go `KVChecksum.MarshalJSON`, which hand-formats
    /// `{"checksum":N,"size":N,"kvs":N}` in exactly this field order.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct as _;
        let mut state = serializer.serialize_struct("KVChecksum", 3)?;
        state.serialize_field("checksum", &self.checksum)?;
        state.serialize_field("size", &self.bytes)?;
        state.serialize_field("kvs", &self.kvs)?;
        state.end()
    }
}

impl std::fmt::Display for KvChecksum {
    /// Go `KVChecksum.String`, which is its JSON form.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            r#"{{"checksum":{},"size":{},"kvs":{}}}"#,
            self.checksum, self.bytes, self.kvs
        )
    }
}

/// Go `DataKVGroupID`: the ID for the data KV group. Index IDs start from 1,
/// so -1 stands for the data rows.
pub const DATA_KV_GROUP_ID: i64 = -1;

/// Go `KVGroupChecksum`: one [`KvChecksum`] per data or index KV group.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KvGroupChecksum {
    m: BTreeMap<i64, KvChecksum>,
    keyspace: Vec<u8>,
}

impl KvGroupChecksum {
    /// Go `NewKVGroupChecksumWithKeyspace`.
    #[must_use]
    pub fn with_keyspace(keyspace: &[u8]) -> Self {
        let mut m = BTreeMap::new();
        m.insert(DATA_KV_GROUP_ID, KvChecksum::with_keyspace(keyspace));
        Self {
            m,
            keyspace: keyspace.to_vec(),
        }
    }

    /// Go `NewKVGroupChecksumForAdd`. It cannot be used with
    /// [`Self::update_one_data_kv`] or [`Self::update_one_index_kv`].
    #[must_use]
    pub fn for_add() -> Self {
        let mut m = BTreeMap::new();
        m.insert(DATA_KV_GROUP_ID, KvChecksum::new());
        Self {
            m,
            keyspace: Vec::new(),
        }
    }

    /// Go `KVGroupChecksum.UpdateOneDataKV`. It does not re-check that the
    /// pair's key is a real data key.
    pub fn update_one_data_kv(&mut self, kv: &KvPair) {
        self.m
            .get_mut(&DATA_KV_GROUP_ID)
            .expect("the data KV group is created by the constructors")
            .update_one(kv);
    }

    /// Go `KVGroupChecksum.UpdateOneIndexKV`. It does not re-check that the
    /// pair's key is a real index key of that index ID.
    pub fn update_one_index_kv(&mut self, index_id: i64, kv: &KvPair) {
        self.get_or_create_one_group(index_id).update_one(kv);
    }

    /// Go `KVGroupChecksum.Add`.
    pub fn add(&mut self, other: &Self) {
        for (id, checksum) in &other.m {
            let checksum = checksum.clone();
            self.get_or_create_one_group(*id).add(&checksum);
        }
    }

    /// Go `KVGroupChecksum.getOrCreateOneGroup`.
    fn get_or_create_one_group(&mut self, id: i64) -> &mut KvChecksum {
        self.m
            .entry(id)
            .or_insert_with(|| KvChecksum::with_keyspace(&self.keyspace))
    }

    /// Go `KVGroupChecksum.AddRawGroup`.
    pub fn add_raw_group(&mut self, id: i64, bytes: u64, kvs: u64, checksum: u64) {
        let tmp = KvChecksum::make(bytes, kvs, checksum);
        self.get_or_create_one_group(id).add(&tmp);
    }

    /// Go `KVGroupChecksum.DataAndIndexSumSize`.
    #[must_use]
    pub fn data_and_index_sum_size(&self) -> (u64, u64) {
        let mut data_size = 0;
        let mut index_size = 0;
        for (id, checksum) in &self.m {
            if *id == DATA_KV_GROUP_ID {
                data_size = checksum.sum_size();
            } else {
                index_size += checksum.sum_size();
            }
        }
        (data_size, index_size)
    }

    /// Go `KVGroupChecksum.DataAndIndexSumKVS`.
    #[must_use]
    pub fn data_and_index_sum_kvs(&self) -> (u64, u64) {
        let mut data_kvs = 0;
        let mut index_kvs = 0;
        for (id, checksum) in &self.m {
            if *id == DATA_KV_GROUP_ID {
                data_kvs = checksum.sum_kvs();
            } else {
                index_kvs += checksum.sum_kvs();
            }
        }
        (data_kvs, index_kvs)
    }

    /// Go `KVGroupChecksum.GetInnerChecksums`: a cloned map of index ID to its
    /// checksum.
    #[must_use]
    pub fn inner_checksums(&self) -> BTreeMap<i64, KvChecksum> {
        self.m.clone()
    }

    /// Go `KVGroupChecksum.MergedChecksum`: all groups merged into one.
    #[must_use]
    pub fn merged_checksum(&self) -> KvChecksum {
        let mut merged = KvChecksum::new();
        for checksum in self.m.values() {
            merged.add(checksum);
        }
        merged
    }

    /// Go `KVGroupChecksum.MarshalLogObject`.
    #[must_use]
    pub fn log_value(&self) -> Value {
        Value::Object(
            self.m
                .iter()
                .map(|(id, checksum)| Field::new(format!("id={id}"), checksum.log_value()))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go `TestChecksum`.
    #[test]
    fn checksum() {
        let mut checksum = KvChecksum::new();
        assert_eq!(checksum.sum(), 0);

        // checksum on nothing
        checksum.update(&[]);
        assert_eq!(checksum.sum(), 0);

        // checksum on real data
        let expect_checksum = 4_850_203_904_608_948_940_u64;

        let kvs = [
            KvPair::new(b"Cop", b"PingCAP"),
            KvPair::new(
                b"Introduction",
                b"Inspired by Google Spanner/F1, PingCAP develops TiDB.",
            ),
        ];

        checksum.update(&kvs);

        let kv_bytes: u64 = kvs
            .iter()
            .map(|kv| (kv.key.len() + kv.val.len()) as u64)
            .sum();
        assert_eq!(checksum.sum_size(), kv_bytes);
        assert_eq!(checksum.sum_kvs(), kvs.len() as u64);
        assert_eq!(checksum.sum(), expect_checksum);

        // recompute on same key-value
        checksum.update(&kvs);
        assert_eq!(checksum.sum_size(), kv_bytes << 1);
        assert_eq!(checksum.sum_kvs(), (kvs.len() as u64) << 1);
        assert_ne!(checksum.sum(), expect_checksum);
    }

    // Go `TestChecksumJSON`.
    #[test]
    fn checksum_json() {
        #[derive(serde::Serialize)]
        struct TestStruct {
            #[serde(rename = "Checksum")]
            checksum: KvChecksum,
        }

        let test_struct = TestStruct {
            checksum: KvChecksum::make(123, 456, 7890),
        };

        let res = serde_json::to_vec(&test_struct).unwrap();
        assert_eq!(
            res,
            br#"{"Checksum":{"checksum":7890,"size":123,"kvs":456}}"#
        );
        // Go `KVChecksum.String` is the same JSON.
        assert_eq!(
            test_struct.checksum.to_string(),
            r#"{"checksum":7890,"size":123,"kvs":456}"#
        );
    }

    // Go `TestGroupChecksum`.
    #[test]
    fn group_checksum() {
        let kv_pair = KvPair::new(b"key", b"val");
        let kv_pair2 = KvPair::new(b"key2", b"val2");

        let mut c = KvGroupChecksum::with_keyspace(b"");
        c.update_one_data_kv(&kv_pair);
        c.update_one_index_kv(1, &kv_pair2);
        let inner = c.inner_checksums();
        assert_eq!(inner.len(), 2);
        assert_eq!(inner[&1].sum_kvs(), 1);
        assert_eq!(inner[&DATA_KV_GROUP_ID].sum_kvs(), 1);

        let mut keyspace_c = KvGroupChecksum::with_keyspace(b"keyspace");
        keyspace_c.update_one_data_kv(&kv_pair);
        keyspace_c.update_one_index_kv(1, &kv_pair2);
        let keyspace_inner = keyspace_c.inner_checksums();
        assert_ne!(inner, keyspace_inner);

        // Go passes a nil keyspace here, which is the empty keyspace.
        let mut c2 = KvGroupChecksum::with_keyspace(&[]);
        c2.update_one_index_kv(1, &kv_pair);
        c2.update_one_index_kv(2, &kv_pair2);
        c.add(&c2);
        let inner = c.inner_checksums();
        assert_eq!(inner.len(), 3);
        assert_eq!(inner[&1].sum_kvs(), 2);
        assert_eq!(inner[&2].sum_kvs(), 1);
        assert_eq!(inner[&DATA_KV_GROUP_ID].sum_kvs(), 1);

        let (data_kv_cnt, index_kv_cnt) = c.data_and_index_sum_kvs();
        assert_eq!(data_kv_cnt, 1);
        assert_eq!(index_kv_cnt, 3);

        let (data_size, index_size) = c.data_and_index_sum_size();
        assert_eq!(data_size, 6);
        assert_eq!(index_size, 22);

        let merged = c.merged_checksum();
        assert_eq!(merged.sum_kvs(), 4);
        assert_eq!(merged.sum_size(), 28);
    }

    // Go `TestKVChecksumOperation`.
    #[test]
    fn kv_checksum_operation() {
        let mut csum = KvChecksum::new();
        let mut other = KvChecksum::make(100, 100, 100);
        csum.add(&other);
        assert_eq!(csum.sum(), 100);
        assert_eq!(csum.sum_size(), 100);
        assert_eq!(csum.sum_kvs(), 100);
        other = KvChecksum::make(10, 20, 30);
        csum.sub(&other);
        assert_eq!(csum.sum(), 100 ^ 30);
        assert_eq!(csum.sum_size(), 90);
        assert_eq!(csum.sum_kvs(), 80);
    }

    // The CRC-64/ECMA implementation against Go's own check value:
    // `crc64.Checksum([]byte("123456789"), crc64.MakeTable(crc64.ECMA))`, and
    // its resumability, which `UpdateOne` depends on.
    #[test]
    fn crc64_matches_the_ecma_check_value() {
        assert_eq!(crc64_ecma_update(0, b"123456789"), 0x995D_C9BB_DF19_39FA);
        assert_eq!(crc64_ecma_update(0, b""), 0);
        assert_eq!(
            crc64_ecma_update(crc64_ecma_update(0, b"1234"), b"56789"),
            crc64_ecma_update(0, b"123456789")
        );
    }

    // A keyspace-seeded checksum counts one prefix per pair and differs from
    // the unseeded one, and `update_one` agrees with the batch `update`.
    #[test]
    fn keyspace_seeds_the_checksum_and_the_size() {
        let kvs = [KvPair::new(b"k1", b"v1"), KvPair::new(b"k2", b"v2")];

        let mut batch = KvChecksum::with_keyspace(b"ks");
        batch.update(&kvs);

        let mut one_by_one = KvChecksum::with_keyspace(b"ks");
        for kv in &kvs {
            one_by_one.update_one(kv);
        }
        assert_eq!(batch, one_by_one);

        // Two pairs of two-byte keys and values, plus the two-byte keyspace
        // prefix counted once per pair.
        assert_eq!(batch.sum_size(), 2 * (2 + 2 + 2));
        assert_eq!(batch.sum_kvs(), 2);

        // The keyspace seeds the CRC, so a single pair checksums differently
        // with and without it. (Over an even number of equal-length messages
        // the seed's contribution cancels under XOR, which is why this
        // compares one pair.)
        let mut seeded_one = KvChecksum::with_keyspace(b"ks");
        seeded_one.update_one(&kvs[0]);
        let mut unseeded_one = KvChecksum::new();
        unseeded_one.update_one(&kvs[0]);
        assert_ne!(seeded_one.sum(), unseeded_one.sum());

        // `MakeKVChecksumWithKeyspace` reconstructs the same seeded state.
        let reconstructed =
            KvChecksum::make_with_keyspace(b"ks", batch.sum_size(), batch.sum_kvs(), batch.sum());
        assert_eq!(reconstructed, batch);
    }

    // `AddRawGroup` and `for_add` build a group checksum from raw numbers, and
    // the log objects nest the way the zap marshalers do.
    #[test]
    fn raw_groups_and_log_objects() {
        let mut group = KvGroupChecksum::for_add();
        group.add_raw_group(DATA_KV_GROUP_ID, 10, 2, 0xAA);
        group.add_raw_group(3, 5, 1, 0x0F);

        let (data_size, index_size) = group.data_and_index_sum_size();
        assert_eq!((data_size, index_size), (10, 5));
        assert_eq!(group.merged_checksum().sum(), 0xAA ^ 0x0F);

        let Value::Object(fields) = group.log_value() else {
            panic!("a group logs as an object");
        };
        // BTreeMap order: the data group (-1) precedes index 3.
        assert_eq!(fields[0].key, "id=-1");
        assert_eq!(fields[1].key, "id=3");
        let Value::Object(inner) = &fields[1].value else {
            panic!("each group logs as an object");
        };
        assert_eq!(
            inner
                .iter()
                .map(|field| field.key.as_str())
                .collect::<Vec<_>>(),
            ["cksum", "size", "kvs"]
        );
    }
}
