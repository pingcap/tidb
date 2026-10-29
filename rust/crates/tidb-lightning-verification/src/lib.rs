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

//! Key-value checksums from Go `pkg/lightning/verification`.

use std::collections::HashMap;

use tidb_log::{Field, Value};

/// Native representation of the Go `common.KvPair` dependency.
pub struct KvPair {
    /// Go `KvPair.Key`.
    pub key: Vec<u8>,
    /// Go `KvPair.Val`.
    pub val: Vec<u8>,
    /// Go `KvPair.RowID`.
    pub row_id: Vec<u8>,
}

/// Go `crc64.ECMA` (the ECMA-182 reflected polynomial).
const CRC64_ECMA: u64 = 0xC96C_5795_D787_0F42;

/// Go `crc64.Update(crc, ecmaTable, data)`. Passing `0` gives
/// `crc64.Checksum(data, ecmaTable)`.
fn crc64_ecma_update(crc: u64, data: &[u8]) -> u64 {
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
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct KvChecksum {
    base: u64,
    prefix_len: isize,
    bytes: u64,
    kvs: u64,
    checksum: u64,
}

impl KvChecksum {
    /// Go `NewKVChecksum`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `NewKVChecksumWithKeyspace`.
    pub fn with_keyspace(keyspace: &[u8]) -> Self {
        Self {
            base: crc64_ecma_update(0, keyspace),
            prefix_len: keyspace.len() as isize,
            ..Self::default()
        }
    }

    /// Go `MakeKVChecksum`.
    pub fn make(bytes: u64, kvs: u64, checksum: u64) -> Self {
        Self {
            bytes,
            kvs,
            checksum,
            ..Self::default()
        }
    }

    /// Go `MakeKVChecksumWithKeyspace`.
    pub fn make_with_keyspace(keyspace: &[u8], bytes: u64, kvs: u64, checksum: u64) -> Self {
        Self {
            base: crc64_ecma_update(0, keyspace),
            prefix_len: keyspace.len() as isize,
            bytes,
            kvs,
            checksum,
        }
    }

    /// Go `KVChecksum.UpdateOne`.
    pub fn update_one(&mut self, kv: &KvPair) {
        let mut sum = crc64_ecma_update(self.base, &kv.key);
        sum = crc64_ecma_update(sum, &kv.val);

        let bytes = self
            .prefix_len
            .wrapping_add(kv.key.len() as isize)
            .wrapping_add(kv.val.len() as isize);
        self.bytes = self.bytes.wrapping_add(bytes as u64);
        self.kvs = self.kvs.wrapping_add(1);
        self.checksum ^= sum;
    }

    /// Go `KVChecksum.Update`.
    pub fn update(&mut self, kvs: &[KvPair]) {
        let mut checksum = 0_u64;
        let mut kv_num = 0_isize;
        let mut bytes = 0_isize;

        for pair in kvs {
            let mut sum = crc64_ecma_update(self.base, &pair.key);
            sum = crc64_ecma_update(sum, &pair.val);
            checksum ^= sum;
            kv_num = kv_num.wrapping_add(1);
            bytes = bytes.wrapping_add(self.prefix_len);
            bytes = bytes
                .wrapping_add(pair.key.len() as isize)
                .wrapping_add(pair.val.len() as isize);
        }

        self.bytes = self.bytes.wrapping_add(bytes as u64);
        self.kvs = self.kvs.wrapping_add(kv_num as u64);
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
    pub fn sum(&self) -> u64 {
        self.checksum
    }

    /// Go `KVChecksum.SumSize`: the total size of the key-value pairs.
    pub fn sum_size(&self) -> u64 {
        self.bytes
    }

    /// Go `KVChecksum.SumKVS`: the total number of key-value pairs.
    pub fn sum_kvs(&self) -> u64 {
        self.kvs
    }

    /// Go `KVChecksum.MarshalLogObject`.
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
pub struct KvGroupChecksum<'a> {
    m: HashMap<i64, KvChecksum>,
    keyspace: &'a [u8],
}

impl<'a> KvGroupChecksum<'a> {
    /// Go `NewKVGroupChecksumWithKeyspace`.
    pub fn with_keyspace(keyspace: &'a [u8]) -> Self {
        let mut m = HashMap::with_capacity(8);
        m.insert(DATA_KV_GROUP_ID, KvChecksum::with_keyspace(keyspace));
        Self { m, keyspace }
    }

    /// Go `NewKVGroupChecksumForAdd`. It cannot be used with
    /// [`Self::update_one_data_kv`] or [`Self::update_one_index_kv`].
    pub fn for_add() -> Self {
        let mut m = HashMap::with_capacity(8);
        m.insert(DATA_KV_GROUP_ID, KvChecksum::new());
        Self { m, keyspace: &[] }
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
            let checksum = *checksum;
            self.get_or_create_one_group(*id).add(&checksum);
        }
    }

    /// Go `KVGroupChecksum.getOrCreateOneGroup`.
    fn get_or_create_one_group(&mut self, id: i64) -> &mut KvChecksum {
        self.m
            .entry(id)
            .or_insert_with(|| KvChecksum::with_keyspace(self.keyspace))
    }

    /// Go `KVGroupChecksum.AddRawGroup`.
    pub fn add_raw_group(&mut self, id: i64, bytes: u64, kvs: u64, checksum: u64) {
        let tmp = KvChecksum::make(bytes, kvs, checksum);
        self.get_or_create_one_group(id).add(&tmp);
    }

    /// Go `KVGroupChecksum.DataAndIndexSumSize`.
    pub fn data_and_index_sum_size(&self) -> (u64, u64) {
        let mut data_size = 0_u64;
        let mut index_size = 0_u64;
        for (id, checksum) in &self.m {
            if *id == DATA_KV_GROUP_ID {
                data_size = checksum.sum_size();
            } else {
                index_size = index_size.wrapping_add(checksum.sum_size());
            }
        }
        (data_size, index_size)
    }

    /// Go `KVGroupChecksum.DataAndIndexSumKVS`.
    pub fn data_and_index_sum_kvs(&self) -> (u64, u64) {
        let mut data_kvs = 0_u64;
        let mut index_kvs = 0_u64;
        for (id, checksum) in &self.m {
            if *id == DATA_KV_GROUP_ID {
                data_kvs = checksum.sum_kvs();
            } else {
                index_kvs = index_kvs.wrapping_add(checksum.sum_kvs());
            }
        }
        (data_kvs, index_kvs)
    }

    /// Go `KVGroupChecksum.GetInnerChecksums`: a cloned map of index ID to its
    /// checksum.
    pub fn inner_checksums(&self) -> HashMap<i64, KvChecksum> {
        self.m.clone()
    }

    /// Go `KVGroupChecksum.MergedChecksum`: all groups merged into one.
    pub fn merged_checksum(&self) -> KvChecksum {
        let mut merged = KvChecksum::new();
        for checksum in self.m.values() {
            merged.add(checksum);
        }
        merged
    }

    /// Go `KVGroupChecksum.MarshalLogObject`.
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

        // Go passes a nil slice here; it has the same empty contents.
        checksum.update(&[]);
        assert_eq!(checksum.sum(), 0);

        // checksum on real data
        let expect_checksum = 4_850_203_904_608_948_940_u64;

        let kvs = [
            KvPair {
                key: b"Cop".to_vec(),
                val: b"PingCAP".to_vec(),
                row_id: Vec::new(),
            },
            KvPair {
                key: b"Introduction".to_vec(),
                val: b"Inspired by Google Spanner/F1, PingCAP develops TiDB.".to_vec(),
                row_id: Vec::new(),
            },
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
    }

    // Go `TestGroupChecksum`.
    #[test]
    fn group_checksum() {
        let kv_pair = KvPair {
            key: b"key".to_vec(),
            val: b"val".to_vec(),
            row_id: Vec::new(),
        };
        let kv_pair2 = KvPair {
            key: b"key2".to_vec(),
            val: b"val2".to_vec(),
            row_id: Vec::new(),
        };

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
        assert!(inner != keyspace_inner);

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
}
