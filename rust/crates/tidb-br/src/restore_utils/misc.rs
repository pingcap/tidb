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

//! Go `br/pkg/restore/utils/misc.go`: the ID maps between a backed-up table and
//! its restored counterpart, plus two small key transforms.

use std::collections::BTreeMap;

use tidb_model::TableInfo;

/// Go `WriteCFName`.
pub const WRITE_CF_NAME: &str = "write";
/// Go `DefaultCFName`.
pub const DEFAULT_CF_NAME: &str = "default";

/// Go `GetPartitionIDMap`: maps old physical ID to new physical ID.
#[must_use]
pub fn get_partition_id_map(new_table: &TableInfo, old_table: &TableInfo) -> BTreeMap<i64, i64> {
    let mut table_id_map = BTreeMap::new();

    if let (Some(old_partition), Some(new_partition)) =
        (old_table.partition.as_ref(), new_table.partition.as_ref())
    {
        let mut name_map_id: BTreeMap<String, i64> = BTreeMap::new();

        old_partition
            .read()
            .definitions
            .with_visible(|definitions| {
                for old in definitions {
                    name_map_id.insert(old.name.lowercase().to_owned(), old.id);
                }
            });
        new_partition
            .read()
            .definitions
            .with_visible(|definitions| {
                for new in definitions {
                    if let Some(old_id) = name_map_id.get(new.name.lowercase()) {
                        table_id_map.insert(*old_id, new.id);
                    }
                }
            });
    }

    table_id_map
}

/// Go `GetTableIDMap`: maps old table ID to new table ID, partitions included.
#[must_use]
pub fn get_table_id_map(new_table: &TableInfo, old_table: &TableInfo) -> BTreeMap<i64, i64> {
    let mut table_id_map = get_partition_id_map(new_table, old_table);
    table_id_map.insert(old_table.id, new_table.id);
    table_id_map
}

/// Go `GetIndexIDMap`: maps old index ID to new index ID by index name.
#[must_use]
pub fn get_index_id_map(new_table: &TableInfo, old_table: &TableInfo) -> BTreeMap<i64, i64> {
    let mut index_id_map = BTreeMap::new();
    for src_index in old_table.indices.iter_deref() {
        for dest_index in new_table.indices.iter_deref() {
            let (src, dest) = (src_index.read(), dest_index.read());
            if src.name == dest.name {
                index_id_map.insert(src.id, dest.id);
            }
        }
    }
    index_id_map
}

/// Go `TruncateTS`: drops the trailing eight-byte commit timestamp.
#[must_use]
pub fn truncate_ts(key: &[u8]) -> &[u8] {
    if key.is_empty() {
        return &[];
    }
    if key.len() < 8 {
        return key;
    }
    &key[..key.len() - 8]
}

/// Go `EncodeKeyPrefix`: mem-comparable-encodes the whole eight-byte groups of
/// `key` and appends the ungrouped tail raw, so the result stays a valid
/// *prefix* of the encoding of any key starting with `key`.
#[must_use]
pub fn encode_key_prefix(key: &[u8]) -> Vec<u8> {
    let mut encoded_prefix = Vec::new();
    let ungrouped_len = key.len() % 8;
    tidb_codec::encode_bytes(&mut encoded_prefix, &key[..key.len() - ungrouped_len]);
    // Drop the trailing padding group `EncodeBytes` always emits.
    encoded_prefix.truncate(encoded_prefix.len() - 9);
    encoded_prefix.extend_from_slice(&key[key.len() - ungrouped_len..]);
    encoded_prefix
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestTruncateTS` (`misc_test.go`).
    #[test]
    fn truncate_ts_drops_the_commit_timestamp() {
        let key_with_ts = b"1212121212121212";
        assert_eq!(b"12121212", truncate_ts(key_with_ts));
        assert_eq!(b"12", truncate_ts(b"12"));
    }

    /// Go `TestEncodeKeyPrefix` (`misc_test.go`).
    #[test]
    fn encode_key_prefix_keeps_the_ungrouped_tail_raw() {
        assert_eq!(
            vec![
                b'1', b'2', b'1', b'2', b'1', b'2', b'1', b'2', 0xff, b'1', b'2', b'1', b'2', b'1',
                b'2', b'1', b'2', 0xff
            ],
            encode_key_prefix(b"1212121212121212")
        );
        assert_eq!(
            vec![
                b'1', b'2', b'1', b'2', b'1', b'2', b'1', b'2', 0xff, b'1', b'2', b'1', b'2', b'1',
                b'2', b'1'
            ],
            encode_key_prefix(b"121212121212121")
        );
        assert_eq!(vec![b'1', b'2'], encode_key_prefix(b"12"));
    }
}
