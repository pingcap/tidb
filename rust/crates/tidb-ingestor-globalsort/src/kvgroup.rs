// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `kvgroup.go`: the names of the sorted KV groups.

/// Go `DataKVGroup`: the group of the data KVs. An index's group is its
/// index ID.
pub const DATA_KV_GROUP: &str = "data";

/// Go `IndexID2KVGroup`.
#[must_use]
pub fn index_id_2_kv_group(index_id: i64) -> String {
    index_id.to_string()
}

/// Go `KVGroup2IndexID`: Go `strconv.ParseInt(kvGroup, 10, 64)`.
///
/// # Errors
/// The group is not an index ID.
pub fn kv_group_2_index_id(kv_group: &str) -> crate::Result<i64> {
    tidb_go_strconv::parse_int(kv_group, 10, 64).map_err(|(_, e)| crate::other(e.to_string()))
}
