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

//! Complete Rust transcreation of Go `pkg/tablecodec`.
//!
//! The crate sits above the dependency-leaf datum codecs and canonical KV
//! handles, preserving Go's real `util/codec -> kv -> tablecodec` direction.

mod table_index;
mod table_row;

/// Dependency-leaf table-key framing shared with transaction diagnostics.
///
/// Its `RecordHandle` is a wire-decoding value. Table row/index operations in
/// this crate use the canonical `tidb_txnkv::Handle`.
pub mod table_key {
    pub use tidb_codec::table_key::*;
}

pub use table_index::{
    common_pk_restored_column_ids, cut_common_handle, cut_index_key, cut_index_key_by_ids,
    decode_handle_in_index_key, decode_handle_in_index_value, decode_index_handle, decode_index_kv,
    decode_index_kv_into, decode_int_handle_in_index_value, decode_temp_index_value,
    encode_handle_in_unique_index_value, filter_overwritten_temp_index_values, generate_index_key,
    generate_index_value, get_index_key_buffer, get_table_index_key_range,
    index_key_to_temp_index_key, index_kv_is_unique, index_value_version, is_index_key,
    is_record_key, is_restored_data, is_table_key, is_temp_index_key, is_untouched_index_kv,
    legacy_non_unique_index_value, restored_padding_field_type, split_index_value, table_index_key,
    table_index_prefix, temp_index_key_to_index_key, temp_index_value_is_untouched,
    truncate_index_value, truncate_index_values, verify_table_ids_for_ranges, CutIndexValues,
    HandleStatus, IndexColumn, IndexInfo, IndexValueSegments, TableColumn, TableIndexError,
    TableInfo, TableKeyRange, TempIndexValue, TempIndexValueElem, TempIndexValueFlag,
    COMMON_HANDLE_FLAG, INDEX_ID_MASK, INDEX_INT_HANDLE_FLAG, INDEX_UINT_FLAG, INDEX_VERSION_FLAG,
    MAX_OLD_ENCODE_VALUE_LEN, PARTITION_ID_FLAG, RESTORE_DATA_FLAG, TABLE_RECORD_ROW_KEY_LEN,
    TEMP_INDEX_PREFIX, UNCOMMITTED_INDEX_KV_FLAG,
};
pub use table_row::{
    cut_table_row, decode_column_value, decode_column_value_into, decode_handle_to_datum_map,
    decode_table_row_into_map, decode_table_row_to_map, encode_old_table_row, encode_table_row,
    encode_table_value, flatten_datum, tablecodec_binary_collation, unflatten_datum,
    unflatten_datums, TableRowError,
};
