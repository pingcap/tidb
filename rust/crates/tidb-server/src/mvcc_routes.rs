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

//! Go `pkg/server/handler/tikvhandler`'s `MvccTxnHandler` for
//! `/mvcc/hex/{hexKey}`, `/mvcc/key/{db}/{table}/{handle}` and
//! `/mvcc/txn/{startTS}/{db}/{table}`, over the store helper's
//! `GetMvccByEncodedKey` / `GetMvccByStartTs` (`RegionStore`), answering
//! `helper.MvccKV` with kvrpcpb's MVCC records through `encoding/json`.
//!
//! The query-string forms are not served here: `/mvcc/key/{db}/{table}`
//! (a clustered handle's column values), `/mvcc/index/...` and the
//! `decode` option all convert or decode column values (Go
//! `formValue2DatumRow`'s `ConvertTo`, `DecodeRowToDatumMap`).

use base64::Engine;
use serde::Serialize;
use tidb_kv::region_map::RegionStore;
use tikv_client::proto::kvrpcpb;

use crate::region_routes::Schema;

fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn is_zero_i32(value: &i32) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn base64_bytes(bytes: &[u8]) -> Option<String> {
    (!bytes.is_empty()).then(|| base64::engine::general_purpose::STANDARD.encode(bytes))
}

/// Go `kvrpcpb.MvccLock` through `encoding/json`.
#[derive(Serialize)]
struct MvccLockJson {
    #[serde(rename = "type", skip_serializing_if = "is_zero_i32")]
    kind: i32,
    #[serde(skip_serializing_if = "is_zero")]
    start_ts: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    short_value: Option<String>,
    #[serde(skip_serializing_if = "is_zero")]
    ttl: u64,
    #[serde(skip_serializing_if = "is_zero")]
    for_update_ts: u64,
    #[serde(skip_serializing_if = "is_zero")]
    txn_size: u64,
    #[serde(skip_serializing_if = "is_false")]
    use_async_commit: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    secondaries: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    rollback_ts: Vec<u64>,
    #[serde(skip_serializing_if = "is_zero")]
    last_change_ts: u64,
    #[serde(skip_serializing_if = "is_zero")]
    versions_to_last_change: u64,
}

/// Go `kvrpcpb.MvccWrite` through `encoding/json`.
#[derive(Serialize)]
struct MvccWriteJson {
    #[serde(rename = "type", skip_serializing_if = "is_zero_i32")]
    kind: i32,
    #[serde(skip_serializing_if = "is_zero")]
    start_ts: u64,
    #[serde(skip_serializing_if = "is_zero")]
    commit_ts: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    short_value: Option<String>,
    #[serde(skip_serializing_if = "is_false")]
    has_overlapped_rollback: bool,
    #[serde(skip_serializing_if = "is_false")]
    has_gc_fence: bool,
    #[serde(skip_serializing_if = "is_zero")]
    gc_fence: u64,
    #[serde(skip_serializing_if = "is_zero")]
    last_change_ts: u64,
    #[serde(skip_serializing_if = "is_zero")]
    versions_to_last_change: u64,
}

/// Go `kvrpcpb.MvccValue` through `encoding/json`.
#[derive(Serialize)]
struct MvccValueJson {
    #[serde(skip_serializing_if = "is_zero")]
    start_ts: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
}

/// Go `kvrpcpb.MvccInfo` through `encoding/json`.
#[derive(Serialize)]
struct MvccInfoJson {
    #[serde(skip_serializing_if = "Option::is_none")]
    lock: Option<MvccLockJson>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    writes: Vec<MvccWriteJson>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    values: Vec<MvccValueJson>,
}

/// Go `kvrpcpb.MvccGetByKeyResponse` through `encoding/json`. The region
/// error is always nil here: the helper retries a region error.
#[derive(Serialize)]
struct MvccGetByKeyResponseJson {
    #[serde(skip_serializing_if = "String::is_empty")]
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    info: Option<MvccInfoJson>,
}

fn response_json(response: &kvrpcpb::MvccGetByKeyResponse) -> MvccGetByKeyResponseJson {
    MvccGetByKeyResponseJson {
        error: response.error.clone(),
        info: response.info.as_ref().map(|info| MvccInfoJson {
            lock: info.lock.as_ref().map(|lock| MvccLockJson {
                kind: lock.r#type,
                start_ts: lock.start_ts,
                primary: base64_bytes(&lock.primary),
                short_value: base64_bytes(&lock.short_value),
                ttl: lock.ttl,
                for_update_ts: lock.for_update_ts,
                txn_size: lock.txn_size,
                use_async_commit: lock.use_async_commit,
                secondaries: lock
                    .secondaries
                    .iter()
                    .map(|key| base64::engine::general_purpose::STANDARD.encode(key))
                    .collect(),
                rollback_ts: lock.rollback_ts.clone(),
                last_change_ts: lock.last_change_ts,
                versions_to_last_change: lock.versions_to_last_change,
            }),
            writes: info
                .writes
                .iter()
                .map(|write| MvccWriteJson {
                    kind: write.r#type,
                    start_ts: write.start_ts,
                    commit_ts: write.commit_ts,
                    short_value: base64_bytes(&write.short_value),
                    has_overlapped_rollback: write.has_overlapped_rollback,
                    has_gc_fence: write.has_gc_fence,
                    gc_fence: write.gc_fence,
                    last_change_ts: write.last_change_ts,
                    versions_to_last_change: write.versions_to_last_change,
                })
                .collect(),
            values: info
                .values
                .iter()
                .map(|value| MvccValueJson {
                    start_ts: value.start_ts,
                    value: base64_bytes(&value.value),
                })
                .collect(),
        }),
    }
}

/// Go `helper.MvccKV`.
#[derive(Serialize)]
struct MvccKv {
    key: String,
    region_id: u64,
    value: MvccGetByKeyResponseJson,
}

fn upper_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

fn to_json(value: &impl Serialize) -> Result<String, String> {
    serde_json::to_string(value).map_err(|error| error.to_string())
}

/// Go `hex.DecodeString`'s errors.
fn hex_decode(text: &str) -> Result<Vec<u8>, String> {
    let bytes = text.as_bytes();
    let nibble = |index: usize| -> Result<u8, String> {
        let byte = bytes[index];
        (byte as char)
            .to_digit(16)
            .map(|digit| digit as u8)
            .ok_or_else(|| format!("encoding/hex: invalid byte: {}", go_quote_rune(byte)))
    };
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut index = 0;
    while index + 1 < bytes.len() {
        out.push((nibble(index)? << 4) | nibble(index + 1)?);
        index += 2;
    }
    if bytes.len() % 2 == 1 {
        nibble(bytes.len() - 1)?;
        return Err("encoding/hex: odd length hex string".to_owned());
    }
    Ok(out)
}

/// Go `%#U` of a byte as `hex.InvalidByteError` prints it.
fn go_quote_rune(byte: u8) -> String {
    let rune = char::from(byte);
    if rune.is_ascii_graphic() || rune == ' ' {
        format!("U+{:04X} '{rune}'", u32::from(byte))
    } else {
        format!("U+{:04X}", u32::from(byte))
    }
}

/// Go `MvccTxnHandler.ServeHTTP` for the routes this module serves.
pub fn mvcc_response(path: &str, store: &dyn RegionStore, schema: &Schema) -> Option<Result<String, String>> {
    let route = path.split_once('?').map_or(path, |(route, _)| route);
    let rest = route.strip_prefix("/mvcc/")?;
    let parts: Vec<&str> = rest.split('/').collect();
    match parts.as_slice() {
        ["hex", hex_key] => Some(handle_mvcc_get_by_hex(store, hex_key)),
        ["key", db_name, table_name, handle] => Some(handle_mvcc_get_by_key(store, schema, db_name, table_name, handle)),
        ["txn", start_ts, db_name, table_name] => Some(handle_mvcc_get_by_txn(store, schema, start_ts, db_name, table_name)),
        _ => None,
    }
}

/// Go `TikvHandlerTool.HandleMvccGetByHex`.
fn handle_mvcc_get_by_hex(store: &dyn RegionStore, hex_key: &str) -> Result<String, String> {
    let encoded_key = hex_decode(hex_key)?;
    let data = store.mvcc_get_by_key(&encoded_key)?;
    let region_id = store.region_id_by_key(&encoded_key)?;
    to_json(&MvccKv {
        key: hex_key.to_uppercase(),
        region_id,
        value: response_json(&data),
    })
}

/// Go `ExtractTableAndPartitionName`: `table(partition)`.
fn extract_table_and_partition_name(text: &str) -> (&str, &str) {
    let Some(start) = text.find('(') else {
        return (text, "");
    };
    let Some(end) = text.find(')') else {
        return (text, "");
    };
    (&text[..start], &text[start + 1..end])
}

/// Go `TikvHandlerTool.GetTable` + `GetPartition`: the physical table's
/// meta and physical id.
pub(crate) fn get_table(
    schema: &Schema,
    db_name: &str,
    table_name: &str,
) -> Result<(tidb_model::GoShared<tidb_model::TableInfo>, i64), String> {
    let (table_name, partition_name) = extract_table_and_partition_name(table_name);
    let catalog = schema();
    let Some((_, table)) = catalog.table_info_by_name(db_name, table_name) else {
        return Err(format!("[schema:1146]Table '{db_name}.{table_name}' doesn't exist"));
    };
    let info = table.read();
    let physical_id = match info.get_partition_info() {
        Some(partitions) => {
            if partition_name.is_empty() {
                return Err("work on partitioned table, please specify the table name like this: table(partition)".to_owned());
            }
            // Go `tables.FindPartitionByName`.
            let definitions = partitions.read().definitions.snapshot();
            definitions
                .iter()
                .find(|definition| definition.name.lowercase() == partition_name.to_lowercase())
                .map(|definition| definition.id)
                .ok_or_else(|| format!("[table:1735]Unknown partition '{partition_name}' in table '{}'", info.name.original()))?
        }
        None => {
            if !partition_name.is_empty() {
                return Err(format!("{} is not a partitionted table", info.name.original()));
            }
            info.id
        }
    };
    drop(info);
    Ok((table, physical_id))
}

/// Go `MvccTxnHandler.handleMvccGetByKey` with a path handle (Go
/// `GetHandle`'s int-handle branch), without the `decode` option.
fn handle_mvcc_get_by_key(
    store: &dyn RegionStore,
    schema: &Schema,
    db_name: &str,
    table_name: &str,
    handle: &str,
) -> Result<String, String> {
    let (table, physical_id) = get_table(schema, db_name, table_name)?;
    if table.read().is_common_handle {
        return Err("For clustered index tables, please use query strings to specify the column values. bad request".to_owned());
    }
    let int_handle = tidb_go_strconv::parse_int(handle, 0, 64).map_err(|(_, error)| error.to_string())?;
    let encoded_key = tidb_tablecodec::table_key::encode_row_key_with_handle(physical_id, &tidb_tablecodec::table_key::RecordHandle::Int(int_handle));
    let data = store.mvcc_get_by_key(&encoded_key)?;
    let region_id = store.region_id_by_key(&encoded_key)?;
    to_json(&MvccKv {
        key: upper_hex(&encoded_key),
        region_id,
        value: response_json(&data),
    })
}

/// Go `MvccTxnHandler.handleMvccGetByTxn`.
fn handle_mvcc_get_by_txn(
    store: &dyn RegionStore,
    schema: &Schema,
    start_ts: &str,
    db_name: &str,
    table_name: &str,
) -> Result<String, String> {
    let start_ts = tidb_go_strconv::parse_int(start_ts, 0, 64).map_err(|(_, error)| error.to_string())?;
    let (_, table_id) = get_table(schema, db_name, table_name)?;
    let start_key = tidb_tablecodec::table_key::encode_table_prefix(table_id);
    let end_key = tidb_tablecodec::table_key::encode_row_key_with_handle(table_id, &tidb_tablecodec::table_key::RecordHandle::Int(i64::MAX));
    match store.mvcc_get_by_start_ts(start_ts as u64, &start_key, &end_key)? {
        Some((key, region_id, response)) => to_json(&MvccKv {
            key: upper_hex(&key),
            region_id,
            value: response_json(&response),
        }),
        // Go `WriteData(nil)`.
        None => Ok("null".to_owned()),
    }
}
