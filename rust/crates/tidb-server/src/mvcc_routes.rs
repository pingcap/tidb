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
//! `/mvcc/hex/{hexKey}`, `/mvcc/key/{db}/{table}[/{handle}]`,
//! `/mvcc/index/{db}/{table}/{index}[/{handle}]` and
//! `/mvcc/txn/{startTS}/{db}/{table}`, over the store helper's
//! `GetMvccByEncodedKey` / `GetMvccByStartTs` (`RegionStore`), answering
//! `helper.MvccKV` with kvrpcpb's MVCC records through `encoding/json`.

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
    let raw_query = path.split_once('?').map_or("", |(_, query)| query);
    match parts.as_slice() {
        ["hex", hex_key] => Some(handle_mvcc_get_by_hex(store, hex_key)),
        ["key", db_name, table_name] => Some(parse_query(raw_query, true).and_then(|values| {
            handle_mvcc_get_by_key(store, schema, db_name, table_name, None, &values)
        })),
        ["key", db_name, table_name, handle] => Some(parse_query(raw_query, true).and_then(|values| {
            handle_mvcc_get_by_key(store, schema, db_name, table_name, Some(handle), &values)
        })),
        ["index", db_name, table_name, index_name] => Some(parse_query(raw_query, true).and_then(|values| {
            handle_mvcc_get_by_idx(store, schema, db_name, table_name, index_name, None, &values)
        })),
        ["index", db_name, table_name, index_name, handle] => Some(parse_query(raw_query, true).and_then(|values| {
            handle_mvcc_get_by_idx(store, schema, db_name, table_name, index_name, Some(handle), &values)
        })),
        ["txn", start_ts, db_name, table_name] => Some(handle_mvcc_get_by_txn(store, schema, start_ts, db_name, table_name)),
        _ => None,
    }
}

/// Go `url.Values`, in the request's key order of first appearance.
pub(crate) type Values = Vec<(String, Vec<String>)>;

fn values_get<'a>(values: &'a Values, key: &str) -> Option<&'a Vec<String>> {
    values.iter().find(|(name, _)| name == key).map(|(_, list)| list)
}

/// Go `parseQuery`: `url.ParseQuery` that keeps a bare key (no `=`) with
/// no value at all.
fn parse_query(mut query: &str, should_unescape: bool) -> Result<Values, String> {
    let mut m: Values = Vec::new();
    let push = |m: &mut Values, key: String, value: Option<String>| {
        let entry = match m.iter().position(|(name, _)| *name == key) {
            Some(position) => &mut m[position].1,
            None => {
                m.push((key, Vec::new()));
                &mut m.last_mut().expect("just pushed").1
            }
        };
        entry.extend(value);
    };
    while !query.is_empty() {
        let mut key = query;
        if let Some(i) = key.find(['&', ';']) {
            (key, query) = (&key[..i], &key[i + 1..]);
        } else {
            query = "";
        }
        if key.is_empty() {
            continue;
        }
        if let Some(i) = key.find('=') {
            let (mut key, mut value) = (key[..i].to_owned(), key[i + 1..].to_owned());
            if should_unescape {
                key = query_unescape(&key)?;
                value = query_unescape(&value)?;
            }
            push(&mut m, key, Some(value));
        } else {
            let key = if should_unescape { query_unescape(key)? } else { key.to_owned() };
            push(&mut m, key, None);
        }
    }
    Ok(m)
}

/// Go `URL.Query()`: `url.ParseQuery` with its error dropped, so a pair
/// holding a `;` or a bad escape is skipped.
pub(crate) fn url_query(mut query: &str) -> Values {
    let mut m: Values = Vec::new();
    while !query.is_empty() {
        let key;
        (key, query) = query.split_once('&').unwrap_or((query, ""));
        if key.contains(';') || key.is_empty() {
            continue;
        }
        let (key, value) = key.split_once('=').unwrap_or((key, ""));
        let (Ok(key), Ok(value)) = (query_unescape(key), query_unescape(value)) else {
            continue;
        };
        match m.iter_mut().find(|(name, _)| *name == key) {
            Some((_, list)) => list.push(value),
            None => m.push((key, vec![value])),
        }
    }
    m
}

/// Go `url.QueryUnescape`.
fn query_unescape(s: &str) -> Result<String, String> {
    let bytes = s.as_bytes();
    let is_hex = |byte: u8| byte.is_ascii_hexdigit();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                if i + 2 >= bytes.len() || !is_hex(bytes[i + 1]) || !is_hex(bytes[i + 2]) {
                    let tail = &bytes[i..bytes.len().min(i + 3)];
                    return Err(format!("invalid URL escape {}", tidb_go_strconv::quote_bytes(tail)));
                }
                let digit = |byte: u8| (byte as char).to_digit(16).expect("checked hex") as u8;
                out.push((digit(bytes[i + 1]) << 4) | digit(bytes[i + 2]));
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
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

/// Go `TikvHandlerTool.GetHandle`: the path's int handle, or a clustered
/// table's primary key from the query's column values.
fn get_handle(
    table: &tidb_model::TableInfo,
    kv_table: &tidb_table::tables::TableCommon,
    handle: Option<&str>,
    values: &Values,
) -> Result<tidb_table::tables::TableHandle, String> {
    if let Some(int_handle_str) = handle {
        if table.is_common_handle {
            return Err("For clustered index tables, please use query strings to specify the column values. bad request".to_owned());
        }
        let int_handle = tidb_go_strconv::parse_int(int_handle_str, 0, 64).map_err(|(_, error)| error.to_string())?;
        return Ok(tidb_table::tables::TableHandle::Int(int_handle));
    }
    let pk_idx = table.indices.iter_deref().find(|index| index.read().primary);
    let Some(pk_idx) = pk_idx.filter(|_| table.is_common_handle) else {
        return Err("Clustered common handle not found. bad request".to_owned());
    };
    let cols = table.cols();
    let pk_cols: Vec<_> = pk_idx
        .read()
        .columns
        .iter_deref()
        .map(|idx_col| cols.get(idx_col.read().offset as usize).expect("primary key column offset"))
        .collect();
    let mut pk_dts = form_value2_datum_row(values, &pk_cols)?;
    let codec = kv_table.codec_table_info();
    tidb_tablecodec::truncate_index_values(&codec, &codec.indices[0], &mut pk_dts).map_err(|error| error.to_string())?;
    let handle_bytes = tidb_codec::Encoder::new(tidb_datatype::new_collation_enabled())
        .encode_key(&pk_dts)
        .map_err(|error| error.to_string())?;
    tidb_txnkv::CommonHandle::new(handle_bytes.clone()).map_err(|error| format!("{error:?}"))?;
    Ok(tidb_table::tables::TableHandle::Common(handle_bytes))
}

/// Go `TikvHandlerTool.formValue2DatumRow` converts URL query string to a
/// Datum Row.
fn form_value2_datum_row(
    values: &Values,
    idx_cols: &[tidb_model::GoShared<tidb_model::ColumnInfo>],
) -> Result<Vec<tidb_datatype::Datum>, String> {
    // HTTP request is not a database session, set timezone to UTC directly
    // here (Go `stmtctx.NewStmtCtxWithTimeZone(time.UTC)`).
    let utc = tidb_datatype::SessionTimeZone::utc();
    let context = tidb_datatype::ConversionContext::new(
        tidb_datatype::STRICT_FLAGS,
        tidb_datatype::ConversionLocation::from_time_zone(&utc),
        &tidb_context::IGNORE_CONVERSION_WARNINGS,
    );
    let mut data = Vec::with_capacity(idx_cols.len());
    for col in idx_cols {
        let col = col.read();
        let col_name = col.name.original();
        let Some(vals) = values_get(values, col_name) else {
            return Err(format!("Missing value for index column {col_name}. bad request"));
        };
        match vals.as_slice() {
            [] => data.push(tidb_datatype::Datum::Null),
            [value] => {
                let b_datum = tidb_datatype::Datum::new_string_datum(value.as_str());
                let converted = b_datum
                    .convert_to_in_context(&col.field_type, &context, &utc)
                    .map_err(|error| error.to_string())?;
                if let Some(error) = converted.error {
                    return Err(error.to_string());
                }
                data.push(converted.value);
            }
            _ => {
                return Err(format!(
                    "Invalid query form for column '{col_name}', it's values are [{}]. Column value should be unique for one index record. bad request",
                    vals.join(" ")
                ))
            }
        }
    }
    Ok(data)
}

/// The catalog's table for `get_table`'s meta.
fn kv_table<'a>(
    catalog: &'a tidb_infoschema::SessionExtendedInfoSchema,
    table: &tidb_model::TableInfo,
) -> Result<&'a tidb_table::tables::TableCommon, String> {
    catalog
        .kv_table_by_id(table.id)
        .ok_or_else(|| format!("[schema:1146]Table '{}' doesn't exist", table.name.original()))
}

/// Go `MvccTxnHandler.handleMvccGetByIdx` gets MVCC info by an index key.
fn handle_mvcc_get_by_idx(
    store: &dyn RegionStore,
    schema: &Schema,
    db_name: &str,
    table_name: &str,
    index_name: &str,
    handle: Option<&str>,
    values: &Values,
) -> Result<String, String> {
    let (table, physical_id) = get_table(schema, db_name, table_name)?;
    let catalog = schema();
    let table = table.read();
    let kv_table = kv_table(&catalog, &table)?;
    let handle = get_handle(&table, kv_table, handle, values)?;

    let mut idx_cols = Vec::new();
    let mut idx = None;
    for v in table.indices.iter_deref() {
        let v = v.read();
        if v.name.original().eq_ignore_ascii_case(index_name) {
            for c in v.columns.iter_deref() {
                idx_cols.push(table.columns.get(c.read().offset as usize).expect("index column offset").clone());
            }
            idx = Some(v.id);
            break;
        }
    }
    let Some(idx) = idx else {
        return Err(format!("Index {index_name} not found! not found"));
    };
    get_mvcc_by_idx_value(store, kv_table, idx, physical_id, values, &idx_cols, &handle)
}

/// Go `TikvHandlerTool.GetMvccByIdxValue`: the index entry's MVCC record and
/// its temporary index entry's.
fn get_mvcc_by_idx_value(
    store: &dyn RegionStore,
    kv_table: &tidb_table::tables::TableCommon,
    idx: i64,
    physical_id: i64,
    values: &Values,
    idx_cols: &[tidb_model::GoShared<tidb_model::ColumnInfo>],
    handle: &tidb_table::tables::TableHandle,
) -> Result<String, String> {
    let idx_row = form_value2_datum_row(values, idx_cols)?;
    let (mut encoded_key, _) = kv_table
        .gen_index_key(idx, idx_row, handle, physical_id, &tidb_datatype::SessionTimeZone::utc())
        .map_err(|error| format!("{error:?}"))?;
    let data = store.mvcc_get_by_key(&encoded_key)?;
    let region_id = store.region_id_by_key(&encoded_key)?;
    let idx_data = MvccKv {
        key: upper_hex(&encoded_key),
        region_id,
        value: response_json(&data),
    };
    tidb_tablecodec::index_key_to_temp_index_key(&mut encoded_key).map_err(|error| error.to_string())?;
    let data = store.mvcc_get_by_key(&encoded_key)?;
    let region_id = store.region_id_by_key(&encoded_key)?;
    let temp_idx_data = MvccKv {
        key: upper_hex(&encoded_key),
        region_id,
        value: response_json(&data),
    };
    to_json(&[idx_data, temp_idx_data])
}

/// Go `handleMvccGetByKey`'s answer with `decode`, in `encoding/json`'s
/// sorted map key order.
#[derive(Serialize)]
struct DecodedMvcc {
    data: std::collections::BTreeMap<String, std::collections::BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    decode_error: Option<String>,
    info: MvccInfoJson,
    key: String,
}

/// Go `MvccTxnHandler.handleMvccGetByKey`.
fn handle_mvcc_get_by_key(
    store: &dyn RegionStore,
    schema: &Schema,
    db_name: &str,
    table_name: &str,
    handle: Option<&str>,
    values: &Values,
) -> Result<String, String> {
    let (table, physical_id) = get_table(schema, db_name, table_name)?;
    let catalog = schema();
    let table = table.read();
    let handle = get_handle(&table, kv_table(&catalog, &table)?, handle, values)?;

    let record_handle = match &handle {
        tidb_table::tables::TableHandle::Int(value) => tidb_tablecodec::table_key::RecordHandle::Int(*value),
        tidb_table::tables::TableHandle::Common(bytes) => tidb_tablecodec::table_key::RecordHandle::Common(bytes.clone()),
    };
    let encoded_key = tidb_tablecodec::table_key::encode_row_key_with_handle(physical_id, &record_handle);
    let data = store.mvcc_get_by_key(&encoded_key)?;
    let region_id = store.region_id_by_key(&encoded_key)?;
    let resp = MvccKv {
        key: upper_hex(&encoded_key),
        region_id,
        value: response_json(&data),
    };
    if values_get(values, "decode").and_then(|list| list.first()).is_none_or(|value| value.is_empty()) {
        return to_json(&resp);
    }
    let col_map: std::collections::BTreeMap<i64, tidb_datatype::FieldType> = table
        .columns
        .iter_deref()
        .map(|col| (col.read().id, col.read().field_type.clone()))
        .collect();

    let Some(info) = data.info.as_ref() else {
        return to_json(&resp);
    };
    let mut datas = std::collections::BTreeMap::new();
    let mut err = None;
    for w in &info.writes {
        if !w.short_value.is_empty() {
            let (record, error) = decode_mvcc_data(&w.short_value, &col_map, &table);
            datas.insert(w.start_ts.to_string(), record);
            err = error;
        }
    }
    for v in &info.values {
        if !v.value.is_empty() {
            let (record, error) = decode_mvcc_data(&v.value, &col_map, &table);
            datas.insert(v.start_ts.to_string(), record);
            err = error;
        }
    }
    if datas.is_empty() {
        return to_json(&resp);
    }
    to_json(&DecodedMvcc {
        data: datas,
        decode_error: err,
        info: resp.value.info.expect("the response has info"),
        key: resp.key,
    })
}

/// Go `MvccTxnHandler.decodeMvccData`: the row's columns by name, and the
/// last error Go's loop kept.
fn decode_mvcc_data(
    bs: &[u8],
    col_map: &std::collections::BTreeMap<i64, tidb_datatype::FieldType>,
    tb: &tidb_model::TableInfo,
) -> (std::collections::BTreeMap<String, String>, Option<String>) {
    let utc = tidb_datatype::SessionTimeZone::utc();
    let (rs, mut err) = match tidb_tablecodec::decode_table_row_to_map(bs, col_map, Some(&utc)) {
        Ok(rs) => (rs, None),
        Err(error) => (std::collections::BTreeMap::new(), Some(error.to_string())),
    };
    let mut record = std::collections::BTreeMap::new();
    for col in tb.columns.iter_deref() {
        let col = col.read();
        if let Some(c) = rs.get(&col.id) {
            let mut data = "nil".to_owned();
            if !c.is_null() {
                (data, err) = match c.sql_bytes() {
                    Ok(bytes) => (String::from_utf8_lossy(&bytes).into_owned(), None),
                    Err(error) => (String::new(), Some(error.to_string())),
                };
            }
            record.insert(col.name.original().to_owned(), data);
        }
    }
    (record, err)
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
