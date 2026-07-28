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

//! The value side of the catalog: scalars are decimal ASCII, catalog objects
//! are `encoding/json`, and policy objects carry a leading magic byte.

use tidb_model::db::DBInfo;
use tidb_model::placement::PolicyInfo;
use tidb_model::table_info::TableInfo;

use crate::error::{MetaError, Result};

/// Go `meta.CurrentMagicByteVer`: policy values are JSON behind a version byte.
pub const CURRENT_MAGIC_BYTE_VER: u8 = 0x00;

/// Go writes every structure scalar as `strconv.FormatInt`.
#[must_use]
pub fn encode_int_value(value: i64) -> Vec<u8> {
    value.to_string().into_bytes()
}

/// Go `TxStructure.GetInt64`: `strconv.ParseInt` over the stored bytes.
///
/// Go returns 0 for a missing key before it ever parses; a caller that has no
/// value should not call this.
pub fn parse_int_value(value: &[u8]) -> Result<i64> {
    std::str::from_utf8(value)
        .map_err(|_| MetaError::InvalidIntValue)?
        .parse()
        .map_err(|_| MetaError::InvalidIntValue)
}

/// Go `meta.attachMagicByte`.
#[must_use]
pub fn attach_magic_byte(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 1);
    out.push(CURRENT_MAGIC_BYTE_VER);
    out.extend_from_slice(data);
    out
}

/// Go `meta.detachMagicByte`. Bytes above `0x3F` select a handler that does
/// not exist yet, and any other JSON-range byte is a version this build cannot
/// read.
pub fn detach_magic_byte(value: &[u8]) -> Result<&[u8]> {
    let (&magic, data) = value.split_first().ok_or(MetaError::MalformedKey)?;
    if magic > 0x3F || magic != CURRENT_MAGIC_BYTE_VER {
        return Err(MetaError::InvalidJson(format!(
            "incompatible magic type handling module: {magic:#04x}"
        )));
    }
    Ok(data)
}

fn from_json<T: serde::de::DeserializeOwned>(value: &[u8]) -> Result<T> {
    serde_json::from_slice(value).map_err(|err| MetaError::InvalidJson(err.to_string()))
}

/// Encodes exactly as Go's `json.Marshal` does, HTML escaping and float
/// formatting included.
fn to_json<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    tidb_model::serde_helpers::to_go_json(value)
        .map_err(|err| MetaError::InvalidJson(err.to_string()))
}

/// Go `json.Unmarshal(value, dbInfo)` in `GetDatabase`/`ListDatabases`.
pub fn parse_db_info(value: &[u8]) -> Result<DBInfo> {
    from_json(value)
}

/// The write side of [`parse_db_info`]. Go `Mutator.CreateDatabase`.
pub fn serialize_db_info(db: &DBInfo) -> Result<Vec<u8>> {
    to_json(db)
}

/// Go `json.Unmarshal(value, tableInfo)` in `GetTable`/`ListTables`, including
/// the `tbInfo.DBID = dbID` fixup Go applies after decoding (`DBID` is
/// `json:"-"`, so it is never stored).
pub fn parse_table_info(value: &[u8], db_id: i64) -> Result<TableInfo> {
    let mut table: TableInfo = from_json(value)?;
    table.db_id = db_id;
    Ok(table)
}

/// The write side of [`parse_table_info`]. Go `Mutator.CreateTableOrView`.
pub fn serialize_table_info(table: &TableInfo) -> Result<Vec<u8>> {
    to_json(table)
}

/// Go `ListPolicies`: JSON behind the magic byte.
pub fn parse_policy_info(value: &[u8]) -> Result<PolicyInfo> {
    from_json(detach_magic_byte(value)?)
}

/// The write side of [`parse_policy_info`]. Go `Mutator.CreatePolicy`.
pub fn serialize_policy_info(policy: &PolicyInfo) -> Result<Vec<u8>> {
    Ok(attach_magic_byte(&to_json(policy)?))
}
