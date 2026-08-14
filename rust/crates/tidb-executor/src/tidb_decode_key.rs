// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::Write as _;

use serde_json::{Map, Number, Value};
use tidb_codec::table_key::{decode_index_key, decode_key_head, decode_record_key, KeyHead};
use tidb_codec::ColumnInfo;
use tidb_datatype::{Datum, FieldType, SessionTimeZone};
use tidb_tablecodec::{decode_column_value, decode_index_kv, HandleStatus};

use crate::kv_table::KvTable;

#[derive(Clone, Debug)]
struct DecodeColumn {
    id: i64,
    name: String,
    field_type: FieldType,
    is_pk_handle: bool,
}

#[derive(Clone, Debug)]
struct DecodeIndex {
    id: i64,
    columns: Vec<usize>,
}

#[derive(Clone, Debug)]
struct DecodeTable {
    logical_id: i64,
    partitioned: bool,
    columns: Vec<DecodeColumn>,
    common_handle_offsets: Vec<usize>,
    indexes: Vec<DecodeIndex>,
    use_new_collation: bool,
}

/// Immutable table metadata used by `TIDB_DECODE_KEY` during one catalog version.
#[derive(Clone, Debug, Default)]
pub struct TidbDecodeKeySnapshot {
    tables: HashMap<i64, DecodeTable>,
}

impl TidbDecodeKeySnapshot {
    pub(crate) fn insert_table(&mut self, table: &KvTable) {
        let metadata = DecodeTable {
            logical_id: table.table_id,
            partitioned: table.partition().is_some(),
            columns: table
                .columns
                .iter()
                .enumerate()
                .map(|(offset, column)| DecodeColumn {
                    id: column.id,
                    name: column.name.to_ascii_lowercase(),
                    field_type: column.field_type.clone(),
                    is_pk_handle: table.is_clustered_handle_column(offset),
                })
                .collect(),
            common_handle_offsets: table.common_handle_offsets().to_vec(),
            indexes: table
                .indexes()
                .iter()
                .map(|index| DecodeIndex {
                    id: index.id,
                    columns: index.column_offsets.clone(),
                })
                .collect(),
            use_new_collation: table.use_new_collation(),
        };
        self.tables.insert(table.table_id, metadata.clone());
        if let Some(partition) = table.partition() {
            for definition in &partition.definitions {
                self.tables.insert(definition.id, metadata.clone());
            }
        }
    }

    /// Decodes the hexadecimal argument using the statement's catalog and time zone.
    pub fn decode(&self, input: &[u8], zone: &SessionTimeZone) -> Result<Vec<u8>, String> {
        let mut key = decode_hex(input).map_err(|partial| invalid_key(&partial))?;
        if let Ok((_, decoded)) = tidb_codec::decode_bytes(&key) {
            key = decoded;
        }
        let physical_table_id = tidb_codec::table_key::decode_table_id(&key);
        if physical_table_id <= 0 {
            return Err(invalid_key(&key));
        }
        let table = self.tables.get(&physical_table_id);
        let output = if tidb_tablecodec::is_record_key(&key) {
            decode_record(&key, physical_table_id, table, zone)?
        } else if tidb_tablecodec::is_index_key(&key) {
            decode_index(&key, physical_table_id, table, zone)?
        } else if tidb_tablecodec::is_table_key(&key) {
            decode_table(physical_table_id, table)
        } else {
            return Err(invalid_key(&key));
        };
        serde_json::to_vec(&Value::Object(output)).map_err(|error| error.to_string())
    }
}

fn decode_record(
    key: &[u8],
    physical_table_id: i64,
    table: Option<&DecodeTable>,
    zone: &SessionTimeZone,
) -> Result<Map<String, Value>, String> {
    let (_, handle) = decode_record_key(key).map_err(|error| error.to_string())?;
    let mut output = Map::new();
    if let Some(value) = handle.int_value() {
        let table_id = add_table_ids(&mut output, physical_table_id, table);
        output.insert("table_id".to_owned(), Value::String(table_id.to_string()));
        let handle_name = table
            .and_then(|table| table.columns.iter().find(|column| column.is_pk_handle))
            .map_or("_tidb_rowid", |column| column.name.as_str());
        output.insert(handle_name.to_owned(), Value::Number(Number::from(value)));
        return Ok(output);
    }

    let Some(table) = table else {
        output.insert("handle".to_owned(), Value::String(handle.to_string()));
        output.insert(
            "table_id".to_owned(),
            Value::Number(Number::from(physical_table_id)),
        );
        return Ok(output);
    };
    if table.common_handle_offsets.is_empty() {
        return Err(format!(
            "primary key not found when decoding record key: {}",
            uppercase_hex(key)
        ));
    }
    let encoded = handle
        .encoded_columns()
        .map_err(|error| error.to_string())?;
    if encoded.len() != table.common_handle_offsets.len() {
        return Err("primary key length not match handle columns number in key".to_owned());
    }
    let mut values = Map::new();
    for (encoded, offset) in encoded.iter().zip(&table.common_handle_offsets) {
        let column = table.columns.get(*offset).ok_or_else(|| {
            format!(
                "column not found when decoding record key: {}",
                uppercase_hex(key)
            )
        })?;
        let datum = decode_column_value(encoded, &column.field_type, Some(zone))
            .map_err(|error| error.to_string())?;
        values.insert(column.name.clone(), datum_json(&datum)?);
    }
    let table_id = add_table_ids(&mut output, physical_table_id, Some(table));
    output.insert("handle".to_owned(), Value::Object(values));
    output.insert("table_id".to_owned(), Value::Number(Number::from(table_id)));
    Ok(output)
}

fn decode_index(
    key: &[u8],
    physical_table_id: i64,
    table: Option<&DecodeTable>,
    zone: &SessionTimeZone,
) -> Result<Map<String, Value>, String> {
    let index_id = match decode_key_head(key) {
        Ok(KeyHead::Index { index_id, .. }) => index_id,
        _ => return Err(format!("invalid record/index key: {}", uppercase_hex(key))),
    };
    let mut output = Map::new();
    if let Some(table) = table {
        let index = table
            .indexes
            .iter()
            .find(|index| index.id == index_id)
            .ok_or_else(|| {
                format!(
                    "index not found when decoding index key: {}",
                    uppercase_hex(key)
                )
            })?;
        let columns = index
            .columns
            .iter()
            .map(|offset| {
                let column = table
                    .columns
                    .get(*offset)
                    .ok_or("invalid index column offset")?;
                Ok(ColumnInfo {
                    id: column.id,
                    is_pk_handle: column.is_pk_handle,
                    virtual_generated: false,
                    field_type: column.field_type.clone(),
                })
            })
            .collect::<Result<Vec<_>, &str>>()
            .map_err(str::to_owned)?;
        let encoded = decode_index_kv(
            table.use_new_collation,
            key,
            &[0],
            columns.len(),
            HandleStatus::NotNeeded,
            &columns,
        )
        .map_err(|error| error.to_string())?;
        let mut values = Map::new();
        for ((encoded, offset), column) in encoded.iter().zip(&index.columns).zip(&columns) {
            let datum = decode_column_value(encoded, &column.field_type, Some(zone))
                .map_err(|error| error.to_string())?;
            let name = &table.columns[*offset].name;
            values.insert(name.clone(), datum_json(&datum)?);
        }
        let table_id = add_table_ids(&mut output, physical_table_id, Some(table));
        output.insert("index_id".to_owned(), Value::Number(Number::from(index_id)));
        output.insert("index_vals".to_owned(), Value::Object(values));
        output.insert("table_id".to_owned(), Value::Number(Number::from(table_id)));
        return Ok(output);
    }

    let (_, _, values) =
        decode_index_key(key).map_err(|_| format!("invalid index key: {}", uppercase_hex(key)))?;
    output.insert("index_id".to_owned(), Value::Number(Number::from(index_id)));
    output.insert("index_vals".to_owned(), Value::String(values.join(", ")));
    output.insert(
        "table_id".to_owned(),
        Value::Number(Number::from(physical_table_id)),
    );
    Ok(output)
}

fn decode_table(physical_table_id: i64, table: Option<&DecodeTable>) -> Map<String, Value> {
    let mut output = Map::new();
    let table_id = add_table_ids(&mut output, physical_table_id, table);
    output.insert("table_id".to_owned(), Value::Number(Number::from(table_id)));
    output
}

fn add_table_ids(
    output: &mut Map<String, Value>,
    physical_table_id: i64,
    table: Option<&DecodeTable>,
) -> i64 {
    let Some(table) = table else {
        return physical_table_id;
    };
    if table.partitioned {
        output.insert(
            "partition_id".to_owned(),
            Value::Number(Number::from(physical_table_id)),
        );
    }
    table.logical_id
}

fn datum_json(datum: &Datum) -> Result<Value, String> {
    if datum.is_null() {
        return Ok(Value::Null);
    }
    let bytes = datum.sql_bytes().map_err(|error| error.to_string())?;
    Ok(Value::String(String::from_utf8_lossy(&bytes).into_owned()))
}

fn decode_hex(input: &[u8]) -> Result<Vec<u8>, Vec<u8>> {
    let mut decoded = Vec::with_capacity(input.len() / 2);
    let mut pairs = input.chunks_exact(2);
    for pair in &mut pairs {
        let Some(high) = hex_nibble(pair[0]) else {
            return Err(decoded);
        };
        let Some(low) = hex_nibble(pair[1]) else {
            return Err(decoded);
        };
        decoded.push((high << 4) | low);
    }
    if pairs.remainder().is_empty() {
        Ok(decoded)
    } else {
        Err(decoded)
    }
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn invalid_key(key: &[u8]) -> String {
    format!("invalid key: {}", uppercase_hex(key))
}

fn uppercase_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02X}").expect("writing to a String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    const UTC: SessionTimeZone = SessionTimeZone::Fixed {
        name: String::new(),
        offset_secs: 0,
    };

    fn decoded(input: &str) -> String {
        String::from_utf8(
            TidbDecodeKeySnapshot::default()
                .decode(input.as_bytes(), &UTC)
                .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn generic_go_vectors() {
        assert_eq!(
            decoded("74800000000000002B5F72800000000000A5D3"),
            r#"{"_tidb_rowid":42451,"table_id":"43"}"#
        );
        assert_eq!(
            decoded("74800000000000ffff5f7205bff199999999999a013131000000000000f9"),
            r#"{"handle":"{1.1, 11}","table_id":65535}"#
        );
        assert_eq!(
            decoded("7480000000000000695F698000000000000001038000000000004E20"),
            r#"{"index_id":1,"index_vals":"20000","table_id":105}"#
        );
        assert_eq!(
            decoded("7480000000000000FF4700000000000000F8"),
            r#"{"table_id":71}"#
        );
    }

    #[test]
    fn malformed_hex_reports_only_the_decoded_prefix() {
        assert_eq!(
            TidbDecodeKeySnapshot::default()
                .decode(b"7480xx", &UTC)
                .unwrap_err(),
            "invalid key: 7480"
        );
    }
}
