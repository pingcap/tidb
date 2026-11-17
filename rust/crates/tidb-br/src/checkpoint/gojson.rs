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

//! Go `encoding/json` for the values checkpoint files hold: `[]byte` as
//! standard base64 (`null` for nil), strings with Go's HTML escaping, and
//! `backuppb.File` / `rtree.Range` with their Go field names and order.

use base64::Engine as _;
use serde_json::Value as Json;
use tidb_proto::backup::{File, TableMeta};

use crate::errors::{self, Result};

/// Go `json.Marshal([]byte)`: `null` for an empty slice is Go's nil; BR
/// only writes non-nil slices here, so an empty one is `""`.
#[must_use]
pub fn bytes(b: &[u8]) -> String {
    format!(
        "\"{}\"",
        base64::engine::general_purpose::STANDARD.encode(b)
    )
}

/// Go `json.Marshal` of a `[]byte` that is nil when empty: `null`.
#[must_use]
pub fn opt_bytes(b: &[u8]) -> String {
    if b.is_empty() {
        "null".to_owned()
    } else {
        bytes(b)
    }
}

/// Go `json.Marshal(string)`: `<`, `>`, `&`, U+2028 and U+2029 escaped.
#[must_use]
pub fn string(s: &str) -> String {
    serde_json::to_string(s)
        .unwrap_or_default()
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}

/// Go's decode of a `[]byte` field.
///
/// # Errors
/// Not base64.
pub fn parse_bytes(v: &Json) -> Result<Vec<u8>> {
    match v {
        Json::Null => Ok(Vec::new()),
        Json::String(s) => base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|e| errors::errorf(e.to_string())),
        other => Err(errors::errorf(format!(
            "json: cannot unmarshal {other} into Go value of type []uint8"
        ))),
    }
}

/// `"name":value,` for a non-zero `omitempty` field.
fn field(out: &mut Vec<String>, name: &str, value: String, empty: bool) {
    if !empty {
        out.push(format!("\"{name}\":{value}"));
    }
}

/// Go `json.Marshal(*backuppb.TableMeta)`.
fn table_meta(m: &TableMeta) -> String {
    let mut f = Vec::new();
    field(
        &mut f,
        "physical_id",
        m.physical_id.to_string(),
        m.physical_id == 0,
    );
    field(&mut f, "crc64xor", m.crc64xor.to_string(), m.crc64xor == 0);
    field(
        &mut f,
        "total_kvs",
        m.total_kvs.to_string(),
        m.total_kvs == 0,
    );
    field(
        &mut f,
        "total_bytes",
        m.total_bytes.to_string(),
        m.total_bytes == 0,
    );
    format!("{{{}}}", f.join(","))
}

/// Go `json.Marshal(*backuppb.File)`: the protobuf struct's `json` tags,
/// all `omitempty`.
#[must_use]
pub fn file(file: &File) -> String {
    let mut f = Vec::new();
    field(&mut f, "name", string(&file.name), file.name.is_empty());
    field(
        &mut f,
        "sha256",
        bytes(&file.sha256),
        file.sha256.is_empty(),
    );
    field(
        &mut f,
        "start_key",
        bytes(&file.start_key),
        file.start_key.is_empty(),
    );
    field(
        &mut f,
        "end_key",
        bytes(&file.end_key),
        file.end_key.is_empty(),
    );
    field(
        &mut f,
        "start_version",
        file.start_version.to_string(),
        file.start_version == 0,
    );
    field(
        &mut f,
        "end_version",
        file.end_version.to_string(),
        file.end_version == 0,
    );
    field(
        &mut f,
        "crc64xor",
        file.crc64xor.to_string(),
        file.crc64xor == 0,
    );
    field(
        &mut f,
        "total_kvs",
        file.total_kvs.to_string(),
        file.total_kvs == 0,
    );
    field(
        &mut f,
        "total_bytes",
        file.total_bytes.to_string(),
        file.total_bytes == 0,
    );
    let metas: Vec<String> = file.table_metas.iter().map(table_meta).collect();
    field(
        &mut f,
        "table_metas",
        format!("[{}]", metas.join(",")),
        file.table_metas.is_empty(),
    );
    field(&mut f, "cf", string(&file.cf), file.cf.is_empty());
    field(&mut f, "size", file.size.to_string(), file.size == 0);
    field(
        &mut f,
        "cipher_iv",
        bytes(&file.cipher_iv),
        file.cipher_iv.is_empty(),
    );
    format!("{{{}}}", f.join(","))
}

/// Go `json.Marshal(*rtree.Range)`: `StartKey`, `EndKey` (the embedded
/// `KeyRange`) and `Files`.
#[must_use]
pub fn range(start_key: &[u8], end_key: &[u8], files: &[File]) -> String {
    let files = if files.is_empty() {
        "null".to_owned()
    } else {
        format!("[{}]", files.iter().map(file).collect::<Vec<_>>().join(","))
    };
    format!(
        r#"{{"StartKey":{},"EndKey":{},"Files":{files}}}"#,
        opt_bytes(start_key),
        opt_bytes(end_key)
    )
}

fn u64_of(v: &Json) -> u64 {
    v.as_u64().unwrap_or(0)
}

/// Go's decode of a `backuppb.File`.
///
/// # Errors
/// A malformed field.
pub fn parse_file(v: &Json) -> Result<File> {
    let table_metas = v["table_metas"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|m| TableMeta {
                    physical_id: m["physical_id"].as_i64().unwrap_or(0),
                    crc64xor: u64_of(&m["crc64xor"]),
                    total_kvs: u64_of(&m["total_kvs"]),
                    total_bytes: u64_of(&m["total_bytes"]),
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(File {
        name: v["name"].as_str().unwrap_or_default().to_owned(),
        sha256: parse_bytes(&v["sha256"])?,
        start_key: parse_bytes(&v["start_key"])?,
        end_key: parse_bytes(&v["end_key"])?,
        start_version: u64_of(&v["start_version"]),
        end_version: u64_of(&v["end_version"]),
        crc64xor: u64_of(&v["crc64xor"]),
        total_kvs: u64_of(&v["total_kvs"]),
        total_bytes: u64_of(&v["total_bytes"]),
        table_metas,
        cf: v["cf"].as_str().unwrap_or_default().to_owned(),
        size: u64_of(&v["size"]),
        cipher_iv: parse_bytes(&v["cipher_iv"])?,
    })
}

/// Go's decode of an `rtree.Range`.
///
/// # Errors
/// A malformed field.
pub fn parse_range(v: &Json) -> Result<(Vec<u8>, Vec<u8>, Vec<File>)> {
    let files = match &v["Files"] {
        Json::Array(a) => a.iter().map(parse_file).collect::<Result<Vec<_>>>()?,
        _ => Vec::new(),
    };
    Ok((
        parse_bytes(&v["StartKey"])?,
        parse_bytes(&v["EndKey"])?,
        files,
    ))
}
