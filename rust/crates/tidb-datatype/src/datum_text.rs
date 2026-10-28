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


//! A datum's stored text, as Go reads a string column or a system
//! variable value (`chunk.Row.GetString`, `Datum.ToString`).

use crate::Datum;

/// The text form a system variable stores for a datum (Go keeps every system
/// variable as a string).
pub fn sysvar_datum_text(value: &Datum) -> Option<String> {
    match value {
        Datum::Null => None,
        Datum::Int(v) => Some(v.to_string()),
        Datum::UInt(v) => Some(v.to_string()),
        Datum::Real(v) => Some(v.to_string()),
        Datum::Decimal(d) => Some(d.to_string()),
        Datum::String(s) => Some(String::from_utf8_lossy(s.bytes()).into_owned()),
        Datum::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
        // `BinaryJSON.String`: the canonical document text a JSON column
        // sends on the wire.
        Datum::Json(j) => Some(j.to_string()),
        // `types.Time.String()`: the text a temporal column prints, fsp
        // included -- what `mysql.bind_info`'s TIMESTAMP(6) rows read back
        // as.
        Datum::Time(t) => Some(t.to_string()),
        // Go `Datum.ToString` for the two set-valued types prints the NAME,
        // not the bit value: an ENUM default read back as `''` instead of
        // `'x'` is what a metadata reader would otherwise report for a
        // column whose stored default is correct.
        Datum::Enum(value, _) => {
            Some(String::from_utf8_lossy(value.name().as_bytes()).into_owned())
        }
        Datum::Set(value, _) => Some(String::from_utf8_lossy(value.name().as_bytes()).into_owned()),
        Datum::Duration(value) => Some(value.to_string()),
        // Go `BinaryLiteral.ToString` is the raw bytes: `SET autocommit =
        // 0x30` is `'0'` and `SET sql_mode = 0x414E5349` is `'ANSI'`.
        Datum::BinaryLiteral(literal) | Datum::Bit(literal) => {
            Some(String::from_utf8_lossy(literal.as_bytes()).into_owned())
        }
        // Go `strconv.FormatFloat(float64(f), 'f', -1, 32)`.
        Datum::Float32(value) => Some(crate::go_strconv::format_float(
            *value, b'f', -1, 32,
        )),
        _ => None,
    }
}
