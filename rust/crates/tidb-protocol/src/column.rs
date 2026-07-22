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

use crate::result::append_length_encoded_bytes;

/// Maximum byte length advertised for a column name by TiDB's server.
pub const MAX_COLUMN_NAME_SIZE: usize = 256;

/// MySQL `utf8mb4_bin`, TiDB's default metadata collation.
pub const DEFAULT_COLLATION_ID: u16 = 46;

/// MySQL `binary` collation.
pub const BINARY_DEFAULT_COLLATION_ID: u16 = 63;

/// Maximum advertised width for a LONG BLOB field.
pub const MAX_LONG_BLOB_WIDTH: u32 = 4_294_967_295;

/// MySQL field-type constants used by column metadata.
/// CHAR/STRING.
pub const TYPE_STRING: u8 = 0xfe;
/// VARCHAR.
pub const TYPE_VARCHAR: u8 = 15;
/// VAR_STRING.
pub const TYPE_VAR_STRING: u8 = 0xfd;
/// BIT.
pub const TYPE_BIT: u8 = 16;
/// Legacy NEWDATE.
pub const TYPE_NEW_DATE: u8 = 14;
/// JSON.
pub const TYPE_JSON: u8 = 0xf5;
/// ENUM.
pub const TYPE_ENUM: u8 = 0xf7;
/// SET.
pub const TYPE_SET: u8 = 0xf8;
/// TINY_BLOB.
pub const TYPE_TINY_BLOB: u8 = 0xf9;
/// MEDIUM_BLOB.
pub const TYPE_MEDIUM_BLOB: u8 = 0xfa;
/// LONG_BLOB.
pub const TYPE_LONG_BLOB: u8 = 0xfb;
/// BLOB.
pub const TYPE_BLOB: u8 = 0xfc;
/// TiDB's vector-float32 extension type.
pub const TYPE_TIDB_VECTOR_FLOAT32: u8 = 0xe1;

/// MySQL field flags used by `ColumnInfo`.
pub const BINARY_FLAG: u16 = 1 << 7;
/// ENUM field flag.
pub const ENUM_FLAG: u16 = 1 << 8;
/// SET field flag.
pub const SET_FLAG: u16 = 1 << 11;

/// A default value emitted by `COM_FIELD_LIST` column metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnDefault {
    /// Emit the protocol `NULL` marker.
    Null,
    /// Go TiDB treats `CURRENT_TIMESTAMP` as a protocol NULL default.
    CurrentTimestamp,
    /// Go TiDB treats `CURRENT_DATE` as a protocol NULL default.
    CurrentDate,
    /// Emit these bytes as a length-encoded default string.
    Bytes(Vec<u8>),
    /// Emit UTF-8 text as a length-encoded default string.
    Text(String),
}

/// The serialization-relevant fields of a text-protocol column definition.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ColumnInfo {
    /// Database/schema name.
    pub schema: String,
    /// Display table name.
    pub table: String,
    /// Original table name.
    pub org_table: String,
    /// Display column name.
    pub name: String,
    /// Original column name.
    pub org_name: String,
    /// Maximum field width advertised to the client.
    pub column_length: u32,
    /// Column charset/collation identifier.
    pub charset: u16,
    /// MySQL field flags.
    pub flag: u16,
    /// Number of decimal places.
    pub decimal: u8,
    /// MySQL field type code.
    pub type_code: u8,
    /// Optional default used by `dump_with_default`.
    pub default_value: Option<ColumnDefault>,
}

impl ColumnInfo {
    /// Appends a column definition without a default value.
    pub fn dump(&self, buffer: &mut Vec<u8>) {
        self.dump_inner(buffer, false);
    }

    /// Appends a column definition with a length-encoded default value.
    pub fn dump_with_default(&self, buffer: &mut Vec<u8>) {
        self.dump_inner(buffer, true);
    }

    fn dump_inner(&self, buffer: &mut Vec<u8>, with_default: bool) {
        append_length_encoded_bytes(buffer, Some(b"def"));
        // The source implementation truncates only `Name` and `OrgName` for
        // old-client compatibility.  Schema and table identifiers are sent
        // in full; applying the 256-byte alias limit to them changes the
        // metadata packet even though it looks superficially symmetric.
        append_length_encoded_bytes(buffer, Some(self.schema.as_bytes()));
        append_length_encoded_bytes(buffer, Some(self.table.as_bytes()));
        append_length_encoded_bytes(buffer, Some(self.org_table.as_bytes()));
        append_length_encoded_bytes(buffer, Some(self.truncate_name(self.name.as_bytes())));
        append_length_encoded_bytes(buffer, Some(self.truncate_name(self.org_name.as_bytes())));

        buffer.push(0x0c);
        buffer.extend_from_slice(&self.dump_charset().to_le_bytes());
        buffer.extend_from_slice(&self.dump_length().to_le_bytes());
        buffer.push(dump_type(self.type_code));
        buffer.extend_from_slice(&dump_flag(self.type_code, self.flag).to_le_bytes());
        buffer.push(self.decimal);
        buffer.extend_from_slice(&[0, 0]);

        if with_default {
            match &self.default_value {
                None
                | Some(ColumnDefault::Null)
                | Some(ColumnDefault::CurrentTimestamp)
                | Some(ColumnDefault::CurrentDate) => append_length_encoded_bytes(buffer, None),
                Some(ColumnDefault::Bytes(bytes)) => {
                    append_length_encoded_bytes(buffer, Some(bytes));
                }
                Some(ColumnDefault::Text(text)) => {
                    append_length_encoded_bytes(buffer, Some(text.as_bytes()));
                }
            }
        }
    }

    fn truncate_name<'a>(&self, name: &'a [u8]) -> &'a [u8] {
        let mut end = name.len().min(MAX_COLUMN_NAME_SIZE);
        // Go slices the UTF-8 bytes before passing them through its metadata
        // encoder.  Rust's `String` must stay valid UTF-8, so retain the
        // largest valid prefix instead of panicking when the byte limit lands
        // inside a multi-byte code point.
        while end > 0 && std::str::from_utf8(&name[..end]).is_err() {
            end -= 1;
        }
        &name[..end]
    }

    fn dump_charset(&self) -> u16 {
        if self.type_code == TYPE_TIDB_VECTOR_FLOAT32 {
            DEFAULT_COLLATION_ID
        } else {
            self.charset
        }
    }

    fn dump_length(&self) -> u32 {
        if self.type_code == TYPE_TIDB_VECTOR_FLOAT32 {
            MAX_LONG_BLOB_WIDTH
        } else {
            self.column_length
        }
    }
}

/// Appends a column definition without a default value.
pub fn dump_column(buffer: &mut Vec<u8>, column: &ColumnInfo) {
    column.dump(buffer);
}

/// Appends a column definition including its default value.
pub fn dump_column_with_default(buffer: &mut Vec<u8>, column: &ColumnInfo) {
    column.dump_with_default(buffer);
}

/// Returns the metadata flags exposed for a MySQL field type.
pub fn dump_flag(type_code: u8, flag: u16) -> u16 {
    match type_code {
        TYPE_SET => flag | SET_FLAG,
        TYPE_ENUM => flag | ENUM_FLAG,
        TYPE_TIDB_VECTOR_FLOAT32 => flag & !BINARY_FLAG,
        _ => flag,
    }
}

/// Returns the type code advertised in metadata.
pub fn dump_type(type_code: u8) -> u8 {
    match type_code {
        TYPE_SET | TYPE_ENUM => TYPE_STRING,
        TYPE_TIDB_VECTOR_FLOAT32 => TYPE_LONG_BLOB,
        TYPE_TINY_BLOB | TYPE_MEDIUM_BLOB | TYPE_LONG_BLOB => TYPE_BLOB,
        _ => type_code,
    }
}
