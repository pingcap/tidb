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

//! Go `pkg/parser/ddl_fieldtype_parser.go` AST payload and restore boundary.

use crate::util::escape_string_literal;

/// One parenthesized field-type argument.
///
/// Numeric precision/scale/length arguments are text. ENUM/SET binary literal
/// members remain raw bytes because Go's parser/restore contract can preserve
/// octets that are not valid UTF-8 (for example GBK `0x91`). Keeping that
/// distinction in the AST avoids forcing invalid data through `String`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnTypeArg {
    /// A UTF-8 argument or string-literal member.
    Text(String),
    /// A binary ENUM/SET member decoded from a hex or bit literal.
    Bytes(Vec<u8>),
}

impl ColumnTypeArg {
    /// Creates a text argument without exposing the enum constructor at call sites.
    pub fn text(value: impl Into<String>) -> Self {
        Self::Text(value.into())
    }

    /// Returns text for numeric arguments, or a lossy view for a binary member.
    pub fn as_text_lossy(&self) -> String {
        match self {
            Self::Text(value) => value.clone(),
            Self::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
        }
    }

    /// Appends the source bytes of this argument to a lossless restore sink.
    pub(crate) fn restore_bytes(&self, out: &mut Vec<u8>) {
        match self {
            Self::Text(value) => push_escaped_bytes(value.as_bytes(), out),
            Self::Bytes(value) => push_escaped_bytes(value, out),
        }
    }
}

/// Escapes only the quote/backslash bytes that would terminate an ENUM/SET
/// string member. All other bytes, including invalid UTF-8, are preserved.
fn push_escaped_bytes(value: &[u8], out: &mut Vec<u8>) {
    for byte in value {
        match byte {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\'' => out.extend_from_slice(b"''"),
            byte => out.push(*byte),
        }
    }
}

/// A column's declared type: its name (restored uppercase, e.g. `INT`,
/// `VARCHAR`, `DECIMAL`) and parenthesized arguments (precision/scale or
/// length, as original digit text).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnType {
    /// The canonical (uppercase) type name.
    pub name: String,
    /// The parenthesized arguments, if any.
    pub args: Vec<ColumnTypeArg>,
    /// Final `UNSIGNED` state after ordered `UNSIGNED`/`SIGNED`/`ZEROFILL`
    /// modifiers. A later `SIGNED` clears this without clearing `ZEROFILL`.
    pub unsigned: bool,
    /// Whether `ZEROFILL` was specified.
    pub zerofill: bool,
    /// Go's `mysql.BinaryFlag` when it is a string-type modifier rather than
    /// the intrinsic representation of `BINARY`/`VARBINARY`/the BLOB family.
    /// It restores before a declared character set.
    pub binary: bool,
    /// `CHARACTER SET name` / `CHARSET name`, if present.
    pub charset: Option<String>,
}

impl ColumnType {
    /// Applies TiDB's parser-time IEEE precision normalization for one
    /// `FLOAT(p)` argument before restore.
    pub fn normalize_float_precision(&mut self) {
        if self.name != "FLOAT" || self.args.len() != 1 {
            return;
        }
        let Ok(precision) = self.args[0].as_text_lossy().parse::<u32>() else {
            return;
        };
        if precision <= 24 {
            self.args.clear();
        } else if precision <= 53 {
            self.name = "DOUBLE".to_string();
            self.args.clear();
        }
    }

    /// Restores the canonical Go `FieldType` spelling for a column owner.
    pub(super) fn restore_into(&self, out: &mut String) {
        out.push_str(&self.name);
        if !self.args.is_empty() {
            out.push('(');
            if self.name == "ENUM" || self.name == "SET" {
                for (index, value) in self.args.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push('\'');
                    out.push_str(&escape_string_literal(&value.as_text_lossy()));
                    out.push('\'');
                }
            } else {
                for (index, value) in self.args.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str(&value.as_text_lossy());
                }
            }
            out.push(')');
        }
        if self.unsigned {
            out.push_str(" UNSIGNED");
        }
        if self.zerofill {
            out.push_str(" ZEROFILL");
        }
        if self.binary {
            out.push_str(" BINARY");
        }
        if !matches!(self.name.as_str(), "ENUM" | "SET") {
            if let Some(charset) = &self.charset {
                out.push_str(" CHARACTER SET ");
                out.push_str(charset);
            }
        }
    }

    /// Appends this field type to a byte-preserving restore sink.
    pub(crate) fn restore_into_bytes(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.name.as_bytes());
        if !self.args.is_empty() {
            out.push(b'(');
            if self.name == "ENUM" || self.name == "SET" {
                for (index, value) in self.args.iter().enumerate() {
                    if index > 0 {
                        out.push(b',');
                    }
                    out.push(b'\'');
                    value.restore_bytes(out);
                    out.push(b'\'');
                }
            } else {
                for (index, value) in self.args.iter().enumerate() {
                    if index > 0 {
                        out.push(b',');
                    }
                    out.extend_from_slice(value.as_text_lossy().as_bytes());
                }
            }
            out.push(b')');
        }
        if self.unsigned {
            out.extend_from_slice(b" UNSIGNED");
        }
        if self.zerofill {
            out.extend_from_slice(b" ZEROFILL");
        }
        if self.binary {
            out.extend_from_slice(b" BINARY");
        }
        if !matches!(self.name.as_str(), "ENUM" | "SET") {
            if let Some(charset) = &self.charset {
                out.extend_from_slice(b" CHARACTER SET ");
                out.extend_from_slice(charset.as_bytes());
            }
        }
    }
}
