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

//! Field-type parsing shared by CREATE and ALTER column definitions.

use tidb_ast::{ColumnType, ColumnTypeArg};
use tidb_lexer::TokenKind;

use crate::{decode_string, PResult, Parser};

impl Parser {
    /// Parses a column type: `INT`/`INTEGER`/`BIGINT`/`VARCHAR`/`CHAR`/
    /// `DECIMAL`/`NUMERIC`/`TEXT`/the direct BLOB/TEXT families/`JSON`/`BIT`/`VECTOR`, optionally followed by
    /// `(args...)`. Numeric options use the same ordered state transition as
    /// Go's `parseIntegerOptions`: `UNSIGNED` sets the flag, `SIGNED` clears
    /// it, and `ZEROFILL` sets both flags. `BIT` deliberately never accepts
    /// them (see below).
    pub(super) fn parse_column_type(&mut self) -> PResult<ColumnType> {
        let accepts_double_precision = self.is_kw("DOUBLE") || self.is_kw("FLOAT8");
        let char_type = self.is_kw("CHAR") || self.is_kw("CHARACTER") || self.is_kw("NCHAR");
        let national_type = self.is_kw("NATIONAL");
        let long_type = self.is_kw("LONG");
        // MariaDB's UUID pseudo-type is accepted only when the compatibility
        // mode is enabled. Go's branch materializes it as CHAR(36), rather
        // than carrying a distinct type through the AST.
        let uuid_type = self.is_kw("UUID");
        let (mut name, boolean_alias, year_type, mut requires_type_arg) =
            if self.is_kw("TINYINT") || self.is_kw("INT1") {
                ("TINYINT", false, false, false)
            } else if self.is_kw("SMALLINT") || self.is_kw("INT2") {
                ("SMALLINT", false, false, false)
            } else if self.is_kw("MEDIUMINT") || self.is_kw("INT3") || self.is_kw("MIDDLEINT") {
                ("MEDIUMINT", false, false, false)
            } else if self.is_kw("INT") || self.is_kw("INTEGER") || self.is_kw("INT4") {
                ("INT", false, false, false)
            } else if self.is_kw("BIGINT") || self.is_kw("INT8") {
                ("BIGINT", false, false, false)
            } else if self.is_kw("FLOAT") || self.is_kw("FLOAT4") {
                ("FLOAT", false, false, false)
            } else if self.is_kw("DOUBLE") || self.is_kw("FLOAT8") {
                ("DOUBLE", false, false, false)
            } else if self.is_kw("REAL") {
                // `REAL_AS_FLOAT` is a session SQL-mode concern. The parser's
                // default mode, like Go's default `parseFieldType` branch,
                // canonicalizes REAL to DOUBLE.
                ("DOUBLE", false, false, false)
            } else if self.is_kw("VARCHAR") {
                ("VARCHAR", false, false, false)
            } else if self.is_kw("NVARCHAR") {
                // Go's NVARCHAR alias is a mandatory-length VARCHAR.
                ("VARCHAR", false, false, true)
            } else if char_type {
                ("CHAR", false, false, false)
            } else if national_type {
                // NATIONAL is a prefix, not a standalone field type. The
                // following CHAR/CHARACTER/VARCHAR/VARCHARACTER spelling is
                // resolved below after the prefix token is consumed.
                ("NATIONAL", false, false, false)
            } else if self.is_kw("BINARY") {
                ("BINARY", false, false, false)
            } else if self.is_kw("VARBINARY") {
                ("VARBINARY", false, false, false)
            } else if self.is_kw("DECIMAL") || self.is_kw("NUMERIC") || self.is_kw("FIXED") {
                ("DECIMAL", false, false, false)
            } else if self.is_kw("TEXT") {
                ("TEXT", false, false, false)
            // Go's `parseFieldType` distinguishes the byte-oriented BLOB
            // family from the character-oriented TEXT family even though their
            // internal type codes overlap.  This AST keeps the restored spelling
            // directly, which is sufficient for the base forms here.  Charset
            // rewriting (`TEXT BYTE`, trailing `BINARY`, etc.) needs a distinct
            // field-type representation and deliberately stays outside this
            // translation slice.
            } else if self.is_kw("TINYBLOB") {
                ("TINYBLOB", false, false, false)
            } else if self.is_kw("BLOB") {
                ("BLOB", false, false, false)
            } else if self.is_kw("MEDIUMBLOB") {
                ("MEDIUMBLOB", false, false, false)
            } else if self.is_kw("LONGBLOB") {
                ("LONGBLOB", false, false, false)
            } else if self.is_kw("TINYTEXT") {
                ("TINYTEXT", false, false, false)
            } else if self.is_kw("MEDIUMTEXT") {
                ("MEDIUMTEXT", false, false, false)
            } else if self.is_kw("LONGTEXT") {
                ("LONGTEXT", false, false, false)
            } else if long_type {
                // Go's LONG branch resolves the following compatibility spelling
                // after consuming LONG. The normal form differs only by storage
                // family: LONG VARBINARY/BYTE is binary MEDIUMBLOB; every other
                // accepted LONG spelling is character MEDIUMTEXT.
                ("MEDIUMTEXT", false, false, false)
            } else if self.is_kw("JSON") {
                ("JSON", false, false, false)
            } else if self.is_kw("VECTOR") {
                ("VECTOR", false, false, false)
            } else if self.is_kw("SPATIAL")
                || self.is_kw("POINT")
                || geometry_type_name(self.peek()).is_some()
            {
                // Go's `spatial, point` branch and its identifier-based
                // geometry aliases all materialize the same TypeGeometry
                // payload.  Keep the canonical restore spelling here rather
                // than retaining the source alias (`POINT`, `POLYGON`, ...).
                ("GEOMETRY", false, false, false)
            } else if self.is_kw("DATE") {
                ("DATE", false, false, false)
            } else if self.is_kw("DATETIME") {
                ("DATETIME", false, false, false)
            } else if self.is_kw("TIME") {
                ("TIME", false, false, false)
            } else if self.is_kw("TIMESTAMP") {
                ("TIMESTAMP", false, false, false)
            } else if self.is_kw("YEAR") || self.is_kw("SQL_TSI_YEAR") {
                ("YEAR", false, true, false)
            } else if self.is_kw("BIT") {
                ("BIT", false, false, false)
            } else if self.is_kw("ENUM") {
                ("ENUM", false, false, false)
            } else if self.is_kw("SET") {
                ("SET", false, false, false)
            } else if self.is_kw("BOOL") || self.is_kw("BOOLEAN") {
                ("TINYINT", true, false, false)
            } else if uuid_type {
                if !self.enable_mariadb {
                    return Err(self.err_here("UUID type requires MariaDB compatibility mode"));
                }
                ("CHAR", false, false, false)
            } else {
                return Err(self.err_here("expected column type"));
            };
        self.bump();
        // UUID has no parenthesized length in Go's MariaDB compatibility
        // branch; reject `UUID(...)` instead of accidentally treating it as
        // a user-supplied CHAR length after the alias normalization.
        if uuid_type && self.is_op("(") {
            return Err(self.err_here("UUID type does not accept a length"));
        }
        // Go's `resolveCharVarchar` promotes CHAR/CHARACTER when the next
        // token is one of its VARCHAR aliases. That grammar requires a field
        // length, unlike ordinary CHAR's optional length.
        if national_type {
            // Go's `parseFieldType` accepts NATIONAL CHAR/CHARACTER with an
            // optional length, and NATIONAL VARCHAR/VARCHARACTER with a
            // required length. NATIONAL CHAR VARYING promotes to VARCHAR.
            if self.is_kw("CHAR") || self.is_kw("CHARACTER") {
                self.bump();
                if self.is_kw("VARYING") || self.is_kw("VARCHAR") || self.is_kw("VARCHARACTER") {
                    self.bump();
                    name = "VARCHAR";
                    requires_type_arg = true;
                } else {
                    name = "CHAR";
                }
            } else if self.is_kw("VARCHAR") || self.is_kw("VARCHARACTER") {
                self.bump();
                name = "VARCHAR";
                requires_type_arg = true;
            } else {
                return Err(self.err_here(
                    "NATIONAL must be followed by CHAR, CHARACTER, VARCHAR, or VARCHARACTER",
                ));
            }
        } else if char_type
            && (self.is_kw("VARYING") || self.is_kw("VARCHAR") || self.is_kw("VARCHARACTER"))
        {
            self.bump();
            name = "VARCHAR";
            requires_type_arg = true;
        }
        // Direct port of Go's LONG compatibility branch. `LONG CHAR` alone
        // is intentionally not consumed; only its `... CHAR VARYING` form
        // is a LONG spelling, exactly like `parseFieldType`.
        if long_type {
            if self.is_kw("VARBINARY") || self.is_kw("BYTE") {
                self.bump();
                name = "MEDIUMBLOB";
            } else if self.is_kw("VARCHAR") || self.is_kw("VARCHARACTER") {
                self.bump();
            } else if (self.is_kw("CHAR") || self.is_kw("CHARACTER")) && self.is_kw_at(1, "VARYING")
            {
                self.bump();
                self.bump();
            }
        }
        // The yacc production accepts PRECISION only after DOUBLE/FLOAT8;
        // `REAL PRECISION` remains invalid, exactly as Go's separate REAL
        // branch leaves PRECISION unconsumed.
        if accepts_double_precision && self.is_kw("PRECISION") {
            self.bump();
        }
        let mut args: Vec<ColumnTypeArg> = Vec::new();
        // Go's VECTOR field-type production accepts an optional element-type
        // spelling only before its optional dimension. FLOAT/FLOAT4 is the
        // canonical Float32 vector and restores as bare VECTOR; DOUBLE/FLOAT8
        // is deliberately a parse error rather than a second vector type.
        if name == "VECTOR" && self.is_op("<") {
            self.bump();
            if self.is_kw("FLOAT") || self.is_kw("FLOAT4") {
                self.bump();
            } else {
                return Err(self.err_here("only VECTOR<FLOAT> is supported"));
            }
            self.expect_op(">")?;
        }
        // `ENUM`/`SET` take a REQUIRED parenthesized list of string-literal
        // members (not the numeric precision/length every other type uses).
        // Go's `parseEnumSetOptions` decodes and right-trims ordinary string
        // members before storing them; keep that canonical payload so the
        // AST restore re-quotes the same values (`ENUM('B','C')`). Binary
        // literal members use Go's `parseEnumSetOptions` byte decoding before
        // they enter the FieldType element list. Keep that decoded payload in
        // the typed field-type argument AST; this covers the source's
        // ASCII/control byte values (for example `b'10101'` -> `\x15`) and
        // preserves invalid bytes for `Stmt::restore_bytes()` without giving
        // the generic numeric-literal parser a second enum/set path.
        if name == "ENUM" || name == "SET" {
            self.expect_op("(")?;
            loop {
                let token = self.peek().clone();
                let value = match token.kind {
                    TokenKind::Str => {
                        self.bump();
                        ColumnTypeArg::text(decode_string(&token.text).trim_end_matches(' '))
                    }
                    TokenKind::HexLit | TokenKind::BitLit => {
                        self.bump();
                        ColumnTypeArg::Bytes(
                            decode_enum_set_binary_literal(&token.text, token.kind).ok_or_else(
                                || self.err_here("invalid binary literal in ENUM/SET"),
                            )?,
                        )
                    }
                    _ => return Err(self.err_here("expected a string literal in ENUM/SET")),
                };
                args.push(value);
                if !self.is_op(",") {
                    break;
                }
                self.bump();
            }
            self.expect_op(")")?;
        } else if boolean_alias {
            // Go's BOOL/BOOLEAN branch materializes TINYINT(1), and unlike
            // the ordinary integer branch does not accept a display width.
            args.push(ColumnTypeArg::text("1"));
        } else if matches!(
            name,
            "JSON"
                | "TINYBLOB"
                | "MEDIUMBLOB"
                | "LONGBLOB"
                | "TINYTEXT"
                | "MEDIUMTEXT"
                | "LONGTEXT"
        ) && self.is_op("(")
        {
            return Err(self.err_here("column type does not allow type arguments"));
        } else if name == "VECTOR" && self.is_op("(") {
            self.bump();
            args.push(ColumnTypeArg::text(self.parse_type_arg()?));
            self.expect_op(")")?;
        } else if self.is_op("(") {
            self.bump();
            args.push(ColumnTypeArg::text(self.parse_type_arg()?));
            if !matches!(name, "BINARY" | "VARBINARY") {
                while self.is_op(",") {
                    self.bump();
                    args.push(ColumnTypeArg::text(self.parse_type_arg()?));
                }
            }
            self.expect_op(")")?;
        } else if requires_type_arg || name == "VARBINARY" {
            // TiDB's field-type grammar requires a declared length for
            // VARBINARY and CHAR/CHARACTER VARYING, unlike BINARY and
            // ordinary CHAR whose length is optional.
            return Err(self.err_here("column type requires a length"));
        } else if name == "BIT" {
            // A bare `BIT` (no explicit length) defaults to `BIT(1)` —
            // confirmed via `godump restore`: `BIT` alone restores as
            // `BIT(1)`, the default length materialized explicitly into
            // the AST rather than left implicit, unlike every other
            // type here (whose own missing-`(...)` case just stays an
            // empty `args`).
            args.push(ColumnTypeArg::text("1"));
        }
        if uuid_type {
            // Go's UUID branch sets the fixed CHAR(36) length itself and does
            // not run the ordinary CHAR length parser.
            args.push(ColumnTypeArg::text("36"));
        }
        // Go's default parser configuration enables strict double-type
        // checking: a single-argument DOUBLE precision is a syntax error
        // (`DOUBLE(10)`), while the `(M,D)` form remains valid. The parser
        // carries no mutable SQL-mode state yet, so keep the public `parse`
        // path on that default and reject the ambiguous one-argument form at
        // the shared field-type boundary used by CREATE and ALTER.
        if name == "DOUBLE" && args.len() == 1 {
            return Err(self.err_here("DOUBLE requires precision and scale"));
        }
        let supports_numeric_options = matches!(
            name,
            "TINYINT"
                | "SMALLINT"
                | "MEDIUMINT"
                | "INT"
                | "BIGINT"
                | "FLOAT"
                | "DOUBLE"
                | "DECIMAL"
        );
        let (unsigned, zerofill) = if supports_numeric_options {
            self.parse_integer_options()
        } else if year_type {
            // `parseFieldType` consumes the same modifier stream after a
            // YEAR type on a scratch FieldType. The flags are intentionally
            // absent from YEAR's stored/restored payload.
            let _ = self.parse_integer_options();
            (false, false)
        } else {
            (false, false)
        };
        let mut ty = ColumnType {
            name: name.to_string(),
            args,
            unsigned,
            zerofill,
            binary: false,
            charset: None,
        };
        // `pkg/parser/ddl_fieldtype_parser.go:parseFieldType` resolves a
        // single-argument FLOAT precision before the AST is restored. Keep
        // this at the shared column-type boundary: CREATE/ALTER/generated
        // columns all consume the same normalized type instead of carrying
        // a generated-column special case.
        ty.normalize_float_precision();
        Ok(ty)
    }

    /// Direct Rust translation of Go's `parseIntegerOptions`: the final
    /// state, rather than the first spelling seen, is the AST contract.
    fn parse_integer_options(&mut self) -> (bool, bool) {
        let mut unsigned = false;
        let mut zerofill = false;
        loop {
            if self.is_kw("UNSIGNED") {
                self.bump();
                unsigned = true;
            } else if self.is_kw("SIGNED") {
                self.bump();
                unsigned = false;
            } else if self.is_kw("ZEROFILL") {
                self.bump();
                zerofill = true;
                unsigned = true;
            } else {
                return (unsigned, zerofill);
            }
        }
    }

    /// Parses one type argument (a precision/scale/length): a plain integer.
    fn parse_type_arg(&mut self) -> PResult<String> {
        if self.peek().kind == TokenKind::IntLit {
            Ok(self.bump().text)
        } else {
            Err(self.err_here("expected integer type argument"))
        }
    }
}

/// Decodes the binary/hex member forms accepted by Go's
/// `parseEnumSetOptions`. The lexer keeps both `0x61`/`x'61'` and
/// `0b01100001`/`b'01100001'` as one token, so this helper only normalizes the
/// token spelling and validates the resulting byte stream. The bytes are kept
/// raw so invalid UTF-8 can be written by the lossless AST restore sink.
fn decode_enum_set_binary_literal(text: &str, kind: TokenKind) -> Option<Vec<u8>> {
    let digits = match kind {
        TokenKind::HexLit => text
            .strip_prefix("0x")
            .or_else(|| text.strip_prefix("0X"))
            .or_else(|| text.strip_prefix("x'").and_then(|s| s.strip_suffix('\'')))
            .or_else(|| text.strip_prefix("X'").and_then(|s| s.strip_suffix('\'')))?,
        TokenKind::BitLit => text
            .strip_prefix("0b")
            .or_else(|| text.strip_prefix("0B"))
            .or_else(|| text.strip_prefix("b'").and_then(|s| s.strip_suffix('\'')))
            .or_else(|| text.strip_prefix("B'").and_then(|s| s.strip_suffix('\'')))?,
        _ => return None,
    };
    if digits.is_empty() {
        return None;
    }
    let bytes = if kind == TokenKind::HexLit {
        let digits = if digits.len() % 2 == 0 {
            digits.to_owned()
        } else {
            format!("0{digits}")
        };
        let mut bytes = Vec::with_capacity(digits.len() / 2);
        for pair in digits.as_bytes().chunks_exact(2) {
            let high = (pair[0] as char).to_digit(16)? as u8;
            let low = (pair[1] as char).to_digit(16)? as u8;
            bytes.push((high << 4) | low);
        }
        bytes
    } else {
        let pad = (8 - digits.len() % 8) % 8;
        let mut padded = String::with_capacity(pad + digits.len());
        padded.push_str(&"0".repeat(pad));
        padded.push_str(digits);
        let mut bytes = Vec::with_capacity(padded.len() / 8);
        for chunk in padded.as_bytes().chunks_exact(8) {
            let mut byte = 0u8;
            for bit in chunk {
                byte = (byte << 1)
                    | match bit {
                        b'0' => 0,
                        b'1' => 1,
                        _ => return None,
                    };
            }
            bytes.push(byte);
        }
        bytes
    };
    Some(bytes)
}

/// The BLOB family is binary by construction in Go's `FieldType` grammar;
/// unlike TEXT, it cannot carry a character-set clause.  Keeping this helper
/// beside `parse_column_type` makes the two halves of that direct translation
/// use the same closed type set.
pub(super) fn type_rejects_charset(name: &str) -> bool {
    matches!(
        name,
        "JSON"
            | "VECTOR"
            | "GEOMETRY"
            | "BINARY"
            | "VARBINARY"
            | "TINYBLOB"
            | "BLOB"
            | "MEDIUMBLOB"
            | "LONGBLOB"
    )
}

/// Go's geometry aliases are lexed as ordinary identifiers (except the
/// reserved `POINT`/`SPATIAL` spellings handled directly in the parser).  The
/// hand parser's `geometryTypeNames` map recognizes these six names and
/// restores every one as the single `GEOMETRY` FieldType spelling.
fn geometry_type_name(token: &tidb_lexer::Token) -> Option<&'static str> {
    if !matches!(token.kind, TokenKind::Ident | TokenKind::Keyword) {
        return None;
    }
    match token.text.to_ascii_uppercase().as_str() {
        "LINESTRING" => Some("LINESTRING"),
        "POLYGON" => Some("POLYGON"),
        "MULTIPOINT" => Some("MULTIPOINT"),
        "MULTILINESTRING" => Some("MULTILINESTRING"),
        "MULTIPOLYGON" => Some("MULTIPOLYGON"),
        "GEOMETRYCOLLECTION" => Some("GEOMETRYCOLLECTION"),
        _ => None,
    }
}

/// The exact `parseStringOptions` family in Go. Do not infer this from
/// `type_rejects_charset`: numeric types also reject a character-set clause,
/// but their grammar never owns string modifiers in the first place.
pub(super) fn type_supports_string_options(name: &str) -> bool {
    matches!(
        name,
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "ENUM" | "SET"
    )
}

/// Converts Go's `FieldType` binary-character-set normal form into this
/// spelling-preserving AST. `parseStringOptions` does not preserve a separate
/// `CHARACTER SET binary` payload: it turns character families into their
/// binary storage type, and enum/set silently retain their native spelling.
pub(super) fn normalize_binary_charset(ty: &mut ColumnType) {
    let replacement = match ty.name.as_str() {
        "CHAR" => Some("BINARY"),
        "VARCHAR" => Some("VARBINARY"),
        "TINYTEXT" => Some("TINYBLOB"),
        "TEXT" => Some("BLOB"),
        "MEDIUMTEXT" => Some("MEDIUMBLOB"),
        "LONGTEXT" => Some("LONGBLOB"),
        "ENUM" | "SET" => None,
        _ => return,
    };
    if let Some(name) = replacement {
        ty.name = name.to_owned();
    }
    ty.binary = false;
    ty.charset = None;
}
