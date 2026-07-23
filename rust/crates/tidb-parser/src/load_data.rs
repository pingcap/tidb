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

//! Direct translation of `pkg/parser/ddl_load_data_parser.go`.

use tidb_ast::{
    ColumnOrUserVar, LoadDataFields, LoadDataLines, LoadDataOnDuplicate, LoadDataOption,
    LoadDataStmt,
};
use tidb_lexer::{canonical_charset, TokenKind};

use crate::{decode_string, PResult, Parser};

impl Parser {
    /// Parses the complete Go-owned `LOAD DATA` grammar.
    ///
    /// Reading client/server/external files and coordinating TiDB's import
    /// pipeline remain an executor concern. The seed executor rejects this
    /// typed statement before transaction or catalog mutation.
    pub(crate) fn parse_load_data(&mut self) -> PResult<LoadDataStmt> {
        self.expect_kw("LOAD")?;
        self.expect_kw("DATA")?;
        let low_priority = if self.is_kw("LOW_PRIORITY") {
            self.bump();
            true
        } else {
            false
        };
        let local = if self.is_kw("LOCAL") {
            self.bump();
            true
        } else {
            false
        };
        self.expect_kw("INFILE")?;
        let path = self.parse_load_string_literal()?;
        let format = if self.is_kw("FORMAT") {
            self.bump();
            Some(self.parse_load_string_literal()?)
        } else {
            None
        };
        let mut on_duplicate = if self.is_kw("REPLACE") {
            self.bump();
            LoadDataOnDuplicate::Replace
        } else if self.is_kw("IGNORE") {
            self.bump();
            LoadDataOnDuplicate::Ignore
        } else {
            LoadDataOnDuplicate::Error
        };
        if local && on_duplicate == LoadDataOnDuplicate::Error {
            on_duplicate = LoadDataOnDuplicate::Ignore;
        }
        self.expect_kw("INTO")?;
        self.expect_kw("TABLE")?;
        let table = self.parse_name_path()?;

        let charset = if self.is_kw("CHARACTER") {
            self.bump();
            self.expect_kw("SET")?;
            let name = self.parse_charset_name()?;
            Some(
                canonical_charset(&name)
                    .map(str::to_owned)
                    .ok_or_else(|| self.err_here("unknown LOAD DATA character set"))?,
            )
        } else {
            None
        };

        let mut fields = LoadDataFields::default();
        let mut lines = LoadDataLines::default();
        loop {
            if self.is_kw("FIELDS") || self.is_kw("COLUMNS") {
                self.bump();
                fields = self.parse_fields_clause(true)?;
            } else if self.is_kw("LINES") {
                self.bump();
                lines = self.parse_lines_clause()?;
            } else {
                break;
            }
        }

        let ignore_lines = if self.is_kw("IGNORE") && self.peek_n(1).kind == TokenKind::IntLit {
            self.bump();
            let number = self
                .bump()
                .text
                .parse::<u64>()
                .map_err(|_| self.err_here("LOAD DATA IGNORE line count is out of range"))?;
            self.expect_kw("LINES")?;
            Some(number)
        } else {
            None
        };

        let columns_and_user_vars = if self.is_op("(") {
            self.bump();
            let mut columns = Vec::new();
            while !self.is_op(")") {
                if self.peek().kind == TokenKind::UserVar {
                    let token = self.bump();
                    let Some(name) = token.text.strip_prefix('@') else {
                        return Err(self.err_here("expected LOAD DATA user variable"));
                    };
                    if name.starts_with('@') {
                        return Err(self.err_here("expected a single-@ LOAD DATA user variable"));
                    }
                    columns.push(ColumnOrUserVar::UserVar(crate::decode_at_name(&token.text)));
                } else {
                    columns.push(ColumnOrUserVar::Column(self.parse_name_or_keyword()?));
                }
                if self.is_op(",") {
                    self.bump();
                } else {
                    break;
                }
            }
            self.expect_op(")")?;
            columns
        } else {
            Vec::new()
        };

        let column_assignments = if self.is_kw("SET") {
            self.bump();
            let mut assignments = vec![self.parse_assignment(true)?];
            while self.is_op(",") {
                self.bump();
                assignments.push(self.parse_assignment(true)?);
            }
            assignments
        } else {
            Vec::new()
        };

        let options = if self.is_kw("WITH") {
            self.bump();
            let mut options = vec![self.parse_load_data_option()?];
            while self.is_op(",") {
                self.bump();
                options.push(self.parse_load_data_option()?);
            }
            options
        } else {
            Vec::new()
        };

        Ok(LoadDataStmt {
            low_priority,
            local,
            path,
            format,
            on_duplicate,
            table,
            charset,
            fields,
            lines,
            ignore_lines,
            columns_and_user_vars,
            column_assignments,
            options,
        })
    }

    pub(crate) fn parse_fields_clause(&mut self, load_data_mode: bool) -> PResult<LoadDataFields> {
        let mut fields = LoadDataFields::default();
        loop {
            if self.is_kw("TERMINATED") {
                self.bump();
                self.expect_kw("BY")?;
                fields.terminated = Some(self.parse_load_string_value()?);
            } else if self.is_kw("OPTIONALLY") {
                self.bump();
                self.expect_kw("ENCLOSED")?;
                self.expect_kw("BY")?;
                let value = self.parse_load_string_value()?;
                self.validate_load_field_separator(&value)?;
                fields.enclosed = Some(value);
                fields.optionally_enclosed = true;
            } else if self.is_kw("ENCLOSED") {
                self.bump();
                self.expect_kw("BY")?;
                let value = self.parse_load_string_value()?;
                self.validate_load_field_separator(&value)?;
                fields.enclosed = Some(value);
            } else if self.is_kw("ESCAPED") {
                self.bump();
                self.expect_kw("BY")?;
                let value = self.parse_load_string_value()?;
                self.validate_load_field_separator(&value)?;
                fields.escaped = Some(value);
            } else if load_data_mode && self.is_kw("DEFINED") {
                self.bump();
                self.expect_kw("NULL")?;
                self.expect_kw("BY")?;
                fields.defined_null_by = Some(self.parse_load_string_value()?);
                if self.is_kw("OPTIONALLY") {
                    self.bump();
                    if self.is_kw("ENCLOSED") {
                        self.bump();
                    }
                    fields.null_optionally_enclosed = true;
                }
            } else {
                break;
            }
        }
        Ok(fields)
    }

    pub(crate) fn parse_lines_clause(&mut self) -> PResult<LoadDataLines> {
        let mut lines = LoadDataLines::default();
        loop {
            if self.is_kw("STARTING") {
                self.bump();
                self.expect_kw("BY")?;
                lines.starting = Some(self.parse_load_string_value()?);
            } else if self.is_kw("TERMINATED") {
                self.bump();
                self.expect_kw("BY")?;
                lines.terminated = Some(self.parse_load_string_value()?);
            } else {
                break;
            }
        }
        Ok(lines)
    }

    fn parse_load_data_option(&mut self) -> PResult<LoadDataOption> {
        let token = self.peek().clone();
        if !matches!(token.kind, TokenKind::Ident | TokenKind::Keyword) {
            return Err(self.err_here("expected LOAD DATA option name"));
        }
        self.bump();
        let value = if self.is_op("=") || self.is_op(":=") {
            self.bump();
            Some(self.parse_import_signed_literal()?)
        } else {
            None
        };
        Ok(LoadDataOption {
            name: token.text.to_ascii_lowercase(),
            value,
        })
    }

    fn parse_load_string_literal(&mut self) -> PResult<String> {
        if self.peek().kind != TokenKind::Str {
            return Err(self.err_here("expected LOAD DATA string literal"));
        }
        Ok(decode_string(&self.bump().text))
    }

    /// Go's `parseStringValue`: a regular quoted string, hexadecimal, or bit
    /// literal becomes the delimiter's string value before AST restore.
    fn parse_load_string_value(&mut self) -> PResult<String> {
        let token = self.peek().clone();
        match token.kind {
            TokenKind::Str => {
                self.bump();
                Ok(decode_string(&token.text))
            }
            TokenKind::HexLit => {
                self.bump();
                decode_load_hex_literal(&token.text)
                    .ok_or_else(|| self.err_here("invalid LOAD DATA hex delimiter"))
            }
            TokenKind::BitLit => {
                self.bump();
                decode_load_bit_literal(&token.text)
                    .ok_or_else(|| self.err_here("invalid LOAD DATA bit delimiter"))
            }
            _ => Err(self.err_here("expected LOAD DATA string value")),
        }
    }

    fn validate_load_field_separator(&self, value: &str) -> PResult<()> {
        if value != "\\" && value.len() > 1 {
            return Err(self.err_here(
                "[parser:1083]Field separator argument is not what is expected; check the manual",
            ));
        }
        Ok(())
    }
}

/// Decodes Go's `ast.BinaryLiteral.ToString()` result for `X'..'` / `0x..`.
fn decode_load_hex_literal(text: &str) -> Option<String> {
    let digits = text
        .strip_prefix("0x")
        .or_else(|| text.strip_prefix("0X"))
        .map(str::to_owned)
        .unwrap_or_else(|| text[1..].trim_matches('\'').to_owned());
    let digits = if digits.len().is_multiple_of(2) {
        digits
    } else {
        format!("0{digits}")
    };
    let bytes: Option<Vec<u8>> = (0..digits.len())
        .step_by(2)
        .map(|offset| u8::from_str_radix(&digits[offset..offset + 2], 16).ok())
        .collect();
    String::from_utf8(bytes?).ok()
}

/// Decodes Go's `ast.BinaryLiteral.ToString()` result for `B'..'` / `0b..`.
fn decode_load_bit_literal(text: &str) -> Option<String> {
    let bits = text
        .strip_prefix("0b")
        .or_else(|| text.strip_prefix("0B"))
        .map(str::to_owned)
        .unwrap_or_else(|| text[1..].trim_matches('\'').to_owned());
    if bits.is_empty() || bits.bytes().any(|byte| byte != b'0' && byte != b'1') {
        return None;
    }
    let padding = (8 - bits.len() % 8) % 8;
    let padded = format!("{}{}", "0".repeat(padding), bits);
    let bytes: Option<Vec<u8>> = padded
        .as_bytes()
        .chunks_exact(8)
        .map(|chunk| {
            std::str::from_utf8(chunk)
                .ok()
                .and_then(|digits| u8::from_str_radix(digits, 2).ok())
        })
        .collect();
    String::from_utf8(bytes?).ok()
}
