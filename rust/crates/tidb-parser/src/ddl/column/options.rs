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

//! Shared `CREATE`/`ALTER TABLE` column-option dispatcher.
//!
//! This is the source-owned Rust leaf for Go
//! `HandParser.parseColumnOptions`. It dispatches to the narrower default,
//! generated/time, CHECK, reference, and inline-key leaves rather than
//! copying their grammar into a broad table parser.

use tidb_ast::{AutoRandomOption, ColumnFormat, ColumnOption, ColumnStorage, InlineKeyOption};
use tidb_lexer::{canonical_collation, TokenKind};

use crate::{decode_string, is_ident_like_name, PResult, Parser};

impl Parser {
    /// Direct Go port of `HandParser.parseColumnOptions`, after the column
    /// type is consumed. The ordered loop is intentional: duplicate and
    /// repeated options remain AST-visible exactly as Go leaves them.
    pub(super) fn parse_column_options(&mut self, column_type: &str) -> PResult<Vec<ColumnOption>> {
        let mut options = Vec::new();
        loop {
            let option = if self.is_kw("NOT") {
                self.bump();
                self.expect_kw("NULL")?;
                ColumnOption::NotNull
            } else if self.is_kw("NULL") {
                self.bump();
                ColumnOption::Null
            } else if self.is_kw("PRIMARY") || self.is_kw("KEY") || self.is_kw("UNIQUE") {
                self.parse_inline_key_option()?
            } else if self.is_kw("AUTO_INCREMENT") {
                self.bump();
                ColumnOption::AutoIncrement
            } else if self.is_kw("SERIAL") {
                // Go's option-level `SERIAL DEFAULT VALUE` is a macro that
                // injects these two entries before the current UNIQUE entry.
                // Keep their written source order rather than collapsing it
                // into a synthetic type or execution-only flag.
                self.bump();
                self.expect_kw("DEFAULT")?;
                self.expect_kw("VALUE")?;
                options.push(ColumnOption::NotNull);
                options.push(ColumnOption::AutoIncrement);
                ColumnOption::InlineKey(InlineKeyOption::unique(false))
            } else if self.is_kw("DEFAULT") {
                self.bump();
                ColumnOption::Default(self.parse_column_default_expression()?)
            } else if self.is_kw("AS") || self.is_kw("GENERATED") {
                self.parse_generated_or_mariadb_row_option()?
            } else if self.is_kw("ON") {
                self.bump();
                self.expect_kw("UPDATE")?;
                ColumnOption::OnUpdate(self.parse_on_update_expr()?)
            } else if self.is_kw("COMMENT") {
                self.bump();
                let token = self.peek().clone();
                if token.kind != TokenKind::Str {
                    return Err(self.err_here("expected a string literal after COMMENT"));
                }
                self.bump();
                ColumnOption::Comment(decode_string(&token.text))
            } else if self.is_kw("SECONDARY_ENGINE_ATTRIBUTE") {
                self.bump();
                self.accept_optional_equals();
                let token = self.peek().clone();
                if token.kind != TokenKind::Str {
                    return Err(
                        self.err_here("expected a string literal after SECONDARY_ENGINE_ATTRIBUTE")
                    );
                }
                self.bump();
                ColumnOption::SecondaryEngineAttribute(decode_string(&token.text))
            } else if self.is_kw("COLLATE") {
                if column_type == "JSON" {
                    return Err(self.err_here("JSON does not allow COLLATE"));
                }
                // Go's yacc action checks `HasCollateOption` only in the
                // base/first-option production.  Consequently a duplicate
                // COLLATE is rejected when the first option is COLLATE, but
                // remains source-valid when an earlier option (for example
                // NOT NULL) precedes both COLLATE clauses.  Consume and
                // validate the collation name before reporting the duplicate,
                // matching Go's post-name error boundary.
                let duplicate_first_option =
                    matches!(options.first(), Some(ColumnOption::Collate(_)));
                self.bump();
                // Go's ColumnOption `CollationName` accepts StringName, so a
                // quoted collation (for example `COLLATE 'binary'`) follows
                // the same restore path as its bare identifier spelling.
                let name = if self.peek().kind == TokenKind::Str {
                    decode_string(&self.bump().text)
                } else {
                    self.parse_charset_name()?
                };
                let collation =
                    canonical_collation(&name).ok_or_else(|| self.err_here("unknown collation"))?;
                if duplicate_first_option {
                    return Err(self.err_here("Multiple COLLATE clauses"));
                }
                ColumnOption::Collate(collation.to_owned())
            } else if self.is_kw("CHECK") {
                self.bump();
                let (check, injected_not_null) = self.parse_check_constraint(None, true)?;
                options.push(ColumnOption::Check(check));
                if injected_not_null {
                    options.push(ColumnOption::NotNull);
                }
                continue;
            } else if self.is_kw("CONSTRAINT")
                && (self.is_kw_at(1, "CHECK")
                    || (is_ident_like_name(self.peek_n(1)) && self.is_kw_at(2, "CHECK")))
            {
                // A non-CHECK `CONSTRAINT` belongs to the enclosing table
                // grammar. Do not consume it merely because a column option
                // loop happens to be active.
                self.bump();
                let name = if self.is_kw("CHECK") {
                    None
                } else {
                    Some(self.bump().text)
                };
                self.expect_kw("CHECK")?;
                let (check, injected_not_null) = self.parse_check_constraint(name, true)?;
                options.push(ColumnOption::Check(check));
                if injected_not_null {
                    options.push(ColumnOption::NotNull);
                }
                continue;
            } else if self.is_kw("REFERENCES") {
                ColumnOption::Reference(self.parse_foreign_key_reference()?)
            } else if self.is_kw("COLUMN_FORMAT") {
                self.bump();
                ColumnOption::ColumnFormat(self.parse_column_format()?)
            } else if self.is_kw("STORAGE") {
                self.bump();
                // Go records a warning after this parse. Rust deliberately
                // has no parser warning channel, so retain the exact AST
                // payload without inventing a separate runtime side effect.
                ColumnOption::Storage(self.parse_column_storage()?)
            } else if self.is_kw("AUTO_RANDOM") {
                self.bump();
                ColumnOption::AutoRandom(self.parse_auto_random_option()?)
            } else {
                break;
            };
            options.push(option);
        }
        self.validate_generated_column_options(&options)?;
        Ok(options)
    }

    fn parse_column_format(&mut self) -> PResult<ColumnFormat> {
        if self.is_kw("DEFAULT") {
            self.bump();
            Ok(ColumnFormat::Default)
        } else if self.is_kw("FIXED") {
            self.bump();
            Ok(ColumnFormat::Fixed)
        } else if self.is_kw("DYNAMIC") {
            self.bump();
            Ok(ColumnFormat::Dynamic)
        } else {
            Err(self.err_here("expected DEFAULT, FIXED, or DYNAMIC after COLUMN_FORMAT"))
        }
    }

    fn parse_column_storage(&mut self) -> PResult<ColumnStorage> {
        if self.is_kw("DEFAULT") {
            self.bump();
            Ok(ColumnStorage::Default)
        } else if self.is_kw("DISK") {
            self.bump();
            Ok(ColumnStorage::Disk)
        } else if self.is_kw("MEMORY") {
            self.bump();
            Ok(ColumnStorage::Memory)
        } else {
            Err(self.err_here("expected DEFAULT, DISK, or MEMORY after STORAGE"))
        }
    }

    fn parse_auto_random_option(&mut self) -> PResult<AutoRandomOption> {
        let mut option = AutoRandomOption {
            shard_bits: None,
            range_bits: None,
        };
        if !self.is_op("(") {
            return Ok(option);
        }
        self.bump();
        option.shard_bits = Some(self.parse_auto_random_bits("shard bits")?);
        if self.is_op(",") {
            self.bump();
            option.range_bits = Some(self.parse_auto_random_bits("range bits")?);
        }
        self.expect_op(")")?;
        Ok(option)
    }

    fn parse_auto_random_bits(&mut self, name: &str) -> PResult<u64> {
        let token = self.peek().clone();
        if token.kind != TokenKind::IntLit {
            return Err(self.err_here(&format!("expected AUTO_RANDOM {name} integer")));
        }
        self.bump();
        token
            .text
            .parse()
            .map_err(|_| self.err_here("AUTO_RANDOM integer is out of range"))
    }
}
