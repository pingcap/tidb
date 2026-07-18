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

//! Complete structural translation of `pkg/parser/ddl_sequence_parser.go`.

use tidb_ast::{
    AlterInstanceStmt, AlterRangeStmt, AlterSequenceStmt, CreateSequenceStmt, DropSequenceStmt,
    PlacementOption, SequenceOption,
};
use tidb_lexer::TokenKind;

use crate::{decode_string, PResult, Parser};

impl Parser {
    /// Parses `CREATE SEQUENCE [IF NOT EXISTS] name [options...]`.
    pub(crate) fn parse_create_sequence(&mut self) -> PResult<CreateSequenceStmt> {
        self.expect_kw("CREATE")?;
        self.expect_kw("SEQUENCE")?;
        let if_not_exists = self.parse_if_not_exists()?;
        let name = self.parse_name_path()?;
        let mut options = Vec::new();
        let mut table_options = Vec::new();
        loop {
            if let Some(option) = self.parse_sequence_option()? {
                options.push(option);
            } else if let Some(option) = self.parse_table_option()? {
                table_options.push(option);
            } else {
                break;
            }
        }
        Ok(CreateSequenceStmt {
            if_not_exists,
            name,
            options,
            table_options,
        })
    }

    /// Parses `ALTER SEQUENCE [IF EXISTS] name option [option ...]`.
    pub(crate) fn parse_alter_sequence(&mut self) -> PResult<AlterSequenceStmt> {
        self.expect_kw("ALTER")?;
        self.expect_kw("SEQUENCE")?;
        let if_exists = self.parse_if_exists()?;
        let name = self.parse_name_path()?;
        let mut options = Vec::new();
        while let Some(option) = self.parse_alter_sequence_option()? {
            options.push(option);
        }
        if options.is_empty() {
            return Err(self.err_here("ALTER SEQUENCE requires at least one option"));
        }
        Ok(AlterSequenceStmt {
            if_exists,
            name,
            options,
        })
    }

    /// Parses `DROP SEQUENCE [IF EXISTS] name [, name2 ...]`.
    pub(crate) fn parse_drop_sequence(&mut self) -> PResult<DropSequenceStmt> {
        self.expect_kw("DROP")?;
        self.expect_kw("SEQUENCE")?;
        let if_exists = self.parse_if_exists()?;
        let mut names = Vec::new();
        loop {
            names.push(self.parse_name_path()?);
            if !self.is_op(",") {
                break;
            }
            self.bump();
        }
        Ok(DropSequenceStmt { if_exists, names })
    }

    /// Parses the only state constructible by Go's
    /// `parseAlterInstanceStmt`: `ALTER INSTANCE RELOAD TLS` plus its
    /// optional rollback qualifier.
    pub(crate) fn parse_alter_instance(&mut self) -> PResult<AlterInstanceStmt> {
        self.expect_kw("ALTER")?;
        self.expect_kw("INSTANCE")?;
        self.expect_keyword_literal("RELOAD")?;
        self.expect_keyword_literal("TLS")?;
        let no_rollback_on_error = if self.is_kw("NO") {
            self.bump();
            self.expect_kw("ROLLBACK")?;
            self.expect_kw("ON")?;
            self.expect_kw("ERROR")?;
            true
        } else {
            false
        };
        Ok(AlterInstanceStmt {
            no_rollback_on_error,
        })
    }

    /// Parses `ALTER RANGE name placement_option`.
    pub(crate) fn parse_alter_range(&mut self) -> PResult<AlterRangeStmt> {
        self.expect_kw("ALTER")?;
        self.expect_kw("RANGE")?;
        let range_token = self.peek().clone();
        if !matches!(range_token.kind, TokenKind::Ident | TokenKind::Keyword) {
            return Err(self.err_here("expected range name"));
        }
        self.bump();

        let placement = if self.is_kw("PLACEMENT") {
            self.bump();
            self.expect_kw("POLICY")?;
            if self.is_op("=") {
                self.bump();
            }
            let policy = self.peek().clone();
            if !matches!(policy.kind, TokenKind::Ident | TokenKind::Keyword) {
                return Err(self.err_here("expected placement policy name"));
            }
            self.bump();
            PlacementOption::Policy(policy.text)
        } else {
            self.parse_placement_option()?
                .ok_or_else(|| self.err_here("ALTER RANGE requires one placement option"))?
        };

        Ok(AlterRangeStmt {
            range_name: range_token.text,
            placement,
        })
    }

    /// Parses one ordinary sequence option, or leaves the token cursor
    /// untouched when the next token belongs to another production.
    fn parse_sequence_option(&mut self) -> PResult<Option<SequenceOption>> {
        let option = if self.is_kw("INCREMENT") {
            self.bump();
            if self.is_kw("BY") || self.is_op("=") {
                self.bump();
            }
            SequenceOption::IncrementBy(self.parse_sequence_integer()?)
        } else if self.is_kw("START") {
            self.bump();
            if self.is_kw("WITH") || self.is_op("=") {
                self.bump();
            }
            SequenceOption::StartWith(self.parse_sequence_integer()?)
        } else if self.is_kw("MINVALUE") {
            self.bump();
            if self.is_op("=") {
                self.bump();
            }
            SequenceOption::MinValue(self.parse_sequence_integer()?)
        } else if self.is_kw("NOMINVALUE") {
            self.bump();
            SequenceOption::NoMinValue
        } else if self.is_kw("NO") {
            self.bump();
            if self.is_kw("MINVALUE") {
                self.bump();
                SequenceOption::NoMinValue
            } else if self.is_kw("MAXVALUE") {
                self.bump();
                SequenceOption::NoMaxValue
            } else if self.is_kw("CACHE") {
                self.bump();
                SequenceOption::NoCache
            } else if self.is_kw("CYCLE") {
                self.bump();
                SequenceOption::NoCycle
            } else {
                return Err(self.err_here("expected MINVALUE, MAXVALUE, CACHE, or CYCLE after NO"));
            }
        } else if self.is_kw("MAXVALUE") {
            self.bump();
            if self.is_op("=") {
                self.bump();
            }
            SequenceOption::MaxValue(self.parse_sequence_integer()?)
        } else if self.is_kw("NOMAXVALUE") {
            self.bump();
            SequenceOption::NoMaxValue
        } else if self.is_kw("CACHE") {
            self.bump();
            if self.is_op("=") {
                self.bump();
            }
            SequenceOption::Cache(self.parse_sequence_integer()?)
        } else if self.is_kw("NOCACHE") {
            self.bump();
            SequenceOption::NoCache
        } else if self.is_kw("CYCLE") {
            self.bump();
            SequenceOption::Cycle
        } else if self.is_kw("NOCYCLE") {
            self.bump();
            SequenceOption::NoCycle
        } else {
            return Ok(None);
        };
        Ok(Some(option))
    }

    /// Extends [`Parser::parse_sequence_option`] with ALTER-only
    /// `RESTART [WITH | =] value`.
    fn parse_alter_sequence_option(&mut self) -> PResult<Option<SequenceOption>> {
        if let Some(option) = self.parse_sequence_option()? {
            return Ok(Some(option));
        }
        if !self.is_kw("RESTART") {
            return Ok(None);
        }
        self.bump();
        if self.is_kw("WITH") || self.is_op("=") {
            self.bump();
            Ok(Some(SequenceOption::RestartWith(
                self.parse_sequence_integer()?,
            )))
        } else {
            Ok(Some(SequenceOption::Restart))
        }
    }

    /// Parses Go's signed sequence integer without introducing a wider or
    /// narrower range at the translation boundary.
    fn parse_sequence_integer(&mut self) -> PResult<i64> {
        let negative = if self.is_op("-") {
            self.bump();
            true
        } else {
            if self.is_op("+") {
                self.bump();
            }
            false
        };
        let token = self.peek().clone();
        if token.kind != TokenKind::IntLit {
            return Err(self.err_here("expected an integer sequence-option value"));
        }
        self.bump();
        let value: u64 = token
            .text
            .parse()
            .map_err(|_| self.err_here("sequence-option value out of signed 64-bit range"))?;
        if negative {
            if value > 1_u64 << 63 {
                return Err(self.err_here("sequence-option value out of signed 64-bit range"));
            }
            Ok((value as i64).wrapping_neg())
        } else if value > i64::MAX as u64 {
            Err(self.err_here("sequence-option value out of signed 64-bit range"))
        } else {
            Ok(value as i64)
        }
    }

    /// Go accepts RELOAD/TLS as either their keyword token or a token whose
    /// literal spells that keyword. Preserve that boundary behavior.
    fn expect_keyword_literal(&mut self, keyword: &str) -> PResult<()> {
        let token = self.peek().clone();
        let matches = token.text.eq_ignore_ascii_case(keyword)
            || (token.kind == TokenKind::Str
                && decode_string(&token.text).eq_ignore_ascii_case(keyword));
        if !matches {
            return Err(self.err_here("expected keyword literal"));
        }
        self.bump();
        Ok(())
    }
}
