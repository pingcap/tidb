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

//! Shared CREATE and ALTER table-option parsing.

use tidb_ast::TableOption;
use tidb_lexer::{canonical_charset, canonical_collation, TokenKind};

use crate::{decode_string, prec, PResult, Parser};

impl Parser {
    /// Parses one typed branch of Go's shared `parseTableOption`
    /// production. A `None` means this is not a table option and leaves the
    /// token untouched for the caller's next grammar production. Neither
    /// CREATE TABLE nor CREATE SEQUENCE may discard it.
    pub(crate) fn parse_table_option(&mut self) -> PResult<Option<TableOption>> {
        let had_default = if self.is_kw("DEFAULT") {
            let next = self.peek_n(1).text.to_ascii_uppercase();
            if !matches!(
                next.as_str(),
                "CHARACTER" | "CHAR" | "CHARSET" | "COLLATE" | "ENCRYPTION" | "PLACEMENT"
            ) {
                return Ok(None);
            }
            self.bump();
            true
        } else {
            false
        };
        let keyword = self.peek().text.to_ascii_uppercase();

        macro_rules! integer_option {
            ($variant:path) => {{
                self.bump();
                self.accept_optional_equals();
                let value = self.parse_table_option_integer(&keyword)?;
                Some($variant(value))
            }};
        }
        macro_rules! string_option {
            ($variant:path) => {{
                self.bump();
                self.accept_optional_equals();
                let value = self.parse_table_option_string(&keyword)?;
                Some($variant(value))
            }};
        }

        let option = match keyword.as_str() {
            "ENGINE" if !had_default => {
                self.bump();
                self.accept_optional_equals();
                Some(TableOption::Engine(self.parse_table_option_word()?))
            }
            "AUTO_INCREMENT" if !had_default => integer_option!(TableOption::AutoIncrement),
            "CHARACTER" | "CHAR" => {
                self.bump();
                self.expect_kw("SET")?;
                self.accept_optional_equals();
                let raw = self.parse_table_option_word()?;
                let charset = canonical_charset(&raw)
                    .ok_or_else(|| self.err_here("unknown character set"))?;
                Some(TableOption::CharacterSet(charset.to_ascii_uppercase()))
            }
            "CHARSET" => {
                self.bump();
                self.accept_optional_equals();
                let raw = self.parse_table_option_word()?;
                let charset = canonical_charset(&raw)
                    .ok_or_else(|| self.err_here("unknown character set"))?;
                Some(TableOption::CharacterSet(charset.to_ascii_uppercase()))
            }
            "COLLATE" => {
                self.bump();
                self.accept_optional_equals();
                let raw = self.parse_table_option_word()?;
                let collation =
                    canonical_collation(&raw).ok_or_else(|| self.err_here("unknown collation"))?;
                Some(TableOption::Collate(collation.to_ascii_uppercase()))
            }
            "ENCRYPTION" => {
                self.bump();
                self.accept_optional_equals();
                let value = self.parse_table_option_string("ENCRYPTION")?;
                if !matches!(value.as_str(), "Y" | "y" | "N" | "n") {
                    return Err(self.err_here("ENCRYPTION value must be Y or N"));
                }
                Some(TableOption::Encryption(value))
            }
            "PLACEMENT" => {
                self.bump();
                if self.is_kw("POLICY") {
                    self.bump();
                }
                let value = if self.is_kw("SET") && self.is_kw_at(1, "DEFAULT") {
                    self.bump();
                    self.bump();
                    "DEFAULT".to_string()
                } else if self.is_kw("SET") {
                    return Err(self.err_here("expected DEFAULT after PLACEMENT POLICY SET"));
                } else {
                    self.accept_optional_equals();
                    if self.is_kw("DEFAULT") {
                        self.bump();
                        "DEFAULT".to_string()
                    } else {
                        self.parse_placement_policy_name()?
                    }
                };
                Some(TableOption::PlacementPolicy(value))
            }
            _ if had_default => {
                return Err(self.err_here(
                    "expected CHARACTER SET/CHARSET/COLLATE/ENCRYPTION/PLACEMENT POLICY after DEFAULT",
                ));
            }
            "COMMENT" => string_option!(TableOption::Comment),
            "ROW_FORMAT" => {
                self.bump();
                self.accept_optional_equals();
                Some(TableOption::RowFormat(
                    self.parse_table_option_word()?.to_ascii_uppercase(),
                ))
            }
            "KEY_BLOCK_SIZE" => integer_option!(TableOption::KeyBlockSize),
            "COMPRESSION" => string_option!(TableOption::Compression),
            "STORAGE" => {
                self.bump();
                if self.is_kw("ENGINE") {
                    self.bump();
                    self.accept_optional_equals();
                    Some(TableOption::Engine(self.parse_table_option_word()?))
                } else if self.is_kw("DISK") || self.is_kw("MEMORY") {
                    Some(TableOption::StorageMedia(
                        self.bump().text.to_ascii_uppercase(),
                    ))
                } else {
                    return Err(self.err_here("expected ENGINE, DISK, or MEMORY after STORAGE"));
                }
            }
            "TABLESPACE" => {
                self.bump();
                self.accept_optional_equals();
                Some(TableOption::Tablespace(self.parse_table_option_word()?))
            }
            "SHARD_ROW_ID_BITS" => integer_option!(TableOption::ShardRowIdBits),
            "PRE_SPLIT_REGIONS" => integer_option!(TableOption::PreSplitRegions),
            "AUTO_ID_CACHE" => integer_option!(TableOption::AutoIdCache),
            "MAX_ROWS" => integer_option!(TableOption::MaxRows),
            "MIN_ROWS" => integer_option!(TableOption::MinRows),
            "AVG_ROW_LENGTH" => integer_option!(TableOption::AvgRowLength),
            "CHECKSUM" => integer_option!(TableOption::Checksum),
            "DELAY_KEY_WRITE" => integer_option!(TableOption::DelayKeyWrite),
            "STATS_PERSISTENT" | "PACK_KEYS" => {
                self.bump();
                self.accept_optional_equals();
                if self.is_kw("DEFAULT") || self.peek().kind == TokenKind::IntLit {
                    self.bump();
                } else {
                    return Err(
                        self.err_here(&format!("expected DEFAULT or an integer after {keyword}"))
                    );
                }
                Some(if keyword == "STATS_PERSISTENT" {
                    TableOption::StatsPersistent
                } else {
                    TableOption::PackKeys
                })
            }
            "AUTO_RANDOM_BASE" => integer_option!(TableOption::AutoRandomBase),
            "NODEGROUP" => integer_option!(TableOption::Nodegroup),
            "AUTOEXTEND_SIZE" => {
                self.bump();
                self.accept_optional_equals();
                // Go accepts the unit-bearing identifier forms used by
                // MySQL/MariaDB (for example `4M`) in addition to plain
                // integer/decimal literals and preserves the spelling.
                Some(TableOption::AutoextendSize(
                    self.parse_table_option_size_value("AUTOEXTEND_SIZE")?,
                ))
            }
            "PAGE_CHECKSUM" => integer_option!(TableOption::PageChecksum),
            "PAGE_COMPRESSED" => integer_option!(TableOption::PageCompressed),
            "PAGE_COMPRESSION_LEVEL" => integer_option!(TableOption::PageCompressionLevel),
            "TRANSACTIONAL" => integer_option!(TableOption::Transactional),
            "IETF_QUOTES" => {
                self.bump();
                self.accept_optional_equals();
                Some(TableOption::IetfQuotes(self.parse_table_option_word()?))
            }
            "SEQUENCE" => integer_option!(TableOption::Sequence),
            "UNION" => {
                self.bump();
                self.accept_optional_equals();
                self.expect_op("(")?;
                let mut tables = Vec::new();
                if !self.is_op(")") {
                    loop {
                        tables.push(self.parse_name_path()?);
                        if !self.is_op(",") {
                            break;
                        }
                        self.bump();
                    }
                }
                self.expect_op(")")?;
                Some(TableOption::Union(tables))
            }
            "CONNECTION" => string_option!(TableOption::Connection),
            "PASSWORD" => string_option!(TableOption::Password),
            "STATS_AUTO_RECALC" => {
                self.bump();
                self.accept_optional_equals();
                let value = if self.is_kw("DEFAULT") {
                    self.bump();
                    "DEFAULT".to_string()
                } else {
                    self.parse_table_option_integer("STATS_AUTO_RECALC")?
                };
                if !matches!(value.as_str(), "DEFAULT" | "0" | "1") {
                    return Err(self.err_here("STATS_AUTO_RECALC accepts only DEFAULT, 0, or 1"));
                }
                Some(TableOption::StatsAutoRecalc(value))
            }
            "STATS_SAMPLE_PAGES" => {
                self.bump();
                self.accept_optional_equals();
                let value = if self.is_kw("DEFAULT") {
                    self.bump();
                    "DEFAULT".to_string()
                } else {
                    self.parse_table_option_integer("STATS_SAMPLE_PAGES")?
                };
                Some(TableOption::StatsSamplePages(value))
            }
            "DATA" | "INDEX" if self.is_kw_at(1, "DIRECTORY") => {
                let is_data = keyword == "DATA";
                self.bump();
                self.bump();
                self.accept_optional_equals();
                let value = self.parse_table_option_string("DATA/INDEX DIRECTORY")?;
                Some(if is_data {
                    TableOption::DataDirectory(value)
                } else {
                    TableOption::IndexDirectory(value)
                })
            }
            "INSERT_METHOD" => {
                self.bump();
                self.accept_optional_equals();
                Some(TableOption::InsertMethod(
                    self.parse_table_option_word()?.to_ascii_uppercase(),
                ))
            }
            "SECONDARY_ENGINE" => {
                self.bump();
                self.accept_optional_equals();
                if self.is_kw("NULL") {
                    self.bump();
                    Some(TableOption::SecondaryEngineNull)
                } else {
                    Some(TableOption::SecondaryEngine(
                        self.parse_table_option_word()?,
                    ))
                }
            }
            "SECONDARY_ENGINE_ATTRIBUTE" => {
                string_option!(TableOption::SecondaryEngineAttribute)
            }
            "ENGINE_ATTRIBUTE" => {
                self.bump();
                self.accept_optional_equals();
                Some(TableOption::EngineAttribute(
                    self.parse_table_option_word()?,
                ))
            }
            "TABLE_CHECKSUM" => integer_option!(TableOption::TableChecksum),
            "STATS_BUCKETS" => integer_option!(TableOption::StatsBuckets),
            "STATS_TOPN" => integer_option!(TableOption::StatsTopN),
            "STATS_SAMPLE_RATE" => {
                self.bump();
                self.accept_optional_equals();
                let token = self.peek().clone();
                if !matches!(
                    token.kind,
                    TokenKind::IntLit | TokenKind::FloatLit | TokenKind::DecLit
                ) {
                    return Err(self.err_here("expected a number after STATS_SAMPLE_RATE"));
                }
                self.bump();
                Some(TableOption::StatsSampleRate(token.text))
            }
            "STATS_COL_CHOICE" => string_option!(TableOption::StatsColChoice),
            "STATS_COL_LIST" => string_option!(TableOption::StatsColList),
            "TTL" => {
                self.bump();
                self.accept_optional_equals();
                let column = self.parse_name()?;
                self.expect_op("+")?;
                self.expect_kw("INTERVAL")?;
                let value = self.parse_expr(prec::NONE)?;
                if self.peek().kind != TokenKind::Keyword {
                    return Err(self.err_here("expected a TTL INTERVAL unit"));
                }
                let unit = self.bump().text.to_ascii_uppercase();
                Some(TableOption::Ttl {
                    column,
                    value: Box::new(value),
                    unit,
                })
            }
            "TTL_ENABLE" => {
                self.bump();
                self.accept_optional_equals();
                let value = self
                    .parse_table_option_string("TTL_ENABLE")?
                    .to_ascii_uppercase();
                let enabled = match value.as_str() {
                    "ON" => true,
                    "OFF" => false,
                    _ => return Err(self.err_here("TTL_ENABLE value must be ON or OFF")),
                };
                Some(TableOption::TtlEnable(enabled))
            }
            "TTL_JOB_INTERVAL" => {
                self.bump();
                self.accept_optional_equals();
                let value = self.parse_table_option_string("TTL_JOB_INTERVAL")?;
                if !valid_ttl_job_interval(&value) {
                    return Err(self.err_here("invalid TTL_JOB_INTERVAL"));
                }
                Some(TableOption::TtlJobInterval(value))
            }
            // Direct Go `parseTableOption` transition: `AFFINITY` takes an
            // optional equals sign followed by a string literal only. Do not
            // normalize or validate its level here; Go performs that later in
            // DDL while AST restore preserves the supplied literal spelling.
            "AFFINITY" => string_option!(TableOption::Affinity),
            _ => None,
        };
        Ok(option)
    }

    pub(super) fn parse_table_option_integer(&mut self, keyword: &str) -> PResult<String> {
        let token = self.peek().clone();
        if token.kind != TokenKind::IntLit {
            return Err(self.err_here(&format!("expected an integer after {keyword}")));
        }
        self.bump();
        Ok(token.text)
    }

    /// Parses Go's `AUTOEXTEND_SIZE` value (`identifier | intLit | decLit`).
    /// Unit-bearing identifiers such as `4M` must remain bare in the AST so
    /// restore does not turn them into a quoted string or lose the suffix.
    fn parse_table_option_size_value(&mut self, keyword: &str) -> PResult<String> {
        let token = self.peek().clone();
        if !matches!(
            token.kind,
            TokenKind::Ident | TokenKind::IntLit | TokenKind::DecLit
        ) {
            return Err(self.err_here(&format!("expected a size after {keyword}")));
        }
        self.bump();
        Ok(token.text)
    }

    pub(super) fn parse_table_option_string(&mut self, keyword: &str) -> PResult<String> {
        let token = self.peek().clone();
        if token.kind != TokenKind::Str {
            return Err(self.err_here(&format!("expected a string literal after {keyword}")));
        }
        self.bump();
        Ok(decode_string(&token.text))
    }

    /// Parses a table option's value: a bare word (an identifier or
    /// keyword, e.g. `InnoDB`, `MEMORY` — `MEMORY`/`BINARY` and similar
    /// engine/charset names lex as keywords, not identifiers) or an
    /// equivalent quoted string (`'InnoDB'`) — MySQL/TiDB accept either
    /// form for `ENGINE`/`CHARACTER SET`/`CHARSET`/`COLLATE`, both
    /// restoring identically (confirmed via `godump restore`, not assumed).
    pub(super) fn parse_table_option_word(&mut self) -> PResult<String> {
        match self.peek().kind {
            TokenKind::Ident | TokenKind::Keyword => Ok(self.bump().text),
            TokenKind::Str => Ok(decode_string(&self.bump().text)),
            _ => Err(self.err_here("expected a table option value")),
        }
    }
}

/// Validates a `TTL_JOB_INTERVAL` duration string, ported from
/// `pkg/parser/ddl_table_option_parser.go`'s
/// `parseTableOptionTTLJobInterval`: a leading `@` is rejected; the value
/// splits into a leading number (digits and `.`) plus a trimmed unit that
/// must be empty or one of the accepted set (case-insensitive); and the
/// number part may hold at most one `.`.
fn valid_ttl_job_interval(value: &str) -> bool {
    let val = value.trim();
    if val.starts_with('@') {
        return false;
    }
    let num_len = val
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(val.len());
    let unit = val[num_len..].trim();
    const UNITS: [&str; 14] = [
        "YEAR",
        "MONTH",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "MICROSECOND",
        "d",
        "h",
        "m",
        "s",
        "ms",
        "us",
        "ns",
    ];
    if !unit.is_empty() && !UNITS.iter().any(|u| u.eq_ignore_ascii_case(unit)) {
        return false;
    }
    val[..num_len].matches('.').count() <= 1
}
