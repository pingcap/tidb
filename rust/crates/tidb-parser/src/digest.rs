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

//! SQL normalization and SHA-256 statement identity.
//!
//! This directly transcreates `pkg/parser/digester.go`. It deliberately works
//! on lexer tokens instead of the AST: normalization must remain useful for
//! malformed SQL and must preserve the Go scanner's exact spacing and token
//! reduction rules.

use sha2::{Digest as _, Sha256};
use tidb_lexer::{Lexer, Token, TokenKind};

/// Literal-redaction behavior accepted by [`normalize`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedactMode {
    /// Return the input byte-for-byte without lexing it.
    Disabled,
    /// Replace literal values with `?` and collapse literal lists.
    Enabled,
    /// Wrap each literal in `‹...›`, doubling marker delimiters in the value.
    Marker,
}

/// A fixed-length statement digest and its cached lowercase hexadecimal text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Digest {
    bytes: Vec<u8>,
    hex: String,
}

impl Digest {
    /// Creates a digest from caller-supplied bytes.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        let bytes = bytes.into();
        let mut hex = String::with_capacity(bytes.len() * 2);
        for byte in &bytes {
            use std::fmt::Write as _;
            write!(&mut hex, "{byte:02x}").expect("writing to String cannot fail");
        }
        Self { bytes, hex }
    }

    /// Returns the lowercase hexadecimal representation.
    pub fn as_str(&self) -> &str {
        &self.hex
    }

    /// Returns the digest bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl std::fmt::Display for Digest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.hex)
    }
}

/// Generates the digest of the enabled-redaction normalized SQL.
#[deprecated(note = "use normalize_digest")]
pub fn digest_hash(sql: &str) -> Digest {
    digest_normalized(&Normalizer::new(sql, false).normalize(RedactMode::Enabled, false, false))
}

/// Generates a digest from SQL that is already normalized.
pub fn digest_normalized(normalized: &str) -> Digest {
    Digest::new(Sha256::digest(normalized.as_bytes()).to_vec())
}

/// Normalizes SQL according to the requested redaction mode.
pub fn normalize(sql: &str, redact: RedactMode) -> String {
    if redact == RedactMode::Disabled {
        return sql.to_owned();
    }
    Normalizer::new(sql, false).normalize(redact, false, false)
}

/// Normalizes SQL with binding-specific list reduction rules.
pub fn normalize_for_binding(sql: &str, for_plan_replayer_reload: bool) -> String {
    Normalizer::new(sql, false).normalize(RedactMode::Enabled, true, for_plan_replayer_reload)
}

/// Normalizes SQL while retaining optimizer-hint comments.
pub fn normalize_keep_hint(sql: &str) -> String {
    Normalizer::new(sql, true).normalize(RedactMode::Enabled, false, false)
}

/// Normalizes SQL and computes its digest in one pass-equivalent operation.
pub fn normalize_digest(sql: &str) -> (String, Digest) {
    let normalized = normalize(sql, RedactMode::Enabled);
    let digest = digest_normalized(&normalized);
    (normalized, digest)
}

/// Normalizes SQL for binding and computes its digest.
pub fn normalize_digest_for_binding(sql: &str) -> (String, Digest) {
    let normalized = normalize_for_binding(sql, false);
    let digest = digest_normalized(&normalized);
    (normalized, digest)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Generic,
    GenericList,
    Ident,
    QuotedIdent,
    Charset,
    String,
    Number,
    Bit,
    Param,
    UserVar,
    Keyword,
    Hint,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalToken {
    kind: Kind,
    literal: String,
}

impl NormalToken {
    fn generic() -> Self {
        Self {
            kind: Kind::Generic,
            literal: "?".to_owned(),
        }
    }

    fn generic_list() -> Self {
        Self {
            kind: Kind::GenericList,
            literal: "...".to_owned(),
        }
    }
}

struct Normalizer<'a> {
    sql: &'a str,
    lexer: Lexer<'a>,
    keep_hint: bool,
    tokens: Vec<NormalToken>,
}

impl<'a> Normalizer<'a> {
    fn new(sql: &'a str, keep_hint: bool) -> Self {
        let mut lexer = if keep_hint {
            Lexer::new(sql).with_keep_hint()
        } else {
            Lexer::new(sql)
        };
        // `NewScanner` leaves window-function keyword recognition disabled;
        // the parser enables it separately, while the digester does not.
        lexer.set_support_window_func(false);
        Self {
            sql,
            lexer,
            keep_hint,
            tokens: Vec::new(),
        }
    }

    fn normalize(
        mut self,
        redact: RedactMode,
        for_binding: bool,
        for_plan_replayer: bool,
    ) -> String {
        loop {
            let source = self.lexer.next_token();
            if source.kind == TokenKind::Invalid {
                if self.sql[source.offset..].starts_with("/*+") {
                    self.tokens.push(NormalToken {
                        kind: Kind::Other,
                        literal: String::new(),
                    });
                }
                break;
            }
            if source.kind == TokenKind::Eof {
                break;
            }
            if source.offset == self.sql.len()
                || (source.offset + 1 == self.sql.len()
                    && self.sql.as_bytes()[source.offset] == b';')
            {
                break;
            }

            let mut current = self.normal_token(&source);
            if !self.keep_hint && self.reduce_optimizer_hint(&mut current) {
                continue;
            }
            self.reduce_literal(&mut current, redact, for_binding, for_plan_replayer);
            if for_plan_replayer {
                self.replace_single_literal_with_in_list(&current);
            } else if for_binding {
                self.reduce_in_list_with_single_literal(&current);
                self.reduce_in_row_list_with_single_literal(&current);
            }
            self.tokens.push(current);
        }

        self.tokens
            .iter()
            .map(|token| match token.kind {
                Kind::UserVar if token.literal.starts_with('@') => token.literal.clone(),
                Kind::Charset => "(_charset)".to_owned(),
                Kind::Ident | Kind::QuotedIdent => format!("`{}`", token.literal),
                _ => token.literal.clone(),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn normal_token(&self, token: &Token) -> NormalToken {
        let raw = &self.sql[token.offset..token.end_offset];
        let kind = match token.kind {
            TokenKind::Ident if raw.starts_with('`') || raw.starts_with('"') => Kind::QuotedIdent,
            TokenKind::Ident => Kind::Ident,
            TokenKind::CharsetIntroducer => Kind::Charset,
            TokenKind::Str => Kind::String,
            TokenKind::IntLit | TokenKind::FloatLit | TokenKind::DecLit | TokenKind::HexLit => {
                Kind::Number
            }
            TokenKind::BitLit => Kind::Bit,
            TokenKind::UserVar => Kind::UserVar,
            TokenKind::Keyword => Kind::Keyword,
            TokenKind::HintComment => Kind::Hint,
            TokenKind::Op if token.text == "?" => Kind::Param,
            _ => Kind::Other,
        };
        NormalToken {
            kind,
            literal: token.text.to_lowercase(),
        }
    }

    fn reduce_optimizer_hint(&mut self, token: &mut NormalToken) -> bool {
        if token.kind == Kind::Hint {
            return true;
        }
        if token.literal == "index"
            && self
                .tokens
                .last()
                .is_some_and(|last| matches!(last.literal.as_str(), "force" | "use" | "ignore"))
        {
            loop {
                let next = self.lexer.next_token();
                if matches!(next.kind, TokenKind::Invalid | TokenKind::Eof) {
                    break;
                }
                if next.text == ")" {
                    self.tokens.pop();
                    return true;
                }
            }
            return false;
        }
        if token.literal == "straight_join" {
            token.literal = "join".to_owned();
        }
        false
    }

    fn reduce_literal(
        &mut self,
        token: &mut NormalToken,
        redact: RedactMode,
        for_binding: bool,
        for_plan_replayer: bool,
    ) {
        if !self.is_literal(token) {
            return;
        }
        if redact == RedactMode::Marker && !for_binding && !for_plan_replayer {
            if matches!(token.literal.as_str(), "?" | "*") {
                return;
            }
            token.literal = format!("‹{}›", token.literal.replace('‹', "‹‹").replace('›', "››"));
            return;
        }
        if token.literal == "*" {
            if self.tokens.last().is_some_and(|last| last.literal == "(") {
                *token = NormalToken::generic();
            }
            return;
        }
        if token.kind == Kind::Number && self.is_prefixed_by_unary() {
            self.tokens.pop();
        }
        if self.is_generic_list() {
            self.tokens.truncate(self.tokens.len() - 2);
            *token = NormalToken::generic_list();
            return;
        }
        if let Some(to_pop) = self.generic_list_with_charset() {
            self.tokens.truncate(self.tokens.len() - to_pop);
            *token = NormalToken::generic_list();
            return;
        }
        if self.is_generic_lists() {
            self.tokens.truncate(self.tokens.len() - 4);
            *token = NormalToken::generic_list();
            return;
        }
        if for_binding && self.is_generic_row_lists_with_in() {
            self.tokens.truncate(self.tokens.len() - 5);
            *token = NormalToken::generic_list();
            return;
        }
        if token.kind == Kind::Number && self.is_order_or_group_by() {
            return;
        }
        *token = NormalToken::generic();
    }

    fn is_literal(&self, token: &NormalToken) -> bool {
        matches!(
            token.kind,
            Kind::Number | Kind::String | Kind::Bit | Kind::Param
        ) || token.literal == "*"
            || (token.kind == Kind::Keyword && token.literal == "null")
            || (token.kind == Kind::Ident && token.literal == "null")
    }

    fn is_prefixed_by_unary(&self) -> bool {
        let Some(last) = self.tokens.last() else {
            return false;
        };
        if !matches!(last.literal.as_str(), "-" | "+") {
            return false;
        }
        let Some(before) = self.tokens.iter().rev().nth(1) else {
            return true;
        };
        matches!(
            before.literal.as_str(),
            "(" | "," | "+" | "-" | ">=" | "is" | "<=" | "=" | "<" | ">" | "select"
        )
    }

    fn is_generic_list(&self) -> bool {
        let Some(last) = self.tokens.get(self.tokens.len().saturating_sub(2)..) else {
            return false;
        };
        last.len() == 2
            && matches!(last[0].kind, Kind::Generic | Kind::GenericList)
            && last[1].literal == ","
    }

    fn generic_list_with_charset(&self) -> Option<usize> {
        if self.tokens.len() < 4 {
            return None;
        }
        let last = &self.tokens[self.tokens.len() - 4..];
        let slice = &last[1..];
        let first_charset = last[0].kind == Kind::Charset;
        (slice[2].kind == Kind::Charset
            && matches!(slice[0].kind, Kind::Generic | Kind::GenericList)
            && slice[1].literal == ",")
            .then_some(3 + usize::from(first_charset))
    }

    fn is_generic_lists(&self) -> bool {
        let Some(last) = self.tokens.get(self.tokens.len().saturating_sub(4)..) else {
            return false;
        };
        last.len() == 4
            && matches!(last[0].kind, Kind::Generic | Kind::GenericList)
            && last[1].literal == ")"
            && last[2].literal == ","
            && last[3].literal == "("
    }

    fn is_generic_row_lists_with_in(&self) -> bool {
        let Some(last) = self.tokens.get(self.tokens.len().saturating_sub(9)..) else {
            return false;
        };
        last.len() >= 9
            && last[0].literal == "in"
            && last[1].literal == "("
            && last[2].literal == "row"
            && last[3].literal == "("
            && matches!(last[4].kind, Kind::Generic | Kind::GenericList)
            && last[5].literal == ")"
            && last[6].literal == ","
            && last[7].literal == "row"
            && last[8].literal == "("
    }

    fn replace_single_literal_with_in_list(&mut self, current: &NormalToken) {
        let Some(last) = self.tokens.get(self.tokens.len().saturating_sub(5)..) else {
            return;
        };
        if last.len() == 5
            && last[0].literal == "in"
            && last[1].literal == "("
            && last[2..].iter().all(|token| token.literal == ".")
            && current.literal == ")"
        {
            self.tokens.truncate(self.tokens.len() - 3);
            self.tokens.push(NormalToken::generic());
        }
    }

    fn reduce_in_list_with_single_literal(&mut self, current: &NormalToken) {
        let Some(last) = self.tokens.get(self.tokens.len().saturating_sub(3)..) else {
            return;
        };
        if last.len() == 3
            && last[0].literal == "in"
            && last[1].literal == "("
            && last[2].kind == Kind::Generic
            && current.literal == ")"
        {
            self.tokens.pop();
            self.tokens.push(NormalToken::generic_list());
        }
    }

    fn reduce_in_row_list_with_single_literal(&mut self, current: &NormalToken) {
        let Some(last) = self.tokens.get(self.tokens.len().saturating_sub(6)..) else {
            return;
        };
        if last.len() == 6
            && last[0].literal == "in"
            && last[1].literal == "("
            && last[2].literal == "row"
            && last[3].literal == "("
            && matches!(last[4].kind, Kind::Generic | Kind::GenericList)
            && last[5].literal == ")"
            && current.literal == ")"
        {
            self.tokens.truncate(self.tokens.len() - 4);
            self.tokens.push(NormalToken::generic_list());
        }
    }

    fn is_order_or_group_by(&self) -> bool {
        let mut n = 2;
        loop {
            if self.tokens.len() < n {
                return false;
            }
            let last = &self.tokens[self.tokens.len() - n..];
            if last[1].literal != "," {
                let (first, second) = if last[1].literal == "(" {
                    if self.tokens.len() < n + 1 {
                        return false;
                    }
                    let expanded = &self.tokens[self.tokens.len() - n - 1..];
                    (&expanded[0], &expanded[1])
                } else {
                    (&last[0], &last[1])
                };
                return matches!(first.literal.as_str(), "order" | "group")
                    && second.literal == "by";
            }
            n += 2;
        }
    }
}
