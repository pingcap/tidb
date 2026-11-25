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

use crate::auth::sha256_hash;
use std::borrow::Cow;
use tidb_lexer::{BorrowedToken, Lexer, TokenKind, decode_quoted_string};

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
        let hex = tidb_goruntime::go_hex::encode_to_string(&bytes);
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
    Digest::new(sha256_hash(normalized.as_bytes()).to_vec())
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
    Integer,
    Number,
    Bit,
    NullLiteral,
    Param,
    UserVar,
    Keyword,
    Hint,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalToken<'a> {
    kind: Kind,
    literal: Cow<'a, str>,
}

impl NormalToken<'_> {
    fn generic() -> Self {
        Self {
            kind: Kind::Generic,
            literal: Cow::Borrowed("?"),
        }
    }

    fn generic_list() -> Self {
        Self {
            kind: Kind::GenericList,
            literal: Cow::Borrowed("..."),
        }
    }

    fn rendered_len(&self) -> usize {
        match self.kind {
            Kind::Charset => "(_charset)".len(),
            Kind::Ident | Kind::QuotedIdent => self.literal.len() + 2,
            _ => self.literal.len(),
        }
    }
}

struct Normalizer<'a> {
    sql: &'a str,
    lexer: Lexer<'a>,
    keep_hint: bool,
    tokens: Vec<NormalToken<'a>>,
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
            tokens: Vec::with_capacity((sql.len() / 3).min(32)),
        }
    }

    fn normalize(
        mut self,
        redact: RedactMode,
        for_binding: bool,
        for_plan_replayer: bool,
    ) -> String {
        loop {
            let source = self.lexer.next_token_borrowed();
            if source.kind == TokenKind::Invalid {
                if self.sql[source.offset..].starts_with("/*+") {
                    self.tokens.push(NormalToken {
                        kind: Kind::Other,
                        literal: Cow::Borrowed(""),
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

            let mut current = self.normal_token(source);
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

        let mut output = String::with_capacity(
            self.tokens
                .iter()
                .map(NormalToken::rendered_len)
                .sum::<usize>()
                + self.tokens.len().saturating_sub(1),
        );
        for (index, token) in self.tokens.into_iter().enumerate() {
            if index != 0 {
                output.push(' ');
            }
            match token.kind {
                Kind::Charset => output.push_str("(_charset)"),
                Kind::Ident | Kind::QuotedIdent => {
                    output.push('`');
                    output.push_str(&token.literal);
                    output.push('`');
                }
                _ => output.push_str(&token.literal),
            }
        }
        output
    }

    fn normal_token(&self, token: BorrowedToken<'a>) -> NormalToken<'a> {
        let raw = &self.sql[token.offset..token.end_offset];
        let kind = match token.kind {
            TokenKind::Ident if raw.starts_with('`') || raw.starts_with('"') => Kind::QuotedIdent,
            TokenKind::Ident => Kind::Ident,
            TokenKind::CharsetIntroducer => Kind::Charset,
            TokenKind::Str => Kind::String,
            TokenKind::IntLit => Kind::Integer,
            TokenKind::FloatLit | TokenKind::DecLit | TokenKind::HexLit => Kind::Number,
            TokenKind::BitLit => Kind::Bit,
            TokenKind::UserVar => Kind::UserVar,
            // Go identifies bare NULL as a keyword after the marker reducer.
            TokenKind::Keyword if raw.eq_ignore_ascii_case("null") => Kind::Ident,
            TokenKind::Keyword if raw == r"\N" => Kind::NullLiteral,
            TokenKind::Keyword => Kind::Keyword,
            TokenKind::HintComment => Kind::Hint,
            TokenKind::Op if token.text == "?" => Kind::Param,
            _ => Kind::Other,
        };
        let literal = match kind {
            Kind::String => string_literal(raw),
            Kind::UserVar => variable_literal(raw),
            Kind::NullLiteral => Cow::Borrowed(raw),
            _ => token.text,
        };
        NormalToken {
            kind,
            literal: lowercase(literal),
        }
    }

    fn reduce_optimizer_hint(&mut self, token: &mut NormalToken<'a>) -> bool {
        if token.kind == Kind::Hint {
            return true;
        }
        if token.literal == "index"
            && self
                .tokens
                .last()
                .is_some_and(|last| matches!(last.literal.as_ref(), "force" | "use" | "ignore"))
        {
            loop {
                let next = self.lexer.next_token_borrowed();
                if matches!(next.kind, TokenKind::Invalid | TokenKind::Eof) {
                    break;
                }
                let ends_hint = if next.kind == TokenKind::Str {
                    string_literal(&next.text) == ")"
                } else {
                    next.text == ")"
                };
                if ends_hint {
                    self.tokens.pop();
                    return true;
                }
            }
            return false;
        }
        if token.literal == "straight_join" {
            token.literal = Cow::Borrowed("join");
        }
        false
    }

    fn reduce_literal(
        &mut self,
        token: &mut NormalToken<'a>,
        redact: RedactMode,
        for_binding: bool,
        for_plan_replayer: bool,
    ) {
        if !self.is_literal(token) {
            return;
        }
        if redact == RedactMode::Marker && !for_binding && !for_plan_replayer {
            if matches!(token.literal.as_ref(), "?" | "*") {
                return;
            }
            token.literal = Cow::Owned(mark_literal(&token.literal));
            return;
        }
        if token.literal == "*" {
            if self.tokens.last().is_some_and(|last| last.literal == "(") {
                *token = NormalToken::generic();
            }
            return;
        }
        if matches!(token.kind, Kind::Number | Kind::Integer) && self.is_prefixed_by_unary() {
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
        if token.kind == Kind::Integer && self.is_order_or_group_by() {
            return;
        }
        *token = NormalToken::generic();
    }

    fn is_literal(&self, token: &NormalToken<'_>) -> bool {
        matches!(
            token.kind,
            Kind::Number
                | Kind::Integer
                | Kind::String
                | Kind::Bit
                | Kind::Param
                | Kind::NullLiteral
        ) || token.literal == "*"
            || (token.kind == Kind::Ident && token.literal == "null")
    }

    fn is_prefixed_by_unary(&self) -> bool {
        let Some(last) = self.tokens.last() else {
            return false;
        };
        if !matches!(last.literal.as_ref(), "-" | "+") {
            return false;
        }
        let Some(before) = self.tokens.iter().rev().nth(1) else {
            return true;
        };
        matches!(
            before.literal.as_ref(),
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

    fn replace_single_literal_with_in_list(&mut self, current: &NormalToken<'_>) {
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

    fn reduce_in_list_with_single_literal(&mut self, current: &NormalToken<'_>) {
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

    fn reduce_in_row_list_with_single_literal(&mut self, current: &NormalToken<'_>) {
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
                return matches!(first.literal.as_ref(), "order" | "group")
                    && second.literal == "by";
            }
            n += 2;
        }
    }
}

fn lowercase(text: Cow<'_, str>) -> Cow<'_, str> {
    if !text.is_ascii() {
        return Cow::Owned(tidb_mysql::to_lowercase(&text));
    }
    if text.bytes().any(|byte| byte.is_ascii_uppercase()) {
        let mut text = text;
        text.to_mut().make_ascii_lowercase();
        return text;
    }
    text
}

/// Go scanString passes the unescaped value to the digester.
fn string_literal(raw: &str) -> Cow<'_, str> {
    let inner = &raw[1..raw.len() - 1];
    if !inner.as_bytes().contains(&raw.as_bytes()[0]) && !inner.as_bytes().contains(&b'\\') {
        return Cow::Borrowed(inner);
    }
    Cow::Owned(
        String::from_utf8(decode_quoted_string(
            raw,
            false,
            tidb_charset::Encoding::Utf8,
        ))
        .expect("decoding a UTF-8 SQL literal preserves UTF-8"),
    )
}

/// Go startWithAt drops quotes around the variable's name.
fn variable_literal(raw: &str) -> Cow<'_, str> {
    let Some(start) = raw.find(['`', '"', '\'']) else {
        return Cow::Borrowed(raw);
    };
    let suffix = &raw[start..];
    let name = if suffix.starts_with('`') {
        Cow::Owned(suffix[1..suffix.len() - 1].replace("``", "`"))
    } else {
        string_literal(suffix)
    };
    let mut decoded = String::with_capacity(start + name.len());
    decoded.push_str(&raw[..start]);
    decoded.push_str(&name);
    Cow::Owned(decoded)
}

fn mark_literal(value: &str) -> String {
    let mut marked = String::with_capacity(value.len() + "‹›".len());
    marked.push('‹');
    let mut start = 0;
    for (index, marker) in value.match_indices(['‹', '›']) {
        let end = index + marker.len();
        marked.push_str(&value[start..end]);
        marked.push_str(marker);
        start = end;
    }
    marked.push_str(&value[start..]);
    marked.push('›');
    marked
}
