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
// See the License for the specific language governing permissions and
// limitations under the License.

//! A faithful Rust port of the TiDB SQL scanner.
//!
//! This mirrors `pkg/parser/lexer.go` and `pkg/parser/misc.go` from the Go tree:
//! the same character classes, the same operator greediness, the same numeric
//! literal classification (INT / FLOAT / DECIMAL / HEX / BIT with identifier
//! fall-backs), and the same keyword-recognition rule (`isTokenIdentifier`),
//! including the builtin-function-before-`(` and the `.`-adjacency cases.
//!
//! Correctness is proved by the `difftest` crate, which compares this lexer's
//! token stream against the production Go scanner over a corpus.
//!
//! Phase 0 of the rewrite is safe Rust end-to-end (`unsafe` is forbidden in the
//! workspace) and depends on no external crates.

mod charset;
mod collation;
mod escape;
mod keywords;
mod reader;
mod reserved;
mod token;

/// Canonicalizes a recognized charset name from the generated TiDB charset table.
pub use charset::canonical_charset;
pub use collation::canonical_collation;
pub use escape::unescape_char;

/// Canonicalizes the legacy charset-introducer subset accepted by Go's
/// `charset.GetDefaultCollationLegacy`. The scanner intentionally recognizes
/// every name in TiDB's charset table so the parser can report the same
/// unsupported-introducer error boundary; only these five canonical names
/// (plus the `utf8mb3` alias normalized by [`canonical_charset`]) may carry an
/// underscore introducer through the expression grammar.
pub fn canonical_legacy_charset(name: &str) -> Option<&'static str> {
    let canonical = canonical_charset(name)?;
    match canonical {
        "utf8" | "utf8mb4" | "ascii" | "latin1" | "binary" => Some(canonical),
        _ => None,
    }
}
pub use reserved::is_reserved;
pub use token::{Token, TokenKind};

/// Returns whether `word` is one of TiDB's builtin-function keyword names.
///
/// The Rust token class intentionally collapses Go's distinct builtin-token
/// and ordinary-keyword IDs, so parser callers combine this textual predicate
/// with source adjacency to reproduce Go's `isIdentLike` boundary (for
/// example, reject `CREATE TABLE COUNT(a)` but accept `CREATE TABLE COUNT (a)`).
/// Keeping the generated keyword table behind this helper prevents parser
/// crates from maintaining a second list.
pub fn is_builtin_function_keyword(word: &str) -> bool {
    let upper = word.to_ascii_uppercase();
    keywords::BUILTIN_FUNC_KEYWORDS
        .binary_search(&upper.as_str())
        .is_ok()
}

use reader::Reader;

/// SQL mode flags that change tokenization. Mirrors the subset of
/// `mysql.SQLMode` the scanner consults. The all-`false` default matches
/// TiDB's default SQL mode (escapes on, no ANSI quotes).
#[derive(Debug, Clone, Copy, Default)]
pub struct SqlMode {
    /// `NO_BACKSLASH_ESCAPES`: backslash is an ordinary character in strings.
    pub no_backslash_escapes: bool,
    /// `ANSI_QUOTES`: double-quoted values are identifiers, not strings.
    pub ansi_quotes: bool,
}

/// The scanner. Construct with [`Lexer::new`], then pull tokens with
/// [`Lexer::next_token`] until [`TokenKind::Eof`].
#[derive(Debug)]
pub struct Lexer<'a> {
    r: Reader<'a>,
    sql_mode: SqlMode,
    /// True for the nested optimizer-hint lexer. Hint query-block names
    /// use `.` as a grammar separator, unlike ordinary SQL user-variable
    /// references where it is part of the variable token.
    hint_mode: bool,
    /// Whether window-function keywords (OVER, ROWS, ...) are recognized.
    support_window_func: bool,
    /// True when a `.` immediately follows the identifier just scanned; used to
    /// keep qualified names (`t.count`) from being read as keywords.
    identifier_dot: bool,
    /// True inside a `/*! ... */` executable comment, so the closing `*/` is
    /// consumed rather than lexed as two operators.
    in_bang_comment: bool,
    /// Uppercased keyword text of the three most recently returned tokens
    /// (`[0]` = most recent), `None` where a token was not a keyword. Mirrors
    /// the scanner's `lastKeyword`/`lastKeyword2`/`lastKeyword3`, which gate
    /// optimizer-hint recognition.
    kw_hist: [Option<&'static str>; 3],
}

/// Keywords after which a `/*+ ... */` hint comment is recognized
/// (`hintedTokens` in pkg/parser/misc.go). Sorted for binary search.
const HINTED_KEYWORDS: &[&str] = &[
    "CREATE",
    "DELETE",
    "INSERT",
    "PARTITION",
    "REPLACE",
    "SELECT",
    "UPDATE",
];

impl<'a> Lexer<'a> {
    /// Creates a scanner over `sql` with the default SQL mode and window
    /// functions enabled (matching `parser.New()` defaults).
    pub fn new(sql: &'a str) -> Self {
        Lexer {
            r: Reader::new(sql),
            sql_mode: SqlMode::default(),
            hint_mode: false,
            support_window_func: true,
            identifier_dot: false,
            in_bang_comment: false,
            kw_hist: [None, None, None],
        }
    }

    /// Overrides the SQL mode.
    pub fn with_sql_mode(mut self, mode: SqlMode) -> Self {
        self.sql_mode = mode;
        self
    }

    /// Uses the optimizer-hint token boundary rules for a nested hint
    /// parser. In this mode, `.` terminates an `@query_block` token so a
    /// `QB_NAME` ViewNameList can use it as its separator.
    pub fn with_hint_mode(mut self) -> Self {
        self.hint_mode = true;
        self
    }

    /// Enables or disables window-function keyword recognition.
    pub fn set_support_window_func(&mut self, v: bool) {
        self.support_window_func = v;
    }

    /// Collects the entire token stream (including the terminal `Eof`).
    pub fn tokenize(mut self) -> Vec<Token> {
        let mut out = Vec::new();
        loop {
            let t = self.next_token();
            let is_eof = t.kind == TokenKind::Eof;
            out.push(t);
            if is_eof {
                break;
            }
        }
        out
    }

    /// Returns the next token, applying keyword recognition and the scanner's
    /// two-word keyword merges (`AS OF`, `MEMBER OF`).
    pub fn next_token(&mut self) -> Token {
        let (kind, mut start, mut end, mut text) = self.scan_token();

        // `AS`/`MEMBER` followed by `OF` merges into a single keyword token
        // whose offset is the `OF` position (matching Scanner.Lex).
        if kind == TokenKind::Keyword {
            let upper = ascii_upper(&text);
            if upper == "AS" || upper == "MEMBER" {
                let saved = (self.r.offset(), self.identifier_dot, self.in_bang_comment);
                let (k2, s2, e2, t2) = self.scan_token();
                if k2 == TokenKind::Keyword && ascii_upper(&t2) == "OF" {
                    text = format!("{text} {t2}");
                    start = s2;
                    end = e2;
                } else {
                    self.r.set_offset(saved.0);
                    self.identifier_dot = saved.1;
                    self.in_bang_comment = saved.2;
                }
            }
        }

        // Shift the keyword history (mirrors Scanner.Lex's lastKeyword shift),
        // recording only the keywords that gate optimizer-hint recognition.
        let interned = if kind == TokenKind::Keyword {
            intern_hint_keyword(&ascii_upper(&text))
        } else {
            None
        };
        self.kw_hist = [interned, self.kw_hist[0], self.kw_hist[1]];

        Token {
            kind,
            offset: start,
            end_offset: end,
            text,
        }
    }

    /// Scans one token with keyword recognition, ANSI-quote handling, and the
    /// unquoting of quoted identifiers, but without the two-word merges or the
    /// keyword-history shift (those live in `next_token`).
    fn scan_token(&mut self) -> (TokenKind, usize, usize, String) {
        let (kind, start, end) = self.scan();
        let raw = &self.r.src()[start..end];

        let mut kind = kind;
        // A charset introducer (`_utf8mb4'x'`) becomes a `CharsetIntroducer`
        // token whose literal is the canonical charset name — a GENUINELY
        // DIFFERENT token class from a plain keyword in real TiDB's own
        // scanner (`underscoreCS`, confirmed via `pkg/parser/lexer.go`'s
        // `handleIdent`: this check is unconditional, NOT gated on a
        // following quote — `_latin1` alone, with nothing after it, is
        // STILL `underscoreCS`, a genuine `ParseError` at the GRAMMAR
        // level once the parser finds no string literal following, not a
        // silent identifier fallback). This precedes keyword lookup.
        let mut introducer: Option<&'static str> = None;
        if kind == TokenKind::Ident && !raw.starts_with('`') {
            if raw == "\\N" {
                // `\N` — a standalone shorthand for the `NULL` literal
                // (see `scan`'s own doc for why this arrives here as an
                // `Ident`-shaped 2-byte span) — promotes to a plain
                // `Keyword` token with canonical text `"NULL"`, reusing
                // the SAME `TokenKind::Keyword` dispatch (`parse_prefix`'s
                // `"NULL" => Ok(Expr::Null)` arm) real TiDB's own
                // `NULL` keyword already goes through — no new parser
                // code needed at all.
                kind = TokenKind::Keyword;
                introducer = Some("NULL");
            } else if let Some(rest) = raw.strip_prefix('_') {
                if let Some(canon) = charset::canonical_charset(rest) {
                    kind = TokenKind::CharsetIntroducer;
                    introducer = Some(canon);
                }
            } else if (raw == "N" || raw == "n") && self.r.peek() == b'\'' {
                // `N'x'`/`n'x'` — the National Character Set shorthand,
                // equivalent to `_utf8'x'` (confirmed via
                // `pkg/parser/lexer.go`'s own `startWithNn`: `tok =
                // underscoreCS; lit = "utf8"` — NOT `"utf8mb4"`, a real,
                // easy-to-miss distinction confirmed via `godump restore`).
                kind = TokenKind::CharsetIntroducer;
                introducer = Some("utf8");
            }
            // Otherwise a bare identifier may still be a keyword.
            if introducer.is_none() && self.try_keyword(raw, start).is_some() {
                kind = TokenKind::Keyword;
            }
        }

        // ANSI_QUOTES: a double-quoted string is a (quoted) identifier instead.
        if self.sql_mode.ansi_quotes && kind == TokenKind::Str && self.r.byte_at(start) == b'"' {
            kind = TokenKind::Ident;
        }

        if kind == TokenKind::Ident || kind == TokenKind::Keyword {
            self.identifier_dot = self.r.peek() == b'.';
        }

        // The token's semantic text. Charset introducers use the canonical
        // name; quoted identifiers are unquoted so their label matches the Go
        // scanner's `Lit` (delimiters stripped, doubled delimiters collapsed).
        let text = match introducer {
            Some(canon) => canon.to_string(),
            None => match kind {
                TokenKind::Ident if raw.starts_with('`') => unquote(raw, '`'),
                TokenKind::Ident if raw.starts_with('"') => unquote(raw, '"'),
                _ => raw.to_string(),
            },
        };

        (kind, start, end, text)
    }

    /// Whether a `/*+ ... */` at the current position is recognized as a hint,
    /// per the scanner's `lastKeyword`/`lastKeyword2`/`lastKeyword3` gating.
    fn hint_recognized(&self) -> bool {
        let hinted =
            matches!(self.kw_hist[0], Some(k) if HINTED_KEYWORDS.binary_search(&k).is_ok());
        if !hinted {
            return false;
        }
        // `... FOR UPDATE /*+ ... */` is ignored unless it is the
        // `CREATE BINDING FOR UPDATE ...` case.
        if self.kw_hist[1] == Some("FOR") {
            self.kw_hist[2] == Some("BINDING")
        } else {
            true
        }
    }

    /// Decides whether the bare identifier `text` (starting at `start`) is a
    /// keyword, mirroring `Scanner.isTokenIdentifier`.
    fn try_keyword(&mut self, text: &str, start: usize) -> Option<()> {
        // Followed immediately by '.': part of a qualified name.
        if self.r.peek() == b'.' {
            return None;
        }
        // Preceded (skipping spaces) by '.': part of a qualified name.
        let src = self.r.src().as_bytes();
        let mut idx = start as isize - 1;
        while idx >= 0 {
            let c = src[idx as usize];
            if c == b' ' {
                idx -= 1;
                continue;
            }
            if c == b'.' {
                return None;
            }
            break;
        }

        let upper = ascii_upper(text);

        // Builtin function keywords only win when directly followed by '('.
        let check_bt_func = self.r.peek() == b'(';
        if check_bt_func && keyword_in(keywords::BUILTIN_FUNC_KEYWORDS, &upper) {
            return Some(());
        }
        if keyword_in(keywords::GENERAL_KEYWORDS, &upper) {
            return Some(());
        }
        if self.support_window_func && keyword_in(keywords::WINDOW_FUNC_KEYWORDS, &upper) {
            return Some(());
        }
        None
    }

    /// Scans one raw token, returning its kind and `[start, end)` byte span.
    /// Keyword recognition is applied later, in `next_token`.
    fn scan(&mut self) -> (TokenKind, usize, usize) {
        self.skip_whitespace();
        let start = self.r.offset();
        if self.r.eof() {
            return (TokenKind::Eof, start, start);
        }

        let ch0 = self.r.peek();

        // High bytes (>= 0x80) begin identifiers.
        if is_ident_extend(ch0) {
            return self.scan_identifier(start);
        }

        match ch0 {
            b'0'..=b'9' => self.scan_number(start),
            b'.' => self.scan_dot(start),
            b'\'' | b'"' => self.scan_string(start),
            b'`' => self.scan_quoted_ident(start),
            b'@' => self.scan_at(start),
            b'x' | b'X' => self.scan_x(start),
            b'b' | b'B' => self.scan_b(start),
            b'n' | b'N' => self.scan_n(start),
            // `\N` — the exact two-byte sequence, CASE-SENSITIVE (`\n`
            // lowercase is a genuine `ParseError`, confirmed via `godump
            // restore`) — is a standalone shorthand for the `NULL`
            // literal, registered as its own multi-char string token in
            // real TiDB's own scanner (`pkg/parser/misc.go`:
            // `initTokenString("\\N", null)`, producing the SAME `null`
            // token the `NULL` keyword itself lexes to) — surfaced here
            // as an `Ident`-shaped span for `scan_token` to promote to a
            // `Keyword` with canonical text `"NULL"`, matching how the
            // `_charset`/`N'...'` introducer cases already work.
            b'\\' if self.r.byte_at(start + 1) == b'N' => {
                self.r.inc_n(2);
                (TokenKind::Ident, start, self.r.offset())
            }
            b'_' | b'$' => self.scan_identifier(start),
            c if is_letter(c) => self.scan_identifier(start),
            b'#' => self.scan_line_comment(start),
            b'-' => self.scan_dash(start),
            b'/' => self.scan_slash(start),
            b'*' => self.scan_star(start),
            _ => self.scan_operator(start),
        }
    }

    fn skip_whitespace(&mut self) {
        self.r.inc_as_long_as(is_space);
    }

    fn scan_identifier(&mut self, start: usize) -> (TokenKind, usize, usize) {
        self.r.inc_as_long_as(is_ident_char);
        (TokenKind::Ident, start, self.r.offset())
    }

    fn scan_quoted_ident(&mut self, start: usize) -> (TokenKind, usize, usize) {
        self.r.inc(); // opening backtick
        loop {
            if self.r.eof() {
                return (TokenKind::Invalid, start, self.r.offset());
            }
            let ch = self.r.read_byte();
            if ch == b'`' {
                if self.r.peek() != b'`' {
                    // Quoted identifiers are never reinterpreted as keywords.
                    return (TokenKind::Ident, start, self.r.offset());
                }
                self.r.inc(); // doubled `` -> literal backtick
            }
        }
    }

    fn scan_string(&mut self, start: usize) -> (TokenKind, usize, usize) {
        let ending = self.r.read_byte();
        loop {
            if self.r.eof() {
                return (TokenKind::Invalid, start, self.r.offset());
            }
            let ch0 = self.r.read_byte();
            if ch0 == ending {
                if self.r.peek() != ending {
                    return (TokenKind::Str, start, self.r.offset());
                }
                self.r.inc(); // doubled quote -> literal quote
            } else if ch0 == b'\\' && !self.sql_mode.no_backslash_escapes {
                if self.r.eof() {
                    return (TokenKind::Invalid, start, self.r.offset());
                }
                self.r.inc(); // consume the escaped char
            }
        }
    }

    /// `@user`, `@@global.x`, `@@session.x`, `@@x`.
    fn scan_at(&mut self, start: usize) -> (TokenKind, usize, usize) {
        self.r.inc(); // '@'
        if self.r.peek() == b'@' {
            self.r.inc();
            // optional global./session./local. prefix
            for pfx in ["global.", "session.", "local."] {
                if self.r.starts_with_ci(pfx) {
                    self.r.inc_n(pfx.len());
                    break;
                }
            }
            let body = self.scan_var_body();
            if body.is_none() || !body.unwrap_or(false) {
                // `@@` and `@@global.` without a name are invalid in the Go
                // scanner; a malformed quoted body is invalid as well.
                return (TokenKind::Invalid, start, self.r.offset());
            }
            return (TokenKind::UserVar, start, self.r.offset());
        }
        if self.scan_var_body().is_none() {
            return (TokenKind::Invalid, start, self.r.offset());
        }
        (TokenKind::UserVar, start, self.r.offset())
    }

    /// The name part of a user/system variable: a quoted string, a backtick
    /// identifier, or a run of user-variable characters.
    /// Returns `Some(has_body)` for a valid body, and `None` for an
    /// unterminated quoted identifier/string.  A punctuation/EOF body is
    /// represented as `Some(false)`: single-`@` variables accept it (the Go
    /// scanner returns the `@` token and leaves the punctuation), whereas
    /// system variables reject a missing name.
    fn scan_var_body(&mut self) -> Option<bool> {
        match self.r.peek() {
            b'\'' | b'"' => {
                let (kind, _, _) = self.scan_string(self.r.offset());
                (kind == TokenKind::Str).then_some(true)
            }
            b'`' => {
                let (kind, _, _) = self.scan_quoted_ident(self.r.offset());
                (kind == TokenKind::Ident).then_some(true)
            }
            c if is_user_var_char(c) => {
                let is_var_char = if self.hint_mode {
                    is_hint_user_var_char
                } else {
                    is_user_var_char
                };
                self.r.inc_as_long_as(is_var_char);
                Some(true)
            }
            _ => Some(false),
        }
    }

    /// `x'..'` hex string literal, otherwise an identifier starting with x/X.
    fn scan_x(&mut self, start: usize) -> (TokenKind, usize, usize) {
        self.r.inc(); // x
        if self.r.peek() == b'\'' {
            self.r.inc();
            self.r.inc_as_long_as(is_hex_digit);
            if self.r.peek() == b'\'' {
                self.r.inc();
                return (TokenKind::HexLit, start, self.r.offset());
            }
            return (TokenKind::Invalid, start, self.r.offset());
        }
        self.r.set_offset(start);
        self.scan_identifier(start)
    }

    /// `b'..'` bit string literal, otherwise an identifier starting with b/B.
    fn scan_b(&mut self, start: usize) -> (TokenKind, usize, usize) {
        self.r.inc(); // b
        if self.r.peek() == b'\'' {
            self.r.inc();
            self.r.inc_as_long_as(|c| c == b'0' || c == b'1');
            if self.r.peek() == b'\'' {
                self.r.inc();
                return (TokenKind::BitLit, start, self.r.offset());
            }
            return (TokenKind::Invalid, start, self.r.offset());
        }
        self.r.set_offset(start);
        self.scan_identifier(start)
    }

    /// Scans the `N`/`n` identifier span. The `N'..'` national charset
    /// introducer's OWN kind conversion (`TokenKind::Ident` ->
    /// `TokenKind::CharsetIntroducer`) happens later, in `scan_token`
    /// (alongside the `_charset` introducer check it shares its "is the
    /// next byte a quote" logic with) — this just scans the identifier
    /// span itself, same as any other identifier.
    fn scan_n(&mut self, start: usize) -> (TokenKind, usize, usize) {
        self.scan_identifier(start)
    }

    fn scan_number(&mut self, start: usize) -> (TokenKind, usize, usize) {
        if self.identifier_dot {
            return self.scan_identifier(start);
        }
        let ch0 = self.r.read_byte(); // first digit
        if ch0 == b'0' {
            match self.r.peek() {
                b'0'..=b'7' => {
                    self.r.inc();
                    self.r.inc_as_long_as(|c| (b'0'..=b'7').contains(&c));
                }
                b'x' | b'X' => {
                    self.r.inc();
                    let p1 = self.r.offset();
                    self.r.inc_as_long_as(is_hex_digit);
                    let p2 = self.r.offset();
                    if p1 == p2 || is_digit(self.r.peek()) {
                        self.r.inc_as_long_as(is_ident_char);
                        return (TokenKind::Ident, start, self.r.offset());
                    }
                    // Hex literal, unless trailing identifier chars glue on.
                    return self.finish_number(start, TokenKind::HexLit);
                }
                b'b' => {
                    self.r.inc();
                    let p1 = self.r.offset();
                    self.r.inc_as_long_as(|c| c == b'0' || c == b'1');
                    let p2 = self.r.offset();
                    if p1 == p2 || is_digit(self.r.peek()) {
                        self.r.inc_as_long_as(is_ident_char);
                        return (TokenKind::Ident, start, self.r.offset());
                    }
                    return self.finish_number(start, TokenKind::BitLit);
                }
                b'.' => return self.scan_float(start),
                b'B' => {
                    self.r.inc_as_long_as(is_ident_char);
                    return (TokenKind::Ident, start, self.r.offset());
                }
                _ => {}
            }
        }

        self.r.inc_as_long_as(is_digit);
        let ch = self.r.peek();
        if ch == b'.' || ch == b'e' || ch == b'E' {
            return self.scan_float(start);
        }
        self.finish_number(start, TokenKind::IntLit)
    }

    /// After a HEX/BIT/INT literal body, a trailing identifier char turns the
    /// whole span into an identifier (`0x1ag`, `0b1z`, `12ab`).
    fn finish_number(&mut self, start: usize, kind: TokenKind) -> (TokenKind, usize, usize) {
        if kind == TokenKind::IntLit {
            let ch0 = self.r.peek();
            if !self.r.eof() && is_ident_char(ch0) {
                self.r.inc_as_long_as(is_ident_char);
                return (TokenKind::Ident, start, self.r.offset());
            }
            let end = self.r.offset();
            // An integer that overflows u64 is a DECIMAL literal, matching
            // toInt -> toDecimal on strconv.ErrRange.
            if self.r.src()[start..end].parse::<u64>().is_err() {
                return (TokenKind::DecLit, start, end);
            }
            return (kind, start, end);
        }
        // HEX/BIT: mirror scanDigits() + identifier-glue check.
        self.r.inc_as_long_as(is_digit);
        let ch0 = self.r.peek();
        if !self.r.eof() && is_ident_char(ch0) {
            self.r.inc_as_long_as(is_ident_char);
            return (TokenKind::Ident, start, self.r.offset());
        }
        (kind, start, self.r.offset())
    }

    fn scan_dot(&mut self, start: usize) -> (TokenKind, usize, usize) {
        self.r.inc(); // '.'
        if self.identifier_dot {
            return (TokenKind::Op, start, self.r.offset());
        }
        if is_digit(self.r.peek()) {
            let (tok, s, e) = self.scan_float(start);
            if tok == TokenKind::Ident {
                return (TokenKind::Invalid, s, e);
            }
            return (tok, s, e);
        }
        (TokenKind::Op, start, self.r.offset())
    }

    /// float = D1 '.' D2 ('e' ['+'|'-'] D3)?. Classifies DEC vs FLOAT, with the
    /// identifier fall-back for a bad exponent (`9e9e`, `9est`).
    fn scan_float(&mut self, start: usize) -> (TokenKind, usize, usize) {
        self.r.set_offset(start);
        self.r.inc_as_long_as(is_digit);
        if self.r.peek() == b'.' {
            self.r.inc();
            self.r.inc_as_long_as(is_digit);
        }
        let ch0 = self.r.peek();
        if ch0 == b'e' || ch0 == b'E' {
            self.r.inc();
            let sign = self.r.peek();
            if sign == b'-' || sign == b'+' {
                self.r.inc();
            }
            if is_digit(self.r.peek()) {
                self.r.inc_as_long_as(is_digit);
                (TokenKind::FloatLit, start, self.r.offset())
            } else {
                // Not a valid exponent: the whole run is an identifier.
                self.r.set_offset(start);
                self.r.inc_as_long_as(is_ident_char);
                (TokenKind::Ident, start, self.r.offset())
            }
        } else {
            (TokenKind::DecLit, start, self.r.offset())
        }
    }

    fn scan_line_comment(&mut self, _start: usize) -> (TokenKind, usize, usize) {
        self.r.inc_as_long_as(|c| c != b'\n');
        self.scan()
    }

    fn scan_dash(&mut self, start: usize) -> (TokenKind, usize, usize) {
        let rest = &self.r.src()[start..];
        if let Some(stripped) = rest.strip_prefix("--") {
            // `-- ` line comment: needs EOL or whitespace after the two dashes.
            if stripped.is_empty() || is_space(stripped.as_bytes()[0]) {
                self.r.inc_as_long_as(|c| c != b'\n');
                return self.scan();
            }
        }
        if rest.starts_with("->>") {
            self.r.inc_n(3);
            return (TokenKind::Op, start, self.r.offset());
        }
        if rest.starts_with("->") {
            self.r.inc_n(2);
            return (TokenKind::Op, start, self.r.offset());
        }
        self.r.inc();
        (TokenKind::Op, start, self.r.offset())
    }

    fn scan_slash(&mut self, start: usize) -> (TokenKind, usize, usize) {
        self.r.inc(); // '/'
        if self.r.peek() != b'*' {
            return (TokenKind::Op, start, self.r.offset());
        }
        self.r.inc(); // '*'
                      // `/*!` and `/*T![..]` executable comments: the marker is skipped and
                      // the enclosed SQL is scanned normally. A `/*+ ... */` becomes a hint
                      // token when recognized in position, otherwise it (and any plain
                      // `/* ... */`) is dropped.
        let mut is_hint = false;
        match self.r.peek() {
            b'!' => {
                self.r.inc();
                // MySQL executable comments carry exactly five version
                // digits in TiDB's scanner.  Keep the helper's min/max
                // contract even though this call site uses one fixed range;
                // the source unit test exercises both branches directly.
                let _ = self.scan_version_digits(5, 5);
                self.in_bang_comment = true;
                return self.scan(); // enter bang comment; body is live SQL
            }
            b'T' => {
                if self.r.byte_at(self.r.offset() + 1) == b'!' {
                    self.r.inc(); // T
                    self.r.inc(); // !
                                  // A TiDB feature comment is executable only when every
                                  // feature id is known to this parser.  Unknown or
                                  // malformed ids are ordinary comments, exactly as
                                  // `tidbfeature.CanParseFeature` makes the Go scanner do.
                                  // TiDB's empty feature id (`FeatureIDTiDB = ""`) emits
                                  // `/*T! ... */` without a bracketed id list. Go treats
                                  // this as an executable comment; only comments that
                                  // actually start a `[...]` list go through the
                                  // feature-gate check below.
                    if self.r.peek() != b'[' {
                        self.in_bang_comment = true;
                        return self.scan();
                    }
                    if let Some(ids) = self.scan_feature_ids() {
                        if ids.iter().all(|id| supported_feature(id)) {
                            self.in_bang_comment = true;
                            return self.scan();
                        }
                    }
                }
            }
            b'+' => {
                is_hint = self.hint_recognized();
            }
            _ => {}
        }
        // Consume through the closing `*/`.
        if !self.consume_block_comment_body() {
            return (TokenKind::Invalid, start, self.r.offset());
        }
        if is_hint {
            return (TokenKind::HintComment, start, self.r.offset());
        }
        // Plain comment or unrecognized hint: dropped; scan the next token.
        self.scan()
    }

    /// Consumes bytes up to and including the closing `*/` of a block comment
    /// (the opening `/*` is already consumed). Returns false on an unterminated
    /// comment.
    fn consume_block_comment_body(&mut self) -> bool {
        loop {
            self.r.inc_as_long_as(|c| c != b'*');
            if self.r.eof() {
                return false;
            }
            self.r.inc(); // '*'
            if self.r.peek() == b'/' {
                self.r.inc();
                return true;
            }
        }
    }

    /// Consumes between `min` and `max` decimal digits.  If fewer than
    /// `min` digits are present, the reader is restored to its original
    /// offset, matching `Scanner.scanVersionDigits` exactly.
    fn scan_version_digits(&mut self, min: usize, max: usize) -> bool {
        let start = self.r.offset();
        for i in 0..max {
            if is_digit(self.r.peek()) {
                self.r.inc();
            } else if i < min {
                self.r.set_offset(start);
                return false;
            } else {
                break;
            }
        }
        true
    }

    /// Parses `[feature1,feature2,...]` using the same three-state grammar as
    /// `Scanner.scanFeatureIDs`.  Invalid input rewinds to the opening `[`;
    /// callers can then consume the surrounding block as an ordinary comment.
    fn scan_feature_ids(&mut self) -> Option<Vec<String>> {
        let start = self.r.offset();
        if self.r.peek() != b'[' {
            return None;
        }
        self.r.inc();
        let mut ids = Vec::new();
        loop {
            if !is_ident_char(self.r.peek()) {
                self.r.set_offset(start);
                return None;
            }
            let item_start = self.r.offset();
            self.r.inc_as_long_as(is_ident_char);
            ids.push(self.r.src()[item_start..self.r.offset()].to_string());
            match self.r.peek() {
                b']' => {
                    self.r.inc();
                    return Some(ids);
                }
                b',' => {
                    self.r.inc();
                }
                _ => {
                    self.r.set_offset(start);
                    return None;
                }
            }
        }
    }

    /// `*`, which closes a `/*! ... */` executable comment when one is open,
    /// otherwise a plain operator.
    fn scan_star(&mut self, start: usize) -> (TokenKind, usize, usize) {
        self.r.inc(); // '*'
        if self.in_bang_comment && self.r.peek() == b'/' {
            self.in_bang_comment = false;
            self.r.inc();
            return self.scan();
        }
        self.identifier_dot = false;
        (TokenKind::Op, start, self.r.offset())
    }

    /// Multi-character operators (greedy) and single-character punctuation.
    fn scan_operator(&mut self, start: usize) -> (TokenKind, usize, usize) {
        let rest = &self.r.src()[start..];
        // Longest first.
        for op in [
            "<=>", "->>", "||", "&&", "&^", ":=", ">=", "<=", "!=", "<>", "<<", ">>", "->",
        ] {
            if rest.starts_with(op) {
                self.r.inc_n(op.len());
                return (TokenKind::Op, start, self.r.offset());
            }
        }
        let kind = if is_single_operator(self.r.peek()) {
            TokenKind::Op
        } else {
            TokenKind::Invalid
        };
        self.r.inc();
        (kind, start, self.r.offset())
    }
}

// ---- character classes (mirror pkg/parser/misc.go) ----

fn is_letter(ch: u8) -> bool {
    ch.is_ascii_lowercase() || ch.is_ascii_uppercase()
}
fn is_digit(ch: u8) -> bool {
    ch.is_ascii_digit()
}
fn is_ident_extend(ch: u8) -> bool {
    ch >= 0x80
}
fn is_ident_char(ch: u8) -> bool {
    is_letter(ch) || is_digit(ch) || ch == b'_' || ch == b'$' || is_ident_extend(ch)
}
fn is_user_var_char(ch: u8) -> bool {
    is_letter(ch) || is_digit(ch) || ch == b'_' || ch == b'$' || ch == b'.' || is_ident_extend(ch)
}

/// Optimizer-hint query-block names use dots as `ViewNameList` separators.
fn is_hint_user_var_char(ch: u8) -> bool {
    is_letter(ch) || is_digit(ch) || ch == b'_' || ch == b'$' || is_ident_extend(ch)
}
fn is_hex_digit(ch: u8) -> bool {
    ch.is_ascii_hexdigit()
}
fn is_space(ch: u8) -> bool {
    // Go's unicode.IsSpace over a byte: ASCII whitespace set.
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

/// Single-byte punctuation registered by `pkg/parser/misc.go`'s token trie.
/// Keeping this explicit prevents arbitrary bytes (notably NUL) from being
/// silently accepted as operators.
fn is_single_operator(ch: u8) -> bool {
    matches!(
        ch,
        b'/' | b'+'
            | b'>'
            | b'<'
            | b'('
            | b')'
            | b'['
            | b']'
            | b';'
            | b','
            | b'&'
            | b'%'
            | b':'
            | b'|'
            | b'!'
            | b'^'
            | b'~'
            | b'?'
            | b'='
            | b'{'
            | b'}'
            | b'\\'
    )
}

/// Feature ids accepted by `pkg/parser/tidb/features.go`'s `featureIDs` map.
fn supported_feature(id: &str) -> bool {
    matches!(
        id,
        "auto_rand"
            | "auto_id_cache"
            | "auto_rand_base"
            | "clustered_index"
            | "force_inc"
            | "placement"
            | "ttl"
            | "global_index"
            | "pre_split"
            | "affinity"
            | "region_split"
    )
}

fn ascii_upper(s: &str) -> String {
    s.as_bytes()
        .iter()
        .map(|&c| if c.is_ascii_lowercase() { c - 32 } else { c } as char)
        .collect()
}

fn keyword_in(table: &[&str], upper: &str) -> bool {
    table.binary_search(&upper).is_ok()
}

/// Interns only the keywords that gate optimizer-hint recognition, so the
/// keyword history can be tracked without allocating. Any other keyword (or a
/// non-keyword) collapses to `None`, which is sufficient because hint gating
/// only inspects these specific keywords.
fn intern_hint_keyword(upper: &str) -> Option<&'static str> {
    match upper {
        "SELECT" => Some("SELECT"),
        "INSERT" => Some("INSERT"),
        "REPLACE" => Some("REPLACE"),
        "UPDATE" => Some("UPDATE"),
        "DELETE" => Some("DELETE"),
        "CREATE" => Some("CREATE"),
        "PARTITION" => Some("PARTITION"),
        "FOR" => Some("FOR"),
        "BINDING" => Some("BINDING"),
        _ => None,
    }
}

/// Strips the surrounding `quote` characters from a quoted identifier and
/// collapses doubled quotes, mirroring the Go scanner's quoted-ident handling.
fn unquote(raw: &str, quote: char) -> String {
    let inner = raw
        .strip_prefix(quote)
        .and_then(|s| s.strip_suffix(quote))
        .unwrap_or(raw);
    let doubled: String = [quote, quote].iter().collect();
    inner.replace(&doubled, &quote.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[path = "lexer_source.rs"]
    mod lexer_source;

    /// Renders a SQL string to its space-joined token labels for compact
    /// assertions (the terminal `Eof` is dropped).
    fn labels(sql: &str) -> String {
        Lexer::new(sql)
            .tokenize()
            .iter()
            .filter(|t| t.kind != TokenKind::Eof)
            .map(|t| t.label())
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn keywords_and_identifiers() {
        assert_eq!(
            labels("SELECT a FROM t"),
            "KW:SELECT IDENT:a KW:FROM IDENT:t"
        );
        // Qualified names keep the member as an identifier, not a keyword.
        assert_eq!(labels("t.count"), "IDENT:t OP:. IDENT:count");
        // COUNT before '(' is the builtin-function keyword.
        assert_eq!(labels("count(*)"), "KW:COUNT OP:( OP:* OP:)");
    }

    #[test]
    fn numeric_literals() {
        assert_eq!(labels("42"), "NUM:INT");
        assert_eq!(labels("3.14"), "NUM:DEC");
        assert_eq!(labels("1e10"), "NUM:FLOAT");
        assert_eq!(labels("0xABCD"), "NUM:HEX");
        assert_eq!(labels("0b1010"), "NUM:BIT");
        assert_eq!(labels("x'1F'"), "NUM:HEX");
        assert_eq!(labels("b'01'"), "NUM:BIT");
        // A digit-led run with trailing ident chars is an identifier.
        assert_eq!(labels("12abc"), "IDENT:12abc");
        // u64 overflow degrades an integer to a decimal.
        assert_eq!(labels("999999999999999999999999999"), "NUM:DEC");
    }

    #[test]
    fn operators_greedy() {
        assert_eq!(labels("a<=>b"), "IDENT:a OP:<=> IDENT:b");
        assert_eq!(labels("a<>b"), "IDENT:a OP:<> IDENT:b");
        assert_eq!(labels("a>>b"), "IDENT:a OP:>> IDENT:b");
        assert_eq!(labels("j->>'$.k'"), "IDENT:j OP:->> STR");
        assert_eq!(labels("a:=1"), "IDENT:a OP::= NUM:INT");
    }

    #[test]
    fn strings_and_quoted_idents() {
        assert_eq!(labels("'a''b'"), "STR");
        assert_eq!(labels("'a\\'b'"), "STR");
        assert_eq!(labels("`my table`"), "IDENT:my table");
        assert_eq!(labels("`a``b`"), "IDENT:a`b");
    }

    #[test]
    fn variables() {
        assert_eq!(labels("@x, @@global.y"), "VAR:@x OP:, VAR:@@global.y");
    }

    #[test]
    fn comments() {
        assert_eq!(labels("a -- c\nb"), "IDENT:a IDENT:b");
        assert_eq!(labels("a # c\nb"), "IDENT:a IDENT:b");
        assert_eq!(labels("a /* c */ b"), "IDENT:a IDENT:b");
        // Executable comment: body is live SQL, markers dropped.
        assert_eq!(labels("/*! SELECT */ 1"), "KW:SELECT NUM:INT");
    }

    #[test]
    fn optimizer_hint_position() {
        // Recognized only right after a hint-accepting keyword.
        assert_eq!(
            labels("SELECT /*+ USE_INDEX(t) */ a"),
            "KW:SELECT OP:/*+ USE_INDEX(t) */ IDENT:a"
        );
        // Not after an arbitrary token: dropped.
        assert_eq!(labels("a /*+ USE_INDEX(t) */ b"), "IDENT:a IDENT:b");
    }

    #[test]
    fn two_word_merges() {
        assert_eq!(
            labels("a MEMBER OF (j)"),
            "IDENT:a KW:MEMBER OF OP:( IDENT:j OP:)"
        );
        assert_eq!(
            labels("t AS OF TIMESTAMP '1'"),
            "IDENT:t KW:AS OF KW:TIMESTAMP STR"
        );
    }

    #[test]
    fn charset_introducer() {
        assert_eq!(labels("_utf8mb4'x'"), "KW:UTF8MB4 STR");
        assert_eq!(labels("_binary 0x1F"), "KW:BINARY NUM:HEX");
        // A leading underscore that is not a charset stays an identifier.
        assert_eq!(labels("_notacharset"), "IDENT:_notacharset");
    }

    #[test]
    fn backslash_n_null() {
        // `\N` (case-sensitive) promotes to a plain `NULL` keyword token.
        assert_eq!(labels("\\N"), "KW:NULL");
        // Lowercase `\n` is NOT the shorthand — the backslash alone falls
        // to the generic single-byte operator token, `n` a separate
        // identifier.
        assert_eq!(labels("\\n"), "OP:\\ IDENT:n");
    }

    /// See `reserved::is_reserved`'s own doc for how this list was derived
    /// from real TiDB's own `pkg/parser/reserved_words.go`.
    #[test]
    fn reserved_keywords() {
        assert!(is_reserved("SELECT"));
        assert!(is_reserved("select")); // case-insensitive
        assert!(is_reserved("Where"));
        assert!(is_reserved("Group"));
        // Real, common NON-reserved keywords, usable as a bare identifier.
        assert!(!is_reserved("UUID"));
        assert!(!is_reserved("STATUS"));
        assert!(!is_reserved("VALUE"));
        assert!(!is_reserved("TYPE"));
        // Not a keyword at all.
        assert!(!is_reserved("MY_TABLE_NAME"));
    }
}
