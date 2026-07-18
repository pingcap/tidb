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

//! Token kinds and the token record produced by [`crate::Lexer`].

/// The lexical class of a token. This is the tokenization-level class; the
/// parser maps [`TokenKind::Keyword`] spans to specific keyword ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    /// End of input.
    Eof,
    /// An illegal character or malformed literal.
    Invalid,
    /// An identifier (bare, backtick-quoted, or ANSI double-quoted).
    Ident,
    /// A recognized keyword or builtin-function keyword.
    Keyword,
    /// A character-set introducer immediately preceding a string literal
    /// (`_utf8mb4'x'`, `_latin1'x'`, `N'x'`/`n'x'`) — a GENUINELY DIFFERENT
    /// token class from [`TokenKind::Keyword`] in real TiDB's own scanner
    /// (`underscoreCS`, distinct from e.g. `binaryType`), even though
    /// `text` holds the SAME canonical charset name a plain keyword token
    /// might otherwise carry (`_binary` and bare `BINARY` both end up with
    /// `text == "binary"`) — the two are syntactically UNRELATED
    /// (`_binary'x'` is a charset-tagged string literal; bare `BINARY 'x'`
    /// is the unary cast-to-binary operator), so collapsing them into one
    /// `TokenKind` would make the parser unable to tell them apart.
    CharsetIntroducer,
    /// A string literal.
    Str,
    /// An integer literal that fits the scanner's integer path.
    IntLit,
    /// A floating-point literal (has a valid exponent).
    FloatLit,
    /// A fixed-point decimal literal (has `.`, no exponent).
    DecLit,
    /// A hexadecimal literal (`0x..` or `x'..'`).
    HexLit,
    /// A bit literal (`0b..` or `b'..'`).
    BitLit,
    /// A user (`@x`) or system (`@@x`) variable reference.
    UserVar,
    /// An operator or punctuation token; `text` holds its exact source.
    Op,
    /// An optimizer hint comment `/*+ ... */` retained as a token (only in the
    /// positions the scanner recognizes hints); `text` is the full comment.
    HintComment,
}

/// A scanned token with its source span and text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    /// The lexical class.
    pub kind: TokenKind,
    /// Byte offset of the token start in the source.
    pub offset: usize,
    /// Byte offset just past the token end.
    pub end_offset: usize,
    /// The token's source text. For identifiers this is the raw span; for
    /// operators it is the exact operator text.
    pub text: String,
}

impl Token {
    /// Renders the engine-neutral differential label used by the `difftest`
    /// crate to compare against the Go scanner's `godump` output.
    pub fn label(&self) -> String {
        match self.kind {
            TokenKind::Eof => "EOF".to_string(),
            TokenKind::Invalid => "INVALID".to_string(),
            TokenKind::Ident => format!("IDENT:{}", self.text),
            // Real TiDB's own scanner surfaces `underscoreCS` through the
            // SAME `KW:<name>` label `godump` uses for a plain keyword —
            // confirmed via the existing `charset_introducer` lexer test,
            // unaffected by promoting this out of `TokenKind::Keyword`.
            TokenKind::Keyword | TokenKind::CharsetIntroducer => {
                format!("KW:{}", ascii_upper(&self.text))
            }
            TokenKind::Str => "STR".to_string(),
            TokenKind::IntLit => "NUM:INT".to_string(),
            TokenKind::FloatLit => "NUM:FLOAT".to_string(),
            TokenKind::DecLit => "NUM:DEC".to_string(),
            TokenKind::HexLit => "NUM:HEX".to_string(),
            TokenKind::BitLit => "NUM:BIT".to_string(),
            TokenKind::UserVar => format!("VAR:{}", self.text),
            // The Go scanner surfaces a hint comment as a token whose Lit is the
            // full `/*+ ... */` span; godump labels that OP:<raw>.
            TokenKind::Op | TokenKind::HintComment => format!("OP:{}", self.text),
        }
    }
}

fn ascii_upper(s: &str) -> String {
    s.chars().map(|c| c.to_ascii_uppercase()).collect()
}
