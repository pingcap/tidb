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

//! Shared node state transcreated from `pkg/parser/ast/base.go`.

use std::cell::OnceCell;
use std::ops::{Deref, DerefMut};

use tidb_datatype::{Encoding, TransformOp};
use tidb_lexer::unescape_char;

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// Source text, decoded text, SQL mode, and source position shared by AST nodes.
///
/// Go uses an atomic state and a package mutex because its interface exposes a
/// lazy cache through shared pointers. A Rust AST node is mutated exclusively,
/// so `OnceCell` preserves the same lazy result without a global lock.
#[derive(Debug, Clone, Default)]
pub struct NodeText {
    utf8_text: OnceCell<Vec<u8>>,
    encoding: Option<Encoding>,
    no_backslash_escapes: bool,
    original: Vec<u8>,
    offset: usize,
}

/// Heap-owned AST payload carrying the source metadata embedded by Go's
/// private `node` base type.
///
/// Statement families were already boxed to keep the root enum small. Keeping
/// metadata at that existing ownership boundary preserves stable movement and
/// pattern matching without a side table or a second wrapper around `Stmt`.
#[derive(Debug, Clone)]
pub struct NodeBox<T> {
    value: Box<T>,
    text: NodeText,
}

impl<T> NodeBox<T> {
    /// Boxes `value` with empty source metadata.
    pub fn new(value: T) -> Self {
        Self {
            value: Box::new(value),
            text: NodeText::default(),
        }
    }

    /// Returns the payload's source metadata.
    pub const fn node_text(&self) -> &NodeText {
        &self.text
    }

    /// Returns mutable payload source metadata.
    pub fn node_text_mut(&mut self) -> &mut NodeText {
        &mut self.text
    }

    /// Replaces the node's exact source bytes.
    pub fn set_text(&mut self, encoding: Option<Encoding>, text: impl Into<Vec<u8>>) {
        self.text.set_text(encoding, text);
    }

    /// Marks whether `NO_BACKSLASH_ESCAPES` was active for this node.
    pub fn set_no_backslash_escapes(&mut self, value: bool) {
        self.text.set_no_backslash_escapes(value);
    }

    /// Returns the node text decoded to UTF-8.
    pub fn text(&self) -> &[u8] {
        self.text.text()
    }

    /// Returns the node's exact original source bytes.
    pub fn original_text(&self) -> &[u8] {
        self.text.original_text()
    }

    /// Sets the byte offset in the original SQL input.
    pub fn set_origin_text_position(&mut self, offset: usize) {
        self.text.set_origin_text_position(offset);
    }

    /// Returns the byte offset in the original SQL input.
    pub const fn origin_text_position(&self) -> usize {
        self.text.origin_text_position()
    }

    /// Consumes the wrapper and returns its payload.
    pub fn into_inner(self) -> T {
        *self.value
    }
}

// Source text and offsets describe where a node came from, not what the AST
// means. Keeping them out of equality mirrors TiDB's expression comparison,
// which clears origin positions before comparing trees.
impl<T: PartialEq> PartialEq for NodeBox<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: Eq> Eq for NodeBox<T> {}

impl<T: crate::Visitable> crate::Visitable for NodeBox<T> {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        self.value.accept(visitor)
    }
}

impl<T> AsRef<T> for NodeBox<T> {
    fn as_ref(&self) -> &T {
        &self.value
    }
}

impl<T> AsMut<T> for NodeBox<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

impl<T> Deref for NodeBox<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> DerefMut for NodeBox<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl PartialEq for NodeText {
    fn eq(&self, other: &Self) -> bool {
        self.encoding == other.encoding
            && self.no_backslash_escapes == other.no_backslash_escapes
            && self.original == other.original
            && self.offset == other.offset
    }
}

impl Eq for NodeText {}

impl NodeText {
    /// Replaces the source text and invalidates the decoded cache.
    pub fn set_text(&mut self, encoding: Option<Encoding>, text: impl Into<Vec<u8>>) {
        self.encoding = encoding;
        self.original = text.into();
        self.utf8_text.take();
    }

    /// Marks whether `NO_BACKSLASH_ESCAPES` was active for this node.
    pub fn set_no_backslash_escapes(&mut self, value: bool) {
        if self.no_backslash_escapes != value {
            self.no_backslash_escapes = value;
            self.utf8_text.take();
        }
    }

    /// Returns UTF-8 SQL text, preserving binary literals as hexadecimal.
    pub fn text(&self) -> &[u8] {
        self.utf8_text.get_or_init(|| match self.encoding {
            None => self.original.clone(),
            Some(encoding) => {
                convert_binary_string_literals(&self.original, encoding, self.no_backslash_escapes)
            }
        })
    }

    /// Returns the exact source bytes supplied by the parser.
    pub fn original_text(&self) -> &[u8] {
        &self.original
    }

    /// Sets the byte offset in the original SQL text.
    pub fn set_origin_text_position(&mut self, offset: usize) {
        self.offset = offset;
    }

    /// Returns the byte offset in the original SQL text.
    pub const fn origin_text_position(&self) -> usize {
        self.offset
    }
}

fn is_printable(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes)
        .is_ok_and(|text| text.chars().all(|character| !character.is_control()))
}

const fn is_ident_char(byte: u8) -> bool {
    byte == b'_' || byte.is_ascii_digit() || byte.is_ascii_lowercase() || byte.is_ascii_uppercase()
}

fn needs_space_before_hex_literal(text: &[u8], quote_start: usize) -> bool {
    quote_start > 0 && is_ident_char(text[quote_start - 1])
}

fn convert_binary_string_literals(
    source: &[u8],
    encoding: Encoding,
    no_backslash_escapes: bool,
) -> Vec<u8> {
    if !source.contains(&b'\'') && !source.contains(&b'"') {
        return encoding
            .transform(source, TransformOp::DECODE_REPLACE)
            .into_parts()
            .0;
    }

    let utf8_text = encoding
        .transform(source, TransformOp::DECODE_REPLACE)
        .into_parts()
        .0;
    let mut output: Option<Vec<u8>> = None;
    let mut last_copied = 0;
    let mut original_index = 0;
    let mut index = 0;

    while index < utf8_text.len() {
        if skip_comment(&utf8_text, source, &mut index, &mut original_index) {
            continue;
        }
        let quote = utf8_text[index];
        if quote != b'\'' && quote != b'"' {
            index += 1;
            continue;
        }

        let utf8_quote_start = index;
        index += 1;
        let Some(original_quote_start) = advance_original_to(source, &mut original_index, quote)
        else {
            break;
        };

        let mut original_quote_end = None;
        while index < utf8_text.len() {
            let byte = utf8_text[index];
            if byte == quote {
                index += 1;
                let Some(original_close) = advance_original_to(source, &mut original_index, quote)
                else {
                    break;
                };
                if index >= utf8_text.len() || utf8_text[index] != quote {
                    original_quote_end = Some(original_close + 1);
                    break;
                }
                index += 1;
                if advance_original_to(source, &mut original_index, quote).is_none() {
                    break;
                }
            } else if byte == b'\\' && !no_backslash_escapes && index + 1 < utf8_text.len() {
                let escaped = utf8_text[index + 1];
                index += 2;
                if matches!(escaped, b'\'' | b'"')
                    && advance_original_to(source, &mut original_index, escaped).is_none()
                {
                    break;
                }
            } else {
                index += 1;
            }
        }

        let Some(original_quote_end) = original_quote_end else {
            continue;
        };
        let decoded = encoding.transform(
            &source[original_quote_start + 1..original_quote_end - 1],
            TransformOp::DECODE,
        );
        if decoded.error().is_none() && is_printable(decoded.bytes()) {
            continue;
        }

        let mut content = Vec::new();
        let mut source_index = original_quote_start + 1;
        let source_end = original_quote_end - 1;
        while source_index < source_end {
            let byte = source[source_index];
            if byte == quote {
                source_index += 1;
                if source_index < source_end && source[source_index] == quote {
                    content.push(quote);
                    source_index += 1;
                }
            } else if byte == b'\\' && !no_backslash_escapes && source_index + 1 < source_end {
                source_index += 1;
                content.extend(unescape_char(source[source_index]));
                source_index += 1;
            } else {
                content.push(byte);
                source_index += 1;
            }
        }

        let output = output.get_or_insert_with(|| Vec::with_capacity(utf8_text.len()));
        output.extend_from_slice(&utf8_text[last_copied..utf8_quote_start]);
        if needs_space_before_hex_literal(&utf8_text, utf8_quote_start) {
            output.push(b' ');
        }
        output.extend_from_slice(b"0x");
        for byte in content {
            output.push(HEX_DIGITS[usize::from(byte >> 4)]);
            output.push(HEX_DIGITS[usize::from(byte & 0x0f)]);
        }
        last_copied = index;
    }

    let Some(mut output) = output else {
        return utf8_text;
    };
    output.extend_from_slice(&utf8_text[last_copied..]);
    output
}

fn skip_comment(
    utf8_text: &[u8],
    source: &[u8],
    index: &mut usize,
    original_index: &mut usize,
) -> bool {
    let position = *index;
    let byte = utf8_text[position];
    if byte == b'-'
        && utf8_text.get(position + 1) == Some(&b'-')
        && (position + 2 == utf8_text.len() || is_unicode_whitespace(&utf8_text[position + 2..]))
    {
        skip_to_eol(utf8_text, source, index, original_index);
        return true;
    }
    if byte == b'#' {
        skip_to_eol(utf8_text, source, index, original_index);
        return true;
    }
    if byte == b'/' && utf8_text.get(position + 1) == Some(&b'*') {
        if matches!(utf8_text.get(position + 2), Some(b'!' | b'+')) {
            return false;
        }
        skip_to_block_end(utf8_text, source, index, original_index);
        return true;
    }
    false
}

fn is_unicode_whitespace(source: &[u8]) -> bool {
    std::str::from_utf8(source)
        .ok()
        .and_then(|text| text.chars().next())
        .is_some_and(char::is_whitespace)
}

fn skip_to_eol(utf8_text: &[u8], source: &[u8], index: &mut usize, original_index: &mut usize) {
    while *index < utf8_text.len() {
        let byte = utf8_text[*index];
        if matches!(byte, b'\'' | b'"') {
            advance_original_to(source, original_index, byte);
        }
        *index += 1;
        if byte == b'\n' {
            return;
        }
    }
}

fn skip_to_block_end(
    utf8_text: &[u8],
    source: &[u8],
    index: &mut usize,
    original_index: &mut usize,
) {
    *index += 2;
    while *index < utf8_text.len() {
        let byte = utf8_text[*index];
        if matches!(byte, b'\'' | b'"') {
            advance_original_to(source, original_index, byte);
        }
        if byte == b'*' && utf8_text.get(*index + 1) == Some(&b'/') {
            *index += 2;
            return;
        }
        *index += 1;
    }
}

fn advance_original_to(source: &[u8], index: &mut usize, byte: u8) -> Option<usize> {
    while *index < source.len() {
        let position = *index;
        *index += 1;
        if source[position] == byte {
            return Some(position);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{Encoding, NodeText};

    fn check(encoding: Encoding, cases: &[(&str, &[u8], &[u8])]) {
        let mut node = NodeText::default();
        for (name, source, expected) in cases {
            node.set_text(Some(encoding), source.to_vec());
            assert_eq!(node.text(), *expected, "{name}");
            assert_eq!(node.original_text(), *source, "{name} source");
        }
    }

    #[test]
    fn test_node_set_text() {
        let mut node = NodeText::default();
        node.set_text(None, "你好".as_bytes());
        assert_eq!(node.text(), "你好".as_bytes());
        node.set_text(Some(Encoding::Gbk), b"\xd2\xbb".to_vec());
        assert_eq!(node.text(), "一".as_bytes());
        node.set_text(Some(Encoding::Gbk), b"\xc1\xd0".to_vec());
        assert_eq!(node.text(), "列".as_bytes());
        node.set_origin_text_position(17);
        assert_eq!(node.origin_text_position(), 17);
    }

    #[test]
    fn test_binary_string_literal_conversion_printable_rows() {
        check(
            Encoding::Utf8,
            &[
                ("single", b"SELECT 'hello world'", b"SELECT 'hello world'"),
                (
                    "double",
                    b"SELECT \"hello world\"",
                    b"SELECT \"hello world\"",
                ),
                (
                    "binary prefix",
                    b"SELECT _binary 'hello world'",
                    b"SELECT _binary 'hello world'",
                ),
                (
                    "utf8 prefix",
                    b"SELECT _utf8'hello world'",
                    b"SELECT _utf8'hello world'",
                ),
                (
                    "utf8mb4 prefix",
                    b"SELECT _utf8mb4'hello world'",
                    b"SELECT _utf8mb4'hello world'",
                ),
                (
                    "national prefix",
                    b"SELECT N'hello world'",
                    b"SELECT N'hello world'",
                ),
                (
                    "doubled quote",
                    b"SELECT 'it''s here'",
                    b"SELECT 'it''s here'",
                ),
                (
                    "escaped quote",
                    b"SELECT 'it\\'s here'",
                    b"SELECT 'it\\'s here'",
                ),
                (
                    "doubled double quote",
                    b"SELECT \"say \"\"hi\"\"\"",
                    b"SELECT \"say \"\"hi\"\"\"",
                ),
                (
                    "backtick",
                    b"SELECT 'has `backtick` inside'",
                    b"SELECT 'has `backtick` inside'",
                ),
                (
                    "prefix word inside",
                    b"SELECT 'the word _binary appears'",
                    b"SELECT 'the word _binary appears'",
                ),
                (
                    "backslash",
                    b"SELECT 'path\\\\to\\\\file'",
                    b"SELECT 'path\\\\to\\\\file'",
                ),
            ],
        );
    }

    #[test]
    fn test_binary_string_literal_conversion() {
        check(
            Encoding::Utf8,
            &[
                ("single", b"SELECT '\xd2\xe4\xa6\xb8'", b"SELECT 0xd2e4a6b8"),
                (
                    "double",
                    b"SELECT \"\xd2\xe4\xa6\xb8\"",
                    b"SELECT 0xd2e4a6b8",
                ),
                (
                    "binary prefix with space",
                    b"SELECT _binary '\xd2\xe4\xa6\xb8'",
                    b"SELECT _binary 0xd2e4a6b8",
                ),
                ("prefix", b"SELECT _binary'\x01'", b"SELECT _binary 0x01"),
                ("utf8 prefix", b"SELECT _utf8'\x01'", b"SELECT _utf8 0x01"),
                (
                    "utf8mb4 prefix",
                    b"SELECT _utf8mb4'\x01'",
                    b"SELECT _utf8mb4 0x01",
                ),
                ("doubled", b"SELECT '\xd2''\xe4'", b"SELECT 0xd227e4"),
                ("escaped", b"SELECT '\xd2\\'\xe4'", b"SELECT 0xd227e4"),
                (
                    "doubled double quote",
                    b"SELECT \"\xd2\"\"\xe4\"",
                    b"SELECT 0xd222e4",
                ),
                ("backtick", b"SELECT '\xd2`\xe4'", b"SELECT 0xd260e4"),
                (
                    "mixed arguments",
                    b"SELECT '\xd2\xe4', 'hello', _binary '\xa1\xb2'",
                    b"SELECT 0xd2e4, 'hello', _binary 0xa1b2",
                ),
                (
                    "truncated utf8",
                    b"SELECT '\xf0\x9f\x98'",
                    b"SELECT 0xf09f98",
                ),
                (
                    "invalid continuation",
                    b"SELECT '\x80\x81'",
                    b"SELECT 0x8081",
                ),
                ("nul", b"SELECT '\x00'", b"SELECT 0x00"),
                (
                    "mixed",
                    b"SELECT 'hello\x00world'",
                    b"SELECT 0x68656c6c6f00776f726c64",
                ),
                (
                    "multiple controls",
                    b"SELECT '\x01\x02\x03\x04\x05'",
                    b"SELECT 0x0102030405",
                ),
            ],
        );
    }

    #[test]
    fn test_binary_string_literal_skips_comments() {
        check(
            Encoding::Utf8,
            &[
                (
                    "dash",
                    b"-- don't\nSELECT 'hello'",
                    b"-- don't\nSELECT 'hello'",
                ),
                (
                    "commented sql",
                    b"-- SELECT * FROM t WHERE name='John'\nSELECT 1",
                    b"-- SELECT * FROM t WHERE name='John'\nSELECT 1",
                ),
                (
                    "double quote in comment",
                    b"-- see table \"users\"\nSELECT \"bar\" FROM t",
                    b"-- see table \"users\"\nSELECT \"bar\" FROM t",
                ),
                ("comment at eof", b"SELECT 1 -- don't", b"SELECT 1 -- don't"),
                (
                    "quote at eol",
                    b"-- ending with '\nSELECT 'hello'",
                    b"-- ending with '\nSELECT 'hello'",
                ),
                ("not a comment", b"SELECT 1 --1", b"SELECT 1 --1"),
                (
                    "hash",
                    b"# user's config\nSELECT 'value'",
                    b"# user's config\nSELECT 'value'",
                ),
                (
                    "block",
                    b"/* it's a test */ SELECT 'value'",
                    b"/* it's a test */ SELECT 'value'",
                ),
                (
                    "multiline block",
                    b"/*\n * don't modify\n */ SELECT 'value'",
                    b"/*\n * don't modify\n */ SELECT 'value'",
                ),
                (
                    "form feed",
                    b"--\x0c don't\nSELECT 'hello'",
                    b"--\x0c don't\nSELECT 'hello'",
                ),
                (
                    "vertical tab",
                    b"--\x0b don't\nSELECT 'hello'",
                    b"--\x0b don't\nSELECT 'hello'",
                ),
                (
                    "version",
                    b"/*!80000 SELECT '\xd2\xe4' */",
                    b"/*!80000 SELECT 0xd2e4 */",
                ),
                (
                    "hint",
                    b"/*+ SET_VAR(charset='\xd2\xe4') */ SELECT 1",
                    b"/*+ SET_VAR(charset=0xd2e4) */ SELECT 1",
                ),
                (
                    "tidb",
                    b"/*T![unsupported] don't */ SELECT 'hello'",
                    b"/*T![unsupported] don't */ SELECT 'hello'",
                ),
                (
                    "mariadb",
                    b"/*M! don't */ SELECT 'hello'",
                    b"/*M! don't */ SELECT 'hello'",
                ),
                (
                    "create view",
                    b"-- (don't use parenthesis)\n\nCREATE OR REPLACE VIEW v AS SELECT 'Attribute' AS t FROM t1 UNION ALL SELECT 'Reference' AS t FROM t2",
                    b"-- (don't use parenthesis)\n\nCREATE OR REPLACE VIEW v AS SELECT 'Attribute' AS t FROM t1 UNION ALL SELECT 'Reference' AS t FROM t2",
                ),
                (
                    "after",
                    b"-- don't\nSELECT '\xd2\xe4'",
                    b"-- don't\nSELECT 0xd2e4",
                ),
            ],
        );
    }

    #[test]
    fn test_binary_string_literal_no_backslash_escapes() {
        let mut node = NodeText::default();
        node.set_text(Some(Encoding::Utf8), b"SELECT '\\n'".to_vec());
        node.set_no_backslash_escapes(true);
        assert_eq!(node.text(), b"SELECT '\\n'");
        node.set_text(Some(Encoding::Utf8), b"SELECT '\\' , 'after'".to_vec());
        assert_eq!(node.text(), b"SELECT '\\' , 'after'");
        node.set_text(Some(Encoding::Utf8), b"SELECT '\xd2\xe4'".to_vec());
        assert_eq!(node.text(), b"SELECT 0xd2e4");
    }

    #[test]
    fn test_binary_string_literal_gbk() {
        check(
            Encoding::Gbk,
            &[
                (
                    "printable",
                    b"select '\xb1\xed1'",
                    "select '表1'".as_bytes(),
                ),
                ("invalid", b"select '\x80\xff'", b"select 0x80ff"),
                ("trail", b"select '\xb9\x5c'", "select '筡'".as_bytes()),
                (
                    "multiple",
                    b"select '\xb9\x5c\xc5\x5c'",
                    "select '筡臷'".as_bytes(),
                ),
                (
                    "before quote",
                    b"select '\xb9\x5c', 'after'",
                    "select '筡', 'after'".as_bytes(),
                ),
            ],
        );
    }

    #[test]
    fn benchmark_convert_binary_string_literals_source_shapes() {
        let query = |clauses: Vec<&str>| format!("SELECT * FROM t1 WHERE {}", clauses.join(" OR "));
        let binary = "c1 = _binary '\u{1}\u{2}\u{3}'";
        let printable = "c1 = 'hello world'";
        let no_quotes = "c1 = 12345";
        let cases = [
            query(vec![no_quotes]),
            query(vec![no_quotes; 200]),
            query(vec![printable]),
            query(vec![printable; 200]),
            query(vec![binary]),
            query(vec![binary; 200]),
            query(vec![binary, printable]),
            query(
                (0..200)
                    .map(|index| if index % 2 == 0 { binary } else { printable })
                    .collect(),
            ),
        ];
        for query in cases {
            let mut node = NodeText::default();
            node.set_text(Some(Encoding::Utf8), query.into_bytes());
            assert!(!node.text().is_empty());
        }
    }
}
