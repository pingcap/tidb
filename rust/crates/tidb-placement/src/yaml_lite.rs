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

//! boundary: `gopkg.in/yaml.v2`'s `UnmarshalStrict`, restricted to the two
//! shapes Go `pkg/ddl/placement` decodes.
//!
//! No YAML crate is vendored in this offline workspace, so the two decode
//! targets Go uses — `[]string` for array constraints and survival
//! preferences, and `map[string]int` for dict constraints — are parsed here
//! directly. The parser reproduces the go-yaml behaviors this package's
//! semantics actually depend on:
//!
//! - a plain scalar in flow context ends at `,`, `?`, `[`, `]`, `{`, `}`, or at
//!   a `:` that is followed by a blank; a `:` followed by a non-blank stays
//!   *inside* the scalar, which is what turns `{+region=us-east-2:2}` into a
//!   single key with a null value and drives Go's wrong-separator diagnosis;
//! - only the first document's node is decoded and trailing tokens are
//!   ignored, which is why go-yaml accepts `{"-zone=sh,+zone=bj": 4}}`;
//! - an empty input leaves the target at its zero value without an error;
//! - `UnmarshalStrict` rejects duplicate mapping keys.
//!
//! Narrowed away, none of which occurs in a placement constraint string:
//! anchors and aliases, tags, multi-document streams, block scalars (`|`/`>`),
//! nested block collections, multi-line plain-scalar folding, and go-yaml's
//! non-decimal integer resolutions. Its error text is this module's own; Go
//! never surfaces a go-yaml message through a sentinel, only through the
//! explanatory tail of `ErrInvalidConstraintsFormat`.

use std::fmt;

/// A parse or decode failure standing in for a `gopkg.in/yaml.v2` error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct YamlError {
    message: String,
}

impl YamlError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for YamlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Node {
    Null,
    Scalar { value: String, quoted: bool },
    Sequence(Vec<Node>),
    Mapping(Vec<(Node, Node)>),
}

/// Go `yaml.UnmarshalStrict(input, &[]string{})`.
pub(crate) fn unmarshal_strict_string_slice(input: &[u8]) -> Result<Vec<String>, YamlError> {
    decode_string_slice(&parse_document(input)?)
}

/// Go `yaml.UnmarshalStrict(input, &map[string]int{})`, keeping the source
/// order of the mapping's keys where Go's map randomizes it.
pub(crate) fn unmarshal_strict_string_int_map(
    input: &[u8],
) -> Result<Vec<(String, i64)>, YamlError> {
    decode_string_int_map(&parse_document(input)?)
}

fn decode_string_slice(node: &Node) -> Result<Vec<String>, YamlError> {
    match node {
        Node::Null => Ok(Vec::new()),
        Node::Sequence(items) => items
            .iter()
            .map(|item| match item {
                Node::Null => Ok(String::new()),
                Node::Scalar { value, .. } => Ok(value.clone()),
                _ => Err(YamlError::new(
                    "cannot unmarshal !!seq or !!map into string",
                )),
            })
            .collect(),
        Node::Scalar { .. } => Err(YamlError::new("cannot unmarshal !!str into []string")),
        Node::Mapping(_) => Err(YamlError::new("cannot unmarshal !!map into []string")),
    }
}

fn decode_string_int_map(node: &Node) -> Result<Vec<(String, i64)>, YamlError> {
    let entries = match node {
        Node::Null => return Ok(Vec::new()),
        Node::Mapping(entries) => entries,
        Node::Scalar { .. } => {
            return Err(YamlError::new("cannot unmarshal !!str into map[string]int"))
        }
        Node::Sequence(_) => {
            return Err(YamlError::new("cannot unmarshal !!seq into map[string]int"))
        }
    };

    let mut decoded: Vec<(String, i64)> = Vec::with_capacity(entries.len());
    for (key, value) in entries {
        let key = match key {
            Node::Scalar { value, .. } => value.clone(),
            Node::Null => String::new(),
            _ => {
                return Err(YamlError::new(
                    "cannot unmarshal !!seq or !!map into string",
                ))
            }
        };
        if decoded.iter().any(|(existing, _)| *existing == key) {
            return Err(YamlError::new(format!("key {key:?} already set in map")));
        }
        let value = match value {
            Node::Null => 0,
            Node::Scalar {
                value,
                quoted: false,
            } => value.parse::<i64>().map_err(|_| {
                YamlError::new(format!("cannot unmarshal !!str `{value}` into int"))
            })?,
            Node::Scalar {
                value,
                quoted: true,
            } => {
                return Err(YamlError::new(format!(
                    "cannot unmarshal !!str `{value}` into int"
                )))
            }
            _ => return Err(YamlError::new("cannot unmarshal !!seq or !!map into int")),
        };
        decoded.push((key, value));
    }
    Ok(decoded)
}

fn parse_document(input: &[u8]) -> Result<Node, YamlError> {
    let input =
        std::str::from_utf8(input).map_err(|_| YamlError::new("invalid UTF-8 in YAML document"))?;
    let mut parser = Parser {
        input,
        bytes: input.as_bytes(),
        pos: 0,
    };
    parser.skip_ignorable();
    if parser.at_end() {
        return Ok(Node::Null);
    }
    // Only the first document's node is decoded; go-yaml's document-end state
    // accepts whatever token follows without consuming it.
    parser.parse_block_node()
}

const fn is_blank(byte: u8) -> bool {
    byte == b' ' || byte == b'\t'
}

const fn is_break(byte: u8) -> bool {
    byte == b'\n' || byte == b'\r'
}

const fn is_flow_indicator(byte: u8) -> bool {
    matches!(byte, b',' | b'?' | b'[' | b']' | b'{' | b'}')
}

struct Parser<'a> {
    input: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl Parser<'_> {
    fn at_end(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<u8> {
        self.bytes.get(self.pos + offset).copied()
    }

    /// Blanks, line breaks, and `#` comments, as between any two tokens.
    fn skip_ignorable(&mut self) {
        loop {
            match self.peek() {
                Some(byte) if is_blank(byte) || is_break(byte) => self.pos += 1,
                Some(b'#') => {
                    while !self.at_end() && !is_break(self.bytes[self.pos]) {
                        self.pos += 1;
                    }
                }
                _ => return,
            }
        }
    }

    fn skip_blanks(&mut self) {
        while matches!(self.peek(), Some(byte) if is_blank(byte)) {
            self.pos += 1;
        }
    }

    /// The zero-based column of the current position on its line.
    fn column(&self) -> usize {
        let line_start = self.bytes[..self.pos]
            .iter()
            .rposition(|byte| is_break(*byte))
            .map_or(0, |index| index + 1);
        self.pos - line_start
    }

    fn parse_block_node(&mut self) -> Result<Node, YamlError> {
        match self.peek() {
            Some(b'[' | b'{') => self.parse_flow_node(),
            Some(byte) if is_flow_indicator(byte) => Err(YamlError::new(
                "did not find expected node content".to_owned(),
            )),
            Some(b'-')
                if self
                    .peek_at(1)
                    .is_none_or(|next| is_blank(next) || is_break(next)) =>
            {
                self.parse_block_sequence()
            }
            Some(b'&' | b'*' | b'!' | b'|' | b'>' | b'%' | b'@' | b'`') => Err(YamlError::new(
                "unsupported YAML construct in a placement option",
            )),
            Some(_) => self.parse_block_scalar_or_mapping(),
            None => Ok(Node::Null),
        }
    }

    fn parse_block_sequence(&mut self) -> Result<Node, YamlError> {
        let indent = self.column();
        let mut items = Vec::new();
        loop {
            self.pos += 1; // the '-' indicator
            self.skip_blanks();
            items.push(self.parse_block_entry_value()?);
            self.skip_ignorable();
            if self.at_end() {
                break;
            }
            if self.peek() == Some(b'-')
                && self
                    .peek_at(1)
                    .is_none_or(|next| is_blank(next) || is_break(next))
            {
                if self.column() != indent {
                    return Err(YamlError::new("inconsistent block sequence indentation"));
                }
                continue;
            }
            break;
        }
        Ok(Node::Sequence(items))
    }

    fn parse_block_scalar_or_mapping(&mut self) -> Result<Node, YamlError> {
        let indent = self.column();
        let mut key = self.scan_scalar(false)?;
        self.skip_blanks();
        if self.peek() != Some(b':') {
            return Ok(key);
        }

        let mut entries = Vec::new();
        loop {
            self.pos += 1; // the ':' indicator
            self.skip_blanks();
            let value = self.parse_block_entry_value()?;
            entries.push((key, value));
            self.skip_ignorable();
            if self.at_end() {
                return Ok(Node::Mapping(entries));
            }
            if self.column() != indent {
                return Err(YamlError::new("inconsistent block mapping indentation"));
            }
            key = self.scan_scalar(false)?;
            self.skip_blanks();
            if self.peek() != Some(b':') {
                return Err(YamlError::new("could not find expected ':'"));
            }
        }
    }

    /// The value after a `-` or `:` indicator: a flow collection, a scalar, or
    /// nothing at all (a null value).
    fn parse_block_entry_value(&mut self) -> Result<Node, YamlError> {
        match self.peek() {
            None => Ok(Node::Null),
            Some(byte) if is_break(byte) => Ok(Node::Null),
            Some(b'#') => {
                self.skip_ignorable();
                Ok(Node::Null)
            }
            Some(b'[' | b'{') => self.parse_flow_node(),
            Some(_) => self.scan_scalar(false),
        }
    }

    fn parse_flow_node(&mut self) -> Result<Node, YamlError> {
        self.skip_ignorable();
        match self.peek() {
            Some(b'[') => self.parse_flow_sequence(),
            Some(b'{') => self.parse_flow_mapping(),
            Some(byte) if is_flow_indicator(byte) => {
                Err(YamlError::new("did not find expected node content"))
            }
            None => Ok(Node::Null),
            Some(_) => self.scan_scalar(true),
        }
    }

    fn parse_flow_sequence(&mut self) -> Result<Node, YamlError> {
        self.pos += 1; // '['
        let mut items = Vec::new();
        loop {
            self.skip_ignorable();
            match self.peek() {
                Some(b']') => {
                    self.pos += 1;
                    return Ok(Node::Sequence(items));
                }
                None => return Err(YamlError::new("did not find expected ',' or ']'")),
                _ => {}
            }
            items.push(self.parse_flow_node()?);
            self.skip_ignorable();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b']') => {
                    self.pos += 1;
                    return Ok(Node::Sequence(items));
                }
                _ => return Err(YamlError::new("did not find expected ',' or ']'")),
            }
        }
    }

    fn parse_flow_mapping(&mut self) -> Result<Node, YamlError> {
        self.pos += 1; // '{'
        let mut entries: Vec<(Node, Node)> = Vec::new();
        loop {
            self.skip_ignorable();
            match self.peek() {
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(Node::Mapping(entries));
                }
                None => return Err(YamlError::new("did not find expected ',' or '}'")),
                _ => {}
            }
            let key = self.parse_flow_node()?;
            self.skip_ignorable();
            let value = if self.peek() == Some(b':') {
                self.pos += 1;
                self.skip_ignorable();
                match self.peek() {
                    Some(b',' | b'}') | None => Node::Null,
                    _ => self.parse_flow_node()?,
                }
            } else {
                Node::Null
            };
            entries.push((key, value));
            self.skip_ignorable();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(Node::Mapping(entries));
                }
                _ => return Err(YamlError::new("did not find expected ',' or '}'")),
            }
        }
    }

    fn scan_scalar(&mut self, flow: bool) -> Result<Node, YamlError> {
        match self.peek() {
            Some(b'"') => self.scan_double_quoted(),
            Some(b'\'') => self.scan_single_quoted(),
            _ => Ok(self.scan_plain(flow)),
        }
    }

    fn scan_plain(&mut self, flow: bool) -> Node {
        let start = self.pos;
        let mut end = self.pos;
        while let Some(byte) = self.peek() {
            if is_break(byte) {
                break;
            }
            if byte == b'#' && self.pos > start && is_blank(self.bytes[self.pos - 1]) {
                break;
            }
            // go-yaml ends a plain scalar at ':' only when a blank follows, so
            // `a:b` is one scalar while `a: b` is a key/value pair.
            if byte == b':'
                && self
                    .peek_at(1)
                    .is_none_or(|next| is_blank(next) || is_break(next))
            {
                break;
            }
            if flow && is_flow_indicator(byte) {
                break;
            }
            self.pos += 1;
            if !is_blank(byte) {
                end = self.pos;
            }
        }
        Node::Scalar {
            value: self.input[start..end].to_owned(),
            quoted: false,
        }
    }

    fn scan_single_quoted(&mut self) -> Result<Node, YamlError> {
        self.pos += 1; // the opening quote
        let mut value = String::new();
        loop {
            match self.peek() {
                None => return Err(YamlError::new("found unexpected end of stream")),
                Some(b'\'') => {
                    if self.peek_at(1) == Some(b'\'') {
                        value.push('\'');
                        self.pos += 2;
                        continue;
                    }
                    self.pos += 1;
                    return Ok(Node::Scalar {
                        value,
                        quoted: true,
                    });
                }
                Some(_) => {
                    let next = self.next_char();
                    value.push(next);
                }
            }
        }
    }

    fn scan_double_quoted(&mut self) -> Result<Node, YamlError> {
        self.pos += 1; // the opening quote
        let mut value = String::new();
        loop {
            match self.peek() {
                None => return Err(YamlError::new("found unexpected end of stream")),
                Some(b'"') => {
                    self.pos += 1;
                    return Ok(Node::Scalar {
                        value,
                        quoted: true,
                    });
                }
                Some(b'\\') => {
                    self.pos += 1;
                    let escaped = self
                        .peek()
                        .ok_or_else(|| YamlError::new("found unexpected end of stream"))?;
                    self.pos += 1;
                    value.push(match escaped {
                        b'n' => '\n',
                        b't' => '\t',
                        b'r' => '\r',
                        b'0' => '\0',
                        other => char::from(other),
                    });
                }
                Some(_) => {
                    let next = self.next_char();
                    value.push(next);
                }
            }
        }
    }

    /// Consumes one whole UTF-8 character, keeping `pos` on a char boundary.
    fn next_char(&mut self) -> char {
        let next = self.input[self.pos..]
            .chars()
            .next()
            .expect("caller checked for a remaining byte");
        self.pos += next.len_utf8();
        next
    }
}

#[cfg(test)]
mod tests {
    use super::{unmarshal_strict_string_int_map, unmarshal_strict_string_slice};

    #[test]
    fn flow_sequences_and_scalars() {
        assert_eq!(
            unmarshal_strict_string_slice(b"[]").expect("empty flow sequence"),
            Vec::<String>::new()
        );
        assert_eq!(
            unmarshal_strict_string_slice(b"").expect("empty document"),
            Vec::<String>::new()
        );
        assert_eq!(
            unmarshal_strict_string_slice(br#"["+zone=sh", "+region=sh"]"#).expect("quoted items"),
            vec!["+zone=sh".to_owned(), "+region=sh".to_owned()]
        );
        assert_eq!(
            unmarshal_strict_string_slice(b"[+region=us]").expect("plain items"),
            vec!["+region=us".to_owned()]
        );
        assert_eq!(
            unmarshal_strict_string_slice(b"- +zone=sh\n- +zone=bj").expect("block sequence"),
            vec!["+zone=sh".to_owned(), "+zone=bj".to_owned()]
        );
        assert!(unmarshal_strict_string_slice(b"]").is_err());
        assert!(unmarshal_strict_string_slice(b"-region=us]").is_err());
        assert!(unmarshal_strict_string_slice(b"+region=us]").is_err());
        assert!(unmarshal_strict_string_slice(br#"{"+region=us": 3}"#).is_err());
    }

    #[test]
    fn flow_mappings() {
        assert_eq!(
            unmarshal_strict_string_int_map(br#"{"+zone=sh,-zone=bj":2, "+zone=sh": 1}"#)
                .expect("double quoted keys"),
            vec![
                ("+zone=sh,-zone=bj".to_owned(), 2),
                ("+zone=sh".to_owned(), 1)
            ]
        );
        assert_eq!(
            unmarshal_strict_string_int_map(b"{'+zone=sh,-zone=bj':2, '+zone=sh': 1}")
                .expect("single quoted keys"),
            vec![
                ("+zone=sh,-zone=bj".to_owned(), 2),
                ("+zone=sh".to_owned(), 1)
            ]
        );
        assert_eq!(
            unmarshal_strict_string_int_map(b"{+disk=ssd: 1}").expect("plain key"),
            vec![("+disk=ssd".to_owned(), 1)]
        );
        // A ':' with no blank after it stays inside the plain scalar, so the
        // whole token is the key and the value is null.
        assert_eq!(
            unmarshal_strict_string_int_map(b"{+region=us-east-2:2}").expect("wrong separator"),
            vec![("+region=us-east-2:2".to_owned(), 0)]
        );
        // go-yaml decodes only the first document's node and ignores the rest.
        assert_eq!(
            unmarshal_strict_string_int_map(br#"{"-zone=sh,+zone=bj": 4}}"#)
                .expect("trailing token ignored"),
            vec![("-zone=sh,+zone=bj".to_owned(), 4)]
        );
        assert_eq!(
            unmarshal_strict_string_int_map(b"+zone=sh: 2\n+zone=bj: 1").expect("block mapping"),
            vec![("+zone=sh".to_owned(), 2), ("+zone=bj".to_owned(), 1)]
        );
        assert!(unmarshal_strict_string_int_map(br#"{+ne=sh,-zone=bj:1, "+zone=sh": 4"#).is_err());
        assert!(unmarshal_strict_string_int_map(br#"{"a": 1, "a": 2}"#).is_err());
    }
}
