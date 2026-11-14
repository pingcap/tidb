//! Go `pkg/lightning/mydump/parser.go` and `parser.rl`: SQL dump rows and chunks.

use crate::{unescape, CsvError, EscapeFlavor};
use tidb_datatype::{parse_bit_str, parse_hex_str, Datum};

/// Go `mydump.Parser`: the row, column and checkpoint contract used by import.
pub trait Parser {
    /// Go `Parser.ReadRow`.
    fn read_row(&mut self) -> Result<(), CsvError>;
    /// Go `Parser.LastRow`.
    fn last_row(&self) -> &Row;
    /// Go `Parser.Pos`.
    fn pos(&self) -> (i64, i64);
    /// Go `Parser.SetPos`.
    fn set_pos(&mut self, pos: i64, row_id: i64) -> Result<(), CsvError>;
    /// Go `Parser.SetRowID`.
    fn set_row_id(&mut self, row_id: i64);
    /// Go `Parser.Columns`.
    fn columns(&self) -> &[String];
    /// Go `Parser.SetColumns`.
    fn set_columns(&mut self, columns: Vec<String>);
    /// Go's concrete parser type assertion in `HandleSkipNRows`.
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

/// Go `parsedef.Row`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Row {
    pub row_id: i64,
    pub row: Vec<Datum>,
    pub length: usize,
}

/// Go `mydump.Chunk`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Chunk {
    pub offset: i64,
    pub end_offset: i64,
    pub real_offset: i64,
    pub columns: Vec<String>,
    pub prev_row_id_max: i64,
    pub row_id_max: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Token {
    RowBegin,
    RowEnd,
    Values,
    Null,
    True,
    False,
    HexString,
    BinString,
    Integer,
    SingleQuoted,
    DoubleQuoted,
    BackQuoted,
    Unquoted,
}

/// Go `ChunkParser`, over the same decompressed in-memory source as `CsvParser`.
pub struct ChunkParser {
    source: Vec<u8>,
    available: usize,
    block_size: usize,
    last_chunk: bool,
    pos: usize,
    columns: Vec<String>,
    last_row: Row,
    no_backslash_escapes: bool,
}

impl ChunkParser {
    /// Go `NewChunkParser` with the default Lightning read block size.
    pub fn new(source: Vec<u8>, no_backslash_escapes: bool) -> Self {
        Self::with_block_size(source, no_backslash_escapes, 64 * 1024)
    }

    /// Go `NewChunkParser`; the supplied read size is scaled by `BufferSizeScale`.
    pub fn with_block_size(
        source: Vec<u8>,
        no_backslash_escapes: bool,
        read_block_size: usize,
    ) -> Self {
        assert!(read_block_size > 0);
        Self {
            source,
            available: 0,
            block_size: read_block_size * 5,
            last_chunk: false,
            pos: 0,
            columns: Vec::new(),
            last_row: Row::default(),
            no_backslash_escapes,
        }
    }

    /// Go `blockParser.Pos`.
    pub fn pos(&self) -> (i64, i64) {
        (self.pos as i64, self.last_row.row_id)
    }
    /// Go `blockParser.SetPos`.
    pub fn set_pos(&mut self, pos: i64, row_id: i64) -> Result<(), CsvError> {
        if pos < 0 {
            return Err(CsvError::Other("negative seek position".to_owned()));
        }
        self.pos = pos as usize;
        self.last_row.row_id = row_id;
        Ok(())
    }
    /// Go `blockParser.SetRowID`.
    pub fn set_row_id(&mut self, row_id: i64) {
        self.last_row.row_id = row_id;
    }
    /// Go `blockParser.Columns`.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }
    /// Go `blockParser.SetColumns`.
    pub fn set_columns(&mut self, columns: Vec<String>) {
        self.columns = columns;
    }
    /// Go `blockParser.LastRow`.
    pub fn last_row(&self) -> &Row {
        &self.last_row
    }

    /// Go `ChunkParser.lex` and `blockParser.readBlock` at read boundaries.
    fn lex(&mut self) -> Result<(Token, Vec<u8>), CsvError> {
        loop {
            match self.lex_buffer() {
                Ok(token) => return Ok(token),
                Err(LexError::Parse(error)) => return Err(error),
                Err(LexError::NeedMore(start)) => {
                    self.pos = start;
                    let remaining = self.source.len().saturating_sub(self.available);
                    self.last_chunk = remaining < self.block_size;
                    self.available += remaining.min(self.block_size);
                    if self.pos == 0
                        && self.available >= 3
                        && self.source.starts_with(b"\xef\xbb\xbf")
                    {
                        self.pos = 3;
                    }
                }
            }
        }
    }

    /// Go `ChunkParser.lex`; token precedence and longest matches follow `parser.rl`.
    fn lex_buffer(&mut self) -> Result<(Token, Vec<u8>), LexError> {
        let s = &self.source[..self.available];
        let mut i = self.pos;
        loop {
            if i >= s.len() {
                if !self.last_chunk {
                    return Err(LexError::NeedMore(i));
                }
                return Err(LexError::Parse(CsvError::Eof));
            }
            if is_space(s[i]) || matches!(s[i], b',' | b';') {
                if i + 1 == s.len() && !self.last_chunk {
                    return Err(LexError::NeedMore(i));
                }
                i += 1;
                continue;
            }
            if s[i..].starts_with(b"/*") {
                let Some(end) = s[i + 2..].windows(2).position(|w| w == b"*/") else {
                    if !self.last_chunk {
                        return Err(LexError::NeedMore(i));
                    }
                    return Err(LexError::Parse(CsvError::Other(
                        "syntax error: unexpected EOF".to_owned(),
                    )));
                };
                i += end + 4;
                continue;
            }
            if s[i..].starts_with(b"--") {
                let comment_start = i;
                while i < s.len() && !matches!(s[i], b'\r' | b'\n') {
                    i += 1;
                }
                if i == s.len() && !self.last_chunk {
                    return Err(LexError::NeedMore(comment_start));
                }
                continue;
            }
            let mut skipped = false;
            for word in [b"convert(".as_slice(), b"using utf8mb4)"] {
                if !self.last_chunk
                    && s.len() - i < word.len()
                    && word[..s.len() - i].eq_ignore_ascii_case(&s[i..])
                {
                    return Err(LexError::NeedMore(i));
                }
                if s.get(i..i + word.len())
                    .is_some_and(|v| v.eq_ignore_ascii_case(word))
                {
                    i += word.len();
                    skipped = true;
                    break;
                }
            }
            if skipped {
                continue;
            }
            break;
        }
        let start = i;
        let tok = match s[i] {
            b'(' => {
                i += 1;
                Token::RowBegin
            }
            b')' => {
                i += 1;
                Token::RowEnd
            }
            b'\'' | b'"' | b'`' => {
                let quote = s[i];
                i += 1;
                if i == s.len() && !self.last_chunk {
                    return Err(LexError::NeedMore(start));
                }
                if i == s.len() && quote != b'`' {
                    return Err(LexError::Parse(CsvError::Other(
                        "syntax error: unexpected EOF".to_owned(),
                    )));
                }
                let mut last_close = None;
                loop {
                    if i >= s.len() {
                        if !self.last_chunk {
                            return Err(LexError::NeedMore(start));
                        }
                        if let Some(end) = last_close {
                            i = end;
                            break;
                        }
                        return Err(LexError::Parse(CsvError::Other("syntax error".to_owned())));
                    }
                    if s[i] == b'\\'
                        && quote != b'`'
                        && !self.no_backslash_escapes
                        && i + 1 < s.len()
                    {
                        i += 2;
                        continue;
                    }
                    if s[i] == quote {
                        i += 1;
                        last_close = Some(i);
                        if i == s.len() && !self.last_chunk {
                            return Err(LexError::NeedMore(start));
                        }
                        if s.get(i) == Some(&quote) {
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                match quote {
                    b'\'' => Token::SingleQuoted,
                    b'"' => Token::DoubleQuoted,
                    _ => Token::BackQuoted,
                }
            }
            b'/' | b'*' => {
                if s[i] == b'/' && i + 1 == s.len() && !self.last_chunk {
                    return Err(LexError::NeedMore(start));
                }
                return Err(LexError::Parse(CsvError::Other(
                    if s[i] == b'/' && i + 1 == s.len() {
                        "syntax error: unexpected EOF"
                    } else {
                        "syntax error"
                    }
                    .to_owned(),
                )));
            }
            _ => {
                while i < s.len() && !is_space(s[i]) && !b",;()'\"`/*".contains(&s[i]) {
                    i += 1;
                }
                if i == s.len() && !self.last_chunk {
                    return Err(LexError::NeedMore(start));
                }
                let text = &s[start..i];
                if text.len() == 1
                    && matches!(text[0], b'x' | b'X' | b'b' | b'B')
                    && s.get(i) == Some(&b'\'')
                {
                    let end = s[i + 1..]
                        .iter()
                        .position(|&v| v == b'\'')
                        .map(|n| i + 1 + n);
                    let hex = text[0].eq_ignore_ascii_case(&b'x');
                    if end.is_none()
                        && !self.last_chunk
                        && s[i + 1..].iter().all(|v| {
                            if hex {
                                v.is_ascii_hexdigit()
                            } else {
                                matches!(v, b'0' | b'1')
                            }
                        })
                    {
                        return Err(LexError::NeedMore(start));
                    }
                    if let Some(end) = end.filter(|&end| {
                        s[i + 1..end].iter().all(|v| {
                            if hex {
                                v.is_ascii_hexdigit()
                            } else {
                                matches!(v, b'0' | b'1')
                            }
                        })
                    }) {
                        i = end + 1;
                        if hex {
                            Token::HexString
                        } else {
                            Token::BinString
                        }
                    } else {
                        Token::Unquoted
                    }
                } else if text.eq_ignore_ascii_case(b"values") {
                    Token::Values
                } else if text.eq_ignore_ascii_case(b"null") {
                    Token::Null
                } else if text.eq_ignore_ascii_case(b"true") {
                    Token::True
                } else if text.eq_ignore_ascii_case(b"false") {
                    Token::False
                } else if text
                    .strip_prefix(b"-")
                    .unwrap_or(text)
                    .iter()
                    .all(u8::is_ascii_digit)
                    && text != b"-"
                {
                    Token::Integer
                } else if text.starts_with(b"0x")
                    && text.len() > 2
                    && text[2..].iter().all(u8::is_ascii_hexdigit)
                {
                    Token::HexString
                } else if text.starts_with(b"0b")
                    && text.len() > 2
                    && text[2..].iter().all(|v| matches!(v, b'0' | b'1'))
                {
                    Token::BinString
                } else {
                    Token::Unquoted
                }
            }
        };
        self.pos = i;
        Ok((tok, s[start..i].to_vec()))
    }

    /// Go `ChunkParser.unescapeString`.
    fn unescape_string(&self, input: &[u8]) -> Vec<u8> {
        if input.len() < 2 || !matches!(input[0], b'\'' | b'"' | b'`') {
            return input.to_vec();
        }
        let quote = input[0];
        let content = &input[1..input.len() - 1];
        let mut dedup = Vec::with_capacity(content.len());
        let mut i = 0;
        while i < content.len() {
            dedup.push(content[i]);
            if content[i] == quote && content.get(i + 1) == Some(&quote) {
                i += 1;
            }
            i += 1;
        }
        let flavor = if quote == b'`' || self.no_backslash_escapes {
            EscapeFlavor::None
        } else {
            EscapeFlavor::MySql
        };
        unescape(&dedup, flavor, b'\\')
    }

    /// Go `ChunkParser.ReadRow`.
    pub fn read_row(&mut self) -> Result<(), CsvError> {
        #[derive(Clone, Copy, PartialEq)]
        enum State {
            TableName,
            Columns,
            Values,
            Row,
        }
        let mut state = State::Values;
        self.last_row.length = 0;
        loop {
            let (tok, content) = match self.lex() {
                Err(CsvError::Eof) if state != State::Values => {
                    return Err(CsvError::Other(format!(
                        "syntax error: premature EOF at offset {}",
                        self.pos
                    )))
                }
                result => result?,
            };
            self.last_row.length += content.len();
            let expected = match state {
                State::TableName => "table name",
                State::Columns => "column list",
                State::Values => "start of row",
                State::Row => "data literal",
            };
            match (state, tok) {
                (State::TableName, Token::RowBegin) => state = State::Columns,
                (State::TableName, Token::Values) | (State::Columns, Token::RowEnd) => {
                    state = State::Values
                }
                (State::TableName, Token::Unquoted | Token::DoubleQuoted | Token::BackQuoted) => {}
                (State::Columns, Token::Unquoted | Token::DoubleQuoted | Token::BackQuoted) => self
                    .columns
                    .push(String::from_utf8_lossy(&self.unescape_string(&content)).to_lowercase()),
                (State::Values, Token::RowBegin) => {
                    self.last_row.row_id += 1;
                    self.last_row.row.clear();
                    state = State::Row;
                }
                (State::Values, Token::Unquoted | Token::DoubleQuoted | Token::BackQuoted) => {
                    self.columns.clear();
                    state = State::TableName;
                }
                (State::Values, Token::Values) => {}
                (State::Row, Token::RowEnd) => return Ok(()),
                (State::Row, Token::Null) => self.last_row.row.push(Datum::Null),
                (State::Row, Token::True) => self.last_row.row.push(Datum::Int(1)),
                (State::Row, Token::False) => self.last_row.row.push(Datum::Int(0)),
                (
                    State::Row,
                    Token::Integer | Token::Unquoted | Token::SingleQuoted | Token::DoubleQuoted,
                ) => {
                    let text = String::from_utf8_lossy(&content);
                    let int = if tok != Token::Integer {
                        None
                    } else if content[0] == b'-' {
                        text.parse::<i64>().ok().map(Datum::Int)
                    } else {
                        text.parse::<u64>().ok().map(Datum::UInt)
                    };
                    self.last_row.row.push(int.unwrap_or_else(|| {
                        Datum::new_string_datum(self.unescape_string(&content))
                    }));
                }
                (State::Row, Token::HexString | Token::BinString) => {
                    let text = String::from_utf8_lossy(&content);
                    let value = if tok == Token::HexString {
                        parse_hex_str(&text)
                    } else {
                        parse_bit_str(&text)
                    }
                    .map_err(|e| CsvError::Other(e.to_string()))?;
                    self.last_row.row.push(Datum::new_binary_literal(value));
                }
                _ => {
                    return Err(CsvError::Other(format!(
                        "syntax error: unexpected {tok:?} ({}) at offset {}, expecting {expected}",
                        String::from_utf8_lossy(&content),
                        self.pos
                    )))
                }
            }
        }
    }
}

impl Parser for ChunkParser {
    fn read_row(&mut self) -> Result<(), CsvError> {
        Self::read_row(self)
    }
    fn last_row(&self) -> &Row {
        Self::last_row(self)
    }
    fn pos(&self) -> (i64, i64) {
        Self::pos(self)
    }
    fn set_pos(&mut self, pos: i64, row_id: i64) -> Result<(), CsvError> {
        Self::set_pos(self, pos, row_id)
    }
    fn set_row_id(&mut self, row_id: i64) {
        Self::set_row_id(self, row_id)
    }
    fn columns(&self) -> &[String] {
        Self::columns(self)
    }
    fn set_columns(&mut self, columns: Vec<String>) {
        Self::set_columns(self, columns)
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl Parser for crate::CsvParser {
    fn read_row(&mut self) -> Result<(), CsvError> {
        Self::read_row(self).map(|_| ())
    }
    fn last_row(&self) -> &Row {
        &self.last_row
    }
    fn pos(&self) -> (i64, i64) {
        (self.pos, self.last_row.row_id)
    }
    fn set_pos(&mut self, pos: i64, row_id: i64) -> Result<(), CsvError> {
        if pos < 0 {
            return Err(CsvError::Other("negative seek position".to_owned()));
        }
        self.source_offset = pos as usize;
        self.pos = pos;
        self.last_row.row_id = row_id;
        Ok(())
    }
    fn set_row_id(&mut self, row_id: i64) {
        self.last_row.row_id = row_id;
    }
    fn columns(&self) -> &[String] {
        &self.columns
    }
    fn set_columns(&mut self, columns: Vec<String>) {
        self.columns = columns;
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Go `ReadChunks`.
pub fn read_chunks(parser: &mut dyn Parser, min_size: i64) -> Result<Vec<Chunk>, CsvError> {
    let (pos, row_id) = parser.pos();
    let mut current = Chunk {
        offset: pos,
        end_offset: pos,
        prev_row_id_max: row_id,
        row_id_max: row_id,
        ..Chunk::default()
    };
    let mut chunks = Vec::new();
    loop {
        match parser.read_row() {
            Ok(()) => {
                (current.end_offset, current.row_id_max) = parser.pos();
                if current.end_offset - current.offset >= min_size {
                    chunks.push(current.clone());
                    current.offset = current.end_offset;
                    current.prev_row_id_max = current.row_id_max;
                }
            }
            Err(CsvError::Eof) => {
                if current.offset < current.end_offset {
                    chunks.push(current);
                }
                return Ok(chunks);
            }
            Err(e) => return Err(e),
        }
    }
}

/// Go `ReadUntil`.
pub fn read_until(parser: &mut dyn Parser, pos: i64) -> Result<(), CsvError> {
    let mut current = 0;
    while current < pos {
        match parser.read_row() {
            Ok(()) => current = parser.pos().0,
            Err(CsvError::Eof) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "parser_tests.rs"]
mod tests;

/// Go Ragel `space`, including the vertical tab.
fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t'..=b'\r')
}

enum LexError {
    NeedMore(usize),
    Parse(CsvError),
}
