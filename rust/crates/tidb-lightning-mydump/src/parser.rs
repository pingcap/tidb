//! Go `pkg/lightning/mydump/parser.go` and `parser.rl`: SQL dump rows and chunks.

use crate::{unescape, CsvError, EscapeFlavor};
use tidb_datatype::{parse_bit_str, parse_hex_str, Datum};
use std::ops::Range;

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
    /// Go `Parser.ScannedPos`: the position in the underlying (compressed)
    /// file.
    fn scanned_pos(&mut self) -> Result<i64, CsvError>;
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

/// Go `ChunkParser`: the SQL dump parser.
pub struct ChunkParser {
    reader: crate::BlockReader,
    /// The unparsed bytes, starting at file offset `base`.
    buf: Vec<u8>,
    base: usize,
    last_chunk: bool,
    pos: usize,
    columns: Vec<String>,
    last_row: Row,
    no_backslash_escapes: bool,
}

impl ChunkParser {
    /// Go `NewChunkParser` over a whole in-memory file with the default
    /// read block size.
    pub fn new(source: Vec<u8>, no_backslash_escapes: bool) -> Self {
        Self::with_block_size(source, no_backslash_escapes, crate::READ_BLOCK_SIZE)
    }

    /// [`Self::new`] with a read block size.
    pub fn with_block_size(source: Vec<u8>, no_backslash_escapes: bool, read_block_size: usize) -> Self {
        Self::from_reader(Box::new(std::io::Cursor::new(source)), no_backslash_escapes, read_block_size)
    }

    /// Go `NewChunkParser`: the read size is scaled by `BufferSizeScale`.
    pub fn from_reader(reader: Box<dyn crate::ReadSeek>, no_backslash_escapes: bool, read_block_size: usize) -> Self {
        assert!(read_block_size > 0);
        Self {
            reader: crate::BlockReader::new(reader, read_block_size * crate::BUFFER_SIZE_SCALE),
            buf: Vec::new(),
            base: 0,
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
        self.reader.seek(pos)?;
        self.buf.clear();
        self.base = pos as usize;
        self.last_chunk = false;
        self.pos = pos as usize;
        self.last_row.row_id = row_id;
        Ok(())
    }

    /// Go `blockParser.ScannedPos`.
    pub fn scanned_pos(&mut self) -> Result<i64, CsvError> {
        self.reader.scanned_pos()
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
    fn lex(&mut self) -> Result<(Token, Range<usize>), CsvError> {
        let mut scan = Scan::Start;
        loop {
            match self.lex_buffer(scan) {
                Ok(token) => return Ok(token),
                Err(LexError::Parse(error)) => return Err(error),
                Err(LexError::NeedMore(start, resume)) => {
                    // Go `readBlock`: keep the unparsed remainder, append the
                    // next block; the block at position 0 loses its BOM.
                    if start != 0 {
                        self.buf.drain(..start);
                    }
                    self.base += start;
                    self.pos = self.base;
                    let (block, last) = self.reader.read_block()?;
                    self.last_chunk = last;
                    if self.base == 0 && self.buf.is_empty() && block.starts_with(b"\xef\xbb\xbf") {
                        self.buf.extend_from_slice(&block[3..]);
                        self.base = 3;
                        self.pos = 3;
                    } else if self.buf.is_empty() {
                        self.buf = block;
                    } else {
                        self.buf.extend_from_slice(&block);
                    }
                    scan = resume;
                }
            }
        }
    }

    /// Go `ChunkParser.lex`; token precedence and longest matches follow `parser.rl`.
    fn lex_buffer(&mut self, mut scan: Scan) -> Result<(Token, Range<usize>), LexError> {
        let s = &self.buf[..];
        let mut i = self.pos - self.base;
        loop {
            if i >= s.len() {
                if !self.last_chunk {
                    return Err(LexError::NeedMore(i, Scan::Start));
                }
                return Err(LexError::Parse(CsvError::Eof));
            }
            if is_space(s[i]) || matches!(s[i], b',' | b';') {
                if i + 1 == s.len() && !self.last_chunk {
                    return Err(LexError::NeedMore(i, Scan::Start));
                }
                i += 1;
                continue;
            }
            if s[i..].starts_with(b"/*") {
                let cursor = match scan {
                    Scan::BlockComment(next) => i + next,
                    _ => i + 2,
                };
                let Some(end) = memchr::memmem::find(&s[cursor..], b"*/") else {
                    if !self.last_chunk {
                        // Recheck the last byte in case it starts the terminator.
                        let next = (s.len() - 1).max(i + 2) - i;
                        return Err(LexError::NeedMore(i, Scan::BlockComment(next)));
                    }
                    return Err(LexError::Parse(CsvError::Other(
                        "syntax error: unexpected EOF".to_owned(),
                    )));
                };
                i = cursor + end + 2;
                scan = Scan::Start;
                continue;
            }
            if s[i..].starts_with(b"--") {
                let cursor = match scan {
                    Scan::LineComment(next) => i + next,
                    _ => i + 2,
                };
                if let Some(end) = memchr::memchr2(b'\r', b'\n', &s[cursor..]) {
                    i = cursor + end;
                } else if !self.last_chunk {
                    return Err(LexError::NeedMore(i, Scan::LineComment(s.len() - i)));
                } else {
                    i = s.len();
                }
                scan = Scan::Start;
                continue;
            }
            let mut skipped = false;
            for word in [b"convert(".as_slice(), b"using utf8mb4)"] {
                if !self.last_chunk
                    && s.len() - i < word.len()
                    && word[..s.len() - i].eq_ignore_ascii_case(&s[i..])
                {
                    return Err(LexError::NeedMore(i, Scan::Start));
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
                i = self.quoted_end(start, quote, scan)?;
                match quote {
                    b'\'' => Token::SingleQuoted,
                    b'"' => Token::DoubleQuoted,
                    _ => Token::BackQuoted,
                }
            }
            b'/' | b'*' => {
                if s[i] == b'/' && i + 1 == s.len() && !self.last_chunk {
                    return Err(LexError::NeedMore(start, Scan::Start));
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
                if let Scan::Unquoted(next) = scan {
                    i = start + next;
                }
                while i < s.len() && !is_space(s[i]) && !b",;()'\"`/*".contains(&s[i]) {
                    i += 1;
                }
                if i == s.len() && !self.last_chunk {
                    return Err(LexError::NeedMore(start, Scan::Unquoted(i - start)));
                }
                let text = &s[start..i];
                if text.len() == 1
                    && matches!(text[0], b'x' | b'X' | b'b' | b'B')
                    && s.get(i) == Some(&b'\'')
                {
                    let hex = text[0].eq_ignore_ascii_case(&b'x');
                    if let Some(end) = self.quoted_base_end(start, i + 1, hex, scan)? {
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
        self.pos = self.base + i;
        Ok((tok, start..i))
    }

    fn quoted_end(&self, start: usize, quote: u8, scan: Scan) -> Result<usize, LexError> {
        let bytes = &self.buf;
        let mut cursor = match scan {
            Scan::Quoted { next, .. } => start + next,
            _ => start + 1,
        };
        let mut last_close = match scan {
            Scan::Quoted { last_close, .. } => last_close,
            _ => None,
        };
        if self.last_chunk && quote != b'`' && start + 1 == bytes.len() {
            return Err(LexError::Parse(CsvError::Other(
                "syntax error: unexpected EOF".to_owned(),
            )));
        }
        let escapes = quote != b'`' && !self.no_backslash_escapes;
        let resume_at = |cursor, last_close| {
            LexError::NeedMore(start, Scan::Quoted { next: cursor - start, last_close })
        };
        loop {
            let tail = &bytes[cursor..];
            let hit = if escapes {
                memchr::memchr2(quote, b'\\', tail)
            } else {
                memchr::memchr(quote, tail)
            };
            let Some(offset) = hit else {
                if !self.last_chunk {
                    return Err(resume_at(bytes.len(), last_close));
                }
                return last_close
                    .map(|end| start + end)
                    .ok_or_else(|| LexError::Parse(CsvError::Other("syntax error".to_owned())));
            };
            let found = cursor + offset;
            let next = found + 1;
            // Leave an undecided quote or escape to be read with the next block.
            if next == bytes.len() && !self.last_chunk {
                return Err(resume_at(found, last_close));
            }
            if bytes[found] != quote {
                cursor = (next + 1).min(bytes.len());
            } else if bytes.get(next) == Some(&quote) {
                last_close = Some(next - start);
                cursor = next + 1;
            } else {
                return Ok(next);
            }
        }
    }

    fn quoted_base_end(
        &self, start: usize, digits: usize, hex: bool, scan: Scan,
    ) -> Result<Option<usize>, LexError> {
        let cursor = match scan {
            Scan::QuotedBase(next) => start + next,
            _ => digits,
        };
        let invalid = |&byte: &u8| {
            if hex { !byte.is_ascii_hexdigit() } else { !matches!(byte, b'0' | b'1') }
        };
        let end = self.buf[cursor..]
            .iter()
            .position(invalid)
            .map_or(self.buf.len(), |offset| cursor + offset);
        if self.buf.get(end) == Some(&b'\'') {
            return Ok(Some(end));
        }
        if end == self.buf.len() && !self.last_chunk {
            return Err(LexError::NeedMore(start, Scan::QuotedBase(end - start)));
        }
        Ok(None)
    }

    /// Go `ChunkParser.unescapeString`.
    fn unescape_string(input: &[u8], no_backslash_escapes: bool) -> Vec<u8> {
        if input.len() < 2 || !matches!(input[0], b'\'' | b'"' | b'`') {
            return input.to_vec();
        }
        let quote = input[0];
        let content = &input[1..input.len() - 1];
        let flavor = if quote == b'`' || no_backslash_escapes {
            EscapeFlavor::None
        } else {
            EscapeFlavor::MySql
        };
        let doubled = [quote; 2];
        let Some(mut pair) = memchr::memmem::find(content, &doubled) else {
            return unescape(content, flavor, b'\\');
        };
        let mut dedup = Vec::with_capacity(content.len() - 1);
        let mut remaining = content;
        loop {
            dedup.extend_from_slice(&remaining[..=pair]);
            remaining = &remaining[pair + 2..];
            let Some(next) = memchr::memmem::find(remaining, &doubled) else {
                break;
            };
            pair = next;
        }
        dedup.extend_from_slice(remaining);
        if flavor != EscapeFlavor::None {
            if let Some(first) = memchr::memchr(b'\\', &dedup).filter(|&i| i + 1 < dedup.len()) {
                return crate::unescape_from(&dedup, b'\\', first);
            }
        }
        dedup
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
            let (tok, range) = match self.lex() {
                Err(CsvError::Eof) if state != State::Values => {
                    return Err(CsvError::Other(format!(
                        "syntax error: premature EOF at offset {}",
                        self.pos
                    )))
                }
                result => result?,
            };
            let content = &self.buf[range];
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
                    .push(String::from_utf8_lossy(&Self::unescape_string(content, self.no_backslash_escapes)).to_lowercase()),
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
                    let int = if tok == Token::Integer {
                        let text = std::str::from_utf8(content).expect("integer tokens are ASCII");
                        if content[0] == b'-' {
                            text.parse::<i64>().ok().map(Datum::Int)
                        } else {
                            text.parse::<u64>().ok().map(Datum::UInt)
                        }
                    } else {
                        None
                    };
                    self.last_row.row.push(int.unwrap_or_else(|| {
                        Datum::new_string_datum(Self::unescape_string(content, self.no_backslash_escapes))
                    }));
                }
                (State::Row, Token::HexString | Token::BinString) => {
                    let text = String::from_utf8_lossy(content);
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
                        String::from_utf8_lossy(content),
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
    fn scanned_pos(&mut self) -> Result<i64, CsvError> {
        Self::scanned_pos(self)
    }
}

impl Parser for crate::CsvParser {
    fn read_row(&mut self) -> Result<(), CsvError> {
        self.read_next_row(None)
    }
    fn last_row(&self) -> &Row {
        &self.last_row
    }
    fn pos(&self) -> (i64, i64) {
        (self.pos, self.last_row.row_id)
    }
    fn set_pos(&mut self, pos: i64, row_id: i64) -> Result<(), CsvError> {
        Self::set_pos(self, pos, row_id)
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
    fn scanned_pos(&mut self) -> Result<i64, CsvError> {
        Self::scanned_pos(self)
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

// Relative to the start of the unfinished token, which becomes offset zero
// when lex refills the buffer.
#[derive(Clone, Copy)]
enum Scan {
    Start,
    BlockComment(usize),
    LineComment(usize),
    Quoted { next: usize, last_close: Option<usize> },
    Unquoted(usize),
    QuotedBase(usize),
}

enum LexError {
    NeedMore(usize, Scan),
    Parse(CsvError),
}
