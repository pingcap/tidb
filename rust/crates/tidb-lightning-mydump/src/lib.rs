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

//! Go `pkg/lightning/mydump`'s CSV reader as `LOAD DATA` drives it:
//! `CSVParser` (`csv_parser.go`), the `blockParser` it embeds and the
//! unescape helpers (`parser.go`), `byteSet` (`bytes.go`) and
//! `CharsetConvertor` (`charset_convertor.go`).
//!
//! The source is the whole (decompressed) client file. It is still fed to
//! the lexer in Go's `LoadDataReadBlockSize * BufferSizeScale` blocks, because
//! the block boundary is observable: the BOM is stripped from the block read
//! at position 0, and the txn-entry-size limit is checked only when a token
//! spans blocks.

use tidb_datatype::Datum;

/// Go `config.CSVConfig`, the fields `LOAD DATA` sets.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CsvConfig {
    /// `FieldsTerminatedBy`.
    pub fields_terminated_by: Vec<u8>,
    /// `FieldsEnclosedBy`.
    pub fields_enclosed_by: Vec<u8>,
    /// `LinesTerminatedBy`.
    pub lines_terminated_by: Vec<u8>,
    /// `NotNull`.
    pub not_null: bool,
    /// `FieldNullDefinedBy`.
    pub field_null_defined_by: Vec<Vec<u8>>,
    /// `TrimLastEmptyField`.
    pub trim_last_empty_field: bool,
    /// `FieldsEscapedBy`.
    pub fields_escaped_by: Vec<u8>,
    /// `LinesStartingBy`.
    pub lines_starting_by: Vec<u8>,
    /// `AllowEmptyLine`.
    pub allow_empty_line: bool,
    /// `QuotedNullIsText`.
    pub quoted_null_is_text: bool,
    /// `UnescapedQuote`.
    pub unescaped_quote: bool,
}

/// Go `config.ReadBlockSize` (64 KiB) times `config.BufferSizeScale` (5).
const BLOCK_BUF_SIZE: usize = 64 * 1024 * 5;

/// Go `config.MaxTxnEntrySizeLimit`, `mydump.LargestEntryLimit`'s value.
const LARGEST_ENTRY_LIMIT: i64 = 120 * 1024 * 1024;

/// A reader error, with Go's error text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CsvError {
    /// `io.EOF`.
    Eof,
    /// `errUnterminatedQuotedField`.
    UnterminatedQuotedField,
    /// `errDanglingBackslash`.
    DanglingBackslash,
    /// `errUnexpectedQuoteField`.
    UnexpectedQuoteField,
    /// The txn-entry-size limit check in `readUntil`.
    RowTooLarge,
    /// A charset conversion or configuration failure.
    Other(String),
}

impl std::fmt::Display for CsvError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Eof => "EOF",
            Self::UnterminatedQuotedField => "syntax error: unterminated quoted field",
            Self::DanglingBackslash => "syntax error: no character after backslash",
            Self::UnexpectedQuoteField => {
                "syntax error: cannot have consecutive fields without separator"
            }
            Self::RowTooLarge => "size of row cannot exceed the max value of txn-entry-size-limit",
            Self::Other(message) => message,
        })
    }
}

/// Go `byteSet`: a 256-bit membership set.
#[derive(Clone, Copy, Debug, Default)]
struct ByteSet([u32; 8]);

impl ByteSet {
    /// Go `makeByteSet`.
    fn new(chars: &[u8]) -> Self {
        let mut set = [0_u32; 8];
        for &c in chars {
            set[usize::from(c >> 5)] |= 1 << u32::from(c & 31);
        }
        Self(set)
    }

    fn contains(&self, c: u8) -> bool {
        self.0[usize::from(c >> 5)] & (1 << u32::from(c & 31)) != 0
    }

    /// Go `IndexAnyByte`.
    fn index_any(&self, s: &[u8]) -> Option<usize> {
        s.iter().position(|&c| self.contains(c))
    }
}

/// Go `escapeFlavor`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EscapeFlavor {
    None,
    MySql,
    MySqlWithNull,
}

/// Go `config.Charset` as `ParseCharset` reads it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceCharset {
    Binary,
    Utf8mb4,
    Gb18030,
    Gbk,
    Latin1,
    Ascii,
}

/// Go `CharsetConvertor`: decodes a GB18030 / GBK / Latin1 (Windows-1252)
/// source to utf8mb4, replacing an undecodable character with
/// `invalid_char_replacement`.
#[derive(Clone, Debug)]
pub struct CharsetConvertor {
    source: SourceCharset,
    invalid_char_replacement: String,
}

impl CharsetConvertor {
    /// Go `NewCharsetConvertor`.
    pub fn new(data_character_set: &str, invalid_char_replacement: &str) -> Result<Self, String> {
        // Go `config.ParseCharset`.
        let source = match data_character_set.to_lowercase().as_str() {
            "" | "binary" => SourceCharset::Binary,
            "utf8" | "utf8mb4" => SourceCharset::Utf8mb4,
            "gb18030" => SourceCharset::Gb18030,
            "gbk" => SourceCharset::Gbk,
            "latin1" => SourceCharset::Latin1,
            "ascii" => SourceCharset::Ascii,
            _ => {
                return Err(format!(
                    "found unsupported data-character-set: {data_character_set}"
                ))
            }
        };
        Ok(Self {
            source,
            invalid_char_replacement: invalid_char_replacement.to_owned(),
        })
    }

    /// Go `precheck`: whether a conversion applies at all.
    fn converts(&self, src: &[u8]) -> bool {
        !src.is_empty()
            && matches!(
                self.source,
                SourceCharset::Gb18030 | SourceCharset::Gbk | SourceCharset::Latin1
            )
    }

    /// Go `Decode`: the source charset to utf8mb4.
    pub fn decode(&self, src: &[u8]) -> Vec<u8> {
        if !self.converts(src) {
            return src.to_vec();
        }
        let decoded: Vec<u8> = match self.source {
            SourceCharset::Latin1 => {
                let (text, _, _) = encoding_rs::WINDOWS_1252.decode(src);
                text.into_owned().into_bytes()
            }
            SourceCharset::Gbk | SourceCharset::Gb18030 => {
                let encoding = if self.source == SourceCharset::Gbk {
                    tidb_datatype::Encoding::Gbk
                } else {
                    tidb_datatype::Encoding::Gb18030
                };
                let mut out = Vec::with_capacity(src.len() * 3 / 2);
                encoding.foreach(src, tidb_datatype::TransformOp::default(), |_, to, _| {
                    out.extend_from_slice(to);
                    true
                });
                out
            }
            _ => unreachable!("converts() admitted only converting charsets"),
        };
        // Go `strings.ReplaceAll(res, string(utf8.RuneError), replacement)`.
        let text = String::from_utf8_lossy(&decoded);
        text.replace('\u{FFFD}', &self.invalid_char_replacement)
            .into_bytes()
    }

    /// Go `Encode`: utf8mb4 to the source charset.
    pub fn encode(&self, src: &[u8]) -> Vec<u8> {
        if !self.converts(src) {
            return src.to_vec();
        }
        let text = String::from_utf8_lossy(src);
        match self.source {
            SourceCharset::Latin1 => {
                let (bytes, _, _) = encoding_rs::WINDOWS_1252.encode(&text);
                bytes.into_owned()
            }
            SourceCharset::Gbk | SourceCharset::Gb18030 => {
                let encoding = if self.source == SourceCharset::Gbk {
                    tidb_datatype::Encoding::Gbk
                } else {
                    tidb_datatype::Encoding::Gb18030
                };
                encoding
                    .transform(src, tidb_datatype::TransformOp::ENCODE_REPLACE)
                    .into_parts()
                    .0
            }
            _ => unreachable!("converts() admitted only converting charsets"),
        }
    }
}

/// Go `unescape` with no delimiter.
fn unescape(input: &[u8], flavor: EscapeFlavor, esc_char: u8) -> Vec<u8> {
    if flavor == EscapeFlavor::None {
        return input.to_vec();
    }
    unescape_by_char(input, esc_char)
}

/// Go `unescapeByChar`: rewrites every `esc_char`-prefixed pair; a trailing
/// lone `esc_char` stays.
fn unescape_by_char(input: &[u8], esc_char: u8) -> Vec<u8> {
    let Some(first) = input.iter().position(|&c| c == esc_char) else {
        return input.to_vec();
    };
    if first + 1 == input.len() {
        return input.to_vec();
    }
    let mut result = Vec::with_capacity(input.len() - 1);
    result.extend_from_slice(&input[..first]);
    let mut remaining = &input[first..];
    while remaining.len() > 1 {
        result.push(escaped_char_to_byte(remaining[1]));
        remaining = &remaining[2..];
        match remaining.iter().position(|&c| c == esc_char) {
            None => {
                result.extend_from_slice(remaining);
                return result;
            }
            Some(next) => {
                result.extend_from_slice(&remaining[..next]);
                remaining = &remaining[next..];
            }
        }
    }
    result.extend_from_slice(remaining);
    result
}

/// Go `escapedCharToByte`.
fn escaped_char_to_byte(input: u8) -> u8 {
    match input {
        b'0' => 0,
        b'b' => 0x08,
        b'n' => b'\n',
        b'r' => b'\r',
        b't' => b'\t',
        b'Z' => 0x1a,
        other => other,
    }
}

/// Go `csvToken`.
type CsvToken = i16;
const CSV_TOKEN_ANY_UNQUOTED: CsvToken = 0;
const CSV_TOKEN_ESCAPED: CsvToken = 0x100;
const CSV_TOKEN_COMMA: CsvToken = 0x200;
const CSV_TOKEN_NEW_LINE: CsvToken = 0x400;
const CSV_TOKEN_DELIMITER: CsvToken = 0x800;

/// Go `field`.
#[derive(Clone, Debug)]
struct Field {
    content: Vec<u8>,
    quoted: bool,
}

/// Go `CSVParser` over an in-memory source.
pub struct CsvParser {
    // blockParser
    source: Vec<u8>,
    source_offset: usize,
    buf: Vec<u8>,
    start: usize,
    pos: i64,
    check_row_len: bool,
    row_start_pos: i64,
    // CSVParser
    not_null: bool,
    field_null_defined_by: Vec<Vec<u8>>,
    trim_last_empty_field: bool,
    comma: Vec<u8>,
    quote: Vec<u8>,
    new_line: Vec<u8>,
    starting_by: Vec<u8>,
    escaped_by: Vec<u8>,
    charset_convertor: Option<CharsetConvertor>,
    quote_byte_set: ByteSet,
    unquote_byte_set: ByteSet,
    new_line_byte_set: ByteSet,
    record_buffer: Vec<u8>,
    field_indexes: Vec<usize>,
    field_is_quoted: Vec<bool>,
    esc_flavor: EscapeFlavor,
    allow_empty_line: bool,
    quoted_null_is_text: bool,
    unescaped_quote: bool,
}

impl CsvParser {
    /// Go `NewCSVParser` (no header parsing, as LOAD DATA builds it).
    pub fn new(
        config: &CsvConfig,
        source: Vec<u8>,
        charset_convertor: Option<CharsetConvertor>,
    ) -> Result<Self, CsvError> {
        let (field_terminator, delimiter, line_terminator) = match &charset_convertor {
            None => (
                config.fields_terminated_by.clone(),
                config.fields_enclosed_by.clone(),
                config.lines_terminated_by.clone(),
            ),
            Some(convertor) => (
                convertor.encode(&config.fields_terminated_by),
                convertor.encode(&config.fields_enclosed_by),
                convertor.encode(&config.lines_terminated_by),
            ),
        };
        let mut quote_stop_set = Vec::new();
        let mut unquote_stop_set = vec![field_terminator[0]];
        if !config.fields_enclosed_by.is_empty() {
            quote_stop_set.push(delimiter[0]);
            unquote_stop_set.push(delimiter[0]);
        }
        let new_line_stop_set = if line_terminator.is_empty() {
            // The encodings of '\r' and '\n' agree in UTF-8 and GBK.
            vec![b'\r', b'\n']
        } else {
            vec![line_terminator[0]]
        };
        unquote_stop_set.extend_from_slice(&new_line_stop_set);
        if !config.lines_starting_by.is_empty()
            && contains_subslice(&config.lines_starting_by, &line_terminator)
        {
            return Err(CsvError::Other(format!(
                "STARTING BY '{}' cannot contain LINES TERMINATED BY '{}'",
                String::from_utf8_lossy(&config.lines_starting_by),
                String::from_utf8_lossy(&line_terminator)
            )));
        }
        let mut esc_flavor = EscapeFlavor::None;
        if !config.fields_escaped_by.is_empty() {
            esc_flavor = EscapeFlavor::MySql;
            quote_stop_set.push(config.fields_escaped_by[0]);
            unquote_stop_set.push(config.fields_escaped_by[0]);
            // MySQL's NULL value `\N` needs special treatment.
            let mut null_marker = config.fields_escaped_by.clone();
            null_marker.push(b'N');
            if !config.not_null && config.field_null_defined_by.contains(&null_marker) {
                esc_flavor = EscapeFlavor::MySqlWithNull;
            }
        }
        Ok(Self {
            source,
            source_offset: 0,
            buf: Vec::new(),
            start: 0,
            pos: 0,
            check_row_len: false,
            row_start_pos: 0,
            not_null: config.not_null,
            field_null_defined_by: config.field_null_defined_by.clone(),
            trim_last_empty_field: config.trim_last_empty_field,
            comma: field_terminator,
            quote: delimiter,
            new_line: line_terminator,
            starting_by: config.lines_starting_by.clone(),
            escaped_by: config.fields_escaped_by.clone(),
            charset_convertor,
            quote_byte_set: ByteSet::new(&quote_stop_set),
            unquote_byte_set: ByteSet::new(&unquote_stop_set),
            new_line_byte_set: ByteSet::new(&new_line_stop_set),
            record_buffer: Vec::new(),
            field_indexes: Vec::new(),
            field_is_quoted: Vec::new(),
            esc_flavor,
            allow_empty_line: config.allow_empty_line,
            quoted_null_is_text: config.quoted_null_is_text,
            unescaped_quote: config.unescaped_quote,
        })
    }

    fn rest(&self) -> &[u8] {
        &self.buf[self.start..]
    }

    fn set_buf(&mut self, buf: Vec<u8>) {
        self.buf = buf;
        self.start = 0;
    }

    /// Go `blockParser.readBlock`: appends the next block (`io.ReadFull`)
    /// to the unparsed bytes; the block read at position 0 loses its BOM.
    fn read_block(&mut self) {
        let end = (self.source_offset + BLOCK_BUF_SIZE).min(self.source.len());
        let mut block = &self.source[self.source_offset..end];
        self.source_offset = end;
        if self.pos == 0 {
            // Go `bom.Clean`.
            if block.starts_with(&[0xEF, 0xBB, 0xBF]) {
                block = &block[3..];
                self.pos += 3;
            }
        }
        let mut next = Vec::with_capacity(self.rest().len() + block.len());
        next.extend_from_slice(self.rest());
        next.extend_from_slice(block);
        self.set_buf(next);
    }

    fn begin_row_len_check(&mut self) {
        self.check_row_len = true;
        self.row_start_pos = self.pos;
    }

    fn end_row_len_check(&mut self) {
        self.check_row_len = false;
    }

    /// Go `readByte`.
    fn read_byte(&mut self) -> Result<u8, CsvError> {
        if self.rest().is_empty() {
            self.read_block();
        }
        let Some(&byte) = self.rest().first() else {
            return Err(CsvError::Eof);
        };
        self.start += 1;
        self.pos += 1;
        Ok(byte)
    }

    /// Go `peekBytes`.
    fn peek_bytes(&mut self, count: usize) -> Result<&[u8], CsvError> {
        if self.rest().len() < count {
            self.read_block();
        }
        if self.rest().is_empty() {
            return Err(CsvError::Eof);
        }
        let count = count.min(self.rest().len());
        Ok(&self.rest()[..count])
    }

    fn skip_bytes(&mut self, n: usize) {
        self.start += n;
        self.pos += n as i64;
    }

    /// Go `tryPeekExact`: (matched, eof).
    fn try_peek_exact(&mut self, content: &[u8]) -> (bool, bool) {
        if content.is_empty() {
            return (true, false);
        }
        match self.peek_bytes(content.len()) {
            Ok(bytes) => (bytes == content, false),
            Err(_) => (false, true),
        }
    }

    /// Go `tryReadExact`.
    fn try_read_exact(&mut self, content: &[u8]) -> bool {
        let (matched, _) = self.try_peek_exact(content);
        if matched {
            self.skip_bytes(content.len());
        }
        matched
    }

    fn try_read_new_line(&mut self, b: u8) -> bool {
        if self.new_line.is_empty() {
            return b == b'\r' || b == b'\n';
        }
        if b != self.new_line[0] {
            return false;
        }
        let rest = self.new_line[1..].to_vec();
        self.try_read_exact(&rest)
    }

    fn try_read_open_delimiter(&mut self, b: u8) -> bool {
        if self.quote.is_empty() || self.quote[0] != b {
            return false;
        }
        let rest = self.quote[1..].to_vec();
        self.try_read_exact(&rest)
    }

    fn try_read_close_delimiter(&mut self, b: u8) -> bool {
        if self.quote[0] != b {
            return false;
        }
        let rest = self.quote[1..].to_vec();
        self.try_read_exact(&rest)
    }

    fn try_read_comma(&mut self, b: u8) -> bool {
        if self.comma[0] != b {
            return false;
        }
        let rest = self.comma[1..].to_vec();
        self.try_read_exact(&rest)
    }

    /// Go `tryReadEscaped`.
    fn try_read_escaped(&mut self, b: u8) -> Result<Option<u8>, CsvError> {
        if self.escaped_by.is_empty()
            || b != self.escaped_by[0]
            || self.esc_flavor == EscapeFlavor::None
        {
            return Ok(None);
        }
        match self.read_byte() {
            Ok(byte) => Ok(Some(byte)),
            Err(CsvError::Eof) => Err(CsvError::DanglingBackslash),
            Err(error) => Err(error),
        }
    }

    /// Go `readQuotedToken`.
    fn read_quoted_token(&mut self, b: u8) -> Result<CsvToken, CsvError> {
        if self.try_read_close_delimiter(b) {
            return Ok(CSV_TOKEN_DELIMITER);
        }
        if let Some(escaped) = self.try_read_escaped(b)? {
            return Ok(CSV_TOKEN_ESCAPED | CsvToken::from(escaped));
        }
        Ok(CsvToken::from(b))
    }

    /// Go `readUnquoteToken`.
    fn read_unquote_token(&mut self, b: u8) -> Result<CsvToken, CsvError> {
        if self.try_read_new_line(b) {
            return Ok(CSV_TOKEN_NEW_LINE);
        }
        if self.try_read_comma(b) {
            return Ok(CSV_TOKEN_COMMA);
        }
        if self.try_read_open_delimiter(b) {
            return Ok(CSV_TOKEN_DELIMITER);
        }
        if let Some(escaped) = self.try_read_escaped(b)? {
            return Ok(CSV_TOKEN_ESCAPED | CsvToken::from(escaped));
        }
        Ok(CsvToken::from(b))
    }

    fn append_csv_token_to_record_buffer(&mut self, token: CsvToken) {
        if token & CSV_TOKEN_ESCAPED != 0 {
            self.record_buffer.push(self.escaped_by[0]);
        }
        self.record_buffer.push(token as u8);
    }

    /// Go `readUntil`: the bytes before the first byte of `chars`, and that
    /// byte (left unconsumed). At the end of the source the error is EOF and
    /// the bytes are everything read.
    fn read_until(&mut self, chars: ByteSet) -> (Vec<u8>, u8, Option<CsvError>) {
        if let Some(index) = chars.index_any(self.rest()) {
            let found = self.rest()[..index].to_vec();
            self.start += index;
            self.pos += index as i64;
            return (found, self.rest()[0], None);
        }
        let mut found = Vec::new();
        loop {
            found.extend_from_slice(self.rest());
            if self.check_row_len
                && self.pos - self.row_start_pos + found.len() as i64 > LARGEST_ENTRY_LIMIT
            {
                return (found, 0, Some(CsvError::RowTooLarge));
            }
            self.set_buf(Vec::new());
            self.read_block();
            if self.rest().is_empty() {
                self.pos += found.len() as i64;
                return (found, 0, Some(CsvError::Eof));
            }
            if let Some(index) = chars.index_any(self.rest()) {
                found.extend_from_slice(&self.rest()[..index]);
                self.start += index;
                self.pos += found.len() as i64;
                return (found, self.rest()[0], None);
            }
        }
    }

    /// Go `readRecord`.
    fn read_record(&mut self) -> Result<Vec<Field>, CsvError> {
        self.record_buffer.clear();
        self.field_indexes.clear();
        self.field_is_quoted.clear();

        let mut is_empty_line = true;
        let mut whitespace_line = true;
        let mut found_starting_by_this_line = false;
        let mut prev_token = CSV_TOKEN_NEW_LINE;
        let mut field_is_quoted = false;

        loop {
            // Drop a whole line without STARTING BY, and anything before it.
            if !self.starting_by.is_empty() && !found_starting_by_this_line {
                let old_pos = self.pos;
                let (content, error) = self.read_until_terminator_inner();
                if let Some(error) = error {
                    if error != CsvError::Eof || content.is_empty() {
                        return Err(error);
                    }
                }
                let Some(index) = find_subslice(&content, &self.starting_by) else {
                    continue;
                };
                found_starting_by_this_line = true;
                let skip = index + self.starting_by.len();
                let mut next = content[skip..].to_vec();
                next.extend_from_slice(self.rest());
                self.set_buf(next);
                self.pos = old_pos + skip as i64;
            }

            let (content, first_byte, error) = self.read_until(self.unquote_byte_set);
            if !content.is_empty() {
                is_empty_line = false;
                if prev_token == CSV_TOKEN_DELIMITER {
                    return Err(CsvError::UnexpectedQuoteField);
                }
                self.record_buffer.extend_from_slice(&content);
                prev_token = CSV_TOKEN_ANY_UNQUOTED;
            }

            let at_eof = error.is_some();
            let first_token = match error {
                Some(error) => {
                    if is_empty_line || error != CsvError::Eof {
                        return Err(error);
                    }
                    // EOF ends the record like a trailing newline.
                    CSV_TOKEN_NEW_LINE
                }
                None => {
                    self.skip_bytes(1);
                    self.read_unquote_token(first_byte)?
                }
            };

            match first_token {
                CSV_TOKEN_COMMA => {
                    whitespace_line = false;
                    self.field_indexes.push(self.record_buffer.len());
                    self.field_is_quoted.push(field_is_quoted);
                    field_is_quoted = false;
                }
                CSV_TOKEN_DELIMITER => {
                    if prev_token != CSV_TOKEN_COMMA && prev_token != CSV_TOKEN_NEW_LINE {
                        if self.unescaped_quote {
                            whitespace_line = false;
                            let quote = self.quote.clone();
                            self.record_buffer.extend_from_slice(&quote);
                            continue;
                        }
                        return Err(CsvError::UnexpectedQuoteField);
                    }
                    self.read_quoted_field()?;
                    field_is_quoted = true;
                    whitespace_line = false;
                }
                CSV_TOKEN_NEW_LINE => {
                    found_starting_by_this_line = false;
                    prev_token = first_token;
                    if !self.allow_empty_line {
                        if is_empty_line {
                            continue;
                        }
                        if !at_eof && whitespace_line && self.record_buffer.trim_ascii().is_empty()
                        {
                            self.record_buffer.clear();
                            continue;
                        }
                    }
                    self.field_indexes.push(self.record_buffer.len());
                    self.field_is_quoted.push(field_is_quoted);
                    break;
                }
                _ => {
                    if prev_token == CSV_TOKEN_DELIMITER {
                        return Err(CsvError::UnexpectedQuoteField);
                    }
                    self.append_csv_token_to_record_buffer(first_token);
                }
            }
            prev_token = first_token;
            is_empty_line = false;
        }
        let mut fields = Vec::with_capacity(self.field_indexes.len());
        let mut previous = 0;
        for (index, &end) in self.field_indexes.iter().enumerate() {
            fields.push(Field {
                content: self.record_buffer[previous..end].to_vec(),
                quoted: self.field_is_quoted[index],
            });
            previous = end;
        }
        Ok(fields)
    }

    /// Go `readQuotedField`.
    fn read_quoted_field(&mut self) -> Result<(), CsvError> {
        loop {
            let (content, terminator, error) = self.read_until(self.quote_byte_set);
            if let Some(error) = error {
                return Err(if error == CsvError::Eof {
                    CsvError::UnterminatedQuotedField
                } else {
                    error
                });
            }
            self.record_buffer.extend_from_slice(&content);
            self.skip_bytes(1);
            let token = self.read_quoted_token(terminator)?;
            if token == CSV_TOKEN_DELIMITER {
                // `""` inside a quoted field is one quote.
                let quote = self.quote.clone();
                if self.try_read_exact(&quote) {
                    self.record_buffer.extend_from_slice(&quote);
                } else if self.unescaped_quote {
                    // An unescaped quote ends the field only before a
                    // separator or a line end.
                    let comma = self.comma.clone();
                    let (is_comma, _) = self.try_peek_exact(&comma);
                    if is_comma {
                        return Ok(());
                    }
                    let new_line = self.new_line.clone();
                    let (is_new_line, eof) = self.try_peek_exact(&new_line);
                    if eof || is_new_line {
                        return Ok(());
                    }
                    self.record_buffer.extend_from_slice(&quote);
                } else {
                    return Ok(());
                }
            } else {
                self.append_csv_token_to_record_buffer(token);
            }
        }
    }

    /// Go `unescapeString`: the field's value, or `None` for NULL.
    fn unescape_string(&self, input: &Field) -> Option<Vec<u8>> {
        let mut unescaped = match &self.charset_convertor {
            Some(convertor) => convertor.decode(&input.content),
            None => input.content.clone(),
        };
        let mut null_marker = self.escaped_by.clone();
        null_marker.push(b'N');
        if self.esc_flavor == EscapeFlavor::MySqlWithNull && unescaped == null_marker {
            return None;
        }
        if !self.escaped_by.is_empty() {
            unescaped = unescape(&unescaped, self.esc_flavor, self.escaped_by[0]);
        }
        let mut is_null = false;
        if !(!self.quote.is_empty() && self.quoted_null_is_text && input.quoted) {
            is_null = !self.not_null && self.field_null_defined_by.contains(&unescaped);
            // `\\N` must not become NULL.
            if self.esc_flavor == EscapeFlavor::MySqlWithNull && unescaped == null_marker {
                is_null = false;
            }
        }
        (!is_null).then_some(unescaped)
    }

    /// Go `ReadRow`: the next record's fields as string datums (NULL where
    /// the field is a NULL marker). `Err(CsvError::Eof)` ends the source.
    pub fn read_row(&mut self) -> Result<Vec<Datum>, CsvError> {
        self.begin_row_len_check();
        let fields = self.read_record();
        self.end_row_len_check();
        let mut fields = fields?;
        if self.trim_last_empty_field && fields.last().is_some_and(|f| f.content.is_empty()) {
            fields.pop();
        }
        Ok(fields
            .iter()
            .map(|field| match self.unescape_string(field) {
                Some(value) => Datum::Bytes(value),
                None => Datum::Null,
            })
            .collect())
    }

    /// Go `ReadUntilTerminator`: skips past the next line terminator.
    pub fn read_until_terminator(&mut self) -> Result<(), CsvError> {
        self.begin_row_len_check();
        let (_, error) = self.read_until_terminator_inner();
        self.end_row_len_check();
        error.map_or(Ok(()), Err)
    }

    fn read_until_terminator_inner(&mut self) -> (Vec<u8>, Option<CsvError>) {
        let mut found = Vec::new();
        loop {
            let (content, first_byte, error) = self.read_until(self.new_line_byte_set);
            found.extend_from_slice(&content);
            if error.is_some() {
                return (found, error);
            }
            self.skip_bytes(1);
            found.push(first_byte);
            if self.try_read_new_line(first_byte) {
                if !self.new_line.is_empty() {
                    found.extend_from_slice(&self.new_line[1..]);
                }
                return (found, None);
            }
        }
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    find_subslice(haystack, needle).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_data_config(terminated: &str, enclosed: &str) -> CsvConfig {
        CsvConfig {
            fields_terminated_by: terminated.as_bytes().to_vec(),
            fields_enclosed_by: enclosed.as_bytes().to_vec(),
            lines_terminated_by: b"\n".to_vec(),
            field_null_defined_by: if enclosed.is_empty() {
                vec![b"\\N".to_vec()]
            } else {
                vec![b"NULL".to_vec(), b"\\N".to_vec()]
            },
            fields_escaped_by: b"\\".to_vec(),
            allow_empty_line: true,
            quoted_null_is_text: true,
            unescaped_quote: true,
            ..CsvConfig::default()
        }
    }

    fn rows(config: &CsvConfig, data: &[u8]) -> Vec<Vec<Option<String>>> {
        let mut parser = CsvParser::new(config, data.to_vec(), None).unwrap();
        let mut out = Vec::new();
        loop {
            match parser.read_row() {
                Ok(row) => out.push(
                    row.into_iter()
                        .map(|datum| match datum {
                            Datum::Bytes(bytes) => Some(String::from_utf8(bytes).unwrap()),
                            _ => None,
                        })
                        .collect(),
                ),
                Err(CsvError::Eof) => return out,
                Err(error) => panic!("{error}"),
            }
        }
    }

    fn s(value: &str) -> Option<String> {
        Some(value.to_owned())
    }

    /// Rows captured from Go on this branch (`LOAD DATA LOCAL ... FIELDS
    /// TERMINATED BY ',' ENCLOSED BY '"'`): enclosed separators, doubled and
    /// unescaped quotes, and the three NULL spellings.
    #[test]
    fn enclosed_fields_parse_like_go() {
        let data = b"1,\"a,b\"\n2,\"q\"\"r\"\n3,\"NULL\"\n4,NULL\n5,\\N\n6,\"x\"y\"\n7,\"\\N\"\n";
        assert_eq!(
            rows(&load_data_config(",", "\""), data),
            vec![
                vec![s("1"), s("a,b")],
                vec![s("2"), s("q\"r")],
                vec![s("3"), s("NULL")],
                vec![s("4"), None],
                vec![s("5"), None],
                vec![s("6"), s("x\"y")],
                vec![s("7"), None],
            ]
        );
    }

    /// Go's tab-separated default: an empty line is a one-field record, a
    /// CR stays in the field, escapes are decoded, and a leading BOM goes.
    #[test]
    fn default_format_parses_like_go() {
        let config = load_data_config("\t", "");
        assert_eq!(
            rows(&config, b"1\ta\n\n2\tb\r\n3\tc\\td\\ne\\0f\\Zg\n"),
            vec![
                vec![s("1"), s("a")],
                vec![s("")],
                vec![s("2"), s("b\r")],
                vec![s("3"), s("c\td\ne\0f\x1ag")],
            ]
        );
        assert_eq!(
            rows(&config, b"\xef\xbb\xbf1\ta\n"),
            vec![vec![s("1"), s("a")]]
        );
        assert_eq!(rows(&config, b"3\tz"), vec![vec![s("3"), s("z")]]);
    }

    #[test]
    fn starting_by_drops_lines_like_go() {
        let mut config = load_data_config(",", "");
        config.lines_terminated_by = b"|".to_vec();
        config.lines_starting_by = b"xx".to_vec();
        assert_eq!(
            rows(&config, b"zzxx1,a|nothing|xx2,b|yyxx3,c"),
            vec![
                vec![s("1"), s("a")],
                vec![s("2"), s("b")],
                vec![s("3"), s("c")],
            ]
        );
        config.lines_starting_by = b"a|".to_vec();
        assert_eq!(
            CsvParser::new(&config, Vec::new(), None)
                .err()
                .unwrap()
                .to_string(),
            "STARTING BY 'a|' cannot contain LINES TERMINATED BY '|'"
        );
    }

    #[test]
    fn an_unterminated_quote_fails_like_go() {
        let config = load_data_config(",", "\"");
        let mut parser = CsvParser::new(&config, b"1,\"abc\n".to_vec(), None).unwrap();
        assert_eq!(
            parser.read_row().unwrap_err().to_string(),
            "syntax error: unterminated quoted field"
        );
    }

    #[test]
    fn decodes_gbk_and_latin1_like_go() {
        let gbk = CharsetConvertor::new("gbk", "\u{FFFD}").unwrap();
        assert_eq!(gbk.decode(b"\xc4\xe3"), "你".as_bytes());
        let latin1 = CharsetConvertor::new("latin1", "\u{FFFD}").unwrap();
        assert_eq!(latin1.decode(b"\xe9\x80"), "é€".as_bytes());
    }
}
