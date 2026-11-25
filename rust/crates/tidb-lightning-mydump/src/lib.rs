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

//! Go `pkg/lightning/mydump`: the CSV parser (`csv_parser.go`), the SQL
//! dump parser and the `blockParser` they embed (`parser.go`), `byteSet`
//! (`bytes.go`), `CharsetConvertor` (`charset_convertor.go`), source
//! routing (`router.go`), file metadata and sizes (`loader.go`), readers
//! (`reader.go`) and table regions (`region.go`).
//!
//! Parsers read their file in Go's `ReadBlockSize * BufferSizeScale`
//! blocks; the block boundary is observable: the BOM is stripped from the
//! block read at position 0, and the txn-entry-size limit is checked only
//! when a token spans blocks.

mod parser;
mod router;
mod loader;
pub use router::{Compression, SourceType, parse_compression_on_file_extension};
pub use loader::{
    estimate_real_size_for_file, parallel_process, sample_file_compress_ratio, ExtendColumnData, FileInfo,
    FilterTable, MDTableMeta, RawFile, SourceFileMeta,
};
mod reader;
pub use reader::{new_reader_opener, open_reader, ParquetParser, ParserReader};
pub mod region;
pub use region::{make_table_regions, DataDivideConfig, TableRegion};
#[cfg(test)]
mod region_tests;

/// Go `parquetfile.FileMeta`: what a Parquet chunk's reader is reopened with.
/// Its one field, `Loc *time.Location`, has only unexported fields, so Go
/// marshals it as `{"Loc":{}}` and a reader reopened from JSON gets the zero
/// `Location` (UTC).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParquetFileMeta {}

impl serde::Serialize for ParquetFileMeta {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("Loc", &serde_json::Map::new())?;
        map.end()
    }
}

impl<'de> serde::Deserialize<'de> for ParquetFileMeta {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        serde::de::IgnoredAny::deserialize(deserializer)?;
        Ok(Self {})
    }
}

/// Go `parquetfile.ReadRowCount`.
pub fn parquet_row_count(
    ctx: &tidb_gocontext::Context,
    store: &std::sync::Arc<dyn tidb_objstore::storeapi::Storage>,
    path: &str,
) -> Result<i64, String> {
    tidb_dumpformat_parquetfile::read_row_count(ctx, store.clone(), path)
}

/// Go `ToStorageCompressType`.
pub fn to_storage_compress_type(compression: Compression) -> Result<tidb_objstore::compressedio::CompressType, String> {
    use tidb_objstore::compressedio::CompressType;
    match compression {
        Compression::GZ => Ok(CompressType::Gzip),
        Compression::Snappy => Ok(CompressType::Snappy),
        Compression::ZStd => Ok(CompressType::Zstd),
        Compression::None => Ok(CompressType::NoCompression),
        other => Err(format!("compression {} doesn't have related storage compressType", other as i32)),
    }
}
pub use parser::{read_chunks, read_until, Chunk, ChunkParser, Parser, Row};

use std::borrow::Cow;
use std::ops::Range;

use tidb_charset::Collation;
use tidb_datatype::{Datum, StringDatum};

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
    /// `Header`.
    pub header: bool,
    /// `HeaderSchemaMatch`.
    pub header_schema_match: bool,
}

/// Go `config.ReadBlockSize` (64 KiB) times `config.BufferSizeScale` (5).
/// Go `config.ReadBlockSize`.
pub const READ_BLOCK_SIZE: usize = 64 * 1024;

/// Go `config.BufferSizeScale`.
pub const BUFFER_SIZE_SCALE: usize = 5;

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
                    tidb_charset::Encoding::Gbk
                } else {
                    tidb_charset::Encoding::Gb18030
                };
                let mut out = Vec::with_capacity(src.len() * 3 / 2);
                encoding.foreach(src, tidb_charset::TransformOp::default(), |_, to, _| {
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
                    tidb_charset::Encoding::Gbk
                } else {
                    tidb_charset::Encoding::Gb18030
                };
                encoding
                    .transform(src, tidb_charset::TransformOp::ENCODE_REPLACE)
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
    let Some(first) = memchr::memchr(esc_char, input) else {
        return input.to_vec();
    };
    if first + 1 == input.len() {
        return input.to_vec();
    }
    unescape_from(input, esc_char, first)
}

fn unescape_from(input: &[u8], esc_char: u8, first: usize) -> Vec<u8> {
    let mut result = Vec::with_capacity(input.len() - 1);
    result.extend_from_slice(&input[..first]);
    let mut remaining = &input[first..];
    while remaining.len() > 1 {
        result.push(escaped_char_to_byte(remaining[1]));
        remaining = &remaining[2..];
        match memchr::memchr(esc_char, remaining) {
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

/// Go `io.ReadSeekCloser`: what a parser reads its file from.
pub trait ReadSeek: std::io::Read + std::io::Seek + Send {}

impl<T: std::io::Read + std::io::Seek + Send> ReadSeek for T {}

/// Go `blockParser`'s reader and `blockBuf`: reads the file a block at a
/// time (`io.ReadFull`).
pub(crate) struct BlockReader {
    reader: Box<dyn ReadSeek>,
    block_buf_size: usize,
}

impl BlockReader {
    pub(crate) fn new(reader: Box<dyn ReadSeek>, block_buf_size: usize) -> Self {
        Self { reader, block_buf_size }
    }

    /// Go `PooledReader.ReadFull(blockBuf)`: the block, and whether it was
    /// the last (a short read).
    pub(crate) fn read_block(&mut self) -> Result<(Vec<u8>, bool), CsvError> {
        let mut block = vec![0; self.block_buf_size];
        let mut n = 0;
        while n < block.len() {
            match self.reader.read(&mut block[n..]) {
                Ok(0) => break,
                Ok(m) => n += m,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(CsvError::Other(e.to_string())),
            }
        }
        block.truncate(n);
        Ok((block, n < self.block_buf_size))
    }

    /// Go `blockParser.SetPos`'s seek.
    pub(crate) fn seek(&mut self, pos: i64) -> Result<(), CsvError> {
        let p = self
            .reader
            .seek(std::io::SeekFrom::Start(u64::try_from(pos).map_err(|_| CsvError::Other("negative seek position".to_owned()))?))
            .map_err(|e| CsvError::Other(e.to_string()))?;
        if p != pos as u64 {
            return Err(CsvError::Other(format!("set pos failed, required position: {pos}, got: {p}")));
        }
        Ok(())
    }

    /// Go `blockParser.ScannedPos`.
    pub(crate) fn scanned_pos(&mut self) -> Result<i64, CsvError> {
        self.reader
            .seek(std::io::SeekFrom::Current(0))
            .map(|p| p as i64)
            .map_err(|e| CsvError::Other(e.to_string()))
    }
}

/// Go `CSVParser`.
pub struct CsvParser {
    last_row: Row,
    columns: Vec<String>,
    // blockParser
    reader: BlockReader,
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
    should_parse_header: bool,
    header_schema_match: bool,
}

impl CsvParser {
    /// Go `NewCSVParser`: reads `reader` in blocks of `block_buf_size *
    /// config.BufferSizeScale`; with `should_parse_header` and
    /// `config.header`, the first `ReadRow` reads the header first.
    pub fn new(
        config: &CsvConfig,
        reader: Box<dyn ReadSeek>,
        block_buf_size: usize,
        should_parse_header: bool,
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
            reader: BlockReader::new(reader, block_buf_size * BUFFER_SIZE_SCALE),
            last_row: Row::default(),
            columns: Vec::new(),
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
            should_parse_header: should_parse_header && config.header,
            header_schema_match: config.header_schema_match,
        })
    }

    /// Go `NewCSVParser` over a whole in-memory file, as LOAD DATA reads
    /// its client file, with Go's default read block size and no header.
    pub fn from_bytes(
        config: &CsvConfig,
        source: Vec<u8>,
        charset_convertor: Option<CharsetConvertor>,
    ) -> Result<Self, CsvError> {
        Self::new(config, Box::new(std::io::Cursor::new(source)), READ_BLOCK_SIZE, false, charset_convertor)
    }

    /// Go `blockParser.SetPos`.
    pub fn set_pos(&mut self, pos: i64, row_id: i64) -> Result<(), CsvError> {
        self.reader.seek(pos)?;
        self.set_buf(Vec::new());
        self.pos = pos;
        self.last_row.row_id = row_id;
        Ok(())
    }

    /// Go `blockParser.ScannedPos`.
    pub fn scanned_pos(&mut self) -> Result<i64, CsvError> {
        self.reader.scanned_pos()
    }

    /// Go `blockParser.Pos`.
    #[must_use]
    pub fn pos(&self) -> (i64, i64) {
        (self.pos, self.last_row.row_id)
    }

    /// Go `blockParser.Columns`.
    #[must_use]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Go `CSVParser.ReadColumns`: the header record; its lowercased names
    /// become the columns when the header must match the schema.
    pub fn read_columns(&mut self) -> Result<(), CsvError> {
        self.begin_row_len_check();
        let result = self.read_record();
        self.end_row_len_check();
        result?;
        if !self.header_schema_match {
            return Ok(());
        }
        self.columns = self
            .record_fields(self.field_indexes.len())
            .map(|(range, quoted)| {
                let name = self.decode_field(&self.record_buffer[range], quoted).unwrap_or_default();
                String::from_utf8_lossy(&name).to_lowercase()
            })
            .collect();
        Ok(())
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
        // A read error ends the input here; Go returns it from `readBlock`.
        let (block, _) = self.reader.read_block().unwrap_or_default();
        // Go `bom.Clean`.
        let prefix = if self.pos == 0 && block.starts_with(&[0xEF, 0xBB, 0xBF]) {
            self.pos += 3;
            3
        } else {
            0
        };
        if self.rest().is_empty() {
            self.set_buf(block);
            self.start = prefix;
            return;
        }
        let block = &block[prefix..];
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

    fn skip_bytes(&mut self, n: usize) {
        self.start += n;
        self.pos += n as i64;
    }

    /// Go `tryPeekExact`: (matched, eof).
    fn try_peek_exact(&mut self, content: impl Fn(&Self) -> &[u8]) -> (bool, bool) {
        let count = content(self).len();
        if count == 0 {
            return (true, false);
        }
        if self.rest().len() < count {
            self.read_block();
        }
        let bytes = self.rest();
        (bytes.starts_with(content(self)), bytes.is_empty())
    }

    /// Go `tryReadExact`.
    fn try_read_exact(&mut self, content: impl Fn(&Self) -> &[u8]) -> bool {
        let (matched, _) = self.try_peek_exact(&content);
        if matched {
            self.skip_bytes(content(self).len());
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
        self.try_read_exact(|p| &p.new_line[1..])
    }

    fn try_read_open_delimiter(&mut self, b: u8) -> bool {
        if self.quote.is_empty() || self.quote[0] != b {
            return false;
        }
        self.try_read_exact(|p| &p.quote[1..])
    }

    fn try_read_close_delimiter(&mut self, b: u8) -> bool {
        if self.quote[0] != b {
            return false;
        }
        self.try_read_exact(|p| &p.quote[1..])
    }

    fn try_read_comma(&mut self, b: u8) -> bool {
        if self.comma[0] != b {
            return false;
        }
        self.try_read_exact(|p| &p.comma[1..])
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

    fn append_csv_token(&self, token: CsvToken, output: &mut Vec<u8>) {
        if token & CSV_TOKEN_ESCAPED != 0 {
            output.push(self.escaped_by[0]);
        }
        output.push(token as u8);
    }

    /// Appends the bytes before `chars` and leaves the stop byte unconsumed.
    /// Like Go `readUntil`, append the remaining bytes when reporting EOF.
    fn read_until(&mut self, chars: ByteSet, output: &mut Vec<u8>) -> (u8, Option<CsvError>) {
        let begin = output.len();
        loop {
            if let Some(index) = chars.index_any(self.rest()) {
                output.extend_from_slice(&self.rest()[..index]);
                self.start += index;
                self.pos += (output.len() - begin) as i64;
                return (self.rest()[0], None);
            }
            output.extend_from_slice(self.rest());
            if self.check_row_len
                && self.pos - self.row_start_pos + (output.len() - begin) as i64 > LARGEST_ENTRY_LIMIT
            {
                return (0, Some(CsvError::RowTooLarge));
            }
            self.set_buf(Vec::new());
            self.read_block();
            if self.rest().is_empty() {
                self.pos += (output.len() - begin) as i64;
                return (0, Some(CsvError::Eof));
            }
        }
    }

    /// Go `readRecord`.
    fn read_record(&mut self) -> Result<(), CsvError> {
        let mut record = std::mem::take(&mut self.record_buffer);
        record.clear();
        self.field_indexes.clear();
        self.field_is_quoted.clear();
        let result = self.read_record_into(&mut record);
        self.record_buffer = record;
        result
    }

    fn read_record_into(&mut self, record: &mut Vec<u8>) -> Result<(), CsvError> {
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

            let previous_len = record.len();
            let (first_byte, error) = self.read_until(self.unquote_byte_set, record);
            if record.len() != previous_len {
                is_empty_line = false;
                if prev_token == CSV_TOKEN_DELIMITER {
                    return Err(CsvError::UnexpectedQuoteField);
                }
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
                    self.field_indexes.push(record.len());
                    self.field_is_quoted.push(field_is_quoted);
                    field_is_quoted = false;
                }
                CSV_TOKEN_DELIMITER => {
                    if prev_token != CSV_TOKEN_COMMA && prev_token != CSV_TOKEN_NEW_LINE {
                        if self.unescaped_quote {
                            whitespace_line = false;
                            record.extend_from_slice(&self.quote);
                            continue;
                        }
                        return Err(CsvError::UnexpectedQuoteField);
                    }
                    self.read_quoted_field(record)?;
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
                        if !at_eof && whitespace_line && record.trim_ascii().is_empty()
                        {
                            record.clear();
                            continue;
                        }
                    }
                    self.field_indexes.push(record.len());
                    self.field_is_quoted.push(field_is_quoted);
                    break;
                }
                _ => {
                    if prev_token == CSV_TOKEN_DELIMITER {
                        return Err(CsvError::UnexpectedQuoteField);
                    }
                    self.append_csv_token(first_token, record);
                }
            }
            prev_token = first_token;
            is_empty_line = false;
        }
        Ok(())
    }

    /// Go `readQuotedField`.
    fn read_quoted_field(&mut self, record: &mut Vec<u8>) -> Result<(), CsvError> {
        loop {
            let (terminator, error) = self.read_until(self.quote_byte_set, record);
            if let Some(error) = error {
                return Err(if error == CsvError::Eof {
                    CsvError::UnterminatedQuotedField
                } else {
                    error
                });
            }
            self.skip_bytes(1);
            let token = self.read_quoted_token(terminator)?;
            if token == CSV_TOKEN_DELIMITER {
                // `""` inside a quoted field is one quote.
                if self.try_read_exact(|p| &p.quote) {
                    record.extend_from_slice(&self.quote);
                } else if self.unescaped_quote {
                    // An unescaped quote ends the field only before a
                    // separator or a line end.
                    let (is_comma, _) = self.try_peek_exact(|p| &p.comma);
                    if is_comma {
                        return Ok(());
                    }
                    let (is_new_line, eof) = self.try_peek_exact(|p| &p.new_line);
                    if eof || is_new_line {
                        return Ok(());
                    }
                    record.extend_from_slice(&self.quote);
                } else {
                    return Ok(());
                }
            } else {
                self.append_csv_token(token, record);
            }
        }
    }

    fn record_fields(&self, count: usize) -> impl Iterator<Item = (Range<usize>, bool)> + '_ {
        self.field_indexes[..count]
            .iter()
            .copied()
            .scan(0, |start, end| Some(std::mem::replace(start, end)..end))
            .zip(self.field_is_quoted.iter().copied())
    }

    /// Go `unescapeString`: an unchanged field borrows the record buffer.
    fn decode_field<'a>(&self, input: &'a [u8], quoted: bool) -> Option<Cow<'a, [u8]>> {
        let mut decoded = match &self.charset_convertor {
            Some(convertor) if convertor.converts(input) => Cow::Owned(convertor.decode(input)),
            _ => Cow::Borrowed(input),
        };
        let escape_null = self.esc_flavor == EscapeFlavor::MySqlWithNull;
        let is_escape_null = |value: &[u8]| value.strip_prefix(self.escaped_by.as_slice()) == Some(b"N");
        if escape_null && is_escape_null(&decoded) {
            return None;
        }
        if let Some(&escape) = self.escaped_by.first() {
            if let Some(pos) = memchr::memchr(escape, &decoded).filter(|&pos| pos + 1 < decoded.len()) {
                decoded = Cow::Owned(unescape_from(&decoded, escape, pos));
            }
        }
        if !self.not_null
            && (self.quote.is_empty() || !self.quoted_null_is_text || !quoted)
            && self.field_null_defined_by.iter().any(|marker| marker.as_slice() == decoded.as_ref())
            && !(escape_null && is_escape_null(&decoded))
        {
            return None;
        }
        Some(decoded)
    }

    /// Reads the next record as byte datums for LOAD DATA, while filling the
    /// typed last row used by the parser trait. A NULL marker yields
    /// `Datum::Null`. `Err(CsvError::Eof)` ends the source.
    pub fn read_row(&mut self) -> Result<Vec<Datum>, CsvError> {
        let mut raw = Vec::new();
        self.read_next_row(Some(&mut raw))?;
        Ok(raw)
    }

    fn read_next_row(&mut self, mut raw: Option<&mut Vec<Datum>>) -> Result<(), CsvError> {
        self.last_row.length = 0;
        self.last_row.row_id += 1;
        if self.should_parse_header {
            self.read_columns()?;
            self.should_parse_header = false;
        }
        self.begin_row_len_check();
        let result = self.read_record();
        self.end_row_len_check();
        result?;

        let mut count = self.field_indexes.len();
        if self.trim_last_empty_field {
            let previous = self.field_indexes.iter().rev().nth(1).copied().unwrap_or(0);
            if self.field_indexes.last() == Some(&previous) {
                count -= 1;
            }
        }
        let len = self.field_indexes[..count].last().copied().unwrap_or(0);
        self.last_row.length = len;

        let mut row = std::mem::take(&mut self.last_row.row);
        row.clear();
        row.reserve(count);
        if let Some(raw) = raw.as_deref_mut() {
            raw.reserve(count);
        }
        // Borrowed fields share one owned copy of the completed record.
        let mut record = None;
        for (range, quoted) in self.record_fields(count) {
            let value = self.decode_field(&self.record_buffer[range.clone()], quoted);
            if let Some(raw) = raw.as_deref_mut() {
                raw.push(value.as_ref().map_or(Datum::Null, |v| Datum::Bytes(v.to_vec())));
            }
            row.push(match value {
                Some(Cow::Borrowed(_)) => {
                    let backing = record.get_or_insert_with(|| {
                        bytes::Bytes::copy_from_slice(&self.record_buffer[..len])
                    });
                    Datum::String(StringDatum::from_shared_bytes(
                        backing.slice(range),
                        Collation::DEFAULT,
                    ))
                }
                Some(Cow::Owned(bytes)) => Datum::new_string_datum(bytes),
                None => Datum::Null,
            });
        }
        self.last_row.row = row;
        Ok(())
    }

    /// Go `ReadUntilTerminator`: skips past the next line terminator,
    /// returning the position after it.
    pub fn read_until_terminator(&mut self) -> Result<i64, CsvError> {
        self.begin_row_len_check();
        let (_, error) = self.read_until_terminator_inner();
        self.end_row_len_check();
        error.map_or(Ok(self.pos), Err)
    }

    fn read_until_terminator_inner(&mut self) -> (Vec<u8>, Option<CsvError>) {
        let mut found = Vec::new();
        loop {
            let (first_byte, error) = self.read_until(self.new_line_byte_set, &mut found);
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

    #[test]
    fn csv_fields_survive_record_reuse_and_raw_rows_keep_their_own_bytes() {
        let mut config = load_data_config(",", "\"");
        config.lines_terminated_by = b"\r\n".to_vec();
        let source = std::io::Cursor::new(b"alpha,beta\r\ngamma,delta\r\n".to_vec());
        let mut parser = CsvParser::new(&config, Box::new(source), 2, false, None).unwrap();

        Parser::read_row(&mut parser).unwrap();
        let first = Parser::last_row(&parser).clone();
        let [Datum::String(left), Datum::String(right)] = first.row.as_slice() else {
            panic!("import should return string datums");
        };
        assert_eq!(left.bytes().as_ptr_range().end, right.bytes().as_ptr());

        let mut raw = parser.read_row().unwrap();
        let [Datum::Bytes(raw_left), Datum::Bytes(raw_right)] = raw.as_mut_slice() else {
            panic!("LOAD DATA should return byte datums");
        };
        assert_eq!(raw_left, b"gamma");
        assert_eq!(raw_right, b"delta");
        raw_left[0] = b'x';
        assert_eq!(
            Parser::last_row(&parser).row[0],
            Datum::new_string_datum(b"gamma".to_vec())
        );
        assert_eq!(left.bytes(), b"alpha");
        assert_eq!(right.bytes(), b"beta");
    }

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
        let mut parser = CsvParser::from_bytes(config, data.to_vec(), None).unwrap();
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
            CsvParser::from_bytes(&config, Vec::new(), None)
                .err()
                .unwrap()
                .to_string(),
            "STARTING BY 'a|' cannot contain LINES TERMINATED BY '|'"
        );
    }

    #[test]
    fn an_unterminated_quote_fails_like_go() {
        let config = load_data_config(",", "\"");
        let mut parser = CsvParser::from_bytes(&config, b"1,\"abc\n".to_vec(), None).unwrap();
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
