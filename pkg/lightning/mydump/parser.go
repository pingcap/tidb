// Copyright 2019 PingCAP, Inc.
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

package mydump

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/worker"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/zeropool"
	"github.com/spkg/bom"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type blockParser struct {
	// states for the lexer
	reader PooledReader
	// stores data that has NOT been parsed yet, it shares same memory as appendBuf.
	buf []byte
	// used to read data from the reader, the data will be moved to other buffers.
	blockBuf    []byte
	isLastChunk bool

	// The list of column names of the last INSERT statement.
	columns []string

	rowPool *zeropool.Pool[[]types.Datum]
	lastRow Row
	// the reader position we have parsed, if the underlying reader is not
	// a compressed file, it's the file position we have parsed too.
	// this value may go backward when failed to read quoted field, but it's
	// for printing error message, and the parser should not be used later,
	// so it's ok, see readQuotedField.
	pos int64

	// cache
	remainBuf *bytes.Buffer
	appendBuf *bytes.Buffer

	// the Logger associated with this parser for reporting failure
	Logger  log.Logger
	metrics *metric.Metrics
}

func makeBlockParser(
	reader ReadSeekCloser,
	blockBufSize int64,
	ioWorkers *worker.Pool,
	metrics *metric.Metrics,
	logger log.Logger,
) blockParser {
	pool := zeropool.New[[]types.Datum](func() []types.Datum {
		return make([]types.Datum, 0, 16)
	})
	return blockParser{
		reader:    MakePooledReader(reader, ioWorkers),
		blockBuf:  make([]byte, blockBufSize*config.BufferSizeScale),
		remainBuf: &bytes.Buffer{},
		appendBuf: &bytes.Buffer{},
		Logger:    logger,
		rowPool:   &pool,
		metrics:   metrics,
	}
}

// ChunkParser is a parser of the data files (the file containing only INSERT
// statements).
type ChunkParser struct {
	blockParser

	escFlavor escapeFlavor
}

// Chunk represents a portion of the data file.
type Chunk struct {
	Offset int64
	// for parquet file, it's the total row count
	// see makeParquetFileRegion
	EndOffset  int64
	RealOffset int64
	// we estimate row-id range of the chunk using file-size divided by some factor(depends on column count)
	// after estimation, we will rebase them for all chunks of this table in this instance,
	// then it's rebased again based on all instances of parallel import.
	// allocatable row-id is in range (PrevRowIDMax, RowIDMax].
	// PrevRowIDMax will be increased during local encoding
	PrevRowIDMax int64
	RowIDMax     int64
	// only assigned when using strict-mode for CSV files and the file contains header
	Columns []string
}

// Row is the content of a row.
type Row struct {
	// RowID is the row id of the row.
	// as objects of this struct is reused, this RowID is increased when reading
	// next row.
	RowID  int64
	Row    []types.Datum
	Length int
}

// MarshalLogArray implements the zapcore.ArrayMarshaler interface
func (row Row) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for _, r := range row.Row {
		encoder.AppendString(r.String())
	}
	return nil
}

type escapeFlavor uint8

const (
	escapeFlavorNone escapeFlavor = iota
	escapeFlavorMySQL
	escapeFlavorMySQLWithNull
)

// Parser provides some methods to parse a source data file.
type Parser interface {
	// Pos returns means the position that parser have already handled. It's mainly used for checkpoint.
	// For normal files it's the file offset we handled.
	// For parquet files it's the row count we handled.
	// For compressed files it's the uncompressed file offset we handled.
	// TODO: replace pos with a new structure to specify position offset and rows offset
	Pos() (pos int64, rowID int64)
	SetPos(pos int64, rowID int64) error
	// ScannedPos always returns the current file reader pointer's location
	ScannedPos() (int64, error)
	Close() error
	ReadRow() error
	LastRow() Row
	RecycleRow(row Row)

	// Columns returns the _lower-case_ column names corresponding to values in
	// the LastRow.
	Columns() []string
	// SetColumns set restored column names to parser
	SetColumns([]string)

	SetLogger(log.Logger)

	SetRowID(rowID int64)
}

// NewChunkParser creates a new parser which can read chunks out of a file.
func NewChunkParser(
	ctx context.Context,
	sqlMode mysql.SQLMode,
	reader ReadSeekCloser,
	blockBufSize int64,
	ioWorkers *worker.Pool,
) *ChunkParser {
	escFlavor := escapeFlavorMySQL
	if sqlMode.HasNoBackslashEscapesMode() {
		escFlavor = escapeFlavorNone
	}
	metrics, _ := metric.FromContext(ctx)
	return &ChunkParser{
		blockParser: makeBlockParser(reader, blockBufSize, ioWorkers, metrics, log.FromContext(ctx)),
		escFlavor:   escFlavor,
	}
}

// SetPos changes the reported position and row ID.
func (parser *blockParser) SetPos(pos int64, rowID int64) error {
	p, err := parser.reader.Seek(pos, io.SeekStart)
	if err != nil {
		return errors.Trace(err)
	}
	if p != pos {
		return errors.Errorf("set pos failed, required position: %d, got: %d", pos, p)
	}
	parser.pos = pos
	parser.lastRow.RowID = rowID
	return nil
}

// ScannedPos gets the read position of current reader.
// this always returns the position of the underlying file, either compressed or not.
func (parser *blockParser) ScannedPos() (int64, error) {
	return parser.reader.Seek(0, io.SeekCurrent)
}

// Pos returns the current file offset.
// Attention: for compressed sql/csv files, pos is the position in uncompressed files
func (parser *blockParser) Pos() (pos int64, lastRowID int64) {
	return parser.pos, parser.lastRow.RowID
}

func (parser *blockParser) Close() error {
	return parser.reader.Close()
}

func (parser *blockParser) Columns() []string {
	return parser.columns
}

func (parser *blockParser) SetColumns(columns []string) {
	parser.columns = columns
}

func (parser *blockParser) logSyntaxError() {
	content := parser.buf
	if len(content) > 256 {
		content = content[:256]
	}
	parser.Logger.Error("syntax error",
		zap.Int64("pos", parser.pos),
		zap.ByteString("content", content),
	)
}

func (parser *blockParser) SetLogger(logger log.Logger) {
	parser.Logger = logger
}

// SetRowID changes the reported row ID when we firstly read compressed files.
func (parser *blockParser) SetRowID(rowID int64) {
	parser.lastRow.RowID = rowID
}

type token byte

const (
	tokNil token = iota
	tokRowBegin
	tokRowEnd
	tokValues
	tokNull
	tokTrue
	tokFalse
	tokHexString
	tokBinString
	tokInteger
	tokSingleQuoted
	tokDoubleQuoted
	tokBackQuoted
	tokUnquoted
)

var tokenDescriptions = [...]string{
	tokNil:          "<Nil>",
	tokRowBegin:     "RowBegin",
	tokRowEnd:       "RowEnd",
	tokValues:       "Values",
	tokNull:         "Null",
	tokTrue:         "True",
	tokFalse:        "False",
	tokHexString:    "HexString",
	tokBinString:    "BinString",
	tokInteger:      "Integer",
	tokSingleQuoted: "SingleQuoted",
	tokDoubleQuoted: "DoubleQuoted",
	tokBackQuoted:   "BackQuoted",
	tokUnquoted:     "Unquoted",
}

// String implements the fmt.Stringer interface
//
// Mainly used for debugging a token.
func (tok token) String() string {
	t := int(tok)
	if t >= 0 && t < len(tokenDescriptions) {
		if description := tokenDescriptions[t]; description != "" {
			return description
		}
	}
	return fmt.Sprintf("<Unknown(%d)>", t)
}

func (parser *blockParser) readBlock() error {
	startTime := time.Now()

	n, err := parser.reader.ReadFull(parser.blockBuf)

	switch err {
	case io.ErrUnexpectedEOF, io.EOF:
		parser.isLastChunk = true
		fallthrough
	case nil:
		// `parser.buf` reference to `appendBuf.Bytes`, so should use remainBuf to
		// hold the `parser.buf` rest data to prevent slice overlap
		parser.remainBuf.Reset()
		parser.remainBuf.Write(parser.buf)
		parser.appendBuf.Reset()
		parser.appendBuf.Write(parser.remainBuf.Bytes())
		blockData := parser.blockBuf[:n]
		if parser.pos == 0 {
			bomCleanedData := bom.Clean(blockData)
			parser.pos += int64(n - len(bomCleanedData))
			blockData = bomCleanedData
		}
		parser.appendBuf.Write(blockData)
		parser.buf = parser.appendBuf.Bytes()
		if parser.metrics != nil {
			parser.metrics.ChunkParserReadBlockSecondsHistogram.Observe(time.Since(startTime).Seconds())
		}
		return nil
	default:
		return errors.Trace(err)
	}
}

var chunkParserUnescapeRegexp = regexp.MustCompile(`(?s)\\.`)

func unescape(
	input string,
	delim string,
	escFlavor escapeFlavor,
	escChar byte,
	unescapeRegexp *regexp.Regexp,
) string {
	if len(delim) > 0 {
		delim2 := delim + delim
		if strings.Contains(input, delim2) {
			input = strings.ReplaceAll(input, delim2, delim)
		}
	}
	if escFlavor != escapeFlavorNone && strings.IndexByte(input, escChar) != -1 {
		input = unescapeRegexp.ReplaceAllStringFunc(input, func(substr string) string {
			switch substr[1] {
			case '0':
				return "\x00"
			case 'b':
				return "\b"
			case 'n':
				return "\n"
			case 'r':
				return "\r"
			case 't':
				return "\t"
			case 'Z':
				return "\x1a"
			default:
				return substr[1:]
			}
		})
	}
	return input
}

func (parser *ChunkParser) unescapeString(input string) string {
	if len(input) >= 2 {
		switch input[0] {
		case '\'', '"':
			return unescape(input[1:len(input)-1], input[:1], parser.escFlavor, '\\', chunkParserUnescapeRegexp)
		case '`':
			return unescape(input[1:len(input)-1], "`", escapeFlavorNone, '\\', chunkParserUnescapeRegexp)
		}
	}
	return input
}

// ReadRow reads a row from the datafile.
func (parser *ChunkParser) ReadRow() error {
	// This parser will recognize contents like:
	//
	// 		`tableName` (...) VALUES (...) (...) (...)
	//
	// Keywords like INSERT, INTO and separators like ',' and ';' are treated
	// like comments and ignored. Therefore, this parser will accept some
	// nonsense input. The advantage is the parser becomes extremely simple,
	// suitable for us where we just want to quickly and accurately split the
	// file apart, not to validate the content.

	type state byte

	const (
		// the state after "INSERT INTO" before the column names or "VALUES"
		stateTableName state = iota

		// the state while reading the column names
		stateColumns

		// the state after reading "VALUES"
		stateValues

		// the state while reading row values
		stateRow
	)

	// Dry-run sample of the state machine, first row:
	//
	//              Input         Token             State
	//              ~~~~~         ~~~~~             ~~~~~
	//
	//                                              stateValues
	//              INSERT
	//              INTO
	//              `tableName`   tokBackQuoted
	//                                              stateTableName (reset columns)
	//              (             tokRowBegin
	//                                              stateColumns
	//              `a`           tokBackQuoted
	//                                              stateColumns (append column)
	//              ,
	//              `b`           tokBackQuoted
	//                                              stateColumns (append column)
	//              )             tokRowEnd
	//                                              stateValues
	//              VALUES
	//                                              stateValues (no-op)
	//              (             tokRowBegin
	//                                              stateRow (reset row)
	//              1             tokInteger
	//                                              stateRow (append value)
	//              ,
	//              2             tokInteger
	//                                              stateRow (append value)
	//              )             tokRowEnd
	//                                              return
	//
	//
	// Second row:
	//
	//              Input         Token             State
	//              ~~~~~         ~~~~~             ~~~~~
	//
	//                                              stateValues
	//              ,
	//              (             tokRowBegin
	//                                              stateRow (reset row)
	//              3             tokInteger
	//                                              stateRow (append value)
	//              )             tokRowEnd
	//                                              return
	//
	// Third row:
	//
	//              Input         Token             State
	//              ~~~~~         ~~~~~             ~~~~~
	//
	//              ;
	//              INSERT
	//              INTO
	//              `database`    tokBackQuoted
	//                                              stateTableName (reset columns)
	//              .
	//              `tableName`   tokBackQuoted
	//                                              stateTableName (no-op)
	//              VALUES
	//                                              stateValues
	//              (             tokRowBegin
	//                                              stateRow (reset row)
	//              4             tokInteger
	//                                              stateRow (append value)
	//              )             tokRowEnd
	//                                              return

	row := &parser.lastRow
	st := stateValues
	row.Length = 0

	for {
		tok, content, err := parser.lex()
		if err != nil {
			if err == io.EOF && st != stateValues {
				return errors.Errorf("syntax error: premature EOF at offset %d", parser.pos)
			}
			return errors.Trace(err)
		}
		row.Length += len(content)
		switch st {
		case stateTableName:
			switch tok {
			case tokRowBegin:
				st = stateColumns
			case tokValues:
				st = stateValues
			case tokUnquoted, tokDoubleQuoted, tokBackQuoted:
			default:
				return errors.Errorf(
					"syntax error: unexpected %s (%s) at offset %d, expecting %s",
					tok, content, parser.pos, "table name",
				)
			}
		case stateColumns:
			switch tok {
			case tokRowEnd:
				st = stateValues
			case tokUnquoted, tokDoubleQuoted, tokBackQuoted:
				columnName := strings.ToLower(parser.unescapeString(string(content)))
				parser.columns = append(parser.columns, columnName)
			default:
				return errors.Errorf(
					"syntax error: unexpected %s (%s) at offset %d, expecting %s",
					tok, content, parser.pos, "column list",
				)
			}
		case stateValues:
			switch tok {
			case tokRowBegin:
				row.RowID++
				row.Row = parser.acquireDatumSlice()
				st = stateRow
			case tokUnquoted, tokDoubleQuoted, tokBackQuoted:
				parser.columns = nil
				st = stateTableName
			case tokValues:
			default:
				return errors.Errorf(
					"syntax error: unexpected %s (%s) at offset %d, expecting %s",
					tok, content, parser.pos, "start of row",
				)
			}
		case stateRow:
			var value types.Datum
			switch tok {
			case tokRowEnd:
				return nil
			case tokNull:
				value.SetNull()
			case tokTrue:
				value.SetInt64(1)
			case tokFalse:
				value.SetInt64(0)
			case tokInteger:
				c := string(content)
				if strings.HasPrefix(c, "-") {
					i, err := strconv.ParseInt(c, 10, 64)
					if err == nil {
						value.SetInt64(i)
						break
					}
				} else {
					u, err := strconv.ParseUint(c, 10, 64)
					if err == nil {
						value.SetUint64(u)
						break
					}
				}
				// if the integer is too long, fallback to treating it as a
				// string (all types that treats integer specially like BIT
				// can't handle integers more than 64 bits anyway)
				fallthrough
			case tokUnquoted, tokSingleQuoted, tokDoubleQuoted:
				value.SetString(parser.unescapeString(string(content)), "utf8mb4_bin")
			case tokHexString:
				hexLit, err := types.ParseHexStr(string(content))
				if err != nil {
					return errors.Trace(err)
				}
				value.SetBinaryLiteral(hexLit)
			case tokBinString:
				binLit, err := types.ParseBitStr(string(content))
				if err != nil {
					return errors.Trace(err)
				}
				value.SetBinaryLiteral(binLit)
			default:
				return errors.Errorf(
					"syntax error: unexpected %s (%s) at offset %d, expecting %s",
					tok, content, parser.pos, "data literal",
				)
			}
			row.Row = append(row.Row, value)
		}
	}
}

// LastRow is the copy of the row parsed by the last call to ReadRow().
func (parser *blockParser) LastRow() Row {
	return parser.lastRow
}

// RecycleRow places the row object back into the allocation pool.
func (parser *blockParser) RecycleRow(row Row) {
	// We need farther benchmarking to make sure whether send a pointer
	// (instead of a slice) here can improve performance.
	parser.rowPool.Put(row.Row[:0])
}

// acquireDatumSlice allocates an empty []types.Datum
func (parser *blockParser) acquireDatumSlice() []types.Datum {
	return parser.rowPool.Get()
}

// ReadChunks parses the entire file and splits it into continuous chunks of
// size >= minSize.
func ReadChunks(parser Parser, minSize int64) ([]Chunk, error) {
	var chunks []Chunk

	pos, lastRowID := parser.Pos()
	cur := Chunk{
		Offset:       pos,
		EndOffset:    pos,
		PrevRowIDMax: lastRowID,
		RowIDMax:     lastRowID,
	}

	for {
		switch err := parser.ReadRow(); errors.Cause(err) {
		case nil:
			cur.EndOffset, cur.RowIDMax = parser.Pos()
			if cur.EndOffset-cur.Offset >= minSize {
				chunks = append(chunks, cur)
				cur.Offset = cur.EndOffset
				cur.PrevRowIDMax = cur.RowIDMax
			}

		case io.EOF:
			if cur.Offset < cur.EndOffset {
				chunks = append(chunks, cur)
			}
			return chunks, nil

		default:
			return nil, errors.Trace(err)
		}
	}
}

// ReadUntil parses the entire file and splits it into continuous chunks of
// size >= minSize.
func ReadUntil(parser Parser, pos int64) error {
	var curOffset int64
	for curOffset < pos {
		switch err := parser.ReadRow(); errors.Cause(err) {
		case nil:
			curOffset, _ = parser.Pos()

		case io.EOF:
			return nil

		default:
			return errors.Trace(err)
		}
	}
	return nil
}

// OpenReader opens a reader for the given file and storage.
func OpenReader(
	ctx context.Context,
	fileMeta *SourceFileMeta,
	store storage.ExternalStorage,
	decompressCfg storage.DecompressConfig,
) (reader storage.ReadSeekCloser, err error) {
	switch {
	case fileMeta.Type == SourceTypeParquet:
		reader, err = OpenParquetReader(ctx, store, fileMeta.Path, fileMeta.FileSize)
	case fileMeta.Compression != CompressionNone:
		compressType, err2 := ToStorageCompressType(fileMeta.Compression)
		if err2 != nil {
			return nil, err2
		}
		reader, err = storage.WithCompression(store, compressType, decompressCfg).Open(ctx, fileMeta.Path, nil)
	default:
		reader, err = store.Open(ctx, fileMeta.Path, nil)
	}
	return
}
