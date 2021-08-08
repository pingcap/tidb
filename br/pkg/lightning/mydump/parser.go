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
// See the License for the specific language governing permissions and
// limitations under the License.

package mydump

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type blockParser struct {
	// states for the lexer
	reader      PooledReader
	buf         []byte
	blockBuf    []byte
	isLastChunk bool

	// The list of column names of the last INSERT statement.
	columns []string

	rowPool *sync.Pool
	lastRow Row
	// Current file offset.
	pos int64

	// cache
	remainBuf *bytes.Buffer
	appendBuf *bytes.Buffer

	// the Logger associated with this parser for reporting failure
	Logger log.Logger
}

func makeBlockParser(reader ReadSeekCloser, blockBufSize int64, ioWorkers *worker.Pool) blockParser {
	return blockParser{
		reader:    MakePooledReader(reader, ioWorkers),
		blockBuf:  make([]byte, blockBufSize*config.BufferSizeScale),
		remainBuf: &bytes.Buffer{},
		appendBuf: &bytes.Buffer{},
		Logger:    log.L(),
		rowPool: &sync.Pool{
			New: func() interface{} {
				return make([]types.Datum, 0, 16)
			},
		},
	}
}

// ChunkParser is a parser of the data files (the file containing only INSERT
// statements).
type ChunkParser struct {
	blockParser

	escFlavor backslashEscapeFlavor
}

// Chunk represents a portion of the data file.
type Chunk struct {
	Offset       int64
	EndOffset    int64
	PrevRowIDMax int64
	RowIDMax     int64
	Columns      []string
}

// Row is the content of a row.
type Row struct {
	RowID int64
	Row   []types.Datum
}

// MarshalLogArray implements the zapcore.ArrayMarshaler interface
func (row Row) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for _, r := range row.Row {
		encoder.AppendString(r.String())
	}
	return nil
}

type backslashEscapeFlavor uint8

const (
	backslashEscapeFlavorNone backslashEscapeFlavor = iota
	backslashEscapeFlavorMySQL
	backslashEscapeFlavorMySQLWithNull
)

type Parser interface {
	Pos() (pos int64, rowID int64)
	SetPos(pos int64, rowID int64) error
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
}

// NewChunkParser creates a new parser which can read chunks out of a file.
func NewChunkParser(
	sqlMode mysql.SQLMode,
	reader ReadSeekCloser,
	blockBufSize int64,
	ioWorkers *worker.Pool,
) *ChunkParser {
	escFlavor := backslashEscapeFlavorMySQL
	if sqlMode.HasNoBackslashEscapesMode() {
		escFlavor = backslashEscapeFlavorNone
	}

	return &ChunkParser{
		blockParser: makeBlockParser(reader, blockBufSize, ioWorkers),
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

// Pos returns the current file offset.
func (parser *blockParser) Pos() (int64, int64) {
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
		parser.appendBuf.Write(parser.blockBuf[:n])
		parser.buf = parser.appendBuf.Bytes()
		metric.ChunkParserReadBlockSecondsHistogram.Observe(time.Since(startTime).Seconds())
		return nil
	default:
		return errors.Trace(err)
	}
}

var unescapeRegexp = regexp.MustCompile(`(?s)\\.`)

func unescape(
	input string,
	delim string,
	escFlavor backslashEscapeFlavor,
) string {
	if len(delim) > 0 {
		delim2 := delim + delim
		if strings.Contains(input, delim2) {
			input = strings.ReplaceAll(input, delim2, delim)
		}
	}
	if escFlavor != backslashEscapeFlavorNone && strings.IndexByte(input, '\\') != -1 {
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
			return unescape(input[1:len(input)-1], input[:1], parser.escFlavor)
		case '`':
			return unescape(input[1:len(input)-1], "`", backslashEscapeFlavorNone)
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

	for {
		tok, content, err := parser.lex()
		if err != nil {
			if err == io.EOF && st != stateValues {
				return errors.Errorf("syntax error: premature EOF at offset %d", parser.pos)
			}
			return errors.Trace(err)
		}
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
	//nolint:staticcheck
	parser.rowPool.Put(row.Row[:0])
}

// acquireDatumSlice allocates an empty []types.Datum
func (parser *blockParser) acquireDatumSlice() []types.Datum {
	return parser.rowPool.Get().([]types.Datum)
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
