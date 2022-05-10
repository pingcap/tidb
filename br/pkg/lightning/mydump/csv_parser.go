// Copyright 2020 PingCAP, Inc.
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
	"io"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/worker"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mathutil"
)

var (
	errUnterminatedQuotedField = errors.NewNoStackError("syntax error: unterminated quoted field")
	errDanglingBackslash       = errors.NewNoStackError("syntax error: no character after backslash")
	errUnexpectedQuoteField    = errors.NewNoStackError("syntax error: cannot have consecutive fields without separator")
)

// CSVParser is basically a copy of encoding/csv, but special-cased for MySQL-like input.
type CSVParser struct {
	blockParser
	cfg *config.CSVConfig

	comma   []byte
	quote   []byte
	newLine []byte

	charsetConvertor *CharsetConvertor
	// These variables are used with IndexAnyByte to search a byte slice for the
	// first index which some special character may appear.
	// quoteByteSet is used inside quoted fields (so the first characters of
	// the closing delimiter and backslash are special).
	// unquoteByteSet is used outside quoted fields (so the first characters
	// of the opening delimiter, separator, terminator and backslash are
	// special).
	// newLineByteSet is used in strict-format CSV dividing (so the first
	// characters of the terminator are special).
	quoteByteSet   byteSet
	unquoteByteSet byteSet
	newLineByteSet byteSet

	// recordBuffer holds the unescaped fields, one after another.
	// The fields can be accessed by using the indexes in fieldIndexes.
	// E.g., For the row `a,"b","c""d",e`, recordBuffer will contain `abc"de`
	// and fieldIndexes will contain the indexes [1, 2, 5, 6].
	recordBuffer []byte

	// fieldIndexes is an index of fields inside recordBuffer.
	// The i'th field ends at offset fieldIndexes[i] in recordBuffer.
	fieldIndexes []int

	lastRecord []string

	escFlavor backslashEscapeFlavor
	// if set to true, csv parser will treat the first non-empty line as header line
	shouldParseHeader bool
}

func NewCSVParser(
	cfg *config.CSVConfig,
	reader ReadSeekCloser,
	blockBufSize int64,
	ioWorkers *worker.Pool,
	shouldParseHeader bool,
	charsetConvertor *CharsetConvertor,
) (*CSVParser, error) {
	var err error
	var separator, delimiter, terminator string
	// Do not do the conversion if the charsetConvertor is nil.
	if charsetConvertor == nil {
		separator = cfg.Separator
		delimiter = cfg.Delimiter
		terminator = cfg.Terminator
	} else {
		separator, delimiter, terminator, err = encodeSpecialSymbols(cfg, charsetConvertor)
		if err != nil {
			return nil, err
		}
	}

	var quoteStopSet, newLineStopSet []byte
	unquoteStopSet := []byte{separator[0]}
	if len(cfg.Delimiter) > 0 {
		quoteStopSet = []byte{delimiter[0]}
		unquoteStopSet = append(unquoteStopSet, delimiter[0])
	}
	if len(terminator) > 0 {
		newLineStopSet = []byte{terminator[0]}
	} else {
		// The character set encoding of '\r' and '\n' is the same in UTF-8 and GBK.
		newLineStopSet = []byte{'\r', '\n'}
	}
	unquoteStopSet = append(unquoteStopSet, newLineStopSet...)

	escFlavor := backslashEscapeFlavorNone
	if cfg.BackslashEscape {
		escFlavor = backslashEscapeFlavorMySQL
		quoteStopSet = append(quoteStopSet, '\\')
		unquoteStopSet = append(unquoteStopSet, '\\')
		// we need special treatment of the NULL value \N, used by MySQL.
		if !cfg.NotNull && cfg.Null == `\N` {
			escFlavor = backslashEscapeFlavorMySQLWithNull
		}
	}

	return &CSVParser{
		blockParser:       makeBlockParser(reader, blockBufSize, ioWorkers),
		cfg:               cfg,
		charsetConvertor:  charsetConvertor,
		comma:             []byte(separator),
		quote:             []byte(delimiter),
		newLine:           []byte(terminator),
		escFlavor:         escFlavor,
		quoteByteSet:      makeByteSet(quoteStopSet),
		unquoteByteSet:    makeByteSet(unquoteStopSet),
		newLineByteSet:    makeByteSet(newLineStopSet),
		shouldParseHeader: shouldParseHeader,
	}, nil
}

// encodeSpecialSymbols will encode the special symbols, e,g, separator, delimiter and terminator
// with the given charset according to the charset convertor.
func encodeSpecialSymbols(cfg *config.CSVConfig, cc *CharsetConvertor) (separator, delimiter, terminator string, err error) {
	// Separator
	separator, err = cc.Encode(cfg.Separator)
	if err != nil {
		return
	}
	// Delimiter
	delimiter, err = cc.Encode(cfg.Delimiter)
	if err != nil {
		return
	}
	// Terminator
	terminator, err = cc.Encode(cfg.Terminator)
	if err != nil {
		return
	}
	return
}

func (parser *CSVParser) unescapeString(input string) (unescaped string, isNull bool, err error) {
	// Convert the input from another charset to utf8mb4 before we return the string.
	if input, err = parser.charsetConvertor.Decode(input); err != nil {
		return
	}
	if parser.escFlavor == backslashEscapeFlavorMySQLWithNull && input == `\N` {
		return input, true, nil
	}
	unescaped = unescape(input, "", parser.escFlavor)
	isNull = parser.escFlavor != backslashEscapeFlavorMySQLWithNull &&
		!parser.cfg.NotNull &&
		unescaped == parser.cfg.Null
	return
}

// csvToken is a type representing either a normal byte or some CSV-specific
// tokens such as the separator (comma), delimiter (quote) and terminator (new
// line).
type csvToken int16

const (
	// csvTokenAnyUnquoted is a placeholder to represent any unquoted character.
	csvTokenAnyUnquoted csvToken = 0
	// csvTokenWithBackslash is a mask indicating an escaped character.
	// The actual token is represented like `csvTokenWithBackslash | 'n'`.
	csvTokenWithBackslash csvToken = 0x100
	// csvTokenComma is the CSV separator token.
	csvTokenComma csvToken = 0x200
	// csvTokenNewLine is the CSV terminator token.
	csvTokenNewLine csvToken = 0x400
	// csvTokenDelimiter is the CSV delimiter token.
	csvTokenDelimiter csvToken = 0x800
)

func (parser *CSVParser) readByte() (byte, error) {
	if len(parser.buf) == 0 {
		if err := parser.readBlock(); err != nil {
			return 0, err
		}
	}
	if len(parser.buf) == 0 {
		return 0, io.EOF
	}
	b := parser.buf[0]
	parser.buf = parser.buf[1:]
	parser.pos++
	return b, nil
}

func (parser *CSVParser) peekBytes(cnt int) ([]byte, error) {
	if len(parser.buf) < cnt {
		if err := parser.readBlock(); err != nil {
			return nil, err
		}
	}
	if len(parser.buf) == 0 {
		return nil, io.EOF
	}
	cnt = mathutil.Min(cnt, len(parser.buf))
	return parser.buf[:cnt], nil
}

func (parser *CSVParser) skipBytes(n int) {
	parser.buf = parser.buf[n:]
	parser.pos += int64(n)
}

// tryReadExact peeks the bytes ahead, and if it matches `content` exactly will
// consume it (advance the cursor) and return `true`.
func (parser *CSVParser) tryReadExact(content []byte) (bool, error) {
	if len(content) == 0 {
		return true, nil
	}
	bs, err := parser.peekBytes(len(content))
	if err == nil {
		if bytes.Equal(bs, content) {
			parser.skipBytes(len(content))
			return true, nil
		}
	} else if errors.Cause(err) == io.EOF {
		err = nil
	}
	return false, err
}

func (parser *CSVParser) tryReadNewLine(b byte) (bool, error) {
	if len(parser.newLine) == 0 {
		return b == '\r' || b == '\n', nil
	}
	if b != parser.newLine[0] {
		return false, nil
	}
	return parser.tryReadExact(parser.newLine[1:])
}

func (parser *CSVParser) tryReadOpenDelimiter(b byte) (bool, error) {
	if len(parser.quote) == 0 || parser.quote[0] != b {
		return false, nil
	}
	return parser.tryReadExact(parser.quote[1:])
}

// tryReadCloseDelimiter is currently equivalent to tryReadOpenDelimiter until
// we support asymmetric delimiters.
func (parser *CSVParser) tryReadCloseDelimiter(b byte) (bool, error) {
	if parser.quote[0] != b {
		return false, nil
	}
	return parser.tryReadExact(parser.quote[1:])
}

func (parser *CSVParser) tryReadComma(b byte) (bool, error) {
	if parser.comma[0] != b {
		return false, nil
	}
	return parser.tryReadExact(parser.comma[1:])
}

func (parser *CSVParser) tryReadBackslashed(bs byte) (bool, byte, error) {
	if bs != '\\' || parser.escFlavor == backslashEscapeFlavorNone {
		return false, 0, nil
	}
	b, err := parser.readByte()
	return true, b, parser.replaceEOF(err, errDanglingBackslash)
}

// readQuoteToken reads a token inside quoted fields.
func (parser *CSVParser) readQuotedToken(b byte) (csvToken, error) {
	if ok, err := parser.tryReadCloseDelimiter(b); ok || err != nil {
		return csvTokenDelimiter, err
	}
	if ok, eb, err := parser.tryReadBackslashed(b); ok || err != nil {
		return csvTokenWithBackslash | csvToken(eb), err
	}
	return csvToken(b), nil
}

// readUnquoteToken reads a token outside quoted fields.
func (parser *CSVParser) readUnquoteToken(b byte) (csvToken, error) {
	if ok, err := parser.tryReadNewLine(b); ok || err != nil {
		return csvTokenNewLine, err
	}
	if ok, err := parser.tryReadComma(b); ok || err != nil {
		return csvTokenComma, err
	}
	if ok, err := parser.tryReadOpenDelimiter(b); ok || err != nil {
		return csvTokenDelimiter, err
	}
	if ok, eb, err := parser.tryReadBackslashed(b); ok || err != nil {
		return csvTokenWithBackslash | csvToken(eb), err
	}
	return csvToken(b), nil
}

func (parser *CSVParser) appendCSVTokenToRecordBuffer(token csvToken) {
	if token&csvTokenWithBackslash != 0 {
		parser.recordBuffer = append(parser.recordBuffer, '\\')
	}
	parser.recordBuffer = append(parser.recordBuffer, byte(token))
}

// readUntil reads the buffer until any character from the `chars` set is found.
// that character is excluded from the final buffer.
func (parser *CSVParser) readUntil(chars *byteSet) ([]byte, byte, error) {
	index := IndexAnyByte(parser.buf, chars)
	if index >= 0 {
		ret := parser.buf[:index]
		parser.buf = parser.buf[index:]
		parser.pos += int64(index)
		return ret, parser.buf[0], nil
	}

	// not found in parser.buf, need allocate and loop.
	var buf []byte
	for {
		buf = append(buf, parser.buf...)
		parser.buf = nil
		if err := parser.readBlock(); err != nil || len(parser.buf) == 0 {
			if err == nil {
				err = io.EOF
			}
			parser.pos += int64(len(buf))
			return buf, 0, errors.Trace(err)
		}
		index := IndexAnyByte(parser.buf, chars)
		if index >= 0 {
			buf = append(buf, parser.buf[:index]...)
			parser.buf = parser.buf[index:]
			parser.pos += int64(len(buf))
			return buf, parser.buf[0], nil
		}
	}
}

func (parser *CSVParser) readRecord(dst []string) ([]string, error) {
	parser.recordBuffer = parser.recordBuffer[:0]
	parser.fieldIndexes = parser.fieldIndexes[:0]

	isEmptyLine := true
	whitespaceLine := true
	prevToken := csvTokenNewLine
	var firstToken csvToken

outside:
	for {
		content, firstByte, err := parser.readUntil(&parser.unquoteByteSet)

		if len(content) > 0 {
			isEmptyLine = false
			if prevToken == csvTokenDelimiter {
				parser.logSyntaxError()
				return nil, errors.AddStack(errUnexpectedQuoteField)
			}
			parser.recordBuffer = append(parser.recordBuffer, content...)
			prevToken = csvTokenAnyUnquoted
		}

		if err != nil {
			if isEmptyLine || errors.Cause(err) != io.EOF {
				return nil, err
			}
			// treat EOF as the same as trailing \n.
			firstToken = csvTokenNewLine
		} else {
			parser.skipBytes(1)
			firstToken, err = parser.readUnquoteToken(firstByte)
			if err != nil {
				return nil, err
			}
		}

		switch firstToken {
		case csvTokenComma:
			whitespaceLine = false
			parser.fieldIndexes = append(parser.fieldIndexes, len(parser.recordBuffer))
		case csvTokenDelimiter:
			if prevToken != csvTokenComma && prevToken != csvTokenNewLine {
				parser.logSyntaxError()
				return nil, errors.AddStack(errUnexpectedQuoteField)
			}
			if err = parser.readQuotedField(); err != nil {
				return nil, err
			}
			whitespaceLine = false
		case csvTokenNewLine:
			// new line = end of record (ignore empty lines)
			prevToken = firstToken
			if isEmptyLine {
				continue
			}
			// skip lines only contain whitespaces
			if err == nil && whitespaceLine && len(bytes.TrimSpace(parser.recordBuffer)) == 0 {
				parser.recordBuffer = parser.recordBuffer[:0]
				continue
			}
			parser.fieldIndexes = append(parser.fieldIndexes, len(parser.recordBuffer))
			break outside
		default:
			if prevToken == csvTokenDelimiter {
				parser.logSyntaxError()
				return nil, errors.AddStack(errUnexpectedQuoteField)
			}
			parser.appendCSVTokenToRecordBuffer(firstToken)
		}
		prevToken = firstToken
		isEmptyLine = false
	}
	// Create a single string and create slices out of it.
	// This pins the memory of the fields together, but allocates once.
	str := string(parser.recordBuffer) // Convert to string once to batch allocations
	dst = dst[:0]
	if cap(dst) < len(parser.fieldIndexes) {
		dst = make([]string, len(parser.fieldIndexes))
	}
	dst = dst[:len(parser.fieldIndexes)]
	var preIdx int
	for i, idx := range parser.fieldIndexes {
		dst[i] = str[preIdx:idx]
		preIdx = idx
	}

	// Check or update the expected fields per record.
	return dst, nil
}

func (parser *CSVParser) readQuotedField() error {
	for {
		content, terminator, err := parser.readUntil(&parser.quoteByteSet)
		err = parser.replaceEOF(err, errUnterminatedQuotedField)
		if err != nil {
			return err
		}
		parser.recordBuffer = append(parser.recordBuffer, content...)
		parser.skipBytes(1)

		token, err := parser.readQuotedToken(terminator)
		if err != nil {
			return err
		}

		switch token {
		case csvTokenDelimiter:
			// encountered '"' -> continue if we're seeing '""'.
			doubledDelimiter, err := parser.tryReadExact(parser.quote)
			if err != nil {
				return err
			}
			if doubledDelimiter {
				// consume the double quotation mark and continue
				parser.recordBuffer = append(parser.recordBuffer, parser.quote...)
			} else {
				// the field is completed, exit.
				return nil
			}
		default:
			parser.appendCSVTokenToRecordBuffer(token)
		}
	}
}

func (parser *CSVParser) replaceEOF(err error, replaced error) error {
	if err == nil || errors.Cause(err) != io.EOF {
		return err
	}
	if replaced != nil {
		parser.logSyntaxError()
		replaced = errors.AddStack(replaced)
	}
	return replaced
}

// ReadRow reads a row from the datafile.
func (parser *CSVParser) ReadRow() error {
	row := &parser.lastRow
	row.Length = 0
	row.RowID++

	// skip the header first
	if parser.shouldParseHeader {
		err := parser.ReadColumns()
		if err != nil {
			return errors.Trace(err)
		}
		parser.shouldParseHeader = false
	}

	records, err := parser.readRecord(parser.lastRecord)
	if err != nil {
		return errors.Trace(err)
	}
	parser.lastRecord = records
	// remove the last empty value
	if parser.cfg.TrimLastSep {
		i := len(records) - 1
		if i >= 0 && len(records[i]) == 0 {
			records = records[:i]
		}
	}

	row.Row = parser.acquireDatumSlice()
	if cap(row.Row) >= len(records) {
		row.Row = row.Row[:len(records)]
	} else {
		row.Row = make([]types.Datum, len(records))
	}
	for i, record := range records {
		row.Length += len(record)
		unescaped, isNull, err := parser.unescapeString(record)
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			row.Row[i].SetNull()
		} else {
			row.Row[i].SetString(unescaped, "utf8mb4_bin")
		}
	}

	return nil
}

func (parser *CSVParser) ReadColumns() error {
	columns, err := parser.readRecord(nil)
	if err != nil {
		return errors.Trace(err)
	}
	parser.columns = make([]string, 0, len(columns))
	for _, colName := range columns {
		colName, _, err = parser.unescapeString(colName)
		if err != nil {
			return errors.Trace(err)
		}
		parser.columns = append(parser.columns, strings.ToLower(colName))
	}
	return nil
}

// ReadUntilTerminator seeks the file until the terminator token is found, and
// returns the file offset beyond the terminator.
// This function is used in strict-format dividing a CSV file.
func (parser *CSVParser) ReadUntilTerminator() (int64, error) {
	for {
		_, firstByte, err := parser.readUntil(&parser.newLineByteSet)
		if err != nil {
			return 0, err
		}
		parser.skipBytes(1)
		if ok, err := parser.tryReadNewLine(firstByte); ok || err != nil {
			return parser.pos, err
		}
	}
}
