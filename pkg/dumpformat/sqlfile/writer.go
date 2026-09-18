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

package sqlfile

import (
	"database/sql"
	"fmt"
	"io"

	"github.com/pingcap/tidb/pkg/dumpformat"
)

// maxBufferedValueSize bounds how much of a row Write holds in its buffer.
// Quoted values are encoded in pieces of at most this many input bytes, and the
// buffer is written out whenever it reaches this size, so the buffer stays at a
// few MiB however wide the row is. Without it the buffer grows to the encoded
// size of the widest row, which is several times the raw size for values of
// hundreds of MiB.
const maxBufferedValueSize = 1 << 20

// Config holds the SQL framing knobs.
type Config struct {
	// StatementSize splits the INSERT statement once the bytes written for the
	// current statement reach it; 0 means a single statement per file.
	StatementSize uint64
	// EscapeBackslash selects backslash escaping instead of single-quote doubling
	// for string values.
	EscapeBackslash bool
}

// Writer encodes rows into `INSERT INTO ... VALUES (..),(..);` statements and
// writes them to an io.Writer, splitting at Config.StatementSize. The caller owns
// buffering and file rotation and must call Close to end the last statement.
type Writer struct {
	w      io.Writer
	cfg    *Config
	kinds  []dumpformat.FieldKind
	prefix []byte
	buf    []byte
	// statementSize and fileSize count each row's tuple plus the 2-byte separator
	// (",\n" or ";\n") that follows it, so a size limit trips one row early and the
	// split lands before the overflowing row. statementSize resets per statement;
	// fileSize never resets and equals the bytes written once Close adds the final
	// separator.
	statementSize uint64
	fileSize      uint64
	inStatement   bool
}

// NewWriter creates a Writer over w. prefix is the INSERT statement prefix
// (e.g. "INSERT INTO `t` VALUES\n"); kinds classifies each column.
func NewWriter(w io.Writer, prefix []byte, kinds []dumpformat.FieldKind, cfg *Config) *Writer {
	return &Writer{w: w, cfg: cfg, kinds: kinds, prefix: prefix}
}

// Write encodes one row's `(..)` tuple and writes it, with the statement prefix
// or row separator, to the underlying writer. len(row) must equal the configured
// column count; a nil field is treated as NULL. A row wider than
// maxBufferedValueSize reaches the underlying writer in several Write calls.
func (sw *Writer) Write(row []sql.RawBytes) error {
	if len(row) != len(sw.kinds) {
		return fmt.Errorf("sqlfile: row has %d fields, want %d", len(row), len(sw.kinds))
	}
	sw.buf = sw.buf[:0]
	// At the statement limit, close it; the ";\n" was counted as the last row's separator.
	if sw.inStatement && sw.cfg.StatementSize > 0 && sw.statementSize >= sw.cfg.StatementSize {
		sw.buf = append(sw.buf, ';', '\n')
		sw.inStatement = false
	}
	if !sw.inStatement {
		sw.buf = append(sw.buf, sw.prefix...)
		sw.statementSize = uint64(len(sw.prefix))
		sw.fileSize += uint64(len(sw.prefix))
		sw.inStatement = true
	} else {
		// This row's leading ",\n" was counted as the previous row's separator.
		sw.buf = append(sw.buf, ',', '\n')
	}
	start := len(sw.buf)
	// flushed counts the bytes of this row, from the prefix or separator on,
	// that were already written out to keep buf bounded.
	var flushed uint64
	sw.buf = append(sw.buf, '(')
	for i, val := range row {
		if i > 0 {
			sw.buf = append(sw.buf, ',')
		}
		n, err := sw.appendValue(val, sw.kinds[i])
		if err != nil {
			return err
		}
		flushed += n
	}
	sw.buf = append(sw.buf, ')')
	// Count the tuple plus the 2-byte separator that will follow it.
	tupleSize := flushed + uint64(len(sw.buf)) - uint64(start) + 2
	sw.statementSize += tupleSize
	sw.fileSize += tupleSize
	_, err := sw.w.Write(sw.buf)
	return err
}

// appendValue appends one field's encoding to buf like AppendValue, but encodes
// a quoted value in pieces of at most maxBufferedValueSize input bytes and
// writes buf out whenever it reaches maxBufferedValueSize. It returns the number
// of bytes written out.
func (sw *Writer) appendValue(val []byte, kind dumpformat.FieldKind) (uint64, error) {
	if val == nil || kind == dumpformat.KindNumber {
		sw.buf = AppendValue(sw.buf, val, val == nil, kind, sw.cfg.EscapeBackslash)
		return sw.flushIfFull()
	}
	sw.buf = appendOpenQuote(sw.buf, kind)
	var flushed uint64
	for len(val) > 0 {
		piece := val[:min(len(val), maxBufferedValueSize)]
		val = val[len(piece):]
		sw.buf = appendQuotedBody(sw.buf, piece, kind, sw.cfg.EscapeBackslash)
		n, err := sw.flushIfFull()
		if err != nil {
			return 0, err
		}
		flushed += n
	}
	sw.buf = append(sw.buf, '\'')
	return flushed, nil
}

// flushIfFull writes out and empties buf once it reaches maxBufferedValueSize,
// returning the number of bytes written.
func (sw *Writer) flushIfFull() (uint64, error) {
	if len(sw.buf) < maxBufferedValueSize {
		return 0, nil
	}
	n := len(sw.buf)
	_, err := sw.w.Write(sw.buf)
	sw.buf = sw.buf[:0]
	return uint64(n), err
}

// EstimateFileSize returns the logical file size for rotation. It excludes any
// preamble the caller wrote directly (e.g. SQL special comments).
func (sw *Writer) EstimateFileSize() uint64 {
	return sw.fileSize
}

// Close terminates the open statement with ";\n". It is a no-op if no statement
// is open.
func (sw *Writer) Close() error {
	if !sw.inStatement {
		return nil
	}
	sw.inStatement = false
	// The final ";\n" was already counted as the last row's separator.
	_, err := sw.w.Write([]byte{';', '\n'})
	return err
}
