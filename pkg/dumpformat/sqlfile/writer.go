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
	"encoding/hex"
	"fmt"
	"io"

	"github.com/pingcap/tidb/pkg/dumpformat"
)

// maxBufferedValueSize bounds how much of a row Write holds in its buffer. A
// string or binary value longer than this is encoded in pieces of at most this
// many input bytes, each written out as soon as it is encoded, and the buffer
// is flushed whenever it reaches this size between values. Without it the
// buffer grows to the encoded size of the widest row, which is several times
// the raw size for values of hundreds of MiB.
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
	// start is where the rest of the tuple begins in buf, and flushed counts the
	// tuple bytes already written out; start drops to 0 after the first flush.
	start := len(sw.buf)
	var flushed uint64
	sw.buf = append(sw.buf, '(')
	for i, val := range row {
		if i > 0 {
			sw.buf = append(sw.buf, ',')
		}
		kind := sw.kinds[i]
		if val != nil && kind != dumpformat.KindNumber && len(val) > maxBufferedValueSize {
			n, err := sw.writeLargeValue(val, kind, start)
			if err != nil {
				return err
			}
			flushed, start = flushed+n, 0
			continue
		}
		sw.buf = AppendValue(sw.buf, val, val == nil, kind, sw.cfg.EscapeBackslash)
		if len(sw.buf) >= maxBufferedValueSize {
			n, err := sw.flushBuf(start)
			if err != nil {
				return err
			}
			flushed, start = flushed+n, 0
		}
	}
	sw.buf = append(sw.buf, ')')
	// Count the tuple plus the 2-byte separator that will follow it.
	tupleSize := flushed + uint64(len(sw.buf)-start) + 2
	sw.statementSize += tupleSize
	sw.fileSize += tupleSize
	_, err := sw.w.Write(sw.buf)
	return err
}

// writeLargeValue appends a quoted string or x'..' literal for val, encoding it
// in pieces of at most maxBufferedValueSize input bytes and flushing after each
// piece. Hex encoding and both string escapings map every input byte on its
// own, so the pieces concatenate to exactly what AppendValue produces. The
// current tuple begins at offset start in buf; writeLargeValue returns how many
// tuple bytes it wrote out, and leaves only the closing quote in buf.
func (sw *Writer) writeLargeValue(val []byte, kind dumpformat.FieldKind, start int) (uint64, error) {
	if kind == dumpformat.KindBytes {
		sw.buf = append(sw.buf, 'x')
	}
	sw.buf = append(sw.buf, '\'')
	var flushed uint64
	for len(val) > 0 {
		piece := val[:min(len(val), maxBufferedValueSize)]
		val = val[len(piece):]
		if kind == dumpformat.KindBytes {
			sw.buf = hex.AppendEncode(sw.buf, piece)
		} else {
			sw.buf = appendEscaped(sw.buf, piece, sw.cfg.EscapeBackslash)
		}
		n, err := sw.flushBuf(start)
		if err != nil {
			return 0, err
		}
		flushed, start = flushed+n, 0
	}
	sw.buf = append(sw.buf, '\'')
	return flushed, nil
}

// flushBuf writes out and empties buf, returning how many of the written bytes
// belong to the current tuple, which begins at offset start in buf.
func (sw *Writer) flushBuf(start int) (uint64, error) {
	n := uint64(len(sw.buf) - start)
	_, err := sw.w.Write(sw.buf)
	sw.buf = sw.buf[:0]
	return n, err
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
