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
// A wide row reaches w in several writes, so a write error can leave a value
// half-written; the Writer then keeps that error and every later Write and
// Close returns it without emitting more bytes, so a truncated row is never
// sealed into a statement that parses.
type Writer struct {
	w      io.Writer
	cfg    *Config
	kinds  []dumpformat.FieldKind
	prefix []byte
	buf    []byte
	// written counts the bytes handed to w; produced adds what buf still holds.
	// stmtStart is produced at the moment the open statement began, so the
	// statement and file sizes are differences of produced rather than separate
	// counters.
	written     uint64
	stmtStart   uint64
	inStatement bool
	// err is the first error a write to w returned.
	err error
}

// produced returns the bytes encoded so far, written out or still buffered.
func (sw *Writer) produced() uint64 {
	return sw.written + uint64(len(sw.buf))
}

// statementSize returns the open statement's size, counting the ";\n" that will
// close it.
func (sw *Writer) statementSize() uint64 {
	return sw.produced() - sw.stmtStart + 2
}

// NewWriter creates a Writer over w. prefix is the INSERT statement prefix
// (e.g. "INSERT INTO `t` VALUES\n"); kinds classifies each column.
func NewWriter(w io.Writer, prefix []byte, kinds []dumpformat.FieldKind, cfg *Config) *Writer {
	return &Writer{w: w, cfg: cfg, kinds: kinds, prefix: prefix}
}

// Write encodes one row's `(..)` tuple and writes it, with the statement prefix
// or row separator, to the underlying writer. len(row) must equal the configured
// column count; a nil field is treated as NULL.
func (sw *Writer) Write(row []sql.RawBytes) error {
	if sw.err != nil {
		return sw.err
	}
	if len(row) != len(sw.kinds) {
		return fmt.Errorf("sqlfile: row has %d fields, want %d", len(row), len(sw.kinds))
	}
	sw.buf = sw.buf[:0]
	if sw.inStatement && sw.cfg.StatementSize > 0 && sw.statementSize() >= sw.cfg.StatementSize {
		sw.buf = append(sw.buf, ';', '\n')
		sw.inStatement = false
	}
	if !sw.inStatement {
		sw.stmtStart = sw.produced()
		sw.buf = append(sw.buf, sw.prefix...)
		sw.inStatement = true
	} else {
		sw.buf = append(sw.buf, ',', '\n')
	}
	sw.buf = append(sw.buf, '(')
	for i, val := range row {
		if i > 0 {
			sw.buf = append(sw.buf, ',')
		}
		if err := sw.appendValue(val, sw.kinds[i]); err != nil {
			return err
		}
	}
	sw.buf = append(sw.buf, ')')
	return sw.flush()
}

// appendValue appends one field's encoding to buf like AppendValue, but encodes
// a quoted value in pieces of at most dumpformat.MaxBufferedValueSize input
// bytes and writes buf out whenever it reaches that size.
func (sw *Writer) appendValue(val []byte, kind dumpformat.FieldKind) error {
	// NULL (a nil val) and numbers are written unquoted and are never large.
	isNull := val == nil
	if isNull || kind == dumpformat.KindNumber {
		sw.buf = AppendValue(sw.buf, val, isNull, kind, sw.cfg.EscapeBackslash)
		return sw.maybeFlush()
	}
	sw.buf = appendOpenQuote(sw.buf, kind)
	for len(val) > 0 {
		var n int
		sw.buf, n = appendQuotedBody(sw.buf, val, dumpformat.MaxBufferedValueSize, kind, sw.cfg.EscapeBackslash)
		val = val[n:]
		if err := sw.maybeFlush(); err != nil {
			return err
		}
	}
	sw.buf = append(sw.buf, '\'')
	return nil
}

// maybeFlush writes buf out once it reaches dumpformat.MaxBufferedValueSize.
func (sw *Writer) maybeFlush() error {
	if len(sw.buf) < dumpformat.MaxBufferedValueSize {
		return nil
	}
	return sw.flush()
}

// flush writes buf out and empties it, keeping the first error it hits.
func (sw *Writer) flush() error {
	n, err := sw.w.Write(sw.buf)
	sw.written += uint64(n)
	sw.buf = sw.buf[:0]
	if err != nil && sw.err == nil {
		sw.err = err
	}
	return err
}

// EstimateFileSize returns the logical file size for rotation. It excludes any
// preamble the caller wrote directly (e.g. SQL special comments).
func (sw *Writer) EstimateFileSize() uint64 {
	if !sw.inStatement {
		return sw.produced()
	}
	return sw.produced() + 2
}

// Close terminates the open statement with ";\n". It is a no-op if no statement
// is open, and writes nothing if an earlier write failed.
func (sw *Writer) Close() error {
	if sw.err != nil {
		return sw.err
	}
	if !sw.inStatement {
		return nil
	}
	sw.inStatement = false
	sw.buf = append(sw.buf[:0], ';', '\n')
	return sw.flush()
}
