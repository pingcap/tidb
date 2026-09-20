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

package csvfile

import (
	"database/sql"
	"fmt"
	"io"

	"github.com/pingcap/tidb/pkg/dumpformat"
)

// Writer is a single-stream CSV encoder that writes framed/escaped rows to an
// io.Writer. The caller owns buffering and file rotation. A failed Write may
// have written part of a row, so abandon the Writer and discard its output.
type Writer struct {
	w       io.Writer
	cfg     *Config
	kinds   []dumpformat.FieldKind
	buf     []byte
	written int64
}

// NewWriter creates a Writer over w.
func NewWriter(w io.Writer, kinds []dumpformat.FieldKind, cfg *Config) *Writer {
	return &Writer{w: w, cfg: cfg, kinds: kinds}
}

// Write encodes one row and writes it, with the line terminator, to the
// underlying writer.
func (cw *Writer) Write(row []sql.RawBytes) error {
	if len(row) != len(cw.kinds) {
		return fmt.Errorf("csvfile: row has %d fields, want %d", len(row), len(cw.kinds))
	}
	cw.buf = cw.buf[:0]
	for i, val := range row {
		if i > 0 {
			cw.buf = append(cw.buf, cw.cfg.FieldsTerminatedBy...)
		}
		if err := cw.appendValue(val, cw.kinds[i]); err != nil {
			return err
		}
	}
	return cw.flush()
}

// appendValue appends one field's encoding to buf like appendField, but encodes
// an enclosed value in pieces of about dumpformat.MaxBufferedValueSize input
// bytes and writes buf out whenever it reaches that size.
func (cw *Writer) appendValue(val []byte, kind dumpformat.FieldKind) error {
	// NULL (a nil val) and numbers are written unenclosed and are never large.
	isNull := val == nil
	if isNull || kind == dumpformat.KindNumber {
		cw.buf = appendField(cw.buf, val, isNull, kind, cw.cfg)
		return cw.maybeFlush()
	}
	cw.buf = append(cw.buf, cw.cfg.FieldsEnclosedBy...)
	for len(val) > 0 {
		var n int
		cw.buf, n = appendFieldBody(cw.buf, val, dumpformat.MaxBufferedValueSize, kind, cw.cfg)
		val = val[n:]
		if err := cw.maybeFlush(); err != nil {
			return err
		}
	}
	cw.buf = append(cw.buf, cw.cfg.FieldsEnclosedBy...)
	return nil
}

// maybeFlush writes out and empties buf once it reaches
// dumpformat.MaxBufferedValueSize.
func (cw *Writer) maybeFlush() error {
	if len(cw.buf) < dumpformat.MaxBufferedValueSize {
		return nil
	}
	n, err := cw.w.Write(cw.buf)
	cw.written += int64(n)
	cw.buf = cw.buf[:0]
	return err
}

// WriteHeader writes a header row: each name as a string field (enclosed when
// FieldsEnclosedBy is set), separated and terminated like a data row.
func (cw *Writer) WriteHeader(names [][]byte) error {
	cw.buf = cw.buf[:0]
	for i, name := range names {
		if i > 0 {
			cw.buf = append(cw.buf, cw.cfg.FieldsTerminatedBy...)
		}
		cw.buf = appendField(cw.buf, name, false, dumpformat.KindString, cw.cfg)
	}
	return cw.flush()
}

// flush appends the line terminator to the scratch and writes it to the file.
func (cw *Writer) flush() error {
	cw.buf = append(cw.buf, cw.cfg.LinesTerminatedBy...)
	n, err := cw.w.Write(cw.buf)
	cw.written += int64(n)
	return err
}

// EstimateFileSize returns the bytes written to the current file.
func (cw *Writer) EstimateFileSize() uint64 {
	return uint64(cw.written)
}

// Close finalizes the writer. Currently it's a no-op.
func (cw *Writer) Close() error {
	return nil
}
