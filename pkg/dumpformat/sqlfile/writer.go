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
)

// SQLWriter encodes rows into `INSERT INTO ... VALUES (..),(..);` statements and
// writes them to an io.Writer. It owns the statement framing and splits at
// Config.StatementSize; the caller owns buffering, file-size rotation and upload
// and must call Close to terminate the open statement.
type SQLWriter struct {
	w      io.Writer
	cfg    *Config
	kinds  []FieldKind
	prefix []byte
	// buf is the reused per-row scratch.
	buf []byte
	// statementSize tracks the bytes accounted for the open statement, including
	// the 2-byte separator anticipated after each row, matching dumpling's
	// currentStatementSize so statements split at the same row.
	statementSize int64
	inStatement   bool
	// fileSize is the logical size for EstimateFileSize: like statementSize it
	// counts the 2-byte separator anticipated after each row, but it never resets,
	// matching dumpling's currentFileSize so files rotate at the same row. At
	// Close the anticipated bytes equal the real ones, so it also equals the
	// actual bytes written.
	fileSize int64
}

// NewSQLWriter creates a SQLWriter over w. prefix is the INSERT statement prefix
// (e.g. "INSERT INTO `t` VALUES\n"); kinds classifies each column; cfg holds the
// statement-size and escaping knobs.
func NewSQLWriter(w io.Writer, prefix []byte, kinds []FieldKind, cfg *Config) *SQLWriter {
	return &SQLWriter{w: w, cfg: cfg, kinds: kinds, prefix: prefix}
}

// Write encodes one row's `(..)` tuple and writes it, with the statement prefix
// or row separator, to the underlying writer. len(row) must equal the configured
// column count; a nil field is treated as NULL.
func (sw *SQLWriter) Write(row []sql.RawBytes) error {
	if len(row) != len(sw.kinds) {
		return fmt.Errorf("sqlfile: row has %d fields, want %d", len(row), len(sw.kinds))
	}
	sw.buf = sw.buf[:0]
	// Close the open statement before this row when it reached the size limit.
	// The terminating ";\n" is the 2 bytes anticipated by the previous row.
	if sw.inStatement && sw.cfg.StatementSize > 0 && sw.statementSize >= sw.cfg.StatementSize {
		sw.buf = append(sw.buf, ';', '\n')
		sw.inStatement = false
	}
	if !sw.inStatement {
		sw.buf = append(sw.buf, sw.prefix...)
		sw.statementSize = int64(len(sw.prefix))
		sw.fileSize += int64(len(sw.prefix))
		sw.inStatement = true
	} else {
		// The leading "," + "\n" of this row is the separator anticipated by the
		// previous row, so it is not re-counted here.
		sw.buf = append(sw.buf, ',', '\n')
	}
	start := len(sw.buf)
	sw.buf = append(sw.buf, '(')
	for i, val := range row {
		if i > 0 {
			sw.buf = append(sw.buf, ',')
		}
		sw.buf = AppendValue(sw.buf, val, val == nil, sw.kinds[i], sw.cfg.EscapeBackslash)
	}
	sw.buf = append(sw.buf, ')')
	// Account the tuple plus the 2-byte separator that will follow it (",\n" for
	// the next row, or ";\n" at statement end), matching dumpling.
	tupleSize := int64(len(sw.buf)-start) + 2
	sw.statementSize += tupleSize
	sw.fileSize += tupleSize
	_, err := sw.w.Write(sw.buf)
	return err
}

// EstimateFileSize returns the logical file size for rotation. It excludes any
// preamble the caller wrote directly (e.g. SQL special comments).
func (sw *SQLWriter) EstimateFileSize() uint64 {
	return uint64(sw.fileSize)
}

// Close terminates the open statement with ";\n". It is a no-op if no statement
// is open.
func (sw *SQLWriter) Close() error {
	if !sw.inStatement {
		return nil
	}
	sw.inStatement = false
	// The final ";\n" is the 2 bytes already anticipated by the last row, so
	// fileSize is not incremented here.
	_, err := sw.w.Write([]byte{';', '\n'})
	return err
}
