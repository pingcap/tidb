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
)

// CSVWriter is a single-stream CSV encoder that writes framed/escaped rows to an
// io.Writer. The caller owns buffering and file rotation.
type CSVWriter struct {
	w       io.Writer
	cfg     *Config
	kinds   []FieldKind
	buf     []byte
	written int64
}

// NewCSVWriter creates a CSVWriter over w. kinds classifies each column
// (Number/String/Bytes) and cfg holds the framing knobs; both are caller inputs.
func NewCSVWriter(w io.Writer, kinds []FieldKind, cfg *Config) *CSVWriter {
	return &CSVWriter{w: w, cfg: cfg, kinds: kinds}
}

// Write encodes one row and writes it, with the line terminator, to the
// underlying writer.
func (cw *CSVWriter) Write(row []sql.RawBytes) error {
	if len(row) != len(cw.kinds) {
		return fmt.Errorf("csvfile: row has %d fields, want %d", len(row), len(cw.kinds))
	}
	cw.buf = cw.buf[:0]
	for i, val := range row {
		if i > 0 {
			cw.buf = append(cw.buf, cw.cfg.Separator...)
		}
		cw.buf = appendField(cw.buf, val, val == nil, cw.kinds[i], cw.cfg)
	}
	return cw.flush()
}

// WriteHeader writes a header row: each name as a string field (delimiter-quoted
// when a Delimiter is set), separated and terminated like a data row.
func (cw *CSVWriter) WriteHeader(names [][]byte) error {
	cw.buf = cw.buf[:0]
	for i, name := range names {
		if i > 0 {
			cw.buf = append(cw.buf, cw.cfg.Separator...)
		}
		cw.buf = appendField(cw.buf, name, false, KindString, cw.cfg)
	}
	return cw.flush()
}

// flush appends the line terminator to the scratch and writes it to the file.
func (cw *CSVWriter) flush() error {
	cw.buf = append(cw.buf, cw.cfg.LineTerminator...)
	n, err := cw.w.Write(cw.buf)
	cw.written += int64(n)
	return err
}

// EstimateFileSize returns the bytes written to the current file.
func (cw *CSVWriter) EstimateFileSize() uint64 {
	return uint64(cw.written)
}

// Close finalizes the writer. Currently it's a no-op.
func (cw *CSVWriter) Close() error {
	return nil
}
