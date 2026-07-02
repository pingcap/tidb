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
// io.Writer, mirroring parquetfile.ParquetWriter. The caller owns buffering and
// file rotation.
type CSVWriter struct {
	w     io.Writer
	cfg   *Config
	kinds []FieldKind
	// buf is the reused per-row scratch.
	buf []byte
	// written tracks the bytes written to the current sink, for EstimateFileSize.
	written int64
}

// NewCSVWriter creates a CSVWriter over w. kinds classifies each column
// (Number/String/Bytes); cfg holds the framing knobs.
func NewCSVWriter(w io.Writer, kinds []FieldKind, cfg *Config) *CSVWriter {
	return &CSVWriter{w: w, cfg: cfg, kinds: kinds}
}

// Reset points the writer at a new sink and resets the size counter, keeping the
// column kinds, config and scratch buffer. Use it when the caller cuts a new
// file.
func (cw *CSVWriter) Reset(w io.Writer) {
	cw.w = w
	cw.written = 0
}

// Write encodes one row and writes it, with the line terminator, to the
// underlying writer. len(row) must equal the configured column count; a nil
// field is treated as NULL.
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

// WriteHeader writes a header row: each name as a quoted string field, separated
// and terminated like a data row.
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

// flush appends the line terminator to the scratch and writes it to the sink.
func (cw *CSVWriter) flush() error {
	cw.buf = append(cw.buf, cw.cfg.LineTerminator...)
	n, err := cw.w.Write(cw.buf)
	cw.written += int64(n)
	return err
}

// EstimateFileSize returns the bytes written to the current sink, mirroring
// parquetfile.ParquetWriter so callers rotate files uniformly across formats.
func (cw *CSVWriter) EstimateFileSize() uint64 {
	return uint64(cw.written)
}

// Close finalizes the writer. CSV has no format trailer, so this is a no-op kept
// for symmetry with ParquetWriter.
func (cw *CSVWriter) Close() error {
	return nil
}
