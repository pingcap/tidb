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
	"bytes"
	"database/sql"
	"fmt"
	"io"
)

// CSVWriter encodes CSV rows to an io.Writer, mirroring parquetfile.ParquetWriter.
// It owns the row framing (field separators + line terminator) and the per-field
// quoting/escaping; the caller supplies each row as already-stringified field
// bytes (Dumpling from sql.RawBytes, the exporter from textrow.AppendValueText).
//
// It is a single-stream encoder: buffering, file-size based rotation and upload
// stay with the caller, which swaps the sink at a row boundary via Reset (e.g.
// Dumpling's buffer-pool flush, the exporter's file cut).
type CSVWriter struct {
	w     io.Writer
	cfg   *Config
	kinds []FieldKind
	// buf is the reused per-row scratch for non-*bytes.Buffer sinks.
	buf []byte
}

// NewCSVWriter creates a CSVWriter over w. kinds classifies each column
// (Number/String/Bytes); cfg holds the framing knobs.
func NewCSVWriter(w io.Writer, kinds []FieldKind, cfg *Config) *CSVWriter {
	return &CSVWriter{w: w, cfg: cfg, kinds: kinds}
}

// Reset points the writer at a new sink, keeping the column kinds, config and
// scratch buffer. Use it when the caller swaps the underlying buffer/file at a
// row boundary.
func (cw *CSVWriter) Reset(w io.Writer) {
	cw.w = w
}

// Write encodes one row and writes it, with the line terminator, to the
// underlying writer. len(row) must equal the configured column count; a nil
// field is treated as NULL.
func (cw *CSVWriter) Write(row []sql.RawBytes) error {
	if len(row) != len(cw.kinds) {
		return fmt.Errorf("csvfile: row has %d fields, want %d", len(row), len(cw.kinds))
	}
	dst := cw.rowDst()
	for i, val := range row {
		if i > 0 {
			dst = append(dst, cw.cfg.Separator...)
		}
		dst = appendField(dst, val, val == nil, cw.kinds[i], cw.cfg)
	}
	dst = append(dst, cw.cfg.LineTerminator...)
	return cw.emit(dst)
}

// WriteHeader writes a header row: each name as a quoted string field, separated
// and terminated like a data row.
func (cw *CSVWriter) WriteHeader(names [][]byte) error {
	dst := cw.rowDst()
	for i, name := range names {
		if i > 0 {
			dst = append(dst, cw.cfg.Separator...)
		}
		dst = appendField(dst, name, false, KindString, cw.cfg)
	}
	dst = append(dst, cw.cfg.LineTerminator...)
	return cw.emit(dst)
}

// rowDst returns the build target for one row. For a *bytes.Buffer sink it
// returns the buffer's available capacity so the encoded row can be appended in
// place (no extra copy); otherwise it reuses the internal scratch.
func (cw *CSVWriter) rowDst() []byte {
	if bb, ok := cw.w.(*bytes.Buffer); ok {
		return bb.AvailableBuffer()
	}
	return cw.buf[:0]
}

// emit writes the encoded row to the sink. For the scratch path it keeps the
// grown buffer for reuse; for the *bytes.Buffer path Write extends the buffer in
// place when the row fit in the available capacity.
func (cw *CSVWriter) emit(dst []byte) error {
	if _, ok := cw.w.(*bytes.Buffer); !ok {
		cw.buf = dst
	}
	_, err := cw.w.Write(dst)
	return err
}

// Close finalizes the writer. CSV has no format trailer, so this is a no-op kept
// for symmetry with ParquetWriter.
func (cw *CSVWriter) Close() error {
	return nil
}
