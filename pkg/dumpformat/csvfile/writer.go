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
	// buf is the reused per-row scratch.
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
	_, err := cw.w.Write(cw.buf)
	return err
}

// Close finalizes the writer. CSV has no format trailer, so this is a no-op kept
// for symmetry with ParquetWriter.
func (cw *CSVWriter) Close() error {
	return nil
}
